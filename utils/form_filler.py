"""
Playwright form filling utilities.

Operates on an already-navigated Page with a known field list from
form_inspector.scan_fields().  Fills fields based on a candidate profile
and the job being applied for.

Design principles
-----------------
- Label-first matching: field labels (or DOM context for anonymous fields)
  are used to decide what to fill.  HTML name/id attributes are unreliable
  across ATS platforms.
- Conservative checkboxes: only tick boxes whose label clearly matches a
  profile skill or a known attribute; never untick pre-selected boxes.
- Dry-run safe: pass dry_run=True to log actions without touching the DOM.
- Returns a structured report so the caller can show what happened.
"""
from __future__ import annotations

import re
from typing import Any

from playwright.async_api import Page


# ---------------------------------------------------------------------------
# Label → profile field mapping for plain text / textarea inputs
# Each entry: ([keywords_in_label], callable(profile, job) -> str)
# The FIRST rule whose keywords all appear in the normalised label wins.
# ---------------------------------------------------------------------------
_TEXT_RULES: list[tuple[list[str], Any]] = [
    (["name"],         lambda p, j: p.get("personal", {}).get("name", "")),
    (["email"],        lambda p, j: p.get("personal", {}).get("email", "")),
    (["phone"],        lambda p, j: p.get("personal", {}).get("phone", "")),
    (["country"],      lambda p, j: p.get("personal", {}).get("phone_country", "") or p.get("personal", {}).get("location", "").split(",")[-1].strip()),
    (["linkedin"],     lambda p, j: p.get("personal", {}).get("linkedin", "")),
    (["portfolio"],    lambda p, j: p.get("personal", {}).get("website", "")),
    (["website"],      lambda p, j: p.get("personal", {}).get("website", "")),
    (["github"],       lambda p, j: p.get("personal", {}).get("github", "")),
    (["location", "based"],   lambda p, j: p.get("personal", {}).get("location", "")),
    (["location"],     lambda p, j: p.get("personal", {}).get("location", "")),
    (["where"],        lambda p, j: p.get("personal", {}).get("location", "")),
    (["city"],         lambda p, j: p.get("personal", {}).get("location", "")),
    (["country"],      lambda p, j: p.get("personal", {}).get("location", "")),
    (["years"],        lambda p, j: _years_label(p)),
    (["start"],        lambda p, j: _years_label(p)),
    (["experience"],   lambda p, j: _years_label(p)),
    (["referral", "hear"],    lambda p, j: "Remotive"),
    (["how did you find"],    lambda p, j: "Remotive"),
    (["salary"],       lambda p, j: p.get("preferences", {}).get("rate", "")),
    (["rate"],         lambda p, j: p.get("preferences", {}).get("rate", "")),
    (["compensation"], lambda p, j: p.get("preferences", {}).get("rate", "")),
    # Freeform fields — filled from the pre-generated cover letter if available.
    (["cover letter"],     lambda p, j: j.get("cover_letter", "")),
    (["motivation"],       lambda p, j: j.get("cover_letter", "")),
    (["why do you want"],  lambda p, j: j.get("cover_letter", "")),
    (["tell us about"],    lambda p, j: j.get("cover_letter", "")),
]

# Timezone label keyword → profile timezone values that match (lowercase)
_TIMEZONE_MATCHES: list[tuple[str, list[str]]] = [
    ("utc −08", ["los_angeles", "pacific", "pst", "pdt", "pt", "utc-8", "utc-08"]),
    ("utc −07", ["denver", "mountain", "mst", "mdt", "utc-7"]),
    ("utc −06", ["chicago", "central", "cst", "cdt", "utc-6"]),
    ("utc −05", ["new_york", "eastern", "est", "edt", "et", "utc-5"]),
    ("utc −04", ["halifax", "atlantic", "ast", "utc-4"]),
    ("utc ±00", ["london", "gmt", "utc+0", "utc0", "utc±0", "dublin", "lisbon"]),
    ("utc +01", ["berlin", "paris", "warsaw", "cet", "utc+1", "amsterdam", "tunis"]),
    ("utc +02", ["istanbul", "eet", "utc+2", "cairo", "bucharest"]),
    ("utc +03", ["moscow", "riyadh", "utc+3", "nairobi"]),
    ("utc +04", ["dubai", "utc+4", "baku"]),
    ("utc +05", ["karachi", "utc+5"]),
    ("utc +06", ["dhaka", "utc+6"]),
    ("utc +07", ["bangkok", "utc+7", "jakarta"]),
    ("utc +08", ["singapore", "taipei", "shanghai", "utc+8", "hongkong"]),
    ("utc +09", ["tokyo", "seoul", "utc+9"]),
    ("utc +10", ["sydney", "utc+10", "brisbane"]),
    ("utc +12", ["auckland", "utc+12", "wellington"]),
]

# Radio / checkbox labels that represent a "developer / engineer" career path
_DEVELOPER_LABELS = [
    "developer", "engineer", "coder", "computer code",
    "programming", "software",
]

# Comfort / autonomy radios: prefer the most positive option by default
_COMFORT_PREFER_HIGH = [
    "very comfortable",
    "highly",
    "super effective",
    "excellent",
]


# Placeholders that are too generic to identify the field — treat these
# the same as having no placeholder at all.
_GENERIC_PLACEHOLDERS = {"your answer", "type your answer", "enter your answer",
                         "write here", "...", "answer"}

# Words that, when combined with a seniority marker, indicate a label is a
# job-title option (e.g. "Sr. Firmware Engineer") rather than a skills or
# career-type checkbox.
_ROLE_WORDS = {"engineer", "designer", "developer", "manager", "director",
               "analyst", "scientist", "architect", "lead", "specialist",
               "producer", "researcher", "strategist", "creator", "writer"}
_SENIORITY_WORDS = {"sr", "jr", "senior", "junior", "mid", "principal",
                    "staff", "vp", "head", "chief"}


def _is_job_title_label(label_lower: str) -> bool:
    """Return True if the label looks like a job-title option (role + seniority)."""
    words = set(re.findall(r"\w+", label_lower))
    return bool(words & _ROLE_WORDS) and bool(words & _SENIORITY_WORDS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fill_form(
    page: Page,
    fields: list[dict],
    profile: dict,
    job: dict,
    dry_run: bool = False,
) -> list[dict]:
    """Fill all detected form fields based on the profile and job.

    Returns a list of action records:
        {"field": label, "type": type, "action": "filled"/"checked"/"skipped", "value": value}
    """
    actions: list[dict] = []

    # Build DOM context for anonymous fields once (expensive JS call).
    context_map = await _build_context_map(page, fields)

    # Track text fields that have no meaningful identifier (including those
    # with generic placeholders like "Your answer") so we can fill by position.
    def _is_anonymous_text(f: dict) -> bool:
        return (
            f["type"] in ("text", "email", "tel", "number", "url")
            and not f["label"]
            and (not f["placeholder"] or f["placeholder"].lower() in _GENERIC_PLACEHOLDERS)
            and not f["name"]
            and not f["id"]
        )

    anon_text_order: list[int] = [i for i, f in enumerate(fields) if _is_anonymous_text(f)]
    anon_text_position: dict[int, int] = {
        field_idx: pos for pos, field_idx in enumerate(anon_text_order)
    }

    # Group checkbox/radio fields by their name attribute so we can handle
    # them as question groups (only one JS click per group for radios).
    handled_groups: set[str] = set()

    for idx, field in enumerate(fields):
        label = _effective_label(field, context_map.get(idx, ""))
        label_lower = label.lower()
        ftype = field["type"]
        fname = field["name"]

        # ---- text-like inputs -----------------------------------------
        if ftype in ("text", "email", "tel", "number", "url", "textarea"):
            value = _resolve_text_value(label_lower, profile, job)

            # For anonymous/generic fields fall back to position heuristic.
            if value == "" and idx in anon_text_position:
                pos = anon_text_position[idx]
                if pos == 0:
                    value = profile.get("personal", {}).get("name", "")
                elif pos == 1:
                    value = profile.get("personal", {}).get("email", "")

            if not value:
                actions.append({"field": label or f"anon-text-{idx}", "type": ftype,
                                 "action": "skipped", "value": ""})
                continue

            if not dry_run:
                try:
                    # Anonymous fields have no id/name/label to locate by;
                    # use placeholder + nth-of-type position instead.
                    if idx in anon_text_position:
                        el = await _locate_by_position(page, field, anon_text_position[idx])
                    else:
                        el = await _locate_field(page, field)
                    if el:
                        await el.fill(value, force=True)
                    else:
                        actions.append({"field": label or f"anon-text-{idx}", "type": ftype,
                                        "action": "error", "value": "element not found"})
                        continue
                except Exception as e:
                    actions.append({"field": label or f"anon-text-{idx}", "type": ftype,
                                    "action": "error", "value": str(e)})
                    continue

            actions.append({"field": label or f"anon-text-{idx}", "type": ftype,
                            "action": "filled", "value": value})

        # ---- checkboxes ------------------------------------------------
        elif ftype == "checkbox":
            should_check = _should_check(label_lower, profile, job)
            if should_check is None:
                actions.append({"field": label, "type": "checkbox",
                                 "action": "skipped", "value": ""})
                continue

            if not dry_run and should_check:
                try:
                    el = await _locate_field(page, field)
                    if el and not await el.is_checked():
                        await el.check(force=True)
                except Exception as e:
                    actions.append({"field": label, "type": "checkbox",
                                    "action": "error", "value": str(e)})
                    continue

            actions.append({"field": label, "type": "checkbox",
                            "action": "checked" if should_check else "skipped",
                            "value": str(should_check)})

        # ---- radio buttons ---------------------------------------------
        elif ftype == "radio":
            group_key = fname or f"radio-group-{idx}"
            if group_key in handled_groups:
                continue  # already handled this question

            # Collect all options in this group.
            group_fields = [
                f for f in fields
                if f["type"] == "radio"
                and (f["name"] == fname if fname else f == field)
            ]
            option_labels = [
                _effective_label(f, context_map.get(fields.index(f), "")).lower()
                for f in group_fields
            ]
            chosen = _pick_radio(option_labels, label_lower, profile, job)

            # Use first option's context (question text) as the group display name.
            group_display = context_map.get(idx, group_key) or group_key

            if chosen is not None:
                chosen_field = group_fields[chosen]
                chosen_label = _effective_label(
                    chosen_field,
                    context_map.get(fields.index(chosen_field), "")
                )
                if not dry_run:
                    try:
                        el = await _locate_field(page, chosen_field)
                        if el:
                            await el.check(force=True)
                    except Exception as e:
                        actions.append({"field": group_display, "type": "radio",
                                        "action": "error", "value": str(e)})
                        handled_groups.add(group_key)
                        continue

                actions.append({"field": group_display, "type": "radio",
                                "action": "selected", "value": chosen_label})
            else:
                actions.append({"field": group_display, "type": "radio",
                                "action": "skipped", "value": ""})

            handled_groups.add(group_key)

        # ---- select dropdowns -----------------------------------------
        elif ftype == "select":
            value = _resolve_text_value(label_lower, profile, job)
            if value and not dry_run:
                try:
                    el = await _locate_field(page, field)
                    if el:
                        # Try label match first, then value, then partial label.
                        try:
                            await el.select_option(label=value)
                        except Exception:
                            try:
                                await el.select_option(value=value)
                            except Exception:
                                # Partial match: find an option containing the value.
                                options = await el.query_selector_all("option")
                                for opt in options:
                                    text = (await opt.inner_text()).strip()
                                    if value.lower() in text.lower():
                                        opt_val = await opt.get_attribute("value") or text
                                        await el.select_option(value=opt_val)
                                        break
                        actions.append({"field": label or fname, "type": "select",
                                        "action": "selected", "value": value})
                    else:
                        actions.append({"field": label or fname, "type": "select",
                                        "action": "error", "value": "element not found"})
                except Exception as e:
                    actions.append({"field": label or fname, "type": "select",
                                    "action": "error", "value": str(e)})
            elif value and dry_run:
                actions.append({"field": label or fname, "type": "select",
                                "action": "selected", "value": value})
            else:
                actions.append({"field": label or fname, "type": "select",
                                "action": "skipped", "value": ""})

        # ---- file inputs ----------------------------------------------
        elif ftype == "file":
            resume_path = _resolve_resume_path(profile, job)
            if resume_path and not dry_run:
                try:
                    el = await _locate_field(page, field)
                    if el:
                        await el.set_input_files(resume_path)
                        actions.append({"field": label or fname, "type": "file",
                                        "action": "uploaded", "value": resume_path})
                    else:
                        actions.append({"field": label or fname, "type": "file",
                                        "action": "error", "value": "element not found"})
                except Exception as e:
                    actions.append({"field": label or fname, "type": "file",
                                    "action": "error", "value": str(e)})
            elif resume_path and dry_run:
                actions.append({"field": label or fname, "type": "file",
                                "action": "uploaded", "value": resume_path})
            else:
                actions.append({"field": label or fname, "type": "file",
                                "action": "skipped", "value": "(no resume path resolved)"})

    return actions


async def try_upload_resume(
    page: Page,
    profile: dict,
    job: dict,
    dry_run: bool = False,
) -> str:
    """Attempt to upload the resume via a custom file-picker button (e.g. Notion).

    Looks for any visible button/label whose text contains 'upload' near a
    'resume' heading.  Uses Playwright's expect_file_chooser() to intercept
    the native file dialog before it opens and set the file programmatically.

    Returns a status string: 'uploaded', 'skipped', or an error message.
    """
    resume_path = _resolve_resume_path(profile, job)
    if not resume_path:
        return "skipped (no resume path resolved)"

    if dry_run:
        return f"would upload {resume_path}"

    # Selectors for common upload trigger buttons.
    upload_selectors = [
        "button:has-text('Upload')",
        "label:has-text('Upload')",
        "[role='button']:has-text('Upload')",
        "button:has-text('Choose file')",
        "button:has-text('Browse')",
    ]

    for selector in upload_selectors:
        try:
            btn = page.locator(selector).first
            if await btn.count() == 0 or not await btn.is_visible(timeout=1500):
                continue
            async with page.expect_file_chooser(timeout=5000) as fc_info:
                await btn.click()
            fc = await fc_info.value
            await fc.set_files(resume_path)
            return f"uploaded {resume_path}"
        except Exception:
            continue

    return "skipped (no upload button found)"


def format_fill_report(actions: list[dict]) -> str:
    """Format the fill action list into a human-readable summary."""
    if not actions:
        return "  (nothing filled)"
    lines = []
    for a in actions:
        icon = {"filled": "✓", "checked": "✓", "selected": "✓", "uploaded": "✓",
                "skipped": "–", "error": "!"}.get(a["action"], "?")
        value_hint = f" = {repr(a['value'])}" if a["value"] else ""
        lines.append(f"  {icon} [{a['type']}] {a['field']}{value_hint}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _effective_label(field: dict, context: str) -> str:
    """Return the best available label for a field.

    Generic placeholders like 'Your answer' are not real labels — skip them
    so that DOM-derived context can be used instead.
    """
    if field.get("label"):
        return field["label"]
    placeholder = field.get("placeholder") or ""
    if placeholder and placeholder.lower() not in _GENERIC_PLACEHOLDERS:
        return placeholder
    return context or ""


def _resolve_text_value(label_lower: str, profile: dict, job: dict) -> str:
    """Match a normalised label against the text rules and return the value."""
    for keywords, getter in _TEXT_RULES:
        if all(kw in label_lower for kw in keywords):
            return getter(profile, job) or ""
    return ""


def _should_check(label_lower: str, profile: dict, job: dict) -> bool | None:
    """Decide whether to check a checkbox.

    Returns True to check, False to leave unchecked, None to skip entirely.
    """
    profile_skills = [s.lower() for s in (profile.get("skills") or [])]
    profile_keywords = [k.lower() for k in (profile.get("keywords") or [])]
    job_title = (job.get("title") or "").lower()
    job_title_words = set(re.findall(r"\w+", job_title)) - {"sr", "jr", "the", "a", "an"}

    # Agreement / consent checkboxes — always check.
    # Normalize apostrophes before matching (curly vs straight).
    label_norm = label_lower.replace("\u2019", "'").replace("\u2018", "'")
    if any(kw in label_norm for kw in ["agree", "i've read", "confirm", "accept", "i understand", "consent"]):
        return True

    # Availability / engagement preferences from profile.
    availability = [a.lower() for a in (profile.get("preferences", {}).get("availability") or [])]
    if availability and any(av in label_lower for av in availability):
        return True

    # Newsletter opt-in — default yes.
    if any(kw in label_lower for kw in ["newsletter", "yes pls", "subscribe", "notify me"]):
        return True

    # Referral source — check Remotive if listed.
    if "remotive" in label_lower:
        return True

    # Job-title shaped labels (e.g. "Sr. Firmware Engineer", "Sr. Brand Designer"):
    # ONLY check the one that matches the specific job being applied for.
    # Compare on specific/distinguishing words only — strip generic role words
    # (engineer, designer, developer…) that appear in every job title, so that
    # "Sr. Frontend Engineer" does NOT match when applying for "Sr. Firmware Engineer".
    if _is_job_title_label(label_lower):
        _stop = _ROLE_WORDS | _SENIORITY_WORDS | {"the", "a", "an"}
        label_specific = set(re.findall(r"\w+", label_lower)) - _stop
        job_specific   = job_title_words - _ROLE_WORDS
        if label_specific and job_specific and (label_specific & job_specific):
            return True
        return None  # different role — leave untouched

    # Strip parenthetical clarifiers before keyword matching so that
    # e.g. "Content Creator (video / 3d / illustrator / artist / etc)"
    # doesn't match the "video" keyword — the core label is "Content Creator".
    core_label = re.sub(r'\(.*?\)', '', label_lower).strip()

    # Skills checkboxes — check if label matches a profile skill.
    for skill in profile_skills:
        if skill in core_label or core_label in skill:
            return True
    for kw in profile_keywords:
        if kw in core_label:
            return True

    # Career type — check the developer/engineer option, but not creative
    # hybrids like "Design Engineer (I've mostly coded up my own designs)".
    _CREATIVE_WORDS = {"design", "pixel", "illustrat", "artist", "3d", "animation"}
    if any(dev in core_label for dev in _DEVELOPER_LABELS):
        if not any(c in core_label for c in _CREATIVE_WORDS):
            return True

    return None  # don't touch


def _pick_radio(
    option_labels: list[str],
    question_label: str,
    profile: dict,
    job: dict,
) -> int | None:
    """Choose the index of the best radio option, or None to skip."""
    if not option_labels:
        return None

    profile_tz = (profile.get("personal", {}).get("timezone") or "").lower()

    # Timezone question.
    if "timezone" in question_label or "utc" in " ".join(option_labels[:2]):
        for i, opt in enumerate(option_labels):
            for tz_key, tz_values in _TIMEZONE_MATCHES:
                if tz_key in opt and any(
                    re.search(r'\b' + re.escape(v) + r'\b', profile_tz)
                    for v in tz_values
                ):
                    return i
        return None  # don't guess timezone

    # Comfort / self-assessment questions — pick the most positive option.
    comfort_triggers = [
        "comfortable", "async", "time management", "prioritiz",
        "communicat", "manag", "client", "punctual",
    ]
    if any(t in question_label for t in comfort_triggers):
        for i, opt in enumerate(option_labels):
            if any(phrase in opt for phrase in _COMFORT_PREFER_HIGH):
                return i
        return 0  # fall back to first option

    return None  # skip unknown radio groups


async def _build_context_map(page: Page, fields: list[dict]) -> dict[int, str]:
    """Look up the DOM for the nearest preceding text block for fields that
    have no label or placeholder of their own (e.g. Notion anonymous inputs).

    Radio/checkbox fields are skipped — their label IS the option text, and
    resolving context for all 160+ of them triggers Notion's lazy-render
    scroll.  Radio group display names fall back to the selected option label.

    For fully anonymous fields (no id, no name, generic placeholder) a single
    batch JS call retrieves context by DOM position without individual lookups.

    Returns a dict mapping field-index → context string.
    """
    context_map: dict[int, str] = {}

    # Fields with id or name: walk DOM per element (cheap, few of these).
    for idx, field in enumerate(fields):
        if field.get("label") or field.get("placeholder"):
            continue
        if not field.get("id") and not field.get("name"):
            continue  # handled by batch below
        try:
            context = await _get_dom_context(page, field)
            if context:
                context_map[idx] = context
        except Exception:
            pass

    # Fully anonymous fields (no id, no name): one JS call to get context
    # for all of them in DOM order, then map back to field indices.
    anon_indices = [
        idx for idx, f in enumerate(fields)
        if not f.get("id") and not f.get("name") and not f.get("label")
        and f.get("type") not in ("checkbox", "radio")
    ]
    if anon_indices:
        placeholder = fields[anon_indices[0]].get("placeholder") or "Your answer"
        tag = fields[anon_indices[0]].get("tag") or "input"
        try:
            batch: list[str] = await page.evaluate(
                """([tag, ph]) => {
                    const sel = ph
                        ? tag + '[placeholder="' + ph + '"], textarea[placeholder="' + ph + '"]'
                        : tag + ', textarea';
                    const inputs = Array.from(document.querySelectorAll(sel))
                        .filter(el => el.offsetParent !== null);
                    if (!inputs.length) return [];

                    // Single pass: walk all nodes in document order.
                    // Track the last short text seen; when we hit a target
                    // input, that text is its label.  Reset after each input
                    // so labels don't bleed across questions.
                    const results = new Array(inputs.length).fill('');
                    let lastText = '';
                    let nextIdx = 0;
                    const walker = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_ALL
                    );
                    let node;
                    while ((node = walker.nextNode()) && nextIdx < inputs.length) {
                        if (node.nodeType === Node.TEXT_NODE) {
                            const pTag = node.parentElement
                                ? node.parentElement.tagName.toLowerCase() : '';
                            if (pTag !== 'style' && pTag !== 'script') {
                                const t = node.textContent.trim();
                                if (t.length > 2 && t.length < 150) lastText = t;
                            }
                        } else if (node.nodeType === Node.ELEMENT_NODE
                                   && node === inputs[nextIdx]) {
                            results[nextIdx] = lastText;
                            lastText = '';
                            nextIdx++;
                        }
                    }
                    return results;
                }""",
                [tag, placeholder],
            )
            for pos, idx in enumerate(anon_indices):
                if pos < len(batch) and batch[pos]:
                    context_map[idx] = batch[pos]
        except Exception:
            pass

    return context_map


async def _get_dom_context(page: Page, field: dict) -> str:
    """Walk the DOM upward from the field element to find the nearest
    preceding text that looks like a question label.

    Searches up to 10 ancestor levels and also checks first-child text
    inside ancestor containers, which is how Notion lays out its form
    question blocks.
    """
    fid = field.get("id") or ""
    fname = field.get("name") or ""
    ftype = field.get("type") or ""
    tag = field.get("tag") or "input"

    if not fid and not fname:
        return ""

    context: str = await page.evaluate(
        """([fid, fname, ftype, tag]) => {
            // Use getElementById to avoid CSS-escaping issues with colons etc.
            let el = fid ? document.getElementById(fid) : null;
            if (!el && fname && !['checkbox','radio'].includes(ftype)) {
                el = document.querySelector(tag + '[name=\"' + fname + '\"]');
            }
            if (!el) return '';
            let node = el.parentElement;
            for (let depth = 0; depth < 10; depth++) {
                if (!node) break;
                let sib = node.previousElementSibling;
                while (sib) {
                    const text = (sib.innerText || '').trim();
                    if (text.length > 2 && text.length < 300) return text;
                    sib = sib.previousElementSibling;
                }
                const direct = (node.firstChild && node.firstChild.textContent || '').trim();
                if (direct.length > 2 && direct.length < 300 && direct !== (el.innerText || '').trim()) {
                    return direct;
                }
                node = node.parentElement;
            }
            return '';
        }""",
        [fid, fname, ftype, tag],
    )
    return (context or "").strip()


def _build_selector(field: dict) -> str:
    """Build a CSS selector that uniquely targets a field element.

    Avoids raw ID selectors for IDs that contain characters requiring
    CSS escaping (colons, brackets, etc.) — use getElementById instead
    for those cases.
    """
    fid = field.get("id") or ""
    fname = field.get("name") or ""
    ftype = field.get("type") or ""
    tag = field.get("tag") or "input"

    # Only use #id selector when the ID is safe (no special CSS chars).
    if fid and re.match(r'^[\w-]+$', fid):
        return f"#{fid}"
    if fname and ftype not in ("checkbox", "radio"):
        return f"{tag}[name='{fname}']"
    return ""


async def _locate_field(page: Page, field: dict):
    """Return an ElementHandle for the field, or None if not locatable."""
    fid = field.get("id") or ""

    # Prefer getElementById — works for any ID including those with colons.
    if fid:
        try:
            el = await page.evaluate_handle(
                "id => document.getElementById(id)", fid
            )
            # evaluate_handle always returns a handle; check it's a real element.
            el = el.as_element()
            if el and await el.is_visible():
                return el
        except Exception:
            pass

    selector = _build_selector(field)
    if selector:
        try:
            el = await page.query_selector(selector)
            if el and await el.is_visible():
                return el
        except Exception:
            pass

    # Fallback: find by label text.
    label = field.get("label")
    if label:
        try:
            el = await page.get_by_label(label).first.element_handle()
            return el
        except Exception:
            pass

    # Last resort: find by placeholder (common on Ashby and other React ATSes
    # that don't use <label for=...> but do set placeholder attributes).
    placeholder = field.get("placeholder")
    if placeholder:
        try:
            el = await page.get_by_placeholder(placeholder).first.element_handle()
            if el and await el.is_visible():
                return el
        except Exception:
            pass

    return None


async def _locate_by_position(page: Page, field: dict, pos: int):
    """Locate a field that has no id/name/label by placeholder + nth position."""
    placeholder = field.get("placeholder") or ""
    tag = field.get("tag") or "input"
    if placeholder:
        try:
            return await page.locator(
                f'{tag}[placeholder="{placeholder}"]'
            ).nth(pos).element_handle()
        except Exception:
            pass
    # Fall back to any visible input of the same type at that position.
    ftype = field.get("type") or "text"
    try:
        return await page.locator(
            f'{tag}[type="{ftype}"]'
        ).nth(pos).element_handle()
    except Exception:
        pass
    return None


def _resolve_resume_path(profile: dict, job: dict) -> str:
    """Return the local path of the best-matching resume PDF for the job.

    Looks at job.recommended_resume first; if not set, picks the resume
    whose tags have the most overlap with the job's title words and profile
    keywords.  Falls back to the first resume in the list.
    """
    import os

    resumes = profile.get("resumes") or []
    if not resumes:
        return ""

    # If the job carries a recommendation, honour it directly.
    recommended = (job or {}).get("recommended_resume") or ""
    if recommended:
        for r in resumes:
            if r.get("name") == recommended:
                path = r.get("path", "")
                if path and os.path.isfile(path):
                    return path

    # Score each resume by tag overlap with job title + profile keywords.
    job_words = set(re.findall(r"\w+", ((job or {}).get("title") or "").lower()))
    profile_kws = {k.lower() for k in (profile.get("keywords") or [])}
    combined = job_words | profile_kws

    best_path = ""
    best_score = -1
    for r in resumes:
        tags = {t.lower() for t in (r.get("tags") or [])}
        score = len(tags & combined)
        if score > best_score:
            path = r.get("path", "")
            if path and os.path.isfile(path):
                best_score = score
                best_path = path

    # Last resort: first resume with an existing file.
    if not best_path:
        for r in resumes:
            path = r.get("path", "")
            if path and os.path.isfile(path):
                return path

    return best_path


def _years_label(profile: dict) -> str:
    """Return a plain-text years-of-experience hint from the profile."""
    summary = profile.get("summary") or []
    for line in summary:
        if "year" in str(line).lower():
            return str(line)
    return ""
