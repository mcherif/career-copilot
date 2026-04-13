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

import asyncio
import re
from typing import Any

from playwright.async_api import Page


# ---------------------------------------------------------------------------
# Label → profile field mapping for plain text / textarea inputs
# Each entry: ([keywords_in_label], callable(profile, job) -> str)
# The FIRST rule whose keywords all appear in the normalised label wins.
# ---------------------------------------------------------------------------
_TEXT_RULES: list[tuple[list[str], Any]] = [
    # First/last name rules must come before the generic "name" rule.
    (["first name"], lambda p, j: (
        p.get("personal", {}).get("name", "") or "").split()[0]),
    (["firstname"], lambda p, j: (
        p.get("personal", {}).get("name", "") or "").split()[0]),
    (["first_name"], lambda p, j: (
        p.get("personal", {}).get("name", "") or "").split()[0]),
    (["last name"], lambda p, j: " ".join(
        (p.get("personal", {}).get("name", "") or "").split()[1:])),
    (["lastname"], lambda p, j: " ".join(
        (p.get("personal", {}).get("name", "") or "").split()[1:])),
    (["last_name"], lambda p, j: " ".join(
        (p.get("personal", {}).get("name", "") or "").split()[1:])),
    (["name"], lambda p, j: p.get("personal", {}).get("name", "")),
    (["email"], lambda p, j: p.get("personal", {}).get("email", "")),
    (["phone"], lambda p, j: j.get("_phone_value")
     or p.get("personal", {}).get("phone", "")),
    # Work authorization — multi-keyword rules MUST come before single-word
    # geographic rules like ["country"] and ["location"] so they win when a
    # question label contains "country" or "location" in a broader context.
    (["legally entitled", "canada"], lambda p, j: "yes" if p.get(
        "work_authorization", {}).get("canada") else "no"),
    (["authorized to work", "canada"], lambda p, j: "yes" if p.get(
        "work_authorization", {}).get("canada") else "no"),
    (["eligible to work", "canada"], lambda p, j: "yes" if p.get(
        "work_authorization", {}).get("canada") else "no"),
    (["legally entitled", "tunisia"], lambda p, j: "yes" if p.get(
        "work_authorization", {}).get("tunisia") else "no"),
    (["sponsorship"], lambda p, j: "no" if not p.get(
        "work_authorization", {}).get("sponsorship_required", True) else "yes"),
    (["require sponsorship"], lambda p, j: "no" if not p.get(
        "work_authorization", {}).get("sponsorship_required", True) else "yes"),
    (["need sponsorship"], lambda p, j: "no" if not p.get(
        "work_authorization", {}).get("sponsorship_required", True) else "yes"),
    # Hispanic / Latino yes-or-no question — derive from profile race.
    (["hispanic", "latino"], lambda p, j: "yes" if any(
        s in (p.get("personal", {}).get("race") or "").lower()
        for s in ("hispanic", "latino", "latina")
    ) else "no"),
    # Non-compete / restrictive covenant — always "no"
    (["non-compete"], lambda p, j: "no"),
    (["noncompete"], lambda p, j: "no"),
    (["restrictive covenant"], lambda p, j: "no"),
    (["non-solicitation"], lambda p, j: "no"),
    # "Are you legally authorized to work in the country/location in which this role is located?"
    # Multi-keyword match wins over the single-word ["country"] / ["location"] below.
    (["authorized to work", "country"], lambda p, j: _auth_for_job_country(p, j)),
    (["authorized to work", "located"], lambda p, j: _auth_for_job_country(p, j)),
    (["authorized to work", "location"], lambda p, j: _auth_for_job_country(p, j)),
    (["eligible to work", "country"], lambda p, j: _auth_for_job_country(p, j)),
    (["eligible to work", "located"], lambda p, j: _auth_for_job_country(p, j)),
    (["eligible to work", "location"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally entitled", "country"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally entitled", "located"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally entitled", "location"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally authorized", "country"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally authorized", "located"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally authorized", "location"], lambda p, j: _auth_for_job_country(p, j)),
    (["legally entitled"], lambda p, j: "yes"),
    (["authorized to work"], lambda p, j: "yes"),
    (["eligible to work"], lambda p, j: "yes"),
    (["work authorization"], lambda p, j: "yes"),
    (["country"], lambda p, j: p.get("personal", {}).get("phone_country", "")
     or p.get("personal", {}).get("location", "").split(",")[-1].strip()),
    (["linkedin"], lambda p, j: p.get("personal", {}).get("linkedin", "")),
    (["portfolio"], lambda p, j: p.get("personal", {}).get(
        "website", "") or p.get("personal", {}).get("github", "")),
    (["website"], lambda p, j: p.get("personal", {}).get(
        "website", "") or p.get("personal", {}).get("github", "")),
    (["github"], lambda p, j: p.get("personal", {}).get("github", "")),
    # "Share a link to your portfolio / recent projects" — point to github when no website.
    (["share", "link"], lambda p, j: p.get("personal", {}).get(
        "website", "") or p.get("personal", {}).get("github", "")),
    (["recent project"], lambda p, j: p.get("personal", {}).get(
        "website", "") or p.get("personal", {}).get("github", "")),
    (["work sample"], lambda p, j: p.get("personal", {}).get(
        "website", "") or p.get("personal", {}).get("github", "")),
    (["location", "based"], lambda p, j: p.get(
        "personal", {}).get("location", "")),
    (["location"], lambda p, j: p.get("personal", {}).get("location", "")),
    (["where"], lambda p, j: p.get("personal", {}).get("location", "")),
    (["city"], lambda p, j: p.get("personal", {}).get("location", "")),
    (["country"], lambda p, j: p.get("personal", {}).get("location", "")),
    (["years of experience"], lambda p, j: _years_label(p)),
    (["years experience"], lambda p, j: _years_label(p)),
    (["how many years"], lambda p, j: _years_label(p)),
    (["years"], lambda p, j: _years_label(p)),
    (["start date"], lambda p, j: _years_label(p)),
    (["referral", "hear"], lambda p, j: p.get("preferences", {}).get(
        "referral_source", "internet search")),
    (["how did you find"], lambda p, j: p.get("preferences", {}).get(
        "referral_source", "internet search")),
    (["hear about"], lambda p, j: p.get("preferences", {}).get(
        "referral_source", "internet search")),
    (["salary"], lambda p, j: p.get("preferences", {}).get("rate", "")),
    (["rate"], lambda p, j: p.get("preferences", {}).get("rate", "")),
    (["compensation"], lambda p, j: p.get("preferences", {}).get("rate", "")),
    # Pronouns
    (["pronouns"], lambda p, j: p.get("personal", {}).get("pronouns", "")),
    (["preferred pronouns"], lambda p, j: p.get("personal", {}).get("pronouns", "")),
    # Current job title / role
    (["current title"], lambda p, j: p.get("personal", {}).get("current_title", "")),
    (["current job title"], lambda p, j: p.get("personal", {}).get("current_title", "")),
    (["current position"], lambda p, j: p.get("personal", {}).get("current_title", "")),
    (["current role"], lambda p, j: p.get("personal", {}).get("current_title", "")),
    (["job title"], lambda p, j: p.get("personal", {}).get("current_title", "")),
    # Current employer / company name.
    (["current company"], lambda p, j: p.get("personal", {}).get("current_company", "")),
    (["current employer"], lambda p, j: p.get("personal", {}).get("current_company", "")),
    (["company name"], lambda p, j: p.get("personal", {}).get("current_company", "")),
    (["organization"], lambda p, j: p.get("personal", {}).get("current_company", "")),
    # "org" is Lever's name attribute for the current company field.
    (["org"], lambda p, j: p.get("personal", {}).get("current_company", "")),
    # Cover letter field — use the pre-generated cover letter.
    # Other freeform/motivational textareas get LLM-generated answers at fill-time.
    (["cover letter"], lambda p, j: j.get("cover_letter", "")),
    (["gender"], lambda p, j: p.get("personal", {}).get("gender", "")),
    (["sex"], lambda p, j: p.get("personal", {}).get("gender", "")),
    (["race"], lambda p, j: p.get("personal", {}).get("race", "")),
    (["ethnicity"], lambda p, j: p.get("personal", {}).get("race", "")),
    (["disability"], lambda p, j: p.get("personal", {}).get("disability", "")),
    (["veteran"], lambda p, j: p.get("personal", {}).get("veteran", "")),
    (["armed forces"], lambda p, j: p.get("personal", {}).get("veteran", "")),
    (["military"], lambda p, j: p.get("personal", {}).get("veteran", "")),
    (["sexual orientation"], lambda p, j: p.get(
        "personal", {}).get("sexual_orientation", "")),
    (["lgbtq"], lambda p, j: p.get("personal", {}).get("sexual_orientation", "")),
    (["lgbtqia"], lambda p, j: p.get("personal", {}).get("sexual_orientation", "")),
    (["2slgbtqia"], lambda p, j: p.get("personal", {}).get("sexual_orientation", "")),
    (["person of colour"], lambda p, j: p.get(
        "personal", {}).get("person_of_colour", "")),
    (["person of color"], lambda p, j: p.get(
        "personal", {}).get("person_of_colour", "")),
    (["colour"], lambda p, j: p.get("personal", {}).get("person_of_colour", "")),
    (["color"], lambda p, j: p.get("personal", {}).get("person_of_colour", "")),
    (["age group"], lambda p, j: _age_range(p)),
    (["age range"], lambda p, j: _age_range(p)),
    (["age bracket"], lambda p, j: _age_range(p)),
    # Strongest / preferred programming language
    # "different from strongest" / "second" → second_language; otherwise strongest_language.
    # Order matters: more-specific patterns first so they win over broader ones.
    (["different", "language"], lambda p, j: p.get(
        "personal", {}).get("second_language", "")),
    (["second", "language"], lambda p, j: p.get(
        "personal", {}).get("second_language", "")),
    (["strongest", "language"], lambda p, j: p.get(
        "personal", {}).get("strongest_language", "")),
    (["primary", "language"], lambda p, j: p.get(
        "personal", {}).get("strongest_language", "")),
    (["preferred", "language"], lambda p, j: p.get("personal", {}).get(
        "second_language", "") or p.get("personal", {}).get("strongest_language", "")),
    # Human/spoken languages — only when the field is clearly about spoken languages,
    # NOT programming languages ("experience with the language", "elixir language" etc.)
    (["what languages", "speak"], lambda p, j: ", ".join(p.get("languages", []))),
    (["speak fluently"], lambda p, j: ", ".join(p.get("languages", []))),
    (["languages do you speak"], lambda p, j: ", ".join(p.get("languages", []))),
    (["spoken language"], lambda p, j: ", ".join(p.get("languages", []))),
]

# Timezone label keyword → profile timezone values that match (lowercase)
_TIMEZONE_MATCHES: list[tuple[str, list[str]]] = [
    ("utc −08", ["los_angeles", "pacific",
     "pst", "pdt", "pt", "utc-8", "utc-08"]),
    ("utc −07", ["denver", "mountain", "mst", "mdt", "utc-7"]),
    ("utc −06", ["chicago", "central", "cst", "cdt", "utc-6"]),
    ("utc −05", ["new_york", "eastern", "est", "edt", "et", "utc-5"]),
    ("utc −04", ["halifax", "atlantic", "ast", "utc-4"]),
    ("utc ±00", ["london", "gmt", "utc+0",
     "utc0", "utc±0", "dublin", "lisbon"]),
    ("utc +01", ["berlin", "paris", "warsaw",
     "cet", "utc+1", "amsterdam", "tunis"]),
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

# Gender value → candidate option texts (lowercase) in order of preference.
_GENDER_SYNONYMS: dict[str, list[str]] = {
    "male":   ["male", "man", "he/him", "he / him", "m"],
    "female": ["female", "woman", "she/her", "she / her", "f"],
    "other":  ["other", "non-binary", "nonbinary", "prefer not to say", "prefer not"],
}

# Demographic field synonyms: profile value → option substrings to look for.
# Each list is searched in order; first match wins.
_DISABILITY_SYNONYMS: dict[str, list[str]] = {
    "no":  ["no disability", "i don't have", "i do not have", "not disabled", "no, i"],
    "yes": ["i have a disability", "yes"],
}
_VETERAN_SYNONYMS: dict[str, list[str]] = {
    "no":  ["not a veteran", "i am not", "no, i", "i don't identify", "not applicable",
            "no veteran", "civilian", "no"],
    "yes": ["i am a veteran", "veteran", "yes"],
}
_ORIENTATION_SYNONYMS: dict[str, list[str]] = {
    # On Yes/No LGBTQ+ identity fields, "straight" maps to selecting "No".
    "straight":  ["heterosexual", "straight", "not lgbtq", "no, i do not", "no"],
    "gay":       ["gay", "homosexual", "same-sex", "yes"],
    "bisexual":  ["bisexual", "bi"],
    "other":     ["other", "prefer not"],
}
_RACE_SYNONYMS: dict[str, list[str]] = {
    "white":         ["white", "caucasian", "european"],
    "black":         ["black", "african american", "african-american"],
    "asian":         ["asian"],
    "hispanic":      ["hispanic", "latino", "latina"],
    "north african": ["north africa", "west asian", "arab", "middle east", "mena",
                      "middle eastern", "north african"],
    "other":         ["other", "multiracial", "two or more"],
}
_COLOUR_SYNONYMS: dict[str, list[str]] = {
    "no":  ["no, i do not", "no, i don't", "no"],
    "yes": ["yes, i do", "yes"],
}
_YES_NO_SYNONYMS: dict[str, list[str]] = {
    "yes": ["yes", "i am", "authorized", "entitled", "eligible", "i do"],
    "no":  ["no", "i am not", "not authorized", "not entitled", "not eligible"],
}
_REFERRAL_SYNONYMS: dict[str, list[str]] = {
    "internet search": ["internet", "online search", "search engine", "google", "web search", "job board"],
    "linkedin":        ["linkedin"],
    "indeed":          ["indeed"],
    "referral":        ["referral", "employee referral", "friend", "colleague"],
    "other":           ["other"],
}

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
    log_fn=None,
) -> list[dict]:
    """Fill all detected form fields based on the profile and job.

    Returns a list of action records:
        {"field": label, "type": type, "action": "filled"/"checked"/"skipped", "value": value}
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    actions: list[dict] = []
    # Track uploaded file paths to prevent the same file being uploaded twice
    # (e.g. resume PDF accidentally filling a mis-detected cover letter field).
    _uploaded_paths: set[str] = set()

    # Build DOM context for anonymous fields once (expensive JS call).
    context_map = await _build_context_map(page, fields)

    # Phone number: if the form has a separate country/country-code field, fill
    # only the local number in the phone field; otherwise use the full
    # international number (country code + local).
    _phone_bare = (profile.get("personal", {}).get("phone") or "").strip()
    _phone_cc   = str(profile.get("personal", {}).get("phone_country_code") or "").strip()
    _phone_intl = f"{_phone_cc} {_phone_bare}".strip() if _phone_cc else _phone_bare
    _has_country_field = any(
        any(kw in (f.get("label") or "").lower()
            for kw in ("country", "country code", "phone code", "dial code", "dialing code"))
        for f in fields
    )
    job = dict(job)  # shallow copy so we don't mutate the caller's dict
    job["_phone_value"] = _phone_bare if _has_country_field else _phone_intl

    # Identify freeform question fields and pre-generate LLM answers so
    # every question on the form gets a tailored, role-specific response.
    llm_answers: dict[int, str] = {}
    if not dry_run:
        from utils.form_answers import generate_answers, is_llm_question

        question_batch: list[tuple[int, str]] = []
        for i, f in enumerate(fields):
            ftype_i = f["type"]
            if ftype_i not in ("textarea", "text", "url"):
                continue
            lbl_i = _effective_label(f, context_map.get(i, ""))
            ctx_i = context_map.get(i, "")
            # Combine context + label for richer question text (e.g. "What
            # about n8n... Motivation").  Prefer the longer / more specific.
            question_text = ctx_i if len(ctx_i) > len(lbl_i) else lbl_i
            if len(question_text) < 12:
                continue
            lbl_check = (f"{ctx_i} {lbl_i}".strip() if ctx_i else lbl_i).lower()
            # For non-textarea fields: skip if a structured rule produces a value
            # (e.g. linkedin URL field, email, phone).  Textareas are always LLM
            # candidates — DOM context can bleed nearby labels (e.g. "linkedin")
            # into the textarea's effective label, causing wrong rule matches.
            if ftype_i != "textarea" and _resolve_text_value(lbl_check, profile, job):
                continue
            if is_llm_question(lbl_check, ftype_i):
                question_batch.append((i, question_text))

        if question_batch:
            _log(f"LLM working on {len(question_batch)} freeform question(s) — please wait"
                 " (this takes a few seconds): " +
                 ", ".join(f'"{q[:50]}"' for _, q in question_batch[:5]))
            llm_answers = await generate_answers(question_batch, job, profile)
            _log(f"LLM done — answers received for {len(question_batch)} question(s).")


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
        # Build a richer match string for value resolution by prepending the
        # DOM context (section header).  This handles ATSes like Ashby that
        # label URL inputs with just "(Link)" inside a "LinkedIn" section —
        # the combined string "LinkedIn (Link)" correctly hits the linkedin rule.
        ctx = context_map.get(idx, "")
        if ctx and ctx.lower() not in label.lower():
            label_lower = f"{ctx} {label}".lower()
        else:
            label_lower = label.lower()
        # If label is still empty, fall back to the field name attribute.
        # This handles ATSes like Comeet that have no <label> elements but
        # use descriptive name= values (firstName, lastName, linkedin, etc.)
        _fname_attr = field.get("name") or ""
        if not label_lower and _fname_attr:
            label_lower = _fname_attr.lower()
        # If the label is still too vague, also try the raw placeholder.
        ph_lower = (field.get("placeholder") or "").lower()
        ftype = field["type"]
        fname = field["name"]

        # ---- text-like inputs -----------------------------------------
        if ftype in ("text", "email", "tel", "number", "url", "textarea"):
            # For textareas: prefer LLM answer first.  DOM context can bleed
            # labels from adjacent fields (e.g. "linkedin"), causing rule-based
            # matching to return a LinkedIn URL for a freeform question.
            if ftype == "textarea" and idx in llm_answers:
                value = llm_answers[idx]
            else:
                # Belt-and-suspenders: when the field has a descriptive name
                # attribute (e.g. Lever's name="email", name="urls[LinkedIn]"),
                # try resolving by name first.  Label detection can return the
                # wrong label when DOM structure is unusual (e.g. Lever's shifted
                # field groups).  Name attributes are always reliable.
                _name_value = _resolve_text_value(_fname_attr.lower(), profile, job) if _fname_attr else ""
                value = _name_value or _resolve_text_value(label_lower, profile, job)
                # Fallback: match by placeholder when label gives nothing
                # (e.g. placeholder="https://www.linkedin.com/in/..." → linkedin rule).
                if not value and ph_lower and ph_lower not in _GENERIC_PLACEHOLDERS:
                    value = _resolve_text_value(ph_lower, profile, job)
                # Fallback: use LLM-generated answer for freeform question fields.
                if not value and idx in llm_answers:
                    value = llm_answers[idx]

            # Last resort for cover-letter-labelled textareas only.
            # Do NOT fall back to cover letter for arbitrary question textareas
            # (e.g. "How many years of experience with Golang?").
            if (not value and ftype == "textarea" and job.get("cover_letter")
                    and any(kw in label_lower for kw in ("cover letter", "cover_letter", "covering letter"))):
                value = job["cover_letter"]

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
                # ARIA combobox (react-select etc.): click + pick option.
                if field.get("role") == "combobox":
                    try:
                        chosen = await _select_combobox_option(page, field, value, label_lower, profile=profile, job=job)
                        if chosen:
                            actions.append({"field": label or f"anon-text-{idx}", "type": "combobox",
                                            "action": "selected", "value": chosen})
                        else:
                            actions.append({"field": label or f"anon-text-{idx}", "type": "combobox",
                                            "action": "skipped", "value": f"no option matched {value!r}"})
                    except Exception as e:
                        actions.append({"field": label or f"anon-text-{idx}", "type": "combobox",
                                        "action": "error", "value": str(e)})
                    continue

                try:
                    # Anonymous fields have no id/name/label to locate by;
                    # use placeholder + nth-of-type position instead.
                    if idx in anon_text_position:
                        el = await _locate_by_position(page, field, anon_text_position[idx])
                    else:
                        el = await _locate_field(page, field)
                    if el:
                        # Skip fields that already have a value — the user
                        # may have filled them manually or a previous pass ran.
                        existing = (await el.input_value()).strip()
                        if existing:
                            actions.append({"field": label or f"anon-text-{idx}", "type": ftype,
                                            "action": "skipped", "value": f"already filled: {existing[:40]}"})
                            continue
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
                _effective_label(f, context_map.get(
                    fields.index(f), "")).lower()
                for f in group_fields
            ]
            chosen = _pick_radio(option_labels, label_lower, profile, job)

            # Use first option's context (question text) as the group display name.
            group_display = context_map.get(idx, group_key) or group_key

            # LLM fallback for radio groups that no rule matched.
            if chosen is None and not dry_run and option_labels:
                try:
                    from utils.form_answers import pick_option as _pick_opt
                    _log(
                        f'LLM picking radio for "{(group_display or label_lower)[:60]}"')
                    chosen_text = await _pick_opt(
                        group_display or label_lower, option_labels, profile, job
                    )
                    if chosen_text:
                        chosen_lower = chosen_text.lower()
                        for i, opt in enumerate(option_labels):
                            if opt.lower() == chosen_lower or chosen_lower in opt.lower():
                                chosen = i
                                break
                except Exception:
                    pass

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

        # ---- yes/no button pairs (Ashby and similar) ------------------
        elif ftype == "yesno":
            answer = _resolve_text_value(label_lower, profile, job)
            if not answer:
                actions.append({"field": label, "type": "yesno",
                                "action": "skipped", "value": ""})
                continue
            btn_text = "Yes" if answer.strip().lower() == "yes" else "No"
            yesno_idx = field.get("_yesno_index", 0)
            if not dry_run:
                try:
                    await page.evaluate("""([idx, btnTxt]) => {
                        const btns = Array.from(document.querySelectorAll('button'));
                        const seen = new Set();
                        const groups = [];
                        for (const b of btns) {
                            if (b.innerText.trim().toLowerCase() !== 'yes') continue;
                            const p = b.parentElement;
                            if (!p || seen.has(p)) continue;
                            const hasNo = Array.from(p.children).some(
                                el => el.tagName === 'BUTTON' &&
                                      el.innerText.trim().toLowerCase() === 'no'
                            );
                            if (!hasNo) continue;
                            seen.add(p);
                            groups.push(p);
                        }
                        if (idx >= groups.length) return;
                        const target = Array.from(
                            groups[idx].querySelectorAll('button')
                        ).find(b => b.innerText.trim().toLowerCase() ===
                                    btnTxt.toLowerCase());
                        if (target) target.click();
                    }""", [yesno_idx, btn_text])
                except Exception as e:
                    actions.append({"field": label, "type": "yesno",
                                    "action": "error", "value": str(e)})
                    continue
            actions.append({"field": label, "type": "yesno",
                            "action": "clicked", "value": btn_text})

        # ---- select dropdowns -----------------------------------------
        # el.type on a <select> element returns "select-one" or "select-multiple",
        # not "select" — handle all three to avoid silently skipping native dropdowns.
        elif ftype in ("select", "select-one", "select-multiple"):
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
                                # Partial match or gender-synonym match.
                                # Fetch all options in one JS round-trip.
                                option_data = await el.evaluate(
                                    "el => Array.from(el.options).map(o => "
                                    "({t: o.text.trim(), v: o.value || o.text.trim()}))"
                                )
                                opt_texts = [o["t"] for o in option_data]
                                opt_vals = [o["v"] for o in option_data]
                                chosen_val = None
                                # 1. Simple partial match (e.g. "male" in "Male").
                                for t, v in zip(opt_texts, opt_vals):
                                    if value.lower() in t.lower():
                                        chosen_val = v
                                        break
                                # 2. Demographic synonym match (gender, disability, veteran, etc.)
                                _SELECT_DEMO_MAP = [
                                    (("gender", "sex"),
                                     _GENDER_SYNONYMS),
                                    (("disability",),
                                     _DISABILITY_SYNONYMS),
                                    (("veteran", "armed forces", "military"),
                                     _VETERAN_SYNONYMS),
                                    (("sexual orientation", "lgbtq", "lgbtqia",
                                     "2slgbtqia"), _ORIENTATION_SYNONYMS),
                                    (("race", "ethnicity"),
                                     _RACE_SYNONYMS),
                                    (("colour", "color", "person of colour",
                                     "person of color"), _COLOUR_SYNONYMS),
                                    (("hear about", "referral source",
                                     "how did you find"), _REFERRAL_SYNONYMS),
                                    (("legally entitled", "authorized to work", "eligible to work",
                                     "work authorization", "sponsorship"), _YES_NO_SYNONYMS),
                                ]
                                if not chosen_val:
                                    for kw_tuple, sdict in _SELECT_DEMO_MAP:
                                        if any(kw in label_lower for kw in kw_tuple):
                                            synonyms = sdict.get(
                                                value.lower(), [value.lower()])
                                            for syn in synonyms:
                                                for t, v in zip(opt_texts, opt_vals):
                                                    if syn in t.lower():
                                                        chosen_val = v
                                                        break
                                                if chosen_val:
                                                    break
                                            break
                                # 3. LLM fallback for unrecognised option text.
                                if not chosen_val:
                                    try:
                                        from utils.form_answers import pick_option as _pick_opt
                                        _log(
                                            f'LLM picking select option for "{label_lower[:60]}"')
                                        _llm_q = label_lower
                                        if any(kw in label_lower for kw in ("years", "experience", "how many", "how long")):
                                            _yrs = (profile.get("personal") or {}).get("years_experience")
                                            if _yrs is not None:
                                                _llm_q = f"{label_lower} (candidate has {_yrs}+ years of experience)"
                                        chosen_text = await _pick_opt(_llm_q, opt_texts, profile, job)
                                        if chosen_text:
                                            for t, v in zip(opt_texts, opt_vals):
                                                if t.lower() == chosen_text.lower():
                                                    chosen_val = v
                                                    break
                                    except Exception:
                                        pass
                                if chosen_val:
                                    await el.select_option(value=chosen_val)
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
            # Check label, id, and name — Greenhouse uses id='cover_letter'
            # with a generic label "Attach" so label alone isn't sufficient.
            _field_id_lower = (field.get("id") or "").lower()
            _field_name_lower = (field.get("name") or "").lower()
            is_cover_letter_field = any(
                kw in label_lower for kw in ("cover letter", "cover_letter", "covering letter")
            ) or any(
                kw in _field_id_lower for kw in ("cover_letter", "coverletter")
            ) or any(
                kw in _field_name_lower for kw in ("cover_letter", "coverletter")
            )
            # DOM ancestry fallback: when static attributes are inconclusive (e.g.
            # new Greenhouse board uses opaque IDs with no label association),
            # walk up the DOM from the file input checking for "cover letter" text
            # in each ancestor's own text nodes and preceding siblings.
            if not is_cover_letter_field and not dry_run:
                fid = field.get("id")
                if fid:
                    try:
                        is_cover_letter_field = await page.evaluate("""(fid) => {
                            const el = document.getElementById(fid);
                            if (!el) return false;
                            let node = el.parentElement;
                            for (let d = 0; d < 8; d++) {
                                if (!node) break;
                                const own = Array.from(node.childNodes)
                                    .filter(n => n.nodeType === Node.TEXT_NODE)
                                    .map(n => n.textContent).join('') +
                                    (node.getAttribute('aria-label') || '') +
                                    (node.getAttribute('data-label') || '');
                                if (/cover.?letter|covering.?letter/i.test(own))
                                    return true;
                                // Also check the heading/label in preceding siblings
                                let sib = node.previousElementSibling;
                                let s = 0;
                                while (sib && s < 3) {
                                    if (/cover.?letter|covering.?letter/i.test(
                                            sib.innerText || '')) return true;
                                    sib = sib.previousElementSibling; s++;
                                }
                                node = node.parentElement;
                            }
                            return false;
                        }""", fid)
                    except Exception:
                        pass
            if is_cover_letter_field:
                file_path = _resolve_cover_letter_path(profile, job)
                skip_reason = "(no cover letter text available)"
            else:
                file_path = _resolve_resume_path(profile, job)
                skip_reason = "(no resume path resolved)"

            if file_path and not dry_run:
                # Deduplicate: skip if this exact file was already uploaded in
                # this fill_form call (prevents resume PDF being uploaded twice
                # when cover-letter field detection fails).
                if file_path in _uploaded_paths:
                    _log(f"Skipping duplicate upload of {file_path} (already uploaded this session)")
                    actions.append({"field": label or fname, "type": "file",
                                    "action": "skipped", "value": f"duplicate: {file_path}",
                                    "is_cover_letter": is_cover_letter_field})
                    continue
                uploaded = False
                upload_err = ""
                fid = field.get("id") or ""
                # Strategy 1: click the visible sibling Attach button (Greenhouse +
                # most ATSes).  expect_file_chooser() safely intercepts the browser
                # file-chooser dialog before it opens, so no OS dialog is shown.
                # expect_file_chooser() is a Page method; when fill_target is a
                # cross-origin Frame (e.g. Greenhouse embed on instacart.careers),
                # we must use the parent Page to intercept the file chooser.
                _fc_page = getattr(page, "page", page)
                if fid and not uploaded:
                    try:
                        # Use Locator (not ElementHandle) so React re-renders
                        # between resume and cover-letter uploads don't cause staleness.
                        # "xpath=ancestor::div[1]//button" finds the upload button even
                        # when it is nested inside a sibling div (Ashby pattern) — unlike
                        # "xpath=../button" which only matches direct-child buttons.
                        btn_loc = page.locator(f'[id="{fid}"]').locator(
                            "xpath=ancestor::div[1]//button").first
                        if await btn_loc.count() > 0 and await btn_loc.is_visible():
                            async with _fc_page.expect_file_chooser(timeout=5000) as fc_info:
                                await btn_loc.click()
                            fc = await fc_info.value
                            await fc.set_files(file_path)
                            uploaded = True
                            if is_cover_letter_field:
                                _log("Cover letter: uploaded PDF via Attach button (Strategy 1)")
                    except Exception:
                        pass
                # Strategy 2: click a visible <label for="id"> (some ATSes).
                if fid and not uploaded:
                    try:
                        lbl = page.locator(f'label[for="{fid}"]:visible').first
                        if await lbl.count() > 0:
                            async with _fc_page.expect_file_chooser(timeout=5000) as fc_info:
                                await lbl.click()
                            fc = await fc_info.value
                            await fc.set_files(file_path)
                            uploaded = True
                            if is_cover_letter_field:
                                _log("Cover letter: uploaded PDF via label click (Strategy 2)")
                    except Exception:
                        pass
                # Strategy 3: direct set_input_files — works for standard visible
                # inputs and also hidden ones (bypasses visibility via getElementById).
                # After setting files, dispatch a native change event so React/Vue/
                # Angular state-managers pick up the new file (some frameworks ignore
                # the programmatic change without an explicit DOM event).
                if not uploaded:
                    try:
                        if fid:
                            el_loc = page.locator(f'[id="{fid}"]').first
                            if await el_loc.count() > 0:
                                if is_cover_letter_field:
                                    _log(
                                        "Cover letter: uploading PDF via set_input_files (Strategy 3)")
                                await el_loc.set_input_files(file_path)
                                # Dispatch change + input events so React registers the file.
                                try:
                                    await page.evaluate(
                                        "id => { const el = document.getElementById(id);"
                                        " if (el) {"
                                        "  el.dispatchEvent(new Event('input', {bubbles:true}));"
                                        "  el.dispatchEvent(new Event('change', {bubbles:true}));"
                                        " } }",
                                        fid,
                                    )
                                except Exception:
                                    pass
                                uploaded = True
                            else:
                                upload_err = "element not found"
                        else:
                            el = await _locate_field(page, field)
                            if el:
                                await el.set_input_files(file_path)
                                uploaded = True
                            else:
                                upload_err = "element not found"
                    except Exception as e:
                        upload_err = str(e)
                # Strategy 0 (cover letter fallback only): if all file-upload paths
                # failed, click "Enter manually" and fill the textarea.  This is the
                # last resort — file upload is always preferred.
                if is_cover_letter_field and not uploaded:
                    cl_text = ((job or {}).get("cover_letter") or "").strip()
                    if cl_text:
                        try:
                            # Find the "Enter manually" button for the cover-letter section.
                            # Greenhouse has two such buttons (resume + CL); we must pick the
                            # right one.  Try progressively broader selectors.
                            manual_btn = None
                            _cl_btn_candidates = [
                                page.locator(
                                    "[data-testid='cover_letter-text']").first,
                                page.locator(
                                    "button[data-testid*='cover_letter']").first,
                                # Ancestor traversal from the hidden #cover_letter input
                                page.locator("#cover_letter").locator(
                                    "xpath=ancestor::div[contains(@class,'file-upload__wrapper')]"
                                ).get_by_role("button", name=re.compile(r"enter.?manually", re.I)).first,
                                page.locator("#cover_letter").locator(
                                    "xpath=ancestor::div[contains(@class,'application-upload') "
                                    "or contains(@class,'upload-wrapper') "
                                    "or contains(@class,'file-upload')]"
                                ).get_by_role("button", name=re.compile(r"enter.?manually", re.I)).first,
                            ]
                            for _cand in _cl_btn_candidates:
                                try:
                                    if await _cand.count() > 0 and await _cand.is_visible(timeout=300):
                                        manual_btn = _cand
                                        break
                                except Exception:
                                    continue
                            # Broad fallback: take the LAST "Enter manually" button on the page
                            # (cover letter section always follows the resume section in Greenhouse).
                            if manual_btn is None:
                                _all_manual = page.get_by_role(
                                    "button", name=re.compile(r"enter.?manually", re.I)
                                )
                                _cnt = await _all_manual.count()
                                if _cnt > 0:
                                    _cand = _all_manual.nth(_cnt - 1)
                                    try:
                                        if await _cand.is_visible(timeout=300):
                                            manual_btn = _cand
                                    except Exception:
                                        pass
                            if manual_btn is not None and await manual_btn.is_visible():
                                _log(
                                    "Cover letter: file upload failed — falling back to 'Enter manually'")
                                await manual_btn.click()
                                # Wait specifically for the cover-letter textarea to appear.
                                # Do NOT use "textarea.last" here — other visible textareas
                                # (e.g. "Why do you want to work here?") would match
                                # immediately and the cover-letter textarea would be missed.
                                _cl_ta = page.locator(
                                    "textarea[id*='cover'], textarea[name*='cover']"
                                ).first
                                try:
                                    await _cl_ta.wait_for(state="visible", timeout=4000)
                                except Exception:
                                    await page.wait_for_timeout(1000)
                                # Try selectors in priority order
                                for ta_loc in [
                                    page.locator(
                                        "textarea[id='cover_letter_text']").first,
                                    page.locator(
                                        "textarea[name='cover_letter_text']").first,
                                    page.locator(
                                        "textarea[id*='cover']").first,
                                    page.locator(
                                        "textarea[name*='cover']").first,
                                ]:
                                    try:
                                        if await ta_loc.count() > 0 and await ta_loc.is_visible():
                                            await ta_loc.click()
                                            await ta_loc.fill(cl_text)
                                            uploaded = True
                                            _log(
                                                "Cover letter: filled via 'Enter manually' textarea (Strategy 0)")
                                            break
                                    except Exception:
                                        continue
                            else:
                                _log(
                                    "Cover letter: no upload path and no 'Enter manually' button found")
                        except Exception:
                            pass
                if uploaded:
                    _uploaded_paths.add(file_path)
                    # Brief pause so React can finish re-rendering after the
                    # upload before the next field (e.g. cover letter) is attempted.
                    await page.wait_for_timeout(600)
                    actions.append({"field": label or fname, "type": "file",
                                    "action": "uploaded", "value": file_path,
                                    "is_cover_letter": is_cover_letter_field})
                else:
                    actions.append({"field": label or fname, "type": "file",
                                    "action": "error", "value": upload_err or "upload failed",
                                    "is_cover_letter": is_cover_letter_field})
            elif file_path and dry_run:
                actions.append({"field": label or fname, "type": "file",
                                "action": "uploaded", "value": file_path})
            else:
                actions.append({"field": label or fname, "type": "file",
                                "action": "skipped", "value": skip_reason})

    return actions


async def try_upload_resume(
    page: Page,
    profile: dict,
    job: dict,
    dry_run: bool = False,
    log_fn=None,
) -> str:
    """Attempt to upload the resume via a custom file-picker button (e.g. Notion).

    Looks for any visible button/label whose text contains 'upload' near a
    'resume' heading.  Uses Playwright's expect_file_chooser() to intercept
    the native file dialog before it opens and set the file programmatically.

    Returns a status string: 'uploaded', 'skipped', or an error message.
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass
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
        "button:has-text('Attach')",          # Lever: "ATTACH RESUME/CV"
        "label:has-text('Attach')",
        "[role='button']:has-text('Attach')",
    ]

    _fc_page = getattr(page, "page", page)  # Frame → its parent Page
    for selector in upload_selectors:
        try:
            locator = page.locator(selector)
            count = await locator.count()
            for i in range(count):
                btn = locator.nth(i)
                if not await btn.is_visible(timeout=300):
                    continue

                # Skip buttons that sit inside a cover letter section.
                # Walk up to 8 ancestor levels and look for "cover letter" text
                # in the node's own text content (not its full subtree, to avoid
                # false positives where the resume section mentions cover letters).
                in_cl_section = await btn.evaluate("""el => {
                    let node = el.parentElement;
                    for (let d = 0; d < 8; d++) {
                        if (!node) break;
                        const own = Array.from(node.childNodes)
                            .filter(n => n.nodeType === Node.TEXT_NODE)
                            .map(n => n.textContent).join('') +
                            (node.getAttribute('aria-label') || '') +
                            (node.getAttribute('data-label') || '');
                        if (/cover.?letter|covering.?letter/i.test(own)) return true;
                        node = node.parentElement;
                    }
                    return false;
                }""")
                if in_cl_section:
                    _log(f"Resume upload: skipping '{selector}' #{i} (inside cover letter section)")
                    continue

                _log(f"Resume upload: clicking '{selector}' #{i}")
                async with _fc_page.expect_file_chooser(timeout=5000) as fc_info:
                    await btn.click()
                fc = await fc_info.value
                await fc.set_files(resume_path)
                _log(f"Resume upload: uploaded via button '{selector}' #{i}")
                return f"uploaded {resume_path}"
        except Exception as e:
            _log(f"Resume upload: '{selector}' failed — {e}")
            continue

    # Fallback: set_input_files directly on a hidden file input.
    # ATSes like Lever hide the <input type="file"> behind a styled button
    # (opacity:0), so it never appears in scanned fields and the button click
    # above may not intercept the chooser reliably.  Playwright's
    # set_input_files bypasses visibility and works on hidden inputs.
    # Prefer inputs whose name/accept hints suggest a resume (not cover letter).
    for file_sel in [
        "input[type='file'][name*='resume']",
        "input[type='file'][name*='cv']",
        "input[type='file'][accept*='pdf']:not([name*='cover'])",
        "input[type='file']:not([name*='cover']):not([id*='cover'])",
    ]:
        try:
            el = page.locator(file_sel).first
            if await el.count() > 0:
                _log(f"Resume upload: set_input_files via '{file_sel}'")
                await el.set_input_files(resume_path)
                _log(f"Resume upload: uploaded via hidden input '{file_sel}'")
                return f"uploaded {resume_path}"
        except Exception as e:
            _log(f"Resume upload: '{file_sel}' failed — {e}")
            continue

    _log("Resume upload: no button or file input found — skipped")
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


# Section headings that indicate a field is inside an employment/work history block.
# When the context-prepended label contains one of these AND the field label is a
# bare "name", it should resolve to the company name, not the candidate's name.
_EMPLOYMENT_SECTION_WORDS: frozenset[str] = frozenset({
    "employment", "employer", "work history", "work experience",
    "experience", "previous employer", "job history",
})

# Work-auth indicators — if any appear in a label, check for a country name.
_WA_LABEL_INDICATORS: frozenset[str] = frozenset({
    "authorized to work", "eligible to work", "legally entitled",
    "legally authorized", "work authorization",
})

# Countries and their regex aliases for label-country detection.
# Ordered longest-first so "united states" is matched before "us".
_LABEL_COUNTRY_PATTERNS: list[tuple[str, list[str]]] = [
    ("united states",  [r"united states", r"\busa\b", r"\bu\.s\.a\.?\b", r"\bu\.s\.\b"]),
    ("united kingdom", [r"united kingdom", r"\bu\.k\.?\b", r"\bgreat britain\b", r"\bbritain\b"]),
    ("new zealand",    [r"new zealand"]),
    ("south africa",   [r"south africa"]),
    ("canada",         [r"\bcanada\b"]),
    ("australia",      [r"\baustralia\b"]),
    ("ireland",        [r"\bireland\b"]),
    ("france",         [r"\bfrance\b"]),
    ("germany",        [r"\bgermany\b"]),
    ("netherlands",    [r"\bnetherlands\b", r"\bholland\b"]),
    ("spain",          [r"\bspain\b"]),
    ("italy",          [r"\bitaly\b"]),
    ("portugal",       [r"\bportugal\b"]),
    ("sweden",         [r"\bsweden\b"]),
    ("norway",         [r"\bnorway\b"]),
    ("denmark",        [r"\bdenmark\b"]),
    ("finland",        [r"\bfinland\b"]),
    ("switzerland",    [r"\bswitzerland\b"]),
    ("austria",        [r"\baustria\b"]),
    ("belgium",        [r"\bbelgium\b"]),
    ("poland",         [r"\bpoland\b"]),
    ("india",          [r"\bindia\b"]),
    ("singapore",      [r"\bsingapore\b"]),
    ("japan",          [r"\bjapan\b"]),
    ("brazil",         [r"\bbrazil\b"]),
    ("mexico",         [r"\bmexico\b"]),
    ("israel",         [r"\bisrael\b"]),
    ("argentina",      [r"\bargentina\b"]),
    ("tunisia",        [r"\btunisia\b"]),
    ("nigeria",        [r"\bnigeria\b"]),
    ("kenya",          [r"\bkenya\b"]),
]


def _resolve_text_value(label_lower: str, profile: dict, job: dict) -> str:
    """Match a normalised label against the text rules and return the value.

    Single-word keywords are matched as whole words to prevent spurious hits
    (e.g. "city" substring-matching inside "ethnicity").
    Multi-word keywords (e.g. "first name") are still matched as substrings.
    """
    # Pre-pass: if the label is a bare "name" inside an employment section context
    # (e.g. Greenhouse "Employment > Name"), return the current company rather than
    # the candidate's personal name.
    label_words_set = set(re.findall(r"\w+", label_lower))
    if label_words_set <= {"name", "company"} or label_lower.strip() in ("name", "company name"):
        if any(word in label_lower for word in _EMPLOYMENT_SECTION_WORDS):
            return profile.get("personal", {}).get("current_company", "") or ""

    # Pre-pass: if the label is a work-auth question that names a specific country,
    # check authorization for THAT country rather than the job's location.
    # This handles "Are you legally authorized to work in the United States?" → "no"
    # independently of the job's location field.
    if any(ind in label_lower for ind in _WA_LABEL_INDICATORS):
        wa = profile.get("work_authorization") or {}
        _META = {"sponsorship_required"}
        authorized = {k.lower() for k, v in wa.items() if v and k.lower() not in _META}
        for canonical, patterns in _LABEL_COUNTRY_PATTERNS:
            if any(re.search(pat, label_lower) for pat in patterns):
                return "yes" if canonical in authorized else "no"

    label_words = set(re.findall(r"\w+", label_lower))
    for keywords, getter in _TEXT_RULES:
        if all(
            (kw in label_lower if " " in kw else kw in label_words)
            for kw in keywords
        ):
            return getter(profile, job) or ""
    return ""


def _should_check(label_lower: str, profile: dict, job: dict) -> bool | None:
    """Decide whether to check a checkbox.

    Returns True to check, False to leave unchecked, None to skip entirely.
    """
    profile_skills = [s.lower() for s in (profile.get("skills") or [])]
    profile_keywords = [k.lower() for k in (profile.get("keywords") or [])]
    job_title = (job.get("title") or "").lower()
    job_title_words = set(re.findall(r"\w+", job_title)) - \
        {"sr", "jr", "the", "a", "an"}

    # Agreement / consent checkboxes — always check.
    # Normalize apostrophes before matching (curly vs straight).
    label_norm = label_lower.replace("\u2019", "'").replace("\u2018", "'")
    if any(kw in label_norm for kw in ["agree", "i've read", "confirm", "accept", "i understand", "consent"]):
        return True

    # Availability / engagement preferences from profile.
    availability = [a.lower() for a in (profile.get(
        "preferences", {}).get("availability") or [])]
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
        job_specific = job_title_words - _ROLE_WORDS
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
    _CREATIVE_WORDS = {"design", "pixel",
                       "illustrat", "artist", "3d", "animation"}
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
    profile_gender = (profile.get("personal", {}).get("gender") or "").lower()

    # Use whole-word matching for single-word keywords to avoid "sex" matching
    # inside "sexual orientation", "race" inside "embrace", etc.
    q_words = set(re.findall(r"\w+", question_label))

    def _syn_match(synonyms: list[str], option: str) -> bool:
        """Return True if any synonym matches the option (word-boundary aware)."""
        opt_lower = option.lower()
        opt_words = set(re.findall(r"\w+", opt_lower))
        return any(
            (s in opt_lower) if " " in s else (s in opt_words)
            for s in synonyms
        )

    # Gender / sex question (whole-word "gender" or "sex", NOT "sexual").
    if "gender" in q_words or "sex" in q_words:
        synonyms = _GENDER_SYNONYMS.get(profile_gender, [profile_gender])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

    # Sexual orientation — multi-word phrase check first so "sexual orientation"
    # wins over any single-word "sex" match (which is guarded above anyway).
    if "sexual orientation" in question_label or any(
        kw in q_words for kw in ("lgbtq", "lgbtqia", "2slgbtqia")
    ):
        val = (profile.get("personal", {}).get(
            "sexual_orientation") or "").lower()
        synonyms = _ORIENTATION_SYNONYMS.get(val, [val])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

    # Non-compete / NDA / restrictive covenant — always "no".
    _noncompete_triggers = (
        "non-compete", "noncompete", "non compete",
        "restrictive covenant", "non-solicitation",
    )
    if any(ph in question_label for ph in _noncompete_triggers):
        for i, opt in enumerate(option_labels):
            if opt.lower().strip() == "no":
                return i
        return None

    # Hispanic / Latino — a separate yes/no question distinct from race.
    # Answer "yes" only if profile race explicitly includes hispanic/latino synonyms.
    if "hispanic" in question_label or "latino" in question_label or "latina" in question_label:
        val = (profile.get("personal", {}).get("race") or "").lower()
        is_hispanic = any(s in val for s in ("hispanic", "latino", "latina"))
        target = "yes" if is_hispanic else "no"
        for i, opt in enumerate(option_labels):
            if target in opt.lower():
                return i
        return None

    # Race / ethnicity (whole-word to avoid "race" in "embrace").
    if any(kw in q_words for kw in ("race", "ethnicity", "ethnic")):
        val = (profile.get("personal", {}).get("race") or "").lower()
        synonyms = _RACE_SYNONYMS.get(val, [val])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

    # Disability.
    if "disability" in q_words or "disabled" in q_words:
        val = (profile.get("personal", {}).get("disability") or "").lower()
        synonyms = _DISABILITY_SYNONYMS.get(val, [val])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

    # Veteran / military.
    if any(kw in q_words for kw in ("veteran", "military")) or "armed forces" in question_label:
        val = (profile.get("personal", {}).get("veteran") or "").lower()
        synonyms = _VETERAN_SYNONYMS.get(val, [val])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

    # Person of colour.
    if any(kw in q_words for kw in ("colour", "color")) or "person of col" in question_label:
        val = (profile.get("personal", {}).get(
            "person_of_colour") or "").lower()
        synonyms = _COLOUR_SYNONYMS.get(val, [val])
        for i, opt in enumerate(option_labels):
            if _syn_match(synonyms, opt):
                return i
        return None

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
    """Look up the DOM for the nearest preceding text block for unlabelled fields.

    All DOM walking is done in a single JS round-trip:
    - Named/id'd fields: batch DOM-walk via getElementById / querySelector
    - Fully anonymous fields: TreeWalker pass in DOM order

    Radio/checkbox fields are skipped — their label IS the option text.
    Returns a dict mapping field-index → context string.
    """
    context_map: dict[int, str] = {}

    # --- Batch 1: fields with id or name but no label/placeholder -----------
    named_indices = [
        idx for idx, f in enumerate(fields)
        if not f.get("label") and not f.get("placeholder")
        and (f.get("id") or f.get("name"))
        and f.get("type") not in ("checkbox", "radio")
    ]
    if named_indices:
        descriptors = [
            {"id": fields[i].get("id") or "", "name": fields[i].get("name") or "",
             "type": fields[i].get("type") or "", "tag": fields[i].get("tag") or "input"}
            for i in named_indices
        ]
        try:
            results: list[str] = await page.evaluate(
                """(descriptors) => {
                    function getCtx(fid, fname, ftype, tag) {
                        let el = fid ? document.getElementById(fid) : null;
                        if (!el && fname && !['checkbox','radio'].includes(ftype))
                            el = document.querySelector(tag + '[name="' + fname + '"]');
                        if (!el) return '';
                        let node = el.parentElement;
                        for (let d = 0; d < 10; d++) {
                            if (!node) break;
                            let sib = node.previousElementSibling;
                            while (sib) {
                                const t = (sib.innerText || '').trim();
                                if (t.length > 2 && t.length < 300) return t;
                                sib = sib.previousElementSibling;
                            }
                            const direct = (node.firstChild &&
                                node.firstChild.textContent || '').trim();
                            if (direct.length > 2 && direct.length < 300 &&
                                direct !== (el.innerText || '').trim()) return direct;
                            node = node.parentElement;
                        }
                        return '';
                    }
                    return descriptors.map(f => getCtx(f.id, f.name, f.type, f.tag));
                }""",
                descriptors,
            )
            for pos, idx in enumerate(named_indices):
                if pos < len(results) and results[pos]:
                    context_map[idx] = results[pos]
        except Exception:
            pass

    # --- Batch 2: fully anonymous fields — TreeWalker in DOM order ----------
    anon_indices = [
        idx for idx, f in enumerate(fields)
        if not f.get("id") and not f.get("name") and not f.get("label")
        and f.get("type") not in ("checkbox", "radio")
    ]
    if anon_indices:
        placeholder = fields[anon_indices[0]].get(
            "placeholder") or "Your answer"
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
                    const results = new Array(inputs.length).fill('');
                    let lastText = '', nextIdx = 0;
                    const walker = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_ALL);
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

    # --- Batch 3: radio group question text -----------------------------------
    # Each radio option label is the option text (e.g. "Man"), not the question
    # ("Which option best describes your gender?").  For ATSes like Ashby the
    # question text lives in a <label for="question_uuid"> where the question
    # UUID is embedded in the radio's name attribute.  Fetch it once per group.
    # name → first field index in that group
    radio_groups: dict[str, int] = {}
    for idx, f in enumerate(fields):
        if f.get("type") != "radio":
            continue
        fname = f.get("name") or ""
        if not fname or fname in radio_groups:
            continue
        radio_groups[fname] = idx

    if radio_groups:
        group_names = list(radio_groups.keys())
        try:
            results: list[str] = await page.evaluate(
                """(names) => names.map(name => {
                    // Ashby: name = "{session_uuid}_{question_uuid}"
                    // Extract the last UUID-shaped segment (36 chars) as question id.
                    const qid = name.length >= 36 ? name.slice(-36) : name;
                    const lb = document.querySelector('label[for="' + qid + '"]');
                    if (lb) return lb.innerText.trim();
                    // Fallback: label pointing to the full name
                    const lb2 = document.querySelector('label[for="' + name + '"]');
                    return lb2 ? lb2.innerText.trim() : '';
                })""",
                group_names,
            )
            for i, gname in enumerate(group_names):
                question_text = results[i] if i < len(results) else ""
                if question_text:
                    for j, f in enumerate(fields):
                        if f.get("type") == "radio" and f.get("name") == gname:
                            if j not in context_map:
                                context_map[j] = question_text
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

    # For non-input comboboxes (button/div with role="combobox") located by
    # aria label association — used for Ashby EEO dropdowns and similar.
    if field.get("role") == "combobox" and field.get("tag", "input") not in ("input", "select"):
        label = field.get("label") or ""
        if label:
            try:
                el = await page.get_by_role("combobox", name=re.compile(
                    re.escape(label[:60]), re.I
                )).first.element_handle()
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
    job_words = set(re.findall(
        r"\w+", ((job or {}).get("title") or "").lower()))
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


def _resolve_cover_letter_path(profile: dict, job: dict) -> str:
    """Write the job's cover letter text to a temp PDF and return its path.

    Returns '' if no cover letter text is available on the job dict.
    The file is written to the system temp dir and reused across calls for
    the same company (filename is stable so Playwright can upload it).
    Falls back to DOCX if fpdf2 is not installed.
    """
    import os
    import tempfile

    text = ((job or {}).get("cover_letter") or "").strip()
    if not text:
        return ""

    company_slug = re.sub(
        r"[^\w-]", "_", ((job or {}).get("company") or "company"))

    # Prefer PDF (accepted by all major ATSes and has no macro-safety warnings).
    # Write via BytesIO to avoid Windows file-locking issues when the same
    # temp path was previously opened by Playwright's set_input_files.
    try:
        from fpdf import FPDF

        def _to_latin1(s: str) -> str:
            _MAP = str.maketrans({
                "\u2018": "'", "\u2019": "'",
                "\u201c": '"', "\u201d": '"',
                "\u2013": "-", "\u2014": "-",
                "\u2026": "...",
                "\u00a0": " ",
            })
            return s.translate(_MAP).encode("latin-1", errors="replace").decode("latin-1")

        fname = f"cover_letter_{company_slug}.pdf"
        path = os.path.join(tempfile.gettempdir(), fname)
        pdf = FPDF()
        pdf.set_margins(25, 25, 25)
        pdf.add_page()
        pdf.set_font("Helvetica", size=11)
        for para in text.split("\n\n"):
            para = para.strip()
            if not para:
                continue
            pdf.multi_cell(0, 6, _to_latin1(para))
            pdf.ln(3)
        # Write bytes → file (avoids re-opening a file already read by Playwright).
        pdf_bytes = pdf.output()
        with open(path, "wb") as fh:
            fh.write(pdf_bytes)
        return path
    except Exception as _exc:
        print(f"[cover_letter] PDF generation error: {_exc}", flush=True)

    # Fallback: DOCX (python-docx already in requirements).
    try:
        from docx import Document

        fname = f"cover_letter_{company_slug}.docx"
        path = os.path.join(tempfile.gettempdir(), fname)
        doc = Document()
        for para in text.split("\n\n"):
            para = para.strip()
            if para:
                doc.add_paragraph(para)
        doc.save(path)
        return path
    except Exception as _exc:
        print(f"[cover_letter] DOCX generation error: {_exc}", flush=True)
        return ""


async def _click_option(page: Page, opt: dict) -> None:
    """Click a combobox option by id (preferred) or by text fallback."""
    if opt.get("id"):
        await page.locator(f'[id="{opt["id"]}"]').first.click()
    else:
        await page.locator('[role="option"]').filter(has_text=opt["text"]).first.click()


async def _select_combobox_option(
    page: Page,
    field: dict,
    value: str,
    label_lower: str,
    profile: dict | None = None,
    job: dict | None = None,
) -> str | None:
    """Click an ARIA combobox (react-select etc.) and select the best matching option.

    Returns the selected option text on success, or None if no match found.
    """
    el = await _locate_field(page, field)
    if not el:
        return None

    await el.click()
    await asyncio.sleep(0.3)

    # Use aria-controls to scope the search to the right listbox.
    aria_controls = await el.get_attribute("aria-controls") or ""

    def _query_opts(listbox_id: str) -> str:
        return """(listboxId) => {
            const lb = listboxId ? document.getElementById(listboxId) : null;
            const root = lb || document;
            return Array.from(root.querySelectorAll('[role="option"]'))
                .filter(o => { const r = o.getBoundingClientRect();
                               return r.width >  0 && r.height > 0; })
                .map(o => ({text: o.innerText.trim(), id: o.id}));
        }"""

    opts: list[dict] = await page.evaluate(_query_opts(aria_controls), aria_controls)

    # For search/filter comboboxes (e.g. country dropdowns), options only
    # appear after typing.  Only try fill() on actual <input> elements —
    # <button>/<div> comboboxes don't support fill().
    is_input = field.get("tag", "input") == "input"
    if not opts and is_input:
        await el.fill(value)
        await asyncio.sleep(0.5)
        aria_controls = await el.get_attribute("aria-controls") or ""
        opts = await page.evaluate(_query_opts(aria_controls), aria_controls)

    # If typing the full value (e.g. "Tunis, Tunisia") produced no options,
    # retry with shorter alternatives so the dropdown has something to show:
    #   1. Country part only: "Tunisia" from "Tunis, Tunisia"
    #   2. City/first word: "Tunis"
    # This gives the LLM meaningful options to choose from.
    if not opts and is_input and ("," in value or " " in value):
        fallback_terms = []
        if "," in value:
            fallback_terms.append(value.split(",")[-1].strip())   # country
            fallback_terms.append(value.split(",")[0].strip())    # city
        else:
            fallback_terms.append(value.split()[0].strip())
        for term in fallback_terms:
            if not term:
                continue
            await el.fill(term)
            await asyncio.sleep(0.5)
            aria_controls = await el.get_attribute("aria-controls") or ""
            opts = await page.evaluate(_query_opts(aria_controls), aria_controls)
            if opts:
                break

    if not opts:
        await el.press("Escape")
        return None

    val_lower = value.lower()
    opt_texts_lower = [ o["text"].lower() for o in opts]

    # 1. Exact case-insensitive match.
    for i, t in enumerate(opt_texts_lower):
        if t == val_lower:
            await _click_option(page, opts[i])
            return opts[i]["text"]

    # 2. Demographic synonym match using label-specific dictionaries.
    _DEMO_SYNONYMS: dict[str, tuple[str, ...]] = {
        # type: ignore[dict-item]
        ("gender", "sex"):               (_GENDER_SYNONYMS,),
        # type: ignore[dict-item]
        ("disability",):                 (_DISABILITY_SYNONYMS,),
        # type: ignore[dict-item]
        ("veteran", "armed forces", "military"): (_VETERAN_SYNONYMS,),
        # type: ignore[dict-item]
        ("sexual orientation", "lgbtq", "lgbtqia", "2slgbtqia"): (_ORIENTATION_SYNONYMS,),
        # type: ignore[dict-item]
        ("race", "ethnicity", "ethnic"): (_RACE_SYNONYMS,),
        # type: ignore[dict-item]
        ("colour", "color", "person of colour", "person of color"): (_COLOUR_SYNONYMS,),
        # type: ignore[dict-item]
        ("hear about", "how did you find", "referral source", "referral"): (_REFERRAL_SYNONYMS,),
        # type: ignore[dict-item]
        ("legally entitled", "authorized to work", "eligible to work", "work authorization", "sponsorship"): (_YES_NO_SYNONYMS,),
    }
    synonym_dict = None
    for label_keys, (sdict,) in _DEMO_SYNONYMS.items():
        if any(kw in label_lower for kw in label_keys):
            synonym_dict = sdict
            break

    if synonym_dict is not None:
        synonyms = synonym_dict.get(val_lower, [val_lower]) if isinstance(synonym_dict, dict) else [val_lower]
        for i, t in enumerate(opt_texts_lower):
            opt_words = set(re.findall(r"\w+", t))
            # Each synonym entry can be either a whole-word check or a phrase match.
            if t in synonyms or any(
                (s in t) if " " in s else (s in opt_words)
                for s in synonyms
            ):
                await _click_option(page, opts[i])
                return opts[i]["text"]

    # 3. Substring match (value contained in option or vice versa).
    for i, t in enumerate(opt_texts_lower):
        if val_lower in t or t in val_lower:
            await _click_option(page, opts[i])
            return opts[i]["text"]

    # 4. LLM fallback — when no synonym/substring rule matches, ask the model.
    if profile is not None:
        try:
            from utils.form_answers import pick_option as _pick_opt
            opt_texts = [o["text"] for o in opts]
            # For location/country questions, append the candidate's location so
            # the LLM has enough context to pick the right option (e.g. "Tunisia"
            # from a region list like "Africa", "EMEA", etc.).
            llm_question = label_lower
            if any(kw in label_lower for kw in ("location", "where", "country", "region", "based")):
                loc = (profile.get("personal") or {}).get("location", "")
                if loc:
                    llm_question = f"{label_lower} (candidate location: {loc})"
            elif any(kw in label_lower for kw in ("years", "experience", "how many", "how long")):
                yrs = (profile.get("personal") or {}).get("years_experience")
                if yrs is not None:
                    llm_question = f"{label_lower} (candidate has {yrs}+ years of experience)"
            chosen_text = await _pick_opt(llm_question, opt_texts, profile, job or {})
            if chosen_text:
                chosen_lower = chosen_text.lower()
                for i, o in enumerate(opts):
                    if o["text"].lower() == chosen_lower:
                        await _click_option(page, opts[i])
                        return opts[i]["text"]
        except Exception:
            pass

    await el.press("Escape")
    return None


def _auth_for_job_country(profile: dict, job: dict) -> str:
    """Answer work-authorization questions that reference the job's country.

    Called for labels like "Are you legally authorized to work in the country
    in which this role is located?".  Extracts the country from the job
    location and cross-references it against work_authorization in the profile.

    Logic:
    - Worldwide / global / no specific country → "yes" (no restriction).
    - Location contains an explicitly authorized country → "yes".
    - Otherwise → "no" (visa / sponsorship required).
    """
    location = (
        job.get("location") or job.get("raw_location_text") or ""
    ).lower()
    wa = profile.get("work_authorization") or {}

    # No country-specific restriction → safe to say yes.
    # "remote" without a restricting qualifier (e.g. "us only") means the work
    # is done from the candidate's own country, so authorization is always yes.
    if not location or any(
        w in location for w in ("worldwide", "anywhere", "global", "remote only", "remote")
    ):
        return "yes"

    # Build the set of countries the candidate is authorized to work in.
    # Each truthy key in work_authorization (except meta-keys) is a country.
    _META = {"sponsorship_required"}
    authorized = {k.lower() for k, v in wa.items() if v and k.lower() not in _META}

    if any(country in location for country in authorized):
        return "yes"

    # Country found in location but not in authorized list → needs sponsorship.
    return "no"


def _age_range(profile: dict) -> str:
    """Return the standard age-bracket string for the person's age (e.g. '45-54')."""
    age = profile.get("personal", {}).get("age")
    if age is None:
        return ""
    try:
        age = int(age)
    except (TypeError, ValueError):
        return ""
    _BRACKETS = [
        (18, 24, "18-24"), (25, 34, "25-34"), (35, 44, "35-44"),
        (45, 54, "45-54"), (55, 64, "55-64"), (65, 74, "65-74"),
    ]
    for lo, hi, label in _BRACKETS:
        if lo <= age <= hi:
            return label
    return "75 or older" if age >= 75 else ""


def _years_label(profile: dict) -> str:
    """Return a years-of-experience string from the profile.

    Prefers the explicit ``personal.years_experience`` integer (e.g. 20),
    then falls back to scanning summary lines for a years-containing sentence.
    """
    yrs = (profile.get("personal") or {}).get("years_experience")
    if yrs is not None:
        try:
            return str(int(yrs))
        except (TypeError, ValueError):
            return str(yrs).strip()
    summary = profile.get("summary") or []
    for line in summary:
        if "year" in str(line).lower():
            return str(line)
    return ""
