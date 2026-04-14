"""
Playwright-based form prefill utility for the web UI.

Called from ui/app.py when the user clicks "Open & Prefill".
"""
from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, Set
from urllib.parse import urlparse, urlunparse

from playwright.async_api import async_playwright  # noqa: F401 — imported for patching
from utils.ats_detector import MANUAL_ONLY_ATS, detect_ats
from utils.form_filler import fill_form, try_upload_resume
from utils.form_inspector import extract_apply_url, scan_fields, try_click_apply

# Domains where Playwright is blocked by bot detection.
SYSTEM_BROWSER_DOMAINS = {
    "remoteok.com",
    "weworkremotely.com",
    "jobicy.com",
    "getonbrd.com",
}


def is_system_browser_domain(url: str) -> bool:
    """Return True if the URL belongs to a bot-protected domain."""
    url_lower = (url or "").lower()
    return any(domain in url_lower for domain in SYSTEM_BROWSER_DOMAINS)


def _ashby_application_url(url: str) -> str:
    """Return the Ashby /application URL for a given job listing URL.

    Inserts '/application' into the URL path before the query string so
    that query parameters (UTM tags etc.) are preserved correctly.
    """
    parsed = urlparse(url)
    new_path = parsed.path.rstrip("/") + "/application"
    return urlunparse(parsed._replace(path=new_path))


def _page_key(url: str) -> str:
    """Normalised URL used as a dedup key — strips query string."""
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
    except Exception:
        return url.split("?")[0].rstrip("/")


async def run_prefill_session(
    job: Dict[str, Any],
    profile: Dict[str, Any],
    headless: bool = False,
    wait_timeout: float = 3600,
    cancel_event=None,
    log_fn=None,
) -> Dict[str, Any]:
    """
    Open a job URL in Playwright, navigate to the application form,
    fill fields from profile, and upload resume.

    Keeps the browser open until the user closes it (or wait_timeout seconds).

    The new-tab handler is registered BEFORE any navigation so that even
    if the user clicks "Apply" while the automation is still waiting on
    the listing page, the resulting Ashby tab is caught and filled.
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    url = (job.get("url") or "").strip()
    if not url:
        return {"status": "failed", "error": "No URL for this job"}

    if is_system_browser_domain(url):
        return {
            "status": "manual",
            "reason": "Site blocks automated browsers — open in system browser",
        }

    # Determine whether this job needs a persisted browser session.
    # euremotejobs jobs redirect to euremote.jobcopilot.com for login.
    # flexa.careers requires LinkedIn OAuth authentication.
    if job.get("source") == "flexa" or "flexa.careers" in url:
        _SESSION_DOMAIN: str | None = "flexa.careers"
        _needs_session = True
    elif job.get("source") == "euremotejobs" or "jobcopilot.com" in url:
        _SESSION_DOMAIN = "jobcopilot.com"
        _needs_session = True
    else:
        _SESSION_DOMAIN = None
        _needs_session = False

    from utils.site_login import session_state_path, save_session_state
    _saved_state = session_state_path(_SESSION_DOMAIN) if _needs_session and _SESSION_DOMAIN else None

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            _ctx_kwargs: dict = {}
            if _saved_state:
                _ctx_kwargs["storage_state"] = _saved_state
                _log(f"Restoring saved {_SESSION_DOMAIN} session…")
            context = await browser.new_context(**_ctx_kwargs)
            page = await context.new_page()

            result: Dict[str, Any] = {"ats": "unknown", "filled": 0, "skipped": 0, "errors": 0}
            filled_urls: Set[str] = set()   # dedup by URL path, not ATS name
            closed_event = asyncio.Event()
            loop = asyncio.get_event_loop()

            def _cancelled() -> bool:
                return cancel_event is not None and cancel_event.is_set()

            # ----------------------------------------------------------------
            # Register the new-tab handler BEFORE navigation so we never miss
            # a tab the user opens while the automation is busy waiting.
            # ----------------------------------------------------------------
            async def _fill_new_tab(new_page) -> None:
                try:
                    await new_page.wait_for_load_state("load", timeout=20000)
                    await _wait_for_spa(new_page)
                    tab_ats = detect_ats(new_page.url)
                    if tab_ats in MANUAL_ONLY_ATS or tab_ats == "unknown":
                        return
                    key = _page_key(new_page.url)
                    if key in filled_urls:
                        return
                    result["ats"] = tab_ats
                    await _do_fill(new_page, profile, job, result, log_fn=_log)
                    filled_urls.add(key)
                    new_page.on("close", lambda: closed_event.set())
                except Exception:
                    pass

            context.on("page", lambda p: loop.create_task(_fill_new_tab(p)))

            # ----------------------------------------------------------------
            # Navigate and attempt to reach the ATS application form.
            # ----------------------------------------------------------------
            _log(f"Loading {url} …")
            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await _wait_for_spa(page)
            except Exception as exc:
                await browser.close()
                return {"status": "failed", "error": f"Page load failed: {exc}"}

            # Resolve direct apply URL from listing aggregators.
            # Skip if already on a known ATS page or if a child frame is already
            # an ATS embed (e.g. Greenhouse on employer career sites).
            # Retry once (with a short wait) only when truly unknown.
            try:
                resolved = None
                if (not _cancelled()
                        and detect_ats(page.url) == "unknown"
                        and _frame_ats(page) is None):
                    _log("Listing page detected — looking for apply link…")
                    resolved = await extract_apply_url(page)
                    if not resolved:
                        await page.wait_for_timeout(1000)
                        resolved = await extract_apply_url(page)
                if resolved and resolved != url:
                    _log(f"Following apply link → {resolved[:80]}")
                    await page.goto(resolved, wait_until="load", timeout=30000)
                    await _wait_for_spa(page)
                elif detect_ats(page.url) == "unknown" and _frame_ats(page) is None:
                    _log("No direct apply link found — trying Apply button…")
            except Exception:
                pass

            # Click through to the application form.
            active_page = page
            try:
                clicked, active_page = await try_click_apply(active_page)
                if clicked:
                    _log(f"Clicked Apply → {active_page.url[:80]}")
                    await _wait_for_spa(active_page)
                    # Dismiss site-specific interstitial modals (e.g. Himalayas "I'm ready to apply").
                    from utils.site_login import dismiss_himalayas_modal
                    await dismiss_himalayas_modal(active_page)
            except Exception:
                pass

            # Auto-login for known sites that gate Apply behind authentication.
            # Called unconditionally — some handlers (jobcopilot Google OAuth)
            # are interactive and don't require stored credentials.
            from utils.site_login import try_site_login
            try:
                credentials = profile.get("credentials", {})
                logged_in = await try_site_login(active_page, active_page.url, credentials, _log)
                if logged_in:
                    await _wait_for_spa(active_page)
                    # After login, try clicking Apply again on the redirected page.
                    try:
                        clicked2, active_page = await try_click_apply(active_page)
                        if clicked2:
                            _log(f"Clicked Apply post-login → {active_page.url[:80]}")
                            await _wait_for_spa(active_page)
                            from utils.site_login import dismiss_himalayas_modal
                            await dismiss_himalayas_modal(active_page)
                    except Exception:
                        pass
            except Exception:
                pass

            # Ashby shortcut: if on a listing page, go to /application directly.
            # Uses _ashby_application_url to correctly insert /application into
            # the path BEFORE any query string (UTM params etc.).
            ats = detect_ats(active_page.url)
            if ats == "ashby" and "/application" not in active_page.url:
                try:
                    app_url = _ashby_application_url(active_page.url)
                    await active_page.goto(app_url, wait_until="load", timeout=20000)
                    await _wait_for_spa(active_page)
                except Exception:
                    pass
                ats = detect_ats(active_page.url)

            # Workable shortcut: listing page is apply.workable.com/{co}/j/{id}
            # and the form is at apply.workable.com/{co}/j/{id}/apply/
            if ats == "workable" and "/apply" not in active_page.url:
                try:
                    parsed = urlparse(active_page.url)
                    apply_path = parsed.path.rstrip("/") + "/apply/"
                    apply_url = urlunparse(parsed._replace(path=apply_path, query=""))
                    _log(f"Workable shortcut → {apply_url}")
                    await active_page.goto(apply_url, wait_until="load", timeout=20000)
                    await _wait_for_spa(active_page)
                except Exception:
                    pass
                ats = detect_ats(active_page.url)

            # Personio shortcut: job listing page at jobs.personio.com/job/{id}
            # has an "Apply for this job" button that reveals the inline form.
            # Unlike Ashby/Workable there is no separate /apply URL — clicking
            # the button expands the form on the same page.
            if ats == "personio":
                try:
                    btn = active_page.locator(
                        "button:has-text('Apply for this job'), "
                        "a:has-text('Apply for this job'), "
                        "button:has-text('Apply for this position'), "
                        "a:has-text('Apply for this position')"
                    ).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=2000):
                        _log("Personio: clicking 'Apply for this job'…")
                        await btn.click()
                        await _wait_for_spa(active_page)
                except Exception:
                    pass

            # Fill whichever page we ended up on if it's an ATS form.
            # Also fill when the page URL is "unknown" but a child frame belongs
            # to a known ATS (e.g. Greenhouse embedded on employer career sites).
            if not _cancelled():
                frame_ats = _frame_ats(active_page)
                if ats in MANUAL_ONLY_ATS:
                    _log(f"ATS '{ats}' requires manual application — fill in browser.")
                elif ats != "unknown" or frame_ats not in (None, "unknown"):
                    if ats == "unknown":
                        ats = frame_ats or "unknown"
                    result["ats"] = ats
                    key = _page_key(active_page.url)
                    if key in filled_urls:
                        _log(f"ATS detected: {ats} — already filled by tab handler, skipping.")
                    else:
                        _log(f"ATS detected: {ats} — filling form…")
                        filled_urls.add(key)
                        await _do_fill(active_page, profile, job, result, log_fn=_log)
                else:
                    _log("No application form detected — browser is open, navigate to the form manually.")

            # ----------------------------------------------------------------
            # Keep browser open.  The polling watcher handles same-tab
            # navigation; new tabs are handled by the context handler above.
            # Cancel event also closes the browser immediately.
            # ----------------------------------------------------------------
            active_page.on("close", lambda: closed_event.set())
            browser.on("disconnected", lambda: closed_event.set())

            if _cancelled():
                closed_event.set()

            watch_task = asyncio.create_task(
                _watch_for_ats_and_fill(active_page, profile, job, result, closed_event, filled_urls, log_fn=_log)
            )

            security_task = asyncio.create_task(
                _watch_for_security_code(context, profile, closed_event, log_fn=_log)
            )

            # Poll cancel_event every 2s so a stop request closes the browser quickly.
            async def _poll_cancel():
                while not closed_event.is_set():
                    await asyncio.sleep(2)
                    if _cancelled():
                        closed_event.set()

            cancel_task = asyncio.create_task(_poll_cancel())

            try:
                await asyncio.wait_for(closed_event.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                pass

            watch_task.cancel()
            security_task.cancel()
            cancel_task.cancel()
            for t in (watch_task, security_task, cancel_task):
                try:
                    await asyncio.wait_for(t, timeout=2)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

            # Persist session state so the next visit skips login.
            if _needs_session and _SESSION_DOMAIN:
                try:
                    await save_session_state(context, _SESSION_DOMAIN)
                    _log(f"{_SESSION_DOMAIN} session saved.")
                except Exception:
                    pass

            try:
                await browser.close()
            except Exception:
                pass

            result["status"] = "cancelled" if _cancelled() else "ok"
            return result

    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


_ITI_COUNTRY_MAP: dict[str, str] = {
    "tunisia": "tn",
    "canada": "ca",
    "united states": "us",
    "united kingdom": "gb",
    "uk": "gb",
    "germany": "de",
    "france": "fr",
    "australia": "au",
    "india": "in",
    "brazil": "br",
    "netherlands": "nl",
    "spain": "es",
    "italy": "it",
    "poland": "pl",
    "portugal": "pt",
    "sweden": "se",
    "norway": "no",
    "denmark": "dk",
    "finland": "fi",
    "switzerland": "ch",
    "austria": "at",
    "belgium": "be",
}


async def _set_iti_phone_country(page, profile: Dict[str, Any], log_fn=None) -> None:
    """Set the intl-tel-input phone country on any phone fields.

    Greenhouse job-boards and similar ATSes use intl-tel-input (iti) which renders
    a flag button for country selection rather than a standard <select>/<input>.
    Tries the iti JS API first; falls back to clicking the flag button and
    selecting from the visible country dropdown.
    """
    phone_country = str((profile.get("personal") or {}).get("phone_country") or "").strip().lower()
    iso2 = _ITI_COUNTRY_MAP.get(phone_country, "")
    if not iso2:
        return
    try:
        # Strategy 1: JS API (works when intlTelInputGlobals is present).
        count = await page.evaluate("""(iso2) => {
            if (!window.intlTelInputGlobals) return 0;
            const inputs = document.querySelectorAll('input[type="tel"]');
            let set = 0;
            for (const inp of inputs) {
                const iti = intlTelInputGlobals.getInstance(inp);
                if (iti) { iti.setCountry(iso2); set++; }
            }
            return set;
        }""", iso2)
        if count:
            if log_fn:
                log_fn(f"Phone country set to {iso2.upper()} via iti API.")
            return

        # Strategy 2: click the flag button, then click the matching country item.
        flag_btn = page.locator(
            ".iti__selected-flag, .iti__flag-container button, "
            "button[class*='iti__selected'], div[class*='iti__selected-flag']"
        ).first
        if await flag_btn.count() == 0 or not await flag_btn.is_visible(timeout=1000):
            return
        await flag_btn.click()
        await asyncio.sleep(0.4)
        # Country items have data-country-code or data-dial-code attribute.
        country_item = page.locator(
            f"[data-country-code='{iso2}'], li.iti__country[data-country-code='{iso2}']"
        ).first
        if await country_item.count() > 0 and await country_item.is_visible(timeout=1000):
            await country_item.scroll_into_view_if_needed()
            await country_item.click()
            if log_fn:
                log_fn(f"Phone country set to {iso2.upper()} via dropdown click.")
        else:
            await page.keyboard.press("Escape")
    except Exception:
        pass


async def _fill_employment_history(page, profile: Dict[str, Any], log_fn=None) -> None:
    """Fill repeating employment history groups using the work_history from the profile.

    Many ATS forms (Greenhouse, Lever) render a single employment group with an
    "Add another" or "Add employment" button.  fill_form fills the first group
    (the current/most-recent role) — this function clicks "Add another" for each
    additional work history entry and fills the revealed fields.
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    work_history = profile.get("work_history") or []
    # First entry is already filled by fill_form; fill entries 2 onward.
    extra_entries = work_history[1:]
    if not extra_entries:
        return

    # Selectors for the "Add another" button in employment sections.
    add_btn_selectors = [
        "button:has-text('Add another')",
        "button:has-text('Add Another')",
        "button:has-text('Add employment')",
        "button:has-text('Add Employment')",
        "button:has-text('Add position')",
        "button:has-text('+ Add')",
        "a:has-text('Add another')",
        "a:has-text('Add employment')",
    ]

    # Field label patterns within each employment group.
    _COMPANY_LABELS = re.compile(r"company|employer|organization", re.I)
    _TITLE_LABELS   = re.compile(r"title|position|role", re.I)
    _START_LABELS   = re.compile(r"start", re.I)
    _END_LABELS     = re.compile(r"end|to\b", re.I)

    for entry in extra_entries:
        company  = str(entry.get("company") or "").strip()
        title    = str(entry.get("title") or "").strip()
        date_from = str(entry.get("from") or "").strip()
        date_to   = str(entry.get("to") or "").strip()

        # Find and click the "Add another" button.
        clicked = False
        for sel in add_btn_selectors:
            try:
                btn = page.locator(sel).last
                if await btn.count() > 0 and await btn.is_visible(timeout=500):
                    await btn.click()
                    await page.wait_for_timeout(1200)
                    clicked = True
                    _log(f"Employment history: clicked 'Add another' for {company}")
                    break
            except Exception:
                continue

        if not clicked:
            _log("Employment history: 'Add another' button not found — stopping after first entry")
            break

        # After clicking, new input fields appear at the bottom of the group list.
        # Uses a single JS call to collect label/placeholder/name for recent
        # visible inputs *and* select elements, then fills by id/name.

        _JS_COLLECT = """(maxBack) => {
            function getLabel(el) {
                if (el.id) {
                    const lbl = document.querySelector('label[for="' + el.id + '"]');
                    if (lbl) return lbl.innerText || '';
                }
                let node = el.parentElement;
                for (let d = 0; d < 5; d++) {
                    if (!node) break;
                    if (node.tagName === 'LABEL') return node.innerText || '';
                    const sib = node.previousElementSibling;
                    if (sib && sib.tagName === 'LABEL') return sib.innerText || '';
                    node = node.parentElement;
                }
                return '';
            }
            const inputSel = "input[type='text'], input:not([type]), " +
                             "input[type='month'], input[type='date']";
            const selectSel = "select";
            function visible(el) {
                const r = el.getBoundingClientRect();
                if (r.width === 0 && r.height === 0) return false;
                const s = window.getComputedStyle(el);
                return s.display !== 'none' && s.visibility !== 'hidden';
            }
            const inputs  = Array.from(document.querySelectorAll(inputSel)).filter(visible);
            const selects = Array.from(document.querySelectorAll(selectSel)).filter(visible);
            const inputSlice  = inputs.slice(Math.max(0, inputs.length - maxBack)).reverse();
            const selectSlice = selects.slice(Math.max(0, selects.length - maxBack)).reverse();
            const map = el => ({
                tag: el.tagName.toLowerCase(),
                id: el.id || '',
                name: el.name || '',
                placeholder: el.placeholder || '',
                ariaLabel: el.getAttribute('aria-label') || '',
                labelText: getLabel(el).trim(),
                options: el.tagName === 'SELECT'
                    ? Array.from(el.options).map(o => o.text.trim())
                    : [],
            });
            return { inputs: inputSlice.map(map), selects: selectSlice.map(map) };
        }"""

        async def _fill_last(pattern: re.Pattern, value: str) -> bool:
            """Fill the last visible text input matching *pattern* with *value*."""
            if not value:
                return False
            try:
                data = await page.evaluate(_JS_COLLECT, 20)
                for cand in data.get("inputs", []):
                    hint = " ".join(filter(None, [
                        cand.get("placeholder", ""),
                        cand.get("ariaLabel", ""),
                        cand.get("labelText", ""),
                        cand.get("name", ""),
                    ]))
                    if not pattern.search(hint):
                        continue
                    cand_id   = cand.get("id", "")
                    cand_name = cand.get("name", "")
                    if cand_id:
                        el = page.locator(f"#{cand_id}").first
                    elif cand_name:
                        el = page.locator(f"[name='{cand_name}']").last
                    else:
                        continue
                    try:
                        await el.fill(value)
                        return True
                    except Exception:
                        continue
            except Exception:
                pass
            return False

        async def _fill_select_last(pattern: re.Pattern, value: str) -> bool:
            """Select an option in the last visible <select> matching *pattern*."""
            if not value:
                return False
            val_lower = value.lower()
            try:
                data = await page.evaluate(_JS_COLLECT, 20)
                for cand in data.get("selects", []):
                    hint = " ".join(filter(None, [
                        cand.get("ariaLabel", ""),
                        cand.get("labelText", ""),
                        cand.get("name", ""),
                    ]))
                    if not pattern.search(hint):
                        continue
                    options = cand.get("options", [])
                    # Find best option: exact → starts-with → contains.
                    chosen = next(
                        (opt for opt in options if opt.lower() == val_lower), None
                    )
                    if not chosen:
                        chosen = next(
                            (opt for opt in options if opt.lower().startswith(val_lower[:3])),
                            None,
                        )
                    if not chosen:
                        chosen = next(
                            (opt for opt in options if val_lower in opt.lower()), None
                        )
                    if not chosen:
                        continue
                    cand_id   = cand.get("id", "")
                    cand_name = cand.get("name", "")
                    if cand_id:
                        el = page.locator(f"#{cand_id}").first
                    elif cand_name:
                        el = page.locator(f"[name='{cand_name}']").last
                    else:
                        continue
                    try:
                        await el.select_option(label=chosen)
                        return True
                    except Exception:
                        try:
                            await el.select_option(value=chosen)
                            return True
                        except Exception:
                            continue
            except Exception:
                pass
            return False

        # Parse "Feb 2022" → month abbreviation + 4-digit year.
        def _parse_date(date_str: str):
            parts = date_str.split()
            month = parts[0] if len(parts) >= 1 else ""
            year  = parts[1] if len(parts) >= 2 else (parts[0] if len(parts) >= 1 and parts[0].isdigit() else "")
            return month, year

        from_month, from_year = _parse_date(date_from)
        to_month,   to_year   = _parse_date(date_to if date_to != "present" else "")

        _START_MONTH_LABELS = re.compile(r"start.*(month|date)|start\s*date.*month", re.I)
        _START_YEAR_LABELS  = re.compile(r"start.*(year)|start\s*date.*year", re.I)
        _END_MONTH_LABELS   = re.compile(r"end.*(month|date)|end\s*date.*month", re.I)
        _END_YEAR_LABELS    = re.compile(r"end.*(year)|end\s*date.*year", re.I)

        filled_company = await _fill_last(_COMPANY_LABELS, company)
        filled_title   = await _fill_last(_TITLE_LABELS, title)

        # Start date: try month SELECT first, then year text input.
        filled_start_month = await _fill_select_last(_START_MONTH_LABELS, from_month) if from_month else False
        filled_start_year  = await _fill_last(_START_YEAR_LABELS, from_year) if from_year else False
        # Fallback: if no month-specific SELECT found, try the broad start pattern as SELECT.
        if not filled_start_month:
            filled_start_month = await _fill_select_last(_START_LABELS, from_month) if from_month else False
        if not filled_start_year:
            filled_start_year = await _fill_last(_START_LABELS, from_year) if from_year else False
        filled_start = filled_start_month or filled_start_year

        # End date: same pattern.
        filled_end_month = await _fill_select_last(_END_MONTH_LABELS, to_month) if to_month else False
        filled_end_year  = await _fill_last(_END_YEAR_LABELS, to_year) if to_year else False
        if not filled_end_month:
            filled_end_month = await _fill_select_last(_END_LABELS, to_month) if to_month else False
        if not filled_end_year:
            filled_end_year = await _fill_last(_END_LABELS, to_year) if to_year else False
        filled_end = filled_end_month or filled_end_year

        _log(
            f"Employment history: filled {company!r} — "
            f"company={filled_company} title={filled_title} "
            f"start={filled_start} end={filled_end}"
        )


async def _do_fill(page, profile: Dict[str, Any], job: Dict[str, Any], result: Dict[str, Any], log_fn=None) -> None:
    """Scan and fill all form fields on the current page; update result in place."""
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    fields, fill_target = await _scan_with_frame_fallback(page)

    if not fields:
        # SPA/iframe may still be rendering — one retry after a short pause.
        await page.wait_for_timeout(2000)
        fields, fill_target = await _scan_with_frame_fallback(page)

    if not fields:
        # Form may be gated behind an "Apply" button (e.g. Comeet).
        # Click it and wait for the apply iframe/form to appear.
        try:
            clicked, active = await try_click_apply(page)
            if clicked:
                target = active if active is not page else page
                await _wait_for_spa(target)
                fields, fill_target = await _scan_with_frame_fallback(page)
        except Exception:
            pass

    cl_file_uploaded = False
    resume_uploaded = False
    if fields:
        try:
            actions = await fill_form(fill_target, fields, profile, job, log_fn=log_fn)
            result["filled"] += sum(
                1 for a in actions if a["action"] in ("filled", "checked", "selected")
            )
            result["skipped"] += sum(1 for a in actions if a["action"] == "skipped")
            result["errors"] += sum(1 for a in actions if a["action"] == "error")
            result["uploads"] = result.get("uploads", 0) + sum(
                1 for a in actions if a["action"] == "uploaded"
            )
            cl_file_uploaded = any(
                a.get("action") == "uploaded" and a.get("is_cover_letter")
                for a in actions
            )
            resume_uploaded = any(
                a.get("action") == "uploaded" and not a.get("is_cover_letter")
                for a in actions
            )
        except Exception:
            pass

        # Second pass: some ATS forms (e.g. Greenhouse) render additional fields
        # conditionally after earlier fields are filled — the canonical example is
        # the "Please identify your race" combobox that only appears after
        # "Are you Hispanic/Latino?" is answered.  Re-scan and fill any new fields.
        try:
            first_ids = {f.get("id") for f in fields if f.get("id")}
            fields2, fill_target2 = await _scan_with_frame_fallback(page)
            # Exclude cover_letter_text — it appears after "Enter manually" is
            # clicked and is already filled; passing it again would cause an
            # unwanted LLM call or overwrite.
            # Also exclude file inputs already seen in the first pass — they
            # would cause a duplicate resume upload.
            new_fields = [
                f for f in fields2
                if f.get("id") and f.get("id") not in first_ids
                and f.get("id") not in ("cover_letter_text",)
                and not (f.get("type") == "file" and resume_uploaded and not f.get("id", "").startswith("cover"))
            ]
            if new_fields:
                new_labels = [f.get("label") or f.get("id") for f in new_fields]
                _log(f"Dynamic fields revealed: {new_labels} — filling now…")
                actions2 = await fill_form(fill_target2, new_fields, profile, job, log_fn=log_fn)
                result["filled"] += sum(
                    1 for a in actions2 if a["action"] in ("filled", "checked", "selected")
                )
                result["skipped"] += sum(1 for a in actions2 if a["action"] == "skipped")
                result["errors"] += sum(1 for a in actions2 if a["action"] == "error")
                result["uploads"] = result.get("uploads", 0) + sum(
                    1 for a in actions2 if a["action"] == "uploaded"
                )
                if not resume_uploaded:
                    resume_uploaded = any(
                        a.get("action") == "uploaded" and not a.get("is_cover_letter")
                        for a in actions2
                    )
        except Exception:
            pass

    # Set intl-tel-input phone country code (Greenhouse job-boards and similar ATSes
    # use the iti library which renders a flag button, not a standard input/select).
    try:
        await _set_iti_phone_country(fill_target, profile, log_fn=_log)
    except Exception:
        pass

    # Only call try_upload_resume if the resume was NOT already uploaded via
    # fill_form (prevents double-upload on standard forms with visible file inputs).
    if not resume_uploaded:
        try:
            upload_result = await try_upload_resume(fill_target, profile, job, log_fn=_log)
            _log(f"Resume upload result: {upload_result}")
        except Exception as exc:
            _log(f"Resume upload error: {exc}")
    else:
        _log("Resume already uploaded via form fields — skipping try_upload_resume")

    # Fill repeating employment history groups ("Add another" pattern).
    try:
        await _fill_employment_history(fill_target, profile, _log)
    except Exception as exc:
        _log(f"Employment history fill error: {exc}")

    # Fall back to "Enter manually" only when the PDF file upload didn't succeed.
    if not cl_file_uploaded:
        try:
            await _fill_cover_letter_manually(fill_target, job)
        except Exception:
            pass

    # Final verification: scan the DOM directly for any cover letter file inputs
    # that are still empty and retry the upload.  This catches cases where the
    # field was detected but skipped (no cover letter text at fill time), or where
    # the earlier upload silently failed.
    try:
        await _ensure_cover_letter_uploaded(fill_target, job, _log)
    except Exception as exc:
        _log(f"Cover letter check error: {exc}")

    # All automated filling is now complete.  The browser stays open so the user
    # can review, fix anything, and click Submit — but no more automation will run.
    total_filled = result.get("filled", 0)
    total_skipped = result.get("skipped", 0)
    _log(
        f"--- Prefill complete: {total_filled} field(s) set, {total_skipped} skipped."
        " Review the form in the browser and click Submit when ready."
        " No further automation will run."
    )


async def _watch_for_ats_and_fill(
    page,
    profile: Dict[str, Any],
    job: Dict[str, Any],
    result: Dict[str, Any],
    closed_event: asyncio.Event,
    filled_urls: Set[str],
    poll_interval: float = 1.5,
    log_fn=None,
) -> None:
    """Poll page.url for same-tab navigation to ATS pages not yet filled."""
    while not closed_event.is_set():
        await asyncio.sleep(poll_interval)
        if closed_event.is_set():
            break
        try:
            current_ats = detect_ats(page.url)
            if current_ats in MANUAL_ONLY_ATS:
                continue
            # Also detect ATS via child frames (embedded Greenhouse etc.)
            if current_ats == "unknown":
                current_ats = _frame_ats(page) or "unknown"
            if current_ats == "unknown":
                continue
            key = _page_key(page.url)
            if key in filled_urls:
                continue
            await _wait_for_spa(page)
            result["ats"] = current_ats
            await _do_fill(page, profile, job, result, log_fn=log_fn)
            filled_urls.add(key)
        except Exception:
            pass


async def _watch_for_security_code(
    context,
    profile: Dict[str, Any],
    closed_event: asyncio.Event,
    poll_interval: float = 3.0,
    log_fn=None,
) -> None:
    """Watch all open pages for a Greenhouse security-code field.

    When found, polls Gmail for the code and fills the field automatically.
    Requires ``credentials.gmail.app_password`` in the profile.
    """
    import time

    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    filled_field_keys: set[str] = set()

    gmail_address = (profile.get("personal", {}).get("email") or "").strip()
    creds = (profile.get("credentials") or {}).get("gmail") or {}
    app_password = (creds.get("app_password") or "").strip()

    while not closed_event.is_set():
        await asyncio.sleep(poll_interval)
        if closed_event.is_set():
            break
        try:
            pages = context.pages
        except Exception:
            continue

        for page in pages:
            if closed_event.is_set():
                break
            try:
                field, field_key = await _find_security_code_input(page)
                if field is None or field_key in filled_field_keys:
                    continue

                # We found an unfilled security code field.
                if not gmail_address or not app_password:
                    _log(
                        "Security code field detected — no Gmail App Password configured."
                        " Enter the code manually (check your email)."
                    )
                    filled_field_keys.add(field_key)
                    continue

                _log("Security code field detected — checking Gmail for code…")
                from utils.gmail_imap import fetch_greenhouse_security_code

                code = await fetch_greenhouse_security_code(
                    gmail_address,
                    app_password,
                    received_after=time.time() - 120,  # accept emails up to 2 min old
                )

                if not code:
                    _log(
                        "Security code email not received within 3 minutes."
                        " Please enter the code manually."
                    )
                    filled_field_keys.add(field_key)
                    continue

                _log(f"Security code received ({code}) — filling field…")
                await field.fill(code)
                filled_field_keys.add(field_key)
                _log("Security code filled. Click 'Resubmit' to complete your application.")

            except Exception:
                pass


async def _find_security_code_input(page):
    """Return (Locator, unique_key) for a visible security-code input, or (None, None)."""
    _SC_SELECTORS = [
        'input[name*="security" i]',
        'input[id*="security" i]',
        'input[name*="security_code" i]',
        'input[id*="security_code" i]',
    ]
    for sel in _SC_SELECTORS:
        try:
            el = page.locator(sel).first
            if await el.count() > 0 and await el.is_visible(timeout=200):
                uid = await el.get_attribute("id") or await el.get_attribute("name") or sel
                key = f"{page.url}::{uid}"
                return el, key
        except Exception:
            continue

    # Broader: any visible text input near "security code" text on the page.
    try:
        has_sc_text = await page.locator(
            "text=/security code/i"
        ).count()
        if has_sc_text > 0:
            # Find the first visible text/tel input that is empty.
            for sel in ('input[type="text"]', 'input[type="tel"]', 'input:not([type])'):
                candidate = page.locator(sel).first
                if await candidate.count() > 0 and await candidate.is_visible(timeout=200):
                    current_val = await candidate.input_value()
                    if not current_val:
                        key = f"{page.url}::text_near_sc"
                        return candidate, key
    except Exception:
        pass

    return None, None


def _frame_ats(page) -> str | None:
    """Return the ATS name detected from any child frame URL, or None."""
    for frame in page.frames[1:]:
        url = frame.url or ""
        if not url or url == "about:blank":
            continue
        ats = detect_ats(url)
        if ats not in (None, "unknown"):
            return ats
    return None


async def _scan_with_frame_fallback(page):
    """Scan the page and its child frames for form fields.

    Returns (fields, fill_target) where fill_target is the page or the
    child frame that contained the fields (e.g. Comeet's /apply iframe).
    """
    try:
        fields = await scan_fields(page)
    except Exception:
        fields = []
    if fields:
        return fields, page

    # Check child frames — some ATSes (e.g. Comeet) embed the application
    # form inside an iframe on the job listing page.
    # Pick the frame with the most fields (not the first) so that cookie
    # banners or social widgets with 1-2 inputs don't win over the real form.
    best_fields: list[dict] = []
    best_frame = None
    for frame in page.frames[1:]:
        url = frame.url or ""
        if not url or url == "about:blank":
            continue
        try:
            frame_fields = await scan_fields(frame)
            if len(frame_fields) > len(best_fields):
                best_fields = frame_fields
                best_frame = frame
        except Exception:
            pass

    if best_frame is not None:
        return best_fields, best_frame

    return [], page


async def _wait_for_spa(page) -> None:
    """Wait for a React/SPA page to finish rendering after navigation.

    If the page already has an ATS embed iframe (e.g. Greenhouse on an
    employer career page), the iframe is already fully loaded at the `load`
    event, so we skip the expensive networkidle wait and just allow a short
    React-render buffer.  This saves ~2s on embedded-ATS pages.
    """
    if _frame_ats(page) is not None:
        await page.wait_for_timeout(200)
        return
    try:
        await page.wait_for_load_state("networkidle", timeout=3000)
    except Exception:
        pass
    await page.wait_for_timeout(500)


async def _ensure_cover_letter_uploaded(page, job: Dict[str, Any], log_fn=None) -> None:
    """DOM-level verification that a cover letter was uploaded.

    After fill_form() + _fill_cover_letter_manually() have run, scan every
    input[type='file'] in the DOM whose surrounding context mentions
    "cover letter".  If any such input still has no file (files.length == 0)
    we attempt a direct set_input_files upload.

    Logs clearly:
      - "Cover letter field filled ✓"                  — already done
      - "Cover letter uploaded ✓"                      — retry succeeded
      - "⚠ Cover letter field found but empty …"       — no text / retry failed

    Does nothing when no cover-letter file field is found on the page
    (the form simply has no cover letter field).
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    # Scan the DOM for file inputs associated with a "cover letter" label / context.
    cl_inputs: list[dict] = []
    try:
        cl_inputs = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('input[type="file"]'))
                .map(el => {
                    const id = el.id || '';
                    const lbl = id ? document.querySelector('label[for="' + id + '"]') : null;
                    const lblText = lbl ? lbl.innerText.toLowerCase() : '';
                    let ctx = lblText + ' ' + (el.name || '').toLowerCase()
                              + ' ' + id.toLowerCase();
                    // Walk up 6 ancestors collecting text
                    let node = el.parentElement;
                    for (let d = 0; d < 6; d++) {
                        if (!node) break;
                        ctx += ' ' + (node.getAttribute('aria-label') || '').toLowerCase();
                        for (const c of node.childNodes)
                            if (c.nodeType === 3) ctx += ' ' + c.textContent.toLowerCase();
                        node = node.parentElement;
                    }
                    if (!/cover.?letter|covering.?letter/.test(ctx)) return null;
                    return { id, hasFile: el.files != null && el.files.length > 0 };
                })
                .filter(Boolean);
        }""")
    except Exception:
        return

    if not cl_inputs:
        return  # Form has no cover letter field — nothing to check.

    already_filled = [f for f in cl_inputs if f.get("hasFile")]
    if already_filled:
        _log("Cover letter field is filled ✓")
        return

    # Field exists but is empty.
    cl_text = ((job or {}).get("cover_letter") or "").strip()
    if not cl_text:
        _log("⚠ Cover letter field found but no cover letter text — fill manually in the browser")
        return

    # Generate the PDF and upload to every unfilled CL input.
    from utils.form_filler import _resolve_cover_letter_path
    file_path = _resolve_cover_letter_path({}, job)
    if not file_path:
        _log("⚠ Cover letter field found but PDF generation failed — fill manually")
        return

    _log("Cover letter not yet uploaded — retrying now…")
    success = False
    for field_info in cl_inputs:
        if field_info.get("hasFile"):
            continue
        fid = field_info.get("id", "")
        try:
            if fid:
                el_loc = page.locator(f'[id="{fid}"]').first
                if await el_loc.count() > 0:
                    await el_loc.set_input_files(file_path)
                    try:
                        await page.evaluate(
                            "id => { const el = document.getElementById(id); if (el) {"
                            " el.dispatchEvent(new Event('input', {bubbles:true}));"
                            " el.dispatchEvent(new Event('change', {bubbles:true})); } }",
                            fid,
                        )
                    except Exception:
                        pass
                    success = True
            else:
                # No id — target all cover-letter file inputs by position
                await page.locator("input[type='file']").first.set_input_files(file_path)
                success = True
        except Exception as exc:
            _log(f"Cover letter retry failed ({exc}) — fill manually")
    if success:
        _log("Cover letter uploaded ✓")


async def _fill_cover_letter_manually(page, job: Dict[str, Any]) -> None:
    """Click 'Enter manually' near the cover letter field and fill the textarea.

    Called after fill_form() as a safety net — the Greenhouse file input is
    visually-hidden so scan_fields() may miss it, and the file-chooser path
    is brittle.  This handler works purely from visible UI elements and is
    independent of field detection.

    `page` may be a Frame (e.g. Greenhouse embedded on instacart.careers).
    """
    cl_text = ((job or {}).get("cover_letter") or "").strip()
    if not cl_text:
        return

    # Find the "Enter manually" button scoped to the cover letter section.
    # Greenhouse forms have two such buttons (resume + cover letter); we must
    # target the cover-letter-specific one.  Try progressively broader selectors.
    manual_btn = None
    _cl_btn_candidates = [
        page.locator("[data-testid='cover_letter-text']").first,
        page.locator("button[data-testid*='cover_letter']").first,
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
    # Broad fallback: last "Enter manually" button (CL section follows resume in Greenhouse).
    if manual_btn is None:
        _all_manual = page.get_by_role("button", name=re.compile(r"enter.?manually", re.I))
        _cnt = await _all_manual.count()
        if _cnt > 0:
            _cand = _all_manual.nth(_cnt - 1)
            try:
                if await _cand.is_visible(timeout=300):
                    manual_btn = _cand
            except Exception:
                pass
    if manual_btn is None:
        return

    # Check if a textarea is already filled (user or earlier run did it).
    for ta_chk in [
        page.locator("textarea[name*='cover']").first,
        page.locator("textarea[id*='cover']").first,
        page.locator("textarea").last,
    ]:
        try:
            if await ta_chk.count() > 0 and await ta_chk.is_visible():
                existing = (await ta_chk.input_value()).strip()
                if existing:
                    return  # already filled
        except Exception:
            pass

    await manual_btn.click()

    # Wait specifically for the cover-letter textarea — not "textarea.last"
    # which would match any already-visible textarea on the page.
    _cl_ta = page.locator("textarea[id*='cover'], textarea[name*='cover']").first
    try:
        await _cl_ta.wait_for(state="visible", timeout=4000)
    except Exception:
        await page.wait_for_timeout(1000)

    # Fill the first matching textarea.
    for ta_loc in [
        page.locator("textarea[id='cover_letter_text']").first,
        page.locator("textarea[name='cover_letter_text']").first,
        page.locator("textarea[id*='cover']").first,
        page.locator("textarea[name*='cover']").first,
    ]:
        try:
            if await ta_loc.count() > 0 and await ta_loc.is_visible():
                await ta_loc.click()
                await ta_loc.fill(cl_text)
                return
        except Exception:
            continue
