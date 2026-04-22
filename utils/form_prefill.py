"""
Playwright-based form prefill utility for the web UI.

Called from ui/app.py when the user clicks "Open & Prefill".
"""
from __future__ import annotations

import asyncio
import datetime
import json
import os
import re
from collections import defaultdict
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


class _TimingCollector:
    """Accumulates [timing] log lines and can print an aggregate summary table.

    Acts as a drop-in _tlog callable: logs the message AND records the timing.
    Regex captures category from lines like:
      [timing] listbox-visible('label'): 123ms
      [timing] add-another wait: 654ms
    """

    _RE = re.compile(r"\[timing\]\s+([^(:]+)[(:].+?(\d+)ms")

    def __init__(self, log_fn=None) -> None:
        self._log = log_fn or (lambda _: None)
        self._entries: list[tuple[str, float]] = []

    def __call__(self, msg: str) -> None:
        self._log(msg)
        m = self._RE.search(msg)
        if m:
            self._entries.append((m.group(1).strip(), float(m.group(2))))

    def summary(self) -> str:
        if not self._entries:
            return ""
        groups: dict[str, list[float]] = defaultdict(list)
        for cat, ms in self._entries:
            groups[cat].append(ms)
        col = 26
        header = f"{'Operation':<{col}} {'N':>4} {'Total':>8} {'Avg':>7} {'Min':>7} {'Max':>7}"
        sep = "-" * len(header)
        rows = ["\n--- Timing Summary ---", header, sep]
        total_all = 0.0
        for cat in sorted(groups):
            vals = groups[cat]
            t = sum(vals)
            total_all += t
            rows.append(
                f"{cat:<{col}} {len(vals):>4} {t:>7.0f}ms {t/len(vals):>6.0f}ms"
                f" {min(vals):>6.0f}ms {max(vals):>6.0f}ms"
            )
        rows.append(sep)
        rows.append(f"{'TOTAL':<{col}} {len(self._entries):>4} {total_all:>7.0f}ms")
        rows.append("----------------------")
        return "\n".join(rows)

    def save(self, job_label: str = "") -> None:
        """Append one JSONL record to logs/timing.jsonl."""
        if not self._entries:
            return
        groups: dict[str, list[float]] = defaultdict(list)
        for cat, ms in self._entries:
            groups[cat].append(ms)
        record = {
            "ts": datetime.datetime.now().isoformat(timespec="seconds"),
            "job": job_label,
            "total_ms": round(sum(ms for _, ms in self._entries)),
            "ops": {
                cat: {"n": len(vals), "total_ms": round(sum(vals)), "avg_ms": round(sum(vals) / len(vals))}
                for cat, vals in groups.items()
            },
        }
        log_path = os.path.join(os.path.dirname(__file__), "..", "logs", "timing.jsonl")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")


async def run_prefill_session(
    job: Dict[str, Any],
    profile: Dict[str, Any],
    headless: bool = False,
    wait_timeout: float = 3600,
    cancel_event=None,
    log_fn=None,
    timing: bool = False,
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
                    await _do_fill(new_page, profile, job, result, log_fn=_log, timing=timing)
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
                        await _do_fill(active_page, profile, job, result, log_fn=_log, timing=timing)
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
                _watch_for_ats_and_fill(active_page, profile, job, result, closed_event, filled_urls, log_fn=_log, timing=timing)
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


async def _fix_split_phone_country(page, profile: Dict[str, Any], log_fn=None) -> None:
    """Fix split phone inputs where a country-code button sits beside a number field.

    Rippling and similar ATSes render phone as [+1 US ▼] [number input].
    scan_fields only captures the number input, so the pipeline fills the full
    international number (e.g. "216 93117117") into a number-only field.
    This function: (1) re-fills the number field with just the local number,
    (2) clicks the country code button and selects the correct country.
    """
    import re as _re

    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    personal = profile.get("personal") or {}
    bare = str(personal.get("phone") or "").strip()
    country = str(personal.get("phone_country") or "").strip()
    cc = str(personal.get("phone_country_code") or "").strip()
    if not bare or not country:
        return

    # Find buttons whose visible text starts with "+" followed by digits (country code picker).
    try:
        all_btns = page.locator("button")
        btn_count = await all_btns.count()
        for i in range(btn_count):
            btn = all_btns.nth(i)
            if not await btn.is_visible(timeout=200):
                continue
            btn_text = (await btn.inner_text()).strip()
            if not _re.match(r"^\+\d", btn_text):
                continue

            # Found a split phone country-code button. Locate the adjacent number input.
            phone_id = await btn.evaluate("""el => {
                const parent = el.parentElement;
                if (!parent) return '';
                const inp = parent.querySelector('input[type="tel"], input[type="text"]');
                if (inp) return inp.id || '';
                const next = parent.nextElementSibling;
                if (next) {
                    const inp2 = next.querySelector('input[type="tel"], input[type="text"]');
                    if (inp2) return inp2.id || '';
                    if (next.tagName === 'INPUT') return next.id || '';
                }
                return '';
            }""")
            if not phone_id:
                continue

            phone_loc = page.locator(f"#{phone_id}").first
            if await phone_loc.count() == 0:
                continue

            current_val = (await phone_loc.input_value()).strip()
            intl = f"{cc} {bare}".strip() if cc else bare
            if current_val not in (intl, bare):
                continue  # not a field we filled — skip

            # Re-fill with bare number if currently has the international format.
            if current_val != bare:
                await phone_loc.fill(bare, force=True)
                _log(f"Split phone: re-filled number input with bare number '{bare}'")

            # If the button already shows the right country code (+216), done.
            if cc and f"+{cc}" in btn_text:
                _log(f"Split phone: country code already correct ({btn_text})")
                return

            # Click the country code button to open the picker dropdown.
            await btn.click()
            await asyncio.sleep(0.4)

            # Many dropdown implementations include a search input — use it.
            search_inp = page.locator(
                "input[placeholder*='Search'], input[placeholder*='search'], "
                "input[aria-label*='Search'], input[aria-label*='search']"
            ).last
            if await search_inp.count() > 0 and await search_inp.is_visible(timeout=500):
                await search_inp.fill(country)
                await asyncio.sleep(0.4)

            # Click the matching country entry.
            country_item = page.locator(
                f"[data-dial-code='{cc}'], li:has-text('{country}'), "
                f"[role='option']:has-text('{country}')"
            ).first
            if await country_item.count() > 0 and await country_item.is_visible(timeout=1000):
                await country_item.click()
                _log(f"Split phone: country changed to {country} (+{cc})")
            else:
                await page.keyboard.press("Escape")
                _log(f"Split phone: could not find '{country}' in dropdown — fill manually")
            return
    except Exception as exc:
        _log(f"Split phone fix error: {exc}")


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


async def _fill_employment_history(page, profile: Dict[str, Any], log_fn=None, timing: bool = False) -> None:
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

    _tlog = timing if callable(timing) else (_log if timing else (lambda _: None))

    work_history = profile.get("work_history") or []
    if not work_history:
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

    # ---------------------------------------------------------------------------
    # JS helpers: find the Nth visible labeled element and return its absolute
    # index within all elements of that type.  We count labeled elements BEFORE
    # clicking "Add another" to know which index belongs to the new entry —
    # this works regardless of where Greenhouse inserts the new group in the DOM
    # and regardless of whether elements have name/id attributes.
    # ---------------------------------------------------------------------------
    _GET_LABEL_FN = """
        function getLabel(el) {
            const id = el.id || '';
            if (id) {
                const lb = document.querySelector('label[for="' + id + '"]');
                if (lb) return lb.innerText.trim();
            }
            const al = el.getAttribute('aria-label');
            if (al && al.trim()) return al.trim();
            const alb = el.getAttribute('aria-labelledby');
            if (alb) {
                const txt = alb.split(' ')
                    .map(lid => document.getElementById(lid))
                    .filter(Boolean)
                    .map(e => e.innerText.trim())
                    .filter(Boolean)
                    .join(' ');
                if (txt) return txt;
            }
            let node = el.parentElement;
            for (let d = 0; d < 8; d++) {
                if (!node) break;
                if (node.tagName === 'LABEL') return node.innerText.trim();
                if (node.id) {
                    const lb = document.querySelector('label[for="' + node.id + '"]');
                    if (lb) return lb.innerText.trim();
                }
                let sib = node.previousElementSibling;
                while (sib) {
                    if (sib.tagName === 'LABEL') return sib.innerText.trim();
                    if (!sib.querySelector('input, textarea, select')) {
                        const inner = sib.querySelector('label');
                        if (inner) return inner.innerText.trim();
                    }
                    sib = sib.previousElementSibling;
                }
                node = node.parentElement;
            }
            return el.placeholder || '';
        }
        function visible(el) {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        }
    """

    # Returns absolute DOM positions of all visible labeled text-inputs.
    # Using the FULL querySelectorAll result (including CSS-hidden elements) so
    # Playwright's .nth() targets the same element as the JS found.
    # Excludes inputs that are *inside* a [role="combobox"] element — those are
    # react-select's internal search inputs, which have no direct label and get
    # misidentified by ancestor traversal (e.g. as "start year" or "end year").
    _JS_LIST_INP_POS = f"""([labelRe]) => {{
        {_GET_LABEL_FN}
        const re = new RegExp(labelRe, 'i');
        const sel = "input:not([type='hidden']):not([type='submit'])" +
                    ":not([type='checkbox']):not([type='radio']):not([type='file'])";
        const allEls = Array.from(document.querySelectorAll(sel));
        return allEls
            .filter(visible)
            .filter(el => !el.closest('[role="combobox"]'))
            .filter(el => re.test(getLabel(el)))
            .map(el => allEls.indexOf(el));
    }}"""

    # Same but for native <select> and ARIA comboboxes.
    _JS_LIST_DD_POS = f"""([labelRe]) => {{
        {_GET_LABEL_FN}
        const re = new RegExp(labelRe, 'i');
        const sel = 'select, [role="combobox"]';
        const allEls = Array.from(document.querySelectorAll(sel));
        return allEls.filter(visible).filter(el => re.test(getLabel(el))).map(el => allEls.indexOf(el));
    }}"""

    _INP_SEL = ("input:not([type='hidden']):not([type='submit'])"
                ":not([type='checkbox']):not([type='radio']):not([type='file'])")

    async def _fill_at_idx(idx: int, value: str) -> bool:
        """Fill the text-input at the given absolute DOM position."""
        if not value:
            return False
        try:
            el = page.locator(_INP_SEL).nth(idx)
            # Diagnostic: log what element we're about to write to.
            try:
                info = await el.evaluate(
                    "e => ({id: e.id, name: e.name, placeholder: e.placeholder, "
                    "type: e.type, 'aria-label': e.getAttribute('aria-label'), "
                    "label: (document.querySelector('label[for=\"'+e.id+'\"]') || {}).innerText || ''})"
                )
                _log(f"  fill_at_idx({idx}, {value!r}): id={info.get('id')!r} name={info.get('name')!r} label={info.get('label')!r}")
            except Exception:
                pass
            try:
                await el.fill(value)
                return True
            except Exception:
                await el.click()
                await el.press("Control+a")
                await el.press_sequentially(value, delay=20)
                return True
        except Exception:
            return False

    async def _fill_dd_at_idx(idx: int, value: str) -> bool:
        """Fill the select/combobox at the given absolute DOM position."""
        if not value:
            return False
        val_lower = value.lower()
        _visible_opts_js = """(listboxId) => {
            const lb = listboxId ? document.getElementById(listboxId) : null;
            const root = lb || document;
            return Array.from(root.querySelectorAll('[role="option"]'))
                .filter(o => { const r = o.getBoundingClientRect();
                               return r.width > 0 && r.height > 0; })
                .map(o => ({text: o.innerText.trim(), id: o.id}));
        }"""
        try:
            el = page.locator('select, [role="combobox"]').nth(idx)
            tag = (await el.evaluate("el => el.tagName.toLowerCase()")).lower()
            if tag == "select":
                opts = await el.evaluate("el => Array.from(el.options).map(o => o.text.trim())")
                chosen = (
                    next((o for o in opts if o.lower() == val_lower), None)
                    or next((o for o in opts if o.lower().startswith(val_lower[:3])), None)
                    or next((o for o in opts if val_lower in o.lower()), None)
                )
                if not chosen:
                    return False
                try:
                    await el.select_option(label=chosen)
                except Exception:
                    await el.select_option(value=chosen)
                return True
            # ARIA combobox — click to open then pick the matching visible option.
            await el.click(timeout=5000)
            await page.wait_for_timeout(600)
            aria_controls = await el.get_attribute("aria-controls") or ""
            opts = await page.evaluate(_visible_opts_js, aria_controls)
            _log(f"  dropdown(idx={idx}, {value!r}): aria={aria_controls!r} opts={[o['text'] for o in opts[:6]]}")
            month_re = re.compile(val_lower[:3], re.I)
            chosen = next((o for o in opts if month_re.search(o["text"])), None)
            if chosen:
                if chosen.get("id"):
                    await page.locator(f'[id="{chosen["id"]}"]').first.click()
                else:
                    scoped = (page.locator(f'#{aria_controls} [role="option"]').filter(has_text=chosen["text"])
                              if aria_controls else
                              page.locator('[role="option"]').filter(has_text=chosen["text"]))
                    await scoped.first.click()
                return True
        except Exception as exc:
            _log(f"  dropdown(idx={idx}, {value!r}): exception: {exc}")
        return False

    def _new_els(pre: list, post: list) -> list:
        """DOM positions that appeared in post but not pre (newly added elements)."""
        pre_set = set(pre)
        return [i for i in post if i not in pre_set]

    # Parse "Feb 2022" → (month_str, year_str).
    def _parse_date(date_str: str):
        parts = date_str.split()
        month = parts[0] if len(parts) >= 1 else ""
        year  = (parts[1] if len(parts) >= 2
                 else (parts[0] if len(parts) == 1 and parts[0].isdigit() else ""))
        return month, year

    async def _check_current_role(co_idx: int) -> bool:
        """Tick the 'Current role' checkbox in the same employment section as the company input."""
        _js = """([coIdx]) => {
            const sel = "input:not([type='hidden']):not([type='submit'])" +
                        ":not([type='checkbox']):not([type='radio']):not([type='file'])";
            const allInputs = Array.from(document.querySelectorAll(sel));
            const companyEl = allInputs[coIdx];
            if (!companyEl) return -1;
            let node = companyEl;
            for (let d = 0; d < 12; d++) {
                if (!node.parentElement) break;
                node = node.parentElement;
                const checkboxes = Array.from(node.querySelectorAll('input[type="checkbox"]'));
                for (const cb of checkboxes) {
                    const lbl = document.querySelector('label[for="' + cb.id + '"]');
                    const txt = (lbl ? lbl.innerText : cb.getAttribute('aria-label') || '').toLowerCase();
                    if (txt.includes('current')) {
                        const allCbs = Array.from(document.querySelectorAll('input[type="checkbox"]'));
                        return allCbs.indexOf(cb);
                    }
                }
            }
            return -1;
        }"""
        try:
            idx = await page.evaluate(_js, [co_idx])
            if idx < 0:
                return False
            cb = page.locator("input[type='checkbox']").nth(idx)
            if await cb.count() > 0 and not await cb.is_checked():
                await cb.check()
                return True
        except Exception:
            pass
        return False

    # -------------------------------------------------------------------------
    # Two-phase fill:
    #   Phase 1 — company, title, months for every entry (serial, entry by entry)
    #   Phase 2 — ONE wait + TWO JS calls to get all year positions, then fill
    #             years for all entries in a single pass.
    #
    # This avoids the 400 ms DOM-settle wait that was needed after EACH entry's
    # month dropdowns (6 entries × 400 ms = 2.4 s saved).  Year positions are
    # stable once all month dropdowns have closed, and all_sy[i] / all_ey[i]
    # reliably maps to entry i by DOM order.
    # -------------------------------------------------------------------------

    # Records for Phase 2: (company, from_year, to_year, is_current, co_idx,
    #                        filled_company, filled_title, filled_sm, filled_em)
    _pending: list[dict] = []

    # Phase 1 — company / title / months.
    for entry_num, entry in enumerate(work_history):
        company   = str(entry.get("company") or "").strip()
        title     = str(entry.get("title") or "").strip()
        date_from = str(entry.get("from") or "").strip()
        date_to   = str(entry.get("to") or "").strip()
        is_current = date_to.lower() == "present"

        from_month, from_year = _parse_date(date_from)
        to_month,   to_year   = _parse_date("" if is_current else date_to)

        if entry_num == 0:
            snap_co = await page.evaluate(_JS_LIST_INP_POS, [r"company|employer"])
            snap_ti = await page.evaluate(_JS_LIST_INP_POS, [r"title|position"])
            snap_sm = await page.evaluate(_JS_LIST_DD_POS,  [r"start\s*(date\s*)?month"])
            snap_em = await page.evaluate(_JS_LIST_DD_POS,  [r"end\s*(date\s*)?month"])
            _log(f"Employment first entry {company!r}: co={snap_co[:2]} ti={snap_ti[:2]} sm={snap_sm} em={snap_em}")
            co_idx = snap_co[0] if snap_co else None
            ti_idx = snap_ti[0] if snap_ti else None
            sm_idx = snap_sm[0] if snap_sm else None
            em_idx = snap_em[0] if snap_em else None
        else:
            pre_co = await page.evaluate(_JS_LIST_INP_POS, [r"company|employer"])
            pre_ti = await page.evaluate(_JS_LIST_INP_POS, [r"title|position"])
            pre_sm = await page.evaluate(_JS_LIST_DD_POS,  [r"start\s*(date\s*)?month"])
            pre_em = await page.evaluate(_JS_LIST_DD_POS,  [r"end\s*(date\s*)?month"])
            _log(f"Employment snapshot for {company!r}: co={len(pre_co)} ti={len(pre_ti)} sm={len(pre_sm)} em={len(pre_em)}")

            clicked = False
            pre_input_count = await page.evaluate(
                "() => Array.from(document.querySelectorAll("
                "  \"input:not([type='hidden']):not([type='submit'])"
                "  :not([type='checkbox']):not([type='radio']):not([type='file'])\""
                ")).filter(el => { const r = el.getBoundingClientRect();"
                "  return r.width > 0 && r.height > 0; }).length"
            )
            for sel in add_btn_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=500):
                        await btn.click()
                        # Wait for a new visible input to appear (Greenhouse pre-renders
                        # sections as hidden DOM nodes and reveals them on click, so we
                        # must filter by bounding rect, not just DOM presence).
                        _t0 = asyncio.get_event_loop().time()
                        try:
                            await page.wait_for_function(
                                "([n]) => Array.from(document.querySelectorAll("
                                "  \"input:not([type='hidden']):not([type='submit'])"
                                "  :not([type='checkbox']):not([type='radio']):not([type='file'])\""
                                ")).filter(el => { const r = el.getBoundingClientRect();"
                                "  return r.width > 0 && r.height > 0; }).length > n",
                                arg=[pre_input_count],
                                timeout=5000,
                            )
                        except Exception:
                            pass
                        _tlog(f"  [timing] add-another wait: {(asyncio.get_event_loop().time()-_t0)*1000:.0f}ms")
                        clicked = True
                        _log(f"Employment history: clicked 'Add another' for {company!r}")
                        break
                except Exception:
                    continue

            if not clicked:
                _log("Employment history: 'Add another' button not found — stopping")
                break

            new_co = _new_els(pre_co, await page.evaluate(_JS_LIST_INP_POS, [r"company|employer"]))
            new_ti = _new_els(pre_ti, await page.evaluate(_JS_LIST_INP_POS, [r"title|position"]))
            new_sm = _new_els(pre_sm, await page.evaluate(_JS_LIST_DD_POS,  [r"start\s*(date\s*)?month"]))
            new_em = _new_els(pre_em, await page.evaluate(_JS_LIST_DD_POS,  [r"end\s*(date\s*)?month"]))
            _log(f"Employment new fields for {company!r}: co={new_co} ti={new_ti} sm={new_sm} em={new_em}")
            co_idx = new_co[0] if new_co else None
            ti_idx = new_ti[0] if new_ti else None
            sm_idx = new_sm[0] if new_sm else None
            em_idx = new_em[0] if new_em else None

        filled_company = await _fill_at_idx(co_idx, company) if co_idx is not None and company else False
        filled_title   = await _fill_at_idx(ti_idx, title)   if ti_idx is not None and title   else False
        filled_sm = await _fill_dd_at_idx(sm_idx, from_month) if sm_idx is not None and from_month else False
        filled_em = (await _fill_dd_at_idx(em_idx, to_month)
                     if em_idx is not None and to_month and not is_current else False)
        _pending.append({
            "company": company, "co_idx": co_idx,
            "from_year": from_year, "to_year": to_year, "is_current": is_current,
            "filled_company": filled_company, "filled_title": filled_title,
            "filled_sm": filled_sm, "filled_em": filled_em,
        })

    # Phase 2 — batch year evaluation and fill.
    # One DOM-settle wait + two JS calls covers all entries.
    await page.wait_for_timeout(400)
    all_sy = await page.evaluate(_JS_LIST_INP_POS, [r"start\s*(date\s*)?year"])
    all_ey = await page.evaluate(_JS_LIST_INP_POS, [r"end\s*(date\s*)?year"])

    for i, pend in enumerate(_pending):
        company    = pend["company"]
        from_year  = pend["from_year"]
        to_year    = pend["to_year"]
        is_current = pend["is_current"]

        sy_idx = all_sy[i] if i < len(all_sy) else None
        ey_idx = all_ey[i] if i < len(all_ey) else None
        _log(f"Employment year fields for {company!r}: sy={sy_idx} ey={ey_idx}")

        pend["filled_sy"] = await _fill_at_idx(sy_idx, from_year) if sy_idx is not None and from_year else False
        pend["filled_ey"] = (await _fill_at_idx(ey_idx, to_year)
                             if ey_idx is not None and to_year and not is_current else False)

    # Final pass: check "current role" AFTER all year fills so that DOM mutations
    # from hiding end-date fields don't invalidate the stale all_sy/all_ey snapshot.
    for pend in _pending:
        company    = pend["company"]
        is_current = pend["is_current"]
        co_idx     = pend["co_idx"]

        checked_current = False
        if is_current and co_idx is not None:
            checked_current = await _check_current_role(co_idx)

        _log(
            f"Employment history: filled {company!r} — "
            f"company={pend['filled_company']} title={pend['filled_title']} "
            f"start={pend['filled_sm']}/{pend['filled_sy']} "
            f"end={pend['filled_em']}/{pend['filled_ey']} "
            f"current={checked_current}"
        )


async def _do_fill(page, profile: Dict[str, Any], job: Dict[str, Any], result: Dict[str, Any], log_fn=None, timing: bool = False) -> None:
    """Scan and fill all form fields on the current page; update result in place."""
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    _timing_arg: bool | _TimingCollector = False
    if timing:
        _timing_arg = _TimingCollector(_log)

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

    # Workable pre-fill: upload resume first so Workable auto-populates
    # name, email, work history, etc. before we fill remaining fields.
    # We intentionally do NOT re-scan after upload — re-scanning returns
    # Workable's resume-parser UI instead of the application form fields,
    # causing radio/button groups to disappear from the fill pass.
    _early_resume_uploaded = False
    if result.get("ats") == "workable" and fields:
        try:
            _log("Workable detected — uploading resume first to enable profile pre-fill…")
            early = await try_upload_resume(fill_target, profile, job, log_fn=_log)
            _log(f"Early resume upload: {early}")
            if early and "uploaded" in early:
                _early_resume_uploaded = True
                _log("Waiting for Workable to process resume pre-fill…")
                await fill_target.wait_for_timeout(3000)
        except Exception as exc:
            _log(f"Early Workable resume upload error: {exc}")

    cl_file_uploaded = False
    resume_uploaded = False
    if fields:
        try:
            actions = await fill_form(fill_target, fields, profile, job, log_fn=log_fn, timing=_timing_arg)
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
                actions2 = await fill_form(fill_target2, new_fields, profile, job, log_fn=log_fn, timing=_timing_arg)
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

    # Fix split phone inputs (e.g. Rippling: [+1 US ▼] [number]).
    # Corrects the number field to bare digits and changes the country code dropdown.
    try:
        await _fix_split_phone_country(fill_target, profile, log_fn=_log)
    except Exception:
        pass

    # Call try_upload_resume for React-based upload buttons (e.g. Workable) that
    # ignore set_input_files().  Skip if early Workable upload already succeeded
    # to avoid uploading twice.  On other ATSes (e.g. Greenhouse) where fill_form
    # correctly uploaded, the button is gone so this returns harmlessly.
    if not _early_resume_uploaded:
        try:
            upload_result = await try_upload_resume(fill_target, profile, job, log_fn=_log)
            _log(f"Resume upload result: {upload_result}")
        except Exception as exc:
            _log(f"Resume upload error: {exc}")

    # Fill repeating employment history groups ("Add another" pattern).
    try:
        await _fill_employment_history(fill_target, profile, _log, timing=_timing_arg)
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
    if isinstance(_timing_arg, _TimingCollector):
        summary = _timing_arg.summary()
        if summary:
            _log(summary)
        job_label = f"{job.get('title', '')} @ {job.get('company', '')}".strip(" @")
        _timing_arg.save(job_label)
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
    timing: bool = False,
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
            await _do_fill(page, profile, job, result, log_fn=log_fn, timing=timing)
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
    _fc_page = getattr(page, "page", page)  # Frame → parent Page for file chooser

    # Strategy 1: file chooser via clicking the "Drop or select" / upload area
    # (works for React-based file inputs like Rippling that ignore set_input_files).
    cl_upload_selectors = [
        "label:has-text('Cover letter')",
        "[aria-label*='Cover letter']",
        "[aria-label*='cover letter']",
        "div:has-text('Drop or select'):near(h2:has-text('Cover letter'))",
        "div:has-text('Drop or select'):near(label:has-text('Cover letter'))",
        "div:has-text('Drop or select'):near(p:has-text('Cover letter'))",
    ]
    for sel in cl_upload_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0 or not await loc.is_visible(timeout=300):
                continue
            async with _fc_page.expect_file_chooser(timeout=4000) as fc_info:
                await loc.click()
            fc = await fc_info.value
            await fc.set_files(file_path)
            _log("Cover letter uploaded via file chooser ✓")
            return
        except Exception:
            continue

    # Strategy 2: set_input_files on the hidden file input (non-React ATSes).
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
