"""
Playwright-based form prefill utility for the web UI.

Called from ui/app.py when the user clicks "Open & Prefill".
Extracted from the open-job CLI command so it can be tested independently.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Set

from playwright.async_api import async_playwright  # noqa: F401 — imported for patching
from utils.ats_detector import MANUAL_ONLY_ATS, detect_ats
from utils.form_filler import fill_form, try_upload_resume
from utils.form_inspector import extract_apply_url, scan_fields, try_click_apply

# Domains where Playwright is blocked by bot detection.
# These must be opened in the user's system browser instead.
SYSTEM_BROWSER_DOMAINS = {
    "remoteok.com",
    "weworkremotely.com",
    "jobicy.com",
    "getonbrd.com",
    "himalayas.app",
}


def is_system_browser_domain(url: str) -> bool:
    """Return True if the URL belongs to a bot-protected domain."""
    url_lower = (url or "").lower()
    return any(domain in url_lower for domain in SYSTEM_BROWSER_DOMAINS)


async def run_prefill_session(
    job: Dict[str, Any],
    profile: Dict[str, Any],
    headless: bool = False,
    wait_timeout: float = 3600,
) -> Dict[str, Any]:
    """
    Open a job URL in Playwright, navigate to the application form,
    fill fields from profile, and upload resume.

    Keeps the browser open until the user closes it (or wait_timeout seconds).
    A background watcher continuously monitors for navigation to any ATS
    application page — so even if the user manually clicks "Apply" on a
    listing aggregator, the form is filled automatically once it loads.

    Returns one of:
        {"status": "ok",     "ats": str, "filled": int, "skipped": int, "errors": int}
        {"status": "manual", "reason": str}   — bot-protected, use system browser
        {"status": "failed", "error": str}
    """
    url = (job.get("url") or "").strip()
    if not url:
        return {"status": "failed", "error": "No URL for this job"}

    if is_system_browser_domain(url):
        return {
            "status": "manual",
            "reason": "Site blocks automated browsers — open in system browser",
        }

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()

            try:
                await page.goto(url, wait_until="load", timeout=30000)
                await _wait_for_spa(page)
            except Exception as exc:
                await browser.close()
                return {"status": "failed", "error": f"Page load failed: {exc}"}

            # Resolve direct apply URL from listing aggregators.
            # Retry once if nothing is found — the button may be JS-rendered.
            try:
                resolved = await extract_apply_url(page)
                if not resolved:
                    await page.wait_for_timeout(3000)
                    resolved = await extract_apply_url(page)
                if resolved and resolved != url:
                    await page.goto(resolved, wait_until="load", timeout=30000)
                    await _wait_for_spa(page)
            except Exception:
                pass

            # Click through to the application form.
            active_page = page
            try:
                clicked, active_page = await try_click_apply(active_page)
                if clicked:
                    await _wait_for_spa(active_page)
            except Exception:
                pass

            # Ashby shortcut: if we're on a job listing page, navigate directly
            # to the /application path which loads the form immediately.
            ats = detect_ats(active_page.url)
            if ats == "ashby" and "/application" not in active_page.url:
                app_url = active_page.url.rstrip("/") + "/application"
                try:
                    await active_page.goto(app_url, wait_until="load", timeout=20000)
                    await _wait_for_spa(active_page)
                except Exception:
                    pass
                ats = detect_ats(active_page.url)

            result: Dict[str, Any] = {"ats": ats, "filled": 0, "skipped": 0, "errors": 0}
            filled_ats: Set[str] = set()

            # Initial fill on whichever page we landed on.
            if ats not in MANUAL_ONLY_ATS and ats != "unknown":
                await _do_fill(active_page, profile, job, result)
                filled_ats.add(ats)

            # Keep browser open — wait for user to close it.
            # Concurrently watch for navigation to any ATS page so we can fill
            # automatically even if the user navigated there manually.
            closed_event = asyncio.Event()
            active_page.on("close", lambda: closed_event.set())
            browser.on("disconnected", lambda: closed_event.set())

            watch_task = asyncio.create_task(
                _watch_for_ats_and_fill(active_page, profile, job, result, closed_event, filled_ats)
            )

            try:
                await asyncio.wait_for(closed_event.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                pass

            watch_task.cancel()
            try:
                await asyncio.wait_for(watch_task, timeout=2)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

            try:
                await browser.close()
            except Exception:
                pass

            result["status"] = "ok"
            return result

    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


async def _do_fill(page, profile: Dict[str, Any], job: Dict[str, Any], result: Dict[str, Any]) -> None:
    """Scan and fill all form fields on the current page; update result in place."""
    try:
        fields = await scan_fields(page)
    except Exception:
        fields = []

    if fields:
        try:
            actions = await fill_form(page, fields, profile, job)
            result["filled"] += sum(
                1 for a in actions if a["action"] in ("filled", "checked", "selected")
            )
            result["skipped"] += sum(1 for a in actions if a["action"] == "skipped")
            result["errors"] += sum(1 for a in actions if a["action"] == "error")
        except Exception:
            pass

    try:
        await try_upload_resume(page, profile, job)
    except Exception:
        pass


async def _watch_for_ats_and_fill(
    page,
    profile: Dict[str, Any],
    job: Dict[str, Any],
    result: Dict[str, Any],
    closed_event: asyncio.Event,
    filled_ats: Set[str],
    poll_interval: float = 1.5,
) -> None:
    """Poll the page URL every poll_interval seconds.

    When it detects navigation to a new ATS application page (one we haven't
    filled yet), waits for the SPA to render then fills the form.  This handles
    the common case where the user manually clicks an "Apply" button on a job
    listing aggregator — the fill fires as soon as they land on the ATS page.
    """
    while not closed_event.is_set():
        await asyncio.sleep(poll_interval)
        if closed_event.is_set():
            break
        try:
            current_ats = detect_ats(page.url)
            if current_ats in MANUAL_ONLY_ATS or current_ats == "unknown":
                continue
            if current_ats in filled_ats:
                continue
            # New ATS page — wait for the SPA to render then fill.
            await _wait_for_spa(page)
            result["ats"] = current_ats
            await _do_fill(page, profile, job, result)
            filled_ats.add(current_ats)
        except Exception:
            pass


async def _wait_for_spa(page) -> None:
    """Wait for a React/SPA page to finish rendering after navigation.

    Uses networkidle (capped at 8 s) then a fixed 1.5 s buffer so that
    lazily-mounted form components have time to appear in the DOM.
    """
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass
    await page.wait_for_timeout(1500)
