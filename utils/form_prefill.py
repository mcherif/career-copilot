"""
Playwright-based form prefill utility for the web UI.

Called from ui/app.py when the user clicks "Open & Prefill".
Extracted from the open-job CLI command so it can be tested independently.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

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
            try:
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

            if ats not in MANUAL_ONLY_ATS:
                try:
                    fields = await scan_fields(active_page)
                except Exception:
                    fields = []

                if fields:
                    try:
                        actions = await fill_form(active_page, fields, profile, job)
                        result["filled"] = sum(
                            1 for a in actions if a["action"] in ("filled", "checked", "selected")
                        )
                        result["skipped"] = sum(1 for a in actions if a["action"] == "skipped")
                        result["errors"] = sum(1 for a in actions if a["action"] == "error")
                    except Exception:
                        pass

                try:
                    await try_upload_resume(active_page, profile, job)
                except Exception:
                    pass

            # Keep browser open — wait for user to close it.
            closed_event = asyncio.Event()
            active_page.on("close", lambda: closed_event.set())
            browser.on("disconnected", lambda: closed_event.set())

            try:
                await asyncio.wait_for(closed_event.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                pass

            try:
                await browser.close()
            except Exception:
                pass

            result["status"] = "ok"
            return result

    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


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
