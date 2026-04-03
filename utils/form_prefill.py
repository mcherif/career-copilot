"""
Playwright-based form prefill utility for the web UI.

Called from ui/app.py when the user clicks "Open & Prefill".
"""
from __future__ import annotations

import asyncio
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
    "himalayas.app",
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
) -> Dict[str, Any]:
    """
    Open a job URL in Playwright, navigate to the application form,
    fill fields from profile, and upload resume.

    Keeps the browser open until the user closes it (or wait_timeout seconds).

    The new-tab handler is registered BEFORE any navigation so that even
    if the user clicks "Apply" while the automation is still waiting on
    the listing page, the resulting Ashby tab is caught and filled.
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
            context = page.context

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
                    await _do_fill(new_page, profile, job, result)
                    filled_urls.add(key)
                    new_page.on("close", lambda: closed_event.set())
                except Exception:
                    pass

            context.on("page", lambda p: loop.create_task(_fill_new_tab(p)))

            # ----------------------------------------------------------------
            # Navigate and attempt to reach the ATS application form.
            # ----------------------------------------------------------------
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
                    resolved = await extract_apply_url(page)
                    if not resolved:
                        await page.wait_for_timeout(1000)
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

            # Fill whichever page we ended up on if it's an ATS form.
            # Also fill when the page URL is "unknown" but a child frame belongs
            # to a known ATS (e.g. Greenhouse embedded on employer career sites).
            if not _cancelled():
                if ats in MANUAL_ONLY_ATS:
                    pass
                elif ats != "unknown" or _frame_ats(active_page) not in (None, "unknown"):
                    if ats == "unknown":
                        ats = _frame_ats(active_page) or "unknown"
                    result["ats"] = ats
                    key = _page_key(active_page.url)
                    await _do_fill(active_page, profile, job, result)
                    filled_urls.add(key)

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
                _watch_for_ats_and_fill(active_page, profile, job, result, closed_event, filled_urls)
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
            cancel_task.cancel()
            for t in (watch_task, cancel_task):
                try:
                    await asyncio.wait_for(t, timeout=2)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

            try:
                await browser.close()
            except Exception:
                pass

            result["status"] = "cancelled" if _cancelled() else "ok"
            return result

    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


async def _do_fill(page, profile: Dict[str, Any], job: Dict[str, Any], result: Dict[str, Any]) -> None:
    """Scan and fill all form fields on the current page; update result in place."""
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

    if fields:
        try:
            actions = await fill_form(fill_target, fields, profile, job)
            result["filled"] += sum(
                1 for a in actions if a["action"] in ("filled", "checked", "selected")
            )
            result["skipped"] += sum(1 for a in actions if a["action"] == "skipped")
            result["errors"] += sum(1 for a in actions if a["action"] == "error")
        except Exception:
            pass

    try:
        await try_upload_resume(fill_target, profile, job)
    except Exception:
        pass


async def _watch_for_ats_and_fill(
    page,
    profile: Dict[str, Any],
    job: Dict[str, Any],
    result: Dict[str, Any],
    closed_event: asyncio.Event,
    filled_urls: Set[str],
    poll_interval: float = 1.5,
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
            await _do_fill(page, profile, job, result)
            filled_urls.add(key)
        except Exception:
            pass


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
