"""Tests for utils/form_prefill.py — Playwright prefill session logic."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from utils.form_prefill import is_system_browser_domain, run_prefill_session

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

JOB = {"id": 1, "title": "SWE", "company": "Acme", "url": "https://jobs.ashbyhq.com/acme/123"}
PROFILE = {"name": "Jane Doe", "email": "jane@example.com"}


def _make_page(url="https://jobs.ashbyhq.com/acme/123/application"):
    page = AsyncMock()
    page.url = url
    page.goto = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    _callbacks = {}

    def _on(event, cb):
        _callbacks.setdefault(event, []).append(cb)

    page.on = MagicMock(side_effect=_on)
    page._callbacks = _callbacks
    return page


def _make_context(page):
    """Return a mock BrowserContext that owns *page* and records callbacks."""
    ctx = MagicMock()
    ctx.new_page = AsyncMock(return_value=page)
    ctx.storage_state = AsyncMock()
    ctx.pages = [page]
    _callbacks = {}

    def _on(event, cb):
        _callbacks.setdefault(event, []).append(cb)

    ctx.on = MagicMock(side_effect=_on)
    ctx._callbacks = _callbacks
    # Give page a back-reference so try_click_apply can reach the context.
    page.context = ctx
    return ctx


def _make_browser(ctx):
    """Return a mock Browser that returns *ctx* from new_context()."""
    browser = AsyncMock()
    browser.new_context = AsyncMock(return_value=ctx)
    browser.close = AsyncMock()
    _callbacks = {}

    def _on(event, cb):
        _callbacks.setdefault(event, []).append(cb)

    browser.on = MagicMock(side_effect=_on)
    browser._callbacks = _callbacks
    return browser


def _make_pw_context(browser):
    """Return a mock async_playwright() context manager."""
    pw = MagicMock()
    pw.chromium = MagicMock()
    pw.chromium.launch = AsyncMock(return_value=browser)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=pw)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _fire_close(page):
    """Trigger all 'close' callbacks registered on page."""
    for cb in page._callbacks.get("close", []):
        cb()


# ---------------------------------------------------------------------------
# is_system_browser_domain
# ---------------------------------------------------------------------------

def test_is_system_browser_domain_true():
    assert is_system_browser_domain("https://remoteok.com/jobs/123") is True


def test_is_system_browser_domain_wwr():
    assert is_system_browser_domain("https://weworkremotely.com/remote-jobs/123") is True


def test_is_system_browser_domain_false():
    assert is_system_browser_domain("https://jobs.ashbyhq.com/acme/123") is False


def test_is_system_browser_domain_empty():
    assert is_system_browser_domain("") is False


def test_is_system_browser_domain_none():
    assert is_system_browser_domain(None) is False


# ---------------------------------------------------------------------------
# run_prefill_session — no URL / system browser
# ---------------------------------------------------------------------------

def test_no_url_returns_failed():
    result = asyncio.run(run_prefill_session({}, PROFILE))
    assert result["status"] == "failed"
    assert "No URL" in result["error"]


def test_empty_url_returns_failed():
    result = asyncio.run(run_prefill_session({"url": ""}, PROFILE))
    assert result["status"] == "failed"


def test_system_browser_domain_returns_manual():
    job = {"url": "https://remoteok.com/jobs/123"}
    result = asyncio.run(run_prefill_session(job, PROFILE))
    assert result["status"] == "manual"
    assert "reason" in result


# ---------------------------------------------------------------------------
# run_prefill_session — page load failure
# ---------------------------------------------------------------------------

def test_page_load_failure():
    page = _make_page()
    page.goto.side_effect = Exception("timeout")
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
         patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
         patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
         patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
         patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
         patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
         patch("utils.form_prefill.detect_ats", return_value="ashby"), \
         patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):

        result = asyncio.run(run_prefill_session(JOB, PROFILE))

    assert result["status"] == "failed"
    assert "Page load failed" in result["error"]


# ---------------------------------------------------------------------------
# run_prefill_session — successful fill
# ---------------------------------------------------------------------------

def _run_with_close(job, profile, page, browser, pw_cm, *, extra_patches=None):
    """Run the session while scheduling a close event so it doesn't hang."""
    patches = {
        "utils.form_prefill.async_playwright": pw_cm,
        "utils.form_prefill.extract_apply_url": AsyncMock(return_value=None),
        "utils.form_prefill.try_click_apply": AsyncMock(return_value=(False, page)),
        "utils.form_prefill.scan_fields": AsyncMock(return_value=[]),
        "utils.form_prefill.fill_form": AsyncMock(return_value=[]),
        "utils.form_prefill.try_upload_resume": AsyncMock(),
        "utils.form_prefill.detect_ats": MagicMock(return_value="ashby"),
    }
    if extra_patches:
        patches.update(extra_patches)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=patches["utils.form_prefill.extract_apply_url"].return_value), \
             patch("utils.form_prefill.try_click_apply", return_value=patches["utils.form_prefill.try_click_apply"].return_value), \
             patch("utils.form_prefill.scan_fields", return_value=patches["utils.form_prefill.scan_fields"].return_value), \
             patch("utils.form_prefill.fill_form", return_value=patches["utils.form_prefill.fill_form"].return_value), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", patches["utils.form_prefill.detect_ats"]):
            return await run_prefill_session(job, profile, wait_timeout=5)

    return asyncio.run(_run())


def test_successful_fill_returns_ok():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    actions = [
        {"action": "filled"}, {"action": "filled"},
        {"action": "skipped"}, {"action": "error"},
    ]

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=["f1", "f2"]), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=actions), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    assert result["filled"] == 2
    assert result["skipped"] == 1
    assert result["errors"] == 1
    assert result["ats"] == "ashby"


def test_manual_only_ats_skips_fill():
    page = _make_page(url="https://jobs.workday.com/acme/123")
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)
    fill_mock = AsyncMock(return_value=[])
    scan_mock = AsyncMock(return_value=["f1"])

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", scan_mock), \
             patch("utils.form_prefill.fill_form", fill_mock), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="workday"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    scan_mock.assert_not_called()
    fill_mock.assert_not_called()
    assert result["filled"] == 0


def test_apply_button_clicked_triggers_timeout_wait():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(True, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    # _wait_for_spa is called after initial nav and after click; each uses 2000 ms.
    page.wait_for_timeout.assert_any_call(2000)
    assert page.wait_for_timeout.call_count >= 2


def test_extract_apply_url_navigates_to_resolved():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)
    resolved_url = "https://jobs.ashbyhq.com/acme/123/application"

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        # First detect_ats call is for the listing page (unknown); subsequent
        # calls are for the resolved Ashby URL.
        _ats_seq = iter(["unknown"] + ["ashby"] * 10)
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=resolved_url), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", side_effect=lambda _: next(_ats_seq)), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    # goto called at least twice: original URL + resolved URL
    assert page.goto.call_count >= 2


def test_extract_apply_url_same_as_original_skips_second_goto():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=JOB["url"]), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    assert page.goto.call_count == 1  # only the initial navigation


def test_scan_fields_exception_handled():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", side_effect=Exception("scan crash")), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    assert result["filled"] == 0


def test_fill_form_exception_handled():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=["f1"]), \
             patch("utils.form_prefill.fill_form", side_effect=Exception("fill crash")), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    assert result["filled"] == 0


def test_resume_upload_exception_handled():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.fill_form", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.try_upload_resume", side_effect=Exception("upload crash")), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"


def test_no_fields_detected_skips_fill():
    page = _make_page()
    ctx = _make_context(page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)
    fill_mock = AsyncMock(return_value=[])

    async def _run():
        async def _close_soon():
            await asyncio.sleep(0.01)
            _fire_close(page)

        asyncio.create_task(_close_soon())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, page)), \
             patch("utils.form_prefill.scan_fields", new_callable=AsyncMock, return_value=[]), \
             patch("utils.form_prefill.fill_form", fill_mock), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", return_value="ashby"), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    fill_mock.assert_not_called()


def test_watcher_fills_after_user_navigates_to_ats():
    """_watch_for_ats_and_fill should fill the form when the page navigates to
    an ATS URL that was not yet in filled_ats (e.g. user manually clicked Apply)."""
    from utils.form_prefill import _watch_for_ats_and_fill

    page = _make_page(url="https://euremotejobs.com/job/some-job/")
    fill_mock = AsyncMock(return_value=[{"action": "filled"}, {"action": "filled"}])
    scan_mock = AsyncMock(return_value=["f1", "f2"])

    # First detect_ats call returns "unknown"; subsequent ones return "ashby"
    # (simulating the user navigating from the listing to the ATS form).
    detect_calls = [0]

    def _detect(url):
        detect_calls[0] += 1
        return "unknown" if detect_calls[0] <= 1 else "ashby"

    result: dict = {"filled": 0, "skipped": 0, "errors": 0}
    filled_urls: set = set()
    closed_event = asyncio.Event()

    async def _run():
        async def _close_later():
            await asyncio.sleep(0.15)
            closed_event.set()

        asyncio.create_task(_close_later())
        with patch("utils.form_prefill.scan_fields", scan_mock), \
             patch("utils.form_prefill.fill_form", fill_mock), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", side_effect=_detect):
            await _watch_for_ats_and_fill(
                page, PROFILE, JOB, result, closed_event, filled_urls, poll_interval=0.05
            )

    asyncio.run(_run())
    assert result["filled"] == 2
    assert result["ats"] == "ashby"
    scan_mock.assert_called()
    fill_mock.assert_called()


def test_new_tab_triggers_fill():
    """When the user clicks a target=_blank Apply link, a new tab opens.
    The context.on('page') handler should detect the ATS URL and fill it."""
    # Listing page — automation stays here, user opens Ashby in a new tab.
    listing_page = _make_page(url="https://euremotejobs.com/job/some-job/")
    # The new Ashby tab that opens when the user clicks Apply.
    ashby_tab = _make_page(url="https://jobs.ashbyhq.com/acme/123/application")
    ctx = _make_context(listing_page)
    browser = _make_browser(ctx)
    pw_cm = _make_pw_context(browser)

    fill_mock = AsyncMock(return_value=[{"action": "filled"}, {"action": "filled"}])
    scan_mock = AsyncMock(return_value=["f1", "f2"])

    def _detect(url):
        if "ashbyhq" in url:
            return "ashby"
        return "unknown"

    async def _run():
        async def _simulate_new_tab():
            # Give the session time to register the context.on("page") handler,
            # then fire it with the new Ashby tab.
            await asyncio.sleep(0.05)
            for cb in ctx._callbacks.get("page", []):
                cb(ashby_tab)
            # Close shortly after so the session ends.
            await asyncio.sleep(0.2)
            _fire_close(listing_page)

        asyncio.create_task(_simulate_new_tab())
        with patch("utils.form_prefill.async_playwright", return_value=pw_cm), \
             patch("utils.form_prefill.extract_apply_url", new_callable=AsyncMock, return_value=None), \
             patch("utils.form_prefill.try_click_apply", new_callable=AsyncMock, return_value=(False, listing_page)), \
             patch("utils.form_prefill.scan_fields", scan_mock), \
             patch("utils.form_prefill.fill_form", fill_mock), \
             patch("utils.form_prefill.try_upload_resume", new_callable=AsyncMock), \
             patch("utils.form_prefill.detect_ats", side_effect=_detect), \
             patch("utils.site_login.try_site_login", new_callable=AsyncMock, return_value=False):
            return await run_prefill_session(JOB, PROFILE, wait_timeout=5)

    result = asyncio.run(_run())
    assert result["status"] == "ok"
    assert result["filled"] == 2
    assert result["ats"] == "ashby"
    scan_mock.assert_called()
    fill_mock.assert_called()
