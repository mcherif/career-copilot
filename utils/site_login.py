"""
Auto-login handlers for job board sites that require authentication
before the Apply button leads to an employer ATS.

Called from form_prefill.py when Playwright lands on a known login page.
Credentials are read from profile.yaml under the `credentials` key.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict


async def dismiss_himalayas_modal(page) -> None:
    """Dismiss the 'I'm ready to apply' interstitial modal on Himalayas.

    This modal appears after login (and sometimes before) when navigating
    to a job application page.  It must be dismissed before the ATS form loads.
    """
    try:
        ready_btn = page.get_by_role("button", name="I'm ready to apply").first
        if await ready_btn.count() > 0 and await ready_btn.is_visible(timeout=3000):
            await ready_btn.click()
            await asyncio.sleep(1)
    except Exception:
        pass


async def try_site_login(
    page,
    url: str,
    credentials: Dict[str, Any],
    log_fn=None,
) -> bool:
    """Detect and handle login pages for known job board sites.

    Returns True if a login was attempted (regardless of success),
    so the caller knows to wait for a redirect.
    """
    def _log(msg: str) -> None:
        if log_fn:
            try:
                log_fn(msg)
            except Exception:
                pass

    url_lower = (url or "").lower()

    if "himalayas.app" in url_lower and (
        "/login" in url_lower or "/signup" in url_lower
    ):
        creds = credentials.get("himalayas.app", {})
        email = (creds.get("email") or "").strip()
        password = (creds.get("password") or "").strip()
        if not email or not password or password == "FILL_IN":
            _log("Himalayas login page detected — add credentials.himalayas.app to profile.yaml")
            return False

        _log("Himalayas login page detected — logging in…")
        try:
            await _himalayas_login(page, email, password)
            return True
        except Exception as exc:
            _log(f"Himalayas login failed: {exc}")
            return False

    return False


async def _himalayas_login(page, email: str, password: str) -> None:
    """Fill and submit the Himalayas login form.

    The signup page (/signup/talent) has a 'Log in' button that toggles
    to the login form and navigates to /login.  The login page has
    email + password inputs and a 'Log in' submit button.
    """
    # If on the signup page, click 'Log in' to switch to the login form.
    if "/signup" in page.url:
        login_btn = page.get_by_role("button", name="Log in").first
        if await login_btn.count() > 0 and await login_btn.is_visible():
            await login_btn.click()
            await page.wait_for_url("**/login**", timeout=5000)

    # Fill credentials.
    await page.locator("input[name='email']").fill(email)
    await page.locator("input[name='password']").fill(password)

    # Submit — the button text is 'Log in' on the /login page.
    submit = page.get_by_role("button", name="Log in").first
    await submit.click()

    # Wait for navigation away from the login page (success) or a short timeout.
    try:
        await page.wait_for_function(
            "() => !window.location.pathname.startsWith('/login') "
            "     && !window.location.pathname.startsWith('/signup')",
            timeout=10000,
        )
    except Exception:
        # May still be on login due to wrong password — caller will log.
        pass

    await asyncio.sleep(1)
    await dismiss_himalayas_modal(page)
