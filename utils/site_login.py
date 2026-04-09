"""
Auto-login handlers for job board sites that require authentication
before the Apply button leads to an employer ATS.

Called from form_prefill.py when Playwright lands on a known login page.
Credentials are read from profile.yaml under the `credentials` key.

Session persistence
-------------------
Sites that use Google OAuth (e.g. euremote.jobcopilot.com) don't require
stored credentials — the OAuth flow is completed interactively in the
Playwright browser window.  After a successful login the browser context's
cookies and localStorage are saved to disk (SESSIONS_DIR/{domain}.json) so
that subsequent visits restore the session and skip the login gate entirely.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict

# Where per-domain session state JSON files are stored.
SESSIONS_DIR = Path.home() / ".career-copilot" / "sessions"


def session_state_path(domain: str) -> str | None:
    """Return the path to saved session state for *domain*, or None if missing."""
    p = SESSIONS_DIR / f"{domain}.json"
    return str(p) if p.exists() else None


async def save_session_state(context, domain: str) -> None:
    """Persist *context* cookies and localStorage to SESSIONS_DIR/{domain}.json."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    await context.storage_state(path=str(SESSIONS_DIR / f"{domain}.json"))


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

    if "jobcopilot.com" in url_lower and (
        "/login" in url_lower or "/signup" in url_lower
    ):
        return await _jobcopilot_google_login(page, _log)

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


async def _jobcopilot_google_login(page, _log) -> bool:
    """Handle the euremote.jobcopilot.com Google OAuth login gate.

    Clicks 'Continue with Google', then waits up to 3 minutes for the
    OAuth popup to complete and the page to navigate away from /login or
    /signup.  The caller (form_prefill.py) saves the resulting session
    state to disk so the next run restores the cookies and skips login.
    """
    _log("jobcopilot.com login required — clicking 'Continue with Google'…")
    try:
        # Try several selector strategies to find the Google button.
        for selector in [
            "text=Continue with Google",
            "button:has-text('Google')",
            "a:has-text('Google')",
            "[aria-label*='Google' i]",
        ]:
            btn = page.locator(selector).first
            if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                await btn.click()
                _log("Google sign-in window opened — complete the login in the browser…")
                break
        else:
            _log(
                "Could not find 'Continue with Google' button — "
                "log in manually in the browser window."
            )
    except Exception as exc:
        _log(f"Could not click Google button ({exc}) — log in manually.")

    # Wait up to 3 minutes for navigation away from the auth pages.
    try:
        await page.wait_for_function(
            "() => !window.location.href.includes('/login')"
            "   && !window.location.href.includes('/signup')",
            timeout=180_000,
        )
        _log("Logged in to jobcopilot.com — session will be saved for future runs.")
        return True
    except Exception:
        _log("Login not completed within 3 minutes — continuing in manual mode.")
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
