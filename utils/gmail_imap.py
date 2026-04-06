"""
Gmail IMAP helper for auto-filling Greenhouse security codes.

Greenhouse sends a one-time security code to the applicant's email address
when a suspicious submission is detected.  This module polls Gmail via IMAP
and returns the code so the prefill session can fill it automatically.

Setup (one-time):
  1. Enable IMAP in Gmail Settings → See All Settings → Forwarding and POP/IMAP.
  2. Create an App Password at https://myaccount.google.com/apppasswords
     (requires 2-Step Verification to be enabled on the account).
  3. Add to profile.yaml:
       credentials:
         gmail:
           app_password: "xxxx xxxx xxxx xxxx"
"""
from __future__ import annotations

import asyncio
import email as _email_mod
import imaplib
import re
import time
from typing import Any


_IMAP_HOST = "imap.gmail.com"
_IMAP_PORT = 993

_SENDER = "no-reply@us.greenhouse-mail.io"
_SUBJECT_PREFIX = "Security code for your application to"

# Code format: 4–16 alphanumeric characters on a line by itself.
_CODE_RE = re.compile(r"^[A-Za-z0-9]{4,16}$")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_greenhouse_security_code(
    gmail_address: str,
    app_password: str,
    *,
    received_after: float | None = None,
    poll_interval: float = 5.0,
    timeout: float = 180.0,
) -> str | None:
    """Poll Gmail IMAP for a Greenhouse security-code email.

    Parameters
    ----------
    gmail_address:   Gmail address to log in with.
    app_password:    Gmail App Password (spaces are stripped automatically).
    received_after:  Unix timestamp; ignore emails older than this.
                     Defaults to the time this function is called.
    poll_interval:   Seconds between IMAP checks.
    timeout:         Give up after this many seconds.

    Returns the code string (e.g. ``"8126wzFp"``) or ``None`` on timeout.
    """
    received_after = received_after or time.time()
    deadline = time.monotonic() + timeout
    app_password = app_password.replace(" ", "")  # strip spaces from App Password

    while time.monotonic() < deadline:
        try:
            code = await asyncio.to_thread(
                _fetch_code_sync, gmail_address, app_password, received_after
            )
            if code:
                return code
        except Exception:
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        await asyncio.sleep(min(poll_interval, remaining))

    return None


# ---------------------------------------------------------------------------
# Synchronous IMAP implementation (run in a thread via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _fetch_code_sync(
    gmail_address: str,
    app_password: str,
    received_after: float,
) -> str | None:
    """Connect to Gmail IMAP and return the latest Greenhouse security code, or None."""
    with imaplib.IMAP4_SSL(_IMAP_HOST, _IMAP_PORT) as imap:
        imap.login(gmail_address, app_password)
        imap.select("INBOX")

        # Search for unread emails from Greenhouse with the expected subject.
        _, data = imap.search(
            None,
            f'(FROM "{_SENDER}" SUBJECT "{_SUBJECT_PREFIX}" UNSEEN)'
        )
        msg_ids = (data[0] or b"").split()
        if not msg_ids:
            return None

        # Check candidates from newest to oldest.
        for msg_id in reversed(msg_ids):
            _, msg_data = imap.fetch(msg_id, "(RFC822)")
            if not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            if not isinstance(raw, bytes):
                continue

            msg = _email_mod.message_from_bytes(raw)

            # Skip emails older than received_after.
            if not _email_is_recent(msg, received_after):
                continue

            body = _extract_text_body(msg)
            code = _parse_code(body)
            if code:
                # Mark as seen so we don't re-process.
                imap.store(msg_id, "+FLAGS", "\\Seen")
                return code

    return None


def _email_is_recent(msg: Any, received_after: float) -> bool:
    """Return True if the email's Date header is at or after received_after."""
    from email.utils import parsedate_to_datetime
    date_str = msg.get("Date", "")
    if not date_str:
        return True  # Can't tell — assume recent.
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.timestamp() >= received_after - 30  # 30s grace period
    except Exception:
        return True


def _extract_text_body(msg: Any) -> str:
    """Return the plain-text body of an email message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def _parse_code(body: str) -> str | None:
    """Extract the security code from the email body.

    Greenhouse emails look like::

        Copy and paste this code into the security code field on your application:

        8126wzFp

    Strategy:
    1. Find the line after the instruction line.
    2. Fallback: any standalone alphanumeric line 4–16 chars.
    """
    lines = body.splitlines()

    # Strategy 1 — line immediately after the copy/paste instruction.
    for i, line in enumerate(lines):
        if "copy and paste" in line.lower() or "security code field" in line.lower():
            # Scan forward for the first non-empty line.
            for candidate in lines[i + 1:]:
                stripped = candidate.strip()
                if stripped and _CODE_RE.match(stripped):
                    return stripped
                if stripped:
                    break  # non-matching non-empty line — stop scanning

    # Strategy 2 — any standalone alphanumeric line 6–16 chars.
    for line in lines:
        stripped = line.strip()
        if len(stripped) >= 6 and _CODE_RE.match(stripped):
            return stripped

    return None
