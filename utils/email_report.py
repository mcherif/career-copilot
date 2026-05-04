import os
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Any
from utils.logger import setup_logger

logger = setup_logger("email_report")


def _build_html(new_jobs: List[Dict[str, Any]], counts: Dict[str, int]) -> str:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    rows = ""
    for status in ("shortlisted", "review", "applied", "deferred", "rejected"):
        n = counts.get(status, 0)
        if status == "shortlisted":
            style = "font-weight:bold; color:#2e7d32;"
        elif status == "review":
            style = "font-weight:bold; color:#e65100;"
        else:
            style = "color:#555;"
        rows += f'<tr><td style="{style}">{status.capitalize()}</td><td style="text-align:right;{style}">{n}</td></tr>'

    new_rows = ""
    for job in new_jobs:
        status = job["status"]
        badge_color = "#2e7d32" if status == "shortlisted" else "#e65100"
        job_url = job.get("url") or ""
        title_cell = (
            f'<a href="{job_url}" style="color:#1565c0;text-decoration:none;">{job["title"]}</a>'
            if job_url else job["title"]
        )
        apply_cell = (
            f'<a href="{job_url}" '
            f'style="font-size:0.8em;color:#fff;background:#1565c0;padding:2px 7px;'
            f'border-radius:3px;text-decoration:none;">Apply</a>'
            if job_url else ""
        )
        new_rows += (
            f'<tr>'
            f'<td><span style="background:{badge_color};color:#fff;padding:2px 6px;border-radius:3px;font-size:0.85em;">{status}</span></td>'
            f'<td>{title_cell}</td>'
            f'<td>{job["company"]}</td>'
            f'<td style="text-align:center;">{job["fit_score"] or "-"}</td>'
            f'<td>{job.get("source","")}</td>'
            f'<td>{apply_cell}</td>'
            f'</tr>'
        )

    new_section = ""
    if new_rows:
        new_section = f"""
        <h3 style="color:#333;margin-top:28px;">New This Run</h3>
        <table style="width:100%;border-collapse:collapse;font-size:0.9em;">
          <thead>
            <tr style="background:#f5f5f5;">
              <th style="padding:6px 8px;text-align:left;">Status</th>
              <th style="padding:6px 8px;text-align:left;">Title</th>
              <th style="padding:6px 8px;text-align:left;">Company</th>
              <th style="padding:6px 8px;text-align:center;">Score</th>
              <th style="padding:6px 8px;text-align:left;">Source</th>
              <th style="padding:6px 8px;text-align:left;"></th>
            </tr>
          </thead>
          <tbody>{new_rows}</tbody>
        </table>
        """

    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px;color:#222;">
      <h2 style="color:#1565c0;">Career Copilot Report &mdash; {now}</h2>

      <h3 style="color:#333;">Pipeline Summary</h3>
      <table style="border-collapse:collapse;font-size:0.95em;min-width:220px;">
        <thead>
          <tr style="background:#f5f5f5;">
            <th style="padding:6px 12px;text-align:left;">Status</th>
            <th style="padding:6px 12px;text-align:right;">Total</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>

      {new_section}

      <p style="margin-top:32px;font-size:0.8em;color:#999;">
        Run: <code>python run_pipeline.py triage</code> to work through review jobs.<br>
        Run: <code>python run_pipeline.py shortlist</code> to see shortlisted jobs.
      </p>
    </body></html>
    """


_KEYRING_SERVICE = "career-copilot"


def _get_credential(key: str) -> str:
    """Read a credential from Windows Credential Manager, fall back to env var."""
    try:
        import keyring
        value = keyring.get_password(_KEYRING_SERVICE, key)
        if value:
            return value
    except Exception:
        pass
    return os.environ.get(key, "")


def send_report(new_jobs: List[Dict[str, Any]], counts: Dict[str, int]) -> bool:
    """Send the job report email. Returns True on success."""
    smtp_host = _get_credential("EMAIL_SMTP_HOST") or "smtp.gmail.com"
    smtp_port = int(_get_credential("EMAIL_SMTP_PORT") or "587")
    email_from = _get_credential("EMAIL_FROM")
    email_to = _get_credential("EMAIL_TO")
    email_password = _get_credential("EMAIL_PASSWORD")

    if not all([email_from, email_to, email_password]):
        logger.error("Email not configured — set EMAIL_FROM, EMAIL_TO, EMAIL_PASSWORD in .env")
        return False

    subject = f"Career Copilot — {counts.get('shortlisted', 0)} shortlisted, {counts.get('review', 0)} in review"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.attach(MIMEText(_build_html(new_jobs, counts), "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(email_from, email_password)
            server.sendmail(email_from, email_to, msg.as_string())
        logger.info(f"Report email sent to {email_to}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False
