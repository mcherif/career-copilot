import re
import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

# ATS domains that, when found in a job description, reliably point to the
# direct application form — use these as the job URL instead of remoteok.com.
_ATS_DOMAINS = [
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "workday.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "recruitee.com",
    "jobvite.com",
    "icims.com",
    "taleo.net",
    "breezy.hr",
    "bamboohr.com",
    "apply.",
]


def _extract_ats_url(html: str) -> str | None:
    """Return the first href in html that points to a known ATS platform."""
    for href in re.findall(r'href=["\']([^"\']+)', html):
        if any(domain in href for domain in _ATS_DOMAINS):
            return href
    return None

logger = setup_logger("remoteok_connector")

# First element of the RemoteOK JSON array is always a metadata/legal object, not a job.
_SKIP_KEYS = {"legal", "api"}


class RemoteOKConnector(BaseConnector):
    def __init__(self):
        self.api_url = "https://remoteok.com/remote-jobs.json"
        self.source_name = "remoteok"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        try:
            # RemoteOK requires a browser-like User-Agent or returns 403.
            headers = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}
            response = requests.get(self.api_url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            # Filter out the metadata object (slug == "legal" or no "position" key).
            jobs = [item for item in data if isinstance(item, dict) and item.get("position")]
            logger.info(f"Successfully fetched {len(jobs)} jobs from {self.source_name}")
            return jobs
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())
            return []

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        # Try to extract a direct ATS apply URL from the description HTML so
        # the browser never has to touch remoteok.com (Google OAuth wall).
        # Fall back to the remoteok listing URL if nothing is found.
        description_html = raw_job.get("description", "")
        url = _extract_ats_url(description_html) or raw_job.get("url", "")

        posted_date = None
        date_str = raw_job.get("date")
        if date_str:
            try:
                posted_date = parser.parse(date_str)
            except Exception:
                pass

        location = raw_job.get("location") or "Worldwide"
        description = raw_job.get("description", "")

        return {
            "external_id": str(raw_job.get("id", "")),
            "source": self.source_name,
            "company": raw_job.get("company", "Unknown"),
            "title": raw_job.get("position", ""),
            "location": location,
            "raw_location_text": location,
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": posted_date,
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name
