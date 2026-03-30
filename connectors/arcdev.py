import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("arcdev_connector")

# Arc.dev public job search endpoint (no auth required for basic listing).
_API_URL = "https://arc.dev/api/v2/remote-jobs"


class ArcDevConnector(BaseConnector):
    def __init__(self):
        self.source_name = "arcdev"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        headers = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}
        try:
            response = requests.get(
                _API_URL,
                headers=headers,
                params={"per_page": 100},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            # Handle both {"jobs": [...]} and a bare list
            jobs = data.get("jobs") or data.get("data") or (data if isinstance(data, list) else [])
            logger.info(f"Successfully fetched {len(jobs)} jobs from {self.source_name}")
            return jobs
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())
            return []

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url") or raw_job.get("job_url") or raw_job.get("apply_url", "")

        posted_date = None
        for date_field in ("published_at", "created_at", "posted_at"):
            if raw_job.get(date_field):
                try:
                    posted_date = parser.parse(raw_job[date_field])
                    break
                except Exception:
                    pass

        location = raw_job.get("location") or raw_job.get("remote_location") or "Worldwide"
        description = raw_job.get("description") or raw_job.get("body", "")
        company = (raw_job.get("company") or {}).get("name", "") if isinstance(raw_job.get("company"), dict) \
            else raw_job.get("company_name") or raw_job.get("company", "Unknown")

        return {
            "external_id": str(raw_job.get("id") or raw_job.get("slug", "")),
            "source": self.source_name,
            "company": company or "Unknown",
            "title": raw_job.get("title") or raw_job.get("position", ""),
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
