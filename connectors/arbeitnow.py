import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("arbeitnow_connector")


class ArbeitnowConnector(BaseConnector):
    def __init__(self):
        self.api_url = "https://www.arbeitnow.com/api/job-board-api"
        self.source_name = "arbeitnow"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        all_jobs: List[Dict[str, Any]] = []
        page = 1

        try:
            while True:
                response = requests.get(
                    self.api_url,
                    params={"page": page},
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                jobs = data.get("data", [])
                if not jobs:
                    break
                # Keep only remote jobs.
                remote_jobs = [j for j in jobs if j.get("remote")]
                all_jobs.extend(remote_jobs)

                # Arbeitnow paginates; stop after page 3 to avoid fetching hundreds of old jobs.
                if page >= 3 or not data.get("links", {}).get("next"):
                    break
                page += 1

            logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())

        return all_jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")

        posted_date = None
        created_at = raw_job.get("created_at")
        if created_at:
            try:
                posted_date = parser.parse(str(created_at)) if isinstance(created_at, str) else \
                              parser.parse(str(created_at))
            except Exception:
                pass

        location = raw_job.get("location", "Remote")
        if not location:
            location = "Remote"
        description = raw_job.get("description", "")

        return {
            "external_id": str(raw_job.get("slug", "")),
            "source": self.source_name,
            "company": raw_job.get("company_name", "Unknown"),
            "title": raw_job.get("title", ""),
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
