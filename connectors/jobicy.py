import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("jobicy_connector")


class JobicyConnector(BaseConnector):
    def __init__(self):
        self.api_url = "https://jobicy.com/api/v2/remote-jobs"
        self.source_name = "jobicy"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        try:
            # count=50 is the max allowed by the API.
            response = requests.get(
                self.api_url,
                params={"count": 50},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            jobs = data.get("jobs", [])
            logger.info(f"Successfully fetched {len(jobs)} jobs from {self.source_name}")
            return jobs
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())
            return []

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")

        posted_date = None
        pub_date = raw_job.get("pubDate")
        if pub_date:
            try:
                posted_date = parser.parse(pub_date)
            except Exception:
                pass

        location = raw_job.get("jobGeo") or "Worldwide"
        # Prefer full description; fall back to excerpt.
        description = raw_job.get("jobDescription") or raw_job.get("jobExcerpt", "")

        return {
            "external_id": str(raw_job.get("id", "")),
            "source": self.source_name,
            "company": raw_job.get("companyName", "Unknown"),
            "title": raw_job.get("jobTitle", ""),
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
