import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("remotive_connector")

class RemotiveConnector(BaseConnector):
    def __init__(self):
        self.api_url = "https://remotive.com/api/remote-jobs"
        self.source_name = "remotive"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        """Fetch raw jobs from Remotive."""
        logger.info(f"Fetching jobs from {self.source_name} API...")
        try:
            response = requests.get(self.api_url, timeout=15)
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
        """Convert a raw job entry to the unified schema."""
        url = raw_job.get("url", "")
        
        # Parse publication date if possible
        posted_date = None
        pub_date_str = raw_job.get("publication_date")
        if pub_date_str:
            try:
                posted_date = parser.parse(pub_date_str)
            except Exception:
                pass

        raw_location = raw_job.get("candidate_required_location", "")
        location = raw_location if raw_location else "Unknown"

        return {
            "external_id": str(raw_job.get("id", "")),
            "source": self.source_name,
            "company": raw_job.get("company_name", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": location,
            "raw_location_text": raw_location,
            "description": raw_job.get("description", ""),
            "description_text": clean_description(raw_job.get("description", "")),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": posted_date,
            "remote_eligibility": None  # Day 2 task
        }

    def get_source_name(self) -> str:
        return self.source_name
