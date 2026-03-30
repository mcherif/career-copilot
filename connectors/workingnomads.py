import traceback
import requests
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("workingnomads_connector")


class WorkingNomadsConnector(BaseConnector):
    def __init__(self):
        self.api_url = "https://www.workingnomads.com/api/exposed_jobs/"
        self.source_name = "workingnomads"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        all_jobs: List[Dict[str, Any]] = []

        try:
            response = requests.get(self.api_url, timeout=15)
            response.raise_for_status()
            jobs = response.json()

            if not isinstance(jobs, list):
                logger.error(f"Unexpected response format from {self.source_name}: expected list")
                return all_jobs

            all_jobs = jobs
            logger.info(f"Successfully fetched {len(all_jobs)} jobs from {self.source_name}")
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())

        return all_jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")

        # Derive external_id from the last path segment of the URL, falling back to title.
        external_id = ""
        if url:
            segment = url.rstrip("/").split("/")[-1]
            external_id = segment if segment else ""
        if not external_id:
            external_id = raw_job.get("title", "")[:80]

        posted_date = None
        pub_date = raw_job.get("pub_date")
        if pub_date:
            try:
                posted_date = parser.parse(str(pub_date))
            except Exception:
                pass

        location = raw_job.get("location", "Remote")
        if not location:
            location = "Remote"

        description = raw_job.get("description", "")

        return {
            "external_id": external_id,
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
