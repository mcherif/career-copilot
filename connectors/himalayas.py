import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import requests

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("himalayas_connector")

BASE_URL = "https://himalayas.app/jobs/api/search"
MAX_PAGES = 10
MAX_AGE_DAYS = 10


class HimalayasConnector(BaseConnector):
    def __init__(self):
        self.source_name = "himalayas"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        all_jobs: List[Dict[str, Any]] = []
        seen_guids: set = set()
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        page = 1

        try:
            while page <= MAX_PAGES:
                response = requests.get(
                    BASE_URL,
                    params={"worldwide": "true", "page": page},
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                jobs = data.get("jobs", [])
                if not jobs:
                    break

                stop_early = False
                for job in jobs:
                    pub = job.get("pubDate")
                    if pub:
                        try:
                            posted = datetime.fromtimestamp(int(pub), tz=timezone.utc)
                            if posted < cutoff:
                                stop_early = True
                                continue
                        except Exception:
                            pass

                    guid = job.get("guid") or job.get("applicationLink", "")
                    if guid and guid not in seen_guids:
                        seen_guids.add(guid)
                        all_jobs.append(job)

                if stop_early:
                    break

                total = data.get("totalCount", 0)
                if page * 20 >= total:
                    break
                page += 1

        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        pub = raw_job.get("pubDate")
        posted_date = None
        if pub:
            try:
                posted_date = datetime.fromtimestamp(int(pub), tz=timezone.utc)
            except Exception:
                pass

        app_link = raw_job.get("applicationLink", "")
        url = app_link if app_link else ""

        description = raw_job.get("description") or raw_job.get("excerpt") or ""

        return {
            "external_id": raw_job.get("guid") or app_link,
            "source": self.source_name,
            "company": raw_job.get("companyName", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": "Remote",
            "raw_location_text": "Remote",
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": posted_date,
            "remote_eligibility": "accept",  # worldwide filter guarantees this
        }

    def get_source_name(self) -> str:
        return self.source_name
