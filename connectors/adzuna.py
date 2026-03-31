import os
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

load_dotenv()

logger = setup_logger("adzuna_connector")

BASE_URL = "https://api.adzuna.com/v1/api/jobs"
MAX_AGE_DAYS = 10
RESULTS_PER_PAGE = 50
MAX_PAGES = 5

# Countries with active tech job markets that allow remote work.
# Querying multiple countries maximises worldwide coverage since Adzuna
# has no single global endpoint.
COUNTRIES = ["gb", "de", "fr", "nl", "at", "be", "au", "ca"]

# Tech roles we're searching for — sent as `what_or` so any match qualifies.
WHAT_OR = (
    "software engineer developer python java javascript typescript "
    "data engineer ml engineer machine learning backend frontend fullstack "
    "devops platform cloud site reliability"
)


def _is_remote(job: Dict[str, Any]) -> bool:
    """Return True if the job location or title suggests fully remote."""
    location = (job.get("location", {}).get("display_name") or "").lower()
    title = (job.get("title") or "").lower()
    description = (job.get("description") or "").lower()

    if "remote" in location:
        return True
    if "remote" in title:
        return True
    # Description snippet often contains "remote" for remote-friendly roles
    if "remote" in description and "not remote" not in description and "no remote" not in description:
        return True
    return False


class AdzunaConnector(BaseConnector):
    def __init__(self):
        self.source_name = "adzuna"
        self.app_id = os.getenv("ADZUNA_APP_ID", "")
        self.app_key = os.getenv("ADZUNA_APP_KEY", "")

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        if not self.app_id or not self.app_key:
            logger.error("ADZUNA_APP_ID / ADZUNA_APP_KEY not set in .env — skipping")
            return []

        logger.info(f"Fetching jobs from {self.source_name} API...")
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set = set()
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)

        for country in COUNTRIES:
            try:
                country_jobs = self._fetch_country(country, seen_ids, cutoff)
                all_jobs.extend(country_jobs)
            except Exception as e:
                logger.error(f"Error fetching country '{country}': {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def _fetch_country(
        self, country: str, seen_ids: set, cutoff: datetime
    ) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            response = requests.get(
                f"{BASE_URL}/{country}/search/{page}",
                params={
                    "app_id": self.app_id,
                    "app_key": self.app_key,
                    "results_per_page": RESULTS_PER_PAGE,
                    "what_or": WHAT_OR,
                    "sort_by": "date",
                    "max_days_old": MAX_AGE_DAYS,
                    "content-type": "application/json",
                },
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            if not results:
                break

            stop_early = False
            for job in results:
                created = job.get("created")
                if created:
                    try:
                        posted = datetime.fromisoformat(created.replace("Z", "+00:00"))
                        if posted < cutoff:
                            stop_early = True
                            continue
                    except Exception:
                        pass

                if not _is_remote(job):
                    continue

                job_id = str(job.get("id", ""))
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    job["_country"] = country
                    jobs.append(job)

            if stop_early:
                break

        return jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        job_id = str(raw_job.get("id", ""))
        url = raw_job.get("redirect_url", "")

        location_obj = raw_job.get("location", {})
        location = location_obj.get("display_name", "Remote")
        country = raw_job.get("_country", "")

        posted_date = None
        created = raw_job.get("created")
        if created:
            try:
                posted_date = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except Exception:
                pass

        company_obj = raw_job.get("company", {})
        company = company_obj.get("display_name", "Unknown") if isinstance(company_obj, dict) else "Unknown"

        description = raw_job.get("description", "")

        return {
            "external_id": f"adzuna_{country}_{job_id}",
            "source": self.source_name,
            "company": company,
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
