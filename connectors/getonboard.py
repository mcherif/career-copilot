import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import requests

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("getonboard_connector")

# Tech-relevant categories only — excludes sales, marketing, HR, customer support, etc.
CATEGORIES = [
    "programming",
    "sysadmin-devops-qa",
    "data-science-analytics",
    "machine-learning-ai",
    "mobile-developer",
    "cybersecurity",
    "hardware-electronics",
]

BASE_URL = "https://www.getonbrd.com/api/v0"
MAX_PAGES = 3
MAX_AGE_DAYS = 10  # Stop paginating once jobs are older than this


class GetOnBoardConnector(BaseConnector):
    def __init__(self):
        self.source_name = "getonboard"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} API...")
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for category in CATEGORIES:
            try:
                category_jobs = self._fetch_category(category, seen_ids)
                all_jobs.extend(category_jobs)
            except Exception as e:
                logger.error(f"Error fetching category '{category}' from {self.source_name}: {e}")
                logger.debug(traceback.format_exc())

        logger.info(
            f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}"
        )
        return all_jobs

    def _fetch_category(
        self, category: str, seen_ids: set
    ) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        page = 1
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)

        while page <= MAX_PAGES:
            response = requests.get(
                f"{BASE_URL}/categories/{category}/jobs",
                params={
                    "remote": "true",
                    "per_page": 100,
                    "page": page,
                    "expand[]": "company",
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()

            page_jobs = data.get("data", [])
            if not page_jobs:
                break

            stop_early = False
            for job in page_jobs:
                published_at = job.get("attributes", {}).get("published_at")
                if published_at:
                    try:
                        if datetime.fromtimestamp(int(published_at), tz=timezone.utc) < cutoff:
                            stop_early = True
                            continue
                    except Exception:
                        pass
                remote_modality = job.get("attributes", {}).get("remote_modality", "")
                if remote_modality in ("hybrid", "no_remote"):
                    continue
                job_id = job.get("id")
                if job_id and job_id not in seen_ids:
                    seen_ids.add(job_id)
                    jobs.append(job)

            if stop_early:
                break

            meta = data.get("meta", {})
            total_pages = meta.get("total_pages", 1)
            if page >= total_pages:
                break

            page += 1

        return jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        job_id = raw_job.get("id", "")
        attrs = raw_job.get("attributes", {})

        # URL from links.public_url
        url = raw_job.get("links", {}).get("public_url", "")
        if not url and job_id:
            url = f"https://www.getonbrd.com/jobs/{job_id}"

        # Company name — present when expand[]=company was used
        company = "Unknown"
        company_data = attrs.get("company", {})
        if isinstance(company_data, dict):
            inner = company_data.get("data", {})
            if isinstance(inner, dict):
                company_attrs = inner.get("attributes", {})
                company = company_attrs.get("name", "Unknown") or "Unknown"

        # Location: use countries list, fall back to "Remote"
        countries = attrs.get("countries", [])
        if countries and countries != ["Remote"]:
            location = ", ".join(str(c) for c in countries)
        else:
            location = "Remote"

        # Description: combine description + functions + projects for full context
        description_parts = [
            attrs.get("description", ""),
            attrs.get("functions", ""),
            attrs.get("projects", ""),
        ]
        description = "\n".join(p for p in description_parts if p)

        # posted_date: published_at is a Unix timestamp
        posted_date = None
        published_at = attrs.get("published_at")
        if published_at:
            try:
                posted_date = datetime.fromtimestamp(int(published_at), tz=timezone.utc)
            except Exception:
                pass

        return {
            "external_id": str(job_id),
            "source": self.source_name,
            "company": company,
            "title": attrs.get("title", ""),
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
