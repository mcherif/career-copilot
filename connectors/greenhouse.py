import re
import traceback
import yaml
from datetime import datetime, timezone
from typing import List, Dict, Any, Set

import requests
from sqlalchemy import create_engine, text

import config
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("greenhouse_connector")

BASE_URL = "https://boards-api.greenhouse.io/v1/boards"
_SLUG_RE = re.compile(r"greenhouse\.io/(?:boards/)?([^/?#]+)")


def _extract_slug(url: str) -> str | None:
    m = _SLUG_RE.search(url)
    return m.group(1).lower() if m else None


def _load_slugs_from_db() -> Set[str]:
    slugs: Set[str] = set()
    try:
        engine = create_engine(config.DATABASE_URL)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT url FROM jobs WHERE ats_type = 'greenhouse' AND url LIKE '%greenhouse.io%'")
            )
            for (url,) in rows:
                slug = _extract_slug(url or "")
                if slug:
                    slugs.add(slug)
    except Exception as e:
        logger.warning(f"Could not query DB for Greenhouse slugs: {e}")
    return slugs


def _load_excluded_slugs() -> Set[str]:
    """Return slugs already handled by direct_ats or belonging to blacklisted companies."""
    excluded: Set[str] = set()
    try:
        with open("profile.yaml", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
        for entry in profile.get("target_companies", []):
            if isinstance(entry, dict):
                m = _SLUG_RE.search(entry.get("careers_url", ""))
                if m:
                    excluded.add(m.group(1).lower())
        for name in profile.get("blacklisted_companies", []):
            excluded.add(str(name).strip().lower().replace(" ", "-"))
    except Exception:
        pass
    return excluded


def _load_target_roles() -> List[str]:
    try:
        with open("profile.yaml", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
        return [r.lower() for r in profile.get("target_roles", [])]
    except Exception:
        return []


def _title_is_relevant(title: str, target_roles: List[str]) -> bool:
    if not target_roles:
        return True
    title_lower = title.lower()
    for role in target_roles:
        for word in role.split():
            if len(word) > 3 and word in title_lower:
                return True
    return False


def _is_remote(job: Dict[str, Any]) -> bool:
    location = (job.get("location", {}).get("name") or "").lower()
    return "remote" in location or "anywhere" in location or "worldwide" in location


class GreenhouseConnector(BaseConnector):
    def __init__(self):
        self.source_name = "greenhouse"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        excluded = _load_excluded_slugs()
        slugs = _load_slugs_from_db() - excluded

        if not slugs:
            logger.info("No Greenhouse slugs found — skipping")
            return []

        logger.info(f"Fetching jobs from {self.source_name} for {len(slugs)} companies: {sorted(slugs)}")
        target_roles = _load_target_roles()
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()

        for slug in sorted(slugs):
            try:
                jobs = self._fetch_company(slug, target_roles, seen_ids)
                all_jobs.extend(jobs)
            except Exception as e:
                logger.error(f"Error fetching Greenhouse slug '{slug}': {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def _fetch_company(
        self, slug: str, target_roles: List[str], seen_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        response = requests.get(
            f"{BASE_URL}/{slug}/jobs",
            params={"content": "true"},
            timeout=15,
        )
        if response.status_code == 404:
            logger.debug(f"Greenhouse slug '{slug}' returned 404 — skipping")
            return []
        response.raise_for_status()

        jobs = response.json().get("jobs", [])
        results = []
        for job in jobs:
            if not _is_remote(job):
                continue
            title = job.get("title", "")
            if not _title_is_relevant(title, target_roles):
                continue
            job_id = str(job.get("id", ""))
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                job["_slug"] = slug
                results.append(job)

        return results

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        slug = raw_job.get("_slug", "")
        job_id = str(raw_job.get("id", ""))
        url = raw_job.get("absolute_url", "")

        location_obj = raw_job.get("location", {})
        location = location_obj.get("name", "Remote") if isinstance(location_obj, dict) else "Remote"

        description = raw_job.get("content", "")

        posted_date = None
        first_published = raw_job.get("first_published") or raw_job.get("updated_at")
        if first_published:
            try:
                posted_date = datetime.fromisoformat(first_published.replace("Z", "+00:00"))
            except Exception:
                pass

        company = raw_job.get("company_name") or slug.replace("-", " ").title()

        return {
            "external_id": f"greenhouse_{job_id}",
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
            "remote_eligibility": "accept",
        }

    def get_source_name(self) -> str:
        return self.source_name
