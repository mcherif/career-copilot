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

logger = setup_logger("lever_connector")

BASE_URL = "https://api.lever.co/v0/postings"
_SLUG_RE = re.compile(r"lever\.co/([^/?#]+)")


def _extract_slug(url: str) -> str | None:
    m = _SLUG_RE.search(url)
    return m.group(1).lower() if m else None


def _load_slugs_from_db() -> Set[str]:
    slugs: Set[str] = set()
    try:
        engine = create_engine(config.DATABASE_URL)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT url FROM jobs WHERE ats_type = 'lever' AND url LIKE '%lever.co%'")
            )
            for (url,) in rows:
                slug = _extract_slug(url or "")
                if slug:
                    slugs.add(slug)
    except Exception as e:
        logger.warning(f"Could not query DB for Lever slugs: {e}")
    return slugs


def _load_slugs_from_profile() -> Set[str]:
    try:
        with open("profile.yaml", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
        entries = profile.get("target_companies", {}).get("lever", [])
        return {str(s).lower() for s in entries}
    except Exception:
        return set()


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
    # Lever stores locations as a list of strings
    locations = job.get("categories", {}).get("location") or ""
    if isinstance(locations, str):
        locations = [locations]
    elif not isinstance(locations, list):
        locations = []
    for loc in locations:
        loc_lower = loc.lower()
        if "remote" in loc_lower or "anywhere" in loc_lower or "worldwide" in loc_lower:
            return True
    # Also check commitment / workplaceType field
    commitment = (job.get("categories", {}).get("commitment") or "").lower()
    if "remote" in commitment:
        return True
    return False


class LeverConnector(BaseConnector):
    def __init__(self):
        self.source_name = "lever"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        slugs = _load_slugs_from_db() | _load_slugs_from_profile()

        if not slugs:
            logger.info("No Lever slugs found — skipping")
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
                logger.error(f"Error fetching Lever slug '{slug}': {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def _fetch_company(
        self, slug: str, target_roles: List[str], seen_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        response = requests.get(
            f"{BASE_URL}/{slug}",
            params={"mode": "json"},
            timeout=15,
        )
        if response.status_code == 404:
            logger.debug(f"Lever slug '{slug}' returned 404 — skipping")
            return []
        response.raise_for_status()

        data = response.json()
        # Lever returns either a list directly or {"data": [...]}
        jobs = data if isinstance(data, list) else data.get("data", [])

        results = []
        for job in jobs:
            if not _is_remote(job):
                continue
            title = job.get("text", "")
            if not _title_is_relevant(title, target_roles):
                continue
            job_id = job.get("id", "")
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                job["_slug"] = slug
                results.append(job)

        return results

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        slug = raw_job.get("_slug", "")
        job_id = raw_job.get("id", "")
        url = raw_job.get("hostedUrl", "") or f"https://jobs.lever.co/{slug}/{job_id}"

        categories = raw_job.get("categories", {})
        location = categories.get("location") or categories.get("allLocations", ["Remote"])[0] if categories.get("allLocations") else "Remote"
        if isinstance(location, list):
            location = location[0] if location else "Remote"

        # Lever description: descriptionPlain > description > lists joined
        description = (
            raw_job.get("descriptionPlain")
            or raw_job.get("description")
            or ""
        )
        if not description:
            # Fall back to joining list sections
            lists = raw_job.get("lists", [])
            parts = [item.get("content", "") for item in lists if item.get("content")]
            description = "\n".join(parts)

        posted_date = None
        created_at = raw_job.get("createdAt")
        if created_at:
            try:
                posted_date = datetime.fromtimestamp(int(created_at) / 1000, tz=timezone.utc)
            except Exception:
                pass

        company = slug.replace("-", " ").title()

        return {
            "external_id": f"lever_{job_id}",
            "source": self.source_name,
            "company": company,
            "title": raw_job.get("text", ""),
            "location": str(location),
            "raw_location_text": str(location),
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": posted_date,
            "remote_eligibility": "accept",
        }

    def get_source_name(self) -> str:
        return self.source_name
