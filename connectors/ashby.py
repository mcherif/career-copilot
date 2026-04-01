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

logger = setup_logger("ashby_connector")

BASE_URL = "https://api.ashbyhq.com/posting-api/job-board"
_SLUG_RE = re.compile(r"ashbyhq\.com/([^/?#]+)")


def _extract_slug(url: str) -> str | None:
    m = _SLUG_RE.search(url)
    return m.group(1).lower() if m else None


def _load_slugs_from_db() -> Set[str]:
    """Extract unique Ashby company slugs from jobs already in the DB."""
    slugs: Set[str] = set()
    try:
        engine = create_engine(config.DATABASE_URL)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT url FROM jobs WHERE ats_type = 'ashby' AND url LIKE '%ashbyhq.com%'")
            )
            for (url,) in rows:
                slug = _extract_slug(url or "")
                if slug:
                    slugs.add(slug)
    except Exception as e:
        logger.warning(f"Could not query DB for Ashby slugs: {e}")
    return slugs


def _load_excluded_slugs() -> Set[str]:
    """Return slugs already handled by direct_ats or belonging to blacklisted companies."""
    excluded: Set[str] = set()
    try:
        with open("profile.yaml", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
        # Slugs covered by direct_ats target list
        for entry in profile.get("target_companies", []):
            if isinstance(entry, dict):
                m = _SLUG_RE.search(entry.get("careers_url", ""))
                if m:
                    excluded.add(m.group(1).lower())
        # Slugs derived from blacklisted company names
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
    """Quick relevance gate: job title must share at least one word with target roles."""
    if not target_roles:
        return True
    title_lower = title.lower()
    for role in target_roles:
        for word in role.split():
            if len(word) > 3 and word in title_lower:
                return True
    return False


class AshbyConnector(BaseConnector):
    def __init__(self):
        self.source_name = "ashby"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        excluded = _load_excluded_slugs()
        slugs = _load_slugs_from_db() - excluded

        if not slugs:
            logger.info("No Ashby company slugs found — skipping (add companies via profile.yaml or run other sources first)")
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
                logger.error(f"Error fetching Ashby slug '{slug}': {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def _fetch_company(
        self, slug: str, target_roles: List[str], seen_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        response = requests.get(f"{BASE_URL}/{slug}", timeout=15)
        if response.status_code == 404:
            logger.debug(f"Ashby slug '{slug}' returned 404 — skipping")
            return []
        response.raise_for_status()

        jobs = response.json().get("jobs", [])
        results = []
        for job in jobs:
            if (job.get("workplaceType") or "").lower() not in ("remote", ""):
                continue
            if not job.get("isRemote"):
                continue
            title = job.get("title", "")
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

        # Prefer applyUrl (direct form) — falls back to jobUrl (listing page)
        apply_url = raw_job.get("applyUrl", "")
        job_url = raw_job.get("jobUrl", "")
        url = apply_url or job_url

        location = raw_job.get("location", "Remote")
        description = raw_job.get("descriptionPlain") or raw_job.get("descriptionHtml") or ""

        posted_date = None
        published_at = raw_job.get("publishedAt")
        if published_at:
            try:
                posted_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            except Exception:
                pass

        company = raw_job.get("organizationName") or slug.replace("-", " ").title()

        return {
            "external_id": f"ashby_{raw_job.get('id', '')}",
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
            "remote_eligibility": "accept",  # workplaceType=Remote + isRemote guarantees this
        }

    def get_source_name(self) -> str:
        return self.source_name
