"""
Direct ATS connector: reads a careers_url per company from profile.yaml,
detects the ATS from the host, and calls the appropriate public API.

Supports: Ashby · Greenhouse · Lever · Workable
Custom career domains are skipped with a warning.

This avoids hardcoding company→ATS mappings that go stale when companies migrate.
"""
import re
import traceback
import yaml
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import requests

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("direct_ats_connector")

# Host → ATS type routing table
_HOST_ROUTING = {
    "jobs.ashbyhq.com":         "ashby",
    "boards.greenhouse.io":     "greenhouse",
    "job-boards.greenhouse.io": "greenhouse",
    "jobs.lever.co":            "lever",
    "apply.workable.com":       "workable",
}


def _parse_careers_url(url: str) -> tuple[str, str]:
    """Return (ats_type, slug) from a careers URL, or ('unknown', '') if unsupported."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    slug = parsed.path.strip("/").split("/")[0]
    ats = _HOST_ROUTING.get(host, "unknown")
    return ats, slug


def _load_target_companies() -> List[Dict[str, str]]:
    """Load [{name, careers_url}, ...] from profile.yaml target_companies."""
    try:
        with open("profile.yaml", encoding="utf-8") as f:
            profile = yaml.safe_load(f) or {}
        entries = profile.get("target_companies", [])
        if isinstance(entries, list):
            return [e for e in entries if isinstance(e, dict) and e.get("careers_url")]
        return []
    except Exception as e:
        logger.warning(f"Could not load target_companies from profile.yaml: {e}")
        return []


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


# ---------------------------------------------------------------------------
# Per-ATS fetchers
# ---------------------------------------------------------------------------

def _fetch_ashby(slug: str, company_name: str, target_roles: List[str]) -> List[Dict[str, Any]]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    r = requests.get(url, timeout=15)
    if r.status_code == 404:
        logger.warning(f"Ashby slug '{slug}' ({company_name}) returned 404")
        return []
    r.raise_for_status()
    jobs = r.json().get("jobs", [])
    results = []
    for job in jobs:
        if not job.get("isRemote"):
            continue
        if not _title_is_relevant(job.get("title", ""), target_roles):
            continue
        job["_company_name"] = company_name
        job["_slug"] = slug
        job["_ats"] = "ashby"
        results.append(job)
    return results


def _fetch_greenhouse(slug: str, company_name: str, target_roles: List[str]) -> List[Dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = requests.get(url, params={"content": "true"}, timeout=15)
    if r.status_code == 404:
        logger.warning(f"Greenhouse slug '{slug}' ({company_name}) returned 404")
        return []
    r.raise_for_status()
    jobs = r.json().get("jobs", [])
    results = []
    for job in jobs:
        loc = (job.get("location", {}).get("name") or "").lower()
        if not any(kw in loc for kw in ("remote", "anywhere", "worldwide")):
            continue
        if not _title_is_relevant(job.get("title", ""), target_roles):
            continue
        job["_company_name"] = company_name
        job["_slug"] = slug
        job["_ats"] = "greenhouse"
        results.append(job)
    return results


def _fetch_lever(slug: str, company_name: str, target_roles: List[str]) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{slug}"
    r = requests.get(url, params={"mode": "json"}, timeout=30)
    if r.status_code == 404:
        logger.warning(f"Lever slug '{slug}' ({company_name}) returned 404")
        return []
    r.raise_for_status()
    data = r.json()
    jobs = data if isinstance(data, list) else data.get("data", [])
    results = []
    for job in jobs:
        cats = job.get("categories", {})
        loc = (cats.get("location") or "").lower()
        commitment = (cats.get("commitment") or "").lower()
        if not any(kw in loc for kw in ("remote", "anywhere", "worldwide")) and "remote" not in commitment:
            continue
        if not _title_is_relevant(job.get("text", ""), target_roles):
            continue
        job["_company_name"] = company_name
        job["_slug"] = slug
        job["_ats"] = "lever"
        results.append(job)
    return results


def _fetch_workable(slug: str, company_name: str, target_roles: List[str]) -> List[Dict[str, Any]]:
    # The public Workable API returns one page only — nextPage token is not accepted.
    endpoint = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    r = requests.post(endpoint, json={}, timeout=15)
    if r.status_code == 404:
        logger.warning(f"Workable slug '{slug}' ({company_name}) returned 404")
        return []
    r.raise_for_status()

    results = []
    for job in r.json().get("results", []):
        if not job.get("remote"):
            continue
        if job.get("state") != "published":
            continue
        if not _title_is_relevant(job.get("title", ""), target_roles):
            continue
        job["_company_name"] = company_name
        job["_slug"] = slug
        job["_ats"] = "workable"
        results.append(job)
    return results


_FETCHERS = {
    "ashby":      _fetch_ashby,
    "greenhouse": _fetch_greenhouse,
    "lever":      _fetch_lever,
    "workable":   _fetch_workable,
}


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

def _normalize_ashby(raw: Dict[str, Any]) -> Dict[str, Any]:
    slug = raw.get("_slug", "")
    url = raw.get("applyUrl") or raw.get("jobUrl", "")
    location = raw.get("location", "Remote")
    description = raw.get("descriptionPlain") or raw.get("descriptionHtml") or ""
    posted_date = None
    published_at = raw.get("publishedAt")
    if published_at:
        try:
            posted_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except Exception:
            pass
    return {
        "external_id": f"ashby_{raw.get('id', '')}",
        "source": "direct_ats",
        "company": raw.get("_company_name") or raw.get("organizationName") or slug.title(),
        "title": raw.get("title", ""),
        "location": location,
        "raw_location_text": location,
        "description": description,
        "description_text": clean_description(description),
        "url": url,
        "ats_type": detect_ats(url),
        "posted_date": posted_date,
        "remote_eligibility": "accept",
    }


def _normalize_greenhouse(raw: Dict[str, Any]) -> Dict[str, Any]:
    slug = raw.get("_slug", "")
    job_id = str(raw.get("id", ""))
    url = raw.get("absolute_url", "")
    loc_obj = raw.get("location", {})
    location = loc_obj.get("name", "Remote") if isinstance(loc_obj, dict) else "Remote"
    description = raw.get("content", "")
    posted_date = None
    ts = raw.get("first_published") or raw.get("updated_at")
    if ts:
        try:
            posted_date = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            pass
    return {
        "external_id": f"greenhouse_{job_id}",
        "source": "direct_ats",
        "company": raw.get("_company_name") or raw.get("company_name") or slug.title(),
        "title": raw.get("title", ""),
        "location": location,
        "raw_location_text": location,
        "description": description,
        "description_text": clean_description(description),
        "url": url,
        "ats_type": detect_ats(url),
        "posted_date": posted_date,
        "remote_eligibility": "accept",
    }


def _normalize_lever(raw: Dict[str, Any]) -> Dict[str, Any]:
    slug = raw.get("_slug", "")
    job_id = raw.get("id", "")
    url = raw.get("hostedUrl") or f"https://jobs.lever.co/{slug}/{job_id}"
    cats = raw.get("categories", {})
    location = cats.get("location") or "Remote"
    if isinstance(location, list):
        location = location[0] if location else "Remote"
    description = raw.get("descriptionPlain") or raw.get("description") or ""
    posted_date = None
    created_at = raw.get("createdAt")
    if created_at:
        try:
            posted_date = datetime.fromtimestamp(int(created_at) / 1000, tz=timezone.utc)
        except Exception:
            pass
    return {
        "external_id": f"lever_{job_id}",
        "source": "direct_ats",
        "company": raw.get("_company_name") or slug.replace("-", " ").title(),
        "title": raw.get("text", ""),
        "location": str(location),
        "raw_location_text": str(location),
        "description": description,
        "description_text": clean_description(description),
        "url": url,
        "ats_type": detect_ats(url),
        "posted_date": posted_date,
        "remote_eligibility": "accept",
    }


def _normalize_workable(raw: Dict[str, Any]) -> Dict[str, Any]:
    slug = raw.get("_slug", "")
    shortcode = raw.get("shortcode", "")
    url = f"https://apply.workable.com/{slug}/j/{shortcode}"
    loc = raw.get("location", {})
    location_parts = [loc.get("city"), loc.get("country")]
    location = ", ".join(p for p in location_parts if p) or "Remote"
    posted_date = None
    published = raw.get("published")
    if published:
        try:
            posted_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except Exception:
            pass
    return {
        "external_id": f"workable_{shortcode}",
        "source": "direct_ats",
        "company": raw.get("_company_name") or slug.replace("-", " ").title(),
        "title": raw.get("title", ""),
        "location": location,
        "raw_location_text": location,
        "description": "",  # Workable listing API doesn't include description
        "description_text": "",
        "url": url,
        "ats_type": "workable",
        "posted_date": posted_date,
        "remote_eligibility": "accept",
    }


_NORMALIZERS = {
    "ashby":      _normalize_ashby,
    "greenhouse": _normalize_greenhouse,
    "lever":      _normalize_lever,
    "workable":   _normalize_workable,
}


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------

class DirectATSConnector(BaseConnector):
    def __init__(self):
        self.source_name = "direct_ats"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        companies = _load_target_companies()
        if not companies:
            logger.info("No target_companies defined in profile.yaml — skipping direct_ats")
            return []

        target_roles = _load_target_roles()
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for entry in companies:
            name = entry.get("name", "")
            careers_url = entry.get("careers_url", "")
            ats, slug = _parse_careers_url(careers_url)

            if ats == "unknown":
                logger.warning(f"Unsupported careers URL for '{name}': {careers_url} — skipping")
                continue

            fetcher = _FETCHERS[ats]
            try:
                jobs = fetcher(slug, name, target_roles)
                for job in jobs:
                    uid = job.get("id") or job.get("shortcode") or job.get("id", "")
                    dedup_key = f"{ats}_{slug}_{uid}"
                    if dedup_key not in seen_ids:
                        seen_ids.add(dedup_key)
                        all_jobs.append(job)
                if jobs:
                    logger.info(f"  {name} ({ats}/{slug}): {len(jobs)} remote jobs")
            except Exception as e:
                logger.error(f"Error fetching '{name}' ({ats}/{slug}): {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} remote jobs from {self.source_name}")
        return all_jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        ats = raw_job.get("_ats", "unknown")
        normalizer = _NORMALIZERS.get(ats)
        if not normalizer:
            return {}
        return normalizer(raw_job)

    def get_source_name(self) -> str:
        return self.source_name
