"""
Flexa Careers connector.

Fetches job listings from https://flexa.careers via their GraphQL API
and JSON-LD structured data on individual job pages.

Strategy
--------
1. Query the GraphQL API with ``jobs(limit=N, sort: DATE_DESC)`` to retrieve
   the most recent listings with title, location, and company name.
2. Filter to engineering-relevant jobs by title keyword.
3. For each filtered job fetch the individual Flexa job page and extract the
   ``JobPosting`` JSON-LD block (which contains the full HTML description and
   ``datePosted`` / ``validThrough`` fields).
4. Skip postings whose ``validThrough`` date has already passed.
5. Return the Flexa page URL as the job URL — the prefill system will open it,
   find the employer apply link via ``extract_apply_url``, and navigate to the
   real ATS.
"""
from __future__ import annotations

import json
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("flexa_connector")

_GRAPHQL_URL = "https://flexa.careers/api/graphql"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
_PAGE_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}

# GraphQL jobs to fetch per request (API limit unknown — 300 is conservative).
_GRAPHQL_BATCH = 300
# Max individual Flexa job pages to fetch per pipeline run.
_MAX_PAGE_FETCHES = 100
# Politeness delay between page fetches (seconds).
_FETCH_DELAY = 0.4

# Engineering-relevant keywords matched as substrings of the job title (lowercase).
_ENGINEERING_KEYWORDS = {
    "engineer", "engineering", "developer", "software", "backend", "frontend",
    "full stack", "full-stack", "devops", "sre", "platform", "infrastructure",
    "data engineer", "data scientist", "machine learning", "ml ", " ml", "ai ",
    " ai", "mlops", "python", "typescript", "golang", "rust", "java", "kotlin",
    "ios", "android", "mobile", "cloud", "kubernetes", "architect", "cto",
    "firmware", "embedded", "systems", "security", "blockchain", "web3",
    "computer vision", "deep learning", "llm", "inference",
}

_GRAPHQL_QUERY = """
{
  jobs(limit: %d, sort: DATE_DESC) {
    id
    title
    url
    location
    company { name }
  }
}
"""


class FlexaConnector(BaseConnector):
    def __init__(self):
        self.source_name = "flexa"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info("Fetching jobs from flexa.careers GraphQL API…")

        raw_jobs = _fetch_graphql_jobs(_GRAPHQL_BATCH)
        logger.info(f"GraphQL returned {len(raw_jobs)} jobs total")

        eng_jobs = [j for j in raw_jobs if _is_engineering_title(j.get("title", ""))]
        logger.info(f"{len(eng_jobs)} match engineering keywords")

        jobs: List[Dict[str, Any]] = []
        for gql_job in eng_jobs[:_MAX_PAGE_FETCHES]:
            try:
                enriched = _enrich_from_page(gql_job)
                if enriched:
                    jobs.append(enriched)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch {gql_job.get('url', '')}: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(jobs)} jobs from flexa.careers")
        return jobs

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = raw_job.get("location") or "Worldwide"

        return {
            "external_id": raw_job.get("id") or url,
            "source": self.source_name,
            "company": raw_job.get("company", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": location,
            "raw_location_text": location,
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            # flexa.careers is a listing page; ats_type resolved at prefill time.
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_graphql_jobs(limit: int) -> List[Dict[str, Any]]:
    """Query the Flexa GraphQL API and return raw job dicts."""
    payload = {"query": _GRAPHQL_QUERY % limit}
    try:
        resp = requests.post(_GRAPHQL_URL, json=payload, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        jobs = (data.get("data") or {}).get("jobs") or []
        return [j for j in jobs if isinstance(j, dict)]
    except Exception as e:
        logger.error(f"GraphQL request failed: {e}")
        logger.debug(traceback.format_exc())
        return []


def _is_engineering_title(title: str) -> bool:
    """Return True if the job title contains an engineering-relevant keyword."""
    t = title.lower()
    return any(kw in t for kw in _ENGINEERING_KEYWORDS)


def _enrich_from_page(gql_job: Dict[str, Any]) -> Dict[str, Any] | None:
    """Fetch the Flexa job page and merge JSON-LD data into the GraphQL job dict."""
    url = (gql_job.get("url") or "").strip()
    if not url:
        return None

    resp = requests.get(url, headers=_PAGE_HEADERS, timeout=15)
    resp.raise_for_status()

    jsonld = _extract_jsonld(resp.text)

    # Skip expired postings.
    if jsonld:
        valid_through = jsonld.get("validThrough")
        if valid_through:
            try:
                vt = dateutil_parser.parse(valid_through)
                if vt.tzinfo is None:
                    vt = vt.replace(tzinfo=timezone.utc)
                if vt < datetime.now(tz=timezone.utc):
                    return None
            except Exception:
                pass

    posted_date = None
    date_str = (jsonld or {}).get("datePosted")
    if date_str:
        try:
            posted_date = dateutil_parser.parse(date_str)
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    description = (jsonld or {}).get("description", "")
    company_name = (
        (gql_job.get("company") or {}).get("name")
        or ((jsonld or {}).get("hiringOrganization") or {}).get("name")
        or "Unknown"
    )
    location = (
        gql_job.get("location")
        or ((jsonld or {}).get("jobLocation") or {}).get("address")
        or "Worldwide"
    )

    return {
        "id": gql_job.get("id", ""),
        "url": url,
        "title": gql_job.get("title", ""),
        "company": company_name,
        "location": location,
        "description": description,
        "posted_date": posted_date,
    }


def _extract_jsonld(html: str) -> Dict[str, Any] | None:
    """Parse the first JobPosting JSON-LD block from page HTML."""
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(match.group(1).strip())
        except Exception:
            continue
        if data.get("@type") == "JobPosting":
            return data
    return None
