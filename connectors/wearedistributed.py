"""
We Are Distributed jobs connector.

Fetches job listings from https://wearedistributed.org/jobs via sitemap.xml
and JSON-LD structured data embedded on each individual job page.

Strategy
--------
1. Parse sitemap.xml to collect all ``/job/<slug>`` URLs.
2. Filter to engineering-relevant slugs (keyword substring match).
3. For each new URL fetch the page and extract the ``JobPosting`` JSON-LD block.
4. Skip postings whose ``validThrough`` date has already passed.
5. Return the wearedistributed.org page URL as the job URL — the prefill
   system will open it, find the employer apply link via ``extract_apply_url``,
   and navigate to the real ATS.
"""
from __future__ import annotations

import json
import re
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("wearedistributed_connector")

_SITEMAP_URL = "https://wearedistributed.org/sitemap.xml"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}

# Max new job pages to fetch per pipeline run (avoids hammering the server).
_MAX_NEW = 120
# Politeness delay between page fetches (seconds).
_FETCH_DELAY = 0.5

# Engineering-relevant keywords matched as substrings of the URL slug.
_ENGINEERING_KEYWORDS = {
    "developer", "engineer", "engineering", "software", "backend", "frontend",
    "fullstack", "full-stack", "devops", "sre", "platform", "infrastructure",
    "data-engineer", "data-scientist", "machine-learning", "-ml-", "-ai-",
    "mlops", "python", "typescript", "golang", "rust", "java", "kotlin",
    "ios", "android", "mobile", "cloud", "kubernetes", "architect", "cto",
    "firmware", "embedded", "systems", "security", "blockchain", "web3",
}

# Sitemap XML namespace.
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class WeAreDistributedConnector(BaseConnector):
    def __init__(self):
        self.source_name = "wearedistributed"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info("Fetching jobs from wearedistributed.org sitemap…")
        try:
            resp = requests.get(_SITEMAP_URL, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            urls = _parse_sitemap(resp.content)
        except Exception as e:
            logger.error(f"Failed to fetch wearedistributed sitemap: {e}")
            logger.debug(traceback.format_exc())
            return []

        eng_urls = [u for u in urls if _is_engineering_url(u)]
        logger.info(
            f"Sitemap: {len(urls)} job URLs total, "
            f"{len(eng_urls)} match engineering keywords"
        )

        jobs: List[Dict[str, Any]] = []
        for url in eng_urls[:_MAX_NEW]:
            try:
                raw = _fetch_job_page(url)
                if raw:
                    jobs.append(raw)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(jobs)} jobs from wearedistributed.org")
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
            # wearedistributed.org is a listing page; ats_type resolved at prefill time.
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_sitemap(content: bytes) -> List[str]:
    """Return /job/ page URLs from the sitemap, newest first."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    entries: List[tuple[str, str]] = []  # (lastmod, url)
    for url_el in root.findall("sm:url", _NS) or root.findall("url"):
        loc = (
            (url_el.findtext("sm:loc", namespaces=_NS) or url_el.findtext("loc") or "")
            .strip()
        )
        if not re.match(r"https://wearedistributed\.org/job/[^/]+$", loc):
            continue
        lastmod = (
            url_el.findtext("sm:lastmod", namespaces=_NS)
            or url_el.findtext("lastmod")
            or ""
        )
        entries.append((lastmod, loc))

    entries.sort(key=lambda x: x[0], reverse=True)
    return [loc for _, loc in entries]


def _is_engineering_url(url: str) -> bool:
    """Return True if the URL slug contains an engineering-relevant keyword."""
    slug = url.rstrip("/").split("/")[-1].lower()
    return any(kw in slug for kw in _ENGINEERING_KEYWORDS)


def _fetch_job_page(url: str) -> Dict[str, Any] | None:
    """Fetch a wearedistributed job page and return a raw job dict from its JSON-LD."""
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    return _extract_jsonld(resp.text, url)


def _extract_jsonld(html: str, page_url: str) -> Dict[str, Any] | None:
    """Parse a JobPosting JSON-LD block from page HTML and return a raw job dict."""
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(match.group(1).strip())
        except Exception:
            continue

        if data.get("@type") != "JobPosting":
            continue

        # Skip expired postings.
        valid_through = data.get("validThrough")
        if valid_through:
            try:
                vt = dateutil_parser.parse(valid_through)
                if vt.tzinfo is None:
                    vt = vt.replace(tzinfo=timezone.utc)
                if vt < datetime.now(tz=timezone.utc):
                    return None
            except Exception:
                pass

        title = (data.get("title") or "").strip()
        company = ((data.get("hiringOrganization") or {}).get("name") or "Unknown").strip()
        description = (data.get("description") or "").strip()

        # Location: prefer applicantLocationRequirements list.
        location = "Worldwide"
        loc_reqs = data.get("applicantLocationRequirements")
        if isinstance(loc_reqs, list):
            names = [r.get("name", "") for r in loc_reqs if r.get("name")]
            if names:
                location = ", ".join(names)
        elif isinstance(loc_reqs, dict) and loc_reqs.get("name"):
            location = loc_reqs["name"]

        posted_date = None
        date_str = data.get("datePosted")
        if date_str:
            try:
                posted_date = dateutil_parser.parse(date_str)
                if posted_date.tzinfo is None:
                    posted_date = posted_date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        slug = page_url.rstrip("/").split("/")[-1]
        return {
            "id": slug,
            "url": page_url,
            "title": title,
            "company": company,
            "location": location,
            "description": description,
            "posted_date": posted_date,
        }

    return None
