"""
Remote100K connector.

Fetches $100K+ remote job listings from https://remote100k.com/ via their
sitemap.xml and JSON-LD structured data embedded on each job page.

Strategy
--------
1. Parse sitemap.xml (handles both flat <urlset> and <sitemapindex>).
2. Filter to /remote-job/ URLs with engineering-relevant slug keywords.
3. For each new URL fetch the page; extract the JobPosting JSON-LD block
   for title, company, salary, and date; then scan the raw HTML for the
   direct ATS apply URL (Ashby, Greenhouse, Lever, etc.) and strip the
   site's ?ref=remote100k tracking parameter.
4. Store the ATS URL directly so detect_ats() classifies it correctly and
   the prefill system navigates straight to the application form.
"""
from __future__ import annotations

import json
import re
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from dateutil import parser as dateutil_parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("remote100k_connector")

_SITEMAP_URL = "https://remote100k.com/sitemap.xml"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}

_MAX_NEW = 150
_FETCH_DELAY = 0.4

_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# ATS domains whose URLs we extract directly from the page HTML.
_ATS_DOMAINS = (
    "jobs.ashbyhq.com",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "apply.workable.com",
    "jobs.smartrecruiters.com",
    "app.dover.com",
    "recruiting.paylocity.com",
)

_ENGINEERING_KEYWORDS = {
    "developer", "engineer", "engineering", "software", "backend", "frontend",
    "fullstack", "full-stack", "devops", "sre", "platform", "infrastructure",
    "data-engineer", "data-scientist", "machine-learning", "-ml-", "-ai-",
    "mlops", "python", "typescript", "golang", "rust", "java", "kotlin",
    "ios", "android", "mobile", "cloud", "kubernetes", "architect",
    "firmware", "embedded", "systems", "security", "blockchain", "web3",
}


class Remote100kConnector(BaseConnector):
    def __init__(self):
        self.source_name = "remote100k"

    def fetch_jobs(self) -> list[dict[str, Any]]:
        logger.info("Fetching jobs from remote100k.com sitemap…")
        try:
            resp = requests.get(_SITEMAP_URL, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            urls = _parse_sitemap(resp.content)
        except Exception as e:
            logger.error(f"Failed to fetch remote100k sitemap: {e}")
            logger.debug(traceback.format_exc())
            return []

        eng_urls = [u for u in urls if _is_engineering_url(u)]
        logger.info(
            f"Sitemap: {len(urls)} job URLs total, "
            f"{len(eng_urls)} match engineering keywords"
        )

        jobs: list[dict[str, Any]] = []
        for url in eng_urls[:_MAX_NEW]:
            try:
                raw = _fetch_job_page(url)
                if raw:
                    jobs.append(raw)
                time.sleep(_FETCH_DELAY)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(jobs)} jobs from remote100k.com")
        return jobs

    def normalize(self, raw_job: dict[str, Any]) -> dict[str, Any]:
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
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_sitemap(content: bytes) -> list[str]:
    """Return /remote-job/ URLs from the sitemap, newest first.

    Handles both flat <urlset> and <sitemapindex> (follows one level of
    child sitemaps to find the one containing job pages).
    """
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    tag = root.tag.lower()

    # Sitemap index — follow child sitemaps to find job URLs.
    if "sitemapindex" in tag:
        all_urls: list[str] = []
        for sm_el in root.findall("sm:sitemap", _NS) or root.findall("sitemap"):
            child_loc = (
                sm_el.findtext("sm:loc", namespaces=_NS)
                or sm_el.findtext("loc")
                or ""
            ).strip()
            if not child_loc:
                continue
            try:
                r = requests.get(child_loc, headers=_HEADERS, timeout=15)
                r.raise_for_status()
                all_urls.extend(_parse_urlset(ET.fromstring(r.content)))
                time.sleep(0.2)
            except Exception:
                continue
        return all_urls

    # Flat urlset.
    return _parse_urlset(root)


def _parse_urlset(root: ET.Element) -> list[str]:
    """Extract and sort /remote-job/ URLs from a <urlset> element."""
    entries: list[tuple[str, str]] = []  # (lastmod, url)
    for url_el in root.findall("sm:url", _NS) or root.findall("url"):
        loc = (
            url_el.findtext("sm:loc", namespaces=_NS)
            or url_el.findtext("loc")
            or ""
        ).strip()
        if not re.match(r"https://remote100k\.com/remote-job/[^/]+/?$", loc):
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
    slug = url.rstrip("/").split("/")[-1].lower()
    return any(kw in slug for kw in _ENGINEERING_KEYWORDS)


def _fetch_job_page(url: str) -> dict[str, Any] | None:
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    return _extract_job(resp.text, url)


def _extract_job(html: str, page_url: str) -> dict[str, Any] | None:
    """Parse JSON-LD and extract ATS apply URL from page HTML."""
    # --- JSON-LD ---
    jsonld: dict[str, Any] = {}
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
            jsonld = data
            break

    if not jsonld:
        return None

    title = (jsonld.get("title") or "").strip()
    company = ((jsonld.get("hiringOrganization") or {}).get("name") or "Unknown").strip()
    description = (jsonld.get("description") or "").strip()

    # Salary from baseSalary block → append to description so it shows in UI.
    bs = jsonld.get("baseSalary") or {}
    bsv = bs.get("value") or {}
    lo, hi, cur = bsv.get("minValue"), bsv.get("maxValue"), bs.get("currency", "")
    if lo or hi:
        salary_str = f"{cur}{lo:,.0f}–{cur}{hi:,.0f}" if lo and hi else f"{cur}{lo or hi:,.0f}"
        if salary_str not in description:
            description = f"{salary_str}\n\n{description}".strip()

    # Location: derive from jobLocationType + description text.
    loc_type = (jsonld.get("jobLocationType") or "").upper()
    location = "Worldwide" if loc_type == "TELECOMMUTE" else "Remote"

    posted_date = None
    date_str = jsonld.get("datePosted")
    if date_str:
        try:
            posted_date = dateutil_parser.parse(date_str)
            if posted_date.tzinfo is None:
                posted_date = posted_date.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # --- ATS apply URL ---
    # The apply URL appears in an <a href="..."> tag in the page source.
    # Strip the ?ref=remote100k tracking parameter before storing.
    apply_url = _extract_ats_url(html) or page_url

    slug = page_url.rstrip("/").split("/")[-1]
    return {
        "id": slug,
        "url": apply_url,
        "title": title,
        "company": company,
        "location": location,
        "description": description,
        "posted_date": posted_date,
    }


def _extract_ats_url(html: str) -> str | None:
    """Find a direct ATS apply URL in the page HTML and strip tracking params."""
    for domain in _ATS_DOMAINS:
        pattern = rf'href=["\']({re.escape("https://" + domain)}[^"\']*)["\']'
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            return _strip_ref_param(m.group(1))
    return None


def _strip_ref_param(url: str) -> str:
    """Remove ?ref= and ?ref=remote100k tracking parameters from a URL."""
    parsed = urlparse(url)
    qs = re.sub(r"(?:^|&)ref=[^&]*", "", parsed.query).strip("&")
    return urlunparse(parsed._replace(query=qs))
