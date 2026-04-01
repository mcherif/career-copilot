import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import requests
from lxml import etree
from dateutil import parser

from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("realworkfromanywhere_connector")

# Main feed — all categories. Only lists jobs that are genuinely worldwide-remote.
_FEED_URL = "https://www.realworkfromanywhere.com/rss.xml"
MAX_AGE_DAYS = 10

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}


class RealWorkFromAnywhereConnector(BaseConnector):
    def __init__(self):
        self.source_name = "realworkfromanywhere"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} RSS feed...")
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        try:
            response = requests.get(_FEED_URL, headers=_HEADERS, timeout=15)
            response.raise_for_status()
            _parser = etree.XMLParser(recover=True)
            root = etree.fromstring(response.content, _parser)
            channel = root.find("channel")
            if channel is None:
                return []

            jobs = []
            for item in channel.findall("item"):
                raw = self._parse_item(item)
                if not raw:
                    continue
                if raw.get("posted_date") and raw["posted_date"] < cutoff:
                    continue
                jobs.append(raw)

            logger.info(f"Successfully fetched {len(jobs)} jobs from {self.source_name}")
            return jobs
        except Exception as e:
            logger.error(f"Error fetching jobs from {self.source_name}: {e}")
            logger.debug(traceback.format_exc())
            return []

    def _parse_item(self, item) -> Dict[str, Any] | None:
        title_el = item.find("title")
        link_el = item.find("link")
        desc_el = item.find("description")
        pub_el = item.find("pubDate")
        author_el = item.find("author")
        guid_el = item.find("guid")

        title_raw = (title_el.text or "").strip() if title_el is not None else ""
        if not title_raw:
            return None

        # Titles are formatted as "Job Title at Company Name"
        if " at " in title_raw:
            title, _, company = title_raw.rpartition(" at ")
        else:
            title, company = title_raw, "Unknown"

        # lxml returns <link> and <guid> text on .text directly
        url = ""
        if guid_el is not None and guid_el.text:
            url = guid_el.text.strip()
        if not url and link_el is not None and link_el.text:
            url = link_el.text.strip()

        description = (desc_el.text or "").strip() if desc_el is not None else ""

        # <author> holds the company name as a fallback
        if company == "Unknown" and author_el is not None and author_el.text:
            company = author_el.text.strip()

        posted_date = None
        if pub_el is not None and pub_el.text:
            try:
                posted_date = parser.parse(pub_el.text)
                if posted_date.tzinfo is None:
                    posted_date = posted_date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        external_id = url.rstrip("/").split("/")[-1] if url else title_raw[:80]

        return {
            "id": external_id,
            "title": title.strip(),
            "company": company.strip(),
            "url": url,
            "description": description,
            "posted_date": posted_date,
        }

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        return {
            "external_id": raw_job.get("id", ""),
            "source": self.source_name,
            "company": raw_job.get("company", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": "Remote",
            "raw_location_text": "Remote",
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": "accept",  # board only lists worldwide-remote jobs
        }

    def get_source_name(self) -> str:
        return self.source_name
