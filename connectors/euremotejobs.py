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

logger = setup_logger("euremotejobs_connector")

_FEED_URL = "https://euremotejobs.com/job-listings/feed/"
MAX_AGE_DAYS = 10

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}


class EURemoteJobsConnector(BaseConnector):
    def __init__(self):
        self.source_name = "euremotejobs"

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
        _NS_CONTENT = "{http://purl.org/rss/1.0/modules/content/}"

        def _t(tag: str) -> str:
            el = item.find(tag)
            return (el.text or "").strip() if el is not None else ""

        title = _t("title")
        if not title:
            return None

        # In lxml, <link> text is directly on .text (not .tail as in stdlib ET)
        url = _t("link") or _t("guid")

        # Prefer the richer content:encoded over plain description
        description = _t(f"{_NS_CONTENT}encoded") or _t("description")

        pub_date_raw = _t("pubDate")
        posted_date = None
        if pub_date_raw:
            try:
                posted_date = parser.parse(pub_date_raw)
                if posted_date.tzinfo is None:
                    posted_date = posted_date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        # Try to extract company from title "Job Title | Company" or "Job Title - Company"
        company = "Unknown"
        for sep in (" | ", " – ", " - "):
            if sep in title:
                parts = title.split(sep, 1)
                title, company = parts[0].strip(), parts[1].strip()
                break

        external_id = url.rstrip("/").split("/")[-1] if url else title[:80]

        return {
            "id": external_id,
            "title": title,
            "company": company,
            "url": url,
            "description": description,
            "posted_date": posted_date,
            "location": "Remote (EU timezone)",
        }

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        return {
            "external_id": raw_job.get("id", ""),
            "source": self.source_name,
            "company": raw_job.get("company", "Unknown"),
            "title": raw_job.get("title", ""),
            "location": raw_job.get("location", "Remote (EU timezone)"),
            "raw_location_text": raw_job.get("location", "Remote (EU timezone)"),
            "description": description,
            "description_text": clean_description(description),
            "url": url,
            "ats_type": detect_ats(url),
            "posted_date": raw_job.get("posted_date"),
            "remote_eligibility": None,  # let the remote filter classify — EU timezone jobs vary
        }

    def get_source_name(self) -> str:
        return self.source_name
