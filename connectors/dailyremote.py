import traceback
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("dailyremote_connector")

# Category feeds most relevant to the target profile.
_FEED_URLS = [
    "https://dailyremote.com/rss",
]


class DailyRemoteConnector(BaseConnector):
    def __init__(self):
        self.source_name = "dailyremote"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} RSS feed...")
        headers = {"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}
        all_jobs: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for feed_url in _FEED_URLS:
            try:
                response = requests.get(feed_url, headers=headers, timeout=15)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                channel = root.find("channel")
                if channel is None:
                    continue
                for item in channel.findall("item"):
                    job = self._parse_item(item)
                    if job and job["id"] not in seen_ids:
                        seen_ids.add(job["id"])
                        all_jobs.append(job)
            except Exception as e:
                logger.error(f"Error fetching feed {feed_url}: {e}")
                logger.debug(traceback.format_exc())

        logger.info(f"Successfully fetched {len(all_jobs)} jobs from {self.source_name}")
        return all_jobs

    def _parse_item(self, item: ET.Element) -> Dict[str, Any] | None:
        title_el = item.find("title")
        link_el = item.find("link")
        desc_el = item.find("description")
        pub_el = item.find("pubDate")

        title = (title_el.text or "").strip() if title_el is not None else ""
        if not title:
            return None

        url = ""
        if link_el is not None and link_el.tail:
            url = link_el.tail.strip()
        if not url:
            guid = item.find("guid")
            if guid is not None and guid.text:
                url = guid.text.strip()

        description = (desc_el.text or "").strip() if desc_el is not None else ""

        posted_date = None
        if pub_el is not None and pub_el.text:
            try:
                posted_date = parser.parse(pub_el.text)
            except Exception:
                pass

        # DailyRemote titles: "Job Title at Company" or just "Job Title"
        company = "Unknown"
        if " at " in title:
            title, _, company = title.rpartition(" at ")

        return {
            "id": url.rstrip("/").split("/")[-1] or title[:80],
            "title": title.strip(),
            "company": company.strip(),
            "url": url,
            "description": description,
            "posted_date": posted_date,
            "location": "Remote",
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
            "remote_eligibility": None,
        }

    def get_source_name(self) -> str:
        return self.source_name
