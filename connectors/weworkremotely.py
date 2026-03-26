import traceback
import requests
import xml.etree.ElementTree as ET
from typing import List, Dict, Any
from dateutil import parser
from connectors.base import BaseConnector
from utils.ats_detector import detect_ats
from utils.text_cleaning import clean_description
from utils.logger import setup_logger

logger = setup_logger("weworkremotely_connector")

# Category RSS feeds most relevant to the target profile.
_FEED_URLS = [
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
]

_WWR_NS = "https://weworkremotely.com"


class WeWorkRemotelyConnector(BaseConnector):
    def __init__(self):
        self.source_name = "weworkremotely"

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching jobs from {self.source_name} RSS feeds...")
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
        region_el = item.find(f"{{{_WWR_NS}}}region")

        title_raw = (title_el.text or "").strip() if title_el is not None else ""
        if not title_raw:
            return None

        # WWR title format: "Company: Job Title at Region" or "Company: Job Title"
        if ": " in title_raw:
            company, _, job_title = title_raw.partition(": ")
            # Strip trailing " at Region" if present
            if " at " in job_title:
                job_title = job_title.rsplit(" at ", 1)[0].strip()
        else:
            company = "Unknown"
            job_title = title_raw

        # The <link> element in RSS 2.0 is a text node between tags (sibling, not child).
        # ElementTree exposes it as the tail of the previous sibling or as item text.
        url = ""
        if link_el is not None and link_el.tail:
            url = link_el.tail.strip()
        # Fallback: look for a guid element
        if not url:
            guid_el = item.find("guid")
            if guid_el is not None and guid_el.text:
                url = guid_el.text.strip()

        description = ""
        if desc_el is not None and desc_el.text:
            description = desc_el.text.strip()

        posted_date = None
        if pub_el is not None and pub_el.text:
            try:
                posted_date = parser.parse(pub_el.text)
            except Exception:
                pass

        location = "Worldwide"
        if region_el is not None and region_el.text:
            location = region_el.text.strip()

        # Use URL path as stable ID since WWR has no numeric id in RSS.
        external_id = url.split("/")[-1] if url else title_raw[:80]

        return {
            "id": external_id,
            "company": company.strip(),
            "title": job_title.strip(),
            "url": url,
            "description": description,
            "posted_date": posted_date,
            "location": location,
        }

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        url = raw_job.get("url", "")
        description = raw_job.get("description", "")
        location = raw_job.get("location", "Worldwide")

        return {
            "external_id": raw_job.get("id", ""),
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
