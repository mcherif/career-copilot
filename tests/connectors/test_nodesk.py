"""
Mocked tests for NodeskConnector.

Covers: sitemap parsing, engineering URL filter, JSON-LD extraction,
expired-job skipping, HTTP error handling, and normalize() shape.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from connectors.nodesk import (
    NodeskConnector,
    _extract_jsonld,
    _is_engineering_url,
    _parse_sitemap,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_FUTURE = (datetime.now(tz=timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
_PAST   = (datetime.now(tz=timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
_TODAY  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _sitemap_xml(*slugs: str) -> bytes:
    items = "\n".join(
        f"""  <url>
    <loc>https://nodesk.co/remote-jobs/{slug}/</loc>
    <lastmod>2026-04-01</lastmod>
  </url>"""
        for slug in slugs
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{items}
  <url>
    <loc>https://nodesk.co/remote-companies/acme/</loc>
  </url>
</urlset>""".encode()


def _job_html(
    title="Senior Engineer",
    company="Acme",
    valid_through=None,
    location_name="Worldwide",
) -> str:
    ld = {
        "@context": "https://schema.org/",
        "@type": "JobPosting",
        "title": title,
        "datePosted": "2026-03-27",
        "hiringOrganization": {"@type": "Organization", "name": company},
        "applicantLocationRequirements": [{"@type": "Country", "name": location_name}],
        "description": "<p>Python and Django</p>",
        "jobLocationType": "TELECOMMUTE",
    }
    if valid_through:
        ld["validThrough"] = valid_through
    blob = json.dumps(ld)
    return f"""<html><head>
<script type="application/ld+json">{blob}</script>
</head><body></body></html>"""


def _mock_response(content, status=200):
    m = MagicMock()
    m.status_code = status
    if isinstance(content, bytes):
        m.content = content
        m.text = content.decode()
    else:
        m.content = content.encode()
        m.text = content
    m.raise_for_status = MagicMock()
    return m


# ---------------------------------------------------------------------------
# _parse_sitemap
# ---------------------------------------------------------------------------

class TestParseSitemap:
    def test_returns_job_urls_only(self):
        xml = _sitemap_xml("acme-senior-engineer", "kodify-fullstack-developer")
        urls = _parse_sitemap(xml)
        assert all("nodesk.co/remote-jobs/" in u for u in urls)
        assert not any("remote-companies" in u for u in urls)

    def test_sorted_newest_first(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://nodesk.co/remote-jobs/old-job/</loc><lastmod>2026-01-01</lastmod></url>
  <url><loc>https://nodesk.co/remote-jobs/new-job/</loc><lastmod>2026-04-01</lastmod></url>
</urlset>"""
        urls = _parse_sitemap(xml)
        assert urls[0].endswith("new-job/")
        assert urls[1].endswith("old-job/")

    def test_malformed_xml_returns_empty(self):
        assert _parse_sitemap(b"not xml at all") == []

    def test_excludes_company_and_article_urls(self):
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://nodesk.co/remote-companies/acme/</loc></url>
  <url><loc>https://nodesk.co/articles/remote-work-tips/</loc></url>
  <url><loc>https://nodesk.co/remote-jobs/acme-senior-engineer/</loc></url>
</urlset>"""
        urls = _parse_sitemap(xml)
        assert not any("remote-companies" in u for u in urls)
        assert not any("articles" in u for u in urls)
        assert any("acme-senior-engineer" in u for u in urls)


# ---------------------------------------------------------------------------
# _is_engineering_url
# ---------------------------------------------------------------------------

class TestIsEngineeringUrl:
    @pytest.mark.parametrize("slug", [
        "acme-senior-engineer",
        "kodify-fullstack-developer",
        "stripe-backend-engineer",
        "gitlab-devops-lead",
        "openai-machine-learning-researcher",
        "deep-embedded-firmware-engineer",
    ])
    def test_engineering_urls_accepted(self, slug):
        assert _is_engineering_url(f"https://nodesk.co/remote-jobs/{slug}/")

    @pytest.mark.parametrize("slug", [
        "stripe-campaign-operations-manager",
        "shopify-merchant-support-advisor",
        "acme-content-writer",
        "company-hr-specialist",
    ])
    def test_non_engineering_urls_rejected(self, slug):
        assert not _is_engineering_url(f"https://nodesk.co/remote-jobs/{slug}/")


# ---------------------------------------------------------------------------
# _extract_jsonld
# ---------------------------------------------------------------------------

class TestExtractJsonld:
    def test_extracts_title_and_company(self):
        html = _job_html("Senior ML Engineer", "DeepCo")
        raw = _extract_jsonld(html, "https://nodesk.co/remote-jobs/deepco-ml-engineer/")
        assert raw is not None
        assert raw["title"] == "Senior ML Engineer"
        assert raw["company"] == "DeepCo"

    def test_extracts_location(self):
        html = _job_html(location_name="Europe")
        raw = _extract_jsonld(html, "https://nodesk.co/remote-jobs/acme-engineer/")
        assert raw["location"] == "Europe"

    def test_expired_job_returns_none(self):
        html = _job_html(valid_through=_PAST)
        raw = _extract_jsonld(html, "https://nodesk.co/remote-jobs/acme-old-role/")
        assert raw is None

    def test_future_valid_through_not_skipped(self):
        html = _job_html(valid_through=_FUTURE)
        raw = _extract_jsonld(html, "https://nodesk.co/remote-jobs/acme-new-role/")
        assert raw is not None

    def test_no_jsonld_returns_none(self):
        raw = _extract_jsonld("<html><body>No data</body></html>", "https://nodesk.co/remote-jobs/foo/")
        assert raw is None

    def test_slug_used_as_id(self):
        html = _job_html()
        raw = _extract_jsonld(html, "https://nodesk.co/remote-jobs/acme-backend-engineer/")
        assert raw["id"] == "acme-backend-engineer"


# ---------------------------------------------------------------------------
# NodeskConnector.fetch_jobs (mocked HTTP)
# ---------------------------------------------------------------------------

class TestNodeskFetch:
    def _make_responses(self, slugs, job_html_map=None):
        """Return a side_effect list: first call is sitemap, rest are job pages."""
        xml = _sitemap_xml(*slugs)
        responses = [_mock_response(xml)]
        for slug in slugs:
            html = (job_html_map or {}).get(slug, _job_html(f"Job at {slug}", "Acme"))
            responses.append(_mock_response(html))
        return responses

    @patch("connectors.nodesk.time.sleep")
    @patch("connectors.nodesk.requests.get")
    def test_returns_jobs(self, mock_get, mock_sleep):
        slugs = ["acme-senior-engineer", "stripe-backend-developer"]
        mock_get.side_effect = self._make_responses(slugs)
        jobs = NodeskConnector().fetch_jobs()
        assert len(jobs) == 2

    @patch("connectors.nodesk.time.sleep")
    @patch("connectors.nodesk.requests.get")
    def test_filters_non_engineering(self, mock_get, mock_sleep):
        slugs = ["acme-senior-engineer", "stripe-content-writer"]
        xml = _sitemap_xml(*slugs)
        mock_get.side_effect = [
            _mock_response(xml),
            _mock_response(_job_html("Senior Engineer", "Acme")),
            # content-writer should never be fetched
        ]
        jobs = NodeskConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Senior Engineer"

    @patch("connectors.nodesk.time.sleep")
    @patch("connectors.nodesk.requests.get")
    def test_skips_expired_jobs(self, mock_get, mock_sleep):
        slugs = ["acme-senior-engineer"]
        xml = _sitemap_xml(*slugs)
        mock_get.side_effect = [
            _mock_response(xml),
            _mock_response(_job_html(valid_through=_PAST)),
        ]
        jobs = NodeskConnector().fetch_jobs()
        assert jobs == []

    @patch("connectors.nodesk.time.sleep")
    @patch("connectors.nodesk.requests.get")
    def test_sitemap_fetch_error_returns_empty(self, mock_get, mock_sleep):
        mock_get.side_effect = Exception("network error")
        jobs = NodeskConnector().fetch_jobs()
        assert jobs == []

    @patch("connectors.nodesk.time.sleep")
    @patch("connectors.nodesk.requests.get")
    def test_page_fetch_error_skips_job(self, mock_get, mock_sleep):
        xml = _sitemap_xml("acme-senior-engineer", "stripe-backend-developer")
        mock_get.side_effect = [
            _mock_response(xml),
            Exception("timeout"),
            _mock_response(_job_html("Backend Developer", "Stripe")),
        ]
        jobs = NodeskConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["company"] == "Stripe"


# ---------------------------------------------------------------------------
# NodeskConnector.normalize
# ---------------------------------------------------------------------------

class TestNodeskNormalize:
    REQUIRED = {
        "external_id", "source", "company", "title", "location",
        "raw_location_text", "description", "description_text",
        "url", "ats_type", "posted_date", "remote_eligibility",
    }

    def _raw(self):
        return {
            "id": "kodify-media-group-senior-fullstack-developer",
            "url": "https://nodesk.co/remote-jobs/kodify-media-group-senior-fullstack-developer/",
            "title": "Senior Fullstack Developer",
            "company": "Kodify Media Group",
            "location": "Worldwide",
            "description": "<p>React and Node.js role.</p>",
            "posted_date": datetime(2026, 3, 27, tzinfo=timezone.utc),
        }

    def test_shape(self):
        n = NodeskConnector().normalize(self._raw())
        for key in self.REQUIRED:
            assert key in n, f"normalize() missing key: '{key}'"
        assert n["source"] == "nodesk"

    def test_html_stripped_from_description_text(self):
        n = NodeskConnector().normalize(self._raw())
        assert "<p>" not in n["description_text"]
        assert "React" in n["description_text"]

    def test_missing_location_defaults_to_worldwide(self):
        raw = self._raw()
        raw.pop("location")
        n = NodeskConnector().normalize(raw)
        assert n["location"] == "Worldwide"
