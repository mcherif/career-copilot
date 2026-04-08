"""
Mocked tests for Remote100kConnector.

Covers: sitemap parsing (flat + index), engineering URL filter, job extraction,
ATS URL detection, tracking-param stripping, and normalize() shape.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from connectors.remote100k import (
    Remote100kConnector,
    _extract_ats_url,
    _extract_job,
    _is_engineering_url,
    _parse_sitemap,
    _strip_ref_param,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _sitemap_xml(*slugs: str) -> bytes:
    items = "\n".join(
        f"  <url><loc>https://remote100k.com/remote-job/{s}</loc>"
        f"<lastmod>2026-04-08</lastmod></url>"
        for s in slugs
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{items}
  <url><loc>https://remote100k.com/companies/acme</loc></url>
</urlset>""".encode()


def _job_html(
    title="Senior Backend Engineer",
    company="Acme",
    apply_url="https://jobs.ashbyhq.com/acme/abc-123?ref=remote100k",
    date_posted="2026-03-15T00:00:00.000Z",
    salary_min=None,
    salary_max=None,
) -> str:
    ld = {
        "@context": "https://schema.org/",
        "@type": "JobPosting",
        "title": title,
        "description": f"{company} is hiring a {title}.",
        "datePosted": date_posted,
        "employmentType": "FULL_TIME",
        "hiringOrganization": {"@type": "Organization", "name": company},
        "jobLocationType": "TELECOMMUTE",
        "directApply": True,
        "url": "https://remote100k.com/remote-job/acme-senior-backend-engineer",
    }
    if salary_min and salary_max:
        ld["baseSalary"] = {
            "@type": "MonetaryAmount",
            "currency": "USD",
            "value": {
                "@type": "QuantitativeValue",
                "minValue": salary_min,
                "maxValue": salary_max,
                "unitText": "YEAR",
            },
        }
    return f"""<html><head>
<script type="application/ld+json">{json.dumps(ld)}</script>
</head><body>
<a href="{apply_url}" style="border-radius:12px">Apply Now</a>
</body></html>"""


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
# _strip_ref_param
# ---------------------------------------------------------------------------

class TestStripRefParam:
    def test_strips_ref_only(self):
        url = "https://jobs.ashbyhq.com/acme/123?ref=remote100k"
        assert _strip_ref_param(url) == "https://jobs.ashbyhq.com/acme/123"

    def test_strips_ref_preserves_other_params(self):
        url = "https://boards.greenhouse.io/acme/jobs/123?gh_src=abc&ref=remote100k"
        result = _strip_ref_param(url)
        assert "ref=" not in result
        assert "gh_src=abc" in result

    def test_url_without_ref_unchanged(self):
        url = "https://jobs.lever.co/acme/abc-123"
        assert _strip_ref_param(url) == url


# ---------------------------------------------------------------------------
# _extract_ats_url
# ---------------------------------------------------------------------------

class TestExtractAtsUrl:
    def test_finds_ashby_url(self):
        html = '<a href="https://jobs.ashbyhq.com/acme/abc?ref=remote100k">Apply</a>'
        result = _extract_ats_url(html)
        assert result == "https://jobs.ashbyhq.com/acme/abc"

    def test_finds_greenhouse_url(self):
        html = '<a href="https://boards.greenhouse.io/acme/jobs/123?ref=r100k">Apply</a>'
        result = _extract_ats_url(html)
        assert result is not None
        assert "greenhouse" in result

    def test_returns_none_when_no_ats_link(self):
        html = "<html><body>No apply link here</body></html>"
        assert _extract_ats_url(html) is None


# ---------------------------------------------------------------------------
# _parse_sitemap (flat urlset)
# ---------------------------------------------------------------------------

class TestParseSitemap:
    def test_returns_remote_job_urls_only(self):
        xml = _sitemap_xml("acme-senior-engineer", "stripe-backend-developer")
        urls = _parse_sitemap(xml)
        assert all("remote100k.com/remote-job/" in u for u in urls)
        assert not any("companies" in u for u in urls)

    def test_sorted_newest_first(self):
        xml = b"""<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://remote100k.com/remote-job/old-job</loc><lastmod>2026-01-01</lastmod></url>
  <url><loc>https://remote100k.com/remote-job/new-job</loc><lastmod>2026-04-08</lastmod></url>
</urlset>"""
        urls = _parse_sitemap(xml)
        assert urls[0].endswith("new-job")
        assert urls[1].endswith("old-job")

    def test_malformed_xml_returns_empty(self):
        assert _parse_sitemap(b"not xml") == []


# ---------------------------------------------------------------------------
# _is_engineering_url
# ---------------------------------------------------------------------------

class TestIsEngineeringUrl:
    @pytest.mark.parametrize("slug", [
        "acme-senior-backend-engineer",
        "stripe-software-engineer-infrastructure",
        "openai-machine-learning-researcher",
        "cursor-fullstack-developer",
    ])
    def test_engineering_slugs_accepted(self, slug):
        assert _is_engineering_url(f"https://remote100k.com/remote-job/{slug}")

    @pytest.mark.parametrize("slug", [
        "stripe-campaign-operations-manager",
        "acme-content-writer",
        "company-hr-specialist",
    ])
    def test_non_engineering_slugs_rejected(self, slug):
        assert not _is_engineering_url(f"https://remote100k.com/remote-job/{slug}")


# ---------------------------------------------------------------------------
# _extract_job
# ---------------------------------------------------------------------------

class TestExtractJob:
    _URL = "https://remote100k.com/remote-job/acme-senior-backend-engineer"

    def test_extracts_title_and_company(self):
        html = _job_html("Senior Backend Engineer", "Acme Corp")
        raw = _extract_job(html, self._URL)
        assert raw is not None
        assert raw["title"] == "Senior Backend Engineer"
        assert raw["company"] == "Acme Corp"

    def test_uses_ats_apply_url(self):
        html = _job_html(apply_url="https://jobs.ashbyhq.com/acme/abc?ref=remote100k")
        raw = _extract_job(html, self._URL)
        assert raw["url"] == "https://jobs.ashbyhq.com/acme/abc"

    def test_falls_back_to_page_url_when_no_ats_link(self):
        html = _job_html(apply_url="https://remote100k.com/some-internal-link")
        raw = _extract_job(html, self._URL)
        # No ATS domain found — falls back to page_url
        assert raw["url"] == self._URL

    def test_salary_prepended_to_description(self):
        html = _job_html(salary_min=150000, salary_max=250000)
        raw = _extract_job(html, self._URL)
        assert "150,000" in raw["description"]
        assert "250,000" in raw["description"]

    def test_posted_date_parsed(self):
        html = _job_html(date_posted="2026-03-15T00:00:00.000Z")
        raw = _extract_job(html, self._URL)
        assert isinstance(raw["posted_date"], datetime)
        assert raw["posted_date"].year == 2026
        assert raw["posted_date"].month == 3

    def test_no_jsonld_returns_none(self):
        raw = _extract_job("<html><body>nothing</body></html>", self._URL)
        assert raw is None

    def test_slug_used_as_id(self):
        html = _job_html()
        raw = _extract_job(html, self._URL)
        assert raw["id"] == "acme-senior-backend-engineer"


# ---------------------------------------------------------------------------
# Remote100kConnector.fetch_jobs (mocked HTTP)
# ---------------------------------------------------------------------------

class TestRemote100kFetch:
    @patch("connectors.remote100k.time.sleep")
    @patch("connectors.remote100k.requests.get")
    def test_returns_engineering_jobs(self, mock_get, mock_sleep):
        xml = _sitemap_xml("acme-senior-engineer", "stripe-backend-developer")
        mock_get.side_effect = [
            _mock_response(xml),
            _mock_response(_job_html("Senior Engineer", "Acme")),
            _mock_response(_job_html("Backend Developer", "Stripe")),
        ]
        jobs = Remote100kConnector().fetch_jobs()
        assert len(jobs) == 2

    @patch("connectors.remote100k.time.sleep")
    @patch("connectors.remote100k.requests.get")
    def test_filters_non_engineering_slugs(self, mock_get, mock_sleep):
        xml = _sitemap_xml("acme-senior-engineer", "stripe-content-writer")
        mock_get.side_effect = [
            _mock_response(xml),
            _mock_response(_job_html("Senior Engineer", "Acme")),
        ]
        jobs = Remote100kConnector().fetch_jobs()
        assert len(jobs) == 1

    @patch("connectors.remote100k.time.sleep")
    @patch("connectors.remote100k.requests.get")
    def test_sitemap_error_returns_empty(self, mock_get, mock_sleep):
        mock_get.side_effect = Exception("network error")
        jobs = Remote100kConnector().fetch_jobs()
        assert jobs == []

    @patch("connectors.remote100k.time.sleep")
    @patch("connectors.remote100k.requests.get")
    def test_page_error_skips_job(self, mock_get, mock_sleep):
        xml = _sitemap_xml("acme-senior-engineer", "stripe-backend-developer")
        mock_get.side_effect = [
            _mock_response(xml),
            Exception("timeout"),
            _mock_response(_job_html("Backend Developer", "Stripe")),
        ]
        jobs = Remote100kConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["company"] == "Stripe"


# ---------------------------------------------------------------------------
# Remote100kConnector.normalize
# ---------------------------------------------------------------------------

class TestRemote100kNormalize:
    REQUIRED = {
        "external_id", "source", "company", "title", "location",
        "raw_location_text", "description", "description_text",
        "url", "ats_type", "posted_date", "remote_eligibility",
    }

    def _raw(self):
        return {
            "id": "acme-senior-backend-engineer",
            "url": "https://jobs.ashbyhq.com/acme/abc-123",
            "title": "Senior Backend Engineer",
            "company": "Acme",
            "location": "Worldwide",
            "description": "$150,000–$250,000\n\nPython and Kubernetes role.",
            "posted_date": datetime(2026, 3, 15, tzinfo=timezone.utc),
        }

    def test_shape(self):
        n = Remote100kConnector().normalize(self._raw())
        for key in self.REQUIRED:
            assert key in n, f"normalize() missing key: '{key}'"
        assert n["source"] == "remote100k"

    def test_ats_type_detected(self):
        n = Remote100kConnector().normalize(self._raw())
        assert n["ats_type"] == "ashby"

    def test_description_text_cleaned(self):
        raw = self._raw()
        raw["description"] = "<p>Python role</p>"
        n = Remote100kConnector().normalize(raw)
        assert "<p>" not in n["description_text"]
        assert "Python" in n["description_text"]
