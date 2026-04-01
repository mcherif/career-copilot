"""
Mocked fetch + normalize tests for Ashby, Greenhouse, and Lever connectors.

No live network calls — requests.get is patched throughout.
Covers:
  - fetch_jobs() success path
  - fetch_jobs() HTTP error -> []
  - fetch_jobs() network exception -> []
  - normalize() key field mappings
  - normalize() missing / partial fields (graceful fallback)
  - Connector-specific logic (slug-based company name, URL building,
    date parsing, remote filtering, list-based description fallback)
"""
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from requests.exceptions import HTTPError, ConnectionError as RequestsConnectionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_mock(payload, status_code=200):
    """Return a MagicMock that mimics a successful requests.Response."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = payload
    if status_code >= 400:
        mock.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}")
    else:
        mock.raise_for_status = MagicMock()
    return mock


def _error_mock(status_code=500):
    return _json_mock({}, status_code)


# ---------------------------------------------------------------------------
# Ashby fixtures
# ---------------------------------------------------------------------------

def _ashby_job(
    job_id="ashby-001",
    title="Senior Software Engineer",
    apply_url="https://jobs.ashbyhq.com/acme/ashby-001",
    job_url="https://ashbyhq.com/acme/ashby-001",
    workplace_type="Remote",
    is_remote=True,
    org_name="Acme Corp",
    description_plain="We love Python.",
    published_at="2026-03-15T10:00:00Z",
    slug="acme",
):
    return {
        "id": job_id,
        "title": title,
        "applyUrl": apply_url,
        "jobUrl": job_url,
        "workplaceType": workplace_type,
        "isRemote": is_remote,
        "organizationName": org_name,
        "descriptionPlain": description_plain,
        "publishedAt": published_at,
        "location": "Remote",
        "_slug": slug,
    }


# ---------------------------------------------------------------------------
# Greenhouse fixtures
# ---------------------------------------------------------------------------

def _greenhouse_job(
    job_id=42,
    title="Backend Engineer",
    absolute_url="https://boards.greenhouse.io/acme/jobs/42",
    location_name="Remote",
    content="<p>Great role</p>",
    first_published="2026-03-20T08:00:00Z",
    company_name="Acme Corp",
    slug="acme",
):
    return {
        "id": job_id,
        "title": title,
        "absolute_url": absolute_url,
        "location": {"name": location_name},
        "content": content,
        "first_published": first_published,
        "company_name": company_name,
        "_slug": slug,
    }


# ---------------------------------------------------------------------------
# Lever fixtures
# ---------------------------------------------------------------------------

def _lever_job(
    job_id="lever-001",
    title="ML Engineer",
    hosted_url="https://jobs.lever.co/acme/lever-001",
    location="Remote",
    description_plain="Great ML role.",
    created_at=1742000000000,  # milliseconds timestamp
    slug="acme",
):
    return {
        "id": job_id,
        "text": title,
        "hostedUrl": hosted_url,
        "categories": {"location": location, "commitment": "Full-time"},
        "descriptionPlain": description_plain,
        "createdAt": created_at,
        "_slug": slug,
    }


# ===========================================================================
# ASHBY
# ===========================================================================

class TestAshbyFetch:
    """Tests for AshbyConnector.fetch_jobs() via _fetch_company() internals."""

    _PATCH_GET = "connectors.ashby.requests.get"
    _PATCH_DB_SLUGS = "connectors.ashby._load_slugs_from_db"
    _PATCH_EXCL_SLUGS = "connectors.ashby._load_excluded_slugs"
    _PATCH_TARGET_ROLES = "connectors.ashby._load_target_roles"

    def _fetch_with_payload(self, jobs_list):
        """Patch DB/profile helpers so fetch_jobs() actually hits _fetch_company."""
        payload = {"jobs": jobs_list}
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(payload)):
            from connectors.ashby import AshbyConnector
            return AshbyConnector().fetch_jobs()

    def test_success_returns_remote_jobs(self):
        job = _ashby_job()
        jobs = self._fetch_with_payload([job])
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Senior Software Engineer"

    def test_non_remote_job_is_filtered(self):
        job = _ashby_job(workplace_type="Onsite", is_remote=False)
        jobs = self._fetch_with_payload([job])
        assert jobs == []

    def test_is_remote_false_filters_even_if_workplace_remote(self):
        # isRemote=False must also be False for inclusion
        job = _ashby_job(workplace_type="Remote", is_remote=False)
        jobs = self._fetch_with_payload([job])
        assert jobs == []

    def test_deduplication_same_id_returned_once(self):
        job = _ashby_job(job_id="dup-001")
        # Pass same job twice
        payload = {"jobs": [job, {**job}]}
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(payload)):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_empty_slugs_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value=set()), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_error_mock(503)):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, side_effect=RequestsConnectionError("timeout")):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert jobs == []

    def test_404_returns_empty_list_for_slug(self):
        mock = _json_mock({}, 404)
        # 404 doesn't raise — connector returns [] silently
        mock.raise_for_status = MagicMock()
        with patch(self._PATCH_DB_SLUGS, return_value={"no-such-co"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=mock):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert jobs == []

    def test_excluded_slugs_are_skipped(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]):
            from connectors.ashby import AshbyConnector
            jobs = AshbyConnector().fetch_jobs()
        assert jobs == []


class TestAshbyNormalize:
    """Tests for AshbyConnector.normalize()."""

    def _normalize(self, raw_job):
        from connectors.ashby import AshbyConnector
        return AshbyConnector().normalize(raw_job)

    def test_basic_field_mapping(self):
        raw = _ashby_job()
        result = self._normalize(raw)
        assert result["title"] == "Senior Software Engineer"
        assert result["company"] == "Acme Corp"
        assert result["source"] == "ashby"
        assert result["url"] == "https://jobs.ashbyhq.com/acme/ashby-001"
        assert result["external_id"] == "ashby_ashby-001"
        assert result["remote_eligibility"] == "accept"

    def test_apply_url_preferred_over_job_url(self):
        raw = _ashby_job(apply_url="https://apply.example.com/job", job_url="https://listing.example.com/job")
        result = self._normalize(raw)
        assert result["url"] == "https://apply.example.com/job"

    def test_job_url_fallback_when_no_apply_url(self):
        raw = _ashby_job(apply_url="", job_url="https://listing.example.com/job")
        result = self._normalize(raw)
        assert result["url"] == "https://listing.example.com/job"

    def test_description_text_cleaned(self):
        raw = _ashby_job(description_plain="<p>We love <b>Python</b>.</p>")
        result = self._normalize(raw)
        # HTML stripped, whitespace normalized
        assert "<p>" not in result["description_text"]
        assert "Python" in result["description_text"]

    def test_description_html_fallback(self):
        raw = _ashby_job(description_plain="")
        raw["descriptionHtml"] = "<p>HTML description</p>"
        result = self._normalize(raw)
        assert "HTML description" in result["description_text"]

    def test_posted_date_parsed_from_iso(self):
        raw = _ashby_job(published_at="2026-03-15T10:00:00Z")
        result = self._normalize(raw)
        assert isinstance(result["posted_date"], datetime)
        assert result["posted_date"].year == 2026
        assert result["posted_date"].month == 3
        assert result["posted_date"].day == 15

    def test_posted_date_none_when_missing(self):
        raw = _ashby_job(published_at=None)
        raw.pop("publishedAt", None)
        result = self._normalize(raw)
        assert result["posted_date"] is None

    def test_posted_date_none_on_bad_value(self):
        raw = _ashby_job(published_at="not-a-date")
        result = self._normalize(raw)
        assert result["posted_date"] is None

    def test_company_derived_from_slug_when_org_name_missing(self):
        raw = _ashby_job(org_name=None, slug="my-startup")
        raw["organizationName"] = None
        result = self._normalize(raw)
        assert result["company"] == "My Startup"

    def test_missing_fields_no_keyerror(self):
        # Minimal raw job with only _slug — should not raise
        result = self._normalize({"_slug": "bare-co"})
        assert result["title"] == ""
        assert result["url"] == ""
        assert result["external_id"] == "ashby_"

    def test_location_defaults_to_remote(self):
        raw = {"_slug": "co"}
        result = self._normalize(raw)
        assert result["location"] == "Remote"


# ===========================================================================
# GREENHOUSE
# ===========================================================================

class TestGreenhouseFetch:
    """Tests for GreenhouseConnector.fetch_jobs() via _fetch_company()."""

    _PATCH_GET = "connectors.greenhouse.requests.get"
    _PATCH_DB_SLUGS = "connectors.greenhouse._load_slugs_from_db"
    _PATCH_EXCL_SLUGS = "connectors.greenhouse._load_excluded_slugs"
    _PATCH_TARGET_ROLES = "connectors.greenhouse._load_target_roles"

    def _fetch_with_payload(self, jobs_list):
        payload = {"jobs": jobs_list}
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(payload)):
            from connectors.greenhouse import GreenhouseConnector
            return GreenhouseConnector().fetch_jobs()

    def test_success_returns_remote_jobs(self):
        job = _greenhouse_job()
        jobs = self._fetch_with_payload([job])
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Backend Engineer"

    def test_non_remote_location_filtered(self):
        job = _greenhouse_job(location_name="New York, NY")
        jobs = self._fetch_with_payload([job])
        assert jobs == []

    def test_remote_in_location_name_passes(self):
        for loc in ("Remote - US", "Anywhere", "Worldwide"):
            job = _greenhouse_job(location_name=loc)
            jobs = self._fetch_with_payload([job])
            assert len(jobs) == 1, f"Expected job with location '{loc}' to pass"

    def test_empty_slugs_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value=set()), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()):
            from connectors.greenhouse import GreenhouseConnector
            jobs = GreenhouseConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_error_mock(503)):
            from connectors.greenhouse import GreenhouseConnector
            jobs = GreenhouseConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, side_effect=RequestsConnectionError("timeout")):
            from connectors.greenhouse import GreenhouseConnector
            jobs = GreenhouseConnector().fetch_jobs()
        assert jobs == []

    def test_404_returns_empty_list_for_slug(self):
        mock = _json_mock({}, 404)
        mock.raise_for_status = MagicMock()
        with patch(self._PATCH_DB_SLUGS, return_value={"no-such-co"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=mock):
            from connectors.greenhouse import GreenhouseConnector
            jobs = GreenhouseConnector().fetch_jobs()
        assert jobs == []

    def test_deduplication_same_id_returned_once(self):
        job = _greenhouse_job(job_id=99)
        payload = {"jobs": [job, {**job}]}
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_EXCL_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(payload)):
            from connectors.greenhouse import GreenhouseConnector
            jobs = GreenhouseConnector().fetch_jobs()
        assert len(jobs) == 1


class TestGreenhouseNormalize:
    """Tests for GreenhouseConnector.normalize()."""

    def _normalize(self, raw_job):
        from connectors.greenhouse import GreenhouseConnector
        return GreenhouseConnector().normalize(raw_job)

    def test_basic_field_mapping(self):
        raw = _greenhouse_job()
        result = self._normalize(raw)
        assert result["title"] == "Backend Engineer"
        assert result["company"] == "Acme Corp"
        assert result["source"] == "greenhouse"
        assert result["url"] == "https://boards.greenhouse.io/acme/jobs/42"
        assert result["external_id"] == "greenhouse_42"
        assert result["remote_eligibility"] == "accept"

    def test_description_text_cleaned(self):
        raw = _greenhouse_job(content="<p>We use <strong>Go</strong> and Python.</p>")
        result = self._normalize(raw)
        assert "<p>" not in result["description_text"]
        assert "Go" in result["description_text"]
        assert "Python" in result["description_text"]

    def test_location_extracted_from_object(self):
        raw = _greenhouse_job(location_name="Remote - Europe")
        result = self._normalize(raw)
        assert result["location"] == "Remote - Europe"

    def test_location_defaults_when_missing(self):
        raw = _greenhouse_job()
        raw["location"] = {}
        result = self._normalize(raw)
        assert result["location"] == "Remote"

    def test_location_defaults_when_not_dict(self):
        raw = _greenhouse_job()
        raw["location"] = None
        result = self._normalize(raw)
        assert result["location"] == "Remote"

    def test_posted_date_from_first_published(self):
        raw = _greenhouse_job(first_published="2026-03-20T08:00:00Z")
        result = self._normalize(raw)
        assert isinstance(result["posted_date"], datetime)
        assert result["posted_date"].year == 2026

    def test_posted_date_falls_back_to_updated_at(self):
        raw = _greenhouse_job(first_published=None)
        raw["first_published"] = None
        raw["updated_at"] = "2026-02-10T12:00:00Z"
        result = self._normalize(raw)
        assert isinstance(result["posted_date"], datetime)
        assert result["posted_date"].month == 2

    def test_posted_date_none_when_no_dates(self):
        raw = _greenhouse_job(first_published=None)
        raw["first_published"] = None
        result = self._normalize(raw)
        assert result["posted_date"] is None

    def test_company_derived_from_slug_when_company_name_missing(self):
        raw = _greenhouse_job(company_name=None, slug="my-startup")
        raw["company_name"] = None
        result = self._normalize(raw)
        assert result["company"] == "My Startup"

    def test_missing_fields_no_keyerror(self):
        result = self._normalize({"_slug": "bare-co"})
        assert result["title"] == ""
        assert result["url"] == ""
        assert result["external_id"] == "greenhouse_"

    def test_raw_location_text_matches_location(self):
        raw = _greenhouse_job(location_name="Remote, Canada")
        result = self._normalize(raw)
        assert result["raw_location_text"] == result["location"]


# ===========================================================================
# LEVER
# ===========================================================================

class TestLeverFetch:
    """Tests for LeverConnector.fetch_jobs() via _fetch_company()."""

    _PATCH_GET = "connectors.lever.requests.get"
    _PATCH_DB_SLUGS = "connectors.lever._load_slugs_from_db"
    _PATCH_PROFILE_SLUGS = "connectors.lever._load_slugs_from_profile"
    _PATCH_TARGET_ROLES = "connectors.lever._load_target_roles"

    def _fetch_with_payload(self, jobs_list):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(jobs_list)):
            from connectors.lever import LeverConnector
            return LeverConnector().fetch_jobs()

    def test_success_list_response_returns_remote_jobs(self):
        job = _lever_job()
        jobs = self._fetch_with_payload([job])
        assert len(jobs) == 1
        assert jobs[0]["text"] == "ML Engineer"

    def test_success_dict_response_with_data_key(self):
        """Lever may return {"data": [...]} instead of a bare list."""
        job = _lever_job()
        payload = {"data": [job]}
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock(payload)):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_non_remote_location_filtered(self):
        job = _lever_job(location="San Francisco, CA")
        # Override categories to use a non-remote location
        job["categories"] = {"location": "San Francisco, CA", "commitment": "Full-time"}
        jobs = self._fetch_with_payload([job])
        assert jobs == []

    def test_remote_in_location_passes(self):
        for loc in ("Remote", "Anywhere", "Worldwide"):
            job = _lever_job(location=loc)
            job["categories"] = {"location": loc}
            jobs = self._fetch_with_payload([job])
            assert len(jobs) == 1, f"Expected job with location '{loc}' to pass"

    def test_remote_in_commitment_passes(self):
        job = _lever_job(location="Toronto")
        job["categories"] = {"location": "Toronto", "commitment": "Remote"}
        jobs = self._fetch_with_payload([job])
        assert len(jobs) == 1

    def test_empty_slugs_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value=set()), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_error_mock(503)):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, side_effect=RequestsConnectionError("timeout")):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert jobs == []

    def test_404_returns_empty_list_for_slug(self):
        mock = _json_mock({}, 404)
        mock.raise_for_status = MagicMock()
        with patch(self._PATCH_DB_SLUGS, return_value={"no-such-co"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=mock):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert jobs == []

    def test_deduplication_same_id_returned_once(self):
        job = _lever_job(job_id="dup-lever")
        with patch(self._PATCH_DB_SLUGS, return_value={"acme"}), \
             patch(self._PATCH_PROFILE_SLUGS, return_value=set()), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock([job, {**job}])):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_profile_slugs_merged_with_db_slugs(self):
        """Slugs from profile are union-ed with DB slugs."""
        job = _lever_job(slug="profile-co")
        job["_slug"] = "profile-co"
        with patch(self._PATCH_DB_SLUGS, return_value=set()), \
             patch(self._PATCH_PROFILE_SLUGS, return_value={"profile-co"}), \
             patch(self._PATCH_TARGET_ROLES, return_value=[]), \
             patch(self._PATCH_GET, return_value=_json_mock([job])):
            from connectors.lever import LeverConnector
            jobs = LeverConnector().fetch_jobs()
        assert len(jobs) == 1


class TestLeverNormalize:
    """Tests for LeverConnector.normalize()."""

    def _normalize(self, raw_job):
        from connectors.lever import LeverConnector
        return LeverConnector().normalize(raw_job)

    def test_basic_field_mapping(self):
        raw = _lever_job()
        result = self._normalize(raw)
        assert result["title"] == "ML Engineer"
        assert result["company"] == "Acme"
        assert result["source"] == "lever"
        assert result["url"] == "https://jobs.lever.co/acme/lever-001"
        assert result["external_id"] == "lever_lever-001"
        assert result["remote_eligibility"] == "accept"

    def test_hosted_url_used_when_present(self):
        raw = _lever_job(hosted_url="https://jobs.lever.co/acme/lever-001")
        result = self._normalize(raw)
        assert result["url"] == "https://jobs.lever.co/acme/lever-001"

    def test_url_built_from_slug_and_id_when_no_hosted_url(self):
        raw = _lever_job(hosted_url="", slug="myco", job_id="abc-123")
        raw["hostedUrl"] = ""
        result = self._normalize(raw)
        assert result["url"] == "https://jobs.lever.co/myco/abc-123"

    def test_company_derived_from_slug(self):
        raw = _lever_job(slug="big-data-startup")
        result = self._normalize(raw)
        assert result["company"] == "Big Data Startup"

    def test_description_text_cleaned(self):
        raw = _lever_job(description_plain="<p>Work with <em>Python</em> and Rust.</p>")
        result = self._normalize(raw)
        assert "<p>" not in result["description_text"]
        assert "Python" in result["description_text"]

    def test_description_plain_preferred(self):
        raw = _lever_job(description_plain="Plain text description")
        raw["description"] = "<p>HTML description</p>"
        result = self._normalize(raw)
        assert result["description"] == "Plain text description"

    def test_description_falls_back_to_lists(self):
        raw = _lever_job(description_plain="")
        raw["descriptionPlain"] = ""
        raw["description"] = ""
        raw["lists"] = [
            {"text": "Responsibilities", "content": "Build APIs"},
            {"text": "Requirements", "content": "Know Python"},
        ]
        result = self._normalize(raw)
        assert "Build APIs" in result["description"]
        assert "Know Python" in result["description"]

    def test_posted_date_from_created_at_ms(self):
        # 1742000000000 ms = some 2025 date
        raw = _lever_job(created_at=1742000000000)
        result = self._normalize(raw)
        assert isinstance(result["posted_date"], datetime)
        assert result["posted_date"].tzinfo is not None  # tz-aware

    def test_posted_date_none_when_missing(self):
        raw = _lever_job()
        raw.pop("createdAt", None)
        result = self._normalize(raw)
        assert result["posted_date"] is None

    def test_posted_date_none_on_bad_value(self):
        raw = _lever_job()
        raw["createdAt"] = "not-a-timestamp"
        result = self._normalize(raw)
        assert result["posted_date"] is None

    def test_location_from_all_locations(self):
        # allLocations wins when the location field is empty
        raw = _lever_job()
        raw["categories"] = {"location": "", "allLocations": ["Remote - EU"]}
        result = self._normalize(raw)
        assert result["location"] == "Remote - EU"

    def test_location_list_uses_first_element(self):
        raw = _lever_job()
        raw["categories"] = {"location": "", "allLocations": ["Remote - US", "Remote - EU"]}
        result = self._normalize(raw)
        assert result["location"] == "Remote - US"

    def test_location_defaults_to_remote_when_missing(self):
        raw = {"_slug": "co", "id": "x", "text": "Engineer"}
        result = self._normalize(raw)
        assert result["location"] == "Remote"

    def test_missing_fields_no_keyerror(self):
        result = self._normalize({"_slug": "bare-co"})
        assert result["title"] == ""
        assert result["external_id"] == "lever_"

    def test_raw_location_text_is_string(self):
        raw = _lever_job()
        raw["categories"] = {"location": ["Remote", "Anywhere"]}
        result = self._normalize(raw)
        assert isinstance(result["raw_location_text"], str)
