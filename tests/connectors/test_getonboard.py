"""
Tests for connectors/getonboard.py — GetOnBoardConnector.
All HTTP calls are mocked.
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_resp(payload, status=200):
    m = MagicMock()
    m.status_code = status
    m.raise_for_status = MagicMock()
    if status >= 400:
        from requests.exceptions import HTTPError
        m.raise_for_status.side_effect = HTTPError(str(status))
    m.json.return_value = payload
    return m


def _job(job_id="gob-1", title="Backend Engineer", modality="fully_remote",
         lang="en", published_at=None, company_name="Acme"):
    ts = published_at or int(datetime.now(tz=timezone.utc).timestamp())
    return {
        "id": job_id,
        "attributes": {
            "title": title,
            "remote_modality": modality,
            "lang": lang,
            "published_at": ts,
            "description": "Great role",
            "functions": "Build stuff",
            "projects": "",
            "countries": [],
            "company": {
                "data": {
                    "attributes": {"name": company_name}
                }
            },
        },
        "links": {"public_url": f"https://www.getonbrd.com/jobs/{job_id}"},
    }


def _page(jobs, total_pages=1):
    return {"data": jobs, "meta": {"total_pages": total_pages}}


# ---------------------------------------------------------------------------
# _load_allowed_lang_codes
# ---------------------------------------------------------------------------

class TestLoadAllowedLangCodes:
    def test_maps_english_to_en(self, tmp_path, monkeypatch):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump({"languages": ["English"]}))
        monkeypatch.chdir(tmp_path)
        import importlib
        import connectors.getonboard as m
        importlib.reload(m)
        assert m._load_allowed_lang_codes() == {"en"}

    def test_maps_multiple_languages(self, tmp_path, monkeypatch):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump({"languages": ["English", "Spanish", "French"]}))
        monkeypatch.chdir(tmp_path)
        import importlib
        import connectors.getonboard as m
        importlib.reload(m)
        codes = m._load_allowed_lang_codes()
        assert "en" in codes
        assert "es" in codes
        assert "fr" in codes

    def test_missing_profile_defaults_to_en(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import importlib
        import connectors.getonboard as m
        importlib.reload(m)
        assert m._load_allowed_lang_codes() == {"en"}

    def test_empty_languages_defaults_to_en(self, tmp_path, monkeypatch):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump({"languages": []}))
        monkeypatch.chdir(tmp_path)
        import importlib
        import connectors.getonboard as m
        importlib.reload(m)
        assert m._load_allowed_lang_codes() == {"en"}

    def test_unknown_language_ignored(self, tmp_path, monkeypatch):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump({"languages": ["Klingon"]}))
        monkeypatch.chdir(tmp_path)
        import importlib
        import connectors.getonboard as m
        importlib.reload(m)
        # Klingon not in map → empty set → default "en"
        assert m._load_allowed_lang_codes() == {"en"}


# ---------------------------------------------------------------------------
# _fetch_category
# ---------------------------------------------------------------------------

class TestFetchCategory:
    _T = "connectors.getonboard.requests.get"

    def _connector(self):
        from connectors.getonboard import GetOnBoardConnector
        return GetOnBoardConnector()

    def test_returns_fully_remote_jobs(self):
        connector = self._connector()
        data = _page([_job()])
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert len(jobs) == 1

    def test_skips_non_remote_modality(self):
        connector = self._connector()
        data = _page([_job(modality="hybrid")])
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert jobs == []

    def test_skips_disallowed_language(self):
        connector = self._connector()
        data = _page([_job(lang="es")])
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert jobs == []

    def test_allows_lang_not_specified(self):
        connector = self._connector()
        data = _page([_job(lang="lang_not_specified")])
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert len(jobs) == 1

    def test_deduplicates_by_id(self):
        connector = self._connector()
        data = _page([_job(job_id="dup")])
        seen = {"dup"}
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", seen, {"en"})
        assert jobs == []

    def test_stops_on_empty_results(self):
        connector = self._connector()
        data = {"data": [], "meta": {"total_pages": 5}}
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert jobs == []

    def test_filters_old_jobs_and_stops_early(self):
        connector = self._connector()
        old_ts = int((datetime.now(tz=timezone.utc) - timedelta(days=20)).timestamp())
        data = _page([_job(published_at=old_ts)])
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_category("programming", set(), {"en"})
        assert jobs == []

    def test_stops_at_last_page(self):
        connector = self._connector()
        data = _page([_job()], total_pages=1)
        with patch(self._T, return_value=_mock_resp(data)) as mock_get:
            connector._fetch_category("programming", set(), {"en"})
        assert mock_get.call_count == 1

    def test_paginates_up_to_max_pages(self):
        from connectors.getonboard import MAX_PAGES
        connector = self._connector()
        data = _page([_job()], total_pages=10)
        with patch(self._T, return_value=_mock_resp(data)) as mock_get:
            connector._fetch_category("programming", set(), {"en"})
        assert mock_get.call_count == MAX_PAGES

    def test_http_error_propagates(self):
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp({}, 500)):
            with pytest.raises(Exception):
                connector._fetch_category("programming", set(), {"en"})


# ---------------------------------------------------------------------------
# fetch_jobs
# ---------------------------------------------------------------------------

class TestFetchJobs:
    _T = "connectors.getonboard.requests.get"

    def test_returns_jobs_from_categories(self):
        from connectors.getonboard import GetOnBoardConnector
        data = _page([_job()])
        with patch(self._T, return_value=_mock_resp(data)), \
             patch("connectors.getonboard._load_allowed_lang_codes", return_value={"en"}):
            jobs = GetOnBoardConnector().fetch_jobs()
        assert len(jobs) > 0

    def test_category_error_continues(self):
        from connectors.getonboard import GetOnBoardConnector
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("API error")
            return _mock_resp(_page([_job()]))

        with patch(self._T, side_effect=side_effect), \
             patch("connectors.getonboard._load_allowed_lang_codes", return_value={"en"}):
            jobs = GetOnBoardConnector().fetch_jobs()
        # First category errored, rest succeeded
        assert len(jobs) >= 1

    def test_source_name(self):
        from connectors.getonboard import GetOnBoardConnector
        assert GetOnBoardConnector().get_source_name() == "getonboard"


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------

class TestNormalize:
    def _connector(self):
        from connectors.getonboard import GetOnBoardConnector
        return GetOnBoardConnector()

    def test_basic_fields(self):
        raw = _job()
        result = self._connector().normalize(raw)
        assert result["source"] == "getonboard"
        assert result["title"] == "Backend Engineer"
        assert result["company"] == "Acme"
        assert result["external_id"] == "gob-1"

    def test_url_from_links(self):
        raw = _job()
        result = self._connector().normalize(raw)
        assert "getonbrd.com" in result["url"]

    def test_url_fallback_when_no_links(self):
        raw = _job()
        raw["links"] = {}
        result = self._connector().normalize(raw)
        assert "getonbrd.com/jobs/gob-1" in result["url"]

    def test_countries_joined_as_location(self):
        raw = _job()
        raw["attributes"]["countries"] = ["US", "CA", "UK"]
        result = self._connector().normalize(raw)
        assert "US" in result["location"]
        assert "CA" in result["location"]

    def test_countries_remote_falls_back(self):
        raw = _job()
        raw["attributes"]["countries"] = ["Remote"]
        result = self._connector().normalize(raw)
        assert result["location"] == "Remote"

    def test_empty_countries_is_remote(self):
        raw = _job()
        result = self._connector().normalize(raw)
        assert result["location"] == "Remote"

    def test_description_concatenated(self):
        raw = _job()
        raw["attributes"]["description"] = "Part A"
        raw["attributes"]["functions"] = "Part B"
        raw["attributes"]["projects"] = "Part C"
        result = self._connector().normalize(raw)
        assert "Part A" in result["description"]
        assert "Part B" in result["description"]
        assert "Part C" in result["description"]

    def test_posted_date_from_unix_timestamp(self):
        raw = _job()
        ts = int(datetime(2026, 3, 24, tzinfo=timezone.utc).timestamp())
        raw["attributes"]["published_at"] = ts
        result = self._connector().normalize(raw)
        assert isinstance(result["posted_date"], datetime)
        assert result["posted_date"].year == 2026

    def test_bad_published_at_gives_none(self):
        raw = _job()
        raw["attributes"]["published_at"] = "not-a-number"
        result = self._connector().normalize(raw)
        assert result["posted_date"] is None

    def test_none_published_at_gives_none(self):
        raw = _job()
        raw["attributes"]["published_at"] = None
        result = self._connector().normalize(raw)
        assert result["posted_date"] is None

    def test_remote_eligibility_is_none(self):
        result = self._connector().normalize(_job())
        assert result["remote_eligibility"] is None

    def test_company_fallback_when_not_dict(self):
        raw = _job()
        raw["attributes"]["company"] = "FlatString"
        result = self._connector().normalize(raw)
        assert result["company"] == "Unknown"

    def test_required_keys_present(self):
        result = self._connector().normalize(_job())
        for key in ("external_id", "source", "company", "title", "location",
                    "description", "url", "posted_date", "remote_eligibility"):
            assert key in result
