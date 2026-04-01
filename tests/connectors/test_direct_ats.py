"""
Tests for connectors/direct_ats.py — pure helper functions and per-ATS fetchers.

All HTTP calls are mocked. profile.yaml loading uses tmp_path + monkeypatch.
"""
import os
from unittest.mock import patch, MagicMock

import pytest
import yaml


# ---------------------------------------------------------------------------
# _parse_careers_url
# ---------------------------------------------------------------------------

class TestParseCareersUrl:
    def _call(self, url):
        from connectors.direct_ats import _parse_careers_url
        return _parse_careers_url(url)

    def test_ashby_url(self):
        ats, slug = self._call("https://jobs.ashbyhq.com/acme/posting")
        assert ats == "ashby"
        assert slug == "acme"

    def test_greenhouse_url(self):
        ats, slug = self._call("https://boards.greenhouse.io/deepco")
        assert ats == "greenhouse"
        assert slug == "deepco"

    def test_greenhouse_job_boards_url(self):
        ats, slug = self._call("https://job-boards.greenhouse.io/acme")
        assert ats == "greenhouse"
        assert slug == "acme"

    def test_lever_url(self):
        ats, slug = self._call("https://jobs.lever.co/startup")
        assert ats == "lever"
        assert slug == "startup"

    def test_workable_url(self):
        ats, slug = self._call("https://apply.workable.com/unicorn")
        assert ats == "workable"
        assert slug == "unicorn"

    def test_unknown_host_returns_unknown(self):
        ats, slug = self._call("https://careers.somecompany.com/jobs")
        assert ats == "unknown"

    def test_empty_string_returns_unknown(self):
        ats, slug = self._call("")
        assert ats == "unknown"


# ---------------------------------------------------------------------------
# _title_is_relevant
# ---------------------------------------------------------------------------

class TestTitleIsRelevant:
    def _call(self, title, roles):
        from connectors.direct_ats import _title_is_relevant
        return _title_is_relevant(title, roles)

    def test_empty_roles_always_relevant(self):
        assert self._call("Sales Manager", []) is True

    def test_matching_word_returns_true(self):
        assert self._call("Senior Backend Engineer", ["software engineer"]) is True

    def test_non_matching_returns_false(self):
        assert self._call("Sales Executive", ["software engineer", "backend developer"]) is False

    def test_short_words_ignored(self):
        # Words ≤3 chars are not checked ("ml" is 2 chars → ignored)
        assert self._call("ML Lead", ["ml"]) is False

    def test_case_insensitive(self):
        assert self._call("BACKEND ENGINEER", ["backend developer"]) is True


# ---------------------------------------------------------------------------
# _load_target_companies — reads profile.yaml
# ---------------------------------------------------------------------------

class TestLoadTargetCompanies:
    def _write_profile(self, tmp_path, data):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump(data))
        return str(tmp_path)

    def test_returns_companies_with_careers_url(self, tmp_path, monkeypatch):
        self._write_profile(tmp_path, {
            "target_companies": [
                {"name": "Acme", "careers_url": "https://jobs.lever.co/acme"},
                {"name": "NoURL"},
            ]
        })
        monkeypatch.chdir(tmp_path)
        from importlib import reload
        import connectors.direct_ats as m
        companies = m._load_target_companies()
        assert len(companies) == 1
        assert companies[0]["name"] == "Acme"

    def test_missing_profile_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import connectors.direct_ats as m
        assert m._load_target_companies() == []

    def test_empty_target_companies_returns_empty(self, tmp_path, monkeypatch):
        self._write_profile(tmp_path, {"target_companies": []})
        monkeypatch.chdir(tmp_path)
        import connectors.direct_ats as m
        assert m._load_target_companies() == []


# ---------------------------------------------------------------------------
# _load_target_roles — reads profile.yaml
# ---------------------------------------------------------------------------

class TestLoadTargetRoles:
    def test_returns_lowercased_roles(self, tmp_path, monkeypatch):
        p = tmp_path / "profile.yaml"
        p.write_text(yaml.dump({"target_roles": ["Backend Engineer", "ML Engineer"]}))
        monkeypatch.chdir(tmp_path)
        import connectors.direct_ats as m
        roles = m._load_target_roles()
        assert "backend engineer" in roles
        assert "ml engineer" in roles

    def test_missing_profile_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import connectors.direct_ats as m
        assert m._load_target_roles() == []


# ---------------------------------------------------------------------------
# Per-ATS fetchers — mocked HTTP
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


class TestFetchAshby:
    _T = "connectors.direct_ats.requests.get"

    def _job(self, title="Backend Engineer", is_remote=True, workplace="remote"):
        return {"id": "ashby-1", "title": title, "isRemote": is_remote,
                "workplaceType": workplace, "applyUrl": "https://ashbyhq.com/apply/1",
                "descriptionPlain": "Great role", "publishedAt": "2026-03-24T00:00:00Z"}

    def test_returns_remote_jobs(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job()]})):
            from connectors.direct_ats import _fetch_ashby
            jobs = _fetch_ashby("acme", "Acme Corp", [])
        assert len(jobs) == 1
        assert jobs[0]["_ats"] == "ashby"

    def test_skips_non_remote_jobs(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job(is_remote=False, workplace="onsite")]})):
            from connectors.direct_ats import _fetch_ashby
            assert _fetch_ashby("acme", "Acme", []) == []

    def test_404_returns_empty(self):
        with patch(self._T, return_value=_mock_resp({}, 404)):
            from connectors.direct_ats import _fetch_ashby
            assert _fetch_ashby("bad-slug", "Co", []) == []

    def test_title_filter_applied(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job(title="Sales Manager")]})):
            from connectors.direct_ats import _fetch_ashby
            jobs = _fetch_ashby("acme", "Acme", ["backend engineer"])
        assert jobs == []


class TestFetchGreenhouse:
    _T = "connectors.direct_ats.requests.get"

    def _job(self, title="Backend Engineer", location="Remote"):
        return {"id": 1, "title": title, "location": {"name": location},
                "absolute_url": "https://greenhouse.io/apply/1",
                "content": "Great role"}

    def test_returns_remote_jobs(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job()]})):
            from connectors.direct_ats import _fetch_greenhouse
            jobs = _fetch_greenhouse("deepco", "DeepCo", [])
        assert len(jobs) == 1
        assert jobs[0]["_ats"] == "greenhouse"

    def test_skips_non_remote_locations(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job(location="New York, NY")]})):
            from connectors.direct_ats import _fetch_greenhouse
            assert _fetch_greenhouse("deepco", "DeepCo", []) == []

    def test_accepts_worldwide_location(self):
        with patch(self._T, return_value=_mock_resp({"jobs": [self._job(location="Worldwide")]})):
            from connectors.direct_ats import _fetch_greenhouse
            assert len(_fetch_greenhouse("deepco", "DeepCo", [])) == 1

    def test_404_returns_empty(self):
        with patch(self._T, return_value=_mock_resp({}, 404)):
            from connectors.direct_ats import _fetch_greenhouse
            assert _fetch_greenhouse("bad", "Co", []) == []


class TestFetchLever:
    _T = "connectors.direct_ats.requests.get"

    def _job(self, title="Backend Eng", location="Remote"):
        return {"id": "lever-1", "text": title,
                "categories": {"location": location, "commitment": "Full-time"},
                "hostedUrl": "https://jobs.lever.co/co/1",
                "descriptionPlain": "role"}

    def test_returns_remote_jobs(self):
        with patch(self._T, return_value=_mock_resp([self._job()])):
            from connectors.direct_ats import _fetch_lever
            jobs = _fetch_lever("startup", "Startup", [])
        assert len(jobs) == 1
        assert jobs[0]["_ats"] == "lever"

    def test_skips_non_remote(self):
        with patch(self._T, return_value=_mock_resp([self._job(location="San Francisco")])):
            from connectors.direct_ats import _fetch_lever
            assert _fetch_lever("startup", "Startup", []) == []

    def test_handles_dict_response_format(self):
        with patch(self._T, return_value=_mock_resp({"data": [self._job()]})):
            from connectors.direct_ats import _fetch_lever
            jobs = _fetch_lever("startup", "Startup", [])
        assert len(jobs) == 1

    def test_404_returns_empty(self):
        with patch(self._T, return_value=_mock_resp({}, 404)):
            from connectors.direct_ats import _fetch_lever
            assert _fetch_lever("bad", "Co", []) == []


class TestFetchWorkable:
    _T = "connectors.direct_ats.requests.post"

    def _job(self, title="Backend", remote=True, state="published"):
        return {"id": "wb-1", "title": title, "remote": remote, "state": state,
                "url": "https://apply.workable.com/co/j/1", "description": "role",
                "location": {"country": "Remote"}}

    def test_returns_remote_published_jobs(self):
        with patch(self._T, return_value=_mock_resp({"results": [self._job()]})):
            from connectors.direct_ats import _fetch_workable
            jobs = _fetch_workable("co", "Co", [])
        assert len(jobs) == 1
        assert jobs[0]["_ats"] == "workable"

    def test_skips_non_remote(self):
        with patch(self._T, return_value=_mock_resp({"results": [self._job(remote=False)]})):
            from connectors.direct_ats import _fetch_workable
            assert _fetch_workable("co", "Co", []) == []

    def test_skips_unpublished(self):
        with patch(self._T, return_value=_mock_resp({"results": [self._job(state="draft")]})):
            from connectors.direct_ats import _fetch_workable
            assert _fetch_workable("co", "Co", []) == []

    def test_404_returns_empty(self):
        with patch(self._T, return_value=_mock_resp({}, 404)):
            from connectors.direct_ats import _fetch_workable
            assert _fetch_workable("bad", "Co", []) == []


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------

class TestNormalizeAshby:
    def _n(self, raw):
        from connectors.direct_ats import _normalize_ashby
        return _normalize_ashby(raw)

    def test_basic_fields(self):
        raw = {"id": "1", "title": "Engineer", "_company_name": "Acme", "_slug": "acme",
               "applyUrl": "https://ashbyhq.com/1", "location": "Remote",
               "descriptionPlain": "role", "publishedAt": "2026-03-24T00:00:00Z"}
        r = self._n(raw)
        assert r["source"] == "direct_ats"
        assert r["company"] == "Acme"
        assert r["title"] == "Engineer"

    def test_date_parsed(self):
        from datetime import datetime
        raw = {"id": "1", "title": "E", "_company_name": "Co", "_slug": "co",
               "publishedAt": "2026-03-24T00:00:00Z", "descriptionPlain": ""}
        assert isinstance(self._n(raw)["posted_date"], datetime)

    def test_bad_date_gives_none(self):
        raw = {"id": "1", "title": "E", "_company_name": "Co", "_slug": "co",
               "publishedAt": "bad-date", "descriptionPlain": ""}
        assert self._n(raw)["posted_date"] is None


class TestNormalizeGreenhouse:
    def _n(self, raw):
        from connectors.direct_ats import _normalize_greenhouse
        return _normalize_greenhouse(raw)

    def test_basic_fields(self):
        raw = {"id": 1, "title": "Engineer", "_company_name": "Acme", "_slug": "acme",
               "absolute_url": "https://greenhouse.io/1",
               "location": {"name": "Remote"},
               "content": "role", "updated_at": "2026-03-24T00:00:00.000Z"}
        r = self._n(raw)
        assert r["source"] == "direct_ats"
        assert r["title"] == "Engineer"


class TestNormalizeLever:
    def _n(self, raw):
        from connectors.direct_ats import _normalize_lever
        return _normalize_lever(raw)

    def test_basic_fields(self):
        raw = {"id": "l1", "text": "Engineer", "_company_name": "Acme", "_slug": "acme",
               "hostedUrl": "https://jobs.lever.co/acme/l1",
               "categories": {"location": "Remote"},
               "descriptionPlain": "role", "createdAt": 1742000000000}
        r = self._n(raw)
        assert r["source"] == "direct_ats"
        assert r["title"] == "Engineer"


class TestNormalizeWorkable:
    def _n(self, raw):
        from connectors.direct_ats import _normalize_workable
        return _normalize_workable(raw)

    def test_basic_fields(self):
        raw = {"id": "wb1", "title": "Engineer", "_company_name": "Acme", "_slug": "acme",
               "url": "https://apply.workable.com/acme/j/1",
               "location": {"country": "Remote"},
               "description": "role", "published_at": "2026-03-24T00:00:00.000Z"}
        r = self._n(raw)
        assert r["source"] == "direct_ats"
        assert r["title"] == "Engineer"
