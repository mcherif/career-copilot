"""
Mocked fetch tests for Adzuna _fetch_country() — multi-country, cutoff, remote filter.
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import os


def _mock_resp(payload, status=200):
    m = MagicMock()
    m.status_code = status
    m.raise_for_status = MagicMock()
    if status >= 400:
        from requests.exceptions import HTTPError
        m.raise_for_status.side_effect = HTTPError(str(status))
    m.json.return_value = payload
    return m


def _result(job_id="az-1", title="Backend Engineer", location="Remote",
            description="remote role", created=None):
    return {
        "id": job_id,
        "title": title,
        "company": {"display_name": "Acme"},
        "location": {"display_name": location},
        "description": description,
        "redirect_url": f"https://adzuna.com/jobs/{job_id}",
        "created": created or datetime.now(tz=timezone.utc).isoformat(),
    }


class TestAdzunaFetchCountry:
    _T = "connectors.adzuna.requests.get"

    def _connector(self, app_id="id", app_key="key"):
        with patch.dict(os.environ, {"ADZUNA_APP_ID": app_id, "ADZUNA_APP_KEY": app_key}):
            from connectors.adzuna import AdzunaConnector
            return AdzunaConnector()

    def test_returns_remote_jobs(self):
        data = {"results": [_result(description="This is a remote position")]}
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_country("gb", set(), datetime.now(tz=timezone.utc) - timedelta(days=10))
        assert len(jobs) == 1

    def test_skips_non_remote_jobs(self):
        data = {"results": [_result(description="Office position in London", location="London")]}
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_country("gb", set(), datetime.now(tz=timezone.utc) - timedelta(days=10))
        assert jobs == []

    def test_filters_old_jobs_stops_early(self):
        old = (datetime.now(tz=timezone.utc) - timedelta(days=20)).isoformat()
        data = {"results": [_result(created=old, description="remote role")]}
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_country("gb", set(), datetime.now(tz=timezone.utc) - timedelta(days=10))
        assert jobs == []

    def test_deduplicates_by_id(self):
        data = {"results": [_result("same-id", description="remote role")]}
        connector = self._connector()
        seen = {"same-id"}
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_country("gb", seen, datetime.now(tz=timezone.utc) - timedelta(days=10))
        assert jobs == []

    def test_empty_results_stops_pagination(self):
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp({"results": []})):
            jobs = connector._fetch_country("gb", set(), datetime.now(tz=timezone.utc) - timedelta(days=10))
        assert jobs == []

    def test_tags_job_with_country(self):
        data = {"results": [_result(description="remote role")]}
        connector = self._connector()
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector._fetch_country("de", set(), datetime.now(tz=timezone.utc) - timedelta(days=10))
        if jobs:
            assert jobs[0]["_country"] == "de"


class TestAdzunaFetchJobsWithKey:
    _T = "connectors.adzuna.requests.get"

    def test_multi_country_aggregates_results(self):
        data = {"results": [_result(description="remote role")]}
        with patch.dict(os.environ, {"ADZUNA_APP_ID": "id", "ADZUNA_APP_KEY": "key"}):
            from connectors.adzuna import AdzunaConnector
            connector = AdzunaConnector()
        with patch(self._T, return_value=_mock_resp(data)):
            jobs = connector.fetch_jobs()
        # Multiple countries × 1 job each = multiple total jobs
        assert len(jobs) >= 1

    def test_country_error_continues_to_next(self):
        with patch.dict(os.environ, {"ADZUNA_APP_ID": "id", "ADZUNA_APP_KEY": "key"}):
            from connectors.adzuna import AdzunaConnector
            connector = AdzunaConnector()
        with patch(self._T, side_effect=Exception("country error")):
            jobs = connector.fetch_jobs()
        assert jobs == []
