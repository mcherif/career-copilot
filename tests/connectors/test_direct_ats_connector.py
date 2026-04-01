"""
Tests for DirectATSConnector.fetch_jobs() and normalize() — the orchestrating class.
Pure helper functions are tested in test_direct_ats.py.
"""
from unittest.mock import patch, MagicMock


def _mock_job(ats="ashby", slug="acme"):
    return {"id": "job-1", "title": "Backend Engineer", "_ats": ats,
            "_slug": slug, "_company_name": "Acme",
            "applyUrl": "https://ashbyhq.com/1", "location": "Remote",
            "descriptionPlain": "role", "publishedAt": "2026-03-24T00:00:00Z"}


class TestDirectATSConnectorFetch:
    def test_returns_empty_when_no_companies(self):
        with patch("connectors.direct_ats._load_target_companies", return_value=[]):
            from connectors.direct_ats import DirectATSConnector
            assert DirectATSConnector().fetch_jobs() == []

    def test_skips_unknown_ats(self):
        companies = [{"name": "Acme", "careers_url": "https://careers.acme.com/jobs"}]
        with patch("connectors.direct_ats._load_target_companies", return_value=companies), \
             patch("connectors.direct_ats._load_target_roles", return_value=[]):
            from connectors.direct_ats import DirectATSConnector
            jobs = DirectATSConnector().fetch_jobs()
        assert jobs == []

    def test_fetches_from_known_ats(self):
        companies = [{"name": "Acme", "careers_url": "https://jobs.ashbyhq.com/acme"}]
        mock_fetcher = MagicMock(return_value=[_mock_job()])
        with patch("connectors.direct_ats._load_target_companies", return_value=companies), \
             patch("connectors.direct_ats._load_target_roles", return_value=[]), \
             patch.dict("connectors.direct_ats._FETCHERS", {"ashby": mock_fetcher}):
            from connectors.direct_ats import DirectATSConnector
            jobs = DirectATSConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_deduplicates_across_companies(self):
        companies = [
            {"name": "Acme", "careers_url": "https://jobs.ashbyhq.com/acme"},
            {"name": "Acme2", "careers_url": "https://jobs.ashbyhq.com/acme"},
        ]
        same_job = _mock_job(slug="acme")
        mock_fetcher = MagicMock(return_value=[same_job])
        with patch("connectors.direct_ats._load_target_companies", return_value=companies), \
             patch("connectors.direct_ats._load_target_roles", return_value=[]), \
             patch.dict("connectors.direct_ats._FETCHERS", {"ashby": mock_fetcher}):
            from connectors.direct_ats import DirectATSConnector
            jobs = DirectATSConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_fetch_error_continues_to_next(self):
        companies = [
            {"name": "Bad", "careers_url": "https://jobs.ashbyhq.com/bad"},
            {"name": "Good", "careers_url": "https://boards.greenhouse.io/good"},
        ]
        mock_ashby = MagicMock(side_effect=Exception("network"))
        mock_greenhouse = MagicMock(return_value=[_mock_job("greenhouse", "good")])
        with patch("connectors.direct_ats._load_target_companies", return_value=companies), \
             patch("connectors.direct_ats._load_target_roles", return_value=[]), \
             patch.dict("connectors.direct_ats._FETCHERS", {"ashby": mock_ashby, "greenhouse": mock_greenhouse}):
            from connectors.direct_ats import DirectATSConnector
            jobs = DirectATSConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_source_name(self):
        from connectors.direct_ats import DirectATSConnector
        assert DirectATSConnector().get_source_name() == "direct_ats"


class TestDirectATSConnectorNormalize:
    def test_dispatches_to_ashby_normalizer(self):
        raw = {"_ats": "ashby", "id": "1", "title": "Dev", "_company_name": "Co",
               "_slug": "co", "descriptionPlain": "role",
               "applyUrl": "https://ashbyhq.com/1", "location": "Remote",
               "publishedAt": "2026-03-24T00:00:00Z"}
        from connectors.direct_ats import DirectATSConnector
        result = DirectATSConnector().normalize(raw)
        assert result["source"] == "direct_ats"
        assert result["title"] == "Dev"

    def test_unknown_ats_returns_empty_dict(self):
        raw = {"_ats": "unknown_platform", "title": "Dev"}
        from connectors.direct_ats import DirectATSConnector
        assert DirectATSConnector().normalize(raw) == {}

    def test_missing_ats_key_returns_empty_dict(self):
        from connectors.direct_ats import DirectATSConnector
        assert DirectATSConnector().normalize({"title": "Dev"}) == {}
