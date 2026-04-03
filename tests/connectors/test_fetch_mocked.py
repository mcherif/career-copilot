"""
Mocked fetch tests for connectors that use HTTP (RSS and JSON APIs).

No live network calls — requests.get is patched with fixture content.
Covers: fetch_jobs() success path, cutoff filtering, error handling,
multi-feed deduplication, and empty/malformed responses.
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

_RECENT_PUB_DATE = (
    datetime.now(tz=timezone.utc) - timedelta(days=3)
).strftime("%a, %d %b %Y %H:%M:%S +0000")


# ---------------------------------------------------------------------------
# RSS fixture builders
# ---------------------------------------------------------------------------

def _rss_envelope(items_xml: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    {items_xml}
  </channel>
</rss>""".encode()


def _rwfa_item(title="Senior Engineer at Acme",
               url="https://www.realworkfromanywhere.com/jobs/slug-1",
               pub_date=None,
               description="Python role") -> str:
    pub_date = pub_date or _RECENT_PUB_DATE
    return f"""<item>
      <title>{title}</title>
      <guid>{url}</guid>
      <description>{description}</description>
      <pubDate>{pub_date}</pubDate>
    </item>"""


def _eu_item(title="Backend Dev | Acme Corp",
             url="https://euremotejobs.com/job/backend-dev/",
             pub_date=None,
             description="EU remote role") -> str:
    pub_date = pub_date or _RECENT_PUB_DATE
    return f"""<item>
      <title>{title}</title>
      <link>{url}</link>
      <pubDate>{pub_date}</pubDate>
      <description>{description}</description>
    </item>"""


def _wwr_item(title="Acme: Backend Engineer",
              url="https://weworkremotely.com/remote-jobs/acme-backend",
              pub_date=None) -> str:
    pub_date = pub_date or _RECENT_PUB_DATE
    # WWR uses link.tail for URL in stdlib ET
    return f"""<item>
      <title>{title}</title>
      <link></link>
      <guid>{url}</guid>
      <pubDate>{pub_date}</pubDate>
      <description>Job description</description>
    </item>"""


def _mock_response(content: bytes, status_code: int = 200):
    mock = MagicMock()
    mock.content = content
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests.exceptions import HTTPError
        mock.raise_for_status.side_effect = HTTPError(f"{status_code}")
    return mock


# ---------------------------------------------------------------------------
# RealWorkFromAnywhere — fetch_jobs()
# ---------------------------------------------------------------------------

class TestRWFAFetch:
    _TARGET = "connectors.realworkfromanywhere.requests.get"

    def test_returns_jobs_from_feed(self):
        xml = _rss_envelope(_rwfa_item())
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Senior Engineer"
        assert jobs[0]["company"] == "Acme"

    def test_filters_out_old_jobs(self):
        old_date = "Mon, 01 Jan 2024 00:00:00 +0000"
        xml = _rss_envelope(_rwfa_item(pub_date=old_date))
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert jobs == []

    def test_keeps_recent_jobs(self):
        recent = datetime.now(tz=timezone.utc) - timedelta(days=3)
        pub_date = recent.strftime("%a, %d %b %Y %H:%M:%S +0000")
        xml = _rss_envelope(_rwfa_item(pub_date=pub_date))
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_http_error_returns_empty_list(self):
        with patch(self._TARGET, return_value=_mock_response(b"", 503)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._TARGET, side_effect=Exception("timeout")):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert jobs == []

    def test_empty_feed_returns_empty_list(self):
        xml = _rss_envelope("")
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert jobs == []

    def test_multiple_items_all_returned(self):
        items = _rwfa_item(url="https://x.com/1") + _rwfa_item(
            title="ML Engineer at DeepCo", url="https://x.com/2"
        )
        xml = _rss_envelope(items)
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert len(jobs) == 2

    def test_no_channel_returns_empty_list(self):
        xml = b'<?xml version="1.0"?><rss version="2.0"></rss>'
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.realworkfromanywhere import RealWorkFromAnywhereConnector
            jobs = RealWorkFromAnywhereConnector().fetch_jobs()
        assert jobs == []


# ---------------------------------------------------------------------------
# EURemoteJobs — fetch_jobs()
# ---------------------------------------------------------------------------

class TestEUFetch:
    _TARGET = "connectors.euremotejobs.requests.get"

    def test_returns_jobs_from_feed(self):
        xml = _rss_envelope(_eu_item())
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.euremotejobs import EURemoteJobsConnector
            jobs = EURemoteJobsConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Backend Dev"
        assert jobs[0]["company"] == "Acme Corp"

    def test_filters_old_jobs(self):
        old_date = "Mon, 01 Jan 2024 00:00:00 +0000"
        xml = _rss_envelope(_eu_item(pub_date=old_date))
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.euremotejobs import EURemoteJobsConnector
            jobs = EURemoteJobsConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty_list(self):
        with patch(self._TARGET, return_value=_mock_response(b"", 503)):
            from connectors.euremotejobs import EURemoteJobsConnector
            jobs = EURemoteJobsConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._TARGET, side_effect=Exception("timeout")):
            from connectors.euremotejobs import EURemoteJobsConnector
            jobs = EURemoteJobsConnector().fetch_jobs()
        assert jobs == []

    def test_no_channel_returns_empty_list(self):
        xml = b'<?xml version="1.0"?><rss version="2.0"></rss>'
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.euremotejobs import EURemoteJobsConnector
            jobs = EURemoteJobsConnector().fetch_jobs()
        assert jobs == []


# ---------------------------------------------------------------------------
# WeWorkRemotely — fetch_jobs()
# ---------------------------------------------------------------------------

class TestWWRFetch:
    _TARGET = "connectors.weworkremotely.requests.get"

    def test_returns_jobs_from_feed(self):
        xml = _rss_envelope(_wwr_item())
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.weworkremotely import WeWorkRemotelyConnector
            jobs = WeWorkRemotelyConnector().fetch_jobs()
        assert len(jobs) >= 1
        assert jobs[0]["company"] == "Acme"
        assert jobs[0]["title"] == "Backend Engineer"

    def test_deduplicates_across_feeds(self):
        # Same item returned from both feeds — should appear once
        xml = _rss_envelope(_wwr_item(url="https://weworkremotely.com/jobs/same-slug"))
        with patch(self._TARGET, return_value=_mock_response(xml)):
            from connectors.weworkremotely import WeWorkRemotelyConnector
            jobs = WeWorkRemotelyConnector().fetch_jobs()
        ids = [j["id"] for j in jobs]
        assert len(ids) == len(set(ids))

    def test_one_feed_error_continues_to_next(self):
        responses = [
            _mock_response(b"", 503),   # first feed fails
            _mock_response(_rss_envelope(_wwr_item()), 200),  # second succeeds
        ]
        with patch(self._TARGET, side_effect=responses):
            from connectors.weworkremotely import WeWorkRemotelyConnector
            jobs = WeWorkRemotelyConnector().fetch_jobs()
        # Should get jobs from the second feed despite first failing
        assert len(jobs) >= 1

    def test_all_feeds_fail_returns_empty_list(self):
        with patch(self._TARGET, side_effect=Exception("timeout")):
            from connectors.weworkremotely import WeWorkRemotelyConnector
            jobs = WeWorkRemotelyConnector().fetch_jobs()
        assert jobs == []


# ---------------------------------------------------------------------------
# Remotive — fetch_jobs()
# ---------------------------------------------------------------------------

class TestRemotiveFetch:
    _TARGET = "connectors.remotive.requests.get"

    def _mock_json_response(self, jobs_list):
        mock = MagicMock()
        mock.raise_for_status = MagicMock()
        mock.json.return_value = {"jobs": jobs_list}
        return mock

    def _sample_job(self, job_id=1):
        return {
            "id": job_id,
            "url": f"https://remotive.com/jobs/{job_id}",
            "title": "Backend Engineer",
            "company_name": "Acme",
            "candidate_required_location": "Worldwide",
            "description": "<p>Python role</p>",
            "publication_date": "2026-03-15T00:00:00",
        }

    def test_returns_jobs_from_api(self):
        with patch(self._TARGET, return_value=self._mock_json_response([self._sample_job()])):
            from connectors.remotive import RemotiveConnector
            jobs = RemotiveConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["title"] == "Backend Engineer"

    def test_empty_jobs_list_returns_empty(self):
        with patch(self._TARGET, return_value=self._mock_json_response([])):
            from connectors.remotive import RemotiveConnector
            jobs = RemotiveConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty_list(self):
        from requests.exceptions import HTTPError
        mock = MagicMock()
        mock.raise_for_status.side_effect = HTTPError("503")
        with patch(self._TARGET, return_value=mock):
            from connectors.remotive import RemotiveConnector
            jobs = RemotiveConnector().fetch_jobs()
        assert jobs == []

    def test_network_exception_returns_empty_list(self):
        with patch(self._TARGET, side_effect=Exception("timeout")):
            from connectors.remotive import RemotiveConnector
            jobs = RemotiveConnector().fetch_jobs()
        assert jobs == []

    def test_multiple_jobs_all_returned(self):
        payload = [self._sample_job(1), self._sample_job(2), self._sample_job(3)]
        with patch(self._TARGET, return_value=self._mock_json_response(payload)):
            from connectors.remotive import RemotiveConnector
            jobs = RemotiveConnector().fetch_jobs()
        assert len(jobs) == 3
