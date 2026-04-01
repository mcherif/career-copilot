"""
Mocked fetch + normalize tests for the remaining simple connectors:
Himalayas, Arbeitnow, WorkingNomads, RemoteOK, Jobicy, RemoteAIJobs,
ArcDev, DynamiteJobs, DailyRemote, Jobspresso, Adzuna.

No live network calls — requests.get is patched throughout.
"""
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _mock_json(payload, status=200):
    m = MagicMock()
    m.json.return_value = payload
    m.raise_for_status = MagicMock()
    if status >= 400:
        from requests.exceptions import HTTPError
        m.raise_for_status.side_effect = HTTPError(str(status))
    return m


def _mock_xml(content: bytes, status=200):
    m = MagicMock()
    m.content = content
    m.raise_for_status = MagicMock()
    if status >= 400:
        from requests.exceptions import HTTPError
        m.raise_for_status.side_effect = HTTPError(str(status))
    return m


def _rss(items_xml=""):
    return f"""<?xml version="1.0"?><rss version="2.0"><channel><title>T</title>{items_xml}</channel></rss>""".encode()


def _item_et(title="Acme: Backend Engineer",
             guid="https://example.com/job/slug",
             pub="Mon, 24 Mar 2026 00:00:00 +0000",
             description="desc"):
    """Build stdlib ET item element (link.tail = URL pattern)."""
    item = ET.Element("item")
    t = ET.SubElement(item, "title"); t.text = title
    link = ET.SubElement(item, "link"); link.tail = guid
    d = ET.SubElement(item, "description"); d.text = description
    p = ET.SubElement(item, "pubDate"); p.text = pub
    return ET.tostring(item, encoding="unicode")


# ---------------------------------------------------------------------------
# Himalayas
# ---------------------------------------------------------------------------

class TestHimalayasFetch:
    _T = "connectors.himalayas.requests.get"

    def _job(self, guid="hm-1", pub=None):
        return {
            "guid": guid,
            "applicationLink": f"https://himalayas.app/apply/{guid}",
            "title": "ML Engineer",
            "companyName": "DeepCo",
            "description": "ML role",
            "excerpt": "short",
            "pubDate": pub or str(int(datetime.now(tz=timezone.utc).timestamp())),
        }

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_json({"jobs": [self._job()], "totalCount": 1})):
            from connectors.himalayas import HimalayasConnector
            jobs = HimalayasConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_filters_old_jobs(self):
        old_ts = str(int((datetime.now(tz=timezone.utc) - timedelta(days=30)).timestamp()))
        with patch(self._T, return_value=_mock_json({"jobs": [self._job(pub=old_ts)], "totalCount": 1})):
            from connectors.himalayas import HimalayasConnector
            jobs = HimalayasConnector().fetch_jobs()
        assert jobs == []

    def test_empty_jobs_stops_pagination(self):
        with patch(self._T, return_value=_mock_json({"jobs": [], "totalCount": 0})):
            from connectors.himalayas import HimalayasConnector
            jobs = HimalayasConnector().fetch_jobs()
        assert jobs == []

    def test_deduplicates_by_guid(self):
        job = self._job(guid="same-guid")
        # Two pages with the same job
        responses = [
            _mock_json({"jobs": [job], "totalCount": 40}),
            _mock_json({"jobs": [job], "totalCount": 40}),
        ]
        with patch(self._T, side_effect=responses):
            from connectors.himalayas import HimalayasConnector
            jobs = HimalayasConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json({}, 503)):
            from connectors.himalayas import HimalayasConnector
            assert HimalayasConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("timeout")):
            from connectors.himalayas import HimalayasConnector
            assert HimalayasConnector().fetch_jobs() == []


class TestHimalayasNormalize:
    def _n(self, raw):
        from connectors.himalayas import HimalayasConnector
        return HimalayasConnector().normalize(raw)

    def _raw(self, **kw):
        base = {"guid": "hm-1", "applicationLink": "https://himalayas.app/apply/1",
                "title": "Dev", "companyName": "Co", "description": "role",
                "excerpt": "short", "pubDate": "1711929600"}
        return {**base, **kw}

    def test_date_from_timestamp(self):
        result = self._n(self._raw())
        assert isinstance(result["posted_date"], datetime)

    def test_bad_timestamp_gives_none(self):
        result = self._n(self._raw(pubDate="not-a-number"))
        assert result["posted_date"] is None

    def test_prefers_description_over_excerpt(self):
        result = self._n(self._raw(description="full desc", excerpt="short"))
        assert result["description"] == "full desc"

    def test_falls_back_to_excerpt(self):
        result = self._n(self._raw(description="", excerpt="excerpt only"))
        assert result["description"] == "excerpt only"

    def test_source_name(self):
        assert self._n(self._raw())["source"] == "himalayas"


# ---------------------------------------------------------------------------
# Arbeitnow
# ---------------------------------------------------------------------------

class TestArbeitnowFetch:
    _T = "connectors.arbeitnow.requests.get"

    def _job(self, remote=True, slug="eng-acme"):
        return {"slug": slug, "title": "Engineer", "company_name": "Acme",
                "remote": remote, "url": "https://arbeitnow.com/jobs/acme",
                "description": "role", "location": "Remote", "created_at": "2026-03-24"}

    def test_returns_remote_jobs_only(self):
        data = {"data": [self._job(remote=True), self._job(remote=False, slug="non-remote")],
                "links": {}}
        with patch(self._T, return_value=_mock_json(data)):
            from connectors.arbeitnow import ArbeitnowConnector
            jobs = ArbeitnowConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_empty_page_stops_loop(self):
        with patch(self._T, return_value=_mock_json({"data": [], "links": {}})):
            from connectors.arbeitnow import ArbeitnowConnector
            jobs = ArbeitnowConnector().fetch_jobs()
        assert jobs == []

    def test_paginates_up_to_3_pages(self):
        page_data = {"data": [self._job()], "links": {"next": "page2"}}
        with patch(self._T, return_value=_mock_json(page_data)):
            from connectors.arbeitnow import ArbeitnowConnector
            jobs = ArbeitnowConnector().fetch_jobs()
        # Should stop at page 3 regardless of "next" link
        assert len(jobs) <= 3

    def test_stops_when_no_next_link(self):
        data = {"data": [self._job()], "links": {}}
        with patch(self._T, return_value=_mock_json(data)):
            from connectors.arbeitnow import ArbeitnowConnector
            jobs = ArbeitnowConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json({}, 503)):
            from connectors.arbeitnow import ArbeitnowConnector
            assert ArbeitnowConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.arbeitnow import ArbeitnowConnector
            assert ArbeitnowConnector().fetch_jobs() == []


class TestArbeitnowNormalize:
    def _n(self, raw):
        from connectors.arbeitnow import ArbeitnowConnector
        return ArbeitnowConnector().normalize(raw)

    def test_date_parsed(self):
        r = self._n({"slug": "s", "title": "Dev", "company_name": "Co",
                     "url": "https://x.com", "description": "", "location": "Remote",
                     "created_at": "2026-03-24"})
        assert isinstance(r["posted_date"], datetime)

    def test_missing_location_defaults_to_remote(self):
        r = self._n({"slug": "s", "title": "Dev", "company_name": "Co",
                     "url": "https://x.com", "description": "", "location": ""})
        assert r["location"] == "Remote"

    def test_source_name(self):
        r = self._n({"slug": "s", "title": "", "company_name": "", "url": "", "description": ""})
        assert r["source"] == "arbeitnow"


# ---------------------------------------------------------------------------
# WorkingNomads
# ---------------------------------------------------------------------------

class TestWorkingNomadsFetch:
    _T = "connectors.workingnomads.requests.get"

    def _job(self, title="Dev"):
        return {"title": title, "company_name": "Acme",
                "url": "https://workingnomads.com/jobs/1",
                "description": "role", "pub_date": "2026-03-24"}

    def test_returns_list_of_jobs(self):
        with patch(self._T, return_value=_mock_json([self._job(), self._job("ML Eng")])):
            from connectors.workingnomads import WorkingNomadsConnector
            jobs = WorkingNomadsConnector().fetch_jobs()
        assert len(jobs) == 2

    def test_non_list_response_returns_empty(self):
        with patch(self._T, return_value=_mock_json({"error": "bad"})):
            from connectors.workingnomads import WorkingNomadsConnector
            jobs = WorkingNomadsConnector().fetch_jobs()
        assert jobs == []

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json({}, 503)):
            from connectors.workingnomads import WorkingNomadsConnector
            assert WorkingNomadsConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.workingnomads import WorkingNomadsConnector
            assert WorkingNomadsConnector().fetch_jobs() == []


class TestWorkingNomadsNormalize:
    def _n(self, raw):
        from connectors.workingnomads import WorkingNomadsConnector
        return WorkingNomadsConnector().normalize(raw)

    def test_external_id_from_url_slug(self):
        r = self._n({"title": "Dev", "company_name": "Co",
                     "url": "https://workingnomads.com/jobs/backend-dev-acme",
                     "description": "", "pub_date": None})
        assert r["external_id"] == "backend-dev-acme"

    def test_external_id_falls_back_to_title(self):
        r = self._n({"title": "Backend Dev", "company_name": "Co",
                     "url": "", "description": "", "pub_date": None})
        assert "Backend Dev" in r["external_id"]

    def test_empty_location_defaults_to_remote(self):
        r = self._n({"title": "D", "company_name": "C", "url": "",
                     "description": "", "pub_date": None, "location": ""})
        assert r["location"] == "Remote"

    def test_date_parsed(self):
        r = self._n({"title": "D", "company_name": "C",
                     "url": "https://x.com/jobs/slug", "description": "",
                     "pub_date": "2026-03-24"})
        assert isinstance(r["posted_date"], datetime)

    def test_source_name(self):
        assert self._n({"title": "", "company_name": "", "url": "",
                        "description": "", "pub_date": None})["source"] == "workingnomads"


# ---------------------------------------------------------------------------
# RemoteOK
# ---------------------------------------------------------------------------

class TestRemoteOKFetch:
    _T = "connectors.remoteok.requests.get"

    def _job(self, job_id="1", has_ats=True):
        desc = '<a href="https://greenhouse.io/apply/1">Apply</a>' if has_ats else "No ATS link"
        return {"id": job_id, "position": "Backend Engineer",
                "company": "Acme", "description": desc, "url": "https://remoteok.com/1",
                "date": "2026-03-24T00:00:00Z", "location": "Worldwide"}

    def test_filters_jobs_without_ats_url(self):
        data = [self._job("1", has_ats=True), self._job("2", has_ats=False)]
        with patch(self._T, return_value=_mock_json(data)):
            from connectors.remoteok import RemoteOKConnector
            jobs = RemoteOKConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_skips_non_dict_items(self):
        data = [{"legal": "notice"}, self._job()]  # first item has no "position"
        with patch(self._T, return_value=_mock_json(data)):
            from connectors.remoteok import RemoteOKConnector
            jobs = RemoteOKConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json([], 403)):
            from connectors.remoteok import RemoteOKConnector
            assert RemoteOKConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.remoteok import RemoteOKConnector
            assert RemoteOKConnector().fetch_jobs() == []


class TestRemoteOKNormalize:
    def _n(self, raw):
        from connectors.remoteok import RemoteOKConnector
        return RemoteOKConnector().normalize(raw)

    def test_ats_url_extracted_from_description(self):
        raw = {"id": "1", "position": "Dev", "company": "Acme", "location": "WW",
               "description": '<a href="https://lever.co/acme/1">Apply</a>',
               "date": "2026-03-24T00:00:00Z"}
        assert "lever.co" in self._n(raw)["url"]

    def test_falls_back_to_remoteok_url(self):
        raw = {"id": "1", "position": "Dev", "company": "Acme", "location": "WW",
               "description": "No ATS", "url": "https://remoteok.com/1",
               "date": "2026-03-24T00:00:00Z"}
        assert self._n(raw)["url"] == "https://remoteok.com/1"

    def test_date_parsed(self):
        raw = {"id": "1", "position": "Dev", "company": "Acme", "location": "WW",
               "description": "role", "date": "2026-03-24T00:00:00Z"}
        assert isinstance(self._n(raw)["posted_date"], datetime)

    def test_title_from_position_field(self):
        raw = {"id": "1", "position": "ML Engineer", "company": "Co",
               "location": "WW", "description": ""}
        assert self._n(raw)["title"] == "ML Engineer"

    def test_source_name(self):
        assert self._n({"id": "1", "position": "", "company": "",
                        "location": "", "description": ""})["source"] == "remoteok"


# ---------------------------------------------------------------------------
# Jobicy
# ---------------------------------------------------------------------------

class TestJobicyFetch:
    _T = "connectors.jobicy.requests.get"

    def _job(self, job_id=1):
        return {"id": job_id, "jobTitle": "Engineer", "companyName": "Acme",
                "url": "https://jobicy.com/jobs/1", "jobDescription": "role",
                "jobGeo": "Worldwide", "pubDate": "Mon, 24 Mar 2026 00:00:00 +0000"}

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_json({"jobs": [self._job()]})):
            from connectors.jobicy import JobicyConnector
            jobs = JobicyConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_empty_jobs_list(self):
        with patch(self._T, return_value=_mock_json({"jobs": []})):
            from connectors.jobicy import JobicyConnector
            assert JobicyConnector().fetch_jobs() == []

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json({}, 503)):
            from connectors.jobicy import JobicyConnector
            assert JobicyConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.jobicy import JobicyConnector
            assert JobicyConnector().fetch_jobs() == []


class TestJobicyNormalize:
    def _n(self, raw):
        from connectors.jobicy import JobicyConnector
        return JobicyConnector().normalize(raw)

    def _raw(self, **kw):
        base = {"id": 1, "jobTitle": "Dev", "companyName": "Co",
                "url": "https://jobicy.com/1", "jobDescription": "role",
                "jobGeo": "Worldwide", "pubDate": "Mon, 24 Mar 2026 00:00:00 +0000"}
        return {**base, **kw}

    def test_date_parsed(self):
        assert isinstance(self._n(self._raw())["posted_date"], datetime)

    def test_bad_date_gives_none(self):
        assert self._n(self._raw(pubDate="bad"))["posted_date"] is None

    def test_falls_back_to_excerpt(self):
        r = self._n(self._raw(jobDescription="", jobExcerpt="excerpt"))
        assert r["description"] == "excerpt"

    def test_geo_fallback(self):
        r = self._n(self._raw(jobGeo=""))
        assert r["location"] == "Worldwide"

    def test_id_coerced_to_string(self):
        assert self._n(self._raw(id=42))["external_id"] == "42"

    def test_source_name(self):
        assert self._n(self._raw())["source"] == "jobicy"


# ---------------------------------------------------------------------------
# RemoteAIJobs (same structure as RWFA but lxml)
# ---------------------------------------------------------------------------

class TestRemoteAIJobsFetch:
    _T = "connectors.remoteaijobs.requests.get"

    def _xml_item(self, title="ML Engineer at DeepCo",
                  guid="https://www.realworkfromanywhere.com/remote-ai-jobs/ml-eng",
                  pub="Mon, 24 Mar 2026 00:00:00 +0000"):
        return f"""<item>
          <title>{title}</title>
          <guid>{guid}</guid>
          <description>AI role</description>
          <pubDate>{pub}</pubDate>
        </item>"""

    def _feed(self, *items):
        body = "".join(items)
        return f"""<?xml version="1.0"?><rss version="2.0"><channel>{body}</channel></rss>""".encode()

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._xml_item()))):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            jobs = RemoteAIJobsConnector().fetch_jobs()
        assert len(jobs) == 1
        assert jobs[0]["title"] == "ML Engineer"
        assert jobs[0]["company"] == "DeepCo"

    def test_filters_old_jobs(self):
        old = "Mon, 01 Jan 2024 00:00:00 +0000"
        with patch(self._T, return_value=_mock_xml(self._feed(self._xml_item(pub=old)))):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            assert RemoteAIJobsConnector().fetch_jobs() == []

    def test_no_channel_returns_empty(self):
        xml = b'<?xml version="1.0"?><rss version="2.0"></rss>'
        with patch(self._T, return_value=_mock_xml(xml)):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            assert RemoteAIJobsConnector().fetch_jobs() == []

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_xml(b"", 503)):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            assert RemoteAIJobsConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            assert RemoteAIJobsConnector().fetch_jobs() == []

    def test_title_without_at_company_is_unknown(self):
        item = self._xml_item(title="ML Engineer")
        with patch(self._T, return_value=_mock_xml(self._feed(item))):
            from connectors.remoteaijobs import RemoteAIJobsConnector
            jobs = RemoteAIJobsConnector().fetch_jobs()
        assert jobs[0]["company"] == "Unknown"

    def test_remote_eligibility_is_accept(self):
        raw = {"id": "slug", "title": "Dev", "company": "Co",
               "url": "https://x.com", "description": "", "posted_date": None}
        from connectors.remoteaijobs import RemoteAIJobsConnector
        assert RemoteAIJobsConnector().normalize(raw)["remote_eligibility"] == "accept"


# ---------------------------------------------------------------------------
# ArcDev
# ---------------------------------------------------------------------------

class TestArcDevFetch:
    _T = "connectors.arcdev.requests.get"

    def _job(self, job_id="arc-1"):
        return {"id": job_id, "title": "Backend Engineer",
                "company": {"name": "Acme"}, "url": "https://arc.dev/jobs/1",
                "description": "role", "published_at": "2026-03-24T00:00:00Z",
                "location": "Worldwide"}

    def test_returns_jobs_dict_format(self):
        with patch(self._T, return_value=_mock_json({"jobs": [self._job()]})):
            from connectors.arcdev import ArcDevConnector
            jobs = ArcDevConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_json({}, 503)):
            from connectors.arcdev import ArcDevConnector
            assert ArcDevConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.arcdev import ArcDevConnector
            assert ArcDevConnector().fetch_jobs() == []


class TestArcDevNormalize:
    def _n(self, raw):
        from connectors.arcdev import ArcDevConnector
        return ArcDevConnector().normalize(raw)

    def test_company_from_dict(self):
        r = self._n({"id": "1", "title": "Dev", "company": {"name": "Acme"},
                     "description": "", "published_at": "2026-03-24T00:00:00Z"})
        assert r["company"] == "Acme"

    def test_company_from_string_field(self):
        r = self._n({"id": "1", "title": "Dev", "company_name": "StrCo",
                     "description": "", "published_at": "2026-03-24T00:00:00Z"})
        assert r["company"] == "StrCo"

    def test_url_from_multiple_fields(self):
        r = self._n({"id": "1", "title": "Dev", "company": "Co",
                     "job_url": "https://arc.dev/1", "description": ""})
        assert r["url"] == "https://arc.dev/1"

    def test_date_from_published_at(self):
        r = self._n({"id": "1", "title": "Dev", "company": "Co",
                     "description": "", "published_at": "2026-03-24T00:00:00Z"})
        assert isinstance(r["posted_date"], datetime)

    def test_date_from_created_at_fallback(self):
        r = self._n({"id": "1", "title": "Dev", "company": "Co",
                     "description": "", "created_at": "2026-03-24T00:00:00Z"})
        assert isinstance(r["posted_date"], datetime)

    def test_source_name(self):
        assert self._n({"id": "1", "title": "", "company": "",
                        "description": ""})["source"] == "arcdev"


# ---------------------------------------------------------------------------
# DynamiteJobs (stdlib ET RSS)
# ---------------------------------------------------------------------------

class TestDynamiteJobsFetch:
    _T = "connectors.dynamitejobs.requests.get"

    def _item(self, title="Backend Dev", guid="https://dynamitejobs.com/1",
               pub="Mon, 24 Mar 2026 00:00:00 +0000", author=None):
        author_tag = f"<author>{author}</author>" if author else ""
        return f"""<item>
          <title>{title}</title>
          <guid>{guid}</guid>
          <description>role</description>
          <pubDate>{pub}</pubDate>
          {author_tag}
        </item>"""

    def _feed(self, *items):
        return f"""<?xml version="1.0"?><rss version="2.0"><channel>{"".join(items)}</channel></rss>""".encode()

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item()))):
            from connectors.dynamitejobs import DynamiteJobsConnector
            jobs = DynamiteJobsConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_company_from_author_tag(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item(author="StartupCo")))):
            from connectors.dynamitejobs import DynamiteJobsConnector
            jobs = DynamiteJobsConnector().fetch_jobs()
        assert jobs[0]["company"] == "StartupCo"

    def test_company_from_em_dash_in_title(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item(title="Backend Dev \u2013 Acme")))):
            from connectors.dynamitejobs import DynamiteJobsConnector
            jobs = DynamiteJobsConnector().fetch_jobs()
        assert jobs[0]["company"] == "Acme"
        assert jobs[0]["title"] == "Backend Dev"

    def test_company_from_dash_in_title(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item(title="Backend Dev - GlobalCo")))):
            from connectors.dynamitejobs import DynamiteJobsConnector
            jobs = DynamiteJobsConnector().fetch_jobs()
        assert jobs[0]["company"] == "GlobalCo"

    def test_no_channel_returns_empty(self):
        with patch(self._T, return_value=_mock_xml(b"<?xml version='1.0'?><rss/>")):
            from connectors.dynamitejobs import DynamiteJobsConnector
            assert DynamiteJobsConnector().fetch_jobs() == []

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_xml(b"", 503)):
            from connectors.dynamitejobs import DynamiteJobsConnector
            assert DynamiteJobsConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.dynamitejobs import DynamiteJobsConnector
            assert DynamiteJobsConnector().fetch_jobs() == []


# ---------------------------------------------------------------------------
# DailyRemote (stdlib ET RSS, multi-feed with dedup)
# ---------------------------------------------------------------------------

class TestDailyRemoteFetch:
    _T = "connectors.dailyremote.requests.get"

    def _item(self, title="Backend Dev at Acme", guid="https://dailyremote.com/1"):
        return f"""<item>
          <title>{title}</title>
          <guid>{guid}</guid>
          <description>role</description>
          <pubDate>Mon, 24 Mar 2026 00:00:00 +0000</pubDate>
        </item>"""

    def _feed(self, *items):
        return f"""<?xml version="1.0"?><rss version="2.0"><channel>{"".join(items)}</channel></rss>""".encode()

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item()))):
            from connectors.dailyremote import DailyRemoteConnector
            jobs = DailyRemoteConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_splits_company_from_title(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item(title="Backend Dev at Acme")))):
            from connectors.dailyremote import DailyRemoteConnector
            jobs = DailyRemoteConnector().fetch_jobs()
        assert jobs[0]["company"] == "Acme"
        assert jobs[0]["title"] == "Backend Dev"

    def test_deduplicates_across_feeds(self):
        xml = self._feed(self._item(guid="https://dailyremote.com/same"))
        # Same content twice (simulates duplicate across feeds)
        with patch(self._T, side_effect=[_mock_xml(xml), _mock_xml(xml)]):
            from connectors.dailyremote import DailyRemoteConnector
            jobs = DailyRemoteConnector().fetch_jobs()
        ids = [j["id"] for j in jobs]
        assert len(ids) == len(set(ids))

    def test_no_channel_continues(self):
        with patch(self._T, return_value=_mock_xml(b"<?xml version='1.0'?><rss/>")):
            from connectors.dailyremote import DailyRemoteConnector
            assert DailyRemoteConnector().fetch_jobs() == []

    def test_exception_continues_to_next_feed(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.dailyremote import DailyRemoteConnector
            assert DailyRemoteConnector().fetch_jobs() == []


# ---------------------------------------------------------------------------
# Jobspresso (stdlib ET RSS, "at" title parsing, WordPress ID)
# ---------------------------------------------------------------------------

class TestJobspressoFetch:
    _T = "connectors.jobspresso.requests.get"

    def _item(self, title="Full Stack Dev at StartupCo",
               guid="https://jobspresso.co/?p=42",
               pub="Mon, 24 Mar 2026 00:00:00 +0000"):
        return f"""<item>
          <title>{title}</title>
          <guid>{guid}</guid>
          <description>role</description>
          <pubDate>{pub}</pubDate>
        </item>"""

    def _feed(self, *items):
        return f"""<?xml version="1.0"?><rss version="2.0"><channel>{"".join(items)}</channel></rss>""".encode()

    def test_returns_jobs(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item()))):
            from connectors.jobspresso import JobspressoConnector
            jobs = JobspressoConnector().fetch_jobs()
        assert len(jobs) == 1

    def test_company_split_from_title(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item()))):
            from connectors.jobspresso import JobspressoConnector
            jobs = JobspressoConnector().fetch_jobs()
        assert jobs[0]["company"] == "StartupCo"
        assert jobs[0]["title"] == "Full Stack Dev"

    def test_wordpress_post_id_as_external_id(self):
        with patch(self._T, return_value=_mock_xml(self._feed(self._item()))):
            from connectors.jobspresso import JobspressoConnector
            jobs = JobspressoConnector().fetch_jobs()
        assert jobs[0]["id"] == "42"

    def test_no_channel_returns_empty(self):
        with patch(self._T, return_value=_mock_xml(b"<?xml version='1.0'?><rss/>")):
            from connectors.jobspresso import JobspressoConnector
            assert JobspressoConnector().fetch_jobs() == []

    def test_http_error_returns_empty(self):
        with patch(self._T, return_value=_mock_xml(b"", 503)):
            from connectors.jobspresso import JobspressoConnector
            assert JobspressoConnector().fetch_jobs() == []

    def test_exception_returns_empty(self):
        with patch(self._T, side_effect=Exception("err")):
            from connectors.jobspresso import JobspressoConnector
            assert JobspressoConnector().fetch_jobs() == []


# ---------------------------------------------------------------------------
# Adzuna (_is_remote helper + normalize, fetch skips with no API key)
# ---------------------------------------------------------------------------

class TestAdzunaIsRemote:
    def _call(self, **kw):
        from connectors.adzuna import _is_remote
        return _is_remote(kw)

    def test_remote_in_location(self):
        assert self._call(location={"display_name": "Remote UK"}, title="", description="")

    def test_remote_in_title(self):
        assert self._call(location={}, title="Remote Backend Engineer", description="")

    def test_remote_in_description(self):
        assert self._call(location={}, title="", description="This is a remote position")

    def test_not_remote_if_no_remote(self):
        assert not self._call(location={"display_name": "London"}, title="Engineer", description="Office role")

    def test_no_remote_negation(self):
        assert not self._call(location={}, title="", description="not remote, not remote")


class TestAdzunaFetchNoKey:
    def test_returns_empty_when_no_api_key(self):
        import os
        with patch.dict(os.environ, {"ADZUNA_APP_ID": "", "ADZUNA_APP_KEY": ""}):
            from connectors.adzuna import AdzunaConnector
            jobs = AdzunaConnector().fetch_jobs()
        assert jobs == []


class TestAdzunaNormalize:
    def _n(self, raw):
        from connectors.adzuna import AdzunaConnector
        return AdzunaConnector().normalize(raw)

    def _raw(self, **kw):
        base = {"id": "12345", "title": "Engineer",
                "company": {"display_name": "Acme"},
                "location": {"display_name": "London"},
                "redirect_url": "https://adzuna.com/jobs/12345",
                "description": "Python role",
                "created": "2026-03-24T00:00:00Z",
                "_country": "gb"}
        return {**base, **kw}

    def test_company_from_display_name(self):
        assert self._n(self._raw())["company"] == "Acme"

    def test_company_fallback_on_non_dict(self):
        r = self._n(self._raw(company="Not a dict"))
        assert r["company"] == "Unknown"

    def test_date_parsed(self):
        assert isinstance(self._n(self._raw())["posted_date"], datetime)

    def test_bad_date_gives_none(self):
        assert self._n(self._raw(created="bad"))["posted_date"] is None

    def test_external_id_includes_country(self):
        r = self._n(self._raw())
        assert "gb" in r["external_id"]
        assert "12345" in r["external_id"]

    def test_source_name(self):
        assert self._n(self._raw())["source"] == "adzuna"

    def test_description_text_cleaned(self):
        r = self._n(self._raw(description="<p>Python</p>"))
        assert "<p>" not in r["description_text"]
        assert "Python" in r["description_text"]
