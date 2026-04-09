"""Tests for the /api/jobs/{id}/open and /api/prefill/status endpoints."""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Force module load before any patching.
import ui.app as _app_module  # noqa: E402
from ui.app import app as _fastapi_app


# ---------------------------------------------------------------------------
# App fixture — isolated from real DB and scheduler
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    """Return a TestClient with DB and scheduler mocked out."""
    with patch.object(_app_module, "_Session") as mock_session_cls, \
         patch.object(_app_module, "_scheduler") as mock_sched, \
         patch.object(_app_module, "_load_sched_config"), \
         patch.object(_app_module, "_apply_schedule"):

        mock_sched.running = False
        _app_module._prefill = {"status": "idle", "job_id": None, "result": None}

        yield TestClient(_fastapi_app, raise_server_exceptions=False), mock_session_cls


def _mock_job(job_id=1, url="https://jobs.ashbyhq.com/acme/123", status="review"):
    job = MagicMock()
    job.id = job_id
    job.title = "Software Engineer"
    job.company = "Acme"
    job.url = url
    job.status = status
    job.location = "Remote"
    job.raw_location_text = "Remote (EU)"
    job.source = "ashby"
    job.fit_score = 75
    job.llm_fit_score = 80
    job.llm_confidence = 85
    job.recommendation = "Recommend"
    job.llm_strengths = '["Python", "FastAPI"]'
    job.skill_gaps = '[]'
    job.fit_explanation = "Good match"
    job.cover_letter = None
    job.description_text = "A great job"
    job.description = None
    job.posted_date = None
    job.created_at = None
    return job


# ---------------------------------------------------------------------------
# POST /api/jobs/{id}/open
# ---------------------------------------------------------------------------

def test_open_job_not_found(client):
    tc, mock_session_cls = client
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None
    mock_session_cls.return_value = session

    r = tc.post("/api/jobs/999/open")
    assert r.status_code == 404


def test_open_job_system_browser_domain(client):
    tc, mock_session_cls = client
    job = _mock_job(url="https://remoteok.com/jobs/123")
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = job
    mock_session_cls.return_value = session

    r = tc.post("/api/jobs/1/open")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["system_browser"] is True
    assert "remoteok.com" in data["url"]


def test_open_job_starts_prefill_thread(client):
    tc, mock_session_cls = client
    job = _mock_job()
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = job
    mock_session_cls.return_value = session

    thread_started = []

    def fake_thread(*args, **kwargs):
        t = MagicMock()
        t.start = MagicMock(side_effect=lambda: thread_started.append(True))
        return t

    with patch.object(_app_module.threading, "Thread", side_effect=fake_thread), \
         patch("builtins.open", MagicMock()), \
         patch.object(_app_module.yaml, "safe_load", return_value={}):
        r = tc.post("/api/jobs/1/open")

    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["system_browser"] is False
    assert len(thread_started) == 1


def test_open_job_already_running(client):
    """A new open request while a session is running should cancel it and succeed."""
    tc, mock_session_cls = client
    import ui.app as app_module
    app_module._prefill["status"] = "running"

    job = _mock_job()
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = job
    mock_session_cls.return_value = session

    with patch("utils.form_prefill.is_system_browser_domain", return_value=False):
        r = tc.post("/api/jobs/1/open")

    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True  # cancel-and-replace, not reject


def test_open_job_profile_load_failure_uses_empty(client):
    """Missing profile.yaml should not crash — falls back to empty dict."""
    tc, mock_session_cls = client
    job = _mock_job()
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = job
    mock_session_cls.return_value = session

    thread_started = []

    def fake_thread(*args, **kwargs):
        t = MagicMock()
        t.start = MagicMock(side_effect=lambda: thread_started.append(True))
        return t

    with patch.object(_app_module.threading, "Thread", side_effect=fake_thread), \
         patch("builtins.open", side_effect=FileNotFoundError):
        r = tc.post("/api/jobs/1/open")

    assert r.status_code == 200
    assert r.json()["ok"] is True


# ---------------------------------------------------------------------------
# GET /api/prefill/status
# ---------------------------------------------------------------------------

def test_prefill_status_idle(client):
    tc, _ = client
    r = tc.get("/api/prefill/status")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "idle"
    assert data["job_id"] is None
    assert data["result"] is None


def test_prefill_status_running(client):
    tc, _ = client
    import ui.app as app_module
    app_module._prefill = {"status": "running", "job_id": 42, "result": None}

    r = tc.get("/api/prefill/status")
    data = r.json()
    assert data["status"] == "running"
    assert data["job_id"] == 42


def test_prefill_status_done(client):
    tc, _ = client
    import ui.app as app_module
    app_module._prefill = {
        "status": "done",
        "job_id": 7,
        "result": {"status": "ok", "filled": 3, "skipped": 1, "errors": 0, "ats": "ashby"},
    }

    r = tc.get("/api/prefill/status")
    data = r.json()
    assert data["status"] == "done"
    assert data["result"]["filled"] == 3
    assert data["result"]["ats"] == "ashby"


# ---------------------------------------------------------------------------
# _run_prefill_thread
# ---------------------------------------------------------------------------

def test_run_prefill_thread_sets_done_on_success():
    _app_module._prefill = {"status": "running", "job_id": 1, "result": None}
    fake_result = {"status": "ok", "filled": 2, "skipped": 0, "errors": 0, "ats": "ashby"}

    with patch.object(_app_module.asyncio, "run", return_value=fake_result):
        _app_module._run_prefill_thread({"url": "https://jobs.ashbyhq.com/x"}, {})

    assert _app_module._prefill["status"] == "done"
    assert _app_module._prefill["result"] == fake_result


def test_run_prefill_thread_sets_done_on_exception():
    _app_module._prefill = {"status": "running", "job_id": 1, "result": None}

    with patch.object(_app_module.asyncio, "run", side_effect=RuntimeError("crash")):
        _app_module._run_prefill_thread({"url": "https://jobs.ashbyhq.com/x"}, {})

    assert _app_module._prefill["status"] == "done"
    assert _app_module._prefill["result"]["status"] == "failed"
    assert "crash" in _app_module._prefill["result"]["error"]
