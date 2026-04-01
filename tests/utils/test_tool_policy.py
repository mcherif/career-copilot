"""
Tests for the Pass 2 policy layer in utils/ask_tools.py:
  - tool_policy_check()
  - confirmation_prompt()
  - mark_job_status transition rules
"""
import pytest
from unittest.mock import patch
from models.database import Job
from utils.ask_tools import tool_policy_check, confirmation_prompt, _VALID_TRANSITIONS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(db_session, status="review", url="https://example.com/job/1", **kwargs):
    url_suffix = url[-3:] if url else "nil"
    job = Job(
        external_id=f"test-{status}-{url_suffix}",
        source="test",
        company="Acme",
        title="Engineer",
        location="Remote",
        raw_location_text="Remote",
        url=url,
        status=status,
        **kwargs,
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job


# ---------------------------------------------------------------------------
# tool_policy_check — open_job
# ---------------------------------------------------------------------------

class TestPolicyOpenJob:
    def test_valid_job_with_url(self, db_session):
        job = _make_job(db_session, status="shortlisted")
        result = tool_policy_check("open_job", {"job_id": job.id}, db_session)
        assert result == {"ok": True}

    def test_job_not_found(self, db_session):
        result = tool_policy_check("open_job", {"job_id": 99999}, db_session)
        assert "error" in result

    def test_job_without_url(self, db_session):
        job = _make_job(db_session, status="shortlisted", url=None)
        result = tool_policy_check("open_job", {"job_id": job.id}, db_session)
        assert "error" in result


# ---------------------------------------------------------------------------
# tool_policy_check — mark_job_status transitions
# ---------------------------------------------------------------------------

class TestPolicyMarkJobStatus:
    def test_review_to_shortlisted_allowed(self, db_session):
        job = _make_job(db_session, status="review")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "shortlisted"}, db_session)
        assert result == {"ok": True}

    def test_review_to_rejected_allowed(self, db_session):
        job = _make_job(db_session, status="review")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "rejected"}, db_session)
        assert result == {"ok": True}

    def test_review_to_deferred_allowed(self, db_session):
        job = _make_job(db_session, status="review")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "deferred"}, db_session)
        assert result == {"ok": True}

    def test_shortlisted_to_applied_allowed(self, db_session):
        job = _make_job(db_session, status="shortlisted")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "applied"}, db_session)
        assert result == {"ok": True}

    def test_shortlisted_to_rejected_allowed(self, db_session):
        job = _make_job(db_session, status="shortlisted")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "rejected"}, db_session)
        assert result == {"ok": True}

    def test_rejected_to_review_allowed(self, db_session):
        job = _make_job(db_session, status="rejected")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "review"}, db_session)
        assert result == {"ok": True}

    def test_applied_is_terminal(self, db_session):
        job = _make_job(db_session, status="applied")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "shortlisted"}, db_session)
        assert "error" in result
        assert "terminal" in result["error"].lower()

    def test_review_to_applied_invalid(self, db_session):
        job = _make_job(db_session, status="review")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "applied"}, db_session)
        assert "error" in result

    def test_rejected_to_applied_invalid(self, db_session):
        job = _make_job(db_session, status="rejected")
        result = tool_policy_check("mark_job_status", {"job_id": job.id, "status": "applied"}, db_session)
        assert "error" in result

    def test_job_not_found(self, db_session):
        result = tool_policy_check("mark_job_status", {"job_id": 99999, "status": "rejected"}, db_session)
        assert "error" in result


# ---------------------------------------------------------------------------
# tool_policy_check — run_full_pipeline
# ---------------------------------------------------------------------------

class TestPolicyRunPipeline:
    def test_no_active_run_allowed(self, db_session):
        result = tool_policy_check("run_full_pipeline", {}, db_session)
        assert result == {"ok": True}

    def test_active_run_blocks_pipeline(self, db_session):
        from models.database import PipelineRun
        from datetime import datetime, timezone, timedelta
        active_run = PipelineRun(
            source="full-run",
            started_at=datetime.now(timezone.utc) - timedelta(minutes=2),
            completed_at=None,
            status="running",
        )
        db_session.add(active_run)
        db_session.commit()
        result = tool_policy_check("run_full_pipeline", {}, db_session)
        assert "error" in result

    def test_old_incomplete_run_does_not_block(self, db_session):
        from models.database import PipelineRun
        from datetime import datetime, timezone, timedelta
        stale_run = PipelineRun(
            source="full-run",
            started_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            completed_at=None,
            status="running",
        )
        db_session.add(stale_run)
        db_session.commit()
        result = tool_policy_check("run_full_pipeline", {}, db_session)
        assert result == {"ok": True}


# ---------------------------------------------------------------------------
# _VALID_TRANSITIONS completeness
# ---------------------------------------------------------------------------

class TestTransitionTable:
    def test_all_statuses_present(self):
        statuses = {"new", "review", "shortlisted", "rejected", "applied", "deferred"}
        assert statuses == set(_VALID_TRANSITIONS.keys())

    def test_applied_has_no_transitions(self):
        assert _VALID_TRANSITIONS["applied"] == set()

    def test_review_can_reach_shortlisted(self):
        assert "shortlisted" in _VALID_TRANSITIONS["review"]

    def test_rejected_can_be_undone(self):
        assert "review" in _VALID_TRANSITIONS["rejected"]


# ---------------------------------------------------------------------------
# confirmation_prompt — readable messages
# ---------------------------------------------------------------------------

class TestConfirmationPrompt:
    def test_open_job_prompt(self, db_session):
        job = _make_job(db_session, status="shortlisted")
        prompt = confirmation_prompt("open_job", {"job_id": job.id}, db_session)
        assert str(job.id) in prompt
        assert "Acme" in prompt or "Engineer" in prompt

    def test_mark_status_prompt_shows_transition(self, db_session):
        job = _make_job(db_session, status="review")
        prompt = confirmation_prompt("mark_job_status", {"job_id": job.id, "status": "rejected"}, db_session)
        assert "rejected" in prompt
        assert "review" in prompt

    def test_run_pipeline_prompt_warns_duration(self, db_session):
        prompt = confirmation_prompt("run_full_pipeline", {}, db_session)
        assert "pipeline" in prompt.lower()
        assert "minutes" in prompt.lower()

    def test_unknown_tool_returns_fallback(self, db_session):
        prompt = confirmation_prompt("unknown_tool", {}, db_session)
        assert "unknown_tool" in prompt
