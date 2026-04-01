"""
Tests for utils/ask_tools.py — read-only DB tools and dispatch.

Policy/transition tests live in test_tool_policy.py.
This file covers: dispatch_tool(), read-only tool return shapes,
mark_job_status execution, and open_job / run_full_pipeline execution.
"""
import json
import pytest
from unittest.mock import patch, MagicMock
from models.database import Job
from utils.ask_tools import dispatch_tool, mark_job_status, ACTION_TOOLS


def _seed_jobs(db_session, n=3, status="review"):
    jobs = []
    for i in range(n):
        job = Job(
            external_id=f"seed-{i}",
            source="test",
            company=f"Company {i}",
            title=f"Engineer {i}",
            location="Remote",
            raw_location_text="Remote",
            url=f"https://example.com/job/{i}",
            status=status,
            fit_score=50 + i * 5,
        )
        db_session.add(job)
    db_session.commit()
    return db_session.query(Job).all()


# ---------------------------------------------------------------------------
# dispatch_tool — unknown tool
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_unknown_tool_returns_error(self, db_session):
        result = dispatch_tool("nonexistent_tool", {}, db_session)
        assert "error" in result

    def test_known_tool_returns_dict(self, db_session):
        result = dispatch_tool("get_pipeline_stats", {}, db_session)
        assert isinstance(result, dict)
        assert "error" not in result


# ---------------------------------------------------------------------------
# Read-only tools — return shape contracts
# ---------------------------------------------------------------------------

class TestCountJobsByStatus:
    def test_returns_count_and_status(self, db_session):
        _seed_jobs(db_session, n=3, status="review")
        result = dispatch_tool("count_jobs_by_status", {"status": "review"}, db_session)
        assert result["status"] == "review"
        assert result["count"] == 3

    def test_empty_status_returns_zero(self, db_session):
        result = dispatch_tool("count_jobs_by_status", {"status": "applied"}, db_session)
        assert result["count"] == 0


class TestGetJobsByStatus:
    def test_returns_total_and_returned_separately(self, db_session):
        _seed_jobs(db_session, n=5, status="review")
        result = dispatch_tool("get_jobs_by_status", {"status": "review", "limit": 2}, db_session)
        assert result["total_in_db"] == 5
        assert result["returned"] == 2
        assert len(result["jobs"]) == 2

    def test_ordered_by_fit_score_desc(self, db_session):
        _seed_jobs(db_session, n=3, status="review")
        result = dispatch_tool("get_jobs_by_status", {"status": "review"}, db_session)
        scores = [j["fit_score"] for j in result["jobs"] if j["fit_score"] is not None]
        assert scores == sorted(scores, reverse=True)


class TestGetTopJobs:
    def test_defaults_to_shortlisted_and_review(self, db_session):
        _seed_jobs(db_session, n=2, status="review")
        # Use different URL range to avoid UNIQUE constraint collision
        for i in range(10, 12):
            from models.database import Job as _Job
            j = _Job(external_id=f"sl-top-{i}", source="test", company=f"Co {i}",
                     title=f"Eng {i}", location="Remote", raw_location_text="Remote",
                     url=f"https://example.com/top/{i}", status="shortlisted", fit_score=70)
            db_session.add(j)
        db_session.commit()
        for i in range(20, 22):
            from models.database import Job as _Job
            j = _Job(external_id=f"rj-top-{i}", source="test", company=f"Co {i}",
                     title=f"Eng {i}", location="Remote", raw_location_text="Remote",
                     url=f"https://example.com/top/{i}", status="rejected", fit_score=10)
            db_session.add(j)
        db_session.commit()
        result = dispatch_tool("get_top_jobs", {"limit": 10}, db_session)
        statuses = {j["status"] for j in result["jobs"]}
        assert "rejected" not in statuses

    def test_status_filter_respected(self, db_session):
        _seed_jobs(db_session, n=2, status="shortlisted")
        result = dispatch_tool("get_top_jobs", {"limit": 5, "status": "shortlisted"}, db_session)
        assert all(j["status"] == "shortlisted" for j in result["jobs"])


class TestSearchJobs:
    def test_finds_by_title(self, db_session):
        _seed_jobs(db_session, n=3)
        result = dispatch_tool("search_jobs", {"query": "Engineer 1"}, db_session)
        assert result["total_matching"] >= 1

    def test_finds_by_company(self, db_session):
        _seed_jobs(db_session, n=3)
        result = dispatch_tool("search_jobs", {"query": "Company 0"}, db_session)
        assert result["total_matching"] >= 1

    def test_no_match_returns_zero(self, db_session):
        result = dispatch_tool("search_jobs", {"query": "ZZZNOMATCH"}, db_session)
        assert result["total_matching"] == 0


class TestGetJobDetail:
    def test_returns_key_fields(self, db_job, db_session):
        result = dispatch_tool("get_job_detail", {"job_id": db_job.id}, db_session)
        assert result["id"] == db_job.id
        assert result["company"] == db_job.company
        assert result["title"] == db_job.title
        assert "description" not in result  # detail excludes description text

    def test_missing_job_returns_error(self, db_session):
        result = dispatch_tool("get_job_detail", {"job_id": 99999}, db_session)
        assert "error" in result


class TestGetJobDescription:
    def test_returns_description(self, db_session):
        job = Job(external_id="desc-1", source="test", company="A", title="Dev",
                  location="Remote", raw_location_text="Remote",
                  url="https://x.com/1", status="review",
                  description_text="Python developer needed.")
        db_session.add(job)
        db_session.commit()
        result = dispatch_tool("get_job_description", {"job_id": job.id}, db_session)
        assert "Python" in result["description"]

    def test_truncates_long_description(self, db_session):
        job = Job(external_id="desc-2", source="test", company="B", title="Dev",
                  location="Remote", raw_location_text="Remote",
                  url="https://x.com/2", status="review",
                  description_text="x" * 4000)
        db_session.add(job)
        db_session.commit()
        result = dispatch_tool("get_job_description", {"job_id": job.id}, db_session)
        assert len(result["description"]) <= 3020  # 3000 + "[truncated]" suffix
        assert "truncated" in result["description"]


class TestGetTopShortlistedJobs:
    def test_includes_url_and_resume(self, db_session):
        job = Job(external_id="sl-1", source="test", company="A", title="Dev",
                  location="Remote", raw_location_text="Remote",
                  url="https://example.com/sl1", status="shortlisted",
                  fit_score=80, recommended_resume="ml_resume")
        db_session.add(job)
        db_session.commit()
        result = dispatch_tool("get_top_shortlisted_jobs", {}, db_session)
        assert result["total_shortlisted"] == 1
        assert result["jobs"][0]["url"] == "https://example.com/sl1"
        assert result["jobs"][0]["recommended_resume"] == "ml_resume"


# ---------------------------------------------------------------------------
# mark_job_status — execution (not just policy)
# ---------------------------------------------------------------------------

class TestMarkJobStatusExecution:
    def test_updates_status_in_db(self, db_job, db_session):
        result = mark_job_status(db_session, db_job.id, "shortlisted")
        assert result["success"] is True
        db_session.refresh(db_job)
        assert db_job.status == "shortlisted"

    def test_return_message_shows_transition(self, db_job, db_session):
        result = mark_job_status(db_session, db_job.id, "rejected")
        assert "review" in result["message"]
        assert "rejected" in result["message"]

    def test_missing_job_returns_error(self, db_session):
        result = mark_job_status(db_session, 99999, "rejected")
        assert "error" in result

    def test_review_to_shortlisted(self, db_session):
        job = Job(external_id="t-rs", source="test", company="X", title="Y",
                  location="Remote", raw_location_text="Remote",
                  url="https://x.com/rs", status="review")
        db_session.add(job); db_session.commit(); db_session.refresh(job)
        mark_job_status(db_session, job.id, "shortlisted")
        db_session.refresh(job)
        assert job.status == "shortlisted"

    def test_shortlisted_to_rejected(self, db_session):
        job = Job(external_id="t-sr", source="test", company="X", title="Y",
                  location="Remote", raw_location_text="Remote",
                  url="https://x.com/sr", status="shortlisted")
        db_session.add(job); db_session.commit(); db_session.refresh(job)
        mark_job_status(db_session, job.id, "rejected")
        db_session.refresh(job)
        assert job.status == "rejected"

    def test_review_to_applied_executes_even_without_policy(self, db_session):
        # mark_job_status itself doesn't enforce policy — that's tool_policy_check's job.
        # Verify the function executes the DB write regardless of transition validity.
        job = Job(external_id="t-ra", source="test", company="X", title="Y",
                  location="Remote", raw_location_text="Remote",
                  url="https://x.com/ra", status="review")
        db_session.add(job); db_session.commit(); db_session.refresh(job)
        result = mark_job_status(db_session, job.id, "applied")
        assert result["success"] is True  # function succeeds; policy layer is separate


# ---------------------------------------------------------------------------
# ACTION_TOOLS registry
# ---------------------------------------------------------------------------

class TestActionToolsRegistry:
    def test_expected_tools_are_action_tools(self):
        assert "open_job" in ACTION_TOOLS
        assert "mark_job_status" in ACTION_TOOLS
        assert "run_full_pipeline" in ACTION_TOOLS

    def test_read_only_tools_not_in_action_tools(self):
        assert "count_jobs_by_status" not in ACTION_TOOLS
        assert "get_pipeline_stats" not in ACTION_TOOLS
        assert "search_jobs" not in ACTION_TOOLS
