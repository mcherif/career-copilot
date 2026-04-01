"""
Tests for the `prune` CLI command and the expired status transition.

Uses an in-memory SQLite session; run_pipeline.SessionLocal is patched
so the command operates against the test DB.
"""
import datetime
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.database import Base, Job


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    yield e
    Base.metadata.drop_all(e)


@pytest.fixture
def session(engine):
    S = sessionmaker(bind=engine)
    s = S()
    yield s
    s.close()


@pytest.fixture
def runner():
    return CliRunner()


def _add_job(session, status="review", days_old=0, **kwargs):
    created = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_old)
    # SQLite stores naive datetimes; strip tz to match SQLAlchemy default behaviour
    job = Job(
        external_id=f"prune-test-{status}-{days_old}-{kwargs.get('suffix', '')}",
        source="test",
        company="Acme",
        title="Engineer",
        location="Remote",
        url=f"https://example.com/{status}-{days_old}-{kwargs.get('suffix', '')}",
        status=status,
        created_at=created.replace(tzinfo=None),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def _invoke_prune(runner, session, *args):
    """Invoke the prune CLI command with SessionLocal patched to use test session."""
    mock_session_cls = MagicMock(return_value=session)
    # Prevent session.close() from actually closing our test session
    session.close = MagicMock()

    from run_pipeline import cli
    with patch("run_pipeline.SessionLocal", mock_session_cls):
        return runner.invoke(cli, ["prune", *args])


# ---------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------

class TestPruneCommand:
    def test_expires_stale_review_job(self, runner, session):
        job = _add_job(session, status="review", days_old=20)
        result = _invoke_prune(runner, session, "--days", "14")
        assert result.exit_code == 0
        session.refresh(job)
        assert job.status == "expired"

    def test_expires_stale_shortlisted_job(self, runner, session):
        job = _add_job(session, status="shortlisted", days_old=20)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "expired"

    def test_expires_stale_new_job(self, runner, session):
        job = _add_job(session, status="new", days_old=20)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "expired"

    def test_does_not_expire_recent_job(self, runner, session):
        job = _add_job(session, status="review", days_old=5)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "review"

    def test_does_not_expire_applied_job(self, runner, session):
        job = _add_job(session, status="applied", days_old=30)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "applied"

    def test_does_not_expire_rejected_job(self, runner, session):
        job = _add_job(session, status="rejected", days_old=30)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "rejected"

    def test_does_not_expire_deferred_job(self, runner, session):
        job = _add_job(session, status="deferred", days_old=30)
        _invoke_prune(runner, session, "--days", "14")
        session.refresh(job)
        assert job.status == "deferred"

    def test_no_stale_jobs_prints_message(self, runner, session):
        _add_job(session, status="review", days_old=5)
        result = _invoke_prune(runner, session, "--days", "14")
        assert result.exit_code == 0
        assert "No stale jobs" in result.output

    def test_custom_days_threshold(self, runner, session):
        old = _add_job(session, status="review", days_old=8, suffix="old")
        recent = _add_job(session, status="review", days_old=3, suffix="recent")
        _invoke_prune(runner, session, "--days", "7")
        session.refresh(old)
        session.refresh(recent)
        assert old.status == "expired"
        assert recent.status == "review"

    def test_output_shows_count_by_status(self, runner, session):
        _add_job(session, status="review", days_old=20, suffix="a")
        _add_job(session, status="shortlisted", days_old=20, suffix="b")
        result = _invoke_prune(runner, session, "--days", "14")
        assert "review" in result.output
        assert "shortlisted" in result.output

    def test_multiple_stale_jobs_all_expired(self, runner, session):
        jobs = [_add_job(session, status="review", days_old=20, suffix=str(i)) for i in range(3)]
        _invoke_prune(runner, session, "--days", "14")
        for job in jobs:
            session.refresh(job)
            assert job.status == "expired"


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

class TestPruneDryRun:
    def test_dry_run_does_not_change_status(self, runner, session):
        job = _add_job(session, status="review", days_old=20)
        result = _invoke_prune(runner, session, "--days", "14", "--dry-run")
        assert result.exit_code == 0
        session.refresh(job)
        assert job.status == "review"

    def test_dry_run_output_says_dry_run(self, runner, session):
        _add_job(session, status="review", days_old=20)
        result = _invoke_prune(runner, session, "--days", "14", "--dry-run")
        assert "DRY RUN" in result.output

    def test_dry_run_still_reports_counts(self, runner, session):
        _add_job(session, status="review", days_old=20, suffix="a")
        _add_job(session, status="new", days_old=20, suffix="b")
        result = _invoke_prune(runner, session, "--days", "14", "--dry-run")
        assert "review" in result.output
        assert "new" in result.output


# ---------------------------------------------------------------------------
# Expired status transition
# ---------------------------------------------------------------------------

class TestExpiredTransition:
    def test_expired_can_be_rescued_to_review(self, db_session):
        from utils.ask_tools import _VALID_TRANSITIONS
        assert "review" in _VALID_TRANSITIONS["expired"]

    def test_applied_cannot_transition_to_expired(self):
        from utils.ask_tools import _VALID_TRANSITIONS
        assert "expired" not in _VALID_TRANSITIONS.get("applied", set())

    def test_expired_not_reachable_from_applied(self):
        from utils.ask_tools import _VALID_TRANSITIONS
        for status, targets in _VALID_TRANSITIONS.items():
            if status == "applied":
                assert "expired" not in targets
