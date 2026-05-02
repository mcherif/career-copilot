"""
Tests for utils/interview_prep.py.

Uses an in-memory SQLite session and mocks requests.post so no real
Ollama instance is needed.
"""
import json
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from models.database import Base, InterviewPrepSheet, Job
from utils.interview_prep import run_interview_prep


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
def profile():
    return {
        "personal": {"name": "Jane Doe", "current_title": "Backend Engineer"},
        "skills": ["Python", "SQL", "Docker"],
        "work_history": [
            {
                "company": "Acme",
                "title": "Senior Engineer",
                "from": "2020",
                "to": "present",
                "highlights": ["Built scalable APIs", "Led team of 5"],
            }
        ],
    }


@pytest.fixture
def job(session):
    j = Job(
        external_id="interview-test-001",
        source="remotive",
        company="TechCorp",
        title="Senior Backend Engineer",
        location="Remote",
        url="https://example.com/jobs/1",
        description="<p>We need a Python expert with SQL and Docker skills.</p>",
        description_text="We need a Python expert with SQL and Docker skills.",
        status="shortlisted",
    )
    session.add(j)
    session.commit()
    session.refresh(j)
    return j


def _ollama_response(content: dict) -> MagicMock:
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = {"message": {"content": json.dumps(content)}}
    return mock


CONTEXT_RESPONSE = {
    "company_snapshot": {
        "industry": "SaaS",
        "likely_size": "50-200",
        "culture_signals": ["fast-paced"],
        "red_flags": [],
    },
    "role_summary": {
        "core_responsibilities": ["Build APIs"],
        "must_have_skills": ["Python", "SQL"],
        "nice_to_have_skills": ["Docker"],
        "seniority_signals": "Senior",
    },
}

QUESTIONS_RESPONSE = {
    "technical_questions": ["Describe your Python API experience.", "How do you optimize SQL queries?"],
    "behavioral_questions": ["Tell me about a time you led a project.", "How do you handle deadlines?"],
}

MAPPING_RESPONSE = {
    "talking_points": [
        {
            "jd_requirement": "Python expertise",
            "candidate_evidence": "Built scalable APIs at Acme",
            "suggested_story": "Discuss the API work at Acme",
        }
    ],
    "gaps_or_risks": [],
}

ACTION_PLAN_RESPONSE = {
    "minutes_0_10": ["Review company website"],
    "minutes_10_20": ["Practice Python API questions"],
    "minutes_20_30": ["Rehearse STAR stories"],
    "priority_note": "Emphasize Python API experience",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_run_interview_prep_success(session, job, profile):
    responses = [
        _ollama_response(CONTEXT_RESPONSE),
        _ollama_response(QUESTIONS_RESPONSE),
        _ollama_response(MAPPING_RESPONSE),
        _ollama_response(ACTION_PLAN_RESPONSE),
    ]
    with patch("utils.interview_prep.requests.post", side_effect=responses):
        sheet = run_interview_prep(job.id, profile, session)

    assert sheet.status == "completed"
    assert sheet.job_application_id == job.id
    assert sheet.generated_at is not None

    # All 7 sections populated
    assert json.loads(sheet.company_snapshot)["industry"] == "SaaS"
    assert json.loads(sheet.role_requirements_summary)["seniority_signals"] == "Senior"
    assert len(json.loads(sheet.likely_technical_questions)) == 2
    assert len(json.loads(sheet.likely_behavioral_questions)) == 2
    assert len(json.loads(sheet.talking_points)) == 1
    assert json.loads(sheet.gaps_or_risks) == []
    assert json.loads(sheet.prep_plan_30_min)["priority_note"] == "Emphasize Python API experience"


def test_run_interview_prep_invalid_job_id(session, profile):
    with pytest.raises(ValueError, match="not found"):
        run_interview_prep(99999, profile, session)

    # No sheet should have been created
    count = session.query(InterviewPrepSheet).count()
    assert count == 0


def test_run_interview_prep_missing_description(session, profile):
    empty_job = Job(
        external_id="interview-nodesc-001",
        source="remotive",
        company="TechCorp",
        title="Engineer",
        location="Remote",
        url="https://example.com/jobs/2",
        description=None,
        description_text=None,
        status="shortlisted",
    )
    session.add(empty_job)
    session.commit()
    session.refresh(empty_job)

    with pytest.raises(ValueError, match="no description"):
        run_interview_prep(empty_job.id, profile, session)

    sheet = session.query(InterviewPrepSheet).filter(
        InterviewPrepSheet.job_application_id == empty_job.id
    ).first()
    assert sheet is not None
    assert sheet.status == "failed"
    assert "empty" in sheet.error_message.lower()


def test_run_interview_prep_timeout_retries(session, job, profile):
    with patch("utils.interview_prep.requests.post", side_effect=requests.Timeout("timed out")):
        with patch("utils.interview_prep.time.sleep"):
            with pytest.raises(RuntimeError, match="timed out"):
                run_interview_prep(job.id, profile, session)

    sheet = session.query(InterviewPrepSheet).filter(
        InterviewPrepSheet.job_application_id == job.id
    ).first()
    assert sheet is not None
    assert sheet.status == "failed"
    assert "timed out" in sheet.error_message.lower()
