"""
Shared fixtures for Career Copilot test suite.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Job


@pytest.fixture(scope="function")
def db_session():
    """In-memory SQLite session, rolled back after each test."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture
def sample_profile():
    """Minimal candidate profile matching profile.yaml schema."""
    return {
        "personal": {"name": "Jane Doe", "email": "jane@example.com"},
        "skills": ["Python", "SQL", "Docker", "AWS"],
        "keywords": ["backend", "api", "llm", "inference"],
        "target_roles": ["software engineer", "backend developer", "ml engineer"],
        "seniority": {
            "preferred": ["senior", "staff"],
            "acceptable": ["mid", "lead"],
        },
        "preferences": {
            "remote_only": True,
            "accepted_regions": ["worldwide", "global", "emea", "europe", "canada"],
            "reject_regions": ["us only"],
            "contractor_ok": True,
        },
        "work_authorization": {
            "canada": True,
            "sponsorship_required": False,
        },
        "languages": ["english"],
        "blacklisted_companies": ["BadCorp"],
        "resumes": [
            {"name": "general_swe", "path": "resumes/jane_swe.pdf", "tags": ["backend", "api"]},
            {"name": "ml_resume", "path": "resumes/jane_ml.pdf", "tags": ["llm", "inference", "ml"]},
        ],
    }


@pytest.fixture
def sample_job():
    """Minimal job dict that passes all hard filters."""
    return {
        "title": "Senior Backend Engineer",
        "company": "Acme Corp",
        "location": "Remote",
        "raw_location_text": "Remote",
        "description": "We are looking for a senior backend engineer.",
        "description_text": "We are looking for a senior backend engineer.",
        "url": "https://example.com/jobs/123",
        "source": "remotive",
        "remote_eligibility": "accept",
    }


@pytest.fixture
def db_job(db_session):
    """A persisted Job row for tests that need a real DB record."""
    job = Job(
        external_id="test-001",
        source="remotive",
        company="Acme Corp",
        title="Senior Backend Engineer",
        location="Remote",
        raw_location_text="Remote",
        url="https://example.com/jobs/123",
        status="review",
        fit_score=55,
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    return job
