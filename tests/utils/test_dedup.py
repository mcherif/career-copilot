"""
Tests for utils/dedup.py — is_duplicate() and generate_job_hash()
"""
import pytest
from models.database import Job
from utils.dedup import is_duplicate, generate_job_hash


def _add_job(db_session, url="https://example.com/1", company="Acme",
             title="Engineer", location="Remote"):
    job = Job(
        external_id=f"dedup-{url[-3:]}",
        source="test",
        company=company,
        title=title,
        location=location,
        raw_location_text=location,
        url=url,
        status="new",
    )
    db_session.add(job)
    db_session.commit()
    return job


# ---------------------------------------------------------------------------
# generate_job_hash
# ---------------------------------------------------------------------------

class TestGenerateJobHash:
    def test_same_inputs_same_hash(self):
        h1 = generate_job_hash("Acme", "Engineer", "Remote")
        h2 = generate_job_hash("Acme", "Engineer", "Remote")
        assert h1 == h2

    def test_different_inputs_different_hash(self):
        h1 = generate_job_hash("Acme", "Engineer", "Remote")
        h2 = generate_job_hash("Acme", "Developer", "Remote")
        assert h1 != h2

    def test_normalizes_whitespace(self):
        h1 = generate_job_hash("Acme Corp", "Senior Engineer", "Remote")
        h2 = generate_job_hash("Acme  Corp", "Senior  Engineer", "Remote")
        assert h1 == h2

    def test_normalizes_punctuation(self):
        h1 = generate_job_hash("Acme, Corp.", "Engineer (Senior)", "Remote")
        h2 = generate_job_hash("Acme Corp", "Engineer Senior", "Remote")
        assert h1 == h2

    def test_case_insensitive(self):
        h1 = generate_job_hash("ACME", "ENGINEER", "REMOTE")
        h2 = generate_job_hash("acme", "engineer", "remote")
        assert h1 == h2

    def test_empty_inputs_stable(self):
        h = generate_job_hash("", "", "")
        assert isinstance(h, str)
        assert len(h) == 32  # MD5 hex digest


# ---------------------------------------------------------------------------
# is_duplicate — URL match
# ---------------------------------------------------------------------------

class TestIsDuplicateByUrl:
    def test_exact_url_match_is_duplicate(self, db_session):
        _add_job(db_session, url="https://example.com/job/42")
        assert is_duplicate({"url": "https://example.com/job/42"}, db_session)

    def test_different_url_not_duplicate(self, db_session):
        _add_job(db_session, url="https://example.com/job/42")
        assert not is_duplicate({"url": "https://example.com/job/99"}, db_session)

    def test_no_url_falls_through_to_hash(self, db_session):
        _add_job(db_session, company="HashCo", title="Dev", location="Remote")
        result = is_duplicate(
            {"url": None, "company": "HashCo", "title": "Dev", "location": "Remote"},
            db_session,
        )
        assert result is True


# ---------------------------------------------------------------------------
# is_duplicate — hash match
# ---------------------------------------------------------------------------

class TestIsDuplicateByHash:
    def test_same_company_title_location_is_duplicate(self, db_session):
        _add_job(db_session, url="https://a.com/1", company="Acme", title="Engineer", location="Remote")
        result = is_duplicate(
            {"url": "https://b.com/2", "company": "Acme", "title": "Engineer", "location": "Remote"},
            db_session,
        )
        assert result is True

    def test_different_title_not_duplicate(self, db_session):
        _add_job(db_session, url="https://a.com/1", company="Acme", title="Engineer", location="Remote")
        result = is_duplicate(
            {"url": "https://b.com/2", "company": "Acme", "title": "Designer", "location": "Remote"},
            db_session,
        )
        assert result is False

    def test_punctuation_normalized_across_sources(self, db_session):
        # The SQL pre-filter uses ilike (case-insensitive exact match) so company
        # names must match for the hash comparison to run. Punctuation normalization
        # applies to the hash itself — tested here via title ("Sr." vs "Sr").
        _add_job(db_session, url="https://a.com/1", company="Acme", title="Sr. Engineer", location="Remote")
        result = is_duplicate(
            {"url": "https://b.com/2", "company": "Acme", "title": "Sr Engineer", "location": "Remote"},
            db_session,
        )
        assert result is True

    def test_empty_db_never_duplicate(self, db_session):
        assert not is_duplicate({"url": "https://x.com/1", "company": "X", "title": "Y", "location": "Z"}, db_session)
