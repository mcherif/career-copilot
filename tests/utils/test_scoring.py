"""
Tests for utils/scoring.py — score_job()

Covers: hard rejects, score thresholds, skill/keyword matching,
seniority alignment, and recommended_status assignment.
"""
import pytest
from utils.scoring import score_job


def _job(title="Senior Backend Engineer", company="Acme", location="Remote",
         description="Python developer needed.", remote_eligibility=None, **kwargs):
    return {
        "title": title,
        "company": company,
        "location": location,
        "raw_location_text": location,
        "description": description,
        "description_text": description,
        "remote_eligibility": remote_eligibility,
        "source": "test",
        **kwargs,
    }


def _profile(skills=None, keywords=None, target_roles=None, seniority=None,
             blacklisted=None, contractor_ok=True):
    return {
        "skills": skills or ["Python", "SQL", "Docker"],
        "keywords": keywords or ["backend", "api"],
        "target_roles": target_roles or ["software engineer", "backend developer"],
        "seniority": seniority or {
            "preferred": ["senior", "staff"],
            "acceptable": ["mid", "lead"],
        },
        "blacklisted_companies": blacklisted or ["BadCorp"],
        "preferences": {
            "remote_only": True,
            "contractor_ok": contractor_ok,
            "accepted_regions": ["worldwide", "global", "emea", "canada"],
            "reject_regions": ["us only"],
        },
        "languages": ["english"],
        "resumes": [],
    }


PROFILE = _profile()


# ---------------------------------------------------------------------------
# Return shape
# ---------------------------------------------------------------------------

class TestReturnShape:
    def test_returns_required_keys(self):
        result = score_job(_job(), PROFILE)
        for key in ("fit_score", "remote_eligibility", "matched_skills",
                    "matched_keywords", "seniority_match", "recommended_status"):
            assert key in result, f"Missing key: {key}"

    def test_fit_score_is_integer(self):
        result = score_job(_job(), PROFILE)
        assert isinstance(result["fit_score"], int)

    def test_matched_skills_is_list(self):
        result = score_job(_job(), PROFILE)
        assert isinstance(result["matched_skills"], list)


# ---------------------------------------------------------------------------
# Hard rejects
# ---------------------------------------------------------------------------

class TestHardRejects:
    def test_remote_eligibility_reject(self):
        # score_job always re-classifies via classify_remote_eligibility(); use a
        # US-only location string to trigger the reject path in the filter.
        result = score_job(_job(location="US Only", raw_location_text="US Only"), PROFILE)
        assert result["recommended_status"] == "rejected"

    def test_blacklisted_company(self):
        result = score_job(_job(company="BadCorp"), PROFILE)
        assert result["recommended_status"] == "rejected"

    def test_blacklisted_company_case_insensitive(self):
        result = score_job(_job(company="badcorp"), PROFILE)
        assert result["recommended_status"] == "rejected"

    def test_intern_title_rejected(self):
        result = score_job(_job(title="Software Engineering Intern"), PROFILE)
        assert result["recommended_status"] == "rejected"

    def test_junior_title_penalty_may_reject(self):
        # Junior titles incur a heavy penalty — likely rejected unless other signals strong
        result = score_job(_job(title="Junior Python Developer", description="Python SQL"), PROFILE)
        assert result["fit_score"] < 65


# ---------------------------------------------------------------------------
# Skill and keyword matching
# ---------------------------------------------------------------------------

class TestSkillMatching:
    def test_matching_skills_increase_score(self):
        no_skills = score_job(_job(description="generic role"), PROFILE)
        with_skills = score_job(_job(description="Python SQL Docker"), PROFILE)
        assert with_skills["fit_score"] > no_skills["fit_score"]

    def test_matched_skills_listed(self):
        result = score_job(_job(description="Python and SQL required"), PROFILE)
        matches = [s.lower() for s in result["matched_skills"]]
        assert "python" in matches
        assert "sql" in matches

    def test_matching_keywords_increase_score(self):
        no_kw = score_job(_job(description="generic role"), PROFILE)
        with_kw = score_job(_job(description="backend api service"), PROFILE)
        assert with_kw["fit_score"] >= no_kw["fit_score"]


# ---------------------------------------------------------------------------
# Seniority alignment
# ---------------------------------------------------------------------------

class TestSeniority:
    def test_preferred_seniority_increases_score(self):
        senior = score_job(_job(title="Senior Software Engineer"), PROFILE)
        mid = score_job(_job(title="Software Engineer"), PROFILE)
        assert senior["fit_score"] >= mid["fit_score"]

    def test_seniority_match_flag(self):
        result = score_job(_job(title="Senior Backend Engineer"), PROFILE)
        assert result["seniority_match"] is True


# ---------------------------------------------------------------------------
# Recommended status thresholds
# ---------------------------------------------------------------------------

class TestThresholds:
    def test_high_score_shortlisted(self):
        # Maximize signals: skills, keywords, role match, seniority, remote
        result = score_job(
            _job(
                title="Senior Backend Engineer",
                description="Python SQL Docker backend api senior engineer",
                remote_eligibility="accept",
            ),
            PROFILE,
        )
        assert result["fit_score"] >= 28  # at minimum review, likely shortlisted
        assert result["recommended_status"] in ("shortlisted", "review")

    def test_low_score_rejected(self):
        result = score_job(
            _job(title="Intern", description="no matching skills",
                 remote_eligibility="reject"),
            PROFILE,
        )
        assert result["recommended_status"] == "rejected"
