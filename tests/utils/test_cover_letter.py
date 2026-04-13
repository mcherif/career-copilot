"""
Tests for utils/cover_letter.py — prompt building and Ollama call.
"""
import json
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _profile():
    return {
        "personal": {"name": "Jane Doe", "location": "Canada", "timezone": "EST"},
        "skills": ["Python", "FastAPI", "Docker"],
        "keywords": ["backend", "api"],
        "target_roles": ["backend engineer"],
        "seniority": {"preferred": ["senior"], "acceptable": ["mid"]},
        "preferences": {
            "accepted_regions": ["worldwide"],
            "reject_regions": [],
            "contractor_ok": True,
        },
        "languages": ["english"],
        "resumes": [{"name": "backend_resume", "path": "resumes/jane.pdf", "tags": ["backend"]}],
    }


def _job(**overrides):
    base = {
        "title": "Senior Backend Engineer",
        "company": "Acme Corp",
        "location": "Remote",
        "raw_location_text": "Remote - Worldwide",
        "description": "We build distributed systems.",
        "description_text": "We build distributed systems.",
        "fit_explanation": "Strong Python and API background.",
        "llm_strengths": json.dumps(["Python expertise", "API design"]),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# build_cover_letter_prompt
# ---------------------------------------------------------------------------

class TestBuildCoverLetterPrompt:
    def test_contains_job_title(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Senior Backend Engineer" in prompt

    def test_contains_company(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Acme Corp" in prompt

    def test_contains_candidate_name(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Jane Doe" in prompt

    def test_contains_job_description(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "distributed systems" in prompt

    def test_contains_strengths_from_json(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Python expertise" in prompt

    def test_strengths_as_plain_list(self):
        from utils.cover_letter import build_cover_letter_prompt
        job = _job(llm_strengths=["Go expertise", "gRPC"])
        prompt = build_cover_letter_prompt(job, _profile())
        assert "Go expertise" in prompt

    def test_no_strengths_still_renders(self):
        from utils.cover_letter import build_cover_letter_prompt
        job = _job(llm_strengths=None, fit_explanation=None)
        prompt = build_cover_letter_prompt(job, _profile())
        assert "Senior Backend Engineer" in prompt

    def test_contains_prior_reasoning(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Strong Python" in prompt

    def test_contains_candidate_skills(self):
        from utils.cover_letter import build_cover_letter_prompt
        prompt = build_cover_letter_prompt(_job(), _profile())
        assert "Python" in prompt

    def test_missing_name_uses_fallback(self):
        from utils.cover_letter import build_cover_letter_prompt
        profile = _profile()
        profile["personal"]["name"] = ""
        prompt = build_cover_letter_prompt(_job(), profile)
        assert "Candidate" in prompt

    def test_description_truncated_to_max_chars(self):
        from utils.cover_letter import build_cover_letter_prompt
        long_desc = "z" * 5000
        job = _job(description_text=long_desc, description="")
        prompt = build_cover_letter_prompt(job, _profile())
        # _job_description caps at 3000 chars — full 5000 must not be present
        assert prompt.count("z") < 4000


# ---------------------------------------------------------------------------
# generate_cover_letter
# ---------------------------------------------------------------------------

class TestGenerateCoverLetter:
    _T = "utils.cover_letter.requests.post"

    def _mock_resp(self, content):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {"message": {"content": content}}
        return m

    def test_returns_cover_letter_on_success(self):
        from utils.cover_letter import generate_cover_letter
        body = "I am writing to express my interest in..."
        with patch(self._T, return_value=self._mock_resp(body)):
            result = generate_cover_letter(_job(), _profile())
        assert result["status"] == "ok"
        assert result["cover_letter"].startswith("Dear Hiring Team,")
        assert body in result["cover_letter"]
        assert result["cover_letter"].endswith("Best,\nJane Doe")

    def test_strips_whitespace_from_response(self):
        from utils.cover_letter import generate_cover_letter
        body = "  \nI bring strong backend skills.\n  "
        with patch(self._T, return_value=self._mock_resp(body)):
            result = generate_cover_letter(_job(), _profile())
        assert body.strip() in result["cover_letter"]
        assert result["cover_letter"].startswith("Dear Hiring Team,")

    def test_returns_failed_on_http_error(self):
        from utils.cover_letter import generate_cover_letter
        from requests.exceptions import HTTPError
        with patch(self._T, side_effect=HTTPError("503")):
            result = generate_cover_letter(_job(), _profile())
        assert result["status"] == "failed"
        assert result["cover_letter"] is None
        assert "error" in result

    def test_returns_failed_on_network_error(self):
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, side_effect=ConnectionError("timeout")):
            result = generate_cover_letter(_job(), _profile())
        assert result["status"] == "failed"

    def test_returns_failed_on_empty_content(self):
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, return_value=self._mock_resp("")):
            result = generate_cover_letter(_job(), _profile())
        assert result["status"] == "failed"
        assert "error" in result

    def test_uses_provided_model(self):
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, return_value=self._mock_resp("letter")) as mock_post:
            generate_cover_letter(_job(), _profile(), model="llama3:8b")
        payload = mock_post.call_args[1]["json"]
        assert payload["model"] == "llama3:8b"

    def test_uses_default_model_when_none(self):
        import config
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, return_value=self._mock_resp("letter")) as mock_post:
            generate_cover_letter(_job(), _profile(), model=None)
        payload = mock_post.call_args[1]["json"]
        assert payload["model"] == config.OLLAMA_MODEL

    def test_system_prompt_in_messages(self):
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, return_value=self._mock_resp("letter")) as mock_post:
            generate_cover_letter(_job(), _profile())
        messages = mock_post.call_args[1]["json"]["messages"]
        roles = [m["role"] for m in messages]
        assert "system" in roles
        assert "user" in roles

    def test_no_format_schema_in_payload(self):
        """Cover letters use free-form text, not a JSON schema."""
        from utils.cover_letter import generate_cover_letter
        with patch(self._T, return_value=self._mock_resp("letter")) as mock_post:
            generate_cover_letter(_job(), _profile())
        payload = mock_post.call_args[1]["json"]
        assert "format" not in payload
