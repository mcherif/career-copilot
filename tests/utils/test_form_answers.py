"""Tests for utils/form_answers.py — LLM question detection and answer generation."""
import asyncio
from unittest.mock import MagicMock, patch

from utils.form_answers import generate_answers, is_llm_question


# ---------------------------------------------------------------------------
# is_llm_question
# ---------------------------------------------------------------------------

def test_textarea_is_llm_question():
    assert is_llm_question("motivation", "textarea") is True


def test_textarea_skipped_for_cover_letter():
    assert is_llm_question("cover letter", "textarea") is False


def test_textarea_skipped_for_standard_fields():
    for label in ("name", "email", "phone", "linkedin", "github", "salary", "resume"):
        assert is_llm_question(label, "textarea") is False, f"should skip {label}"


def test_text_input_with_motivation_keyword():
    assert is_llm_question("motivation", "text") is True


def test_text_input_with_why_keyword():
    assert is_llm_question("why are you interested", "text") is True


def test_text_input_with_no_question_keyword():
    # Generic text input with no question-like keyword — not LLM territory.
    assert is_llm_question("some random field", "text") is False


def test_select_never_llm():
    assert is_llm_question("motivation", "select") is False


# ---------------------------------------------------------------------------
# generate_answers
# ---------------------------------------------------------------------------

JOB = {"title": "Senior Engineer", "company": "Acme", "description_text": "Build things."}
PROFILE = {"personal": {"name": "Jane"}, "skills": ["Python"]}


def test_generate_answers_returns_dict():
    fake_answer = "I am passionate about this role."

    with patch("utils.form_answers._call_ollama", return_value=fake_answer):
        result = asyncio.run(generate_answers([(0, "Why do you want this job?")], JOB, PROFILE))

    assert result == {0: fake_answer}


def test_generate_answers_empty_input():
    result = asyncio.run(generate_answers([], JOB, PROFILE))
    assert result == {}


def test_generate_answers_empty_ollama_response_excluded():
    with patch("utils.form_answers._call_ollama", return_value=""):
        result = asyncio.run(generate_answers([(0, "Question?")], JOB, PROFILE))

    assert result == {}


def test_generate_answers_multiple_questions_concurrent():
    """All questions are answered and keyed by their field index."""
    answers = {
        "Why do you want this role?": "Answer A",
        "What motivates you?": "Answer B",
    }

    def _fake(question, job, profile):
        return answers.get(question, "")

    with patch("utils.form_answers._call_ollama", side_effect=_fake):
        result = asyncio.run(generate_answers(
            [(1, "Why do you want this role?"), (3, "What motivates you?")],
            JOB, PROFILE,
        ))

    assert result == {1: "Answer A", 3: "Answer B"}


def test_generate_answers_ollama_exception_excluded():
    """If Ollama raises, that question is silently skipped."""
    def _fake(question, job, profile):
        raise RuntimeError("ollama down")

    with patch("utils.form_answers._call_ollama", side_effect=_fake):
        result = asyncio.run(generate_answers([(0, "Question?")], JOB, PROFILE))

    assert result == {}


def test_call_ollama_returns_empty_on_request_error():
    """Network failure → empty string, no exception propagated."""
    from utils.form_answers import _call_ollama

    with patch("utils.form_answers.requests.post", side_effect=ConnectionError("timeout")):
        result = _call_ollama("What motivates you?", JOB, PROFILE)

    assert result == ""


def test_call_ollama_parses_content():
    from utils.form_answers import _call_ollama

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"message": {"content": "Great answer."}}
    mock_resp.raise_for_status = MagicMock()

    with patch("utils.form_answers.requests.post", return_value=mock_resp):
        result = _call_ollama("Question?", JOB, PROFILE)

    assert result == "Great answer."
