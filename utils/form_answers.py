"""
LLM-powered answers for freeform application questions.

Called from fill_form when a textarea or narrative text field cannot be
satisfied by static profile data.  Each detected question gets a short,
tailored Ollama response before the fill pass runs.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

import requests

import config
from utils.llm_analysis import _candidate_summary, _job_description

_SYSTEM_PROMPT = (
    "You are helping a job candidate fill out their application. "
    "Answer the question briefly (2-4 sentences), in first person, "
    "specific to the role and company. "
    "Plain text only — no bullet points, no markdown, no headers."
)

# Labels that clearly map to standard profile fields — never route to LLM.
_SKIP_LABELS = {
    "name", "first name", "last name", "full name",
    "email", "phone", "linkedin", "github", "portfolio",
    "website", "url", "location", "city", "country",
    "salary", "rate", "compensation", "years", "experience",
    "cover letter", "resume", "cv",
}


def is_llm_question(label_lower: str, ftype: str) -> bool:
    """Return True if this field should receive an LLM-generated answer.

    Textareas are always candidates (except cover-letter fields which are
    handled by the standard cover_letter rule).  Text/URL inputs only if
    their label contains a question-like keyword.
    """
    if ftype not in ("textarea", "text", "url"):
        return False
    if any(skip in label_lower for skip in _SKIP_LABELS):
        return False
    if ftype == "textarea":
        return True
    question_triggers = [
        "motivation", "why", "what about", "what draws",
        "what excites", "how would", "describe", "tell us",
        "greatest", "challenge", "proud", "achievement",
    ]
    return any(t in label_lower for t in question_triggers)


def _build_prompt(question: str, job: Dict[str, Any], profile: Dict[str, Any]) -> str:
    name = (profile.get("personal") or {}).get("name") or "the candidate"
    return "\n".join([
        f"Candidate: {name}",
        "",
        f"Role: {job.get('title', '')} at {job.get('company', '')}",
        "",
        "Job description (excerpt):",
        _job_description(job, max_chars=1200),
        "",
        "Candidate profile:",
        _candidate_summary(profile),
        "",
        f"Application question: {question}",
        "",
        "Answer (2-4 sentences, first person, plain text):",
    ])


def _call_ollama(question: str, job: Dict[str, Any], profile: Dict[str, Any]) -> str:
    """Synchronous Ollama call.  Returns answer text, or '' on failure."""
    payload = {
        "model": config.OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_prompt(question, job, profile)},
        ],
        "stream": False,
        "keep_alive": "10m",
    }
    try:
        r = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
        r.raise_for_status()
        return str(r.json().get("message", {}).get("content") or "").strip()
    except Exception:
        return ""


async def generate_answers(
    questions: List[Tuple[int, str]],
    job: Dict[str, Any],
    profile: Dict[str, Any],
) -> Dict[int, str]:
    """Generate LLM answers for a list of (field_idx, question_text) pairs.

    Runs all Ollama calls concurrently in thread executors so they don't
    block the Playwright event loop.  Returns {field_idx: answer_text}.
    """
    if not questions:
        return {}

    loop = asyncio.get_event_loop()

    async def _one(idx: int, question: str) -> Tuple[int, str]:
        answer = await loop.run_in_executor(None, _call_ollama, question, job, profile)
        return idx, answer

    results = await asyncio.gather(
        *[_one(idx, q) for idx, q in questions],
        return_exceptions=True,
    )

    return {
        idx: answer
        for result in results
        if not isinstance(result, Exception)
        for idx, answer in [result]
        if answer
    }
