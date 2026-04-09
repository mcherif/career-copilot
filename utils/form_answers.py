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
    "Answer the question briefly (2-4 sentences), in first person, specific to the role and company. "
    "CRITICAL: Only use facts explicitly stated in the candidate profile below. "
    "Do NOT invent, guess, or embellish any company names, job titles, project names, "
    "colleagues, specific achievements, or metrics that are not in the profile. "
    "If the profile lacks enough detail for a specific answer, give a brief, honest "
    "general answer based only on the skills and experience areas listed. "
    "Plain text only — no bullet points, no markdown, no headers."
)

# Labels that clearly map to standard profile fields — never route to LLM.
_SKIP_LABELS = {
    "name", "first name", "last name", "full name",
    "email", "phone", "linkedin", "github",
    "salary", "rate",
    "cover letter", "resume", "cv",
}
# NOTE: "compensation", "years", "experience", "location", "city", "country"
# are intentionally NOT here — substring-matching them blocks freeform questions
# like "3+ years of experience with Python" or "compensation requirements".
# The _resolve_text_value guard in fill_form already skips LLM when a
# rule-based value is available for structured fields.


def is_llm_question(label_lower: str, ftype: str) -> bool:
    """Return True if this field should receive an LLM-generated answer.

    Textareas are always candidates (except cover-letter/profile-data fields).
    Text/URL inputs only if their label contains a question-like keyword.
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
        "share", "recent project", "work sample", "examples of",
        "please provide", "please explain", "tell me",
        "please confirm", "confirm that", "are you ready",
        "what was", "what were", "how long", "fastest",
        "compensation", "salary requirement", "rate requirement",
        "specifications", "internet speed",
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


def _call_ollama_pick(
    question: str, options: List[str], profile: Dict[str, Any], job: Dict[str, Any]
) -> str | None:
    """Use LLM to pick the best option from a list for a dropdown/radio field.

    Returns the exact option text, or None on failure.
    """
    p = profile.get("personal", {})
    profile_summary = ", ".join(filter(None, [
        f"name={p.get('name', '')}",
        f"location={p.get('location', '')}",
        f"gender={p.get('gender', '')}",
        f"race={p.get('race', '')}",
        f"sexual_orientation={p.get('sexual_orientation', '')}",
        f"disability={p.get('disability', '')}",
        f"veteran={p.get('veteran', '')}",
        f"years_experience={p['years_experience']}" if p.get('years_experience') is not None else "",
        f"age={p['age']}" if p.get('age') is not None else "",
    ]))
    opts_text = "\n".join(f"{i + 1}. {o}" for i, o in enumerate(options))
    prompt = "\n".join([
        f"Candidate profile: {profile_summary}",
        "",
        f"Form question: {question}",
        "",
        "Options:",
        opts_text,
        "",
        "Pick the number of the most appropriate option for this candidate.",
        "Reply with just the number, nothing else.",
    ])
    payload = {
        "model": config.OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content":
                "You are helping fill a job application. "
                "Pick the best matching option for the candidate. "
                "Reply with just the option number."},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "keep_alive": "10m",
    }
    try:
        import re as _re
        r = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
        r.raise_for_status()
        content = str(r.json().get("message", {}).get("content") or "").strip()
        m = _re.search(r"\d+", content)
        if m:
            idx = int(m.group()) - 1
            if 0 <= idx < len(options):
                return options[idx]
    except Exception:
        pass
    return None


async def pick_option(
    question: str,
    options: List[str],
    profile: Dict[str, Any],
    job: Dict[str, Any],
) -> str | None:
    """Async wrapper: ask the LLM to pick the best option for a choice field.

    Only called when rule-based synonym matching fails.
    Returns the exact option text on success, or None.
    """
    if not options or not question:
        return None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _call_ollama_pick, question, options, profile, job)


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
