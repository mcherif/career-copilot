"""
Cover letter generation via Ollama.

Generates a tailored, concise cover letter for a shortlisted job using
the candidate profile and any prior LLM analysis already in the DB.
"""
import json
from typing import Any, Dict

import requests

import config
from utils.llm_analysis import _candidate_summary, _job_description


COVER_LETTER_SYSTEM_PROMPT = (
    "You are a professional cover letter writer. "
    "Write concise, authentic cover letters in plain text (no markdown, no headers). "
    "Three short paragraphs: why this role, what you bring, brief close. "
    "Match the tone to the company. Never use generic filler phrases like 'I am excited to apply'."
)


def build_cover_letter_prompt(job: Dict[str, Any], profile: Dict[str, Any]) -> str:
    personal = profile.get("personal", {})
    name = str(personal.get("name") or "").strip()

    strengths_raw = job.get("llm_strengths") or ""
    if isinstance(strengths_raw, str):
        try:
            strengths = json.loads(strengths_raw)
        except (json.JSONDecodeError, ValueError):
            strengths = [s.strip() for s in strengths_raw.split(",") if s.strip()]
    else:
        strengths = list(strengths_raw)

    reasoning = str(job.get("fit_explanation") or "").strip()

    lines = [
        f"Candidate name: {name or 'Candidate'}",
        "",
        "Candidate profile:",
        _candidate_summary(profile),
        "",
        f"Job title: {job.get('title', '')}",
        f"Company: {job.get('company', '')}",
        f"Location: {job.get('raw_location_text') or job.get('location') or 'Remote'}",
        "",
        "Job description:",
        _job_description(job, max_chars=3000),
    ]

    if strengths:
        lines += ["", "Key strengths identified for this role:", *[f"- {s}" for s in strengths[:5]]]

    if reasoning:
        lines += ["", f"Prior assessment: {reasoning}"]

    lines += [
        "",
        "Write a cover letter (plain text, 3 short paragraphs, no markdown, no salutation line). "
        "Do not start with 'Dear' or a greeting — start directly with the opening paragraph.",
    ]

    return "\n".join(lines)


def generate_cover_letter(
    job: Dict[str, Any],
    profile: Dict[str, Any],
    model: str | None = None,
) -> Dict[str, Any]:
    """
    Call Ollama to generate a cover letter for the job.

    Returns:
        {"cover_letter": str, "status": "ok"} on success
        {"cover_letter": None, "status": "failed", "error": str} on failure
    """
    model = model or config.OLLAMA_MODEL
    prompt = build_cover_letter_prompt(job, profile)

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": COVER_LETTER_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "keep_alive": "10m",
    }

    try:
        response = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        content = str(data.get("message", {}).get("content") or "").strip()
        if not content:
            raise ValueError("Ollama returned empty content")
        return {"cover_letter": content, "status": "ok"}
    except Exception as exc:
        return {"cover_letter": None, "status": "failed", "error": str(exc)}
