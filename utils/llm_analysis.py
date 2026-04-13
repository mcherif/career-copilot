import json
from typing import Any, Dict, Iterable

import requests

import config
from utils.resume_selector import select_resume
from utils.text_cleaning import clean_description

RECOMMENDATION_VALUES = {"shortlist", "review", "reject"}

ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "fit_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "strengths": {
            "type": "array",
            "items": {"type": "string"},
        },
        "skill_gaps": {
            "type": "array",
            "items": {"type": "string"},
        },
        "recommendation": {
            "type": "string",
            "enum": ["shortlist", "review", "reject"],
        },
        "reasoning": {"type": "string"},
        "recommended_resume": {"type": "string"},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
    },
    "required": [
        "fit_score",
        "strengths",
        "skill_gaps",
        "recommendation",
        "reasoning",
        "recommended_resume",
        "confidence",
    ],
    "additionalProperties": False,
}

def _clamp(value: Any, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = minimum
    return max(minimum, min(parsed, maximum))

def _string_list(items: Iterable[Any], limit: int = 5) -> list[str]:
    values = []
    for item in items or []:
        text = str(item or "").strip()
        if text:
            values.append(text)
        if len(values) >= limit:
            break
    return values

def _job_description(job: Dict[str, Any], max_chars: int = 3500) -> str:
    description = str(job.get("description_text") or "").strip()
    if not description:
        description = clean_description(str(job.get("description") or ""))
    return description[:max_chars]

def _candidate_summary(profile: Dict[str, Any]) -> str:
    personal = profile.get("personal", {})
    seniority = profile.get("seniority", {})

    summary_lines = []

    # Identity
    name = str(personal.get("name") or "").strip()
    current_title = str(personal.get("current_title") or "").strip()
    current_company = str(personal.get("current_company") or "").strip()
    years_exp = personal.get("years_experience")
    id_parts = []
    if name:
        id_parts.append(name)
    if current_title and current_company:
        id_parts.append(f"{current_title} at {current_company}")
    elif current_title:
        id_parts.append(current_title)
    if years_exp is not None:
        id_parts.append(f"{years_exp} years experience")
    if id_parts:
        summary_lines.append(f"- {', '.join(id_parts)}")

    highlights = _string_list(profile.get("summary"), limit=6)
    for highlight in highlights:
        summary_lines.append(f"- {highlight}")

    location = str(personal.get("location") or "").strip()
    timezone = str(personal.get("timezone") or "").strip()
    if location or timezone:
        summary_lines.append(f"- Location: {location or 'unknown'}; timezone: {timezone or 'unknown'}")

    skills = profile.get("skills", [])[:12]
    if skills:
        summary_lines.append(f"- Core skills: {', '.join(map(str, skills))}")

    keywords = profile.get("keywords", [])[:8]
    if keywords:
        summary_lines.append(f"- Focus areas: {', '.join(map(str, keywords))}")

    # Work history — include verbatim so the LLM has real facts to cite
    work_history = profile.get("work_history", [])
    if work_history:
        summary_lines.append("- Work history (use ONLY these facts — do not invent others):")
        for entry in work_history[:6]:
            company = str(entry.get("company") or "").strip()
            title = str(entry.get("title") or "").strip()
            frm = str(entry.get("from") or "").strip()
            to = str(entry.get("to") or "present").strip()
            period = f"{frm}–{to}" if frm else to
            header = f"  • {company} ({period}): {title}" if company else f"  • {title} ({period})"
            summary_lines.append(header)
            for hl in (entry.get("highlights") or [])[:4]:
                summary_lines.append(f"    – {hl}")
    else:
        summary_lines.append(
            "- Work history: not provided. Do NOT invent company names or specific roles."
        )

    target_roles = profile.get("target_roles", [])[:6]
    if target_roles:
        summary_lines.append(f"- Target roles: {', '.join(map(str, target_roles))}")

    preferred_levels = seniority.get("preferred", [])
    if preferred_levels:
        summary_lines.append(f"- Seniority: {', '.join(map(str, preferred_levels[:4]))}")

    # Free-form Q&A context — facts to cite when answering application questions.
    notes = profile.get("notes", [])
    if notes:
        summary_lines.append("- Additional context (cite these facts when answering application questions):")
        for note in notes:
            summary_lines.append(f"  • {str(note).strip()}")

    return "\n".join(summary_lines)

def build_analysis_prompt(job: Dict[str, Any], profile: Dict[str, Any]) -> str:
    resume_names = [str(item.get("name")) for item in profile.get("resumes", []) if item.get("name")]
    default_resume = select_resume(job, profile).get("resume_name", "")
    output_contract = {
        "fit_score": "integer 0-100",
        "strengths": ["short bullet strings"],
        "skill_gaps": ["short bullet strings"],
        "recommendation": "one of shortlist, review, reject",
        "reasoning": "short concise explanation",
        "recommended_resume": f"one of {resume_names or ['']}",
        "confidence": "integer 0-100",
    }

    return (
        "Analyze this job for the candidate below.\n"
        "Return valid JSON only. Do not include markdown or commentary outside the JSON object.\n\n"
        "Candidate summary:\n"
        f"{_candidate_summary(profile)}\n\n"
        f"Available resumes: {', '.join(resume_names) if resume_names else 'none'}\n"
        f"Current rule-based score: {job.get('fit_score') if job.get('fit_score') is not None else 0}\n"
        f"Remote eligibility: {job.get('remote_eligibility') or 'unknown'}\n"
        f"Current rule-based resume suggestion: {default_resume or 'none'}\n\n"
        f"Job title: {job.get('title', '')}\n"
        f"Company: {job.get('company', '')}\n"
        f"Location: {job.get('raw_location_text') or job.get('location') or 'unknown'}\n"
        "Job description:\n"
        f"{_job_description(job)}\n\n"
        "Return JSON using this contract:\n"
        f"{json.dumps(output_contract, ensure_ascii=False)}"
    )

def parse_llm_response(text: str, allowed_resumes: list[str] | None = None) -> Dict[str, Any]:
    cleaned_text = str(text or "").strip()
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text.strip("`")
        if cleaned_text.startswith("json"):
            cleaned_text = cleaned_text[4:].strip()

    payload = json.loads(cleaned_text)
    recommendation = str(payload.get("recommendation") or "").strip().lower()
    if recommendation not in RECOMMENDATION_VALUES:
        raise ValueError(f"Invalid recommendation value: {recommendation}")

    recommended_resume = str(payload.get("recommended_resume") or "").strip()
    if allowed_resumes and recommended_resume and recommended_resume not in allowed_resumes:
        recommended_resume = ""

    return {
        "llm_fit_score": _clamp(payload.get("fit_score"), 0, 100),
        "llm_strengths": _string_list(payload.get("strengths")),
        "skill_gaps": _string_list(payload.get("skill_gaps")),
        "recommendation": recommendation,
        "fit_explanation": str(payload.get("reasoning") or "").strip(),
        "recommended_resume": recommended_resume,
        "llm_confidence": _clamp(payload.get("confidence"), 0, 100),
        "llm_status": "completed",
    }

def fallback_analysis(job: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "llm_fit_score": None,
        "llm_strengths": [],
        "skill_gaps": [],
        "recommendation": None,
        "fit_explanation": None,
        "recommended_resume": None,
        "llm_confidence": None,
        "llm_status": "failed",
    }

def analyze_job_with_ollama(job: Dict[str, Any], profile: Dict[str, Any], model: str) -> Dict[str, Any]:
    prompt = build_analysis_prompt(job, profile)
    resume_names = [str(item.get("name")) for item in profile.get("resumes", []) if item.get("name")]
    default_resume = select_resume(job, profile).get("resume_name", "")

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You evaluate job fit for a candidate. "
                    "Return valid JSON only and follow the provided schema exactly."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "format": ANALYSIS_SCHEMA,
        "keep_alive": "10m",
    }

    try:
        response = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        content = str(data.get("message", {}).get("content") or "").strip()
        if not content:
            raise ValueError("Ollama response did not contain message.content")

        result = parse_llm_response(content, resume_names)
        if not result["recommended_resume"] and default_resume:
            result["recommended_resume"] = default_resume
        return result
    except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
        result = fallback_analysis(job)
        result["error"] = str(exc)
        return result
    except Exception as exc:
        result = fallback_analysis(job)
        result["error"] = str(exc)
        return result
