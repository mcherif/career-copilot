"""
Interview prep sheet generation pipeline.

Orchestrates 6 sequential steps to produce a personalized prep sheet
grounded strictly in the candidate's profile and the job description.
"""
import datetime
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

import config
from models.database import InterviewPrepSheet, Job
from utils.llm_analysis import _candidate_summary
from utils.text_cleaning import clean_description


def _jd_text(job: Job, max_chars: int = 4000) -> str:
    text = str(job.description_text or "").strip()
    if not text:
        text = clean_description(str(job.description or ""))
    return text[:max_chars]


def _call_ollama(prompt: str, schema: dict) -> dict:
    """Call Ollama with retry on Timeout. Raises RuntimeError on exhausted retries or other failures."""
    payload = {
        "model": config.OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You generate structured interview preparation content. "
                    "Return valid JSON only. Ground ALL content strictly in the "
                    "provided job description and candidate profile. "
                    "Do NOT produce generic interview advice."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "format": schema,
        "keep_alive": "10m",
    }

    last_exc: Exception = RuntimeError("No attempts made")
    for attempt in range(config.MAX_RETRIES):
        try:
            response = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            content = str(data.get("message", {}).get("content") or "").strip()
            if not content:
                raise ValueError("Ollama returned empty content")
            return json.loads(content)
        except requests.Timeout as exc:
            last_exc = exc
            if attempt < config.MAX_RETRIES - 1:
                time.sleep(config.RETRY_BACKOFF)
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(str(exc)) from exc

    raise RuntimeError(f"Ollama timed out after {config.MAX_RETRIES} attempts") from last_exc


def _step_context_analysis(company: str, jd: str) -> Tuple[dict, dict]:
    schema = {
        "type": "object",
        "properties": {
            "company_snapshot": {
                "type": "object",
                "properties": {
                    "industry": {"type": "string"},
                    "likely_size": {"type": "string"},
                    "culture_signals": {"type": "array", "items": {"type": "string"}},
                    "red_flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["industry", "likely_size", "culture_signals", "red_flags"],
            },
            "role_summary": {
                "type": "object",
                "properties": {
                    "core_responsibilities": {"type": "array", "items": {"type": "string"}},
                    "must_have_skills": {"type": "array", "items": {"type": "string"}},
                    "nice_to_have_skills": {"type": "array", "items": {"type": "string"}},
                    "seniority_signals": {"type": "string"},
                },
                "required": ["core_responsibilities", "must_have_skills", "nice_to_have_skills", "seniority_signals"],
            },
        },
        "required": ["company_snapshot", "role_summary"],
        "additionalProperties": False,
    }
    prompt = (
        f"Company: {company}\n\nJob description:\n{jd}\n\n"
        "Extract company context and role requirements. "
        "Base ONLY on what is explicitly stated or clearly implied by this JD."
    )
    result = _call_ollama(prompt, schema)
    return result["company_snapshot"], result["role_summary"]


def _step_question_generation(jd: str) -> Tuple[List[str], List[str]]:
    schema = {
        "type": "object",
        "properties": {
            "technical_questions": {"type": "array", "items": {"type": "string"}, "minItems": 3, "maxItems": 8},
            "behavioral_questions": {"type": "array", "items": {"type": "string"}, "minItems": 3, "maxItems": 8},
        },
        "required": ["technical_questions", "behavioral_questions"],
        "additionalProperties": False,
    }
    prompt = (
        f"Job description:\n{jd}\n\n"
        "Predict the most likely interview questions for this specific role. "
        "technical_questions: questions testing skills explicitly required by this JD. "
        "behavioral_questions: questions targeting the responsibilities and culture signals in this JD. "
        "Do NOT include generic questions unrelated to this JD."
    )
    result = _call_ollama(prompt, schema)
    return result["technical_questions"], result["behavioral_questions"]


def _step_profile_mapping(jd: str, candidate_summary: str) -> Tuple[list, list]:
    schema = {
        "type": "object",
        "properties": {
            "talking_points": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "jd_requirement": {"type": "string"},
                        "candidate_evidence": {"type": "string"},
                        "suggested_story": {"type": "string"},
                    },
                    "required": ["jd_requirement", "candidate_evidence", "suggested_story"],
                    "additionalProperties": False,
                },
                "minItems": 1,
            },
            "gaps_or_risks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "jd_requirement": {"type": "string"},
                        "gap_description": {"type": "string"},
                        "mitigation": {"type": "string"},
                    },
                    "required": ["jd_requirement", "gap_description", "mitigation"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["talking_points", "gaps_or_risks"],
        "additionalProperties": False,
    }
    prompt = (
        f"Job description:\n{jd}\n\nCandidate profile:\n{candidate_summary}\n\n"
        "Map this candidate's experience to this JD. "
        "talking_points: concrete matches between candidate history and JD requirements with story suggestions. "
        "gaps_or_risks: JD requirements the candidate does not clearly satisfy, with mitigation advice. "
        "Cite ONLY facts from the candidate profile above — do NOT invent experience."
    )
    result = _call_ollama(prompt, schema)
    return result["talking_points"], result["gaps_or_risks"]


def _step_action_plan(
    company_snapshot: dict,
    role_summary: dict,
    technical_questions: list,
    behavioral_questions: list,
    talking_points: list,
    gaps_or_risks: list,
) -> dict:
    schema = {
        "type": "object",
        "properties": {
            "minutes_0_10": {"type": "array", "items": {"type": "string"}},
            "minutes_10_20": {"type": "array", "items": {"type": "string"}},
            "minutes_20_30": {"type": "array", "items": {"type": "string"}},
            "priority_note": {"type": "string"},
        },
        "required": ["minutes_0_10", "minutes_10_20", "minutes_20_30", "priority_note"],
        "additionalProperties": False,
    }
    context = {
        "company": company_snapshot,
        "role": role_summary,
        "technical_questions": technical_questions[:4],
        "behavioral_questions": behavioral_questions[:4],
        "talking_points": [tp.get("jd_requirement") for tp in talking_points[:4]],
        "gaps": [g.get("jd_requirement") for g in gaps_or_risks[:3]],
    }
    prompt = (
        f"Interview context:\n{json.dumps(context, indent=2)}\n\n"
        "Generate a structured 30-minute prep plan. "
        "minutes_0_10: what to review/prepare in the first 10 minutes. "
        "minutes_10_20: practice focus for the next 10 minutes. "
        "minutes_20_30: final review and confidence-building. "
        "priority_note: the single most critical thing to nail for this interview."
    )
    return _call_ollama(prompt, schema)


def run_interview_prep(
    job_application_id: int,
    profile: Dict[str, Any],
    session: Any,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> InterviewPrepSheet:
    """
    Run the full interview prep pipeline for a job application.

    Args:
        job_application_id: ID of the Job record in the database.
        profile: Loaded candidate profile dict.
        session: SQLAlchemy session (caller manages lifecycle).
        progress_callback: Optional callable(step, message) for progress reporting.

    Returns:
        The saved InterviewPrepSheet with status='completed'.

    Raises:
        ValueError: Job not found, or job has no description.
        RuntimeError: AI generation failed after retries.
    """
    def _progress(step: int, msg: str) -> None:
        if progress_callback:
            progress_callback(step, msg)

    # Step 1: Data Retrieval
    _progress(1, "Fetching job and profile...")
    job = session.query(Job).filter(Job.id == job_application_id).first()
    if not job:
        raise ValueError(f"Job application ID {job_application_id} not found")

    jd = _jd_text(job)
    if not jd:
        sheet = InterviewPrepSheet(
            job_application_id=job.id,
            status="failed",
            error_message="Job description is empty — cannot generate prep sheet",
        )
        session.add(sheet)
        session.commit()
        raise ValueError(f"Job {job_application_id} has no description")

    candidate_summary = _candidate_summary(profile)

    # Upsert a processing record so partial state is visible during generation
    sheet = session.query(InterviewPrepSheet).filter(
        InterviewPrepSheet.job_application_id == job.id
    ).first()
    if sheet is None:
        sheet = InterviewPrepSheet(job_application_id=job.id, status="processing")
        session.add(sheet)
    else:
        sheet.status = "processing"
        sheet.error_message = None
    session.commit()

    try:
        # Step 2: Context Analysis
        _progress(2, "Analyzing company and role...")
        company_snapshot, role_summary = _step_context_analysis(job.company or "", jd)

        # Step 3: Question Generation
        _progress(3, "Generating likely questions...")
        technical_questions, behavioral_questions = _step_question_generation(jd)

        # Step 4: Profile Mapping
        _progress(4, "Mapping your profile to the JD...")
        talking_points, gaps_or_risks = _step_profile_mapping(jd, candidate_summary)

        # Step 5: Action Plan
        _progress(5, "Building 30-min prep plan...")
        prep_plan = _step_action_plan(
            company_snapshot, role_summary,
            technical_questions, behavioral_questions,
            talking_points, gaps_or_risks,
        )

        # Step 6: Storage
        _progress(6, "Saving to database...")
        sheet.company_snapshot = json.dumps(company_snapshot)
        sheet.role_requirements_summary = json.dumps(role_summary)
        sheet.likely_technical_questions = json.dumps(technical_questions)
        sheet.likely_behavioral_questions = json.dumps(behavioral_questions)
        sheet.talking_points = json.dumps(talking_points)
        sheet.gaps_or_risks = json.dumps(gaps_or_risks)
        sheet.prep_plan_30_min = json.dumps(prep_plan)
        sheet.status = "completed"
        sheet.generated_at = datetime.datetime.utcnow()
        session.commit()
        session.refresh(sheet)

    except RuntimeError as exc:
        sheet.status = "failed"
        sheet.error_message = str(exc)
        session.commit()
        raise

    return sheet
