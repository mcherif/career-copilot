"""
Resume parser: extract structured profile data from a PDF resume using Ollama.

Usage:
    python -m utils.resume_parser path/to/resume.pdf [output.yaml]

API endpoint: POST /api/profile/parse-resume (multipart, field: file)

Hash-based change detection: the SHA-256 of the uploaded PDF is stored in
profile.yaml under ``_resume_hash``.  If the same file is re-uploaded, the
existing profile is returned unchanged — preserving any hand-edits made after
the initial parse.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict

import requests
import yaml

import config

# ---------------------------------------------------------------------------
# PDF hash
# ---------------------------------------------------------------------------

def pdf_sha256(pdf_path: str) -> str:
    """Return the SHA-256 hex digest of a PDF file."""
    h = hashlib.sha256()
    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# PDF text extraction
# ---------------------------------------------------------------------------

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract raw text from a PDF file using pdfminer.six."""
    try:
        from pdfminer.high_level import extract_text as _extract
        return _extract(pdf_path) or ""
    except ImportError as exc:
        raise RuntimeError(
            "pdfminer.six is required: pip install pdfminer.six"
        ) from exc


# ---------------------------------------------------------------------------
# LLM prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a resume parser. Extract structured information from the resume text below. "
    "Return valid JSON only — no markdown, no commentary. "
    "Use exactly the schema provided. "
    "For any field you cannot determine from the resume, use null or an empty list."
)

_OUTPUT_SCHEMA = {
    "name": "string",
    "email": "string or null",
    "phone": "string or null",
    "linkedin": "string or null",
    "github": "string or null",
    "location": "string or null",
    "current_title": "string or null",
    "years_experience": "integer or null",
    "summary": ["list of short highlight strings (3-6 items)"],
    "skills": ["list of technical skill strings"],
    "keywords": ["list of domain/technology keyword strings"],
    "target_roles": ["list of job-title strings the candidate is targeting"],
    "languages": ["list of spoken language strings"],
    "work_history": [
        {
            "company": "string",
            "title": "string",
            "from": "string (e.g. 'Jan 2022' or '2022')",
            "to": "string (e.g. 'Dec 2024', '2024', or 'present')",
            "highlights": ["list of short bullet strings (2-4 items)"],
        }
    ],
    "education": [
        {
            "school": "string (full institution name)",
            "degree": "string (e.g. 'Bachelor\\'s', 'Master\\'s', 'PhD')",
            "field": "string or null (field of study / major)",
            "from": "string or null",
            "to": "string or null",
        }
    ],
    "patents": ["list of patent strings or null"],
    "certifications": ["list of certification strings or null"],
}

_SCHEMA_JSON = json.dumps(_OUTPUT_SCHEMA, indent=2)


def _build_prompt(resume_text: str) -> str:
    # Truncate to avoid token limits (~12000 chars covers most full resumes)
    truncated = resume_text[:12000]
    return "\n".join([
        "Resume text:",
        truncated,
        "",
        "Extract the information and return JSON matching this schema exactly:",
        _SCHEMA_JSON,
    ])


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def _call_ollama(resume_text: str, model: str | None = None) -> Dict[str, Any]:
    """Send resume text to Ollama and return parsed JSON."""
    model = model or config.OLLAMA_MODEL
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_prompt(resume_text)},
        ],
        "stream": False,
        "keep_alive": "10m",
    }
    response = requests.post(config.OLLAMA_URL, json=payload, timeout=config.LLM_TIMEOUT)
    response.raise_for_status()
    content = str(response.json().get("message", {}).get("content") or "").strip()

    # Strip markdown fences if present
    if content.startswith("```"):
        content = content.strip("`")
        if content.startswith("json"):
            content = content[4:].strip()

    return json.loads(content)


# ---------------------------------------------------------------------------
# Profile YAML builder
# ---------------------------------------------------------------------------

def _safe_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _safe_list(val: Any, limit: int = 20) -> list:
    if not isinstance(val, list):
        return []
    return [str(x).strip() for x in val if str(x).strip()][:limit]


def build_profile_yaml(
    parsed: Dict[str, Any],
    existing: Dict[str, Any] | None = None,
    resume_hash: str | None = None,
) -> str:
    """Convert LLM-parsed resume data into a profile.yaml string.

    If ``existing`` is provided, non-empty LLM values override existing ones
    but structural sections (credentials, target_companies, blacklisted_companies,
    resumes, preferences) are preserved from the existing profile.
    """
    base: Dict[str, Any] = existing or {}

    personal = dict(base.get("personal") or {})

    # Infer current_company / current_title from the most recent work entry
    # when the LLM didn't return them directly.
    _wh = parsed.get("work_history") or []
    _first_wh = _wh[0] if isinstance(_wh, list) and _wh and isinstance(_wh[0], dict) else {}
    _inferred_company = _safe_str(_first_wh.get("company"))
    _inferred_title   = _safe_str(_first_wh.get("title"))

    personal.update({
        k: v for k, v in {
            "name": _safe_str(parsed.get("name")),
            "email": _safe_str(parsed.get("email")),
            "phone": _safe_str(parsed.get("phone")),
            "linkedin": _safe_str(parsed.get("linkedin")),
            "github": _safe_str(parsed.get("github")),
            "location": _safe_str(parsed.get("location")),
            "current_title": _safe_str(parsed.get("current_title")) or _inferred_title,
            "current_company": _safe_str(parsed.get("current_company")) or _inferred_company,
            "years_experience": parsed.get("years_experience"),
        }.items()
        if v is not None
    })

    work_history = []
    for entry in (parsed.get("work_history") or []):
        if not isinstance(entry, dict):
            continue
        wh = {
            "company": _safe_str(entry.get("company")) or "",
            "title": _safe_str(entry.get("title")) or "",
            "from": _safe_str(entry.get("from")) or "",
            "to": _safe_str(entry.get("to")) or "present",
        }
        highlights = _safe_list(entry.get("highlights"), 6)
        if highlights:
            wh["highlights"] = highlights
        work_history.append(wh)

    education = []
    for entry in (parsed.get("education") or []):
        if not isinstance(entry, dict):
            continue
        # Accept both "school" and "institution" from the LLM output.
        school = _safe_str(entry.get("school") or entry.get("institution")) or ""
        edu: Dict[str, Any] = {
            "school": school,
            "degree": _safe_str(entry.get("degree")) or "",
        }
        if field := _safe_str(entry.get("field")):
            edu["field"] = field
        if from_ := _safe_str(entry.get("from")):
            edu["from"] = from_
        if to_ := _safe_str(entry.get("to")):
            edu["to"] = to_
        education.append(edu)

    profile: Dict[str, Any] = {
        "personal": personal,
    }

    if work_history:
        profile["work_history"] = work_history

    summary = _safe_list(parsed.get("summary"), 8)
    if summary:
        profile["summary"] = summary

    skills = _safe_list(parsed.get("skills"), 20)
    if skills:
        profile["skills"] = skills

    keywords_raw = _safe_list(parsed.get("keywords"), 20)
    profile["keywords"] = [k.lower() for k in keywords_raw] if keywords_raw else []

    target_roles = _safe_list(parsed.get("target_roles"), 10)
    if target_roles:
        profile["target_roles"] = [r.lower() for r in target_roles]

    # Preserve seniority from existing or use defaults
    profile["seniority"] = base.get("seniority") or {
        "preferred": ["senior", "staff", "principal"],
        "acceptable": ["mid", "lead"],
    }

    languages = _safe_list(parsed.get("languages"), 10)
    if languages:
        profile["languages"] = [lang.lower() for lang in languages]

    if education:
        profile["education"] = education

    patents = _safe_list(parsed.get("patents"), 10)
    if patents:
        profile["patents"] = patents

    certifications = _safe_list(parsed.get("certifications"), 10)
    if certifications:
        profile["certifications"] = certifications

    # Preserve sections that aren't extracted from resumes
    for section in ("target_companies", "blacklisted_companies", "preferences",
                    "work_authorization", "resumes", "credentials"):
        if section in base:
            profile[section] = base[section]

    # Store the resume hash so re-uploads of the same file are detected
    if resume_hash:
        profile["_resume_hash"] = resume_hash

    return yaml.dump(profile, allow_unicode=True, sort_keys=False, default_flow_style=False)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def parse_resume(pdf_path: str, model: str | None = None) -> Dict[str, Any]:
    """Extract PDF text, call Ollama, return raw parsed dict."""
    text = extract_text_from_pdf(pdf_path)
    return _call_ollama(text, model=model)


def parse_resume_to_yaml(
    pdf_path: str,
    existing_profile: Dict[str, Any] | None = None,
    model: str | None = None,
) -> tuple[str, bool]:
    """Full pipeline: PDF → LLM → YAML string.

    Returns (yaml_str, was_reparsed).
    If the PDF hash matches ``_resume_hash`` in ``existing_profile``, the
    existing profile is returned unchanged (was_reparsed=False) to preserve
    any hand-edits made after the initial parse.
    """
    file_hash = pdf_sha256(pdf_path)
    existing = existing_profile or {}

    if existing.get("_resume_hash") == file_hash:
        # Same file — return existing profile untouched
        return yaml.dump(existing, allow_unicode=True, sort_keys=False,
                         default_flow_style=False), False

    parsed = parse_resume(pdf_path, model=model)
    result_yaml = build_profile_yaml(parsed, existing=existing, resume_hash=file_hash)
    return result_yaml, True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Parse a PDF resume into profile.yaml format")
    ap.add_argument("pdf", help="Path to the PDF resume")
    ap.add_argument("output", nargs="?", default="-",
                    help="Output YAML file (default: stdout)")
    ap.add_argument("--model", default=None, help="Ollama model override")
    ap.add_argument("--merge", default=None,
                    help="Merge into existing profile.yaml (path)")
    args = ap.parse_args()

    existing = None
    if args.merge:
        with open(args.merge, encoding="utf-8") as fh:
            existing = yaml.safe_load(fh) or {}

    result_yaml, was_reparsed = parse_resume_to_yaml(args.pdf, existing_profile=existing, model=args.model)

    if not was_reparsed:
        print("Resume unchanged — profile already up to date (hash match). Use --merge without an existing hash to force re-parse.", file=sys.stderr)
        sys.exit(0)

    if args.output == "-":
        sys.stdout.write(result_yaml)
    else:
        Path(args.output).write_text(result_yaml, encoding="utf-8")
        print(f"Written to {args.output}", file=sys.stderr)
