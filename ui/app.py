"""
Career Copilot — local web UI (FastAPI).

Start via:  python run_pipeline.py ui
Or directly: uvicorn ui.app:app --port 7860
"""
import json
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

import config
from models.database import Job

# ---------------------------------------------------------------------------
# App + DB
# ---------------------------------------------------------------------------

app = FastAPI(title="Career Copilot UI")

_engine = create_engine(config.DATABASE_URL, connect_args={"check_same_thread": False})
_Session = sessionmaker(bind=_engine)

HTML_PATH = Path(__file__).parent / "index.html"


def _db():
    s = _Session()
    try:
        yield s
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Pipeline state (in-memory, single-user local tool)
# ---------------------------------------------------------------------------

_pipeline: Dict[str, Any] = {
    "status": "idle",   # idle | running | done | failed
    "started_at": None,
    "finished_at": None,
    "steps": {
        "fetch":    {"status": "pending", "detail": ""},
        "evaluate": {"status": "pending", "detail": ""},
        "analyze":  {"status": "pending", "detail": ""},
    },
    "log": [],
    "error": None,
}
_pipeline_lock = threading.Lock()


def _run_pipeline_subprocess():
    global _pipeline
    python = sys.executable
    cmd = [python, "run_pipeline.py", "full-run", "--email"]

    with _pipeline_lock:
        _pipeline["status"] = "running"
        _pipeline["started_at"] = datetime.utcnow().isoformat()
        _pipeline["finished_at"] = None
        _pipeline["error"] = None
        _pipeline["log"] = []
        _pipeline["steps"] = {
            "fetch":    {"status": "pending", "detail": ""},
            "evaluate": {"status": "pending", "detail": ""},
            "analyze":  {"status": "pending", "detail": ""},
        }

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        for line in proc.stdout:
            line = line.rstrip()
            with _pipeline_lock:
                _pipeline["log"].append(line)
                _update_steps_from_log(line)

        proc.wait()
        with _pipeline_lock:
            if proc.returncode == 0:
                _pipeline["status"] = "done"
                for step in _pipeline["steps"].values():
                    if step["status"] != "done":
                        step["status"] = "done"
            else:
                _pipeline["status"] = "failed"
                _pipeline["error"] = f"Exit code {proc.returncode}"
    except Exception as exc:
        with _pipeline_lock:
            _pipeline["status"] = "failed"
            _pipeline["error"] = str(exc)
    finally:
        with _pipeline_lock:
            _pipeline["finished_at"] = datetime.utcnow().isoformat()


def _update_steps_from_log(line: str):
    """Heuristically map log lines to step states (no lock needed — caller holds it)."""
    low = line.lower()
    steps = _pipeline["steps"]

    if "fetching jobs" in low or "fetch" in low and "source" in low:
        steps["fetch"]["status"] = "running"
    elif "successfully fetched" in low:
        steps["fetch"]["status"] = "done"
        steps["fetch"]["detail"] = line.split("INFO")[-1].strip() if "INFO" in line else line
        steps["evaluate"]["status"] = "running"
    elif "evaluating" in low or "scoring" in low or "evaluate" in low:
        steps["evaluate"]["status"] = "running"
    elif "successfully evaluated" in low or "evaluation complete" in low:
        steps["evaluate"]["status"] = "done"
        steps["analyze"]["status"] = "running"
    elif "analyzing" in low or "llm" in low and "job" in low:
        steps["analyze"]["status"] = "running"
    elif "full pipeline run complete" in low:
        steps["fetch"]["status"] = "done"
        steps["evaluate"]["status"] = "done"
        steps["analyze"]["status"] = "done"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AVATAR_COLORS = [
    "linear-gradient(135deg,#4f8ef7,#7fb3ff)",
    "linear-gradient(135deg,#10b981,#34d399)",
    "linear-gradient(135deg,#f59e0b,#fbbf24)",
    "linear-gradient(135deg,#8b5cf6,#a78bfa)",
    "linear-gradient(135deg,#ef4444,#f87171)",
    "linear-gradient(135deg,#ec4899,#f472b6)",
    "linear-gradient(135deg,#06b6d4,#67e8f9)",
    "linear-gradient(135deg,#f97316,#fb923c)",
    "linear-gradient(135deg,#6366f1,#818cf8)",
    "linear-gradient(135deg,#14b8a6,#2dd4bf)",
]


def _avatar_color(company: str) -> str:
    return _AVATAR_COLORS[hash(company or "") % len(_AVATAR_COLORS)]


def _avatar_text(company: str) -> str:
    words = (company or "?").split()
    if len(words) >= 2:
        return (words[0][0] + words[1][0]).upper()
    return (company or "?")[:2].upper()


def _parse_json_list(raw) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (json.JSONDecodeError, TypeError):
        return [s.strip() for s in str(raw).split(",") if s.strip()]


def _job_to_dict(job: Job) -> Dict[str, Any]:
    score = job.llm_fit_score or job.fit_score
    return {
        "id": job.id,
        "title": job.title or "",
        "company": job.company or "",
        "location": job.raw_location_text or job.location or "Remote",
        "source": job.source or "",
        "status": job.status or "new",
        "fit_score": score,
        "rule_score": job.fit_score,
        "llm_confidence": job.llm_confidence,
        "recommendation": job.recommendation,
        "strengths": _parse_json_list(job.llm_strengths),
        "gaps": _parse_json_list(job.skill_gaps),
        "reasoning": job.fit_explanation or "",
        "cover_letter": job.cover_letter or "",
        "description": job.description_text or job.description or "",
        "url": job.url or "",
        "posted_date": job.posted_date.isoformat() if job.posted_date else None,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "avatar_color": _avatar_color(job.company),
        "avatar_text": _avatar_text(job.company),
    }


# ---------------------------------------------------------------------------
# Routes — static
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    if not HTML_PATH.exists():
        raise HTTPException(500, "index.html not found")
    return HTML_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Routes — data
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def stats():
    session = _Session()
    try:
        rows = session.query(Job.status, func.count()).group_by(Job.status).all()
        counts = {status: n for status, n in rows}
        total = sum(counts.values())
        return {"counts": counts, "total": total}
    finally:
        session.close()


@app.get("/api/jobs")
async def list_jobs(status: str = "review", limit: int = 200):
    session = _Session()
    try:
        jobs = (
            session.query(Job)
            .filter(Job.status == status)
            .order_by(Job.fit_score.desc().nullslast(), Job.id.desc())
            .limit(limit)
            .all()
        )
        return {"jobs": [_job_to_dict(j) for j in jobs], "total": len(jobs)}
    finally:
        session.close()


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        return _job_to_dict(job)
    finally:
        session.close()


class StatusUpdate(BaseModel):
    status: str


@app.post("/api/jobs/{job_id}/status")
async def update_status(job_id: int, body: StatusUpdate):
    allowed = {"shortlisted", "rejected", "deferred", "review", "applied", "expired"}
    if body.status not in allowed:
        raise HTTPException(400, f"Invalid status: {body.status}")
    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        job.status = body.status
        session.commit()
        return {"ok": True, "id": job_id, "status": body.status}
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


@app.post("/api/jobs/{job_id}/cover-letter")
async def generate_cover(job_id: int):
    from utils.cover_letter import generate_cover_letter
    import yaml

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")

        try:
            with open("profile.yaml", encoding="utf-8") as f:
                profile = yaml.safe_load(f) or {}
        except Exception:
            profile = {}

        result = generate_cover_letter(_job_to_dict(job), profile)
        if result["status"] == "ok":
            job.cover_letter = result["cover_letter"]
            session.commit()
            return {"ok": True, "cover_letter": result["cover_letter"]}
        else:
            raise HTTPException(500, result.get("error", "Generation failed"))
    except HTTPException:
        raise
    except Exception as exc:
        session.rollback()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Routes — pipeline
# ---------------------------------------------------------------------------

@app.get("/api/pipeline/status")
async def pipeline_status():
    with _pipeline_lock:
        return dict(_pipeline)


@app.post("/api/pipeline/run")
async def pipeline_run():
    with _pipeline_lock:
        if _pipeline["status"] == "running":
            return {"ok": False, "message": "Pipeline already running"}

    thread = threading.Thread(target=_run_pipeline_subprocess, daemon=True)
    thread.start()
    return {"ok": True, "message": "Pipeline started"}
