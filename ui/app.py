"""
Career Copilot — local web UI (FastAPI).

Start via:  python run_pipeline.py ui
Or directly: uvicorn ui.app:app --port 7860
"""
import asyncio
import json
import re
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError:  # pragma: no cover — apscheduler optional at import time
    BackgroundScheduler = None  # type: ignore[assignment,misc]
    CronTrigger = None  # type: ignore[assignment]
    IntervalTrigger = None  # type: ignore[assignment]
from fastapi import FastAPI, HTTPException, UploadFile, File
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
# Schedule
# ---------------------------------------------------------------------------

_SCHEDULE_PATH = Path(__file__).parent.parent / "ui_schedule.json"
_DEFAULT_SCHEDULE: Dict[str, Any] = {"mode": "off", "interval_hours": 4, "times": []}
_sched_config: Dict[str, Any] = dict(_DEFAULT_SCHEDULE)
_scheduler = BackgroundScheduler(timezone="UTC") if BackgroundScheduler else None


def _load_sched_config() -> None:
    global _sched_config
    if _SCHEDULE_PATH.exists():
        try:
            _sched_config = json.loads(_SCHEDULE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass


def _save_sched_config() -> None:
    _SCHEDULE_PATH.write_text(json.dumps(_sched_config, indent=2), encoding="utf-8")


def _scheduled_run() -> None:
    with _pipeline_lock:
        if _pipeline["status"] == "running":
            return
    threading.Thread(target=_run_pipeline_subprocess, daemon=True).start()


def _apply_schedule() -> None:
    if not _scheduler:
        return
    _scheduler.remove_all_jobs()
    mode = _sched_config.get("mode", "off")
    if mode == "interval":
        hours = max(1, int(_sched_config.get("interval_hours", 4)))
        _scheduler.add_job(_scheduled_run, IntervalTrigger(hours=hours), id="pi")
    elif mode == "daily":
        for i, t in enumerate(_sched_config.get("times", [])):
            try:
                h, m = t.strip().split(":")
                _scheduler.add_job(_scheduled_run, CronTrigger(hour=int(h), minute=int(m)), id=f"pd_{i}")
            except Exception:
                pass


def _next_run_iso() -> str:
    if not _scheduler:
        return ""
    runs = [j.next_run_time for j in _scheduler.get_jobs() if j.next_run_time]
    return min(runs).isoformat() if runs else ""


def _read_task_scheduler() -> Dict[str, Any]:
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/fo", "CSV", "/v"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            if "run_pipeline" in line.lower():
                name = line.split('","')[0].strip('"').strip(",\"")
                return {"found": True, "name": name}
    except Exception:
        pass
    return {"found": False, "name": ""}


@app.on_event("startup")
def _on_startup() -> None:
    _load_sched_config()
    _apply_schedule()
    if _scheduler:
        _scheduler.start()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


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
    score = job.llm_fit_score if job.llm_fit_score is not None else job.fit_score
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
        # Close the Playwright browser when the user marks a job as applied,
        # so they can immediately open the next job without hitting the
        # "prefill already running" guard.
        if body.status in ("applied", "rejected"):
            with _prefill_lock:
                if _prefill["status"] == "running" and _prefill["job_id"] == job_id:
                    _prefill_cancel.set()
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


@app.get("/api/jobs/{job_id}/cover-letter/pdf")
async def download_cover_pdf(job_id: int):
    """Return the cover letter for job_id as a downloadable PDF."""
    import io
    import re as _re
    from fastapi.responses import StreamingResponse

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        text = (job.cover_letter or "").strip()
        if not text:
            raise HTTPException(404, "No cover letter for this job")

        company = (job.company or "company").strip()
        title = (job.title or "position").strip()

        try:
            from fpdf import FPDF
        except ImportError as exc:
            raise HTTPException(500, f"fpdf2 not installed: {exc}") from exc

        def _to_latin1(s: str) -> str:
            """Map common Unicode typographic chars to Latin-1 equivalents."""
            _MAP = str.maketrans({
                "\u2018": "'", "\u2019": "'",   # left/right single quotes
                "\u201c": '"', "\u201d": '"',   # left/right double quotes
                "\u2013": "-", "\u2014": "-",   # en/em dash
                "\u2026": "...",                 # ellipsis
                "\u00a0": " ",                   # non-breaking space
            })
            return s.translate(_MAP).encode("latin-1", errors="replace").decode("latin-1")

        pdf = FPDF()
        pdf.set_margins(25, 25, 25)
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 13)
        from fpdf.enums import XPos, YPos
        pdf.cell(0, 8, _to_latin1(f"{title} - {company}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(4)
        pdf.set_font("Helvetica", size=11)
        for para in text.split("\n\n"):
            para = para.strip()
            if not para:
                continue
            pdf.multi_cell(0, 6, _to_latin1(para))
            pdf.ln(3)

        buf = io.BytesIO(pdf.output())
        slug = _re.sub(r"[^\w-]", "_", company.lower())[:40]
        filename = f"cover_letter_{slug}.pdf"
        return StreamingResponse(
            buf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(exc))
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Prefill state
# ---------------------------------------------------------------------------

_prefill: Dict[str, Any] = {"status": "idle", "job_id": None, "result": None, "log": [], "started_at": None}
_prefill_lock = threading.Lock()
_prefill_cancel = threading.Event()


def _prefill_log(msg: str) -> None:
    """Append a timestamped message to the prefill log (thread-safe)."""
    entry = f"{datetime.utcnow().strftime('%H:%M:%S')} {msg}"
    with _prefill_lock:
        _prefill["log"].append(entry)
    print(entry, flush=True)  # also echo to terminal


def _cancel_existing_prefill() -> None:
    """Signal any running prefill session to stop and reset state.

    Does not wait for the browser thread to exit — the cancel event is
    enough for run_prefill_session to close the browser on its next await.
    """
    _prefill_cancel.set()
    with _prefill_lock:
        _prefill["status"] = "idle"
        _prefill["job_id"] = None
        _prefill["result"] = None
        _prefill["log"] = []


def _scrape_job_meta(url: str) -> Dict[str, Any]:
    """Fetch *url* and extract job title, company, and description.

    Tries in order:
    1. JSON-LD ``JobPosting`` schema (most reliable, used by Greenhouse,
       Lever, Ashby, and most modern ATS pages for SEO).
    2. OpenGraph ``og:title`` / ``og:site_name`` / ``og:description`` meta tags.
    3. ``<title>`` tag — parsed on common separators (`` | ``, `` - ``, `` · ``).

    Returns a dict with keys ``title``, ``company``, ``description``
    (all strings, any may be empty if detection failed).
    """
    import requests as _req
    try:
        resp = _req.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; career-copilot/1.0)"}, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return {}

    # --- 1. JSON-LD JobPosting ---
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(m.group(1).strip())
        except Exception:
            continue
        if data.get("@type") == "JobPosting":
            title = (data.get("title") or "").strip()
            company = ((data.get("hiringOrganization") or {}).get("name") or "").strip()
            description = (data.get("description") or "").strip()
            if title or company:
                return {"title": title, "company": company, "description": description}

    # --- 2. OpenGraph meta tags ---
    def _meta(prop: str) -> str:
        m = re.search(
            r'<meta[^>]+(?:property|name)=["\']' + re.escape(prop) + r'["\'][^>]+content=["\'](.*?)["\']',
            html, re.IGNORECASE,
        )
        if not m:
            # Also handle content= before property=
            m = re.search(
                r'<meta[^>]+content=["\'](.*?)["\'][^>]+(?:property|name)=["\']' + re.escape(prop) + r'["\']',
                html, re.IGNORECASE,
            )
        return m.group(1).strip() if m else ""

    og_title = _meta("og:title")
    og_site  = _meta("og:site_name")
    og_desc  = _meta("og:description")

    # --- 3. <title> tag fallback ---
    page_title_m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    page_title = re.sub(r'<[^>]+>', '', page_title_m.group(1)).strip() if page_title_m else ""

    inferred_title = og_title
    inferred_company = og_site

    if not inferred_title and page_title:
        for sep in (" | ", " - ", " · ", " — "):
            parts = [p.strip() for p in page_title.split(sep) if p.strip()]
            if len(parts) >= 2:
                inferred_title = parts[0]
                if not inferred_company:
                    inferred_company = parts[1]
                break
        if not inferred_title:
            inferred_title = page_title

    return {
        "title": inferred_title,
        "company": inferred_company,
        "description": og_desc,
    }


def _run_prefill_thread(job_dict: Dict[str, Any], profile: Dict[str, Any]) -> None:
    with _prefill_lock:
        _prefill["log"] = []
        _prefill["started_at"] = datetime.utcnow().isoformat()
        _prefill["status"] = "running"  # "starting" → "running" so the UI poll keeps going

    _prefill_log(f"Starting prefill for: {job_dict.get('title', '')} @ {job_dict.get('company', '')}")

    # For direct URL fills, scrape the page to enrich missing title / company /
    # description before the cover letter is generated.
    if job_dict.get("source") == "direct" and (
        not job_dict.get("company")
        or not job_dict.get("title")
        or job_dict.get("title") == "Direct Fill"
        or not job_dict.get("description_text")
    ):
        _prefill_log("Scraping job metadata from URL…")
        try:
            meta = _scrape_job_meta(job_dict["url"])
            if meta:
                job_dict = dict(job_dict)
                if meta.get("title") and (
                    not job_dict.get("title") or job_dict.get("title") == "Direct Fill"
                ):
                    job_dict["title"] = meta["title"]
                if meta.get("company") and not job_dict.get("company"):
                    job_dict["company"] = meta["company"]
                if meta.get("description") and not job_dict.get("description_text"):
                    job_dict["description_text"] = meta["description"]
                _prefill_log(
                    f"Metadata: title={job_dict.get('title')!r}  "
                    f"company={job_dict.get('company')!r}"
                )
        except Exception as exc:
            _prefill_log(f"Metadata scrape failed: {exc}")

    # Auto-generate cover letter if the job doesn't have one yet.
    if not job_dict.get("cover_letter"):
        _prefill_log("Generating cover letter via LLM…")
        try:
            from utils.cover_letter import generate_cover_letter
            cl_result = generate_cover_letter(job_dict, profile)
            if cl_result.get("status") == "ok":
                job_dict = dict(job_dict)
                job_dict["cover_letter"] = cl_result["cover_letter"]
                preview = cl_result["cover_letter"][:120].replace("\n", " ")
                _prefill_log(f"Cover letter generated: {preview}…")
                # Persist to DB so it's available in the UI too.
                session = _Session()
                try:
                    db_job = session.query(Job).filter(Job.id == job_dict["id"]).first()
                    if db_job:
                        db_job.cover_letter = cl_result["cover_letter"]
                        session.commit()
                except Exception:
                    pass
                finally:
                    session.close()
            else:
                _prefill_log("Cover letter generation failed — will skip upload.")
        except Exception as exc:
            _prefill_log(f"Cover letter error: {exc}")
    else:
        cl_text = job_dict.get("cover_letter", "")
        preview = cl_text[:120].replace("\n", " ")
        _prefill_log(f"Cover letter ready: {preview}…")

    # Save cover letter PDF now so the user always has a file to upload manually,
    # even if the automated upload fails or the ATS field isn't detected.
    if job_dict.get("cover_letter"):
        try:
            from utils.form_filler import _resolve_cover_letter_path
            _resolve_cover_letter_path({}, job_dict, log_fn=_prefill_log)
        except Exception as exc:
            _prefill_log(f"Cover letter PDF save error: {exc}")

    _prefill_log("Launching browser…")
    from utils.form_prefill import run_prefill_session

    try:
        result = asyncio.run(run_prefill_session(job_dict, profile, cancel_event=_prefill_cancel, log_fn=_prefill_log, timing=__import__("os").environ.get("CC_PROFILE") == "1"))
    except Exception as exc:
        result = {"status": "failed", "error": str(exc)}

    st = result.get("status", "")
    if st == "failed":
        _prefill_log(f"Prefill failed: {result.get('error', 'unknown error')}")
    elif st == "manual":
        _prefill_log(f"Manual: {result.get('reason', 'open in system browser')}")
    elif st == "cancelled":
        _prefill_log("Prefill stopped by user.")
    else:
        filled = result.get("filled", 0)
        skipped = result.get("skipped", 0)
        errors = result.get("errors", 0)
        uploads = result.get("uploads", 0)
        ats = result.get("ats", "unknown")
        _prefill_log(f"Session done ({ats}): {filled} filled, {uploads} uploaded, {skipped} skipped, {errors} errors.")

    with _prefill_lock:
        _prefill["status"] = "done"
        _prefill["result"] = result


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


# ---------------------------------------------------------------------------
# Routes — schedule
# ---------------------------------------------------------------------------

class ScheduleConfig(BaseModel):
    mode: str
    interval_hours: int = 4
    times: List[str] = []


@app.get("/api/schedule")
async def get_schedule():
    return {
        **_sched_config,
        "next_run": _next_run_iso(),
        "task_scheduler": _read_task_scheduler(),
    }


@app.post("/api/schedule")
async def set_schedule(body: ScheduleConfig):
    global _sched_config
    if body.mode not in {"off", "interval", "daily"}:
        raise HTTPException(400, f"Invalid mode: {body.mode}")
    _sched_config = {"mode": body.mode, "interval_hours": body.interval_hours, "times": body.times}
    _apply_schedule()
    _save_sched_config()
    return {"ok": True, "next_run": _next_run_iso(), **_sched_config}


# ---------------------------------------------------------------------------
# Routes — open / prefill
# ---------------------------------------------------------------------------

@app.post("/api/jobs/{job_id}/open")
async def open_job(job_id: int):
    from utils.form_prefill import is_system_browser_domain

    session = _Session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
        job_dict = _job_to_dict(job)
    finally:
        session.close()

    # Bot-protected domains: tell the UI to open in system browser directly.
    if is_system_browser_domain(job_dict.get("url", "")):
        return {"ok": True, "system_browser": True, "url": job_dict.get("url", "")}

    # Cancel any existing session before starting a new one.
    _cancel_existing_prefill()
    _prefill_cancel.clear()

    with _prefill_lock:
        _prefill["status"] = "starting"
        _prefill["job_id"] = job_id
        _prefill["result"] = None
        _prefill["log"] = []

    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}

    threading.Thread(target=_run_prefill_thread, args=(job_dict, profile), daemon=True).start()
    return {"ok": True, "system_browser": False, "message": "Browser opening…"}


@app.get("/api/prefill/status")
async def get_prefill_status():
    with _prefill_lock:
        return dict(_prefill)


@app.post("/api/prefill/stop")
async def stop_prefill():
    _prefill_cancel.set()
    # Check status without holding the lock when logging — _prefill_log itself
    # acquires _prefill_lock, so calling it inside another with _prefill_lock
    # would deadlock (threading.Lock is not reentrant).
    with _prefill_lock:
        was_running = _prefill["status"] == "running"
    if was_running:
        _prefill_log("Stop requested by user.")
    return {"ok": True}


class DirectFillRequest(BaseModel):
    url: str
    title: str = ""
    company: str = ""
    description: str = ""


@app.post("/api/prefill/url")
async def prefill_url(body: DirectFillRequest):
    """Open a browser directly on the provided application URL and fill the form.

    Use this when the real application form URL is known but is behind auth or
    bot-detection that prevents the normal pipeline from reaching it.
    The browser opens at the URL; if a login is still required the user can
    complete it manually before the form is auto-filled.
    """
    if not body.url or not body.url.startswith("http"):
        raise HTTPException(400, "A valid http(s) URL is required")

    # Cancel any existing session before starting a new one.
    _cancel_existing_prefill()
    _prefill_cancel.clear()

    with _prefill_lock:
        _prefill["status"] = "running"
        _prefill["job_id"] = None
        _prefill["result"] = None
        _prefill["log"] = []

    try:
        with open("profile.yaml", encoding="utf-8") as fh:
            profile = yaml.safe_load(fh) or {}
    except Exception:
        profile = {}

    # Build a synthetic job dict — no DB record needed.
    job_dict = {
        "id": None,
        "url": body.url,
        "title": body.title or "Direct Fill",
        "company": body.company or "",
        "description_text": body.description or "",
        "source": "direct",
        "status": "shortlisted",
        "cover_letter": None,
    }

    threading.Thread(target=_run_prefill_thread, args=(job_dict, profile), daemon=True).start()
    return {"ok": True, "message": "Browser opening…"}


# ---------------------------------------------------------------------------
# Resume → profile.yaml parser
# ---------------------------------------------------------------------------

@app.post("/api/profile/parse-resume")
async def parse_resume_endpoint(file: UploadFile = File(...)):
    """Accept a PDF resume upload and return a profile.yaml string.

    The returned YAML is merged into the existing profile.yaml on disk
    (structural sections like credentials, target_companies, etc. are preserved).
    The merged result is also written back to profile.yaml.
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted")

    import tempfile
    import os
    from utils.resume_parser import parse_resume_to_yaml

    # Save upload to a temp file
    suffix = ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Load existing profile to preserve structural sections
        existing: dict = {}
        try:
            with open("profile.yaml", encoding="utf-8") as fh:
                existing = yaml.safe_load(fh) or {}
        except Exception:
            pass

        result_yaml, was_reparsed = parse_resume_to_yaml(tmp_path, existing_profile=existing)

        if not was_reparsed:
            return {"ok": True, "unchanged": True,
                    "message": "Resume unchanged — profile.yaml not modified"}

        # Write merged result back to profile.yaml
        try:
            with open("profile.yaml", "w", encoding="utf-8") as fh:
                fh.write(result_yaml)
        except Exception as write_err:
            return {"ok": False, "error": f"Could not write profile.yaml: {write_err}",
                    "yaml": result_yaml}

        return {"ok": True, "unchanged": False, "yaml": result_yaml}
    except Exception as exc:
        raise HTTPException(500, str(exc))
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
