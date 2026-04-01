"""
Tool definitions for the Career Copilot interactive assistant (ask command).

Each tool function accepts a SQLAlchemy session as its first argument plus
any parameters declared in TOOL_SCHEMAS.  dispatch_tool() is the single
call-site used by the tool-calling loop in run_pipeline.py.

Pass 1 — read-only DB tools (no confirmation required):
  count_jobs_by_status, get_jobs_by_status, get_top_jobs,
  search_jobs, get_job_detail, get_job_description, get_pipeline_stats

Pass 2 — local action tools (confirmation required): see future commits
Pass 3 — external action tools (email): see future commits
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import Job, PipelineRun

# Valid status values shared across schemas and validation
_STATUSES = ["new", "review", "shortlisted", "rejected", "applied", "deferred"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _job_summary(job: Job) -> dict:
    return {
        "id": job.id,
        "company": job.company,
        "title": job.title,
        "status": job.status,
        "fit_score": job.fit_score,
        "llm_fit_score": job.llm_fit_score,
        "recommendation": job.recommendation,
        "source": job.source,
    }


def _score_order(query):
    """Order by fit_score descending, nulls last."""
    return query.order_by(func.coalesce(Job.fit_score, 0).desc())


# ---------------------------------------------------------------------------
# Pass 1 — read-only tools
# ---------------------------------------------------------------------------

def count_jobs_by_status(session: Session, status: str) -> dict:
    """Count jobs in a given status bucket."""
    try:
        count = session.query(func.count(Job.id)).filter(Job.status == status).scalar()
        return {"status": status, "count": count}
    except Exception as e:
        return {"error": str(e)}


def get_jobs_by_status(session: Session, status: str, limit: int = 20) -> dict:
    """Return jobs filtered by status, ordered by fit score."""
    try:
        total = session.query(func.count(Job.id)).filter(Job.status == status).scalar()
        jobs = (
            _score_order(session.query(Job).filter(Job.status == status))
            .limit(limit)
            .all()
        )
        return {
            "status": status,
            "total_in_db": total,
            "returned": len(jobs),
            "jobs": [_job_summary(j) for j in jobs],
        }
    except Exception as e:
        return {"error": str(e)}


def get_top_jobs(session: Session, limit: int = 5, status: str = None) -> dict:
    """Return the top N jobs by fit score, optionally filtered by status."""
    try:
        q = session.query(Job)
        if status:
            q = q.filter(Job.status == status)
        else:
            q = q.filter(Job.status.in_(["shortlisted", "review"]))
        total = q.with_entities(func.count(Job.id)).scalar()
        jobs = _score_order(q).limit(limit).all()
        return {"total_matching": total, "returned": len(jobs), "jobs": [_job_summary(j) for j in jobs]}
    except Exception as e:
        return {"error": str(e)}


def search_jobs(session: Session, query: str) -> dict:
    """Search jobs by keyword against title or company name."""
    try:
        like = f"%{query}%"
        q = session.query(Job).filter(Job.title.ilike(like) | Job.company.ilike(like))
        total = q.with_entities(func.count(Job.id)).scalar()
        jobs = _score_order(q).limit(20).all()
        return {"query": query, "total_matching": total, "returned": len(jobs), "jobs": [_job_summary(j) for j in jobs]}
    except Exception as e:
        return {"error": str(e)}


def get_job_detail(session: Session, job_id: int) -> dict:
    """Return key fields for a single job (no description text)."""
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            return {"error": f"Job {job_id} not found"}
        return {
            "id": job.id,
            "company": job.company,
            "title": job.title,
            "source": job.source,
            "status": job.status,
            "rule_status": job.rule_status,
            "fit_score": job.fit_score,
            "llm_fit_score": job.llm_fit_score,
            "recommendation": job.recommendation,
            "llm_confidence": job.llm_confidence,
            "remote_eligibility": job.remote_eligibility,
            "location": job.location,
            "url": job.url,
            "posted_date": str(job.posted_date) if job.posted_date else None,
            "recommended_resume": job.recommended_resume,
        }
    except Exception as e:
        return {"error": str(e)}


def get_job_description(session: Session, job_id: int) -> dict:
    """Return the full description text for a job (truncated at 3000 chars)."""
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            return {"error": f"Job {job_id} not found"}
        desc = job.description_text or job.description or ""
        truncated = len(desc) > 3000
        return {
            "id": job.id,
            "company": job.company,
            "title": job.title,
            "description": desc[:3000] + ("  [truncated]" if truncated else ""),
        }
    except Exception as e:
        return {"error": str(e)}


def get_schedule(session: Session) -> dict:
    """Query Windows Task Scheduler for CareerCopilot scheduled tasks."""
    # LastTaskResult codes: 0 = success, 1 = incorrect function, 267011 = task has not run yet
    _RESULT_LABELS = {0: "success", 267011: "never run", 1: "failed"}

    try:
        ps = (
            "Get-ScheduledTask | Where-Object { $_.TaskName -like 'CareerCopilot*' } | "
            "ForEach-Object { "
            "  $task = $_; "
            "  $info = $task | Get-ScheduledTaskInfo; "
            "  $triggers = $task.Triggers | ForEach-Object { $_.StartBoundary }; "
            "  [PSCustomObject]@{ "
            "    task_name            = $task.TaskName; "
            "    task_state           = $task.State.ToString(); "
            "    configured_times     = ($triggers | Sort-Object | ForEach-Object { if ($_) { ([datetime]$_).ToString('HH:mm') } }); "
            "    next_scheduled_run   = if ($info.NextRunTime.Year -gt 2000) { $info.NextRunTime.ToString('yyyy-MM-dd HH:mm') } else { $null }; "
            "    last_attempted_run   = if ($info.LastRunTime.Year -gt 2000) { $info.LastRunTime.ToString('yyyy-MM-dd HH:mm') } else { $null }; "
            "    last_run_result_code = $info.LastTaskResult; "
            "    last_run_result      = if ($info.LastTaskResult -eq 0) { 'success' } elseif ($info.LastTaskResult -eq 267011) { 'never run' } else { 'failed (code ' + $info.LastTaskResult + ')' } "
            "  } "
            "} | ConvertTo-Json -Compress"
        )
        result = subprocess.run(
            ["powershell", "-Command", ps],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0 or not result.stdout.strip():
            return {"error": "No CareerCopilot scheduled tasks found", "detail": result.stderr.strip()}
        raw = json.loads(result.stdout)
        tasks = raw if isinstance(raw, list) else [raw]
        return {"scheduled_tasks": tasks}
    except Exception as e:
        return {"error": str(e)}


def get_pipeline_stats(session: Session) -> dict:
    """Return overall job counts by status and recent pipeline run history."""
    try:
        counts = dict(
            session.query(Job.status, func.count(Job.id)).group_by(Job.status).all()
        )
        runs = (
            session.query(PipelineRun)
            .order_by(PipelineRun.started_at.desc())
            .limit(5)
            .all()
        )
        return {
            "total_jobs": sum(counts.values()),
            "by_status": counts,
            "recent_pipeline_runs": [
                {
                    "source": r.source,
                    "started_at": str(r.started_at),
                    "status": r.status,
                    "jobs_fetched": r.jobs_fetched,
                    "jobs_new": r.jobs_new,
                    "error": r.error_message,
                }
                for r in runs
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def get_recent_runs(session: Session, limit: int = 10) -> dict:
    """Return recent pipeline runs with timing, outcome, and new job counts."""
    try:
        runs = (
            session.query(PipelineRun)
            .order_by(PipelineRun.started_at.desc())
            .limit(limit)
            .all()
        )
        total_runs = session.query(func.count(PipelineRun.id)).scalar()
        records = []
        for r in runs:
            duration_s = None
            if r.started_at and r.completed_at:
                duration_s = int((r.completed_at - r.started_at).total_seconds())
            records.append({
                "id": r.id,
                "source": r.source,
                "started_at": str(r.started_at),
                "completed_at": str(r.completed_at) if r.completed_at else None,
                "duration_seconds": duration_s,
                "outcome": r.status,
                "jobs_fetched": r.jobs_fetched,
                "jobs_new": r.jobs_new,
                "jobs_duplicates": r.jobs_duplicates,
                "error": r.error_message,
            })
        return {"total_runs_ever": total_runs, "returned": len(records), "runs": records}
    except Exception as e:
        return {"error": str(e)}


def get_jobs_needing_review(session: Session, limit: int = 20) -> dict:
    """Return jobs currently in review status, ordered by fit score — the triage queue."""
    try:
        total = session.query(func.count(Job.id)).filter(Job.status == "review").scalar()
        jobs = (
            _score_order(session.query(Job).filter(Job.status == "review"))
            .limit(limit)
            .all()
        )
        return {
            "total_awaiting_review": total,
            "returned": len(jobs),
            "jobs": [_job_summary(j) for j in jobs],
        }
    except Exception as e:
        return {"error": str(e)}


def get_top_shortlisted_jobs(session: Session, limit: int = 10) -> dict:
    """Return shortlisted jobs ordered by fit score — the apply queue."""
    try:
        total = session.query(func.count(Job.id)).filter(Job.status == "shortlisted").scalar()
        jobs = (
            _score_order(session.query(Job).filter(Job.status == "shortlisted"))
            .limit(limit)
            .all()
        )
        enriched = []
        for j in jobs:
            entry = _job_summary(j)
            entry["llm_confidence"] = j.llm_confidence
            entry["recommended_resume"] = j.recommended_resume
            entry["url"] = j.url
            enriched.append(entry)
        return {
            "total_shortlisted": total,
            "returned": len(enriched),
            "jobs": enriched,
        }
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Pass 2 — action tools (confirmation required before execution)
# ---------------------------------------------------------------------------

ACTION_TOOLS = {"open_job", "mark_job_status", "run_full_pipeline"}

# Valid status transitions — terminal states have an empty set
_VALID_TRANSITIONS: dict[str, set] = {
    "new":        {"review", "shortlisted", "rejected", "deferred"},
    "review":     {"shortlisted", "rejected", "deferred"},
    "shortlisted":{"applied", "rejected", "deferred"},
    "rejected":   {"review"},
    "deferred":   {"review", "rejected"},
    "applied":    set(),  # terminal
}


def tool_policy_check(name: str, args: dict, session: Session) -> dict:
    """Pre-confirmation policy gate. Returns {"ok": True} or {"error": "..."}."""
    if name == "open_job":
        job = session.query(Job).filter(Job.id == args.get("job_id")).first()
        if not job:
            return {"error": f"Job {args.get('job_id')} not found"}
        if not job.url:
            return {"error": f"Job {args.get('job_id')} has no URL"}
        return {"ok": True}

    if name == "mark_job_status":
        job_id, new_status = args.get("job_id"), args.get("status")
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            return {"error": f"Job {job_id} not found"}
        allowed = _VALID_TRANSITIONS.get(job.status, set())
        if new_status not in allowed:
            if not allowed:
                return {"error": f"Job {job_id} is '{job.status}' — terminal state, no further transitions allowed"}
            return {"error": f"Cannot move job {job_id} from '{job.status}' to '{new_status}'. Allowed: {sorted(allowed)}"}
        return {"ok": True}

    if name == "run_full_pipeline":
        cutoff = datetime.utcnow() - timedelta(minutes=10)
        active = (
            session.query(PipelineRun)
            .filter(PipelineRun.started_at >= cutoff, PipelineRun.completed_at.is_(None))
            .first()
        )
        if active:
            return {"error": f"Pipeline may already be running (started {active.started_at}). Check with 'get_recent_runs'."}
        return {"ok": True}

    return {"ok": True}


def confirmation_prompt(name: str, args: dict, session: Session) -> str:
    """Return a human-readable confirmation string for an action tool."""
    if name == "open_job":
        job = session.query(Job).filter(Job.id == args.get("job_id")).first()
        if job:
            return f"Open job {job.id} ({job.title} @ {job.company}) in the browser?"
        return f"Open job {args.get('job_id')} in the browser?"

    if name == "mark_job_status":
        job_id, new_status = args.get("job_id"), args.get("status")
        job = session.query(Job).filter(Job.id == job_id).first()
        if job:
            return f"Mark job {job_id} ({job.title} @ {job.company}) as '{new_status}'? (currently: '{job.status}')"
        return f"Mark job {job_id} as '{new_status}'?"

    if name == "run_full_pipeline":
        src = args.get("source")
        suffix = f" (source: {src})" if src else ""
        return f"Run the full pipeline{suffix}? This may take several minutes."

    return f"Execute {name}?"


def open_job(session: Session, job_id: int) -> dict:
    """Open a job in the browser using the existing open-job command (with Playwright prefill)."""
    job = session.query(Job).filter(Job.id == job_id).first()
    if not job:
        return {"error": f"Job {job_id} not found"}
    try:
        # Delegate to the CLI command — it handles Playwright, bot-detection, applied marking.
        # subprocess.run blocks until the user closes the browser session, which is correct.
        subprocess.run(
            [sys.executable, "run_pipeline.py", "open-job", "--job-id", str(job_id)],
            check=False,
        )
        return {"success": True, "message": f"Browser session for job {job_id} ({job.title} @ {job.company}) completed."}
    except Exception as e:
        return {"error": str(e)}


def mark_job_status(session: Session, job_id: int, status: str) -> dict:
    """Update a job's lifecycle status."""
    job = session.query(Job).filter(Job.id == job_id).first()
    if not job:
        return {"error": f"Job {job_id} not found"}
    old_status = job.status
    job.status = status
    try:
        session.commit()
        return {"success": True, "message": f"Job {job_id} ({job.title} @ {job.company}): '{old_status}' → '{status}'"}
    except Exception as e:
        session.rollback()
        return {"error": str(e)}


def run_full_pipeline(session: Session, source: str = None) -> dict:
    """Launch the full pipeline as a background process (non-blocking)."""
    cmd = [sys.executable, "run_pipeline.py", "full-run"]
    if source:
        cmd += ["--source", source]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return {
            "success": True,
            "pid": proc.pid,
            "message": f"Pipeline started (PID {proc.pid}). Check progress with 'get_recent_runs' or 'get_pipeline_stats'.",
        }
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Registry and dispatch
# ---------------------------------------------------------------------------

TOOL_REGISTRY: dict = {
    # Pass 1 — read-only
    "count_jobs_by_status": count_jobs_by_status,
    "get_jobs_by_status": get_jobs_by_status,
    "get_top_jobs": get_top_jobs,
    "search_jobs": search_jobs,
    "get_job_detail": get_job_detail,
    "get_job_description": get_job_description,
    "get_pipeline_stats": get_pipeline_stats,
    "get_schedule": get_schedule,
    "get_recent_runs": get_recent_runs,
    "get_jobs_needing_review": get_jobs_needing_review,
    "get_top_shortlisted_jobs": get_top_shortlisted_jobs,
    # Pass 2 — actions (confirmation required)
    "open_job": open_job,
    "mark_job_status": mark_job_status,
    "run_full_pipeline": run_full_pipeline,
}


def dispatch_tool(name: str, args: dict, session: Session) -> dict:
    """Look up and call a tool by name, injecting the session."""
    fn = TOOL_REGISTRY.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    try:
        return fn(session=session, **args)
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Ollama-compatible tool schemas
# ---------------------------------------------------------------------------

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "count_jobs_by_status",
            "description": "Count how many jobs are in a given status bucket.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": _STATUSES},
                },
                "required": ["status"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_jobs_by_status",
            "description": "Return jobs from the database filtered by status, ordered by fit score descending.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": _STATUSES},
                    "limit": {"type": "integer", "description": "Max jobs to return (default 20)"},
                },
                "required": ["status"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_jobs",
            "description": "Return the top N jobs by fit score. Defaults to shortlisted and review jobs if no status given.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Number of jobs to return"},
                    "status": {"type": "string", "enum": _STATUSES, "description": "Optional status filter"},
                },
                "required": ["limit"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_jobs",
            "description": "Search jobs by keyword against job title or company name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_job_detail",
            "description": (
                "Fetch key details for a single job by its integer ID. "
                "Does not include description text — use get_job_description for that."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer"},
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_job_description",
            "description": "Fetch the full description text for a job. Only call this when the user explicitly asks to read the description.",
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer"},
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline_stats",
            "description": "Return overall job counts by status and recent pipeline run history. Use this for questions like 'what happened in the last run' or 'how many jobs were fetched today'.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schedule",
            "description": "Query Windows Task Scheduler to find out when Career Copilot is scheduled to run automatically, including next and last run times.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_runs",
            "description": "Return recent pipeline run history with start time, duration, outcome, and new job counts. Use this for questions like 'what happened recently', 'did the last run succeed', or 'how many new jobs were found today'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Number of recent runs to return (default 10)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_jobs_needing_review",
            "description": "Return jobs currently in review status ordered by fit score — the triage queue. Use this for questions like 'what should I look at next' or 'what jobs are waiting for review'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max jobs to return (default 20)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_shortlisted_jobs",
            "description": "Return shortlisted jobs ordered by fit score — the apply queue. Includes URL and recommended resume. Use this for questions like 'what should I apply to next' or 'show me my shortlisted jobs'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Number of jobs to return (default 10)"},
                },
                "required": [],
            },
        },
    },
    # ---- Pass 2 — action tools ------------------------------------------------
    {
        "type": "function",
        "function": {
            "name": "open_job",
            "description": (
                "Open a job posting in the browser and trigger Playwright form prefill. "
                "Use this when the user says 'open job N', 'apply to job N', or 'show me job N'. "
                "Requires confirmation before executing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer", "description": "The job ID to open."},
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "mark_job_status",
            "description": (
                "Update the lifecycle status of a job. "
                "Use this when the user says 'mark job N as applied', 'reject job N', 'shortlist job N', etc. "
                "Valid transitions: review→shortlisted/rejected/deferred, shortlisted→applied/rejected/deferred, "
                "rejected→review (undo), applied is terminal. "
                "Requires confirmation before executing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer"},
                    "status": {"type": "string", "enum": _STATUSES},
                },
                "required": ["job_id", "status"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_full_pipeline",
            "description": (
                "Launch the full job pipeline (fetch + evaluate + LLM analyze) as a background process. "
                "Use this when the user says 'run the pipeline', 'refresh jobs', 'fetch new jobs', etc. "
                "Requires confirmation before executing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Optional: limit to a specific source (e.g. 'ashby', 'remotive')."},
                },
                "required": [],
            },
        },
    },
]
