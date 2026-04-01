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
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-ScheduledTask | Where-Object { $_.TaskName -like 'CareerCopilot*' } | "
             "ForEach-Object { $info = $_ | Get-ScheduledTaskInfo; "
             "[PSCustomObject]@{ "
             "  Name = $_.TaskName; "
             "  State = $_.State.ToString(); "
             "  NextRun = $info.NextRunTime.ToString('yyyy-MM-dd HH:mm'); "
             "  LastRun = $info.LastRunTime.ToString('yyyy-MM-dd HH:mm'); "
             "  LastResult = $info.LastTaskResult "
             "} } | ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0 or not result.stdout.strip():
            return {"error": "No CareerCopilot scheduled tasks found", "detail": result.stderr.strip()}
        raw = json.loads(result.stdout)
        # PowerShell returns a dict (not list) when there's only one task
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


# ---------------------------------------------------------------------------
# Registry and dispatch
# ---------------------------------------------------------------------------

TOOL_REGISTRY: dict = {
    "count_jobs_by_status": count_jobs_by_status,
    "get_jobs_by_status": get_jobs_by_status,
    "get_top_jobs": get_top_jobs,
    "search_jobs": search_jobs,
    "get_job_detail": get_job_detail,
    "get_job_description": get_job_description,
    "get_pipeline_stats": get_pipeline_stats,
    "get_schedule": get_schedule,
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
]
