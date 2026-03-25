# Career Copilot - System Architecture

For a high-level overview see `README.md`.

This document describes the internal architecture and technical design of Career Copilot.

---

## Overview

Career Copilot is a modular job discovery and application assistant that automates repetitive job search tasks while keeping a human in control of all final decisions.

The system performs four main tasks:

1. Discover remote job listings from multiple sources
2. Evaluate opportunities against a candidate profile
3. Assist with application form prefilling
4. Require human approval before submission

---

## System Pipeline

Job Sources
-> Ingestion Pipeline
-> Normalization & Deduplication
-> SQLite Database
-> Career Intelligence Engine
-> LLM Job Analysis
-> Application Prefill Agent
-> Human Approval Gate
-> Application Submission

Current implemented CLI orchestration:

```text
python run_pipeline.py full-run
        |
        v
[FETCH]
  -> pull jobs from source
  -> normalize
  -> dedupe
  -> insert new jobs

        |
        v
[EVALUATE]
  -> compute remote eligibility
  -> compute rule-based fit_score
  -> assign rule_status
  -> initialize or preserve final status
  -> select recommended resume

        |
        v
[ANALYZE]
  -> send selected jobs to Ollama
  -> structured JSON reasoning
  -> conservative promotion/demotion
  -> persist LLM fields
```

Result in `jobs`:

```text
job metadata
rule_status + rule-based scoring
recommended resume
LLM reasoning + confidence
final job status
```

State model:

- Deterministic layer:
  - `rule_status`
  - `fit_score`
- Semantic layer:
  - `llm_fit_score`
  - `recommendation`
  - `llm_confidence`
  - `llm_status`
- Final decision layer:
  - `status`

---

## Core Components

### Job Sources

Implemented sources:

- Remotive API

Planned sources (stubs exist, not yet tested):

- RemoteOK JSON endpoint
- Greenhouse company boards

### Ingestion Pipeline

Responsible for:

- fetching job listings
- rate limiting
- retry logic
- pipeline run tracking

### Normalization & Deduplication

All job sources are converted into a unified internal schema.

Duplicates are detected using:

- URL matching
- Hash of `(company + title + location)`

### Database Layer

Stack:

- SQLite
- SQLAlchemy
- Alembic

Tables:

- `jobs`
- `application_history`
- `pipeline_runs`

The `jobs` table now intentionally separates deterministic and final decision state so reevaluation does not overwrite LLM-refined outcomes.

### Career Intelligence Engine

Evaluates job relevance using:

- remote eligibility filtering
- rule-based scoring
- application history checks
- resume selection

The `evaluate` step persists deterministic fields back to the `jobs` table:

- `fit_score`
- `rule_status`
- `remote_eligibility`
- `recommended_resume`

Evaluation policy:

- newly fetched jobs start as `status='new'`
- `evaluate` always refreshes `rule_status`
- `evaluate` only initializes or updates final `status` when the job has not already been refined by a successful LLM analysis
- this prevents `evaluate --all-jobs` from erasing previous LLM promotions or rejections

### LLM Job Analysis

Uses local LLM models through **Ollama** for deeper evaluation:

- fit scoring
- strengths and skill gaps
- apply/skip recommendation

The current implementation uses the Ollama `/api/chat` endpoint with structured JSON output and conservative status updates:

- only selected jobs are analyzed, defaulting to `status='review'`
- malformed or failed LLM responses do not break the pipeline
- LLM output is persisted into dedicated fields such as:
  - `llm_fit_score`
  - `llm_strengths`
  - `fit_explanation`
  - `skill_gaps`
  - `recommendation`
  - `llm_confidence`
  - `llm_status`
- LLM analysis updates the final operational `status`, but does not overwrite `rule_status`

### Human Review Commands

The current CLI includes human-facing review shortcuts for inspecting the queue without querying SQLite directly:

- `python run_pipeline.py shortlist`
- `python run_pipeline.py review`
- `python run_pipeline.py rejected`

These commands present the current final `status`, rule-based score, LLM recommendation, LLM confidence, and recommended resume in a review-friendly format.

### Application Prefill Agent

> **Not yet implemented.** A proof-of-concept (`playground_playwright.py`) exists that opens a shortlisted job URL in a Chromium browser via Playwright. Full ATS-specific form detection and field prefilling has not been built yet.

Planned support:

- Greenhouse
- Lever

Unsupported platforms will fall back to manual mode.

### Human Approval Gate

> **Not yet implemented.** Human review is currently done via the CLI review commands (`shortlist`, `review`, `rejected`), which display job details without triggering any submission.

Planned behaviour once implemented:

- approve
- edit
- skip

### Application Submission

> **Not yet implemented.**

Planned behaviour once implemented:

- the application is submitted
- the result is logged
- `application_history` is updated
