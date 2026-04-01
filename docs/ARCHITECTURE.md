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

```text
python run_pipeline.py full-run
        |
        v
[FETCH]
  -> pull jobs from all enabled sources
  -> normalize to unified schema
  -> deduplicate (URL + company+title+location hash)
  -> insert new jobs into SQLite

        |
        v
[EVALUATE]
  -> compute remote_eligibility (rule-based classifier)
  -> compute rule-based fit_score
  -> assign rule_status (shortlisted / review / rejected)
  -> initialize or preserve final status
  -> select recommended resume

        |
        v
[ANALYZE]
  -> send review-status jobs to local Ollama LLM
  -> structured JSON reasoning (fit score, strengths, gaps)
  -> conservative promotion/demotion
  -> persist LLM fields
```

Result persisted per job:

```text
job metadata
rule_status + fit_score
recommended_resume
LLM reasoning + confidence
final status
```

---

## State Model

Each job passes through three layers:

| Layer | Fields | Notes |
|---|---|---|
| Deterministic | `rule_status`, `fit_score`, `remote_eligibility`, `matched_skills` | Always refreshed on re-evaluate |
| Semantic (LLM) | `llm_fit_score`, `recommendation`, `llm_confidence`, `llm_status`, `fit_explanation`, `llm_strengths`, `skill_gaps` | Set by Ollama; preserved across re-evaluate |
| Decision | `status` | Initialized from rule layer; updated by LLM; manually overridable |

Evaluation policy: `evaluate` always refreshes `rule_status` but only touches final `status` when the job has not already been refined by a successful LLM analysis. This prevents `evaluate --all-jobs` from erasing prior LLM promotions or manual decisions.

---

## Core Components

### Job Sources

Sources are implemented as `BaseConnector` subclasses in `connectors/`.

**Aggregate job boards (JSON/RSS):**

| Connector | Source | Notes |
|---|---|---|
| `RemotiveConnector` | [Remotive](https://remotive.com) | General remote tech jobs |
| `RemoteOKConnector` | [RemoteOK](https://remoteok.com) | Only jobs with extractable ATS links (avoids subscription wall) |
| `WeWorkRemotelyConnector` | [WeWorkRemotely](https://weworkremotely.com) | Curated remote tech jobs |
| `ArbeitnowConnector` | [Arbeitnow](https://www.arbeitnow.com) | EU-focused remote jobs |
| `JobicyConnector` | [Jobicy](https://jobicy.com) | Remote tech jobs |
| `JobspressoConnector` | [Jobspresso](https://jobspresso.co) | Curated remote jobs |
| `DynamiteJobsConnector` | [Dynamite Jobs](https://dynamitejobs.com) | Remote-first jobs |
| `GetOnBoardConnector` | [GetOnBoard](https://www.getonbrd.com) | LatAm-focused, fully remote only |
| `HimalayasConnector` | [Himalayas](https://himalayas.app) | Worldwide-only remote jobs |
| `AdzunaConnector` | [Adzuna](https://www.adzuna.com) | 8 countries (gb/de/fr/nl/at/be/au/ca), remote-filtered |

**Direct ATS connectors:**

| Connector | Source | Notes |
|---|---|---|
| `DirectATSConnector` | Ashby / Greenhouse / Lever / Workable | Curated `target_companies` list from `profile.yaml`; ATS auto-detected from `careers_url` host |
| `AshbyConnector` | Ashby API | DB-discovered Ashby boards not in the Direct ATS list |
| `GreenhouseConnector` | Greenhouse API | DB-discovered Greenhouse boards not in the Direct ATS list |
| `LeverConnector` | Lever API | DB-discovered Lever boards not in the Direct ATS list |

**Direct ATS host routing:**

```
jobs.ashbyhq.com          → Ashby  (GET /posting-api/job-board/{slug})
boards.greenhouse.io       → Greenhouse  (GET /v1/boards/{slug}/jobs)
job-boards.greenhouse.io   → Greenhouse
jobs.lever.co              → Lever  (GET /v0/postings/{slug}?mode=json)
apply.workable.com         → Workable  (POST /api/v3/accounts/{slug}/jobs)
```

### Remote Eligibility Filter (`utils/remote_filter.py`)

`classify_remote_eligibility(job, profile)` returns `accept`, `review`, or `reject`.

Key rejection patterns (in order):

1. `raw_location` matches known US-only location strings (`usa`, `united states`, `us`)
2. Greenhouse-style prefixes: `us-remote`, `us-east`, `us-west`, etc.
3. US substrings in location (`united states`, ` usa`, `(u.s.)`, `(us)`, etc.) unless a broad-region override (`worldwide`, `emea`, etc.) is also present
4. `Remote - [Country]` pattern where the country is not in the user's `accepted_regions`
5. Description contains hard-reject keywords (`us only`, `must reside in the us`, `security clearance required`, etc.)
6. Geographic-only locations with no `remote`/`worldwide`/`global` hint and no accepted-region match

### Ingestion Pipeline

`run_pipeline.py` orchestrates:

- fetching from each connector
- normalizing via `connector.normalize(raw_job)`
- deduplication via `utils/dedup.py` (URL + content hash)
- upsert into `jobs` table

### Database Layer

- SQLite via SQLAlchemy
- Migrations via Alembic
- Tables: `jobs`, `application_history`, `pipeline_runs`

### Career Intelligence Engine (`utils/scoring.py`, `utils/application_filter.py`)

Evaluates job relevance:

- remote eligibility classification
- rule-based `fit_score` from skill overlap, title match, seniority
- blacklisted company filtering
- resume selection (`utils/resume_selector.py`) — matches job keywords against resume tags

### LLM Job Analysis (`utils/llm_analysis.py`)

Uses local LLM models through **Ollama** (`/api/chat`):

- structured JSON output with fit score, strengths, gaps, recommendation
- conservative status updates: only promotes review→shortlist or review→rejected
- malformed or failed responses do not break the pipeline
- LLM output stored in dedicated fields; never overwrites `rule_status`

### Application Prefill Agent (`utils/form_inspector.py`, `utils/form_filler.py`)

`open-job` opens a shortlisted job in a Playwright browser window and:

1. Detects ATS from the job URL (`utils/ats_detector.py`)
2. Scans visible form fields — captures `tag`, `type`, `name`, `id`, `placeholder`, `label` (via `<label for=...>`, `aria-label`, or `aria-labelledby`)
3. Builds DOM context for unlabeled fields by walking up the element tree
4. Fills text fields by matching labels against `_TEXT_RULES` (name, email, phone, LinkedIn, GitHub, location, etc.)
5. Handles checkboxes (skills, consent, availability) and radio groups (timezone, career type)
6. Uploads the recommended resume via native file dialog interception
7. Bot-protected sites (RemoteOK, WeWorkRemotely, Jobicy) open in the system browser without prefill

### Human Approval Gate

All submissions are manual. The pipeline:

- opens the form in a visible browser window
- prefills what it can
- waits for the user to review, edit, and submit
- prompts to mark the job as `applied` after submission

`application_history` is updated on mark-applied.
