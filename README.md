# Career Copilot

An intelligent job discovery and application assistant for remote technical roles.

It fetches jobs from multiple sources, scores them against your profile, runs a local LLM analysis to surface the best matches, and assists with application form filling — while keeping **you in control of every submission**.

---

## Design Principles

**Privacy First** — All LLM processing runs locally via [Ollama](https://ollama.com). No data sent to external AI services.

**Human in the Loop** — No application is submitted automatically. Every submission requires your explicit approval.

**Assistive, not blind** — The pipeline reduces mechanical work (searching, filtering, form filling) while you make the final calls.

---

## Job Sources

| Source | Type | Notes |
|---|---|---|
| Remotive | JSON API | General remote tech jobs |
| Arbeitnow | JSON API | European-focused remote jobs |
| Jobicy | JSON API | Remote tech jobs |
| Jobspresso | RSS | Curated remote jobs |
| Dynamite Jobs | RSS | Remote-first jobs |
| Working Nomads | JSON API | Remote jobs for nomads |
| GetOnBoard | JSON API | Tech jobs, LatAm-focused (fully remote only) |
| Himalayas | JSON API | Worldwide-only remote jobs |
| RemoteOK | JSON API | Remote jobs, worldwide-heavy |
| WeWorkRemotely | RSS | Curated remote tech jobs |
| Working Nomads | JSON API | Disabled by default (Proxify approval required) |

Jobs older than 10 days are filtered out at fetch time across all sources.

---

## Pipeline

```
full-run
  │
  ├─ FETCH        Pull from all sources → normalize → deduplicate → store
  │
  ├─ EVALUATE     Rule-based scoring against your profile
  │                 remote eligibility · skill overlap · seniority · title relevance
  │                 → status: shortlisted / review / rejected
  │
  └─ ANALYZE      Local LLM (Ollama) pass on review jobs
                    → promotes to shortlisted or rejects with explanation
```

Job state model:

- **Rule layer** — `fit_score`, `rule_status`, `remote_eligibility`, `matched_skills`
- **LLM layer** — `llm_fit_score`, `recommendation`, `llm_confidence`, `fit_explanation`
- **Decision layer** — `status` (new → review → shortlisted / rejected / applied)

---

## Command Reference

Run `python run_pipeline.py help` for the full reference. Key commands:

| Command | What it does |
|---|---|
| `full-run` | Fetch + evaluate + LLM analyze in one shot |
| `full-run --email` | Same, plus email digest if new jobs found |
| `triage` | Work through review jobs: shortlist / reject / open / skip |
| `open-job` | Open a shortlisted job in browser with form prefill |
| `stats` | Job counts by status |
| `shortlist` | List shortlisted jobs |
| `review` | List review jobs |
| `rescore` | Re-apply scoring rules to existing review jobs |
| `setup-credentials` | Store email credentials in Windows Credential Manager |

---

## Setup

### 1. Install dependencies

```powershell
pip install -r requirements.txt
```

### 2. Configure your profile

Edit `profile.yaml` with your skills, target roles, seniority, location preferences, and blacklisted companies.

### 3. Set up Ollama

Install [Ollama](https://ollama.com) and pull a model:

```powershell
ollama pull qwen2.5:7b
```

### 4. (Optional) Configure email reports

```powershell
python run_pipeline.py setup-credentials
```

Credentials are stored in Windows Credential Manager — never written to disk.

Copy `.env.example` to `.env` and set `EMAIL_SMTP_HOST` / `EMAIL_SMTP_PORT` if needed (defaults to Gmail).

### 5. (Optional) Schedule automated runs

`schedule_run.bat` is pre-configured to run `full-run --email`. Register it with Windows Task Scheduler:

The default schedule runs at 8am, 12pm, 4pm, and 8pm daily.

---

## Profile

`profile.yaml` drives all filtering and scoring:

```yaml
skills:          # matched against job titles and descriptions
keywords:        # domain-specific terms (gpu, llm, inference, etc.)
target_roles:    # role titles you're targeting
seniority:       # preferred and acceptable levels
blacklisted_companies:
preferences:
  remote_only: true
  accepted_regions: [worldwide, emea, europe, canada, ...]
  reject_regions: [us only, israel only]
  contractor_ok: true
resumes:         # multiple resumes with tags — best match selected per job
```

---

## Application Assistance

`open-job` opens a job in a Playwright browser window and:

1. Navigates to the application form
2. Detects the ATS (Greenhouse, Lever, Ashby, etc.)
3. Prefills fields from your profile (name, email, phone, LinkedIn, GitHub)
4. Attempts to upload the best-matched resume
5. Waits for your review — **you submit manually**
6. Prompts you to mark the job as `applied`

Bot-protected sites (remoteok.com, weworkremotely.com, jobicy.com) open in your system browser without prefill.
