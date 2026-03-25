# Career Copilot

Career Copilot is an intelligent job discovery and application assistant designed for remote technical roles.

It automatically discovers job opportunities, evaluates them against your profile, and assists with application form filling while always keeping a **human in control of final submissions**.

The goal is to reduce the time spent searching and applying to jobs while maintaining safety, transparency, and full oversight.

## Why this project exists

Job searching often requires repeating the same manual steps across dozens of platforms.
Career Copilot focuses on automating the mechanical parts of the process while keeping humans responsible for the final decision.

The goal is **assistive automation, not blind automation**.

---

## Core Features

Career Copilot helps automate the most repetitive parts of the job search process.

**Implemented:**

- Discover remote jobs from multiple sources
- Normalize and deduplicate job listings
- Evaluate jobs against your skills and preferences
- Generate explanations for why a job fits your profile

**Planned:**

- Prefill application forms on supported job platforms
- Require **explicit human approval** before submission

---

## Design Principles

**Privacy First** - All LLM processing happens locally using open-weight models through Ollama.

**Human in the Loop** - No application is submitted automatically. Every submission requires explicit approval.

**Safety by Default** - Development mode runs with `DRY_RUN=true` to prevent accidental submissions.

**Modular Architecture** - Each component (job discovery, evaluation, automation) works independently.

---

## System Pipeline

Job Sources -> Ingestion Pipeline -> Normalization -> Database -> Career Intelligence -> LLM Analysis -> Prefill Automation -> Human Approval -> Submission

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

Result: `jobs` table

```text
job metadata
rule_status + rule-based scoring
recommended resume
LLM reasoning + confidence
final job status
```

State model:

- Deterministic layer: `rule_status`, `fit_score`
- Semantic layer: `llm_fit_score`, `recommendation`, `llm_confidence`, `llm_status`
- Final decision layer: `status`

Example production command:

```powershell
python run_pipeline.py full-run `
  --source remotive `
  --profile profile.yaml `
  --model qwen2.5:7b `
  --analyze-status review `
  --analyze-limit 10
```

Human-facing review commands:

```powershell
python run_pipeline.py shortlist
python run_pipeline.py review
python run_pipeline.py rejected
```

Full technical documentation:

- `docs/ARCHITECTURE.md`

Planned enhancements and future work:

- `docs/improvements.md`
