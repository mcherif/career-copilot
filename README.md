# Career Copilot

Career Copilot is an intelligent job discovery and application assistant designed for remote technical roles.

It automatically discovers job opportunities, evaluates them against your profile, and assists with application form filling — while always keeping a **human in control of final submissions**.

The goal is to reduce the time spent searching and applying to jobs while maintaining safety, transparency, and full oversight.

## Why this project exists

Job searching often requires repeating the same manual steps across dozens of platforms.  
Career Copilot focuses on automating the *mechanical parts* of the process while keeping humans responsible for the final decision.

The goal is **assistive automation, not blind automation**.

---

## Core Features

Career Copilot helps automate the most repetitive parts of the job search process:

- Discover remote jobs from multiple sources
- Normalize and deduplicate job listings
- Evaluate jobs against your skills and preferences
- Generate explanations for why a job fits your profile
- Prefill application forms on supported job platforms
- Require **explicit human approval** before submission

---

## Design Principles

**Privacy First** – All LLM processing happens locally using open-weight models through Ollama.

**Human in the Loop** – No application is submitted automatically. Every submission requires explicit approval.

**Safety by Default** – Development mode runs with `DRY_RUN=true` to prevent accidental submissions.

**Modular Architecture** – Each component (job discovery, evaluation, automation) works independently.

---

## System Pipeline

Job Sources → Ingestion Pipeline → Normalization → Database → Career Intelligence → LLM Analysis → Prefill Automation → Human Approval → Submission

Full technical documentation:

➡ **docs/ARCHITECTURE.md**
