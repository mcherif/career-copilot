# Career Copilot – System Architecture

For a high-level overview see **README.md**.

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
→ Ingestion Pipeline
→ Normalization & Deduplication
→ SQLite Database
→ Career Intelligence Engine
→ LLM Job Analysis
→ Application Prefill Agent
→ Human Approval Gate
→ Application Submission

---

## Core Components

### Job Sources

Supported sources:

- Remotive API
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

- jobs
- application_history
- pipeline_runs

### Career Intelligence Engine

Evaluates job relevance using:

- remote eligibility filtering
- rule-based scoring
- application history checks
- resume selection

### LLM Job Analysis

Uses local LLM models through **Ollama** for deeper evaluation:

- fit scoring
- strengths and skill gaps
- apply/skip recommendation

### Application Prefill Agent

Playwright-based automation supports:

- Greenhouse
- Lever

Unsupported platforms fall back to manual mode.

### Human Approval Gate

Every application must be reviewed before submission.

Users can:

- approve
- edit
- skip

### Application Submission

After approval:

- the application is submitted
- the result is logged
- application_history is updated
