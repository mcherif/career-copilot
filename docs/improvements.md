# Future Improvements

## Database & Deduplication

- **Job Deduplication Optimization**: Currently, the `is_duplicate` check dynamically recalculates hashes in Python for all fetched rows from a specific company each time. This is sufficient for Day 1, but as the database scales and a company accumulates many postings, this fallback method will become slower.
  - **Future Actions**: 
    1. Store a dedicated `job_hash` column natively in the database to allow immediate hash-based queries.
    2. Alternatively, implement a normalized composite key strategy within the database schema to enforce uniqueness constraints directly at the database engine level.

## Profile Generation

- **LLM-assisted profile drafting**: The current design assumes the user curates `profile.yaml` directly. A stronger long-term flow is to use the LLM to draft the profile from one or more resumes, then keep the approved YAML as the canonical source of truth.
  - **Why**:
    1. It reduces repetitive manual setup for new users.
    2. It keeps production scoring deterministic by relying on a reviewed `profile.yaml`, not fresh inference on every run.
    3. It creates a clean human-in-the-loop step for correcting omissions, inflated claims, or domain mismatches.
  - **Future Actions**:
    1. Let the user provide one or more resume files.
    2. Generate a draft profile summary, skills, keywords, and target roles with the local LLM.
    3. Present the draft for approval and manual edits.
    4. Save the approved result to `profile.yaml` and use that file as the persistent profile for scoring and LLM analysis.
