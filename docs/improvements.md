# Future Improvements

## Database & Deduplication

- **Job Deduplication Optimization**: Currently, the `is_duplicate` check dynamically recalculates hashes in Python for all fetched rows from a specific company each time. This is sufficient for Day 1, but as the database scales and a company accumulates many postings, this fallback method will become slower.
  - **Future Actions**: 
    1. Store a dedicated `job_hash` column natively in the database to allow immediate hash-based queries.
    2. Alternatively, implement a normalized composite key strategy within the database schema to enforce uniqueness constraints directly at the database engine level.
