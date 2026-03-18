# Environment-based config with safety controls

DATABASE_URL = "sqlite:///career_copilot.db"

# Safety settings
DRY_RUN = True  # Default to safe mode
SAFETY_LIMITS = {
    "max_applications_per_day": 5,
    "max_auto_opens_per_session": 10,
    "require_confirmation_after": 3
}

# Rate limiting
RATE_LIMIT_DELAY = 2  # seconds between API calls
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

# Job sources (API keys stored in environment variables if needed later)
REMOTIVE_API_URL = "https://remotive.com/api/remote-jobs"
REMOTEOK_API_URL = "https://remoteok.com/api"
