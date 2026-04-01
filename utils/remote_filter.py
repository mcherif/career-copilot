import re
from typing import Dict, Any, Iterable

DEFAULT_ACCEPT_KEYWORDS = [
    "remote anywhere",
    "remote worldwide",
    "remote global",
    "work from anywhere",
    "fully remote anywhere",
    "remote async",
]

DEFAULT_REJECT_KEYWORDS = [
    "remote us only",
    "must reside in the us",
    "must be based in the us",
    "must be located in the us",
    "must live in the us",
    "us citizenship required",
    "security clearance required",
    "remote within us",
    "usa timezones",
    "us timezones",
    "us hours only",
    "north america only",
    "usa only",
    "usa-only",
    "us only",
    "us-only",
    "united states only",
    "us residents only",
    "based in the united states",
    "located in the united states",
]

REVIEW_KEYWORDS = [
    "remote",
    "fully remote",
    "remote-first",
]

US_ONLY_LOCATIONS = {
    "usa",
    "united states",
    "us",
}

# Substrings that, when found in raw_location, indicate US restriction
# unless a broader region (worldwide, emea, etc.) is also present.
_US_LOCATION_SUBSTRINGS = ("united states", " usa", "u.s.a", "(u.s.", "(u.s)", "(us)", "(us ")
_BROAD_REGION_OVERRIDES = ("worldwide", "global", "emea", "europe", "anywhere", "international")

MIXED_REGION_HINTS = [
    "americas",
    "asia",
    "oceania",
    "australia",
    "africa",
    "middle east",
    "israel",
    "usa",
    "united states",
]


def _normalize_entries(items: Iterable[Any]) -> list[str]:
    normalized = []
    for item in items or []:
        value = str(item or "").strip().lower()
        if value:
            normalized.append(value)
    return normalized


def _phrase_in_text(phrases: Iterable[str], text: str) -> bool:
    return any(phrase and phrase in text for phrase in phrases)


def _token_in_text(token: str, text: str) -> bool:
    if not token or not text:
        return False
    pattern = r"(?<!\w)" + re.escape(token) + r"(?!\w)"
    return re.search(pattern, text) is not None


def classify_remote_eligibility(job: Dict[str, Any], profile: Dict[str, Any] | None = None) -> str:
    """Classify a job listing as accept, review, or reject for remote eligibility."""
    raw_location = str(job.get("raw_location_text")
                       or job.get("location") or "").strip().lower()
    cleaned_desc = str(job.get("description_text") or job.get(
        "description") or "").strip().lower()
    combined_text = f"{raw_location} {cleaned_desc}".strip()

    preferences = (profile or {}).get("preferences", {})
    accepted_regions = _normalize_entries(
        preferences.get("accepted_regions", []))
    reject_regions = _normalize_entries(preferences.get("reject_regions", []))

    # Treat explicit work authorization regions as acceptable geography hints too.
    work_auth = (profile or {}).get("work_authorization", {})
    accepted_regions.extend(
        region.strip().lower()
        for region, allowed in work_auth.items()
        if allowed and str(region).strip()
    )
    accepted_regions.extend(
        ["worldwide", "global", "anywhere", "remote anywhere"])

    if raw_location in US_ONLY_LOCATIONS:
        return "reject"

    # Catch "Remote - United States", "Remote (U.S.)", "Remote (US)", etc.
    if any(us in raw_location for us in _US_LOCATION_SUBSTRINGS):
        if not any(broad in raw_location for broad in _BROAD_REGION_OVERRIDES):
            # If an accepted profile region also appears (e.g. "Remote (US or Canada)"),
            # downgrade to review rather than hard reject.
            _generic = {"worldwide", "global", "anywhere", "remote anywhere"}
            profile_specific = [r for r in accepted_regions if r not in _generic]
            if any(r in raw_location for r in profile_specific):
                return "review"
            return "reject"

    remote_only = (profile or {}).get("preferences", {}).get("remote_only", False)
    if remote_only and "hybrid" in raw_location:
        return "reject"

    if _phrase_in_text(DEFAULT_REJECT_KEYWORDS, combined_text):
        return "reject"

    if _phrase_in_text(reject_regions, combined_text):
        return "reject"

    if raw_location in {"worldwide", "global", "anywhere"}:
        return "accept"

    if _phrase_in_text(DEFAULT_ACCEPT_KEYWORDS, combined_text):
        return "accept"

    accepted_hit = any(_token_in_text(region, raw_location)
                       for region in accepted_regions)
    mixed_region_hit = any(_token_in_text(region, raw_location)
                           for region in MIXED_REGION_HINTS)

    if accepted_hit:
        if mixed_region_hit and not any(_token_in_text(region, raw_location) for region in reject_regions):
            return "review"
        return "accept"

    if _phrase_in_text(REVIEW_KEYWORDS, combined_text) or raw_location:
        return "review"

    return "review"
