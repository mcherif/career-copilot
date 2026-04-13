import re as _re

ATS_PATTERNS = {
    "greenhouse": ["greenhouse.io"],
    "lever": ["lever.co"],
    "workday": ["workday.com", "myworkdayjobs.com"],
    "ashby": ["ashbyhq.com"],
    "workable": ["workable.com"],
    "smartrecruiters": ["smartrecruiters.com"],
    "notion": ["notion.so", "notion.site"],
    "recruitee": ["recruitee.com"],
    "comeet": ["comeet.com", "app.comeet.co"],
    "ateam": ["a.team", "onboarding.a.team"],
    "personio": ["personio.com"],
}

# ATS platforms that use custom React/SPA forms incompatible with scan_fields.
MANUAL_ONLY_ATS = {"ateam", "workday"}

# Regex patterns against the full URL for ATSes that use custom employer domains.
_ATS_URL_REGEXES: list[tuple[str, _re.Pattern]] = [
    # Comeet: careers.tether.io/o/{slug}/c/new  (or /c/{id} variants)
    ("comeet", _re.compile(r"/o/[^/?#]+/c/", _re.I)),
]


def detect_ats(url: str) -> str:
    """Detect ATS from job URL.

    Args:
        url: The url string to inspect

    Returns:
        One of the known ATS names (e.g. greenhouse, lever) or unknown.
    """
    if not url:
        return "unknown"

    url_lower = url.lower()

    for ats, patterns in ATS_PATTERNS.items():
        for p in patterns:
            if p in url_lower:
                return ats

    # Regex-based detection for ATSes that use custom employer domains.
    for ats, pattern in _ATS_URL_REGEXES:
        if pattern.search(url):
            return ats

    return "unknown"
