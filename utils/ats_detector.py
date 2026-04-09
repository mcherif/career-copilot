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
                
    return "unknown"
