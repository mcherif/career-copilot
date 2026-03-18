ATS_PATTERNS = {
    "greenhouse": ["greenhouse.io"],
    "lever": ["lever.co"],
    "workday": ["workday.com", "myworkdayjobs.com"],
    "ashby": ["ashbyhq.com"],
    "smartrecruiters": ["smartrecruiters.com"]
}

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
