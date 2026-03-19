import re
from typing import Dict, Any
from utils.remote_filter import classify_remote_eligibility

def _find_matches(text: str, candidates: list) -> list:
    """Find robust whole-word and symbol matches of terms in text."""
    if not text or not candidates:
        return []
        
    text_lower = text.lower()
    matches = []
    
    for term in candidates:
        term_lower = str(term).lower()
        # Escape special chars (like C++) and use non-word boundary matching
        # to ensure "C" doesn't match "CEO" and "Python" doesn't match "Pythonic"
        escaped = re.escape(term_lower)
        pattern = r'(?:\b|\s)' + escaped + r'(?:\b|\s|[.,;!?)])'
        
        if re.search(pattern, text_lower):
            matches.append(term)
            
    return matches

def _unique(items: list) -> list:
    seen = set()
    ordered = []
    for item in items:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered

def _matches_seniority_level(text: str, level: str) -> bool:
    normalized_level = str(level or "").strip().lower()
    if not normalized_level or not text:
        return False

    aliases = {
        "senior": ["senior", "sr", "sr.", "snr", "snr."],
        "mid": ["mid", "mid-level", "midlevel", "intermediate"],
        "lead": ["lead", "tech lead", "technical lead"],
    }

    candidates = aliases.get(normalized_level, [normalized_level])
    return bool(_find_matches(text, candidates))

def _title_role_score(title: str, target_roles: list) -> int:
    if not title:
        return 0

    title_lower = title.lower()
    normalized_roles = [str(role).lower() for role in target_roles or [] if str(role).strip()]

    if any(role in title_lower for role in normalized_roles):
        return 20

    broad_patterns = [
        ("ai", ("engineer", "architect", "developer")),
        ("machine learning", ("engineer", "developer", "architect")),
        ("ml", ("engineer", "developer", "architect", "ops")),
        ("backend", ("engineer", "developer")),
        ("software", ("engineer", "developer")),
        ("full-stack", ("engineer", "developer")),
        ("platform", ("engineer", "developer")),
        ("cloud", ("engineer", "infrastructure", "platform")),
        ("inference", ("engineer", "platform")),
        ("mlops", tuple()),
    ]

    for stem, suffixes in broad_patterns:
        if stem not in title_lower:
            continue
        if not suffixes or any(suffix in title_lower for suffix in suffixes):
            return 10

    return 0

def _has_title_relevance(title: str, title_skill_matches: list, title_keyword_matches: list, role_score: int) -> bool:
    if role_score > 0 or title_skill_matches or title_keyword_matches:
        return True

    title_lower = title.lower()
    domain_tokens = [
        "ai",
        "machine learning",
        "ml",
        "backend",
        "platform",
        "cloud",
        "inference",
        "mlops",
        "firmware",
        "embedded",
        "systems",
    ]
    role_tokens = ["engineer", "developer", "architect"]
    return any(token in title_lower for token in domain_tokens) and any(token in title_lower for token in role_tokens)

def score_job(job: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluates a job against a user profile using deterministic rules.
    
    Returns a dictionary mapping the scoring breakdown and final recommended status.
    """
    score = 0
    title = str(job.get("title", "")).lower()
    description = str(job.get("description_text") or job.get("description", "")).lower()
    combined_text = f"{title} {description}"
    
    result = {
        "fit_score": 0,
        # Always recompute remote eligibility from raw job fields so rescoring
        # picks up updated rules and profile geography instead of stale DB state.
        "remote_eligibility": classify_remote_eligibility(job, profile),
        "matched_skills": [],
        "matched_keywords": [],
        "seniority_match": False,
        "contractor_bonus": False,
        "recommended_status": "new"
    }
    
    # 1. Hard Rejects
    if result["remote_eligibility"] == "reject":
        result["recommended_status"] = "rejected"
        return result
        
    if "junior" in combined_text or "intern" in title:
        score -= 30
        
    # G. Timezone / Region warnings
    if "pst hours" in combined_text or "us hours only" in combined_text or "pacific time" in combined_text:
        score -= 20
        
    # E. Remote scoring
    if result["remote_eligibility"] == "accept":
        score += 20
    elif result["remote_eligibility"] == "review":
        score += 5
        
    # A. Skills overlap
    skills = profile.get("skills", [])
    title_skills = _find_matches(title, skills)
    description_skills = [skill for skill in _find_matches(description, skills) if skill not in title_skills]
    matched_skills = _unique(title_skills + description_skills)
    result["matched_skills"] = matched_skills
    skills_score = min((len(title_skills) * 12) + (len(description_skills) * 4), 32)
    score += skills_score
    
    # B. Keywords overlap
    keywords = profile.get("keywords", [])
    title_keywords = _find_matches(title, keywords)
    description_keywords = [keyword for keyword in _find_matches(description, keywords) if keyword not in title_keywords]
    matched_keywords = _unique(title_keywords + description_keywords)
    result["matched_keywords"] = matched_keywords
    keywords_score = min((len(title_keywords) * 6) + (len(description_keywords) * 2), 12)
    score += keywords_score
    
    # C. Role match
    target_roles = profile.get("target_roles", [])
    role_score = _title_role_score(title, target_roles)
    score += role_score
        
    # D. Seniority match
    seniority = profile.get("seniority", {})
    preferred_levels = seniority.get("preferred", [])
    acceptable_levels = seniority.get("acceptable", [])
            
    for level in preferred_levels:
        if _matches_seniority_level(title, level) or _matches_seniority_level(description[:500], level):
            score += 10
            result["seniority_match"] = True
            break
            
    if not result["seniority_match"]:
        for level in acceptable_levels:
            if _matches_seniority_level(title, level) or _matches_seniority_level(description[:500], level):
                score += 5
                result["seniority_match"] = True
                break
                
    # F. Contractor friendliness
    prefs = profile.get("preferences", {})
    contract_words = ["contract", "contractor", "freelance", "consulting"]
    
    if any(cw in combined_text for cw in contract_words):
        if prefs.get("contractor_ok", False):
            score += 10
            result["contractor_bonus"] = True
        else:
            score -= 15 # Severe penalty if contract isn't wanted

    has_title_relevance = _has_title_relevance(title, title_skills, title_keywords, role_score)
    if not has_title_relevance and score < 45:
        result["fit_score"] = score
        result["recommended_status"] = "rejected"
        return result
            
    # Final thresholding
    result["fit_score"] = score
    if score >= 65:
        result["recommended_status"] = "shortlisted"
    elif score >= 35:
        result["recommended_status"] = "review" 
    else:
        result["recommended_status"] = "rejected"
        
    return result
