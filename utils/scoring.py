import re
from typing import Dict, Any
from utils.remote_filter import classify_remote_eligibility

# Sources that require a paid subscription or don't have a direct apply URL.
# Jobs from these sources are capped at 'review' so they never reach shortlisted.
_NO_DIRECT_APPLY_SOURCES: frozenset[str] = frozenset({
    "weworkremotely",  # subscription required to view full job / apply
})

TITLE_REJECT_KEYWORDS = [
    # Sales / BD
    "sales manager", "sales director", "sales executive", "sales representative",
    "regional sales", "account executive", "account manager",
    # Marketing / Social
    "social media", "marketing manager", "marketing specialist", "brand manager", "brand director",
    # Customer-facing non-tech
    "customer service", "customer support",
    # Writing / content
    "copywriter", "content writer", "freelance writer",
    # Recruiting
    "recruiter", "talent acquisition",
    # Non-tech consulting
    "career advancement", "career consultant", "career coach",
    "energy solutions", "energy advisor",
    "implementation consultant",
    # ERP / non-engineering
    "sap consultant", "sap berater", "s/4hana",
]

KEYWORD_ALIASES = {
    "computer vision": ["cv"],
    "cv": ["computer vision"],
    "llm": ["large language model", "large language models", "genai", "generative ai"],
    "mlops": ["ml infra", "ml infrastructure", "deployment", "production ml"],
    "model serving": ["serving", "inference serving"],
    "iot": ["internet of things"],
    "embedded": ["embedded systems"],
    "ai": ["artificial intelligence", "ai systems", "enterprise ai", "production ai"],
    "nlp": ["natural language processing"],
}

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

def _profile_blob(profile: Dict[str, Any]) -> str:
    parts = []
    for key in ("skills", "keywords", "target_roles", "summary"):
        value = profile.get(key, [])
        if isinstance(value, list):
            parts.extend(str(item or "") for item in value)
        elif value:
            parts.append(str(value))
    return " ".join(parts).lower()

def _expanded_keywords(profile: Dict[str, Any]) -> list:
    explicit_keywords = [str(keyword).strip() for keyword in profile.get("keywords", []) if str(keyword).strip()]
    expanded = list(explicit_keywords)

    for keyword in explicit_keywords:
        expanded.extend(KEYWORD_ALIASES.get(keyword.lower(), []))

    # Add a few adjacent AI-domain keywords when the profile clearly targets those areas.
    profile_text = _profile_blob(profile)
    if any(token in profile_text for token in ["llm", "machine learning", "ml engineer", "mlops", "ai engineer", "pytorch"]):
        expanded.append("ai")
    if any(token in profile_text for token in ["llm", "ai engineer", "machine learning", "ml engineer", "computer vision"]):
        expanded.append("nlp")
    if any(token in profile_text for token in ["computer vision", "opencv"]):
        expanded.append("cv")

    return _unique(expanded)

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
        "devops",
        "infrastructure",
        "data",
        "microservices",
        "distributed",
        "api",
        "gpu",
        "llm",
    ]
    role_tokens = ["engineer", "developer", "architect", "specialist", "lead"]
    return any(token in title_lower for token in domain_tokens) and any(token in title_lower for token in role_tokens)

# Languages that may be explicitly required by employers.
# English is intentionally omitted — it's ubiquitous and nearly always implied.
# Canonical key must match what the user puts in profile.languages.
_KNOWN_LANG_NAMES: dict[str, list[str]] = {
    "mandarin": ["mandarin"],
    "chinese":  ["chinese", "cantonese"],
    "japanese": ["japanese"],
    "korean":   ["korean"],
    "french":   ["french"],
    "german":   ["german", "deutsch"],
    "dutch":    ["dutch"],
    "spanish":  ["spanish"],
    "portuguese": ["portuguese"],
    "italian":  ["italian"],
    "russian":  ["russian"],
    "arabic":   ["arabic"],
    "hindi":    ["hindi"],
    "hebrew":   ["hebrew"],
    "turkish":  ["turkish"],
    "polish":   ["polish"],
    "swedish":  ["swedish"],
    "danish":   ["danish"],
    "norwegian": ["norwegian"],
    "finnish":  ["finnish"],
    "thai":     ["thai"],
    "ukrainian": ["ukrainian"],
}

# Words that introduce a language requirement; we look for language names
# within a ±70-character window around each match.
_LANG_INDICATOR_RE = re.compile(
    r"\b(?:fluent|native|bilingual|mother.?tongue|proficient|proficiency|"
    r"language skills?|language requirements?|business.?level|conversational)\b",
    re.IGNORECASE,
)


def _required_languages_in_text(text: str) -> set[str]:
    """Return canonical language names that appear in a language-requirement context."""
    text_lower = text.lower()
    found: set[str] = set()
    for m in _LANG_INDICATOR_RE.finditer(text_lower):
        start = max(0, m.start() - 70)
        end = min(len(text_lower), m.end() + 70)
        window = text_lower[start:end]
        for lang, aliases in _KNOWN_LANG_NAMES.items():
            if any(re.search(r"\b" + alias + r"\b", window) for alias in aliases):
                found.add(lang)
    return found


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

    blacklist = [str(c).strip().lower() for c in profile.get("blacklisted_companies", []) if str(c).strip()]
    company = str(job.get("company", "")).strip().lower()
    if blacklist and any(b == company or b in company for b in blacklist):
        result["recommended_status"] = "rejected"
        return result

    if any(kw in title for kw in TITLE_REJECT_KEYWORDS):
        result["recommended_status"] = "rejected"
        return result

    # Hard reject: job explicitly requires a language the candidate doesn't speak.
    # Detect patterns like "fluent mandarin", "japanese speaker", "bilingual chinese".
    profile_langs = {str(lang).strip().lower() for lang in (profile or {}).get("languages", [])}
    _scan_text = title + " " + description[:2_000]
    _required_langs = _required_languages_in_text(_scan_text)
    if _required_langs - profile_langs:
        result["recommended_status"] = "rejected"
        return result

    # Reject jobs written in a language the candidate doesn't speak.
    # Markers per language that rarely appear in English/French/Arabic text.
    _LANG_MARKERS = {
        "spanish": ["experiencia", "conocimiento", "ingenier", "buscamos", "dise\u00f1",
                    "construir", "colaborar", "licenciatura", "responsabilidades", "requisitos"],
        "portuguese": ["experi\u00eancia", "conhecimento", "engenharia", "desenvolvedor",
                       "habilidades", "requisitos", "respons\u00e1vel", "constru\u00e7\u00e3o"],
        "german": ["kenntnisse", "erfahrung", "anforderungen", "berufserfahrung",
                   "kenntnisse", "wir suchen", "stellenbeschreibung", "aufgaben"],
    }
    profile_langs = {str(lang).lower() for lang in (profile or {}).get("languages", ["english"])}
    desc_lower = str(job.get("description_text") or job.get("description") or "").lower()
    for lang, markers in _LANG_MARKERS.items():
        if lang not in profile_langs and sum(1 for m in markers if m in desc_lower) >= 3:
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
        score += 10
        
    # A. Skills overlap
    skills = profile.get("skills", [])
    title_skills = _find_matches(title, skills)
    description_skills = [skill for skill in _find_matches(description, skills) if skill not in title_skills]
    matched_skills = _unique(title_skills + description_skills)
    result["matched_skills"] = matched_skills
    skills_score = min((len(title_skills) * 12) + (len(description_skills) * 4), 32)
    score += skills_score
    
    # B. Keywords overlap
    keywords = _expanded_keywords(profile)
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
    if not has_title_relevance and score < 40:
        result["fit_score"] = score
        result["recommended_status"] = "rejected"
        return result

    # Final thresholding
    result["fit_score"] = score
    if score >= 65:
        result["recommended_status"] = "shortlisted"
    elif score >= 28:
        result["recommended_status"] = "review"
    else:
        result["recommended_status"] = "rejected"

    # Sources without a direct apply path are capped at review so they never
    # reach the shortlist (no point surfacing jobs we can't act on).
    source = str(job.get("source", "")).lower()
    if source in _NO_DIRECT_APPLY_SOURCES and result["recommended_status"] == "shortlisted":
        result["recommended_status"] = "review"

    return result
