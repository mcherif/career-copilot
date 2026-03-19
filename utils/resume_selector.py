from typing import Dict, Any
from utils.scoring import _find_matches

def select_resume(job: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    """Select the most relevant resume based on job description tag matching.
    
    Args:
        job: Dictionary payload containing the job title and description text.
        profile: The candidate's loaded profile.yaml dictionary.
        
    Returns:
        A dictionary containing the chosen resume_name, resume_path, and tag_matches.
    """
    resumes = profile.get("resumes", [])
    if not resumes:
        return {}

    title = str(job.get("title", "")).lower()
    description = str(job.get("description_text") or job.get("description", "")).lower()
    combined_text = f"{title} {description}"

    best_resume = None
    best_matches = []
    # Using -1 so that even if a resume has 0 tags, it can still structurally become the best_resume temporarily
    max_score = -1 

    for idx, resume in enumerate(resumes):
        tags = resume.get("tags", [])
        matches = _find_matches(combined_text, tags)
        score = len(matches)
        
        # Track the resume that has the absolute highest number of tag overlaps
        if score > max_score:
            max_score = score
            best_resume = resume
            best_matches = matches
            
    # Default fallback: If literally zero tags match any resume, default to "software_engineer"
    if max_score <= 0:
        fallback = next((r for r in resumes if r.get("name") == "software_engineer"), resumes[-1])
        return {
            "resume_name": fallback.get("name", "default_resume"),
            "resume_path": fallback.get("path", ""),
            "tag_matches": []
        }

    return {
        "resume_name": best_resume.get("name", "unknown"),
        "resume_path": best_resume.get("path", ""),
        "tag_matches": best_matches
    }
