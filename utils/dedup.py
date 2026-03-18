import hashlib
import re
from typing import Dict, Any
from sqlalchemy.orm import Session
from models.database import Job

def _normalize_text(text: Any) -> str:
    """Aggressively normalize text for hashing by removing all non-alphanumeric characters."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', str(text).lower())

def generate_job_hash(company: str, title: str, location: str) -> str:
    """Generate unique hash for deduplication.
    
    Aggressively normalizes inputs to handle whitespace and punctuation differences,
    then returns an MD5 hex digest.
    """
    company_norm = _normalize_text(company)
    title_norm = _normalize_text(title)
    location_norm = _normalize_text(location)
    
    hash_str = f"{company_norm}|{title_norm}|{location_norm}"
    return hashlib.md5(hash_str.encode('utf-8')).hexdigest()

def is_duplicate(job_data: Dict[str, Any], session: Session) -> bool:
    """Check if job already exists by URL or hash."""
    url = job_data.get("url")
    if url:
        existing_url = session.query(Job).filter(Job.url == url).first()
        if existing_url:
            return True
            
    company = job_data.get("company", "")
    title = job_data.get("title", "")
    location = job_data.get("location", "")
    target_hash = generate_job_hash(company, title, location)
    
    # Since we do not explicitly store the hash column in Day-1 schema,
    # we narrow candidates by company (case-insensitive filter) 
    # and then calculate the hash locally to verify.
    potential_matches = session.query(Job).filter(
        Job.company.ilike(company)
    ).all()
    
    for existing_job in potential_matches:
        existing_hash = generate_job_hash(
            existing_job.company, 
            existing_job.title, 
            existing_job.location
        )
        if existing_hash == target_hash:
            return True
            
    return False
