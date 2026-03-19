from typing import Dict, Any
from sqlalchemy.orm import Session
from models.database import ApplicationHistory

def has_already_applied(job: Dict[str, Any], session: Session) -> bool:
    """Check if the candidate has already applied for this role.
    
    Queries the application_history table for a case-insensitive match 
    on company and job title.
    
    Args:
        job: Dictionary containing normalized job details ('company', 'title').
        session: Active SQLAlchemy database session.
        
    Returns:
        True if an application record exists, False otherwise.
    """
    company = str(job.get("company", ""))
    title = str(job.get("title", "") or job.get("job_title", ""))
    
    if not company or not title:
        # Cannot reliably deduplicate if core identifying fields are missing
        return False
        
    # Query database for case-insensitive match
    existing_application = session.query(ApplicationHistory).filter(
        ApplicationHistory.company.ilike(company),
        ApplicationHistory.job_title.ilike(title)
    ).first()
    
    return existing_application is not None
