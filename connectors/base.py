from typing import List, Dict, Any

class BaseConnector:
    """Base interface for all job source connectors."""

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        """Fetch raw jobs from source"""
        raise NotImplementedError
    
    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to unified schema"""
        raise NotImplementedError
    
    def get_source_name(self) -> str:
        """Return source identifier"""
        raise NotImplementedError
