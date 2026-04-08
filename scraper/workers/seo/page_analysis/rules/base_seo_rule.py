"""
Base SEO Rule Structure for Modular Rule Engine

All SEO rules must subclass BaseSEORuleV2 and implement evaluate().
Unlike AI scoring BaseRule (which returns scores), SEO rules return issue documents.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from bson.objectid import ObjectId


class BaseSEORuleV2(ABC):
    """Abstract base class for all modular SEO rules.
    
    Each subclass MUST set these class attributes:
        rule_id   (str):  e.g. "TITLE_MISSING"
        rule_no   (int):  e.g. 1  — MUST match existing DB values exactly
        category  (str):  e.g. "Content"
        severity  (str):  "high" | "medium" | "low"
        description (str): Human-readable description
    """

    rule_id: str = ""
    rule_no: int = 0
    category: str = ""
    severity: str = "medium"
    description: str = ""

    @abstractmethod
    def evaluate(self, normalized: dict, job_id: str, project_id: str, url: str) -> list:
        """
        Evaluate rule against normalized page data.

        Args:
            normalized: Output of normalize_page_data()
            job_id:     Current job ID string
            project_id: Current project ID string
            url:        Page URL being analyzed

        Returns:
            List of issue dicts (empty list if rule passes).
            Each dict MUST be created via self.create_issue().
        """
        pass

    def create_issue(self, job_id, project_id, url,
                     issue_message, detected_value, expected_value,
                     data_key=None, data_path=None):
        """Create a standardized issue document.
        
        Output format is IDENTICAL to the existing create_issue() in page_analysis.py.
        This ensures zero change to DB storage format.
        """
        return {
            "projectId": ObjectId(project_id),
            "seo_jobId": ObjectId(job_id),
            "page_url": url,
            "rule_no": self.rule_no,
            "category": self.category,
            "severity": self.severity,
            "issue_code": self.rule_id,
            "rule_id": self.rule_id,
            "issue_message": issue_message,
            "detected_value": detected_value,
            "expected_value": expected_value,
            "data_key": data_key,
            "data_path": data_path,
            "created_at": datetime.utcnow()
        }

    def __repr__(self):
        return f"SEORule({self.rule_id}, no={self.rule_no}, cat={self.category})"
