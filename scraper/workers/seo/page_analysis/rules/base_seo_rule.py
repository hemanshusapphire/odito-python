"""
Base SEO Rule Structure for Modular Rule Engine

All SEO rules must subclass BaseSEORuleV2 and implement evaluate().
Unlike AI scoring BaseRule (which returns scores), SEO rules return issue documents.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from bson.objectid import ObjectId
from typing import List, Optional

from .issue_identity import lifecycle_fields


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
    excluded_page_types: Optional[List[str]] = None  # Safe: None default, not mutable list

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
                     data_key=None, data_path=None, impact=None, recommendation=None,
                     context=None, before_snapshot=None, severity=None):
        """Create a standardized issue document with SEO insights.

        severity: optional per-call override of self.severity. Most rules emit
                 one finding type and should leave this unset (falls back to
                 the class-level self.severity, unchanged). Rules whose
                 evaluate() can emit multiple distinct finding types at
                 different severities (e.g. "entity schema entirely missing"
                 vs "entity schema present but missing a recommended field")
                 should pass the correct severity explicitly here rather than
                 mutating self.severity — that keeps each create_issue() call
                 self-contained and avoids one branch's severity leaking into
                 another if a future edit reorders or adds branches.

        detected_value must be the RAW detected content (actual text, actual URL,
        actual number) — NEVER a diagnostic message string like
        "Description length: 112 characters".  Store diagnostic info in context={}.

        context: optional dict for pre-computed values that the recommendation
                 engine and UI can consume directly, e.g.:
                 {
                   "detected_length": 112,
                   "target_min": 120,
                   "target_max": 160,
                   "page_title": "...",
                   "h1_text": "..."
                 }

        before_snapshot: optional dict capturing the actual pre-fix page state
                 for issue types that support it (title, meta description, h1,
                 image alt text, canonical) — a structured, typed payload like
                 {"type": "meta_description", "metaDescription": "Old text..."},
                 refreshed every re-analysis just like detected_value/context.
                 Used by the Task fix-history feature to show a real "Before"
                 value instead of re-deriving one from live data later. Rules
                 that don't pass this fall back to detected_value/context for
                 display — leave it unset rather than fabricating a value.
        """
        created_at = datetime.utcnow()

        issue = {
            "projectId": ObjectId(project_id),
            "seo_jobId": ObjectId(job_id),
            "page_url": url,
            "rule_no": self.rule_no,
            "category": self.category,
            "severity": severity if severity is not None else self.severity,
            "issue_code": self.rule_id,
            "rule_id": self.rule_id,
            "issue_message": issue_message,
            "detected_value": detected_value,
            "expected_value": expected_value,
            "data_key": data_key,
            "data_path": data_path,
            "created_at": created_at,
            # P0-003 lifecycle metadata — see issue_identity.py for the
            # dedup_key contract. Additive only: every pre-existing field
            # above is unchanged.
            **lifecycle_fields(project_id, url, self.rule_id, data_path, created_at)
        }

        if impact:
            issue["impact"] = impact
        if recommendation:
            issue["recommendation"] = recommendation
        if context:
            issue["context"] = context
        if before_snapshot is not None:
            issue["before_snapshot"] = before_snapshot

        return issue

    def get_excluded_page_types(self) -> List[str]:
        """Safely get excluded page types list, avoiding mutable default bug."""
        return self.excluded_page_types or []

    def __repr__(self):
        return f"SEORule({self.rule_id}, no={self.rule_no}, cat={self.category})"
