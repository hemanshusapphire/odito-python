"""
Base Rule Structure for AI Visibility Scoring v2

All rules must follow this exact schema and implement the required methods.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import json

class BaseRule(ABC):
    """Base class for all AI visibility scoring rules"""

    # Class-level metadata — override in every subclass for meaningful issue output
    TITLE: str = ""
    DESCRIPTION: str = ""
    RECOMMENDATION: str = ""

    def __init__(self, rule_config: Dict[str, Any]):
        """
        Initialize rule with configuration

        Required config fields:
        - rule_id: str
        - category: str
        - description: str
        - weight: float
        - max_score: float
        - applies_to: "page" | "domain"
        - is_required: bool (whether missing feature should create issue)
        """
        self.rule_id = rule_config["rule_id"]
        self.category = rule_config["category"]
        self.description = rule_config["description"]
        self.weight = rule_config["weight"]
        self.max_score = rule_config["max_score"]
        self.applies_to = rule_config["applies_to"]
        self.is_required = rule_config.get("is_required", False)

        self._validate_config()

    def _validate_config(self):
        """Validate rule configuration with fault tolerance"""
        if not self.rule_id or not isinstance(self.rule_id, str):
            raise ValueError(f"Invalid rule_id: {self.rule_id}")

        if not self.category or not isinstance(self.category, str):
            raise ValueError(f"Invalid category: {self.category}")

        print(f"[RULE_DEBUG] rule={self.rule_id} | weight={self.weight} | enabled={getattr(self, 'enabled', True)}")

        if self.weight is None:
            print(f"[RULE_FIX] Rule {self.rule_id} has weight=None, defaulting to 1.0")
            self.weight = 1.0

        if not isinstance(self.weight, (int, float)):
            raise ValueError(f"Invalid weight type for {self.rule_id}: expected float/int, got {type(self.weight)}")

        if self.weight <= 0:
            print(f"[RULE_FIX] Rule {self.rule_id} has weight={self.weight}, auto-disabling")
            self.enabled = False

        if not isinstance(self.max_score, (int, float)) or self.max_score <= 0:
            raise ValueError(f"Invalid max_score: {self.max_score}")

        if self.applies_to not in ["page", "domain"]:
            raise ValueError(f"Invalid applies_to: {self.applies_to}")

    # ── Issue intelligence ──────────────────────────────────────────────────

    def should_create_issue(self, normalized_score: float) -> bool:
        """
        Determine whether this score warrants creating an issue.

        Required rules (is_required=True): issue when score < 75.
        Optional rules: issue only when score < 40 (genuine failure).
        """
        if self.is_required:
            return normalized_score < 75
        return normalized_score < 40

    def get_severity(self, normalized_score: float) -> str:
        """
        Context-aware severity.  Never returns 'high' for optional rules.

        Required rules:
          < 30  → critical
          30–59 → high
          60–74 → medium

        Optional rules:
          < 20  → medium
          20–39 → low
        """
        if self.is_required:
            if normalized_score < 30:
                return "high"
            elif normalized_score < 60:
                return "high"
            else:
                return "medium"
        else:
            if normalized_score < 20:
                return "medium"
            return "low"

    def get_issue_context(self, data: Dict[str, Any], normalized_score: float) -> Dict[str, Any]:
        """
        Return issue metadata when this rule fails.

        Subclasses should override to supply:
          - detected_value  — what was found on the page
          - expected_value  — what should be present
          - evidence        — supporting signals dict

        The default implementation produces sensible fallbacks from
        TITLE / DESCRIPTION / RECOMMENDATION class attributes.
        """
        return {
            "title": self.TITLE or self.description,
            "description": (
                self.DESCRIPTION
                or f"'{self.description}' needs improvement (score: {normalized_score:.0f}/100)."
            ),
            "detected_value": None,
            "expected_value": None,
            "recommendation": (
                self.RECOMMENDATION
                or "Review and improve this aspect of your page's AI visibility."
            ),
            "evidence": {
                "rule_id": self.rule_id,
                "normalized_score": round(normalized_score, 1),
            },
            "confidence": 90 if self.is_required else 70,
        }

    # ── Core rule interface ─────────────────────────────────────────────────

    @abstractmethod
    def evaluate(self, data: Dict[str, Any]) -> float:
        """
        Evaluate rule against page data.

        Args:
            data: Extraction data from seo_ai_visibility collection

        Returns:
            Raw score (0 to max_score)
        """
        pass

    def normalize(self, raw_score: float) -> float:
        """Normalize raw score to 0-100 scale."""
        clamped_score = max(0, min(raw_score, self.max_score))
        return (clamped_score / self.max_score) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary for storage"""
        return {
            "rule_id": self.rule_id,
            "category": self.category,
            "description": self.description,
            "weight": self.weight,
            "max_score": self.max_score,
            "applies_to": self.applies_to,
            "is_required": self.is_required,
        }

    def __repr__(self):
        return f"Rule({self.rule_id}, {self.category}, weight={self.weight})"
