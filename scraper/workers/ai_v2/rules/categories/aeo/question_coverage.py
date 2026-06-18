"""
AEO — Question Coverage card rules (2 rules).

AEO-047  Question Based H2 Headings
AEO-049  HowTo Schema On Process Pages
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAEO047QuestionH2(BaseRule):
    RULE_ID  = "AEO-047"
    HUB, CARD, SEVERITY = "aeo", "question_coverage", "high"
    ISSUE_TITLE       = "H2 Headings Are Not Question-Optimized"
    ISSUE_DESCRIPTION = "Fewer than 30% of H2 headings are phrased as questions. AI systems prefer section headings that mirror user query patterns for section-level extraction."
    RECOMMENDATION    = "Rephrase at least 30% of H2 headings as natural language questions that reflect what a user would ask about each section's topic."
    EXPECTED_IMPACT   = "high"
    _MIN_RATIO = 0.30

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        content = page.get("content", {})
        total   = int(content.get("total_h2", 0))
        q_count = int(content.get("question_h2_count", 0))
        ratio   = float(content.get("question_h2_ratio", 0.0))

        if total == 0:
            # No H2s at all — separate structural issue, mark FAIL with context.
            return self._fail({
                "total_h2": 0,
                "question_h2_count": 0,
                "question_ratio": 0.0,
                "note": "No H2 headings found on page",
            })
        if ratio >= self._MIN_RATIO:
            return self._pass({
                "total_h2": total,
                "question_h2_count": q_count,
                "question_ratio": ratio,
                "threshold": self._MIN_RATIO,
            })
        return self._fail({
            "total_h2": total,
            "question_h2_count": q_count,
            "question_ratio": ratio,
            "threshold": self._MIN_RATIO,
        })


class RuleAEO049HowToSchema(BaseRule):
    RULE_ID  = "AEO-049"
    HUB, CARD, SEVERITY = "aeo", "question_coverage", "medium"
    ISSUE_TITLE       = "HowTo Schema Missing on Process Page"
    ISSUE_DESCRIPTION = "This page describes a step-by-step process but lacks HowTo schema, missing rich result eligibility and AI step-extraction accuracy."
    RECOMMENDATION    = "Add HowTo schema with numbered steps matching the page's instructional content. Each step must include a 'name' and 'text' property."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        content          = page.get("content", {})
        is_process       = bool(content.get("is_process_page", False))
        howto            = page.get("schema", {}).get("howto") or {}
        howto_present    = bool(howto.get("present", False))
        steps_valid      = bool(howto.get("steps_valid", False))

        if not is_process:
            # Not a process page — rule does not apply, auto-PASS.
            return self._pass({"process_page_detected": False, "note": "Rule not applicable to this page type"})

        if howto_present and steps_valid:
            return self._pass({
                "process_page_detected": True,
                "howto_schema_present": True,
                "step_count": howto.get("step_count", 0),
                "steps_valid": True,
            })
        return self._fail({
            "process_page_detected": True,
            "howto_schema_present": howto_present,
            "step_count": howto.get("step_count", 0),
            "steps_valid": steps_valid,
        })


RULES = [
    RuleAEO047QuestionH2(),
    RuleAEO049HowToSchema(),
]
