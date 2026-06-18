"""
AEO — FAQ Coverage card rules (1 rule).

AEO-048  FAQ Schema Matches Content
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAEO048FAQSchemaMatchesContent(BaseRule):
    RULE_ID  = "AEO-048"
    HUB, CARD, SEVERITY = "aeo", "faq_coverage", "high"
    ISSUE_TITLE       = "FAQ Schema Contains Entries With No Visible Content Match"
    ISSUE_DESCRIPTION = "One or more FAQ schema entries have no corresponding visible question/answer pair on the page. Schema/content mismatches risk rich result penalties and degrade AI extraction accuracy."
    RECOMMENDATION    = "For each mismatched entry: either add the Q&A as visible content on the page, or remove the entry from the FAQPage schema. Every schema FAQ must have a visible counterpart."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        faq = page.get("faq", {})
        schema_present  = bool(faq.get("schema_present", False))
        visible_present = bool(faq.get("visible_present", False))
        schema_count    = int(faq.get("schema_qa_count", 0))
        visible_count   = int(faq.get("visible_qa_count", 0))
        all_matched     = bool(faq.get("all_matched", False))
        mismatches      = faq.get("mismatches", [])

        # No FAQ schema → FAIL (signal missing).
        if not schema_present:
            return self._fail({
                "schema_present": False,
                "reason": "No FAQPage schema present",
            })

        if all_matched:
            return self._pass({
                "schema_present": True,
                "schema_qa_count": schema_count,
                "visible_qa_count": visible_count,
                "mismatches": [],
                "all_matched": True,
            })

        return self._fail({
            "schema_present": True,
            "schema_qa_count": schema_count,
            "visible_qa_count": visible_count,
            "mismatches": mismatches,
            "all_matched": False,
            "orphaned_schema_entries": len(mismatches),
        })


RULES = [
    RuleAEO048FAQSchemaMatchesContent(),
]
