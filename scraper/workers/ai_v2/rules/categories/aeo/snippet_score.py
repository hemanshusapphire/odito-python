"""
AEO — Snippet Score card rules (3 rules).

AEO-051  Bullet / Number Lists
AEO-052  Comparison Tables
AEO-054  Authoritative External Sources
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAEO051Lists(BaseRule):
    RULE_ID  = "AEO-051"
    HUB, CARD, SEVERITY = "aeo", "snippet_score", "high"
    ISSUE_TITLE       = "Insufficient List Formatting for AI Extraction"
    ISSUE_DESCRIPTION = "This page has fewer than 2 qualifying lists (each needing 3+ items). List-formatted content is the primary target for AI list-snippet extraction."
    RECOMMENDATION    = "Convert key sections (steps, features, benefits, requirements) into bullet or numbered lists with at least 3 items each."
    EXPECTED_IMPACT   = "high"
    _MIN_LISTS = 2

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        lists = page.get("content", {}).get("lists", {})
        qualifying = int(lists.get("qualifying", 0))
        total      = int(lists.get("total", 0))
        if qualifying >= self._MIN_LISTS:
            return self._pass({
                "qualifying_lists": qualifying,
                "total_lists": total,
                "threshold": self._MIN_LISTS,
            })
        return self._fail({
            "qualifying_lists": qualifying,
            "total_lists": total,
            "threshold": self._MIN_LISTS,
        })


class RuleAEO052ComparisonTables(BaseRule):
    RULE_ID  = "AEO-052"
    HUB, CARD, SEVERITY = "aeo", "snippet_score", "medium"
    ISSUE_TITLE       = "Comparison Content Not Structured as Table"
    ISSUE_DESCRIPTION = "This page discusses comparisons but has no qualifying HTML table. AI systems cannot extract structured comparative data from prose."
    RECOMMENDATION    = "Convert comparison content into an HTML table with clear column headers and one row per compared item."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        content  = page.get("content", {})
        tables   = content.get("tables", {})
        has_intent    = bool(content.get("has_comparison_intent", False))
        qualifying    = int(tables.get("qualifying", 0))

        # Only fire if page has comparison intent keywords.
        if not has_intent:
            return self._pass({
                "comparison_intent": False,
                "note": "No comparison intent detected — rule not applicable",
            })
        if qualifying >= 1:
            return self._pass({
                "comparison_intent": True,
                "qualifying_tables": qualifying,
            })
        return self._fail({
            "comparison_intent": True,
            "qualifying_tables": 0,
            "total_tables": int(tables.get("total", 0)),
        })


class RuleAEO054AuthoritativeSources(BaseRule):
    RULE_ID  = "AEO-054"
    HUB, CARD, SEVERITY = "aeo", "snippet_score", "medium"
    ISSUE_TITLE       = "No Authoritative External Sources Cited"
    ISSUE_DESCRIPTION = "This page makes claims but cites no authoritative external sources (.gov, .edu, recognised industry authorities). AI quality evaluators weight citations as credibility signals."
    RECOMMENDATION    = "Add 1-3 outbound links to authoritative sources that directly support your page's key claims."
    EXPECTED_IMPACT   = "medium"
    _MIN_AUTHORITY = 1

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        ext = page.get("content", {}).get("external_links", {})
        authority_count = int(ext.get("authority", 0))
        total_external  = int(ext.get("total", 0))
        examples        = ext.get("authority_examples", [])
        if authority_count >= self._MIN_AUTHORITY:
            return self._pass({
                "authority_links": authority_count,
                "total_external": total_external,
                "examples": examples,
            })
        return self._fail({
            "authority_links": 0,
            "total_external": total_external,
        })


RULES = [
    RuleAEO051Lists(),
    RuleAEO052ComparisonTables(),
    RuleAEO054AuthoritativeSources(),
]
