"""
AEO — Voice Search card rules (1 rule).

AEO-055  Speakable Schema
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAEO055SpeakableSchema(BaseRule):
    RULE_ID  = "AEO-055"
    HUB, CARD, SEVERITY = "aeo", "voice_search", "low"
    ISSUE_TITLE       = "Speakable Schema Missing"
    ISSUE_DESCRIPTION = "No Speakable schema found. Voice assistants cannot identify which sections of this page to read aloud for spoken AI responses."
    RECOMMENDATION    = "Add Speakable schema targeting your summary paragraph and H1. Use stable CSS selectors (e.g. '.article-summary', 'h1') not dynamic IDs."
    EXPECTED_IMPACT   = "low"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        speakable = page.get("schema", {}).get("speakable") or {}
        present   = bool(speakable.get("present", False))
        selectors = speakable.get("css_selectors", []) or []
        if present and selectors:
            return self._pass({
                "speakable_present": True,
                "css_selectors": selectors,
                "selector_count": len(selectors),
            })
        return self._fail({"speakable_present": False})


RULES = [
    RuleAEO055SpeakableSchema(),
]
