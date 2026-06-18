"""
AISO — Citability card rules (3 rules).

AISO-C1  Content Rendered Without JS Dependency
AISO-C2  Canonical Tag Present
AISO-C3  Indexable (not noindex)
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAISOC1JSDependency(BaseRule):
    RULE_ID  = "AISO-C1"
    HUB, CARD, SEVERITY = "aiso", "citability", "high"
    ISSUE_TITLE       = "Content Only Available via JavaScript Rendering"
    ISSUE_DESCRIPTION = "Key page content is only accessible after JavaScript execution. Most AI crawlers cannot render JS, making your content invisible to them."
    RECOMMENDATION    = "Ensure all substantive body content is present in the raw HTML response without requiring JavaScript execution. Use server-side rendering or static generation."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        tech = page.get("technical", {})
        js_only_raw = tech.get("js_rendered_only")
        # None means headless never ran — we cannot confirm content is in raw HTML.
        if js_only_raw is None:
            return self._fail({
                "js_rendered_only": None,
                "reason": "Headless rendering not performed — JS dependency cannot be verified",
            })
        if not bool(js_only_raw):
            return self._pass({"js_rendered_only": False, "content_in_html": True})
        return self._fail({"js_rendered_only": True, "content_in_html": False})


class RuleAISOC2CanonicalTag(BaseRule):
    RULE_ID  = "AISO-C2"
    HUB, CARD, SEVERITY = "aiso", "citability", "medium"
    ISSUE_TITLE       = "Canonical Tag Missing"
    ISSUE_DESCRIPTION = "No canonical tag found. AI crawlers and search engines cannot identify the authoritative version of this page, risking duplicate content signals."
    RECOMMENDATION    = 'Add a self-referencing canonical tag in the <head>: <link rel="canonical" href="[this-page-url]">.'
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        tech = page.get("technical", {})
        present = bool(tech.get("canonical_present", False))
        canonical_url = tech.get("canonical_url")
        if present:
            return self._pass({"canonical_present": True, "canonical_url": canonical_url})
        return self._fail({"canonical_present": False, "canonical_url": None})


class RuleAISOC3Indexable(BaseRule):
    RULE_ID  = "AISO-C3"
    HUB, CARD, SEVERITY = "aiso", "citability", "critical"
    ISSUE_TITLE       = "Page Marked noindex — Excluded from AI Indexing"
    ISSUE_DESCRIPTION = "A noindex directive is present on this page. It is explicitly excluded from being indexed by search engines and AI crawlers."
    RECOMMENDATION    = "Remove the noindex meta tag or X-Robots-Tag header if this page should be discoverable. Only use noindex on pages that must remain private."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        noindex = bool(page.get("technical", {}).get("noindex", False))
        if not noindex:
            return self._pass({"indexable": True, "noindex": False})
        return self._fail({"indexable": False, "noindex": True})


RULES = [
    RuleAISOC1JSDependency(),
    RuleAISOC2CanonicalTag(),
    RuleAISOC3Indexable(),
]
