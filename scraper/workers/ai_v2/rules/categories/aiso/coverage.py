"""
AISO — Coverage card rules (3 rules).

AISO-CV1  XML Sitemap Exists
AISO-CV2  Internal Linking Present
AISO-CV3  Minimum Content Depth
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAISOCV1XmlSitemap(BaseRule):
    RULE_ID  = "AISO-CV1"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "XML Sitemap Missing"
    ISSUE_DESCRIPTION = "No XML sitemap was found. AI crawlers and search engines rely on sitemaps to discover and prioritise your content."
    RECOMMENDATION    = "Create an XML sitemap at /sitemap.xml. Reference it in robots.txt with a Sitemap: directive and submit it in Google Search Console."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        sitemap = page.get("technical", {}).get("sitemap", {})
        exists  = bool(sitemap.get("exists", False))
        if exists:
            return self._pass({"sitemap_exists": True, "url": sitemap.get("url")})
        return self._fail({"sitemap_exists": False})


class RuleAISOCV2InternalLinking(BaseRule):
    RULE_ID  = "AISO-CV2"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Insufficient Internal Linking"
    ISSUE_DESCRIPTION = "This page has fewer than 3 internal links, limiting AI crawlers' ability to discover related content and understand topical structure."
    RECOMMENDATION    = "Add at least 3 contextually relevant internal links to related pages on your site."
    EXPECTED_IMPACT   = "medium"
    _MIN = 3

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        count = int(page.get("content", {}).get("internal_link_count", 0))
        if count >= self._MIN:
            return self._pass({"internal_link_count": count, "threshold": self._MIN})
        return self._fail({"internal_link_count": count, "threshold": self._MIN})


class RuleAISOCV3ContentDepth(BaseRule):
    RULE_ID  = "AISO-CV3"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Insufficient Content Depth"
    ISSUE_DESCRIPTION = "Page body content is under 300 words — too thin for AI systems to extract substantive answers or reliable citations."
    RECOMMENDATION    = "Expand the page to at least 300 words of substantive, topic-relevant body content."
    EXPECTED_IMPACT   = "medium"
    _MIN = 300

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        word_count = int(page.get("content", {}).get("word_count", 0))
        if word_count >= self._MIN:
            return self._pass({"word_count": word_count, "threshold": self._MIN})
        return self._fail({"word_count": word_count, "threshold": self._MIN})


RULES = [
    RuleAISOCV1XmlSitemap(),
    RuleAISOCV2InternalLinking(),
    RuleAISOCV3ContentDepth(),
]
