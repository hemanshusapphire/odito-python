"""
AISO — Crawlability card rules (10 rules).

AISO-001  GPTBot Not Blocked
AISO-002  PerplexityBot Not Blocked
AISO-003  ClaudeBot Not Blocked
AISO-004  Google Extended Not Blocked
AISO-005  Bingbot Not Blocked
AISO-006  llms.txt Exists
AISO-007  llms.txt Accurate
AISO-008  GPTBot Explicitly Allowed
AISO-009  ClaudeBot Explicitly Allowed
AISO-010  PerplexityBot Explicitly Allowed
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAISO001GPTBot(BaseRule):
    RULE_ID  = "AISO-001"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "critical"
    ISSUE_TITLE       = "GPTBot Access Blocked"
    ISSUE_DESCRIPTION = "robots.txt is preventing OpenAI's GPTBot from crawling this page. ChatGPT and all GPT-powered products cannot cite your content."
    RECOMMENDATION    = "Remove the Disallow directive for GPTBot. If blocked by a wildcard rule, add: User-agent: GPTBot / Allow: / beneath the wildcard block."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        blocked = page.get("crawlability", {}).get("robots", {}).get("gptbot_blocked", False)
        if not blocked:
            return self._pass({"blocked": False, "user_agent": "GPTBot", "crawl_permitted": True})
        return self._fail({"blocked": True, "user_agent": "GPTBot"})


class RuleAISO002PerplexityBot(BaseRule):
    RULE_ID  = "AISO-002"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "critical"
    ISSUE_TITLE       = "PerplexityBot Access Blocked"
    ISSUE_DESCRIPTION = "PerplexityBot is blocked, removing your content from Perplexity AI's answer and citation pool entirely."
    RECOMMENDATION    = "Remove the PerplexityBot block from robots.txt, or add an explicit Allow: / override if blocked by a wildcard rule."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        blocked = page.get("crawlability", {}).get("robots", {}).get("perplexitybot_blocked", False)
        if not blocked:
            return self._pass({"blocked": False, "user_agent": "PerplexityBot", "crawl_permitted": True})
        return self._fail({"blocked": True, "user_agent": "PerplexityBot"})


class RuleAISO003ClaudeBot(BaseRule):
    RULE_ID  = "AISO-003"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "high"
    ISSUE_TITLE       = "ClaudeBot Access Blocked"
    ISSUE_DESCRIPTION = "Anthropic's ClaudeBot is blocked, reducing citation probability across Claude AI products."
    RECOMMENDATION    = "Remove the ClaudeBot block from robots.txt."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        blocked = page.get("crawlability", {}).get("robots", {}).get("claudebot_blocked", False)
        if not blocked:
            return self._pass({"blocked": False, "user_agent": "ClaudeBot", "crawl_permitted": True})
        return self._fail({"blocked": True, "user_agent": "ClaudeBot"})


class RuleAISO004GoogleExtended(BaseRule):
    RULE_ID  = "AISO-004"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "critical"
    ISSUE_TITLE       = "Google-Extended Blocked — AI Overview Visibility Eliminated"
    ISSUE_DESCRIPTION = "Google-Extended is blocked, removing this page from Google AI Overviews and Gemini — the highest-volume AI answer surface."
    RECOMMENDATION    = "Remove the Google-Extended block immediately. If blocked via wildcard, add: User-agent: Google-Extended / Allow: /."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        blocked = page.get("crawlability", {}).get("robots", {}).get("google_extended_blocked", False)
        if not blocked:
            return self._pass({"blocked": False, "user_agent": "Google-Extended", "crawl_permitted": True})
        return self._fail({"blocked": True, "user_agent": "Google-Extended"})


class RuleAISO005Bingbot(BaseRule):
    RULE_ID  = "AISO-005"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "high"
    ISSUE_TITLE       = "Bingbot Blocked — Microsoft Copilot Visibility Lost"
    ISSUE_DESCRIPTION = "Bingbot is blocked. Microsoft Copilot and all Bing AI products cannot cite your pages."
    RECOMMENDATION    = "Remove the Bingbot block from robots.txt."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        blocked = page.get("crawlability", {}).get("robots", {}).get("bingbot_blocked", False)
        if not blocked:
            return self._pass({"blocked": False, "user_agent": "Bingbot", "crawl_permitted": True})
        return self._fail({"blocked": True, "user_agent": "Bingbot"})


class RuleAISO006LlmsTxtExists(BaseRule):
    RULE_ID  = "AISO-006"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "medium"
    ISSUE_TITLE       = "llms.txt File Missing"
    ISSUE_DESCRIPTION = "No llms.txt found. AI crawlers lack a structured signal for which pages to prioritise."
    RECOMMENDATION    = "Create an llms.txt file at your domain root following the llmstxt.org specification."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        llms = page.get("crawlability", {}).get("llms_txt", {})
        exists = bool(llms.get("exists", False))
        if exists:
            return self._pass({
                "found": True,
                "location": llms.get("location"),
                "status_code": llms.get("status_code"),
                "byte_size": llms.get("byte_size"),
            })
        return self._fail({"found": False, "checked_paths": ["/llms.txt", "/llms-full.txt"]})


class RuleAISO007LlmsTxtAccurate(BaseRule):
    RULE_ID  = "AISO-007"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "medium"
    ISSUE_TITLE       = "llms.txt Contains Broken or Outdated References"
    ISSUE_DESCRIPTION = "Your llms.txt lists pages that no longer exist or are malformed, degrading AI crawler trust."
    RECOMMENDATION    = "Audit all URLs in llms.txt. Remove deleted pages and update moved ones. Run this review monthly."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        llms = page.get("crawlability", {}).get("llms_txt", {})
        if not llms.get("exists", False):
            return self._fail({"found": False, "reason": "llms.txt absent — cannot be accurate"})
        broken    = llms.get("broken_urls", [])
        malformed = bool(llms.get("malformed", False))
        if not broken and not malformed:
            return self._pass({
                "valid": True,
                "url_count": llms.get("url_count", 0),
                "broken_urls": [],
                "malformed": False,
            })
        return self._fail({
            "valid": False,
            "url_count": llms.get("url_count", 0),
            "broken_urls": broken,
            "malformed": malformed,
        })


class RuleAISO008GPTBotExplicitAllow(BaseRule):
    RULE_ID  = "AISO-008"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "medium"
    ISSUE_TITLE       = "GPTBot Not Explicitly Allowed"
    ISSUE_DESCRIPTION = "robots.txt does not contain a named GPTBot Allow directive. While GPTBot may not be blocked, an explicit allow signals deliberate AI-readiness to crawlers and auditors."
    RECOMMENDATION    = "Add a dedicated GPTBot stanza to robots.txt: User-agent: GPTBot / Allow: /"
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        explicit = page.get("crawlability", {}).get("robots", {}).get("gptbot_explicit_allow", False)
        if explicit:
            return self._pass({"user_agent": "GPTBot", "explicit_allow": True})
        return self._fail({"user_agent": "GPTBot", "explicit_allow": False})


class RuleAISO009ClaudeBotExplicitAllow(BaseRule):
    RULE_ID  = "AISO-009"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "medium"
    ISSUE_TITLE       = "ClaudeBot Not Explicitly Allowed"
    ISSUE_DESCRIPTION = "robots.txt does not contain a named ClaudeBot Allow directive. An explicit allow signals deliberate support for Anthropic's crawler."
    RECOMMENDATION    = "Add a dedicated ClaudeBot stanza to robots.txt: User-agent: ClaudeBot / Allow: /"
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        explicit = page.get("crawlability", {}).get("robots", {}).get("claudebot_explicit_allow", False)
        if explicit:
            return self._pass({"user_agent": "ClaudeBot", "explicit_allow": True})
        return self._fail({"user_agent": "ClaudeBot", "explicit_allow": False})


class RuleAISO010PerplexityBotExplicitAllow(BaseRule):
    RULE_ID  = "AISO-010"
    HUB, CARD, SEVERITY = "aiso", "crawlability", "medium"
    ISSUE_TITLE       = "PerplexityBot Not Explicitly Allowed"
    ISSUE_DESCRIPTION = "robots.txt does not contain a named PerplexityBot Allow directive. An explicit allow signals deliberate support for Perplexity AI's crawler."
    RECOMMENDATION    = "Add a dedicated PerplexityBot stanza to robots.txt: User-agent: PerplexityBot / Allow: /"
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        explicit = page.get("crawlability", {}).get("robots", {}).get("perplexitybot_explicit_allow", False)
        if explicit:
            return self._pass({"user_agent": "PerplexityBot", "explicit_allow": True})
        return self._fail({"user_agent": "PerplexityBot", "explicit_allow": False})


RULES = [
    RuleAISO001GPTBot(),
    RuleAISO002PerplexityBot(),
    RuleAISO003ClaudeBot(),
    RuleAISO004GoogleExtended(),
    RuleAISO005Bingbot(),
    RuleAISO006LlmsTxtExists(),
    RuleAISO007LlmsTxtAccurate(),
    RuleAISO008GPTBotExplicitAllow(),
    RuleAISO009ClaudeBotExplicitAllow(),
    RuleAISO010PerplexityBotExplicitAllow(),
]
