"""
AI Accessibility Category Rules

Domain-level crawler accessibility rules owned by the AI Visibility Worker.

These rules read per-bot signals from `domain_signals` injected into page_data
by the worker before scoring.  Because all pages of a domain share the same
domain signals, every page receives the same ai_accessibility category score.

Signal-quality scoring model
-----------------------------
Explicitly Allowed  (explicitlyAllowed=True)           → 100
Mentioned but not blocked (mentioned=True, not blocked) →  80
Accessible via wildcard (inheritsWildcard=True)        →  60
No robots.txt       (robots_exists=False)              →  50
Unclear/no data     (signal empty)                     →  30
Explicitly Blocked  (explicitlyBlocked=True)           →   0

llms.txt is NOT a rule here.  It is applied as a post-processing bonus
in the worker after the category score is computed (0 penalty when absent,
+5 or +10 when present).
"""

from __future__ import annotations

from typing import Any, Dict

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule


def _signal_quality_score(signal: Dict[str, Any], robots_exists) -> float:
    """
    Translate a single bot signal into a numeric quality score.

    Parameters
    ----------
    signal        Single-bot dict from aiCrawlerSignals (may be empty/None).
    robots_exists True = robots.txt existed, False = not found, None = unknown.
    """
    if not signal:
        return 30.0  # No extraction data at all

    if signal.get("explicitlyBlocked"):
        return 0.0

    if signal.get("explicitlyAllowed"):
        return 100.0

    if signal.get("mentioned") and not signal.get("explicitlyBlocked"):
        # Mentioned with only sub-path restrictions → configured but not fully open
        return 80.0

    if robots_exists is False:
        # No robots.txt → implicit allow (no AI-specific configuration)
        return 50.0

    if signal.get("inheritsWildcard") and signal.get("accessible"):
        # robots.txt exists but bot relies on wildcard
        return 60.0

    if signal.get("accessible"):
        return 60.0

    return 30.0  # Accessible state unclear


class GPTBotAccessibleRule(BaseRule):
    """GPTBot (OpenAI) crawler accessibility."""

    def __init__(self):
        super().__init__({
            "rule_id": "gptbot_accessible",
            "category": "ai_accessibility",
            "description": "GPTBot is accessible — OpenAI can crawl this domain",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        domain = data.get("domain_signals", {})
        signals = domain.get("aiCrawlerSignals", {})
        robots_exists = domain.get("robotsExists")
        return _signal_quality_score(signals.get("gptbot", {}), robots_exists) / 10.0


class ClaudeBotAccessibleRule(BaseRule):
    """ClaudeBot (Anthropic) crawler accessibility."""

    def __init__(self):
        super().__init__({
            "rule_id": "claudebot_accessible",
            "category": "ai_accessibility",
            "description": "ClaudeBot is accessible — Anthropic can crawl this domain",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        domain = data.get("domain_signals", {})
        signals = domain.get("aiCrawlerSignals", {})
        robots_exists = domain.get("robotsExists")
        return _signal_quality_score(signals.get("claudebot", {}), robots_exists) / 10.0


class DeepSeekBotAccessibleRule(BaseRule):
    """DeepSeekBot (DeepSeek) crawler accessibility."""

    def __init__(self):
        super().__init__({
            "rule_id": "deepseek_accessible",
            "category": "ai_accessibility",
            "description": "DeepSeekBot is accessible — DeepSeek can crawl this domain",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        domain = data.get("domain_signals", {})
        signals = domain.get("aiCrawlerSignals", {})
        robots_exists = domain.get("robotsExists")
        return _signal_quality_score(signals.get("deepseek", {}), robots_exists) / 10.0


class PerplexityBotAccessibleRule(BaseRule):
    """PerplexityBot crawler accessibility."""

    def __init__(self):
        super().__init__({
            "rule_id": "perplexitybot_accessible",
            "category": "ai_accessibility",
            "description": "PerplexityBot is accessible — Perplexity can crawl this domain",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        domain = data.get("domain_signals", {})
        signals = domain.get("aiCrawlerSignals", {})
        robots_exists = domain.get("robotsExists")
        return _signal_quality_score(signals.get("perplexitybot", {}), robots_exists) / 10.0


class GoogleExtendedAccessibleRule(BaseRule):
    """Google-Extended (Gemini / AI Overviews) crawler accessibility."""

    def __init__(self):
        super().__init__({
            "rule_id": "google_extended_accessible",
            "category": "ai_accessibility",
            "description": "Google-Extended is accessible — Gemini and AI Overviews can use this content",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        domain = data.get("domain_signals", {})
        signals = domain.get("aiCrawlerSignals", {})
        robots_exists = domain.get("robotsExists")
        return _signal_quality_score(signals.get("googleExtended", {}), robots_exists) / 10.0


def register_ai_accessibility_rules(registry):
    """Register all AI Accessibility rules."""
    registry.register(GPTBotAccessibleRule())
    registry.register(ClaudeBotAccessibleRule())
    registry.register(DeepSeekBotAccessibleRule())
    registry.register(PerplexityBotAccessibleRule())
    registry.register(GoogleExtendedAccessibleRule())
