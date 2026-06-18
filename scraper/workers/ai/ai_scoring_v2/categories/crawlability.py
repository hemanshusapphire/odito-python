"""
Crawlability Category Rules

Extracted from ai_impact.py as part of the 4-Hub Architecture split.
Owns the robots.txt / indexability gate rule that drives the crawlability
gate in scoring_engine.py.  Rule retains category="ai_impact" during the
transition period — category key rename happens in T2.1.

IMPORTANT: scoring_engine.py checks rule_id="robots_txt_non_blocking" by
string match for the crawlability gate.  Do not change the rule_id.
"""

from __future__ import annotations

from typing import Any, Dict

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule


class RobotsTxtNonBlockingRule(BaseRule):
    """Rule 15 — Page is indexable and not blocked by robots.txt.

    Three-tier fallback:
      1. Full AI-crawler data from enhanced_technical_signals.
      2. technical_signals.indexable / followable from the normalised pipeline.
      3. Neutral partial score when no crawlability data is available.
    """

    def __init__(self):
        super().__init__({
            "rule_id": "robots_txt_non_blocking",
            "category": "ai_impact",
            "description": "Page is indexable and AI crawlers are not blocked",
            "weight": 2.5,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        enhanced = data.get("enhanced_technical_signals", {})
        crawlability = enhanced.get("crawlability", {})
        if crawlability:
            if not crawlability.get("robots_txt_accessible", True):
                return 0.0
            if crawlability.get("ai_crawlers_blocked", []):
                return 0.0
            return 10.0

        tech = data.get("technical_signals", {})
        if tech:
            if not tech.get("indexable", True):
                return 0.0
            if not tech.get("followable", True):
                return 5.0
            return 8.0

        return 5.0


def register_crawlability_rules(registry) -> None:
    """Register all Crawlability rules (sub-set of ai_impact)."""
    registry.register(RobotsTxtNonBlockingRule())
