"""
V2 Rule Registry.

Holds all registered rules organised by hub → card → rule_id.
Evaluates a page against every rule and returns structured results
consumed by the scoring layer.

Hub / card taxonomy:
    aiso
        crawlability   AISO-001 … AISO-010
        citability     AISO-C1  … AISO-C3
        authority      AISO-A1  … AISO-A3
        coverage       AISO-CV1 … AISO-CV3
    aeo
        answer_readiness   AEO-046, AEO-050, AEO-053
        question_coverage  AEO-047, AEO-049
        faq_coverage       AEO-048
        snippet_score      AEO-051, AEO-052, AEO-054
        voice_search       AEO-055
    geo
        entity_authority       GEO-059, GEO-060, GEO-061
        knowledge_graph_score  GEO-062, GEO-063
        brand_corroboration    GEO-065, GEO-G1, GEO-G2, GEO-G3
"""

from typing import Any
from .base import BaseRule, RuleResult


# Hub → card ordering (determines display order and aggregation structure).
HUB_CARD_MAP: dict[str, list[str]] = {
    "aiso": ["crawlability", "citability", "authority", "coverage"],
    "aeo":  ["answer_readiness", "question_coverage", "faq_coverage", "snippet_score", "voice_search"],
    "geo":  ["entity_authority", "knowledge_graph_score", "brand_corroboration"],
}


class RuleRegistry:
    """
    Registers rules and evaluates them against a page document.

    Usage:
        registry = RuleRegistry()
        registry.register(MyRule())
        results = registry.evaluate_page(ai_page_doc)
    """

    def __init__(self):
        # rule_id → BaseRule instance
        self._rules: dict[str, BaseRule] = {}
        # hub → card → [rule_id, ...]
        self._index: dict[str, dict[str, list[str]]] = {
            hub: {card: [] for card in cards}
            for hub, cards in HUB_CARD_MAP.items()
        }

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self, rule: BaseRule) -> None:
        """Register a single rule instance."""
        if not rule.RULE_ID:
            raise ValueError(f"Rule {rule.__class__.__name__} has no RULE_ID")
        if rule.RULE_ID in self._rules:
            raise ValueError(f"Duplicate RULE_ID: {rule.RULE_ID}")

        hub  = rule.HUB.lower()
        card = rule.CARD.lower()

        if hub not in self._index:
            raise ValueError(f"Unknown hub '{hub}' for rule {rule.RULE_ID}")
        if card not in self._index[hub]:
            raise ValueError(f"Unknown card '{card}' for hub '{hub}', rule {rule.RULE_ID}")

        self._rules[rule.RULE_ID] = rule
        self._index[hub][card].append(rule.RULE_ID)

    def register_all(self, rules: list[BaseRule]) -> None:
        for rule in rules:
            self.register(rule)

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate_page(self, page: dict[str, Any]) -> dict[str, RuleResult]:
        """
        Run every registered rule against the ai_pages document.

        Returns:
            dict mapping rule_id → RuleResult
        """
        results: dict[str, RuleResult] = {}
        for rule_id, rule in self._rules.items():
            try:
                results[rule_id] = rule.evaluate(page)
            except Exception as exc:
                # A rule crash must not abort the whole page evaluation.
                # Treat as FAIL with error evidence.
                results[rule_id] = RuleResult(
                    rule_id=rule_id,
                    hub=rule.HUB,
                    card=rule.CARD,
                    result="FAIL",
                    evidence={"error": str(exc), "rule_crashed": True},
                    severity=rule.SEVERITY,
                    issue_title=rule.ISSUE_TITLE,
                    issue_description=rule.ISSUE_DESCRIPTION,
                    recommendation=rule.RECOMMENDATION,
                    expected_impact=rule.EXPECTED_IMPACT,
                )
        return results

    # ── Introspection ─────────────────────────────────────────────────────────

    def get_rules_for_card(self, hub: str, card: str) -> list[str]:
        """Return the ordered list of rule_ids for a given hub/card."""
        return self._index.get(hub, {}).get(card, [])

    def get_all_rule_ids(self) -> list[str]:
        return list(self._rules.keys())

    def total_rules(self) -> int:
        return len(self._rules)

    def summary(self) -> dict[str, Any]:
        """Return a human-readable registry summary (hub → card → count)."""
        out: dict[str, Any] = {"total": self.total_rules(), "hubs": {}}
        for hub, cards in self._index.items():
            out["hubs"][hub] = {
                card: len(ids) for card, ids in cards.items()
            }
        return out


# ── Module-level singleton populated by loader.py ─────────────────────────────
_registry: RuleRegistry | None = None


def get_registry() -> RuleRegistry:
    """Return the pre-loaded singleton registry (populated by loader.py)."""
    if _registry is None:
        raise RuntimeError(
            "Rule registry not initialised. Call loader.load_all_rules() first."
        )
    return _registry


def _set_registry(registry: RuleRegistry) -> None:
    """Internal — called by loader.py after registering all rules."""
    global _registry
    _registry = registry
