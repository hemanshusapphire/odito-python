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
from .page_type_matrix import SKIP_MATRIX, GEO_LOCALBUSINESS_RULES


# Hub → card ordering (determines display order and aggregation structure).
HUB_CARD_MAP: dict[str, list[str]] = {
    "aiso": ["crawlability", "citability", "authority", "coverage"],
    "aeo":  ["answer_readiness", "question_coverage", "faq_coverage", "snippet_score", "voice_search"],
    "geo":  ["entity_authority", "knowledge_graph_score", "brand_corroboration", "schema_coverage"],
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

    # ── Dependency checking ───────────────────────────────────────────────────

    def _check_dependencies(
        self, rule: BaseRule, resolved: dict[str, RuleResult]
    ) -> str | None:
        """
        Verify all DEPENDS_ON conditions against already-evaluated results.

        Returns None if all dependencies are satisfied, or a skip-reason string
        if any dependency is not met.

        Condition tokens (case-insensitive):
            PASS     — dependency must have result == "PASS"
            EXECUTED — dependency must have been run (result != "SKIPPED")
        """
        for dep_spec in rule.DEPENDS_ON:
            parts = dep_spec.split(":", 1)
            if len(parts) != 2:
                continue
            dep_id, condition = parts[0].strip(), parts[1].strip().upper()
            dep_result = resolved.get(dep_id)

            if dep_result is None:
                return (
                    f"Dependency {dep_id} has not been evaluated — "
                    f"check registration order in loader.py"
                )

            if condition == "PASS":
                if dep_result.result != "PASS":
                    return (
                        f"Skipped because prerequisite {dep_id} did not pass "
                        f"(was {dep_result.result})"
                    )
            elif condition == "EXECUTED":
                if dep_result.result == "SKIPPED":
                    return (
                        f"Skipped because prerequisite {dep_id} was itself skipped"
                    )

        return None

    def _check_page_type_skip(
        self, rule: BaseRule, page_type: str, page: dict[str, Any]
    ) -> str | None:
        """
        Return a skip-reason string if this rule should not evaluate on this
        page type.  Return None if the rule should proceed.

        Gate order:
          1. Domain-scope rules always bypass — never skipped by page type.
          2. Known page type (non-generic) — consult SKIP_MATRIX.
          3. Generic page type — schema-presence fallback for GEO LocalBusiness
             rules: skip unless LocalBusiness schema is actually present.
        """
        # Guard 1: domain-scope rules are never gated by page type.
        if rule.SCOPE == "domain":
            return None

        pt = (page_type or "generic").strip().lower()

        # Guard 2: known page type — consult SKIP_MATRIX.
        if pt != "generic":
            skip_set = SKIP_MATRIX.get(rule.RULE_ID)
            if skip_set and pt in skip_set:
                return (
                    f"Rule {rule.RULE_ID} not applicable to '{pt}' pages "
                    f"(page-type gate)"
                )
            return None

        # Guard 3: generic page — schema-presence fallback for GEO LocalBusiness
        # rules. Evaluate if LocalBusiness schema is present on this page;
        # skip otherwise to avoid false failures on non-local-business pages
        # whose URL did not match a known pattern.
        if rule.RULE_ID in GEO_LOCALBUSINESS_RULES:
            lb = (page.get("schema") or {}).get("local_business") or {}
            if not lb.get("present", False):
                return (
                    "Generic page with no LocalBusiness schema — "
                    "GEO local entity rules not applicable"
                )

        return None

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate_page(self, page: dict[str, Any]) -> dict[str, RuleResult]:
        """
        Run every registered rule against the ai_pages document, respecting
        DEPENDS_ON declarations and page-type applicability.

        Gate order per rule:
          1. Dependency gate  — DEPENDS_ON conditions
          2. Page-type gate   — SKIP_MATRIX + generic schema-presence fallback
          3. Rule evaluation  — rule.evaluate(page)

        Skipped rules receive result="SKIPPED" and are excluded from scoring
        and issue generation.

        Returns:
            dict mapping rule_id → RuleResult
        """
        page_type = (page.get("page_type") or "generic").strip().lower()

        results: dict[str, RuleResult] = {}
        for rule_id, rule in self._rules.items():
            # Gate 1 — dependency check
            skip_reason = self._check_dependencies(rule, results)
            if skip_reason is not None:
                results[rule_id] = rule._skip(skip_reason)
                continue

            # Gate 2 — page-type check
            skip_reason = self._check_page_type_skip(rule, page_type, page)
            if skip_reason is not None:
                results[rule_id] = rule._skip(skip_reason)
                continue

            try:
                results[rule_id] = rule.evaluate(page)
            except Exception as exc:
                # A rule crash must not abort the whole page evaluation.
                # Treat as FAIL with error evidence so issues are surfaced.
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
                    scope=rule.SCOPE,
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
