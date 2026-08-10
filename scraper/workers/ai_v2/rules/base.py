"""
V2 BaseRule.

Every rule in V2:
  - extends BaseRule
  - declares class-level metadata (RULE_ID, HUB, CARD, SEVERITY, etc.)
  - implements evaluate(page: dict) -> RuleResult

There are NO scores, weights, normalization, or category averages.
evaluate() returns exactly PASS, FAIL, or SKIPPED with structured evidence.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Any


Verdict = Literal["PASS", "FAIL", "SKIPPED"]
Scope   = Literal["page", "domain"]


@dataclass
class RuleResult:
    """Immutable output of a single rule evaluation."""
    rule_id:     str
    hub:         str
    card:        str
    result:      Verdict
    evidence:    dict[str, Any] = field(default_factory=dict)
    severity:    str = "medium"
    issue_title: str = ""
    issue_description: str = ""
    recommendation:    str = ""
    expected_impact:   str = ""
    scope:         str = "page"   # "page" | "domain"
    skipped_reason: str = ""

    @property
    def passed(self) -> bool:
        return self.result == "PASS"

    @property
    def skipped(self) -> bool:
        return self.result == "SKIPPED"

    def to_dict(self) -> dict[str, Any]:
        d = {
            "rule_id":          self.rule_id,
            "hub":              self.hub,
            "card":             self.card,
            "result":           self.result,
            "evidence":         self.evidence,
            "severity":         self.severity,
            "issue_title":      self.issue_title,
            "issue_description": self.issue_description,
            "recommendation":   self.recommendation,
            "expected_impact":  self.expected_impact,
            "scope":            self.scope,
        }
        if self.skipped_reason:
            d["skipped_reason"] = self.skipped_reason
        return d


class BaseRule(ABC):
    """
    Abstract base for all V2 rules.

    Subclasses MUST declare:
        RULE_ID            str  — unique identifier e.g. "AISO-001"
        HUB                str  — "aiso" | "aeo" | "geo"
        CARD               str  — "crawlability" | "answer_readiness" | ...
        SEVERITY           str  — "critical" | "high" | "medium" | "low"
        ISSUE_TITLE        str
        ISSUE_DESCRIPTION  str
        RECOMMENDATION     str
        EXPECTED_IMPACT    str

    Subclasses MAY declare:
        DEPENDS_ON         list[str]  — e.g. ["AISO-006:PASS", "AISO-007:EXECUTED"]

    Subclasses MUST implement:
        evaluate(page: dict) -> RuleResult
    """

    RULE_ID:           str = ""
    HUB:               str = ""
    CARD:              str = ""
    SEVERITY:          str = "medium"
    SCOPE:             str = "page"   # "page" | "domain"
    ISSUE_TITLE:       str = ""
    ISSUE_DESCRIPTION: str = ""
    RECOMMENDATION:    str = ""
    EXPECTED_IMPACT:   str = ""
    DEPENDS_ON:        list = []  # list[str] — "RULE_ID:CONDITION" specs

    # ── Factory helpers ──────────────────────────────────────────────────────

    def _pass(self, evidence: dict[str, Any]) -> RuleResult:
        return RuleResult(
            rule_id=self.RULE_ID,
            hub=self.HUB,
            card=self.CARD,
            result="PASS",
            evidence=evidence,
            severity=self.SEVERITY,
            issue_title=self.ISSUE_TITLE,
            issue_description=self.ISSUE_DESCRIPTION,
            recommendation=self.RECOMMENDATION,
            expected_impact=self.EXPECTED_IMPACT,
            scope=self.SCOPE,
        )

    def _fail(self, evidence: dict[str, Any]) -> RuleResult:
        return RuleResult(
            rule_id=self.RULE_ID,
            hub=self.HUB,
            card=self.CARD,
            result="FAIL",
            evidence=evidence,
            severity=self.SEVERITY,
            issue_title=self.ISSUE_TITLE,
            issue_description=self.ISSUE_DESCRIPTION,
            recommendation=self.RECOMMENDATION,
            expected_impact=self.EXPECTED_IMPACT,
            scope=self.SCOPE,
        )

    def _skip(self, reason: str) -> RuleResult:
        return RuleResult(
            rule_id=self.RULE_ID,
            hub=self.HUB,
            card=self.CARD,
            result="SKIPPED",
            evidence={},
            severity=self.SEVERITY,
            issue_title=self.ISSUE_TITLE,
            issue_description=self.ISSUE_DESCRIPTION,
            recommendation=self.RECOMMENDATION,
            expected_impact=self.EXPECTED_IMPACT,
            scope=self.SCOPE,
            skipped_reason=reason,
        )

    # ── Interface ─────────────────────────────────────────────────────────────

    @abstractmethod
    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        """
        Evaluate this rule against the ai_pages document.

        Args:
            page: full ai_pages document for one URL.

        Returns:
            RuleResult with result="PASS", "FAIL", or "SKIPPED" and structured evidence.
        """

    def __repr__(self) -> str:
        return f"<Rule {self.RULE_ID} [{self.HUB}/{self.CARD}] severity={self.SEVERITY}>"
