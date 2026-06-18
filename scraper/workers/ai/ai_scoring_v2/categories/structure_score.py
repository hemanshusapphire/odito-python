"""
Structure Score Rules

Extracted from llm_readiness.py as part of the 4-Hub Architecture split.
Covers content format signals: description quality and semantic subtopic depth.

All rules retain category="llm_readiness" during the transition period.
Category key rename happens in T2.1.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule


class DescriptionMinimum50CharactersRule(BaseRule):
    """Rule 32 — Schema description minimum 50 characters."""

    def __init__(self):
        super().__init__({
            "rule_id": "description_minimum_50_characters",
            "category": "llm_readiness",
            "description": "Schema description minimum 50 characters",
            "weight": 0.9,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}

        for item in structured_data.get("@graph", []):
            description = item.get("description", "")
            if len(description) >= 50:
                return 10.0
            if len(description) >= 30:
                return 5.0
            if len(description) >= 10:
                return 2.0
        return 0.0


class SemanticSubtopicsCoveredRule(BaseRule):
    """Rule 44 — Semantic subtopics covered via heading depth and entity diversity."""

    def __init__(self):
        super().__init__({
            "rule_id": "semantic_subtopics_covered",
            "category": "llm_readiness",
            "description": "Semantic subtopics covered",
            "weight": 1.6,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        heading_metrics = data.get("heading_metrics", {})
        entity_metrics  = data.get("entity_metrics", {})
        score = 0

        h3_count = heading_metrics.get("h3_count", 0)
        if h3_count >= 3:   score += 5
        elif h3_count >= 2: score += 3
        elif h3_count >= 1: score += 1

        unique_types = entity_metrics.get("unique_entity_types", 0)
        if unique_types >= 4:   score += 3
        elif unique_types >= 2: score += 2
        elif unique_types >= 1: score += 1

        h2_count = heading_metrics.get("h2_count", 0)
        if h2_count >= 2:
            score += 2

        return min(score, self.max_score)


def register_structure_score_rules(registry) -> None:
    """Register all Structure Score rules (sub-set of llm_readiness)."""
    registry.register(DescriptionMinimum50CharactersRule())
    registry.register(SemanticSubtopicsCoveredRule())
