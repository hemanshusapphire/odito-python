"""
Citability Score Rules

Extracted from llm_readiness.py as part of the 4-Hub Architecture split.
Covers content depth, entity signals, authorship, and citation authority —
the signals that determine whether AI systems will cite a page.

All rules retain category="llm_readiness" during the transition period.
Category key rename happens in T2.1.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule


class ServicePages800WordsRule(BaseRule):
    """Rule 22 — Content depth: sufficient word count for the page type."""

    _THRESHOLDS: Dict[str, tuple] = {
        "Service":      (800, 600, 400, 200),
        "BlogPost":     (600, 400, 250, 100),
        "Article":      (600, 400, 250, 100),
        "NewsArticle":  (400, 250, 150,  75),
        "Product":      (300, 200, 100,  50),
        "HomePage":     (300, 200, 100,  50),
        "LandingPage":  (400, 250, 150,  75),
        "Unknown":      (400, 300, 150,  75),
    }

    def __init__(self):
        super().__init__({
            "rule_id": "service_pages_800_words",
            "category": "llm_readiness",
            "description": "Sufficient content depth (word count) for the page type",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        content_metrics = data.get("content_metrics", {})
        page_type_props = data.get("page_type_properties", {})
        detected_type = page_type_props.get("detected_type", "Unknown")
        word_count = content_metrics.get("word_count", 0)

        hi, mid, lo, min_t = self._THRESHOLDS.get(detected_type, self._THRESHOLDS["Unknown"])

        if word_count >= hi:    return 10.0
        if word_count >= mid:   return  6.0
        if word_count >= lo:    return  3.0
        if word_count >= min_t: return  1.0
        return 0.0


class TopicClustersInternalLinksRule(BaseRule):
    """Rule 23 — Topic clustering via internal links and content depth."""

    def __init__(self):
        super().__init__({
            "rule_id": "topic_clusters_internal_links",
            "category": "llm_readiness",
            "description": "Content has topical depth with internal links to related pages",
            "weight": 1.1,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        heading_metrics = data.get("heading_metrics", {})
        entity_metrics  = data.get("entity_metrics", {})
        link_sig        = data.get("link_signals", {})
        score = 0

        if link_sig:
            topic_links  = link_sig.get("topic_cluster_links", 0)
            contextual   = link_sig.get("contextual_internal", 0)
            has_clusters = link_sig.get("has_topic_clusters", False)

            if has_clusters or topic_links >= 3:
                score += 6
            elif contextual >= 5:
                score += 4
            elif contextual >= 2:
                score += 3
            elif link_sig.get("internal", 0) >= 3:
                score += 1
        else:
            h2_count = heading_metrics.get("h2_count", 0)
            if h2_count >= 3:   score += 3
            elif h2_count >= 2: score += 2
            elif h2_count >= 1: score += 1

        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if entity_density >= 5:   score += 3
        elif entity_density >= 3: score += 2
        elif entity_density >= 1: score += 1

        h3_count = heading_metrics.get("h3_count", 0)
        if h3_count >= 2 and score < self.max_score:
            score = min(score + 1, self.max_score)

        return min(score, self.max_score)


class ClearEntityFirst150WordsRule(BaseRule):
    """Rule 29 — Clear entity in first 150 words."""

    def __init__(self):
        super().__init__({
            "rule_id": "clear_entity_first_150_words",
            "category": "llm_readiness",
            "description": "Clear entity in first 150 words",
            "weight": 1.8,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        entity_graph = data.get("unified_entity_graph", {})
        content_metrics = data.get("content_metrics", {})
        score = 0

        if entity_graph.get("primary_entity"):
            score += 6

        primary_mentions = entity_graph.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)

        if word_count > 0:
            mention_ratio = primary_mentions / word_count * 1000
            if mention_ratio >= 10:
                score += 4
            elif mention_ratio >= 5:
                score += 2
            elif mention_ratio >= 2:
                score += 1

        return min(score, self.max_score)


class SameAsArrayLinksActiveRule(BaseRule):
    """Rule 30 — sameAs array links active."""

    def __init__(self):
        super().__init__({
            "rule_id": "sameas_array_links_active",
            "category": "llm_readiness",
            "description": "sameAs array links active",
            "weight": 1.8,
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

        sameas_count = 0
        for item in structured_data.get("@graph", []):
            if item.get("sameAs"):
                sameas = item["sameAs"]
                sameas_count += len(sameas) if isinstance(sameas, list) else 1

        if sameas_count >= 3:   return 10.0
        if sameas_count >= 2:   return  6.0
        if sameas_count >= 1:   return  3.0
        return 0.0


class LastUpdatedDateVisibleRule(BaseRule):
    """Rule 43 — Last updated date visible."""

    def __init__(self):
        super().__init__({
            "rule_id": "last_updated_date_visible",
            "category": "llm_readiness",
            "description": "Last updated date visible",
            "weight": 1.7,
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
            if item.get("dateModified"):
                return 10.0
            if item.get("datePublished"):
                return 5.0
        return 0.0


class StatisticsHaveSourceLinksRule(BaseRule):
    """Rule 45 — Content links to authoritative external sources."""

    def __init__(self):
        super().__init__({
            "rule_id": "statistics_have_source_links",
            "category": "llm_readiness",
            "description": "Content links to authoritative external sources",
            "weight": 1.5,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        score = 0

        link_sig = data.get("link_signals", {})
        external = link_sig.get("external", 0)
        if external >= 5:   score += 7
        elif external >= 3: score += 5
        elif external >= 1: score += 3

        graph = data.get("structured_data", {}).get("@graph", [])
        _CITATION_TYPES = {"Citation", "Dataset", "ScholarlyArticle", "ResearchProject"}
        for item in graph:
            raw_type = item.get("@type", "")
            types = set(raw_type if isinstance(raw_type, list) else [raw_type])
            if types & _CITATION_TYPES:
                score = min(score + 3, self.max_score)
                break
            if item.get("citation") or item.get("isPartOf"):
                score = min(score + 2, self.max_score)
                break

        return min(float(score), self.max_score)


def register_citability_score_rules(registry) -> None:
    """Register all Citability Score rules (sub-set of llm_readiness)."""
    registry.register(ServicePages800WordsRule())
    registry.register(TopicClustersInternalLinksRule())
    registry.register(ClearEntityFirst150WordsRule())
    registry.register(SameAsArrayLinksActiveRule())
    registry.register(LastUpdatedDateVisibleRule())
    registry.register(StatisticsHaveSourceLinksRule())
