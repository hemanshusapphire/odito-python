"""
Voice & Intent Score Category Rules

Evaluates content optimization for voice search and user intent.
Focuses on conversational language, intent matching, and voice search readiness.
"""

import json
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class BulletNumberedListsUsedRule(BaseRule):
    """Rule 51 — Bullet / numbered lists are present in the content.

    Previously checked H2/H3 counts and short-paragraph ratios — headings are
    NOT lists.  This produced high false positives (pages with multiple H2s but
    no lists scored 8/10).  Fixed to use ai_visibility_signals.lists when
    available, with a content-structure fallback using short_paragraph_ratio and
    HowTo/ItemList schema signals as proxies.
    """

    def __init__(self):
        config = {
            "rule_id": "bullet_numbered_lists_used",
            "category": "voice_intent",
            "description": "Bullet / numbered lists present in content",
            "weight": 1.8,
            "max_score": 10,
            "applies_to": "page",
        }
        super().__init__(config)

    def evaluate(self, data: Dict[str, Any]) -> float:
        # Tier 1: real list detection from old pipeline
        ai_signals = data.get("ai_visibility_signals", {})
        lists = ai_signals.get("lists", {})
        if lists:
            total_items = lists.get("total_items", 0) or lists.get("item_count", 0)
            list_count  = lists.get("list_count", 0)
            if total_items >= 10 or list_count >= 3:  return 10.0
            if total_items >= 5  or list_count >= 2:  return  7.0
            if total_items >= 1  or list_count >= 1:  return  4.0
            return 0.0

        # Tier 2: schema ItemList / HowTo as list-content proxy
        graph = data.get("structured_data", {}).get("@graph", [])
        schema_list_signal = 0
        for item in graph:
            raw_type = item.get("@type", "")
            types = raw_type if isinstance(raw_type, list) else [raw_type]
            if "ItemList" in types:
                items = item.get("itemListElement", [])
                schema_list_signal = max(schema_list_signal, len(items) if isinstance(items, list) else 1)
            if "HowTo" in types:
                steps = item.get("step", [])
                schema_list_signal = max(schema_list_signal, len(steps) if isinstance(steps, list) else 1)
        if schema_list_signal >= 5: return 8.0
        if schema_list_signal >= 2: return 5.0
        if schema_list_signal >= 1: return 3.0

        # Tier 3: short-paragraph ratio + step_metrics as weakest proxy
        content_metrics = data.get("content_metrics", {})
        step_metrics    = data.get("step_metrics", {})
        score = 0
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if short_ratio >= 0.4:   score += 4
        elif short_ratio >= 0.2: score += 2
        elif short_ratio >= 0.1: score += 1
        if step_metrics.get("howto_schema_present", False):
            score = min(score + 3, self.max_score)
        elif step_metrics.get("steps_detected", 0) >= 3:
            score = min(score + 2, self.max_score)
        return min(float(score), self.max_score)

class ComparisonTablesPresentRule(BaseRule):
    """Rule 52 — Comparison tables are present in the content.

    Previously always returned 0 in the normalized pipeline because
    ai_visibility_signals was not populated by to_legacy_aliases.  Fixed with
    a two-tier fallback: real table data from ai_visibility_signals when
    available, then schema Table/ItemList/Dataset types as a secondary proxy.
    """

    def __init__(self):
        config = {
            "rule_id": "comparison_tables_present",
            "category": "voice_intent",
            "description": "Comparison tables present in page content",
            "weight": 1.7,
            "max_score": 10,
            "applies_to": "page",
        }
        super().__init__(config)

    def evaluate(self, data: Dict[str, Any]) -> float:
        # Tier 1: real HTML table detection (old pipeline)
        ai_signals = data.get("ai_visibility_signals", {})
        tables = ai_signals.get("tables", {})
        if tables:
            if not tables.get("present", False):
                return 0.0
            rows    = tables.get("rows", 0)
            headers = tables.get("headers", 0)
            if rows >= 3 and headers >= 2:
                return 10.0   # Substantial table with headers and data rows
            if rows >= 1:
                return 7.0    # Simple table
            return 5.0        # Table element detected but no row data

        # Tier 2: schema types that imply tabular/structured data
        graph = data.get("structured_data", {}).get("@graph", [])
        _TABLE_TYPES = {"Table", "DataTable", "Dataset", "ItemList"}
        for item in graph:
            raw_type = item.get("@type", "")
            types = set(raw_type if isinstance(raw_type, list) else [raw_type])
            if types & _TABLE_TYPES:
                return 6.0   # Schema indicates tabular data; can't verify HTML

        return 0.0

class StepByStepContentRule(BaseRule):
    """Rule 55 — Step-by-step / how-to content.

    Previously permanently capped at 2/10 because step_metrics was hardcoded
    to {"step_section_present": False} in to_legacy_aliases — steps_detected
    and howto_detected were always missing keys that defaulted to 0/False.

    Fixed: to_legacy_aliases now provides steps_detected (substantial_sections
    proxy) and howto_schema_present (HowTo @type check).  This rule consumes
    those real signals.
    """

    def __init__(self):
        config = {
            "rule_id": "step_by_step_content",
            "category": "voice_intent",
            "description": "Step-by-step or how-to content structure present",
            "weight": 1.8,
            "max_score": 10,
            "applies_to": "page",
        }
        super().__init__(config)

    def evaluate(self, data: Dict[str, Any]) -> float:
        step_metrics    = data.get("step_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        score = 0

        # HowTo schema is the strongest single signal
        if step_metrics.get("howto_schema_present", False):
            score += 5
        elif step_metrics.get("howto_detected", False):
            score += 3

        # Substantial content sections as sequential-step proxy
        steps = step_metrics.get("steps_detected", 0)
        if steps >= 4:   score += 3
        elif steps >= 2: score += 2
        elif steps >= 1: score += 1

        # H3 depth implies sequential breakdown
        h3_count = heading_metrics.get("h3_count", 0)
        if h3_count >= 3 and score < self.max_score:
            score = min(score + 2, self.max_score)

        return min(float(score), self.max_score)

# Register Voice & Intent rules (3 pure AI visibility rules)
def register_voice_intent_rules(registry):
    """Register all Voice & Intent category rules"""
    registry.register(BulletNumberedListsUsedRule())
    registry.register(ComparisonTablesPresentRule())
    registry.register(StepByStepContentRule())
