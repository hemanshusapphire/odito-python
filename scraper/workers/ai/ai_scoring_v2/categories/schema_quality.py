"""
Schema Quality Category Rules

Extracted from ai_impact.py as part of the 4-Hub Architecture split.
All rules retain category="ai_impact" during the transition period so
metric_mapper.py formulas remain valid.  Category key rename happens in T2.1.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Set

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

_VALID_ORG_TYPES: Set[str] = {
    "Organization", "Corporation",
    "MarketingAgency",
    "LocalBusiness", "ProfessionalService", "LegalService", "AccountingService",
    "FinancialService", "InsuranceAgency", "RealEstateAgent", "EmploymentAgency",
    "HomeAndConstructionBusiness", "Locksmith", "MovingCompany", "Plumber",
    "HVACBusiness", "Electrician",
    "MedicalOrganization", "Hospital", "MedicalClinic", "Dentist",
    "Pharmacy", "Physician", "Optician", "VeterinaryCare",
    "EducationalOrganization", "CollegeOrUniversity", "School",
    "HighSchool", "MiddleSchool", "ElementarySchool",
    "GovernmentOrganization", "GovernmentOffice", "NGO", "LibrarySystem",
    "NewsMediaOrganization", "RadioStation", "TelevisionStation",
    "Store", "FoodEstablishment", "Restaurant", "CafeOrCoffeeShop",
    "FastFoodRestaurant", "Bakery", "Winery", "Distillery", "Brewery",
    "LodgingBusiness", "Hotel", "Motel", "BedAndBreakfast", "Resort",
    "EntertainmentBusiness", "SportsOrganization", "SportsTeam",
    "PerformingGroup", "MusicGroup", "TheaterGroup",
    "SoftwareApplication",
    "WebSite",
}

_PRECISE_ORG_TYPES: Set[str] = {
    "MarketingAgency", "LegalService", "MedicalClinic", "Hospital",
    "CollegeOrUniversity", "NewsMediaOrganization", "NGO", "Restaurant",
    "Hotel", "Dentist", "Pharmacy", "FinancialService", "RealEstateAgent",
}


class PrimaryOrganizationSchemaRule(BaseRule):
    """Rule 1 — Primary Organization schema on homepage"""

    def __init__(self):
        super().__init__({
            "rule_id": "primary_organization_schema",
            "category": "ai_impact",
            "description": "Primary Organization schema on homepage",
            "weight": 1.5,
            "max_score": 10,
            "applies_to": "page",
            "is_required": True,
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}

        score = 0
        for item in structured_data.get("@graph", []):
            if item.get("@type") == "Organization":
                score += 6
                if item.get("name"):
                    score += 2
                if item.get("url"):
                    score += 2
                break
        return min(score, self.max_score)


class CorrectTypeRule(BaseRule):
    """Rule 2 — Correct @type covers the full schema.org org/entity taxonomy."""

    def __init__(self):
        super().__init__({
            "rule_id": "correct_type",
            "category": "ai_impact",
            "description": "Correct schema.org @type for the primary entity",
            "weight": 1.3,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        schema_sig = data.get("schema_signals", {})
        all_types: List[str] = schema_sig.get("all_types", [])

        if not all_types:
            for item in data.get("structured_data", {}).get("@graph", []):
                raw = item.get("@type", "")
                if isinstance(raw, list):
                    all_types.extend(raw)
                elif raw:
                    all_types.append(raw)

        if not all_types:
            return 0.0

        type_set = set(all_types)
        if type_set & _PRECISE_ORG_TYPES:
            return 10.0
        if type_set & _VALID_ORG_TYPES:
            return 8.0
        return 0.0


class SchemaValidJSONLDRule(BaseRule):
    """Rule 3 — Schema valid JSON-LD (no parse errors)."""

    def __init__(self):
        super().__init__({
            "rule_id": "schema_valid_jsonld",
            "category": "ai_impact",
            "description": "Schema valid JSON-LD (no parse errors)",
            "weight": 2.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": True,
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        schema_sig = data.get("schema_signals", {})
        if schema_sig:
            if not schema_sig.get("valid_json", True) or schema_sig.get("parse_errors", 0) > 0:
                return 0.0
            if not schema_sig.get("all_types"):
                return 5.0
            return 10.0

        graph = data.get("structured_data", {}).get("@graph", [])
        if not graph:
            return 5.0
        return 10.0


class NoPluginDuplicateSchemasRule(BaseRule):
    """Rule 34 — No duplicate singleton schema types."""

    _SINGLETON_TYPES = {"Organization", "WebSite", "WebPage", "BreadcrumbList"}

    def __init__(self):
        super().__init__({
            "rule_id": "no_plugin_duplicate_schemas",
            "category": "ai_impact",
            "description": "No duplicate singleton schema types (Organization, WebSite, etc.)",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        schema_sig = data.get("schema_signals", {})
        if schema_sig:
            duplicate_types: List[str] = schema_sig.get("duplicate_types", [])
            if not duplicate_types:
                return 10.0
            if len(duplicate_types) == 1:
                return 5.0
            return 0.0

        graph = data.get("structured_data", {}).get("@graph", [])
        type_counts: Dict[str, int] = {}
        for item in graph:
            raw = item.get("@type", "")
            types = raw if isinstance(raw, list) else [raw]
            for t in types:
                if t in self._SINGLETON_TYPES:
                    type_counts[t] = type_counts.get(t, 0) + 1
        true_dupes = [t for t, n in type_counts.items() if n > 1]
        if not true_dupes:
            return 10.0
        if len(true_dupes) == 1:
            return 5.0
        return 0.0


class SemanticHTMLTagsUsedRule(BaseRule):
    """Rule 49 — Valid heading hierarchy with H1 and H2 structure."""

    def __init__(self):
        super().__init__({
            "rule_id": "semantic_html_tags_used",
            "category": "ai_impact",
            "description": "Valid heading hierarchy with H1 and H2 structure",
            "weight": 1.8,
            "max_score": 10,
            "applies_to": "page",
        })

    def evaluate(self, data: Dict[str, Any]) -> float:
        heading_metrics = data.get("heading_metrics", {})
        score = 0

        h1_count = heading_metrics.get("h1_count", 0)
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)

        if h1_count == 1:
            score += 4
        elif h1_count > 1:
            score += 1

        if h2_count >= 3:
            score += 4
        elif h2_count >= 2:
            score += 3
        elif h2_count >= 1:
            score += 2

        if heading_metrics.get("heading_sequence_valid", False):
            score += 2

        if h3_count >= 2 and score < self.max_score:
            score = min(score + 1, self.max_score)

        return min(score, self.max_score)


def register_schema_quality_rules(registry) -> None:
    """Register all Schema Quality rules (sub-set of ai_impact)."""
    registry.register(PrimaryOrganizationSchemaRule())
    registry.register(CorrectTypeRule())
    registry.register(SchemaValidJSONLDRule())
    registry.register(NoPluginDuplicateSchemasRule())
    registry.register(SemanticHTMLTagsUsedRule())
