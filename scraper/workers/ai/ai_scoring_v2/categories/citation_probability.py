"""
Citation Probability Score Category Rules

Evaluates likelihood of content being cited by AI systems and other sources.
Focuses on authority, trustworthiness, and citation-worthy content elements.
"""

import json
from typing import Any, Dict

from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule


class NoEntityFragmentationRule(BaseRule):
    """Rule 11 — No entity fragmentation"""
    
    def __init__(self):
        config = {
            "rule_id": "no_entity_fragmentation",
            "category": "citation_probability",
            "description": "No entity fragmentation",
            "weight": 1.8,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for entity fragmentation"""
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for primary entity (indicates no fragmentation)
        if entity_graph.get("primary_entity"):
            score += 6
        
        # Check for reasonable entity count (not too fragmented)
        entities = entity_graph.get("entities", [])
        if 1 <= len(entities) <= 10:
            score += 4  # Good entity count
        elif len(entities) <= 15:
            score += 2  # Some fragmentation
        
        return min(score, self.max_score)

class VisibleAuthorNameRule(BaseRule):
    """Rule 20 — Visible author name"""
    
    def __init__(self):
        config = {
            "rule_id": "visible_author_name",
            "category": "citation_probability",
            "description": "Visible author name",
            "weight": 1.5,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for visible author name"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person/Author with name
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                if item.get("name"):
                    score += 10
                    break
        
        return min(score, self.max_score)

class PersonSchemaLinkedToOrganizationRule(BaseRule):
    """Rule 21 — Person schema linked to Organization"""
    
    def __init__(self):
        config = {
            "rule_id": "person_schema_linked_to_organization",
            "category": "citation_probability",
            "description": "Person schema linked to Organization",
            "weight": 0.9,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if Person schema is linked to Organization"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person linked to Organization
        person_found = False
        organization_found = False
        
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                person_found = True
                # Check if linked to organization
                if item.get("worksFor") or item.get("affiliation"):
                    score += 5
            elif item.get("@type") == "Organization":
                organization_found = True
        
        if person_found and organization_found:
            score += 5  # Both present
        
        return min(score, self.max_score)

class DedicatedAuthorPageRule(BaseRule):
    """Rule 38 — Dedicated author page"""
    
    def __init__(self):
        config = {
            "rule_id": "dedicated_author_page",
            "category": "citation_probability",
            "description": "Dedicated author page",
            "weight": 0.8,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for dedicated author page"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person with URL (indicates dedicated page)
        for item in graph:
            if item.get("@type") == "Person":
                if item.get("url"):
                    score += 10
                    break
        
        return min(score, self.max_score)

# Register Citation Probability rules (4 pure AI visibility rules)
def register_citation_probability_rules(registry):
    """Register all Citation Probability category rules"""
    registry.register(NoEntityFragmentationRule())
    registry.register(VisibleAuthorNameRule())
    registry.register(PersonSchemaLinkedToOrganizationRule())
    registry.register(DedicatedAuthorPageRule())
