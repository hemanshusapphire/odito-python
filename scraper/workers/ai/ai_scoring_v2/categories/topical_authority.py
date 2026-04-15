"""
Topical Authority Score Category Rules

Evaluates topical authority and entity expertise.
Focuses on entity consistency, schema relationships, and authority signals.
"""

import json
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class OnlyOnePrimaryEntityRule(BaseRule):
    """Rule 16 — Only ONE primary entity site-wide"""
    
    def __init__(self):
        config = {
            "rule_id": "only_one_primary_entity",
            "category": "topical_authority",
            "description": "Only ONE primary entity site-wide",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for single primary entity"""
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for primary entity
        if entity_graph.get("primary_entity"):
            score += 6
        
        # Check for reasonable entity count (not too fragmented)
        entities = entity_graph.get("entities", [])
        if len(entities) <= 5:
            score += 4  # Focused entity structure
        elif len(entities) <= 10:
            score += 2
        else:
            score += 0  # Too many entities
        
        return min(score, self.max_score)

class ConsistentIDAcrossPagesRule(BaseRule):
    """Rule 17 — Consistent @id across pages"""
    
    def __init__(self):
        config = {
            "rule_id": "consistent_id_across_pages",
            "category": "topical_authority",
            "description": "Consistent @id across pages",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for consistent @id usage"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for @id usage
        id_count = 0
        for item in graph:
            if item.get("@id"):
                id_count += 1
        
        if id_count >= 1:
            score += 10  # Has @id
        else:
            score += 0  # No @id
        
        return min(score, self.max_score)

class ChildSchemasReferenceMainIDRule(BaseRule):
    """Rule 18 — Child schemas reference main @id"""
    
    def __init__(self):
        config = {
            "rule_id": "child_schemas_reference_main_id",
            "category": "topical_authority",
            "description": "Child schemas reference main @id",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if child schemas reference main @id"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Find main entity @id
        main_id = None
        for item in graph:
            if item.get("@type") == "Organization":
                main_id = item.get("@id")
                break
        
        # Check if other entities reference main entity
        references = 0
        if main_id:
            for item in graph:
                if item.get("@id") != main_id:  # Not the main entity
                    # Check for references to main entity
                    if (item.get("isPartOf") == main_id or 
                        item.get("parentOrganization") == main_id or
                        item.get("worksFor") == main_id):
                        references += 1
        
        if references >= 1:
            score += 10
        elif main_id:
            score += 5  # Has main ID but no references
        
        return min(score, self.max_score)

class OpeningHoursSpecificationRule(BaseRule):
    """Rule 63 — openingHoursSpecification"""
    
    def __init__(self):
        config = {
            "rule_id": "opening_hours_specification",
            "category": "topical_authority",
            "description": "openingHoursSpecification",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for openingHoursSpecification"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Only relevant for local business types
        business_types = {"LocalBusiness", "Restaurant", "Store", "Hotel", "ProfessionalService"}
        has_business_type = False
        
        for item in graph:
            item_type = item.get("@type", "")
            if isinstance(item_type, list):
                if any(bt in item_type for bt in business_types):
                    has_business_type = True
                    break
            elif item_type in business_types:
                has_business_type = True
                break
        
        if not has_business_type:
            return 0.0
        
        # Check for openingHoursSpecification
        for item in graph:
            if item.get("openingHoursSpecification"):
                score += 10
                break
            elif item.get("@type") == "OpeningHoursSpecification":
                score += 10
                break
        
        return min(score, self.max_score)

class EventSchemaRule(BaseRule):
    """Rule 65 — Event schema"""
    
    def __init__(self):
        config = {
            "rule_id": "event_schema",
            "category": "topical_authority",
            "description": "Event schema",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for Event schema"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Event schema
        for item in graph:
            if item.get("@type") == "Event":
                score += 10
                break
        
        return min(score, self.max_score)

class AggregateRatingSchemaRule(BaseRule):
    """Rule 66 — AggregateRating schema"""
    
    def __init__(self):
        config = {
            "rule_id": "aggregate_rating_schema",
            "category": "topical_authority",
            "description": "AggregateRating schema",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for AggregateRating schema"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for AggregateRating
        for item in graph:
            if item.get("@type") == "AggregateRating":
                score += 10
                break
            elif item.get("aggregateRating"):
                score += 10
                break
        
        return min(score, self.max_score)

class ServiceProductSchemaWithOffersRule(BaseRule):
    """Rule 67 — Service/Product schema with offers"""
    
    def __init__(self):
        config = {
            "rule_id": "service_product_schema_with_offers",
            "category": "topical_authority",
            "description": "Service/Product schema with offers",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for Service/Product schema with offers"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Service or Product with offers
        for item in graph:
            if item.get("@type") in ["Service", "Product"]:
                if item.get("offers"):
                    score += 10
                    break
                else:
                    score += 5  # Has Service/Product but no offers
                    break
        
        return min(score, self.max_score)

# Register all Topical Authority rules (7 rules)
def register_topical_authority_rules(registry):
    """Register all Topical Authority category rules"""
    registry.register(OnlyOnePrimaryEntityRule())
    registry.register(ConsistentIDAcrossPagesRule())
    registry.register(ChildSchemasReferenceMainIDRule())
    registry.register(OpeningHoursSpecificationRule())
    registry.register(EventSchemaRule())
    registry.register(AggregateRatingSchemaRule())
    registry.register(ServiceProductSchemaWithOffersRule())
