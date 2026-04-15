"""
Topical Authority Score Category Rules

Evaluates topical authority and expertise in specific subject areas.
Focuses on entity richness, topic coverage, and authority signals.
"""

import json
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class EntityRichnessRule(BaseRule):
    """Evaluates richness and diversity of entities"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_richness",
            "category": "topical_authority",
            "description": "Evaluates richness and diversity of entities",
            "weight": 2.0,
            "max_score": 20,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity richness"""
        entity_metrics = data.get("entity_metrics", {})
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # CONTINUOUS: Entity count scaling (0-6 points)
        entity_count = entity_metrics.get("entity_count", 0)
        # Logarithmic scaling: 1 entity = 1 point, 5 entities = 3 points, 15+ entities = 6 points
        if entity_count > 0:
            entity_score = min(entity_count / 15.0, 1.0) * 6
            score += entity_score
        
        # CONTINUOUS: Unique entity types scaling (0-6 points)
        unique_types = entity_metrics.get("unique_entity_types", 0)
        # Smooth scaling: 1 type = 2 points, 3 types = 4 points, 6+ types = 6 points
        if unique_types > 0:
            type_score = min(unique_types / 6.0, 1.0) * 6
            score += type_score
        
        # CONTINUOUS: Entity density scaling (0-4 points)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        # Optimal density: 3-10 per 1000 words
        if entity_density > 0:
            density_score = min(entity_density / 10.0, 1.0) * 4
            score += density_score
        
        # CONTINUOUS: Relationship count scaling (0-4 points)
        relationships = entity_graph.get("relationships", [])
        if relationships:
            # Logarithmic scaling: 1 relationship = 1 point, 3 relationships = 3 points, 5+ relationships = 4 points
            relationship_score = min(len(relationships) / 5.0, 1.0) * 4
            score += relationship_score
        
        return min(score, self.max_score)

class TopicalDepthRule(BaseRule):
    """Evaluates depth of topical coverage"""
    
    def __init__(self):
        config = {
            "rule_id": "topical_depth",
            "category": "topical_authority",
            "description": "Evaluates depth of topical coverage",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate topical depth"""
        content_metrics = data.get("content_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check content length (indicates depth)
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 2000:
            score += 5
        elif word_count >= 1500:
            score += 4
        elif word_count >= 1000:
            score += 3
        elif word_count >= 500:
            score += 2
        elif word_count >= 300:
            score += 1
        
        # Check heading structure depth
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        if h2_count >= 4 and h3_count >= 3:
            score += 4
        elif h2_count >= 3 and h3_count >= 2:
            score += 3
        elif h2_count >= 2 and h3_count >= 1:
            score += 2
        elif h2_count >= 2:
            score += 1
        
        # Check entity density (indicates rich topic coverage)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if entity_density >= 8:
            score += 3
        elif entity_density >= 6:
            score += 2
        elif entity_density >= 4:
            score += 1
        
        # Check paragraph count (indicates detailed coverage)
        paragraph_count = content_metrics.get("paragraph_count", 0)
        if paragraph_count >= 8:
            score += 3
        elif paragraph_count >= 6:
            score += 2
        elif paragraph_count >= 4:
            score += 1
        
        return min(score, self.max_score)

class PrimaryEntityAuthorityRule(BaseRule):
    """Evaluates authority of primary entity"""
    
    def __init__(self):
        config = {
            "rule_id": "primary_entity_authority",
            "category": "topical_authority",
            "description": "Evaluates authority of primary entity",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate primary entity authority"""
        entity_graph = data.get("unified_entity_graph", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for primary entity
        primary_entity = entity_graph.get("primary_entity")
        if primary_entity:
            score += 5
            
            # Check primary entity properties
            if primary_entity.get("name"):
                score += 2
            if primary_entity.get("url"):
                score += 2
            if primary_entity.get("sameAs"):
                score += 2
        
        # Check primary entity mentions
        primary_mentions = entity_graph.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)
        
        # MATHEMATICAL SAFETY: Prevent division by zero
        if word_count > 0:
            mention_ratio = primary_mentions / word_count * 1000
        else:
            mention_ratio = 0
        
        if mention_ratio >= 5:
            score += 4
        elif mention_ratio >= 3:
            score += 3
        elif mention_ratio >= 2:
            score += 2
        elif mention_ratio >= 1:
            score += 1
        
        return min(score, self.max_score)

class EntityRelationshipsRule(BaseRule):
    """Evaluates quality and quantity of entity relationships"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_relationships",
            "category": "topical_authority",
            "description": "Evaluates quality and quantity of entity relationships",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity relationships"""
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # CONTINUOUS: Relationship count scaling (0-4 points)
        relationships = entity_graph.get("relationships", [])
        if relationships:
            # Logarithmic scaling: 1 relationship = 1 point, 3 relationships = 3 points, 5+ relationships = 4 points
            relationship_score = min(len(relationships) / 5.0, 1.0) * 4
            score += relationship_score
        elif len(relationships) >= 1:
            score += 1
        
        # CONTINUOUS: Relationship diversity scaling (0-3 points)
        relationship_types = set()
        for rel in relationships:
            if rel.get("type"):
                relationship_types.add(rel["type"])
        
        if relationship_types:
            # Smooth scaling: 1 type = 1 point, 2 types = 2 points, 3+ types = 3 points
            diversity_score = min(len(relationship_types) / 3.0, 1.0) * 3
            score += diversity_score
        
        # CONTINUOUS: Bidirectional relationships scaling (0-3 points)
        bidirectional = 0
        for rel in relationships:
            if rel.get("bidirectional"):
                bidirectional += 1
        
        if bidirectional > 0:
            # Linear scaling: 1 bidirectional = 1 point, 2+ bidirectional = 3 points
            bidirectional_score = min(bidirectional / 2.0, 1.0) * 3
            score += bidirectional_score
        
        return min(score, self.max_score)

class TopicConsistencyRule(BaseRule):
    """Evaluates consistency of topic throughout content"""
    
    def __init__(self):
        config = {
            "rule_id": "topic_consistency",
            "category": "topical_authority",
            "description": "Evaluates consistency of topic throughout content",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate topic consistency"""
        entity_metrics = data.get("entity_metrics", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check entity density consistency
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if 5 <= entity_density <= 12:  # Optimal density
            score += 4
        elif 3 <= entity_density <= 15:
            score += 3
        elif 2 <= entity_density <= 18:
            score += 2
        elif entity_density >= 1:
            score += 1
        
        # Check primary entity consistency
        primary_mentions = entity_metrics.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)
        
        # MATHEMATICAL SAFETY: Prevent division by zero
        if word_count > 0:
            mention_ratio = primary_mentions / word_count * 1000
        else:
            mention_ratio = 0
        
        if 2 <= mention_ratio <= 6:  # Good consistency
            score += 3
        elif 1 <= mention_ratio <= 8:
            score += 2
        elif mention_ratio > 0:
            score += 1
        
        # Check for reasonable entity type diversity
        unique_types = entity_metrics.get("unique_entity_types", 0)
        if 3 <= unique_types <= 5:  # Focused but comprehensive
            score += 3
        elif 2 <= unique_types <= 6:
            score += 2
        elif 1 <= unique_types <= 7:
            score += 1
        
        return min(score, self.max_score)

class ExpertiseSignalsRule(BaseRule):
    """Evaluates expertise and authority signals"""
    
    def __init__(self):
        config = {
            "rule_id": "expertise_signals",
            "category": "topical_authority",
            "description": "Evaluates expertise and authority signals",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate expertise signals"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for expert entities
        entities = entity_graph.get("entities", [])
        expert_entities = 0
        for entity in entities:
            if entity.get("@type") in [
                "Person", "Organization", "ProfessionalService",
                "MedicalOrganization", "EducationalOrganization"
            ]:
                expert_entities += 1
        
        if expert_entities >= 3:
            score += 4
        elif expert_entities >= 2:
            score += 3
        elif expert_entities >= 1:
            score += 2
        
        # Check for expertise indicators in structured data
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Person":
                if item.get("jobTitle") or item.get("knowsAbout") or item.get("award"):
                    score += 3
                if item.get("alumniOf") or item.get("worksFor"):
                    score += 2
        
        # Check for organizational expertise
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("award") or item.get("hasCredential"):
                    score += 2
        
        return min(score, self.max_score)

class ContentComprehensivenessRule(BaseRule):
    """Evaluates comprehensiveness of content coverage"""
    
    def __init__(self):
        config = {
            "rule_id": "content_comprehensiveness",
            "category": "topical_authority",
            "description": "Evaluates comprehensiveness of content coverage",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content comprehensiveness"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        step_metrics = data.get("step_metrics", {})
        
        score = 0
        
        # Check content length
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 1500:
            score += 3
        elif word_count >= 1000:
            score += 2
        elif word_count >= 500:
            score += 1
        
        # Check heading structure
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        if h2_count >= 3:
            score += 2
        elif h2_count >= 2:
            score += 1
        
        # Check for diverse content types
        content_types = 0
        if faq_metrics.get("faq_detected"):
            content_types += 1
        if step_metrics.get("step_section_present"):
            content_types += 1
        if h3_count >= 2:
            content_types += 1
        
        if content_types >= 2:
            score += 3
        elif content_types >= 1:
            score += 2
        
        # Check paragraph structure
        paragraph_count = content_metrics.get("paragraph_count", 0)
        if paragraph_count >= 6:
            score += 2
        elif paragraph_count >= 4:
            score += 1
        
        return min(score, self.max_score)

class EntityValidationRule(BaseRule):
    """Evaluates validation and quality of entities"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_validation",
            "category": "topical_authority",
            "description": "Evaluates validation and quality of entities",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity validation"""
        entity_graph = data.get("unified_entity_graph", {})
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        
        # Check for entity completeness
        entities = entity_graph.get("entities", [])
        complete_entities = 0
        for entity in entities:
            if entity.get("name") and entity.get("@type"):
                complete_entities += 1
        
        if complete_entities >= 5:
            score += 4
        elif complete_entities >= 3:
            score += 3
        elif complete_entities >= 1:
            score += 2
        
        # Check for external references
        entities_with_refs = 0
        for entity in entities:
            if entity.get("sameAs") or entity.get("url"):
                entities_with_refs += 1
        
        if entities_with_refs >= 3:
            score += 3
        elif entities_with_refs >= 1:
            score += 2
        
        # Check for structured data consistency
        graph = structured_data.get("@graph", [])
        if len(graph) >= 2:
            score += 3
        
        return min(score, self.max_score)

class EntityMentionDistributionRule(BaseRule):
    """Evaluates distribution of entity mentions throughout content"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_mention_distribution",
            "category": "topical_authority",
            "description": "Evaluates distribution of entity mentions throughout content",
            "weight": 0.6,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity mention distribution"""
        entity_metrics = data.get("entity_metrics", {})
        content_metrics = data.get("content_metrics", {})
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check primary entity mentions relative to content length
        primary_mentions = entity_metrics.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)
        
        # Calculate mention density per 1000 words
        if word_count > 0:
            mention_density = (primary_mentions / word_count) * 1000
            
            # Optimal distribution: 3-8 mentions per 1000 words
            if 3 <= mention_density <= 8:
                score += 4
            elif 2 <= mention_density <= 10:
                score += 3
            elif 1 <= mention_density <= 12:
                score += 2
            elif mention_density > 0:
                score += 1
        
        # Check entity count vs unique types (indicates distribution)
        entity_count = entity_metrics.get("entity_count", 0)
        unique_types = entity_metrics.get("unique_entity_types", 0)
        
        if entity_count > 0 and unique_types > 0:
            # Good ratio indicates distributed mentions
            ratio = entity_count / unique_types
            if 2 <= ratio <= 5:
                score += 3
            elif 1.5 <= ratio <= 7:
                score += 2
            elif ratio >= 1:
                score += 1
        
        # Check for relationships (indicates interconnected entities)
        relationships = entity_graph.get("relationships", [])
        if len(relationships) >= 3:
            score += 3
        elif len(relationships) >= 1:
            score += 1
        
        return min(score, self.max_score)

class SchemaTypeDiversityRule(BaseRule):
    """Evaluates diversity of schema types indicating topic breadth"""
    
    def __init__(self):
        config = {
            "rule_id": "schema_type_diversity",
            "category": "topical_authority",
            "description": "Evaluates diversity of schema types indicating topic breadth",
            "weight": 0.5,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate schema type diversity"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Collect all schema types
        schema_types = set()
        for item in graph:
            item_type = item.get("@type", "")
            if isinstance(item_type, str):
                schema_types.add(item_type)
            elif isinstance(item_type, list):
                schema_types.update(item_type)
        
        # Also get entity types
        entities = entity_graph.get("entities", [])
        for entity in entities:
            entity_type = entity.get("@type", "")
            if entity_type:
                schema_types.add(entity_type)
        
        # Score based on type diversity
        type_count = len(schema_types)
        if type_count >= 6:
            score += 5
        elif type_count >= 4:
            score += 4
        elif type_count >= 3:
            score += 3
        elif type_count >= 2:
            score += 2
        elif type_count >= 1:
            score += 1
        
        # Check for presence of core types (indicates comprehensive coverage)
        core_types = {"Organization", "Person", "WebPage", "Article"}
        present_core = len(core_types.intersection(schema_types))
        
        if present_core >= 3:
            score += 3
        elif present_core >= 2:
            score += 2
        elif present_core >= 1:
            score += 1
        
        # Bonus for specialized types (indicates topic depth)
        specialized_types = {"Product", "Service", "Event", "Place", "Dataset"}
        present_specialized = len(specialized_types.intersection(schema_types))
        
        if present_specialized >= 2:
            score += 2
        elif present_specialized >= 1:
            score += 1
        
        return min(score, self.max_score)

class ContentDepthIndicatorsRule(BaseRule):
    """Evaluates content depth indicators through structural analysis"""
    
    def __init__(self):
        config = {
            "rule_id": "content_depth_indicators",
            "category": "topical_authority",
            "description": "Evaluates content depth indicators through structural analysis",
            "weight": 0.7,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content depth indicators"""
        heading_metrics = data.get("heading_metrics", {})
        content_metrics = data.get("content_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        
        score = 0
        
        # Check heading depth (indicates subtopic coverage)
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        
        # Deep structure indicates comprehensive coverage
        if h2_count >= 3 and h3_count >= 3:
            score += 3
        elif h2_count >= 3 and h3_count >= 1:
            score += 2
        elif h2_count >= 2:
            score += 1
        
        # Check paragraph density (paragraphs per heading)
        paragraph_count = content_metrics.get("paragraph_count", 0)
        if h2_count > 0:
            paragraphs_per_section = paragraph_count / max(h2_count, 1)
            if 2 <= paragraphs_per_section <= 5:  # Optimal depth
                score += 3
            elif 1.5 <= paragraphs_per_section <= 6:
                score += 2
            elif paragraphs_per_section >= 1:
                score += 1
        
        # Check entity density (indicates rich topic coverage)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if 6 <= entity_density <= 12:  # High but not excessive
            score += 3
        elif 4 <= entity_density <= 15:
            score += 2
        elif entity_density >= 2:
            score += 1
        
        # Check word count relative to structure
        word_count = content_metrics.get("word_count", 0)
        total_headings = h2_count + h3_count
        
        if total_headings > 0:
            words_per_heading = word_count / total_headings
            if 150 <= words_per_heading <= 400:  # Good depth per section
                score += 2
            elif 100 <= words_per_heading <= 500:
                score += 1
        
        return min(score, self.max_score)

# Register all Topical Authority rules
def register_topical_authority_rules(registry):
    """Register all Topical Authority category rules"""
    registry.register(EntityRichnessRule())
    registry.register(TopicalDepthRule())
    registry.register(PrimaryEntityAuthorityRule())
    registry.register(EntityRelationshipsRule())
    registry.register(TopicConsistencyRule())
    registry.register(ExpertiseSignalsRule())
    registry.register(ContentComprehensivenessRule())
    registry.register(EntityValidationRule())
    registry.register(EntityMentionDistributionRule())
    registry.register(SchemaTypeDiversityRule())
    registry.register(ContentDepthIndicatorsRule())
