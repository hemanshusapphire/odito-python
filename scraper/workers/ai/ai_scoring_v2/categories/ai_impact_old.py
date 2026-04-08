"""
AI Impact Score Category Rules

Evaluates how well content is optimized for AI understanding and processing.
Focuses on structured data, entity clarity, and AI-friendly content organization.
"""

import json
from typing import Dict, Any
from rule_base import BaseRule

class StructuredDataCompletenessRule(BaseRule):
    """Evaluates completeness and quality of structured data"""
    
    def __init__(self):
        config = {
            "rule_id": "structured_data_completeness",
            "category": "ai_impact",
            "description": "Evaluates completeness and quality of structured data",
            "weight": 2.0,
            "max_score": 20,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate structured data completeness"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        if not structured_data:
            return 0
        
        score = 0
        
        # CONTINUOUS: Graph completeness scaling (0-8 points)
        graph = structured_data.get("@graph", [])
        if graph:
            # Scale based on graph size: 1 entity = 2 points, 5+ entities = 8 points
            graph_score = min(len(graph) / 5.0, 1.0) * 8
            score += graph_score
        
        # CONTINUOUS: Entity type diversity scaling (0-6 points)
        entity_types = set()
        for item in graph:
            if "@type" in item:
                entity_types.add(item["@type"])
        
        # Smooth scaling: 1 type = 2 points, 2 types = 4 points, 3+ types = 6 points
        type_score = min(len(entity_types) / 3.0, 1.0) * 6
        score += type_score
        
        # CONTINUOUS: Context presence (0-4 points)
        if structured_data.get("@context"):
            score += 4
        
        # CONTINUOUS: Organization presence (0-2 points)
        has_organization = any(
            item.get("@type") == "Organization" 
            for item in graph
        )
        if has_organization:
            score += 2
        
        return min(score, self.max_score)

class EntityGraphQualityRule(BaseRule):
    """Evaluates quality and richness of entity graph"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_graph_quality",
            "category": "ai_impact",
            "description": "Evaluates quality and richness of entity graph",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity graph quality"""
        entity_graph = data.get("unified_entity_graph", {})
        if not entity_graph:
            return 0
        
        score = 0
        
        # CONTINUOUS: Primary entity presence (0-5 points)
        if entity_graph.get("primary_entity"):
            score += 5
        
        # CONTINUOUS: Entity count with logarithmic scaling (0-5 points)
        entities = entity_graph.get("entities", [])
        # Logarithmic scaling: 2 entities = 1 point, 5 entities = 3 points, 10+ entities = 5 points
        if entities:
            entity_score = min(len(entities) / 10.0, 1.0) * 5
            score += entity_score
        
        # CONTINUOUS: Relationship count with logarithmic scaling (0-5 points)
        relationships = entity_graph.get("relationships", [])
        if relationships:
            # Logarithmic scaling: 1 relationship = 1 point, 2 relationships = 3 points, 5+ relationships = 5 points
            relationship_score = min(len(relationships) / 5.0, 1.0) * 5
            score += relationship_score
        
        return min(score, self.max_score)

class ContentSemanticClarityRule(BaseRule):
    """Evaluates semantic clarity and topic focus"""
    
    def __init__(self):
        config = {
            "rule_id": "content_semantic_clarity",
            "category": "ai_impact",
            "description": "Evaluates semantic clarity and topic focus",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate semantic clarity"""
        content_metrics = data.get("content_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        
        score = 0
        
        # CONTINUOUS: Word count scaling (0-3 points)
        word_count = content_metrics.get("word_count", 0)
        # Smooth scaling: 300 words = 1 point, 500 words = 2 points, 1000+ words = 3 points
        word_score = min(word_count / 1000.0, 1.0) * 3
        score += word_score
        
        # CONTINUOUS: Entity density scaling (0-4 points)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        # Logarithmic scaling: 1 entity/1k words = 1 point, 5 entities/1k words = 3 points, 10+ entities/1k words = 4 points
        if entity_density > 0:
            density_score = min(entity_density / 10.0, 1.0) * 4
            score += density_score
        
        # CONTINUOUS: Readability scaling (0-3 points)
        readability = content_metrics.get("readability_score", 0)
        # Optimal range 40-80, peak at 60
        if readability > 0:
            # Distance from optimal (60), max distance 40
            distance = abs(readability - 60)
            readability_score = max(0, 3 - (distance / 40) * 3)
            score += readability_score
        
        return min(score, self.max_score)

class SchemaMarkupValidationRule(BaseRule):
    """Validates schema markup correctness and completeness"""
    
    def __init__(self):
        config = {
            "rule_id": "schema_markup_validation",
            "category": "ai_impact",
            "description": "Validates schema markup correctness and completeness",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate schema markup validation"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        if not structured_data:
            return 0
        
        score = 0
        
        # Check for proper @context
        if structured_data.get("@context") in [
            "https://schema.org", 
            "http://schema.org"
        ]:
            score += 3
        
        # Check for @type consistency
        graph = structured_data.get("@graph", [])
        if graph:
            has_types = all(item.get("@type") for item in graph)
            if has_types:
                score += 4
        
        # Check for required properties in common types
        for item in graph:
            item_type = item.get("@type", "")
            if item_type == "Organization":
                if item.get("name") and item.get("url"):
                    score += 3
            elif item_type == "WebPage":
                if item.get("name") and item.get("url"):
                    score += 2
            elif item_type == "Article":
                if item.get("headline") and item.get("author"):
                    score += 3
        
        return min(score, self.max_score)

class AIContentOptimizationRule(BaseRule):
    """Evaluates content optimization for AI processing"""
    
    def __init__(self):
        config = {
            "rule_id": "ai_content_optimization",
            "category": "ai_impact",
            "description": "Evaluates content optimization for AI processing",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate AI content optimization"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check heading structure
        if heading_metrics.get("heading_sequence_valid"):
            score += 3
        
        # Check paragraph structure
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 50 <= avg_paragraph_length <= 150:
            score += 3
        elif 30 <= avg_paragraph_length <= 200:
            score += 2
        elif avg_paragraph_length > 0:
            score += 1
        
        # Check content organization
        h1_count = heading_metrics.get("h1_count", 0)
        h2_count = heading_metrics.get("h2_count", 0)
        
        if h1_count == 1 and h2_count >= 2:
            score += 4
        elif h1_count == 1 and h2_count >= 1:
            score += 2
        elif h1_count >= 1:
            score += 1
        
        return min(score, self.max_score)

class TechnicalAIReadinessRule(BaseRule):
    """Evaluates technical readiness for AI processing"""
    
    def __init__(self):
        config = {
            "rule_id": "technical_ai_readiness",
            "category": "ai_impact",
            "description": "Evaluates technical readiness for AI processing",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate technical AI readiness"""
        score = 0
        
        # Check for HTTP status
        if data.get("http_status_code") == 200:
            score += 2
        
        # Check for response time
        response_time = data.get("response_time_ms", 0)
        if response_time <= 1000:
            score += 2
        elif response_time <= 2000:
            score += 1
        
        # Check for extraction quality flags
        quality_flags = data.get("quality_flags", {})
        if not quality_flags.get("no_main_content_detected", True):
            score += 3
        
        if not quality_flags.get("malformed_structure", True):
            score += 3
        
        return min(score, self.max_score)

class EntityConsistencyRule(BaseRule):
    """Evaluates entity consistency across content"""
    
    def __init__(self):
        config = {
            "rule_id": "entity_consistency",
            "category": "ai_impact",
            "description": "Evaluates entity consistency across content",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate entity consistency"""
        entity_graph = data.get("unified_entity_graph", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check primary entity mentions
        primary_mentions = entity_graph.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)
        
        # MATHEMATICAL SAFETY: Prevent division by zero
        if word_count > 0:
            mention_ratio = primary_mentions / word_count * 1000
        else:
            mention_ratio = 0
        
        if mention_ratio >= 5:
            score += 5
        elif mention_ratio >= 2:
            score += 3
        elif mention_ratio >= 1:
            score += 1
        
        # Check entity type consistency
        entities = entity_graph.get("entities", [])
        entity_types = set()
        for entity in entities:
            if entity.get("@type"):
                entity_types.add(entity["@type"])
        
        if len(entity_types) <= 5:  # Not too many types
            score += 3
        
        # Check for duplicate entities
        entity_names = [entity.get("name", "") for entity in entities]
        unique_names = len(set(entity_names))
        if unique_names == len(entity_names):
            score += 2
        
        return min(score, self.max_score)

class CrossReferenceQualityRule(BaseRule):
    """Evaluates cross-references and internal linking quality"""
    
    def __init__(self):
        config = {
            "rule_id": "cross_reference_quality",
            "category": "ai_impact",
            "description": "Evaluates cross-references and internal linking quality",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate cross-reference quality"""
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # CONTINUOUS: Relationship count scaling (0-5 points)
        relationships = entity_graph.get("relationships", [])
        if relationships:
            # Logarithmic scaling: 1 relationship = 1 point, 3 relationships = 3 points, 5+ relationships = 5 points
            relationship_score = min(len(relationships) / 5.0, 1.0) * 5
            score += relationship_score
        
        # CONTINUOUS: SameAs references scaling (0-3 points)
        entities = entity_graph.get("entities", [])
        sameas_count = 0
        for entity in entities:
            if entity.get("sameAs"):
                sameas_count += 1
        
        if sameas_count > 0:
            # Smooth scaling: 1 sameAs = 1 point, 2 sameAs = 2 points, 3+ sameAs = 3 points
            sameas_score = min(sameas_count / 3.0, 1.0) * 3
            score += sameas_score
        
        # CONTINUOUS: URL consistency scaling (0-2 points)
        url_consistency = 0
        for entity in entities:
            if entity.get("url"):
                url_consistency += 1
        
        if url_consistency > 0:
            # Linear scaling: 1 URL = 1 point, 2+ URLs = 2 points
            url_score = min(url_consistency / 2.0, 1.0) * 2
            score += url_score
        
        has_urls = 0
        for entity in entities:
            if entity.get("url"):
                has_urls += 1
        
        if has_urls >= len(entities) * 0.8:
            score += 2
        
        return score

class BreadcrumbNavigationRule(BaseRule):
    """Evaluates breadcrumb navigation presence and quality"""
    
    def __init__(self):
        config = {
            "rule_id": "breadcrumb_navigation",
            "category": "ai_impact",
            "description": "Evaluates breadcrumb navigation presence and quality",
            "weight": 0.6,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate breadcrumb navigation"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for BreadcrumbList schema
        has_breadcrumb = False
        breadcrumb_items = 0
        
        for item in graph:
            if item.get("@type") == "BreadcrumbList":
                has_breadcrumb = True
                item_list = item.get("itemListElement", [])
                breadcrumb_items = len(item_list)
                break
        
        if has_breadcrumb:
            score += 5
            # Bonus for multiple breadcrumb levels
            if breadcrumb_items >= 3:
                score += 3
            elif breadcrumb_items >= 2:
                score += 2
            elif breadcrumb_items >= 1:
                score += 1
        
        return min(score, self.max_score)

class ContentFreshnessRule(BaseRule):
    """Evaluates content freshness signals"""
    
    def __init__(self):
        config = {
            "rule_id": "content_freshness",
            "category": "ai_impact",
            "description": "Evaluates content freshness signals",
            "weight": 0.5,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content freshness"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        for item in graph:
            # Check for date fields indicating content freshness
            if any(date_field in item for date_field in ["dateModified", "datePublished", "dateCreated"]):
                score += 4
                
                # Bonus for having multiple date fields
                date_fields = sum(1 for field in ["dateModified", "datePublished", "dateCreated"] if field in item)
                if date_fields >= 2:
                    score += 3
                elif date_fields >= 1:
                    score += 1
                break
        
        # Check for Article or BlogPosting types (typically have freshness signals)
        for item in graph:
            if item.get("@type") in ["Article", "BlogPosting", "NewsArticle"]:
                score += 3
                break
        
        return min(score, self.max_score)

class ImageSchemaCompletenessRule(BaseRule):
    """Evaluates image schema completeness for visual content"""
    
    def __init__(self):
        config = {
            "rule_id": "image_schema_completeness",
            "category": "ai_impact",
            "description": "Evaluates image schema completeness for visual content",
            "weight": 0.7,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate image schema completeness"""
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
        
        # Check for ImageObject in structured data
        image_count = 0
        complete_images = 0
        
        for item in graph:
            if item.get("@type") == "ImageObject":
                image_count += 1
                # Check for required image properties
                if item.get("url") and item.get("width") and item.get("height"):
                    complete_images += 1
                elif item.get("url") and item.get("caption"):
                    complete_images += 1
        
        # Score based on image presence and completeness
        if image_count >= 1:
            score += 3
            if complete_images >= 2:
                score += 4
            elif complete_images >= 1:
                score += 2
        
        # Check for primary entity with image
        primary_entity = entity_graph.get("primary_entity", {})
        if primary_entity and (primary_entity.get("image") or primary_entity.get("logo")):
            score += 3
        
        return min(score, self.max_score)

# Register all AI Impact rules
def register_ai_impact_rules(registry):
    """Register all AI Impact category rules"""
    registry.register(StructuredDataCompletenessRule())
    registry.register(EntityGraphQualityRule())
    registry.register(ContentSemanticClarityRule())
    registry.register(SchemaMarkupValidationRule())
    registry.register(AIContentOptimizationRule())
    registry.register(TechnicalAIReadinessRule())
    registry.register(EntityConsistencyRule())
    registry.register(CrossReferenceQualityRule())
    registry.register(BreadcrumbNavigationRule())
    registry.register(ContentFreshnessRule())
    registry.register(ImageSchemaCompletenessRule())
