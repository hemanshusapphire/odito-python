"""
LLM Readiness Score Category Rules

Evaluates how well content is prepared for Large Language Model processing.
Focuses on clarity, structure, and machine-readability factors.
"""

import json
from typing import Dict, Any
from rule_base import BaseRule

class ServicePages800WordsRule(BaseRule):
    """Rule 22 — Service pages 800+ words"""
    
    def __init__(self):
        config = {
            "rule_id": "service_pages_800_words",
            "category": "llm_readiness",
            "description": "Service pages 800+ words",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for service pages with 800+ words"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        word_count = content_metrics.get("word_count", 0)
        
        if word_count >= 800:
            score += 10
        elif word_count >= 600:
            score += 6
        elif word_count >= 400:
            score += 3
        elif word_count >= 200:
            score += 1
        
        return min(score, self.max_score)

class TopicClustersInternalLinksRule(BaseRule):
    """Rule 23 — Topic clusters with internal links"""
    
    def __init__(self):
        config = {
            "rule_id": "topic_clusters_internal_links",
            "category": "llm_readiness",
            "description": "Topic clusters with internal links",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for topic clusters with internal links"""
        heading_metrics = data.get("heading_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        
        score = 0
        
        # Check for multiple headings (indicates topic clusters)
        h2_count = heading_metrics.get("h2_count", 0)
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        
        if h2_count >= 3:
            score += 5
        elif h2_count >= 2:
            score += 3
        elif h2_count >= 1:
            score += 1
        
        # Check for entity density (indicates topic richness)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if entity_density >= 5:
            score += 3
        elif entity_density >= 3:
            score += 2
        elif entity_density >= 1:
            score += 1
        
        # Check for H3 presence (subtopics)
        h3_count = heading_metrics.get("h3_count", 0)
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        if h3_count >= 2:
            score += 2
        
        return min(score, self.max_score)

class WebPAVIFImagesLazyLoadingRule(BaseRule):
    """Rule 26 — WebP / AVIF images + lazy loading"""
    
    def __init__(self):
        config = {
            "rule_id": "webp_avif_images_lazy_loading",
            "category": "llm_readiness",
            "description": "WebP / AVIF images + lazy loading",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for modern image formats and lazy loading"""
        # This would typically check image elements and loading attributes
        # Using content quality as proxy
        score = 0
        
        # Check for structured data with images
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        graph = structured_data.get("@graph", [])
        
        # Check for ImageObject
        for item in graph:
            if item.get("@type") == "ImageObject":
                score += 5
                break
        
        # Check for page performance (proxy for optimization)
        response_time = data.get("response_time_ms", 0)
        if response_time <= 1000:
            score += 3
        elif response_time <= 2000:
            score += 1
        
        # Bonus for well-structured content
        if score > 0:
            score += 2
        
        return min(score, self.max_score)

class ClearEntityFirst150WordsRule(BaseRule):
    """Rule 29 — Clear entity in first 150 words"""
    
    def __init__(self):
        config = {
            "rule_id": "clear_entity_first_150_words",
            "category": "llm_readiness",
            "description": "Clear entity in first 150 words",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for clear entity in first 150 words"""
        entity_graph = data.get("unified_entity_graph", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for primary entity
        if entity_graph.get("primary_entity"):
            score += 6
        
        # Check for early entity mentions
        primary_mentions = entity_graph.get("primary_entity_mentions_in_text", 0)
        word_count = content_metrics.get("word_count", 1)
        
        if word_count > 0:
            mention_ratio = primary_mentions / word_count * 1000
            if mention_ratio >= 10:  # High early mention density
                score += 4
            elif mention_ratio >= 5:
                score += 2
            elif mention_ratio >= 2:
                score += 1
        
        return min(score, self.max_score)

class SameAsArrayLinksActiveRule(BaseRule):
    """Rule 30 — sameAs array links active"""
    
    def __init__(self):
        config = {
            "rule_id": "sameas_array_links_active",
            "category": "llm_readiness",
            "description": "sameAs array links active",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for active sameAs array links"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for sameAs arrays
        sameas_count = 0
        for item in graph:
            if item.get("sameAs"):
                sameas = item["sameAs"]
                if isinstance(sameas, list):
                    sameas_count += len(sameas)
                else:
                    sameas_count += 1
        
        if sameas_count >= 3:
            score += 10
        elif sameas_count >= 2:
            score += 6
        elif sameas_count >= 1:
            score += 3
        
        return min(score, self.max_score)

class GeoCoordinatesInSchemaRule(BaseRule):
    """Rule 31 — GeoCoordinates in schema"""
    
    def __init__(self):
        config = {
            "rule_id": "geo_coordinates_in_schema",
            "category": "llm_readiness",
            "description": "GeoCoordinates in schema",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for GeoCoordinates in schema"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for GeoCoordinates
        for item in graph:
            if item.get("@type") == "GeoCoordinates":
                score += 10
                break
            elif item.get("@type") in ["Place", "LocalBusiness"]:
                if item.get("geo"):
                    score += 10
                    break
        
        return min(score, self.max_score)

class DescriptionMinimum50CharactersRule(BaseRule):
    """Rule 32 — description minimum 50 characters"""
    
    def __init__(self):
        config = {
            "rule_id": "description_minimum_50_characters",
            "category": "llm_readiness",
            "description": "description minimum 50 characters",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for description with minimum 50 characters"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for description length
        for item in graph:
            description = item.get("description", "")
            if len(description) >= 50:
                score += 10
                break
            elif len(description) >= 30:
                score += 5
            elif len(description) >= 10:
                score += 2
        
        return min(score, self.max_score)

class AreaServedDefinedRule(BaseRule):
    """Rule 33 — areaServed defined"""
    
    def __init__(self):
        config = {
            "rule_id": "area_served_defined",
            "category": "llm_readiness",
            "description": "areaServed defined",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for areaServed definition"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for areaServed
        for item in graph:
            if item.get("areaServed"):
                score += 10
                break
        
        return min(score, self.max_score)

class LastUpdatedDateVisibleRule(BaseRule):
    """Rule 43 — Last updated date visible"""
    
    def __init__(self):
        config = {
            "rule_id": "last_updated_date_visible",
            "category": "llm_readiness",
            "description": "Last updated date visible",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for last updated date"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for dateModified
        for item in graph:
            if item.get("dateModified"):
                score += 10
                break
            elif item.get("datePublished"):
                score += 5
                break
        
        return min(score, self.max_score)

class SemanticSubtopicsCoveredRule(BaseRule):
    """Rule 44 — Semantic subtopics covered"""
    
    def __init__(self):
        config = {
            "rule_id": "semantic_subtopics_covered",
            "category": "llm_readiness",
            "description": "Semantic subtopics covered",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for semantic subtopics coverage"""
        heading_metrics = data.get("heading_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        
        score = 0
        
        # Check for H3 headings (subtopics)
        h3_count = heading_metrics.get("h3_count", 0)
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        
        if h3_count >= 3:
            score += 5
        elif h3_count >= 2:
            score += 3
        elif h3_count >= 1:
            score += 1
        
        # Check for entity diversity (semantic richness)
        unique_types = entity_metrics.get("unique_entity_types", 0)
        if unique_types >= 4:
            score += 3
        elif unique_types >= 2:
            score += 2
        elif unique_types >= 1:
            score += 1
        
        # Check for H2 headings (main topics)
        h2_count = heading_metrics.get("h2_count", 0)
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        if h2_count >= 2:
            score += 2
        
        return min(score, self.max_score)

class StatisticsHaveSourceLinksRule(BaseRule):
    """Rule 45 — Statistics have source links"""
    
    def __init__(self):
        config = {
            "rule_id": "statistics_have_source_links",
            "category": "llm_readiness",
            "description": "Statistics have source links",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for statistics with source links"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for citations and references
        for item in graph:
            if item.get("@type") in ["Citation", "Dataset", "ResearchProject"]:
                score += 6
                break
            elif item.get("citation") or item.get("isPartOf"):
                score += 4
                break
        
        # Check for sameAs (external references)
        for item in graph:
            if item.get("sameAs"):
                score += 4
                break
        
        return min(score, self.max_score)

class ShortParagraphs3To5LinesRule(BaseRule):
    """Rule 54 — Short paragraphs (3–5 lines)"""
    
    def __init__(self):
        config = {
            "rule_id": "short_paragraphs_3_to_5_lines",
            "category": "llm_readiness",
            "description": "Short paragraphs (3–5 lines)",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for short paragraphs (3-5 lines)"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for short paragraph ratio
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if 0.3 <= short_ratio <= 0.6:
            score += 6
        elif 0.2 <= short_ratio <= 0.7:
            score += 4
        elif 0.1 <= short_ratio <= 0.8:
            score += 2
        elif short_ratio > 0:
            score += 1
        
        # Check average paragraph length
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 40 <= avg_paragraph_length <= 100:  # Approx 3-5 lines
            score += 4
        elif 30 <= avg_paragraph_length <= 120:
            score += 2
        
        return min(score, self.max_score)

# Register all LLM Readiness rules (12 rules)
def register_llm_readiness_rules(registry):
    """Register all LLM Readiness category rules"""
    registry.register(ServicePages800WordsRule())
    registry.register(TopicClustersInternalLinksRule())
    registry.register(WebPAVIFImagesLazyLoadingRule())
    registry.register(ClearEntityFirst150WordsRule())
    registry.register(SameAsArrayLinksActiveRule())
    registry.register(GeoCoordinatesInSchemaRule())
    registry.register(DescriptionMinimum50CharactersRule())
    registry.register(AreaServedDefinedRule())
    registry.register(LastUpdatedDateVisibleRule())
    registry.register(SemanticSubtopicsCoveredRule())
    registry.register(StatisticsHaveSourceLinksRule())
    registry.register(ShortParagraphs3To5LinesRule())
