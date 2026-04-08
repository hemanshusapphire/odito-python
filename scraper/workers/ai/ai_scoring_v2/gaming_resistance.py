"""
Gaming Resistance Layer for AI Visibility Scoring v2

Applies anti-gaming dampening coefficients with authority super-signal layer
and system-wide spam penalties.
"""

import math
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class GamingResistanceLayer:
    """Applies anti-gaming measures to prevent score inflation"""
    
    def __init__(self):
        # Constants for inflation detection
        self.k_entity = 2.0  # Entity inflation sensitivity
        self.k_faq = 1.5     # FAQ inflation sensitivity
        self.k_heading = 1.2 # Heading inflation sensitivity
        self.k_step = 1.0    # Step inflation sensitivity
        self.k_word = 0.8    # Word inflation sensitivity
        self.k_structured = 1.8  # Structured data inflation sensitivity
        
        # Minimum thresholds for content metrics
        self.min_answer_length = 50  # Minimum answer length in characters
        self.min_entity_density = 2   # Minimum entities per 1000 words
        self.min_heading_depth = 2    # Minimum heading levels
    
    def apply_anti_gaming_dampening(self, raw_scores: Dict[str, float], data: Dict[str, Any]) -> Dict[str, float]:
        """Main method to apply all anti-gaming measures with system-wide spam penalty"""
        
        # Phase 1: Detect inflation patterns
        inflation_patterns = self.detect_inflation_patterns(data)
        
        # Phase 2: Apply efficiency curves with authority super-signal layer
        dampened_scores = self.apply_efficiency_curves(raw_scores, inflation_patterns)
        
        # Phase 3: Apply system-wide spam penalty (NEW)
        spam_penalized_scores = self.apply_system_wide_spam_penalty(dampened_scores, inflation_patterns, data)
        
        # Phase 4: Apply density validation
        final_scores = self.apply_density_validation(spam_penalized_scores, data)
        
        return final_scores
    
    def detect_inflation_patterns(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Detect various inflation patterns in the content"""
        
        inflation_scores = {}
        
        # Get content metrics early to avoid scope issues
        content_metrics = data.get("content_metrics", {})
        word_count = content_metrics.get("word_count", 1)
        
        # Entity inflation detection
        entity_graph = data.get("unified_entity_graph", {})
        entities = entity_graph.get("entities", [])
        
        if entities and content_metrics:
            entity_density = len(entities) / word_count * 1000
            
            # Store entity data for authority preservation
            entity_data = {
                "entity_graph": entity_graph,
                "content_metrics": content_metrics,
                "uniqueness_ratio": self._calculate_uniqueness_ratio(entities),
                "entity_density": entity_density,
                "has_organization": any(e.get("@type") == "Organization" for e in entities),
                "cross_link_strength": self._calculate_cross_link_strength(entity_graph)
            }
            inflation_scores["entity_data"] = entity_data
            
            # Check for entity inflation (too many entities for content length)
            if entity_density > 15:  # More than 15 entities per 1000 words
                entity_inflation = min((entity_density - 15) / 20, 1.0)
            else:
                entity_inflation = 0
            inflation_scores["entity_inflation"] = entity_inflation
        
        # FAQ inflation detection
        faq_metrics = data.get("faq_metrics", {})
        faq_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        
        if faq_count > 0:
            # Check for FAQ inflation (many questions but few good answers)
            answer_ratio = qa_pairs / faq_count
            if faq_count > 10 and answer_ratio < 0.3:
                faq_inflation = min((faq_count - 10) / 20 * (1 - answer_ratio), 1.0)
            else:
                faq_inflation = 0
            inflation_scores["faq_inflation"] = faq_inflation
        
        # Heading inflation detection
        heading_metrics = data.get("heading_metrics", {})
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        paragraph_count = content_metrics.get("paragraph_count", 1)
        
        # Check for heading inflation (too many headings for content)
        if paragraph_count > 0:
            heading_density = (h2_count + h3_count) / paragraph_count
            if heading_density > 0.8:  # More than 0.8 headings per paragraph
                heading_inflation = min((heading_density - 0.8) / 0.5, 1.0)
            else:
                heading_inflation = 0
            inflation_scores["heading_inflation"] = heading_inflation
        
        # Step inflation detection
        step_metrics = data.get("step_metrics", {})
        step_count = step_metrics.get("step_count", 0)
        
        # Check for step inflation (many steps but little content)
        if step_count > 10 and word_count < 1000:
            step_inflation = min((step_count - 10) / 15, 1.0)
        else:
            step_inflation = 0
        inflation_scores["step_inflation"] = step_inflation
        
        # Word inflation detection (excessive word count without substance)
        if word_count > 5000:
            # Check if content has corresponding structure
            total_headings = h2_count + h3_count
            if total_headings < 10:  # Few headings for many words
                word_inflation = min((word_count - 5000) / 10000, 1.0)
            else:
                word_inflation = 0
            inflation_scores["word_inflation"] = word_inflation
        
        # Structured data inflation detection
        structured_data = data.get("structured_data", {})
        graph = structured_data.get("@graph", [])
        
        if len(graph) > 10:
            # Check for cross-linking between entities
            cross_link_ratio = self._calculate_cross_link_strength(entity_graph)
            
            # Inflation: many nodes but weak cross-linking
            if len(graph) > 5 and cross_link_ratio < 0.3:
                structured_inflation = min((5 - cross_link_ratio * 10) / 5, 1.0)
            else:
                structured_inflation = 0
            
            # Store structured data for authority preservation
            structured_data_info = {
                "structured_richness": min(len(graph) / 10, 1.0),
                "cross_link_strength": cross_link_ratio,
                "has_organization": any(item.get("@type") == "Organization" for item in graph)
            }
            inflation_scores["structured_data"] = structured_data_info
            inflation_scores["structured_inflation"] = structured_inflation
        
        return inflation_scores
    
    def apply_efficiency_curves(self, raw_scores: Dict[str, float], inflation_patterns: Dict[str, float]) -> Dict[str, float]:
        """Apply sigmoid-based dampening with authority super-signal layer"""
        
        adjusted_scores = raw_scores.copy()
        
        # PHASE 2: AUTHORITY SUPER-SIGNAL LAYER
        # Compute authority strength from multiple signals
        authority_strength = self._compute_authority_strength(inflation_patterns)
        
        # Apply authority-based dampening reduction
        dampening_reduction = 1.0
        authority_boost = 1.0
        
        if authority_strength > 0.85:
            # Very high authority: reduce dampening by 80%, boost score by 20%
            dampening_reduction = 0.2
            authority_boost = 1.20  # 20% boost
        elif authority_strength > 0.75:
            # High authority: reduce dampening by 60%, boost score by 10%
            dampening_reduction = 0.4
            authority_boost = 1.10  # 10% boost
        elif authority_strength > 0.65:
            # Medium-high authority: reduce dampening by 40%
            dampening_reduction = 0.6
        
        # Apply dampening based on inflation patterns with authority consideration
        for category, raw_score in raw_scores.items():
            dampening_factor = 1.0
            
            # Category-specific dampening
            if category == "topical_authority":
                # Affected by entity inflation
                entity_inflation = inflation_patterns.get("entity_inflation", 0)
                if entity_inflation > 0:
                    # AUTHORITY PRESERVATION: Enhanced with authority super-signal
                    entity_data = inflation_patterns.get("entity_data", {})
                    uniqueness_ratio = entity_data.get("uniqueness_ratio", 0.5)
                    
                    # If high uniqueness, reduce inflation sensitivity significantly
                    if uniqueness_ratio > 0.8:  # Very high diversity - definitely legitimate
                        sensitivity_modifier = 0.2 * dampening_reduction
                    elif uniqueness_ratio > 0.7:  # High diversity - likely legitimate
                        sensitivity_modifier = 0.4 * dampening_reduction
                    elif uniqueness_ratio > 0.5:  # Medium diversity
                        sensitivity_modifier = 0.7 * dampening_reduction
                    else:
                        sensitivity_modifier = 1.0  # No reduction for low diversity
                    
                    # Apply sigmoid dampening with authority consideration
                    dampening_factor = 1.0 - (entity_inflation * sensitivity_modifier)
            
            elif category == "ai_impact":
                # Affected by structured data inflation
                structured_inflation = inflation_patterns.get("structured_inflation", 0)
                if structured_inflation > 0:
                    # AUTHORITY PRESERVATION: Check structured data quality
                    structured_data = inflation_patterns.get("structured_data", {})
                    has_organization = structured_data.get("has_organization", False)
                    cross_link_strength = structured_data.get("cross_link_strength", 0.5)
                    
                    # If strong structured signals, reduce sensitivity
                    if has_organization and cross_link_strength > 0.7:
                        sensitivity_modifier = 0.3 * dampening_reduction
                    elif has_organization:
                        sensitivity_modifier = 0.6 * dampening_reduction
                    else:
                        sensitivity_modifier = 1.0
                    
                    dampening_factor = 1.0 - (structured_inflation * sensitivity_modifier)
            
            # Apply the dampening factor
            adjusted_scores[category] = raw_score * dampening_factor
        
        # Apply authority boost to all categories (helps authority break ceiling)
        if authority_boost > 1.0:
            for category in adjusted_scores:
                adjusted_scores[category] = min(adjusted_scores[category] * authority_boost, 100.0)
        
        return adjusted_scores
    
    def _compute_authority_strength(self, inflation_patterns: Dict[str, float]) -> float:
        """Compute authority strength from multiple signals"""
        
        authority_signals = []
        
        # Entity uniqueness ratio
        entity_data = inflation_patterns.get("entity_data", {})
        uniqueness_ratio = entity_data.get("uniqueness_ratio", 0.5)
        authority_signals.append(uniqueness_ratio)
        
        # Entity type diversity
        entity_graph = entity_data.get("entity_graph", {})
        entities = entity_graph.get("entities", [])
        if entities:
            entity_types = set(entity.get("@type") for entity in entities if entity.get("@type"))
            diversity_ratio = len(entity_types) / max(len(entities), 1)
            authority_signals.append(diversity_ratio)
        
        # Entity relationship strength
        relationships = entity_graph.get("relationships", [])
        if entities and relationships:
            relationship_strength = min(len(relationships) / len(entities), 1.0)
            authority_signals.append(relationship_strength)
        
        # Content depth per entity
        content_metrics = entity_data.get("content_metrics", {})
        word_count = content_metrics.get("word_count", 1)
        depth_per_entity = min(word_count / max(len(entities), 1) / 100, 1.0)
        authority_signals.append(depth_per_entity)
        
        # Structured data richness
        structured_data = inflation_patterns.get("structured_data", {})
        structured_richness = structured_data.get("structured_richness", 0.5)
        authority_signals.append(structured_richness)
        
        # Consistency between Article + Organization + Person
        has_article = any(e.get("@type") == "Article" for e in entities)
        has_organization = any(e.get("@type") == "Organization" for e in entities)
        has_person = any(e.get("@type") == "Person" for e in entities)
        
        consistency_score = 0.5  # Base
        if has_article and has_organization and has_person:
            consistency_score = 1.0
        elif has_article and has_organization:
            consistency_score = 0.8
        elif has_organization:
            consistency_score = 0.6
        
        authority_signals.append(consistency_score)
        
        # Compute weighted mean of all authority signals
        if authority_signals:
            authority_strength = sum(authority_signals) / len(authority_signals)
        else:
            authority_strength = 0.5
        
        return authority_strength
    
    def apply_system_wide_spam_penalty(self, adjusted_scores: Dict[str, float], inflation_patterns: Dict[str, float], data: Dict[str, Any]) -> Dict[str, float]:
        """PHASE 3: Apply system-wide spam penalty instead of local penalties"""
        
        # Compute spam index from multiple signals
        spam_index = self._compute_spam_index(inflation_patterns, data)
        
        # Apply global dampening multiplier if spam detected
        if spam_index > 2.5:  # Threshold for spam detection
            # sigmoid(-spam_index + 3) gives:
            # - 0.95 at spam_index 1.0
            # - 0.73 at spam_index 2.0  
            # - 0.27 at spam_index 3.0
            # - 0.05 at spam_index 4.0
            global_dampening = 1 / (1 + math.exp(-(-spam_index + 3)))
            
            # Apply to ALL categories except voice_intent (preserve for gaming resistance)
            for category in adjusted_scores:
                if category != "voice_intent":
                    adjusted_scores[category] *= global_dampening
        
        return adjusted_scores
    
    def _compute_spam_index(self, inflation_patterns: Dict[str, float], data: Dict[str, Any]) -> float:
        """Compute spam index from multiple signals"""
        
        spam_signals = []
        
        # Entity density vs uniqueness ratio
        entity_data = inflation_patterns.get("entity_data", {})
        entity_graph = entity_data.get("entity_graph", {})
        entities = entity_graph.get("entities", [])
        content_metrics = data.get("content_metrics", {})
        word_count = content_metrics.get("word_count", 1)
        
        if entities and word_count > 0:
            entity_density = len(entities) / word_count * 1000
            uniqueness_ratio = entity_data.get("uniqueness_ratio", 0.5)
            
            if uniqueness_ratio > 0:
                density_uniqueness_ratio = entity_density / uniqueness_ratio
                spam_signals.append(min(density_uniqueness_ratio / 10, 5.0))
        
        # FAQ count vs answer depth
        faq_metrics = data.get("faq_metrics", {})
        faq_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        
        if faq_count > 0:
            answer_depth = qa_pairs / faq_count
            if answer_depth > 0:
                faq_depth_ratio = faq_count / answer_depth
                spam_signals.append(min(faq_depth_ratio / 5, 5.0))
        
        # Heading density vs paragraph density
        heading_metrics = data.get("heading_metrics", {})
        paragraph_count = content_metrics.get("paragraph_count", 1)
        
        total_headings = (heading_metrics.get("h1_count", 0) + 
                         heading_metrics.get("h2_count", 0) + 
                         heading_metrics.get("h3_count", 0))
        
        if paragraph_count > 0:
            heading_density = total_headings / paragraph_count
            spam_signals.append(min(heading_density * 2, 5.0))
        
        # Repetition score (simplified - could be enhanced)
        entity_types = [e.get("@type") for e in entities if e.get("@type")]
        if entity_types:
            type_counts = {}
            for entity_type in entity_types:
                type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
            
            # High repetition of same entity type
            max_repetition = max(type_counts.values()) if type_counts else 1
            repetition_ratio = max_repetition / len(entity_types)
            spam_signals.append(repetition_ratio * 3)
        
        # Compute weighted mean of spam signals
        if spam_signals:
            spam_index = sum(spam_signals) / len(spam_signals)
        else:
            spam_index = 0.0
        
        return spam_index
    
    def apply_density_validation(self, adjusted_scores: Dict[str, float], data: Dict[str, Any]) -> Dict[str, float]:
        """Apply content density validation with smooth sigmoid scaling"""
        
        content_metrics = data.get("content_metrics", {})
        word_count = content_metrics.get("word_count", 1)
        
        # Apply thin content smoothing using sigmoid
        quality_flags = data.get("quality_flags", {})
        if quality_flags.get("low_word_count", False):
            # Smooth sigmoid scaling for thin content (no sudden cliffs)
            thin_content_factor = 1 / (1 + math.exp(-(word_count - 200) / 100))  # Sigmoid centered at 200 words
            # Apply gentle scaling across all categories
            for category in adjusted_scores:
                adjusted_scores[category] *= (0.3 + 0.7 * thin_content_factor)  # Scale between 30% and 100%
        
        return adjusted_scores
    
    def _calculate_uniqueness_ratio(self, entities: List[Dict[str, Any]]) -> float:
        """Calculate entity uniqueness ratio"""
        if not entities:
            return 0.5
        
        entity_types = set()
        for entity in entities:
            if entity.get("@type"):
                entity_types.add(entity["@type"])
        
        return len(entity_types) / len(entities)
    
    def _calculate_cross_link_strength(self, entity_graph: Dict[str, Any]) -> float:
        """Calculate cross-link strength between entities"""
        relationships = entity_graph.get("relationships", [])
        entities = entity_graph.get("entities", [])
        
        if not entities or not relationships:
            return 0.5
        
        # Calculate ratio of actual relationships to possible relationships
        max_possible = len(entities) * (len(entities) - 1) / 2
        return min(len(relationships) / max_possible, 1.0)

def create_gaming_resistance_layer() -> GamingResistanceLayer:
    """Factory function to create gaming resistance layer"""
    return GamingResistanceLayer()
