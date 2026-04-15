"""
LLM Readiness Score Category Rules

Evaluates how well content is prepared for Large Language Model processing.
Focuses on clarity, structure, and machine-readability factors.
"""

from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class ContentStructureClarityRule(BaseRule):
    """Evaluates clarity of content structure for LLM processing"""
    
    def __init__(self):
        config = {
            "rule_id": "content_structure_clarity",
            "category": "llm_readiness",
            "description": "Evaluates clarity of content structure for LLM processing",
            "weight": 2.0,
            "max_score": 20,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content structure clarity"""
        heading_metrics = data.get("heading_metrics", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check heading hierarchy
        if heading_metrics.get("heading_sequence_valid"):
            score += 6
        
        # Check for proper H1 usage
        h1_count = heading_metrics.get("h1_count", 0)
        h1_count = h1_count[0] if isinstance(h1_count, list) else h1_count
        if h1_count == 1:
            score += 4
        elif h1_count == 0:
            score += 0
        else:
            score += 2  # Multiple H1s but not terrible
        
        # Check for sufficient H2s
        h2_count = heading_metrics.get("h2_count", 0)
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        if h2_count >= 3:
            score += 4
        elif h2_count >= 2:
            score += 3
        elif h2_count >= 1:
            score += 2
        
        # Check paragraph structure
        paragraph_count = content_metrics.get("paragraph_count", 0)
        paragraph_count = paragraph_count[0] if isinstance(paragraph_count, list) else paragraph_count
        if paragraph_count >= 5:
            score += 3
        elif paragraph_count >= 3:
            score += 2
        elif paragraph_count >= 1:
            score += 1
        
        # Check for question headings (LLM-friendly)
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        if question_headings >= 2:
            score += 3
        elif question_headings >= 1:
            score += 1
        
        return min(score, self.max_score)

class ReadabilityOptimizationRule(BaseRule):
    """Evaluates readability optimization for LLM comprehension"""
    
    def __init__(self):
        config = {
            "rule_id": "readability_optimization",
            "category": "llm_readiness",
            "description": "Evaluates readability optimization for LLM comprehension",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate readability optimization"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check Flesch readability score
        readability = content_metrics.get("readability_score", 0)
        if 50 <= readability <= 70:  # Optimal for LLM
            score += 6
        elif 40 <= readability <= 80:
            score += 4
        elif 30 <= readability <= 90:
            score += 2
        elif readability > 0:
            score += 1
        
        # Check sentence length
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 15 <= avg_sentence_length <= 20:  # Optimal range
            score += 4
        elif 12 <= avg_sentence_length <= 25:
            score += 3
        elif 10 <= avg_sentence_length <= 30:
            score += 2
        elif avg_sentence_length > 0:
            score += 1
        
        # Check paragraph length distribution
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        long_ratio = content_metrics.get("long_paragraph_ratio", 0)
        
        # Good mix of short and medium paragraphs
        if short_ratio >= 0.2 and long_ratio <= 0.3:
            score += 3
        elif short_ratio >= 0.1 and long_ratio <= 0.5:
            score += 2
        elif short_ratio >= 0.05:
            score += 1
        
        # Check average paragraph length
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 50 <= avg_paragraph_length <= 150:
            score += 2
        elif 30 <= avg_paragraph_length <= 200:
            score += 1
        
        return min(score, self.max_score)

class ContentLengthOptimizationRule(BaseRule):
    """Evaluates content length optimization for LLM processing"""
    
    def __init__(self):
        config = {
            "rule_id": "content_length_optimization",
            "category": "llm_readiness",
            "description": "Evaluates content length optimization for LLM processing",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content length optimization"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        word_count = content_metrics.get("word_count", 0)
        
        # Optimal range for LLM processing
        if 800 <= word_count <= 2000:
            score += 5
        elif 500 <= word_count <= 3000:
            score += 4
        elif 300 <= word_count <= 4000:
            score += 3
        elif 200 <= word_count <= 5000:
            score += 2
        elif word_count >= 100:
            score += 1
        
        # Check for sufficient content for meaningful analysis
        if word_count >= 300:
            score += 3
        elif word_count >= 150:
            score += 2
        elif word_count >= 50:
            score += 1
        
        # Penalty for extremely short content
        if word_count < 50:
            score = max(0, score - 2)
        
        return min(score, self.max_score)

class LanguageClarityRule(BaseRule):
    """Evaluates language clarity and simplicity"""
    
    def __init__(self):
        config = {
            "rule_id": "language_clarity",
            "category": "llm_readiness",
            "description": "Evaluates language clarity and simplicity",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate language clarity"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check sentence complexity (inverse of avg length)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 10 <= avg_sentence_length <= 18:  # Clear, simple sentences
            score += 4
        elif 8 <= avg_sentence_length <= 22:
            score += 3
        elif 6 <= avg_sentence_length <= 25:
            score += 2
        elif avg_sentence_length > 0:
            score += 1
        
        # Check paragraph complexity
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 40 <= avg_paragraph_length <= 120:  # Manageable paragraphs
            score += 3
        elif 30 <= avg_paragraph_length <= 150:
            score += 2
        elif 20 <= avg_paragraph_length <= 180:
            score += 1
        
        # Check for balanced paragraph distribution
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if 0.1 <= short_ratio <= 0.4:  # Good mix
            score += 3
        
        return min(score, self.max_score)

class SemanticCoherenceRule(BaseRule):
    """Evaluates semantic coherence and topic consistency"""
    
    def __init__(self):
        config = {
            "rule_id": "semantic_coherence",
            "category": "llm_readiness",
            "description": "Evaluates semantic coherence and topic consistency",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate semantic coherence"""
        entity_metrics = data.get("entity_metrics", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check entity density (indicates focused content)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if 5 <= entity_density <= 15:  # Good density
            score += 4
        elif 3 <= entity_density <= 20:
            score += 3
        elif 2 <= entity_density <= 25:
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
        
        if 2 <= mention_ratio <= 8:  # Good consistency
            score += 3
        elif 1 <= mention_ratio <= 12:
            score += 2
        elif mention_ratio >= 0.5:
            score += 1
        
        # Check for reasonable entity diversity
        unique_types = entity_metrics.get("unique_entity_types", 0)
        if 2 <= unique_types <= 6:  # Focused but comprehensive
            score += 3
        elif 1 <= unique_types <= 8:
            score += 2
        elif unique_types >= 1:
            score += 1
        
        return min(score, self.max_score)

class TechnicalFormattingRule(BaseRule):
    """Evaluates technical formatting for LLM processing"""
    
    def __init__(self):
        config = {
            "rule_id": "technical_formatting",
            "category": "llm_readiness",
            "description": "Evaluates technical formatting for LLM processing",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate technical formatting"""
        quality_flags = data.get("quality_flags", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check for proper structure
        if not quality_flags.get("malformed_structure", True):
            score += 4
        
        # Check for main content detection
        if not quality_flags.get("no_main_content_detected", True):
            score += 3
        
        # Check heading structure score
        structure_score = heading_metrics.get("heading_structure_score_input", 0)
        if structure_score >= 80:
            score += 3
        elif structure_score >= 60:
            score += 2
        elif structure_score >= 40:
            score += 1
        
        return min(score, self.max_score)

class VocabularyComplexityRule(BaseRule):
    """Evaluates vocabulary complexity for LLM understanding"""
    
    def __init__(self):
        config = {
            "rule_id": "vocabulary_complexity",
            "category": "llm_readiness",
            "description": "Evaluates vocabulary complexity for LLM understanding",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate vocabulary complexity"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Use readability as proxy for vocabulary complexity
        readability = content_metrics.get("readability_score", 0)
        
        # Optimal range indicates balanced vocabulary
        if 40 <= readability <= 70:
            score += 5
        elif 30 <= readability <= 80:
            score += 4
        elif 20 <= readability <= 85:
            score += 3
        elif 10 <= readability <= 90:
            score += 2
        elif readability > 0:
            score += 1
        
        # Check sentence length variation (indicates vocabulary variety)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 12 <= avg_sentence_length <= 22:  # Balanced complexity
            score += 3
        elif 10 <= avg_sentence_length <= 25:
            score += 2
        elif 8 <= avg_sentence_length <= 28:
            score += 1
        
        # Check paragraph length (indicates expression complexity)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 60 <= avg_paragraph_length <= 140:  # Good balance
            score += 2
        elif 40 <= avg_paragraph_length <= 180:
            score += 1
        
        return min(score, self.max_score)

class ContentOrganizationRule(BaseRule):
    """Evaluates overall content organization for LLM processing"""
    
    def __init__(self):
        config = {
            "rule_id": "content_organization",
            "category": "llm_readiness",
            "description": "Evaluates overall content organization for LLM processing",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content organization"""
        heading_metrics = data.get("heading_metrics", {})
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        step_metrics = data.get("step_metrics", {})
        
        score = 0
        
        # Check heading hierarchy completeness
        h1_count = heading_metrics.get("h1_count", 0)
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        
        # Safe type handling
        h1_count = h1_count[0] if isinstance(h1_count, list) else h1_count
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        
        if h1_count == 1 and h2_count >= 2:
            score += 4
        elif h1_count >= 1 and h2_count >= 1:
            score += 3
        elif h1_count >= 1:
            score += 2
        
        # Check for logical content flow
        if h3_count >= 1 and h2_count >= 2:
            score += 3
        elif h2_count >= 3:
            score += 2
        elif h2_count >= 1:
            score += 1
        
        # Check for structured content types
        if faq_metrics.get("faq_detected"):
            score += 2
        
        if step_metrics.get("step_section_present"):
            score += 2
        
        # Check paragraph organization
        paragraph_count = content_metrics.get("paragraph_count", 0)
        paragraph_count = paragraph_count[0] if isinstance(paragraph_count, list) else paragraph_count
        if paragraph_count >= 4:
            score += 2
        elif paragraph_count >= 2:
            score += 1
        
        # Check for question-based organization
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        if question_headings >= 1:
            score += 2
        
        return min(score, self.max_score)

class ListStructureQualityRule(BaseRule):
    """Evaluates list and table structure quality for LLM parsing"""
    
    def __init__(self):
        config = {
            "rule_id": "list_structure_quality",
            "category": "llm_readiness",
            "description": "Evaluates list and table structure quality for LLM parsing",
            "weight": 0.6,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate list structure quality"""
        content_metrics = data.get("content_metrics", {})
        step_metrics = data.get("step_metrics", {})
        
        score = 0
        
        # Check for step sections (indicates structured list content)
        if step_metrics.get("step_section_present"):
            step_count = step_metrics.get("step_count", 0)
            if step_count >= 5:
                score += 4
            elif step_count >= 3:
                score += 3
            elif step_count >= 1:
                score += 2
        
        # Check paragraph structure for list-like patterns
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if 0.2 <= short_ratio <= 0.5:  # Good balance of short paragraphs (list items)
            score += 3
        elif 0.1 <= short_ratio <= 0.6:
            score += 2
        elif short_ratio >= 0.05:
            score += 1
        
        # Check paragraph count vs sentence count for structure
        paragraph_count = content_metrics.get("paragraph_count", 0)
        if 3 <= paragraph_count <= 15:  # Optimal range for structured content
            score += 3
        elif 2 <= paragraph_count <= 20:
            score += 2
        elif paragraph_count >= 1:
            score += 1
        
        return min(score, self.max_score)

class ContentChunkingRule(BaseRule):
    """Evaluates content chunking for optimal LLM token processing"""
    
    def __init__(self):
        config = {
            "rule_id": "content_chunking",
            "category": "llm_readiness",
            "description": "Evaluates content chunking for optimal LLM token processing",
            "weight": 0.5,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content chunking"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check average paragraph length (optimal chunking)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 40 <= avg_paragraph_length <= 100:  # Optimal chunk size
            score += 4
        elif 30 <= avg_paragraph_length <= 120:
            score += 3
        elif 20 <= avg_paragraph_length <= 150:
            score += 2
        elif avg_paragraph_length > 0:
            score += 1
        
        # Check sentence length variation (indicates good chunking)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 12 <= avg_sentence_length <= 20:  # Standard sentence length
            score += 3
        elif 10 <= avg_sentence_length <= 25:
            score += 2
        elif avg_sentence_length > 0:
            score += 1
        
        # Check for heading-based sectioning (helps chunking)
        h2_count = heading_metrics.get("h2_count", 0)
        if h2_count >= 3:
            score += 3
        elif h2_count >= 2:
            score += 2
        elif h2_count >= 1:
            score += 1
        
        return min(score, self.max_score)

class SemanticHeadingQualityRule(BaseRule):
    """Evaluates semantic quality of headings for LLM understanding"""
    
    def __init__(self):
        config = {
            "rule_id": "semantic_heading_quality",
            "category": "llm_readiness",
            "description": "Evaluates semantic quality of headings for LLM understanding",
            "weight": 0.7,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate semantic heading quality"""
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Check for question headings (high semantic value)
        question_headings = heading_metrics.get("question_headings", 0)
        h2_count = heading_metrics.get("h2_count", 0)
        
        # Safe type handling
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        
        if question_headings >= 1 and h2_count >= 1:
            # Ratio of question headings to total headings
            question_ratio = question_headings / max(h2_count, 1)
            if question_ratio >= 0.3:  # 30%+ are questions
                score += 5
            elif question_ratio >= 0.2:
                score += 4
            elif question_ratio >= 0.1:
                score += 2
            elif question_ratio > 0:
                score += 1
        elif question_headings >= 1:
            score += 2
        
        # Check heading hierarchy depth (semantic structure)
        h3_count = heading_metrics.get("h3_count", 0)
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        if h2_count >= 2 and h3_count >= 2:
            score += 3
        elif h2_count >= 2 and h3_count >= 1:
            score += 2
        elif h2_count >= 1:
            score += 1
        
        # Bonus for FAQ presence (semantic Q&A structure)
        if faq_metrics.get("faq_detected"):
            score += 2
        
        return min(score, self.max_score)

# Register all LLM Readiness rules
def register_llm_readiness_rules(registry):
    """Register all LLM Readiness category rules"""
    registry.register(ContentStructureClarityRule())
    registry.register(ReadabilityOptimizationRule())
    registry.register(ContentLengthOptimizationRule())
    registry.register(LanguageClarityRule())
    registry.register(SemanticCoherenceRule())
    registry.register(TechnicalFormattingRule())
    registry.register(VocabularyComplexityRule())
    registry.register(ContentOrganizationRule())
    registry.register(ListStructureQualityRule())
    registry.register(ContentChunkingRule())
    registry.register(SemanticHeadingQualityRule())
