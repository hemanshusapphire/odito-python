"""
Voice & Intent Score Category Rules

Evaluates content optimization for voice search and user intent.
Focuses on conversational language, intent matching, and voice search readiness.
"""

from typing import Dict, Any
from rule_base import BaseRule

class BulletNumberedListsUsedRule(BaseRule):
    """Rule 51 — Bullet / numbered lists used"""
    
    def __init__(self):
        config = {
            "rule_id": "bullet_numbered_lists_used",
            "category": "voice_intent",
            "description": "Bullet / numbered lists used",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for bullet and numbered lists"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check for structured content (indicates lists)
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        
        # Multiple headings often indicate list-based content
        if h2_count >= 2 or h3_count >= 3:
            score += 5
        elif h2_count >= 1 or h3_count >= 1:
            score += 3
        
        # Check for short paragraphs (often used in lists)
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if short_ratio >= 0.2:
            score += 3
        elif short_ratio >= 0.1:
            score += 2
        
        # Check for content length (lists need sufficient content)
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 300:
            score += 2
        elif word_count >= 150:
            score += 1
        
        return min(score, self.max_score)

class ComparisonTablesPresentRule(BaseRule):
    """Rule 52 — Comparison tables present"""
    
    def __init__(self):
        config = {
            "rule_id": "comparison_tables_present",
            "category": "voice_intent",
            "description": "Comparison tables present",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for comparison tables"""
        # Check the actual extracted signal for tables
        ai_signals = data.get("ai_visibility_signals", {})
        tables = ai_signals.get("tables", {})
        
        # Return score based on actual table presence
        if tables.get("present", False):
            # Bonus points for more substantial tables
            rows = tables.get("rows", 0)
            headers = tables.get("headers", 0)
            
            if rows >= 3 and headers >= 2:
                return 10.0  # Full score for substantial tables
            elif rows >= 1:
                return 7.0   # Partial score for simple tables
            else:
                return 5.0   # Minimal score for table detection
        else:
            return 0.0  # No score if no tables present

class ConversationalToneRule(BaseRule):
    """Rule 53 — Conversational tone detected"""
    
    def __init__(self):
        config = {
            "rule_id": "conversational_tone",
            "category": "voice_intent",
            "description": "Conversational tone detected",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for conversational language patterns"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Check for question-based headings (conversational structure)
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        if question_headings >= 2:
            score += 4
        elif question_headings >= 1:
            score += 2
        
        # Check for Q&A pairs (direct conversational format)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        if qa_pairs >= 3:
            score += 3
        elif qa_pairs >= 1:
            score += 2
        
        # Check for reasonable sentence length (conversational = shorter)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 8 <= avg_sentence_length <= 18:
            score += 3  # Short, conversational sentences
        elif 6 <= avg_sentence_length <= 22:
            score += 1
        
        return min(score, self.max_score)

class StepByStepContentRule(BaseRule):
    """Rule 55 — Step-by-step / how-to content"""
    
    def __init__(self):
        config = {
            "rule_id": "step_by_step_content",
            "category": "voice_intent",
            "description": "Step-by-step / how-to content",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for step-by-step content patterns"""
        step_metrics = data.get("step_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for step-by-step detection via step_metrics
        steps_detected = step_metrics.get("steps_detected", 0)
        if steps_detected >= 3:
            score += 5
        elif steps_detected >= 1:
            score += 3
        
        # Check for how-to schema signal
        if step_metrics.get("howto_detected", False):
            score += 3
        
        # Check for numbered/ordered content via heading structure
        h3_count = heading_metrics.get("h3_count", 0)
        h3_count = h3_count[0] if isinstance(h3_count, list) else h3_count
        if h3_count >= 3:
            score += 2  # Multiple sub-headings suggest sequential steps
        
        return min(score, self.max_score)

# Register all Voice & Intent rules (4 rules)
def register_voice_intent_rules(registry):
    """Register all Voice & Intent category rules"""
    registry.register(BulletNumberedListsUsedRule())
    registry.register(ComparisonTablesPresentRule())
    registry.register(ConversationalToneRule())
    registry.register(StepByStepContentRule())
