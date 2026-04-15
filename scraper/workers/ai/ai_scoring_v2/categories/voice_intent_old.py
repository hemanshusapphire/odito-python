"""
Voice & Intent Score Category Rules

Evaluates content optimization for voice search and user intent.
Focuses on conversational language, intent matching, and voice search readiness.
"""

from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule
from scraper.workers.ai.ai_scoring_v2.scoring_engine import safe_len

class IntentClarityRule(BaseRule):
    """Evaluates clarity and strength of user intent"""
    
    def __init__(self):
        config = {
            "rule_id": "intent_clarity",
            "category": "voice_intent",
            "description": "Evaluates clarity and strength of user intent",
            "weight": 2.0,
            "max_score": 30,  # Increased from 20
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate intent clarity"""
        intent_metrics = data.get("intent_metrics", {})
        
        score = 0
        
        # Check intent confidence
        confidence = intent_metrics.get("confidence", "low")
        if confidence == "high":
            score += 15  # Increased from 8
        elif confidence == "medium":
            score += 10  # Increased from 5
        elif confidence == "low":
            score += 5   # Increased from 2
        
        # Check dominant intent percentage
        intent_dist = intent_metrics.get("intent_distribution", {})
        if intent_dist:
            max_intent = max(intent_dist.values())
            if max_intent >= 60:
                score += 6
            elif max_intent >= 40:
                score += 4
            elif max_intent >= 25:
                score += 2
            elif max_intent >= 15:
                score += 1
        
        # Check for balanced intent distribution
        intents = list(intent_dist.values())
        if len(intents) >= 3:
            # Check if there's a clear primary intent
            sorted_intents = sorted(intents, reverse=True)
            if sorted_intents[0] >= sorted_intents[1] * 1.5:
                score += 4
            elif sorted_intents[0] >= sorted_intents[1] * 1.2:
                score += 2
            elif sorted_intents[0] > sorted_intents[1]:
                score += 1
        
        # Check for informational intent (voice search friendly)
        informational = intent_dist.get("informational", 0)
        if informational >= 30:
            score += 2
        
        return min(score, self.max_score)

class ConversationalLanguageRule(BaseRule):
    """Evaluates conversational language patterns"""
    
    def __init__(self):
        config = {
            "rule_id": "conversational_language",
            "category": "voice_intent",
            "description": "Evaluates conversational language patterns",
            "weight": 1.5,
            "max_score": 25,  # Increased from 15
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate conversational language with quality dominance"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        question_count = question_count[0] if isinstance(question_count, list) else question_count
        qa_pairs = qa_pairs[0] if isinstance(qa_pairs, list) else qa_pairs
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check sentence length (conversational)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 12 <= avg_sentence_length <= 18:
            score += 10  # Increased from 5
        
        # Check readability (conversational)
        readability = content_metrics.get("readability_score", 0)
        if 60 <= readability <= 80:
            score += 8   # Increased from 4
        elif 50 <= readability <= 85:
            score += 6   # Increased from 3
        elif 40 <= readability <= 90:
            score += 4   # Increased from 2
        elif readability > 0:
            score += 2   # Increased from 1
        
        # Check paragraph length (conversational)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 40 <= avg_paragraph_length <= 100:
            score += 3
        elif 30 <= avg_paragraph_length <= 120:
            score += 2
        elif 20 <= avg_paragraph_length <= 150:
            score += 1
        
        # Check for question headings (conversational)
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        if question_headings >= 2:
            score += 3
        elif question_headings >= 1:
            score += 2
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class VoiceSearchReadinessRule(BaseRule):
    """Evaluates voice search readiness factors"""
    
    def __init__(self):
        config = {
            "rule_id": "voice_search_readiness",
            "category": "voice_intent",
            "description": "Evaluates voice search readiness factors",
            "weight": 1.5,
            "max_score": 100,  # Increased dramatically to allow high scores
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate voice search readiness with quality validation"""
        faq_metrics = data.get("faq_metrics", {})
        content_metrics = data.get("content_metrics", {})
        intent_metrics = data.get("intent_metrics", {})
        
        score = 0
        
        # CONTINUOUS: FAQ presence with quality check (0-5 points)
        if faq_metrics.get("faq_detected"):
            score += 5
        
        # CONTINUOUS: Question count with answer quality ratio (0-4 points)
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        
        if question_count > 0:
            # Calculate answer quality ratio (estimated from qa_pairs)
            answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
            
            # DOMINANCE: Answer quality controls 70-80% of score
            # Structure (question count) limited to 20% maximum
            
            # Apply HARD CAP for poor quality content FIRST
            if answer_quality_ratio < 0.5:
                max_allowed_score = 15.0  # Very low cap for poor quality
            else:
                max_allowed_score = 100.0  # No cap for good quality
            
            # Calculate raw structure score (dramatically increased for good quality)
            if answer_quality_ratio >= 0.9 and question_count >= 30:
                raw_structure_score = 80.0  # Very high score for excellent quality + many questions
            elif answer_quality_ratio >= 0.8 and question_count >= 20:
                raw_structure_score = 60.0  # High score for good quality + many questions
            else:
                raw_structure_score = min(question_count / 5.0, 1.0) * 15.0  # Normal scoring
            
            # Apply sigmoid dominance curve based on answer quality
            # sigmoid(answer_quality_ratio * 8 - 4) gives:
            # - 0.02 at ratio 0.0
            # - 0.12 at ratio 0.25  
            # - 0.50 at ratio 0.50
            # - 0.88 at ratio 0.75
            # - 0.98 at ratio 1.00
            import math
            dominance = 1 / (1 + math.exp(-(answer_quality_ratio * 8 - 4)))
            
            # Final voice score is structure multiplied by quality dominance
            final_voice_score = raw_structure_score * dominance
            
            # Apply the hard cap
            final_voice_score = min(final_voice_score, max_allowed_score)
            
            score += final_voice_score
        
        # CONTINUOUS: Informational intent with paragraph depth (0-3 points)
        intent_dist = intent_metrics.get("intent_distribution", {})
        informational = intent_dist.get("informational", 0)
        paragraph_count = content_metrics.get("paragraph_count", 1)
        
        if informational > 0:
            # Check if informational content has sufficient depth
            paragraph_per_info = paragraph_count / max(informational / 20, 1)  # Paragraphs per 20% informational
            
            if paragraph_per_info >= 3:  # Good depth
                info_score = min(informational / 50.0, 1.0) * 3
            elif paragraph_per_info >= 1.5:  # Moderate depth
                info_score = min(informational / 60.0, 1.0) * 2
            else:  # Shallow content - penalty
                info_score = min(informational / 80.0, 1.0) * 1
            
            score += info_score
        
        # CONTINUOUS: Conversational readability with word count consideration (0-3 points)
        readability = content_metrics.get("readability_score", 0)
        word_count = content_metrics.get("word_count", 1)
        
        if readability > 0:
            # Optimal range for voice: 50-75, but consider content length
            if 50 <= readability <= 75:
                if word_count >= 300:  # Sufficient content
                    score += 3
                else:
                    score += 2  # Penalty for short content
            elif 40 <= readability <= 85:
                if word_count >= 500:
                    score += 2
                else:
                    score += 1
            else:
                # Poor readability - minimal score
                score += max(0, 1 - abs(readability - 62.5) / 37.5)
        
        return min(score, self.max_score)

class QuestionOptimizationRule(BaseRule):
    """Evaluates question optimization for voice search"""
    
    def __init__(self):
        config = {
            "rule_id": "question_optimization",
            "category": "voice_intent",
            "description": "Evaluates question optimization for voice search",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate question optimization with answer quality dominance"""
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Calculate raw structure score (question-based scoring)
        raw_structure_score = 0
        
        # Check question headings
        question_headings = heading_metrics.get("question_headings", 0)
        if safe_len(question_headings) >= 4:
            raw_structure_score += 4
        elif safe_len(question_headings) >= 2:
            raw_structure_score += 3
        elif safe_len(question_headings) >= 1:
            raw_structure_score += 2
        
        # Check Q&A pairs (already quality-filtered)
        if safe_len(qa_pairs) >= 3:
            raw_structure_score += 3
        elif safe_len(qa_pairs) >= 1:
            raw_structure_score += 2
        
        # Check total questions
        if safe_len(question_count) >= 6:
            raw_structure_score += 3
        elif safe_len(question_count) >= 3:
            raw_structure_score += 2
        elif safe_len(question_count) >= 1:
            raw_structure_score += 1
        
        # Apply sigmoid dominance curve based on answer quality
        import math
        dominance = 1 / (1 + math.exp(-(answer_quality_ratio * 8 - 4)))
        
        # Final score is structure multiplied by quality dominance
        final_score = raw_structure_score * dominance
        
        # Apply hard cap for poor quality content
        if answer_quality_ratio < 0.5:
            max_cap = 25
            final_score = min(final_score, max_cap)
        
        return min(final_score, self.max_score)

class NaturalLanguagePatternsRule(BaseRule):
    """Evaluates natural language patterns with quality consideration"""
    
    def __init__(self):
        config = {
            "rule_id": "natural_language_patterns",
            "category": "voice_intent",
            "description": "Evaluates natural language patterns with quality consideration",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate natural language patterns with quality dominance"""
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check sentence length variation (natural speech)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 14 <= avg_sentence_length <= 16:  # Very natural
            score += 4
        elif 12 <= avg_sentence_length <= 18:
            score += 3
        elif 10 <= avg_sentence_length <= 20:
            score += 2
        elif avg_sentence_length > 0:
            score += 1
        
        # Check paragraph length (natural speech chunks)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 50 <= avg_paragraph_length <= 80:
            score += 3
        elif 40 <= avg_paragraph_length <= 100:
            score += 2
        elif 30 <= avg_paragraph_length <= 120:
            score += 1
        
        # Check for balanced short paragraphs
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        if 0.2 <= short_ratio <= 0.4:
            score += 3
        elif 0.1 <= short_ratio <= 0.5:
            score += 2
        elif short_ratio >= 0.05:
            score += 1
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class IntentClarityRule(BaseRule):
    """Evaluates intent clarity with answer quality consideration"""
    
    def __init__(self):
        config = {
            "rule_id": "intent_clarity",
            "category": "voice_intent",
            "description": "Evaluates intent clarity with answer quality consideration",
            "weight": 1.5,
            "max_score": 20,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate intent clarity with answer quality dominance"""
        intent_metrics = data.get("intent_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check intent distribution clarity
        intent_dist = intent_metrics.get("intent_distribution", {})
        if intent_dist:
            # Check for dominant intent
            max_intent = max(intent_dist.values())
            if max_intent >= 50:
                score += 12
            elif max_intent >= 35:
                score += 9
            elif max_intent >= 25:
                score += 6
            elif max_intent >= 15:
                score += 3
        
        # Check confidence level
        confidence = intent_metrics.get("confidence", "low")
        if confidence == "high":
            score += 8
        elif confidence == "medium":
            score += 5
        elif confidence == "low":
            score += 2
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class UserIntentAlignmentRule(BaseRule):
    """Evaluates alignment with user intent with quality consideration"""
    
    def __init__(self):
        config = {
            "rule_id": "user_intent_alignment",
            "category": "voice_intent",
            "description": "Evaluates alignment with user intent with quality consideration",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate user intent alignment with quality dominance"""
        intent_metrics = data.get("intent_metrics", {})
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check intent distribution clarity
        intent_dist = intent_metrics.get("intent_distribution", {})
        if intent_dist:
            # Check for dominant intent
            max_intent = max(intent_dist.values())
            if max_intent >= 50:
                score += 4
            elif max_intent >= 35:
                score += 3
            elif max_intent >= 25:
                score += 2
            elif max_intent >= 15:
                score += 1
        
        # Check confidence level
        confidence = intent_metrics.get("confidence", "low")
        if confidence == "high":
            score += 3
        elif confidence == "medium":
            score += 2
        elif confidence == "low":
            score += 1
        
        # Check for informational intent (voice search primary)
        informational = intent_dist.get("informational", 0)
        if informational >= 35:
            score += 3
        elif informational >= 20:
            score += 2
        elif informational >= 10:
            score += 1
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class VoiceQueryCompatibilityRule(BaseRule):
    """Evaluates compatibility with voice queries with quality consideration"""
    
    def __init__(self):
        config = {
            "rule_id": "voice_query_compatibility",
            "category": "voice_intent",
            "description": "Evaluates compatibility with voice queries with quality consideration",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate voice query compatibility with quality dominance"""
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        question_count = question_count[0] if isinstance(question_count, list) else question_count
        qa_pairs = qa_pairs[0] if isinstance(qa_pairs, list) else qa_pairs
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check for direct answer capability
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        qa_pairs = qa_pairs[0] if isinstance(qa_pairs, list) else qa_pairs
        if qa_pairs >= 2:
            score += 3
        elif qa_pairs >= 1:
            score += 2
        
        # Check for question-based structure
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        if question_headings >= 2:
            score += 2
        elif question_headings >= 1:
            score += 1
        
        # Check for concise content
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 30 <= avg_paragraph_length <= 90:
            score += 2
        elif 20 <= avg_paragraph_length <= 120:
            score += 1
        
        # Check for readable content
        readability = content_metrics.get("readability_score", 0)
        if 55 <= readability <= 75:
            score += 3
        elif 45 <= readability <= 85:
            score += 2
        elif readability > 0:
            score += 1
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class ContextualRelevanceRule(BaseRule):
    """Evaluates contextual relevance for voice queries with quality consideration"""
    
    def __init__(self):
        config = {
            "rule_id": "contextual_relevance",
            "category": "voice_intent",
            "description": "Evaluates contextual relevance for voice queries with quality consideration",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate contextual relevance with quality dominance"""
        content_metrics = data.get("content_metrics", {})
        entity_metrics = data.get("entity_metrics", {})
        intent_metrics = data.get("intent_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Calculate answer quality ratio for quality dominance
        question_count = faq_metrics.get("question_count", 0)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        answer_quality_ratio = qa_pairs / question_count if question_count > 0 else 0
        
        # Apply quality dominance to ALL scoring
        if answer_quality_ratio < 0.5:
            quality_multiplier = 0.3  # Heavy penalty for poor quality
        elif answer_quality_ratio < 0.8:
            quality_multiplier = 0.7  # Moderate penalty for medium quality
        else:
            quality_multiplier = 1.0  # Full score for good quality
        
        # Check content length (sufficient for context)
        word_count = content_metrics.get("word_count", 0)
        if 500 <= word_count <= 1500:
            score += 3
        elif 300 <= word_count <= 2000:
            score += 2
        elif word_count >= 200:
            score += 1
        
        # Check entity density (context richness)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if 4 <= entity_density <= 10:
            score += 3
        elif 3 <= entity_density <= 12:
            score += 2
        elif entity_density >= 2:
            score += 1
        
        # Check intent clarity (context understanding)
        confidence = intent_metrics.get("confidence", "low")
        if confidence == "high":
            score += 4
        elif confidence == "medium":
            score += 2
        elif confidence == "low":
            score += 1
        
        # Apply quality dominance
        final_score = score * quality_multiplier
        
        return min(final_score, self.max_score)

class CommandPhraseDetectionRule(BaseRule):
    """Evaluates presence of command/action phrases for voice commands"""
    
    def __init__(self):
        config = {
            "rule_id": "command_phrase_detection",
            "category": "voice_intent",
            "description": "Evaluates presence of command/action phrases for voice commands",
            "weight": 0.6,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate command phrase detection"""
        intent_metrics = data.get("intent_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check for transactional intent (indicates commands)
        intent_dist = intent_metrics.get("intent_distribution", {})
        transactional = intent_dist.get("transactional", 0)
        commercial = intent_dist.get("commercial", 0)
        
        # Transactional/commercial intent often correlates with command phrases
        if transactional >= 20 or commercial >= 20:
            score += 3
        elif transactional >= 10 or commercial >= 10:
            score += 2
        elif transactional > 0 or commercial > 0:
            score += 1
        
        # Check for action-oriented headings (indicates instructions/commands)
        question_headings = heading_metrics.get("question_headings", 0)
        h2_count = heading_metrics.get("h2_count", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        
        # Non-question headings might indicate commands/how-to
        if h2_count > question_headings and h2_count > 0:
            action_ratio = (h2_count - question_headings) / h2_count
            if action_ratio >= 0.5:
                score += 4
            elif action_ratio >= 0.3:
                score += 3
            elif action_ratio > 0:
                score += 2
        
        # Check intent confidence (command detection works better with clear intent)
        confidence = intent_metrics.get("confidence", "low")
        if confidence == "high":
            score += 3
        elif confidence == "medium":
            score += 2
        elif confidence == "low":
            score += 1
        
        return min(score, self.max_score)

class ConversationalFlowQualityRule(BaseRule):
    """Evaluates natural conversational flow in content"""
    
    def __init__(self):
        config = {
            "rule_id": "conversational_flow_quality",
            "category": "voice_intent",
            "description": "Evaluates natural conversational flow in content",
            "weight": 0.7,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate conversational flow quality"""
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        intent_metrics = data.get("intent_metrics", {})
        
        score = 0
        
        # Check for FAQ presence (indicates conversational Q&A format)
        if faq_metrics.get("faq_detected"):
            qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
            if qa_pairs >= 3:
                score += 4
            elif qa_pairs >= 1:
                score += 2
        
        # Check sentence length variation (natural speech has variation)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        short_ratio = content_metrics.get("short_paragraph_ratio", 0)
        
        # Natural flow: moderate sentences with some short elements
        if 12 <= avg_sentence_length <= 18 and short_ratio >= 0.1:
            score += 3
        elif 10 <= avg_sentence_length <= 20 and short_ratio >= 0.05:
            score += 2
        elif avg_sentence_length > 0:
            score += 1
        
        # Check readability for conversational tone
        readability = content_metrics.get("readability_score", 0)
        if 55 <= readability <= 75:  # Sweet spot for conversational
            score += 3
        elif 45 <= readability <= 80:
            score += 2
        elif readability > 0:
            score += 1
        
        # Check for informational intent (conversational queries)
        intent_dist = intent_metrics.get("intent_distribution", {})
        informational = intent_dist.get("informational", 0)
        if informational >= 40:
            score += 2
        elif informational >= 25:
            score += 1
        
        return min(score, self.max_score)

# Register all Voice & Intent rules
def register_voice_intent_rules(registry):
    """Register all Voice & Intent category rules"""
    registry.register(IntentClarityRule())
    registry.register(ConversationalLanguageRule())
    registry.register(VoiceSearchReadinessRule())
    registry.register(QuestionOptimizationRule())
    registry.register(NaturalLanguagePatternsRule())
    registry.register(UserIntentAlignmentRule())
    registry.register(VoiceQueryCompatibilityRule())
    registry.register(ContextualRelevanceRule())
    registry.register(CommandPhraseDetectionRule())
    registry.register(ConversationalFlowQualityRule())
