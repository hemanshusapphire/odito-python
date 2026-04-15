"""
AEO (Answer Engine Optimization) Score Category Rules

Evaluates content optimization for answer engines and voice search.
Focuses on question-answer format, structured answers, and voice search readiness.
"""

import json
import re
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class First60WordsDirectAnswerRule(BaseRule):
    """Rule 27 — First 60 words contain direct answer"""
    
    def __init__(self):
        config = {
            "rule_id": "first_60_words_direct_answer",
            "category": "aeo_score",
            "description": "First 60 words contain direct answer",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if first 60 words contain direct answer"""
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for direct answer indicators in early content
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 60:
            score += 5
        elif word_count >= 40:
            score += 3
        elif word_count >= 20:
            score += 1
        
        # Check for concise opening (indicates direct answer)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 30 <= avg_paragraph_length <= 80:
            score += 3
        elif 20 <= avg_paragraph_length <= 100:
            score += 2
        
        # Check for question-answer structure
        faq_metrics = data.get("faq_metrics", {})
        if faq_metrics.get("faq_detected"):
            score += 2
        
        return min(score, self.max_score)

class FAQSchemaMatchesContentRule(BaseRule):
    """Rule 28 — FAQ schema matches visible content"""
    
    def __init__(self):
        config = {
            "rule_id": "faq_schema_matches_content",
            "category": "aeo_score",
            "description": "FAQ schema matches visible content",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": False  # FAQ is optional, not all pages need it
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if FAQ schema matches visible content"""
        faq_metrics = data.get("faq_metrics", {})
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        
        # Check for FAQ detection in content
        if faq_metrics.get("faq_detected"):
            score += 4
        
        # Check for FAQ schema
        if faq_metrics.get("faq_schema_detected"):
            score += 4
        
        # Check for Q&A pairs (indicates matching content)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        if qa_pairs >= 3:
            score += 2
        elif qa_pairs >= 1:
            score += 1
        
        return min(score, self.max_score)

class FAQSection5To10QuestionsRule(BaseRule):
    """Rule 42 — FAQ section 5–10 questions"""
    
    def __init__(self):
        config = {
            "rule_id": "faq_section_5_to_10_questions",
            "category": "aeo_score",
            "description": "FAQ section 5–10 questions",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": False  # FAQ section is optional
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for FAQ section with 5-10 questions"""
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        question_count = faq_metrics.get("question_count", 0)
        
        # Optimal range: 5-10 questions
        if 5 <= question_count <= 10:
            score += 8
        elif 3 <= question_count <= 12:
            score += 5
        elif question_count >= 1:
            score += 2
        
        # Bonus for FAQ schema presence
        if faq_metrics.get("faq_schema_detected"):
            score += 2
        
        return min(score, self.max_score)

class QuestionBasedH2HeadingsRule(BaseRule):
    """Rule 50 — Question-based H2 headings"""
    
    def __init__(self):
        config = {
            "rule_id": "question_based_h2_headings",
            "category": "aeo_score",
            "description": "Question-based H2 headings",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": False  # Question headings are optional
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for question-based H2 headings"""
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check for question headings
        question_headings = heading_metrics.get("question_headings", 0)
        question_headings = len(question_headings) if isinstance(question_headings, list) else question_headings
        
        if question_headings >= 3:
            score += 6
        elif question_headings >= 2:
            score += 4
        elif question_headings >= 1:
            score += 2
        
        # Check H2 count for context
        h2_count = heading_metrics.get("h2_count", 0)
        h2_count = h2_count[0] if isinstance(h2_count, list) else h2_count
        
        if h2_count >= 2:
            score += 2
        
        # Bonus for high question ratio
        if h2_count > 0 and question_headings / h2_count >= 0.5:
            score += 2
        
        return min(score, self.max_score)

class DirectAnswerFormatRule(BaseRule):
    """Rule 60 — Direct answer format"""
    
    def __init__(self):
        config = {
            "rule_id": "direct_answer_format",
            "category": "aeo_score",
            "description": "Direct answer format",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for direct answer format"""
        content_metrics = data.get("content_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        
        score = 0
        
        # Check for concise paragraphs (direct answers)
        avg_paragraph_length = content_metrics.get("avg_paragraph_length", 0)
        if 30 <= avg_paragraph_length <= 80:
            score += 4
        elif 20 <= avg_paragraph_length <= 100:
            score += 2
        
        # Check for Q&A pairs (direct format)
        qa_pairs = faq_metrics.get("qa_pairs_detected", 0)
        if qa_pairs >= 2:
            score += 3
        elif qa_pairs >= 1:
            score += 2
        
        # Check for short sentences (direct communication)
        avg_sentence_length = content_metrics.get("avg_sentence_length", 0)
        if 10 <= avg_sentence_length <= 18:
            score += 3
        elif 8 <= avg_sentence_length <= 22:
            score += 2
        
        return min(score, self.max_score)

class ContentCitesSourcesRule(BaseRule):
    """Rule 61 — Content cites sources"""
    
    def __init__(self):
        config = {
            "rule_id": "content_cites_sources",
            "category": "aeo_score",
            "description": "Content cites sources",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if content cites sources"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for citation references
        for item in graph:
            if item.get("@type") in ["Citation", "CreativeWork", "Article"]:
                if item.get("citation") or item.get("isPartOf") or item.get("sameAs"):
                    score += 4
                    break
        
        # Check for author attribution (source citation)
        for item in graph:
            if item.get("@type") in ["Person", "Organization"]:
                if item.get("name") and item.get("url"):
                    score += 3
                    break
        
        # Check for dataset or research references
        for item in graph:
            if item.get("@type") in ["Dataset", "ResearchProject"]:
                score += 3
                break
        
        return min(score, self.max_score)

# Register all AEO Score rules (6 rules)
def register_aeo_score_rules(registry):
    """Register all AEO Score category rules"""
    registry.register(First60WordsDirectAnswerRule())
    registry.register(FAQSchemaMatchesContentRule())
    registry.register(FAQSection5To10QuestionsRule())
    registry.register(QuestionBasedH2HeadingsRule())
    registry.register(DirectAnswerFormatRule())
    registry.register(ContentCitesSourcesRule())
