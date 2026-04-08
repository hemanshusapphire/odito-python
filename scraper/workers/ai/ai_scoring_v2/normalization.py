"""
Normalization utilities for AI Visibility Scoring v2

Provides deterministic normalization functions for scores and categories.
"""

from typing import Dict, List, Any
import math

class ScoreNormalizer:
    """Handles score normalization with deterministic output"""
    
    @staticmethod
    def clamp_score(score: float, min_val: float = 0.0, max_val: float = 100.0) -> float:
        """
        Clamp score between min and max values
        
        Args:
            score: Raw score to clamp
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            Clamped score
        """
        return max(min_val, min(max_val, score))
    
    @staticmethod
    def normalize_rule_score(raw_score: float, max_score: float) -> float:
        """
        Normalize individual rule score to 0-100 scale
        
        Args:
            raw_score: Raw score from rule evaluation
            max_score: Maximum possible score for the rule
            
        Returns:
            Normalized score (0-100)
        """
        if max_score <= 0:
            return 0.0
        
        # Clamp to max_score first
        clamped_raw = ScoreNormalizer.clamp_score(raw_score, 0, max_score)
        
        # Normalize to 0-100
        normalized = (clamped_raw / max_score) * 100
        
        # Ensure deterministic rounding
        return round(normalized, 3)
    
    @staticmethod
    def calculate_category_score(rule_scores: List[Dict[str, Any]], category_name: str = "") -> float:
        """
        Calculate weighted category score
        
        Formula: sum(rule_score * rule_weight) / sum(rule_weight) * 100
        
        Args:
            rule_scores: List of rule results with 'score' and 'weight'
            category_name: Name of the category (for special handling)
            
        Returns:
            Category score (0-100)
        """
        if not rule_scores:
            return 0.0
        
        total_weighted_score = 0.0
        total_weight = 0.0
        
        for rule_result in rule_scores:
            score = rule_result.get("score", 0.0)
            weight = rule_result.get("weight", 1.0)
            
            total_weighted_score += score * weight
            total_weight += weight
        
        if total_weight == 0:
            return 0.0
        
        # Calculate weighted average
        weighted_average = total_weighted_score / total_weight
        
        # No special category boosts - all categories treated equally
        
        # Clamp and round for deterministic output
        return round(ScoreNormalizer.clamp_score(weighted_average), 3)
    
    @staticmethod
    def calculate_overall_score(category_scores: Dict[str, float], 
                              category_weights: Dict[str, float]) -> float:
        """
        Calculate overall page score from category scores
        
        Formula: sum(category_score * category_weight) / 100
        
        Args:
            category_scores: Dictionary of category scores
            category_weights: Dictionary of category weights (must sum to 100)
            
        Returns:
            Overall score (0-100)
        """
        if not category_scores or not category_weights:
            return 0.0
        
        total_weighted_score = 0.0
        
        for category, score in category_scores.items():
            weight = category_weights.get(category, 0.0)
            total_weighted_score += score * weight
        
        # Divide by 100 since weights sum to 100
        overall_score = total_weighted_score / 100
        
        # Clamp and round for deterministic output
        return round(ScoreNormalizer.clamp_score(overall_score), 3)
    
    @staticmethod
    def calculate_website_score(page_scores: List[float]) -> float:
        """
        Calculate website-level score as average of page scores
        
        Args:
            page_scores: List of page scores
            
        Returns:
            Website score (0-100)
        """
        if not page_scores:
            return 0.0
        
        # Calculate average
        average_score = sum(page_scores) / len(page_scores)
        
        # Clamp and round for deterministic output
        return round(ScoreNormalizer.clamp_score(average_score), 3)

class CategoryWeights:
    """Defines category weights for scoring"""
    
    # Default weights - must sum to 100
    DEFAULT_WEIGHTS = {
        "ai_impact": 15.0,           # AI processing optimization
        "citation_probability": 15.0, # Citation likelihood
        "llm_readiness": 15.0,       # LLM processing readiness
        "aeo_score": 15.0,           # Answer engine optimization
        "topical_authority": 15.0,    # Topical authority
        "voice_intent": 25.0          # Voice search and intent (increased for gaming resistance)
    }
    
    @staticmethod
    def validate_weights(weights: Dict[str, float]) -> bool:
        """
        Validate that weights sum to 100 and all categories are present
        
        Args:
            weights: Dictionary of category weights
            
        Returns:
            True if valid, False otherwise
        """
        required_categories = set(CategoryWeights.DEFAULT_WEIGHTS.keys())
        provided_categories = set(weights.keys())
        
        # Check all required categories are present
        if not required_categories.issubset(provided_categories):
            return False
        
        # Check weights sum to 100 (with small tolerance for floating point)
        total_weight = sum(weights.values())
        return abs(total_weight - 100.0) < 0.001
    
    @staticmethod
    def get_default_weights() -> Dict[str, float]:
        """Get default category weights"""
        return CategoryWeights.DEFAULT_WEIGHTS.copy()

class ScoreValidator:
    """Validates scoring calculations and outputs"""
    
    @staticmethod
    def validate_rule_result(rule_result: Dict[str, Any]) -> bool:
        """
        Validate individual rule result structure
        
        Args:
            rule_result: Rule result dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ["rule_id", "category", "score", "weight", "max_score"]
        
        # Check required fields
        for field in required_fields:
            if field not in rule_result:
                return False
        
        # Check score ranges (normalized scores should be 0-100)
        score = rule_result.get("score", 0.0)
        max_score = rule_result.get("max_score", 0.0)
        weight = rule_result.get("weight", 0.0)
        
        if not (0 <= score <= 100):
            return False
        
        if weight <= 0:
            return False
        
        if max_score <= 0:
            return False
        
        return True
    
    @staticmethod
    def validate_category_breakdown(category_breakdown: Dict[str, Any]) -> bool:
        """
        Validate category breakdown structure
        
        Args:
            category_breakdown: Category breakdown dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ["category", "score", "rule_count", "rule_results"]
        
        # Check required fields
        for field in required_fields:
            if field not in category_breakdown:
                return False
        
        # Check score range
        score = category_breakdown.get("score", 0.0)
        if not (0 <= score <= 100):
            return False
        
        # Check rule results
        rule_results = category_breakdown.get("rule_results", [])
        if not isinstance(rule_results, list):
            return False
        
        # Validate each rule result
        for rule_result in rule_results:
            if not ScoreValidator.validate_rule_result(rule_result):
                return False
        
        return True
    
    @staticmethod
    def validate_page_score(page_score: Dict[str, Any]) -> bool:
        """
        Validate complete page score structure
        
        Args:
            page_score: Page score dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            "page_url", "page_ai_score", "category_scores", 
            "rule_breakdown", "scoring_version"
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in page_score:
                return False
        
        # Check overall score range
        overall_score = page_score.get("page_ai_score", 0.0)
        if not (0 <= overall_score <= 100):
            return False
        
        # Check category scores
        category_scores = page_score.get("category_scores", {})
        if not isinstance(category_scores, dict):
            return False
        
        for category, score in category_scores.items():
            if not (0 <= score <= 100):
                return False
        
        # Check rule breakdown
        rule_breakdown = page_score.get("rule_breakdown", [])
        if not isinstance(rule_breakdown, list):
            return False
        
        for rule_result in rule_breakdown:
            if not ScoreValidator.validate_rule_result(rule_result):
                return False
        
        return True
