"""
Base Rule Structure for AI Visibility Scoring v2

All rules must follow this exact schema and implement the required methods.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import json

class BaseRule(ABC):
    """Base class for all AI visibility scoring rules"""
    
    def __init__(self, rule_config: Dict[str, Any]):
        """
        Initialize rule with configuration
        
        Required config fields:
        - rule_id: str
        - category: str  
        - description: str
        - weight: float
        - max_score: float
        - applies_to: "page" | "domain"
        - is_required: bool (whether missing feature should create issue)
        """
        self.rule_id = rule_config["rule_id"]
        self.category = rule_config["category"]
        self.description = rule_config["description"]
        self.weight = rule_config["weight"]
        self.max_score = rule_config["max_score"]
        self.applies_to = rule_config["applies_to"]
        self.is_required = rule_config.get("is_required", False)  # Default: not required
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate rule configuration with fault tolerance"""
        if not self.rule_id or not isinstance(self.rule_id, str):
            raise ValueError(f"Invalid rule_id: {self.rule_id}")
        
        if not self.category or not isinstance(self.category, str):
            raise ValueError(f"Invalid category: {self.category}")
        
        # [RULE_DEBUG] Log rule configuration
        print(f"[RULE_DEBUG] rule={self.rule_id} | weight={self.weight} | enabled={getattr(self, 'enabled', True)}")
        
        # Handle weight validation with auto-disable for zero weight
        if self.weight is None:
            print(f"[RULE_FIX] Rule {self.rule_id} has weight=None, defaulting to 1.0")
            self.weight = 1.0
        
        if not isinstance(self.weight, (int, float)):
            raise ValueError(f"Invalid weight type for {self.rule_id}: expected float/int, got {type(self.weight)}")
        
        if self.weight <= 0:
            print(f"[RULE_FIX] Rule {self.rule_id} has weight={self.weight}, auto-disabling")
            self.enabled = False
            # Don't raise exception - auto-disable instead
        
        # Validate other fields
        if not isinstance(self.max_score, (int, float)) or self.max_score <= 0:
            raise ValueError(f"Invalid max_score: {self.max_score}")
            
        if self.applies_to not in ["page", "domain"]:
            raise ValueError(f"Invalid applies_to: {self.applies_to}")
    
    @abstractmethod
    def evaluate(self, data: Dict[str, Any]) -> float:
        """
        Evaluate rule against data
        
        Args:
            data: Extraction data from seo_ai_visibility collection
            
        Returns:
            Raw score (0 to max_score)
        """
        pass
    
    def normalize(self, raw_score: float) -> float:
        """
        Normalize raw score to 0-100 scale
        
        Args:
            raw_score: Raw score from evaluate()
            
        Returns:
            Normalized score (0-100)
        """
        # Clamp to max_score first
        clamped_score = min(raw_score, self.max_score)
        # Ensure non-negative
        clamped_score = max(0, clamped_score)
        # Normalize to 0-100
        return (clamped_score / self.max_score) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary for storage"""
        return {
            "rule_id": self.rule_id,
            "category": self.category,
            "description": self.description,
            "weight": self.weight,
            "max_score": self.max_score,
            "applies_to": self.applies_to,
            "is_required": self.is_required
        }
    
    def __repr__(self):
        return f"Rule({self.rule_id}, {self.category}, weight={self.weight})"
