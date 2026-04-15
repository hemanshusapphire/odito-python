"""
Rule Registry for AI Visibility Scoring v2

Manages registration and retrieval of all scoring rules.
"""

from typing import Dict, List, Type, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class RuleRegistry:
    """Registry for managing scoring rules"""
    
    def __init__(self):
        self._rules: Dict[str, BaseRule] = {}
        self._categories: Dict[str, List[BaseRule]] = {}
    
    def register(self, rule: BaseRule):
        """
        Register a rule in the registry
        
        Args:
            rule: Rule instance to register
        """
        if rule.rule_id in self._rules:
            raise ValueError(f"Rule {rule.rule_id} already registered")
        
        self._rules[rule.rule_id] = rule
        
        # Add to category mapping
        if rule.category not in self._categories:
            self._categories[rule.category] = []
        self._categories[rule.category].append(rule)
    
    def get_rule(self, rule_id: str) -> BaseRule:
        """Get a specific rule by ID"""
        if rule_id not in self._rules:
            raise ValueError(f"Rule {rule_id} not found")
        return self._rules[rule_id]
    
    def get_rules_by_category(self, category: str) -> List[BaseRule]:
        """Get all rules for a specific category"""
        return self._categories.get(category, [])
    
    def get_all_rules(self) -> List[BaseRule]:
        """Get all registered rules"""
        return list(self._rules.values())
    
    def get_categories(self) -> List[str]:
        """Get all available categories"""
        return list(self._categories.keys())
    
    def get_rule_count(self) -> int:
        """Get total number of registered rules"""
        return len(self._rules)
    
    def validate_registry(self) -> Dict[str, Any]:
        """
        Validate the registry for completeness
        
        Returns:
            Validation report
        """
        report = {
            "total_rules": len(self._rules),
            "categories": {},
            "issues": []
        }
        
        for category, rules in self._categories.items():
            category_report = {
                "rule_count": len(rules),
                "total_weight": sum(rule.weight for rule in rules),
                "rules": [{"id": rule.rule_id, "weight": rule.weight} for rule in rules]
            }
            report["categories"][category] = category_report
            
            # Check for issues
            if len(rules) < 8:
                report["issues"].append(f"Category {category} has fewer than 8 rules")
            if len(rules) > 15:
                report["issues"].append(f"Category {category} has more than 15 rules")
        
        return report

# Global registry instance
rule_registry = RuleRegistry()
