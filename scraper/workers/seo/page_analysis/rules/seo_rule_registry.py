"""
SEO Rule Registry

Manages registration and retrieval of all modular SEO rules.
Follows the same pattern as ai_scoring_v2/rule_registry.py.
"""

from typing import Dict, List


class SEORuleRegistry:
    """Registry for managing modular SEO rules"""

    def __init__(self):
        self._rules: Dict[str, object] = {}          # rule_id -> rule instance
        self._categories: Dict[str, List[object]] = {}  # category -> [rules]

    def register(self, rule):
        """
        Register a rule in the registry.

        Args:
            rule: Instance of BaseSEORuleV2 subclass

        Raises:
            ValueError: If rule_id or rule_no already registered
        """
        if rule.rule_id in self._rules:
            raise ValueError(f"SEO Rule {rule.rule_id} already registered")

        self._rules[rule.rule_id] = rule

        # Add to category mapping
        if rule.category not in self._categories:
            self._categories[rule.category] = []
        self._categories[rule.category].append(rule)

    def get_rule(self, rule_id: str):
        """Get a specific rule by ID"""
        if rule_id not in self._rules:
            raise ValueError(f"SEO Rule {rule_id} not found")
        return self._rules[rule_id]

    def get_all_rules(self) -> list:
        """Get all registered rules in insertion order"""
        return list(self._rules.values())

    def get_rules_by_category(self, category: str) -> list:
        """Get all rules for a specific category"""
        return self._categories.get(category, [])

    def get_categories(self) -> list:
        """Get all available categories"""
        return list(self._categories.keys())

    def get_rule_count(self) -> int:
        """Get total number of registered rules"""
        return len(self._rules)

    def has_rule(self, rule_id: str) -> bool:
        """Check if a rule is registered"""
        return rule_id in self._rules
