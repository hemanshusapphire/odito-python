"""
AI Visibility Scoring v2 Categories Package

Contains all scoring category implementations.
"""

# Import all category registration functions
from .ai_impact import register_ai_impact_rules
from .citation_probability import register_citation_probability_rules
from .llm_readiness import register_llm_readiness_rules
from .aeo_score import register_aeo_score_rules
from .topical_authority import register_topical_authority_rules
from .voice_intent import register_voice_intent_rules

# Export registration functions
__all__ = [
    "register_ai_impact_rules",
    "register_citation_probability_rules", 
    "register_llm_readiness_rules",
    "register_aeo_score_rules",
    "register_topical_authority_rules",
    "register_voice_intent_rules"
]

def register_all_categories(rule_registry):
    """Register all categories in the rule registry"""
    register_ai_impact_rules(rule_registry)
    register_citation_probability_rules(rule_registry)
    register_llm_readiness_rules(rule_registry)
    register_aeo_score_rules(rule_registry)
    register_topical_authority_rules(rule_registry)
    register_voice_intent_rules(rule_registry)
