"""
LLM Readiness Category — coordinator module.

Rules have been split into two focused sub-modules:
  - citability_score.py  (ServicePages800Words, TopicClusters, ClearEntity,
                          SameAs, LastUpdated, StatisticsSourceLinks)
  - structure_score.py   (DescriptionMinimum50Chars, SemanticSubtopics)

This file re-exports both registration functions and provides the unified
register_llm_readiness_rules() entry-point so the categories __init__.py and
any other callers require no changes during the Week 1 transition.
"""

from .citability_score import register_citability_score_rules
from .structure_score import register_structure_score_rules


def register_llm_readiness_rules(registry) -> None:
    """Register all LLM Readiness rules (citability + structure score)."""
    register_citability_score_rules(registry)
    register_structure_score_rules(registry)
