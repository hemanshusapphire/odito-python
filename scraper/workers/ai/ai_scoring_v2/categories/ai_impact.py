"""
AI Impact Category — coordinator module.

Rules have been split into two focused sub-modules:
  - schema_quality.py   (PrimaryOrgSchema, CorrectType, SchemaValidJSONLD,
                         NoPluginDuplicateSchemas, SemanticHTMLTags)
  - crawlability.py     (RobotsTxtNonBlocking — drives the scoring-engine gate)

This file re-exports both registration functions and provides the unified
register_ai_impact_rules() entry-point so the categories __init__.py and any
other callers require no changes during the Week 1 transition.
"""

from .schema_quality import register_schema_quality_rules
from .crawlability import register_crawlability_rules


def register_ai_impact_rules(registry) -> None:
    """Register all AI Impact rules (schema quality + crawlability)."""
    register_schema_quality_rules(registry)
    register_crawlability_rules(registry)
