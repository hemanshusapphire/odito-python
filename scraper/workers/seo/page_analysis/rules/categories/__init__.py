"""
SEO Rule Categories — Central Registration

Imports and calls all category registration functions so that
all rules are registered with the SEORuleRegistry on startup.
"""

from ..seo_rule_registry import SEORuleRegistry

# Category registration imports
from .title_rules import register_title_rules
from .meta_rules import register_meta_rules
from .heading_rules import register_heading_rules
from .image_rules import register_image_rules
from .social_rules import register_social_rules
from .schema_rules import register_schema_rules
from .technical_rules import register_technical_rules
from .international_rules import register_international_rules
from .crawlability_rules import register_crawlability_rules
from .performance_rules import register_performance_rules
from .accessibility_rules import register_accessibility_rules
from .general_rules import register_general_rules
from .tracking_rules import register_tracking_rules
from .content_rules import register_content_rules  # legacy, now empty


def register_all_seo_categories(registry=None):
    """Register all SEO rule categories with the registry."""
    if registry is None:
        registry = SEORuleRegistry()

    # Core SEO
    register_title_rules(registry)
    register_meta_rules(registry)
    register_heading_rules(registry)
    register_image_rules(registry)

    # Social & Schema
    register_social_rules(registry)
    register_schema_rules(registry)

    # Technical & Crawlability
    register_technical_rules(registry)
    register_crawlability_rules(registry)

    # International
    register_international_rules(registry)

    # Performance & Accessibility
    register_performance_rules(registry)
    register_accessibility_rules(registry)

    # Content Quality, Security, E-E-A-T
    register_general_rules(registry)

    # Tracking
    register_tracking_rules(registry)

    # Legacy (no-op, kept for compat)
    register_content_rules(registry)

    return registry
