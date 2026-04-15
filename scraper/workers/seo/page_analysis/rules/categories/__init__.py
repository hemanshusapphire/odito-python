"""
SEO Rule Categories — Central Registration

Imports and calls all category registration functions so that
all rules are registered with the SEORuleRegistry on startup.
"""

from ..seo_rule_registry import SEORuleRegistry

# Category registration imports - NEW RULES ONLY
from .internal_rules import register_internal_rules
from .link_rules import register_link_rules
from .redirect_rules import register_redirect_rules
from .content_rules import register_content_rules
from .social_rules import register_social_rules
from .image_rules import register_image_rules
from .crawlability_rules import register_crawlability_rules
from .schema_rules import register_schema_rules
from .eeat_rules import register_eeat_rules
from .accessibility_rules import register_accessibility_rules
from .seo_enhancements import register_seo_enhancement_rules


def register_all_seo_categories(registry=None):
    """Register all SEO rule categories with the registry."""
    if registry is None:
        registry = SEORuleRegistry()

    # Internal Pages
    register_internal_rules(registry)

    # Links & Redirects
    register_link_rules(registry)
    register_redirect_rules(registry)

    # Content & Meta
    register_content_rules(registry)

    # Social & Schema
    register_social_rules(registry)
    register_schema_rules(registry)

    # Images
    register_image_rules(registry)

    # Crawlability & Technical
    register_crawlability_rules(registry)

    # E-E-A-T & Trust
    register_eeat_rules(registry)

    # Accessibility
    register_accessibility_rules(registry)

    # SEO Enhancements (S1-S6)
    register_seo_enhancement_rules(registry)

    return registry
