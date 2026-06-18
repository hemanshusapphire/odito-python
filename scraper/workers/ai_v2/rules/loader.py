"""
Rule loader.

Imports every card module, collects their RULES list, and registers all
38 rules into the singleton RuleRegistry.

Call load_all_rules() once at worker startup before any scoring.
"""

from .registry import RuleRegistry, _set_registry

from .categories.aiso.crawlability   import RULES as AISO_CRAWLABILITY
from .categories.aiso.citability     import RULES as AISO_CITABILITY
from .categories.aiso.authority      import RULES as AISO_AUTHORITY
from .categories.aiso.coverage       import RULES as AISO_COVERAGE

from .categories.aeo.answer_readiness  import RULES as AEO_ANSWER_READINESS
from .categories.aeo.question_coverage import RULES as AEO_QUESTION_COVERAGE
from .categories.aeo.faq_coverage      import RULES as AEO_FAQ_COVERAGE
from .categories.aeo.snippet_score     import RULES as AEO_SNIPPET_SCORE
from .categories.aeo.voice_search      import RULES as AEO_VOICE_SEARCH

from .categories.geo.entity_authority      import RULES as GEO_ENTITY_AUTHORITY
from .categories.geo.knowledge_graph_score import RULES as GEO_KNOWLEDGE_GRAPH
from .categories.geo.brand_corroboration   import RULES as GEO_BRAND_CORROBORATION

_ALL_RULES = (
    AISO_CRAWLABILITY
    + AISO_CITABILITY
    + AISO_AUTHORITY
    + AISO_COVERAGE
    + AEO_ANSWER_READINESS
    + AEO_QUESTION_COVERAGE
    + AEO_FAQ_COVERAGE
    + AEO_SNIPPET_SCORE
    + AEO_VOICE_SEARCH
    + GEO_ENTITY_AUTHORITY
    + GEO_KNOWLEDGE_GRAPH
    + GEO_BRAND_CORROBORATION
)


def load_all_rules() -> RuleRegistry:
    """
    Register every V2 rule and return the populated registry.
    Also sets the module-level singleton so get_registry() works.
    """
    registry = RuleRegistry()
    registry.register_all(_ALL_RULES)
    _set_registry(registry)
    print(f"✅ V2 rule registry loaded — {registry.total_rules()} rules across {len(registry.summary()['hubs'])} hubs")
    return registry
