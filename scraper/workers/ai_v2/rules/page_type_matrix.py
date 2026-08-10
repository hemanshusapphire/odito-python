"""
Page-Type Aware Schema Validation Matrix
=========================================

Single source of truth for page-type gating in the V2 rule engine.

Three exports consumed by registry.py:

    SKIP_MATRIX            dict[rule_id, set[page_type]]
        Rules are SKIPPED when the page's detected type is in their skip set.
        Domain-scope rules (AISO-001–010) are never in this matrix — the
        registry gate bypasses domain rules before consulting it.

    GEO_LOCALBUSINESS_RULES  frozenset[rule_id]
        GEO rules that depend on LocalBusiness schema being present.
        Used by the registry gate for the "generic" page-type fallback:
        if page_type == "generic", these rules skip unless
        page["schema"]["local_business"]["present"] is True.

    EXPECTED_SCHEMAS       dict[page_type, dict]
        Documentation-only matrix — not used by the rule gate.
        Shows which schemas are required vs optional per page type.
        Consumed by future frontend "schema checklist" features.

Canonical page types (lowercase, from page_type_detector.py):
    homepage  article   service   product   collection
    faq       about     contact   pricing   legal
    listing   blog      generic

Adding a new schema rule
------------------------
1. Write the rule class in rules/categories/geo/schema_coverage.py (or the
   appropriate card module) and register it in loader.py.
2. Add its RULE_ID here with the set of page types where it should skip.
   For schema_coverage rules: skip = ALL_PAGE_TYPES minus the applicable set.
3. No other files need changing.

Adding a new page type
----------------------
1. Add the type to page_type_detector.py canonical list and detection logic.
2. Review every entry in SKIP_MATRIX and add the new type where appropriate.
3. Add a row to EXPECTED_SCHEMAS for documentation.
"""

from typing import Any

# ── Skip sets shared across multiple GEO rules ───────────────────────────────
# LocalBusiness is expected on: homepage, contact, location, about
# NOT expected on the types listed in this set.
_GEO_LB_SKIP: frozenset[str] = frozenset({
    "service", "article", "blog", "product",
    "faq", "pricing", "legal", "collection", "listing",
})

# ── SKIP_MATRIX ───────────────────────────────────────────────────────────────

SKIP_MATRIX: dict[str, frozenset[str]] = {

    # ── GEO: Entity Authority ─────────────────────────────────────────────────
    # LocalBusiness required on: homepage, contact, location, about
    # → skip everywhere else
    "GEO-059": _GEO_LB_SKIP,
    "GEO-060": _GEO_LB_SKIP,
    "GEO-061": _GEO_LB_SKIP,

    # ── GEO: Knowledge Graph Score ────────────────────────────────────────────
    # City mention / Maps embed only relevant alongside LocalBusiness
    "GEO-062": _GEO_LB_SKIP,
    "GEO-063": _GEO_LB_SKIP,

    # ── GEO: Brand Corroboration ──────────────────────────────────────────────
    # NAP consistency, SameAs links, AggregateRating: tied to LocalBusiness
    "GEO-G1": _GEO_LB_SKIP,
    "GEO-G2": _GEO_LB_SKIP,
    "GEO-G3": _GEO_LB_SKIP,
    # GEO-065 (hreflang): multilingual signal — evaluates on all page types

    # ── AEO: FAQ Coverage ─────────────────────────────────────────────────────
    # FAQPage schema expected only on faq and article pages
    "AEO-048": frozenset({
        "homepage", "contact", "pricing", "legal",
        "product", "service", "collection", "listing", "about",
    }),

    # ── AISO: Coverage ────────────────────────────────────────────────────────
    # FAQ/HowTo schema is only reasonably expected on informational/content
    # pages (article, blog, faq, service, product, about, homepage, listing,
    # collection). Transactional/utility page types (contact, legal, pricing)
    # are not expected to carry FAQ or HowTo markup — skip rather than FAIL.
    "AISO-CV7": frozenset({"contact", "legal", "pricing"}),

    # ── AEO: Answer Readiness ─────────────────────────────────────────────────
    # Direct answer / intro depth / filler only meaningful on content pages
    "AEO-046": frozenset({"contact", "pricing", "legal", "collection", "listing"}),
    "AEO-050": frozenset({"contact", "pricing", "legal", "collection", "listing"}),
    "AEO-053": frozenset({"contact", "pricing", "legal", "collection", "listing"}),

    # ── AEO: Question Coverage ────────────────────────────────────────────────
    # Question H2 ratio only meaningful on content pages
    "AEO-047": frozenset({"contact", "pricing", "legal", "collection", "listing"}),
    # AEO-049 (HowTo): uses internal is_process_page detection — no matrix entry

    # ── AEO: Snippet Score ────────────────────────────────────────────────────
    # List formatting and authority links only meaningful on content pages
    "AEO-051": frozenset({"contact", "pricing", "legal", "collection", "listing"}),
    # AEO-052 (comparison tables): uses internal has_comparison_intent — no entry
    "AEO-054": frozenset({"contact", "pricing", "legal", "collection", "listing"}),

    # ── AEO: Voice Search ─────────────────────────────────────────────────────
    # Speakable schema not meaningful on purely transactional / legal pages
    "AEO-055": frozenset({"contact", "pricing", "legal", "collection", "listing"}),

    # ── AISO: Authority ───────────────────────────────────────────────────────
    # Author schema only expected on content pages
    "AISO-A2": frozenset({
        "contact", "pricing", "legal", "collection", "listing", "product",
    }),
    # Organization schema expected on: homepage, contact, about, generic
    # (LocalBusiness is an Organization subtype — local pages satisfy this rule)
    "AISO-A1": frozenset({
        "service", "article", "blog", "faq", "product",
        "pricing", "legal", "collection", "listing",
    }),
    # AISO-A3 (About info): universal — no entry

    # ── GEO: Schema Coverage ──────────────────────────────────────────────────
    # Each rule applies ONLY to its target page type; skip all others.
    # service pages → Service schema
    "GEO-S1": frozenset({
        "homepage", "article", "faq", "about", "contact", "pricing", "legal",
        "listing", "blog", "product", "collection", "generic",
    }),
    # article + blog pages → Article / BlogPosting schema
    "GEO-S2": frozenset({
        "homepage", "service", "faq", "about", "contact", "pricing", "legal",
        "listing", "product", "collection", "generic",
    }),
    # faq pages → FAQPage schema
    "GEO-S3": frozenset({
        "homepage", "service", "article", "blog", "about", "contact", "pricing",
        "legal", "listing", "product", "collection", "generic",
    }),
    # product pages → Product schema
    "GEO-S4": frozenset({
        "homepage", "service", "article", "blog", "faq", "about", "contact",
        "pricing", "legal", "listing", "collection", "generic",
    }),
    # about pages → Organization schema
    "GEO-S5": frozenset({
        "homepage", "service", "article", "blog", "faq", "contact", "pricing",
        "legal", "listing", "product", "collection", "generic",
    }),
}

# ── GEO LocalBusiness rule set ────────────────────────────────────────────────
# Used by the registry gate for the generic-page fallback:
#   if page_type == "generic" AND rule_id in GEO_LOCALBUSINESS_RULES:
#       check page["schema"]["local_business"]["present"]
#       → False: skip  /  True: evaluate
GEO_LOCALBUSINESS_RULES: frozenset[str] = frozenset({
    "GEO-059", "GEO-060", "GEO-061",
    "GEO-062", "GEO-063",
    "GEO-G1",  "GEO-G2",  "GEO-G3",
})

# ── EXPECTED_SCHEMAS (documentation / future frontend use) ───────────────────

EXPECTED_SCHEMAS: dict[str, dict[str, list[str]]] = {
    "homepage": {
        "required": ["Organization", "WebSite"],
        "optional": ["LocalBusiness", "FAQPage"],
        "not_applicable": ["Article", "Product"],
    },
    "contact": {
        "required": ["LocalBusiness"],
        "optional": ["Organization"],
        "not_applicable": ["Article", "FAQPage"],
    },
    "location": {
        "required": ["LocalBusiness"],
        "optional": ["FAQPage", "AggregateRating"],
        "not_applicable": ["Article"],
    },
    "about": {
        "required": ["Organization"],
        "optional": ["Person", "LocalBusiness"],
        "not_applicable": ["Article", "Product"],
    },
    "service": {
        "required": [],
        "optional": ["Service", "FAQPage", "BreadcrumbList"],
        "not_applicable": ["LocalBusiness", "Article"],
    },
    "faq": {
        "required": ["FAQPage"],
        "optional": ["BreadcrumbList"],
        "not_applicable": ["Product", "LocalBusiness"],
    },
    "article": {
        "required": ["Article"],
        "optional": ["Person", "BreadcrumbList"],
        "not_applicable": ["LocalBusiness", "Product"],
    },
    "blog": {
        "required": [],
        "optional": ["BreadcrumbList"],
        "not_applicable": ["LocalBusiness", "Product"],
    },
    "product": {
        "required": ["Product"],
        "optional": ["AggregateRating", "Offer"],
        "not_applicable": ["LocalBusiness", "Article"],
    },
    "pricing": {
        "required": [],
        "optional": [],
        "not_applicable": ["LocalBusiness", "Article", "FAQPage"],
    },
    "legal": {
        "required": [],
        "optional": [],
        "not_applicable": ["LocalBusiness", "Article", "FAQPage"],
    },
    "collection": {
        "required": [],
        "optional": ["BreadcrumbList"],
        "not_applicable": ["LocalBusiness", "Article"],
    },
    "listing": {
        "required": [],
        "optional": ["BreadcrumbList"],
        "not_applicable": ["LocalBusiness", "Article"],
    },
    "generic": {
        "required": [],
        "optional": [],
        "not_applicable": [],  # Resolved at runtime via schema presence
    },
}

# ── Public helper ─────────────────────────────────────────────────────────────

def should_skip(
    rule_id:   str,
    page_type: str,
    page:      dict[str, Any],
    scope:     str = "page",
) -> tuple[bool, str]:
    """
    Determine whether a rule should be skipped for this page.

    Args:
        rule_id:   Rule identifier, e.g. "GEO-059".
        page_type: Lowercase canonical page type from ai_pages["page_type"].
        page:      Full ai_pages document (needed for generic fallback).
        scope:     Rule SCOPE — "page" or "domain".

    Returns:
        (True,  reason_string) — caller should skip this rule
        (False, "")            — caller should evaluate this rule
    """
    # Domain-scope rules are never page-type gated.
    if scope == "domain":
        return False, ""

    pt = (page_type or "generic").strip().lower()

    # Known page type — consult SKIP_MATRIX.
    if pt != "generic":
        skip_set = SKIP_MATRIX.get(rule_id)
        if skip_set and pt in skip_set:
            return True, (
                f"Rule {rule_id} not applicable to '{pt}' pages"
            )
        return False, ""

    # Generic page — schema-presence fallback for GEO LocalBusiness rules.
    if rule_id in GEO_LOCALBUSINESS_RULES:
        lb = (page.get("schema") or {}).get("local_business") or {}
        if not lb.get("present", False):
            return True, (
                "Generic page with no LocalBusiness schema — "
                "GEO local entity rules not applicable"
            )

    return False, ""
