"""
Schema Applicability Matrix
============================

Canonical source of truth for which Schema.org @types are required,
recommended, or not applicable per page type.

Used by:
  - on-page SEO schema validation rules  (schema_rules.py)
  - AI V2 schema visibility rules        (page_type_matrix.py)
  - InvalidSchemaTypeRule                (detects wrong schema for page type)

Page types mirror the canonical set from page_type_detector.py:
  homepage, article, service, product, collection, faq, about,
  contact, pricing, legal, listing, blog, location, generic
"""

from typing import Dict, FrozenSet

# Per-page-type schema applicability.
# "not_applicable" schemas on a page with the given type are structural errors
# that MUST be reported as InvalidSchemaTypeRule issues.
SCHEMA_APPLICABILITY: Dict[str, Dict[str, FrozenSet[str]]] = {
    "homepage": {
        "required":        frozenset(["Organization"]),
        "recommended":     frozenset(["WebSite", "WebPage", "LocalBusiness", "FAQPage"]),
        "not_applicable":  frozenset(["Product", "ProductGroup"]),
    },
    "about": {
        "required":        frozenset(["Organization"]),
        "recommended":     frozenset(["WebPage", "Person"]),
        "not_applicable":  frozenset(["Product", "ProductGroup", "LocalBusiness"]),
    },
    "contact": {
        "required":        frozenset(["LocalBusiness"]),
        "recommended":     frozenset(["ContactPage", "Organization"]),
        "not_applicable":  frozenset(["Article", "BlogPosting", "NewsArticle", "Product", "FAQPage"]),
    },
    "service": {
        "required":        frozenset([]),
        "recommended":     frozenset(["Service", "WebPage", "FAQPage", "BreadcrumbList"]),
        "not_applicable":  frozenset(["Article", "BlogPosting", "NewsArticle", "LocalBusiness", "Product", "ContactPage"]),
    },
    "article": {
        "required":        frozenset(["Article"]),
        "recommended":     frozenset(["Person", "BreadcrumbList"]),
        "not_applicable":  frozenset(["Product", "ProductGroup", "LocalBusiness", "Service", "ContactPage", "FAQPage"]),
    },
    "blog": {
        "required":        frozenset([]),
        "recommended":     frozenset(["BreadcrumbList", "Article", "BlogPosting"]),
        "not_applicable":  frozenset(["Product", "ProductGroup", "LocalBusiness", "Service", "ContactPage"]),
    },
    "faq": {
        "required":        frozenset(["FAQPage"]),
        "recommended":     frozenset(["BreadcrumbList", "WebPage"]),
        "not_applicable":  frozenset(["Product", "ProductGroup", "LocalBusiness", "Article", "BlogPosting"]),
    },
    "product": {
        "required":        frozenset(["Product"]),
        "recommended":     frozenset(["AggregateRating", "BreadcrumbList"]),
        "not_applicable":  frozenset(["LocalBusiness", "Article", "BlogPosting", "Service", "ContactPage", "Organization"]),
    },
    "pricing": {
        "required":        frozenset([]),
        "recommended":     frozenset([]),
        "not_applicable":  frozenset(["LocalBusiness", "Article", "BlogPosting", "FAQPage", "Product"]),
    },
    "legal": {
        "required":        frozenset([]),
        "recommended":     frozenset([]),
        "not_applicable":  frozenset(["LocalBusiness", "Article", "BlogPosting", "FAQPage", "Product", "AggregateRating"]),
    },
    "collection": {
        "required":        frozenset([]),
        "recommended":     frozenset(["BreadcrumbList", "CollectionPage", "ItemList"]),
        "not_applicable":  frozenset(["LocalBusiness", "Article", "BlogPosting", "Service"]),
    },
    "listing": {
        "required":        frozenset([]),
        "recommended":     frozenset(["BreadcrumbList", "ItemList"]),
        "not_applicable":  frozenset(["LocalBusiness", "Article", "BlogPosting", "Service"]),
    },
    "location": {
        "required":        frozenset(["LocalBusiness"]),
        "recommended":     frozenset(["Service", "FAQPage"]),
        "not_applicable":  frozenset(["Article", "BlogPosting", "Product", "ContactPage"]),
    },
    "generic": {
        "required":        frozenset([]),
        "recommended":     frozenset([]),
        "not_applicable":  frozenset([]),  # Unknown page type — validate conservatively
    },
}

# Schema families: mutually exclusive groups where having 2+ on the same page is a conflict.
# Entity schemas (Organization + LocalBusiness) are intentionally in the same family but
# are allowed to coexist (LocalBusiness extends Organization).
# "entity" is excluded from conflict detection so they can coexist.
SCHEMA_FAMILIES: Dict[str, FrozenSet[str]] = {
    "content":   frozenset(["Article", "BlogPosting", "NewsArticle", "TechArticle"]),
    "product":   frozenset(["Product", "ProductGroup"]),
    "service":   frozenset(["Service"]),
    "faq":       frozenset(["FAQPage", "QAPage"]),
    "entity":    frozenset(["Organization", "LocalBusiness", "Person"]),  # allowed together
}

# Schema families where having multiple types IS a conflict (entity excluded).
CONFLICTING_FAMILIES: FrozenSet[str] = frozenset(["content", "product", "service"])

# Schemas that are always allowed on any page type (structural / utility schemas).
UNIVERSAL_SCHEMAS: FrozenSet[str] = frozenset([
    "WebPage", "WebSite", "BreadcrumbList", "SiteLinksSearchBox",
    "AggregateRating", "Review", "ItemList",
])
