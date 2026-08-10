"""
GEO — Schema Coverage card rules (5 rules).

Validates that each page type carries its expected schema.org markup.
These rules are page-type-specific and are skipped via page_type_matrix.py
for every page type where the schema is not applicable.

GEO-S1  Service Schema       (service pages)
GEO-S2  Article Schema       (article + blog pages)
GEO-S3  FAQPage Schema       (faq pages)
GEO-S4  Product Schema       (product pages)
GEO-S5  Organization Schema  (about pages)
"""

from typing import Any
from ...base import BaseRule, RuleResult
from ...schema_hierarchy import satisfies_type, matching_type

_ARTICLE_TYPES = frozenset({"Article", "BlogPosting", "NewsArticle"})


class RuleGEOS1ServiceSchema(BaseRule):
    RULE_ID  = "GEO-S1"
    HUB, CARD, SEVERITY = "geo", "schema_coverage", "medium"
    ISSUE_TITLE       = "Service Schema Missing"
    ISSUE_DESCRIPTION = (
        "This service page has no Service schema markup. "
        "AI systems cannot extract structured service details such as name, "
        "description, or provider — reducing citation probability for service queries."
    )
    RECOMMENDATION = (
        "Add a Service JSON-LD block to this page with at minimum: "
        "name, description, provider (linked to your Organization), and serviceType."
    )
    EXPECTED_IMPACT = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        types_present = (page.get("schema") or {}).get("types_present") or []
        if "Service" in types_present:
            return self._pass({"schema_type": "Service", "schema_present": True})
        return self._fail({
            "schema_type": "Service",
            "schema_present": False,
            "types_found": types_present,
        })


class RuleGEOS2ArticleSchema(BaseRule):
    RULE_ID  = "GEO-S2"
    HUB, CARD, SEVERITY = "geo", "schema_coverage", "medium"
    ISSUE_TITLE       = "Article Schema Missing"
    ISSUE_DESCRIPTION = (
        "This content page has no Article or BlogPosting schema. "
        "Generative AI models use Article schema to identify authorship, "
        "publish date, and topical authority — all key citation factors."
    )
    RECOMMENDATION = (
        "Add an Article (or BlogPosting) JSON-LD block with: "
        "headline, author (Person), datePublished, dateModified, and publisher (Organization)."
    )
    EXPECTED_IMPACT = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        types_present = (page.get("schema") or {}).get("types_present") or []
        found = [t for t in types_present if t in _ARTICLE_TYPES]
        if found:
            return self._pass({
                "schema_type": found[0],
                "schema_present": True,
            })
        return self._fail({
            "expected": ["Article", "BlogPosting"],
            "schema_present": False,
            "types_found": types_present,
        })


class RuleGEOS3FAQPageSchema(BaseRule):
    RULE_ID  = "GEO-S3"
    HUB, CARD, SEVERITY = "geo", "schema_coverage", "medium"
    ISSUE_TITLE       = "FAQPage Schema Missing"
    ISSUE_DESCRIPTION = (
        "This FAQ page has no FAQPage schema. "
        "AI answer engines extract Q&A pairs directly from FAQPage markup — "
        "without it, questions and answers on this page cannot be cited in responses."
    )
    RECOMMENDATION = (
        "Add a FAQPage JSON-LD block with mainEntity entries mapping every visible "
        "question to its answer using Question and acceptedAnswer."
    )
    EXPECTED_IMPACT = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        schema     = page.get("schema") or {}
        types_present = schema.get("types_present") or []
        faq_pairs     = schema.get("faq_schema_pairs") or []
        if "FAQPage" in types_present or len(faq_pairs) > 0:
            return self._pass({
                "faq_schema_present": True,
                "qa_count": len(faq_pairs),
            })
        return self._fail({
            "faq_schema_present": False,
            "types_found": types_present,
        })


class RuleGEOS4ProductSchema(BaseRule):
    RULE_ID  = "GEO-S4"
    HUB, CARD, SEVERITY = "geo", "schema_coverage", "medium"
    ISSUE_TITLE       = "Product Schema Missing"
    ISSUE_DESCRIPTION = (
        "This product page has no Product schema. "
        "AI shopping assistants and knowledge graph engines cannot extract product "
        "details — making this product invisible in AI-assisted purchase recommendations."
    )
    RECOMMENDATION = (
        "Add a Product JSON-LD block with: name, description, image, offers "
        "(including price and availability), and brand."
    )
    EXPECTED_IMPACT = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        types_present = (page.get("schema") or {}).get("types_present") or []
        if "Product" in types_present:
            return self._pass({"schema_type": "Product", "schema_present": True})
        return self._fail({
            "schema_type": "Product",
            "schema_present": False,
            "types_found": types_present,
        })


class RuleGEOS5OrganizationSchema(BaseRule):
    RULE_ID  = "GEO-S5"
    HUB, CARD, SEVERITY = "geo", "schema_coverage", "medium"
    ISSUE_TITLE       = "Organization Schema Missing on About Page"
    ISSUE_DESCRIPTION = (
        "This about page has no Organization schema. "
        "About pages are the primary location where AI systems expect to find "
        "entity identity signals — without Organization markup, AI models cannot "
        "build a knowledge graph entity for this business."
    )
    RECOMMENDATION = (
        "Add an Organization JSON-LD block with: name, url, logo, "
        "description, foundingDate, sameAs (social profiles), and contactPoint."
    )
    EXPECTED_IMPACT = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        schema        = page.get("schema") or {}
        types_present = schema.get("types_present") or []

        if satisfies_type(types_present, "Organization"):
            matched = matching_type(types_present, "Organization")
            org = schema.get("organization") or {}
            lb  = schema.get("local_business") or {}
            return self._pass({
                "organization_present": True,
                "matched_type":         matched,
                "via_subtype":          matched != "Organization",
                "name":                 org.get("name") or lb.get("name"),
            })
        return self._fail({
            "organization_present": False,
            "types_found":          types_present,
        })


RULES = [
    RuleGEOS1ServiceSchema(),
    RuleGEOS2ArticleSchema(),
    RuleGEOS3FAQPageSchema(),
    RuleGEOS4ProductSchema(),
    RuleGEOS5OrganizationSchema(),
]
