"""
GEO — Brand Corroboration card rules (4 rules).

GEO-065  Hreflang (multi-locale signal)
GEO-G1   NAP Consistency
GEO-G2   Entity Consistency (SameAs links)
GEO-G3   Brand Corroboration (AggregateRating schema)
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleGEO065Hreflang(BaseRule):
    RULE_ID  = "GEO-065"
    HUB, CARD, SEVERITY = "geo", "brand_corroboration", "low"
    ISSUE_TITLE       = "Hreflang Missing on Multi-Locale Site"
    ISSUE_DESCRIPTION = "Your site serves content in multiple languages or regions but lacks hreflang tags. AI systems and search engines cannot correctly attribute regional entity versions to geographic queries."
    RECOMMENDATION    = "Implement hreflang link elements for all language/region combinations. Include a self-referencing tag on every page and an x-default fallback. Use BCP-47 language codes (e.g. en-US, fr-FR)."
    EXPECTED_IMPACT   = "low"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        hreflang = page.get("geo", {}).get("hreflang", {})
        present      = bool(hreflang.get("present", False))
        count        = int(hreflang.get("count", 0))
        self_ref     = bool(hreflang.get("has_self_ref", False))
        x_default    = bool(hreflang.get("has_x_default", False))

        if not present:
            return self._fail({
                "hreflang_present": False,
                "reason": "No hreflang tags found",
            })

        valid = present and self_ref
        if valid:
            return self._pass({
                "hreflang_present": True,
                "count": count,
                "self_reference": self_ref,
                "x_default": x_default,
                "tags": hreflang.get("tags", [])[:5],
            })
        return self._fail({
            "hreflang_present": True,
            "count": count,
            "self_reference": self_ref,
            "x_default": x_default,
        })


class RuleGEOG1NAPConsistency(BaseRule):
    RULE_ID  = "GEO-G1"
    HUB, CARD, SEVERITY = "geo", "brand_corroboration", "high"
    ISSUE_TITLE       = "NAP Inconsistency — Schema and Visible Text Do Not Match"
    ISSUE_DESCRIPTION = "The business Name, Address, or Phone in structured schema does not match what appears as visible text on the page. Inconsistent NAP signals confuse knowledge graph entity resolution."
    RECOMMENDATION    = "Ensure the business name, address, and phone number in your LocalBusiness schema exactly match the visible NAP text on your contact or homepage."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        geo          = page.get("geo", {})
        consistent   = bool(geo.get("nap_consistent", False))
        visible_nap  = geo.get("visible_nap", {})
        schema_nap   = geo.get("schema_nap", {})

        # No LocalBusiness schema → NAP fields absent → FAIL.
        if not schema_nap.get("name") and not schema_nap.get("phone"):
            return self._fail({
                "nap_consistent": False,
                "reason": "No LocalBusiness schema — NAP fields absent",
            })

        if consistent:
            return self._pass({
                "nap_consistent": True,
                "name_match": visible_nap.get("name_in_text"),
                "city_match": visible_nap.get("city_in_text"),
                "phone_match": visible_nap.get("phone_in_text"),
            })
        return self._fail({
            "nap_consistent": False,
            "name_match":  visible_nap.get("name_in_text"),
            "city_match":  visible_nap.get("city_in_text"),
            "phone_match": visible_nap.get("phone_in_text"),
            "schema_name": schema_nap.get("name"),
            "schema_phone": schema_nap.get("phone"),
        })


class RuleGEOG2EntityConsistency(BaseRule):
    RULE_ID  = "GEO-G2"
    HUB, CARD, SEVERITY = "geo", "brand_corroboration", "medium"
    ISSUE_TITLE       = "No SameAs Links Declared"
    ISSUE_DESCRIPTION = "LocalBusiness schema has no sameAs property linking to authoritative external profiles. Without these, knowledge graph systems cannot corroborate entity identity across platforms."
    RECOMMENDATION    = "Add a sameAs array to your LocalBusiness schema pointing to your Google Business Profile, Wikidata entry, and major directory profiles."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        same_as = page.get("geo", {}).get("same_as_links", []) or []
        lb      = page.get("schema", {}).get("local_business") or {}

        if not lb.get("present", False):
            return self._fail({
                "same_as_count": 0,
                "reason": "No LocalBusiness schema — SameAs links absent",
            })

        if len(same_as) >= 1:
            return self._pass({"same_as_count": len(same_as), "examples": same_as[:3]})
        return self._fail({"same_as_count": 0, "same_as_links": []})


class RuleGEOG3BrandCorroboration(BaseRule):
    RULE_ID  = "GEO-G3"
    HUB, CARD, SEVERITY = "geo", "brand_corroboration", "medium"
    ISSUE_TITLE       = "AggregateRating Schema Missing"
    ISSUE_DESCRIPTION = "No AggregateRating schema found. Review signals in structured data strengthen entity trust and corroborate brand reputation to knowledge graph systems."
    RECOMMENDATION    = "Add AggregateRating schema to your LocalBusiness with ratingValue, reviewCount, and bestRating properties if your business has verifiable reviews."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        schema_types = page.get("schema", {}).get("types_present", []) or []
        has_rating   = "AggregateRating" in schema_types

        lb = page.get("schema", {}).get("local_business") or {}
        if not lb.get("present", False):
            return self._fail({
                "aggregate_rating_present": False,
                "reason": "No LocalBusiness schema — AggregateRating cannot be present",
            })

        if has_rating:
            return self._pass({"aggregate_rating_present": True})
        return self._fail({"aggregate_rating_present": False})


RULES = [
    RuleGEO065Hreflang(),
    RuleGEOG1NAPConsistency(),
    RuleGEOG2EntityConsistency(),
    RuleGEOG3BrandCorroboration(),
]
