"""
GEO — Entity Authority card rules (3 rules).

GEO-059  LocalBusiness Schema
GEO-060  GeoCoordinates
GEO-061  OpeningHoursSpecification
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleGEO059LocalBusinessSchema(BaseRule):
    RULE_ID  = "GEO-059"
    HUB, CARD, SEVERITY = "geo", "entity_authority", "critical"
    ISSUE_TITLE       = "LocalBusiness Schema Missing"
    ISSUE_DESCRIPTION = "No LocalBusiness schema found. AI systems and knowledge graph engines cannot identify this business as a local entity, making geographic entity association impossible."
    RECOMMENDATION    = "Add LocalBusiness schema (or a specific subtype matching your business category) with complete name, PostalAddress, telephone, and URL in JSON-LD in the <head>."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        lb = page.get("schema", {}).get("local_business") or {}
        present = bool(lb.get("present", False))
        addr    = lb.get("address", {}) or {}

        required_ok = bool(
            lb.get("name")
            and lb.get("telephone")
            and addr.get("complete", False)
        )

        if present and required_ok:
            return self._pass({
                "schema_present": True,
                "type": lb.get("type"),
                "name": lb.get("name"),
                "address_complete": addr.get("complete"),
                "telephone_present": bool(lb.get("telephone")),
            })
        if present and not required_ok:
            return self._fail({
                "schema_present": True,
                "missing_fields": [
                    f for f, v in {
                        "name": lb.get("name"),
                        "telephone": lb.get("telephone"),
                        "address": addr.get("complete"),
                    }.items() if not v
                ],
            })
        return self._fail({"schema_present": False})


class RuleGEO060GeoCoordinates(BaseRule):
    RULE_ID  = "GEO-060"
    HUB, CARD, SEVERITY = "geo", "entity_authority", "high"
    ISSUE_TITLE       = "GeoCoordinates Missing from LocalBusiness Schema"
    ISSUE_DESCRIPTION = "LocalBusiness schema lacks precise coordinates. Knowledge graph systems must rely on address text parsing alone, introducing entity matching ambiguity."
    RECOMMENDATION    = "Add a GeoCoordinates object inside your LocalBusiness schema with accurate latitude and longitude decimal values."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        geo = page.get("geo", {}).get("geocoords", {})
        present = bool(geo.get("present", False))
        lat     = geo.get("latitude")
        lng     = geo.get("longitude")
        if present and lat is not None and lng is not None:
            return self._pass({"geocoords_present": True, "latitude": lat, "longitude": lng})
        return self._fail({"geocoords_present": False})


class RuleGEO061OpeningHours(BaseRule):
    RULE_ID  = "GEO-061"
    HUB, CARD, SEVERITY = "geo", "entity_authority", "high"
    ISSUE_TITLE       = "Business Hours Not in Structured Schema"
    ISSUE_DESCRIPTION = "AI assistants cannot answer 'is this business open?' queries because OpeningHoursSpecification is missing from the LocalBusiness schema."
    RECOMMENDATION    = "Add OpeningHoursSpecification entries to your LocalBusiness schema for every operating day, using ISO 8601 time format (HH:MM)."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        hours = page.get("geo", {}).get("opening_hours", {})
        present = bool(hours.get("present", False))
        count   = int(hours.get("count", 0))
        if present and count >= 1:
            return self._pass({"hours_present": True, "entry_count": count})
        return self._fail({"hours_present": False, "entry_count": 0})


RULES = [
    RuleGEO059LocalBusinessSchema(),
    RuleGEO060GeoCoordinates(),
    RuleGEO061OpeningHours(),
]
