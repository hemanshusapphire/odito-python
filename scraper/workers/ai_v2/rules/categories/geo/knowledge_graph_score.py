"""
GEO — Knowledge Graph Score card rules (2 rules).

GEO-062  Google Maps Embed
GEO-063  City Mention In H1 & First 100 Words
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleGEO062MapsEmbed(BaseRule):
    RULE_ID  = "GEO-062"
    HUB, CARD, SEVERITY = "geo", "knowledge_graph_score", "medium"
    ISSUE_TITLE       = "Google Maps Embed Missing"
    ISSUE_DESCRIPTION = "No Google Maps embed found. The direct visual and technical link between this page and the Maps knowledge graph entity is absent."
    RECOMMENDATION    = "Embed a Google Maps iframe on your contact or location page using the embed URL from Google Maps > Share > Embed a map."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        present = bool(page.get("geo", {}).get("maps_embed_present", False))
        if present:
            return self._pass({"maps_embed_present": True})
        return self._fail({"maps_embed_present": False})


class RuleGEO063CityMention(BaseRule):
    RULE_ID  = "GEO-063"
    HUB, CARD, SEVERITY = "geo", "knowledge_graph_score", "high"
    ISSUE_TITLE       = "City Name Missing from H1 or Opening Content"
    ISSUE_DESCRIPTION = "The target city does not appear in the H1 and/or the opening 100 words. Geographic relevance signals are weakened for local AI queries."
    RECOMMENDATION    = "Include the city name naturally in the H1 heading and within the first 100 words of body text. One natural mention in each position is sufficient."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        geo       = page.get("geo", {})
        city      = geo.get("target_city")
        in_h1     = bool(geo.get("city_in_h1", False))
        in_100    = bool(geo.get("city_in_first_100", False))

        if not city:
            return self._fail({
                "city": None,
                "reason": "No target city declared in LocalBusiness schema",
            })

        both_met = in_h1 and in_100
        if both_met:
            return self._pass({"city": city, "in_h1": True, "in_first_100_words": True})
        return self._fail({
            "city": city,
            "in_h1": in_h1,
            "in_first_100_words": in_100,
            "both_conditions_met": False,
        })


RULES = [
    RuleGEO062MapsEmbed(),
    RuleGEO063CityMention(),
]
