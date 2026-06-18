"""
AISO — Authority card rules (3 rules).

AISO-A1  Organization Schema Present
AISO-A2  Author / Publisher Identified
AISO-A3  About / Company Information Present
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAISOA1OrganizationSchema(BaseRule):
    RULE_ID  = "AISO-A1"
    HUB, CARD, SEVERITY = "aiso", "authority", "high"
    ISSUE_TITLE       = "Organization Schema Missing"
    ISSUE_DESCRIPTION = "No Organization or LocalBusiness schema detected. AI systems cannot identify the entity behind this website, reducing authority confidence."
    RECOMMENDATION    = "Add Organization schema (or LocalBusiness for local businesses) with name, url, and logo properties to your homepage and key pages."
    EXPECTED_IMPACT   = "high"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        entities = page.get("entities", {})
        present  = bool(entities.get("organization_present", False))
        if present:
            return self._pass({"organization_present": True, "name": entities.get("organization_name")})
        return self._fail({"organization_present": False})


class RuleAISOA2AuthorPublisher(BaseRule):
    RULE_ID  = "AISO-A2"
    HUB, CARD, SEVERITY = "aiso", "authority", "medium"
    ISSUE_TITLE       = "Author or Publisher Not Identified"
    ISSUE_DESCRIPTION = "No author or publisher schema detected on this page. AI systems weigh identified authorship as an E-E-A-T expertise signal."
    RECOMMENDATION    = "Add Person schema for the author, or an 'author' property within Article/BlogPosting schema on content pages."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        entities = page.get("entities", {})
        present  = bool(entities.get("author_present", False))
        if present:
            return self._pass({"author_present": True, "name": entities.get("author_name")})
        return self._fail({"author_present": False})


class RuleAISOA3AboutCompany(BaseRule):
    RULE_ID  = "AISO-A3"
    HUB, CARD, SEVERITY = "aiso", "authority", "medium"
    ISSUE_TITLE       = "About / Company Information Not Detectable"
    ISSUE_DESCRIPTION = "No About page link or company information was found. AI systems use company context to validate entity authority and filter low-trust sources."
    RECOMMENDATION    = "Add an About page and link to it from the main navigation. Include company name, founding context, and mission on that page."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        present = bool(page.get("entities", {}).get("about_company_present", False))
        if present:
            return self._pass({"about_company_present": True})
        return self._fail({"about_company_present": False})


RULES = [
    RuleAISOA1OrganizationSchema(),
    RuleAISOA2AuthorPublisher(),
    RuleAISOA3AboutCompany(),
]
