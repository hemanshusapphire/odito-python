"""OrganizationSchemaRule severity-by-finding-type regression tests.

Background: this rule can emit three genuinely different finding types under
the same rule_id/issue_code ("organization_schema"):
  1. Entity schema (Organization/LocalBusiness) entirely missing.
  2. Entity schema present but missing a REQUIRED field (name, url).
  3. Entity schema present but missing a RECOMMENDED field (logo, sameAs,
     address) — a soft nudge, not a failure.

Before this fix, all three inherited the rule's class-level severity="high",
so a page with a complete, valid Organization schema that simply had no
street address (e.g. a SaaS company) was surfaced with the same severity as
a page with no entity schema at all. create_issue() now accepts an explicit
per-call `severity` override (base_seo_rule.py) and OrganizationSchemaRule
uses it so finding (3) is "low" while (1) and (2) stay "high".

Stdlib unittest only, same conventions as the other rules tests. Run from
python_workers/:

    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_organization_schema_rule -v
"""

import unittest

from scraper.workers.seo.page_analysis.rules.categories.schema_rules import (
    OrganizationSchemaRule,
)
from scraper.shared.schema import flatten_schema

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://example.com/"


def _org(**overrides):
    base = {
        "@type": "Organization",
        "@id": "https://example.com/#organization",
        "name": "Example Inc",
        "url": "https://example.com",
        "logo": {"@type": "ImageObject", "url": "https://example.com/logo.png"},
        "sameAs": ["https://facebook.com/example", "https://linkedin.com/company/example"],
        "address": {"@type": "PostalAddress", "streetAddress": "1 Example St"},
    }
    base.update(overrides)
    return base


class CaseA_SchemaEntirelyMissing(unittest.TestCase):
    """No Organization or LocalBusiness schema anywhere on the page."""

    def test_high_severity_and_missing_schema_message(self):
        normalized = {
            "url": URL,
            "page_type": "homepage",
            "structured_data": [{"@type": "WebSite", "@id": "https://example.com/#website"}],
        }
        issues = OrganizationSchemaRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "high")
        self.assertIn("Missing Organization or LocalBusiness schema", issues[0]["issue_message"])
        self.assertEqual(issues[0]["data_path"], "structured_data.organization")


class CaseB_RequiredFieldMissing(unittest.TestCase):
    """Organization exists but the required `name` field is missing."""

    def test_high_severity_and_incomplete_required_message(self):
        normalized = {
            "url": URL,
            "page_type": "homepage",
            "structured_data": [_org(name=None)],
        }
        issues = OrganizationSchemaRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        required_issue = next(i for i in issues if i["data_path"] == "structured_data.organization.missing_fields")
        self.assertEqual(required_issue["severity"], "high")
        self.assertIn("missing required fields", required_issue["issue_message"])
        self.assertIn("name", required_issue["issue_message"])
        # Must not be misreported as a full absence.
        self.assertNotIn("Missing Organization or LocalBusiness", required_issue["issue_message"])


class CaseC_OnlyRecommendedFieldMissing(unittest.TestCase):
    """Organization has name+url (+logo+sameAs) but no address — the exact
    real-world oditoai.com scenario that was misreported."""

    def test_low_severity_incomplete_message_and_never_says_missing(self):
        normalized = {
            "url": URL,
            "page_type": "homepage",
            "structured_data": [_org(address=None)],
        }
        issues = OrganizationSchemaRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(len(issues), 1)
        issue = issues[0]
        self.assertEqual(issue["severity"], "low")
        self.assertEqual(issue["data_path"], "structured_data.organization.incomplete")
        self.assertIn("missing recommended fields: address", issue["issue_message"])
        self.assertNotIn("Missing Organization", issue["issue_message"])

    def test_matches_live_oditoai_homepage_schema(self):
        """Same shape as the real oditoai.com/ Organization schema verified
        live against seo_page_data during the investigation: name, url,
        logo, sameAs (5 links) all present; address absent."""
        normalized = {
            "url": "https://oditoai.com/",
            "page_type": "homepage",
            "structured_data": [{
                "@type": "Organization",
                "@id": "https://oditoai.com/#organization",
                "name": "Odito.ai",
                "url": "https://oditoai.com",
                "logo": {"@type": "ImageObject", "url": "https://oditoai.com/oditologo.png"},
                "description": "AI-powered enterprise SEO + AI visibility auditing platform",
                "sameAs": [
                    "https://www.linkedin.com/company/oditoai",
                    "https://www.facebook.com/oditoai",
                    "https://www.instagram.com/oditoai",
                    "https://twitter.com/oditoai",
                    "https://www.youtube.com/@oditoai",
                ],
            }],
        }
        issues = OrganizationSchemaRule().evaluate(
            normalized, JOB_ID, PROJECT_ID, "https://oditoai.com/"
        )
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "low")
        self.assertNotIn("Missing Organization", issues[0]["issue_message"])


class CaseD_FullyCompleteOrganization(unittest.TestCase):
    """name, url, logo, sameAs, address all present → no issue at all."""

    def test_no_issues(self):
        normalized = {
            "url": URL,
            "page_type": "homepage",
            "structured_data": [_org()],
        }
        issues = OrganizationSchemaRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(issues, [])


class CaseE_ValidOrganizationInsideGraph(unittest.TestCase):
    """A complete Organization schema nested inside a top-level @graph block
    must flatten correctly and validate as fully present — no false positive."""

    def test_graph_wrapped_complete_organization_produces_no_issue(self):
        raw_jsonld = {
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "WebSite", "@id": "https://example.com/#website"},
                _org(),
            ],
        }
        flattened = flatten_schema(raw_jsonld)
        # Sanity check the flattening itself worked before trusting the rule result.
        self.assertEqual(len(flattened), 2)
        self.assertTrue(any(s.get("@type") == "Organization" for s in flattened))

        normalized = {"url": URL, "page_type": "homepage", "structured_data": flattened}
        issues = OrganizationSchemaRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(issues, [])


class CaseF_LocalBusinessWithAddress(unittest.TestCase):
    """Contact/location pages require LocalBusiness specifically; a complete
    LocalBusiness (including address) must validate cleanly."""

    def test_complete_localbusiness_on_contact_page_produces_no_issue(self):
        normalized = {
            "url": "https://example.com/contact",
            "page_type": "contact",
            "structured_data": [_org(**{"@type": "LocalBusiness"})],
        }
        issues = OrganizationSchemaRule().evaluate(
            normalized, JOB_ID, PROJECT_ID, "https://example.com/contact"
        )
        self.assertEqual(issues, [])

    def test_missing_localbusiness_on_contact_page_is_high_severity(self):
        normalized = {
            "url": "https://example.com/contact",
            "page_type": "contact",
            "structured_data": [{"@type": "WebSite"}],
        }
        issues = OrganizationSchemaRule().evaluate(
            normalized, JOB_ID, PROJECT_ID, "https://example.com/contact"
        )
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "high")
        self.assertEqual(issues[0]["data_path"], "structured_data.localbusiness")


class CreateIssueSeverityOverride(unittest.TestCase):
    """base_seo_rule.create_issue() severity override — backward compatibility."""

    def test_default_falls_back_to_class_level_severity(self):
        rule = OrganizationSchemaRule()  # class-level severity="high"
        issue = rule.create_issue(JOB_ID, PROJECT_ID, URL, "msg", "detected", "expected")
        self.assertEqual(issue["severity"], "high")

    def test_explicit_override_wins(self):
        rule = OrganizationSchemaRule()
        issue = rule.create_issue(
            JOB_ID, PROJECT_ID, URL, "msg", "detected", "expected", severity="low"
        )
        self.assertEqual(issue["severity"], "low")


if __name__ == "__main__":
    unittest.main(verbosity=2)
