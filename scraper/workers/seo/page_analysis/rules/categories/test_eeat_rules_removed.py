"""Regression: two removed SEO rules stay removed.

Covers the removal of:
  1. "Person schema uses generic entity name" — the non-human-name issue
     branch inside PersonSchemaLinkedRule (rule_id "person_schema_linked").
     The RULE itself stays (3 other issues); only this branch is gone.
  2. "Page missing visible date information" — the entire ContentFreshnessRule
     (rule_id "content_freshness"), class + registration.

Stdlib unittest only, same conventions as the other rules tests. Run from
python_workers/:

    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_eeat_rules_removed -v
"""

import unittest

from scraper.workers.seo.page_analysis.rules.seo_rule_registry import SEORuleRegistry
from scraper.workers.seo.page_analysis.rules.categories import register_all_seo_categories
from scraper.workers.seo.page_analysis.rules.categories import eeat_rules
from scraper.workers.seo.page_analysis.rules.categories.eeat_rules import (
    PersonSchemaLinkedRule,
    ContextValidator,
    register_eeat_rules,
)

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://example.com/blog/post"

# The rule set registered by register_all_seo_categories() after removing
# ContentFreshnessRule (this task) and, subsequently, the two accessibility
# rules alt_text_accessibility + text_contrast. Kept explicit so an accidental
# re-registration (or an unrelated rule being dropped) trips this test.
EXPECTED_TOTAL_RULES = 69
EXPECTED_EEAT_RULES = ["author_name_bio", "author_photo", "person_schema_linked", "privacy_terms_pages"]


def _build_registry():
    registry = SEORuleRegistry()
    register_all_seo_categories(registry)
    return registry


class RemovedRulesNotRegistered(unittest.TestCase):
    def setUp(self):
        self.registry = _build_registry()

    def test_content_freshness_rule_is_not_registered(self):
        self.assertFalse(self.registry.has_rule("content_freshness"))

    def test_content_freshness_rule_class_is_gone(self):
        self.assertFalse(
            hasattr(eeat_rules, "ContentFreshnessRule"),
            "ContentFreshnessRule class must be deleted, not just unregistered",
        )

    def test_person_schema_linked_rule_still_registered(self):
        self.assertTrue(self.registry.has_rule("person_schema_linked"))

    def test_eeat_category_contains_exactly_the_surviving_rules(self):
        got = [r.rule_id for r in self.registry.get_rules_by_category("EEAT")]
        self.assertEqual(got, EXPECTED_EEAT_RULES)

    def test_total_registered_rule_count(self):
        self.assertEqual(self.registry.get_rule_count(), EXPECTED_TOTAL_RULES)

    def test_register_eeat_rules_registers_four_rules(self):
        registry = SEORuleRegistry()
        register_eeat_rules(registry)
        self.assertEqual(
            [r.rule_id for r in registry.get_all_rules()],
            EXPECTED_EEAT_RULES,
        )


class Rule1GenericEntityNameBranchRemoved(unittest.TestCase):
    """PersonSchemaLinkedRule must no longer emit the generic-entity-name
    issue, while still emitting its other three issues."""

    def test_non_human_person_name_produces_no_issue_when_otherwise_complete(self):
        normalized = {
            "structured_data": [{
                "@type": "Person",
                "name": "Sapphire Digital Agency",   # non-human entity name
                "worksFor": {"@id": "#org"},
                "sameAs": ["https://linkedin.com/x"],
                "jobTitle": "SEO Lead",
            }]
        }
        issues = PersonSchemaLinkedRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(issues, [])

    def test_no_issue_message_mentions_generic_entity_name(self):
        normalized = {
            "structured_data": [{"@type": "Person", "name": "Marketing Team"}]
        }
        issues = PersonSchemaLinkedRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        messages = [i["issue_message"] for i in issues]
        self.assertFalse(any("generic entity name" in m for m in messages), messages)
        self.assertFalse(any(i.get("data_path") == "structured_data.person.name" for i in issues))

    def test_person_schema_linked_still_emits_its_other_issues(self):
        normalized = {
            "structured_data": [{"@type": "Person", "name": "John Smith"}]  # incomplete
        }
        issues = PersonSchemaLinkedRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        messages = {i["issue_message"] for i in issues}
        self.assertIn("Person schema missing worksFor property", messages)
        self.assertIn("Person schema missing identity verification (sameAs/url)", messages)
        self.assertIn("Person schema missing role/expertise information", messages)
        for i in issues:
            self.assertEqual(i["rule_id"], "person_schema_linked")
            self.assertEqual(i["rule_no"], 106)


class SharedLogicPreserved(unittest.TestCase):
    def test_is_human_author_helper_still_available(self):
        is_human, _ = ContextValidator.is_human_author("Jane Doe")
        self.assertTrue(is_human)
        is_human, _ = ContextValidator.is_human_author("Growth Agency")
        self.assertFalse(is_human)


class EnginePipelineUnaffected(unittest.TestCase):
    """The full engine builds, other date/schema rules still run, and no
    removed issue is ever produced."""

    def _analyze(self, normalized):
        from scraper.workers.seo.page_analysis.rules.seo_rule_engine import SEORuleEngine
        engine = SEORuleEngine(_build_registry())
        return engine.analyze_page(normalized, JOB_ID, PROJECT_ID, URL)

    def test_other_schema_and_date_rules_still_execute(self):
        registry = _build_registry()
        # A representative sample of rules that must remain.
        for rule_id in ("author_name_bio", "privacy_terms_pages", "person_schema_linked"):
            self.assertTrue(registry.has_rule(rule_id), rule_id)

    def test_analyze_page_never_emits_removed_issues(self):
        # A page with a non-human Person schema AND no visible date — the two
        # exact conditions the removed rules used to fire on.
        normalized = {
            "content": "Welcome to our services. We help businesses grow.",
            "content_text": "Welcome to our services. We help businesses grow.",
            "meta_tags": {},
            "headings": [{"tag": "h1", "text": "Our Services"}],
            "structured_data": [{"@type": "Person", "name": "Sapphire Digital Agency"}],
            "images": [],
            "links": [],
            "internal_links": [],
            "external_links": [],
            "last_updated_signals": {},
        }
        result = self._analyze(normalized)
        all_items = result["issues"] + result["recommendations"]
        rule_ids = {i.get("rule_id") for i in all_items}
        messages = [i.get("issue_message", "") for i in all_items]

        self.assertNotIn("content_freshness", rule_ids)
        self.assertFalse(any("Page missing visible date information" in m for m in messages), messages)
        self.assertFalse(any("generic entity name" in m for m in messages), messages)
        # Sanity: the pipeline still ran and produced a summary.
        self.assertIn("summary", result)
        self.assertEqual(result["summary"]["total_rules"], EXPECTED_TOTAL_RULES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
