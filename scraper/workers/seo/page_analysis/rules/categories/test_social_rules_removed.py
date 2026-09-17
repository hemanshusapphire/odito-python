"""Regression: the three Social Tags rules stay disabled.

Covers disabling (NOT deleting — unlike titlePixelLengthRule/
KeywordNotInTitleRule elsewhere in this directory, the classes are kept
intact, just unregistered):
  1. OgTagsMissingRule      (rule_id "og_tags_missing",           rule_no 71)
  2. OgTagsIncompleteRule   (rule_id "og_tags_incomplete",         rule_no 72)
  3. TwitterCardTagsMissingRule (rule_id "twitter_card_tags_missing", rule_no 73)

All three are correctly-functioning rules with severity="low" that were
disabled for product reasons — see register_social_rules() in
social_rules.py — not because they were buggy. They only started actually
appearing in the On-Page Issues UI once the page_analysis.py persistence
bug (recommendations silently dropped, see
scraper/workers/seo/page_analysis/test_page_analysis_upsert.py's
TestIssuesToPersistIncludesRecommendations) was fixed; disabling them here
is a product decision made after seeing them surface for the first time,
not a rule-correctness fix.

Stdlib unittest only, same conventions as the other rules tests. Run from
python_workers/:

    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_social_rules_removed -v
"""

import unittest

from scraper.workers.seo.page_analysis.rules.seo_rule_registry import SEORuleRegistry
from scraper.workers.seo.page_analysis.rules.categories import register_all_seo_categories
from scraper.workers.seo.page_analysis.rules.categories.social_rules import (
    OgTagsMissingRule,
    OgTagsIncompleteRule,
    TwitterCardTagsMissingRule,
    register_social_rules,
)

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://example.com/page"

DISABLED_RULE_IDS = ["og_tags_missing", "og_tags_incomplete", "twitter_card_tags_missing"]

# Registry-wide total after this disable. See the sibling "*_removed.py"
# files in this directory for the running history; all of them must be
# kept in sync whenever any rule anywhere is added, removed, or disabled.
EXPECTED_TOTAL_RULES = 64


def _build_registry():
    registry = SEORuleRegistry()
    register_all_seo_categories(registry)
    return registry


class SocialTagsRulesDisabled(unittest.TestCase):
    def setUp(self):
        self.registry = _build_registry()

    def test_none_of_the_three_rules_are_registered(self):
        for rule_id in DISABLED_RULE_IDS:
            self.assertFalse(self.registry.has_rule(rule_id), rule_id)

    def test_social_tags_category_is_empty(self):
        self.assertEqual(self.registry.get_rules_by_category("Social Tags"), [])

    def test_rule_classes_still_exist(self):
        # Disabled, not deleted — a future re-enable is a one-line uncomment
        # in register_social_rules(), not a rewrite.
        self.assertTrue(callable(OgTagsMissingRule))
        self.assertTrue(callable(OgTagsIncompleteRule))
        self.assertTrue(callable(TwitterCardTagsMissingRule))

    def test_rules_still_work_correctly_when_called_directly(self):
        # Proves this is a registration-only disable: the rule logic itself
        # is untouched and still correctly detects a real gap.
        normalized = {"meta_tags": {}}
        issues = OgTagsMissingRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "low")

    def test_register_social_rules_registers_nothing(self):
        registry = SEORuleRegistry()
        register_social_rules(registry)
        self.assertEqual(registry.get_all_rules(), [])

    def test_total_registered_rule_count(self):
        self.assertEqual(self.registry.get_rule_count(), EXPECTED_TOTAL_RULES)


class EnginePipelineUnaffected(unittest.TestCase):
    """The full engine builds and never emits any of the disabled rules'
    issues, even against a page shaped to trigger every one of them."""

    def test_analyze_page_never_emits_disabled_social_issues(self):
        from scraper.workers.seo.page_analysis.rules.seo_rule_engine import SEORuleEngine

        normalized = {
            "content": "Welcome to our services. We help businesses grow.",
            "content_text": "Welcome to our services. We help businesses grow.",
            "title": "Home",
            "meta_tags": {},  # no og:*/twitter:* tags at all -> would fire all three
            "headings": [{"tag": "h1", "text": "We help businesses grow their revenue"}],
            "structured_data": [],
            "images": [],
            "links": [],
            "internal_links": [],
            "external_links": [],
        }
        engine = SEORuleEngine(_build_registry())
        result = engine.analyze_page(normalized, JOB_ID, PROJECT_ID, URL)
        all_items = result["issues"] + result["recommendations"]
        rule_ids = {i.get("rule_id") for i in all_items}

        for rule_id in DISABLED_RULE_IDS:
            self.assertNotIn(rule_id, rule_ids)
        self.assertEqual(result["summary"]["total_rules"], EXPECTED_TOTAL_RULES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
