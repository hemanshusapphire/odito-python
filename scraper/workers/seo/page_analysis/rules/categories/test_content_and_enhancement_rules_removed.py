"""Regression: removed "H1 topic not in title" rule and the whole
title-pixel-length rule (desktop and mobile) stay removed.

Covers the removal of:
  1. "Primary topic from H1 not found in title" — the whole
     ``KeywordNotInTitleRule`` (rule_id "keyword_not_in_title", rule_no 39,
     category "Content"). The sibling ``KeywordNotInH1Rule``
     (rule_id "keyword_not_in_h1") is untouched and must still fire.
  2. The entire title pixel-length rule — ``titlePixelLengthRule``
     (rule_id "title_pixel_length", rule_no 30, category
     "SEO Enhancements"). This previously emitted four issues (desktop
     FAIL >600px, desktop WARNING >500px, mobile FAIL >500px, mobile
     WARNING >400px); the two WARNING tiers were pruned first, and this
     removes the rule (and its remaining FAIL tiers) entirely. The
     sibling ``anchorTextOptimizationRule``/``anchorTextDiversityRule``
     in the same category are untouched.

Stdlib unittest only, same conventions as the other rules tests. Run from
python_workers/:

    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_content_and_enhancement_rules_removed -v
"""

import unittest

from scraper.workers.seo.page_analysis.rules.seo_rule_registry import SEORuleRegistry
from scraper.workers.seo.page_analysis.rules.categories import register_all_seo_categories
from scraper.workers.seo.page_analysis.rules.categories import content_rules
from scraper.workers.seo.page_analysis.rules.categories import seo_enhancements
from scraper.workers.seo.page_analysis.rules.categories.content_rules import (
    KeywordNotInH1Rule,
    register_content_rules,
)
from scraper.workers.seo.page_analysis.rules.categories.seo_enhancements import (
    register_seo_enhancement_rules,
)

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://example.com/page"

# Content category rules that survive the removal, in registration order.
EXPECTED_CONTENT_RULES = [
    "multiple_meta_descriptions", "multiple_title_tags", "title_missing",
    "meta_description_missing", "title_too_short", "title_too_long",
    "meta_description_too_short", "meta_description_too_long", "h1_missing",
    "multiple_h1_tags", "heading_hierarchy_skipped", "thin_content",
    "keyword_not_in_h1", "duplicate_content",
]

# SEO Enhancements category rules that survive the removal (metaDescriptionCtrRule
# and imageAltTextQualityRule are separately DISABLED, not registered either).
EXPECTED_SEO_ENHANCEMENT_RULES = ["anchor_text_optimization", "anchor_text_diversity"]

# Registry-wide total. 68 -> 67 (title_pixel_length removed, this file) ->
# 64 (og_tags_missing/og_tags_incomplete/twitter_card_tags_missing disabled,
# see test_social_rules_removed.py). Every "*_removed.py" test file in this
# directory that asserts a total rule count must be kept in sync whenever
# any rule anywhere is added, removed, or (re)disabled.
EXPECTED_TOTAL_RULES = 64


def _build_registry():
    registry = SEORuleRegistry()
    register_all_seo_categories(registry)
    return registry


class KeywordNotInTitleRuleRemoved(unittest.TestCase):
    def setUp(self):
        self.registry = _build_registry()

    def test_rule_is_not_registered(self):
        self.assertFalse(self.registry.has_rule("keyword_not_in_title"))

    def test_rule_class_is_gone(self):
        self.assertFalse(
            hasattr(content_rules, "KeywordNotInTitleRule"),
            "KeywordNotInTitleRule class must be deleted, not just unregistered",
        )

    def test_sibling_keyword_not_in_h1_rule_still_registered_and_works(self):
        self.assertTrue(self.registry.has_rule("keyword_not_in_h1"))
        normalized = {
            "primary_keyword": "website audit tool",
            "title": "Contact Sales | Odito.ai",
            "headings": [{"tag": "h1", "text": "Master Your SEO With Precision"}],
        }
        issues = KeywordNotInH1Rule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertTrue(issues)

    def test_content_category_contains_exactly_the_surviving_rules(self):
        got = [r.rule_id for r in self.registry.get_rules_by_category("Content")]
        self.assertEqual(got, EXPECTED_CONTENT_RULES)

    def test_register_content_rules_never_registers_removed_rule(self):
        registry = SEORuleRegistry()
        register_content_rules(registry)
        self.assertNotIn("keyword_not_in_title", [r.rule_id for r in registry.get_all_rules()])

    def test_total_registered_rule_count(self):
        self.assertEqual(self.registry.get_rule_count(), EXPECTED_TOTAL_RULES)


class TitlePixelLengthRuleRemoved(unittest.TestCase):
    """The whole titlePixelLengthRule (desktop + mobile, all tiers) must be
    gone: not registered, and the class itself deleted from the module."""

    def setUp(self):
        self.registry = _build_registry()

    def test_rule_is_not_registered(self):
        self.assertFalse(self.registry.has_rule("title_pixel_length"))

    def test_rule_class_is_gone(self):
        self.assertFalse(
            hasattr(seo_enhancements, "titlePixelLengthRule"),
            "titlePixelLengthRule class must be deleted, not just unregistered",
        )

    def test_seo_enhancements_category_contains_exactly_the_surviving_rules(self):
        got = [r.rule_id for r in self.registry.get_rules_by_category("SEO Enhancements")]
        self.assertEqual(got, EXPECTED_SEO_ENHANCEMENT_RULES)

    def test_register_seo_enhancement_rules_never_registers_removed_rule(self):
        registry = SEORuleRegistry()
        register_seo_enhancement_rules(registry)
        self.assertNotIn("title_pixel_length", [r.rule_id for r in registry.get_all_rules()])

    def test_no_issue_message_mentions_title_pixel_width_truncation(self):
        # Belt-and-suspenders: no surviving rule should coincidentally emit
        # wording that used to belong to the removed rule.
        for r in self.registry.get_all_rules():
            self.assertNotIn("pixel", r.description.lower())

    def test_total_registered_rule_count(self):
        self.assertEqual(self.registry.get_rule_count(), EXPECTED_TOTAL_RULES)


class EnginePipelineUnaffected(unittest.TestCase):
    """The full engine builds and never emits any of the removed issues."""

    def test_analyze_page_never_emits_removed_issues(self):
        from scraper.workers.seo.page_analysis.rules.seo_rule_engine import SEORuleEngine

        normalized = {
            "content": "Welcome to our services. We help businesses grow.",
            "content_text": "Welcome to our services. We help businesses grow.",
            "title": "Home",
            "meta_tags": {},
            "headings": [{"tag": "h1", "text": "We help businesses grow their revenue"}],
            "structured_data": [],
            "images": [],
            "links": [],
            "internal_links": [],
            "external_links": [],
            "title_pixel_width": 700,
            "desktop_status": "FAIL",
            "mobile_status": "FAIL",
        }
        engine = SEORuleEngine(_build_registry())
        result = engine.analyze_page(normalized, JOB_ID, PROJECT_ID, URL)
        all_items = result["issues"] + result["recommendations"]
        rule_ids = {i.get("rule_id") for i in all_items}
        messages = [i.get("issue_message", "") for i in all_items]

        self.assertNotIn("keyword_not_in_title", rule_ids)
        self.assertNotIn("title_pixel_length", rule_ids)
        self.assertFalse(any("Primary topic from H1 not found in title" in m for m in messages), messages)
        self.assertFalse(any("approaching mobile limit" in m for m in messages), messages)
        self.assertFalse(any("approaching desktop limit" in m for m in messages), messages)
        self.assertFalse(any("Title too long for desktop" in m for m in messages), messages)
        self.assertFalse(any("Title too long for mobile" in m for m in messages), messages)
        self.assertIn("summary", result)
        self.assertEqual(result["summary"]["total_rules"], EXPECTED_TOTAL_RULES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
