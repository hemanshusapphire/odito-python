"""Regression: two removed Accessibility rules stay removed.

Covers the removal of:
  1. "Images Missing Alt Text" (Accessibility) — the whole
     ``AltTextAccessibilityRule`` (rule_id "alt_text_accessibility", rule_no 111,
     category "Accessibility"). NOTE: the separate Images-category rule
     ``ImagesMissingAltTextRule`` (rule_id "images_missing_alt_text", rule_no 81)
     is intentionally kept and must still work.
  2. "Insufficient colour contrast" — the whole ``TextContrastRule``
     (rule_id "text_contrast", rule_no 112, category "Accessibility").

The headless axe-core scan and the other six Accessibility rules are untouched.

Stdlib unittest only. Run from python_workers/:

    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_accessibility_rules_removed -v
"""

import unittest

from scraper.workers.seo.page_analysis.rules.seo_rule_registry import SEORuleRegistry
from scraper.workers.seo.page_analysis.rules.categories import register_all_seo_categories
from scraper.workers.seo.page_analysis.rules.categories import accessibility_rules
from scraper.workers.seo.page_analysis.rules.categories.accessibility_rules import (
    register_accessibility_rules,
)
from scraper.workers.seo.page_analysis.rules.categories.image_rules import ImagesMissingAltTextRule

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://example.com/page"

# Accessibility rules that survive the removal, in registration order.
EXPECTED_ACCESSIBILITY_RULES = [
    "form_labels",
    "keyboard_accessibility",
    "focus_indicators",
    "page_language",
    "video_captions",
    "tap_target_size",
]


def _build_registry():
    registry = SEORuleRegistry()
    register_all_seo_categories(registry)
    return registry


class RemovedRulesNotRegistered(unittest.TestCase):
    def setUp(self):
        self.registry = _build_registry()

    def test_alt_text_accessibility_rule_not_registered(self):
        self.assertFalse(self.registry.has_rule("alt_text_accessibility"))

    def test_text_contrast_rule_not_registered(self):
        self.assertFalse(self.registry.has_rule("text_contrast"))

    def test_rule_classes_are_deleted(self):
        self.assertFalse(hasattr(accessibility_rules, "AltTextAccessibilityRule"))
        self.assertFalse(hasattr(accessibility_rules, "TextContrastRule"))

    def test_accessibility_category_is_exactly_the_survivors(self):
        got = [r.rule_id for r in self.registry.get_rules_by_category("Accessibility")]
        self.assertEqual(got, EXPECTED_ACCESSIBILITY_RULES)

    def test_register_accessibility_rules_registers_only_survivors(self):
        registry = SEORuleRegistry()
        register_accessibility_rules(registry)
        self.assertEqual(
            [r.rule_id for r in registry.get_all_rules()],
            EXPECTED_ACCESSIBILITY_RULES,
        )

    def test_no_rule_emits_the_removed_issue_messages(self):
        """A page with a missing-alt image AND a contrast axe violation must not
        produce either removed issue from the Accessibility category."""
        normalized = {
            "images": [{"src": "https://example.com/a.png", "alt": ""}],
            "headless": {
                "axeViolations": [
                    {"id": "color-contrast", "description": "Elements must have sufficient color contrast", "nodes": 3},
                ],
            },
        }
        for rule in _build_registry().get_rules_by_category("Accessibility"):
            for issue in rule.evaluate(normalized, JOB_ID, PROJECT_ID, URL):
                self.assertNotIn(issue.get("rule_id"), ("alt_text_accessibility", "text_contrast"))
                self.assertNotIn("Insufficient colour contrast", issue.get("issue_message", ""))


class SurvivingRulesStillWork(unittest.TestCase):
    def test_images_category_alt_text_rule_still_fires(self):
        """The Images-category ImagesMissingAltTextRule (rule_no 81) is a
        different rule and must still flag missing alt text."""
        registry = _build_registry()
        self.assertTrue(registry.has_rule("images_missing_alt_text"))

        normalized = {"images": [{"src": "https://example.com/logo.png", "alt": ""}]}
        issues = ImagesMissingAltTextRule().evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["rule_id"], "images_missing_alt_text")
        self.assertEqual(issues[0]["issue_message"], "Images Missing Alt Text")

    def test_other_accessibility_rules_still_consume_axe_violations(self):
        """FormLabelsRule / VideoCaptionsRule still read headless axeViolations —
        the shared array and headless scan are untouched."""
        registry = _build_registry()
        for rule_id in ("form_labels", "video_captions", "keyboard_accessibility",
                        "focus_indicators", "page_language", "tap_target_size"):
            self.assertTrue(registry.has_rule(rule_id), rule_id)

        normalized = {
            "headless": {
                "axeViolations": [
                    {"id": "label", "description": "Form elements must have labels", "nodes": 2},
                ],
            },
        }
        form_rule = next(r for r in registry.get_rules_by_category("Accessibility")
                         if r.rule_id == "form_labels")
        issues = form_rule.evaluate(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertTrue(any(i["rule_id"] == "form_labels" for i in issues))


class EnginePipelineUnaffected(unittest.TestCase):
    def test_full_engine_builds_and_summary_count_is_69(self):
        from scraper.workers.seo.page_analysis.rules.seo_rule_engine import SEORuleEngine
        engine = SEORuleEngine(_build_registry())
        normalized = {
            "images": [{"src": "https://example.com/a.png", "alt": ""}],
            "headless": {"axeViolations": [{"id": "color-contrast", "description": "contrast", "nodes": 1}]},
            "content": "hello world", "content_text": "hello world",
            "meta_tags": {}, "headings": [], "structured_data": [],
            "links": [], "internal_links": [], "external_links": [],
        }
        result = engine.analyze_page(normalized, JOB_ID, PROJECT_ID, URL)
        self.assertEqual(result["summary"]["total_rules"], 69)
        all_items = result["issues"] + result["recommendations"]
        rule_ids = {i.get("rule_id") for i in all_items}
        self.assertNotIn("alt_text_accessibility", rule_ids)
        self.assertNotIn("text_contrast", rule_ids)


if __name__ == "__main__":
    unittest.main(verbosity=2)
