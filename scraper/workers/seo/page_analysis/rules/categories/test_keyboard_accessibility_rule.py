"""KeyboardAccessibilityRule — turns the stored keyboard audit into issue documents.

v2 audits carry the EXACT elements; the rule puts them on the issue's `context` (for the
recommendation and the UI) and `before_snapshot` (for the task's before-state) without
changing issue identity (dedup_key = project|url|issue_code|data_path is unchanged, so
existing tasks stay linked). v1 (counters only) data no longer produces the bogus trap.

Stdlib unittest only. Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.rules.categories.test_keyboard_accessibility_rule -v
"""

import json
import unittest
from pathlib import Path

from scraper.workers.seo.page_analysis.rules.categories.accessibility_rules import KeyboardAccessibilityRule
from scraper.workers.seo.page_analysis.rules.issue_identity import compute_dedup_key

JOB_ID = "64b7f3a2c9e77a0012345678"
PROJECT_ID = "64b7f3a2c9e77a0012345679"
URL = "https://naxonify.com/"

# A REAL audit captured from https://naxonify.com/ (shared with the Node tests).
FIXTURE = Path(__file__).resolve().parents[7] / "odito_backend" / "src" / "modules" / "issue-context" / "__fixtures__" / "naxonify-home-keyboard-audit.json"


def evaluate(keyboard):
    return KeyboardAccessibilityRule().evaluate({"headless": {"keyboard_analysis": keyboard}}, JOB_ID, PROJECT_ID, URL)


def by_path(issues):
    return {i["data_path"]: i for i in issues}


def element(**over):
    e = {"tag": "a", "role": "link", "accessibleName": "About", "selector": "#a", "selectorUnique": True, "domPath": "nav > a",
         "classes": ["x"], "tabindex": None, "container": {"selector": "nav", "role": "nav"}, "rect": {"x": 1, "y": 2, "w": 3, "h": 4},
         "computed": {"effectiveBackground": "rgb(255, 255, 255)", "color": "x"},
         "focusIndicator": {"status": "missing", "reason": "no_visual_change_on_focus", "signals": [], "before": {"outline": "none"}, "after": {"outline": "none"},
                            "focusRuleFound": False, "focusRules": [], "suppressingRule": {"selector": "a", "stylesheet": "inline <style>"}}}
    e.update(over)
    return e


def v2(**over):
    k = {"keyboard_navigation_checked": True, "audit_version": 2, "audit_method": "m", "tested_at": "2026-09-26T05:00:00+00:00",
         "focus_trap_detected": False, "unreachable_elements": 0, "small_click_targets": 0, "missing_focus_outline": 0,
         "tab_stops_visited": 12, "focusable_visible": 12, "traversal": {"completed": True}, "technology": {"cms": "WordPress"},
         "affected_elements": {"missing_focus_indicator": [], "missing_focus_indicator_total": 0, "unreachable": [], "weak_focus_indicator": []},
         "trap_details": {"detected": False, "intentional": False, "verdict": "none"}, "focus_sequence": []}
    k.update(over)
    return k


class RealNaxonifyAudit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.keyboard = json.loads(FIXTURE.read_text(encoding="utf-8"))
        cls.issues = evaluate(cls.keyboard)

    def test_exactly_one_issue_and_it_is_the_missing_indicator_one_not_a_trap(self):
        self.assertEqual([i["data_path"] for i in self.issues], ["keyboard_analysis.missing_focus_outline"])

    def test_message_reports_the_real_numbers(self):
        self.assertEqual(self.issues[0]["issue_message"], "Missing focus indicators: 40 of 44 keyboard-focusable elements show no visible focus indicator")

    def test_detected_value_is_the_raw_selectors_not_a_diagnostic_sentence(self):
        dv = self.issues[0]["detected_value"]
        self.assertIsInstance(dv, list)
        self.assertEqual(len(dv), 25)
        self.assertTrue(all(isinstance(s, str) and s for s in dv))
        self.assertFalse(any("Keyboard navigation test" in s for s in dv))

    def test_context_carries_the_structured_audit(self):
        ctx = self.issues[0]["context"]
        self.assertEqual(ctx["issueType"], "missing_focus_indicator")
        self.assertEqual(ctx["affectedElementCount"], 40)
        self.assertEqual(ctx["testedElementCount"], 44)
        self.assertEqual(len(ctx["affectedElements"]), 25)
        self.assertTrue(ctx["listTruncated"])
        self.assertEqual(ctx["technology"]["builder"], "Divi")
        self.assertEqual(ctx["auditVersion"], 2)
        first = ctx["affectedElements"][0]
        self.assertEqual(first["accessibleName"], "Naxonify Transform Grow Lead")
        self.assertEqual(first["focusIndicator"]["status"], "missing")
        self.assertEqual(first["focusIndicator"]["suppressingRule"]["stylesheet"], "inline <style>")
        json.dumps(ctx)  # JSON-serializable

    def test_context_stays_small_and_has_no_raw_html_or_computed_dumps(self):
        blob = json.dumps(self.issues[0]["context"])
        self.assertLess(len(blob), 60_000)
        self.assertNotIn('"computed"', blob)

    def test_before_snapshot_lists_the_elements_for_the_task(self):
        snap = self.issues[0]["before_snapshot"]
        self.assertEqual(snap["type"], "keyboard_accessibility")
        self.assertEqual(snap["finding"], "missing_focus_indicator")
        self.assertEqual(snap["count"], 40)
        self.assertEqual(len(snap["elements"]), 25)

    def test_issue_identity_is_unchanged_so_existing_tasks_stay_linked(self):
        issue = self.issues[0]
        self.assertEqual(issue["issue_code"], "keyboard_accessibility")
        self.assertEqual(issue["dedup_key"], compute_dedup_key(PROJECT_ID, URL, "keyboard_accessibility", "keyboard_analysis.missing_focus_outline"))


class V2Findings(unittest.TestCase):
    def test_no_findings_no_issues(self):
        self.assertEqual(evaluate(v2()), [])

    def test_unchecked_navigation_produces_nothing(self):
        self.assertEqual(evaluate({"keyboard_navigation_checked": False}), [])

    def test_an_unintended_trap_is_reported_with_container_cycle_and_sequence(self):
        trap = {"detected": True, "intentional": False, "verdict": "hidden_container_trap", "suspectedCause": "closed drawer in tab order", "cycleLength": 2,
                "container": {"selector": "div.mobile-menu", "role": "div", "visible": False},
                "cycle": [{"selector": "div.mobile-menu a.one", "tag": "a", "accessibleName": "One"}, {"selector": "div.mobile-menu a.two", "tag": "a", "accessibleName": "Two"}]}
        issues = evaluate(v2(focus_trap_detected=True, trap_details=trap, focus_sequence=[{"step": 1, "from": "document start", "to": "#logo"}]))
        issue = by_path(issues)["keyboard_analysis.focus_trap_detected"]
        self.assertEqual(issue["issue_message"], "Focus trap detected in div.mobile-menu - keyboard users cannot navigate away from 2 elements")
        self.assertEqual(issue["detected_value"], ["div.mobile-menu a.one", "div.mobile-menu a.two"])
        self.assertEqual(issue["context"]["trapDetails"]["verdict"], "hidden_container_trap")
        self.assertEqual(issue["context"]["focusSequence"][0]["to"], "#logo")
        self.assertEqual(issue["before_snapshot"]["container"], "div.mobile-menu")

    def test_an_intentional_modal_trap_is_NOT_an_issue(self):
        trap = {"detected": True, "intentional": True, "verdict": "intentional_modal", "container": {"selector": "#dlg", "role": "dialog"}}
        self.assertEqual(evaluate(v2(focus_trap_detected=False, trap_details=trap)), [])

    def test_unreachable_elements_are_named(self):
        issues = evaluate(v2(unreachable_elements=1, affected_elements={"missing_focus_indicator": [], "missing_focus_indicator_total": 0, "weak_focus_indicator": [],
                                                                            "unreachable": [{"selector": "#lost", "tag": "button", "accessibleName": "Lost"}]}))
        issue = by_path(issues)["keyboard_analysis.unreachable_elements"]
        self.assertEqual(issue["detected_value"], ["#lost"])
        self.assertEqual(issue["context"]["affectedElements"][0]["accessibleName"], "Lost")

    def test_weak_indicators_alone_do_not_create_an_issue(self):
        self.assertEqual(evaluate(v2(affected_elements={"missing_focus_indicator": [], "missing_focus_indicator_total": 0, "unreachable": [], "weak_focus_indicator": [element()]})), [])

    def test_all_three_findings_on_one_page_are_separate_issues_with_distinct_identities(self):
        trap = {"detected": True, "intentional": False, "verdict": "unintended", "cycleLength": 1, "cycle": [{"selector": "#t", "tag": "a"}], "container": None}
        issues = evaluate(v2(focus_trap_detected=True, trap_details=trap,
                             affected_elements={"missing_focus_indicator": [element()], "missing_focus_indicator_total": 1, "weak_focus_indicator": [], "unreachable": [{"selector": "#lost", "tag": "a"}]}))
        self.assertEqual(len(issues), 3)
        self.assertEqual(len({i["dedup_key"] for i in issues}), 3)
        self.assertTrue(all(i["issue_code"] == "keyboard_accessibility" for i in issues))

    def test_a_trap_without_an_identified_container_says_so(self):
        trap = {"detected": True, "intentional": False, "verdict": "unintended", "cycleLength": 3, "cycle": [{"selector": "#a"}], "container": None}
        issue = evaluate(v2(focus_trap_detected=True, trap_details=trap))[0]
        self.assertIn("on this page", issue["issue_message"])
        self.assertEqual(issue["before_snapshot"]["container"], None)


class LegacyV1Data(unittest.TestCase):
    V1 = {"keyboard_navigation_checked": True, "focus_trap_detected": True, "unreachable_elements": 2, "small_click_targets": 0,
          "missing_focus_outline": 10, "total_tab_presses": 10, "focus_order": [{"tag": "a", "id": "", "selector": "a"}] * 5}

    def test_the_v1_trap_heuristic_is_no_longer_trusted(self):
        """v1 compared tag+id, so any three id-less links looked like 'the same element' — a false positive."""
        paths = {i["data_path"] for i in evaluate(self.V1)}
        self.assertNotIn("keyboard_analysis.focus_trap_detected", paths)

    def test_v1_counters_still_produce_the_previous_messages_until_the_page_is_re_scanned(self):
        issues = by_path(evaluate(self.V1))
        self.assertEqual(issues["keyboard_analysis.missing_focus_outline"]["issue_message"], "Missing focus indicators: 10 elements lack visible focus outlines")
        self.assertEqual(issues["keyboard_analysis.unreachable_elements"]["issue_message"], "Unreachable interactive elements detected: 2 elements not reachable by keyboard")
        self.assertNotIn("context", issues["keyboard_analysis.missing_focus_outline"])

    def test_v1_identity_matches_v2_identity_for_the_same_finding(self):
        v1 = by_path(evaluate(self.V1))["keyboard_analysis.missing_focus_outline"]
        v2_issue = evaluate(v2(affected_elements={"missing_focus_indicator": [element()], "missing_focus_indicator_total": 1, "weak_focus_indicator": [], "unreachable": []}))[0]
        self.assertEqual(v1["dedup_key"], v2_issue["dedup_key"])


if __name__ == "__main__":
    unittest.main()
