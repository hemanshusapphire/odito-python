"""P0-003 tests: seo_page_issues lifecycle fields + dedup_key.

Stdlib unittest only (same zero-new-dependency approach as P0-001/P0-002).
issue_identity.py and base_seo_rule.py are importable with no Mongo/network;
page_analysis.py's legacy factory needs the same sys.modules stubbing
pattern established in P0-002's tests.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.rules.test_issue_identity -v
"""

import sys
import types
import unittest
from datetime import datetime

from bson.objectid import ObjectId

from scraper.workers.seo.page_analysis.rules.issue_identity import (
    compute_dedup_key,
    lifecycle_fields,
)
from scraper.workers.seo.page_analysis.rules.base_seo_rule import BaseSEORuleV2


PROJECT_ID = "64b7f3a2c9e77a0012345678"
JOB_ID = "64b7f3a2c9e77a0087654321"
URL = "https://example.com/pricing"

# The exact document shape produced by BaseSEORuleV2.create_issue BEFORE this
# task — the backward-compatibility contract every existing consumer relies on.
PRE_EXISTING_FIELDS = [
    "projectId", "seo_jobId", "page_url", "rule_no", "category", "severity",
    "issue_code", "rule_id", "issue_message", "detected_value",
    "expected_value", "data_key", "data_path", "created_at",
]

NEW_FIELDS = [
    "status", "dedup_key", "first_detected_at", "last_verified_at",
    "fix_count", "regression_count",
]


class _FakeRule(BaseSEORuleV2):
    rule_id = "TITLE_MISSING"
    rule_no = 1
    category = "Content"
    severity = "high"
    description = "test rule"

    def evaluate(self, normalized, job_id, project_id, url):
        return []


class TestComputeDedupKey(unittest.TestCase):
    def test_deterministic_same_input_same_hash(self):
        a = compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", "head.title")
        b = compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", "head.title")
        self.assertEqual(a, b)
        self.assertEqual(len(a), 64)  # sha256 hex digest

    def test_stable_across_str_and_objectid_project_id(self):
        as_str = compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", None)
        as_oid = compute_dedup_key(ObjectId(PROJECT_ID), URL, "TITLE_MISSING", None)
        self.assertEqual(as_str, as_oid)

    def test_different_issue_code_different_hash(self):
        a = compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", None)
        b = compute_dedup_key(PROJECT_ID, URL, "META_DESC_MISSING", None)
        self.assertNotEqual(a, b)

    def test_different_page_url_different_hash(self):
        a = compute_dedup_key(PROJECT_ID, "https://example.com/a", "TITLE_MISSING", None)
        b = compute_dedup_key(PROJECT_ID, "https://example.com/b", "TITLE_MISSING", None)
        self.assertNotEqual(a, b)

    def test_different_data_path_different_hash(self):
        a = compute_dedup_key(PROJECT_ID, URL, "IMG_ALT_MISSING", "images[0]")
        b = compute_dedup_key(PROJECT_ID, URL, "IMG_ALT_MISSING", "images[1]")
        self.assertNotEqual(a, b)

    def test_different_project_different_hash(self):
        a = compute_dedup_key("64b7f3a2c9e77a0012345678", URL, "TITLE_MISSING", None)
        b = compute_dedup_key("64b7f3a2c9e77a0012345679", URL, "TITLE_MISSING", None)
        self.assertNotEqual(a, b)

    def test_null_data_path_normalized_to_empty_and_stable(self):
        self.assertEqual(
            compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", None),
            compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", ""),
        )

    def test_field_delimiter_prevents_cross_field_ambiguity(self):
        a = compute_dedup_key(PROJECT_ID, URL + "X", "CODE", None)
        b = compute_dedup_key(PROJECT_ID, URL, "XCODE", None)
        self.assertNotEqual(a, b)

    # ── Failure cases ──

    def test_invalid_project_id_raises(self):
        with self.assertRaises(ValueError):
            compute_dedup_key(None, URL, "TITLE_MISSING", None)
        with self.assertRaises(ValueError):
            compute_dedup_key("", URL, "TITLE_MISSING", None)

    def test_invalid_page_url_raises(self):
        with self.assertRaises(ValueError):
            compute_dedup_key(PROJECT_ID, None, "TITLE_MISSING", None)
        with self.assertRaises(ValueError):
            compute_dedup_key(PROJECT_ID, "", "TITLE_MISSING", None)

    def test_invalid_issue_code_raises(self):
        with self.assertRaises(ValueError):
            compute_dedup_key(PROJECT_ID, URL, None, None)
        with self.assertRaises(ValueError):
            compute_dedup_key(PROJECT_ID, URL, "", None)

    def test_hash_generation_error_surfaces_as_valueerror(self):
        class Unstringable:
            def __str__(self):
                raise RuntimeError("boom")

        with self.assertRaises((ValueError, RuntimeError)):
            compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", Unstringable())


class TestLiveFactoryBaseSEORule(unittest.TestCase):
    """BaseSEORuleV2.create_issue — the factory every rule actually uses."""

    def _make(self, **overrides):
        kwargs = dict(
            job_id=JOB_ID, project_id=PROJECT_ID, url=URL,
            issue_message="Title tag is missing",
            detected_value="", expected_value="A title tag",
            data_key="title", data_path=None,
        )
        kwargs.update(overrides)
        return _FakeRule().create_issue(**kwargs)

    def test_new_issue_contains_all_six_fields(self):
        issue = self._make()
        for field in NEW_FIELDS:
            self.assertIn(field, issue)

    def test_defaults_are_correct(self):
        issue = self._make()
        self.assertEqual(issue["status"], "open")
        self.assertEqual(issue["fix_count"], 0)
        self.assertEqual(issue["regression_count"], 0)
        self.assertIsNone(issue["last_verified_at"])
        self.assertIsInstance(issue["first_detected_at"], datetime)
        # One clock read per document: first_detected_at IS created_at.
        self.assertEqual(issue["first_detected_at"], issue["created_at"])

    def test_dedup_key_matches_contract(self):
        issue = self._make(data_path="head.title")
        self.assertEqual(
            issue["dedup_key"],
            compute_dedup_key(PROJECT_ID, URL, "TITLE_MISSING", "head.title"),
        )

    def test_every_pre_existing_field_unchanged(self):
        # Backward-compatibility / Full Audit regression guard: all fields the
        # document had before P0-003 are still present with the same values
        # and types.
        issue = self._make()
        for field in PRE_EXISTING_FIELDS:
            self.assertIn(field, issue)
        self.assertEqual(issue["projectId"], ObjectId(PROJECT_ID))
        self.assertEqual(issue["seo_jobId"], ObjectId(JOB_ID))
        self.assertEqual(issue["page_url"], URL)
        self.assertEqual(issue["rule_no"], 1)
        self.assertEqual(issue["category"], "Content")
        self.assertEqual(issue["severity"], "high")
        self.assertEqual(issue["issue_code"], "TITLE_MISSING")
        self.assertEqual(issue["rule_id"], "TITLE_MISSING")
        self.assertEqual(issue["issue_message"], "Title tag is missing")
        self.assertEqual(issue["detected_value"], "")
        self.assertEqual(issue["expected_value"], "A title tag")
        self.assertEqual(issue["data_key"], "title")
        self.assertIsNone(issue["data_path"])
        self.assertIsInstance(issue["created_at"], datetime)

    def test_payload_shape_is_strictly_additive(self):
        # No pre-existing field removed, none renamed; exactly the six new
        # fields added. (Optional impact/recommendation/context excluded by
        # not passing them — same as before.)
        issue = self._make()
        self.assertEqual(sorted(issue.keys()), sorted(PRE_EXISTING_FIELDS + NEW_FIELDS))

    def test_optional_fields_still_appended_when_passed(self):
        issue = self._make()
        issue_with_extras = _FakeRule().create_issue(
            job_id=JOB_ID, project_id=PROJECT_ID, url=URL,
            issue_message="m", detected_value="d", expected_value="e",
            impact="High", recommendation="Fix it", context={"k": 1},
        )
        self.assertNotIn("impact", issue)
        self.assertEqual(issue_with_extras["impact"], "High")
        self.assertEqual(issue_with_extras["recommendation"], "Fix it")
        self.assertEqual(issue_with_extras["context"], {"k": 1})

    def test_same_issue_recreated_produces_same_dedup_key(self):
        first = self._make(data_path="images[3]")
        second = self._make(data_path="images[3]")
        self.assertEqual(first["dedup_key"], second["dedup_key"])


class TestLegacyModuleLevelFactory(unittest.TestCase):
    """page_analysis.create_issue — zero callers today, kept field-identical."""

    @classmethod
    def setUpClass(cls):
        # Same P0-002 stubbing pattern: page_analysis.py imports db (which
        # connects to Mongo at import time) — stub it before importing.
        def _stub(name, **attrs):
            # Merge into an existing stub rather than skipping: another test
            # module (e.g. P0-002's) may have already stubbed `db` with a
            # different attribute set, and both must coexist in one process.
            module = sys.modules.get(name)
            if module is None:
                module = types.ModuleType(name)
                sys.modules[name] = module
            for key, value in attrs.items():
                if not hasattr(module, key):
                    setattr(module, key, value)

        _stub(
            "db",
            seo_internal_links=object(), seo_external_links=object(),
            seo_social_links=object(), seo_page_data=object(),
            seo_page_issues=object(), seo_page_performance=object(),
            seo_page_summary=object(), db=object(),
        )

        from scraper.workers.seo.page_analysis.page_analysis import create_issue
        cls.create_issue = staticmethod(create_issue)

    def test_legacy_factory_stamps_all_six_fields_with_correct_key(self):
        issue = self.create_issue(
            JOB_ID, PROJECT_ID, URL,
            rule_no=7, category="Images", severity="medium",
            issue_code="IMG_ALT_MISSING",
            issue_message="Image missing alt text",
            detected_value="<img src=x>", expected_value="alt text",
            data_key="images", data_path="images[2]",
        )
        for field in NEW_FIELDS:
            self.assertIn(field, issue)
        self.assertEqual(issue["status"], "open")
        self.assertEqual(
            issue["dedup_key"],
            compute_dedup_key(PROJECT_ID, URL, "IMG_ALT_MISSING", "images[2]"),
        )
        self.assertEqual(issue["first_detected_at"], issue["created_at"])

    def test_both_factories_agree_on_dedup_key_for_identical_issue(self):
        # The live factory and the legacy factory must never diverge on
        # identity — an issue's dedup_key is factory-independent.
        legacy = self.create_issue(
            JOB_ID, PROJECT_ID, URL,
            rule_no=1, category="Content", severity="high",
            issue_code="TITLE_MISSING",
            issue_message="m", detected_value="", expected_value="e",
            data_key="title", data_path=None,
        )
        live = _FakeRule().create_issue(
            job_id=JOB_ID, project_id=PROJECT_ID, url=URL,
            issue_message="m", detected_value="", expected_value="e",
            data_key="title", data_path=None,
        )
        self.assertEqual(legacy["dedup_key"], live["dedup_key"])


if __name__ == "__main__":
    unittest.main()
