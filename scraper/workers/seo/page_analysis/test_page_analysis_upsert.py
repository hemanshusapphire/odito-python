"""P0-005 tests: seo_page_issues dedup_key unique index + upsert write path.

Stdlib unittest only (same approach as P0-001..P0-004). page_analysis.py
imports db (Mongo at import) — stubbed via sys.modules BEFORE import, same
pattern as prior tasks. Index-behavior tests that need REAL unique-index
semantics run against the local Mongo on a throwaway collection and
auto-skip when Mongo is unreachable.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.test_page_analysis_upsert -v
"""

import copy
import sys
import types
import unittest
from datetime import datetime

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError


def _stub_module(name, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        if not hasattr(module, key):
            setattr(module, key, value)


_stub_module(
    "db",
    seo_internal_links=object(), seo_external_links=object(),
    seo_social_links=object(), seo_page_data=object(),
    seo_page_issues=object(), seo_page_performance=object(),
    seo_page_summary=object(), db=object(),
)

from scraper.workers.seo.page_analysis.page_analysis import (  # noqa: E402
    upsert_issues,
    LIFECYCLE_INSERT_ONLY_FIELDS,
    issues_to_persist,
)
from scraper.workers.seo.page_analysis.rules.issue_identity import compute_dedup_key  # noqa: E402


PROJECT_ID = "64b7f3a2c9e77a0012345678"


def _issue(url="https://example.com/a", code="TITLE_MISSING", data_path=None, **extra):
    """A complete factory-shaped issue document (P0-003 fields included)."""
    created = datetime(2026, 7, 27, 10, 0, 0)
    doc = {
        "projectId": PROJECT_ID,
        "seo_jobId": "64b7f3a2c9e77a0087654321",
        "page_url": url,
        "rule_no": 1,
        "category": "Content",
        "severity": "high",
        "issue_code": code,
        "rule_id": code,
        "issue_message": "msg",
        "detected_value": "d",
        "expected_value": "e",
        "data_key": "title",
        "data_path": data_path,
        "created_at": created,
        "status": "open",
        "dedup_key": compute_dedup_key(PROJECT_ID, url, code, data_path),
        "first_detected_at": created,
        "last_verified_at": None,
        "fix_count": 0,
        "regression_count": 0,
    }
    doc.update(extra)
    return doc


class FakeBulkResult:
    def __init__(self, upserted, modified, matched):
        self.upserted_count = upserted
        self.modified_count = modified
        self.matched_count = matched


class RecordingCollection:
    def __init__(self, errors=None):
        self.calls = []
        self._errors = list(errors or [])

    def bulk_write(self, ops, ordered=True):
        self.calls.append((list(ops), ordered))
        if self._errors:
            error = self._errors.pop(0)
            if error is not None:
                raise error
        return FakeBulkResult(len(ops), 0, 0)


class InMemoryCollection:
    """Enough of bulk_write(UpdateOne, upsert=True) semantics keyed on
    dedup_key: $set applied always on match, $setOnInsert only on insert."""

    def __init__(self):
        self.docs = {}

    def bulk_write(self, ops, ordered=True):
        upserted = modified = matched = 0
        for op in ops:
            key = op._filter["dedup_key"]
            update = op._doc
            if key in self.docs:
                matched += 1
                self.docs[key].update(update.get("$set", {}))
                modified += 1
            else:
                new_doc = {}
                new_doc.update(update.get("$setOnInsert", {}))
                new_doc.update(update.get("$set", {}))
                new_doc["dedup_key"] = key
                self.docs[key] = new_doc
                upserted += 1
        return FakeBulkResult(upserted, modified, matched)


def _bulk_error(write_errors):
    return BulkWriteError({"writeErrors": write_errors})


class TestIssuesToPersistIncludesRecommendations(unittest.TestCase):
    """Regression: SEORuleEngine.analyze_page() splits findings into
    "issues" (severity high/medium) and "recommendations" (severity
    low/info) purely for coverage bookkeeping — both are real findings a
    rule created via create_issue() and both must reach seo_page_issues.

    Concretely triggered by OrganizationSchemaRule's recommended-fields
    branch: correcting its severity from "high" to "low" made the engine
    route it into "recommendations", and the caller previously only ever
    read page_result["issues"] — so the finding silently stopped being
    persisted even though the rule was still creating it every run.
    """

    def test_merges_issues_and_recommendations(self):
        page_result = {
            "issues": [_issue(code="TITLE_MISSING")],
            "recommendations": [_issue(code="organization_schema", extra_field="low-severity-finding")],
        }
        merged = issues_to_persist(page_result)
        self.assertEqual(len(merged), 2)
        codes = [i["issue_code"] for i in merged]
        self.assertIn("TITLE_MISSING", codes)
        self.assertIn("organization_schema", codes)

    def test_low_severity_only_finding_is_not_dropped(self):
        # The exact regression shape: a rule fires ONLY a low-severity
        # recommendation (no high/medium issue alongside it) — e.g. a valid
        # Organization schema missing just the recommended `address` field.
        page_result = {"issues": [], "recommendations": [_issue(code="organization_schema")]}
        merged = issues_to_persist(page_result)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["issue_code"], "organization_schema")

    def test_missing_recommendations_key_does_not_error(self):
        # Early-exit paths in analyze_page_seo() (normalization failure,
        # incomplete document) return {"issues": [], "summary": {...}} with
        # no "recommendations" key at all.
        page_result = {"issues": [_issue()], "summary": {"skipped": True}}
        merged = issues_to_persist(page_result)
        self.assertEqual(len(merged), 1)

    def test_empty_result_returns_empty_list(self):
        self.assertEqual(issues_to_persist({}), [])


class TestOpConstruction(unittest.TestCase):
    def test_single_bulk_call_ordered_false_filtered_on_dedup_key(self):
        collection = RecordingCollection()
        issues = [_issue(url="https://example.com/a"), _issue(url="https://example.com/b")]

        upsert_issues(issues, collection=collection)

        self.assertEqual(len(collection.calls), 1)  # batching preserved: one batch
        ops, ordered = collection.calls[0]
        self.assertEqual(ordered, False)  # ordering behavior preserved
        for op, issue in zip(ops, issues):
            self.assertEqual(op._filter, {"dedup_key": issue["dedup_key"]})
            self.assertTrue(op._upsert)

    def test_set_and_setoninsert_split_reunites_to_full_payload(self):
        # Existing payload preserved exactly: the union of $set and
        # $setOnInsert must equal the factory document, no field lost,
        # no overlap between the two.
        collection = RecordingCollection()
        issue = _issue(data_path="head.title")

        upsert_issues([issue], collection=collection)

        op = collection.calls[0][0][0]
        set_fields = op._doc["$set"]
        insert_only = op._doc["$setOnInsert"]
        self.assertEqual(set(set_fields) & set(insert_only), set())
        reunited = {**set_fields, **insert_only}
        self.assertEqual(reunited, issue)
        for field in LIFECYCLE_INSERT_ONLY_FIELDS:
            self.assertIn(field, insert_only)
            self.assertNotIn(field, set_fields)
        self.assertIn("dedup_key", insert_only)


class TestUpsertSemantics(unittest.TestCase):
    def test_new_issue_inserted_complete(self):
        collection = InMemoryCollection()
        issue = _issue()

        result = upsert_issues([issue], collection=collection)

        self.assertEqual(result.upserted_count, 1)
        self.assertEqual(collection.docs[issue["dedup_key"]], issue)

    def test_existing_issue_updated_not_duplicated(self):
        collection = InMemoryCollection()
        first = _issue(detected_value="OLD", seo_jobId="jobA")
        upsert_issues([first], collection=collection)

        second = _issue(detected_value="NEW", seo_jobId="jobB")
        upsert_issues([second], collection=collection)

        self.assertEqual(len(collection.docs), 1)  # duplicate no longer created
        stored = collection.docs[first["dedup_key"]]
        self.assertEqual(stored["detected_value"], "NEW")
        self.assertEqual(stored["seo_jobId"], "jobB")

    def test_lifecycle_state_preserved_on_reanalysis(self):
        # An existing issue with advanced lifecycle state (as P3-002 will
        # produce) must NOT be reset by a re-analysis upsert.
        collection = InMemoryCollection()
        existing = _issue()
        existing.update({"status": "verified_fixed", "fix_count": 2, "last_verified_at": datetime(2026, 7, 1)})
        collection.docs[existing["dedup_key"]] = copy.deepcopy(existing)

        fresh = _issue()  # factory output: status=open, fix_count=0
        upsert_issues([fresh], collection=collection)

        stored = collection.docs[existing["dedup_key"]]
        self.assertEqual(stored["status"], "verified_fixed")
        self.assertEqual(stored["fix_count"], 2)
        self.assertEqual(stored["last_verified_at"], datetime(2026, 7, 1))
        self.assertEqual(stored["detected_value"], "d")  # analysis payload refreshed

    def test_distinct_data_paths_produce_distinct_documents(self):
        # The rule-fix scenario: desktop + mobile title findings coexist.
        collection = InMemoryCollection()
        desktop = _issue(code="title_pixel_length", data_path="title_pixel_width.desktop")
        mobile = _issue(code="title_pixel_length", data_path="title_pixel_width.mobile")

        upsert_issues([desktop, mobile], collection=collection)

        self.assertEqual(len(collection.docs), 2)


class TestFailureHandling(unittest.TestCase):
    def test_duplicate_key_race_retried_once_with_losers_only(self):
        issues = [_issue(url=f"https://example.com/{i}") for i in range(3)]
        collection = RecordingCollection(errors=[_bulk_error([{"index": 1, "code": 11000}]), None])

        upsert_issues(issues, collection=collection)

        self.assertEqual(len(collection.calls), 2)
        retry_ops, retry_ordered = collection.calls[1]
        self.assertEqual(retry_ordered, False)
        self.assertEqual(len(retry_ops), 1)
        self.assertEqual(retry_ops[0]._filter, {"dedup_key": issues[1]["dedup_key"]})

    def test_non_duplicate_bulk_error_reraised_no_retry(self):
        collection = RecordingCollection(errors=[_bulk_error([{"index": 0, "code": 121}])])
        with self.assertRaises(BulkWriteError):
            upsert_issues([_issue()], collection=collection)
        self.assertEqual(len(collection.calls), 1)

    def test_retry_failure_propagates(self):
        collection = RecordingCollection(errors=[
            _bulk_error([{"index": 0, "code": 11000}]),
            _bulk_error([{"index": 0, "code": 11000}]),
        ])
        with self.assertRaises(BulkWriteError):
            upsert_issues([_issue()], collection=collection)
        self.assertEqual(len(collection.calls), 2)


# ── Live-Mongo integration (throwaway collection, auto-skip without Mongo) ──

def _live_db():
    try:
        from pymongo import MongoClient
        from env_config import get_config  # project-standard URI resolution
        uri = get_config().get("database.uri")
        if not uri:
            return None
        client = MongoClient(uri, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        return client["odito_p0005_test"]
    except Exception:
        return None


_LIVE = _live_db()


@unittest.skipUnless(_LIVE is not None, "local MongoDB not reachable")
class TestLiveIndexBehavior(unittest.TestCase):
    """Real unique-index semantics on a throwaway collection."""

    def setUp(self):
        self.coll = _LIVE["issues_index_test"]
        self.coll.drop()

    def tearDown(self):
        self.coll.drop()

    def test_unique_index_created_and_repeat_initialization_safe(self):
        self.coll.create_index([("dedup_key", 1)], unique=True, name="unique_dedup_key")
        # Repeated initialization (same spec) must not raise.
        self.coll.create_index([("dedup_key", 1)], unique=True, name="unique_dedup_key")
        indexes = self.coll.index_information()
        self.assertIn("unique_dedup_key", indexes)
        self.assertTrue(indexes["unique_dedup_key"].get("unique"))

    def test_duplicate_insert_rejected_by_index(self):
        from pymongo.errors import DuplicateKeyError
        self.coll.create_index([("dedup_key", 1)], unique=True, name="unique_dedup_key")
        self.coll.insert_one({"dedup_key": "k1", "v": 1})
        with self.assertRaises(DuplicateKeyError):
            self.coll.insert_one({"dedup_key": "k1", "v": 2})

    def test_index_creation_fails_loudly_with_preexisting_duplicates(self):
        from pymongo.errors import DuplicateKeyError, OperationFailure
        self.coll.insert_many([{"dedup_key": "k1"}, {"dedup_key": "k1"}])
        with self.assertRaises((DuplicateKeyError, OperationFailure)):
            self.coll.create_index([("dedup_key", 1)], unique=True, name="unique_dedup_key")

    def test_upsert_flow_end_to_end_against_real_index(self):
        self.coll.create_index([("dedup_key", 1)], unique=True, name="unique_dedup_key")
        issue = _issue()

        upsert_issues([issue], collection=self.coll)
        stored_first = self.coll.find_one({"dedup_key": issue["dedup_key"]})
        self.assertIsNotNone(stored_first)
        self.assertEqual(stored_first["status"], "open")

        changed = _issue(detected_value="CHANGED")
        upsert_issues([changed], collection=self.coll)

        self.assertEqual(self.coll.count_documents({}), 1)  # no duplicate created
        stored = self.coll.find_one({"dedup_key": issue["dedup_key"]})
        self.assertEqual(stored["detected_value"], "CHANGED")


if __name__ == "__main__":
    unittest.main()
