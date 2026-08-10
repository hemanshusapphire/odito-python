"""P0-004 tests: seo_page_issues lifecycle backfill migration.

Stdlib unittest only (same approach as P0-001/002/003). The migration's
db import is lazy (inside main()), so run_migration is tested directly with
an injected in-memory fake collection — no Mongo, no network, no stubbing.

Run from python_workers/:
    python -m unittest scripts.test_backfill_seo_page_issues_lifecycle -v
"""

import copy
import unittest
from datetime import datetime
from unittest import mock

from scraper.workers.seo.page_analysis.rules import issue_identity
from scripts import backfill_seo_page_issues_lifecycle as migration


PROJECT_ID = "64b7f3a2c9e77a0012345678"


def _legacy_issue(_id, url="https://example.com/a", code="TITLE_MISSING",
                  data_path=None, **extra):
    """A pre-P0-003 document: none of the six lifecycle fields."""
    doc = {
        "_id": _id,
        "projectId": PROJECT_ID,
        "seo_jobId": "64b7f3a2c9e77a0087654321",
        "page_url": url,
        "rule_no": 1,
        "category": "Content",
        "severity": "high",
        "issue_code": code,
        "rule_id": code,
        "issue_message": "msg",
        "detected_value": "",
        "expected_value": "e",
        "data_key": "title",
        "data_path": data_path,
        "created_at": datetime(2026, 1, 15, 12, 0, 0),
    }
    doc.update(extra)
    return doc


def _compliant_issue(_id, **overrides):
    """A P0-003-era document: all six lifecycle fields present."""
    doc = _legacy_issue(_id, **overrides)
    doc.update({
        "status": "open",
        "dedup_key": issue_identity.compute_dedup_key(
            doc["projectId"], doc["page_url"], doc["issue_code"], doc["data_path"]
        ),
        "first_detected_at": doc["created_at"],
        "last_verified_at": None,
        "fix_count": 0,
        "regression_count": 0,
    })
    return doc


class FakeBulkResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class FakeCollection:
    """In-memory seo_page_issues implementing exactly the operations the
    migration uses: count_documents, find(missing-filter), bulk_write of
    UpdateOne($set, dedup_key-$exists:false guard), and the fixed duplicate-
    detection aggregate pipeline."""

    def __init__(self, docs, fail_on_bulk_write_call=None):
        self.docs = {doc["_id"]: copy.deepcopy(doc) for doc in docs}
        self.bulk_write_calls = 0
        self.writes_per_id = {}
        self._fail_on_call = fail_on_bulk_write_call

    def _matches_missing(self, doc):
        return "dedup_key" not in doc

    def count_documents(self, query):
        if not query:
            return len(self.docs)
        if query == migration.MISSING_FILTER:
            return sum(1 for doc in self.docs.values() if self._matches_missing(doc))
        raise AssertionError(f"unexpected count_documents query: {query}")

    def find(self, query, projection=None):
        assert query == migration.MISSING_FILTER
        matching = [doc for doc in self.docs.values() if self._matches_missing(doc)]
        if projection is None:
            return [copy.deepcopy(doc) for doc in matching]
        return [
            {key: copy.deepcopy(doc[key]) for key in projection if key in doc}
            for doc in matching
        ]

    def bulk_write(self, ops, ordered=True):
        self.bulk_write_calls += 1
        if self._fail_on_call is not None and self.bulk_write_calls == self._fail_on_call:
            raise ConnectionError("simulated interruption mid-migration")
        modified = 0
        for op in ops:
            doc = self.docs.get(op._filter["_id"])
            if doc is None or "dedup_key" in doc:
                continue  # $exists:false guard — no-op
            doc.update(op._doc["$set"])
            modified += 1
            self.writes_per_id[doc["_id"]] = self.writes_per_id.get(doc["_id"], 0) + 1
        return FakeBulkResult(modified)

    def aggregate(self, pipeline, allowDiskUse=False):
        groups = {}
        for doc in self.docs.values():
            key = (
                doc.get("projectId"),
                doc.get("page_url"),
                doc.get("issue_code"),
                doc.get("data_path") if doc.get("data_path") is not None else "",
            )
            groups.setdefault(key, []).append(doc["_id"])
        for (project_id, page_url, issue_code, data_path), ids in groups.items():
            if len(ids) > 1:
                yield {
                    "_id": {
                        "projectId": project_id,
                        "page_url": page_url,
                        "issue_code": issue_code,
                        "data_path": data_path,
                    },
                    "count": len(ids),
                    "ids": ids,
                }


class TestDryRun(unittest.TestCase):
    def test_dry_run_makes_zero_writes_and_reports_counts(self):
        collection = FakeCollection([
            _legacy_issue("a"), _legacy_issue("b", url="https://example.com/b"),
            _compliant_issue("c", url="https://example.com/c"),
        ])
        before = copy.deepcopy(collection.docs)

        summary = migration.run_migration(collection, apply=False)

        self.assertEqual(collection.bulk_write_calls, 0)
        self.assertEqual(collection.docs, before)
        self.assertEqual(summary["mode"], "DRY_RUN")
        self.assertEqual(summary["documents_scanned"], 3)
        self.assertEqual(summary["documents_requiring_update"], 2)
        self.assertEqual(summary["documents_already_compliant"], 1)
        self.assertEqual(summary["documents_updated"], 0)

    def test_duplicate_report_correct_including_none_vs_empty_data_path(self):
        collection = FakeCollection([
            # Three docs collapsing to ONE identity group: data_path None
            # and "" are hash-equivalent, matching the dedup_key contract.
            _legacy_issue("a", url="https://example.com/x", code="TITLE_MISSING", data_path=None),
            _legacy_issue("b", url="https://example.com/x", code="TITLE_MISSING", data_path=""),
            _compliant_issue("c", url="https://example.com/x", code="TITLE_MISSING", data_path=None),
            # Distinct identity — not a duplicate.
            _legacy_issue("d", url="https://example.com/y", code="TITLE_MISSING"),
        ])

        summary = migration.run_migration(collection, apply=False)

        duplicates = summary["duplicates"]
        self.assertEqual(duplicates["duplicate_group_count"], 1)
        self.assertEqual(duplicates["duplicate_document_count"], 3)
        self.assertEqual(len(duplicates["sample_groups"]), 1)
        sample = duplicates["sample_groups"][0]
        self.assertEqual(sample["count"], 3)
        self.assertEqual(set(sample["ids"]), {"a", "b", "c"})


class TestApplyMode(unittest.TestCase):
    def test_apply_updates_missing_fields_with_helper_computed_key(self):
        doc = _legacy_issue("a", data_path="images[2]")
        collection = FakeCollection([doc])

        summary = migration.run_migration(collection, apply=True)

        updated = collection.docs["a"]
        for field in migration.LIFECYCLE_FIELDS:
            self.assertIn(field, updated)
        self.assertEqual(updated["status"], "open")
        self.assertEqual(updated["fix_count"], 0)
        self.assertEqual(updated["regression_count"], 0)
        self.assertIsNone(updated["last_verified_at"])
        # first_detected_at mirrors the original created_at, not "now".
        self.assertEqual(updated["first_detected_at"], datetime(2026, 1, 15, 12, 0, 0))
        # dedup_key byte-identical to what the P0-003 factory would produce.
        self.assertEqual(
            updated["dedup_key"],
            issue_identity.compute_dedup_key(PROJECT_ID, doc["page_url"], "TITLE_MISSING", "images[2]"),
        )
        self.assertEqual(summary["documents_updated"], 1)

    def test_populated_fields_never_overwritten(self):
        # Pathological partial doc: has a populated status (say a manual
        # data fix set it to "reopened") but no dedup_key. The migration
        # must fill ONLY the missing fields and leave status untouched.
        doc = _legacy_issue("a", status="reopened", fix_count=3)
        collection = FakeCollection([doc])

        migration.run_migration(collection, apply=True)

        updated = collection.docs["a"]
        self.assertEqual(updated["status"], "reopened")
        self.assertEqual(updated["fix_count"], 3)
        self.assertIn("dedup_key", updated)
        self.assertIn("first_detected_at", updated)

    def test_compliant_documents_not_touched(self):
        compliant = _compliant_issue("c")
        collection = FakeCollection([compliant])
        before = copy.deepcopy(collection.docs)

        summary = migration.run_migration(collection, apply=True)

        self.assertEqual(collection.docs, before)
        self.assertEqual(summary["documents_requiring_update"], 0)
        self.assertEqual(summary["documents_updated"], 0)
        self.assertEqual(collection.bulk_write_calls, 0)

    def test_idempotent_second_run_zero_writes(self):
        collection = FakeCollection([_legacy_issue("a"), _legacy_issue("b", url="https://example.com/b")])

        first = migration.run_migration(collection, apply=True)
        second = migration.run_migration(collection, apply=True)

        self.assertEqual(first["documents_updated"], 2)
        self.assertEqual(second["documents_requiring_update"], 0)
        self.assertEqual(second["documents_updated"], 0)
        # No document was ever written more than once across both runs.
        self.assertTrue(all(count == 1 for count in collection.writes_per_id.values()))

    def test_batching_flushes_at_batch_size(self):
        docs = [_legacy_issue(str(i), url=f"https://example.com/{i}") for i in range(5)]
        collection = FakeCollection(docs)

        summary = migration.run_migration(collection, apply=True, batch_size=2)

        self.assertEqual(collection.bulk_write_calls, 3)  # 2 + 2 + 1
        self.assertEqual(summary["batches_executed"], 3)
        self.assertEqual(summary["documents_updated"], 5)

    def test_interrupted_migration_resumes_without_double_processing(self):
        docs = [_legacy_issue(str(i), url=f"https://example.com/{i}") for i in range(4)]
        collection = FakeCollection(docs, fail_on_bulk_write_call=2)

        with self.assertRaises(ConnectionError):
            migration.run_migration(collection, apply=True, batch_size=1)

        # First batch landed before the crash; rest untouched.
        applied_after_crash = sum(
            1 for doc in collection.docs.values() if "dedup_key" in doc
        )
        self.assertEqual(applied_after_crash, 1)

        # Fresh run (no injected failure): field-existence filter selects
        # only the 3 unprocessed docs — no progress file needed.
        collection._fail_on_call = None
        resume_summary = migration.run_migration(collection, apply=True, batch_size=1)

        self.assertEqual(resume_summary["documents_requiring_update"], 3)
        self.assertEqual(resume_summary["documents_updated"], 3)
        self.assertTrue(all(count == 1 for count in collection.writes_per_id.values()))

    def test_invalid_documents_skipped_and_reported(self):
        invalid = _legacy_issue("bad")
        del invalid["page_url"]  # no identity → compute_dedup_key raises
        collection = FakeCollection([invalid, _legacy_issue("good")])

        summary = migration.run_migration(collection, apply=True)

        self.assertEqual(summary["documents_invalid_skipped"], 1)
        self.assertEqual(summary["invalid_sample_ids"], ["bad"])
        self.assertEqual(summary["documents_updated"], 1)
        self.assertNotIn("dedup_key", collection.docs["bad"])
        self.assertIn("dedup_key", collection.docs["good"])


class TestHelperReuse(unittest.TestCase):
    def test_hash_helper_is_imported_not_reimplemented(self):
        # The migration's compute_dedup_key must be the exact P0-003
        # function object — identity check, not just equal output.
        self.assertIs(migration.compute_dedup_key, issue_identity.compute_dedup_key)

    def test_migration_delegates_hashing_to_the_shared_helper(self):
        collection = FakeCollection([_legacy_issue("a")])
        with mock.patch.object(
            migration, "compute_dedup_key", wraps=issue_identity.compute_dedup_key
        ) as spied:
            migration.run_migration(collection, apply=True)
        spied.assert_called_once_with(PROJECT_ID, "https://example.com/a", "TITLE_MISSING", None)


if __name__ == "__main__":
    unittest.main()
