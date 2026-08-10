"""P3-002 tests: reconcile_issue_lifecycle (OPEN/RESOLVED/REOPENED transitions).

Stdlib unittest only, same conventions as test_issue_identity.py and
test_page_analysis_upsert.py. In-memory fake collection for fast/offline
coverage; a live-Mongo suite (auto-skip without local Mongo) proves real
$in/$nin/$inc semantics against the actual driver.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.rules.test_issue_lifecycle_reconciliation -v
"""

import unittest
from datetime import datetime

from bson.objectid import ObjectId

from scraper.workers.seo.page_analysis.rules.issue_identity import (
    compute_dedup_key,
    reconcile_issue_lifecycle,
)

PROJECT_ID = "64b7f3a2c9e77a0012345678"
URL = "https://example.com/pricing"


def _key(code, data_path=None):
    return compute_dedup_key(PROJECT_ID, URL, code, data_path)


class FakeResult:
    def __init__(self, modified_count):
        self.modified_count = modified_count


class InMemoryIssuesCollection:
    """Enough update_many semantics ($set/$inc, $in/$nin on dedup_key) to
    exercise reconcile_issue_lifecycle without a real Mongo connection."""

    def __init__(self, docs):
        # keyed by dedup_key for convenience; each doc keeps its own fields.
        self.docs = {d["dedup_key"]: dict(d) for d in docs}

    def update_many(self, filt, update):
        matched = [
            d for d in self.docs.values()
            if d.get("projectId") == filt["projectId"]
            and d.get("page_url") == filt["page_url"]
            and d.get("status") == filt["status"]
            and self._dedup_matches(d, filt.get("dedup_key"))
        ]
        for doc in matched:
            doc.update(update.get("$set", {}))
            for field, amount in update.get("$inc", {}).items():
                doc[field] = doc.get(field, 0) + amount
        return FakeResult(len(matched))

    @staticmethod
    def _dedup_matches(doc, clause):
        if clause is None:
            return True
        if "$in" in clause:
            return doc["dedup_key"] in clause["$in"]
        if "$nin" in clause:
            return doc["dedup_key"] not in clause["$nin"]
        return True


def _doc(code, status, data_path=None, **extra):
    doc = {
        "projectId": ObjectId(PROJECT_ID),
        "page_url": URL,
        "issue_code": code,
        "dedup_key": _key(code, data_path),
        "status": status,
        "first_detected_at": datetime(2026, 1, 1),
        "last_verified_at": None,
        "fix_count": 0,
        "regression_count": 0,
    }
    doc.update(extra)
    return doc


class TestStillOpen(unittest.TestCase):
    def test_open_issue_still_detected_stays_open_and_refreshes_timestamp(self):
        collection = InMemoryIssuesCollection([_doc("TITLE_MISSING", "open")])
        now = datetime(2026, 7, 29, 12, 0, 0)

        outcome = reconcile_issue_lifecycle(
            collection, PROJECT_ID, URL, {_key("TITLE_MISSING")}, now
        )

        stored = collection.docs[_key("TITLE_MISSING")]
        self.assertEqual(stored["status"], "open")
        self.assertEqual(stored["last_verified_at"], now)
        self.assertEqual(stored["fix_count"], 0)
        self.assertEqual(stored["regression_count"], 0)
        self.assertEqual(outcome["still_open_refreshed"], 1)
        self.assertEqual(outcome["resolved"], 0)
        self.assertEqual(outcome["reopened"], 0)


class TestResolve(unittest.TestCase):
    def test_open_issue_not_detected_becomes_resolved(self):
        collection = InMemoryIssuesCollection([_doc("META_DESCRIPTION_MISSING", "open")])
        now = datetime(2026, 7, 29, 12, 0, 0)

        outcome = reconcile_issue_lifecycle(collection, PROJECT_ID, URL, set(), now)

        stored = collection.docs[_key("META_DESCRIPTION_MISSING")]
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_at"], now)
        self.assertEqual(stored["last_verified_at"], now)
        self.assertEqual(stored["fix_count"], 1)
        self.assertEqual(stored["regression_count"], 0)
        self.assertEqual(outcome["resolved"], 1)

    def test_only_issues_absent_from_current_run_resolve_others_stay_open(self):
        collection = InMemoryIssuesCollection([
            _doc("TITLE_MISSING", "open"),
            _doc("META_DESCRIPTION_MISSING", "open"),
        ])
        now = datetime(2026, 7, 29, 12, 0, 0)

        # Only TITLE_MISSING detected this run — META_DESCRIPTION_MISSING resolves.
        reconcile_issue_lifecycle(collection, PROJECT_ID, URL, {_key("TITLE_MISSING")}, now)

        self.assertEqual(collection.docs[_key("TITLE_MISSING")]["status"], "open")
        self.assertEqual(collection.docs[_key("META_DESCRIPTION_MISSING")]["status"], "resolved")


class TestReopen(unittest.TestCase):
    def test_resolved_issue_detected_again_becomes_reopened(self):
        collection = InMemoryIssuesCollection([
            _doc("META_DESCRIPTION_MISSING", "resolved", resolved_at=datetime(2026, 7, 1), fix_count=1)
        ])
        now = datetime(2026, 7, 29, 12, 0, 0)

        outcome = reconcile_issue_lifecycle(
            collection, PROJECT_ID, URL, {_key("META_DESCRIPTION_MISSING")}, now
        )

        stored = collection.docs[_key("META_DESCRIPTION_MISSING")]
        self.assertEqual(stored["status"], "open")
        self.assertEqual(stored["regression_count"], 1)
        self.assertEqual(stored["last_verified_at"], now)
        self.assertEqual(stored["fix_count"], 1)  # unchanged by a reopen
        # resolved_at is left as the record of the last resolution.
        self.assertEqual(stored["resolved_at"], datetime(2026, 7, 1))
        self.assertEqual(outcome["reopened"], 1)

    def test_full_cycle_open_resolved_reopened_resolved_again(self):
        collection = InMemoryIssuesCollection([_doc("TITLE_MISSING", "open")])
        key = _key("TITLE_MISSING")
        t1, t2, t3 = datetime(2026, 7, 1), datetime(2026, 7, 10), datetime(2026, 7, 20)

        reconcile_issue_lifecycle(collection, PROJECT_ID, URL, set(), t1)  # -> resolved
        self.assertEqual(collection.docs[key]["status"], "resolved")
        self.assertEqual(collection.docs[key]["fix_count"], 1)

        reconcile_issue_lifecycle(collection, PROJECT_ID, URL, {key}, t2)  # -> reopened
        self.assertEqual(collection.docs[key]["status"], "open")
        self.assertEqual(collection.docs[key]["regression_count"], 1)

        reconcile_issue_lifecycle(collection, PROJECT_ID, URL, set(), t3)  # -> resolved again
        self.assertEqual(collection.docs[key]["status"], "resolved")
        self.assertEqual(collection.docs[key]["fix_count"], 2)
        self.assertEqual(collection.docs[key]["resolved_at"], t3)


class TestNeverDeletes(unittest.TestCase):
    def test_history_fields_never_touched_by_any_transition(self):
        first_detected = datetime(2026, 1, 1)
        collection = InMemoryIssuesCollection([_doc("TITLE_MISSING", "open", first_detected_at=first_detected)])
        now = datetime(2026, 7, 29)

        reconcile_issue_lifecycle(collection, PROJECT_ID, URL, set(), now)

        self.assertEqual(collection.docs[_key("TITLE_MISSING")]["first_detected_at"], first_detected)
        self.assertEqual(len(collection.docs), 1)  # no document ever removed


class TestEmptyCurrentSet(unittest.TestCase):
    def test_zero_issues_detected_resolves_every_open_issue_on_the_page(self):
        collection = InMemoryIssuesCollection([
            _doc("TITLE_MISSING", "open"),
            _doc("META_DESCRIPTION_MISSING", "open"),
            _doc("H1_MISSING", "resolved"),
        ])
        now = datetime(2026, 7, 29)

        outcome = reconcile_issue_lifecycle(collection, PROJECT_ID, URL, set(), now)

        self.assertEqual(collection.docs[_key("TITLE_MISSING")]["status"], "resolved")
        self.assertEqual(collection.docs[_key("META_DESCRIPTION_MISSING")]["status"], "resolved")
        self.assertEqual(collection.docs[_key("H1_MISSING")]["status"], "resolved")  # untouched, stays resolved
        self.assertEqual(outcome["resolved"], 2)
        self.assertEqual(outcome["reopened"], 0)


# ── Live-Mongo integration (throwaway collection, auto-skip without Mongo) ──

def _live_db():
    try:
        from pymongo import MongoClient
        from env_config import get_config
        uri = get_config().get("database.uri")
        if not uri:
            return None
        client = MongoClient(uri, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        return client["odito_p3002_test"]
    except Exception:
        return None


_LIVE = _live_db()


@unittest.skipUnless(_LIVE is not None, "local MongoDB not reachable")
class TestLiveReconciliation(unittest.TestCase):
    def setUp(self):
        self.coll = _LIVE["issues_lifecycle_test"]
        self.coll.drop()

    def tearDown(self):
        self.coll.drop()

    def test_end_to_end_open_resolve_reopen_against_real_mongo(self):
        key = _key("TITLE_MISSING")
        self.coll.insert_one(_doc("TITLE_MISSING", "open"))

        now1 = datetime(2026, 7, 1)
        reconcile_issue_lifecycle(self.coll, PROJECT_ID, URL, set(), now1)
        stored = self.coll.find_one({"dedup_key": key})
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["fix_count"], 1)

        now2 = datetime(2026, 7, 15)
        reconcile_issue_lifecycle(self.coll, PROJECT_ID, URL, {key}, now2)
        stored = self.coll.find_one({"dedup_key": key})
        self.assertEqual(stored["status"], "open")
        self.assertEqual(stored["regression_count"], 1)

        self.assertEqual(self.coll.count_documents({}), 1)  # never duplicated/deleted


if __name__ == "__main__":
    unittest.main()
