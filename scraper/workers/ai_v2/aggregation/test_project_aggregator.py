"""AI_VISIBILITY aggregation correctness prerequisite: tests for
aggregate_project()'s move from job_id-scoped reads to project_id-only reads.

Root cause under test: ai_scores/ai_issues previously kept every page's
CURRENT state per job_id, and aggregate_project() read `{project_id, job_id}`.
A future single-URL verification run would create a new job_id whose
one-page aggregate would win the "most recent" read on every Node-side
consumer (they all query ai_projects by project_id, sorted by computed_at
desc — verified across aiHubController.js, AuditHistoryService.js,
AiHubSnapshotService.js, V2IssueExtractor.js, contextExtractor.js).

Stdlib unittest only, fake in-memory Mongo-like collections (no real DB
needed for these — index correctness itself is verified separately, live,
against odito_dev, since a fake collection can't prove an index spec).

Run from python_workers/:
    python -m unittest scraper.workers.ai_v2.aggregation.test_project_aggregator -v
"""

import sys
import types
import unittest
from datetime import datetime, timezone


def _stub_module(name, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        if not hasattr(module, key):
            setattr(module, key, value)


_stub_module("db", ai_scores=object(), ai_issues=object(), ai_projects=object())

import scraper.workers.ai_v2.aggregation.project_aggregator as project_aggregator


class FakeCollection:
    """Minimal in-memory stand-in for the subset of the pymongo API this
    module actually calls: find, find_one, find_one_and_update ($set +
    upsert), count_documents. Matching is flat-key equality, which is all
    aggregate_project()'s filters ever use."""

    def __init__(self):
        self.docs = []
        self._next_id = 1

    @staticmethod
    def _matches(doc, filt):
        return all(doc.get(k) == v for k, v in filt.items())

    def find(self, filt=None):
        filt = filt or {}
        return [d for d in self.docs if self._matches(d, filt)]

    def find_one(self, filt=None):
        filt = filt or {}
        for d in self.docs:
            if self._matches(d, filt):
                return d
        return None

    def count_documents(self, filt=None):
        return len(self.find(filt))

    def find_one_and_update(self, filter, update, upsert=False, return_document=None):
        for d in self.docs:
            if self._matches(d, filter):
                d.update(update.get("$set", {}))
                return d
        if upsert:
            new_doc = dict(filter)
            new_doc.update(update.get("$set", {}))
            new_doc["_id"] = self._next_id
            self._next_id += 1
            self.docs.append(new_doc)
            return new_doc
        return None


PROJECT = "p1"
OTHER_PROJECT = "p2"


def _score_doc(project_id, job_id, url, aiso=100, aeo=100, geo=100):
    return {
        "project_id": project_id,
        "job_id": job_id,
        "url": url,
        "hubs": {
            "aiso": {"cards": {"c1": {"passed": aiso, "total": 100}}},
            "aeo":  {"cards": {"c1": {"passed": aeo,  "total": 100}}},
            "geo":  {"cards": {"c1": {"passed": geo,  "total": 100}}},
        },
    }


class AggregatorTestCase(unittest.TestCase):
    def setUp(self):
        self._orig_scores = project_aggregator.ai_scores
        self._orig_issues = project_aggregator.ai_issues
        self._orig_projects = project_aggregator.ai_projects
        self._orig_hub_card_map = project_aggregator.HUB_CARD_MAP

        self.ai_scores = FakeCollection()
        self.ai_issues = FakeCollection()
        self.ai_projects = FakeCollection()

        project_aggregator.ai_scores = self.ai_scores
        project_aggregator.ai_issues = self.ai_issues
        project_aggregator.ai_projects = self.ai_projects
        project_aggregator.HUB_CARD_MAP = {"aiso": ["c1"], "aeo": ["c1"], "geo": ["c1"]}

        # aggregate_project ObjectId-coerces its inputs; feed it real ObjectIds
        # so plain string project/job ids used for readability elsewhere in
        # this file don't break that coercion.
        from bson import ObjectId
        self.ObjectId = ObjectId

    def tearDown(self):
        project_aggregator.ai_scores = self._orig_scores
        project_aggregator.ai_issues = self._orig_issues
        project_aggregator.ai_projects = self._orig_projects
        project_aggregator.HUB_CARD_MAP = self._orig_hub_card_map


class TestCoreProof(AggregatorTestCase):
    """The exact bug the user described: single-URL verification cannot
    overwrite the project-wide aggregate with a one-page result."""

    def test_single_page_verification_cannot_overwrite_project_wide_aggregate(self):
        pid = self.ObjectId()
        full_audit_job = self.ObjectId()

        # Full Audit: 3 pages scored under one job_id.
        for url in ("https://a.com/1", "https://a.com/2", "https://a.com/3"):
            self.ai_scores.docs.append(_score_doc(pid, full_audit_job, url))
        project_aggregator.aggregate_project(pid, full_audit_job)

        project_doc = self.ai_projects.find_one({"project_id": pid})
        self.assertEqual(project_doc["pages_scored"], 3)

        # Later: a single-URL verification run re-scores ONE of those pages
        # under a brand-new job_id (this is what score_and_save's fixed
        # upsert filter does — updates the SAME doc in place, not a new one).
        verification_job = self.ObjectId()
        for d in self.ai_scores.docs:
            if d["url"] == "https://a.com/1":
                d["job_id"] = verification_job

        project_doc = project_aggregator.aggregate_project(pid, verification_job)

        # The aggregate must still reflect all 3 project pages, not 1.
        self.assertEqual(project_doc["pages_scored"], 3)

    def test_only_one_ai_projects_doc_exists_per_project_after_a_second_job(self):
        pid = self.ObjectId()
        job_a = self.ObjectId()
        job_b = self.ObjectId()

        self.ai_scores.docs.append(_score_doc(pid, job_a, "https://a.com/1"))
        project_aggregator.aggregate_project(pid, job_a)
        project_aggregator.aggregate_project(pid, job_b)

        self.assertEqual(len(self.ai_projects.find({"project_id": pid})), 1)


class TestRegression(AggregatorTestCase):
    def test_full_audit_all_pages_counted_same_job_id(self):
        pid = self.ObjectId()
        job = self.ObjectId()
        for url in ("https://a.com/1", "https://a.com/2"):
            self.ai_scores.docs.append(_score_doc(pid, job, url))

        result = project_aggregator.aggregate_project(pid, job)
        self.assertEqual(result["pages_scored"], 2)

    def test_historical_aggregation_across_pages_scored_under_different_past_job_ids_still_counted(self):
        # Before this fix, ai_scores accumulated one doc per (project, job,
        # url) forever; a project's pages could legitimately have been
        # scored under different job_ids over time (retries, partial runs).
        # project_id-only aggregation must still see all of them.
        pid = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, self.ObjectId(), "https://a.com/1"))
        self.ai_scores.docs.append(_score_doc(pid, self.ObjectId(), "https://a.com/2"))

        result = project_aggregator.aggregate_project(pid, self.ObjectId())
        self.assertEqual(result["pages_scored"], 2)

    def test_multiple_runs_of_the_same_job_id_are_idempotent(self):
        pid = self.ObjectId()
        job = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, job, "https://a.com/1"))

        first = project_aggregator.aggregate_project(pid, job)
        second = project_aggregator.aggregate_project(pid, job)

        self.assertEqual(first["pages_scored"], second["pages_scored"])
        self.assertEqual(len(self.ai_projects.docs), 1)

    def test_score_math_unchanged(self):
        pid = self.ObjectId()
        job = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, job, "https://a.com/1", aiso=50, aeo=100, geo=0))

        result = project_aggregator.aggregate_project(pid, job)
        self.assertEqual(result["hubs"]["aiso"]["score"], 50.0)
        self.assertEqual(result["hubs"]["aeo"]["score"], 100.0)
        self.assertEqual(result["hubs"]["geo"]["score"], 0.0)
        self.assertEqual(result["overall_score"], 50.0)

    def test_issue_severity_summary_is_project_scoped_not_job_scoped(self):
        pid = self.ObjectId()
        old_job = self.ObjectId()
        new_job = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, new_job, "https://a.com/1"))
        # Issues generated under an OLDER job_id for this same project must
        # still count — issues_summary is a project-wide current count.
        self.ai_issues.docs.append({"project_id": pid, "job_id": old_job, "severity": "critical"})
        self.ai_issues.docs.append({"project_id": pid, "job_id": new_job, "severity": "high"})

        result = project_aggregator.aggregate_project(pid, new_job)
        self.assertEqual(result["issues_summary"]["critical"], 1)
        self.assertEqual(result["issues_summary"]["high"], 1)
        self.assertEqual(result["issues_summary"]["total"], 2)

    def test_project_isolation_other_projects_scores_never_leak_in(self):
        pid = self.ObjectId()
        other_pid = self.ObjectId()
        job = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, job, "https://a.com/1"))
        self.ai_scores.docs.append(_score_doc(other_pid, job, "https://b.com/1"))

        result = project_aggregator.aggregate_project(pid, job)
        self.assertEqual(result["pages_scored"], 1)

    def test_job_id_still_recorded_on_output_doc_as_provenance(self):
        pid = self.ObjectId()
        job = self.ObjectId()
        self.ai_scores.docs.append(_score_doc(pid, job, "https://a.com/1"))

        result = project_aggregator.aggregate_project(pid, job)
        self.assertEqual(result["job_id"], job)


class TestFailureCases(AggregatorTestCase):
    def test_zero_pages_for_project_returns_zero_scored_not_an_error(self):
        pid = self.ObjectId()
        result = project_aggregator.aggregate_project(pid, self.ObjectId())
        self.assertEqual(result["pages_scored"], 0)
        self.assertEqual(result["overall_score"], 0)


if __name__ == "__main__":
    unittest.main()
