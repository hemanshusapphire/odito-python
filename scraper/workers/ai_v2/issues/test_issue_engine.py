"""Prerequisite B: AI Visibility domain issue lifecycle correctness.

clear_domain_issues_for_job() previously deleted domain-scoped ai_issues
scoped to {project_id, job_id, scope:"domain"} — only the CURRENT job's own
domain issues. Any run under a NEW job_id (a repeated Full Audit, or a
future single-URL verification run) left the previous run's domain issues
in place, so after Prerequisite A made aggregate_project() project-wide,
those stale domain issues would be double-counted forever.

The fix drops job_id from the delete filter: domain issue identity is
(project_id, rule_id) — one current issue per rule for the whole project.

Stdlib unittest only, fake in-memory ai_issues collection (same convention
as ../aggregation/test_project_aggregator.py and ../scoring/test_scorer.py).

Run from python_workers/:
    python -m unittest scraper.workers.ai_v2.issues.test_issue_engine -v
"""

import sys
import types
import unittest

from bson.objectid import ObjectId


def _stub_module(name, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        if not hasattr(module, key):
            setattr(module, key, value)


_stub_module("db", ai_issues=object())

import scraper.workers.ai_v2.issues.issue_engine as issue_engine


class FakeCollection:
    """Minimal in-memory stand-in for the subset of the pymongo API this
    module calls: delete_many, insert_many, find, count_documents."""

    def __init__(self):
        self.docs = []

    @staticmethod
    def _matches(doc, filt):
        return all(doc.get(k) == v for k, v in filt.items())

    def delete_many(self, filt):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not self._matches(d, filt)]
        return types.SimpleNamespace(deleted_count=before - len(self.docs))

    def insert_many(self, docs):
        self.docs.extend(docs)

    def find(self, filt=None):
        filt = filt or {}
        return [d for d in self.docs if self._matches(d, filt)]

    def count_documents(self, filt=None):
        return len(self.find(filt))


class FakeRuleResult:
    def __init__(self, rule_id, scope="page", result="FAIL"):
        self.rule_id = rule_id
        self.scope = scope
        self.result = result
        self.passed = result == "PASS"
        self.hub = "aiso"
        self.card = "c1"
        self.severity = "high"
        self.issue_title = f"{rule_id} title"
        self.issue_description = f"{rule_id} description"
        self.recommendation = f"{rule_id} recommendation"
        self.expected_impact = "medium"
        self.evidence = {}


PROJECT = ObjectId()


def _score_doc(url="https://a.com/1", score_id=None):
    return {"_id": score_id or ObjectId(), "url": url}


def _page_doc(page_id=None):
    return {"_id": page_id or ObjectId()}


class IssueEngineTestCase(unittest.TestCase):
    def setUp(self):
        self._orig_ai_issues = issue_engine.ai_issues
        self.ai_issues = FakeCollection()
        issue_engine.ai_issues = self.ai_issues

    def tearDown(self):
        issue_engine.ai_issues = self._orig_ai_issues

    def _run_job(self, project_id, job_id, page_results, domain_rule_fails):
        """Simulate one run_v2_pipeline invocation: clear domain issues once,
        then generate_and_save_issues per page, sharing one _domain_seen set
        — mirroring pipeline.py's exact call pattern."""
        issue_engine.clear_domain_issues_for_job(project_id, job_id)
        domain_seen = set()
        all_issues = []
        for score_doc, page_doc in page_results:
            rule_results = {}
            for rid in domain_rule_fails:
                rule_results[rid] = FakeRuleResult(rid, scope="domain", result="FAIL")
            issues = issue_engine.generate_and_save_issues(
                rule_results, score_doc, page_doc, project_id, job_id, domain_seen
            )
            all_issues.extend(issues)
        return all_issues


class TestCoreProof(IssueEngineTestCase):
    """Stale domain issues from a previous job_id must be removed, and
    project-wide issue counts must not double-count them."""

    def test_stale_domain_issues_from_a_previous_job_id_are_removed(self):
        job_a = ObjectId()
        self._run_job(PROJECT, job_a, [(_score_doc(), _page_doc())], ["AISO-D1"])
        self.assertEqual(self.ai_issues.count_documents({"scope": "domain"}), 1)

        job_b = ObjectId()
        self._run_job(PROJECT, job_b, [(_score_doc(), _page_doc())], ["AISO-D1"])

        # Only ONE current domain issue for this rule — not two (one per job).
        domain_docs = self.ai_issues.find({"project_id": PROJECT, "scope": "domain"})
        self.assertEqual(len(domain_docs), 1)
        self.assertEqual(domain_docs[0]["job_id"], job_b)

    def test_project_wide_issue_counts_do_not_double_count_across_repeated_full_audits(self):
        job_a = ObjectId()
        self._run_job(PROJECT, job_a, [(_score_doc(), _page_doc())], ["AISO-D1", "AEO-D2"])
        self.assertEqual(self.ai_issues.count_documents({"project_id": PROJECT, "scope": "domain"}), 2)

        job_b = ObjectId()
        self._run_job(PROJECT, job_b, [(_score_doc(), _page_doc())], ["AISO-D1", "AEO-D2"])

        # Still 2 — not 4 — after a second full run under a new job_id.
        self.assertEqual(self.ai_issues.count_documents({"project_id": PROJECT, "scope": "domain"}), 2)

    def test_no_duplicate_domain_issues_for_the_same_rule_within_one_run(self):
        job = ObjectId()
        pages = [(_score_doc("https://a.com/1"), _page_doc()), (_score_doc("https://a.com/2"), _page_doc())]
        self._run_job(PROJECT, job, pages, ["AISO-D1"])

        self.assertEqual(self.ai_issues.count_documents({"scope": "domain", "rule_id": "AISO-D1"}), 1)


class TestRegression(IssueEngineTestCase):
    def test_full_audit_domain_issues_generated_correctly_single_run(self):
        job = ObjectId()
        issues = self._run_job(PROJECT, job, [(_score_doc(), _page_doc())], ["AISO-D1"])
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["scope"], "domain")
        self.assertIsNone(issues[0]["score_id"])
        self.assertIsNone(issues[0]["page_id"])

    def test_domain_issues_regenerated_correctly_when_rule_still_fails(self):
        job_a = ObjectId()
        self._run_job(PROJECT, job_a, [(_score_doc(), _page_doc())], ["AISO-D1"])
        job_b = ObjectId()
        self._run_job(PROJECT, job_b, [(_score_doc(), _page_doc())], ["AISO-D1"])

        docs = self.ai_issues.find({"scope": "domain", "rule_id": "AISO-D1"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["job_id"], job_b)

    def test_domain_issue_cleared_when_rule_no_longer_fails(self):
        job_a = ObjectId()
        self._run_job(PROJECT, job_a, [(_score_doc(), _page_doc())], ["AISO-D1"])
        job_b = ObjectId()
        # Rule passes now — no domain fails this run.
        self._run_job(PROJECT, job_b, [(_score_doc(), _page_doc())], [])

        self.assertEqual(self.ai_issues.count_documents({"scope": "domain"}), 0)

    def test_page_scoped_issues_are_unaffected_by_domain_cleanup(self):
        job = ObjectId()
        issue_engine.clear_domain_issues_for_job(PROJECT, job)

        score_doc = _score_doc()
        page_doc = _page_doc()
        rule_results = {"PAGE-1": FakeRuleResult("PAGE-1", scope="page", result="FAIL")}
        issue_engine.generate_and_save_issues(rule_results, score_doc, page_doc, PROJECT, job, set())

        self.assertEqual(self.ai_issues.count_documents({"scope": "page"}), 1)

        # A later run's domain cleanup must not touch this page issue.
        issue_engine.clear_domain_issues_for_job(PROJECT, ObjectId())
        self.assertEqual(self.ai_issues.count_documents({"scope": "page"}), 1)

    def test_multiple_runs_same_job_id_idempotent(self):
        job = ObjectId()
        self._run_job(PROJECT, job, [(_score_doc(), _page_doc())], ["AISO-D1"])
        self._run_job(PROJECT, job, [(_score_doc(), _page_doc())], ["AISO-D1"])

        self.assertEqual(self.ai_issues.count_documents({"scope": "domain"}), 1)

    def test_repeated_full_audit_different_job_ids_no_leftovers(self):
        for _ in range(3):
            job = ObjectId()
            self._run_job(PROJECT, job, [(_score_doc(), _page_doc())], ["AISO-D1", "AEO-D2", "GEO-D3"])

        self.assertEqual(self.ai_issues.count_documents({"project_id": PROJECT, "scope": "domain"}), 3)

    def test_future_url_verification_style_run_a_second_job_id_still_clears_prior_domain_issues(self):
        # Simulates the shape of a future single-URL job: a new job_id,
        # touching only one page, still correctly clears/replaces the
        # project's domain issues (no orchestration or job-creation
        # behavior is exercised here — purely the lifecycle contract).
        job_a = ObjectId()
        self._run_job(PROJECT, job_a, [(_score_doc("https://a.com/1"), _page_doc()),
                                        (_score_doc("https://a.com/2"), _page_doc())], ["AISO-D1"])
        verification_job = ObjectId()
        self._run_job(PROJECT, verification_job, [(_score_doc("https://a.com/1"), _page_doc())], ["AISO-D1"])

        docs = self.ai_issues.find({"scope": "domain"})
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["job_id"], verification_job)

    def test_project_isolation_other_projects_domain_issues_untouched(self):
        other_project = ObjectId()
        job = ObjectId()
        self._run_job(other_project, job, [(_score_doc(), _page_doc())], ["AISO-D1"])

        issue_engine.clear_domain_issues_for_job(PROJECT, ObjectId())

        self.assertEqual(self.ai_issues.count_documents({"project_id": other_project, "scope": "domain"}), 1)


class TestFailureCases(IssueEngineTestCase):
    def test_no_domain_rule_fails_leaves_zero_domain_issues(self):
        job = ObjectId()
        issues = self._run_job(PROJECT, job, [(_score_doc(), _page_doc())], [])
        self.assertEqual(issues, [])
        self.assertEqual(self.ai_issues.count_documents({"scope": "domain"}), 0)


if __name__ == "__main__":
    unittest.main()
