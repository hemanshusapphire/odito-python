"""P3-002 tests: execute_page_analysis_logic's wiring of issue lifecycle
reconciliation — which pages get reconciled, with which dedup_keys, and
which ones are safely excluded.

reconcile_issue_lifecycle itself (the transition logic) is tested in
rules/test_issue_lifecycle_reconciliation.py; this file tests only the
integration: successfully_analyzed_pages tracking, dedup_keys_by_page
grouping, and the "a page whose analysis threw must never be reconciled"
safety property.

Reuses the exact FakeCollection/FakeDb/PageAnalysisTestCase stubbing pattern
established in test_page_analysis_url_filter.py — FakeCollection already
implements update_many(), recording every call, which is exactly what
reconcile_issue_lifecycle needs.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.test_page_analysis_lifecycle_wiring -v
"""

import sys
import types
import unittest
from datetime import datetime

from bson.objectid import ObjectId


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

import scraper.workers.seo.page_analysis.page_analysis as page_analysis
from api.analysis import PageAnalysisJob
from scraper.workers.seo.page_analysis.rules.issue_identity import compute_dedup_key


PROJECT_ID = str(ObjectId())
JOB_ID = str(ObjectId())
SOURCE_JOB_ID = str(ObjectId())


class FakeCollection:
    def __init__(self, find_result=None, find_one_result=None):
        self.find_calls = []
        self._find_result = list(find_result or [])
        self._find_one_result = find_one_result
        self.inserted = []
        self.update_calls = []

    def find(self, query=None, *args, **kwargs):
        self.find_calls.append(query)
        return list(self._find_result)

    def find_one(self, query=None, *args, **kwargs):
        return self._find_one_result

    def insert_many(self, docs, ordered=True):
        self.inserted.extend(docs)
        class Result:
            inserted_ids = list(range(len(docs)))
        return Result()

    def insert_one(self, doc):
        self.inserted.append(doc)
        class Result:
            inserted_id = ObjectId()
        return Result()

    def update_many(self, filt, update):
        self.update_calls.append((filt, update))
        class Result:
            matched_count = 0
            modified_count = 0
        return Result()


class FakeDb:
    def __init__(self):
        self.domain_technical_report = FakeCollection(find_one_result={})
        self.seo_headless_data = FakeCollection(find_result=[])
        self.seo_crawl_graph = FakeCollection(find_result=[])
        self.jobs = FakeCollection(find_one_result=None)


def _page(url, **overrides):
    doc = {"url": url, "projectId": ObjectId(PROJECT_ID), "scrape_status": "SUCCESS"}
    doc.update(overrides)
    return doc


class LifecycleWiringTestCase(unittest.TestCase):
    PATCHED_NAMES = [
        "seo_page_data", "seo_page_performance", "seo_page_summary",
        "seo_internal_links", "seo_page_issues", "db", "analyze_page_seo",
        "is_job_cancelled", "send_progress_update", "send_completion_callback",
        "send_failure_callback", "send_crawl_summary", "upsert_issues",
    ]

    def setUp(self):
        self._originals = {name: getattr(page_analysis, name) for name in self.PATCHED_NAMES}

        self.seo_page_data = FakeCollection()
        self.seo_page_performance = FakeCollection(find_result=[])
        self.seo_page_summary = FakeCollection()
        self.seo_internal_links = FakeCollection()
        self.seo_page_issues = FakeCollection()
        self.fake_db = FakeDb()

        page_analysis.seo_page_data = self.seo_page_data
        page_analysis.seo_page_performance = self.seo_page_performance
        page_analysis.seo_page_summary = self.seo_page_summary
        page_analysis.seo_internal_links = self.seo_internal_links
        page_analysis.seo_page_issues = self.seo_page_issues
        page_analysis.db = self.fake_db
        page_analysis.is_job_cancelled = lambda job_id: False
        page_analysis.send_progress_update = lambda *a, **k: None
        page_analysis.send_completion_callback = lambda job_id, stats, result_data=None: None
        page_analysis.send_failure_callback = lambda job_id, msg: None
        page_analysis.send_crawl_summary = lambda *a, **k: None
        page_analysis.upsert_issues = lambda issues, collection=None: type("R", (), {"upserted_count": len(issues), "modified_count": 0})()

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(page_analysis, name, value)

    def _job(self, urls=None):
        kwargs = dict(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), sourceJobId=SOURCE_JOB_ID)
        if urls is not None:
            kwargs["urls"] = urls
        return PageAnalysisJob(**kwargs)

    def _page_urls_reconciled(self):
        """Every distinct page_url reconcile_issue_lifecycle's update_many
        calls touched (both collections' filt include page_url)."""
        return {filt["page_url"] for filt, _ in self.seo_page_issues.update_calls}


class TestSuccessfulPagesReconciled(LifecycleWiringTestCase):
    def test_a_page_with_issues_is_reconciled_with_its_own_dedup_keys(self):
        self.seo_page_data._find_result = [_page("https://example.com/a")]

        def fake_analyze(page, job_id, project_id, **kwargs):
            return {
                "issues": [{
                    "page_url": "https://example.com/a",
                    "issue_code": "TITLE_MISSING",
                    "dedup_key": compute_dedup_key(PROJECT_ID, "https://example.com/a", "TITLE_MISSING", None),
                }],
                "summary": {},
            }
        page_analysis.analyze_page_seo = fake_analyze

        page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/a"]))

        self.assertIn("https://example.com/a", self._page_urls_reconciled())

    def test_a_page_with_zero_issues_is_still_reconciled_empty_current_set(self):
        # A page that's now fully clean (nothing detected) must still run
        # reconciliation so its previously-open issues resolve.
        self.seo_page_data._find_result = [_page("https://example.com/clean")]
        page_analysis.analyze_page_seo = lambda page, job_id, project_id, **kwargs: {"issues": [], "summary": {}}

        page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/clean"]))

        self.assertIn("https://example.com/clean", self._page_urls_reconciled())
        # The "resolve everything open" call has no dedup_key clause at all.
        resolve_calls = [
            (filt, update) for filt, update in self.seo_page_issues.update_calls
            if filt.get("page_url") == "https://example.com/clean" and filt.get("status") == "open"
        ]
        self.assertEqual(len(resolve_calls), 1)
        self.assertNotIn("dedup_key", resolve_calls[0][0])


class TestFailedPageNeverReconciled(LifecycleWiringTestCase):
    def test_a_page_whose_analysis_throws_is_excluded_from_reconciliation(self):
        # Two pages: "a" analyzes fine, "b" throws. Only "a" may ever be
        # reconciled — "b"'s previously-open issues must be left untouched
        # since it was never actually re-checked this run.
        self.seo_page_data._find_result = [
            _page("https://example.com/a"),
            _page("https://example.com/b"),
        ]

        def flaky_analyze(page, job_id, project_id, **kwargs):
            if page["url"] == "https://example.com/b":
                raise RuntimeError("boom")
            return {"issues": [], "summary": {}}
        page_analysis.analyze_page_seo = flaky_analyze

        page_analysis.execute_page_analysis_logic(
            self._job(urls=["https://example.com/a", "https://example.com/b"])
        )

        reconciled = self._page_urls_reconciled()
        self.assertIn("https://example.com/a", reconciled)
        self.assertNotIn("https://example.com/b", reconciled)


if __name__ == "__main__":
    unittest.main()
