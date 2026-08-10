"""P2-001 tests: PAGE_ANALYSIS worker optional URL-scope filtering.

Stdlib unittest only (same approach as every prior task this session).
page_analysis.py imports `db` at module load time (real db.py connects to
Mongo) — stubbed via sys.modules before import, same pattern as
test_page_analysis_upsert.py.

Robustness note: rather than relying on the shared sys.modules['db'] stub
(fragile if another test module in the same process claims it first with
inert placeholders — a real risk when multiple test files run together via
`python -m unittest a b c`), every test here REBINDS the module-level
collection/function names directly on the imported page_analysis module
after import. This works regardless of what got bound at import time and
never depends on import order relative to sibling test files.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_analysis.test_page_analysis_url_filter -v
"""

import sys
import types
import unittest
from datetime import datetime

from bson.objectid import ObjectId
from pydantic import ValidationError


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


PROJECT_ID = str(ObjectId())
JOB_ID = str(ObjectId())
SOURCE_JOB_ID = str(ObjectId())


class FakeCollection:
    """Minimal, explicit fake — no auto-magic, every call is recorded."""

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


class PageAnalysisTestCase(unittest.TestCase):
    """Rebinds page_analysis module globals; restores them in tearDown."""

    PATCHED_NAMES = [
        "seo_page_data", "seo_page_performance", "seo_page_summary",
        "seo_internal_links", "db", "analyze_page_seo", "is_job_cancelled",
        "send_progress_update", "send_completion_callback",
        "send_failure_callback", "send_crawl_summary", "upsert_issues",
    ]

    def setUp(self):
        self._originals = {name: getattr(page_analysis, name) for name in self.PATCHED_NAMES}

        self.seo_page_data = FakeCollection()
        self.seo_page_performance = FakeCollection(find_result=[])
        self.seo_page_summary = FakeCollection()
        self.seo_internal_links = FakeCollection()
        self.fake_db = FakeDb()

        self.analyze_calls = []
        self.completion_calls = []
        self.failure_calls = []

        page_analysis.seo_page_data = self.seo_page_data
        page_analysis.seo_page_performance = self.seo_page_performance
        page_analysis.seo_page_summary = self.seo_page_summary
        page_analysis.seo_internal_links = self.seo_internal_links
        page_analysis.db = self.fake_db
        page_analysis.is_job_cancelled = lambda job_id: False
        page_analysis.send_progress_update = lambda *a, **k: None
        page_analysis.send_completion_callback = lambda job_id, stats, result_data=None: self.completion_calls.append((job_id, stats, result_data))
        page_analysis.send_failure_callback = lambda job_id, msg: self.failure_calls.append((job_id, msg))
        page_analysis.send_crawl_summary = lambda *a, **k: None
        page_analysis.upsert_issues = lambda issues, collection=None: type("R", (), {"upserted_count": len(issues), "modified_count": 0})()

        def fake_analyze(page, job_id, project_id, **kwargs):
            self.analyze_calls.append(page.get("url"))
            return {"issues": [], "summary": {}}
        page_analysis.analyze_page_seo = fake_analyze

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(page_analysis, name, value)

    def _job(self, urls=None):
        kwargs = dict(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), sourceJobId=SOURCE_JOB_ID)
        if urls is not None:
            kwargs["urls"] = urls
        return PageAnalysisJob(**kwargs)

    def _find_query(self):
        self.assertEqual(len(self.seo_page_data.find_calls), 1)
        return self.seo_page_data.find_calls[0]


class TestFiltering(PageAnalysisTestCase):
    def test_one_url_filters_to_exactly_that_page(self):
        self.seo_page_data._find_result = [_page("https://example.com/a")]
        result = page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/a"]))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": ["https://example.com/a"]})
        self.assertEqual(self.analyze_calls, ["https://example.com/a"])
        self.assertEqual(result["stats"]["totalPages"], 1)

    def test_multiple_urls_filters_to_exactly_those_pages(self):
        self.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        urls = ["https://example.com/a", "https://example.com/b"]
        page_analysis.execute_page_analysis_logic(self._job(urls=urls))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": urls})

    def test_url_not_found_yields_zero_pages_not_an_error(self):
        self.seo_page_data._find_result = []  # simulates: filter URL doesn't match any SUCCESS-scraped doc
        result = page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/does-not-exist"]))

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stats"]["totalPages"], 0)
        self.assertEqual(self.failure_calls, [])

    def test_duplicate_urls_in_filter_are_passed_through_unchanged(self):
        # $in is set-semantic at the query-execution level regardless of
        # duplicate array entries — no dedup step is needed or added here.
        dup = ["https://example.com/a", "https://example.com/a"]
        self.seo_page_data._find_result = [_page("https://example.com/a")]
        page_analysis.execute_page_analysis_logic(self._job(urls=dup))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": dup})

    def test_empty_list_is_treated_as_no_filter_matching_existing_canonical_urls_convention(self):
        self.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        page_analysis.execute_page_analysis_logic(self._job(urls=[]))

        query = self._find_query()
        self.assertNotIn("url", query)
        self.assertEqual(self.analyze_calls, ["https://example.com/a", "https://example.com/b"])


class TestRegression(PageAnalysisTestCase):
    def test_full_audit_unchanged_no_urls_field_at_all(self):
        self.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        page_analysis.execute_page_analysis_logic(self._job(urls=None))

        query = self._find_query()
        self.assertEqual(query, {"projectId": ObjectId(PROJECT_ID), "scrape_status": "SUCCESS"})
        self.assertNotIn("url", query)

    def test_full_audit_query_byte_identical_shape_to_pre_p2_001(self):
        # The exact two keys that existed before this task, nothing added,
        # nothing removed, when no filter is supplied.
        self.seo_page_data._find_result = []
        page_analysis.execute_page_analysis_logic(self._job())
        query = self._find_query()
        self.assertEqual(set(query.keys()), {"projectId", "scrape_status"})

    def test_issue_generation_unchanged_analyze_page_seo_called_once_per_matched_page_with_same_args_shape(self):
        pages = [_page("https://example.com/a"), _page("https://example.com/b")]
        self.seo_page_data._find_result = pages
        captured = []
        def spy_analyze(page, job_id, project_id, **kwargs):
            captured.append((page.get("url"), job_id, project_id, sorted(kwargs.keys())))
            return {"issues": [{"issue_code": "X"}], "summary": {"s": 1}}
        page_analysis.analyze_page_seo = spy_analyze

        result = page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/a"]))

        # analyze_page_seo is still called for every page the query returned
        # (filtering happens at the DB layer, not by skipping in the loop),
        # with the same positional/keyword argument shape as Full Audit.
        self.assertEqual(len(captured), len(self.seo_page_data._find_result))
        for _, job_id, project_id, kwarg_names in captured:
            self.assertEqual(job_id, JOB_ID)
            self.assertEqual(project_id, PROJECT_ID)
            self.assertEqual(kwarg_names, ["crawl_graph_lookup", "headless_lookup", "performance_lookup", "technical_report"])

    def test_batching_unchanged_total_pages_reflects_query_result_size_not_input_url_count(self):
        # "Batching" here = the existing per-page loop/progress-reporting
        # shape; requesting 3 URLs but only 2 matching is reflected honestly.
        self.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        result = page_analysis.execute_page_analysis_logic(
            self._job(urls=["https://example.com/a", "https://example.com/b", "https://example.com/missing"])
        )
        self.assertEqual(result["stats"]["totalPages"], 2)
        self.assertEqual(result["stats"]["pagesAnalyzed"], 2)


class TestFailureCases(PageAnalysisTestCase):
    def test_malformed_urls_non_list_rejected_by_pydantic_before_the_worker_ever_runs(self):
        with self.assertRaises(ValidationError):
            PageAnalysisJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                             sourceJobId=SOURCE_JOB_ID, urls="not-a-list")

    def test_malformed_urls_non_string_elements_rejected_by_pydantic(self):
        with self.assertRaises(ValidationError):
            PageAnalysisJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                             sourceJobId=SOURCE_JOB_ID, urls=[123, 456])

    def test_database_failure_on_the_filtered_query_is_caught_by_the_existing_outer_handler(self):
        # Existing (pre-P2-001) contract: the outer except block logs, fires
        # send_failure_callback, THEN re-raises as HTTPException(500) — it
        # does not swallow and return an error dict. This is unchanged by
        # the URL filter; only the query dict that triggers the failure is
        # new.
        from fastapi import HTTPException

        def raising_find(query=None, *a, **k):
            raise ConnectionError("simulated Mongo outage")
        self.seo_page_data.find = raising_find

        with self.assertRaises(HTTPException) as ctx:
            page_analysis.execute_page_analysis_logic(self._job(urls=["https://example.com/a"]))

        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("simulated Mongo outage", ctx.exception.detail)
        self.assertEqual(len(self.failure_calls), 1)
        self.assertIn("simulated Mongo outage", self.failure_calls[0][1])

    def test_worker_exception_during_per_page_analysis_is_isolated_per_page_not_fatal(self):
        # Existing per-page try/except contract: one page's analyze_page_seo
        # exception must not abort the whole job, filtered or not.
        self.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        def flaky_analyze(page, job_id, project_id, **kwargs):
            if page.get("url") == "https://example.com/a":
                raise RuntimeError("simulated rule engine crash")
            return {"issues": [], "summary": {}}
        page_analysis.analyze_page_seo = flaky_analyze

        result = page_analysis.execute_page_analysis_logic(
            self._job(urls=["https://example.com/a", "https://example.com/b"])
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["stats"]["failedAnalyses"], 1)
        self.assertEqual(result["stats"]["pagesAnalyzed"], 1)
        self.assertEqual(self.failure_calls, [])  # job-level failure callback NOT triggered


if __name__ == "__main__":
    unittest.main()
