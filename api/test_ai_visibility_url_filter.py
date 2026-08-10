"""P2-003 tests: AI_VISIBILITY worker optional URL-scope filtering.

Stdlib unittest only. api/ai_visibility.py is DIFFERENT from P2-001/P2-002's
worker modules in one important structural way, verified by reading the code
first: execute_ai_visibility_v2() imports its dependencies (seo_page_data,
run_v2_pipeline, build_v2_domain_report, build_site_structure_lookup) LAZILY
— INSIDE the function body, on every call — not at module level. So
rebinding names on the `ai_visibility` module object (the P2-001/P2-002
pattern) would do nothing; these lazy `from X import Y` statements resolve
fresh against sys.modules[X] every call. The correct interception point is
mutating the attributes on the actual stub modules in sys.modules directly,
saved/restored per test.

_notify_node / _send_progress ARE module-level names in ai_visibility.py
itself, so those ARE rebound directly on the imported module (matching the
established pattern for those two).

Run from python_workers/:
    python -m unittest api.test_ai_visibility_url_filter -v
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


_stub_module("db", db=object())
_stub_module("scraper.workers.ai_v2.pipeline", run_v2_pipeline=lambda **k: {})
_stub_module("scraper.workers.ai_v2.domain_report_adapter", build_v2_domain_report=lambda *a, **k: {})
_stub_module("scraper.workers.ai_v2.extraction.site_structure", build_site_structure_lookup=lambda *a, **k: {})

import api.ai_visibility as ai_visibility
from api.ai_visibility import AIVisibilityJob


PROJECT_ID = str(ObjectId())
JOB_ID = str(ObjectId())


class FakeSeoPageDataCollection:
    """Supports the exact chained call this worker makes: .find(query).sort(...)."""

    def __init__(self, docs=None):
        self.find_calls = []
        self._docs = list(docs or [])

    def find(self, query=None, *a, **k):
        self.find_calls.append(query)
        return FakeCursor(list(self._docs))


class FakeCursor(list):
    def sort(self, *a, **k):
        return self  # ordering already fixed by the fixture; no-op sort proxy


def _page(url, extraction_status="SUCCESS"):
    return {"projectId": ObjectId(PROJECT_ID), "url": url, "extraction_status": extraction_status}


class AIVisibilityTestCase(unittest.TestCase):
    def setUp(self):
        self._original_db_seo_page_data = getattr(sys.modules["db"], "seo_page_data", None)
        self._original_run_v2_pipeline = sys.modules["scraper.workers.ai_v2.pipeline"].run_v2_pipeline
        self._original_build_domain_report = sys.modules["scraper.workers.ai_v2.domain_report_adapter"].build_v2_domain_report
        self._original_build_site_structure = sys.modules["scraper.workers.ai_v2.extraction.site_structure"].build_site_structure_lookup
        self._original_notify_node = ai_visibility._notify_node
        self._original_send_progress = ai_visibility._send_progress

        self.fake_collection = FakeSeoPageDataCollection()
        sys.modules["db"].seo_page_data = self.fake_collection

        self.pipeline_calls = []
        def fake_pipeline(**kwargs):
            self.pipeline_calls.append(kwargs)
            return {"pages_scored": len(kwargs["page_docs"]), "issues_summary": {"total": 0}}
        sys.modules["scraper.workers.ai_v2.pipeline"].run_v2_pipeline = fake_pipeline

        self.domain_report_calls = []
        sys.modules["scraper.workers.ai_v2.domain_report_adapter"].build_v2_domain_report = \
            lambda project_id: self.domain_report_calls.append(project_id) or {}

        self.site_structure_calls = []
        sys.modules["scraper.workers.ai_v2.extraction.site_structure"].build_site_structure_lookup = \
            lambda project_id: self.site_structure_calls.append(project_id) or {}

        self.notify_calls = []
        ai_visibility._notify_node = lambda job_id, endpoint, payload: self.notify_calls.append((job_id, endpoint, payload))
        ai_visibility._send_progress = lambda *a, **k: None

    def tearDown(self):
        sys.modules["db"].seo_page_data = self._original_db_seo_page_data
        sys.modules["scraper.workers.ai_v2.pipeline"].run_v2_pipeline = self._original_run_v2_pipeline
        sys.modules["scraper.workers.ai_v2.domain_report_adapter"].build_v2_domain_report = self._original_build_domain_report
        sys.modules["scraper.workers.ai_v2.extraction.site_structure"].build_site_structure_lookup = self._original_build_site_structure
        ai_visibility._notify_node = self._original_notify_node
        ai_visibility._send_progress = self._original_send_progress

    def _job(self, urls=None):
        kwargs = dict(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), sourceJobId=str(ObjectId()))
        if urls is not None:
            kwargs["urls"] = urls
        return AIVisibilityJob(**kwargs)

    def _find_query(self):
        self.assertEqual(len(self.fake_collection.find_calls), 1)
        return self.fake_collection.find_calls[0]


class TestFiltering(AIVisibilityTestCase):
    def test_one_url_filters_to_exactly_that_page(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        result = ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": ["https://example.com/a"]})
        self.assertEqual(len(self.pipeline_calls[0]["page_docs"]), 1)
        self.assertEqual(result["status"], "success")

    def test_multiple_urls_filters_to_exactly_those_pages(self):
        self.fake_collection._docs = [_page("https://example.com/a"), _page("https://example.com/b")]
        urls = ["https://example.com/a", "https://example.com/b"]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=urls))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": urls})

    def test_url_not_found_is_graceful_no_pages_status_not_a_failure(self):
        # Matches this worker's EXISTING (pre-P2-003) zero-pages contract —
        # graceful, like PAGE_ANALYSIS, NOT a failure like SEO_SCORING.
        # Verified precisely, not assumed from either sibling worker.
        self.fake_collection._docs = []
        result = ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/missing"]))

        self.assertEqual(result["status"], "no_pages")
        self.assertEqual(result["stats"]["pages_processed"], 0)
        self.assertEqual(self.pipeline_calls, [])

    def test_duplicate_urls_in_filter_are_passed_through_unchanged(self):
        dup = ["https://example.com/a", "https://example.com/a"]
        self.fake_collection._docs = [_page("https://example.com/a")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=dup))

        query = self._find_query()
        self.assertEqual(query["url"], {"$in": dup})

    def test_empty_list_is_treated_as_no_filter(self):
        self.fake_collection._docs = [_page("https://example.com/a"), _page("https://example.com/b")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=[]))

        query = self._find_query()
        self.assertNotIn("url", query)


class TestRegression(AIVisibilityTestCase):
    def test_full_audit_unchanged_no_urls_field_at_all(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=None))

        query = self._find_query()
        self.assertEqual(query, {"projectId": ObjectId(PROJECT_ID)})
        self.assertNotIn("url", query)

    def test_ai_visibility_scoring_pipeline_receives_identical_kwargs_shape_filtered_or_not(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))
        call = self.pipeline_calls[0]

        self.assertEqual(set(call.keys()), {"project_id", "job_id", "page_docs", "domain_report", "site_structure_lookup"})
        self.assertEqual(call["project_id"], PROJECT_ID)
        self.assertEqual(call["job_id"], JOB_ID)

    def test_recommendation_generation_inputs_domain_report_and_site_structure_unaffected_by_url_filter(self):
        # domain_report / site_structure_lookup are project-wide, domain-level
        # signals — correctly NOT scoped to the URL filter (a single-page
        # verification should still see the whole project's technical/domain
        # context). Verified they're still built from project_id alone,
        # never touched by job.urls.
        self.fake_collection._docs = [_page("https://example.com/a")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        self.assertEqual(self.domain_report_calls, [PROJECT_ID])
        self.assertEqual(self.site_structure_calls, [PROJECT_ID])

    def test_aggregate_visibility_unchanged_stats_derived_from_pipeline_result_not_recomputed_here(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        sys.modules["scraper.workers.ai_v2.pipeline"].run_v2_pipeline = \
            lambda **k: {"pages_scored": 7, "issues_summary": {"total": 3}}

        result = ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        self.assertEqual(result["stats"]["pages_processed"], 7)
        self.assertEqual(result["stats"]["issues_found"], 3)

    def test_database_writes_unchanged_notify_node_called_with_same_shape(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        self.assertEqual(len(self.notify_calls), 1)
        job_id, endpoint, payload = self.notify_calls[0]
        self.assertEqual(endpoint, "complete")
        self.assertEqual(set(payload.keys()), {"stats", "result_data"})


class TestFailureCases(AIVisibilityTestCase):
    def test_malformed_urls_non_list_rejected_by_pydantic(self):
        with self.assertRaises(ValidationError):
            AIVisibilityJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), urls="not-a-list")

    def test_malformed_urls_non_string_elements_rejected_by_pydantic(self):
        with self.assertRaises(ValidationError):
            AIVisibilityJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), urls=[123])

    def test_database_failure_on_the_filtered_query_calls_fail_notification_and_does_not_raise(self):
        # Existing (pre-P2-003) contract: outer try/except catches, notifies
        # Node with a "fail" callback, and returns an error-status dict — no
        # re-raise. A THIRD distinct contract shape across the three Phase 2
        # workers (PAGE_ANALYSIS re-raises HTTPException; SEO_SCORING
        # returns None; this one returns a dict) — verified by reading the
        # code, not assumed from either sibling.
        def raising_find(query=None, *a, **k):
            raise ConnectionError("simulated Mongo outage")
        self.fake_collection.find = raising_find

        result = ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        self.assertEqual(result["status"], "error")
        self.assertIn("simulated Mongo outage", result["message"])
        self.assertEqual(len(self.notify_calls), 1)
        self.assertEqual(self.notify_calls[0][1], "fail")

    def test_worker_exception_in_the_pipeline_itself_is_caught_and_reported_not_fatal_to_the_process(self):
        self.fake_collection._docs = [_page("https://example.com/a")]
        def raising_pipeline(**kwargs):
            raise RuntimeError("simulated V2 pipeline crash")
        sys.modules["scraper.workers.ai_v2.pipeline"].run_v2_pipeline = raising_pipeline

        result = ai_visibility.execute_ai_visibility_v2(self._job(urls=["https://example.com/a"]))

        self.assertEqual(result["status"], "error")
        self.assertIn("simulated V2 pipeline crash", result["message"])


if __name__ == "__main__":
    unittest.main()
