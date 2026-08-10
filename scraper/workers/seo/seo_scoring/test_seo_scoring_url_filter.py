"""P2-002 tests: SEO_SCORING worker optional URL-scope filtering.

Stdlib unittest only. seo_scoring.py does NOT import `db` collections at
module level (unlike page_analysis.py) — it calls get_db_connection()
(itself a module-level function doing `import db; return db.db`) fresh
each invocation. So the robust rebind-after-import lesson from P2-001
applies slightly differently here: we monkey-patch get_db_connection
itself (a name bound directly on the seo_scoring module) rather than
individual collection names — same principle, adapted to this worker's
actual structure, verified by reading the code first rather than assumed
from the sibling worker's shape.

Two more worker-specific contract facts verified by reading the code
(NOT assumed from PAGE_ANALYSIS's P2-001 behavior, which differs):
- execute_seo_scoring_logic returns None on every path (no result dict) —
  assertions here are on side effects (callback calls, query/write args),
  not a return value.
- On any exception, the function logs + calls send_failure_callback and
  falls through to a bare `except: ... ` with NO re-raise (unlike
  PAGE_ANALYSIS, which re-raises as HTTPException). There is also NO
  per-page try/except around the scoring loop — a single page's scoring
  exception propagates to the SAME outer handler and fails the whole job,
  not isolated per-page like PAGE_ANALYSIS.
- Schema discovery (build_canonical_field_mapping) is bypassed via direct
  monkey-patch rather than replicated — it's pre-existing, untouched
  functionality unrelated to URL filtering.

Run from python_workers/:
    python -m unittest scraper.workers.seo.seo_scoring.test_seo_scoring_url_filter -v
"""

import sys
import types
import unittest
from datetime import datetime

from bson.objectid import ObjectId
from pydantic import ValidationError
from pymongo.errors import BulkWriteError


def _stub_module(name, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        if not hasattr(module, key):
            setattr(module, key, value)


_stub_module("db", db=object())
# Merge-safe additions below: if another test module in this process (e.g.
# P0-002's test_page_scraping_upsert.py) already stubbed scraper.shared.utils
# or config.config WITHOUT these specific names, _stub_module's "only set if
# not already present" merge logic would otherwise leave them missing and
# these imports would fail with ImportError — confirmed by hitting exactly
# that failure (twice, one transitive import at a time) when this file runs
# after test_page_scraping_upsert.py in the same `python -m unittest a b c`
# invocation. Placeholder bodies/values are fine: setUp() rebinds the real
# functions directly on the seo_scoring module after import; the
# config.config constants below are only needed to satisfy
# scraper.shared.schema's import line (pulled in transitively via
# api/seo_scoring.py -> validate_seo_scoring_input) and are never exercised
# by anything under test here.
_stub_module(
    "scraper.shared.utils",
    send_completion_callback=lambda *a, **k: None,
    send_failure_callback=lambda *a, **k: None,
)
_stub_module(
    "config.config",
    SOCIAL_DOMAINS_FOR_SCHEMA=[],
    LOCALBUSINESS_REQUIRED_FIELDS=[],
    FAQ_INDICATORS=[],
    TESTIMONIAL_INDICATORS=[],
)

import scraper.workers.seo.seo_scoring.seo_scoring as seo_scoring
from api.seo_scoring import SeoScoringJob


PROJECT_ID = str(ObjectId())
JOB_ID = str(ObjectId())
SOURCE_JOB_ID = str(ObjectId())

FIELD_MAPPING = {
    "project": "projectId",
    "url_data": "url",
    "url_issues": "page_url",
    "severity": "severity",
    "rule": "issue_code",
}


class FakeCollection:
    def __init__(self, find_result=None, find_one_result=None, count_result=None):
        self.find_calls = []
        self.count_calls = []
        self._find_result = list(find_result or [])
        self._find_one_result = find_one_result
        self._count_result = count_result
        self.bulk_write_calls = []
        self.update_one_calls = []
        self._find_override = None

    def count_documents(self, query):
        self.count_calls.append(query)
        if self._count_result is not None:
            return self._count_result
        return len(self._find_result)

    def find(self, query=None, projection=None, *a, **k):
        self.find_calls.append(query)
        if self._find_override is not None:
            return self._find_override(query)
        return list(self._find_result)

    def find_one(self, query=None, *a, **k):
        return self._find_one_result

    def bulk_write(self, ops, ordered=True):
        self.bulk_write_calls.append(list(ops))
        class Result:
            matched_count = 0
            upserted_count = len(ops)
        return Result()

    def update_one(self, filt, update):
        self.update_one_calls.append((filt, update))
        class Result:
            matched_count = 1
            modified_count = 1
        return Result()


class FakeDb:
    def __init__(self, pages=None, issues=None, page_scores=None):
        self.seo_page_data = FakeCollection(find_result=pages or [])
        self.seo_page_issues = FakeCollection(find_result=issues or [])
        self.seo_page_performance = FakeCollection(find_result=[])
        self.seo_page_scores = FakeCollection(find_result=page_scores if page_scores is not None else [])
        self.seoprojects = FakeCollection(find_one_result={"_id": ObjectId(PROJECT_ID), "project_name": "test"})


def _page(url, word_count=300):
    return {"projectId": ObjectId(PROJECT_ID), "url": url, "scrape_status": "SUCCESS",
            "content": {"word_count": word_count}}


def _issue(url, severity="low"):
    return {"projectId": ObjectId(PROJECT_ID), "page_url": url, "severity": severity, "issue_code": "X"}


class SeoScoringTestCase(unittest.TestCase):
    PATCHED_NAMES = ["get_db_connection", "is_job_cancelled", "send_completion_callback",
                     "send_failure_callback", "build_canonical_field_mapping"]

    def setUp(self):
        self._originals = {name: getattr(seo_scoring, name) for name in self.PATCHED_NAMES}

        self.completion_calls = []
        self.failure_calls = []
        self.fake_db = FakeDb()

        seo_scoring.get_db_connection = lambda: self.fake_db
        seo_scoring.is_job_cancelled = lambda job_id: False
        seo_scoring.send_completion_callback = lambda job_id, stats: self.completion_calls.append((job_id, stats))
        seo_scoring.send_failure_callback = lambda job_id, msg: self.failure_calls.append((job_id, msg))
        # Schema discovery is pre-existing, untouched, unrelated machinery —
        # bypassed with a fixed, realistic mapping rather than replicated.
        seo_scoring.build_canonical_field_mapping = lambda schemas: dict(FIELD_MAPPING)

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(seo_scoring, name, value)

    def _job(self, urls=None):
        kwargs = dict(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()), sourceJobId=SOURCE_JOB_ID)
        if urls is not None:
            kwargs["urls"] = urls
        return SeoScoringJob(**kwargs)

    def _page_query(self):
        self.assertGreaterEqual(len(self.fake_db.seo_page_data.find_calls), 1)
        return self.fake_db.seo_page_data.find_calls[0]


class TestFiltering(SeoScoringTestCase):
    def test_one_url_filters_to_exactly_that_page(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        self.fake_db.seo_page_issues._find_result = [_issue("https://example.com/a")]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))

        query = self._page_query()
        self.assertEqual(query["url"], {"$in": ["https://example.com/a"]})
        self.assertEqual(len(self.fake_db.seo_page_scores.bulk_write_calls[0]), 1)
        self.assertEqual(self.completion_calls[0][1]["pagesScored"], 1)

    def test_multiple_urls_filters_to_exactly_those_pages(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]
        urls = ["https://example.com/a", "https://example.com/b"]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=urls))

        query = self._page_query()
        self.assertEqual(query["url"], {"$in": urls})

    def test_url_not_found_matches_the_existing_zero_pages_failure_path_unchanged(self):
        # SEO_SCORING's PRE-EXISTING (untouched) contract treats zero
        # matched pages as a failure, not a graceful empty success — this
        # applies identically whether zero pages exist project-wide (today)
        # or zero pages match a URL filter (new) because it is the SAME,
        # unmodified check. Preserving this exactly, not special-casing the
        # filtered case, is what "only change page selection" requires.
        self.fake_db.seo_page_data._find_result = []

        seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/does-not-exist"]))

        self.assertEqual(len(self.failure_calls), 1)
        self.assertIn("No pages found", self.failure_calls[0][1])
        self.assertEqual(self.completion_calls, [])

    def test_duplicate_urls_in_filter_are_passed_through_unchanged(self):
        dup = ["https://example.com/a", "https://example.com/a"]
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a")]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=dup))

        query = self._page_query()
        self.assertEqual(query["url"], {"$in": dup})

    def test_empty_list_is_treated_as_no_filter(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a"), _page("https://example.com/b")]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=[]))

        query = self._page_query()
        self.assertNotIn("url", query)


class TestRegression(SeoScoringTestCase):
    def test_full_audit_unchanged_no_urls_field_at_all(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        seo_scoring.execute_seo_scoring_logic(self._job(urls=None))

        query = self._page_query()
        self.assertEqual(query, {"projectId": ObjectId(PROJECT_ID), "scrape_status": "SUCCESS"})
        self.assertNotIn("url", query)

    def test_scoring_algorithm_unchanged_identical_score_for_identical_inputs_filtered_or_not(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a", word_count=500)]
        self.fake_db.seo_page_issues._find_result = [_issue("https://example.com/a", "high")]
        seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))
        filtered_op = self.fake_db.seo_page_scores.bulk_write_calls[0][0]

        self.fake_db2 = FakeDb(
            pages=[_page("https://example.com/a", word_count=500)],
            issues=[_issue("https://example.com/a", "high")],
        )
        seo_scoring.get_db_connection = lambda: self.fake_db2
        seo_scoring.execute_seo_scoring_logic(self._job(urls=None))
        full_audit_op = self.fake_db2.seo_page_scores.bulk_write_calls[0][0]

        self.assertEqual(filtered_op._doc["$set"]["page_score"], full_audit_op._doc["$set"]["page_score"])
        self.assertEqual(filtered_op._doc["$set"]["page_grade"], full_audit_op._doc["$set"]["page_grade"])

    def test_aggregate_score_query_unchanged_never_filtered_by_url_even_when_the_page_query_is(self):
        # The website-level re-read (Phase 7) must remain project-scoped
        # only — this is the "aggregation algorithm: DO NOT CHANGE"
        # requirement, verified directly on the query the aggregate step
        # actually issues.
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        self.fake_db.seo_page_issues._find_result = []
        self.fake_db.seo_page_scores._find_result = [{"page_score": 80}]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))

        aggregate_query = self.fake_db.seo_page_scores.find_calls[0]
        self.assertEqual(aggregate_query, {"projectId": ObjectId(PROJECT_ID)})
        self.assertNotIn("url", aggregate_query)

    def test_database_writes_unchanged_upsert_filter_and_project_update_shape(self):
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        self.fake_db.seo_page_scores._find_result = [{"page_score": 90}]

        seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))

        op = self.fake_db.seo_page_scores.bulk_write_calls[0][0]
        self.assertEqual(op._filter, {"projectId": ObjectId(PROJECT_ID), "page_url": "https://example.com/a"})
        self.assertTrue(op._upsert)

        project_filt, project_update = self.fake_db.seoprojects.update_one_calls[0]
        self.assertEqual(project_filt, {"_id": ObjectId(PROJECT_ID)})
        self.assertEqual(
            set(project_update["$set"].keys()),
            {"website_score", "website_grade", "pages_scored", "last_scored_at", "scoring_version"},
        )


class TestFailureCases(SeoScoringTestCase):
    def test_malformed_urls_non_list_rejected_by_pydantic(self):
        with self.assertRaises(ValidationError):
            SeoScoringJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                          sourceJobId=SOURCE_JOB_ID, urls="not-a-list")

    def test_malformed_urls_non_string_elements_rejected_by_pydantic(self):
        with self.assertRaises(ValidationError):
            SeoScoringJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                          sourceJobId=SOURCE_JOB_ID, urls=[123])

    def test_database_failure_on_the_filtered_query_calls_failure_callback_and_does_not_raise(self):
        # Existing (pre-P2-002) contract: bare except -> log + failure
        # callback -> falls through with NO re-raise (different from
        # PAGE_ANALYSIS, which re-raises HTTPException — verified by
        # reading the code, not assumed from that sibling worker).
        def raising_find(query=None, *a, **k):
            raise ConnectionError("simulated Mongo outage")
        self.fake_db.seo_page_data.find = raising_find

        result = seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))

        self.assertIsNone(result)
        self.assertEqual(len(self.failure_calls), 1)
        self.assertIn("simulated Mongo outage", self.failure_calls[0][1])

    def test_scoring_exception_is_not_isolated_per_page_fails_whole_job_matching_existing_contract(self):
        # Existing (pre-P2-002) contract: no per-page try/except exists
        # around the scoring loop (verified by reading the code) — one
        # page's exception fails the entire job via the outer handler, it
        # is NOT isolated per-page like PAGE_ANALYSIS.
        self.fake_db.seo_page_data._find_result = [_page("https://example.com/a", word_count="not-a-number")]

        result = seo_scoring.execute_seo_scoring_logic(self._job(urls=["https://example.com/a"]))

        self.assertIsNone(result)
        self.assertEqual(len(self.failure_calls), 1)
        self.assertEqual(self.completion_calls, [])


if __name__ == "__main__":
    unittest.main()
