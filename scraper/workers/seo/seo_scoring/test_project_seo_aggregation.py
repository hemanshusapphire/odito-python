"""F4-016 tests: PROJECT_SEO_AGGREGATION worker + SEO_SCORING's batched skip.

Same stdlib-unittest / monkey-patch / FakeCollection conventions as the
sibling test_seo_scoring_url_filter.py (get_db_connection is patched
directly on the module rather than individual collection names, since
seo_scoring.py resolves its db connection fresh per call).

Run from python_workers/:
    python -m unittest scraper.workers.seo.seo_scoring.test_project_seo_aggregation -v
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


_stub_module("db", db=object())
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
from api.seo_scoring import SeoScoringJob, ProjectSeoAggregationJob


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
    def __init__(self, find_result=None, find_one_result=None):
        self.find_calls = []
        self._find_result = list(find_result or [])
        self._find_one_result = find_one_result
        self.update_one_calls = []
        self.bulk_write_calls = []

    def find(self, query=None, projection=None, *a, **k):
        self.find_calls.append(query)
        return list(self._find_result)

    def find_one(self, query=None, *a, **k):
        if self._find_one_result is not None:
            return self._find_one_result
        return self._find_result[0] if self._find_result else None

    def update_one(self, filt, update):
        self.update_one_calls.append((filt, update))
        class Result:
            matched_count = 1
            modified_count = 1
        return Result()

    def bulk_write(self, ops, ordered=True):
        self.bulk_write_calls.append(list(ops))
        class Result:
            matched_count = 0
            upserted_count = len(ops)
        return Result()

    def count_documents(self, query):
        return len(self._find_result)


class FakeDb:
    def __init__(self, page_scores=None):
        self.seo_page_data = FakeCollection(find_result=[])
        self.seo_page_issues = FakeCollection(find_result=[])
        self.seo_page_performance = FakeCollection(find_result=[])
        self.seo_page_scores = FakeCollection(find_result=page_scores or [])
        self.seoprojects = FakeCollection(find_one_result={"_id": ObjectId(PROJECT_ID), "project_name": "test"})


def _page(url, word_count=300):
    return {"projectId": ObjectId(PROJECT_ID), "url": url, "scrape_status": "SUCCESS",
            "content": {"word_count": word_count}}


class ProjectSeoAggregationTestCase(unittest.TestCase):
    PATCHED_NAMES = ["get_db_connection", "is_job_cancelled", "send_completion_callback",
                     "send_failure_callback", "build_canonical_field_mapping"]

    def setUp(self):
        self._originals = {name: getattr(seo_scoring, name) for name in self.PATCHED_NAMES}

        self.completion_calls = []
        self.failure_calls = []

        seo_scoring.is_job_cancelled = lambda job_id: False
        seo_scoring.send_completion_callback = lambda job_id, stats: self.completion_calls.append((job_id, stats))
        seo_scoring.send_failure_callback = lambda job_id, msg: self.failure_calls.append((job_id, msg))
        seo_scoring.build_canonical_field_mapping = lambda schemas: dict(FIELD_MAPPING)

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(seo_scoring, name, value)


class TestDetectProjectFieldFromPageScores(ProjectSeoAggregationTestCase):
    def test_detects_projectId_field(self):
        db = FakeDb(page_scores=[{"projectId": ObjectId(PROJECT_ID), "page_score": 80}])
        self.assertEqual(seo_scoring.detect_project_field_from_page_scores(db), "projectId")

    def test_detects_alternate_field_name(self):
        db = FakeDb(page_scores=[{"seo_project_id": ObjectId(PROJECT_ID), "page_score": 80}])
        self.assertEqual(seo_scoring.detect_project_field_from_page_scores(db), "seo_project_id")

    def test_returns_none_when_seo_page_scores_is_empty(self):
        db = FakeDb(page_scores=[])
        self.assertIsNone(seo_scoring.detect_project_field_from_page_scores(db))


class TestExecuteProjectSeoAggregationLogic(ProjectSeoAggregationTestCase):
    def _job(self):
        return ProjectSeoAggregationJob(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                                         batchId="batch-1", sourceJobId=SOURCE_JOB_ID)

    def test_averages_all_page_scores_for_the_project_and_persists_to_seoprojects(self):
        fake_db = FakeDb(page_scores=[
            {"projectId": ObjectId(PROJECT_ID), "page_score": 80},
            {"projectId": ObjectId(PROJECT_ID), "page_score": 90},
            {"projectId": ObjectId(PROJECT_ID), "page_score": 100},
        ])
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_project_seo_aggregation_logic(self._job())

        self.assertEqual(len(self.completion_calls), 1)
        self.assertEqual(self.completion_calls[0][1]["websiteScore"], 90.0)

        project_filt, project_update = fake_db.seoprojects.update_one_calls[0]
        self.assertEqual(project_filt, {"_id": ObjectId(PROJECT_ID)})
        self.assertEqual(project_update["$set"]["website_score"], 90.0)
        self.assertEqual(project_update["$set"]["pages_scored"], 3)

    def test_reads_all_seo_page_scores_not_scoped_to_job_id(self):
        # This job's own jobId must never appear in the seo_page_scores
        # query — PROJECT_SEO_AGGREGATION reads the WHOLE project's current
        # state, same reasoning as project_aggregator.aggregate_project's
        # own job_id-independent read.
        fake_db = FakeDb(page_scores=[{"projectId": ObjectId(PROJECT_ID), "page_score": 75}])
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_project_seo_aggregation_logic(self._job())

        query = fake_db.seo_page_scores.find_calls[0]
        self.assertEqual(query, {"projectId": ObjectId(PROJECT_ID)})
        self.assertNotIn("jobId", query)

    def test_no_seo_page_scores_for_project_calls_failure_callback(self):
        fake_db = FakeDb(page_scores=[])
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_project_seo_aggregation_logic(self._job())

        self.assertEqual(len(self.failure_calls), 1)
        self.assertIn("No seo_page_scores found", self.failure_calls[0][1])
        self.assertEqual(self.completion_calls, [])

    def test_database_failure_calls_failure_callback_and_does_not_raise(self):
        fake_db = FakeDb(page_scores=[{"projectId": ObjectId(PROJECT_ID), "page_score": 80}])
        def raising_find(query=None, projection=None, *a, **k):
            raise ConnectionError("simulated Mongo outage")
        fake_db.seo_page_scores.find = raising_find
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_project_seo_aggregation_logic(self._job())

        self.assertEqual(len(self.failure_calls), 1)
        self.assertIn("simulated Mongo outage", self.failure_calls[0][1])
        self.assertEqual(self.completion_calls, [])


class TestSeoScoringBatchedSkip(ProjectSeoAggregationTestCase):
    """SEO_SCORING (per-page) must defer Phase 7 when job.batchId is set."""

    def _job(self, batch_id=None):
        kwargs = dict(jobId=JOB_ID, projectId=PROJECT_ID, userId=str(ObjectId()),
                      sourceJobId=SOURCE_JOB_ID, urls=["https://example.com/a"])
        if batch_id is not None:
            kwargs["batchId"] = batch_id
        return SeoScoringJob(**kwargs)

    def test_batched_job_does_not_write_website_score_to_seoprojects(self):
        fake_db = FakeDb(page_scores=[{"page_score": 80}])
        fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_seo_scoring_logic(self._job(batch_id="batch-42"))

        self.assertEqual(fake_db.seoprojects.update_one_calls, [],
                          "a batched page's SEO_SCORING must not also run Phase 7 inline")

    def test_non_batched_job_still_writes_website_score_unchanged(self):
        fake_db = FakeDb(page_scores=[{"page_score": 80}])
        fake_db.seo_page_data._find_result = [_page("https://example.com/a")]
        seo_scoring.get_db_connection = lambda: fake_db

        seo_scoring.execute_seo_scoring_logic(self._job(batch_id=None))

        self.assertEqual(len(fake_db.seoprojects.update_one_calls), 1)


if __name__ == "__main__":
    unittest.main()
