"""P0-002 tests: seo_page_data write path insert_many -> upsert.

Stdlib unittest only (matching P0-001's zero-new-dependency, built-in-runner
approach — no pytest exists in this repo). Heavy import-time dependencies of
page_scraping.py (db connects to Mongo at import; scraper.shared modules pull
browser tooling) are stubbed in sys.modules BEFORE the module under test is
imported, so these tests run with no Mongo and no network.

Run from python_workers/:
    python -m unittest scraper.workers.seo.page_scraping.test_page_scraping_upsert -v
"""

import sys
import types
import unittest
from unittest import mock

from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError


def _stub_module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module


# ---------------------------------------------------------------------------
# Stub every import-time side-effectful dependency, then import the real
# module under test. The stubs are inert placeholders — none of the write-path
# code under test touches them (the collection is injected per-test).
# ---------------------------------------------------------------------------
_stub_module(
    "db",
    seo_internal_links=object(),
    seo_page_data=object(),
    seo_page_failures=object(),
    jobs=object(),
)
_stub_module("config")
_stub_module("config.config", USER_AGENTS=["test-agent/1.0"])
_stub_module("scraper.shared.orchestrator", scrape_page_data=lambda *a, **k: None)
_stub_module("scraper.shared.url_selector", get_top_urls=lambda *a, **k: [])
_stub_module(
    "scraper.shared.fetcher",
    probe_rendering_need=lambda *a, **k: False,
    set_audit_user_agent=lambda *a, **k: None,
    is_js_rendering_domain=lambda *a, **k: False,
    get_request_user_agent=lambda *a, **k: "test-agent/1.0",
    get_pooled_session=lambda *a, **k: None,
)
_stub_module(
    "scraper.shared.utils",
    normalize_url=lambda u, *a, **k: u,
    get_registrable_domain=lambda u, *a, **k: u,
)
_stub_module("shared.context_enrichment", enrich_from_page_data=lambda *a, **k: {})

from scraper.workers.seo.page_scraping.page_scraping import upsert_page_data_results  # noqa: E402


def _doc(project_id, url, **extra):
    base = {"projectId": project_id, "url": url, "scrape_status": "SUCCESS"}
    base.update(extra)
    return base


def _bulk_write_error(write_errors):
    return BulkWriteError({"writeErrors": write_errors})


class RecordingCollection:
    """Captures bulk_write calls; optionally raises queued errors in order."""

    def __init__(self, errors=None):
        self.calls = []  # list of (requests, ordered)
        self._errors = list(errors or [])

    def bulk_write(self, requests, ordered=True):
        self.calls.append((list(requests), ordered))
        if self._errors:
            error = self._errors.pop(0)
            if error is not None:
                raise error
        return mock.Mock(upserted_count=len(requests), modified_count=0)


class InMemoryCollection:
    """Just enough of bulk_write(ReplaceOne, upsert=True) semantics for
    integration-style assertions: keyed on (projectId, url), replace-if-
    exists, insert-if-missing."""

    def __init__(self):
        self.docs = {}

    def bulk_write(self, requests, ordered=True):
        for op in requests:
            # pymongo.ReplaceOne has no public accessors; tests reach into
            # the documented-private fields it stores its arguments on.
            key = (op._filter["projectId"], op._filter["url"])
            if key in self.docs or op._upsert:
                self.docs[key] = op._doc
        return mock.Mock()


class TestUpsertOpConstruction(unittest.TestCase):
    """The exact frozen contract: ReplaceOne, {projectId, url} filter,
    upsert=True, ordered=False, document passed through unmodified."""

    def test_ops_are_replaceone_with_project_url_filter_and_upsert(self):
        collection = RecordingCollection()
        docs = [_doc("p1", "https://a.com/x"), _doc("p1", "https://a.com/y")]

        upsert_page_data_results(docs, "job1", collection=collection)

        self.assertEqual(len(collection.calls), 1)
        requests, ordered = collection.calls[0]
        self.assertEqual(ordered, False)
        expected = [
            ReplaceOne({"projectId": "p1", "url": "https://a.com/x"}, docs[0], upsert=True),
            ReplaceOne({"projectId": "p1", "url": "https://a.com/y"}, docs[1], upsert=True),
        ]
        self.assertEqual(requests, expected)

    def test_document_fields_are_preserved_exactly(self):
        # Full Audit regression guard: the replacement document must be the
        # input document itself — no fields added, removed, or rewritten by
        # the write path.
        collection = RecordingCollection()
        doc = _doc(
            "p1",
            "https://a.com/x",
            seo_jobId="j1",
            sourceJobId=None,
            scrapedAt="2026-07-27",
            screenshot_path=None,
            internal_links=["https://a.com/y"],
            http_status_code=200,
            response_time_ms=123,
        )
        snapshot = dict(doc)

        upsert_page_data_results([doc], "job1", collection=collection)

        requests, _ = collection.calls[0]
        self.assertIs(requests[0]._doc, doc)
        self.assertEqual(doc, snapshot)


class TestUpsertSemantics(unittest.TestCase):
    """Integration-style: behavior against Mongo-like replace/insert rules."""

    def test_new_page_inserted(self):
        collection = InMemoryCollection()
        doc = _doc("p1", "https://a.com/new")

        upsert_page_data_results([doc], "job1", collection=collection)

        self.assertEqual(collection.docs[("p1", "https://a.com/new")], doc)

    def test_existing_page_overwritten_not_skipped(self):
        # The exact stale-data bug this task removes: under insert_many the
        # old document survived; under upsert the fresh scrape must win.
        collection = InMemoryCollection()
        stale = _doc("p1", "https://a.com/x", title="OLD TITLE", scrapedAt="2026-01-01")
        collection.docs[("p1", "https://a.com/x")] = stale

        fresh = _doc("p1", "https://a.com/x", title="NEW TITLE", scrapedAt="2026-07-27")
        upsert_page_data_results([fresh], "job2", collection=collection)

        stored = collection.docs[("p1", "https://a.com/x")]
        self.assertEqual(stored["title"], "NEW TITLE")
        self.assertIsNot(stored, stale)

    def test_duplicate_url_across_jobs_updates_single_document(self):
        collection = InMemoryCollection()
        first = _doc("p1", "https://a.com/x", seo_jobId="jobA")
        second = _doc("p1", "https://a.com/x", seo_jobId="jobB")

        upsert_page_data_results([first], "jobA", collection=collection)
        upsert_page_data_results([second], "jobB", collection=collection)

        self.assertEqual(len(collection.docs), 1)
        self.assertEqual(collection.docs[("p1", "https://a.com/x")]["seo_jobId"], "jobB")

    def test_same_url_different_projects_stay_separate(self):
        collection = InMemoryCollection()
        upsert_page_data_results([_doc("p1", "https://a.com/x")], "j1", collection=collection)
        upsert_page_data_results([_doc("p2", "https://a.com/x")], "j2", collection=collection)

        self.assertEqual(len(collection.docs), 2)


class TestConcurrentDuplicateKeyRace(unittest.TestCase):
    """Two concurrent upserts for the same missing (projectId, url): the
    loser's write fails with code 11000 and must be retried once (on retry
    the document exists, so ReplaceOne matches and replaces)."""

    def test_raced_upserts_are_retried_once_with_only_the_losers(self):
        docs = [
            _doc("p1", "https://a.com/a"),
            _doc("p1", "https://a.com/b"),  # index 1 loses the race
            _doc("p1", "https://a.com/c"),
        ]
        collection = RecordingCollection(
            errors=[_bulk_write_error([{"index": 1, "code": 11000}]), None]
        )

        upsert_page_data_results(docs, "job1", collection=collection)

        self.assertEqual(len(collection.calls), 2)
        retry_requests, retry_ordered = collection.calls[1]
        self.assertEqual(retry_ordered, False)
        self.assertEqual(
            retry_requests,
            [ReplaceOne({"projectId": "p1", "url": "https://a.com/b"}, docs[1], upsert=True)],
        )

    def test_multiple_raced_upserts_all_retried(self):
        docs = [_doc("p1", f"https://a.com/{i}") for i in range(4)]
        collection = RecordingCollection(
            errors=[
                _bulk_write_error([{"index": 0, "code": 11000}, {"index": 3, "code": 11000}]),
                None,
            ]
        )

        upsert_page_data_results(docs, "job1", collection=collection)

        retry_requests, _ = collection.calls[1]
        retried_urls = {op._filter["url"] for op in retry_requests}
        self.assertEqual(retried_urls, {"https://a.com/0", "https://a.com/3"})


class TestFailurePropagation(unittest.TestCase):
    def test_non_duplicate_bulk_write_error_is_reraised(self):
        collection = RecordingCollection(
            errors=[_bulk_write_error([{"index": 0, "code": 121}])]  # document validation failure
        )

        with self.assertRaises(BulkWriteError):
            upsert_page_data_results([_doc("p1", "https://a.com/x")], "job1", collection=collection)

        # No retry attempted for a non-duplicate error class.
        self.assertEqual(len(collection.calls), 1)

    def test_mixed_duplicate_and_real_errors_are_reraised(self):
        collection = RecordingCollection(
            errors=[
                _bulk_write_error(
                    [{"index": 0, "code": 11000}, {"index": 1, "code": 121}]
                )
            ]
        )

        with self.assertRaises(BulkWriteError):
            upsert_page_data_results(
                [_doc("p1", "https://a.com/x"), _doc("p1", "https://a.com/y")],
                "job1",
                collection=collection,
            )

        self.assertEqual(len(collection.calls), 1)

    def test_retry_failure_propagates_not_silently_swallowed(self):
        # If even the retry fails, the error surfaces as a job failure — no
        # silent partial write.
        collection = RecordingCollection(
            errors=[
                _bulk_write_error([{"index": 0, "code": 11000}]),
                _bulk_write_error([{"index": 0, "code": 11000}]),
            ]
        )

        with self.assertRaises(BulkWriteError):
            upsert_page_data_results([_doc("p1", "https://a.com/x")], "job1", collection=collection)

        self.assertEqual(len(collection.calls), 2)

    def test_generic_exception_propagates(self):
        class ExplodingCollection:
            def bulk_write(self, requests, ordered=True):
                raise ConnectionError("Mongo connection lost")

        with self.assertRaises(ConnectionError):
            upsert_page_data_results(
                [_doc("p1", "https://a.com/x")], "job1", collection=ExplodingCollection()
            )


if __name__ == "__main__":
    unittest.main()
