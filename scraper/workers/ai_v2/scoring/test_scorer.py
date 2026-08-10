"""AI_VISIBILITY aggregation correctness prerequisite: tests for
score_and_save()'s upsert filter moving from (project_id, job_id, url) to
(project_id, url).

This is the write-side half of the fix: aggregate_project() reading
project_id-only only produces a correct aggregate if ai_scores actually
holds ONE current doc per (project, url) rather than accumulating a new
doc per job_id forever. job_id is still stamped into the document via
$set (provenance), just no longer part of the upsert identity.

Stdlib unittest only, fake in-memory collection (same convention as
../aggregation/test_project_aggregator.py).

Run from python_workers/:
    python -m unittest scraper.workers.ai_v2.scoring.test_scorer -v
"""

import sys
import types
import unittest

from bson.objectid import ObjectId
from pymongo import ReturnDocument


def _stub_module(name, **attrs):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    for key, value in attrs.items():
        if not hasattr(module, key):
            setattr(module, key, value)


_stub_module("db", ai_scores=object())

import scraper.workers.ai_v2.scoring.scorer as scorer


class FakeCollection:
    def __init__(self):
        self.docs = []
        self._next_id = 1

    @staticmethod
    def _matches(doc, filt):
        return all(doc.get(k) == v for k, v in filt.items())

    def find_one(self, filt=None):
        filt = filt or {}
        for d in self.docs:
            if self._matches(d, filt):
                return d
        return None

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


class FakeRuleResult:
    def __init__(self, result="PASS", passed=True, evidence=None, skipped_reason=None):
        self.result = result
        self.passed = passed
        self.evidence = evidence
        self.skipped_reason = skipped_reason


class FakeRegistry:
    """Just enough of RuleRegistry for build_score_document's card loop."""

    def get_rules_for_card(self, hub, card):
        return [f"{hub}-{card}-r1"]


def _results(passed=True):
    return {"aiso-c1-r1": FakeRuleResult(result="PASS" if passed else "FAIL", passed=passed)}


class ScorerTestCase(unittest.TestCase):
    def setUp(self):
        self._orig_ai_scores = scorer.ai_scores
        self.ai_scores = FakeCollection()
        scorer.ai_scores = self.ai_scores
        # Only one hub/card needed to exercise score_and_save's upsert path.
        self._orig_hub_card_map = scorer.HUB_CARD_MAP
        scorer.HUB_CARD_MAP = {"aiso": ["c1"]}

    def tearDown(self):
        scorer.ai_scores = self._orig_ai_scores
        scorer.HUB_CARD_MAP = self._orig_hub_card_map


class TestCoreProof(ScorerTestCase):
    def test_rescoring_same_url_under_a_new_job_id_updates_the_same_doc_not_a_duplicate(self):
        project_id = ObjectId()
        page = {"_id": ObjectId(), "url": "https://a.com/1"}

        first = scorer.score_and_save(page, _results(), FakeRegistry(), project_id, ObjectId())
        job_b = ObjectId()
        second = scorer.score_and_save(page, _results(), FakeRegistry(), project_id, job_b)

        self.assertEqual(len(self.ai_scores.docs), 1)
        self.assertEqual(first["_id"], second["_id"])
        # job_id is still stamped as provenance — just not part of identity.
        self.assertEqual(second["job_id"], job_b)


class TestRegression(ScorerTestCase):
    def test_full_audit_different_urls_still_create_separate_docs(self):
        project_id = ObjectId()
        job_id = ObjectId()
        scorer.score_and_save({"_id": ObjectId(), "url": "https://a.com/1"}, _results(), FakeRegistry(), project_id, job_id)
        scorer.score_and_save({"_id": ObjectId(), "url": "https://a.com/2"}, _results(), FakeRegistry(), project_id, job_id)

        self.assertEqual(len(self.ai_scores.docs), 2)

    def test_different_projects_same_url_still_create_separate_docs(self):
        job_id = ObjectId()
        page = {"_id": ObjectId(), "url": "https://a.com/1"}
        scorer.score_and_save(page, _results(), FakeRegistry(), ObjectId(), job_id)
        scorer.score_and_save(page, _results(), FakeRegistry(), ObjectId(), job_id)

        self.assertEqual(len(self.ai_scores.docs), 2)

    def test_saved_doc_contains_expected_shape(self):
        project_id = ObjectId()
        page = {"_id": ObjectId(), "url": "https://a.com/1"}
        saved = scorer.score_and_save(page, _results(passed=False), FakeRegistry(), project_id, ObjectId())

        self.assertEqual(saved["project_id"], project_id)
        self.assertEqual(saved["url"], "https://a.com/1")
        self.assertIn("hubs", saved)
        self.assertIn("summary", saved)


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
