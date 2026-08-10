"""
V2 Scoring layer.

Receives rule results (rule_id → RuleResult) from the registry and produces
the ai_scores document.

Formula — no weights, no normalization:
    card_score  = (passed_rules / total_rules) * 100
    hub_score   = average of its card scores
    overall     = not stored at page level (project aggregator computes it)
"""

import sys, os
from datetime import datetime, timezone
from typing import Any
from bson import ObjectId
from pymongo import ReturnDocument

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import ai_scores  # V2 collection

from ..rules.base     import RuleResult
from ..rules.registry import RuleRegistry, HUB_CARD_MAP


def _card_score(rule_ids: list[str], results: dict[str, RuleResult]) -> dict[str, Any]:
    """Compute a single card's score block. SKIPPED rules are excluded from total and passed."""
    executed_ids = [rid for rid in rule_ids if results.get(rid) and results[rid].result != "SKIPPED"]
    skipped_ids  = [rid for rid in rule_ids if results.get(rid) and results[rid].result == "SKIPPED"]

    total  = len(executed_ids)
    passed = sum(1 for rid in executed_ids if results[rid].passed)
    score  = round((passed / total) * 100, 2) if total else 0.0

    rules_detail = {}
    for rid in rule_ids:
        r = results.get(rid)
        if r:
            entry: dict[str, Any] = {"result": r.result, "evidence": r.evidence}
            if r.result == "SKIPPED" and r.skipped_reason:
                entry["skipped_reason"] = r.skipped_reason
            rules_detail[rid] = entry

    return {
        "score":   score,
        "passed":  passed,
        "total":   total,
        "skipped": len(skipped_ids),
        "rules":   rules_detail,
    }


def _hub_score(card_scores: dict[str, dict]) -> float:
    """Hub score = simple average of its card percentage scores."""
    values = [c["score"] for c in card_scores.values() if c["total"] > 0]
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def build_score_document(
    page: dict[str, Any],
    results: dict[str, RuleResult],
    registry: RuleRegistry,
    page_id: Any,
    project_id: Any,
    job_id: Any,
) -> dict[str, Any]:
    """
    Build the complete ai_scores document for one page.

    Args:
        page:       ai_pages document (provides url).
        results:    rule_id → RuleResult from registry.evaluate_page().
        registry:   loaded RuleRegistry (provides card → rule_id mapping).
        page_id:    _id of the ai_pages document.
        project_id: project ObjectId.
        job_id:     job ObjectId.

    Returns:
        ai_scores document dict (not yet persisted).
    """
    hubs: dict[str, Any] = {}
    total_passed  = 0
    total_rules   = 0
    total_skipped = 0

    for hub, cards in HUB_CARD_MAP.items():
        card_blocks: dict[str, Any] = {}
        for card in cards:
            rule_ids   = registry.get_rules_for_card(hub, card)
            card_block = _card_score(rule_ids, results)
            card_blocks[card] = card_block
            total_passed  += card_block["passed"]
            total_rules   += card_block["total"]
            total_skipped += card_block["skipped"]

        hubs[hub] = {
            "score": _hub_score(card_blocks),
            "cards": card_blocks,
        }

    overall_pass_rate = round((total_passed / total_rules) * 100, 2) if total_rules else 0.0

    return {
        "project_id": ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id,
        "job_id":     ObjectId(job_id)      if not isinstance(job_id, ObjectId)      else job_id,
        "page_id":    ObjectId(page_id)     if not isinstance(page_id, ObjectId)     else page_id,
        "url":        page.get("url", ""),
        "version":    "v2",
        "scored_at":  datetime.now(timezone.utc),
        "hubs":       hubs,
        "summary": {
            "total_rules":      total_rules,
            "total_passed":     total_passed,
            "total_failed":     total_rules - total_passed,
            "total_skipped":    total_skipped,
            "overall_pass_rate": overall_pass_rate,
        },
    }


def score_and_save(
    page: dict[str, Any],
    results: dict[str, RuleResult],
    registry: RuleRegistry,
    project_id: Any,
    job_id: Any,
) -> dict[str, Any]:
    """
    Build the ai_scores document and upsert it into MongoDB.

    Returns the saved document (with _id).
    """
    page_id = page.get("_id")
    doc     = build_score_document(page, results, registry, page_id, project_id, job_id)
    url     = doc["url"]

    print(f"[V2] Saving ai_score: {url}")

    # Identity is (project_id, url) — NOT job_id. job_id still travels inside
    # `doc` via $set (provenance: "which job last scored this page"), but a
    # rescore under a different job_id updates this SAME page's doc in place
    # instead of creating a duplicate, so project-level aggregation never
    # sees stale, superseded score rows from earlier job_ids.
    saved = ai_scores.find_one_and_update(
        filter={
            "project_id": doc["project_id"],
            "url":        url,
        },
        update={"$set": doc},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if saved is None:
        saved = ai_scores.find_one({
            "project_id": doc["project_id"],
            "url":        url,
        })

    if saved is None:
        raise RuntimeError(f"[V2] ai_scores upsert returned no document for URL: {url}")

    print(f"[V2] Saved ai_score: {url} | _id={saved.get('_id')}")
    return saved
