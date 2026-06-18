"""
V2 Project Aggregator.

Reads all ai_scores documents for a (project_id, job_id) and computes
project-level hub/card averages, then upserts into ai_projects.

Data flow:
    ai_scores (all pages) → project_aggregator → ai_projects
"""

import sys, os
from datetime import datetime, timezone
from typing import Any
from bson import ObjectId

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import ai_scores, ai_issues, ai_projects  # V2 collections

from ..rules.registry import HUB_CARD_MAP


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 2) if values else 0.0


def aggregate_project(project_id: Any, job_id: Any) -> dict[str, Any]:
    """
    Aggregate all page scores for this job run into a project-level document.

    Args:
        project_id: ObjectId of the project.
        job_id:     ObjectId of the job run.

    Returns:
        The saved ai_projects document.
    """
    pid = ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id
    jid = ObjectId(job_id)     if not isinstance(job_id, ObjectId)     else job_id

    page_score_docs = list(ai_scores.find({"project_id": pid, "job_id": jid}))
    pages_scored    = len(page_score_docs)

    # Accumulate per-hub, per-card pass/total counts across all pages.
    hub_card_passed: dict[str, dict[str, list[int]]] = {}
    hub_card_total:  dict[str, dict[str, list[int]]] = {}

    for hub, cards in HUB_CARD_MAP.items():
        hub_card_passed[hub] = {c: [] for c in cards}
        hub_card_total[hub]  = {c: [] for c in cards}

    for score_doc in page_score_docs:
        for hub, cards in HUB_CARD_MAP.items():
            hub_data = score_doc.get("hubs", {}).get(hub, {})
            for card in cards:
                card_data = hub_data.get("cards", {}).get(card, {})
                hub_card_passed[hub][card].append(card_data.get("passed", 0))
                hub_card_total[hub][card].append(card_data.get("total", 0))

    # Build hub/card aggregation blocks.
    hubs_out: dict[str, Any] = {}
    for hub, cards in HUB_CARD_MAP.items():
        card_blocks: dict[str, Any] = {}
        for card in cards:
            total_passed_sum = sum(hub_card_passed[hub][card])
            total_rules_sum  = sum(hub_card_total[hub][card])
            card_score = round((total_passed_sum / total_rules_sum) * 100, 2) if total_rules_sum else 0.0
            card_blocks[card] = {
                "score":        card_score,
                "total_passed": total_passed_sum,
                "total_rules":  total_rules_sum,
            }

        card_scores = [c["score"] for c in card_blocks.values() if c["total_rules"] > 0]
        hubs_out[hub] = {
            "score": _avg(card_scores),
            "cards": card_blocks,
        }

    # Issue summary from ai_issues.
    severity_counts: dict[str, int] = {}
    for sev in ("critical", "high", "medium", "low"):
        severity_counts[sev] = ai_issues.count_documents({
            "project_id": pid,
            "job_id":     jid,
            "severity":   sev,
        })
    total_issues = sum(severity_counts.values())

    doc = {
        "project_id":   pid,
        "job_id":       jid,
        "version":      "v2",
        "computed_at":  datetime.now(timezone.utc),
        "pages_scored": pages_scored,
        "hubs":         hubs_out,
        "issues_summary": {
            "total":    total_issues,
            **severity_counts,
        },
    }

    ai_projects.find_one_and_update(
        filter={"project_id": pid, "job_id": jid},
        update={"$set": doc},
        upsert=True,
    )

    saved = ai_projects.find_one({"project_id": pid, "job_id": jid})
    return saved
