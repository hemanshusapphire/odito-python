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
    Aggregate all current page scores for this project into a project-level document.

    Reads by project_id alone (NOT job_id): ai_scores/ai_issues hold the
    CURRENT state per (project, url) / (project, page), so a job_id-scoped
    read here would only reflect whichever job most recently touched each
    page rather than the whole project — which is exactly wrong for a
    future single-URL verification run. job_id is still recorded on the
    output document as provenance ("which job triggered this aggregation").

    Args:
        project_id: ObjectId of the project.
        job_id:     ObjectId of the job run that triggered this aggregation.

    Returns:
        The saved ai_projects document.
    """
    pid = ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id
    jid = ObjectId(job_id)     if not isinstance(job_id, ObjectId)     else job_id

    page_score_docs = list(ai_scores.find({"project_id": pid}))
    pages_scored    = len(page_score_docs)
    print(f"[V2 AGGREGATOR] Scores found: {pages_scored}")

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

            # Every page evaluates the same rule set, so total_rules_sum == rule_count × pages_scored.
            # Integer division is always exact here.
            rule_count      = (total_rules_sum // pages_scored) if pages_scored else 0
            checks_executed = total_rules_sum                       # rule_count × pages_scored
            total_failed    = total_rules_sum - total_passed_sum

            card_blocks[card] = {
                "score":           card_score,
                # ── legacy fields (kept for backward compatibility) ──────────────
                "total_passed":    total_passed_sum,
                "total_rules":     total_rules_sum,
                # ── new breakdown fields ──────────────────────────────────────────
                "rule_count":      rule_count,
                "pages_scored":    pages_scored,
                "checks_executed": checks_executed,
                "total_failed":    total_failed,
            }

        card_scores = [c["score"] for c in card_blocks.values() if c["total_rules"] > 0]
        hubs_out[hub] = {
            "score": _avg(card_scores),
            "cards": card_blocks,
        }

    # Issue summary from ai_issues — project-wide, same reasoning as the
    # ai_scores read above: a job_id-scoped count would only reflect the
    # most recent job's issues, not the whole project's current issues.
    severity_counts: dict[str, int] = {}
    for sev in ("critical", "high", "medium", "low"):
        severity_counts[sev] = ai_issues.count_documents({
            "project_id": pid,
            "severity":   sev,
        })
    total_issues = sum(severity_counts.values())

    # overall_score = average of the three hub scores
    aiso_score = hubs_out.get("aiso", {}).get("score", 0)
    aeo_score  = hubs_out.get("aeo",  {}).get("score", 0)
    geo_score  = hubs_out.get("geo",  {}).get("score", 0)
    overall_score = round((aiso_score + aeo_score + geo_score) / 3, 2)

    print(f"[V2 AGGREGATOR] overall_score={overall_score}  AISO={aiso_score}  AEO={aeo_score}  GEO={geo_score}")

    doc = {
        "project_id":    pid,
        "job_id":        jid,
        "version":       "v2",
        "computed_at":   datetime.now(timezone.utc),
        "pages_scored":  pages_scored,
        "overall_score": overall_score,
        "hubs":          hubs_out,
        "issues_summary": {
            "total":    total_issues,
            **severity_counts,
        },
    }

    # Identity is project_id alone — one CURRENT aggregate doc per project.
    # Every Node-side reader already fetches by project_id sorted by
    # computed_at desc; keeping job_id in the filter let a new job_id create
    # a second "latest" doc that silently superseded the real aggregate.
    ai_projects.find_one_and_update(
        filter={"project_id": pid},
        update={"$set": doc},
        upsert=True,
    )

    saved = ai_projects.find_one({"project_id": pid})
    return saved
