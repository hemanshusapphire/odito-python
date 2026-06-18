"""
V2 Issue Engine.

For every FAIL result in the ai_scores document, emit one ai_issues document.

Data flow:
    ai_scores (FAIL rules) → issue_engine → ai_issues
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

from db import ai_issues  # V2 collection

from ..rules.base import RuleResult


def _build_issue(
    result: RuleResult,
    url: str,
    score_id: Any,
    page_id: Any,
    project_id: Any,
    job_id: Any,
) -> dict[str, Any]:
    """Build a single ai_issues document from a FAIL RuleResult."""
    return {
        "project_id":       ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id,
        "job_id":           ObjectId(job_id)      if not isinstance(job_id, ObjectId)      else job_id,
        "page_id":          ObjectId(page_id)     if not isinstance(page_id, ObjectId)     else page_id,
        "score_id":         ObjectId(score_id)    if not isinstance(score_id, ObjectId)    else score_id,
        "url":              url,
        "rule_id":          result.rule_id,
        "hub":              result.hub,
        "card":             result.card,
        "severity":         result.severity,
        "issue_title":      result.issue_title,
        "issue_description": result.issue_description,
        "recommendation":   result.recommendation,
        "expected_impact":  result.expected_impact,
        "evidence":         result.evidence,
        "created_at":       datetime.now(timezone.utc),
        "version":          "v2",
    }


def generate_and_save_issues(
    rule_results: dict[str, RuleResult],
    score_doc: dict[str, Any],
    page_doc: dict[str, Any],
    project_id: Any,
    job_id: Any,
) -> list[dict[str, Any]]:
    """
    Convert every FAIL in rule_results into an ai_issues document and bulk-upsert.

    Args:
        rule_results: rule_id → RuleResult from registry.evaluate_page().
        score_doc:    saved ai_scores document (provides _id and url).
        page_doc:     saved ai_pages document (provides _id).
        project_id:   project ObjectId.
        job_id:       job ObjectId.

    Returns:
        List of saved issue documents.
    """
    score_id = score_doc.get("_id")
    page_id  = page_doc.get("_id")
    url      = score_doc.get("url", "")

    # Remove previous issues for this (score_id) so a re-run is idempotent.
    ai_issues.delete_many({"score_id": ObjectId(score_id) if not isinstance(score_id, ObjectId) else score_id})

    issue_docs = []
    for result in rule_results.values():
        if result.result == "FAIL":
            issue_docs.append(
                _build_issue(result, url, score_id, page_id, project_id, job_id)
            )

    if issue_docs:
        ai_issues.insert_many(issue_docs)

    return issue_docs
