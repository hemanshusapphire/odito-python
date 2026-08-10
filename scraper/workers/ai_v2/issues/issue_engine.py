"""
V2 Issue Engine.

For every FAIL result in the ai_scores document, emit one ai_issues document.

Data flow:
    ai_scores (FAIL rules) → issue_engine → ai_issues

Domain-scoped rules (SCOPE = "domain" on the rule class) emit exactly ONE
issue per (project_id, job_id, rule_id) regardless of how many pages were
evaluated.  The issue is attached to the domain root URL with page_id=None
and score_id=None.

Page-scoped rules (SCOPE = "page", the default) behave as before: one issue
document per FAIL per page, keyed to the page's score_id for idempotent
re-runs.

call clear_domain_issues_for_job() once at the start of each pipeline run to
guarantee idempotency when the same job_id is reprocessed.
"""

import sys, os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
from bson import ObjectId

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import ai_issues  # V2 collection

from ..rules.base import RuleResult


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_object_id(value: Any) -> ObjectId | None:
    """Convert str → ObjectId; pass ObjectId through; return None for None."""
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return value
    return ObjectId(value)


def _root_url(url: str) -> str:
    """Return scheme://netloc for a page URL (the domain root)."""
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
    except Exception:
        pass
    return url


def _build_issue(
    result: RuleResult,
    url: str,
    score_id: Any,
    page_id: Any,
    project_id: Any,
    job_id: Any,
) -> dict[str, Any]:
    """Build a single ai_issues document from a FAIL RuleResult.

    score_id and page_id may be None for domain-scoped issues.
    """
    return {
        "project_id":        _to_object_id(project_id),
        "job_id":            _to_object_id(job_id),
        "page_id":           _to_object_id(page_id),
        "score_id":          _to_object_id(score_id),
        "url":               url,
        "scope":             result.scope,
        "rule_id":           result.rule_id,
        "hub":               result.hub,
        "card":              result.card,
        "severity":          result.severity,
        "issue_title":       result.issue_title,
        "issue_description": result.issue_description,
        "recommendation":    result.recommendation,
        "expected_impact":   result.expected_impact,
        "evidence":          result.evidence,
        "created_at":        datetime.now(timezone.utc),
        "version":           "v2",
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clear_domain_issues_for_job(project_id: Any, job_id: Any) -> None:
    """Delete all domain-scoped issues for this PROJECT before the pipeline loop.

    Called once per job at the start of run_v2_pipeline. Domain issues
    represent a project-wide fact (e.g. missing robots.txt), not a
    per-page one, so their identity is (project_id, rule_id) — one
    current issue per rule for the whole project, regardless of which
    job last computed it. Scoping this delete to job_id (the previous
    behavior) only cleared the CURRENT job's own domain issues, so any
    run under a NEW job_id — a repeated Full Audit, or a future
    single-URL verification run — left the previous run's domain issues
    in place, and since aggregate_project() now counts issues
    project-wide, they were double-counted.

    job_id is still accepted (call-site compatibility with
    run_v2_pipeline) but is no longer part of the delete filter.
    """
    ai_issues.delete_many({
        "project_id": _to_object_id(project_id),
        "scope":      "domain",
    })


def generate_and_save_issues(
    rule_results: dict[str, RuleResult],
    score_doc: dict[str, Any],
    page_doc: dict[str, Any],
    project_id: Any,
    job_id: Any,
    _domain_seen: set[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Convert every FAIL in rule_results into an ai_issues document and bulk-insert.

    Domain-scoped rules
    -------------------
    A domain rule creates at most ONE issue per (project_id, job_id, rule_id)
    across the entire job run.  Deduplication is handled by the caller-supplied
    ``_domain_seen`` set, which accumulates rule_ids across all page iterations.
    The issue is attached to the domain root URL (scheme://netloc) with
    page_id=None and score_id=None so it is never deleted by the per-page
    score cleanup below.

    Page-scoped rules
    -----------------
    One issue document per FAIL per page, keyed to score_id.  The
    delete_many at the start clears the previous run's issues for this page
    so reruns remain idempotent.

    Args:
        rule_results:  rule_id → RuleResult from registry.evaluate_page().
        score_doc:     saved ai_scores document (provides _id and url).
        page_doc:      saved ai_pages document (provides _id).
        project_id:    project ObjectId.
        job_id:        job ObjectId.
        _domain_seen:  mutable set shared across all pages in the job run.
                       Tracks which domain rule_ids already have an issue
                       document for this job.  Callers must pass the same
                       set instance for every page.  Defaults to a fresh
                       set (safe for single-page or test usage).

    Returns:
        List of newly inserted issue documents.
    """
    if _domain_seen is None:
        _domain_seen = set()

    score_id = score_doc.get("_id")
    page_id  = page_doc.get("_id")
    url      = score_doc.get("url", "")
    domain   = _root_url(url)

    # Remove previous page-level issues for this score so reruns are idempotent.
    # Domain issues (score_id=None) are intentionally not touched here.
    if score_id is not None:
        ai_issues.delete_many({"score_id": _to_object_id(score_id)})

    issue_docs: list[dict[str, Any]] = []

    for result in rule_results.values():
        if result.result != "FAIL":
            continue

        if result.scope == "domain":
            # Emit once per (project, job, rule) — skip if already created.
            if result.rule_id in _domain_seen:
                continue
            _domain_seen.add(result.rule_id)
            issue_docs.append(
                _build_issue(result, domain, None, None, project_id, job_id)
            )
        else:
            issue_docs.append(
                _build_issue(result, url, score_id, page_id, project_id, job_id)
            )

    if issue_docs:
        ai_issues.insert_many(issue_docs)

    return issue_docs
