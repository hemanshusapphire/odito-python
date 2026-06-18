"""
V2 Pipeline — entry point.

Orchestrates the full data flow for one job run:

    Scraping output (seo_page_data + domain_technical_reports)
        ↓
    Extraction  → ai_pages
        ↓
    Rule Engine → ai_scores
        ↓
    Issue Engine → ai_issues
        ↓
    Project Aggregation → ai_projects

Usage (called from the AI_VISIBILITY worker):

    from scraper.workers.ai_v2.pipeline import run_v2_pipeline

    run_v2_pipeline(
        project_id=job["projectId"],
        job_id=job["_id"],
        page_docs=page_scraping_results,       # list of seo_page_data docs
        domain_report=domain_technical_report,  # single domain_technical_reports doc
    )
"""

import sys, os
from typing import Any

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from .rules.loader                        import load_all_rules
from .extraction.extractor                import extract_and_save
from .scoring.scorer                      import score_and_save
from .issues.issue_engine                 import generate_and_save_issues
from .aggregation.project_aggregator      import aggregate_project


def run_v2_pipeline(
    project_id: Any,
    job_id: Any,
    page_docs: list[dict[str, Any]],
    domain_report: dict[str, Any],
) -> dict[str, Any]:
    """
    Run the full V2 pipeline for all pages in a job.

    Args:
        project_id:    ObjectId of the project.
        job_id:        ObjectId of the current job.
        page_docs:     List of seo_page_data documents (one per page).
        domain_report: Single domain_technical_reports document for the domain.

    Returns:
        The ai_projects aggregation document.
    """
    # Step 1 — Load rules (idempotent if called multiple times).
    registry = load_all_rules()

    page_results: list[dict[str, Any]] = []

    for page_data in page_docs:
        url = page_data.get("url", "unknown")
        try:
            # Step 2 — Extraction → ai_pages
            page_doc = extract_and_save(page_data, domain_report, project_id, job_id)

            # Step 3 — Rule evaluation
            rule_results = registry.evaluate_page(page_doc)

            # Step 4 — Scoring → ai_scores
            score_doc = score_and_save(page_doc, rule_results, registry, project_id, job_id)

            # Step 5 — Issue generation → ai_issues
            issues = generate_and_save_issues(rule_results, score_doc, page_doc, project_id, job_id)

            page_results.append({
                "url":          url,
                "status":       "ok",
                "passed":       score_doc["summary"]["total_passed"],
                "failed":       score_doc["summary"]["total_failed"],
                "issues_count": len(issues),
            })
            print(f"  ✅ V2 [{url}] passed={score_doc['summary']['total_passed']} failed={score_doc['summary']['total_failed']}")

        except Exception as exc:
            page_results.append({"url": url, "status": "error", "error": str(exc)})
            print(f"  ❌ V2 [{url}] error: {exc}")

    # Step 6 — Project aggregation → ai_projects
    project_doc = aggregate_project(project_id, job_id)

    aiso = project_doc["hubs"].get("aiso", {}).get("score", 0)
    aeo  = project_doc["hubs"].get("aeo",  {}).get("score", 0)
    geo  = project_doc["hubs"].get("geo",  {}).get("score", 0)

    print(
        f"✅ V2 pipeline complete — "
        f"{len(page_docs)} pages | "
        f"AISO={aiso}% AEO={aeo}% GEO={geo}% | "
        f"Issues={project_doc['issues_summary']['total']}"
    )

    return project_doc
