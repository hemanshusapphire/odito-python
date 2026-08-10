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
        site_structure_lookup=site_structure_lookup,  # url -> {is_orphan, click_depth}
    )
"""

import sys, os, traceback
from typing import Any

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from .rules.loader                        import load_all_rules
from .extraction.extractor                import extract_and_save
from .scoring.scorer                      import score_and_save
from .issues.issue_engine                 import generate_and_save_issues, clear_domain_issues_for_job
from .aggregation.project_aggregator      import aggregate_project


def run_v2_pipeline(
    project_id: Any,
    job_id: Any,
    page_docs: list[dict[str, Any]],
    domain_report: dict[str, Any],
    site_structure_lookup: dict[str, dict] | None = None,
    skip_project_aggregation: bool = False,
) -> dict[str, Any]:
    """
    Run the full V2 pipeline for all pages in a job.

    Args:
        project_id:    ObjectId of the project.
        job_id:        ObjectId of the current job.
        page_docs:     List of seo_page_data documents (one per page).
        domain_report: Single domain_technical_reports document for the domain.
        site_structure_lookup: url -> {is_orphan, click_depth} map, loaded
            ONCE by the caller (see extraction.site_structure.build_site_structure_lookup)
            and reused for every page here — avoids one seo_crawl_graph query
            per page. Optional/defaults to {} so existing callers that don't
            pass it keep working unchanged (site_structure signals simply
            come back empty, and AISO-CV4/CV5 SKIP accordingly).
        skip_project_aggregation: F4-016 — True for a page belonging to a
            Verification Batch. Steps 1-5 (per-page extraction/scoring/issues)
            are unchanged; only Step 6 (aggregate_project) is deferred to the
            separate PROJECT_AI_AGGREGATION job, fired once per batch instead
            of once per page. Full Audit / single-URL verification (default
            False) are byte-identical to before.

    Returns:
        The ai_projects aggregation document (or a zeroed stub when
        skip_project_aggregation is True — no aggregation was persisted).
    """
    site_structure_lookup = site_structure_lookup or {}

    # Step 1 — Load rules (idempotent if called multiple times).
    registry = load_all_rules()

    print(f"[V2] Total seo_page_data pages fetched: {len(page_docs)}")

    # Clear any domain-scoped issues left from a previous run of this job_id.
    # Must happen before the loop so the first page can create fresh domain issues.
    clear_domain_issues_for_job(project_id, job_id)

    # Tracks which domain rule_ids have already emitted an issue this run.
    # Shared across all page iterations — passed into generate_and_save_issues.
    _domain_seen: set[str] = set()

    page_results: list[dict[str, Any]] = []

    for page_data in page_docs:
        url = page_data.get("url", "unknown")
        try:
            print(f"[V2] Processing page: {url}")

            # Step 2 — Extraction → ai_pages
            page_doc = extract_and_save(page_data, domain_report, project_id, job_id, site_structure_lookup)

            # Step 3 — Rule evaluation
            rule_results = registry.evaluate_page(page_doc)

            # Step 4 — Scoring → ai_scores
            score_doc = score_and_save(page_doc, rule_results, registry, project_id, job_id)

            # Step 5 — Issue generation → ai_issues
            issues = generate_and_save_issues(rule_results, score_doc, page_doc, project_id, job_id, _domain_seen)

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
            print(traceback.format_exc())

    ok_count    = sum(1 for r in page_results if r["status"] == "ok")
    error_count = sum(1 for r in page_results if r["status"] == "error")
    print(f"[V2] ai_pages generated:  {ok_count}")
    print(f"[V2] ai_scores generated: {ok_count}")
    if error_count:
        print(f"[V2] Pages failed (skipped from scoring): {error_count}")
        for r in page_results:
            if r["status"] == "error":
                print(f"  [V2] FAILED: {r['url']} — {r['error']}")

    # Step 6 — Project aggregation → ai_projects
    # F4-016: batched Verification runs defer this to the separate
    # PROJECT_AI_AGGREGATION job (fired once per batch) instead of running it
    # once per page — aggregate_project itself is unchanged either way.
    if skip_project_aggregation:
        print(f"[V2] Batched run — deferring project aggregation to PROJECT_AI_AGGREGATION")
        project_doc = {
            "hubs": {},
            "overall_score": 0,
            "issues_summary": {"total": 0},
            "pages_scored": ok_count,
        }
    else:
        project_doc = aggregate_project(project_id, job_id)

    aiso    = project_doc["hubs"].get("aiso", {}).get("score", 0)
    aeo     = project_doc["hubs"].get("aeo",  {}).get("score", 0)
    geo     = project_doc["hubs"].get("geo",  {}).get("score", 0)
    overall = project_doc.get("overall_score", 0)

    print(
        f"✅ V2 pipeline complete — "
        f"{len(page_docs)} input | {ok_count} scored | {error_count} failed | "
        f"overall={overall}% AISO={aiso}% AEO={aeo}% GEO={geo}% | "
        f"Issues={project_doc['issues_summary']['total']}"
    )

    return project_doc
