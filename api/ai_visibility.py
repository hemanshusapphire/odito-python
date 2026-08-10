"""AI Visibility API — V2 pipeline only."""

import os
import traceback
from datetime import datetime
from typing import Optional, List
from urllib.parse import urlparse, urlunparse

import requests
from bson.objectid import ObjectId
from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter()


def _normalize_url(url: str) -> str:
    """Lowercase hostname, strip trailing slash, drop fragment."""
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        path = p.path.rstrip("/") or "/"
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", p.query, ""))
    except Exception:
        return url.strip()


def _dedup_pages(pages: list[dict]) -> list[dict]:
    """
    Collapse duplicate URLs from seo_page_data into one best-quality doc per URL.

    Preference order (higher = better):
        1. extraction_status == "SUCCESS"
        2. raw_html present

    Logs every duplicate found so nothing is silently dropped.
    """
    seen: dict[str, dict] = {}

    def _quality(doc: dict) -> tuple[int, int]:
        return (
            1 if doc.get("extraction_status") == "SUCCESS" else 0,
            1 if doc.get("raw_html") else 0,
        )

    for page in pages:
        norm = _normalize_url(page.get("url", ""))
        if norm not in seen:
            seen[norm] = page
        else:
            existing = seen[norm]
            if _quality(page) > _quality(existing):
                print(
                    f"[AI_V2] DEDUP: replacing '{existing.get('url')}' "
                    f"with higher-quality '{page.get('url')}'"
                )
                seen[norm] = page
            else:
                print(
                    f"[AI_V2] DEDUP: dropping duplicate '{page.get('url')}' "
                    f"(kept '{existing.get('url')}')"
                )

    return list(seen.values())


class AIVisibilityJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    domain: Optional[str] = ""
    sourceJobId: Optional[str] = ""
    input_data: Optional[dict] = None
    # P2-003: optional URL-scope filter, same dedicated top-level-field
    # convention as PageAnalysisJob (P2-001) / SeoScoringJob (P2-002) — not
    # nested inside input_data, for consistency across all three Phase 2
    # workers. Empty/absent means "no filter"; Full Audit never sets this.
    urls: Optional[List[str]] = []
    # F4-016: set only for a page belonging to a Verification Batch — tells
    # run_v2_pipeline to defer Step 6 (project aggregation) to the
    # PROJECT_AI_AGGREGATION job instead of running it inline.
    batchId: Optional[str] = None


class ProjectAiAggregationJob(BaseModel):
    """F4-016: one per Verification Batch, never one per URL."""
    jobId: str
    projectId: str
    userId: str
    batchId: Optional[str] = None
    sourceJobId: Optional[str] = None


def _notify_node(job_id: str, endpoint: str, payload: dict) -> None:
    """Fire-and-forget callback to Node.js backend."""
    node_url = os.environ.get("NODE_BACKEND_URL")
    if not node_url:
        return
    try:
        requests.post(f"{node_url}/api/jobs/{job_id}/{endpoint}", json=payload, timeout=10)
    except Exception:
        pass


def _send_progress(job_id: str, percentage: int, step: str, message: str) -> None:
    node_url = os.environ.get("NODE_BACKEND_URL")
    if not node_url:
        return
    try:
        requests.post(
            f"{node_url}/api/jobs/{job_id}/progress",
            json={"percentage": percentage, "step": step, "message": message},
            timeout=5,
        )
    except Exception:
        pass


def execute_ai_visibility_v2(job: AIVisibilityJob) -> dict:
    """
    Execute the V2 AI pipeline for a project.

    Data flow:
        seo_page_data  →  run_v2_pipeline()  →  ai_pages / ai_scores / ai_issues / ai_projects
    """
    start = datetime.utcnow()
    print(f"[AI_V2] Starting | jobId={job.jobId} | projectId={job.projectId}")

    try:
        from db import seo_page_data
        from scraper.workers.ai_v2.pipeline import run_v2_pipeline
        from scraper.workers.ai_v2.domain_report_adapter import build_v2_domain_report
        from scraper.workers.ai_v2.extraction.site_structure import build_site_structure_lookup

        project_oid = ObjectId(job.projectId)

        # Fetch ALL seo_page_data docs for this project — no status or html filter.
        # The extractor handles missing raw_html gracefully (returns early from
        # _enrich_from_raw_html). Filtering here was silently dropping pages.
        #
        # P2-003: optional URL-scope filter. Empty/absent job.urls (the
        # default) leaves this query byte-identical to Full Audit's — same
        # dict shape, same .sort("url", 1) (untouched, so match ordering is
        # whatever it already was either way). A non-empty list narrows via
        # $in, matching the exact convention already used by
        # PageAnalysisJob/SeoScoringJob (P2-001/P2-002): an empty list is
        # treated the same as "not provided".
        page_query = {"projectId": project_oid}
        url_filter = getattr(job, 'urls', None)
        if url_filter:
            page_query["url"] = {"$in": url_filter}

        pages = list(
            seo_page_data.find(page_query)
            .sort("url", 1)
        )

        print(f"[AI_V2] Total seo_page_data pages fetched: {len(pages)}")

        # Deduplicate: seo_page_data can contain multiple docs for the same URL
        # (retries, trailing-slash variants, http/https pairs).  Collapsing here
        # ensures 1 ai_pages + 1 ai_scores doc per unique URL.
        pages = _dedup_pages(pages)
        print(f"[AI_V2] Unique URLs after deduplication: {len(pages)}")

        if not pages:
            print("[AI_V2] No scraped pages found — aborting")
            _notify_node(
                job.jobId,
                "complete",
                {"stats": {"pages_processed": 0, "successful_pages": 0, "failed_pages": 0}},
            )
            return {"status": "no_pages", "jobId": job.jobId, "stats": {"pages_processed": 0}}

        _send_progress(job.jobId, 10, "V2 Pipeline", f"Processing {len(pages)} pages")

        domain_report = build_v2_domain_report(job.projectId)
        # Loaded once here (single seo_crawl_graph query) and threaded through
        # the whole pipeline — every page's extraction reuses this same dict
        # instead of querying the crawl graph itself. Mirrors how domain_report
        # is already loaded once and threaded through.
        site_structure_lookup = build_site_structure_lookup(job.projectId)

        project_doc = run_v2_pipeline(
            project_id=job.projectId,
            job_id=job.jobId,
            page_docs=pages,
            domain_report=domain_report,
            site_structure_lookup=site_structure_lookup,
            skip_project_aggregation=bool(getattr(job, 'batchId', None)),
        )

        duration_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
        pages_scored = project_doc.get("pages_scored", len(pages))
        issues_total = project_doc.get("issues_summary", {}).get("total", 0)

        stats = {
            "pages_processed": pages_scored,
            "successful_pages": pages_scored,
            "failed_pages": 0,
            "issues_found": issues_total,
            "duration_ms": duration_ms,
            "pipeline_version": "v2",
        }

        _notify_node(job.jobId, "complete", {"stats": stats, "result_data": stats})

        print(f"[AI_V2] Completed | jobId={job.jobId} | pages={pages_scored} | issues={issues_total} | {duration_ms}ms")
        return {"status": "success", "jobId": job.jobId, "stats": stats}

    except Exception as exc:
        duration_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
        print(f"[AI_V2] Failed | jobId={job.jobId} | error={exc}")
        print(traceback.format_exc())
        _notify_node(job.jobId, "fail", {"error": str(exc)})
        return {"status": "error", "jobId": job.jobId, "message": str(exc),
                "stats": {"pages_processed": 0, "duration_ms": duration_ms}}


def process_ai_visibility_core(job: AIVisibilityJob) -> dict:
    """Shared logic for both HTTP route and polling-loop handler."""
    from main import completed_jobs, completed_jobs_lock

    with completed_jobs_lock:
        if job.jobId in completed_jobs:
            print(f"[AI_V2] Skipping already-completed job | jobId={job.jobId}")
            return {"status": "already_completed", "jobId": job.jobId}

    result = execute_ai_visibility_v2(job)

    with completed_jobs_lock:
        completed_jobs.add(job.jobId)

    return result


@router.post("/jobs/ai-visibility")
async def api_ai_visibility(request: Request):
    """HTTP route for AI_VISIBILITY (dispatched from Node.js)."""
    try:
        body = await request.json()

        missing = [f for f in ("jobId", "projectId", "userId") if not body.get(f)]
        if missing:
            return {"status": "error", "error": f"Missing fields: {missing}"}

        try:
            job = AIVisibilityJob(**body)
        except Exception as e:
            return {"status": "error", "error": f"Validation failed: {e}"}

        return process_ai_visibility_core(job)

    except Exception as exc:
        return {"status": "error", "error": str(exc), "traceback": traceback.format_exc()}


async def handle_ai_visibility(job: AIVisibilityJob) -> dict:
    """Polling-loop handler for AI_VISIBILITY jobs."""
    try:
        return process_ai_visibility_core(job)
    except Exception as exc:
        print(f"[AI_V2] Handler error | jobId={job.jobId} | error={exc}")
        return {"status": "error", "jobId": job.jobId, "error": str(exc)}


def execute_project_ai_aggregation_logic(job: ProjectAiAggregationJob) -> dict:
    """PROJECT_AI_AGGREGATION (F4-016) — project-level AI visibility
    aggregation, run exactly once per Verification Batch by chainingEngine's
    barrier, instead of once per URL. Calls aggregate_project directly
    (unchanged) rather than going through run_v2_pipeline's per-page steps.
    """
    from scraper.workers.ai_v2.aggregation.project_aggregator import aggregate_project

    print(f"[AI_V2] PROJECT_AI_AGGREGATION started | jobId={job.jobId} | projectId={job.projectId}")

    try:
        project_doc = aggregate_project(job.projectId, job.jobId)

        stats = {
            "overallScore": project_doc.get("overall_score", 0),
            "pagesScored": project_doc.get("pages_scored", 0),
            "issuesFound": project_doc.get("issues_summary", {}).get("total", 0),
        }

        _notify_node(job.jobId, "complete", {"stats": stats, "result_data": stats})
        print(f"[AI_V2] PROJECT_AI_AGGREGATION completed | jobId={job.jobId} | overall={stats['overallScore']}")
        return {"status": "success", "jobId": job.jobId, "stats": stats}

    except Exception as exc:
        print(f"[AI_V2] PROJECT_AI_AGGREGATION failed | jobId={job.jobId} | error={exc}")
        print(traceback.format_exc())
        _notify_node(job.jobId, "fail", {"error": str(exc)})
        return {"status": "error", "jobId": job.jobId, "message": str(exc)}


@router.post("/jobs/project-ai-aggregation")
def handle_project_ai_aggregation(job: ProjectAiAggregationJob) -> dict:
    """Handle PROJECT_AI_AGGREGATION job (F4-016) — dispatched from Node.js
    (PUSH mode) or claimed directly via main.py's polling loop (PULL mode),
    same dual entry-point convention as handle_ai_visibility above."""
    from main import completed_jobs, completed_jobs_lock

    with completed_jobs_lock:
        if job.jobId in completed_jobs:
            print(f"[AI_V2] Skipping already-completed job | jobId={job.jobId}")
            return {"status": "already_completed", "jobId": job.jobId}

    result = execute_project_ai_aggregation_logic(job)

    with completed_jobs_lock:
        completed_jobs.add(job.jobId)

    return result
