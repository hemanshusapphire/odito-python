"""CRAWL_GRAPH job API route."""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone

router = APIRouter()


class CrawlGraphJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_SCRAPING job


@router.post("/jobs/crawl-graph")
def handle_crawl_graph(job: CrawlGraphJob):
    """Handle CRAWL_GRAPH job dispatched from Node.js"""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.crawl_graph.crawl_graph_worker import execute_crawl_graph

    print(f"[ROUTE] CRAWL_GRAPH handler entered | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")

    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed CRAWL_GRAPH job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }

        print(f"[WORKER] CRAWL_GRAPH started | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")

        # Execute crawl graph analysis
        result = execute_crawl_graph(job)

        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)

        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "CRAWL_GRAPH job accepted and processing"
        }

    except Exception as e:
        print(f"[ERROR] CRAWL_GRAPH handler failed | jobId={job.jobId} | reason=\"{str(e)}\" | timestamp={datetime.now(timezone.utc).isoformat()}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
