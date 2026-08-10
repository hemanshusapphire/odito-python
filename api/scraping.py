"""Page scraping API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl
from bson.objectid import ObjectId
from datetime import datetime

router = APIRouter()

class PageScrapingJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    urls: list[str] = []
    canonical_urls: list[str] = []  # Canonical URL set from URL_QUALIFICATION (preferred)
    sourceJobId: str | None = None
    # URL-level retry (chainingEngine._maybeCreatePageScrapingRetryChunks):
    # is_retry/retry_round distinguish a retry chunk (canonical_urls sourced
    # from seo_page_failures, not URL_SELECTION) from an initial chunk.
    # retry_round is 1-indexed (1 = first retry round); 0 means "not a retry".
    is_retry: bool = False
    retry_round: int = 0

@router.post("/jobs/page-scraping")
def handle_page_scraping(job: PageScrapingJob):
    """Handle PAGE_SCRAPING job dispatched from Node.js"""
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.page_scraping.page_scraping import execute_page_scraping_logic
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed PAGE_SCRAPING job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[WORKER] PAGE_SCRAPING started | jobId={job.jobId}")
        
        # Execute page scraping immediately (no polling loop)
        result = execute_page_scraping_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "PAGE_SCRAPING job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] PAGE_SCRAPING handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
