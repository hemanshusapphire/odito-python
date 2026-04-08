"""Headless accessibility scanning API routes."""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timezone

router = APIRouter()


class HeadlessAccessibilityJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to TECHNICAL_DOMAIN job
    urls: Optional[List[str]] = []


@router.post("/jobs/headless-accessibility")
def handle_headless_accessibility(job: HeadlessAccessibilityJob):
    """Handle HEADLESS_ACCESSIBILITY job dispatched from Node.js"""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.headless_accessibility.worker import execute_headless_accessibility

    print(f"[ROUTE] HEADLESS_ACCESSIBILITY handler entered | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")

    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed HEADLESS_ACCESSIBILITY job | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }

        print(f"[WORKER] HEADLESS_ACCESSIBILITY started | jobId={job.jobId} | urls={len(job.urls)} | timestamp={datetime.now(timezone.utc).isoformat()}")

        # Execute headless accessibility scanning
        print(f"[ROUTE] Calling execute_headless_accessibility | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")
        result = execute_headless_accessibility(job)
        print(f"[ROUTE] execute_headless_accessibility returned | jobId={job.jobId} | result={result.get('status', 'unknown')} | timestamp={datetime.now(timezone.utc).isoformat()}")

        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)

        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "HEADLESS_ACCESSIBILITY job accepted and processing"
        }

    except Exception as e:
        print(f"[ERROR] HEADLESS_ACCESSIBILITY handler failed | jobId={job.jobId} | reason=\"{str(e)}\" | timestamp={datetime.now(timezone.utc).isoformat()}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
