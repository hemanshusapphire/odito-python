"""SEO scoring API routes."""

from typing import Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from scraper.shared.schema import validate_seo_scoring_input

router = APIRouter()

class SeoScoringJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_ANALYSIS job
    # P2-002: optional URL-scope filter, same convention as PageAnalysisJob
    # (P2-001) / PageScrapingJob / HeadlessAccessibilityJob. Empty/absent
    # means "no filter" — Full Audit never sets this and is unaffected.
    urls: Optional[List[str]] = []
    # F4-016: set only for a page belonging to a Verification Batch — tells
    # execute_seo_scoring_logic to defer Phase 7 (website-level scoring) to
    # the PROJECT_SEO_AGGREGATION job instead of running it inline.
    batchId: Optional[str] = None

class ProjectSeoAggregationJob(BaseModel):
    """F4-016: one per Verification Batch, never one per URL."""
    jobId: str
    projectId: str
    userId: str
    batchId: Optional[str] = None
    sourceJobId: Optional[str] = None

@router.post("/jobs/seo-scoring")
def handle_seo_scoring(job: SeoScoringJob):
    """Handle SEO_SCORING job dispatched from Node.js"""
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.seo_scoring.seo_scoring import execute_seo_scoring_logic
    from scraper.shared.utils import send_completion_callback, send_failure_callback
    
    try:
        # Validate input data
        validate_seo_scoring_input(job.dict())
        
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed SEO_SCORING job {job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"🔍 Worker picked SEO_SCORING job")
        
        # Execute SEO scoring immediately (no polling loop)
        result = execute_seo_scoring_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "SEO_SCORING job accepted and processing"
        }
        
    except Exception as e:
        print(f"❌ Failed to handle SEO_SCORING job {job.jobId}: {str(e)}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }

@router.post("/jobs/project-seo-aggregation")
def handle_project_seo_aggregation(job: ProjectSeoAggregationJob):
    """Handle PROJECT_SEO_AGGREGATION job (F4-016) — dispatched from Node.js
    (PUSH mode) or claimed directly via main.py's polling loop (PULL mode),
    same dual entry-point convention as handle_seo_scoring above."""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.seo_scoring.seo_scoring import execute_project_seo_aggregation_logic

    try:
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed PROJECT_SEO_AGGREGATION job {job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }

        print(f"🔍 Worker picked PROJECT_SEO_AGGREGATION job")

        execute_project_seo_aggregation_logic(job)

        with completed_jobs_lock:
            completed_jobs.add(job.jobId)

        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "PROJECT_SEO_AGGREGATION job accepted and processing"
        }

    except Exception as e:
        print(f"❌ Failed to handle PROJECT_SEO_AGGREGATION job {job.jobId}: {str(e)}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
