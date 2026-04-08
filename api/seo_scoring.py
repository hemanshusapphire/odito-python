"""SEO scoring API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from scraper.shared.schema import validate_seo_scoring_input

router = APIRouter()

class SeoScoringJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_ANALYSIS job

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
