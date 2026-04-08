"""Page analysis API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson.objectid import ObjectId

router = APIRouter()

class PageAnalysisJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_SCRAPING job

@router.post("/jobs/page-analysis")
def handle_page_analysis(job: PageAnalysisJob):
    """Handle PAGE_ANALYSIS job dispatched from Node.js"""
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.page_analysis.page_analysis import execute_page_analysis_logic
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed PAGE_ANALYSIS job {job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"🔍 Worker picked PAGE_ANALYSIS job")
        
        # Execute page analysis immediately (no polling loop)
        result = execute_page_analysis_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "PAGE_ANALYSIS job accepted and processing"
        }
        
    except Exception as e:
        print(f"❌ Failed to handle PAGE_ANALYSIS job {job.jobId}: {str(e)}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
