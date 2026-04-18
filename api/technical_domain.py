"""Technical domain data collection API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

class TechnicalDomainJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    domain: str

@router.post("/jobs/technical-domain")
def handle_technical_domain(job: TechnicalDomainJob):
    """Handle TECHNICAL_DOMAIN job dispatched from Node.js"""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.technical_domain.worker import execute_technical_domain
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed TECHNICAL_DOMAIN job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[HANDLER] TECHNICAL_DOMAIN job received | jobId={job.jobId} | domain={job.domain}")
        
        # Execute technical domain data collection
        result = execute_technical_domain(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "TECHNICAL_DOMAIN job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] TECHNICAL_DOMAIN handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
