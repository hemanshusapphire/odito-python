"""Job management API routes."""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
import datetime

router = APIRouter()

class CancelJobRequest(BaseModel):
    jobId: str

class LinkDiscoveryJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    main_url: str

@router.post("/jobs/link-discovery")
def handle_link_discovery(job: LinkDiscoveryJob, request: Request):
    """Handle LINK_DISCOVERY job dispatched from Node.js"""
    
    # DEBUG: Log all incoming request details
    print(f"🎯 [PYTHON] Link discovery endpoint called at {datetime.datetime.now()}")
    print(f"🎯 [PYTHON] Request URL: {request.url}")
    print(f"🎯 [PYTHON] Client IP: {request.client.host}")
    print(f"🎯 [PYTHON] Request headers: {dict(request.headers)}")
    print(f"🎯 [PYTHON] Request body: {job.dict()}")
    print(f"🎯 [PYTHON] Job details - jobId: {job.jobId}, projectId: {job.projectId}, userId: {job.userId}")
    print(f"🎯 [PYTHON] Main URL: {job.main_url}")
    
    try:
        print(f"✅ [PYTHON] Starting link discovery for jobId: {job.jobId}")
        
        # Import here to avoid circular imports
        from scraper.workers.seo.link_discovery.link_discovery import execute_link_discovery
        
        result = execute_link_discovery(job)
        
        print(f"✅ [PYTHON] Link discovery completed for jobId: {job.jobId}")
        print(f"✅ [PYTHON] Result: {result}")
        
        return result
        
    except Exception as e:
        print(f"❌ [PYTHON] Error in link discovery: {str(e)}")
        print(f"❌ [PYTHON] Full error details: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/jobs/cancel")
def cancel_job(request: CancelJobRequest):
    """Mark a job as cancelled"""
    # Import here to avoid circular imports
    from main import cancelled_jobs, cancelled_jobs_lock
    
    with cancelled_jobs_lock:
        cancelled_jobs.add(request.jobId)
    print(f"🛑 Job {request.jobId} marked as cancelled by user")
    return {"success": True, "message": f"Job {request.jobId} cancelled"}

def is_job_cancelled(job_id: str) -> bool:
    """Check if a job has been cancelled"""
    # Import here to avoid circular imports
    from main import cancelled_jobs, cancelled_jobs_lock
    
    with cancelled_jobs_lock:
        return job_id in cancelled_jobs
