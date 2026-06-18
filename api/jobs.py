"""Job management API routes."""

from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
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


def _run_link_discovery(job: "LinkDiscoveryJob"):
    """Execute the crawl in the background (off the request lifecycle).

    Completion/failure is reported via the existing /complete and /fail
    callbacks — the HTTP request lifetime is NO LONGER the source of truth.
    Any exception is swallowed here because execute_link_discovery already
    posts a /fail callback to Node before raising.
    """
    try:
        from scraper.workers.seo.link_discovery.link_discovery import execute_link_discovery
        result = execute_link_discovery(job)
        print(f"✅ [PYTHON] Link discovery finished for jobId: {job.jobId} | result={result}")
    except Exception as e:
        # execute_link_discovery already fired the /fail callback; just log here.
        print(f"❌ [PYTHON] Background link discovery error for jobId {job.jobId}: {type(e).__name__}: {e}")


@router.post("/jobs/link-discovery")
def handle_link_discovery(job: LinkDiscoveryJob, background_tasks: BackgroundTasks):
    """Accept a LINK_DISCOVERY job and run it asynchronously.

    Returns 202 Accepted immediately so the Node dispatcher's request never
    blocks for the full crawl. This eliminates the 30s-timeout false-FAILED
    race (and the duplicate-crawl risk it caused): the worker reports the real
    outcome through the /complete and /fail callbacks instead.
    """
    print(f"🎯 [PYTHON] Link discovery accepted at {datetime.datetime.now()} | jobId={job.jobId} | url={job.main_url}")

    try:
        background_tasks.add_task(_run_link_discovery, job)
        return JSONResponse(
            status_code=202,
            content={"status": "accepted", "jobId": job.jobId, "message": "Link discovery started"},
        )
    except Exception as e:
        print(f"❌ [PYTHON] Failed to schedule link discovery: {type(e).__name__}: {e}")
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
