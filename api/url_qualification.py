"""URL Qualification API route."""

from fastapi import APIRouter
from datetime import datetime, timezone

router = APIRouter()


@router.post("/jobs/url-qualification")
async def handle_url_qualification(job_data: dict):
    """Handle URL_QUALIFICATION job dispatched from Node.js."""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.url_qualification.worker import (
        UrlQualificationJob,
        execute_url_qualification,
    )

    job_id = job_data.get("jobId", "")
    print(
        f"[ROUTE] URL_QUALIFICATION handler entered | jobId={job_id} "
        f"| timestamp={datetime.now(timezone.utc).isoformat()}"
    )

    try:
        with completed_jobs_lock:
            if job_id in completed_jobs:
                print(f"[ROUTE] Skipping already completed URL_QUALIFICATION | jobId={job_id}")
                return {"status": "already_completed", "jobId": job_id}

        job = UrlQualificationJob(
            jobId=job_data.get("jobId", ""),
            projectId=job_data.get("projectId", ""),
            userId=job_data.get("userId", ""),
            sourceJobId=job_data.get("sourceJobId", ""),
        )

        print(f"[ROUTE] Calling execute_url_qualification | jobId={job_id}")
        result = await execute_url_qualification(job)

        with completed_jobs_lock:
            completed_jobs.add(job_id)

        return {"status": "accepted", "jobId": job_id}

    except Exception as exc:
        print(f"[ERROR] URL_QUALIFICATION handler failed | jobId={job_id} | reason=\"{exc}\"")
        return {"status": "error", "jobId": job_id, "error": str(exc)}
