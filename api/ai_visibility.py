"""AI Visibility API routes."""

from fastapi import APIRouter, HTTPException, Request
from scraper.workers.ai.ai_visibility.ai_visibility import AIVisibilityJob
import traceback

router = APIRouter()

@router.post("/jobs/ai-visibility")
async def handle_ai_visibility(request: Request):
    """Handle AI_VISIBILITY job dispatched from Node.js"""
    try:
        # Debug: Print raw request body
        body = await request.json()
        print("RAW BODY:", body)
        
        # Validate required fields before parsing
        required_fields = ['jobId', 'projectId', 'userId']
        missing_fields = [field for field in required_fields if field not in body or body[field] is None]
        
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            print(f"[ERROR] {error_msg}")
            return {
                "status": "error",
                "error": error_msg,
                "received_fields": list(body.keys())
            }
        
        # Convert to Pydantic model with error handling
        try:
            job = AIVisibilityJob(**body)
        except Exception as validation_error:
            print(f"[ERROR] Pydantic validation failed: {validation_error}")
            print(f"[ERROR] Validation error details: {traceback.format_exc()}")
            return {
                "status": "error",
                "error": f"Validation failed: {str(validation_error)}",
                "received_body": body
            }
        
        print("PARSED VALUES:")
        print(f"  jobId: {job.jobId}")
        print(f"  projectId: {job.projectId}")
        print(f"  userId: {job.userId}")
        
        # Import here to avoid circular imports
        from main import completed_jobs, completed_jobs_lock
        from scraper.workers.ai.ai_visibility.ai_visibility import execute_ai_visibility
        
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed AI_VISIBILITY job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[WORKER] AI_VISIBILITY started | jobId={job.jobId}")
        
        # Execute AI visibility immediately (no polling loop)
        result = execute_ai_visibility(job=job, aiProjectId=job.projectId)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        print(f"[WORKER] AI_VISIBILITY completed | jobId={job.jobId}")
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "AI_VISIBILITY job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] AI_VISIBILITY handler failed | reason=\"{str(e)}\"")
        print(f"[ERROR] Full traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
