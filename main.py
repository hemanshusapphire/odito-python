# Standard library imports
import os
import sys
import threading
import logging
import random
import string
from datetime import datetime
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Import centralized config
import env_config as centralized_config
from env_config import get_config, validate_environment

# Validate environment on startup
validate_environment()
config = get_config()

# Third-party imports
import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Local imports
from api.health import router as health_router
from api.jobs import router as jobs_router, is_job_cancelled
from api.scraping import router as scraping_router, handle_page_scraping
from api.analysis import router as analysis_router, handle_page_analysis
from api.performance import router as performance_router
from api.domain_performance import router as domain_performance_router
from api.seo_scoring import router as seo_scoring_router
from api.ai_visibility import router as ai_visibility_router
from api.ai_visibility_scoring_v2 import router as ai_visibility_scoring_v2_router, AIVisibilityScoringV2Job
from api.technical_domain import router as technical_domain_router
from api.headless_accessibility import router as headless_accessibility_router
from api.crawl_graph import router as crawl_graph_router
from api.keyword_research import router as keyword_research_router
from api.onboarding import router as onboarding_router
from scraper.workers.ai.ai_visibility.ai_visibility import execute_ai_visibility, AIVisibilityJob

# Configure logging to suppress third-party errors
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

app = FastAPI()

# Include API routers
app.include_router(health_router, prefix="/api", tags=["health"])
app.include_router(jobs_router, prefix="/api", tags=["jobs"])
app.include_router(scraping_router, prefix="/api", tags=["scraping"])
app.include_router(analysis_router, prefix="/api", tags=["analysis"])
app.include_router(performance_router, prefix="/api", tags=["performance"])
app.include_router(domain_performance_router, prefix="/api", tags=["domain_performance"])
app.include_router(seo_scoring_router, prefix="/api", tags=["seo_scoring"])
app.include_router(ai_visibility_router, prefix="/api", tags=["ai_visibility"])
app.include_router(ai_visibility_scoring_v2_router, prefix="/api", tags=["ai_visibility_scoring_v2"])
app.include_router(technical_domain_router, prefix="/api", tags=["technical_domain"])
app.include_router(headless_accessibility_router, prefix="/api", tags=["headless_accessibility"])
app.include_router(crawl_graph_router, prefix="/api", tags=["crawl_graph"])
app.include_router(keyword_research_router, prefix="/api", tags=["keyword_research"])
app.include_router(onboarding_router, prefix="/api", tags=["onboarding"])

# Global set to track cancelled jobs
cancelled_jobs = set()
cancelled_jobs_lock = threading.Lock()

# Global set to track completed jobs (defensive guard)
completed_jobs = set()
completed_jobs_lock = threading.Lock()

class CancelJobRequest(BaseModel):
    jobId: str

class JobClaimRequest(BaseModel):
    job_type: str
    worker_id: str

class JobClaimResponse(BaseModel):
    success: bool
    message: str
    data: dict | None = None

class JobCompletion(BaseModel):
    error: str | None = None
    stats: dict | None = None



def send_progress_update(job_id: str, percentage: int, step: str, message: str, subtext: str = None):
    """Send progress update to Node.js backend"""
    try:
        node_backend_url = config.get_service_url('node_backend')
        progress_url = f"{node_backend_url}/api/jobs/{job_id}/progress"
        
        payload = {
            "percentage": percentage,
            "step": step,
            "message": message,
            "subtext": subtext
        }
        
        response = requests.post(progress_url, json=payload, timeout=5)
        response.raise_for_status()
        
        print(f"📊 Progress update sent: {percentage}% - {step}")
        
    except Exception as e:
        print(f"⚠️ Failed to send progress update: {e}")
        # Don't raise exception - progress updates are non-critical

def generate_worker_id():
    """Generate a unique worker ID"""
    return f"worker-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"


@app.post("/jobs/ai-visibility")
def handle_ai_visibility(job: AIVisibilityJob):
    """Handle AI_VISIBILITY job dispatched dealt Node.js"""
    return execute_ai_visibility(job)

@app.post("/jobs/ai-visibility-scoring")
def handle_ai_visibility_scoring(job: AIVisibilityScoringV2Job):
    """Handle AI_VISIBILITY_SCORING job dispatched dealt Node.js"""
    try:
        # Detailed logging of incoming request
        print(f"[AI_VISIBILITY_SCORING] Received job request")
        print(f"[AI_VISIBILITY_SCORING] Job ID: {job.jobId}")
        print(f"[AI_VISIBILITY_SCORING] Project ID: {job.projectId}")
        print(f"[AI_VISIBILITY_SCORING] User ID: {job.userId}")
        print(f"[AI_VISIBILITY_SCORING] Source Job ID: {job.sourceJobId}")
        
        # Import worker function
        from scraper.workers.ai.ai_scoring_v2.ai_scoring_v2_worker import execute_ai_visibility_scoring_logic
        
        print(f"[AI_VISIBILITY_SCORING] Starting scoring logic execution")
        
        # Execute scoring logic
        result = execute_ai_visibility_scoring_logic(job.dict())
        
        print(f"[AI_VISIBILITY_SCORING] Scoring logic completed with status: {result.get('status', 'unknown')}")
        
        if result.get("status") == "failed":
            error_detail = result.get("error", "Scoring failed")
            print(f"[AI_VISIBILITY_SCORING] Job failed: {error_detail}")
            raise HTTPException(status_code=500, detail=error_detail)
        
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions (already have proper status codes)
        raise
    except Exception as e:
        # Catch any other exceptions and return meaningful error instead of 500 crash
        error_msg = f"Unexpected error in AI_VISIBILITY_SCORING handler: {str(e)}"
        print(f"[AI_VISIBILITY_SCORING] ERROR: {error_msg}")
        import traceback
        print(f"[AI_VISIBILITY_SCORING] Stack trace: {traceback.format_exc()}")
        
        # Return proper error response instead of crashing
        return {
            "status": "failed",
            "error": error_msg,
            "jobId": getattr(job, 'jobId', 'unknown')
        }

@app.post("/workers/claim")
def claim_job(request: JobClaimRequest):
    try:
        print(f"🔄 Worker {request.worker_id} requesting job of type: {request.job_type}")
        
        # DEBUG: Add these prints
        node_backend_url = config.get_service_url('node_backend')
        print(f"🔍 DEBUG: NODE_BACKEND_URL = '{node_backend_url}'")
        
        node_url = f"{node_backend_url}/api/workers/claim"
        print(f"🔍 DEBUG: Final node_url = '{node_url}'")
        
        claim_payload = {
            "job_type": request.job_type,
            "worker_id": request.worker_id
        }
        print(f"🔍 DEBUG: claim_payload = {claim_payload}")
        
        print(f"📡 Sending claim request to: {node_url}")
        print(f"📦 Payload: {claim_payload}")
        
        response = requests.post(node_url, json=claim_payload, timeout=10)
        print(f"🔍 DEBUG: Response status = {response.status_code}")
        print(f"🔍 DEBUG: Response text = {response.text}")
        response.raise_for_status()
        
        result = response.json()
        print(f"📥 Response from Node: {result}")
        
        if result.get("success"):
            job_data = result.get("data", {})
            print(f"✅ Job claimed successfully: {job_data}")
            return JobClaimResponse(
                success=True,
                message="Job claimed successfully",
                data=job_data
            )
        else:
            print(f"❌ Failed to claim job: {result.get('message', 'Unknown error')}")
            return JobClaimResponse(
                success=False,
                message=result.get('message', 'Failed to claim job'),
                data=None
            )
            
    except Exception as e:
        print(f"❌ Error claiming job: {str(e)}")
        return JobClaimResponse(
            success=False,
            message=f"Error claiming job: {str(e)}",
            data=None
        )

def test_node_connection():
    """Test connection to Node.js backend manually"""
    import requests
    
    node_backend_url = config.get_service_url('node_backend')
    test_url = f"{node_backend_url}/api/workers/claim"
    test_payload = {"job_type": "LINK_DISCOVERY", "worker_id": "debug-test"}
    
    print(f"🧪 MANUAL TEST: URL = {test_url}")
    print(f"🧪 MANUAL TEST: Payload = {test_payload}")
    
    try:
        response = requests.post(test_url, json=test_payload, timeout=10)
        print(f"🧪 MANUAL TEST: Status = {response.status_code}")
        print(f"🧪 MANUAL TEST: Response = {response.text}")
        print(f"🧪 MANUAL TEST: Headers = {dict(response.headers)}")
    except Exception as e:
        print(f"🧪 MANUAL TEST: Exception = {e}")

if __name__ == "__main__":
    test_node_connection()
    
    worker_id = sys.argv[1] if len(sys.argv) > 1 else generate_worker_id()
    
    print(f"🤖 Starting Python worker with ID: {worker_id}")
    print("🚀 Worker ready to receive dispatched jobs from Node.js")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
