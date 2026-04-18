# Standard library imports
import os
import sys
import threading
import logging
import random
import string
from datetime import datetime
from pathlib import Path

# 🔥🔥🔥 EXECUTION VERIFICATION 🔥🔥🔥
print("🔥🔥🔥 MAIN.PY EXECUTING - VERIFIED 🔥🔥🔥")
print("📂 FILE PATH:", __file__)
print("📁 CWD:", os.getcwd())
print("🧠 PROCESS ID:", os.getpid())
print("🐍 PYTHON PATH:", sys.executable)
print("📦 SYS PATH:", sys.path[:3])  # First 3 entries only
print("🔥🔥🔥 END VERIFICATION 🔥🔥🔥")

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
import asyncio
import httpx
from fastapi import FastAPI
from contextlib import asynccontextmanager
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
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown hooks"""
    # Startup
    print("🚀 STARTING BACKGROUND WORKER LOOP")
    print("🎯 LIFESPAN STARTUP - FastAPI starting up")
    enable_polling = os.getenv('ENABLE_POLLING', 'false').lower() == 'true'
    print(f"🔧 ENABLE_POLLING env var: {os.getenv('ENABLE_POLLING')}")
    print(f"🔧 enable_polling boolean: {enable_polling}")
    
    # Force start worker loop regardless of env var
    print("✅ FORCE STARTING poll_for_jobs()")
    asyncio.create_task(poll_for_jobs())
    print("[POLL] Job polling started")
    
    yield
    # Shutdown - close shared HTTP client
    try:
        from scraper.shared.http_client import close_http_client
        await close_http_client()
        print("[SHUTDOWN] Shared HTTP client closed")
    except Exception as e:
        print(f"[SHUTDOWN] Error closing HTTP client: {e}")

app = FastAPI(lifespan=lifespan)

# Thread-safe tracking for parallel jobs
running_parallel_jobs = set()
running_jobs_lock = threading.Lock()

PARALLEL_JOB_TYPES = {'PAGE_SCRAPING', 'HEADLESS_ACCESSIBILITY', 'CRAWL_GRAPH', 'AI_VISIBILITY'}

def run_job_in_thread(job, job_type):
    """Run a job in a separate thread with thread-safe logging"""
    thread_id = threading.current_thread().ident
    job_id = job['_id']
    
    print(f"[THREAD-{thread_id}] Starting {job_type} job | jobId={job_id}")
    
    try:
        # Run the async process_claimed_job in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(process_claimed_job(job))
        finally:
            loop.close()
        
        print(f"[THREAD-{thread_id}] Completed {job_type} job | jobId={job_id}")
    except Exception as e:
        print(f"[THREAD-{thread_id}] ERROR in {job_type} job | jobId={job_id} | error={str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Remove from running jobs
        with running_jobs_lock:
            if job_id in running_parallel_jobs:
                running_parallel_jobs.remove(job_id)

# PULL model polling function
async def poll_for_jobs():
    """Poll for jobs from MongoDB via Node.js API"""
    print("� POLLING LOOP STARTED")
    backend_url = config.get_service_url('backend')
    print(f"🌐 Backend URL: {backend_url}")
    
    # List of job types to poll for (in priority order)
    job_types = [
        'PAGE_SCRAPING',
        'HEADLESS_ACCESSIBILITY', 
        'PERFORMANCE_MOBILE',
        'PERFORMANCE_DESKTOP',
        'PAGE_ANALYSIS',
        'SEO_SCORING',
        'AI_VISIBILITY',
        'AI_VISIBILITY_SCORING',
        'TECHNICAL_DOMAIN',
        'CRAWL_GRAPH'
    ]
    
    print("📋 JOB TYPES LIST:", job_types)
    print("📋 TECHNICAL_DOMAIN IN LIST:", 'TECHNICAL_DOMAIN' in job_types)
    
    from scraper.shared.http_client import get_http_client
    
    while True:
        try:
            print("🔁 Polling for jobs...")
            print(" POLLING LOOP ITERATION START")
            print(f"[POLL DEBUG] job_types = {job_types}")
            for job_type in job_types:
                print(f"🔁 Trying job_type: {job_type}")
                print(f"[POLL DEBUG] Trying job_type = {job_type}")
                print(f"[POLL] Checking for job type: {job_type}")
                print(f"[POLL] Requesting job type: {job_type}")
                try:
                    claim_url = f"{backend_url}/api/jobs/claim"
                    print(f"🌐 Claim URL: {claim_url}?job_type={job_type}")
                    print(f"[POLL DEBUG] Claim URL = {claim_url}?job_type={job_type}")
                    print(f"[POLL] Claim API URL: {claim_url}")
                    print(f"[POLL] Claim params: job_type={job_type}")
                    http_client = get_http_client()
                    response = await http_client.get(
                        claim_url,
                        params={'job_type': job_type}
                    )
                    print(f"📥 Response status: {response.status_code}")
                    print(f"📥 Response body: {response.text}")
                    print(f"[POLL] Response for {job_type}: {response}")
                    print(f"[POLL] Response for {job_type}: status={response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        print(f"[POLL] Backend response for {job_type}: {data}")
                        if data.get('success') and data.get('data'):
                            # Backend returns {success: true, data: {job_id, jobType, ...}}
                            job_data = data['data']
                            job_id = job_data.get('job_id')
                            print(f"[POLL] Job FOUND for {job_type}: {job_id}")
                            print(f"[CLAIM] Claimed job: {job_id} | type: {job_type}")
                            # Construct job object from response fields
                            job = {
                                '_id': job_data.get('job_id'),
                                'jobType': job_data.get('jobType'),
                                'project_id': job_data.get('projectId'),
                                'input_data': job_data.get('input_data'),
                                'status': job_data.get('status')
                            }
                            print(f"[POLL] Claimed {job_type} job: {job['_id']}")
                            
                            # Check if this job should run in parallel
                            if job_type in PARALLEL_JOB_TYPES:
                                job_id = job['_id']
                                
                                # Check if job is already running (prevent duplicates)
                                with running_jobs_lock:
                                    if job_id in running_parallel_jobs:
                                        print(f"[POLL] Job {job_id} already running in parallel, skipping")
                                        continue
                                    running_parallel_jobs.add(job_id)
                                
                                # Run in background thread
                                thread = threading.Thread(
                                    target=run_job_in_thread,
                                    args=(job, job_type),
                                    daemon=True
                                )
                                thread.start()
                                print(f"[POLL] Started {job_type} in background thread | jobId={job_id}")
                                # Don't break - continue polling for other parallel jobs
                            else:
                                # Synchronous execution for other job types
                                await process_claimed_job(job)
                                break  # Process one job at a time, then restart cycle
                        else:
                            print(f"[POLL] No job in response for {job_type}")
                except Exception as e:
                    print(f"[POLL] Error polling for {job_type}: {e}")
            
            await asyncio.sleep(2)  # Poll every 2 seconds for faster job pickup
        except Exception as e:
            print("❌ Polling error:", e)
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)  # Wait before retrying

def normalize_job_to_model(job: dict, job_type: str):
    """Convert dict job to appropriate Pydantic model based on job type"""
    input_data = job.get('input_data', {})
    
    # Extract common fields with fallbacks
    job_id = job.get('_id') or job.get('job_id')
    project_id = job.get('project_id') or job.get('projectId')
    user_id = job.get('user_id') or job.get('userId') or 'unknown'
    
    if job_type == 'TECHNICAL_DOMAIN':
        from api.technical_domain import TechnicalDomainJob
        return TechnicalDomainJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            domain=input_data.get('domain')
        )
    elif job_type == 'PAGE_SCRAPING':
        from api.scraping import PageScrapingJob
        return PageScrapingJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            urls=input_data.get('urls', []),
            sourceJobId=input_data.get('source_job_id')
        )
    elif job_type == 'HEADLESS_ACCESSIBILITY':
        from api.headless_accessibility import HeadlessAccessibilityJob
        return HeadlessAccessibilityJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', ''),
            urls=input_data.get('urls', [])
        )
    elif job_type == 'PAGE_ANALYSIS':
        from api.analysis import PageAnalysisJob
        return PageAnalysisJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'PERFORMANCE_MOBILE':
        from api.performance import PerformanceMobileJob
        return PerformanceMobileJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'PERFORMANCE_DESKTOP':
        from api.performance import PerformanceDesktopJob
        return PerformanceDesktopJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'SEO_SCORING':
        from api.seo_scoring import SeoScoringJob
        return SeoScoringJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'AI_VISIBILITY':
        from scraper.workers.ai.ai_visibility.ai_visibility import AIVisibilityJob
        return AIVisibilityJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            domain=input_data.get('domain', ''),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'AI_VISIBILITY_SCORING':
        from api.ai_visibility_scoring_v2 import AIVisibilityScoringV2Job
        return AIVisibilityScoringV2Job(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'CRAWL_GRAPH':
        from api.crawl_graph import CrawlGraphJob
        return CrawlGraphJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    else:
        # Return dict as fallback for unknown job types
        print(f"[WARNING] Unknown job type {job_type}, returning dict")
        return job

async def process_claimed_job(job):
    """Process a claimed job using existing logic"""
    try:
        job_id = str(job['_id'])
        job_type = job['jobType']
        
        print(f"[PROCESS] Entered process_claimed_job | jobId={job.get('job_id')} | type={job_type}")
        print(f"[POLL] Worker picked job {job['_id']} (type: {job_type})")
        print(f"[DEBUG] About to execute handler for {job_type}")
        
        # Normalize job dict to Pydantic model
        print(f"[DEBUG] Normalizing job from dict to model | type={job_type}")
        job_model = normalize_job_to_model(job, job_type)
        print(f"[PROCESS] Job normalized | type={job_type} | model={type(job_model).__name__}")
        print(f"[DEBUG] Job normalized successfully | type={type(job_model).__name__}")
        
        # Import handlers dynamically to avoid circular imports
        from api.headless_accessibility import handle_headless_accessibility
        from api.performance import handle_performance_mobile, handle_performance_desktop
        from api.seo_scoring import handle_seo_scoring
        from api.ai_visibility import handle_ai_visibility
        from scraper.workers.seo.technical_domain.worker import execute_technical_domain
        from api.crawl_graph import handle_crawl_graph
        
        # Map job types to existing handlers (call synchronously, not await)
        if job_type == 'PAGE_SCRAPING':
            print(f"[DEBUG] Executing PAGE_SCRAPING handler")
            handle_page_scraping(job_model)
        elif job_type == 'PAGE_ANALYSIS':
            print(f"[DEBUG] Executing PAGE_ANALYSIS handler")
            handle_page_analysis(job_model)
        elif job_type == 'HEADLESS_ACCESSIBILITY':
            print(f"[DEBUG] Executing HEADLESS_ACCESSIBILITY handler")
            await handle_headless_accessibility(job_model)
        elif job_type == 'PERFORMANCE_MOBILE':
            print(f"[DEBUG] Executing PERFORMANCE_MOBILE handler")
            print(f"[DEBUG] BEFORE handler call | jobType={job_type} | jobId={job_id}")
            result = await handle_performance_mobile(job_model)
            print(f"[DEBUG] AFTER handler call | jobType={job_type} | jobId={job_id} | result={result}")
        elif job_type == 'PERFORMANCE_DESKTOP':
            print(f"[DEBUG] Executing PERFORMANCE_DESKTOP handler")
            print(f"[DEBUG] BEFORE handler call | jobType={job_type} | jobId={job_id}")
            result = handle_performance_desktop(job_model)
            print(f"[DEBUG] AFTER handler call | jobType={job_type} | jobId={job_id} | result={result}")
        elif job_type == 'SEO_SCORING':
            print(f"[DEBUG] Executing SEO_SCORING handler")
            handle_seo_scoring(job_model)
        elif job_type == 'AI_VISIBILITY':
            print(f"[DEBUG] Executing AI_VISIBILITY handler")
            await handle_ai_visibility(job_model)
        elif job_type == 'AI_VISIBILITY_SCORING':
            print(f"[DEBUG] Executing AI_VISIBILITY_SCORING handler")
            from api.ai_visibility_scoring_v2 import handle_ai_visibility_scoring
            await handle_ai_visibility_scoring(job_model)
        elif job_type == 'TECHNICAL_DOMAIN':
            print(f"[DEBUG] TECHNICAL_DOMAIN handler triggered")
            print(f"[DISPATCH] About to execute handler for TECHNICAL_DOMAIN")
            print(f"[DEBUG] Executing TECHNICAL_DOMAIN worker")
            execute_technical_domain(job_model)
            print(f"[DISPATCH] Finished handler for TECHNICAL_DOMAIN")
        elif job_type == 'CRAWL_GRAPH':
            print(f"[DEBUG] Executing CRAWL_GRAPH handler")
            handle_crawl_graph(job_model)
        else:
            print(f"[WARNING] Unknown job type: {job_type}")
        
        print(f"[DONE] Job completed {job['_id']}")
        
    except Exception as e:
        print(f"[ERROR] Failed to process job {job['_id']}: {e}")
        print(f"[ERROR] PROCESS_JOB FAILED: {str(e)}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")

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
    # 🚨 CRASH TEST REMOVED - Worker can now run
    test_node_connection()
    
    worker_id = sys.argv[1] if len(sys.argv) > 1 else generate_worker_id()
    
    print(f"🤖 Starting Python worker with ID: {worker_id}")
    print("🚀 Worker ready to receive dispatched jobs from Node.js")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
