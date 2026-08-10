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
from api.technical_domain import router as technical_domain_router
from api.headless_accessibility import router as headless_accessibility_router
from api.url_qualification import router as url_qualification_router
from api.crawl_graph import router as crawl_graph_router
from api.homepage_audit import router as homepage_audit_router
from api.pagespeed import router as pagespeed_router
from api.homepage_pagespeed import router as homepage_pagespeed_router
from api.website_extraction import router as website_extraction_router
from api.ai_visibility import AIVisibilityJob

# Configure logging to suppress third-party noise
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

def _log(level, msg):
    if level == 'DEBUG' and LOG_LEVEL != 'DEBUG':
        return
    print(f"[{level}] {msg}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown hooks"""
    asyncio.create_task(poll_for_jobs())
    _log('INFO', 'Job polling started')

    yield

    try:
        from scraper.shared.http_client import close_http_client
        await close_http_client()
        _log('INFO', 'Shared HTTP client closed')
    except Exception as e:
        _log('WARN', f'Error closing HTTP client: {e}')

    try:
        from scraper.shared.browser_pool import shutdown_browser_pool
        shutdown_browser_pool()
        _log('INFO', 'Browser pool closed')
    except Exception as e:
        _log('WARN', f'Error closing browser pool: {e}')

app = FastAPI(lifespan=lifespan)

# Thread-safe tracking for parallel jobs
running_parallel_jobs = set()
running_jobs_lock = threading.Lock()

PARALLEL_JOB_TYPES = {
    'PAGE_SCRAPING', 'HEADLESS_ACCESSIBILITY', 'CRAWL_GRAPH', 'AI_VISIBILITY', 'URL_QUALIFICATION',
    # LINK_DISCOVERY/DOMAIN_PERFORMANCE call synchronous, blocking execute_*
    # functions with no internal await -- without a dedicated thread here,
    # claiming one of these on the poller's own event loop would stall
    # poll_for_jobs() (and every other coroutine sharing this single-worker
    # event loop) for the job's full duration, exactly like the non-parallel
    # 'else' branch below already does for other sync job types.
    'LINK_DISCOVERY', 'DOMAIN_PERFORMANCE',
}

def run_job_in_thread(job, job_type):
    """Run a job in a separate thread with thread-safe logging"""
    job_id = job['_id']
    _log('INFO', f'Job started in thread | type={job_type} | jobId={job_id}')
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(process_claimed_job(job))
        finally:
            loop.close()
        _log('INFO', f'Job completed in thread | type={job_type} | jobId={job_id}')
    except Exception as e:
        _log('ERROR', f'Job failed in thread | type={job_type} | jobId={job_id} | error={str(e)}')
        import traceback
        traceback.print_exc()
    finally:
        with running_jobs_lock:
            if job_id in running_parallel_jobs:
                running_parallel_jobs.remove(job_id)

# PULL model polling function
async def poll_for_jobs():
    """Poll for jobs from MongoDB via Node.js API"""
    backend_url = config.get_service_url('backend')

    job_types = [
        'LINK_DISCOVERY',
        'DOMAIN_PERFORMANCE',
        'URL_QUALIFICATION',
        'PAGE_SCRAPING',
        'HEADLESS_ACCESSIBILITY',
        'PERFORMANCE_MOBILE',
        'PERFORMANCE_DESKTOP',
        'PAGE_ANALYSIS',
        'SEO_SCORING',
        'AI_VISIBILITY',
        'TECHNICAL_DOMAIN',
        'CRAWL_GRAPH',
        # F4-016: project-level Verification Batch aggregation — runs once
        # per batch, not once per URL. Deliberately NOT in PARALLEL_JOB_TYPES:
        # each is a single project-wide recomputation (same class of work as
        # SEO_SCORING/AI_VISIBILITY's own per-page runs), so it's serialized
        # on the poller's event loop exactly like those are.
        'PROJECT_SEO_AGGREGATION',
        'PROJECT_AI_AGGREGATION',
    ]

    from scraper.shared.http_client import get_http_client

    while True:
        try:
            for job_type in job_types:
                try:
                    claim_url = f"{backend_url}/api/jobs/claim"
                    http_client = get_http_client()
                    response = await http_client.get(
                        claim_url,
                        params={'job_type': job_type}
                    )

                    if response.status_code == 200:
                        data = response.json()
                        if data.get('success') and data.get('data'):
                            job_data = data['data']
                            job_id = job_data.get('job_id')
                            _log('INFO', f'Job claimed | type={job_type} | jobId={job_id}')
                            job = {
                                '_id': job_data.get('job_id'),
                                'jobType': job_data.get('jobType'),
                                'project_id': job_data.get('projectId'),
                                'input_data': job_data.get('input_data'),
                                'status': job_data.get('status')
                            }

                            if job_type in PARALLEL_JOB_TYPES:
                                job_id = job['_id']
                                with running_jobs_lock:
                                    if job_id in running_parallel_jobs:
                                        continue
                                    running_parallel_jobs.add(job_id)
                                thread = threading.Thread(
                                    target=run_job_in_thread,
                                    args=(job, job_type),
                                    daemon=True
                                )
                                thread.start()
                            else:
                                await process_claimed_job(job)
                                break
                except Exception as e:
                    _log('ERROR', f'Poll error for {job_type}: {e}')

            await asyncio.sleep(2)
        except Exception as e:
            _log('ERROR', f'Polling loop error: {e}')
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)

def normalize_job_to_model(job: dict, job_type: str):
    """Convert dict job to appropriate Pydantic model based on job type"""
    input_data = job.get('input_data', {})

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
    elif job_type == 'URL_QUALIFICATION':
        from scraper.workers.seo.url_qualification.worker import UrlQualificationJob
        return UrlQualificationJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', ''),
            canonicalHost=input_data.get('canonical_host') or None,
        )
    elif job_type == 'PAGE_SCRAPING':
        from api.scraping import PageScrapingJob
        return PageScrapingJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            urls=input_data.get('urls', []),
            canonical_urls=input_data.get('canonical_urls', []),
            sourceJobId=input_data.get('source_job_id'),
            is_retry=input_data.get('is_retry', False),
            retry_round=input_data.get('retry_round', 0)
        )
    elif job_type == 'HEADLESS_ACCESSIBILITY':
        from api.headless_accessibility import HeadlessAccessibilityJob
        return HeadlessAccessibilityJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', ''),
            urls=input_data.get('urls', []),
            canonical_urls=input_data.get('canonical_urls', []),
        )
    elif job_type == 'PAGE_ANALYSIS':
        from api.analysis import PageAnalysisJob
        return PageAnalysisJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', ''),
            urls=input_data.get('urls', [])
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
            sourceJobId=input_data.get('source_job_id', ''),
            urls=input_data.get('urls', []),
            batchId=input_data.get('batchId')
        )
    elif job_type == 'AI_VISIBILITY':
        from api.ai_visibility import AIVisibilityJob
        return AIVisibilityJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            domain=input_data.get('domain', ''),
            sourceJobId=input_data.get('source_job_id', ''),
            urls=input_data.get('urls', []),
            batchId=input_data.get('batchId')
        )
    elif job_type == 'PROJECT_SEO_AGGREGATION':
        from api.seo_scoring import ProjectSeoAggregationJob
        return ProjectSeoAggregationJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            batchId=input_data.get('batchId'),
            sourceJobId=input_data.get('source_job_id')
        )
    elif job_type == 'PROJECT_AI_AGGREGATION':
        from api.ai_visibility import ProjectAiAggregationJob
        return ProjectAiAggregationJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            batchId=input_data.get('batchId'),
            sourceJobId=input_data.get('source_job_id')
        )
    elif job_type == 'CRAWL_GRAPH':
        from api.crawl_graph import CrawlGraphJob
        return CrawlGraphJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            sourceJobId=input_data.get('source_job_id', '')
        )
    elif job_type == 'LINK_DISCOVERY':
        from api.jobs import LinkDiscoveryJob
        return LinkDiscoveryJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            main_url=input_data.get('main_url', '')
        )
    elif job_type == 'DOMAIN_PERFORMANCE':
        from api.domain_performance import DomainPerformanceJob
        return DomainPerformanceJob(
            jobId=str(job_id),
            projectId=str(project_id),
            userId=str(user_id),
            main_url=input_data.get('main_url', '')
        )
    else:
        _log('WARN', f'Unknown job type {job_type}, returning dict')
        return job

async def process_claimed_job(job):
    """Process a claimed job using existing logic"""
    try:
        job_id = str(job['_id'])
        job_type = job['jobType']

        _log('INFO', f'Job started | type={job_type} | jobId={job_id}')

        job_model = normalize_job_to_model(job, job_type)

        from api.headless_accessibility import handle_headless_accessibility
        from api.performance import handle_performance_mobile, handle_performance_desktop
        from api.seo_scoring import handle_seo_scoring
        from api.ai_visibility import handle_ai_visibility
        from scraper.workers.seo.technical_domain.worker import execute_technical_domain
        from api.crawl_graph import handle_crawl_graph

        if job_type == 'URL_QUALIFICATION':
            from scraper.workers.seo.url_qualification.worker import execute_url_qualification
            await execute_url_qualification(job_model)
        elif job_type == 'PAGE_SCRAPING':
            handle_page_scraping(job_model)
        elif job_type == 'PAGE_ANALYSIS':
            handle_page_analysis(job_model)
        elif job_type == 'HEADLESS_ACCESSIBILITY':
            await handle_headless_accessibility(job_model)
        elif job_type == 'PERFORMANCE_MOBILE':
            result = await handle_performance_mobile(job_model)
        elif job_type == 'PERFORMANCE_DESKTOP':
            result = handle_performance_desktop(job_model)
        elif job_type == 'SEO_SCORING':
            handle_seo_scoring(job_model)
        elif job_type == 'AI_VISIBILITY':
            await handle_ai_visibility(job_model)
        elif job_type == 'PROJECT_SEO_AGGREGATION':
            from api.seo_scoring import handle_project_seo_aggregation
            handle_project_seo_aggregation(job_model)
        elif job_type == 'PROJECT_AI_AGGREGATION':
            from api.ai_visibility import handle_project_ai_aggregation
            handle_project_ai_aggregation(job_model)
        elif job_type == 'TECHNICAL_DOMAIN':
            execute_technical_domain(job_model)
        elif job_type == 'CRAWL_GRAPH':
            handle_crawl_graph(job_model)
        elif job_type == 'LINK_DISCOVERY':
            from scraper.workers.seo.link_discovery.link_discovery import execute_link_discovery
            execute_link_discovery(job_model)
        elif job_type == 'DOMAIN_PERFORMANCE':
            from api.domain_performance import execute_domain_performance_logic
            execute_domain_performance_logic(job_model)
        else:
            _log('WARN', f'Unknown job type: {job_type}')

        _log('INFO', f'Job completed | type={job_type} | jobId={job_id}')

    except Exception as e:
        _log('ERROR', f'Job failed | jobId={job.get("_id")} | error={e}')
        import traceback
        print(traceback.format_exc())

# Include API routers
app.include_router(health_router, prefix="/api", tags=["health"])
app.include_router(jobs_router, prefix="/api", tags=["jobs"])
app.include_router(scraping_router, prefix="/api", tags=["scraping"])
app.include_router(analysis_router, prefix="/api", tags=["analysis"])
app.include_router(performance_router, prefix="/api", tags=["performance"])
app.include_router(domain_performance_router, prefix="/api", tags=["domain_performance"])
app.include_router(seo_scoring_router, prefix="/api", tags=["seo_scoring"])
app.include_router(ai_visibility_router, prefix="/api", tags=["ai_visibility"])
app.include_router(technical_domain_router, prefix="/api", tags=["technical_domain"])
app.include_router(headless_accessibility_router, prefix="/api", tags=["headless_accessibility"])
app.include_router(url_qualification_router, prefix="/api", tags=["url_qualification"])
app.include_router(crawl_graph_router, prefix="/api", tags=["crawl_graph"])
app.include_router(homepage_audit_router, prefix="/api", tags=["homepage_audit"])
app.include_router(pagespeed_router, prefix="/api", tags=["pagespeed"])
app.include_router(homepage_pagespeed_router, prefix="/api", tags=["homepage_pagespeed"])
app.include_router(website_extraction_router, tags=["website_extraction"])

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
    except Exception as e:
        _log('WARN', f'Failed to send progress update: {e}')

def generate_worker_id():
    """Generate a unique worker ID"""
    return f"worker-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"


@app.post("/workers/claim")
def claim_job(request: JobClaimRequest):
    try:
        node_backend_url = config.get_service_url('node_backend')
        node_url = f"{node_backend_url}/api/workers/claim"
        claim_payload = {
            "job_type": request.job_type,
            "worker_id": request.worker_id
        }
        response = requests.post(node_url, json=claim_payload, timeout=10)
        response.raise_for_status()

        result = response.json()
        if result.get("success"):
            job_data = result.get("data", {})
            return JobClaimResponse(
                success=True,
                message="Job claimed successfully",
                data=job_data
            )
        else:
            return JobClaimResponse(
                success=False,
                message=result.get('message', 'Failed to claim job'),
                data=None
            )

    except Exception as e:
        _log('ERROR', f'Error claiming job: {str(e)}')
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
