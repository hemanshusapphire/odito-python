"""Homepage Audit API Routes - FastAPI endpoint for free tier audit"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl
from scraper.workers.homepage_audit.pipeline import run_homepage_audit

router = APIRouter()


class HomepageAuditRequest(BaseModel):
    url: str


@router.post("/homepage-audit")
async def homepage_audit(request: HomepageAuditRequest):
    """
    Run synchronous homepage audit.
    
    Accepts a URL and returns SEO + AI analysis for the homepage only.
    Response time: < 4 seconds
    """
    try:
        # Validate URL format
        url = request.url.strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL is required")
        
        # Run audit with 4 second timeout
        result = await run_homepage_audit(url, timeout=4)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        return {
            'success': False,
            'error': 'server_error',
            'message': str(e)
        }
