"""PageSpeed Insights API endpoint"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl
from typing import Optional
import logging

from scraper.workers.homepage_audit.pagespeed import run_pagespeed_analysis

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/homepage-pagespeed", tags=["homepage-pagespeed"])


class PageSpeedRequest(BaseModel):
    url: HttpUrl
    timeout: Optional[int] = 120


class PageSpeedResponse(BaseModel):
    success: bool
    mobile: dict
    desktop: dict
    error: Optional[str] = None
    message: Optional[str] = None


@router.post("/", response_model=PageSpeedResponse)
async def get_pagespeed_metrics(request: PageSpeedRequest):
    """
    Get PageSpeed Insights metrics for a URL.
    
    This endpoint runs PageSpeed analysis separately from the main audit
    to avoid blocking the main audit pipeline.
    
    Args:
        request: PageSpeedRequest with URL and optional timeout
        
    Returns:
        PageSpeedResponse with mobile and desktop metrics
    """
    try:
        url = str(request.url)
        print("[HOMEPAGE_PAGESPEED] Endpoint HIT")
        print(f"[HOMEPAGE_PAGESPEED] URL: {url}")
        logger.info(f"Starting homepage PageSpeed analysis for: {url}")
        
        # Run PageSpeed analysis
        result = run_pagespeed_analysis(url)
        print("[HOMEPAGE_PAGESPEED] Response SENT")
        
        logger.info(f"PageSpeed analysis completed: success={result['success']}")
        return PageSpeedResponse(**result)
        
    except Exception as e:
        logger.error(f"PageSpeed endpoint error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"PageSpeed analysis failed: {str(e)}"
        )


@router.get("/health")
async def pagespeed_health():
    """Check PageSpeed service health"""
    return {
        "status": "healthy",
        "service": "pagespeed",
        "description": "PageSpeed Insights API integration"
    }
