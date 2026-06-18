from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any
import logging

from scraper.workers.homepage_audit.pagespeed import run_pagespeed_analysis

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

class PageSpeedRequest(BaseModel):
    url: str

@router.post("/homepage-pagespeed")
async def homepage_pagespeed(data: PageSpeedRequest):
    url = data.url

    print("[HOMEPAGE_PAGESPEED] HIT")
    print(f"[HOMEPAGE_PAGESPEED] URL: {url}")
    logger.info(f"Starting homepage PageSpeed analysis for: {url}")

    try:
        result = run_pagespeed_analysis(url)
        print("[HOMEPAGE_PAGESPEED] Response SENT")
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        print(f"[HOMEPAGE_PAGESPEED] ERROR: {str(e)}")
        logger.error(f"Homepage PageSpeed analysis failed: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }
