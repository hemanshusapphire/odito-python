"""Keyword research API routes."""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone

router = APIRouter()


class KeywordResearchJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    keyword: str
    depth: Optional[int] = 2


class KeywordRankingRequest(BaseModel):
    domain: str
    keywords: List[str]
    location: Optional[str] = "India"
    language: Optional[str] = "en"
    device: Optional[str] = "desktop"
    depth: Optional[int] = 100


@router.post("/jobs/keyword-research")
def handle_keyword_research(job: KeywordResearchJob):
    """Handle KEYWORD_RESEARCH job dispatched from Node.js"""
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.keyword_research.keyword_research import execute_keyword_research

    print(f"[ROUTE] KEYWORD_RESEARCH handler entered | jobId={job.jobId} | keyword=\"{job.keyword}\" | timestamp={datetime.now(timezone.utc).isoformat()}")

    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed KEYWORD_RESEARCH job | jobId={job.jobId} | timestamp={datetime.now(timezone.utc).isoformat()}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }

        print(f"[WORKER] KEYWORD_RESEARCH started | jobId={job.jobId} | keyword=\"{job.keyword}\" | depth={job.depth} | timestamp={datetime.now(timezone.utc).isoformat()}")

        # Execute keyword research
        result = execute_keyword_research(job)

        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)

        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "KEYWORD_RESEARCH job accepted and processing"
        }

    except Exception as e:
        print(f"[ERROR] KEYWORD_RESEARCH handler failed | jobId={job.jobId} | reason=\"{str(e)}\" | timestamp={datetime.now(timezone.utc).isoformat()}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }


@router.get("/test")
def test_endpoint():
    """Test endpoint to verify router is working"""
    return {"message": "keyword_research router is working"}


@router.post("/check-rankings")
def check_keyword_rankings(request: KeywordRankingRequest):
    """Handle keyword ranking check requests"""
    print(f"[ROUTE] CHECK_RANKINGS handler entered | domain=\"{request.domain}\" | keywords={len(request.keywords)} | location=\"{request.location}\" | timestamp={datetime.now(timezone.utc).isoformat()}")

    try:
        from scraper.workers.seo.keyword_research.dataforseo_client import DataForSEOClient

        # Initialize DataForSEO client
        client = DataForSEOClient()

        # Map location to DataForSEO location code
        location_code = 2036  # Default to India
        if request.location.lower() in ["india", "nashik", "maharashtra"]:
            location_code = 2036  # India
        elif request.location.lower() in ["united states", "usa", "us"]:
            location_code = 2840  # United States

        print(f"[RANKING] Using location code {location_code} for location=\"{request.location}\"")

        # Get rankings for all keywords
        rankings = client.get_multiple_keyword_rankings(
            keywords=request.keywords,
            domain=request.domain,
            location_code=location_code,
            language_code=request.language,
            device=request.device,
            depth=request.depth
        )

        print(f"[RANKING] Completed ranking check | domain=\"{request.domain}\" | keywords={len(rankings)} | timestamp={datetime.now(timezone.utc).isoformat()}")

        return {
            "success": True,
            "data": {
                "domain": request.domain,
                "location": request.location,
                "location_code": location_code,
                "rankings": rankings,
                "summary": {
                    "total_keywords": len(rankings),
                    "found_keywords": len([r for r in rankings if r.get('found', False)]),
                    "average_rank": calculate_average_rank(rankings)
                }
            }
        }

    except Exception as e:
        print(f"[ERROR] CHECK_RANKINGS handler failed | domain=\"{request.domain}\" | reason=\"{str(e)}\" | timestamp={datetime.now(timezone.utc).isoformat()}")
        return {
            "success": False,
            "error": str(e)
        }


def calculate_average_rank(rankings):
    """Calculate average ranking from results"""
    found_rankings = [r for r in rankings if r.get('found', False) and isinstance(r.get('rank'), (int, float))]
    
    if not found_rankings:
        return 0
    
    total_rank = sum(r['rank'] for r in found_rankings)
    return round(total_rank / len(found_rankings), 1)
