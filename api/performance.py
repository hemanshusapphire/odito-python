"""Performance analysis API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson.objectid import ObjectId
from datetime import datetime
import os
import requests

router = APIRouter()

class PerformanceMobileJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_SCRAPING job

class PerformanceDesktopJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to PAGE_SCRAPING job

@router.post("/jobs/performance-mobile")
def handle_performance_mobile(job: PerformanceMobileJob):
    """Handle PERFORMANCE_MOBILE job dispatched from Node.js"""
    print(f"[DEBUG] PERFORMANCE_MOBILE endpoint called | jobId={job.jobId} | projectId={job.projectId}")
    
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.performance_analysis.performance_mobile import execute_performance_mobile_logic
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed PERFORMANCE_MOBILE job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[WORKER] PERFORMANCE_MOBILE started | jobId={job.jobId}")
        
        # Execute performance analysis immediately (no polling loop)
        result = execute_performance_mobile_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "PERFORMANCE_MOBILE job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] PERFORMANCE_MOBILE handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }

@router.post("/jobs/performance-desktop")
def handle_performance_desktop(job: PerformanceDesktopJob):
    """Handle PERFORMANCE_DESKTOP job dispatched from Node.js"""
    print(f"[DEBUG] PERFORMANCE_DESKTOP endpoint called | jobId={job.jobId} | projectId={job.projectId}")
    
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from scraper.workers.seo.performance_analysis.performance_desktop import execute_performance_desktop_logic
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed PERFORMANCE_DESKTOP job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[WORKER] PERFORMANCE_DESKTOP started | jobId={job.jobId}")
        
        # Execute performance analysis immediately (no polling loop)
        result = execute_performance_desktop_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "PERFORMANCE_DESKTOP job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] PERFORMANCE_DESKTOP handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }
