"""AI Visibility Scoring v2 API routes"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, validator
from bson.objectid import ObjectId
from typing import Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class AIVisibilityScoringV2Job(BaseModel):
    jobId: str
    projectId: str
    userId: Optional[str] = None  # Made optional to handle missing userId
    sourceJobId: str

    @validator('projectId')
    def validate_project_id(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid projectId format')
        return v

    @validator('userId')
    def validate_user_id(cls, v):
        # If userId is None or 'unknown', skip validation (backend may not provide it)
        if v is None or v == 'unknown':
            return v
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid userId format')
        return v


def process_ai_visibility_scoring_core(job: AIVisibilityScoringV2Job):
    """
    Core AI_VISIBILITY_SCORING processing logic (shared between API and worker).
    
    Args:
        job: Pydantic AIVisibilityScoringV2Job model
        
    Returns:
        dict with status and result
    """
    try:
        # Debug logging
        print(f"[AI_VISIBILITY_SCORING] Started for project: {job.projectId}")
        print(f"[AI_VISIBILITY_SCORING] Job ID: {job.jobId}")
        print(f"[AI_VISIBILITY_SCORING] Source Job ID: {job.sourceJobId}")
        
        # === CRITICAL VALIDATION: Check source job exists ===
        if not job.sourceJobId or job.sourceJobId == '':
            error_msg = "Source job ID is required for AI visibility scoring"
            print(f"[ERROR] {error_msg}")
            return {"status": "failed", "error": error_msg}
        
        # === CRITICAL VALIDATION: Check AI visibility data exists ===
        try:
            from db import seo_ai_visibility, jobs
            from bson.objectid import ObjectId
            
            # Check if source job exists and completed
            source_job = jobs.find_one({"_id": ObjectId(job.sourceJobId)})
            if not source_job:
                error_msg = f"Source job not found: {job.sourceJobId}"
                print(f"[ERROR] {error_msg}")
                return {"status": "failed", "error": error_msg}
            
            print(f"[VALIDATION] Source job found: {source_job.get('jobType', 'unknown')} | status: {source_job.get('status', 'unknown')}")
            
            # Check if AI visibility data exists for this project
            ai_visibility_count = seo_ai_visibility.count_documents({"projectId": ObjectId(job.projectId)})
            print(f"[VALIDATION] Found {ai_visibility_count} AI visibility records for project {job.projectId}")
            
            if ai_visibility_count == 0:
                error_msg = f"No AI visibility data found for project {job.projectId}. Please run AI_VISIBILITY job first."
                print(f"[ERROR] {error_msg}")
                return {"status": "failed", "error": error_msg}
                
        except Exception as validation_error:
            error_msg = f"Validation failed: {str(validation_error)}"
            print(f"[ERROR] {error_msg}")
            return {"status": "failed", "error": error_msg}
        
        # Import worker function
        from scraper.workers.ai.ai_scoring_v2.ai_scoring_v2_worker import execute_ai_visibility_scoring_logic
        
        # Execute scoring logic
        result = execute_ai_visibility_scoring_logic(job.dict())
        
        if result.get("status") == "failed":
            error_detail = result.get("error", "Scoring failed")
            print(f"[ERROR] Scoring failed: {error_detail}")
            return result
        
        print(f"[AI_VISIBILITY_SCORING] Completed successfully | jobId={job.jobId}")
        return result
        
    except Exception as e:
        error_msg = f"Unexpected error in AI visibility scoring: {str(e)}"
        logger.error(f"{error_msg} | jobId={job.jobId} | projectId={job.projectId}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {"status": "failed", "error": error_msg}


@router.post("/jobs/ai-visibility-scoring")
async def api_ai_visibility_scoring(job: AIVisibilityScoringV2Job):
    """API route for AI_VISIBILITY_SCORING (HTTP dispatch from Node.js)"""
    result = process_ai_visibility_scoring_core(job)
    
    if result.get("status") == "failed":
        error_detail = result.get("error", "Scoring failed")
        raise HTTPException(status_code=500, detail=error_detail)
    
    return result


async def handle_ai_visibility_scoring(job: AIVisibilityScoringV2Job):
    """
    Worker handler for AI_VISIBILITY_SCORING (called from polling loop).
    
    Args:
        job: Pydantic AIVisibilityScoringV2Job model (already normalized from dict)
        
    Returns:
        dict with status and result
    """
    try:
        print(f"[WORKER-AI-SCORING] AI_VISIBILITY_SCORING handler entered | jobId={job.jobId}")
        
        # Call core logic directly
        return process_ai_visibility_scoring_core(job)
        
    except Exception as e:
        print(f"[ERROR] AI_VISIBILITY_SCORING worker handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        import traceback
        print(f"[ERROR] Full traceback: {traceback.format_exc()}")
        return {
            "status": "failed",
            "jobId": job.jobId,
            "error": str(e)
        }
