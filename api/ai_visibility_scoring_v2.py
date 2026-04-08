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
    userId: str
    sourceJobId: str

    @validator('projectId')
    def validate_project_id(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid projectId format')
        return v

    @validator('userId')
    def validate_user_id(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid userId format')
        return v

@router.post("/jobs/ai-visibility-scoring")
async def create_ai_visibility_scoring_job(job: AIVisibilityScoringV2Job):
    """Create AI Visibility Scoring v2 job"""
    try:
        # Debug logging
        print(f"[AI_VISIBILITY_SCORING] Started for project: {job.projectId}")
        print(f"[AI_VISIBILITY_SCORING] Job ID: {job.jobId}")
        print(f"[AI_VISIBILITY_SCORING] Source Job ID: {job.sourceJobId}")
        
        # Import worker function
        from scraper.workers.ai.ai_scoring_v2.ai_scoring_v2_worker import execute_ai_visibility_scoring_logic
        
        # Execute scoring logic
        result = execute_ai_visibility_scoring_logic(job.dict())
        
        if result["status"] == "failed":
            raise HTTPException(status_code=500, detail=result.get("error", "Scoring failed"))
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating AI visibility scoring v2 job: {e}")
        raise HTTPException(status_code=500, detail=str(e))
