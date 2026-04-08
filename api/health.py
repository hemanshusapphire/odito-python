"""Health check API route."""

from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "python-worker"}
