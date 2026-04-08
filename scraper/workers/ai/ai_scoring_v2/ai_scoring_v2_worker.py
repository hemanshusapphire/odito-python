"""
AI Visibility Scoring v2 Worker

New scoring worker that replaces the legacy issue-based scoring system.
Uses rule-based scoring with 6 categories and 48+ rules.
"""

import os
import sys
import threading
import logging
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from bson.objectid import ObjectId
from pymongo import UpdateOne

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(__file__))  # Add current directory

from db import seo_ai_visibility, seo_ai_page_scores, seo_ai_visibility_project, seo_ai_visibility_issues, seoprojects
from scraper.shared.utils import send_completion_callback

def send_progress_update(job_id: str, percentage: int, step: str, message: str, subtext: str = None):
    """Send progress update to Node.js backend - SAFE VERSION"""
    try:
        from env_config import get_config
        config = get_config()
        node_backend_url = config.get_service_url('node_backend')
        progress_url = f"{node_backend_url}/api/jobs/{job_id}/progress"
        
        payload = {
            "percentage": percentage,
            "step": step,
            "message": message,
            "subtext": subtext
        }
        
        # === PHASE 1 SAFETY ADDITION ===
        # Safe progress update with proper error handling and logging
        response = requests.post(progress_url, json=payload, timeout=5)
        response.raise_for_status()
        
    except requests.RequestException as e:
        # Log network errors but don't fail the job
        print(f"[SAFETY] Progress update network error | jobId={job_id} | error={e}")
    except Exception as e:
        # Log any other errors but don't fail the job
        print(f"[SAFETY] Progress update unexpected error | jobId={job_id} | error={e}")
    # === SAFETY: Never re-raise exceptions - progress updates are non-critical

# Import new scoring engine
from rule_registry import rule_registry
from scoring_engine import ScoringEngine
from metric_mapper import derive_dashboard_metrics
from categories.ai_impact import register_ai_impact_rules
from categories.citation_probability import register_citation_probability_rules
from categories.llm_readiness import register_llm_readiness_rules
from categories.aeo_score import register_aeo_score_rules
from categories.topical_authority import register_topical_authority_rules
from categories.voice_intent import register_voice_intent_rules

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIVisibilityScoringJob:
    """Job data structure for AI visibility scoring"""
    
    def __init__(self, job_data: Dict[str, Any]):
        self.jobId = job_data.get("jobId")
        self.projectId = job_data.get("projectId")
        self.userId = job_data.get("userId")
        self.aiProjectId = job_data.get("aiProjectId") or self.projectId
        self.sourceJobId = job_data.get("sourceJobId")

def is_job_cancelled(job_id: str) -> bool:
    """Check if job has been cancelled"""
    try:
        # This would integrate with the main job cancellation system
        # For now, return False
        return False
    except Exception:
        return False

def initialize_scoring_engine() -> ScoringEngine:
    """Initialize and configure the scoring engine"""
    try:
        # Register all category rules
        register_ai_impact_rules(rule_registry)
        register_citation_probability_rules(rule_registry)
        register_llm_readiness_rules(rule_registry)
        register_aeo_score_rules(rule_registry)
        register_topical_authority_rules(rule_registry)
        register_voice_intent_rules(rule_registry)
        
        # Validate registry
        validation_report = rule_registry.validate_registry()
        if validation_report["issues"]:
            logger.warning(f"Registry validation issues: {validation_report['issues']}")
        
        # Create scoring engine
        engine = ScoringEngine(rule_registry)
        
        logger.info(f"Scoring engine initialized with {validation_report['total_rules']} rules")
        
        # Log category breakdown
        for category, report in validation_report["categories"].items():
            logger.info(f"  {category}: {report['rule_count']} rules, weight: {report['total_weight']}")
        
        return engine
        
    except Exception as e:
        logger.error(f"Failed to initialize scoring engine: {e}")
        raise

def discover_collection_schema() -> Dict[str, Any]:
    """Discover schema for AI visibility collection"""
    try:
        # Get a sample document to understand schema
        sample = seo_ai_visibility.find_one()
        if sample:
            # Remove _id for cleaner schema
            schema_sample = {k: type(v).__name__ for k, v in sample.items() if k != "_id"}
            logger.info(f"Discovered AI visibility schema: {list(schema_sample.keys())}")
            return {"fields": list(schema_sample.keys()), "sample": schema_sample}
        else:
            logger.warning("No documents found in seo_ai_visibility collection")
            return {"fields": [], "sample": {}}
    except Exception as e:
        logger.error(f"Error discovering collection schema: {e}")
        return {"fields": [], "sample": {}}

def detect_project_field() -> Optional[str]:
    """Detect the project field name in the collection"""
    possible_fields = ["projectId", "project_id", "seoProjectId"]
    
    try:
        sample = seo_ai_visibility.find_one()
        if sample:
            for field in possible_fields:
                if field in sample:
                    logger.info(f"Detected project field: {field}")
                    return field
    except Exception as e:
        logger.error(f"Error detecting project field: {e}")
    
    logger.warning("Could not detect project field, defaulting to 'projectId'")
    return "projectId"

def detect_url_field() -> Optional[str]:
    """Detect the URL field name in the collection"""
    possible_fields = ["url", "page_url", "pageUrl"]
    
    try:
        sample = seo_ai_visibility.find_one()
        if sample:
            for field in possible_fields:
                if field in sample:
                    logger.info(f"Detected URL field: {field}")
                    return field
    except Exception as e:
        logger.error(f"Error detecting URL field: {e}")
    
    logger.warning("Could not detect URL field, defaulting to 'url'")
    return "url"

def fetch_pages_for_scoring(project_id: str, project_field: str) -> List[Dict[str, Any]]:
    """Fetch all pages for a project that need scoring"""
    try:
        # Build query
        query = {project_field: ObjectId(project_id)}
        
        # Fetch all pages for the project
        pages = list(seo_ai_visibility.find(query))
        
        logger.info(f"Fetched {len(pages)} pages for scoring")
        return pages
        
    except Exception as e:
        logger.error(f"Error fetching pages for scoring: {e}")
        raise

def execute_ai_visibility_scoring_v2(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute AI visibility scoring v2
    
    Args:
        job_data: Job configuration data
        
    Returns:
        Job completion results
    """
    try:
        # Initialize job
        job = AIVisibilityScoringJob(job_data)
        logger.info(f"[WORKER] AI_VISIBILITY_SCORING_V2 started | jobId={job.jobId} | projectId={job.projectId}")
        
        # Send initial progress
        send_progress_update(job.jobId, 10, "Initializing", "Setting up scoring engine")
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled during initialization | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Initialize scoring engine
        scoring_engine = initialize_scoring_engine()
        
        # Discover collection schema
        send_progress_update(job.jobId, 20, "Discovery", "Analyzing data structure")
        schema_info = discover_collection_schema()
        project_field = detect_project_field()
        url_field = detect_url_field()
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled during discovery | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Fetch pages for scoring
        send_progress_update(job.jobId, 30, "Data Collection", "Fetching pages for scoring")
        pages_data = fetch_pages_for_scoring(job.projectId, project_field)
        
        if not pages_data:
            logger.warning(f"[WORKER] No pages found for scoring | jobId={job.jobId} | projectId={job.projectId}")
            return {
                "status": "completed",
                "jobId": job.jobId,
                "stats": {
                    "pages_scored": 0,
                    "website_score": 0,
                    "categories": {}
                }
            }
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled during data collection | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Score all pages
        send_progress_update(job.jobId, 40, "Scoring", "Evaluating page content")
        page_scores = []
        processed_count = 0
        
        for i, page_data in enumerate(pages_data):
            try:
                # Check for cancellation before each page
                if is_job_cancelled(job.jobId):
                    logger.info(f"[WORKER] Job cancelled during scoring | jobId={job.jobId} | processed={processed_count}")
                    return {"status": "cancelled", "jobId": job.jobId}
                
                # Score the page
                page_score = scoring_engine.score_page(page_data)
                page_scores.append(page_score)
                processed_count += 1
                
                # Derive and store issues from rule_breakdown
                # Issues are informational only and do NOT influence scoring
                try:
                    issues = derive_issues_from_rule_breakdown(
                        page_score.get("rule_breakdown", []),
                        job.projectId,
                        page_score.get("page_url", "")
                    )
                    store_page_issues(issues, job.projectId, page_score.get("page_url", ""))
                except Exception as issue_error:
                    # Log but do not fail scoring if issue storage fails
                    logger.warning(f"[WORKER] Issue storage failed but continuing scoring | url={page_score.get('page_url', '')} | error={issue_error}")
                
                # Calculate and store dashboard metrics in seo_ai_visibility collection
                try:
                    category_scores = page_score.get("category_scores", {})
                    dashboard_metrics = derive_dashboard_metrics(category_scores)
                    update_page_ai_visibility(page_score, dashboard_metrics, job.projectId)
                except Exception as metrics_error:
                    # Log but do not fail scoring if metrics storage fails
                    logger.warning(f"[WORKER] Dashboard metrics storage failed but continuing scoring | url={page_score.get('page_url', '')} | error={metrics_error}")
                
                # Update progress
                progress = 40 + (50 * (i + 1) / len(pages_data))
                send_progress_update(
                    job.jobId, 
                    int(progress), 
                    "Scoring", 
                    f"Processed {processed_count}/{len(pages_data)} pages"
                )
                
            except Exception as e:
                logger.error(f"[WORKER] Error scoring page {page_data.get('url', 'unknown')}: {e}")
                continue
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled after scoring | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Store page scores
        send_progress_update(job.jobId, 90, "Storage", "Saving scoring results")
        store_page_scores(page_scores, job.projectId, url_field)
        
        # Calculate and store website score
        website_result = scoring_engine.score_website(pages_data)
        store_website_score(website_result, job.projectId)
        
        # Send completion progress
        send_progress_update(job.jobId, 100, "Complete", f"Scored {processed_count} pages successfully")
        
        # Prepare completion stats
        completion_stats = {
            "pages_scored": processed_count,
            "website_score": website_result["website_ai_score"],
            "categories": website_result["category_averages"],
            "scoring_version": "v2"
        }
        
        logger.info(f"[WORKER] AI_VISIBILITY_SCORING_V2 completed | jobId={job.jobId} | pages_scored={processed_count} | website_score={website_result['website_ai_score']}")
        
        # Send completion callback to Node.js
        try:
            send_completion_callback(job.jobId, completion_stats)
            logger.info(f"[WORKER] Completion callback sent | jobId={job.jobId}")
        except Exception as callback_error:
            logger.error(f"[WORKER] Failed to send completion callback | jobId={job.jobId} | error={callback_error}")
            # Don't fail the job - callback is non-critical
        
        return {
            "status": "completed",
            "jobId": job.jobId,
            "stats": completion_stats
        }
        
    except Exception as e:
        logger.error(f"[WORKER] AI_VISIBILITY_SCORING_V2 failed | jobId={job.jobId} | error={e}")
        return {
            "status": "failed",
            "jobId": job.jobId,
            "error": str(e)
        }

def derive_issues_from_rule_breakdown(rule_breakdown: List[Dict[str, Any]], project_id: str, page_url: str) -> List[Dict[str, Any]]:
    """
    Derive issues from rule breakdown scores.
    
    Severity mapping:
    - rule_score < 40 → "high"
    - 40 ≤ rule_score < 70 → "medium"
    - rule_score ≥ 70 → no issue
    
    Issues are informational only and do NOT influence scoring.
    """
    issues = []
    
    for rule in rule_breakdown:
        rule_score = rule.get("score", 100)
        
        # Only create issues for scores below 70
        if rule_score < 70:
            # Determine severity based on score
            if rule_score < 40:
                severity = "high"
            else:
                severity = "medium"
            
            # Create issue document
            issue = {
                "projectId": ObjectId(project_id),
                "page_url": page_url,
                "rule_id": rule.get("rule_id", "unknown"),
                "category": rule.get("category", "unknown"),
                "rule_score": rule_score,
                "severity": severity,
                "message": rule.get("rule_name", f"Rule {rule.get('rule_id', 'unknown')} scored {rule_score:.1f}"),
                "created_at": datetime.utcnow()
            }
            issues.append(issue)
    
    return issues

def store_page_issues(issues: List[Dict[str, Any]], project_id: str, page_url: str):
    """
    Store page issues in database.
    
    Safeguards:
    - Deletes existing issues for the page before inserting new ones
    - Only inserts if issues exist
    - Issues are informational only, do not affect scoring
    """
    try:
        if not issues:
            # No issues to store - delete any existing issues for this page
            result = seo_ai_visibility_issues.delete_many({
                "projectId": ObjectId(project_id),
                "page_url": page_url
            })
            if result.deleted_count > 0:
                logger.info(f"[WORKER] Cleared {result.deleted_count} existing issues for page | url={page_url}")
            return
        
        # Delete existing issues for this page
        delete_result = seo_ai_visibility_issues.delete_many({
            "projectId": ObjectId(project_id),
            "page_url": page_url
        })
        
        if delete_result.deleted_count > 0:
            logger.info(f"[WORKER] Deleted {delete_result.deleted_count} existing issues before storing new issues | url={page_url}")
        
        # Insert new issues
        insert_result = seo_ai_visibility_issues.insert_many(issues)
        
        logger.info(f"[WORKER] Stored page issues | url={page_url} | count={len(issues)} | inserted={len(insert_result.inserted_ids)}")
        
    except Exception as e:
        logger.error(f"[WORKER] Error storing page issues | url={page_url} | error={e}")
        # Do not raise - issues are informational only and should not break scoring

def update_page_ai_visibility(page_score: Dict[str, Any], dashboard_metrics: Dict[str, float], project_id: str):
    """
    Update seo_ai_visibility collection with dashboard metrics while preserving existing fields.
    
    This function adds dashboard_metrics to the existing ai_visibility structure:
    ai_visibility.dashboard_metrics
    """
    try:
        page_url = page_score.get("page_url", "")
        category_scores = page_score.get("category_scores", {})
        
        if not page_url:
            logger.warning("[WORKER] Cannot update AI visibility - missing page URL")
            return
        
        # Debug log: Show derived dashboard metrics
        logger.info(f"[METRICS] Derived dashboard metrics: {dashboard_metrics}")
        
        # Update the seo_ai_visibility collection, preserving existing fields
        update_doc = {
            "$set": {
                "ai_visibility.score": page_score.get("page_ai_score", 0),
                "ai_visibility.categories": category_scores,
                "ai_visibility.dashboard_metrics": dashboard_metrics,
                "updated_at": datetime.utcnow()
            }
        }
        
        # Use the detected URL field for the query
        url_field = detect_url_field()
        filter_doc = {
            "projectId": ObjectId(project_id),
            url_field: page_url
        }
        
        result = seo_ai_visibility.update_one(filter_doc, update_doc)
        
        if result.matched_count > 0:
            logger.info(f"[WORKER] Updated AI visibility with dashboard metrics | url={page_url} | metrics={len(dashboard_metrics)}")
        else:
            logger.warning(f"[WORKER] AI visibility document not found for update | url={page_url}")
        
    except Exception as e:
        logger.error(f"[WORKER] Error updating AI visibility | url={page_url} | error={e}")
        # Do not raise - dashboard metrics are non-critical

def store_page_scores(page_scores: List[Dict[str, Any]], project_id: str, url_field: str):
    """Store page scores in database"""
    try:
        if not page_scores:
            logger.warning("No page scores to store")
            return
        
        # Prepare bulk operations
        bulk_ops = []
        for page_score in page_scores:
            # Create pure scoring document with only required fields
            pure_page_score = {
                "page_url": page_score["page_url"],
                "projectId": ObjectId(project_id),
                "final_score": page_score["page_ai_score"],
                "category_scores": page_score["category_scores"],
                "rule_breakdown": page_score["rule_breakdown"],
                "updated_at": datetime.utcnow()
            }
            
            # Create upsert operation
            filter_doc = {
                "projectId": ObjectId(project_id),
                "page_url": page_score["page_url"]
            }
            
            update_doc = {"$set": pure_page_score}
            
            bulk_ops.append(
                UpdateOne(filter_doc, update_doc, upsert=True)
            )
        
        # Execute bulk operation
        if bulk_ops:
            result = seo_ai_page_scores.bulk_write(bulk_ops)
            logger.info(f"[WORKER] Stored page scores | upserted={result.upserted_count} | modified={result.modified_count}")
        
    except Exception as e:
        logger.error(f"Error storing page scores: {e}")
        raise

def store_website_score(website_result: Dict[str, Any], project_id: str):
    """Store website-level score in BOTH project collections"""
    try:
        # Derive 12 dashboard metrics from 6 category averages
        dashboard_metrics = derive_dashboard_metrics(website_result.get("category_averages", {}))
        
        # === UPDATE 1: seo_ai_visibility_project (existing - preserve functionality) ===
        update_doc_project = {
            "$set": {
                "summary.overallScore": website_result["website_ai_score"],
                "summary.categoryAverages": website_result["category_averages"],
                "summary.dashboardMetrics": dashboard_metrics,
                "summary.pagesScored": website_result["pages_scored"],
                "summary.totalPages": website_result["pages_scored"],
                "aiStatus": "completed",
                "completedAt": datetime.utcnow(),
                "scoring_version": "v2",
                "updated_at": datetime.utcnow()
            }
        }
        
        # === UPDATE 2: seoprojects (NEW - store where frontend expects it) ===
        update_doc_seo = {
            "$set": {
                "ai_visibility.score": website_result["website_ai_score"],
                "ai_visibility.pages_scored": website_result["pages_scored"],
                "ai_visibility.categories": website_result["category_averages"],
                "ai_visibility.dashboard_metrics": dashboard_metrics,
                "ai_visibility.scoring_version": "v2",
                "last_ai_analysis_at": datetime.utcnow()
            }
        }
        
        # Execute both updates
        result1 = seo_ai_visibility_project.update_one(
            {"_id": ObjectId(project_id)},
            update_doc_project
        )
        
        result2 = seoprojects.update_one(
            {"_id": ObjectId(project_id)},
            update_doc_seo
        )
        
        if result1.matched_count > 0 and result2.matched_count > 0:
            logger.info(f"[WORKER] Updated both collections | projectId={project_id} | dashboardMetrics={len(dashboard_metrics)} metrics")
        elif result1.matched_count > 0:
            logger.warning(f"[WORKER] Updated seo_ai_visibility_project only | seoprojects not found | projectId={project_id}")
        elif result2.matched_count > 0:
            logger.warning(f"[WORKER] Updated seoprojects only | seo_ai_visibility_project not found | projectId={project_id}")
        else:
            logger.warning(f"[WORKER] Neither collection found for update | projectId={project_id}")
        
    except Exception as e:
        logger.error(f"Error storing website score: {e}")
        raise

# Main execution function
def execute_ai_visibility_scoring_logic(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry point for AI visibility scoring v2"""
    return execute_ai_visibility_scoring_v2(job_data)

if __name__ == "__main__":
    # Test execution
    test_job = {
        "jobId": "test-job-123",
        "projectId": "507f1f77bcf86cd799439011",
        "userId": "507f1f77bcf86cd799439012",
        "aiProjectId": "507f1f77bcf86cd799439011"
    }
    
    result = execute_ai_visibility_scoring_v2(test_job)
    print(f"Test result: {result}")
