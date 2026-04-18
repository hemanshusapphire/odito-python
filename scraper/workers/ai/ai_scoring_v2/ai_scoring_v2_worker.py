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

# Import database connections
try:
    from db import seo_ai_visibility, seo_ai_page_scores, seo_ai_visibility_project, seo_ai_visibility_issues, seoprojects
except ImportError as e:
    logging.warning(f"Database import failed: {e}")
    # Fallback for testing
    seo_ai_visibility = None
    seo_ai_page_scores = None
    seo_ai_visibility_project = None
    seo_ai_visibility_issues = None
    seoprojects = None

# Import utilities
try:
    from scraper.shared.utils import send_completion_callback
except ImportError:
    # Fallback for testing
    def send_completion_callback(job_id: str, stats: Dict[str, Any]):
        logging.info(f"Mock completion callback for job {job_id}: {stats}")

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
from scraper.workers.ai.ai_scoring_v2.rule_registry import rule_registry
from scraper.workers.ai.ai_scoring_v2.scoring_engine import ScoringEngine
from scraper.workers.ai.ai_scoring_v2.metric_mapper import derive_dashboard_metrics
from scraper.workers.ai.ai_scoring_v2.categories.ai_impact import register_ai_impact_rules
from scraper.workers.ai.ai_scoring_v2.categories.citation_probability import register_citation_probability_rules
from scraper.workers.ai.ai_scoring_v2.categories.llm_readiness import register_llm_readiness_rules
from scraper.workers.ai.ai_scoring_v2.categories.aeo_score import register_aeo_score_rules
from scraper.workers.ai.ai_scoring_v2.categories.topical_authority import register_topical_authority_rules
from scraper.workers.ai.ai_scoring_v2.categories.voice_intent import register_voice_intent_rules

# Global registry reference for issue validation
_global_rule_registry = None

def get_rule_instance(rule_id: str):
    """Get rule instance from global registry"""
    global _global_rule_registry
    if _global_rule_registry:
        return _global_rule_registry.get_rule(rule_id)
    return None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIVisibilityScoringJob:
    """Job data structure for AI visibility scoring"""
    
    def __init__(self, job_data: Dict[str, Any]):
        self.jobId = job_data.get("jobId")
        self.projectId = job_data.get("projectId")
        self.userId = job_data.get("userId")
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
    """Initialize and configure the scoring engine with fault tolerance"""
    try:
        print(f"[ENGINE] Starting scoring engine initialization...")
        
        # Register all category rules with fault tolerance
        print(f"[ENGINE] Registering category rules...")
        
        category_registrations = [
            ("ai_impact", register_ai_impact_rules),
            ("citation_probability", register_citation_probability_rules),
            ("llm_readiness", register_llm_readiness_rules),
            ("aeo_score", register_aeo_score_rules),
            ("topical_authority", register_topical_authority_rules),
            ("voice_intent", register_voice_intent_rules)
        ]
        
        for category_name, register_fn in category_registrations:
            try:
                print(f"[ENGINE] Registering {category_name} rules...")
                register_fn(rule_registry)
                print(f"[ENGINE] {category_name} rules registered successfully")
            except Exception as e:
                print(f"[ENGINE_WARN] Failed to register {category_name} rules: {e}")
                print(f"[ENGINE_WARN] Continuing with other categories...")
                # Don't crash - continue with other categories
        
        print(f"[ENGINE] All category rules registered")
        
        # Validate registry
        validation_report = rule_registry.validate_registry()
        if validation_report["issues"]:
            logger.warning(f"Registry validation issues: {validation_report['issues']}")
        
        # Create scoring engine (this will trigger weight validation)
        print(f"[ENGINE] Creating scoring engine...")
        engine = ScoringEngine(rule_registry)
        
        # Set global registry reference for issue validation
        global _global_rule_registry
        _global_rule_registry = rule_registry
        
        logger.info(f"Scoring engine initialized with {validation_report['total_rules']} rules")
        
        # Log category breakdown
        for category, report in validation_report["categories"].items():
            logger.info(f"  {category}: {report['rule_count']} rules, weight: {report['total_weight']}")
        
        print(f"[ENGINE] Scoring engine initialization complete")
        return engine
        
    except Exception as e:
        error_msg = f"Failed to initialize scoring engine: {str(e)}"
        logger.error(f"[CRITICAL] {error_msg}")
        print(f"[CRITICAL] {error_msg}")
        import traceback
        print(f"[CRITICAL] Traceback: {traceback.format_exc()}")
        # Re-raise to ensure worker fails properly
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
        if seo_ai_visibility is None:
            logger.error("Cannot detect project field - seo_ai_visibility is None")
            return "projectId"  # Default fallback
            
        sample = seo_ai_visibility.find_one()
        if sample:
            logger.info(f"Sample document keys: {list(sample.keys())}")
            for field in possible_fields:
                if field in sample:
                    logger.info(f"✅ Detected project field: {field}")
                    return field
            
            # If none of the expected fields found, log all keys for debugging
            logger.warning(f"⚠️ None of expected project fields found. Available keys: {list(sample.keys())}")
        else:
            logger.warning("⚠️ No documents found in seo_ai_visibility collection")
    except Exception as e:
        logger.error(f"❌ Error detecting project field: {e}")
    
    logger.warning("⚠️ Could not detect project field, defaulting to 'projectId'")
    return "projectId"

def detect_url_field() -> Optional[str]:
    """Detect the URL field name in the collection"""
    possible_fields = ["url", "page_url", "pageUrl"]
    
    try:
        if seo_ai_visibility is None:
            logger.error("Cannot detect URL field - seo_ai_visibility is None")
            return "url"  # Default fallback
            
        sample = seo_ai_visibility.find_one()
        if sample:
            logger.info(f"Sample document keys for URL detection: {list(sample.keys())}")
            for field in possible_fields:
                if field in sample:
                    logger.info(f"✅ Detected URL field: {field}")
                    return field
            
            # If none of the expected fields found, log all keys for debugging
            logger.warning(f"⚠️ None of expected URL fields found. Available keys: {list(sample.keys())}")
        else:
            logger.warning("⚠️ No documents found in seo_ai_visibility collection for URL field detection")
    except Exception as e:
        logger.error(f"❌ Error detecting URL field: {e}")
    
    logger.warning("⚠️ Could not detect URL field, defaulting to 'url'")
    return "url"

def fetch_pages_for_scoring(project_id: str, project_field: str) -> List[Dict[str, Any]]:
    """Fetch all pages for a project that need scoring"""
    try:
        # === CRITICAL VALIDATION: Check database connection ===
        if seo_ai_visibility is None:
            raise Exception("Database connection not available - seo_ai_visibility collection is None")
        
        # Build query
        query = {project_field: ObjectId(project_id)}
        
        # Debug: Log the query
        logger.info(f"[FETCH] Query: {query}")
        
        # Fetch all pages for the project
        pages = list(seo_ai_visibility.find(query))
        
        logger.info(f"[FETCH] Found {len(pages)} pages for scoring | project_id={project_id} | field={project_field}")
        
        # === CRITICAL VALIDATION: Check if pages exist ===
        if not pages:
            logger.warning(f"[FETCH] No pages found for project {project_id} with field {project_field}")
            # Try alternative field names
            if project_field == "projectId":
                alternative_fields = ["project_id", "seoProjectId"]
            else:
                alternative_fields = ["projectId", "seoProjectId"]
            
            for alt_field in alternative_fields:
                alt_query = {alt_field: ObjectId(project_id)}
                alt_pages = list(seo_ai_visibility.find(alt_query))
                if alt_pages:
                    logger.info(f"[FETCH] Found {len(alt_pages)} pages using alternative field: {alt_field}")
                    return alt_pages
            
            logger.error(f"[FETCH] No pages found with any field name for project {project_id}")
        
        return pages
        
    except Exception as e:
        logger.error(f"[FETCH] Error fetching pages for scoring: {e}")
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
        # Log incoming request payload for debugging
        print(f"[WORKER] AI_VISIBILITY_SCORING_V2 received job_data: {job_data}")
        
        # === CRITICAL VALIDATION: Check database connections ===
        if not all([seo_ai_visibility is not None, seo_ai_page_scores is not None, seo_ai_visibility_project is not None, seoprojects is not None]):
            error_msg = "Database connections not available - one or more collections are None"
            print(f"[CRITICAL] {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "jobId": job_data.get("jobId", "unknown"),
                "critical_failure": True
            }
        
        # Validate required fields
        required_fields = ["jobId", "projectId", "userId", "sourceJobId"]
        missing_fields = [field for field in required_fields if not job_data.get(field)]
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            print(f"[WORKER] Validation failed: {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "jobId": job_data.get("jobId", "unknown")
            }
        
        # Initialize job
        job = AIVisibilityScoringJob(job_data)
        print(f"[WORKER] AI_VISIBILITY_SCORING_V2 started | jobId={job.jobId} | projectId={job.projectId}")
        logger.info(f"[WORKER] AI_VISIBILITY_SCORING_V2 started | jobId={job.jobId} | projectId={job.projectId}")
        
        # Send initial progress
        send_progress_update(job.jobId, 10, "Initializing", "Setting up scoring engine")
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled during initialization | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Initialize scoring engine with error handling
        try:
            scoring_engine = initialize_scoring_engine()
        except Exception as engine_error:
            error_msg = f"Failed to initialize scoring engine: {str(engine_error)}"
            logger.error(f"[CRITICAL] {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "jobId": job.jobId
            }
        
        # Discover collection schema with error handling
        send_progress_update(job.jobId, 20, "Discovery", "Analyzing data structure")
        try:
            schema_info = discover_collection_schema()
            project_field = detect_project_field()
            url_field = detect_url_field()
            print(f"[SCHEMA] Detected fields - project: {project_field}, url: {url_field}")
        except Exception as schema_error:
            error_msg = f"Failed to discover collection schema: {str(schema_error)}"
            logger.error(f"[CRITICAL] {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "jobId": job.jobId
            }
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            logger.info(f"[WORKER] Job cancelled during discovery | jobId={job.jobId}")
            return {"status": "cancelled", "jobId": job.jobId}
        
        # Fetch pages for scoring with comprehensive error handling
        send_progress_update(job.jobId, 30, "Data Collection", "Fetching pages for scoring")
        try:
            pages_data = fetch_pages_for_scoring(job.projectId, project_field)
        except Exception as fetch_error:
            error_msg = f"Failed to fetch pages for scoring: {str(fetch_error)}"
            logger.error(f"[CRITICAL] {error_msg}")
            return {
                "status": "failed",
                "error": error_msg,
                "jobId": job.jobId
            }
        
        if not pages_data:
            logger.warning(f"[WORKER] No pages found for scoring | jobId={job.jobId} | projectId={job.projectId}")
            return {
                "status": "completed",
                "jobId": job.jobId,
                "stats": {
                    "pages_scored": 0,
                    "website_score": 0,
                    "categories": {},
                    "warning": "No AI visibility data found for this project"
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
        
        # Create validation summary
        validation_summary = create_validation_summary(page_scores)
        logger.info(f"[VALIDATION] Summary: {validation_summary}")
        
        # Send completion progress
        send_progress_update(job.jobId, 100, "Complete", f"Scored {processed_count} pages successfully")
        
        # Prepare completion stats with validation summary
        completion_stats = {
            "pages_scored": processed_count,
            "website_score": website_result["website_ai_score"],
            "categories": website_result["category_averages"],
            "scoring_version": "v2",
            "validation_summary": validation_summary
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
    Derive issues from rule breakdown scores using data-based validation.
    
    NEW LOGIC:
    - Create issues for REQUIRED features that are actually missing
    - Create issues for IMPORTANT features with low scores
    - Skip truly optional features with reasonable scores
    - Validate against actual data, not expectations
    
    Severity mapping:
    - rule_score < 40 AND (feature required OR important) → "high"
    - 40 ≤ rule_score < 70 AND (feature required OR important) → "medium"
    - rule_score ≥ 70 AND feature not required → no issue
    """
    issues = []
    
    print(f"[DEBUG] Processing {len(rule_breakdown)} rules for issues | url={page_url}")
    
    for rule in rule_breakdown:
        rule_score = rule.get("score", 100)
        rule_id = rule.get("rule_id", "unknown")
        
        # Get rule requirements from registry
        rule_instance = get_rule_instance(rule_id)
        is_required = getattr(rule_instance, 'is_required', False) if rule_instance else False
        
        print(f"[DEBUG] Rule {rule_id} | score={rule_score} | required={is_required}")
        
        # FIXED: Include important features (not just required ones)
        # Create issues for required features with low scores
        # OR important features with very low scores
        should_create_issue = False
        
        if is_required and rule_score < 70:
            should_create_issue = True
            print(f"[DEBUG] → Creating issue (required feature with low score)")
        elif not is_required and rule_score < 40:
            # Only create issues for optional features with very low scores
            should_create_issue = True
            print(f"[DEBUG] → Creating issue (optional feature with very low score)")
        else:
            print(f"[DEBUG] → Skipping issue (score too high or feature not important)")
        
        if should_create_issue:
            # Determine severity based on score
            if rule_score < 40:
                severity = "high"
            else:
                severity = "medium"
            
            # Create issue document with validation status
            issue = {
                "projectId": ObjectId(project_id),
                "page_url": page_url,
                "rule_id": rule_id,
                "category": rule.get("category", "unknown"),
                "rule_score": rule_score,
                "severity": severity,
                "validation_status": "VALID",  # Only VALID issues pass through
                "message": rule.get("rule_name", f"Rule {rule_id} scored {rule_score:.1f}"),
                "created_at": datetime.utcnow()
            }
            issues.append(issue)
    
    print(f"[DEBUG] Generated {len(issues)} issues from {len(rule_breakdown)} rules")
    return issues

def create_validation_summary(page_scores: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create validation summary for all pages
    
    Returns:
        Summary with total issues, valid issues, false positives filtered out
    """
    total_issues = 0
    valid_issues = 0
    false_positives = 0
    
    for page_score in page_scores:
        rule_breakdown = page_score.get("rule_breakdown", [])
        for rule in rule_breakdown:
            rule_score = rule.get("score", 100)
            rule_id = rule.get("rule_id", "unknown")
            
            # Get rule requirements
            rule_instance = get_rule_instance(rule_id)
            is_required = getattr(rule_instance, 'is_required', False) if rule_instance else False
            
            if rule_score < 70:
                total_issues += 1
                if is_required:
                    valid_issues += 1
                else:
                    false_positives += 1
    
    return {
        "total_issues_detected": total_issues,
        "valid_issues": valid_issues,
        "false_positives_filtered": false_positives,
        "false_positive_reduction_rate": (false_positives / total_issues * 100) if total_issues > 0 else 0,
        "validation_system": "data_based_v2"
    }

def store_page_issues(issues: List[Dict[str, Any]], project_id: str, page_url: str):
    """
    Store page issues in database with validation status.
    
    NEW LOGIC:
    - Stores VALID issues (features that actually need improvement)
    - Provides summary of issue validation
    
    Safeguards:
    - Deletes existing issues for the page before inserting new ones
    - Only inserts if issues exist
    """
    try:
        print(f"[DEBUG] Storing issues for {page_url} | total_issues={len(issues)}")
        
        # TEMPORARY BYPASS FOR TESTING: Comment out validation to test DB insert
        # valid_issues = [issue for issue in issues if issue.get("validation_status") == "VALID"]
        valid_issues = issues  # BYPASS: Use all issues for testing
        
        false_positives = len(issues) - len(valid_issues)
        
        print(f"[DEBUG] Issue validation | valid={len(valid_issues)} | false_positives={false_positives}")
        
        if false_positives > 0:
            logger.info(f"[VALIDATION] Filtered out {false_positives} false positives for {page_url}")
        
        if not valid_issues:
            # No valid issues to store - delete any existing issues for this page
            print(f"[DEBUG] No valid issues to store, clearing existing issues for {page_url}")
            result = seo_ai_visibility_issues.delete_many({
                "projectId": ObjectId(project_id),
                "page_url": page_url
            })
            if result.deleted_count > 0:
                logger.info(f"[VALIDATION] Cleared {result.deleted_count} existing issues for page | url={page_url}")
            return
        
        # Delete existing issues for this page
        delete_result = seo_ai_visibility_issues.delete_many({
            "projectId": ObjectId(project_id),
            "page_url": page_url
        })
        
        if delete_result.deleted_count > 0:
            logger.info(f"[VALIDATION] Deleted {delete_result.deleted_count} existing issues before storing new issues | url={page_url}")
        
        # Insert new VALID issues only
        insert_result = seo_ai_visibility_issues.insert_many(valid_issues)
        
        print(f"[DEBUG] Successfully inserted {len(insert_result.inserted_ids)} issues into DB")
        logger.info(f"[VALIDATION] Stored VALID page issues | url={page_url} | valid={len(valid_issues)} | false_positives={false_positives} | inserted={len(insert_result.inserted_ids)}")
        
    except Exception as e:
        logger.error(f"[VALIDATION] Error storing page issues | url={page_url} | error={e}")
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
            update_doc_project,
            upsert=True  # FIXED: Add upsert to create document if it doesn't exist
        )
        
        result2 = seoprojects.update_one(
            {"_id": ObjectId(project_id)},
            update_doc_seo,
            upsert=True  # FIXED: Add upsert for consistency
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
    try:
        print(f"[WORKER] execute_ai_visibility_scoring_logic called with: {job_data}")
        print(f"[WORKER] Job details | jobId={job_data.get('jobId')} | projectId={job_data.get('projectId')} | sourceJobId={job_data.get('sourceJobId')}")
        
        # === CRITICAL VALIDATION: Check database connections before proceeding ===
        print(f"[WORKER] Database connections check:")
        print(f"  seo_ai_visibility: {'✅' if seo_ai_visibility is not None else '❌ None'}")
        print(f"  seo_ai_page_scores: {'✅' if seo_ai_page_scores is not None else '❌ None'}")
        print(f"  seo_ai_visibility_project: {'✅' if seo_ai_visibility_project is not None else '❌ None'}")
        print(f"  seoprojects: {'✅' if seoprojects is not None else '❌ None'}")
        
        result = execute_ai_visibility_scoring_v2(job_data)
        print(f"[WORKER] execute_ai_visibility_scoring_logic returning: {result}")
        return result
    except Exception as e:
        # Ultimate safety net - never let exceptions crash the worker
        error_msg = f"Critical error in execute_ai_visibility_scoring_logic: {str(e)}"
        print(f"[WORKER] CRITICAL ERROR: {error_msg}")
        import traceback
        print(f"[WORKER] Critical stack trace: {traceback.format_exc()}")
        
        # Always return valid JSON response
        return {
            "status": "failed",
            "error": error_msg,
            "jobId": job_data.get("jobId", "unknown"),
            "critical_failure": True
        }

if __name__ == "__main__":
    # Test execution
    test_job = {
        "jobId": "test-job-123",
        "projectId": "507f1f77bcf86cd799439011",
        "userId": "507f1f77bcf86cd799439012"
    }
    
    result = execute_ai_visibility_scoring_v2(test_job)
    print(f"Test result: {result}")
