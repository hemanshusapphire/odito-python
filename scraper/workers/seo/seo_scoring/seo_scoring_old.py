"""SEO scoring worker implementation."""

import threading
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import Dict, List, Any

# Import shared utilities
from scraper.shared.utils import send_completion_callback, send_failure_callback
from scraper.shared.schema import validate_seo_scoring_input
from main import is_job_cancelled

# Global MongoDB connection (shared with main)
db_client = None
db = None

def get_db_connection():
    """Get MongoDB connection (shared with main)"""
    global db_client, db
    if db_client is None:
        # Use same connection string as main
        import os
        from env_config import get_config
        config = get_config()
        mongo_uri = config.get('database.uri')
        db_client = MongoClient(mongo_uri)
        db = db_client.get_database()
    return db

def is_page_valid(page_data: Dict[str, Any]) -> bool:
    """Check if page is valid for scoring based on rules"""
    try:
        print(f"[DEBUG] Validating page | keys={list(page_data.keys())} | jobId={getattr(page_data, 'jobId', 'unknown')}")
        
        # Rule 1: scrape_status == "SUCCESS" (relaxed - check multiple field names)
        scrape_status = (page_data.get("scrape_status") or 
                         page_data.get("scrapeStatus") or 
                         page_data.get("status"))
        
        if scrape_status != "SUCCESS":
            print(f"[DEBUG] Failed scrape_status rule | status={scrape_status}")
            return False
        
        # Rule 2: http_status_code in 2xx (relaxed - check multiple field names)
        http_status = (page_data.get("http_status_code") or 
                      page_data.get("httpStatusCode") or 
                      page_data.get("status_code"))
        
        if not http_status or not (200 <= http_status < 300):
            print(f"[DEBUG] Failed http_status rule | status={http_status}")
            return False
        
        # Rule 3: meta_tags.robots != "noindex" (relaxed - check multiple field names)
        meta_tags = (page_data.get("meta_tags") or 
                    page_data.get("metaTags") or 
                    page_data.get("meta") or {})
        
        if isinstance(meta_tags, dict):
            robots_directive = (meta_tags.get("robots") or 
                              meta_tags.get("robotsMeta") or 
                              meta_tags.get("robot"))
        else:
            robots_directive = ""
            
        if "noindex" in str(robots_directive).lower():
            print(f"[DEBUG] Failed robots rule | robots={robots_directive}")
            return False
        
        print(f"[DEBUG] Page validation passed | status={scrape_status} | http={http_status} | robots={robots_directive}")
        return True
    except Exception as e:
        print(f"Error validating page: {e}")
        return False

def calculate_page_score(issues: List[Dict[str, Any]]) -> int:
    """Calculate SEO score for page based on issues"""
    try:
        total_penalty = 0
        
        for issue in issues:
            # Handle multiple possible severity field names
            severity = (issue.get("severity") or 
                       issue.get("level") or 
                       issue.get("priority") or 
                       issue.get("type")).lower()
            
            if severity == "high" or severity == "critical":
                total_penalty += 15
            elif severity == "medium" or severity == "moderate":
                total_penalty += 8
            elif severity == "low" or severity == "info":
                total_penalty += 3
            else:
                total_penalty += 3  # Default to low penalty
        
        # Calculate score (100 - total penalties, minimum 0)
        score = max(0, 100 - total_penalty)
        return score
    except Exception as e:
        print(f"Error calculating page score: {e}")
        return 0

def execute_seo_scoring_logic(job):
    """Execute SEO scoring job logic"""
    try:
        print(f"[WORKER] SEO_SCORING started | jobId={job.jobId}")
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            print(f"[WORKER] SEO_SCORING cancelled | jobId={job.jobId}")
            return
        
        db = get_db_connection()
        
        # DEBUG: Log raw counts first
        total_pages = db.seo_page_data.count_documents({"projectId": job.projectId})
        total_issues = db.seo_page_issues.count_documents({"projectId": job.projectId})
        print(f"[DEBUG] Raw data | pages={total_pages} | issues={total_issues} | jobId={job.jobId}")
        
        # Get page issues for the project (relaxed query initially)
        page_issues_cursor = db.seo_page_issues.find({
            "projectId": job.projectId
        })
        
        # DEBUG: Check first issue document structure
        first_issue = None
        for issue_doc in page_issues_cursor:
            first_issue = issue_doc
            break
        
        if first_issue:
            print(f"[DEBUG] First issue structure | keys={list(first_issue.keys())} | jobId={job.jobId}")
            # Reset cursor to get all issues
            page_issues_cursor = db.seo_page_issues.find({
                "projectId": job.projectId
            })
        else:
            print(f"[DEBUG] No issues found | jobId={job.jobId}")
        
        # Group issues by page URL - handle multiple possible field names
        page_issues_by_url = {}
        for issue_doc in page_issues_cursor:
            # Try multiple possible URL field names
            page_url = (issue_doc.get("page_url") or 
                        issue_doc.get("url") or 
                        issue_doc.get("pageUrl") or 
                        issue_doc.get("link_url"))
            
            if page_url:
                if page_url not in page_issues_by_url:
                    page_issues_by_url[page_url] = []
                page_issues_by_url[page_url].append(issue_doc)
        
        print(f"[WORKER] Found issues for {len(page_issues_by_url)} pages | jobId={job.jobId}")
        
        # Get page data for validation (relaxed query)
        page_data_cursor = db.seo_page_data.find({
            "projectId": job.projectId
        })
        
        # DEBUG: Check first page data structure
        first_page = None
        for page_doc in page_data_cursor:
            first_page = page_doc
            break
        
        if first_page:
            print(f"[DEBUG] First page structure | keys={list(first_page.keys())} | jobId={job.jobId}")
            # Reset cursor to get all pages
            page_data_cursor = db.seo_page_data.find({
                "projectId": job.projectId
            })
        else:
            print(f"[DEBUG] No page data found | jobId={job.jobId}")
        
        # Create lookup for page data by URL - handle multiple possible field names
        page_data_by_url = {}
        for page_doc in page_data_cursor:
            # Try multiple possible URL field names
            page_url = (page_doc.get("url") or 
                        page_doc.get("page_url") or 
                        page_doc.get("pageUrl") or 
                        page_doc.get("link_url"))
            
            if page_url:
                page_data_by_url[page_url] = page_doc
        
        print(f"[DEBUG] Page data lookup created | pages={len(page_data_by_url)} | jobId={job.jobId}")
        
        # Calculate scores for valid pages
        page_scores = []
        total_score = 0
        valid_pages_count = 0
        
        for page_url, issues in page_issues_by_url.items():
            # Check cancellation periodically
            if is_job_cancelled(job.jobId):
                print(f"[WORKER] SEO_SCORING cancelled during processing | jobId={job.jobId}")
                return
            
            # Get page data for validation
            page_data = page_data_by_url.get(page_url, {})
            
            # Validate page with relaxed rules initially
            if not is_page_valid(page_data):
                print(f"[WORKER] Skipping invalid page | url={page_url} | jobId={job.jobId}")
                continue
            
            valid_pages_count += 1
            
            # Calculate page score
            page_score = calculate_page_score(issues)
            
            # Create score document
            score_doc = {
                "projectId": job.projectId,
                "page_url": page_url,
                "page_score": page_score,
                "issues_count": len(issues),
                "high_issues_count": len([i for i in issues if (i.get("severity") or i.get("level") or i.get("priority") or "low").lower() in ["high", "critical"]]),
                "medium_issues_count": len([i for i in issues if (i.get("severity") or i.get("level") or i.get("priority") or "low").lower() in ["medium", "moderate"]]),
                "low_issues_count": len([i for i in issues if (i.get("severity") or i.get("level") or i.get("priority") or "low").lower() in ["low", "info"]]),
                "jobId": job.jobId,
                "scored_at": datetime.utcnow(),
                "sourceJobId": job.sourceJobId
            }
            
            page_scores.append(score_doc)
            total_score += page_score
            valid_pages_count += 1
        
        # Calculate website score (average of page scores)
        website_score = total_score / valid_pages_count if valid_pages_count > 0 else 0
        
        print(f"[WORKER] Calculated scores | pages={valid_pages_count} | website_score={website_score:.2f} | jobId={job.jobId}")
        
        # Bulk upsert with idempotency (unique index on projectId, page_url)
        if page_scores:
            # Create bulk operations for upsert
            bulk_ops = []
            for score_doc in page_scores:
                bulk_ops.append({
                    "replaceOne": {
                        "filter": {
                            "projectId": score_doc["projectId"],
                            "page_url": score_doc["page_url"]
                        },
                        "replacement": score_doc,
                        "upsert": True
                    }
                })
            
            # Execute bulk operation
            result = db.seo_page_scores.bulk_write(bulk_ops)
            print(f"[WORKER] Bulk upsert completed | matched={result.matched_count} | upserted={result.upserted_count} | jobId={job.jobId}")
        
        # Prepare completion stats
        stats = {
            "pagesScored": valid_pages_count,
            "websiteScore": round(website_score, 2),
            "totalIssues": sum(len(issues) for issues in page_issues_by_url.values()),
            "highIssues": sum(len([i for i in issues if i.get("severity", "low").lower() == "high"]) for issues in page_issues_by_url.values()),
            "mediumIssues": sum(len([i for i in issues if i.get("severity", "low").lower() == "medium"]) for issues in page_issues_by_url.values()),
            "lowIssues": sum(len([i for i in issues if i.get("severity", "low").lower() == "low"]) for issues in page_issues_by_url.values())
        }
        
        # Send completion callback
        send_completion_callback(job.jobId, stats)
        print(f"[WORKER] SEO_SCORING completed | jobId={job.jobId} | pagesScored={valid_pages_count} | websiteScore={website_score:.2f}")
        
    except Exception as e:
        print(f"[ERROR] SEO_SCORING failed | jobId={job.jobId} | error={str(e)}")
        send_failure_callback(job.jobId, str(e))
