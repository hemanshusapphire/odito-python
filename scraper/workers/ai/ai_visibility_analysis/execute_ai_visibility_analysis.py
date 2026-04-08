import os
from datetime import datetime
from bson.objectid import ObjectId
from fastapi import HTTPException
import requests

# Import AI visibility collections
from db import seo_ai_visibility, seo_ai_visibility_issues, db
from scraper.shared.utils import send_completion_callback, send_failure_callback

# Import the analysis function
from .ai_visibility_analysis import analyze_ai_visibility_page

def execute_ai_visibility_analysis_logic(job):
    """Execute AI_VISIBILITY_ANALYSIS job logic - mirrors execute_page_analysis_logic()"""
    # Track start time for duration calculation
    start_time = datetime.utcnow()
    duration_ms = 0  # Initialize before try block
    
    """Execute AI_VISIBILITY_ANALYSIS job logic"""
    try:
        # Validation guard - ensure job_id is available
        job_id = getattr(job, 'jobId', None)
        assert job_id is not None, "job_id missing in AI_VISIBILITY_ANALYSIS"
        
        # Validation guard - ensure project_id is available
        project_id = getattr(job, 'projectId', None)
        assert project_id is not None, "project_id missing in AI_VISIBILITY_ANALYSIS"
        
        # Convert project_id to ObjectId for MongoDB queries
        project_object_id = ObjectId(project_id)
        
        print(f"[WORKER] AI_VISIBILITY_ANALYSIS started | jobId={job_id} | projectId={project_id}")
        
        # STEP 1: Load all AI visibility data for this project
        try:
            pages = list(seo_ai_visibility.find({
                "projectId": project_object_id
            }))
            
            print(f"[WORKER] AI_VISIBILITY_ANALYSIS loaded {len(pages)} pages | jobId={job_id}")
            
            # DEBUG: Print page URLs to verify data
            if pages:
                print(f"[DEBUG] Sample page URLs: {[p.get('url', 'NO_URL') for p in pages[:3]]}")
                print(f"[DEBUG] First page keys: {list(pages[0].keys()) if pages else 'NO_PAGES'}")
            else:
                print(f"[DEBUG] No pages found for projectId={project_object_id}")
                # DEBUG: Check if any pages exist without project filter
                all_pages = list(seo_ai_visibility.find({}))
                print(f"[DEBUG] Total pages in collection: {len(all_pages)}")
                if all_pages:
                    sample_project_ids = [p.get('projectId') for p in all_pages[:3]]
                    print(f"[DEBUG] Sample projectIds: {sample_project_ids}")
            
        except Exception as load_error:
            print(f"[ERROR] Failed to load AI visibility data: {load_error}")
            send_failure_callback(job_id, f"Failed to load AI visibility data: {load_error}")
            return {
                "status": "failed",
                "error": str(load_error),
                "jobId": job_id
            }
        
        if not pages:
            print(f"[WORKER] AI_VISIBILITY_ANALYSIS completed | jobId={job_id} | analyzed=0 | failed=0 | issues=0")
            
            stats = {
                "totalPages": 0,
                "pagesAnalyzed": 0,
                "issuesFound": 0
            }
            send_completion_callback(job_id, stats)
            
            return {
                "status": "completed",
                "jobId": job_id,
                "stats": stats
            }
        
        # STEP 2: Analyze all pages for AI visibility issues
        all_issues = []
        successful_analyses = 0
        failed_analyses = 0
        
        for page in pages:
            try:
                page_url = page.get("url", "")
                if not page_url:
                    print(f"[WARNING] Skipping page without URL | jobId={job_id}")
                    failed_analyses += 1
                    continue
                
                print(f"[WORKER] Analyzing AI visibility for page: {page_url} | jobId={job_id}")
                
                # Analyze page for AI visibility issues
                page_issues = analyze_ai_visibility_page(page, job_id, project_id)
                
                if page_issues:
                    all_issues.extend(page_issues)
                    print(f"[WORKER] Found {len(page_issues)} AI visibility issues for {page_url} | jobId={job_id}")
                else:
                    print(f"[WORKER] No AI visibility issues found for {page_url} | jobId={job_id}")
                
                successful_analyses += 1
                
            except Exception as page_error:
                print(f"[ERROR] Failed to analyze AI visibility for page: {page_error} | jobId={job_id}")
                failed_analyses += 1
                continue
        
        # STEP 3: Clean existing issues for this project to prevent accumulation
        try:
            delete_result = seo_ai_visibility_issues.delete_many({
                "projectId": project_object_id
            })
            print(f"[WORKER] AI_VISIBILITY_ANALYSIS cleaned existing issues | deleted={delete_result.deleted_count} | jobId={job_id}")
        except Exception as cleanup_error:
            print(f"[ERROR] Failed to cleanup existing AI visibility issues: {cleanup_error}")
        
        # STEP 4: Store all issues in bulk
        if all_issues:
            try:
                result = seo_ai_visibility_issues.insert_many(all_issues, ordered=False)
                print(f"[WORKER] AI_VISIBILITY_ANALYSIS inserted: {len(result.inserted_ids)} issues | jobId={job_id}")
            except Exception as insert_error:
                print(f"[WORKER] AI_VISIBILITY_ANALYSIS insert failed: {insert_error}")
                
                # Try one by one to see which fail
                for i, issue in enumerate(all_issues):
                    try:
                        seo_ai_visibility_issues.insert_one(issue)
                        print(f"[WORKER] AI_VISIBILITY_ANALYSIS inserted {i}: {issue.get('issue_code')} | jobId={job_id}")
                    except Exception as single_error:
                        print(f"[WORKER] AI_VISIBILITY_ANALYSIS failed {i}: {issue.get('issue_code')} - {single_error} | jobId={job_id}")
        else:
            print("[WORKER] AI_VISIBILITY_ANALYSIS no issues to insert!")
        
        # STEP 4: Calculate completion stats
        duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        print(f"[WORKER] AI_VISIBILITY_ANALYSIS completed | jobId={job_id} | analyzed={successful_analyses} | failed={failed_analyses} | issues={len(all_issues)}")
        
        stats = {
            "totalPages": len(pages),
            "pagesAnalyzed": successful_analyses,
            "issuesFound": len(all_issues),
            "failedAnalyses": failed_analyses,
            "duration_ms": duration_ms
        }
        
        # STEP 5: Send completion callback
        send_completion_callback(job_id, stats)
        
        return {
            "status": "completed",
            "jobId": job_id,
            "stats": stats,
            "issuesFound": len(all_issues),
            "pagesAnalyzed": successful_analyses
        }
        
    except AssertionError as assert_error:
        print(f"[ERROR] AI_VISIBILITY_ANALYSIS validation failed: {assert_error} | jobId={job_id}")
        send_failure_callback(job_id, f"Validation failed: {assert_error}")
        return {
            "status": "failed",
            "error": str(assert_error),
            "jobId": job_id
        }
        
    except Exception as e:
        print(f"[ERROR] AI_VISIBILITY_ANALYSIS failed: {str(e)} | jobId={job_id}")
        send_failure_callback(job_id, str(e))
        return {
            "status": "failed",
            "error": str(e),
            "jobId": job_id
        }
