import re

import os

from datetime import datetime

from bson.objectid import ObjectId

from fastapi import HTTPException

import requests

from db import seo_internal_links, seo_external_links, seo_social_links, seo_page_data, seo_page_issues, seo_page_performance, seo_page_summary, db
from urllib.parse import urlparse






def safe_string(value):

    """Safely extract string value from various data types"""

    if isinstance(value, str):

        return value

    if isinstance(value, list) and value and isinstance(value[0], str):

        return value[0]

    return ""



def normalize_text(value):

    """Normalize text values from seo_page_data - prevents all .split() errors"""

    if value is None:

        return ""

    if isinstance(value, str):

        return value.strip()

    if isinstance(value, list):

        if value and isinstance(value[0], str):

            return value[0].strip()

        elif value:

            return str(value[0]).strip()

        return ""

    if isinstance(value, dict):

        # Handle nested objects from scraper

        text_val = value.get("text") or value.get("content") or value.get("value")

        if text_val:

            return str(text_val).strip()

        return ""

    return str(value).strip()


def _normalize_lookup_url(url):
    """Normalize URL for consistent lookup matching.
    Strips trailing slashes, lowercases scheme+host, preserves path casing.
    Prevents silent lookup misses from trivial URL differences."""
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    parsed = urlparse(url)
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").lower()
    path = parsed.path.rstrip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{host}{path}{query}" if host else url.rstrip("/")



def send_progress_update(job_id: str, percentage: int, step: str, message: str, subtext: str = None):

    """Send progress update to Node.js backend"""

    try:

        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")

        progress_url = f"{node_backend_url}/api/jobs/{job_id}/progress"

        

        payload = {

            "percentage": percentage,

            "step": step,

            "message": message,

            "subtext": subtext

        }

        

        response = requests.post(progress_url, json=payload, timeout=5)

        response.raise_for_status()

        

    except Exception as e:

        # Don't raise exception - progress updates are non-critical

        pass



def execute_page_analysis_logic(job):

    # Track start time for duration calculation

    start_time = datetime.utcnow()

    duration_ms = 0  # Initialize before try block

    

    """Execute PAGE_ANALYSIS job logic"""

    try:

        # Validation guard - ensure job_id is available

        job_id = getattr(job, 'jobId', None)

        assert job_id is not None, "job_id missing in PAGE_ANALYSIS"

        

        print(f"[WORKER] PAGE_ANALYSIS started | jobId={job_id}")

        

        # Check for cancellation early

        if is_job_cancelled(job_id):

            print(f"[WORKER] PAGE_ANALYSIS cancelled | jobId={job_id}")

            return {"status": "cancelled", "jobId": job_id, "message": "Job cancelled by user"}

        

        # Query pages scraped by the project (not by specific jobId)

        pages = list(seo_page_data.find({

            "projectId": ObjectId(job.projectId),

            "scrape_status": "SUCCESS"

        }))

        

        # Also fetch performance data for this project

        performance_data = list(seo_page_performance.find({

            "projectId": ObjectId(job.projectId)

        }))

        

        # Create lookup map for performance data by page_url and device_type

        performance_lookup = {}

        for perf in performance_data:

            page_url = perf.get("page_url", "")

            device_type = perf.get("device_type", "")

            if page_url and device_type:

                if page_url not in performance_lookup:

                    performance_lookup[page_url] = {}

                performance_lookup[page_url][device_type] = perf

        

        # Bulk-fetch remaining collections (once per job, not per page)

        technical_report = db.domain_technical_report.find_one({"projectId": ObjectId(job.projectId)}) or {}

        headless_data_list = list(db.seo_headless_data.find({"projectId": ObjectId(job.projectId)}))
        headless_lookup = {_normalize_lookup_url(h.get("url", "")): h for h in headless_data_list}
        
        # Process headless data for accessibility analysis

        crawl_graph_list = list(db.seo_crawl_graph.find({"projectId": ObjectId(job.projectId)}))
        crawl_graph_lookup = {_normalize_lookup_url(c.get("page_url", "")): c for c in crawl_graph_list}

        print(f"[WORKER] PAGE_ANALYSIS started | jobId={job_id} | pagesFound={len(pages)} | performanceRecords={len(performance_data)} | headlessRecords={len(headless_data_list)} | crawlGraphRecords={len(crawl_graph_list)}")

        

        if not pages:

            print(f"[WORKER] PAGE_ANALYSIS completed | jobId={job_id} | analyzed=0 | failed=0 | issues=0")

            stats = {

                "totalPages": 0,

                "pagesAnalyzed": 0,

                "issuesFound": 0

            }

            send_completion_callback(job_id, stats)

            return {

                "status": "success",

                "jobId": job_id,

                "stats": stats,

                "message": "No pages to analyze"

            }

        

        print(f"[WORKER] PAGE_ANALYSIS started | jobId={job_id} | pagesFound={len(pages)}")

        

        # Analyze each page and collect issues and summaries
        all_issues = []
        all_summaries = []
        successful_analyses = 0
        failed_analyses = 0
        completed_analyses = 0
        total_pages = len(pages)

        

        for page in pages:

            try:

                # Check cancellation before processing each page

                if is_job_cancelled(job_id):

                    print(f"🛑 Job {job_id} cancelled during analysis")

                    return {"status": "cancelled", "jobId": job_id, "message": "Job cancelled by user"}

                

                page_result = analyze_page_seo(
                    page, job_id, job.projectId,
                    performance_lookup=performance_lookup,
                    headless_lookup=headless_lookup,
                    crawl_graph_lookup=crawl_graph_lookup,
                    technical_report=technical_report
                )

                # Extract issues and summary from new engine return format
                page_issues = page_result.get("issues", [])
                page_summary = page_result.get("summary", {})

                # Create summary document for storage
                summary_doc = {
                    "projectId": ObjectId(job.projectId),
                    "seo_jobId": ObjectId(job_id),
                    "page_url": page.get("url"),
                    **page_summary,
                    "created_at": datetime.utcnow()
                }
                all_summaries.append(summary_doc)
                all_issues.extend(page_issues)

                successful_analyses += 1

                completed_analyses += 1

                

                # Send progress update after each successful page analysis

                percentage = int((completed_analyses / total_pages) * 100)

                send_progress_update(

                    job_id, 

                    percentage, 

                    "Analyzing", 

                    "Analyzing SEO issues", 

                    f"{completed_analyses} of {total_pages} pages analyzed"

                )

                

            except Exception as page_error:

                print(f"[ERROR] PAGE_ANALYSIS page failed | jobId={job_id} | url={page.get('url', 'unknown')} | reason=\"{page_error}\"")

                failed_analyses += 1

                completed_analyses += 1

                

                # Send progress update after each failed page analysis

                percentage = int((completed_analyses / total_pages) * 100)

                send_progress_update(

                    job_id, 

                    percentage, 

                    "Analyzing", 

                    "Analyzing SEO issues", 

                    f"{completed_analyses} of {total_pages} pages analyzed"

                )

                

                continue

        

        # 🔥 HARD LOGS: PAGE_ANALYSIS INSERT
        print(f"PAGE_ANALYSIS inserting: {len(all_issues)} total issues")
        print(f"PAGE_ANALYSIS inserting: {len(all_summaries)} total summaries")

        # Insert issues (existing logic)
        if all_issues:
            try:
                result = seo_page_issues.insert_many(all_issues, ordered=False)
                print(f"PAGE_ANALYSIS inserted: {len(result.inserted_ids)} issues")
            except Exception as insert_error:
                print(f"PAGE_ANALYSIS insert failed: {insert_error}")
                # Try one by one to see which fail
                for i, issue in enumerate(all_issues):
                    try:
                        seo_page_issues.insert_one(issue)
                        print(f"PAGE_ANALYSIS inserted {i}: {issue.get('issue_code')}")
                    except Exception as single_error:
                        print(f"PAGE_ANALYSIS failed {i}: {issue.get('issue_code')} - {single_error}")
        else:
            print("PAGE_ANALYSIS no issues to insert!")

        # Insert summaries (new logic)
        if all_summaries:
            try:
                result = seo_page_summary.insert_many(all_summaries, ordered=False)
                print(f"PAGE_ANALYSIS inserted: {len(result.inserted_ids)} summaries")
            except Exception as insert_error:
                print(f"PAGE_ANALYSIS summary insert failed: {insert_error}")
                # Try one by one to see which fail
                for i, summary in enumerate(all_summaries):
                    try:
                        seo_page_summary.insert_one(summary)
                        print(f"PAGE_ANALYSIS inserted summary {i}: {summary.get('page_url')}")
                    except Exception as single_error:
                        print(f"PAGE_ANALYSIS failed summary {i}: {summary.get('page_url')} - {single_error}")
        else:
            print("PAGE_ANALYSIS no summaries to insert!")

        

        # Update seo_internal_links with analysis completion status

        try:

            # Get all URLs that were analyzed for this job

            analyzed_urls = [page.get("url") for page in pages]

            

            # Update each internal link with analysis completion

            if analyzed_urls:

                # Trace back to original LINK_DISCOVERY job ID

                # PAGE_ANALYSIS -> PAGE_SCRAPING -> LINK_DISCOVERY

                original_seo_job_id = None

                if hasattr(job, 'sourceJobId') and job.sourceJobId:

                    # Get the PAGE_SCRAPING job to find its source (LINK_DISCOVERY)

                    page_scraping_job = db.jobs.find_one({"_id": ObjectId(job.sourceJobId)})

                    if page_scraping_job and page_scraping_job.get("input_data", {}).get("source_job_id"):

                        original_seo_job_id = page_scraping_job["input_data"]["source_job_id"]

                

                # Fallback to current job ID if tracing fails

                target_seo_job_id = ObjectId(original_seo_job_id) if original_seo_job_id else ObjectId(job.jobId)

                

                seo_internal_links.update_many(

                    {"url": {"$in": analyzed_urls}, "seo_jobId": target_seo_job_id},

                    {

                        "$set": {

                            "analyzedAt": datetime.utcnow()

                        }

                    }

                )

                print(f"[WORKER] Updated internal links analysis status | jobId={job_id} | analyzed={len(analyzed_urls)}")

        except Exception as update_error:

            print(f"[ERROR] Failed to update internal links analysis status | jobId={job_id} | reason=\"{str(update_error)}\"")

        

        # Prepare completion stats

        stats = {

            "totalPages": len(pages),

            "pagesAnalyzed": successful_analyses,

            "failedAnalyses": failed_analyses,

            "issuesFound": len(all_issues)

        }



        # Calculate duration before sending callbacks

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        

        # Store stats in result_data for summary aggregation

        result_data = {

            "totalPages": len(pages),

            "pagesAnalyzed": successful_analyses,

            "failedAnalyses": failed_analyses,

            "issuesFound": len(all_issues),

            "duration_ms": duration_ms

        }

        

        print(f"[WORKER] PAGE_ANALYSIS completed | jobId={job_id} | analyzed={successful_analyses} | failed={failed_analyses} | issues={len(all_issues)}")

        

        # Update seo_internal_links with analysis status

        try:

            # Get all URLs that were successfully analyzed

            analyzed_urls = [page.get("url") for page in pages if page.get("url")]

            

            if analyzed_urls:

                result = seo_internal_links.update_many(

                    {"url": {"$in": analyzed_urls}, "projectId": ObjectId(job.projectId)},

                    {

                        "$set": {

                            "analyzedAt": datetime.utcnow()

                        }

                    }

                )

                print(f"[WORKER] Updated internal links analysis status | jobId={job_id} | matched={result.matched_count} | modified={result.modified_count}")

                if result.matched_count == 0:

                    print(f"[WARNING] No matching internal links found for analysis status update | jobId={job_id} | projectId={job.projectId}")

                elif result.modified_count < result.matched_count:

                    print(f"[WARNING] Some internal links already analyzed | jobId={job_id} | modified={result.modified_count}/{result.matched_count}")

        except Exception as update_error:

            print(f"[ERROR] Failed to update internal links analysis status | jobId={job_id} | reason=\"{str(update_error)}\"")

        

        # Send completion callback to Node.js

        send_completion_callback(job_id, stats, result_data)

        

        # Send crawl summary to Node.js (additional call)

        send_crawl_summary(job_id, job.projectId, stats, duration_ms, job.sourceJobId)

        

        return {

            "status": "success",

            "jobId": job_id,

            "stats": stats,

            "duration_ms": duration_ms,

            "message": "Page analysis completed and issues stored"

        }

        

    except Exception as e:

        print(f"[ERROR] PAGE_ANALYSIS failed | jobId={job_id} | reason=\"{str(e)}\"")

        

        # Safely compute duration even in exception case

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        

        # Send failure callback to Node.js

        send_failure_callback(job_id, str(e))

        

        raise HTTPException(status_code=500, detail=f"Page analysis failed: {str(e)}")



def normalize_page_data(page):

    """Centralized normalization function for all SEO data extraction."""

    # Convert content.headings dict to flat list of heading objects

    content_headings = page.get("content", {}).get("headings", {})

    headings_list = []

    if content_headings and isinstance(content_headings, dict):

        for level, headings in content_headings.items():

            if isinstance(headings, list):

                for text in headings:

                    headings_list.append({

                        "tag": level,

                        "text": text,

                        "level": int(level[1:]) if level.startswith("h") else 0

                    })

    

    # Convert tracking to synthetic scripts list for analytics rules

    tracking = page.get("tracking", {})

    scripts_list = []

    # IMPORTANT: Handle tracking even if empty dict to preserve structure
    # Empty dict is falsy in Python, so we check if it's a dict instead
    if isinstance(tracking, dict):

        # Create synthetic script objects from tracking data

        # Check both old field names and new analytics_detected flag for compatibility
        if tracking.get("google_analytics") or tracking.get("analytics_detected"):

            scripts_list.append({

                "src": "https://www.googletagmanager.com/gtag/js?id=GA-ANALYTICS",

                "content": "Google Analytics",

                "type": "analytics"

            })

        # Check for GTM via old or new field names
        has_gtm = tracking.get("google_tag_manager") or ("GTM" in tracking.get("analytics_types", []))
        if has_gtm:

            scripts_list.append({

                "src": "https://www.googletagmanager.com/gtm.js?id=GTM-CONTAINER",

                "content": "Google Tag Manager",

                "type": "analytics"

            })

        if tracking.get("facebook_pixel"):

            scripts_list.append({

                "src": "https://connect.facebook.net/en_US/fbevents.js",

                "content": "Facebook Pixel",

                "type": "tracking"

            })

        if tracking.get("linkedin_pixel"):

            scripts_list.append({

                "src": "https://px.ads.linkedin.com/collect/",

                "content": "LinkedIn Pixel",

                "type": "tracking"

            })

    

    # Normalize images: convert width/height to integers if numeric strings
    images_normalized = []
    for img in page.get("images", []):
        normalized_img = dict(img)  # Shallow copy
        # Convert width to int if it's a numeric string
        if normalized_img.get("width") is not None:
            try:
                if isinstance(normalized_img["width"], str) and normalized_img["width"].isdigit():
                    normalized_img["width"] = int(normalized_img["width"])
            except (ValueError, TypeError):
                pass  # Keep original value if conversion fails
        # Convert height to int if it's a numeric string
        if normalized_img.get("height") is not None:
            try:
                if isinstance(normalized_img["height"], str) and normalized_img["height"].isdigit():
                    normalized_img["height"] = int(normalized_img["height"])
            except (ValueError, TypeError):
                pass  # Keep original value if conversion fails
        images_normalized.append(normalized_img)

    # Normalize all fields with proper fallbacks - FIXED MAPPING
    return {
        # Top-level fields (correct as-is)
        "url": normalize_text(page.get("url")),
        "title": normalize_text(page.get("title")),
        "canonical": normalize_text(page.get("canonical")),
        "html_lang": page.get("html_lang"),
        "structured_data": page.get("structured_data", []),
        "hreflangs": page.get("hreflangs", []),
        "images": images_normalized,
        "image_analysis": page.get("image_analysis", {}),
        "tracking": tracking,
        "doctype": page.get("doctype_present"),
        "theme_color_present": page.get("theme_color_present", False),

        # NESTED fields — these were all broken before
        "headings": headings_list,  # Already properly converted from content.headings
        "content_text": normalize_text(page.get("content", {}).get("text")),
        "word_count": page.get("content", {}).get("word_count", 0),
        "meta_description": normalize_text(page.get("meta_tags", {}).get("description", "")),
        "viewport": normalize_text(page.get("meta_tags", {}).get("viewport", "")),

        # OG tags — rebuild as flat dict from meta_tags (FIXED)
        "og_tags": {
            "title": normalize_text(page.get("meta_tags", {}).get("og:title", "")),
            "description": normalize_text(page.get("meta_tags", {}).get("og:description", "")),
            "image": normalize_text(page.get("meta_tags", {}).get("og:image", "")),
            "url": normalize_text(page.get("meta_tags", {}).get("og:url", "")),
            "type": normalize_text(page.get("meta_tags", {}).get("og:type", "")),
        },

        # Social — map from actual social structure
        "social": page.get("social", {}),

        # meta_tags — pass full dict for charset, robots, author, keywords rules
        "meta_tags": page.get("meta_tags", {}),

        # Scripts — synthetic list from tracking (keep existing logic)
        "scripts": scripts_list,

        # Include new page signals for enhanced rule compatibility
        "review_schema_present": page.get("review_schema_present", False),
        "hreflang_present": page.get("hreflang_present", False)
    }



def _is_document_complete(normalized):
    """
    Returns False if the document is missing critical content fields.
    These indicate a failed or incomplete scrape — rules should not run.
    """
    required_fields = ["title", "content_text", "headings", "images"]
    missing = [f for f in required_fields if not normalized.get(f)]
    if len(missing) >= 3:  # if 3+ core fields missing, skip this page
        return False
    return True


def analyze_page_seo(page, job_id, project_id,
                     performance_lookup=None, headless_lookup=None,
                     crawl_graph_lookup=None, technical_report=None):

    """Analyze a single page for SEO issues using modular rule engine"""

    try:

        normalized = normalize_page_data(page)

        url = normalized["url"]

    except Exception as norm_error:

        print(f"[ERROR] Normalization failed for {page.get('url', 'unknown')}: {norm_error}")

        return {"issues": [], "summary": {"skipped": True, "reason": "normalization_failed"}}

    # Check document completeness BEFORE running any rules
    if not _is_document_complete(normalized):
        print(f"[SKIP] Incomplete document for {url} — scrape may have failed")
        return {"issues": [], "summary": {"skipped": True, "reason": "incomplete_document"}}

    # Build unified rule context (superset of normalized — backward compatible)
    lookup_url = _normalize_lookup_url(url)
    
    # Match headless data for this URL
    headless_match = (headless_lookup or {}).get(lookup_url)
    
    rule_context = {
        **normalized,
        "performance": (performance_lookup or {}).get(lookup_url, (performance_lookup or {}).get(url, {})),
        "headless": (headless_lookup or {}).get(lookup_url, {}),
        "crawl_graph": (crawl_graph_lookup or {}).get(lookup_url, {}),
        "technical_report": technical_report or {},
    }
    
    # Process headless data for accessibility analysis

    # Use the modular rule engine
    from scraper.workers.seo.page_analysis.rules.seo_rule_engine import get_seo_engine

    engine = get_seo_engine()

    return engine.analyze_page(rule_context, job_id, project_id, url)


def create_issue(job_id, project_id, url, rule_no, category, severity, issue_code, issue_message, detected_value, expected_value, data_key=None, data_path=None):
    """Create a standardized issue document"""

    return {
        "projectId": ObjectId(project_id),
        "seo_jobId": ObjectId(job_id),
        "page_url": url,
        "rule_no": rule_no,
        "category": category,
        "severity": severity,
        "issue_code": issue_code,
        "rule_id": issue_code,  # NEW: Primary identifier for SEO rules
        "issue_message": issue_message,
        "detected_value": detected_value,
        "expected_value": expected_value,
        "data_key": data_key,  # NEW: Reference to seo_page_data field
        "data_path": data_path,  # NEW: Optional sub-filter for complex data
        "created_at": datetime.utcnow()
    }


def is_job_cancelled(job_id):

    """Check if a job has been cancelled"""

    from main import cancelled_jobs, cancelled_jobs_lock

    with cancelled_jobs_lock:

        return job_id in cancelled_jobs



def send_completion_callback(job_id, stats, result_data=None):

    """Send completion callback to Node.js"""

    import requests

    import os

    try:

        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")

        node_url = f"{node_backend_url}/api/jobs/{job_id}/complete"

        callback_payload = {"stats": stats}

        

        # Include result_data if provided

        if result_data:

            callback_payload["result_data"] = result_data

        

        response = requests.post(node_url, json=callback_payload, timeout=30)

        response.raise_for_status()

        

        print(f"✅ Successfully notified Node.js of page analysis completion")

        

    except Exception as callback_error:

        print(f"⚠️ Failed to notify Node.js of completion: {callback_error}")

        # Don't raise - job execution is complete regardless of notification



def send_failure_callback(job_id, error_message):

    """Send failure callback to Node.js"""

    import requests

    import os

    try:

        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")

        node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"

        fail_payload = {"error": error_message}

        requests.post(node_fail_url, json=fail_payload, timeout=10)

    except:

        pass



def send_crawl_summary(job_id, project_id, analysis_stats, analysis_duration_ms, source_job_id=None):

    """Send crawl summary to Node.js after PAGE_ANALYSIS completion"""

    import requests

    import os

    try:

        # Get previous job results from MongoDB for complete summary

        

        def to_object_id(value):

            try:

                return ObjectId(value)

            except Exception:

                return None



        project_object_id = to_object_id(project_id)

        source_job_object_id = to_object_id(source_job_id) if source_job_id else None



        # Get PAGE_SCRAPING results (prefer chain from PAGE_ANALYSIS -> PAGE_SCRAPING)

        page_scraping_job = None

        if source_job_object_id:

            page_scraping_job = db.jobs.find_one({"_id": source_job_object_id})



        if not page_scraping_job and project_object_id:

            page_scraping_job = db.jobs.find_one({

                "project_id": project_object_id,

                "jobType": "PAGE_SCRAPING",

                "status": "completed"

            }, sort=[("created_at", -1)])



        # Get LINK_DISCOVERY results (prefer chain from PAGE_SCRAPING -> LINK_DISCOVERY)

        link_discovery_job = None

        if page_scraping_job:

            link_discovery_job_id = to_object_id(page_scraping_job.get("input_data", {}).get("source_job_id"))

            if link_discovery_job_id:

                link_discovery_job = db.jobs.find_one({"_id": link_discovery_job_id})



        if not link_discovery_job and project_object_id:

            link_discovery_job = db.jobs.find_one({

                "project_id": project_object_id,

                "jobType": "LINK_DISCOVERY",

                "status": "completed"

            }, sort=[("created_at", -1)])

        

        # Build crawl summary payload with complete data from DATABASE (primary source)

        link_discovery_result = link_discovery_job.get("result_data", {}) if link_discovery_job else {}

        page_scraping_result = page_scraping_job.get("result_data", {}) if page_scraping_job else {}



        # PRIMARY: Read from database collections (source of truth)

        internal_links = 0

        external_links = 0

        social_links = 0

        

        if project_object_id:

            internal_links = seo_internal_links.count_documents({"projectId": project_object_id})

            external_links = seo_external_links.count_documents({"projectId": project_object_id})

            social_links = seo_social_links.count_documents({"projectId": project_object_id})



        discovered_total = internal_links + external_links + social_links



        # PRIMARY: Read crawled pages from database

        scraped_successful = 0

        scraped_failed = 0

        scraped_total = 0

        

        if project_object_id:

            scraped_successful = seo_page_data.count_documents({

                "projectId": project_object_id,

                "scrape_status": "SUCCESS"

            })

            scraped_failed = seo_page_data.count_documents({

                "projectId": project_object_id,

                "scrape_status": "FAILED"

            })

            scraped_total = scraped_successful + scraped_failed



        # Calculate success rate from actual database counts

        scraped_success_rate = round((scraped_successful / scraped_total) * 100, 2) if scraped_total > 0 else 0



        link_discovery_duration_ms = link_discovery_result.get("duration_ms", 0)

        page_scraping_duration_ms = page_scraping_result.get("duration_ms", 0)

        total_crawl_duration_ms = link_discovery_duration_ms + page_scraping_duration_ms + analysis_duration_ms



        crawl_summary = {

            "projectId": project_id,

            "seo_jobId": job_id,

            "crawl_summary": {

                "discovered_links": {

                    "internal_links": internal_links,

                    "external_links": external_links,

                    "social_links": social_links,

                    "total": discovered_total

                },

                "crawled_pages": {

                    "total": scraped_total,

                    "successful": scraped_successful,

                    "failed": scraped_failed,

                    "success_rate": scraped_success_rate

                },

                "analysis_results": {

                    "pages_analyzed": seo_page_data.count_documents({

                        "projectId": project_object_id,

                        "scrape_status": "SUCCESS"

                    }),

                    "issues_found": seo_page_issues.count_documents({

                        "projectId": project_object_id

                    }),

                    "failed_analyses": analysis_stats.get("failedAnalyses", 0)

                },

                "timing": {

                    "page_analysis_duration_ms": analysis_duration_ms,

                    "total_crawl_duration_ms": total_crawl_duration_ms

                }

            }

        }

        

        # Validate and log summary data completeness

        discovered_total = crawl_summary['crawl_summary']['discovered_links']['total']

        crawled_successful = crawl_summary['crawl_summary']['crawled_pages']['successful']

        analyzed_pages = crawl_summary['crawl_summary']['analysis_results']['pages_analyzed']

        total_duration = crawl_summary['crawl_summary']['timing']['total_crawl_duration_ms']

        

        print(f"[API] Crawl summary from database | jobId={job_id} | discovered={discovered_total} | crawled={crawled_successful} | analyzed={analyzed_pages} | duration={total_duration}ms")

        

        # Send summary to Node.js

        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")

        summary_url = f"{node_backend_url}/api/jobs/{job_id}/summary"

        

        response = requests.post(summary_url, json=crawl_summary, timeout=30)

        response.raise_for_status()

        

        print(f"✅ Successfully sent crawl summary to Node.js | jobId={job_id} | total_duration={crawl_summary['crawl_summary']['timing']['total_crawl_duration_ms']}ms")

        

    except Exception as summary_error:

        print(f"⚠️ Failed to send crawl summary to Node.js | jobId={job_id} | error=\"{str(summary_error)}\"")

        # Don't raise - analysis completion is more important than summary

