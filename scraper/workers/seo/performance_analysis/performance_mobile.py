"""Performance Mobile Analysis Worker."""

import os
import asyncio
import requests
from datetime import datetime
from bson.objectid import ObjectId
from bs4 import BeautifulSoup
from pymongo import InsertOne
from db import seo_page_performance, seo_page_data


# ---------------------------------------------------------------------------
# Feature 3 — Render Blocking Detection (Rules 175, 180, 181)
# ---------------------------------------------------------------------------
def detect_render_blocking_resources(raw_html: str, lighthouse_data: dict = None) -> dict:
    """
    Detect render-blocking CSS and JS resources.
    Prefers Lighthouse audit data if available; falls back to raw HTML parsing.

    Args:
        raw_html: Raw HTML string of the page.
        lighthouse_data: Optional Lighthouse/PSI audit JSON with renderBlockingResources.

    Returns:
        Structured render-blocking analysis dict.
    """
    try:
        # --- Prefer Lighthouse data if available ---
        if lighthouse_data:
            rb_audit = (
                lighthouse_data
                .get("lighthouseResult", {})
                .get("audits", {})
                .get("render-blocking-resources", {})
            )
            if rb_audit and rb_audit.get("details", {}).get("items"):
                items = rb_audit["details"]["items"]
                blocking_css = [i for i in items if i.get("url", "").endswith(".css")]
                blocking_js = [i for i in items if i.get("url", "").endswith(".js")]
                total = len(items)
                return {
                    "source": "lighthouse",
                    "blocking_css_count": len(blocking_css),
                    "blocking_js_count": len(blocking_js),
                    "total_blocking_count": total,
                    "render_blocking_flag": total > 3
                }

        # --- Fallback: raw HTML parsing ---
        if not raw_html:
            return {
                "source": "none",
                "blocking_css_count": 0,
                "blocking_js_count": 0,
                "total_blocking_count": 0,
                "render_blocking_flag": False,
                "note": "no_html_available"
            }

        soup = BeautifulSoup(raw_html, "lxml")
        head = soup.find("head")
        if not head:
            return {
                "source": "html_parse",
                "blocking_css_count": 0,
                "blocking_js_count": 0,
                "total_blocking_count": 0,
                "render_blocking_flag": False,
                "note": "no_head_element"
            }

        # Count blocking JS: <script> in <head> without async or defer
        blocking_js = 0
        for script in head.find_all("script", src=True):
            has_async = script.has_attr("async")
            has_defer = script.has_attr("defer")
            if not has_async and not has_defer:
                blocking_js += 1

        # Count blocking CSS: <link rel="stylesheet"> in <head>
        # Exclude preload and print-only stylesheets
        blocking_css = 0
        for link in head.find_all("link", rel=True):
            rel_values = [r.lower() for r in (link.get("rel") or [])]
            media = (link.get("media") or "").lower()
            if "stylesheet" in rel_values and "preload" not in rel_values:
                # print-only stylesheets are not render-blocking
                if media != "print":
                    blocking_css += 1

        total = blocking_css + blocking_js
        return {
            "source": "html_parse",
            "blocking_css_count": blocking_css,
            "blocking_js_count": blocking_js,
            "total_blocking_count": total,
            "render_blocking_flag": total > 3
        }
    except Exception as e:
        return {
            "source": "error",
            "blocking_css_count": 0,
            "blocking_js_count": 0,
            "total_blocking_count": 0,
            "render_blocking_flag": False,
            "error": str(e)
        }


def send_progress_update(job_id: str, percentage: int, step: str, message: str, subtext: str = None):
    """Send progress update to Node.js backend (sync version for backward compatibility)"""
    try:
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
        
        print(f"📊 Progress update sent: {percentage}% - {step}")
        
    except Exception as e:
        print(f"⚠️ Failed to send progress update: {e}")


async def send_progress_update_async(job_id: str, percentage: int, step: str, message: str, subtext: str = None):
    """Send progress update to Node.js backend (async version)"""
    try:
        from scraper.shared.http_client import async_post
        
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
        
        await async_post(progress_url, json=payload, timeout=5)
        print(f"📊 Progress update sent: {percentage}% - {step}")
        
    except Exception as e:
        print(f"⚠️ Failed to send progress update: {e}")


async def execute_performance_mobile_logic(job):
    """Execute PERFORMANCE_MOBILE job logic (async version)"""
    print(f"[DEBUG] execute_performance_mobile_logic START | jobId={job.jobId}")
    return await execute_performance_mobile_logic_async(job)


async def execute_performance_mobile_logic_async(job):
    """Execute PERFORMANCE_MOBILE job logic (async version with concurrent page processing)"""
    job_id = job.jobId
    project_id = job.projectId
    source_job_id = job.sourceJobId
    
    print(f"[DEBUG] execute_performance_mobile_logic_async START | jobId={job_id}")
    print(f"[WORKER] PERFORMANCE_MOBILE started | jobId={job_id} | sourceJobId={source_job_id}")
    
    try:
        # 1. Send initial progress
        await send_progress_update_async(job_id, 10, "PERFORMANCE_MOBILE", "Starting mobile performance analysis")
        
        # Get pages from seo_page_data collection by projectId only
        if isinstance(project_id, str):
            try:
                project_id_obj = ObjectId(project_id)
            except Exception as e:
                print(f"[ERROR] Failed to convert projectId to ObjectId: {e}")
                project_id_obj = project_id
        else:
            project_id_obj = project_id
        
        query_filter = {"projectId": project_id_obj}
        pages = list(seo_page_data.find(query_filter))
        
        print(f"[PERFORMANCE] Pages fetched: {len(pages)}")
        
        if not pages:
            print(f"[WARNING] No pages found for performance analysis | jobId={job_id} | projectId={project_id}")
            await send_progress_update_async(job_id, 100, "PERFORMANCE_MOBILE", "No pages found", "0 pages processed")
            return {
                "status": "completed",
                "jobId": job_id,
                "message": "No pages found for performance analysis",
                "pages_processed": 0,
                "performance_records_created": 0
            }
        
        print(f"[WORKER] Found {len(pages)} pages for performance analysis | jobId={job_id} | projectId={project_id}")
        await send_progress_update_async(job_id, 20, "PERFORMANCE_MOBILE", f"Found {len(pages)} pages to analyze", f"Processing {len(pages)} pages")
        
        # 3. Process pages concurrently with semaphore (max 3 concurrent for safety)
        semaphore = asyncio.Semaphore(3)
        
        async def process_page_limited(page, index):
            async with semaphore:
                return await process_single_page_async(page, index, job_id, project_id, pages)
        
        tasks = [process_page_limited(page, i) for i, page in enumerate(pages)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and collect documents for bulk insert
        analyzed_pages = []
        failed_pages = []
        bulk_operations = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                page_url = pages[i].get("url", "")
                print(f"[ERROR] Page processing failed | jobId={job_id} | page={page_url} | error=\"{str(result)}\"")
                failed_pages.append({"url": page_url, "error": str(result)})
            else:
                if result.get("success") and result.get("data"):
                    analyzed_pages.append(result["url"])
                    bulk_operations.append(InsertOne(result["data"]))
                else:
                    failed_pages.append({"url": result["url"], "error": result.get("error", "Unknown error")})
        
        # Bulk insert all performance data in one operation
        if bulk_operations:
            try:
                seo_page_performance.bulk_write(bulk_operations, ordered=False)
                print(f"[DB] Bulk inserted {len(bulk_operations)} performance records")
            except Exception as bulk_error:
                print(f"[ERROR] Bulk insert failed | jobId={job_id} | error=\"{str(bulk_error)}\"")
                # Fallback to individual inserts if bulk fails
                for i, result in enumerate(results):
                    if not isinstance(result, Exception) and result.get("success") and result.get("data"):
                        try:
                            seo_page_performance.insert_one(result["data"])
                        except Exception as individual_error:
                            print(f"[ERROR] Individual insert failed | url={result.get('url')} | error=\"{str(individual_error)}\"")
                    failed_pages.append({"url": result["url"], "error": result.get("error", "Unknown error")})
        
        # 4. Final stats and completion
        stats = {
            "totalPages": len(pages),
            "analyzedPages": len(analyzed_pages),
            "failedPages": len(failed_pages),
            "deviceType": "mobile"
        }
        
        result_data = {
            "device_type": "mobile",
            "pages_processed": analyzed_pages,
            "pages_failed": failed_pages,
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
        
        print(f"[WORKER] PERFORMANCE_MOBILE completed | jobId={job_id} | analyzed={len(analyzed_pages)} | failed={len(failed_pages)}")
        
        # 5. Send completion callback to Node.js
        await send_completion_callback_async(job_id, stats, result_data)
        
        return {
            "status": "completed",
            "jobId": job_id,
            "stats": stats,
            "result_data": result_data
        }
        
    except Exception as e:
        print(f"[ERROR] PERFORMANCE_MOBILE failed | jobId={job_id} | error=\"{str(e)}\"")
        
        # Send failure callback to Node.js
        await send_failure_callback_async(job_id, str(e))
        
        return {
            "status": "failed",
            "jobId": job_id,
            "error": str(e)
        }


async def process_single_page_async(page, index, job_id, project_id, pages):
    """Process a single page for performance analysis (async) - returns document for bulk insert"""
    page_url = page.get("url", "")
    
    if not page_url:
        print(f"[WARNING] Skipping page with empty URL | jobId={job_id} | page_index={index}")
        return {"success": False, "url": "", "error": "Empty page URL", "data": None}
    
    try:
        # Process page for performance analysis
        performance_data = {
            "projectId": ObjectId(project_id),
            "seo_jobId": ObjectId(job_id),
            "page_url": page_url,
            "device_type": "mobile",
            "analyzed_at": datetime.utcnow(),
            # Placeholder metrics - will be replaced with real PageSpeed data
            "performance_score": 85,
            "first_contentful_paint": 1.2,
            "largest_contentful_paint": 2.1,
            "cumulative_layout_shift": 0.1,
            "total_blocking_time": 150,
            "speed_index": 1.8,
            "time_to_interactive": 2.5,
            "status": "completed",
            "error": None
        }
        
        # --- Feature 3: Render Blocking Detection ---
        raw_html = page.get("raw_html", "")
        try:
            performance_data["render_blocking_analysis"] = detect_render_blocking_resources(raw_html)
        except Exception as rb_err:
            print(f"[WARNING] Render blocking detection failed | url={page_url} | error={rb_err}")
        
        # Send progress update only every 5 pages to reduce overhead
        if index % 5 == 0:
            progress = 25 + int((index + 1) / len(pages) * 60)
            await send_progress_update_async(job_id, progress, "PERFORMANCE_MOBILE", f"Analyzed {index + 1} pages")
        
        return {"success": True, "url": page_url, "data": performance_data}
        
    except Exception as page_error:
        print(f"[ERROR] Failed to analyze page | jobId={job_id} | page={page_url} | error=\"{str(page_error)}\"")
        return {"success": False, "url": page_url, "error": str(page_error), "data": None}


def send_completion_callback(job_id: str, stats: dict, result_data: dict):
    """Send completion callback to Node.js backend (sync version for backward compatibility)"""
    try:
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
        callback_payload = {"stats": stats, "result_data": result_data}
        
        response = requests.post(node_url, json=callback_payload, timeout=10)
        response.raise_for_status()
        
        print(f"[API] PERFORMANCE_MOBILE completion sent | jobId={job_id}")
        
    except Exception as callback_error:
        print(f"[ERROR] Failed to send PERFORMANCE_MOBILE completion | jobId={job_id} | error=\"{str(callback_error)}\"")
        # Try to mark job as failed instead
        try:
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            fail_payload = {"error": str(callback_error), "stats": stats}
            requests.post(node_fail_url, json=fail_payload, timeout=10)
        except:
            print(f"[CRITICAL] Failed to mark job as failed | jobId={job_id}")


async def send_completion_callback_async(job_id: str, stats: dict, result_data: dict):
    """Send completion callback to Node.js backend (async version)"""
    try:
        from scraper.shared.http_client import async_post
        
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
        callback_payload = {"stats": stats, "result_data": result_data}
        
        await async_post(node_url, json=callback_payload, timeout=10)
        print(f"[API] PERFORMANCE_MOBILE completion sent | jobId={job_id}")
        
    except Exception as callback_error:
        print(f"[ERROR] Failed to send PERFORMANCE_MOBILE completion | jobId={job_id} | error=\"{str(callback_error)}\"")
        # Try to mark job as failed instead
        try:
            node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            fail_payload = {"error": str(callback_error), "stats": stats}
            await async_post(node_fail_url, json=fail_payload, timeout=10)
        except:
            print(f"[CRITICAL] Failed to mark job as failed | jobId={job_id}")


def send_failure_callback(job_id: str, error: str):
    """Send failure callback to Node.js backend (sync version for backward compatibility)"""
    try:
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
        fail_payload = {"error": error}
        
        response = requests.post(node_fail_url, json=fail_payload, timeout=10)
        response.raise_for_status()
        
        print(f"[API] PERFORMANCE_MOBILE failure sent | jobId={job_id}")
        
    except Exception as callback_error:
        print(f"[CRITICAL] Failed to send PERFORMANCE_MOBILE failure | jobId={job_id} | error=\"{str(callback_error)}\"")


async def send_failure_callback_async(job_id: str, error: str):
    """Send failure callback to Node.js backend (async version)"""
    try:
        from scraper.shared.http_client import async_post
        
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
        fail_payload = {"error": error}
        
        await async_post(node_fail_url, json=fail_payload, timeout=10)
        print(f"[API] PERFORMANCE_MOBILE failure sent | jobId={job_id}")
        
    except Exception as callback_error:
        print(f"[CRITICAL] Failed to send PERFORMANCE_MOBILE failure | jobId={job_id} | error=\"{str(callback_error)}\"")
