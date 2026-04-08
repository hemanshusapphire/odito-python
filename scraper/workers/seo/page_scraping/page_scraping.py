"""Page scraping worker implementation."""



import os
import re
import difflib
import random
from urllib.parse import urlparse, urljoin

import threading

from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed

from bson.objectid import ObjectId



# Third-party imports

from fastapi import HTTPException

from pydantic import BaseModel

import requests
from bs4 import BeautifulSoup



# Local imports

from scraper.shared.orchestrator import scrape_page_data
from scraper.shared.url_selector import get_top_urls
# from scraper.shared.screenshots import clear_screenshot_registry, take_page_screenshot  # DISABLED

from scraper.shared.utils import normalize_url, get_registrable_domain

from db import seo_internal_links, seo_page_data
from config.config import USER_AGENTS



class PageScrapingJob(BaseModel):

    jobId: str

    projectId: str

    userId: str

    urls: list[str]  # Deterministic input from LINK_DISCOVERY

    sourceJobId: str | None = None  # Reference to LINK_DISCOVERY job



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



def is_job_cancelled(job_id: str) -> bool:

    """Check if a job has been cancelled"""

    # Import here to avoid circular imports

    from main import cancelled_jobs, cancelled_jobs_lock

    

    with cancelled_jobs_lock:

        return job_id in cancelled_jobs



# ---------------------------------------------------------------------------
# Feature 1 — Cloaking Detection (Rule 216)
# ---------------------------------------------------------------------------
def _extract_visible_text(html: str) -> str:
    """Extract and normalize visible text from HTML for cloaking comparison."""
    try:
        soup = BeautifulSoup(html, "lxml")
        # Remove non-visible elements
        for tag in soup.find_all(["script", "style", "noscript", "head"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text
    except Exception:
        return ""


def detect_cloaking(raw_html: str, rendered_html: str) -> dict:
    """
    Compare raw server HTML vs JS-rendered HTML to detect cloaking.
    Pure comparison function — caller provides both HTML versions.
    Flags cloaking if text similarity < 70%.
    """
    try:
        raw_text = _extract_visible_text(raw_html)
        rendered_text = _extract_visible_text(rendered_html)

        # Edge case: both empty or very short — skip comparison
        if len(raw_text) < 50 and len(rendered_text) < 50:
            return {
                "cloaking_checked": True,
                "cloaking_similarity_score": 1.0,
                "cloaking_flagged": False,
                "note": "insufficient_text_for_comparison"
            }

        similarity = difflib.SequenceMatcher(None, raw_text, rendered_text).ratio()

        return {
            "cloaking_checked": True,
            "cloaking_similarity_score": round(similarity, 4),
            "cloaking_flagged": similarity < 0.70
        }
    except Exception as e:
        return {
            "cloaking_checked": False,
            "error": str(e)
        }


def _fetch_raw_html_only(url: str, timeout: int = 8) -> str:
    """
    Lightweight raw HTTP GET — no Selenium, no JS detection.
    Used to capture raw server HTML for cloaking comparison.
    """
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html"
        }
        res = requests.get(url, headers=headers, timeout=timeout)
        res.raise_for_status()
        return res.text
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Feature 2 — Media Detection (Rules 237, 238)
# ---------------------------------------------------------------------------
def detect_media_elements(html: str) -> dict:
    """
    Parse DOM to detect video/audio elements and accessibility compliance.
    Returns structured media analysis object.
    """
    try:
        soup = BeautifulSoup(html, "lxml")

        has_video = bool(soup.find("video"))
        has_audio = bool(soup.find("audio"))
        has_captions = bool(soup.find("track", attrs={"kind": "captions"}))

        # Check for transcript links (anchor tags containing "transcript" in text or href)
        has_transcript = False
        for a_tag in soup.find_all("a", href=True):
            link_text = (a_tag.get_text() or "").lower()
            link_href = (a_tag.get("href") or "").lower()
            if "transcript" in link_text or "transcript" in link_href:
                has_transcript = True
                break

        return {
            "has_video": has_video,
            "has_audio": has_audio,
            "has_captions": has_captions,
            "has_transcript": has_transcript
        }
    except Exception as e:
        return {
            "has_video": False,
            "has_audio": False,
            "has_captions": False,
            "has_transcript": False,
            "error": str(e)
        }


# ---------------------------------------------------------------------------
# Internal Link Extraction for CRAWL_GRAPH
# ---------------------------------------------------------------------------
_EXCLUDED_SCHEMES = frozenset(["mailto:", "tel:", "javascript:"])


def extract_internal_links(soup, page_url: str) -> list:
    """
    Extract normalized same-domain internal links from a BeautifulSoup object.
    Lightweight — no HTTP calls, operates on already-parsed HTML.

    Returns a deduplicated list of normalized internal URLs.
    Always returns a list (empty if no links found).
    """
    try:
        parsed_page = urlparse(page_url)
        page_domain = get_registrable_domain(page_url)
        if not page_domain:
            return []

        seen = set()
        results = []

        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()

            # Skip empty, fragment-only, and excluded schemes
            if not href or href.startswith("#"):
                continue
            if any(href.lower().startswith(s) for s in _EXCLUDED_SCHEMES):
                continue

            # Resolve relative URLs
            try:
                absolute_url = urljoin(page_url, href)
            except Exception:
                continue

            # Remove fragment
            parsed = urlparse(absolute_url)
            if not parsed.netloc:
                continue

            # Same-domain check
            link_domain = get_registrable_domain(absolute_url)
            if link_domain != page_domain:
                continue

            # Normalize and deduplicate
            normalized = normalize_url(absolute_url)
            if normalized and normalized not in seen:
                seen.add(normalized)
                results.append(normalized)

        return results
    except Exception:
        return []


def execute_page_scraping_logic(job: PageScrapingJob):

    # Track start time for duration calculation

    start_time = datetime.utcnow()

    duration_ms = 0  # Initialize before try block

    

    try:

        # Use SAME deterministic type-based URL selection as Headless Worker
        urls_to_scrape = get_top_urls(job.projectId, limit=25)
        
        total_pages = len(urls_to_scrape)
        
        print(f"[DEBUG] Initial selected URLs: {len(urls_to_scrape)} URLs")
        print(f"[DEBUG] URLs: {urls_to_scrape[:5]}...")  # Show first 5 for debugging
        print(f"[WORKER] PAGE_SCRAPING started | jobId={job.jobId} | selectedUrls={total_pages} | sameLogicAsHeadless=true")

        

        # Clear screenshot registry at the start of each job - DISABLED
        # clear_screenshot_registry()

        

        # Check for cancellation early

        if is_job_cancelled(job.jobId):

            print(f"[WORKER] PAGE_SCRAPING cancelled | jobId={job.jobId}")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Initialize counters for failure tolerance

        successful_pages = 0

        failed_pages = 0

        completed_pages = 0

        

        def scrape_single_url(url):

            """Scrape a single URL - optimized for concurrent processing"""

            nonlocal successful_pages, failed_pages, completed_pages

            

            # Check cancellation before processing each URL

            if is_job_cancelled(job.jobId):

                return None

            

            try:

                # Scrape page data using comprehensive extraction

                page_data = scrape_page_data(url)

                

                # Take screenshot (best-effort, failures don't affect scraping) - DISABLED
                screenshot_path = None
                # screenshot_path = take_page_screenshot(url, job.jobId, job.projectId)

                

                if page_data and page_data.get("extraction_status") == "SUCCESS":

                    # Add job metadata and screenshot path, including HTTP metrics

                    page_data.update({

                        "seo_jobId": ObjectId(job.jobId),

                        "projectId": ObjectId(job.projectId),

                        "sourceJobId": ObjectId(job.sourceJobId) if job.sourceJobId else None,

                        "scrapedAt": datetime.utcnow(),

                        "scrape_status": "SUCCESS",

                        "screenshot_path": screenshot_path,

                        "http_status_code": page_data.get("http_status_code"),

                        "response_time_ms": page_data.get("response_time_ms")

                    })

                    successful_pages += 1

                    completed_pages += 1

                    

                    # Send progress update after each successful page

                    percentage = int((completed_pages / total_pages) * 100)

                    send_progress_update(

                        job.jobId, 

                        percentage, 

                        "Scraping", 

                        "Scraping website pages", 

                        f"{completed_pages} of {total_pages} pages scraped"

                    )

                    # --- Internal Link Extraction for CRAWL_GRAPH ---
                    try:
                        raw_html_for_links = page_data.get("raw_html", "")
                        if raw_html_for_links:
                            link_soup = BeautifulSoup(raw_html_for_links, "lxml")
                            page_data["internal_links"] = extract_internal_links(link_soup, url)
                        else:
                            page_data["internal_links"] = []
                    except Exception as link_err:
                        print(f"[WARNING] Internal link extraction failed for {url}: {link_err}")
                        page_data["internal_links"] = []

                    # --- Feature 1: Cloaking Detection ---
                    try:
                        # page_data["raw_html"] is the "best" HTML from fetch_html()
                        # (possibly Selenium-rendered). Get raw server HTML separately.
                        raw_server_html = _fetch_raw_html_only(url)
                        rendered_html = page_data.get("raw_html", "")
                        if raw_server_html and rendered_html:
                            page_data["cloaking_analysis"] = detect_cloaking(raw_server_html, rendered_html)
                        else:
                            page_data["cloaking_analysis"] = {"cloaking_checked": False, "note": "html_unavailable"}
                    except Exception as cloak_err:
                        print(f"[WARNING] Cloaking detection failed for {url}: {cloak_err}")
                        page_data["cloaking_analysis"] = {"cloaking_checked": False, "error": str(cloak_err)}

                    # --- Feature 2: Media Detection ---
                    try:
                        html_for_media = page_data.get("raw_html", "")
                        if html_for_media:
                            page_data["media_analysis"] = detect_media_elements(html_for_media)
                    except Exception as media_err:
                        print(f"[WARNING] Media detection failed for {url}: {media_err}")

                    return page_data

                else:

                    failed_pages += 1

                    completed_pages += 1

                    

                    # Send progress update after each failed page

                    percentage = int((completed_pages / total_pages) * 100)

                    send_progress_update(

                        job.jobId, 

                        percentage, 

                        "Scraping", 

                        "Scraping website pages", 

                        f"{completed_pages} of {total_pages} pages scraped"

                    )

                    

                    return {

                        "url": url,

                        "seo_jobId": ObjectId(job.jobId),

                        "projectId": ObjectId(job.projectId),

                        "sourceJobId": ObjectId(job.sourceJobId) if job.sourceJobId else None,

                        "scrapedAt": datetime.utcnow(),

                        "scrape_status": "FAILED",

                        "error": page_data.get("error", "Extraction failed"),

                        "screenshot_path": screenshot_path,

                        "http_status_code": page_data.get("http_status_code"),

                        "response_time_ms": page_data.get("response_time_ms"),

                        "internal_links": []

                    }

                    

            except Exception as e:

                failed_pages += 1

                completed_pages += 1

                

                # Send progress update after each exception

                percentage = int((completed_pages / total_pages) * 100)

                send_progress_update(

                    job.jobId, 

                    percentage, 

                    "Scraping", 

                    "Scraping website pages", 

                    f"{completed_pages} of {total_pages} pages scraped"

                )

                

                return {

                    "url": url,

                    "seo_jobId": ObjectId(job.jobId),

                    "projectId": ObjectId(job.projectId),

                    "sourceJobId": ObjectId(job.sourceJobId) if job.sourceJobId else None,

                    "scrapedAt": datetime.utcnow(),

                    "scrape_status": "FAILED",

                    "error": str(e),

                    "screenshot_path": None,

                    "internal_links": []

                }       

        

        # Process URLs with ThreadPoolExecutor (max 6 workers as required)

        all_results = []
        
        # DEBUG: Verify URLs before scraping (no modifications should happen)
        print(f"[DEBUG] Before scraping: {len(urls_to_scrape)} URLs ready for processing")
        print(f"[DEBUG] Final URLs to scrape: {urls_to_scrape[:5]}...")  # Show first 5
        
        with ThreadPoolExecutor(max_workers=6) as executor:

            # Submit only first 25 scraping tasks
            futures = [executor.submit(scrape_single_url, url) for url in urls_to_scrape]

            

            # Collect results as they complete

            for future in as_completed(futures):

                # Check cancellation during result collection

                if is_job_cancelled(job.jobId):

                    print(f"🛑 Job {job.jobId} cancelled during scraping")

                    # Cancel remaining futures

                    for f in futures:

                        f.cancel()

                    return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

                

                result = future.result()

                if result:

                    all_results.append(result)

        

        # Final cancellation check before storing results

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} cancelled before completion")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Store all results in bulk (successful and failed)

        if all_results:

            seo_page_data.insert_many(all_results, ordered=False)

        

        # Update seo_internal_links with crawl status and HTTP metrics after PAGE_SCRAPING completion

        try:

            # Get all URLs for this job (job.urls contains strings, not dicts)

            job_urls = job.urls

            

            # Update each internal link with crawl status and HTTP metrics

            for result in all_results:

                # Defensive guard: ensure result is a dictionary and has required fields

                if not result or not isinstance(result, dict):

                    continue

                    

                url = result.get("url")

                if not url or not isinstance(url, str):

                    continue

                    

                update_result = seo_internal_links.update_one(

                    {"url": url, "seo_jobId": ObjectId(job.sourceJobId)},

                    {

                        "$set": {

                            "crawledAt": datetime.utcnow()

                        }

                    }

                )

                if update_result.matched_count == 0:

                    print(f"[WARNING] No matching internal link found for update | url={url} | seo_jobId={job.sourceJobId}")

                elif update_result.modified_count == 0:

                    print(f"[WARNING] Internal link found but not modified | url={url} | seo_jobId={job.sourceJobId}")

            print(f"[WORKER] Updated internal links with crawl timestamp | jobId={job.jobId} | updated={len([r for r in all_results if r and isinstance(r, dict) and r.get('url')])}")

        except Exception as update_error:

            print(f"[ERROR] Failed to update internal links with crawl timestamp | jobId={job.jobId} | reason=\"{str(update_error)}\"")

        

        # Calculate duration before sending callbacks

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)



        # Prepare completion stats

        stats = {

            "totalUrls": total_pages,

            "successfulPages": successful_pages,

            "failedPages": failed_pages,

            "successRate": round((successful_pages / total_pages) * 100, 2) if total_pages > 0 else 0

        }

        

        # Store stats in result_data for summary aggregation

        result_data = {

            "totalUrls": total_pages,

            "successfulPages": successful_pages,

            "failedPages": failed_pages,

            "successRate": round((successful_pages / total_pages) * 100, 2) if total_pages > 0 else 0,

            "duration_ms": duration_ms

        }

        

        print(f"[WORKER] PAGE_SCRAPING completed | jobId={job.jobId} | success={successful_pages} | failed={failed_pages}")

        

        # Send completion callback to Node.js (fire-and-forget)

        try:
            import requests
            # Validate required environment variables
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            node_url = f"{node_backend_url}/api/jobs/{job.jobId}/complete"

            callback_payload = {"stats": stats, "result_data": result_data}

            
            # Fire-and-forget completion notification
            response = requests.post(node_url, json=callback_payload, timeout=30)
            response.raise_for_status()

            print(f" Successfully notified Node.js of page scraping completion")

        except requests.exceptions.Timeout:
            # Fire-and-forget: timeout doesn't mean job failed
            pass

        except Exception as callback_error:
            # Log but don't fail the job - completion is best-effort
            print(f" Failed to notify Node.js of completion (job still succeeded): {callback_error}")

        # Always return success - job execution is complete regardless of notification

        return {

            "status": "success",

            "jobId": job.jobId,

            "stats": stats,

            "duration_ms": duration_ms,

            "message": "Page scraping completed and results stored"

        }

        

    except Exception as e:

        print(f" Job {job.jobId} failed: {str(e)}")

        

        # Safely compute duration even in exception case

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        

        # Send failure callback to Node.js

        try:
            import requests
            # Validate required environment variables
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            node_fail_url = f"{node_backend_url}/api/jobs/{job.jobId}/fail"

            fail_payload = {"error": str(e)}

            response = requests.post(node_fail_url, json=fail_payload, timeout=10)
            response.raise_for_status()

        except requests.exceptions.Timeout:
            # Fire-and-forget: timeout doesn't mean job failed
            pass
        except:
            pass

            

        raise HTTPException(status_code=500, detail=str(e))

