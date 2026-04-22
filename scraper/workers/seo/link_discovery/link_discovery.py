"""Link discovery worker implementation."""



import os

import sys

import json

import time

import threading

import logging

import random

import string

import warnings

import hashlib

from datetime import datetime

from urllib.parse import urljoin



# Third-party imports

import requests

from fastapi import HTTPException

from pydantic import BaseModel, HttpUrl

from concurrent.futures import ThreadPoolExecutor, as_completed

from bson.objectid import ObjectId



# Local imports

from scraper.shared.links import extract_links_from_sitemap, extract_sitemaps_from_robots, extract_all_links_from_html

from scraper.shared.utils import normalize_url, get_registrable_domain

from scraper.shared.fetcher import fetch_html

# from scraper.shared.screenshots import capture_homepage_screenshot  # DISABLED

from scraper.shared.recursive_sitemap import discover_all_sitemap_urls


from db import seo_internal_links, seo_external_links, seo_social_links, seo_page_data, seo_page_issues, db



class LinkDiscoveryJob(BaseModel):

    jobId: str

    projectId: str

    userId: str

    main_url: HttpUrl



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

        

        print(f"📊 Progress update sent: {percentage}% - {step}")

        

    except Exception as e:

        print(f"⚠️ Failed to send progress update: {e}")

        # Don't raise exception - progress updates are non-critical



def is_job_cancelled(job_id: str) -> bool:

    """Check if a job has been cancelled"""

    # Import here to avoid circular imports

    from main import cancelled_jobs, cancelled_jobs_lock

    

    with cancelled_jobs_lock:

        return job_id in cancelled_jobs



def execute_link_discovery(job: LinkDiscoveryJob):

    # Track start time for duration calculation

    start_time = datetime.utcnow()

    duration_ms = 0  # Initialize before try block

    

    try:

        url = str(job.main_url)

        base_domain = get_registrable_domain(url)

        

        print(f"🔄 Starting LINK_DISCOVERY job {job.jobId} for URL: {url}")

        

        # Send initial progress update

        send_progress_update(job.jobId, 5, "Start", "Your website crawling has been started", "Initializing audit process")

        

        # Check for cancellation early

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} was cancelled before processing")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # 1. Capture homepage screenshot (failure-safe, does not block link discovery)

        try:

            print(f"[WORKER] Capturing homepage screenshot | jobId={job.jobId} | url={url} - DISABLED")
            screenshot_result = {"status": "disabled", "error": "Screenshot functionality disabled"}
            # screenshot_result = capture_homepage_screenshot(url, job.jobId, job.projectId)

            if screenshot_result["status"] == "captured":

                print(f"[WORKER] Homepage screenshot captured | jobId={job.jobId} | path={screenshot_result.get('screenshot_path')}")

            elif screenshot_result["status"] == "failed":

                print(f"[WORKER] Homepage screenshot failed | jobId={job.jobId} | error={screenshot_result.get('error')}")

            else:

                print(f"[WORKER] Homepage screenshot skipped | jobId={job.jobId} | reason={screenshot_result.get('error')}")

        except Exception as screenshot_error:

            print(f"[WORKER] Homepage screenshot error | jobId={job.jobId} | error=\"{str(screenshot_error)}\"")

            # Continue with link discovery regardless of screenshot failure

        

        # Global deduplication sets per job

        seen_internal = set()

        seen_external = set()

        seen_social = set()

        

        # Initialize all_internal_links accumulator to prevent NameError

        all_internal_links: list[dict] = []

        

        # Per-job HTML cache to avoid re-fetching

        html_cache = {}
        
        # Discover all internal URLs from sitemaps and crawling
        all_internal_urls = set()
        sitemap_discovery_stats = {}  # Store sitemap discovery statistics
        
        # Send progress for main URL crawling
        send_progress_update(job.jobId, 20, "Find", "Looking for all links", "Crawling main page and extracting internal links")

        

        # Check for cancellation

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} cancelled before main URL crawl")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # 2. Extract links from main URL first with final redirect handling
        try:
            main_html, main_status, main_response_time, final_url = fetch_html(url, timeout=10, allow_redirects=True)
            
            # Use final redirected URL for normalization
            if final_url:
                normalized_main_url = normalize_url(final_url)
                print(f"[WORKER] Main URL redirected | original={url} | final={final_url} | normalized={normalized_main_url}")
                url = normalized_main_url  # Update working URL to normalized final URL
            else:
                normalized_main_url = normalize_url(url)
                url = normalized_main_url

            if main_status == 200 and main_html:
                main_internal_links, main_external_links, main_social_links = extract_all_links_from_html(main_html, url, base_domain)
                
                # Add main page internal links to both collections
                for link in main_internal_links:
                    normalized_link = normalize_url(link) if isinstance(link, str) else normalize_url(link.get("url", ""))

                    if normalized_link:
                        all_internal_urls.add(normalized_link)
                        all_internal_links.append(normalized_link)  # Add to accumulator for iteration
                        
                print(f"📊 Main page extracted: {len(main_internal_links)} internal, 0 external (disabled), {len(main_social_links)} social links")
        except Exception as main_error:
            print(f"⚠️ Failed to extract from main URL: {main_error}")

        

        # 3. Get sitemap URLs using universal recursive discovery with strict filtering
        print(f"[WORKER] Starting universal recursive sitemap discovery | jobId={job.jobId} | url={url}")
        
        try:
            # Use new recursive sitemap discovery with built-in strict filtering
            discovered_sitemap_urls, url_metadata, sitemap_discovery_stats = discover_all_sitemap_urls(
                url,  # Use normalized main URL
                max_depth=5,
                max_sitemaps=50
            )
            
            print(f"[WORKER] Recursive sitemap discovery completed | jobId={job.jobId}")
            print(f"[WORKER] Sitemaps processed: {sitemap_discovery_stats['sitemaps_processed']}")
            print(f"[WORKER] Total URLs discovered: {len(discovered_sitemap_urls)}")
            
            # Add discovered URLs to our collections (already filtered by recursive discovery)
            for sitemap_url in discovered_sitemap_urls:
                normalized_sitemap_url = normalize_url(sitemap_url)  # Normalize sitemap URLs too
                all_internal_urls.add(normalized_sitemap_url)
                all_internal_links.append(normalized_sitemap_url)
                
        except Exception as sitemap_error:
            print(f"[WORKER] Recursive sitemap discovery failed | jobId={job.jobId} | error=\"{str(sitemap_error)}\"")
            # Fallback to basic extraction if recursive discovery fails
            try:
                sitemap_urls = extract_links_from_sitemap(url)
                url_metadata = {}  # Empty metadata for fallback
                for sitemap_url in sitemap_urls:
                    normalized = normalize_url(sitemap_url)
                    all_internal_urls.add(normalized)
                    all_internal_links.append(normalized)
                sitemap_discovery_stats = {
                    'sitemaps_processed': 1,
                    'sitemap_indexes_found': 0,
                    'urlsets_found': 1,
                    'failed_sitemaps': 0,
                    'recursion_depth_used': 0,
                    'total_urls': len(sitemap_urls)
                }
            except Exception as fallback_error:
                print(f"[WORKER] Fallback sitemap extraction also failed | jobId={job.jobId} | error=\"{str(fallback_error)}\"")
                url_metadata = {}  # Empty metadata for fallback
                sitemap_discovery_stats = {
                    'sitemaps_processed': 0,
                    'sitemap_indexes_found': 0,
                    'urlsets_found': 0,
                    'failed_sitemaps': 1,
                    'recursion_depth_used': 0,
                    'total_urls': 0
                }

        # Check cancellation during sitemap processing
        if is_job_cancelled(job.jobId):
            print(f"🛑 Job {job.jobId} cancelled during sitemap processing")
            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Send progress update after sitemap discovery
        send_progress_update(job.jobId, 25, "Find", "Looking for all links", f"Sitemap discovery completed, found {len(all_internal_urls)} internal URLs")

        # Check for cancellation
        if is_job_cancelled(job.jobId):
            print(f"🛑 Job {job.jobId} cancelled before analysis phase")
            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        # 3. Cap crawl depth for Phase-1 (max 100 internal pages)

        if len(all_internal_urls) > 100:

            all_internal_urls = set(list(all_internal_urls)[:100])

        

        # Send progress for analysis phase

        current_total_links = len(all_internal_links)

        send_progress_update(job.jobId, 30, "Analyze", "Analyzing all links", f"Found {current_total_links} internal links, scanning for more...")

        

        # Check for cancellation

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} cancelled before analysis phase")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # 4. Store all discovered internal links using bulk insert with deduplication
        internal_docs = []
        
        for link in all_internal_links:
            # all_internal_links contains string URLs, not dict objects
            link_url = link if isinstance(link, str) else link.get("url", "")
            
            # Normalize every URL before deduplication check
            if link_url:
                normalized_link_url = normalize_url(link_url)
                
                if normalized_link_url and normalized_link_url not in seen_internal:
                    seen_internal.add(normalized_link_url)
                    
                    # Get metadata for this URL if available
                    metadata = url_metadata.get(normalized_link_url, {})
                    
                    # Build document with optional type and sourceSitemap fields
                    doc = {
                        "url": normalized_link_url,
                        "sourceUrl": url,  # Use normalized main URL as source
                        "seo_jobId": ObjectId(job.jobId),
                        "projectId": ObjectId(job.projectId),
                        "discoveredAt": datetime.utcnow()
                    }
                    
                    # Add type and sourceSitemap if available (backward compatibility)
                    if "type" in metadata:
                        doc["type"] = metadata["type"]
                    if "sourceSitemap" in metadata:
                        doc["sourceSitemap"] = metadata["sourceSitemap"]
                    
                    internal_docs.append(doc)
                    
                    if is_job_cancelled(job.jobId):
                        print(f"🛑 Job {job.jobId} cancelled during internal link processing")
                        return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Bulk insert internal links

        if internal_docs:

            seo_internal_links.insert_many(internal_docs, ordered=False)

            

        internal_count = len(internal_docs)

        

        # 5. Extract external and social links from ALL internal pages in parallel

        

        # Progress tracking for parallel processing

        total_pages = len(all_internal_urls)

        processed_pages = 0

        

        # Track total links discovered so far for progress reporting

        total_links_found = len(seen_internal)  # Start with internal links already found

        

        def fetch_and_extract_links(internal_url):

            """Fetch page and extract links - optimized for parallel processing"""

            nonlocal processed_pages, total_links_found

            

            # Check cancellation before processing each page

            if is_job_cancelled(job.jobId):

                print(f"🛑 Job {job.jobId} cancelled during page processing")

                return internal_url, [], [], []

            

            try:

                # Check HTML cache first

                if internal_url in html_cache:

                    page_html = html_cache[internal_url]

                else:

                    page_html, page_status, page_response_time, _ = fetch_html(internal_url, timeout=8)  # Reduced timeout

                    if page_status != 200:

                        return internal_url, [], [], []  # Return empty results

                    html_cache[internal_url] = page_html

                

                # Extract all links using centralized function
                page_internal_links, external_links, social_links = extract_all_links_from_html(page_html, internal_url, base_domain)
                
                # Normalize all discovered links
                normalized_page_internal = [normalize_url(link) if isinstance(link, str) else normalize_url(link.get("url", "")) for link in page_internal_links]
                # External links disabled - skip normalization
                normalized_social = [{**link_data, "url": normalize_url(link_data["url"])} for link_data in social_links]
                
                # Update progress after each page
                processed_pages += 1
                
                # Update total links found count (external links excluded)
                total_links_found = len(seen_internal) + len(seen_social)
                
                progress_percentage = 30 + int((processed_pages / total_pages) * 60)  # 30% to 90%
                send_progress_update(
                    job.jobId, 
                    progress_percentage, 
                    "Analyze", 
                    "Analyzing all links", 
                    f"Discovered {total_links_found} links so far..."
                )
                
                return internal_url, [], normalized_social, normalized_page_internal  # External links disabled

                

            except Exception as page_error:

                # Silent error handling to reduce log noise

                processed_pages += 1  # Still increment to avoid getting stuck

                return internal_url, [], [], []

        

        # Process pages in parallel with ThreadPoolExecutor

        external_docs = []

        social_docs = []

        # all_internal_links already initialized at function start

        

        with ThreadPoolExecutor(max_workers=8) as executor:

            # Submit all tasks

            futures = [executor.submit(fetch_and_extract_links, internal_url) for internal_url in all_internal_urls]

            

            # Collect results as they complete

            for future in as_completed(futures):

                # Check cancellation during result collection

                if is_job_cancelled(job.jobId):

                    print(f"🛑 Job {job.jobId} cancelled during result collection")

                    # Cancel remaining futures

                    for f in futures:

                        f.cancel()

                    return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

                

                internal_url, external_links, social_links, page_internal_links = future.result()

                

                # Add internal links to accumulator

                all_internal_links.extend(page_internal_links)

                

                # External links disabled - skip storage

                

                # Add social links (deduplicated)

                for link_data in social_links:

                    normalized_url = link_data["url"]

                    if normalized_url not in seen_social:

                        social_docs.append({

                            "seo_jobId": ObjectId(job.jobId),

                            "projectId": ObjectId(job.projectId),

                            "platform": link_data["platform"],

                            "url": normalized_url,

                            "sourceUrl": link_data["sourceUrl"],

                            "discoveredAt": datetime.utcnow()

                        })

                        seen_social.add(normalized_url)

        

        # Final cancellation check before completion

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} cancelled before completion")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Bulk insert social links (external links disabled - skip insert)
        if social_docs:
            seo_social_links.insert_many(social_docs, ordered=False)

            

        external_count = 0  # External links disabled

        social_count = len(social_docs)



        # Calculate duration before sending callbacks

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)



        # 5. Prepare completion stats (external links excluded from total)

        total_urls = len(seen_internal) + len(seen_social)

        stats = {
            "internalLinksCount": internal_count,
            "externalLinksCount": external_count,
            "socialLinksCount": social_count,
            "totalUrlsFound": total_urls,
            "sitemapDiscovery": sitemap_discovery_stats
        }

        

        # Store stats in result_data for summary aggregation

        result_data = {
            "internalLinksCount": internal_count,
            "externalLinksCount": external_count,
            "socialLinksCount": social_count,
            "totalUrlsFound": total_urls,
            "duration_ms": duration_ms,
            "sitemapDiscovery": sitemap_discovery_stats
        }

        

        # Send final progress update before completion

        send_progress_update(job.jobId, 95, "Complete", "Finalizing results", f"Found {total_urls} total links ({internal_count} internal, {external_count} external, {social_count} social)")

        

        print(f"✅ Job {job.jobId} completed: {stats}")

        print(f"📊 Found {internal_count} internal, 0 external (disabled), {social_count} social links")

        

        # 6. Send completion callback to Node.js

        try:
            # Validate required environment variables
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            node_url = f"{node_backend_url}/api/jobs/{job.jobId}/complete"

            callback_payload = {"stats": stats, "result_data": result_data}

            try:
                response = requests.post(node_url, json=callback_payload, timeout=10)
                response.raise_for_status()
            except Exception as callback_error:
                print(f"❌ Failed to notify Node.js: {callback_error}")

                # Try to mark job as failed instead
                try:
                    node_backend_url = os.environ.get("NODE_BACKEND_URL")
                    if not node_backend_url:
                        raise Exception("NODE_BACKEND_URL is required")
                    node_fail_url = f"{node_backend_url}/api/jobs/{job.jobId}/fail"

                    fail_payload = {"error": str(callback_error), "stats": stats}

                    requests.post(node_fail_url, json=fail_payload, timeout=10)
                except Exception as fail_error:
                    print(f"❌ Failed to mark job as failed: {fail_error}")

            print(f"✅ Successfully notified Node.js of job completion")

        except Exception as callback_error:
            print(f"❌ Failed to notify Node.js: {callback_error}")

            # Try to mark job as failed instead
            try:
                node_backend_url = os.environ.get("NODE_BACKEND_URL")
                if not node_backend_url:
                    raise Exception("NODE_BACKEND_URL is required")
                node_fail_url = f"{node_backend_url}/api/jobs/{job.jobId}/fail"

                fail_payload = {"error": str(callback_error), "stats": stats}

                requests.post(node_fail_url, json=fail_payload, timeout=10)
            except Exception as fail_error:
                print(f"❌ Failed to mark job as failed: {fail_error}")

            raise HTTPException(status_code=500, detail="Failed to notify Node.js of completion")

        

        return {

            "status": "success",

            "jobId": job.jobId,

            "stats": stats,

            "duration_ms": duration_ms,

            "message": "Link discovery completed and results stored"

        }

        

    except Exception as e:

        print(f"❌ Job {job.jobId} failed: {str(e)}")

        

        # Safely compute duration even in exception case

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        

        # Send failure callback to Node.js

        try:
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            node_fail_url = f"{node_backend_url}/api/jobs/{job.jobId}/fail"

            fail_payload = {"error": str(e)}

            try:
                requests.post(node_fail_url, json=fail_payload, timeout=10)
            except Exception as fail_error:
                print(f"❌ Failed to mark job as failed: {fail_error}")
        except Exception as fail_error:
            print(f"❌ Failed to mark job as failed: {fail_error}")

        except:

            pass

            

        raise HTTPException(status_code=500, detail=str(e))

