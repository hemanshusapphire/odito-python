"""Link discovery worker implementation."""


def normalize_url_for_selection(url: str) -> str:
    """
    Normalize URL for deterministic selection.
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL (lowercase, trimmed, no trailing slash)
    """
    if not url:
        return url
    
    # Convert to lowercase
    normalized = url.lower()
    
    # Trim whitespace
    normalized = normalized.strip()
    
    # Remove trailing slash (except for root URL)
    if normalized != "https:///" and normalized != "http:///" and normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    
    return normalized


def classify_url_type(url: str) -> str:
    """
    Classify URL into priority categories.
    
    Priority order (highest to lowest):
    1. main - /pages/, /services/, /service/, /about, /contact
    2. collection - /collections/
    3. product - /products/
    4. blog - /blogs/
    5. other - everything else
    
    Args:
        url: URL to classify
        
    Returns:
        Type string: "main", "collection", "product", "blog", or "other"
    """
    if not url:
        return "other"
    
    url_lower = url.lower()
    
    # MAIN - highest priority
    main_patterns = ["/pages/", "/services/", "/service/", "/about", "/contact"]
    for pattern in main_patterns:
        if pattern in url_lower:
            return "main"
    
    # COLLECTION
    if "/collections/" in url_lower:
        return "collection"
    
    # PRODUCT
    if "/products/" in url_lower:
        return "product"
    
    # BLOG
    if "/blogs/" in url_lower:
        return "blog"
    
    # OTHER - lowest priority
    return "other"


def select_urls_deterministically(urls_set, limit: int = 25) -> set:
    """
    Select URLs using deterministic priority-based selection.
    
    Selection order (strict priority):
    1. main (pages + services + about + contact)
    2. collection
    3. product
    4. blog
    5. other
    
    Each group is sorted alphabetically to ensure determinism.
    
    Args:
        urls_set: Set of discovered URLs
        limit: Maximum number of URLs to select (default: 25)
        
    Returns:
        Set of selected URLs
    """
    # Group URLs by type
    groups = {
        "main": [],
        "collection": [],
        "product": [],
        "blog": [],
        "other": []
    }
    
    for url in urls_set:
        url_type = classify_url_type(url)
        groups[url_type].append(url)
    
    # Sort each group alphabetically for determinism
    for url_type in groups:
        groups[url_type].sort()
    
    # Select URLs in priority order
    selected_urls = []
    priority_order = ["main", "collection", "product", "blog", "other"]
    
    for url_type in priority_order:
        # Add URLs from this group until we reach the limit
        for url in groups[url_type]:
            if len(selected_urls) >= limit:
                break
            selected_urls.append(url)
        
        if len(selected_urls) >= limit:
            break
    
    # Log selection results
    print(f"[DETERMINISTIC_SELECTION] Total URLs discovered: {len(urls_set)}")
    print(f"[DETERMINISTIC_SELECTION] URLs per type:")
    for url_type in priority_order:
        count = len(groups[url_type])
        selected_count = sum(1 for url in selected_urls if classify_url_type(url) == url_type)
        print(f"  {url_type}: {count} discovered, {selected_count} selected")
    print(f"[DETERMINISTIC_SELECTION] Final selection: {len(selected_urls)} URLs")
    
    return set(selected_urls)



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

from urllib.parse import urljoin, urlparse



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

from scraper.shared.url_filters import should_skip_seo_url


from db import seo_internal_links, seo_external_links, seo_social_links, seo_page_data, seo_page_issues, db



class ProgressReporter:
    """Throttled, non-blocking progress emitter for link discovery.

    Root cause addressed: the worker previously fired a synchronous HTTP POST on
    every page (blocking crawl threads for up to 5s each) and the backend did a
    MongoDB read per call. This reporter:

      * Coalesces updates — only emits when the percentage advances by at least
        ``min_delta`` or the step changes (milestones force-emit).
      * Emits asynchronously on a daemon thread so crawl workers never block.
      * Includes ``projectId`` so the backend can emit over websockets without a
        per-update ``getJobById`` database read.
    """

    def __init__(self, job_id: str, project_id: str, min_delta: int = 5):
        self.job_id = job_id
        self.project_id = project_id
        self.min_delta = min_delta
        self._last_pct = -1000
        self._last_step = None
        self._lock = threading.Lock()
        self._node_url = os.environ.get("NODE_BACKEND_URL")

    def send(self, percentage: int, step: str, message: str, subtext: str = None, force: bool = False):
        with self._lock:
            if (not force
                    and step == self._last_step
                    and (percentage - self._last_pct) < self.min_delta):
                return
            self._last_pct = percentage
            self._last_step = step
        threading.Thread(
            target=self._post,
            args=(percentage, step, message, subtext),
            daemon=True,
        ).start()

    def _post(self, percentage, step, message, subtext):
        try:
            if not self._node_url:
                return
            requests.post(
                f"{self._node_url}/api/jobs/{self.job_id}/progress",
                json={
                    "percentage": percentage,
                    "step": step,
                    "message": message,
                    "subtext": subtext,
                    "projectId": str(self.project_id) if self.project_id else None,
                },
                timeout=5,
            )
        except Exception as e:
            print(f"⚠️ Failed to send progress update: {e}")


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

    # Performance instrumentation (monotonic clock, immune to wall-clock skew)
    perf_t0 = time.perf_counter()
    metrics = {
        "sitemap_discovery_ms": 0,
        "fetch_phase_ms": 0,
        "db_insert_ms": 0,
        "total_ms": 0,
    }

    # Throttled, non-blocking progress reporter (replaces per-page blocking POSTs)
    progress = ProgressReporter(job.jobId, job.projectId)

    try:

        url = str(job.main_url)

        base_domain = get_registrable_domain(url)

        # Safe default: if the redirect-resolution fetch below fails entirely
        # (before it can set the real value), URL discovery still proceeds
        # exactly as before this change — normalize_url() no-ops when
        # canonical_host is None.
        canonical_host = None



        print(f"🔄 Starting LINK_DISCOVERY job {job.jobId} for URL: {url}")



        # Send initial progress update

        progress.send(5, "Start", "Your website crawling has been started", "Initializing audit process", force=True)

        

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

        

        # RC-9: discovery funnel counters — make each stage's contribution visible so a
        # silent regression (like the previous homepage-extraction crash) is immediately
        # obvious in logs and persisted stats.
        discovery_funnel = {
            "homepageAnchors": 0,
            "sitemapUrls": 0,
            "secondLevelStored": 0,
        }

        # Global deduplication sets per job

        seen_internal = set()

        seen_external = set()

        seen_social = set()

        

        # Initialize all_internal_links accumulator to prevent NameError

        all_internal_links: list[dict] = []

        

        # Per-job HTML cache to avoid re-fetching

        html_cache = {}
        # RC-7: html_cache and the progress counters below are mutated by up to 8
        # ThreadPoolExecutor workers. Guard them with a lock to avoid lost writes,
        # duplicate fetches and corrupted progress percentages.
        shared_state_lock = threading.Lock()

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
            # fetch_html follows redirects internally and returns the final URL as the 5th value.
            # Static-first: discovery only needs anchors, so avoid eager browser renders.
            main_html, main_status, main_response_time, _main_headers, final_url = fetch_html(url, timeout=10, prefer_static_for_links=True)

            # Use final redirected URL for normalization (e.g. apex -> www, http -> https)
            if final_url:
                normalized_main_url = normalize_url(final_url)
                if normalized_main_url != normalize_url(url):
                    print(f"[WORKER] Main URL redirected | original={url} | final={final_url} | normalized={normalized_main_url}")
                url = normalized_main_url  # Update working URL to normalized final URL
            else:
                url = normalize_url(url)

            # Recompute base_domain from the final URL so internal classification matches reality
            base_domain = get_registrable_domain(url)

            # canonical_host: the already-resolved seed URL's host is the single
            # source of truth for this audit run. Every URL discovered from here
            # on (sitemap, anchors, second-level) is folded onto this exact host
            # form via normalize_url(..., canonical_host=canonical_host) so that
            # e.g. https://example.com/ and https://www.example.com/ never end
            # up as two separate seo_internal_links records. Whichever form
            # (www or non-www) the site's own redirect resolved to wins — this
            # is never hardcoded.
            canonical_host = urlparse(url).netloc

            if main_status == 200 and main_html:
                # RC-8: seed the per-job HTML cache with the homepage so it is not
                # re-fetched if the normalized homepage URL is among the selected pages.
                with shared_state_lock:
                    html_cache[url] = main_html

                main_internal_links, main_external_links, main_social_links = extract_all_links_from_html(main_html, url, base_domain)

                # Add main page internal links to both collections
                homepage_added = 0
                for link in main_internal_links:
                    normalized_link = normalize_url(link, canonical_host=canonical_host) if isinstance(link, str) else normalize_url(link.get("url", ""), canonical_host=canonical_host)

                    if normalized_link and normalized_link not in all_internal_urls and not should_skip_seo_url(normalized_link):
                        all_internal_urls.add(normalized_link)
                        all_internal_links.append(normalized_link)  # Add to accumulator for iteration
                        homepage_added += 1

                discovery_funnel["homepageAnchors"] = homepage_added
                print(f"📊 Main page extracted: {len(main_internal_links)} internal anchors ({homepage_added} new), 0 external (disabled), {len(main_social_links)} social links")
            else:
                print(f"⚠️ Main URL returned status={main_status}, html_len={len(main_html) if main_html else 0} — homepage anchors unavailable")
        except Exception as main_error:
            import traceback
            print(f"⚠️ Failed to extract from main URL: {type(main_error).__name__}: {main_error}")
            print(traceback.format_exc())

        

        # 3. Get sitemap URLs using universal recursive discovery with strict filtering
        print(f"[WORKER] Starting universal recursive sitemap discovery | jobId={job.jobId} | url={url}")
        
        _sitemap_t0 = time.perf_counter()
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
            
            discovery_funnel["sitemapUrls"] = len(discovered_sitemap_urls)

            # Add discovered URLs to our collections (recursive_sitemap already filtered;
            # should_skip_seo_url is a defensive layer for any that slipped through)
            for sitemap_url in discovered_sitemap_urls:
                normalized_sitemap_url = normalize_url(sitemap_url, canonical_host=canonical_host)  # Normalize sitemap URLs too
                if not should_skip_seo_url(normalized_sitemap_url):
                    all_internal_urls.add(normalized_sitemap_url)
                    all_internal_links.append(normalized_sitemap_url)
                
        except Exception as sitemap_error:
            print(f"[WORKER] Recursive sitemap discovery failed | jobId={job.jobId} | error=\"{str(sitemap_error)}\"")
            # Fallback to basic extraction if recursive discovery fails
            try:
                sitemap_urls = extract_links_from_sitemap(url)
                url_metadata = {}  # Empty metadata for fallback
                for sitemap_url in sitemap_urls:
                    normalized = normalize_url(sitemap_url, canonical_host=canonical_host)
                    if not should_skip_seo_url(normalized):
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

        metrics["sitemap_discovery_ms"] = int((time.perf_counter() - _sitemap_t0) * 1000)

        # Check cancellation during sitemap processing
        if is_job_cancelled(job.jobId):
            print(f"🛑 Job {job.jobId} cancelled during sitemap processing")
            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}



        # Send progress update after sitemap discovery
        progress.send(25, "Find", "Looking for all links", f"Sitemap discovery completed, found {len(all_internal_urls)} internal URLs")

        # Check for cancellation
        if is_job_cancelled(job.jobId):
            print(f"🛑 Job {job.jobId} cancelled before analysis phase")
            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        # 3. Apply deterministic URL selection (max 25 URLs with priority ordering)
        all_internal_urls = select_urls_deterministically(all_internal_urls, limit=25)

        

        # Send progress for analysis phase

        current_total_links = len(all_internal_links)

        progress.send(30, "Analyze", "Analyzing all links", f"Found {current_total_links} internal links, scanning for more...")

        

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
                normalized_link_url = normalize_url(link_url, canonical_host=canonical_host)
                
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

            _db_t0 = time.perf_counter()
            seo_internal_links.insert_many(internal_docs, ordered=False)
            metrics["db_insert_ms"] += int((time.perf_counter() - _db_t0) * 1000)



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

                # Check HTML cache first (thread-safe read)

                with shared_state_lock:
                    cached_html = html_cache.get(internal_url)

                if cached_html is not None:

                    page_html = cached_html

                else:

                    # Static-first: avoid eager browser renders during discovery
                    page_html, page_status, page_response_time, _, _page_final_url = fetch_html(internal_url, timeout=8, prefer_static_for_links=True)  # Reduced timeout

                    if page_status != 200:

                        return internal_url, [], [], []  # Return empty results

                    with shared_state_lock:
                        html_cache[internal_url] = page_html



                # Extract all links using centralized function
                page_internal_links, external_links, social_links = extract_all_links_from_html(page_html, internal_url, base_domain)
                
                # Normalize all discovered links
                normalized_page_internal = [normalize_url(link, canonical_host=canonical_host) if isinstance(link, str) else normalize_url(link.get("url", ""), canonical_host=canonical_host) for link in page_internal_links]
                # External links disabled - skip normalization
                normalized_social = [{**link_data, "url": normalize_url(link_data["url"])} for link_data in social_links]
                
                # Update progress after each page (thread-safe counter update)
                with shared_state_lock:
                    processed_pages += 1
                    # Update total links found count (external links excluded)
                    total_links_found = len(seen_internal) + len(seen_social)
                    current_processed = processed_pages
                    current_total_found = total_links_found

                progress_percentage = 30 + int((current_processed / total_pages) * 60) if total_pages else 90  # 30% to 90%
                # Throttled + async: coalesces to ~every 5% and never blocks this thread
                progress.send(
                    progress_percentage,
                    "Analyze",
                    "Analyzing all links",
                    f"Discovered {current_total_found} links so far..."
                )
                
                return internal_url, [], normalized_social, normalized_page_internal  # External links disabled

                

            except Exception as page_error:

                # Reduced log noise but keep the reason visible for diagnosis
                print(f"[WORKER] Page fetch/extract failed | url={internal_url} | {type(page_error).__name__}: {str(page_error)[:120]}")

                with shared_state_lock:
                    processed_pages += 1  # Still increment to avoid getting stuck

                return internal_url, [], [], []

        

        # Process pages in parallel with ThreadPoolExecutor

        external_docs = []

        social_docs = []

        # RC-5: second-level internal links discovered while fetching the selected pages.
        # Previously these were appended to all_internal_links AFTER the bulk insert and
        # therefore never persisted, making discovery effectively depth-1 / sitemap-only.
        second_level_docs = []

        # all_internal_links already initialized at function start

        _fetch_t0 = time.perf_counter()

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

                # RC-5: persist newly discovered (second-level) internal links.
                # Dedupe against seen_internal so we never double-store.
                # should_skip_seo_url guard prevents archive/author/tag URLs
                # from bypassing the centralized filter at this stage.
                for sl in page_internal_links:
                    sl_url = sl if isinstance(sl, str) else (sl.get("url", "") if isinstance(sl, dict) else "")
                    if not sl_url:
                        continue
                    sl_norm = normalize_url(sl_url, canonical_host=canonical_host)
                    if sl_norm and sl_norm not in seen_internal and not should_skip_seo_url(sl_norm):
                        seen_internal.add(sl_norm)
                        second_level_docs.append({
                            "url": sl_norm,
                            "sourceUrl": internal_url,
                            "seo_jobId": ObjectId(job.jobId),
                            "projectId": ObjectId(job.projectId),
                            "discoveredAt": datetime.utcnow()
                        })

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

        metrics["fetch_phase_ms"] = int((time.perf_counter() - _fetch_t0) * 1000)

        # Final cancellation check before completion

        if is_job_cancelled(job.jobId):

            print(f"🛑 Job {job.jobId} cancelled before completion")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # RC-5: bulk insert second-level internal links discovered during page crawl
        if second_level_docs:
            try:
                _db_t1 = time.perf_counter()
                seo_internal_links.insert_many(second_level_docs, ordered=False)
                metrics["db_insert_ms"] += int((time.perf_counter() - _db_t1) * 1000)
                internal_count += len(second_level_docs)
                discovery_funnel["secondLevelStored"] = len(second_level_docs)
                print(f"📊 Stored {len(second_level_docs)} second-level internal links")
            except Exception as sl_err:
                print(f"⚠️ Failed to bulk-insert second-level internal links: {sl_err}")

        # Bulk insert social links (external links disabled - skip insert)
        if social_docs:
            _db_t2 = time.perf_counter()
            seo_social_links.insert_many(social_docs, ordered=False)
            metrics["db_insert_ms"] += int((time.perf_counter() - _db_t2) * 1000)

            

        external_count = 0  # External links disabled

        social_count = len(social_docs)



        # Calculate duration before sending callbacks

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)



        # 5. Prepare completion stats (external links excluded from total)

        total_urls = len(seen_internal) + len(seen_social)

        # RC-9: emit the discovery funnel so each stage's contribution is auditable
        print(
            f"[FUNNEL] jobId={job.jobId} | homepageAnchors={discovery_funnel['homepageAnchors']} "
            f"| sitemapUrls={discovery_funnel['sitemapUrls']} "
            f"| secondLevelStored={discovery_funnel['secondLevelStored']} "
            f"| internalStored={internal_count} | social={social_count} | total={total_urls}"
        )

        # Structured performance metrics for observability / benchmarking
        metrics["total_ms"] = int((time.perf_counter() - perf_t0) * 1000)
        print(
            f"[METRICS] jobId={job.jobId} | total_ms={metrics['total_ms']} "
            f"| sitemap_discovery_ms={metrics['sitemap_discovery_ms']} "
            f"| fetch_phase_ms={metrics['fetch_phase_ms']} "
            f"| db_insert_ms={metrics['db_insert_ms']} "
            f"| pages_fetched={total_pages} | urls_total={total_urls}"
        )

        stats = {
            "internalLinksCount": internal_count,
            "externalLinksCount": external_count,
            "socialLinksCount": social_count,
            "totalUrlsFound": total_urls,
            "sitemapDiscovery": sitemap_discovery_stats,
            "discoveryFunnel": discovery_funnel,
            "canonicalHost": canonical_host,
            "canonicalUrl": url,
        }



        # Store stats in result_data for summary aggregation

        result_data = {
            "internalLinksCount": internal_count,
            "externalLinksCount": external_count,
            "socialLinksCount": social_count,
            "totalUrlsFound": total_urls,
            "duration_ms": duration_ms,
            "sitemapDiscovery": sitemap_discovery_stats,
            "discoveryFunnel": discovery_funnel,
            "canonicalHost": canonical_host,
            "canonicalUrl": url,
        }

        

        # Send final progress update before completion

        progress.send(95, "Complete", "Finalizing results", f"Found {total_urls} total links ({internal_count} internal, {external_count} external, {social_count} social)", force=True)

        

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

