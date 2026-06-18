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
from scraper.shared.fetcher import probe_rendering_need, set_audit_user_agent
# from scraper.shared.screenshots import clear_screenshot_registry, take_page_screenshot  # DISABLED

from scraper.shared.utils import normalize_url, get_registrable_domain
from shared.context_enrichment import enrich_from_page_data

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
# SEO ENRICHMENT LAYER - P0 FUNCTIONS
# ---------------------------------------------------------------------------

def check_link_status_batch(links: list, base_url: str, timeout: int = 3, max_links: int = 20) -> dict:
    """Check HTTP status for links in parallel. Returns only broken_links_count."""
    if not links:
        return {"broken_links_count": 0}

    try:
        broken_count = 0

        def check_single_link(url):
            try:
                response = requests.head(
                    url,
                    timeout=(1, timeout),
                    allow_redirects=True,
                    headers={'User-Agent': random.choice(USER_AGENTS)}
                )
                return response.status_code
            except Exception:
                return None

        limited_links = links[:max_links]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_single_link, url): url for url in limited_links}
            for future in as_completed(futures):
                status_code = future.result()
                if status_code is not None and status_code >= 400:
                    broken_count += 1

        return {"broken_links_count": broken_count}

    except Exception as e:
        return {"broken_links_count": 0, "error": str(e)}


def calculate_html_metrics(html: str) -> dict:
    """
    Calculate code-to-HTML ratio and size metrics.
    
    Args:
        html: Raw HTML string
        
    Returns:
        dict with html_size_bytes, visible_text_bytes, code_to_html_ratio
    """
    try:
        if not html:
            return {
                "html_size_bytes": 0,
                "visible_text_bytes": 0,
                "code_to_html_ratio": 0,
                "error": "no_html_provided"
            }
        
        # Total HTML size
        html_size = len(html.encode('utf-8'))
        
        # Create copy for visible text extraction
        text_soup = BeautifulSoup(html, "lxml")
        
        # Remove non-visible elements
        for element in text_soup(["script", "style", "noscript", "meta", "link"]):
            element.decompose()
        
        # Get visible text
        visible_text = text_soup.get_text(strip=True, separator=' ')
        visible_text_size = len(visible_text.encode('utf-8'))
        
        # Calculate ratio
        code_to_html_ratio = (visible_text_size / html_size * 100) if html_size > 0 else 0
        
        return {
            "html_size_bytes": html_size,
            "visible_text_bytes": visible_text_size,
            "code_to_html_ratio": round(code_to_html_ratio, 2),
            "code_size_bytes": html_size - visible_text_size
        }
        
    except Exception as e:
        return {
            "html_size_bytes": 0,
            "visible_text_bytes": 0,
            "code_to_html_ratio": 0,
            "error": str(e)
        }


def analyze_url_structure(current_url: str, internal_links: list) -> dict:
    """
    Analyze URL structure for SEO issues.
    
    Args:
        current_url: Current page URL
        internal_links: List of internal links
        
    Returns:
        dict with URL structure analysis
    """
    try:
        from urllib.parse import urlparse, parse_qs
        
        parsed = urlparse(current_url)
        
        # Check for parameters
        has_params = bool(parsed.query)
        param_count = len(parse_qs(parsed.query)) if has_params else 0
        
        # Check for double slashes in path
        path_clean = parsed.path.replace('https://', '').replace('http://', '')
        has_double_slash = '//' in path_clean
        
        # URL length analysis
        url_length = len(current_url)
        
        # Analyze internal links for length issues
        long_urls = []
        for link in internal_links[:100]:  # Limit for performance
            if len(link) > 115:
                long_urls.append({
                    "url": link,
                    "length": len(link),
                    "excess_length": len(link) - 115
                })
        
        return {
            "url_length": url_length,
            "has_parameters": has_params,
            "parameter_count": param_count,
            "has_double_slash": has_double_slash,
            "long_urls_count": len(long_urls),
            "long_urls": long_urls[:10]  # Limit output
        }
        
    except Exception as e:
        return {
            "url_length": 0,
            "has_parameters": False,
            "parameter_count": 0,
            "has_double_slash": False,
            "long_urls_count": 0,
            "error": str(e)
        }


def detect_mixed_content(soup: BeautifulSoup, base_url: str) -> dict:
    """
    Detect HTTP resources on HTTPS pages.
    
    Args:
        soup: BeautifulSoup object
        base_url: Base URL of the page
        
    Returns:
        dict with mixed content analysis
    """
    try:
        if not base_url.startswith('https://'):
            return {
                "mixed_content_detected": False,
                "mixed_content_count": 0,
                "mixed_resources": [],
                "reason": "page_not_https"
            }
        
        mixed_resources = []
        
        # Check all resource types
        resource_tags = {
            'img': 'src',
            'script': 'src', 
            'link': 'href',
            'iframe': 'src',
            'video': 'src',
            'audio': 'src'
        }
        
        for tag_name, attr in resource_tags.items():
            for tag in soup.find_all(tag_name):
                resource_url = tag.get(attr)
                if resource_url and resource_url.startswith('http://'):
                    mixed_resources.append({
                        "tag": tag_name,
                        "url": resource_url,
                        "attribute": attr
                    })
        
        return {
            "mixed_content_detected": len(mixed_resources) > 0,
            "mixed_content_count": len(mixed_resources),
            "mixed_resources": mixed_resources[:20]  # Limit output
        }
        
    except Exception as e:
        return {
            "mixed_content_detected": False,
            "mixed_content_count": 0,
            "mixed_resources": [],
            "error": str(e)
        }


def detect_navigation_enhanced(soup: BeautifulSoup) -> dict:
    """
    Enhanced navigation detection using multi-layered approach.
    
    Detects navigation through:
    1. Semantic HTML (<nav>, role="navigation", aria-label)
    2. Structural patterns (ul > li > a with multiple links)
    3. Link density and context validation
    4. Fallback content analysis
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        dict with navigation detection results
    """
    detection_result = {
        "has_navigation": False,
        "detection_method": None,
        "nav_elements_found": [],
        "link_count": 0
    }
    
    try:
        # Layer 1: Semantic HTML Detection
        nav_elements = []
        
        # Check for <nav> tag (any class)
        nav_tags = soup.find_all('nav')
        for nav in nav_tags:
            nav_elements.append({
                'type': 'nav_tag',
                'class': nav.get('class', []),
                'id': nav.get('id', ''),
                'role': nav.get('role', '')
            })
        
        # Check for role="navigation"
        role_nav = soup.find_all(attrs={'role': re.compile(r'navigation', re.IGNORECASE)})
        for nav in role_nav:
            if nav.name not in ['nav']:  # Avoid duplicates
                nav_elements.append({
                    'type': 'role_navigation',
                    'tag': nav.name,
                    'class': nav.get('class', []),
                    'id': nav.get('id', '')
                })
        
        # Check for aria-label containing navigation/menu
        aria_nav = soup.find_all(attrs={'aria-label': re.compile(r'(navigation|menu|main)', re.IGNORECASE)})
        for nav in aria_nav:
            if nav.name not in ['nav'] and not any(n['type'] == 'role_navigation' for n in nav_elements):
                nav_elements.append({
                    'type': 'aria_label',
                    'tag': nav.name,
                    'class': nav.get('class', []),
                    'aria_label': nav.get('aria-label', '')
                })
        
        if nav_elements:
            detection_result['nav_elements_found'] = nav_elements
            detection_result['has_navigation'] = True
            detection_result['detection_method'] = 'semantic_html'
            return detection_result
        
        # Layer 2: Structural Pattern Detection
        # Look for ul > li > a structures with multiple links
        all_uls = soup.find_all('ul')
        for ul in all_uls:
            # Get all direct li children
            lis = ul.find_all('li', recursive=False)
            if len(lis) >= 3:  # Require at least 3 items for navigation
                # Check if lis contain links
                link_count = 0
                for li in lis:
                    links = li.find_all('a', href=True)
                    if links:
                        link_count += 1
                
                if link_count >= 3:
                    # Check if this ul is in a navigation-like container
                    parent = ul.parent
                    if parent:
                        parent_class = ' '.join(parent.get('class', [])).lower()
                        parent_id = parent.get('id', '').lower()
                        parent_role = parent.get('role', '').lower()
                        
                        # Check for navigation indicators in parent
                        nav_indicators = ['nav', 'menu', 'navigation', 'header', 'sidebar', 'aside']
                        has_nav_indicator = (
                            any(ind in parent_class for ind in nav_indicators) or
                            any(ind in parent_id for ind in nav_indicators) or
                            'navigation' in parent_role
                        )
                        
                        if has_nav_indicator:
                            nav_elements.append({
                                'type': 'structural_pattern',
                                'parent_tag': parent.name,
                                'parent_class': parent.get('class', []),
                                'parent_id': parent.get('id', ''),
                                'link_count': link_count
                            })
                            detection_result['link_count'] = link_count
                            detection_result['has_navigation'] = True
                            detection_result['detection_method'] = 'structural_pattern'
                            return detection_result
        
        # Layer 3: Header/Footer Context Detection
        # Check header for link lists
        header = soup.find('header')
        if header:
            header_links = header.find_all('a', href=True)
            if len(header_links) >= 3:
                # Filter out footer-like links
                non_footer_links = [
                    link for link in header_links
                    if not any(term in link.get_text().lower() for term in ['privacy', 'terms', 'legal', 'copyright'])
                ]
                if len(non_footer_links) >= 3:
                    nav_elements.append({
                        'type': 'header_context',
                        'link_count': len(non_footer_links)
                    })
                    detection_result['link_count'] = len(non_footer_links)
                    detection_result['has_navigation'] = True
                    detection_result['detection_method'] = 'header_context'
                    return detection_result
        
        # Layer 4: Fallback - Class Pattern Detection (Enhanced)
        # Check for any element with navigation-related classes
        class_patterns = [
            r'nav', r'menu', r'navigation', r'navbar', r'main-menu',
            r'primary-menu', r'secondary-menu', r'top-menu', r'header-menu'
        ]
        
        for pattern in class_patterns:
            elements = soup.find_all(class_=re.compile(pattern, re.IGNORECASE))
            for elem in elements:
                # Check if element has links
                links = elem.find_all('a', href=True)
                if len(links) >= 3:
                    # Filter out footer links
                    non_footer_links = [
                        link for link in links
                        if not any(term in link.get_text().lower() for term in ['privacy', 'terms', 'legal'])
                    ]
                    if len(non_footer_links) >= 3:
                        nav_elements.append({
                            'type': 'class_pattern',
                            'tag': elem.name,
                            'class': elem.get('class', []),
                            'link_count': len(non_footer_links)
                        })
                        detection_result['link_count'] = len(non_footer_links)
                        detection_result['has_navigation'] = True
                        detection_result['detection_method'] = 'class_pattern'
                        return detection_result
        
        detection_result['nav_elements_found'] = nav_elements
        return detection_result
        
    except Exception as e:
        return {
            "has_navigation": False,
            "detection_method": "error",
            "error": str(e)
        }


def analyze_404_page(soup: BeautifulSoup, status_code: int, nav_detection: dict = None) -> dict:
    """
    Analyze 404 page content for helpful navigation.
    
    Args:
        soup: BeautifulSoup object
        status_code: HTTP status code
        nav_detection: Optional pre-computed navigation detection results
        
    Returns:
        dict with 404 page analysis
    """
    try:
        if status_code != 404:
            return {
                "is_404_page": False,
                "custom_404_detected": False
            }
        
        # Use provided or enhanced navigation detection
        if not nav_detection:
            nav_detection = detect_navigation_enhanced(soup)
        
        has_navigation = nav_detection.get('has_navigation', False)
        
        has_home_link = bool(soup.find('a', href=re.compile(r'^(\/|#|https?:\/\/.*\/?$)', re.IGNORECASE)) or 
                           soup.find('a', string=re.compile(r'home|homepage', re.IGNORECASE)))
        has_search = bool(soup.find('input', type='search') or soup.find('form'))
        
        # Check for helpful text
        page_text = soup.get_text().lower()
        helpful_indicators = ['not found', 'page not found', 'error 404', 'broken link', 'moved', 'home', 'search']
        has_helpful_text = any(indicator in page_text for indicator in helpful_indicators)
        
        # Determine if it's a custom 404
        is_custom_404 = has_navigation or has_home_link or has_search or has_helpful_text
        
        return {
            "is_404_page": True,
            "custom_404_detected": is_custom_404,
            "has_navigation": has_navigation,
            "has_home_link": has_home_link,
            "has_search": has_search,
            "has_helpful_text": has_helpful_text,
            "navigation_detection": nav_detection
        }
        
    except Exception as e:
        return {
            "is_404_page": False,
            "custom_404_detected": False,
            "has_navigation": False,
            "has_home_link": False,
            "has_search": False,
            "has_helpful_text": False,
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

        # Use canonical_urls from URL_QUALIFICATION if provided; fall back to DB selection.
        # canonical_urls guarantees the same URL set across all parallel workers.
        if getattr(job, 'canonical_urls', None):
            urls_to_scrape = list(job.canonical_urls)
            print(f"[DEBUG] Using canonical_urls from URL_QUALIFICATION: {len(urls_to_scrape)} URLs")
        else:
            urls_to_scrape = get_top_urls(job.projectId, limit=25)
            print(f"[DEBUG] Fallback: deterministic URL selection: {len(urls_to_scrape)} URLs")

        total_pages = len(urls_to_scrape)

        print(f"[DEBUG] URLs: {urls_to_scrape[:5]}...")  # Show first 5 for debugging
        print(f"[WORKER] PAGE_SCRAPING started | jobId={job.jobId} | selectedUrls={total_pages}")

        

        # Clear screenshot registry at the start of each job - DISABLED
        # clear_screenshot_registry()

        

        # Check for cancellation early

        if is_job_cancelled(job.jobId):

            print(f"[WORKER] PAGE_SCRAPING cancelled | jobId={job.jobId}")

            return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

        

        # Thread-safe counters — shared across 8 worker threads.
        # All mutations go through _counter_lock to prevent race conditions.
        successful_pages = 0
        failed_pages = 0
        completed_pages = 0
        _counter_lock = __import__('threading').Lock()

        TARGET_SUCCESSES = 25  # Stop collecting results once this many succeed

        def scrape_single_url(url):
            """Scrape a single URL - optimized for concurrent processing"""
            nonlocal successful_pages, failed_pages, completed_pages

            

            # Check cancellation before processing each URL

            if is_job_cancelled(job.jobId):

                return None

            

            try:
                print(f"[SCRAPE] Starting URL: {url}")
                
                # Scrape page data using comprehensive extraction with timeout
                # Use threading.Timer for cross-platform timeout (Windows-compatible)
                import threading
                
                result_container = {'page_data': None, 'exception': None}
                
                def scrape_with_timeout():
                    try:
                        result_container['page_data'] = scrape_page_data(url)
                    except Exception as e:
                        result_container['exception'] = e
                
                # Start scraping in a thread
                thread = threading.Thread(target=scrape_with_timeout)
                thread.start()
                thread.join(timeout=120)  # 120-second timeout
                
                if thread.is_alive():
                    print(f"[SCRAPE] TIMEOUT for URL: {url} | timeout=120s")
                    with _counter_lock:
                        failed_pages += 1
                        completed_pages += 1
                    return None

                # Check for exceptions
                if result_container['exception']:
                    print(f"[SCRAPE] EXCEPTION for URL: {url} | error={str(result_container['exception'])}")
                    with _counter_lock:
                        failed_pages += 1
                        completed_pages += 1
                    return None

                page_data = result_container['page_data']

                # Check if scraping succeeded
                if not page_data:
                    print(f"[SCRAPE] FAILED for URL: {url} | reason=scrape_page_data returned None")
                    with _counter_lock:
                        failed_pages += 1
                        completed_pages += 1
                    return None

                # Check status code — non-200 is a content failure for PAGE_SCRAPING
                status_code = page_data.get("http_status_code")
                if status_code and status_code != 200:
                    print(f"[SCRAPE] FAILED for URL: {url} | status_code={status_code} | reason=non-200 status")
                    with _counter_lock:
                        failed_pages += 1
                        completed_pages += 1
                    return None

                # Check extraction status
                if page_data.get("extraction_status") != "SUCCESS":
                    print(f"[SCRAPE] FAILED for URL: {url} | extraction_status={page_data.get('extraction_status')} | reason=extraction failed")
                    with _counter_lock:
                        failed_pages += 1
                        completed_pages += 1
                    return None
                
                print(f"[SCRAPE] SUCCESS for URL: {url} | status_code={status_code}")

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

                    with _counter_lock:
                        successful_pages += 1
                        completed_pages += 1
                        _completed_snap = completed_pages

                    # Throttled progress update (every 3 pages or at 100%)
                    percentage = int((_completed_snap / total_pages) * 100) if total_pages > 0 else 100

                    if _completed_snap % 3 == 0 or _completed_snap == total_pages or percentage == 100:

                        send_progress_update(

                            job.jobId,

                            percentage,

                            "Scraping",

                            "Scraping website pages",

                            f"{_completed_snap} of {total_pages} pages scraped"

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

                    # --- SEO ENRICHMENT LAYER - P0 FUNCTIONS ---
                    try:
                        # Get necessary data for SEO analysis
                        raw_html = page_data.get("raw_html", "")
                        internal_links = page_data.get("internal_links", [])
                        http_status = page_data.get("http_status_code", 200)

                        # Create BeautifulSoup object ONCE for all DOM analysis
                        seo_soup = BeautifulSoup(raw_html, "lxml") if raw_html else None
                        
                        # A. Link Status Analysis
                        if internal_links:
                            link_analysis = check_link_status_batch(internal_links, url)
                            page_data.update(link_analysis)
                        
                        # B. HTML Metrics
                        if raw_html:
                            html_metrics = calculate_html_metrics(raw_html)
                            page_data.update(html_metrics)
                        
                        # C. URL Structure Analysis
                        url_structure = analyze_url_structure(url, internal_links)
                        page_data.update(url_structure)
                        
                        # D. Mixed Content Detection
                        if seo_soup:
                            mixed_content = detect_mixed_content(seo_soup, url)
                            page_data.update(mixed_content)
                        
                        # E. Navigation Detection (for ALL pages)
                        if seo_soup:
                            nav_detection = detect_navigation_enhanced(seo_soup)
                            has_home_link = bool(seo_soup.find('a', href=re.compile(r'^(\/|#|https?:\/\/.*\/?$)', re.IGNORECASE)) or 
                                               seo_soup.find('a', string=re.compile(r'home|homepage', re.IGNORECASE)))
                            
                            page_data.update({
                                "has_navigation": nav_detection.get('has_navigation', False),
                                "has_home_link": has_home_link,
                                "navigation_detection": nav_detection
                            })
                        else:
                            nav_detection = None
                        
                        # H. 404 Page Analysis
                        error_page_analysis = analyze_404_page(seo_soup, http_status, nav_detection)
                        page_data.update(error_page_analysis)
                        
                        # Concise validation log (replaces verbose debug block)
                        required_fields = [
                            'broken_links_count', 'code_to_html_ratio',
                            'mixed_content_detected', 'url_length', 'custom_404_detected'
                        ]
                        missing = [f for f in required_fields if f not in page_data]
                        if missing:
                            print(f"[SEO] ⚠️ Missing fields for {url}: {missing}")
                        
                    except Exception as seo_err:
                        print(f"[SEO ERROR] Enrichment failed for {url}: {seo_err}")
                        import traceback
                        traceback.print_exc()
                        page_data.update({
                            "seo_enrichment_status": "partial",
                            "seo_enrichment_error": str(seo_err)[:200]
                        })

                    # --- CONTEXT ENRICHMENT (framework, CMS, page type) ---
                    # Runs after SEO enrichment so raw_html is guaranteed to be present.
                    # Stores structured results under page_context AND flattens to top-level
                    # framework / cms / page_type fields for backward compat.
                    try:
                        page_context = enrich_from_page_data(page_data, url)
                        page_data["page_context"] = page_context

                        # Flatten for backward-compat with existing queries
                        fw = page_context.get("framework", {})
                        cms = page_context.get("cms")
                        pt = page_context.get("pageType", {})

                        page_data["framework"] = fw.get("key", "generic")
                        page_data["cms"] = cms.get("name") if cms else None
                        # Dedicated page_type fields — top-level, directly queryable
                        page_data["page_type"] = pt.get("name", "Generic")
                        page_data["page_type_confidence"] = pt.get("confidence", 0)

                        print(
                            f"[CONTEXT] {url} | "
                            f"framework={fw.get('name')} ({fw.get('confidence')}%) | "
                            f"cms={cms.get('name') if cms else 'none'} | "
                            f"pageType={pt.get('name')} ({pt.get('confidence')}%)"
                        )
                    except Exception as ctx_err:
                        print(f"[CONTEXT ERROR] Context enrichment failed for {url}: {ctx_err}")
                        # Non-fatal: leave framework/cms/page_type as whatever was set before

                    return page_data

            except TimeoutError as te:

                with _counter_lock:
                    failed_pages += 1
                    completed_pages += 1
                    _snap = completed_pages

                print(f"[TIMEOUT] URL scraping timed out after 60s: {url}")

                percentage = int((_snap / total_pages) * 100) if total_pages > 0 else 100

                send_progress_update(

                    job.jobId,

                    percentage,

                    "Scraping",

                    "Scraping website pages",

                    f"{_snap} of {total_pages} pages scraped"

                )

                

                return {

                    "url": url,

                    "seo_jobId": ObjectId(job.jobId),

                    "projectId": ObjectId(job.projectId),

                    "sourceJobId": ObjectId(job.sourceJobId) if job.sourceJobId else None,

                    "scrapedAt": datetime.utcnow(),

                    "scrape_status": "TIMEOUT",

                    "error": f"Timeout after 60 seconds: {str(te)}",

                    "screenshot_path": None,

                    "internal_links": []

                }

            except Exception as e:

                with _counter_lock:
                    failed_pages += 1
                    completed_pages += 1
                    _snap = completed_pages

                percentage = int((_snap / total_pages) * 100) if total_pages > 0 else 100

                send_progress_update(

                    job.jobId,

                    percentage,

                    "Scraping",

                    "Scraping website pages",

                    f"{_snap} of {total_pages} pages scraped"

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

        

        # Process URLs with ThreadPoolExecutor (OPTIMIZED: increased to 8 workers for better performance)

        all_results = []
        
        # DEBUG: Verify URLs before scraping (no modifications should happen)
        print(f"[DEBUG] Before scraping: {len(urls_to_scrape)} URLs ready for processing")
        print(f"[DEBUG] Final URLs to scrape: {urls_to_scrape[:5]}...")  # Show first 5

        # Fix B: one stable User-Agent shared by all 8 threads for this audit run.
        # Eliminates per-request random UA variation that can cause servers to return
        # different HTML (UA-differentiated content) between audit runs.
        audit_ua = random.choice(USER_AGENTS)
        set_audit_user_agent(audit_ua)

        # Probe first 5 URLs before fan-out so all worker threads agree on
        # static vs Playwright from the start, eliminating the render race condition.
        if urls_to_scrape:
            for probe_url in urls_to_scrape[:5]:
                if probe_rendering_need(probe_url):
                    break  # Domain flagged as JS — cache populated, stop probing

        with ThreadPoolExecutor(max_workers=8) as executor:

            # Submit ALL canonical URLs — success-targeting means we keep collecting
            # until TARGET_SUCCESSES succeed or the full pool is exhausted.
            futures = [executor.submit(scrape_single_url, url) for url in urls_to_scrape]

            # Collect results as they complete
            for future in as_completed(futures):

                if is_job_cancelled(job.jobId):

                    print(f"🛑 Job {job.jobId} cancelled during scraping")

                    for f in futures:
                        f.cancel()

                    return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}

                result = future.result()

                if result:
                    all_results.append(result)
                    # Stop collecting once we have enough successful pages.
                    # In-flight threads still run to completion but their results are discarded.
                    if len(all_results) >= TARGET_SUCCESSES:
                        for f in futures:
                            f.cancel()
                        break

        

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

            # Build bulk operations instead of sequential update_one calls
            from pymongo import UpdateOne
            bulk_ops = []
            for result in all_results:
                if not result or not isinstance(result, dict):
                    continue
                url = result.get("url")
                if not url or not isinstance(url, str):
                    continue
                bulk_ops.append(
                    UpdateOne(
                        {"url": url, "seo_jobId": ObjectId(job.sourceJobId)},
                        {"$set": {"crawledAt": datetime.utcnow()}}
                    )
                )
            
            if bulk_ops:
                bulk_result = seo_internal_links.bulk_write(bulk_ops, ordered=False)
                print(f"[WORKER] Bulk updated internal links | jobId={job.jobId} | matched={bulk_result.matched_count} | modified={bulk_result.modified_count}")

        except Exception as update_error:
            print(f"[ERROR] Failed to update internal links | jobId={job.jobId} | reason=\"{str(update_error)}\"")

        

        # Calculate duration before sending callbacks

        end_time = datetime.utcnow()

        duration_ms = int((end_time - start_time).total_seconds() * 1000)



        # Read final counter values once under lock for consistent reporting
        with _counter_lock:
            _final_successful = successful_pages
            _final_failed = failed_pages
            _final_completed = completed_pages

        attempted_urls = _final_completed
        _success_rate = round((_final_successful / attempted_urls) * 100, 2) if attempted_urls > 0 else 0

        # Prepare completion stats (backward-compatible keys + new metrics)
        stats = {
            "totalUrls": total_pages,
            "attemptedUrls": attempted_urls,
            "successfulPages": _final_successful,
            "failedPages": _final_failed,
            "successRate": _success_rate,
            "targetReached": _final_successful >= TARGET_SUCCESSES,
            "poolExhausted": attempted_urls >= total_pages and _final_successful < TARGET_SUCCESSES,
        }

        result_data = {
            "discoveredUrls": total_pages,
            "candidateUrls": total_pages,
            "attemptedUrls": attempted_urls,
            "successfulPages": _final_successful,
            "failedUrls": _final_failed,
            "successRate": _success_rate,
            "targetReached": _final_successful >= TARGET_SUCCESSES,
            "poolExhausted": attempted_urls >= total_pages and _final_successful < TARGET_SUCCESSES,
            "duration_ms": duration_ms,
        }

        

        print(f"[WORKER] PAGE_SCRAPING completed | jobId={job.jobId} | success={_final_successful} | failed={_final_failed} | targetReached={stats['targetReached']}")

        

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

    finally:
        # Always clear the audit-level UA so the module-level variable doesn't
        # leak into subsequent audit runs within the same process lifetime.
        set_audit_user_agent(None)

