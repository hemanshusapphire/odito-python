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
# SEO ENRICHMENT LAYER - P0 FUNCTIONS
# ---------------------------------------------------------------------------

def check_link_status_batch(links: list, base_url: str, timeout: int = 7, max_links: int = 30) -> dict:
    """
    Check HTTP status for internal/external links with parallel processing.
    
    Args:
        links: List of URLs to check
        base_url: Base URL for domain comparison
        timeout: Request timeout in seconds
        max_links: Maximum links to process
        
    Returns:
        dict with internal_links_status, external_links_status, broken_links_count
    """
    if not links:
        return {
            "internal_links_status": {},
            "external_links_status": {},
            "broken_links_count": 0
        }
    
    try:
        base_domain = get_registrable_domain(base_url)
        link_status = {"internal": {}, "external": {}}
        broken_count = 0
        
        def check_single_link(url):
            try:
                response = requests.head(
                    url, 
                    timeout=timeout, 
                    allow_redirects=True,
                    headers={'User-Agent': random.choice(USER_AGENTS)}
                )
                return {
                    'status_code': response.status_code,
                    'final_url': response.url,
                    'redirect_count': len(response.history) if hasattr(response, 'history') else 0,
                    'error': None
                }
            except Exception as e:
                return {
                    'status_code': None,
                    'final_url': url,
                    'redirect_count': 0,
                    'error': str(e)[:100]
                }
        
        # Process links in parallel with limits
        limited_links = links[:max_links]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_single_link, url): url for url in limited_links}
            
            for future in as_completed(futures):
                url = futures[future]
                result = future.result()
                
                # Categorize as internal/external
                link_domain = get_registrable_domain(url)
                is_internal = link_domain == base_domain
                
                category = "internal" if is_internal else "external"
                link_status[category][url] = result
                
                # Count broken links (only real 4XX/5XX errors, not timeouts)
                if result['status_code'] is None:
                    # timeout / network issue - DO NOT COUNT as broken
                    continue
                elif result['status_code'] >= 400:
                    broken_count += 1
        
        return {
            "internal_links_status": link_status["internal"],
            "external_links_status": link_status["external"],
            "broken_links_count": broken_count
        }
        
    except Exception as e:
        return {
            "internal_links_status": {},
            "external_links_status": {},
            "broken_links_count": 0,
            "error": str(e)
        }


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


def calculate_keyword_density(text: str, title: str, meta_description: str) -> dict:
    """
    Calculate keyword density and identify primary keyword.
    
    Args:
        text: Page text content
        title: Page title
        meta_description: Meta description
        
    Returns:
        dict with keyword analysis
    """
    try:
        if not text or len(text.strip()) < 50:
            return {
                "keyword_density": 0,
                "primary_keyword": None,
                "keyword_count": 0,
                "total_words": 0,
                "error": "insufficient_text"
            }
        
        # Clean and normalize text
        words = re.findall(r'\b[a-z]+\b', text.lower())
        total_words = len(words)
        
        if total_words < 10:
            return {
                "keyword_density": 0,
                "primary_keyword": None,
                "keyword_count": 0,
                "total_words": total_words
            }
        
        # Get word frequencies
        from collections import Counter
        word_freq = Counter(words)
        
        # Remove common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'may', 'might', 'can', 'this', 'that', 'these',
            'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'what', 'which', 'who',
            'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
            'most', 'other', 'some', 'such', 'only', 'own', 'same', 'so', 'than', 'too',
            'very', 'just', 'now', 'also', 'back', 'even', 'further', 'still', 'yet'
        }
        
        # Filter out stop words and short words
        meaningful_words = {word: count for word, count in word_freq.items() 
                          if word not in stop_words and len(word) > 2}
        
        # Find primary keyword (most frequent meaningful word)
        if meaningful_words:
            primary_keyword = max(meaningful_words, key=meaningful_words.get)
            keyword_count = meaningful_words[primary_keyword]
            keyword_density = (keyword_count / total_words * 100)
            
            # Check if keyword appears in title/meta
            in_title = primary_keyword in title.lower() if title else False
            in_meta = primary_keyword in meta_description.lower() if meta_description else False
            
            return {
                "keyword_density": round(keyword_density, 2),
                "primary_keyword": primary_keyword,
                "keyword_count": keyword_count,
                "total_words": total_words,
                "in_title": in_title,
                "in_meta_description": in_meta
            }
        
        return {
            "keyword_density": 0,
            "primary_keyword": None,
            "keyword_count": 0,
            "total_words": total_words
        }
        
    except Exception as e:
        return {
            "keyword_density": 0,
            "primary_keyword": None,
            "keyword_count": 0,
            "total_words": 0,
            "error": str(e)
        }


def detect_iframe_wrapping(soup: BeautifulSoup) -> dict:
    """
    Detect full-site iframe wrapping.
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        dict with iframe analysis
    """
    try:
        iframes = soup.find_all('iframe')
        
        for iframe in iframes:
            # Check for full-site iframe indicators
            width = iframe.get('width', '').lower()
            height = iframe.get('height', '').lower()
            src = iframe.get('src', '')
            
            # Full-site iframe characteristics
            is_full_width = width in ['100%', '100vw', '1000px', '1200px', '1920px']
            is_full_height = height in ['100%', '100vh', '800px', '900px', '1080px']
            
            if is_full_width and is_full_height:
                return {
                    "full_site_iframe_detected": True,
                    "iframe_src": src,
                    "iframe_dimensions": {"width": width, "height": height},
                    "is_external": bool(src and src.startswith('http') and not src.startswith('https://www.sapphiredigitalagency.com'))
                }
        
        return {"full_site_iframe_detected": False}
        
    except Exception as e:
        return {"full_site_iframe_detected": False, "error": str(e)}


def analyze_404_page(soup: BeautifulSoup, status_code: int) -> dict:
    """
    Analyze 404 page content for helpful navigation.
    
    Args:
        soup: BeautifulSoup object
        status_code: HTTP status code
        
    Returns:
        dict with 404 page analysis
    """
    try:
        if status_code != 404:
            return {
                "is_404_page": False,
                "custom_404_detected": False,
                "has_navigation": False,
                "has_home_link": False,
                "has_search": False,
                "has_helpful_text": False
            }
        
        # Check for helpful elements (improved detection)
        has_navigation = bool(soup.find('nav') or soup.find('header') or soup.find('div', class_=re.compile(r'nav|menu', re.IGNORECASE)))
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
            "has_helpful_text": has_helpful_text
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

                    # --- SEO ENRICHMENT LAYER - P0 FUNCTIONS ---
                    try:
                        print(f"\n🔍 [SEO DEBUG] Starting enrichment for URL: {url}")
                        
                        # Get necessary data for SEO analysis
                        raw_html = page_data.get("raw_html", "")
                        internal_links = page_data.get("internal_links", [])
                        http_status = page_data.get("http_status_code", 200)
                        
                        # Create BeautifulSoup object for DOM analysis
                        seo_soup = BeautifulSoup(raw_html, "lxml") if raw_html else None
                        
                        print(f"[SEO DEBUG] raw_html exists: {bool(raw_html)}, length: {len(raw_html) if raw_html else 0}")
                        print(f"[SEO DEBUG] internal_links count: {len(internal_links)}")
                        print(f"[SEO DEBUG] http_status: {http_status}")
                        print(f"[SEO DEBUG] seo_soup exists: {bool(seo_soup)}")
                        
                        # A. Link Status Analysis
                        print(f"[SEO DEBUG] Checking link status analysis...")
                        if internal_links:
                            print(f"[SEO DEBUG] Running link analysis with {len(internal_links)} links")
                            link_analysis = check_link_status_batch(internal_links, url)
                            print(f"[SEO DEBUG] Link analysis result keys: {list(link_analysis.keys())}")
                            page_data.update(link_analysis)
                        else:
                            print(f"[SEO DEBUG] Skipping link analysis - no internal links")
                        
                        # B. HTML Metrics
                        print(f"[SEO DEBUG] Checking HTML metrics...")
                        if raw_html:
                            print(f"[SEO DEBUG] Running HTML metrics calculation")
                            html_metrics = calculate_html_metrics(raw_html)
                            print(f"[SEO DEBUG] HTML metrics keys: {list(html_metrics.keys())}")
                            page_data.update(html_metrics)
                        else:
                            print(f"[SEO DEBUG] Skipping HTML metrics - no raw HTML")
                        
                        # C. URL Structure Analysis
                        print(f"[SEO DEBUG] Running URL structure analysis...")
                        url_structure = analyze_url_structure(url, internal_links)
                        print(f"[SEO DEBUG] URL structure keys: {list(url_structure.keys())}")
                        page_data.update(url_structure)
                        
                        # D. Mixed Content Detection
                        print(f"[SEO DEBUG] Checking mixed content detection...")
                        if seo_soup:
                            print(f"[SEO DEBUG] Running mixed content detection")
                            mixed_content = detect_mixed_content(seo_soup, url)
                            print(f"[SEO DEBUG] Mixed content keys: {list(mixed_content.keys())}")
                            page_data.update(mixed_content)
                        else:
                            print(f"[SEO DEBUG] Skipping mixed content - no soup")
                        
                        # E. Keyword Density
                        print(f"[SEO DEBUG] Checking keyword density...")
                        content_text = page_data.get("content", {}).get("text", "")
                        page_title = page_data.get("title", "")
                        meta_desc_list = page_data.get("meta_tags", {}).get("description", [])
                        meta_description = meta_desc_list[0] if meta_desc_list else ""
                        
                        print(f"[SEO DEBUG] content_text length: {len(content_text)}")
                        print(f"[SEO DEBUG] page_title: {page_title}")
                        print(f"[SEO DEBUG] meta_description exists: {bool(meta_description)}")
                        
                        if content_text:
                            print(f"[SEO DEBUG] Running keyword density calculation")
                            keyword_analysis = calculate_keyword_density(content_text, page_title, meta_description)
                            print(f"[SEO DEBUG] Keyword analysis keys: {list(keyword_analysis.keys())}")
                            page_data.update(keyword_analysis)
                        else:
                            print(f"[SEO DEBUG] Skipping keyword density - no content text")
                        
                        # F. iFrame Detection
                        print(f"[SEO DEBUG] Checking iframe detection...")
                        if seo_soup:
                            print(f"[SEO DEBUG] Running iframe detection")
                            iframe_analysis = detect_iframe_wrapping(seo_soup)
                            print(f"[SEO DEBUG] iFrame analysis keys: {list(iframe_analysis.keys())}")
                            page_data.update(iframe_analysis)
                        else:
                            print(f"[SEO DEBUG] Skipping iframe detection - no soup")
                        
                        # G. 404 Page Analysis
                        print(f"[SEO DEBUG] Running 404 page analysis...")
                        error_page_analysis = analyze_404_page(seo_soup, http_status)
                        print(f"[SEO DEBUG] 404 analysis keys: {list(error_page_analysis.keys())}")
                        page_data.update(error_page_analysis)
                        
                        # FINAL VALIDATION
                        print(f"\n🔍 [SEO DEBUG] ENRICHMENT COMPLETE - VALIDATING REQUIRED FIELDS:")
                        required_fields = [
                            'broken_links_count', 'internal_links_status', 'external_links_status',
                            'code_to_html_ratio', 'keyword_density', 'primary_keyword',
                            'mixed_content_detected', 'url_length', 'has_parameters',
                            'has_double_slash', 'long_urls_count', 'full_site_iframe_detected',
                            'custom_404_detected'
                        ]
                        
                        missing_fields = []
                        present_fields = []
                        for field in required_fields:
                            if field in page_data:
                                present_fields.append(f"{field}: {page_data[field]}")
                            else:
                                missing_fields.append(field)
                        
                        print(f"[SEO DEBUG] ✅ PRESENT FIELDS ({len(present_fields)}):")
                        for field in present_fields:
                            print(f"  - {field}")
                        
                        if missing_fields:
                            print(f"[SEO DEBUG] ❌ MISSING FIELDS ({len(missing_fields)}):")
                            for field in missing_fields:
                                print(f"  - {field}")
                        else:
                            print(f"[SEO DEBUG] 🎉 ALL REQUIRED FIELDS PRESENT!")
                        
                    except Exception as seo_err:
                        print(f"[SEO ERROR] ❌ SEO enrichment failed for {url}: {seo_err}")
                        import traceback
                        traceback.print_exc()
                        # Add error status but don't crash
                        page_data.update({
                            "seo_enrichment_status": "partial",
                            "seo_enrichment_error": str(seo_err)[:200]
                        })

                    # FINAL OUTPUT LOGGING
                    print(f"\n🔍 [SEO DEBUG] FINAL PAGE DATA OUTPUT:")
                    import json
                    try:
                        output_sample = json.dumps(page_data, indent=2, default=str)[:3000]
                        print(output_sample)
                        if len(json.dumps(page_data, indent=2, default=str)) > 3000:
                            print("\n... (truncated for display)")
                    except Exception as json_err:
                        print(f"[SEO DEBUG] Could not serialize page_data: {json_err}")
                        print(f"[SEO DEBUG] page_data keys: {list(page_data.keys()) if isinstance(page_data, dict) else 'not_dict'}")
                        print(f"[SEO DEBUG] page_data type: {type(page_data)}")

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

