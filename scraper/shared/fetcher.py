"""HTTP fetching and Selenium rendering functionality."""

import os
import re
import random
import threading
import logging
import warnings
from datetime import datetime
import time

# === PHASE 1 SAFETY ADDITION ===
# Large HTML protection constant
MAX_HTML_SIZE_MB = 5

# Third-party imports
import requests
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning

# Optional Selenium import
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
    
    # Enforce max 2 concurrent Selenium sessions
    SELENIUM_SEMAPHORE = threading.Semaphore(2)
    
except ImportError:
    SELENIUM_AVAILABLE = False
    SELENIUM_SEMAPHORE = None

# Optional Playwright import (preferred over Selenium for stability)
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
    PLAYWRIGHT_SEMAPHORE = threading.Semaphore(3)  # Max 3 concurrent Playwright sessions
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PLAYWRIGHT_SEMAPHORE = None

# Local imports
from config.config import USER_AGENTS
from .utils import get_domain

# Suppress BeautifulSoup XML parsing warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Configure logging to suppress third-party errors
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# JS Domain Cache
_js_rendering_cache = {}


def _is_js_cached(url: str) -> bool:
    """Check if domain is cached for JS rendering."""
    return get_domain(url) in _js_rendering_cache


def _cache_js_domain(url: str):
    """Cache domain for JS rendering."""
    domain = get_domain(url)
    if domain:
        _js_rendering_cache[domain] = True


def needs_js_rendering(html: str) -> bool:
    """Determine if page needs JavaScript rendering.
    
    Enhanced detection for:
    - React/Next/Vue/Angular SPA frameworks
    - Shopify Liquid templates
    - Client-side rendered content
    - Minimal server-side content
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ")
    words = re.findall(r"\b\w+\b", text)
    
    # Condition 1: Very light content (likely needs JS rendering)
    is_light_content = len(words) < 100
    
    # Condition 2: SPA framework markers
    has_js_markers = bool(
        soup.find(id="root") or 
        soup.find(id="__next") or 
        soup.find(id="app") or
        soup.find(id="vue-app") or
        soup.find("ng-app")
    )
    
    # Condition 3: Shopify-specific markers
    has_shopify_markers = bool(
        "shopify" in html.lower() or
        "myshopify.com" in html.lower() or
        soup.find("script", src=re.compile(r"shopify", re.IGNORECASE))
    )
    
    # Condition 4: Missing critical content despite having HTML structure
    has_title = bool(soup.find("title"))
    has_body_content = len(soup.get_text(strip=True)) > 200
    is_suspicious = has_title and not has_body_content
    
    # Trigger JS rendering if ANY condition is met
    return is_light_content or has_js_markers or has_shopify_markers or is_suspicious


def fetch_html_playwright(url: str, timeout: int = 30) -> tuple[str, int, int, dict]:
    """Fetch HTML using Playwright for JavaScript rendering (preferred over Selenium)."""
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright not installed")
    
    # Enforce max 3 concurrent Playwright sessions
    if PLAYWRIGHT_SEMAPHORE:
        PLAYWRIGHT_SEMAPHORE.acquire()
    
    browser = None
    context = None
    page = None
    
    try:
        start_time = time.time()
        p = sync_playwright().start()
        
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            ignore_https_errors=True
        )
        page = context.new_page()
        
        # Navigate with timeout
        page.set_default_timeout(timeout * 1000)
        response = page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
        
        # Wait for JS rendering with error handling
        try:
            page.wait_for_timeout(2000)  # Reduced to 2s for SPA hydration
        except Exception:
            pass  # Ignore wait timeout, continue with content
        
        # Get HTML
        html = page.content()
        status_code = response.status if response else 200
        response_time_ms = int((time.time() - start_time) * 1000)
        
        # Clean up
        try:
            if page:
                page.close()
            if context:
                context.close()
            if browser:
                browser.close()
            p.stop()
        except Exception:
            pass  # Ignore cleanup errors
        
        return html, status_code, response_time_ms, {}
            
    except Exception as e:
        # Clean up on error
        try:
            if page:
                page.close()
            if context:
                context.close()
            if browser:
                browser.close()
        except Exception:
            pass
        raise RuntimeError(f"Playwright error: {str(e)}")
    finally:
        if PLAYWRIGHT_SEMAPHORE:
            PLAYWRIGHT_SEMAPHORE.release()


def fetch_html_selenium(url: str, timeout: int = 30) -> tuple[str, int, int, dict]:
    """Fetch HTML using Selenium for JavaScript rendering (fallback if Playwright unavailable)."""
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium not installed")

    # Enforce max 2 concurrent Selenium sessions
    if SELENIUM_SEMAPHORE:
        SELENIUM_SEMAPHORE.acquire()
    
    driver = None
    try:
        start_time = time.time()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(timeout)
        driver.get(url)

        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            pass

        response_time_ms = int((time.time() - start_time) * 1000)
        return driver.page_source, 200, response_time_ms, {}

    except WebDriverException as e:
        raise RuntimeError(str(e))

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass  # Ignore cleanup errors
        # Release semaphore for next Selenium session
        if SELENIUM_SEMAPHORE:
            SELENIUM_SEMAPHORE.release()


def fetch_html(url: str, timeout: int = 8) -> tuple[str, int, int, dict]:
    """Primary HTML fetching with JS detection and Playwright/Selenium fallback.
    
    Hybrid approach:
    1. Try HTTP first (fast, lightweight)
    2. If JS detected or HTTP fails, fallback to Playwright (stable)
    3. If Playwright unavailable, fallback to Selenium (legacy)
    
    Returns: (html, status_code, response_time_ms, response_headers)
    """
    
    # === PHASE 1 SAFETY ADDITION ===
    # Large HTML protection
    MAX_HTML_SIZE = MAX_HTML_SIZE_MB * 1024 * 1024  # Convert MB to bytes
    
    # Validate URL format
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid URL: {url}")
    
    # Safe URL normalization - don't modify original
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Malformed URL: {url}")
    except Exception as e:
        raise ValueError(f"URL parsing failed: {url} - {e}")

    # Step 1: Check if this is a Shopify site or cached for JS rendering
    # For Shopify sites, use Playwright directly (more reliable than HTTP)
    domain = get_domain(url)
    is_shopify = False
    
    # Check if URL indicates Shopify
    if domain and ('shopify' in domain.lower() or 'myshopify.com' in url.lower()):
        is_shopify = True
        print(f"[FETCH] Shopify site detected, using Playwright directly: {url}")
    
    if is_shopify or _is_js_cached(url):
        if PLAYWRIGHT_AVAILABLE:
            try:
                html, status, rt, _ = fetch_html_playwright(url, timeout * 3)
                if html and len(html) > 1000:
                    _cache_js_domain(url)
                    return html, status, rt, {}
                else:
                    print(f"[FETCH] Playwright returned minimal content, falling back to HTTP")
            except Exception as e:
                print(f"[FETCH] Playwright failed: {str(e)[:100]}, falling back to HTTP")
        elif SELENIUM_AVAILABLE:
            try:
                html, status, rt, _ = fetch_html_selenium(url, timeout * 3)
                if html and len(html) > 1000:
                    _cache_js_domain(url)
                    return html, status, rt, {}
                else:
                    print(f"[FETCH] Selenium returned minimal content, falling back to HTTP")
            except Exception as e:
                print(f"[FETCH] Selenium failed: {str(e)[:100]}, falling back to HTTP")
        else:
            print(f"[FETCH] No browser available, using HTTP")

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    # === PHASE 1 SAFETY ADDITION ===
    # ENGINEER-LEVEL FIX: Check Content-Length header before downloading
    try:
        head_response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)
        content_length = head_response.headers.get('Content-Length')
        if content_length and int(content_length) > MAX_HTML_SIZE:
            error_msg = f"HTML too large (header): {content_length} bytes > {MAX_HTML_SIZE} bytes"
            print(f"[SAFETY] {error_msg}")
            raise ValueError(error_msg)
    except requests.RequestException:
        # If HEAD request fails, proceed with full request (will check size after download)
        pass

    # === PHASE 2: HTTP retry with exponential backoff ===
    for attempt in range(3):
        try:
            start_time = time.time()
            res = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            res.raise_for_status()
            html = res.text
            resp_headers = dict(res.headers)
            response_time_ms = int((time.time() - start_time) * 1000)
            
            # === PHASE 1 SAFETY ADDITION ===
            # Large HTML protection
            if len(html.encode('utf-8')) > MAX_HTML_SIZE:
                error_msg = f"HTML too large: {len(html)} bytes > {MAX_HTML_SIZE} bytes"
                print(f"[SAFETY] {error_msg}")
                raise ValueError(error_msg)
            
            # === PHASE 3: JS detection and browser fallback ===
            if needs_js_rendering(html):
                print(f"[FETCH] JS rendering detected for {url}, switching to browser")
                _cache_js_domain(url)
                
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        html, status, rt, _ = fetch_html_playwright(url, timeout * 3)
                        # If browser returns valid HTML, use it
                        if html and len(html) > 1000:
                            return html, status, rt, resp_headers
                        else:
                            # Browser returned minimal content, fall back to HTTP
                            print(f"[FETCH] Browser returned minimal content ({len(html)} chars), using HTTP HTML")
                            return html, res.status_code, response_time_ms, resp_headers
                    except Exception as playwright_err:
                        print(f"[FETCH] Playwright failed: {str(playwright_err)[:100]}, using HTTP HTML")
                        return html, res.status_code, response_time_ms, resp_headers
                elif SELENIUM_AVAILABLE:
                    try:
                        html, status, rt, _ = fetch_html_selenium(url, timeout * 3)
                        if html and len(html) > 1000:
                            return html, status, rt, resp_headers
                        else:
                            print(f"[FETCH] Selenium returned minimal content, using HTTP HTML")
                            return html, res.status_code, response_time_ms, resp_headers
                    except Exception as selenium_err:
                        print(f"[FETCH] Selenium failed: {str(selenium_err)[:100]}, using HTTP HTML")
                        return html, res.status_code, response_time_ms, resp_headers
                else:
                    print(f"[FETCH] No browser available, using HTTP-only HTML")

            return html, res.status_code, response_time_ms, resp_headers
            
        except requests.RequestException as e:
            print(f"[FETCH] HTTP attempt {attempt + 1} failed for {url}: {str(e)[:100]}")
            
            if attempt == 2:  # Final attempt - use browser fallback
                print(f"[FETCH] All HTTP attempts failed, using browser fallback for {url}")
                _cache_js_domain(url)
                
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        html, status, response_time_ms, _ = fetch_html_playwright(url, timeout * 3)
                        return html, status, response_time_ms, {}
                    except Exception as playwright_err:
                        print(f"[FETCH] Playwright fallback failed: {str(playwright_err)[:100]}")
                        if SELENIUM_AVAILABLE:
                            html, status, response_time_ms, _ = fetch_html_selenium(url, timeout * 3)
                            return html, status, response_time_ms, {}
                        raise
                elif SELENIUM_AVAILABLE:
                    html, status, response_time_ms, _ = fetch_html_selenium(url, timeout * 3)
                    return html, status, response_time_ms, {}
                else:
                    raise RuntimeError(f"All fetch methods failed for {url}: {str(e)}")
            else:
                # Exponential backoff: 0.5s, 1s, 2s
                backoff_time = 0.5 * (2 ** attempt)
                print(f"[FETCH] Retrying in {backoff_time}s after attempt {attempt + 1} failure")
                time.sleep(backoff_time)
                continue
