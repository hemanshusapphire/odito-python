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
    """Determine if page needs JavaScript rendering."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ")
    words = re.findall(r"\b\w+\b", text)

    # Rule: Only use Selenium if BOTH conditions are met:
    # 1. Word count < 50 (very light content)
    # AND
    # 2. React/Next markers found
    has_js_markers = bool(soup.find(id="root") or soup.find(id="__next") or soup.find(id="app"))
    
    return len(words) < 50 and has_js_markers


def fetch_html_selenium(url: str, timeout: int = 30) -> tuple[str, int, int, dict]:
    """Fetch HTML using Selenium for JavaScript rendering."""
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
            driver.quit()
        # Release semaphore for next Selenium session
        if SELENIUM_SEMAPHORE:
            SELENIUM_SEMAPHORE.release()


def fetch_html(url: str, timeout: int = 8) -> tuple[str, int, int, dict]:
    """Primary HTML fetching with JS detection and Selenium fallback - SAFE VERSION
    Returns: (html, status_code, response_time_ms, response_headers)"""
    
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

    if SELENIUM_AVAILABLE and _is_js_cached(url):
        html, status, rt, _ = fetch_html_selenium(url, timeout * 3)
        return html, status, rt, {}

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html"
    }

            # === PHASE 1 SAFETY ADDITION ===
    # ENGINEER-LEVEL FIX: Check Content-Length header before downloading
    try:
        head_response = requests.head(url, headers=headers, timeout=5)
        content_length = head_response.headers.get('Content-Length')
        if content_length and int(content_length) > MAX_HTML_SIZE:
            error_msg = f"HTML too large (header): {content_length} bytes > {MAX_HTML_SIZE} bytes"
            print(f"[SAFETY] {error_msg}")
            raise ValueError(error_msg)
    except requests.RequestException:
        # If HEAD request fails, proceed with full request (will check size after download)
        pass

    # === PHASE 1 SAFETY ADDITION ===
    # HTTP retry with exponential backoff
    for attempt in range(3):
        try:
            start_time = time.time()
            res = requests.get(url, headers=headers, timeout=timeout)
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
            

            if SELENIUM_AVAILABLE and needs_js_rendering(html):
                html, status, _, _ = fetch_html_selenium(url, timeout * 3)
                _cache_js_domain(url)
                return html, status, response_time_ms, resp_headers

            return html, res.status_code, response_time_ms, resp_headers
            
        except requests.RequestException as e:
            if attempt == 2:  # Final attempt
                if SELENIUM_AVAILABLE:
                    html, status, response_time_ms, _ = fetch_html_selenium(url, timeout * 3)
                    _cache_js_domain(url)
                    return html, status, response_time_ms, {}
                raise
            else:
                # Exponential backoff: 0.5s, 1s, 2s
                backoff_time = 0.5 * (2 ** attempt)
                print(f"[SAFETY] Retrying in {backoff_time}s after attempt {attempt + 1} failure")
                time.sleep(backoff_time)
                continue
