"""HTTP fetching and Selenium rendering functionality."""

import os
import re
import random
import threading
import logging
import warnings
from collections import OrderedDict
from datetime import datetime
import time

# === PHASE 1 SAFETY ADDITION ===
# Large HTML protection constant
MAX_HTML_SIZE_MB = 5

# Third-party imports
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover
    Retry = None
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

# ---------------------------------------------------------------------------
# Pooled HTTP session (connection + TLS reuse).
# A single module-level Session with a sized connection pool eliminates the
# new-TCP+TLS-handshake-per-request cost of bare requests.get/head. The urllib3
# pool backing the adapter is thread-safe, so the Session is safe to share
# across the discovery ThreadPoolExecutor workers.
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    session = requests.Session()
    retry = None
    if Retry is not None:
        retry = Retry(
            total=1,
            connect=1,
            read=1,
            backoff_factor=0.3,
            status_forcelist=(502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
        )
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=50,
        max_retries=retry if retry is not None else 1,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_session = _build_session()

# Minimum number of anchors a static HTML page must contain before we trust it
# for link discovery without escalating to a (costly) browser render.
MIN_STATIC_ANCHORS = 5

# ---------------------------------------------------------------------------
# Per-URL rendering-decision cache.
#
# Deliberately keyed by the exact URL, never by domain: one page's rendering
# requirement must never be assumed for another page on the same site (a real
# site can easily mix static marketing pages with a JS-heavy app section on
# the same domain). Bounded + LRU-evicted so a long-lived worker process
# serving many audits over time cannot grow this without limit. Thread-safe
# via a single lock — reads/writes are O(1) dict operations, negligible next
# to actual page-fetch cost.
# ---------------------------------------------------------------------------
_RENDER_DECISION_CACHE_MAX_SIZE = 5000
_render_decision_cache: "OrderedDict[str, bool]" = OrderedDict()
_render_decision_cache_lock = threading.Lock()

# Fix B: Audit-level stable User-Agent — set once before fan-out, cleared after.
# Module-level so all 8 ThreadPoolExecutor threads read the same value without locking.
_audit_user_agent: str | None = None



def _is_js_cached(url: str) -> bool | None:
    """Return this exact URL's previously-recorded rendering decision.

    Returns None if this URL has never been evaluated before — callers must
    treat None as "unknown", not as False.
    """
    with _render_decision_cache_lock:
        if url in _render_decision_cache:
            _render_decision_cache.move_to_end(url)
            return _render_decision_cache[url]
        return None


def _cache_js_domain(url: str, needs_js: bool = True):
    """Record this exact URL's rendering decision (page-specific, bounded, LRU-evicted).

    Kept under the historical name for callers already using it, but the
    cache key is now the URL itself, not its domain — see module docstring
    above for why domain-wide caching was removed.
    """
    with _render_decision_cache_lock:
        _render_decision_cache[url] = needs_js
        _render_decision_cache.move_to_end(url)
        if len(_render_decision_cache) > _RENDER_DECISION_CACHE_MAX_SIZE:
            _render_decision_cache.popitem(last=False)


def set_audit_user_agent(ua: str | None) -> None:
    """Set or clear the per-audit stable User-Agent shared across all worker threads."""
    global _audit_user_agent
    _audit_user_agent = ua


def _get_request_ua() -> str:
    """Return the audit-stable UA if one is set, else fall back to a random choice."""
    return _audit_user_agent if _audit_user_agent else random.choice(USER_AGENTS)


# ---------------------------------------------------------------------------
# Public, read-only accessors for internal fetch state.
#
# Added so callers outside this module (e.g. PAGE_SCRAPING's cloaking-detection
# feature) can reuse the same per-URL JS-rendering knowledge, connection
# pool, and stable User-Agent that fetch_html() already maintains, instead of
# duplicating a fresh HTTP session / UA-selection / cache of their own. These
# are pure wrappers — zero new logic, no change to fetch_html()'s behavior or
# return signature.
# ---------------------------------------------------------------------------
def is_js_rendering_domain(url: str) -> bool:
    """Was this exact URL previously determined (this process lifetime) to
    require JS rendering? Thin public wrapper around the internal per-URL
    cache used by fetch_html()'s static-first escalation logic.

    Despite the name (kept for backward compatibility with existing callers),
    this is a per-URL, not per-domain, decision — see the cache's own
    docstring above.
    """
    return _is_js_cached(url) is True


def get_request_user_agent() -> str:
    """Public accessor for the audit-stable User-Agent (falls back to random)."""
    return _get_request_ua()


def get_pooled_session() -> requests.Session:
    """Public accessor for the shared, connection-pooled requests.Session."""
    return _session


def probe_rendering_need(url: str, timeout: int = 10) -> bool:
    """Pre-probe whether this specific URL requires JS rendering.

    Called by PAGE_SCRAPING before its ThreadPoolExecutor starts, against a
    handful of that chunk's own URLs, purely as a warm-up: it pre-populates
    this one URL's entry in the per-URL rendering-decision cache so that if
    the same exact URL is fetched again later in the run, the redundant
    detection work (and, if applicable, a wasted static-HTTP attempt before
    falling back to Playwright) is skipped. It intentionally has no effect on
    any other URL, including other pages on the same domain — see the cache's
    own docstring for why domain-wide propagation was removed.

    Args:
        url: The specific URL to probe.
        timeout: HTTP timeout in seconds (kept short — probe is best-effort)

    Returns:
        True if this URL requires JS rendering, False if static HTML is
        sufficient. Failures are swallowed silently; worst case the per-page
        logic in fetch_html() runs the detection again on the real fetch.
    """
    cached = _is_js_cached(url)
    if cached is not None:
        return cached  # already decided for this exact URL this process lifetime

    try:
        headers = {
            "User-Agent": _get_request_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        res = _session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        html = res.text
        soup = BeautifulSoup(html, "lxml")

        if needs_js_rendering(html, soup=soup):
            _cache_js_domain(url, needs_js=True)
            print(f"[RENDER] probe: JS rendering required for {url!r} — cached for this URL")
            return True

        _cache_js_domain(url, needs_js=False)
        print(f"[RENDER] probe: static HTML sufficient for {url!r}")
        return False

    except Exception as e:
        print(f"[RENDER] probe: failed for {url!r} ({e!s:.80}) — per-page detection unchanged")
        return False


def _has_spa_framework_fingerprint(html_lower: str, soup: "BeautifulSoup") -> bool:
    """Look for genuine SPA hydration/framework fingerprints -- not generic
    container ids. A bare ``id="app"`` or ``id="root"`` div is a common,
    framework-agnostic layout convention used by plenty of static sites and
    page builders; on its own it is not evidence of any JS framework, so it
    is deliberately NOT checked here. Every check below is a marker that a
    real framework's own runtime writes into the page, and that ordinary
    prose/static HTML has no reason to ever contain.
    """
    # React: hydration attributes React itself writes onto rendered DOM nodes.
    if "data-reactroot" in html_lower or "data-reactid" in html_lower:
        return True

    # Next.js: the canonical hydration payload script Next.js always emits.
    if soup.find("script", id=re.compile(r"^__next_data__$", re.IGNORECASE)):
        return True
    if "/_next/static/" in html_lower:
        return True

    # Vue: SSR marker, or the data-v-XXXXXXXX scoped-style attribute every
    # compiled Vue single-file component carries on its rendered elements.
    if 'data-server-rendered="true"' in html_lower:
        return True
    if re.search(r"data-v-[0-9a-f]{6,}", html_lower):
        return True

    # Angular: the ng-version attribute Angular CLI apps always render on
    # their root component, or the legacy AngularJS 1.x ng-app directive --
    # both require the "=" immediately after so a coincidental substring in
    # unrelated text/URLs (e.g. "tracking-apps") can never match.
    if re.search(r"\bng-version\s*=", html_lower) or re.search(r"\bng-app\s*=", html_lower):
        return True

    # Gatsby: its own, near-unique root container id.
    if soup.find(id="___gatsby"):
        return True

    # Nuxt: its own root container id, or the runtime data global it sets.
    if soup.find(id="__nuxt") or "window.__nuxt__" in html_lower:
        return True

    return False


def _has_shopify_fingerprint(html_lower: str, soup: "BeautifulSoup") -> bool:
    """Look for genuine Shopify platform signals -- never a bare "shopify"
    text match, which matches equally well when a page's own prose simply
    *mentions* Shopify (e.g. an article titled "Shopify SEO" or a sentence
    listing well-known e-commerce brands). Every check below is something
    only an actual Shopify-hosted storefront's theme engine emits.
    """
    # Shopify's own asset CDN -- only ever appears in <script>/<link> src
    # attributes on a real Shopify storefront, never in ordinary body text.
    if "cdn.shopify.com" in html_lower or "cdn.shopifycdn.net" in html_lower:
        return True

    # The global `Shopify` JS object every Shopify theme initializes.
    if "window.shopify" in html_lower or re.search(r"\bshopify\s*=\s*shopify\s*\|\|", html_lower):
        return True

    # Shopify-specific meta tags (checkout API token, digital wallet, etc.)
    # and the occasional explicit generator meta tag.
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or "").strip().lower()
        if name.startswith("shopify-"):
            return True
        if name == "generator" and "shopify" in (meta.get("content") or "").lower():
            return True

    return False


def needs_js_rendering(html: str, soup: "BeautifulSoup | None" = None) -> bool:
    """Determine if page needs JavaScript rendering.

    Enhanced detection for:
    - React/Next/Vue/Angular/Gatsby/Nuxt SPA frameworks (genuine fingerprints only)
    - Shopify storefronts (genuine platform signals only)
    - Client-side rendered content
    - Minimal server-side content

    Accepts an optional pre-parsed ``soup`` so callers that already parsed the
    HTML do not pay for a second BeautifulSoup parse (single-pass).

    Deliberately page-specific: this function only ever inspects the HTML it
    is given and returns a decision for that one page. Callers are
    responsible for not generalizing the result to any other URL (see the
    per-URL rendering-decision cache above).
    """
    if soup is None:
        soup = BeautifulSoup(html, "lxml")

    html_lower = html.lower()

    # Condition 1: Very light visible content (likely client-rendered).
    visible_text = soup.get_text(strip=True)
    is_light_content = len(visible_text) < 600

    # Condition 2: genuine SPA framework fingerprints (not generic container ids)
    has_js_markers = _has_spa_framework_fingerprint(html_lower, soup)

    # Condition 3: genuine Shopify platform signals (not a text mention of the word)
    has_shopify_markers = _has_shopify_fingerprint(html_lower, soup)

    # Condition 4: Missing critical content despite having HTML structure
    has_title = bool(soup.find("title"))
    is_suspicious = has_title and len(visible_text) <= 200

    # Trigger JS rendering if ANY condition is met
    return is_light_content or has_js_markers or has_shopify_markers or is_suspicious


def detect_js_framework_early(html_snippet: str, resp_headers: dict = None) -> str | None:
    """Detect JS framework from response headers or first ~3KB of HTML.

    Returns framework name if detected, None otherwise.
    Used to skip remaining HTTP retries and go straight to Playwright.

    Every pattern below anchors on an actual attribute assignment or a
    near-unique runtime token, never a bare word, so it cannot match generic
    prose, URL slugs, or filenames that merely happen to contain the same
    letters (e.g. "tracking-apps" must never match Angular's "ng-app").
    """
    # Check response headers first (cheapest detection). Headers are set by
    # the server/CDN itself, not page content, so these are already low-risk;
    # scoped to specific known header names/values rather than "any header
    # containing this substring" for extra precision.
    if resp_headers:
        headers_lower = {k.lower(): v.lower() for k, v in resp_headers.items() if isinstance(v, str)}

        # Next.js
        if 'x-powered-by' in headers_lower and 'next.js' in headers_lower['x-powered-by']:
            return 'nextjs'

        # Shopify: its own well-known response headers.
        if 'x-shopid' in headers_lower or 'x-shopify-stage' in headers_lower or 'x-sorting-hat-podid' in headers_lower:
            return 'shopify'

        # Nuxt
        if 'x-powered-by' in headers_lower and 'nuxt' in headers_lower['x-powered-by']:
            return 'nuxt'

    # Check first 3KB of HTML for framework markers (no full parse needed)
    snippet = html_snippet[:3000].lower() if html_snippet else ""

    if 'id="__next_data__"' in snippet or '/_next/static/' in snippet:
        return 'nextjs'
    if 'data-reactroot' in snippet or 'data-reactid' in snippet:
        return 'react'
    if re.search(r"\bng-version\s*=", snippet) or re.search(r"\bng-app\s*=", snippet):
        return 'angular'
    if 'id="__nuxt"' in snippet or 'window.__nuxt__' in snippet:
        return 'nuxt'
    if 'data-server-rendered="true"' in snippet or re.search(r"data-v-[0-9a-f]{6,}", snippet):
        return 'vue'
    if 'id="___gatsby"' in snippet or 'content="gatsby' in snippet:
        return 'gatsby'

    return None


def fetch_html_playwright(url: str, timeout: int = 30, user_agent: str = None) -> tuple[str, int, int, dict, str]:
    """Fetch HTML using Playwright for JavaScript rendering (preferred over Selenium).

    Uses a persistent, thread-local browser from the shared BrowserPool instead
    of launching a fresh Chromium per call. A new lightweight context/page is
    created per fetch and always closed. Readiness is detected via
    ``networkidle`` (with a bounded timeout) rather than a fixed 1.5s sleep.

    Returns: (html, status_code, response_time_ms, response_headers, final_url)
    """
    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright not installed")

    from .browser_pool import get_browser_pool

    # Enforce a ceiling on *concurrent* renders to bound memory/CPU. With the
    # pool this also bounds how many browsers are simultaneously busy.
    if PLAYWRIGHT_SEMAPHORE:
        PLAYWRIGHT_SEMAPHORE.acquire()

    try:
        start_time = time.time()
        timeout_ms = timeout * 1000
        pool = get_browser_pool()

        with pool.page(timeout_ms=timeout_ms, block_resources=True, user_agent=user_agent) as page:
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Intelligent readiness: wait for network to settle, bounded so we
            # never hang on pages with long-polling/analytics. Falls back
            # gracefully if the page never reaches networkidle.
            try:
                page.wait_for_load_state("networkidle", timeout=min(5000, timeout_ms))
            except Exception:
                pass  # Continue with whatever has rendered so far

            html = page.content()
            status_code = response.status if response else 200
            try:
                final_url = page.url or url
            except Exception:
                final_url = url

        response_time_ms = int((time.time() - start_time) * 1000)
        return html, status_code, response_time_ms, {}, final_url

    except Exception as e:
        raise RuntimeError(f"Playwright error: {str(e)}")
    finally:
        if PLAYWRIGHT_SEMAPHORE:
            PLAYWRIGHT_SEMAPHORE.release()


def fetch_html_selenium(url: str, timeout: int = 30) -> tuple[str, int, int, dict, str]:
    """Fetch HTML using Selenium for JavaScript rendering (fallback if Playwright unavailable).

    Returns: (html, status_code, response_time_ms, response_headers, final_url)
    """
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

        try:
            final_url = driver.current_url or url
        except Exception:
            final_url = url
        response_time_ms = int((time.time() - start_time) * 1000)
        return driver.page_source, 200, response_time_ms, {}, final_url

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


def _download_html_streaming(url: str, headers: dict, timeout: int, max_bytes: int):
    """GET a URL with a streaming, size-capped read (no separate HEAD probe).

    Aborts the download as soon as the body exceeds ``max_bytes`` instead of
    paying for a HEAD round-trip up front. Returns the requests.Response (with
    body already consumed) and the decoded HTML string.
    """
    res = _session.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=True)
    try:
        res.raise_for_status()
        total = 0
        chunks = []
        for chunk in res.iter_content(chunk_size=16384):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"HTML too large: >{max_bytes} bytes (aborted mid-stream)")
            chunks.append(chunk)
        raw = b"".join(chunks)
    finally:
        res.close()

    encoding = res.encoding or res.apparent_encoding or "utf-8"
    try:
        html = raw.decode(encoding, errors="replace")
    except (LookupError, TypeError):
        html = raw.decode("utf-8", errors="replace")
    return res, html


def fetch_html(url: str, timeout: int = 8, prefer_static_for_links: bool = False) -> tuple[str, int, int, dict, str]:
    """Primary HTML fetching with JS detection and Playwright/Selenium fallback.

    Hybrid approach:
    1. Try HTTP first (fast, lightweight)
    2. If JS detected or HTTP fails, fallback to Playwright (stable)
    3. If Playwright unavailable, fallback to Selenium (legacy)

    When ``prefer_static_for_links`` is True (used by link discovery), the
    expensive browser render is only triggered if the static HTML yields too
    few anchors to be useful — anchors and sitemap URLs are almost always
    present in server-rendered HTML, so this avoids most browser launches.

    Returns: (html, status_code, response_time_ms, response_headers, final_url)
             final_url is the post-redirect URL (falls back to the input url).
    """

    # Track the final (post-redirect) URL; defaults to the input until a fetch resolves it
    final_url = url

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
    
    # Static-first mode (link discovery) skips the eager browser path; it will
    # escalate to a render later only if the static HTML has too few anchors.
    # _is_js_cached(url) is a per-URL decision (see cache docstring above) --
    # only THIS exact URL's own prior outcome can shortcut straight to a
    # browser render here, never another page's.
    if (is_shopify or _is_js_cached(url) is True) and not prefer_static_for_links:
        if PLAYWRIGHT_AVAILABLE:
            try:
                html, status, rt, _, final_url = fetch_html_playwright(url, timeout * 3, user_agent=_get_request_ua())
                if html and len(html) > 1000:
                    _cache_js_domain(url)
                    return html, status, rt, {}, final_url
                else:
                    print(f"[FETCH] Playwright returned minimal content, falling back to HTTP")
            except Exception as e:
                print(f"[FETCH] Playwright failed: {str(e)[:100]}, falling back to HTTP")
        elif SELENIUM_AVAILABLE:
            try:
                html, status, rt, _, final_url = fetch_html_selenium(url, timeout * 3)
                if html and len(html) > 1000:
                    _cache_js_domain(url)
                    return html, status, rt, {}, final_url
                else:
                    print(f"[FETCH] Selenium returned minimal content, falling back to HTTP")
            except Exception as e:
                print(f"[FETCH] Selenium failed: {str(e)[:100]}, falling back to HTTP")
        else:
            print(f"[FETCH] No browser available, using HTTP")

    headers = {
        "User-Agent": _get_request_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    # === PHASE 2: HTTP retry with exponential backoff ===
    # NOTE: the previous HEAD-before-GET probe was removed. We now stream the GET
    # and abort mid-download if it exceeds MAX_HTML_SIZE, saving one full
    # round-trip (and TLS handshake) per page.
    for attempt in range(2):  # Reduced from 3 to 2 — early detection catches JS sites faster
        try:
            start_time = time.time()
            res, html = _download_html_streaming(url, headers, timeout, MAX_HTML_SIZE)
            final_url = res.url or url  # post-redirect URL from the HTTP response
            resp_headers = dict(res.headers)
            response_time_ms = int((time.time() - start_time) * 1000)

            # Single-pass parse: parse the HTML once and reuse the soup for both
            # JS detection and (in static-first mode) anchor counting.
            soup = BeautifulSoup(html, "lxml")

            # === Static-first short-circuit (link discovery) ===
            # If the server-rendered HTML already contains enough anchors, trust
            # it and skip the browser entirely — even for SPA/Shopify markers.
            if prefer_static_for_links:
                anchor_count = len(soup.find_all("a", href=True))
                if anchor_count >= MIN_STATIC_ANCHORS:
                    return html, res.status_code, response_time_ms, resp_headers, final_url
                print(f"[FETCH] Static HTML had only {anchor_count} anchors for {url}, considering render")

            # === Early framework detection — skip to browser immediately ===
            framework = detect_js_framework_early(html, resp_headers)
            if framework:
                print(f"[FETCH] Framework detected: {framework} for {url}, skipping to browser")
                _cache_js_domain(url)
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        html, status, rt, _, final_url = fetch_html_playwright(url, timeout * 3, user_agent=_get_request_ua())
                        if html and len(html) > 1000:
                            return html, status, rt, resp_headers, final_url
                    except Exception as pw_err:
                        print(f"[FETCH] Playwright failed for {framework} site: {str(pw_err)[:100]}")
                # Fall through to needs_js_rendering check

            # === PHASE 3: JS detection and browser fallback ===
            if needs_js_rendering(html, soup=soup):
                print(f"[FETCH] JS rendering detected for {url}, switching to browser")
                _cache_js_domain(url)
                
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        html, status, rt, _, final_url = fetch_html_playwright(url, timeout * 3, user_agent=_get_request_ua())
                        # If browser returns valid HTML, use it
                        if html and len(html) > 1000:
                            return html, status, rt, resp_headers, final_url
                        else:
                            # Browser returned minimal content, fall back to HTTP
                            print(f"[FETCH] Browser returned minimal content ({len(html)} chars), using HTTP HTML")
                            return html, res.status_code, response_time_ms, resp_headers, final_url
                    except Exception as playwright_err:
                        print(f"[FETCH] Playwright failed: {str(playwright_err)[:100]}, using HTTP HTML")
                        return html, res.status_code, response_time_ms, resp_headers, final_url
                elif SELENIUM_AVAILABLE:
                    try:
                        html, status, rt, _, final_url = fetch_html_selenium(url, timeout * 3)
                        if html and len(html) > 1000:
                            return html, status, rt, resp_headers, final_url
                        else:
                            print(f"[FETCH] Selenium returned minimal content, using HTTP HTML")
                            return html, res.status_code, response_time_ms, resp_headers, final_url
                    except Exception as selenium_err:
                        print(f"[FETCH] Selenium failed: {str(selenium_err)[:100]}, using HTTP HTML")
                        return html, res.status_code, response_time_ms, resp_headers, final_url
                else:
                    print(f"[FETCH] No browser available, using HTTP-only HTML")

            return html, res.status_code, response_time_ms, resp_headers, final_url

        except requests.RequestException as e:
            print(f"[FETCH] HTTP attempt {attempt + 1} failed for {url}: {str(e)[:100]}")
            
            if attempt == 1:  # Final attempt (2 total) - use browser fallback
                print(f"[FETCH] All HTTP attempts failed, using browser fallback for {url}")
                _cache_js_domain(url)
                
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        html, status, response_time_ms, _, final_url = fetch_html_playwright(url, timeout * 3, user_agent=_get_request_ua())
                        return html, status, response_time_ms, {}, final_url
                    except Exception as playwright_err:
                        print(f"[FETCH] Playwright fallback failed: {str(playwright_err)[:100]}")
                        if SELENIUM_AVAILABLE:
                            html, status, response_time_ms, _, final_url = fetch_html_selenium(url, timeout * 3)
                            return html, status, response_time_ms, {}, final_url
                        raise
                elif SELENIUM_AVAILABLE:
                    html, status, response_time_ms, _, final_url = fetch_html_selenium(url, timeout * 3)
                    return html, status, response_time_ms, {}, final_url
                else:
                    raise RuntimeError(f"All fetch methods failed for {url}: {str(e)}")
            else:
                # Single backoff: 0.5s
                backoff_time = 0.5
                print(f"[FETCH] Retrying in {backoff_time}s after attempt {attempt + 1} failure")
                time.sleep(backoff_time)
                continue
