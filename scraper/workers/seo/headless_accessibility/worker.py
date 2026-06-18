"""
HEADLESS_ACCESSIBILITY worker — Playwright-based accessibility scanning.

For each URL fetched from database:
  1. Render page in a headless Chromium browser
  2. Inject and run axe-core for accessibility violations
  3. Collect basic DOM metrics (element count, heading structure)
  4. Store structured results in seo_headless_data collection

Single browser instance per job, concurrency limited to 3 pages at a time.
Individual URL failures do NOT fail the entire job.
Self-sufficient: Fetches URLs directly from seo_page_data collection.
"""

import os
import asyncio
import requests
import traceback
from datetime import datetime, timezone
from pathlib import Path
import time
from bson.objectid import ObjectId

# Import database collections
from db import seo_page_data
from scraper.shared.url_selector import get_top_urls
from scraper.shared.http_client import get_http_client
from scraper.shared.perf_tracker import PerformanceTracker, WorkerTimingContext
from scraper.shared.dom_stability import (
    wait_for_dom_stable,
    wait_for_critical_hydration,
    detect_tabbable_elements,
    setup_resource_blocking,
)


# ---------------------------------------------------------------------------
# axe-core — local bundled version (eliminates CDN dependency)
# ---------------------------------------------------------------------------
AXE_CDN_URL = "https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.1/axe.min.js"  # Fallback only
_AXE_LOCAL_PATH = Path(__file__).resolve().parent.parent.parent.parent / "shared" / "vendor" / "axe.min.js"
_AXE_SCRIPT_CACHE = None
ENABLE_RESOURCE_BLOCKING = os.environ.get("BLOCK_HEAVY_RESOURCES", "1") == "1"


def _load_axe_script() -> str:
    """Load axe-core script from local vendor bundle (cached after first load)."""
    global _AXE_SCRIPT_CACHE
    if _AXE_SCRIPT_CACHE is not None:
        return _AXE_SCRIPT_CACHE
    if _AXE_LOCAL_PATH.exists():
        _AXE_SCRIPT_CACHE = _AXE_LOCAL_PATH.read_text(encoding="utf-8")
        print(f"[HEADLESS_A11Y] axe-core loaded from local bundle ({len(_AXE_SCRIPT_CACHE)} bytes)")
    else:
        print(f"[HEADLESS_A11Y] WARNING: Local axe.min.js not found at {_AXE_LOCAL_PATH}, will use CDN fallback")
        _AXE_SCRIPT_CACHE = ""
    return _AXE_SCRIPT_CACHE


# ---------------------------------------------------------------------------
# Smart failure classification — prevents wasted retries
# ---------------------------------------------------------------------------
def _classify_error(error) -> dict:
    """Classify an error and return retry strategy.

    Returns dict with:
        max_retries: int — how many retries are appropriate
        delay_s: float — delay before retry
        category: str — error classification
    """
    err_str = str(error).lower()

    # DNS / connection failures — no retry
    if any(s in err_str for s in ["dns", "name or service not known", "nodename nor servname",
                                    "getaddrinfo", "err_name_not_resolved"]):
        return {"max_retries": 0, "delay_s": 0, "category": "DNS_FAILURE"}

    if any(s in err_str for s in ["connection refused", "err_connection_refused"]):
        return {"max_retries": 0, "delay_s": 0, "category": "CONNECTION_REFUSED"}

    # HTTP 403/429 — limited retry
    if any(s in err_str for s in ["403", "forbidden"]):
        return {"max_retries": 1, "delay_s": 2.0, "category": "HTTP_FORBIDDEN"}

    if "429" in err_str or "rate limit" in err_str:
        return {"max_retries": 1, "delay_s": 3.0, "category": "RATE_LIMITED"}

    # SSL errors — no retry
    if any(s in err_str for s in ["ssl", "certificate", "err_cert"]):
        return {"max_retries": 0, "delay_s": 0, "category": "SSL_ERROR"}

    # Timeout — one retry
    if any(s in err_str for s in ["timeout", "timed out"]):
        return {"max_retries": 1, "delay_s": 1.0, "category": "TIMEOUT"}

    # Browser crash — retry
    if any(s in err_str for s in ["browser", "crashed", "target closed", "context"]):
        return {"max_retries": 2, "delay_s": 1.0, "category": "BROWSER_CRASH"}

    # Default: transient — retry twice
    return {"max_retries": 2, "delay_s": 0.5, "category": "TRANSIENT"}

AXE_RUN_SCRIPT = """
() => {
    return new Promise((resolve, reject) => {
        if (typeof axe === 'undefined') {
            reject(new Error('axe-core not loaded'));
            return;
        }
        axe.run(document, {
            runOnly: ['wcag2a', 'wcag2aa', 'best-practice'],
            resultTypes: ['violations']
        }).then(results => {
            resolve({
                violations: results.violations.map(v => ({
                    id: v.id,
                    impact: v.impact,
                    description: v.description,
                    helpUrl: v.helpUrl,
                    nodes: v.nodes.length,
                    tags: v.tags,
                    nodeDetails: v.nodes.slice(0, 5).map(n => ({
                        target: n.target || [],
                        html: n.html || ''
                    }))
                })),
                violationCount: results.violations.length,
                passedCount: results.passes ? results.passes.length : 0
            });
        }).catch(reject);
    });
}
"""

DOM_METRICS_SCRIPT = """
() => {
    const headings = {};
    ['h1','h2','h3','h4','h5','h6'].forEach(tag => {
        headings[tag] = document.querySelectorAll(tag).length;
    });
    return {
        totalElements: document.querySelectorAll('*').length,
        headings: headings,
        images: document.querySelectorAll('img').length,
        imagesWithoutAlt: document.querySelectorAll('img:not([alt])').length,
        links: document.querySelectorAll('a').length,
        forms: document.querySelectorAll('form').length,
        inputs: document.querySelectorAll('input, textarea, select').length,
        buttons: document.querySelectorAll('button, [role="button"]').length,
        ariaLandmarks: document.querySelectorAll('[role="banner"],[role="navigation"],[role="main"],[role="contentinfo"],[role="complementary"],[role="search"]').length,
        ariaLandmarksDetail: {
            main: document.querySelectorAll('main, [role="main"]').length,
            nav: document.querySelectorAll('nav, [role="navigation"]').length,
            header: document.querySelectorAll('header, [role="banner"]').length,
            footer: document.querySelectorAll('footer, [role="contentinfo"]').length,
            aside: document.querySelectorAll('aside, [role="complementary"]').length,
            search: document.querySelectorAll('[role="search"]').length
        },
        title: document.title || '',
        lang: document.documentElement.lang || ''
    };
}
"""


# ---------------------------------------------------------------------------
# Feature 4 — Keyboard Accessibility (Rules 227-242)
# JS script to collect focus state AFTER a real Playwright Tab press
# ---------------------------------------------------------------------------
KEYBOARD_FOCUS_COLLECTOR = """
() => {
    const el = document.activeElement;
    if (!el || el === document.body) {
        return { tag: 'body', focused: false };
    }
    const rect = el.getBoundingClientRect();
    const styles = window.getComputedStyle(el);
    
    // Generate CSS selector for element reference
    let selector = el.tagName.toLowerCase();
    if (el.id) {
        selector += '#' + el.id;
    } else if (el.className && typeof el.className === 'string' && el.className) {
        selector += '.' + el.className.split(' ').filter(c => c).join('.');
    }
    
    return {
        tag: el.tagName.toLowerCase(),
        id: el.id || '',
        className: (el.className && typeof el.className === 'string') ? el.className.substring(0, 100) : '',
        selector: selector,
        focused: true,
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        outlineStyle: styles.outlineStyle || 'none',
        outlineWidth: styles.outlineWidth || '0px',
        outlineColor: styles.outlineColor || ''
    };
}
"""


META_VIEWPORT_SCRIPT = """
() => {
    const viewportMeta = document.querySelector('meta[name="viewport"]');
    return {
        viewportContent: viewportMeta ? viewportMeta.getAttribute('content') : null
    };
}
"""


async def _simulate_keyboard_navigation(page) -> dict:
    """
    HYBRID keyboard accessibility simulation.

    Phase 1: Detect all focusable elements via JS (instant, no key presses).
    Phase 2: Simulate min(detected_count, 10) Tab presses with Playwright's
             native keyboard API for real focus behavior verification.

    Detects:
      - Focus traps (same element focused >= 3 consecutive times)
      - Small click targets (width or height < 24px)
      - Missing focus outline (outline-style is 'none' or outline-width is '0px')
      - Unreachable elements (focus never moves from body)
    """
    # Phase 1: JS-based tabbable element detection
    try:
        tabbable_info = await detect_tabbable_elements(page)
        detected_count = tabbable_info.get("visible_focusable", 0)
    except Exception:
        tabbable_info = {"visible_focusable": 0, "elements": []}
        detected_count = 0

    # Phase 2: Strategic Tab simulation — min(detected, 10) presses
    TAB_COUNT = min(max(detected_count, 5), 10)  # At least 5, at most 10
    focus_order = []
    small_click_targets = 0
    small_click_targets_list = []
    missing_focus_outline = 0
    focus_trap_detected = False
    unreachable_count = 0

    for i in range(TAB_COUNT):
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(50)  # 50ms focus settle (down from 100ms)

        try:
            focus_info = await page.evaluate(KEYBOARD_FOCUS_COLLECTOR)
        except Exception:
            focus_info = {"tag": "unknown", "focused": False}

        focus_order.append(focus_info)

        if not focus_info.get("focused", False):
            unreachable_count += 1
            continue

        # Check small click target
        w = focus_info.get("width", 0)
        h = focus_info.get("height", 0)
        if (w > 0 and w < 24) or (h > 0 and h < 24):
            small_click_targets += 1
            small_click_targets_list.append({
                "selector": focus_info.get("selector", ""),
                "width": w,
                "height": h
            })

        # Check missing focus outline
        outline_style = focus_info.get("outlineStyle", "none")
        outline_width = focus_info.get("outlineWidth", "0px")
        if outline_style == "none" or outline_width == "0px":
            missing_focus_outline += 1

    # Detect focus trap: same element focused >= 3 consecutive times
    if len(focus_order) >= 3:
        consecutive = 1
        for i in range(1, len(focus_order)):
            prev_id = (focus_order[i - 1].get("tag", "") + focus_order[i - 1].get("id", ""))
            curr_id = (focus_order[i].get("tag", "") + focus_order[i].get("id", ""))
            if prev_id == curr_id and focus_order[i].get("focused", False):
                consecutive += 1
                if consecutive >= 3:
                    focus_trap_detected = True
                    break
            else:
                consecutive = 1

    return {
        "keyboard_navigation_checked": True,
        "focus_trap_detected": focus_trap_detected,
        "unreachable_elements": unreachable_count,
        "small_click_targets": small_click_targets,
        "small_click_targets_list": small_click_targets_list,
        "missing_focus_outline": missing_focus_outline,
        "total_tab_presses": TAB_COUNT,
        "detected_focusable_elements": detected_count,
        "focus_order": [
            {"tag": f.get("tag"), "id": f.get("id", ""), "selector": f.get("selector", "")}
            for f in focus_order if f.get("focused", False)
        ]
    }


async def _scan_single_url(browser, url, semaphore, perf_ctx=None, timeout_ms=30000):
    """
    Scan a single URL in its own browser context.

    Optimizations vs original:
      - Local axe-core injection (no CDN fetch)
      - MutationObserver-based DOM stabilization (no hardcoded waits)
      - Resource blocking for ads/trackers/analytics
      - Smart failure classification (fewer wasted retries)
      - Per-URL performance profiling
    """
    async with semaphore:
        tracker = perf_ctx.url_tracker(url) if perf_ctx else PerformanceTracker("A11Y_URL", url=url)

        result = {
            "url": url,
            "render_status": "failed",
            "axeViolations": [],
            "domMetrics": {},
            "error": None,
            "scannedAt": datetime.now(timezone.utc).isoformat(),
            "attempts": 0,
            "statusCode": None
        }

        NAVIGATION_TIMEOUT = 60000  # 60s (down from 90s — smart waits compensate)
        SELECTOR_TIMEOUT = 20000    # 20s (down from 30s)
        max_retries = 2             # Default; overridden by error classification
        first_error_classification = None

        for attempt in range(1, max_retries + 1):
            result["attempts"] = attempt
            context = None
            page = None

            try:
                # --- Context creation ---
                with tracker.stage("context_creation"):
                    context = await browser.new_context(
                        viewport={"width": 1366, "height": 768},
                        ignore_https_errors=True,
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        locale="en-US",
                        timezone_id="America/New_York"
                    )
                    page = await context.new_page()
                    page.set_default_timeout(SELECTOR_TIMEOUT)

                # --- Resource blocking ---
                if ENABLE_RESOURCE_BLOCKING:
                    with tracker.stage("resource_blocking"):
                        await setup_resource_blocking(page)

                # --- Navigation ---
                with tracker.stage("navigation"):
                    try:
                        response = await asyncio.wait_for(
                            page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT),
                            timeout=NAVIGATION_TIMEOUT / 1000 + 5
                        )
                        status_code = response.status if response else 0
                        result["statusCode"] = status_code
                    except asyncio.TimeoutError:
                        result["error"] = f"Navigation timeout on attempt {attempt}"
                        raise
                    except Exception as nav_err:
                        result["error"] = f"Navigation failed: {str(nav_err)}"
                        raise

                # --- Smart DOM stabilization (replaces hardcoded 5s + 3s waits) ---
                with tracker.stage("dom_stabilization"):
                    # First: wait for framework hydration
                    hydration = await wait_for_critical_hydration(page, timeout_ms=4000)

                    # Then: wait for DOM mutations to settle
                    stability = await wait_for_dom_stable(page, timeout_ms=6000, stable_ms=300)

                # --- Inject axe-core (local bundle, no CDN) ---
                with tracker.stage("axe_injection"):
                    axe_script = _load_axe_script()
                    axe_loaded = False

                    if axe_script:
                        # Local injection — fast path
                        try:
                            await page.evaluate(axe_script)
                            axe_loaded = True
                        except Exception as local_err:
                            print(f"  ⚠️ Local axe injection failed for {url}: {local_err}")

                    if not axe_loaded:
                        # CDN fallback (preserves original behavior)
                        try:
                            http_client = get_http_client()
                            axe_response = await http_client.get(AXE_CDN_URL, timeout=10)
                            await page.evaluate(axe_response.text)
                            axe_loaded = True
                        except Exception as cdn_err:
                            result["error"] = f"axe-core load failed: {str(cdn_err)}"
                            raise Exception("axe-core failed to load (local + CDN)")

                # --- Run axe-core ---
                with tracker.stage("axe_execution"):
                    try:
                        axe_results = await page.evaluate(AXE_RUN_SCRIPT)
                        result["axeViolations"] = axe_results.get("violations", [])
                        result["axeViolationCount"] = axe_results.get("violationCount", 0)
                        result["axePassedCount"] = axe_results.get("passedCount", 0)
                    except Exception as axe_err:
                        result["axeViolations"] = []
                        if not result["error"]:
                            result["error"] = f"axe-core run failed: {str(axe_err)}"
                        raise

                # --- Collect DOM metrics ---
                with tracker.stage("dom_metrics"):
                    try:
                        dom_metrics = await page.evaluate(DOM_METRICS_SCRIPT)
                        result["domMetrics"] = dom_metrics
                    except Exception:
                        result["domMetrics"] = {}

                # --- Collect meta viewport ---
                try:
                    viewport_data = await page.evaluate(META_VIEWPORT_SCRIPT)
                    result["viewportMeta"] = viewport_data
                except Exception:
                    result["viewportMeta"] = {}

                # --- Keyboard accessibility (hybrid approach) ---
                with tracker.stage("keyboard_navigation"):
                    try:
                        keyboard_result = await asyncio.wait_for(
                            _simulate_keyboard_navigation(page),
                            timeout=15  # Down from 30s — fewer tabs now
                        )
                        result["keyboard_analysis"] = keyboard_result
                    except asyncio.TimeoutError:
                        result["keyboard_analysis"] = {
                            "keyboard_navigation_checked": False,
                            "error": "Keyboard navigation timeout (15s)"
                        }
                    except Exception as kb_err:
                        result["keyboard_analysis"] = {
                            "keyboard_navigation_checked": False,
                            "error": str(kb_err)
                        }

                # --- Success ---
                result["render_status"] = "success"
                result["loadTime"] = round(tracker.get_total_ms() / 1000, 2)
                tracker.log_summary(prefix="A11Y_URL")
                if perf_ctx:
                    perf_ctx.complete_url(tracker)
                break

            except Exception as e:
                result["error"] = str(e)

                # Smart failure classification — decide retry strategy
                if first_error_classification is None:
                    first_error_classification = _classify_error(e)
                    max_retries = first_error_classification["max_retries"] + 1  # +1 because attempt starts at 1
                    delay = first_error_classification["delay_s"]
                    category = first_error_classification["category"]
                    print(f"  ⚠️ Error classified as {category} for {url} | maxRetries={max_retries-1}")

                    if delay > 0 and attempt < max_retries:
                        await asyncio.sleep(delay)

                if attempt >= max_retries:
                    result["render_status"] = "failed"
                    result["loadTime"] = round(tracker.get_total_ms() / 1000, 2)
                    result["error_category"] = first_error_classification["category"] if first_error_classification else "UNKNOWN"
                    if perf_ctx:
                        perf_ctx.complete_url(tracker)

            finally:
                # Always clean up browser resources
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass
                if context:
                    try:
                        await context.close()
                    except Exception:
                        pass

        return result


async def _run_accessibility_scan(job_id, project_id, urls, node_backend_url):
    """
    Core async logic: launch browser, scan all URLs, store results.
    Includes performance profiling and optimized browser configuration.
    """
    from playwright.async_api import async_playwright

    semaphore = asyncio.Semaphore(3)  # Max 3 concurrent pages
    all_results = []
    total = len(urls)

    # Performance profiling context for the entire job
    perf_ctx = WorkerTimingContext("HEADLESS_A11Y", job_id=job_id)
    job_tracker = PerformanceTracker("HEADLESS_A11Y", job_id=job_id)

    # Pre-load axe-core script (cached for all URLs)
    _load_axe_script()

    print(f"[HEADLESS_A11Y] Starting scan | jobId={job_id} | urls={total}")

    with job_tracker.stage("browser_launch"):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-site-isolation-trials",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--js-flags=--max-old-space-size=256",
                    "--window-size=1366,768"
                ]
            )

            try:
                # Process URLs with semaphore-limited concurrency
                # Reduced timeout: smart retries + no CDN = faster per-URL
                with job_tracker.stage("url_scanning"):
                    tasks = [
                        asyncio.wait_for(
                            _scan_single_url(browser, url, semaphore, perf_ctx=perf_ctx),
                            timeout=180  # 3min (down from 5min — fewer retries now)
                        )
                        for url in urls
                    ]
                    all_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Convert exceptions to error results
                processed_results = []
                for i, res in enumerate(all_results):
                    if isinstance(res, Exception):
                        processed_results.append({
                            "url": urls[i],
                            "render_status": "failed",
                            "axeViolations": [],
                            "domMetrics": {},
                            "error": str(res),
                            "scannedAt": datetime.now(timezone.utc).isoformat()
                        })
                    else:
                        processed_results.append(res)

                all_results = processed_results

            finally:
                await browser.close()

    # Store results in MongoDB via Node.js API
    success_count = sum(1 for r in all_results if r["render_status"] == "success")
    failed_count = total - success_count

    print(f"[HEADLESS_A11Y] Scan complete | success={success_count} | failed={failed_count} | timestamp={datetime.now(timezone.utc).isoformat()}")

    # Store each result via the backend API
    print(f"[HEADLESS_A11Y] Starting DB store | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
    try:
        store_url = f"{node_backend_url}/api/jobs/headless-accessibility-report"
        store_payload = {
            "projectId": project_id,
            "seo_jobId": job_id,
            "results": all_results
        }
        print(f"[HEADLESS_A11Y] Posting to storage endpoint | url={store_url} | resultsCount={len(all_results)}")
        http_client = get_http_client()
        store_response = await http_client.post(store_url, json=store_payload, timeout=30)
        
        # Validate response status
        if store_response.status_code == 200:
            response_data = store_response.json()
            inserted_count = response_data.get('data', {}).get('insertedCount', len(all_results))
            print(f"[HEADLESS_A11Y] Storage response status: 200 | InsertedCount: {inserted_count} | timestamp={datetime.now(timezone.utc).isoformat()}")
            print(f"[HEADLESS_A11Y] Results stored successfully | projectId={project_id} | count={len(all_results)} | response={response_data.get('message', 'OK')} | timestamp={datetime.now(timezone.utc).isoformat()}")
        else:
            raise Exception(f"HTTP {store_response.status_code}: {store_response.text}")
            
    except Exception as store_err:
        print(f"❌ [HEADLESS_A11Y] CRITICAL: Failed to store results | jobId={job_id} | error={store_err}")
        # CRITICAL: Raise exception to ensure completion API is called in execute_headless_accessibility
        raise Exception(f"Storage failed: {str(store_err)}")

    # Log job-level performance summary
    perf_ctx.log_job_summary()

    return {
        "totalUrls": total,
        "successCount": success_count,
        "failedCount": failed_count,
        "results": all_results
    }


async def execute_headless_accessibility(job):
    """
    Execute HEADLESS_ACCESSIBILITY job.

    Args:
        job: Pydantic model with jobId, projectId, userId, sourceJobId

    Returns:
        dict with status and results
    """
    job_id = job.jobId
    project_id = job.projectId
    # Validate required environment variables
    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        raise Exception("NODE_BACKEND_URL is required")

    print(f"[HEADLESS_A11Y] Starting | jobId={job_id} | projectId={project_id} | timestamp={datetime.now(timezone.utc).isoformat()}")

    # STEP 2: Resolve URL list.
    # Priority: canonical_urls from URL_QUALIFICATION → legacy job.urls → DB fallback.
    # canonical_urls guarantees the same URL set as PAGE_SCRAPING (parallel worker).
    urls = []

    if getattr(job, 'canonical_urls', None):
        urls = list(job.canonical_urls)
        print(f"[HEADLESS_A11Y] Using canonical_urls from URL_QUALIFICATION | totalUrls={len(urls)} | jobId={job_id}")
    elif getattr(job, 'urls', None):
        urls = list(job.urls)
        print(f"[HEADLESS_A11Y] Using urls from job input (legacy) | totalUrls={len(urls)} | jobId={job_id}")
    else:
        # DB fallback: deterministic type-based selection (pre-URL_QUALIFICATION deployments)
        try:
            urls = get_top_urls(project_id, limit=25)
            print(f"[HEADLESS_A11Y] DB fallback URL selection | totalUrls={len(urls)} | jobId={job_id}")
        except Exception as exc:
            print(f"[HEADLESS_A11Y] DB fallback failed | jobId={job_id} | error={exc}")

    # STEP 3: Safety guard - graceful handling if no URLs found
    if not urls:
        print(f"[HEADLESS_A11Y] No URLs found in database | jobId={job_id} | projectId={project_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        # Report completion with empty results (graceful, not failure)
        try:
            print(f"[HEADLESS_A11Y] Calling completion endpoint | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            http_client = get_http_client()
            await http_client.post(complete_url, json={"stats": {"totalUrls": 0, "successCount": 0, "failedCount": 0}}, timeout=10)
            print(f"[HEADLESS_A11Y] Completion endpoint called | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        except Exception:
            pass
        return {"status": "completed", "jobId": job_id, "totalUrls": 0}

    try:
        print(f"[HEADLESS_A11Y] Starting async scan | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        # Run the async scan with global timeout to prevent indefinite hangs
        # Increased to 1800s (30 min) to account for retry mechanism and slower pages
        scan_results = await asyncio.wait_for(
            _run_accessibility_scan(job_id, project_id, urls, node_backend_url),
            timeout=1800
        )
        print(f"[HEADLESS_A11Y] Async scan completed | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")

        # Report job completion
        stats = {
            "totalUrls": scan_results["totalUrls"],
            "successCount": scan_results["successCount"],
            "failedCount": scan_results["failedCount"]
        }

        try:
            print(f"[HEADLESS_A11Y] Calling completion endpoint | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            http_client = get_http_client()
            complete_response = await http_client.post(
                complete_url,
                json={"stats": stats},
                timeout=10
            )
            complete_response.raise_for_status()
            print(f"[HEADLESS_A11Y] Job completion reported | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        except Exception as complete_error:
            print(f"⚠️ [HEADLESS_A11Y] Failed to report completion | jobId={job_id} | error={str(complete_error)} | timestamp={datetime.now(timezone.utc).isoformat()}")
            # Try to report failure
            try:
                fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
                http_client = get_http_client()
                await http_client.post(
                    fail_url,
                    json={"error": f"Completion reporting failed: {str(complete_error)}", "stats": stats},
                    timeout=10
                )
            except Exception:
                pass

        print(f"[HEADLESS_A11Y] Completed | jobId={job_id} | success={stats['successCount']} | failed={stats['failedCount']} | timestamp={datetime.now(timezone.utc).isoformat()}")

        return {
            "status": "completed",
            "jobId": job_id,
            **stats
        }

    except asyncio.TimeoutError:
        print(f"❌ [HEADLESS_A11Y] Global timeout exceeded (1800s) | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        # Report failure to Node.js
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            http_client = get_http_client()
            await http_client.post(
                fail_url,
                json={"error": "Global timeout exceeded (30 minutes)", "stats": {"totalUrls": len(urls), "successCount": 0, "failedCount": len(urls)}},
                timeout=10
            )
            print(f"[HEADLESS_A11Y] Timeout failure reported | jobId={job_id}")
        except Exception as fail_error:
            print(f"⚠️ [HEADLESS_A11Y] Failed to report timeout failure | error={str(fail_error)}")
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": "Global timeout exceeded (30 minutes)"
        }

    except Exception as e:
        print(f"❌ [HEADLESS_A11Y] Worker failed | jobId={job_id} | error={str(e)} | timestamp={datetime.now(timezone.utc).isoformat()}")
        traceback.print_exc()

        # Report failure to Node.js
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            http_client = get_http_client()
            await http_client.post(
                fail_url,
                json={"error": str(e), "stats": {}},
                timeout=10
            )
            print(f"[HEADLESS_A11Y] Failure reported | jobId={job_id}")
        except Exception as fail_error:
            print(f"⚠️ [HEADLESS_A11Y] Failed to report failure | error={str(fail_error)}")

        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": str(e)
        }
