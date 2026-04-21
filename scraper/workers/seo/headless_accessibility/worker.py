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
import time
from bson.objectid import ObjectId

# Import database collections
from db import seo_page_data
from scraper.shared.url_selector import get_top_urls
from scraper.shared.http_client import get_http_client


# ---------------------------------------------------------------------------
# axe-core injection script (minified CDN fallback)
# ---------------------------------------------------------------------------
AXE_CDN_URL = "https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.9.1/axe.min.js"

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
    Simulate 20 Tab presses using Playwright's native keyboard API
    and collect focus metrics after each press.

    Detects:
      - Focus traps (same element focused >= 3 consecutive times)
      - Small click targets (width or height < 24px)
      - Missing focus outline (outline-style is 'none' or outline-width is '0px')
      - Unreachable elements (focus never moves from body)
    """
    TAB_COUNT = 20
    focus_order = []
    small_click_targets = 0
    small_click_targets_list = []
    missing_focus_outline = 0
    focus_trap_detected = False
    unreachable_count = 0

    for i in range(TAB_COUNT):
        await page.keyboard.press("Tab")
        # Brief pause for focus to settle
        await page.wait_for_timeout(100)

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
        "focus_order": [
            {"tag": f.get("tag"), "id": f.get("id", ""), "selector": f.get("selector", "")}
            for f in focus_order if f.get("focused", False)
        ]
    }


async def _scan_single_url(browser, url, semaphore, timeout_ms=30000):
    """
    Scan a single URL in its own browser context with retry mechanism.
    Returns structured result dict with comprehensive error handling.
    """
    async with semaphore:
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

        # Retry up to 3 times for robustness
        MAX_RETRIES = 3
        NAVIGATION_TIMEOUT = 90000  # 90s
        SELECTOR_TIMEOUT = 30000    # 30s

        for attempt in range(1, MAX_RETRIES + 1):
            result["attempts"] = attempt
            attempt_start_time = time.time()
            context = None
            page = None

            print(f"  🔄 Attempt {attempt}/{MAX_RETRIES} for {url} | timestamp={datetime.now(timezone.utc).isoformat()}")

            try:
                # Create browser context with realistic user agent and anti-bot settings
                context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    ignore_https_errors=True,
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    locale="en-US",
                    timezone_id="America/New_York"
                )
                page = await context.new_page()

                # Set default timeout for all operations
                page.set_default_timeout(SELECTOR_TIMEOUT)

                # Navigate with domcontentloaded (more reliable than networkidle for SPAs)
                try:
                    response = await asyncio.wait_for(
                        page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT),
                        timeout=NAVIGATION_TIMEOUT / 1000 + 5  # Add buffer
                    )
                    status_code = response.status if response else 0
                    result["statusCode"] = status_code
                    load_time = time.time() - attempt_start_time
                    print(f"  ✅ Navigation successful for {url} | status={status_code} | loadTime={load_time:.2f}s")
                except asyncio.TimeoutError:
                    load_time = time.time() - attempt_start_time
                    print(f"  ⚠️ Navigation timeout for {url} | attempt={attempt} | loadTime={load_time:.2f}s")
                    result["error"] = f"Navigation timeout (90s) on attempt {attempt}"
                    if attempt < MAX_RETRIES:
                        print(f"  🔄 Retrying {url} after timeout...")
                        continue
                    raise
                except Exception as nav_err:
                    load_time = time.time() - attempt_start_time
                    print(f"  ⚠️ Navigation failed for {url} | attempt={attempt} | error={nav_err} | loadTime={load_time:.2f}s")
                    result["error"] = f"Navigation failed on attempt {attempt}: {str(nav_err)}"
                    if attempt < MAX_RETRIES:
                        print(f"  🔄 Retrying {url} after navigation error...")
                        continue
                    raise

                # Wait for SPA hydration (JS rendering)
                await page.wait_for_timeout(5000)  # 5s for SPA hydration

                # Verify DOM is ready - check document.readyState and body content
                try:
                    dom_ready = await page.evaluate("""
                        () => {
                            return {
                                readyState: document.readyState,
                                hasBody: document.body !== null,
                                bodyChildren: document.body ? document.body.children.length : 0
                            };
                        }
                    """)
                    print(f"  📊 DOM state for {url} | readyState={dom_ready['readyState']} | hasBody={dom_ready['hasBody']} | bodyChildren={dom_ready['bodyChildren']}")

                    # If document is not complete or body is empty, wait longer
                    if dom_ready['readyState'] != 'complete' or dom_ready['bodyChildren'] == 0:
                        print(f"  ⏳ Waiting for full DOM load for {url}...")
                        await page.wait_for_timeout(3000)  # Additional 3s wait

                        # Try optional networkidle as fallback for SPAs
                        try:
                            await page.wait_for_load_state('networkidle', timeout=10000)
                            print(f"  ✅ Networkidle achieved for {url}")
                        except Exception:
                            print(f"  ℹ️ Networkidle timeout (non-critical) for {url}")
                except Exception as dom_check_err:
                    print(f"  ⚠️ DOM check failed for {url}: {dom_check_err}")
                    # Continue anyway, DOM might still be usable

                # Inject axe-core with retry logic
                axe_loaded = False
                for axe_attempt in range(2):
                    try:
                        http_client = get_http_client()
                        axe_response = await http_client.get(AXE_CDN_URL, timeout=15)
                        axe_script = axe_response.text
                        await page.evaluate(axe_script)
                        axe_loaded = True
                        print(f"  ✅ axe-core loaded for {url} | attempt={axe_attempt + 1}")
                        break
                    except Exception as axe_load_err:
                        print(f"  ⚠️ axe-core CDN load attempt {axe_attempt + 1} failed for {url}: {axe_load_err}")
                        if axe_attempt == 1:
                            result["error"] = f"axe-core load failed: {str(axe_load_err)}"

                if not axe_loaded:
                    print(f"  ❌ axe-core failed to load for {url} after retries")
                    if attempt < MAX_RETRIES:
                        print(f"  🔄 Retrying {url} after axe-core load failure...")
                        continue
                    raise Exception("axe-core failed to load after retries")

                # Run axe-core (only after DOM is confirmed ready)
                try:
                    axe_results = await page.evaluate(AXE_RUN_SCRIPT)
                    result["axeViolations"] = axe_results.get("violations", [])
                    result["axeViolationCount"] = axe_results.get("violationCount", 0)
                    result["axePassedCount"] = axe_results.get("passedCount", 0)
                    print(f"  ✅ axe-core executed for {url} | violations={result['axeViolationCount']}")
                except Exception as axe_err:
                    print(f"  ⚠️ axe-core run failed for {url}: {axe_err}")
                    result["axeViolations"] = []
                    if not result["error"]:
                        result["error"] = f"axe-core run failed: {str(axe_err)}"
                    if attempt < MAX_RETRIES:
                        print(f"  🔄 Retrying {url} after axe-core run failure...")
                        continue
                    raise

                # Collect DOM metrics
                try:
                    dom_metrics = await page.evaluate(DOM_METRICS_SCRIPT)
                    result["domMetrics"] = dom_metrics
                    print(f"  ✅ DOM metrics collected for {url} | totalElements={dom_metrics.get('totalElements', 0)}")
                except Exception as dom_err:
                    print(f"  ⚠️ DOM metrics failed for {url}: {dom_err}")
                    result["domMetrics"] = {}

                # Collect meta viewport
                try:
                    viewport_data = await page.evaluate(META_VIEWPORT_SCRIPT)
                    result["viewportMeta"] = viewport_data
                except Exception as viewport_err:
                    print(f"  ⚠️ Viewport meta failed for {url}: {viewport_err}")
                    result["viewportMeta"] = {}

                # Feature 4: Keyboard Accessibility Simulation
                try:
                    keyboard_result = await asyncio.wait_for(
                        _simulate_keyboard_navigation(page),
                        timeout=30
                    )
                    result["keyboard_analysis"] = keyboard_result
                    print(f"  ✅ Keyboard navigation completed for {url}")
                except asyncio.TimeoutError:
                    print(f"  ⚠️ Keyboard navigation timeout for {url}")
                    result["keyboard_analysis"] = {
                        "keyboard_navigation_checked": False,
                        "error": "Keyboard navigation timeout (30s)"
                    }
                except Exception as kb_err:
                    print(f"  ⚠️ Keyboard navigation failed for {url}: {kb_err}")
                    result["keyboard_analysis"] = {
                        "keyboard_navigation_checked": False,
                        "error": str(kb_err)
                    }

                # Success! Mark as successful and break retry loop
                result["render_status"] = "success"
                total_load_time = time.time() - attempt_start_time
                result["loadTime"] = round(total_load_time, 2)
                print(f"  ✅ Scan successful for {url} | attempt={attempt} | totalLoadTime={total_load_time:.2f}s | status={status_code}")
                break

            except Exception as e:
                total_load_time = time.time() - attempt_start_time
                print(f"  ❌ Attempt {attempt} failed for {url} | error={e} | loadTime={total_load_time:.2f}s")
                result["error"] = str(e)

                # Clean up on failure
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

                # If this was the last attempt, mark as failed
                if attempt == MAX_RETRIES:
                    result["render_status"] = "failed"
                    result["loadTime"] = round(total_load_time, 2)
                    print(f"  ❌ All retries exhausted for {url} | finalStatus=failed | error={e}")
                else:
                    print(f"  🔄 Retrying {url} (attempt {attempt + 1}/{MAX_RETRIES})...")

            finally:
                # Clean up resources on success too
                if result["render_status"] == "success":
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
    """
    from playwright.async_api import async_playwright

    semaphore = asyncio.Semaphore(3)  # Max 3 concurrent pages
    all_results = []
    total = len(urls)

    print(f"[HEADLESS_A11Y] Starting scan | jobId={job_id} | urls={total} | timestamp={datetime.now(timezone.utc).isoformat()}")

    print(f"[HEADLESS_A11Y] Browser launching | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
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
                "--window-size=1366,768"
            ]
        )
        print(f"[HEADLESS_A11Y] Browser launched | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")

        try:
            # Process URLs in batches with semaphore-limited concurrency
            # Wrap each task with asyncio.wait_for to prevent indefinite hangs
            # Increased timeout to 300s (5 min) to account for 3 retry attempts
            tasks = [
                asyncio.wait_for(_scan_single_url(browser, url, semaphore), timeout=300)
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
        print(f"❌ [HEADLESS_A11Y] CRITICAL: Failed to store results | jobId={job_id} | error={store_err} | timestamp={datetime.now(timezone.utc).isoformat()}")
        # CRITICAL: Raise exception to ensure completion API is called in execute_headless_accessibility
        raise Exception(f"Storage failed: {str(store_err)}")

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

    # STEP 2: Get URLs using deterministic type-based selection
    urls = []
    
    # First try: Use deterministic type-based selection from seo_internal_links
    try:
        print(f"[HEADLESS_A11Y] Using deterministic type-based URL selection | projectId={project_id} | jobId={job_id}")
        
        # Use the shared URL selector for consistent results
        urls = get_top_urls(project_id, limit=25)
        
        print(f"[HEADLESS_A11Y] Deterministic selection complete | totalUrls={len(urls)} | jobId={job_id}")
        
    except Exception as selection_error:
        print(f"[HEADLESS_A11Y] Deterministic selection failed, falling back to scraped pages | jobId={job_id} | error={str(selection_error)}")
        
        # Fallback to original logic
        project_id_obj = ObjectId(project_id)
        print(f"[HEADLESS_A11Y] Fetching URLs from seo_page_data | projectId={project_id} | jobId={job_id}")
        
        # Query seo_page_data collection for successfully scraped pages
        pages = list(seo_page_data.find({
            "projectId": project_id_obj,
            "extraction_status": "SUCCESS"  # Only process successfully scraped pages
        }))
        
        # Extract URLs from page data
        urls = [page["url"] for page in pages if page.get("url")]
        
        print(f"[HEADLESS_A11Y] seo_page_data fetch complete | totalUrls={len(urls)} | jobId={job_id} | pagesFound={len(pages)}")
        
        # If still no URLs, try seo_internal_links fallback
        if not urls:
            try:
                print(f"[HEADLESS_A11Y] No scraped pages found, trying seo_internal_links | projectId={project_id} | jobId={job_id}")
                
                # Import internal links collection
                from db import seo_internal_links
                
                # Query by projectId to get all discovered internal links for this project
                internal_links = list(seo_internal_links.find({"projectId": project_id_obj}))
                urls = [link["url"] for link in internal_links if link.get("url")]
                
                print(f"[HEADLESS_A11Y] seo_internal_links fallback complete | totalUrls={len(urls)} | jobId={job_id} | linksFound={len(internal_links)}")
                
            except Exception as fallback_error:
                print(f"[HEADLESS_A11Y] seo_internal_links fallback failed | jobId={job_id} | error={str(fallback_error)}")

    # Third try: Use URLs from job input if provided (legacy support)
    if not urls and hasattr(job, 'urls') and job.urls:
        urls = job.urls
        print(f"[HEADLESS_A11Y] Using URLs from job input | totalUrls={len(urls)} | jobId={job_id}")

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
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": str(e)
        }
