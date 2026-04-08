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
                    tags: v.tags
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
    return {
        tag: el.tagName.toLowerCase(),
        id: el.id || '',
        className: (el.className && typeof el.className === 'string') ? el.className.substring(0, 100) : '',
        focused: true,
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        outlineStyle: styles.outlineStyle || 'none',
        outlineWidth: styles.outlineWidth || '0px',
        outlineColor: styles.outlineColor || ''
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
        "missing_focus_outline": missing_focus_outline,
        "total_tab_presses": TAB_COUNT,
        "focus_order": [
            {"tag": f.get("tag"), "id": f.get("id", "")}
            for f in focus_order if f.get("focused", False)
        ]
    }


async def _scan_single_url(browser, url, semaphore, timeout_ms=30000):
    """
    Scan a single URL in its own browser context.
    Returns structured result dict.
    """
    async with semaphore:
        context = None
        page = None
        result = {
            "url": url,
            "render_status": "failed",
            "axeViolations": [],
            "domMetrics": {},
            "error": None,
            "scannedAt": datetime.now(timezone.utc).isoformat()
        }

        try:
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                ignore_https_errors=True
            )
            page = await context.new_page()

            # Navigate
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            status_code = response.status if response else 0

            # Wait briefly for JS rendering
            await page.wait_for_timeout(2000)

            # Inject axe-core
            try:
                axe_script = requests.get(AXE_CDN_URL, timeout=10).text
                await page.evaluate(axe_script)
            except Exception as axe_load_err:
                print(f"  ⚠️ axe-core CDN load failed for {url}: {axe_load_err}")
                # Try to continue without axe — still collect DOM metrics
                result["error"] = f"axe-core load failed: {str(axe_load_err)}"

            # Run axe-core
            try:
                axe_results = await page.evaluate(AXE_RUN_SCRIPT)
                result["axeViolations"] = axe_results.get("violations", [])
                result["axeViolationCount"] = axe_results.get("violationCount", 0)
                result["axePassedCount"] = axe_results.get("passedCount", 0)
            except Exception as axe_err:
                print(f"  ⚠️ axe-core run failed for {url}: {axe_err}")
                result["axeViolations"] = []
                if not result["error"]:
                    result["error"] = f"axe-core run failed: {str(axe_err)}"

            # Collect DOM metrics
            try:
                dom_metrics = await page.evaluate(DOM_METRICS_SCRIPT)
                result["domMetrics"] = dom_metrics
            except Exception as dom_err:
                print(f"  ⚠️ DOM metrics failed for {url}: {dom_err}")
                result["domMetrics"] = {}

            # --- Feature 4: Keyboard Accessibility Simulation ---
            try:
                keyboard_result = await _simulate_keyboard_navigation(page)
                result["keyboard_analysis"] = keyboard_result
            except Exception as kb_err:
                print(f"  ⚠️ Keyboard navigation failed for {url}: {kb_err}")
                result["keyboard_analysis"] = {
                    "keyboard_navigation_checked": False,
                    "error": str(kb_err)
                }

            result["render_status"] = "success"
            result["statusCode"] = status_code

        except Exception as e:
            result["render_status"] = "failed"
            result["error"] = str(e)
            print(f"  ❌ Scan failed for {url}: {e}")

        finally:
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
                "--disable-gpu"
            ]
        )
        print(f"[HEADLESS_A11Y] Browser launched | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")

        try:
            # Process URLs in batches with semaphore-limited concurrency
            tasks = [
                _scan_single_url(browser, url, semaphore)
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
        store_response = requests.post(store_url, json=store_payload, timeout=30)
        
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
        # CRITICAL: Mark job as FAILED if storage fails
        return {
            "status": "failed",
            "jobId": job_id,
            "error": f"Storage failed: {str(store_err)}",
            "totalUrls": total,
            "successCount": 0,
            "failedCount": total
        }

    return {
        "totalUrls": total,
        "successCount": success_count,
        "failedCount": failed_count,
        "results": all_results
    }


def execute_headless_accessibility(job):
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
            requests.post(complete_url, json={"stats": {"totalUrls": 0, "successCount": 0, "failedCount": 0}}, timeout=10)
            print(f"[HEADLESS_A11Y] Completion endpoint called | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        except Exception:
            pass
        return {"status": "completed", "jobId": job_id, "totalUrls": 0}

    try:
        print(f"[HEADLESS_A11Y] Starting async scan | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        # Run the async scan
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            scan_results = loop.run_until_complete(
                _run_accessibility_scan(job_id, project_id, urls, node_backend_url)
            )
            print(f"[HEADLESS_A11Y] Async scan completed | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        finally:
            loop.close()

        # Report job completion
        stats = {
            "totalUrls": scan_results["totalUrls"],
            "successCount": scan_results["successCount"],
            "failedCount": scan_results["failedCount"]
        }

        try:
            print(f"[HEADLESS_A11Y] Calling completion endpoint | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            complete_response = requests.post(
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
                requests.post(
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

    except Exception as e:
        print(f"❌ [HEADLESS_A11Y] Worker failed | jobId={job_id} | error={str(e)} | timestamp={datetime.now(timezone.utc).isoformat()}")
        traceback.print_exc()

        # Report failure to Node.js
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            requests.post(
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
