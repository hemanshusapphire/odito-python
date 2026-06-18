"""
Smart DOM stabilization utilities for Playwright-based workers.

Replaces hardcoded wait_for_timeout() calls with event-driven strategies
that adapt to React, Next.js, Vue, Angular, and Shopify hydration patterns.

Usage:
    from scraper.shared.dom_stability import wait_for_dom_stable, wait_for_critical_hydration

    await wait_for_dom_stable(page, timeout_ms=8000, stable_ms=300)
"""


# ---------------------------------------------------------------------------
# JS Scripts — injected into the page context
# ---------------------------------------------------------------------------

# MutationObserver-based DOM stability detector.
# Resolves when no DOM mutations occur for `stableMs` milliseconds.
# Falls back to timeout to prevent infinite hangs on highly dynamic pages.
DOM_STABILITY_SCRIPT = """
(options) => {
    return new Promise((resolve) => {
        const stableMs = options.stableMs || 300;
        const timeoutMs = options.timeoutMs || 8000;
        let timer = null;
        let mutationCount = 0;
        const startTime = Date.now();

        const observer = new MutationObserver((mutations) => {
            mutationCount += mutations.length;
            // Reset the stability timer on each mutation
            if (timer) clearTimeout(timer);
            timer = setTimeout(() => {
                observer.disconnect();
                resolve({
                    stable: true,
                    mutations: mutationCount,
                    elapsed_ms: Date.now() - startTime,
                    reason: 'dom_stable'
                });
            }, stableMs);
        });

        // Start observing
        observer.observe(document.body || document.documentElement, {
            childList: true,
            subtree: true,
            attributes: false,  // Ignore attribute changes (animations, etc.)
            characterData: false
        });

        // Start initial stability timer (in case DOM is already stable)
        timer = setTimeout(() => {
            observer.disconnect();
            resolve({
                stable: true,
                mutations: mutationCount,
                elapsed_ms: Date.now() - startTime,
                reason: 'initial_stable'
            });
        }, stableMs);

        // Hard timeout fallback
        setTimeout(() => {
            observer.disconnect();
            if (timer) clearTimeout(timer);
            resolve({
                stable: false,
                mutations: mutationCount,
                elapsed_ms: Date.now() - startTime,
                reason: 'timeout'
            });
        }, timeoutMs);
    });
}
"""

# Hydration readiness checker — detects framework-specific hydration signals.
HYDRATION_CHECK_SCRIPT = """
() => {
    const result = {
        readyState: document.readyState,
        hasBody: document.body !== null,
        bodyChildren: document.body ? document.body.children.length : 0,
        bodyTextLength: document.body ? document.body.innerText.length : 0,
        framework: 'unknown',
        hydrated: false
    };

    // React detection
    const reactRoot = document.getElementById('root') || document.getElementById('__next');
    if (reactRoot) {
        result.framework = document.getElementById('__next') ? 'nextjs' : 'react';
        // React 18+ hydration marker
        result.hydrated = reactRoot.hasAttribute('data-reactroot') ||
                         reactRoot._reactRootContainer !== undefined ||
                         reactRoot.children.length > 0;
    }

    // Vue detection
    const vueRoot = document.getElementById('app') || document.getElementById('vue-app');
    if (vueRoot && vueRoot.__vue_app__) {
        result.framework = 'vue';
        result.hydrated = true;
    }

    // Angular detection
    const ngRoot = document.querySelector('[ng-version]') || document.querySelector('app-root');
    if (ngRoot) {
        result.framework = 'angular';
        result.hydrated = ngRoot.children.length > 0;
    }

    // Shopify detection
    if (window.Shopify || document.querySelector('script[src*="shopify"]')) {
        result.framework = 'shopify';
        result.hydrated = document.body.children.length > 3;
    }

    // Generic hydration: body has meaningful content
    if (!result.hydrated) {
        result.hydrated = result.bodyTextLength > 100 && result.bodyChildren > 2;
    }

    return result;
}
"""

# Tabbable element detector — used by hybrid keyboard navigation.
TABBABLE_ELEMENTS_SCRIPT = """
() => {
    const FOCUSABLE_SELECTOR = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled]):not([type="hidden"])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
        '[contenteditable="true"]'
    ].join(',');

    const elements = Array.from(document.querySelectorAll(FOCUSABLE_SELECTOR));

    // Filter to visible, non-zero-size elements
    const visible = elements.filter(el => {
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
            return false;
        }
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    });

    return {
        total_focusable: elements.length,
        visible_focusable: visible.length,
        elements: visible.slice(0, 30).map(el => ({
            tag: el.tagName.toLowerCase(),
            id: el.id || '',
            type: el.type || '',
            role: el.getAttribute('role') || '',
            tabindex: el.getAttribute('tabindex') || '',
            text: (el.textContent || '').trim().substring(0, 50)
        }))
    };
}
"""

# Resource blocking patterns — URLs/domains to intercept and abort.
# These are ads, trackers, analytics, and heavy media that don't affect
# accessibility scanning or SEO extraction.
BLOCKED_RESOURCE_PATTERNS = [
    # Analytics & tracking
    "google-analytics.com",
    "googletagmanager.com",
    "gtag/js",
    "analytics.",
    "hotjar.com",
    "hj.hotjar.com",
    "mixpanel.com",
    "segment.com",
    "segment.io",
    "amplitude.com",
    "fullstory.com",
    "clarity.ms",
    "mouseflow.com",
    "crazyegg.com",
    "optimizely.com",
    "newrelic.com",
    "nr-data.net",
    "sentry.io",
    "bugsnag.com",

    # Advertising
    "doubleclick.net",
    "googlesyndication.com",
    "googleadservices.com",
    "adservice.google.com",
    "facebook.net/en_US/fbevents",
    "connect.facebook.net",
    "ads-twitter.com",
    "ads.linkedin.com",
    "adsrvr.org",
    "criteo.com",
    "taboola.com",
    "outbrain.com",

    # Heatmaps & session recording
    "luckyorange.com",
    "inspectlet.com",
    "logrocket.com",

    # Chat widgets (non-essential for auditing)
    "intercom.io",
    "drift.com",
    "tawk.to",
    "livechatinc.com",
    "crisp.chat",
    "zendesk.com/embeddable",
]

# Resource types that are safe to block for accessibility/SEO scanning
BLOCKED_RESOURCE_TYPES = frozenset([
    "media",       # Video/audio elements
    "websocket",   # WebSocket connections (analytics, chat)
])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def wait_for_dom_stable(page, timeout_ms: int = 8000, stable_ms: int = 300) -> dict:
    """Wait for DOM to stabilize using MutationObserver.

    Replaces hardcoded wait_for_timeout() with event-driven detection.
    Works with React, Next.js, Vue, Angular, Shopify hydration.

    Args:
        page: Playwright page object
        timeout_ms: Maximum wait time (default: 8000ms)
        stable_ms: Duration of no-mutations to consider stable (default: 300ms)

    Returns:
        dict with {stable: bool, mutations: int, elapsed_ms: int, reason: str}
    """
    try:
        result = await page.evaluate(
            DOM_STABILITY_SCRIPT,
            {"stableMs": stable_ms, "timeoutMs": timeout_ms}
        )
        return result
    except Exception as e:
        # If script fails (e.g., page crashed), return graceful fallback
        return {
            "stable": False,
            "mutations": 0,
            "elapsed_ms": 0,
            "reason": f"error: {str(e)[:100]}"
        }


async def wait_for_critical_hydration(page, timeout_ms: int = 5000) -> dict:
    """Check if the page has completed framework hydration.

    Uses framework-specific detection (React, Next.js, Vue, Angular, Shopify)
    with a polling fallback for SSR/static sites.

    Args:
        page: Playwright page object
        timeout_ms: Maximum wait time (default: 5000ms)

    Returns:
        dict with {readyState, hasBody, bodyChildren, framework, hydrated}
    """
    import time

    start = time.perf_counter()
    poll_interval_ms = 200
    max_polls = timeout_ms // poll_interval_ms

    for _ in range(max_polls):
        try:
            result = await page.evaluate(HYDRATION_CHECK_SCRIPT)
            if result.get("hydrated", False):
                return result
        except Exception:
            pass

        elapsed = (time.perf_counter() - start) * 1000
        if elapsed >= timeout_ms:
            break

        await page.wait_for_timeout(poll_interval_ms)

    # Final check
    try:
        return await page.evaluate(HYDRATION_CHECK_SCRIPT)
    except Exception as e:
        return {
            "readyState": "unknown",
            "hasBody": False,
            "bodyChildren": 0,
            "framework": "unknown",
            "hydrated": False,
            "error": str(e)[:100]
        }


async def detect_tabbable_elements(page) -> dict:
    """Detect all visible, focusable elements on the page.

    Used by the hybrid keyboard navigation strategy to determine
    how many Tab presses are actually needed.

    Returns:
        dict with {total_focusable, visible_focusable, elements: [...]}
    """
    try:
        return await page.evaluate(TABBABLE_ELEMENTS_SCRIPT)
    except Exception as e:
        return {
            "total_focusable": 0,
            "visible_focusable": 0,
            "elements": [],
            "error": str(e)[:100]
        }


def should_block_resource(url: str, resource_type: str) -> bool:
    """Determine if a resource request should be blocked.

    Used with Playwright's page.route() for request interception.
    Blocks ads, trackers, analytics, and heavy media while preserving
    resources needed for accessibility, rendering, and SEO extraction.

    Args:
        url: Request URL
        resource_type: Playwright resource type (script, image, stylesheet, etc.)

    Returns:
        True if the resource should be blocked
    """
    # Block heavy resource types unconditionally
    if resource_type in BLOCKED_RESOURCE_TYPES:
        return True

    # Check URL against blocked patterns
    url_lower = url.lower()
    for pattern in BLOCKED_RESOURCE_PATTERNS:
        if pattern in url_lower:
            return True

    return False


async def setup_resource_blocking(page):
    """Configure Playwright page to block unnecessary resources.

    Call this after page creation but before navigation.

    Args:
        page: Playwright page object
    """
    async def handle_route(route):
        request = route.request
        if should_block_resource(request.url, request.resource_type):
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", handle_route)
