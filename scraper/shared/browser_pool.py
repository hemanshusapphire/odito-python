"""Persistent, thread-safe Playwright browser pool.

Why this exists
---------------
The previous implementation launched a brand-new Chromium browser (a full OS
process cold start) for *every* page that needed JS rendering, then tore it down.
Across a 25-page discovery run this added 25+ browser launches.

The Playwright **sync** API is thread-affine: a ``Browser`` launched on one
thread cannot be driven from another thread. Discovery fetches pages with a
``ThreadPoolExecutor``, so we cannot share a single global browser across all
worker threads. Instead we keep **one persistent browser per worker thread**
(thread-local) and reuse it for every page that thread handles, creating only a
lightweight ``BrowserContext`` + ``Page`` per fetch.

This reduces browser launches from O(pages) to O(threads) per job while staying
sync-safe, and guarantees cleanup of pages/contexts/browsers (no leaks).
"""

from __future__ import annotations

import threading
import time
import random
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:  # pragma: no cover - environment dependent
    PLAYWRIGHT_AVAILABLE = False

from config.config import USER_AGENTS

# Chromium hardening / container-safety flags (unchanged from previous impl).
_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
]


class _ThreadBrowser:
    """A persistent browser bound to a single worker thread."""

    __slots__ = ("playwright", "browser", "pages_served", "created_at")

    def __init__(self, playwright, browser):
        self.playwright = playwright
        self.browser = browser
        self.pages_served = 0
        self.created_at = time.time()


class BrowserPool:
    """Manages one persistent browser per thread with lifecycle limits.

    A browser is recycled when it (a) exceeds ``max_pages_per_browser``,
    (b) exceeds ``max_browser_lifetime`` seconds, or (c) becomes disconnected
    (crash recovery). All live browsers are tracked in a registry so the worker
    can close everything on shutdown.
    """

    def __init__(self, max_pages_per_browser: int = 60, max_browser_lifetime: float = 600.0):
        self._local = threading.local()
        self._registry: list[_ThreadBrowser] = []
        self._registry_lock = threading.Lock()
        self.max_pages_per_browser = max_pages_per_browser
        self.max_browser_lifetime = max_browser_lifetime

    # ------------------------------------------------------------------ #
    # internal lifecycle
    # ------------------------------------------------------------------ #
    def _new_browser(self) -> _ThreadBrowser:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
        tb = _ThreadBrowser(pw, browser)
        with self._registry_lock:
            self._registry.append(tb)
        logger.info("[BROWSER_POOL] Launched browser for thread=%s (live=%d)",
                    threading.current_thread().name, len(self._registry))
        return tb

    def _is_stale(self, tb: _ThreadBrowser) -> bool:
        if (time.time() - tb.created_at) > self.max_browser_lifetime:
            return True
        if tb.pages_served >= self.max_pages_per_browser:
            return True
        try:
            if not tb.browser.is_connected():
                return True
        except Exception:
            return True
        return False

    def _retire(self, tb: _ThreadBrowser) -> None:
        with self._registry_lock:
            if tb in self._registry:
                self._registry.remove(tb)
        try:
            tb.browser.close()
        except Exception:
            pass
        try:
            tb.playwright.stop()
        except Exception:
            pass
        if getattr(self._local, "tb", None) is tb:
            self._local.tb = None

    def _get_thread_browser(self) -> _ThreadBrowser:
        tb = getattr(self._local, "tb", None)
        if tb is not None and self._is_stale(tb):
            self._retire(tb)
            tb = None
        if tb is None:
            tb = self._new_browser()
            self._local.tb = tb
        return tb

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    @contextmanager
    def page(self, timeout_ms: int, block_resources: bool = True, user_agent: str = None):
        """Yield a fresh Page on this thread's persistent browser.

        The context + page are always closed on exit. If the browser crashed
        mid-flight it is retired so the next call relaunches cleanly.
        """
        tb = self._get_thread_browser()
        context = None
        page = None
        try:
            context = tb.browser.new_context(
                user_agent=user_agent if user_agent else random.choice(USER_AGENTS),
                viewport={"width": 1366, "height": 768},
                ignore_https_errors=True,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)

            if block_resources:
                try:
                    from .dom_stability import should_block_resource
                    page.route("**/*", lambda route: (
                        route.abort()
                        if should_block_resource(route.request.url, route.request.resource_type)
                        else route.continue_()
                    ))
                except Exception:
                    pass  # Non-critical: render without resource blocking

            tb.pages_served += 1
            yield page
        except Exception:
            # A failure here may indicate a dead browser — retire it so the
            # thread relaunches a healthy one next time (crash recovery).
            self._retire(tb)
            raise
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass

    def shutdown(self) -> None:
        """Close every live browser. Safe to call multiple times."""
        with self._registry_lock:
            registry = list(self._registry)
            self._registry.clear()
        for tb in registry:
            try:
                tb.browser.close()
            except Exception:
                pass
            try:
                tb.playwright.stop()
            except Exception:
                pass
        logger.info("[BROWSER_POOL] Shutdown complete (closed %d browsers)", len(registry))


_pool_singleton: BrowserPool | None = None
_pool_lock = threading.Lock()


def get_browser_pool() -> BrowserPool:
    """Return the process-wide browser pool (lazy singleton)."""
    global _pool_singleton
    if _pool_singleton is None:
        with _pool_lock:
            if _pool_singleton is None:
                _pool_singleton = BrowserPool()
    return _pool_singleton


def shutdown_browser_pool() -> None:
    """Close all browsers and reset the singleton (called on worker shutdown)."""
    global _pool_singleton
    if _pool_singleton is not None:
        _pool_singleton.shutdown()
        _pool_singleton = None
