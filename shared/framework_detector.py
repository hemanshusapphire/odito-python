"""
Shared Framework + CMS Detector
================================

Produces structured detection output used by:
  - page_scraping worker  (stores to seo_page_data.page_context)
  - context_enrichment    (orchestrator)
  - backfill utility

Output schema
-------------
  detect_structured() → {
      "name":       "Next.js",   # canonical display name
      "key":        "nextjs",    # lowercase slug used by backend
      "confidence": 95,          # 0-100
      "source":     "html"       # signal that triggered detection
  }

  detect_cms_structured() → {
      "name":       "WordPress",
      "confidence": 98
  }  |  None if not a CMS

Canonical framework keys
------------------------
  nextjs, wordpress, shopify, react, vuejs, nuxtjs, angular,
  gatsby, webflow, wix, generic
"""

from typing import Dict, List, Any, Optional
import re


# Map canonical key → display name
_DISPLAY_NAMES = {
    "nextjs":    "Next.js",
    "wordpress": "WordPress",
    "shopify":   "Shopify",
    "react":     "React",
    "vuejs":     "Vue.js",
    "nuxtjs":    "Nuxt.js",
    "angular":   "Angular",
    "gatsby":    "Gatsby",
    "webflow":   "Webflow",
    "wix":       "Wix",
    "generic":   "Generic HTML",
}

# Frameworks that also function as a CMS (name to expose)
_CMS_FRAMEWORKS = {
    "wordpress": "WordPress",
    "shopify":   "Shopify",
    "webflow":   "Webflow",
    "wix":       "Wix",
}


class FrameworkDetector:
    """
    Unified framework + CMS detection.

    Each signal source produces a (framework_key, confidence, source) tuple.
    The highest-confidence non-generic signal wins.
    """

    CANONICAL_KEYS = list(_DISPLAY_NAMES.keys())

    def __init__(self):
        # ── HTML/source patterns (medium confidence = 70-80) ─────────────────
        # Ordered by specificity — more specific patterns listed first.
        # Each entry: (pattern, framework_key, confidence)
        self._html_patterns: List[tuple] = [
            # Next.js — look for the JSON data island first (definitive)
            (r"__NEXT_DATA__",               "nextjs",    95),
            (r"/_next/static/",              "nextjs",    90),
            (r'id="__next"',                 "nextjs",    88),
            (r"__next",                      "nextjs",    75),
            (r"next\.js",                    "nextjs",    72),

            # Nuxt — must check before Vue to avoid false positives
            (r"__NUXT__",                    "nuxtjs",    95),
            (r"/_nuxt/",                     "nuxtjs",    90),
            (r"nuxt-link",                   "nuxtjs",    75),
            (r"data-n-head",                 "nuxtjs",    80),

            # Vue (after Nuxt — Nuxt is a Vue superset)
            (r'data-v-[0-9a-f]{7,}',        "vuejs",     90),
            (r"vue\.min\.js",                "vuejs",     85),
            (r'id="app"\s+data-server',      "vuejs",     80),

            # Angular
            (r'ng-version="',                "angular",   95),
            (r"\bng-app\b",                  "angular",   88),
            (r"angular\.min\.js",            "angular",   85),
            (r"/assets/main\.[a-f0-9]+\.js", "angular",   70),

            # WordPress
            (r"wp-content/",                 "wordpress", 95),
            (r"wp-includes/",                "wordpress", 95),
            (r"/wp-json/",                   "wordpress", 90),
            (r"wp-embed\.min\.js",           "wordpress", 88),

            # Shopify
            (r"cdn\.shopify\.com",           "shopify",   95),
            (r'class="shopify-section"',     "shopify",   90),
            (r"Shopify\.theme",              "shopify",   90),
            (r"/s/files/1/",                 "shopify",   85),

            # Gatsby
            (r"/page-data/app-data\.json",   "gatsby",    95),
            (r"gatsby-image",                "gatsby",    85),
            (r"___gatsby",                   "gatsby",    90),

            # Webflow
            (r"webflow\.js",                 "webflow",   90),
            (r'data-wf-page="',              "webflow",   95),
            (r"w-webflow-badge",             "webflow",   85),

            # Wix
            (r"static\.wixstatic\.com",      "wix",       95),
            (r"wix-code-sdk",                "wix",       90),

            # React (last — very generic markers; many frameworks use React)
            (r'data-reactroot',              "react",     85),
            (r"react-dom\.min\.js",          "react",     80),
            (r'id="root"',                   "react",     60),
        ]

        # ── Generator meta tag patterns (high confidence = 85-98) ────────────
        self._generator_patterns: List[tuple] = [
            (r"wordpress\s+[\d.]+",   "wordpress", 98),
            (r"wordpress",            "wordpress", 95),
            (r"next\.js",             "nextjs",    95),
            (r"gatsby",               "gatsby",    95),
            (r"webflow",              "webflow",   95),
            (r"wix",                  "wix",       95),
            (r"shopify",              "shopify",   90),
            (r"nuxt",                 "nuxtjs",    90),
        ]

        # ── HTTP response header patterns ────────────────────────────────────
        self._header_patterns: List[tuple] = [
            # header_name_pattern, header_value_pattern, framework_key, confidence
            (r"x-powered-by",    r"next\.js",             "nextjs",    98),
            (r"x-shopify-stage", r"",                     "shopify",   98),
            (r"x-wix-request-id",r"",                     "wix",       95),
            (r"x-powered-by",    r"php",                  "wordpress", 55),  # low; PHP alone is weak
        ]

    # ── Public interface ─────────────────────────────────────────────────────

    def detect_structured(
        self,
        html:      Optional[str] = None,
        generator: Optional[str] = None,
        headers:   Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Detect framework and return structured result.

        Returns:
            { "name": "Next.js", "key": "nextjs", "confidence": 95, "source": "html" }
        """
        candidates: List[tuple] = []  # (key, confidence, source)

        # Generator takes priority (highest reliability)
        if generator:
            result = self._match_generator(generator)
            if result:
                candidates.append(result)

        # HTTP headers (very high confidence when matched)
        if headers:
            result = self._match_headers(headers)
            if result:
                candidates.append(result)

        # HTML source (broad but reliable when specific patterns match)
        if html:
            result = self._match_html(html)
            if result:
                candidates.append(result)

        # Pick highest-confidence non-generic signal
        candidates.sort(key=lambda x: x[1], reverse=True)
        for key, confidence, source in candidates:
            if key != "generic":
                return self._build_result(key, confidence, source)

        return self._build_result("generic", 50, "fallback")

    def detect_cms_structured(
        self,
        framework_result: Optional[Dict[str, Any]] = None,
        html: Optional[str] = None,
        generator: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Detect CMS separately (only returns a value for known CMS platforms).

        A framework detected as a CMS (WordPress, Shopify, Webflow, Wix) is
        promoted to the cms field.  React/Next.js/Vue etc. are frameworks, not CMS.

        Returns:
            { "name": "WordPress", "confidence": 98 }  or  None
        """
        # Use framework result if available
        if framework_result:
            key = framework_result.get("key", "generic")
            if key in _CMS_FRAMEWORKS:
                return {
                    "name":       _CMS_FRAMEWORKS[key],
                    "confidence": framework_result.get("confidence", 90),
                }

        # Standalone CMS detection (when called without framework result)
        if html or generator:
            fw = self.detect_structured(html=html, generator=generator)
            key = fw.get("key", "generic")
            if key in _CMS_FRAMEWORKS:
                return {
                    "name":       _CMS_FRAMEWORKS[key],
                    "confidence": fw.get("confidence", 85),
                }

        return None

    # ── Legacy compatibility (returns plain string) ──────────────────────────

    def detect_from_html(self, html: str) -> str:
        result = self._match_html(html)
        return result[0] if result else "generic"

    def detect_from_generator(self, generator: str) -> str:
        result = self._match_generator(generator)
        return result[0] if result else "generic"

    def detect_from_headers(self, headers: Dict[str, str]) -> str:
        result = self._match_headers(headers)
        return result[0] if result else "generic"

    def detect_from_context(
        self,
        html:      Optional[str] = None,
        generator: Optional[str] = None,
        headers:   Optional[Dict[str, str]] = None,
        meta_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Legacy: returns canonical key string."""
        result = self.detect_structured(html=html, generator=generator, headers=headers)
        return result["key"]

    # ── Private ──────────────────────────────────────────────────────────────

    def _match_html(self, html: str) -> Optional[tuple]:
        if not html:
            return None
        # Search in first 50 KB — framework markers are always in <head>/<body> open
        sample = html[:50_000].lower()
        best = None
        for pattern, key, confidence in self._html_patterns:
            if re.search(pattern, sample):
                if best is None or confidence > best[1]:
                    best = (key, confidence, "html")
                    if confidence >= 95:
                        break  # definitive signal; no need to keep scanning
        return best

    def _match_generator(self, generator: str) -> Optional[tuple]:
        if not generator:
            return None
        gen_lower = generator.lower()
        for pattern, key, confidence in self._generator_patterns:
            if re.search(pattern, gen_lower):
                return (key, confidence, "generator")
        return None

    def _match_headers(self, headers: Dict[str, str]) -> Optional[tuple]:
        if not headers:
            return None
        headers_lower = {k.lower(): v.lower() for k, v in headers.items()}
        for hname_pat, hval_pat, key, confidence in self._header_patterns:
            for header_name, header_value in headers_lower.items():
                if re.search(hname_pat, header_name):
                    if not hval_pat or re.search(hval_pat, header_value):
                        return (key, confidence, "header")
        return None

    @staticmethod
    def _build_result(key: str, confidence: int, source: str) -> Dict[str, Any]:
        return {
            "name":       _DISPLAY_NAMES.get(key, key.capitalize()),
            "key":        key,
            "confidence": max(0, min(100, confidence)),
            "source":     source,
        }

    def _normalize_framework(self, framework: str) -> str:
        """Normalize a free-form framework string to a canonical key."""
        if not framework:
            return "generic"
        fw_lower = framework.lower()
        if fw_lower in self.CANONICAL_KEYS:
            return fw_lower
        variations = {
            "next.js": "nextjs", "next": "nextjs",
            "wp": "wordpress",
            "reactjs": "react",
            "nuxt": "nuxtjs",
            "vue": "vuejs", "vue.js": "vuejs",
            "ng": "angular",
        }
        return variations.get(fw_lower, "generic")


# Singleton
framework_detector = FrameworkDetector()
