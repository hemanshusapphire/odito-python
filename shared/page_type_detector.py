"""
Shared Page Type Detector
==========================

Produces structured detection output used by:
  - page_scraping worker  (stores to seo_page_data.page_context)
  - context_enrichment    (orchestrator)
  - backfill utility

Output schema
-------------
  detect_structured() → {
      "name":       "Contact",
      "confidence": 98
  }

Canonical types (lowercase stored in DB, title-case for display)
----------------------------------------------------------------
  homepage, article, service, product, collection, faq, about,
  contact, pricing, legal, listing, blog, generic

Detection priority (highest → lowest confidence)
-------------------------------------------------
  1. Schema.org @type   (90)
  2. URL path patterns  (60–85)
  3. Title / H1 content (40–70)
  4. HTML signals       (40–60)
"""

from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import re


# Canonical type → display name
_DISPLAY_NAMES = {
    "homepage":   "Homepage",
    "article":    "Article",
    "service":    "Service",
    "product":    "Product",
    "collection": "Collection",
    "faq":        "FAQ",
    "about":      "About",
    "contact":    "Contact",
    "pricing":    "Pricing",
    "legal":      "Legal",
    "listing":    "Listing",
    "blog":       "Blog",
    "generic":    "Generic",
}

# Schema.org @type → canonical type
_SCHEMA_MAP = {
    "WebPage":          "generic",
    "WebSite":          "homepage",
    "Article":          "article",
    "BlogPosting":      "article",
    "NewsArticle":      "article",
    "TechArticle":      "article",
    "Product":          "product",
    "ProductGroup":     "product",
    "Service":          "service",
    "FAQPage":          "faq",
    "QAPage":           "faq",
    "CollectionPage":   "collection",
    "AboutPage":        "about",
    "ContactPage":      "contact",
    "BreadcrumbList":   "generic",
    "Organization":     "generic",
    "LocalBusiness":    "generic",
    "ItemList":         "listing",
    "SearchResultsPage":"listing",
    "ProfilePage":      "generic",
}

# URL path → (canonical_type, confidence)
# Patterns are evaluated in order; first match wins.
# More specific patterns have higher confidence.
_URL_RULES: List[tuple] = [
    # Homepage
    (r"^/?$",                       "homepage",   90),
    (r"^/index\.(html?|php)$",      "homepage",   90),

    # Pricing
    (r"/pricing",                   "pricing",    88),
    (r"/plans",                     "pricing",    80),
    (r"/subscription",              "pricing",    72),

    # Legal
    (r"/privacy[-_]policy",         "legal",      90),
    (r"/privacy$",                  "legal",      82),
    (r"/terms[-_](of[-_](service|use))?", "legal", 90),
    (r"/terms$",                    "legal",      85),
    (r"/cookie[-_]policy",          "legal",      88),
    (r"/gdpr",                      "legal",      85),
    (r"/disclaimer",                "legal",      80),
    (r"/legal",                     "legal",      80),

    # Contact
    (r"/contact[-_]us",             "contact",    90),
    (r"/contact$",                  "contact",    88),
    (r"/get[-_]in[-_]touch",        "contact",    85),
    (r"/reach[-_]us",               "contact",    85),
    (r"/reach[-_]out",              "contact",    80),

    # About
    (r"/about[-_]us",               "about",      90),
    (r"/about$",                    "about",      88),
    (r"/our[-_]story",              "about",      82),
    (r"/company",                   "about",      72),
    (r"/team",                      "about",      65),
    (r"/who[-_]we[-_]are",          "about",      78),

    # FAQ
    (r"/faq",                       "faq",        88),
    (r"/faqs",                      "faq",        88),
    (r"/help/",                     "faq",        70),
    (r"/support/",                  "faq",        65),
    (r"/knowledge[-_]base",         "faq",        72),
    (r"/questions",                 "faq",        72),

    # Blog (must check before /article to avoid /blog/article-slug misfire)
    (r"/blog/?$",                   "blog",       85),
    (r"/blog/",                     "article",    80),  # blog post
    (r"/posts?/",                   "article",    78),
    (r"/news/",                     "article",    78),
    (r"/article/",                  "article",    80),
    (r"/stories/",                  "article",    75),
    (r"/insights/",                 "article",    72),
    (r"/resources/[^/]+$",          "article",    68),

    # Product
    (r"/product/",                  "product",    85),
    (r"/products/",                 "product",    82),
    (r"/shop/[^/]+$",               "product",    78),
    (r"/item/",                     "product",    78),
    (r"/p/[^/]+$",                  "product",    75),

    # Service
    (r"/services?/",                "service",    82),
    (r"/solutions/",                "service",    75),
    (r"/offerings?/",               "service",    72),

    # Collection / Listing
    (r"/category/",                 "collection", 80),
    (r"/categories/",               "collection", 78),
    (r"/collection/",               "collection", 80),
    (r"/catalog/",                  "collection", 78),
    (r"/shop/?$",                   "collection", 75),
    (r"/tag/",                      "listing",    72),
    (r"/archive/",                  "listing",    72),
    (r"/search/?",                  "listing",    68),
]

# Title/H1 keyword → (canonical_type, confidence)
# Short-circuits when URL gives no signal.
_TITLE_RULES: List[tuple] = [
    (r"\bcontact\s+us\b",           "contact",    72),
    (r"\bcontact\b",                "contact",    60),
    (r"\bget\s+in\s+touch\b",       "contact",    70),
    (r"\babout\s+us\b",             "about",      72),
    (r"\babout\b",                  "about",      58),
    (r"\bour\s+story\b",            "about",      68),
    (r"\bpricing\b",                "pricing",    72),
    (r"\bplans?\s+(&|and)\s+pricing", "pricing",  78),
    (r"\bfaq\b",                    "faq",        72),
    (r"\bfrequently\s+asked\b",     "faq",        75),
    (r"\bprivacy\s+policy\b",       "legal",      78),
    (r"\bterms\s+of\s+(service|use)\b", "legal",  78),
    (r"\bcookie\s+policy\b",        "legal",      72),
    (r"\bproduct\b",                "product",    55),
    (r"\bservice[s]?\b",            "service",    50),
]

# HTML body signals → (canonical_type, confidence)
_HTML_RULES: List[tuple] = [
    # Contact signals
    (r'<form[^>]+(?:contact|inquiry|enquiry)',  "contact",  65),
    (r'type=["\']email["\']',                   "contact",  55),

    # Pricing signals
    (r'class=["\'][^"\']*price[^"\']*["\']',    "pricing",  62),
    (r'class=["\'][^"\']*plan[^"\']*["\']',     "pricing",  58),
    (r'\$[\d,]+\s*/\s*(?:mo|month|yr|year)',    "pricing",  70),

    # Article signals
    (r'<article\b',                             "article",  68),
    (r'class=["\'][^"\']*(?:post|entry)[-_]content', "article", 65),
    (r'<time\b[^>]+datetime=',                  "article",  55),

    # Product signals
    (r'(?:add[-_]to[-_]cart|add.to.basket)',    "product",  72),
    (r'class=["\'][^"\']*product[-_]price',     "product",  68),

    # FAQ signals
    (r'(?:class|id)=["\'][^"\']*(?:faq|accordion)[^"\']*["\']', "faq", 65),

    # Legal signals
    (r'last\s+(?:updated|revised)',             "legal",    55),
    (r'effective\s+date',                       "legal",    55),
]


class PageTypeDetector:
    """Unified page type detection with multi-signal confidence scoring."""

    CANONICAL_TYPES = list(_DISPLAY_NAMES.keys())

    def detect_structured(
        self,
        url:        Optional[str] = None,
        schema_type = None,            # str | list[str]
        title:      Optional[str] = None,
        h1:         Optional[str] = None,
        html:       Optional[str] = None,
        breadcrumbs: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Detect page type from multiple signals.

        Returns:
            { "name": "Contact", "confidence": 98 }
        """
        candidates: List[tuple] = []  # (type, confidence)

        # ── 1. Schema.org (highest confidence) ───────────────────────────────
        if schema_type:
            result = self._from_schema(schema_type)
            if result:
                candidates.append(result)

        # ── 2. URL path ───────────────────────────────────────────────────────
        if url:
            result = self._from_url(url)
            if result:
                candidates.append(result)

        # ── 3. Title / H1 ─────────────────────────────────────────────────────
        text_signals = " ".join(filter(None, [title, h1]))
        if text_signals:
            result = self._from_text(text_signals)
            if result:
                candidates.append(result)

        # ── 4. Breadcrumbs ────────────────────────────────────────────────────
        if breadcrumbs:
            result = self._from_breadcrumbs(breadcrumbs)
            if result:
                candidates.append(result)

        # ── 5. HTML body signals ──────────────────────────────────────────────
        if html:
            result = self._from_html(html[:30_000])  # scan head + above fold only
            if result:
                candidates.append(result)

        # Pick highest-confidence non-generic result
        candidates.sort(key=lambda x: x[1], reverse=True)
        for ptype, confidence in candidates:
            if ptype != "generic":
                return {
                    "name":       _DISPLAY_NAMES.get(ptype, ptype.capitalize()),
                    "confidence": confidence,
                }

        # Nothing detected
        return {"name": "Generic", "confidence": 40}

    # ── Legacy compatibility ─────────────────────────────────────────────────

    def detect_from_url(self, url: str) -> str:
        result = self._from_url(url)
        return _DISPLAY_NAMES.get(result[0], "Generic") if result else "Generic"

    def detect_from_schema(self, schema_type) -> str:
        result = self._from_schema(schema_type)
        return _DISPLAY_NAMES.get(result[0], "Generic") if result else "Generic"

    def detect_from_html(self, html: str, url: Optional[str] = None) -> str:
        result = self._from_html(html)
        if result:
            return _DISPLAY_NAMES.get(result[0], "Generic")
        if url:
            return self.detect_from_url(url)
        return "Generic"

    def detect_from_context(
        self,
        url:         Optional[str] = None,
        schema_type: Optional[str] = None,
        html:        Optional[str] = None,
        meta_data:   Optional[Dict[str, Any]] = None,
    ) -> str:
        """Legacy: returns canonical display name string."""
        result = self.detect_structured(url=url, schema_type=schema_type, html=html)
        return result["name"]

    # ── Private ──────────────────────────────────────────────────────────────

    def _from_schema(self, schema_type) -> Optional[tuple]:
        types = schema_type if isinstance(schema_type, list) else [schema_type]
        for t in types:
            mapped = _SCHEMA_MAP.get(t)
            if mapped and mapped != "generic":
                return (mapped, 90)
        return None

    def _from_url(self, url: str) -> Optional[tuple]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        path = parsed.path.lower().rstrip("/") or "/"

        # Exact homepage check
        if path in ("", "/", "/index.html", "/index.htm", "/index.php"):
            return ("homepage", 90)

        for pattern, ptype, confidence in _URL_RULES:
            if re.search(pattern, path):
                return (ptype, confidence)
        return None

    def _from_text(self, text: str) -> Optional[tuple]:
        text_lower = text.lower()
        for pattern, ptype, confidence in _TITLE_RULES:
            if re.search(pattern, text_lower):
                return (ptype, confidence)
        return None

    def _from_breadcrumbs(self, breadcrumbs: List[str]) -> Optional[tuple]:
        if not breadcrumbs:
            return None
        combined = " ".join(b.lower() for b in breadcrumbs)
        return self._from_text(combined)

    def _from_html(self, html: str) -> Optional[tuple]:
        if not html:
            return None
        html_lower = html.lower()
        best = None
        for pattern, ptype, confidence in _HTML_RULES:
            if re.search(pattern, html_lower):
                if best is None or confidence > best[1]:
                    best = (ptype, confidence)
        return best

    def _normalize_type(self, page_type: str) -> str:
        if not page_type:
            return "generic"
        pt_lower = page_type.lower()
        if pt_lower in _DISPLAY_NAMES:
            return pt_lower
        # Title-case lookup
        for key, display in _DISPLAY_NAMES.items():
            if display.lower() == pt_lower:
                return key
        return "generic"


# Singleton
page_type_detector = PageTypeDetector()
