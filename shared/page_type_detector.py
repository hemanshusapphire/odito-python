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
      "confidence": 98,
      "signals":    {
          "url":        {"type": "Contact", "confidence": 88},
          "breadcrumb": None,
          "template":   None,
          "content":    {"type": "Contact", "confidence": 72},
          "schema":     None,
      }
  }

Canonical types (lowercase stored in DB, title-case for display)
----------------------------------------------------------------
  homepage, article, service, product, collection, faq, about,
  contact, pricing, legal, listing, blog, generic

Classification weighting (multi-signal accumulation)
-----------------------------------------------------
  URL Pattern       = 30%  — most reliable structural signal
  Breadcrumb        = 25%  — user-visible hierarchy confirms URL intent
  Page Template     = 20%  — HTML class/element patterns
  Content Intent    = 15%  — Title / H1 keyword signals
  Schema Signals    = 10%  — tie-breaker ONLY; schema can NEVER override URL

  Schema intentionally has the lowest weight to prevent circular validation:
  wrong schema on a page must not cause it to be misclassified as that type,
  which would then make the wrong schema appear "valid".
"""

from collections import defaultdict
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
    (r"/who[-_]we[-_]are",          "about",      78),
    (r"/company",                   "about",      72),
    (r"/team",                      "about",      65),
    (r"^/about[-_]",                "about",      70),  # /about-naxonify, /about-company, etc.

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

# Signal weights (must sum to 100)
_SIGNAL_WEIGHTS = {
    "url":        30,
    "breadcrumb": 25,
    "template":   20,
    "content":    15,
    "schema":     10,
}


class PageTypeDetector:
    """Unified page type detection with multi-signal weighted confidence scoring."""

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
        Detect page type from multiple signals using weighted scoring.

        Signals are weighted so schema can never override URL/breadcrumb evidence.
        This prevents circular validation where a wrong schema on a page would
        cause it to be classified as that schema type, making the wrong schema
        appear valid.

        Returns:
            {
                "name":       "Service",
                "confidence": 77,
                "signals":    {"url": {"type": "Service", "confidence": 82}, ...}
            }
        """
        scores: Dict[str, float] = defaultdict(float)
        fired: List[tuple] = []   # (signal_name, page_type, raw_confidence)
        signal_log: Dict[str, Any] = {}

        # ── 1. URL (weight: 30) ───────────────────────────────────────────────
        if url:
            result = self._from_url(url)
            if result:
                ptype, conf = result
                scores[ptype] += (conf / 100.0) * _SIGNAL_WEIGHTS["url"]
                fired.append(("url", ptype, conf))
                signal_log["url"] = {"type": _DISPLAY_NAMES.get(ptype, ptype), "confidence": conf}
            else:
                signal_log["url"] = None

        # ── 2. Breadcrumbs (weight: 25) ───────────────────────────────────────
        if breadcrumbs:
            result = self._from_breadcrumbs(breadcrumbs)
            if result:
                ptype, conf = result
                scores[ptype] += (conf / 100.0) * _SIGNAL_WEIGHTS["breadcrumb"]
                fired.append(("breadcrumb", ptype, conf))
                signal_log["breadcrumb"] = {"type": _DISPLAY_NAMES.get(ptype, ptype), "confidence": conf}
            else:
                signal_log["breadcrumb"] = None

        # ── 3. Page template / HTML signals (weight: 20) ─────────────────────
        if html:
            result = self._from_html(html[:30_000])  # scan head + above fold only
            if result:
                ptype, conf = result
                scores[ptype] += (conf / 100.0) * _SIGNAL_WEIGHTS["template"]
                fired.append(("template", ptype, conf))
                signal_log["template"] = {"type": _DISPLAY_NAMES.get(ptype, ptype), "confidence": conf}
            else:
                signal_log["template"] = None

        # ── 4. Content intent / Title + H1 (weight: 15) ──────────────────────
        text_signals = " ".join(filter(None, [title, h1]))
        if text_signals:
            result = self._from_text(text_signals)
            if result:
                ptype, conf = result
                scores[ptype] += (conf / 100.0) * _SIGNAL_WEIGHTS["content"]
                fired.append(("content", ptype, conf))
                signal_log["content"] = {"type": _DISPLAY_NAMES.get(ptype, ptype), "confidence": conf}
            else:
                signal_log["content"] = None

        # ── 5. Schema signals (weight: 10) — tie-breaker, never primary ───────
        # Schema evidence is deliberately last and lowest-weighted.
        # A page with wrong schema (e.g. Article on /service/) scores:
        #   service URL = 82 × 0.30 = 24.6 pts  ← wins
        #   article schema = 90 × 0.10 = 9.0 pts ← loses
        if schema_type:
            result = self._from_schema(schema_type)
            if result:
                ptype, conf = result
                scores[ptype] += (conf / 100.0) * _SIGNAL_WEIGHTS["schema"]
                fired.append(("schema", ptype, conf))
                signal_log["schema"] = {"type": _DISPLAY_NAMES.get(ptype, ptype), "confidence": conf}
            else:
                signal_log["schema"] = None

        if not scores:
            return {"name": "Generic", "confidence": 40, "signals": signal_log}

        # ── Pick winner: highest accumulated score, ignoring generic ──────────
        winner_type: Optional[str] = None
        for ptype, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            if ptype != "generic":
                winner_type = ptype
                break

        if not winner_type:
            return {"name": "Generic", "confidence": 40, "signals": signal_log}

        # ── Confidence calculation ────────────────────────────────────────────
        # Base = highest raw confidence among NON-SCHEMA signals agreeing with winner.
        # Using non-schema signals as base prevents schema-only pages from reporting
        # 90% confidence when the schema may be wrong (e.g. template plugin setting
        # Article schema on every page including /about, /contact, /service).
        # +3 per additional agreeing signal (max +15) for multi-signal convergence.
        # -5 per conflicting signal (max -20) for disagreement.
        winner_signals  = [(n, t, c) for n, t, c in fired if t == winner_type]
        conflict_signals = [(n, t, c) for n, t, c in fired if t != winner_type and t != "generic"]

        non_schema_winner = [(n, t, c) for n, t, c in winner_signals if n != "schema"]
        if non_schema_winner:
            primary_conf = max(c for _, _, c in non_schema_winner)
        else:
            # Schema is the ONLY signal — unreliable alone; cap at 45.
            primary_conf = 45

        agreement_bonus = min(15, (len(winner_signals) - 1) * 3)
        conflict_penalty = min(20, len(conflict_signals) * 5)

        confidence = max(40, min(99, int(primary_conf + agreement_bonus - conflict_penalty)))

        return {
            "name":       _DISPLAY_NAMES.get(winner_type, winner_type.capitalize()),
            "confidence": confidence,
            "signals":    signal_log,
        }

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
        _meta_data:  Optional[Dict[str, Any]] = None,
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
