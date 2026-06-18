"""
Context Enrichment Orchestrator
================================

Single entry point called by the page scraping worker to produce
the structured `page_context` dict stored in seo_page_data.

Output schema
-------------
  enrich_page_context() → {
      "framework": {
          "name":       "Next.js",
          "key":        "nextjs",
          "confidence": 95,
          "source":     "html"
      },
      "cms": {
          "name":       "WordPress",
          "confidence": 98
      } | None,
      "pageType": {
          "name":       "Contact",
          "confidence": 98
      }
  }

The caller (page_scraping.py) also flattens these into the top-level
`framework`, `cms`, and `page_type` fields for backward compat with
existing queries that read those flat fields directly.
"""

from typing import Any, Dict, List, Optional

from shared.framework_detector import framework_detector
from shared.page_type_detector  import page_type_detector


def enrich_page_context(
    url:         str,
    html:        Optional[str]              = None,
    generator:   Optional[str]              = None,
    headers:     Optional[Dict[str, str]]   = None,
    title:       Optional[str]              = None,
    h1:          Optional[str]              = None,
    schema_types: Optional[List[str]]       = None,
    breadcrumbs:  Optional[List[str]]       = None,
) -> Dict[str, Any]:
    """
    Run framework + CMS + page-type detection on a single page.

    All arguments except `url` are optional.  Providing more signals
    improves confidence.

    Args:
        url:          Canonical page URL (used for page-type URL patterns).
        html:         Raw HTML source (most comprehensive signal source).
        generator:    Content of <meta name="generator"> tag.
        headers:      HTTP response headers dict.
        title:        Page <title> text.
        h1:           First H1 text.
        schema_types: List of schema.org @type values detected on the page.
        breadcrumbs:  Breadcrumb trail as a list of labels.

    Returns:
        Structured page_context dict.
    """
    # ── Framework detection ───────────────────────────────────────────────────
    framework = framework_detector.detect_structured(
        html=html,
        generator=generator,
        headers=headers,
    )

    # ── CMS detection (derived from framework + direct signals) ───────────────
    cms = framework_detector.detect_cms_structured(
        framework_result=framework,
        html=html,
        generator=generator,
    )

    # ── Page type detection ────────────────────────────────────────────────────
    # Pick the primary schema type (first non-WebPage/WebSite type if available)
    primary_schema = _pick_primary_schema(schema_types)

    page_type = page_type_detector.detect_structured(
        url=url,
        schema_type=primary_schema,
        title=title,
        h1=h1,
        html=html,
        breadcrumbs=breadcrumbs,
    )

    return {
        "framework": framework,
        "cms":       cms,        # None when not a CMS platform
        "pageType":  page_type,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pick_primary_schema(schema_types: Optional[List]) -> Optional[str]:
    """
    Choose the most informative schema type from a list.

    Ignores generic types like WebPage, WebSite, Organization that don't
    carry page-type signal.  Returns None if no informative type found.
    """
    if not schema_types:
        return None

    LOW_SIGNAL_TYPES = {
        "WebPage", "WebSite", "Organization", "LocalBusiness",
        "BreadcrumbList", "ImageObject", "SiteLinksSearchBox",
    }

    # Flatten if nested (some workers store [{@type: ...}])
    flat = []
    for item in schema_types:
        if isinstance(item, str):
            flat.append(item)
        elif isinstance(item, dict):
            t = item.get("@type")
            if isinstance(t, list):
                flat.extend(t)
            elif isinstance(t, str):
                flat.append(t)

    # Prefer informative types
    informative = [t for t in flat if t not in LOW_SIGNAL_TYPES]
    if informative:
        return informative[0]

    return flat[0] if flat else None


def extract_generator_from_page_data(page_data: Dict[str, Any]) -> Optional[str]:
    """
    Safely extract the generator meta tag value from a page_data dict.

    Handles both list and string storage formats.
    """
    meta_tags = page_data.get("meta_tags") or {}
    gen = meta_tags.get("generator")
    if isinstance(gen, list):
        return gen[0] if gen else None
    if isinstance(gen, str):
        return gen or None
    return None


def extract_schema_types_from_page_data(page_data: Dict[str, Any]) -> List[str]:
    """
    Extract schema.org @type values from a page_data dict.

    Handles the array-of-objects format stored by the Python scraper.
    """
    schemas = page_data.get("structured_data") or []
    types = []
    for schema in schemas:
        if not isinstance(schema, dict):
            continue
        t = schema.get("@type")
        if isinstance(t, list):
            types.extend(t)
        elif isinstance(t, str):
            types.append(t)
    return types


def extract_h1_from_page_data(page_data: Dict[str, Any]) -> Optional[str]:
    """Extract first H1 text from various heading storage formats."""
    # Format A: content.headings.h1 list
    content_h1 = page_data.get("content", {}).get("headings", {}).get("h1")
    if isinstance(content_h1, list) and content_h1:
        return content_h1[0]
    if isinstance(content_h1, str) and content_h1:
        return content_h1

    # Format B: top-level headings dict
    top_h1 = page_data.get("headings", {})
    if isinstance(top_h1, dict):
        h1_list = top_h1.get("h1", [])
        if isinstance(h1_list, list) and h1_list:
            return h1_list[0]

    # Format C: flat list of {tag, text}
    if isinstance(top_h1, list):
        for h in top_h1:
            if isinstance(h, dict) and h.get("tag", "").lower() == "h1":
                return h.get("text")

    return None


def enrich_from_page_data(page_data: Dict[str, Any], url: str) -> Dict[str, Any]:
    """
    Convenience wrapper: extract all inputs from an existing page_data
    dict and run enrichment.

    Used by both the scraper (after scraping) and the backfill script.
    """
    return enrich_page_context(
        url=url,
        html=page_data.get("raw_html") or None,
        generator=extract_generator_from_page_data(page_data),
        title=page_data.get("title") or None,
        h1=extract_h1_from_page_data(page_data),
        schema_types=extract_schema_types_from_page_data(page_data),
    )
