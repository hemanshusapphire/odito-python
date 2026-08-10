"""
Extraction: technical signals.

Reads robots/meta directives, canonical tag, JS-rendering dependency, and
sitemap existence from seo_page_data and domain_technical_reports.

All output stored under ai_pages["technical"].
"""

import re
from typing import Any


_NOINDEX_META = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]+content=["\'][^"\']*noindex',
    re.IGNORECASE,
)

_CANONICAL_PATTERN = re.compile(
    r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def extract_technical(
    page_data: dict[str, Any],
    domain_report: dict[str, Any],
    raw_html: str = "",
) -> dict[str, Any]:
    """
    Extract technical crawlability signals for the AISO Citability card.

    Args:
        page_data:     document from seo_page_data / seo_ai_visibility.
        domain_report: document from domain_technical_reports.
        raw_html:      raw HTML string for meta/canonical parsing.

    Returns:
        Technical sub-document for ai_pages.
    """
    # ── noindex ───────────────────────────────────────────────────────────────
    # Check meta robots in the scraper result first, fallback to raw HTML.
    meta_robots: str = (page_data.get("meta_robots") or "").lower()
    noindex_from_meta = "noindex" in meta_robots
    noindex_from_html = bool(_NOINDEX_META.search(raw_html)) if raw_html else False
    noindex = noindex_from_meta or noindex_from_html

    # ── canonical ─────────────────────────────────────────────────────────────
    canonical_url: str | None = page_data.get("canonical_url") or None
    if not canonical_url and raw_html:
        match = _CANONICAL_PATTERN.search(raw_html)
        if match:
            canonical_url = match.group(1)

    page_url: str = page_data.get("url", "") or ""
    canonical_present = bool(canonical_url)
    canonical_is_self = (
        canonical_url.rstrip("/") == page_url.rstrip("/")
        if (canonical_url and page_url)
        else False
    )

    # ── JS-only rendering dependency ─────────────────────────────────────────
    # Flag is set by the headless worker when significant content is only
    # present in the JS-rendered version and not in the raw HTML response.
    js_rendered_only: bool = bool(page_data.get("js_rendered_only", False))

    # ── Sitemap ───────────────────────────────────────────────────────────────
    sitemap_data: dict = domain_report.get("sitemap", {}) or {}
    sitemap_exists: bool = bool(sitemap_data.get("exists", False))
    sitemap_url: str | None = sitemap_data.get("url") or None
    # Breadth of the sitemap (AISO-CV8). None when the adapter has no count
    # (e.g. sitemap doesn't exist, or predates this field) — rules must treat
    # None as "unknown", not zero.
    sitemap_url_count = sitemap_data.get("url_count")

    # ── Reachability (AISO-CV9) ─────────────────────────────────────────────
    # discovered/qualified come from the URL_QUALIFICATION worker's stats,
    # bridged in by domain_report_adapter.py. Both are None when qualification
    # stats haven't been persisted for this project yet (older audits).
    reachability_data: dict = domain_report.get("reachability", {}) or {}
    reachability = {
        "discovered": reachability_data.get("discovered"),
        "qualified":  reachability_data.get("qualified"),
    }

    # ── URL parameter / duplicate-variant hygiene (AISO-CV10) ──────────────
    # Written directly onto seo_page_data by the scraper (page_scraping.py) —
    # read straight off page_data, no new computation needed here.
    url_hygiene = {
        "has_parameters":   bool(page_data.get("has_parameters", False)),
        "parameter_count":  page_data.get("parameter_count", 0),
        "has_double_slash": bool(page_data.get("has_double_slash", False)),
    }

    return {
        "noindex":           noindex,
        "canonical_present": canonical_present,
        "canonical_url":     canonical_url,
        "canonical_is_self": canonical_is_self,
        "js_rendered_only":  js_rendered_only,
        "sitemap": {
            "exists":    sitemap_exists,
            "url":       sitemap_url,
            "url_count": sitemap_url_count,
        },
        "reachability": reachability,
        "url_hygiene":  url_hygiene,
    }
