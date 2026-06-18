"""
Extraction: GEO signals.

Reads LocalBusiness schema (from schema extraction), page content, and
technical signals to produce the GEO sub-document for ai_pages.

All output stored under ai_pages["geo"].
"""

import re
from typing import Any


_MAPS_PATTERN = re.compile(
    r'src=["\']https?://(www\.)?google\.com/maps/embed',
    re.IGNORECASE,
)

_HREFLANG_PATTERN = re.compile(
    r'<link[^>]+hreflang=["\']([^"\']+)["\']',
    re.IGNORECASE,
)

_PHONE_PATTERN = re.compile(
    r"(\+?\d[\d\s\-().]{6,}\d)"
)


def _normalize_str(value: Any) -> str | None:
    if not value:
        return None
    return str(value).strip().lower()


def _nap_match(schema_val: Any, visible_val: Any) -> bool:
    """Loose match: both values non-empty and share at least the first word."""
    a = _normalize_str(schema_val)
    b = _normalize_str(visible_val)
    if not a or not b:
        return False
    return a.split()[0] == b.split()[0]


def extract_geo(
    page_data: dict[str, Any],
    schema: dict[str, Any],
    raw_html: str = "",
    content_sig: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Extract GEO signals.

    Args:
        page_data:   document from seo_page_data / seo_ai_visibility.
        schema:      already-extracted schema sub-document (from schema.py).
        raw_html:    raw HTML string (used for Maps embed + hreflang detection).
        content_sig: output of extract_content() — provides pre-computed h1 list
                     and first_100_words. Preferred over reading page_data directly.

    Returns:
        GEO sub-document for ai_pages.
    """
    lb = schema.get("local_business") or {}
    lb_addr = lb.get("address", {}) or {}
    geo_coords = lb.get("geo", {}) or {}
    hours = lb.get("opening_hours", {}) or {}

    # ── City in H1 / first 100 words ─────────────────────────────────────────
    city: str = lb_addr.get("city") or ""
    if content_sig:
        h1_list: list = content_sig.get("h1", []) or []
        first_100: str = content_sig.get("first_100_words", "") or ""
    else:
        # Fallback: read directly from raw scraper doc fields.
        heading_metrics: dict = page_data.get("heading_metrics", {}) or {}
        h1_list = heading_metrics.get("h1", []) or []
        body_text_raw: str = (
            (page_data.get("main_content", {}) or {}).get("text", "")
            or page_data.get("body_text", "")
            or ""
        )
        first_100 = " ".join(body_text_raw.split()[:100])

    city_lower = city.lower().strip() if city else ""
    city_in_h1 = city_lower and any(city_lower in h.lower() for h in h1_list)
    city_in_first_100 = city_lower and city_lower in first_100.lower()

    # ── Google Maps embed ─────────────────────────────────────────────────────
    maps_embed = bool(_MAPS_PATTERN.search(raw_html)) if raw_html else False

    # ── Hreflang ──────────────────────────────────────────────────────────────
    hreflang_tags: list[str] = []
    if raw_html:
        hreflang_tags = _HREFLANG_PATTERN.findall(raw_html)

    hreflang_self_ref = any(
        tag == page_data.get("lang", "en") or tag.startswith("x-default")
        for tag in hreflang_tags
    )
    hreflang_x_default = "x-default" in hreflang_tags

    # ── Visible NAP (name/address/phone in page text) ─────────────────────────
    body_text: str = (
        (page_data.get("main_content", {}) or {}).get("text", "")
        or page_data.get("body_text", "")
        or ""
    )

    phones_in_text = _PHONE_PATTERN.findall(body_text)
    visible_phone = phones_in_text[0].strip() if phones_in_text else None

    schema_phone = lb.get("telephone")
    schema_name  = lb.get("name")
    schema_city  = lb_addr.get("city")

    # NAP consistency: schema values appear in visible body text.
    name_match  = bool(schema_name  and schema_name.lower()  in body_text.lower())
    city_match  = bool(schema_city  and schema_city.lower()  in body_text.lower())
    phone_match = bool(schema_phone and schema_phone in body_text)

    # ── SameAs links ─────────────────────────────────────────────────────────
    same_as: list = lb.get("same_as", []) or []

    return {
        "target_city": city or None,
        "city_in_h1":            bool(city_in_h1),
        "city_in_first_100":     bool(city_in_first_100),
        "maps_embed_present":    maps_embed,
        "hreflang": {
            "present":        bool(hreflang_tags),
            "tags":           hreflang_tags,
            "count":          len(hreflang_tags),
            "has_self_ref":   hreflang_self_ref,
            "has_x_default":  hreflang_x_default,
        },
        "schema_nap": {
            "name":      schema_name,
            "address":   lb_addr.get("street"),
            "city":      schema_city,
            "phone":     schema_phone,
            "complete":  bool(schema_name and lb_addr.get("street") and schema_phone),
        },
        "visible_nap": {
            "phone_found": visible_phone,
            "name_in_text":  name_match,
            "city_in_text":  city_match,
            "phone_in_text": phone_match,
        },
        "nap_consistent": name_match and city_match and phone_match,
        "same_as_links":  same_as,
        "geocoords": {
            "present":   geo_coords.get("present", False),
            "latitude":  geo_coords.get("latitude"),
            "longitude": geo_coords.get("longitude"),
        },
        "opening_hours": {
            "present": hours.get("present", False),
            "count":   hours.get("count", 0),
        },
    }
