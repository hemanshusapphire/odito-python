"""
SEO Rule Utilities — Shared Helpers

Centralised helper functions used across multiple SEO rule category files.
Import these instead of redefining them locally.
"""

from urllib.parse import urlparse


# ── Primary Keyword Extraction ────────────────────────────────

def _keyword_from_context(normalized):
    """
    Extract primary keyword with this priority order:
    1. Project-level target_keyword field (if present in normalized)
    2. Job-level primary_keyword field (if present in normalized)
    3. meta_tags.keywords[0] (if meaningful — not empty, >= 3 chars)
    4. First H1 text as fallback (if >= 3 chars)
    Returns lowercase string or None.
    """
    # 1. Project-level target keyword
    target_kw = normalized.get("target_keyword", "")
    if target_kw and isinstance(target_kw, str):
        target_kw = target_kw.strip().lower()
        if len(target_kw) >= 3:
            return target_kw

    # 2. Job-level primary keyword
    primary_kw = normalized.get("primary_keyword", "")
    if primary_kw and isinstance(primary_kw, str):
        primary_kw = primary_kw.strip().lower()
        if len(primary_kw) >= 3:
            return primary_kw

    # 3. meta_tags.keywords[0]
    meta_tags = normalized.get("meta_tags", {})
    keywords_list = meta_tags.get("keywords", [])
    if keywords_list and isinstance(keywords_list, list) and keywords_list[0]:
        kw = keywords_list[0]
        if isinstance(kw, str):
            keyword = kw.split(",")[0].strip().lower()
            if keyword and len(keyword) >= 3:
                return keyword

    # Priority 4: H1 fallback — only use if it looks like a real keyword
    # (not a full sentence, not a site name pattern, not too long)
    headings = normalized.get("headings", [])
    if isinstance(headings, list):
        for h in headings:
            if isinstance(h, dict) and h.get("tag") == "h1":
                text = h.get("text", "").strip().lower()
                word_count = len(text.split())
                # Reject: too long, too short, or looks like a sentence
                if (3 <= len(text) <= 40 
                    and word_count <= 5
                    and " - " not in text
                    and " | " not in text
                    and not text.endswith((".com", ".net", ".org"))):
                    return text
    
    # No usable keyword found
    return None


# ── Schema Helpers ────────────────────────────────────────────

def _get_schemas(normalized):
    """Get structured data list from normalized data."""
    return normalized.get("structured_data", [])


def _find_schema_by_type(schemas, type_name):
    """Find first schema dict matching a given @type."""
    if not schemas or not isinstance(schemas, list):
        return None
    
    if not type_name:
        return None
    
    target_type_lower = type_name.lower()
    
    for s in schemas:
        if isinstance(s, dict):
            schema_type = s.get("@type", "")
            if isinstance(schema_type, list):
                # Handle @type as array
                for t in schema_type:
                    if isinstance(t, str) and t.lower() == target_type_lower:
                        return s
            elif isinstance(schema_type, str):
                # Handle @type as string
                if schema_type.lower() == target_type_lower:
                    return s
    return None


def _is_valid_https_url(url_str):
    """Check whether a URL string is a valid HTTPS URL."""
    if not url_str:
        return False
    try:
        parsed = urlparse(str(url_str).strip())
        return parsed.scheme == "https" and bool(parsed.netloc)
    except Exception:
        return False


def _is_valid_url(url_str):
    """Check whether a URL string is a valid HTTP or HTTPS URL."""
    if not url_str:
        return False
    try:
        parsed = urlparse(str(url_str).strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


# ── Performance Helpers ───────────────────────────────────────

def _get_perf(normalized, device="mobile"):
    """Get performance data dict for a given device type."""
    perf = normalized.get("performance", {})
    if isinstance(perf, dict):
        # Try device-nested first: {"mobile": {...}}
        if device in perf:
            return perf[device]
        # Flat dict fallback (direct metrics)
        if "performance_score" in perf:
            return perf
    return {}


# ── Accessibility / Headless Helpers ──────────────────────────

def _get_headless(normalized):
    """Get headless data dict."""
    return normalized.get("headless", {})


def _get_axe(normalized):
    """Get axe-core violations list from headless data."""
    headless = _get_headless(normalized)
    return headless.get("axeViolations", [])


def _get_dom_metrics(normalized):
    """Get DOM metrics dict from headless data."""
    headless = _get_headless(normalized)
    return headless.get("domMetrics", {})


def _get_keyboard(normalized):
    """Get keyboard analysis dict from headless data."""
    headless = _get_headless(normalized)
    return headless.get("keyboard_analysis", {})


# ── Anti-False-Positive Helpers ───────────────────────────────

def _safe_get_first_value(field):
    """Safely extract first value from field that could be string or list."""
    if field is None:
        return ""
    if isinstance(field, list):
        if not field:
            return ""
        first_item = field[0]
        return str(first_item).strip() if first_item is not None else ""
    if isinstance(field, str):
        return field.strip()
    return str(field).strip() if field is not None else ""


def _normalize_meta_tags(meta_tags):
    """Normalize meta tags dict with case-insensitive keys."""
    if not meta_tags or not isinstance(meta_tags, dict):
        return {}
    normalized = {}
    for key, value in meta_tags.items():
        if key is not None:
            normalized_key = str(key).lower().strip()
            normalized[normalized_key] = value
    return normalized


def _get_meta_tag_value(normalized_meta, tag_name):
    """Get meta tag value with safe extraction."""
    if not normalized_meta:
        return ""
    value = normalized_meta.get(tag_name.lower())
    if value is not None:
        return _safe_get_first_value(value)
    return ""


def _has_noindex_directive(normalized_meta):
    """Only flag if 'noindex' is explicitly present in robots meta."""
    if not normalized_meta:
        return False
    robots_value = _get_meta_tag_value(normalized_meta, "robots")
    if not robots_value:
        return False
    robots_content = robots_value.lower()
    return "noindex" in robots_content
