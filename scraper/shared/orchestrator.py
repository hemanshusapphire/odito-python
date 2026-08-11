"""Orchestrator for coordinating SEO data extraction pipeline."""

import json as _json
from datetime import datetime
from typing import Optional

# Third-party imports
import requests
from bs4 import BeautifulSoup

# Local imports
from .fetcher import fetch_html, validate_html_integrity, InvalidHtmlContentError
from .seo import (
    extract_head_and_meta_data, extract_social_media_data,
    extract_internationalization_data, extract_visual_branding_data,
    extract_content_analysis, extract_image_data, extract_tracking_data,
    extract_page_signals
)
from .schema import extract_structured_data
from .utils import create_content_hash
from .intelligence import extract_seo_intelligence
from .enhanced_seo_extraction import extract_enhanced_seo_signals


def extract_comprehensive_seo_data(html: str, base_url: str, response_headers: dict = None) -> dict:
    """
    Extract ALL SEO data from HTML without duplicates.
    Production-grade extraction following enterprise SEO crawler patterns.
    """
    try:
        soup = BeautifulSoup(html, "lxml")

        # Defense in depth: fetch_html() already validates response integrity
        # before ever returning html (see validate_html_integrity in
        # fetcher.py), but extraction_status must reflect real structural
        # validity for ANY caller of this function, not merely "no Python
        # exception was raised while parsing" — BeautifulSoup/lxml do not
        # raise on corrupted/binary-as-text input, they simply return empty
        # results for every tag lookup, which previously left
        # extraction_status="SUCCESS" on fully garbled documents (forensic
        # finding, 2026-08-11).
        is_valid, integrity_diagnostics = validate_html_integrity(html)
        if not is_valid:
            return {
                "url": base_url,
                "scraped_at": datetime.utcnow().isoformat(),
                "extraction_status": "FAILED",
                "extraction_failure_reason": integrity_diagnostics.get("reason"),
            }

        # Initialize result structure
        seo_data = {
            "url": base_url,
            "scraped_at": datetime.utcnow().isoformat(),
            "extraction_status": "SUCCESS"
        }

        # 1. HEAD + SEO META DATA
        extract_head_and_meta_data(soup, seo_data)

        # 2. SOCIAL MEDIA META (Open Graph, Twitter, etc.)
        extract_social_media_data(soup, seo_data)

        # 3. INTERNATIONALIZATION
        extract_internationalization_data(soup, seo_data)

        # 4. VISUAL/BRANDING
        extract_visual_branding_data(soup, seo_data)

        # 5. STRUCTURED DATA
        extract_structured_data(soup, seo_data)

        # 6. CONTENT ANALYSIS
        extract_content_analysis(soup, seo_data)

        # 7. IMAGES
        extract_image_data(soup, base_url, seo_data)

        # 8. TRACKING & ANALYTICS
        extract_tracking_data(soup, seo_data)

        # 9. PAGE SIGNALS (review, analytics, doctype, theme-color, hreflang, facebook pixel)
        # Returns: (top_level_signals, tracking_updates)
        top_level_signals, tracking_updates = extract_page_signals(html, soup)

        # Add top-level signals
        seo_data.update(top_level_signals)

        # Merge tracking updates into tracking object (corrects old values with new accurate detections)
        if seo_data.get("tracking"):
            seo_data["tracking"].update(tracking_updates)

        # 10. SEO INTELLIGENCE (schema_validation + security)
        # Reads from already-extracted seo_data — no BeautifulSoup instantiation.
        seo_data["seo_intelligence"] = extract_seo_intelligence(
            seo_data, response_headers or {}, base_url
        )

        # 11. SINGLE JSON-LD PARSE — shared across all enhanced signal extractors.
        # Each entry: (raw_content: str, schemas: list[dict], is_valid_json: bool)
        json_ld_entries = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                content = script.string.strip() if script.string else ''
                if not content:
                    continue
                parsed = _json.loads(content)
                schemas = []
                if isinstance(parsed, dict):
                    if '@graph' in parsed:
                        g = parsed['@graph']
                        schemas = g if isinstance(g, list) else [g]
                    else:
                        schemas = [parsed]
                elif isinstance(parsed, list):
                    schemas = parsed
                json_ld_entries.append((content, schemas, True))
            except (_json.JSONDecodeError, TypeError):
                if 'content' in dir():
                    json_ld_entries.append((content, [], False))

        # 12. ENHANCED SEO SIGNALS — stored at top level so rule-engine consumers
        # can access them via normalized.get("signal_key", {}) without path changes.
        seo_data.update(extract_enhanced_seo_signals(soup, html, base_url, json_ld_entries))

        return seo_data

    except Exception as e:
        return {
            "url": base_url,
            "error": str(e),
            "scraped_at": datetime.utcnow().isoformat(),
            "extraction_status": "FAILED"
        }


def _classify_fetch_exception(e: Exception) -> dict:
    """
    Classify an exception raised by fetch_html() into a normalized
    failure_type for persistent failure tracking (seo_page_failures).
    Read-only classification of the real exception — never fabricates a
    reason. Mirrors the same substring/exception-type approach already used
    by url_qualification/worker.py's _probe_url() for consistency.
    """
    # InvalidHtmlContentError is fetch_html()'s own response-integrity
    # failure (see fetcher.py) — not a requests exception, so it must be
    # checked before the requests.exceptions branches below. Diagnostics are
    # folded into error_message (rather than added as new failure_info
    # keys/seo_page_failures fields) so the existing failure schema is
    # unchanged; the reason/content_encoding/replacement ratio are still
    # fully visible in the stored failure record.
    if isinstance(e, InvalidHtmlContentError):
        diag = e.diagnostics or {}
        msg = (
            f"reason={diag.get('reason')} content_encoding={diag.get('content_encoding')} "
            f"content_type={diag.get('content_type')} replacement_char_ratio={diag.get('replacement_char_ratio')} "
            f"http_status={diag.get('http_status')}"
        )
        return {"failure_type": "INVALID_HTML_RESPONSE", "error_message": msg[:500]}

    # SSLError is a ConnectionError subclass in requests — must check first.
    if isinstance(e, requests.exceptions.SSLError):
        return {"failure_type": "SSL_ERROR", "error_message": str(e)[:500]}

    if isinstance(e, requests.exceptions.ConnectionError):
        s = str(e).lower()
        if "getaddrinfo" in s or "name or service not known" in s or "nodename nor servname" in s:
            return {"failure_type": "DNS_ERROR", "error_message": str(e)[:500]}
        return {"failure_type": "CONNECTION_ERROR", "error_message": str(e)[:500]}

    if isinstance(e, requests.exceptions.Timeout):
        return {"failure_type": "TIMEOUT", "error_message": str(e)[:500]}

    if isinstance(e, requests.exceptions.HTTPError):
        return {"failure_type": "HTTP_ERROR", "error_message": str(e)[:500]}

    if isinstance(e, requests.exceptions.RequestException):
        # TooManyRedirects, InvalidURL, and any other request-layer failure
        # not covered above — same bucket as a generic connection problem.
        return {"failure_type": "CONNECTION_ERROR", "error_message": str(e)[:500]}

    module_name = type(e).__module__ or ""
    if "playwright" in module_name or "selenium" in module_name:
        return {"failure_type": "RENDER_FAILED", "error_message": str(e)[:500]}

    return {"failure_type": "UNEXPECTED_EXCEPTION", "error_message": str(e)[:500]}


def scrape_page_data(url: str, failure_info: Optional[dict] = None) -> dict:
    """
    Scrape comprehensive SEO data from a single URL.
    Uses existing fetch_html logic with JS detection and Selenium fallback.

    failure_info: optional dict, populated in place with
    {failure_type, error_message, http_status_code} whenever this call
    returns None. Purely additive and backward compatible — every existing
    caller that omits this argument sees identical behavior/return values
    to before; only a caller that passes a dict gets failure detail.
    """
    try:
        # Use existing fetch_html function (includes JS detection and Selenium fallback)
        html, status_code, response_time, response_headers, _final_url = fetch_html(url, timeout=30)

        if status_code != 200 or not html:
            if failure_info is not None:
                if status_code and status_code != 200:
                    failure_info["failure_type"] = "NON_200_STATUS"
                    failure_info["http_status_code"] = status_code
                    failure_info["error_message"] = f"HTTP {status_code}"
                else:
                    failure_info["failure_type"] = "EMPTY_RESPONSE"
                    failure_info["http_status_code"] = status_code
                    failure_info["error_message"] = "Empty or missing HTML content"
            return None

        # Extract SEO data (including intelligence layer)
        seo_data = extract_comprehensive_seo_data(html, url, response_headers)
        seo_data["http_status_code"] = status_code
        seo_data["response_time_ms"] = response_time

        # Store raw HTML if size is acceptable
        if html and len(html.encode("utf-8")) < 15 * 1024 * 1024:
            seo_data["raw_html"] = html

        return seo_data

    except Exception as e:
        if failure_info is not None:
            failure_info.update(_classify_fetch_exception(e))
            failure_info.setdefault("http_status_code", None)
        return None
