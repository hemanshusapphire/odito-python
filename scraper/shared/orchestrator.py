"""Orchestrator for coordinating SEO data extraction pipeline."""

from datetime import datetime

# Third-party imports
from bs4 import BeautifulSoup

# Local imports
from .fetcher import fetch_html
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
        
        # 10. SEO INTELLIGENCE (advanced analysis across 7 groups)
        # Uses a fresh soup to avoid issues with decomposed elements from content analysis
        intelligence_soup = BeautifulSoup(html, "lxml")
        seo_data["seo_intelligence"] = extract_seo_intelligence(
            html, intelligence_soup, seo_data, response_headers or {}, base_url
        )
        
        # 11. ENHANCED SEO SIGNALS (structured raw data extraction)
        # Uses original soup to maintain DOM context for positioning analysis
        seo_data["enhanced_signals"] = extract_enhanced_seo_signals(soup, html, base_url, response_headers or {})
        
        return seo_data
        
    except Exception as e:
        return {
            "url": base_url,
            "error": str(e),
            "scraped_at": datetime.utcnow().isoformat(),
            "extraction_status": "FAILED"
        }


def scrape_page_data(url: str) -> dict:
    """
    Scrape comprehensive SEO data from a single URL.
    Uses existing fetch_html logic with JS detection and Selenium fallback.
    """
    try:
        # Use existing fetch_html function (includes JS detection and Selenium fallback)
        html, status_code, response_time, response_headers = fetch_html(url, timeout=10)
        
        if status_code != 200 or not html:
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
        return None
