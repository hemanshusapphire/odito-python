"""SEO data extraction functionality."""

import os
import re
import json
from datetime import datetime
from urllib.parse import urljoin

# Third-party imports
from bs4 import BeautifulSoup

# Local imports
from config.config import (
    CRITICAL_META_TAGS, CONTENT_SELECTORS, INTERNATIONAL_SIGNALS,
    FAQ_INDICATORS, TESTIMONIAL_INDICATORS, SOCIAL_DOMAINS_FOR_SCHEMA,
    LOCALBUSINESS_REQUIRED_FIELDS, OTHER_TRACKING_INDICATORS,
    SOCIAL_MEDIA_INDICATORS
)
from .utils import clean_text


def extract_page_signals(html: str, soup: BeautifulSoup) -> tuple:
    """
    Extract critical page signals.
    
    Returns:
        tuple: (top_level_signals, tracking_updates)
        - top_level_signals: dict with review_schema_present, doctype_present, theme_color_present, hreflang_present
        - tracking_updates: dict with analytics_detected, analytics_types, facebook_pixel to merge into tracking object
                            Also includes backward compatibility fields (google_analytics, google_tag_manager)
    """
    top_level_signals = {
        "review_schema_present": False,
        "doctype_present": False,
        "theme_color_present": False,
        "hreflang_present": False
    }
    
    tracking_updates = {
        "analytics_detected": False,
        "analytics_types": [],
        "facebook_pixel": False,
        "google_analytics": False,  # Backward compatibility with rule layer
        "google_tag_manager": False  # Backward compatibility with rule layer
    }
    
    try:
        html_lower = html.lower() if html else ""
        
        # 1. DOCTYPE detection (check first non-empty line of raw HTML)
        try:
            for line in html.split('\n'):
                stripped = line.strip()
                if stripped:
                    if stripped.lower().startswith('<!doctype'):
                        top_level_signals["doctype_present"] = True
                    break
        except:
            pass
        
        # 2. Review Schema detection (Review + AggregateRating in JSON-LD)
        try:
            for script in soup.find_all("script", type="application/ld+json"):
                if script.string:
                    try:
                        schema_data = json.loads(script.string)
                        # Handle both dict and list JSON-LD structures
                        schemas_to_check = [schema_data] if isinstance(schema_data, dict) else (schema_data if isinstance(schema_data, list) else [])
                        
                        for schema in schemas_to_check:
                            if isinstance(schema, dict):
                                schema_type = schema.get("@type", "")
                                if schema_type in ["Review", "AggregateRating"]:
                                    top_level_signals["review_schema_present"] = True
                                    break
                        
                        if top_level_signals["review_schema_present"]:
                            break
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
        except:
            pass
        
        # 3. Analytics detection (GA4, UA, GTM) - updates tracking object
        try:
            analytics_types = []
            
            # GA4 detection: gtag( AND G- pattern
            if re.search(r'gtag\s*\(', html_lower) and re.search(r'G-[A-Z0-9]{10,}', html, re.IGNORECASE):
                analytics_types.append("GA4")
            # GA4 script pattern
            elif re.search(r'googletagmanager\.com/gtag/js\?id=G-', html_lower):
                analytics_types.append("GA4")
            
            # Universal Analytics detection: UA- pattern
            if re.search(r'UA-\d{4,10}-\d{1,5}', html):
                if "UA" not in analytics_types:
                    analytics_types.append("UA")
            
            # Google Tag Manager detection: GTM or googletagmanager.com
            if re.search(r'GTM-[A-Z0-9]+', html, re.IGNORECASE) or 'googletagmanager.com' in html_lower or 'datalayer' in html_lower:
                if "GTM" not in analytics_types:
                    analytics_types.append("GTM")
            
            if analytics_types:
                tracking_updates["analytics_detected"] = True
                tracking_updates["analytics_types"] = analytics_types
                # Set old field names for backward compatibility with rule layer
                tracking_updates["google_analytics"] = True
                if "GTM" in analytics_types:
                    tracking_updates["google_tag_manager"] = True
        except:
            pass
        
        # 4. Theme Color detection (meta name="theme-color")
        try:
            theme_meta = soup.find("meta", attrs={"name": re.compile(r"^theme-color$", re.IGNORECASE)})
            if theme_meta and theme_meta.get("content"):
                top_level_signals["theme_color_present"] = True
        except:
            pass
        
        # 5. Hreflang detection (link rel="alternate" hreflang)
        try:
            hreflang_links = soup.find_all("link", rel=re.compile(r"alternate", re.IGNORECASE), attrs={"hreflang": True})
            if hreflang_links:
                top_level_signals["hreflang_present"] = True
        except:
            pass
        
        # 6. Facebook Pixel detection (connect.facebook.net OR fbq( OR facebook.com/tr) - updates tracking object
        try:
            if ('connect.facebook.net' in html_lower or 
                re.search(r'fbq\s*\(', html_lower) or 
                'facebook.com/tr' in html_lower):
                tracking_updates["facebook_pixel"] = True
        except:
            pass
        
    except Exception as e:
        # Return defaults if any unexpected error
        pass
    
    return top_level_signals, tracking_updates



def extract_head_and_meta_data(soup: BeautifulSoup, seo_data: dict):
    """Extract doctype, language, title, and all meta tags with validation."""
    # Doctype detection with validation
    doctype = None
    if soup.doctype:
        doctype = str(soup.doctype).strip()
        # Doctype data extracted
    else:
        # Doctype missing data extracted
        pass
    
    # HTML language
    html_lang = soup.html.get("lang") if soup.html else None
    # Language data extracted
    
    # Title
    title = soup.title.string.strip() if soup.title else None
    # Title data extracted
    
    # Enhanced meta tags collection with validation
    meta_tags = {}
    found_critical = []
    
    for meta in soup.find_all("meta"):
        key = meta.get("name") or meta.get("property") or meta.get("http-equiv") or meta.get("charset")
        value = meta.get("content")
        
        if key and value:
            key = key.lower()
            if key not in meta_tags:
                meta_tags[key] = []
            meta_tags[key].append(value.strip())
            
            # Track critical meta tags
            if key in CRITICAL_META_TAGS:
                found_critical.append(key)
    
    # Check for missing critical meta tags
    missing_critical = set(CRITICAL_META_TAGS) - set(found_critical)
    # Missing meta tags data extracted
    
    # Meta description data extracted
    
    # Canonical URL
    canonical = None
    canonical_link = soup.find("link", rel="canonical")
    if canonical_link:
        canonical = canonical_link.get("href")
        # Canonical URL data extracted
    else:
        # Canonical missing data extracted
        pass
    
    # Store in seo_data
    seo_data.update({
        "doctype": doctype,
        "html_lang": html_lang,
        "title": title,
        "meta_tags": meta_tags,
        "canonical": canonical
    })


def extract_social_media_data(soup: BeautifulSoup, seo_data: dict):
    """Extract social media meta tags (Open Graph, Twitter, Facebook, Pinterest)."""
    meta_tags = seo_data.get("meta_tags", {})
    
    # Open Graph
    open_graph = {k: v for k, v in meta_tags.items() if k.startswith("og:")}
    
    # Twitter Card
    twitter = {k: v for k, v in meta_tags.items() if k.startswith("twitter:")}
    
    # Facebook-specific
    facebook = {k: v for k, v in meta_tags.items() if k.startswith("fb:")}
    
    # Pinterest
    pinterest = {k: v for k, v in meta_tags.items() if k.startswith("pin:")}
    
    seo_data.update({
        "social": {
            "open_graph": open_graph,
            "twitter": twitter,
            "facebook": facebook,
            "pinterest": pinterest
        }
    })


def extract_internationalization_data(soup: BeautifulSoup, seo_data: dict):
    """Extract and validate internationalization data (hreflang links)."""
    hreflangs = []
    hreflang_links = soup.find_all("link", rel="alternate", hreflang=True)
    
    # Extract all hreflang links
    for link in hreflang_links:
        hreflangs.append({
            "lang": link["hreflang"],
            "url": link["href"]
        })
    
    # Validate hreflang implementation
    if hreflangs:
        languages = [h["lang"] for h in hreflangs]
        urls = [h["url"] for h in hreflangs]
        
        # Check for self-referencing hreflang
        page_url = seo_data.get("url", "")
        has_self_reference = any(url.startswith(page_url.rstrip("/")) or page_url.startswith(url.rstrip("/")) for url in urls)
        
        # Hreflang self-reference data extracted
        
        # X-default hreflang data extracted
        
        # Duplicate languages data extracted
        
        # Validate language codes
        invalid_langs = []
        for lang in languages:
            if lang != "x-default" and not re.match(r'^[a-z]{2}(-[A-Z]{2})?$', lang):
                invalid_langs.append(lang)
        
        # Invalid language codes data extracted
        
        # Check canonical vs hreflang mismatch
        canonical = seo_data.get("canonical")
        if canonical and canonical not in urls:
            # Canonical mismatch found
            pass
        else:
            # No canonical mismatch found
            pass
    
    else:
        # Check if internationalization is needed but missing
        content_text = soup.get_text().lower()
        has_international_signals = any(signal in content_text for signal in INTERNATIONAL_SIGNALS)
        
        # International signals data extracted
        pass
    
    seo_data["hreflangs"] = hreflangs


def extract_visual_branding_data(soup: BeautifulSoup, seo_data: dict):
    """Extract visual branding elements (theme color, apple icons)."""
    meta_tags = seo_data.get("meta_tags", {})
    
    # Theme color
    theme_color = meta_tags.get("theme-color", [None])[0] if meta_tags.get("theme-color") and meta_tags.get("theme-color") else None
    
    # Apple touch icons
    apple_icons = []
    for icon in soup.find_all("link", rel="apple-touch-icon"):
        apple_icons.append({
            "sizes": icon.get("sizes"),
            "href": icon.get("href")
        })
    
    seo_data.update({
        "visual_branding": {
            "theme_color": theme_color,
            "apple_icons": apple_icons
        }
    })


def extract_content_analysis(soup: BeautifulSoup, seo_data: dict):
    """Extract and validate content structure (headings, text content)."""
    # Extract all heading levels (h1-h6)
    headings = {}
    heading_texts = {}
    
    for i in range(1, 7):
        tag = f"h{i}"
        headings[tag] = []
        heading_texts[tag] = []
        
        for h in soup.find_all(tag):
            text = h.get_text(strip=True)
            if text:
                headings[tag].append(text)
                heading_texts[tag].append(text)
    
    # Validate heading hierarchy
    h1_count = len(headings.get("h1", []))
    h2_texts = headings.get("h2", [])
    all_headings_flat = []
    
    # Flatten all headings for analysis
    for level in range(1, 7):
        tag = f"h{level}"
        all_headings_flat.extend([(level, text) for text in headings.get(tag, [])])
    
    # H1 tags data extracted
    
    # Check H1 length
    h1_text = headings.get("h1", [""])[0] if headings.get("h1") else ""
    # H1 length data extracted
    
    # Check for duplicate H2 tags
    if h2_texts:
        duplicate_h2s = []
        seen_h2s = set()
        for h2_text in h2_texts:
            if h2_text in seen_h2s:
                duplicate_h2s.append(h2_text)
            else:
                seen_h2s.add(h2_text)
        
        # Duplicate H2 tags data extracted
        pass
    
    # Check for skipped heading levels
    if all_headings_flat:
        previous_level = 0
        skipped_levels = []
        
        for level, text in all_headings_flat:
            if previous_level > 0:
                # Check if we skipped any levels (e.g., H2 -> H4)
                if level > previous_level + 1:
                    skipped_levels.extend(range(previous_level + 1, level))
            previous_level = level
        
        # Skipped heading levels data extracted
        pass
    
    # Check for heading structure quality
    if h1_count == 1 and len(h2_texts) == 0:
        # Page has H1 but no H2s - might need better structure
        content_length = len(soup.get_text())
        # Heading structure quality data extracted
        pass
    
    # Remove script and style elements
    for element in soup(["script", "style"]):
        element.decompose()
    
    # Get clean text content
    text_content = soup.get_text(strip=True, separator=' ')
    
    seo_data.update({
        "content": {
            "headings": headings,
            "text": text_content,
            "word_count": len(text_content.split()) if text_content else 0,
            "heading_analysis": {
                "h1_count": h1_count,
                "h2_count": len(h2_texts),
                "total_headings": len(all_headings_flat),
                "has_proper_hierarchy": h1_count == 1 and len(skipped_levels) == 0 if all_headings_flat else False
            }
        }
    })


def extract_image_data(soup: BeautifulSoup, base_url: str, seo_data: dict):
    """Extract ALL images with comprehensive SEO analysis."""
    images = []
    total_images = 0
    images_without_alt = 0
    images_with_empty_alt = 0
    images_with_poor_alt = 0
    missing_dimensions = 0
    
    for img in soup.find_all("img", src=True):
        total_images += 1
        src = img.get("src")
        if not src:
            continue
            
        # Convert to absolute URL
        if not src.startswith("http"):
            src = urljoin(base_url, src)
        
        # Extract file extension
        path = src.split('?')[0]  # Remove query parameters
        ext = os.path.splitext(path)[1].lower().replace('.', '')
        
        # Alt text analysis
        alt_text = img.get("alt", "")
        alt_quality = "good"
        
        if not alt_text:
            images_without_alt += 1
            alt_quality = "missing"
        elif alt_text.strip() == "":
            images_with_empty_alt += 1
            alt_quality = "empty"
        elif len(alt_text) < 3 or alt_text.lower() in ["image", "picture", "img", "photo"]:
            images_with_poor_alt += 1
            alt_quality = "poor"
        
        # Dimension analysis
        has_width = bool(img.get("width"))
        has_height = bool(img.get("height"))
        
        if not has_width or not has_height:
            missing_dimensions += 1
        
        # Loading analysis
        loading = img.get("loading", "")
        decoding = img.get("decoding", "")
        
        # Check for potential CLS issues (images without dimensions)
        cls_risk = not has_width or not has_height
        
        images.append({
            "src": src,
            "alt": alt_text,
            "alt_quality": alt_quality,
            "title": img.get("title"),
            "extension": ext,
            "width": img.get("width"),
            "height": img.get("height"),
            "loading": loading,
            "decoding": decoding,
            "cls_risk": cls_risk,
            "has_dimensions": has_width and has_height,
            "is_decorative": alt_text == "" and has_width and has_height
        })
    
    # Calculate alt text metrics
    alt_text_percentage = ((total_images - images_without_alt - images_with_empty_alt) / total_images * 100) if total_images > 0 else 100
    
    # Add image SEO issues
    # Image alt text analysis data extracted
    
    # Image empty alt text data extracted
    
    # Image poor alt text data extracted
    
    # Image dimensions data extracted
    
    # Check for lazy loading optimization
    images_without_lazy = sum(1 for img in images if not img.get("loading") and img.get("src"))
    # Image lazy loading data extracted
    
    seo_data["images"] = images
    seo_data["image_analysis"] = {
        "total_images": total_images,
        "images_with_alt": total_images - images_without_alt - images_with_empty_alt,
        "alt_text_percentage": round(alt_text_percentage, 1),
        "images_without_alt": images_without_alt,
        "images_with_empty_alt": images_with_empty_alt,
        "images_with_poor_alt": images_with_poor_alt,
        "missing_dimensions": missing_dimensions,
        "cls_risk_images": sum(1 for img in images if img["cls_risk"]),
        "decorative_images": sum(1 for img in images if img["is_decorative"])
    }


def extract_tracking_data(soup: BeautifulSoup, seo_data: dict):
    """Extract and validate tracking pixels and analytics setup."""
    tracking = {
        "google_analytics": False,
        "google_tag_manager": False,
        "facebook_pixel": False,
        "linkedin_pixel": False,
        "other_tracking": [],
        "analytics_detected": False,
        "analytics_types": []
    }
    
    # Get full page HTML for comprehensive searching
    page_html = str(soup)
    page_html_lower = page_html.lower()
    scripts = soup.find_all("script")
    
    # Compile patterns for detection
    # Google Analytics 4 patterns
    ga4_patterns = [
        re.compile(r'googletagmanager\.com/gtag/js\?id=G-[A-Z0-9]+', re.IGNORECASE),
        re.compile(r'gtag\([\'"]config[\'"],\s*[\'"]G-[A-Z0-9]+[\'"]', re.IGNORECASE),
        re.compile(r'G-[A-Z0-9]{10,}', re.IGNORECASE),  # GA4 property ID pattern
        re.compile(r'gtag\s*\(', re.IGNORECASE),          # gtag function call
    ]
    
    # Universal Analytics patterns
    ua_patterns = [
        re.compile(r'googletagmanager\.com/ga\.js', re.IGNORECASE),
        re.compile(r'UA-\d{4,10}-\d{1,5}', re.IGNORECASE),  # UA property ID
        re.compile(r'_gaq\.push', re.IGNORECASE),           # Classic GA push
        re.compile(r'_setAccount', re.IGNORECASE),          # Classic GA account
    ]
    
    # Google Tag Manager patterns
    gtm_patterns = [
        re.compile(r'googletagmanager\.com/ns\.html\?id=GTM-[A-Z0-9]+', re.IGNORECASE),
        re.compile(r'googletagmanager\.com/gtm\.js\?id=GTM-[A-Z0-9]+', re.IGNORECASE),
        re.compile(r'GTM-[A-Z0-9]+', re.IGNORECASE),       # GTM property ID
        re.compile(r'dataLayer', re.IGNORECASE),           # GTM dataLayer
    ]
    
    # Facebook Pixel patterns
    fb_patterns = [
        re.compile(r'facebook\.com/tr\?id=[0-9]+', re.IGNORECASE),
        re.compile(r'fbq\([\'"]init[\'"],\s*[\'"][0-9]+[\'"]', re.IGNORECASE),
        re.compile(r'connect\.facebook\.net.*fbevents\.js', re.IGNORECASE)
    ]
    
    # LinkedIn Pixel patterns
    li_patterns = [
        re.compile(r'linkedin\.com/insight-tag.*pid=[0-9]+', re.IGNORECASE),
        re.compile(r'lintrk\([\'"]track[\'"]', re.IGNORECASE),
        re.compile(r'platform\.linkedin\.com.*insight\.js', re.IGNORECASE)
    ]
    
    # Process all script tags
    for script in scripts:
        script_content = script.string or str(script)
        script_content_lower = script_content.lower()
        
        # Check GA4
        for pattern in ga4_patterns:
            if pattern.search(script_content):
                tracking["google_analytics"] = True
                if "analytics_types" not in tracking or "GA4" not in tracking["analytics_types"]:
                    tracking["analytics_types"].append("GA4")
                break
        
        # Check Universal Analytics
        for pattern in ua_patterns:
            if pattern.search(script_content):
                tracking["google_analytics"] = True
                if "analytics_types" not in tracking or "UA" not in tracking["analytics_types"]:
                    tracking["analytics_types"].append("UA")
                break
        
        # Check GTM
        for pattern in gtm_patterns:
            if pattern.search(script_content):
                tracking["google_tag_manager"] = True
                if "analytics_types" not in tracking or "GTM" not in tracking["analytics_types"]:
                    tracking["analytics_types"].append("GTM")
                break
        
        # Check Facebook
        for pattern in fb_patterns:
            if pattern.search(script_content):
                tracking["facebook_pixel"] = True
                break
        
        # Check LinkedIn
        for pattern in li_patterns:
            if pattern.search(script_content):
                tracking["linkedin_pixel"] = True
                break
    
    # Also check full HTML for common patterns not in scripts
    # This catches GA/GTM injected via attributes, meta tags, or other locations
    if not tracking["google_analytics"]:
        # Check for GA4 property IDs anywhere
        if re.search(r'G-[A-Z0-9]{10,}', page_html, re.IGNORECASE):
            tracking["google_analytics"] = True
            if "GA4" not in tracking["analytics_types"]:
                tracking["analytics_types"].append("GA4")
        # Check for UA property IDs anywhere
        elif re.search(r'UA-\d{4,10}-\d{1,5}', page_html, re.IGNORECASE):
            tracking["google_analytics"] = True
            if "UA" not in tracking["analytics_types"]:
                tracking["analytics_types"].append("UA")
    
    if not tracking["google_tag_manager"]:
        # Check for GTM identifiers or dataLayer
        if re.search(r'GTM-[A-Z0-9]+', page_html, re.IGNORECASE) or 'datalayer' in page_html_lower:
            tracking["google_tag_manager"] = True
            if "GTM" not in tracking["analytics_types"]:
                tracking["analytics_types"].append("GTM")
    
    # Detect other tracking scripts
    for indicator in OTHER_TRACKING_INDICATORS:
        if indicator.lower() in page_html_lower:
            if indicator not in tracking["other_tracking"]:
                tracking["other_tracking"].append(indicator)
    
    # Set combined analytics_detected flag for easy frontend checking
    tracking["analytics_detected"] = (
        tracking["google_analytics"] or 
        tracking["google_tag_manager"] or 
        tracking["facebook_pixel"] or 
        tracking["linkedin_pixel"]
    )
    
    # Remove duplicates from analytics_types
    tracking["analytics_types"] = list(set(tracking["analytics_types"]))
    
    # Additional validation checks
    if not tracking["facebook_pixel"]:
        # Check if there's any social media presence that would benefit from FB tracking
        content_text = soup.get_text().lower()
        has_social_content = any(indicator in content_text for indicator in SOCIAL_MEDIA_INDICATORS)
    
    # Check for conversion event tracking
    has_conversion_forms = bool(soup.find_all("form"))
    has_conversion_buttons = bool(soup.find_all("button"))
    
    seo_data["tracking"] = tracking
