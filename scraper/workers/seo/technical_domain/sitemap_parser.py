"""Advanced sitemap parser with recursive index handling and namespace support."""

import xml.etree.ElementTree as ET
import requests
import re
import time
import random
import gzip
try:
    import brotli
    HAS_BROTLI = True
except ImportError:
    HAS_BROTLI = False
from urllib.parse import urlparse, urljoin
from config.config import USER_AGENTS


def parse_sitemap_recursive(sitemap_url: str, max_depth: int = 3, current_depth: int = 0) -> dict:
    """
    Recursively parse sitemap XML files, handling both sitemap indexes and URL sitemaps.
    
    Args:
        sitemap_url: URL of the sitemap to parse
        max_depth: Maximum recursion depth to prevent infinite loops
        current_depth: Current recursion depth
        
    Returns:
        dict with keys: urls (list), child_sitemaps (list), total_urls, is_index
    """
    if current_depth >= max_depth:
        print(f"⚠️ Max recursion depth reached | url={sitemap_url}")
        return {"urls": [], "child_sitemaps": [], "total_urls": 0, "is_index": False}
    
    result = {
        "urls": [],
        "child_sitemaps": [],
        "total_urls": 0,
        "is_index": False
    }
    
    try:
        # Fetch sitemap with proper headers
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/xml, text/xml, */*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0"
        }
        
        # Add retry logic for rate limiting
        max_retries = 3
        for attempt in range(max_retries):
            response = requests.get(sitemap_url, headers=headers, timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(1, 3)
                    print(f"⚠️ Sitemap rate limited (429) | url={sitemap_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Sitemap rate limited after retries | url={sitemap_url}")
                    return result
            else:
                print(f"⚠️ Failed to fetch sitemap | url={sitemap_url} | status={response.status_code}")
                return result
        
        if response.status_code != 200:
            return result
        
        # Handle compressed content (gzip, brotli, or plain text)
        content = response.content
        content_encoding = response.headers.get('content-encoding', '').lower()
        
        try:
            if content_encoding == 'gzip' or content.startswith(b'\x1f\x8b'):
                content = gzip.decompress(content).decode('utf-8')
                print(f"📦 Decompressed gzip content | url={sitemap_url}")
            elif content_encoding == 'br' and HAS_BROTLI:
                content = brotli.decompress(content).decode('utf-8')
                print(f"📦 Decompressed brotli content | url={sitemap_url}")
            elif content_encoding == 'br' and not HAS_BROTLI:
                print(f"⚠️ Brotli compression detected but brotli library not available | url={sitemap_url}")
                return result
            else:
                content = content.decode('utf-8')
        except Exception as e:
            # Fallback to regular text decoding
            try:
                content = response.text.strip()
            except Exception:
                print(f"⚠️ Failed to decode content | url={sitemap_url} | error={str(e)}")
                return result
        
        content = content.strip()
        
        # Parse XML with namespace handling
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            print(f"⚠️ XML parse error | url={sitemap_url} | error={str(e)}")
            return result
        
        # Handle XML namespaces
        namespace = ''
        if root.tag.startswith('{'):
            namespace = root.tag.split('}')[0] + '}'
        
        # Check if this is a sitemap index by examining the root tag
        root_tag = root.tag.lower()
        if 'sitemapindex' in root_tag:
            result["is_index"] = True
            print(f"📋 Sitemap index detected | url={sitemap_url}")
            
            # Extract child sitemap URLs
            sitemap_elements = root.findall(f".//{namespace}sitemap")
            for sitemap_elem in sitemap_elements:
                loc_elem = sitemap_elem.find(f"{namespace}loc")
                if loc_elem is not None and loc_elem.text:
                    child_url = loc_elem.text.strip()
                    if child_url not in result["child_sitemaps"]:
                        result["child_sitemaps"].append(child_url)
            
            print(f"🔗 Found {len(result['child_sitemaps'])} child sitemaps in index")
            
            # Recursively parse child sitemaps
            for child_url in result["child_sitemaps"][:10]:  # Limit to prevent excessive requests
                try:
                    # Add longer delay between child sitemap requests to avoid rate limiting
                    time.sleep(random.uniform(2, 4))
                    
                    child_result = parse_sitemap_recursive(child_url, max_depth, current_depth + 1)
                    result["urls"].extend(child_result["urls"])
                    result["total_urls"] += child_result["total_urls"]
                    
                except Exception as e:
                    print(f"⚠️ Failed to parse child sitemap | url={child_url} | error={str(e)}")
                    continue
            
            return result
        
        # This is a regular URL sitemap
        print(f"📄 URL sitemap detected | url={sitemap_url}")
        
        # Extract URLs with namespace-aware parsing
        url_elements = root.findall(f".//{namespace}url")
        
        for url_elem in url_elements:
            loc_elem = url_elem.find(f"{namespace}loc")
            if loc_elem is not None and loc_elem.text:
                url = loc_elem.text.strip()
                if url not in result["urls"]:
                    result["urls"].append(url)
        
        result["total_urls"] = len(result["urls"])
        print(f"🔗 Extracted {len(result['urls'])} URLs from sitemap")
        
        return result
        
    except Exception as e:
        print(f"⚠️ Sitemap parsing failed | url={sitemap_url} | error={str(e)}")
        return result


def count_urls_in_sitemap(sitemap_url: str) -> int:
    """
    Count total URLs in a sitemap, including recursive parsing of sitemap indexes.
    
    Args:
        sitemap_url: URL of the sitemap
        
    Returns:
        Total number of URLs found
    """
    try:
        result = parse_sitemap_recursive(sitemap_url)
        return result["total_urls"]
    except Exception as e:
        print(f"⚠️ URL counting failed | url={sitemap_url} | error={str(e)}")
        return 0


def extract_sitemap_urls(sitemap_url: str, max_urls: int = 1000) -> list:
    """
    Extract URLs from sitemap, with optional limit.
    
    Args:
        sitemap_url: URL of the sitemap
        max_urls: Maximum number of URLs to return
        
    Returns:
        List of URLs found
    """
    try:
        result = parse_sitemap_recursive(sitemap_url)
        urls = result["urls"][:max_urls]
        print(f"🔗 Extracted {len(urls)} URLs (limited to {max_urls})")
        return urls
    except Exception as e:
        print(f"⚠️ URL extraction failed | url={sitemap_url} | error={str(e)}")
        return []


def validate_sitemap_content(content) -> bool:
    """
    Validate if content looks like a proper sitemap XML.
    
    Args:
        content: XML content to validate (string or bytes)
        
    Returns:
        True if content appears to be a valid sitemap
    """
    try:
        # Handle bytes content
        if isinstance(content, bytes):
            try:
                # Try to decompress if it appears to be gzipped
                if content.startswith(b'\x1f\x8b'):
                    content = gzip.decompress(content).decode('utf-8')
                elif HAS_BROTLI and content.startswith(b'\x8b'):
                    # Try brotli decompression (common magic bytes)
                    content = brotli.decompress(content).decode('utf-8')
                else:
                    content = content.decode('utf-8')
            except Exception:
                return False
        
        content = str(content).strip()
        
        # Reject HTML content (Cloudflare pages, error pages, etc.)
        html_indicators = ['<!doctype html', '<html', '<head', '<body', 'verifying your connection', 'cloudflare']
        content_lower = content.lower()
        
        for indicator in html_indicators:
            if indicator in content_lower:
                return False
        
        # Must have core sitemap XML structure indicators
        required_indicators = ['urlset', 'sitemapindex']
        optional_indicators = ['<url>', '<sitemap>', '<loc>', 'xmlns=']
        
        has_required = any(indicator in content_lower for indicator in required_indicators)
        has_optional = any(indicator in content_lower for indicator in optional_indicators)
        
        if not has_required:
            return False
        
        # Try parsing as XML to ensure it's well-formed
        try:
            root = ET.fromstring(content)
            # Check root element is sitemap-related
            root_tag = root.tag.lower()
            if not any(tag in root_tag for tag in ['urlset', 'sitemapindex']):
                return False
            return True
        except ET.ParseError:
            return False
        
    except (ET.ParseError, UnicodeDecodeError, ValueError):
        return False
