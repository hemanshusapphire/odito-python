"""Fallback strategies for robots.txt and sitemap.xml when direct access is blocked."""

import requests
import re
from urllib.parse import urlparse, urljoin
from config.config import USER_AGENTS
import random


def fetch_homepage_for_sitemap_hints(domain: str) -> dict:
    """
    Fetch homepage and look for sitemap references in HTML.
    
    Args:
        domain: Domain with protocol (e.g., https://example.com)
        
    Returns:
        dict with keys: found_sitemaps, robots_content
    """
    result = {
        "found_sitemaps": [],
        "robots_content": ""
    }
    
    try:
        # Extract hostname from domain
        parsed = urlparse(domain)
        hostname = parsed.hostname
        
        if not hostname:
            return result
            
        homepage_url = f"https://{hostname}/"
        
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0"
        }
        
        response = requests.get(homepage_url, headers=headers, timeout=10, allow_redirects=True)
        
        if response.status_code == 200:
            html_content = response.text
            
            # Look for sitemap references in various formats
            sitemap_patterns = [
                r'<link[^>]+rel=["\']sitemap["\'][^>]+href=["\']([^"\']+)["\']',
                r'<link[^>]+href=["\']([^"\']*sitemap[^"\']*)["\'][^>]+rel=["\']sitemap["\']',
                r'href=["\']([^"\']*sitemap\.xml[^"\']*)["\']',
                r'href=["\']([^"\']*robots\.txt[^"\']*)["\']'
            ]
            
            for pattern in sitemap_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    # Convert relative URLs to absolute
                    if match.startswith('/'):
                        absolute_url = f"https://{hostname}{match}"
                    elif not match.startswith(('http://', 'https://')):
                        absolute_url = urljoin(homepage_url, match)
                    else:
                        absolute_url = match
                    
                    if absolute_url not in result["found_sitemaps"]:
                        result["found_sitemaps"].append(absolute_url)
            
            # Look for robots.txt content in meta tags or comments
            robots_patterns = [
                r'<meta[^>]+name=["\']robots["\'][^>]+content=["\']([^"\']+)["\']',
                r'<!--\s*robots:\s*([^>]+)\s*-->',
                r'/\*\s*robots:\s*([^*]+)\s*\*/'
            ]
            
            robots_lines = []
            for pattern in robots_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    robots_lines.append(f"# Found in HTML: {match.strip()}")
            
            if robots_lines:
                result["robots_content"] = "\n".join(robots_lines)
            
            print(f"✅ Fallback: Found {len(result['found_sitemaps'])} sitemap references in homepage")
            
    except Exception as e:
        print(f"⚠️ Fallback: Failed to fetch homepage | error={str(e)}")
    
    return result


def try_common_sitemap_urls(domain: str) -> list:
    """
    Try common sitemap URL patterns when direct /sitemap.xml is blocked.
    
    Args:
        domain: Domain with protocol (e.g., https://example.com)
        
    Returns:
        list of accessible sitemap URLs
    """
    found_sitemaps = []
    
    try:
        parsed = urlparse(domain)
        hostname = parsed.hostname
        
        if not hostname:
            return found_sitemaps
            
        base_url = f"https://{hostname}"
        
        # Common sitemap patterns
        common_patterns = [
            "/sitemap.xml",
            "/sitemap_index.xml", 
            "/sitemaps.xml",
            "/sitemap/sitemap.xml",
            "/wp-sitemap.xml",
            "/sitemap1.xml",
            "/sitemap_news.xml",
            "/sitemap_image.xml",
            "/sitemap_video.xml",
            "/sitemap_mobile.xml",
            "/sitemap-products.xml",
            "/sitemap-categories.xml",
            "/sitemap-posts.xml",
            "/sitemap-pages.xml",
            "/sitemap-tags.xml",
            "/sitemap-authors.xml",
            "/sitemap-archives.xml",
            "/sitemap-custom.xml",
            "/robots.txt"  # Also try robots.txt as fallback
        ]
        
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
        
        for pattern in common_patterns:
            try:
                test_url = f"{base_url}{pattern}"
                response = requests.get(test_url, headers=headers, timeout=5, allow_redirects=True)
                
                if response.status_code == 200:
                    # Quick validation that it's actually XML or robots.txt content
                    content = response.text.strip()
                    if (content.startswith('<?xml') or 
                        '<urlset' in content.lower() or 
                        '<sitemapindex' in content.lower() or
                        'User-agent:' in content or
                        'Disallow:' in content or
                        'Allow:' in content):
                        
                        found_sitemaps.append(test_url)
                        print(f"✅ Fallback: Found accessible {pattern} | url={test_url}")
                        
                        # Don't try too many - stop after first few successes
                        if len(found_sitemaps) >= 3:
                            break
                            
            except Exception:
                # Silently continue for each pattern
                continue
                
    except Exception as e:
        print(f"⚠️ Fallback: Error trying common sitemap patterns | error={str(e)}")
    
    return found_sitemaps
