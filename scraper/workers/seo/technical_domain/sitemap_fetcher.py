"""Sitemap.xml fetcher for domain-level technical data collection."""

import requests
import random
import time
import re
from urllib.parse import urlparse
from config.config import USER_AGENTS
from .fallback_fetcher import fetch_homepage_for_sitemap_hints, try_common_sitemap_urls
from .sitemap_parser import count_urls_in_sitemap, validate_sitemap_content


def fetch_sitemap(domain: str) -> dict:
    """
    Fetch /sitemap.xml for the given domain.
    Tries both the original domain and www variation if needed.
    
    Returns:
        dict with keys: status, exists, content, url_count
    """
    result = {
        "status": None,
        "exists": False,
        "content": "",
        "url_count": 0
    }
    
    # Extract hostname from domain
    parsed = urlparse(domain)
    hostname = parsed.hostname
    
    if not hostname:
        print(f"⚠️ sitemap fetch failed - invalid hostname | domain={domain}")
        result["status"] = 0
        return result
    
    # Try both hostname variations
    hostnames_to_try = [hostname]
    
    # Add www variation if not already present
    if not hostname.startswith('www.'):
        hostnames_to_try.append(f"www.{hostname}")
    elif hostname.startswith('www.'):
        # If original has www, also try without www
        base_hostname = hostname[4:]  # Remove 'www.'
        hostnames_to_try.append(base_hostname)
    
    # Remove duplicates while preserving order
    seen = set()
    hostnames_to_try = [h for h in hostnames_to_try if not (h in seen or seen.add(h))]
    
    for try_hostname in hostnames_to_try:
        sitemap_url = f"https://{try_hostname}/sitemap.xml"
        
        # Retry logic for 429 status codes
        max_retries = 3
        for attempt in range(max_retries):
            try:
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
                
                response = requests.get(sitemap_url, headers=headers, timeout=10, allow_redirects=True)
                
                if response.status_code == 200:
                    content = response.text
                    
                    # Validate content is actually a sitemap
                    if not validate_sitemap_content(content):
                        print(f"⚠️ Content doesn't appear to be a valid sitemap | url={sitemap_url}")
                        if result["status"] is None:
                            result["status"] = response.status_code
                        break
                    
                    # Use recursive parser for accurate URL counting
                    url_count = count_urls_in_sitemap(sitemap_url)
                    
                    result.update({
                        "status": response.status_code,
                        "exists": True,
                        "content": content[:100000],  # Cap at 100KB to avoid huge sitemaps
                        "url_count": url_count
                    })
                    
                    print(f"✅ sitemap.xml found | url={sitemap_url} | size={len(content)} bytes | urls={result['url_count']}")
                    return result
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(1, 3)  # Better exponential backoff with jitter
                        print(f"⚠️ sitemap.xml rate limited (429) | url={sitemap_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ sitemap.xml rate limited after retries | url={sitemap_url}")
                        if result["status"] is None:
                            result["status"] = 429
                        break
                else:
                    print(f"⚠️ sitemap.xml returned status {response.status_code} | url={sitemap_url}")
                    # Store the status but continue trying other hostnames
                    if result["status"] is None:
                        result["status"] = response.status_code
                    break  # Don't retry non-429 errors
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt + random.uniform(0.5, 1.5)
                    print(f"⚠️ sitemap.xml request timed out | url={sitemap_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"⚠️ sitemap.xml request timed out after retries | url={sitemap_url}")
                    if result["status"] is None:
                        result["status"] = 0
                    break
            except requests.exceptions.ConnectionError as e:
                print(f"⚠️ sitemap.xml connection error | url={sitemap_url} | error={str(e)}")
                if result["status"] is None:
                    result["status"] = 0
                break  # Don't retry connection errors
            except Exception as e:
                print(f"⚠️ sitemap.xml fetch failed | url={sitemap_url} | error={str(e)}")
                if result["status"] is None:
                    result["status"] = 0
                break
    
    # If we get here, no sitemap was found on any hostname variation
    if result["status"] == 429:
        print(f"❌ sitemap.xml blocked (429 rate limited) on all hostname variations | trying fallback")
        # Try fallback strategies
        fallback_result = fetch_homepage_for_sitemap_hints(domain)
        common_sitemaps = try_common_sitemap_urls(domain)
        
        # Combine fallback results
        all_sitemap_urls = fallback_result["found_sitemaps"] + common_sitemaps
        
        if all_sitemap_urls:
            # Try to fetch the first found sitemap
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
            
            for sitemap_url in all_sitemap_urls[:3]:  # Try first 3 found
                try:
                    response = requests.get(sitemap_url, headers=headers, timeout=10, allow_redirects=True)
                    if response.status_code == 200:
                        content = response.text
                        
                        # Validate content is actually a sitemap
                        if not validate_sitemap_content(content):
                            print(f"⚠️ Fallback content doesn't appear to be a valid sitemap | url={sitemap_url}")
                            continue
                        
                        # Use recursive parser for accurate URL counting
                        url_count = count_urls_in_sitemap(sitemap_url)
                        
                        result.update({
                            "status": response.status_code,
                            "exists": True,
                            "content": content[:100000],
                            "url_count": url_count
                        })
                        print(f"✅ sitemap.xml found via fallback | url={sitemap_url} | size={len(content)} bytes | urls={result['url_count']}")
                        return result
                except Exception:
                    continue
            
            print(f"❌ sitemap.xml fallback failed | found {len(all_sitemap_urls)} candidates but none accessible")
        else:
            print(f"❌ sitemap.xml fallback failed | no sitemap references found in homepage or common patterns")
    else:
        print(f"⚠️ sitemap.xml not found on any hostname variation | tried={hostnames_to_try}")
    
    return result
