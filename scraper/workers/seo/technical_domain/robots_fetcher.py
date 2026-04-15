"""Robots.txt fetcher for domain-level technical data collection."""

import requests
import random
import time
from urllib.parse import urlparse
from config.config import USER_AGENTS
from .fallback_fetcher import fetch_homepage_for_sitemap_hints, try_common_sitemap_urls


def fetch_robots(domain: str) -> dict:
    """
    Fetch /robots.txt for the given domain.
    Tries both the original domain and www variation if needed.
    
    Returns:
        dict with keys: status, exists, content
    """
    result = {
        "status": None,
        "exists": False,
        "content": ""
    }
    
    # Extract hostname from domain
    parsed = urlparse(domain)
    hostname = parsed.hostname
    
    if not hostname:
        print(f"⚠️ robots fetch failed - invalid hostname | domain={domain}")
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
        robots_url = f"https://{try_hostname}/robots.txt"
        
        # Retry logic for 429 status codes
        max_retries = 3
        for attempt in range(max_retries):
            try:
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/plain, */*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Cache-Control": "max-age=0"
                }
                
                response = requests.get(robots_url, headers=headers, timeout=10, allow_redirects=True)
                
                if response.status_code == 200:
                    result.update({
                        "status": response.status_code,
                        "exists": True,
                        "content": response.text[:50000]  # Cap at 50KB to avoid huge files
                    })
                    
                    print(f"✅ robots.txt found | url={robots_url} | size={len(response.text)} bytes")
                    return result
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(1, 3)  # Better exponential backoff with jitter
                        print(f"⚠️ robots.txt rate limited (429) | url={robots_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ robots.txt rate limited after retries | url={robots_url}")
                        if result["status"] is None:
                            result["status"] = 429
                        break
                else:
                    print(f"⚠️ robots.txt returned status {response.status_code} | url={robots_url}")
                    # Store the status but continue trying other hostnames
                    if result["status"] is None:
                        result["status"] = response.status_code
                    break  # Don't retry non-429 errors
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt + random.uniform(0.5, 1.5)
                    print(f"⚠️ robots.txt request timed out | url={robots_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"⚠️ robots.txt request timed out after retries | url={robots_url}")
                    if result["status"] is None:
                        result["status"] = 0
                    break
            except requests.exceptions.ConnectionError as e:
                print(f"⚠️ robots.txt connection error | url={robots_url} | error={str(e)}")
                if result["status"] is None:
                    result["status"] = 0
                break  # Don't retry connection errors
            except Exception as e:
                print(f"⚠️ robots.txt fetch failed | url={robots_url} | error={str(e)}")
                if result["status"] is None:
                    result["status"] = 0
                break
    
    # If we get here, no robots.txt was found on any hostname variation
    if result["status"] == 429:
        print(f"❌ robots.txt blocked (429 rate limited) on all hostname variations | trying fallback")
        # Try fallback strategy
        fallback_result = fetch_homepage_for_sitemap_hints(domain)
        if fallback_result["robots_content"]:
            result["content"] = fallback_result["robots_content"]
            result["exists"] = True
            result["status"] = 200  # Mark as found via fallback
            print(f"✅ robots.txt found via homepage fallback | size={len(fallback_result['robots_content'])} chars")
            return result
        else:
            print(f"❌ robots.txt fallback also failed | no robots content found in homepage")
    else:
        print(f"⚠️ robots.txt not found on any hostname variation | tried={hostnames_to_try}")
    
    return result
