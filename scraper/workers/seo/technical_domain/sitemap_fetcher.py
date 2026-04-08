"""Sitemap.xml fetcher for domain-level technical data collection."""

import requests
import random
import re
from urllib.parse import urlparse
from config.config import USER_AGENTS


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
        
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/xml, text/xml, */*"
            }
            
            response = requests.get(sitemap_url, headers=headers, timeout=10, allow_redirects=True)
            
            if response.status_code == 200:
                content = response.text
                result.update({
                    "status": response.status_code,
                    "exists": True,
                    "content": content[:100000],  # Cap at 100KB to avoid huge sitemaps
                    "url_count": len(re.findall(r'<loc>', content, re.IGNORECASE))
                })
                
                print(f"✅ sitemap.xml found | url={sitemap_url} | size={len(content)} bytes | urls={result['url_count']}")
                return result
            else:
                print(f"⚠️ sitemap.xml returned status {response.status_code} | url={sitemap_url}")
                # Store the status but continue trying other hostnames
                if result["status"] is None:
                    result["status"] = response.status_code
                
        except requests.exceptions.Timeout:
            print(f"⚠️ sitemap.xml request timed out | url={sitemap_url}")
            if result["status"] is None:
                result["status"] = 0
        except requests.exceptions.ConnectionError as e:
            print(f"⚠️ sitemap.xml connection error | url={sitemap_url} | error={str(e)}")
            if result["status"] is None:
                result["status"] = 0
        except Exception as e:
            print(f"⚠️ sitemap.xml fetch failed | url={sitemap_url} | error={str(e)}")
            if result["status"] is None:
                result["status"] = 0
    
    # If we get here, no sitemap was found on any hostname variation
    print(f"⚠️ sitemap.xml not found on any hostname variation | tried={hostnames_to_try}")
    
    return result
