"""Robots.txt fetcher for domain-level technical data collection."""

import requests
import random
from urllib.parse import urlparse
from config.config import USER_AGENTS


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
        
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/plain, */*"
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
            else:
                print(f"⚠️ robots.txt returned status {response.status_code} | url={robots_url}")
                # Store the status but continue trying other hostnames
                if result["status"] is None:
                    result["status"] = response.status_code
                
        except requests.exceptions.Timeout:
            print(f"⚠️ robots.txt request timed out | url={robots_url}")
            if result["status"] is None:
                result["status"] = 0
        except requests.exceptions.ConnectionError as e:
            print(f"⚠️ robots.txt connection error | url={robots_url} | error={str(e)}")
            if result["status"] is None:
                result["status"] = 0
        except Exception as e:
            print(f"⚠️ robots.txt fetch failed | url={robots_url} | error={str(e)}")
            if result["status"] is None:
                result["status"] = 0
    
    # If we get here, no robots.txt was found on any hostname variation
    print(f"⚠️ robots.txt not found on any hostname variation | tried={hostnames_to_try}")
    
    return result
