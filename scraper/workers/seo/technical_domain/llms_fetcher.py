"""llms.txt fetcher for AI crawler visibility analysis."""

import requests
import random
import time
import re
from urllib.parse import urlparse
from config.config import USER_AGENTS


def parse_llms_txt_content(content: str) -> dict:
    """
    Parse llms.txt content to detect AI crawler directives and bot mentions.
    
    Args:
        content: Raw llms.txt content
        
    Returns:
        dict with parsed results
    """
    if not content:
        return {
            "hasAllow": False,
            "hasDisallow": False,
            "mentionedBots": {
                "GPTBot": False,
                "ClaudeBot": False,
                "PerplexityBot": False
            }
        }
    
    # Convert to lowercase for case-insensitive matching
    content_lower = content.lower()
    
    # Check for directives
    has_allow = bool(re.search(r'\ballow\b', content_lower))
    has_disallow = bool(re.search(r'\bdisallow\b', content_lower))
    
    # Check for bot mentions (case-insensitive, partial matches allowed)
    mentioned_bots = {
        "GPTBot": bool(re.search(r'gptbot', content_lower)),
        "ClaudeBot": bool(re.search(r'claudebot', content_lower)),
        "PerplexityBot": bool(re.search(r'perplexitybot', content_lower)),
        "GitHubCopilot": bool(re.search(r'githubcopilot|copilot', content_lower)),
        "Gemini": bool(re.search(r'gemini', content_lower))
    }
    
    return {
        "hasAllow": has_allow,
        "hasDisallow": has_disallow,
        "mentionedBots": mentioned_bots
    }


def fetch_llms_txt(domain: str) -> dict:
    """
    Fetch /llms.txt for the given domain and analyze AI crawler visibility.
    Tries both the original domain and www variation if needed.
    
    Args:
        domain: Domain with protocol (e.g., https://example.com)
        
    Returns:
        dict with keys: found, hasAllow, hasDisallow, mentionedBots, rawContentLength
    """
    result = {
        "found": False,
        "hasAllow": False,
        "hasDisallow": False,
        "mentionedBots": {
            "GPTBot": False,
            "ClaudeBot": False,
            "PerplexityBot": False,
            "GitHubCopilot": False,
            "Gemini": False
        },
        "rawContentLength": 0
    }
    
    # Extract hostname from domain
    parsed = urlparse(domain)
    hostname = parsed.hostname
    
    if not hostname:
        print(f"⚠️ llms.txt fetch failed - invalid hostname | domain={domain}")
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
        llms_url = f"https://{try_hostname}/llms.txt"
        
        print(f"[LLMS_TXT] Fetching: {llms_url}")
        
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
                
                response = requests.get(llms_url, headers=headers, timeout=10, allow_redirects=True)
                
                print(f"[LLMS_TXT] Status: {response.status_code}")
                
                if response.status_code == 200:
                    content = response.text
                    parsed = parse_llms_txt_content(content)
                    
                    result.update({
                        "found": True,
                        "hasAllow": parsed["hasAllow"],
                        "hasDisallow": parsed["hasDisallow"],
                        "mentionedBots": parsed["mentionedBots"],
                        "rawContentLength": len(content)
                    })
                    
                    print(f"[LLMS_TXT] Allow found: {parsed['hasAllow']}")
                    print(f"[LLMS_TXT] Disallow found: {parsed['hasDisallow']}")
                    bots_detected = ", ".join([f"{bot}={found}" for bot, found in parsed["mentionedBots"].items()])
                    print(f"[LLMS_TXT] Bots detected: {bots_detected}")
                    
                    return result
                elif response.status_code == 404:
                    print(f"[LLMS_TXT] Not found (404) | url={llms_url}")
                    # 404 is expected for many domains, continue trying other hostnames
                    break
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(1, 3)  # Better exponential backoff with jitter
                        print(f"[LLMS_TXT] Rate limited (429) | url={llms_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"[LLMS_TXT] Rate limited after retries | url={llms_url}")
                        break
                else:
                    print(f"[LLMS_TXT] Returned status {response.status_code} | url={llms_url}")
                    break  # Don't retry non-429 errors
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt + random.uniform(0.5, 1.5)
                    print(f"[LLMS_TXT] Request timed out | url={llms_url} | retry in {wait_time:.1f}s | attempt {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"[LLMS_TXT] Request timed out after retries | url={llms_url}")
                    break
            except requests.exceptions.ConnectionError as e:
                print(f"[LLMS_TXT] Connection error | url={llms_url} | error={str(e)}")
                break  # Don't retry connection errors
            except Exception as e:
                print(f"[LLMS_TXT] Fetch failed | url={llms_url} | error={str(e)}")
                break
    
    # If we get here, no llms.txt was found on any hostname variation
    print(f"[LLMS_TXT] Not found on any hostname variation | tried={hostnames_to_try}")
    
    return result
