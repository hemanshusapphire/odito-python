"""Enhanced HTTPS redirect checker for comprehensive domain-level technical data collection."""

import requests
import random
from urllib.parse import urlparse
from config.config import USER_AGENTS


def check_https_redirect_comprehensive(hostname: str) -> dict:
    """
    Comprehensively check HTTPS redirects for all 4 domain variants.
    
    Tests:
    1. http://domain
    2. https://domain  
    3. http://www.domain
    4. https://www.domain
    
    Args:
        hostname: Base hostname without www (e.g., "example.com")
        
    Returns:
        dict with comprehensive redirect analysis
    """
    result = {
        "base_domain": hostname,
        "https_redirect": False,
        "redirect_chain": [],
        "final_url": None,
        "variant_results": {}
    }
    
    if not hostname:
        print(f"⚠️ HTTPS redirect check failed - invalid hostname | hostname={hostname}")
        return result
    
    # Define all 4 variants to test
    variants = [
        f"http://{hostname}",
        f"https://{hostname}",
        f"http://www.{hostname}",
        f"https://www.{hostname}"
    ]
    
    print(f"[HTTPS_REDIRECT] Testing all 4 variants for base domain: {hostname}")
    
    # Test each variant
    for variant in variants:
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html, */*"
            }
            
            response = requests.get(variant, headers=headers, timeout=10, allow_redirects=False)
            
            variant_result = {
                "status_code": response.status_code,
                "redirects_to": response.headers.get('Location', None),
                "has_location": 'Location' in response.headers
            }
            
            result["variant_results"][variant] = variant_result
            
            print(f"[HTTPS_REDIRECT] {variant}: {response.status_code} {'-> ' + response.headers.get('Location', '') if 'Location' in response.headers else ''}")
            
        except Exception as e:
            result["variant_results"][variant] = {
                "status_code": 0,
                "error": str(e),
                "has_location": False
            }
            print(f"[HTTPS_REDIRECT] {variant}: Error - {str(e)}")
    
    # Primary test: Check if http://domain redirects to HTTPS
    primary_http = f"http://{hostname}"
    
    if primary_http in result["variant_results"]:
        primary_result = result["variant_results"][primary_http]
        
        if (primary_result["status_code"] in [301, 302, 303, 307, 308] and 
            primary_result["has_location"]):
            
            redirect_url = primary_result["redirects_to"]
            
            # Build redirect chain
            redirect_chain = [primary_http]
            
            # Follow redirects manually to capture complete chain
            current_url = redirect_url
            max_redirects = 10
            redirect_count = 0
            
            while redirect_count < max_redirects:
                try:
                    headers = {
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept": "text/html, */*"
                    }
                    
                    response = requests.get(current_url, headers=headers, timeout=10, allow_redirects=False)
                    redirect_chain.append(current_url)
                    
                    if response.status_code not in [301, 302, 303, 307, 308]:
                        # Reached final destination
                        break
                    
                    if 'Location' not in response.headers:
                        break
                    
                    current_url = response.headers['Location']
                    redirect_count += 1
                    
                except Exception as e:
                    print(f"[HTTPS_REDIRECT] Chain broken at step {redirect_count}: {str(e)}")
                    break
            
            result["redirect_chain"] = redirect_chain
            result["final_url"] = redirect_chain[-1] if redirect_chain else primary_http
            
            # Determine if this is a proper HTTP→HTTPS redirect
            if len(redirect_chain) >= 2:
                final_url = redirect_chain[-1]
                final_parsed = urlparse(final_url)
                
                started_with_http = True  # We started with HTTP
                ended_with_https = final_parsed.scheme == 'https'
                
                # Check if final hostname matches base domain or www variant
                final_host = final_parsed.hostname
                same_domain = (
                    final_host == hostname or
                    final_host == f"www.{hostname}"
                )
                
                result["https_redirect"] = started_with_http and ended_with_https and same_domain
    
    if result["https_redirect"]:
        print(f"✅ [HTTPS_REDIRECT] Proper HTTP→HTTPS redirect detected | chain_length={len(result['redirect_chain'])}")
    else:
        print(f"⚠️ [HTTPS_REDIRECT] No proper HTTP→HTTPS redirect detected")
    
    return result


def check_https_redirect(hostname: str) -> dict:
    """
    Simplified HTTPS redirect check (maintains backward compatibility).
    
    Args:
        hostname: Base hostname without www (e.g., "example.com")
        
    Returns:
        dict with keys: https_redirect, redirect_chain, final_url
    """
    comprehensive_result = check_https_redirect_comprehensive(hostname)
    
    # Return simplified result for backward compatibility
    return {
        "https_redirect": comprehensive_result["https_redirect"],
        "redirect_chain": comprehensive_result["redirect_chain"],
        "final_url": comprehensive_result["final_url"]
    }
