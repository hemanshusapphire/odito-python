"""HTTPS redirect checker for domain-level technical data collection."""

import requests
import random
from urllib.parse import urlparse
from config.config import USER_AGENTS


def check_https_redirect(hostname: str) -> dict:
    """
    Check if domain correctly redirects from HTTP to HTTPS.
    
    Args:
        hostname: Hostname only (e.g., "example.com")
        
    Returns:
        dict with keys: https_redirect, redirect_chain, final_url
    """
    result = {
        "https_redirect": False,
        "redirect_chain": [],
        "final_url": None
    }
    
    if not hostname:
        print(f"⚠️ HTTPS redirect check failed - invalid hostname | hostname={hostname}")
        return result
    
    # Always start the check from http://hostname
    http_url = f"http://{hostname}"
    
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html, */*"
        }
        
        # Perform the request WITHOUT automatically following redirects
        response = requests.get(
            http_url, 
            headers=headers, 
            timeout=10, 
            allow_redirects=False
        )
        
        # Initialize redirect chain with starting URL
        redirect_chain = [http_url]
        current_url = http_url
        
        # Follow redirects manually to capture the complete chain
        max_redirects = 10
        redirect_count = 0
        
        while (response.status_code in [301, 302, 303, 307, 308] and 
               'Location' in response.headers and 
               redirect_count < max_redirects):
            
            redirect_url = response.headers['Location']
            
            # Handle relative URLs
            if redirect_url.startswith('/'):
                parsed_current = urlparse(current_url)
                redirect_url = f"{parsed_current.scheme}://{parsed_current.netloc}{redirect_url}"
            
            redirect_chain.append(redirect_url)
            current_url = redirect_url
            
            # Follow the redirect
            try:
                response = requests.get(
                    redirect_url,
                    headers=headers,
                    timeout=10,
                    allow_redirects=False
                )
                redirect_count += 1
            except Exception as redirect_error:
                print(f"⚠️ HTTPS redirect chain broken at step {redirect_count} | url={redirect_url} | error={str(redirect_error)}")
                break
        
        result["redirect_chain"] = redirect_chain
        result["final_url"] = redirect_chain[-1] if redirect_chain else http_url
        
        # Correct detection rule: Check if redirect chain leads from http://domain → https://domain
        if len(redirect_chain) >= 2:
            initial_url = redirect_chain[0]
            final_url = redirect_chain[-1]
            
            initial_parsed = urlparse(initial_url)
            final_parsed = urlparse(final_url)
            
            # Check if started with HTTP and ended with HTTPS
            started_with_http = initial_parsed.scheme == 'http'
            ended_with_https = final_parsed.scheme == 'https'
            
            # Check if hostname matches (allowing www variations)
            initial_host = initial_parsed.hostname
            final_host = final_parsed.hostname
            
            same_domain = (
                initial_host == final_host or
                initial_host == f"www.{final_host}" or
                final_host == f"www.{initial_host}"
            )
            
            result["https_redirect"] = started_with_http and ended_with_https and same_domain
        
        if result["https_redirect"]:
            print(f"✅ HTTPS redirect detected | http={http_url} | https={result['final_url']} | chain_length={len(redirect_chain)}")
        else:
            print(f"⚠️ No HTTPS redirect detected | http={http_url} | final={result['final_url']} | chain_length={len(redirect_chain)}")
            
    except requests.exceptions.Timeout:
        print(f"⚠️ HTTPS redirect check timed out | url={http_url}")
    except requests.exceptions.ConnectionError as e:
        print(f"⚠️ HTTPS redirect check connection error | url={http_url} | error={str(e)}")
    except Exception as e:
        print(f"⚠️ HTTPS redirect check failed | url={http_url} | error={str(e)}")
    
    return result
