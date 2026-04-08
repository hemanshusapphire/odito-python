#!/usr/bin/env python3
"""
Test script for SSL and HTTPS redirect checking functionality.
"""

import sys
import os

# Add the python_workers directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from scraper.workers.seo.technical_domain.ssl_checker import check_ssl_certificate
from scraper.workers.seo.technical_domain.https_redirect_checker import check_https_redirect


def test_ssl_certificate():
    """Test SSL certificate checking."""
    print("=== Testing SSL Certificate Check ===")
    
    test_domains = [
        "https://google.com",
        "https://github.com", 
        "https://expired.badssl.com",  # Should fail - expired cert
        "https://wrong.host.badssl.com",  # Should fail - hostname mismatch
    ]
    
    for domain in test_domains:
        print(f"\nTesting: {domain}")
        result = check_ssl_certificate(domain)
        print(f"Result: {result}")


def test_https_redirect():
    """Test HTTPS redirect checking."""
    print("\n=== Testing HTTPS Redirect Check ===")
    
    test_domains = [
        "https://google.com",  # Should redirect
        "https://github.com",   # Should redirect  
        "https://httpbin.org",  # Should redirect
    ]
    
    for domain in test_domains:
        print(f"\nTesting: {domain}")
        result = check_https_redirect(domain)
        print(f"Result: {result}")


if __name__ == "__main__":
    try:
        test_ssl_certificate()
        test_https_redirect()
        print("\n=== Tests completed ===")
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)
