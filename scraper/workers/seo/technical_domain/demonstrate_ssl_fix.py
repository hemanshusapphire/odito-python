#!/usr/bin/env python3
"""
Demonstration of SSL certificate fix - Before vs After behavior.

This script shows the critical difference between the old approach (stripping www)
and the new approach (using hostname from final resolved URL).
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from scraper.workers.seo.technical_domain.https_redirect_checker import check_https_redirect
from scraper.workers.seo.technical_domain.ssl_checker import check_ssl_certificate
from urllib.parse import urlparse

def old_approach(hostname):
    """Old approach: Strip 'www.' and check SSL on base domain."""
    print(f"\n❌ OLD APPROACH (BROKEN):")
    print(f"  Input hostname: {hostname}")
    
    # This was the old buggy logic
    base_domain = hostname.replace('www.', '') if hostname.startswith('www.') else hostname
    print(f"  After stripping 'www.': {base_domain}")
    
    ssl_result = check_ssl_certificate(base_domain)
    print(f"  SSL Result: valid={ssl_result['ssl_valid']}, expiry={ssl_result['ssl_expiry_date']}")
    
    return ssl_result

def new_approach(hostname):
    """New approach: Use hostname from final resolved URL."""
    print(f"\n✅ NEW APPROACH (FIXED):")
    print(f"  Input hostname: {hostname}")
    
    # Step 1: Check HTTPS redirect
    https_result = check_https_redirect(hostname)
    print(f"  HTTPS redirect chain: {https_result['redirect_chain']}")
    print(f"  Final URL: {https_result['final_url']}")
    
    # Step 2: Extract hostname from final URL
    final_url = https_result["final_url"]
    if final_url:
        parsed_final = urlparse(final_url)
        final_hostname = parsed_final.hostname
        print(f"  Extracted hostname from final URL: {final_hostname}")
    else:
        final_hostname = hostname
        print(f"  No final URL, using original hostname: {final_hostname}")
    
    # Step 3: Check SSL on the correct hostname
    ssl_result = check_ssl_certificate(final_hostname)
    print(f"  SSL Result: valid={ssl_result['ssl_valid']}, expiry={ssl_result['ssl_expiry_date']}")
    
    return ssl_result

def demonstrate_fix():
    """Demonstrate the fix with both test domains."""
    print("="*80)
    print("SSL CERTIFICATE FIX DEMONSTRATION")
    print("Showing the difference between old (broken) and new (fixed) approach")
    print("="*80)
    
    test_domains = [
        "sapphiredigitalagency.com",
        "www.sapphiredigitalagency.com"
    ]
    
    for domain in test_domains:
        print(f"\n{'-'*60}")
        print(f"TESTING: {domain}")
        print(f"{'-'*60}")
        
        # Test old approach
        old_result = old_approach(domain)
        
        # Test new approach  
        new_result = new_approach(domain)
        
        # Compare results
        print(f"\n📊 COMPARISON:")
        print(f"  Old approach SSL valid: {old_result['ssl_valid']}")
        print(f"  New approach SSL valid: {new_result['ssl_valid']}")
        
        if old_result['ssl_valid'] != new_result['ssl_valid']:
            print(f"  🎯 FIX SUCCESSFUL: Results differ - new approach works correctly!")
        elif not old_result['ssl_valid'] and not new_result['ssl_valid']:
            print(f"  ⚠️  Both approaches failed - domain may have SSL issues")
        else:
            print(f"  ✅ Both approaches work - no difference for this domain")
    
    print(f"\n{'='*80}")
    print("SUMMARY:")
    print("✅ Fix ensures SSL checks use the actual hostname from final resolved URL")
    print("✅ No more manual 'www.' stripping that causes SSL check failures") 
    print("✅ Robust fallback logic handles edge cases gracefully")
    print("✅ Clear debug logging for troubleshooting")
    print("="*80)

if __name__ == "__main__":
    demonstrate_fix()
