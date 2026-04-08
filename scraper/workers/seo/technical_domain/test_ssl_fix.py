#!/usr/bin/env python3
"""
Test script to validate SSL certificate fix for technical_domain worker.

Tests both:
- sapphiredigitalagency.com
- www.sapphiredigitalagency.com

Expected output: Both should show valid SSL certificates with proper expiry dates.
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from scraper.workers.seo.technical_domain.https_redirect_checker import check_https_redirect
from scraper.workers.seo.technical_domain.ssl_checker import check_ssl_certificate
from urllib.parse import urlparse

def test_domain_fix(domain_name):
    """Test the SSL fix for a given domain."""
    print(f"\n{'='*60}")
    print(f"Testing domain: {domain_name}")
    print(f"{'='*60}")
    
    # Step 1: Check HTTPS redirect (simulates worker's first step)
    print(f"\n[STEP 1] Checking HTTPS redirect for {domain_name}")
    https_result = check_https_redirect(domain_name)
    
    print(f"HTTPS redirect result: {https_result['https_redirect']}")
    print(f"Final URL: {https_result['final_url']}")
    print(f"Redirect chain: {https_result['redirect_chain']}")
    
    # Step 2: Extract hostname from final URL (simulates worker's SSL logic)
    final_url = https_result["final_url"]
    final_hostname = None
    
    if final_url:
        try:
            parsed_final = urlparse(final_url)
            final_hostname = parsed_final.hostname
            print(f"\n[STEP 2] Extracted hostname from final URL")
            print(f"Final URL: {final_url}")
            print(f"Final hostname: {final_hostname}")
        except Exception as parse_error:
            print(f"[ERROR] Failed to parse final URL: {parse_error}")
            final_hostname = domain_name
    else:
        print(f"[INFO] No final URL available, using base domain")
        final_hostname = domain_name
    
    # Step 3: Check SSL certificate using final hostname (simulates worker's SSL check)
    print(f"\n[STEP 3] Checking SSL certificate for hostname: {final_hostname}")
    ssl_result = check_ssl_certificate(final_hostname)
    
    print(f"\nSSL Results for {domain_name}:")
    print(f"  SSL Valid: {ssl_result['ssl_valid']}")
    print(f"  Expiry Date: {ssl_result['ssl_expiry_date']}")
    print(f"  Days Remaining: {ssl_result['ssl_days_remaining']}")
    
    # Step 4: Validate expected output format
    print(f"\n[STEP 4] Validation")
    
    expected_format = {
        "sslValid": ssl_result["ssl_valid"],
        "sslExpiryDate": ssl_result["ssl_expiry_date"], 
        "sslDaysRemaining": ssl_result["ssl_days_remaining"]
    }
    
    print(f"Expected JSON output:")
    import json
    print(json.dumps(expected_format, indent=2))
    
    # Validation checks
    issues = []
    if not ssl_result["ssl_valid"]:
        issues.append("❌ SSL certificate is not valid")
    if not ssl_result["ssl_expiry_date"]:
        issues.append("❌ SSL expiry date is missing")
    if ssl_result["ssl_days_remaining"] is None:
        issues.append("❌ SSL days remaining is missing")
    elif ssl_result["ssl_days_remaining"] < 0:
        issues.append(f"❌ SSL certificate has expired ({ssl_result['ssl_days_remaining']} days ago)")
    
    if issues:
        print(f"\n❌ VALIDATION FAILED for {domain_name}:")
        for issue in issues:
            print(f"  {issue}")
        return False
    else:
        print(f"\n✅ VALIDATION PASSED for {domain_name}")
        return True

def main():
    """Run tests for both domains."""
    print("SSL Certificate Fix Validation Test")
    print("Testing the fix for SSL hostname extraction from final URL")
    
    test_domains = [
        "sapphiredigitalagency.com",
        "www.sapphiredigitalagency.com"
    ]
    
    results = []
    for domain in test_domains:
        try:
            result = test_domain_fix(domain)
            results.append((domain, result))
        except Exception as e:
            print(f"\n❌ TEST FAILED for {domain} with exception:")
            print(f"  Error type: {type(e).__name__}")
            print(f"  Error: {str(e)}")
            results.append((domain, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for domain, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {domain}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! SSL fix is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Please review the implementation.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
