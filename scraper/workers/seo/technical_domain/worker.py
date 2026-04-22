"""
TECHNICAL_DOMAIN worker - Pure data collection for domain-level technical data.

Fetches /robots.txt, /sitemap.xml, and /llms.txt for the project domain.
Stores raw data via the Node.js backend API.
Reports job completion so the pipeline continues.

NO scoring logic. NO rule evaluation. NO parsing validation.
"""

import os
import re
import requests
from scraper.workers.seo.technical_domain.robots_fetcher import fetch_robots
from scraper.workers.seo.technical_domain.sitemap_fetcher import fetch_sitemap
from scraper.workers.seo.technical_domain.llms_fetcher import fetch_llms_txt
from scraper.workers.seo.technical_domain.ssl_checker import check_ssl_certificate
from scraper.workers.seo.technical_domain.https_redirect_checker import check_https_redirect



def execute_technical_domain(job):
    """
    Execute TECHNICAL_DOMAIN job: fetch robots.txt, sitemap.xml, llms.txt, SSL certificate, and HTTPS redirect, store results.
    
    Args:
        job: Pydantic model with jobId, projectId, userId, domain
        
    Returns:
        dict with status and results
    """
    job_id = job.jobId
    project_id = job.projectId
    domain = job.domain
    
    print(f"[WORKER] TECHNICAL_DOMAIN STARTED | jobId={job_id}")
    print(f"[START] ═══════════════════════════════════════════════════════════════")
    print(f"[START] Processing TECHNICAL_DOMAIN job")
    print(f"[START] jobId={job_id}")
    print(f"[START] projectId={project_id}")
    print(f"[START] domain={domain}")
    print(f"[START] ═══════════════════════════════════════════════════════════════")
    
    # Validate required environment variables
    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        print(f"[ERROR] ❌ NODE_BACKEND_URL is required")
        raise Exception("NODE_BACKEND_URL is required")
    
    print(f"[STEP 0] Environment validation passed")
    print(f"[STEP 0] NODE_BACKEND_URL={node_backend_url}")
    
    # Domain normalization: extract base hostname (without www)
    from urllib.parse import urlparse
    
    print(f"[STEP 0] Parsing domain...")
    # Parse the domain to extract hostname
    parsed_domain = urlparse(domain if domain.startswith(('http://', 'https://')) else f'https://{domain}')
    hostname = parsed_domain.hostname
    
    print(f"[STEP 0] Parsed hostname={hostname}")
    
    if not hostname:
        print(f"[ERROR] ❌ Invalid domain format | domain={domain}")
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": "Invalid domain format"
        }
    
    # Store base domain without www prefix (as requested)
    base_domain = hostname.replace('www.', '') if hostname.startswith('www.') else hostname
    
    # Keep original domain for robots/sitemap (with https://), but store base domain only
    domain_with_protocol = f"https://{hostname}"
    
    print(f"[STEP 0] Domain normalization complete")
    print(f"[STEP 0] base_domain={base_domain}")
    print(f"[STEP 0] domain_with_protocol={domain_with_protocol}")
    
    try:
        # Step 1: Fetch robots.txt
        print(f"[STEP 1] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 1] Fetching robots.txt")
        print(f"[STEP 1] domain={domain_with_protocol}")
        robots_result = fetch_robots(domain_with_protocol)
        print(f"[STEP 1] ✅ robots.txt fetch complete | status={robots_result['status']} | exists={robots_result['exists']}")
        print(f"[STEP 1] ═══════════════════════════════════════════════════════════")
        
        # Add delay to prevent rate limiting
        import time
        import random
        print(f"[STEP 1] Waiting 2-4s before next request...")
        time.sleep(random.uniform(2, 4))  # 2-4 second random delay between requests
        
        # Step 2: Fetch sitemap.xml
        print(f"[STEP 2] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 2] Fetching sitemap.xml")
        print(f"[STEP 2] domain={domain_with_protocol}")
        sitemap_result = fetch_sitemap(domain_with_protocol)
        print(f"[STEP 2] ✅ sitemap.xml fetch complete | status={sitemap_result['status']} | exists={sitemap_result['exists']} | url_count={sitemap_result['url_count']}")
        print(f"[STEP 2] ═══════════════════════════════════════════════════════════")
        
        # Add delay to prevent rate limiting
        print(f"[STEP 2] Waiting 2-4s before next request...")
        time.sleep(random.uniform(2, 4))  # 2-4 second random delay between requests
        
        # Step 3: Fetch llms.txt
        print(f"[STEP 3] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 3] Fetching llms.txt")
        print(f"[STEP 3] domain={domain_with_protocol}")
        llms_result = fetch_llms_txt(domain_with_protocol)
        print(f"[STEP 3] ✅ llms.txt fetch complete | status={llms_result['status']} | exists={llms_result['exists']} | hasAllow={llms_result['hasAllow']} | hasDisallow={llms_result['hasDisallow']}")
        print(f"[STEP 3] ═══════════════════════════════════════════════════════════")
        
        # Step 4: Check HTTPS redirect first (using base domain)
        print(f"[STEP 4] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 4] Checking HTTPS redirect")
        print(f"[STEP 4] base_domain={base_domain}")
        https_redirect_result = check_https_redirect(base_domain)
        print(f"[STEP 4] ✅ HTTPS redirect check complete | https_redirect={https_redirect_result['https_redirect']} | final_url={https_redirect_result['final_url']}")
        print(f"[STEP 4] ═══════════════════════════════════════════════════════════")
        
        # Step 5: Extract hostname from final URL for SSL checking
        print(f"[STEP 5] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 5] Extracting hostname from final URL for SSL check")
        final_url = https_redirect_result["final_url"]
        final_hostname = None
        
        if final_url:
            try:
                parsed_final = urlparse(final_url)
                final_hostname = parsed_final.hostname
                print(f"[STEP 5] ✅ Extracted hostname | final_url={final_url} | final_hostname={final_hostname}")
            except Exception as parse_error:
                print(f"[STEP 5] ⚠️ Failed to parse final URL, using base domain | error={str(parse_error)}")
                final_hostname = base_domain  # Fallback to base domain
        else:
            print(f"[STEP 5] ⚠️ No final URL available, using base domain")
            final_hostname = base_domain
        
        print(f"[STEP 5] Final hostname for SSL check: {final_hostname}")
        print(f"[STEP 5] ═══════════════════════════════════════════════════════════")
        
        # Step 6: Check SSL certificate using hostname from final resolved URL
        print(f"[STEP 6] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 6] Checking SSL certificate")
        print(f"[STEP 6] hostname={final_hostname}")
        ssl_result = check_ssl_certificate(final_hostname)
        
        # Add debug logging for SSL results
        if ssl_result["ssl_valid"]:
            print(f"[STEP 6] ✅ SSL check successful | hostname={final_hostname} | expiry={ssl_result['ssl_expiry_date']} | days_remaining={ssl_result['ssl_days_remaining']}")
        else:
            print(f"[STEP 6] ❌ SSL check failed | hostname={final_hostname} | ssl_valid=False")
        print(f"[STEP 6] ═══════════════════════════════════════════════════════════")
        
        # Step 7: Store results via Node.js API
        print(f"[STEP 7] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 7] Storing results via Node.js API")
        print(f"[STEP 7] projectId={project_id}")
        
        report_data = {
            "projectId": project_id,
            "domain": base_domain,  # Store base domain without www as requested
            "robotsStatus": robots_result["status"],
            "robotsExists": robots_result["exists"],
            "robotsContent": robots_result["content"],
            "sitemapStatus": sitemap_result["status"],
            "sitemapExists": sitemap_result["exists"],
            "sitemapContent": sitemap_result["content"],
            "parsedSitemapUrlCount": sitemap_result["url_count"],
            "llmsTxt": llms_result,
            "sslValid": ssl_result["ssl_valid"],
            "sslExpiryDate": ssl_result["ssl_expiry_date"],
            "sslDaysRemaining": ssl_result["ssl_days_remaining"],
            "httpsRedirect": https_redirect_result["https_redirect"],
            "redirectChain": https_redirect_result["redirect_chain"],
            "finalUrl": https_redirect_result["final_url"]
        }
        
        print(f"[STEP 7] Report data prepared | keys={list(report_data.keys())}")
        
        try:
            store_url = f"{node_backend_url}/api/jobs/domain-technical-report"
            print(f"[STEP 7] POST request to: {store_url}")
            store_response = requests.post(store_url, json=report_data, timeout=10)
            print(f"[STEP 7] API response status: {store_response.status_code}")
            print(f"[STEP 7] API response body: {store_response.text[:200]}" if len(store_response.text) > 200 else f"[STEP 7] API response body: {store_response.text}")
            store_response.raise_for_status()
            print(f"[STEP 7] ✅ Report stored successfully")
        except Exception as store_error:
            print(f"[STEP 7] ❌ Failed to store report | error={str(store_error)}")
            # Non-critical: continue even if storage fails
        print(f"[STEP 7] ═══════════════════════════════════════════════════════════")
        
        # Step 8: Report job completion to Node.js
        print(f"[STEP 8] ═══════════════════════════════════════════════════════════")
        print(f"[STEP 8] Reporting job completion to Node.js")
        print(f"[STEP 8] jobId={job_id}")
        
        stats = {
            "robotsExists": robots_result["exists"],
            "robotsStatus": robots_result["status"],
            "sitemapExists": sitemap_result["exists"],
            "sitemapStatus": sitemap_result["status"],
            "parsedSitemapUrlCount": sitemap_result["url_count"],
            "llmsTxtFound": llms_result.get("found", llms_result.get("exists", False)),
            "llmsTxtHasAllow": llms_result["hasAllow"],
            "llmsTxtHasDisallow": llms_result["hasDisallow"],
            "llmsTxtMentionedBots": llms_result["mentionedBots"],
            "llmsTxtRawContentLength": llms_result.get("rawContentLength", 0),
            "sslValid": ssl_result["ssl_valid"],
            "sslDaysRemaining": ssl_result["ssl_days_remaining"],
            "httpsRedirect": https_redirect_result["https_redirect"]
        }
        
        print(f"[STEP 8] Stats prepared: {stats}")
        
        try:
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            print(f"[STEP 8] POST request to: {complete_url}")
            complete_response = requests.post(
                complete_url,
                json={"stats": stats},
                timeout=10
            )
            print(f"[STEP 8] API response status: {complete_response.status_code}")
            print(f"[STEP 8] API response body: {complete_response.text[:200]}" if len(complete_response.text) > 200 else f"[STEP 8] API response body: {complete_response.text}")
            complete_response.raise_for_status()
            print(f"[STEP 8] ✅ Job completion reported successfully")
        except Exception as complete_error:
            print(f"[STEP 8] ❌ Failed to report completion | error={str(complete_error)}")
            # Try to report failure so pipeline can continue via failJob fallback
            print(f"[STEP 8] Attempting to report failure as fallback...")
            try:
                fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
                fail_response = requests.post(
                    fail_url,
                    json={"error": f"Completion reporting failed: {str(complete_error)}", "stats": stats},
                    timeout=10
                )
                print(f"[STEP 8] Failure fallback response status: {fail_response.status_code}")
            except Exception as fail_error:
                print(f"[STEP 8] ❌ Failure fallback also failed | error={str(fail_error)}")
        print(f"[STEP 8] ═══════════════════════════════════════════════════════════")
        
        print(f"[END] ═══════════════════════════════════════════════════════════════")
        print(f"[END] Job completed successfully")
        print(f"[END] jobId={job_id}")
        print(f"[END] robots={robots_result['exists']} | sitemap={sitemap_result['exists']} | sitemapUrls={sitemap_result['url_count']} | llmsTxt={llms_result.get('found', llms_result.get('exists', False))} | ssl={ssl_result['ssl_valid']} | httpsRedirect={https_redirect_result['https_redirect']}")
        print(f"[END] ═══════════════════════════════════════════════════════════════")
        
        print(f"[WORKER] TECHNICAL_DOMAIN COMPLETED | jobId={job_id}")
        
        return {
            "status": "completed",
            "jobId": job_id,
            "robots_exists": robots_result["exists"],
            "sitemap_exists": sitemap_result["exists"],
            "sitemap_url_count": sitemap_result["url_count"],
            "llms_exists": llms_result["exists"],
            "llms_status": llms_result["status"],
            "llms_has_allow": llms_result["hasAllow"],
            "llms_has_disallow": llms_result["hasDisallow"],
            "llms_mentioned_bots": llms_result["mentionedBots"],
            "ssl_valid": ssl_result["ssl_valid"],
            "ssl_days_remaining": ssl_result["ssl_days_remaining"],
            "https_redirect": https_redirect_result["https_redirect"]
        }
        
    except Exception as e:
        print(f"[ERROR] ═══════════════════════════════════════════════════════════")
        print(f"[ERROR] ❌ Worker failed")
        print(f"[ERROR] TECHNICAL_DOMAIN FAILED: {str(e)}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        print(f"[ERROR] jobId={job_id}")
        print(f"[ERROR] error={str(e)}")
        print(f"[ERROR] Stack trace:")
        import traceback
        traceback.print_exc()
        print(f"[ERROR] ═══════════════════════════════════════════════════════════")
        
        # Report failure to Node.js so pipeline continues via failJob fallback
        print(f"[ERROR] Attempting to report failure to Node.js...")
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            fail_response = requests.post(
                fail_url,
                json={"error": str(e), "stats": {}},
                timeout=10
            )
            print(f"[ERROR] Failure API response status: {fail_response.status_code}")
            print(f"[ERROR] ✅ Failure reported to Node.js")
        except Exception as fail_error:
            print(f"[ERROR] ❌ Failed to report failure | error={str(fail_error)}")
        
        # Return success so the HTTP handler doesn't raise an exception
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": str(e)
        }
