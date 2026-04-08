"""
TECHNICAL_DOMAIN worker - Pure data collection for domain-level technical data.

Fetches /robots.txt and /sitemap.xml for the project domain.
Stores raw data via the Node.js backend API.
Reports job completion so the pipeline continues.

NO scoring logic. NO rule evaluation. NO parsing validation.
"""

import os
import re
import requests
from scraper.workers.seo.technical_domain.robots_fetcher import fetch_robots
from scraper.workers.seo.technical_domain.sitemap_fetcher import fetch_sitemap
from scraper.workers.seo.technical_domain.ssl_checker import check_ssl_certificate
from scraper.workers.seo.technical_domain.https_redirect_checker import check_https_redirect



def execute_technical_domain(job):
    """
    Execute TECHNICAL_DOMAIN job: fetch robots.txt, sitemap.xml, SSL certificate, and HTTPS redirect, store results.
    
    Args:
        job: Pydantic model with jobId, projectId, userId, domain
        
    Returns:
        dict with status and results
    """
    job_id = job.jobId
    project_id = job.projectId
    domain = job.domain
    # Validate required environment variables
    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        raise Exception("NODE_BACKEND_URL is required")
    
    # Domain normalization: extract base hostname (without www)
    from urllib.parse import urlparse
    
    # Parse the domain to extract hostname
    parsed_domain = urlparse(domain if domain.startswith(('http://', 'https://')) else f'https://{domain}')
    hostname = parsed_domain.hostname
    
    if not hostname:
        print(f"❌ [TECHNICAL_DOMAIN] Invalid domain format | domain={domain}")
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": "Invalid domain format"
        }
    
    # Store base domain without www prefix (as requested)
    base_domain = hostname.replace('www.', '') if hostname.startswith('www.') else hostname
    
    # Keep original domain for robots/sitemap (with https://), but store base domain only
    domain_with_protocol = f"https://{hostname}"
    
    print(f"[TECHNICAL_DOMAIN] Starting | jobId={job_id} | domain={domain}")
    
    try:
        # Step 1: Fetch robots.txt
        print(f"[TECHNICAL_DOMAIN] Fetching robots.txt | domain={domain_with_protocol}")
        robots_result = fetch_robots(domain_with_protocol)
        
        # Step 2: Fetch sitemap.xml
        print(f"[TECHNICAL_DOMAIN] Fetching sitemap.xml | domain={domain_with_protocol}")
        sitemap_result = fetch_sitemap(domain_with_protocol)
        
        # Step 3: Check HTTPS redirect first (using base domain)
        print(f"[TECHNICAL_DOMAIN] Checking HTTPS redirect | base_domain={base_domain}")
        https_redirect_result = check_https_redirect(base_domain)
        
        # Step 4: Extract hostname from final URL for SSL checking
        final_url = https_redirect_result["final_url"]
        final_hostname = None
        
        if final_url:
            try:
                parsed_final = urlparse(final_url)
                final_hostname = parsed_final.hostname
                print(f"[TECHNICAL_DOMAIN] Extracted hostname from final URL | final_url={final_url} | final_hostname={final_hostname}")
            except Exception as parse_error:
                print(f"[TECHNICAL_DOMAIN] Failed to parse final URL | final_url={final_url} | error={str(parse_error)}")
                final_hostname = base_domain  # Fallback to base domain
        else:
            print(f"[TECHNICAL_DOMAIN] No final URL available, using base domain for SSL | base_domain={base_domain}")
            final_hostname = base_domain
        
        # Step 5: Check SSL certificate using hostname from final resolved URL
        print(f"[TECHNICAL_DOMAIN] Checking SSL certificate | hostname={final_hostname}")
        ssl_result = check_ssl_certificate(final_hostname)
        
        # Add debug logging for SSL results
        if ssl_result["ssl_valid"]:
            print(f"[TECHNICAL_DOMAIN] SSL check successful | hostname={final_hostname} | expiry={ssl_result['ssl_expiry_date']} | days_remaining={ssl_result['ssl_days_remaining']}")
        else:
            print(f"[TECHNICAL_DOMAIN] SSL check failed | hostname={final_hostname} | ssl_valid=False")
        
        # Step 6: Store results via Node.js API
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
            "sslValid": ssl_result["ssl_valid"],
            "sslExpiryDate": ssl_result["ssl_expiry_date"],
            "sslDaysRemaining": ssl_result["ssl_days_remaining"],
            "httpsRedirect": https_redirect_result["https_redirect"],
            "redirectChain": https_redirect_result["redirect_chain"],
            "finalUrl": https_redirect_result["final_url"]
        }
        
        try:
            store_url = f"{node_backend_url}/api/jobs/domain-technical-report"
            store_response = requests.post(store_url, json=report_data, timeout=10)
            store_response.raise_for_status()
            print(f"[TECHNICAL_DOMAIN] Report stored | projectId={project_id}")
        except Exception as store_error:
            print(f"⚠️ [TECHNICAL_DOMAIN] Failed to store report | error={str(store_error)}")
            # Non-critical: continue even if storage fails
        
        # Step 7: Report job completion to Node.js
        stats = {
            "robotsExists": robots_result["exists"],
            "robotsStatus": robots_result["status"],
            "sitemapExists": sitemap_result["exists"],
            "sitemapStatus": sitemap_result["status"],
            "parsedSitemapUrlCount": sitemap_result["url_count"],
            "sslValid": ssl_result["ssl_valid"],
            "sslDaysRemaining": ssl_result["ssl_days_remaining"],
            "httpsRedirect": https_redirect_result["https_redirect"]
        }
        
        try:
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            complete_response = requests.post(
                complete_url,
                json={"stats": stats},
                timeout=10
            )
            complete_response.raise_for_status()
            print(f"[TECHNICAL_DOMAIN] Job completion reported | jobId={job_id}")
        except Exception as complete_error:
            print(f"⚠️ [TECHNICAL_DOMAIN] Failed to report completion | error={str(complete_error)}")
            # Try to report failure so pipeline can continue via failJob fallback
            try:
                fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
                requests.post(
                    fail_url,
                    json={"error": f"Completion reporting failed: {str(complete_error)}", "stats": stats},
                    timeout=10
                )
            except Exception:
                pass
        
        print(f"[TECHNICAL_DOMAIN] Completed | jobId={job_id} | robots={robots_result['exists']} | sitemap={sitemap_result['exists']} | sitemapUrls={sitemap_result['url_count']} | ssl={ssl_result['ssl_valid']} | httpsRedirect={https_redirect_result['https_redirect']}")
        
        return {
            "status": "completed",
            "jobId": job_id,
            "robots_exists": robots_result["exists"],
            "sitemap_exists": sitemap_result["exists"],
            "sitemap_url_count": sitemap_result["url_count"],
            "ssl_valid": ssl_result["ssl_valid"],
            "ssl_days_remaining": ssl_result["ssl_days_remaining"],
            "https_redirect": https_redirect_result["https_redirect"]
        }
        
    except Exception as e:
        print(f"❌ [TECHNICAL_DOMAIN] Worker failed | jobId={job_id} | error={str(e)}")
        
        # Report failure to Node.js so pipeline continues via failJob fallback
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            requests.post(
                fail_url,
                json={"error": str(e), "stats": {}},
                timeout=10
            )
            print(f"[TECHNICAL_DOMAIN] Failure reported | jobId={job_id}")
        except Exception as fail_error:
            print(f"⚠️ [TECHNICAL_DOMAIN] Failed to report failure | error={str(fail_error)}")
        
        # Return success so the HTTP handler doesn't raise an exception
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": str(e)
        }
