"""Homepage Audit Pipeline - Orchestrates the complete audit flow"""

import asyncio
from urllib.parse import urlparse

from .scraper import scrape_homepage
from .seo import analyze_seo
from .ai import analyze_ai_visibility
from .security import analyze_security
from .mapper import map_audit_response


async def run_homepage_audit(url: str, timeout: int = 30) -> dict:
    """
    Run complete homepage audit pipeline.
    
    Flow:
    1. Scrape page (any URL accepted)
    2. Run SEO analysis (sequential)
    3. Run AI visibility analysis (sequential)
    4. Run security analysis (sequential)
    5. Fetch PageSpeed metrics (background, non-blocking)
    6. Map response
    7. Return result
    
    Args:
        url: URL to audit (any URL accepted)
        timeout: Total timeout in seconds (default: 30)
    
    Returns:
        Audit result dict
    """
    import time
    start_time = time.time()
    
    try:
        # Step 1: Scrape page (with timeout)
        print(f"[HOMEPAGE_AUDIT] Starting audit for: {url}")
        print(f"[HOMEPAGE_AUDIT] Timeout set to: {timeout}s (reliability mode)")
        
        scrape_start = time.time()
        seo_data = await asyncio.to_thread(scrape_homepage, url, timeout=30)
        scrape_time = time.time() - scrape_start
        print(f"[HOMEPAGE_AUDIT] Scraping completed in: {scrape_time:.2f}s")
        
        if 'error' in seo_data:
            error_type = seo_data.get('error', 'unknown')
            error_msg = seo_data.get('message', 'Scraping failed')
            
            # Map specific error types
            if error_type == 'timeout':
                return {
                    'success': False,
                    'error': 'timeout',
                    'message': 'This website is taking longer to analyze. Please try again.'
                }
            elif error_type == 'request_failed':
                return {
                    'success': False,
                    'error': 'fetch_failed',
                    'message': f'Failed to fetch page: {error_msg}'
                }
            else:
                return {
                    'success': False,
                    'error': error_type,
                    'message': error_msg
                }
        
        # Step 2: Run SEO analysis (sequential)
        seo_start = time.time()
        seo_result = await asyncio.to_thread(analyze_seo, seo_data)
        seo_time = time.time() - seo_start
        print(f"[HOMEPAGE_AUDIT] SEO analysis completed in: {seo_time:.2f}s")
        
        # Step 3: Run AI visibility analysis (sequential)
        ai_start = time.time()
        ai_result = await asyncio.to_thread(analyze_ai_visibility, seo_data)
        ai_time = time.time() - ai_start
        print(f"[HOMEPAGE_AUDIT] AI analysis completed in: {ai_time:.2f}s")
        
        # Step 4: Run security analysis (sequential)
        security_start = time.time()
        security_result = await asyncio.to_thread(analyze_security, seo_data)
        security_time = time.time() - security_start
        print(f"[HOMEPAGE_AUDIT] Security analysis completed in: {security_time:.2f}s")
        
        # Step 5: Set performance status to processing (PSI handled separately via /pagespeed endpoint)
        # This makes audit non-blocking - PageSpeed runs async in background
        # Structure matches canonical schema: {status, score, mobile, desktop}
        seo_data['performance_metrics'] = {
            'status': 'processing',
            'score': None,  # No score until PageSpeed completes
            'mobile': None,  # Will be populated by /homepage-pagespeed endpoint
            'desktop': None,  # Will be populated by /homepage-pagespeed endpoint
        }
        print(f"[HOMEPAGE_AUDIT] Performance metrics set to processing - PSI will run separately (non-blocking)")
        
        # Step 6: Map response
        map_start = time.time()
        response = map_audit_response(seo_data, seo_result, ai_result, security_result)
        map_time = time.time() - map_start
        print(f"[HOMEPAGE_AUDIT] Response mapping completed in: {map_time:.2f}s")
        
        elapsed = time.time() - start_time
        print(f"[HOMEPAGE_AUDIT] TOTAL AUDIT completed in {elapsed:.2f}s for: {url}")
        
        return response
        
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        print(f"[HOMEPAGE_AUDIT] Timeout after {elapsed:.2f}s for: {url}")
        return {
            'success': False,
            'error': 'timeout',
            'message': 'This website is taking longer to analyze. Please try again.'
        }
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[HOMEPAGE_AUDIT] Error after {elapsed:.2f}s for: {url} - {str(e)}")
        return {
            'success': False,
            'error': 'pipeline_error',
            'message': f'Internal error: {str(e)}'
        }
