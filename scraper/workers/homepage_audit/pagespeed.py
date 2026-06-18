"""Standalone PageSpeed Insights API handler"""

import os
import requests
import time
import hashlib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Global cache to prevent duplicate calls
_pagespeed_cache = {}

def fetch_pagespeed_optimized(url: str, timeout: int = 180) -> dict:
    """
    Fetch PageSpeed metrics with optimized parameters for non-blocking execution.
    
    Uses category=performance to reduce response time from 60+ seconds to 39-42 seconds.
    """
    # Load API key from environment
    api_key = os.getenv('PSI_API_KEY', '')
    
    if not api_key:
        print("[PAGESPEED] No API key found")
        return {
            'success': False,
            'error': 'no_api_key',
            'message': 'PageSpeed API key not configured'
        }
    
    api_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    
    result = {
        'success': False,
        'mobile': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
        'desktop': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
        'error': None,
        'message': None
    }
    
    # Process both strategies
    for strategy in ['mobile', 'desktop']:
        try:
            # Hard 180s timeout for both strategies
            request_timeout = 180
            
            print(f"[PAGESPEED] {strategy} timeout set to {request_timeout}s")
            
            params = {
                'url': url,
                'key': api_key,
                'strategy': strategy,
                'category': 'performance'  # OPTIMIZED: Only performance category
            }
            
            print(f"[PAGESPEED] Fetching {strategy} metrics for: {url}")
            response = requests.get(api_url, params=params, timeout=request_timeout)
            response.raise_for_status()
            
            data = response.json()
            lighthouse = data.get('lighthouseResult', {})
            audits = lighthouse.get('audits', {})
            
            # Extract performance score
            score = lighthouse.get('categories', {}).get('performance', {}).get('score', 0)
            score = int(score * 100) if score else 0
            
            # Extract Core Web Vitals
            fcp = audits.get('first-contentful-paint', {}).get('displayValue', 'N/A')
            lcp = audits.get('largest-contentful-paint', {}).get('displayValue', 'N/A')
            tbt = audits.get('total-blocking-time', {}).get('displayValue', 'N/A')
            cls = audits.get('cumulative-layout-shift', {}).get('displayValue', 'N/A')
            
            result[strategy] = {
                'score': score,
                'fcp': fcp,
                'lcp': lcp,
                'tbt': tbt,
                'cls': cls
            }
            
            print(f"[PAGESPEED] {strategy.capitalize()} success: score={score}, FCP={fcp}, LCP={lcp}, TBT={tbt}, CLS={cls}")
            
        except requests.Timeout:
            print(f"[PAGESPEED] {strategy.capitalize()} timeout after {request_timeout}s")
            # Set strategy to null for failed requests, don't fail entire request
            result[strategy] = None
            result['message'] = f'{strategy.capitalize()} analysis timed out'
        except requests.RequestException as e:
            print(f"[PAGESPEED] {strategy.capitalize()} request failed: {str(e)}")
            # Set strategy to null for failed requests, don't fail entire request
            result[strategy] = None
            result['message'] = f'{strategy.capitalize()} analysis failed'
        except Exception as e:
            print(f"[PAGESPEED] {strategy.capitalize()} error: {str(e)}")
            # Set strategy to null for failed requests, don't fail entire request
            result[strategy] = None
            result['message'] = f'{strategy.capitalize()} analysis error'
    
    # Check if we got any successful results
    mobile_success = result['mobile'] is not None and result['mobile'].get('fcp') != 'N/A'
    desktop_success = result['desktop'] is not None and result['desktop'].get('fcp') != 'N/A'
    
    if mobile_success or desktop_success:
        result['success'] = True
        if mobile_success and desktop_success:
            result['message'] = 'PageSpeed analysis completed'
        else:
            result['message'] = 'PageSpeed analysis completed partially'
        
        # Calculate canonical composite score (average of available device scores)
        # This is the SINGLE SOURCE OF TRUTH for the performance score after PageSpeed completes.
        valid_scores = []
        if result['mobile'] is not None and result['mobile'].get('score', 0) > 0:
            valid_scores.append(result['mobile']['score'])
        if result['desktop'] is not None and result['desktop'].get('score', 0) > 0:
            valid_scores.append(result['desktop']['score'])
        
        if valid_scores:
            result['score'] = round(sum(valid_scores) / len(valid_scores))
        else:
            result['score'] = None
    elif not result['error']:
        result['error'] = 'no_data'
        result['message'] = 'No performance data available'
        result['score'] = None
    
    return result


def run_pagespeed_analysis(url: str) -> dict:
    """
    Main entry point for PageSpeed analysis.
    
    Args:
        url: URL to analyze
        
    Returns:
        {
            'success': bool,
            'mobile': { 'fcp', 'lcp', 'tbt', 'cls' },
            'desktop': { 'fcp', 'lcp', 'tbt', 'cls' },
            'error': str | None,
            'message': str | None,
            'status': str  # 'completed' | 'failed' | 'fallback'
        }
    """
    print(f"[PAGESPEED] Starting analysis for: {url}")
    
    # Create unique audit_id from URL
    audit_id = hashlib.md5(url.encode()).hexdigest()[:8]
    
    # Check cache to prevent duplicate calls
    if audit_id in _pagespeed_cache:
        print(f"[PAGESPEED] Using cached result for audit_id: {audit_id}")
        cached_result = _pagespeed_cache[audit_id]
        cached_result['message'] = 'Using cached result'
        return cached_result
    
    try:
        result = fetch_pagespeed_optimized(url, timeout=180)
        
        if result['success']:
            print(f"[PAGESPEED] Analysis completed: success={result['success']}")
            result['status'] = 'completed'
        else:
            print(f"[PAGESPEED] Analysis failed, using fallback")
            result = {
                'success': True,  # Don't fail the audit
                'mobile': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
                'desktop': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
                'score': None,  # No canonical score — fallback_score used by mapper instead
                'error': None,
                'message': 'PageSpeed analysis failed - using fallback score',
                'status': 'fallback',
                'fallback_score': 50
            }
        
        # Cache the result
        _pagespeed_cache[audit_id] = result
        return result
        
    except Exception as e:
        print(f"[PAGESPEED] Critical error: {str(e)} - using fallback")
        result = {
            'success': True,  # Don't fail the audit
            'mobile': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
            'desktop': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
            'score': None,  # No canonical score — fallback_score used by mapper instead
            'error': None,
            'message': 'PageSpeed analysis failed - using fallback score',
            'status': 'fallback',
            'fallback_score': 50
        }
        
        # Cache the fallback result
        _pagespeed_cache[audit_id] = result
        return result
