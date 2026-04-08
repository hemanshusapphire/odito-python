"""Domain performance analysis API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson.objectid import ObjectId
from datetime import datetime
import os
import requests
from urllib.parse import urlparse

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

router = APIRouter()

class DomainPerformanceJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    main_url: str

@router.post("/jobs/domain-performance")
def handle_domain_performance(job: DomainPerformanceJob):
    """Handle DOMAIN_PERFORMANCE job dispatched from Node.js"""
    print(f"[DEBUG] DOMAIN_PERFORMANCE endpoint called | jobId={job.jobId} | projectId={job.projectId}")
    
    # Import here to avoid circular imports
    from main import completed_jobs, completed_jobs_lock
    from db import db
    
    try:
        # Defensive guard: skip if already completed
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"ℹ️ Skipping already completed DOMAIN_PERFORMANCE job | jobId={job.jobId}")
                return {
                    "status": "already_completed",
                    "jobId": job.jobId,
                    "message": "Job already completed"
                }
        
        print(f"[WORKER] DOMAIN_PERFORMANCE started | jobId={job.jobId}")
        
        # Execute domain performance analysis
        result = execute_domain_performance_logic(job)
        
        # Mark as completed
        with completed_jobs_lock:
            completed_jobs.add(job.jobId)
        
        return {
            "status": "accepted",
            "jobId": job.jobId,
            "message": "DOMAIN_PERFORMANCE job accepted and processing"
        }
        
    except Exception as e:
        print(f"[ERROR] DOMAIN_PERFORMANCE handler failed | jobId={job.jobId} | reason=\"{str(e)}\"")
        return {
            "status": "error",
            "jobId": job.jobId,
            "error": str(e)
        }

def validate_scores(metrics, device_type):
    """Validate scores and log warnings for zero values"""
    if 'error' in metrics:
        print(f"[VALIDATION] {device_type} metrics contain error, skipping score validation")
        return
        
    score_fields = ['performance_score', 'accessibility_score', 'best_practices_score', 'seo_score']
    zero_scores = []
    
    for field in score_fields:
        score = metrics.get(field, 0)
        if score == 0:
            zero_scores.append(field)
    
    if zero_scores:
        print(f"[WARNING] {device_type} scores that are 0: {zero_scores}")
        print(f"[WARNING] This may indicate missing categories in Lighthouse response or API configuration issues")
        
        # Log available categories for debugging
        if 'categories_debug' in metrics:
            print(f"[DEBUG] Available categories in {device_type}: {metrics['categories_debug']}")
    else:
        print(f"[VALIDATION] All {device_type} scores are non-zero: { {field: metrics.get(field, 'N/A') for field in score_fields} }")

def execute_domain_performance_logic(job: DomainPerformanceJob):
    """Execute domain-level PageSpeed analysis for both mobile and desktop"""
    print(f"[DOMAIN_PERFORMANCE] Starting analysis for {job.main_url}")
    
    # Get MongoDB connection
    from db import db
    
    # PageSpeed API key from environment
    api_key = os.getenv('PSI_API_KEY')
    if not api_key or api_key == 'your_actual_api_key_here':
        raise ValueError("PSI_API_KEY not found in environment variables or still set to placeholder value")
    
    # Debug log to verify API key is loaded (showing first 6 chars only)
    print(f"[DOMAIN_PERFORMANCE] Using PSI API key: {api_key[:6]}***")
    
    # Validate and format URL
    main_url = job.main_url.strip()
    if not main_url.startswith(('http://', 'https://')):
        main_url = 'https://' + main_url
    
    # Remove trailing slashes for consistency (but keep one if present)
    if main_url.endswith('/') and main_url.count('/') > 3:
        main_url = main_url.rstrip('/')
    
    print(f"[DOMAIN_PERFORMANCE] Validated URL: {main_url}")
    
    # Extract domain from URL
    parsed_url = urlparse(main_url)
    domain = parsed_url.netloc
    if not domain:
        raise Exception(f"Invalid URL format: {main_url}")
    
    # PageSpeed API endpoint
    api_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    
    mobile_metrics = {}
    desktop_metrics = {}
    
    # Test mobile performance
    print(f"[DOMAIN_PERFORMANCE] Testing mobile performance for {domain}")
    try:
        mobile_params = {
            'url': main_url,
            'strategy': 'mobile',
            'key': api_key,
            'category': ['performance', 'accessibility', 'best-practices', 'seo']  # Request all categories
        }
        
        print(f"[DOMAIN_PERFORMANCE] Mobile request params: {mobile_params}")
        mobile_response = requests.get(api_url, params=mobile_params, timeout=120)
        
        print(f"[DOMAIN_PERFORMANCE] Mobile response status: {mobile_response.status_code}")
        
        if mobile_response.status_code != 200:
            print(f"[DOMAIN_PERFORMANCE] Mobile error response: {mobile_response.text}")
            raise Exception(f"Mobile PageSpeed API failed: {mobile_response.status_code} - {mobile_response.text}")
        
        mobile_data = mobile_response.json()
        mobile_metrics = extract_metrics(mobile_data)
        print(f"[DOMAIN_PERFORMANCE] Mobile metrics extracted: score={mobile_metrics.get('performance_score')}")
        
    except Exception as e:
        print(f"[ERROR] Mobile performance test failed: {str(e)}")
        # Continue with desktop test even if mobile fails
        mobile_metrics = {
            'error': str(e), 
            'performance_score': 0,
            'accessibility_score': 0,
            'best_practices_score': 0,
            'seo_score': 0
        }
    
    # Test desktop performance
    print(f"[DOMAIN_PERFORMANCE] Testing desktop performance for {domain}")
    try:
        desktop_params = {
            'url': main_url,
            'strategy': 'desktop',
            'key': api_key,
            'category': ['performance', 'accessibility', 'best-practices', 'seo']  # Request all categories
        }
        
        print(f"[DOMAIN_PERFORMANCE] Desktop request params: {desktop_params}")
        desktop_response = requests.get(api_url, params=desktop_params, timeout=120)
        
        print(f"[DOMAIN_PERFORMANCE] Desktop response status: {desktop_response.status_code}")
        
        if desktop_response.status_code != 200:
            print(f"[DOMAIN_PERFORMANCE] Desktop error response: {desktop_response.text}")
            raise Exception(f"Desktop PageSpeed API failed: {desktop_response.status_code} - {desktop_response.text}")
        
        desktop_data = desktop_response.json()
        desktop_metrics = extract_metrics(desktop_data)
        print(f"[DOMAIN_PERFORMANCE] Desktop metrics extracted: score={desktop_metrics.get('performance_score')}")
        
    except Exception as e:
        print(f"[ERROR] Desktop performance test failed: {str(e)}")
        # Continue with storing partial results even if desktop fails
        desktop_metrics = {
            'error': str(e), 
            'performance_score': 0,
            'accessibility_score': 0,
            'best_practices_score': 0,
            'seo_score': 0
        }
    
    # Check if both tests failed
    if 'error' in mobile_metrics and 'error' in desktop_metrics:
        raise Exception("Both mobile and desktop PageSpeed tests failed")
    
    # Validate scores before storing
    validate_scores(mobile_metrics, 'mobile')
    validate_scores(desktop_metrics, 'desktop')
    
    # Store results in MongoDB
    performance_doc = {
        'project_id': ObjectId(job.projectId),
        'domain': domain,
        'mobile': mobile_metrics,
        'desktop': desktop_metrics,
        'tested_at': datetime.utcnow(),
        'job_id': job.jobId
    }
    
    # Upsert to ensure only one document per project
    from db import seo_domain_performance
    seo_domain_performance.update_one(
        {'project_id': ObjectId(job.projectId)},
        {'$set': performance_doc},
        upsert=True
    )
    
    print(f"[DOMAIN_PERFORMANCE] Analysis complete for {domain} | mobile_score={mobile_metrics.get('performance_score')} | desktop_score={desktop_metrics.get('performance_score')}")
    
    # Update job status in MongoDB
    from db import jobs
    jobs.update_one(
        {'_id': ObjectId(job.jobId)},
        {
            '$set': {
                'status': 'completed',
                'completed_at': datetime.utcnow(),
                'result_data': {
                    'domain': domain,
                    'mobile_score': mobile_metrics.get('performance_score'),
                    'desktop_score': desktop_metrics.get('performance_score')
                }
            }
        }
    )
    
    return {
        'domain': domain,
        'mobile_metrics': mobile_metrics,
        'desktop_metrics': desktop_metrics
    }

def extract_metrics(pagespeed_data):
    """Extract key performance metrics from PageSpeed API response"""
    metrics = {}
    
    try:
        lighthouse_result = pagespeed_data['lighthouseResult']
        categories = lighthouse_result['categories']
        audits = lighthouse_result['audits']
        
        # Extract Lighthouse scores (multiply by 100 for 0-100 scale)
        metrics['performance_score'] = categories.get('performance', {}).get('score', 0) * 100
        
        # Other scores may not be available if only performance category requested
        metrics['accessibility_score'] = categories.get('accessibility', {}).get('score', 0) * 100
        metrics['best_practices_score'] = categories.get('best-practices', {}).get('score', 0) * 100
        metrics['seo_score'] = categories.get('seo', {}).get('score', 0) * 100
        
        # Log available categories for debugging
        print(f"[CATEGORIES] Available categories: {list(categories.keys())}")
        
        # Store categories debug info for validation
        metrics['categories_debug'] = list(categories.keys())
        
        print(f"[CATEGORIES] Performance score: {metrics['performance_score']}")
        if 'accessibility' in categories:
            print(f"[CATEGORIES] Accessibility score: {metrics['accessibility_score']}")
        else:
            print(f"[CATEGORIES] Accessibility category NOT FOUND in response")
        if 'best-practices' in categories:
            print(f"[CATEGORIES] Best practices score: {metrics['best_practices_score']}")
        else:
            print(f"[CATEGORIES] Best practices category NOT FOUND in response")
        if 'seo' in categories:
            print(f"[CATEGORIES] SEO score: {metrics['seo_score']}")
        else:
            print(f"[CATEGORIES] SEO category NOT FOUND in response")
        
        # Log available audit keys (sample)
        print(f"[AUDITS] Total audits available: {len(audits)}")
        audit_keys = list(audits.keys())
        print(f"[AUDITS] Sample audit keys: {audit_keys[:10]}...")  # Show first 10
        
        # Check for specific opportunity audits
        missing_opportunities = []
        for opp_key in ['render-blocking-resources', 'unused-javascript', 'uses-responsive-images', 'uses-text-compression', 'uses-long-cache-ttl']:
            if opp_key in audits:
                print(f"[AUDITS] Found opportunity: {opp_key} - score: {audits[opp_key].get('score')}")
            else:
                missing_opportunities.append(opp_key)
        
        if missing_opportunities:
            print(f"[AUDITS] Missing opportunities: {missing_opportunities}")
        
        # Check for specific diagnostic audits  
        missing_diagnostics = []
        for diag_key in ['mainthread-work-breakdown', 'bootup-time', 'network-requests']:
            if diag_key in audits:
                print(f"[AUDITS] Found diagnostic: {diag_key} - score: {audits[diag_key].get('score')}")
            else:
                missing_diagnostics.append(diag_key)
                
        if missing_diagnostics:
            print(f"[AUDITS] Missing diagnostics: {missing_diagnostics}")
        
        # Get Core Web Vitals and other metrics
        metrics['metrics'] = {}
        
        # First Contentful Paint (FCP)
        if 'first-contentful-paint' in audits:
            fcp = audits['first-contentful-paint']
            metrics['metrics']['fcp'] = {
                'value': fcp.get('numericValue', 0) / 1000,  # Convert to seconds
                'unit': 's',
                'display_value': fcp.get('displayValue', 'N/A')
            }
        
        # Largest Contentful Paint (LCP)
        if 'largest-contentful-paint' in audits:
            lcp = audits['largest-contentful-paint']
            metrics['metrics']['lcp'] = {
                'value': lcp.get('numericValue', 0) / 1000,  # Convert to seconds
                'unit': 's',
                'display_value': lcp.get('displayValue', 'N/A')
            }
        
        # Cumulative Layout Shift (CLS)
        if 'cumulative-layout-shift' in audits:
            cls = audits['cumulative-layout-shift']
            metrics['metrics']['cls'] = {
                'value': cls.get('numericValue', 0),
                'unit': 'score',
                'display_value': cls.get('displayValue', 'N/A')
            }
        
        # Total Blocking Time (TBT)
        if 'total-blocking-time' in audits:
            tbt = audits['total-blocking-time']
            metrics['metrics']['tbt'] = {
                'value': tbt.get('numericValue', 0),  # Already in milliseconds
                'unit': 'ms',
                'display_value': tbt.get('displayValue', 'N/A')
            }
        
        # Speed Index
        if 'speed-index' in audits:
            si = audits['speed-index']
            metrics['metrics']['speed_index'] = {
                'value': si.get('numericValue', 0) / 1000,  # Convert to seconds
                'unit': 's',
                'display_value': si.get('displayValue', 'N/A')
            }
        
        # Time to Interactive (TTI)
        if 'interactive' in audits:
            tti = audits['interactive']
            metrics['metrics']['tti'] = {
                'value': tti.get('numericValue', 0) / 1000,  # Convert to seconds
                'unit': 's',
                'display_value': tti.get('displayValue', 'N/A')
            }
        
        # Extract opportunities
        metrics['opportunities'] = extract_opportunities(audits)
        
        # Extract diagnostics
        metrics['diagnostics'] = extract_diagnostics(audits)
        
        # Keep backward compatibility - maintain old field structure
        for metric_name, metric_data in metrics['metrics'].items():
            metrics[metric_name] = metric_data
        
        # Log extraction summary
        print(f"[METRICS] Extraction complete:")
        print(f"  - Performance score: {metrics.get('performance_score', 'N/A')}")
        print(f"  - Accessibility score: {metrics.get('accessibility_score', 'N/A')}")
        print(f"  - Best practices score: {metrics.get('best_practices_score', 'N/A')}")
        print(f"  - SEO score: {metrics.get('seo_score', 'N/A')}")
        print(f"  - Opportunities: {len(metrics.get('opportunities', []))}")
        print(f"  - Diagnostics: {len(metrics.get('diagnostics', []))}")
        print(f"  - Core metrics: {len(metrics.get('metrics', {}))}")
        
    except Exception as e:
        print(f"[ERROR] Failed to extract metrics: {str(e)}")
        metrics['error'] = str(e)
    
    return metrics

def extract_opportunities(audits):
    """Extract opportunity audits from PageSpeed data"""
    opportunities = []
    
    # Updated opportunity keys based on actual Lighthouse audit IDs
    opportunity_keys = [
        'render-blocking-resources',
        'unused-javascript', 
        'uses-responsive-images',
        'uses-text-compression',
        'uses-long-cache-ttl',
        # Additional common opportunities
        'uses-webp-images',
        'modern-image-formats',
        'efficient-animated-images',
        'offscreen-images',
        'properly-size-images',
        'unused-css-rules',
        'legacy-javascript',
        'modern-javascript'
    ]
    
    for key in opportunity_keys:
        if key in audits:
            audit = audits[key]
            # Only include opportunities that have room for improvement (score < 1)
            if audit.get('score') is not None and audit.get('score') < 1:
                opportunity = {
                    'id': key,
                    'title': audit.get('title', key),
                    'description': audit.get('description', ''),
                    'score': audit.get('score', 0),
                    'displayValue': audit.get('displayValue', 'N/A'),
                    'numericValue': audit.get('numericValue', 0),
                    'numericUnit': audit.get('numericUnit', ''),
                    'details': audit.get('details', {})
                }
                opportunities.append(opportunity)
                print(f"[OPPORTUNITY] Found: {key} - score={audit.get('score')} - {audit.get('displayValue', 'N/A')}")
            else:
                print(f"[OPPORTUNITY] Skipped (good score): {key} - score={audit.get('score')}")
        else:
            print(f"[OPPORTUNITY] Not found in audits: {key}")
    
    print(f"[OPPORTUNITY] Total opportunities extracted: {len(opportunities)}")
    return opportunities

def extract_diagnostics(audits):
    """Extract diagnostic audits from PageSpeed data"""
    diagnostics = []
    
    # Updated diagnostic keys based on actual Lighthouse audit IDs
    diagnostic_keys = [
        'mainthread-work-breakdown',
        'bootup-time',
        'network-requests',
        # Additional useful diagnostics
        'resource-summary',
        'script-treemap-data',
        'layout-shift-elements',
        'cumulative-layout-shift',
        'largest-contentful-paint',
        'first-contentful-paint',
        'speed-index',
        'interactive',
        'total-blocking-time',
        'server-response-time'
    ]
    
    for key in diagnostic_keys:
        if key in audits:
            audit = audits[key]
            diagnostic = {
                'id': key,
                'title': audit.get('title', key),
                'description': audit.get('description', ''),
                'score': audit.get('score', 0),
                'displayValue': audit.get('displayValue', 'N/A'),
                'numericValue': audit.get('numericValue', 0),
                'numericUnit': audit.get('numericUnit', ''),
                'details': audit.get('details', {})
            }
            diagnostics.append(diagnostic)
            print(f"[DIAGNOSTIC] Found: {key} - score={audit.get('score')} - {audit.get('displayValue', 'N/A')}")
        else:
            print(f"[DIAGNOSTIC] Not found in audits: {key}")
    
    print(f"[DIAGNOSTIC] Total diagnostics extracted: {len(diagnostics)}")
    return diagnostics
