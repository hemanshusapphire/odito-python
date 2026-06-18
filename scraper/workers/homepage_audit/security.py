"""Security Analysis - Lightweight security header audit for homepage audit"""

from typing import List, Dict


def analyze_security(seo_data: dict) -> dict:
    """
    Run security analysis on scraped data.
    Implements 6 critical security header checks.
    
    Args:
        seo_data: Scraped page data from scraper.py
    
    Returns:
        dict with issues list, score, rules_summary, and rules_details
    """
    if 'error' in seo_data:
        return {
            'issues': [],
            'score': 0,
            'error': seo_data.get('error'),
            'rules_summary': {'total': 0, 'passed': 0, 'failed': 0},
            'rules_details': []
        }
    
    issues = []
    rules = []  # Track all rules (pass/fail)
    
    # Helper function to add rule result
    def add_rule(rule_id, status, severity, message, category):
        rules.append({
            'rule_id': rule_id,
            'status': status,
            'severity': severity,
            'message': message,
            'category': category
        })
    
    # Get security headers from scraped data
    security_headers = seo_data.get('security_headers', {})
    
    # Security Check 1: Content Security Policy (HIGH)
    csp_header = security_headers.get('content_security_policy', {})
    if not csp_header.get('present', False):
        add_rule('csp_missing', 'fail', 'high', 'Content Security Policy header is missing.', 'security')
        issues.append({
            'rule_id': 'csp_missing',
            'severity': 'high',
            'message': 'Content Security Policy header is missing.',
            'category': 'security'
        })
    else:
        add_rule('csp_present', 'pass', 'low', 'Content Security Policy header detected.', 'security')
    
    # Security Check 2: HSTS (HIGH)
    hsts_header = security_headers.get('strict_transport_security', {})
    if not hsts_header.get('present', False):
        add_rule('hsts_missing', 'fail', 'high', 'Strict-Transport-Security header is missing.', 'security')
        issues.append({
            'rule_id': 'hsts_missing',
            'severity': 'high',
            'message': 'Strict-Transport-Security header is missing.',
            'category': 'security'
        })
    else:
        add_rule('hsts_present', 'pass', 'low', 'Strict-Transport-Security header detected.', 'security')
    
    # Security Check 3: X-Frame-Options (HIGH)
    x_frame_header = security_headers.get('x_frame_options', {})
    if not x_frame_header.get('present', False):
        add_rule('x_frame_missing', 'fail', 'high', 'X-Frame-Options header is missing.', 'security')
        issues.append({
            'rule_id': 'x_frame_missing',
            'severity': 'high',
            'message': 'X-Frame-Options header is missing.',
            'category': 'security'
        })
    else:
        add_rule('x_frame_present', 'pass', 'low', 'X-Frame-Options header detected.', 'security')
    
    # Security Check 4: X-Content-Type-Options (MEDIUM)
    x_content_type_header = security_headers.get('x_content_type_options', {})
    if not x_content_type_header.get('present', False):
        add_rule('x_content_type_missing', 'fail', 'medium', 'X-Content-Type-Options header is missing.', 'security')
        issues.append({
            'rule_id': 'x_content_type_missing',
            'severity': 'medium',
            'message': 'X-Content-Type-Options header is missing.',
            'category': 'security'
        })
    else:
        add_rule('x_content_type_present', 'pass', 'low', 'X-Content-Type-Options header detected.', 'security')
    
    # Security Check 5: Referrer Policy (MEDIUM)
    referrer_header = security_headers.get('referrer_policy', {})
    if not referrer_header.get('present', False):
        add_rule('referrer_policy_missing', 'fail', 'medium', 'Referrer-Policy header is missing.', 'security')
        issues.append({
            'rule_id': 'referrer_policy_missing',
            'severity': 'medium',
            'message': 'Referrer-Policy header is missing.',
            'category': 'security'
        })
    else:
        add_rule('referrer_policy_present', 'pass', 'low', 'Referrer-Policy header detected.', 'security')
    
    # Security Check 6: Permissions Policy (MEDIUM)
    permissions_header = security_headers.get('permissions_policy', {})
    if not permissions_header.get('present', False):
        add_rule('permissions_policy_missing', 'fail', 'medium', 'Permissions-Policy header is missing.', 'security')
        issues.append({
            'rule_id': 'permissions_policy_missing',
            'severity': 'medium',
            'message': 'Permissions-Policy header is missing.',
            'category': 'security'
        })
    else:
        add_rule('permissions_policy_present', 'pass', 'low', 'Permissions-Policy header detected.', 'security')
    
    # Calculate security score using deterministic logic
    score = calculate_security_score(security_headers)
    
    # Build rules summary
    total = len(rules)
    passed = sum(1 for r in rules if r['status'] == 'pass')
    failed = sum(1 for r in rules if r['status'] == 'fail')
    
    # Limit issues for free tier (all security issues are important)
    issues = issues[:10]
    
    return {
        'issues': issues,
        'score': score,
        'rules_summary': {
            'total': total,
            'passed': passed,
            'failed': failed
        },
        'rules_details': rules
    }


def calculate_security_score(security_headers: dict) -> int:
    """
    Calculate security score based on 6 security header checks.
    
    Scoring:
    - 6 headers present: 100
    - 5 headers present: 85
    - 4 headers present: 70
    - 3 headers present: 50
    - 2 headers present: 35
    - 1 header present: 20
    - 0 headers present: 0
    
    Args:
        security_headers: Security headers data from scraper
    
    Returns:
        Score from 0 to 100
    """
    # Count present security headers
    present_count = 0
    
    header_checks = [
        'content_security_policy',
        'strict_transport_security', 
        'x_frame_options',
        'x_content_type_options',
        'referrer_policy',
        'permissions_policy'
    ]
    
    for header_name in header_checks:
        header_data = security_headers.get(header_name, {})
        if header_data.get('present', False):
            present_count += 1
    
    # Deterministic scoring based on present headers
    score_mapping = {
        6: 100,
        5: 85,
        4: 70,
        3: 50,
        2: 35,
        1: 20,
        0: 0
    }
    
    return score_mapping.get(present_count, 0)
