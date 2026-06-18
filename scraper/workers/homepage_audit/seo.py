"""SEO Analysis - Lightweight rule execution for homepage audit"""

from typing import List, Dict


def analyze_seo(seo_data: dict) -> dict:
    """
    Run SEO analysis on scraped data.
    Implements top 10 high-impact SEO rules.
    
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
    
    # Rule 1: Title tag presence (CRITICAL)
    if not seo_data.get('title'):
        add_rule('title_missing', 'fail', 'critical', 'Title tag is missing', 'on_page')
        issues.append({
            'rule_id': 'title_missing',
            'severity': 'critical',
            'message': 'Title tag is missing',
            'category': 'on_page'
        })
    elif len(seo_data.get('title', '')) < 30:
        add_rule('title_too_short', 'fail', 'critical', 'Title tag is too short (less than 30 characters)', 'on_page')
        issues.append({
            'rule_id': 'title_too_short',
            'severity': 'critical',
            'message': 'Title tag is too short (less than 30 characters)',
            'category': 'on_page'
        })
    elif len(seo_data.get('title', '')) > 60:
        add_rule('title_too_long', 'fail', 'medium', 'Title tag is too long (more than 60 characters)', 'on_page')
        issues.append({
            'rule_id': 'title_too_long',
            'severity': 'medium',
            'message': 'Title tag is too long (more than 60 characters)',
            'category': 'on_page'
        })
    else:
        add_rule('title_optimal', 'pass', 'low', 'Title tag length is optimal (30-60 characters)', 'on_page')
    
    # Rule 2: Meta description presence (CRITICAL)
    if not seo_data.get('meta_description'):
        add_rule('meta_description_missing', 'fail', 'critical', 'Meta description is missing', 'on_page')
        issues.append({
            'rule_id': 'meta_description_missing',
            'severity': 'critical',
            'message': 'Meta description is missing',
            'category': 'on_page'
        })
    elif len(seo_data.get('meta_description', '')) < 120:
        add_rule('meta_description_too_short', 'fail', 'critical', 'Meta description is too short (less than 120 characters)', 'on_page')
        issues.append({
            'rule_id': 'meta_description_too_short',
            'severity': 'critical',
            'message': 'Meta description is too short (less than 120 characters)',
            'category': 'on_page'
        })
    else:
        add_rule('meta_description_optimal', 'pass', 'low', 'Meta description length is optimal (120+ characters)', 'on_page')
    
    # Rule 3: H1 tag presence (CRITICAL)
    if not seo_data.get('h1'):
        add_rule('h1_missing', 'fail', 'critical', 'H1 tag is missing', 'on_page')
        issues.append({
            'rule_id': 'h1_missing',
            'severity': 'critical',
            'message': 'H1 tag is missing',
            'category': 'on_page'
        })
    else:
        add_rule('h1_present', 'pass', 'low', 'H1 tag is present', 'on_page')
    
    # Rule 4: HTTPS usage (CRITICAL)
    if not seo_data.get('is_https'):
        add_rule('https_missing_seo', 'fail', 'critical', 'Website is not using HTTPS', 'technical')
        issues.append({
            'rule_id': 'https_missing_seo',
            'severity': 'critical',
            'message': 'Website is not using HTTPS',
            'category': 'technical'
        })
    else:
        add_rule('https_enabled_seo', 'pass', 'low', 'Website is using HTTPS', 'technical')
    
    # Rule 5: Canonical URL (CRITICAL)
    if not seo_data.get('has_canonical'):
        add_rule('canonical_missing', 'fail', 'critical', 'Canonical URL tag is missing', 'technical')
        issues.append({
            'rule_id': 'canonical_missing',
            'severity': 'critical',
            'message': 'Canonical URL tag is missing',
            'category': 'technical'
        })
    else:
        add_rule('canonical_present', 'pass', 'low', 'Canonical URL tag is present', 'technical')
    
    # Rule 6: Robots meta tag (LOW)
    if not seo_data.get('has_robots_meta'):
        add_rule('robots_meta_missing', 'fail', 'low', 'Robots meta tag is missing', 'technical')
        issues.append({
            'rule_id': 'robots_meta_missing',
            'severity': 'low',
            'message': 'Robots meta tag is missing',
            'category': 'technical'
        })
    else:
        add_rule('robots_meta_present', 'pass', 'low', 'Robots meta tag is present', 'technical')
    
    # Rule 8: Favicon (LOW)
    if not seo_data.get('has_favicon'):
        add_rule('favicon_missing', 'fail', 'low', 'Favicon is missing', 'technical')
        issues.append({
            'rule_id': 'favicon_missing',
            'severity': 'low',
            'message': 'Favicon is missing',
            'category': 'technical'
        })
    else:
        add_rule('favicon_present', 'pass', 'low', 'Favicon is present', 'technical')
    
    # Rule 9: Content length (MEDIUM)
    word_count = seo_data.get('word_count', 0)
    if word_count < 300:
        add_rule('content_too_short', 'fail', 'medium', f'Page content is too short ({word_count} words, recommended: 300+)', 'content')
        issues.append({
            'rule_id': 'content_too_short',
            'severity': 'medium',
            'message': f'Page content is too short ({word_count} words, recommended: 300+)',
            'category': 'content'
        })
    else:
        add_rule('content_optimal', 'pass', 'low', f'Page content length is optimal ({word_count} words)', 'content')
    
    # Rule 10: H2 tags (LOW)
    if seo_data.get('h2_count', 0) == 0:
        add_rule('h2_missing', 'fail', 'low', 'No H2 tags found (recommended for content structure)', 'on_page')
        issues.append({
            'rule_id': 'h2_missing',
            'severity': 'low',
            'message': 'No H2 tags found (recommended for content structure)',
            'category': 'on_page'
        })
    else:
        add_rule('h2_present', 'pass', 'low', f'H2 tags found ({seo_data.get("h2_count", 0)} tags)', 'on_page')
    
    # Rule 11: JS-heavy site detection (CRITICAL)
    word_count = seo_data.get('word_count', 0)
    if word_count < 50:
        add_rule('content_not_readable', 'fail', 'critical', f'Low word count ({word_count}) - possible JS-heavy site or blocked content', 'technical')
        issues.append({
            'rule_id': 'content_not_readable',
            'severity': 'critical',
            'message': f'Low word count ({word_count}) - possible JS-heavy site or blocked content',
            'category': 'technical'
        })
    else:
        add_rule('content_readable', 'pass', 'low', 'Content is readable (not JS-heavy)', 'technical')
    
    # Rule 12: HTML lang attribute (MEDIUM)
    html_lang = seo_data.get('html_lang', '')
    if not html_lang:
        add_rule('html_lang_missing', 'fail', 'medium', 'HTML lang attribute is missing', 'technical')
        issues.append({
            'rule_id': 'html_lang_missing',
            'severity': 'medium',
            'message': 'HTML lang attribute is missing',
            'category': 'technical'
        })
    else:
        add_rule('html_lang_present', 'pass', 'low', f'HTML lang attribute is present ({html_lang})', 'technical')
    
    # Rule 13: Hreflang tags (LOW)
    hreflang_tags = seo_data.get('hreflang_tags', [])
    if not hreflang_tags:
        add_rule('hreflang_missing', 'fail', 'low', 'Hreflang tags are missing (recommended for multilingual sites)', 'technical')
        issues.append({
            'rule_id': 'hreflang_missing',
            'severity': 'low',
            'message': 'Hreflang tags are missing (recommended for multilingual sites)',
            'category': 'technical'
        })
    else:
        add_rule('hreflang_present', 'pass', 'low', f'Hreflang tags found ({len(hreflang_tags)} tags)', 'technical')
    
    # Rule 14: Image alt text (MEDIUM)
    image_alt = seo_data.get('image_alt_percentage', {})
    alt_percentage = image_alt.get('percentage', 100)
    if image_alt.get('total', 0) > 0 and alt_percentage < 80:
        add_rule('image_alt_low', 'fail', 'medium', f'Only {alt_percentage}% of images have alt text (recommended: 80%+)', 'on_page')
        issues.append({
            'rule_id': 'image_alt_low',
            'severity': 'medium',
            'message': f'Only {alt_percentage}% of images have alt text (recommended: 80%+)',
            'category': 'on_page'
        })
    else:
        add_rule('image_alt_good', 'pass', 'low', f'Image alt text is good ({alt_percentage}% with alt)', 'on_page')
    
    # Rule 15: Noindex check (CRITICAL)
    if seo_data.get('has_noindex', False):
        add_rule('noindex_detected', 'fail', 'critical', 'Page has noindex directive - will not be indexed by search engines', 'technical')
        issues.append({
            'rule_id': 'noindex_detected',
            'severity': 'critical',
            'message': 'Page has noindex directive - will not be indexed by search engines',
            'category': 'technical'
        })
    else:
        add_rule('noindex_not_detected', 'pass', 'low', 'Page does not have noindex directive', 'technical')
    
    # Rule 16: Robots.txt (LOW)
    robots_txt = seo_data.get('robots_txt', {})
    if not robots_txt.get('exists', False):
        add_rule('robots_txt_missing', 'fail', 'low', 'robots.txt file is missing', 'technical')
        issues.append({
            'rule_id': 'robots_txt_missing',
            'severity': 'low',
            'message': 'robots.txt file is missing',
            'category': 'technical'
        })
    else:
        add_rule('robots_txt_present', 'pass', 'low', 'robots.txt file is present', 'technical')
    
    # Rule 17: Sitemap.xml (LOW)
    sitemap_xml = seo_data.get('sitemap_xml', {})
    if not sitemap_xml.get('exists', False):
        add_rule('sitemap_xml_missing', 'fail', 'low', 'sitemap.xml file is missing', 'technical')
        issues.append({
            'rule_id': 'sitemap_xml_missing',
            'severity': 'low',
            'message': 'sitemap.xml file is missing',
            'category': 'technical'
        })
    else:
        add_rule('sitemap_xml_present', 'pass', 'low', 'sitemap.xml file is present', 'technical')
    
    # Rule 18: HTTP to HTTPS redirect (MEDIUM)
    https_redirect = seo_data.get('https_redirect', {})
    if not https_redirect.get('redirects_to_https', False) and https_redirect.get('original_url', '').startswith('http://'):
        add_rule('https_redirect_missing', 'fail', 'medium', 'HTTP does not redirect to HTTPS', 'technical')
        issues.append({
            'rule_id': 'https_redirect_missing',
            'severity': 'medium',
            'message': 'HTTP does not redirect to HTTPS',
            'category': 'technical'
        })
    else:
        add_rule('https_redirect_good', 'pass', 'low', 'HTTP redirects to HTTPS or already using HTTPS', 'technical')
    
    # Rule 19: Google Analytics (MEDIUM)
    # ADDED: Google Analytics detection rule
    analytics = seo_data.get('analytics', {})
    if analytics.get('detected', False):
        analytics_type = analytics.get('type', 'Unknown')
        add_rule('google_analytics_present', 'pass', 'low', f'Google Analytics is installed ({analytics_type})', 'technical')
    else:
        add_rule('google_analytics_missing', 'fail', 'medium', 'Google Analytics is not detected', 'technical')
        issues.append({
            'rule_id': 'google_analytics_missing',
            'severity': 'medium',
            'message': 'Google Analytics is not detected',
            'category': 'technical'
        })
    
    # Calculate score using strict check-based logic
    score = calculate_seo_score(seo_data)
    
    # Build rules summary
    total = len(rules)
    passed = sum(1 for r in rules if r['status'] == 'pass')
    failed = sum(1 for r in rules if r['status'] == 'fail')
    
    # Limit to top 10 issues for free tier
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


def calculate_seo_score(seo_data: dict) -> int:
    """
    Calculate SEO score based on 17 strict checks from page_info.
    
    Checks:
    1. Title Tag (title_status == "Good")
    2. Meta Description (meta_status == "Good")
    3. Hreflang (len(hreflang_tags) > 0)
    4. Language (bool(html_lang))
    5. H1 Tag (h1_count >= 1)
    6. H2–H6 Tags (h2+h3+h4+h5 > 0)
    7. Content Length (word_count >= 300)
    8. Image Alt (percentage >= 80)
    9. Canonical (bool(canonical_url))
    10. Noindex Tag (has_noindex == False)
    11. Noindex Header (x_robots_tag empty or no "noindex")
    12. SSL (is_https == True)
    13. HTTPS Redirect (redirects_to_https == True)
    14. Robots.txt (sitemap_xml.exists == True)
    15. XML Sitemap (sitemap_xml.exists == True)
    16. Analytics (analytics.detected == True)
    17. Schema (has_schema == True)
    
    Formula: (passed / 17) * 100
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        Score from 0 to 100
    """
    checks = []
    
    # Check 1: Title Tag
    checks.append(seo_data.get('title_status') == "Good")
    
    # Check 2: Meta Description
    checks.append(seo_data.get('meta_status') == "Good")
    
    # Check 3: Hreflang
    checks.append(len(seo_data.get('hreflang_tags', [])) > 0)
    
    # Check 4: Language
    checks.append(bool(seo_data.get('html_lang')))
    
    # Check 5: H1 Tag
    checks.append(seo_data.get('h1_count', 0) >= 1)
    
    # Check 6: H2–H6 Tags
    h2_h6_count = (seo_data.get('h2_count', 0) + 
                   seo_data.get('h3_count', 0) + 
                   seo_data.get('h4_count', 0) + 
                   seo_data.get('h5_count', 0))
    checks.append(h2_h6_count > 0)
    
    # Check 7: Content Length
    checks.append(seo_data.get('word_count', 0) >= 300)
    
    # Check 8: Image Alt
    image_alt = seo_data.get('image_alt_percentage', {})
    checks.append(image_alt.get('percentage', 0) >= 80)
    
    # Check 9: Canonical
    checks.append(bool(seo_data.get('canonical_url')))
    
    # Check 10: Noindex Tag
    checks.append(seo_data.get('has_noindex', False) == False)
    
    # Check 11: Noindex Header
    x_robots = seo_data.get('x_robots_tag', '')
    checks.append(x_robots == "" or "noindex" not in x_robots.lower())
    
    # Check 12: SSL
    checks.append(seo_data.get('is_https', False) == True)
    
    # Check 13: HTTPS Redirect
    https_redirect = seo_data.get('https_redirect', {})
    checks.append(https_redirect.get('redirects_to_https', False) == True)
    
    # Check 14: Robots.txt
    sitemap_xml = seo_data.get('sitemap_xml', {})
    checks.append(sitemap_xml.get('exists', False) == True)
    
    # Check 15: XML Sitemap (same as robots.txt for this check)
    checks.append(sitemap_xml.get('exists', False) == True)
    
    # Check 16: Analytics
    analytics = seo_data.get('analytics', {})
    checks.append(analytics.get('detected', False) == True)
    
    # Check 17: Schema
    checks.append(seo_data.get('has_schema', False) == True)
    
    # Calculate score based on passed checks
    passed = sum(checks)
    total = 17
    
    return round((passed / total) * 100)
