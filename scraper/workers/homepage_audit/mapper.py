"""Response Mapper - Format audit results for API response"""

from .security import analyze_security


# Rule mappings for section checks
ON_PAGE_RULES = [
    {"rule_id": "title_optimal", "name": "Title Tag"},
    {"rule_id": "meta_description_optimal", "name": "Meta Description"},
    {"rule_id": "hreflang_missing", "name": "Hreflang"},
    {"rule_id": "html_lang_present", "name": "Language"},
    {"rule_id": "h1_present", "name": "H1 Tag"},
    {"rule_id": "h2_present", "name": "H2–H6 Tags"},
    {"rule_id": "content_optimal", "name": "Content Length"},
    {"rule_id": "image_alt_low", "name": "Image Alt"},
    {"rule_id": "noindex_not_detected", "name": "Noindex Tag"},
    {"rule_id": "robots_meta_present", "name": "Robots Meta"},
    {"rule_id": "google_analytics_present", "name": "Analytics"}
]

TECHNICAL_RULES = [
    {"rule_id": "canonical_present", "name": "Canonical"},
    {"rule_id": "https_enabled_seo", "name": "SSL"},
    {"rule_id": "https_redirect_good", "name": "HTTPS Redirect"},
    {"rule_id": "robots_txt_present", "name": "Robots.txt"},
    {"rule_id": "sitemap_xml_present", "name": "XML Sitemap"},
    {"rule_id": "schema_present", "name": "Schema"}
]

AI_RULES = [
    {"rule_id": "identity_schema", "name": "Identity Schema"},
    {"rule_id": "llm_readability", "name": "LLM Readability"},
    {"rule_id": "llms_txt", "name": "llms.txt"},
    {"rule_id": "structured_data_depth", "name": "Structured Data Depth"},
    {"rule_id": "faq_optimization", "name": "FAQ Optimization"},
    {"rule_id": "content_chunking", "name": "Content Chunking"}
]

# AI check weights for weighted scoring
AI_WEIGHTS = {
    "Identity Schema": 10,
    "LLM Readability": 20,
    "llms.txt": 25,
    "Structured Data Depth": 15,
    "FAQ Optimization": 15,
    "Content Chunking": 15
}

PERFORMANCE_RULES = [
    {"rule_id": "load_time_fast", "name": "Load Time"}
]

SECURITY_RULES = [
    {"rule_id": "csp_present", "name": "Content Security Policy"},
    {"rule_id": "hsts_present", "name": "HSTS"},
    {"rule_id": "x_frame_present", "name": "X-Frame-Options"},
    {"rule_id": "x_content_type_present", "name": "X-Content-Type-Options"},
    {"rule_id": "referrer_policy_present", "name": "Referrer Policy"},
    {"rule_id": "permissions_policy_present", "name": "Permissions Policy"}
]


def build_section_checks(rule_mappings, rules_details):
    """
    Build section checks dynamically from rules_details.
    
    Args:
        rule_mappings: List of {rule_id, name} mappings
        rules_details: List of all rule results with status
    
    Returns:
        List of {name, status} checks
    """
    checks = []
    
    for rule in rule_mappings:
        match = next(
            (r for r in rules_details if r["rule_id"] == rule["rule_id"]),
            None
        )
        
        # If rule not found, default to fail
        status = match["status"] if match else "fail"
        
        checks.append({
            "name": rule["name"],
            "status": status
        })
    
    return checks


def build_ai_checks(seo_data: dict) -> list:
    """
    Build AI (GEO) section checks from page_info data.
    
    6 checks with weighted scoring:
    - Identity Schema (10)
    - LLM Readability (20)
    - llms.txt (25)
    - Structured Data Depth (15)
    - FAQ Optimization (15)
    - Content Chunking (15)
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        List of {name, status} checks for AI section
    """
    checks = []
    
    # Check 1: Identity Schema - pass if present (even via fallback)
    identity_schema = seo_data.get('identity_schema', {})
    identity_pass = identity_schema.get('present', False) == True
    checks.append({
        "name": "Identity Schema",
        "rule_id": "identity_schema_missing",
        "status": "pass" if identity_pass else "fail",
        "severity": "low" if identity_pass else "high",
        "message": "Identity schema is present for AI understanding" if identity_pass else "Identity schema is missing - AI cannot identify your organization",
        "category": "ai"
    })
    
    # Check 2: LLM Readability - fail if score < 70 OR is_js_heavy
    llm_readability = seo_data.get('llm_readability', {})
    readability_score = llm_readability.get('score', 0)
    is_js_heavy = llm_readability.get('is_js_heavy', False)
    readability_pass = readability_score >= 70 and not is_js_heavy
    checks.append({
        "name": "LLM Readability",
        "rule_id": "llm_readability_poor",
        "status": "pass" if readability_pass else "fail",
        "severity": "low" if readability_pass else "high",
        "message": "Content structure is optimized for LLMs" if readability_pass else "Content structure is difficult for LLMs to understand",
        "category": "ai"
    })
    
    # Check 3: llms.txt - fail if not found
    llms_txt = seo_data.get('llms_txt', {})
    llms_pass = llms_txt.get('exists', False) == True
    checks.append({
        "name": "llms.txt",
        "rule_id": "llms_txt_missing",
        "status": "pass" if llms_pass else "fail",
        "severity": "low" if llms_pass else "high",
        "message": "llms.txt file exists for AI search engines" if llms_pass else "llms.txt file is missing - AI search engines can't discover your content",
        "category": "ai"
    })
    
    # Check 4: Structured Data Depth - fail unless multiple schema types detected
    schema_info = seo_data.get('schema_info', {})
    schema_types = schema_info.get('types_detected', [])
    structured_depth_pass = len(schema_types) >= 2
    checks.append({
        "name": "Structured Data Depth",
        "rule_id": "structured_data_limited",
        "status": "pass" if structured_depth_pass else "fail",
        "severity": "low" if structured_depth_pass else "high",
        "message": "Multiple schema types detected for rich AI understanding" if structured_depth_pass else "Limited structured data - AI understanding is restricted",
        "category": "ai"
    })
    
    # Check 5: FAQ Optimization - fail unless rich/optimized FAQ exists
    # Requires more than just FAQ schema presence
    has_faq_schema = 'FAQPage' in schema_types
    faq_pass = has_faq_schema and len(schema_types) >= 2 and readability_score >= 60
    checks.append({
        "name": "FAQ Optimization",
        "rule_id": "faq_not_optimized",
        "status": "pass" if faq_pass else "fail",
        "severity": "low" if faq_pass else "high",
        "message": "FAQ is properly optimized for AI search" if faq_pass else "FAQ is not optimized for AI search engines",
        "category": "ai"
    })
    
    # Check 6: Content Chunking - pass if headings exist (H1 + H2 structure)
    h1_count = seo_data.get('h1_count', 0)
    h2_count = seo_data.get('h2_count', 0)
    chunking_pass = h1_count >= 1 and h2_count >= 2
    checks.append({
        "name": "Content Chunking",
        "rule_id": "content_chunking",
        "status": "pass" if chunking_pass else "fail",
        "severity": "low" if chunking_pass else "medium",
        "message": "Content is properly chunked with headings for AI understanding" if chunking_pass else "Content lacks proper heading structure for AI understanding",
        "category": "ai"
    })
    
    return checks


def build_on_page_checks(seo_data: dict) -> list:
    """
    Build ON-PAGE section checks from page_info data.
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        List of {name, status} checks for ON-PAGE section
    """
    checks = []
    
    # Check 1: Title Tag
    title_status = seo_data.get('title_status') == "Good"
    checks.append({
        "name": "Title Tag",
        "rule_id": "title_tag",
        "status": "pass" if title_status else "fail",
        "severity": "low" if title_status else "medium",
        "message": "Title tag is properly formatted" if title_status else "Title tag is missing or improperly formatted",
        "category": "on_page"
    })
    
    # Check 2: Meta Description
    meta_status = seo_data.get('meta_status') == "Good"
    checks.append({
        "name": "Meta Description",
        "rule_id": "meta_description",
        "status": "pass" if meta_status else "fail",
        "severity": "low" if meta_status else "medium",
        "message": "Meta description is properly formatted" if meta_status else "Meta description is missing or too short",
        "category": "on_page"
    })
    
    # Check 3: Hreflang
    hreflang_pass = len(seo_data.get('hreflang_tags', [])) > 0
    checks.append({
        "name": "Hreflang",
        "rule_id": "hreflang_missing",
        "status": "pass" if hreflang_pass else "fail",
        "severity": "low" if hreflang_pass else "high",
        "message": "Hreflang tags are present for international SEO" if hreflang_pass else "Hreflang tags are missing for international SEO",
        "category": "on_page"
    })
    
    # Check 4: Language
    lang_pass = bool(seo_data.get('html_lang'))
    checks.append({
        "name": "Language",
        "rule_id": "html_lang",
        "status": "pass" if lang_pass else "fail",
        "severity": "low" if lang_pass else "medium",
        "message": "HTML lang attribute is set" if lang_pass else "HTML lang attribute is missing",
        "category": "on_page"
    })
    
    # Check 5: H1 Tag
    h1_pass = seo_data.get('h1_count', 0) >= 1
    checks.append({
        "name": "H1 Tag",
        "rule_id": "h1_missing",
        "status": "pass" if h1_pass else "fail",
        "severity": "low" if h1_pass else "high",
        "message": "H1 tag is present" if h1_pass else "H1 tag is missing",
        "category": "on_page"
    })
    
    # Check 6: H2–H6 Tags
    h2_h6_count = (seo_data.get('h2_count', 0) + 
                   seo_data.get('h3_count', 0) + 
                   seo_data.get('h4_count', 0) + 
                   seo_data.get('h5_count', 0))
    h2_h6_pass = h2_h6_count > 0
    checks.append({
        "name": "H2–H6 Tags",
        "rule_id": "h2_h6_structure",
        "status": "pass" if h2_h6_pass else "fail",
        "severity": "low" if h2_h6_pass else "medium",
        "message": "Content has proper heading structure" if h2_h6_pass else "Content lacks proper heading structure",
        "category": "on_page"
    })
    
    # Check 7: Content Length
    content_pass = seo_data.get('word_count', 0) >= 300
    checks.append({
        "name": "Content Length",
        "rule_id": "content_length",
        "status": "pass" if content_pass else "fail",
        "severity": "low" if content_pass else "medium",
        "message": "Content has sufficient length for SEO" if content_pass else "Content is too short for optimal SEO",
        "category": "on_page"
    })
    
    # Check 8: Image Alt
    image_alt = seo_data.get('image_alt_percentage', {})
    alt_pass = image_alt.get('percentage', 0) >= 80
    checks.append({
        "name": "Image Alt",
        "rule_id": "image_alt_text",
        "status": "pass" if alt_pass else "fail",
        "severity": "low" if alt_pass else "medium",
        "message": "Images have proper alt text" if alt_pass else "Many images lack alt text",
        "category": "on_page"
    })
    
    # Check 9: Noindex Tag
    noindex_pass = seo_data.get('has_noindex', False) == False
    checks.append({
        "name": "Noindex Tag",
        "rule_id": "noindex_tag_detected",
        "status": "pass" if noindex_pass else "fail",
        "severity": "low" if noindex_pass else "high",
        "message": "No noindex tag blocking search indexing" if noindex_pass else "Noindex tag detected - page won't be indexed",
        "category": "on_page"
    })
    
    # Check 10: Noindex Header
    x_robots = seo_data.get('x_robots_tag', '')
    x_robots_pass = x_robots == "" or "noindex" not in x_robots.lower()
    checks.append({
        "name": "Noindex Header",
        "rule_id": "noindex_header_detected",
        "status": "pass" if x_robots_pass else "fail",
        "severity": "low" if x_robots_pass else "high",
        "message": "No noindex header blocking search indexing" if x_robots_pass else "Noindex header detected - page won't be indexed",
        "category": "on_page"
    })
    
    # Check 11: Analytics
    analytics = seo_data.get('analytics', {})
    analytics_pass = analytics.get('detected', False) == True
    checks.append({
        "name": "Analytics",
        "rule_id": "analytics_missing",
        "status": "pass" if analytics_pass else "fail",
        "severity": "low" if analytics_pass else "medium",
        "message": "Analytics tracking is properly configured" if analytics_pass else "Analytics tracking is missing",
        "category": "on_page"
    })
    
    return checks


def build_technical_checks(seo_data: dict) -> list:
    """
    Build TECHNICAL section checks from page_info data.
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        List of {name, status} checks for TECHNICAL section
    """
    checks = []
    
    # Check 1: Canonical
    canonical_pass = bool(seo_data.get('canonical_url'))
    checks.append({
        "name": "Canonical",
        "rule_id": "canonical_missing",
        "status": "pass" if canonical_pass else "fail",
        "severity": "low" if canonical_pass else "medium",
        "message": "Canonical URL is properly set" if canonical_pass else "Canonical URL is missing",
        "category": "technical"
    })
    
    # Check 2: SSL
    ssl_pass = seo_data.get('is_https', False) == True
    checks.append({
        "name": "SSL",
        "rule_id": "ssl_certificate",
        "status": "pass" if ssl_pass else "fail",
        "severity": "low" if ssl_pass else "high",
        "message": "SSL certificate is properly configured" if ssl_pass else "SSL certificate is missing or invalid",
        "category": "technical"
    })
    
    # Check 3: HTTPS Redirect
    https_redirect = seo_data.get('https_redirect', {})
    redirect_pass = https_redirect.get('redirects_to_https', False) == True
    checks.append({
        "name": "HTTPS Redirect",
        "rule_id": "https_redirect",
        "status": "pass" if redirect_pass else "fail",
        "severity": "low" if redirect_pass else "medium",
        "message": "HTTP properly redirects to HTTPS" if redirect_pass else "HTTP does not redirect to HTTPS",
        "category": "technical"
    })
    
    # Check 4: Robots.txt
    robots_txt = seo_data.get('robots_txt', {})
    robots_pass = robots_txt.get('exists', False) == True
    checks.append({
        "name": "Robots.txt",
        "rule_id": "robots_txt",
        "status": "pass" if robots_pass else "fail",
        "severity": "low" if robots_pass else "medium",
        "message": "Robots.txt file exists and is accessible" if robots_pass else "Robots.txt file is missing",
        "category": "technical"
    })
    
    # Check 5: XML Sitemap
    sitemap_xml = seo_data.get('sitemap_xml', {})
    sitemap_pass = sitemap_xml.get('exists', False) == True
    checks.append({
        "name": "XML Sitemap",
        "rule_id": "xml_sitemap",
        "status": "pass" if sitemap_pass else "fail",
        "severity": "low" if sitemap_pass else "medium",
        "message": "XML sitemap exists and is accessible" if sitemap_pass else "XML sitemap is missing",
        "category": "technical"
    })
    
    # Check 6: Schema
    schema_pass = seo_data.get('has_schema', False) == True
    checks.append({
        "name": "Schema",
        "rule_id": "structured_data",
        "status": "pass" if schema_pass else "fail",
        "severity": "low" if schema_pass else "medium",
        "message": "Structured data (schema) is present" if schema_pass else "Structured data (schema) is missing",
        "category": "technical"
    })
    
    return checks


def generate_issues_from_checks(on_page_checks: list, technical_checks: list, ai_checks: list, performance_checks: list, security_checks: list) -> list:
    """
    Generate issues from failed section checks dynamically.
    
    Args:
        on_page_checks: List of {name, status} for on-page section
        technical_checks: List of {name, status} for technical section  
        ai_checks: List of {name, status} for AI section
        performance_checks: List of {name, status} for performance section
        security_checks: List of {name, status} for security section
    
    Returns:
        List of issue objects for failed checks
    """
    issues = []
    
    # Mapping of check names to issue details
    check_mappings = {
        # AI Checks (High Impact)
        "Identity Schema": {
            "rule_id": "identity_schema_missing",
            "severity": "high",
            "message": "No identity schema detected. Add Organization or Person schema for better AI understanding.",
            "category": "ai"
        },
        "LLM Readability": {
            "rule_id": "llm_readability_poor", 
            "severity": "high",
            "message": "Content structure could be improved for LLMs. Restructure content for better AI readability.",
            "category": "ai"
        },
        "llms.txt": {
            "rule_id": "llms_txt_missing",
            "severity": "high", 
            "message": "llms.txt not found. Add llms.txt to improve AI discoverability for AI search engines.",
            "category": "ai"
        },
        "Structured Data Depth": {
            "rule_id": "structured_data_limited",
            "severity": "high",
            "message": "Your structured data is limited. Add more schema types for better AI understanding.",
            "category": "ai"
        },
        "FAQ Optimization": {
            "rule_id": "faq_not_optimized",
            "severity": "high",
            "message": "Improve FAQ content to enhance visibility in AI search results and featured snippets.",
            "category": "ai"
        },
        "Content Chunking": {
            "rule_id": "content_structure_poor",
            "severity": "high",
            "message": "Content lacks proper heading structure. Add H2 sections for better content organization.",
            "category": "ai"
        },
        
        # Technical Checks (Low/Medium Impact)
        "Canonical": {
            "rule_id": "canonical_missing",
            "severity": "medium",
            "message": "Canonical tag missing. Add canonical URL to prevent duplicate content issues.",
            "category": "technical"
        },
        "SSL": {
            "rule_id": "ssl_missing",
            "severity": "high",
            "message": "SSL certificate not found. Enable HTTPS for security and better rankings.",
            "category": "technical"
        },
        "HTTPS Redirect": {
            "rule_id": "https_redirect_missing",
            "severity": "high",
            "message": "HTTP doesn't redirect to HTTPS. Set up proper HTTPS redirect.",
            "category": "technical"
        },
        "Robots.txt": {
            "rule_id": "robots_txt_missing",
            "severity": "high",
            "message": "Robots.txt file not found. Add robots.txt to guide search engine crawling.",
            "category": "technical"
        },
        "XML Sitemap": {
            "rule_id": "sitemap_missing",
            "severity": "high",
            "message": "XML sitemap not found. Add sitemap to help search engines discover your content.",
            "category": "technical"
        },
        "Schema": {
            "rule_id": "schema_missing",
            "severity": "medium",
            "message": "Schema markup not found. Add structured data for better search visibility.",
            "category": "technical"
        },
        
        # On-Page Checks (Low/Medium Impact)
        "Title Tag": {
            "rule_id": "title_missing",
            "severity": "high",
            "message": "Title tag missing or too short. Add compelling title (50-60 characters).",
            "category": "on_page"
        },
        "Meta Description": {
            "rule_id": "meta_missing",
            "severity": "medium",
            "message": "Meta description missing. Add description (150-160 characters) for better click-through rates.",
            "category": "on_page"
        },
        "H1 Tag": {
            "rule_id": "h1_missing",
            "severity": "high",
            "message": "H1 tag missing. Add single H1 tag for better content structure.",
            "category": "on_page"
        },
        "Image Alt": {
            "rule_id": "image_alt_low",
            "severity": "medium",
            "message": "Many images lack alt text. Add alt text for accessibility and image SEO.",
            "category": "on_page"
        },
        "Language": {
            "rule_id": "lang_missing",
            "severity": "low",
            "message": "HTML lang attribute missing. Add lang attribute for better international SEO.",
            "category": "on_page"
        },
        "Hreflang": {
            "rule_id": "hreflang_missing",
            "severity": "high",
            "message": "Hreflang tags missing. Add hreflang for international SEO targeting.",
            "category": "on_page"
        },
        "Noindex Tag": {
            "rule_id": "noindex_tag_detected",
            "severity": "high",
            "message": "Noindex tag detected. This page will not be indexed by search engines.",
            "category": "on_page"
        },
        "Noindex Header": {
            "rule_id": "noindex_header_detected",
            "severity": "high",
            "message": "Noindex header detected. This page will not be indexed by search engines.",
            "category": "on_page"
        },
        "Analytics": {
            "rule_id": "analytics_missing",
            "severity": "medium",
            "message": "Analytics tracking not detected. Add Google Analytics for visitor insights.",
            "category": "on_page"
        },
        
        # Performance Checks (High Impact)
        "Load Time": {
            "rule_id": "load_time_slow",
            "severity": "high",
            "message": "Page load time is slow. Optimize images, caching, and server response time.",
            "category": "performance"
        },
        
        # Security Checks (High/Medium Impact)
        "Content Security Policy": {
            "rule_id": "csp_missing",
            "severity": "high",
            "message": "Content Security Policy header is missing. Add CSP to prevent XSS attacks.",
            "category": "security"
        },
        "HSTS": {
            "rule_id": "hsts_missing",
            "severity": "high",
            "message": "Strict-Transport-Security header is missing. Add HSTS to enforce HTTPS.",
            "category": "security"
        },
        "X-Frame-Options": {
            "rule_id": "x_frame_missing",
            "severity": "high",
            "message": "X-Frame-Options header is missing. Add XFO to prevent clickjacking attacks.",
            "category": "security"
        },
        "X-Content-Type-Options": {
            "rule_id": "x_content_type_missing",
            "severity": "medium",
            "message": "X-Content-Type-Options header is missing. Add XCTO to prevent MIME sniffing.",
            "category": "security"
        },
        "Referrer Policy": {
            "rule_id": "referrer_policy_missing",
            "severity": "medium",
            "message": "Referrer-Policy header is missing. Add referrer policy to control referrer information.",
            "category": "security"
        },
        "Permissions Policy": {
            "rule_id": "permissions_policy_missing",
            "severity": "medium",
            "message": "Permissions-Policy header is missing. Add permissions policy to control browser features.",
            "category": "security"
        }
    }
    
    # Process all section checks
    all_checks = [
        (on_page_checks, "on_page"),
        (technical_checks, "technical"), 
        (ai_checks, "ai"),
        (performance_checks, "performance"),
        (security_checks, "security")
    ]
    
    for checks, section in all_checks:
        for check in checks:
            if check.get('status') == 'fail':
                check_name = check.get('name')
                mapping = check_mappings.get(check_name)
                
                if mapping:
                    issues.append({
                        'rule_id': mapping['rule_id'],
                        'status': 'fail',
                        'severity': mapping['severity'],
                        'message': mapping['message'],
                        'category': mapping['category']
                    })
                else:
                    # Default issue for unmapped checks
                    issues.append({
                        'rule_id': f"{check_name.lower().replace(' ', '_')}_fail",
                        'status': 'fail',
                        'severity': 'low',
                        'message': f"{check_name} check failed. Review and fix this issue.",
                        'category': section
                    })
    
    return issues


def build_performance_checks(seo_data: dict) -> list:
    """
    Build PERFORMANCE section checks from page_info data.
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        List of {name, status} checks for PERFORMANCE section
    """
    checks = []
    
    # Check 1: Load Time
    response_time_ms = seo_data.get('response_time_ms', 0)
    load_time_pass = response_time_ms <= 3000
    checks.append({
        "name": "Load Time",
        "rule_id": "load_time_slow",
        "status": "pass" if load_time_pass else "fail",
        "severity": "low" if load_time_pass else "high",
        "message": "Page loads quickly (≤3 seconds)" if load_time_pass else "Page load time is slow (>3 seconds)",
        "category": "performance"
    })
    
    # Check 2: Mobile Viewport
    viewport_pass = seo_data.get('has_viewport', False) == True
    checks.append({
        "name": "Mobile Viewport",
        "rule_id": "mobile_viewport",
        "status": "pass" if viewport_pass else "fail",
        "severity": "low" if viewport_pass else "medium",
        "message": "Mobile viewport meta tag is properly configured" if viewport_pass else "Mobile viewport meta tag is missing",
        "category": "performance"
    })
    
    return checks


def build_security_checks(security_result: dict) -> list:
    """
    Build SECURITY section checks from security analysis results.
    
    Args:
        security_result: Security analysis result with rules_details
    
    Returns:
        List of {name, status} checks for SECURITY section
    """
    checks = []
    
    # Get security rules from security analysis
    security_rules = security_result.get('rules_details', [])
    
    # Rule ID to name mapping for frontend display
    rule_name_mapping = {
        "csp_missing": "Content Security Policy",
        "hsts_missing": "HSTS", 
        "x_frame_missing": "X-Frame-Options",
        "x_content_type_missing": "X-Content-Type-Options",
        "referrer_policy_missing": "Referrer Policy",
        "permissions_policy_missing": "Permissions Policy"
    }
    
    # Define specific severity levels for security rules
    security_severity_map = {
        "csp_missing": "high",
        "hsts_missing": "high", 
        "x_frame_missing": "high",
        "x_content_type_missing": "medium",
        "referrer_policy_missing": "medium",
        "permissions_policy_missing": "medium"
    }
    
    # Build checks from security rules
    for rule in security_rules:
        rule_id = rule.get('rule_id', '')
        status = rule.get('status', 'fail')
        
        # Only process security-related rules
        if rule_id in rule_name_mapping:
            # Use specific severity for failed checks, low for passed checks
            severity = security_severity_map.get(rule_id, 'medium') if status == 'fail' else 'low'
            
            checks.append({
                "name": rule_name_mapping[rule_id],
                "rule_id": rule_id,
                "status": status,
                "severity": severity,
                "message": rule.get('message', f'{rule_name_mapping[rule_id]} is {"missing" if status == "fail" else "present"}'),
                "category": "security"
            })
    
    return checks


def build_accessibility_checks(accessibility_metrics: dict) -> list:
    """
    Build ACCESSIBILITY section checks from accessibility analysis results.
    
    Converts raw accessibility_metrics.issues into canonical visible checks
    following the SAME architecture as all other sections.
    
    Args:
        accessibility_metrics: Accessibility metrics dict with issues and landmarks
    
    Returns:
        List of {name, rule_id, status, severity, message, category} checks for ACCESSIBILITY section
    """
    checks = []
    
    # If accessibility is still processing or not available, return empty
    if not accessibility_metrics or accessibility_metrics.get('status') == 'processing':
        return checks
    
    issues = accessibility_metrics.get('issues', {})
    landmarks = accessibility_metrics.get('landmarks', {})
    
    # Define canonical accessibility checks with explicit severity
    accessibility_check_defs = [
        {
            'name': 'Axe Violations',
            'rule_id': 'axe_violations',
            'count': issues.get('axe_violations', 0),
            'fail_severity': 'critical',
            'fail_message': lambda c: f'Found {c} axe-core accessibility violations',
            'pass_message': 'No axe-core accessibility violations detected'
        },
        {
            'name': 'Missing Labels',
            'rule_id': 'missing_form_labels',
            'count': issues.get('missing_labels', 0),
            'fail_severity': 'critical',
            'fail_message': lambda c: f'Found {c} form elements missing accessible labels',
            'pass_message': 'All form elements have accessible labels'
        },
        {
            'name': 'Focus Issues',
            'rule_id': 'focus_issues',
            'count': issues.get('focus_issues', 0),
            'fail_severity': 'medium',
            'fail_message': lambda c: f'Found {c} focus management issues affecting keyboard navigation',
            'pass_message': 'Focus management is properly implemented'
        },
        {
            'name': 'Clickable Divs',
            'rule_id': 'clickable_divs',
            'count': issues.get('clickable_divs', 0),
            'fail_severity': 'medium',
            'fail_message': lambda c: f'Found {c} clickable div elements that should be buttons or links',
            'pass_message': 'All clickable elements use proper semantic HTML'
        },
        {
            'name': 'Autoplay Media',
            'rule_id': 'autoplay_media',
            'count': issues.get('autoplay_media', 0),
            'fail_severity': 'medium',
            'fail_message': lambda c: f'Found {c} autoplaying media elements that may interfere with screen readers',
            'pass_message': 'No autoplaying media detected'
        },
        {
            'name': 'Main Landmark',
            'rule_id': 'landmark_main',
            'count': 0 if landmarks.get('main') else 1,
            'fail_severity': 'medium',
            'fail_message': lambda c: 'Missing main landmark. Screen readers cannot easily navigate to primary content',
            'pass_message': 'Main landmark detected'
        },
        {
            'name': 'Navigation Landmark',
            'rule_id': 'landmark_nav',
            'count': 0 if landmarks.get('nav') else 1,
            'fail_severity': 'medium',
            'fail_message': lambda c: 'Missing navigation landmark. Screen reader users cannot easily access navigation menus',
            'pass_message': 'Navigation landmark detected'
        },
        {
            'name': 'Header Landmark',
            'rule_id': 'landmark_header',
            'count': 0 if landmarks.get('header') else 1,
            'fail_severity': 'low',
            'fail_message': lambda c: 'Missing header landmark. Inconsistent page structure for screen reader users',
            'pass_message': 'Header landmark detected'
        },
        {
            'name': 'Footer Landmark',
            'rule_id': 'landmark_footer',
            'count': 0 if landmarks.get('footer') else 1,
            'fail_severity': 'low',
            'fail_message': lambda c: 'Missing footer landmark. Inconsistent page structure for screen reader users',
            'pass_message': 'Footer landmark detected'
        }
    ]
    
    for check_def in accessibility_check_defs:
        count = check_def['count']
        status = 'fail' if count > 0 else 'pass'
        checks.append({
            'name': check_def['name'],
            'rule_id': check_def['rule_id'],
            'status': status,
            'severity': check_def['fail_severity'] if status == 'fail' else 'low',
            'message': check_def['fail_message'](count) if status == 'fail' else check_def['pass_message'],
            'category': 'accessibility'
        })
    
    return checks


def calculate_score_from_rules(rules):
    """
    Calculate score from rules based on pass/fail ratio.
    
    Formula: (passed / total) * 100
    
    Args:
        rules: List of rule dicts with status
    
    Returns:
        Score from 0 to 100
    """
    if not rules:
        return 0
    
    passed = sum(1 for r in rules if r["status"] == "pass")
    total = len(rules)
    
    return int((passed / total) * 100)


def get_grade(score):
    """
    Get grade letter from score.
    
    Args:
        score: Score from 0 to 100
    
    Returns:
        Grade letter (A, B, C, F)
    """
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 50:
        return "C"
    return "F"


def normalize_severity(value):
    """
    Normalize severity to only allow: critical, medium, low
    
    Mapping:
    - critical → critical
    - high → critical
    - medium → medium
    - warning → medium
    - low → low
    - info → low
    """
    if not value:
        return 'low'
    
    value = value.lower()
    
    if value in ['critical', 'high']:
        return 'critical'
    elif value in ['medium', 'warning']:
        return 'medium'
    else:
        return 'low'


def get_canonical_severity_count(checks):
    """
    Get canonical severity counts from visible section checks.
    
    Args:
        checks: List of {name, status} checks from a section
    
    Returns:
        dict with critical, warnings, passed counts
    """
    critical = 0
    warnings = 0
    passed = 0
    
    for check in checks:
        if check.get('status') == 'pass':
            passed += 1
        else:
            # For failed checks, we need to map to severity
            # This will be handled at the issue level, not check level
            warnings += 1
    
    return {
        'critical': critical,
        'warnings': warnings,
        'passed': passed
    }


def generate_sections_summary(sections_dict):
    """
    Generate canonical summary from ALL visible section checks ONLY.
    
    Single source of truth: sections[].checks
    No mixing of data sources. No fallback logic.
    
    Args:
        sections_dict: Dict containing all sections with their checks
    
    Returns:
        dict with total, critical, warnings, passed counts
    """
    all_checks = []
    
    # Collect checks from all sections (except summary itself)
    for section_key, section_data in sections_dict.items():
        if section_key == 'summary':  # Skip summary itself
            continue
        checks = section_data.get('checks', [])
        all_checks.extend(checks)
    
    total_checks = len(all_checks)
    passed_checks = sum(1 for check in all_checks if check.get('status') == 'pass')
    
    # Count failed checks by severity DIRECTLY from checks (canonical source)
    critical_count = sum(1 for check in all_checks if check.get('status') != 'pass' and normalize_severity(check.get('severity')) == 'critical')
    warning_count = sum(1 for check in all_checks if check.get('status') != 'pass' and normalize_severity(check.get('severity')) != 'critical')
    
    # Calculate total failed for verification
    failed_checks = total_checks - passed_checks
    
    # Safety: if counts don't add up, distribute remaining as warnings
    if critical_count + warning_count != failed_checks:
        warning_count = failed_checks - critical_count
    
    return {
        'total': total_checks,
        'critical': critical_count,
        'warnings': warning_count,
        'passed': passed_checks
    }


def calculate_canonical_issue_summary(on_page_checks, technical_checks, ai_checks, performance_checks, security_checks, accessibility_checks=None, issues=None):
    """
    DEPRECATED: Use generate_sections_summary() instead.
    Kept for backward compatibility during transition.
    
    Calculate canonical issue summary from visible section checks ONLY.
    
    Single source of truth: ALL counts derive from sections[].checks.
    No mixing of data sources. No fallback logic.
    
    Args:
        on_page_checks: On-page section checks
        technical_checks: Technical section checks
        ai_checks: AI section checks
        performance_checks: Performance section checks
        security_checks: Security section checks
        accessibility_checks: Accessibility section checks (optional - may arrive async)
        issues: DEPRECATED - kept for signature compat, not used
    
    Returns:
        dict with total, critical, warnings, passed counts
    """
    # Count all visible checks from ALL sections (including accessibility if available)
    all_checks = on_page_checks + technical_checks + ai_checks + performance_checks + security_checks
    if accessibility_checks:
        all_checks = all_checks + accessibility_checks
    total_checks = len(all_checks)
    
    # Count passed checks
    passed_checks = sum(1 for check in all_checks if check.get('status') == 'pass')
    
    # Count failed checks by severity DIRECTLY from checks (canonical source)
    critical_count = sum(1 for check in all_checks if check.get('status') != 'pass' and normalize_severity(check.get('severity')) == 'critical')
    warning_count = sum(1 for check in all_checks if check.get('status') != 'pass' and normalize_severity(check.get('severity')) != 'critical')
    
    # Calculate total failed for verification
    failed_checks = total_checks - passed_checks
    
    # Safety: if counts don't add up, distribute remaining as warnings
    if critical_count + warning_count != failed_checks:
        warning_count = failed_checks - critical_count
    
    return {
        'total': total_checks,
        'critical': critical_count,
        'warnings': warning_count,
        'passed': passed_checks
    }


def dedupe_issues(issues):
    """
    Deduplicate issues by rule_id and category.
    Keeps the issue with the highest severity for each unique (rule_id, category) pair.
    Severity priority: critical > medium > low
    """
    severity_priority = {
        'critical': 3,
        'medium': 2,
        'low': 1
    }
    
    issues_dict = {}
    
    for issue in issues:
        key = (issue.get("rule_id"), issue.get("category"))
        current_severity = normalize_severity(issue.get("severity"))
        current_priority = severity_priority.get(current_severity, 0)
        
        if key not in issues_dict:
            issues_dict[key] = issue
        else:
            existing_severity = normalize_severity(issues_dict[key].get("severity"))
            existing_priority = severity_priority.get(existing_severity, 0)
            
            # Keep the issue with higher severity
            if current_priority > existing_priority:
                issues_dict[key] = issue
    
    return list(issues_dict.values())


def map_audit_response(seo_data: dict, seo_result: dict, ai_result: dict, security_result: dict = None) -> dict:
    """
    Map audit results to API response format.
    
    Args:
        seo_data: Scraped page data
        seo_result: SEO analysis result
        ai_result: AI visibility result
        security_result: Security analysis result (optional)
    
    Returns:
        Formatted response dict with full rule visibility and performance insights
    """
    if 'error' in seo_data:
        return {
            'success': False,
            'error': seo_data.get('error'),
            'message': seo_data.get('message', 'Scraping failed')
        }
    
    # Extract rules details (all rules with pass/fail status)
    seo_rules = seo_result.get('rules_details', [])
    ai_rules = ai_result.get('rules_details', [])
    
    # Run security analysis if not provided
    if security_result is None:
        security_result = analyze_security(seo_data)
    
    security_rules = security_result.get('rules_details', [])
    all_rules = seo_rules + ai_rules + security_rules
    
    # Update severity values in rules_details to match new mappings
    for rule in all_rules:
        rule_id = rule.get('rule_id')
        
        # Update specific rule severities to match our new mappings
        if rule_id in ['llm_readability_poor', 'llms_txt_missing']:
            rule['severity'] = 'high'
        elif rule_id == 'hreflang_missing':
            rule['severity'] = 'high'
        elif rule_id == 'h1_missing':
            rule['severity'] = 'high'
        elif rule_id in ['noindex_tag_detected', 'noindex_header_detected']:
            rule['severity'] = 'high'
        elif rule_id == 'analytics_missing':
            rule['severity'] = 'medium'
        elif rule_id == 'https_redirect_missing':
            rule['severity'] = 'high'
        elif rule_id == 'robots_txt_missing':
            rule['severity'] = 'high'
        elif rule_id == 'sitemap_missing':
            rule['severity'] = 'high'
        elif rule_id in ['identity_schema_missing', 'structured_data_limited', 'faq_not_optimized', 'content_structure_poor']:
            rule['severity'] = 'high'
        else:
            # Apply normal severity normalization for other rules
            rule['severity'] = normalize_severity(rule.get('severity'))
    
    # Group rules by category for score calculation
    on_page_rules = [r for r in all_rules if r.get('category') == 'on_page']
    ai_rules_filtered = [r for r in all_rules if r.get('category') == 'ai']
    technical_rules = [r for r in all_rules if r.get('category') == 'technical']
    content_rules = [r for r in all_rules if r.get('category') == 'content']
    
    # Calculate scores from rules (same source used for sections)
    # On-Page score = strict check-based logic (11 checks only)
    # Check 1: Title Tag
    title_pass = seo_data.get('title_status') == "Good"
    
    # Check 2: Meta Description
    meta_pass = seo_data.get('meta_status') == "Good"
    
    # Check 3: Hreflang
    hreflang_pass = len(seo_data.get('hreflang_tags', [])) > 0
    
    # Check 4: Language
    lang_pass = bool(seo_data.get('html_lang'))
    
    # Check 5: H1 Tag
    h1_pass = seo_data.get('h1_count', 0) >= 1
    
    # Check 6: H2–H6 Tags
    h2_h6_count = (seo_data.get('h2_count', 0) + 
                   seo_data.get('h3_count', 0) + 
                   seo_data.get('h4_count', 0) + 
                   seo_data.get('h5_count', 0))
    h2_h6_pass = h2_h6_count > 0
    
    # Check 7: Content Length
    content_pass = seo_data.get('word_count', 0) >= 300
    
    # Check 8: Image Alt
    image_alt = seo_data.get('image_alt_percentage', {})
    alt_pass = image_alt.get('percentage', 0) >= 80
    
    # Check 9: Noindex Tag
    noindex_pass = seo_data.get('has_noindex', False) == False
    
    # Check 10: Noindex Header
    x_robots = seo_data.get('x_robots_tag', '')
    x_robots_pass = x_robots == "" or "noindex" not in x_robots.lower()
    
    # Check 11: Analytics
    analytics = seo_data.get('analytics', {})
    analytics_pass = analytics.get('detected', False) == True
    
    # Calculate On-Page score based on passed checks
    on_page_checks_list = [title_pass, meta_pass, hreflang_pass, lang_pass, h1_pass, 
                           h2_h6_pass, content_pass, alt_pass, noindex_pass,
                           x_robots_pass, analytics_pass]
    on_page_passed = sum(on_page_checks_list)
    on_page_score = round((on_page_passed / 11) * 100)
    
    # Technical score = strict check-based logic (6 checks only)
    # Check 1: Canonical
    canonical_pass = bool(seo_data.get('canonical_url'))
    
    # Check 2: SSL
    ssl_pass = seo_data.get('is_https', False) == True
    
    # Check 3: HTTPS Redirect
    https_redirect = seo_data.get('https_redirect', {})
    redirect_pass = https_redirect.get('redirects_to_https', False) == True
    
    # Check 4: Robots.txt
    robots_txt = seo_data.get('robots_txt', {})
    robots_pass = robots_txt.get('exists', False) == True
    
    # Check 5: XML Sitemap
    sitemap_xml = seo_data.get('sitemap_xml', {})
    sitemap_pass = sitemap_xml.get('exists', False) == True
    
    # Check 6: Schema
    schema_pass = seo_data.get('has_schema', False) == True
    
    # Calculate Technical score based on passed checks
    technical_checks_list = [canonical_pass, ssl_pass, redirect_pass, robots_pass, sitemap_pass, schema_pass]
    technical_passed = sum(technical_checks_list)
    technical_score = round((technical_passed / 6) * 100)
    
    # AI score = weighted check-based logic (6 checks)
    # Check 1: Identity Schema present (weight: 10)
    identity_schema = seo_data.get('identity_schema', {})
    identity_pass = identity_schema.get('present', False) == True
    
    # Check 2: LLM Readability score >= 70 AND not JS heavy (weight: 20)
    llm_readability = seo_data.get('llm_readability', {})
    readability_score = llm_readability.get('score', 0)
    is_js_heavy = llm_readability.get('is_js_heavy', False)
    readability_pass = readability_score >= 70 and not is_js_heavy
    
    # Check 3: llms.txt exists (weight: 25)
    llms_txt = seo_data.get('llms_txt', {})
    llms_pass = llms_txt.get('exists', False) == True
    
    # Check 4: Structured Data Depth - multiple schema types (weight: 15)
    schema_info = seo_data.get('schema_info', {})
    schema_types = schema_info.get('types_detected', [])
    structured_depth_pass = len(schema_types) >= 2
    
    # Check 5: FAQ Optimization - rich FAQ with good readability (weight: 15)
    has_faq_schema = 'FAQPage' in schema_types
    faq_pass = has_faq_schema and len(schema_types) >= 2 and readability_score >= 60
    
    # Check 6: Content Chunking - H1 + H2 structure (weight: 15)
    h1_count = seo_data.get('h1_count', 0)
    h2_count = seo_data.get('h2_count', 0)
    chunking_pass = h1_count >= 1 and h2_count >= 2
    
    # Calculate AI score using weighted scoring
    ai_weight_map = {
        "identity_pass": 10,
        "readability_pass": 20,
        "llms_pass": 25,
        "structured_depth_pass": 15,
        "faq_pass": 15,
        "chunking_pass": 15
    }
    ai_checks_map = {
        "identity_pass": identity_pass,
        "readability_pass": readability_pass,
        "llms_pass": llms_pass,
        "structured_depth_pass": structured_depth_pass,
        "faq_pass": faq_pass,
        "chunking_pass": chunking_pass
    }
    ai_score = sum(weight for check, weight in ai_weight_map.items() if ai_checks_map[check])
    ai_score = min(100, ai_score)
    
    # ========== CANONICAL PERFORMANCE SCORE (single calculation) ==========
    # This is the ONLY place performance_score is calculated.
    # It runs BEFORE overall_score so they are always consistent.
    # No duplicate calculations allowed below this point.
    performance_metrics = seo_data.get('performance_metrics', {})
    response_time_ms = seo_data.get('response_time_ms', 0)
    perf_status = performance_metrics.get('status', 'processing')

    # Check if we have fallback_score from PageSpeed failure
    fallback_score = performance_metrics.get('fallback_score', None)
    if fallback_score is not None:
        performance_score = fallback_score
        performance_category = 'fallback'
        performance_label = 'Fallback'
        performance_message = 'PageSpeed analysis failed - using fallback score.'
        performance_color = 'orange'
        response_time_ms = 0  # Set to 0 for fallback
        print(f"[MAPPER] Using fallback performance score: {performance_score}")
    elif perf_status == 'processing':
        # PageSpeed not yet available — use response_time_ms as provisional score
        if response_time_ms < 2000:
            performance_score = 75
            performance_category = 'fast'
            performance_label = 'Fast'
            performance_message = 'Your website loads quickly. Great for SEO and user experience.'
            performance_color = 'green'
        elif response_time_ms <= 4000:
            performance_score = 50
            performance_category = 'average'
            performance_label = 'Average'
            performance_message = 'Your website has average load speed. Optimization can improve rankings.'
            performance_color = 'yellow'
        elif response_time_ms <= 6000:
            performance_score = 35
            performance_category = 'needs_improvement'
            performance_label = 'Needs Improvement'
            performance_message = 'Your website load speed needs improvement. This can impact SEO and user experience.'
            performance_color = 'yellow'
        else:
            performance_score = 20
            performance_category = 'slow'
            performance_label = 'Slow'
            performance_message = 'Your website is slow. This can negatively impact SEO and user experience.'
            performance_color = 'red'
    else:
        # Use response_time_ms for normal calculation (completed/failed status)
        if response_time_ms < 2000:
            performance_score = 100
            performance_category = 'fast'
            performance_label = 'Fast'
            performance_message = 'Your website loads quickly. Great for SEO and user experience.'
            performance_color = 'green'
        elif response_time_ms <= 4000:
            performance_score = 75
            performance_category = 'average'
            performance_label = 'Average'
            performance_message = 'Your website has average load speed. Optimization can improve rankings.'
            performance_color = 'yellow'
        elif response_time_ms <= 6000:
            performance_score = 50
            performance_category = 'needs_improvement'
            performance_label = 'Needs Improvement'
            performance_message = 'Your website load speed needs improvement. This can impact SEO and user experience.'
            performance_color = 'yellow'
        else:
            performance_score = 25
            performance_category = 'slow'
            performance_label = 'Slow'
            performance_message = 'Your website is slow. This can negatively impact SEO and user experience.'
            performance_color = 'red'

    # Overall score calculation (no penalty, purely section-based)
    # Uses the SAME performance_score calculated above — always consistent
    overall_score = round((on_page_score * 0.4) + (technical_score * 0.2) + (ai_score * 0.2) + (performance_score * 0.2))
    
    # Score explanation will be generated after checks are defined (canonical source)
    
    # performance_score, performance_category, performance_label, performance_message,
    # performance_color, and response_time_ms are already set by the CANONICAL calculation above.
    # No second calculation permitted — single source of truth.
    
    performance_insight = {
        'load_time_ms': response_time_ms,
        'category': performance_category,
        'label': performance_label,
        'message': performance_message,
        'color': performance_color,
        'insights': []
    }
    
    # Set performance_explanation after performance_category is defined
    performance_explanation = f"Performance {performance_score}/100"
    if perf_status == 'error':
        performance_explanation += " (API failed - using fallback)"
    elif performance_category == 'slow':
        performance_explanation += " (slow load time)"
    elif performance_category == 'average':
        performance_explanation += " (average load time)"
    else:
        performance_explanation += " (fast load time)"
    
    # Severity priority for top_issues sorting
    severity_priority = {
        'critical': 3,
        'medium': 2,
        'low': 1
    }
    
    # Add analysis steps for frontend UX
    analysis_steps = [
        'Fetching website',
        'Measuring performance',
        'Analyzing SEO',
        'Running AI checks',
        'Calculating score'
    ]
    
    # Build section checks dynamically from page_info data
    
    # Build section checks dynamically from page_info data
    on_page_checks = build_on_page_checks(seo_data)
    technical_checks = build_technical_checks(seo_data)
    ai_checks = build_ai_checks(seo_data)
    performance_checks = build_performance_checks(seo_data)
    security_checks = build_security_checks(security_result)
    
    # Legacy issues generation removed - sections.checks is now canonical source
    
    # Build accessibility checks from accessibility_metrics (if available)
    accessibility_metrics = seo_data.get('accessibility_metrics', {})
    accessibility_checks = build_accessibility_checks(accessibility_metrics)
    
    # Calculate scores before building sections dict
    security_score = security_result.get('score', 0)
    accessibility_score = accessibility_metrics.get('score', 0) if accessibility_metrics.get('status') != 'processing' else 0
    
    # Build sections dict with all checks
    sections = {
        'on_page': {
            'score': on_page_score,
            'grade': get_grade(on_page_score),
            'checks': on_page_checks
        },
        'technical': {
            'score': technical_score,
            'grade': get_grade(technical_score),
            'checks': technical_checks
        },
        'ai': {
            'score': ai_score,
            'grade': get_grade(ai_score),
            'checks': ai_checks,
            'hidden_issues': sum(1 for c in ai_checks if c['status'] == 'fail') * 5 + 8
        },
        'performance': {
            'score': performance_score,
            'grade': get_grade(performance_score),
            'checks': performance_checks
        },
        'security': {
            'score': security_score,
            'grade': get_grade(security_score),
            'checks': security_checks
        },
        'accessibility': {
            'score': accessibility_score,
            'grade': get_grade(accessibility_score) if accessibility_score > 0 else None,
            'checks': accessibility_checks,
            'status': accessibility_metrics.get('status', 'processing')
        }
    }
    
    # Generate score explanation based on section checks (canonical source)
    seo_critical = sum(1 for c in on_page_checks if c.get('status') != 'pass' and normalize_severity(c.get('severity')) == 'critical')
    ai_critical = sum(1 for c in ai_checks if c.get('status') != 'pass' and normalize_severity(c.get('severity')) == 'critical')
    
    seo_explanation = f"SEO score {on_page_score}/100"
    if seo_critical > 0:
        seo_explanation += f" ({seo_critical} critical issues)"
    elif on_page_score >= 90:
        seo_explanation += " (excellent)"
    elif on_page_score >= 70:
        seo_explanation += " (good)"
    
    ai_explanation = f"AI score {ai_score}/100"
    if ai_critical > 0:
        ai_explanation += f" ({ai_critical} critical issues)"
    elif ai_score >= 80:
        ai_explanation += " (good)"
    elif ai_score >= 60:
        ai_explanation += " (average)"
    
    technical_explanation = f"Technical {technical_score}/100"
    technical_critical = sum(1 for c in technical_checks if c.get('status') != 'pass' and normalize_severity(c.get('severity')) == 'critical')
    if technical_critical > 0:
        technical_explanation += f" ({technical_critical} critical issues)"
    elif technical_score >= 90:
        technical_explanation += " (excellent)"
    elif technical_score >= 70:
        technical_explanation += " (good)"
    
    # performance_explanation is already defined above in performance calculation section
    
    score_explanation = {
        'seo': seo_explanation,
        'technical': technical_explanation,
        'ai': ai_explanation,
        'performance': performance_explanation
    }
    
    # Generate canonical summary from ALL visible section checks
    sections['summary'] = generate_sections_summary(sections)
    
    # Generate top_issues from failed section checks (canonical source)
    all_failed_checks = []
    for section_name, section_data in sections.items():
        if section_name == 'summary':
            continue
        checks = section_data.get('checks', [])
        failed_checks = [c for c in checks if c.get('status') != 'pass']
        all_failed_checks.extend(failed_checks)
    
    # Sort failed checks by severity
    sorted_checks = sorted(all_failed_checks, key=lambda x: severity_priority.get(normalize_severity(x.get('severity')), 0), reverse=True)
    
    # Add priority to top issues (limit to top 10)
    top_issues = []
    for idx, check in enumerate(sorted_checks[:10], start=1):
        top_issues.append({
            'priority': idx,
            'rule_id': check.get('rule_id'),
            'severity': normalize_severity(check.get('severity')),
            'message': check.get('message'),
            'category': check.get('category')
        })
    
    # Update combined summary based on visible checks only (canonical source)
    total_on_page = len(on_page_checks)
    passed_on_page = sum(1 for check in on_page_checks if check.get('status') == 'pass')
    failed_on_page = total_on_page - passed_on_page
    
    total_technical = len(technical_checks)
    passed_technical = sum(1 for check in technical_checks if check.get('status') == 'pass')
    failed_technical = total_technical - passed_technical
    
    total_ai = len(ai_checks)
    passed_ai = sum(1 for check in ai_checks if check.get('status') == 'pass')
    failed_ai = total_ai - passed_ai
    
    total_performance = len(performance_checks)
    passed_performance = sum(1 for check in performance_checks if check.get('status') == 'pass')
    failed_performance = total_performance - passed_performance
    
    total_security = len(security_checks)
    passed_security = sum(1 for check in security_checks if check.get('status') == 'pass')
    failed_security = total_security - passed_security
    
    # Accessibility section counts (may be 0 if still processing)
    total_accessibility = len(accessibility_checks)
    passed_accessibility = sum(1 for check in accessibility_checks if check.get('status') == 'pass')
    failed_accessibility = total_accessibility - passed_accessibility
    
    # Security and accessibility scores already calculated above
    
    # Canonical combined summary from ALL visible section checks (including accessibility)
    combined_summary = {
        'total': total_on_page + total_technical + total_ai + total_performance + total_security + total_accessibility,
        'passed': passed_on_page + passed_technical + passed_ai + passed_performance + passed_security + passed_accessibility,
        'failed': failed_on_page + failed_technical + failed_ai + failed_performance + failed_security + failed_accessibility
    }
    
    # FIXED: Calculate confidence score based on visible section checks only (canonical)
    total_visible_checks = combined_summary.get('total', 0)
    passed_visible_checks = combined_summary.get('passed', 0)
    confidence_score = int((passed_visible_checks / total_visible_checks * 100)) if total_visible_checks > 0 else 0
    
    # Adjust confidence score for errors
    if seo_data.get('error'):
        confidence_score = max(0, confidence_score - 5)
    if not seo_data.get('title') or not seo_data.get('meta_description'):
        confidence_score = max(0, confidence_score - 5)
    
    # Calculate hidden_issues (visible_checks - visible_issues_count)
    visible_issues_count = len(all_failed_checks)
    hidden_issues = max(0, total_visible_checks - visible_issues_count)
    
    # Note: total_pages is now calculated in Node.js based on word_count
    # Python returns None, Node.js will populate it
    return {
        'success': True,
        'data': {
            'url': seo_data.get('url'),
            'score': overall_score,
            'grade': get_grade(overall_score),
            'on_page_score': on_page_score,
            'technical_score': technical_score,
            'ai_score': ai_score,
            'performance_score': performance_score,  # Backward compat — canonical source is `performance.score`
            'security_score': security_score,  # NEW: Security score from security analysis
            # ========== CANONICAL PERFORMANCE OBJECT (single source of truth) ==========
            # All frontend components MUST read from `performance.score`.
            # No other score path is authoritative.
            'performance': {
                'status': perf_status,
                'score': performance_score if perf_status != 'processing' else None,
                'mobile': performance_metrics.get('mobile') if perf_status != 'processing' else None,
                'desktop': performance_metrics.get('desktop') if perf_status != 'processing' else None,
                'grade': get_grade(performance_score) if perf_status != 'processing' else None,
                'checks': performance_checks,
                'insights': performance_insight,
            },
            'sections': sections,
            'score_explanation': score_explanation,
            'top_issues': top_issues,  # Derived from sections.checks[]
            'rules_summary': combined_summary,  # Backward compatibility - total/passed/failed counts
            'rules_details': all_rules,  # Enterprise-only - all rules with pass/fail status
            'analysis_steps': analysis_steps,  # New: frontend UX steps
            'confidence_score': confidence_score,  # New: confidence score
            'hidden_issues': hidden_issues,  # Fixed: calculated value
            'page_info': {
                'title': seo_data.get('title', ''),
                'title_length': seo_data.get('title_length', 0),
                'title_status': seo_data.get('title_status', ''),
                'meta_description': seo_data.get('meta_description', ''),
                'meta_length': seo_data.get('meta_length', 0),
                'meta_status': seo_data.get('meta_status', ''),
                'word_count': seo_data.get('word_count', 0),
                'has_schema': seo_data.get('has_schema', False),
                'schema_info': seo_data.get('schema_info', {}),
                'is_https': seo_data.get('is_https', False),
                'response_time_ms': response_time_ms,
                'final_url': seo_data.get('final_url', ''),
                'h1_count': seo_data.get('h1_count', 0),
                'h2_count': seo_data.get('h2_count', 0),
                'h3_count': seo_data.get('h3_count', 0),
                'h4_count': seo_data.get('h4_count', 0),
                'h5_count': seo_data.get('h5_count', 0),
                'h6_count': seo_data.get('h6_count', 0),
                'has_robots_meta': seo_data.get('has_robots_meta', False),
                'robots_meta_content': seo_data.get('robots_meta_content', ''),
                'has_viewport': seo_data.get('has_viewport', False),  # FIXED: Viewport detection from HTML (most reliable)
                'html_lang': seo_data.get('html_lang', ''),
                'hreflang_tags': seo_data.get('hreflang_tags', []),
                'image_alt_percentage': seo_data.get('image_alt_percentage', {}),
                'x_robots_tag': seo_data.get('x_robots_tag', ''),
                'has_noindex': seo_data.get('has_noindex', False),
                'canonical_url': seo_data.get('canonical_url', ''),
                'sitemap_xml': seo_data.get('sitemap_xml', {}),
                'https_redirect': seo_data.get('https_redirect', {}),
                'analytics': seo_data.get('analytics', {}),
                'identity_schema': seo_data.get('identity_schema', {}),
                'llm_readability': seo_data.get('llm_readability', {}),
                'llms_txt': seo_data.get('llms_txt', {}),
                'social_signals': seo_data.get('social_signals', {}),  # ADDED: Social media signal detection
                'local_seo': seo_data.get('local_seo', {}),  # ADDED: Local SEO signal detection
                'performance_metrics': seo_data.get('performance_metrics', {}),  # Legacy path — prefer `data.performance` for canonical access
                'accessibility_metrics': seo_data.get('accessibility_metrics', {})  # ADDED: Accessibility metrics for accessibility section
            },
            'analysis_depth': 'homepage_only',
            'total_pages': None,  # Will be populated by Node.js
            'locked': True
        }
    }
