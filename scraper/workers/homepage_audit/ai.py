"""AI Visibility Analysis - Lightweight rule execution for homepage audit"""

from typing import List, Dict
import re


def analyze_ai_visibility(seo_data: dict) -> dict:
    """
    Run AI visibility analysis on scraped data.
    Implements top 8 high-impact AI rules (no fake info-level items).
    No database access, uses only scraped data.
    
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
    text_content = seo_data.get('text_content', '').lower()
    
    # Helper function to add rule result
    def add_rule(rule_id, status, severity, message, category):
        rules.append({
            'rule_id': rule_id,
            'status': status,
            'severity': severity,
            'message': message,
            'category': category
        })
    
    # Rule 1: Schema.org structured data (CRITICAL)
    if not seo_data.get('has_schema'):
        add_rule('schema_missing', 'fail', 'critical', 'Schema.org structured data is missing', 'technical')
        issues.append({
            'rule_id': 'schema_missing',
            'severity': 'critical',
            'message': 'Schema.org structured data is missing',
            'category': 'technical'
        })
    else:
        add_rule('schema_present', 'pass', 'low', 'Schema.org structured data is present', 'technical')
    
    # Rule 2: Content structure (CRITICAL)
    if not seo_data.get('h1'):
        add_rule('content_structure_poor', 'fail', 'critical', 'Poor content structure (missing H1)', 'content')
        issues.append({
            'rule_id': 'content_structure_poor',
            'severity': 'critical',
            'message': 'Poor content structure (missing H1)',
            'category': 'content'
        })
    elif seo_data.get('h2_count', 0) < 2:
        add_rule('content_structure_basic', 'fail', 'medium', 'Basic content structure (few H2 tags)', 'content')
        issues.append({
            'rule_id': 'content_structure_basic',
            'severity': 'medium',
            'message': 'Basic content structure (few H2 tags)',
            'category': 'content'
        })
    else:
        add_rule('content_structure_good', 'pass', 'low', f'Good content structure (H1 + {seo_data.get("h2_count", 0)} H2 tags)', 'content')
    
    # Rule 3: Author information (HIGH)
    author_indicators = ['author', 'by ', 'written by', 'published by']
    has_author = any(indicator in text_content for indicator in author_indicators)
    
    if not has_author:
        add_rule('author_info_missing', 'fail', 'critical', 'Author information not clearly visible', 'content')
        issues.append({
            'rule_id': 'author_info_missing',
            'severity': 'critical',
            'message': 'Author information not clearly visible',
            'category': 'content'
        })
    else:
        add_rule('author_info_present', 'pass', 'low', 'Author information is visible', 'content')
    
    # Rule 4: Citation format (MEDIUM)
    # FIXED: Now checks for outbound links (different domain) and keywords deterministically
    citation_patterns = [r'\[\d+\]', r'\(\d{4}\)', r'source:', r'reference:', r'references:', r'sources:']
    has_citations = any(re.search(pattern, text_content) for pattern in citation_patterns)
    
    # Check for keywords
    citation_keywords = ['source', 'reference', 'study', 'research', 'according to', 'cited']
    has_keywords = any(keyword in text_content for keyword in citation_keywords)
    
    # Check for outbound links (different domain)
    from urllib.parse import urlparse
    base_url = seo_data.get('url', '')
    base_domain = urlparse(base_url).netloc if base_url else ''
    
    # Check for external URLs in text
    url_patterns = [r'https?://[^\s]+', r'www\.[^\s]+']
    urls = re.findall(r'https?://[^\s]+', text_content) + re.findall(r'www\.[^\s]+', text_content)
    
    has_outbound_links = False
    for url in urls:
        url_domain = urlparse(url).netloc if url.startswith('http') else urlparse('http://' + url).netloc
        if url_domain and url_domain != base_domain:
            has_outbound_links = True
            break
    
    # Pass if any of the three conditions are met
    if not has_citations and not has_keywords and not has_outbound_links:
        add_rule('citations_missing', 'fail', 'medium', 'Citations or references not found', 'content')
        issues.append({
            'rule_id': 'citations_missing',
            'severity': 'medium',
            'message': 'Citations or references not found',
            'category': 'content'
        })
    else:
        add_rule('citations_present', 'pass', 'info', 'Citations or references found', 'content')
    
    # Rule 5: Direct answers (HIGH)
    # FIXED: Now detects Q&A patterns where "?" is followed by answer text
    # Pattern: question ending with "?" followed by substantial answer text
    sentences = text_content.split('.')
    has_direct_answers = False
    
    for i, sentence in enumerate(sentences):
        # Check if sentence ends with question mark
        if '?' in sentence:
            # Check if next sentence has substantial content (answer)
            if i + 1 < len(sentences):
                next_sentence = sentences[i + 1].strip()
                # Answer should have at least 15 words to be considered substantial
                word_count = len(next_sentence.split())
                if word_count >= 15:
                    has_direct_answers = True
                    break
    
    # Also check for common Q&A phrases
    qa_patterns = [r'what is', r'how to', r'why is', r'when to', r'where is', r'how do', r'why do', r'what are']
    has_qa_phrases = any(re.search(pattern, text_content) for pattern in qa_patterns)
    
    if not has_direct_answers and not has_qa_phrases:
        add_rule('direct_answers_missing', 'fail', 'medium', 'Direct Q&A format not detected', 'content')
        issues.append({
            'rule_id': 'direct_answers_missing',
            'severity': 'medium',
            'message': 'Direct Q&A format not detected',
            'category': 'content'
        })
    else:
        add_rule('direct_answers_present', 'pass', 'low', 'Direct Q&A format detected', 'content')
    
    # Rule 6: Content length for AI (MEDIUM)
    word_count = seo_data.get('word_count', 0)
    if word_count < 300:
        add_rule('content_too_short_for_ai', 'fail', 'medium', f'Content too short for AI analysis ({word_count} words, recommended: 300+)', 'content')
        issues.append({
            'rule_id': 'content_too_short_for_ai',
            'severity': 'medium',
            'message': f'Content too short for AI analysis ({word_count} words, recommended: 300+)',
            'category': 'content'
        })
    else:
        add_rule('content_optimal_for_ai', 'pass', 'low', f'Content length optimal for AI ({word_count} words)', 'content')
    
    # Rule 7: FAQ schema (MEDIUM)
    # FIXED: Only fail if FAQ content exists AND FAQ schema not present
    # Check if FAQ content exists
    faq_indicators = ['faq', 'frequently asked questions', 'questions?', 'common questions']
    has_faq_content = any(indicator in text_content for indicator in faq_indicators)
    
    # Check if FAQ schema exists in schema_info
    schema_info = seo_data.get('schema_info', {})
    schema_types = schema_info.get('types_detected', [])
    # FIXED: Check if FAQPage is in schema types
    has_faq_schema = any('faq' in str(t).lower() or 'FAQPage' in str(t) for t in schema_types)
    
    if has_faq_schema:
        # FIXED: If FAQPage detected, mark as present
        add_rule('faq_schema_present', 'pass', 'low', 'FAQ schema is present', 'ai')
    elif has_faq_content and not has_faq_schema:
        add_rule('faq_schema_missing', 'fail', 'medium', 'FAQ content exists but FAQ schema is missing', 'ai')
        issues.append({
            'rule_id': 'faq_schema_missing',
            'severity': 'medium',
            'message': 'FAQ content exists but FAQ schema is missing',
            'category': 'ai'
        })
    else:
        add_rule('faq_not_applicable', 'pass', 'low', 'No FAQ content detected', 'ai')
    
    # Rule 8: HTTPS for AI trust (CRITICAL)
    if not seo_data.get('is_https'):
        add_rule('https_missing_ai', 'fail', 'critical', 'Website not using HTTPS (affects AI trust)', 'ai')
        issues.append({
            'rule_id': 'https_missing_ai',
            'severity': 'critical',
            'message': 'Website not using HTTPS (affects AI trust)',
            'category': 'ai'
        })
    else:
        add_rule('https_enabled_ai', 'pass', 'low', 'Website using HTTPS (AI trust)', 'ai')
    
    # Rule 9: Identity schema (MEDIUM)
    # ADDED: Identity schema detection for AI SEO
    identity_schema = seo_data.get('identity_schema', {})
    if identity_schema.get('present', False):
        types = identity_schema.get('types', [])
        source = identity_schema.get('source', 'schema')
        
        # FIXED: Handle empty types in fallback case to avoid empty brackets
        if types:
            types_str = ', '.join(types[:2])  # Show first 2 types
            add_rule('identity_schema_present', 'pass', 'low', f'Identity schema detected ({types_str})', 'ai')
        elif source == 'fallback_meta':
            name = identity_schema.get('name', '')
            add_rule('identity_schema_present', 'pass', 'low', f'Identity detected via meta tags ({name})', 'ai')
        elif source == 'fallback_footer':
            name = identity_schema.get('name', '')
            add_rule('identity_schema_present', 'pass', 'low', f'Identity detected via footer ({name})', 'ai')
        else:
            add_rule('identity_schema_present', 'pass', 'low', f'Identity schema detected', 'ai')
    else:
        add_rule('identity_schema_missing', 'fail', 'medium', 'No identity schema found (recommended for brand authority)', 'ai')
        issues.append({
            'rule_id': 'identity_schema_missing',
            'severity': 'medium',
            'message': 'No identity schema found (recommended for brand authority)',
            'category': 'ai'
        })
    
    # Rule 10: LLM readability (MEDIUM)
    # ADDED: LLM readability scoring for AI SEO
    llm_readability = seo_data.get('llm_readability', {})
    llm_score = llm_readability.get('score', 0)
    if llm_score >= 70:
        add_rule('llm_readability_good', 'pass', 'low', f'Content is well-structured for LLMs (score: {llm_score}/100)', 'ai')
    else:
        add_rule('llm_readability_poor', 'fail', 'medium', f'Content structure could be improved for LLMs (score: {llm_score}/100)', 'ai')
        issues.append({
            'rule_id': 'llm_readability_poor',
            'severity': 'medium',
            'message': f'Content structure could be improved for LLMs (score: {llm_score}/100)',
            'category': 'ai'
        })
    
    # Rule 11: llms.txt (LOW)
    # ADDED: llms.txt detection for AI SEO
    llms_txt = seo_data.get('llms_txt', {})
    if llms_txt.get('exists', False):
        add_rule('llms_txt_present', 'pass', 'low', 'llms.txt is present (AI discoverability)', 'ai')
    else:
        add_rule('llms_txt_missing', 'fail', 'low', 'llms.txt not found (recommended for AI discoverability)', 'ai')
        issues.append({
            'rule_id': 'llms_txt_missing',
            'severity': 'low',
            'message': 'llms.txt not found (recommended for AI discoverability)',
            'category': 'ai'
        })
    
    # Calculate score using strict check-based logic
    score = calculate_ai_score(seo_data)
    
    # Build rules summary
    total = len(rules)
    passed = sum(1 for r in rules if r['status'] == 'pass')
    failed = sum(1 for r in rules if r['status'] == 'fail')
    
    # Limit to top 8 issues for free tier
    issues = issues[:8]
    
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


def calculate_ai_score(seo_data: dict) -> int:
    """
    Calculate AI visibility score based on 3 strict checks.
    
    Checks:
    1. Identity Schema present
    2. LLM Readability score >= 70
    3. llms.txt exists
    
    Formula: (passed / 3) * 100
    
    Args:
        seo_data: Scraped page data with page_info
    
    Returns:
        Score from 0 to 100
    """
    # Check 1: Identity Schema
    identity_schema = seo_data.get('identity_schema', {})
    identity_pass = identity_schema.get('present', False) == True
    
    # Check 2: LLM Readability
    llm_readability = seo_data.get('llm_readability', {})
    readability_pass = llm_readability.get('score', 0) >= 70
    
    # Check 3: llms.txt
    llms_txt = seo_data.get('llms_txt', {})
    llms_pass = llms_txt.get('exists', False) == True
    
    # Calculate score based on passed checks
    checks = [identity_pass, readability_pass, llms_pass]
    passed = sum(checks)
    total = 3
    
    return round((passed / total) * 100)
