"""
Internal Pages SEO Rules
Rules for internal page quality, error handling, and URL structure.
"""

from ..base_seo_rule import BaseSEORuleV2


class BrokenLinksRule(BaseSEORuleV2):
    rule_id = "broken_links"
    rule_no = 1
    category = "Internal Pages"
    severity = "high"
    description = "Broken links waste crawl budget, hurt UX, and leak link equity"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        broken_count = normalized.get("broken_links_count", 0)
        
        if broken_count > 0:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Found {broken_count} broken link(s) that return 4XX/5XX errors",
                f"Broken links count: {broken_count}",
                "0 broken links",
                data_key="broken_links_count",
                data_path="broken_links_count",
                impact="Broken links waste crawl budget, reduce user trust, and leak PageRank. Search engines may lower rankings for pages with poor user experience.",
                recommendation="Fix or redirect broken URLs using 301 redirects to relevant pages. Implement regular link monitoring to catch future issues quickly."
            ))
        
        return issues


class Custom404PageRule(BaseSEORuleV2):
    rule_id = "custom_404_page"
    rule_no = 2
    category = "Internal Pages"
    severity = "medium"
    description = "A missing 404 page confuses users and provides no recovery path"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        is_404_page = normalized.get("is_404_page", False)
        custom_404_detected = normalized.get("custom_404_detected", False)
        has_navigation = normalized.get("has_navigation", False)
        has_home_link = normalized.get("has_home_link", False)
        
        # Enhanced validation for 404 pages - check multiple data sources
        navigation_detected = has_navigation
        if not navigation_detected:
            # Check nested sources
            navigation_detected = (
                normalized.get("navigation_detection", {}).get("has_navigation", False) or
                normalized.get("body_signals", {}).get("structure", {}).get("has_nav", False) or
                normalized.get("enhanced_extraction_v2", {}).get("page_metadata", {}).get("structure_metrics", {}).get("has_navigation", False)
            )
        
        home_link_detected = has_home_link
        if not home_link_detected:
            # Check for home link in content
            content = normalized.get("content", "")
            if content and any(indicator in content.lower() for indicator in ["home", "homepage", "back to home"]):
                home_link_detected = True
        
        # Only check if this is actually a 404 page
        if is_404_page and not custom_404_detected and not navigation_detected and not home_link_detected:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "404 page lacks custom elements (navigation, search, helpful content)",
                f"No helpful navigation or custom elements found on 404 page - Navigation: {navigation_detected}, Home Link: {home_link_detected}",
                "Custom 404 page with helpful elements",
                data_key="custom_404_detected",
                data_path="custom_404_detected",
                impact="Poor 404 pages create dead-end user experiences, increasing bounce rates and signaling low-quality site navigation to search engines.",
                recommendation="Add navigation menu, homepage link, search bar, and helpful content suggestions to your 404 page. Guide users back to relevant content instead of showing generic error messages."
            ))
        
        return issues


class FourXXErrorPagesRule(BaseSEORuleV2):
    rule_id = "4xx_error_pages"
    rule_no = 3
    category = "Internal Pages"
    severity = "medium"
    description = "Client-side errors (403, 410, 429) signal crawlability issues"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        status_code = normalized.get("status_code")
        if status_code and status_code >= 400 and status_code < 500 and status_code != 404:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Page returns 4XX error: {status_code}",
                f"HTTP status {status_code}",
                "200 OK status",
                data_key="status_code",
                data_path="status_code"
            ))
        
        return issues


class FiveXXServerErrorRule(BaseSEORuleV2):
    rule_id = "5xx_server_error"
    rule_no = 4
    category = "Internal Pages"
    severity = "high"
    description = "Server errors cause ranking drops and crawl budget waste"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        status_code = normalized.get("status_code")
        if status_code and status_code >= 500:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Page returns 5XX server error: {status_code}",
                f"HTTP status {status_code}",
                "200 OK status",
                data_key="status_code",
                data_path="status_code"
            ))
        
        return issues


class TimeoutErrorRule(BaseSEORuleV2):
    rule_id = "timeout_errors"
    rule_no = 5
    category = "Internal Pages"
    severity = "medium"
    description = "Timeout errors signal server instability — bots abandon timed-out pages"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for timeout indicators in the response
        error_info = normalized.get("error_info", {})
        if error_info.get("timeout") or error_info.get("status") == "timeout":
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Page request timed out",
                "Request timeout detected",
                "Page loads successfully without timeout",
                data_key="error_info",
                data_path="error_info.timeout"
            ))
        
        return issues


class MixedHttpHttpsRule(BaseSEORuleV2):
    rule_id = "mixed_http_https"
    rule_no = 6
    category = "Internal Pages"
    severity = "high"
    description = "Mixed content triggers security warnings and indexing inconsistencies"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        mixed_detected = normalized.get("mixed_content_detected", False)
        mixed_count = normalized.get("mixed_content_count", 0)
        
        if mixed_detected:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Found {mixed_count} HTTP resource(s) on HTTPS page - creates security vulnerabilities",
                f"Mixed content count: {mixed_count}",
                "All resources use HTTPS",
                data_key="mixed_content_detected",
                data_path="mixed_content_detected",
                impact="Mixed content triggers browser security warnings, breaks HTTPS trust signals, and can cause search engines to downgrade rankings due to security concerns.",
                recommendation="Convert all HTTP resources to HTTPS. Update image URLs, script sources, stylesheet links, and any third-party resources to use secure HTTPS protocols."
            ))
        
        return issues


class NonSeoFriendlyUrlsRule(BaseSEORuleV2):
    rule_id = "non_seo_friendly_urls"
    rule_no = 7
    category = "Internal Pages"
    severity = "medium"
    description = "URLs with parameters create duplicate content and confuse crawlers"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        has_params = normalized.get("has_parameters", False)
        param_count = normalized.get("parameter_count", 0)
        
        if has_params:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"URL contains {param_count} parameter(s) which may dilute SEO value",
                f"Parameters count: {param_count}",
                "Clean URL structure without parameters",
                data_key="has_parameters",
                data_path="has_parameters",
                impact="URL parameters can create duplicate content issues, dilute link equity, and make URLs less shareable and memorable for users.",
                recommendation="Use clean, static URLs whenever possible. If parameters are necessary, implement canonical tags and use URL rewriting to create user-friendly paths."
            ))
        
        return issues


class DoubleSlashUrlsRule(BaseSEORuleV2):
    rule_id = "double_slash_urls"
    rule_no = 8
    category = "Internal Pages"
    severity = "low"
    description = "Double slashes create duplicate URL variants and confuse canonical resolution"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        has_double_slash = normalized.get("has_double_slash", False)
        
        if has_double_slash:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "URL contains double slashes which can cause crawling and indexing issues",
                "Double slash detected in URL structure",
                "Clean URL structure without double slashes",
                data_key="has_double_slash",
                data_path="has_double_slash",
                impact="Double slashes create duplicate URL variants, confuse search engine crawlers, and can split PageRank across multiple URL versions.",
                recommendation="Fix double slash issues in URL structure. Ensure proper URL normalization in internal links, redirects, and canonical tag implementation."
            ))
        
        return issues


class LowCodeToHtmlRatioRule(BaseSEORuleV2):
    rule_id = "low_code_to_html_ratio"
    rule_no = 9
    category = "Internal Pages"
    severity = "low"
    description = "Heavy code with thin visible content weakens topical signals"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        ratio = normalized.get("code_to_html_ratio", 0)
        
        # Check multiple data sources for ratio
        if ratio == 0:
            # Try nested sources
            ratio = (
                normalized.get("page_metrics", {}).get("code_to_html_ratio", 0) or
                normalized.get("body_signals", {}).get("content_metrics", {}).get("code_to_html_ratio", 0) or
                normalized.get("enhanced_extraction_v2", {}).get("page_metadata", {}).get("content_metrics", {}).get("code_to_html_ratio", 0)
            )
        
        # Convert to float if needed
        try:
            ratio = float(ratio)
        except (ValueError, TypeError):
            ratio = 0
        
        if ratio < 5:
            # FAIL with Medium severity
            self.severity = "medium"
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Very low content ratio ({ratio:.1f}%). Page has too much code vs visible content.",
                f"Ratio: {ratio:.1f}% (threshold: 5%)",
                "Content-to-code ratio ≥ 5%",
                data_key="code_to_html_ratio",
                data_path="code_to_html_ratio",
                impact="Very low content ratio indicates thin content with excessive code, weakening topical signals and potentially triggering quality penalties from search engines.",
                recommendation="Increase meaningful content and reduce unnecessary scripts. Move JavaScript/CSS to external files, remove unused code, and add valuable text content."
            ))
        elif ratio < 10:
            # WARNING with Low severity
            self.severity = "low"
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Low content ratio ({ratio:.1f}%). Could benefit from more content or cleaner code.",
                f"Ratio: {ratio:.1f}% (threshold: 10%)",
                "Content-to-code ratio ≥ 10%",
                data_key="code_to_html_ratio",
                data_path="code_to_html_ratio",
                impact="Low content ratio suggests insufficient text content relative to page size, potentially limiting SEO keyword relevance and user value.",
                recommendation="Add more substantial content or optimize code structure. Consider expanding content sections and removing redundant scripts or inline styles."
            ))
        
        return issues


class LongUrlsRule(BaseSEORuleV2):
    rule_id = "long_urls"
    rule_no = 12
    category = "Internal Pages"
    severity = "low"
    description = "Long URLs hurt readability, CTR, and shareability"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        long_urls_count = normalized.get("long_urls_count", 0)
        
        if long_urls_count > 0:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Found {long_urls_count} URL(s) longer than 115 characters",
                f"Long URLs count: {long_urls_count}",
                "No URLs longer than 115 characters",
                data_key="long_urls_count",
                data_path="long_urls_count",
                impact="Long URLs hurt readability, reduce click-through rates in search results, and are difficult for users to share and remember.",
                recommendation="Shorten long URLs while maintaining descriptive keywords. Use URL shortening for tracking parameters when needed and focus on concise, meaningful paths."
            ))
        
        return issues


class NavigationVisibilityRule(BaseSEORuleV2):
    rule_id = "navigation_visibility"
    rule_no = 15
    category = "Internal Pages"
    severity = "medium"
    description = "Navigation visible to users but undetectable by crawlers weakens internal linking"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check multiple sources for navigation detection
        has_navigation = normalized.get("has_navigation", False)
        
        # Enhanced validation - check nested sources including new detection data
        navigation_sources = [
            has_navigation,
            normalized.get("navigation_detection", {}).get("has_navigation", False),
            normalized.get("body_signals", {}).get("structure", {}).get("has_nav", False),
            normalized.get("enhanced_extraction_v2", {}).get("page_metadata", {}).get("structure_metrics", {}).get("has_navigation", False),
            normalized.get("technical_signals", {}).get("navigation_detection", {}).get("has_navigation", False)
        ]
        
        # If any source detects navigation, check if it's properly structured for crawlers
        if any(navigation_sources):
            # Get detailed detection info if available
            nav_detection = normalized.get("navigation_detection", {})
            detection_method = nav_detection.get("detection_method", "unknown")
            nav_elements = nav_detection.get("nav_elements_found", [])
            link_count = nav_detection.get("link_count", 0)
            
            # Check if navigation is properly structured (semantic HTML)
            raw_html = normalized.get("raw_html", "")
            has_semantic_nav = False
            
            if raw_html:
                raw_lower = raw_html.lower()
                semantic_indicators = [
                    '<nav',
                    'role="navigation"',
                    'aria-label',
                    'nav ',
                    'navigation',
                    'menu'
                ]
                has_semantic_nav = any(indicator in raw_lower for indicator in semantic_indicators)
            
            # Check for accessible navigation structure
            has_accessible_nav = False
            if raw_html:
                raw_lower = raw_html.lower()
                accessible_indicators = [
                    '<ul',
                    '<li',
                    'a href',
                    '<a ',
                    'href='
                ]
                has_accessible_nav = any(indicator in raw_lower for indicator in accessible_indicators)
            
            # If navigation exists but lacks proper structure, flag it
            if not has_semantic_nav or not has_accessible_nav:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Navigation detected but may not be properly visible to crawlers",
                    f"Detection method: {detection_method} | Links: {link_count} | Semantic: {has_semantic_nav}, Accessible: {has_accessible_nav}",
                    "Semantic navigation structure with proper HTML elements",
                    data_key="has_navigation",
                    data_path="has_navigation",
                    impact="Navigation that's not semantically structured may be missed by crawlers, reducing internal linking strength and page authority distribution.",
                    recommendation="Use semantic HTML5 navigation elements (<nav>) with proper ARIA labels and structured lists. Ensure navigation links are accessible and properly formatted for crawler detection."
                ))
        else:
            # No navigation detected at all - perform enhanced check
            # Use raw_html because we are looking for actual HTML tags (e.g., <nav, <ul>, class=...)
            raw_html = normalized.get("raw_html", "")
            
            # Fallback: Try to detect navigation directly from raw HTML
            if raw_html:
                raw_lower = raw_html.lower()
                
                # Check for semantic HTML indicators
                has_nav_tag = '<nav' in raw_lower
                has_role_nav = 'role="navigation"' in raw_lower
                has_aria_nav = 'aria-label' in raw_lower and ('menu' in raw_lower or 'navigation' in raw_lower)
                
                # Check for structural patterns
                has_ul_structure = '<ul>' in raw_lower and '<li>' in raw_lower and '<a href=' in raw_lower
                
                # Check for class-based indicators
                nav_class_patterns = ['class="nav', 'class="menu', 'class="navigation', "class='nav", "class='menu", "class='navigation"]
                has_nav_class = any(pattern in raw_lower for pattern in nav_class_patterns)
                
                # If any fallback detection succeeds, skip the issue
                if any([has_nav_tag, has_role_nav, has_aria_nav, (has_ul_structure and has_nav_class)]):
                    # Navigation exists but wasn't detected by the scraper
                    # This is a data pipeline issue, not a website issue
                    return issues
            
            # No navigation detected by any method
            issues.append(self.create_issue(
                job_id, project_id, url,
                "No navigation detected on page",
                "No navigation elements found using semantic, structural, or class-based detection",
                "Clear navigation structure for users and crawlers",
                data_key="has_navigation",
                data_path="has_navigation",
                impact="Missing navigation eliminates internal linking opportunities and makes it difficult for users and crawlers to discover related content.",
                recommendation="Add clear navigation structure using semantic HTML5 elements (<nav>). Include links to important pages and ensure navigation is accessible to both users and search engine crawlers."
            ))
        
        return issues


class SchemaMarkupRule(BaseSEORuleV2):
    rule_id = "schema_markup"
    rule_no = 13
    category = "Internal Pages"
    severity = "high"
    description = "Missing schema leaves rich results unused and weakens entity understanding"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        # ── Collect structured_data from all known storage locations ────────────
        structured_data = normalized.get("structured_data", [])
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )

        # ── Collect json_ld_present / json_ld_count from all known locations ────
        json_ld_present = normalized.get("json_ld_present", False)
        json_ld_count   = normalized.get("json_ld_count", 0)

        if not json_ld_present:
            json_ld_present = (
                normalized.get("format_detection", {}).get("json_ld_present", False) or
                normalized.get("schema_format_signals", {}).get("json_ld_present", False)
            )

        if json_ld_count == 0:
            json_ld_count = (
                normalized.get("format_detection", {}).get("json_ld_count", 0) or
                normalized.get("schema_format_signals", {}).get("json_ld_count", 0)
            )

        # ── Pipeline-gap inference ───────────────────────────────────────────────
        # extract_structured_data() in schema.py reads EXCLUSIVELY from
        # <script type="application/ld+json"> tags.  It never sets json_ld_present
        # or json_ld_count in the normalised dict, so those fields always default
        # to False/0 even when valid JSON-LD is present.
        #
        # Because structured_data can only be populated from JSON-LD scripts, a
        # non-empty list is definitive proof that JSON-LD exists.  Infer the flag
        # here so downstream logic does not misclassify the page.
        if not json_ld_present and structured_data:
            json_ld_present = True
            json_ld_count   = len(structured_data)

        # ── Decision ─────────────────────────────────────────────────────────────
        has_structured_data = bool(structured_data) or json_ld_present
        has_valid_json_ld   = json_ld_present and json_ld_count > 0

        if not has_valid_json_ld:
            if has_structured_data and not json_ld_present:
                # Genuine non-JSON-LD format (Microdata/RDFa detected via a format
                # detection path but no JSON-LD scripts found).
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Schema markup detected but not in JSON-LD format - crawlers may ignore it",
                    f"Schema present but JSON-LD: {json_ld_present}, Count: {json_ld_count}",
                    "Valid JSON-LD schema markup present",
                    data_key="json_ld_present",
                    data_path="json_ld_present",
                    impact="Schema markup in non-JSON-LD formats (Microdata, RDFa) may be ignored by Google and AI crawlers, missing rich result opportunities.",
                    recommendation="Convert schema markup to JSON-LD format. JSON-LD is the preferred format for Google and provides better crawler support and easier maintenance."
                ))
            elif not has_structured_data:
                # No schema detected at all.
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "No schema markup found on page",
                    "No structured data detected",
                    "Valid JSON-LD schema markup present",
                    data_key="structured_data",
                    data_path="structured_data"
                ))

        return issues


class FaqSchemaRule(BaseSEORuleV2):
    rule_id = "faq_schema"
    rule_no = 14
    category = "Internal Pages"
    severity = "medium"
    description = "FAQ schema unlocks expanded SERP results and powers AEO/AI panels"

    # Heading text must contain one of these exact terms to count as an FAQ heading.
    # Generic question words ("what", "how", "why") are explicitly excluded because
    # marketing section headings ("What We Do", "Why Choose Us") are not FAQ content.
    _FAQ_HEADING_KEYWORDS = frozenset([
        "faq", "frequently asked", "frequently asked questions",
        "common questions", "questions & answers", "q&a", "q & a",
    ])

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        # ── Schema detection ─────────────────────────────────────────────────
        structured_data = normalized.get("structured_data", [])
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )

        has_faq_schema = False
        for schema in structured_data:
            if isinstance(schema, dict):
                schema_type = schema.get("@type")
                types = schema_type if isinstance(schema_type, list) else [schema_type]
                if "FAQPage" in types:
                    has_faq_schema = True
                    break

        # faq_howto_signals is stored under this key by the enhanced scraper
        faq_howto = normalized.get("faq_howto_signals", {})
        faq_schema_present = (
            has_faq_schema
            or normalized.get("faq_schema_present", False)
            or faq_howto.get("faq_schema_present", False)
            or normalized.get("schema_analysis", {}).get("faq_schema_present", False)
        )

        # Page already has FAQ schema — nothing to flag
        if faq_schema_present:
            return issues

        # ── Content evidence — three high-precision signals only ─────────────
        #
        # Signal A: Scraper found an explicit FAQ section (a heading containing
        #   "faq"/"frequently asked"/"common questions" followed by Q&A items).
        #   This is computed by extract_faq_howto_signals() and is reliable.
        faq_section_count = faq_howto.get("faq_section_count", 0)

        # Signal B: Multiple confirmed question+answer pairs — heading ending
        #   with '?' followed by an answer paragraph (>20 chars). Requires ≥3
        #   pairs so isolated section openers don't trigger the rule.
        qa_pattern_count = faq_howto.get("qa_pattern_count", 0)

        # Signal C: A heading explicitly names the FAQ section (high-precision
        #   keywords only). Generic question words are not checked here because
        #   "What We Do" / "Why Choose Us" are marketing headings, not FAQ headings.
        has_faq_heading = False
        for heading in normalized.get("headings", []):
            heading_text = heading.get("text", "").lower().strip()
            if any(kw in heading_text for kw in self._FAQ_HEADING_KEYWORDS):
                has_faq_heading = True
                break

        has_faq_content = (
            faq_section_count > 0    # explicit FAQ section detected by scraper
            or has_faq_heading       # heading says "FAQ" / "Frequently Asked Questions"
            or qa_pattern_count >= 3 # 3+ real question(?) + answer pairs
        )

        if has_faq_content:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "FAQ content found but no FAQ schema markup",
                (
                    f"FAQ content detected | sections={faq_section_count} "
                    f"| qa_pairs={qa_pattern_count} | faq_heading={has_faq_heading}"
                ),
                "FAQPage schema markup for FAQ content",
                data_key="faq_schema_present",
                data_path="faq_schema_present",
                impact="Missing FAQ schema markup loses rich result opportunities in SERPs and AI-powered answer boxes, reducing visibility and click-through rates.",
                recommendation="Add FAQPage schema markup to your FAQ content. Include question-answer pairs in proper JSON-LD format to enable rich snippets and improve AI search visibility."
            ))

        return issues




def register_internal_rules(registry):
    """Register all internal page rules with the registry."""
    registry.register(BrokenLinksRule())
    registry.register(Custom404PageRule())
    registry.register(FourXXErrorPagesRule())
    registry.register(FiveXXServerErrorRule())
    registry.register(TimeoutErrorRule())
    registry.register(MixedHttpHttpsRule())
    registry.register(NonSeoFriendlyUrlsRule())
    registry.register(DoubleSlashUrlsRule())
    # DISABLED: Very low content ratio
    # registry.register(LowCodeToHtmlRatioRule())
    registry.register(LongUrlsRule())
    registry.register(NavigationVisibilityRule())
    registry.register(SchemaMarkupRule())
    registry.register(FaqSchemaRule())
