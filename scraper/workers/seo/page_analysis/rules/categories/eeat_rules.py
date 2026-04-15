"""
E-E-A-T / Trust SEO Rules
Rules for author information, expertise, content quality, and trust signals.
"""

from ..base_seo_rule import BaseSEORuleV2
import re
from urllib.parse import urlparse


class ContextValidator:
    """Multi-layer validation utilities for SEO rules"""
    
    @staticmethod
    def detect_page_type(url, content, headings):
        """Detect page type using URL patterns, content intent, and headings"""
        url_lower = url.lower()
        content_lower = content.lower()
        
        # URL pattern detection
        if any(pattern in url_lower for pattern in ['/blog/', '/article/', '/post/', '/news/']):
            return 'blog'
        elif any(pattern in url_lower for pattern in ['/service/', '/services/', '/solution/']):
            return 'service'
        elif any(pattern in url_lower for pattern in ['/product/', '/products/', '/item/']):
            return 'product'
        elif any(pattern in url_lower for pattern in ['/about/', '/team/', '/company/']):
            return 'about'
        elif any(pattern in url_lower for pattern in ['/contact/', '/support/']):
            return 'contact'
        
        # Content intent detection
        if any(indicator in content_lower for indicator in ['blog', 'article', 'post', 'guide', 'tutorial']):
            return 'blog'
        elif any(indicator in content_lower for indicator in ['service', 'solution', 'consulting', 'agency']):
            return 'service'
        elif any(indicator in content_lower for indicator in ['product', 'price', 'buy', 'shop', 'cart']):
            return 'product'
        
        # Heading signals
        h1_texts = [h.get('text', '').lower() for h in headings if h.get('tag') == 'h1']
        if h1_texts:
            h1_text = h1_texts[0]
            if any(word in h1_text for word in ['blog', 'article', 'guide']):
                return 'blog'
            elif any(word in h1_text for word in ['service', 'solution']):
                return 'service'
            elif any(word in h1_text for word in ['product', 'price']):
                return 'product'
        
        return 'general'
    
    @staticmethod
    def validate_schema_correctness(page_type, structured_data):
        """Validate schema matches page type"""
        schema_types = []
        for schema in structured_data:
            schema_type = schema.get('@type')
            if isinstance(schema_type, list):
                schema_types.extend(schema_type)
            else:
                schema_types.append(schema_type)
        
        # Invalid schema combinations
        if page_type == 'service' and 'Article' in schema_types:
            return False, "Article schema on service page"
        elif page_type == 'product' and 'Article' in schema_types:
            return False, "Article schema on product page"
        elif page_type == 'blog' and 'Article' not in schema_types and 'BlogPosting' not in schema_types:
            return False, "Missing Article/BlogPosting schema on blog page"
        
        return True, "Schema matches page type"
    
    @staticmethod
    def is_human_author(author_name):
        """Validate author appears to be human, not entity"""
        if not author_name:
            return False, "Empty author name"
        
        author_lower = author_name.lower()
        
        # Reject non-human entities
        non_human = ['agency', 'team', 'company', 'group', 'staff', 'admin', 'editor', 'service', 'solution']
        if any(word in author_lower for word in non_human):
            return False, f"Non-human author: {author_name}"
        
        # Check for human-like name pattern (First + Last)
        words = author_name.strip().split()
        if len(words) < 2:
            return False, f"Single word author: {author_name}"
        
        # Should have at least 2 words and reasonable length
        if len(words) >= 2 and all(len(word) > 1 for word in words[:2]):
            return True, "Human author pattern detected"
        
        return False, f"Invalid author pattern: {author_name}"
    
    @staticmethod
    def is_ui_visible(content, indicators):
        """Check if information is visible in UI, not just meta"""
        content_lower = content.lower()
        return any(indicator.lower() in content_lower for indicator in indicators)
    
    @staticmethod
    def validate_same_as(same_as_links):
        """Validate sameAs completeness and quality"""
        if not same_as_links:
            return False, "Missing sameAs links"
        
        if len(same_as_links) < 2:
            return False, f"Insufficient sameAs links: {len(same_as_links)}"
        
        # Check for valid social media patterns
        valid_patterns = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'youtube.com', 'pinterest.com', 'tiktok.com'
        ]
        
        valid_links = 0
        for link in same_as_links:
            if any(pattern in link.lower() for pattern in valid_patterns):
                valid_links += 1
        
        if valid_links < 2:
            return False, f"Insufficient valid social links: {valid_links}"
        
        return True, f"Valid sameAs with {valid_links} social links"
    
    @staticmethod
    def calculate_confidence(passed_checks, total_checks):
        """Calculate confidence score 0-1"""
        if total_checks == 0:
            return 0.0
        return passed_checks / total_checks


class AuthorNameBioRule(BaseSEORuleV2):
    rule_id = "author_name_bio"
    rule_no = 104
    category = "EEAT"
    severity = "high"
    description = "Named real authors are a core E-E-A-T signal — AI cites authored content over anonymous"
    excluded_page_types = ['contact', 'privacy', 'terms', 'login']  # Exclude utility pages

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Multi-layer validation
        content = normalized.get("content", "")
        meta_tags = normalized.get("meta_tags", {})
        structured_data = normalized.get("structured_data", [])
        headings = normalized.get("headings", [])
        
        # Enhanced validation - check multiple sources
        author_signals = normalized.get("author_signals", {})
        if not author_signals:
            # Fallback to basic checks
            author_signals = {
                "author_text_elements": [],
                "author_links": [],
                "author_schema_data": structured_data,
                "author_bio_sections": [],
                "author_element_count": 0,
                "author_link_count": 0,
                "author_schema_count": 0,
                "bio_section_count": 0
            }
        
        # Context validation - detect page type
        page_type = ContextValidator.detect_page_type(url, content, headings)
        
        # Validation layers
        validation_results = {
            'exists': False,
            'is_contextually_valid': False,
            'is_complete': False,
            'is_user_visible': False,
            'is_semantically_correct': False
        }
        
        failure_reasons = []
        
        # 1. EXISTENCE: Check author information exists
        meta_author = meta_tags.get("author", [])
        author_name = meta_author[0] if meta_author and meta_author[0] else ""
        
        # Enhanced author detection from multiple sources
        schema_author = None
        for schema in structured_data:
            if schema.get("author"):
                schema_author = schema["author"]
                break
            elif schema.get("@type") == "Person":
                schema_author = schema
                break
        
        # Check author signals from enhanced extraction
        has_author_elements = (
            author_signals.get("author_element_count", 0) > 0 or
            author_signals.get("author_schema_count", 0) > 0 or
            author_signals.get("author_link_count", 0) > 0
        )
        
        if author_name or schema_author or has_author_elements:
            validation_results['exists'] = True
        else:
            failure_reasons.append("No author information found in meta, schema, or content")
        
        # 2. CONTEXT VALIDATION: Author should be on content pages
        if page_type in ['blog', 'general', 'service']:
            validation_results['is_contextually_valid'] = True
        else:
            failure_reasons.append(f"Author on {page_type} page may not be appropriate")
        
        # 3. HUMAN ENTITY VALIDATION: Must be human, not organization
        detected_author_name = author_name
        if schema_author and isinstance(schema_author, dict):
            detected_author_name = schema_author.get('name', '')
        
        if detected_author_name:
            is_human, human_reason = ContextValidator.is_human_author(detected_author_name)
            if is_human:
                validation_results['is_semantically_correct'] = True
            else:
                failure_reasons.append(f"Non-human author: {detected_author_name}")
        elif has_author_elements:
            # Check if schema author is Person type
            if author_signals.get("author_schema_count", 0) > 0:
                validation_results['is_semantically_correct'] = True
            else:
                failure_reasons.append("Author appears to be entity, not human")
        else:
            failure_reasons.append("No valid author name to validate")
        
        # 4. UI VISIBILITY: Must be visible in content, not just meta
        author_ui_indicators = ["author", "by ", "written by", "posted by", "published by"]
        has_visible_author = (
            ContextValidator.is_ui_visible(content, author_ui_indicators) or
            author_signals.get("author_element_count", 0) > 0
        )
        
        if has_visible_author:
            validation_results['is_user_visible'] = True
        else:
            failure_reasons.append("Author not visible in page content")
        
        # 5. COMPLETENESS: Must have bio section with real information
        bio_indicators = [
            "about the author", "author bio", "biography", "about author", "author profile",
            "experience", "credentials", "background", "expertise", "qualification"
        ]
        
        has_bio_section = (
            ContextValidator.is_ui_visible(content, bio_indicators) or
            author_signals.get("bio_section_count", 0) > 0
        )
        
        # Enhanced bio validation - check for substantial content
        if has_bio_section:
            # Look for substantial bio content (not just "About the author" heading)
            bio_patterns = [
                r'author.*\d+\s+years?\s+experience',
                r'background.*in.*',
                r'expertise.*includes?',
                r'specialized.*in',
                r'worked.*with',
                r'certified.*in'
            ]
            
            has_substantial_bio = any(re.search(pattern, content.lower()) for pattern in bio_patterns)
            if has_substantial_bio:
                validation_results['is_complete'] = True
            else:
                failure_reasons.append("Author bio section lacks substantial content")
        else:
            failure_reasons.append("Missing author bio section")
        
        # Calculate confidence score
        passed_checks = sum(validation_results.values())
        total_checks = len(validation_results)
        confidence_score = ContextValidator.calculate_confidence(passed_checks, total_checks)
        
        # FAIL if any critical validation fails
        if not all([
            validation_results['exists'],
            validation_results['is_user_visible'],
            validation_results['is_semantically_correct'],
            validation_results['is_complete']
        ]):
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Author validation failed: {', '.join(failure_reasons)}",
                f"Failures: {len(failure_reasons)} | Confidence: {confidence_score:.2f} | Author: {detected_author_name or 'None'}",
                "Visible human author with complete bio section including experience and credentials",
                data_key="author_signals",
                data_path="author_signals",
                impact="Missing or incomplete author information reduces EEAT signals, making content appear anonymous or AI-generated to search engines.",
                recommendation="Add visible human author with real name, complete bio section including experience, credentials, and background information. Avoid generic entity names like 'S-Agency'."
            ))
        
        return issues


class AuthorPhotoRule(BaseSEORuleV2):
    rule_id = "author_photo"
    rule_no = 105
    category = "EEAT"
    severity = "medium"
    description = "Real author photos signal human expertise; stock images flag AI-generated content"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        content = normalized.get("content", "").lower()
        images = normalized.get("images", [])
        
        # Enhanced detection - check if author information is present on page
        author_indicators = ["author", "by ", "written by", "posted by", "published by"]
        has_author_content = ContextValidator.is_ui_visible(content, author_indicators)
        
        if has_author_content:
            # Enhanced author image detection
            author_images = []
            testimonial_images = []
            
            for image in images:
                alt = image.get("alt", "").lower()
                src = image.get("src", "").lower()
                
                # Author-specific indicators
                author_keywords = ["author", "avatar", "profile", "bio", "headshot"]
                testimonial_keywords = ["testimonial", "jennifer", "mike", "sarah", "david", "emily", "client"]
                
                is_author_image = any(keyword in alt or keyword in src for keyword in author_keywords)
                is_testimonial_image = any(keyword in alt or keyword in src for keyword in testimonial_keywords)
                
                if is_author_image and not is_testimonial_image:
                    author_images.append(image)
                elif is_testimonial_image:
                    testimonial_images.append(image)
            
            # Additional check: look for images near author-related content
            if not author_images:
                # Check for images that might be author photos based on context
                content_sections = content.split('\n')
                for i, section in enumerate(content_sections):
                    if any(indicator in section for indicator in author_indicators):
                        # Look for images in nearby content (within 3 sections)
                        for j in range(max(0, i-3), min(len(content_sections), i+4)):
                            if any(img.get("src", "") in content_sections[j] for img in images):
                                # Found image near author content
                                nearby_images = [img for img in images if img.get("src", "") in content_sections[j]]
                                author_images.extend(nearby_images)
                                break
            
            # Validation logic
            if not author_images:
                if testimonial_images:
                    # Has images but they're testimonials, not author
                    issues.append(self.create_issue(
                        job_id, project_id, url,
                        "Author section missing real person photo (found testimonial images instead)",
                        f"Found {len(testimonial_images)} testimonial images, 0 author photos",
                        "Real author photo next to author bio (distinguished from testimonials)",
                        data_key="images",
                        data_path="images.author_photo",
                        impact="Missing author photo while having testimonial photos suggests anonymous or AI-generated content, reducing EEAT credibility.",
                        recommendation="Add real author photo next to author bio section. Ensure author photos are distinct from testimonial/client images."
                    ))
                else:
                    # No images at all
                    issues.append(self.create_issue(
                        job_id, project_id, url,
                        "Author section missing real person photo",
                        "No author image found on page with author content",
                        "Real photo next to author bio",
                        data_key="images",
                        data_path="images.author_photo",
                        impact="Missing author photo reduces human expertise signals and may indicate anonymous or low-quality content.",
                        recommendation="Add real author photo next to author bio section. Use professional headshot, not stock images or generic graphics."
                    ))
        
        return issues


class PersonSchemaLinkedRule(BaseSEORuleV2):
    rule_id = "person_schema_linked"
    rule_no = 106
    category = "EEAT"
    severity = "high"
    description = "Person schema linked via worksFor strengthens AI entity graph connections"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        structured_data = normalized.get("structured_data", [])
        
        # Enhanced validation for Person schema
        has_person_schema = False
        has_works_for = False
        person_schema = None
        author_name = ""
        
        for schema in structured_data:
            if schema.get("@type") == "Person":
                has_person_schema = True
                person_schema = schema
                author_name = schema.get("name", "")
                
                # Check for worksFor property
                if schema.get("worksFor"):
                    has_works_for = True
                    break
        
        if has_person_schema:
            # Enhanced validation - check if it's a real person vs generic entity
            is_human, human_reason = ContextValidator.is_human_author(author_name)
            
            # Check for social links in Person schema
            has_same_as = bool(person_schema.get("sameAs"))
            has_url = bool(person_schema.get("url"))
            has_job_title = bool(person_schema.get("jobTitle") or person_schema.get("description"))
            
            # Validation issues
            if not is_human:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Person schema uses generic entity name: '{author_name}'",
                    f"Non-human author detected: {human_reason}",
                    "Person schema with real human name (First + Last)",
                    data_key="structured_data",
                    data_path="structured_data.person.name",
                    impact="Generic entity names in Person schema weaken entity graph and reduce EEAT credibility with AI systems.",
                    recommendation="Use real human name in Person schema. Replace 'S-Agency' with actual person's name like 'John Smith'."
                ))
            
            if not has_works_for:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Person schema missing worksFor property",
                    f"Person: '{author_name}' without organization link",
                    "Person schema with worksFor pointing to main Organization @id",
                    data_key="structured_data",
                    data_path="structured_data.person.worksfor",
                    impact="Missing worksFor property breaks entity graph connection between person and organization.",
                    recommendation="Add worksFor property to Person schema linking to the Organization schema @id."
                ))
            
            if not has_same_as and not has_url:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Person schema missing identity verification (sameAs/url)",
                    f"Person: '{author_name}' lacks social links or profile URL",
                    "Person schema with sameAs (social profiles) or URL (profile page)",
                    data_key="structured_data",
                    data_path="structured_data.person.identity",
                    impact="Missing identity verification reduces entity authority and makes person appear less credible.",
                    recommendation="Add sameAs array with social media URLs or URL property pointing to author's profile page."
                ))
            
            if not has_job_title:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Person schema missing role/expertise information",
                    f"Person: '{author_name}' lacks jobTitle or description",
                    "Person schema with jobTitle and/or description of expertise",
                    data_key="structured_data",
                    data_path="structured_data.person.role",
                    impact="Missing role information reduces expertise signals and EEAT authority.",
                    recommendation="Add jobTitle property and/or description explaining the person's role and expertise."
                ))
        
        return issues


class AboutPageRule(BaseSEORuleV2):
    rule_id = "about_page"
    rule_no = 107
    category = "EEAT"
    severity = "high"
    description = "A detailed About page is reviewed by both quality raters and AI systems"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check if this is an about page
        url_lower = url.lower()
        is_about_page = any(indicator in url_lower for indicator in ["about", "/about-us", "about.html"])
        
        if is_about_page:
            content = normalized.get("content", "")
            
            # Check word count
            import re
            visible_text = re.sub(r'<[^>]+>', '', content).strip()
            word_count = len(visible_text.split())
            
            if word_count < 300:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"About page too short: {word_count} words",
                    f"Word count: {word_count} (minimum: 300)",
                    "About page with ≥ 300 words including story, team, and timeline",
                    data_key="content",
                    data_path="content.word_count"
                ))
            
            # Check for required elements
            content_lower = content.lower()
            required_elements = ["team", "story", "founded", "mission", "since", "year"]
            missing_elements = []
            
            for element in required_elements:
                if element not in content_lower:
                    missing_elements.append(element)
            
            if len(missing_elements) > 3:  # Allow some flexibility
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"About page missing key elements: {', '.join(missing_elements)}",
                    f"Missing: {missing_elements}",
                    "About page with story, team, mission, and founding information",
                    data_key="content",
                    data_path="content.about_elements"
                ))
        
        return issues


class PrivacyTermsPagesRule(BaseSEORuleV2):
    rule_id = "privacy_terms_pages"
    rule_no = 108
    category = "EEAT"
    severity = "high"
    description = "Missing policy pages are an instant E-E-A-T red flag for quality raters"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Enhanced validation - check if this is homepage or main page
        url_lower = url.lower()
        is_main_page = (
            url.endswith('/') or 
            url.endswith('/index.html') or
            'about' not in url_lower and 
            'privacy' not in url_lower and 
            'terms' not in url_lower and
            'contact' not in url_lower
        )
        
        if is_main_page:
            # Check for privacy and terms links in content
            content = normalized.get("content", "").lower()
            
            # Look for privacy policy and terms links
            privacy_indicators = [
                "privacy policy", "privacy", "privacy.html", 
                "/privacy", "privacy-policy"
            ]
            
            terms_indicators = [
                "terms of service", "terms and conditions", "terms", 
                "terms.html", "/terms", "terms-conditions"
            ]
            
            has_privacy_link = any(indicator in content for indicator in privacy_indicators)
            has_terms_link = any(indicator in content for indicator in terms_indicators)
            
            # Check footer specifically
            footer_patterns = [
                r'<footer[^>]*>.*?</footer>',
                r'class="footer".*?</div>',
                r'id="footer".*?</div>'
            ]
            
            footer_content = ""
            import re
            for pattern in footer_patterns:
                match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                if match:
                    footer_content = match.group(0).lower()
                    break
            
            # Check in footer specifically
            privacy_in_footer = any(indicator in footer_content for indicator in privacy_indicators)
            terms_in_footer = any(indicator in footer_content for indicator in terms_indicators)
            
            # Validation issues
            if not has_privacy_link and not privacy_in_footer:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Missing Privacy Policy link",
                    "No privacy policy link found in page content or footer",
                    "Privacy Policy link in footer or main navigation",
                    data_key="content",
                    data_path="content.privacy_link",
                    impact="Missing privacy policy is an instant EEAT red flag for quality raters and legal compliance.",
                    recommendation="Add Privacy Policy link in footer or main navigation. Ensure the privacy policy page exists and has substantial content."
                ))
            
            if not has_terms_link and not terms_in_footer:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Missing Terms of Service link",
                    "No terms of service link found in page content or footer",
                    "Terms of Service link in footer or main navigation",
                    data_key="content",
                    data_path="content.terms_link",
                    impact="Missing terms of service is an EEAT red flag and may indicate lack of professionalism.",
                    recommendation="Add Terms of Service link in footer or main navigation. Ensure the terms page exists and has proper legal content."
                ))
        
        return issues


class FirstHandExperienceRule(BaseSEORuleV2):
    rule_id = "firsthand_experience"
    rule_no = 109
    category = "EEAT"
    severity = "high"
    description = "Specific metrics, case study results, and original data prove human expert authorship"
    excluded_page_types = ['contact', 'privacy', 'terms', 'login']  # Exclude utility pages

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        content = normalized.get("content", "").lower()
        
        # Enhanced detection for first-hand experience indicators
        experience_indicators = [
            "we tested", "our study", "we found", "our research", "we analyzed",
            "case study", "test results", "our data", "we measured", "experiment",
            "we conducted", "our team", "in our testing", "based on our",
            "our results", "our analysis", "our investigation", "we surveyed",
            "we worked with", "our clients", "we helped", "we achieved"
        ]
        
        found_indicators = [indicator for indicator in experience_indicators if indicator in content]
        
        # Enhanced detection for specific metrics with context
        import re
        
        # Look for specific numbers with business context
        metric_patterns = [
            r'\d+(?:\.\d+)?%\s+(?:increase|decrease|improvement|reduction|growth)',
            r'\$?\d+(?:,\d{3})*(?:\.\d+)?\s+(?:revenue|sales|profit|income|savings)',
            r'\d+(?:,\d{3})*\s+(?:customers|clients|users|leads|conversions)',
            r'\d+\s+(?:years?|months?|weeks?)\s+of\s+experience',
            r'from\s+\d+\s+to\s+\d+',
            r'\d+x\s+(?:return|roi|growth)'
        ]
        
        numbers_with_context = []
        for pattern in metric_patterns:
            matches = re.findall(pattern, content)
            numbers_with_context.extend(matches)
        
        # Enhanced case study detection
        case_study_patterns = [
            r'case study[:\s]*[^.!?]*[.!?]',
            r'client[:\s]*[^.!?]*result[:\s]*[^.!?]*[.!?]',
            r'project[:\s]*[^.!?]*outcome[:\s]*[^.!?]*[.!?]',
            r'test[:\s]*[^.!?]*result[:\s]*[^.!?]*[.!?]',
            r'experiment[:\s]*[^.!?]*show[:\s]*[^.!?]*[.!?]'
        ]
        
        case_studies = []
        for pattern in case_study_patterns:
            matches = re.findall(pattern, content)
            case_studies.extend(matches)
        
        # Look for real client names (evidence of actual work)
        client_name_patterns = [
            r'[A-Z][a-z]+\s+[A-Z][a-z]+\s+(?:said|reported|experienced|achieved)',
            r'client[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
            r'for\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+we'
        ]
        
        client_names = []
        for pattern in client_name_patterns:
            matches = re.findall(pattern, content)
            client_names.extend(matches)
        
        # Look for evidence of real work (screenshots, data, etc.)
        evidence_patterns = [
            r'screenshot',
            r'before.*after',
            r'data.*shows?',
            r'results.*show',
            r'we.*documented',
            r'actual.*results',
            r'real.*client'
        ]
        
        has_evidence = any(pattern in content for pattern in evidence_patterns)
        
        # Validation logic
        evidence_score = 0
        
        if found_indicators:
            evidence_score += len(found_indicators) * 0.2
        
        if numbers_with_context:
            evidence_score += len(numbers_with_context) * 0.3
        
        if case_studies:
            evidence_score += len(case_studies) * 0.4
        
        if client_names:
            evidence_score += len(client_names) * 0.5
        
        if has_evidence:
            evidence_score += 0.3
        
        # FAIL if insufficient evidence of first-hand experience
        if evidence_score < 1.0:  # Require substantial evidence
            missing_elements = []
            
            if not found_indicators:
                missing_elements.append("first-hand language")
            if not numbers_with_context:
                missing_elements.append("specific metrics")
            if not case_studies:
                missing_elements.append("case studies")
            if not client_names:
                missing_elements.append("real client names")
            if not has_evidence:
                missing_elements.append("proof/evidence")
            
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Content lacks first-hand experience signals (evidence score: {evidence_score:.1f})",
                f"Missing: {', '.join(missing_elements)} | Found: {len(found_indicators)} indicators, {len(numbers_with_context)} metrics",
                "Content with quantified first-hand experience, case studies, and real client evidence",
                data_key="content",
                data_path="content.experience",
                impact="Content without first-hand experience appears as marketing claims rather than expert knowledge, reducing EEAT and search rankings.",
                recommendation="Add specific case studies with real client names, quantified results with before/after data, screenshots or documentation of actual work. Use first-hand language like 'we tested', 'our data shows', 'we achieved X results for Y client'."
            ))
        
        return issues


class ContentFreshnessRule(BaseSEORuleV2):
    rule_id = "content_freshness"
    rule_no = 110
    category = "EEAT"
    severity = "medium"
    description = "AI systems and Google's freshness algorithm downrank outdated content"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        content = normalized.get("content", "")
        
        # Enhanced validation - check for VISIBLE date information in content
        date_indicators = [
            "last updated", "updated on", "published on", "posted on",
            "last modified", "date:", "published:", "updated:",
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
            "updated:", "published:", "modified:", "last reviewed"
        ]
        
        content_lower = content.lower()
        has_visible_date = any(indicator in content_lower for indicator in date_indicators)
        
        # Look for actual date patterns in visible content
        import re
        date_patterns = [
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',  # MM/DD/YYYY or MM-DD-YYYY
            r'\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b',    # YYYY/MM/DD or YYYY-MM-DD
            r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]* \d{1,2},? \d{4}\b',  # Month DD, YYYY
            r'\b\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*,? \d{4}\b',  # DD Month YYYY
        ]
        
        has_actual_date = any(re.search(pattern, content_lower) for pattern in date_patterns)
        
        # Check for schema dates (but these are NOT sufficient for user visibility)
        schema_dates = normalized.get("last_updated_signals", {}).get("schema_dates", [])
        date_meta_tags = normalized.get("last_updated_signals", {}).get("date_meta_tags", [])
        
        has_schema_dates = len(schema_dates) > 0 or len(date_meta_tags) > 0
        
        # Enhanced validation - check if dates are in meaningful context
        meaningful_date_patterns = [
            r'updated\s+(?:on|:)?\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'published\s+(?:on|:)?\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'last\s+updated\s+(?:on|:)?\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'modified\s+(?:on|:)?\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}'
        ]
        
        has_meaningful_date = any(re.search(pattern, content_lower) for pattern in meaningful_date_patterns)
        
        # FAIL if no visible date found (meta-only dates are NOT sufficient)
        if not has_visible_date or not has_actual_date or not has_meaningful_date:
            missing_elements = []
            
            if not has_visible_date:
                missing_elements.append("visible date indicators")
            if not has_actual_date:
                missing_elements.append("actual date patterns")
            if not has_meaningful_date:
                missing_elements.append("meaningful date context")
            
            schema_info = "" if has_schema_dates else " (schema dates present but not visible)"
            
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Page missing visible date information{schema_info}",
                f"Missing: {', '.join(missing_elements)} | Schema dates: {'Yes' if has_schema_dates else 'No'}",
                "Visible last-updated or published date in page content (not just schema/meta)",
                data_key="last_updated_signals",
                data_path="last_updated_signals",
                impact="Dates visible only in schema/meta are insufficient for users. Missing visible dates reduce trust and EEAT signals.",
                recommendation="Add visible publication or last-updated date in page content. Use formats like 'Last updated: Month DD, YYYY' or 'Published: Month DD, YYYY' near the title or at the bottom of the content."
            ))
        
        return issues


def register_eeat_rules(registry):
    """Register all E-E-A-T rules with the registry."""
    registry.register(AuthorNameBioRule())
    registry.register(AuthorPhotoRule())
    registry.register(PersonSchemaLinkedRule())
    registry.register(AboutPageRule())
    registry.register(PrivacyTermsPagesRule())
    registry.register(FirstHandExperienceRule())
    registry.register(ContentFreshnessRule())
