"""
Content SEO Rules
Rules for meta tags, titles, headings, and content quality.
"""

from ..base_seo_rule import BaseSEORuleV2


class MultipleMetaDescriptionsRule(BaseSEORuleV2):
    rule_id = "multiple_meta_descriptions"
    rule_no = 27
    category = "Content"
    severity = "medium"
    description = "Duplicate meta descriptions cause conflicting SERP snippet signals"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        descriptions = meta_tags.get("description", [])
        if len(descriptions) > 1:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Multiple meta descriptions found: {len(descriptions)}",
                f"Meta descriptions: {descriptions}",
                "Exactly one meta description",
                data_key="meta_tags",
                data_path="meta_tags.description"
            ))
        
        return issues


class MultipleTitleTagsRule(BaseSEORuleV2):
    rule_id = "multiple_title_tags"
    rule_no = 28
    category = "Content"
    severity = "high"
    description = "Multiple title tags produce unpredictable SERP titles"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        titles = meta_tags.get("title", [])
        if len(titles) > 1:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Multiple title tags found: {len(titles)}",
                f"Title tags: {titles}",
                "Exactly one title tag",
                data_key="meta_tags",
                data_path="meta_tags.title"
            ))
        
        return issues


class TitleMissingRule(BaseSEORuleV2):
    rule_id = "title_missing"
    rule_no = 29
    category = "Content"
    severity = "high"
    description = "Missing titles are among the most damaging on-page SEO issues"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        title = normalized.get("title", "")
        
        if not title or not title.strip():
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Title tag is missing or empty",
                "No title content",
                "Non-empty title tag",
                data_key="title",
                data_path="title"
            ))
        
        return issues


class MetaDescriptionMissingRule(BaseSEORuleV2):
    rule_id = "meta_description_missing"
    rule_no = 30
    category = "Content"
    severity = "high"
    description = "Missing meta descriptions reduce CTR; Google auto-generates poor ones"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        descriptions = meta_tags.get("description", [])
        if not descriptions or not descriptions[0] or not descriptions[0].strip():
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Meta description is missing or empty",
                "No meta description content",
                "Unique meta description",
                data_key="meta_tags",
                data_path="meta_tags.description"
            ))
        
        return issues


class TitleTooShortRule(BaseSEORuleV2):
    rule_id = "title_too_short"
    rule_no = 31
    category = "Content"
    severity = "high"
    description = "Short titles under-communicate relevance and waste SERP real estate"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        title = normalized.get("title", "")
        
        if title and len(title.strip()) < 30:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title too short: {len(title)} characters",
                f"Title length: {len(title)} characters",
                "Title between 30-60 characters",
                data_key="title",
                data_path="title"
            ))
        
        return issues


class TitleTooLongRule(BaseSEORuleV2):
    rule_id = "title_too_long"
    rule_no = 32
    category = "Content"
    severity = "medium"
    description = "Long titles are truncated in SERPs — key info gets cut off"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        title = normalized.get("title", "")
        
        if title and len(title.strip()) > 60:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title too long: {len(title)} characters",
                f"Title length: {len(title)} characters",
                "Title ≤ 60 characters",
                data_key="title",
                data_path="title"
            ))
        
        return issues


class MetaDescriptionTooShortRule(BaseSEORuleV2):
    rule_id = "meta_description_too_short"
    rule_no = 33
    category = "Content"
    severity = "medium"
    description = "Short descriptions fail to communicate value and leave CTR on the table"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        descriptions = meta_tags.get("description", [])
        if descriptions and descriptions[0]:
            desc_length = len(descriptions[0].strip())
            
            # Enhanced validation - check multiple quality factors
            meta_ctr_score = normalized.get("meta_ctr_score", 0)
            has_power_word = normalized.get("has_power_word", False)
            has_cta = normalized.get("has_cta", False)
            
            # Quality issues that make even 158-char descriptions weak
            quality_issues = []
            if meta_ctr_score < 70:
                quality_issues.append(f"low CTR score ({meta_ctr_score}/100)")
            if not has_power_word:
                quality_issues.append("no power words")
            if not has_cta:
                quality_issues.append("no call-to-action")
            
            # Length-based validation
            if desc_length < 120:
                # Clear FAIL - too short
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Meta description too short: {desc_length} characters",
                    f"Description length: {desc_length} characters (minimum: 120)",
                    "Description between 120-158 characters",
                    data_key="meta_tags",
                    data_path="meta_tags.description",
                    impact="Short meta descriptions fail to communicate value proposition and waste SERP real estate, reducing click-through rates.",
                    recommendation="Expand meta description to 120-158 characters. Include value proposition, keywords, and compelling reasons to click."
                ))
            elif desc_length >= 120 and desc_length <= 158 and quality_issues:
                # Borderline case - length OK but quality poor
                issues_text = ", ".join(quality_issues)
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Meta description length acceptable ({desc_length} chars) but quality issues: {issues_text}",
                    f"Length: {desc_length} chars | Issues: {len(quality_issues)} | CTR score: {meta_ctr_score}/100",
                    "Strong meta description with power words and CTA",
                    data_key="meta_tags",
                    data_path="meta_tags.description",
                    impact="Meta description meets length requirements but lacks compelling elements that drive click-through rates and engagement.",
                    recommendation="Improve meta description quality by adding power words (free, expert, proven), clear call-to-action, and stronger value proposition while maintaining 120-158 character length."
                ))
        
        return issues


class MetaDescriptionTooLongRule(BaseSEORuleV2):
    rule_id = "meta_description_too_long"
    rule_no = 34
    category = "Content"
    severity = "medium"
    description = "Over-length descriptions are truncated in SERPs"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        descriptions = meta_tags.get("description", [])
        if descriptions and descriptions[0]:
            desc_length = len(descriptions[0].strip())
            if desc_length > 158:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Meta description too long: {desc_length} characters",
                    f"Description length: {desc_length} characters",
                    "Description ≤ 158 characters",
                    data_key="meta_tags",
                    data_path="meta_tags.description"
                ))
        
        return issues


class H1MissingRule(BaseSEORuleV2):
    rule_id = "h1_missing"
    rule_no = 35
    category = "Content"
    severity = "high"
    description = "Missing H1 weakens content hierarchy and removes primary topical signal"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        headings = normalized.get("headings", [])
        h1_tags = [h for h in headings if h.get("tag") == "h1"]
        
        if not h1_tags or not any(h1.get("text", "").strip() for h1 in h1_tags):
            issues.append(self.create_issue(
                job_id, project_id, url,
                "H1 tag is missing or empty",
                "No H1 content",
                "Descriptive non-empty H1",
                data_key="headings",
                data_path="headings.h1"
            ))
        
        return issues


class MultipleH1TagsRule(BaseSEORuleV2):
    rule_id = "multiple_h1_tags"
    rule_no = 36
    category = "Content"
    severity = "high"
    description = "Multiple H1s confuse document hierarchy and dilute keyword emphasis"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        headings = normalized.get("headings", [])
        h1_tags = [h for h in headings if h.get("tag") == "h1"]
        
        # Count non-empty H1 tags
        non_empty_h1 = [h1 for h1 in h1_tags if h1.get("text", "").strip()]
        if len(non_empty_h1) > 1:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Multiple H1 tags found: {len(non_empty_h1)}",
                f"H1 tags: {[h.get('text') for h in non_empty_h1]}",
                "Exactly one H1 tag",
                data_key="headings",
                data_path="headings.h1"
            ))
        
        return issues


class HeadingHierarchySkippedRule(BaseSEORuleV2):
    rule_id = "heading_hierarchy_skipped"
    rule_no = 37
    category = "Content"
    severity = "medium"
    description = "Skipped heading levels break document outline — hurts AEO and screen readers"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        headings = normalized.get("headings", [])
        
        if not headings:
            return issues
        
        # Enhanced hierarchy validation - track actual sequence
        hierarchy_errors = []
        previous_level = 0
        heading_sequence = []
        
        for heading in headings:
            tag = heading.get("tag", "").lower()
            if tag.startswith('h') and tag[1:].isdigit():
                level = int(tag[1:])
                heading_sequence.append(level)
                
                # Check for skipped levels (more than 1 level jump)
                if previous_level > 0 and level > previous_level + 1:
                    hierarchy_errors.append(f"from H{previous_level} → H{level} (H{level-1} missing)")
                
                previous_level = level
        
        # Report specific hierarchy errors
        if hierarchy_errors:
            errors_text = ", ".join(hierarchy_errors)
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Heading hierarchy errors detected: {errors_text}",
                f"Hierarchy errors: {len(hierarchy_errors)} | Sequence: {heading_sequence}",
                "Proper sequential heading hierarchy (H1 → H2 → H3)",
                data_key="headings",
                data_path="headings.hierarchy",
                impact="Skipped heading levels break document outline, confuse search engines about content structure, and hurt accessibility for screen readers.",
                recommendation="Fix heading hierarchy by ensuring proper sequential order. Use H1 for main title, H2 for main sections, H3 for subsections, etc. Never skip heading levels."
            ))
        
        return issues


class ThinContentRule(BaseSEORuleV2):
    rule_id = "thin_content"
    rule_no = 38
    category = "Content"
    severity = "high"
    description = "Thin pages lack depth to rank and are filtered by Helpful Content systems"
    excluded_page_types = ['contact', 'privacy', 'terms', 'login']  # Exclude utility pages

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        content = normalized.get("content", "")
        
        if content:
            import re
            # Remove HTML tags and count words
            visible_text = re.sub(r'<[^>]+>', '', content).strip()
            words = visible_text.split()
            word_count = len(words)
            
            # Different thresholds for different page types (simplified)
            threshold = 300  # Default threshold
            if word_count < threshold:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Thin content: {word_count} words",
                    f"Word count: {word_count} (minimum: {threshold})",
                    f"Content with ≥ {threshold} words",
                    data_key="content",
                    data_path="content.word_count"
                ))
        
        return issues


class KeywordNotInTitleRule(BaseSEORuleV2):
    rule_id = "keyword_not_in_title"
    rule_no = 39
    category = "Content"
    severity = "high"
    description = "Missing primary keyword in title is a fundamental ranking signal failure"
    excluded_page_types = ['contact', 'privacy', 'terms', 'login']  # Exclude utility pages

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # This requires target keyword knowledge
        # For implementation, we'd need the target keyword for this page
        # Simplified implementation would check if H1 content appears in title
        
        title = normalized.get("title", "").lower()
        headings = normalized.get("headings", [])
        h1_tags = [h for h in headings if h.get("tag") == "h1"]
        
        if h1_tags:
            h1_text = h1_tags[0].get("text", "").lower()
            # Simple check: if H1 has substantial content, should appear in title
            if len(h1_text.split()) > 2 and h1_text not in title:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Primary topic from H1 not found in title",
                    f"H1: '{h1_text}' not in title: '{title}'",
                    "Primary keyword present in title",
                    data_key="title",
                    data_path="title"
                ))
        
        return issues


class KeywordNotInH1Rule(BaseSEORuleV2):
    rule_id = "keyword_not_in_h1"
    rule_no = 40
    category = "Content"
    severity = "high"
    description = "H1 is the strongest on-page signal — missing keyword is a basic SEO miss"
    excluded_page_types = ['contact', 'privacy', 'terms', 'login']  # Exclude utility pages

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Get primary keyword and H1 text
        primary_keyword = normalized.get("primary_keyword", "")
        headings = normalized.get("headings", [])
        h1_tags = [h for h in headings if h.get("tag") == "h1"]
        
        if not primary_keyword or not h1_tags:
            return issues
        
        h1_text = h1_tags[0].get("text", "").lower()
        keyword_lower = primary_keyword.lower()
        
        # Enhanced keyword matching - check for broken word scenarios
        keyword_found = False
        broken_word_detected = False
        
        # Check for exact match
        if keyword_lower in h1_text:
            keyword_found = True
        else:
            # Check for broken word scenarios (e.g., "AgencyWithout" instead of "Agency Without")
            h1_words = h1_text.split()
            for i, word in enumerate(h1_words):
                # Check if keyword is split across word boundaries
                if keyword_lower.startswith(word.lower()):
                    remaining_keyword = keyword_lower[len(word):]
                    # Check if next word contains the remaining part
                    if i + 1 < len(h1_words):
                        next_word = h1_words[i + 1].lower()
                        if remaining_keyword.startswith(next_word):
                            broken_word_detected = True
                            break
                # Check if word contains keyword but broken
                elif word.lower().startswith(keyword_lower) and len(word) > len(keyword_lower):
                    # Check if it's a compound word like "AgencyWithout"
                    if any(char.isupper() for char in word[len(keyword_lower):]):
                        broken_word_detected = True
                        break
        
        # Report issues
        if broken_word_detected:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Primary keyword '{primary_keyword}' found in H1 but broken/compound word format",
                f"H1: '{h1_tags[0].get('text', '')}' - Keyword detected as broken compound word",
                f"Clean, separate keyword '{primary_keyword}' in H1",
                data_key="headings",
                data_path="headings.h1",
                impact="Broken keyword format in H1 reduces SEO effectiveness and may not be properly recognized by search engines as a keyword match.",
                recommendation="Separate compound words in H1 to ensure clean keyword presence. Change 'AgencyWithout' to 'Agency Without' for proper keyword recognition."
            ))
        elif not keyword_found:
            # Check if keyword topic appears in H1 (fallback)
            title = normalized.get("title", "").lower()
            title_words = title.split()[:3]
            main_topic = " ".join(title_words)
            
            if main_topic not in h1_text:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Primary keyword '{primary_keyword}' not found in H1",
                    f"H1: '{h1_tags[0].get('text', '')}' - Keyword: '{primary_keyword}'",
                    f"Include keyword '{primary_keyword}' in H1",
                    data_key="headings",
                    data_path="headings.h1",
                    impact="Missing primary keyword in H1 weakens the strongest on-page SEO signal and reduces relevance for target queries.",
                    recommendation="Add primary keyword to H1 tag. Ensure it appears naturally and accurately describes the page content."
                ))
        
        return issues


class DuplicateContentRule(BaseSEORuleV2):
    rule_id = "duplicate_content"
    rule_no = 41
    category = "Content"
    severity = "high"
    description = "Duplicate content splits ranking signals and wastes crawl budget"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # This requires cross-page analysis
        # For single-page analysis, we can't detect duplicates
        # This would need to be implemented at site level
        
        return issues




def register_content_rules(registry):
    """Register all content rules with the registry."""
    registry.register(MultipleMetaDescriptionsRule())
    registry.register(MultipleTitleTagsRule())
    registry.register(TitleMissingRule())
    registry.register(MetaDescriptionMissingRule())
    registry.register(TitleTooShortRule())
    registry.register(TitleTooLongRule())
    registry.register(MetaDescriptionTooShortRule())
    registry.register(MetaDescriptionTooLongRule())
    registry.register(H1MissingRule())
    registry.register(MultipleH1TagsRule())
    registry.register(HeadingHierarchySkippedRule())
    registry.register(ThinContentRule())
    registry.register(KeywordNotInTitleRule())
    registry.register(KeywordNotInH1Rule())
    registry.register(DuplicateContentRule())
