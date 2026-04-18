"""
SEO Enhancement Rules (S1-S6)
Advanced SEO optimization rules for enhanced analysis.
"""

from ..base_seo_rule import BaseSEORuleV2


class titlePixelLengthRule(BaseSEORuleV2):
    """S1/S2 - Title pixel length analysis for desktop and mobile optimization."""
    rule_id = "title_pixel_length"
    rule_no = 30
    category = "SEO Enhancements"
    severity = "medium"
    description = "Title pixel length affects display and CTR in search results"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        title_pixel_width = normalized.get("title_pixel_width", 0)
        desktop_status = normalized.get("desktop_status", "PASS")
        mobile_status = normalized.get("mobile_status", "PASS")
        
        # Check desktop status (≤600px optimal)
        if desktop_status == "FAIL":
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title too long for desktop ({title_pixel_width}px > 600px) - gets truncated in search results",
                f"Desktop title width: {title_pixel_width}px",
                "Title width ≤ 600px",
                data_key="title_pixel_width",
                data_path="title_pixel_width",
                impact="Titles longer than 600px get truncated in desktop search results, reducing CTR and brand visibility.",
                recommendation="Shorten title to under 600px (~75 characters). Focus on primary keywords and brand name."
            ))
        elif desktop_status == "WARNING":
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title approaching desktop limit ({title_pixel_width}px > 500px) - may get truncated",
                f"Desktop title width: {title_pixel_width}px",
                "Title width ≤ 500px",
                data_key="title_pixel_width",
                data_path="title_pixel_width",
                impact="Titles over 500px risk truncation on some desktop displays, potentially reducing click-through rates.",
                recommendation="Consider shortening title to under 500px (~62 characters) for optimal desktop display."
            ))
        
        # Check mobile status (≤500px optimal)
        if mobile_status == "FAIL":
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title too long for mobile ({title_pixel_width}px > 500px) - gets truncated in mobile search",
                f"Mobile title width: {title_pixel_width}px",
                "Title width ≤ 500px",
                data_key="title_pixel_width",
                data_path="title_pixel_width",
                impact="Mobile titles over 500px get severely truncated, significantly reducing mobile search CTR.",
                recommendation="Shorten title to under 500px (~62 characters) for optimal mobile search display."
            ))
        elif mobile_status == "WARNING":
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Title approaching mobile limit ({title_pixel_width}px > 400px) - may get truncated on mobile",
                f"Mobile title width: {title_pixel_width}px",
                "Title width ≤ 400px",
                data_key="title_pixel_width",
                data_path="title_pixel_width",
                impact="Mobile titles over 400px risk truncation on smaller mobile screens, affecting user experience.",
                recommendation="Aim for under 400px (~50 characters) for best mobile search performance."
            ))
        
        return issues


class metaDescriptionCtrRule(BaseSEORuleV2):
    """S3 - Meta description CTR optimization analysis."""
    rule_id = "meta_description_ctr"
    rule_no = 31
    category = "SEO Enhancements"
    severity = "medium"
    description = "Meta description optimization affects search CTR"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_ctr_score = normalized.get("meta_ctr_score", 0)
        has_power_word = normalized.get("has_power_word", False)
        has_cta = normalized.get("has_cta", False)
        
        # Low CTR score (below 60)
        if meta_ctr_score < 60:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Meta description has low CTR optimization score ({meta_ctr_score}/100) - missing power words or CTAs",
                f"CTR score: {meta_ctr_score}/100",
                "CTR score ≥ 60/100",
                data_key="meta_ctr_score",
                data_path="meta_ctr_score",
                impact="Low CTR optimization reduces search result click-through rates and traffic potential.",
                recommendation="Add power words (free, easy, best, proven) and CTAs (get, try, learn) to meta description."
            ))
        
        # Missing power words
        if not has_power_word:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Meta description lacks power words that increase CTR",
                "Power words: None detected",
                "Include power words like 'free', 'easy', 'best', 'proven'",
                data_key="has_power_word",
                data_path="has_power_word",
                impact="Power words increase emotional response and click-through rates in search results.",
                recommendation="Add relevant power words to meta description to improve CTR and engagement."
            ))
        
        # Missing CTA
        if not has_cta:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Meta description lacks call-to-action words",
                "CTA words: None detected",
                "Include CTA words like 'get', 'try', 'learn', 'discover'",
                data_key="has_cta",
                data_path="has_cta",
                impact="Missing CTAs reduce user engagement and click-through rates from search results.",
                recommendation="Add clear call-to-action words to encourage users to click and engage."
            ))
        
        return issues


class imageAltTextQualityRule(BaseSEORuleV2):
    """S4 - Image alt text quality analysis."""
    rule_id = "image_alt_text_quality"
    rule_no = 32
    category = "SEO Enhancements"
    severity = "medium"
    description = "Image alt text quality affects accessibility and SEO"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        image_analysis = normalized.get("image_analysis", {})
        alt_text_quality_score = image_analysis.get("alt_text_quality_score", 100)
        poor_alt_count = image_analysis.get("poor_alt_count", 0)
        
        # Low alt text quality score
        if alt_text_quality_score < 70:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Low image alt text quality ({alt_text_quality_score}%) - {poor_alt_count} images need better alt text",
                f"Alt text quality: {alt_text_quality_score}%",
                "Alt text quality ≥ 70%",
                data_key="alt_text_quality_score",
                data_path="image_analysis.alt_text_quality_score",
                impact="Poor alt text reduces accessibility, hurts image SEO rankings, and misses keyword opportunities.",
                recommendation="Improve alt text by adding descriptive, keyword-rich descriptions for all images."
            ))
        elif alt_text_quality_score < 85:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Moderate image alt text quality ({alt_text_quality_score}%) - some images could be improved",
                f"Alt text quality: {alt_text_quality_score}%",
                "Alt text quality ≥ 85%",
                data_key="alt_text_quality_score",
                data_path="image_analysis.alt_text_quality_score",
                impact="Moderate alt text quality may limit image search visibility and accessibility compliance.",
                recommendation="Enhance remaining alt text descriptions to be more descriptive and keyword-optimized."
            ))
        
        # High number of poor alt texts
        if poor_alt_count > 5:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"High number of poor alt texts ({poor_alt_count} images) - significant accessibility and SEO impact",
                f"Poor alt count: {poor_alt_count}",
                "Poor alt count ≤ 5",
                data_key="poor_alt_count",
                data_path="image_analysis.poor_alt_count",
                impact="Many poor alt texts severely impact accessibility compliance and image search optimization.",
                recommendation="Prioritize fixing alt text for the most important images, focusing on descriptive, keyword-rich content."
            ))
        
        return issues


class anchorTextOptimizationRule(BaseSEORuleV2):
    """S5 - Internal anchor text optimization analysis."""
    rule_id = "anchor_text_optimization"
    rule_no = 33
    category = "SEO Enhancements"
    severity = "medium"
    description = "Anchor text optimization affects internal linking SEO value"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        content = normalized.get("content", {})
        anchor_analysis = content.get("anchor_analysis", {})
        generic_anchor_ratio = anchor_analysis.get("generic_anchor_ratio", 0)
        keyword_rich_anchor_ratio = anchor_analysis.get("keyword_rich_anchor_ratio", 100)
        
        # High generic anchor ratio
        if generic_anchor_ratio > 30:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"High generic anchor text ratio ({generic_anchor_ratio}%) - missing SEO optimization opportunities",
                f"Generic anchors: {generic_anchor_ratio}%",
                "Generic anchors ≤ 30%",
                data_key="generic_anchor_ratio",
                data_path="content.anchor_analysis.generic_anchor_ratio",
                impact="Generic anchors waste internal link equity and miss opportunities to reinforce topical relevance.",
                recommendation="Replace generic anchors with descriptive, keyword-rich text that describes the target page content."
            ))
        elif generic_anchor_ratio > 20:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Moderate generic anchor text ratio ({generic_anchor_ratio}%) - could be optimized for better SEO",
                f"Generic anchors: {generic_anchor_ratio}%",
                "Generic anchors ≤ 20%",
                data_key="generic_anchor_ratio",
                data_path="content.anchor_analysis.generic_anchor_ratio",
                impact="Moderate generic anchor usage reduces internal linking effectiveness and topical signal strength.",
                recommendation="Improve remaining generic anchors with more descriptive, keyword-focused text."
            ))
        
        # Low keyword-rich anchor ratio
        if keyword_rich_anchor_ratio < 60:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Low keyword-rich anchor ratio ({keyword_rich_anchor_ratio}%) - missing internal linking optimization",
                f"Keyword-rich anchors: {keyword_rich_anchor_ratio}%",
                "Keyword-rich anchors ≥ 60%",
                data_key="keyword_rich_anchor_ratio",
                data_path="content.anchor_analysis.keyword_rich_anchor_ratio",
                impact="Low keyword-rich anchor ratio reduces internal SEO value and topical authority building.",
                recommendation="Increase keyword-rich anchor text to strengthen internal linking and topical relevance signals."
            ))
        
        return issues


class anchorTextDiversityRule(BaseSEORuleV2):
    """S6 - Anchor text diversity analysis."""
    rule_id = "anchor_text_diversity"
    rule_no = 34
    category = "SEO Enhancements"
    severity = "low"
    description = "Anchor text diversity affects natural link profile"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        content = normalized.get("content", {})
        anchor_analysis = content.get("anchor_analysis", {})
        anchor_diversity_issue = anchor_analysis.get("anchor_diversity_issue", False)
        
        # Anchor text diversity issue
        if anchor_diversity_issue:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Low anchor text diversity - same anchor text used excessively (>30% of internal links)",
                "Anchor diversity issue detected",
                "Diverse anchor text usage",
                data_key="anchor_diversity_issue",
                data_path="content.anchor_analysis.anchor_diversity_issue",
                impact="Low anchor text diversity appears unnatural and may reduce SEO effectiveness for internal linking.",
                recommendation="Vary anchor text more naturally. Use different descriptive phrases that target the same keywords."
            ))
        
        return issues


def register_seo_enhancement_rules(registry):
    """Register all SEO enhancement rules with the registry."""
    registry.register(titlePixelLengthRule())
    # DISABLED: Meta description CTR optimization issues
    # registry.register(metaDescriptionCtrRule())
    # DISABLED: Low image alt text quality
    # registry.register(imageAltTextQualityRule())
    registry.register(anchorTextOptimizationRule())
    registry.register(anchorTextDiversityRule())
