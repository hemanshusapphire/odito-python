"""
Social Tags SEO Rules
Rules for Open Graph, Twitter Cards, and social media optimization.
"""

from ..base_seo_rule import BaseSEORuleV2


class OgTagsMissingRule(BaseSEORuleV2):
    rule_id = "og_tags_missing"
    rule_no = 71
    category = "Social Tags"
    severity = "low"
    description = "Missing Open Graph tags result in blank previews when pages are shared"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        # Check for core OG tags
        og_tags_found = []
        for tag_name in meta_tags:
            if tag_name.startswith("og:"):
                og_tags_found.append(tag_name)
        
        core_og_tags = ["og:title", "og:description", "og:image", "og:url"]
        missing_og_tags = [tag for tag in core_og_tags if tag not in og_tags_found]
        
        if missing_og_tags:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Missing core OG tags: {', '.join(missing_og_tags)}",
                f"Found OG tags: {og_tags_found}",
                f"All core OG tags: {', '.join(core_og_tags)}",
                data_key="meta_tags",
                data_path="meta_tags.og_tags"
            ))
        
        return issues


class OgTagsIncompleteRule(BaseSEORuleV2):
    rule_id = "og_tags_incomplete"
    rule_no = 72
    category = "Social Tags"
    severity = "low"
    description = "Incomplete OG tags produce poor social card previews"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        core_og_tags = ["og:title", "og:description", "og:image", "og:url"]
        incomplete_tags = []
        
        for tag in core_og_tags:
            tag_values = meta_tags.get(tag, [])
            if not tag_values or not tag_values[0] or not tag_values[0].strip():
                incomplete_tags.append(tag)
        
        if incomplete_tags:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Incomplete OG tags (empty): {', '.join(incomplete_tags)}",
                f"Empty OG tags: {incomplete_tags}",
                "All 4 core OG properties present and non-empty",
                data_key="meta_tags",
                data_path="meta_tags.og_tags.incomplete"
            ))
        
        return issues


class TwitterCardTagsMissingRule(BaseSEORuleV2):
    rule_id = "twitter_card_tags_missing"
    rule_no = 73
    category = "Social Tags"
    severity = "low"
    description = "Missing Twitter Card tags result in plain-link previews on X/Twitter"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        meta_tags = normalized.get("meta_tags", {})
        
        # Check for Twitter Card tags
        twitter_tags_found = []
        for tag_name in meta_tags:
            if tag_name.startswith("twitter:"):
                twitter_tags_found.append(tag_name)
        
        core_twitter_tags = ["twitter:card", "twitter:title", "twitter:description", "twitter:image"]
        missing_twitter_tags = [tag for tag in core_twitter_tags if tag not in twitter_tags_found]
        
        if missing_twitter_tags:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Missing Twitter Card tags: {', '.join(missing_twitter_tags)}",
                f"Found Twitter tags: {twitter_tags_found}",
                f"All Twitter Card tags: {', '.join(core_twitter_tags)}",
                data_key="meta_tags",
                data_path="meta_tags.twitter_tags"
            ))
        
        return issues


def register_social_rules(registry):
    """Register all social tag rules with the registry."""
    registry.register(OgTagsMissingRule())
    registry.register(OgTagsIncompleteRule())
    registry.register(TwitterCardTagsMissingRule())
