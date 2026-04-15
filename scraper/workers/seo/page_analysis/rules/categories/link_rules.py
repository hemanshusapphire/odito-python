"""
Links SEO Rules
Rules for internal links, redirects, and link equity flow.
"""

from ..base_seo_rule import BaseSEORuleV2


class LinksToRedirectingUrlsRule(BaseSEORuleV2):
    rule_id = "links_to_redirecting_urls"
    rule_no = 20
    category = "Links"
    severity = "medium"
    description = "Linking to redirects wastes crawl budget and dilutes link equity"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        links = normalized.get("internal_links", [])
        
        for link in links:
            status = link.get("status")
            if status in [301, 302]:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Internal link points to redirecting URL: {link.get('url')} (status: {status})",
                    f"Link to {link.get('url')} redirects with {status}",
                    "Direct link to final destination URL",
                    data_key="internal_links",
                    data_path=f"internal_links.{link.get('url')}"
                ))
        
        return issues


class RelNofollowInternalRule(BaseSEORuleV2):
    rule_id = "rel_nofollow_internal"
    rule_no = 21
    category = "Links"
    severity = "high"
    description = "Internal nofollow restricts crawl paths and blocks PageRank flow"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        links = normalized.get("internal_links", [])
        
        for link in links:
            rel = link.get("rel", [])
            if "nofollow" in rel:
                # Skip login/legal pages as these are acceptable
                url_lower = link.get('url', '').lower()
                if not any(page in url_lower for page in ['login', 'admin', 'legal', 'privacy', 'terms']):
                    issues.append(self.create_issue(
                        job_id, project_id, url,
                        f"Internal link has rel=nofollow: {link.get('url')}",
                        f"Nofollow on internal link: {link.get('url')}",
                        "No nofollow on internal links",
                        data_key="internal_links",
                        data_path=f"internal_links.{link.get('url')}.rel"
                    ))
        
        return issues


class OrphanPagesRule(BaseSEORuleV2):
    rule_id = "orphan_pages"
    rule_no = 22
    category = "Links"
    severity = "high"
    description = "Orphan pages receive no PageRank, are rarely crawled, and rank poorly"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # This is a simplified check - in practice you'd need site-wide data
        # For now, we'll check if the current page has any internal links pointing to it
        # This would require cross-page analysis which is beyond single-page scope
        
        # Skip this rule for now as it requires multi-page analysis
        # issues.append(...)  # Would need site-wide link graph
        
        return issues


class ClickDepthRule(BaseSEORuleV2):
    rule_id = "click_depth"
    rule_no = 23
    category = "Links"
    severity = "medium"
    description = "Pages buried more than 3 clicks from homepage receive little crawl attention"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # This requires site-wide navigation analysis
        # For single-page analysis, we can't determine click depth
        # This rule would need to be implemented at the site level
        
        return issues


def register_link_rules(registry):
    """Register all link rules with the registry."""
    registry.register(LinksToRedirectingUrlsRule())
    registry.register(RelNofollowInternalRule())
    registry.register(OrphanPagesRule())
    registry.register(ClickDepthRule())
