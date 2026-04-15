"""
Crawlability SEO Rules
Rules for robots.txt, HTTPS, canonical tags, and search engine access.
"""

from ..base_seo_rule import BaseSEORuleV2


class RobotsTxtBlockingRule(BaseSEORuleV2):
    rule_id = "robots_txt_blocking"
    rule_no = 90
    category = "Crawlability"
    severity = "high"
    description = "Misconfigured robots.txt can accidentally block entire site sections from Google"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check if robots.txt exists and allows this page
        robots_info = normalized.get("robots_txt", {})
        if robots_info.get("blocked", False):
            blocked_by = robots_info.get("blocked_by", "unknown")
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Page blocked by robots.txt: {blocked_by}",
                f"Blocked by: {blocked_by}",
                "Page allowed in robots.txt",
                data_key="robots_txt",
                data_path="robots_txt.blocked"
            ))
        
        return issues


class HttpsNotEnforcedRule(BaseSEORuleV2):
    rule_id = "https_not_enforced"
    rule_no = 91
    category = "Crawlability"
    severity = "high"
    description = "HTTP pages leak ranking signals and trigger browser security warnings"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        if url.startswith("http://"):
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Page served over HTTP (not HTTPS)",
                f"HTTP URL: {url}",
                "HTTPS URL",
                data_key="url",
                data_path="url"
            ))
        
        return issues


class RedirectChainsRule(BaseSEORuleV2):
    rule_id = "redirect_chains_crawlability"
    rule_no = 92
    category = "Crawlability"
    severity = "medium"
    description = "Each redirect hop costs crawl budget and dilutes PageRank"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        redirect_info = normalized.get("redirect_info", {})
        chain_length = redirect_info.get("chain_length", 0)
        
        if chain_length >= 3:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Long redirect chain: {chain_length} hops",
                f"Chain length: {chain_length}",
                "Single-hop redirect (≤ 2 hops)",
                data_key="redirect_info",
                data_path="redirect_info.chain_length"
            ))
        
        return issues


class CanonicalTagErrorsRule(BaseSEORuleV2):
    rule_id = "canonical_tag_errors"
    rule_no = 93
    category = "Crawlability"
    severity = "high"
    description = "Missing or wrong canonicals split indexing signals across duplicate pages"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for canonical tag
        canonical = normalized.get("canonical", "")
        
        if not canonical:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Missing canonical tag",
                "No canonical URL specified",
                "Self-referencing canonical tag",
                data_key="canonical",
                data_path="canonical"
            ))
        elif canonical != url:
            # Check if canonical points to a different URL
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Canonical points to different URL: {canonical}",
                f"Canonical: {canonical}, Page URL: {url}",
                f"Canonical should point to: {url}",
                data_key="canonical",
                data_path="canonical"
            ))
        
        return issues


class GooglebotJsRenderingBlockedRule(BaseSEORuleV2):
    rule_id = "googlebot_js_rendering_blocked"
    rule_no = 94
    category = "Crawlability"
    severity = "high"
    description = "JS-rendered content not accessible to Googlebot is effectively invisible"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for resources that might block JS rendering
        robots_info = normalized.get("robots_txt", {})
        disallowed_resources = robots_info.get("disallowed_resources", [])
        
        # Check for common JS/CSS blocking patterns
        js_blocking_patterns = ['/*.js', '/css/', '/scripts/', '/assets/']
        css_blocking_patterns = ['/*.css', '/css/', '/styles/', '/assets/']
        
        blocked_js = any(pattern in disallowed_resources for pattern in js_blocking_patterns)
        blocked_css = any(pattern in disallowed_resources for pattern in css_blocking_patterns)
        
        if blocked_js or blocked_css:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"JS/CSS resources blocked by robots.txt (JS: {blocked_js}, CSS: {blocked_css})",
                f"Blocked resources: {disallowed_resources}",
                "JS and CSS resources accessible to Googlebot",
                data_key="robots_txt",
                data_path="robots_txt.disallowed_resources"
            ))
        
        return issues


class NoindexOnAdminPagesRule(BaseSEORuleV2):
    rule_id = "noindex_on_admin_pages"
    rule_no = 95
    category = "Crawlability"
    severity = "high"
    description = "Indexable admin pages waste crawl budget and expose internal tools to search"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check if this is an admin/login page
        url_lower = url.lower()
        admin_patterns = ['/admin', '/login', '/dashboard', '/wp-admin', '/wp-login']
        
        is_admin_page = any(pattern in url_lower for pattern in admin_patterns)
        
        if is_admin_page:
            meta_tags = normalized.get("meta_tags", {})
            robots_meta = meta_tags.get("robots", [])
            
            has_noindex = any("noindex" in str(robot).lower() for robot in robots_meta)
            
            if not has_noindex:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Admin page missing noindex tag: {url}",
                    f"Robots meta: {robots_meta}",
                    "noindex tag on admin/login pages",
                    data_key="meta_tags",
                    data_path="meta_tags.robots"
                ))
        
        return issues


def register_crawlability_rules(registry):
    """Register all crawlability rules with the registry."""
    registry.register(RobotsTxtBlockingRule())
    registry.register(HttpsNotEnforcedRule())
    registry.register(RedirectChainsRule())
    registry.register(CanonicalTagErrorsRule())
    registry.register(GooglebotJsRenderingBlockedRule())
    registry.register(NoindexOnAdminPagesRule())
