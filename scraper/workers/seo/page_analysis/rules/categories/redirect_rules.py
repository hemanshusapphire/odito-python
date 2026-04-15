"""
Redirects SEO Rules
Rules for redirect chains, meta refresh, and redirect loops.
"""

from ..base_seo_rule import BaseSEORuleV2


class RedirectChainsRule(BaseSEORuleV2):
    rule_id = "redirect_chains"
    rule_no = 24
    category = "Redirects"
    severity = "medium"
    description = "Redirect chains slow load time and leak link equity at each hop"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check if this page is part of a redirect chain
        redirect_info = normalized.get("redirect_info", {})
        chain_length = redirect_info.get("chain_length", 0)
        
        if chain_length > 1:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Redirect chain detected: {chain_length} hops",
                f"Chain length: {chain_length}",
                "Single-hop redirect (chain length: 1)",
                data_key="redirect_info",
                data_path="redirect_info.chain_length"
            ))
        
        return issues


class MetaRefreshRule(BaseSEORuleV2):
    rule_id = "meta_refresh"
    rule_no = 25
    category = "Redirects"
    severity = "high"
    description = "Meta refresh bypasses browser redirect handling — confusing for bots"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        meta_tags = normalized.get("meta_tags", {})
        
        # Check for meta refresh tags
        for tag_name, tag_content in meta_tags.items():
            if tag_name.lower() == "http-equiv" and tag_content:
                for content in tag_content:
                    if "refresh" in str(content).lower() and "url=" in str(content).lower():
                        issues.append(self.create_issue(
                            job_id, project_id, url,
                            f"Meta refresh redirect found: {content}",
                            f"Meta refresh: {content}",
                            "No meta refresh redirects",
                            data_key="meta_tags",
                            data_path="meta_tags.http-equiv"
                        ))
        
        return issues


class RedirectLoopRule(BaseSEORuleV2):
    rule_id = "redirect_loop"
    rule_no = 26
    category = "Redirects"
    severity = "high"
    description = "Redirect loops result in complete page inaccessibility for bots and users"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        redirect_info = normalized.get("redirect_info", {})
        has_loop = redirect_info.get("loop", False)
        
        if has_loop:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Redirect loop detected",
                "Page redirects in a loop",
                "No redirect loops",
                data_key="redirect_info",
                data_path="redirect_info.loop"
            ))
        
        return issues


def register_redirect_rules(registry):
    """Register all redirect rules with the registry."""
    registry.register(RedirectChainsRule())
    registry.register(MetaRefreshRule())
    registry.register(RedirectLoopRule())
