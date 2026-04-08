"""
Crawlability SEO Rules (Rules 146, 150, 152–154, 157–158, 160–165, 167–168, 179–180, 182)

Rules for robots.txt, sitemap, orphan pages, click depth,
breadcrumbs, URL structure, and crawl efficiency.
"""

import re
from urllib.parse import urlparse
from ..base_seo_rule import BaseSEORuleV2
from ..seo_rule_utils import _keyword_from_context


# ═══════════════════════════════════════════════════════════════
# URL STRUCTURE (Rules 146, 150, 152–154, 157–158)
# ═══════════════════════════════════════════════════════════════

class UrlMaxLengthRule(BaseSEORuleV2):
    rule_id = "URL_MAX_LENGTH"
    rule_no = 146
    category = "Crawlability"
    severity = "high"
    description = "URL should be ≤200 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        if page_url and len(page_url) > 200:
            return [self.create_issue(
                job_id, project_id, url,
                f"URL is {len(page_url)} characters (max 200)",
                len(page_url), "≤200 characters",
                data_key="url"
            )]
        return []


class UrlSeoFriendlyRule(BaseSEORuleV2):
    rule_id = "URL_SEO_FRIENDLY"
    rule_no = 150
    category = "Crawlability"
    severity = "high"
    description = "URL should be SEO-friendly (lowercase, hyphens, no special chars)"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        if not page_url:
            return []
        try:
            path = urlparse(page_url).path
        except Exception:
            return []
        issues = []
        if path != path.lower():
            issues.append("mixed case")
        if "_" in path:
            issues.append("underscores")
        if re.search(r'[^a-zA-Z0-9/\-._~]', path):
            issues.append("special characters")
        if issues:
            return [self.create_issue(
                job_id, project_id, url,
                f"URL is not SEO-friendly: {', '.join(issues)}",
                path[:80], "Lowercase, hyphens, no special chars",
                data_key="url"
            )]
        return []


class UrlNoQueryParamsRule(BaseSEORuleV2):
    rule_id = "URL_NO_QUERY_PARAMS"
    rule_no = 152
    category = "Crawlability"
    severity = "high"
    description = "URL should avoid query parameters"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        if page_url and "?" in page_url:
            return [self.create_issue(
                job_id, project_id, url,
                "URL contains query parameters",
                page_url, "Use clean URLs without ?params",
                data_key="url"
            )]
        return []


class UrlContainsKeywordRule(BaseSEORuleV2):
    rule_id = "URL_CONTAINS_KEYWORD"
    rule_no = 153
    category = "Crawlability"
    severity = "medium"
    description = "URL should contain primary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        page_url = normalized.get("url", "").lower()
        # Check if keyword (or hyphenated version) appears in URL
        keyword_slug = keyword.replace(" ", "-")
        if keyword.replace(" ", "") not in page_url and keyword_slug not in page_url:
            return [self.create_issue(
                job_id, project_id, url,
                "URL does not contain primary keyword",
                page_url, f"Include: {keyword_slug}",
                data_key="url"
            )]
        return []


class UrlMaxDepthRule(BaseSEORuleV2):
    rule_id = "URL_MAX_DEPTH"
    rule_no = 154
    category = "Crawlability"
    severity = "high"
    description = "URL depth should be ≤4 levels"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        if not page_url:
            return []
        try:
            path = urlparse(page_url).path
            segments = [s for s in path.split("/") if s]
            if len(segments) > 4:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"URL depth is {len(segments)} levels (max 4)",
                    len(segments), "≤4 directory levels",
                    data_key="url"
                )]
        except Exception:
            pass
        return []


class DoctypePresentRule(BaseSEORuleV2):
    rule_id = "DOCTYPE_PRESENT"
    rule_no = 157
    category = "Crawlability"
    severity = "high"
    description = "DOCTYPE declaration must be present"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("doctype"):
            return [self.create_issue(
                job_id, project_id, url,
                "DOCTYPE declaration is missing",
                "None", "<!DOCTYPE html>",
                data_key="doctype"
            )]
        return []


class ThemeColorPresentRule(BaseSEORuleV2):
    rule_id = "THEME_COLOR_PRESENT"
    rule_no = 158
    category = "Crawlability"
    severity = "low"
    description = "Theme color should be declared"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("theme_color_present"):
            return [self.create_issue(
                job_id, project_id, url,
                "Theme color meta tag is missing",
                "None", '<meta name="theme-color" content="#...">',
                data_key="theme_color_present"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# ROBOTS.TXT + SITEMAP (Rules 160–165)
# ═══════════════════════════════════════════════════════════════

class RobotsTxtExistsRule(BaseSEORuleV2):
    rule_id = "ROBOTS_TXT_EXISTS"
    rule_no = 160
    category = "Crawlability"
    severity = "high"
    description = "robots.txt must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        robots = tech.get("robots_txt", {})
        if isinstance(robots, dict) and not robots.get("exists", True):
            return [self.create_issue(
                job_id, project_id, url,
                "robots.txt file not found",
                "Missing", "Create robots.txt at domain root",
                data_key="technical_report"
            )]
        return []


class RobotsTxtNotBlockImportantRule(BaseSEORuleV2):
    rule_id = "ROBOTS_TXT_NOT_BLOCK_IMPORTANT"
    rule_no = 161
    category = "Crawlability"
    severity = "high"
    description = "robots.txt must not block important pages"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        robots = tech.get("robots_txt", {})
        if isinstance(robots, dict) and robots.get("blocks_important"):
            return [self.create_issue(
                job_id, project_id, url,
                "robots.txt blocks important pages",
                "Blocking important pages", "Review Disallow rules",
                data_key="technical_report"
            )]
        return []


class RobotsTxtSitemapRefRule(BaseSEORuleV2):
    rule_id = "ROBOTS_TXT_SITEMAP_REF"
    rule_no = 162
    category = "Crawlability"
    severity = "high"
    description = "robots.txt should reference sitemap"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        robots = tech.get("robots_txt", {})
        if isinstance(robots, dict) and robots.get("exists"):
            if not robots.get("references_sitemap"):
                return [self.create_issue(
                    job_id, project_id, url,
                    "robots.txt does not reference sitemap",
                    "No Sitemap directive", "Add Sitemap: directive",
                    data_key="technical_report"
                )]
        return []


class SitemapExistsRule(BaseSEORuleV2):
    rule_id = "SITEMAP_EXISTS"
    rule_no = 163
    category = "Crawlability"
    severity = "high"
    description = "XML sitemap must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        sitemap = tech.get("sitemap", {})
        if isinstance(sitemap, dict) and not sitemap.get("exists", True):
            return [self.create_issue(
                job_id, project_id, url,
                "XML sitemap not found",
                "Missing", "Create sitemap.xml",
                data_key="technical_report"
            )]
        return []


class SitemapValidRule(BaseSEORuleV2):
    rule_id = "SITEMAP_VALID"
    rule_no = 164
    category = "Crawlability"
    severity = "high"
    description = "Sitemap should return HTTP 200"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        sitemap = tech.get("sitemap", {})
        if isinstance(sitemap, dict) and sitemap.get("exists"):
            status = sitemap.get("status_code")
            if status and status != 200:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"Sitemap returns HTTP {status}",
                    status, "HTTP 200",
                    data_key="technical_report"
                )]
        return []


class SitemapContainsPageRule(BaseSEORuleV2):
    rule_id = "SITEMAP_CONTAINS_PAGE"
    rule_no = 165
    category = "Crawlability"
    severity = "high"
    description = "Current page should be listed in sitemap"

    def evaluate(self, normalized, job_id, project_id, url):
        tech = normalized.get("technical_report")
        if not tech or not isinstance(tech, dict):
            return []  # External data unavailable
        sitemap = tech.get("sitemap", {})
        if isinstance(sitemap, dict):
            urls = sitemap.get("urls", [])
            if urls:
                page_url = normalized.get("url", "").rstrip("/").lower()
                sitemap_set = {u.rstrip("/").lower() for u in urls}
                if page_url and page_url not in sitemap_set:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Page not found in sitemap",
                        page_url, "Add page to sitemap.xml",
                        data_key="technical_report"
                    )]
        return []


# ═══════════════════════════════════════════════════════════════
# CRAWL GRAPH (Rules 167–168, 179–180, 182)
# ═══════════════════════════════════════════════════════════════

class InternalLinksMinRule(BaseSEORuleV2):
    rule_id = "INTERNAL_LINKS_MIN"
    rule_no = 167
    category = "Crawlability"
    severity = "high"
    description = "Page should have ≥3 internal links"

    def evaluate(self, normalized, job_id, project_id, url):
        graph = normalized.get("crawl_graph")
        if not graph or not isinstance(graph, dict):
            return []
        outbound = graph.get("outboundLinks", 0)
        if outbound < 3:
            return [self.create_issue(
                job_id, project_id, url,
                f"Page has only {outbound} internal outbound links",
                outbound, "≥3 internal links",
                data_key="crawl_graph"
            )]
        return []


class InternalLinksMaxRule(BaseSEORuleV2):
    rule_id = "INTERNAL_LINKS_MAX"
    rule_no = 168
    category = "Crawlability"
    severity = "high"
    description = "Page should not have excessive internal links (≤100)"

    def evaluate(self, normalized, job_id, project_id, url):
        graph = normalized.get("crawl_graph")
        if not graph or not isinstance(graph, dict):
            return []
        outbound = graph.get("outboundLinks", 0)
        if outbound > 100:
            return [self.create_issue(
                job_id, project_id, url,
                f"Page has {outbound} internal links (max 100)",
                outbound, "≤100 internal links",
                data_key="crawl_graph"
            )]
        return []


class ClickDepthMaxRule(BaseSEORuleV2):
    rule_id = "CLICK_DEPTH_MAX"
    rule_no = 179
    category = "Crawlability"
    severity = "high"
    description = "Click depth from homepage should be ≤3"

    def evaluate(self, normalized, job_id, project_id, url):
        graph = normalized.get("crawl_graph")
        if not graph or not isinstance(graph, dict):
            return []
        depth = graph.get("clickDepthFromHomepage")
        if depth is not None and depth > 3:
            return [self.create_issue(
                job_id, project_id, url,
                f"Click depth is {depth} (max 3 from homepage)",
                depth, "≤3 clicks from homepage",
                data_key="crawl_graph"
            )]
        return []


class OrphanPageRule(BaseSEORuleV2):
    rule_id = "ORPHAN_PAGE"
    rule_no = 180
    category = "Crawlability"
    severity = "high"
    description = "Page should not be orphaned (0 inbound links)"

    def evaluate(self, normalized, job_id, project_id, url):
        graph = normalized.get("crawl_graph")
        if not graph or not isinstance(graph, dict):
            return []
        if graph.get("isOrphan"):
            return [self.create_issue(
                job_id, project_id, url,
                "Page is orphaned (no inbound links)",
                0, "Add internal links pointing to this page",
                data_key="crawl_graph"
            )]
        return []


# Rule 182 (BREADCRUMBS_PRESENT) removed — duplicates Rule 191 (SCHEMA_BREADCRUMBLIST) in schema_rules.py


# ── Registration ──────────────────────────────────────────────

def register_crawlability_rules(registry):
    """Register all Crawlability rules"""
    # URL structure (146, 150, 152–154, 157–158)
    registry.register(UrlMaxLengthRule())
    registry.register(UrlSeoFriendlyRule())
    registry.register(UrlNoQueryParamsRule())
    registry.register(UrlContainsKeywordRule())
    registry.register(UrlMaxDepthRule())
    registry.register(DoctypePresentRule())
    registry.register(ThemeColorPresentRule())
    # Robots.txt + Sitemap (160–165)
    registry.register(RobotsTxtExistsRule())
    registry.register(RobotsTxtNotBlockImportantRule())
    registry.register(RobotsTxtSitemapRefRule())
    registry.register(SitemapExistsRule())
    registry.register(SitemapValidRule())
    registry.register(SitemapContainsPageRule())
    # Crawl Graph (167–168, 179–180, 182)
    registry.register(InternalLinksMinRule())
    registry.register(InternalLinksMaxRule())
    registry.register(ClickDepthMaxRule())
    registry.register(OrphanPageRule())
    # BreadcrumbsPresentRule (182) removed — duplicate of SCHEMA_BREADCRUMBLIST (191)
