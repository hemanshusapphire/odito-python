"""
Social SEO Rules (Rules 49–56, 71–73, 128–130, 148)

Open Graph validation, Pinterest tags, and social sharing optimization.
"""

import re
from urllib.parse import urlparse, urljoin
from ..base_seo_rule import BaseSEORuleV2
from ..utils import safe_str
from ..seo_rule_utils import _keyword_from_context
from ..unified_validators import check_social_tags, check_pinterest_tags


# ── Helpers ───────────────────────────────────────────────────

# _keyword_from_context imported from seo_rule_utils


def _is_valid_url(url_str):
    if not url_str:
        return False
    try:
        parsed = urlparse(url_str.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def _resolve_and_normalize_url(url_str, base_url):
    """Resolve relative URL against base URL and normalize."""
    if not url_str or not base_url:
        return url_str
    
    # Skip resolution for obviously invalid URLs
    if not _is_valid_url(url_str) and not url_str.startswith(('/', './', '../')):
        # Invalid URL that's not a relative path - return as-is for validation
        return url_str
    
    # Resolve relative URLs
    resolved = urljoin(base_url, url_str)
    
    # Normalize: lowercase, remove trailing slash
    normalized = resolved.lower().rstrip("/")
    
    return normalized


# ═══════════════════════════════════════════════════════════════
# OPEN GRAPH (Rules 49–56)
# ═══════════════════════════════════════════════════════════════

class OgTitleExistsRule(BaseSEORuleV2):
    rule_id = "OG_TITLE_EXISTS"
    rule_no = 49
    category = "Social"
    severity = "high"
    description = "og:title must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation that checks meta_tags with og: prefix
        social_check = check_social_tags(normalized)
        
        if not social_check['og_tags'].get('og:title'):
            return [self.create_issue(
                job_id, project_id, url,
                "OG title is missing",
                "None",
                '<meta property="og:title" content="Page Title">',
                data_key="meta_tags",
                data_path="og:title"
            )]
        
        return []


class OgDescriptionExistsRule(BaseSEORuleV2):
    rule_id = "OG_DESCRIPTION_EXISTS"
    rule_no = 50
    category = "Social"
    severity = "high"
    description = "og:description must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation that checks meta_tags with og: prefix
        social_check = check_social_tags(normalized)
        
        if not social_check['og_tags'].get('og:description'):
            return [self.create_issue(
                job_id, project_id, url,
                "OG description is missing",
                "None", "Add og:description meta property",
                data_key="meta_tags",
                data_path="og:description"
            )]
        return []


class OgImageExistsRule(BaseSEORuleV2):
    rule_id = "OG_IMAGE_EXISTS"
    rule_no = 51
    category = "Social"
    severity = "high"
    description = "og:image must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation that checks meta_tags with og: prefix
        social_check = check_social_tags(normalized)
        
        if not social_check['og_tags'].get('og:image'):
            return [self.create_issue(
                job_id, project_id, url,
                "OG image is missing",
                "None", "Add og:image meta property",
                data_key="meta_tags",
                data_path="og:image"
            )]
        return []


class OgUrlExistsRule(BaseSEORuleV2):
    rule_id = "OG_URL_EXISTS"
    rule_no = 52
    category = "Social"
    severity = "high"
    description = "og:url must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        from ..seo_rule_utils import _normalize_meta_tags, _get_meta_tag_value
        
        # GUARD: Skip if meta_tags data is not available
        meta_tags = normalized.get("meta_tags")
        if not meta_tags or not isinstance(meta_tags, dict):
            return []
        
        normalized_meta = _normalize_meta_tags(meta_tags)
        og_url = _get_meta_tag_value(normalized_meta, "og:url")
        
        if not og_url:
            return [self.create_issue(
                job_id, project_id, url,
                "OG URL is missing",
                "None", "Add og:url meta property",
                data_key="meta_tags",
                data_path="og:url"
            )]
        return []


class OgTypeExistsRule(BaseSEORuleV2):
    rule_id = "OG_TYPE_EXISTS"
    rule_no = 53
    category = "Social"
    severity = "high"
    description = "og:type must exist"

    def evaluate(self, normalized, job_id, project_id, url):
        from ..seo_rule_utils import _normalize_meta_tags, _get_meta_tag_value
        
        # GUARD: Skip if meta_tags data is not available
        meta_tags = normalized.get("meta_tags")
        if not meta_tags or not isinstance(meta_tags, dict):
            return []
        
        normalized_meta = _normalize_meta_tags(meta_tags)
        og_type = _get_meta_tag_value(normalized_meta, "og:type")
        
        if not og_type:
            return [self.create_issue(
                job_id, project_id, url,
                "OG type is missing",
                "None", "Add og:type (e.g. website, article)",
                data_key="meta_tags",
                data_path="og:type"
            )]
        return []


class OgTitleMatchesPageRule(BaseSEORuleV2):
    rule_id = "OG_TITLE_MATCHES_PAGE"
    rule_no = 54
    category = "Social"
    severity = "medium"
    description = "og:title should match page title"

    def evaluate(self, normalized, job_id, project_id, url):
        og_title = safe_str(normalized.get("og_tags", {}).get("og:title", ""))
        page_title = safe_str(normalized.get("title", ""))
        if og_title and page_title:
            og_words = set(og_title.lower().split())
            title_words = set(page_title.lower().split())
            if og_words and title_words:
                overlap = len(og_words & title_words) / max(len(og_words), len(title_words))
                if overlap < 0.5:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "og:title significantly differs from page title",
                        og_title[:60], "Should closely match page title",
                        data_key="og_tags"
                    )]
        return []


class OgContainsKeywordRule(BaseSEORuleV2):
    rule_id = "OG_CONTAINS_KEYWORD"
    rule_no = 55
    category = "Social"
    severity = "medium"
    description = "OG tags should contain primary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        og = normalized.get("og_tags", {})
        og_text = " ".join([
            safe_str(og.get("og:title", "")),
            safe_str(og.get("og:description", "")),
        ]).lower()
        if keyword.lower() not in og_text:
            return [self.create_issue(
                job_id, project_id, url,
                "OG tags do not contain primary keyword",
                "Not found", f"Include: {keyword}",
                data_key="og_tags"
            )]
        return []


class OgValidUrlsRule(BaseSEORuleV2):
    rule_id = "OG_VALID_URLS"
    rule_no = 56
    category = "Social"
    severity = "high"
    description = "OG URLs should be valid and use HTTPS"

    def evaluate(self, normalized, job_id, project_id, url):
        og = normalized.get("og_tags", {})
        issues_found = []
        for key in ("og:image", "og:url"):
            value = safe_str(og.get(key, ""))
            if value:
                # Resolve relative URLs before validation
                resolved_url = _resolve_and_normalize_url(value, url)
                if not _is_valid_url(resolved_url):
                    issues_found.append(f"{key} invalid URL")
                elif resolved_url.startswith("http://"):
                    issues_found.append(f"{key} uses HTTP")
        if issues_found:
            return [self.create_issue(
                job_id, project_id, url,
                "OG tag URL issues: " + "; ".join(issues_found),
                "; ".join(issues_found), "Use valid HTTPS URLs",
                data_key="og_tags"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# PINTEREST (Rules 71–73, 128–130)
# ═══════════════════════════════════════════════════════════════

class PinterestMediaPresentRule(BaseSEORuleV2):
    rule_id = "PINTEREST_MEDIA_PRESENT"
    rule_no = 71
    category = "Social"
    severity = "info"  # Changed from "info" to emphasize it's optional
    description = "pin:media should be present (optional)"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation for Pinterest tags
        pinterest_check = check_pinterest_tags(normalized)
        
        if not pinterest_check['present']:
            return [self.create_issue(
                job_id, project_id, url,
                "pin:media tag is missing (optional for Pinterest)",
                "None", 
                "Add pin:media for Pinterest sharing (optional)",
                data_key="social", 
                data_path="pinterest"
            )]
        return []


class PinterestDescriptionPresentRule(BaseSEORuleV2):
    rule_id = "PINTEREST_DESCRIPTION_PRESENT"
    rule_no = 72
    category = "Social"
    severity = "info"
    description = "pin:description should be present (optional)"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation for Pinterest tags
        pinterest_check = check_pinterest_tags(normalized)
        
        if not pinterest_check['present']:
            return [self.create_issue(
                job_id, project_id, url,
                "pin:description tag is missing (optional for Pinterest)",
                "None", 
                "Add pin:description for Pinterest sharing (optional)",
                data_key="social", 
                data_path="pinterest"
            )]
        return []


class PinterestUrlMatchRule(BaseSEORuleV2):
    rule_id = "PINTEREST_URL_MATCH"
    rule_no = 73
    category = "Social"
    severity = "medium"
    description = "pin:url should match page URL"

    def evaluate(self, normalized, job_id, project_id, url):
        pinterest = normalized.get("social", {}).get("pinterest", {})
        pin_url = pinterest.get("pin:url") or pinterest.get("url") or ""
        page_url = normalized.get("url", "")
        if pin_url and page_url:
            if pin_url.rstrip("/").lower() != page_url.rstrip("/").lower():
                return [self.create_issue(
                    job_id, project_id, url,
                    "pin:url does not match page URL",
                    pin_url, f"Expected: {page_url}",
                    data_key="social", data_path="pinterest"
                )]
        return []


class PinterestMediaValidUrlRule(BaseSEORuleV2):
    rule_id = "PINTEREST_MEDIA_VALID_URL"
    rule_no = 128
    category = "Social"
    severity = "high"
    description = "pin:media must be a valid image URL"

    def evaluate(self, normalized, job_id, project_id, url):
        pinterest = normalized.get("social", {}).get("pinterest", {})
        media = pinterest.get("pin:media") or pinterest.get("media") or ""
        if media and not _is_valid_url(media):
            return [self.create_issue(
                job_id, project_id, url,
                "pin:media is not a valid URL",
                media[:80], "Use valid HTTPS image URL",
                data_key="social", data_path="pinterest"
            )]
        return []


class PinterestDescKeywordRule(BaseSEORuleV2):
    rule_id = "PINTEREST_DESC_KEYWORD"
    rule_no = 129
    category = "Social"
    severity = "medium"
    description = "pin:description should contain keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        pinterest = normalized.get("social", {}).get("pinterest", {})
        desc = pinterest.get("pin:description") or pinterest.get("description") or ""
        if desc and keyword.lower() not in desc.lower():
            return [self.create_issue(
                job_id, project_id, url,
                "pin:description does not contain keyword",
                desc[:60], f"Include: {keyword}",
                data_key="social", data_path="pinterest"
            )]
        return []


class PinterestTagsValidRule(BaseSEORuleV2):
    rule_id = "PINTEREST_TAGS_VALID"
    rule_no = 130
    category = "Social"
    severity = "high"
    description = "All Pinterest tags should be valid"

    def evaluate(self, normalized, job_id, project_id, url):
        pinterest = normalized.get("social", {}).get("pinterest", {})
        if not pinterest:
            return []
        # Check that any declared Pinterest tag has a non-empty value
        empty_tags = [k for k, v in pinterest.items() if not v]
        if empty_tags:
            return [self.create_issue(
                job_id, project_id, url,
                f"Empty Pinterest tag(s): {', '.join(empty_tags[:3])}",
                ", ".join(empty_tags[:3]), "All Pinterest tags should have values",
                data_key="social", data_path="pinterest"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# SOCIAL SHARING (Rule 148)
# ═══════════════════════════════════════════════════════════════

class SocialSharingOptimizedRule(BaseSEORuleV2):
    rule_id = "SOCIAL_SHARING_OPTIMIZED"
    rule_no = 148
    category = "Social"
    severity = "high"
    description = "Social sharing optimized (OG + Twitter card)"

    def evaluate(self, normalized, job_id, project_id, url):
        og = normalized.get("og_tags", {})
        twitter = normalized.get("social", {}).get("twitter_card", {})
        missing = []
        if not og.get("og:title"):
            missing.append("og:title")
        if not og.get("og:description"):
            missing.append("og:description")
        if not og.get("og:image"):
            missing.append("og:image")
        if not twitter.get("twitter:card") and not twitter.get("card"):
            missing.append("twitter:card")
        if missing:
            return [self.create_issue(
                job_id, project_id, url,
                f"Social sharing not fully optimized — missing: {', '.join(missing)}",
                ", ".join(missing), "Add OG + Twitter Card meta tags",
                data_key="social"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_social_rules(registry):
    """Register all Social rules"""
    # Open Graph (49–56)
    registry.register(OgTitleExistsRule())
    registry.register(OgDescriptionExistsRule())
    registry.register(OgImageExistsRule())
    registry.register(OgUrlExistsRule())
    registry.register(OgTypeExistsRule())
    registry.register(OgTitleMatchesPageRule())
    registry.register(OgContainsKeywordRule())
    registry.register(OgValidUrlsRule())
    # Pinterest (71–73, 128–130)
    registry.register(PinterestMediaPresentRule())
    registry.register(PinterestDescriptionPresentRule())
    registry.register(PinterestUrlMatchRule())
    registry.register(PinterestMediaValidUrlRule())
    registry.register(PinterestDescKeywordRule())
    registry.register(PinterestTagsValidRule())
    # Social sharing (148)
    registry.register(SocialSharingOptimizedRule())
