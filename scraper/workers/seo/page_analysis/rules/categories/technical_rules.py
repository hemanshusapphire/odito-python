"""
Technical SEO Rules (Rules 43–48 + Viewport/URL rules)

Canonical URL validation, viewport, URL structure, DOCTYPE, plus Apple Touch Icons (62–65).
"""

import re
from urllib.parse import urlparse
from ..base_seo_rule import BaseSEORuleV2
from ..unified_validators import check_apple_touch_icon


# ── Helpers ───────────────────────────────────────────────────

def _is_valid_url(url_str):
    if not url_str:
        return False
    try:
        parsed = urlparse(str(url_str).strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# CANONICAL URL (Rules 43–48)
# ═══════════════════════════════════════════════════════════════

class CanonicalPresentRule(BaseSEORuleV2):
    rule_id = "CANONICAL_PRESENT"
    rule_no = 43
    category = "Technical"
    severity = "high"
    description = "Canonical URL must be present"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("canonical"):
            return [self.create_issue(
                job_id, project_id, url,
                "Canonical URL is missing",
                "None", '<link rel="canonical" href="...">',
                data_key="canonical"
            )]
        return []


class CanonicalValidUrlRule(BaseSEORuleV2):
    rule_id = "CANONICAL_VALID_URL"
    rule_no = 44
    category = "Technical"
    severity = "high"
    description = "Canonical must be a valid URL"

    def evaluate(self, normalized, job_id, project_id, url):
        canonical = normalized.get("canonical", "")
        if canonical and not _is_valid_url(canonical):
            return [self.create_issue(
                job_id, project_id, url,
                "Canonical URL is not valid",
                canonical, "Valid absolute URL",
                data_key="canonical"
            )]
        return []


class CanonicalHttpsRule(BaseSEORuleV2):
    rule_id = "CANONICAL_HTTPS"
    rule_no = 45
    category = "Technical"
    severity = "high"
    description = "Canonical URL should use HTTPS"

    def evaluate(self, normalized, job_id, project_id, url):
        canonical = normalized.get("canonical", "")
        if canonical and canonical.startswith("http://"):
            return [self.create_issue(
                job_id, project_id, url,
                "Canonical URL uses HTTP instead of HTTPS",
                canonical, "Use HTTPS for canonical URL",
                data_key="canonical"
            )]
        return []


class CanonicalNoQueryParamsRule(BaseSEORuleV2):
    rule_id = "CANONICAL_NO_QUERY_PARAMS"
    rule_no = 46
    category = "Technical"
    severity = "high"
    description = "Canonical URL should not have query params or fragments"

    def evaluate(self, normalized, job_id, project_id, url):
        canonical = normalized.get("canonical", "")
        if canonical:
            if "?" in canonical or "#" in canonical:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Canonical URL contains query params or fragment",
                    canonical, "Remove ? and # from canonical",
                    data_key="canonical"
                )]
        return []


class CanonicalMatchesPageRule(BaseSEORuleV2):
    rule_id = "CANONICAL_MATCHES_PAGE"
    rule_no = 47
    category = "Technical"
    severity = "high"
    description = "Canonical URL should match page URL"

    def evaluate(self, normalized, job_id, project_id, url):
        canonical = normalized.get("canonical", "")
        page_url = normalized.get("url", "")
        if canonical and page_url:
            canon_clean = canonical.rstrip("/").lower()
            page_clean = page_url.rstrip("/").lower()
            if canon_clean != page_clean:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Canonical URL does not match page URL",
                    canonical, f"Expected: {page_url}",
                    data_key="canonical"
                )]
        return []


class CanonicalConsistentWwwRule(BaseSEORuleV2):
    rule_id = "CANONICAL_CONSISTENT_WWW"
    rule_no = 48
    category = "Technical"
    severity = "medium"
    description = "Canonical URL should be consistent with www/non-www"

    def evaluate(self, normalized, job_id, project_id, url):
        canonical = normalized.get("canonical", "")
        page_url = normalized.get("url", "")
        if canonical and page_url:
            try:
                canon_host = urlparse(canonical).netloc.lower()
                page_host = urlparse(page_url).netloc.lower()
                canon_www = canon_host.startswith("www.")
                page_www = page_host.startswith("www.")
                if canon_www != page_www:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Canonical URL www/non-www mismatch with page URL",
                        f"Canonical: {canon_host}, Page: {page_host}",
                        "Consistent www/non-www",
                        data_key="canonical"
                    )]
            except Exception:
                pass
        return []


# ═══════════════════════════════════════════════════════════════
# APPLE TOUCH ICONS (Rules 62–65)
# ═══════════════════════════════════════════════════════════════

class AppleTouchIconPresentRule(BaseSEORuleV2):
    rule_id = "APPLE_TOUCH_ICON_PRESENT"
    rule_no = 62
    category = "Technical"
    severity = "medium"
    description = "Apple touch icon should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation that checks BOTH meta_tags AND visual_branding
        icon_check = check_apple_touch_icon(normalized)
        
        if not icon_check['present']:
            return [self.create_issue(
                job_id, project_id, url,
                "Apple touch icon is missing",
                "None", 
                '<link rel="apple-touch-icon" href="...">',
                data_key="visual_branding",
                data_path="apple_icons"
            )]
        
        # Icon found - no issue (even if source is visual_branding instead of meta_tags)
        return []


class AppleTouchIconValidUrlRule(BaseSEORuleV2):
    rule_id = "APPLE_TOUCH_ICON_VALID_URL"
    rule_no = 63
    category = "Technical"
    severity = "high"
    description = "Apple touch icon must have valid URL"

    def evaluate(self, normalized, job_id, project_id, url):
        # Use unified validation to get comprehensive icon data
        icon_check = check_apple_touch_icon(normalized)
        
        if not icon_check['present']:
            return []  # Skip URL validation if no icons found
        
        # Check URLs from both sources
        invalid_urls = []
        
        # Check meta_tags URLs - FIXED: Handle list properly
        meta_tags = normalized.get("meta_tags", {})
        apple_touch_icons = meta_tags.get("apple-touch-icon", [])
        
        # FIXED: Ensure we have a list to iterate
        if isinstance(apple_touch_icons, str):
            apple_touch_icons = [apple_touch_icons]
        elif not isinstance(apple_touch_icons, list):
            apple_touch_icons = []
        
        for icon_url in apple_touch_icons:
            if icon_url and not self._is_valid_url(str(icon_url)):
                invalid_urls.append(str(icon_url))
        
        # Check visual_branding URLs
        visual_details = icon_check['details'].get('visual_branding', {})
        for icon in visual_details.get('valid_icons', []):
            if icon.get('href') and not self._is_valid_url(str(icon['href'])):
                invalid_urls.append(str(icon['href']))
        
        if invalid_urls:
            return [self.create_issue(
                job_id, project_id, url,
                f"Apple touch icon has invalid URL(s): {', '.join(invalid_urls[:2])}",
                '; '.join(invalid_urls[:2]), 
                "Use valid HTTPS URLs for apple touch icons",
                data_key="visual_branding",
                data_path="apple_icons"
            )]
        
        return []
    
    def _is_valid_url(self, url_str):
        """Check if URL string is valid."""
        if not url_str:
            return False
        try:
            parsed = urlparse(str(url_str).strip())
            return parsed.scheme in ("http", "https") and bool(parsed.netloc)
        except Exception:
            return False


# DISABLED: Cannot be implemented with current normalized data structure.
# Rule is unregistered. See seo_rule_engine.py.
# To re-enable: scraper must provide raw HTML icon size.
class AppleTouchIconSizeRule(BaseSEORuleV2):
    rule_id = "APPLE_TOUCH_ICON_SIZE"
    rule_no = 64
    category = "Technical"
    severity = "medium"
    description = "Apple touch icon should use recommended sizes"

    def evaluate(self, normalized, job_id, project_id, url):
        # Best-effort: we can only check if icon is present, not its actual size
        # from meta_tags. This is informational.
        return []


class AppleTouchIconMultipleSizesRule(BaseSEORuleV2):
    rule_id = "APPLE_TOUCH_ICON_MULTIPLE_SIZES"
    rule_no = 65
    category = "Technical"
    severity = "medium"
    description = "Multiple icon sizes should be declared"

    def evaluate(self, normalized, job_id, project_id, url):
        meta_tags = normalized.get("meta_tags", {})
        icon_values = meta_tags.get("apple-touch-icon", [])
        if isinstance(icon_values, list) and len(icon_values) == 1:
            return [self.create_issue(
                job_id, project_id, url,
                "Only one apple-touch-icon size declared",
                "1 size", "Declare multiple sizes (180×180, 120×120, etc.)",
                data_key="meta_tags"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# VIEWPORT + URL + DOCTYPE (existing rules, remapped rule_no)
# ═══════════════════════════════════════════════════════════════

class ViewportPresentRule(BaseSEORuleV2):
    rule_id = "VIEWPORT_PRESENT"
    rule_no = 149
    category = "Technical"
    severity = "high"
    description = "Viewport must be present for mobile-friendly page"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("viewport") or "width" not in (normalized.get("viewport") or "").lower():
            return [self.create_issue(
                job_id, project_id, url,
                "Viewport meta tag is missing or incomplete",
                None, '<meta name="viewport" content="width=device-width, initial-scale=1">',
                data_key="viewport"
            )]
        return []


class HttpsEnforcedRule(BaseSEORuleV2):
    rule_id = "HTTPS_ENFORCED"
    rule_no = 178
    category = "Technical"
    severity = "high"
    description = "HTTPS must be enforced site-wide"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        if page_url and page_url.startswith("http://"):
            return [self.create_issue(
                job_id, project_id, url,
                "Page uses HTTP instead of HTTPS",
                page_url, "Enforce HTTPS site-wide",
                data_key="url"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_technical_rules(registry):
    """Register all Technical rules"""
    # Canonical (43–48)
    registry.register(CanonicalPresentRule())
    registry.register(CanonicalValidUrlRule())
    registry.register(CanonicalHttpsRule())
    registry.register(CanonicalNoQueryParamsRule())
    registry.register(CanonicalMatchesPageRule())
    registry.register(CanonicalConsistentWwwRule())
    # Apple Touch Icons (62–65)
    registry.register(AppleTouchIconPresentRule())
    registry.register(AppleTouchIconValidUrlRule())
    # registry.register(AppleTouchIconSizeRule())  # DISABLED: rule_no 64
    registry.register(AppleTouchIconMultipleSizesRule())
    # Viewport + HTTPS
    registry.register(ViewportPresentRule())
    registry.register(HttpsEnforcedRule())
