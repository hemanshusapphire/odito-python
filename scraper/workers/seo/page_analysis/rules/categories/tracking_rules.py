"""
Tracking SEO Rules (Rules 155, 156)

Analytics, tag manager, and tracking detection.
Note: Rules 155–156 are implementable; original rules 134–135 excluded.
"""

from ..base_seo_rule import BaseSEORuleV2


class AnalyticsPresentRule(BaseSEORuleV2):
    rule_id = "ANALYTICS_PRESENT"
    rule_no = 155
    category = "Tracking"
    severity = "high"
    description = "Analytics tracking should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        tracking = normalized.get("tracking", {})
        scripts = normalized.get("scripts", [])
        # Check tracking dict
        has_ga = tracking.get("google_analytics") or tracking.get("ga4")
        has_gtm = tracking.get("google_tag_manager")
        # Check scripts array
        if not has_ga and not has_gtm:
            ga_patterns = ["google-analytics.com", "googletagmanager.com", "gtag/js", "analytics.js"]
            has_ga = any(
                any(p in str(s).lower() for p in ga_patterns)
                for s in scripts
            )
        if not has_ga and not has_gtm:
            return [self.create_issue(
                job_id, project_id, url,
                "Analytics tracking not detected",
                "None", "Add Google Analytics or GTM",
                data_key="tracking"
            )]
        return []


class FacebookPixelPresentRule(BaseSEORuleV2):
    rule_id = "FB_PIXEL_PRESENT"
    rule_no = 156
    category = "Tracking"
    severity = "medium"
    description = "Facebook/Meta Pixel should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        tracking = normalized.get("tracking", {})
        scripts = normalized.get("scripts", [])
        has_fb = tracking.get("facebook_pixel") or tracking.get("meta_pixel")
        if not has_fb:
            fb_patterns = ["facebook.net/en_US/fbevents.js", "fbq(", "connect.facebook.net"]
            has_fb = any(
                any(p in str(s).lower() for p in fb_patterns)
                for s in scripts
            )
        if not has_fb:
            return [self.create_issue(
                job_id, project_id, url,
                "Facebook/Meta Pixel not detected",
                "None", "Add FB pixel for remarketing",
                data_key="tracking"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_tracking_rules(registry):
    """Register all Tracking rules"""
    registry.register(AnalyticsPresentRule())
    registry.register(FacebookPixelPresentRule())
