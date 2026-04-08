"""
Performance SEO Rules (Rules 170–176)

Core Web Vitals, PageSpeed Score, render-blocking resources,
mobile-friendliness and performance optimization rules.
"""

from ..base_seo_rule import BaseSEORuleV2
from ..seo_rule_utils import _get_perf


# _get_perf imported from seo_rule_utils


def _get_best_performance_score(normalized):
    """Get the worst performance score across available devices."""
    mobile_score = _get_perf(normalized, "mobile").get("performance_score")
    desktop_score = _get_perf(normalized, "desktop").get("performance_score")
    
    # Collect available scores
    valid_scores = [s for s in [mobile_score, desktop_score] if s is not None]
    
    if not valid_scores:
        return None, None  # No scores available
    
    # Use worst score (minimum) for stricter evaluation
    worst_score = min(valid_scores)
    
    # Determine which device provided the worst score
    selected_device = "mobile"
    if mobile_score is not None and mobile_score == worst_score:
        selected_device = "mobile"
    elif desktop_score is not None and desktop_score == worst_score:
        selected_device = "desktop"
    
    return worst_score, selected_device


# ═══════════════════════════════════════════════════════════════
# CORE WEB VITALS (Rules 170–176)
# ═══════════════════════════════════════════════════════════════

class LcpGoodRule(BaseSEORuleV2):
    rule_id = "LCP_GOOD"
    rule_no = 170
    category = "Performance"
    severity = "high"
    description = "LCP should be ≤2.5 seconds"

    def evaluate(self, normalized, job_id, project_id, url):
        perf_raw = normalized.get("performance")
        if not perf_raw or not isinstance(perf_raw, dict):
            return []
        perf = _get_perf(normalized)
        lcp = perf.get("largest_contentful_paint")
        if lcp is not None and lcp > 2.5:
            return [self.create_issue(
                job_id, project_id, url,
                f"LCP is {lcp}s (should be ≤2.5s)",
                lcp, "≤2.5 seconds",
                data_key="performance"
            )]
        return []


class ClsGoodRule(BaseSEORuleV2):
    rule_id = "CLS_GOOD"
    rule_no = 171
    category = "Performance"
    severity = "high"
    description = "CLS should be ≤0.1"

    def evaluate(self, normalized, job_id, project_id, url):
        perf_raw = normalized.get("performance")
        if not perf_raw or not isinstance(perf_raw, dict):
            return []
        perf = _get_perf(normalized)
        cls = perf.get("cumulative_layout_shift")
        if cls is not None and cls > 0.1:
            return [self.create_issue(
                job_id, project_id, url,
                f"CLS is {cls} (should be ≤0.1)",
                cls, "≤0.1",
                data_key="performance"
            )]
        return []


class TbtGoodRule(BaseSEORuleV2):
    rule_id = "TBT_GOOD"
    rule_no = 172
    category = "Performance"
    severity = "high"
    description = "TBT should be ≤200ms"

    def evaluate(self, normalized, job_id, project_id, url):
        perf_raw = normalized.get("performance")
        if not perf_raw or not isinstance(perf_raw, dict):
            return []
        perf = _get_perf(normalized)
        tbt = perf.get("total_blocking_time")
        if tbt is not None and tbt > 200:
            return [self.create_issue(
                job_id, project_id, url,
                f"TBT is {tbt}ms (should be ≤200ms)",
                tbt, "≤200ms",
                data_key="performance"
            )]
        return []


class PageSpeedScoreRule(BaseSEORuleV2):
    rule_id = "PAGESPEED_SCORE"
    rule_no = 173
    category = "Performance"
    severity = "high"
    description = "PageSpeed score should be ≥90"

    def evaluate(self, normalized, job_id, project_id, url):
        # Rule disabled - always return no issues
        return []


class SpeedIndexRule(BaseSEORuleV2):
    rule_id = "SPEED_INDEX"
    rule_no = 174
    category = "Performance"
    severity = "high"
    description = "Speed Index should be ≤3.4s"

    def evaluate(self, normalized, job_id, project_id, url):
        perf_raw = normalized.get("performance")
        if not perf_raw or not isinstance(perf_raw, dict):
            return []
        perf = _get_perf(normalized)
        si = perf.get("speed_index")
        if si is not None and si > 3.4:
            return [self.create_issue(
                job_id, project_id, url,
                f"Speed Index is {si}s (should be ≤3.4s)",
                si, "≤3.4 seconds",
                data_key="performance"
            )]
        return []


class RenderBlockingRule(BaseSEORuleV2):
    rule_id = "RENDER_BLOCKING"
    rule_no = 175
    category = "Performance"
    severity = "high"
    description = "Minimize render-blocking resources (≤3)"

    def evaluate(self, normalized, job_id, project_id, url):
        perf_raw = normalized.get("performance")
        if not perf_raw or not isinstance(perf_raw, dict):
            return []
        perf = _get_perf(normalized)
        rb = perf.get("render_blocking_analysis", {})
        if isinstance(rb, dict):
            count = rb.get("total_blocking_count", 0)
            if count > 3:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{count} render-blocking resources detected",
                    count, "≤3 render-blocking resources",
                    data_key="performance"
                )]
        return []


class MobileFriendlyRule(BaseSEORuleV2):
    rule_id = "MOBILE_FRIENDLY"
    rule_no = 176
    category = "Performance"
    severity = "high"
    description = "Page must be mobile-friendly"

    def evaluate(self, normalized, job_id, project_id, url):
        viewport = normalized.get("viewport", "")
        if not viewport or "width=device-width" not in viewport.lower().replace(" ", ""):
            return [self.create_issue(
                job_id, project_id, url,
                "Page may not be mobile-friendly (no viewport)",
                viewport or "None", "width=device-width, initial-scale=1",
                data_key="viewport"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_performance_rules(registry):
    """Register all Performance rules"""
    registry.register(LcpGoodRule())
    registry.register(ClsGoodRule())
    registry.register(TbtGoodRule())
    registry.register(PageSpeedScoreRule())
    registry.register(SpeedIndexRule())
    registry.register(RenderBlockingRule())
    registry.register(MobileFriendlyRule())
