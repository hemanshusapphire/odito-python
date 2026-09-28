"""
Accessibility SEO Rules
Rules for WCAG compliance, screen reader accessibility, and inclusive design.
"""

import os
from ..base_seo_rule import BaseSEORuleV2
from bson.objectid import ObjectId


# Feature flag check - Defensive guard to prevent rule execution when disabled
def _should_skip_accessibility_rules():
    """Check if accessibility rules should be skipped via feature flag."""
    return os.getenv('DISABLE_ACCESSIBILITY_RULES', '').lower() == 'true'


class FormLabelsRule(BaseSEORuleV2):
    rule_id = "form_labels"
    rule_no = 113
    category = "Accessibility"
    severity = "high"
    description = "Unlabelled form inputs break screen reader accessibility and fail WCAG 1.3.1"

    # axe-core rule IDs that specifically indicate missing / broken form labels
    _FORM_LABEL_AXE_IDS = {
        "label", "label-title-only", "label-content-name-mismatch",
        "form-field-multiple-labels", "aria-input-field-name", "select-name",
    }

    def evaluate(self, normalized, job_id, project_id, url):
        if _should_skip_accessibility_rules():
            return []

        issues = []
        headless = normalized.get("headless", {})
        axe_violations = headless.get("axeViolations", [])

        form_label_violations = [
            v for v in axe_violations
            if v.get("id", "") in self._FORM_LABEL_AXE_IDS
        ]

        for violation in form_label_violations:
            nodes = violation.get("nodes", 0)
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Form input missing label: {violation.get('description', 'Input element lacks an accessible label')}",
                f"{nodes} form element(s) missing accessible labels",
                "Every input, select, and textarea must have an associated <label> or aria-label",
                data_key="headless",
                # Per-violation suffix — see ColorContrastRule for rationale.
                data_path=f"axeViolations.{violation.get('id', 'unknown')}"
            ))

        return issues



# ── Keyboard audit v2 helpers ────────────────────────────────────────────────
# The headless worker (keyboard_audit.py) now stores the EXACT elements, so the
# issue can say which ones failed. These helpers only reshape that stored data
# into the compact, JSON-serializable structures that go onto the issue
# document (`context` for the recommendation/UI, `before_snapshot` for the
# task's before-state). Nothing here re-decides pass/fail.

_MAX_ISSUE_ELEMENTS = 25


def _slim_element(e):
    """Compact per-element record for an issue document (no computed-style dumps)."""
    fi = e.get("focusIndicator") or {}
    out = {
        "tag": e.get("tag"),
        "role": e.get("role"),
        "accessibleName": e.get("accessibleName"),
        "text": e.get("text"),
        "selector": e.get("selector"),
        "selectorUnique": e.get("selectorUnique"),
        "domPath": e.get("domPath"),
        "href": e.get("href"),
        "classes": e.get("classes"),
        "tabindex": e.get("tabindex"),
        "container": e.get("container"),
        "rect": e.get("rect"),
        "disabled": e.get("disabled"),
    }
    if e.get("xpath"):
        out["xpath"] = e["xpath"]
    if fi:
        out["focusIndicator"] = {
            "status": fi.get("status"),
            "reason": fi.get("reason"),
            "signals": fi.get("signals"),
            "before": fi.get("before"),
            "after": fi.get("after"),
            "focusRuleFound": fi.get("focusRuleFound"),
            "focusRules": fi.get("focusRules"),
            "suppressingRule": fi.get("suppressingRule"),
        }
    return {k: v for k, v in out.items() if v is not None}


def _audit_frame(keyboard, url, issue_type):
    """Fields shared by every keyboard finding's context."""
    return {
        "issueType": issue_type,
        "pageUrl": url,
        "auditVersion": keyboard.get("audit_version"),
        "auditMethod": keyboard.get("audit_method"),
        "testedAt": keyboard.get("tested_at"),
        "testedElementCount": keyboard.get("tab_stops_visited"),
        "focusableVisible": keyboard.get("focusable_visible"),
        "traversal": keyboard.get("traversal"),
        "technology": keyboard.get("technology"),
        "cssRulesUnavailable": keyboard.get("css_rules_unavailable"),
    }


class KeyboardAccessibilityRule(BaseSEORuleV2):
    rule_id = "keyboard_accessibility"
    rule_no = 114
    category = "Accessibility"
    severity = "high"
    description = "Keyboard-only users must reach every interactive element without a mouse"

    def evaluate(self, normalized, job_id, project_id, url):
        # Defensive guard - skip if feature flag is enabled
        if _should_skip_accessibility_rules():
            return []

        headless = normalized.get("headless", {})
        keyboard = headless.get("keyboard_analysis", {})

        if not keyboard.get("keyboard_navigation_checked", False):
            # If keyboard navigation wasn't checked, skip this rule
            return []

        if keyboard.get("audit_version", 1) >= 2:
            return self._evaluate_v2(keyboard, job_id, project_id, url)
        return self._evaluate_legacy(keyboard, job_id, project_id, url)

    # ── v2: exact elements, real trap analysis ──────────────────────────────
    def _evaluate_v2(self, keyboard, job_id, project_id, url):
        issues = []
        affected = keyboard.get("affected_elements", {}) or {}

        # Focus trap — only UNINTENDED / improper traps are stored as
        # focus_trap_detected (an intentional, closable modal is not a failure).
        trap = keyboard.get("trap_details", {}) or {}
        if keyboard.get("focus_trap_detected", False):
            container = (trap.get("container") or {}).get("selector")
            cycle = trap.get("cycle") or []
            where = f"in {container}" if container else "on this page"
            n = trap.get("cycleLength") or len(cycle)
            ctx = _audit_frame(keyboard, url, "focus_trap")
            ctx.update({
                "trapDetails": trap,
                "focusSequence": (keyboard.get("focus_sequence") or [])[:30],
                "affectedElementCount": len(cycle) or 1,
                "affectedElements": cycle[:_MAX_ISSUE_ELEMENTS],
            })
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Focus trap detected {where} - keyboard users cannot navigate away from {n} element{'s' if n != 1 else ''}",
                [c.get("selector") for c in cycle if c.get("selector")],
                "Keyboard users must be able to move focus out of every component (a modal may trap focus only while it is open and must be closable)",
                data_key="headless",
                data_path="keyboard_analysis.focus_trap_detected",
                context=ctx,
                before_snapshot={
                    "type": "keyboard_accessibility", "finding": "focus_trap",
                    "verdict": trap.get("verdict"), "container": container,
                    "elements": [{"selector": c.get("selector"), "tag": c.get("tag"), "accessibleName": c.get("accessibleName")} for c in cycle[:_MAX_ISSUE_ELEMENTS]],
                },
            ))

        # Unreachable — visible tabbable elements a completed traversal never focused.
        unreachable = affected.get("unreachable") or []
        if unreachable:
            ctx = _audit_frame(keyboard, url, "unreachable_elements")
            ctx.update({"affectedElementCount": len(unreachable), "affectedElements": unreachable[:_MAX_ISSUE_ELEMENTS]})
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Unreachable interactive elements detected: {len(unreachable)} elements not reachable by keyboard",
                [u.get("selector") for u in unreachable if u.get("selector")],
                "All interactive elements must be reachable via Tab navigation",
                data_key="headless",
                data_path="keyboard_analysis.unreachable_elements",
                context=ctx,
                before_snapshot={
                    "type": "keyboard_accessibility", "finding": "unreachable_elements",
                    "elements": [{"selector": u.get("selector"), "tag": u.get("tag"), "accessibleName": u.get("accessibleName")} for u in unreachable[:_MAX_ISSUE_ELEMENTS]],
                },
            ))

        # Missing focus indicator — judged by a computed-style diff (unfocused vs
        # focused), so outline:none alone is not a failure if another visible
        # indicator (box-shadow, border, background, underline...) is applied.
        missing_total = affected.get("missing_focus_indicator_total", len(affected.get("missing_focus_indicator") or []))
        missing = affected.get("missing_focus_indicator") or []
        if missing_total > 0:
            tested = keyboard.get("tab_stops_visited") or 0
            ctx = _audit_frame(keyboard, url, "missing_focus_indicator")
            ctx.update({
                "affectedElementCount": missing_total,
                "affectedElements": [_slim_element(e) for e in missing[:_MAX_ISSUE_ELEMENTS]],
                "listTruncated": missing_total > min(len(missing), _MAX_ISSUE_ELEMENTS),
            })
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Missing focus indicators: {missing_total} of {tested} keyboard-focusable elements show no visible focus indicator" if tested else f"Missing focus indicators: {missing_total} elements show no visible focus indicator",
                [e.get("selector") for e in missing[:_MAX_ISSUE_ELEMENTS] if e.get("selector")],
                "All interactive elements must have a visible focus indicator (outline, ring, border or equivalent) when focused",
                data_key="headless",
                data_path="keyboard_analysis.missing_focus_outline",
                context=ctx,
                before_snapshot={
                    "type": "keyboard_accessibility", "finding": "missing_focus_indicator", "count": missing_total,
                    "elements": [{"selector": e.get("selector"), "tag": e.get("tag"), "accessibleName": e.get("accessibleName")} for e in missing[:_MAX_ISSUE_ELEMENTS]],
                },
            ))
        return issues

    # ── legacy (audit v1 data, not yet re-scanned) ──────────────────────────
    def _evaluate_legacy(self, keyboard, job_id, project_id, url):
        """v1 data only has counters. The v1 focus-trap heuristic compared
        tag+id and reported a trap for any three consecutive links without an id,
        so it is NOT trusted here; unreachable/missing counters are kept as-is
        until the page is re-scanned with the v2 audit."""
        issues = []

        unreachable_count = keyboard.get("unreachable_elements", 0)
        if unreachable_count > 0:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Unreachable interactive elements detected: {unreachable_count} elements not reachable by keyboard",
                f"Keyboard navigation test found {unreachable_count} unreachable elements",
                "All interactive elements must be reachable via Tab navigation",
                data_key="headless",
                data_path="keyboard_analysis.unreachable_elements"
            ))

        missing_outline_count = keyboard.get("missing_focus_outline", 0)
        if missing_outline_count > 0:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Missing focus indicators: {missing_outline_count} elements lack visible focus outlines",
                f"Keyboard navigation test found {missing_outline_count} elements without focus indicators",
                "All interactive elements must have visible focus indicators when focused",
                data_key="headless",
                data_path="keyboard_analysis.missing_focus_outline"
            ))

        return issues


class FocusIndicatorsRule(BaseSEORuleV2):
    rule_id = "focus_indicators"
    rule_no = 115
    category = "Accessibility"
    severity = "high"
    description = "Removing CSS focus outlines fails WCAG 2.4.11 — WCAG 2.2 criterion"

    def evaluate(self, normalized, job_id, project_id, url):
        # Defensive guard - skip if feature flag is enabled
        if _should_skip_accessibility_rules():
            return []
        
        issues = []

        # Search raw HTML (includes inline styles and <style> blocks) for CSS
        # that globally removes focus outlines. normalized["content"] is plain text —
        # raw_html is the only field that contains CSS source.
        raw_html = normalized.get("raw_html", "") or ""

        focus_removal_patterns = [
            "outline:0",
            "outline:none",
            "outline: 0",
            "outline: none",
            "outline-width:0",
            "outline-width: 0",
        ]

        for pattern in focus_removal_patterns:
            if pattern in raw_html.lower():
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"CSS removes focus indicators: '{pattern}' found in page source",
                    f"Found: {pattern}",
                    "Visible focus ring on keyboard focus for all interactive elements",
                    data_key="raw_html",
                    data_path="raw_html.focus_indicators"
                ))
                break

        return issues


class PageLanguageRule(BaseSEORuleV2):
    rule_id = "page_language"
    rule_no = 116
    category = "Accessibility"
    severity = "medium"
    description = "Missing lang attribute prevents screen readers using correct pronunciation"

    def evaluate(self, normalized, job_id, project_id, url):
        # Defensive guard - skip if feature flag is enabled
        if _should_skip_accessibility_rules():
            return []
        
        issues = []
        
        # Check for html lang attribute from headless data
        headless = normalized.get("headless", {})
        dom_metrics = headless.get("domMetrics", {})
        html_lang = dom_metrics.get("lang", "")
        
        if not html_lang:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "HTML missing lang attribute",
                "No lang attribute on html tag",
                "lang attribute present and correct on every page",
                data_key="headless",
                data_path="domMetrics.lang"
            ))
        elif len(html_lang) < 2:  # Basic validation for ISO 639-1 codes
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Invalid lang attribute: '{html_lang}'",
                f"Lang attribute: '{html_lang}' (too short)",
                "Valid ISO 639-1 language code (e.g., 'en', 'es')",
                data_key="headless",
                data_path="domMetrics.lang"
            ))
        
        return issues


class VideoCaptionsRule(BaseSEORuleV2):
    rule_id = "video_captions"
    rule_no = 117
    category = "Accessibility"
    severity = "medium"
    description = "Videos without captions fail WCAG 1.2.2 and exclude deaf/hard-of-hearing users"

    def evaluate(self, normalized, job_id, project_id, url):
        # Defensive guard - skip if feature flag is enabled
        if _should_skip_accessibility_rules():
            return []
        
        issues = []
        
        # ❌ videos array not available in normalized data
        # ✅ Check axe violations for video-related accessibility issues instead
        headless = normalized.get("headless", {})
        axe_violations = headless.get("axeViolations", [])
        
        # Look for video-related violations in axe results
        video_violations = [v for v in axe_violations if any(
            keyword in v.get("description", "").lower() or 
            keyword in v.get("id", "").lower()
            for keyword in ["video", "caption", "media", "track"]
        )]
        
        for violation in video_violations:
            nodes = violation.get("nodes", 0)
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Video accessibility issue: {violation.get('description', 'Video accessibility problem detected')}",
                f"{nodes} video element(s) missing captions or accessibility attributes",
                "All videos should have captions, tracks, and proper accessibility attributes",
                data_key="headless",
                # Per-violation suffix — see ColorContrastRule for rationale.
                data_path=f"axeViolations.{violation.get('id', 'unknown')}"
            ))
        
        # If no axe violations found but we want to ensure video analysis happened
        # Note: Since we don't have video data in normalized, we can't do detailed analysis
        # This rule now relies entirely on axe-core for video accessibility detection
        
        return issues


class TapTargetSizeRule(BaseSEORuleV2):
    rule_id = "tap_target_size"
    rule_no = 118
    category = "Accessibility"
    severity = "medium"
    description = "WCAG 2.2 criterion 2.5.8 requires minimum 24×24px tap targets — new in 2024"

    def evaluate(self, normalized, job_id, project_id, url):
        # Defensive guard - skip if feature flag is enabled
        if _should_skip_accessibility_rules():
            return []
        
        issues = []
        
        # ✅ Use real tap target data from headless worker keyboard analysis
        headless = normalized.get("headless", {})
        keyboard = headless.get("keyboard_analysis", {})
        
        if not keyboard.get("keyboard_navigation_checked", False):
            # If keyboard navigation wasn't checked, skip this rule
            return issues
        
        # Check for small click targets detected during keyboard navigation
        small_targets_count = keyboard.get("small_click_targets", 0)
        
        if small_targets_count > 0:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Small tap targets detected: {small_targets_count} interactive elements smaller than 24×24px",
                f"Keyboard navigation analysis found {small_targets_count} small click targets",
                "All interactive elements should be at least 24×24 CSS pixels for touch accessibility",
                data_key="headless",
                data_path="keyboard_analysis.small_click_targets"
            ))
        
        # Also check DOM metrics for button count to provide context
        dom_metrics = headless.get("domMetrics", {})
        buttons_count = dom_metrics.get("buttons", 0)
        
        if buttons_count > 0 and small_targets_count == 0:
            # If we have buttons but no small targets detected, still note the analysis
            # This helps users know the rule was checked
            pass  # No issue - this is good!
        
        return issues


def register_accessibility_rules(registry):
    """Register all accessibility rules with the registry."""
    # REMOVED: Images Missing Alt Text (alt_text_accessibility, rule_no 111)
    # REMOVED: Insufficient colour contrast (text_contrast, rule_no 112)
    registry.register(FormLabelsRule())               # 113
    registry.register(KeyboardAccessibilityRule())    # 114
    registry.register(FocusIndicatorsRule())          # 115
    registry.register(PageLanguageRule())             # 116
    registry.register(VideoCaptionsRule())            # 117
    registry.register(TapTargetSizeRule())            # 118
