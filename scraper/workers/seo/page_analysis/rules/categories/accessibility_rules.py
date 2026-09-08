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
        
        issues = []
        
        # ✅ Use real keyboard navigation data from headless worker
        headless = normalized.get("headless", {})
        keyboard = headless.get("keyboard_analysis", {})
        
        if not keyboard.get("keyboard_navigation_checked", False):
            # If keyboard navigation wasn't checked, skip this rule
            return issues
        
        # Check for focus traps
        if keyboard.get("focus_trap_detected", False):
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Focus trap detected - keyboard users cannot navigate away from certain elements",
                "Keyboard navigation test detected focus trap",
                "All interactive elements must allow keyboard users to navigate away",
                data_key="headless",
                data_path="keyboard_analysis.focus_trap_detected"
            ))
        
        # Check for unreachable elements
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
        
        # Check for missing focus outlines
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
