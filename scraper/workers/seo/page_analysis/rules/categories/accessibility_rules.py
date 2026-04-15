"""
Accessibility SEO Rules
Rules for WCAG compliance, screen reader accessibility, and inclusive design.
"""

from ..base_seo_rule import BaseSEORuleV2
from bson.objectid import ObjectId
from datetime import datetime


class FormInputsLabelsRule(BaseSEORuleV2):
    rule_id = "form_inputs_labels"
    rule_no = 111  # Updated rule number since we removed the previous rule
    category = "Accessibility"
    severity = "high"
    description = "Unlabelled form inputs break screen reader accessibility and fail WCAG 1.3.1"


class TextContrastRule(BaseSEORuleV2):
    rule_id = "text_contrast"
    rule_no = 112
    category = "Accessibility"
    severity = "high"
    description = "Low contrast fails WCAG 2.2 Level AA — Google and users with visual impairments penalise this"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # This requires CSS analysis which may not be available in normalized data
        # Simplified implementation would check for obvious contrast issues
        
        # For now, we'll skip this rule as it requires detailed CSS parsing
        # In a full implementation, you'd extract colors from CSS and calculate contrast ratios
        
        return issues


class FormInputsLabelsRule(BaseSEORuleV2):
    rule_id = "form_inputs_labels"
    rule_no = 113
    category = "Accessibility"
    severity = "high"
    description = "Unlabelled form inputs break screen reader accessibility and fail WCAG 1.3.1"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # ✅ Use headless data instead of missing forms array
        headless = normalized.get("headless", {})
        dom_metrics = headless.get("domMetrics", {})
        
        forms_count = dom_metrics.get("forms", 0)
        inputs_count = dom_metrics.get("inputs", 0)
        
        # If we have forms but no detailed input analysis, create a general issue
        if forms_count > 0 and inputs_count > 0:
            # Check if axe violations contain form-related issues
            axe_violations = headless.get("axeViolations", [])
            form_violations = [v for v in axe_violations if any(tag in v.get("tags", []) for tag in ["wcag2aa", "forms", "labels"])]
            
            if form_violations:
                for violation in form_violations:
                    if "label" in violation.get("description", "").lower() or "form" in violation.get("description", "").lower():
                        issues.append(self.create_issue(
                            job_id, project_id, url,
                            f"Form accessibility issue: {violation.get('description', 'Unknown form issue')}",
                            f"WCAG violation detected by axe-core: {violation.get('id', 'unknown')}",
                            "All form inputs must have proper labels and accessibility attributes",
                            data_key="headless",
                            data_path="axeViolations"
                        ))
            else:
                # If no specific violations found but forms exist, flag for manual review
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Forms detected ({forms_count} forms, {inputs_count} inputs) - manual accessibility review recommended",
                    f"Forms present but detailed input analysis not available",
                    "All form inputs should be manually checked for proper labels and accessibility",
                    data_key="headless",
                    data_path="domMetrics"
                ))
        
        return issues


class KeyboardAccessibilityRule(BaseSEORuleV2):
    rule_id = "keyboard_accessibility"
    rule_no = 113  # Updated from 114
    category = "Accessibility"
    severity = "high"
    description = "Keyboard-only users must reach every interactive element without a mouse"

    def evaluate(self, normalized, job_id, project_id, url):
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
    rule_no = 114  # Updated from 115
    category = "Accessibility"
    severity = "high"
    description = "Removing CSS focus outlines fails WCAG 2.4.11 — WCAG 2.2 criterion"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for CSS that removes focus indicators
        content = normalized.get("content", "")
        
        # Look for common focus outline removal patterns
        focus_removal_patterns = [
            "outline:0",
            "outline:none",
            "outline: 0",
            "outline: none",
            "outline-width:0",
            "outline-width: 0"
        ]
        
        for pattern in focus_removal_patterns:
            if pattern in content.lower():
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"CSS removes focus indicators: {pattern}",
                    f"Found: {pattern}",
                    "Visible focus ring on keyboard focus for all interactive elements",
                    data_key="content",
                    data_path="content.focus_indicators"
                ))
                break
        
        return issues


class PageLanguageRule(BaseSEORuleV2):
    rule_id = "page_language"
    rule_no = 115  # Updated from 116
    category = "Accessibility"
    severity = "high"
    description = "Missing lang attribute prevents screen readers using correct pronunciation"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for html lang attribute
        html_lang = normalized.get("html_lang", "")
        
        if not html_lang:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "HTML missing lang attribute",
                "No lang attribute on html tag",
                "lang attribute present and correct on every page",
                data_key="html_lang",
                data_path="html_lang"
            ))
        elif len(html_lang) < 2:  # Basic validation for ISO 639-1 codes
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Invalid lang attribute: '{html_lang}'",
                f"Lang attribute: '{html_lang}' (too short)",
                "Valid ISO 639-1 language code (e.g., 'en', 'es')",
                data_key="html_lang",
                data_path="html_lang"
            ))
        
        return issues


class VideoCaptionsRule(BaseSEORuleV2):
    rule_id = "video_captions"
    rule_no = 116  # Updated from 117
    category = "Accessibility"
    severity = "medium"
    description = "Videos without captions fail WCAG 1.2.2 and exclude deaf/hard-of-hearing users"

    def evaluate(self, normalized, job_id, project_id, url):
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
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Video accessibility issue: {violation.get('description', 'Video accessibility problem detected')}",
                f"axe-core detected: {violation.get('id', 'video-violation')}",
                "All videos should have captions, tracks, and proper accessibility attributes",
                data_key="headless",
                data_path="axeViolations"
            ))
        
        # If no axe violations found but we want to ensure video analysis happened
        # Note: Since we don't have video data in normalized, we can't do detailed analysis
        # This rule now relies entirely on axe-core for video accessibility detection
        
        return issues


class TapTargetSizeRule(BaseSEORuleV2):
    rule_id = "tap_target_size"
    rule_no = 117  # Updated from 118
    category = "Accessibility"
    severity = "medium"
    description = "WCAG 2.2 criterion 2.5.8 requires minimum 24×24px tap targets — new in 2024"

    def evaluate(self, normalized, job_id, project_id, url):
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


class AxeViolationsRule(BaseSEORuleV2):
    rule_id = "axe_violations"
    rule_no = 118  # New rule number
    category = "Accessibility"
    severity = "high"
    description = "Professional accessibility violations detected by axe-core testing"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # 🚀 GAME CHANGER: Use real axe-core violations
        headless = normalized.get("headless", {})
        axe_violations = headless.get("axeViolations", [])
        
        for violation in axe_violations:
            # Map axe impact to our severity levels
            impact = violation.get("impact", "moderate")
            severity_mapping = {
                "critical": "high",
                "serious": "high", 
                "moderate": "medium",
                "minor": "low"
            }
            rule_severity = severity_mapping.get(impact, "medium")
            
            # Create issue for each axe violation
            issues.append({
                "projectId": ObjectId(project_id),
                "seo_jobId": ObjectId(job_id),
                "page_url": url,
                "rule_no": self.rule_no,
                "category": self.category,
                "severity": rule_severity,
                "issue_code": f"axe_{violation.get('id', 'unknown')}",
                "rule_id": self.rule_id,
                "issue_message": f"axe-core: {violation.get('description', 'Accessibility issue detected')}",
                "detected_value": f"Impact: {impact}, Nodes affected: {violation.get('nodes', 0)}",
                "expected_value": "No WCAG violations",
                "data_key": "headless",
                "data_path": "axeViolations",
                "created_at": datetime.utcnow(),
                "axe_data": {  # Include original axe data for reference
                    "axe_id": violation.get("id"),
                    "help_url": violation.get("helpUrl"),
                    "impact": impact,
                    "tags": violation.get("tags", [])
                }
            })
        
        return issues


def register_accessibility_rules(registry):
    """Register all accessibility rules with the registry."""
    registry.register(TextContrastRule())
    registry.register(FormInputsLabelsRule())
    registry.register(KeyboardAccessibilityRule())
    registry.register(FocusIndicatorsRule())
    registry.register(PageLanguageRule())
    registry.register(VideoCaptionsRule())
    registry.register(TapTargetSizeRule())
    registry.register(AxeViolationsRule())  # 🚀 NEW: Process real axe violations
