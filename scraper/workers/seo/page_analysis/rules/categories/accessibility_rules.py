"""
Accessibility SEO Rules (Rules 213–228, 230, 233, 236)

axe-core violation handling, DOM metrics, keyboard navigation,
ARIA landmarks, form labels, and focus indicators.
"""

from ..base_seo_rule import BaseSEORuleV2
from ..seo_rule_utils import _get_axe, _get_dom_metrics, _get_keyboard

# Helpers (_get_axe, _get_dom_metrics, _get_keyboard) imported from seo_rule_utils

def _get_headless(normalized):
    return normalized.get("headless", {})


# ═══════════════════════════════════════════════════════════════
# AXE-CORE VIOLATIONS (Rules 213–216) - FIXED: Deduplicated
# ═══════════════════════════════════════════════════════════════

class AxeNoViolationsRule(BaseSEORuleV2):
    rule_id = "AXE_NO_VIOLATIONS"
    rule_no = 213
    category = "Accessibility"
    severity = "high"
    description = "No axe-core violations found"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        # GUARD: Skip if headless data is not available
        if not headless or not isinstance(headless, dict):
            return []
        
        violations = _get_axe(normalized)
        if violations:
            # FIXED: Create summary issue instead of individual violation issues
            unique_violation_types = set()
            impacts = {"critical": 0, "serious": 0, "moderate": 0, "minor": 0}
            
            for v in violations:
                if isinstance(v, dict):
                    violation_id = v.get("id", "unknown")
                    impact = v.get("impact", "minor")
                    unique_violation_types.add(violation_id)
                    if impact in impacts:
                        impacts[impact] += 1
            
            impact_summary = ", ".join([f"{count} {impact}" for impact, count in impacts.items() if count > 0])
            
            return [self.create_issue(
                job_id, project_id, url,
                f"{len(violations)} accessibility violations found: {len(unique_violation_types)} unique types ({impact_summary})",
                len(violations), "0 axe-core violations",
                data_key="headless"
            )]
        return []


class AxeNoCriticalRule(BaseSEORuleV2):
    rule_id = "AXE_NO_CRITICAL"
    rule_no = 214
    category = "Accessibility"
    severity = "high"
    description = "No critical axe-core violations"

    def evaluate(self, normalized, job_id, project_id, url):
        # FIXED: Skip individual critical violations - handled by summary rule
        return []  # Prevents duplicate counting


class AxeNoSeriousRule(BaseSEORuleV2):
    rule_id = "AXE_NO_SERIOUS"
    rule_no = 215
    category = "Accessibility"
    severity = "high"
    description = "No serious axe-core violations"

    def evaluate(self, normalized, job_id, project_id, url):
        # FIXED: Skip individual serious violations - handled by summary rule
        return []  # Prevents duplicate counting


class AxeMaxModerateRule(BaseSEORuleV2):
    rule_id = "AXE_MAX_MODERATE"
    rule_no = 216
    category = "Accessibility"
    severity = "medium"
    description = "Moderate violations ≤5"

    def evaluate(self, normalized, job_id, project_id, url):
        # FIXED: Skip individual moderate violations - handled by summary rule
        return []  # Prevents duplicate counting


# ═══════════════════════════════════════════════════════════════
# IMAGES ALT TEXT (Rule 217)
# ═══════════════════════════════════════════════════════════════

class ImagesAllHaveAltRule(BaseSEORuleV2):
    rule_id = "IMAGES_ALL_HAVE_ALT"
    rule_no = 217
    category = "Accessibility"
    severity = "high"
    description = "All images must have alt text"

    def evaluate(self, normalized, job_id, project_id, url):
        images = normalized.get("images", [])
        if not images:
            return []
        
        # Get image analysis data if available
        image_analysis = normalized.get("image_analysis", {})
        
        # Calculate actual missing alt text (excluding decorative images)
        missing_count = 0
        for img in images:
            if not isinstance(img, dict):
                continue
            
            alt = img.get("alt", "")
            # Check if image is decorative (role="presentation" or explicitly marked)
            is_decorative = (
                img.get("role") == "presentation" or 
                img.get("is_decorative") is True or
                alt == ""  # Empty alt indicates decorative
            )
            
            # Count as missing only if no alt and not decorative
            if not alt and not is_decorative:
                missing_count += 1
        
        # If we have analysis data, use that for more accurate counting
        if image_analysis and isinstance(image_analysis, dict):
            images_without_alt = image_analysis.get("images_without_alt", 0)
            decorative_images = image_analysis.get("decorative_images", 0)
            # Use analysis data but ensure we don't get negative values
            actual_missing = max(0, images_without_alt - decorative_images)
            missing_count = max(missing_count, actual_missing)
        
        if missing_count > 0:
            return [self.create_issue(
                job_id, project_id, url,
                f"{missing_count} image(s) missing alt text",
                missing_count, "All non-decorative images need alt text",
                data_key="images"
            )]
        
        return []


# ═══════════════════════════════════════════════════════════════
# DOM STRUCTURE (Rules 218–222)
# ═══════════════════════════════════════════════════════════════

class DomElementCountRule(BaseSEORuleV2):
    rule_id = "DOM_ELEMENT_COUNT"
    rule_no = 218
    category = "Accessibility"
    severity = "medium"
    description = "Total DOM elements should be ≤1500"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        dm = _get_dom_metrics(normalized)
        total = dm.get("totalElements", 0)
        if total > 1500:
            return [self.create_issue(
                job_id, project_id, url,
                f"DOM has {total} elements (recommend ≤1500)",
                total, "≤1500 elements",
                data_key="headless"
            )]
        return []


class FormLabelsRule(BaseSEORuleV2):
    rule_id = "FORM_LABELS"
    rule_no = 219
    category = "Accessibility"
    severity = "high"
    description = "Form inputs should have labels"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        dm = _get_dom_metrics(normalized)
        inputs = dm.get("inputs", 0)
        forms = dm.get("forms", 0)
        if inputs > 0 and forms > 0:
            # Check axe for form-related violations
            violations = _get_axe(normalized)
            label_issues = [
                v for v in violations
                if isinstance(v, dict) and v.get("id") in ("label", "input-image-alt", "select-name")
            ]
            if label_issues:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"Form input(s) missing labels ({len(label_issues)} issue(s))",
                    len(label_issues), "All form inputs need associated labels",
                    data_key="headless"
                )]
        return []


class AriaLandmarksRule(BaseSEORuleV2):
    rule_id = "ARIA_LANDMARKS"
    rule_no = 221
    category = "Accessibility"
    severity = "high"
    description = "ARIA landmarks should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        dm = _get_dom_metrics(normalized)
        landmarks = dm.get("ariaLandmarks", 0)
        if landmarks is None:
            return []  # Data unavailable, do not fire issue
        if landmarks == 0:
            return [self.create_issue(
                job_id, project_id, url,
                "No ARIA landmarks found",
                0, "Add role=navigation, main, banner, etc.",
                data_key="headless"
            )]
        return []


class ButtonsHaveLabelsRule(BaseSEORuleV2):
    rule_id = "BUTTONS_HAVE_LABELS"
    rule_no = 222
    category = "Accessibility"
    severity = "high"
    description = "All buttons should have accessible labels"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        violations = _get_axe(normalized)
        button_issues = [
            v for v in violations
            if isinstance(v, dict) and v.get("id") in ("button-name",)
        ]
        if button_issues:
            node_count = sum(
                len(v.get("nodes", [])) if isinstance(v.get("nodes"), list) else 0
                for v in button_issues
            )
            return [self.create_issue(
                job_id, project_id, url,
                "Buttons without accessible labels found",
                node_count,
                "All buttons need accessible names",
                data_key="headless"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# HEADING HIERARCHY (Rule 223)
# ═══════════════════════════════════════════════════════════════

class HeadingOrderLogicalRule(BaseSEORuleV2):
    rule_id = "HEADING_ORDER_LOGICAL_A11Y"
    rule_no = 223
    category = "Accessibility"
    severity = "high"
    description = "Heading order should be logical for accessibility"

    def evaluate(self, normalized, job_id, project_id, url):
        """Only check axe-core accessibility heading violations.
        Structural heading hierarchy is handled by Rule 229 (HeadingHierarchyLogicalRule).
        """
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        violations = _get_axe(normalized)
        heading_issues = [
            v for v in violations
            if isinstance(v, dict) and v.get("id") in ("heading-order", "page-has-heading-one")
        ]
        if heading_issues:
            ids = [v.get("id", "?") for v in heading_issues]
            return [self.create_issue(
                job_id, project_id, url,
                f"Accessibility heading violation(s): {', '.join(ids)}",
                ", ".join(ids), "Fix axe-core heading-order / page-has-heading-one",
                data_key="headless"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# KEYBOARD ACCESSIBILITY (Rules 224–228, 230)
# ═══════════════════════════════════════════════════════════════

class KeyboardNavigationCheckedRule(BaseSEORuleV2):
    rule_id = "KEYBOARD_NAV_CHECKED"
    rule_no = 224
    category = "Accessibility"
    severity = "high"
    description = "Keyboard navigation should be functional"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        kb = _get_keyboard(normalized)
        if kb and not kb.get("keyboard_navigation_checked"):
            return [self.create_issue(
                job_id, project_id, url,
                "Keyboard navigation could not be checked",
                "Not checked", "Ensure keyboard accessibility",
                data_key="headless"
            )]
        return []


class NoFocusTrapRule(BaseSEORuleV2):
    rule_id = "NO_FOCUS_TRAP"
    rule_no = 225
    category = "Accessibility"
    severity = "high"
    description = "No focus traps detected"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        kb = _get_keyboard(normalized)
        if kb and kb.get("focus_trap_detected"):
            return [self.create_issue(
                job_id, project_id, url,
                "Focus trap detected during keyboard navigation",
                "Trap detected", "Fix focus trapping elements",
                data_key="headless"
            )]
        return []


class SmallClickTargetsRule(BaseSEORuleV2):
    rule_id = "SMALL_CLICK_TARGETS"
    rule_no = 226
    category = "Accessibility"
    severity = "high"
    description = "No small click targets (<24px)"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        kb = _get_keyboard(normalized)
        if kb:
            small = kb.get("small_click_targets", 0)
            if small > 0:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{small} small click target(s) detected (<24px)",
                    small, "All interactive elements ≥24×24px",
                    data_key="headless"
                )]
        return []


class FocusIndicatorRule(BaseSEORuleV2):
    rule_id = "FOCUS_INDICATOR"
    rule_no = 227
    category = "Accessibility"
    severity = "high"
    description = "Focus indicators should be visible"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        kb = _get_keyboard(normalized)
        if kb:
            missing = kb.get("missing_focus_outline", 0)
            if missing > 0:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{missing} element(s) with missing focus outline",
                    missing, "All focusable elements need visible outline",
                    data_key="headless"
                )]
        return []


class UnreachableElementsRule(BaseSEORuleV2):
    rule_id = "UNREACHABLE_ELEMENTS"
    rule_no = 228
    category = "Accessibility"
    severity = "high"
    description = "All interactive elements should be keyboard-reachable"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        kb = _get_keyboard(normalized)
        if kb:
            unreachable = kb.get("unreachable_elements", 0)
            total_tabs = kb.get("total_tab_presses", 20)
            if unreachable > total_tabs * 0.5:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{unreachable} of {total_tabs} tab presses reached no element",
                    unreachable, "All elements should be keyboard-reachable",
                    data_key="headless"
                )]
        return []


class SkipNavigationRule(BaseSEORuleV2):
    rule_id = "SKIP_NAVIGATION"
    rule_no = 230
    category = "Accessibility"
    severity = "high"
    description = "Skip navigation link should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        violations = _get_axe(normalized)
        skip_issues = [
            v for v in violations
            if isinstance(v, dict) and v.get("id") in ("bypass", "skip-link")
        ]
        if skip_issues:
            return [self.create_issue(
                job_id, project_id, url,
                "Skip navigation link missing or broken",
                "Missing", "Add skip-to-content link",
                data_key="headless"
            )]
        return []


class HtmlLangA11yRule(BaseSEORuleV2):
    rule_id = "HTML_LANG_A11Y"
    rule_no = 233
    category = "Accessibility"
    severity = "high"
    description = "HTML lang attribute must be present for screen readers"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("html_lang"):
            return [self.create_issue(
                job_id, project_id, url,
                "HTML lang attribute is missing",
                "None", 'Add lang="en" to <html>',
                data_key="html_lang"
            )]
        return []


class LinksHaveDescriptiveTextRule(BaseSEORuleV2):
    rule_id = "LINKS_DESCRIPTIVE_TEXT"
    rule_no = 236
    category = "Accessibility"
    severity = "high"
    description = "Links should have descriptive text"

    def evaluate(self, normalized, job_id, project_id, url):
        headless = normalized.get("headless")
        if not headless or not isinstance(headless, dict):
            return []
        violations = _get_axe(normalized)
        link_issues = [
            v for v in violations
            if isinstance(v, dict) and v.get("id") in ("link-name",)
        ]
        if link_issues:
            node_count = sum(
                len(v.get("nodes", [])) if isinstance(v.get("nodes"), list) else 0
                for v in link_issues
            )
            return [self.create_issue(
                job_id, project_id, url,
                "Links without descriptive text found",
                node_count,
                "All links need descriptive text (not 'click here')",
                data_key="headless"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_accessibility_rules(registry):
    """Register all Accessibility rules"""
    # Axe (213–216)
    registry.register(AxeNoViolationsRule())
    registry.register(AxeNoCriticalRule())
    registry.register(AxeNoSeriousRule())
    registry.register(AxeMaxModerateRule())
    # Images (217)
    registry.register(ImagesAllHaveAltRule())
    # DOM (218–223)
    registry.register(DomElementCountRule())
    registry.register(FormLabelsRule())
    registry.register(AriaLandmarksRule())
    registry.register(ButtonsHaveLabelsRule())
    registry.register(HeadingOrderLogicalRule())
    # Keyboard (224–228, 230)
    registry.register(KeyboardNavigationCheckedRule())
    registry.register(NoFocusTrapRule())
    registry.register(SmallClickTargetsRule())
    registry.register(FocusIndicatorRule())
    registry.register(UnreachableElementsRule())
    registry.register(SkipNavigationRule())
    # Misc A11y (233, 236)
    registry.register(HtmlLangA11yRule())
    registry.register(LinksHaveDescriptiveTextRule())
