"""
Shared Issue Normalizer
========================

NORMALIZES ALL ISSUE SOURCES INTO ONE UNIVERSAL STRUCTURE.

This is the SINGLE SOURCE OF TRUTH for issue intelligence.
Both AI Recommendation Engine and DIY Guide Renderer consume this structure.

Supported Sources:
- seo_ai_visibility_issues (AI visibility rules)
- seo_page_issues (On-page SEO issues)
- seo_headless_data (Accessibility issues)
- Technical issues (from various sources)
- Future issue systems

Target Normalized Structure:
{
  id,
  source,
  ruleId,
  issueCode,
  category,
  severity,
  title,
  description,
  recommendation,
  evidence,
  detectedValue,
  expectedValue,

  context: {
    pageType,
    framework,
    cms,
    schemas,
    wordCount
  },

  metadata: {
    issueType,
    implementationType,
    fixPriority,
    confidence
  }
}
"""

from typing import Dict, List, Any, Optional
from enum import Enum


class IssueSource(Enum):
    """Issue source types."""
    AI_VISIBILITY = "ai_visibility"
    ON_PAGE_SEO = "on_page_seo"
    ACCESSIBILITY = "accessibility"
    TECHNICAL = "technical"
    PERFORMANCE = "performance"
    SECURITY = "security"


class PageType(Enum):
    """Canonical page types."""
    HOMEPAGE = "Homepage"
    ARTICLE = "Article"
    PRODUCT = "Product"
    SERVICE = "Service"
    COLLECTION = "Collection"
    FAQ = "FAQ"
    ABOUT = "About"
    CONTACT = "Contact"
    LISTING = "Listing"
    GENERIC = "Generic"


class Framework(Enum):
    """Supported frameworks."""
    NEXTJS = "nextjs"
    WORDPRESS = "wordpress"
    SHOPIFY = "shopify"
    REACT = "react"
    NUXT = "nuxtjs"
    GATSBY = "gatsby"
    WEBFLOW = "webflow"
    WIX = "wix"
    GENERIC = "generic"


class IssueNormalizer:
    """Normalizes issues from all sources into universal structure."""

    def __init__(self):
        self.canonical_page_types = [t.value for t in PageType]
        self.canonical_frameworks = [f.value for f in Framework]

    def normalize_issue(
        self,
        raw_issue: Dict[str, Any],
        source: IssueSource,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Normalize a single issue from any source into universal structure.

        Args:
            raw_issue: Raw issue data from source collection
            source: IssueSource enum
            context: Optional context dict (pageType, framework, etc.)

        Returns:
            Normalized issue dict with universal structure
        """
        if source == IssueSource.AI_VISIBILITY:
            return self._normalize_ai_visibility_issue(raw_issue, context)
        elif source == IssueSource.ON_PAGE_SEO:
            return self._normalize_on_page_issue(raw_issue, context)
        elif source == IssueSource.ACCESSIBILITY:
            return self._normalize_accessibility_issue(raw_issue, context)
        elif source == IssueSource.TECHNICAL:
            return self._normalize_technical_issue(raw_issue, context)
        else:
            return self._normalize_generic_issue(raw_issue, source, context)

    def normalize_batch(
        self,
        raw_issues: List[Dict[str, Any]],
        source: IssueSource,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Normalize a batch of issues."""
        return [
            self.normalize_issue(issue, source, context)
            for issue in raw_issues
        ]

    def _normalize_ai_visibility_issue(
        self,
        raw: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Normalize AI visibility issue from seo_ai_visibility_issues."""
        return {
            "id": raw.get("_id"),
            "source": IssueSource.AI_VISIBILITY.value,
            "ruleId": raw.get("rule_id"),
            "issueCode": raw.get("rule_id"),  # AI visibility uses rule_id as code
            "category": raw.get("category"),
            "severity": raw.get("severity"),
            "title": raw.get("issue") or raw.get("title"),
            "description": raw.get("issue_message") or raw.get("description"),
            "recommendation": raw.get("recommendation"),
            "evidence": raw.get("evidence"),
            "detectedValue": raw.get("detected_value"),
            "expectedValue": raw.get("expected_value"),
            "pageUrl": raw.get("page_url"),
            "context": self._build_context(raw, context),
            "metadata": self._build_metadata(raw, IssueSource.AI_VISIBILITY),
        }

    def _normalize_on_page_issue(
        self,
        raw: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Normalize on-page SEO issue from seo_page_issues."""
        return {
            "id": raw.get("_id"),
            "source": IssueSource.ON_PAGE_SEO.value,
            "ruleId": raw.get("rule_no"),
            "issueCode": raw.get("issue_code"),
            "category": raw.get("category"),
            "severity": raw.get("severity"),
            "title": raw.get("issue"),
            "description": raw.get("issue_message"),
            "recommendation": raw.get("recommendation"),
            "evidence": raw.get("evidence"),
            "detectedValue": raw.get("detected_value"),
            "expectedValue": raw.get("expected_value"),
            "pageUrl": raw.get("page_url"),
            "context": self._build_context(raw, context),
            "metadata": self._build_metadata(raw, IssueSource.ON_PAGE_SEO),
        }

    def _normalize_accessibility_issue(
        self,
        raw: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Normalize accessibility issue from seo_headless_data."""
        # Accessibility issues have different structure
        return {
            "id": raw.get("_id"),
            "source": IssueSource.ACCESSIBILITY.value,
            "ruleId": raw.get("rule_id") or raw.get("id"),
            "issueCode": raw.get("issue_code") or raw.get("id"),
            "category": "accessibility",
            "severity": self._map_accessibility_severity(raw.get("impact")),
            "title": raw.get("description") or raw.get("help"),
            "description": raw.get("help") or raw.get("description"),
            "recommendation": raw.get("help"),
            "evidence": {
                "selector": raw.get("selector"),
                "html": raw.get("html"),
                "failureSummary": raw.get("failureSummary"),
            },
            "detectedValue": raw.get("data"),
            "expectedValue": None,
            "pageUrl": raw.get("url"),
            "context": self._build_context(raw, context),
            "metadata": self._build_metadata(raw, IssueSource.ACCESSIBILITY),
        }

    def _normalize_technical_issue(
        self,
        raw: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Normalize technical issue from various sources."""
        return {
            "id": raw.get("_id"),
            "source": IssueSource.TECHNICAL.value,
            "ruleId": raw.get("rule_id") or raw.get("check_id"),
            "issueCode": raw.get("issue_code") or raw.get("check_id"),
            "category": raw.get("category") or "technical",
            "severity": raw.get("severity") or "medium",
            "title": raw.get("title") or raw.get("check_name"),
            "description": raw.get("description") or raw.get("message"),
            "recommendation": raw.get("recommendation"),
            "evidence": raw.get("evidence"),
            "detectedValue": raw.get("detected_value"),
            "expectedValue": raw.get("expected_value"),
            "pageUrl": raw.get("url") or raw.get("page_url"),
            "context": self._build_context(raw, context),
            "metadata": self._build_metadata(raw, IssueSource.TECHNICAL),
        }

    def _normalize_generic_issue(
        self,
        raw: Dict[str, Any],
        source: IssueSource,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Generic normalization for unknown sources."""
        return {
            "id": raw.get("_id"),
            "source": source.value,
            "ruleId": raw.get("rule_id") or raw.get("id"),
            "issueCode": raw.get("issue_code") or raw.get("id"),
            "category": raw.get("category") or "unknown",
            "severity": raw.get("severity") or "medium",
            "title": raw.get("title") or raw.get("name"),
            "description": raw.get("description") or raw.get("message"),
            "recommendation": raw.get("recommendation"),
            "evidence": raw.get("evidence"),
            "detectedValue": raw.get("detected_value"),
            "expectedValue": raw.get("expected_value"),
            "pageUrl": raw.get("url") or raw.get("page_url"),
            "context": self._build_context(raw, context),
            "metadata": self._build_metadata(raw, source),
        }

    def _build_context(
        self,
        raw: Dict[str, Any],
        provided_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build context object from raw data or provided context."""
        # Use provided context if available (from context extractor)
        if provided_context:
            return {
                "pageType": self._normalize_page_type(provided_context.get("pageType")),
                "framework": self._normalize_framework(provided_context.get("framework")),
                "cms": provided_context.get("cms"),
                "schemas": provided_context.get("detectedSchemas", []),
                "wordCount": provided_context.get("wordCount", 0),
            }

        # Extract from raw issue if context not provided
        return {
            "pageType": self._extract_page_type_from_raw(raw),
            "framework": self._extract_framework_from_raw(raw),
            "cms": raw.get("cms"),
            "schemas": raw.get("schemas", []),
            "wordCount": raw.get("wordCount", 0),
        }

    def _build_metadata(
        self,
        raw: Dict[str, Any],
        source: IssueSource
    ) -> Dict[str, Any]:
        """Build metadata object."""
        return {
            "issueType": self._infer_issue_type(raw, source),
            "implementationType": self._infer_implementation_type(raw),
            "fixPriority": self._infer_fix_priority(raw),
            "confidence": raw.get("confidence", 1.0),
        }

    def _normalize_page_type(self, page_type: Optional[str]) -> str:
        """Normalize page type to canonical value."""
        if not page_type:
            return PageType.GENERIC.value

        page_type_lower = page_type.lower()

        # Map to canonical types
        type_mappings = {
            "homepage": PageType.HOMEPAGE.value,
            "home": PageType.HOMEPAGE.value,
            "article": PageType.ARTICLE.value,
            "blogposting": PageType.ARTICLE.value,
            "newsarticle": PageType.ARTICLE.value,
            "blog": PageType.ARTICLE.value,
            "post": PageType.ARTICLE.value,
            "product": PageType.PRODUCT.value,
            "service": PageType.SERVICE.value,
            "collection": PageType.COLLECTION.value,
            "category": PageType.LISTING.value,
            "faq": PageType.FAQ.value,
            "faqpage": PageType.FAQ.value,
            "about": PageType.ABOUT.value,
            "contact": PageType.CONTACT.value,
            "listing": PageType.LISTING.value,
            "webpage": PageType.GENERIC.value,
        }

        return type_mappings.get(page_type_lower, PageType.GENERIC.value)

    def _normalize_framework(self, framework: Optional[str]) -> str:
        """Normalize framework to canonical value."""
        if not framework:
            return Framework.GENERIC.value

        framework_lower = framework.lower()

        # Map to canonical frameworks
        framework_mappings = {
            "nextjs": Framework.NEXTJS.value,
            "next.js": Framework.NEXTJS.value,
            "wordpress": Framework.WORDPRESS.value,
            "shopify": Framework.SHOPIFY.value,
            "react": Framework.REACT.value,
            "nuxtjs": Framework.NUXT.value,
            "nuxt": Framework.NUXT.value,
            "gatsby": Framework.GATSBY.value,
            "webflow": Framework.WEBFLOW.value,
            "wix": Framework.WIX.value,
        }

        return framework_mappings.get(framework_lower, Framework.GENERIC.value)

    def _extract_page_type_from_raw(self, raw: Dict[str, Any]) -> str:
        """Extract page type from raw issue data."""
        # Try common field names
        for field in ["pageType", "page_type", "detected_type"]:
            if field in raw:
                return self._normalize_page_type(raw[field])

        return PageType.GENERIC.value

    def _extract_framework_from_raw(self, raw: Dict[str, Any]) -> str:
        """Extract framework from raw issue data."""
        # Try common field names
        for field in ["framework", "detected_framework"]:
            if field in raw:
                return self._normalize_framework(raw[field])

        return Framework.GENERIC.value

    def _map_accessibility_severity(self, impact: Optional[str]) -> str:
        """Map axe-core impact to severity."""
        if not impact:
            return "medium"

        impact_lower = impact.lower()
        if impact_lower == "critical":
            return "high"
        elif impact_lower == "serious":
            return "high"
        elif impact_lower == "moderate":
            return "medium"
        elif impact_lower == "minor":
            return "low"
        else:
            return "medium"

    def _infer_issue_type(self, raw: Dict[str, Any], source: IssueSource) -> str:
        """Infer issue type from category and source."""
        category = raw.get("category", "").lower()

        type_mappings = {
            "schema": "schema",
            "content": "content",
            "technical": "technical",
            "accessibility": "accessibility",
            "performance": "performance",
            "security": "security",
            "link": "link",
            "multimedia": "multimedia",
        }

        for key, value in type_mappings.items():
            if key in category:
                return value

        # Default based on source
        if source == IssueSource.ACCESSIBILITY:
            return "accessibility"
        elif source == IssueSource.TECHNICAL:
            return "technical"
        else:
            return "general"

    def _infer_implementation_type(self, raw: Dict[str, Any]) -> str:
        """Infer implementation type (code, config, content, etc.)."""
        category = raw.get("category", "").lower()

        if "schema" in category or "json-ld" in category:
            return "schema"
        elif "meta" in category or "title" in category:
            return "metadata"
        elif "content" in category:
            return "content"
        elif "accessibility" in category:
            return "accessibility"
        elif "technical" in category or "performance" in category:
            return "technical"
        else:
            return "general"

    def _infer_fix_priority(self, raw: Dict[str, Any]) -> str:
        """Infer fix priority from severity and other signals."""
        severity = raw.get("severity", "medium")

        if severity in ["high", "critical"]:
            return "immediate"
        elif severity == "medium":
            return "soon"
        else:
            return "eventual"


# Singleton instance
issue_normalizer = IssueNormalizer()
