"""
AI Impact Score Category Rules

Evaluates how well content is optimized for AI understanding and processing.
Focuses on structured data, entity clarity, and AI-friendly content organization.
"""

import json
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class PrimaryOrganizationSchemaRule(BaseRule):
    """Rule 1 — Primary Organization schema on homepage"""
    
    def __init__(self):
        config = {
            "rule_id": "primary_organization_schema",
            "category": "ai_impact",
            "description": "Primary Organization schema on homepage",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": True  # Organization schema is required for businesses
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for primary Organization schema"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Organization schema
        for item in graph:
            if item.get("@type") == "Organization":
                score += 6
                # Check for required properties
                if item.get("name"):
                    score += 2
                if item.get("url"):
                    score += 2
                break
        
        return min(score, self.max_score)

class CorrectTypeRule(BaseRule):
    """Rule 2 — Correct @type (Organization / MarketingAgency)"""
    
    def __init__(self):
        config = {
            "rule_id": "correct_type",
            "category": "ai_impact",
            "description": "Correct @type (Organization / MarketingAgency)",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for correct @type values"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for correct Organization types
        valid_types = ["Organization", "MarketingAgency", "LocalBusiness", "ProfessionalService"]
        for item in graph:
            item_type = item.get("@type", "")
            if item_type in valid_types:
                score += 8
                # Bonus for specific type
                if item_type in ["MarketingAgency", "LocalBusiness"]:
                    score += 2
                break
        
        return min(score, self.max_score)

class SchemaValidJSONLDRule(BaseRule):
    """Rule 3 — Schema valid JSON-LD"""
    
    def __init__(self):
        config = {
            "rule_id": "schema_valid_jsonld",
            "category": "ai_impact",
            "description": "Schema valid JSON-LD",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": True  # Valid schema is required if present
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if schema is valid JSON-LD"""
        structured_data = data.get("structured_data", {})
        
        score = 0
        
        # Check if structured data exists and is valid JSON
        if structured_data:
            if isinstance(structured_data, str):
                try:
                    json.loads(structured_data)
                    score += 10  # Valid JSON
                except (json.JSONDecodeError, TypeError):
                    score += 0  # Invalid JSON
            elif isinstance(structured_data, dict):
                score += 10  # Already parsed dict
        
        return min(score, self.max_score)

class ContextExactlySchemaOrgRule(BaseRule):
    """Rule 4 — @context exactly https://schema.org"""
    
    def __init__(self):
        config = {
            "rule_id": "context_exactly_schema_org",
            "category": "ai_impact",
            "description": "@context exactly https://schema.org",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for exact @context value"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        
        # Check for exact context
        context = structured_data.get("@context", "")
        if context == "https://schema.org":
            score += 10
        elif context in ["http://schema.org", "https://schema.org/"]:
            score += 5  # Close but not exact
        
        return min(score, self.max_score)

class NameFieldMatchesBrandRule(BaseRule):
    """Rule 5 — name field matches brand name"""
    
    def __init__(self):
        config = {
            "rule_id": "name_field_matches_brand",
            "category": "ai_impact",
            "description": "name field matches brand name",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if name field matches brand name"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for name field in Organization
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("name"):
                    score += 10  # Name exists
                break
        
        return min(score, self.max_score)

class URLPointsToCanonicalHomepageRule(BaseRule):
    """Rule 6 — url points to canonical homepage"""
    
    def __init__(self):
        config = {
            "rule_id": "url_points_to_canonical_homepage",
            "category": "ai_impact",
            "description": "url points to canonical homepage",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if URL points to canonical homepage"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for URL field in Organization
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("url"):
                    score += 10  # URL exists
                break
        
        return min(score, self.max_score)

class LogoURLReturns200Rule(BaseRule):
    """Rule 7 — logo URL returns 200 status"""
    
    def __init__(self):
        config = {
            "rule_id": "logo_url_returns_200",
            "category": "ai_impact",
            "description": "logo URL returns 200 status",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if logo URL exists and is accessible"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for logo field
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("logo"):
                    score += 10  # Logo exists
                break
        
        return min(score, self.max_score)

class XMLSitemapExistsValidRule(BaseRule):
    """Rule 14 — XML sitemap exists and valid - DISABLED"""
    
    def __init__(self):
        config = {
            "rule_id": "xml_sitemap_exists_valid",
            "category": "ai_impact",
            "description": "XML sitemap exists and valid - DISABLED",
            "weight": 0.0,  # Disabled - no weight
            "max_score": 10,
            "applies_to": "page",
            "enabled": False  # Explicitly disabled
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Rule disabled - always returns neutral score"""
        return 0.0  # Disabled rule - no impact on score

class RobotsTxtNonBlockingRule(BaseRule):
    """Rule 15 — robots.txt non-blocking"""
    
    def __init__(self):
        config = {
            "rule_id": "robots_txt_non_blocking",
            "category": "ai_impact",
            "description": "robots.txt non-blocking",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if robots.txt is not blocking"""
        # Check the actual extracted signal for robots.txt
        technical_signals = data.get("enhanced_technical_signals", {})
        crawlability = technical_signals.get("crawlability", {})
        
        # Check if robots.txt is accessible (not blocking)
        if not crawlability.get("robots_txt_accessible", False):
            return 0.0  # No score if robots.txt is blocking
        
        # NEW: Check AI crawler access
        blocked_crawlers = crawlability.get("ai_crawlers_blocked", [])
        if blocked_crawlers:
            # CRITICAL: Block score if AI crawlers are blocked
            return 0.0
        
        return 10.0  # Full score only if robots.txt accessible AND AI crawlers allowed

class NoPluginDuplicateSchemasRule(BaseRule):
    """Rule 34 — No plugin duplicate schemas"""
    
    def __init__(self):
        config = {
            "rule_id": "no_plugin_duplicate_schemas",
            "category": "ai_impact",
            "description": "No plugin duplicate schemas",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for duplicate schemas from plugins"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for reasonable number of schemas (not excessive)
        if 1 <= len(graph) <= 10:
            score += 10  # Good schema count
        elif len(graph) <= 15:
            score += 5   # Some duplicates possible
        else:
            score += 0   # Too many schemas (likely duplicates)
        
        return min(score, self.max_score)

class LLMsTxtFileExistsRule(BaseRule):
    """Rule 48 — llms.txt file exists"""
    
    def __init__(self):
        config = {
            "rule_id": "llms_txt_file_exists",
            "category": "ai_impact",
            "description": "llms.txt file exists",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page",
            "is_required": False  # Optional feature, no issue if missing
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for llms.txt file existence"""
        # Check the actual extracted signal
        ai_signals = data.get("ai_visibility_signals", {})
        llms_txt = ai_signals.get("llms_txt", {})
        
        # Return score based on actual llms.txt existence
        if llms_txt.get("exists", False):
            return 10.0  # Full score if llms.txt exists
        else:
            return 0.0  # No score if llms.txt doesn't exist

class SemanticHTMLTagsUsedRule(BaseRule):
    """Rule 49 — Semantic HTML tags used"""
    
    def __init__(self):
        config = {
            "rule_id": "semantic_html_tags_used",
            "category": "ai_impact",
            "description": "Semantic HTML tags used",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for semantic HTML tags usage"""
        heading_metrics = data.get("heading_metrics", {})
        
        score = 0
        
        # Check for proper heading structure
        if heading_metrics.get("heading_sequence_valid"):
            score += 5
        
        # Check for H1 presence
        h1_count = heading_metrics.get("h1_count", 0)
        if h1_count >= 1:
            score += 3
        
        # Check for H2 presence
        h2_count = heading_metrics.get("h2_count", 0)
        if h2_count >= 1:
            score += 2
        
        return min(score, self.max_score)

class BreadcrumbListSchemaRule(BaseRule):
    """Rule 68 — BreadcrumbList schema"""
    
    def __init__(self):
        config = {
            "rule_id": "breadcrumblist_schema",
            "category": "ai_impact",
            "description": "BreadcrumbList schema",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for BreadcrumbList schema"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for BreadcrumbList
        for item in graph:
            if item.get("@type") == "BreadcrumbList":
                score += 10
                break
        
        return min(score, self.max_score)

# Register all AI Impact rules (12 rules - XMLSitemapExistsValidRule removed due to weight=0)
def register_ai_impact_rules(registry):
    """Register all AI Impact category rules"""
    registry.register(PrimaryOrganizationSchemaRule())
    registry.register(CorrectTypeRule())
    registry.register(SchemaValidJSONLDRule())
    registry.register(ContextExactlySchemaOrgRule())
    registry.register(NameFieldMatchesBrandRule())
    registry.register(URLPointsToCanonicalHomepageRule())
    registry.register(LogoURLReturns200Rule())
    # XMLSitemapExistsValidRule removed - had weight=0 and was disabled
    registry.register(RobotsTxtNonBlockingRule())
    registry.register(NoPluginDuplicateSchemasRule())
    registry.register(LLMsTxtFileExistsRule())
    registry.register(SemanticHTMLTagsUsedRule())
    registry.register(BreadcrumbListSchemaRule())
