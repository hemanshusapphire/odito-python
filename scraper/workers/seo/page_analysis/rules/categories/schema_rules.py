"""
Schema / Structured Data SEO Rules
Rules for JSON-LD schema markup validation and optimization.
"""

from ..base_seo_rule import BaseSEORuleV2
import sys
import os
from urllib.parse import urlparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .eeat_rules import ContextValidator


class OrganizationSchemaRule(BaseSEORuleV2):
    rule_id = "organization_schema"
    rule_no = 96
    category = "Schema"
    severity = "high"
    description = "Primary entity schema is required for AI and Knowledge Graph to identify the brand"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check multiple sources for structured data
        structured_data = normalized.get("structured_data", [])
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )
        
        has_organization_schema = False
        org_schema = None
        
        for schema in structured_data:
            schema_type = schema.get("@type")
            if isinstance(schema_type, list):
                if "Organization" in schema_type or "LocalBusiness" in schema_type:
                    has_organization_schema = True
                    org_schema = schema
                    break
            elif schema_type in ["Organization", "LocalBusiness"]:
                has_organization_schema = True
                org_schema = schema
                break
        
        if not has_organization_schema:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Missing Organization or LocalBusiness schema",
                "No entity schema found",
                "Organization or LocalBusiness JSON-LD with name, url, logo, sameAs, address",
                data_key="structured_data",
                data_path="structured_data.organization",
                impact="Missing entity schema prevents AI and Knowledge Graph from properly identifying your brand, limiting entity recognition and authority building.",
                recommendation="Add Organization schema with required fields: name, url, logo, sameAs (social profiles), and address for local businesses."
            ))
        else:
            # Enhanced validation for Organization schema
            required_fields = ["name", "url"]
            recommended_fields = ["logo", "sameAs", "address"]
            missing_fields = []
            missing_recommended = []
            
            for field in required_fields:
                if not org_schema.get(field):
                    missing_fields.append(field)
            
            for field in recommended_fields:
                if not org_schema.get(field):
                    missing_recommended.append(field)
            
            if missing_fields:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Organization schema missing required fields: {', '.join(missing_fields)}",
                    f"Missing: {missing_fields}",
                    f"Complete schema with: {', '.join(required_fields)}",
                    data_key="structured_data",
                    data_path="structured_data.organization.missing_fields",
                    impact="Incomplete Organization schema reduces entity recognition and Knowledge Graph building effectiveness.",
                    recommendation="Add missing required fields to Organization schema for proper entity identification."
                ))
            
            if missing_recommended:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Organization schema incomplete - missing recommended fields: {', '.join(missing_recommended)}",
                    f"Missing recommended: {missing_recommended}",
                    f"Enhanced schema with: {', '.join(recommended_fields)}",
                    data_key="structured_data",
                    data_path="structured_data.organization.incomplete",
                    impact="Missing sameAs and address fields weakens AI entity recognition and Knowledge Graph completeness.",
                    recommendation="Add sameAs array with social media URLs and address information for comprehensive entity representation."
                ))
        
        return issues


class ArticleSchemaRule(BaseSEORuleV2):
    rule_id = "article_schema"
    rule_no = 97
    category = "Schema"
    severity = "high"
    description = "Article schema with publish date is required for Google News and AI recency signals"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Multi-layer validation
        content = normalized.get("content", "")
        structured_data = normalized.get("structured_data", [])
        headings = normalized.get("headings", [])
        
        # Context validation - detect page type
        page_type = ContextValidator.detect_page_type(url, content, headings)
        
        # Validation layers
        validation_results = {
            'exists': False,
            'is_contextually_valid': False,
            'is_complete': False,
            'is_user_visible': False,
            'is_semantically_correct': False
        }
        
        failure_reasons = []
        
        # 1. SEMANTIC VALIDATION: Article schema should NOT be on service/product pages
        schema_correct, schema_reason = ContextValidator.validate_schema_correctness(page_type, structured_data)
        if schema_correct:
            validation_results['is_semantically_correct'] = True
        else:
            failure_reasons.append(schema_reason)
        
        # 2. EXISTENCE: Check for Article schema on appropriate pages
        has_article_schema = False
        article_schema = None
        
        for schema in structured_data:
            schema_type = schema.get("@type")
            if isinstance(schema_type, list):
                if any(t in ["Article", "BlogPosting", "NewsArticle"] for t in schema_type):
                    has_article_schema = True
                    article_schema = schema
                    break
            elif schema_type in ["Article", "BlogPosting", "NewsArticle"]:
                has_article_schema = True
                article_schema = schema
                break
        
        if has_article_schema:
            validation_results['exists'] = True
        else:
            failure_reasons.append("No Article schema found")
        
        # 3. CONTEXT VALIDATION: Article schema should be on content pages
        if page_type in ['blog', 'general']:
            validation_results['is_contextually_valid'] = True
        else:
            failure_reasons.append(f"Article schema on {page_type} page is inappropriate")
        
        # 4. COMPLETENESS: Check required fields
        if article_schema:
            required_fields = ["datePublished", "dateModified", "headline"]
            missing_fields = []
            
            for field in required_fields:
                if not article_schema.get(field):
                    missing_fields.append(field)
            
            if not missing_fields:
                validation_results['is_complete'] = True
            else:
                failure_reasons.append(f"Missing required fields: {', '.join(missing_fields)}")
        else:
            failure_reasons.append("No article schema to validate completeness")
        
        # 5. UI VISIBILITY: Dates should be visible in content, not just schema
        date_indicators = ["published", "updated", "date:", "posted on", "last updated"]
        if ContextValidator.is_ui_visible(content, date_indicators):
            validation_results['is_user_visible'] = True
        else:
            failure_reasons.append("Publication dates not visible in page content")
        
        # Calculate confidence score
        passed_checks = sum(validation_results.values())
        total_checks = len(validation_results)
        confidence_score = ContextValidator.calculate_confidence(passed_checks, total_checks)
        
        # FAIL if any critical validation fails
        if not all([
            validation_results['exists'],
            validation_results['is_semantically_correct'],
            validation_results['is_complete']
        ]) or (page_type in ['blog', 'general'] and not validation_results['is_user_visible']):
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Article schema validation failed: {', '.join(failure_reasons)}",
                f"Failures: {len(failure_reasons)} | Confidence: {confidence_score:.2f}",
                "Article schema with complete fields on appropriate page type",
                data_key="structured_data",
                data_path="structured_data.article"
            ))
        
        return issues


class BreadcrumbListSchemaRule(BaseSEORuleV2):
    rule_id = "breadcrumblist_schema"
    rule_no = 98
    category = "Schema"
    severity = "medium"
    description = "Breadcrumb schema enables rich results and helps AI understand site hierarchy"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check multiple sources for structured data and breadcrumb signals
        structured_data = normalized.get("structured_data", [])
        breadcrumb_schema_present = normalized.get("breadcrumb_schema_present", False)
        breadcrumb_detected = normalized.get("breadcrumb_detected", False)
        
        # Enhanced validation - check nested sources
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )
        
        if not breadcrumb_schema_present:
            breadcrumb_schema_present = (
                normalized.get("schema_analysis", {}).get("breadcrumb_schema_present", False) or
                normalized.get("breadcrumb_schema_signals", {}).get("breadcrumb_schema_present", False)
            )
        
        if not breadcrumb_detected:
            breadcrumb_detected = (
                normalized.get("breadcrumb_dom_signals", {}).get("breadcrumb_detected", False) or
                normalized.get("navigation_detection", {}).get("breadcrumb_detected", False)
            )
        
        # Validation layers
        validation_results = {
            'exists': False,
            'is_contextually_valid': False,
            'is_complete': False,
            'is_user_visible': False,
            'is_semantically_correct': False
        }
        
        failure_reasons = []
        
        # 1. SCHEMA EXISTENCE: Check if breadcrumb schema exists
        has_breadcrumb_schema = False
        breadcrumb_schema = None
        
        for schema in structured_data:
            schema_type = schema.get("@type")
            if isinstance(schema_type, list):
                if "BreadcrumbList" in schema_type:
                    has_breadcrumb_schema = True
                    breadcrumb_schema = schema
                    break
            elif schema_type == "BreadcrumbList":
                has_breadcrumb_schema = True
                breadcrumb_schema = schema
                break
        
        if has_breadcrumb_schema:
            validation_results['exists'] = True
        else:
            failure_reasons.append("No BreadcrumbList schema found")
        
        # 2. UI VISIBILITY: Check if breadcrumb navigation exists in content
        content = normalized.get("content", "")
        breadcrumb_indicators = ["breadcrumb", "nav", "breadcrumb-nav", "breadcrumb-list", "breadcrumbs"]
        has_breadcrumb_nav = ContextValidator.is_ui_visible(content, breadcrumb_indicators)
        
        if has_breadcrumb_nav or breadcrumb_detected:
            validation_results['is_user_visible'] = True
        else:
            failure_reasons.append("No visible breadcrumb navigation in page content")
        
        # 3. COMPLETENESS: Check if breadcrumb has proper structure
        if has_breadcrumb_schema and breadcrumb_schema:
            breadcrumb_list = breadcrumb_schema.get("itemListElement", [])
            if breadcrumb_list and len(breadcrumb_list) > 0:
                validation_results['is_complete'] = True
            else:
                failure_reasons.append("BreadcrumbList schema has no items")
        
        # 4. SEMANTIC CORRECTNESS: Check breadcrumb items structure
        if has_breadcrumb_schema and breadcrumb_schema and validation_results['is_complete']:
            breadcrumb_list = breadcrumb_schema.get("itemListElement", [])
            valid_items = 0
            for item in breadcrumb_list:
                if (item.get("@type") == "ListItem" and 
                    item.get("name") and 
                    item.get("item")):
                    valid_items += 1
            
            if valid_items > 0:
                validation_results['is_semantically_correct'] = True
            else:
                failure_reasons.append("Breadcrumb items missing required fields (name, item)")
        
        # 5. CONTEXTUAL VALIDATION: Overall assessment
        passed_validations = sum(validation_results.values())
        if passed_validations >= 3:  # At least 3 of 5 validations pass
            validation_results['is_contextually_valid'] = True
        
        # Issue reporting
        if not validation_results['exists']:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Missing BreadcrumbList schema markup",
                f"No breadcrumb schema found | UI breadcrumbs: {validation_results['is_user_visible']}",
                "BreadcrumbList schema with proper navigation structure",
                data_key="structured_data",
                data_path="structured_data.breadcrumblist",
                impact="Missing breadcrumb schema loses rich snippet opportunities and reduces AI understanding of site structure.",
                recommendation="Add BreadcrumbList schema markup that matches your visible breadcrumb navigation. Include proper ListItem structure with name and item fields."
            ))
        elif not validation_results['is_contextually_valid']:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Breadcrumb schema implementation incomplete: {', '.join(failure_reasons)}",
                f"Validations passed: {sum(validation_results.values())}/5 | Issues: {failure_reasons}",
                "Complete breadcrumb schema with visible navigation and proper structure",
                data_key="structured_data",
                data_path="structured_data.breadcrumblist.incomplete",
                impact="Incomplete breadcrumb schema reduces rich snippet effectiveness and AI site structure understanding.",
                recommendation="Ensure breadcrumb schema is complete, visible, and semantically correct. Match schema items with visible breadcrumb navigation."
            ))
        
        return issues


class ProductSchemaRule(BaseSEORuleV2):
    rule_id = "product_schema"
    rule_no = 99
    category = "Schema"
    severity = "medium"
    description = "Product schema with price and availability powers Shopping rich results"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        structured_data = normalized.get("structured_data", [])
        
        # Check if this looks like a product page
        content = normalized.get("content", "").lower()
        is_product_page = any(indicator in content for indicator in ["price", "buy", "cart", "product", "shop"])
        
        if is_product_page:
            has_product_schema = False
            product_schema = None
            
            for schema in structured_data:
                if schema.get("@type") == "Product":
                    has_product_schema = True
                    product_schema = schema
                    break
            
            if not has_product_schema:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Product page missing Product schema",
                    "No Product schema found on product page",
                    "Product JSON-LD with nested Offer schema",
                    data_key="structured_data",
                    data_path="structured_data.product"
                ))
            else:
                # Check for Offer schema
                offers = product_schema.get("offers", [])
                if not offers:
                    issues.append(self.create_issue(
                        job_id, project_id, url,
                        "Product schema missing Offer information",
                        "No offers found in Product schema",
                        "Product schema with nested Offer containing price and availability",
                        data_key="structured_data",
                        data_path="structured_data.product.offers"
                    ))
        
        return issues


class AggregateRatingSchemaRule(BaseSEORuleV2):
    rule_id = "aggregate_rating_schema"
    rule_no = 100
    category = "Schema"
    severity = "high"
    description = "AggregateRating enables star ratings in SERPs — high-CTR rich result"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check multiple sources for structured data
        structured_data = normalized.get("structured_data", [])
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )
        
        # Enhanced detection for testimonials and ratings
        content = normalized.get("content", "").lower()
        
        # Check for testimonials section
        testimonial_indicators = [
            "what agencies say",
            "testimonials",
            "customer reviews",
            "client testimonials",
            "what our clients say",
            "reviews",
            "ratings",
            "star",
            "★",
            "testimonial",
            "satisfied clients"
        ]
        
        has_testimonials = any(indicator in content for indicator in testimonial_indicators)
        
        # Check for rating patterns in content
        rating_patterns = ["rating", "review", "score", "stars", "★", "⭐", "5/5", "4.5/5"]
        has_ratings = any(pattern in content for pattern in rating_patterns)
        
        # Check for person names (testimonials often have names)
        has_person_names = bool(
            normalized.get("content", {}).get("headings", {}).get("h4", []) or
            any("—" in content or name in content for name in ["jennifer", "mike", "sarah", "david", "emily"])
        )
        
        if has_testimonials or has_ratings or has_person_names:
            has_aggregate_rating = False
            
            # Check for AggregateRating schema
            for schema in structured_data:
                if schema.get("@type") == "AggregateRating":
                    has_aggregate_rating = True
                    break
                
                # Also check nested in other schemas
                if "aggregateRating" in schema:
                    has_aggregate_rating = True
                    break
            
            if not has_aggregate_rating:
                evidence_parts = []
                if has_testimonials:
                    evidence_parts.append("testimonials section")
                if has_ratings:
                    evidence_parts.append("rating indicators")
                if has_person_names:
                    evidence_parts.append("person names")
                
                evidence_text = ", ".join(evidence_parts)
                
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Page displays {evidence_text} but no AggregateRating schema",
                    f"Found: {evidence_text} | Missing: AggregateRating structured data",
                    "AggregateRating JSON-LD for star ratings in SERPs",
                    data_key="structured_data",
                    data_path="structured_data.aggregate_rating",
                    impact="Missing AggregateRating schema prevents star ratings from appearing in search results, reducing CTR and trust signals.",
                    recommendation="Add AggregateRating schema with ratingValue, reviewCount, and bestRating fields to enable star rich snippets in search results."
                ))
        
        return issues


class SameAsArrayRule(BaseSEORuleV2):
    rule_id = "sameas_array"
    rule_no = 101
    category = "Schema"
    severity = "high"
    description = "sameAs links are how AI and Knowledge Graph connect your brand across the web"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check multiple sources for structured data
        structured_data = normalized.get("structured_data", [])
        if not structured_data:
            structured_data = (
                normalized.get("schema_data", {}).get("structured_data", []) or
                normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", []) or
                normalized.get("body_signals", {}).get("structured_data", [])
            )
        
        # Check Organization/LocalBusiness schemas for sameAs
        for schema in structured_data:
            schema_type = schema.get("@type")
            if isinstance(schema_type, list):
                if not any(t in ["Organization", "LocalBusiness"] for t in schema_type):
                    continue
            elif schema_type not in ["Organization", "LocalBusiness"]:
                continue
            
            same_as = schema.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]
            
            # Enhanced validation for sameAs
            validation_results = {
                'exists': len(same_as) > 0,
                'is_contextually_valid': False,
                'has_multiple': len(same_as) >= 2,
                'has_social_profiles': False,
                'excludes_self_reference': True
            }
            
            failure_reasons = []
            
            # Check for multiple social profiles
            social_domains = ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'youtube.com', 'pinterest.com']
            social_count = 0
            
            for url in same_as:
                if any(domain in url.lower() for domain in social_domains):
                    social_count += 1
                
                # Check if it's just self-referencing (useless)
                current_domain = normalized.get("url", "").replace("https://", "").replace("http://", "").split("/")[0]
                if current_domain and current_domain in url.lower():
                    validation_results['excludes_self_reference'] = False
                    failure_reasons.append("Contains self-referencing URL")
            
            validation_results['has_social_profiles'] = social_count >= 2
            
            if not validation_results['exists']:
                failure_reasons.append("No sameAs array found")
            elif not validation_results['has_multiple']:
                failure_reasons.append("Less than 2 sameAs URLs")
            elif not validation_results['has_social_profiles']:
                failure_reasons.append("No social media profiles found")
            
            # Contextual validation
            passed_validations = sum(validation_results.values())
            if passed_validations >= 3:
                validation_results['is_contextually_valid'] = True
            
            # Issue reporting
            if not validation_results['exists']:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"{schema_type} schema missing sameAs array",
                    "No social profile links in entity schema",
                    "sameAs array with 2+ social media URLs",
                    data_key="structured_data",
                    data_path=f"structured_data.{schema_type.lower()}.sameas",
                    impact="Missing sameAs array prevents AI and Knowledge Graph from connecting your brand across social platforms, weakening entity authority.",
                    recommendation="Add sameAs array to Organization schema with links to Facebook, LinkedIn, Instagram, Twitter, and other social profiles."
                ))
            elif not validation_results['is_contextually_valid']:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"sameAs array validation failed: {', '.join(failure_reasons)}",
                    f"Social profiles: {social_count} | Total URLs: {len(same_as)} | Validations passed: {passed_validations}/5",
                    "sameAs array with 2+ valid social media URLs",
                    data_key="structured_data",
                    data_path=f"structured_data.{schema_type.lower()}.sameas",
                    impact="Incomplete or invalid sameAs array reduces AI entity recognition and Knowledge Graph building effectiveness.",
                    recommendation="Enhance sameAs array with at least 2-3 social media profile URLs. Remove self-referencing URLs and ensure all links are valid social profiles."
                ))
        
        return issues


class DeprecatedSchemaTypesRule(BaseSEORuleV2):
    rule_id = "deprecated_schema_types"
    rule_no = 102
    category = "Schema"
    severity = "high"
    description = "Several schema types deprecated in 2025–2026 return zero rich result value"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        structured_data = normalized.get("structured_data", [])
        
        # List of deprecated schema types (simplified - would need to maintain current list)
        deprecated_types = [
            "Blog",  # Deprecated in favor of BlogPosting
            "Recipe",  # Some deprecated properties
            # Add more as needed based on current schema.org changes
        ]
        
        for schema in structured_data:
            schema_type = schema.get("@type")
            if schema_type in deprecated_types:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Deprecated schema type found: {schema_type}",
                    f"Deprecated type: {schema_type}",
                    "Current schema.org supported types",
                    data_key="structured_data",
                    data_path=f"structured_data.{schema_type.lower()}"
                ))
        
        return issues


class DuplicateSchemaFormatsRule(BaseSEORuleV2):
    rule_id = "duplicate_schema_formats"
    rule_no = 103
    category = "Schema"
    severity = "medium"
    description = "Having both JSON-LD and Microdata for same entity creates validation errors"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        
        # Check for different schema formats
        json_ld_schemas = normalized.get("structured_data", [])
        microdata_schemas = normalized.get("microdata", [])
        
        # Get entity types from each format
        json_ld_types = set(schema.get("@type") for schema in json_ld_schemas if schema.get("@type"))
        microdata_types = set(item.get("itemtype") for item in microdata_schemas if item.get("itemtype"))
        
        # Check for overlapping entity types
        overlapping_types = json_ld_types.intersection(microdata_types)
        
        if overlapping_types:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Duplicate schema formats for: {', '.join(overlapping_types)}",
                f"Found in both JSON-LD and Microdata: {overlapping_types}",
                "Single markup format per entity (JSON-LD preferred)",
                data_key="structured_data",
                data_path="structured_data.duplicate_formats"
            ))
        
        return issues


def register_schema_rules(registry):
    """Register all schema rules with the registry."""
    registry.register(OrganizationSchemaRule())
    registry.register(ArticleSchemaRule())
    registry.register(BreadcrumbListSchemaRule())
    registry.register(ProductSchemaRule())
    registry.register(AggregateRatingSchemaRule())
    registry.register(SameAsArrayRule())
    registry.register(DeprecatedSchemaTypesRule())
    registry.register(DuplicateSchemaFormatsRule())
