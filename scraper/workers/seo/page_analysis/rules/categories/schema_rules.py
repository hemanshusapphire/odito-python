"""
Schema / Structured Data SEO Rules
Rules for JSON-LD schema markup validation and optimization.
"""

import re
from ..base_seo_rule import BaseSEORuleV2
import sys
import os
from urllib.parse import urlparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .eeat_rules import ContextValidator
from shared.page_type_detector import page_type_detector
from shared.schema_applicability import (
    SCHEMA_APPLICABILITY,
    SCHEMA_FAMILIES,
    CONFLICTING_FAMILIES,
    UNIVERSAL_SCHEMAS,
)

# Display names for page types (mirrors page_type_detector._DISPLAY_NAMES)
_PAGE_TYPE_DISPLAY = {
    "homepage": "Homepage", "article": "Article", "service": "Service",
    "product": "Product", "collection": "Collection", "faq": "FAQ",
    "about": "About", "contact": "Contact", "pricing": "Pricing",
    "legal": "Legal", "listing": "Listing", "blog": "Blog",
    "location": "Location", "generic": "Generic",
}


# ── Schema applicability matrix ───────────────────────────────────────────────
#
# Mirrors the pattern in ai_v2/rules/page_type_matrix.py.
# Canonical page types come from page_type_detector.py (always lowercase):
#   homepage  article  service  product  collection  faq
#   about     contact  pricing  legal    listing     blog  generic

# Pages that must carry an Organization or LocalBusiness entity schema.
# All other page types either don't need it or satisfy the requirement by
# referencing the homepage entity via @id.
_ORG_APPLICABLE: frozenset[str] = frozenset({
    "homepage", "about", "contact", "location", "generic",
})

# Contact and location pages must specifically have LocalBusiness (not just Organization).
_LOCALBUSINESS_REQUIRED: frozenset[str] = frozenset({
    "contact", "location",
})

# Pages where Article/BlogPosting absence is an error.
_ARTICLE_APPLICABLE: frozenset[str] = frozenset({
    "article", "blog",
})

# Pages where BreadcrumbList validation is skipped entirely.
# Breadcrumbs are not a meaningful signal on root-level or transactional pages.
_BREADCRUMB_SKIP: frozenset[str] = frozenset({
    "homepage", "contact", "pricing", "legal",
})

# Pages where AggregateRating validation is applicable.
_AGGREGATE_RATING_APPLICABLE: frozenset[str] = frozenset({
    "homepage", "about", "service", "product", "location", "generic",
})

# Numeric rating pattern: must be something like "4.5/5", "98/100", "5 stars",
# "★★★★★", etc. — not just the word "reviews" or "rating" in prose.
_NUMERIC_RATING_RE = re.compile(
    r'\b[1-5](?:\.\d)?\s*/\s*5\b'           # 4.5/5, 5/5
    r'|\b[1-9]\d?\s*/\s*100\b'               # 98/100
    r'|\b[1-5]\s*(?:stars?|out\s+of\s+5)\b'  # "5 stars", "4 out of 5"
    r'|★{2,}|⭐{2,}'                         # ★★★★ or ⭐⭐⭐⭐
    r'|\b(?:4|5|4\.[0-9]|5\.0)\s*(?:stars?|rating)\b',  # "4.5 stars", "5 rating"
    re.IGNORECASE,
)


# ── Module-level helpers ──────────────────────────────────────────────────────

def _get_page_type(normalized: dict) -> str:
    """
    Return the canonical lowercase page type for this page.

    URL pattern detection takes priority over the stored `page_type` field because
    stored values may be stale — the old classifier used schema @type as the
    highest-confidence signal (90), causing pages with Article schema (e.g. a
    WordPress plugin on every page) to be misclassified as "article" even when the
    URL clearly indicated a different type (about, contact, blog index, etc.).

    Reading order:
    1. URL-pattern detection — immune to stale schema-first classification data.
       If the URL matches a pattern with confidence ≥ 65, use it.
    2. Stored `page_type` flat field — used when URL gives no pattern match
       (e.g. custom slugs like /about-naxonify that need content signals too).
    3. Full multi-signal detection via page_type_detector.detect_structured().
    """
    url = normalized.get("url", "")

    # 1. URL-based detection (most reliable; immune to stale data)
    if url:
        url_result = page_type_detector._from_url(url)
        if url_result:
            ptype, conf = url_result
            if ptype != "generic" and conf >= 65:
                return ptype

    # 2. Stored page_type (may be stale from old schema-first classifier)
    raw = normalized.get("page_type", "")
    if raw and raw.strip().lower() not in ("", "generic", "general"):
        return raw.strip().lower()

    # 3. Full multi-signal detection as last resort
    if url:
        detected = page_type_detector.detect_structured(url=url)
        name = detected.get("name", "generic").lower()
        if name != "generic" and detected.get("confidence", 0) >= 55:
            return name

    return "generic"


def _get_structured_data(normalized: dict) -> list:
    """Return structured data from the best available source in normalized."""
    sd = normalized.get("structured_data", [])
    if sd:
        return sd
    return (
        normalized.get("schema_data", {}).get("structured_data", [])
        or normalized.get("enhanced_extraction_v2", {}).get("schema_data", {}).get("structured_data", [])
        or normalized.get("body_signals", {}).get("structured_data", [])
    )


def _has_entity_reference(structured_data: list) -> bool:
    """
    Return True if any schema on this page references an external Organization
    entity via a typed @id link.

    This satisfies the entity-relationship requirement for non-entity pages
    (blog, service, article, etc.) that link to the homepage Organization entity:
        {"publisher": {"@id": "https://example.com/#organization"}}

    That is equivalent to Organisation entity presence — a duplicate full schema
    should NOT be required on every page.
    """
    reference_keys = ("publisher", "isPartOf", "author", "provider", "sourceOrganization")
    for schema in structured_data:
        for key in reference_keys:
            ref = schema.get(key)
            if isinstance(ref, dict) and ref.get("@id"):
                return True
            if isinstance(ref, str) and ref.startswith("http"):
                return True
    return False


# ── Rules ─────────────────────────────────────────────────────────────────────

class OrganizationSchemaRule(BaseSEORuleV2):
    rule_id = "organization_schema"
    rule_no = 96
    category = "Schema"
    severity = "high"
    description = "Primary entity schema is required for AI and Knowledge Graph to identify the brand"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        # ── Page-type applicability gate ─────────────────────────────────────
        # Organization/entity schema is only required on identity pages:
        #   homepage, about, contact, location, generic.
        #
        # Non-entity pages (service, article, blog, faq, product, pricing, legal,
        # collection, listing) satisfy the entity requirement by referencing the
        # homepage Organization via @id — they do NOT need a duplicate full schema.
        #
        # This mirrors AISO-A1 in ai_v2/rules/page_type_matrix.py which skips the
        # same rule on service, article, blog, faq, product, pricing, legal,
        # collection, and listing pages.
        page_type = _get_page_type(normalized)
        if page_type not in _ORG_APPLICABLE:
            return issues  # N/A for this page type

        structured_data = _get_structured_data(normalized)

        # For non-entity pages that somehow reached here as "generic", accept an
        # @id reference as a valid entity link rather than requiring a full schema.
        # (This path only fires for genuinely generic pages, not blog/service/etc.)

        # ── Locate entity schema ─────────────────────────────────────────────
        has_organization = False
        has_localbusiness = False
        org_schema = None

        for schema in structured_data:
            schema_type = schema.get("@type")
            types = schema_type if isinstance(schema_type, list) else [schema_type]
            if "LocalBusiness" in types:
                has_localbusiness = True
                has_organization = True
                org_schema = schema
                break
            if "Organization" in types and not has_organization:
                has_organization = True
                org_schema = schema

        # ── Report missing schema ────────────────────────────────────────────
        # A full Organization schema satisfies this check on every applicable
        # page type, including contact/location. LocalBusiness is a stronger
        # entity type (it adds address/telephone/geo/openingHours on top of
        # Organization) and is still the better choice for a business that has
        # a physical, customer-facing location — but requiring LocalBusiness
        # specifically, with no Organization-only path, incorrectly forces
        # physical-location data onto every contact/location page even for
        # businesses that don't have one (e.g. a SaaS company's /contact page).
        # `address` is already checked as a "recommended field" below for
        # whichever entity type is present, so a contact/location page with
        # Organization but no street address still gets that softer,
        # lower-priority nudge instead of a hard failure duplicating the same
        # gap under two different issue messages.
        requires_localbusiness = page_type in _LOCALBUSINESS_REQUIRED

        if not has_organization:
            if requires_localbusiness:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    "Missing Organization or LocalBusiness schema",
                    "No entity schema found",
                    "Organization or LocalBusiness JSON-LD with name, url, logo, sameAs (address, telephone, geo, openingHours recommended if this is a physical-location business)",
                    data_key="structured_data",
                    data_path="structured_data.localbusiness",
                    impact="Missing entity schema prevents Knowledge Graph and Maps from identifying your brand or physical location.",
                    recommendation="Add Organization schema (or LocalBusiness with address, telephone, openingHours, and geo coordinates if you have a physical location)."
                ))
            else:
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

        # ── Validate completeness of existing schema ─────────────────────────
        if org_schema:
            required_fields = ["name", "url"]
            recommended_fields = ["logo", "sameAs", "address"]
            missing_fields = [f for f in required_fields if not org_schema.get(f)]
            missing_recommended = [f for f in recommended_fields if not org_schema.get(f)]

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

            # sameAs is intentionally excluded here — SameAsArrayRule owns that check.
            # Reporting sameAs here as well creates a duplicate issue when it is the
            # only missing recommended field.
            non_sameas_missing = [f for f in missing_recommended if f != "sameAs"]
            if non_sameas_missing:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Organization schema incomplete - missing recommended fields: {', '.join(non_sameas_missing)}",
                    f"Missing recommended: {non_sameas_missing}",
                    f"Enhanced schema with: {', '.join(recommended_fields)}",
                    data_key="structured_data",
                    data_path="structured_data.organization.incomplete",
                    impact="Missing recommended fields weakens AI entity recognition and Knowledge Graph completeness.",
                    recommendation="Add logo and address information for comprehensive entity representation."
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

        # ── Page-type gate: only fire on content pages ────────────────────────
        stored_pt = _get_page_type(normalized)
        if stored_pt in ("article", "blog"):
            is_content_page = True
        elif stored_pt == "generic":
            # Fall back to lightweight ContextValidator for truly unknown pages
            content  = normalized.get("content", "")
            headings = normalized.get("headings", [])
            cv_type  = ContextValidator.detect_page_type(url, content, headings)
            is_content_page = cv_type in ("blog", "general")
        else:
            # Known non-content type (service, product, contact, etc.) — skip.
            # If Article schema IS present on those pages, InvalidSchemaTypeRule
            # will report it as an "Invalid schema for page type" issue.
            return issues

        if not is_content_page:
            return issues

        structured_data = _get_structured_data(normalized)

        # ── Check Article schema existence ────────────────────────────────────
        has_article_schema = False
        article_schema = None
        for schema in structured_data:
            schema_type = schema.get("@type")
            types = schema_type if isinstance(schema_type, list) else [schema_type]
            if any(t in ["Article", "BlogPosting", "NewsArticle"] for t in types):
                has_article_schema = True
                article_schema = schema
                break

        if not has_article_schema:
            issues.append(self.create_issue(
                job_id, project_id, url,
                "Article schema missing on content page",
                "No Article schema found",
                "Article JSON-LD with headline, datePublished, dateModified, author",
                data_key="structured_data",
                data_path="structured_data.article",
                impact="Missing Article schema prevents Google News inclusion and reduces AI recency signals for content pages.",
                recommendation="Add Article (or BlogPosting/NewsArticle) schema with headline, datePublished, dateModified, and author fields."
            ))
            return issues

        # ── Completeness check ────────────────────────────────────────────────
        required_fields = ["datePublished", "dateModified", "headline"]
        missing_fields  = [f for f in required_fields if not article_schema.get(f)]
        if missing_fields:
            issues.append(self.create_issue(
                job_id, project_id, url,
                f"Article schema missing required fields: {', '.join(missing_fields)}",
                f"Missing: {missing_fields}",
                "Complete Article schema with headline, datePublished, dateModified",
                data_key="structured_data",
                data_path="structured_data.article.incomplete",
                impact="Incomplete Article schema reduces rich result eligibility and AI date/recency scoring.",
                recommendation=f"Add missing fields to Article schema: {', '.join(missing_fields)}."
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

        # ── Page-type applicability gate ─────────────────────────────────────
        # BreadcrumbList is not meaningful on root/transactional pages where
        # breadcrumbs are unusual or absent by design.
        page_type = _get_page_type(normalized)
        if page_type in _BREADCRUMB_SKIP:
            return issues

        structured_data = _get_structured_data(normalized)
        breadcrumb_schema_present = normalized.get("breadcrumb_schema_present", False)
        breadcrumb_detected = normalized.get("breadcrumb_detected", False)

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

        validation_results = {
            'exists': False,
            'is_contextually_valid': False,
            'is_complete': False,
            'is_user_visible': False,
            'is_semantically_correct': False
        }
        failure_reasons = []

        # 1. SCHEMA EXISTENCE
        has_breadcrumb_schema = False
        breadcrumb_schema = None
        for schema in structured_data:
            schema_type = schema.get("@type")
            types = schema_type if isinstance(schema_type, list) else [schema_type]
            if "BreadcrumbList" in types:
                has_breadcrumb_schema = True
                breadcrumb_schema = schema
                break

        if has_breadcrumb_schema:
            validation_results['exists'] = True
        else:
            failure_reasons.append("No BreadcrumbList schema found")

        # 2. UI VISIBILITY
        content = normalized.get("content", "")
        breadcrumb_indicators = ["breadcrumb", "nav", "breadcrumb-nav", "breadcrumb-list", "breadcrumbs"]
        has_breadcrumb_nav = ContextValidator.is_ui_visible(content, breadcrumb_indicators)

        if has_breadcrumb_nav or breadcrumb_detected:
            validation_results['is_user_visible'] = True
        else:
            failure_reasons.append("No visible breadcrumb navigation in page content")

        # 3. COMPLETENESS
        if has_breadcrumb_schema and breadcrumb_schema:
            breadcrumb_list = breadcrumb_schema.get("itemListElement", [])
            if breadcrumb_list and len(breadcrumb_list) > 0:
                validation_results['is_complete'] = True
            else:
                failure_reasons.append("BreadcrumbList schema has no items")

        # 4. SEMANTIC CORRECTNESS
        if has_breadcrumb_schema and breadcrumb_schema and validation_results['is_complete']:
            breadcrumb_list = breadcrumb_schema.get("itemListElement", [])
            valid_items = sum(
                1 for item in breadcrumb_list
                if item.get("@type") == "ListItem" and item.get("name") and item.get("item")
            )
            if valid_items > 0:
                validation_results['is_semantically_correct'] = True
            else:
                failure_reasons.append("Breadcrumb items missing required fields (name, item)")

        # 5. CONTEXTUAL VALIDATION
        if sum(validation_results.values()) >= 3:
            validation_results['is_contextually_valid'] = True

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
        structured_data = _get_structured_data(normalized)

        is_product_page = self._is_product_page(normalized, url, structured_data)

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

    @staticmethod
    def _is_product_page(normalized, url, structured_data):
        """
        Determine whether a page is a genuine product page using a confidence-ranked
        signal hierarchy. Raw content keywords are NOT a valid signal — they fire on
        service, about, blog, and home pages that merely mention "price" or "product".

        Priority (highest → lowest):
          1. page_type flat field (from page_type_detector, set by context enrichment)
          2. Schema.org @type = Product / ProductGroup on the page
          3. URL path segments that unambiguously indicate a product detail page
        """
        # 1. Stored page_type (most authoritative)
        stored_pt = _get_page_type(normalized)
        if stored_pt == "product":
            return True
        if stored_pt and stored_pt not in ("generic",):
            return False  # Explicit non-product type

        # 2. Schema.org @type
        for schema in structured_data:
            schema_type = schema.get("@type")
            types = schema_type if isinstance(schema_type, list) else [schema_type]
            if any(t in ("Product", "ProductGroup") for t in types if t):
                return True

        # 3. URL path — only patterns that unambiguously point to product detail pages
        detected = page_type_detector.detect_structured(url=url)
        if detected.get("name", "").lower() == "product" and detected.get("confidence", 0) >= 75:
            return True

        return False


class AggregateRatingSchemaRule(BaseSEORuleV2):
    rule_id = "aggregate_rating_schema"
    rule_no = 100
    category = "Schema"
    severity = "high"
    description = "AggregateRating enables star ratings in SERPs — high-CTR rich result"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        # ── Page-type applicability gate ─────────────────────────────────────
        # AggregateRating is applicable on pages that can legitimately display
        # ratings: homepage, about, service pages, product pages, location pages.
        # It is NOT applicable on pure content pages (article, blog), contact,
        # pricing, legal, or FAQ pages.
        page_type = _get_page_type(normalized)
        if page_type not in _AGGREGATE_RATING_APPLICABLE:
            return issues

        structured_data = _get_structured_data(normalized)
        content = normalized.get("content", "").lower()

        # ── Rating evidence: require numeric rating signals, not just keywords ─
        # Keywords like "reviews", "testimonials", "rating" appear in prose and
        # navigation menus on nearly every page — they are not evidence of actual
        # ratings being displayed.  Only fire when there are measurable rating
        # artefacts: star symbols, numeric scores, or an explicit rating scale.
        has_numeric_rating = bool(_NUMERIC_RATING_RE.search(content))

        # Testimonials section is an acceptable signal on its own because dedicated
        # testimonial blocks with names/quotes indicate social proof content.
        # BUT require at least one specific testimonial keyword (not just "reviews").
        specific_testimonial_keywords = [
            "what our clients say",
            "client testimonials",
            "customer testimonials",
            "what agencies say",
            "satisfied clients",
            "testimonials",
        ]
        has_testimonials = any(kw in content for kw in specific_testimonial_keywords)

        # Fire only when there is BOTH a testimonial section AND numeric rating
        # evidence, OR when numeric ratings alone are clearly displayed.
        should_check = has_numeric_rating or (has_testimonials and has_numeric_rating)
        if not should_check:
            return issues

        # ── Check for AggregateRating schema ─────────────────────────────────
        has_aggregate_rating = False
        for schema in structured_data:
            if schema.get("@type") == "AggregateRating":
                has_aggregate_rating = True
                break
            if "aggregateRating" in schema:
                has_aggregate_rating = True
                break

        if not has_aggregate_rating:
            evidence_parts = []
            if has_testimonials:
                evidence_parts.append("testimonials section")
            if has_numeric_rating:
                evidence_parts.append("numeric rating indicators")
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

        structured_data = _get_structured_data(normalized)

        for schema in structured_data:
            schema_type = schema.get("@type")
            if isinstance(schema_type, list):
                if not any(t in ["Organization", "LocalBusiness"] for t in schema_type):
                    continue
            elif schema_type not in ["Organization", "LocalBusiness"]:
                continue

            # Guard: only validate sameAs when required fields (name, url) are present.
            # If name or url are missing, OrganizationSchemaRule already reports the broader
            # incompleteness.  Firing a sameAs-specific issue on top of that is misleading —
            # the root problem is not sameAs, it is the schema being fundamentally incomplete.
            if not schema.get("name") or not schema.get("url"):
                continue

            same_as = schema.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]

            validation_results = {
                'exists': len(same_as) > 0,
                'is_contextually_valid': False,
                'has_multiple': len(same_as) >= 2,
                'has_social_profiles': False,
                'excludes_self_reference': True
            }
            failure_reasons = []

            social_domains = ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'youtube.com', 'pinterest.com']
            social_count = 0
            current_domain = normalized.get("url", "").replace("https://", "").replace("http://", "").split("/")[0]

            for sa_url in same_as:
                if any(domain in sa_url.lower() for domain in social_domains):
                    social_count += 1
                if current_domain and current_domain in sa_url.lower():
                    validation_results['excludes_self_reference'] = False
                    failure_reasons.append("Contains self-referencing URL")

            validation_results['has_social_profiles'] = social_count >= 2

            if not validation_results['exists']:
                failure_reasons.append("No sameAs array found")
            elif not validation_results['has_multiple']:
                failure_reasons.append("Less than 2 sameAs URLs")
            elif not validation_results['has_social_profiles']:
                failure_reasons.append("No social media profiles found")

            passed_validations = sum(validation_results.values())
            if passed_validations >= 3:
                validation_results['is_contextually_valid'] = True

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
        structured_data = _get_structured_data(normalized)

        deprecated_types = [
            "Blog",       # Deprecated in favour of BlogPosting
            "Recipe",     # Some deprecated properties
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

        json_ld_schemas = normalized.get("structured_data", [])
        microdata_schemas = normalized.get("microdata", [])

        json_ld_types = set(schema.get("@type") for schema in json_ld_schemas if schema.get("@type"))
        microdata_types = set(item.get("itemtype") for item in microdata_schemas if item.get("itemtype"))

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


class InvalidSchemaTypeRule(BaseSEORuleV2):
    rule_id = "invalid_schema_type"
    rule_no = 104
    category = "Schema"
    severity = "high"
    description = "Detects schema types that don't match the page's detected content type"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        page_type = _get_page_type(normalized)
        if page_type == "generic":
            return issues  # Can't determine invalid vs valid without a known page type

        applicability = SCHEMA_APPLICABILITY.get(page_type, SCHEMA_APPLICABILITY["generic"])
        not_applicable = applicability.get("not_applicable", frozenset())
        if not not_applicable:
            return issues

        structured_data = _get_structured_data(normalized)
        if not structured_data:
            return issues

        # Collect all @types present on the page
        detected_types = []
        for schema in structured_data:
            schema_type_val = schema.get("@type")
            if isinstance(schema_type_val, list):
                detected_types.extend(t for t in schema_type_val if t)
            elif isinstance(schema_type_val, str) and schema_type_val:
                detected_types.append(schema_type_val)

        page_display = _PAGE_TYPE_DISPLAY.get(page_type, page_type.capitalize())
        recommended  = applicability.get("recommended", frozenset())
        suggested    = ", ".join(sorted(recommended)[:3]) if recommended else "WebPage"

        for invalid_type in detected_types:
            if invalid_type in not_applicable and invalid_type not in UNIVERSAL_SCHEMAS:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Invalid {invalid_type} schema on {page_display} page",
                    f"Found: {invalid_type} | Page type: {page_display}",
                    f"Use schema appropriate for {page_display} pages (e.g., {suggested})",
                    data_key="structured_data",
                    data_path=f"structured_data.{invalid_type.lower()}",
                    impact=(
                        f"Using {invalid_type} schema on a {page_display} page confuses "
                        f"search engines about page intent and may cause incorrect rich result eligibility."
                    ),
                    recommendation=(
                        f"Remove {invalid_type} schema from this {page_display} page "
                        f"and add the appropriate schema type instead."
                    ),
                    context={
                        "invalid_type":    invalid_type,
                        "page_type":       page_type,
                        "detected_types":  detected_types,
                        "suggested":       suggested,
                    }
                ))

        return issues


class SchemaTypeConflictRule(BaseSEORuleV2):
    rule_id = "schema_type_conflict"
    rule_no = 105
    category = "Schema"
    severity = "medium"
    description = "Detects mutually exclusive schema types present on the same page"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []

        structured_data = _get_structured_data(normalized)
        if not structured_data:
            return issues

        # Collect all @types present on the page
        detected_types = []
        for schema in structured_data:
            schema_type_val = schema.get("@type")
            if isinstance(schema_type_val, list):
                detected_types.extend(t for t in schema_type_val if t)
            elif isinstance(schema_type_val, str) and schema_type_val:
                detected_types.append(schema_type_val)

        if not detected_types:
            return issues

        page_type   = _get_page_type(normalized)
        page_display = _PAGE_TYPE_DISPLAY.get(page_type, page_type.capitalize())

        # Check each conflicting schema family
        for family_name in CONFLICTING_FAMILIES:
            family_types = SCHEMA_FAMILIES.get(family_name, frozenset())
            found_in_family = [t for t in detected_types if t in family_types]
            if len(found_in_family) >= 2:
                issues.append(self.create_issue(
                    job_id, project_id, url,
                    f"Conflicting {family_name} schema types: {', '.join(found_in_family)}",
                    f"Multiple {family_name} schemas: {', '.join(found_in_family)}",
                    f"Use only the most specific {family_name} schema type for this page",
                    data_key="structured_data",
                    data_path="structured_data.conflict",
                    impact=(
                        f"Having {len(found_in_family)} conflicting {family_name} schemas "
                        f"({', '.join(found_in_family)}) causes search engine confusion about "
                        f"page content type and may invalidate rich result eligibility."
                    ),
                    recommendation=(
                        f"Keep only the most specific {family_name} schema "
                        f"and remove the others: {', '.join(found_in_family[1:])}."
                    ),
                    context={
                        "conflicting_types": found_in_family,
                        "family":            family_name,
                        "page_type":         page_type,
                    }
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
    registry.register(InvalidSchemaTypeRule())
    registry.register(SchemaTypeConflictRule())
