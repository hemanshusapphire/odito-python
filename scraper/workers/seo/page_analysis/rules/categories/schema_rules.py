"""
Schema.org SEO Rules (Rules 57–61, 122–125, 189–193)

Rules for JSON-LD structured data, LocalBusiness, Organization,
BreadcrumbList, Article/BlogPosting, and FAQPage schemas.
"""

from urllib.parse import urlparse
from ..base_seo_rule import BaseSEORuleV2
from ..seo_rule_utils import (
    _keyword_from_context,
    _get_schemas,
    _find_schema_by_type,
    _is_valid_https_url,
)


# Helpers (_get_schemas, _find_schema_by_type, _is_valid_https_url,
# _keyword_from_context) imported from seo_rule_utils


# List of deprecated schema types (per Google 2025+ guidance)
DEPRECATED_SCHEMA_TYPES = {
    "DataCatalog", "DataFeed", "EducationalOccupationalProgram",
}

VALID_SCHEMA_CONTEXTS = {
    "https://schema.org", "http://schema.org",
    "https://schema.org/", "http://schema.org/",
}

# Pre-computed normalized set (hoisted from evaluate to avoid per-call rebuild)
_VALID_CONTEXTS_NORMALIZED = {c.rstrip("/") for c in VALID_SCHEMA_CONTEXTS}


# ═══════════════════════════════════════════════════════════════
# CORE SCHEMA.ORG RULES (Rules 57–61)
# ═══════════════════════════════════════════════════════════════

class SchemaJsonLdPresentRule(BaseSEORuleV2):
    rule_id = "SCHEMA_JSONLD_PRESENT"
    rule_no = 57
    category = "Schema"
    severity = "high"
    description = "JSON-LD structured data must be present"

    def evaluate(self, normalized, job_id, project_id, url):
        schemas = _get_schemas(normalized)
        if not schemas:
            return [self.create_issue(
                job_id, project_id, url,
                "No JSON-LD structured data found",
                "None", "Add JSON-LD schema markup",
                data_key="structured_data"
            )]
        return []


class SchemaValidContextRule(BaseSEORuleV2):
    rule_id = "SCHEMA_VALID_CONTEXT"
    rule_no = 58
    category = "Schema"
    severity = "high"
    description = "Schema @context must be valid"

    def evaluate(self, normalized, job_id, project_id, url):
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                context = s.get("@context", "")
                if context and str(context).strip().rstrip("/") not in _VALID_CONTEXTS_NORMALIZED:
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"Invalid schema @context: {context}",
                        str(context), "https://schema.org",
                        data_key="structured_data"
                    )]
        return []


class SchemaValidTypeRule(BaseSEORuleV2):
    rule_id = "SCHEMA_VALID_TYPE"
    rule_no = 59
    category = "Schema"
    severity = "high"
    description = "Schema @type must be valid"

    def evaluate(self, normalized, job_id, project_id, url):
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                schema_type = s.get("@type", "")
                if not schema_type:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Schema missing @type",
                        "None", "Add valid @type to schema",
                        data_key="structured_data"
                    )]
        return []


class SchemaUrlMatchesPageRule(BaseSEORuleV2):
    rule_id = "SCHEMA_URL_MATCHES_PAGE"
    rule_no = 60
    category = "Schema"
    severity = "high"
    description = "Schema URL should match page URL"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "").rstrip("/").lower()
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                schema_url = str(s.get("url", "")).rstrip("/").lower()
                if schema_url and page_url and schema_url != page_url:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Schema URL does not match page URL",
                        schema_url, page_url,
                        data_key="structured_data"
                    )]
        return []


class SchemaNameAlignsWithTitleRule(BaseSEORuleV2):
    rule_id = "SCHEMA_NAME_ALIGNS_TITLE"
    rule_no = 61
    category = "Schema"
    severity = "high"
    description = "Schema name should align with page title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "").lower()
        if not title:
            return []
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                name = str(s.get("name", "")).lower()
                if name and title:
                    # Check word overlap
                    name_words = set(name.split())
                    title_words = set(title.split())
                    if name_words and title_words:
                        overlap = len(name_words & title_words) / max(len(name_words), len(title_words))
                        if overlap < 0.3:
                            return [self.create_issue(
                                job_id, project_id, url,
                                "Schema name doesn't align with page title",
                                name[:60], f"Should reflect: {title[:60]}",
                                data_key="structured_data"
                            )]
        return []


# ═══════════════════════════════════════════════════════════════
# LOCALBUSINESS SCHEMA (Rules 122–125)
# ═══════════════════════════════════════════════════════════════

class SchemaLocalBizPhoneRule(BaseSEORuleV2):
    rule_id = "SCHEMA_LOCALBIZ_PHONE"
    rule_no = 122
    category = "Schema"
    severity = "high"
    description = "LocalBusiness telephone should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        lb = _find_schema_by_type(_get_schemas(normalized), "LocalBusiness")
        if lb and not lb.get("telephone"):
            return [self.create_issue(
                job_id, project_id, url,
                "LocalBusiness schema missing telephone",
                "None", "Add telephone to LocalBusiness",
                data_key="structured_data"
            )]
        return []


class SchemaLocalBizAddressRule(BaseSEORuleV2):
    rule_id = "SCHEMA_LOCALBIZ_ADDRESS"
    rule_no = 123
    category = "Schema"
    severity = "high"
    description = "LocalBusiness address should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        lb = _find_schema_by_type(_get_schemas(normalized), "LocalBusiness")
        if lb and not lb.get("address"):
            return [self.create_issue(
                job_id, project_id, url,
                "LocalBusiness schema missing address",
                "None", "Add PostalAddress to LocalBusiness",
                data_key="structured_data"
            )]
        return []


class SchemaImageValidRule(BaseSEORuleV2):
    rule_id = "SCHEMA_IMAGE_VALID"
    rule_no = 124
    category = "Schema"
    severity = "high"
    description = "Schema image should be valid URL + HTTPS"

    def evaluate(self, normalized, job_id, project_id, url):
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                image = s.get("image")
                if image:
                    img_url = image if isinstance(image, str) else (image.get("url", "") if isinstance(image, dict) else "")
                    if img_url and not _is_valid_https_url(img_url):
                        return [self.create_issue(
                            job_id, project_id, url,
                            "Schema image is not valid HTTPS URL",
                            str(img_url)[:80], "Use valid HTTPS image URL",
                            data_key="structured_data"
                        )]
        return []


class SchemaDescriptionKeywordRule(BaseSEORuleV2):
    rule_id = "SCHEMA_DESCRIPTION_KEYWORD"
    rule_no = 125
    category = "Schema"
    severity = "medium"
    description = "Schema description should contain keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                desc = s.get("description", "")
                if desc and keyword.lower() not in str(desc).lower():
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Schema description does not contain keyword",
                        str(desc)[:60], f"Include: {keyword}",
                        data_key="structured_data"
                    )]
                    break
        return []


# ═══════════════════════════════════════════════════════════════
# SCHEMA 2026 RULES (Rules 189–193)
# ═══════════════════════════════════════════════════════════════

class SchemaTypeNotDeprecatedRule(BaseSEORuleV2):
    rule_id = "SCHEMA_TYPE_NOT_DEPRECATED"
    rule_no = 189
    category = "Schema"
    severity = "high"
    description = "Schema @type must not be deprecated"

    def evaluate(self, normalized, job_id, project_id, url):
        for s in _get_schemas(normalized):
            if isinstance(s, dict):
                schema_type = s.get("@type", "")
                if schema_type in DEPRECATED_SCHEMA_TYPES:
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"Schema uses deprecated type: {schema_type}",
                        schema_type, "Use supported schema types",
                        data_key="structured_data"
                    )]
        return []


class SchemaNoDuplicateFormatsRule(BaseSEORuleV2):
    rule_id = "SCHEMA_NO_DUPLICATE_FORMATS"
    rule_no = 190
    category = "Schema"
    severity = "high"
    description = "No duplicate schema formats (JSON-LD + Microdata)"

    def evaluate(self, normalized, job_id, project_id, url):
        # We only have JSON-LD data; if structured_data has multiple entries
        # of same type, that could indicate duplicate formats
        schemas = _get_schemas(normalized)
        types_seen = {}
        for s in schemas:
            if isinstance(s, dict):
                stype = s.get("@type", "")
                if stype:
                    types_seen[stype] = types_seen.get(stype, 0) + 1
        dupes = {t: c for t, c in types_seen.items() if c > 1}
        if dupes:
            return [self.create_issue(
                job_id, project_id, url,
                f"Duplicate schema types: {', '.join(f'{t}(×{c})' for t, c in dupes.items())}",
                str(dupes), "Use one format per schema type",
                data_key="structured_data"
            )]
        return []


class SchemaBreadcrumbListRule(BaseSEORuleV2):
    rule_id = "SCHEMA_BREADCRUMBLIST"
    rule_no = 191
    category = "Schema"
    severity = "high"
    description = "BreadcrumbList schema on category pages"

    def evaluate(self, normalized, job_id, project_id, url):
        page_url = normalized.get("url", "")
        # Heuristic: if URL has 2+ path segments, it's likely a category/deep page
        path_segments = [s for s in page_url.split("/") if s and "." not in s]
        if len(path_segments) >= 3:  # domain + at least 2 path segments
            has_breadcrumb = _find_schema_by_type(_get_schemas(normalized), "BreadcrumbList")
            if not has_breadcrumb:
                return [self.create_issue(
                    job_id, project_id, url,
                    "BreadcrumbList schema missing on deep page",
                    "None", "Add BreadcrumbList for navigation",
                    data_key="structured_data"
                )]
        return []


class SchemaArticleDatePublishedRule(BaseSEORuleV2):
    rule_id = "SCHEMA_ARTICLE_DATE"
    rule_no = 192
    category = "Schema"
    severity = "high"
    description = "Article/BlogPosting schema should have datePublished"

    def evaluate(self, normalized, job_id, project_id, url):
        for stype in ("Article", "BlogPosting", "NewsArticle"):
            schema = _find_schema_by_type(_get_schemas(normalized), stype)
            if schema and not schema.get("datePublished"):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{stype} schema missing datePublished",
                    "None", "Add datePublished to article schema",
                    data_key="structured_data"
                )]
        return []


class SchemaFaqPageRule(BaseSEORuleV2):
    rule_id = "SCHEMA_FAQPAGE"
    rule_no = 193
    category = "Schema"
    severity = "medium"
    description = "FAQPage schema where relevant"

    def evaluate(self, normalized, job_id, project_id, url):
        content = normalized.get("content_text", "").lower()
        faq_indicators = ["faq", "frequently asked", "questions and answers", "q&a"]
        has_faq_content = any(ind in content for ind in faq_indicators)
        if has_faq_content:
            has_faq_schema = _find_schema_by_type(_get_schemas(normalized), "FAQPage")
            if not has_faq_schema:
                return [self.create_issue(
                    job_id, project_id, url,
                    "FAQ content detected but no FAQPage schema",
                    "FAQ keywords found", "Add FAQPage schema",
                    data_key="structured_data"
                )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_schema_rules(registry):
    """Register all Schema rules"""
    # Core (57–61)
    registry.register(SchemaJsonLdPresentRule())
    registry.register(SchemaValidContextRule())
    registry.register(SchemaValidTypeRule())
    registry.register(SchemaUrlMatchesPageRule())
    registry.register(SchemaNameAlignsWithTitleRule())
    # LocalBusiness (122–125)
    registry.register(SchemaLocalBizPhoneRule())
    registry.register(SchemaLocalBizAddressRule())
    registry.register(SchemaImageValidRule())
    registry.register(SchemaDescriptionKeywordRule())
    # Schema 2026 (189–193)
    registry.register(SchemaTypeNotDeprecatedRule())
    registry.register(SchemaNoDuplicateFormatsRule())
    registry.register(SchemaBreadcrumbListRule())
    registry.register(SchemaArticleDatePublishedRule())
    registry.register(SchemaFaqPageRule())
