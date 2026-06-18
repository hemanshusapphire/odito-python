"""
SEO Intelligence extraction module.

Produces only the two active intelligence groups:
  - schema_validation  (consumed by technicalChecks.service.js)
  - security           (consumed by technicalChecks.service.js + TechnicalResolver.js)

Reads from already-extracted seo_data["structured_data"] and response_headers.
No HTML re-parse required.
"""

from urllib.parse import urlparse

from config.config import DEPRECATED_SCHEMA_TYPES


def extract_seo_intelligence(seo_data: dict, response_headers: dict, base_url: str) -> dict:
    """
    Produce schema_validation and security intelligence groups.
    Both groups read from already-extracted data — no BeautifulSoup instantiation.
    """
    intelligence = {}

    try:
        intelligence["schema_validation"] = _extract_schema_intelligence(seo_data)
    except Exception as e:
        intelligence["schema_validation"] = {"error": str(e)}

    try:
        intelligence["security"] = _extract_security_intelligence(response_headers, base_url)
    except Exception as e:
        intelligence["security"] = {"error": str(e)}

    return intelligence


def _extract_schema_intelligence(seo_data: dict) -> dict:
    """Validate and analyze structured data schemas from already-parsed seo_data."""
    raw_schemas = seo_data.get("structured_data", [])
    validated = []
    has_faq = False
    has_how_to = False
    has_article = False
    has_breadcrumb = False

    for schema in raw_schemas:
        if not isinstance(schema, dict):
            continue

        schema_type = schema.get("@type", "Unknown")
        if isinstance(schema_type, list):
            schema_type = schema_type[0] if schema_type else "Unknown"

        entry = {
            "type": schema_type,
            "is_valid": True,
            "missing_required": [],
            "deprecated": schema_type in DEPRECATED_SCHEMA_TYPES
        }

        if schema_type == "BreadcrumbList":
            has_breadcrumb = True
            items = schema.get("itemListElement", [])
            entry["has_items"] = bool(items)
            entry["item_count"] = len(items) if isinstance(items, list) else 0
            if not items:
                entry["is_valid"] = False
                entry["missing_required"].append("itemListElement")

        elif schema_type in ("Article", "NewsArticle", "BlogPosting"):
            has_article = True
            required = ["headline", "author", "datePublished"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        elif schema_type == "FAQPage":
            has_faq = True
            main_entity = schema.get("mainEntity", [])
            entry["question_count"] = len(main_entity) if isinstance(main_entity, list) else 0
            if not main_entity:
                entry["is_valid"] = False
                entry["missing_required"].append("mainEntity")

        elif schema_type == "HowTo":
            has_how_to = True
            steps = schema.get("step", [])
            entry["step_count"] = len(steps) if isinstance(steps, list) else 0
            if not steps:
                entry["is_valid"] = False
                entry["missing_required"].append("step")

        elif schema_type == "Organization":
            required = ["name", "url"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        elif schema_type == "LocalBusiness":
            required = ["name", "address", "description"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        if entry["deprecated"]:
            entry["is_valid"] = False

        validated.append(entry)

    return {
        "schemas": validated,
        "has_faq_page": has_faq,
        "has_how_to": has_how_to,
        "has_article": has_article,
        "has_breadcrumb": has_breadcrumb
    }


def _extract_security_intelligence(response_headers: dict, base_url: str) -> dict:
    """Extract security and response header intelligence from HTTP response headers."""
    headers_lower = {k.lower(): v for k, v in (response_headers or {}).items()}

    security_headers = {
        "csp": "content-security-policy" in headers_lower,
        "hsts": "strict-transport-security" in headers_lower,
        "x_frame_options": "x-frame-options" in headers_lower,
        "x_content_type_options": "x-content-type-options" in headers_lower,
    }

    final_url = base_url
    location = headers_lower.get("location")
    if location:
        final_url = location

    redirect_chain = []
    if location and location != base_url:
        redirect_chain.append({"from": base_url, "to": location})

    return {
        "response_headers": response_headers or {},
        "security_headers": security_headers,
        "final_url": final_url,
        "redirect_chain": redirect_chain
    }
