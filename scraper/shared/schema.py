"""Structured data (JSON-LD schema) extraction and validation."""

import json
import re
from datetime import datetime

# Third-party imports
from bs4 import BeautifulSoup

# Local imports
from config.config import (
    SOCIAL_DOMAINS_FOR_SCHEMA, LOCALBUSINESS_REQUIRED_FIELDS,
    FAQ_INDICATORS, TESTIMONIAL_INDICATORS
)


def flatten_schema(schema_obj):
    """
    Recursively flatten JSON-LD schema objects that contain @graph arrays.
    
    If schema_obj contains "@graph", extracts each item from the @graph array as a separate schema.
    Otherwise, returns the object as-is.
    
    Returns:
        list: Flattened list of schema objects
    """
    if not isinstance(schema_obj, dict):
        return [schema_obj] if schema_obj else []
    
    # If this schema contains @graph, flatten it
    if "@graph" in schema_obj:
        graph_array = schema_obj.get("@graph", [])
        flattened = []
        if isinstance(graph_array, list):
            for item in graph_array:
                # Recursively flatten each item in case of nested @graph
                flattened.extend(flatten_schema(item))
        return flattened
    
    # No @graph, return as single-item list
    return [schema_obj]


def extract_structured_data(soup: BeautifulSoup, seo_data: dict):
    """Extract and validate structured data (JSON-LD schemas)."""
    schemas = []
    schema_types = []
    
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            if script.string:
                schema = json.loads(script.string)
                # Flatten any @graph structures
                flattened_schemas = flatten_schema(schema)
                schemas.extend(flattened_schemas)
                # Track schema types for validation
                for flat_schema in flattened_schemas:
                    if isinstance(flat_schema, dict):
                        schema_types.append(flat_schema.get("@type", "Unknown"))
        except (json.JSONDecodeError, Exception):
            continue  # Skip invalid JSON
    
    # Validate structured data
    if schemas:
        # Check for Organization schema
        organization_schemas = []
        localbusiness_schemas = []
        
        for schema in schemas:
            if isinstance(schema, dict):
                schema_type = schema.get("@type")
                if schema_type == "Organization":
                    organization_schemas.append(schema)
                elif schema_type == "LocalBusiness":
                    localbusiness_schemas.append(schema)
            elif isinstance(schema, list):
                for item in schema:
                    if isinstance(item, dict):
                        schema_type = item.get("@type")
                        if schema_type == "Organization":
                            organization_schemas.append(item)
                        elif schema_type == "LocalBusiness":
                            localbusiness_schemas.append(item)
        
        # Validate Organization schemas
        for org_schema in organization_schemas:
            # Check for sameAs (social proof)
            # Data extraction only - no issue generation
            # Social proof validation data extracted
            
            # E-E-A-T signals data extracted
            pass
        
        # Validate LocalBusiness schemas
        for lb_schema in localbusiness_schemas:
            # Check required fields
            missing_required = []
            
            for field in LOCALBUSINESS_REQUIRED_FIELDS:
                if not lb_schema.get(field):
                    missing_required.append(field)
            
            # Required fields data extracted
            
            # Validate address country
            address = lb_schema.get("address", {})
            if isinstance(address, dict):
                country = address.get("addressCountry")
                if country:
                    # Check if country is a proper country code, not a ZIP code
                    pass  # Data extraction only - no issue generation
                # Address country data extracted
                # Country format data extracted
            pass
        
        # Check for FAQ schema
        faq_schemas = []
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("@type") == "FAQPage":
                faq_schemas.append(schema)
            elif isinstance(schema, list):
                for item in schema:
                    if isinstance(item, dict) and item.get("@type") == "FAQPage":
                        faq_schemas.append(item)
        
        # Check if page needs FAQ schema (About page, services page, etc.)
        if not faq_schemas:
            content_text = soup.get_text().lower()
            has_faq_content = any(indicator in content_text for indicator in FAQ_INDICATORS)
            
        # FAQ schema data extracted
        pass
        
        # Check for BreadcrumbList schema
        breadcrumb_schemas = []
        for schema in schemas:
            if isinstance(schema, dict) and schema.get("@type") == "BreadcrumbList":
                breadcrumb_schemas.append(schema)
            elif isinstance(schema, list):
                for item in schema:
                    if isinstance(item, dict) and item.get("@type") == "BreadcrumbList":
                        breadcrumb_schemas.append(item)
        
        # Check if page needs breadcrumbs
        if not breadcrumb_schemas:
            has_navigation = bool(soup.find("nav") or soup.find("ol", class_="breadcrumb") or soup.find("ul", class_="breadcrumb"))
        # Breadcrumb schema data extracted
        pass
        
        # Check for Review schema
        review_schemas = []
        for schema in schemas:
            if isinstance(schema, dict):
                schema_type = schema.get("@type")
                if schema_type == "Review" or schema_type == "AggregateRating":
                    review_schemas.append(schema)
            elif isinstance(schema, list):
                for item in schema:
                    if isinstance(item, dict):
                        schema_type = item.get("@type")
                        if schema_type == "Review" or schema_type == "AggregateRating":
                            review_schemas.append(item)
        
        # Check if page has testimonials but no review schema
        if not review_schemas:
            content_text = soup.get_text().lower()
            has_testimonials = any(indicator in content_text for indicator in TESTIMONIAL_INDICATORS)
            
            # Review schema data extracted
            pass
    
    seo_data["structured_data"] = schemas


def validate_seo_scoring_input(job_data):
    """Validate SEO scoring job input data"""
    required_fields = ["jobId", "projectId", "userId", "sourceJobId"]
    
    for field in required_fields:
        if not job_data.get(field):
            raise ValueError(f"Missing required field: {field}")
    
    # Validate MongoDB ObjectId format for IDs
    import re
    objectid_pattern = re.compile(r'^[0-9a-fA-F]{24}$')
    
    for id_field in ["jobId", "projectId", "userId", "sourceJobId"]:
        if not objectid_pattern.match(job_data[id_field]):
            raise ValueError(f"Invalid ObjectId format for {id_field}")
    
    return True
