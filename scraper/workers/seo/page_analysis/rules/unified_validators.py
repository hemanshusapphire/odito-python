"""
Unified Validation Layer for SEO Rules

Centralized validation functions that aggregate data from multiple sources:
- meta_tags
- structured_data  
- visual_branding
- headers
- social data

This eliminates false positives by checking ALL relevant data sources
and provides consistent validation logic across all rules.
"""

from urllib.parse import urlparse
from .seo_rule_utils import _normalize_meta_tags, _get_meta_tag_value, _get_schemas, _find_schema_by_type


# ═══════════════════════════════════════════════════════════════
# APPLE TOUCH ICON VALIDATION
# ═══════════════════════════════════════════════════════════════

def check_apple_touch_icon(normalized):
    """
    Unified Apple Touch Icon validation.
    
    Checks BOTH meta_tags AND visual_branding.apple_icons for presence.
    Returns detailed status for accurate rule evaluation.
    
    Args:
        normalized: Normalized SEO data dict
        
    Returns:
        dict: {
            'present': bool,
            'source': str,  # 'meta_tags', 'visual_branding', or 'both'
            'details': dict  # Additional info for debugging
        }
    """
    result = {
        'present': False,
        'source': None,
        'details': {}
    }
    
    # Check 1: meta_tags for apple-touch-icon
    meta_tags = normalized.get("meta_tags", {})
    normalized_meta = _normalize_meta_tags(meta_tags)
    
    has_meta_icon = (
        "apple-touch-icon" in normalized_meta or 
        "apple-touch-icon-precomposed" in normalized_meta
    )
    
    if has_meta_icon:
        icon_values = normalized_meta.get("apple-touch-icon", [])
        precomposed_values = normalized_meta.get("apple-touch-icon-precomposed", [])
        result['details']['meta_tags'] = {
            'apple-touch-icon': icon_values,
            'apple-touch-icon-precomposed': precomposed_values
        }
    
    # Check 2: visual_branding.apple_icons array
    visual_branding = normalized.get("visual_branding", {})
    apple_icons = visual_branding.get("apple_icons", [])
    
    has_visual_icons = bool(apple_icons)
    valid_visual_icons = []
    
    if has_visual_icons:
        for icon in apple_icons:
            if isinstance(icon, dict):
                # FIXED: Accept any valid href, don't require strict URL validation
                href = icon.get("href")
                if href and isinstance(href, str) and href.strip():
                    valid_visual_icons.append({
                        'href': href.strip(),
                        'sizes': icon.get("sizes"),  # Optional - don't require
                        'rel': icon.get("rel", "apple-touch-icon")
                    })
                # FIXED: Also accept icons without href but with other valid properties
                elif icon.get("rel") or icon.get("sizes"):
                    valid_visual_icons.append(icon)
    
    if valid_visual_icons:
        result['details']['visual_branding'] = {
            'total_icons': len(apple_icons),
            'valid_icons': len(valid_visual_icons),
            'icons': valid_visual_icons
        }
    
    # Determine overall presence
    has_meta = bool(has_meta_icon)
    has_visual = bool(valid_visual_icons)
    
    result['present'] = has_meta or has_visual
    
    if has_meta and has_visual:
        result['source'] = 'both'
    elif has_meta:
        result['source'] = 'meta_tags'
    elif has_visual:
        result['source'] = 'visual_branding'
    
    return result


# ═══════════════════════════════════════════════════════════════
# AUTHOR VALIDATION
# ═══════════════════════════════════════════════════════════════

def check_author(normalized):
    """
    Unified Author validation.
    
    Checks meta_tags AND structured_data for author information.
    Supports multiple schema types (Person, Organization, Article, etc.)
    
    Args:
        normalized: Normalized SEO data dict
        
    Returns:
        dict: {
            'present': bool,
            'source': str,  # 'meta_tags', 'structured_data', or 'both'
            'details': dict  # Author info found
        }
    """
    result = {
        'present': False,
        'source': None,
        'details': {}
    }
    
    # Check 1: meta_tags author
    meta_tags = normalized.get("meta_tags", {})
    normalized_meta = _normalize_meta_tags(meta_tags)
    author_meta = _get_meta_tag_value(normalized_meta, "author")
    
    # FIXED: Remove strict truthy check, accept any non-empty author
    if author_meta is not None and str(author_meta).strip():
        result['details']['meta_tags'] = {
            'author': str(author_meta).strip(),
            'length': len(str(author_meta).strip())
        }
    
    # Check 2: structured_data author
    schemas = _get_schemas(normalized)
    author_structured = []
    
    for schema in schemas:
        if not isinstance(schema, dict):
            continue
            
        # Direct author field
        if "author" in schema:
            author_field = schema["author"]
            if isinstance(author_field, str) and str(author_field).strip():
                author_structured.append({
                    'schema_type': schema.get("@type", "unknown"),
                    'author': str(author_field).strip(),
                    'field': 'author'
                })
            elif isinstance(author_field, dict):
                # FIXED: Check multiple possible name fields
                name = author_field.get("name") or author_field.get("givenName") or author_field.get("familyName")
                if name:
                    author_structured.append({
                        'schema_type': schema.get("@type", "unknown"),
                        'author': name,
                        'field': 'author.name'
                    })
            elif isinstance(author_field, list):
                for item in author_field:
                    if isinstance(item, dict):
                        # FIXED: Check multiple possible name fields in list items
                        name = item.get("name") or item.get("givenName") or item.get("familyName")
                        if name:
                            author_structured.append({
                                'schema_type': schema.get("@type", "unknown"),
                                'author': name,
                                'field': 'author[].name'
                            })
                    elif isinstance(item, str) and str(item).strip():
                        author_structured.append({
                            'schema_type': schema.get("@type", "unknown"),
                            'author': str(item).strip(),
                            'field': 'author[]'
                        })
        
        # FIXED: Enhanced Person schema detection
        if schema.get("@type") == "Person":
            person_name = (
                schema.get("name") or 
                schema.get("givenName") or 
                schema.get("familyName") or
                (schema.get("alternateName") if isinstance(schema.get("alternateName"), str) else None)
            )
            if person_name:
                author_structured.append({
                    'schema_type': "Person",
                    'author': person_name,
                    'field': 'Person.name'
                })
        
        # FIXED: Enhanced Organization schema detection
        if schema.get("@type") in ["Organization", "LocalBusiness", "Corporation"]:
            org_name = schema.get("name") or schema.get("legalName") or schema.get("alternateName")
            if org_name:
                author_structured.append({
                    'schema_type': schema.get("@type"),
                    'author': org_name,
                    'field': 'Organization.name',
                    'has_contact': bool(schema.get("telephone") or schema.get("email") or schema.get("contactPoint"))
                })
        
        # FIXED: Check nested author references (Article -> author -> @id)
        if isinstance(schema.get("author"), dict) and schema.get("author").get("@id"):
            author_ref = schema["author"]["@id"]
            # Look for the referenced author in other schemas
            for ref_schema in schemas:
                if ref_schema.get("@id") == author_ref and ref_schema.get("name"):
                    author_structured.append({
                        'schema_type': ref_schema.get("@type", "unknown"),
                        'author': ref_schema.get("name"),
                        'field': 'author.@id.reference'
                    })
                    break
        
        # FIXED: Check creator field as alternative to author
        if "creator" in schema and "author" not in schema:
            creator_field = schema["creator"]
            if isinstance(creator_field, str) and str(creator_field).strip():
                author_structured.append({
                    'schema_type': schema.get("@type", "unknown"),
                    'author': str(creator_field).strip(),
                    'field': 'creator'
                })
            elif isinstance(creator_field, dict) and creator_field.get("name"):
                author_structured.append({
                    'schema_type': schema.get("@type", "unknown"),
                    'author': creator_field.get("name"),
                    'field': 'creator.name'
                })
    
    if author_structured:
        result['details']['structured_data'] = author_structured
    
    # FIXED: Determine overall presence without early return
    has_meta = bool(author_meta is not None and str(author_meta).strip())
    has_structured = bool(author_structured)
    
    result['present'] = has_meta or has_structured
    
    if has_meta and has_structured:
        result['source'] = 'both'
    elif has_meta:
        result['source'] = 'meta_tags'
    elif has_structured:
        result['source'] = 'structured_data'
    
    return result


# ═══════════════════════════════════════════════════════════════
# SOCIAL TAGS VALIDATION
# ═══════════════════════════════════════════════════════════════

def check_social_tags(normalized):
    """
    Unified Social Tags validation.
    
    Checks both meta_tags (with og: prefix) and social.open_graph structure.
    Validates Open Graph, Twitter Card, and other social tags.
    
    Args:
        normalized: Normalized SEO data dict
        
    Returns:
        dict: {
            'present': bool,
            'og_tags': dict,
            'twitter_tags': dict,
            'other_social': dict,
            'missing_required': list,
            'data_source': str
        }
    """
    result = {
        'present': False,
        'og_tags': {},
        'twitter_tags': {},
        'other_social': {},
        'missing_required': [],
        'data_source': 'meta_tags'  # or 'social_structure'
    }
    
    # Check 1: meta_tags with prefixes (primary source)
    meta_tags = normalized.get("meta_tags", {})
    normalized_meta = _normalize_meta_tags(meta_tags)
    
    # Open Graph tags from meta_tags
    og_tags = {}
    required_og = ['og:title', 'og:description', 'og:image', 'og:url', 'og:type']
    
    for tag in required_og:
        value = _get_meta_tag_value(normalized_meta, tag)
        if value:
            og_tags[tag] = value
    
    # Additional OG tags
    optional_og = ['og:site_name', 'og:locale', 'og:image:width', 'og:image:height']
    for tag in optional_og:
        value = _get_meta_tag_value(normalized_meta, tag)
        if value:
            og_tags[tag] = value
    
    result['og_tags'] = og_tags
    
    # Twitter Card tags
    twitter_tags = {}
    twitter_prefixes = ['twitter:', 'twitter']
    
    for key, value in normalized_meta.items():
        if any(key.startswith(prefix) for prefix in twitter_prefixes):
            twitter_tags[key] = value[0] if isinstance(value, list) and value else value
    
    result['twitter_tags'] = twitter_tags
    
    # Other social tags (Pinterest, etc.)
    other_social = {}
    social_prefixes = ['pin:', 'fb:']
    
    for key, value in normalized_meta.items():
        if any(key.startswith(prefix) for prefix in social_prefixes):
            other_social[key] = value[0] if isinstance(value, list) and value else value
    
    result['other_social'] = other_social
    
    # Check 2: social.open_graph structure (fallback/backup)
    social_data = normalized.get("social", {})
    social_og = social_data.get("open_graph", {})
    
    if social_og and not og_tags:
        # Use social structure if meta_tags empty
        result['og_tags'] = social_og
        result['data_source'] = 'social_structure'
    
    # Determine missing required OG tags
    missing_og = [tag for tag in required_og if tag not in og_tags]
    result['missing_required'] = missing_og
    
    # Overall presence (has basic OG tags)
    result['present'] = len(og_tags) >= 2  # At least title + description
    
    return result


# ═══════════════════════════════════════════════════════════════
# SECURITY HEADERS VALIDATION
# ═══════════════════════════════════════════════════════════════

def check_security_headers(normalized):
    """
    Unified Security Headers validation.
    
    Checks both meta_tags and headers for security-related headers.
    Note: Most security headers are HTTP headers, not meta tags.
    
    Args:
        normalized: Normalized SEO data dict
        
    Returns:
        dict: {
            'present': dict,
            'missing': list,
            'severity': str,  # 'info' for most, 'medium' for critical ones
            'notes': list
        }
    """
    result = {
        'present': {},
        'missing': [],
        'severity': 'info',
        'notes': []
    }
    
    # Security headers that should be HTTP headers (not meta tags)
    http_headers = normalized.get("headers", {})
    meta_tags = normalized.get("meta_tags", {})
    normalized_meta = _normalize_meta_tags(meta_tags)
    
    # Critical security headers (HTTP level)
    critical_headers = {
        'Content-Security-Policy': http_headers.get('content-security-policy'),
        'X-Frame-Options': http_headers.get('x-frame-options'),
        'X-Content-Type-Options': http_headers.get('x-content-type-options'),
        'Strict-Transport-Security': http_headers.get('strict-transport-security')
    }
    
    # Check for critical headers
    for header, value in critical_headers.items():
        if value:
            result['present'][header] = {
                'value': value,
                'source': 'http_header'
            }
        else:
            result['missing'].append(header)
    
    # Check meta tags for CSP (less common but possible)
    csp_meta = _get_meta_tag_value(normalized_meta, "content-security-policy")
    if csp_meta and 'Content-Security-Policy' not in result['present']:
        result['present']['Content-Security-Policy'] = {
            'value': csp_meta,
            'source': 'meta_tag'
        }
        # Remove from missing if it was there
        result['missing'] = [h for h in result['missing'] if h != 'Content-Security-Policy']
    
    # Determine severity based on what's missing
    critical_missing = [h for h in result['missing'] if h in ['X-Frame-Options', 'X-Content-Type-Options']]
    if critical_missing:
        result['severity'] = 'medium'
    else:
        result['severity'] = 'info'
    
    # Add notes
    result['notes'] = [
        "Security headers are typically implemented at HTTP header level, not meta tags",
        "CSP via meta tag has limited effectiveness compared to HTTP header",
        "Missing security headers are security best practices, not direct SEO factors"
    ]
    
    return result


# ═══════════════════════════════════════════════════════════════
# PINTEREST VALIDATION (Optional)
# ═══════════════════════════════════════════════════════════════

def check_pinterest_tags(normalized):
    """
    Pinterest tags validation (platform-specific, optional).
    
    Args:
        normalized: Normalized SEO data dict
        
    Returns:
        dict: {
            'present': bool,
            'tags': dict,
            'severity': str,  # Always 'info' - Pinterest is optional
            'recommendation': str
        }
    """
    result = {
        'present': False,
        'tags': {},
        'severity': 'info',
        'recommendation': 'Pinterest tags are optional - add only if targeting Pinterest audience'
    }
    
    # Check meta_tags for Pinterest tags
    meta_tags = normalized.get("meta_tags", {})
    normalized_meta = _normalize_meta_tags(meta_tags)
    
    pinterest_tags = {}
    pinterest_prefixes = ['pin:', 'pinterest']
    
    for key, value in normalized_meta.items():
        if any(key.startswith(prefix) for prefix in pinterest_prefixes):
            pinterest_tags[key] = value[0] if isinstance(value, list) and value else value
    
    # Check social structure for Pinterest
    social_data = normalized.get("social", {})
    social_pinterest = social_data.get("pinterest", {})
    
    if social_pinterest:
        pinterest_tags.update(social_pinterest)
    
    result['tags'] = pinterest_tags
    result['present'] = bool(pinterest_tags)
    
    return result


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def get_validation_summary(validation_result):
    """
    Get a human-readable summary of validation result.
    
    Args:
        validation_result: Result from any check_* function
        
    Returns:
        str: Human-readable summary
    """
    if not validation_result.get('present'):
        return "Not found"
    
    source = validation_result.get('source', 'unknown')
    details = validation_result.get('details', {})
    
    if source == 'both':
        return f"Found in multiple sources: {list(details.keys())}"
    elif source == 'meta_tags':
        return "Found in meta tags"
    elif source == 'visual_branding':
        return f"Found in visual branding ({details.get('visual_branding', {}).get('valid_icons', 0)} valid icons)"
    elif source == 'structured_data':
        return f"Found in structured data ({len(details.get('structured_data', []))} entries)"
    else:
        return f"Found in {source}"


def is_valid_url(url_str):
    """Check if URL string is valid."""
    if not url_str:
        return False
    try:
        parsed = urlparse(str(url_str).strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False
