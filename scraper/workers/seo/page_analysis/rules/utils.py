"""
SEO Rule Utilities — safe meta tag access helpers.

Used across rule files to handle the meta_tags / og_tags
values being stored as lists instead of strings.
"""


def safe_get_meta(meta_dict, key, default=""):
    """Safely get a meta tag value, unwrapping single-element lists."""
    val = meta_dict.get(key, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val or default


def safe_str(value):
    """Coerce value to string, unwrapping single-element lists."""
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value) if value is not None else ""


def normalize_string_compare(value):
    """Normalize string for comparison: strip, lowercase, handle lists."""
    if value is None:
        return ""
    
    # Handle list values
    if isinstance(value, list):
        if not value:
            return ""
        value = value[0]
    
    # Convert to string and normalize
    str_value = str(value)
    
    # Normalize URLs: remove trailing slash
    if str_value.startswith(('http://', 'https://')):
        str_value = str_value.rstrip('/')
    
    return str_value.strip().lower()
