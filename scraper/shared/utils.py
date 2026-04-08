"""Utility functions for URL normalization, text cleaning, and shared helpers."""

import re
import hashlib
import requests
import os
from urllib.parse import urlparse, urljoin, urlunparse, unquote
from config.config import SOCIAL_DOMAINS


def send_completion_callback(job_id, stats, result_data=None):
    """Send completion callback to Node.js (fire-and-forget)"""
    try:
        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
        
        callback_payload = {"stats": stats}
        
        # Include result_data if provided
        if result_data:
            callback_payload["result_data"] = result_data
        
        # Fire-and-forget with short timeout
        response = requests.post(node_url, json=callback_payload, timeout=5)
        print(f"✅ Successfully notified Node.js of job completion")
        
    except requests.exceptions.Timeout:
        print(f"⚠️ Node notification timeout (ignored) - job completed successfully")
        # Don't raise - job execution is complete regardless of notification
    except Exception as callback_error:
        print(f"⚠️ Failed to notify Node.js of completion: {callback_error}")
        # Don't raise - job execution is complete regardless of notification


def send_failure_callback(job_id, error_message):
    """Send failure callback to Node.js (fire-and-forget)"""
    try:
        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        node_fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
        
        fail_payload = {"error": error_message}
        
        # Fire-and-forget with short timeout
        requests.post(node_fail_url, json=fail_payload, timeout=5)
        print(f"✅ Notified Node.js of job failure")
        
    except requests.exceptions.Timeout:
        print(f"⚠️ Node failure notification timeout (ignored)")
        # Silent failure for error callbacks
    except:
        print(f"⚠️ Failed to notify Node.js of job failure")
        # Silent failure for error callbacks


def normalize_url(url: str) -> str:
    """Normalize URL for consistent processing with HTTPS enforcement and trailing slash handling."""
    try:
        # Decode URL encoding first
        url = unquote(url)
        
        parsed = urlparse(url)
        
        # Force HTTPS scheme
        scheme = "https"
        
        # Lowercase domain only
        netloc = parsed.netloc.lower()
        
        # Remove tracking parameters (utm, fbclid, etc.)
        # Keep only essential query parameters
        query_params = []
        if parsed.query:
            for param in parsed.query.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    # Skip tracking parameters
                    if not any(tracking in key.lower() for tracking in ['utm_', 'fbclid', 'gclid', 'msclkid', 'campaign', 'source', 'medium']):
                        query_params.append(f"{key}={value}")
        
        query = '&'.join(query_params) if query_params else ''
        
        # Handle path - remove trailing slash except for root, preserve original case
        path = parsed.path.rstrip("/")
        if path == "":
            path = "/"
        
        # Remove fragment
        fragment = ''
        
        # Reconstruct URL
        normalized = urlunparse((scheme, netloc, path, '', query, fragment))
        
        return normalized
        
    except Exception:
        # Fallback to basic normalization if anything fails
        try:
            parsed = urlparse(url)
            scheme = "https"
            netloc = parsed.netloc.lower()
            path = parsed.path.rstrip("/")
            if path == "":
                path = "/"
            return f"{scheme}://{netloc}{path}"
        except:
            return url


def get_registrable_domain(url: str) -> str:
    """Extract registrable domain (handles www, subdomains)."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Remove www. for comparison
    if domain.startswith("www."):
        domain = domain[4:]
    
    return domain


def classify_link(url: str, base_domain: str):
    """Classify link as internal, social, or external."""
    parsed = urlparse(url)
    link_domain = parsed.netloc.lower()
    
    # Internal: same registrable domain (handles subdomains, CDNs, etc.)
    if link_domain.endswith(base_domain):
        return "internal", None
    
    # Social: known social domains
    for domain, platform in SOCIAL_DOMAINS.items():
        if link_domain.endswith(domain):
            return "social", platform
    
    # External: everything else
    return "external", None


def clean_text(text: str) -> str:
    """Clean extracted text by removing noise and normalizing."""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove common form noise
    text = re.sub(r'(submit|send|click here|continue|next|previous|back)', '', text, flags=re.IGNORECASE)
    
    # Remove garbage phone numbers (basic patterns)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE]', text)
    text = re.sub(r'\b\+?1?[-.]?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}\b', '[PHONE]', text)
    
    # Remove email patterns
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', text)
    
    return text.strip()


def create_content_hash(title: str, meta_description: str, main_content: str) -> str:
    """Create content hash for deduplication."""
    content = title + meta_description + main_content[:1000]
    return hashlib.md5(content.encode()).hexdigest()


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""
