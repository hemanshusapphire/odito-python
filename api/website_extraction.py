"""
Website Extraction API
Extracts business data from a website URL for the manual fallback onboarding flow.

Extracts:
- Page title, meta description, OG tags
- JSON-LD / Schema.org structured data
- Contact info (address, phone, email)
- Social media links
- Business name heuristics
- Keywords from meta tags and content
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict, Any
import httpx
import re
import json
import asyncio
from urllib.parse import urljoin, urlparse

router = APIRouter()

# ─── Request / Response Models ───────────────────────────────────────────

class WebsiteExtractionRequest(BaseModel):
    url: str

class ContactInfo(BaseModel):
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None

class WebsiteExtractionResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    error: Optional[str] = None

# ─── Configuration ───────────────────────────────────────────────────────

SCRAPE_TIMEOUT = 25  # seconds (lower than Node.js 30s to avoid double timeout)
MAX_HTML_SIZE = 5 * 1024 * 1024  # 5MB max HTML response
USER_AGENT = "OditoBot/1.0 (+https://odito.ai/bot)"

# ─── Extraction Endpoint ─────────────────────────────────────────────────

@router.post("/api/extract-website-data", response_model=WebsiteExtractionResponse)
async def extract_website_data(request: WebsiteExtractionRequest):
    """
    Fetch a website and extract business-relevant data.
    Returns structured data for onboarding project creation.
    """
    url = request.url.strip()

    if not url:
        raise HTTPException(status_code=400, detail="URL is required")

    # Validate URL scheme
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="Only HTTP and HTTPS URLs are allowed")

    try:
        html = await fetch_page(url)
        if not html:
            return WebsiteExtractionResponse(
                success=False,
                error="empty_response",
                message="Could not fetch any content from this website"
            )

        # Run all extractors
        data = extract_all(html, url)

        return WebsiteExtractionResponse(
            success=True,
            data=data
        )

    except httpx.TimeoutException:
        return WebsiteExtractionResponse(
            success=False,
            error="timeout",
            message="Website took too long to respond"
        )
    except httpx.ConnectError:
        return WebsiteExtractionResponse(
            success=False,
            error="connection_failed",
            message="Could not connect to this website. Please check the URL."
        )
    except Exception as e:
        print(f"[WEBSITE_EXTRACTION] Error extracting from {url}: {str(e)}")
        return WebsiteExtractionResponse(
            success=False,
            error="extraction_failed",
            message=f"Failed to extract data: {str(e)}"
        )


# ═══════════════════════════════════════════════════════════════════════════
#  Page Fetching
# ═══════════════════════════════════════════════════════════════════════════

async def fetch_page(url: str) -> Optional[str]:
    """Fetch page HTML with timeout and size limits."""
    async with httpx.AsyncClient(
        timeout=SCRAPE_TIMEOUT,
        follow_redirects=True,
        max_redirects=5,
        verify=False  # Some small business sites have bad SSL
    ) as client:
        response = await client.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9"
            }
        )
        response.raise_for_status()

        # Check content type
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            raise HTTPException(status_code=400, detail="URL does not return HTML content")

        # Enforce size limit
        content = response.text
        if len(content) > MAX_HTML_SIZE:
            content = content[:MAX_HTML_SIZE]

        return content


# ═══════════════════════════════════════════════════════════════════════════
#  Master Extraction
# ═══════════════════════════════════════════════════════════════════════════

def extract_all(html: str, url: str) -> Dict[str, Any]:
    """Run all extractors and merge results."""
    data = {}

    # 1. Title
    data["title"] = extract_title(html)

    # 2. Meta tags
    meta = extract_meta_tags(html)
    data["meta_description"] = meta.get("description", "")
    data["meta_keywords"] = meta.get("keywords", "")

    # 3. Open Graph
    og = extract_og_tags(html)
    data["og_title"] = og.get("og:title", "")
    data["og_description"] = og.get("og:description", "")
    data["og_image"] = og.get("og:image", "")
    data["og_site_name"] = og.get("og:site_name", "")

    # 4. JSON-LD / Schema.org
    schema = extract_schema_org(html)
    data["schema_org"] = schema
    if schema:
        data["schema_type"] = schema.get("@type", "")
        data["schema_address"] = extract_schema_address(schema)
        data["schema_phone"] = schema.get("telephone", "")
        data["business_name"] = schema.get("name", "") or data.get("og_site_name", "")
    else:
        data["business_name"] = data.get("og_site_name", "") or data.get("title", "")

    # 5. Contact info from page content
    contact = extract_contact_info(html)
    data["contact_info"] = contact
    # Merge schema contact info as fallbacks
    if not contact.get("phone") and data.get("schema_phone"):
        contact["phone"] = data["schema_phone"]
    if not contact.get("address") and data.get("schema_address"):
        contact["address"] = data["schema_address"]
    data["address"] = contact.get("address", "")
    data["phone"] = contact.get("phone", "")
    data["email"] = contact.get("email", "")

    # 6. Social links
    data["social_links"] = extract_social_links(html, url)

    # 7. Keywords from meta tags
    keywords = []
    if data.get("meta_keywords"):
        keywords = [k.strip() for k in data["meta_keywords"].split(",") if k.strip()]
    data["keywords"] = keywords[:10]

    return data


# ═══════════════════════════════════════════════════════════════════════════
#  Individual Extractors
# ═══════════════════════════════════════════════════════════════════════════

def extract_title(html: str) -> str:
    """Extract <title> tag content."""
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if match:
        return clean_text(match.group(1))
    return ""


def extract_meta_tags(html: str) -> Dict[str, str]:
    """Extract meta name/content pairs."""
    metas = {}
    pattern = r'<meta\s+(?:[^>]*?\s+)?(?:name|property)=["\']([^"\']+)["\'][^>]*?\s+content=["\']([^"\']*)["\']'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        key = match.group(1).lower().strip()
        value = match.group(2).strip()
        metas[key] = value

    # Also match content-first order
    pattern2 = r'<meta\s+(?:[^>]*?\s+)?content=["\']([^"\']*)["\'][^>]*?\s+(?:name|property)=["\']([^"\']+)["\']'
    for match in re.finditer(pattern2, html, re.IGNORECASE):
        value = match.group(1).strip()
        key = match.group(2).lower().strip()
        if key not in metas:
            metas[key] = value

    return metas


def extract_og_tags(html: str) -> Dict[str, str]:
    """Extract Open Graph meta tags."""
    og = {}
    # property="og:xxx" content="yyy"
    pattern = r'<meta\s+(?:[^>]*?\s+)?property=["\']([^"\']+)["\'][^>]*?\s+content=["\']([^"\']*)["\']'
    for match in re.finditer(pattern, html, re.IGNORECASE):
        key = match.group(1).lower().strip()
        value = match.group(2).strip()
        if key.startswith("og:"):
            og[key] = value

    # content-first order
    pattern2 = r'<meta\s+(?:[^>]*?\s+)?content=["\']([^"\']*)["\'][^>]*?\s+property=["\']([^"\']+)["\']'
    for match in re.finditer(pattern2, html, re.IGNORECASE):
        value = match.group(1).strip()
        key = match.group(2).lower().strip()
        if key.startswith("og:") and key not in og:
            og[key] = value

    return og


def extract_schema_org(html: str) -> Optional[Dict]:
    """Extract first JSON-LD schema from <script type="application/ld+json">."""
    pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)

    for raw_json in matches:
        try:
            data = json.loads(raw_json.strip())

            # Handle @graph arrays — find the most relevant entry
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") in (
                        "LocalBusiness", "Organization", "Corporation",
                        "Store", "Restaurant", "WebSite", "WebPage"
                    ):
                        return item
                return data[0] if data else None

            if isinstance(data, dict):
                # Handle @graph inside dict
                if "@graph" in data and isinstance(data["@graph"], list):
                    for item in data["@graph"]:
                        if isinstance(item, dict) and item.get("@type") in (
                            "LocalBusiness", "Organization", "Corporation",
                            "Store", "Restaurant", "WebSite", "WebPage"
                        ):
                            return item
                return data

        except (json.JSONDecodeError, TypeError):
            continue

    return None


def extract_schema_address(schema: Dict) -> str:
    """Extract formatted address from schema.org data."""
    if not schema:
        return ""

    address = schema.get("address", {})
    if isinstance(address, str):
        return address

    if isinstance(address, dict):
        parts = [
            address.get("streetAddress", ""),
            address.get("addressLocality", ""),
            address.get("addressRegion", ""),
            address.get("postalCode", ""),
            address.get("addressCountry", "")
        ]
        return ", ".join(p for p in parts if p).strip(", ")

    return ""


def extract_contact_info(html: str) -> Dict[str, str]:
    """Extract contact information from page content using regex patterns."""
    contact = {}

    # Phone patterns (international formats)
    phone_patterns = [
        r'(?:tel:|phone:|call\s*:?\s*)[\s]*([+]?[\d\s\-\(\)\.]{7,20})',
        r'href=["\']tel:([^"\']+)["\']',
        r'(\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})',
    ]
    for pattern in phone_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            phone = match.group(1).strip()
            if len(phone) >= 7:
                contact["phone"] = phone
                break

    # Email patterns
    email_patterns = [
        r'href=["\']mailto:([^"\'?]+)',
        r'[\w.+-]+@[\w-]+\.[\w.-]+',
    ]
    for pattern in email_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            email = match.group(1) if '(' in pattern or 'mailto' in pattern else match.group(0)
            email = email.strip()
            # Basic email validation
            if re.match(r'^[\w.+-]+@[\w-]+\.[\w.-]+$', email):
                contact["email"] = email
                break

    # Address: look for common address patterns near "address" keywords
    address_pattern = r'(?:address|location|office|headquarters?)[\s:]*</?\w+[^>]*>?\s*([^<]{10,200})'
    match = re.search(address_pattern, html, re.IGNORECASE)
    if match:
        addr = clean_text(match.group(1))
        if len(addr) > 10:
            contact["address"] = addr

    return contact


def extract_social_links(html: str, base_url: str) -> List[str]:
    """Extract social media profile links."""
    social_domains = [
        "facebook.com", "twitter.com", "x.com", "instagram.com",
        "linkedin.com", "youtube.com", "pinterest.com", "tiktok.com",
        "github.com", "medium.com"
    ]

    links = set()
    href_pattern = r'href=["\']([^"\']+)["\']'

    for match in re.finditer(href_pattern, html, re.IGNORECASE):
        href = match.group(1).strip()
        for domain in social_domains:
            if domain in href.lower():
                # Normalize relative URLs
                if href.startswith("/"):
                    href = urljoin(base_url, href)
                if href.startswith("http"):
                    links.add(href)
                break

    return list(links)[:20]


# ═══════════════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════════════

def clean_text(text: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&[^;]+;', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()
