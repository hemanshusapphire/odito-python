"""Link extraction and sitemap processing functionality."""

import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin

# Third-party imports
import requests
from bs4 import BeautifulSoup

# Local imports
from .utils import normalize_url, classify_link


def extract_sitemaps_from_robots(base_url: str) -> list[str]:
    """Extract sitemap URLs from robots.txt."""
    robots_url = base_url.rstrip("/") + "/robots.txt"
    sitemaps = []

    try:
        res = requests.get(robots_url, timeout=10)
        if res.status_code != 200:
            return []

        for line in res.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())

    except Exception:
        pass

    return sitemaps


def extract_links_from_sitemap(base_url: str) -> list[str]:
    """Extract URLs from sitemap.xml and sitemap_index.xml."""
    sitemap_urls = [
        base_url.rstrip("/") + "/sitemap.xml",
        base_url.rstrip("/") + "/sitemap_index.xml"
    ]

    # Add robots.txt-discovered sitemaps
    sitemap_urls.extend(extract_sitemaps_from_robots(base_url))

    links = []

    for sitemap_url in sitemap_urls:
        try:
            res = requests.get(sitemap_url, timeout=10)
            if res.status_code != 200:
                continue

            root = ET.fromstring(res.text)

            for loc in root.iter():
                if loc.tag.endswith("loc") and loc.text:
                    links.append(loc.text.strip())

            if links:
                return links

        except Exception:
            continue

    return []


def extract_internal_links_from_html(html: str, base_url: str) -> list[dict]:
    """Extract internal links from HTML content."""
    soup = BeautifulSoup(html, "lxml")

    base_domain = base_url.split("//")[-1].split("/")[0].replace("www.", "")
    extracted_links = []
    seen = set()

    # Try HTML links first
    for a in soup.find_all("a", href=True):
        href = a.get("href").strip()
        text = a.get_text(strip=True)

        if (
            not href
            or href.startswith("#")
            or href.startswith("mailto:")
            or href.startswith("tel:")
            or href.startswith("javascript:")
        ):
            continue

        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)
        link_domain = parsed.netloc.replace("www.", "")

        if link_domain.endswith(base_domain):
            clean_url = parsed.scheme + "://" + parsed.netloc + parsed.path

            if clean_url not in seen:
                seen.add(clean_url)
                extracted_links.append({
                    "url": clean_url,
                    "text": text
                })

    # Fallback to sitemap (incl. robots.txt)
    if not extracted_links:
        sitemap_links = extract_links_from_sitemap(base_url)
        for url in sitemap_links:
            extracted_links.append({
                "url": url,
                "text": "sitemap"
            })

    return extracted_links


def extract_all_links_from_html(html: str, base_url: str, base_domain: str) -> tuple[list, list, list]:
    """Extract all links (internal, external, social) from HTML content."""
    soup = BeautifulSoup(html, "lxml")
    external_links = []
    social_links = []
    internal_links_found = []
    
    for a in soup.find_all("a", href=True):
        href = a.get("href").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue
            
        absolute_url = href if href.startswith("http") else urljoin(base_url, href)
        normalized_url = normalize_url(absolute_url)
        
        # Classify the link
        link_type, platform = classify_link(normalized_url, base_domain)
        
        if link_type == "internal":
            internal_links_found.append(normalized_url)
        elif link_type == "social":
            social_links.append({
                "platform": platform,
                "url": normalized_url,
                "sourceUrl": base_url
            })
        elif link_type == "external":
            external_links.append({
                "url": normalized_url,
                "sourceUrl": base_url
            })
    
    return internal_links_found, external_links, social_links
