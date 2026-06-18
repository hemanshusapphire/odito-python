"""
Extraction: entity signals (Authority + GEO-G2 entity consistency).

Reads organization name, author/publisher identity, and About page signals
from seo_page_data / seo_ai_visibility.

All output stored under ai_pages["entities"].
"""

import re
from typing import Any


_ABOUT_URL_PATTERN = re.compile(
    r"/about(?:-us)?/?$|/company/?$|/our-story/?$|/team/?$|/who-we-are/?$",
    re.IGNORECASE,
)


def extract_entities(
    page_data: dict[str, Any],
    schema: dict[str, Any],
    internal_links: list[dict],
) -> dict[str, Any]:
    """
    Extract entity authority signals.

    Args:
        page_data:      document from seo_page_data / seo_ai_visibility.
        schema:         already-extracted schema sub-document (from schema.py).
        internal_links: list of internal link dicts {href, text} for About detection.

    Returns:
        Entities sub-document for ai_pages.
    """
    org = schema.get("organization") or {}
    lb  = schema.get("local_business") or {}
    author = schema.get("author") or {}

    # ── Organization present ──────────────────────────────────────────────────
    org_present = bool(org.get("present")) or bool(lb.get("present"))
    org_name    = org.get("name") or lb.get("name")

    # ── Author / Publisher ────────────────────────────────────────────────────
    author_present = bool(author.get("present"))
    author_name    = author.get("name")

    # ── About / company page detection ───────────────────────────────────────
    has_about_link = any(
        _ABOUT_URL_PATTERN.search(lnk.get("href", ""))
        for lnk in internal_links
        if isinstance(lnk, dict)
    )

    # Also check if this page IS the about page.
    page_url: str = page_data.get("url", "") or ""
    is_about_page = bool(_ABOUT_URL_PATTERN.search(page_url))

    # Body text for company-info presence check.
    body_text: str = (
        (page_data.get("main_content", {}) or {}).get("text", "")
        or page_data.get("body_text", "")
        or ""
    )
    # Simple heuristic: about/company info is present when the body contains
    # the org name and at least one founding/mission/team keyword.
    company_keywords = re.compile(
        r"\b(founded|established|our team|our mission|about us|who we are|"
        r"our story|company|organization)\b",
        re.IGNORECASE,
    )
    company_info_in_body = bool(
        org_name
        and org_name.lower() in body_text.lower()
        and company_keywords.search(body_text)
    )

    about_company_present = has_about_link or is_about_page or company_info_in_body

    return {
        "organization_present": org_present,
        "organization_name":    org_name,
        "author_present":       author_present,
        "author_name":          author_name,
        "about_company_present": about_company_present,
        "has_about_link":       has_about_link,
        "is_about_page":        is_about_page,
    }
