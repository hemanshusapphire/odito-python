"""
Extraction: FAQ signals.

Compares FAQ schema Q&A pairs against visible FAQ content on the page to
detect mismatches (the core of rule AEO-048).

All output stored under ai_pages["faq"].
"""

import re
from typing import Any


def _normalize(text: str) -> str:
    """Lowercase, strip punctuation for fuzzy comparison."""
    return re.sub(r"[^a-z0-9\s]", "", text.lower()).strip()


def _questions_match(schema_q: str, visible_q: str, threshold: float = 0.6) -> bool:
    """
    Return True if schema question and visible question share enough tokens
    to be considered the same question.
    """
    a_tokens = set(_normalize(schema_q).split())
    b_tokens = set(_normalize(visible_q).split())
    if not a_tokens:
        return False
    overlap = len(a_tokens & b_tokens) / len(a_tokens)
    return overlap >= threshold


def extract_faq(
    schema_pairs: list[dict],
    page_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Extract and cross-validate FAQ signals.

    Args:
        schema_pairs: FAQ Q&A pairs already parsed from structured data
                      (output of extraction/schema.py faq_schema_pairs).
        page_data:    document from seo_page_data / seo_ai_visibility.

    Returns:
        FAQ sub-document for ai_pages.
    """
    # Visible FAQ pairs — sourced from the scraper's faq_metrics block.
    faq_metrics: dict = page_data.get("faq_metrics", {}) or {}
    visible_pairs: list[dict] = faq_metrics.get("qa_pairs", []) or []

    # Cross-match: for every schema pair, find a matching visible pair.
    mismatches: list[str] = []
    matched_count = 0

    for sp in schema_pairs:
        schema_q = sp.get("question", "")
        found = any(
            _questions_match(schema_q, vp.get("question", ""))
            for vp in visible_pairs
        )
        if found:
            matched_count += 1
        else:
            mismatches.append(schema_q)

    schema_count  = len(schema_pairs)
    visible_count = len(visible_pairs)
    all_matched   = schema_count > 0 and matched_count == schema_count

    return {
        "schema_qa_count":  schema_count,
        "visible_qa_count": visible_count,
        "matched_count":    matched_count,
        "mismatches":       mismatches,
        "all_matched":      all_matched,
        "schema_present":   schema_count > 0,
        "visible_present":  visible_count > 0,
    }
