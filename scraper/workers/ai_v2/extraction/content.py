"""
Extraction: content signals for AEO rules.

Reads the body text, headings, lists, tables, and prose quality signals from
the page scraping result (seo_page_data / seo_ai_visibility).

All output stored under ai_pages["content"].
"""

import re
from typing import Any


# Common AI filler phrases (lowercase, stripped).
_FILLER_PHRASES: list[str] = [
    "in today's digital landscape",
    "in today's fast-paced world",
    "it's important to note that",
    "it is important to note that",
    "it goes without saying",
    "as we have explored",
    "in conclusion",
    "in summary",
    "to summarize",
    "needless to say",
    "without further ado",
    "delve into",
    "dive deep into",
    "at the end of the day",
    "all things considered",
    "as previously mentioned",
    "in the digital age",
    "leverage",         # only when used generically
    "game-changer",
    "paradigm shift",
]

# Question word prefixes for H2 detection.
_QUESTION_WORDS = re.compile(
    r"^(who|what|when|where|why|how|is|are|can|does|should|will|do|has|have)\b",
    re.IGNORECASE,
)

# Signals that a page is instructional/process-type.
_PROCESS_SIGNALS = re.compile(
    r"\bstep\s+\d+\b|how\s+to\b|instructions?\b",
    re.IGNORECASE,
)

# Authority domain patterns (.gov, .edu, and recognised industry authorities).
_AUTHORITY_DOMAIN = re.compile(
    r"https?://[^/\s]*\.(?:gov|edu)(?:/|$)"
    r"|https?://(?:www\.)?(?:wikipedia|w3|ieee|acm|who|schema)\.org(?:/|$)"
    r"|https?://developer\.mozilla\.org(?:/|$)"
    r"|https?://developers\.google\.com(?:/|$)"
    r"|https?://web\.dev(?:/|$)",
    re.IGNORECASE,
)


def _count_words(text: str) -> int:
    return len(text.split()) if text else 0


def _first_n_words(text: str, n: int) -> str:
    return " ".join(text.split()[:n])


def _is_question_h2(heading: str) -> bool:
    stripped = heading.strip().rstrip("?")
    return bool(_QUESTION_WORDS.match(heading.strip())) or heading.strip().endswith("?")


def extract_content(page_data: dict[str, Any]) -> dict[str, Any]:
    """
    Extract content-layer signals from page scraping data.

    Args:
        page_data: document from seo_page_data or seo_ai_visibility.

    Returns:
        Content sub-document for ai_pages.
    """
    # ── Body text ─────────────────────────────────────────────────────────────
    body_text: str = (
        page_data.get("main_content", {}) or {}
    ).get("text", "") or page_data.get("body_text", "") or ""

    word_count       = _count_words(body_text)
    first_60_words   = _first_n_words(body_text, 60)
    first_100_words  = _first_n_words(body_text, 100)

    # Direct answer detection: first 60 words contains a declarative sentence
    # (ends with a period and has a verb-like structure; not purely introductory).
    intro_phrases = re.compile(
        r"^(welcome|in this|today we|this article|this guide|this post|"
        r"are you|have you|if you|let'?s)",
        re.IGNORECASE,
    )
    has_direct_answer = bool(
        re.search(
            r"\b(is|are|was|were|can|does|do|means?|refers?\s+to|"
            r"helps?|provides?|enables?|allows?|lets?|gives?|offers?|delivers?)\b",
            first_60_words,
            re.I,
        )
        and not intro_phrases.match(first_60_words.strip())
    )

    # Intro word count: words before the first declarative sentence.
    first_sentence_end = re.search(r"[.!?]", body_text)
    intro_word_count = (
        _count_words(body_text[: first_sentence_end.start()])
        if first_sentence_end
        else word_count
    )

    # ── Headings ──────────────────────────────────────────────────────────────
    heading_metrics: dict = page_data.get("heading_metrics", {}) or {}
    h1_list: list[str]   = heading_metrics.get("h1", []) or []
    h2_list: list[str]   = heading_metrics.get("h2", []) or []

    question_h2_count = sum(1 for h in h2_list if _is_question_h2(h))
    total_h2          = len(h2_list)
    question_ratio    = round(question_h2_count / total_h2, 3) if total_h2 else 0.0

    # ── Lists ─────────────────────────────────────────────────────────────────
    content_metrics: dict = page_data.get("content_metrics", {}) or {}
    raw_lists: list = content_metrics.get("lists", []) or []
    # A qualifying list has ≥ 3 items.
    qualifying_lists = [lst for lst in raw_lists if isinstance(lst, dict) and lst.get("item_count", 0) >= 3]

    # ── Tables ────────────────────────────────────────────────────────────────
    raw_tables: list = content_metrics.get("tables", []) or []
    qualifying_tables = [
        t for t in raw_tables
        if isinstance(t, dict)
        and t.get("columns", 0) >= 2
        and t.get("rows", 0) >= 3
    ]

    # ── Filler phrases ────────────────────────────────────────────────────────
    body_lower = body_text.lower()
    found_fillers = [phrase for phrase in _FILLER_PHRASES if phrase in body_lower]

    # ── External links ────────────────────────────────────────────────────────
    external_links: list = content_metrics.get("external_links", []) or []
    authority_links = [
        lnk for lnk in external_links
        if isinstance(lnk, dict) and _AUTHORITY_DOMAIN.search(lnk.get("href", ""))
    ]

    # ── Internal links ────────────────────────────────────────────────────────
    internal_links: list = content_metrics.get("internal_links", []) or []

    # ── Process page detection ────────────────────────────────────────────────
    h1_text = " ".join(h1_list)
    is_process_page = bool(
        _PROCESS_SIGNALS.search(h1_text) or _PROCESS_SIGNALS.search(first_100_words)
    )

    # ── Comparison intent ─────────────────────────────────────────────────────
    comparison_keywords = re.compile(r"\bvs\.?\b|\bcompare\b|\bdifference between\b", re.I)
    has_comparison_intent = bool(comparison_keywords.search(body_text))

    return {
        "word_count":          word_count,
        "first_60_words":      first_60_words,
        "first_100_words":     first_100_words,
        "has_direct_answer":   has_direct_answer,
        "intro_word_count":    intro_word_count,
        "h1":                  h1_list,
        "h2_headings":         h2_list,
        "total_h2":            total_h2,
        "question_h2_count":   question_h2_count,
        "question_h2_ratio":   question_ratio,
        "lists": {
            "total":            len(raw_lists),
            "qualifying":       len(qualifying_lists),
            "qualifying_items": qualifying_lists,
        },
        "tables": {
            "total":            len(raw_tables),
            "qualifying":       len(qualifying_tables),
        },
        "filler_phrases": {
            "count":   len(found_fillers),
            "found":   found_fillers,
        },
        "external_links": {
            "total":     len(external_links),
            "authority": len(authority_links),
            "authority_examples": [lnk.get("href") for lnk in authority_links[:3]],
        },
        "internal_links":        internal_links,
        "internal_link_count":   len(internal_links),
        "is_process_page":       is_process_page,
        "has_comparison_intent": has_comparison_intent,
    }
