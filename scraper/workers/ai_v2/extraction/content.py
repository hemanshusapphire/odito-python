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

# Question word prefixes for H2 detection — expanded to cover which/was/were/did/would/could/might.
_QUESTION_WORDS = re.compile(
    r"^(who|what|when|where|why|how|which|is|are|was|were|can|does|should|"
    r"will|do|has|have|did|would|could|might)\b",
    re.IGNORECASE,
)

# Signals that a page is instructional/process-type.
_PROCESS_SIGNALS = re.compile(
    r"\bstep\s+\d+\b|how\s+to\b|instructions?\b",
    re.IGNORECASE,
)

# Authority domain patterns.
# Bugs fixed:
#   - who.org → who.int (World Health Organization is at who.int, not who.org)
#   - Removed schema.org (vocabulary reference, not a citable authority source)
#   - Added ietf.org, iso.org, ncbi.nlm.nih.gov, nature.com, springer.com,
#     plos.org, reuters.com, apnews.com
_AUTHORITY_DOMAIN = re.compile(
    # Government and educational institutions
    r"https?://[^/\s]*\.(?:gov|edu)(?:/|$)"
    # Standards bodies
    r"|https?://(?:www\.)?(?:w3|ieee|acm)\.org(?:/|$)"
    r"|https?://(?:www\.)?ietf\.org(?:/|$)"
    r"|https?://(?:www\.)?iso\.org(?:/|$)"
    # Knowledge bases
    r"|https?://(?:www\.)?wikipedia\.org(?:/|$)"
    # International health authority — who.INT not who.org
    r"|https?://(?:www\.)?who\.int(?:/|$)"
    r"|https?://(?:www\.)?ncbi\.nlm\.nih\.gov(?:/|$)"
    # Major scientific publishers
    r"|https?://(?:www\.)?nature\.com(?:/|$)"
    r"|https?://(?:www\.)?springer\.com(?:/|$)"
    r"|https?://(?:journals\.)?plos\.org(?:/|$)"
    # Developer documentation
    r"|https?://developer\.mozilla\.org(?:/|$)"
    r"|https?://developers\.google\.com(?:/|$)"
    r"|https?://web\.dev(?:/|$)"
    # Globally recognized wire services
    r"|https?://(?:www\.)?reuters\.com(?:/|$)"
    r"|https?://(?:www\.)?apnews\.com(?:/|$)",
    re.IGNORECASE,
)

# Generic introductory openers that signal scene-setting, NOT a direct answer.
_INTRO_STARTERS = re.compile(
    r"^(welcome|in this|today we|this article|this guide|this post|this page|"
    r"are you|have you|if you|let'?s|in the following|in order to|"
    r"we will|you will|by the end|read on|in this blog|join us|"
    r"whether you|looking for|searching for)",
    re.IGNORECASE,
)

# Vague brand/marketing openers that indicate no direct answer is being given.
_VAGUE_OPENERS = re.compile(
    r"^(we are (?:committed|dedicated|passionate|here to|excited|proud|pleased|happy)|"
    r"we believe|we strive|we aim|our mission is|our goal is|our vision|our commitment|"
    r"as (?:a |the |an )?leading|with (?:years|decades|over \d+) of experience|"
    r"at [A-Z][a-z]+,\s+we)",
    re.IGNORECASE,
)

# Substantive claim patterns — identity/definition, factual specifics, location, price.
# Used by both has_direct_answer and _measure_intro_depth.
_SUBSTANTIVE_CLAIM = re.compile(
    r"\b(?:is|are|was|were)\s+(?:a|an|the)\s+\w+"        # "X is a/an/the Y"
    r"|\b(?:means?|refers?\s+to|defined\s+as|also\s+known\s+as)\b"
    r"|\bstarting\s+(?:from|at)\s+[\$€£¥\d]"             # pricing
    r"|\blocated\s+(?:at|in|on)\b"                        # location
    r"|\b\d+\s+(?:years?|locations?|products?|services?|employees?|clients?|customers?)\b"
    r"|\bcontact\s+us\s+(?:at|by|on|via)\b"
    r"|\bopen\s+(?:\d|monday|tuesday|wednesday|thursday|friday|saturday|sunday)",
    re.IGNORECASE,
)

# Comparison intent — expanded beyond vs/compare/difference between.
_COMPARISON_INTENT = re.compile(
    r"\bvs\.?\b|\bversus\b|\bcompare\b|\bcomparison\b|\bcompared\s+(?:to|with)\b"
    r"|\bdifference\s+between\b|\bdifferences\b"
    r"|\balternative\s+to\b|\balternatives?\b"
    r"|\bpros?\s+(?:and\s+)?cons?\b|\badvantages?\b|\bdisadvantages?\b"
    r"|\bside[\s-]by[\s-]side\b|\bfeature\s+matrix\b"
    r"|\bbetter\s+than\b|\bworse\s+than\b"
    r"|\bwhich\s+(?:is|are)\s+(?:better|best|worse|right)\b"
    r"|\btop\s+\d+\b|\bbest\s+\w+\s+for\b|\branking\b",
    re.IGNORECASE,
)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def _count_words(text: str) -> int:
    return len(text.split()) if text else 0


def _first_n_words(text: str, n: int) -> str:
    return " ".join(text.split()[:n])


def _is_question_h2(heading: str) -> bool:
    h = heading.strip()
    return bool(_QUESTION_WORDS.match(h)) or h.endswith("?")


def _measure_intro_depth(body_text: str) -> int:
    """
    Count words before the first sentence that contains a substantive claim.

    Walks the body text sentence by sentence (split at [.!?] boundaries) and
    accumulates word counts until a sentence matches _SUBSTANTIVE_CLAIM, which
    indicates the page has started delivering real information.

    Capped at 300 words so the loop stays bounded on long pages with no
    substantive opening.  Returns the total accumulated word count if no
    substantive sentence is found within the window.
    """
    sentences = _SENTENCE_SPLIT.split(body_text[:2000])
    words_seen = 0
    for sentence in sentences:
        if _SUBSTANTIVE_CLAIM.search(sentence):
            return words_seen
        words_seen += _count_words(sentence)
        if words_seen >= 300:
            break
    return words_seen


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

    word_count      = _count_words(body_text)
    first_60_words  = _first_n_words(body_text, 60)
    first_100_words = _first_n_words(body_text, 100)

    # Direct answer detection: first 60 words must contain a substantive claim
    # AND must not open with a generic intro or vague brand statement.
    # The old approach (any verb + not intro phrase) was too broad — nearly every
    # page contains "is/are" and doesn't start with "welcome", producing near-
    # universal false positives.
    has_direct_answer = (
        not _INTRO_STARTERS.match(first_60_words.strip())
        and not _VAGUE_OPENERS.match(first_60_words.strip())
        and bool(_SUBSTANTIVE_CLAIM.search(first_60_words))
    )

    # Intro depth: sentence-by-sentence word count before the first substantive
    # sentence.  Old approach (words before first [.!?]) was unreliable because
    # a single long sentence without punctuation returned word_count, and a
    # one-word opener returned 1 regardless of actual answer depth.
    intro_word_count = _measure_intro_depth(body_text)

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
    qualifying_lists = [
        lst for lst in raw_lists
        if isinstance(lst, dict) and lst.get("item_count", 0) >= 3
    ]

    # ── Tables ────────────────────────────────────────────────────────────────
    raw_tables: list = content_metrics.get("tables", []) or []
    # Require has_headers=True to exclude layout tables (set by _enrich_from_raw_html
    # based on presence of <th> elements).
    qualifying_tables = [
        t for t in raw_tables
        if isinstance(t, dict)
        and t.get("columns", 0) >= 2
        and t.get("rows", 0) >= 3
        and t.get("has_headers", False)
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
    has_comparison_intent = bool(_COMPARISON_INTENT.search(body_text))

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
