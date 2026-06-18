"""
Structure extractors: navigation, headings, paragraphs.

NAVIGATION (fixes "has_navigation true/false conflict" and "navigation_links=0
despite visible nav")
----------------------------------------------------------------------------
There were two independent nav detectors disagreeing (selector-based vs raw
`<nav>`), and link counting used a different container than detection. Here a
SINGLE detector finds the nav container (semantic -> ARIA -> builder heuristics)
and counts links INSIDE THAT SAME container, so presence and link-count are always
consistent. Works on the immutable master tree (chrome intact).
"""

from __future__ import annotations

import re
from typing import List, Optional

from bs4.element import Tag

from ..rendered_dom import RenderedDOM
from ..signals import NavigationSignals, HeadingSignals, ParagraphSignals

# Builder/CMS nav containers that don't use semantic <nav> (Elementor, Divi, Bootstrap, WP).
_NAV_CLASS_HINTS = (
    "elementor-nav-menu", "pxl-nav-menu", "pxl-header-nav", "nav-menu",
    "navbar-nav", "main-menu", "primary-menu", "menu-primary", "site-navigation",
    "navigation", "navbar", "menu",
)
_HEADER_CLASS_HINTS = ("site-header", "page-header", "masthead", "elementor-location-header", "header")
_FOOTER_CLASS_HINTS = ("site-footer", "page-footer", "elementor-location-footer", "footer", "colophon")

_QUESTION_WORDS = ("what", "how", "why", "when", "where", "which", "who", "can", "do", "does", "is", "are")


def _links_in(container: Optional[Tag]) -> int:
    if container is None:
        return 0
    return len(container.find_all("a", href=True))


def _find_nav_container(dom: RenderedDOM):
    """Return (container, method). Priority: semantic <nav> -> ARIA -> class heuristic.

    A class-heuristic container only counts if it actually holds a cluster of
    links (>=2) — that is what distinguishes a real menu from a stray .menu class.
    """
    nav = dom.find("nav")
    if nav is not None:
        return nav, "semantic"

    aria_nav = dom.select_one('[role="navigation"]')
    if aria_nav is not None:
        return aria_nav, "aria"

    for hint in _NAV_CLASS_HINTS:
        for el in dom.select(f'[class*="{hint}"]'):
            if len(el.find_all("a", href=True)) >= 2:
                return el, "class_heuristic"

    # Last resort: a <ul> of links living inside a header (classic menu pattern).
    header = dom.find("header")
    if header is not None:
        for ul in header.find_all("ul"):
            if len(ul.find_all("a", href=True)) >= 3:
                return ul, "class_heuristic"
    return None, ""


def _find_footer(dom: RenderedDOM) -> Optional[Tag]:
    footer = dom.find("footer")
    if footer is not None:
        return footer
    aria = dom.select_one('[role="contentinfo"]')
    if aria is not None:
        return aria
    for hint in _FOOTER_CLASS_HINTS:
        el = dom.select_one(f'[class*="{hint}"]')
        if el is not None:
            return el
    return None


def _find_header(dom: RenderedDOM) -> Optional[Tag]:
    header = dom.find("header")
    if header is not None:
        return header
    aria = dom.select_one('[role="banner"]')
    if aria is not None:
        return aria
    for hint in _HEADER_CLASS_HINTS:
        el = dom.select_one(f'[class*="{hint}"]')
        if el is not None:
            return el
    return None


def extract_navigation(dom: RenderedDOM) -> NavigationSignals:
    nav_container, method = _find_nav_container(dom)
    header = _find_header(dom)
    footer = _find_footer(dom)

    nav_link_count = _links_in(nav_container)
    evidence: List[str] = []
    if nav_container is not None:
        evidence.append(f"nav<{nav_container.name}> via {method}")
    if header is not None:
        evidence.append(f"header<{header.name}>")
    if footer is not None:
        evidence.append(f"footer<{footer.name}>")

    return NavigationSignals(
        has_navigation=nav_container is not None and nav_link_count >= 2,
        has_header=header is not None,
        has_footer=footer is not None,
        nav_link_count=nav_link_count,
        footer_link_count=_links_in(footer),
        primary_nav_items=nav_link_count,
        detection_method=method,
        evidence=evidence,
    )


def _heading_text_is_question(text: str) -> bool:
    t = text.strip().lower()
    if not t:
        return False
    if t.endswith("?"):
        return True
    first = t.split()[0] if t.split() else ""
    return first in _QUESTION_WORDS


def extract_headings(dom: RenderedDOM) -> HeadingSignals:
    counts = {f"h{i}": 0 for i in range(1, 7)}
    sequence: List[int] = []
    question_count = 0

    for tag in dom.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        level = int(tag.name[1])
        counts[tag.name] += 1
        sequence.append(level)
        if _heading_text_is_question(tag.get_text()):
            question_count += 1

    # Hierarchy validity: never jump down more than one level (e.g., h2 -> h4).
    violations: List[str] = []
    prev = 0
    for lvl in sequence:
        if prev and lvl > prev + 1:
            violations.append(f"h{prev}->h{lvl}")
        prev = lvl

    return HeadingSignals(
        h1_count=counts["h1"], h2_count=counts["h2"], h3_count=counts["h3"],
        h4_count=counts["h4"], h5_count=counts["h5"], h6_count=counts["h6"],
        hierarchy_valid=len(violations) == 0,
        sequence_violations=violations,
        question_headings=question_count,
        multiple_h1=counts["h1"] > 1,
    )


def extract_paragraphs(dom: RenderedDOM) -> ParagraphSignals:
    """Paragraph metrics from main content (chrome excluded, via cached clone)."""
    paras = dom.content_paragraphs()
    lengths = []
    for p in paras:
        words = len(p.get_text(strip=True).split())
        if words > 0:
            lengths.append(words)

    if not lengths:
        return ParagraphSignals(count=len(paras))

    total = len(lengths)
    avg = sum(lengths) / total
    short = sum(1 for w in lengths if w <= 40) / total
    long_ = sum(1 for w in lengths if w > 120) / total
    return ParagraphSignals(
        count=len(paras),
        avg_length=round(avg, 2),
        short_paragraph_ratio=round(short, 3),
        long_paragraph_ratio=round(long_, 3),
    )
