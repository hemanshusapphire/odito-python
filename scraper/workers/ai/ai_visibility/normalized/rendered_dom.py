"""
RenderedDOM — immutable, parse-once DOM abstraction.

ROOT PROBLEM THIS FIXES
-----------------------
The legacy worker parsed the HTML 2+ times and then ran:

    for element in soup(['script','style','nav','footer','header','aside']):
        element.decompose()

against the *shared* soup mid-pipeline. Every extractor that ran afterwards saw
a mutilated tree, producing fake navigation/footer/link/accessibility issues and
inconsistent paragraph counts.

CONTRACT
--------
* The page is parsed exactly once into an immutable master tree.
* Extractors NEVER mutate the master tree. They either:
    - traverse it read-only (chrome-aware extractors: navigation, links, a11y), or
    - use `content_soup()` — a cached clone with chrome/script/style removed
      (content/paragraph/text extractors), or
    - call `clone()` for the rare case that genuinely needs a private mutable tree.
* Expensive derived artifacts (visible text, content text, <p> list) are computed
  once and cached.

The class is intentionally framework-agnostic: it wraps already-rendered HTML
(seo_page_data.raw_html, produced by the Page Scraper's fetch_html() which uses
Playwright/Selenium for SPA/Shopify/Next/React). It does no fetching itself.
"""

from __future__ import annotations

import re
import copy
from functools import lru_cache
from typing import List, Optional

from bs4 import BeautifulSoup
from bs4.element import Tag

# Elements that are page "chrome" rather than primary content. Removed (on a
# clone only) when computing main content / paragraph / readability metrics.
CHROME_TAGS = ("script", "style", "noscript", "template", "nav", "header", "footer", "aside")
# Non-rendered tags stripped for any visible-text computation.
INVISIBLE_TAGS = ("script", "style", "noscript", "template")

_WS_RE = re.compile(r"\s+")


class RenderedDOM:
    """Immutable wrapper around a parsed, already-rendered HTML document."""

    __slots__ = (
        "url",
        "html",
        "_soup",
        "_content_soup",
        "_visible_text",
        "_content_text",
        "_paragraphs",
        "_node_count",
        "_parser",
    )

    def __init__(self, html: str, url: str = "", parser: str = "html.parser"):
        if not isinstance(html, str):
            raise TypeError(f"RenderedDOM expects html as str, got {type(html)}")
        self.url = url or ""
        self.html = html
        self._parser = parser
        # Single parse of the master tree.
        self._soup = BeautifulSoup(html, parser)
        # Lazily computed, cached artifacts.
        self._content_soup: Optional[BeautifulSoup] = None
        self._visible_text: Optional[str] = None
        self._content_text: Optional[str] = None
        self._paragraphs: Optional[List[Tag]] = None
        self._node_count: Optional[int] = None

    # ── read-only access to the master tree ────────────────────────────────
    @property
    def soup(self) -> BeautifulSoup:
        """The master parsed tree. READ-ONLY — callers must not mutate it."""
        return self._soup

    def find(self, *args, **kwargs):
        return self._soup.find(*args, **kwargs)

    def find_all(self, *args, **kwargs):
        return self._soup.find_all(*args, **kwargs)

    def select(self, selector: str):
        try:
            return self._soup.select(selector)
        except Exception:
            # Invalid/unsupported CSS selector — never blow up an extractor.
            return []

    def select_one(self, selector: str):
        try:
            return self._soup.select_one(selector)
        except Exception:
            return None

    def exists(self, selector: str) -> bool:
        return bool(self.select(selector))

    # ── document-level helpers ──────────────────────────────────────────────
    @property
    def title(self) -> str:
        t = self._soup.find("title")
        return (t.get_text(strip=True) if t else "") or ""

    def meta_content(self, name: str) -> str:
        tag = self._soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag.get("content", "").strip()
        return ""

    @property
    def lang(self) -> str:
        html_tag = self._soup.find("html")
        return (html_tag.get("lang", "") if html_tag else "") or ""

    @property
    def node_count(self) -> int:
        if self._node_count is None:
            self._node_count = len(self._soup.find_all())
        return self._node_count

    @property
    def body(self) -> Optional[Tag]:
        return self._soup.find("body")

    # ── cached text artifacts ───────────────────────────────────────────────
    def visible_text(self) -> str:
        """All visible text (scripts/styles removed), computed once.

        Non-destructive: works on a clone, never the master tree.
        """
        if self._visible_text is None:
            clone = self._clone()
            for el in clone(INVISIBLE_TAGS):
                el.decompose()
            self._visible_text = _WS_RE.sub(" ", clone.get_text(separator=" ", strip=True))
        return self._visible_text

    def content_soup(self) -> BeautifulSoup:
        """A cached clone with chrome (nav/header/footer/aside) + scripts removed.

        This is the ONE place chrome stripping happens, and it happens on a clone,
        so the master tree keeps its nav/footer/header for chrome-aware extractors.
        """
        if self._content_soup is None:
            clone = self._clone()
            for el in clone(CHROME_TAGS):
                el.decompose()
            self._content_soup = clone
        return self._content_soup

    def content_text(self) -> str:
        """Visible text of the main content (chrome removed), computed once."""
        if self._content_text is None:
            self._content_text = _WS_RE.sub(
                " ", self.content_soup().get_text(separator=" ", strip=True)
            )
        return self._content_text

    def content_paragraphs(self) -> List[Tag]:
        """<p> elements within main content (chrome removed), computed once."""
        if self._paragraphs is None:
            self._paragraphs = self.content_soup().find_all("p")
        return self._paragraphs

    # ── cloning ──────────────────────────────────────────────────────────────
    def clone(self) -> BeautifulSoup:
        """Return a fresh, private, mutable copy of the master tree.

        Use only when an extractor genuinely needs to mutate. The master tree is
        never affected.
        """
        return self._clone()

    def _clone(self) -> BeautifulSoup:
        try:
            return copy.copy(self._soup)
        except Exception:
            # Fallback: re-parse. Costs one extra parse but guarantees isolation.
            return BeautifulSoup(self.html, self._parser)

    def __repr__(self) -> str:
        return f"RenderedDOM(url={self.url!r}, nodes={self.node_count})"
