"""
Normalizer — orchestrates extract-once and assembles the single NormalizedSignals
object for a page.

Each metric extractor runs exactly once. The DOM is parsed once (RenderedDOM) and
the JSON-LD graph is parsed once and shared with the entity/FAQ/media/page-type
extractors. No extractor re-parses HTML or recomputes another's metric.
"""

from __future__ import annotations

from typing import Optional

from .rendered_dom import RenderedDOM
from .signals import NormalizedSignals, validate_signals
from .extractors.schema_signals import extract_schema
from .extractors.structure import extract_navigation, extract_headings, extract_paragraphs
from .extractors.content import extract_content, extract_faq, extract_entities
from .extractors.technical import (
    extract_accessibility, extract_links, extract_media, extract_technical, extract_page_type,
)


def extract_normalized_signals(
    html: str,
    url: str = "",
    http_status_code: int = 200,
    response_time_ms: int = 0,
) -> NormalizedSignals:
    """Build the NormalizedSignals for a single page from already-rendered HTML.

    Fault tolerant: any single extractor failure is recorded as a warning and the
    rest of the signals are still produced.
    """
    signals = NormalizedSignals(
        url=url,
        http_status_code=http_status_code,
        response_time_ms=response_time_ms,
        html_size_bytes=len(html.encode("utf-8", errors="ignore")),
    )

    dom = RenderedDOM(html, url)

    def _run(name: str, fn):
        try:
            return fn()
        except Exception as exc:  # never let one extractor break the rest
            signals.extraction_warnings.append(f"{name}: {exc}")
            return None

    # Parse schema ONCE; the flattened graph is reused everywhere.
    schema_result = _run("schema", lambda: extract_schema(dom))
    if schema_result is not None:
        signals.schema, graph = schema_result
    else:
        graph = []

    nav = _run("navigation", lambda: extract_navigation(dom))
    if nav is not None:
        signals.navigation = nav

    headings = _run("headings", lambda: extract_headings(dom))
    if headings is not None:
        signals.headings = headings

    paragraphs = _run("paragraphs", lambda: extract_paragraphs(dom))
    if paragraphs is not None:
        signals.paragraphs = paragraphs

    content = _run("content", lambda: extract_content(dom))
    if content is not None:
        signals.content = content

    faq = _run("faq", lambda: extract_faq(dom, graph))
    if faq is not None:
        signals.faq = faq

    entities = _run("entities", lambda: extract_entities(dom, graph, signals.content.word_count))
    if entities is not None:
        signals.entities = entities

    accessibility = _run("accessibility", lambda: extract_accessibility(dom))
    if accessibility is not None:
        signals.accessibility = accessibility

    links = _run("links", lambda: extract_links(dom))
    if links is not None:
        signals.links = links

    media = _run("media", lambda: extract_media(dom, graph))
    if media is not None:
        signals.media = media

    technical = _run("technical", lambda: extract_technical(dom))
    if technical is not None:
        signals.technical = technical

    page_type = _run("page_type", lambda: extract_page_type(dom, graph))
    if page_type is not None:
        signals.page_type = page_type

    signals.extraction_warnings.extend(validate_signals(signals))
    return signals
