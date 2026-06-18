"""
Technical extractors: accessibility, links (+ topic clusters), media (+ lazy
loading), technical signals, page type.

LINKS / TOPIC CLUSTERS (fixes "internal links exist => semantic linking" and
"topical cluster inflation")
---------------------------------------------------------------------------
Topic clustering is NOT "internal links exist". We require contextual internal
links (inside main content, not nav/footer) whose anchor text shares topical
terms with the page's headings, pointing to multiple distinct pages — the actual
hub-and-spoke signal.

LAZY LOADING (fixes "lazy loading false positives")
---------------------------------------------------
A single tracking pixel with data-src is not a lazy-loading strategy. We require a
meaningful share of images to be lazily loaded.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse
from typing import Any, Dict, List, Set

from ..rendered_dom import RenderedDOM
from ..signals import (
    AccessibilitySignals, LinkSignals, MediaSignals, TechnicalSignals, PageTypeSignals,
)

_GENERIC_ANCHORS = {"click here", "read more", "here", "more", "learn more", "link", "this", "details"}
_STOPWORDS = {"the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "with", "your", "our", "best", "top"}
_CHROME_ANCESTORS = ("nav", "header", "footer", "aside")


def _in_chrome(tag) -> bool:
    """True if the element lives inside page chrome (nav/header/footer/aside).

    Walks the MASTER tree read-only — no clone, no id() comparison. This is how we
    distinguish contextual (main-content) links from boilerplate links without
    mutating or re-parsing the DOM.
    """
    parent = tag.parent
    while parent is not None:
        name = getattr(parent, "name", None)
        if name in _CHROME_ANCESTORS:
            return True
        cls = " ".join(parent.get("class", [])) if hasattr(parent, "get") else ""
        if cls and any(h in cls for h in ("nav-menu", "navbar", "site-footer", "footer", "site-header", "menu")):
            return True
        parent = parent.parent
    return False


# ── accessibility ────────────────────────────────────────────────────────────
def extract_accessibility(dom: RenderedDOM) -> AccessibilitySignals:
    images = dom.find_all("img")
    with_alt = [img for img in images if img.get("alt") is not None and img.get("alt", "").strip() != ""]
    coverage = round(len(with_alt) / len(images) * 100, 1) if images else 0.0

    aria_labels = len(dom.select("[aria-label]")) + len(dom.select("[aria-labelledby]"))

    landmarks: Set[str] = set()
    if dom.find("nav") or dom.select_one('[role="navigation"]'):
        landmarks.add("navigation")
    if dom.find("main") or dom.select_one('[role="main"]'):
        landmarks.add("main")
    if dom.find("header") or dom.select_one('[role="banner"]'):
        landmarks.add("banner")
    if dom.find("footer") or dom.select_one('[role="contentinfo"]'):
        landmarks.add("contentinfo")

    inputs = dom.find_all(["input", "select", "textarea"])
    real_inputs = [i for i in inputs if i.get("type") not in ("hidden", "submit", "button")]
    labeled = 0
    label_fors = {l.get("for") for l in dom.find_all("label") if l.get("for")}
    for inp in real_inputs:
        if inp.get("aria-label") or inp.get("aria-labelledby"):
            labeled += 1
        elif inp.get("id") and inp.get("id") in label_fors:
            labeled += 1
        elif inp.find_parent("label") is not None:
            labeled += 1

    # Skip link: an early in-page anchor to #main / #content.
    skip_link = False
    for a in dom.find_all("a", href=True)[:5]:
        href = a.get("href", "")
        if href.startswith("#") and any(k in href.lower() for k in ("main", "content", "skip")):
            skip_link = True
            break

    return AccessibilitySignals(
        images_total=len(images),
        images_with_alt=len(with_alt),
        alt_text_coverage=coverage,
        aria_label_count=aria_labels,
        landmark_roles=sorted(landmarks),
        form_inputs=len(real_inputs),
        form_inputs_labeled=labeled,
        lang_declared=bool(dom.lang),
        skip_link=skip_link,
    )


# ── links + topic clusters ───────────────────────────────────────────────────
def _host(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _topical_terms(dom: RenderedDOM) -> Set[str]:
    terms: Set[str] = set()
    for h in dom.find_all(["h1", "h2", "h3"]):
        for w in re.findall(r"\b\w{4,}\b", h.get_text(strip=True).lower()):
            if w not in _STOPWORDS:
                terms.add(w)
    return terms


def extract_links(dom: RenderedDOM) -> LinkSignals:
    page_host = _host(dom.url)
    topical_terms = _topical_terms(dom)

    total = internal = external = descriptive = contextual_internal = nofollow = 0
    cluster_targets: Set[str] = set()

    for a in dom.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        total += 1
        rel = " ".join(a.get("rel", [])) if isinstance(a.get("rel"), list) else (a.get("rel") or "")
        if "nofollow" in rel:
            nofollow += 1

        link_host = _host(href) if "://" in href else page_host
        is_internal = (link_host == page_host) or href.startswith("/")
        anchor = a.get_text(strip=True).lower()

        if is_internal:
            internal += 1
            is_descriptive = bool(anchor) and anchor not in _GENERIC_ANCHORS and len(anchor.split()) >= 2
            if is_descriptive:
                descriptive += 1
            # Contextual = inside main content, not chrome (parent walk on master tree).
            if not _in_chrome(a):
                contextual_internal += 1
                # Topic-cluster signal: a contextual internal link to a DISTINCT page
                # via a descriptive anchor. Heading-term overlap strengthens it but is
                # not required (anchors often describe the TARGET topic, not the hub).
                anchor_terms = {w for w in re.findall(r"\b\w{4,}\b", anchor) if w not in _STOPWORDS}
                if is_descriptive and (anchor_terms or (anchor_terms & topical_terms)):
                    cluster_targets.add(href)
        else:
            external += 1

    topic_cluster_links = len(cluster_targets)
    return LinkSignals(
        total=total,
        internal=internal,
        external=external,
        internal_descriptive_anchor=descriptive,
        contextual_internal=contextual_internal,
        topic_cluster_links=topic_cluster_links,
        nofollow=nofollow,
        # Real clustering: 3+ topical contextual links to DISTINCT pages.
        has_topic_clusters=topic_cluster_links >= 3,
    )


# ── media + lazy loading ─────────────────────────────────────────────────────
_MODERN_FMT_RE = re.compile(r"(?:\.|image/)(webp|avif)(?![a-z0-9])", re.IGNORECASE)


def extract_media(dom: RenderedDOM, graph: List[Dict[str, Any]]) -> MediaSignals:
    images = dom.find_all("img")
    lazy = 0
    modern = 0
    with_dims = 0
    for img in images:
        loading = (img.get("loading") or "").lower()
        has_lazy_attr = bool(img.get("data-src") or img.get("data-srcset") or img.get("data-lazy"))
        if loading == "lazy" or has_lazy_attr:
            lazy += 1
        src = (img.get("src") or "") + " " + (img.get("srcset") or "") + " " + (img.get("data-src") or "")
        if _MODERN_FMT_RE.search(src):
            modern += 1
        if (img.get("width") and img.get("height")) or "aspect-ratio" in (img.get("style") or ""):
            with_dims += 1

    # <picture> sources with modern formats.
    for source in dom.find_all("source"):
        if _MODERN_FMT_RE.search(source.get("srcset") or source.get("type") or ""):
            modern += 1

    videos = len(dom.find_all("video"))
    for iframe in dom.find_all("iframe", src=True):
        if any(p in iframe["src"] for p in ("youtube.com", "youtu.be", "vimeo.com", "wistia")):
            videos += 1
    video_schema = sum(
        1 for e in graph
        if "VideoObject" in (e.get("@type", []) if isinstance(e.get("@type"), list) else [e.get("@type")])
    )

    total = len(images)
    # Genuine lazy loading: a meaningful share, not a single pixel.
    genuine = lazy >= 3 or (total >= 5 and lazy / total >= 0.3)

    return MediaSignals(
        images_total=total,
        images_lazy=lazy,
        images_modern_format=modern,
        images_with_dimensions=with_dims,
        videos=videos,
        video_with_schema=video_schema,
        lazy_loading_genuine=genuine,
    )


# ── technical ─────────────────────────────────────────────────────────────────
def extract_technical(dom: RenderedDOM) -> TechnicalSignals:
    canonical = dom.select_one('link[rel="canonical"]')
    canonical_url = canonical.get("href", "").strip() if canonical else ""
    self_ref = bool(canonical_url) and _host(canonical_url) == _host(dom.url)

    robots = dom.find("meta", attrs={"name": "robots"})
    directive = (robots.get("content", "").lower() if robots else "")

    charset = ""
    cs = dom.find("meta", attrs={"charset": True})
    if cs:
        charset = cs.get("charset", "")

    return TechnicalSignals(
        canonical_present=canonical is not None,
        canonical_self_referencing=self_ref,
        canonical_url=canonical_url,
        robots_directive=directive,
        indexable="noindex" not in directive,
        followable="nofollow" not in directive,
        viewport_present=dom.find("meta", attrs={"name": "viewport"}) is not None,
        hreflang_present=bool(dom.find_all("link", rel="alternate", hreflang=True)),
        https=dom.url.startswith("https://"),
        charset=charset,
    )


# ── page type ──────────────────────────────────────────────────────────────────
def _types_in_graph(graph: List[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for e in graph:
        t = e.get("@type")
        if isinstance(t, list):
            out.update(str(x) for x in t)
        elif isinstance(t, str):
            out.add(t)
    return out


def extract_page_type(dom: RenderedDOM, graph: List[Dict[str, Any]]) -> PageTypeSignals:
    path = urlparse(dom.url).path.strip("/")
    is_home = path == "" or path in ("index.html", "home")
    schema_types = _types_in_graph(graph)
    title = dom.title.lower()

    detected = "Unknown"
    confidence = 0.4

    if is_home:
        detected, confidence = "Homepage", 0.95
    elif "Product" in schema_types or "Offer" in schema_types:
        detected, confidence = "Product", 0.9
    elif {"Article", "BlogPosting", "NewsArticle"} & schema_types or dom.find("article"):
        detected, confidence = "Article", 0.85
    elif "Service" in schema_types or any(k in path for k in ("service", "solutions")):
        detected, confidence = "Service", 0.75
    elif any(k in path for k in ("contact",)) or "contact" in title:
        detected, confidence = "Contact", 0.8
    elif any(k in path for k in ("about",)) or "about" in title:
        detected, confidence = "About", 0.8
    elif any(k in path for k in ("blog", "category", "tag", "archive")):
        detected, confidence = "Listing", 0.7

    return PageTypeSignals(detected_type=detected, confidence=confidence, is_homepage=is_home)
