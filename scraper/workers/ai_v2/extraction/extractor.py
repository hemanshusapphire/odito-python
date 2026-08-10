"""
Extraction orchestrator.

Reads from:
    seo_page_data         — page scraping result
    domain_technical_reports — robots.txt, sitemap, llms.txt

Assembles all sub-extractor outputs into a single ai_pages document and
upserts it into the ai_pages collection.

Data flow:
    Scraping → Extraction → ai_pages
"""

import sys, os, traceback
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse, urlunparse
from bson import ObjectId
from pymongo import ReturnDocument

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import ai_pages  # V2 collection declared in db.py

from .crawlability   import extract_crawlability
from .schema         import extract_schema
from .content        import extract_content
from .faq            import extract_faq
from .geo            import extract_geo
from .technical      import extract_technical
from .entities       import extract_entities
from .site_structure import extract_site_structure


def _normalize_url(url: str) -> str:
    """Lowercase hostname, strip trailing slash, drop fragment."""
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        path = p.path.rstrip("/") or "/"
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", p.query, ""))
    except Exception:
        return url.strip()


def _safe_extract(label: str, func: Callable, *args, url: str = "?", **kwargs) -> dict:
    """
    Call func(*args, **kwargs) and return {} on any exception.

    Logs the full traceback so the failure is visible without killing the page.
    """
    try:
        result = func(*args, **kwargs)
        return result if isinstance(result, dict) else {}
    except Exception as exc:
        print(f"[V2 EXTRACTOR] {label} failed for {url}: {exc}")
        print(traceback.format_exc())
        return {}


def _enrich_from_raw_html(page_data: dict[str, Any]) -> None:
    """
    Parse raw_html with BeautifulSoup and inject the signal keys that V2
    sub-extractors read but seo_page_data does not pre-store.

    Only populates keys that are absent; skips the expensive HTML parse when
    signals are already present (e.g., page_data came from seo_ai_visibility).
    Modifies page_data in-place.
    """
    raw_html = page_data.get("raw_html", "") or ""
    if not raw_html:
        return

    has_heading_metrics = bool((page_data.get("heading_metrics") or {}).get("h1") is not None)
    has_structured_data = page_data.get("structured_data") is not None
    has_body_text = bool(
        page_data.get("body_text")
        or (page_data.get("main_content") or {}).get("text")
    )
    has_content_metrics = bool(page_data.get("content_metrics"))

    if has_heading_metrics and has_structured_data and has_body_text and has_content_metrics:
        return

    try:
        import json
        import re
        from urllib.parse import urlparse
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw_html, "lxml")

        # ── heading_metrics: h1/h2/... as lists of text strings ──────────────
        if not has_heading_metrics:
            hm: dict[str, list[str]] = {}
            for lvl in ("h1", "h2", "h3", "h4", "h5", "h6"):
                hm[lvl] = [
                    tag.get_text(strip=True)
                    for tag in soup.find_all(lvl)
                    if tag.get_text(strip=True)
                ]
            page_data["heading_metrics"] = hm

        # ── body_text / main_content.text ────────────────────────────────────
        if not has_body_text:
            main = soup.find("main") or soup.find("article") or soup.find("body")
            body_text = main.get_text(separator=" ", strip=True) if main else ""
            page_data["body_text"] = body_text
            page_data.setdefault("main_content", {})["text"] = body_text

        # ── structured_data: list of JSON-LD dicts ───────────────────────────
        if not has_structured_data:
            sd: list[dict] = []
            for script in soup.find_all("script", {"type": "application/ld+json"}):
                try:
                    parsed = json.loads(script.string or "")
                    if isinstance(parsed, list):
                        sd.extend(parsed)
                    elif isinstance(parsed, dict):
                        sd.append(parsed)
                except (json.JSONDecodeError, TypeError):
                    pass
            page_data["structured_data"] = sd

        # ── content_metrics: lists, tables, links ────────────────────────────
        if not has_content_metrics:
            page_url = page_data.get("url", "") or ""
            base_domain = urlparse(page_url).netloc if page_url else ""

            _BOILERPLATE = {"nav", "footer", "header"}
            lists: list[dict] = []
            for tag in soup.find_all(["ul", "ol"]):
                # Skip navigation/chrome containers — they inflate qualifying list counts.
                if any(p.name in _BOILERPLATE for p in tag.parents):
                    continue
                items = [
                    li.get_text(strip=True)
                    for li in tag.find_all("li", recursive=False)
                    if li.get_text(strip=True)
                ]
                if items:
                    lists.append({"type": tag.name, "item_count": len(items), "items": items[:10]})

            tables: list[dict] = []
            for tbl in soup.find_all("table"):
                rows = tbl.find_all("tr")
                if rows:
                    col_count = max(
                        (len(r.find_all(["td", "th"])) for r in rows), default=0
                    )
                    # has_headers distinguishes data tables from layout tables;
                    # content.py uses this to exclude layout tables from qualifying_tables.
                    has_headers = bool(tbl.find("th"))
                    tables.append({"rows": len(rows), "columns": col_count, "has_headers": has_headers})

            external_links: list[dict] = []
            internal_links: list[dict] = []
            for a in soup.find_all("a", href=True):
                # Exclude links inside navigation chrome — nav/footer/header links
                # inflate authority counts and distort external link signals.
                if any(p.name in _BOILERPLATE for p in a.parents):
                    continue
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if href.startswith("http"):
                    link_domain = urlparse(href).netloc
                    if base_domain and link_domain == base_domain:
                        internal_links.append({"href": href, "text": text})
                    else:
                        external_links.append({"href": href, "text": text})
                elif href.startswith("/"):
                    internal_links.append({"href": href, "text": text})

            page_data["content_metrics"] = {
                "lists":          lists,
                "tables":         tables,
                "external_links": external_links[:50],
                "internal_links": internal_links[:50],
            }

        # ── faq_metrics: visible Q&A pairs ──────────────────────────────────
        if not page_data.get("faq_metrics"):
            qa_pairs: list[dict] = []
            q_re = re.compile(
                r"\?$|^(who|what|when|where|why|how|is|are|can|does|do|will|should)\b",
                re.IGNORECASE,
            )
            # dl/dt/dd pattern
            for dl in soup.find_all("dl"):
                for dt in dl.find_all("dt"):
                    q = dt.get_text(strip=True)
                    dd = dt.find_next_sibling("dd")
                    a = dd.get_text(strip=True) if dd else ""
                    if q and a:
                        qa_pairs.append({"question": q, "answer": a[:500]})
            # heading+paragraph pattern
            for heading in soup.find_all(["h2", "h3", "h4"]):
                heading_text = heading.get_text(strip=True)
                if q_re.search(heading_text):
                    sibling = heading.find_next_sibling(["p", "div"])
                    if sibling:
                        a = sibling.get_text(strip=True)
                        if a:
                            qa_pairs.append({"question": heading_text, "answer": a[:500]})
            page_data["faq_metrics"] = {"qa_pairs": qa_pairs}

        # ── meta_robots / canonical_url ──────────────────────────────────────
        if not page_data.get("meta_robots"):
            meta_r = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
            if meta_r:
                page_data["meta_robots"] = meta_r.get("content", "")

        if not page_data.get("canonical_url"):
            can = soup.find("link", rel="canonical")
            if can:
                page_data["canonical_url"] = can.get("href", "")

    except Exception as exc:
        print(f"[V2 ENRICH] HTML parse failed for {page_data.get('url', '?')}: {exc}")


def build_ai_page(
    page_data: dict[str, Any],
    domain_report: dict[str, Any],
    project_id: Any,
    job_id: Any,
    site_structure_lookup: dict[str, dict] | None = None,
) -> dict[str, Any]:
    """
    Build a complete ai_pages document from scraper output.

    Args:
        page_data:     seo_page_data document for this page.
        domain_report: domain_technical_reports document for this domain.
        project_id:    ObjectId of the parent project.
        job_id:        ObjectId of the current job run.
        site_structure_lookup: url -> {is_orphan, click_depth} map loaded
            once per job by the caller (see pipeline.run_v2_pipeline) —
            this function performs no crawl-graph query of its own.

    Returns:
        ai_pages document dict (not yet persisted).
    """
    # Shallow copy so we don't mutate the original MongoDB document.
    # _enrich_from_raw_html fills in signal keys that seo_page_data omits.
    enriched = dict(page_data)
    _enrich_from_raw_html(enriched)

    raw_html: str = enriched.get("raw_html", "") or ""
    # Normalize URL so trailing-slash/case variants produce the same document key.
    url: str = _normalize_url(enriched.get("url", "") or "")

    # Run all sub-extractors with isolated fault tolerance: one crashed extractor
    # must not prevent the page document from being saved.
    schema      = _safe_extract("schema",       extract_schema,       enriched,                                               url=url)
    content_sig = _safe_extract("content",      extract_content,      enriched,                                               url=url)
    crawl_sig   = _safe_extract("crawlability", extract_crawlability, domain_report,                                          url=url)
    tech_sig    = _safe_extract("technical",    extract_technical,    enriched, domain_report, raw_html,                      url=url)
    faq_sig     = _safe_extract("faq",          extract_faq,          schema.get("faq_schema_pairs", []), enriched,           url=url)
    entity_sig  = _safe_extract("entities",     extract_entities,     enriched, schema, content_sig.get("internal_links", []), url=url)
    geo_sig     = _safe_extract("geo",          extract_geo,          enriched, schema, raw_html, content_sig=content_sig,    url=url)
    site_struct_sig = _safe_extract("site_structure", extract_site_structure, site_structure_lookup or {}, url, url=url)

    # Normalize page_type from seo_page_data (stored as title-case "Contact",
    # "Homepage", etc. by context_enrichment.py) to lowercase canonical key.
    # Falls back to "generic" when absent — gate treats generic conservatively.
    raw_page_type = enriched.get("page_type") or "generic"
    page_type     = raw_page_type.strip().lower()

    return {
        "project_id":  ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id,
        "job_id":      ObjectId(job_id)      if not isinstance(job_id, ObjectId)      else job_id,
        "url":         url,
        "domain":      enriched.get("domain", ""),
        "scraped_at":  enriched.get("scraped_at") or datetime.now(timezone.utc),
        "version":     "v2",
        "page_type":   page_type,
        "crawlability": crawl_sig,
        "schema":       schema,
        "faq":          faq_sig,
        "entities":     entity_sig,
        "content":      content_sig,
        "geo":          geo_sig,
        "technical":    tech_sig,
        "site_structure": site_struct_sig,
    }


def extract_and_save(
    page_data: dict[str, Any],
    domain_report: dict[str, Any],
    project_id: Any,
    job_id: Any,
    site_structure_lookup: dict[str, dict] | None = None,
) -> dict[str, Any]:
    """
    Build the ai_pages document and upsert it into MongoDB.

    Returns the saved document (with _id).
    """
    doc = build_ai_page(page_data, domain_report, project_id, job_id, site_structure_lookup)
    url = doc["url"]

    print(f"[V2] Saving ai_page: {url}")

    result = ai_pages.find_one_and_update(
        filter={
            "project_id": doc["project_id"],
            "job_id":     doc["job_id"],
            "url":        url,
        },
        update={"$set": doc},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if result is None:
        result = ai_pages.find_one({
            "project_id": doc["project_id"],
            "job_id":     doc["job_id"],
            "url":        url,
        })

    if result is None:
        raise RuntimeError(f"[V2] ai_pages upsert returned no document for URL: {url}")

    print(f"[V2] Saved ai_page: {url} | _id={result.get('_id')}")
    return result
