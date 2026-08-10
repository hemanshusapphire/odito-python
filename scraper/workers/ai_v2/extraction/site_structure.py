"""
Extraction: site structure signals (orphan pages, click depth from homepage).

Reads seo_crawl_graph — computed once per job by the CRAWL_GRAPH worker.

Performance: seo_crawl_graph is loaded ONCE per job via build_site_structure_lookup(),
called by the AI_VISIBILITY worker before the pipeline's per-page loop — the exact
same pattern already used for domain_report (see domain_report_adapter.py). The
resulting lookup dict is threaded through run_v2_pipeline() -> extract_and_save()
-> build_ai_page(), so every page's extraction does a pure in-memory dict lookup
instead of issuing its own Mongo query. There is no process-global or module-level
cache here — no state persists between calls, no memory-leak or stale-cache risk,
and the implementation is trivially horizontally scalable across worker processes.

All output stored under ai_pages["site_structure"].
"""

import sys, os
from typing import Any
from bson import ObjectId

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import seo_crawl_graph


def _normalize_lookup_url(url: str) -> str:
    """
    Light local normalization for matching against extractor.py's normalized
    ai_pages URL. Does not import extractor.py's _normalize_url() directly to
    avoid cross-module coupling; duplicates only the cheap, stable part
    (lowercase, strip trailing slash) rather than the full urlparse pipeline,
    which is enough to close most casing/trailing-slash mismatches against
    seo_crawl_graph's stored url strings.
    """
    if not url:
        return ""
    u = url.strip().lower()
    if u.endswith("/") and len(u) > len("https://x/"):
        u = u.rstrip("/")
    return u


def build_site_structure_lookup(project_id: Any) -> dict[str, dict]:
    """
    Load ALL seo_crawl_graph rows for a project in a single query and build a
    url -> {is_orphan, click_depth} lookup map.

    Call ONCE per job (from ai_visibility.py, alongside build_v2_domain_report)
    and pass the returned dict through run_v2_pipeline() for every page to
    reuse — this is what avoids the N+1 query pattern across potentially
    thousands of pages in a single audit.

    Returns {} when the project has no crawl graph data yet (e.g. an older
    audit run before CRAWL_GRAPH existed) — callers must treat a missing URL
    key as "unknown", not "false"/"zero".
    """
    pid = ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id
    docs = seo_crawl_graph.find(
        {"projectId": pid},
        {"url": 1, "isOrphan": 1, "clickDepthFromHomepage": 1, "_id": 0},
    )

    lookup: dict[str, dict] = {}
    for d in docs:
        norm_url = _normalize_lookup_url(d.get("url", ""))
        if not norm_url:
            continue
        lookup[norm_url] = {
            "is_orphan":   bool(d.get("isOrphan", False)),
            "click_depth": d.get("clickDepthFromHomepage"),
        }
    return lookup


def extract_site_structure(lookup: dict[str, dict], url: str) -> dict[str, Any]:
    """
    Pure lookup against an already-loaded site structure map — no I/O.

    Args:
        lookup: dict returned by build_site_structure_lookup(), loaded once
                per job and passed down through the pipeline.
        url:    normalized page URL (matches extractor.py's _normalize_url()
                output as closely as the local normalization allows).

    Returns {} when this URL has no crawl graph entry.
    """
    if not lookup:
        return {}
    entry = lookup.get(_normalize_lookup_url(url))
    if entry is None:
        return {}
    return dict(entry)
