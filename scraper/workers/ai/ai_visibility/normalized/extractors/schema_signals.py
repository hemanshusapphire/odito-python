"""
Schema (JSON-LD) extractor — parses the document's structured data ONCE and
exposes the flattened @graph for reuse by the entity and FAQ extractors.

SEMANTIC VALIDATION (fixes "duplicate schema false positives")
--------------------------------------------------------------
The legacy logic flagged ANY repeated @type as a duplicate. That is wrong: a
catalogue page legitimately has many Product nodes; an article hub has many
Article nodes. We only flag duplicates for types that must be unique per page
(Organization, WebSite, WebPage, BreadcrumbList). Repeats of collection types
are not issues.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from ..rendered_dom import RenderedDOM
from ..signals import SchemaSignals

# Types that should appear at most once per page; repeats are genuine issues.
SINGLETON_TYPES = {"Organization", "WebSite", "WebPage", "BreadcrumbList"}


def _flatten(node: Any) -> List[Dict[str, Any]]:
    """Flatten @graph / arrays / nested structures into a flat entity list."""
    out: List[Dict[str, Any]] = []
    if isinstance(node, dict):
        if "@graph" in node and isinstance(node["@graph"], list):
            for item in node["@graph"]:
                out.extend(_flatten(item))
        else:
            out.append(node)
    elif isinstance(node, list):
        for item in node:
            out.extend(_flatten(item))
    return out


def _type_of(entity: Dict[str, Any]) -> List[str]:
    t = entity.get("@type")
    if isinstance(t, list):
        return [str(x) for x in t]
    if isinstance(t, str):
        return [t]
    return []


def parse_json_ld(dom: RenderedDOM) -> Tuple[List[Dict[str, Any]], int, int]:
    """Parse every JSON-LD script once.

    Returns (graph_entities, raw_script_count, parse_errors).
    """
    graph: List[Dict[str, Any]] = []
    parse_errors = 0
    scripts = dom.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            graph.extend(_flatten(json.loads(raw)))
        except (json.JSONDecodeError, TypeError, ValueError):
            parse_errors += 1
    return graph, len(scripts), parse_errors


def extract_schema(dom: RenderedDOM) -> Tuple[SchemaSignals, List[Dict[str, Any]]]:
    """Return SchemaSignals plus the flattened graph (for entity/FAQ reuse)."""
    graph, raw_count, parse_errors = parse_json_ld(dom)

    types: List[str] = []
    for ent in graph:
        types.extend(_type_of(ent))

    # Semantic duplicate detection — only singleton types count.
    seen: Dict[str, int] = {}
    for t in types:
        seen[t] = seen.get(t, 0) + 1
    duplicate_types = sorted(
        t for t, n in seen.items() if n > 1 and t in SINGLETON_TYPES
    )

    sameas_count = 0
    for ent in graph:
        sa = ent.get("sameAs")
        if isinstance(sa, list):
            sameas_count += len(sa)
        elif isinstance(sa, str):
            sameas_count += 1

    breadcrumb = next((e for e in graph if "BreadcrumbList" in _type_of(e)), None)
    breadcrumb_levels = 0
    if breadcrumb and isinstance(breadcrumb.get("itemListElement"), list):
        breadcrumb_levels = len(breadcrumb["itemListElement"])

    sig = SchemaSignals(
        present=bool(graph),
        json_ld_count=raw_count,
        types=sorted(set(types)),
        duplicate_types=duplicate_types,
        valid_json=(parse_errors == 0),
        parse_errors=parse_errors,
        faq_schema=any("FAQPage" in _type_of(e) for e in graph),
        breadcrumb_present=breadcrumb is not None,
        breadcrumb_levels=breadcrumb_levels,
        organization_present=any("Organization" in _type_of(e) for e in graph),
        sameas_count=sameas_count,
        graph=graph,
    )
    return sig, graph
