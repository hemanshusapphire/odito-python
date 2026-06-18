"""
Issue Engine + Severity Engine.

Turns rule results into real "issue intelligence" — failed/warning rules only,
with rich, human copy and context-aware severity. Replaces the legacy
derive_issues_from_rule_breakdown() that emitted "Rule X scored Y" logs.

ISSUE DOC (target schema + backward-compat aliases)
---------------------------------------------------
{
  projectId, page_url, rule_id, category, severity, status,
  title, description, detected_value, expected_value,
  recommendation, evidence, confidence, created_at,
  # compat aliases (so existing aggregations/frontend keep working):
  message (= title), rule_score
}

SEVERITY (context-aware — fixes "weak severity assignment")
-----------------------------------------------------------
Severity depends on page type, whether the feature is required, AI-visibility
impact and crawlability impact — not just the raw score.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from .issue_catalog import get_catalog_entry, get_detected_value
except ImportError:  # direct execution / non-package context
    from issue_catalog import get_catalog_entry, get_detected_value

SEVERITY_ORDER = {"critical": 4, "high": 3, "medium": 2, "low": 1}

# Rules whose failure is a crawlability/identity blocker -> critical when failing.
CRITICAL_WHEN_FAILING = {
    "schema_valid_jsonld",
    "robots_txt_non_blocking",
    "primary_organization_schema",
    "url_points_to_canonical_homepage",
    "no_plugin_duplicate_schemas",
}

# Per-rule severity by page type (overrides the score-based base).
# Mirrors the product spec: e.g. missing FAQ = medium on homepage, low on article.
PAGE_TYPE_SEVERITY: Dict[str, Dict[str, str]] = {
    "faq_section_5_to_10_questions": {
        "Homepage": "medium", "Service": "low", "Product": "low",
        "Article": "low", "Listing": "low", "_default": "low",
    },
    "faq_schema_matches_content": {
        "Homepage": "medium", "Service": "low", "Product": "low",
        "Article": "low", "_default": "low",
    },
    "service_pages_800_words": {
        "Service": "high", "Product": "medium", "_default": "low",
    },
    "first_60_words_direct_answer": {
        "Article": "high", "Service": "medium", "_default": "medium",
    },
    "sameas_array_links_active": {
        "Homepage": "high", "_default": "medium",
    },
}


def compute_severity(
    rule_id: str,
    score: float,
    is_required: bool,
    page_type: str = "Unknown",
) -> str:
    """Context-aware severity. Returns critical | high | medium | low."""
    # 1. Page-type-specific override takes precedence when the rule is failing.
    pt_map = PAGE_TYPE_SEVERITY.get(rule_id)
    if pt_map is not None:
        return pt_map.get(page_type, pt_map.get("_default", "low"))

    # 2. Crawlability/identity blockers.
    if rule_id in CRITICAL_WHEN_FAILING and score < 40:
        return "high"

    # 3. Score- and requirement-based base.
    if is_required:
        if score < 30:
            return "high"
        if score < 60:
            return "high"
        return "medium"
    # optional features never escalate above medium
    if score < 20:
        return "medium"
    return "low"


def should_create_issue(score: float, is_required: bool) -> bool:
    """Issue only for genuinely failing/weak rules; passed rules are never stored."""
    return score < 75 if is_required else score < 40


def _status_from(severity: str) -> str:
    return "failed" if severity in ("critical", "high") else "warning"


def _page_type_of(page_data: Dict[str, Any]) -> str:
    if not page_data:
        return "Unknown"
    ns = page_data.get("normalized_signals") or {}
    pt = (ns.get("page_type") or {}).get("detected_type")
    if pt:
        return pt
    return (page_data.get("page_type_properties") or {}).get("detected_type", "Unknown")


def build_issues(
    rule_breakdown: List[Dict[str, Any]],
    page_data: Dict[str, Any],
    project_id: Any,
    page_url: str,
    rule_lookup,
    object_id_factory,
) -> List[Dict[str, Any]]:
    """Build rich issue docs for the failing/warning rules of one page.

    rule_lookup(rule_id) -> rule instance (for is_required/category/description)
    object_id_factory(project_id) -> ObjectId for storage
    """
    page_type = _page_type_of(page_data)
    legacy_aliases = page_data if page_data else {}
    issues: List[Dict[str, Any]] = []

    for rule in rule_breakdown:
        rule_id = rule.get("rule_id", "unknown")
        score = float(rule.get("score", 100))
        category = rule.get("category", "unknown")

        rule_inst = rule_lookup(rule_id) if rule_lookup else None
        is_required = bool(getattr(rule_inst, "is_required", False)) if rule_inst else False
        rule_desc = getattr(rule_inst, "description", "") if rule_inst else ""

        if not should_create_issue(score, is_required):
            continue

        severity = compute_severity(rule_id, score, is_required, page_type)
        catalog = get_catalog_entry(rule_id, rule_desc)
        detected = get_detected_value(rule_id, legacy_aliases)
        confidence = 92 if is_required else 75

        title = catalog["title"]
        issue = {
            "projectId": object_id_factory(project_id),
            "page_url": page_url,
            "rule_id": rule_id,
            "category": category,
            "severity": severity,
            "status": _status_from(severity),
            "title": title,
            "description": catalog["description"],
            "detected_value": detected,
            "expected_value": catalog.get("expected") or None,
            "recommendation": catalog["recommendation"],
            "evidence": {
                "rule_id": rule_id,
                "normalized_score": round(score, 1),
                "page_type": page_type,
                "is_required": is_required,
            },
            "confidence": confidence,
            "created_at": datetime.utcnow(),
            # ── backward-compat aliases (dependency-map requirement) ──
            "message": title,
            "rule_score": round(score, 1),
        }
        issues.append(issue)

    return issues


def store_issues_bulk(collection, issues_by_page: Dict[str, List[Dict[str, Any]]],
                      project_id: Any, object_id_factory) -> Dict[str, int]:
    """Replace all issues for the given pages in one efficient pass.

    Deletes existing issues for the pages we just scored, then bulk-inserts the
    new ones. Pages with zero issues are cleared (so resolved issues disappear).
    Returns {deleted, inserted}.
    """
    page_urls = list(issues_by_page.keys())
    if not page_urls:
        return {"deleted": 0, "inserted": 0}

    pid = object_id_factory(project_id)
    deleted = collection.delete_many({
        "projectId": pid,
        "page_url": {"$in": page_urls},
    }).deleted_count

    all_docs: List[Dict[str, Any]] = []
    for docs in issues_by_page.values():
        all_docs.extend(docs)

    inserted = 0
    if all_docs:
        inserted = len(collection.insert_many(all_docs, ordered=False).inserted_ids)

    return {"deleted": deleted, "inserted": inserted}
