"""
Extraction: Schema.org signals.

Parses the structured_data list from seo_page_data / seo_ai_visibility
and extracts the schema types V2 rules need.

All output stored under ai_pages["schema"].
"""

from typing import Any


def _find_types(structured_data: list, *types: str) -> list[dict]:
    """Return all schema items whose @type matches any of the requested types (case-insensitive)."""
    needle = {t.lower() for t in types}
    result = []
    for item in structured_data:
        raw_type = item.get("@type", "")
        variants = raw_type if isinstance(raw_type, list) else [raw_type]
        if any(v.lower() in needle for v in variants):
            result.append(item)
    return result


def _first(structured_data: list, *types: str) -> dict | None:
    hits = _find_types(structured_data, *types)
    return hits[0] if hits else None


def extract_schema(page_data: dict[str, Any]) -> dict[str, Any]:
    """
    Extract schema signals from the page's structured data.

    Args:
        page_data: document from seo_page_data or seo_ai_visibility.

    Returns:
        Schema sub-document for ai_pages.
    """
    raw = page_data.get("structured_data", []) or []
    if isinstance(raw, str):
        import json
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        raw = [raw] if isinstance(raw, dict) else []

    types_present = []
    for item in raw:
        t = item.get("@type", "")
        if isinstance(t, list):
            types_present.extend(t)
        elif t:
            types_present.append(t)

    # ── LocalBusiness ────────────────────────────────────────────────────────
    lb_subtypes = [
        "LocalBusiness", "Restaurant", "MedicalClinic", "LegalService",
        "AutoDealer", "HealthAndBeautyBusiness", "HomeAndConstructionBusiness",
        "LodgingBusiness", "EntertainmentBusiness", "FoodEstablishment",
        "Store", "FinancialService", "SportsActivityLocation",
    ]
    lb = _first(raw, *lb_subtypes)
    local_business = None
    if lb:
        addr = lb.get("address", {}) or {}
        geo  = lb.get("geo", {}) or {}
        hours = lb.get("openingHoursSpecification", []) or []
        same_as = lb.get("sameAs", []) or []
        local_business = {
            "present":    True,
            "type":       lb.get("@type"),
            "name":       lb.get("name"),
            "telephone":  lb.get("telephone"),
            "url":        lb.get("url"),
            "address": {
                "street":   addr.get("streetAddress"),
                "city":     addr.get("addressLocality"),
                "region":   addr.get("addressRegion"),
                "postal":   addr.get("postalCode"),
                "country":  addr.get("addressCountry"),
                "complete": bool(
                    addr.get("streetAddress")
                    and addr.get("addressLocality")
                ),
            },
            "geo": {
                "present":   bool(geo.get("latitude") and geo.get("longitude")),
                "latitude":  geo.get("latitude"),
                "longitude": geo.get("longitude"),
            },
            "opening_hours": {
                "present": bool(hours),
                "count":   len(hours),
                "entries": hours[:7],
            },
            "same_as": same_as if isinstance(same_as, list) else [same_as],
        }

    # ── Organization ─────────────────────────────────────────────────────────
    org = _first(raw, "Organization", "Corporation", "NGO", "GovernmentOrganization")
    organization = None
    if org:
        organization = {
            "present": True,
            "name":    org.get("name"),
            "url":     org.get("url"),
            "logo":    bool(org.get("logo")),
            "same_as": org.get("sameAs"),
        }

    # ── Author / Person ───────────────────────────────────────────────────────
    author_item = _first(raw, "Person")
    article_item = _first(raw, "Article", "BlogPosting", "NewsArticle", "WebPage")
    author = None
    if author_item:
        author = {"present": True, "name": author_item.get("name"), "source": "Person schema"}
    elif article_item and article_item.get("author"):
        a = article_item["author"]
        name = a.get("name") if isinstance(a, dict) else None
        author = {"present": True, "name": name, "source": "Article.author"}

    # ── FAQ schema ────────────────────────────────────────────────────────────
    faq_items = _find_types(raw, "FAQPage")
    faq_pairs: list[dict] = []
    for faq in faq_items:
        for entry in (faq.get("mainEntity", []) or []):
            q = entry.get("name", "") or ""
            a_block = entry.get("acceptedAnswer", {}) or {}
            a = a_block.get("text", "") or ""
            if q:
                faq_pairs.append({"question": q.strip(), "answer": a.strip()})

    # ── HowTo schema ──────────────────────────────────────────────────────────
    howto_item = _first(raw, "HowTo")
    howto = None
    if howto_item:
        steps = howto_item.get("step", []) or []
        howto = {
            "present":     True,
            "name":        howto_item.get("name"),
            "step_count":  len(steps),
            "steps_valid": all(
                s.get("name") and s.get("text") for s in steps
                if isinstance(s, dict)
            ),
        }

    # ── Speakable schema ──────────────────────────────────────────────────────
    speakable_item = _first(raw, "WebPage", "Article", "NewsArticle")
    speakable = None
    if speakable_item and speakable_item.get("speakable"):
        spk = speakable_item["speakable"]
        css = spk.get("cssSelector", []) if isinstance(spk, dict) else []
        xp  = spk.get("xpath", [])        if isinstance(spk, dict) else []
        speakable = {
            "present":   True,
            "css_selectors": css if isinstance(css, list) else [css],
            "xpath":         xp  if isinstance(xp,  list) else [xp],
        }

    return {
        "types_present":  list(set(types_present)),
        "local_business": local_business,
        "organization":   organization,
        "author":         author,
        "faq_schema_pairs": faq_pairs,
        "howto":          howto,
        "speakable":      speakable,
    }
