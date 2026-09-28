"""
Visible rating extraction (ratingValue / reviewCount / bestRating).

Extracts the aggregate rating a page actually DISPLAYS, so the backend can
build AggregateRating JSON-LD from verified page content only. Nothing here
estimates, averages, rounds or fills in a value.

Precision over recall:
  * A candidate needs BOTH a rating value AND a review/rating count, found in
    the same small element (the smallest element that contains both).
  * The rating value must sit on an explicit scale ("4.8/5", "4.8 out of 5",
    "4.8 stars", "Rated 4.8") and lie inside it.
  * Approximate counts ("120+ reviews", "over 100 reviews") are rejected —
    AggregateRating needs the real number.
  * An element that contains more than one rating value or more than one count
    is skipped as ambiguous.
When nothing qualifies the result is simply "no candidates" — the caller must
then report that reliable rating data is unavailable, never guess.

Also reports an AggregateRating that is already present as on-page microdata
(itemprop), which the JSON-LD based rule does not see.
"""

import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, Tag

from .faq_pair_extraction import visible_text

MAX_CANDIDATES = 5
_MIN_TEXT = 6
_MAX_TEXT = 300
_MAX_COUNT = 1_000_000_000
_SCALES = (5, 10, 100)

_SKIP_TAGS = {"script", "style", "noscript", "template", "head", "meta", "link", "html", "body"}

_VALUE = r"\d{1,3}(?:\.\d{1,2})?"

# 4.8/5  ·  4.8 / 5  ·  98/100   (not part of a date/fraction chain like 5/5/2024)
_RATING_SLASH = re.compile(rf"(?<![\d./])({_VALUE})\s*/\s*(5|10|100)(?![\d.]|\s*/\s*\d)")
# 4.8 out of 5
_RATING_OUT_OF = re.compile(rf"(?<![\d./])({_VALUE})\s*(?:out\s+of|of)\s*(5|10|100)(?![\d.])", re.IGNORECASE)
# Rated 4.8  ·  Rating: 4.8  ·  4.8 stars  ·  ★★★★★ 4.8   (scale 5 is implied by the star vocabulary)
_RATING_IMPLICIT = re.compile(
    rf"(?:\b(?:rated|rating(?:\s+of)?|average(?:\s+rating)?|score)\s*[:\-]?\s*({_VALUE})(?![\d.]|\s*[/%])"
    rf"|(?<![\d./])({_VALUE})\s*(?:★|⭐|stars?\b)"
    rf"|[★⭐]+\s*({_VALUE})(?![\d./%]))",
    re.IGNORECASE,
)

_COUNT = re.compile(
    r"(?<![\d.,])(\d{1,3}(?:,\d{3})+|\d+)(\s*\+)?\s*"
    r"(?:(?:verified|customer|client|google|user|happy|real|genuine)\s+)*"
    r"(reviews?|ratings?|votes?)\b",
    re.IGNORECASE,
)
_APPROX_BEFORE = re.compile(r"(?:over|more\s+than|nearly|almost|about|around|approximately|approx\.?|up\s+to|~)\s*$", re.IGNORECASE)


def _to_number(raw: str) -> Optional[float]:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _rating_matches(text: str) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    for m in _RATING_SLASH.finditer(text):
        found.append({"value": m.group(1), "best": int(m.group(2)), "span": m.span()})
    for m in _RATING_OUT_OF.finditer(text):
        found.append({"value": m.group(1), "best": int(m.group(2)), "span": m.span()})
    for m in _RATING_IMPLICIT.finditer(text):
        value = m.group(1) or m.group(2) or m.group(3)
        if value:
            found.append({"value": value, "best": 5, "span": m.span(), "implicit": True})

    # The same figure can be matched by two patterns ("4.8/5" also hits "rated 4.8").
    unique: List[Dict[str, Any]] = []
    for f in sorted(found, key=lambda x: x["span"][0]):
        if any(not (f["span"][1] <= u["span"][0] or f["span"][0] >= u["span"][1]) for u in unique):
            continue
        unique.append(f)
    return unique


def _count_matches(text: str) -> List[Dict[str, Any]]:
    found = []
    for m in _COUNT.finditer(text):
        approximate = bool(m.group(2)) or bool(_APPROX_BEFORE.search(text[max(0, m.start() - 16):m.start()]))
        number = int(m.group(1).replace(",", ""))
        kind = "reviewCount" if m.group(3).lower().startswith("review") else "ratingCount"
        found.append({"number": number, "kind": kind, "approximate": approximate})
    return found


def _candidate_from_text(text: str) -> Optional[Dict[str, Any]]:
    ratings = _rating_matches(text)
    counts = _count_matches(text)
    # Ambiguous or incomplete -> not reliable.
    if len(ratings) != 1 or len(counts) != 1:
        return None
    rating, count = ratings[0], counts[0]
    if count["approximate"]:
        return None
    value = _to_number(rating["value"])
    best = rating["best"]
    if value is None or best not in _SCALES or not (0 < value <= best):
        return None
    if not (1 <= count["number"] <= _MAX_COUNT):
        return None
    return {
        "ratingValue": value,
        "bestRating": best,
        "worstRating": None,
        count["kind"]: count["number"],
        "source": "text",
        "evidence": text[:_MAX_TEXT],
    }


def _microdata_value(scope: Tag, prop: str) -> Optional[str]:
    el = scope.find(attrs={"itemprop": prop})
    if el is None:
        return None
    raw = el.get("content")
    if raw is None:
        raw = visible_text(el)
    raw = (raw or "").strip()
    return raw or None


def _extract_microdata(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    scope = soup.find(attrs={"itemprop": "aggregateRating"})
    if scope is None:
        scope = soup.find(attrs={"itemtype": re.compile(r"schema\.org/AggregateRating", re.IGNORECASE)})
    if scope is None:
        return None
    return {
        "ratingValue": _microdata_value(scope, "ratingValue"),
        "reviewCount": _microdata_value(scope, "reviewCount"),
        "ratingCount": _microdata_value(scope, "ratingCount"),
        "bestRating": _microdata_value(scope, "bestRating"),
    }


def extract_rating_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Returns:
        {
          "rating_candidates": [ {ratingValue, bestRating, worstRating,
                                  reviewCount|ratingCount, source, evidence}, ... ],
          "rating_candidate_count": int,
          "microdata_aggregate_rating": {...} | None,
        }
    """
    matched: Dict[int, Dict[str, Any]] = {}
    elements: Dict[int, Tag] = {}
    for el in soup.find_all(True):
        if el.name in _SKIP_TAGS:
            continue
        text = visible_text(el)
        if not (_MIN_TEXT <= len(text) <= _MAX_TEXT):
            continue
        candidate = _candidate_from_text(text)
        if candidate:
            matched[id(el)] = candidate
            elements[id(el)] = el
        if len(matched) >= 200:
            break

    # Keep only the smallest element that carries both figures.
    minimal: List[Dict[str, Any]] = []
    for key, el in elements.items():
        if any(isinstance(d, Tag) and id(d) in matched for d in el.descendants):
            continue
        minimal.append(matched[key])

    # De-duplicate identical figures (the same widget rendered twice, e.g. mobile + desktop).
    seen = set()
    candidates: List[Dict[str, Any]] = []
    for c in minimal:
        key = (c["ratingValue"], c["bestRating"], c.get("reviewCount"), c.get("ratingCount"))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(c)
        if len(candidates) >= MAX_CANDIDATES:
            break

    return {
        "rating_candidates": candidates,
        "rating_candidate_count": len(candidates),
        "microdata_aggregate_rating": _extract_microdata(soup),
    }
