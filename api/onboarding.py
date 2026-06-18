"""Onboarding API routes — keyword generation & ranking check via DataForSEO.

These endpoints are called synchronously from the Node.js backend during
the ARIAChat onboarding flow.  They are NOT part of the async job pipeline.
"""

import os
import time
import requests
import base64
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timezone

router = APIRouter()


# ── DataForSEO credentials (reuses same env vars as keyword_research) ──────

DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "")


def _auth_header():
    """Generate HTTP Basic Auth header for DataForSEO."""
    creds = f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}"
    return f"Basic {base64.b64encode(creds.encode()).decode()}"


# ── Country → DataForSEO location_code map (shared with keyword_research) ──

COUNTRY_TO_LOCATION_CODE = {
    "US": 2840, "IN": 2356, "UK": 2826, "GB": 2826,
    "CA": 2124, "AU": 2036, "DE": 2315, "FR": 2250,
    "ES": 2246, "IT": 2240, "JP": 2132, "BR": 2075,
    "MX": 2239, "KR": 2131, "RU": 2306,
}


# ── Domain normalisation helper ────────────────────────────────────────────

def normalize_domain(raw: str) -> str:
    """
    Strip protocol, www, trailing slashes so that
    'https://www.example.com/page' → 'example.com'
    """
    if not raw:
        return ""
    
    d = raw.strip().lower()
    d = re.sub(r'^https?://', '', d)
    d = re.sub(r'^www\.', '', d)
    d = d.rstrip('/')
    # Take only the hostname (drop path/query)
    d = d.split('/')[0].split('?')[0]
    return d


def extract_domain_from_url(url: str) -> str:
    """
    Extract base domain from full URL.
    Handles various URL formats from SERP results.
    """
    if not url:
        return ""
    
    # Remove protocol if present
    url = re.sub(r'^https?://', '', url)
    
    # Remove www prefix
    url = re.sub(r'^www\.', '', url)
    
    # Split on first slash to get domain
    domain = url.split('/')[0].split('?')[0]
    
    return domain.lower().strip()


# ═══════════════════════════════════════════════════════════════════════════
#  1) KEYWORD GENERATION
# ═══════════════════════════════════════════════════════════════════════════

class GenerateKeywordsRequest(BaseModel):
    sub_type: str                         # e.g. "IT services"
    location: Optional[str] = None        # e.g. "New York"  (for local)
    lat: Optional[float] = None           # latitude from Google Places (local SEO)
    lng: Optional[float] = None           # longitude from Google Places (local SEO)
    country: str = "US"
    language: str = "en"


class GenerateKeywordsResponse(BaseModel):
    keywords: List[str]


KEYWORD_SUGGESTIONS_URL = (
    "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_suggestions/live"
)


def _clean_query(sub_type: str, location: Optional[str] = None) -> str:
    """Clean and normalise query for DataForSEO API.

    Strips noise from the location, extracts the city name, and appends it to
    the sub_type — but only when the city name is not already present in the
    sub_type string (prevents "Nashik IT Agency Nashik" duplication).
    """
    sub_type = sub_type.strip()

    city = ""
    if location:
        location_clean = re.sub(
            r'\d+.*?(st|ave|avenue|street|blvd|boulevard|floor|fl|#)\s*',
            '', location, flags=re.IGNORECASE
        )
        # Remove 5- and 6-digit postal codes (US ZIP and Indian PIN)
        location_clean = re.sub(r'\b\d{5,6}\b', '', location_clean)
        location_clean = re.sub(r',\s*USA$', '', location_clean, flags=re.IGNORECASE)
        location_clean = re.sub(r'\bSuite\s+\d+\b', '', location_clean, flags=re.IGNORECASE)

        parts = [p.strip() for p in location_clean.split(',') if p.strip()]
        if parts:
            city = parts[0]

    # Only append city if it is not already contained in the sub_type (case-insensitive)
    query = sub_type
    if city and city.lower() not in sub_type.lower():
        query = f"{sub_type} {city}"

    print(f"[ONBOARDING] Cleaned query | original=\"{sub_type} {location or ''}\" | clean=\"{query}\"")
    return query


def _safe_extract_keywords(data: dict) -> List[dict]:
    """Safely extract keywords from DataForSEO response with null checks."""
    items = []
    
    try:
        # Log full response for debugging
        print(f"[ONBOARDING] Full API response structure: {list(data.keys())}")
        
        # STEP 1: Check tasks exists and is not empty
        tasks = data.get("tasks")
        if not tasks or not isinstance(tasks, list):
            print(f"[ONBOARDING] No valid tasks in response")
            return []
        
        print(f"[ONBOARDING] Found {len(tasks)} tasks")
        
        # STEP 2: Check first task exists
        first_task = tasks[0]
        if not first_task:
            print(f"[ONBOARDING] First task is null/empty")
            return []
        
        # STEP 3: Check result exists and is not null
        result = first_task.get("result")
        if not result or not isinstance(result, list):
            print(f"[ONBOARDING] No valid result in first task")
            return []
        
        print(f"[ONBOARDING] Found {len(result)} results")
        
        # STEP 4: Check first result exists
        first_result = result[0]
        if not first_result:
            print(f"[ONBOARDING] First result is null/empty")
            return []
        
        # STEP 5: Check items exists and is not empty
        items_list = first_result.get("items")
        if not items_list or not isinstance(items_list, list):
            print(f"[ONBOARDING] No valid items in first result")
            return []
        
        print(f"[ONBOARDING] Found {len(items_list)} items - extracting keywords")
        
        # STEP 6: Extract keywords safely from each item
        for i, item in enumerate(items_list):
            try:
                if not item:
                    continue
                
                # Method 1: keyword_data structure
                keyword_data = item.get("keyword_data")
                if keyword_data:
                    keyword_text = keyword_data.get("keyword", "")
                    if keyword_text:
                        # Get search volume safely
                        volume = 0
                        keyword_info = keyword_data.get("keyword_info", {})
                        if keyword_info and isinstance(keyword_info, dict):
                            volume = keyword_info.get("search_volume", 0) or 0
                        
                        items.append({"keyword": keyword_text.strip(), "volume": int(volume)})
                        continue
                
                # Method 2: direct keyword field
                keyword_text = item.get("keyword", "")
                if keyword_text:
                    volume = item.get("search_volume", 0) or 0
                    items.append({"keyword": keyword_text.strip(), "volume": int(volume)})
                    continue
                
            except Exception as e:
                print(f"[ONBOARDING] Error extracting item {i}: {e}")
                continue
        
        print(f"[ONBOARDING] Extracted {len(items)} keywords safely")
        
    except Exception as e:
        print(f"[ONBOARDING] Critical error in _safe_extract_keywords: {e}")
        import traceback
        print(f"[ONBOARDING] Traceback: {traceback.format_exc()}")
    
    return items


def _get_fallback_keywords(sub_type: str) -> List[str]:
    """Generate fallback keywords based on business type."""
    sub_type_lower = sub_type.lower().strip()
    
    fallback_map = {
        "it services": ["IT services", "IT support", "IT company", "managed IT services", "IT solutions"],
        "digital marketing": ["digital marketing", "marketing agency", "online marketing", "SEO services", "social media marketing"],
        "consulting": ["consulting services", "business consulting", "management consulting", "strategy consulting", "consulting firm"],
        "restaurant": ["restaurant", "local restaurant", "best restaurant", "dining", "food restaurant"],
        "salon": ["salon", "beauty salon", "hair salon", "local salon", "salon services"],
        "gym": ["gym", "fitness center", "local gym", "gym near me", "fitness gym"],
        "agency": ["agency", "digital agency", "marketing agency", "creative agency", "agency services"],
        "software": ["software", "software company", "tech software", "software solutions", "software development"],
        "ecommerce": ["ecommerce", "online store", "e-commerce store", "online shopping", "buy online"],
    }
    
    # Check for exact matches first
    if sub_type_lower in fallback_map:
        return fallback_map[sub_type_lower]
    
    # Check for partial matches
    for key, fallbacks in fallback_map.items():
        if key in sub_type_lower or sub_type_lower in key:
            return fallbacks
    
    # Generic fallbacks
    return [
        f"{sub_type}",
        f"{sub_type} services",
        f"professional {sub_type}",
        f"local {sub_type}",
        f"best {sub_type}"
    ]


def _resolve_location_code(lat: Optional[float], lng: Optional[float],
                            address: Optional[str], country: str) -> int:
    """Resolve the best DataForSEO location_code.

    Uses the DataForSEO Locations API + Haversine distance to find the nearest
    city-level code when coordinates are available.  Falls back to the
    country-level code from the static map when they are not.
    """
    if lat is not None and lng is not None:
        try:
            locations_url = "https://api.dataforseo.com/v3/serp/google/locations"
            headers = {"Authorization": _auth_header(), "Content-Type": "application/json"}
            resp = requests.get(locations_url, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            locations = data.get("tasks", [{}])[0].get("result", []) or []

            # Filter to same country when possible
            country_upper = country.upper()
            candidates = [
                loc for loc in locations
                if loc.get("location_lat") and loc.get("location_lng")
                and (not country_upper or loc.get("country_iso_code", "").upper() == country_upper
                     or not any(
                         loc.get("country_iso_code", "").upper() == c
                         for c in list(COUNTRY_TO_LOCATION_CODE.keys())
                     ))
            ]
            if not candidates:
                candidates = [loc for loc in locations if loc.get("location_lat") and loc.get("location_lng")]

            best = None
            min_dist = float("inf")
            for loc in candidates:
                lat2, lng2 = loc["location_lat"], loc["location_lng"]
                # Haversine
                from math import radians, sin, cos, sqrt, atan2
                R = 6371
                dlat = radians(lat2 - lat)
                dlng = radians(lng2 - lng)
                a = sin(dlat / 2) ** 2 + cos(radians(lat)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
                dist = R * 2 * atan2(sqrt(a), sqrt(1 - a))
                if dist < min_dist:
                    min_dist = dist
                    best = loc

            if best:
                print(f"[ONBOARDING] City-level location resolved | code={best['location_code']} name={best.get('location_name')} dist={min_dist:.1f}km")
                return int(best["location_code"])
        except Exception as e:
            print(f"[ONBOARDING] City-level lookup failed, using country fallback | error={e}")

    return COUNTRY_TO_LOCATION_CODE.get(country.upper(), 2840)


@router.post("/onboarding/generate-keywords", response_model=GenerateKeywordsResponse)
def generate_keywords(req: GenerateKeywordsRequest):
    """Return top-5 keyword suggestions for a given business sub-type."""

    # STEP 1: Clean the query
    clean_query = _clean_query(req.sub_type, req.location)

    # STEP 2: Resolve location code — city-level when lat/lng are provided
    location_code = _resolve_location_code(req.lat, req.lng, req.location, req.country)
    language_code = req.language.lower() or "en"

    payload = [{
        "keyword": clean_query,
        "location_code": location_code,
        "language_code": language_code,
        "limit": 10,
        "include_seed_keyword": True,
        "include_serp_info": False,
    }]

    headers = {
        "Authorization": _auth_header(),
        "Content-Type": "application/json",
    }

    print(f"[ONBOARDING] generate-keywords | clean_query=\"{clean_query}\" | loc={location_code} | lang={language_code}")

    MAX_RETRIES = 2
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(KEYWORD_SUGGESTIONS_URL, json=payload, headers=headers, timeout=30)

            if resp.status_code == 401:
                raise HTTPException(status_code=502, detail="DataForSEO authentication failed")
            if resp.status_code == 402:
                raise HTTPException(status_code=502, detail="DataForSEO insufficient credits")
            resp.raise_for_status()

            data = resp.json()

            if data.get("status_code") != 20000:
                raise Exception(f"DataForSEO error: {data.get('status_message')}")

            # STEP 2: Safe keyword extraction
            items = _safe_extract_keywords(data)

            # STEP 3: Handle empty results with fallback
            if not items:
                print(f"[ONBOARDING] No keywords from API, using fallback | query=\"{clean_query}\"")
                fallback_keywords = _get_fallback_keywords(req.sub_type)
                print(f"[ONBOARDING] Fallback keywords: {fallback_keywords}")
                return GenerateKeywordsResponse(keywords=fallback_keywords[:5])

            # STEP 4: Sort by volume descending, take top 5, deduplicate
            seen = set()
            unique = []
            for kw in sorted(items, key=lambda x: x["volume"], reverse=True):
                norm = kw["keyword"].strip().lower()
                if norm not in seen and norm:  # Extra check for empty strings
                    seen.add(norm)
                    unique.append(kw["keyword"])
                if len(unique) >= 5:
                    break

            print(f"[ONBOARDING] ✅ Keywords generated | count={len(unique)} | top=\"{unique[0] if unique else ''}\"")
            return GenerateKeywordsResponse(keywords=unique)

        except HTTPException:
            raise  # Don't retry HTTP errors from our side
        except Exception as e:
            last_error = e
            print(f"[ONBOARDING] ❌ keyword gen attempt {attempt} failed | error=\"{e}\"")
            
            # Log full response on failure for debugging
            if 'resp' in locals():
                try:
                    print(f"[ONBOARDING] Failed response content: {resp.text[:500]}")
                except:
                    pass
            
            if attempt < MAX_RETRIES:
                time.sleep(2)

    # STEP 5: Final fallback if all retries fail
    print(f"[ONBOARDING] All retries failed, using final fallback | query=\"{clean_query}\"")
    fallback_keywords = _get_fallback_keywords(req.sub_type)
    return GenerateKeywordsResponse(keywords=fallback_keywords[:5])


# ═══════════════════════════════════════════════════════════════════════════
#  2) RANKING CHECK
# ═══════════════════════════════════════════════════════════════════════════

class CheckRankingRequest(BaseModel):
    domain: str                           # User's website
    keywords: List[str]                   # Up to 5
    location_code: int = 2840
    language_code: str = "en"


class RankingUrl(BaseModel):
    rank: int
    url: str
    type: str                             # "homepage" | "internal_page"


class KeywordRank(BaseModel):
    keyword: str
    rank: Optional[int] = None            # best_rank — kept for backward compat
    best_rank: Optional[int] = None       # lowest (best) position found
    ranking_urls: List[RankingUrl] = []   # all domain occurrences in the SERP


class CheckRankingResponse(BaseModel):
    results: List[KeywordRank]


SERP_API_URL = (
    "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
)


def _classify_url_type(url: str) -> str:
    """Return 'homepage' if URL path is empty/root, else 'internal_page'."""
    try:
        from urllib.parse import urlparse
        path = urlparse(url).path.rstrip('/')
        return "homepage" if path == "" else "internal_page"
    except Exception:
        return "internal_page"


@router.post("/onboarding/check-ranking", response_model=CheckRankingResponse)
def check_ranking(req: CheckRankingRequest):
    """Check ranking position for each keyword in Google top-100."""

    # 🚨 STEP 1: VERIFY INPUT - VERY FIRST LINE
    print("🚨 ENTRY req.keywords:", req.keywords)
    print("🚨 TYPE:", type(req.keywords))
    print("🚨 ENTRY RAW REQUEST:", {
        "domain": req.domain,
        "keywords": req.keywords,
        "location_code": req.location_code,
        "language_code": req.language_code
    })

    clean_domain = normalize_domain(req.domain)
    results: List[KeywordRank] = []

    # CRITICAL LOG: Capture keywords received at Python worker
    print(f"🔍 DEBUG: Python worker received keywords:", {
        "request_keywords": req.keywords,
        "keywords_type": type(req.keywords),
        "keywords_length": len(req.keywords) if req.keywords else 0,
        "keywords_string": str(req.keywords),
        "full_request": {
            "domain": req.domain,
            "keywords": req.keywords,
            "location_code": req.location_code,
            "language_code": req.language_code
        }
    })

    headers = {
        "Authorization": _auth_header(),
        "Content-Type": "application/json",
    }

    print(f"[ONBOARDING] check-ranking | domain=\"{req.domain}\" → clean=\"{clean_domain}\" | keywords={req.keywords}")

    # 🚨 STEP 2: TRACK VARIABLE FLOW - BEFORE LOOP
    print("🚨 STEP BEFORE LOOP:", req.keywords)
    print("🚨 VARIABLE ID:", id(req.keywords))
    print("🚨 IS SAME OBJECT?", req.keywords is req.keywords)

    for kw in req.keywords[:5]:  # Hard cap at 5
        # 🚨 STEP 3: INSIDE LOOP - TRACK EACH KEYWORD
        print("🚨 USING KEYWORD:", kw)
        print("🚨 KEYWORD TYPE:", type(kw))
        print("🚨 KEYWORD FROM INDEX:", req.keywords.index(kw) if kw in req.keywords else "NOT_FOUND")
        
        rank = None
        try:
            print(f"[ONBOARDING] Checking keyword: \"{kw}\"")
            
            payload = [{
                "keyword": kw,
                "location_code": req.location_code,
                "language_code": req.language_code,
                "depth": 100,  # Top 100 results
                "device": "desktop",
                "os": "windows",
            }]

            # 🚨 STEP 4: CHECK API PAYLOAD - WHAT'S SENT TO DATASEO
            print("🚨 API PAYLOAD:", {
                "keyword_being_sent": kw,
                "keyword_in_payload": payload[0]["keyword"],
                "full_payload": payload[0],
                "payload_matches_kw": payload[0]["keyword"] == kw
            })

            try:
                resp = requests.post(SERP_API_URL, json=payload, headers=headers, timeout=30)

                if resp.status_code in (401, 402):
                    print(f"[ONBOARDING] SERP API auth/credit error for \"{kw}\"")
                    results.append(KeywordRank(keyword=kw, rank=None))
                    continue

                resp.raise_for_status()
                data = resp.json()

            except requests.exceptions.Timeout:
                print(f"[ONBOARDING] SERP API timeout for \"{kw}\"")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue
                
            except requests.exceptions.ConnectionError as e:
                print(f"[ONBOARDING] SERP API connection error for \"{kw}\": {str(e)}")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue
                
            except requests.exceptions.RequestException as e:
                print(f"[ONBOARDING] SERP API request error for \"{kw}\": {str(e)}")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue
                
            except Exception as e:
                print(f"[ONBOARDING] Unexpected error for \"{kw}\": {str(e)}")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue

            if data.get("status_code") != 20000:
                print(f"[ONBOARDING] SERP API error for \"{kw}\": {data.get('status_message')}")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue

            # Search through SERP items for domain match
            tasks = data.get("tasks", [])
            if not tasks:
                print(f"[ONBOARDING] No tasks in SERP response for \"{kw}\"")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue
                
            first_task = tasks[0]
            if not first_task.get("result"):
                print(f"[ONBOARDING] No results in first task for \"{kw}\"")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue

            serp_results_found = 0
            domains_checked = []
            ranking_urls_for_kw: List[RankingUrl] = []
            seen_urls: set = set()

            print(f"[ONBOARDING] 🔍 SEARCHING FOR DOMAIN: \"{clean_domain}\" in keyword \"{kw}\"")

            for result_item in first_task["result"]:
                items = result_item.get("items", [])
                if not items:
                    continue

                for serp_item in items:
                    if serp_item.get("type") != "organic":
                        continue

                    serp_results_found += 1

                    # Resolve URL — try all fields DataForSEO may use
                    serp_url = (
                        serp_item.get("url")
                        or serp_item.get("snippet_url")
                        or serp_item.get("link")
                        or ""
                    )

                    serp_domain = serp_item.get("domain", "")
                    if serp_url:
                        extracted = extract_domain_from_url(serp_url)
                        if extracted:
                            serp_domain = extracted

                    serp_domain_clean = normalize_domain(serp_domain)
                    rank_group = serp_item.get("rank_group")
                    rank_absolute = serp_item.get("rank_absolute")
                    item_rank = rank_group or rank_absolute

                    if serp_domain_clean:
                        domains_checked.append(serp_domain_clean)

                    # Domain matching — three strategies
                    is_match = (
                        serp_domain_clean == clean_domain
                        or (clean_domain and serp_domain_clean and clean_domain in serp_domain_clean)
                        or (serp_domain_clean and clean_domain and serp_domain_clean in clean_domain)
                    )

                    if is_match and item_rank and serp_url not in seen_urls:
                        seen_urls.add(serp_url)
                        url_type = _classify_url_type(serp_url)
                        ranking_urls_for_kw.append(RankingUrl(
                            rank=item_rank,
                            url=serp_url,
                            type=url_type
                        ))
                        print(f"[ONBOARDING] 🎯 MATCH rank={item_rank} type={url_type} url=\"{serp_url[:80]}\"")
                        # No break — scan all 100 results for every domain occurrence

            # Sort by rank ascending so index-0 is always the best position
            ranking_urls_for_kw.sort(key=lambda u: u.rank)

            if ranking_urls_for_kw:
                best_rank = ranking_urls_for_kw[0].rank
                rank = best_rank
                print(f"[ONBOARDING] 📊 \"{kw}\": best_rank={best_rank} | {len(ranking_urls_for_kw)} ranking URL(s)")
            else:
                best_rank = None
                rank = None
                print(f"[ONBOARDING] ❌ NO MATCH for \"{kw}\" | checked {len(domains_checked)} domains")

        except Exception as e:
            print(f"[ONBOARDING] SERP check failed for \"{kw}\" | error=\"{e}\"")
            import traceback
            print(f"[ONBOARDING] Traceback: {traceback.format_exc()}")
            rank = None
            best_rank = None
            ranking_urls_for_kw = []

        results.append(KeywordRank(
            keyword=kw,
            rank=rank,
            best_rank=best_rank,
            ranking_urls=ranking_urls_for_kw
        ))

    # 🚨 STEP 5: FINAL TRACE - WHAT WAS ACTUALLY PROCESSED
    print("🚨 FINAL TRACE COMPLETE:", {
        "original_keywords": req.keywords,
        "processed_keywords": [r.keyword for r in results],
        "keywords_match": req.keywords == [r.keyword for r in results],
        "results_count": len(results)
    })

    print(f"[ONBOARDING] check-ranking complete | results={len(results)} | rankings_found={sum(1 for r in results if r.rank is not None)}")
    return CheckRankingResponse(results=results)
