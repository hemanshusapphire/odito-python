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
    country: str = "US"
    language: str = "en"


class GenerateKeywordsResponse(BaseModel):
    keywords: List[str]


KEYWORD_SUGGESTIONS_URL = (
    "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_suggestions/live"
)


def _clean_query(sub_type: str, location: Optional[str] = None) -> str:
    """Clean and normalize query for DataForSEO API.
    
    Extracts only essential parts:
    - sub_type: e.g. "IT services"
    - location city: e.g. "New York"
    
    Removes:
    - Street addresses
    - Floor numbers
    - ZIP codes
    - Extra noise
    """
    import re
    
    # Clean sub_type
    sub_type = sub_type.strip()
    
    # Extract city from location if provided
    city = ""
    if location:
        # Remove street numbers, floor info, zip codes
        location_clean = re.sub(r'\d+.*?(st|ave|avenue|street|blvd|boulevard|floor|fl|#)\s*', '', location, flags=re.IGNORECASE)
        location_clean = re.sub(r'\b\d{5}\b', '', location_clean)  # Remove ZIP codes
        location_clean = re.sub(r',\s*USA$', '', location_clean, flags=re.IGNORECASE)
        location_clean = re.sub(r'\bSuite\s+\d+\b', '', location_clean, flags=re.IGNORECASE)
        
        # Extract first city name (before commas)
        parts = location_clean.split(',')
        if parts:
            city = parts[0].strip()
        
        # If city is empty after cleaning, try to extract from remaining parts
        if not city and len(parts) > 1:
            city = parts[1].strip()
    
    # Build clean query
    query = sub_type
    if city:
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


@router.post("/onboarding/generate-keywords", response_model=GenerateKeywordsResponse)
def generate_keywords(req: GenerateKeywordsRequest):
    """Return top-5 keyword suggestions for a given business sub-type."""

    # STEP 1: Clean the query
    clean_query = _clean_query(req.sub_type, req.location)
    
    location_code = COUNTRY_TO_LOCATION_CODE.get(req.country.upper(), 2840)
    language_code = req.language.lower() or "en"

    payload = [{
        "keyword": clean_query,
        "location_code": location_code,
        "language_code": language_code,
        "limit": 10,          # Fetch extra, pick top 5 by volume
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


class KeywordRank(BaseModel):
    keyword: str
    rank: Optional[int] = None            # None = not in top 100


class CheckRankingResponse(BaseModel):
    results: List[KeywordRank]


SERP_API_URL = (
    "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
)


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

            resp = requests.post(SERP_API_URL, json=payload, headers=headers, timeout=30)

            if resp.status_code in (401, 402):
                print(f"[ONBOARDING] SERP API auth/credit error for \"{kw}\"")
                results.append(KeywordRank(keyword=kw, rank=None))
                continue

            resp.raise_for_status()
            data = resp.json()

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
            rank = None  # 🚨 CRITICAL FIX: Initialize rank for each keyword
            
            print(f"[ONBOARDING] 🔍 SEARCHING FOR DOMAIN: \"{clean_domain}\" in keyword \"{kw}\"")
            print(f"[ONBOARDING] 📋 ALL SERP RESULTS FOR \"{kw}\":")
            
            for result_item in first_task["result"]:
                items = result_item.get("items", [])
                if not items:
                    continue
                    
                for serp_item in items:
                    if serp_item.get("type") != "organic":
                        continue
                    
                    serp_results_found += 1
                    
                    # Try multiple URL fields that DataForSEO might return
                    serp_url = ""
                    if "url" in serp_item:
                        serp_url = serp_item["url"]
                    elif "snippet_url" in serp_item:
                        serp_url = serp_item["snippet_url"]
                    elif "link" in serp_item:
                        serp_url = serp_item["link"]
                    
                    # Also try domain field directly
                    serp_domain = serp_item.get("domain", "")
                    
                    # Extract domain from URL if we have one
                    if serp_url:
                        extracted_domain = extract_domain_from_url(serp_url)
                        if extracted_domain:
                            serp_domain = extracted_domain
                    
                    # Normalize SERP domain for comparison
                    serp_domain_clean = normalize_domain(serp_domain)
                    
                    # Get rank info
                    rank_group = serp_item.get("rank_group")
                    rank_absolute = serp_item.get("rank_absolute")
                    
                    # Debug logging - Show ALL results
                    print(f"[ONBOARDING]   #{serp_results_found}: domain=\"{serp_domain_clean}\" | rank_group={rank_group} | rank_absolute={rank_absolute} | url=\"{serp_url[:60] if serp_url else 'N/A'}\"")
                    
                    # Store for domain checking
                    if serp_domain_clean:
                        domains_checked.append(serp_domain_clean)

                    # Multiple matching strategies
                    is_match = False
                    
                    # Strategy 1: Exact domain match
                    if serp_domain_clean == clean_domain:
                        is_match = True
                        print(f"[ONBOARDING] ✅ Exact match: \"{serp_domain_clean}\" == \"{clean_domain}\"")
                    
                    # Strategy 2: Domain contains match (for subdomains)
                    elif clean_domain and serp_domain_clean and clean_domain in serp_domain_clean:
                        is_match = True
                        print(f"[ONBOARDING] ✅ Contains match: \"{clean_domain}\" in \"{serp_domain_clean}\"")
                    
                    # Strategy 3: SERP domain contains target domain
                    elif serp_domain_clean and clean_domain and serp_domain_clean in clean_domain:
                        is_match = True
                        print(f"[ONBOARDING] ✅ Reverse contains match: \"{serp_domain_clean}\" in \"{clean_domain}\"")

                    if is_match:
                        rank = serp_item.get("rank_group") or serp_item.get("rank_absolute")
                        print(f"[ONBOARDING] 🎯 FOUND MATCH! \"{kw}\" → rank {rank} | domain=\"{serp_domain_clean}\" | url=\"{serp_url[:80] if serp_url else 'N/A'}\"")
                        break
                
                if rank is not None:
                    break

            print(f"[ONBOARDING] 📊 SUMMARY FOR \"{kw}\": Checked {len(domains_checked)} domains, found {len(set(domains_checked))} unique domains")

            if rank is None:
                print(f"[ONBOARDING] ❌ NO MATCH for \"{kw}\" | checked {len(domains_checked)} domains: {domains_checked[:10]}")
                if len(domains_checked) > 10:
                    print(f"[ONBOARDING] ... and {len(domains_checked) - 10} more domains")
                print(f"[ONBOARDING] 📊 FINAL RESULT for \"{kw}\": NOT FOUND (rank=None)")
            else:
                print(f"[ONBOARDING] 📊 FINAL RESULT for \"{kw}\": RANK {rank}")

        except Exception as e:
            print(f"[ONBOARDING] SERP check failed for \"{kw}\" | error=\"{e}\"")
            import traceback
            print(f"[ONBOARDING] Traceback: {traceback.format_exc()}")
            rank = None  # 🚨 CRITICAL FIX: Ensure rank is None on exception

        print(f"[ONBOARDING] 🎯 KEYWORD COMPLETE: \"{kw}\" → rank {rank}")
        results.append(KeywordRank(keyword=kw, rank=rank))

    # 🚨 STEP 5: FINAL TRACE - WHAT WAS ACTUALLY PROCESSED
    print("🚨 FINAL TRACE COMPLETE:", {
        "original_keywords": req.keywords,
        "processed_keywords": [r.keyword for r in results],
        "keywords_match": req.keywords == [r.keyword for r in results],
        "results_count": len(results)
    })

    print(f"[ONBOARDING] check-ranking complete | results={len(results)} | rankings_found={sum(1 for r in results if r.rank is not None)}")
    return CheckRankingResponse(results=results)
