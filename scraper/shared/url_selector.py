"""
Shared URL selection utilities for deterministic, type-based URL selection.
Used by Page Scraping, Headless Accessibility, and URL Qualification workers.
"""

from typing import List, Dict, Any
from bson.objectid import ObjectId
from db import seo_internal_links
from .url_filters import should_skip_seo_url

# Type rank used for deterministic ordering across all workers
_TYPE_RANK: Dict[str, int] = {
    "main": 0,
    "service": 1,
    "blog": 2,
    "portfolio": 3,
}


def get_candidate_pool(project_id: str, max_size: int = 20000) -> List[Dict[str, Any]]:
    """
    Return ALL discovered URLs for a project with type metadata, sorted
    deterministically by (type_rank ASC, url_length ASC, url ASC).

    Used by URL_QUALIFICATION to build the probe candidate pool. The only
    caller (url_qualification/worker.py) always passes its own
    CANDIDATE_POOL_SIZE explicitly, so this default is a fallback only.

    Args:
        project_id: Project ID (string or ObjectId)
        max_size:   Upper bound on returned pool (default 20000 — high enough
                    to cover real-world single-site discovery counts so the
                    URL Selection screen can show the full discovered set;
                    still bounded to protect against pathological outliers)

    Returns:
        List of dicts: {url, type, type_rank, sourceUrl, sourceSitemap}
        sourceUrl/sourceSitemap are discovery-provenance fields carried
        straight through from seo_internal_links (may be None) — purely
        additive, for downstream explainability (e.g. URL_QUALIFICATION's
        qualification_details); no effect on pool selection/ordering.
    """
    if isinstance(project_id, str):
        project_id = ObjectId(project_id)

    print(f"[URL_SELECTOR] Building candidate pool | projectId={project_id} | maxSize={max_size}")

    try:
        docs = list(seo_internal_links.find(
            {"projectId": project_id},
            {"url": 1, "type": 1, "sourceUrl": 1, "sourceSitemap": 1, "_id": 0}
        ))

        seen: set = set()
        unique: List[Dict[str, Any]] = []
        for doc in docs:
            url = doc.get("url")
            if not url or url in seen:
                continue
            if should_skip_seo_url(url):
                continue
            seen.add(url)
            page_type = (doc.get("type") or "other").strip().lower()
            unique.append({
                "url": url,
                "type": page_type,
                "type_rank": _TYPE_RANK.get(page_type, 99),
                "sourceUrl": doc.get("sourceUrl"),
                "sourceSitemap": doc.get("sourceSitemap"),
            })

        # Deterministic: type_rank ASC, URL length ASC, URL lexicographic ASC
        unique.sort(key=lambda d: (d["type_rank"], len(d["url"]), d["url"]))

        result = unique[:max_size]
        print(f"[URL_SELECTOR] Candidate pool built | total={len(result)}")
        return result

    except Exception as exc:
        print(f"[URL_SELECTOR] get_candidate_pool ERROR: {exc}")
        return []


def get_top_urls(project_id: str, limit: int = 25) -> List[str]:
    """
    Get top URLs for a project using deterministic, type-based selection
    
    Priority order:
    1. main pages
    2. service pages
    3. other pages (fallback)
    
    Args:
        project_id: Project ID (string or ObjectId)
        limit: Maximum number of URLs to return (default: 25)
        
    Returns:
        List of URLs in deterministic order
    """
    # Convert to ObjectId if needed
    if isinstance(project_id, str):
        project_id = ObjectId(project_id)
    
    print(f"[URL_SELECTOR] Getting top {limit} URLs for project {project_id}")
    
    try:
        # Step 1: Get primary URLs (main + service pages)
        primary_urls = list(seo_internal_links.find(
            {
                "projectId": project_id,
                "type": {"$in": ["main", "service"]}
            },
            {"url": 1, "type": 1}
        ).sort({
            "type": 1,  # main first, then service
            "url": 1   # alphabetical for consistency
        }).limit(limit))
        
        primary_url_list = [doc["url"] for doc in primary_urls if doc.get("url") and not should_skip_seo_url(doc["url"])]
        
        print(f"[URL_SELECTOR] Primary URLs found: {len(primary_url_list)} (main: {sum(1 for doc in primary_urls if doc.get('type') == 'main')}, service: {sum(1 for doc in primary_urls if doc.get('type') == 'service')})")
        
        # Step 2: If we need more URLs, get fallback URLs
        if len(primary_url_list) < limit:
            remaining_needed = limit - len(primary_url_list)
            
            print(f"[URL_SELECTOR] Need {remaining_needed} more URLs from fallback")
            
            # Get fallback URLs (excluding main/service to avoid duplicates)
            fallback_urls = list(seo_internal_links.find(
                {
                    "projectId": project_id,
                    "$or": [
                        {"type": {"$nin": ["main", "service"]}},
                        {"type": {"$exists": False}}
                    ]
                },
                {"url": 1, "type": 1}
            ).sort({
                "type": 1,  # deterministic by type
                "url": 1    # alphabetical for consistency
            }).limit(remaining_needed))
            
            fallback_url_list = [doc["url"] for doc in fallback_urls if doc.get("url") and not should_skip_seo_url(doc["url"])]
            
            print(f"[URL_SELECTOR] Fallback URLs found: {len(fallback_url_list)}")
            
            # Combine primary and fallback
            final_urls = primary_url_list + fallback_url_list
        else:
            final_urls = primary_url_list
        
        # Step 3: Remove duplicates and limit to exact count
        seen = set()
        unique_urls = []
        for url in final_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
                if len(unique_urls) >= limit:
                    break
        
        print(f"[URL_SELECTOR] Final selection: {len(unique_urls)} URLs (main: {sum(1 for doc in primary_urls if doc.get('type') == 'main')}, service: {sum(1 for doc in primary_urls if doc.get('type') == 'service')}, fallback: {len(unique_urls) - len(primary_url_list)})")
        
        return unique_urls[:limit]
    
    except Exception as e:
        print(f"[URL_SELECTOR] ERROR: Failed to get URLs from database: {e}")
        # Return empty list on error to allow graceful handling
        return []


def get_urls_from_job_or_db(project_id: str, job_urls: List[str] = None, limit: int = 25) -> List[str]:
    """
    Get URLs either from job input or from database using type-based selection
    
    Args:
        project_id: Project ID
        job_urls: URLs provided in job (optional)
        limit: Maximum number of URLs
        
    Returns:
        List of URLs to process
    """
    if job_urls and len(job_urls) > 0:
        # Use job URLs but apply deterministic ordering, filtering, and limit
        print(f"[URL_SELECTOR] Using {len(job_urls)} URLs from job input")

        # Deduplicate, filter excluded pages, then sort for consistency
        sorted_urls = sorted(u for u in set(job_urls) if not should_skip_seo_url(u))
        return sorted_urls[:limit]
    else:
        # Use type-based selection from database
        return get_top_urls(project_id, limit)
