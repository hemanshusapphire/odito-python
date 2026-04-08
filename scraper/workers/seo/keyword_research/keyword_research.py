"""KEYWORD_RESEARCH worker — Orchestrates keyword research via DataForSEO API.

Flow:
  1. Receive job payload (keyword, depth, projectId)
  2. Call DataForSEO Related Keywords API
  3. Process and normalize results
  4. Store raw response in seo_keyword_research collection
  5. Store processed keywords in seo_keyword_opportunities collection
  6. Callback to Node.js with completion status
"""

import os
import requests
import traceback
from datetime import datetime, timezone
from bson import ObjectId

from db import seo_keyword_research, seo_keyword_opportunities, seoprojects
from scraper.workers.seo.keyword_research.dataforseo_client import DataForSEOClient
from scraper.workers.seo.keyword_research.keyword_processor import KeywordProcessor


def fetch_project_settings(project_id: str) -> dict:
    """
    Fetch project location and language settings from MongoDB.
    
    Args:
        project_id: MongoDB project ID
        
    Returns:
        dict: Project settings with country, language, keywords
    """
    try:
        print(f"[KEYWORD_RESEARCH] Fetching project settings | projectId={project_id}")
        
        project = seoprojects.find_one({"_id": ObjectId(project_id)})
        
        if not project:
            print(f"[KEYWORD_RESEARCH] Project not found | projectId={project_id}")
            return {}
            
        settings = {
            "country": project.get("country", "US"),
            "language": project.get("language", "en"),
            "keywords": project.get("keywords", [])
        }
        
        print(f"[KEYWORD_RESEARCH] Project settings retrieved | country={settings['country']} | language={settings['language']} | keywords={settings['keywords']}")
        return settings
        
    except Exception as e:
        print(f"[KEYWORD_RESEARCH] Failed to fetch project settings | projectId={project_id} | error={str(e)}")
        return {}


def execute_keyword_research(job):
    """
    Execute keyword research via DataForSEO API.
    
    Args:
        job: Pydantic model with jobId, projectId, userId, keyword, depth

    Returns:
        dict with status and results
    """
    job_id = job.jobId
    project_id = ObjectId(job.projectId)  # FIXED: Convert to ObjectId
    keyword = job.keyword
    depth = job.depth or 2
    # Validate required environment variables
    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        raise Exception("NODE_BACKEND_URL is required")

    print(f"[KEYWORD_RESEARCH] Starting | jobId={job_id} | keyword=\"{keyword}\" | depth={depth} | timestamp={datetime.now(timezone.utc).isoformat()}")

    try:
        # STEP 0: Validate keyword input
        print(f"[KEYWORD_RESEARCH] Validating keyword input | jobId={job_id}")
        if not keyword or keyword.strip() == "":
            print(f"[KEYWORD_RESEARCH] Empty keyword provided, using fallback | jobId={job_id}")
            keyword = "default seo keyword"
        elif keyword.startswith("http"):
            print(f"[KEYWORD_RESEARCH] URL detected as keyword, using fallback | jobId={job_id} | invalid_keyword=\"{keyword}\"")
            keyword = "default seo keyword"
        else:
            keyword = keyword.strip()
        
        print(f"[KEYWORD_RESEARCH] Using keyword: \"{keyword}\" | jobId={job_id}")

        # STEP 1: Fetch project settings for location and language
        print(f"[KEYWORD_RESEARCH] Fetching project settings | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        project_settings = fetch_project_settings(project_id)
        
        # Debug: Show keyword source comparison
        project_keywords = project_settings.get('keywords', [])
        print(f"[KEYWORD_RESEARCH] Keyword source analysis | jobId={job_id} | job_keyword=\"{keyword}\" | project_keywords={project_keywords}")
        
        # STEP 2: Map country to DataForSEO location code
        COUNTRY_TO_LOCATION_CODE = {
            "US": 2840,  # United States
            "IN": 2356,  # India
            "UK": 2826,  # United Kingdom
            "GB": 2826,  # Great Britain (alternative)
            "CA": 2124,  # Canada
            "AU": 2036,  # Australia
            "DE": 2315,  # Germany
            "FR": 2250,  # France
            "ES": 2246,  # Spain
            "IT": 2240,  # Italy
            "JP": 2132,  # Japan
            "BR": 2075,  # Brazil
            "MX": 2239,  # Mexico
            "KR": 2131,  # South Korea
            "RU": 2306,  # Russia
        }
        
        country = project_settings.get('country', 'US').upper()
        language_code = project_settings.get('language', 'en').lower()
        location_code = COUNTRY_TO_LOCATION_CODE.get(country, 2840)  # Default to US
        
        print(f"[KEYWORD_RESEARCH] Final API parameters | jobId={job_id} | keyword=\"{keyword}\" | country=\"{country}\" | location_code={location_code} | language_code=\"{language_code}\"")

        # STEP 3: Call DataForSEO API with simplified parameters
        print(f"[KEYWORD_RESEARCH] Calling DataForSEO API | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        client = DataForSEOClient()
        raw_response = client.get_related_keywords(
            keyword=keyword, 
            depth=depth,
            location_code=location_code,
            language_code=language_code
        )

        # DEBUG: Log full API response structure (first 2000 chars)
        import json
        print(f"[KEYWORD_RESEARCH] DEBUG - Full API Response Structure | jobId={job_id}")
        print(json.dumps(raw_response, indent=2)[:2000])
        if len(json.dumps(raw_response)) > 2000:
            print("... (truncated for brevity)")

        # STEP 4: Store raw API response
        print(f"[KEYWORD_RESEARCH] Storing raw response | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        raw_doc = {
            "project_id": project_id,  # FIXED: Already converted to ObjectId above
            "job_id": job_id,
            "seed_keyword": keyword,
            "depth": depth,
            "raw_api_response": raw_response,
            "created_at": datetime.now(timezone.utc),
            "country_used": country,
            "location_code_used": location_code,
            "language_code_used": language_code
        }
        seo_keyword_research.insert_one(raw_doc)
        print(f"[KEYWORD_RESEARCH] Raw response stored | jobId={job_id}")

        # STEP 4: Process and normalize keywords
        print(f"[KEYWORD_RESEARCH] Processing keywords | jobId={job_id} | timestamp={datetime.now(timezone.utc).isoformat()}")
        processor = KeywordProcessor()
        processed_keywords = processor.process_results(raw_response, keyword)

        # FAIL-SAFE: Check if processing failed
        if not processed_keywords:
            print(f"[KEYWORD_RESEARCH] WARNING: No keywords processed from API response | jobId={job_id}")
            print(f"[KEYWORD_RESEARCH] This indicates a parsing issue - check above DEBUG logs for API structure")
            
            # Try to provide additional debugging info
            tasks_count = len(raw_response.get("tasks", []))
            print(f"[KEYWORD_RESEARCH] Response summary: {tasks_count} tasks found")
            
            if tasks_count > 0:
                first_task = raw_response["tasks"][0]
                results_count = len(first_task.get("result", []))
                print(f"[KEYWORD_RESEARCH] First task has {results_count} results")
                
                if results_count > 0:
                    first_result = first_task["result"][0]
                    items_count = len(first_result.get("items", []))
                    print(f"[KEYWORD_RESEARCH] First result has {items_count} items")
                    print(f"[KEYWORD_RESEARCH] First result keys: {list(first_result.keys())}")
                else:
                    print(f"[KEYWORD_RESEARCH] First result keys (no items): {list(first_result.keys())}")
            else:
                print(f"[KEYWORD_RESEARCH] Response keys (no tasks): {list(raw_response.keys())}")

        # STEP 5: Store processed keywords
        keywords_stored = 0
        if processed_keywords:
            print(f"[KEYWORD_RESEARCH] Storing {len(processed_keywords)} keywords | jobId={job_id}")

            # Prepare bulk operations with upsert to handle duplicates
            bulk_ops = []
            for kw in processed_keywords:
                # CRITICAL FIX: Include all required fields for frontend compatibility
                bulk_ops.append({
                    "filter": {
                        "project_id": project_id,  # FIXED: Already converted to ObjectId above
                        "keyword": kw["keyword"]
                    },
                    "update": {
                        "$set": {
                            "project_id": project_id,  # FIXED: Already converted to ObjectId above
                            "job_id": job_id,
                            "keyword": kw["keyword"],
                            "search_volume": kw.get("search_volume", 0),
                            "difficulty": kw.get("difficulty", 0),
                            "cpc": kw.get("cpc", 0),
                            "intent": kw.get("intent", "informational"),
                            "serp_features": kw.get("serp_features", []),
                            "keyword_info": kw.get("keyword_info", {}),
                            "keyword_properties": kw.get("keyword_properties", {}),
                            "keyword_difficulty": kw.get("keyword_difficulty", {}),
                            "impressions_info": kw.get("impressions_info", {}),
                            "created_at": datetime.now(timezone.utc),
                            "updated_at": datetime.now(timezone.utc)
                        }
                    },
                    "upsert": True
                })

            # Execute bulk upserts
            from pymongo import UpdateOne
            mongo_ops = [
                UpdateOne(op["filter"], op["update"], upsert=op["upsert"])
                for op in bulk_ops
            ]
            result = seo_keyword_opportunities.bulk_write(mongo_ops)
            keywords_stored = result.upserted_count + result.modified_count
            print(f"[KEYWORD_RESEARCH] Keywords stored | jobId={job_id} | upserted={result.upserted_count} | modified={result.modified_count}")
        else:
            print(f"[KEYWORD_RESEARCH] No keywords to store | jobId={job_id}")

        # STEP 5: Report job completion
        stats = {
            "keywords_found": len(processed_keywords),
            "keywords_stored": keywords_stored,
            "seed_keyword": keyword,
            "depth": depth
        }

        try:
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            complete_response = requests.post(
                complete_url,
                json={"stats": stats},
                timeout=10
            )
            complete_response.raise_for_status()
            print(f"[KEYWORD_RESEARCH] Job completion reported | jobId={job_id} | keywords_found={len(processed_keywords)} | timestamp={datetime.now(timezone.utc).isoformat()}")
        except Exception as complete_error:
            print(f"⚠️ [KEYWORD_RESEARCH] Failed to report completion | jobId={job_id} | error={str(complete_error)}")
            # Try to report failure instead
            try:
                fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
                requests.post(
                    fail_url,
                    json={"error": f"Completion reporting failed: {str(complete_error)}", "stats": stats},
                    timeout=10
                )
            except Exception:
                pass

        print(f"[KEYWORD_RESEARCH] Completed | jobId={job_id} | keywords_found={len(processed_keywords)} | timestamp={datetime.now(timezone.utc).isoformat()}")

        return {
            "status": "completed",
            "jobId": job_id,
            **stats
        }

    except Exception as e:
        print(f"❌ [KEYWORD_RESEARCH] Worker failed | jobId={job_id} | error={str(e)} | timestamp={datetime.now(timezone.utc).isoformat()}")
        traceback.print_exc()

        # Report failure to Node.js
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            requests.post(
                fail_url,
                json={"error": str(e), "stats": {}},
                timeout=10
            )
            print(f"[KEYWORD_RESEARCH] Failure reported | jobId={job_id}")
        except Exception as fail_error:
            print(f"⚠️ [KEYWORD_RESEARCH] Failed to report failure | error={str(fail_error)}")

        return {
            "status": "failed",
            "jobId": job_id,
            "error": str(e)
        }
