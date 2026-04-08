"""DataForSEO API client for Related Keywords endpoint.

Handles authentication, request formatting, retry logic, and error handling
for the DataForSEO Labs Google Related Keywords API.
"""

import os
import time
import requests
import base64
from datetime import datetime, timezone


class DataForSEOClient:
    """Client for the DataForSEO APIs (Related Keywords and SERP)."""

    RELATED_KEYWORDS_API_URL = "https://api.dataforseo.com/v3/dataforseo_labs/google/related_keywords/live"
    SERP_API_URL = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 2  # seconds

    def __init__(self):
        self.login = os.getenv("DATAFORSEO_LOGIN", "")
        self.password = os.getenv("DATAFORSEO_PASSWORD", "")

        if not self.login or not self.password:
            print("[DATAFORSEO] WARNING: DATAFORSEO_LOGIN or DATAFORSEO_PASSWORD not set in environment")

    def _get_auth_header(self):
        """Generate HTTP Basic Auth header."""
        credentials = f"{self.login}:{self.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def get_related_keywords(self, keyword, depth=2, limit=50, location_code=2840, language_code="en"):
        """
        Fetch related keywords from DataForSEO API.

        Args:
            keyword: Seed keyword to find related keywords for
            depth: Depth of related keywords tree (1-4)
            limit: Maximum number of results
            location_code: DataForSEO location code (default: 2840 = United States)
            language_code: DataForSEO language code (default: "en")

        Returns:
            dict: Raw API response data

        Raises:
            Exception: If all retry attempts fail
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "depth": depth,
            "limit": limit
        }]

        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/json"
        }

        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                print(f"[DATAFORSEO] API request attempt {attempt}/{self.MAX_RETRIES} | keyword=\"{keyword}\" | depth={depth} | location_code={location_code} | language_code={language_code} | timestamp={datetime.now(timezone.utc).isoformat()}")

                response = requests.post(
                    self.API_URL,
                    json=payload,
                    headers=headers,
                    timeout=60
                )

                # Check HTTP status
                if response.status_code == 401:
                    raise Exception("DataForSEO authentication failed — check DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD")

                if response.status_code == 402:
                    raise Exception("DataForSEO account has insufficient credits")

                response.raise_for_status()

                data = response.json()

                # Check API-level status
                if data.get("status_code") != 20000:
                    error_msg = data.get("status_message", "Unknown API error")
                    raise Exception(f"DataForSEO API error: {error_msg} (code: {data.get('status_code')})")

                print(f"[DATAFORSEO] API request successful | keyword=\"{keyword}\" | location_code={location_code} | language_code={language_code} | timestamp={datetime.now(timezone.utc).isoformat()}")
                return data

            except requests.exceptions.Timeout:
                last_error = Exception(f"DataForSEO API timeout on attempt {attempt}")
                print(f"[DATAFORSEO] Request timeout | attempt={attempt}/{self.MAX_RETRIES}")

            except requests.exceptions.ConnectionError as e:
                last_error = Exception(f"DataForSEO connection error: {str(e)}")
                print(f"[DATAFORSEO] Connection error | attempt={attempt}/{self.MAX_RETRIES} | error=\"{str(e)}\"")

            except Exception as e:
                last_error = e
                # Don't retry auth or credit errors
                if "authentication failed" in str(e).lower() or "insufficient credits" in str(e).lower():
                    print(f"[DATAFORSEO] Non-retryable error | reason=\"{str(e)}\"")
                    raise
                print(f"[DATAFORSEO] Request failed | attempt={attempt}/{self.MAX_RETRIES} | reason=\"{str(e)}\"")

            # Exponential backoff before retry
            if attempt < self.MAX_RETRIES:
                backoff = self.RETRY_BACKOFF_BASE ** attempt
                print(f"[DATAFORSEO] Retrying in {backoff}s...")
                time.sleep(backoff)

        # All retries exhausted
        raise Exception(f"DataForSEO API failed after {self.MAX_RETRIES} attempts: {str(last_error)}")

    def get_keyword_ranking(self, keyword, domain, location_code=2036, language_code="en", device="desktop", depth=100):
        """
        Get keyword ranking for a specific domain using SERP API.

        Args:
            keyword: Keyword to search for
            domain: Domain to check ranking for (e.g., "wowinfotech.com")
            location_code: DataForSEO location code (default: 2036 = India)
            language_code: DataForSEO language code (default: "en")
            device: Device type (default: "desktop")
            depth: Maximum depth to search (default: 100)

        Returns:
            dict: Ranking information with rank, url, title or None if not found
        """
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "device": device,
            "depth": depth
        }]

        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/json"
        }

        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                print(f"[DATAFORSEO_SERP] API request attempt {attempt}/{self.MAX_RETRIES} | keyword=\"{keyword}\" | domain=\"{domain}\" | location_code={location_code} | timestamp={datetime.now(timezone.utc).isoformat()}")

                response = requests.post(
                    self.SERP_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=60
                )

                # Check HTTP status
                if response.status_code == 401:
                    raise Exception("DataForSEO authentication failed — check DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD")

                if response.status_code == 402:
                    raise Exception("DataForSEO account has insufficient credits")

                response.raise_for_status()

                data = response.json()

                # Check API-level status
                if data.get("status_code") != 20000:
                    error_msg = data.get("status_message", "Unknown API error")
                    raise Exception(f"DataForSEO API error: {error_msg} (code: {data.get('status_code')})")

                print(f"[DATAFORSEO_SERP] API request successful | keyword=\"{keyword}\" | domain=\"{domain}\" | timestamp={datetime.now(timezone.utc).isoformat()}")

                # Process SERP results
                if not data.get('tasks') or len(data['tasks']) == 0:
                    raise Exception("No tasks in SERP response")

                task = data['tasks'][0]
                
                if task.get('status_code') != 20000:
                    raise Exception(f"SERP Task Error: {task.get('status_message', 'Unknown error')}")

                result = task.get('result', [])
                if not result or len(result) == 0:
                    raise Exception("No results in SERP task")

                organic_items = result[0].get('items', [])
                if not organic_items:
                    return {
                        'keyword': keyword,
                        'rank': 'Not Found',
                        'url': None,
                        'title': None,
                        'found': False,
                        'total_results': 0
                    }

                print(f"[DATAFORSEO_SERP] Found {len(organic_items)} organic results for keyword=\"{keyword}\"")

                # Search for our domain
                for item in organic_items:
                    item_domain = item.get('domain', '').lower()
                    
                    if domain.lower() in item_domain:
                        rank_group = item.get('rank_group')
                        url = item.get('url', 'N/A')
                        title = item.get('title', 'N/A')
                        
                        print(f"[DATAFORSEO_SERP] Found domain '{domain}' at rank {rank_group} for keyword=\"{keyword}\"")
                        
                        return {
                            'keyword': keyword,
                            'rank': rank_group,
                            'url': url,
                            'title': title,
                            'found': True,
                            'total_results': len(organic_items)
                        }

                # Domain not found
                print(f"[DATAFORSEO_SERP] Domain '{domain}' not found in top {len(organic_items)} results for keyword=\"{keyword}\"")
                
                return {
                    'keyword': keyword,
                    'rank': 'Not Found',
                    'url': None,
                    'title': None,
                    'found': False,
                    'total_results': len(organic_items)
                }

            except requests.exceptions.Timeout:
                last_error = Exception(f"DataForSEO SERP API timeout on attempt {attempt}")
                print(f"[DATAFORSEO_SERP] Request timeout | attempt={attempt}/{self.MAX_RETRIES}")

            except requests.exceptions.ConnectionError as e:
                last_error = Exception(f"DataForSEO SERP connection error: {str(e)}")
                print(f"[DATAFORSEO_SERP] Connection error | attempt={attempt}/{self.MAX_RETRIES} | error=\"{str(e)}\"")

            except Exception as e:
                last_error = e
                # Don't retry auth or credit errors
                if "authentication failed" in str(e).lower() or "insufficient credits" in str(e).lower():
                    print(f"[DATAFORSEO_SERP] Non-retryable error | reason=\"{str(e)}\"")
                    raise
                print(f"[DATAFORSEO_SERP] Request failed | attempt={attempt}/{self.MAX_RETRIES} | reason=\"{str(e)}\"")

            # Exponential backoff before retry
            if attempt < self.MAX_RETRIES:
                backoff = self.RETRY_BACKOFF_BASE ** attempt
                print(f"[DATAFORSEO_SERP] Retrying in {backoff}s...")
                time.sleep(backoff)

        # All retries exhausted
        raise Exception(f"DataForSEO SERP API failed after {self.MAX_RETRIES} attempts: {str(last_error)}")

    def get_multiple_keyword_rankings(self, keywords, domain, location_code=2036, language_code="en", device="desktop", depth=100):
        """
        Get rankings for multiple keywords.

        Args:
            keywords: List of keywords to search for
            domain: Domain to check ranking for
            location_code: DataForSEO location code (default: 2036 = India)
            language_code: DataForSEO language code (default: "en")
            device: Device type (default: "desktop")
            depth: Maximum depth to search (default: 100)

        Returns:
            list: List of ranking results for each keyword
        """
        results = []
        
        for keyword in keywords:
            try:
                result = self.get_keyword_ranking(
                    keyword=keyword,
                    domain=domain,
                    location_code=location_code,
                    language_code=language_code,
                    device=device,
                    depth=depth
                )
                results.append(result)
            except Exception as e:
                print(f"[DATAFORSEO_SERP] Failed to get ranking for keyword=\"{keyword}\": {str(e)}")
                results.append({
                    'keyword': keyword,
                    'rank': 'Error',
                    'url': None,
                    'title': None,
                    'found': False,
                    'error': str(e),
                    'total_results': 0
                })
        
        return results
