"""Keyword result processor for DataForSEO Related Keywords API responses.

Extracts and normalizes keyword data from the nested DataForSEO response
structure into flat records suitable for MongoDB storage.
"""

from datetime import datetime, timezone


class KeywordProcessor:
    """Processes raw DataForSEO API responses into normalized keyword records."""

    @staticmethod
    def process_results(raw_response, seed_keyword):
        """
        Extract and normalize keywords from a DataForSEO Related Keywords response.

        Args:
            raw_response: Complete API response dict from DataForSEO
            seed_keyword: The original seed keyword used for the query

        Returns:
            list[dict]: Normalized keyword records ready for MongoDB insertion
        """
        keywords = []
        seen_keywords = set()  # Deduplicate within a single response

        try:
            print(f"[KEYWORD_PROCESSOR] Processing API response | seed=\"{seed_keyword}\"")
            
            # STEP 1: Check API-level errors first
            tasks = raw_response.get("tasks", [])
            print(f"[KEYWORD_PROCESSOR] Found {len(tasks)} tasks in response")
            
            if not tasks:
                print(f"[KEYWORD_PROCESSOR] ERROR: No tasks in API response | seed=\"{seed_keyword}\"")
                return keywords

            # STEP 2: Check first task for API errors
            first_task = tasks[0]
            task_status_code = first_task.get("status_code")
            task_status_message = first_task.get("status_message", "Unknown error")
            
            if task_status_code != 20000:
                print(f"[KEYWORD_PROCESSOR] API ERROR: {task_status_message} (code: {task_status_code}) | seed=\"{seed_keyword}\"")
                return keywords

            # STEP 3: Extract results safely
            results = first_task.get("result") or []
            print(f"[KEYWORD_PROCESSOR] Found {len(results)} results in first task")
            
            # CRITICAL FIX: Check if results is empty before accessing [0]
            if not results:
                print(f"[KEYWORD_PROCESSOR] ERROR: No results in task | seed=\"{seed_keyword}\"")
                return keywords
            
            # STEP 4: Extract items from first result
            first_result = results[0]
            items = first_result.get("items", [])
            print(f"[KEYWORD_PROCESSOR] Found {len(items)} items in first result")
            
            if items:
                # Process standard structure
                for i, item in enumerate(items):
                    keyword_data = KeywordProcessor._extract_keyword_from_item(item, i, seed_keyword, seen_keywords)
                    if keyword_data:
                        keywords.append(keyword_data)
            else:
                print(f"[KEYWORD_PROCESSOR] No items in first result, checking for alternative structures")
                # Check for related_keywords directly in result
                related_keywords = first_result.get("related_keywords", [])
                if related_keywords:
                    print(f"[KEYWORD_PROCESSOR] Found {len(related_keywords)} related_keywords in alternative structure")
                    for i, kw_data in enumerate(related_keywords):
                        keyword_data = KeywordProcessor._extract_from_related_keywords(kw_data, i, seed_keyword, seen_keywords)
                        if keyword_data:
                            keywords.append(keyword_data)
                else:
                    print(f"[KEYWORD_PROCESSOR] First result structure: {list(first_result.keys())}")
                    # Print full result for debugging
                    import json
                    print(f"[KEYWORD_PROCESSOR] Full first result: {json.dumps(first_result, indent=2)[:1000]}")

            print(f"[KEYWORD_PROCESSOR] Processed {len(keywords)} keywords | seed=\"{seed_keyword}\"")

        except Exception as e:
            print(f"[KEYWORD_PROCESSOR] Error processing results | seed=\"{seed_keyword}\" | error=\"{str(e)}\"")
            import traceback
            print(f"[KEYWORD_PROCESSOR] Full traceback: {traceback.format_exc()}")

        return keywords

    @staticmethod
    def _extract_keyword_from_item(item, index, seed_keyword, seen_keywords):
        """Extract keyword data from standard DataForSEO item structure."""
        try:
            print(f"[KEYWORD_PROCESSOR] Processing item {index+1}")
            print(f"[KEYWORD_PROCESSOR] Item structure: {list(item.keys())}")
            
            # Initialize with safe defaults
            keyword_text = None
            search_volume = 0
            competition = 0.0
            cpc = 0.0
            difficulty = 0
            serp_features = []  # Will be replaced with detected features
            
            # Method 1: Direct keyword_data structure
            if "keyword_data" in item:
                keyword_data = item["keyword_data"]
                keyword_text = keyword_data.get("keyword")
                
                # Try keyword_info nested structure
                if "keyword_info" in keyword_data:
                    keyword_info = keyword_data["keyword_info"]
                    search_volume = keyword_info.get("search_volume", 0) or 0
                    competition = keyword_info.get("competition", 0.0) or 0.0
                    cpc = keyword_info.get("cpc", 0.0) or 0.0
                
                # Try keyword_properties for difficulty
                if "keyword_properties" in keyword_data:
                    keyword_properties = keyword_data["keyword_properties"]
                    difficulty = keyword_properties.get("keyword_difficulty", 0) or 0
                
                # Extract SERP features from keyword_info if available
                if "keyword_info" in keyword_data:
                    serp_data = keyword_data["keyword_info"].get("serp_info", {})
                    if serp_data:
                        serp_features = list(serp_data.keys())
                    else:
                        # Use detection logic when no SERP data available
                        serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)
                else:
                    # Use detection logic when no keyword_info available
                    serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)
            
            # Method 2: Direct fields in item
            elif "keyword" in item:
                keyword_text = item.get("keyword")
                search_volume = item.get("search_volume", 0) or 0
                competition = item.get("competition", 0.0) or 0.0
                cpc = item.get("cpc", 0.0) or 0.0
                difficulty = item.get("difficulty") or item.get("keyword_difficulty", 0) or 0
                
                # Extract SERP features if present
                if "serp_features" in item:
                    serp_features_raw = item["serp_features"]
                    if isinstance(serp_features_raw, list):
                        serp_features = serp_features_raw
                    elif isinstance(serp_features_raw, dict):
                        serp_features = list(serp_features_raw.keys())
                    else:
                        # Use detection logic when serp_features is invalid
                        serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)
                else:
                    # Use detection logic when no serp_features field present
                    serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)

            # Method 3: Check for any field that looks like a keyword
            else:
                print(f"[KEYWORD_PROCESSOR] Unknown item structure for item {index+1}")
                return None

            # Validate keyword
            if not keyword_text or not keyword_text.strip():
                print(f"[KEYWORD_PROCESSOR] Skipping item {index+1} - no keyword text")
                return None

            # Deduplicate within response
            keyword_lower = keyword_text.lower().strip()
            if keyword_lower in seen_keywords:
                print(f"[KEYWORD_PROCESSOR] Skipping duplicate keyword: \"{keyword_text}\"")
                return None
            seen_keywords.add(keyword_lower)

            # CRITICAL: Classify intent based on keyword patterns
            intent = KeywordProcessor._classify_intent(keyword_text)
            
            # CRITICAL: Ensure SERP features are always detected and populated
            if not serp_features or len(serp_features) == 0:
                serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)
            
            # CRITICAL: Normalize data types and ensure consistency
            search_volume = int(search_volume) if search_volume else 0
            difficulty = int(difficulty) if difficulty else 0
            cpc = float(cpc) if cpc else 0.0
            competition = float(competition) if competition else 0.0
            
            # Ensure serp_features is always an array and never empty
            if not isinstance(serp_features, list):
                serp_features = []
            if len(serp_features) == 0:
                serp_features = ["organic"]  # Final fallback

            print(f"[KEYWORD_PROCESSOR] Extracted keyword: \"{keyword_text}\" | volume: {search_volume} | difficulty: {difficulty} | intent: {intent}")

            return {
                "keyword": keyword_text.strip(),
                "search_volume": search_volume,
                "competition": competition,
                "cpc": cpc,
                "difficulty": difficulty,
                "intent": intent,  # NEW: Intent classification
                "serp_features": serp_features,  # NEW: Always array
                "source_keyword": seed_keyword,
                "created_at": datetime.now(timezone.utc)
            }

        except Exception as e:
            print(f"[KEYWORD_PROCESSOR] Error extracting from item {index+1}: {str(e)}")
            return None

    @staticmethod
    def detect_serp_features(keyword: str, search_volume: int) -> list:
        """
        Detect SERP features based on keyword patterns and search volume.
        
        Args:
            keyword: The keyword to analyze
            search_volume: Monthly search volume
            
        Returns:
            list: Detected SERP features (always contains at least 'organic')
        """
        try:
            # Safety check for None keyword
            if not keyword or not isinstance(keyword, str):
                print(f"[SERP_FEATURES] Invalid keyword, using default features")
                return ["organic"]
            
            keyword_lower = keyword.lower().strip()
            serp_features = ["organic"]  # Always include organic
            
            # Rule 1: High volume keywords get AI overview
            if search_volume > 10000:
                serp_features.append("ai_overview")
            
            # Rule 2: Informational patterns get People Also Ask
            informational_patterns = ["how", "what", "why", "guide", "tutorial"]
            if any(pattern in keyword_lower for pattern in informational_patterns):
                if "people_also_ask" not in serp_features:
                    serp_features.append("people_also_ask")
            
            # Rule 3: Local intent patterns get Local Pack
            local_patterns = ["near me", "local", "services", "company"]
            if any(pattern in keyword_lower for pattern in local_patterns):
                if "local_pack" not in serp_features:
                    serp_features.append("local_pack")
            
            # Rule 4: Video intent gets Video carousel
            video_patterns = ["video", "youtube"]
            if any(pattern in keyword_lower for pattern in video_patterns):
                if "video" not in serp_features:
                    serp_features.append("video")
            
            # Remove duplicates and return
            serp_features = list(dict.fromkeys(serp_features))  # Preserve order, remove duplicates
            
            print(f"[SERP_FEATURES] keyword=\"{keyword}\" → {serp_features}")
            return serp_features
            
        except Exception as e:
            print(f"[SERP_FEATURES] Error detecting features for keyword=\"{keyword}\": {str(e)}")
            return ["organic"]  # Fallback safety

    @staticmethod
    def _classify_intent(keyword_text):
        """
        Classify keyword intent based on common patterns.
        
        Args:
            keyword_text: The keyword to classify
            
        Returns:
            str: One of 'informational', 'commercial', 'navigational'
        """
        keyword_lower = keyword_text.lower().strip()
        
        # Commercial intent indicators
        commercial_patterns = [
            'buy', 'price', 'cost', 'cheap', 'best', 'review', 'deal', 'discount',
            'service', 'agency', 'company', 'near me', 'for sale', 'quote',
            'pricing', 'rates', 'affordable', 'professional', 'expert'
        ]
        
        # Navigational intent indicators  
        navigational_patterns = [
            'login', 'signin', 'account', 'dashboard', 'portal', 'console',
            'analytics', 'tools', 'software', 'app', 'website', 'official',
            'support', 'help', 'contact', 'customer service'
        ]
        
        # Check for commercial intent first (most specific)
        for pattern in commercial_patterns:
            if pattern in keyword_lower:
                return 'commercial'
        
        # Check for navigational intent
        for pattern in navigational_patterns:
            if pattern in keyword_lower:
                return 'navigational'
        
        # Default to informational
        return 'informational'

    @staticmethod
    def _extract_from_related_keywords(kw_data, index, seed_keyword, seen_keywords):
        """Extract keyword from alternative related_keywords structure."""
        try:
            keyword_text = kw_data.get("keyword") or kw_data.get("related_keyword")
            if not keyword_text:
                return None
                
            keyword_lower = keyword_text.lower().strip()
            if keyword_lower in seen_keywords:
                return None
            seen_keywords.add(keyword_lower)

            # CRITICAL: Classify intent and normalize data
            intent = KeywordProcessor._classify_intent(keyword_text)
            
            search_volume = int(kw_data.get("search_volume", 0)) or 0
            competition = float(kw_data.get("competition", 0.0)) or 0.0
            cpc = float(kw_data.get("cpc", 0.0)) if kw_data.get("cpc") else 0.0
            difficulty = int(kw_data.get("difficulty", 0)) if kw_data.get("difficulty") else 0
            
            # Handle SERP features with detection logic
            serp_features = []
            if "serp_features" in kw_data:
                serp_raw = kw_data["serp_features"]
                if isinstance(serp_raw, list):
                    serp_features = serp_raw
                elif isinstance(serp_raw, dict):
                    serp_features = list(serp_raw.keys())
            
            # CRITICAL: Always detect SERP features if empty or missing
            if not serp_features or len(serp_features) == 0:
                serp_features = KeywordProcessor.detect_serp_features(keyword_text, search_volume)

            return {
                "keyword": keyword_text.strip(),
                "search_volume": search_volume,
                "competition": competition,
                "cpc": cpc,
                "difficulty": difficulty,
                "intent": intent,  # NEW: Intent classification
                "serp_features": serp_features,  # NEW: Always array
                "source_keyword": seed_keyword,
                "created_at": datetime.now(timezone.utc)
            }
        except Exception as e:
            print(f"[KEYWORD_PROCESSOR] Error extracting from related_keywords {index+1}: {str(e)}")
            return None
