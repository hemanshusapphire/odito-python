"""
Improved AI Visibility Extraction Functions
Production-grade extraction logic for 60+ AI SEO rules
"""

import re
import json
from typing import Dict, List, Any
from bs4 import BeautifulSoup


def extract_comprehensive_nap_signals(soup) -> Dict[str, Any]:
    """Extract comprehensive NAP (Name, Address, Phone) signals for local SEO"""
    nap_data = {
        "business_name": "",
        "business_legal_name": "",
        "phone": {"primary": "", "formatted": "", "variations": []},
        "address": {
            "street": "", "city": "", "state": "", "zip": "", "country": "",
            "full": "", "formatted": ""
        },
        "geo_coordinates": {"lat": 0, "lng": 0, "present": False},
        "opening_hours": {"present": False, "hours_specification": []},
        "localbusiness_schema": {"present": False, "complete": False, "type": ""},
        "nap_consistency": {"consistent": False, "variations": [], "completeness_score": 0}
    }
    
    try:
        # 1. Extract from LocalBusiness/Organization schema
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = _flatten_graph_entities(data)
                
                for entity in entities:
                    entity_type = entity.get('@type', '')
                    if isinstance(entity_type, list):
                        entity_type = entity_type[0] if entity_type else ""
                    
                    if entity_type in ['LocalBusiness', 'Organization', 'Restaurant', 'Hotel', 'Store']:
                        nap_data["localbusiness_schema"]["present"] = True
                        nap_data["localbusiness_schema"]["type"] = entity_type
                        
                        # Business name (support both name and legalName)
                        if entity.get('name'):
                            nap_data["business_name"] = entity['name']
                        if entity.get('legalName'):
                            nap_data["business_legal_name"] = entity['legalName']
                        
                        # Complete address extraction
                        if 'address' in entity:
                            address = entity['address']
                            if isinstance(address, dict):
                                nap_data["address"]["street"] = address.get('streetAddress', '')
                                nap_data["address"]["city"] = address.get('addressLocality', '')
                                nap_data["address"]["state"] = address.get('addressRegion', '')
                                nap_data["address"]["zip"] = address.get('postalCode', '')
                                nap_data["address"]["country"] = address.get('addressCountry', '')
                                
                                # Full formatted address
                                address_parts = [
                                    nap_data["address"]["street"],
                                    nap_data["address"]["city"],
                                    nap_data["address"]["state"],
                                    nap_data["address"]["zip"]
                                ]
                                nap_data["address"]["full"] = ', '.join(filter(None, address_parts))
                                
                                if 'addressCountry' in address:
                                    nap_data["address"]["formatted"] = f"{nap_data['address']['full']}, {address['addressCountry']}"
                                else:
                                    nap_data["address"]["formatted"] = nap_data["address"]["full"]
                        
                        # Phone extraction
                        if 'telephone' in entity:
                            nap_data["phone"]["primary"] = entity['telephone']
                        
                        # Geo coordinates
                        if 'geo' in entity:
                            geo = entity['geo']
                            if isinstance(geo, dict):
                                nap_data["geo_coordinates"]["lat"] = float(geo.get('latitude', 0))
                                nap_data["geo_coordinates"]["lng"] = float(geo.get('longitude', 0))
                                nap_data["geo_coordinates"]["present"] = True
                        
                        # Opening hours
                        if 'openingHoursSpecification' in entity:
                            nap_data["opening_hours"]["present"] = True
                            nap_data["opening_hours"]["hours_specification"] = entity['openingHoursSpecification']
                        elif 'openingHours' in entity:
                            nap_data["opening_hours"]["present"] = True
                            if isinstance(entity['openingHours'], list):
                                nap_data["opening_hours"]["hours_specification"] = entity['openingHours']
                            else:
                                nap_data["opening_hours"]["hours_specification"] = [entity['openingHours']]
                        
                        break
            except:
                continue
        
        # 2. Extract NAP from page text using regex patterns
        page_text = soup.get_text()
        
        # Phone number patterns (comprehensive)
        phone_patterns = [
            r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',  # (123) 456-7890
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',   # 123-456-7890
            r'\+1\s*\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # +1 123-456-7890
            r'\+44\s*\d{4}\s*\d{6}',            # UK format
            r'\d{2}\s*\d{4}\s*\d{4}'             # International format
        ]
        
        phone_variations = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, page_text)
            phone_variations.extend(matches)
        
        nap_data["phone"]["variations"] = list(set(phone_variations))
        
        # 3. Business name extraction from common HTML patterns
        business_selectors = [
            '.business-name', '.company-name', '.organization-name',
            '[class*="business"]', '[class*="company"]',
            'h1', 'h2', '.site-title', '.brand'
        ]
        
        for selector in business_selectors:
            elements = soup.select(selector)
            for elem in elements[:3]:  # Check first 3 elements
                text = elem.get_text().strip()
                if text and len(text) > 2 and len(text) < 100:
                    if not nap_data["business_name"]:
                        nap_data["business_name"] = text
                    break
        
        # 4. Address extraction from HTML patterns
        address_patterns = [
            r'\d+\s+[\w\s]+,\s*[\w\s]+,\s*[A-Z]{2}\s*\d{5}',  # US address
            r'\d+\s+[\w\s]+,\s*[\w\s]+,\s*[\w\s]+\s*\d{5}',  # International
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, page_text)
            if matches and not nap_data["address"]["full"]:
                nap_data["address"]["full"] = matches[0]
                break
        
        # 5. Calculate NAP completeness score
        completeness_factors = [
            bool(nap_data["business_name"]),
            bool(nap_data["phone"]["primary"]),
            bool(nap_data["address"]["full"]),
            nap_data["geo_coordinates"]["present"],
            nap_data["opening_hours"]["present"]
        ]
        
        nap_data["nap_consistency"]["completeness_score"] = (sum(completeness_factors) / len(completeness_factors)) * 100
        
        # 6. Check NAP consistency between schema and HTML
        if nap_data["phone"]["primary"] and phone_variations:
            normalized_primary = re.sub(r'[^\d]', '', nap_data["phone"]["primary"])
            for variation in phone_variations:
                normalized_var = re.sub(r'[^\d]', '', variation)
                if normalized_var == normalized_primary:
                    nap_data["nap_consistency"]["consistent"] = True
                    break
        
        if not nap_data["nap_consistency"]["consistent"]:
            nap_data["nap_consistency"]["variations"] = phone_variations[:5]
        
        # 7. Check LocalBusiness schema completeness
        nap_data["localbusiness_schema"]["complete"] = bool(
            nap_data["business_name"] and 
            nap_data["address"]["full"] and 
            nap_data["phone"]["primary"]
        )
        
    except Exception as e:
        print(f"[NAP_SIGNALS] Extraction error: {e}")
    
    return nap_data


def extract_entity_mentions_and_signals(soup, main_text: str) -> Dict[str, Any]:
    """Extract entity mentions and signals from content"""
    entity_signals = {
        "primary_entity_name": "",
        "entity_mentions": [],
        "entity_mention_count": 0,
        "entity_density_per_1000_words": 0,
        "named_entities_detected": {"persons": [], "organizations": [], "locations": [], "dates": []},
        "entity_context_analysis": {"first_mention_position": 0, "distribution": "uniform"}
    }
    
    try:
        # 1. Get primary entity from schema
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = _flatten_graph_entities(data)
                
                # Find primary entity (first non-WebPage entity)
                for entity in entities:
                    entity_type = entity.get('@type', '')
                    if isinstance(entity_type, list):
                        entity_type = entity_type[0] if entity_type else ""
                    
                    if entity_type not in ['WebPage'] and entity.get('name'):
                        entity_signals["primary_entity_name"] = entity['name']
                        break
                        
                if entity_signals["primary_entity_name"]:
                    break
            except:
                continue
        
        # 2. Extract entity mentions from text
        if main_text and entity_signals["primary_entity_name"]:
            primary_name = entity_signals["primary_entity_name"]
            
            # Count exact mentions
            exact_mentions = len(re.findall(re.escape(primary_name), main_text, re.IGNORECASE))
            
            # Count partial mentions (for multi-word entities)
            words_in_name = primary_name.split()
            partial_mentions = 0
            if len(words_in_name) > 1:
                for word in words_in_name:
                    if len(word) > 3:  # Only count meaningful words
                        partial_mentions += len(re.findall(r'\b' + re.escape(word) + r'\b', main_text, re.IGNORECASE))
            
            entity_signals["entity_mention_count"] = exact_mentions
            entity_signals["entity_mentions"] = [
                {
                    "entity": primary_name,
                    "mention_type": "exact",
                    "count": exact_mentions,
                    "positions": [m.start() for m in re.finditer(re.escape(primary_name), main_text, re.IGNORECASE)]
                }
            ]
            
            if partial_mentions > 0:
                entity_signals["entity_mentions"].append({
                    "entity": primary_name,
                    "mention_type": "partial",
                    "count": partial_mentions,
                    "words": words_in_name
                })
            
            # Calculate entity density
            total_words = len(re.findall(r'\b\w+\b', main_text))
            if total_words > 0:
                entity_signals["entity_density_per_1000_words"] = round((exact_mentions / total_words) * 1000, 2)
            
            # Find first mention position
            first_match = re.search(re.escape(primary_name), main_text, re.IGNORECASE)
            if first_match:
                entity_signals["entity_context_analysis"]["first_mention_position"] = first_match.start()
        
        # 3. Basic named entity detection using patterns
        # Person names (simple pattern)
        person_pattern = r'\b([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
        person_matches = re.findall(person_pattern, main_text)
        entity_signals["named_entities_detected"]["persons"] = list(set(person_matches))[:10]
        
        # Organization names (capitalized multi-word)
        org_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b'
        org_matches = re.findall(org_pattern, main_text)
        # Filter out common non-organization words
        org_matches = [org for org in org_matches if len(org.split()) >= 2 and org not in entity_signals["named_entities_detected"]["persons"]]
        entity_signals["named_entities_detected"]["organizations"] = list(set(org_matches))[:10]
        
        # Location names (simple city/state pattern)
        location_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})\b'
        location_matches = re.findall(location_pattern, main_text)
        entity_signals["named_entities_detected"]["locations"] = list(set(location_matches))[:10]
        
        # Date patterns
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{4}-\d{2}-\d{2}\b',      # YYYY-MM-DD
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b'  # Month DD, YYYY
        ]
        
        date_matches = []
        for pattern in date_patterns:
            date_matches.extend(re.findall(pattern, main_text))
        
        entity_signals["named_entities_detected"]["dates"] = list(set(date_matches))[:10]
        
    except Exception as e:
        print(f"[ENTITY_SIGNALS] Extraction error: {e}")
    
    return entity_signals


def extract_aeo_answer_signals(soup, main_text: str) -> Dict[str, Any]:
    """Extract Answer Engine Optimization signals"""
    aeo_signals = {
        "direct_answer_in_first_paragraph": False,
        "first_paragraph_word_count": 0,
        "first_paragraph_answer_length": 0,
        "question_headings": [],
        "faq_section_detected": False,
        "faq_questions_count": 0,
        "answer_snippet_length": 0,
        "featured_snippet_ready": False,
        "snippet_format": "",
        "concise_answers": [],
        "definition_patterns": [],
        "how_to_steps": 0,
        "list_content": {"ordered": 0, "unordered": 0},
        "table_content": {"present": False, "rows": 0, "headers": 0}
    }
    
    try:
        # 1. Analyze first paragraph for direct answers
        paragraphs = main_text.split('\n\n')
        if paragraphs:
            first_para = paragraphs[0].strip()
            aeo_signals["first_paragraph_word_count"] = len(first_para.split())
            
            # Check if first paragraph contains a direct answer (40-60 words)
            word_count = len(first_para.split())
            if 40 <= word_count <= 60:
                # Check for answer indicators
                answer_indicators = [
                    'is defined as', 'refers to', 'means that', 'is a', 'are defined as',
                    'can be described as', 'is characterized by', 'involves'
                ]
                
                if any(indicator in first_para.lower() for indicator in answer_indicators):
                    aeo_signals["direct_answer_in_first_paragraph"] = True
                    aeo_signals["first_paragraph_answer_length"] = word_count
                    aeo_signals["answer_snippet_length"] = word_count
        
        # 2. Extract question headings
        question_headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            heading_text = heading.get_text().strip()
            # Check if heading is a question
            if (heading_text.endswith('?') or 
                any(q_word in heading_text.lower() for q_word in ['what', 'how', 'why', 'when', 'where', 'which', 'who'])):
                question_headings.append({
                    "text": heading_text,
                    "level": heading.name,
                    "word_count": len(heading_text.split())
                })
        
        aeo_signals["question_headings"] = question_headings
        
        # 3. FAQ detection
        faq_indicators = ['faq', 'frequently asked', 'questions', 'questions and answers']
        faq_detected = False
        
        # Check for FAQ in headings
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            if any(indicator in heading.get_text().lower() for indicator in faq_indicators):
                faq_detected = True
                break
        
        # Check for FAQ schema
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = _flatten_graph_entities(data)
                for entity in entities:
                    if entity.get('@type') == 'FAQPage':
                        faq_detected = True
                        if 'mainEntity' in entity:
                            faq_questions = entity['mainEntity']
                            if isinstance(faq_questions, list):
                                aeo_signals["faq_questions_count"] = len(faq_questions)
                            else:
                                aeo_signals["faq_questions_count"] = 1
                        break
            except:
                continue
        
        aeo_signals["faq_section_detected"] = faq_detected
        
        # 4. Definition pattern detection
        definition_patterns = [
            r'(\w+)\s+is\s+(a|an)\s+([^,.!?]+)',      # X is a Y
            r'(\w+)\s+refers\s+to\s+([^,.!?]+)',      # X refers to Y
            r'(\w+)\s+can\s+be\s+defined\s+as\s+([^,.!?]+)',  # X can be defined as Y
            r'(\w+)\s+means\s+([^,.!?]+)',            # X means Y
            r'The\s+term\s+(\w+)\s+refers\s+to\s+([^,.!?]+)'  # The term X refers to Y
        ]
        
        definitions = []
        for pattern in definition_patterns:
            matches = re.findall(pattern, main_text, re.IGNORECASE)
            for match in matches:
                if len(match) >= 2:
                    definitions.append({
                        "term": match[0],
                        "definition": match[1] if len(match) == 2 else match[2],
                        "pattern": pattern
                    })
        
        aeo_signals["definition_patterns"] = definitions[:5]
        
        # 5. How-to steps detection
        how_to_patterns = [
            r'step\s+\d+[:.]?\s*([^.\n]+)',     # Step 1: description
            r'\d+\.\s*([^.\n]+)',               # 1. description
            r'first[:,]?\s*([^.\n]+)',          # First: description
            r'second[:,]?\s*([^.\n]+)',         # Second: description
            r'next[:,]?\s*([^.\n]+)'            # Next: description
        ]
        
        steps = []
        for pattern in how_to_patterns:
            matches = re.findall(pattern, main_text, re.IGNORECASE)
            steps.extend(matches)
        
        aeo_signals["how_to_steps"] = len(steps)
        
        # 6. List content analysis
        ordered_lists = soup.find_all('ol')
        unordered_lists = soup.find_all('ul')
        
        aeo_signals["list_content"]["ordered"] = len(ordered_lists)
        aeo_signals["list_content"]["unordered"] = len(unordered_lists)
        
        # 7. Table content analysis
        tables = soup.find_all('table')
        if tables:
            aeo_signals["table_content"]["present"] = True
            total_rows = sum(len(table.find_all('tr')) for table in tables)
            total_headers = sum(len(table.find_all('th')) for table in tables)
            aeo_signals["table_content"]["rows"] = total_rows
            aeo_signals["table_content"]["headers"] = total_headers
        
        # 8. Featured snippet readiness assessment
        snippet_ready = False
        snippet_format = ""
        
        if aeo_signals["direct_answer_in_first_paragraph"]:
            snippet_ready = True
            snippet_format = "paragraph"
        elif aeo_signals["how_to_steps"] > 0:
            snippet_ready = True
            snippet_format = "steps"
        elif aeo_signals["list_content"]["ordered"] > 0:
            snippet_ready = True
            snippet_format = "ordered_list"
        elif aeo_signals["table_content"]["present"]:
            snippet_ready = True
            snippet_format = "table"
        elif aeo_signals["definition_patterns"]:
            snippet_ready = True
            snippet_format = "definition"
        
        aeo_signals["featured_snippet_ready"] = snippet_ready
        aeo_signals["snippet_format"] = snippet_format
        
        # 9. Concise answers detection (under 60 words)
        sentences = re.split(r'[.!?]+', main_text)
        concise_answers = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            word_count = len(sentence.split())
            if 20 <= word_count <= 60:  # Concise but meaningful
                # Check if it answers a question
                if any(indicator in sentence.lower() for indicator in ['is', 'are', 'can', 'will', 'should', 'does']):
                    concise_answers.append({
                        "text": sentence,
                        "word_count": word_count,
                        "position": len(concise_answers)
                    })
        
        aeo_signals["concise_answers"] = concise_answers[:3]  # Top 3 concise answers
        
    except Exception as e:
        print(f"[AEO_SIGNALS] Extraction error: {e}")
    
    return aeo_signals


def extract_enhanced_technical_signals(soup, url: str) -> Dict[str, Any]:
    """Extract enhanced technical signals for AI visibility"""
    technical_signals = {
        "robots_meta": {"directive": "", "indexable": True, "followable": True},
        "viewport_meta": {"present": False, "content": ""},
        "hreflang_tags": {"present": False, "languages": [], "return_tags": [], "self_referencing": False},
        "breadcrumb_schema": {"present": False, "levels": 0, "consistent": False},
        "canonical_tag": {"present": False, "self_referencing": False, "url": ""},
        "structured_data_validation": {"valid_json": True, "parse_errors": 0, "warnings": 0},
        "page_language": {"declared": "", "detected": "", "consistent": True},
        "content_type": {"declared": "", "charset": ""},
        "meta_tags_completeness": {"title": False, "description": False, "h1": False}
    }
    
    try:
        # 1. Robots meta analysis
        robots_meta = soup.find('meta', attrs={'name': 'robots'})
        if robots_meta:
            robots_content = robots_meta.get('content', '').lower()
            technical_signals["robots_meta"]["directive"] = robots_content
            technical_signals["robots_meta"]["indexable"] = 'noindex' not in robots_content
            technical_signals["robots_meta"]["followable"] = 'nofollow' not in robots_content
        
        # 2. Viewport meta
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        if viewport:
            technical_signals["viewport_meta"]["present"] = True
            technical_signals["viewport_meta"]["content"] = viewport.get('content', '')
        
        # 3. hreflang analysis
        hreflang_links = soup.find_all('link', rel='alternate', hreflang=True)
        if hreflang_links:
            technical_signals["hreflang_tags"]["present"] = True
            languages = []
            return_tags = []
            
            for link in hreflang_links:
                hreflang = link.get('hreflang', '')
                href = link.get('href', '')
                
                if hreflang:
                    languages.append(hreflang)
                    
                    # Check for return tags (x-default)
                    if hreflang == 'x-default':
                        return_tags.append(href)
                    
                    # Check self-referencing hreflang
                    if href == url:
                        technical_signals["hreflang_tags"]["self_referencing"] = True
            
            technical_signals["hreflang_tags"]["languages"] = list(set(languages))
            technical_signals["hreflang_tags"]["return_tags"] = return_tags
        
        # 4. Breadcrumb schema detection
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        breadcrumb_found = False
        breadcrumb_levels = 0
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = _flatten_graph_entities(data)
                
                for entity in entities:
                    if entity.get('@type') == 'BreadcrumbList':
                        breadcrumb_found = True
                        if 'itemListElement' in entity:
                            breadcrumb_levels = len(entity['itemListElement'])
                        break
            except:
                continue
        
        technical_signals["breadcrumb_schema"]["present"] = breadcrumb_found
        technical_signals["breadcrumb_schema"]["levels"] = breadcrumb_levels
        
        # 5. Canonical tag analysis
        canonical = soup.find('link', rel='canonical')
        if canonical:
            canonical_url = canonical.get('href', '')
            technical_signals["canonical_tag"]["present"] = True
            technical_signals["canonical_tag"]["url"] = canonical_url
            technical_signals["canonical_tag"]["self_referencing"] = canonical_url == url
        
        # 6. Structured data validation
        parse_errors = 0
        for script in json_ld_scripts:
            try:
                json.loads(script.string)
            except json.JSONDecodeError:
                parse_errors += 1
        
        technical_signals["structured_data_validation"]["parse_errors"] = parse_errors
        technical_signals["structured_data_validation"]["valid_json"] = parse_errors == 0
        
        # 7. Page language detection
        html_tag = soup.find('html')
        if html_tag:
            lang_attr = html_tag.get('lang', '')
            technical_signals["page_language"]["declared"] = lang_attr
        
        # Meta language
        lang_meta = soup.find('meta', attrs={'http-equiv': 'content-language'})
        if lang_meta:
            content_lang = lang_meta.get('content', '')
            if not technical_signals["page_language"]["declared"]:
                technical_signals["page_language"]["declared"] = content_lang
            elif content_lang != technical_signals["page_language"]["declared"]:
                technical_signals["page_language"]["consistent"] = False
        
        # 8. Content type and charset
        content_type_meta = soup.find('meta', attrs={'http-equiv': 'content-type'})
        if content_type_meta:
            content_type = content_type_meta.get('content', '')
            technical_signals["content_type"]["declared"] = content_type
            
            if 'charset=' in content_type:
                charset = content_type.split('charset=')[1].split(';')[0].strip()
                technical_signals["content_type"]["charset"] = charset
        
        # 9. Meta tags completeness
        title_tag = soup.find('title')
        if title_tag and title_tag.get_text().strip():
            technical_signals["meta_tags_completeness"]["title"] = True
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content', '').strip():
            technical_signals["meta_tags_completeness"]["description"] = True
        
        h1_tag = soup.find('h1')
        if h1_tag and h1_tag.get_text().strip():
            technical_signals["meta_tags_completeness"]["h1"] = True
        
    except Exception as e:
        print(f"[TECHNICAL_SIGNALS] Extraction error: {e}")
    
    return technical_signals


def _flatten_graph_entities(data):
    """Helper function to flatten @graph entities"""
    entities = []
    
    if isinstance(data, dict):
        if '@graph' in data:
            graph_items = data['@graph']
            if isinstance(graph_items, list):
                entities.extend(graph_items)
            else:
                entities.append(graph_items)
        else:
            entities.append(data)
    elif isinstance(data, list):
        for item in data:
            entities.extend(_flatten_graph_entities(item))
    
    return entities


# Performance optimization utilities
def safe_extract_text(element, max_length: int = 500) -> str:
    """Safely extract text with length limits"""
    if not element:
        return ""
    
    try:
        text = element.get_text(strip=True)
        return text[:max_length] if text else ""
    except Exception:
        return ""

def safe_extract_attr(element, attr: str, default: str = "") -> str:
    """Safely extract attribute with fallback"""
    if not element:
        return default
    
    try:
        return element.get(attr, default) or default
    except Exception:
        return default
