"""
Enhanced SEO Data Extraction - Active Signal Extractors

Active signals (with verified consumers in the rule engine):
  - author_signals          (eeat_rules.py AuthorNameBioRule)
  - last_updated_signals    (eeat_rules.py DatePublishedRule)
  - schema_format_signals   (internal_rules.py SchemaMarkupRule)
  - faq_howto_signals       (internal_rules.py FaqSchemaRule)
  - breadcrumb_dom_signals  (schema_rules.py BreadcrumbListSchemaRule)
  - breadcrumb_schema_signals (schema_rules.py BreadcrumbListSchemaRule)

json_ld_entries parameter (optional): pre-parsed JSON-LD list from orchestrator.
Each entry is a tuple of (raw_content: str, schemas: list[dict], is_valid_json: bool).
When provided, functions skip their own soup.find_all() + json.loads() loops entirely.
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple
from bs4 import BeautifulSoup, Tag
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# 6) Author Info Signals
# ---------------------------------------------------------------------------

def extract_author_signals(soup: BeautifulSoup, json_ld_entries=None) -> Dict[str, Any]:
    """
    Extract author information signals.

    DOM Selectors Used:
    - Elements containing author-related text
    - Link elements with author-related hrefs
    - Schema.org author data (via json_ld_entries when provided)

    Returns:
        Dict with author information data
    """
    try:
        author_patterns = [
            r'author[s]?', r'by\s+\w+', r'written\s+by', r'posted\s+by',
            r'published\s+by', r'contributor', r'writer', r'journalist'
        ]

        author_elements = []
        author_links = []
        author_schema_data = []

        # Text-based author detection
        for element in soup.find_all(['p', 'div', 'span', 'footer', 'header']):
            text = element.get_text(strip=True).lower()
            element_text = element.get_text(strip=True)

            for pattern in author_patterns:
                if re.search(pattern, text):
                    author_elements.append({
                        'tag': element.name,
                        'text_preview': element_text[:100],
                        'matched_pattern': pattern,
                        'has_link': bool(element.find('a'))
                    })
                    break

        # Link-based author detection
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text(strip=True).lower()

            author_indicators = ['author', 'profile', 'user', 'writer', 'contributor']
            if (any(indicator in href for indicator in author_indicators) or
                    any(indicator in text for indicator in author_indicators)):
                author_links.append({
                    'href': link.get('href'),
                    'text': link.get_text(strip=True),
                    'title': link.get('title', '')
                })

        # Schema.org author data — use pre-parsed entries when available
        if json_ld_entries is not None:
            for _content, schemas, is_valid in json_ld_entries:
                if not is_valid:
                    continue
                for schema in schemas:
                    if isinstance(schema, dict) and 'author' in schema:
                        author_schema_data.append({
                            'schema_type': schema.get('@type'),
                            'author_data': schema['author']
                        })
        else:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    schema_data = json.loads(script.string)
                    schemas = [schema_data] if isinstance(schema_data, dict) else schema_data
                    for schema in schemas:
                        if isinstance(schema, dict) and 'author' in schema:
                            author_schema_data.append({
                                'schema_type': schema.get('@type'),
                                'author_data': schema['author']
                            })
                except (json.JSONDecodeError, TypeError):
                    continue

        # Author bio section detection
        bio_sections = []
        bio_keywords = ['about the author', 'author bio', 'about', 'biography', 'profile']

        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            heading_text = heading.get_text(strip=True).lower()
            if any(keyword in heading_text for keyword in bio_keywords):
                next_elements = heading.find_next_siblings(['p', 'div'], limit=3)
                bio_content = ' '.join([elem.get_text(strip=True) for elem in next_elements])

                bio_sections.append({
                    'heading': heading.get_text(strip=True),
                    'content_preview': bio_content[:200],
                    'content_length': len(bio_content)
                })

        return {
            'author_text_elements': author_elements,
            'author_links': author_links,
            'author_schema_data': author_schema_data,
            'author_bio_sections': bio_sections,
            'author_element_count': len(author_elements),
            'author_link_count': len(author_links),
            'author_schema_count': len(author_schema_data),
            'bio_section_count': len(bio_sections)
        }

    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 7) Last Updated Signals
# ---------------------------------------------------------------------------

def extract_last_updated_signals(soup: BeautifulSoup, html: str, json_ld_entries=None) -> Dict[str, Any]:
    """
    Extract last updated and freshness signals.

    DOM Selectors Used:
    - time elements
    - datetime attributes
    - Schema.org date fields (via json_ld_entries when provided)
    - Text patterns for update mentions

    Returns:
        Dict with last updated data
    """
    try:
        # HTML5 time elements
        time_elements = []
        for time_elem in soup.find_all('time'):
            time_elements.append({
                'datetime': time_elem.get('datetime'),
                'text': time_elem.get_text(strip=True),
                'has_datetime': bool(time_elem.get('datetime'))
            })

        # Schema.org date detection — use pre-parsed entries when available
        schema_dates = []
        date_fields_to_check = ['datePublished', 'dateModified', 'uploadDate', 'dateCreated']

        if json_ld_entries is not None:
            for _content, schemas, is_valid in json_ld_entries:
                if not is_valid:
                    continue
                for schema in schemas:
                    if isinstance(schema, dict):
                        for field in date_fields_to_check:
                            if field in schema:
                                schema_dates.append({
                                    'schema_type': schema.get('@type'),
                                    'date_field': field,
                                    'date_value': schema[field]
                                })
        else:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    schema_data = json.loads(script.string)
                    schemas = [schema_data] if isinstance(schema_data, dict) else schema_data
                    for schema in schemas:
                        if isinstance(schema, dict):
                            for field in date_fields_to_check:
                                if field in schema:
                                    schema_dates.append({
                                        'schema_type': schema.get('@type'),
                                        'date_field': field,
                                        'date_value': schema[field]
                                    })
                except (json.JSONDecodeError, TypeError):
                    continue

        # Text-based update detection
        update_patterns = [
            r'updated?\s+(?:on|:)?\s*([a-zA-Z]+\s+\d{1,2},?\s+\d{4})',
            r'last\s+(?:updated|modified|revised)\s+(?:on|:)?\s*([a-zA-Z]+\s+\d{1,2},?\s+\d{4})',
            r'recently\s+updated',
            r'fresh\s+content',
            r'newly\s+added'
        ]

        update_mentions = []
        all_text = soup.get_text(separator=' ', strip=True)

        for pattern in update_patterns:
            matches = re.finditer(pattern, all_text, re.IGNORECASE)
            for match in matches:
                update_mentions.append({
                    'pattern': pattern,
                    'matched_text': match.group(0)[:100],
                    'full_match': match.group(0),
                    'has_date': len(match.groups()) > 0
                })

        # Meta tags with dates
        date_meta_tags = []
        date_meta_names = ['article:published_time', 'article:modified_time', 'date', 'last-modified', 'updated']

        for meta in soup.find_all('meta'):
            name = meta.get('name', '').lower() or meta.get('property', '').lower()
            content = meta.get('content', '')

            if any(date_name in name for date_name in date_meta_names):
                date_meta_tags.append({
                    'name': meta.get('name') or meta.get('property'),
                    'content': content,
                    'name_type': 'name' if meta.get('name') else 'property'
                })

        return {
            'time_elements': time_elements,
            'schema_dates': schema_dates,
            'update_mentions': update_mentions,
            'date_meta_tags': date_meta_tags,
            'time_element_count': len(time_elements),
            'schema_date_count': len(schema_dates),
            'update_mention_count': len(update_mentions),
            'date_meta_count': len(date_meta_tags)
        }

    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 8) Schema Format Signals
# ---------------------------------------------------------------------------

def extract_schema_format_signals(soup: BeautifulSoup, json_ld_entries=None) -> Dict[str, Any]:
    """
    Extract structured data format signals with robust JSON-LD detection.

    DOM Selectors Used:
    - script[type="application/ld+json"] (via json_ld_entries when provided)
    - elements with itemtype attribute (Microdata)
    - elements with vocab/typeof attributes (RDFa)

    Returns:
        Dict with schema format data
    """
    try:
        json_ld_scripts = []
        json_ld_types = []

        if json_ld_entries is not None:
            for content, schemas, is_valid in json_ld_entries:
                if not content:
                    continue
                if is_valid:
                    script_types = []
                    for schema in schemas:
                        if isinstance(schema, dict):
                            schema_type = schema.get('@type')
                            if schema_type:
                                if isinstance(schema_type, list):
                                    script_types.extend(schema_type)
                                else:
                                    script_types.append(schema_type)
                    json_ld_types.extend(script_types)
                    json_ld_scripts.append({
                        'content_length': len(content),
                        'schema_count': len(schemas),
                        'schema_types': script_types,
                        'is_valid_json': True,
                        'has_context': any('@context' in s for s in schemas if isinstance(s, dict)),
                        'raw_content': content[:500]
                    })
                else:
                    json_ld_scripts.append({
                        'content_length': len(content),
                        'is_valid_json': False,
                        'raw_content': content[:200]
                    })
        else:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    content = script.string.strip() if script.string else ''
                    if not content:
                        continue

                    cleaned_content = content
                    cleaned_content = cleaned_content.replace('&quot;', '"').replace('&apos;', "'").replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
                    cleaned_content = re.sub(r',(\s*[}\]])', r'\1', cleaned_content)

                    try:
                        parsed = json.loads(cleaned_content)

                        schemas = []
                        if isinstance(parsed, dict):
                            if '@graph' in parsed:
                                schemas = parsed['@graph'] if isinstance(parsed['@graph'], list) else [parsed['@graph']]
                            else:
                                schemas = [parsed]
                        elif isinstance(parsed, list):
                            schemas = parsed

                        script_types = []
                        for schema in schemas:
                            if isinstance(schema, dict):
                                schema_type = schema.get('@type')
                                if schema_type:
                                    if isinstance(schema_type, list):
                                        script_types.extend(schema_type)
                                    else:
                                        script_types.append(schema_type)

                        json_ld_types.extend(script_types)
                        json_ld_scripts.append({
                            'content_length': len(content),
                            'schema_count': len(schemas),
                            'schema_types': script_types,
                            'is_valid_json': True,
                            'has_context': any('@context' in s for s in schemas if isinstance(s, dict)),
                            'raw_content': content[:500]
                        })

                    except json.JSONDecodeError as json_err:
                        json_ld_scripts.append({
                            'content_length': len(content),
                            'is_valid_json': False,
                            'parse_error': str(json_err),
                            'raw_content': content[:200]
                        })
                        print(f"⚠️ JSON-LD parse error: {json_err}")

                except Exception as e:
                    print(f"⚠️ JSON-LD script processing error: {e}")
                    continue

        # Microdata detection
        microdata_elements = []
        for element in soup.find_all(attrs={'itemtype': True}):
            microdata_elements.append({
                'tag': element.name,
                'itemtype': element.get('itemtype'),
                'itemscope': element.has_attr('itemscope'),
                'itemprop_count': len(element.find_all(attrs={'itemprop': True})),
                'text_preview': element.get_text(strip=True)[:100]
            })

        # RDFa detection
        rdfa_elements = []
        for element in soup.find_all(attrs={'vocab': True}):
            rdfa_elements.append({
                'tag': element.name,
                'vocab': element.get('vocab'),
                'typeof': element.get('typeof'),
                'property_count': len(element.find_all(attrs={'property': True})),
                'text_preview': element.get_text(strip=True)[:100]
            })

        for element in soup.find_all(attrs={'typeof': True}):
            if not element.get('vocab'):
                rdfa_elements.append({
                    'tag': element.name,
                    'typeof': element.get('typeof'),
                    'vocab': None,
                    'property_count': len(element.find_all(attrs={'property': True})),
                    'text_preview': element.get_text(strip=True)[:100]
                })

        total_detected = len(json_ld_scripts) + len(microdata_elements) + len(rdfa_elements)

        return {
            'json_ld_present': len(json_ld_scripts) > 0,
            'json_ld_scripts': json_ld_scripts,
            'json_ld_count': len(json_ld_scripts),
            'json_ld_types': list(set(json_ld_types)),
            'microdata_present': len(microdata_elements) > 0,
            'microdata_elements': microdata_elements,
            'microdata_count': len(microdata_elements),
            'rdfa_present': len(rdfa_elements) > 0,
            'rdfa_elements': rdfa_elements,
            'rdfa_count': len(rdfa_elements),
            'total_schema_elements': total_detected
        }

    except Exception as e:
        print(f"❌ Schema format extraction failed: {e}")
        return {
            'json_ld_present': False,
            'json_ld_count': 0,
            'json_ld_types': [],
            'microdata_present': False,
            'microdata_count': 0,
            'rdfa_present': False,
            'rdfa_count': 0,
            'total_schema_elements': 0,
            'extraction_error': str(e)
        }


# ---------------------------------------------------------------------------
# 9) FAQ / HowTo Signals
# ---------------------------------------------------------------------------

def extract_faq_howto_signals(soup: BeautifulSoup, json_ld_entries=None) -> Dict[str, Any]:
    """
    Extract FAQ and HowTo structured signals.

    DOM Selectors Used:
    - Schema.org FAQPage/HowTo detection (via json_ld_entries when provided)
    - DOM pattern analysis for Q&A structures
    - Step-by-step content detection

    Returns:
        Dict with FAQ/HowTo data
    """
    try:
        faq_schema = []
        howto_schema = []

        if json_ld_entries is not None:
            for _content, schemas, is_valid in json_ld_entries:
                if not is_valid:
                    continue
                for schema in schemas:
                    if isinstance(schema, dict):
                        schema_type = schema.get('@type', '')
                        if schema_type == 'FAQPage':
                            faq_schema.append(schema)
                        elif schema_type == 'HowTo':
                            howto_schema.append(schema)
        else:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    schema_data = json.loads(script.string)
                    schemas = [schema_data] if isinstance(schema_data, dict) else schema_data
                    for schema in schemas:
                        if isinstance(schema, dict):
                            schema_type = schema.get('@type', '')
                            if schema_type == 'FAQPage':
                                faq_schema.append(schema)
                            elif schema_type == 'HowTo':
                                howto_schema.append(schema)
                except (json.JSONDecodeError, TypeError):
                    continue

        # DOM pattern analysis for Q&A structures
        qa_patterns = []

        for heading in soup.find_all(['h2', 'h3', 'h4']):
            heading_text = heading.get_text(strip=True).lower()
            is_question = heading_text.endswith('?')

            if is_question:
                answer_elements = []
                next_elem = heading.find_next_sibling()

                while next_elem and next_elem.name in ['p', 'div', 'ul', 'ol'] and len(answer_elements) < 3:
                    if next_elem.name in ['p', 'div']:
                        text = next_elem.get_text(strip=True)
                        if len(text) > 20:
                            answer_elements.append({
                                'type': next_elem.name,
                                'text_preview': text[:150],
                                'text_length': len(text)
                            })
                    elif next_elem.name in ['ul', 'ol']:
                        items = next_elem.find_all('li', limit=5)
                        answer_elements.append({
                            'type': 'list',
                            'item_count': len(items),
                            'items_preview': [li.get_text(strip=True)[:50] for li in items[:2]]
                        })
                    next_elem = next_elem.find_next_sibling()

                if answer_elements:
                    qa_patterns.append({
                        'question': heading.get_text(strip=True),
                        'question_level': heading.name,
                        'answer_elements': answer_elements,
                        'has_answer': True
                    })

        # Step-by-step content detection
        step_patterns = []
        step_indicators = [
            r'step\s+\d+', r'\d+\.\s', r'first\s*,?\s*second', r'next\s*,?\s*then',
            r'finally', r'lastly', r'after\s+that', r'the\s+next\s+step'
        ]

        for element in soup.find_all(['p', 'h2', 'h3', 'li']):
            text = element.get_text(strip=True).lower()
            for pattern in step_indicators:
                if re.search(pattern, text):
                    step_patterns.append({
                        'tag': element.name,
                        'text': element.get_text(strip=True),
                        'matched_pattern': pattern,
                        'is_heading': element.name in ['h2', 'h3', 'h4']
                    })
                    break

        # FAQ section detection
        faq_sections = []
        faq_keywords = ['faq', 'frequently asked questions', 'questions', 'common questions']

        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            heading_text = heading.get_text(strip=True).lower()

            if any(keyword in heading_text for keyword in faq_keywords):
                faq_items = []
                current_element = heading.find_next_sibling()

                while current_element and len(faq_items) < 10:
                    if current_element.name in ['h2', 'h3', 'h4']:
                        question_text = current_element.get_text(strip=True)
                        if len(question_text) > 10:
                            faq_items.append({'type': 'question', 'text': question_text})
                    elif current_element.name in ['p', 'div']:
                        text = current_element.get_text(strip=True)
                        if len(text) > 20:
                            faq_items.append({'type': 'answer', 'text_preview': text[:100]})
                    current_element = current_element.find_next_sibling()

                if faq_items:
                    faq_sections.append({
                        'section_heading': heading.get_text(strip=True),
                        'faq_items': faq_items,
                        'item_count': len(faq_items)
                    })

        return {
            'faq_schema_present': len(faq_schema) > 0,
            'faq_schema_count': len(faq_schema),
            'faq_schema': faq_schema,
            'howto_schema_present': len(howto_schema) > 0,
            'howto_schema_count': len(howto_schema),
            'howto_schema': howto_schema,
            'qa_patterns': qa_patterns,
            'qa_pattern_count': len(qa_patterns),
            'step_patterns': step_patterns,
            'step_pattern_count': len(step_patterns),
            'faq_sections': faq_sections,
            'faq_section_count': len(faq_sections)
        }

    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 10) Breadcrumb DOM Extraction
# ---------------------------------------------------------------------------

def extract_breadcrumb_dom_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract breadcrumb navigation from DOM structure.

    DOM Selectors Used:
    - nav[aria-label="breadcrumb"]
    - [class*="breadcrumb"], [class*="breadcrumbs"], [class*="bread-crumb"]
    - ol/ul structures with multiple linked items in top 30% DOM

    Returns:
        Dict with breadcrumb DOM data
    """
    try:
        all_elements = list(soup.find_all())
        total_elements = len(all_elements)
        top_30_threshold = int(total_elements * 0.3) if total_elements > 0 else 0

        breadcrumb_data = {
            "breadcrumb_detected": False,
            "breadcrumb_text_path": [],
            "breadcrumb_links": [],
            "breadcrumb_dom_position": None,
            "breadcrumb_selector_used": None,
            "breadcrumb_html_preview": ""
        }

        # Strategy 1: nav[aria-label="breadcrumb"] (highest confidence)
        breadcrumb_nav = soup.find('nav', attrs={'aria-label': re.compile(r'breadcrumb', re.IGNORECASE)})
        if breadcrumb_nav:
            breadcrumb_links = breadcrumb_nav.find_all('a', href=True)
            if len(breadcrumb_links) >= 2:
                breadcrumb_data.update({
                    "breadcrumb_detected": True,
                    "breadcrumb_text_path": [link.get_text(strip=True) for link in breadcrumb_links],
                    "breadcrumb_links": [link.get('href') for link in breadcrumb_links],
                    "breadcrumb_dom_position": all_elements.index(breadcrumb_nav) if breadcrumb_nav in all_elements else -1,
                    "breadcrumb_selector_used": "nav[aria-label='breadcrumb']",
                    "breadcrumb_html_preview": str(breadcrumb_nav)[:200]
                })
                return breadcrumb_data

        # Strategy 2: Class-based detection
        breadcrumb_classes = ['breadcrumb', 'breadcrumbs', 'bread-crumb']
        for class_name in breadcrumb_classes:
            elements = soup.find_all(attrs={'class': re.compile(class_name, re.IGNORECASE)})

            for element in elements:
                if element.find_parent(['header', 'footer']):
                    continue

                element_text = element.get_text().lower()
                menu_indicators = ['menu', 'navigation', 'nav', 'main']
                if any(indicator in element_text for indicator in menu_indicators):
                    continue

                breadcrumb_links = element.find_all('a', href=True)
                if len(breadcrumb_links) >= 2:
                    dom_position = all_elements.index(element) if element in all_elements else -1

                    if dom_position <= top_30_threshold:
                        breadcrumb_data.update({
                            "breadcrumb_detected": True,
                            "breadcrumb_text_path": [link.get_text(strip=True) for link in breadcrumb_links],
                            "breadcrumb_links": [link.get('href') for link in breadcrumb_links],
                            "breadcrumb_dom_position": dom_position,
                            "breadcrumb_selector_used": f"[class*='{class_name}']",
                            "breadcrumb_html_preview": str(element)[:200]
                        })
                        return breadcrumb_data

        # Strategy 3: Structural detection (ol/ul with links in top 30%)
        for list_elem in soup.find_all(['ol', 'ul']):
            if list_elem.find_parent(['header', 'footer']):
                continue

            linked_items = list_elem.find_all('li')
            linked_items_with_links = [li for li in linked_items if li.find('a', href=True)]

            if len(linked_items_with_links) >= 2:
                dom_position = all_elements.index(list_elem) if list_elem in all_elements else -1

                if dom_position <= top_30_threshold:
                    links = [li.find('a', href=True) for li in linked_items_with_links]
                    hrefs = [link.get('href') for link in links if link]

                    if len(set(hrefs)) < 2:
                        continue

                    breadcrumb_data.update({
                        "breadcrumb_detected": True,
                        "breadcrumb_text_path": [link.get_text(strip=True) for link in links],
                        "breadcrumb_links": hrefs,
                        "breadcrumb_dom_position": dom_position,
                        "breadcrumb_selector_used": "ol/ul structural detection",
                        "breadcrumb_html_preview": str(list_elem)[:200]
                    })
                    return breadcrumb_data

        return breadcrumb_data

    except Exception as e:
        return {
            "breadcrumb_detected": False,
            "extraction_error": str(e),
            "breadcrumb_text_path": [],
            "breadcrumb_links": [],
            "breadcrumb_dom_position": None,
            "breadcrumb_selector_used": None
        }


# ---------------------------------------------------------------------------
# 11) Breadcrumb Schema Extraction
# ---------------------------------------------------------------------------

def extract_breadcrumb_schema_signals(soup: BeautifulSoup, json_ld_entries=None) -> Dict[str, Any]:
    """
    Extract BreadcrumbList structured data from all schema formats.

    DOM Selectors Used:
    - script[type="application/ld+json"] (via json_ld_entries when provided)
    - [itemtype="https://schema.org/BreadcrumbList"] (Microdata)
    - [typeof="BreadcrumbList"] (RDFa)

    Returns:
        Dict with breadcrumb schema data
    """
    try:
        schema_data = {
            "breadcrumb_schema_present": False,
            "breadcrumb_schema_format": None,
            "breadcrumb_item_count": 0,
            "breadcrumb_schema_raw": None,
            "breadcrumb_items": []
        }

        # Strategy 1: JSON-LD BreadcrumbList detection
        if json_ld_entries is not None:
            for content, schemas, is_valid in json_ld_entries:
                if not is_valid:
                    continue
                for schema in schemas:
                    if not isinstance(schema, dict):
                        continue
                    schema_type = schema.get('@type')
                    if isinstance(schema_type, str):
                        types = [schema_type]
                    elif isinstance(schema_type, list):
                        types = schema_type
                    else:
                        continue

                    if 'BreadcrumbList' in types:
                        items = schema.get('itemListElement', [])
                        breadcrumb_items = []
                        for item in items:
                            if isinstance(item, dict):
                                breadcrumb_items.append({
                                    'name': item.get('name'),
                                    'url': item.get('item', {}).get('@id') if isinstance(item.get('item'), dict) else item.get('item'),
                                    'position': item.get('position')
                                })
                        schema_data.update({
                            "breadcrumb_schema_present": True,
                            "breadcrumb_schema_format": "json-ld",
                            "breadcrumb_item_count": len(items),
                            "breadcrumb_schema_raw": content[:500],
                            "breadcrumb_items": breadcrumb_items
                        })
                        return schema_data
        else:
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_ld_scripts:
                try:
                    if not script.string:
                        continue
                    parsed_schema = json.loads(script.string.strip())
                    schemas = [parsed_schema] if isinstance(parsed_schema, dict) else parsed_schema

                    for schema in schemas:
                        if not isinstance(schema, dict):
                            continue
                        schema_type = schema.get('@type')
                        if isinstance(schema_type, str):
                            types = [schema_type]
                        elif isinstance(schema_type, list):
                            types = schema_type
                        else:
                            continue

                        if 'BreadcrumbList' in types:
                            items = schema.get('itemListElement', [])
                            breadcrumb_items = []
                            for item in items:
                                if isinstance(item, dict):
                                    breadcrumb_items.append({
                                        'name': item.get('name'),
                                        'url': item.get('item', {}).get('@id') if isinstance(item.get('item'), dict) else item.get('item'),
                                        'position': item.get('position')
                                    })
                            schema_data.update({
                                "breadcrumb_schema_present": True,
                                "breadcrumb_schema_format": "json-ld",
                                "breadcrumb_item_count": len(items),
                                "breadcrumb_schema_raw": script.string.strip()[:500],
                                "breadcrumb_items": breadcrumb_items
                            })
                            return schema_data

                except (json.JSONDecodeError, TypeError, ValueError):
                    continue

        # Strategy 2: Microdata BreadcrumbList detection
        for element in soup.find_all(attrs={'itemtype': re.compile(r'BreadcrumbList', re.IGNORECASE)}):
            itemtype = element.get('itemtype', '')
            if 'breadcrumblist' in itemtype.lower():
                items = element.find_all(attrs={'itemprop': re.compile(r'itemListElement', re.IGNORECASE)})
                breadcrumb_items = []
                for item in items:
                    name_elem = item.find(attrs={'itemprop': 'name'})
                    url_elem = item.find(attrs={'itemprop': 'item'})
                    breadcrumb_items.append({
                        'name': name_elem.get_text(strip=True) if name_elem else None,
                        'url': url_elem.get('href') if url_elem and url_elem.name == 'a' else None,
                        'position': None
                    })
                schema_data.update({
                    "breadcrumb_schema_present": True,
                    "breadcrumb_schema_format": "microdata",
                    "breadcrumb_item_count": len(items),
                    "breadcrumb_schema_raw": str(element)[:300],
                    "breadcrumb_items": breadcrumb_items
                })
                return schema_data

        # Strategy 3: RDFa BreadcrumbList detection
        for element in soup.find_all(attrs={'typeof': re.compile(r'BreadcrumbList', re.IGNORECASE)}):
            typeof_attr = element.get('typeof', '')
            if 'breadcrumblist' in typeof_attr.lower():
                items = element.find_all(attrs={'property': re.compile(r'itemListElement', re.IGNORECASE)})
                breadcrumb_items = []
                for item in items:
                    name_elem = item.find(attrs={'property': 'name'})
                    url_elem = item.find(attrs={'property': 'url'})
                    breadcrumb_items.append({
                        'name': name_elem.get_text(strip=True) if name_elem else None,
                        'url': url_elem.get('href') if url_elem and url_elem.name == 'a' else None,
                        'position': None
                    })
                schema_data.update({
                    "breadcrumb_schema_present": True,
                    "breadcrumb_schema_format": "rdfa",
                    "breadcrumb_item_count": len(items),
                    "breadcrumb_schema_raw": str(element)[:300],
                    "breadcrumb_items": breadcrumb_items
                })
                return schema_data

        return schema_data

    except Exception as e:
        return {
            "breadcrumb_schema_present": False,
            "extraction_error": str(e),
            "breadcrumb_schema_format": None,
            "breadcrumb_item_count": 0,
            "breadcrumb_schema_raw": None,
            "breadcrumb_items": []
        }


# ---------------------------------------------------------------------------
# Main Integration Function
# ---------------------------------------------------------------------------

def extract_enhanced_seo_signals(
    soup: BeautifulSoup,
    html: str,
    base_url: str,
    json_ld_entries=None
) -> Dict[str, Any]:
    """
    Extract all active enhanced SEO signals in a single call.

    Args:
        soup: BeautifulSoup object of the page
        html: Raw HTML string
        base_url: Base URL of the page
        json_ld_entries: Pre-parsed JSON-LD list from orchestrator.
                         Each entry: (raw_content: str, schemas: list[dict], is_valid: bool).
                         When provided, all JSON-LD find_all + json.loads calls are skipped.

    Returns:
        Dict containing all active signal categories (stored at top level of seo_data).
    """
    try:
        return {
            'author_signals':           extract_author_signals(soup, json_ld_entries),
            'last_updated_signals':     extract_last_updated_signals(soup, html, json_ld_entries),
            'schema_format_signals':    extract_schema_format_signals(soup, json_ld_entries),
            'faq_howto_signals':        extract_faq_howto_signals(soup, json_ld_entries),
            'breadcrumb_dom_signals':   extract_breadcrumb_dom_signals(soup),
            'breadcrumb_schema_signals': extract_breadcrumb_schema_signals(soup, json_ld_entries),
        }

    except Exception as e:
        return {'extraction_error': str(e), 'all_categories_failed': True}
