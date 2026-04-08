"""
Enhanced SEO Data Extraction - Advanced Raw Data Signals

This module provides structured extraction functions for additional SEO signals
without any scoring logic or rule evaluation. Pure data extraction only.

Categories:
1) Mixed Language Signals
2) Featured Snippet Structure Signals  
3) Voice/Natural Language Signals
4) CTR Signals (Raw only)
5) Image Context Signals
6) Author Info Signals
7) Last Updated Signals
8) Schema Format Signals
9) FAQ / HowTo Signals
"""

import re
import json
from typing import Dict, List, Any, Optional, Tuple
from bs4 import BeautifulSoup, Tag
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# 1) Mixed Language Signals
# ---------------------------------------------------------------------------

def extract_mixed_language_signals(soup: BeautifulSoup, html: str) -> Dict[str, Any]:
    """
    Extract language detection signals from page content.
    
    DOM Selectors Used:
    - [lang] attribute detection
    - All text content for analysis
    - Paragraph-level segmentation
    
    Returns:
        Dict with language detection data
    """
    try:
        # Get all text content
        all_text = soup.get_text(separator=' ', strip=True)
        paragraphs = soup.find_all('p')
        
        # Language signals from HTML attributes
        html_lang = soup.html.get('lang') if soup.html else None
        lang_attributes = []
        for element in soup.find_all(attrs={'lang': True}):
            lang_attributes.append({
                'tag': element.name,
                'lang': element.get('lang'),
                'text_preview': element.get_text()[:100].strip()
            })
        
        # Paragraph-level language analysis (raw text extraction only)
        paragraph_data = []
        for i, p in enumerate(paragraphs[:20]):  # Limit to first 20 paragraphs
            text = p.get_text(strip=True)
            if len(text) > 10:  # Skip very short paragraphs
                paragraph_data.append({
                    'index': i,
                    'text_length': len(text),
                    'text_preview': text[:100],
                    'has_lang_attribute': bool(p.get('lang')),
                    'lang_attribute': p.get('lang')
                })
        
        # Character-level analysis for language detection hints
        non_ascii_chars = len([c for c in all_text if ord(c) > 127])
        total_chars = len(all_text)
        
        return {
            'html_lang': html_lang,
            'lang_attributes': lang_attributes,
            'total_text_length': total_chars,
            'non_ascii_char_count': non_ascii_chars,
            'non_ascii_percentage': round((non_ascii_chars / total_chars * 100), 2) if total_chars > 0 else 0,
            'paragraph_count': len(paragraphs),
            'paragraph_analysis': paragraph_data
        }
        
    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 2) Featured Snippet Structure Signals
# ---------------------------------------------------------------------------

def extract_featured_snippet_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract structural signals that indicate featured snippet potential.
    
    DOM Selectors Used:
    - ol, ul (ordered/unordered lists)
    - table elements
    - h2, h3, h4 headings
    - p elements under headings
    - Top 30% DOM position calculation
    
    Returns:
        Dict with featured snippet structure data
    """
    try:
        all_elements = list(soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'table', 'dl']))
        total_elements = len(all_elements)
        top_30_percent_threshold = int(total_elements * 0.3) if total_elements > 0 else 0
        
        # Lists near top of page
        top_lists = []
        for i, element in enumerate(soup.find_all(['ul', 'ol'])):
            if i <= top_30_percent_threshold:
                list_items = element.find_all('li', limit=10)
                top_lists.append({
                    'type': element.name,
                    'position': i,
                    'item_count': len(list_items),
                    'items_preview': [li.get_text(strip=True)[:50] for li in list_items[:3]]
                })
        
        # Tables near top
        top_tables = []
        for i, table in enumerate(soup.find_all('table')):
            if i <= top_30_percent_threshold:
                rows = table.find_all('tr', limit=5)
                top_tables.append({
                    'position': i,
                    'row_count': len(rows),
                    'has_headers': bool(table.find('th')),
                    'preview': table.get_text()[:100].strip()
                })
        
        # Definition-style paragraphs (short paragraphs under H2)
        definition_patterns = []
        for h2 in soup.find_all('h2'):
            next_element = h2.find_next_sibling(['p', 'div'])
            if next_element and next_element.name == 'p':
                text = next_element.get_text(strip=True)
                if 20 <= len(text) <= 200:  # Short definition style
                    definition_patterns.append({
                        'heading': h2.get_text(strip=True),
                        'definition': text,
                        'definition_length': len(text)
                    })
        
        # Question-based headings detection
        question_words = ['who', 'what', 'when', 'where', 'why', 'how', 'is', 'are', 'can', 'should', 'will']
        question_headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = heading.get_text(strip=True).lower()
            if (text.startswith('who ') or text.startswith('what ') or text.startswith('when ') or 
                text.startswith('where ') or text.startswith('why ') or text.startswith('how ') or
                text.endswith('?') or any(text.startswith(qw + ' ') for qw in question_words)):
                question_headings.append({
                    'level': heading.name,
                    'text': heading.get_text(strip=True),
                    'is_question': text.endswith('?'),
                    'starts_with_question_word': any(text.startswith(qw + ' ') for qw in question_words)
                })
        
        return {
            'total_elements': total_elements,
            'top_30_percent_threshold': top_30_percent_threshold,
            'top_lists': top_lists,
            'top_tables': top_tables,
            'definition_patterns': definition_patterns,
            'question_headings': question_headings,
            'list_count_top': len(top_lists),
            'table_count_top': len(top_tables),
            'definition_count': len(definition_patterns),
            'question_heading_count': len(question_headings)
        }
        
    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 3) Voice/Natural Language Signals
# ---------------------------------------------------------------------------

def extract_voice_language_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract voice search and natural language signals.
    
    DOM Selectors Used:
    - All text content for sentence analysis
    - Heading elements for FAQ patterns
    - Question pattern detection
    
    Returns:
        Dict with voice/natural language data
    """
    try:
        all_text = soup.get_text(separator=' ', strip=True)
        
        # Question sentence detection
        question_indicators = ['who', 'what', 'when', 'where', 'why', 'how', 'is', 'are', 'can', 'could', 'would', 'should', 'will', 'do', 'does', 'did']
        sentences = re.split(r'[.!?]+', all_text)
        question_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) > 10:
                sentence_lower = sentence.lower()
                if (sentence.endswith('?') or 
                    any(sentence_lower.startswith(qi + ' ') for qi in question_indicators)):
                    question_sentences.append({
                        'text': sentence[:100],
                        'length': len(sentence),
                        'ends_with_question_mark': sentence.endswith('?'),
                        'starts_with_question_word': any(sentence_lower.startswith(qi + ' ') for qi in question_indicators)
                    })
        
        # Conversational phrases detection
        conversational_patterns = [
            r'\byou\b', r'\byour\b', r'\bhow to\b', r'\bstep by step\b', 
            r'\beasy way\b', r'\bquick guide\b', r'\bsimple\b', r'\bbest way\b'
        ]
        conversational_matches = {}
        for pattern in conversational_patterns:
            matches = re.findall(pattern, all_text, re.IGNORECASE)
            if matches:
                conversational_matches[pattern.replace(r'\b', '').replace(r'\\b', '')] = len(matches)
        
        # FAQ-like heading patterns
        faq_headings = []
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            text = heading.get_text(strip=True)
            text_lower = text.lower()
            
            faq_indicators = ['faq', 'frequently asked', 'questions', 'common questions']
            if any(indicator in text_lower for indicator in faq_indicators):
                faq_headings.append({
                    'level': heading.name,
                    'text': text,
                    'contains_faq_keywords': True
                })
        
        return {
            'total_sentences': len([s for s in sentences if len(s.strip()) > 5]),
            'question_sentences': question_sentences[:10],  # Limit to first 10
            'question_sentence_count': len(question_sentences),
            'conversational_matches': conversational_matches,
            'total_conversational_matches': sum(conversational_matches.values()),
            'faq_headings': faq_headings,
            'faq_heading_count': len(faq_headings)
        }
        
    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 4) CTR Signals (Raw only)
# ---------------------------------------------------------------------------

def extract_ctr_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract click-through rate related signals (raw data only).
    
    DOM Selectors Used:
    - title tag
    - meta description
    - h1, h2 headings
    - All text for number/emotional word detection
    
    Returns:
        Dict with CTR-related raw data
    """
    try:
        # Title analysis
        title = soup.title.string.strip() if soup.title else ''
        title_length = len(title)
        
        # Numbers in title
        title_numbers = re.findall(r'\d+', title)
        title_number_count = len(title_numbers)
        
        # Meta description analysis
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_desc.get('content', '').strip() if meta_desc else ''
        meta_desc_length = len(meta_description)
        
        # Emotional/power words detection (list only, no scoring)
        emotional_words = [
            'amazing', 'awesome', 'best', 'better', 'biggest', 'breakthrough', 'complete',
            'comprehensive', 'easy', 'effective', 'essential', 'exclusive', 'expert',
            'fast', 'free', 'guaranteed', 'guide', 'helpful', 'incredible', 'instant',
            'latest', 'master', 'new', 'perfect', 'powerful', 'pro', 'professional',
            'quick', 'simple', 'step-by-step', 'top', 'ultimate', 'valuable'
        ]
        
        title_lower = title.lower()
        meta_desc_lower = meta_description.lower()
        
        emotional_in_title = [word for word in emotional_words if word in title_lower]
        emotional_in_meta = [word for word in emotional_words if word in meta_desc_lower]
        
        # H1 analysis
        h1_tags = soup.find_all('h1')
        h1_texts = [h1.get_text(strip=True) for h1 in h1_tags]
        h1_lengths = [len(text) for text in h1_texts]
        
        return {
            'title': title,
            'title_length': title_length,
            'title_numbers': title_numbers,
            'title_number_count': title_number_count,
            'meta_description': meta_description,
            'meta_description_length': meta_desc_length,
            'emotional_words_in_title': emotional_in_title,
            'emotional_words_in_meta': emotional_in_meta,
            'emotional_word_count_title': len(emotional_in_title),
            'emotional_word_count_meta': len(emotional_in_meta),
            'h1_count': len(h1_tags),
            'h1_texts': h1_texts,
            'h1_lengths': h1_lengths
        }
        
    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 5) Image Context Signals
# ---------------------------------------------------------------------------

def extract_image_context_signals(soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
    """
    Extract image context and positioning signals.
    
    DOM Selectors Used:
    - img elements
    - Parent/sibling navigation
    - h1-h6 heading elements
    - p paragraph elements
    
    Returns:
        Dict with image context data
    """
    try:
        all_images = soup.find_all('img')
        all_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        all_elements = list(soup.find_all())
        total_elements = len(all_elements)
        
        image_data = []
        
        for i, img in enumerate(all_images):
            # DOM position and above-the-fold estimation
            dom_position = i
            is_above_fold = dom_position < (total_elements * 0.3)  # Top 30% considered above fold
            
            # Image basic info
            src = img.get('src', '')
            alt = img.get('alt', '')
            width = img.get('width')
            height = img.get('height')
            
            # Try to get dimensions from attributes
            try:
                img_width = int(width) if width else None
                img_height = int(height) if height else None
            except (ValueError, TypeError):
                img_width = img_height = None
            
            # Find nearest heading (search backwards and forwards)
            nearest_heading = None
            heading_distance = None
            
            for j, heading in enumerate(all_headings):
                # Simple distance calculation based on document order
                heading_pos = all_elements.index(heading) if heading in all_elements else -1
                if heading_pos >= 0:
                    distance = abs(heading_pos - dom_position)
                    if heading_distance is None or distance < heading_distance:
                        heading_distance = distance
                        nearest_heading = {
                            'text': heading.get_text(strip=True),
                            'level': heading.name,
                            'distance': distance
                        }
            
            # Surrounding paragraph text
            surrounding_text = ''
            parent = img.parent
            if parent:
                if parent.name == 'p':
                    surrounding_text = parent.get_text(strip=True)
                else:
                    # Look for adjacent paragraphs
                    prev_p = img.find_previous_sibling('p')
                    next_p = img.find_next_sibling('p')
                    if prev_p:
                        surrounding_text += prev_p.get_text(strip=True) + ' '
                    if next_p:
                        surrounding_text += next_p.get_text(strip=True)
            
            image_data.append({
                'dom_index': i,
                'src': src,
                'alt': alt,
                'width': img_width,
                'height': img_height,
                'is_above_fold_estimated': is_above_fold,
                'nearest_heading': nearest_heading,
                'surrounding_text_preview': surrounding_text[:200],
                'surrounding_text_length': len(surrounding_text),
                'has_alt': bool(alt),
                'alt_length': len(alt)
            })
        
        return {
            'total_images': len(all_images),
            'total_elements': total_elements,
            'images': image_data,
            'images_with_alt': len([img for img in image_data if img['has_alt']]),
            'images_above_fold': len([img for img in image_data if img['is_above_fold_estimated']]),
            'images_with_dimensions': len([img for img in image_data if img['width'] and img['height']])
        }
        
    except Exception as e:
        return {'error': str(e), 'extraction_failed': True}


# ---------------------------------------------------------------------------
# 6) Author Info Signals
# ---------------------------------------------------------------------------

def extract_author_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract author information signals.
    
    DOM Selectors Used:
    - Elements containing author-related text
    - Link elements with author-related hrefs
    - Schema.org author data
    
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
        
        # Schema.org author data
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

def extract_last_updated_signals(soup: BeautifulSoup, html: str) -> Dict[str, Any]:
    """
    Extract last updated and freshness signals.
    
    DOM Selectors Used:
    - time elements
    - datetime attributes
    - Schema.org date fields
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
        
        # Schema.org date detection
        schema_dates = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                schema_data = json.loads(script.string)
                schemas = [schema_data] if isinstance(schema_data, dict) else schema_data
                
                for schema in schemas:
                    if isinstance(schema, dict):
                        date_fields = ['datePublished', 'dateModified', 'uploadDate', 'dateCreated']
                        for field in date_fields:
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

def extract_schema_format_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract structured data format signals with robust JSON-LD detection.
    
    DOM Selectors Used:
    - script[type="application/ld+json"]
    - elements with itemtype attribute (Microdata)
    - elements with vocab/typeof attributes (RDFa)
    
    Returns:
        Dict with schema format data
    """
    try:
        # JSON-LD detection with robust parsing
        json_ld_scripts = []
        json_ld_types = []
        
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                content = script.string.strip() if script.string else ''
                if not content:
                    continue
                
                # Clean common JSON issues
                cleaned_content = content
                # Remove HTML entities
                cleaned_content = cleaned_content.replace('&quot;', '"').replace('&apos;', "'").replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
                # Remove trailing commas before closing brackets/braces (common malformed JSON)
                cleaned_content = re.sub(r',(\s*[}\]])', r'\1', cleaned_content)
                
                # Parse JSON with error handling
                try:
                    parsed = json.loads(cleaned_content)
                    
                    # Handle different JSON structures
                    schemas = []
                    if isinstance(parsed, dict):
                        # Check for @graph wrapper
                        if '@graph' in parsed:
                            schemas = parsed['@graph'] if isinstance(parsed['@graph'], list) else [parsed['@graph']]
                        else:
                            schemas = [parsed]
                    elif isinstance(parsed, list):
                        schemas = parsed
                    else:
                        schemas = []
                    
                    # Extract schema types from all valid schemas
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
                        'raw_content': content[:500]  # Store raw for debugging
                    })
                    
                except json.JSONDecodeError as json_err:
                    # Still count the script even if malformed
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
        
        # Additional RDFa detection (typeof without vocab)
        for element in soup.find_all(attrs={'typeof': True}):
            if not element.get('vocab'):
                rdfa_elements.append({
                    'tag': element.name,
                    'typeof': element.get('typeof'),
                    'vocab': None,
                    'property_count': len(element.find_all(attrs={'property': True})),
                    'text_preview': element.get_text(strip=True)[:100]
                })
        
        # Calculate totals
        total_detected = len(json_ld_scripts) + len(microdata_elements) + len(rdfa_elements)
        
        return {
            'json_ld_present': len(json_ld_scripts) > 0,
            'json_ld_scripts': json_ld_scripts,
            'json_ld_count': len(json_ld_scripts),
            'json_ld_types': list(set(json_ld_types)),  # Remove duplicates
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

def extract_faq_howto_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract FAQ and HowTo structured signals.
    
    DOM Selectors Used:
    - Schema.org FAQPage/HowTo detection
    - DOM pattern analysis for Q&A structures
    - Step-by-step content detection
    
    Returns:
        Dict with FAQ/HowTo data
    """
    try:
        # Schema.org FAQ and HowTo detection
        faq_schema = []
        howto_schema = []
        
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
        
        # Look for question-answer patterns (headings followed by paragraphs)
        question_indicators = ['what', 'how', 'why', 'when', 'where', 'who', 'is', 'are', 'can', 'should', 'do', 'does']
        
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            heading_text = heading.get_text(strip=True).lower()
            
            # Check if heading looks like a question
            is_question = (heading_text.endswith('?') or 
                          any(heading_text.startswith(qi + ' ') for qi in question_indicators))
            
            if is_question:
                # Look for answer in following elements
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
                # Look for FAQ items in following content
                faq_items = []
                current_element = heading.find_next_sibling()
                
                while current_element and len(faq_items) < 10:
                    if current_element.name in ['h2', 'h3', 'h4']:
                        # Potential question
                        question_text = current_element.get_text(strip=True)
                        if len(question_text) > 10:
                            faq_items.append({
                                'type': 'question',
                                'text': question_text
                            })
                    elif current_element.name in ['p', 'div']:
                        # Potential answer
                        text = current_element.get_text(strip=True)
                        if len(text) > 20:
                            faq_items.append({
                                'type': 'answer',
                                'text_preview': text[:100]
                            })
                    
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
        # Get all elements for DOM position calculation
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
            if len(breadcrumb_links) >= 2:  # Require multiple links
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
                # Skip if in header/footer (likely menus)
                if element.find_parent(['header', 'footer']):
                    continue
                
                # Skip if contains menu indicators
                element_text = element.get_text().lower()
                menu_indicators = ['menu', 'navigation', 'nav', 'main']
                if any(indicator in element_text for indicator in menu_indicators):
                    continue
                
                breadcrumb_links = element.find_all('a', href=True)
                if len(breadcrumb_links) >= 2:
                    dom_position = all_elements.index(element) if element in all_elements else -1
                    
                    # Only consider if in top 30% of DOM
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
        list_elements = soup.find_all(['ol', 'ul'])
        
        for list_elem in list_elements:
            # Skip if in header/footer
            if list_elem.find_parent(['header', 'footer']):
                continue
            
            # Check if it has multiple linked list items
            linked_items = list_elem.find_all('li')
            linked_items_with_links = [li for li in linked_items if li.find('a', href=True)]
            
            if len(linked_items_with_links) >= 2:
                dom_position = all_elements.index(list_elem) if list_elem in all_elements else -1
                
                # Must be in top 30% of DOM
                if dom_position <= top_30_threshold:
                    # Additional check: links should be hierarchical (different paths)
                    links = [li.find('a', href=True) for li in linked_items_with_links]
                    hrefs = [link.get('href') for link in links if link]
                    
                    # Skip if all links point to same section (likely menu)
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

def extract_breadcrumb_schema_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Extract BreadcrumbList structured data from all schema formats.
    
    DOM Selectors Used:
    - script[type="application/ld+json"] with @type == "BreadcrumbList"
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
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_ld_scripts:
            try:
                if not script.string:
                    continue
                    
                parsed_schema = json.loads(script.string.strip())
                
                # Handle both single object and array of objects
                schemas = [parsed_schema] if isinstance(parsed_schema, dict) else parsed_schema
                
                for schema in schemas:
                    if not isinstance(schema, dict):
                        continue
                    
                    schema_type = schema.get('@type')
                    
                    # Handle @type as string or array
                    if isinstance(schema_type, str):
                        types = [schema_type]
                    elif isinstance(schema_type, list):
                        types = schema_type
                    else:
                        continue
                    
                    if 'BreadcrumbList' in types:
                        # Extract breadcrumb items
                        items = schema.get('itemListElement', [])
                        item_count = len(items)
                        
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
                            "breadcrumb_item_count": item_count,
                            "breadcrumb_schema_raw": script.string.strip()[:500],  # Truncate for storage
                            "breadcrumb_items": breadcrumb_items
                        })
                        return schema_data
                        
            except (json.JSONDecodeError, TypeError, ValueError):
                continue  # Skip malformed JSON
        
        # Strategy 2: Microdata BreadcrumbList detection
        microdata_elements = soup.find_all(attrs={'itemtype': re.compile(r'BreadcrumbList', re.IGNORECASE)})
        
        for element in microdata_elements:
            # Extract itemtype attribute
            itemtype = element.get('itemtype', '')
            
            if 'breadcrumblist' in itemtype.lower():
                # Find breadcrumb items using itemprop
                items = element.find_all(attrs={'itemprop': re.compile(r'itemListElement', re.IGNORECASE)})
                
                breadcrumb_items = []
                for item in items:
                    name_elem = item.find(attrs={'itemprop': 'name'})
                    url_elem = item.find(attrs={'itemprop': 'item'})
                    
                    breadcrumb_items.append({
                        'name': name_elem.get_text(strip=True) if name_elem else None,
                        'url': url_elem.get('href') if url_elem and url_elem.name == 'a' else None,
                        'position': item.get('data-position') or None  # Microdata doesn't have standard position
                    })
                
                schema_data.update({
                    "breadcrumb_schema_present": True,
                    "breadcrumb_schema_format": "microdata",
                    "breadcrumb_item_count": len(items),
                    "breadcrumb_schema_raw": str(element)[:300],  # HTML snippet
                    "breadcrumb_items": breadcrumb_items
                })
                return schema_data
        
        # Strategy 3: RDFa BreadcrumbList detection
        rdfa_elements = soup.find_all(attrs={'typeof': re.compile(r'BreadcrumbList', re.IGNORECASE)})
        
        for element in rdfa_elements:
            typeof_attr = element.get('typeof', '')
            
            if 'breadcrumblist' in typeof_attr.lower():
                # Find breadcrumb items using property attribute
                items = element.find_all(attrs={'property': re.compile(r'itemListElement', re.IGNORECASE)})
                
                breadcrumb_items = []
                for item in items:
                    name_elem = item.find(attrs={'property': 'name'})
                    url_elem = item.find(attrs={'property': 'url'})
                    
                    breadcrumb_items.append({
                        'name': name_elem.get_text(strip=True) if name_elem else None,
                        'url': url_elem.get('href') if url_elem and url_elem.name == 'a' else None,
                        'position': None  # RDFa doesn't have standard position
                    })
                
                schema_data.update({
                    "breadcrumb_schema_present": True,
                    "breadcrumb_schema_format": "rdfa",
                    "breadcrumb_item_count": len(items),
                    "breadcrumb_schema_raw": str(element)[:300],  # HTML snippet
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
# 12) Security Headers Extraction
# ---------------------------------------------------------------------------

def extract_security_headers_signals(response_headers: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Extract security headers from HTTP response.
    
    Headers to Check (case insensitive):
    - content-security-policy (CSP)
    - strict-transport-security (HSTS)
    - x-frame-options
    - x-content-type-options
    - referrer-policy
    - permissions-policy
    
    Returns:
        Dict with security headers data
    """
    try:
        if not response_headers:
            return {
                "security_headers": {
                    "csp": None,
                    "hsts": None,
                    "x_frame_options": None,
                    "x_content_type_options": None,
                    "referrer_policy": None,
                    "permissions_policy": None
                },
                "headers_detected": 0,
                "total_security_headers": 6,
                "note": "no_response_headers_provided"
            }
        
        # Normalize headers to lowercase for case-insensitive matching
        normalized_headers = {}
        for key, value in response_headers.items():
            normalized_headers[key.lower().strip()] = value.strip() if value else None
        
        security_headers = {
            "csp": normalized_headers.get('content-security-policy'),
            "hsts": normalized_headers.get('strict-transport-security'),
            "x_frame_options": normalized_headers.get('x-frame-options'),
            "x_content_type_options": normalized_headers.get('x-content-type-options'),
            "referrer_policy": normalized_headers.get('referrer-policy'),
            "permissions_policy": normalized_headers.get('permissions-policy')
        }
        
        return {
            "security_headers": security_headers,
            "headers_detected": sum(1 for v in security_headers.values() if v is not None),
            "total_security_headers": 6
        }
        
    except Exception as e:
        return {
            "security_headers": {
                "csp": None,
                "hsts": None,
                "x_frame_options": None,
                "x_content_type_options": None,
                "referrer_policy": None,
                "permissions_policy": None
            },
            "extraction_error": str(e),
            "headers_detected": 0,
            "total_security_headers": 6
        }


# ---------------------------------------------------------------------------
# Main Integration Function
# ---------------------------------------------------------------------------

def extract_enhanced_seo_signals(soup: BeautifulSoup, html: str, base_url: str, response_headers: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Extract all enhanced SEO signals in a single call.
    
    Args:
        soup: BeautifulSoup object of the page
        html: Raw HTML string
        base_url: Base URL of the page
        response_headers: HTTP response headers dict (optional)
        
    Returns:
        Dict containing all enhanced signal categories
    """
    try:
        enhanced_signals = {
            'mixed_language_signals': extract_mixed_language_signals(soup, html),
            'featured_snippet_signals': extract_featured_snippet_signals(soup),
            'voice_language_signals': extract_voice_language_signals(soup),
            'ctr_signals': extract_ctr_signals(soup),
            'image_context_signals': extract_image_context_signals(soup, base_url),
            'author_signals': extract_author_signals(soup),
            'last_updated_signals': extract_last_updated_signals(soup, html),
            'schema_format_signals': extract_schema_format_signals(soup),
            'faq_howto_signals': extract_faq_howto_signals(soup),
            'breadcrumb_dom_signals': extract_breadcrumb_dom_signals(soup),
            'breadcrumb_schema_signals': extract_breadcrumb_schema_signals(soup),
            'security_headers_signals': extract_security_headers_signals(response_headers)
        }
        
        return enhanced_signals
        
    except Exception as e:
        return {
            'extraction_error': str(e),
            'all_categories_failed': True
        }
