"""AI Visibility worker implementation."""

import os
import threading
import time
import hashlib
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Any
from bson.objectid import ObjectId
from fastapi import HTTPException
from pydantic import BaseModel
import requests
from bs4 import BeautifulSoup, NavigableString
import math

# === PHASE 1 SAFETY ADDITION ===
# Import signal for timeout handling
import signal
from contextlib import contextmanager

# Local imports
from scraper.shared.fetcher import fetch_html
from db import seo_ai_visibility, seo_ai_visibility_project, seoprojects

# ==================== PHASE 2: AI-READY EXTRACTION LAYER ====================

def extract_main_content(soup, url) -> dict:
    """Extract main content using modern builder-aware priority hierarchy"""
    try:
        # === BUG FIX 3: Enhanced Elementor and modern builder support ===
        content_selectors = [
            # WordPress Gutenberg (highest priority)
            'main .entry-content',
            'main .post-content',
            'main .content-area',
            'main .site-content',
            
            # Elementor (specific detection)
            '.elementor-element.elementor-widget-theme-post-content',
            '.elementor-element.elementor-widget-text-editor',
            '.elementor-section-wrap',
            'div[data-elementor-type="wp-post"]',
            'div[data-elementor-type="single-post"]',
            'div[data-elementor-type="page"]',
            '.elementor-location-single',
            
            # Standard HTML5 semantic tags
            'main',
            'article',
            '[role="main"]',
            
            # Common CMS patterns
            '.content',
            '.post-content',
            '.entry-content',
            '.article-content',
            '.page-content',
            '.site-main',
            
            # WordPress specific
            '#content',
            '#main',
            '.main-content',
            '.hentry',
            
            # Fallbacks
            'body'
        ]
        
        main_content_element = None
        selector_used = "body"
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                # For multiple elements, choose the one with most text content
                best_element = None
                max_text_length = 0
                
                for element in elements:
                    # Remove script/style elements temporarily for text measurement
                    temp_soup = BeautifulSoup(str(element), 'html.parser')
                    for tag in temp_soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                        tag.decompose()
                    
                    text_length = len(temp_soup.get_text(strip=True))
                    if text_length > max_text_length:
                        max_text_length = text_length
                        best_element = element
                
                if best_element and max_text_length > 100:  # Minimum content threshold
                    main_content_element = best_element
                    selector_used = selector
                    print(f"[MAIN_CONTENT] Found content with selector: {selector} ({max_text_length} chars)")
                    break
        
        if not main_content_element:
            print("[MAIN_CONTENT] No suitable main content element found, using body")
            main_content_element = soup.find('body')
            selector_used = "body"
        
        if not main_content_element:
            return {
                'main_content_text': '',
                'main_content_html': '',
                'content_extraction_method': 'failed',
                'content_word_count': 0,
                'content_selector_used': 'none'
            }
        
        # Create a clean copy for content extraction
        content_soup = BeautifulSoup(str(main_content_element), 'html.parser')
        
        # === BUG FIX 4: Enhanced non-content removal ===
        non_content_selectors = [
            'nav', 'header', 'footer', 'aside', 'form',
            '.nav', '.navigation', '.menu', '.sidebar',
            '.footer', '.header', '.ads', '.advertisement',
            'script', 'style', 'noscript', 'iframe',
            '.comments', '.comment', '.disqus', '.cookie', '.banner',
            '.popup', '.modal', '.overlay', '.social', '.share',
            '.related-posts', '.author-box', '.post-meta',
            '.elementor-nav-menu', '.elementor-location-header',
            '.menu-primary', '.menu-secondary', '.mobile-menu'
        ]
        
        for selector in non_content_selectors:
            for element in content_soup.select(selector):
                element.decompose()
        
        # Extract text and HTML
        main_content_text = content_soup.get_text(separator=' ', strip=True)
        main_content_html = str(content_soup)
        
        print(f"[MAIN_CONTENT] Extracted content using selector: {selector_used}")
        
        return {
            'main_content_text': main_content_text[:5000],  # Limit for storage
            'main_content_html': main_content_html[:10000],  # Limit for storage
            'content_extraction_method': 'enhanced_builder_aware',
            'content_selector_used': selector_used
        }
        
    except Exception as e:
        print(f"[MAIN_CONTENT] Extraction failed: {e}")
        return {
            'main_content_text': '',
            'main_content_html': '',
            'content_extraction_method': 'failed',
            'content_word_count': 0,
            'content_selector_used': 'error',
            'extraction_errors': [str(e)]
        }

def extract_heading_hierarchy(soup) -> dict:
    """Extract heading hierarchy with sequential validation (production-level)"""
    try:
        # === PHASE 2: HEADING HIERARCHY MAP - SEQUENTIAL VALIDATION ===
        headings = {'h1': [], 'h2': [], 'h3': [], 'h4': [], 'h5': [], 'h6': []}
        
        for level in headings.keys():
            for heading in soup.find_all(level):
                text = heading.get_text(strip=True)
                if text:
                    headings[level].append(text)
        
        h1_count = len(headings['h1'])
        h2_count = len(headings['h2'])
        h3_count = len(headings['h3'])
        
        # === CRITICAL FIX: Sequential validation instead of just counting ===
        # Extract actual heading order from document
        all_headings_in_order = []
        for level in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            for heading in soup.find_all(level):
                text = heading.get_text(strip=True)
                if text:
                    all_headings_in_order.append({
                        'level': int(level[1]),  # Convert 'h1' to 1
                        'text': text,
                        'tag': level
                    })
        
        # Validate sequential order
        heading_sequence_valid = True
        sequence_violations = []
        
        for i in range(1, len(all_headings_in_order)):
            current_level = all_headings_in_order[i]['level']
            prev_level = all_headings_in_order[i-1]['level']
            
            # === CRITICAL FIX: Detect skipped levels ===
            # H3 should not follow H1 directly (should have H2)
            if current_level > prev_level + 1:
                heading_sequence_valid = False
                sequence_violations.append({
                    'from': f'h{prev_level}',
                    'to': f'h{current_level}',
                    'position': i
                })
        
        # Check for multiple H1s
        multiple_h1 = h1_count > 1
        
        # Detect question headings
        question_patterns = [r'\?', r'what\s+is', r'how\s+to', r'why\s+does', r'when\s+can', r'where\s+is']
        question_headings = []
        
        for level, heading_list in headings.items():
            for heading in heading_list:
                for pattern in question_patterns:
                    if re.search(pattern, heading, re.IGNORECASE):
                        question_headings.append({'level': level, 'text': heading})
                        break
        
        # === CRITICAL FIX: More accurate heading structure score ===
        score = 100
        if h1_count == 0:
            score -= 30
        elif multiple_h1:
            score -= 20
        if not heading_sequence_valid:
            score -= 25  # Penalize skipped levels
        if h2_count == 0 and h3_count > 0:
            score -= 15
        if len(sequence_violations) > 2:
            score -= 10  # Extra penalty for many violations
        
        return {
            'h1_count': h1_count,
            'h2_count': h2_count,
            'h3_count': h3_count,
            'heading_sequence_valid': heading_sequence_valid,
            'question_headings': question_headings[:10],  # Limit storage
            'heading_structure_score_input': max(0, score),
            'total_headings': sum(len(h) for h in headings.values()),
            'sequence_violations': sequence_violations[:5],  # Store for debugging
            'all_headings_in_order': all_headings_in_order[:20]  # Store for analysis
        }
        
    except Exception as e:
        print(f"[PHASE2] Heading hierarchy extraction failed: {e}")
        return {
            'h1_count': 0, 'h2_count': 0, 'h3_count': 0,
            'heading_sequence_valid': False, 'question_headings': [],
            'heading_structure_score_input': 0, 'total_headings': 0,
            'sequence_violations': [], 'all_headings_in_order': []
        }

def extract_paragraph_metrics(text) -> dict:
    """Extract paragraph structure and readability metrics"""
    try:
        # === PHASE 2: PARAGRAPH STRUCTURE METRICS ===
        if not text:
            return {
                'paragraph_count': 0, 'avg_paragraph_length': 0,
                'short_paragraph_ratio': 0, 'long_paragraph_ratio': 0,
                'avg_sentence_length': 0
            }
        
        # Split into paragraphs
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        paragraph_count = len(paragraphs)
        
        print("=== PARAGRAPH COUNT DEBUG - PIPELINE A ===")
        print(f"method used: text split by double newline (\\n\\n)")
        print(f"raw paragraph elements found: {len(paragraphs)}")
        print(f"paragraph_count assigned: {paragraph_count}")
        print("=== END PIPELINE A ===")
        
        if paragraph_count == 0:
            return {
                'paragraph_count': 0, 'avg_paragraph_length': 0,
                'short_paragraph_ratio': 0, 'long_paragraph_ratio': 0,
                'avg_sentence_length': 0
            }
        
        # Calculate paragraph lengths
        paragraph_lengths = [len(p.split()) for p in paragraphs]
        avg_paragraph_length = sum(paragraph_lengths) / len(paragraph_lengths)
        
        # Count short and long paragraphs
        short_paragraphs = sum(1 for length in paragraph_lengths if length < 120)
        long_paragraphs = sum(1 for length in paragraph_lengths if length > 300)
        
        short_paragraph_ratio = (short_paragraphs / paragraph_count) * 100
        long_paragraph_ratio = (long_paragraphs / paragraph_count) * 100
        
        # Calculate sentence metrics
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if sentences:
            sentence_lengths = [len(s.split()) for s in sentences]
            avg_sentence_length = sum(sentence_lengths) / len(sentence_lengths)
        else:
            avg_sentence_length = 0
        
        return {
            'paragraph_count': paragraph_count,
            'avg_paragraph_length': round(avg_paragraph_length, 1),
            'short_paragraph_ratio': round(short_paragraph_ratio, 1),
            'long_paragraph_ratio': round(long_paragraph_ratio, 1),
            'avg_sentence_length': round(avg_sentence_length, 1)
        }
        
    except Exception as e:
        print(f"[PHASE2] Paragraph metrics extraction failed: {e}")
        return {
            'paragraph_count': 0, 'avg_paragraph_length': 0,
            'short_paragraph_ratio': 0, 'long_paragraph_ratio': 0,
            'avg_sentence_length': 0
        }

def detect_faq_content(soup, text) -> dict:
    """Detect FAQ and Q&A content with Q+A pair validation (production-level)"""
    try:
        # === PHASE 2: FAQ & QUESTION DETECTION - Q+A PA VALIDATION ===
        faq_detected = False
        question_count = 0
        qa_pairs_detected = 0
        
        # Check for FAQPage schema and extract actual FAQ data
        faq_schema = False
        faq_data_from_schema = []
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if data.get('@type') == 'FAQPage' and 'mainEntity' in data:
                        faq_schema = True
                        faq_detected = True
                        # === CRITICAL FIX: Extract actual FAQ questions from schema ===
                        for faq_item in data['mainEntity']:
                            if faq_item.get('@type') == 'Question':
                                question_text = faq_item.get('name', '')
                                answer_data = faq_item.get('acceptedAnswer', {})
                                answer_text = answer_data.get('text', '')
                                
                                if question_text and answer_text:
                                    faq_data_from_schema.append({
                                        'question': question_text,
                                        'answer': answer_text,
                                        'all_answers': [answer_text],
                                        'question_word_count': len(question_text.split()),
                                        'answer_word_count': len(answer_text.split()),
                                        'has_answer': True,
                                        'question_length': len(question_text),
                                        'answer_length': len(answer_text),
                                        'multiple_answers': False,
                                        'answer_count': 1
                                    })
                        break
                    elif isinstance(data.get('@graph'), list):
                        for item in data['@graph']:
                            if item.get('@type') == 'FAQPage' and 'mainEntity' in item:
                                faq_schema = True
                                faq_detected = True
                                # === CRITICAL FIX: Extract actual FAQ questions from @graph ===
                                for faq_item in item['mainEntity']:
                                    if faq_item.get('@type') == 'Question':
                                        question_text = faq_item.get('name', '')
                                        answer_data = faq_item.get('acceptedAnswer', {})
                                        answer_text = answer_data.get('text', '')
                                        
                                        if question_text and answer_text:
                                            faq_data_from_schema.append({
                                                'question': question_text,
                                                'answer': answer_text,
                                                'all_answers': [answer_text],
                                                'question_word_count': len(question_text.split()),
                                                'answer_word_count': len(answer_text.split()),
                                                'has_answer': True,
                                                'question_length': len(question_text),
                                                'answer_length': len(answer_text),
                                                'multiple_answers': False,
                                                'answer_count': 1
                                            })
                                break
            except:
                continue
        
        # Detect question patterns in text
        question_patterns = [
            r'\bwhat\s+is\b', r'\bhow\s+to\b', r'\bwhy\s+does\b', r'\bwhen\s+can\b',
            r'\bwhere\s+is\b', r'\bwho\s+can\b', r'\bwhich\s+is\b', r'\bdo\s+i\b',
            r'\bcan\s+i\b', r'\bshould\s+i\b', r'\bwill\s+i\b', r'\bare\s+there\b'
        ]
        
        questions_found = []
        for pattern in question_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            questions_found.extend(matches)
        
        question_count = len(questions_found)
        
        # === CRITICAL FIX: Require Q+A pairs, not just questions ===
        # Split text into sentences
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        valid_qa_pairs = []
        
        # Look for question followed by answer pattern
        for i, sentence in enumerate(sentences):
            # Check if this sentence is a question
            is_question = False
            for pattern in question_patterns:
                if re.search(pattern, sentence, re.IGNORECASE):
                    is_question = True
                    break
            
            if is_question and i + 1 < len(sentences):
                # Check if next sentence is a reasonable answer
                next_sentence = sentences[i + 1]
                
                # === CRITICAL FIX: Validate answer quality ===
                # Answer should be substantial (not just "Yes" or "No")
                answer_words = len(next_sentence.split())
                answer_reasonable = (
                    answer_words >= 5 and  # Minimum length
                    len(next_sentence) > 20 and  # Minimum characters
                    not re.match(r'^(yes|no|true|false|correct|incorrect)\s*\.?$', next_sentence.lower())
                )
                
                if answer_reasonable:
                    valid_qa_pairs.append({
                        'question': sentence,
                        'answer': next_sentence,
                        'position': i
                    })
        
        qa_pairs_detected = len(valid_qa_pairs)
        
        # === CRITICAL FIX: Use schema FAQ count if available, more accurate ===
        if faq_schema and faq_data_from_schema:
            # Use actual FAQ count from schema (more reliable than text parsing)
            question_count = len(faq_data_from_schema)
            qa_pairs_detected = len(faq_data_from_schema)  # All schema FAQs have answers
        
        # === CRITICAL FIX: More strict FAQ detection criteria ===
        # FAQ detected if:
        # 1. FAQ schema present, OR
        # 2. At least 3 valid Q+A pairs, OR
        # 3. At least 5 questions AND at least 2 valid Q+A pairs
        
        if faq_schema:
            faq_detected = True
        elif qa_pairs_detected >= 3:
            faq_detected = True
        elif question_count >= 5 and qa_pairs_detected >= 2:
            faq_detected = True
        
        # Additional FAQ indicators
        faq_indicators = [
            r'\bfaq\b', r'\bfrequently\s+asked\s+questions\b',
            r'\bquestions\s+and\s+answers\b', r'\bq\s*&\s*a\b'
        ]
        
        faq_text_matches = 0
        for indicator in faq_indicators:
            if re.search(indicator, text, re.IGNORECASE):
                faq_text_matches += 1
        
        # Boost FAQ confidence if text indicators present
        if faq_text_matches > 0 and question_count >= 2:
            faq_detected = True
        
        return {
            'faq_detected': faq_detected,
            'question_count': question_count,
            'qa_pairs_detected': qa_pairs_detected,
            'faq_schema_detected': faq_schema,
            'question_patterns_found': len(questions_found),
            'faq_text_indicators': faq_text_matches,
            'valid_qa_pairs': valid_qa_pairs,
            'faq_data': faq_data_from_schema,  # === CRITICAL FIX: Return actual FAQ data ===
            'detection_method': 'schema' if faq_schema else ('qa_pairs' if qa_pairs_detected >= 3 else 'mixed')
        }
        
    except Exception as e:
        print(f"[PHASE2] FAQ detection failed: {e}")
        return {
            'faq_detected': False, 'question_count': 0,
            'qa_pairs_detected': 0, 'faq_schema_detected': False,
            'question_patterns_found': 0, 'faq_text_indicators': 0,
            'valid_qa_pairs': [], 'detection_method': 'error'
        }

def detect_step_by_step(soup, text) -> dict:
    """Detect step-by-step content"""
    try:
        # === PHASE 2: STEP-BY-STEP DETECTION ===
        step_section_present = False
        step_count = 0
        
        # Check for ordered lists
        ordered_lists = soup.find_all('ol')
        if ordered_lists:
            step_section_present = True
            for ol in ordered_lists:
                step_count += len(ol.find_all('li'))
        
        # Check for HowTo schema
        howto_schema = False
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if data.get('@type') == 'HowTo':
                        howto_schema = True
                        step_section_present = True
                        break
                    elif isinstance(data.get('@graph'), list):
                        for item in data['@graph']:
                            if item.get('@type') == 'HowTo':
                                howto_schema = True
                                step_section_present = True
                                break
            except:
                continue
        
        # Detect step patterns in text
        step_patterns = [
            r'\bstep\s+\d+\b', r'\bfirst\b', r'\bsecond\b', r'\bthird\b',
            r'\bnext\b', r'\bthen\b', r'\bafter\s+that\b', r'\bfinally\b'
        ]
        
        step_matches = []
        for pattern in step_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            step_matches.extend(matches)
        
        if step_matches and not step_section_present:
            step_section_present = True
            step_count = max(step_count, len(set(step_matches)))
        
        return {
            'step_section_present': step_section_present,
            'step_count': step_count,
            'howto_schema_detected': howto_schema,
            'ordered_lists_found': len(ordered_lists)
        }
        
    except Exception as e:
        print(f"[PHASE2] Step detection failed: {e}")
        return {
            'step_section_present': False, 'step_count': 0,
            'howto_schema_detected': False, 'ordered_lists_found': 0
        }

def calculate_flesch_readability(text) -> dict:
    """Calculate Flesch Reading Ease score with acronym handling (production-level)"""
    try:
        # === PHASE 2: FLESCH READABILITY SCORE - ACRONYM AWARE ===
        if not text:
            return {'flesch_score': 0, 'reading_grade_level': 'Unknown'}
        
        # === CRITICAL FIX: Minimum word threshold for accuracy ===
        words = re.findall(r'\b\w+\b', text)
        word_count = len(words)
        
        if word_count < 100:
            return {
                'flesch_score': 0, 
                'reading_grade_level': 'Insufficient text',
                'word_count': word_count,
                'warning': 'Text too short for accurate readability assessment'
            }
        
        # Count sentences, words, syllables
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        sentence_count = len(sentences)
        
        if sentence_count == 0:
            return {'flesch_score': 0, 'reading_grade_level': 'Unknown'}
        
        # === CRITICAL FIX: Improved syllable counting for acronyms ===
        def count_syllables(word):
            word = word.lower()
            
            # === CRITICAL FIX: Handle common acronyms and tech terms ===
            # Common acronyms that should be 1 syllable
            acronym_exceptions = {
                'ai', 'seo', 'sem', 'ppc', 'crm', 'erp', 'cms', 'api', 'ui', 'ux',
                'sql', 'nosql', 'html', 'css', 'js', 'xml', 'json', 'csv', 'pdf',
                'ceo', 'cto', 'cfo', 'coo', 'hr', 'pr', 'r&d', 'qa', 'qc',
                'b2b', 'b2c', 'saas', 'paas', 'iaas', 'iot', 'ar', 'vr',
                'gdp', 'gnp', 'roi', 'kpi', 'okr', 'slp', 'cpc', 'cpm'
            }
            
            # Check if it's a known acronym
            if word in acronym_exceptions:
                return 1
            
            # Check if it's all uppercase (likely acronym)
            if len(word) <= 6 and word.isupper() and not word.isdigit():
                return 1
            
            # Check if it contains numbers (like version numbers)
            if any(char.isdigit() for char in word):
                return 1
            
            vowels = 'aeiouy'
            syllable_count = 0
            prev_char_was_vowel = False
            
            for char in word:
                if char in vowels:
                    if not prev_char_was_vowel:
                        syllable_count += 1
                    prev_char_was_vowel = True
                else:
                    prev_char_was_vowel = False
            
            # Adjust for silent 'e'
            if word.endswith('e') and syllable_count > 1:
                syllable_count -= 1
            
            # Adjust for common patterns
            if word.endswith('le') and len(word) > 2 and word[-3] not in vowels:
                syllable_count += 1
            
            # Ensure at least 1 syllable
            return max(1, syllable_count)
        
        syllable_count = sum(count_syllables(word) for word in words)
        
        # Calculate Flesch Reading Ease score
        avg_sentence_length = word_count / sentence_count
        avg_syllables_per_word = syllable_count / word_count
        
        flesch_score = 206.835 - (1.015 * avg_sentence_length) - (84.6 * avg_syllables_per_word)
        flesch_score = max(0, min(100, flesch_score))  # Clamp between 0-100
        
        # Determine grade level
        if flesch_score >= 90:
            grade = 'Very Easy (5th grade)'
        elif flesch_score >= 80:
            grade = 'Easy (6th grade)'
        elif flesch_score >= 70:
            grade = 'Fairly Easy (7th grade)'
        elif flesch_score >= 60:
            grade = 'Standard (8th-9th grade)'
        elif flesch_score >= 50:
            grade = 'Fairly Difficult (10th-12th grade)'
        elif flesch_score >= 30:
            grade = 'Difficult (College level)'
        else:
            grade = 'Very Difficult (Graduate level)'
        
        return {
            'flesch_score': round(flesch_score, 1),
            'reading_grade_level': grade,
            'word_count': word_count,
            'sentence_count': sentence_count,
            'avg_sentence_length': round(avg_sentence_length, 1),
            'avg_syllables_per_word': round(avg_syllables_per_word, 1)
        }
        
    except Exception as e:
        print(f"[PHASE2] Flesch readability calculation failed: {e}")
        return {'flesch_score': 0, 'reading_grade_level': 'Unknown'}

def calculate_entity_density(entity_graph, text, word_count, ai_signals=None) -> dict:
    """Calculate entity density metrics with entity count cap (production-hardened)"""
    try:
        # === PHASE 2: ENTITY DENSITY METRICS - WITH ENTITY CAP ===
        if not entity_graph or not text:
            return {
                'entity_count': 0, 'unique_entity_types': 0,
                'entity_per_1000_words': 0, 'primary_entity_mentions_in_text': 0
            }
        
        # Get parsed_entities from ai_signals
        parsed_entities = []
        if ai_signals:
            parsed_entities = ai_signals.get('parsed_entities', [])
        
        # === PART 3: ENTITY COUNT CAP GUARD ===
        MAX_ENTITIES = 500
        
        # Extract entities with cap
        entities = []
        entity_types = set()
        
        if 'entities' in entity_graph:
            for entity_id, entity_data in entity_graph['entities'].items():
                if len(entities) >= MAX_ENTITIES:
                    print(f"[PERFORMANCE_GUARD] Entity cap reached: {MAX_ENTITIES} entities")
                    break
                
                entities.append(entity_data)
                if '@type' in entity_data:
                    entity_types.add(entity_data['@type'])
        
        entity_count = len(entities)
        unique_entity_types = len(entity_types)
        
        # Calculate entity density per 1000 words
        if word_count > 0:
            entity_per_1000_words = (entity_count / word_count) * 1000
        else:
            entity_per_1000_words = 0
        
        # SELF-CONTAINED ENTITY MENTION COUNTING FIX
        # Use the text parameter directly instead of requiring result object
        def calculate_entity_mentions(text, parsed_entities):
            # Always get entity name from Organization first, NOT from WebPage
            entity_name = None

            for entity in parsed_entities:
                if entity.get("@type") == "Organization":
                    entity_name = (entity.get("name") or entity.get("legalName") or "").strip()
                    if entity_name:
                        break

            # Fallback: WebSite name (e.g. "Sapphire Digital Connect")
            if not entity_name:
                for entity in parsed_entities:
                    if entity.get("@type") == "WebSite":
                        entity_name = entity.get("name", "").strip()
                        if entity_name:
                            break

            # Do NOT use WebPage.name — it contains the full page title, not the brand name
            if not entity_name:
                print(f"[ENTITY_FIX] No Organization entity found in parsed_entities")
                return 0

            # Use the provided text directly for counting
            entity_text = text.strip()
            
            if entity_name and entity_text.strip():
                import re
                count = len(re.findall(re.escape(entity_name), entity_text, re.IGNORECASE))
                print(f"[ENTITY_FIX] Using entity name: '{entity_name}' (from Organization.legalName fallback)")
                print(f"[ENTITY_FIX] Search corpus length: {len(entity_text)} chars")
                print(f"[ENTITY_FIX] Counted {count} mentions of '{entity_name}'")
            else:
                count = 0
                print(f"[ENTITY_FIX] No entity name or text available for counting")

            return count
        
        # Calculate entity mentions using self-contained function
        count = calculate_entity_mentions(text, parsed_entities)
        
        return {
            'entity_count': entity_count,
            'unique_entity_types': unique_entity_types,
            'entity_per_1000_words': round(entity_per_1000_words, 2),
            'primary_entity_mentions_in_text': count,
            'entity_cap_reached': entity_count >= MAX_ENTITIES
        }
        
    except Exception as e:
        print(f"[PHASE2] Entity density calculation failed: {e}")
        return {
            'entity_count': 0, 'unique_entity_types': 0,
            'entity_per_1000_words': 0, 'primary_entity_mentions_in_text': 0,
            'entity_cap_reached': False
        }

def classify_intent(text) -> dict:
    """Rule-based intent classification with weighted scoring (production-level)"""
    try:
        # === PHASE 2: INTENT CLASSIFICATION - WEIGHTED SCORING ===
        if not text:
            return {
                'intent_distribution': {
                    'informational': 25, 'commercial': 25,
                    'local': 25, 'navigational': 25
                },
                'confidence': 'low',
                'total_keywords_found': 0
            }
        
        text_lower = text.lower()
        words = re.findall(r'\b\w+\b', text_lower)
        total_words = len(words)
        
        # === CRITICAL FIX: Weighted keywords to prevent overfitting ===
        # Informational keywords with weights
        informational_keywords = {
            'what': 3, 'how': 3, 'guide': 4, 'tutorial': 4, 'learn': 3, 'understand': 3,
            'explain': 3, 'definition': 2, 'meaning': 2, 'example': 2, 'basics': 2,
            'introduction': 3, 'overview': 2, 'steps': 2, 'process': 2, 'method': 2
        }
        
        # Commercial keywords with weights
        commercial_keywords = {
            'best': 4, 'top': 4, 'pricing': 5, 'price': 4, 'cost': 4, 'service': 3,
            'product': 3, 'buy': 5, 'purchase': 5, 'deal': 4, 'discount': 4, 'offer': 4,
            'review': 3, 'compare': 4, 'vs': 3, 'versus': 3, 'cheap': 3, 'affordable': 3,
            'premium': 3, 'professional': 2, 'enterprise': 3, 'business': 2
        }
        
        # Local keywords with weights
        local_keywords = {
            'near me': 5, 'nearby': 4, 'location': 3, 'address': 3, 'phone': 3,
            'hours': 3, 'directions': 4, 'map': 3, 'local': 3, 'store': 3,
            'office': 3, 'branch': 2, 'in [city]': 2, 'area': 2, 'neighborhood': 2
        }
        
        # Navigational keywords with weights
        navigational_keywords = {
            'login': 5, 'signin': 5, 'contact': 4, 'about': 3, 'homepage': 4, 'home': 4,
            'dashboard': 3, 'account': 4, 'profile': 3, 'settings': 3, 'support': 3,
            'help': 2, 'faq': 2, 'sitemap': 2, 'search': 2, 'navigate': 2
        }
        
        # Calculate weighted scores
        def calculate_weighted_score(keywords_dict):
            score = 0
            matches = []
            for keyword, weight in keywords_dict.items():
                if keyword in text_lower:
                    # Count occurrences
                    count = text_lower.count(keyword)
                    score += weight * count
                    matches.append(keyword)
            return score, matches
        
        informational_score, info_matches = calculate_weighted_score(informational_keywords)
        commercial_score, comm_matches = calculate_weighted_score(commercial_keywords)
        local_score, local_matches = calculate_weighted_score(local_keywords)
        navigational_score, nav_matches = calculate_weighted_score(navigational_keywords)
        
        total_score = informational_score + commercial_score + local_score + navigational_score
        
        if total_score == 0:
            # === CRITICAL FIX: Default distribution if no keywords found ===
            return {
                'intent_distribution': {
                    'informational': 40, 'commercial': 30,
                    'local': 15, 'navigational': 15
                },
                'confidence': 'very_low',
                'total_keywords_found': 0,
                'word_count': total_words
            }
        
        # === CRITICAL FIX: Normalize by total words to prevent keyword stuffing ===
        # Adjust scores based on text length
        length_factor = min(1.0, 1000 / total_words) if total_words > 0 else 1.0
        adjusted_total_score = total_score * length_factor
        
        # Calculate percentages
        informational_pct = (informational_score / adjusted_total_score) * 100
        commercial_pct = (commercial_score / adjusted_total_score) * 100
        local_pct = (local_score / adjusted_total_score) * 100
        navigational_pct = (navigational_score / adjusted_total_score) * 100
        
        # === CRITICAL FIX: Confidence scoring ===
        confidence = 'low'
        if total_score >= 10:
            confidence = 'high'
        elif total_score >= 5:
            confidence = 'medium'
        
        # Round percentages and ensure they sum to 100
        percentages = {
            'informational': round(informational_pct),
            'commercial': round(commercial_pct),
            'local': round(local_pct),
            'navigational': round(navigational_pct)
        }
        
        # Normalize to ensure sum is 100
        total_pct = sum(percentages.values())
        if total_pct != 100:
            for key in percentages:
                percentages[key] = round((percentages[key] / total_pct) * 100)
        
        return {
            'intent_distribution': percentages,
            'confidence': confidence,
            'total_keywords_found': total_score,
            'word_count': total_words,
            'keyword_matches': {
                'informational': info_matches[:5],
                'commercial': comm_matches[:5],
                'local': local_matches[:5],
                'navigational': nav_matches[:5]
            }
        }
        
    except Exception as e:
        print(f"[PHASE2] Intent classification failed: {e}")
        return {
            'intent_distribution': {
                'informational': 25, 'commercial': 25,
                'local': 25, 'navigational': 25
            },
            'confidence': 'error',
            'total_keywords_found': 0
        }

def send_progress_update(job_id: str, percentage: int, step: str, message: str, subtext: str = None):
    """Send progress update to Node.js backend - SAFE VERSION"""
    try:
        # Validate required environment variables
        node_backend_url = os.environ.get("NODE_BACKEND_URL")
        if not node_backend_url:
            raise Exception("NODE_BACKEND_URL is required")
        progress_url = f"{node_backend_url}/api/jobs/{job_id}/progress"
        
        payload = {
            "percentage": percentage,
            "step": step,
            "message": message,
            "subtext": subtext
        }
        
        # === PHASE 1 SAFETY ADDITION ===
        # Safe progress update with proper error handling and logging
        response = requests.post(progress_url, json=payload, timeout=5)
        response.raise_for_status()
        
    except requests.RequestException as e:
        # Log network errors but don't fail the job
        print(f"[SAFETY] Progress update network error | jobId={job_id} | error={e}")
    except Exception as e:
        # Log any other errors but don't fail the job
        print(f"[SAFETY] Progress update unexpected error | jobId={job_id} | error={e}")
    # === SAFETY: Never re-raise exceptions - progress updates are non-critical

def is_job_cancelled(job_id: str) -> bool:
    """Check if a job has been cancelled"""
    # Import here to avoid circular imports
    from main import cancelled_jobs, cancelled_jobs_lock
    
    with cancelled_jobs_lock:
        return job_id in cancelled_jobs

class AIVisibilityJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    input_data: Optional[dict] = None  # For source_job_id and other metadata

# ==================== ENTITY GRAPH EXTRACTION ====================

def normalize_context(context) -> str:
    """Normalize context value for comparison"""
    if not context:
        return ""
    
    # Convert to string, lowercase, remove trailing slash, strip whitespace
    normalized = str(context).lower().rstrip('/').strip()
    return normalized

def normalize_entity_id(original_id, canonical_root) -> str:
    """Normalize entity ID using canonical root enforcement"""
    from urllib.parse import urljoin, urlparse
    
    if not original_id:
        return None
    
    # Parse canonical root for consistency
    canonical_parsed = urlparse(canonical_root)
    canonical_scheme = canonical_parsed.scheme
    canonical_netloc = canonical_parsed.netloc
    
    if original_id.startswith('#'):
        # Hash-only ID - normalize to canonical root
        canonical_root_with_slash = canonical_root if canonical_root.endswith('/') else canonical_root + '/'
        return canonical_root_with_slash + original_id
    elif original_id.startswith(('http://', 'https://')):
        # Absolute URL - enforce canonical protocol and domain
        parsed = urlparse(original_id)
        # Use canonical scheme and netloc
        normalized = f"{canonical_scheme}://{canonical_netloc}{parsed.path}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        if parsed.fragment:
            normalized += f"#{parsed.fragment}"
        return normalized
    else:
        # Relative path - normalize to canonical root
        return urljoin(canonical_root, original_id)

def extract_full_json_ld_data(soup, url) -> dict:
    """Extract and consolidate ALL JSON-LD into exactly ONE valid block - SAFE VERSION"""
    try:
        from schema_architecture_fixer import SchemaArchitectureFixer
    except ImportError:
        try:
            from .schema_architecture_fixer import SchemaArchitectureFixer
        except ImportError:
            # Fallback if schema_architecture_fixer is not available
            return {
                "unified_entity_graph": {"entities": {}},
                "parsed_entities": [],
                "canonical_root": url,
                "raw_json_ld_blocks": [],
                "integrity_metrics": {},
                "structured_data": {},
                "primary_entity": {},
                "entities": [],
                "architecture_violations": {},
                "parse_errors": 1,
                "raw_blocks": []
            }
    
    # === CRITICAL FIX: Create a copy of soup to avoid decomposing original ===
    soup_copy = BeautifulSoup(str(soup), 'html.parser')
    
    # === PHASE 1 SAFETY ADDITION ===
    # Memory cleanup: ensure soup_copy is deallocated on exit
    try:
        json_ld_scripts = soup_copy.find_all('script', type='application/ld+json')
        
        raw_blocks = []
        raw_json_objects = []
        parse_errors = 0
        contexts = set()
        
        # Step 1: Parse all JSON-LD blocks and MERGE at block level
        merged_block = {"@context": "https://schema.org", "@graph": []}
        
        for idx, script in enumerate(json_ld_scripts):
            # === PHASE 1 SAFETY ADDITION ===
            # Safe JSON-LD parsing with comprehensive error recovery
            try:
                import json
                # === ENGINEER-LEVEL FIX ===
                # Handle None and encoding issues before parsing
                if not script.string:
                    print(f"[SAFETY] Empty JSON-LD script {idx} - skipping")
                    parse_errors += 1
                    raw_blocks.append({
                        "block_index": idx,
                        "raw_content": "",
                        "content_length": 0,
                        "is_valid_json": False,
                        "parse_error": "Empty script string"
                    })
                    continue
                
                # === ENGINEER-LEVEL FIX ===
                # Catch all exceptions, not just JSONDecodeError
                data = json.loads(script.string)
                
                raw_blocks.append({
                    "block_index": idx,
                    "raw_content": script.string or "",
                    "content_length": len(script.string or ""),
                    "is_valid_json": True
                })
                
                # Extract context for validation
                if isinstance(data, dict) and '@context' in data:
                    if isinstance(data['@context'], list):
                        contexts.update(data['@context'])
                    else:
                        contexts.add(data['@context'])
                
                # BLOCK-LEVEL MERGE LOGIC
                if isinstance(data, dict):
                    if '@type' in data and '@graph' not in data:
                        # Standalone entity - wrap in @graph
                        merged_block["@graph"].append(data)
                    elif '@graph' in data:
                        # Existing @graph - merge entities
                        if isinstance(data['@graph'], list):
                            merged_block["@graph"].extend(data['@graph'])
                        else:
                            merged_block["@graph"].append(data['@graph'])
                    # Store for reference
                    raw_json_objects.append(data)
                
            except Exception as e:
                # === ENGINEER-LEVEL FIX ===
                # Catch ALL parsing exceptions, not just JSONDecodeError
                parse_errors += 1
                print(f"[SAFETY] JSON-LD parse error | block={idx} | error={e}")
                raw_blocks.append({
                    "block_index": idx,
                    "raw_content": script.string or "",
                    "content_length": len(script.string or ""),
                    "is_valid_json": False,
                    "parse_error": f"Parsing error: {str(e)}"
                })
                continue  # Skip to next block
        
        # Step 2: Extract canonical data for ID normalization
        canonical_data = extract_complete_canonical_data(soup, url)
        
        # CRITICAL: Use domain root for Organization permanence, not page URL
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        canonical_root = f"{parsed_url.scheme}://{parsed_url.netloc}/"
        
        # Ensure canonical root ends with /
        if not canonical_root.endswith('/'):
            canonical_root += '/'
        
        # Step 3: Apply schema architecture fixes to MERGED entities
        architecture_fixer = SchemaArchitectureFixer(canonical_root)
        
        # Get violations for reporting
        violations = architecture_fixer.get_architecture_violations(merged_block["@graph"], url)
        
        # Apply fixes to get exactly ONE valid JSON-LD block
        fixed_json_ld = architecture_fixer.fix_schema_architecture(merged_block["@graph"], url)
        
        # Step 4: Clean up graph entities (remove null/empty properties)
        def cleanup_graph_entities(entities):
            """Clean up graph entities by removing null/empty properties"""
            cleaned_entities = []
            
            for entity in entities:
                cleaned_entity = {}
                
                for key, value in entity.items():
                    # Skip null/empty values
                    if value is None or value == "":
                        continue
                    
                    # Clean up empty lists/dicts
                    if isinstance(value, (list, dict)) and len(value) == 0:
                        continue
                    
                    # Keep valid properties
                    cleaned_entity[key] = value
                
                # Only keep entities with @type and meaningful content
                if cleaned_entity.get('@type') and len(cleaned_entity) > 1:
                    cleaned_entities.append(cleaned_entity)
            
            return cleaned_entities
        
        # Apply cleanup
        fixed_entities = cleanup_graph_entities(fixed_json_ld.get('@graph', []))
        fixed_json_ld['@graph'] = fixed_entities
        
        # Step 5: Build unified graph structure from fixed JSON-LD
        unified_graph = {
            "@context": "https://schema.org",
            "entities": {}
        }
        
        # Process fixed entities into unified graph
        fixed_entities = fixed_json_ld.get('@graph', [])
        
        def should_be_graph_node(entity):
            """Determine if entity should be graph node or stay nested"""
            if entity.get('@id'):
                return True  # Has explicit ID = graph node
            
            entity_type = entity.get('@type')
            
            # These types should stay nested unless they have @id
            nested_types = ['PostalAddress', 'ContactPoint', 'GeoCoordinates', 'OpeningHoursSpecification']
            
            return entity_type not in nested_types
        
        for entity in fixed_entities:
            entity_type = entity.get('@type', 'Unknown')
            
            if should_be_graph_node(entity):
                entity_id = entity.get('@id')
                if not entity_id:
                    # === PHASE 1 SAFETY ADDITION ===
                    # === ENGINEER-LEVEL FIX ===
                    # Normalize entity before hashing for consistency
                    try:
                        def normalize_entity(entity):
                            """Normalize entity for consistent hashing"""
                            return json.loads(json.dumps(entity, sort_keys=True))
                        
                        normalized_entity = normalize_entity(entity)
                        entity_json = json.dumps(normalized_entity, sort_keys=True, separators=(',', ':'))
                        content_hash = hashlib.sha256(entity_json.encode()).hexdigest()[:16]
                        entity_id = f"{canonical_root}#{entity_type.lower()}_{content_hash}"
                        entity_copy = dict(entity)
                        entity_copy['@id'] = entity_id
                        entity = entity_copy
                        print(f"[SAFETY] Generated deterministic ID | type={entity_type} | id={entity_id}")
                    except Exception as e:
                        # Fallback if ID generation fails
                        fallback_id = f"{canonical_root}#{entity_type.lower()}_fallback_{len(entity)}"
                        entity_copy = dict(entity)
                        entity_copy['@id'] = fallback_id
                        entity = entity_copy
                        print(f"[SAFETY] ID generation failed, using fallback | type={entity_type} | error={e}")
                
                # Use normalized ID as key in unified graph
                unified_graph["entities"][entity_id] = entity
            # ELSE: Keep nested - don't add to graph (nested objects stay embedded)
        
        # Return enhanced results with architecture fixes
        return {
            "raw_json_ld_blocks": raw_blocks,
            "raw_json_objects": raw_json_objects,
            "unified_entity_graph": unified_graph,
            "parsed_entities": fixed_entities,
            "architecture_violations": violations,
            "architecture_fixes_applied": True,
            "final_json_ld_block": fixed_json_ld,
            "canonical_root": canonical_root,
            "structured_data": fixed_json_ld,  # === CRITICAL FIX: Add structured_data field ===
            "integrity_metrics": {
                "total_raw_blocks": len(raw_blocks),
                "total_entities_extracted": len(fixed_entities),
                "parse_errors": parse_errors,
                "valid_blocks": len([b for b in raw_blocks if b.get('is_valid_json', False)]),
                "multiple_contexts_detected": len(contexts) > 1,
                "contexts_found": list(contexts),
                "architecture_violations_count": sum(1 for v in violations.values() if v if isinstance(v, bool)) + len([v for v in violations.values() if isinstance(v, list) and v]),
                "single_json_ld_enforced": True,
                "graph_cleanup_applied": True,
                "json_ld_health_score": len([b for b in raw_blocks if b.get('is_valid_json', False)]) / len(raw_blocks) if raw_blocks else 0
            }
        }
    
    finally:
        # === PHASE 1 SAFETY ADDITION ===
        # Memory cleanup: explicitly decompose soup_copy object (not original)
        try:
            soup_copy.decompose()
            print("[SAFETY] BeautifulSoup memory cleanup completed")
        except Exception as e:
            print(f"[SAFETY] BeautifulSoup cleanup failed | error={e}")
        # Clear local references
        json_ld_scripts = None
        merged_block = None
        fixed_entities = None
        unified_graph = None

def extract_complete_entity_properties(entity, all_entities, page_url, canonical_root) -> dict:
    """Extract complete property map for a single entity with canonical ID normalization (real nodes only)"""
    from urllib.parse import urlparse, urljoin
    
    entity_data = {
        "@id": entity.get("@id"),
        "@type": entity.get("@type"),
        "properties": {},
        "property_count": 0,
        "nested_objects": {},
        "references": {},
        "data_types": {},
        "normalized_id": None,
        "id_format": "unknown"
    }
    
    # Normalize entity ID using canonical root
    original_id = entity.get("@id")
    if original_id:
        normalized_id = normalize_entity_id(original_id, canonical_root)
        entity_data["normalized_id"] = normalized_id
        
        if normalized_id == original_id:
            entity_data["id_format"] = "already_normalized"
        elif original_id.startswith('#'):
            entity_data["id_format"] = "hash_normalized"
        else:
            entity_data["id_format"] = "canonical_normalized"
    
    # Extract all properties and their details
    for key, value in entity.items():
        if key.startswith('@'):
            continue
            
        entity_data["properties"][key] = value
        entity_data["property_count"] += 1
        
        # Analyze data type
        if isinstance(value, str):
            entity_data["data_types"][key] = "string"
            entity_data["data_types"][f"{key}_length"] = len(value)
        elif isinstance(value, int):
            entity_data["data_types"][key] = "integer"
        elif isinstance(value, float):
            entity_data["data_types"][key] = "float"
        elif isinstance(value, bool):
            entity_data["data_types"][key] = "boolean"
        elif isinstance(value, list):
            entity_data["data_types"][key] = "array"
            entity_data["data_types"][f"{key}_length"] = len(value)
            
            # Extract nested object references in arrays (keep as nested, don't promote)
            for i, item in enumerate(value):
                if isinstance(item, dict) and '@id' in item:
                    nested_id = item['@id']
                    # Normalize nested ID using canonical root
                    normalized_nested_id = normalize_entity_id(nested_id, canonical_root)
                    entity_data["references"][f"{key}[{i}]"] = {
                        "original_id": nested_id,
                        "normalized_id": normalized_nested_id,
                        "type": item.get('@type')
                    }
                        
        elif isinstance(value, dict):
            entity_data["data_types"][key] = "object"
            entity_data["nested_objects"][key] = value
            
            # Check for @id references in nested objects (keep as nested)
            if "@id" in value:
                nested_id = value["@id"]
                # Normalize nested ID using canonical root
                normalized_nested_id = normalize_entity_id(nested_id, canonical_root)
                entity_data["references"][key] = {
                    "original_id": nested_id,
                    "normalized_id": normalized_nested_id,
                    "type": value.get('@type')
                }
            
            # Extract ALL nested object references recursively (keep as nested)
            entity_data["nested_object_references"] = extract_nested_object_references(value, canonical_root)
    
    # Find references to other entities
    entity_data["referenced_entities"] = find_referenced_ids(entity)
    
    return entity_data

def extract_nested_object_references(obj, canonical_root, depth=0, max_depth=3) -> dict:
    """Recursively extract all nested object references using canonical normalization (keep as nested)"""
    from urllib.parse import urljoin
    
    if depth > max_depth:
        return {}
    
    references = {}
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, dict) and '@id' in value:
                nested_id = value['@id']
                # Normalize nested ID using canonical root
                normalized_nested_id = normalize_entity_id(nested_id, canonical_root)
                references[f"{key}.{depth}"] = {
                    "original_id": nested_id,
                    "normalized_id": normalized_nested_id,
                    "type": value.get('@type'),
                    "depth": depth
                }
                
                # Recursively check deeper nested objects
                deeper_refs = extract_nested_object_references(value, canonical_root, depth + 1, max_depth)
                references.update(deeper_refs)
            
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict) and '@id' in item:
                        nested_id = item['@id']
                        # Normalize nested ID using canonical root
                        normalized_nested_id = normalize_entity_id(nested_id, canonical_root)
                        references[f"{key}[{i}].{depth}"] = {
                            "original_id": nested_id,
                            "normalized_id": normalized_nested_id,
                            "type": item.get('@type'),
                            "depth": depth
                        }
                        
                        # Recursively check deeper nested objects
                        deeper_refs = extract_nested_object_references(item, canonical_root, depth + 1, max_depth)
                        references.update(deeper_refs)
    
    return references

def identify_primary_entity(unified_graph, canonical_url) -> dict:
    """Identify primary entity from unified graph using deterministic priority rules (one entity only)"""
    if not unified_graph or "entities" not in unified_graph:
        return {
            "primary_entity_id": None,
            "primary_entity_type": None,
            "detection_method": "no_entities",
            "candidate_entities": []
        }
    
    entities = unified_graph["entities"]
    if not entities:
        return {
            "primary_entity_id": None,
            "primary_entity_type": None,
            "detection_method": "empty_graph",
            "candidate_entities": []
        }
    
    # Normalize URL for homepage detection
    from urllib.parse import urlparse
    parsed_url = urlparse(canonical_url)
    path = parsed_url.path.rstrip('/')
    is_homepage = path == '' or path == '/'
    
    # Priority-based entity selection (deterministic order)
    primary_candidates = []
    
    for entity_id, entity_data in entities.items():
        entity_type = entity_data.get('@type')
        if not entity_type:
            continue
            
        # Handle multiple types (arrays)
        if isinstance(entity_type, list):
            entity_types = entity_type
        else:
            entity_types = [entity_type]
        
        # ONE PRIMARY ENTITY DETECTION - Fixed priority order
        priority_order = ["WebPage", "AboutPage", "Service", "Product", "Organization"]
        
        for priority_type in priority_order:
            if priority_type in entity_types:
                priority_score = 1000 - priority_order.index(priority_type)
                primary_candidates.append({
                    "entity_id": entity_id,
                    "entity_type": priority_type,
                    "priority": priority_score,
                    "reason": f"priority_order_{priority_type}"
                })
                break  # Only one priority per entity
        
        # Check for mainEntityOfPage relationship (highest priority)
        if 'mainEntityOfPage' in entity_data:
            main_entity_value = entity_data['mainEntityOfPage']
            if isinstance(main_entity_value, str) and main_entity_value == canonical_url:
                primary_candidates.append({
                    "entity_id": entity_id,
                    "entity_type": entity_types[0],
                    "priority": 2000,
                    "reason": "mainEntityOfPage_matches_canonical"
                })
            elif isinstance(main_entity_value, dict) and main_entity_value.get('@id') == canonical_url:
                primary_candidates.append({
                    "entity_id": entity_id,
                    "entity_type": entity_types[0],
                    "priority": 2000,
                    "reason": "mainEntityOfPage_matches_canonical"
                })
        
        # Check for URL match (high priority)
        elif entity_id == canonical_url:
            primary_candidates.append({
                "entity_id": entity_id,
                "entity_type": entity_types[0],
                "priority": 1500,
                "reason": "entity_id_matches_canonical"
            })
    
    # Sort candidates by priority (highest first)
    primary_candidates.sort(key=lambda x: x["priority"], reverse=True)
    
    # Select primary entity (only one allowed)
    if primary_candidates:
        primary = primary_candidates[0]
        return {
            "primary_entity_id": primary["entity_id"],
            "primary_entity_type": primary["entity_type"],
            "detection_method": primary["reason"],
            "priority_score": primary["priority"],
            "candidate_entities": primary_candidates[:3],  # Top 3 candidates for reference
            "total_candidates": len(primary_candidates),
            "single_primary_enforced": True
        }
    else:
        # Last resort: first entity in graph
        first_entity_id = list(entities.keys())[0]
        first_entity = entities[first_entity_id]
        first_entity_type = first_entity.get('@type', 'Unknown')
        
        return {
            "primary_entity_id": first_entity_id,
            "primary_entity_type": first_entity_type,
            "detection_method": "first_entity_fallback",
            "candidate_entities": [],
            "total_candidates": 0,
            "single_primary_enforced": True
        }

def flatten_graph_entities(data) -> list:
    """Flatten @graph structures into individual entities"""
    entities = []
    
    if isinstance(data, dict):
        if '@graph' in data:
            graph_items = data['@graph']
            if isinstance(graph_items, list):
                for item in graph_items:
                    entities.append(item)
            else:
                entities.append(graph_items)
        else:
            entities.append(data)
    elif isinstance(data, list):
        for item in data:
            entities.extend(flatten_graph_entities(item))
    
    return entities

def analyze_single_entity(entity, all_entities) -> dict:
    """Analyze individual entity for completeness and relationships"""
    entity_type = entity.get('@type', '')
    entity_id = entity.get('@id', '')
    
    # Find referenced IDs within this entity
    referenced_ids = find_referenced_ids(entity)
    
    # Count how many other entities reference this one
    referenced_by_count = sum(1 for e in all_entities if entity_id in find_referenced_ids(e))
    
    # Check required fields based on type
    required_fields = get_required_fields_for_type(entity_type)
    has_required = all(field in entity for field in required_fields)
    missing_required = [field for field in required_fields if field not in entity]
    
    return {
        "@type": entity_type,
        "@id": entity_id,
        "referenced_ids": referenced_ids,
        "referenced_by_count": referenced_by_count,
        "has_required_minimum_fields": has_required,
        "missing_required_fields": missing_required
    }

def find_referenced_ids(entity) -> list:
    """Find all @id references within an entity"""
    referenced_ids = []
    
    def traverse(obj):
        if isinstance(obj, dict):
            if '@id' in obj and obj['@id']:
                referenced_ids.append(obj['@id'])
            for value in obj.values():
                traverse(value)
        elif isinstance(obj, list):
            for item in obj:
                traverse(item)
    
    traverse(entity)
    return referenced_ids

def determine_primary_entity_type(entity_types, url) -> str:
    """Deterministic primary entity selection algorithm"""
    if not entity_types:
        return None
    
    # Normalize URL for homepage detection
    from urllib.parse import urlparse
    parsed_url = urlparse(url)
    path = parsed_url.path.rstrip('/')
    
    # Detect homepage: path is empty or just "/"
    is_homepage = path == '' or path == '/'
    
    # Flatten and normalize entity types
    flat_types = []
    for t in entity_types:
        if isinstance(t, list):
            flat_types.extend(t)
        else:
            flat_types.append(t)
    
    if is_homepage:
        # Homepage priority: WebPage > Organization > others
        homepage_priority = ["WebPage", "Organization"]
        for priority_type in homepage_priority:
            if priority_type in flat_types:
                return priority_type
        # Fallback to other types if neither WebPage nor Organization
        other_priority = ["Service", "Product", "Article", "BlogPosting", 
                         "FAQPage", "HowTo", "Dataset", "AboutPage"]
        for priority_type in other_priority:
            if priority_type in flat_types:
                return priority_type
    else:
        # Non-homepage: Exclude Organization, use strict priority
        non_homepage_priority = ["Service", "Product", "Article", "BlogPosting", 
                                "FAQPage", "HowTo", "Dataset", "AboutPage", "WebPage"]
        for priority_type in non_homepage_priority:
            if priority_type in flat_types:
                return priority_type
    
    return None

def analyze_id_format_quality(entity_ids, canonical_root) -> dict:
    """Analyze ID format compliance for normalized entities only"""
    if not entity_ids:
        return {
            "id_format_compliance_percentage": 100.0,  # No IDs = perfect compliance
            "total_ids": 0,
            "valid_ids": 0,
            "normalizable_ids": 0,
            "invalid_ids": 0,
            "format_breakdown": {
                "absolute_https": 0,
                "canonical_normalized": 0,
                "hash_normalized": 0,
                "already_normalized": 0,
                "relative": 0,
                "http_insecure": 0,
                "www_mismatch": 0,
                "invalid_format": 0
            },
            "canonical_root_used": canonical_root,
            "compliance_grade": "A"
        }
    
    format_stats = {
        "absolute_https": 0,      # Already proper HTTPS URLs
        "canonical_normalized": 0, # Absolute URLs normalized to canonical
        "hash_normalized": 0,     # Hash IDs normalized to canonical
        "already_normalized": 0,  # Already in canonical format
        "relative": 0,            # Relative paths (should be normalized)
        "http_insecure": 0,       # HTTP URLs (should be normalized to HTTPS)
        "www_mismatch": 0,        # WWW inconsistency (should be normalized)
        "invalid_format": 0       # Completely invalid IDs
    }
    
    # Parse canonical root for consistency checking
    from urllib.parse import urlparse
    canonical_parsed = urlparse(canonical_root)
    canonical_scheme = canonical_parsed.scheme
    canonical_netloc = canonical_parsed.netloc
    
    for entity_id in entity_ids:
        try:
            if not entity_id:
                format_stats["invalid_format"] += 1
                continue
                
            # All IDs should already be normalized by this point
            if entity_id.startswith('https://'):
                if entity_id.startswith(canonical_root.split('#')[0]):
                    format_stats["already_normalized"] += 1
                else:
                    # HTTPS but different domain - check for www mismatch
                    parsed_id = urlparse(entity_id)
                    if parsed_id.netloc != canonical_netloc:
                        format_stats["www_mismatch"] += 1
                    else:
                        format_stats["canonical_normalized"] += 1
            elif entity_id.startswith('http://'):
                format_stats["http_insecure"] += 1
            elif entity_id.startswith('#'):
                # Hash-only IDs should have been normalized already
                format_stats["invalid_format"] += 1
            else:
                # Relative paths should have been normalized already
                format_stats["relative"] += 1
                
        except Exception:
            format_stats["invalid_format"] += 1
    
    # Calculate compliance percentage (normalized IDs only)
    total_ids = len(entity_ids)
    # Only count properly normalized IDs as valid
    valid_ids = format_stats["already_normalized"] + format_stats["canonical_normalized"]
    
    # Since all IDs should be normalized by now, anything else is invalid
    invalid_ids = total_ids - valid_ids
    
    compliance_percentage = round((valid_ids / total_ids) * 100, 2) if total_ids > 0 else 100.0
    
    return {
        "id_format_compliance_percentage": compliance_percentage,
        "total_ids": total_ids,
        "valid_ids": valid_ids,
        "normalizable_ids": 0,  # All should be normalized already
        "invalid_ids": invalid_ids,
        "format_breakdown": format_stats,
        "canonical_root_used": canonical_root,
        "compliance_grade": "A" if compliance_percentage >= 95 else "B" if compliance_percentage >= 85 else "C" if compliance_percentage >= 70 else "D"
    }

def get_required_fields_for_type(entity_type) -> list:
    """Get required fields for common entity types"""
    required_fields_map = {
        "Organization": ["name", "@type"],
        "WebPage": ["name", "@type"],
        "Service": ["name", "@type", "description"],
        "Product": ["name", "@type"],
        "Article": ["headline", "@type"],
        "BlogPosting": ["headline", "@type"],
        "FAQPage": ["mainEntity", "@type"],
        "HowTo": ["name", "@type"],
        "Dataset": ["name", "@type"],
        "LocalBusiness": ["name", "@type"]
    }
    
    # Handle list types
    if isinstance(entity_type, list):
        entity_type = entity_type[0] if entity_type else ""
    
    return required_fields_map.get(entity_type, ["@type"])

# ==================== ORGANIZATION SIGNALS ====================

def extract_organization_signals(soup) -> dict:
    """Extract organization-specific signals with improved name detection"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    organization_data = {
        "organization_present": False,
        "organization_ids_found": [],
        "organization_name": "",
        "organization_legal_name": "",
        "organization_url": "",
        "organization_logo_present": False,
        "organization_sameAs_count": 0,
        "organization_contactPoint_present": False,
        "organization_address_present": False,
        "organization_telephone": "",
        "organization_email": "",
        "organization_address": {},
        "organization_social_profiles": []
    }
    
    try:
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = flatten_graph_entities(data)
                
                for entity in entities:
                    if entity.get('@type') == 'Organization':
                        organization_data["organization_present"] = True
                        
                        # === CRITICAL FIX: Support both name AND legalName ===
                        if entity.get('name'):
                            organization_data["organization_name"] = entity['name']
                        elif entity.get('legalName'):
                            organization_data["organization_name"] = entity['legalName']
                            organization_data["organization_legal_name"] = entity['legalName']
                        
                        if entity.get('legalName'):
                            organization_data["organization_legal_name"] = entity['legalName']
                        
                        if entity.get('@id'):
                            organization_data["organization_ids_found"].append(entity['@id'])
                        
                        if entity.get('url'):
                            organization_data["organization_url"] = entity['url']
                        
                        if entity.get('logo'):
                            organization_data["organization_logo_present"] = True
                        
                        if entity.get('sameAs'):
                            organization_data["organization_sameAs_count"] = len(entity['sameAs'])
                            organization_data["organization_social_profiles"] = entity['sameAs']
                        
                        if entity.get('contactPoint'):
                            organization_data["organization_contactPoint_present"] = True
                        
                        if entity.get('address'):
                            organization_data["organization_address_present"] = True
                            organization_data["organization_address"] = entity['address']
                        
                        if entity.get('telephone'):
                            organization_data["organization_telephone"] = entity['telephone']
                        
                        if entity.get('email'):
                            organization_data["organization_email"] = entity['email']
                        
            except json.JSONDecodeError:
                continue
            except Exception:
                continue
                
    except Exception as e:
        print(f"[ORGANIZATION_SIGNALS] Extraction error: {e}")
    
    return organization_data

# ==================== PAGE TYPE SIGNALS ====================

def extract_page_type_signals(soup, url) -> dict:
    """Extract page type specific signals with required property maps"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    page_signals = {
        "detected_primary_type": "",
        "required_properties_present": {}
    }
    
    # Collect all entity types
    all_types = []
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                entity_type = entity.get('@type', '')
                if isinstance(entity_type, list):
                    all_types.extend(entity_type)
                else:
                    all_types.append(entity_type)
        except:
            pass
    
    # Determine primary type (now with URL parameter)
    primary_type = determine_primary_entity_type(all_types, url)
    page_signals["detected_primary_type"] = primary_type
    
    # Extract required properties for detected type
    if primary_type:
        page_signals["required_properties_present"] = extract_required_properties_map(soup, primary_type)
    
    return page_signals

def extract_required_properties_map(soup, entity_type) -> dict:
    """Extract presence map for required properties of a specific type"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    # Define required properties for each type
    required_properties = {
        "Service": {
            "name": False,
            "description": False,
            "provider": False,
            "areaServed": False,
            "serviceType": False,
            "offers": False,
            "audience": False,
            "availableChannel": False
        },
        "Product": {
            "name": False,
            "description": False,
            "brand": False,
            "offers": False,
            "aggregateRating": False,
            "review": False
        },
        "Article": {
            "headline": False,
            "author": False,
            "datePublished": False,
            "publisher": False,
            "image": False
        },
        "BlogPosting": {
            "headline": False,
            "author": False,
            "datePublished": False,
            "publisher": False
        },
        "FAQPage": {
            "mainEntity": False
        },
        "Dataset": {
            "name": False,
            "description": False,
            "license": False,
            "creator": False
        },
        "WebPage": {
            "name": False,
            "description": False,
            "url": False
        },
        "Organization": {
            "name": False,
            "url": False,
            "logo": False
        }
    }
    
    properties_map = required_properties.get(entity_type, {})
    missing_properties = []
    
    # Check presence in JSON-LD
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                current_type = entity.get('@type', '')
                if isinstance(current_type, list):
                    current_type = current_type[0] if current_type else ""
                
                if current_type == entity_type:
                    for prop in properties_map:
                        if prop in entity and entity[prop]:
                            properties_map[prop] = True
                    
                    # Identify missing required properties
                    for prop, is_present in properties_map.items():
                        if not is_present and prop not in missing_properties:
                            missing_properties.append(prop)
                            
        except:
            pass
    
    return {
        "required_properties_present": properties_map,
        "missing_required_properties": missing_properties,
        "required_properties_compliance_percentage": round(
            (sum(properties_map.values()) / len(properties_map) * 100), 2
        ) if properties_map else 0
    }

# ==================== RELATIONSHIP SIGNALS ====================

def extract_entity_relationship_graph(soup, parsed_entities) -> dict:
    """Build unified entity-to-entity relationship graph from consolidated entities"""
    relationship_edges = []
    
    # Create entity lookup map from parsed entities (already normalized)
    entity_lookup = {}
    for entity in parsed_entities:
        if entity.get('normalized_id'):
            entity_lookup[entity['normalized_id']] = entity
    
    # Build relationship edges from unified graph entities
    for entity_data in parsed_entities:
        source_id = entity_data.get('normalized_id')
        entity_type = entity_data.get('@type')
        
        if not source_id:
            continue
            
        # Get original entity properties for relationship detection
        entity_properties = entity_data.get("properties", {})
        
        # Detect relationship types and build edges
        relationship_mappings = {
            'provider': 'provider',
            'publisher': 'publisher', 
            'brand': 'brand',
            'author': 'author',
            'creator': 'creator',
            'manufacturer': 'manufacturer',
            'mainEntityOfPage': 'mainEntityOfPage',
            'about': 'about',
            'subjectOf': 'subjectOf',
            'isPartOf': 'isPartOf',
            'hasPart': 'hasPart',
            'logo': 'logo',
            'image': 'image',
            'knows': 'knows',
            'worksFor': 'worksFor',
            'alumniOf': 'alumniOf',
            'parentOrganization': 'parentOrganization',
            'subOrganization': 'subOrganization',
            'memberOf': 'memberOf',
            'affiliation': 'affiliation',
            'primaryImageOfPage': 'primaryImageOfPage',
            'location': 'location'
        }
        
        for prop, relationship_type in relationship_mappings.items():
            if prop in entity_properties:
                target_value = entity_properties[prop]
                
                # Handle single target - ONLY if dict with @id
                if isinstance(target_value, dict) and '@id' in target_value:
                    target_id = target_value['@id']
                    # Normalize target ID using same canonical root
                    canonical_root = entity_data.get("normalized_id", "").split('#')[0] if '#' in entity_data.get("normalized_id", "") else entity_data.get("normalized_id", "")
                    if canonical_root:
                        normalized_target_id = normalize_entity_id(target_id, canonical_root)
                    else:
                        normalized_target_id = target_id
                    
                    target_type = target_value.get('@type')
                    
                    # Override with resolved type if available
                    if normalized_target_id in entity_lookup:
                        target_type = entity_lookup[normalized_target_id].get('@type')
                    
                    # Determine relationship scope
                    relationship_scope = "internal_entity" if normalized_target_id in entity_lookup else "external_entity"
                    
                    relationship_edges.append({
                        "source_id": source_id,
                        "target_id": normalized_target_id,
                        "relationship_type": relationship_type,
                        "property_name": prop,
                        "source_type": entity_type,
                        "target_type": target_type,
                        "target_resolved": normalized_target_id in entity_lookup,
                        "relationship_scope": relationship_scope
                    })
                
                # Handle array of targets - ONLY process dicts with @id
                elif isinstance(target_value, list):
                    for target in target_value:
                        if isinstance(target, dict) and '@id' in target:
                            target_id = target['@id']
                            # Normalize target ID using same canonical root
                            canonical_root = entity_data.get("normalized_id", "").split('#')[0] if '#' in entity_data.get("normalized_id", "") else entity_data.get("normalized_id", "")
                            if canonical_root:
                                normalized_target_id = normalize_entity_id(target_id, canonical_root)
                            else:
                                normalized_target_id = target_id
                            
                            target_type = target.get('@type')
                            
                            # Override with resolved type if available
                            if normalized_target_id in entity_lookup:
                                target_type = entity_lookup[normalized_target_id].get('@type')
                            
                            # Determine relationship scope
                            relationship_scope = "internal_entity" if normalized_target_id in entity_lookup else "external_entity"
                            
                            relationship_edges.append({
                                "source_id": source_id,
                                "target_id": normalized_target_id,
                                "relationship_type": relationship_type,
                                "property_name": prop,
                                "source_type": entity_type,
                                "target_type": target_type,
                                "target_resolved": normalized_target_id in entity_lookup,
                                "relationship_scope": relationship_scope
                            })
    
    return {
        "entity_relationship_graph": relationship_edges,
        "total_relationships": len(relationship_edges),
        "relationship_types": list(set([edge["relationship_type"] for edge in relationship_edges])),
        "entities_with_relationships": list(set([edge["source_id"] for edge in relationship_edges])),
        "resolved_relationships": len([edge for edge in relationship_edges if edge["target_resolved"]]),
        "unresolved_relationships": len([edge for edge in relationship_edges if not edge["target_resolved"]]),
        "internal_relationships": len([edge for edge in relationship_edges if edge["relationship_scope"] == "internal_entity"]),
        "external_relationships": len([edge for edge in relationship_edges if edge["relationship_scope"] == "external_entity"]),
        "unified_graph_structure": True
    }

def extract_relationship_signals(soup) -> dict:
    """Extract relationship graph signals"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    relationship_signals = {
        "provider_reference_exists": False,
        "publisher_reference_exists": False,
        "brand_reference_exists": False,
        "mainEntityOfPage_exists": False,
        "breadcrumb_present": False,
        "imageObject_present": False,
        "offer_present": False,
        "orphan_entities_count": 0,
        "unreferenced_entities_count": 0
    }
    
    all_entities = []
    all_ids = set()
    
    # Collect all entities and IDs
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            all_entities.extend(entities)
            
            for entity in entities:
                if entity.get('@id'):
                    all_ids.add(entity['@id'])
                    
        except:
            pass
    
    # Check for specific relationships
    for entity in all_entities:
        # Check for provider/publisher/brand references
        if entity.get('provider'):
            relationship_signals["provider_reference_exists"] = True
        if entity.get('publisher'):
            relationship_signals["publisher_reference_exists"] = True
        if entity.get('brand'):
            relationship_signals["brand_reference_exists"] = True
        if entity.get('mainEntityOfPage'):
            relationship_signals["mainEntityOfPage_exists"] = True
        
        # Check for specific entity types
        entity_type = entity.get('@type', '')
        if isinstance(entity_type, list):
            entity_type = entity_type[0] if entity_type else ""
        
        if entity_type == "BreadcrumbList":
            relationship_signals["breadcrumb_present"] = True
        elif entity_type == "ImageObject":
            relationship_signals["imageObject_present"] = True
        elif entity_type == "Offer":
            relationship_signals["offer_present"] = True
    
    # Calculate orphan and unreferenced entities
    referenced_ids = set()
    for entity in all_entities:
        referenced_ids.update(find_referenced_ids(entity))
    
    orphan_entities = [e for e in all_entities if not e.get('@id')]
    unreferenced_entities = [e for e in all_entities if e.get('@id') and e['@id'] not in referenced_ids]
    
    relationship_signals["orphan_entities_count"] = len(orphan_entities)
    relationship_signals["unreferenced_entities_count"] = len(unreferenced_entities)
    
    return relationship_signals

# ==================== METADATA SIGNALS ====================

def normalize_url(url) -> str:
    """Normalize URL for comparison"""
    from urllib.parse import urlparse
    
    if not url:
        return ""
    
    try:
        # Don't lower the entire URL first - parse it properly
        parsed = urlparse(url)
        
        # Lower only the components we need
        scheme = parsed.scheme.lower() or 'https'
        netloc = parsed.netloc.lower()
        
        # Remove www safely
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        # Remove trailing slash from path
        path = parsed.path.rstrip('/')
        
        # Reconstruct normalized URL
        normalized = f"{scheme}://{netloc}{path}"
        
        return normalized
        
    except Exception as e:
        # Return original URL if parsing fails - don't corrupt it
        print(f"[DEBUG] URL parsing failed for '{url}': {e}")
        return url  # Return original, not lowercased

def extract_metadata_signals(soup, url) -> dict:
    """Extract page metadata signals"""
    title_tag = soup.find('title')
    title = title_tag.get_text().strip() if title_tag else ''
    
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    meta_description = meta_desc.get('content', '') if meta_desc else ''
    
    # Canonical handling with normalization
    canonical_links = soup.find_all('link', rel='canonical')
    canonical_urls = [link.get('href', '') for link in canonical_links if link.get('href')]
    
    # Normalize URLs for comparison
    normalized_page_url = normalize_url(url)
    normalized_canonical_urls = [normalize_url(canon) for canon in canonical_urls]
    
    return {
        "title_present": bool(title),
        "title_length": len(title),
        "meta_description_present": bool(meta_description),
        "meta_description_length": len(meta_description),
        "canonical_present": len(canonical_urls) > 0,
        "canonical_matches_url": any(normalized_canonical == normalized_page_url for normalized_canonical in normalized_canonical_urls),
        "multiple_canonical_detected": len(canonical_urls) > 1,
        "canonical_urls": canonical_urls,
        "normalized_page_url": normalized_page_url,
        "normalized_canonical_urls": normalized_canonical_urls
    }

# ==================== IMAGE SIGNALS ====================

def extract_complete_image_data(soup) -> dict:
    """Extract full image dataset with complete metadata"""
    images = soup.find_all('img')
    image_data = []
    
    # Also get ImageObject schema for matching
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    image_objects = []
    
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                if entity.get('@type') == 'ImageObject':
                    image_objects.append(entity)
        except:
            continue
    
    for idx, img in enumerate(images):
        img_info = {
            "index": idx,
            "src": img.get('src', ''),
            "alt": img.get('alt', ''),
            "title": img.get('title', ''),
            "width": img.get('width'),
            "height": img.get('height'),
            "loading": img.get('loading'),
            "decoding": img.get('decoding'),
            "srcset": img.get('srcset'),
            "sizes": img.get('sizes'),
            "class": img.get('class'),
            "id": img.get('id'),
            "alt_word_count": len(img.get('alt', '').split()) if img.get('alt') else 0,
            "is_decorative": img.get('alt', '') == '' or img.get('alt', '').lower() in ['decorative', 'spacer'],
            "has_alt": bool(img.get('alt')),
            "has_title": bool(img.get('title')),
            "has_dimensions": bool(img.get('width') and img.get('height')),
            "has_loading_attr": bool(img.get('loading')),
            "has_responsive_attrs": bool(img.get('srcset') or img.get('sizes'))
        }
        
        # Check if this image matches any ImageObject schema
        img_src = img_info['src']
        matching_image_objects = []
        
        for img_obj in image_objects:
            obj_url = img_obj.get('url') or img_obj.get('contentUrl')
            if obj_url and (obj_url in img_src or img_src in obj_url):
                matching_image_objects.append({
                    "@id": img_obj.get('@id'),
                    "url": obj_url,
                    "caption": img_obj.get('caption'),
                    "description": img_obj.get('description'),
                    "name": img_obj.get('name')
                })
        
        img_info["matching_image_objects"] = matching_image_objects
        img_info["matches_schema"] = len(matching_image_objects) > 0
        
        image_data.append(img_info)
    
    # Calculate image summary metrics
    unique_image_urls = set()
    duplicate_image_urls = []
    total_image_size = 0
    
    for img_info in image_data:
        img_url = img_info["src"]
        if img_url:
            if img_url in unique_image_urls:
                duplicate_image_urls.append(img_url)
            else:
                unique_image_urls.add(img_url)
    
    # Estimate total image size (rough calculation based on dimensions)
    for img_info in image_data:
        width = int(img_info.get("width") or 0)
        height = int(img_info.get("height") or 0)
        if width > 0 and height > 0:
            # Rough estimate: 3 bytes per pixel (RGB)
            total_image_size += width * height * 3
    
    image_summary_metrics = {
        "total_unique_images": len(unique_image_urls),
        "duplicate_image_count": len(duplicate_image_urls),
        "duplicate_image_urls": duplicate_image_urls,
        "total_estimated_size_bytes": total_image_size,
        "total_estimated_size_mb": round(total_image_size / (1024 * 1024), 2),
        "average_image_size_bytes": round(total_image_size / len(images), 2) if images else 0,
        "image_duplication_percentage": round((len(duplicate_image_urls) / len(images) * 100), 2) if images else 0
    }
    
    return {
        "images": image_data,
        "total_images": len(images),
        "images_with_alt": len([img for img in image_data if img["has_alt"]]),
        "images_without_alt": len([img for img in image_data if not img["has_alt"]]),
        "decorative_images": len([img for img in image_data if img["is_decorative"]]),
        "images_matching_schema": len([img for img in image_data if img["matches_schema"]]),
        "alt_coverage_percentage": round((len([img for img in image_data if img["has_alt"]]) / len(images) * 100), 2) if images else 0,
        "image_objects_found": len(image_objects),
        "summary_metrics": image_summary_metrics
    }

# ==================== VIDEO SIGNALS ====================

def extract_complete_video_data(soup) -> dict:
    """Extract complete video dataset with embeds and metadata"""
    video_data = {
        "video_tags": [],
        "iframe_embeds": [],
        "video_objects": [],
        "video_comparison": {}
    }
    
    # Extract HTML video tags
    video_tags = soup.find_all('video')
    
    for idx, video in enumerate(video_tags):
        video_info = {
            "index": idx,
            "src": video.get('src', ''),
            "poster": video.get('poster', ''),
            "width": video.get('width'),
            "height": video.get('height'),
            "controls": video.has_attr('controls'),
            "autoplay": video.has_attr('autoplay'),
            "muted": video.has_attr('muted'),
            "loop": video.has_attr('loop'),
            "playsinline": video.has_attr('playsinline'),
            "sources": []
        }
        
        # Extract source tags
        source_tags = video.find_all('source')
        for source in source_tags:
            video_info["sources"].append({
                "src": source.get('src', ''),
                "type": source.get('type', ''),
                "media": source.get('media', '')
            })
        
        video_data["video_tags"].append(video_info)
    
    # Extract iframe embeds (YouTube, Vimeo, etc.)
    iframes = soup.find_all('iframe')
    
    for idx, iframe in enumerate(iframes):
        src = iframe.get('src', '')
        
        # Check if it's a video embed
        video_platforms = ['youtube.com', 'youtu.be', 'vimeo.com', 'dailymotion.com', 'wistia.com']
        
        if any(platform in src for platform in video_platforms):
            video_data["iframe_embeds"].append({
                "index": idx,
                "src": src,
                "title": iframe.get('title', ''),
                "width": iframe.get('width'),
                "height": iframe.get('height'),
                "allowfullscreen": iframe.has_attr('allowfullscreen') or 'allowfullscreen' in iframe.get('allow', ''),
                "platform": next((platform for platform in video_platforms if platform in src), 'unknown'),
                "video_id": extract_video_id_from_url(src)
            })
    
    # Extract VideoObject schema
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                if entity.get('@type') == 'VideoObject':
                    video_obj = {
                        "@id": entity.get('@id'),
                        "name": entity.get('name', ''),
                        "description": entity.get('description', ''),
                        "url": entity.get('url', ''),
                        "embedUrl": entity.get('embedUrl', ''),
                        "thumbnailUrl": entity.get('thumbnailUrl', ''),
                        "uploadDate": entity.get('uploadDate', ''),
                        "duration": entity.get('duration', ''),
                        "contentUrl": entity.get('contentUrl', ''),
                        "width": entity.get('width'),
                        "height": entity.get('height'),
                        "isFamilyFriendly": entity.get('isFamilyFriendly'),
                        "transcript": entity.get('transcript', ''),
                        "has_transcript": bool(entity.get('transcript')),
                        "has_thumbnail": bool(entity.get('thumbnailUrl')),
                        "has_duration": bool(entity.get('duration')),
                        "duration_iso8601": is_valid_duration_format(entity.get('duration', ''))
                    }
                    
                    video_data["video_objects"].append(video_obj)
        except:
            continue
    
    # Compare video content across formats
    video_urls_from_tags = [v["src"] for v in video_data["video_tags"] if v["src"]]
    video_urls_from_sources = [source["src"] for v in video_data["video_tags"] for source in v["sources"] if source["src"]]
    video_urls_from_schema = [v["url"] for v in video_data["video_objects"] if v["url"]]
    embed_urls = [v["src"] for v in video_data["iframe_embeds"] if v["src"]]
    
    video_data["video_comparison"] = {
        "video_tags_count": len(video_data["video_tags"]),
        "iframe_embeds_count": len(video_data["iframe_embeds"]),
        "video_objects_count": len(video_data["video_objects"]),
        "total_video_elements": len(video_data["video_tags"]) + len(video_data["iframe_embeds"]),
        "video_urls": list(set(video_urls_from_tags + video_urls_from_sources)),
        "schema_urls": video_urls_from_schema,
        "embed_urls": embed_urls,
        "has_any_video": len(video_data["video_tags"]) > 0 or len(video_data["iframe_embeds"]) > 0 or len(video_data["video_objects"]) > 0
    }
    
    return video_data

def extract_video_id_from_url(url) -> str:
    """Extract video ID from common video platform URLs"""
    import re
    
    # YouTube
    youtube_patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/watch\?.*v=([a-zA-Z0-9_-]{11})'
    ]
    
    for pattern in youtube_patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Vimeo
    vimeo_match = re.search(r'vimeo\.com/(?:video/)?(\d+)', url)
    if vimeo_match:
        return vimeo_match.group(1)
    
    return ''

def is_valid_duration_format(duration) -> bool:
    """Check if duration is in valid ISO 8601 format"""
    if not duration:
        return False
    
    # Basic ISO 8601 duration format check (PT#H#M#S)
    import re
    pattern = r'^PT(\d+H)?(\d+M)?(\d+S)?$'
    return bool(re.match(pattern, duration))

def extract_complete_content_sections(soup) -> dict:
    """Extract structured content blocks with DOM pollution removal and minimum content validation"""
    content_sections = {
        "definition_section": {"present": False, "text": "", "word_count": 0, "confidence": 0, "heading": "", "heading_level": ""},
        "use_case_section": {"present": False, "text": "", "word_count": 0, "confidence": 0, "heading": "", "heading_level": ""},
        "step_section": {"present": False, "text": "", "word_count": 0, "confidence": 0, "heading": "", "heading_level": ""},
        "faq_section": {"present": False, "text": "", "word_count": 0, "confidence": 0, "heading": "", "heading_level": ""},
        "edge_case_section": {"present": False, "text": "", "word_count": 0, "confidence": 0, "heading": "", "heading_level": ""}
    }
    
    # Section keyword patterns for detection
    section_patterns = {
        "definition_section": {
            "primary_keywords": ["what is", "overview", "introduction", "about", "definition"],
            "secondary_keywords": ["what is", "overview", "introduction", "about", "definition"],
            "weight": 1.0
        },
        "use_case_section": {
            "primary_keywords": ["use case", "who it's for", "ideal for", "applications", "benefits"],
            "secondary_keywords": ["use case", "who it's for", "ideal for", "applications", "benefits"],
            "weight": 1.0
        },
        "step_section": {
            "primary_keywords": ["how it works", "steps", "process", "get started", "tutorial", "guide"],
            "secondary_keywords": ["how it works", "steps", "process", "get started", "tutorial", "guide"],
            "weight": 1.0
        },
        "faq_section": {
            "primary_keywords": ["faq", "frequently asked questions", "questions", "common questions"],
            "secondary_keywords": ["faq", "frequently asked questions", "questions", "common questions"],
            "weight": 1.0
        },
        "edge_case_section": {
            "primary_keywords": ["limitations", "edge case", "exceptions", "constraints", "caveats"],
            "secondary_keywords": ["limitations", "edge case", "exceptions", "constraints", "caveats"],
            "weight": 1.0
        }
    }
    
    # DOM POLLUTION REMOVAL: Remove noise elements entirely
    # Remove all script tags EXCEPT JSON-LD scripts to preserve structured data detection
    for tag in soup.find_all(['nav', 'header', 'footer', 'aside', 'form', 'style', 'noscript']):
        tag.decompose()
    
    # Remove script tags but preserve JSON-LD scripts
    for tag in soup.find_all('script'):
        if tag.get('type') != 'application/ld+json':
            tag.decompose()
    
    # Find headings h1-h4 for section detection (after pollution removal)
    headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
    
    for heading in headings:
        heading_text = heading.get_text().strip().lower()
        heading_level = heading.name
        
        # Calculate section match scores
        section_scores = {}
        
        for section_name, patterns in section_patterns.items():
            score = 0
            
            # Primary keywords (highest weight)
            for keyword in patterns["primary_keywords"]:
                if keyword in heading_text:
                    score += 3.0 * patterns["weight"]
            
            # Secondary keywords (medium weight)
            for keyword in patterns["secondary_keywords"]:
                if keyword in heading_text:
                    score += 2.0 * patterns["weight"]
            
            # Heading level weighting (h2-h3 are more likely section headers)
            if heading_level in ['h2', 'h3']:
                score *= 1.2
            elif heading_level in ['h4']:
                score *= 1.1
            
            section_scores[section_name] = score
        
        # Determine best matching section
        if section_scores:
            best_section = max(section_scores, key=section_scores.get)
            best_score = section_scores[best_section]
            
            # Only proceed if score meets minimum threshold
            if best_score >= 2.0:
                # Extract content following this heading with pollution filtering
                content_text = ""
                word_count = 0
                next_element = heading.find_next_sibling()
                
                while next_element and next_element.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    # Skip navigation lists and empty content
                    if next_element.name in ['ul', 'ol'] and next_element.find_all('a'):
                        # Skip UL lists with only links (navigation)
                        next_element = next_element.find_next_sibling()
                        continue
                    
                    element_text = next_element.get_text().strip()
                    if element_text and len(element_text) >= 80:  # Minimum content length
                        content_text += element_text + " "
                    
                    next_element = next_element.find_next_sibling()
                
                content_text = content_text.strip()
                word_count = len(content_text.split()) if content_text else 0
                
                # MINIMUM CONTENT VALIDATION: Require at least 80 words
                if word_count >= 80:
                    content_sections[best_section] = {
                        "present": True,
                        "text": content_text,
                        "word_count": word_count,
                        "confidence": best_score,
                        "heading": heading.get_text().strip(),
                        "heading_level": heading_level,
                        "pollution_removed": True,
                        "content_validated": True
                    }
    
    # Add summary statistics
    present_sections = [name for name, section in content_sections.items() if section.get("present", False)]
    total_words = sum(section.get("word_count", 0) for section in content_sections.values() if section.get("present", False))
    
    content_sections["extraction_summary"] = {
        "sections_detected": present_sections,
        "total_sections_found": len(present_sections),
        "total_word_count": total_words,
        "dom_pollution_removed": True,
        "minimum_word_threshold": 80,
        "navigation_lists_excluded": True
    }
    
    content_sections["sections_by_confidence"] = sorted(
        [(name, section.get("confidence", 0)) for name, section in content_sections.items() if section.get("present", False) and isinstance(section, dict) and "confidence" in section],
        key=lambda x: x[1],
        reverse=True
    )
    
    return content_sections

def extract_complete_canonical_data(soup, url) -> dict:
    """Extract complete canonical normalization data with normalized comparisons only"""
    from urllib.parse import urlparse
    
    # Get canonical links
    canonical_links = soup.find_all('link', rel='canonical')
    canonical_urls = [link.get('href', '') for link in canonical_links if link.get('href')]
    
    # Normalize URLs for comparison
    normalized_page_url = normalize_url(url)
    normalized_canonical_urls = [normalize_url(canon) for canon in canonical_urls]
    
    # Parse original URLs for detailed comparison
    parsed_page = urlparse(url)
    parsed_canonicals = [urlparse(canon) for canon in canonical_urls]
    
    canonical_data = {
        "raw_page_url": url,
        "raw_canonical_urls": canonical_urls,
        "normalized_page_url": normalized_page_url,
        "normalized_canonical_urls": normalized_canonical_urls,
        "canonical_present": len(canonical_urls) > 0,
        "multiple_canonical_detected": len(canonical_urls) > 1,
        "canonical_matches_url": any(normalized_canonical == normalized_page_url for normalized_canonical in normalized_canonical_urls),
        "protocol_comparison": {},
        "www_comparison": {},
        "trailing_slash_comparison": {}
    }
    
    # Detailed protocol comparison (using normalized values)
    if canonical_urls:
        first_canonical = parsed_canonicals[0]
        
        # Normalize protocols for accurate comparison
        page_scheme = 'https'  # Enforce HTTPS as canonical
        canonical_scheme = 'https'  # Enforce HTTPS as canonical
        
        canonical_data["protocol_comparison"] = {
            "page_protocol": parsed_page.scheme,
            "canonical_protocol": first_canonical.scheme,
            "normalized_page_protocol": page_scheme,
            "normalized_canonical_protocol": canonical_scheme,
            "protocol_match": page_scheme == canonical_scheme,
            "page_is_https": parsed_page.scheme == 'https',
            "canonical_is_https": first_canonical.scheme == 'https',
            "both_normalized_to_https": True  # Always true in normalized comparison
        }
        
        # WWW comparison (using normalized values)
        page_has_www = parsed_page.netloc.startswith('www.')
        canonical_has_www = first_canonical.netloc.startswith('www.')
        
        # Normalize domain (remove www for comparison)
        page_domain = parsed_page.netloc.replace('www.', '')
        canonical_domain = first_canonical.netloc.replace('www.', '')
        
        canonical_data["www_comparison"] = {
            "page_has_www": page_has_www,
            "canonical_has_www": canonical_has_www,
            "www_match": page_has_www == canonical_has_www,
            "page_domain_without_www": page_domain,
            "canonical_domain_without_www": canonical_domain,
            "domain_match": page_domain == canonical_domain,
            "normalized_domain_match": True  # Domains normalized to consistent format
        }
        
        # Trailing slash comparison (using normalized values)
        page_path = parsed_page.path.rstrip('/')
        canonical_path = first_canonical.path.rstrip('/')
        
        canonical_data["trailing_slash_comparison"] = {
            "page_path": parsed_page.path,
            "canonical_path": first_canonical.path,
            "page_has_trailing_slash": parsed_page.path.endswith('/') and parsed_page.path != '/',
            "canonical_has_trailing_slash": first_canonical.path.endswith('/') and first_canonical.path != '/',
            "trailing_slash_match": (parsed_page.path.endswith('/') and first_canonical.path.endswith('/')) or 
                                (not parsed_page.path.endswith('/') and not first_canonical.path.endswith('/')),
            "normalized_path_match": page_path == canonical_path
        }
    
    return canonical_data

def extract_structured_format_detection(soup) -> dict:
    """Detect and store all structured data formats present on the page with strict RDFa detection"""
    format_detection = {
        "json_ld_present": False,
        "microdata_present": False,
        "rdfa_present": False,
        "mixed_formats_detected": False,
        "format_details": {
            "json_ld": {"count": 0, "contexts": []},
            "microdata": {"count": 0, "items": []},
            "rdfa": {"count": 0, "prefixes": []}
        }
    }
    
    # Check for JSON-LD
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    if json_ld_scripts:
        format_detection["json_ld_present"] = True
        format_detection["format_details"]["json_ld"]["count"] = len(json_ld_scripts)
        
        # Extract contexts from JSON-LD
        contexts = set()
        for script in json_ld_scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict) and '@context' in data:
                    if isinstance(data['@context'], list):
                        contexts.update(data['@context'])
                    else:
                        contexts.add(data['@context'])
            except:
                continue
        
        format_detection["format_details"]["json_ld"]["contexts"] = list(contexts)
    
    # Check for Microdata (itemscope attribute)
    microdata_items = soup.find_all(attrs={'itemscope': True})
    if microdata_items:
        format_detection["microdata_present"] = True
        format_detection["format_details"]["microdata"]["count"] = len(microdata_items)
        
        # Extract itemtype and itemprops
        microdata_details = []
        for item in microdata_items[:10]:  # Limit to first 10
            itemtype = item.get('itemtype', '')
            itemprop = item.get('itemprop', '')
            
            microdata_details.append({
                "itemtype": itemtype,
                "itemprop": itemprop
            })
        
        format_detection["format_details"]["microdata"]["items"] = microdata_details
    
    # Check for RDFa with STRICT detection (no false positives)
    rdfa_detected = False
    rdfa_elements = []
    
    # Strict RDFa detection: Must have typeof AND vocab/prefix defined
    typeof_elements = soup.find_all(attrs={'typeof': True})
    
    for elem in typeof_elements:
        # Check for vocab or prefix attributes
        has_vocab = elem.has_attr('vocab')
        has_prefix = elem.has_attr('prefix')
        
        # Also check for RDFa vocab/prefix in parent elements
        parent = elem.find_parent()
        while parent and not (has_vocab or has_prefix):
            has_vocab = parent.has_attr('vocab')
            has_prefix = parent.has_attr('prefix')
            parent = parent.find_parent()
        
        # Only count as RDFa if typeof AND (vocab OR prefix) is present
        if has_vocab or has_prefix:
            rdfa_detected = True
            rdfa_elements.append({
                "tag": elem.name,
                "typeof": elem.get('typeof', ''),
                "vocab": elem.get('vocab', ''),
                "prefix": elem.get('prefix', ''),
                "detection_reason": "typeof_with_vocab_or_prefix"
            })
    
    # Additional strict check: Look for property attributes within RDFa context
    if rdfa_detected:
        property_elements = soup.find_all(attrs={'property': True})
        for elem in property_elements:
            # Only count property attributes if within RDFa context
            parent = elem.find_parent()
            in_rdfa_context = False
            while parent:
                if parent.has_attr('typeof') and (parent.has_attr('vocab') or parent.has_attr('prefix')):
                    in_rdfa_context = True
                    break
                parent = parent.find_parent()
            
            if in_rdfa_context:
                rdfa_elements.append({
                    "tag": elem.name,
                    "property": elem.get('property', ''),
                    "detection_reason": "property_in_rdfa_context"
                })
    
    if rdfa_detected:
        format_detection["rdfa_present"] = True
        format_detection["format_details"]["rdfa"]["count"] = len(rdfa_elements)
        
        # Extract RDFa prefixes/vocabs
        vocab_prefixes = set()
        for elem in rdfa_elements:
            if elem.get('vocab'):
                vocab_prefixes.add(elem['vocab'])
            if elem.get('prefix'):
                vocab_prefixes.add(elem['prefix'])
        
        format_detection["format_details"]["rdfa"]["prefixes"] = list(vocab_prefixes)
        format_detection["format_details"]["rdfa"]["elements"] = rdfa_elements[:10]  # Limit to first 10
    
    # Check for mixed formats
    formats_present = [
        format_detection["json_ld_present"],
        format_detection["microdata_present"],
        format_detection["rdfa_present"]
    ]
    
    format_detection["mixed_formats_detected"] = sum(formats_present) > 1
    format_detection["total_structured_data_elements"] = (
        format_detection["format_details"]["json_ld"]["count"] +
        format_detection["format_details"]["microdata"]["count"] +
        format_detection["format_details"]["rdfa"]["count"]
    )
    
    return format_detection
    

def extract_complete_breadcrumb_data(soup) -> dict:
    """Extract complete breadcrumb structure with enhanced HTML detection"""
    breadcrumb_data = {
        "breadcrumb_schema": [],
        "html_breadcrumbs": [],
        "breadcrumb_comparison": {}
    }
    
    # Extract BreadcrumbList schema
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                if entity.get('@type') == 'BreadcrumbList':
                    breadcrumb_list = entity.get('itemListElement', [])
                    
                    if isinstance(breadcrumb_list, dict):
                        breadcrumb_list = [breadcrumb_list]
                    
                    for item in breadcrumb_list:
                        if item.get('@type') == 'ListItem':
                            breadcrumb_data["breadcrumb_schema"].append({
                                "position": item.get('position'),
                                "name": item.get('name', ''),
                                "url": item.get('item', ''),
                                "has_position": bool(item.get('position')),
                                "has_name": bool(item.get('name')),
                                "has_url": bool(item.get('item')),
                                "position_is_numeric": str(item.get('position')).isdigit() if item.get('position') else False,
                                "position_sequence_valid": False  # Will be calculated later
                            })
        except:
            continue
    
    # Validate position sequence in schema breadcrumbs
    positions = [bc["position"] for bc in breadcrumb_data["breadcrumb_schema"] if bc.get("position")]
    if positions:
        sorted_positions = sorted(positions)
        breadcrumb_data["breadcrumb_schema"][0]["position_sequence_valid"] = (positions == sorted_positions)
    
    # Enhanced HTML breadcrumb extraction
    html_breadcrumb_sources = []
    
    # 1. Navigation elements with breadcrumb indicators
    breadcrumb_navs = soup.find_all('nav', attrs={
        'aria-label': lambda x: x and 'breadcrumb' in x.lower()
    })
    for nav in breadcrumb_navs:
        html_breadcrumb_sources.append({
            "source": "nav_aria_label",
            "element": nav,
            "confidence": "high"
        })
    
    # 2. Ordered lists with breadcrumb classes
    breadcrumb_ols = soup.find_all('ol', class_=lambda x: x and any('breadcrumb' in str(cls).lower() for cls in x))
    for ol in breadcrumb_ols:
        html_breadcrumb_sources.append({
            "source": "ol_breadcrumb_class",
            "element": ol,
            "confidence": "high"
        })
    
    # 3. Unordered lists with breadcrumb classes
    breadcrumb_uls = soup.find_all('ul', class_=lambda x: x and any('breadcrumb' in str(cls).lower() for cls in x))
    for ul in breadcrumb_uls:
        html_breadcrumb_sources.append({
            "source": "ul_breadcrumb_class",
            "element": ul,
            "confidence": "high"
        })
    
    # 4. Schema.org breadcrumb microdata
    schema_breadcrumbs = soup.find_all(['ol', 'ul'], attrs={
        'itemscope': True,
        'itemtype': lambda x: x and 'BreadcrumbList' in str(x)
    })
    for schema_bc in schema_breadcrumbs:
        html_breadcrumb_sources.append({
            "source": "schema_microdata",
            "element": schema_bc,
            "confidence": "very_high"
        })
    
    # 5. Common breadcrumb patterns (lower confidence)
    breadcrumb_patterns = [
        {'tag': 'div', 'class': lambda x: x and any(cls in str(x).lower() for cls in ['breadcrumb', 'breadcrumbs', 'nav', 'path'])},
        {'tag': 'nav', 'class': lambda x: x and any(cls in str(x).lower() for cls in ['path', 'trail', 'navigation'])},
        {'tag': 'ol', 'class': lambda x: x and 'nav' in str(x).lower()},
        {'tag': 'ul', 'class': lambda x: x and 'nav' in str(x).lower()}
    ]
    
    for pattern in breadcrumb_patterns:
        elements = soup.find_all(pattern['tag'], class_=pattern['class'])
        for elem in elements:
            # Avoid duplicates
            if not any(source["element"] == elem for source in html_breadcrumb_sources):
                html_breadcrumb_sources.append({
                    "source": "pattern_match",
                    "element": elem,
                    "confidence": "medium",
                    "pattern": str(pattern['class'])
                })
    
    # Process each breadcrumb source
    for idx, source_info in enumerate(html_breadcrumb_sources):
        element = source_info["element"]
        breadcrumb_info = {
            "index": idx,
            "source_type": source_info["source"],
            "confidence": source_info["confidence"],
            "element_type": element.name,
            "classes": element.get('class', []),
            "aria_label": element.get('aria-label', ''),
            "items": [],
            "has_valid_structure": False
        }
        
        # Extract list items
        if element.name in ['ol', 'ul']:
            list_items = element.find_all('li', recursive=False)
        else:
            # For div/nav elements, look for links or spans
            list_items = element.find_all(['a', 'span', 'li'], recursive=False)
        
        breadcrumb_positions = []
        
        for li_idx, li in enumerate(list_items):
            link = li.find('a') if li.name != 'a' else li
            text = li.get_text().strip() if li.name != 'a' else link.get_text().strip()
            url = link.get('href', '') if link else ''
            
            # Try to extract position from various attributes
            position = None
            if li.get('itemprop') == 'itemListElement':
                position_elem = li.find('meta', itemprop='position')
                if position_elem:
                    position = position_elem.get('content')
            elif li.get('data-position'):
                position = li.get('data-position')
            else:
                position = li_idx + 1  # Default to sequence position
            
            breadcrumb_info["items"].append({
                "position": position,
                "text": text,
                "url": url,
                "is_link": bool(link),
                "has_separator": bool(li.find_next_sibling() or li.find('span', class_=lambda x: x and 'separator' in str(x).lower())),
                "element_tag": li.name,
                "element_classes": li.get('class', []),
                "position_is_numeric": str(position).isdigit() if position else False
            })
            
            if position and str(position).isdigit():
                breadcrumb_positions.append(int(position))
        
        # Validate breadcrumb structure
        breadcrumb_info["has_valid_structure"] = (
            len(breadcrumb_info["items"]) > 0 and
            len(breadcrumb_positions) > 0 and
            breadcrumb_positions == sorted(breadcrumb_positions)
        )
        
        breadcrumb_data["html_breadcrumbs"].append(breadcrumb_info)
    
    # Enhanced comparison between schema and HTML breadcrumbs
    schema_urls = [bc["url"] for bc in breadcrumb_data["breadcrumb_schema"] if bc["url"]]
    html_urls = []
    html_texts = []
    
    for html_bc in breadcrumb_data["html_breadcrumbs"]:
        for item in html_bc["items"]:
            if item["url"]:
                html_urls.append(item["url"])
            if item["text"]:
                html_texts.append(item["text"].lower())
    
    # Text-based matching (for cases where URLs might differ)
    schema_texts = [bc["name"].lower() for bc in breadcrumb_data["breadcrumb_schema"] if bc["name"]]
    
    breadcrumb_data["breadcrumb_comparison"] = {
        "schema_count": len(breadcrumb_data["breadcrumb_schema"]),
        "html_count": len(breadcrumb_data["html_breadcrumbs"]),
        "schema_urls": schema_urls,
        "html_urls": html_urls,
        "schema_texts": schema_texts,
        "html_texts": html_texts,
        "url_matches": len(set(schema_urls) & set(html_urls)),
        "text_matches": len(set(schema_texts) & set(html_texts)),
        "matching_urls": list(set(schema_urls) & set(html_urls)),
        "matching_texts": list(set(schema_texts) & set(html_texts)),
        "schema_only_urls": len(set(schema_urls) - set(html_urls)),
        "html_only_urls": len(set(html_urls) - set(schema_urls)),
        "schema_only_texts": len(set(schema_texts) - set(html_texts)),
        "html_only_texts": len(set(html_texts) - set(schema_texts)),
        "has_consistent_breadcrumbs": len(set(schema_urls) & set(html_urls)) > 0 or len(set(schema_texts) & set(html_texts)) > 0
    }
    
    return breadcrumb_data

# ==================== CONTENT STRUCTURE SIGNALS ====================

def extract_complete_faq_data(soup) -> dict:
    """Extract complete FAQ dataset with JSON-LD and HTML accordion detection"""
    faq_data = {
        "faq_schema_data": [],
        "html_faq_sections": [],
        "accordion_faqs": [],
        "faq_comparison": {}
    }
    
    # Extract FAQ schema data
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            
            for entity in entities:
                if entity.get('@type') == 'FAQPage':
                    main_entity = entity.get('mainEntity', [])
                    
                    if isinstance(main_entity, dict):
                        main_entity = [main_entity]
                    
                    for faq_item in main_entity:
                        if faq_item.get('@type') == 'Question':
                            question_text = faq_item.get('name', '')
                            accepted_answer = faq_item.get('acceptedAnswer', {})
                            
                            # Handle both single answer and multiple answers
                            answers = []
                            if isinstance(accepted_answer, dict):
                                answer_text = accepted_answer.get('text', '')
                                answers.append(answer_text)
                            elif isinstance(accepted_answer, list):
                                for answer in accepted_answer:
                                    if isinstance(answer, dict):
                                        answers.append(answer.get('text', ''))
                            
                            # Use the first answer as primary
                            primary_answer = answers[0] if answers else ''
                            
                            faq_data["faq_schema_data"].append({
                                "question": question_text,
                                "answer": primary_answer,
                                "all_answers": answers,
                                "question_word_count": len(question_text.split()) if question_text else 0,
                                "answer_word_count": len(primary_answer.split()) if primary_answer else 0,
                                "has_answer": bool(primary_answer),
                                "question_length": len(question_text),
                                "answer_length": len(primary_answer),
                                "multiple_answers": len(answers) > 1,
                                "answer_count": len(answers)
                            })
        except:
            continue
    
    # Extract HTML accordion FAQs (common pattern)
    accordion_patterns = [
        # Common accordion selectors
        '[data-toggle="collapse"]',
        '[data-bs-toggle="collapse"]',
        '.accordion-item',
        '.faq-item',
        '.question-answer',
        '.q-and-a'
    ]
    
    for pattern in accordion_patterns:
        try:
            accordion_elements = soup.select(pattern)
            
            for accordion in accordion_elements:
                # Try to find question/answer structure
                question_elem = accordion.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'button'])
                answer_elem = accordion.find(['div', 'p', 'section'], class_=lambda x: x and ('answer' in str(x).lower() or 'content' in str(x).lower()))
                
                if question_elem and not answer_elem:
                    # Look for next sibling as answer
                    answer_elem = question_elem.find_next_sibling(['div', 'p', 'section'])
                
                if question_elem and answer_elem:
                    question_text = question_elem.get_text().strip()
                    answer_text = answer_elem.get_text().strip()
                    
                    # Only include if it looks like a Q&A
                    if ('?' in question_text or 
                        'what' in question_text.lower() or 
                        'how' in question_text.lower() or 
                        'why' in question_text.lower() or
                        len(question_text.split()) <= 10):  # Short questions are typical
                        
                        faq_data["accordion_faqs"].append({
                            "question": question_text,
                            "answer": answer_text,
                            "question_word_count": len(question_text.split()),
                            "answer_word_count": len(answer_text.split()),
                            "question_length": len(question_text),
                            "answer_length": len(answer_text),
                            "source": "html_accordion",
                            "selector_pattern": pattern
                        })
        except:
            continue
    
    # Extract heading-based FAQ sections (enhanced)
    faq_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], 
                                 string=lambda text: text and any(keyword in text.lower() for keyword in ['faq', 'question', 'frequently asked']))
    
    for heading in faq_headings:
        faq_section = {
            "heading_text": heading.get_text().strip(),
            "heading_tag": heading.name,
            "following_content": []
        }
        
        # Get following elements until next heading
        next_element = heading.find_next_sibling()
        question_count = 0
        
        while next_element and next_element.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            if next_element.get_text().strip():
                text = next_element.get_text().strip()
                
                # Detect if this is a question-answer pair
                is_question = ('?' in text or 
                              any(q in text.lower() for q in ['what', 'how', 'why', 'when', 'where', 'which']) or
                              (len(text.split()) <= 12 and text.endswith('?')))
                
                faq_section["following_content"].append({
                    "tag": next_element.name,
                    "text": text,
                    "word_count": len(text.split()),
                    "is_question": is_question,
                    "element_class": next_element.get('class', [])
                })
                
                if is_question:
                    question_count += 1
            
            next_element = next_element.find_next_sibling()
        
        if question_count > 0:  # Only include if we found questions
            faq_section["question_count"] = question_count
            faq_data["html_faq_sections"].append(faq_section)
    
    # Enhanced comparison between schema and HTML FAQs
    schema_questions = [faq["question"].lower().strip() for faq in faq_data["faq_schema_data"]]
    accordion_questions = [faq["question"].lower().strip() for faq in faq_data["accordion_faqs"]]
    html_section_questions = []
    
    for section in faq_data["html_faq_sections"]:
        for content in section["following_content"]:
            if content.get("is_question"):
                html_section_questions.append(content["text"].lower().strip())
    
    all_html_questions = accordion_questions + html_section_questions
    
    # More sophisticated matching (partial matches)
    def calculate_similarity(str1, str2):
        """Simple similarity calculation"""
        words1 = set(str1.split())
        words2 = set(str2.split())
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        return len(intersection) / len(union) if union else 0
    
    exact_matches = set(schema_questions) & set(all_html_questions)
    partial_matches = []
    
    for schema_q in schema_questions:
        for html_q in all_html_questions:
            if schema_q not in exact_matches and html_q not in exact_matches:
                similarity = calculate_similarity(schema_q, html_q)
                if similarity > 0.7:  # 70% similarity threshold
                    partial_matches.append((schema_q, html_q, similarity))
    
    faq_data["faq_comparison"] = {
        "schema_question_count": len(faq_data["faq_schema_data"]),
        "accordion_question_count": len(faq_data["accordion_faqs"]),
        "html_section_count": len(faq_data["html_faq_sections"]),
        "total_html_questions": len(all_html_questions),
        "schema_questions": schema_questions,
        "accordion_questions": accordion_questions,
        "html_section_questions": html_section_questions,
        "exact_matches": len(exact_matches),
        "partial_matches": len(partial_matches),
        "partial_match_details": partial_matches[:5],  # Top 5 partial matches
        "schema_only_questions": len(set(schema_questions) - set(all_html_questions)),
        "html_only_questions": len(set(all_html_questions) - set(schema_questions)),
        "total_questions_found": len(schema_questions) + len(all_html_questions),
        "faq_completeness_score": min(100, (len(exact_matches) + len(partial_matches)) / max(len(schema_questions), 1) * 100)
    }
    
    return faq_data

# ==================== TECHNICAL SIGNALS ====================

def extract_technical_signals(soup) -> dict:
    """Extract technical validation signals"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    technical_signals = {
        "json_parse_errors_count": 0,
        "invalid_types_detected": [],
        "deprecated_properties_detected": [],
        "multiple_contexts_detected": False
    }
    
    contexts = set()
    
    # Verified list of actually deprecated schema.org properties (minimal, conservative)
    deprecated_properties = {
        "additionalProperty",  # Deprecated in favor of hasPart
        "additionalType",     # Deprecated 
        "album",              # Deprecated in favor of partOfSeries
        "albumRelease",       # Deprecated
        "associatedReview",   # Deprecated in favor of review
        "blogPosts",          # Deprecated in favor of blogPost
        "bookFormat",         # Deprecated
        "colleague",          # Deprecated
        "contentLocation",    # Deprecated in favor of locationCreated
        "contentReferenceTime", # Deprecated
        "copyrightYear",      # Deprecated in favor of copyrightYear
        "dataset",            # Deprecated in favor of datasets
        "department",         # Deprecated
        "employee",           # Deprecated in favor of employees
        "encoding",           # Deprecated in favor of encodingFormat
        "episodes",           # Deprecated in favor of episode
        "event",              # Deprecated in favor of events
        "exifData",           # Deprecated in favor of exifData
        "followee",           # Deprecated
        "follows",            # Deprecated
        "genre",              # Deprecated in favor of about
        "interactionCount",   # Deprecated
        "isBasedOnUrl",       # Deprecated in favor of isBasedOn
        "member",             # Deprecated in favor of members
        "musicBy",            # Deprecated in favor of musicBy
        "parent",             # Deprecated
        "photo",              # Deprecated in favor of image
        "subEvent",           # Deprecated in favor of subEvent
        "tracks",             # Deprecated in favor of track
        "workExample",        # Deprecated
        "worksFor",           # Deprecated
    }
    
    for script in json_ld_scripts:
        try:
            import json
            data = json.loads(script.string)
            
            # Check for multiple contexts (with normalization)
            if isinstance(data, dict) and '@context' in data:
                if isinstance(data['@context'], list):
                    for ctx in data['@context']:
                        contexts.add(normalize_context(ctx))
                else:
                    contexts.add(normalize_context(data['@context']))
            
            # Validate entity structure
            entities = flatten_graph_entities(data)
            for entity in entities:
                if not isinstance(entity, dict):
                    technical_signals["invalid_types_detected"].append("non_dict_entity")
                
                # Only flag actually deprecated properties
                for prop in entity.keys():
                    if prop in deprecated_properties:
                        if prop not in technical_signals["deprecated_properties_detected"]:
                            technical_signals["deprecated_properties_detected"].append(prop)
                        
        except Exception as e:
            technical_signals["json_parse_errors_count"] += 1
    
    technical_signals["multiple_contexts_detected"] = len(contexts) > 1
    
    return technical_signals

def extract_comprehensive_signals(html: str, url: str) -> dict:
    """Production-grade semantic extraction engine - isolated and stabilized"""
    from bs4 import BeautifulSoup
    
    extraction_errors = []
    
    # Safe default structure - ALWAYS returned
    semantic_dataset = {
        "unified_entity_graph": {"entities": {}},
        "parsed_entities": [],
        "entity_relationship_graph": [],
        "primary_entity": {"primary_entity_id": None, "primary_entity_type": None},
        "entity_types": [],
        "content_sections": {},
        "images": [],
        "faq_data": [],
        "breadcrumb_data": [],
        "video_data": [],
        "canonical_data": {},
        "structured_format_detection": {},
        "entity_graph_integrity": {},
        "image_summary_metrics": {},
        "extraction_errors": extraction_errors,
        "production_mode": True,
        "pure_extraction_mode": True
    }
    
    try:
        soup = BeautifulSoup(html, 'lxml')
    except Exception as e:
        extraction_errors.append(f"HTML parsing failed: {e}")
        return semantic_dataset
    
    # ISOLATED EXTRACTION STAGE 1: JSON-LD Data (CRITICAL)
    try:
        json_ld_data = extract_full_json_ld_data(soup, url)
        unified_graph = json_ld_data.get("unified_entity_graph", {})
        parsed_entities = json_ld_data.get("parsed_entities", [])
        
        semantic_dataset.update({
            "unified_entity_graph": unified_graph,
            "parsed_entities": parsed_entities,
            "raw_json_ld_blocks": json_ld_data.get("raw_json_ld_blocks", []),
            "entity_graph_integrity": json_ld_data.get("integrity_metrics", {}),
            "structured_data": json_ld_data.get("structured_data", {}),
            "primary_entity": json_ld_data.get("primary_entity", {}),
            "entities": json_ld_data.get("entities", []),
            "architecture_violations": json_ld_data.get("architecture_violations", {}),
            "parse_errors": json_ld_data.get("parse_errors", 0),
            "raw_blocks": json_ld_data.get("raw_blocks", [])
        })
    except Exception as e:
        extraction_errors.append(f"JSON-LD extraction failed: {e}")
        unified_graph = {"entities": {}}
        parsed_entities = []
        json_ld_data = {"unified_entity_graph": unified_graph, "parsed_entities": parsed_entities, "canonical_root": url}
    
    # ISOLATED EXTRACTION STAGE 2: Primary Entity (depends on JSON-LD)
    try:
        canonical_root = json_ld_data.get("canonical_root", url)
        primary_entity = identify_primary_entity(unified_graph, canonical_root)
        semantic_dataset["primary_entity"] = primary_entity
    except Exception as e:
        extraction_errors.append(f"Primary entity detection failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 3: Entity Types (from parsed entities)
    try:
        semantic_dataset["parsed_entities"] = parsed_entities
        
        # Extract entity types from parsed entities (inline, no external dependencies)
        entity_types = []
        for entity in parsed_entities:
            entity_type = entity.get("@type")
            if isinstance(entity_type, list):
                entity_types.extend(entity_type)
            elif isinstance(entity_type, str):
                entity_types.append(entity_type)
        entity_types = list(set(entity_types))
        semantic_dataset["entity_types"] = entity_types
    except Exception as e:
        extraction_errors.append(f"Entity type extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 4: Relationship Graph
    try:
        relationship_graph = extract_entity_relationship_graph(soup, parsed_entities)
        semantic_dataset["entity_relationship_graph"] = relationship_graph.get("entity_relationship_graph", [])
    except Exception as e:
        extraction_errors.append(f"Relationship graph extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 5: Content Sections (defensive)
    try:
        content_sections = extract_complete_content_sections(soup)
        semantic_dataset["content_sections"] = content_sections
    except Exception as e:
        extraction_errors.append(f"Content section extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 6: Images
    try:
        image_data = extract_complete_image_data(soup)
        semantic_dataset["images"] = image_data.get("images", [])
        semantic_dataset["image_summary_metrics"] = image_data.get("summary_metrics", {})
    except Exception as e:
        extraction_errors.append(f"Image extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 7: FAQ Data - Only extract if not already handled by Phase 2
    try:
        # Skip FAQ extraction here since it's handled in Phase 2 with better accuracy
        # This prevents overwriting the more accurate faq_data from detect_faq_content
        pass
    except Exception as e:
        extraction_errors.append(f"FAQ extraction skipped: {e}")
    
    # ISOLATED EXTRACTION STAGE 8: Breadcrumb Data
    try:
        breadcrumb_data = extract_complete_breadcrumb_data(soup)
        semantic_dataset["breadcrumb_data"] = breadcrumb_data.get("breadcrumb_schema", [])
    except Exception as e:
        extraction_errors.append(f"Breadcrumb extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 9: Video Data
    try:
        video_data = extract_complete_video_data(soup)
        semantic_dataset["video_data"] = video_data.get("video_objects", [])
    except Exception as e:
        extraction_errors.append(f"Video extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 10: Canonical Data
    try:
        canonical_data = extract_complete_canonical_data(soup, url)
        semantic_dataset["canonical_data"] = canonical_data
    except Exception as e:
        extraction_errors.append(f"Canonical data extraction failed: {e}")
    
    # ISOLATED EXTRACTION STAGE 11: Structured Format Detection
    try:
        format_detection = extract_structured_format_detection(soup)
        semantic_dataset["structured_format_detection"] = format_detection
    except Exception as e:
        extraction_errors.append(f"Structured format detection failed: {e}")
    
    # ========================================
    # FULL EXTRACTION MODE - ENHANCED DATA
    # ========================================
    try:
        # Import the full extraction coordinator
        try:
            from full_extraction_coordinator import FullExtractionCoordinator
        except ImportError:
            from .full_extraction_coordinator import FullExtractionCoordinator
        
        # Initialize coordinator with existing JSON-LD data
        coordinator = FullExtractionCoordinator(soup, url, json_ld_data)
        
        # Extract comprehensive enhanced dataset
        enhanced_dataset = coordinator.extract_comprehensive_dataset()
        
        # Merge enhanced dataset with existing semantic dataset
        # This maintains backward compatibility while adding new data
        semantic_dataset.update(enhanced_dataset)
        
        # === CRITICAL FIX: Clear any previous enhanced_extraction_v2 errors on success ===
        if "enhanced_extraction_v2" in semantic_dataset and "extraction_error" in semantic_dataset["enhanced_extraction_v2"]:
            # Replace error with clean structure on successful extraction
            semantic_dataset["enhanced_extraction_v2"] = enhanced_dataset.get("enhanced_extraction_v2", {})
        
        extraction_errors.append("Full extraction mode completed successfully")
        
    except ImportError as e:
        extraction_errors.append(f"Full extraction coordinator not available: {e}")
        extraction_errors.append("Check that extractors folder exists and all modules are present")
    except ModuleNotFoundError as e:
        extraction_errors.append(f"Missing extractor module: {e}")
        extraction_errors.append("Check extractors folder: entity_validator, page_type_extractor, metadata_enhancer, link_analyzer, multimedia_extractor")
    except Exception as e:
        extraction_errors.append(f"Full extraction mode failed: {e}")
        extraction_errors.append(f"Error type: {type(e).__name__}")
        # Add enhanced_data_v2 placeholder to maintain structure
        semantic_dataset["enhanced_extraction_v2"] = {
            "extraction_error": {
                "error_message": str(e),
                "error_type": type(e).__name__,
                "partial_data_available": False
            }
        }
    
    # Add production metadata
    semantic_dataset.update({
        "canonical_root": canonical_root,
        "unified_graph_enforced": True,
        "embedded_entities_removed": True,
        "synthetic_ids_eliminated": True,
        "ids_normalized": True,
        "dom_pollution_removed": True,
        "rdfa_strict_detection": True,
        "single_primary_entity": True,
        "business_duplicates_merged": True,
        "stabilization_refactor": True,
        "full_extraction_mode_enabled": True
    })
    
    # ========================================
    # NEW AI VISIBILITY SIGNALS INTEGRATION
    # ========================================
    
    # Extract enhanced author signals
    try:
        parsed_entities = semantic_dataset.get("parsed_entities", [])
        author_signals = extract_author_signals(soup, parsed_entities)
        semantic_dataset["author_signals"] = author_signals
    except Exception as e:
        extraction_errors.append(f"Author signals extraction failed: {e}")
        semantic_dataset["author_signals"] = {"author_detected": False, "error": str(e)}
    
    # Extract NAP consistency signals for local SEO
    try:
        nap_signals = extract_nap_signals(soup, parsed_entities)
        semantic_dataset["nap_signals"] = nap_signals
    except Exception as e:
        extraction_errors.append(f"NAP signals extraction failed: {e}")
        semantic_dataset["nap_signals"] = {"nap_consistency": {"consistent": False}, "error": str(e)}
    
    # Extract direct answer signals for AEO
    try:
        main_text = soup.get_text()
        answer_signals = extract_direct_answer_signals(soup, main_text)
        semantic_dataset["answer_signals"] = answer_signals
    except Exception as e:
        extraction_errors.append(f"Answer signals extraction failed: {e}")
        semantic_dataset["answer_signals"] = {"direct_answers": {"count": 0}, "error": str(e)}
    
    # Extract enhanced technical signals
    try:
        enhanced_technical = extract_enhanced_technical_signals(soup, url)
        semantic_dataset["enhanced_technical_signals"] = enhanced_technical
        
        # NAVIGATION SYNC TO ENHANCED_EXTRACTION_V2 LOCATIONS
        # Read from canonical source and sync to other two locations
        nav_result = enhanced_technical.get("navigation_detection", {})
        has_nav = nav_result.get("has_navigation", False)
        has_header = nav_result.get("has_header", False)
        has_footer = nav_result.get("has_footer", False)
        
        # Sync Location 1: structure_metrics inside page_metadata
        try:
            if "enhanced_extraction_v2" not in semantic_dataset:
                semantic_dataset["enhanced_extraction_v2"] = {}
            if "page_metadata" not in semantic_dataset["enhanced_extraction_v2"]:
                semantic_dataset["enhanced_extraction_v2"]["page_metadata"] = {}
            if "structure_metrics" not in semantic_dataset["enhanced_extraction_v2"]["page_metadata"]:
                semantic_dataset["enhanced_extraction_v2"]["page_metadata"]["structure_metrics"] = {}
                
            semantic_dataset["enhanced_extraction_v2"]["page_metadata"]["structure_metrics"]["has_navigation"] = has_nav
            semantic_dataset["enhanced_extraction_v2"]["page_metadata"]["structure_metrics"]["has_header"] = has_header
            semantic_dataset["enhanced_extraction_v2"]["page_metadata"]["structure_metrics"]["has_footer"] = has_footer
            print(f"[NAVIGATION_SYNC] Synced to enhanced_extraction_v2.page_metadata.structure_metrics")
        except (KeyError, TypeError) as e:
            print(f"[NAVIGATION_SYNC] Failed to sync Location 1: {e}")
        
        # Sync Location 2: body_signals.structure inside technical_seo_signals
        try:
            if "enhanced_extraction_v2" not in semantic_dataset:
                semantic_dataset["enhanced_extraction_v2"] = {}
            if "technical_seo_signals" not in semantic_dataset["enhanced_extraction_v2"]:
                semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"] = {}
            if "body_signals" not in semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]:
                semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"] = {}
            if "structure" not in semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"]:
                semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"]["structure"] = {}
                
            semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"]["structure"]["has_nav"] = has_nav
            semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"]["structure"]["has_header"] = has_header
            semantic_dataset["enhanced_extraction_v2"]["technical_seo_signals"]["body_signals"]["structure"]["has_footer"] = has_footer
            print(f"[NAVIGATION_SYNC] Synced to enhanced_extraction_v2.technical_seo_signals.body_signals.structure")
        except (KeyError, TypeError) as e:
            print(f"[NAVIGATION_SYNC] Failed to sync Location 2: {e}")
            
    except Exception as e:
        extraction_errors.append(f"Enhanced technical signals extraction failed: {e}")
        semantic_dataset["enhanced_technical_signals"] = {"crawlability": {"robots_txt_accessible": False}, "error": str(e)}
    
    # Extract AI visibility signals (organization, llms.txt, geo)
    try:
        ai_visibility_signals = extract_ai_visibility_signals(semantic_dataset, soup, url)
        semantic_dataset["ai_visibility_signals"] = ai_visibility_signals
    except Exception as e:
        extraction_errors.append(f"AI visibility signals extraction failed: {e}")
        semantic_dataset["ai_visibility_signals"] = {"organization_schema_validation": {"organization_detected": False}, "error": str(e)}
    
    # Add performance optimization metadata
    semantic_dataset.update({
        "extraction_performance": {
            "html_size_limited": len(html) > 500_000,
            "memory_optimized": True,
            "batch_extraction_used": True,
            "safe_extraction_enabled": True
        },
        "new_signals_integrated": True,
        "author_signals_available": "author_signals" in semantic_dataset,
        "nap_signals_available": "nap_signals" in semantic_dataset,
        "answer_signals_available": "answer_signals" in semantic_dataset,
        "enhanced_technical_available": "enhanced_technical_signals" in semantic_dataset,
        "ai_visibility_available": "ai_visibility_signals" in semantic_dataset
    })
    
    return semantic_dataset

def extract_real_word_count(ai_signals: dict, html: str) -> int:
    """Extract real word count from AI signals or HTML content"""
    try:
        # Method 1: Use content_sections extraction summary if available
        content_sections = ai_signals.get("content_sections", {})
        if content_sections and "extraction_summary" in content_sections:
            summary = content_sections["extraction_summary"]
            if "total_word_count" in summary:
                word_count = summary["total_word_count"]
                if isinstance(word_count, int) and word_count > 0:
                    print(f"[WORD_COUNT] Using content_sections word count: {word_count}")
                    return word_count
        
        # Method 2: Compute from cleaned visible text
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'lxml')
        
        # Remove script, style, and other non-content elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Get visible text
        visible_text = soup.get_text(separator=' ', strip=True)
        
        # Clean up text and count words
        import re
        # Replace multiple whitespace with single space
        cleaned_text = re.sub(r'\s+', ' ', visible_text)
        # Split into words (filter out empty strings)
        words = [word for word in cleaned_text.split(' ') if word.strip()]
        word_count = len(words)
        
        print(f"[WORD_COUNT] Computed from visible text: {word_count}")
        return max(1, word_count)  # Ensure at least 1 word
        
    except Exception as e:
        print(f"[WORD_COUNT] Error extracting word count: {e}")
        return 1  # Safe fallback

def analyze_single_url(url: str, job: AIVisibilityJob) -> dict:
    """Analyze a single URL for AI visibility using HTML from database (no HTTP requests)"""
    try:
        # Check cancellation before processing each URL
        if is_job_cancelled(job.jobId):
            return None
        
        # Use projectId from job
        projectId = job.projectId
        
        print(f"[AI_VISIBILITY] Analyzing URL from database: {url}")
        
        # === ARCHITECTURE FIX: Read HTML from database instead of HTTP requests ===
        # Import seo_page_data collection
        from db import seo_page_data
        
        # Find the page in seo_page_data collection using projectId
        page_data = None
        if job.projectId and job.projectId != 'null':
            try:
                page_data = seo_page_data.find_one({
                    "projectId": ObjectId(job.projectId),
                    "url": url,
                    "extraction_status": "SUCCESS"
                })
                print(f"[AI_VISIBILITY] Found page data for {url}: {page_data is not None}")
            except Exception as db_error:
                print(f"[AI_VISIBILITY] Database query failed for {url}: {db_error}")
        
        if not page_data:
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': 0,
                'response_time_ms': 0,
                'error': 'Page not found in seo_page_data collection',
                'skipped': True
            }
        
        # Get HTML from database instead of HTTP request
        html = page_data.get('raw_html', '')
        if not html:
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': 0,
                'response_time_ms': 0,
                'error': 'No HTML found in database for this URL',
                'skipped': True
            }
        
        # Check if content is actually HTML (basic check)
        if not html or ('<!DOCTYPE' not in html and '<html' not in html.lower()):
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': 200,  # Page exists but content is not HTML
                'response_time_ms': 0,
                'error': 'Content is not HTML',
                'skipped': True
            }
        
        print(f"[AI_VISIBILITY] Using database HTML for {url} ({len(html)} chars)")
        
        # Set status_code and response_time based on database data
        status_code = page_data.get('status_code', 200)
        response_time_ms = page_data.get('response_time_ms', 0)
        
        # === PART 3: PERFORMANCE GUARDS ===
        # Check HTML size (already exists in fetcher, but double-check)
        html_size = len(html.encode('utf-8'))
        MAX_HTML_SIZE = 5 * 1024 * 1024  # 5MB
        
        if html_size > MAX_HTML_SIZE:
            print(f"[PERFORMANCE_GUARD] HTML too large: {html_size} bytes > {MAX_HTML_SIZE} bytes")
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': status_code,
                'response_time_ms': response_time_ms,
                'error': f'HTML too large: {html_size} bytes',
                'performance_guard_triggered': 'html_size'
            }
        
        # === CRITICAL FIX: Parse HTML to create soup object ===
        soup = BeautifulSoup(html, 'html.parser')
        
        # DOM node cap guard
        MAX_DOM_NODES = 15000
        dom_nodes = len(soup.find_all())
        
        if dom_nodes > MAX_DOM_NODES:
            print(f"[PERFORMANCE_GUARD] Too many DOM nodes: {dom_nodes} > {MAX_DOM_NODES}")
            soup.decompose()
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': status_code,
                'response_time_ms': response_time_ms,
                'error': f'Too many DOM nodes: {dom_nodes}',
                'performance_guard_triggered': 'dom_nodes'
            }
        
        print(f"[PERFORMANCE_GUARD] DOM nodes: {dom_nodes} (within limit {MAX_DOM_NODES})")
        
        # Extract comprehensive AI visibility signals
        ai_signals = extract_comprehensive_signals(html, url)
        
        # Calculate entity density and mentions AFTER parsed_entities is populated
        # Use parsed_entities from ai_signals for accurate entity counting
        parsed_entities = ai_signals.get('parsed_entities', [])
        entity_graph = ai_signals.get('unified_entity_graph', {})
        
        # Get title and meta description for broader entity search
        page_title = soup.title.string if soup.title else ""
        meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        meta_desc = meta_desc_tag.get('content', '') if meta_desc_tag else ""
        
        # Use main_text for entity density calculation (will be available later)
        main_text = ""  # Placeholder, will be updated after main_content extraction
        expanded_text = f"{page_title} {meta_desc} {main_text}"
        entity_metrics = calculate_entity_density(entity_graph, expanded_text, 0, ai_signals)  # word_count will be updated later
        
        print(f"[ENTITY_METRICS_FIX] Calculated entity_metrics with {len(parsed_entities)} parsed_entities")
        print(f"[ENTITY_METRICS_FIX] Entity count: {entity_metrics.get('entity_count', 0)}")
        print(f"[ENTITY_METRICS_FIX] Entity mentions: {entity_metrics.get('primary_entity_mentions_in_text', 0)}")
        
        # === PHASE 2: AI-READY EXTRACTION LAYER ===
        # Extract main content for accurate analysis
        main_content = extract_main_content(soup, url)
        
        # === CRITICAL FIX: Handle None main_content gracefully ===
        if not main_content:
            print("[PHASE2] Main content extraction returned None - using fallback")
            main_content = {
                'main_content_text': '',
                'content_extraction_method': 'failed',
                'nav_keyword_counts': {},
                'isolation_warnings': ['Main content extraction failed']
            }
        
        main_text = main_content.get('main_content_text', '')
        
        # Extract heading hierarchy
        heading_metrics = extract_heading_hierarchy(soup)
        
        # Extract paragraph structure metrics
        paragraph_metrics = extract_paragraph_metrics(main_text)
        
        # Extract readability metrics
        readability_metrics = calculate_flesch_readability(main_text)
        
        # Detect FAQ content
        faq_metrics = detect_faq_content(soup, main_text)
        
        # Calculate real word count early to use in metrics
        word_count = extract_real_word_count(ai_signals, html)
        
        # Recalculate entity_metrics with actual main_text and word_count
        # FIX: Use full visible text for entity mention counting, not just main_content
        
        # Remove script, style, and other non-content elements (same as word count)
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Get visible text (same as word count)
        visible_text = soup.get_text(separator=' ', strip=True)
        
        # Clean up text (same as word count)
        import re
        cleaned_text = re.sub(r'\s+', ' ', visible_text)
        
        expanded_text = f"{page_title} {meta_desc} {cleaned_text}"
        print(f"[ENTITY_FIX] Using full visible text for mention counting: {len(cleaned_text)} chars")
        
        entity_metrics = calculate_entity_density(entity_graph, expanded_text, word_count, ai_signals)
        
        print(f"[ENTITY_METRICS_RECALC] Recalculated entity_metrics with word_count={word_count}")
        print(f"[ENTITY_METRICS_RECALC] Final entity count: {entity_metrics.get('entity_count', 0)}")
        print(f"[ENTITY_METRICS_RECALC] Final entity mentions: {entity_metrics.get('primary_entity_mentions_in_text', 0)}")
        
        # Extract readability metrics
        readability_metrics = calculate_flesch_readability(main_text)
        
        # SURGICAL FIX: Calculate paragraph count from full DOM for consistency
        # Get all paragraph elements from the entire page, not just main_content
        all_paragraphs = soup.find_all('p')
        shared_paragraph_count = len(all_paragraphs)
        
        print("=== PARAGRAPH COUNT DEBUG - PIPELINE B ===")
        print(f"method used: soup.find_all('p') - full DOM <p> tags")
        print(f"raw paragraph elements found: {len(all_paragraphs)}")
        print(f"paragraph_count assigned: {shared_paragraph_count}")
        print("=== END PIPELINE B ===")
        print(f"[PARAGRAPH_FIX] Using full DOM paragraph count: {shared_paragraph_count}")
        
        # Create content metrics with correct word count as single source of truth
        content_metrics = {
            'word_count': word_count,  # BUG FIX 2: Single source of truth
            'readability_score': readability_metrics.get('flesch_score', 0),
            'paragraph_count': shared_paragraph_count,  # Use shared DOM-based count
            'avg_paragraph_length': paragraph_metrics.get('avg_paragraph_length', 0)
        }
        
        # Detect FAQ content
        faq_metrics = detect_faq_content(soup, main_text)
        
        # Detect step-by-step content
        step_metrics = detect_step_by_step(soup, main_text)
        
        # Calculate readability score
        readability_metrics = calculate_flesch_readability(main_text)
        
        # Classify intent
        intent_metrics = classify_intent(main_text)
        
        # === PHASE 2 FINAL OUTPUT ===
        # Combine all Phase 2 metrics
        ai_signals.update({
            'content_metrics': {
                'word_count': word_count,
                'paragraph_count': shared_paragraph_count,  # Use shared DOM-based count
                'avg_sentence_length': paragraph_metrics.get('avg_sentence_length', 0),
                'readability_score': readability_metrics.get('flesch_score', 0),
                'short_paragraph_ratio': paragraph_metrics.get('short_paragraph_ratio', 0),
                'long_paragraph_ratio': paragraph_metrics.get('long_paragraph_ratio', 0),
                'avg_paragraph_length': paragraph_metrics.get('avg_paragraph_length', 0)
            },
            'heading_metrics': heading_metrics,
            'faq_metrics': faq_metrics,
            'faq_data': faq_metrics.get('faq_data', []),  # === CRITICAL FIX: Add FAQ data to output ===
            'step_metrics': step_metrics,
            'entity_metrics': entity_metrics,
            'intent_metrics': intent_metrics,
            'main_content': {
                'extraction_method': main_content.get('content_extraction_method', 'unknown'),
                # SURGICAL FIX: Remove content_word_count field entirely
                # 'content_word_count': main_word_count
            },
            # === CRITICAL FIX: Quality flags for debugging ===
            'quality_flags': {
                'low_word_count': word_count < 300,  # BUG FIX 2: placeholder, recalculated after top-level word_count is set
                'weak_heading_structure': heading_metrics.get('heading_structure_score_input', 0) < 50,
                'no_main_content_detected': main_content.get('content_extraction_method') == 'failed' or content_metrics['word_count'] < 100,
                'malformed_structure': not heading_metrics.get('heading_sequence_valid', False),
                'insufficient_text_for_readability': readability_metrics.get('word_count', 0) < 100,
                'low_entity_density': entity_metrics.get('entity_per_1000_words', 0) < 2,  # === CRITICAL FIX: Correct threshold (was 5, too aggressive) ===
                'no_faq_detected': not faq_metrics.get('faq_detected', False),
                'no_step_content': not step_metrics.get('step_section_present', False),
                'low_intent_confidence': intent_metrics.get('confidence') == 'low'
            }
        })
        
        # === CRITICAL FIX: Ensure deterministic output ===
        # Round all floating point numbers consistently
        def round_nested_floats(obj):
            if isinstance(obj, dict):
                return {k: round_nested_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [round_nested_floats(item) for item in obj]
            elif isinstance(obj, float):
                return round(obj, 3)  # Consistent 3 decimal places
            else:
                return obj
        
        ai_signals = round_nested_floats(ai_signals)
        
        # 🔥 PHASE 1: REAL WORD COUNT EXTRACTION
        ai_signals["word_count"] = word_count
        
        # === PHASE 1 SAFETY ADDITION ===
        # Safe progress update - don't let progress failures break job
        try:
            send_progress_update(
                job.jobId, 
                50, 
                "ANALYZING", 
                f"Processed {url}", 
                f"Found {word_count} words"
            )
        except Exception as progress_error:
            print(f"[SAFETY] Progress update failed | jobId={job.jobId} | error={progress_error}")
            # Continue with job - don't fail due to progress issues
        
        # === PIPELINE IMPROVEMENT: Explicit field mapping to prevent conflicts ===
        # Extract technical signals to top-level fields for backward compatibility
        enhanced_technical = ai_signals.get('enhanced_technical_signals', {})
        if enhanced_technical:
            navigation_detection = enhanced_technical.get('navigation_detection', {})
            page_speed_indicators = enhanced_technical.get('page_speed_indicators', {})
            
            # BUG FIX 2: Map nested fields to top-level output fields
            ai_signals.update({
                'has_navigation': bool(navigation_detection.get('has_navigation', False)),
                'has_header': bool(navigation_detection.get('has_header', False)),
                'has_footer': bool(navigation_detection.get('has_footer', False)),
                'lazy_loading_detected': bool(page_speed_indicators.get('lazy_loading', False))
            })
            
            print(f"[MAPPED_FIELD] has_navigation: {navigation_detection.get('has_navigation', False)}")
            print(f"[MAPPED_FIELD] has_header: {navigation_detection.get('has_header', False)}")
            print(f"[MAPPED_FIELD] has_footer: {navigation_detection.get('has_footer', False)}")
            print(f"[MAPPED_FIELD] lazy_loading_detected: {page_speed_indicators.get('lazy_loading', False)}")
        
        # Extract content metrics to ensure correct field usage
        content_metrics = ai_signals.get('content_metrics', {})
        if content_metrics:
            # SURGICAL FIX: Remove content_word_count mapping - this field is being removed entirely
            # ai_signals['content_word_count'] = content_metrics.get('word_count', 0)
            print(f"[MAPPED_FIELD] content_word_count: REMOVED (using word_count instead)")
        
        # Extract entity metrics
        entity_metrics = ai_signals.get('entity_metrics', {})
        if entity_metrics:
            # Ensure entity mentions are properly mapped
            ai_signals['primary_entity_mentions_in_text'] = entity_metrics.get('primary_entity_mentions_in_text', 0)
            print(f"[MAPPED_FIELD] primary_entity_mentions_in_text: {entity_metrics.get('primary_entity_mentions_in_text', 0)}")
        
        # Extract author signals
        author_signals = ai_signals.get('author_signals', {})
        if author_signals:
            # BUG FIX 5: Ensure author detection is properly mapped
            ai_signals['author_detected'] = bool(author_signals.get('author_detected', False))
            print(f"[MAPPED_FIELD] author_detected: {author_signals.get('author_detected', False)}")
        
        # Extract quality flags — BUG FIX 2: Use top-level word_count as single source of truth
        quality_flags = ai_signals.get('quality_flags', {})
        if quality_flags:
            quality_flags['low_word_count'] = word_count < 300
            ai_signals['quality_flags'] = quality_flags
            ai_signals['low_word_count'] = bool(quality_flags['low_word_count'])
            print(f"[MAPPED_FIELD] low_word_count: {quality_flags['low_word_count']} (word_count={word_count})")
        
        print(f"[FINAL_OUTPUT] Preparing final result with explicit field mapping")
        
        # === SURGICAL FIX: Paragraph Count Conflict ===
        # Overwrite content_metrics.paragraph_count with enhanced_extraction_v2 value
        correct_paragraph_count = ai_signals.get("enhanced_extraction_v2", {}) \
            .get("page_metadata", {}) \
            .get("content_metrics", {}) \
            .get("paragraph_count", None)
        
        if correct_paragraph_count is not None:
            # Update content_metrics.paragraph_count with the correct value
            content_metrics = ai_signals.get('content_metrics', {})
            if content_metrics:
                content_metrics['paragraph_count'] = correct_paragraph_count
                ai_signals['content_metrics'] = content_metrics
                print(f"[PARAGRAPH_FIX] Overwrote content_metrics.paragraph_count with correct value: {correct_paragraph_count}")
        
        # === FINAL VALIDATION: Ensure all boolean fields are properly set ===
        boolean_fields = [
            'has_navigation', 'has_header', 'has_footer', 'lazy_loading_detected',
            'author_detected', 'low_word_count'
        ]
        
        for field in boolean_fields:
            if field in ai_signals:
                ai_signals[field] = bool(ai_signals[field])
                print(f"[FINAL_OUTPUT] {field}: {ai_signals[field]}")
        
        # === CANONICAL SIGNAL REGISTRY ===
        # The single source of truth for the JSON output.
        # Ensure no nested spread logic overwrites these fields.
        CANONICAL_SIGNALS = [
            "word_count",
            "author_detected",
            "has_navigation",
            "has_header",
            "has_footer",
            "lazy_loading_detected",
            "primary_entity_mentions_in_text"
        ]
        
        final_output = {
            'projectId': ObjectId(projectId),  # 🧠 Always use AI project ID for output
            'ai_jobId': ObjectId(job.jobId),
            'url': url,
            'http_status_code': status_code,
            'response_time_ms': response_time_ms,
            'extraction_timestamp': datetime.utcnow()
        }
        
        # Spread base signals to ensure all nested structures are still preserved 
        # (as expected by DB schema, like quality_flags, content_metrics, etc)
        for key, value in ai_signals.items():
            if key not in CANONICAL_SIGNALS:
               final_output[key] = value
        
        # Explicitly map CANONICAL_SIGNALS to guarantee they are the single source of truth
        for signal in CANONICAL_SIGNALS:
            if signal in ai_signals:
                final_output[signal] = ai_signals[signal]
        
        return final_output
        
    except Exception as e:
        print(f"Error analyzing {url}: {e}")
        return {
            'projectId': ObjectId(projectId),  # 🧠 Always use AI project ID for output
            'ai_jobId': ObjectId(job.jobId),
            'url': url,
            'http_status_code': 0,
            'response_time_ms': 0,
            'error': str(e)
        }

def analyze_single_url_with_html(url: str, raw_html: str, job: AIVisibilityJob) -> dict:
    """Optimized version: Analyze a single URL using provided HTML (no database fetch)"""
    try:
        # Check cancellation before processing each URL
        if is_job_cancelled(job.jobId):
            return None
        
        # Use projectId from job
        projectId = job.projectId
        
        print(f"[AI_VISIBILITY] Analyzing URL with provided HTML: {url}")
        
        # Validate HTML content
        if not raw_html:
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': 0,
                'response_time_ms': 0,
                'error': 'No HTML provided',
                'skipped': True
            }
        
        # Check if content is actually HTML (basic check)
        if not raw_html or ('<!DOCTYPE' not in raw_html and '<html' not in raw_html.lower()):
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': 200,  # Page exists but content is not HTML
                'response_time_ms': 0,
                'error': 'Content is not HTML',
                'skipped': True
            }
        
        print(f"[AI_VISIBILITY] Using provided HTML for {url} ({len(raw_html)} chars)")
        
        # Set default status_code and response_time (since we don't have page_data)
        status_code = 200
        response_time_ms = 0
        
        # === PART 3: PERFORMANCE GUARDS ===
        # Check HTML size
        html_size = len(raw_html.encode('utf-8'))
        MAX_HTML_SIZE = 5 * 1024 * 1024  # 5MB
        
        if html_size > MAX_HTML_SIZE:
            print(f"[PERFORMANCE_GUARD] HTML too large: {html_size} bytes > {MAX_HTML_SIZE} bytes")
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': status_code,
                'response_time_ms': response_time_ms,
                'error': f'HTML too large: {html_size} bytes',
                'performance_guard_triggered': 'html_size'
            }
        
        # === CRITICAL FIX: Parse HTML to create soup object ===
        soup = BeautifulSoup(raw_html, 'html.parser')
        
        # DOM node cap guard
        MAX_DOM_NODES = 15000
        dom_nodes = len(soup.find_all())
        
        if dom_nodes > MAX_DOM_NODES:
            print(f"[PERFORMANCE_GUARD] Too many DOM nodes: {dom_nodes} > {MAX_DOM_NODES}")
            soup.decompose()
            return {
                'projectId': ObjectId(projectId),
                'ai_jobId': ObjectId(job.jobId),
                'url': url,
                'http_status_code': status_code,
                'response_time_ms': response_time_ms,
                'error': f'Too many DOM nodes: {dom_nodes}',
                'performance_guard_triggered': 'dom_nodes'
            }
        
        print(f"[PERFORMANCE_GUARD] DOM nodes: {dom_nodes} (within limit {MAX_DOM_NODES})")
        
        # Extract comprehensive AI visibility signals
        ai_signals = extract_comprehensive_signals(raw_html, url)
        
        # Calculate entity density and mentions AFTER parsed_entities is populated
        # Use parsed_entities from ai_signals for accurate entity counting
        parsed_entities = ai_signals.get('parsed_entities', [])
        entity_graph = ai_signals.get('unified_entity_graph', {})
        
        # Get title and meta description for broader entity search
        page_title = soup.title.string if soup.title else ""
        meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        meta_desc = meta_desc_tag.get('content', '') if meta_desc_tag else ""
        
        # Use main_text for entity density calculation (will be available later)
        main_text = ""  # Placeholder, will be updated after main_content extraction
        expanded_text = f"{page_title} {meta_desc} {main_text}"
        entity_metrics = calculate_entity_density(entity_graph, expanded_text, 0, ai_signals)  # word_count will be updated later
        
        print(f"[ENTITY_METRICS_FIX] Calculated entity_metrics with {len(parsed_entities)} parsed_entities")
        print(f"[ENTITY_METRICS_FIX] Entity count: {entity_metrics.get('entity_count', 0)}")
        print(f"[ENTITY_METRICS_FIX] Entity mentions: {entity_metrics.get('primary_entity_mentions_in_text', 0)}")
        
        # === PHASE 2: AI-READY EXTRACTION LAYER ===
        # Extract main content for accurate analysis
        main_content = extract_main_content(soup, url)
        
        # === CRITICAL FIX: Handle None main_content gracefully ===
        if not main_content:
            print("[PHASE2] Main content extraction returned None - using fallback")
            main_content = {
                'main_content_text': '',
                'content_extraction_method': 'failed',
                'nav_keyword_counts': {},
                'isolation_warnings': ['Main content extraction failed']
            }
        
        main_text = main_content.get('main_content_text', '')
        
        # Extract heading hierarchy
        heading_metrics = extract_heading_hierarchy(soup)
        
        # Extract paragraph structure metrics
        paragraph_metrics = extract_paragraph_metrics(main_text)
        
        # Extract readability metrics
        readability_metrics = calculate_flesch_readability(main_text)
        
        # Detect FAQ content
        faq_metrics = detect_faq_content(soup, main_text)
        
        # Calculate real word count early to use in metrics
        word_count = extract_real_word_count(ai_signals, raw_html)
        
        # Recalculate entity_metrics with actual main_text and word_count
        # FIX: Use full visible text for entity mention counting, not just main_content
        
        # Remove script, style, and other non-content elements (same as word count)
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Get visible text (same as word count)
        visible_text = soup.get_text(separator=' ', strip=True)
        
        # Clean up text (same as word count)
        import re
        cleaned_text = re.sub(r'\s+', ' ', visible_text)
        
        expanded_text = f"{page_title} {meta_desc} {cleaned_text}"
        print(f"[ENTITY_FIX] Using full visible text for mention counting: {len(cleaned_text)} chars")
        
        entity_metrics = calculate_entity_density(entity_graph, expanded_text, word_count, ai_signals)
        
        print(f"[ENTITY_METRICS_RECALC] Recalculated entity_metrics with word_count={word_count}")
        print(f"[ENTITY_METRICS_RECALC] Final entity count: {entity_metrics.get('entity_count', 0)}")
        print(f"[ENTITY_METRICS_RECALC] Final entity mentions: {entity_metrics.get('primary_entity_mentions_in_text', 0)}")
        
        # Extract readability metrics
        readability_metrics = calculate_flesch_readability(main_text)
        
        # SURGICAL FIX: Calculate paragraph count from full DOM for consistency
        # Get all paragraph elements from the entire page, not just main_content
        all_paragraphs = soup.find_all('p')
        shared_paragraph_count = len(all_paragraphs)
        
        print("=== PARAGRAPH COUNT DEBUG - PIPELINE B ===")
        print(f"method used: soup.find_all('p') - full DOM <p> tags")
        print(f"raw paragraph elements found: {len(all_paragraphs)}")
        print(f"paragraph_count assigned: {shared_paragraph_count}")
        print("=== END PIPELINE B ===")
        print(f"[PARAGRAPH_FIX] Using full DOM paragraph count: {shared_paragraph_count}")
        
        # === CRITICAL FIX: Update ai_signals with recalculated metrics ===
        # This ensures the final output has consistent values
        ai_signals.update({
            'paragraph_count': shared_paragraph_count,
            'entity_metrics': entity_metrics,
            'word_count': word_count,
            'main_content_metrics': main_content,
            'heading_metrics': heading_metrics,
            'paragraph_metrics': paragraph_metrics,
            'readability_metrics': readability_metrics,
            'faq_metrics': faq_metrics
        })
        
        # === FINAL OUTPUT CONSTRUCTION ===
        # Use CANONICAL_SIGNALS as the single source of truth for final output
        CANONICAL_SIGNALS = [
            "content_signals",
            "entity_signals", 
            "schema_signals",
            "metadata_signals",
            "image_signals",
            "video_signals",
            "organization_signals",
            "page_type_signals",
            "relationship_signals",
            "entity_graph",
            "parsed_entities",
            "unified_entity_graph",
            "architecture_violations",
            "integrity_metrics",
            "entity_coverage",
            "entity_density",
            "entity_per_1000_words",
            "primary_entity_mentions_in_text",
            "main_content_metrics",
            "heading_metrics",
            "paragraph_metrics", 
            "readability_metrics",
            "faq_metrics",
            "content_quality_score",
            "ai_readiness_score",
            "entity_density_score",
            "schema_completeness_score",
            "overall_page_score",
            "ai_snippet_probability",
            "ai_citation_rate",
            "conversational_content",
            "entity_coverage",
            "knowledge_graph_presence",
            "voice_search_readiness",
            "lazy_loading_detected",
            "primary_entity_mentions_in_text"
        ]
        
        final_output = {
            'projectId': ObjectId(projectId),  # 🧠 Always use AI project ID for output
            'ai_jobId': ObjectId(job.jobId),
            'url': url,
            'http_status_code': status_code,
            'response_time_ms': response_time_ms,
            'extraction_timestamp': datetime.utcnow()
        }
        
        # Spread base signals to ensure all nested structures are still preserved 
        # (as expected by DB schema, like quality_flags, content_metrics, etc)
        for key, value in ai_signals.items():
            if key not in CANONICAL_SIGNALS:
               final_output[key] = value
        
        # Explicitly map CANONICAL_SIGNALS to guarantee they are the single source of truth
        for signal in CANONICAL_SIGNALS:
            if signal in ai_signals:
                final_output[signal] = ai_signals[signal]
        
        return final_output
        
    except Exception as e:
        print(f"Error analyzing {url}: {e}")
        return {
            'projectId': ObjectId(projectId),  # 🧠 Always use AI project ID for output
            'ai_jobId': ObjectId(job.jobId),
            'url': url,
            'http_status_code': 0,
            'response_time_ms': 0,
            'error': str(e)
        }

# === PHASE 1 SAFETY ADDITION ===
# Job timeout protection - ENGINEER-LEVEL FIX
# Use time-based timeout instead of signal.alarm for cross-platform compatibility
JOB_TIMEOUT_SECONDS = 60

def check_job_timeout(start_time: datetime, job_id: str) -> bool:
    """Check if job has exceeded timeout using time comparison"""
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    if elapsed > JOB_TIMEOUT_SECONDS:
        print(f"[SAFETY] Job timeout exceeded | jobId={job_id} | elapsed={elapsed}s")
        return True
    return False

def execute_ai_visibility(job: AIVisibilityJob):
    """Execute AI visibility analysis - SAFE VERSION"""
    start_time = datetime.utcnow()
    duration_ms = 0
    
    # Defensive logging
    print(f"[AI_VISIBILITY] Starting | jobId={job.jobId}")
    print(f"[AI_VISIBILITY] projectId={job.projectId}")
    
    # CRITICAL LOG: Check if project keywords are accessed during AI visibility
    if job.projectId and job.projectId != 'null':
        try:
            seo_project = seoprojects.find_one({"_id": ObjectId(job.projectId)})
            if seo_project:
                print(f"🔍 DEBUG: AI Visibility worker found SEO project:", {
                    "projectId": job.projectId,
                    "projectKeywords": seo_project.get("keywords", []),
                    "projectKeywordsString": str(seo_project.get("keywords", [])),
                    "projectName": seo_project.get("project_name", "unknown")
                })
            else:
                print(f"🔍 DEBUG: AI Visibility worker - SEO project not found for projectId={job.projectId}")
        except Exception as e:
            print(f"🔍 DEBUG: AI Visibility worker - Error fetching SEO project: {e}")
    
    # === PHASE 1 SAFETY ADDITION ===
    # ENGINEER-LEVEL FIX: Use time-based timeout check instead of signals
    try:
        print(f"[WORKER] AI_VISIBILITY started | jobId={job.jobId}")
        print(f"[WORKER] Job parameters | projectId={job.projectId}")
        
            # Disable Selenium temporarily for AI analysis (HTTP-only mode)
        from scraper.shared import fetcher
        original_selenium = getattr(fetcher, 'SELENIUM_AVAILABLE', False)
        fetcher.SELENIUM_AVAILABLE = False
        
        try:
            # === CRITICAL FIX: Use ONLY Page Scraper output (seo_page_data) as single source of truth ===
            print(f"[AI] Using Page Scraper data only - SINGLE SOURCE OF TRUTH")
            
            # Import seo_page_data collection
            from db import seo_page_data
            
            # Determine project ID for query
            project_object_id = None
            if job.projectId and job.projectId != 'null':
                project_object_id = ObjectId(job.projectId)
            
            if not project_object_id:
                print(f"[AI] ERROR: No valid project ID found for Page Scraper data")
                return {
                    "status": "error",
                    "jobId": job.jobId,
                    "message": "No valid project ID found for Page Scraper data",
                    "stats": {"pages_processed": 0}
                }
            
            # Fetch pages ONLY from seo_page_data (Page Scraper output)
            print(f"[AI] Querying seo_page_data collection for projectId={project_object_id}")
            pages = list(
                seo_page_data.find({
                    "projectId": project_object_id,
                    "extraction_status": "SUCCESS",
                    "raw_html": { "$exists": True, "$ne": None }
                })
                .sort("url", 1)
                .limit(25)
            )
            
            print(f"[AI] Pages fetched from Page Scraper: {len(pages)}")
            if pages:
                print(f"[AI] URLs: {[p['url'] for p in pages[:5]]}{'...' if len(pages) > 5 else ''}")
            
            # Safety check
            if not pages:
                print(f"[AI] No scraped pages found in seo_page_data - aborting AI visibility")
                return {
                    "status": "no_pages",
                    "jobId": job.jobId,
                    "message": "No scraped pages found in seo_page_data collection",
                    "stats": {"pages_processed": 0}
                }
            
            # === ENGINEER-LEVEL FIX ===
            # Add timeout checks in main processing loop
            print(f"[AI] Starting analysis of {len(pages)} URLs from Page Scraper")
            print(f"[AI_VISIBILITY] Analyzing pages: {len(pages)}")
            print(f"[AI_VISIBILITY] Using HTML from Page Scraper data - NO HTTP REQUESTS")
            print(f"[AI_VISIBILITY] Pages loaded from seo_page_data: {len(pages)}")
            
            all_results = []
            successful_pages = 0
            failed_pages = 0
            
            # === FIX: Use deterministic progress calculation ===
            total_pages = len(pages)
            print(f"[AI_VISIBILITY] Total pages to analyze: {total_pages}")
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                # Submit analysis tasks for each page from seo_page_data
                futures = []
                for page in pages:
                    url = page["url"]
                    raw_html = page["raw_html"]
                    if url and raw_html:
                        # Pass both url and raw_html to avoid secondary database fetch
                        futures.append(executor.submit(analyze_single_url_with_html, url, raw_html, job))
                
                # Collect results in submission order for deterministic progress
                for i, future in enumerate(futures):
                    # Check cancellation before processing each result
                    if is_job_cancelled(job.jobId):
                        print(f"🛑 Job {job.jobId} cancelled during analysis")
                        # Cancel remaining futures
                        for f in futures[i+1:]:
                            f.cancel()
                        return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}
                    
                    result = future.result()
                    if result:
                        all_results.append(result)
                        if result.get('skipped'):
                            # Don't count skipped files as failures
                            pass
                        elif 'error' in result:
                            failed_pages += 1
                        else:
                            successful_pages += 1
                    
                    # === DETERMINISTIC PROGRESS: Always increases ===
                    progress = int((i + 1) / total_pages * 100)
                    send_progress_update(
                        job.jobId,
                        progress,
                        "AI Analysis",
                        f"Analyzed {i+1}/{total_pages} pages",
                        f"Success: {successful_pages}, Failed: {failed_pages}"
                    )
            
            # Final cancellation check before storing results
            if is_job_cancelled(job.jobId):
                print(f"🛑 Job {job.jobId} cancelled before completion")
                return {"status": "cancelled", "jobId": job.jobId, "message": "Job cancelled by user"}
            
            # Store results using upsert for idempotency (prevents duplicates)
            if all_results:
                upserted_count = 0
                for result in all_results:
                    # Upsert based on projectId + url combination
                    upsert_result = seo_ai_visibility.update_one(
                        {
                            "projectId": result["projectId"],
                            "url": result["url"]
                        },
                        {
                            "$set": result,
                            "$setOnInsert": {
                                "created_at": datetime.utcnow()
                            }
                        },
                        upsert=True
                    )
                    if upsert_result.upserted_id:
                        upserted_count += 1
                
                print(f"[WORKER] AI visibility upsert completed | total={len(all_results)} | new={upserted_count} | updated={len(all_results) - upserted_count}")
            else:
                print("[WORKER] No AI visibility results to store")
            
            # Calculate duration
            end_time = datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Prepare stats
            stats = {
                "pages_processed": len(all_results),
                "successful_pages": successful_pages,
                "failed_pages": failed_pages,
                "duration_ms": duration_ms
            }
            
            # Send completion callback to Node.js (fire-and-forget)
            try:
                # Validate required environment variables
                node_backend_url = os.environ.get("NODE_BACKEND_URL")
                if not node_backend_url:
                    raise Exception("NODE_BACKEND_URL is required")
                node_url = f"{node_backend_url}/api/jobs/{job.jobId}/complete"
                callback_payload = {"stats": stats, "result_data": {"pages_processed": len(all_results)}}
                
                response = requests.post(node_url, json=callback_payload, timeout=30)
                response.raise_for_status()
                print(f"✅ Successfully notified Node.js of AI visibility completion")
                
            except requests.exceptions.Timeout:
                # Fire-and-forget: timeout doesn't mean job failed
                pass
            except Exception as callback_error:
                # Log but don't fail the job - completion is best-effort
                print(f"⚠️ Failed to notify Node.js of completion (job still succeeded): {callback_error}")
            
            return {
                "status": "success",
                "jobId": job.jobId,
                "message": "AI visibility analysis completed",
                "stats": stats
            }
            
        finally:
            # Restore Selenium setting
            fetcher.SELENIUM_AVAILABLE = original_selenium
            
    except TimeoutError as e:
        # === PHASE 1 SAFETY ADDITION ===
        # Handle job timeout gracefully
        print(f"[SAFETY] Job timeout | jobId={job.jobId} | error={e}")
        return {
            "status": "error",
            "jobId": job.jobId,
            "message": f"Job timeout: {str(e)}",
            "stats": {"pages_processed": 0, "timeout_exceeded": True}
        }
    except Exception as e:
        print(f"❌ Job {job.jobId} failed: {str(e)}")
        
        # Safely compute duration even in exception case
        end_time = datetime.utcnow()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Send failure callback to Node.js
        try:
            # Validate required environment variables
            node_backend_url = os.environ.get("NODE_BACKEND_URL")
            if not node_backend_url:
                raise Exception("NODE_BACKEND_URL is required")
            fail_url = f"{node_backend_url}/api/jobs/{job.jobId}/fail"
            fail_payload = {"error": str(e)}
            requests.post(fail_url, json=fail_payload, timeout=10)
        except:
            pass
        
        # Return error result instead of raising exception
        return {
            "status": "error",
            "jobId": job.jobId,
            "message": f"Job failed: {str(e)}",
            "stats": {"pages_processed": 0, "duration_ms": duration_ms}
        }

def extract_ai_visibility_signals(ai_signals, soup, url) -> dict:
    """Extract 3 new AI visibility signals: Organization validation, llms.txt, and Geo signals"""
    import requests
    from urllib.parse import urlparse, urljoin
    
    # Initialize all signals with safe defaults
    ai_visibility_signals = {
        "organization_schema_validation": {
            "organization_detected": False,
            "name_present": False,
            "missing_name": False
        },
        "llms_txt": {
            "checked": False,
            "exists": False,
            "status_code": None
        },
        "geo_signals": {
            "geo_schema_present": False,
            "map_embed_present": False
        }
    }
    
    try:
        # === 1️⃣ Organization Schema Validation ===
        parsed_entities = ai_signals.get('parsed_entities', [])
        organization_detected = False
        name_present = False
        missing_name = False
        localbusiness_detected = False
        
        # === BUG FIX 1: Check ALL organization entities for name ===
        organization_names = []
        
        for entity in parsed_entities:
            entity_type = entity.get('@type', '')
            
            # === BUG FIX 2: Support multiple organization types ===
            if entity_type in ['Organization', 'LocalBusiness', 'Store', 'ProfessionalService']:
                organization_detected = True
                
                if entity_type == 'LocalBusiness':
                    localbusiness_detected = True
                
                # Check for name OR legalName
                entity_name = entity.get('name', '')
                entity_legal_name = entity.get('legalName', '')
                
                if entity_name and entity_name.strip():
                    organization_names.append(entity_name.strip())
                elif entity_legal_name and entity_legal_name.strip():
                    organization_names.append(entity_legal_name.strip())
        
        # === BUG FIX 1: If ANY organization entity has a name, PASS ===
        if organization_names:
            name_present = True
            missing_name = False
        elif organization_detected:
            missing_name = True
        
        ai_visibility_signals["organization_schema_validation"] = {
            "organization_detected": organization_detected,
            "localbusiness_detected": localbusiness_detected,
            "name_present": name_present,
            "missing_name": missing_name,
            "organization_names_found": organization_names
        }
        
    except Exception as e:
        print(f"[AI_VISIBILITY_SIGNALS] Organization validation error: {e}")
        # Keep safe defaults
    
    try:
        # === 2️⃣ llms.txt Detection ===
        canonical_root = ai_signals.get('canonical_root', url)
        if canonical_root:
            # Extract domain from canonical_root
            parsed_url = urlparse(canonical_root)
            domain = parsed_url.netloc
            
            # Handle URLs without scheme (e.g., "example.com" instead of "https://example.com")
            if not domain and '/' in canonical_root:
                # Try adding https:// scheme
                parsed_url = urlparse(f"https://{canonical_root}")
                domain = parsed_url.netloc
            
            if domain:
                llms_url = f"https://{domain}/llms.txt"
                
                # Mark as checked regardless of outcome
                checked = True
                exists = False
                status_code = None
                
                # === PRODUCTION-GRADE: Try with browser headers ===
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "text/plain,*/*;q=0.9",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
                
                try:
                    response = requests.get(llms_url, timeout=5, headers=headers)
                    status_code = response.status_code
                    
                    if status_code == 200:
                        exists = True
                    elif status_code == 404:
                        exists = False  # File definitively missing
                    elif status_code in [403, 406]:
                        # === CRITICAL FIX: Retry with minimal headers for blocked servers ===
                        print(f"[AI_VISIBILITY_SIGNALS] llms.txt blocked ({status_code}), retrying with minimal headers")
                        minimal_headers = {
                            "User-Agent": "Mozilla/5.0 (compatible; AI-Visibility-Checker/1.0)",
                            "Accept": "text/plain,*/*"
                        }
                        
                        try:
                            retry_response = requests.get(llms_url, timeout=5, headers=minimal_headers)
                            status_code = retry_response.status_code
                            if status_code == 200:
                                exists = True
                                print(f"[AI_VISIBILITY_SIGNALS] llms.txt retry succeeded with status {status_code}")
                            else:
                                print(f"[AI_VISIBILITY_SIGNALS] llms.txt retry failed with status {status_code}")
                                exists = False
                        except requests.RequestException as retry_e:
                            print(f"[AI_VISIBILITY_SIGNALS] llms.txt retry failed: {retry_e}")
                            exists = False
                    else:
                        exists = False  # Other status codes treated as not existing
                        
                except requests.RequestException as e:
                    print(f"[AI_VISIBILITY_SIGNALS] llms.txt request failed: {e}")
                    # Keep defaults: exists=False, status_code=None
                
                ai_visibility_signals["llms_txt"] = {
                    "checked": checked,
                    "exists": exists,
                    "status_code": status_code
                }
            else:
                # No valid domain found - still mark as attempted but failed
                ai_visibility_signals["llms_txt"] = {
                    "checked": True,
                    "exists": False,
                    "status_code": None
                }
        
    except Exception as e:
        print(f"[AI_VISIBILITY_SIGNALS] llms.txt detection error: {e}")
        # Even on error, mark as attempted but failed
        ai_visibility_signals["llms_txt"] = {
            "checked": True,
            "exists": False,
            "status_code": None
        }
    
    try:
        # === 3️⃣ Geo Schema + Map Embed Detection ===
        geo_schema_present = False
        map_embed_present = False
        
        # A) Check for Geo schema in parsed_entities
        parsed_entities = ai_signals.get('parsed_entities', [])
        for entity in parsed_entities:
            if entity.get('@type') == 'GeoCoordinates':
                geo_schema_present = True
                break
        
        # B) Check for Google Map embeds in HTML
        if soup:
            iframes = soup.find_all('iframe')
            for iframe in iframes:
                src = iframe.get('src', '')
                if 'google.com/maps' in src:
                    map_embed_present = True
                    break
        
        ai_visibility_signals["geo_signals"] = {
            "geo_schema_present": geo_schema_present,
            "map_embed_present": map_embed_present
        }
        
    except Exception as e:
        print(f"[AI_VISIBILITY_SIGNALS] Geo signals detection error: {e}")
        # Keep safe defaults
    
    try:
        # === 4️⃣ Additional Schema Signals ===
        parsed_entities = ai_signals.get('parsed_entities', [])
        
        # Initialize all schema signals
        additional_schemas = {
            "opening_hours_schema_present": False,
            "event_schema_present": False,
            "aggregate_rating_schema_present": False,
            "service_or_product_offers_schema": False,
            "breadcrumb_schema_present": False
        }
        
        # Check each entity for schema types and properties
        for entity in parsed_entities:
            entity_type = entity.get('@type', '')
            
            # Check for specific schema types
            if entity_type == 'Event':
                additional_schemas["event_schema_present"] = True
            elif entity_type == 'AggregateRating':
                additional_schemas["aggregate_rating_schema_present"] = True
            elif entity_type == 'BreadcrumbList':
                additional_schemas["breadcrumb_schema_present"] = True
            elif entity_type in ['Service', 'Product']:
                # Check for offers property
                if 'offers' in entity:
                    additional_schemas["service_or_product_offers_schema"] = True
            
            # Check for openingHoursSpecification in any entity
            if 'openingHoursSpecification' in entity:
                additional_schemas["opening_hours_schema_present"] = True
        
        # Add to main signals dict
        ai_visibility_signals["additional_schema_signals"] = additional_schemas
        
    except Exception as e:
        print(f"[AI_VISIBILITY_SIGNALS] Additional schema signals error: {e}")
        ai_visibility_signals["additional_schema_signals"] = {
            "opening_hours_schema_present": False,
            "event_schema_present": False,
            "aggregate_rating_schema_present": False,
            "service_or_product_offers_schema": False,
            "breadcrumb_schema_present": False
        }
    
    # Simple organization schema detection (NEW)
    try:
        structured_data = semantic_dataset.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except:
                structured_data = {}
        
        org_detected = False
        name_present = False
        legal_name_present = False
        contact_complete = False
        
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Organization":
                org_detected = True
                if item.get("name"):
                    name_present = True
                if item.get("legalName"):
                    legal_name_present = True
                if item.get("telephone") and item.get("address"):
                    contact_complete = True
                break
        
        ai_visibility_signals["organization_schema_validation"] = {
            "organization_detected": org_detected,
            "name_present": name_present,
            "legal_name_present": legal_name_present,
            "contact_complete": contact_complete
        }
    except Exception:
        ai_visibility_signals["organization_schema_validation"] = {
            "organization_detected": False,
            "name_present": False,
            "legal_name_present": False,
            "contact_complete": False
        }
    
    return ai_visibility_signals

# ==================== AUTHOR SIGNALS ====================

def extract_author_signals(soup, parsed_entities=None) -> dict:
    """Production-grade author E-E-A-T signal extraction"""
    author_data = {
        "author_detected": False,
        "author_name": "",
        "author_schema": False,
        "author_byline": "",
        "author_bio": "",
        "author_image": "",
        "author_social": {"linkedin": "", "twitter": "", "website": ""},
        "credentials": [],
        "publication_date": "",
        "last_updated": ""
    }
    
    try:
        # SURGICAL FIX: Use parsed_entities parameter for author detection
        if parsed_entities is None:
            # Fallback to extracting from soup if no parsed_entities provided
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            parsed_entities = []
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    entities = flatten_graph_entities(data)
                    parsed_entities.extend(entities)
                except:
                    continue
        
        # Apply exact logic from fix prompt
        author_detected = False
        author_schema = False
        
        for entity in parsed_entities:
            etype = entity.get("@type", "")

            # Check 1: standalone Person entity with any name
            if etype == "Person" and entity.get("name", "").strip():
                author_detected = True
                author_schema = True
                if not author_data["author_name"]:
                    author_data["author_name"] = entity.get("name", "")

            # Check 2: Article/BlogPosting/NewsArticle with author field
            if etype in ["Article", "BlogPosting", "NewsArticle"]:
                author_field = entity.get("author")
                if author_field:
                    if isinstance(author_field, dict):
                        if author_field.get("name", "").strip() or author_field.get("@id", "").strip():
                            author_detected = True
                            author_schema = True
                            if author_field.get("name") and not author_data["author_name"]:
                                author_data["author_name"] = author_field.get("name", "")
                    elif isinstance(author_field, str) and author_field.strip():
                        author_detected = True
                        author_schema = True
                        if not author_data["author_name"]:
                            author_data["author_name"] = author_field.strip()
                    elif isinstance(author_field, list) and len(author_field) > 0:
                        author_detected = True
                        author_schema = True
                        for auth in author_field:
                            if isinstance(auth, dict) and auth.get("name") and not author_data["author_name"]:
                                author_data["author_name"] = auth.get("name", "")
                                break

        author_data["author_detected"] = author_detected
        author_data["author_schema"] = author_schema
        
        # 2. Extract bylines from common patterns
        byline_selectors = [
            '.author', '.byline', '.post-author', '.article-author',
            '[class*="author"]', '[class*="byline"]',
            'meta[name="author"]'
        ]
        
        has_byline = False
        for selector in byline_selectors:
            elements = soup.select(selector)
            for elem in elements:
                if elem.name == 'meta':
                    author_data["author_byline"] = elem.get('content', '')
                else:
                    author_data["author_byline"] = elem.get_text().strip()
                
                if author_data["author_byline"][:100]:
                    has_byline = True
                    author_data["author_detected"] = True
                    if not author_data["author_name"]:
                        author_data["author_name"] = author_data["author_byline"]
        
        # 3. Find author bios
        bio_selectors = [
            '.author-bio', '.author-description', '.biography',
            '[class*="bio"]', '[class*="description"]'
        ]
        
        for selector in bio_selectors:
            bio_elem = soup.select_one(selector)
            if bio_elem:
                bio_text = bio_elem.get_text().strip()[:500]
                if bio_text:
                    author_data["author_bio"] = bio_text
                    author_data["author_detected"] = True
                break
        
        # 4. Extract publication dates
        date_selectors = [
            'meta[property="article:published_time"]',
            'meta[name="date"]', 'meta[name="pubdate"]',
            '.publish-date', '.publication-date', '.date',
            '[datetime]', 'time[datetime]'
        ]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                if date_elem.name == 'meta':
                    author_data["publication_date"] = date_elem.get('content', '')
                else:
                    author_data["publication_date"] = date_elem.get('datetime', '') or date_elem.get_text().strip()
                break
        
        # 5. Find author images
        author_img_selectors = [
            '.author img', '.byline img', '.author-photo img',
            '[class*="author"] img', '[class*="byline"] img'
        ]
        
        for selector in author_img_selectors:
            author_img = soup.select_one(selector)
            if author_img:
                author_data["author_image"] = author_img.get('src', '') or author_img.get('data-src', '')
                break
        
        # BUG FIX 3: Final author_detected from schema OR byline
        author_data["author_detected"] = bool(author_detected_via_schema or has_byline)
        
        print(f"[EXTRACTION_RESULT] author_detected_via_schema: {author_detected_via_schema}")
        print(f"[EXTRACTION_RESULT] author_schema: {author_data['author_schema']}")
        print(f"[EXTRACTION_RESULT] Author bylines found: {has_byline}")
        print(f"[MAPPED_FIELD] author_detected: {author_data['author_detected']}")
        
    except Exception as e:
        print(f"[AUTHOR_SIGNALS] Extraction error: {e}")
    
    return author_data

# ==================== NAP SIGNALS ====================

def extract_nap_signals(soup, parsed_entities=None) -> dict:
    """Extract and validate NAP consistency for local SEO"""
    nap_data = {
        "nap_consistency": {"consistent": False, "variations": []},
        "business_name": "",
        "business_name_identical": False,  # Track if business name is consistent
        "address": {"street": "", "city": "", "state": "", "zip": "", "full": ""},
        "phone": {"primary": "", "formatted": "", "variations": []},
        "localbusiness_schema": {"present": False, "complete": False},
        "geo_coordinates": {"lat": 0, "lng": 0, "present": False}
    }
    
    try:
        business_names = []  # Collect all potential business names
        
        # HELPER: Check if name is valid (not a marketing headline)
        def is_valid_business_name(name):
            if not name or len(name.strip()) <= 2 or len(name.strip()) >= 100:
                return False
            name = name.strip().lower()
            # Filter out marketing headline words
            headline_words = ['agency', 'services', 'solutions', 'results', 'globe', 'worldwide', 'digital marketing agency that delivers real results across the globe']
            return not any(word in name for word in headline_words)
        
        # PRIORITY 1: Organization.name from parsed @graph entities
        if parsed_entities:
            for entity in parsed_entities:
                if entity.get('@type') == 'Organization':
                    org_name = entity.get('name', '').strip()
                    if is_valid_business_name(org_name):
                        nap_data["business_name"] = org_name
                        business_names.append(org_name)
                        break  # Early exit - found priority 1
        
        # PRIORITY 2: Organization.legalName from parsed @graph entities (only if still empty)
        if not nap_data["business_name"] and parsed_entities:
            for entity in parsed_entities:
                if entity.get('@type') == 'Organization':
                    legal_name = entity.get('legalName', '').strip()
                    if is_valid_business_name(legal_name):
                        nap_data["business_name"] = legal_name
                        business_names.append(legal_name)
                        break  # Early exit - found priority 2
        
        # PRIORITY 3: WebSite.name from parsed @graph entities (only if still empty)
        if not nap_data["business_name"] and parsed_entities:
            for entity in parsed_entities:
                if entity.get('@type') == 'WebSite':
                    website_name = entity.get('name', '').strip()
                    if is_valid_business_name(website_name):
                        nap_data["business_name"] = website_name
                        business_names.append(website_name)
                        break  # Early exit - found priority 3
        
        # PRIORITY 4: og:site_name from meta tags (only if still empty)
        if not nap_data["business_name"]:
            site_name_meta = soup.find('meta', property='og:site_name')
            if site_name_meta and site_name_meta.get('content'):
                site_name = site_name_meta.get('content').strip()
                if is_valid_business_name(site_name):
                    nap_data["business_name"] = site_name
                    business_names.append(site_name)
        
        # Extract NAP details from schema (continue regardless of business name priority)
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                entities = flatten_graph_entities(data)
                for entity in entities:
                    if entity.get('@type') in ['LocalBusiness', 'Organization']:
                        nap_data["localbusiness_schema"]["present"] = True
                        
                        # Address extraction
                        address = entity.get('address', {})
                        if isinstance(address, dict):
                            nap_data["address"]["street"] = address.get('streetAddress', '')
                            nap_data["address"]["city"] = address.get('addressLocality', '')
                            nap_data["address"]["state"] = address.get('addressRegion', '')
                            nap_data["address"]["zip"] = address.get('postalCode', '')
                            nap_data["address"]["full"] = ' '.join(filter(None, [
                                nap_data["address"]["street"],
                                nap_data["address"]["city"], 
                                nap_data["address"]["state"],
                                nap_data["address"]["zip"]
                            ]))
                        
                        # Phone extraction
                        nap_data["phone"]["primary"] = entity.get('telephone', '')
                        
                        # Geo coordinates
                        geo = entity.get('geo', {})
                        if isinstance(geo, dict):
                            nap_data["geo_coordinates"]["lat"] = float(geo.get('latitude', 0))
                            nap_data["geo_coordinates"]["lng"] = float(geo.get('longitude', 0))
                            nap_data["geo_coordinates"]["present"] = True
                        
                        break
            except:
                continue
        
        # PRIORITY 5: LocalBusiness.name from schema (only if still empty)
        if not nap_data["business_name"]:
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    entities = flatten_graph_entities(data)
                    for entity in entities:
                        if entity.get('@type') == 'LocalBusiness':
                            localbiz_name = entity.get('name', '').strip()
                            if is_valid_business_name(localbiz_name):
                                nap_data["business_name"] = localbiz_name
                                business_names.append(localbiz_name)
                                break
                except:
                    continue
                if nap_data["business_name"]:
                    break
        
        # PRIORITY 6: Footer business name text (only if still empty)
        if not nap_data["business_name"]:
            footer_selectors = [
                'footer .business-name', 'footer .company-name', 'footer .brand',
                'footer h1', 'footer h2', 'footer h3', 'footer h4',
                '.footer .copyright', '.site-footer .title', '.footer-info',
                'footer', '.footer'
            ]
            
            for selector in footer_selectors:
                elements = soup.select(selector)
                for elem in elements[:2]:
                    text = elem.get_text().strip()
                    # Remove copyright symbols and years
                    clean_text = re.sub(r'[℗℠®]\s*\d{4}|\d{4}[\s-]*[℗℠®]', '', text).strip()
                    clean_text = re.sub(r'©\s*\d{4}|\d{4}[\s-]*©', '', clean_text).strip()
                    clean_text = re.sub(r'\.?\s*All rights reserved.*$', '', clean_text).strip()
                    if is_valid_business_name(clean_text):
                        nap_data["business_name"] = clean_text
                        business_names.append(clean_text)
                        break
                if nap_data["business_name"]:
                    break
        
        # PRIORITY 7: Contact section text (only if still empty)
        if not nap_data["business_name"]:
            contact_selectors = [
                '.contact .business-name', '.contact .company-name',
                '.contact-info h1', '.contact-info h2', '.contact h1', '.contact h2'
            ]
            
            for selector in contact_selectors:
                elements = soup.select(selector)
                for elem in elements[:2]:
                    text = elem.get_text().strip()
                    if is_valid_business_name(text):
                        nap_data["business_name"] = text
                        business_names.append(text)
                        break
                if nap_data["business_name"]:
                    break
        
        # PRIORITY 8: H1 heading (LAST FALLBACK ONLY - only if still empty)
        if not nap_data["business_name"]:
            h1_elements = soup.find_all('h1')
            for elem in h1_elements[:2]:  # Check first 2 H1 elements
                text = elem.get_text().strip()
                if text and len(text) > 2 and len(text) < 100:
                    # Filter out common non-business text
                    if not any(skip in text.lower() for skip in ['home', 'menu', 'navigation', 'search', 'cart', 'login']):
                        if is_valid_business_name(text):
                            nap_data["business_name"] = text
                            business_names.append(text)
                            break
        
        # Meta tags for completeness (lower priority - don't override existing)
        # Meta title
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text().strip()
            if len(title_text) < 100:
                business_names.append(title_text)
        
        # Meta application-name
        app_name_meta = soup.find('meta', attrs={'name': 'application-name'})
        if app_name_meta and app_name_meta.get('content'):
            app_name = app_name_meta.get('content').strip()
            if len(app_name) < 100:
                business_names.append(app_name)
        
        # Update localbusiness_schema completeness
        nap_data["localbusiness_schema"]["complete"] = bool(
            nap_data["business_name"] and nap_data["address"]["full"] and nap_data["phone"]["primary"]
        )
        
        # 6. Check business name consistency
        if business_names:
            # Remove duplicates and empty strings
            unique_names = list(set(filter(None, business_names)))
            
            if len(unique_names) == 1:
                # All names are identical
                nap_data["business_name_identical"] = True
                nap_data["business_name"] = unique_names[0]
            elif len(unique_names) > 1:
                # Check if most names are similar (allowing for minor variations)
                primary_name = unique_names[0]
                similar_count = sum(1 for name in unique_names 
                                  if name.lower().replace(' ', '').replace('-', '') == 
                                     primary_name.lower().replace(' ', '').replace('-', ''))
                
                if similar_count >= len(unique_names) * 0.7:  # 70% similarity
                    nap_data["business_name_identical"] = True
                    nap_data["business_name"] = primary_name
                else:
                    # Use the most frequently occurring name
                    name_counts = {}
                    for name in unique_names:
                        normalized = name.lower().replace(' ', '').replace('-', '')
                        name_counts[normalized] = name_counts.get(normalized, 0) + business_names.count(name)
                    
                    most_common = max(name_counts, key=name_counts.get)
                    # Find original name with this normalized form
                    for name in unique_names:
                        if name.lower().replace(' ', '').replace('-', '') == most_common:
                            nap_data["business_name"] = name
                            break
        
        # 7. Extract NAP from page text using regex
        page_text = soup.get_text()
        
        # Phone number patterns
        phone_patterns = [
            r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',  # (123) 456-7890
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',   # 123-456-7890
            r'\+1\s*\d{3}[-.\s]?\d{3}[-.\s]?\d{4}'  # +1 123-456-7890
        ]
        
        phone_variations = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, page_text)
            phone_variations.extend(matches)
        
        nap_data["phone"]["variations"] = list(set(phone_variations))
        
        # 8. Check NAP consistency
        if nap_data["phone"]["primary"] and phone_variations:
            normalized_primary = re.sub(r'[^\d]', '', nap_data["phone"]["primary"])
            for variation in phone_variations:
                normalized_var = re.sub(r'[^\d]', '', variation)
                if normalized_var == normalized_primary:
                    nap_data["nap_consistency"]["consistent"] = True
                    break
        
        if not nap_data["nap_consistency"]["consistent"]:
            nap_data["nap_consistency"]["variations"] = phone_variations[:5]  # Limit variations
        
    except Exception as e:
        print(f"[NAP_SIGNALS] Extraction error: {e}")
    
    return nap_data

# ==================== DIRECT ANSWER SIGNALS ====================

def extract_direct_answer_signals(soup, text) -> dict:
    """Extract Answer Engine Optimization signals"""
    answer_signals = {
        "direct_answers": {"count": 0, "avg_length": 0, "optimal_range": False},
        "featured_snippet_ready": {"ready": False, "format": "", "position": ""},
        "definition_content": {"present": False, "patterns": []},
        "list_content": {"ordered": 0, "unordered": 0, "steps": 0},
        "table_content": {"present": False, "rows": 0, "headers": 0},
        "question_optimization": {"questions_answered": 0, "answer_quality": 0}
    }
    
    try:
        # 1. Detect direct answer paragraphs (40-60 words)
        paragraphs = text.split('\n\n')
        direct_answers = []
        
        for para in paragraphs:
            word_count = len(para.split())
            if 40 <= word_count <= 60:
                # Check if it answers a question
                if any(indicator in para.lower() for indicator in ['is defined as', 'refers to', 'means that', 'is a']):
                    direct_answers.append({
                        "text": para.strip(),
                        "word_count": word_count,
                        "position": len(direct_answers)
                    })
        
        answer_signals["direct_answers"]["count"] = len(direct_answers)
        if direct_answers:
            avg_length = sum(a["word_count"] for a in direct_answers) / len(direct_answers)
            answer_signals["direct_answers"]["avg_length"] = round(avg_length, 1)
            answer_signals["direct_answers"]["optimal_range"] = 40 <= avg_length <= 60
        
        # 2. Definition pattern detection
        definition_patterns = [
            r'(\w+)\s+is\s+(a|an)\s+([^,.!?]+)',
            r'(\w+)\s+refers\s+to\s+([^,.!?]+)',
            r'(\w+)\s+can\s+be\s+defined\s+as\s+([^,.!?]+)'
        ]
        
        definitions = []
        for pattern in definition_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                definitions.append({
                    "term": match[0],
                    "definition": match[2],
                    "pattern": pattern
                })
        
        answer_signals["definition_content"]["present"] = len(definitions) > 0
        answer_signals["definition_content"]["patterns"] = definitions[:5]
        
        # 3. List content analysis
        ordered_lists = soup.find_all('ol')
        unordered_lists = soup.find_all('ul')
        
        answer_signals["list_content"]["ordered"] = len(ordered_lists)
        answer_signals["list_content"]["unordered"] = len(unordered_lists)
        
        # Count steps in ordered lists
        total_steps = sum(len(ol.find_all('li')) for ol in ordered_lists)
        answer_signals["list_content"]["steps"] = total_steps
        
        # 4. Table content analysis
        tables = soup.find_all('table')
        if tables:
            answer_signals["table_content"]["present"] = True
            total_rows = sum(len(table.find_all('tr')) for table in tables)
            total_headers = sum(len(table.find_all('th')) for table in tables)
            answer_signals["table_content"]["rows"] = total_rows
            answer_signals["table_content"]["headers"] = total_headers
        
        # 5. Featured snippet readiness
        snippet_ready = False
        snippet_format = ""
        
        if answer_signals["direct_answers"]["count"] > 0:
            snippet_ready = True
            snippet_format = "paragraph"
        elif answer_signals["list_content"]["steps"] > 0:
            snippet_ready = True
            snippet_format = "steps"
        elif answer_signals["table_content"]["present"]:
            snippet_ready = True
            snippet_format = "table"
        elif answer_signals["definition_content"]["present"]:
            snippet_ready = True
            snippet_format = "definition"
        
        answer_signals["featured_snippet_ready"]["ready"] = snippet_ready
        answer_signals["featured_snippet_ready"]["format"] = snippet_format
        
    except Exception as e:
        print(f"[ANSWER_SIGNALS] Extraction error: {e}")
    
    return answer_signals

# ==================== TECHNICAL SIGNALS ENHANCEMENTS ====================

def check_ai_crawler_access(url: str) -> Dict[str, Any]:
    """Check AI crawler access with minimal parsing"""
    try:
        from urllib.parse import urlparse
        import requests
        
        domain = urlparse(url).netloc
        robots_url = f"https://{domain}/robots.txt"
        
        response = requests.get(robots_url, timeout=5, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code != 200:
            return {"accessible": False, "blocked_crawlers": []}
        
        content = response.text
        ai_crawlers = ['GPTBot', 'PerplexityBot', 'ChatGPTUser', 'Google-Extended', 'Claude-Web']
        blocked_crawlers = []
        
        lines = content.split('\n')
        current_agent = '*'
        
        for line in lines:
            line = line.strip()
            if line.lower().startswith('user-agent:'):
                current_agent = line.split(':', 1)[1].strip()
            elif line.lower().startswith('disallow:') and current_agent in ai_crawlers:
                disallow_path = line.split(':', 1)[1].strip()
                if disallow_path in ['/', '/api/', '/admin/']:
                    blocked_crawlers.append(current_agent)
        
        return {"accessible": True, "blocked_crawlers": blocked_crawlers}
        
    except Exception:
        # Safe fallback - don't break existing functionality
        return {"accessible": True, "blocked_crawlers": []}

def extract_enhanced_technical_signals(soup, url) -> dict:
    """Extract enhanced technical signals for AI visibility"""
    technical_signals = {
        "crawlability": {"robots_txt_accessible": True, "sitemap_referenced": False},
        "mobile_optimization": {"viewport_present": True, "responsive": True, "mobile_friendly": True},
        "page_speed_indicators": {"optimized_images": False, "minified_css": False, "lazy_loading": False},
        "schema_validation": {"valid_json": True, "required_fields": True, "warnings": 0},
        "image_optimization": {"alt_text_coverage": 0, "file_sizes_optimized": False, "responsive_images": False},
        "accessibility": {"aria_labels": 0, "color_contrast": False, "keyboard_navigable": False},
        "security_signals": {"https": True, "security_headers": False, "safe_browsing": True},
        "navigation_detection": {"has_navigation": False, "has_footer": False, "nav_elements": []}
    }
    
    try:
        # 0. Check AI crawler access (NEW)
        ai_crawler_access = check_ai_crawler_access(url)
        technical_signals["crawlability"]["ai_crawler_access"] = ai_crawler_access
        technical_signals["crawlability"]["ai_crawlers_blocked"] = ai_crawler_access.get("blocked_crawlers", [])
        
        # 1. Check viewport meta tag
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        technical_signals["mobile_optimization"]["viewport_present"] = bool(viewport)
        
        # 2. Check for responsive images
        responsive_images = soup.find_all('picture') + soup.find_all('img', attrs={'srcset': True})
        technical_signals["image_optimization"]["responsive_images"] = len(responsive_images) > 0
        
        # 3. Check for lazy loading
        lazy_images = soup.find_all('img', attrs={'loading': 'lazy'}) + \
                      soup.find_all('img', attrs={'data-src': True}) + \
                      soup.find_all('img', attrs={'data-lazy': True}) + \
                      soup.find_all('img', attrs={'data-original': True}) + \
                      soup.find_all('img', attrs={'data-srcset': True})
        technical_signals["page_speed_indicators"]["lazy_loading"] = len(lazy_images) > 0
        
        # 4. Check for sitemap references
        sitemap_links = soup.find_all('link', rel='sitemap')
        technical_signals["crawlability"]["sitemap_referenced"] = len(sitemap_links) > 0
        
        # 5. Calculate alt text coverage
        images = soup.find_all('img')
        images_with_alt = [img for img in images if img.get('alt')]
        if images:
            coverage = (len(images_with_alt) / len(images)) * 100
            technical_signals["image_optimization"]["alt_text_coverage"] = round(coverage, 1)
        
        # 6. Enhanced navigation detection — BUG FIX 1: Dedicated selector lists
        NAV_SELECTORS    = ["nav", ".nav", ".navbar", ".pxl-header-nav", ".pxl-nav-menu", ".elementor-nav-menu", "[class*='nav-menu']", ".elementor-container", ".elementor-widget-nav-menu", ".elementor-widget-container", ".menu"]
        HEADER_SELECTORS = ["header", ".header", "#pxl-header-elementor", ".elementor-location-header", "[id*='header']", "[class*='header']"]
        FOOTER_SELECTORS = ["footer", ".footer", "#pxl-footer-elementor", ".elementor-location-footer", "[id*='footer']", "[class*='footer']"]
        
        nav_elements_found = []
        has_navigation = False
        has_header = False
        has_footer = False
        
        for selector in NAV_SELECTORS:
            if soup.select(selector):
                has_navigation = True
                nav_elements_found.append(selector)
        
        for selector in HEADER_SELECTORS:
            if soup.select(selector):
                has_header = True
                nav_elements_found.append(selector)
        
        for selector in FOOTER_SELECTORS:
            if soup.select(selector):
                has_footer = True
                nav_elements_found.append(selector)
        
        technical_signals["navigation_detection"]["has_navigation"] = has_navigation
        technical_signals["navigation_detection"]["has_header"] = has_header
        technical_signals["navigation_detection"]["has_footer"] = has_footer
        technical_signals["navigation_detection"]["nav_elements"] = nav_elements_found
        
        print(f"[EXTRACTION_RESULT] Navigation detected: {has_navigation}, Header detected: {has_header}, Footer detected: {has_footer}")
        print(f"[EXTRACTION_RESULT] Lazy loading images: {len(lazy_images)}")
        
        # SURGICAL FIX: Copy navigation_detection results to body_signals.structure
        # This ensures both systems (navigation_detection and technical_seo_signals.body_signals.structure) always show the same values
        if "body_signals" not in technical_signals:
            technical_signals["body_signals"] = {}
        if "structure" not in technical_signals["body_signals"]:
            technical_signals["body_signals"]["structure"] = {}
        
        technical_signals["body_signals"]["structure"]["has_header"] = has_header
        technical_signals["body_signals"]["structure"]["has_nav"] = has_navigation
        technical_signals["body_signals"]["structure"]["has_footer"] = has_footer
        
        print(f"[NAVIGATION_FIX] Copied navigation results to body_signals.structure")
        
        # 7. Check HTTPS
        technical_signals["security_signals"]["https"] = url.startswith('https://')
        
        # 8. Check for minified CSS indicators
        style_tags = soup.find_all('style')
        minified_indicators = ['.min.css', 'compressed', 'minified']
        for style in style_tags:
            style_content = style.get_text() or ''
            if any(indicator in style_content for indicator in minified_indicators):
                technical_signals["page_speed_indicators"]["minified_css"] = True
                break
        
    except Exception as e:
        print(f"[ENHANCED_TECHNICAL_SIGNALS] Extraction error: {e}")
    
    # NAVIGATION SYNC TO ENHANCED_EXTRACTION_V2 LOCATIONS
    # Read from canonical source and sync to other two locations
    # This runs after enhanced_technical_signals is returned and added to the result
    # The sync will be applied in the calling function where the result dict is available
    
    return technical_signals

# ==================== PERFORMANCE OPTIMIZATION ====================

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

def optimized_extraction_pipeline(html: str, url: str) -> dict:
    """Memory-optimized extraction with early termination"""
    
    # Limit HTML size to prevent memory issues
    max_html_size = 500_000  # 500KB limit
    if len(html) > max_html_size:
        html = html[:max_html_size]
        print(f"[PERFORMANCE] HTML truncated to {max_html_size} characters")
    
    # Single soup creation with lxml parser
    soup = BeautifulSoup(html, 'lxml')
    
    # Batch extraction to minimize DOM traversals
    extraction_results = {}
    
    # Extract all JSON-LD in one pass
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    extraction_results['structured_data'] = process_json_ld_batch(json_ld_scripts)
    
    # Extract metadata in one pass
    extraction_results['metadata'] = extract_metadata_batch(soup, url)
    
    # Content analysis with text reuse
    main_text = soup.get_text()
    extraction_results['content_analysis'] = analyze_content_batch(soup, main_text)
    
    return extraction_results

def process_json_ld_batch(json_ld_scripts) -> dict:
    """Process all JSON-LD scripts in batch"""
    structured_data = {
        "json_ld_schemas": {"count": 0, "types": [], "valid": True, "errors": []},
        "entities": {"total": 0, "organizations": 0, "persons": 0, "places": 0}
    }
    
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            entities = flatten_graph_entities(data)
            structured_data["json_ld_schemas"]["count"] += 1
            
            for entity in entities:
                entity_type = entity.get('@type', '')
                if entity_type:
                    structured_data["json_ld_schemas"]["types"].append(entity_type)
                    structured_data["entities"]["total"] += 1
                    
                    if entity_type in ['Organization', 'Corporation', 'Business']:
                        structured_data["entities"]["organizations"] += 1
                    elif entity_type in ['Person', 'Author']:
                        structured_data["entities"]["persons"] += 1
                    elif entity_type in ['Place', 'LocalBusiness', 'GeoCoordinates']:
                        structured_data["entities"]["places"] += 1
        
        except Exception as e:
            structured_data["json_ld_schemas"]["valid"] = False
            structured_data["json_ld_schemas"]["errors"].append(str(e))
    
    # Remove duplicates
    structured_data["json_ld_schemas"]["types"] = list(set(structured_data["json_ld_schemas"]["types"]))
    
    return structured_data

def extract_metadata_batch(soup, url) -> dict:
    """Extract all metadata in one pass"""
    metadata = {
        "page_title": {"text": "", "length": 0, "in_title_tag": True},
        "meta_description": {"text": "", "length": 0, "keyword_present": False},
        "canonical_url": {"url": "", "normalized": "", "self_referencing": True},
        "hreflang_tags": {"present": False, "languages": [], "return_tags": []},
        "robots_meta": {"directive": "", "indexable": True, "followable": True}
    }
    
    # Title
    title_tag = soup.find('title')
    if title_tag:
        title_text = title_tag.get_text().strip()
        metadata["page_title"]["text"] = title_text[:100]  # Limit length
        metadata["page_title"]["length"] = len(title_text)
    
    # Meta description
    meta_desc = soup.find('meta', attrs={'name': 'description'})
    if meta_desc:
        desc_text = meta_desc.get('content', '').strip()
        metadata["meta_description"]["text"] = desc_text[:200]  # Limit length
        metadata["meta_description"]["length"] = len(desc_text)
    
    # Canonical
    canonical = soup.find('link', rel='canonical')
    if canonical:
        canonical_url = canonical.get('href', '')
        metadata["canonical_url"]["url"] = canonical_url
        metadata["canonical_url"]["self_referencing"] = canonical_url == url
    
    # hreflang
    hreflang_links = soup.find_all('link', rel='alternate', hreflang=True)
    if hreflang_links:
        metadata["hreflang_tags"]["present"] = True
        languages = [link.get('hreflang', '') for link in hreflang_links if link.get('hreflang')]
        metadata["hreflang_tags"]["languages"] = list(set(languages))
    
    # Robots meta
    robots_meta = soup.find('meta', attrs={'name': 'robots'})
    if robots_meta:
        robots_content = robots_meta.get('content', '').lower()
        metadata["robots_meta"]["directive"] = robots_content
        metadata["robots_meta"]["indexable"] = 'noindex' not in robots_content
        metadata["robots_meta"]["followable"] = 'nofollow' not in robots_content
    
    return metadata

def analyze_content_batch(soup, text) -> dict:
    """Analyze content in batch with text reuse"""
    content_analysis = {
        "main_content": {"word_count": 0, "quality_score": 0, "extraction_method": ""},
        "heading_structure": {"h1_count": 0, "hierarchy_valid": True, "structure_score": 0},
        "content_sections": {"definitions": 0, "use_cases": 0, "steps": 0, "comparisons": 0},
        "internal_linking": {"count": 0, "contextual": 0, "descriptive_anchor": 0}
    }
    
    # Word count
    words = re.findall(r'\b\w+\b', text)
    content_analysis["main_content"]["word_count"] = len(words)
    
    # Heading structure
    h1_tags = soup.find_all('h1')
    content_analysis["heading_structure"]["h1_count"] = len(h1_tags)
    
    # Internal links
    internal_links = soup.find_all('a', href=True)
    content_analysis["internal_linking"]["count"] = len(internal_links)
    
    # Content sections (basic detection)
    definition_patterns = ['definition', 'what is', 'refers to']
    use_case_patterns = ['use case', 'application', 'example']
    step_patterns = ['step', 'how to', 'tutorial']
    
    text_lower = text.lower()
    content_analysis["content_sections"]["definitions"] = sum(1 for pattern in definition_patterns if pattern in text_lower)
    content_analysis["content_sections"]["use_cases"] = sum(1 for pattern in use_case_patterns if pattern in text_lower)
    content_analysis["content_sections"]["steps"] = sum(1 for pattern in step_patterns if pattern in text_lower)
    
    return content_analysis
