"""Enhanced metadata extraction for comprehensive page analysis."""

from typing import Dict, List, Any, Optional
import re
from urllib.parse import urlparse

class MetadataEnhancer:
    """Extract comprehensive metadata from HTML and structured data."""
    
    def __init__(self, soup, url: str):
        self.soup = soup
        self.url = url
        self.parsed_url = urlparse(url)
        
    def extract_all_metadata(self) -> Dict[str, Any]:
        """Extract all metadata signals."""
        return {
            'page_metadata': {
                **self._extract_basic_metadata(),
                **self._extract_social_metadata(),
                **self._extract_technical_metadata(),
                **self._extract_heading_structure(),
                **self._extract_content_metrics()
            }
        }
    
    def _extract_basic_metadata(self) -> Dict[str, Any]:
        """Extract basic page metadata."""
        # Title
        title_tag = self.soup.find('title')
        title = title_tag.get_text().strip() if title_tag else ''
        
        # Meta description
        meta_desc = self.soup.find('meta', attrs={'name': 'description'})
        meta_description = meta_desc.get('content', '') if meta_desc else ''
        
        # Meta keywords
        meta_keywords = self.soup.find('meta', attrs={'name': 'keywords'})
        keywords = meta_keywords.get('content', '') if meta_keywords else ''
        
        # Canonical URL
        canonical_links = self.soup.find_all('link', rel='canonical')
        canonical_urls = [link.get('href', '') for link in canonical_links if link.get('href')]
        canonical_url = canonical_urls[0] if canonical_urls else ''
        
        # Normalize URLs for comparison
        canonical_matches = self._normalize_url(canonical_url) == self._normalize_url(self.url) if canonical_url else False
        
        return {
            'title': title,
            'title_length': len(title),
            'title_present': bool(title),
            'meta_description': meta_description,
            'meta_description_length': len(meta_description),
            'meta_description_present': bool(meta_description),
            'meta_keywords': keywords,
            'keywords_present': bool(keywords),
            'canonical_url': canonical_url,
            'canonical_present': bool(canonical_url),
            'canonical_matches_url': canonical_matches,
            'multiple_canonical_detected': len(canonical_urls) > 1,
            'canonical_urls': canonical_urls
        }
    
    def _extract_social_metadata(self) -> Dict[str, Any]:
        """Extract social media metadata (OpenGraph, Twitter Cards)."""
        # OpenGraph tags
        og_tags = {}
        og_meta = self.soup.find_all('meta', attrs={'property': lambda x: x and x.startswith('og:')})
        for meta in og_meta:
            property_name = meta.get('property', '')
            content = meta.get('content', '')
            if property_name and content:
                og_tags[property_name] = content
        
        # Twitter Card tags
        twitter_tags = {}
        twitter_meta = self.soup.find_all('meta', attrs={'name': lambda x: x and x.startswith('twitter:')})
        for meta in twitter_meta:
            name = meta.get('name', '')
            content = meta.get('content', '')
            if name and content:
                twitter_tags[name] = content
        
        return {
            'opengraph': {
                'present': len(og_tags) > 0,
                'tags': og_tags,
                'title': og_tags.get('og:title', ''),
                'description': og_tags.get('og:description', ''),
                'image': og_tags.get('og:image', ''),
                'type': og_tags.get('og:type', ''),
                'url': og_tags.get('og:url', ''),
                'site_name': og_tags.get('og:site_name', ''),
                'locale': og_tags.get('og:locale', '')
            },
            'twitter_card': {
                'present': len(twitter_tags) > 0,
                'tags': twitter_tags,
                'card': twitter_tags.get('twitter:card', ''),
                'title': twitter_tags.get('twitter:title', ''),
                'description': twitter_tags.get('twitter:description', ''),
                'image': twitter_tags.get('twitter:image', ''),
                'site': twitter_tags.get('twitter:site', ''),
                'creator': twitter_tags.get('twitter:creator', '')
            }
        }
    
    def _extract_technical_metadata(self) -> Dict[str, Any]:
        """Extract technical metadata."""
        # Robots meta
        robots_meta = self.soup.find('meta', attrs={'name': 'robots'})
        robots_content = robots_meta.get('content', '') if robots_meta else ''
        
        # Hreflang tags
        hreflang_tags = []
        hreflang_links = self.soup.find_all('link', attrs={'rel': 'alternate', 'hreflang': True})
        for link in hreflang_links:
            hreflang_tags.append({
                'hreflang': link.get('hreflang', ''),
                'href': link.get('href', '')
            })
        
        # Language detection
        html_tag = self.soup.find('html')
        lang_attr = html_tag.get('lang', '') if html_tag else ''
        
        # Viewport meta
        viewport_meta = self.soup.find('meta', attrs={'name': 'viewport'})
        viewport_content = viewport_meta.get('content', '') if viewport_meta else ''
        
        # Content type
        content_type_meta = self.soup.find('meta', attrs={'http-equiv': 'content-type'})
        content_type = content_type_meta.get('content', '') if content_type_meta else ''
        
        # Generator
        generator_meta = self.soup.find('meta', attrs={'name': 'generator'})
        generator = generator_meta.get('content', '') if generator_meta else ''
        
        return {
            'robots': {
                'present': bool(robots_content),
                'content': robots_content,
                'noindex': 'noindex' in robots_content.lower(),
                'nofollow': 'nofollow' in robots_content.lower(),
                'noarchive': 'noarchive' in robots_content.lower()
            },
            'hreflang': {
                'present': len(hreflang_tags) > 0,
                'tags': hreflang_tags,
                'count': len(hreflang_tags)
            },
            'language': {
                'html_lang': lang_attr,
                'lang_present': bool(lang_attr)
            },
            'viewport': {
                'present': bool(viewport_content),
                'content': viewport_content
            },
            'content_type': {
                'present': bool(content_type),
                'content': content_type
            },
            'generator': {
                'present': bool(generator),
                'content': generator
            }
        }
    
    def _extract_heading_structure(self) -> Dict[str, Any]:
        """Extract heading hierarchy and counts."""
        headings = {
            'h1': [],
            'h2': [],
            'h3': [],
            'h4': [],
            'h5': [],
            'h6': []
        }
        
        # Extract all headings
        for level in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            heading_elements = self.soup.find_all(level)
            for heading in heading_elements:
                headings[level].append({
                    'text': heading.get_text().strip(),
                    'id': heading.get('id', ''),
                    'class': heading.get('class', []),
                    'position': len(headings[level])
                })
        
        # Analyze hierarchy
        hierarchy_issues = []
        previous_level = 0
        
        for level in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            level_num = int(level[1])
            if headings[level] and previous_level > 0 and level_num > previous_level + 1:
                hierarchy_issues.append(f"Skipped heading level: H{previous_level} to H{level_num}")
            if headings[level]:
                previous_level = level_num
        
        return {
            'counts': {
                'h1_count': len(headings['h1']),
                'h2_count': len(headings['h2']),
                'h3_count': len(headings['h3']),
                'h4_count': len(headings['h4']),
                'h5_count': len(headings['h5']),
                'h6_count': len(headings['h6']),
                'total_headings': sum(len(headings[level]) for level in headings)
            },
            'headings': headings,
            'hierarchy_issues': hierarchy_issues,
            'has_h1': len(headings['h1']) > 0,
            'multiple_h1': len(headings['h1']) > 1,
            'proper_hierarchy': len(hierarchy_issues) == 0
        }
    
    def _extract_content_metrics(self) -> Dict[str, Any]:
        """Extract content structure metrics."""
        # BUG FIX 1: Detect navigation/header/footer BEFORE decomposing elements
        NAV_SELECTORS    = ["nav", ".nav", ".navbar", ".pxl-header-nav", ".pxl-nav-menu", ".elementor-nav-menu", "[class*='nav-menu']"]
        HEADER_SELECTORS = ["header", ".header", "#pxl-header-elementor", ".elementor-location-header", "[id*='header']", "[class*='header']"]
        FOOTER_SELECTORS = ["footer", ".footer", "#pxl-footer-elementor", ".elementor-location-footer", "[id*='footer']", "[class*='footer']"]
        
        has_navigation = any(self.soup.select(s) for s in NAV_SELECTORS)
        has_header = any(self.soup.select(s) for s in HEADER_SELECTORS)
        has_footer = any(self.soup.select(s) for s in FOOTER_SELECTORS)
        has_main = bool(self.soup.find('main'))
        has_article = bool(self.soup.find('article'))
        has_section = bool(self.soup.find('section'))
        has_aside = bool(self.soup.find('aside'))
        
        # Remove script and style elements
        for element in self.soup(['script', 'style', 'nav', 'footer', 'header']):
            element.decompose()
        
        # Get clean text
        text = self.soup.get_text()
        
        # Basic metrics
        words = text.split()
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        paragraphs = self.soup.find_all('p')
        
        # Links
        links = self.soup.find_all('a', href=True)
        
        # Images
        images = self.soup.find_all('img')
        
        # Lists
        lists = self.soup.find_all(['ul', 'ol'])
        
        # Tables
        tables = self.soup.find_all('table')
        
        # Forms
        forms = self.soup.find_all('form')
        
        return {
            'content_metrics': {
                'word_count': len(words),
                'character_count': len(text),
                'sentence_count': len(sentences),
                'paragraph_count': len(paragraphs),
                'average_sentence_length': len(words) // len(sentences) if sentences else 0,
                'average_paragraph_length': len(words) // len(paragraphs) if paragraphs else 0,
                'readability_score': self._calculate_readability_score(words, sentences)
            },
            'structure_metrics': {
                'link_count': len(links),
                'image_count': len(images),
                'list_count': len(lists),
                'table_count': len(tables),
                'form_count': len(forms),
                'has_navigation': has_navigation,
                'has_header': has_header,
                'has_footer': has_footer,
                'has_main': has_main,
                'has_article': has_article,
                'has_section': has_section,
                'has_aside': has_aside
            }
        }
    
    def _calculate_readability_score(self, words: List[str], sentences: List[str]) -> float:
        """Calculate simple readability score (Flesch-like)."""
        if not sentences or not words:
            return 0.0
            
        avg_sentence_length = len(words) / len(sentences)
        
        # Simple readability score (higher = easier to read)
        # Based on average sentence length
        if avg_sentence_length <= 15:
            return 100.0
        elif avg_sentence_length <= 20:
            return 80.0
        elif avg_sentence_length <= 25:
            return 60.0
        else:
            return max(20.0, 100.0 - (avg_sentence_length - 25) * 2)
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for comparison."""
        if not url:
            return ''
            
        parsed = urlparse(url)
        # Reconstruct without fragment and with consistent scheme
        normalized = f"https://{parsed.netloc}{parsed.path}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        return normalized.lower()
