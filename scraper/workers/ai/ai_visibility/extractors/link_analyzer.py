"""Comprehensive link analysis and extraction."""

from typing import Dict, List, Any, Set, Optional
import re
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup

class LinkAnalyzer:
    """Extract and analyze all links on the page."""
    
    def __init__(self, soup, base_url: str):
        self.soup = soup
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc.lower()
        self.base_scheme = urlparse(base_url).scheme
        
    def extract_all_links(self) -> Dict[str, Any]:
        """Extract comprehensive link analysis."""
        all_links = self._extract_raw_links()
        
        return {
            'link_analysis': {
                **self._classify_links(all_links),
                **self._analyze_link_quality(all_links),
                **self._extract_anchor_text_analysis(all_links),
                **self._detect_link_patterns(all_links)
            }
        }
    
    def _extract_raw_links(self) -> List[Dict[str, Any]]:
        """Extract all links with full metadata."""
        links = []
        link_elements = self.soup.find_all('a', href=True)
        
        for i, link in enumerate(link_elements):
            href = link.get('href', '').strip()
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:', 'ftp:')):
                continue
                
            # Normalize URL
            normalized_url = self._normalize_url(href)
            
            link_data = {
                'index': i,
                'raw_href': href,
                'normalized_url': normalized_url,
                'anchor_text': link.get_text().strip(),
                'title': link.get('title', ''),
                'target': link.get('target', ''),
                'rel': link.get('rel', []),
                'class': link.get('class', []),
                'id': link.get('id', ''),
                'parent_tag': link.parent.name if link.parent else None,
                'is_nofollow': self._is_nofollow(link),
                'is_sponsored': self._is_sponsored(link),
                'is_ugc': self._is_ugc(link)
            }
            
            links.append(link_data)
        
        return links
    
    def _classify_links(self, links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Classify links by type and analyze distribution."""
        internal_links = []
        external_links = []
        fragment_links = []
        relative_links = []
        mailto_links = []
        tel_links = []
        
        internal_domains = set()
        external_domains = set()
        
        for link in links:
            url = link['normalized_url']
            parsed = urlparse(url)
            
            if url.startswith('#'):
                fragment_links.append(link)
            elif url.startswith('mailto:'):
                mailto_links.append(link)
            elif url.startswith('tel:'):
                tel_links.append(link)
            elif not parsed.netloc:
                # Relative link
                relative_links.append(link)
                internal_links.append(link)
            elif parsed.netloc.lower() == self.base_domain:
                # Internal link
                internal_links.append(link)
                internal_domains.add(parsed.netloc.lower())
            else:
                # External link
                external_links.append(link)
                external_domains.add(parsed.netloc.lower())
        
        # Analyze internal link structure
        internal_link_analysis = self._analyze_internal_links(internal_links)
        
        # Analyze external link structure
        external_link_analysis = self._analyze_external_links(external_links)
        
        return {
            'classification': {
                'total_links': len(links),
                'internal_links': len(internal_links),
                'external_links': len(external_links),
                'fragment_links': len(fragment_links),
                'relative_links': len(relative_links),
                'mailto_links': len(mailto_links),
                'tel_links': len(tel_links),
                'internal_domains': list(internal_domains),
                'external_domains': list(external_domains),
                'internal_link_urls': [link['normalized_url'] for link in internal_links],
                'external_link_urls': [link['normalized_url'] for link in external_links]
            },
            'internal_analysis': internal_link_analysis,
            'external_analysis': external_link_analysis
        }
    
    def _analyze_internal_links(self, internal_links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze internal link patterns."""
        # Group by path patterns
        path_patterns = {}
        for link in internal_links:
            parsed = urlparse(link['normalized_url'])
            path = parsed.path
            
            # Extract pattern (first two path segments)
            path_parts = [p for p in path.split('/') if p][:2]
            pattern = '/' + '/'.join(path_parts) if path_parts else '/'
            
            if pattern not in path_patterns:
                path_patterns[pattern] = []
            path_patterns[pattern].append(link)
        
        # Find duplicate internal links
        url_counts = {}
        duplicates = []
        for link in internal_links:
            url = link['normalized_url']
            url_counts[url] = url_counts.get(url, 0) + 1
            if url_counts[url] == 2:  # First time we detect duplicate
                duplicates.append(url)
        
        # Analyze anchor text quality
        anchor_text_analysis = self._analyze_anchor_text_quality(internal_links)
        
        return {
            'path_distribution': {pattern: len(links) for pattern, links in path_patterns.items()},
            'duplicate_urls': duplicates,
            'duplicate_count': len(duplicates),
            'unique_internal_links': len(internal_links) - len(duplicates),
            'anchor_text_quality': anchor_text_analysis,
            'links_with_nofollow': len([l for l in internal_links if l['is_nofollow']]),
            'links_in_navigation': self._count_navigation_links(internal_links)
        }
    
    def _analyze_external_links(self, external_links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze external link patterns."""
        # Group by domain
        domain_analysis = {}
        for link in external_links:
            parsed = urlparse(link['normalized_url'])
            domain = parsed.netloc.lower()
            
            if domain not in domain_analysis:
                domain_analysis[domain] = {
                    'count': 0,
                    'links': [],
                    'has_nofollow': False,
                    'has_sponsored': False
                }
            
            domain_analysis[domain]['count'] += 1
            domain_analysis[domain]['links'].append(link)
            if link['is_nofollow']:
                domain_analysis[domain]['has_nofollow'] = True
            if link['is_sponsored']:
                domain_analysis[domain]['has_sponsored'] = True
        
        # Identify common external domains
        common_domains = sorted(domain_analysis.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        
        # Check for broken external links (basic check)
        broken_indicators = ['404', 'error', 'not-found', 'dead', 'broken']
        potentially_broken = []
        
        for link in external_links:
            anchor_lower = link['anchor_text'].lower()
            url_lower = link['normalized_url'].lower()
            
            for indicator in broken_indicators:
                if indicator in anchor_lower or indicator in url_lower:
                    potentially_broken.append(link)
                    break
        
        # Detect content citations - external links with reference keywords
        citation_keywords = ['source', 'study', 'research', 'report']
        reference_links = []

        for link in external_links:
            anchor_lower = link['anchor_text'].lower()
            if any(keyword in anchor_lower for keyword in citation_keywords):
                reference_links.append(link)

        return {
            'domain_distribution': {domain: data['count'] for domain, data in domain_analysis.items()},
            'unique_external_domains': len(domain_analysis),
            'top_external_domains': [(domain, data['count']) for domain, data in common_domains],
            'domains_with_nofollow': len([d for d, data in domain_analysis.items() if data['has_nofollow']]),
            'domains_with_sponsored': len([d for d, data in domain_analysis.items() if data['has_sponsored']]),
            'potentially_broken_links': potentially_broken,
            'potentially_broken_count': len(potentially_broken),
            'secure_external_links': len([l for l in external_links if l['normalized_url'].startswith('https://')]),
            'http_external_links': len([l for l in external_links if l['normalized_url'].startswith('http://')]),
            'content_citations': {
                'content_cites_sources': len(reference_links) > 0,
                'source_links_count': len(reference_links),
                'reference_links': reference_links[:5]  # Limit storage
            }
        }
    
    def _analyze_link_quality(self, links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze overall link quality metrics."""
        # Links with descriptive anchor text
        descriptive_anchors = []
        generic_anchors = []
        
        generic_phrases = [
            'click here', 'read more', 'learn more', 'here', 'more',
            'link', 'page', 'website', 'site', 'url', 'download'
        ]
        
        for link in links:
            anchor = link['anchor_text'].lower().strip()
            if len(anchor) < 3:
                generic_anchors.append(link)
            elif any(phrase in anchor for phrase in generic_phrases):
                generic_anchors.append(link)
            else:
                descriptive_anchors.append(link)
        
        # Links with accessibility attributes
        accessible_links = []
        for link in links:
            if (link.get('title') or 
                link.get('aria-label') or 
                'aria-describedby' in str(link.get('attributes', {}))):
                accessible_links.append(link)
        
        return {
            'quality_metrics': {
                'descriptive_anchor_links': len(descriptive_anchors),
                'generic_anchor_links': len(generic_anchors),
                'descriptive_anchor_percentage': (len(descriptive_anchors) / len(links) * 100) if links else 0,
                'accessible_links': len(accessible_links),
                'accessible_percentage': (len(accessible_links) / len(links) * 100) if links else 0,
                'links_with_rel_attributes': len([l for l in links if l['rel']]),
                'links_with_target_blank': len([l for l in links if l['target'] == '_blank'])
            }
        }
    
    def _extract_anchor_text_analysis(self, links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze anchor text patterns."""
        anchor_texts = [link['anchor_text'] for link in links if link['anchor_text'].strip()]
        
        if not anchor_texts:
            return {
                'anchor_text_analysis': {
                    'total_with_text': 0,
                    'average_length': 0,
                    'short_anchors': 0,
                    'long_anchors': 0,
                    'empty_anchors': len(links) - len(anchor_texts)
                }
            }
        
        lengths = [len(text) for text in anchor_texts]
        avg_length = sum(lengths) / len(lengths)
        
        short_anchors = [text for text in anchor_texts if len(text) <= 3]
        long_anchors = [text for text in anchor_texts if len(text) > 50]
        
        return {
            'anchor_text_analysis': {
                'total_with_text': len(anchor_texts),
                'average_length': round(avg_length, 1),
                'short_anchors': len(short_anchors),
                'long_anchors': len(long_anchors),
                'empty_anchors': len(links) - len(anchor_texts),
                'short_anchor_percentage': (len(short_anchors) / len(links) * 100) if links else 0,
                'long_anchor_percentage': (len(long_anchors) / len(links) * 100) if links else 0
            }
        }
    
    def _detect_link_patterns(self, links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Detect common link patterns and structures."""
        # Navigation links
        nav_links = []
        nav_selectors = ['nav a', '.nav a', '.navigation a', '.menu a', '[role="navigation"] a']
        
        for selector in nav_selectors:
            try:
                nav_elements = self.soup.select(selector)
                for nav_element in nav_links:
                    if nav_element.get('href'):
                        nav_links.append(nav_element.get('href'))
            except:
                continue
        
        # Footer links
        footer_links = []
        footer_selectors = ['footer a', '.footer a', '[role="contentinfo"] a']
        
        for selector in footer_selectors:
            try:
                footer_elements = self.soup.select(selector)
                for footer_element in footer_links:
                    if footer_element.get('href'):
                        footer_links.append(footer_element.get('href'))
            except:
                continue
        
        # Breadcrumb links
        breadcrumb_links = []
        breadcrumb_selectors = ['.breadcrumb a', '.breadcrumbs a', '[aria-label="breadcrumb"] a']
        
        for selector in breadcrumb_selectors:
            try:
                breadcrumb_elements = self.soup.select(selector)
                for breadcrumb_element in breadcrumb_links:
                    if breadcrumb_element.get('href'):
                        breadcrumb_links.append(breadcrumb_element.get('href'))
            except:
                continue
        
        # Pagination links
        pagination_patterns = ['page', 'p=', 'pg=', '/page/', 'next', 'prev', 'previous']
        pagination_links = []
        
        for link in links:
            url_lower = link['normalized_url'].lower()
            if any(pattern in url_lower for pattern in pagination_patterns):
                pagination_links.append(link)
        
        return {
            'structural_patterns': {
                'navigation_links': len(nav_links),
                'footer_links': len(footer_links),
                'breadcrumb_links': len(breadcrumb_links),
                'pagination_links': len(pagination_links),
                'has_navigation': len(nav_links) > 0,
                'has_footer': len(footer_links) > 0,
                'has_breadcrumbs': len(breadcrumb_links) > 0,
                'has_pagination': len(pagination_links) > 0
            }
        }
    
    def _normalize_url(self, href: str) -> str:
        """Normalize URL to absolute form."""
        if not href:
            return ''
            
        # Remove fragment for normalization
        if '#' in href:
            href = href.split('#')[0]
        
        # Convert to absolute URL
        if href.startswith(('http://', 'https://')):
            return href
        elif href.startswith('//'):
            return f"{self.base_scheme}:{href}"
        else:
            return urljoin(self.base_url, href)
    
    def _is_nofollow(self, link) -> bool:
        """Check if link has nofollow attribute."""
        rel = link.get('rel', [])
        if isinstance(rel, str):
            rel = [rel]
        return 'nofollow' in [r.lower() for r in rel]
    
    def _is_sponsored(self, link) -> bool:
        """Check if link has sponsored attribute."""
        rel = link.get('rel', [])
        if isinstance(rel, str):
            rel = [rel]
        return 'sponsored' in [r.lower() for r in rel]
    
    def _is_ugc(self, link) -> bool:
        """Check if link has ugc (user-generated content) attribute."""
        rel = link.get('rel', [])
        if isinstance(rel, str):
            rel = [rel]
        return 'ugc' in [r.lower() for r in rel]
    
    def _analyze_anchor_text_quality(self, links: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze anchor text quality for internal links."""
        if not links:
            return {'average_length': 0, 'descriptive_count': 0, 'generic_count': 0}
        
        total_length = sum(len(link['anchor_text']) for link in links)
        avg_length = total_length / len(links)
        
        generic_patterns = [
            r'^click here$', r'^read more$', r'^learn more$', r'^here$',
            r'^more$', r'^link$', r'^page$', r'^\d+$'  # Just numbers
        ]
        
        descriptive_count = 0
        generic_count = 0
        
        for link in links:
            text = link['anchor_text'].lower().strip()
            if any(re.match(pattern, text) for pattern in generic_patterns):
                generic_count += 1
            elif len(text.strip()) > 0:
                descriptive_count += 1
        
        return {
            'average_length': round(avg_length, 1),
            'descriptive_count': descriptive_count,
            'generic_count': generic_count,
            'descriptive_percentage': (descriptive_count / len(links) * 100) if links else 0
        }
    
    def _count_navigation_links(self, internal_links: List[Dict[str, Any]]) -> int:
        """Count links that appear to be in navigation areas."""
        nav_count = 0
        for link in internal_links:
            parent = link.get('parent_tag')
            classes = link.get('class', [])
            
            if (parent in ['nav', 'header'] or 
                any('nav' in str(c).lower() or 'menu' in str(c).lower() for c in classes)):
                nav_count += 1
        
        return nav_count
