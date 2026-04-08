"""
Performance Optimizations for AI Visibility Scraper
Production-grade optimizations for speed, memory, and reliability
"""

import gc
import re
import json
import time
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
from functools import lru_cache
import threading
import hashlib


class PerformanceOptimizedScraper:
    """Production-grade scraper with performance optimizations"""
    
    def __init__(self, max_html_size: int = 500_000, enable_caching: bool = True):
        self.max_html_size = max_html_size
        self.enable_caching = enable_caching
        self.extraction_cache = {} if enable_caching else None
        self.cache_lock = threading.Lock()
        self.performance_stats = {
            "extractions_completed": 0,
            "total_processing_time": 0,
            "cache_hits": 0,
            "memory_saved": 0
        }
    
    def optimize_html_for_processing(self, html: str) -> str:
        """Optimize HTML before processing to reduce memory usage"""
        try:
            # 1. Size limiting
            if len(html) > self.max_html_size:
                original_size = len(html)
                html = html[:self.max_html_size]
                print(f"[PERFORMANCE] HTML truncated from {original_size:,} to {self.max_html_size:,} characters")
                self.performance_stats["memory_saved"] += (original_size - self.max_html_size)
            
            # 2. Remove unnecessary whitespace
            html = re.sub(r'\s+', ' ', html)
            
            # 3. Remove comments
            html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
            
            # 4. Remove empty script/style content (keep tags for structure detection)
            html = re.sub(r'<script[^>]*>\s*</script>', '<script></script>', html)
            html = re.sub(r'<style[^>]*>\s*</style>', '<style></style>', html)
            
            return html
            
        except Exception as e:
            print(f"[PERFORMANCE] HTML optimization failed: {e}")
            return html
    
    def create_optimized_soup(self, html: str, parser: str = 'lxml') -> BeautifulSoup:
        """Create optimized BeautifulSoup object with minimal memory footprint"""
        try:
            # Use lxml for speed and memory efficiency
            soup = BeautifulSoup(html, parser)
            
            # Remove heavy elements that aren't needed for extraction
            for tag in soup.find_all(['script', 'style', 'noscript']):
                # Keep the tag but clear its contents to maintain structure
                tag.clear()
            
            return soup
            
        except Exception as e:
            print(f"[PERFORMANCE] Soup creation failed, falling back to html.parser: {e}")
            return BeautifulSoup(html, 'html.parser')
    
    def extract_with_cache(self, extraction_func, cache_key: str, *args, **kwargs):
        """Extract data with caching to avoid redundant processing"""
        if not self.enable_caching:
            return extraction_func(*args, **kwargs)
        
        # Generate cache hash
        cache_hash = hashlib.md5(f"{cache_key}_{str(args)}_{str(kwargs)}".encode()).hexdigest()
        
        with self.cache_lock:
            if cache_hash in self.extraction_cache:
                self.performance_stats["cache_hits"] += 1
                return self.extraction_cache[cache_hash]
        
        # Perform extraction
        result = extraction_func(*args, **kwargs)
        
        # Cache result (limit cache size)
        with self.cache_lock:
            if len(self.extraction_cache) < 1000:  # Limit cache to 1000 items
                self.extraction_cache[cache_hash] = result
        
        return result
    
    def batch_extract_json_ld(self, soup) -> List[Dict[str, Any]]:
        """Batch extract all JSON-LD scripts with error handling"""
        json_ld_data = []
        
        try:
            scripts = soup.find_all('script', type='application/ld+json')
            
            for script in scripts:
                try:
                    if script.string:
                        data = json.loads(script.string)
                        json_ld_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"[PERFORMANCE] JSON-LD parse error: {e}")
                    continue
                except Exception as e:
                    print(f"[PERFORMANCE] JSON-LD extraction error: {e}")
                    continue
        
        except Exception as e:
            print(f"[PERFORMANCE] JSON-LD batch extraction failed: {e}")
        
        return json_ld_data
    
    def safe_extract_text_with_limit(self, element, max_length: int = 500) -> str:
        """Safely extract text with length limits and memory efficiency"""
        if not element:
            return ""
        
        try:
            # Use get_text with strip for efficiency
            text = element.get_text(strip=True)
            
            # Limit length to prevent memory issues
            if len(text) > max_length:
                text = text[:max_length] + "..."
            
            return text
            
        except Exception:
            return ""
    
    def extract_metadata_batch(self, soup) -> Dict[str, Any]:
        """Extract all metadata in a single pass for efficiency"""
        metadata = {
            "title": "",
            "meta_description": "",
            "canonical_url": "",
            "robots_meta": "",
            "viewport": "",
            "language": "",
            "hreflang_links": [],
            "open_graph": {},
            "twitter_card": {}
        }
        
        try:
            # Title
            title_tag = soup.find('title')
            if title_tag:
                metadata["title"] = self.safe_extract_text_with_limit(title_tag, 100)
            
            # Meta tags in single pass
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                name = meta.get('name', '').lower()
                property_attr = meta.get('property', '').lower()
                content = meta.get('content', '')
                
                if not content:
                    continue
                
                # Meta description
                if name == 'description':
                    metadata["meta_description"] = content[:200]
                
                # Robots
                elif name == 'robots':
                    metadata["robots_meta"] = content
                
                # Viewport
                elif name == 'viewport':
                    metadata["viewport"] = content
                
                # Language
                elif name == 'language' or property_attr == 'og:locale':
                    metadata["language"] = content
                
                # Open Graph
                elif property_attr.startswith('og:'):
                    og_key = property_attr[3:]  # Remove 'og:' prefix
                    metadata["open_graph"][og_key] = content
                
                # Twitter Card
                elif name.startswith('twitter:'):
                    twitter_key = name[8:]  # Remove 'twitter:' prefix
                    metadata["twitter_card"][twitter_key] = content
            
            # Canonical URL
            canonical = soup.find('link', rel='canonical')
            if canonical:
                metadata["canonical_url"] = canonical.get('href', '')
            
            # hreflang links
            hreflang_links = soup.find_all('link', rel='alternate', hreflang=True)
            for link in hreflang_links:
                hreflang = link.get('hreflang', '')
                href = link.get('href', '')
                if hreflang and href:
                    metadata["hreflang_links"].append({
                        "lang": hreflang,
                        "url": href
                    })
            
            # HTML lang attribute
            html_tag = soup.find('html')
            if html_tag and not metadata["language"]:
                metadata["language"] = html_tag.get('lang', '')
        
        except Exception as e:
            print(f"[PERFORMANCE] Metadata batch extraction failed: {e}")
        
        return metadata
    
    def extract_links_batch(self, soup, base_url: str = "") -> Dict[str, Any]:
        """Extract all links in a single pass with categorization"""
        links_data = {
            "total_links": 0,
            "internal_links": 0,
            "external_links": 0,
            "links_with_text": 0,
            "empty_links": 0,
            "noreferrer_links": 0,
            "sponsored_links": 0,
            "link_distribution": {
                "in_nav": 0,
                "in_footer": 0,
                "in_main": 0,
                "in_sidebar": 0
            }
        }
        
        try:
            from urllib.parse import urlparse
            
            # Get base domain for internal/external detection
            if base_url:
                try:
                    base_domain = urlparse(base_url).netloc
                except:
                    base_domain = ""
            else:
                base_domain = ""
            
            # Find all links in single pass
            all_links = soup.find_all('a', href=True)
            links_data["total_links"] = len(all_links)
            
            for link in all_links:
                href = link.get('href', '')
                text = self.safe_extract_text_with_limit(link, 100)
                rel_attrs = link.get('rel', [])
                
                # Skip empty/invalid links
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    links_data["empty_links"] += 1
                    continue
                
                # Count links with text
                if text:
                    links_data["links_with_text"] += 1
                
                # Check rel attributes
                if 'noreferrer' in rel_attrs:
                    links_data["noreferrer_links"] += 1
                if 'sponsored' in rel_attrs:
                    links_data["sponsored_links"] += 1
                
                # Internal vs External detection
                if base_domain:
                    try:
                        link_domain = urlparse(href).netloc
                        if link_domain == base_domain:
                            links_data["internal_links"] += 1
                        else:
                            links_data["external_links"] += 1
                    except:
                        # If parsing fails, assume external
                        links_data["external_links"] += 1
                else:
                    links_data["external_links"] += 1
                
                # Link location detection
                parent = link.find_parent()
                while parent and parent.name != 'body':
                    if parent.name in ['nav', 'header']:
                        links_data["link_distribution"]["in_nav"] += 1
                        break
                    elif parent.name in ['footer']:
                        links_data["link_distribution"]["in_footer"] += 1
                    elif parent.name in ['main', 'article']:
                        links_data["link_distribution"]["in_main"] += 1
                    elif parent.name in ['aside', 'sidebar']:
                        links_data["link_distribution"]["in_sidebar"] += 1
                    parent = parent.find_parent()
        
        except Exception as e:
            print(f"[PERFORMANCE] Link batch extraction failed: {e}")
        
        return links_data
    
    def extract_images_batch(self, soup) -> Dict[str, Any]:
        """Extract all images in a single pass with optimization"""
        images_data = {
            "total_images": 0,
            "images_with_alt": 0,
            "images_with_title": 0,
            "responsive_images": 0,
            "lazy_loaded": 0,
            "next_gen_formats": 0,
            "image_sources": {"http": 0, "https": 0, "data": 0, "relative": 0},
            "optimization_indicators": {
                "width_height": 0,
                "loading_attr": 0,
                "srcset_present": 0,
                "sizes_present": 0
            }
        }
        
        try:
            all_images = soup.find_all('img')
            images_data["total_images"] = len(all_images)
            
            for img in all_images:
                src = img.get('src', '')
                alt = img.get('alt', '')
                title = img.get('title', '')
                width = img.get('width')
                height = img.get('height')
                loading = img.get('loading', '')
                srcset = img.get('srcset', '')
                sizes = img.get('sizes', '')
                
                # Alt text
                if alt:
                    images_data["images_with_alt"] += 1
                
                # Title text
                if title:
                    images_data["images_with_title"] += 1
                
                # Source type
                if src.startswith('https://'):
                    images_data["image_sources"]["https"] += 1
                elif src.startswith('http://'):
                    images_data["image_sources"]["http"] += 1
                elif src.startswith('data:'):
                    images_data["image_sources"]["data"] += 1
                else:
                    images_data["image_sources"]["relative"] += 1
                
                # Optimization indicators
                if width and height:
                    images_data["optimization_indicators"]["width_height"] += 1
                
                if loading:
                    images_data["optimization_indicators"]["loading_attr"] += 1
                    if loading == 'lazy':
                        images_data["lazy_loaded"] += 1
                
                if srcset:
                    images_data["optimization_indicators"]["srcset_present"] += 1
                    images_data["responsive_images"] += 1
                
                if sizes:
                    images_data["optimization_indicators"]["sizes_present"] += 1
                
                # Next-gen formats
                if any(ext in src.lower() for ext in ['.webp', '.avif', '.jxl']):
                    images_data["next_gen_formats"] += 1
        
        except Exception as e:
            print(f"[PERFORMANCE] Image batch extraction failed: {e}")
        
        return images_data
    
    def cleanup_memory(self):
        """Force garbage collection to free memory"""
        try:
            gc.collect()
        except Exception as e:
            print(f"[PERFORMANCE] Memory cleanup failed: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        stats = self.performance_stats.copy()
        
        if stats["extractions_completed"] > 0:
            stats["avg_processing_time"] = stats["total_processing_time"] / stats["extractions_completed"]
        else:
            stats["avg_processing_time"] = 0
        
        if self.enable_caching and stats["extractions_completed"] > 0:
            stats["cache_hit_rate"] = (stats["cache_hits"] / stats["extractions_completed"]) * 100
        else:
            stats["cache_hit_rate"] = 0
        
        stats["cache_size"] = len(self.extraction_cache) if self.extraction_cache else 0
        
        return stats


# Utility functions for performance optimization
@lru_cache(maxsize=1000)
def cached_regex_compile(pattern: str):
    """Cache compiled regex patterns for better performance"""
    return re.compile(pattern)


def safe_json_parse(json_string: str) -> Optional[Dict[str, Any]]:
    """Safely parse JSON with error handling and memory efficiency"""
    if not json_string:
        return None
    
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        return None
    except Exception:
        return None


def extract_text_content_efficient(soup, content_selectors: List[str]) -> str:
    """Efficiently extract text content using priority selectors"""
    for selector in content_selectors:
        try:
            element = soup.select_one(selector)
            if element:
                # Remove script/style elements from content
                for script in element.find_all(['script', 'style']):
                    script.decompose()
                
                return element.get_text(separator=' ', strip=True)
        except Exception:
            continue
    
    # Fallback to body
    body = soup.find('body')
    if body:
        return body.get_text(separator=' ', strip=True)
    
    return ""


def validate_and_limit_data(data: Dict[str, Any], max_depth: int = 10, max_size: int = 10000) -> Dict[str, Any]:
    """Validate and limit data size to prevent memory issues"""
    def _limit_depth(obj, current_depth=0):
        if current_depth >= max_depth:
            return "[MAX_DEPTH_REACHED]"
        
        if isinstance(obj, dict):
            return {k: _limit_depth(v, current_depth + 1) for k, v in list(obj.items())[:100]}
        elif isinstance(obj, list):
            return [_limit_depth(item, current_depth + 1) for item in obj[:100]]
        elif isinstance(obj, str) and len(obj) > max_size:
            return obj[:max_size] + "...[TRUNCATED]"
        else:
            return obj
    
    return _limit_depth(data)


# Performance monitoring decorator
def monitor_performance(func):
    """Decorator to monitor function performance"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.time()
            processing_time = (end_time - start_time) * 1000  # Convert to milliseconds
            print(f"[PERFORMANCE] {func.__name__} took {processing_time:.2f}ms")
    
    return wrapper


# Memory-efficient text processing
def process_large_text(text: str, chunk_size: int = 10000) -> List[str]:
    """Process large text in chunks to reduce memory usage"""
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        chunks.append(chunk)
    
    return chunks


# Export main classes and functions
__all__ = [
    'PerformanceOptimizedScraper',
    'cached_regex_compile',
    'safe_json_parse',
    'extract_text_content_efficient',
    'validate_and_limit_data',
    'monitor_performance',
    'process_large_text'
]
