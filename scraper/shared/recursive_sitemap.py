"""Universal Recursive Sitemap Discovery System - CMS-agnostic, Production-ready"""

import requests
import gzip
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from typing import Set, List, Optional, Tuple
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Shared normalizer — MUST match the normalization used by the link-discovery worker,
# otherwise url_metadata keys won't match lookups (RC-6).
from .utils import normalize_url as shared_normalize_url
from .url_filters import should_skip_seo_url

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type priority for conflict resolution (lower number = higher priority)
TYPE_PRIORITY = {
    "main": 1,
    "service": 2,
    "blog": 3,
    "category": 4,
    "other": 99
}

def classify_sitemap_type(sitemap_url: str) -> str:
    """
    Classify sitemap type based on URL patterns.
    
    Args:
        sitemap_url: Full sitemap URL
        
    Returns:
        Type string: "main", "service", "blog", "category", or "other"
    """
    sitemap_lower = sitemap_url.lower()
    
    # Service patterns
    if "service" in sitemap_lower:
        return "service"
    
    # Blog patterns
    if "blog" in sitemap_lower or "post" in sitemap_lower:
        return "blog"
    
    # Main page patterns
    if "page" in sitemap_lower or "standard" in sitemap_lower:
        return "main"
    
    # Category patterns
    if "category" in sitemap_lower:
        return "category"
    
    # Default fallback
    return "other"

class RecursiveSitemapDiscovery:
    """Universal recursive sitemap discovery system for any CMS structure"""
    
    def __init__(self, base_url: str, max_depth: int = 5, max_sitemaps: int = 50, timeout: int = 10, max_concurrency: int = 8):
        """
        Initialize recursive sitemap discovery

        Args:
            base_url: Target website URL
            max_depth: Maximum recursion depth for sitemap indexes
            max_sitemaps: Maximum number of sitemaps to process
            timeout: Request timeout in seconds
            max_concurrency: Max sitemaps fetched in parallel per BFS level
        """
        self.base_url = base_url.rstrip('/')
        self.max_depth = max_depth
        self.max_sitemaps = max_sitemaps
        self.timeout = timeout
        self.max_concurrency = max(1, max_concurrency)
        # Guards all shared mutable state (discovered_urls, url_metadata, stats,
        # processed_sitemaps, sitemap_count, depth_map) during parallel traversal.
        self._lock = threading.Lock()
        self.session = requests.Session()
        # Use a realistic browser User-Agent. Many CDNs/WAFs (Cloudflare, Akamai,
        # Shopify bot rules) return 403/empty for non-browser UAs, which previously
        # caused total discovery failure on protected sites.
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'application/xml,text/xml,text/html,application/xhtml+xml,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        # Tracking
        self.discovered_urls: Set[str] = set()
        self.url_metadata: dict = {}  # url -> {"type": str, "sourceSitemap": str}
        self.processed_sitemaps: Set[str] = set()
        self.sitemap_count = 0
        self.depth_map = {}  # Track depth of each sitemap
        
        # Statistics
        self.stats = {
            'total_urls': 0,
            'sitemaps_processed': 0,
            'sitemap_indexes_found': 0,
            'urlsets_found': 0,
            'failed_sitemaps': 0,
            'recursion_depth_used': 0
        }

    def is_valid_business_sitemap(self, sitemap_url: str) -> bool:
        """
        Validate that sitemap is business content, not theme/builder artifacts.
        Filters junk custom post types at sitemap-file level (primary defense).
        
        Returns False for template/builder sitemaps to prevent processing.
        """
        url_lower = sitemap_url.lower()
        
        # Junk sitemap filename patterns (comprehensive CMS coverage)
        junk_patterns = [
            # Panda template engine
            'pxl-template',
            # Elementor builder
            'elementor_library',
            'elementor-template',
            # Beaver Builder
            'fl-builder',
            'fl-template',
            # Divi / Extra theme
            'divi-template',
            'extra-template',
            # Generic theme artifacts
            '-template-sitemap',
            '-builder-sitemap',
            '-templates-sitemap',
            # WooCommerce product templates/variations
            'wp-product-template',
            'woo-template',
            # ACF (Advanced Custom Fields) template posts
            'acf-template',
            # Custom taxonomy templates
            '/draft-sitemap',
            '/revision-sitemap',
            '/trash-sitemap',
            # Gutenberg templates
            '/wp-templates',
            # Exclude common test/preview sitemaps
            'test-sitemap',
            'preview-sitemap',
            'staging-sitemap',
            '-sample-sitemap'
        ]
        
        for pattern in junk_patterns:
            if pattern in url_lower:
                logger.info(f"[DISCOVERY] Filtered junk sitemap: {sitemap_url} (matched pattern: {pattern})")
                return False
        
        return True
    
    def is_valid_internal_url(self, url: str) -> bool:
        """Check if URL belongs to the target domain"""
        try:
            parsed_base = urlparse(self.base_url)
            parsed_url = urlparse(url)
            
            # Check same domain (allow subdomains)
            base_domain = parsed_base.netloc.replace('www.', '')
            url_domain = parsed_url.netloc.replace('www.', '')
            
            return url_domain.endswith(base_domain) or base_domain.endswith(url_domain)
        except:
            return False
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL for consistent storage.

        Delegates to the shared normalizer so that discovered_urls and url_metadata
        keys are byte-for-byte identical to what the link-discovery worker computes
        when it looks metadata back up (RC-6). A previous bespoke implementation here
        diverged (kept original case / scheme), causing every metadata lookup to miss.
        """
        if not url:
            return url
        if not urlparse(url).scheme:
            url = 'https://' + url
        return shared_normalize_url(url)
    
    def fetch_sitemap(self, sitemap_url: str) -> Optional[str]:
        """Fetch sitemap content with support for compression and redirects"""
        try:
            logger.info(f"[DISCOVERY] Fetching sitemap: {sitemap_url}")
            
            response = self.session.get(
                sitemap_url, 
                timeout=self.timeout,
                allow_redirects=True,
                headers={'Accept-Encoding': 'gzip, deflate'}
            )
            
            if response.status_code == 200:
                # Handle gzip compression
                if response.headers.get('content-encoding') == 'gzip':
                    try:
                        content = gzip.decompress(response.content).decode('utf-8')
                    except:
                        content = response.text
                else:
                    content = response.text
                
                logger.info(f"[DISCOVERY] Successfully fetched {sitemap_url} ({len(content)} chars)")
                return content
            else:
                logger.warning(f"[DISCOVERY] HTTP {response.status_code} for {sitemap_url}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"[DISCOVERY] Timeout for {sitemap_url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"[DISCOVERY] Request failed for {sitemap_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"[DISCOVERY] Unexpected error fetching {sitemap_url}: {e}")
            return None
    
    def detect_sitemap_type(self, content: str) -> Tuple[str, List[str]]:
        """
        Detect sitemap type and extract URLs
        
        Returns:
            Tuple of (sitemap_type, urls_list)
            sitemap_type: 'sitemapindex' or 'urlset'
        """
        try:
            # Parse XML with proper namespace handling
            root = ET.fromstring(content)
            
            # Handle XML namespaces properly
            namespace = ''
            if root.tag.startswith('{'):
                namespace = root.tag.split('}')[0] + '}'
            
            # Extract local name from namespaced tag
            local_tag = root.tag.split('}')[-1] if '}' in root.tag else root.tag
            sitemap_type = local_tag.lower()
            urls = []
            
            if 'sitemapindex' in sitemap_type:
                # Extract child sitemap URLs
                for loc in root.findall(f'.//{namespace}loc'):
                    if loc.text and loc.text.strip():
                        urls.append(loc.text.strip())
                
                logger.info(f"[DISCOVERY] Found sitemapindex with {len(urls)} children")
                self.stats['sitemap_indexes_found'] += 1
                
            elif 'urlset' in sitemap_type:
                # Extract page URLs
                for url in root.findall(f'.//{namespace}url'):
                    loc = url.find(f'{namespace}loc')
                    if loc is not None and loc.text and loc.text.strip():
                        urls.append(loc.text.strip())
                
                logger.info(f"[DISCOVERY] Found urlset with {len(urls)} URLs")
                self.stats['urlsets_found'] += 1
                
            else:
                logger.warning(f"[DISCOVERY] Unknown sitemap type: {sitemap_type}")
                return 'unknown', []
            
            return sitemap_type, urls
            
        except ET.ParseError as e:
            logger.error(f"[DISCOVERY] XML parse error: {e}")
            # Try alternative parsing method for malformed XML
            try:
                # Remove all namespaces as fallback
                content_clean = re.sub(r'<[^>]*xmlns[^>]*>', '', content)
                content_clean = re.sub(r'\sxmlns[^>]*', '', content_clean)
                root = ET.fromstring(content_clean)
                
                # Extract local name from namespaced tag
                local_tag = root.tag.split('}')[-1] if '}' in root.tag else root.tag
                sitemap_type = local_tag.lower()
                urls = []
                
                if 'sitemapindex' in sitemap_type:
                    for loc in root.findall('.//loc'):
                        if loc.text and loc.text.strip():
                            urls.append(loc.text.strip())
                    self.stats['sitemap_indexes_found'] += 1
                    
                elif 'urlset' in sitemap_type:
                    for url in root.findall('.//url'):
                        loc = url.find('loc')
                        if loc is not None and loc.text and loc.text.strip():
                            urls.append(loc.text.strip())
                    self.stats['urlsets_found'] += 1
                
                logger.info(f"[DISCOVERY] Fallback parsing succeeded: {sitemap_type} with {len(urls)} URLs")
                return sitemap_type, urls
                
            except Exception as fallback_error:
                logger.error(f"[DISCOVERY] Fallback parsing also failed: {fallback_error}")
                return 'error', []
                
        except Exception as e:
            logger.error(f"[DISCOVERY] Error detecting sitemap type: {e}")
            return 'error', []
    
    def _ingest_urlset(self, sitemap_url: str, urls: list) -> None:
        """Filter, normalize and store page URLs from a urlset.

        MUST be called while holding ``self._lock`` (mutates shared dicts/sets).
        Logic is unchanged from the original serial implementation.
        """
        internal_urls = []
        filtered_count = 0

        sitemap_type_classification = classify_sitemap_type(sitemap_url)
        logger.info(f"[TYPE-DETECT] sitemap={sitemap_url}, type={sitemap_type_classification}")

        for url in urls:
            if self.is_valid_internal_url(url):
                normalized_url = self.normalize_url(url)

                # Centralized URL filter — shared with link_discovery and url_selector
                if should_skip_seo_url(normalized_url):
                    filtered_count += 1
                    continue

                if normalized_url not in self.discovered_urls:
                    # First time seeing this URL
                    self.discovered_urls.add(normalized_url)
                    self.url_metadata[normalized_url] = {
                        "type": sitemap_type_classification,
                        "sourceSitemap": sitemap_url
                    }
                    internal_urls.append(normalized_url)
                else:
                    # URL already exists, apply priority logic
                    existing_metadata = self.url_metadata.get(normalized_url, {})
                    existing_type = existing_metadata.get("type", "other")

                    # Keep the type with higher priority (lower number)
                    if TYPE_PRIORITY.get(sitemap_type_classification, 99) < TYPE_PRIORITY.get(existing_type, 99):
                        self.url_metadata[normalized_url] = {
                            "type": sitemap_type_classification,
                            "sourceSitemap": sitemap_url
                        }

        logger.info(f"[DISCOVERY] Added {len(internal_urls)} internal URLs from {sitemap_url} ({filtered_count} filtered)")
        self.stats['total_urls'] += len(internal_urls)

    def _process_one(self, sitemap_url: str, current_depth: int) -> list:
        """Fetch + classify a single sitemap.

        Network fetch happens OUTSIDE the lock (the parallel win); shared-state
        mutation (parse stats, urlset ingestion) happens UNDER the lock so the
        BFS workers never corrupt discovered_urls/url_metadata/stats.

        Returns the list of child sitemap URLs (non-empty only for sitemapindex).
        """
        content = self.fetch_sitemap(sitemap_url)
        if not content:
            with self._lock:
                self.stats['failed_sitemaps'] += 1
            return []

        with self._lock:
            sitemap_type, urls = self.detect_sitemap_type(content)

            if sitemap_type == 'sitemapindex':
                logger.info(f"[DISCOVERY] Found sitemapindex {sitemap_url} with {len(urls)} children")
                return list(urls)
            elif sitemap_type == 'urlset':
                self._ingest_urlset(sitemap_url, urls)
                return []
            else:
                logger.error(f"[DISCOVERY] Cannot process sitemap type: {sitemap_type}")
                self.stats['failed_sitemaps'] += 1
                return []
    
    def discover_sitemaps_from_robots(self) -> List[str]:
        """Extract sitemap URLs from robots.txt (with junk filtering)"""
        robots_url = urljoin(self.base_url, '/robots.txt')
        sitemaps = []
        
        try:
            logger.info(f"[DISCOVERY] Checking robots.txt: {robots_url}")
            response = self.session.get(robots_url, timeout=self.timeout)
            
            if response.status_code == 200:
                for line in response.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        if sitemap_url and self.is_valid_business_sitemap(sitemap_url):
                            sitemaps.append(sitemap_url)
                
                logger.info(f"[DISCOVERY] Found {len(sitemaps)} valid business sitemaps in robots.txt")
            
        except Exception as e:
            logger.warning(f"[DISCOVERY] Failed to fetch robots.txt: {e}")
        
        return sitemaps
    
    def sitemap_exists(self, sitemap_url: str) -> bool:
        """Probe whether a sitemap location is reachable.

        Fixes RC-3: a plain HEAD without allow_redirects rejected any sitemap that
        301/302-redirects (e.g. /sitemap.xml -> /sitemap_index.xml). We now follow
        redirects on HEAD and fall back to a ranged GET when the server does not
        support HEAD (405/501) or returns an ambiguous status.
        """
        try:
            resp = self.session.head(sitemap_url, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                return True
            # Some servers don't implement HEAD or block it — verify with a light GET
            if resp.status_code in (403, 405, 501) or resp.status_code >= 400:
                get_resp = self.session.get(
                    sitemap_url, timeout=self.timeout, allow_redirects=True,
                    headers={'Range': 'bytes=0-2047'}
                )
                if get_resp.status_code in (200, 206):
                    return True
        except requests.RequestException as e:
            # Last resort: try a GET in case HEAD specifically is blocked
            try:
                get_resp = self.session.get(sitemap_url, timeout=self.timeout, allow_redirects=True)
                return get_resp.status_code == 200
            except requests.RequestException:
                logger.warning(f"[DISCOVERY] Sitemap probe failed for {sitemap_url}: {e}")
        return False

    def discover_initial_sitemaps(self) -> List[str]:
        """Find initial sitemap URLs using multiple strategies (with junk filtering)"""
        sitemaps = []
        
        # Strategy 1: robots.txt (filtering already applied in discover_sitemaps_from_robots)
        robots_sitemaps = self.discover_sitemaps_from_robots()
        sitemaps.extend(robots_sitemaps)
        
        # Strategy 2 + 3: Common + WordPress sitemap locations.
        # Existence probes are network round-trips, so run them in parallel
        # (order-preserving) instead of one-at-a-time.
        candidate_locations = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemaps.xml',
            '/sitemap/sitemap.xml',
            '/wp-sitemap.xml',          # WordPress
            '/sitemap_index.xml.gz',    # Compressed
            '/wp-sitemaps.xml',         # WordPress (plural)
        ]

        # Build the unique candidate list (skip junk + robots-discovered dupes)
        candidates = []
        seen_candidates = set(sitemaps)
        for location in candidate_locations:
            sitemap_url = self.base_url + location
            if sitemap_url not in seen_candidates and self.is_valid_business_sitemap(sitemap_url):
                seen_candidates.add(sitemap_url)
                candidates.append(sitemap_url)

        if candidates:
            with ThreadPoolExecutor(max_workers=min(self.max_concurrency, len(candidates))) as ex:
                results = list(ex.map(
                    lambda u: (u, self.sitemap_exists(u)),
                    candidates,
                ))
            for sitemap_url, exists in results:
                if exists:
                    sitemaps.append(sitemap_url)
                    logger.info(f"[DISCOVERY] Found sitemap at: {sitemap_url}")

        logger.info(f"[DISCOVERY] Initial sitemap discovery found {len(sitemaps)} valid business sitemaps")
        return sitemaps
    
    def discover_all_urls(self) -> Tuple[Set[str], dict]:
        """
        Main method to discover all URLs from recursive sitemap processing
        
        Returns:
            Tuple of (urls_set, url_metadata_dict)
        """
        start_time = time.time()
        logger.info(f"[DISCOVERY] Starting recursive sitemap discovery for {self.base_url}")

        # Find initial sitemaps
        initial_sitemaps = self.discover_initial_sitemaps()

        if not initial_sitemaps:
            logger.warning(f"[DISCOVERY] No sitemaps found for {self.base_url}")
            return set(), {}

        # Breadth-first traversal: each level (a set of sitemaps at the same
        # depth) is fetched+parsed in parallel with bounded concurrency. Shared
        # state is claimed/mutated under self._lock so the workers are race-free,
        # while the slow network fetch happens outside the lock.
        frontier = [(s, 0) for s in initial_sitemaps]

        while frontier:
            # Atomically claim a batch (respecting max_sitemaps / max_depth / dedupe)
            batch = []
            with self._lock:
                for (sitemap_url, depth) in frontier:
                    if self.sitemap_count >= self.max_sitemaps:
                        logger.warning(f"[DISCOVERY] Max sitemap count ({self.max_sitemaps}) reached")
                        break
                    if depth >= self.max_depth:
                        continue
                    if sitemap_url in self.processed_sitemaps:
                        continue
                    self.processed_sitemaps.add(sitemap_url)
                    self.sitemap_count += 1
                    self.depth_map[sitemap_url] = depth
                    self.stats['recursion_depth_used'] = max(self.stats['recursion_depth_used'], depth)
                    batch.append((sitemap_url, depth))

            if not batch:
                break

            next_frontier = []
            with ThreadPoolExecutor(max_workers=min(self.max_concurrency, len(batch))) as ex:
                future_map = {
                    ex.submit(self._process_one, sitemap_url, depth): (sitemap_url, depth)
                    for (sitemap_url, depth) in batch
                }
                for fut in as_completed(future_map):
                    sitemap_url, depth = future_map[fut]
                    try:
                        children = fut.result()
                    except Exception as e:
                        logger.warning(f"[DISCOVERY] Error processing {sitemap_url}: {e}")
                        children = []
                    for child in children:
                        next_frontier.append((child, depth + 1))

            frontier = next_frontier

        # Update final stats
        self.stats['sitemaps_processed'] = len(self.processed_sitemaps)
        
        duration = time.time() - start_time
        logger.info(f"[DISCOVERY] Recursive discovery completed in {duration:.2f}s")
        logger.info(f"[DISCOVERY] Total unique URLs collected: {len(self.discovered_urls)}")
        logger.info(f"[DISCOVERY] Sitemaps processed: {self.stats['sitemaps_processed']}")
        logger.info(f"[DISCOVERY] Sitemap indexes: {self.stats['sitemap_indexes_found']}")
        logger.info(f"[DISCOVERY] URL sets: {self.stats['urlsets_found']}")
        logger.info(f"[DISCOVERY] Failed sitemaps: {self.stats['failed_sitemaps']}")
        logger.info(f"[DISCOVERY] Max recursion depth used: {self.stats['recursion_depth_used']}")
        
        return self.discovered_urls, self.url_metadata
    
    def get_statistics(self) -> dict:
        """Get discovery statistics"""
        return {
            **self.stats,
            'unique_urls': len(self.discovered_urls),
            'base_url': self.base_url,
            'max_depth_limit': self.max_depth,
            'max_sitemaps_limit': self.max_sitemaps
        }


def discover_all_sitemap_urls(base_url: str, max_depth: int = 5, max_sitemaps: int = 50, max_concurrency: int = 8) -> Tuple[Set[str], dict, dict]:
    """
    Convenience function to discover all URLs from sitemaps

    Args:
        base_url: Target website URL
        max_depth: Maximum recursion depth
        max_sitemaps: Maximum sitemaps to process
        max_concurrency: Max sitemaps fetched in parallel per BFS level

    Returns:
        Tuple of (urls_set, url_metadata_dict, statistics_dict)
    """
    discovery = RecursiveSitemapDiscovery(base_url, max_depth, max_sitemaps, max_concurrency=max_concurrency)
    urls, url_metadata = discovery.discover_all_urls()
    stats = discovery.get_statistics()

    return urls, url_metadata, stats


# Example usage and testing
if __name__ == "__main__":
    # Test with the target site
    test_url = "https://www.sapphiredigitalagency.com"
    
    print(f"🔍 Testing recursive sitemap discovery for: {test_url}")
    
    urls, stats = discover_all_sitemap_urls(test_url, max_depth=5, max_sitemaps=50)
    
    print(f"\n📊 Results:")
    print(f"Total URLs discovered: {len(urls)}")
    print(f"Sitemaps processed: {stats['sitemaps_processed']}")
    print(f"Sitemap indexes found: {stats['sitemap_indexes_found']}")
    print(f"URL sets found: {stats['urlsets_found']}")
    print(f"Failed sitemaps: {stats['failed_sitemaps']}")
    print(f"Max recursion depth used: {stats['recursion_depth_used']}")
    
    print(f"\n🔗 First 10 URLs:")
    for i, url in enumerate(list(urls)[:10]):
        print(f"{i+1}. {url}")
    
    if len(urls) > 10:
        print(f"... and {len(urls) - 10} more URLs")
