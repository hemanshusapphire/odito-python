"""Lightweight homepage scraper - no JS rendering, fast execution"""

import re
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def scrape_homepage(url: str, timeout: int = 6) -> dict:
    """
    Scrape HTML and extract basic SEO data.
    No JS rendering, no heavy processing.
    Accepts any URL (not just homepage).
    
    Args:
        url: URL to scrape
        timeout: Request timeout in seconds (default: 6)
    
    Returns:
        dict with extracted data or error
    """
    import time
    start_time = time.time()
    
    try:
        # Normalize URL
        url = normalize_url(url)
        
        print(f"[SCRAPER] Fetching: {url} (timeout: {timeout}s - reliability mode)")
        
        # Fetch HTML with timeout, headers, and redirect following
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        # Capture final URL (after redirects) and headers
        final_url = response.url
        response_headers = dict(response.headers)
        
        html = response.text
        elapsed = time.time() - start_time
        print(f"[SCRAPER] Fetched in {elapsed:.2f}s: {final_url}")
        
        # Parse HTML
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract basic fields
        title = extract_title(soup)
        meta_description = extract_meta_description(soup)
        
        # Extract data
        data = {
            'url': url,
            'final_url': final_url,
            'status_code': response.status_code,
            'response_headers': response_headers,
            'title': title,
            'title_length': len(title),
            'title_status': get_title_status(title),
            'meta_description': meta_description,
            'meta_length': len(meta_description),
            'meta_status': get_meta_status(meta_description),
            'h1': extract_h1(soup),
            'h1_count': len(soup.find_all('h1')),
            'h2_count': len(soup.find_all('h2')),
            'h3_count': len(soup.find_all('h3')),
            'h4_count': len(soup.find_all('h4')),
            'h5_count': len(soup.find_all('h5')),
            'h6_count': len(soup.find_all('h6')),
            'schema_info': extract_schema_info(soup),  # MOVED BEFORE text extraction to prevent script removal
            'has_schema': extract_schema_info(soup)['has_schema'],  # FIXED: Use parsed result, not simple check
            'text_content': extract_text_content(soup),
            'word_count': count_words(extract_text_content(soup)),
            'has_canonical': bool(soup.find('link', rel='canonical')),
            'canonical_url': extract_canonical_url(soup),
            'has_robots_meta': bool(soup.find('meta', attrs={'name': 'robots'})),
            'robots_meta_content': extract_robots_meta_content(soup),
            'has_favicon': bool(soup.find('link', rel='icon')) or bool(soup.find('link', rel='shortcut icon')),
            'has_viewport': check_viewport_meta(soup),  # FIXED: Viewport detection from HTML (most reliable)
            'is_https': url.startswith('https://'),
            'html_size': len(html),
            'response_time_ms': int(response.elapsed.total_seconds() * 1000),
            'html_lang': extract_html_lang(soup),
            'hreflang_tags': extract_hreflang_tags(soup),
            'image_alt_percentage': calculate_image_alt_percentage(soup),
            'x_robots_tag': extract_x_robots_tag(response_headers),
            'has_noindex': check_noindex(soup, response_headers),
            'robots_txt': check_robots_txt(final_url),
            'sitemap_xml': check_sitemap_xml(final_url),
            'https_redirect': check_https_redirect(url, final_url, timeout=30),
            'analytics': detect_google_analytics(soup, html),
            'identity_schema': detect_identity_schema(soup),
            'llms_txt': check_llms_txt(final_url),
            'social_signals': extract_social_signals(soup, html),  # ADDED: Social media signal detection
            'security_headers': extract_security_headers(response_headers),  # ADDED: Security headers detection
            'performance_metrics': {}  # ADDED: Placeholder for PageSpeed metrics (fetched asynchronously in pipeline)
        }
        
        # Calculate LLM readability after data is created
        data['llm_readability'] = calculate_llm_readability(data, soup)
        
        # ADDED: Local SEO signal detection (after data is created to access schema_info)
        data['local_seo'] = extract_local_seo_signals(soup, html, data.get('schema_info', {}))
        
        return data
        
    except requests.Timeout:
        elapsed = time.time() - start_time
        print(f"[SCRAPER] Timeout after {elapsed:.2f}s: {url}")
        return {'error': 'timeout', 'message': 'Request timed out'}
    except requests.RequestException as e:
        elapsed = time.time() - start_time
        error_msg = str(e)
        print(f"[SCRAPER] Request failed after {elapsed:.2f}s: {url} - {error_msg}")
        return {'error': 'request_failed', 'message': error_msg}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[SCRAPER] Scraping failed after {elapsed:.2f}s: {url} - {str(e)}")
        return {'error': 'scraping_failed', 'message': str(e)}


def normalize_url(url: str) -> str:
    """Normalize URL to ensure it has scheme and no trailing slash"""
    url = url.strip()
    
    # Add scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Remove trailing slash
    if url.endswith('/'):
        url = url[:-1]
    
    return url


def extract_title(soup: BeautifulSoup) -> str:
    """Extract page title"""
    title_tag = soup.find('title')
    return title_tag.get_text().strip() if title_tag else ''


def extract_meta_description(soup: BeautifulSoup) -> str:
    """Extract meta description"""
    meta = soup.find('meta', attrs={'name': 'description'})
    if meta:
        return meta.get('content', '').strip()
    return ''


def extract_h1(soup: BeautifulSoup) -> str:
    """Extract first H1 tag"""
    h1 = soup.find('h1')
    return h1.get_text().strip() if h1 else ''


def extract_text_content(soup: BeautifulSoup) -> str:
    """Extract visible text content"""
    # Remove script and style elements
    for script in soup(['script', 'style', 'noscript']):
        script.decompose()
    
    text = soup.get_text(separator=' ')
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def count_words(text: str) -> int:
    """Count words in text"""
    if not text:
        return 0
    words = re.findall(r'\b\w+\b', text)
    return len(words)


def get_title_status(title: str) -> str:
    """Get status based on title length"""
    if not title:
        return 'Missing'
    length = len(title)
    if length < 30:
        return 'Too Short'
    elif length > 60:
        return 'Too Long'
    else:
        return 'Good'


def get_meta_status(meta: str) -> str:
    """Get status based on meta description length"""
    if not meta:
        return 'Missing'
    length = len(meta)
    if length < 120:
        return 'Too Short'
    else:
        return 'Good'


def extract_canonical_url(soup: BeautifulSoup) -> str:
    """Extract canonical URL"""
    canonical = soup.find('link', rel='canonical')
    if canonical:
        return canonical.get('href', '').strip()
    return ''


def extract_robots_meta_content(soup: BeautifulSoup) -> str:
    """Extract robots meta content"""
    meta = soup.find('meta', attrs={'name': 'robots'})
    if meta:
        return meta.get('content', '').strip()
    return ''


def extract_html_lang(soup: BeautifulSoup) -> str:
    """Extract HTML lang attribute"""
    html_tag = soup.find('html')
    if html_tag:
        return html_tag.get('lang', '').strip()
    return ''


def extract_local_seo_signals(soup: BeautifulSoup, html: str, schema_info: dict) -> dict:
    """
    Extract local SEO signals from HTML.
    
    # ADDED: Local SEO detection for address, phone, local business schema, Google Business Profile
    """
    import re
    
    local_seo = {
        'address_phone': {
            'found': False,
            'phone': None,
            'address': None,
            'message': ''
        },
        'local_schema': False,
        'google_business_profile': False
    }
    
    # Step 1: Phone detection using strict regex
    phone_pattern = r'(\+?\d{1,3}[\s\-]?)?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{4}'
    phone_matches = re.findall(phone_pattern, html)
    phone_found = None
    
    if phone_matches:
        for match in phone_matches:
            phone_clean = re.sub(r'[^\d\+\s\-]', '', match).strip()
            
            # FIXED: Filter out dates and invalid patterns
            # Reject if looks like date (YYYY-MM-DD or YYYY/MM/DD)
            if re.match(r'\d{4}[\-\/]\d{2}[\-\/]\d{2}', phone_clean):
                continue
            
            # Reject if contains only year/month pattern
            if re.match(r'\d{4}[\-\/]\d{2}', phone_clean):
                continue
            
            # Reject if length < 8 digits
            digits_only = re.sub(r'[^\d]', '', phone_clean)
            if len(digits_only) < 8:
                continue
            
            phone_found = phone_clean
            break
    
    # Step 2: Address detection from specific sections only
    address_found = None
    
    # FIXED: Extract only from footer, contact sections, and address tags
    address_sections = []
    
    # Check footer
    footer = soup.find('footer')
    if footer:
        address_sections.append(footer.get_text())
    
    # Check contact sections
    contact_sections = soup.find_all(['div', 'section'], class_=lambda x: x and 'contact' in x.lower())
    for section in contact_sections:
        address_sections.append(section.get_text())
    
    # Check address tags
    address_tags = soup.find_all('address')
    for tag in address_tags:
        address_sections.append(tag.get_text())
    
    # Limit text size and clean
    clean_text = ' '.join(address_sections)[:500].lower()
    
    if clean_text:
        address_keywords = ["street", "road", "avenue", "lane", "suite", "city", "zip"]
        
        # Look for number + word pattern + keyword
        for keyword in address_keywords:
            # Pattern: number + words + keyword
            pattern = rf'\d+[\w\s]*{keyword}'
            match = re.search(pattern, clean_text)
            if match:
                # Extract reasonable context
                start = max(0, match.start() - 20)
                end = min(len(clean_text), match.end() + 50)
                address_candidate = clean_text[start:end].strip()
                
                # Must contain both number and keyword
                if re.search(r'\d', address_candidate) and keyword in address_candidate:
                    address_found = address_candidate[:100] + '...' if len(address_candidate) > 100 else address_candidate
                    break
    
    # Step 3: FIXED: Strict validation logic - AND condition only
    has_phone = phone_found is not None
    has_address = address_found is not None
    
    # FIXED: Only true if BOTH phone AND address found
    if has_phone and has_address:
        local_seo['address_phone']['found'] = True
        local_seo['address_phone']['message'] = 'Address and phone found'
    else:
        local_seo['address_phone']['found'] = False
        if not has_phone and not has_address:
            local_seo['address_phone']['message'] = 'Missing Address and Phone'
        elif not has_phone:
            local_seo['address_phone']['message'] = 'Missing Phone'
        else:
            local_seo['address_phone']['message'] = 'Missing Address'
    
    local_seo['address_phone']['phone'] = phone_found
    local_seo['address_phone']['address'] = address_found
    
    # Step 4: Local Business Schema detection
    schema_types = schema_info.get('types_detected', [])
    local_business_types = [
        'LocalBusiness', 'Organization', 'ProfessionalService', 'Store',
        'Restaurant', 'Hotel', 'Hospital', 'School', 'Bank', 'Pharmacy'
    ]
    
    for schema_type in schema_types:
        if any(local_type in schema_type for local_type in local_business_types):
            local_seo['local_schema'] = True
            break
    
    # Step 5: Google Business Profile detection
    links = soup.find_all('a', href=True)
    for link in links:
        href = link.get('href', '').lower()
        if any(gbp_pattern in href for gbp_pattern in [
            'google.com/maps', 'g.page', 'google.com/business',
            'maps.google.com', 'google.com/maps/place'
        ]):
            local_seo['google_business_profile'] = True
            break
    
    return local_seo


def extract_social_signals(soup: BeautifulSoup, html: str) -> dict:
    """
    Extract social media signals from HTML.
    
    # ADDED: Social signal detection for Facebook, Twitter, Instagram, LinkedIn, YouTube
    """
    from urllib.parse import urlparse, urljoin
    import re
    
    social_signals = {
        'facebook': {'found': False, 'url': ''},
        'twitter': {'found': False, 'url': ''},
        'instagram': {'found': False, 'url': ''},
        'linkedin': {'found': False, 'url': ''},
        'youtube': {'found': False, 'url': ''},
        'open_graph': False,
        'facebook_pixel': False,
        'twitter_cards': False
    }
    
    # Step 1: Extract all links and detect social platforms
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link.get('href', '').strip()
        if not href or href.startswith('#') or href.startswith('javascript:'):
            continue
            
        # Normalize URL
        if href.startswith('//'):
            href = 'https:' + href
        elif not href.startswith(('http://', 'https://')):
            continue  # Skip relative URLs for social detection
        
        # Parse domain
        parsed = urlparse(href.lower())
        domain = parsed.netloc.replace('www.', '')
        
        # Detect social platforms
        if 'facebook.com' in domain and not social_signals['facebook']['found']:
            social_signals['facebook'] = {'found': True, 'url': href}
        elif ('twitter.com' in domain or 'x.com' in domain) and not social_signals['twitter']['found']:
            social_signals['twitter'] = {'found': True, 'url': href}
        elif 'instagram.com' in domain and not social_signals['instagram']['found']:
            social_signals['instagram'] = {'found': True, 'url': href}
        elif 'linkedin.com' in domain and not social_signals['linkedin']['found']:
            social_signals['linkedin'] = {'found': True, 'url': href}
        elif ('youtube.com' in domain or 'youtu.be' in domain) and not social_signals['youtube']['found']:
            social_signals['youtube'] = {'found': True, 'url': href}
    
    # Step 2: Check Open Graph tags
    og_tags = soup.find_all('meta', property=lambda x: x and x.startswith('og:'))
    if og_tags:
        social_signals['open_graph'] = True
    
    # Step 3: Check Facebook Pixel
    if 'fbq(' in html or 'connect.facebook.net' in html:
        social_signals['facebook_pixel'] = True
    
    # Step 4: Check Twitter Cards
    twitter_tags = soup.find_all('meta', attrs={'name': lambda x: x and x.startswith('twitter:')})
    if twitter_tags:
        social_signals['twitter_cards'] = True
    
    return social_signals


def check_viewport_meta(soup: BeautifulSoup) -> bool:
    """
    Check if viewport meta tag is present in HTML.
    
    # FIXED: Primary viewport detection using HTML (most reliable)
    """
    viewport_tag = soup.find('meta', attrs={'name': 'viewport'})
    if viewport_tag and viewport_tag.get('content'):
        return True
    return False


def extract_hreflang_tags(soup: BeautifulSoup) -> list:
    """Extract all hreflang tags"""
    hreflang_tags = []
    tags = soup.find_all('link', rel='alternate', hreflang=True)
    for tag in tags:
        hreflang_tags.append({
            'hreflang': tag.get('hreflang', '').strip(),
            'href': tag.get('href', '').strip()
        })
    return hreflang_tags


def calculate_image_alt_percentage(soup: BeautifulSoup) -> dict:
    """Calculate percentage of images with alt text"""
    images = soup.find_all('img')
    total_images = len(images)
    if total_images == 0:
        return {'total': 0, 'with_alt': 0, 'percentage': 100}
    
    images_with_alt = sum(1 for img in images if img.get('alt') and img.get('alt').strip())
    percentage = int((images_with_alt / total_images) * 100)
    
    return {
        'total': total_images,
        'with_alt': images_with_alt,
        'percentage': percentage
    }


def extract_x_robots_tag(headers: dict) -> str:
    """Extract X-Robots-Tag from response headers"""
    if not headers:
        return ''
    
    # Check both 'X-Robots-Tag' and 'x-robots-tag' (case insensitive)
    for key, value in headers.items():
        if key.lower() == 'x-robots-tag':
            return str(value).strip()
    return ''


def check_noindex(soup: BeautifulSoup, headers: dict) -> bool:
    """Check if page has noindex directive (meta robots or X-Robots-Tag)"""
    # Check meta robots
    robots_meta = soup.find('meta', attrs={'name': 'robots'})
    if robots_meta:
        content = robots_meta.get('content', '').lower()
        if 'noindex' in content:
            return True
    
    # Check X-Robots-Tag header
    x_robots = extract_x_robots_tag(headers)
    if x_robots and 'noindex' in x_robots.lower():
        return True
    
    return False


def check_robots_txt(url: str, timeout: int = 30) -> dict:
    """Check if robots.txt exists and is accessible"""
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        response = requests.get(robots_url, headers=headers, timeout=timeout, allow_redirects=True)
        
        return {
            'exists': response.status_code == 200,
            'status_code': response.status_code,
            'url': robots_url
        }
    except:
        return {
            'exists': False,
            'status_code': None,
            'url': robots_url
        }


def check_sitemap_xml(url: str, timeout: int = 30) -> dict:
    """Check if sitemap.xml exists and is accessible"""
    try:
        parsed = urlparse(url)
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        response = requests.get(sitemap_url, headers=headers, timeout=timeout, allow_redirects=True)
        
        return {
            'exists': response.status_code == 200,
            'status_code': response.status_code,
            'url': sitemap_url
        }
    except:
        return {
            'exists': False,
            'status_code': None,
            'url': sitemap_url
        }


def extract_schema_info(soup: BeautifulSoup) -> dict:
    """
    Extract schema.org structured data information.
    Detects JSON-LD, Microdata, and RDFa formats.
    
    # FIXED: Improved detection to eliminate false negatives
    """
    schema_info = {
        'has_schema': False,
        'formats': [],
        'types_detected': []
    }
    
    # Detect JSON-LD - FIXED to use script.string or script.get_text() as fallback
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    for script in json_ld_scripts:
        try:
            import json
            # FIXED: Use script.string or script.get_text() as fallback
            raw_json = script.string or script.get_text()
            
            if not raw_json:
                continue
            
            raw_json = raw_json.strip()
            
            # FIXED: Set has_schema immediately when valid JSON is found
            schema_info['has_schema'] = True
            if 'json-ld' not in schema_info['formats']:
                schema_info['formats'].append('json-ld')
            
            data = json.loads(raw_json)
            
            # Handle list
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "@type" in item:
                        schema_type = item["@type"]
                        if isinstance(schema_type, list):
                            schema_info['types_detected'].extend(schema_type)
                        else:
                            schema_info['types_detected'].append(schema_type)
            
            # Handle dict
            elif isinstance(data, dict):
                if "@type" in data:
                    schema_type = data["@type"]
                    if isinstance(schema_type, list):
                        schema_info['types_detected'].extend(schema_type)
                    else:
                        schema_info['types_detected'].append(schema_type)
        except Exception:
            # FIXED: Fallback detection - if "@type" in raw_json, still mark as having schema
            raw_json = script.string or script.get_text()
            if raw_json and "@type" in raw_json:
                schema_info['has_schema'] = True
                if 'json-ld' not in schema_info['formats']:
                    schema_info['formats'].append('json-ld')
                schema_info['types_detected'].append('detected')
    
    # Detect Microdata
    microdata_elements = soup.find_all(attrs={'itemscope': True})
    if microdata_elements:
        schema_info['has_schema'] = True
        if 'microdata' not in schema_info['formats']:
            schema_info['formats'].append('microdata')
        
        for elem in microdata_elements:
            itemtype = elem.get('itemtype', '')
            if itemtype:
                # Extract type from URL (e.g., https://schema.org/Organization -> Organization)
                type_name = itemtype.split('/')[-1]
                if type_name and type_name not in schema_info['types_detected']:
                    schema_info['types_detected'].append(type_name)
    
    # Detect RDFa (basic check for typeof attribute)
    rdfa_elements = soup.find_all(attrs={'typeof': True})
    if rdfa_elements:
        schema_info['has_schema'] = True
        if 'rdfa' not in schema_info['formats']:
            schema_info['formats'].append('rdfa')
        
        for elem in rdfa_elements:
            typeof = elem.get('typeof', '')
            if typeof:
                if typeof not in schema_info['types_detected']:
                    schema_info['types_detected'].append(typeof)
    
    # Remove duplicates
    schema_info['types_detected'] = list(set(schema_info['types_detected']))
    
    return schema_info


def detect_identity_schema(soup: BeautifulSoup) -> dict:
    """
    Detect identity-related schema (Organization, LocalBusiness, Person, WebSite).
    
    # IMPROVED: Added fallback logic for meta tags when schema not found
    """
    identity_schema = {
        'present': False,
        'types': [],
        'name': '',
        'url': '',
        'has_logo': False,
        'source': 'schema'
    }
    
    # Find all JSON-LD scripts
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    
    for script in json_ld_scripts:
        try:
            import json
            if not script.string:
                continue
            data = json.loads(script.string)
            
            # Handle single object or array
            schemas = [data] if isinstance(data, dict) else data if isinstance(data, list) else []
            
            for schema in schemas:
                if not isinstance(schema, dict):
                    continue
                
                schema_type = schema.get('@type', '')
                if isinstance(schema_type, str):
                    schema_types = [schema_type]
                elif isinstance(schema_type, list):
                    schema_types = schema_type
                else:
                    schema_types = []
                
                # Check for identity-related types
                identity_types = ['Organization', 'LocalBusiness', 'Person', 'WebSite', 'Corporation', 'ProfessionalService']
                found_identity = any(any(t.lower() in st.lower() for st in schema_types) for t in identity_types)
                
                if found_identity:
                    identity_schema['present'] = True
                    identity_schema['types'].extend(schema_types)
                    
                    # Extract name
                    if not identity_schema['name']:
                        identity_schema['name'] = schema.get('name', '')
                    
                    # Extract URL
                    if not identity_schema['url']:
                        identity_schema['url'] = schema.get('url', '')
                    
                    # Check for logo
                    if not identity_schema['has_logo']:
                        identity_schema['has_logo'] = bool(schema.get('logo'))
        except:
            continue
    
    # IMPROVED: Fallback logic - check meta tags if no schema found
    if not identity_schema['present']:
        # Check for og:site_name
        og_site_name = soup.find('meta', property='og:site_name')
        if og_site_name:
            identity_schema['present'] = True
            identity_schema['name'] = og_site_name.get('content', '')
            identity_schema['source'] = 'fallback_meta'
        
        # Check for footer brand name (look for common footer selectors)
        footer = soup.find('footer')
        if footer and not identity_schema['present']:
            footer_text = footer.get_text().strip()
            if len(footer_text) < 100 and footer_text:  # Likely brand name if short
                identity_schema['present'] = True
                identity_schema['name'] = footer_text[:50]
                identity_schema['source'] = 'fallback_footer'
    
    # Remove duplicates
    identity_schema['types'] = list(set(identity_schema['types']))
    
    return identity_schema


def calculate_llm_readability(seo_data: dict, soup: BeautifulSoup) -> dict:
    """
    Calculate LLM readability score based on content structure.
    
    # FIXED: Scoring now properly matches rating and handles JS-heavy logic correctly
    """
    word_count = seo_data.get('word_count', 0)
    h1_count = seo_data.get('h1_count', 0)
    h2_count = seo_data.get('h2_count', 0)
    html_size = seo_data.get('html_size', 0)
    text_content = seo_data.get('text_content', '')
    
    # Calculate text-to-HTML ratio
    text_size = len(text_content)
    text_html_ratio = (text_size / html_size * 100) if html_size > 0 else 0
    
    # Check if JS-heavy (low word count relative to HTML size)
    is_js_heavy = word_count < 50 or text_html_ratio < 5
    
    # FIXED: Base score = 50, then add/deduct
    score = 50
    
    # +20 if word_count > 1000
    if word_count > 1000:
        score += 20
    # +10 if word_count > 500
    elif word_count > 500:
        score += 10
    
    # +10 if H1 + H2 present
    if h1_count >= 1 and h2_count >= 1:
        score += 10
    
    # +10 if good paragraph structure (check for <p> tags)
    paragraphs = soup.find_all('p')
    if len(paragraphs) >= 5:
        score += 10
    
    # -20 if JS-heavy
    if is_js_heavy:
        score -= 20
    
    # -10 if text/html ratio < threshold
    if text_html_ratio < 5:
        score -= 10
    
    # Ensure score is between 0 and 100
    score = max(0, min(100, score))
    
    # Determine rating based on score - FIXED to be consistent
    if score >= 80:
        rating = 'Good'
    elif score >= 50:
        rating = 'Average'
    else:
        rating = 'Poor'
    
    return {
        'score': score,
        'word_count': word_count,
        'has_structure': h1_count >= 1 and h2_count >= 1,
        'is_js_heavy': is_js_heavy,
        'text_html_ratio': round(text_html_ratio, 2),
        'rating': rating
    }


def check_llms_txt(url: str, timeout: int = 30) -> dict:
    """
    Check if llms.txt exists for AI discoverability.
    
    # ADDED: llms.txt detection for AI SEO
    """
    try:
        parsed = urlparse(url)
        llms_url = f"{parsed.scheme}://{parsed.netloc}/llms.txt"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
        }
        response = requests.get(llms_url, headers=headers, timeout=timeout, allow_redirects=True)
        
        return {
            'exists': response.status_code == 200,
            'status_code': response.status_code,
            'url': llms_url
        }
    except:
        return {
            'exists': False,
            'status_code': None,
            'url': llms_url
        }


def detect_google_analytics(soup: BeautifulSoup, html: str) -> dict:
    """
    Detect Google Analytics implementation in HTML.
    Detects GA4, GTM, and Universal Analytics.
    
    # ADDED: Google Analytics detection
    """
    analytics = {
        'detected': False,
        'type': None,
        'measurement_ids': [],
        'implementation': None
    }
    
    html_lower = html.lower()
    
    # Detect GA4 (Google Analytics 4)
    ga4_patterns = ['gtag(', 'gtag.js']
    ga4_detected = False
    measurement_ids = []
    
    if any(pattern in html_lower for pattern in ga4_patterns):
        # Extract measurement IDs (G-XXXXXXXXXX) - more specific pattern to avoid false positives
        # GA4 IDs are typically G- followed by 10+ alphanumeric characters
        ga4_ids = re.findall(r'g-[a-z0-9]{8,}', html_lower)
        measurement_ids.extend([gid.upper() for gid in ga4_ids])
        
        if measurement_ids:
            ga4_detected = True
            analytics['detected'] = True
            analytics['type'] = 'GA4'
            analytics['measurement_ids'] = list(set(measurement_ids))
            analytics['implementation'] = 'gtag'
    
    # Detect Google Tag Manager (GTM)
    gtm_patterns = ['googletagmanager.com', 'gtm-']
    gtm_detected = False
    gtm_ids = []
    
    if any(pattern in html_lower for pattern in gtm_patterns):
        # Extract GTM IDs (GTM-XXXXXXX)
        gtm_ids = re.findall(r'gtm-[a-z0-9]+', html_lower)
        
        if gtm_ids:
            gtm_detected = True
            analytics['detected'] = True
            # If GA4 was not detected, set type to GTM, otherwise keep GA4
            if not analytics['type']:
                analytics['type'] = 'GTM'
            analytics['measurement_ids'].extend([gid.upper() for gid in gtm_ids])
            analytics['measurement_ids'] = list(set(analytics['measurement_ids']))
            # If GTM is the primary implementation
            if not analytics['implementation']:
                analytics['implementation'] = 'tag_manager'
    
    # Detect Universal Analytics (legacy)
    universal_patterns = ['analytics.js', 'ua-']
    universal_detected = False
    ua_ids = []
    
    if any(pattern in html_lower for pattern in universal_patterns):
        # Extract UA IDs (UA-XXXXXXXXX)
        ua_ids = re.findall(r'ua-\d+-\d+', html_lower)
        
        if ua_ids:
            universal_detected = True
            analytics['detected'] = True
            # If GA4/GTM was not detected, set type to Universal
            if not analytics['type']:
                analytics['type'] = 'Universal'
            analytics['measurement_ids'].extend([uid.upper() for uid in ua_ids])
            analytics['measurement_ids'] = list(set(analytics['measurement_ids']))
            if not analytics['implementation']:
                analytics['implementation'] = 'analytics_js'
    
    # Check script src patterns
    scripts = soup.find_all('script')
    for script in scripts:
        src = script.get('src', '').lower()
        if 'googletagmanager.com/gtag/js' in src:
            analytics['detected'] = True
            if not analytics['type']:
                analytics['type'] = 'GA4'
            analytics['implementation'] = 'gtag'
        elif 'google-analytics.com/analytics.js' in src:
            analytics['detected'] = True
            if not analytics['type']:
                analytics['type'] = 'Universal'
            analytics['implementation'] = 'analytics_js'
    
    return analytics


def fetch_pagespeed_metrics(url: str, timeout: int = 30) -> dict:
    """
    Fetch performance metrics from PageSpeed Insights API for both mobile and desktop.
    
    FIXED: Added category=performance, proper timeouts, sequential execution, improved logging
    """
    import time
    
    # Load API key from environment
    api_key = os.getenv('PSI_API_KEY', '')
    
    if not api_key:
        print("[PAGESPEED] No API key found, skipping performance metrics")
        return {
            'mobile': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
            'desktop': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'}
        }
    
    performance_metrics = {
        'mobile': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'},
        'desktop': {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'}
    }
    
    api_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    
    # FIXED: Sequential execution - mobile first, then desktop
    for strategy in ['mobile', 'desktop']:
        start_time = time.time()
        try:
            # FIXED: Added category=performance parameter
            params = {
                'url': url,
                'key': api_key,
                'strategy': strategy,
                'category': 'performance'  # CRITICAL: Only performance category
            }
            
            print(f"[PAGESPEED] Fetching {strategy} metrics for: {url}")
            response = requests.get(api_url, params=params, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # FIXED: Safe extraction
            audits = data.get("lighthouseResult", {}).get("audits", {})
            
            # Extract Core Web Vitals safely
            fcp = audits.get("first-contentful-paint", {}).get("displayValue", "N/A")
            lcp = audits.get("largest-contentful-paint", {}).get("displayValue", "N/A")
            tbt = audits.get("total-blocking-time", {}).get("displayValue", "N/A")
            cls = audits.get("cumulative-layout-shift", {}).get("displayValue", "N/A")
            
            performance_metrics[strategy] = {
                'fcp': fcp,
                'lcp': lcp,
                'tbt': tbt,
                'cls': cls
            }
            
            elapsed = time.time() - start_time
            print(f"[PAGESPEED] {strategy.capitalize()} success ({elapsed:.1f}s): FCP={fcp}, LCP={lcp}, TBT={tbt}, CLS={cls}")
            
        except requests.Timeout:
            elapsed = time.time() - start_time
            print(f"[PAGESPEED] Timeout after {elapsed:.1f}s for {strategy}")
            # FIXED: No retry for timeout - just log and continue
            performance_metrics[strategy] = {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'}
        except requests.RequestException as e:
            elapsed = time.time() - start_time
            print(f"[PAGESPEED] API error for {strategy} ({elapsed:.1f}s): {str(e)}")
            performance_metrics[strategy] = {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'}
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[PAGESPEED] Unexpected error for {strategy} ({elapsed:.1f}s): {str(e)}")
            performance_metrics[strategy] = {'fcp': 'N/A', 'lcp': 'N/A', 'tbt': 'N/A', 'cls': 'N/A'}
    
    return performance_metrics


def check_https_redirect(url: str, final_url: str, timeout: int = 30) -> dict:
    """
    Check if HTTP redirects to HTTPS.
    If input URL is already HTTPS, test the HTTP version separately.
    
    # FIXED: Now properly tests HTTP→HTTPS redirect even when input is HTTPS
    """
    parsed = urlparse(url)
    http_url = f"http://{parsed.netloc}{parsed.path}"
    
    # If input was already HTTPS, test HTTP version separately
    if url.startswith('https://'):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*'
            }
            response = requests.get(http_url, headers=headers, timeout=timeout, allow_redirects=True)
            
            return {
                'redirects_to_https': response.url.startswith('https://'),
                'tested_url': http_url,
                'final_url': response.url,
                'status_code': response.status_code
            }
        except:
            return {
                'redirects_to_https': False,
                'tested_url': http_url,
                'final_url': http_url,
                'status_code': None
            }
    
    # If input was HTTP, check if it redirected to HTTPS
    if final_url.startswith('https://'):
        return {
            'redirects_to_https': True,
            'tested_url': url,
            'final_url': final_url,
            'status_code': None
        }
    
    return {
        'redirects_to_https': False,
        'tested_url': url,
        'final_url': final_url,
        'status_code': None
    }


def extract_security_headers(response_headers: dict) -> dict:
    """
    Extract security headers from HTTP response.
    
    Args:
        response_headers: Dictionary of response headers
    
    Returns:
        dict with security header presence/absence information
    """
    # Normalize header names to lowercase for case-insensitive matching
    normalized_headers = {k.lower(): v for k, v in response_headers.items()}
    
    security_headers = {
        'content_security_policy': {
            'present': 'content-security-policy' in normalized_headers,
            'value': normalized_headers.get('content-security-policy', '')
        },
        'strict_transport_security': {
            'present': 'strict-transport-security' in normalized_headers,
            'value': normalized_headers.get('strict-transport-security', '')
        },
        'x_frame_options': {
            'present': 'x-frame-options' in normalized_headers,
            'value': normalized_headers.get('x-frame-options', '')
        },
        'x_content_type_options': {
            'present': 'x-content-type-options' in normalized_headers,
            'value': normalized_headers.get('x-content-type-options', '')
        },
        'referrer_policy': {
            'present': 'referrer-policy' in normalized_headers,
            'value': normalized_headers.get('referrer-policy', '')
        },
        'permissions_policy': {
            'present': 'permissions-policy' in normalized_headers,
            'value': normalized_headers.get('permissions-policy', '')
        }
    }
    
    return security_headers
