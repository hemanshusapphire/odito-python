"""Enhanced multimedia extraction for images and videos."""

from typing import Dict, List, Any, Optional
import re
from urllib.parse import urlparse, urljoin

class MultimediaExtractor:
    """Extract comprehensive multimedia data with enhanced analysis."""
    
    def __init__(self, soup, entities: List[Dict], base_url: str):
        self.soup = soup
        self.entities = entities
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc.lower()
        
    def extract_all_multimedia(self) -> Dict[str, Any]:
        """Extract all multimedia data."""
        return {
            'enhanced_multimedia': {
                **self._extract_enhanced_images(),
                **self._extract_enhanced_videos(),
                **self._extract_audio_content(),
                **self._extract_multimedia_analysis()
            }
        }
    
    def _extract_enhanced_images(self) -> Dict[str, Any]:
        """Extract comprehensive image data with enhanced analysis."""
        images = self.soup.find_all('img')
        image_objects = [e for e in self.entities if self._get_entity_type(e) == 'ImageObject']
        
        enhanced_images = []
        duplicate_urls = set()
        size_estimates = {}
        
        for i, img in enumerate(images):
            src = img.get('src', '')
            if not src:
                continue
                
            # Normalize URL
            normalized_src = self._normalize_url(src)
            
            # Check for duplicates
            is_duplicate = normalized_src in duplicate_urls
            duplicate_urls.add(normalized_src)
            
            # Extract basic attributes
            img_data = {
                'index': i,
                'src': src,
                'normalized_src': normalized_src,
                'alt': img.get('alt', ''),
                'title': img.get('title', ''),
                'width': self._parse_dimension(img.get('width')),
                'height': self._parse_dimension(img.get('height')),
                'loading': img.get('loading', ''),
                'decoding': img.get('decoding', ''),
                'srcset': img.get('srcset', ''),
                'sizes': img.get('sizes', ''),
                'class': img.get('class', []),
                'id': img.get('id', ''),
                'style': img.get('style', ''),
                'is_duplicate': is_duplicate,
                
                # Enhanced analysis
                'alt_analysis': self._analyze_alt_text(img.get('alt', '')),
                'size_estimate': self._estimate_image_size(img),
                'responsiveness': self._analyze_responsiveness(img),
                'performance': self._analyze_image_performance(img),
                'accessibility': self._analyze_image_accessibility(img),
                'schema_match': self._find_matching_schema_image(normalized_src, image_objects)
            }
            
            enhanced_images.append(img_data)
        
        # Calculate summary metrics
        summary = self._calculate_image_summary(enhanced_images, image_objects)
        
        return {
            'enhanced_images': enhanced_images,
            'image_summary': summary,
            'image_objects_from_schema': self._extract_schema_image_objects(image_objects),
            'image_performance_issues': self._identify_image_performance_issues(enhanced_images),
            'accessibility_issues': self._identify_image_accessibility_issues(enhanced_images)
        }
    
    def _extract_enhanced_videos(self) -> Dict[str, Any]:
        """Extract comprehensive video data with enhanced analysis."""
        video_objects = [e for e in self.entities if self._get_entity_type(e) == 'VideoObject']
        
        # Extract video tags
        video_tags = self.soup.find_all('video')
        enhanced_videos = []
        
        for i, video in enumerate(video_tags):
            video_data = {
                'index': i,
                'src': video.get('src', ''),
                'poster': video.get('poster', ''),
                'width': self._parse_dimension(video.get('width')),
                'height': self._parse_dimension(video.get('height')),
                'controls': video.has_attr('controls'),
                'autoplay': video.has_attr('autoplay'),
                'muted': video.has_attr('muted'),
                'loop': video.has_attr('loop'),
                'playsinline': video.has_attr('playsinline'),
                
                # Enhanced analysis
                'poster_analysis': self._analyze_video_poster(video.get('poster', '')),
                'source_analysis': self._analyze_video_sources(video),
                'accessibility': self._analyze_video_accessibility(video)
            }
            
            enhanced_videos.append(video_data)
        
        # Extract iframe embeds
        iframe_videos = self._extract_iframe_videos()
        
        # Extract schema video objects
        schema_videos = self._extract_schema_video_objects(video_objects)
        
        # Cross-reference HTML and schema videos
        video_cross_reference = self._cross_reference_videos(enhanced_videos, iframe_videos, schema_videos)
        
        # Extract Google Maps embeds
        google_maps_embeds = self._extract_google_maps_embeds()
        
        return {
            'html_videos': enhanced_videos,
            'iframe_videos': iframe_videos,
            'schema_videos': schema_videos,
            'video_cross_reference': video_cross_reference,
            'video_summary': self._calculate_video_summary(enhanced_videos, iframe_videos, schema_videos),
            'google_maps_embeds': google_maps_embeds,
            'google_maps_embed_present': len(google_maps_embeds) > 0
        }
    
    def _extract_audio_content(self) -> Dict[str, Any]:
        """Extract audio content analysis."""
        audio_objects = [e for e in self.entities if self._get_entity_type(e) == 'AudioObject']
        audio_tags = self.soup.find_all('audio')
        
        enhanced_audio = []
        for i, audio in enumerate(audio_tags):
            audio_data = {
                'index': i,
                'src': audio.get('src', ''),
                'controls': audio.has_attr('controls'),
                'autoplay': audio.has_attr('autoplay'),
                'loop': audio.has_attr('loop'),
                'muted': audio.has_attr('muted'),
                'preload': audio.get('preload', ''),
                'source_analysis': self._analyze_audio_sources(audio)
            }
            enhanced_audio.append(audio_data)
        
        return {
            'audio_content': {
                'html_audio': enhanced_audio,
                'schema_audio_objects': audio_objects,
                'total_audio_elements': len(enhanced_audio),
                'has_audio_content': len(enhanced_audio) > 0 or len(audio_objects) > 0
            }
        }
    
    def _extract_multimedia_analysis(self) -> Dict[str, Any]:
        """Overall multimedia analysis and insights."""
        images = self.soup.find_all('img')
        videos = self.soup.find_all('video')
        iframes = self.soup.find_all('iframe')
        
        # Calculate multimedia ratios
        total_text_length = len(self.soup.get_text())
        total_multimedia = len(images) + len(videos) + len(iframes)
        
        multimedia_ratio = total_multimedia / max(total_text_length / 1000, 1)  # multimedia per 1000 chars
        
        return {
            'multimedia_analysis': {
                'content_balance': {
                    'image_count': len(images),
                    'video_count': len(videos),
                    'iframe_count': len(iframes),
                    'total_multimedia': total_multimedia,
                    'text_to_multimedia_ratio': round(multimedia_ratio, 2),
                    'multimedia_heavy': multimedia_ratio > 1.0,
                    'text_heavy': multimedia_ratio < 0.1
                },
                'performance_implications': {
                    'large_images': len([img for img in images if self._is_large_image(img)]),
                    'unoptimized_images': len([img for img in images if self._is_unoptimized_image(img)]),
                    'videos_without_poster': len([video for video in videos if not video.get('poster')]),
                    'external_multimedia': len(self._get_external_multimedia(images, videos, iframes))
                },
                'accessibility_summary': {
                    'images_without_alt': len([img for img in images if not img.get('alt')]),
                    'images_with_poor_alt': len([img for img in images if self._has_poor_alt_text(img)]),
                    'videos_without_controls': len([video for video in videos if not video.has_attr('controls')]),
                    'multimedia_accessibility_score': self._calculate_multimedia_accessibility_score(images, videos)
                }
            }
        }
    
    def _analyze_alt_text(self, alt_text: str) -> Dict[str, Any]:
        """Analyze alt text quality."""
        if not alt_text:
            return {
                'present': False,
                'word_count': 0,
                'character_count': 0,
                'quality_score': 0,
                'issues': ['Missing alt text'],
                'is_descriptive': False,
                'is_too_short': False,
                'is_too_long': False,
                'contains_file_extension': False
            }
        
        word_count = len(alt_text.split())
        char_count = len(alt_text)
        
        issues = []
        quality_score = 100
        
        # Check for common issues
        if word_count < 3:
            issues.append('Too short - less than 3 words')
            quality_score -= 30
            is_too_short = True
        else:
            is_too_short = False
            
        if word_count > 20:
            issues.append('Too long - more than 20 words')
            quality_score -= 20
            is_too_long = True
        else:
            is_too_long = False
        
        # Check for file extensions
        if any(ext in alt_text.lower() for ext in ['.jpg', '.png', '.gif', '.jpeg', '.webp']):
            issues.append('Contains file extension')
            quality_score -= 20
            contains_file_extension = True
        else:
            contains_file_extension = False
        
        # Check for generic text
        generic_phrases = ['image', 'picture', 'photo', 'graphic', 'icon', 'logo']
        if any(phrase in alt_text.lower() for phrase in generic_phrases):
            issues.append('Contains generic phrases')
            quality_score -= 15
        
        is_descriptive = word_count >= 5 and not contains_file_extension
        
        return {
            'present': True,
            'word_count': word_count,
            'character_count': char_count,
            'quality_score': max(0, quality_score),
            'issues': issues,
            'is_descriptive': is_descriptive,
            'is_too_short': is_too_short,
            'is_too_long': is_too_long,
            'contains_file_extension': contains_file_extension
        }
    
    def _estimate_image_size(self, img) -> Dict[str, Any]:
        """Estimate image file size and dimensions."""
        width = self._parse_dimension(img.get('width'))
        height = self._parse_dimension(img.get('height'))
        
        # Basic size estimation
        if width and height:
            # Rough estimate: 3 bytes per pixel for RGB
            estimated_bytes = width * height * 3
            estimated_kb = estimated_bytes / 1024
            estimated_mb = estimated_kb / 1024
            
            # Categorize size
            if estimated_mb > 5:
                size_category = 'very_large'
            elif estimated_mb > 1:
                size_category = 'large'
            elif estimated_kb > 500:
                size_category = 'medium'
            else:
                size_category = 'small'
        else:
            estimated_bytes = 0
            estimated_kb = 0
            estimated_mb = 0
            size_category = 'unknown'
        
        return {
            'width': width,
            'height': height,
            'has_dimensions': bool(width and height),
            'estimated_size_bytes': estimated_bytes,
            'estimated_size_kb': estimated_kb,
            'estimated_size_mb': estimated_mb,
            'size_category': size_category,
            'aspect_ratio': width / height if width and height else None
        }
    
    def _analyze_responsiveness(self, img) -> Dict[str, Any]:
        """Analyze image responsiveness."""
        srcset = img.get('srcset', '')
        sizes = img.get('sizes', '')
        loading = img.get('loading', '')
        
        is_responsive = bool(srcset or sizes)
        is_lazy = loading == 'lazy'
        
        responsiveness_score = 0
        if is_responsive:
            responsiveness_score += 50
        if is_lazy:
            responsiveness_score += 30
        if sizes:
            responsiveness_score += 20
        
        return {
            'is_responsive': is_responsive,
            'has_srcset': bool(srcset),
            'has_sizes': bool(sizes),
            'is_lazy_loaded': is_lazy,
            'loading_strategy': loading,
            'responsiveness_score': responsiveness_score,
            'modern_image_handling': is_responsive and is_lazy
        }
    
    def _analyze_image_performance(self, img) -> Dict[str, Any]:
        """Analyze image performance aspects."""
        src = img.get('src', '')
        
        # Check format
        format_indicators = {
            'webp': src.lower().endswith('.webp'),
            'avif': src.lower().endswith('.avif'),
            'jpg': src.lower().endswith(('.jpg', '.jpeg')),
            'png': src.lower().endswith('.png'),
            'gif': src.lower().endswith('.gif'),
            'svg': src.lower().endswith('.svg')
        }
        
        # Check for modern format
        modern_formats = ['webp', 'avif']
        is_modern_format = any(format_indicators[fmt] for fmt in modern_formats)
        
        # Check for optimization indicators
        optimization_indicators = {
            'has_width': bool(img.get('width')),
            'has_height': bool(img.get('height')),
            'has_loading_attr': bool(img.get('loading')),
            'has_decoding_attr': bool(img.get('decoding'))
        }
        
        performance_score = 0
        if is_modern_format:
            performance_score += 30
        if optimization_indicators['has_width'] and optimization_indicators['has_height']:
            performance_score += 25
        if optimization_indicators['has_loading_attr']:
            performance_score += 25
        if optimization_indicators['has_decoding_attr']:
            performance_score += 20
        
        return {
            'format': format_indicators,
            'is_modern_format': is_modern_format,
            'optimization_indicators': optimization_indicators,
            'performance_score': performance_score,
            'needs_optimization': performance_score < 50
        }
    
    def _analyze_image_accessibility(self, img) -> Dict[str, Any]:
        """Analyze image accessibility."""
        alt = img.get('alt', '')
        title = img.get('title', '')
        role = img.get('role', '')
        aria_label = img.get('aria-label', '')
        
        accessibility_score = 0
        issues = []
        
        # Alt text is primary accessibility feature
        if alt:
            accessibility_score += 40
            if len(alt.split()) >= 5:
                accessibility_score += 20
        else:
            issues.append('Missing alt text')
        
        # Additional accessibility features
        if title:
            accessibility_score += 10
        if aria_label:
            accessibility_score += 15
        if role == 'presentation' and not alt:
            # Explicitly marked as decorative
            accessibility_score += 15
        
        return {
            'accessibility_score': accessibility_score,
            'accessibility_issues': issues,
            'is_accessible': accessibility_score >= 60,
            'is_decorative': role == 'presentation',
            'has_accessibility_enhancements': bool(title or aria_label)
        }
    
    def _find_matching_schema_image(self, image_url: str, image_objects: List[Dict]) -> Optional[Dict]:
        """Find matching ImageObject schema for an image."""
        for img_obj in image_objects:
            obj_url = img_obj.get('url') or img_obj.get('contentUrl')
            if obj_url and (obj_url in image_url or image_url in obj_url):
                return {
                    'schema_id': img_obj.get('@id'),
                    'schema_url': obj_url,
                    'schema_name': img_obj.get('name'),
                    'schema_caption': img_obj.get('caption'),
                    'schema_description': img_obj.get('description')
                }
        return None
    
    def _calculate_image_summary(self, enhanced_images: List[Dict], image_objects: List[Dict]) -> Dict[str, Any]:
        """Calculate comprehensive image summary."""
        if not enhanced_images:
            return {
                'total_images': 0,
                'with_alt': 0,
                'without_alt': 0,
                'duplicate_images': 0,
                'responsive_images': 0,
                'optimized_images': 0,
                'accessible_images': 0,
                'schema_matches': 0
            }
        
        with_alt = len([img for img in enhanced_images if img['alt_analysis']['present']])
        without_alt = len(enhanced_images) - with_alt
        duplicates = len([img for img in enhanced_images if img['is_duplicate']])
        responsive = len([img for img in enhanced_images if img['responsiveness']['is_responsive']])
        optimized = len([img for img in enhanced_images if img['performance']['performance_score'] >= 50])
        accessible = len([img for img in enhanced_images if img['accessibility']['accessibility_score'] >= 60])
        schema_matches = len([img for img in enhanced_images if img['schema_match']])
        
        return {
            'total_images': len(enhanced_images),
            'with_alt': with_alt,
            'without_alt': without_alt,
            'duplicate_images': duplicates,
            'responsive_images': responsive,
            'optimized_images': optimized,
            'accessible_images': accessible,
            'schema_matches': schema_matches,
            'alt_coverage_percentage': (with_alt / len(enhanced_images) * 100),
            'responsiveness_percentage': (responsive / len(enhanced_images) * 100),
            'optimization_percentage': (optimized / len(enhanced_images) * 100),
            'accessibility_percentage': (accessible / len(enhanced_images) * 100),
            'schema_match_percentage': (schema_matches / len(enhanced_images) * 100) if enhanced_images else 0
        }
    
    def _extract_iframe_videos(self) -> List[Dict[str, Any]]:
        """Extract iframe video embeds."""
        iframes = self.soup.find_all('iframe')
        video_platforms = ['youtube.com', 'youtu.be', 'vimeo.com', 'dailymotion.com', 'twitch.tv']
        
        enhanced_iframes = []
        for i, iframe in enumerate(iframes):
            src = iframe.get('src', '')
            if not any(platform in src for platform in video_platforms):
                continue
                
            platform = next((platform for platform in video_platforms if platform in src), 'unknown')
            video_id = self._extract_video_id_from_url(src, platform)
            
            iframe_data = {
                'index': i,
                'src': src,
                'low_entity_density': entity_metrics.get('entity_per_1000_words', 0) < 2,  
                'width': self._parse_dimension(iframe.get('width')),
                'height': self._parse_dimension(iframe.get('height')),
                'platform': platform,
                'video_id': video_id,
                'allowfullscreen': iframe.has_attr('allowfullscreen') or 'allowfullscreen' in iframe.get('allow', ''),
                'loading': iframe.get('loading', ''),
                'accessibility': {
                    'has_title': bool(iframe.get('title')),
                    'has_accessible_name': bool(iframe.get('title') or iframe.get('aria-label'))
                }
            }
            
            enhanced_iframes.append(iframe_data)
        
        return enhanced_iframes
    
    def _extract_google_maps_embeds(self) -> List[Dict[str, Any]]:
        """Extract Google Maps embeds from iframes."""
        iframes = self.soup.find_all('iframe')
        google_maps_embeds = []
        
        for i, iframe in enumerate(iframes):
            src = iframe.get('src', '')
            if 'google.com/maps/embed' in src:
                google_maps_embeds.append({
                    'index': i,
                    'src': src,
                    'title': iframe.get('title', ''),
                    'width': iframe.get('width'),
                    'height': iframe.get('height')
                })
        
        return google_maps_embeds
    
    def _extract_schema_video_objects(self, video_objects: List[Dict]) -> List[Dict[str, Any]]:
        """Extract enhanced data from VideoObject schema."""
        enhanced_schema_videos = []
        
        for i, video_obj in enumerate(video_objects):
            enhanced_data = {
                'index': i,
                'schema_id': video_obj.get('@id'),
                'name': video_obj.get('name'),
                'description': video_obj.get('description'),
                'contentUrl': video_obj.get('contentUrl'),
                'embedUrl': video_obj.get('embedUrl'),
                'thumbnail': video_obj.get('thumbnail'),
                'uploadDate': video_obj.get('uploadDate'),
                'duration': video_obj.get('duration'),
                'transcript': video_obj.get('transcript'),
                'caption': video_obj.get('caption'),
                
                # Enhanced analysis
                'duration_analysis': self._analyze_video_duration(video_obj.get('duration')),
                'thumbnail_analysis': self._analyze_video_thumbnail(video_obj.get('thumbnail')),
                'transcript_analysis': self._analyze_video_transcript(video_obj.get('transcript'))
            }
            
            enhanced_schema_videos.append(enhanced_data)
        
        return enhanced_schema_videos
    
    # Helper methods
    def _get_entity_type(self, entity: Dict) -> Optional[str]:
        """Get entity type handling multiple types."""
        entity_type = entity.get('@type')
        if isinstance(entity_type, list):
            return entity_type[0] if entity_type else None
        return entity_type
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL to absolute form."""
        if url.startswith(('http://', 'https://')):
            return url
        return urljoin(self.base_url, url)
    
    def _parse_dimension(self, dimension: Any) -> Optional[int]:
        """Parse dimension value to integer."""
        if dimension is None:
            return None
        try:
            return int(str(dimension).replace('px', ''))
        except (ValueError, TypeError):
            return None
    
    def _is_large_image(self, img) -> bool:
        """Check if image is likely large."""
        width = self._parse_dimension(img.get('width'))
        height = self._parse_dimension(img.get('height'))
        return width and height and (width > 1200 or height > 800)
    
    def _is_unoptimized_image(self, img) -> bool:
        """Check if image appears unoptimized."""
        src = img.get('src', '').lower()
        has_dimensions = bool(img.get('width') and img.get('height'))
        has_loading = bool(img.get('loading'))
        
        return (not has_dimensions or 
                not has_loading or 
                any(src.endswith(ext) for ext in ['.jpg', '.jpeg', '.png']) and not any(src.endswith(ext) for ext in ['.webp', '.avif']))
    
    def _get_external_multimedia(self, images, videos, iframes) -> List[str]:
        """Get external multimedia URLs."""
        external_urls = []
        
        for img in images:
            src = img.get('src', '')
            if src and urlparse(src).netloc != self.base_domain:
                external_urls.append(src)
        
        for video in videos:
            src = video.get('src', '')
            if src and urlparse(src).netloc != self.base_domain:
                external_urls.append(src)
        
        for iframe in iframes:
            src = iframe.get('src', '')
            if src and urlparse(src).netloc != self.base_domain:
                external_urls.append(src)
        
        return external_urls
    
    def _has_poor_alt_text(self, img) -> bool:
        """Check if image has poor alt text."""
        alt = img.get('alt', '')
        if not alt:
            return True
        
        word_count = len(alt.split())
        return word_count < 3 or any(ext in alt.lower() for ext in ['.jpg', '.png', '.gif'])
    
    def _calculate_multimedia_accessibility_score(self, images, videos) -> float:
        """Calculate overall multimedia accessibility score."""
        if not images and not videos:
            return 100.0
        
        total_score = 0
        total_items = 0
        
        # Image accessibility
        for img in images:
            alt = img.get('alt', '')
            if alt:
                total_score += 40
                if len(alt.split()) >= 5:
                    total_score += 20
            total_items += 100
        
        # Video accessibility
        for video in videos:
            if video.has_attr('controls'):
                total_score += 50
            if video.get('poster'):
                total_score += 30
            total_items += 100
        
        return (total_score / total_items * 100) if total_items > 0 else 0
    
    def _extract_video_id_from_url(self, url: str, platform: str) -> Optional[str]:
        """Extract video ID from URL based on platform."""
        if platform == 'youtube.com':
            match = re.search(r'v=([^&]+)', url)
            return match.group(1) if match else None
        elif platform == 'youtu.be':
            return url.split('/')[-1].split('?')[0]
        elif platform == 'vimeo.com':
            match = re.search(r'vimeo\.com/(\d+)', url)
            return match.group(1) if match else None
        return None
    
    def _analyze_video_duration(self, duration: str) -> Dict[str, Any]:
        """Analyze video duration."""
        if not duration:
            return {'present': False, 'seconds': 0, 'formatted': ''}
        
        # Parse ISO 8601 duration
        match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            
            total_seconds = hours * 3600 + minutes * 60 + seconds
            formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            
            return {
                'present': True,
                'seconds': total_seconds,
                'formatted': formatted,
                'is_short': total_seconds < 60,
                'is_long': total_seconds > 3600
            }
        
        return {'present': False, 'seconds': 0, 'formatted': ''}
    
    def _analyze_video_thumbnail(self, thumbnail: Any) -> Dict[str, Any]:
        """Analyze video thumbnail."""
        if not thumbnail:
            return {'present': False}
        
        if isinstance(thumbnail, dict):
            return {
                'present': True,
                'url': thumbnail.get('url') or thumbnail.get('contentUrl'),
                'width': thumbnail.get('width'),
                'height': thumbnail.get('height'),
                'format': thumbnail.get('format', 'unknown')
            }
        elif isinstance(thumbnail, str):
            return {
                'present': True,
                'url': thumbnail,
                'width': None,
                'height': None,
                'format': 'unknown'
            }
        
        return {'present': False}
    
    def _analyze_video_transcript(self, transcript: Any) -> Dict[str, Any]:
        """Analyze video transcript."""
        if not transcript:
            return {'present': False}
        
        transcript_text = str(transcript)
        word_count = len(transcript_text.split())
        char_count = len(transcript_text)
        
        return {
            'present': True,
            'word_count': word_count,
            'character_count': char_count,
            'estimated_reading_time': word_count / 200  # 200 words per minute
        }
    
    def _cross_reference_videos(self, html_videos: List[Dict], iframe_videos: List[Dict], schema_videos: List[Dict]) -> Dict[str, Any]:
        """Cross-reference HTML videos, iframes, and schema videos."""
        return {
            'html_to_schema_matches': len(self._find_html_schema_matches(html_videos, schema_videos)),
            'iframe_to_schema_matches': len(self._find_iframe_schema_matches(iframe_videos, schema_videos)),
            'total_unique_videos': len(set([
                v.get('src', '') for v in html_videos
            ] + [
                v.get('src', '') for v in iframe_videos
            ] + [
                v.get('contentUrl', '') for v in schema_videos
            ]))
        }
    
    def _find_html_schema_matches(self, html_videos: List[Dict], schema_videos: List[Dict]) -> List[Dict]:
        """Find matches between HTML videos and schema videos."""
        matches = []
        for html_video in html_videos:
            for schema_video in schema_videos:
                if (html_video.get('src') and schema_video.get('contentUrl') and
                    html_video['src'] in schema_video['contentUrl']):
                    matches.append({
                        'html_src': html_video['src'],
                        'schema_url': schema_video['contentUrl'],
                        'schema_name': schema_video.get('name')
                    })
        return matches
    
    def _find_iframe_schema_matches(self, iframe_videos: List[Dict], schema_videos: List[Dict]) -> List[Dict]:
        """Find matches between iframe videos and schema videos."""
        matches = []
        for iframe in iframe_videos:
            for schema_video in schema_videos:
                if (iframe.get('video_id') and 
                    iframe['video_id'] in str(schema_video.get('embedUrl', ''))):
                    matches.append({
                        'iframe_src': iframe['src'],
                        'schema_embedUrl': schema_video.get('embedUrl'),
                        'schema_name': schema_video.get('name')
                    })
        return matches
    
    def _calculate_video_summary(self, html_videos: List[Dict], iframe_videos: List[Dict], schema_videos: List[Dict]) -> Dict[str, Any]:
        """Calculate comprehensive video summary."""
        return {
            'total_videos': len(html_videos) + len(iframe_videos),
            'html_video_count': len(html_videos),
            'iframe_video_count': len(iframe_videos),
            'schema_video_count': len(schema_videos),
            'videos_with_poster': len([v for v in html_videos if v.get('poster')]),
            'videos_with_controls': len([v for v in html_videos if v.get('controls')]),
            'schema_videos_with_duration': len([v for v in schema_videos if v.get('duration')]),
            'schema_videos_with_thumbnail': len([v for v in schema_videos if v.get('thumbnail')]),
            'schema_videos_with_transcript': len([v for v in schema_videos if v.get('transcript')])
        }
    
    def _extract_schema_image_objects(self, image_objects: List[Dict]) -> List[Dict[str, Any]]:
        """Extract enhanced data from ImageObject schema."""
        enhanced_schema_images = []
        
        for i, img_obj in enumerate(image_objects):
            enhanced_data = {
                'index': i,
                'schema_id': img_obj.get('@id'),
                'name': img_obj.get('name'),
                'description': img_obj.get('description'),
                'contentUrl': img_obj.get('contentUrl'),
                'url': img_obj.get('url'),
                'thumbnail': img_obj.get('thumbnail'),
                'caption': img_obj.get('caption'),
                'width': img_obj.get('width'),
                'height': img_obj.get('height'),
                'format': img_obj.get('encodingFormat', 'unknown')
            }
            
            enhanced_schema_images.append(enhanced_data)
        
        return enhanced_schema_images
    
    def _identify_image_performance_issues(self, enhanced_images: List[Dict]) -> List[Dict[str, Any]]:
        """Identify image performance issues."""
        issues = []
        
        for img in enhanced_images:
            img_issues = []
            
            if img['performance']['needs_optimization']:
                img_issues.append('Needs performance optimization')
            
            if img['size_estimate']['size_category'] in ['large', 'very_large']:
                img_issues.append('Large image size')
            
            # === CRITICAL FIX: Safe access to is_lazy field ===
            is_lazy_loaded = img.get('responsiveness', {}).get('is_lazy', False)
            if not is_lazy_loaded:
                img_issues.append('Missing lazy loading')
            
            if img_issues:
                issues.append({
                    'image_url': img['normalized_src'],
                    'issues': img_issues,
                    'performance_score': img['performance']['performance_score']
                })
        
        return issues
    
    def _identify_image_accessibility_issues(self, enhanced_images: List[Dict]) -> List[Dict[str, Any]]:
        """Identify image accessibility issues."""
        issues = []
        
        for img in enhanced_images:
            if not img['accessibility']['is_accessible']:
                issues.append({
                    'image_url': img['normalized_src'],
                    'accessibility_score': img['accessibility']['accessibility_score'],
                    'accessibility_issues': img['accessibility']['accessibility_issues']
                })
        
        return issues
    
    def _analyze_video_poster(self, poster_url: str) -> Dict[str, Any]:
        """Analyze video poster image."""
        if not poster_url:
            return {'present': False}
        
        return {
            'present': True,
            'url': poster_url,
            'is_local': not urlparse(poster_url).netloc or urlparse(poster_url).netloc == self.base_domain
        }
    
    def _analyze_video_sources(self, video) -> Dict[str, Any]:
        """Analyze video source elements."""
        sources = video.find_all('source')
        
        source_analysis = []
        for source in sources:
            source_data = {
                'src': source.get('src', ''),
                'type': source.get('type', ''),
                'media': source.get('media', '')
            }
            source_analysis.append(source_data)
        
        return {
            'has_source_elements': len(sources) > 0,
            'source_count': len(sources),
            'sources': source_analysis
        }
    
    def _analyze_video_accessibility(self, video) -> Dict[str, Any]:
        """Analyze video accessibility."""
        return {
            'has_controls': video.has_attr('controls'),
            'has_poster': bool(video.get('poster')),
            'has_title': bool(video.get('title')),
            'has_accessible_name': bool(video.get('title') or video.get('aria-label'))
        }
    
    def _analyze_audio_sources(self, audio) -> Dict[str, Any]:
        """Analyze audio source elements."""
        sources = audio.find_all('source')
        
        source_analysis = []
        for source in sources:
            source_data = {
                'src': source.get('src', ''),
                'type': source.get('type', '')
            }
            source_analysis.append(source_data)
        
        return {
            'has_source_elements': len(sources) > 0,
            'source_count': len(sources),
            'sources': source_analysis
        }
