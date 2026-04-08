"""Full Extraction Coordinator - Centralized comprehensive data extraction."""

from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup

# Import extractors
try:
    from extractors.entity_validator import EntityValidator
    from extractors.page_type_extractor import PageTypeExtractor
    from extractors.metadata_enhancer import MetadataEnhancer
    from extractors.link_analyzer import LinkAnalyzer
    from extractors.multimedia_extractor import MultimediaExtractor
    from extractors.video_enhancer import VideoEnhancer  # Backward compatibility
except ImportError:
    from .extractors.entity_validator import EntityValidator
    from .extractors.page_type_extractor import PageTypeExtractor
    from .extractors.metadata_enhancer import MetadataEnhancer
    from .extractors.link_analyzer import LinkAnalyzer
    from .extractors.multimedia_extractor import MultimediaExtractor
    from .extractors.video_enhancer import VideoEnhancer  # Backward compatibility

class FullExtractionCoordinator:
    """Centralized coordinator for comprehensive AI visibility extraction."""
    
    def __init__(self, soup: BeautifulSoup, url: str, json_ld_data: Dict):
        self.soup = soup
        self.url = url
        self.json_ld_data = json_ld_data
        
        # Extract core data from JSON-LD
        self.entities = json_ld_data.get('parsed_entities', [])
        self.unified_graph = json_ld_data.get('unified_entity_graph', {})
        self.canonical_root = json_ld_data.get('canonical_root', url)
        
        # Initialize extractors
        self.entity_validator = EntityValidator(self.canonical_root)
        self.page_type_extractor = PageTypeExtractor(soup, self.entities, url)
        self.metadata_enhancer = MetadataEnhancer(soup, url)
        self.link_analyzer = LinkAnalyzer(soup, url)
        self.multimedia_extractor = MultimediaExtractor(soup, self.entities, url)
        self.video_enhancer = VideoEnhancer(soup, self.entities, url)  # Backward compatibility
        
    def extract_comprehensive_dataset(self) -> Dict[str, Any]:
        """Extract complete dataset with all enhanced data."""
        try:
            # Phase 1: Entity validation and analysis
            entity_validation = self.entity_validator.validate_entities(
                self.entities, 
                self.json_ld_data.get('entity_relationship_graph', [])
            )
            
            # Phase 2: Page type specific extraction
            page_type_properties = self.page_type_extractor.extract_all_page_type_properties()
            
            # Phase 3: Enhanced metadata extraction
            enhanced_metadata = self.metadata_enhancer.extract_all_metadata()
            
            # Phase 4: Comprehensive link analysis
            link_analysis = self.link_analyzer.extract_all_links()
            
            # Phase 5: Multimedia extraction and analysis
            multimedia_analysis = self.multimedia_extractor.extract_all_multimedia()
            
            # Phase 6: Cross-page preparation data
            cross_page_data = self._extract_cross_page_preparation_data()
            
            # Phase 7: Content structure enhancement
            enhanced_content_structure = self._enhance_content_structure()
            
            # Phase 8: Technical SEO signals
            technical_seo_signals = self._extract_technical_seo_signals()
            
            # Combine all enhanced data
            enhanced_dataset = {
                'enhanced_extraction_v2': {
                    'extraction_metadata': {
                        'extraction_version': '2.0',
                        'extraction_timestamp': self._get_timestamp(),
                        'url': self.url,
                        'canonical_root': self.canonical_root,
                        'total_entities_extracted': len(self.entities),
                        'extraction_phases_completed': [
                            'entity_validation',
                            'page_type_properties', 
                            'enhanced_metadata',
                            'link_analysis',
                            'multimedia_analysis',
                            'cross_page_preparation',
                            'content_structure_enhancement',
                            'technical_seo_signals'
                        ]
                    },
                    **entity_validation,
                    **page_type_properties,
                    **enhanced_metadata,
                    **link_analysis,
                    **multimedia_analysis,
                    'cross_page_preparation': cross_page_data,
                    'enhanced_content_structure': enhanced_content_structure,
                    'technical_seo_signals': technical_seo_signals,
                    'extraction_summary': self._generate_extraction_summary(
                        entity_validation, page_type_properties, enhanced_metadata,
                        link_analysis, multimedia_analysis
                    )
                }
            }
            
            return enhanced_dataset
            
        except Exception as e:
            # Return error-safe structure
            return {
                'enhanced_extraction_v2': {
                    'extraction_error': {
                        'error_message': str(e),
                        'error_type': 'extraction_coordinator_error',
                        'partial_data_available': False
                    }
                }
            }
    
    def _extract_cross_page_preparation_data(self) -> Dict[str, Any]:
        """Extract data needed for cross-page consistency validation."""
        # Extract page title
        title_tag = self.soup.find('title')
        page_title = title_tag.get_text().strip() if title_tag else ''
        
        # Find primary entity
        primary_entity = None
        if self.entities:
            # Use existing primary entity detection logic
            primary_entity = self.page_type_extractor.primary_entity
        
        # Create entity definition fingerprint
        entity_fingerprint = self._create_entity_fingerprint(self.entities)
        
        # Detect organization reference
        organization_reference = self._detect_organization_reference(self.entities)
        
        # Extract schema version information
        schema_version = self._detect_schema_version()
        
        # Extract canonical root domain
        from urllib.parse import urlparse
        canonical_domain = urlparse(self.canonical_root).netloc.lower()
        
        return {
            'page_title': page_title,
            'primary_entity_id': primary_entity.get('@id') if primary_entity else None,
            'primary_entity_type': primary_entity.get('@type') if primary_entity else None,
            'entity_definitions_hash': entity_fingerprint,
            'organization_reference_used': organization_reference,
            'schema_version_used': schema_version,
            'canonical_root_domain': canonical_domain,
            'page_fingerprint': self._create_page_fingerprint(page_title, entity_fingerprint),
            'extraction_readiness': {
                'ready_for_cross_page_validation': bool(primary_entity and entity_fingerprint),
                'has_organization_context': bool(organization_reference),
                'has_consistent_schema': bool(schema_version)
            }
        }
    
    def _enhance_content_structure(self) -> Dict[str, Any]:
        """Enhance existing content structure with additional analysis."""
        # Get existing content sections (from original extraction)
        existing_sections = self.json_ld_data.get('content_sections', {})
        
        # Enhanced analysis
        enhanced_sections = {}
        for section_name, section_data in existing_sections.items():
            if isinstance(section_data, dict) and section_name != 'extraction_summary':
                enhanced_sections[section_name] = {
                    **section_data,
                    'enhanced_analysis': {
                        'keyword_density': self._calculate_keyword_density(section_data.get('text', '')),
                        'readability_score': self._calculate_readability_score(section_data.get('text', '')),
                        'semantic_structure': self._analyze_semantic_structure(section_data.get('text', '')),
                        'content_quality_indicators': self._analyze_content_quality(section_data.get('text', ''))
                    }
                }
        
        # Overall content analysis
        all_text = ' '.join([
            section.get('text', '') for section in existing_sections.values() 
            if isinstance(section, dict) and section.get('text')
        ])
        
        return {
            'enhanced_sections': enhanced_sections,
            'overall_content_analysis': {
                'total_word_count': len(all_text.split()),
                'total_character_count': len(all_text),
                'content_depth_score': self._calculate_content_depth(enhanced_sections),
                'topic_coherence': self._analyze_topic_coherence(all_text),
                'content_structure_quality': self._evaluate_content_structure_quality(enhanced_sections)
            }
        }
    
    def _extract_technical_seo_signals(self) -> Dict[str, Any]:
        """Extract technical SEO signals and indicators."""
        # Head analysis
        head = self.soup.find('head')
        head_signals = {}
        
        if head:
            # Meta tags analysis
            meta_tags = head.find_all('meta')
            head_signals['meta_tags'] = {
                'total_count': len(meta_tags),
                'unique_names': len(set(meta.get('name', '') for meta in meta_tags if meta.get('name'))),
                'charset_present': bool(head.find('meta', attrs={'charset': True})),
                'viewport_present': bool(head.find('meta', attrs={'name': 'viewport'})),
                'description_present': bool(head.find('meta', attrs={'name': 'description'})),
                'keywords_present': bool(head.find('meta', attrs={'name': 'keywords'})),
                'robots_present': bool(head.find('meta', attrs={'name': 'robots'})),
                'canonical_present': bool(head.find('link', rel='canonical'))
            }
            
            # Link tags analysis
            link_tags = head.find_all('link')
            head_signals['link_tags'] = {
                'total_count': len(link_tags),
                'stylesheets': len([link for link in link_tags if link.get('rel') == 'stylesheet']),
                'preconnect': len([link for link in link_tags if link.get('rel') == 'preconnect']),
                'dns_prefetch': len([link for link in link_tags if link.get('rel') == 'dns-prefetch']),
                'preload': len([link for link in link_tags if link.get('rel') == 'preload']),
                'hreflang_count': len([link for link in link_tags if link.get('hreflang')])
            }
            
            # Script tags analysis
            script_tags = head.find_all('script')
            head_signals['script_tags'] = {
                'total_count': len(script_tags),
                'json_ld_count': len([script for script in script_tags if script.get('type') == 'application/ld+json']),
                'external_scripts': len([script for script in script_tags if script.get('src')]),
                'inline_scripts': len([script for script in script_tags if not script.get('src')])
            }
        
        # Body structure analysis
        body = self.soup.find('body')
        body_signals = {}
        
        if body:
            body_signals['structure'] = {
                'has_header': bool(body.find('header')),
                'has_main': bool(body.find('main')),
                'has_nav': bool(body.find('nav')),
                'has_footer': bool(body.find('footer')),
                'has_section': bool(body.find('section')),
                'has_article': bool(body.find('article')),
                'has_aside': bool(body.find('aside')),
                'semantic_html_usage': self._calculate_semantic_html_usage(body)
            }
            
            # Performance indicators
            body_signals['performance'] = {
                'total_images': len(body.find_all('img')),
                'total_scripts': len(body.find_all('script')),
                'total_styles': len(body.find_all('link', rel='stylesheet')),
                'images_without_dimensions': len([
                    img for img in body.find_all('img') 
                    if not img.get('width') or not img.get('height')
                ]),
                'scripts_with_async': len([script for script in body.find_all('script') if script.get('async')]),
                'scripts_with_defer': len([script for script in body.find_all('script') if script.get('defer')])
            }
        
        return {
            'head_signals': head_signals,
            'body_signals': body_signals,
            'technical_seo_score': self._calculate_technical_seo_score(head_signals, body_signals)
        }
    
    def _create_entity_fingerprint(self, entities: List[Dict]) -> str:
        """Create fingerprint of entity definitions for cross-page comparison."""
        if not entities:
            return ''
        
        # Sort entities by @id for consistent fingerprinting
        sorted_entities = sorted(entities, key=lambda x: x.get('@id', ''))
        
        # Create fingerprint string
        fingerprint_parts = []
        for entity in sorted_entities:
            entity_id = entity.get('@id', '')
            entity_type = entity.get('@type', '')
            
            # Create property fingerprint (sorted keys)
            properties = sorted([k for k in entity.keys() if k not in ['@id', '@type', '@context']])
            property_fingerprint = '|'.join(properties)
            
            fingerprint_parts.append(f"{entity_type}:{entity_id}:{property_fingerprint}")
        
        return hash('|'.join(fingerprint_parts))
    
    def _detect_organization_reference(self, entities: List[Dict]) -> Optional[Dict]:
        """Detect if page references an Organization entity."""
        for entity in entities:
            entity_type = entity.get('@type')
            if isinstance(entity_type, list):
                if 'Organization' in entity_type:
                    return {
                        'entity_id': entity.get('@id'),
                        'entity_name': entity.get('name'),
                        'reference_type': 'direct_entity'
                    }
            elif entity_type == 'Organization':
                return {
                    'entity_id': entity.get('@id'),
                    'entity_name': entity.get('name'),
                    'reference_type': 'direct_entity'
                }
        
        # Check for organization references in properties
        for entity in entities:
            for key, value in entity.items():
                if isinstance(value, dict) and value.get('@type') == 'Organization':
                    return {
                        'entity_id': value.get('@id'),
                        'entity_name': value.get('name'),
                        'reference_type': 'property_reference'
                    }
        
        return None
    
    def _detect_schema_version(self) -> Optional[str]:
        """Detect schema.org version being used."""
        # Look for version in @context
        contexts = self.json_ld_data.get('contexts', [])
        
        for context in contexts:
            if 'schema.org' in context:
                if 'v' in context or 'version' in context:
                    return context
                elif 'http://schema.org' in context:
                    return 'http://schema.org'
                elif 'https://schema.org' in context:
                    return 'https://schema.org'
        
        return None
    
    def _create_page_fingerprint(self, title: str, entity_fingerprint: str) -> str:
        """Create overall page fingerprint."""
        combined = f"{title}:{entity_fingerprint}"
        return hash(combined)
    
    def _calculate_keyword_density(self, text: str) -> Dict[str, Any]:
        """Calculate keyword density for content."""
        if not text:
            return {'word_count': 0, 'unique_words': 0, 'density_analysis': {}}
        
        words = text.lower().split()
        word_count = len(words)
        word_freq = {}
        
        for word in words:
            # Remove punctuation and normalize
            clean_word = ''.join(c for c in word if c.isalnum())
            if clean_word:
                word_freq[clean_word] = word_freq.get(clean_word, 0) + 1
        
        # Calculate density
        density_analysis = {}
        for word, freq in sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]:
            density = (freq / word_count) * 100
            density_analysis[word] = round(density, 2)
        
        return {
            'word_count': word_count,
            'unique_words': len(word_freq),
            'density_analysis': density_analysis
        }
    
    def _calculate_readability_score(self, text: str) -> float:
        """Calculate simple readability score."""
        if not text:
            return 0.0
        
        sentences = text.split('.')
        words = text.split()
        
        if not sentences:
            return 0.0
        
        avg_sentence_length = len(words) / len(sentences)
        
        # Simple readability scoring
        if avg_sentence_length <= 15:
            return 100.0
        elif avg_sentence_length <= 20:
            return 80.0
        elif avg_sentence_length <= 25:
            return 60.0
        else:
            return max(20.0, 100.0 - (avg_sentence_length - 25) * 2)
    
    def _analyze_semantic_structure(self, text: str) -> Dict[str, Any]:
        """Analyze semantic structure of content."""
        if not text:
            return {'structured_sentences': 0, 'complex_sentences': 0, 'simple_sentences': 0}
        
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        
        structured_sentences = 0
        complex_sentences = 0
        simple_sentences = 0
        
        for sentence in sentences:
            words = sentence.split()
            
            # Complex sentence indicators
            if (len(words) > 20 or 
                any(conj in sentence.lower() for conj in ['because', 'although', 'however', 'therefore', 'moreover'])):
                complex_sentences += 1
            elif len(words) < 10:
                simple_sentences += 1
            else:
                structured_sentences += 1
        
        return {
            'total_sentences': len(sentences),
            'structured_sentences': structured_sentences,
            'complex_sentences': complex_sentences,
            'simple_sentences': simple_sentences,
            'complexity_ratio': (complex_sentences / len(sentences) * 100) if sentences else 0
        }
    
    def _analyze_content_quality(self, text: str) -> Dict[str, Any]:
        """Analyze content quality indicators."""
        if not text:
            return {'quality_score': 0, 'indicators': {}}
        
        words = text.split()
        sentences = text.split('.')
        
        # Quality indicators
        indicators = {
            'adequate_length': len(words) >= 50,
            'proper_sentences': len(sentences) > 1,
            'avg_sentence_length': 10 <= len(words) / len(sentences) <= 25 if sentences else False,
            'has_punctuation': any(punct in text for punct in ['?', '!', ',', ';']),
            'capitalization': text[0].isupper() if text else False
        }
        
        quality_score = sum(indicators.values()) / len(indicators) * 100
        
        return {
            'quality_score': quality_score,
            'indicators': indicators,
            'quality_grade': 'A' if quality_score >= 80 else 'B' if quality_score >= 60 else 'C'
        }
    
    def _calculate_content_depth(self, enhanced_sections: Dict) -> float:
        """Calculate content depth score based on sections."""
        if not enhanced_sections:
            return 0.0
        
        section_scores = []
        for section_name, section_data in enhanced_sections.items():
            if isinstance(section_data, dict):
                word_count = section_data.get('word_count', 0)
                confidence = section_data.get('confidence', 0)
                
                # Score based on word count and confidence
                section_score = min(word_count / 100, 1.0) * confidence
                section_scores.append(section_score)
        
        return sum(section_scores) / len(section_scores) if section_scores else 0.0
    
    def _analyze_topic_coherence(self, text: str) -> Dict[str, Any]:
        """Analyze topic coherence of content."""
        if not text:
            return {'coherence_score': 0, 'topic_keywords': []}
        
        # Simple topic analysis based on frequent words
        words = [w.lower().strip('.,!?;:') for w in text.split() if len(w) > 3]
        word_freq = {}
        
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get top keywords
        top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Simple coherence based on keyword distribution
        if len(top_keywords) < 3:
            coherence_score = 30.0
        else:
            # Check if top keywords appear throughout the text
            top_words = [word for word, freq in top_keywords[:5]]
            text_parts = text.lower().split('.')
            
            coherence_parts = 0
            for part in text_parts:
                if any(word in part for word in top_words):
                    coherence_parts += 1
            
            coherence_score = (coherence_parts / len(text_parts) * 100) if text_parts else 0
        
        return {
            'coherence_score': min(coherence_score, 100.0),
            'topic_keywords': [{'word': word, 'frequency': freq} for word, freq in top_keywords],
            'coherence_grade': 'High' if coherence_score >= 70 else 'Medium' if coherence_score >= 40 else 'Low'
        }
    
    def _evaluate_content_structure_quality(self, enhanced_sections: Dict) -> Dict[str, Any]:
        """Evaluate overall content structure quality."""
        section_types = list(enhanced_sections.keys())
        
        # Expected sections for good structure
        expected_sections = ['definition_section', 'use_case_section', 'step_section']
        present_expected = [s for s in expected_sections if s in section_types]
        
        structure_score = len(present_expected) / len(expected_sections) * 100
        
        # Additional quality factors
        has_multiple_sections = len(enhanced_sections) >= 2
        sections_with_content = len([
            s for s in enhanced_sections.values() 
            if isinstance(s, dict) and s.get('word_count', 0) > 50
        ])
        
        content_quality_factor = sections_with_content / len(enhanced_sections) if enhanced_sections else 0
        
        final_score = (structure_score * 0.6) + (content_quality_factor * 40)
        
        return {
            'structure_score': round(structure_score, 1),
            'content_quality_factor': round(content_quality_factor * 100, 1),
            'final_quality_score': round(final_score, 1),
            'has_expected_sections': len(present_expected) >= 2,
            'sections_with_substantial_content': sections_with_content,
            'quality_grade': 'A' if final_score >= 80 else 'B' if final_score >= 60 else 'C'
        }
    
    def _calculate_semantic_html_usage(self, body) -> float:
        """Calculate semantic HTML usage percentage."""
        all_elements = body.find_all()
        semantic_elements = body.find_all([
            'header', 'nav', 'main', 'section', 'article', 'aside', 'footer',
            'figure', 'figcaption', 'details', 'summary', 'time', 'mark'
        ])
        
        if not all_elements:
            return 0.0
        
        return (len(semantic_elements) / len(all_elements)) * 100
    
    def _calculate_technical_seo_score(self, head_signals: Dict, body_signals: Dict) -> float:
        """Calculate overall technical SEO score."""
        score = 0.0
        max_score = 100.0
        
        # Head signals (60% of score)
        if head_signals:
            meta_score = 0
            meta_tags = head_signals.get('meta_tags', {})
            
            if meta_tags.get('description_present'):
                meta_score += 15
            if meta_tags.get('viewport_present'):
                meta_score += 10
            if meta_tags.get('robots_present'):
                meta_score += 10
            if meta_tags.get('canonical_present'):
                meta_score += 15
            if meta_tags.get('charset_present'):
                meta_score += 10
            
            score += meta_score
        
        # Body signals (40% of score)
        if body_signals:
            body_score = 0
            structure = body_signals.get('structure', {})
            performance = body_signals.get('performance', {})
            
            if structure.get('has_main'):
                body_score += 15
            if structure.get('has_header'):
                body_score += 10
            if structure.get('semantic_html_usage', 0) > 20:
                body_score += 10
            
            # Performance deductions
            if performance.get('images_without_dimensions', 0) == 0:
                body_score += 5
            
            score += body_score
        
        return min(score, max_score)
    
    def _generate_extraction_summary(self, entity_validation: Dict, page_type_properties: Dict,
                                   enhanced_metadata: Dict, link_analysis: Dict, 
                                   multimedia_analysis: Dict) -> Dict[str, Any]:
        """Generate comprehensive extraction summary."""
        return {
            'extraction_completeness': {
                'entity_validation_complete': bool(entity_validation.get('entity_validation')),
                'page_type_properties_complete': bool(page_type_properties.get('page_type_properties')),
                'enhanced_metadata_complete': bool(enhanced_metadata.get('page_metadata')),
                'link_analysis_complete': bool(link_analysis.get('link_analysis')),
                'multimedia_analysis_complete': bool(multimedia_analysis.get('enhanced_multimedia'))
            },
            'data_quality_indicators': {
                'total_entities_found': entity_validation.get('entity_validation', {}).get('total_entities', 0),
                'page_type_confidence': page_type_properties.get('page_type_properties', {}).get('confidence', 0),
                'metadata_completeness': self._calculate_metadata_completeness(enhanced_metadata),
                'link_analysis_depth': self._calculate_link_analysis_depth(link_analysis),
                'multimedia_coverage': self._calculate_multimedia_coverage(multimedia_analysis)
            },
            'extraction_performance': {
                'extraction_phases_completed': 8,
                'data_points_extracted': self._count_total_data_points(
                    entity_validation, page_type_properties, enhanced_metadata,
                    link_analysis, multimedia_analysis
                ),
                'extraction_quality_score': self._calculate_overall_quality_score(
                    entity_validation, page_type_properties, enhanced_metadata,
                    link_analysis, multimedia_analysis
                )
            }
        }
    
    def _calculate_metadata_completeness(self, enhanced_metadata: Dict) -> float:
        """Calculate metadata completeness score."""
        metadata = enhanced_metadata.get('page_metadata', {})
        
        if not metadata:
            return 0.0
        
        completeness_factors = [
            metadata.get('title_present', False),
            metadata.get('meta_description_present', False),
            metadata.get('canonical_present', False),
            len(metadata.get('opengraph', {}).get('tags', {})) > 0,
            len(metadata.get('twitter_card', {}).get('tags', {})) > 0,
            metadata.get('heading_structure', {}).get('has_h1', False)
        ]
        
        return (sum(completeness_factors) / len(completeness_factors)) * 100
    
    def _calculate_link_analysis_depth(self, link_analysis: Dict) -> float:
        """Calculate link analysis depth score."""
        analysis = link_analysis.get('link_analysis', {})
        
        if not analysis:
            return 0.0
        
        classification = analysis.get('classification', {})
        
        depth_factors = [
            classification.get('total_links', 0) > 0,
            classification.get('internal_links', 0) > 0,
            classification.get('external_links', 0) > 0,
            len(analysis.get('internal_analysis', {})) > 0,
            len(analysis.get('external_analysis', {})) > 0,
            len(analysis.get('quality_metrics', {})) > 0
        ]
        
        return (sum(depth_factors) / len(depth_factors)) * 100
    
    def _calculate_multimedia_coverage(self, multimedia_analysis: Dict) -> float:
        """Calculate multimedia coverage score."""
        multimedia = multimedia_analysis.get('enhanced_multimedia', {})
        
        if not multimedia:
            return 0.0
        
        coverage_factors = [
            len(multimedia.get('enhanced_images', [])) > 0,
            len(multimedia.get('html_videos', [])) > 0 or len(multimedia.get('iframe_videos', [])) > 0,
            len(multimedia.get('image_objects_from_schema', [])) > 0,
            len(multimedia.get('schema_videos', [])) > 0,
            multimedia.get('audio_content', {}).get('has_audio_content', False)
        ]
        
        return (sum(coverage_factors) / len(coverage_factors)) * 100
    
    def _count_total_data_points(self, *datasets) -> int:
        """Count total data points extracted."""
        total = 0
        for dataset in datasets:
            if isinstance(dataset, dict):
                total += len(str(dataset)) // 100  # Rough estimate
        return total
    
    def _calculate_overall_quality_score(self, *datasets) -> float:
        """Calculate overall extraction quality score."""
        scores = []
        
        # Entity validation score
        entity_val = datasets[0].get('entity_validation', {})
        if entity_val:
            entity_score = 100 - (len(entity_val.get('invalid_schema_types', [])) * 10)
            scores.append(max(0, entity_score))
        
        # Page type confidence
        page_type = datasets[1].get('page_type_properties', {})
        if page_type:
            scores.append(page_type.get('confidence', 0))
        
        # Metadata completeness
        metadata = datasets[2].get('page_metadata', {})
        if metadata:
            scores.append(self._calculate_metadata_completeness(datasets[2]))
        
        # Link analysis depth
        if datasets[3]:
            scores.append(self._calculate_link_analysis_depth(datasets[3]))
        
        # Multimedia coverage
        if datasets[4]:
            scores.append(self._calculate_multimedia_coverage(datasets[4]))
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
