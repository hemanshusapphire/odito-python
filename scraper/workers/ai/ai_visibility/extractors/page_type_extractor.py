"""Page type-specific property extraction."""

from typing import Dict, List, Any, Optional
import re
from urllib.parse import urlparse

class PageTypeExtractor:
    """Extract properties specific to different page types."""
    
    def __init__(self, soup, entities: List[Dict], url: str):
        self.soup = soup
        self.entities = entities
        self.url = url
        self.primary_entity = self._find_primary_entity()
        
    def extract_all_page_type_properties(self) -> Dict[str, Any]:
        """Extract properties based on detected page type."""
        page_type = self._detect_page_type()
        
        extractors = {
            'Service': self._extract_service_properties,
            'Product': self._extract_product_properties,
            'Article': self._extract_article_properties,
            'BlogPosting': self._extract_article_properties,
            'NewsArticle': self._extract_article_properties,
            'FAQPage': self._extract_faq_properties,
            'Dataset': self._extract_dataset_properties,
            'WebPage': self._extract_webpage_properties
        }
        
        extractor = extractors.get(page_type, lambda: {})
        properties = extractor()
        
        return {
            'page_type_properties': {
                'detected_type': page_type,
                'confidence': self._calculate_type_confidence(page_type),
                **properties
            }
        }
    
    def _find_primary_entity(self) -> Optional[Dict]:
        """Find the primary entity from the entities list."""
        if not self.entities:
            return None
            
        # Look for entity marked as primary or with highest priority
        priority_order = ['WebPage', 'Service', 'Product', 'Article', 'Organization']
        
        for priority_type in priority_order:
            for entity in self.entities:
                entity_type = entity.get('@type')
                if isinstance(entity_type, list):
                    if priority_type in entity_type:
                        return entity
                elif entity_type == priority_type:
                    return entity
        
        # Fallback to first entity
        return self.entities[0] if self.entities else None
    
    def _detect_page_type(self) -> str:
        """Detect the primary page type."""
        if not self.primary_entity:
            return 'WebPage'
            
        entity_type = self.primary_entity.get('@type')
        if isinstance(entity_type, list):
            # Return the first recognized type
            for t in entity_type:
                if t in ['Service', 'Product', 'Article', 'BlogPosting', 'NewsArticle', 'FAQPage', 'Dataset']:
                    return t
            return entity_type[0] if entity_type else 'WebPage'
        
        return entity_type if entity_type else 'WebPage'
    
    def _calculate_type_confidence(self, page_type: str) -> float:
        """Calculate confidence score for page type detection."""
        if not self.primary_entity:
            return 0.5
            
        entity_type = self.primary_entity.get('@type')
        if isinstance(entity_type, list):
            return 0.8 if page_type in entity_type else 0.5
        elif entity_type == page_type:
            return 1.0
        else:
            return 0.3
    
    def _extract_service_properties(self) -> Dict[str, Any]:
        """Extract Service-specific properties."""
        service_props = {}
        
        if not self.primary_entity:
            return service_props
            
        # Direct property extraction
        service_props['serviceType'] = self.primary_entity.get('serviceType')
        service_props['provider'] = self._extract_provider_info()
        service_props['areaServed'] = self.primary_entity.get('areaServed')
        service_props['audience'] = self.primary_entity.get('audience')
        service_props['availableChannel'] = self.primary_entity.get('availableChannel')
        service_props['hasOfferCatalog'] = self.primary_entity.get('hasOfferCatalog')
        
        # Extract offers (full Offer objects)
        service_props['offers'] = self._extract_offers()
        
        # Extract from other entities if primary doesn't have serviceType
        if not service_props['serviceType']:
            service_props['serviceType'] = self._find_service_type_from_entities()
        
        return service_props
    
    def _extract_product_properties(self) -> Dict[str, Any]:
        """Extract Product-specific properties."""
        product_props = {}
        
        if not self.primary_entity:
            return product_props
            
        # Direct property extraction
        product_props['brand'] = self.primary_entity.get('brand')
        product_props['sku'] = self.primary_entity.get('sku')
        product_props['aggregateRating'] = self.primary_entity.get('aggregateRating')
        product_props['reviewCount'] = self.primary_entity.get('reviewCount')
        
        # Extract offers with price and availability
        offers = self._extract_offers()
        if offers:
            product_props['offers'] = offers
            
        return product_props
    
    def _extract_article_properties(self) -> Dict[str, Any]:
        """Extract Article/BlogPosting-specific properties."""
        article_props = {}
        
        if not self.primary_entity:
            return article_props
            
        # Direct property extraction
        article_props['headline'] = self.primary_entity.get('headline')
        article_props['author'] = self.primary_entity.get('author')
        article_props['publisher'] = self.primary_entity.get('publisher')
        article_props['datePublished'] = self.primary_entity.get('datePublished')
        article_props['dateModified'] = self.primary_entity.get('dateModified')
        article_props['mainEntityOfPage'] = self.primary_entity.get('mainEntityOfPage')
        
        # Extract from HTML if missing from schema
        if not article_props['headline']:
            article_props['headline'] = self._extract_headline_from_html()
        
        if not article_props['author']:
            article_props['author'] = self._extract_author_from_html()
        
        return article_props
    
    def _extract_faq_properties(self) -> Dict[str, Any]:
        """Extract FAQPage-specific properties."""
        faq_props = {}
        
        # Extract FAQ data from JSON-LD
        faq_data = self._extract_faq_from_schema()
        faq_props.update(faq_data)
        
        # Extract FAQ data from HTML if schema is missing
        if not faq_props.get('questions'):
            html_faq = self._extract_faq_from_html()
            faq_props.update(html_faq)
        
        return faq_props
    
    def _extract_dataset_properties(self) -> Dict[str, Any]:
        """Extract Dataset-specific properties."""
        dataset_props = {}
        
        if not self.primary_entity:
            return dataset_props
            
        # Direct property extraction
        dataset_props['creator'] = self.primary_entity.get('creator')
        dataset_props['distribution'] = self.primary_entity.get('distribution')
        dataset_props['license'] = self.primary_entity.get('license')
        dataset_props['datePublished'] = self.primary_entity.get('datePublished')
        dataset_props['dateModified'] = self.primary_entity.get('dateModified')
        
        return dataset_props
    
    def _extract_webpage_properties(self) -> Dict[str, Any]:
        """Extract generic WebPage properties."""
        webpage_props = {}
        
        if not self.primary_entity:
            return webpage_props
            
        webpage_props['name'] = self.primary_entity.get('name')
        webpage_props['description'] = self.primary_entity.get('description')
        webpage_props['about'] = self.primary_entity.get('about')
        webpage_props['mainEntity'] = self.primary_entity.get('mainEntity')
        
        return webpage_props
    
    def _extract_offers(self) -> List[Dict[str, Any]]:
        """Extract Offer objects from entities."""
        offers = []
        
        # Check primary entity for offers
        if self.primary_entity:
            primary_offers = self.primary_entity.get('offers')
            if primary_offers:
                if isinstance(primary_offers, list):
                    offers.extend(primary_offers)
                else:
                    offers.append(primary_offers)
        
        # Check other entities for Offer type
        for entity in self.entities:
            entity_type = entity.get('@type')
            if isinstance(entity_type, list):
                if 'Offer' in entity_type:
                    offers.append(entity)
            elif entity_type == 'Offer':
                offers.append(entity)
        
        return offers
    
    def _extract_provider_info(self) -> Dict[str, Any]:
        """Extract provider information."""
        if not self.primary_entity:
            return {}
            
        provider = self.primary_entity.get('provider')
        if provider:
            return provider if isinstance(provider, dict) else {'name': str(provider)}
        
        # Look for Organization entities
        for entity in self.entities:
            entity_type = entity.get('@type')
            if isinstance(entity_type, list):
                if 'Organization' in entity_type:
                    return entity
            elif entity_type == 'Organization':
                return entity
        
        return {}
    
    def _find_service_type_from_entities(self) -> Optional[str]:
        """Find serviceType from other entities."""
        for entity in self.entities:
            service_type = entity.get('serviceType')
            if service_type:
                return service_type
        return None
    
    def _extract_headline_from_html(self) -> Optional[str]:
        """Extract headline from HTML (h1, title)."""
        # Try h1 first
        h1 = self.soup.find('h1')
        if h1:
            return h1.get_text().strip()
        
        # Try title tag
        title = self.soup.find('title')
        if title:
            return title.get_text().strip()
        
        return None
    
    def _extract_author_from_html(self) -> Optional[str]:
        """Extract author from HTML."""
        # Look for author meta tags
        author_meta = self.soup.find('meta', attrs={'name': 'author'})
        if author_meta:
            return author_meta.get('content', '').strip()
        
        # Look for byline elements
        byline_selectors = [
            '.author', '.byline', '[class*="author"]', 
            '[rel="author"]', '.post-author'
        ]
        
        for selector in byline_selectors:
            element = self.soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        return None
    
    def _extract_faq_from_schema(self) -> Dict[str, Any]:
        """Extract FAQ data from JSON-LD schema."""
        faq_data = {
            'questions': [],
            'total_questions': 0,
            'average_answer_length': 0,
            'visible_content_match': False
        }
        
        for entity in self.entities:
            entity_type = entity.get('@type')
            if isinstance(entity_type, list):
                if 'FAQPage' not in entity_type:
                    continue
            elif entity_type != 'FAQPage':
                continue
            
            main_entity = entity.get('mainEntity', [])
            if isinstance(main_entity, dict):
                main_entity = [main_entity]
            
            questions = []
            total_answer_length = 0
            
            for item in main_entity:
                if item.get('@type') in ['Question', 'question']:
                    question_text = item.get('name', '')
                    answer_text = ''
                    
                    # Extract answer
                    accepted_answer = item.get('acceptedAnswer')
                    suggested_answer = item.get('suggestedAnswer')
                    
                    if accepted_answer:
                        answer_text = accepted_answer.get('text', '')
                    elif suggested_answer:
                        if isinstance(suggested_answer, list):
                            answer_text = ' '.join([ans.get('text', '') for ans in suggested_answer])
                        else:
                            answer_text = suggested_answer.get('text', '')
                    
                    if question_text and answer_text:
                        questions.append({
                            'question': question_text,
                            'answer': answer_text,
                            'answer_word_count': len(answer_text.split())
                        })
                        total_answer_length += len(answer_text.split())
            
            faq_data['questions'] = questions
            faq_data['total_questions'] = len(questions)
            faq_data['average_answer_length'] = total_answer_length // len(questions) if questions else 0
            faq_data['visible_content_match'] = self._check_faq_visible_match(questions)
            break
        
        return faq_data
    
    def _extract_faq_from_html(self) -> Dict[str, Any]:
        """Extract FAQ data from HTML structure."""
        faq_data = {
            'questions': [],
            'total_questions': 0,
            'average_answer_length': 0,
            'visible_content_match': False
        }
        
        # Look for FAQ sections
        faq_selectors = [
            '.faq', '.faqs', '[class*="faq"]',
            '.question', '.questions', '[class*="question"]',
            '.accordion', '[class*="accordion"]'
        ]
        
        questions = []
        total_answer_length = 0
        
        for selector in faq_selectors:
            faq_elements = self.soup.select(selector)
            
            for faq_elem in faq_elements:
                # Look for question-answer patterns
                headings = faq_elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                
                for heading in headings:
                    question_text = heading.get_text().strip()
                    
                    # Look for answer after heading
                    answer_text = ''
                    next_elem = heading.find_next_sibling()
                    
                    while next_elem and next_elem.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        if next_elem.get_text().strip():
                            answer_text += next_elem.get_text().strip() + ' '
                        next_elem = next_elem.find_next_sibling()
                    
                    if question_text and answer_text:
                        questions.append({
                            'question': question_text,
                            'answer': answer_text.strip(),
                            'answer_word_count': len(answer_text.split())
                        })
                        total_answer_length += len(answer_text.split())
        
        faq_data['questions'] = questions
        faq_data['total_questions'] = len(questions)
        faq_data['average_answer_length'] = total_answer_length // len(questions) if questions else 0
        faq_data['visible_content_match'] = self._check_faq_visible_match(questions)
        
        return faq_data
    
    def _check_faq_visible_match(self, questions: List[Dict]) -> bool:
        """Check if FAQ content matches visible DOM content."""
        if not questions:
            return False
            
        page_text = self.soup.get_text().lower()
        
        for qa in questions[:5]:  # Check first 5 questions
            question = qa['question'].lower()
            answer = qa['answer'].lower()
            
            if question not in page_text or answer not in page_text:
                return False
        
        return True
