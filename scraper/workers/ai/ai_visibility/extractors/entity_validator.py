"""Entity validation and schema.org compliance extraction."""

from typing import Dict, List, Any, Set
import re
from urllib.parse import urlparse

# Schema.org valid types (conservative subset for performance)
SCHEMA_ORG_TYPES = {
    # Core types
    'Thing', 'CreativeWork', 'Intangible', 'StructuredValue',
    # Creative works
    'Article', 'BlogPosting', 'NewsArticle', 'TechArticle', 'ScholarlyArticle',
    'Book', 'Movie', 'MusicRecording', 'Recipe', 'Review', 'Comment',
    'WebPage', 'AboutPage', 'FAQPage', 'ContactPage', 'ProfilePage',
    'CollectionPage', 'SearchResultsPage',
    # Organizations
    'Organization', 'Corporation', 'EducationalOrganization', 'GovernmentOrganization',
    'LocalBusiness', 'Restaurant', 'Hotel', 'Store', 'ProfessionalService',
    # Products/Services
    'Product', 'Service', 'Vehicle', 'IndividualProduct',
    # People
    'Person', 'Audience',
    # Places
    'Place', 'LocalBusiness', 'PostalAddress', 'GeoCoordinates',
    # Events
    'Event', 'BusinessEvent', 'EducationEvent', 'SportsEvent',
    # Media
    'ImageObject', 'VideoObject', 'AudioObject', 'MediaObject',
    # Data
    'Dataset', 'DataFeed', 'PropertyValue', 'QuantitativeValue',
    # Technical
    'SoftwareApplication', 'WebSite', 'WebApplication', 'MobileApplication',
    'BreadcrumbList', 'ListItem', 'ItemList',
    # Commerce
    'Offer', 'Demand', 'AggregateOffer', 'PriceSpecification',
    'Brand', 'Organization', 'Person', 'Product',
    # Reviews/Ratings
    'AggregateRating', 'Rating', 'Review',
    # FAQ
    'Question', 'Answer',
    # How-to
    'HowTo', 'HowToStep', 'HowToSection',
    # Local business
    'LocalBusiness', 'FoodEstablishment', 'Restaurant',
    # Medical/Health
    'MedicalEntity', 'MedicalCondition', 'MedicalProcedure',
    # Other common
    'JobPosting', 'RealEstateListing', 'Vehicle', 'TouristAttraction'
}

# Required properties per type (minimal critical set)
REQUIRED_PROPERTIES = {
    'Product': ['name'],
    'Service': ['name', 'description'],
    'Article': ['headline'],
    'BlogPosting': ['headline'],
    'NewsArticle': ['headline'],
    'WebPage': ['name'],
    'Organization': ['name'],
    'Person': ['name'],
    'Place': ['name'],
    'Event': ['name', 'startDate'],
    'Offer': ['price'],
    'Product': ['name'],
    'Restaurant': ['name'],
    'Hotel': ['name'],
    'JobPosting': ['title', 'description'],
    'FAQPage': ['mainEntity'],
    'HowTo': ['name', 'step'],
    'Dataset': ['name'],
    'ImageObject': ['contentUrl'],
    'VideoObject': ['contentUrl'],
    'BreadcrumbList': ['itemListElement'],
    'ListItem': ['position', 'name']
}

class EntityValidator:
    """Comprehensive entity validation and analysis."""
    
    def __init__(self, canonical_root: str):
        self.canonical_root = canonical_root
        self.invalid_types_detected = set()
        self.missing_required_properties = {}
        self.duplicate_ids = set()
        self.orphan_entities = set()
        self.isolated_entities = set()
        
    def validate_entities(self, entities: List[Dict], relationships: List[Dict]) -> Dict[str, Any]:
        """Perform complete entity validation."""
        validation_results = {
            'entity_validation': {
                'total_entities': len(entities),
                'valid_schema_types': 0,
                'invalid_schema_types': [],
                'duplicate_entity_ids': [],
                'orphan_entities': [],
                'isolated_entities': [],
                'missing_required_properties': {},
                'entity_type_distribution': {},
                'id_format_compliance': {
                    'total_with_ids': 0,
                    'https_ids': 0,
                    'http_ids': 0,
                    'relative_ids': 0,
                    'hash_ids': 0,
                    'invalid_ids': 0
                }
            }
        }
        
        if not entities:
            return validation_results
            
        # Step 1: Validate each entity
        entity_ids = set()
        entity_references = set()
        
        for entity in entities:
            self._validate_single_entity(entity, entity_ids, entity_references, validation_results)
        
        # Step 2: Detect duplicate IDs
        if len(entity_ids) != len(entities):
            id_counts = {}
            for entity in entities:
                entity_id = entity.get('@id')
                if entity_id:
                    id_counts[entity_id] = id_counts.get(entity_id, 0) + 1
            
            validation_results['entity_validation']['duplicate_entity_ids'] = [
                entity_id for entity_id, count in id_counts.items() if count > 1
            ]
        
        # Step 3: Analyze relationships for orphan/isolated detection
        self._analyze_entity_connectivity(entities, relationships, entity_references, validation_results)
        
        # Step 4: Compile missing required properties
        validation_results['entity_validation']['missing_required_properties'] = self.missing_required_properties
        
        return validation_results
    
    def _validate_single_entity(self, entity: Dict, entity_ids: Set[str], 
                               entity_references: Set[str], results: Dict):
        """Validate a single entity."""
        entity_id = entity.get('@id')
        entity_type = entity.get('@type')
        
        # Track entity IDs
        if entity_id:
            entity_ids.add(entity_id)
            
            # Validate ID format
            self._validate_id_format(entity_id, results)
        
        # Validate entity type
        type_validation = self._validate_entity_type(entity_type)
        if type_validation['is_valid']:
            results['entity_validation']['valid_schema_types'] += 1
        else:
            if type_validation['type'] not in results['entity_validation']['invalid_schema_types']:
                results['entity_validation']['invalid_schema_types'].append(type_validation['type'])
        
        # Update type distribution
        if entity_type:
            if isinstance(entity_type, list):
                for t in entity_type:
                    results['entity_validation']['entity_type_distribution'][t] = \
                        results['entity_validation']['entity_type_distribution'].get(t, 0) + 1
            else:
                results['entity_validation']['entity_type_distribution'][entity_type] = \
                    results['entity_validation']['entity_type_distribution'].get(entity_type, 0) + 1
        
        # Check required properties
        missing_props = self._check_required_properties(entity, entity_type)
        if missing_props:
            if entity_type not in self.missing_required_properties:
                self.missing_required_properties[entity_type] = []
            self.missing_required_properties[entity_type].extend(missing_props)
        
        # Extract entity references
        self._extract_entity_references(entity, entity_references)
    
    def _validate_id_format(self, entity_id: str, results: Dict):
        """Validate entity ID format."""
        id_compliance = results['entity_validation']['id_format_compliance']
        id_compliance['total_with_ids'] += 1
        
        if entity_id.startswith('https://'):
            id_compliance['https_ids'] += 1
        elif entity_id.startswith('http://'):
            id_compliance['http_ids'] += 1
        elif entity_id.startswith('#'):
            id_compliance['hash_ids'] += 1
        elif entity_id.startswith('/') or not entity_id.startswith(('http://', 'https://')):
            id_compliance['relative_ids'] += 1
        else:
            id_compliance['invalid_ids'] += 1
    
    def _validate_entity_type(self, entity_type) -> Dict[str, Any]:
        """Validate entity type against schema.org."""
        if not entity_type:
            return {'is_valid': False, 'type': None, 'reason': 'missing_type'}
        
        # Handle multiple types
        types_to_check = [entity_type] if isinstance(entity_type, str) else entity_type
        
        for single_type in types_to_check:
            if single_type in SCHEMA_ORG_TYPES:
                return {'is_valid': True, 'type': single_type, 'reason': 'valid_schema_type'}
        
        return {'is_valid': False, 'type': entity_type, 'reason': 'invalid_schema_type'}
    
    def _check_required_properties(self, entity: Dict, entity_type) -> List[str]:
        """Check for missing required properties."""
        if not entity_type:
            return []
        
        # Handle multiple types - check if any have required properties
        types_to_check = [entity_type] if isinstance(entity_type, str) else entity_type
        
        missing_props = []
        for single_type in types_to_check:
            if single_type in REQUIRED_PROPERTIES:
                required = REQUIRED_PROPERTIES[single_type]
                for prop in required:
                    if prop not in entity:
                        missing_props.append(prop)
        
        return missing_props
    
    def _extract_entity_references(self, entity: Dict, entity_references: Set[str]):
        """Extract all entity references from an entity."""
        def extract_refs(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == '@id' and isinstance(value, str):
                        entity_references.add(value)
                    elif isinstance(value, str) and value.startswith(('http://', 'https://', '#', '/')):
                        # Potential entity reference
                        entity_references.add(value)
                    else:
                        extract_refs(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_refs(item)
        
        # Skip @id and @type keys, extract from all other values
        entity_copy = dict(entity)
        entity_copy.pop('@id', None)
        entity_copy.pop('@type', None)
        entity_copy.pop('@context', None)
        
        extract_refs(entity_copy)
    
    def _analyze_entity_connectivity(self, entities: List[Dict], relationships: List[Dict], 
                                   entity_references: Set[str], results: Dict):
        """Analyze entity connectivity for orphan/isolated detection."""
        entity_ids = {entity.get('@id') for entity in entities if entity.get('@id')}
        
        # Find orphan entities (entities with no references to them)
        referenced_entities = set()
        for ref in entity_references:
            if ref in entity_ids:
                referenced_entities.add(ref)
        
        orphan_entities = entity_ids - referenced_entities
        results['entity_validation']['orphan_entities'] = list(orphan_entities)
        
        # Find isolated entities (entities with no outgoing references)
        entity_with_outgoing_refs = set()
        for entity in entities:
            entity_id = entity.get('@id')
            if entity_id:
                refs = set()
                self._extract_entity_references(entity, refs)
                # Remove self-reference
                refs.discard(entity_id)
                if refs:
                    entity_with_outgoing_refs.add(entity_id)
        
        isolated_entities = entity_ids - entity_with_outgoing_refs
        results['entity_validation']['isolated_entities'] = list(isolated_entities)
