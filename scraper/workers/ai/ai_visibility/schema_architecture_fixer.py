"""Schema Architecture Fixer - Enforces structured data architecture rules."""

from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
import json

class SchemaArchitectureFixer:
    """Fixes schema.org architecture violations according to global rules."""
    
    def __init__(self, canonical_root: str):
        self.canonical_root = canonical_root
        self.canonical_domain = urlparse(canonical_root).netloc
        
    def fix_schema_architecture(self, entities: List[Dict], url: str) -> Dict[str, Any]:
        """Apply all architecture fixes and return single valid JSON-LD block."""
        
        # Step 1: Consolidate Organization entities
        fixed_entities = self._consolidate_organization_entities(entities)
        
        # Step 2: Remove LocalBusiness from subpages
        fixed_entities = self._remove_subpage_localbusiness(fixed_entities, url)
        
        # Step 3: Add Organization reference to all pages
        fixed_entities = self._ensure_organization_reference(fixed_entities)
        
        # Step 4: Standardize entity IDs
        fixed_entities = self._standardize_entity_ids(fixed_entities)
        
        # Step 5: Fix addressCountry values
        fixed_entities = self._fix_address_country_values(fixed_entities)
        
        # Step 6: Add missing required properties
        fixed_entities = self._add_missing_required_properties(fixed_entities, url)
        
        # Step 7: Remove duplicate entities
        fixed_entities = self._remove_duplicate_entities(fixed_entities)
        
        # Return exactly ONE valid JSON-LD structure
        return {
            "@context": "https://schema.org",
            "@graph": fixed_entities
        }
    
    def _consolidate_organization_entities(self, entities: List[Dict]) -> List[Dict]:
        """Consolidate Organization and LocalBusiness into single Organization."""
        org_entities = []
        localbusiness_entities = []
        other_entities = []
        
        for entity in entities:
            entity_type = entity.get('@type')
            if entity_type == 'Organization':
                org_entities.append(entity)
            elif entity_type == 'LocalBusiness':
                localbusiness_entities.append(entity)
            else:
                other_entities.append(entity)
        
        if not org_entities and not localbusiness_entities:
            return other_entities
        
        # Create consolidated Organization
        consolidated_org = {
            "@type": "Organization",
            "@id": f"{self.canonical_root}#organization",
            "name": None,
            "url": self.canonical_root.rstrip('/')
        }
        
        # Merge data from Organization entities
        for org in org_entities:
            self._merge_entity_data(consolidated_org, org)
        
        # Merge data from LocalBusiness entities (only on homepage)
        for localbiz in localbusiness_entities:
            localbiz_name = localbiz.get('name', '').strip()
            # Skip LocalBusiness with empty names
            if not localbiz_name:
                continue
            
            # Check for name conflict with existing Organization
            org_name = consolidated_org.get('name')
            if org_name is not None:
                org_name = org_name.strip()
            if org_name and localbiz_name and localbiz_name != org_name:
                print(f"[WARNING] LocalBusiness name '{localbiz_name}' conflicts with Organization name '{org_name}' - keeping Organization name")
                # Still merge non-name properties but don't override Organization name
                self._merge_entity_data(consolidated_org, localbiz, exclude_keys=['name'])
            else:
                # No conflict or no org name, safe to merge
                self._merge_entity_data(consolidated_org, localbiz)
            
            # Convert LocalBusiness-specific properties
            if localbiz.get('telephone'):
                consolidated_org['telephone'] = localbiz['telephone']
            if localbiz.get('address'):
                consolidated_org['address'] = localbiz['address']
            if localbiz.get('openingHours'):
                consolidated_org['openingHours'] = localbiz['openingHours']
        
        return [consolidated_org] + other_entities
    
    def _remove_subpage_localbusiness(self, entities: List[Dict], url: str) -> List[Dict]:
        """Remove LocalBusiness entities from subpages (only allowed on homepage)."""
        if self._is_homepage(url):
            return entities
        
        # Remove LocalBusiness from subpages
        return [e for e in entities if e.get('@type') != 'LocalBusiness']
    
    def _ensure_organization_reference(self, entities: List[Dict]) -> List[Dict]:
        """Ensure all pages reference the Organization."""
        # Check if Organization already exists
        has_organization = any(e.get('@type') == 'Organization' for e in entities)
        
        if has_organization:
            return entities
        
        # Add Organization reference to primary entity
        organization_ref = {
            "@id": f"{self.canonical_root}#organization"
        }
        
        for entity in entities:
            entity_type = entity.get('@type')
            if entity_type in ['Service', 'Product', 'Article', 'WebPage', 'AboutPage']:
                if 'provider' not in entity:
                    entity['provider'] = organization_ref
                break
        
        return entities
    
    def _standardize_entity_ids(self, entities: List[Dict]) -> List[Dict]:
        """Standardize all entity IDs to canonical root format."""
        for entity in entities:
            entity_id = entity.get('@id')
            if not entity_id:
                continue
            
            # Fix www/non-www consistency
            if '://' in entity_id:
                parsed = urlparse(entity_id)
                if parsed.netloc != self.canonical_domain:
                    # Standardize to canonical domain
                    entity_id = entity_id.replace(parsed.netloc, self.canonical_domain)
                    entity['@id'] = entity_id
            
            # Ensure Organization has permanent ID
            if entity.get('@type') == 'Organization':
                entity['@id'] = f"{self.canonical_root}#organization"
        
        return entities
    
    def _fix_address_country_values(self, entities: List[Dict]) -> List[Dict]:
        """Fix addressCountry to ISO format with enhanced validation."""
        country_mapping = {
            'United States': 'US',
            'USA': 'US',
            'U.S.A.': 'US',
            'United Kingdom': 'GB',
            'UK': 'GB',
            'U.K.': 'GB',
            'Canada': 'CA',
            'Australia': 'AU'
        }
        
        for entity in entities:
            address = entity.get('address')
            if isinstance(address, dict):
                country = address.get('addressCountry')
                if country:
                    # Normalize: remove extra spaces, take first meaningful token
                    clean_country = country.strip()
                    
                    # Handle cases like "NY 10007" - extract potential country
                    tokens = clean_country.split()
                    if len(tokens) > 1:
                        # Take first token that could be a country
                        for token in tokens:
                            if token.upper() in country_mapping or (len(token) == 2 and token.isalpha()):
                                clean_country = token
                                break
                        else:
                            # No valid country found, use first token
                            clean_country = tokens[0]
                    
                    # Apply mapping or validation
                    if clean_country in country_mapping:
                        address['addressCountry'] = country_mapping[clean_country]
                    elif len(clean_country) == 2 and clean_country.isalpha():
                        address['addressCountry'] = clean_country.upper()
                    else:
                        # Invalid country - remove to maintain compliance
                        del address['addressCountry']
        
        return entities
    
    def _add_missing_required_properties(self, entities: List[Dict], url: str) -> List[Dict]:
        """Add missing required properties for each entity type."""
        for entity in entities:
            entity_type = entity.get('@type')
            
            if entity_type == 'AboutPage':
                if 'description' not in entity:
                    # Try to extract from page content
                    entity['description'] = f"About page for {self.canonical_domain}"
                if 'mainEntity' not in entity:
                    entity['mainEntity'] = {"@id": f"{self.canonical_root}#organization"}
            
            elif entity_type in ['Article', 'BlogPosting']:
                if 'author' not in entity:
                    entity['author'] = {"@type": "Organization", "@id": f"{self.canonical_root}#organization"}
                if 'publisher' not in entity:
                    entity['publisher'] = {"@type": "Organization", "@id": f"{self.canonical_root}#organization"}
            
            elif entity_type == 'Service':
                if 'serviceType' not in entity:
                    entity['serviceType'] = "Professional Services"
                if 'provider' not in entity:
                    entity['provider'] = {"@id": f"{self.canonical_root}#organization"}
        
        return entities
    
    def _remove_duplicate_entities(self, entities: List[Dict]) -> List[Dict]:
        """Remove duplicate entities based on property signature, not hash."""
        def create_signature(entity):
            """Create signature based on properties, not order"""
            if entity.get('@id'):
                return f"id:{entity['@id']}"
            
            # Property-based signature for entities without @id
            entity_type = entity.get('@type', '')
            sig_parts = [entity_type]
            
            # Add key properties for specific types
            if entity_type == 'PostalAddress':
                sig_parts.extend([
                    entity.get('streetAddress', ''),
                    entity.get('addressLocality', ''),
                    entity.get('addressRegion', ''),
                    entity.get('postalCode', ''),
                    entity.get('addressCountry', '')
                ])
            elif entity_type == 'Organization':
                sig_parts.extend([
                    entity.get('name', ''),
                    entity.get('url', '')
                ])
            elif entity_type == 'LocalBusiness':
                sig_parts.extend([
                    entity.get('name', ''),
                    entity.get('address', {}).get('streetAddress', '') if entity.get('address') else '',
                    entity.get('telephone', '')
                ])
            elif entity_type == 'ContactPoint':
                sig_parts.extend([
                    entity.get('telephone', ''),
                    entity.get('email', ''),
                    entity.get('contactType', '')
                ])
            else:
                # Generic signature - use all string properties
                for key, value in sorted(entity.items()):
                    if key not in ['@type', '@context'] and isinstance(value, str):
                        sig_parts.append(value)
            
            return ":".join(str(part).lower().strip() for part in sig_parts if str(part).strip())
        
        seen_signatures = {}
        unique_entities = []
        
        for entity in entities:
            signature = create_signature(entity)
            
            if signature not in seen_signatures:
                seen_signatures[signature] = len(unique_entities)
                unique_entities.append(entity)
            else:
                # Duplicate found - merge properties
                existing_idx = seen_signatures[signature]
                self._merge_entity_data(unique_entities[existing_idx], entity)
        
        return unique_entities
    
    def _merge_entity_data(self, target: Dict, source: Dict, exclude_keys=None):
        """Merge data from source entity into target entity."""
        if exclude_keys is None:
            exclude_keys = []
        
        for key, value in source.items():
            if key in ['@type', '@id'] or key in exclude_keys:
                continue
            
            if key not in target or target[key] is None:
                target[key] = value
            elif isinstance(target[key], list) and isinstance(value, list):
                target[key].extend(value)
            elif isinstance(target[key], dict) and isinstance(value, dict):
                target[key].update(value)
            # Keep existing value for other types (prefer first occurrence)
    
    def _is_homepage(self, url: str) -> bool:
        """Check if URL is homepage."""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        return path == '' or path == '/'
    
    def get_architecture_violations(self, entities: List[Dict], url: str) -> Dict[str, Any]:
        """Identify specific architecture violations."""
        violations = {
            "multiple_organization_entities": 0,
            "localbusiness_on_subpage": False,
            "missing_organization_reference": False,
            "non_canonical_ids": [],
            "invalid_address_country": [],
            "missing_required_properties": []
        }
        
        # Count Organization entities
        org_count = sum(1 for e in entities if e.get('@type') == 'Organization')
        
        # Count LocalBusiness entities only if they have non-empty names different from Organization
        org_name = None
        for e in entities:
            if e.get('@type') == 'Organization' and e.get('name'):
                org_name = e.get('name', '').strip()
                break
        
        localbiz_count = 0
        for e in entities:
            if e.get('@type') == 'LocalBusiness':
                localbiz_name = e.get('name')
                if localbiz_name is not None:
                    localbiz_name = localbiz_name.strip()
                # Only count as separate entity if name is non-empty and different from Organization name
                if localbiz_name and (not org_name or localbiz_name != org_name):
                    localbiz_count += 1
        
        violations["multiple_organization_entities"] = org_count + localbiz_count
        
        # Check LocalBusiness on subpage
        if not self._is_homepage(url) and localbiz_count > 0:
            violations["localbusiness_on_subpage"] = True
        
        # Check Organization reference
        has_organization = org_count > 0 or localbiz_count > 0
        has_org_reference = any(
            e.get('provider', {}).get('@id') == f"{self.canonical_root}#organization" 
            for e in entities
        )
        if not has_organization and not has_org_reference:
            violations["missing_organization_reference"] = True
        
        # Check non-canonical IDs
        for entity in entities:
            entity_id = entity.get('@id')
            if entity_id and '://' in entity_id:
                parsed = urlparse(entity_id)
                if parsed.netloc != self.canonical_domain:
                    violations["non_canonical_ids"].append(entity_id)
        
        # Check addressCountry
        for entity in entities:
            address = entity.get('address')
            if isinstance(address, dict):
                country = address.get('addressCountry')
                if country and len(country) != 2:
                    violations["invalid_address_country"].append(country)
        
        # Check missing required properties
        for entity in entities:
            entity_type = entity.get('@type')
            if entity_type == 'AboutPage':
                if 'description' not in entity:
                    violations["missing_required_properties"].append(f"AboutPage missing description")
                if 'mainEntity' not in entity:
                    violations["missing_required_properties"].append(f"AboutPage missing mainEntity")
            elif entity_type in ['Article', 'BlogPosting']:
                if 'author' not in entity:
                    violations["missing_required_properties"].append(f"{entity_type} missing author")
                if 'publisher' not in entity:
                    violations["missing_required_properties"].append(f"{entity_type} missing publisher")
            elif entity_type == 'Service':
                if 'serviceType' not in entity:
                    violations["missing_required_properties"].append("Service missing serviceType")
                if 'provider' not in entity:
                    violations["missing_required_properties"].append("Service missing provider")
        
        return violations
