"""
Citation Probability Score Category Rules

Evaluates likelihood of content being cited by AI systems and other sources.
Focuses on authority, trustworthiness, and citation-worthy content elements.
"""

import json
from typing import Dict, Any
from rule_base import BaseRule

class BusinessNameIdenticalRule(BaseRule):
    """Rule 8 — Business name identical everywhere"""
    
    def __init__(self):
        config = {
            "rule_id": "business_name_identical",
            "category": "citation_probability",
            "description": "Business name identical everywhere",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for consistent business name"""
        # Check the actual extracted NAP signals
        nap_signals = data.get("nap_signals", {})
        
        # Return score based on actual business name detection
        business_name = nap_signals.get("business_name", "")
        if business_name and len(business_name.strip()) > 0:
            # Check if business name is identical across locations
            if nap_signals.get("business_name_identical", False):
                return 10.0  # Full score for identical business name
            else:
                return 6.0   # Partial score for detected but not identical
        else:
            return 0.0  # No score if no business name detected

class AddressIdenticalRule(BaseRule):
    """Rule 9 — Address identical everywhere"""
    
    def __init__(self):
        config = {
            "rule_id": "address_identical",
            "category": "citation_probability",
            "description": "Address identical everywhere",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for consistent address"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for address in Organization
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("address"):
                    score += 10  # Address present
                break
        
        return min(score, self.max_score)

class NAPMatchesFooterContactRule(BaseRule):
    """Rule 10 — NAP matches footer/contact page"""
    
    def __init__(self):
        config = {
            "rule_id": "nap_matches_footer_contact",
            "category": "citation_probability",
            "description": "NAP matches footer/contact page",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for NAP consistency"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for NAP (Name, Address, Phone) in Organization
        for item in graph:
            if item.get("@type") == "Organization":
                nap_fields = 0
                if item.get("name"):
                    nap_fields += 1
                if item.get("address"):
                    nap_fields += 1
                if item.get("telephone"):
                    nap_fields += 1
                
                if nap_fields == 3:
                    score += 10
                elif nap_fields >= 2:
                    score += 6
                elif nap_fields >= 1:
                    score += 3
                break
        
        return min(score, self.max_score)

class NoEntityFragmentationRule(BaseRule):
    """Rule 11 — No entity fragmentation"""
    
    def __init__(self):
        config = {
            "rule_id": "no_entity_fragmentation",
            "category": "citation_probability",
            "description": "No entity fragmentation",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for entity fragmentation"""
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for primary entity (indicates no fragmentation)
        if entity_graph.get("primary_entity"):
            score += 6
        
        # Check for reasonable entity count (not too fragmented)
        entities = entity_graph.get("entities", [])
        if 1 <= len(entities) <= 10:
            score += 4  # Good entity count
        elif len(entities) <= 15:
            score += 2  # Some fragmentation
        
        return min(score, self.max_score)

class AboutContactPrivacyTermsPagesRule(BaseRule):
    """Rule 12 — About / Contact / Privacy / Terms pages exist"""
    
    def __init__(self):
        config = {
            "rule_id": "about_contact_privacy_terms_pages",
            "category": "citation_probability",
            "description": "About / Contact / Privacy / Terms pages exist",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for essential pages"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for WebPage types that might be essential pages
        essential_pages = 0
        for item in graph:
            if item.get("@type") == "WebPage":
                url = item.get("url", "").lower()
                if any(page in url for page in ["about", "contact", "privacy", "terms"]):
                    essential_pages += 1
        
        if essential_pages >= 4:
            score += 10
        elif essential_pages >= 3:
            score += 8
        elif essential_pages >= 2:
            score += 5
        elif essential_pages >= 1:
            score += 3
        
        return min(score, self.max_score)

class PhoneE164FormatRule(BaseRule):
    """Rule 19 — Phone in E.164 format"""
    
    def __init__(self):
        config = {
            "rule_id": "phone_e164_format",
            "category": "citation_probability",
            "description": "Phone in E.164 format",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for E.164 phone format"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for phone in E.164 format (+country_code number)
        import re
        e164_pattern = r'^\+\d{1,3}\d{6,14}$'
        
        for item in graph:
            phone = item.get("telephone") or item.get("phone")
            if phone and re.match(e164_pattern, phone.replace(" ", "")):
                score += 10
                break
            elif phone:  # Phone exists but not E.164
                score += 5
                break
        
        return min(score, self.max_score)

class VisibleAuthorNameRule(BaseRule):
    """Rule 20 — Visible author name"""
    
    def __init__(self):
        config = {
            "rule_id": "visible_author_name",
            "category": "citation_probability",
            "description": "Visible author name",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for visible author name"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person/Author with name
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                if item.get("name"):
                    score += 10
                    break
        
        return min(score, self.max_score)

class PersonSchemaLinkedToOrganizationRule(BaseRule):
    """Rule 21 — Person schema linked to Organization"""
    
    def __init__(self):
        config = {
            "rule_id": "person_schema_linked_to_organization",
            "category": "citation_probability",
            "description": "Person schema linked to Organization",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check if Person schema is linked to Organization"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person linked to Organization
        person_found = False
        organization_found = False
        
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                person_found = True
                # Check if linked to organization
                if item.get("worksFor") or item.get("affiliation"):
                    score += 5
            elif item.get("@type") == "Organization":
                organization_found = True
        
        if person_found and organization_found:
            score += 5  # Both present
        
        return min(score, self.max_score)

class AuthorBioWithCredentialsRule(BaseRule):
    """Rule 36 — Author bio with credentials"""
    
    def __init__(self):
        config = {
            "rule_id": "author_bio_with_credentials",
            "category": "citation_probability",
            "description": "Author bio with credentials",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for author bio with credentials"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person with credentials
        for item in graph:
            if item.get("@type") == "Person":
                credentials = 0
                if item.get("jobTitle"):
                    credentials += 1
                if item.get("knowsAbout"):
                    credentials += 1
                if item.get("award"):
                    credentials += 1
                if item.get("alumniOf"):
                    credentials += 1
                if item.get("worksFor"):
                    credentials += 1
                
                if credentials >= 3:
                    score += 10
                elif credentials >= 2:
                    score += 6
                elif credentials >= 1:
                    score += 3
                break
        
        return min(score, self.max_score)

class AuthorPhotoRule(BaseRule):
    """Rule 37 — Author photo"""
    
    def __init__(self):
        config = {
            "rule_id": "author_photo",
            "category": "citation_probability",
            "description": "Author photo",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for author photo"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person with image
        for item in graph:
            if item.get("@type") == "Person":
                if item.get("image"):
                    score += 10
                    break
        
        return min(score, self.max_score)

class DedicatedAuthorPageRule(BaseRule):
    """Rule 38 — Dedicated author page"""
    
    def __init__(self):
        config = {
            "rule_id": "dedicated_author_page",
            "category": "citation_probability",
            "description": "Dedicated author page",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for dedicated author page"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Person with URL (indicates dedicated page)
        for item in graph:
            if item.get("@type") == "Person":
                if item.get("url"):
                    score += 10
                    break
        
        return min(score, self.max_score)

class BusinessRegistrationDetailsRule(BaseRule):
    """Rule 46 — Business registration details"""
    
    def __init__(self):
        config = {
            "rule_id": "business_registration_details",
            "category": "citation_probability",
            "description": "Business registration details",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for business registration details"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Organization with registration details
        for item in graph:
            if item.get("@type") == "Organization":
                reg_details = 0
                if item.get("foundingDate"):
                    reg_details += 1
                if item.get("taxID"):
                    reg_details += 1
                if item.get("leiCode"):
                    reg_details += 1
                if item.get("duns"):
                    reg_details += 1
                
                if reg_details >= 2:
                    score += 10
                elif reg_details >= 1:
                    score += 5
                break
        
        return min(score, self.max_score)

class GoogleMapsEmbedCorrectRule(BaseRule):
    """Rule 62 — Google Maps embed correct"""
    
    def __init__(self):
        config = {
            "rule_id": "google_maps_embed_correct",
            "category": "citation_probability",
            "description": "Google Maps embed correct",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Check for Google Maps embed"""
        # Check the actual extracted signal for Google Maps embed
        multimedia_data = data.get("multimedia_data", {})
        
        # Return score based on actual Google Maps embed presence
        if multimedia_data.get("google_maps_embed_present", False):
            return 10.0  # Full score if Google Maps embed is present
        else:
            return 0.0  # No score if Google Maps embed is not present

# Register all Citation Probability rules (13 rules)
def register_citation_probability_rules(registry):
    """Register all Citation Probability category rules"""
    registry.register(BusinessNameIdenticalRule())
    registry.register(AddressIdenticalRule())
    registry.register(NAPMatchesFooterContactRule())
    registry.register(NoEntityFragmentationRule())
    registry.register(AboutContactPrivacyTermsPagesRule())
    registry.register(PhoneE164FormatRule())
    registry.register(VisibleAuthorNameRule())
    registry.register(PersonSchemaLinkedToOrganizationRule())
    registry.register(AuthorBioWithCredentialsRule())
    registry.register(AuthorPhotoRule())
    registry.register(DedicatedAuthorPageRule())
    registry.register(BusinessRegistrationDetailsRule())
    registry.register(GoogleMapsEmbedCorrectRule())
