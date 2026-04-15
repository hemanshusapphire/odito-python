"""
Citation Probability Score Category Rules

Evaluates likelihood of content being cited by AI systems and other sources.
Focuses on authority, trustworthiness, and citation-worthy content elements.
"""

import json
from typing import Dict, Any
from scraper.workers.ai.ai_scoring_v2.rule_base import BaseRule

class AuthoritySignalsRule(BaseRule):
    """Evaluates authority signals in content"""
    
    def __init__(self):
        config = {
            "rule_id": "authority_signals",
            "category": "citation_probability",
            "description": "Evaluates authority signals in content",
            "weight": 2.0,
            "max_score": 20,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate authority signals"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for organization authority
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Organization":
                # Check for authority properties
                if item.get("sameAs"):  # Social profiles, etc.
                    score += 5
                if item.get("url") and item.get("name"):
                    score += 3
                if item.get("foundingDate") or item.get("address"):
                    score += 2
        
        # Check for author authority
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                if item.get("sameAs"):  # Author profiles
                    score += 4
                if item.get("jobTitle") or item.get("worksFor"):
                    score += 3
        
        # Check for entity authority
        entities = entity_graph.get("entities", [])
        authoritative_entities = 0
        for entity in entities:
            if entity.get("@type") in ["Organization", "Person", "GovernmentOrganization"]:
                authoritative_entities += 1
        
        if authoritative_entities >= 3:
            score += 3
        elif authoritative_entities >= 1:
            score += 1
        
        return min(score, self.max_score)

class TrustworthinessIndicatorsRule(BaseRule):
    """Evaluates trustworthiness indicators"""
    
    def __init__(self):
        config = {
            "rule_id": "trustworthiness_indicators",
            "category": "citation_probability",
            "description": "Evaluates trustworthiness indicators",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate trustworthiness indicators"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for contact information
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Organization":
                if item.get("telephone") or item.get("email") or item.get("contactPoint"):
                    score += 4
        
        # Check for content freshness indicators
        if content_metrics.get("word_count", 0) >= 500:
            score += 3  # Substantial content
        
        # Check for structured data completeness
        if len(graph) >= 2:
            score += 3
        
        # Check for legal/about pages indicators
        for item in graph:
            if item.get("@type") == "WebPage":
                if "about" in item.get("url", "").lower() or "contact" in item.get("url", "").lower():
                    score += 2
        
        # Check for quality content indicators
        readability = content_metrics.get("readability_score", 0)
        if 30 <= readability <= 70:  # Professional readability
            score += 3
        
        return min(score, self.max_score)

class ContentDepthRule(BaseRule):
    """Evaluates content depth and comprehensiveness"""
    
    def __init__(self):
        config = {
            "rule_id": "content_depth",
            "category": "citation_probability",
            "description": "Evaluates content depth and comprehensiveness",
            "weight": 1.5,
            "max_score": 15,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate content depth"""
        content_metrics = data.get("content_metrics", {})
        heading_metrics = data.get("heading_metrics", {})
        faq_metrics = data.get("faq_metrics", {})
        step_metrics = data.get("step_metrics", {})
        
        score = 0
        
        # Check word count
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 2000:
            score += 5
        elif word_count >= 1000:
            score += 3
        elif word_count >= 500:
            score += 1
        
        # Check heading structure depth
        h2_count = heading_metrics.get("h2_count", 0)
        h3_count = heading_metrics.get("h3_count", 0)
        if h2_count >= 5 and h3_count >= 3:
            score += 4
        elif h2_count >= 3 and h3_count >= 1:
            score += 2
        elif h2_count >= 2:
            score += 1
        
        # Check for FAQ content
        if faq_metrics.get("faq_detected"):
            faq_count = faq_metrics.get("question_count", 0)
            if faq_count >= 5:
                score += 3
            elif faq_count >= 3:
                score += 2
            elif faq_count >= 1:
                score += 1
        
        # Check for step-by-step content
        if step_metrics.get("step_section_present"):
            step_count = step_metrics.get("step_count", 0)
            if step_count >= 5:
                score += 3
            elif step_count >= 3:
                score += 2
            elif step_count >= 1:
                score += 1
        
        return min(score, self.max_score)

class ExpertiseIndicatorsRule(BaseRule):
    """Evaluates expertise indicators in content"""
    
    def __init__(self):
        config = {
            "rule_id": "expertise_indicators",
            "category": "citation_probability",
            "description": "Evaluates expertise indicators in content",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate expertise indicators"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for author expertise
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Person":
                if item.get("jobTitle") or item.get("knowsAbout") or item.get("award"):
                    score += 3
                if item.get("alumniOf") or item.get("worksFor"):
                    score += 2
        
        # Check for specialized entities
        entities = entity_graph.get("entities", [])
        specialized_entities = 0
        for entity in entities:
            if entity.get("@type") in [
                "MedicalEntity", "Drug", "MedicalProcedure", "MedicalTest",
                "ResearchProject", "ScholarlyArticle", "Thesis"
            ]:
                specialized_entities += 1
        
        if specialized_entities >= 2:
            score += 3
        elif specialized_entities >= 1:
            score += 1
        
        # Check for professional content types
        for item in graph:
            if item.get("@type") in ["ScholarlyArticle", "MedicalWebPage", "Report"]:
                score += 2
        
        return min(score, self.max_score)

class DataAndSourcesRule(BaseRule):
    """Evaluates presence of data and source references"""
    
    def __init__(self):
        config = {
            "rule_id": "data_and_sources",
            "category": "citation_probability",
            "description": "Evaluates presence of data and source references",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate data and sources"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for dataset references
        graph = structured_data.get("@graph", [])
        for item in graph:
            if item.get("@type") == "Dataset":
                score += 4
        
        # Check for cited works
        for item in graph:
            if item.get("@type") in ["Citation", "CreativeWork"]:
                score += 3
        
        # Check for statistical content indicators
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 1000:  # Longer content more likely to contain data
            score += 2
        
        # Check for organization with research focus
        for item in graph:
            if item.get("@type") == "Organization":
                if "research" in item.get("name", "").lower() or "university" in item.get("name", "").lower():
                    score += 1
        
        return min(score, self.max_score)

class UniquenessValueRule(BaseRule):
    """Evaluates uniqueness and value proposition"""
    
    def __init__(self):
        config = {
            "rule_id": "uniqueness_value",
            "category": "citation_probability",
            "description": "Evaluates uniqueness and value proposition",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate uniqueness and value"""
        entity_metrics = data.get("entity_metrics", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check for unique entities
        unique_entity_types = entity_metrics.get("unique_entity_types", 0)
        if unique_entity_types >= 5:
            score += 4
        elif unique_entity_types >= 3:
            score += 2
        elif unique_entity_types >= 1:
            score += 1
        
        # Check for substantial original content
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 1500:
            score += 3
        elif word_count >= 800:
            score += 2
        elif word_count >= 300:
            score += 1
        
        # Check for entity density (indicates rich content)
        entity_density = entity_metrics.get("entity_per_1000_words", 0)
        if entity_density >= 8:
            score += 3
        elif entity_density >= 5:
            score += 2
        elif entity_density >= 2:
            score += 1
        
        return min(score, self.max_score)

class TechnicalQualityRule(BaseRule):
    """Evaluates technical quality affecting citation likelihood"""
    
    def __init__(self):
        config = {
            "rule_id": "technical_quality",
            "category": "citation_probability",
            "description": "Evaluates technical quality affecting citation likelihood",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate technical quality"""
        quality_flags = data.get("quality_flags", {})
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        
        # Check quality flags
        if not quality_flags.get("no_main_content_detected", True):
            score += 3
        
        if not quality_flags.get("malformed_structure", True):
            score += 2
        
        if not quality_flags.get("low_word_count", True):
            score += 2
        
        # Check content structure
        paragraph_count = content_metrics.get("paragraph_count", 0)
        if paragraph_count >= 5:
            score += 2
        elif paragraph_count >= 3:
            score += 1
        
        # Check for extraction success
        if data.get("http_status_code") == 200:
            score += 1
        
        return min(score, self.max_score)

class ExternalValidationRule(BaseRule):
    """Evaluates external validation signals"""
    
    def __init__(self):
        config = {
            "rule_id": "external_validation",
            "category": "citation_probability",
            "description": "Evaluates external validation signals",
            "weight": 1.0,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate external validation"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        entity_graph = data.get("unified_entity_graph", {})
        
        score = 0
        
        # Check for sameAs references (external validation)
        graph = structured_data.get("@graph", [])
        sameas_count = 0
        for item in graph:
            if item.get("sameAs"):
                sameas_count += len(item["sameAs"]) if isinstance(item["sameAs"], list) else 1
        
        if sameas_count >= 3:
            score += 5
        elif sameas_count >= 1:
            score += 3
        
        # Check for external entity references
        entities = entity_graph.get("entities", [])
        external_entities = 0
        for entity in entities:
            if entity.get("sameAs") or entity.get("url"):
                external_entities += 1
        
        if external_entities >= 3:
            score += 3
        elif external_entities >= 1:
            score += 1
        
        # Check for review/rating systems
        for item in graph:
            if item.get("@type") in ["Review", "Rating", "AggregateRating"]:
                score += 2
        
        return min(score, self.max_score)

class SocialProofSignalsRule(BaseRule):
    """Evaluates social proof signals like reviews and ratings"""
    
    def __init__(self):
        config = {
            "rule_id": "social_proof_signals",
            "category": "citation_probability",
            "description": "Evaluates social proof signals like reviews and ratings",
            "weight": 0.6,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate social proof signals"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for review/rating aggregates
        for item in graph:
            item_type = item.get("@type", "")
            if item_type in ["AggregateRating", "Review", "Rating"]:
                score += 4
                # Bonus for rating value and count
                if item.get("ratingValue"):
                    score += 2
                if item.get("reviewCount") or item.get("ratingCount"):
                    score += 2
                break
        
        # Check for Product or Service with offers/reviews
        for item in graph:
            item_type = item.get("@type", "")
            if item_type in ["Product", "Service", "LocalBusiness"]:
                if item.get("aggregateRating") or item.get("review"):
                    score += 2
                break
        
        return min(score, self.max_score)

class ContactCompletenessRule(BaseRule):
    """Evaluates completeness of contact information for trust signals"""
    
    def __init__(self):
        config = {
            "rule_id": "contact_completeness",
            "category": "citation_probability",
            "description": "Evaluates completeness of contact information for trust signals",
            "weight": 0.5,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate contact completeness"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for Organization or LocalBusiness with contact info
        for item in graph:
            item_type = item.get("@type", "")
            if item_type in ["Organization", "LocalBusiness", "Person"]:
                contact_points = 0
                
                # Check various contact fields
                if item.get("telephone") or item.get("phone"):
                    contact_points += 1
                if item.get("email"):
                    contact_points += 1
                if item.get("url") or item.get("website"):
                    contact_points += 1
                if item.get("address") or item.get("location"):
                    contact_points += 1
                
                # Check for contactPoint array
                if item.get("contactPoint"):
                    contact_points += 2
                
                # Score based on contact completeness
                if contact_points >= 4:
                    score += 7
                elif contact_points >= 3:
                    score += 5
                elif contact_points >= 2:
                    score += 3
                elif contact_points >= 1:
                    score += 1
                break
        
        return min(score, self.max_score)

class CitationFormatReadinessRule(BaseRule):
    """Evaluates readiness for academic/professional citation"""
    
    def __init__(self):
        config = {
            "rule_id": "citation_format_readiness",
            "category": "citation_probability",
            "description": "Evaluates readiness for academic/professional citation",
            "weight": 0.7,
            "max_score": 10,
            "applies_to": "page"
        }
        super().__init__(config)
    
    def evaluate(self, data: Dict[str, Any]) -> float:
        """Evaluate citation format readiness"""
        structured_data = data.get("structured_data", {})
        if isinstance(structured_data, str):
            try:
                import json
                structured_data = json.loads(structured_data)
            except (json.JSONDecodeError, TypeError):
                structured_data = {}
        content_metrics = data.get("content_metrics", {})
        
        score = 0
        graph = structured_data.get("@graph", [])
        
        # Check for academic/professional content types
        academic_types = ["ScholarlyArticle", "MedicalWebPage", "Report", "Dataset", "Thesis"]
        for item in graph:
            if item.get("@type") in academic_types:
                score += 5
                # Bonus for citation fields
                if item.get("citation") or item.get("isPartOf"):
                    score += 2
                break
        
        # Check for author information (critical for citation)
        for item in graph:
            if item.get("@type") in ["Person", "Author"]:
                if item.get("name"):
                    score += 2
                if item.get("affiliation") or item.get("alumniOf"):
                    score += 1
                break
        
        # Check content length (longer content more likely to be cited)
        word_count = content_metrics.get("word_count", 0)
        if word_count >= 1500:
            score += 2
        elif word_count >= 1000:
            score += 1
        
        return min(score, self.max_score)

# Register all Citation Probability rules
def register_citation_probability_rules(registry):
    """Register all Citation Probability category rules"""
    registry.register(AuthoritySignalsRule())
    registry.register(TrustworthinessIndicatorsRule())
    registry.register(ContentDepthRule())
    registry.register(ExpertiseIndicatorsRule())
    registry.register(DataAndSourcesRule())
    registry.register(UniquenessValueRule())
    registry.register(TechnicalQualityRule())
    registry.register(ExternalValidationRule())
    registry.register(SocialProofSignalsRule())
    registry.register(ContactCompletenessRule())
    registry.register(CitationFormatReadinessRule())
