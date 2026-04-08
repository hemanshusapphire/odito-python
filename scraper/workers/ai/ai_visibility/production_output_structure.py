"""
Production-Grade AI Visibility Scraper Output Structure
Ideal JSON structure for 60+ AI SEO rules evaluation
"""

IDEAL_OUTPUT_STRUCTURE = {
    # ==================== CORE METADATA ====================
    "metadata": {
        "page_title": {
            "text": "",
            "length": 0,
            "in_title_tag": True,
            "keyword_placement": "",
            "optimization_score": 0
        },
        "meta_description": {
            "text": "",
            "length": 0,
            "keyword_present": False,
            "call_to_action": False,
            "optimization_score": 0
        },
        "canonical_url": {
            "url": "",
            "normalized": "",
            "self_referencing": True,
            "accessible": True
        },
        "hreflang_tags": {
            "present": False,
            "languages": [],
            "return_tags": [],
            "self_referencing": False,
            "implementation_quality": "complete"
        },
        "robots_meta": {
            "directive": "",
            "indexable": True,
            "followable": True,
            "max_snippet": "",
            "max_image_preview": ""
        },
        "open_graph": {
            "present": False,
            "complete": False,
            "types": [],
            "image_present": False,
            "description_present": False
        },
        "twitter_card": {
            "present": False,
            "type": "",
            "complete": False,
            "image_present": False
        },
        "language_declaration": {
            "declared": "",
            "detected": "",
            "consistent": True,
            "locale_format": "proper"
        },
        "content_type": {
            "declared": "",
            "charset": "",
            "proper_format": True
        }
    },
    
    # ==================== STRUCTURED DATA ====================
    "structured_data": {
        "json_ld_schemas": {
            "count": 0,
            "types": [],
            "valid": True,
            "errors": [],
            "warnings": 0,
            "duplicate_types": []
        },
        "microdata_present": {
            "present": False,
            "items": [],
            "itemtypes": []
        },
        "rdfa_present": {
            "present": False,
            "triples": [],
            "vocabularies": []
        },
        "entities": {
            "total": 0,
            "organizations": 0,
            "persons": 0,
            "places": 0,
            "articles": 0,
            "products": 0,
            "services": 0,
            "events": 0,
            "reviews": 0,
            "primary_entity": {
                "type": "",
                "name": "",
                "id": "",
                "properties": {}
            }
        },
        "breadcrumbs": {
            "schema_present": False,
            "html_present": False,
            "consistent": False,
            "levels": 0,
            "hierarchy_depth": 0
        },
        "faq": {
            "schema_present": False,
            "questions_count": 0,
            "with_answers": 0,
            "html_detected": False,
            "total_questions": 0
        },
        "howto": {
            "present": False,
            "step_count": 0,
            "tools_supplies": 0,
            "estimated_time": ""
        },
        "article_schema": {
            "present": False,
            "type": "",
            "author_present": False,
            "date_present": False,
            "publisher_present": False
        },
        "product_schema": {
            "present": False,
            "offers_count": 0,
            "reviews_count": 0,
            "aggregate_rating": 0,
            "availability": ""
        },
        "local_business": {
            "present": False,
            "name": "",
            "address_complete": False,
            "phone_present": False,
            "geo_present": False,
            "opening_hours": False
        },
        "event_schema": {
            "present": False,
            "dates_complete": False,
            "location_present": False,
            "offers_present": False
        },
        "organization_schema": {
            "present": False,
            "name": "",
            "legal_name": "",
            "url": "",
            "logo_present": False,
            "contact_present": False,
            "social_profiles": []
        }
    },
    
    # ==================== CONTENT ANALYSIS ====================
    "content_analysis": {
        "main_content": {
            "word_count": 0,
            "character_count": 0,
            "quality_score": 0,
            "extraction_method": "",
            "content_to_html_ratio": 0,
            "readability_score": 0,
            "language_confidence": 0
        },
        "heading_structure": {
            "h1_count": 0,
            "h2_count": 0,
            "h3_count": 0,
            "h4_count": 0,
            "h5_count": 0,
            "h6_count": 0,
            "hierarchy_valid": True,
            "structure_score": 0,
            "sequence_violations": [],
            "question_headings": 0,
            "heading_depth_optimal": True
        },
        "paragraph_metrics": {
            "count": 0,
            "avg_length": 0,
            "short_paragraph_ratio": 0,
            "long_paragraph_ratio": 0,
            "readability_score": 0,
            "flesch_kincaid": 0,
            "complex_sentences": 0
        },
        "content_sections": {
            "definitions": 0,
            "use_cases": 0,
            "steps": 0,
            "comparisons": 0,
            "examples": 0,
            "statistics": 0,
            "quotes": 0,
            "section_diversity": 0
        },
        "keyword_analysis": {
            "primary_keyword": "",
            "keyword_density": 0,
            "stuffing_detected": False,
            "semantic_terms": [],
            "topic_coverage": 0,
            "lsi_score": 0
        },
        "content_freshness": {
            "publication_date": "",
            "last_updated": "",
            "freshness_days": 0,
            "content_recency": "recent"
        },
        "content_length": {
            "characters": 0,
            "words": 0,
            "comprehensive": False,
            "thin_content": False,
            "length_category": "optimal"
        },
        "unique_value": {
            "originality_score": 0,
            "duplicate_detected": False,
            "value_proposition": "",
            "expertise_indicators": 0
        }
    },
    
    # ==================== ENTITY ANALYSIS ====================
    "entity_analysis": {
        "primary_entity": {
            "name": "",
            "type": "",
            "confidence": 0,
            "schema_backed": True
        },
        "entity_mentions": {
            "total_mentions": 0,
            "primary_entity_mentions": 0,
            "entity_density_per_1000_words": 0,
            "first_mention_position": 0,
            "mention_distribution": "uniform"
        },
        "named_entities": {
            "persons": [],
            "organizations": [],
            "locations": [],
            "dates": [],
            "products": [],
            "brands": []
        },
        "entity_relationships": {
            "total_relationships": 0,
            "internal_entities": 0,
            "external_entities": 0,
            "relationship_types": [],
            "graph_connectivity": 0
        },
        "entity_consistency": {
            "schema_vs_html_consistent": True,
            "name_variations": [],
            "canonical_entity": ""
        },
        "knowledge_graph_signals": {
            "sameas_links": 0,
            "wikipedia_references": 0,
            "official_website": False,
            "social_profiles": 0
        }
    },
    
    # ==================== AUTHOR SIGNALS ====================
    "author_signals": {
        "author_detected": False,
        "author_name": "",
        "author_schema": {
            "present": False,
            "type": "",
            "complete": False
        },
        "author_byline": {
            "present": False,
            "text": "",
            "sources": []
        },
        "author_credentials": {
            "present": False,
            "expertise": [],
            "job_title": "",
            "years_experience": 0,
            "publications": 0,
            "awards": 0
        },
        "author_contact": {
            "email": "",
            "social_links": {
                "linkedin": "",
                "twitter": "",
                "website": "",
                "other": []
            },
            "contact_form": False
        },
        "author_bio": {
            "present": False,
            "length": 0,
            "expertise_indicators": 0,
            "professional_background": 0
        },
        "author_image": {
            "present": False,
            "alt_text": "",
            "professional": False,
            "headshot": False
        },
        "author_social_proof": {
            "linkedin": {"present": False, "verified": False, "followers": 0},
            "twitter": {"present": False, "verified": False, "followers": 0},
            "other_platforms": []
        },
        "author_expertise": {
            "topic_expertise": 0,
            "industry_expertise": 0,
            "academic_credentials": 0,
            "professional_certifications": 0
        },
        "author_consistency": {
            "same_across_pages": True,
            "variations": [],
            "canonical_author": ""
        },
        "publication_dates": {
            "datePublished": "",
            "dateModified": "",
            "freshness": 0,
            "recency_category": "recent"
        }
    },
    
    # ==================== LOCAL SEO SIGNALS ====================
    "local_seo_signals": {
        "nap_consistency": {
            "consistent": False,
            "variations": [],
            "completeness_score": 0,
            "canonical_nap": ""
        },
        "business_information": {
            "name": "",
            "legal_name": "",
            "category": "",
            "verified": False,
            "business_type": ""
        },
        "address_details": {
            "street": "",
            "city": "",
            "state": "",
            "zip": "",
            "country": "",
            "formatted": "",
            "complete": False
        },
        "phone_numbers": {
            "primary": "",
            "formatted": "",
            "clickable": False,
            "variations": [],
            "international_format": False
        },
        "geo_coordinates": {
            "present": False,
            "lat": 0,
            "lng": 0,
            "accurate": False,
            "precision": "city"
        },
        "service_area": {
            "defined": False,
            "radius": 0,
            "cities_served": [],
            "regions_served": []
        },
        "local_reviews": {
            "present": False,
            "count": 0,
            "average_rating": 0,
            "recent_reviews": 0,
            "review_sources": []
        },
        "local_citations": {
            "mentions": 0,
            "consistent": False,
            "quality": 0,
            "authoritative_sources": 0
        },
        "opening_hours": {
            "present": False,
            "complete_specification": False,
            "hours_specification": [],
            "christmas_hours": False
        },
        "local_schema": {
            "localbusiness_present": False,
            "type": "",
            "complete": False,
            "additional_properties": []
        }
    },
    
    # ==================== ANSWER ENGINE SIGNALS ====================
    "answer_engine_signals": {
        "direct_answers": {
            "count": 0,
            "avg_length": 0,
            "optimal_range": False,
            "first_paragraph_answer": False,
            "answer_quality": 0
        },
        "featured_snippet_ready": {
            "ready": False,
            "format": "",
            "position": "",
            "confidence": 0,
            "optimization_score": 0
        },
        "question_optimization": {
            "questions_answered": 0,
            "question_headings": 0,
            "answer_quality": 0,
            "question_coverage": 0
        },
        "definition_content": {
            "present": False,
            "patterns": [],
            "comprehensive": False,
            "definition_count": 0
        },
        "structured_answers": {
            "lists": {"ordered": 0, "unordered": 0},
            "tables": {"present": False, "rows": 0, "headers": 0},
            "steps": 0,
            "comparisons": 0,
            "recipes": 0
        },
        "concise_explanations": {
            "count": 0,
            "word_range": False,
            "clear_language": False,
            "actionable": 0
        },
        "voice_search_ready": {
            "natural_language": False,
            "conversational": False,
            "actionable": False,
            "readability_optimized": False
        },
        "faq_optimization": {
            "faq_present": False,
            "questions_count": 0,
            "schema_backed": False,
            "html_consistent": False,
            "answer_quality": 0
        },
        "how_to_optimization": {
            "how_to_present": False,
            "step_count": 0,
            "tools_materials": 0,
            "time_estimates": 0,
            "image_steps": 0
        }
    },
    
    # ==================== TECHNICAL SIGNALS ====================
    "technical_signals": {
        "crawlability": {
            "robots_txt_accessible": True,
            "sitemap_referenced": False,
            "crawlable": True,
            "indexable": True,
            "blocked_resources": 0
        },
        "mobile_optimization": {
            "viewport_present": True,
            "responsive": True,
            "mobile_friendly": True,
            "tap_targets": True,
            "font_sizes": True
        },
        "page_speed_indicators": {
            "optimized_images": False,
            "minified_css": False,
            "minified_js": False,
            "lazy_loading": False,
            "compression": False,
            "caching": False
        },
        "schema_validation": {
            "valid_json": True,
            "required_fields": True,
            "warnings": 0,
            "errors": 0,
            "deprecated_properties": []
        },
        "image_optimization": {
            "total_images": 0,
            "with_alt_text": 0,
            "alt_text_coverage": 0,
            "file_sizes_optimized": False,
            "responsive_images": False,
            "next_gen_formats": False
        },
        "video_optimization": {
            "total_videos": 0,
            "with_transcript": 0,
            "with_captions": 0,
            "accessible": False,
            "optimized_thumbnails": False
        },
        "accessibility": {
            "aria_labels": 0,
            "color_contrast": False,
            "keyboard_navigable": False,
            "screen_reader_friendly": False,
            "form_labels": 0
        },
        "security_signals": {
            "https": True,
            "security_headers": False,
            "safe_browsing": True,
            "mixed_content": False,
            "csp_present": False
        },
        "performance_metrics": {
            "dom_size": 0,
            "requests": 0,
            "optimization_score": 0,
            "core_web_vitals_indicators": False
        },
        "browser_compatibility": {
            "modern_standards": True,
            "fallbacks": False,
            "errors": 0,
            "deprecated_tags": 0
        },
        "internal_linking": {
            "total_links": 0,
            "internal_links": 0,
            "external_links": 0,
            "contextual_links": 0,
            "descriptive_anchor_text": 0,
            "link_depth": 0,
            "orphan_pages": 0
        }
    },
    
    # ==================== AI VISIBILITY SPECIFIC ====================
    "ai_visibility_signals": {
        "organization_validation": {
            "organization_detected": False,
            "name_present": False,
            "legal_name_present": False,
            "contact_complete": False
        },
        "llms_txt": {
            "checked": False,
            "exists": False,
            "status_code": None,
            "content_accessible": False
        },
        "geo_signals": {
            "geo_schema_present": False,
            "map_embed_present": False,
            "coordinates_accurate": False
        },
        "ai_crawler_optimization": {
            "structured_data_complete": False,
            "entity_relationships_clear": False,
            "content_machine_readable": False,
            "semantic_markup_comprehensive": False
        },
        "generative_ai_ready": {
            "authoritative_content": False,
            "factual_accuracy_indicators": 0,
            "source_citations": 0,
            "expertise_signals": 0,
            "trustworthiness_score": 0
        }
    },
    
    # ==================== PERFORMANCE & QUALITY ====================
    "extraction_metadata": {
        "extraction_timestamp": "",
        "processing_time_ms": 0,
        "html_size_bytes": 0,
        "extraction_method": "production_grade",
        "errors_encountered": [],
        "warnings_generated": [],
        "data_completeness": 0,
        "confidence_score": 0,
        "extraction_version": "2.0"
    }
}


def get_production_output_template():
    """Return the complete production output structure"""
    return IDEAL_OUTPUT_STRUCTURE.copy()


def validate_output_structure(data: dict) -> dict:
    """Validate that extracted data matches the expected structure"""
    validation_results = {
        "valid": True,
        "missing_sections": [],
        "invalid_types": [],
        "completeness_score": 0
    }
    
    total_sections = len(IDEAL_OUTPUT_STRUCTURE)
    present_sections = 0
    
    for section_key, section_structure in IDEAL_OUTPUT_STRUCTURE.items():
        if section_key in data:
            present_sections += 1
            
            # Validate structure type (basic check)
            if not isinstance(data[section_key], type(section_structure)):
                validation_results["invalid_types"].append(section_key)
        else:
            validation_results["missing_sections"].append(section_key)
    
    validation_results["completeness_score"] = (present_sections / total_sections) * 100
    validation_results["valid"] = (
        len(validation_results["missing_sections"]) == 0 and
        len(validation_results["invalid_types"]) == 0
    )
    
    return validation_results


def calculate_ai_visibility_score(data: dict) -> dict:
    """Calculate overall AI visibility score from extracted data"""
    score_breakdown = {
        "overall_score": 0,
        "metadata_score": 0,
        "structured_data_score": 0,
        "content_quality_score": 0,
        "entity_signals_score": 0,
        "author_eat_score": 0,
        "local_seo_score": 0,
        "answer_engine_score": 0,
        "technical_seo_score": 0,
        "ai_readiness_score": 0
    }
    
    weights = {
        "metadata_score": 0.10,
        "structured_data_score": 0.20,
        "content_quality_score": 0.15,
        "entity_signals_score": 0.10,
        "author_eat_score": 0.15,
        "local_seo_score": 0.10,
        "answer_engine_score": 0.10,
        "technical_seo_score": 0.05,
        "ai_readiness_score": 0.05
    }
    
    # Calculate individual scores (simplified for example)
    # In production, this would be much more sophisticated
    
    # Metadata score
    metadata = data.get("metadata", {})
    meta_score = 0
    if metadata.get("page_title", {}).get("text"):
        meta_score += 25
    if metadata.get("meta_description", {}).get("text"):
        meta_score += 25
    if metadata.get("canonical_url", {}).get("present"):
        meta_score += 25
    if metadata.get("hreflang_tags", {}).get("present"):
        meta_score += 25
    score_breakdown["metadata_score"] = meta_score
    
    # Structured data score
    structured_data = data.get("structured_data", {})
    schema_score = 0
    if structured_data.get("json_ld_schemas", {}).get("count", 0) > 0:
        schema_score += 30
    if structured_data.get("entities", {}).get("total", 0) > 0:
        schema_score += 30
    if structured_data.get("breadcrumbs", {}).get("present"):
        schema_score += 20
    if structured_data.get("faq", {}).get("present"):
        schema_score += 20
    score_breakdown["structured_data_score"] = schema_score
    
    # Calculate weighted overall score
    overall_score = 0
    for score_key, weight in weights.items():
        overall_score += score_breakdown[score_key] * weight
    
    score_breakdown["overall_score"] = round(overall_score, 1)
    
    return score_breakdown


# Export constants
__all__ = [
    'IDEAL_OUTPUT_STRUCTURE',
    'get_production_output_template',
    'validate_output_structure',
    'calculate_ai_visibility_score'
]
