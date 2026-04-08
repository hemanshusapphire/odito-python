import os
import re
from datetime import datetime
from bson.objectid import ObjectId
from fastapi import HTTPException
import requests

# Import AI visibility collections
from db import seo_ai_visibility, seo_ai_visibility_issues, db
from scraper.rules.ai_visibility_rules import AI_VISIBILITY_RULES, get_ai_rule, ai_rule_exists

# 🔥 FIX 3: ISO 3166-1 alpha-2 country codes for strict validation
VALID_ISO_ALPHA2 = {
    "AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BQ","BA","BW","BV","BR","IO","BN","BG","BF","BI","CV","KH","CM","CA","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CW","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","SZ","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","NC","NZ","NI","NE","NG","NU","NF","MK","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","BL","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","SS","ES","LK","SD","SR","SJ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","US","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"
}

def safe_string(value):
    """Safely extract string value from various data types"""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value and isinstance(value[0], str):
        return value[0]
    return ""

def normalize_text(value):
    """Normalize text values from seo_ai_visibility_data - prevents all .split() errors"""
    if value is None:
        return ""
    
    if isinstance(value, str):
        return value.strip()
    
    if isinstance(value, list):
        if value and isinstance(value[0], str):
            return value[0].strip()
    
    return str(value).strip()

def create_ai_visibility_issue(job_id, project_id, url, rule_no, category, severity, issue_code, issue_message, detected_value, expected_value, data_key=None, data_path=None):
    """Create a standardized AI visibility issue document"""
    return {
        "projectId": ObjectId(project_id),
        "ai_jobId": ObjectId(job_id),
        "page_url": url,
        "rule_no": rule_no,
        "category": category,
        "severity": severity,
        "issue_code": issue_code,
        "rule_id": issue_code,  # Primary identifier for AI visibility rules
        "issue_message": issue_message,
        "detected_value": detected_value,
        "expected_value": expected_value,
        "data_key": data_key,
        "data_path": data_path,
        "created_at": datetime.utcnow()
    }

def execute_ai_visibility_rule(rule_id, rule_config, normalized, job_id, project_id, url, rule_no):
    """Execute a single AI visibility rule with proper error handling."""
    issues = []
    
    rule_name = rule_config.get("name", rule_id)
    category = rule_config.get("category", "Unknown")
    severity = rule_config.get("severity", "medium")
    
    try:
        if rule_id == "JSON_LD_MULTIPLE_BLOCKS":
            # Check for multiple JSON-LD blocks
            json_ld_blocks = normalized.get("json_ld_blocks", [])
            if isinstance(json_ld_blocks, list) and len(json_ld_blocks) > 1:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Multiple JSON-LD blocks detected",
                    len(json_ld_blocks), 1,  # detected, expected
                    data_key="json_ld_blocks"
                ))
        
        elif rule_id == "SINGLE_JSON_LD_BLOCK_MISSING":
            # Check for missing single consolidated block
            final_block = normalized.get("final_json_ld_block", {})
            if not final_block or not final_block.get("@graph"):
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Single consolidated JSON-LD block missing",
                    "None", "Single @graph block",
                    data_key="final_json_ld_block"
                ))
        
        elif rule_id == "ORGANIZATION_PERMANENCE_VIOLATION":
            # Check Organization ID permanence
            entities = normalized.get("parsed_entities", [])
            org_entities = [e for e in entities if e.get("@type") == "Organization"]
            
            for org in org_entities:
                org_id = org.get("@id", "")
                if org_id and "#" in org_id and not org_id.endswith("#organization"):
                    issues.append(create_ai_visibility_issue(
                        job_id, project_id, url, rule_no, category, severity,
                        rule_id, "Organization ID not permanent",
                        org_id, "domain.com/#organization",
                        data_key="organization.@id"
                    ))
        
        elif rule_id == "DUPLICATE_ENTITY_ELIMINATION_FAILURE":
            # Check for duplicate entities
            entities = normalized.get("parsed_entities", [])
            entity_signatures = {}
            duplicates = []
            
            for entity in entities:
                entity_type = entity.get("@type", "")
                entity_id = entity.get("@id", "")
                
                # 🔥 FIX 2: Include entity_id for proper uniqueness
                sig_parts = [entity_type, entity_id]
                
                if entity_type == "PostalAddress":
                    sig_parts.extend([
                        entity.get("streetAddress", ""),
                        entity.get("addressLocality", ""),
                        entity.get("postalCode", ""),
                        entity.get("addressCountry", "")
                    ])
                
                # 🔥 FIX 2: Use "|" separator and proper signature building
                signature = "|".join(str(part).lower().strip() for part in sig_parts if part)
                
                if signature in entity_signatures:
                    duplicates.append(entity_type)
                else:
                    entity_signatures[signature] = True
            
            if duplicates:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Duplicate entities not eliminated",
                    len(duplicates), "No duplicates",
                    data_key="parsed_entities"
                ))
        
        elif rule_id == "SYNTHETIC_ID_GENERATION":
            # Check for synthetic IDs
            entities = normalized.get("parsed_entities", [])
            synthetic_ids = []
            
            for entity in entities:
                entity_id = entity.get("@id", "")
                if entity_id.startswith("synthetic_"):
                    synthetic_ids.append(entity_id)
            
            if synthetic_ids:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Synthetic IDs detected in graph",
                    len(synthetic_ids), "No synthetic IDs",
                    data_key="parsed_entities.@id"
                ))
        
        elif rule_id == "ISO_COUNTRY_NORMALIZATION_PARTIAL":
            # Check country normalization with strict ISO 3166-1 alpha-2 validation
            entities = normalized.get("parsed_entities", [])
            invalid_countries = []
            
            for entity in entities:
                address = entity.get("address", {})
                if isinstance(address, dict):
                    country = address.get("addressCountry", "")
                    # 🔥 FIX 3: Use strict ISO 3166-1 alpha-2 validation
                    if country and country not in VALID_ISO_ALPHA2:
                        invalid_countries.append(country)
            
            if invalid_countries:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Country codes not fully normalized to ISO format",
                    len(invalid_countries), "ISO 3166-1 alpha-2",
                    data_key="address.addressCountry"
                ))
        
        elif rule_id == "GRAPH_CLEANLINESS_VIOLATIONS":
            # Check for graph cleanliness
            entities = normalized.get("parsed_entities", [])
            violations = []
            
            for entity in entities:
                # Check for null/empty properties
                for key, value in entity.items():
                    if value is None or value == "":
                        violations.append(f"{entity.get('@type', 'Unknown')}.{key}")
                
                # Check for empty lists/dicts
                address = entity.get("address", {})
                if isinstance(address, dict) and len(address) == 0:
                    violations.append(f"{entity.get('@type', 'Unknown')}.address")
            
            if violations:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Graph contains null/empty properties",
                    len(violations), "Clean properties",
                    data_key="parsed_entities"
                ))
        
        elif rule_id == "MISSING_SCHEMA_ARCHITECTURE_FIXES":
            # Check if architecture fixes were applied
            fixes_applied = normalized.get("architecture_fixes_applied", False)
            if not fixes_applied:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Required schema architecture fixes not applied",
                    False, True,
                    data_key="architecture_fixes_applied"
                ))
        
        elif rule_id == "ENTITY_ID_INCONSISTENCY":
            # Check entity ID consistency
            entities = normalized.get("parsed_entities", [])
            canonical_root = normalized.get("canonical_root", "")
            inconsistent_ids = []
            
            for entity in entities:
                entity_id = entity.get("@id", "")
                if entity_id and canonical_root and not entity_id.startswith(canonical_root):
                    inconsistent_ids.append(entity_id)
            
            if inconsistent_ids:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Entity IDs inconsistent across graph",
                    len(inconsistent_ids), "Canonical root consistency",
                    data_key="parsed_entities.@id"
                ))
        
        elif rule_id == "NESTED_OBJECT_GRAPH_NODES":
            # Check for nested objects as graph nodes
            entities = normalized.get("parsed_entities", [])
            nested_types = ["PostalAddress", "ContactPoint", "GeoCoordinates", "OpeningHoursSpecification"]
            nested_as_nodes = []
            
            for entity in entities:
                entity_type = entity.get("@type", "")
                entity_id = entity.get("@id", "")
                
                if entity_type in nested_types and entity_id:
                    nested_as_nodes.append(entity_type)
            
            if nested_as_nodes:
                issues.append(create_ai_visibility_issue(
                    job_id, project_id, url, rule_no, category, severity,
                    rule_id, "Nested objects incorrectly treated as graph nodes",
                    len(nested_as_nodes), "Nested objects stay embedded",
                    data_key="parsed_entities.@type"
                ))
        
    except Exception as rule_error:
        print(f"[ERROR] AI rule {rule_id} failed for {url}: {rule_error}")
        # Continue processing other rules
    
    return issues

def analyze_ai_visibility_page(page, job_id, project_id):
    """Analyze a single page for AI visibility issues using ALL rules from ai_visibility_rules.py"""
    issues = []
    
    # STEP 1: Normalize all page data
    try:
        normalized = {
            "url": safe_string(page.get("url", "")),
            "json_ld_blocks": page.get("raw_json_ld_blocks", []),  # 🔥 FIX 1: Map raw_json_ld_blocks correctly
            "final_json_ld_block": page.get("final_json_ld_block", {}),
            "parsed_entities": page.get("parsed_entities", []),
            "architecture_fixes_applied": page.get("architecture_fixes_applied", False),
            "canonical_root": page.get("canonical_root", ""),
            "unified_entity_graph": page.get("unified_entity_graph", {}),
            "raw_json_ld_blocks": page.get("raw_json_ld_blocks", []),
            "architecture_violations": page.get("architecture_violations", {}),
            "integrity_metrics": page.get("integrity_metrics", {})
        }
        
        url = normalized["url"]
        if not url:
            print(f"[WARNING] No URL found for page in AI visibility analysis")
            return issues
        
    except Exception as normalize_error:
        print(f"[ERROR] Failed to normalize AI visibility page data: {normalize_error}")
        return issues
    
    # STEP 2: Execute ALL rules from registry
    from scraper.rules.ai_visibility_rules import AI_VISIBILITY_RULES
    
    print(f"[DEBUG] Executing {len(AI_VISIBILITY_RULES)} AI visibility rules for {url}")
    
    # 🔥 PHASE 2: RULE NUMBER FIX - Use enumerate for correct rule_no
    for index, (rule_id, rule_config) in enumerate(AI_VISIBILITY_RULES.items(), start=1):
        try:
            rule_issues = execute_ai_visibility_rule(rule_id, rule_config, normalized, job_id, project_id, url, index)
            if rule_issues:
                print(f"[DEBUG] Rule {rule_id} (#{index}) found {len(rule_issues)} issues")
                issues.extend(rule_issues)
            else:
                print(f"[DEBUG] Rule {rule_id} (#{index}) found no issues")
        except Exception as rule_error:
            print(f"[ERROR] AI rule {rule_id} (#{index}) failed for {url}: {rule_error}")
            continue
    
    print(f"[DEBUG] Total issues found for {url}: {len(issues)}")
    
    # 🔥 CRITICAL FIX: Save issues to database
    if issues:
        try:
            seo_ai_visibility_issues.insert_many(issues)
            print(f"[DEBUG] Saved {len(issues)} issues to database for {url}")
        except Exception as save_error:
            print(f"[ERROR] Failed to save issues for {url}: {save_error}")
    
    return issues
