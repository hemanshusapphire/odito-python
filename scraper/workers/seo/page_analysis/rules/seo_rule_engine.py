"""
SEO Rule Engine

Centralized execution engine for modular SEO rules.
Replaces the monolithic execute_rule() if/elif chain.
"""

from .seo_rule_registry import SEORuleRegistry
from bson.objectid import ObjectId
from typing import Dict, List, Any
from .categories import register_all_seo_categories


class SEORuleEngine:
    """Centralized SEO rule execution engine"""

    def __init__(self, registry: SEORuleRegistry):
        self.registry = registry

    def _validate_object_id(self, id_str: str, id_type: str) -> ObjectId:
        """
        Validate and convert string to ObjectId.
        
        Args:
            id_str: String to validate and convert
            id_type: Type description for error messages ('job_id' or 'project_id')
            
        Returns:
            ObjectId instance
            
        Raises:
            ValueError: If id_str is not a valid ObjectId
        """
        if not id_str:
            raise ValueError(f"{id_type} cannot be empty")
        
        if not isinstance(id_str, str):
            raise ValueError(f"{id_type} must be a string, got {type(id_str).__name__}")
        
        try:
            return ObjectId(id_str)
        except Exception as e:
            raise ValueError(f"Invalid {id_type} format: '{id_str}'. Must be a valid 24-character hex string or 12-byte input. Error: {e}")

    def analyze_page(self, normalized: dict, job_id: str, project_id: str, url: str) -> dict:
        """
        Execute all registered rules against normalized page data.

        Args:
            normalized: Output of normalize_page_data() — UNCHANGED
            job_id:     Current job ID string
            project_id: Current project ID string
            url:        Page URL being analyzed

        Returns:
            Dict with:
            - "issues": List of issue documents (SAME FORMAT as before)
            - "recommendations": List of info-severity items (NEW)
            - "summary": Coverage summary document
            
        Raises:
            ValueError: If job_id or project_id are not valid ObjectIds
        """
        # STRICT VALIDATION BEFORE RULE EXECUTION
        try:
            validated_job_id = self._validate_object_id(job_id, 'job_id')
            validated_project_id = self._validate_object_id(project_id, 'project_id')
            print(f"[VALIDATION] ✅ ObjectId validation passed for job_id={job_id}, project_id={project_id}")
        except ValueError as validation_error:
            print(f"[VALIDATION] ❌ ObjectId validation failed: {validation_error}")
            raise validation_error

        issues = []
        recommendations = []  # FIXED: Separate recommendations from issues
        all_rules = self.registry.get_all_rules()
        total_rules = len(all_rules)
        
        # Summary tracking
        category_stats = {}
        failed_count = 0
        passed_count = 0
        applicable_rules = 0
        recommendation_count = 0  # FIXED: Track recommendations separately

        for rule in all_rules:
            try:
                rule_issues = rule.evaluate(normalized, job_id, project_id, url)
                
                # Initialize category stats if needed
                if rule.category not in category_stats:
                    category_stats[rule.category] = {"failed": 0, "passed": 0, "skipped": 0, "recommendations": 0}
                
                if rule_issues:
                    # FIXED: Separate issues from recommendations based on severity
                    high_medium_issues = []
                    info_recommendations = []
                    
                    for issue in rule_issues:
                        if hasattr(issue, 'get') and issue.get('severity') in ['info', 'low']:
                            info_recommendations.append(issue)
                        elif isinstance(issue, dict) and issue.get('severity') in ['info', 'low']:
                            info_recommendations.append(issue)
                        else:
                            high_medium_issues.append(issue)
                    
                    # Add high/medium severity issues
                    if high_medium_issues:
                        issues.extend(high_medium_issues)
                        failed_count += 1
                        category_stats[rule.category]["failed"] += 1
                        print(f"[RULE] ✅ {rule.rule_id}: {len(high_medium_issues)} issues generated")
                    
                    # Add info/low severity recommendations
                    if info_recommendations:
                        recommendations.extend(info_recommendations)
                        recommendation_count += 1
                        category_stats[rule.category]["recommendations"] += 1
                        print(f"[RULE] ℹ️ {rule.rule_id}: {len(info_recommendations)} recommendations generated")
                    
                    applicable_rules += 1
                else:
                    # Rule passed (empty return = passed for now)
                    passed_count += 1
                    category_stats[rule.category]["passed"] += 1
                    applicable_rules += 1
                    print(f"[RULE] ✅ {rule.rule_id}: No issues")
                    
            except Exception as e:
                # Rule skipped due to error
                if rule.category not in category_stats:
                    category_stats[rule.category] = {"failed": 0, "passed": 0, "skipped": 0, "recommendations": 0}
                category_stats[rule.category]["skipped"] += 1
                print(f"[ERROR] Rule {rule.rule_id} failed for {url}: {e}")
                continue

        # FIXED: Apply global deduplication to prevent duplicate issues
        deduplicated_issues = self._deduplicate_issues(issues)
        deduplicated_recommendations = self._deduplicate_issues(recommendations)

        # Generate summary document
        summary = {
            "total_rules": total_rules,
            "applicable_rules": applicable_rules,
            "failed_count": failed_count,
            "passed_count": passed_count,
            "recommendation_count": recommendation_count,  # FIXED: Track recommendations
            "skipped_count": total_rules - applicable_rules,
            "unique_issues": len(deduplicated_issues),  # FIXED: Count unique issues
            "unique_recommendations": len(deduplicated_recommendations),  # FIXED: Count unique recommendations
            "category_breakdown": category_stats
        }

        print(f"[SUMMARY] Total unique issues: {len(deduplicated_issues)} for {url}")
        print(f"[SUMMARY] Total recommendations: {len(deduplicated_recommendations)} for {url}")
        print(f"[SUMMARY] Coverage: {passed_count}/{applicable_rules} passed ({(passed_count/applicable_rules*100):.1f}%)" if applicable_rules > 0 else f"[SUMMARY] No applicable rules for {url}")
        
        return {
            "issues": deduplicated_issues,
            "recommendations": deduplicated_recommendations,  # FIXED: Return recommendations separately
            "summary": summary
        }

    def _deduplicate_issues(self, issues: list) -> list:
        """
        FIXED: Global deduplication to prevent duplicate issues.
        
        Uses (rule_id + message + url) as deduplication key.
        
        Args:
            issues: List of issue dictionaries
            
        Returns:
            Deduplicated list of issues
        """
        if not issues:
            return []
        
        seen_keys = set()
        deduplicated = []
        
        for issue in issues:
            if not isinstance(issue, dict):
                continue
            
            # Create deduplication key
            rule_id = issue.get('rule_id', '')
            message = issue.get('message', '')
            url = issue.get('url', '')
            
            dedupe_key = f"{rule_id}:{message}:{url}"
            
            if dedupe_key not in seen_keys:
                seen_keys.add(dedupe_key)
                deduplicated.append(issue)
            else:
                print(f"[DEDUPE] Removed duplicate issue: {rule_id} - {message[:50]}...")
        
        if len(deduplicated) < len(issues):
            print(f"[DEDUPE] Removed {len(issues) - len(deduplicated)} duplicate issues")
        
        return deduplicated


# ── Singleton ────────────────────────────────────────────────

_engine_instance = None


def get_seo_engine() -> SEORuleEngine:
    """Get or create the singleton SEO rule engine.
    
    Rules are registered once at first call, then reused.
    This avoids re-registering on every page analysis.
    """
    global _engine_instance
    if _engine_instance is None:
        registry = SEORuleRegistry()
        register_all_seo_categories(registry)
        _engine_instance = SEORuleEngine(registry)
        print(f"[ENGINE] SEO Rule Engine initialized | rules={registry.get_rule_count()} | categories={len(registry.get_categories())}")
    return _engine_instance
