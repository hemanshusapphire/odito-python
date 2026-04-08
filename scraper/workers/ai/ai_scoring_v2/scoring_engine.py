"""
Main Scoring Engine for AI Visibility Scoring v2

Orchestrates rule execution, score calculation, and result aggregation.
"""

from typing import Dict, List, Any, Optional
try:
    from .rule_registry import RuleRegistry
    from .normalization import ScoreNormalizer, CategoryWeights, ScoreValidator
except ImportError:
    # Fallback for direct execution
    from rule_registry import RuleRegistry
    from normalization import ScoreNormalizer, CategoryWeights, ScoreValidator
import logging
import json

# Configure logging
logger = logging.getLogger(__name__)

def safe_len(value):
    """Safe length helper for list/int comparisons"""
    if isinstance(value, list):
        return len(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except:
            return 0
    return 0

class ScoringEngine:
    """Main scoring engine for AI visibility v2"""
    
    def __init__(self, rule_registry: RuleRegistry):
        """
        Initialize scoring engine
        
        Args:
            rule_registry: Registry containing all scoring rules
        """
        self.rule_registry = rule_registry
        self.category_weights = CategoryWeights.get_default_weights()
        self.normalizer = ScoreNormalizer()
        self.validator = ScoreValidator()
        
        # Validate registry on initialization
        self._validate_registry()
    
    def _validate_registry(self):
        """Validate rule registry is complete and valid"""
        validation_report = self.rule_registry.validate_registry()
        
        if validation_report["issues"]:
            logger.warning(f"Registry validation issues: {validation_report['issues']}")
        
        # Validate category weights
        if not CategoryWeights.validate_weights(self.category_weights):
            raise ValueError("Invalid category weights configuration")
        
        logger.info(f"Scoring engine initialized with {validation_report['total_rules']} rules")
    
    def score_page(self, page_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score a single page using all rules
        
        Args:
            page_data: Extraction data from seo_ai_visibility collection
            
        Returns:
            Complete page score with breakdowns
        """
        try:
            # Initialize results structure
            page_score = {
                "page_url": page_data.get("url", ""),
                "page_ai_score": 0.0,
                "category_scores": {},
                "category_breakdowns": {},
                "rule_breakdown": [],
                "scoring_version": "v2"
            }
            
            # Process each category
            category_scores = {}
            category_breakdowns = {}
            all_rule_results = []
            
            for category in self.rule_registry.get_categories():
                category_result = self._score_category(page_data, category)
                
                category_scores[category] = category_result["score"]
                category_breakdowns[category] = category_result
                all_rule_results.extend(category_result["rule_results"])
            
            # Calculate final score as pure weighted average of category scores
            overall_score = self.normalizer.calculate_overall_score(
                category_scores, 
                self.category_weights
            )
            
            # Apply final clamp to ensure 0-100 bounds
            overall_score = max(0.0, min(overall_score, 100.0))
            
            # No distribution expansion - pure weighted average only
            
            # Assemble final result
            page_score["page_ai_score"] = overall_score
            page_score["category_scores"] = category_scores
            page_score["category_breakdowns"] = category_breakdowns
            page_score["rule_breakdown"] = all_rule_results
            
            # Validate final result
            if not self.validator.validate_page_score(page_score):
                logger.error(f"Invalid page score generated for {page_data.get('url', 'unknown')}")
                raise ValueError("Generated page score failed validation")
            
            return page_score
            
        except Exception as e:
            logger.error(f"Error scoring page {page_data.get('url', 'unknown')}: {e}")
            raise
    
    def _score_category(self, page_data: Dict[str, Any], category: str) -> Dict[str, Any]:
        """
        Score a single category
        
        Args:
            page_data: Page extraction data
            category: Category to score
            
        Returns:
            Category score with rule breakdown
        """
        try:
            # Get rules for this category
            rules = self.rule_registry.get_rules_by_category(category)
            
            if not rules:
                logger.warning(f"No rules found for category: {category}")
                return {
                    "category": category,
                    "score": 0.0,
                    "rule_count": 0,
                    "rule_results": []
                }
            
            # Execute rules
            rule_results = []
            for rule in rules:
                try:
                    rule_result = self._execute_rule(rule, page_data)
                    rule_results.append(rule_result)
                except Exception as e:
                    logger.debug(f"Error executing rule {rule.rule_id}: {e}")
                    # Add zero-score rule result to maintain consistency
                    rule_results.append({
                        "rule_id": rule.rule_id,
                        "category": rule.category,
                        "score": 0.0,
                        "weight": rule.weight,
                        "max_score": rule.max_score,
                        "error": str(e)
                    })
            
            # Calculate category score
            category_score = self.normalizer.calculate_category_score(rule_results, category)
            
            # No special category boosts - all categories treated equally
            
            # Assemble category result
            category_result = {
                "category": category,
                "score": category_score,
                "rule_count": len(rules),
                "rule_results": rule_results
            }
            
            # Validate category result
            if not self.validator.validate_category_breakdown(category_result):
                logger.error(f"Invalid category breakdown generated for {category}")
                raise ValueError("Generated category breakdown failed validation")
            
            return category_result
            
        except Exception as e:
            logger.error(f"Error scoring category {category}: {e}")
            raise
    
    def _execute_rule(self, rule, page_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single rule
        
        Args:
            rule: Rule instance to execute
            page_data: Page data to evaluate
            
        Returns:
            Rule result with score and metadata
        """
        try:
            # --- SAFE NORMALIZATION LAYER ---
            normalized = page_data.copy()
            
            # Fix JSON string issue
            structured = normalized.get("structured_data")
            if isinstance(structured, str):
                try:
                    normalized["structured_data"] = json.loads(structured)
                except:
                    normalized["structured_data"] = {}
            
            # Fix unified_entity_graph JSON string
            entity_graph = normalized.get("unified_entity_graph")
            if isinstance(entity_graph, str):
                try:
                    normalized["unified_entity_graph"] = json.loads(entity_graph)
                except:
                    normalized["unified_entity_graph"] = {}
            
            # Fix JSON string for faq_metrics
            faq_metrics = normalized.get("faq_metrics")
            if isinstance(faq_metrics, str):
                try:
                    normalized["faq_metrics"] = json.loads(faq_metrics)
                except:
                    normalized["faq_metrics"] = {}
            
            # Ensure key dict fields are dict (but don't overwrite existing ones)
            for key in [
                "content_metrics",
                "heading_metrics", 
                "entity_metrics",
                "intent_metrics",
                "faq_metrics",
                "step_metrics",
                "main_content"
            ]:
                if key not in normalized or not isinstance(normalized.get(key), dict):
                    normalized[key] = {}
            # ----------------------------------
            
            # Evaluate rule
            raw_score = rule.evaluate(normalized)
            
            # Normalize score
            normalized_score = rule.normalize(raw_score)
            
            # Round for deterministic output
            normalized_score = round(normalized_score, 3)
            
            return {
                "rule_id": rule.rule_id,
                "category": rule.category,
                "score": normalized_score,
                "weight": rule.weight,
                "max_score": rule.max_score,
                "raw_score": round(raw_score, 3)
            }
            
        except Exception as e:
            logger.debug(f"Error executing rule {rule.rule_id}: {e}")
            raise
    
    def score_website(self, pages_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Score multiple pages and calculate website-level metrics
        
        Args:
            pages_data: List of page extraction data
            
        Returns:
            Website-level scoring results
        """
        try:
            if not pages_data:
                return {
                    "website_ai_score": 0.0,
                    "pages_scored": 0,
                    "category_averages": {},
                    "scoring_version": "v2"
                }
            
            # Score each page
            page_scores = []
            category_totals = {}
            category_counts = {}
            
            for page_data in pages_data:
                try:
                    page_score = self.score_page(page_data)
                    page_scores.append(page_score)
                    
                    # Accumulate category scores
                    for category, score in page_score["category_scores"].items():
                        if category not in category_totals:
                            category_totals[category] = 0.0
                            category_counts[category] = 0
                        category_totals[category] += score
                        category_counts[category] += 1
                        
                except Exception as e:
                    logger.error(f"Error scoring page {page_data.get('url', 'unknown')}: {e}")
                    continue
            
            # Calculate website score
            page_score_values = [ps["page_ai_score"] for ps in page_scores]
            website_score = self.normalizer.calculate_website_score(page_score_values)
            
            # Calculate category averages
            category_averages = {}
            for category in category_totals:
                if category_counts[category] > 0:
                    average = category_totals[category] / category_counts[category]
                    category_averages[category] = round(average, 3)
            
            # Assemble website result
            website_result = {
                "website_ai_score": website_score,
                "pages_scored": len(page_scores),
                "category_averages": category_averages,
                "scoring_version": "v2"
            }
            
            logger.info(f"Website scoring completed: {len(page_scores)} pages scored")
            return website_result
            
        except Exception as e:
            logger.error(f"Error scoring website: {e}")
            raise
    
    def get_rule_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about loaded rules
        
        Returns:
            Rule statistics dictionary
        """
        validation_report = self.rule_registry.validate_registry()
        
        stats = {
            "total_rules": validation_report["total_rules"],
            "categories": {},
            "category_weights": self.category_weights,
            "scoring_version": "v2"
        }
        
        for category, category_report in validation_report["categories"].items():
            stats["categories"][category] = {
                "rule_count": category_report["rule_count"],
                "total_weight": category_report["total_weight"],
                "rules": category_report["rules"]
            }
        
        return stats
    
        
        
    def set_category_weights(self, weights: Dict[str, float]):
        """
        Update category weights
        
        Args:
            weights: New category weights (must sum to 100)
        """
        if not CategoryWeights.validate_weights(weights):
            raise ValueError("Invalid category weights: must sum to 100 and include all categories")
        
        self.category_weights = weights.copy()
        logger.info("Category weights updated successfully")
        logger.info("Category weights updated successfully")
