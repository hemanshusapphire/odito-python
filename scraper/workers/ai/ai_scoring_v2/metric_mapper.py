"""
Metric Mapper — Derives 12 dashboard metrics from 6 category scores.

Pure mathematical transformation layer. No new scoring categories.
No database access. No rule evaluation. Fully deterministic.

Usage:
    from metric_mapper import derive_dashboard_metrics
    
    dashboard = derive_dashboard_metrics(category_scores)
    # Returns dict with 12 keys, each 0-100
"""

from typing import Dict

# Each metric is a weighted blend of category scores.
# Weights within each formula sum to 1.0.
METRIC_FORMULAS: Dict[str, Dict[str, float]] = {
    "ai_readiness": {
        "llm_readiness": 0.40,
        "ai_impact": 0.35,
        "topical_authority": 0.25,
    },
    "schema_coverage": {
        "ai_impact": 0.70,
        "topical_authority": 0.30,
    },
    "faq_optimization": {
        "aeo_score": 0.80,
        "voice_intent": 0.20,
    },
    "conversational_score": {
        "voice_intent": 0.60,
        "aeo_score": 0.40,
    },
    "ai_snippet_probability": {
        "aeo_score": 0.40,
        "ai_impact": 0.35,
        "llm_readiness": 0.25,
    },
    "ai_citation_rate": {
        "citation_probability": 0.75,
        "topical_authority": 0.25,
    },
    "knowledge_graph": {
        "topical_authority": 0.60,
        "ai_impact": 0.40,
    },
    "entity_coverage": {
        "topical_authority": 0.65,
        "citation_probability": 0.35,
    },
    "llm_indexability": {
        "llm_readiness": 0.70,
        "ai_impact": 0.30,
    },
    "structured_data_depth": {
        "ai_impact": 0.75,
        "llm_readiness": 0.25,
    },
    "entity_coverage_pct": {
        "topical_authority": 1.00,
    },
    "geo_score": {
        "ai_impact": 0.20,
        "citation_probability": 0.20,
        "llm_readiness": 0.15,
        "aeo_score": 0.20,
        "topical_authority": 0.15,
        "voice_intent": 0.10,
    },
}

# Human-readable labels for frontend display
METRIC_LABELS: Dict[str, str] = {
    "ai_readiness": "AI Readiness",
    "schema_coverage": "Schema Coverage",
    "faq_optimization": "FAQ Optimization",
    "conversational_score": "Conversational Score",
    "ai_snippet_probability": "AI Snippet Probability",
    "ai_citation_rate": "AI Citation Rate",
    "knowledge_graph": "Knowledge Graph",
    "entity_coverage": "Entity Coverage",
    "llm_indexability": "LLM Indexability",
    "structured_data_depth": "Structured Data Depth",
    "entity_coverage_pct": "Entity Coverage %",
    "geo_score": "GEO Score",
}


def derive_dashboard_metrics(category_scores: Dict[str, float]) -> Dict[str, float]:
    """
    Compute all 12 dashboard metrics from 6 category scores.
    
    Args:
        category_scores: Dict with keys like 'ai_impact', 'citation_probability', etc.
                         Each value should be 0-100.
    
    Returns:
        Dict with 12 dashboard metric keys, each value clamped to 0-100 and rounded to 2 decimal places.
    """
    if not category_scores:
        return {key: 0.0 for key in METRIC_FORMULAS}
    
    metrics = {}
    for metric_key, formula in METRIC_FORMULAS.items():
        score = sum(
            category_scores.get(cat, 0.0) * weight
            for cat, weight in formula.items()
        )
        # Clamp to 0-100 and round for deterministic output
        metrics[metric_key] = round(max(0.0, min(score, 100.0)), 2)
    
    return metrics


def get_metric_labels() -> Dict[str, str]:
    """Return human-readable labels for all dashboard metrics."""
    return METRIC_LABELS.copy()
