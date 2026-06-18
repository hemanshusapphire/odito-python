"""
Metric Mapper — Derives 15 dashboard metrics from 6 category scores.

Pure mathematical transformation layer. No new scoring categories.
No database access. No rule evaluation. Fully deterministic.

Keys 1-11 are legacy keys (preserved for backward compatibility).
Keys 12-15 are hub-native aliases used by the AEO Hub frontend.
Both sets coexist; no existing consumer breaks.

Usage:
    from metric_mapper import derive_dashboard_metrics

    dashboard = derive_dashboard_metrics(category_scores)
    # Returns dict with 15 keys, each 0-100
"""

from typing import Dict

# Each metric is a weighted blend of category scores.
# Weights within each formula sum to 1.0.
METRIC_FORMULAS: Dict[str, Dict[str, float]] = {
    "ai_readiness": {
        "llm_readiness":    0.35,
        "ai_impact":        0.30,
        "topical_authority": 0.20,
        "ai_accessibility": 0.15,
    },
    "schema_coverage": {
        "ai_impact":        0.70,
        "topical_authority": 0.30,
    },
    "faq_optimization": {
        "aeo_score":   0.80,
        "voice_intent": 0.20,
    },
    "conversational_score": {
        "voice_intent": 0.60,
        "aeo_score":    0.40,
    },
    "ai_snippet_probability": {
        "aeo_score":    0.40,
        "ai_impact":    0.35,
        "llm_readiness": 0.25,
    },
    "ai_citation_rate": {
        "citation_probability": 0.75,
        "topical_authority":    0.25,
    },
    "knowledge_graph": {
        "topical_authority": 0.60,
        "ai_impact":         0.40,
    },
    "entity_coverage": {
        "topical_authority":    0.65,
        "citation_probability": 0.35,
    },
    "llm_indexability": {
        "llm_readiness":    0.65,
        "ai_impact":        0.25,
        "ai_accessibility": 0.10,
    },
    "structured_data_depth": {
        "ai_impact":    0.75,
        "llm_readiness": 0.25,
    },
    "geo_score": {
        "ai_impact":          0.19,
        "citation_probability": 0.13,
        "llm_readiness":      0.17,
        "aeo_score":          0.19,
        "topical_authority":  0.13,
        "voice_intent":       0.10,
        "ai_accessibility":   0.09,
    },

    # ── Hub-native aliases (AEO Hub scope) ──────────────────────────────
    # Same formulas as their legacy counterparts; separate keys so the
    # AEO Hub frontend can request them by semantic name without coupling
    # to legacy metric key names.  Legacy keys are preserved untouched.
    "answer_readiness": {       # AEO alias for faq_optimization
        "aeo_score":    0.80,
        "voice_intent": 0.20,
    },
    "snippet_opportunities": {  # AEO alias for ai_snippet_probability
        "aeo_score":     0.40,
        "ai_impact":     0.35,
        "llm_readiness": 0.25,
    },
    "question_coverage": {      # AEO alias for conversational_score
        "voice_intent": 0.60,
        "aeo_score":    0.40,
    },
    "structure_score": {        # AEO alias for structured_data_depth
        "ai_impact":     0.75,
        "llm_readiness": 0.25,
    },
}

# Human-readable labels for frontend display
METRIC_LABELS: Dict[str, str] = {
    # Legacy keys
    "ai_readiness":          "AI Readiness",
    "schema_coverage":       "Schema Coverage",
    "faq_optimization":      "FAQ Optimization",
    "conversational_score":  "Conversational Score",
    "ai_snippet_probability": "AI Snippet Probability",
    "ai_citation_rate":      "AI Citation Rate",
    "knowledge_graph":       "Knowledge Graph",
    "entity_coverage":       "Entity Coverage",
    "llm_indexability":      "LLM Indexability",
    "structured_data_depth": "Structured Data Depth",
    "geo_score":             "GEO Score",
    # Hub-native aliases (AEO Hub)
    "answer_readiness":      "Answer Readiness",
    "snippet_opportunities": "Snippet Opportunities",
    "question_coverage":     "Question Coverage",
    "structure_score":       "Structure Score",
}


def derive_dashboard_metrics(category_scores: Dict[str, float]) -> Dict[str, float]:
    """
    Compute all 15 dashboard metrics from 6 category scores.

    Args:
        category_scores: Dict with keys like 'ai_impact', 'citation_probability', etc.
                         Each value should be 0-100.

    Returns:
        Dict with 15 dashboard metric keys (11 legacy + 4 hub-native AEO aliases),
        each value clamped to 0-100 and rounded to 2 decimal places.
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
