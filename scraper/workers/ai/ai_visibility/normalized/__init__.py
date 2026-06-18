"""
Normalized AI Visibility extraction architecture.

Single source of truth for page signals:

    raw_html  ->  RenderedDOM  ->  extract_normalized_signals()  ->  NormalizedSignals

The RenderedDOM is parsed ONCE and is immutable. Each metric has exactly one
extractor and one validator, producing one normalized output consumed by the
score engine, the issue engine and backend aggregations.

This package replaces the legacy multi-schema extraction logic in
ai_visibility.py / improved_extractions.py / full_extraction_coordinator.py.
Legacy fields are still emitted as compatibility aliases during migration.
"""

from .rendered_dom import RenderedDOM
from .signals import NormalizedSignals, SIGNAL_SECTIONS
from .normalizer import extract_normalized_signals

__all__ = [
    "RenderedDOM",
    "NormalizedSignals",
    "SIGNAL_SECTIONS",
    "extract_normalized_signals",
]
