"""
NormalizedSignals — the single source of truth for page-level AI visibility data.

Every metric is produced by exactly one extractor and stored in exactly one place
here. The score engine, issue engine and backend aggregations consume ONLY this
object (via to_dict()). Legacy flat keys are emitted via to_legacy_aliases() so the
existing scoring rules, DB schema and dashboard aggregations keep working unchanged
during migration.

ROOT PROBLEMS THIS FIXES
------------------------
* Multiple truth sources: one canonical key per metric, defined here.
* Key-name mismatches (faq_detected vs faq_section_detected, entity_per_1000_words
  vs entity_density_per_1000_words): the canonical names live here and the alias
  layer translates once.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

SIGNAL_SECTIONS = (
    "navigation", "headings", "paragraphs", "content", "faq", "entities",
    "schema", "accessibility", "links", "media", "technical", "page_type",
)


# ── per-metric sections ─────────────────────────────────────────────────────
@dataclass
class NavigationSignals:
    has_navigation: bool = False
    has_header: bool = False
    has_footer: bool = False
    nav_link_count: int = 0
    footer_link_count: int = 0
    primary_nav_items: int = 0
    detection_method: str = ""          # "semantic" | "aria" | "class_heuristic" | ""
    evidence: List[str] = field(default_factory=list)


@dataclass
class HeadingSignals:
    h1_count: int = 0
    h2_count: int = 0
    h3_count: int = 0
    h4_count: int = 0
    h5_count: int = 0
    h6_count: int = 0
    hierarchy_valid: bool = True
    sequence_violations: List[str] = field(default_factory=list)
    question_headings: int = 0
    multiple_h1: bool = False


@dataclass
class ParagraphSignals:
    count: int = 0
    avg_length: float = 0.0             # words per paragraph
    short_paragraph_ratio: float = 0.0
    long_paragraph_ratio: float = 0.0


@dataclass
class ContentSignals:
    word_count: int = 0
    char_count: int = 0
    readability_score: float = 0.0      # Flesch reading ease
    avg_sentence_length: float = 0.0
    content_to_html_ratio: float = 0.0
    content_depth_score: float = 0.0
    substantial_sections: int = 0
    thin_content: bool = False
    extraction_method: str = ""


@dataclass
class FaqSignals:
    detected: bool = False              # FAQ present (schema OR genuine visible Q&A)
    schema_detected: bool = False
    question_count: int = 0
    qa_pairs_detected: int = 0
    answered_count: int = 0
    source: str = "none"                # "schema" | "visible" | "both" | "none"


@dataclass
class EntitySignals:
    primary_entity: str = ""
    primary_entity_type: str = ""
    primary_entity_mentions: int = 0
    entity_count: int = 0
    entity_per_1000_words: float = 0.0
    unique_entity_types: int = 0
    schema_vs_html_consistent: bool = True
    name_variations: List[str] = field(default_factory=list)
    named_entities: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class SchemaSignals:
    present: bool = False
    json_ld_count: int = 0
    types: List[str] = field(default_factory=list)
    duplicate_types: List[str] = field(default_factory=list)
    valid_json: bool = True
    parse_errors: int = 0
    faq_schema: bool = False
    breadcrumb_present: bool = False
    breadcrumb_levels: int = 0
    organization_present: bool = False
    sameas_count: int = 0
    graph: List[Dict[str, Any]] = field(default_factory=list)  # parsed @graph entities


@dataclass
class AccessibilitySignals:
    images_total: int = 0
    images_with_alt: int = 0
    alt_text_coverage: float = 0.0
    aria_label_count: int = 0
    landmark_roles: List[str] = field(default_factory=list)
    form_inputs: int = 0
    form_inputs_labeled: int = 0
    lang_declared: bool = False
    skip_link: bool = False


@dataclass
class LinkSignals:
    total: int = 0
    internal: int = 0
    external: int = 0
    internal_descriptive_anchor: int = 0
    contextual_internal: int = 0       # links inside main content (not chrome)
    topic_cluster_links: int = 0       # contextual internal links with topical anchors
    nofollow: int = 0
    has_topic_clusters: bool = False   # semantic: real clustering, not "links exist"


@dataclass
class MediaSignals:
    images_total: int = 0
    images_lazy: int = 0
    images_modern_format: int = 0      # webp/avif
    images_with_dimensions: int = 0
    videos: int = 0
    video_with_schema: int = 0
    lazy_loading_genuine: bool = False  # semantic: meaningful lazy loading, not 1 pixel


@dataclass
class TechnicalSignals:
    canonical_present: bool = False
    canonical_self_referencing: bool = False
    canonical_url: str = ""
    robots_directive: str = ""
    indexable: bool = True
    followable: bool = True
    viewport_present: bool = False
    hreflang_present: bool = False
    https: bool = False
    charset: str = ""


@dataclass
class PageTypeSignals:
    detected_type: str = "Unknown"
    confidence: float = 0.0
    is_homepage: bool = False


# ── top-level container ──────────────────────────────────────────────────────
@dataclass
class NormalizedSignals:
    url: str = ""
    http_status_code: int = 200
    response_time_ms: int = 0
    html_size_bytes: int = 0
    extraction_version: str = "3.0"
    extraction_timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    extraction_warnings: List[str] = field(default_factory=list)

    navigation: NavigationSignals = field(default_factory=NavigationSignals)
    headings: HeadingSignals = field(default_factory=HeadingSignals)
    paragraphs: ParagraphSignals = field(default_factory=ParagraphSignals)
    content: ContentSignals = field(default_factory=ContentSignals)
    faq: FaqSignals = field(default_factory=FaqSignals)
    entities: EntitySignals = field(default_factory=EntitySignals)
    schema: SchemaSignals = field(default_factory=SchemaSignals)
    accessibility: AccessibilitySignals = field(default_factory=AccessibilitySignals)
    links: LinkSignals = field(default_factory=LinkSignals)
    media: MediaSignals = field(default_factory=MediaSignals)
    technical: TechnicalSignals = field(default_factory=TechnicalSignals)
    page_type: PageTypeSignals = field(default_factory=PageTypeSignals)

    # ── canonical serialization ─────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        """Canonical nested representation (the single source of truth)."""
        return {
            "url": self.url,
            "http_status_code": self.http_status_code,
            "response_time_ms": self.response_time_ms,
            "html_size_bytes": self.html_size_bytes,
            "extraction_version": self.extraction_version,
            "extraction_timestamp": self.extraction_timestamp,
            "extraction_warnings": list(self.extraction_warnings),
            "normalized_signals": {
                "navigation": asdict(self.navigation),
                "headings": asdict(self.headings),
                "paragraphs": asdict(self.paragraphs),
                "content": asdict(self.content),
                "faq": asdict(self.faq),
                "entities": asdict(self.entities),
                "schema": {k: v for k, v in asdict(self.schema).items() if k != "graph"},
                "accessibility": asdict(self.accessibility),
                "links": asdict(self.links),
                "media": asdict(self.media),
                "technical": asdict(self.technical),
                "page_type": asdict(self.page_type),
            },
        }

    # ── backward-compat aliases ──────────────────────────────────────────────
    def to_legacy_aliases(self) -> Dict[str, Any]:
        """Flat legacy keys consumed by the existing scoring rules, the
        seo_ai_visibility DB schema and the dashboard aggregations.

        Keeping these means the score engine and backend keep working unchanged
        while we migrate consumers to the canonical schema.
        """
        h = self.headings
        c = self.content
        p = self.paragraphs
        e = self.entities
        f = self.faq
        s = self.schema
        lk = self.links
        m = self.media
        t = self.technical
        nav = self.navigation

        def _has_type(entity: Any, type_name: str) -> bool:
            raw = entity.get("@type", "")
            return type_name in (raw if isinstance(raw, list) else [raw])

        return {
            # ── existing keys (unchanged — scoring rules depend on these) ──────
            "content_metrics": {
                "word_count": c.word_count,
                "paragraph_count": p.count,
                "avg_sentence_length": c.avg_sentence_length,
                "readability_score": c.readability_score,
                "short_paragraph_ratio": p.short_paragraph_ratio,
                "long_paragraph_ratio": p.long_paragraph_ratio,
                "avg_paragraph_length": p.avg_length,
                "content_depth_score": c.content_depth_score,
            },
            "heading_metrics": {
                "h1_count": h.h1_count,
                "h2_count": h.h2_count,
                "h3_count": h.h3_count,
                "h4_count": h.h4_count,
                "h5_count": h.h5_count,
                "h6_count": h.h6_count,
                "question_headings": h.question_headings,
                "heading_sequence_valid": h.hierarchy_valid,
                "heading_structure_score_input": _heading_structure_score(h),
            },
            "faq_metrics": {
                "faq_detected": f.detected,
                "faq_schema_detected": f.schema_detected,
                "question_count": f.question_count,
                "qa_pairs_detected": f.qa_pairs_detected,
            },
            "entity_metrics": {
                "entity_count": e.entity_count,
                "entity_per_1000_words": e.entity_per_1000_words,
                "unique_entity_types": e.unique_entity_types,
                "primary_entity_mentions_in_text": e.primary_entity_mentions,
            },
            "unified_entity_graph": {
                "primary_entity": e.primary_entity,
                "primary_entity_mentions_in_text": e.primary_entity_mentions,
                # FIX: was always {} so len() always returned 0, breaking
                # no_entity_fragmentation and only_one_primary_entity thresholds.
                # list(range(n)) gives len() == entity_count as rules expect.
                "entities": list(range(e.entity_count)),
                "entity_count": e.entity_count,
            },
            "structured_data": {"@graph": s.graph},
            "page_type_properties": {
                "detected_type": self.page_type.detected_type,
                "confidence": self.page_type.confidence,
            },
            # FIX: was hardcoded {"step_section_present": False} — steps_detected
            # and howto_schema_present were always missing, capping
            # step_by_step_content at 2/10.
            "step_metrics": {
                "step_section_present": c.substantial_sections > 0,
                "steps_detected": c.substantial_sections,
                "howto_detected": False,
                "howto_schema_present": any(_has_type(item, "HowTo") for item in s.graph),
            },
            # ── new keys exposed to scoring rules ─────────────────────────────
            # Pre-computed schema signals — rules no longer need to re-parse @graph
            # for information the extractor already computed.
            "schema_signals": {
                "valid_json": s.valid_json,
                "parse_errors": s.parse_errors,
                "duplicate_types": s.duplicate_types,
                "organization_present": s.organization_present,
                "sameas_count": s.sameas_count,
                "breadcrumb_present": s.breadcrumb_present,
                "breadcrumb_levels": s.breadcrumb_levels,
                "faq_schema": s.faq_schema,
                "all_types": s.types,
            },
            # Technical signals — enable robots/canonical/https checks
            "technical_signals": {
                "canonical_present": t.canonical_present,
                "canonical_self_referencing": t.canonical_self_referencing,
                "https": t.https,
                "indexable": t.indexable,
                "followable": t.followable,
                "robots_directive": t.robots_directive,
            },
            # Link signals — enable real internal-link and citation checks
            "link_signals": {
                "total": lk.total,
                "internal": lk.internal,
                "external": lk.external,
                "contextual_internal": lk.contextual_internal,
                "topic_cluster_links": lk.topic_cluster_links,
                "has_topic_clusters": lk.has_topic_clusters,
            },
            # Media signals — enable real image-format and lazy-loading checks
            "media_signals": {
                "images_total": m.images_total,
                "images_modern_format": m.images_modern_format,
                "images_lazy": m.images_lazy,
                "lazy_loading_genuine": m.lazy_loading_genuine,
            },
            # Navigation signals — enable trust-page and NAP footer detection
            "navigation_signals": {
                "has_footer": nav.has_footer,
                "footer_link_count": nav.footer_link_count,
                "has_navigation": nav.has_navigation,
                "has_header": nav.has_header,
            },
            # ── flat top-level fields (DB schema / older consumers) ────────────
            "word_count": c.word_count,
            "paragraph_count": p.count,
            "has_navigation": nav.has_navigation,
            "has_header": nav.has_header,
            "has_footer": nav.has_footer,
            "lazy_loading_detected": m.lazy_loading_genuine,
            "primary_entity_mentions_in_text": e.primary_entity_mentions,
            "response_time_ms": self.response_time_ms,
        }


def _heading_structure_score(h: HeadingSignals) -> float:
    """Cheap 0-100 proxy for heading structure quality (legacy quality_flags input)."""
    score = 0.0
    if h.h1_count == 1:
        score += 50
    elif h.h1_count >= 1:
        score += 20
    if h.h2_count >= 1:
        score += 30
    if h.h3_count >= 1:
        score += 10
    if h.hierarchy_valid:
        score += 10
    return min(score, 100.0)


def validate_signals(signals: NormalizedSignals) -> List[str]:
    """Strict-ish type/range validation. Returns a list of warning strings
    (empty = clean). Never raises — extraction must be fault tolerant."""
    warnings: List[str] = []

    def _check(cond: bool, msg: str):
        if not cond:
            warnings.append(msg)

    _check(isinstance(signals.content.word_count, int) and signals.content.word_count >= 0,
           "content.word_count invalid")
    _check(0.0 <= signals.accessibility.alt_text_coverage <= 100.0,
           "accessibility.alt_text_coverage out of range")
    _check(signals.links.internal <= signals.links.total,
           "links.internal exceeds links.total")
    _check(signals.faq.qa_pairs_detected <= max(signals.faq.question_count, signals.faq.qa_pairs_detected),
           "faq pair/question mismatch")
    _check(signals.headings.h1_count >= 0, "headings.h1_count invalid")
    return warnings
