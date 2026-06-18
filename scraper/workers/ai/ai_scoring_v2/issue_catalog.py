"""
Issue catalog — human, AI-visibility-focused copy for every rule.

Replaces "Rule XYZ scored 20" with real titles, descriptions and recommendations.
One central place to maintain issue intelligence instead of 55 method overrides.

Each entry:
    title         short human title (frontend card heading)
    description   what is wrong and why it matters for AI visibility
    recommendation concrete fix
    expected      expected_value shown to the user
    detector      optional dotted path into legacy aliases for detected_value
                  (e.g. "heading_metrics.h1_count")

Rules absent from the catalog fall back to a humanized form of the rule's own
description (never "Rule X scored Y").
"""

from __future__ import annotations

from typing import Any, Dict, Optional

ISSUE_CATALOG: Dict[str, Dict[str, str]] = {
    # ── ai_impact ──────────────────────────────────────────────────────────
    "primary_organization_schema": {
        "title": "Missing Organization schema",
        "description": "No primary Organization schema was found. AI engines rely on Organization markup to identify and attribute your brand as an entity.",
        "recommendation": "Add a JSON-LD Organization schema with name, url, logo and sameAs to the homepage and reference it site-wide.",
        "expected": "Organization JSON-LD present",
    },
    "schema_valid_jsonld": {
        "title": "Invalid JSON-LD structured data",
        "description": "Structured data on this page failed to parse as valid JSON-LD, so AI crawlers will ignore it.",
        "recommendation": "Validate every JSON-LD block (no trailing commas, correct quoting) using the Rich Results test and fix parse errors.",
        "expected": "All JSON-LD parses without errors",
    },
    "correct_type": {
        "title": "Schema @type is missing or incorrect",
        "description": "Entities lack a correct schema.org @type, so AI engines cannot classify the content.",
        "recommendation": "Set a precise @type (Article, Product, Service, LocalBusiness, etc.) on each schema entity.",
        "expected": "Valid schema.org @type on each entity",
    },
    "xml_sitemap_exists_valid": {
        "title": "XML sitemap missing or invalid",
        "description": "A valid XML sitemap helps AI crawlers discover and prioritize your pages.",
        "recommendation": "Publish a valid /sitemap.xml and reference it in robots.txt.",
        "expected": "Valid XML sitemap",
    },
    "robots_txt_non_blocking": {
        "title": "robots.txt blocks crawlers",
        "description": "robots.txt is blocking crawlers (potentially AI crawlers), preventing your content from being indexed by answer engines.",
        "recommendation": "Allow GPTBot, PerplexityBot, Google-Extended and other AI crawlers in robots.txt unless intentionally excluded.",
        "expected": "AI crawlers allowed",
    },
    "no_plugin_duplicate_schemas": {
        "title": "Duplicate schema entities detected",
        "description": "Multiple conflicting copies of a singleton schema (e.g. two Organizations) were found, usually from plugins. This confuses entity resolution.",
        "recommendation": "Consolidate to a single Organization/WebSite/WebPage entity and remove plugin-generated duplicates.",
        "expected": "One canonical entity per singleton type",
    },
    "semantic_html_tags_used": {
        "title": "Weak semantic HTML structure",
        "description": "The page makes little use of semantic landmarks (main, article, section, nav), making content harder for AI to segment.",
        "recommendation": "Wrap primary content in <main>/<article> and use <section>, <nav>, <header>, <footer> appropriately.",
        "expected": "Semantic landmarks present",
    },
    # ── citation_probability ────────────────────────────────────────────────
    "no_entity_fragmentation": {
        "title": "Entity fragmentation detected",
        "description": "The same entity is represented multiple inconsistent ways, splitting authority signals.",
        "recommendation": "Use a single @id for the entity and reference it consistently across schemas.",
        "expected": "Single consistent entity",
    },
    "visible_author_name": {
        "title": "No visible author",
        "description": "No visible author is attributed on this page. Author attribution helps AI engines identify and cite the content source.",
        "recommendation": "Add a visible author byline and matching Person schema.",
        "expected": "Visible author byline",
    },
    "person_schema_linked_to_organization": {
        "title": "Author not linked to organization",
        "description": "The author Person schema isn't linked to the Organization, weakening entity attribution for AI knowledge graphs.",
        "recommendation": "Link the Person schema to the Organization via worksFor/affiliation.",
        "expected": "Person linked to Organization",
    },
    "dedicated_author_page": {
        "title": "No dedicated author page",
        "description": "There is no dedicated author page to consolidate authorship signals for AI entity understanding.",
        "recommendation": "Create an author profile page and link bylines to it.",
        "expected": "Author page present",
    },
    # ── llm_readiness ─────────────────────────────────────────────────────────
    "service_pages_800_words": {
        "title": "Thin service page content",
        "description": "This service page has too little content for AI engines to extract a comprehensive answer.",
        "recommendation": "Expand the service page to 800+ words covering benefits, process, FAQs and proof.",
        "expected": "800+ words",
        "detector": "content_metrics.word_count",
    },
    "topic_clusters_internal_links": {
        "title": "Weak topical clustering",
        "description": "The page does not link out to enough related sub-topics, so AI engines see it as isolated rather than part of a topic cluster.",
        "recommendation": "Add contextual internal links to 3+ related pages using descriptive anchors.",
        "expected": "3+ contextual cluster links",
    },
    "clear_entity_first_150_words": {
        "title": "Primary entity unclear in intro",
        "description": "The primary entity isn't clearly stated in the first 150 words, making it harder for AI to grasp the subject.",
        "recommendation": "Name the primary entity explicitly in the opening paragraph.",
        "expected": "Entity in first 150 words",
    },
    "sameas_array_links_active": {
        "title": "Missing sameAs links",
        "description": "No active sameAs links connect your entity to authoritative profiles (Wikipedia, social, Crunchbase).",
        "recommendation": "Add a sameAs array with verified profile URLs to Organization/Person schema.",
        "expected": "Active sameAs links",
    },
    "description_minimum_50_characters": {
        "title": "Schema description too short",
        "description": "The schema description is under 50 characters, giving AI engines little to work with.",
        "recommendation": "Write a 50–160 character schema description summarizing the entity.",
        "expected": "50+ character description",
    },
    "last_updated_date_visible": {
        "title": "No visible freshness date",
        "description": "No published/updated date is exposed, so AI engines can't judge content freshness.",
        "recommendation": "Show a visible last-updated date and add dateModified to schema.",
        "expected": "Visible date + dateModified",
    },
    "semantic_subtopics_covered": {
        "title": "Subtopics under-covered",
        "description": "The page covers few subtopics, limiting topical depth that AI engines reward.",
        "recommendation": "Add H2/H3 sections covering related subtopics and questions.",
        "expected": "Multiple subtopic sections",
    },
    "statistics_have_source_links": {
        "title": "Statistics lack sources",
        "description": "Statistics are presented without source attribution, lowering trust for AI citation.",
        "recommendation": "Cite authoritative sources for statistics with outbound links.",
        "expected": "Sourced statistics",
    },
    # ── aeo_score ───────────────────────────────────────────────────────────
    "first_60_words_direct_answer": {
        "title": "No direct answer up front",
        "description": "The opening doesn't provide a concise direct answer, hurting featured-snippet and AI-answer eligibility.",
        "recommendation": "Lead with a 40–60 word direct answer to the page's core question.",
        "expected": "Direct answer in first 60 words",
    },
    "faq_schema_matches_content": {
        "title": "FAQ schema/content mismatch",
        "description": "FAQ schema does not match visible Q&A (or is absent), so answer engines can't trust the FAQ.",
        "recommendation": "Ensure FAQPage schema exactly mirrors visible questions and answers.",
        "expected": "FAQ schema matches content",
    },
    "faq_section_5_to_10_questions": {
        "title": "FAQ section missing or sparse",
        "description": "A focused FAQ (5–10 questions) is a strong answer-engine signal. This page lacks one.",
        "recommendation": "Add a 5–10 question FAQ targeting real user questions, with FAQPage schema.",
        "expected": "5–10 question FAQ",
        "detector": "faq_metrics.question_count",
    },
    "question_based_h2_headings": {
        "title": "Few question-based headings",
        "description": "Question-style headings help AI engines match your content to user queries.",
        "recommendation": "Phrase key H2 headings as the questions users actually ask.",
        "expected": "Question-based headings",
    },
    "direct_answer_format": {
        "title": "Answer format not optimized",
        "description": "Content isn't formatted for direct answers (concise paragraphs, clear sentences).",
        "recommendation": "Use concise paragraphs and clear sentences that answer questions directly.",
        "expected": "Direct-answer formatting",
    },
    "content_cites_sources": {
        "title": "Content lacks source citations",
        "description": "The content does not cite sources, reducing trustworthiness for AI answers.",
        "recommendation": "Cite reputable sources and link to them from the content.",
        "expected": "Cited sources",
    },
    # ── topical_authority ─────────────────────────────────────────────────────
    "only_one_primary_entity": {
        "title": "Multiple competing primary entities",
        "description": "More than one primary entity competes for focus, diluting topical authority.",
        "recommendation": "Designate a single primary entity per page and subordinate the rest.",
        "expected": "One primary entity",
    },
    "consistent_id_across_pages": {
        "title": "Inconsistent entity @id across pages",
        "description": "The entity @id varies across pages, preventing AI engines from unifying your entity.",
        "recommendation": "Use a stable, absolute @id for shared entities on every page.",
        "expected": "Consistent @id",
    },
    "child_schemas_reference_main_id": {
        "title": "Child schemas don't reference main entity",
        "description": "Child schemas don't reference the main entity @id, breaking the entity graph.",
        "recommendation": "Reference the main entity @id from child schemas (about/isPartOf/publisher).",
        "expected": "Children reference main @id",
    },
    "event_schema": {
        "title": "No Event schema",
        "description": "Event schema enables rich event answers. Optional unless the page covers events.",
        "recommendation": "Add Event schema with dates, location and offers where relevant.",
        "expected": "Event schema present",
    },
    # ── voice_intent ────────────────────────────────────────────────────────
    "bullet_numbered_lists_used": {
        "title": "Few lists for scannability",
        "description": "Bulleted/numbered lists improve voice and AI answer extraction. This page uses few.",
        "recommendation": "Convert suitable content into bulleted or numbered lists.",
        "expected": "Lists present",
    },
    "comparison_tables_present": {
        "title": "No comparison tables",
        "description": "Comparison tables are highly extractable by AI engines. None were found.",
        "recommendation": "Add comparison tables where you compare options, plans or features.",
        "expected": "Comparison table present",
    },
    "step_by_step_content": {
        "title": "No step-by-step content",
        "description": "Step-by-step instructions are strong answer-engine signals for how-to queries.",
        "recommendation": "Add clearly numbered step-by-step instructions where relevant, with HowTo schema.",
        "expected": "Step-by-step content",
    },
}


def _humanize(rule_id: str, fallback_desc: str) -> str:
    if fallback_desc:
        return fallback_desc
    return rule_id.replace("_", " ").strip().capitalize()


def get_catalog_entry(rule_id: str, rule_description: str = "") -> Dict[str, Any]:
    """Return rich issue copy for a rule, with a safe humanized fallback."""
    entry = ISSUE_CATALOG.get(rule_id)
    if entry:
        return dict(entry)
    human = _humanize(rule_id, rule_description)
    return {
        "title": human,
        "description": f"{human} needs attention to improve this page's AI visibility.",
        "recommendation": "Review and improve this aspect of the page for AI search engines.",
        "expected": "",
    }


def get_detected_value(rule_id: str, legacy_aliases: Optional[Dict[str, Any]]) -> Any:
    """Resolve a rule's detected_value from legacy aliases via its detector path."""
    entry = ISSUE_CATALOG.get(rule_id) or {}
    path = entry.get("detector")
    if not path or not legacy_aliases:
        return None
    node: Any = legacy_aliases
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return None
    return node
