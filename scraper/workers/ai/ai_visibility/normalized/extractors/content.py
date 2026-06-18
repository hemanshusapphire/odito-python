"""
Content, FAQ and entity extractors.

CONTENT DEPTH (fixes "content_depth_score=0 on large pages" and
"sections_with_substantial_content=0")
---------------------------------------------------------------
Legacy depth came only from json_ld_data['content_sections'] (definition/use_case/
step sections). A 2,000-word page with normal <section>/<h2> blocks scored 0.
Here depth is computed from the ACTUAL rendered content: word count, number of
substantial sections (delimited by <section>/<article> or h2 blocks with >50 words),
and heading diversity.

FAQ (fixes FAQ false positives/negatives)
-----------------------------------------
Not "the word FAQ appears". We require either valid FAQPage schema with Q&A, or
genuine visible Q&A structure (question headings followed by answer text,
<details>/<summary>, or <dl><dt>/<dd>). detected requires >=2 real pairs OR schema.

ENTITY (fixes entity key mismatch + adds real consistency check)
----------------------------------------------------------------
Canonical key entity_per_1000_words; primary-entity mentions counted against the
visible content text; schema-vs-HTML consistency actually verified.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from ..rendered_dom import RenderedDOM
from ..signals import ContentSignals, FaqSignals, EntitySignals

_SENTENCE_RE = re.compile(r"[.!?]+")
_WORD_RE = re.compile(r"\b\w+\b")
_VOWEL_GROUP_RE = re.compile(r"[aeiouy]+", re.IGNORECASE)
_QUESTION_WORDS = ("what", "how", "why", "when", "where", "which", "who", "can", "do", "does", "is", "are")


def _count_syllables(word: str) -> int:
    word = word.lower().strip()
    if not word:
        return 0
    groups = _VOWEL_GROUP_RE.findall(word)
    count = len(groups)
    if word.endswith("e") and count > 1:
        count -= 1
    return max(count, 1)


def _flesch_reading_ease(text: str) -> float:
    words = _WORD_RE.findall(text)
    sentences = [s for s in _SENTENCE_RE.split(text) if s.strip()]
    if not words or not sentences:
        return 0.0
    syllables = sum(_count_syllables(w) for w in words)
    wps = len(words) / len(sentences)
    spw = syllables / len(words)
    score = 206.835 - (1.015 * wps) - (84.6 * spw)
    return round(max(0.0, min(score, 100.0)), 2)


def extract_content(dom: RenderedDOM) -> ContentSignals:
    content_text = dom.content_text()
    words = _WORD_RE.findall(content_text)
    word_count = len(words)
    sentences = [s for s in _SENTENCE_RE.split(content_text) if s.strip()]
    avg_sentence_length = round(word_count / len(sentences), 2) if sentences else 0.0

    # Substantial sections: <section>/<article> or h2-delimited blocks with >50 words.
    substantial = 0
    content_soup = dom.content_soup()
    explicit_sections = content_soup.find_all(["section", "article"])
    if explicit_sections:
        for sec in explicit_sections:
            if len(sec.get_text(strip=True).split()) > 50:
                substantial += 1
    else:
        # Fall back to h2-delimited blocks.
        for h2 in content_soup.find_all("h2"):
            block_words = 0
            for sib in h2.next_siblings:
                name = getattr(sib, "name", None)
                if name in ("h1", "h2"):
                    break
                if name:
                    block_words += len(sib.get_text(strip=True).split())
            if block_words > 50:
                substantial += 1

    # Content depth 0-100: blends length, section richness, heading diversity.
    length_score = min(word_count / 1200.0, 1.0) * 60.0           # up to 60 pts for ~1200+ words
    section_score = min(substantial / 4.0, 1.0) * 25.0            # up to 25 pts for 4+ sections
    heading_diversity = len({h.name for h in content_soup.find_all(["h2", "h3", "h4"])})
    diversity_score = min(heading_diversity / 3.0, 1.0) * 15.0    # up to 15 pts
    depth = round(length_score + section_score + diversity_score, 2)

    html_len = max(len(dom.html), 1)
    return ContentSignals(
        word_count=word_count,
        char_count=len(content_text),
        readability_score=_flesch_reading_ease(content_text),
        avg_sentence_length=avg_sentence_length,
        content_to_html_ratio=round(len(content_text) / html_len, 4),
        content_depth_score=depth,
        substantial_sections=substantial,
        thin_content=word_count < 300,
        extraction_method="normalized_content_clone",
    )


def _type_of(entity: Dict[str, Any]) -> List[str]:
    t = entity.get("@type")
    if isinstance(t, list):
        return [str(x) for x in t]
    if isinstance(t, str):
        return [t]
    return []


def extract_faq(dom: RenderedDOM, graph: List[Dict[str, Any]]) -> FaqSignals:
    # 1. Schema FAQ — must have actual questions.
    schema_questions = 0
    schema_answered = 0
    for ent in graph:
        if "FAQPage" in _type_of(ent):
            main = ent.get("mainEntity", [])
            if isinstance(main, dict):
                main = [main]
            if isinstance(main, list):
                for q in main:
                    if isinstance(q, dict) and "Question" in _type_of(q):
                        schema_questions += 1
                        ans = q.get("acceptedAnswer") or q.get("suggestedAnswer")
                        if ans:
                            schema_answered += 1
    schema_detected = schema_questions > 0

    # 2. Genuine visible Q&A structure.
    visible_pairs = 0

    # 2a. <details>/<summary>
    details = dom.find_all("details")
    visible_pairs += sum(1 for d in details if d.find("summary"))

    # 2b. <dl> definition lists used as Q&A
    for dl in dom.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if dts and dds:
            visible_pairs += min(len(dts), len(dds))

    # 2c. Question headings followed by an answer paragraph.
    for h in dom.find_all(["h2", "h3", "h4"]):
        text = h.get_text(strip=True).lower()
        is_q = text.endswith("?") or (text.split() and text.split()[0] in _QUESTION_WORDS)
        if not is_q:
            continue
        for sib in h.next_siblings:
            name = getattr(sib, "name", None)
            if name in ("p", "div", "span", "ul", "ol"):
                if len(sib.get_text(strip=True)) > 20:
                    visible_pairs += 1
                break
            if name and name.startswith("h"):
                break

    qa_pairs = max(schema_answered, visible_pairs)
    question_count = max(schema_questions, visible_pairs)
    detected = schema_detected or visible_pairs >= 2

    if schema_detected and visible_pairs >= 2:
        source = "both"
    elif schema_detected:
        source = "schema"
    elif visible_pairs >= 2:
        source = "visible"
    else:
        source = "none"

    return FaqSignals(
        detected=detected,
        schema_detected=schema_detected,
        question_count=question_count,
        qa_pairs_detected=qa_pairs,
        answered_count=max(schema_answered, visible_pairs),
        source=source,
    )


_GENERIC_TYPES = {"WebPage", "WebSite", "BreadcrumbList", "ListItem", "ImageObject", "SearchAction"}


def extract_entities(dom: RenderedDOM, graph: List[Dict[str, Any]], word_count: int) -> EntitySignals:
    content_text = dom.content_text()

    # Primary entity = first named, non-generic entity in the graph.
    primary_name = ""
    primary_type = ""
    all_types: List[str] = []
    named_count = 0
    for ent in graph:
        types = _type_of(ent)
        all_types.extend(types)
        if ent.get("name"):
            named_count += 1
        if not primary_name and ent.get("name") and not (set(types) & _GENERIC_TYPES):
            primary_name = str(ent.get("name"))
            primary_type = types[0] if types else ""

    # Count primary-entity mentions in the actual visible content.
    mentions = 0
    if primary_name:
        mentions = len(re.findall(re.escape(primary_name), content_text, re.IGNORECASE))

    per_1000 = round((mentions / word_count) * 1000, 2) if word_count > 0 else 0.0

    # Real schema-vs-HTML consistency: does the schema's primary entity actually
    # appear in the rendered content?
    consistent = True
    variations: List[str] = []
    if primary_name:
        consistent = mentions > 0
        if not consistent:
            # Maybe a variation (e.g., "Acme" vs "Acme Inc.") appears.
            core = primary_name.split()[0] if primary_name.split() else primary_name
            if core and len(core) > 2 and re.search(re.escape(core), content_text, re.IGNORECASE):
                consistent = True
                variations.append(core)

    return EntitySignals(
        primary_entity=primary_name,
        primary_entity_type=primary_type,
        primary_entity_mentions=mentions,
        entity_count=named_count,
        entity_per_1000_words=per_1000,
        unique_entity_types=len(set(all_types)),
        schema_vs_html_consistent=consistent,
        name_variations=variations,
        named_entities={},
    )
