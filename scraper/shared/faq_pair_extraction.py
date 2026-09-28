"""
FAQ question/answer pair extraction.

Extracts the FAQ Q/A pairs that are actually VISIBLE in a page's HTML, with
the full question and full answer text (not the 100-150 char previews that
extract_faq_howto_signals() stores for its heuristic counters). The result is
the sole source the backend uses to build FAQPage JSON-LD — nothing is ever
generated, paraphrased, truncated or invented here.

Design rules:
  * Precision over recall. A pair is only emitted when a question and its
    answer are structurally tied together (details/summary, dl/dt/dd, an
    aria-controls accordion, a question heading followed by its answer
    blocks, or a known accordion class pair) AND the question looks like a
    question. When in doubt the pair is dropped — the caller reports
    "detected but not reliably extractable" instead of guessing.
  * Text is the visible text only (tags stripped, whitespace collapsed).
    Nothing else is normalised, so the wording is exactly what a visitor sees.
  * Output order is document order.
"""

import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, NavigableString, Tag

MAX_PAIRS = 50
MAX_QUESTION_LEN = 500
MAX_ANSWER_LEN = 10000

_HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]
_QUESTION_HEADING_TAGS = ["h2", "h3", "h4", "h5", "h6"]
_MAX_ANSWER_BLOCKS = 4

_SKIP_TAGS = {"script", "style", "noscript", "template"}
_BLOCK_TAGS = {
    "p", "div", "li", "ul", "ol", "br", "h1", "h2", "h3", "h4", "h5", "h6",
    "tr", "td", "th", "table", "section", "article", "blockquote", "dd", "dt",
    "dl", "details", "summary", "figure", "figcaption", "pre",
}

_QUESTION_START = re.compile(
    r"^(who|whom|whose|what|when|where|why|how|which|is|are|am|was|were|can|"
    r"could|do|does|did|will|would|should|shall|may|might|has|have|had)\b",
    re.IGNORECASE,
)

# Heading text that opens an FAQ section (same vocabulary as FaqSchemaRule).
_FAQ_HEADING_RE = re.compile(
    r"\bfaqs?\b|frequently asked|common questions|questions\s*(&|and)\s*answers|"
    r"\bq\s*&\s*a\b|\bq\s*and\s*a\b",
    re.IGNORECASE,
)

_FAQ_CLASS_RE = re.compile(r"faq|frequently[-_ ]?asked|q[-_]?and[-_]?a\b", re.IGNORECASE)

# Accordion widgets (Elementor, Divi, Bootstrap, generic themes) render the
# question and the answer as adjacent siblings with these class vocabularies.
_QUESTION_CLASS_RE = re.compile(
    r"question|accordion[-_]?(title|header|button|heading|trigger)|tab[-_]?title|"
    r"toggle[-_]?title|faq[-_]?(title|q\b|head)",
    re.IGNORECASE,
)
_ANSWER_CLASS_RE = re.compile(
    r"answer|accordion[-_]?(content|body|panel|collapse)|tab[-_]?content|"
    r"toggle[-_]?content|faq[-_]?(content|a\b|body)|\bpanel\b|collapse",
    re.IGNORECASE,
)


def _visible_text(node: Any) -> str:
    """Visible text of a node, whitespace-collapsed, block boundaries as spaces."""
    parts: List[str] = []

    def walk(n: Any) -> None:
        for child in n.children:
            if type(child) is NavigableString:
                parts.append(str(child))
            elif isinstance(child, Tag):
                if child.name in _SKIP_TAGS:
                    continue
                is_block = child.name in _BLOCK_TAGS
                if is_block:
                    parts.append(" ")
                walk(child)
                if is_block:
                    parts.append(" ")

    if isinstance(node, Tag):
        walk(node)
    elif type(node) is NavigableString:
        parts.append(str(node))
    return re.sub(r"\s+", " ", "".join(parts)).strip()


# Public name for the shared "visible text of a node" helper (also used by rating_extraction).
visible_text = _visible_text


def _attr_tokens(el: Tag) -> str:
    classes = el.get("class") or []
    if isinstance(classes, str):
        classes = classes.split()
    return " ".join([str(el.get("id") or ""), *classes]).lower()


def _in_faq_container(el: Tag) -> bool:
    """True if the element or one of its (near) ancestors is marked as FAQ via id/class."""
    node: Optional[Tag] = el
    depth = 0
    while isinstance(node, Tag) and node.name not in ("body", "html", "[document]") and depth < 8:
        if _FAQ_CLASS_RE.search(_attr_tokens(node)):
            return True
        node = node.parent
        depth += 1
    return False


def _build_faq_heading_scope(soup: BeautifulSoup) -> set:
    """
    ids of every heading that sits inside an FAQ section: the FAQ heading itself
    plus every following deeper-level heading until a heading of the same or a
    higher level that does not itself open an FAQ section.
    """
    active: set = set()
    faq_level: Optional[int] = None
    for heading in soup.find_all(_HEADING_TAGS):
        level = int(heading.name[1])
        text = _visible_text(heading)
        if _FAQ_HEADING_RE.search(text):
            faq_level = level
            active.add(id(heading))
            continue
        if faq_level is not None:
            if level <= faq_level:
                faq_level = None
            else:
                active.add(id(heading))
    return active


def _looks_like_question(text: str, in_faq: bool, lenient: bool) -> bool:
    if not text or len(text) > MAX_QUESTION_LEN:
        return False
    if text.endswith("?") or text.endswith("？"):
        return True
    if not in_faq:
        return False
    return lenient or bool(_QUESTION_START.match(text))


def _next_heading_is_faq(el: Tag, heading_scope: set) -> bool:
    prev = el.find_previous(_HEADING_TAGS)
    return prev is not None and id(prev) in heading_scope


def _answer_from_heading(heading: Tag) -> str:
    """Answer blocks that follow a question heading, up to the next heading."""

    def collect(node: Tag) -> List[str]:
        blocks: List[str] = []
        for sib in node.find_next_siblings():
            if sib.name in _HEADING_TAGS or sib.name == "hr":
                break
            if sib.find(_HEADING_TAGS):
                break
            if sib.name in _SKIP_TAGS:
                continue
            text = _visible_text(sib)
            if not text:
                continue
            blocks.append(text)
            if len(blocks) >= _MAX_ANSWER_BLOCKS:
                break
        return blocks

    node: Tag = heading
    for _ in range(3):
        blocks = collect(node)
        if blocks:
            return " ".join(blocks)
        parent = node.parent
        if not isinstance(parent, Tag) or parent.name in ("body", "html", "[document]"):
            break
        # Only climb out of a wrapper that holds nothing but this node.
        if len(parent.find_all(True, recursive=False)) != 1:
            break
        node = parent
    return ""


def _norm_key(question: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()


def extract_faq_pairs(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Returns:
        {
          "pairs": [{"question", "answer", "source"}, ...]   # document order, deduped
          "count": int,
        }
    """
    order = {id(el): i for i, el in enumerate(soup.find_all(True))}
    heading_scope = _build_faq_heading_scope(soup)

    candidates: List[Dict[str, Any]] = []

    def add(q_el: Tag, question: str, answer: str, source: str) -> None:
        question = re.sub(r"\s+", " ", question).strip()
        answer = re.sub(r"\s+", " ", answer).strip()
        if not question or not answer:
            return
        if len(question) > MAX_QUESTION_LEN or len(answer) > MAX_ANSWER_LEN:
            return
        if _norm_key(question) == _norm_key(answer):
            return
        candidates.append({
            "order": order.get(id(q_el), 0),
            "question": question,
            "answer": answer,
            "source": source,
        })

    def in_faq(el: Tag) -> bool:
        return _in_faq_container(el) or _next_heading_is_faq(el, heading_scope)

    # 1) <details><summary>Question</summary>Answer</details>
    for details in soup.find_all("details"):
        summary = details.find("summary")
        if summary is None:
            continue
        question = _visible_text(summary)
        if not _looks_like_question(question, in_faq(details), lenient=True):
            continue
        answer_parts = [
            _visible_text(child) for child in details.children
            if child is not summary and (isinstance(child, Tag) or type(child) is NavigableString)
            and not (isinstance(child, Tag) and child.name in _SKIP_TAGS)
        ]
        add(summary, question, " ".join(p for p in answer_parts if p), "details")

    # 2) <dl><dt>Question</dt><dd>Answer</dd></dl>
    for dt in soup.find_all("dt"):
        question = _visible_text(dt)
        if not _looks_like_question(question, in_faq(dt), lenient=True):
            continue
        answers: List[str] = []
        for sib in dt.find_next_siblings():
            if sib.name == "dt":
                break
            if sib.name == "dd":
                text = _visible_text(sib)
                if text:
                    answers.append(text)
        add(dt, question, " ".join(answers), "dl")

    # 3) Accessible accordions: <button aria-controls="panel-id">Question</button>
    for trigger in soup.find_all(attrs={"aria-controls": True}):
        text = _visible_text(trigger)
        if not text or len(text) > MAX_QUESTION_LEN:
            continue
        # Cheap rejection first: nav toggles and the like are never questions.
        if not (text.endswith("?") or in_faq(trigger)):
            continue
        if not _looks_like_question(text, in_faq(trigger), lenient=True):
            continue
        target_ids = str(trigger.get("aria-controls") or "").split()
        target = soup.find(id=target_ids[0]) if target_ids else None
        if target is None or target is trigger:
            continue
        add(trigger, text, _visible_text(target), "aria")

    # 4) Question headings followed by their answer blocks
    for heading in soup.find_all(_QUESTION_HEADING_TAGS):
        question = _visible_text(heading)
        if not _looks_like_question(question, id(heading) in heading_scope or _in_faq_container(heading), lenient=False):
            continue
        add(heading, question, _answer_from_heading(heading), "heading")

    # 5) Accordion widgets with question/answer class pairs
    for el in soup.find_all(True):
        if el.name in _HEADING_TAGS or el.name in ("details", "summary", "dt", "dd"):
            continue
        if not _QUESTION_CLASS_RE.search(_attr_tokens(el)):
            continue
        question = _visible_text(el)
        if not _looks_like_question(question, in_faq(el), lenient=True):
            continue
        nxt = el.find_next_sibling()
        if nxt is None or not _ANSWER_CLASS_RE.search(_attr_tokens(nxt)):
            continue
        add(el, question, _visible_text(nxt), "class")

    candidates.sort(key=lambda c: c["order"])
    seen: set = set()
    pairs: List[Dict[str, str]] = []
    for c in candidates:
        key = _norm_key(c["question"])
        if not key or key in seen:
            continue
        seen.add(key)
        pairs.append({"question": c["question"], "answer": c["answer"], "source": c["source"]})
        if len(pairs) >= MAX_PAIRS:
            break

    return {"pairs": pairs, "count": len(pairs)}
