"""
SEO Intelligence extraction module.

Computes advanced SEO signals from already-extracted page data.
Adds seo_intelligence sub-object with 7 analysis groups.

NO scoring logic. NO rule evaluation. Data extraction only.
"""

import re
import math
import copy
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin, unquote

from bs4 import BeautifulSoup

from config.config import FILLER_WORDS, QUESTION_WORDS, DEPRECATED_SCHEMA_TYPES


# ============================================================
# Pre-compiled regex patterns (module-level for performance)
# ============================================================
_RE_SENTENCE_SPLIT = re.compile(r'[.!?]+')
_RE_WORD = re.compile(r'\b[a-zA-Z]+\b')
_RE_QA_BLOCK = re.compile(
    r'(?:<(?:h[2-6]|dt|strong|b)[^>]*>.*?\?.*?</(?:h[2-6]|dt|strong|b)>)',
    re.IGNORECASE | re.DOTALL
)
_RE_SYLLABLE = re.compile(r'[aeiouy]+', re.IGNORECASE)
_RE_NOISE_ATTR = re.compile(
    r'header|footer|nav|menu|modal|popup|sidebar|cookie|banner',
    re.IGNORECASE
)
_RE_HERO_EXCLUDE = re.compile(r'logo|icon|favicon', re.IGNORECASE)


def extract_seo_intelligence(html: str, soup: BeautifulSoup, seo_data: dict,
                              response_headers: dict, base_url: str) -> dict:
    """
    Orchestrate all 7 intelligence groups.
    Each group is wrapped in try/except so a failure in one never blocks others.

    Args:
        html: Raw HTML string
        soup: BeautifulSoup parsed object
        seo_data: Already-extracted SEO data from existing extractors
        response_headers: HTTP response headers dict
        base_url: The page URL

    Returns:
        dict with keys: title_meta, heading_analysis_extended, content_analysis,
                        schema_validation, image_context, security, link_analysis
    """
    intelligence = {}

    try:
        intelligence["title_meta"] = _extract_title_meta_intelligence(soup, seo_data)
    except Exception as e:
        intelligence["title_meta"] = {"error": str(e)}

    try:
        intelligence["heading_analysis_extended"] = _extract_heading_intelligence(soup, seo_data)
    except Exception as e:
        intelligence["heading_analysis_extended"] = {"error": str(e)}

    try:
        intelligence["content_analysis"] = _extract_content_intelligence(html, soup)
    except Exception as e:
        intelligence["content_analysis"] = {"error": str(e)}

    try:
        intelligence["schema_validation"] = _extract_schema_intelligence(seo_data)
    except Exception as e:
        intelligence["schema_validation"] = {"error": str(e)}

    try:
        intelligence["image_context"] = _extract_image_context_intelligence(soup, seo_data, base_url)
    except Exception as e:
        intelligence["image_context"] = {"error": str(e)}

    try:
        intelligence["security"] = _extract_security_intelligence(response_headers, base_url)
    except Exception as e:
        intelligence["security"] = {"error": str(e)}

    try:
        intelligence["link_analysis"] = _extract_link_intelligence(soup, seo_data, base_url)
    except Exception as e:
        intelligence["link_analysis"] = {"error": str(e)}

    return intelligence


# ============================================================
# Helper utilities
# ============================================================

def _count_syllables(word: str) -> int:
    """Approximate syllable count for Flesch readability."""
    word = word.lower().strip()
    if len(word) <= 3:
        return 1
    matches = _RE_SYLLABLE.findall(word)
    count = len(matches)
    if word.endswith('e') and count > 1:
        count -= 1
    return max(count, 1)


def _compute_text_similarity(text_a: str, text_b: str) -> float:
    """Simple word-overlap Jaccard similarity between two texts."""
    if not text_a or not text_b:
        return 0.0
    words_a = set(text_a.lower().split())
    words_b = set(text_b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union = words_a | words_b
    return round(len(intersection) / len(union), 2)


def _extract_weighted_keywords(texts: list, top_n: int = 3) -> list:
    """Extract top N keywords from combined texts, excluding filler words."""
    combined = " ".join(t for t in texts if t)
    words = _RE_WORD.findall(combined.lower())
    filtered = [w for w in words if w not in FILLER_WORDS and len(w) > 2]
    counts = Counter(filtered)
    return [word for word, _ in counts.most_common(top_n)]


def _is_noise_element(el) -> bool:
    """Check if an element is a noise container based on class/id patterns."""
    if not hasattr(el, 'attrs'):
        return False
    classes = " ".join(el.get("class", []))
    el_id = el.get("id", "") or ""
    combined = f"{classes} {el_id}"
    return bool(_RE_NOISE_ATTR.search(combined))


def _get_clean_content_soup(html: str) -> tuple[BeautifulSoup, dict]:
    """
    Build a cleaned soup for content analysis.
    Uses DOM cloning to avoid mutating original soup.
    Returns cleaned soup and debug info.
    """
    original_soup = BeautifulSoup(html, "lxml")
    debug_info = {}
    
    # 1. Identify main content container WITHOUT mutating original
    selected_container = None
    container_type = None
    
    # Priority: <main> -> <article> -> largest <div>
    main = original_soup.find("main")
    if main:
        selected_container = main
        container_type = "main"
    else:
        article = original_soup.find("article")
        if article:
            selected_container = article
            container_type = "article"
        else:
            # Find largest div by text length
            best_div = None
            best_len = 0
            for div in original_soup.find_all("div"):
                text_len = len(div.get_text(strip=True))
                if text_len > best_len:
                    best_len = text_len
                    best_div = div
            
            if best_div and best_len > 100:
                selected_container = best_div
                container_type = "div"
        
        # Fallback to body if no suitable container found
        if not selected_container:
            selected_container = original_soup.find("body") or original_soup
            container_type = "body"
    
    # 2. Deep clone ONLY the selected container
    container_clone = copy.deepcopy(selected_container)
    cloned_soup = BeautifulSoup(str(container_clone), "lxml")
    
    # Store original text length for debugging
    original_text_len = len(container_clone.get_text(strip=True))
    debug_info["original_container_type"] = container_type
    debug_info["original_text_length"] = original_text_len
    
    # 3. Perform structural cleaning ONLY inside cloned container
    # Remove structural tags
    for el in cloned_soup(["nav", "footer", "aside", "header"]):
        el.decompose()
    
    # Remove noise elements by class/id patterns
    noise_patterns = [
        r'header', r'footer', r'nav', r'menu', r'sidebar', 
        r'popup', r'modal', r'banner', r'cookie'
    ]
    noise_regex = re.compile('|'.join(noise_patterns), re.IGNORECASE)
    
    for el in cloned_soup.find_all(True):
        if not hasattr(el, 'attrs') or not el.attrs:
            continue
        classes = ' '.join(el.get('class', []))
        el_id = el.get('id', '')
        combined = f"{classes} {el_id}"
        if noise_regex.search(combined):
            el.decompose()
    
    # Remove form elements
    for el in cloned_soup(["form", "input", "select", "textarea", "button", "label"]):
        el.decompose()
    
    # Remove small elements (likely noise)
    for el in cloned_soup.find_all(True):
        if hasattr(el, 'get_text') and len(el.get_text(strip=True)) < 30:
            # Only remove if it's not a heading or important tag
            if el.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a']:
                el.decompose()
    
    # Store cleaned text length for debugging
    cleaned_text_len = len(cloned_soup.get_text(strip=True))
    debug_info["cleaned_text_length"] = cleaned_text_len
    debug_info["text_reduction_ratio"] = round(1 - (cleaned_text_len / max(original_text_len, 1)), 2)
    
    return cloned_soup, debug_info


def _remove_duplicate_blocks(text: str, min_words: int = 20) -> str:
    """Remove exact repeated sequences of > min_words words."""
    words = text.split()
    if len(words) < min_words * 2:
        return text

    # Build blocks of min_words consecutive words and track duplicates
    seen_blocks = set()
    remove_indices = set()

    for i in range(len(words) - min_words + 1):
        block = " ".join(words[i:i + min_words])
        if block in seen_blocks:
            for j in range(i, min(i + min_words, len(words))):
                remove_indices.add(j)
        else:
            seen_blocks.add(block)

    if not remove_indices:
        return text

    cleaned = [w for i, w in enumerate(words) if i not in remove_indices]
    return " ".join(cleaned)


def _normalize_link_url(href: str, base_url: str) -> str:
    """Normalize a URL for deduplication: decode, strip, lowercase domain, remove trailing slashes."""
    href = href.strip()
    # Decode URL encoding
    href = unquote(href)
    # Strip trailing whitespace/encoded spaces
    href = href.rstrip()

    # Resolve to absolute
    if href.startswith("http"):
        abs_url = href
    else:
        abs_url = urljoin(base_url, href)

    try:
        parsed = urlparse(abs_url)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return abs_url.rstrip("/")


# ============================================================
# GROUP 1 — TITLE + META INTELLIGENCE
# (Fix 5: First paragraph extraction hardened)
# ============================================================

def _extract_title_meta_intelligence(soup: BeautifulSoup, seo_data: dict) -> dict:
    """Extract title/meta intelligence signals."""
    title = seo_data.get("title", "") or ""
    meta_tags = seo_data.get("meta_tags", {})
    headings = seo_data.get("content", {}).get("headings", {})
    h1_text = headings.get("h1", [""])[0] if headings.get("h1") else ""

    # Title DOM position
    title_dom_position = None
    if soup.head:
        for idx, child in enumerate(soup.head.children):
            if hasattr(child, 'name') and child.name == 'title':
                title_dom_position = idx
                break

    # Normalized title
    normalized_title = title.strip().lower() if title else ""

    # ── Fix 5: First paragraph extraction ──
    # Skip empty <p>, form labels, short blocks < 40 chars.
    # Fallback: first 2 sentences from cleaned content.
    first_paragraph = ""
    for p_tag in soup.find_all("p"):
        p_text = p_tag.get_text(strip=True)
        if not p_text or len(p_text) < 40:
            continue
        # Skip if inside a form
        if p_tag.find_parent("form"):
            continue
        # Skip if parent is a noise element
        parent = p_tag.parent
        if parent and _is_noise_element(parent):
            continue
        first_paragraph = p_text[:500]
        break

    # Fallback: first 2 sentences from body text
    if not first_paragraph:
        body = soup.find("body")
        if body:
            body_text = body.get_text(separator=" ", strip=True)
            sentences = [s.strip() for s in _RE_SENTENCE_SPLIT.split(body_text) if s.strip() and len(s.strip()) >= 40]
            if sentences:
                first_paragraph = ". ".join(sentences[:2])[:500]

    # Primary keywords from title + H1 + first paragraph
    primary_keywords = _extract_weighted_keywords([title, h1_text, first_paragraph])

    # Keyword position index in title
    title_words = title.lower().split() if title else []
    keyword_position_index = None
    if primary_keywords and title_words:
        for i, word in enumerate(title_words):
            if word.strip(".,!?;:") in primary_keywords:
                keyword_position_index = i
                break

    # Title word count
    title_word_count = len(title_words)

    # Filler words detected in title
    filler_in_title = [w for w in title_words if w.strip(".,!?;:") in FILLER_WORDS]

    # Keyword stuffing score (ratio of most frequent word to total)
    keyword_stuffing_score = 0.0
    if title_words:
        word_counts = Counter(w.strip(".,!?;:") for w in title_words if w.strip(".,!?;:"))
        if word_counts:
            most_common_count = word_counts.most_common(1)[0][1]
            keyword_stuffing_score = round(most_common_count / len(title_words), 2)

    # Similarity: title vs og:title
    og_title_list = meta_tags.get("og:title", [])
    og_title = og_title_list[0] if og_title_list else ""
    title_og_similarity = _compute_text_similarity(title, og_title)

    # Similarity: title vs meta description
    desc_list = meta_tags.get("description", [])
    meta_desc = desc_list[0] if desc_list else ""
    title_desc_similarity = _compute_text_similarity(title, meta_desc)

    return {
        "title_dom_position": title_dom_position,
        "normalized_title": normalized_title,
        "first_paragraph": first_paragraph,
        "primary_keywords": primary_keywords,
        "keyword_position_index": keyword_position_index,
        "title_word_count": title_word_count,
        "filler_words_detected": filler_in_title,
        "keyword_stuffing_score": keyword_stuffing_score,
        "title_og_title_similarity": title_og_similarity,
        "title_meta_description_similarity": title_desc_similarity
    }


# ============================================================
# GROUP 2 — HEADING STRUCTURE INTELLIGENCE
# (Fix 2: Parent level logic corrected)
# ============================================================

def _extract_heading_intelligence(soup: BeautifulSoup, seo_data: dict) -> dict:
    """Extract enhanced heading structure analysis."""
    title = seo_data.get("title", "") or ""
    primary_keywords = _extract_weighted_keywords([title])

    headings_list = []
    hierarchy_errors = []
    skipped_levels = []
    duplicate_headings = []
    case_violations = []
    question_headings = []
    seen_texts = {}
    dom_index = 0

    # ── Fix 2: Track heading stack for parent_level ──
    # Stack holds (level, dom_index) of preceding headings.
    # parent_level = closest earlier heading with a strictly lower level number.
    heading_stack = []  # [(level, dom_index), ...]

    for tag in soup.find_all(re.compile(r'^h[1-6]$')):
        text = tag.get_text(strip=True)
        if not text:
            continue

        level = int(tag.name[1])

        # Find parent: closest preceding heading with lower level
        parent_level = None
        for prev_level, _ in reversed(heading_stack):
            if prev_level < level:
                parent_level = prev_level
                break

        # Case analysis
        is_all_caps = text == text.upper() and text != text.lower()
        is_sentence_case = text[0].isupper() and text[1:] != text[1:].upper() if len(text) > 1 else True

        # Question word detection
        first_word = text.split()[0].lower().strip(".,!?;:") if text.split() else ""
        starts_with_question = first_word in QUESTION_WORDS

        # Keyword presence score
        heading_words = set(text.lower().split())
        keyword_matches = sum(1 for kw in primary_keywords if kw in heading_words)
        keyword_presence = round(keyword_matches / max(len(primary_keywords), 1), 2)

        headings_list.append({
            "text": text,
            "level": level,
            "dom_index": dom_index,
            "parent_level": parent_level,
            "is_all_caps": is_all_caps,
            "is_sentence_case": is_sentence_case,
            "starts_with_question_word": starts_with_question,
            "keyword_presence_score": keyword_presence
        })

        # Hierarchy errors: skipped levels from last heading to this one
        prev_level_val = heading_stack[-1][0] if heading_stack else 0
        if prev_level_val > 0 and level > prev_level_val + 1:
            gap = list(range(prev_level_val + 1, level))
            skipped_levels.extend(gap)
            hierarchy_errors.append({
                "from_level": prev_level_val,
                "to_level": level,
                "skipped": gap,
                "text": text
            })

        # Duplicate detection
        if text in seen_texts:
            duplicate_headings.append({"text": text, "level": level, "first_at_index": seen_texts[text]})
        else:
            seen_texts[text] = dom_index

        # Case violations
        if is_all_caps:
            case_violations.append({"text": text, "level": level, "issue": "ALL_CAPS"})

        # Question headings
        if starts_with_question:
            question_headings.append({"text": text, "level": level})

        heading_stack.append((level, dom_index))
        dom_index += 1

    return {
        "headings": headings_list,
        "hierarchy_errors": hierarchy_errors,
        "skipped_levels": list(set(skipped_levels)),
        "duplicate_headings": duplicate_headings,
        "heading_case_violations": case_violations,
        "question_headings": question_headings
    }


# ============================================================
# GROUP 3 — CONTENT INTELLIGENCE
# (Fix 1: Structural noise cleaning hardened)
# ============================================================

def _extract_content_intelligence(html: str, soup: BeautifulSoup) -> dict:
    """Extract content-level intelligence signals with hardened noise removal."""
    # ── Fix 1: Deep structural cleaning without DOM mutation ──
    content_soup, debug_info = _get_clean_content_soup(html)

    full_text = content_soup.get_text(separator=" ", strip=True)

    # Remove exact duplicate blocks (> 20 words repeated)
    full_text = _remove_duplicate_blocks(full_text)

    words = full_text.split()

    # First 200 words (from cleaned content)
    first_200_words = " ".join(words[:200])

    # Paragraph count (only meaningful paragraphs)
    paragraphs = content_soup.find_all("p")
    meaningful_paras = [p for p in paragraphs if len(p.get_text(strip=True)) >= 20]
    paragraph_count = len(meaningful_paras)

    # Average sentence length
    sentences = [s.strip() for s in _RE_SENTENCE_SPLIT.split(full_text) if s.strip() and len(s.split()) >= 3]
    sentence_lengths = [len(s.split()) for s in sentences]
    avg_sentence_length = round(sum(sentence_lengths) / max(len(sentence_lengths), 1), 1)

    # Flesch Reading Ease Score
    total_words = len(words)
    total_sentences = max(len(sentences), 1)
    total_syllables = sum(_count_syllables(w) for w in words[:2000])  # Cap at 2000 words for perf
    words_for_syllables = min(total_words, 2000)

    readability_score = 0.0
    if words_for_syllables > 0:
        readability_score = round(
            206.835
            - 1.015 * (words_for_syllables / total_sentences)
            - 84.6 * (total_syllables / max(words_for_syllables, 1)),
            1
        )
        readability_score = max(0.0, min(100.0, readability_score))

    # Internal repetition ratio (from cleaned paragraphs, excluding nav duplication)
    para_texts = [p.get_text(strip=True) for p in meaningful_paras if p.get_text(strip=True)]
    para_counts = Counter(para_texts)
    duplicate_paras = sum(c - 1 for c in para_counts.values() if c > 1)
    repetition_ratio = round(duplicate_paras / max(len(para_texts), 1), 2)

    # FAQ blocks detected (Q&A patterns in cleaned HTML)
    cleaned_html = str(content_soup)
    faq_blocks = len(_RE_QA_BLOCK.findall(cleaned_html))

    # Intent classification heuristic (on cleaned text)
    intent = _classify_intent(full_text)

    # Include debug info for validation
    result = {
        "first_200_words": first_200_words,
        "paragraph_count": paragraph_count,
        "word_count": total_words,
        "average_sentence_length": avg_sentence_length,
        "readability_score": readability_score,
        "internal_repetition_ratio": repetition_ratio,
        "faq_blocks_detected": faq_blocks,
        "intent_classification": intent,
        "debug_info": debug_info
    }
    
    return result


def _classify_intent(text: str) -> str:
    """Classify page intent as informational / commercial / transactional."""
    text_lower = text.lower()

    transactional_signals = ["buy", "order", "purchase", "add to cart", "checkout",
                             "subscribe", "sign up", "get started", "download",
                             "free trial", "pricing", "discount", "coupon"]
    commercial_signals = ["best", "top", "review", "compare", "vs", "alternative",
                          "affordable", "cheap", "premium", "professional",
                          "services", "solutions", "package", "plan"]
    informational_signals = ["what is", "how to", "guide", "tutorial", "learn",
                             "understand", "definition", "meaning", "explained",
                             "tips", "steps", "example"]

    t_score = sum(1 for s in transactional_signals if s in text_lower)
    c_score = sum(1 for s in commercial_signals if s in text_lower)
    i_score = sum(1 for s in informational_signals if s in text_lower)

    max_score = max(t_score, c_score, i_score)
    if max_score == 0:
        return "informational"
    if t_score == max_score:
        return "transactional"
    if c_score == max_score:
        return "commercial"
    return "informational"


# ============================================================
# GROUP 4 — STRUCTURED DATA DEPTH (unchanged)
# ============================================================

def _extract_schema_intelligence(seo_data: dict) -> dict:
    """Validate and analyze structured data schemas."""
    raw_schemas = seo_data.get("structured_data", [])
    validated = []
    has_faq = False
    has_how_to = False
    has_article = False
    has_breadcrumb = False

    for schema in raw_schemas:
        if not isinstance(schema, dict):
            continue

        schema_type = schema.get("@type", "Unknown")
        if isinstance(schema_type, list):
            schema_type = schema_type[0] if schema_type else "Unknown"

        entry = {
            "type": schema_type,
            "is_valid": True,
            "missing_required": [],
            "deprecated": schema_type in DEPRECATED_SCHEMA_TYPES
        }

        # BreadcrumbList validation
        if schema_type == "BreadcrumbList":
            has_breadcrumb = True
            items = schema.get("itemListElement", [])
            entry["has_items"] = bool(items)
            entry["item_count"] = len(items) if isinstance(items, list) else 0
            if not items:
                entry["is_valid"] = False
                entry["missing_required"].append("itemListElement")

        # Article validation
        elif schema_type in ("Article", "NewsArticle", "BlogPosting"):
            has_article = True
            required = ["headline", "author", "datePublished"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        # FAQPage validation
        elif schema_type == "FAQPage":
            has_faq = True
            main_entity = schema.get("mainEntity", [])
            entry["question_count"] = len(main_entity) if isinstance(main_entity, list) else 0
            if not main_entity:
                entry["is_valid"] = False
                entry["missing_required"].append("mainEntity")

        # HowTo validation
        elif schema_type == "HowTo":
            has_how_to = True
            steps = schema.get("step", [])
            entry["step_count"] = len(steps) if isinstance(steps, list) else 0
            if not steps:
                entry["is_valid"] = False
                entry["missing_required"].append("step")

        # Organization validation
        elif schema_type == "Organization":
            required = ["name", "url"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        # LocalBusiness validation
        elif schema_type == "LocalBusiness":
            required = ["name", "address", "description"]
            for field in required:
                if not schema.get(field):
                    entry["missing_required"].append(field)
            if entry["missing_required"]:
                entry["is_valid"] = False

        if entry["deprecated"]:
            entry["is_valid"] = False

        validated.append(entry)

    return {
        "schemas": validated,
        "has_faq_page": has_faq,
        "has_how_to": has_how_to,
        "has_article": has_article,
        "has_breadcrumb": has_breadcrumb
    }


# ============================================================
# GROUP 5 — IMAGE CONTEXT INTELLIGENCE
# (Fix 3: Hero detection refined)
# ============================================================

def _extract_image_context_intelligence(soup: BeautifulSoup, seo_data: dict, base_url: str) -> dict:
    """Extract image context intelligence with refined hero detection."""
    images = seo_data.get("images", [])
    page_domain = urlparse(base_url).netloc.lower()

    hero_image = None
    logo_image = None
    cdn_domains = set()
    decorative_count = 0

    # ── Fix 3: Locate first H1 DOM position for hero filtering ──
    first_h1 = soup.find("h1")
    h1_dom_position = None
    if first_h1:
        # Approximate DOM index by counting preceding elements
        h1_dom_position = len(list(first_h1.find_all_previous()))

    # Build a set of images inside header/nav for hero exclusion
    header_nav_srcs = set()
    for container in soup.find_all(["header", "nav"]):
        for img in container.find_all("img", src=True):
            header_nav_srcs.add(img.get("src", "").strip())

    # Track best hero candidate by visible area
    best_hero = None
    best_hero_area = 0

    for idx, img_data in enumerate(images):
        src = img_data.get("src", "")
        alt = img_data.get("alt", "")
        width = img_data.get("width")
        height = img_data.get("height")

        # Parse numeric dimensions
        w_num = 0
        h_num = 0
        try:
            w_num = int(str(width).replace("px", "").strip()) if width else 0
        except (ValueError, TypeError):
            pass
        try:
            h_num = int(str(height).replace("px", "").strip()) if height else 0
        except (ValueError, TypeError):
            pass

        src_lower = src.lower()
        alt_lower = alt.lower()

        # Logo detection (runs first, before hero, so hero can exclude logos)
        if not logo_image and ("logo" in src_lower or "logo" in alt_lower):
            logo_image = {"src": src, "dom_index": idx}

        # ── Fix 3: Hero candidate filtering ──
        # Skip if filename contains logo/icon/favicon
        if _RE_HERO_EXCLUDE.search(src_lower):
            pass  # not a hero candidate
        # Skip if too small
        elif w_num < 300 or h_num < 200:
            pass  # not a hero candidate
        # Skip if inside header/nav
        elif src.strip() in header_nav_srcs:
            pass  # not a hero candidate
        else:
            area = w_num * h_num
            if area > best_hero_area:
                # Compute confidence score
                confidence = 0.5  # Base
                if h1_dom_position is not None and idx > 0:
                    confidence += 0.2  # Appears after some content
                if w_num >= 600:
                    confidence += 0.15
                if h_num >= 400:
                    confidence += 0.15
                confidence = min(round(confidence, 2), 1.0)

                best_hero = {
                    "src": src,
                    "dom_index": idx,
                    "width": w_num,
                    "height": h_num,
                    "hero_image_confidence_score": confidence
                }
                best_hero_area = area

        # CDN detection: image domain differs from page domain
        try:
            img_domain = urlparse(src).netloc.lower()
            if img_domain and img_domain != page_domain and not img_domain.endswith(page_domain):
                cdn_domains.add(img_domain)
        except Exception:
            pass

        # Decorative classification: no alt + small size or role=presentation
        if img_data.get("is_decorative", False):
            decorative_count += 1
        elif not alt and w_num > 0 and w_num < 50 and h_num > 0 and h_num < 50:
            decorative_count += 1

    hero_image = best_hero

    # Also check soup for role="presentation" images
    for img in soup.find_all("img", attrs={"role": "presentation"}):
        decorative_count += 1

    return {
        "hero_image": hero_image,
        "logo_image": logo_image,
        "cdn_usage_detected": len(cdn_domains) > 0,
        "cdn_domains": list(cdn_domains),
        "decorative_images": decorative_count
    }


# ============================================================
# GROUP 6 — SECURITY & RESPONSE HEADERS (unchanged)
# ============================================================

def _extract_security_intelligence(response_headers: dict, base_url: str) -> dict:
    """Extract security and response header intelligence."""
    headers_lower = {k.lower(): v for k, v in (response_headers or {}).items()}

    security_headers = {
        "csp": "content-security-policy" in headers_lower,
        "hsts": "strict-transport-security" in headers_lower,
        "x_frame_options": "x-frame-options" in headers_lower,
        "x_content_type_options": "x-content-type-options" in headers_lower,
    }

    # Final URL (from Location header chain) — if no redirect, same as base_url
    final_url = base_url
    location = headers_lower.get("location")
    if location:
        final_url = location

    # Redirect chain detection (from response headers we only see the last hop)
    redirect_chain = []
    if location and location != base_url:
        redirect_chain.append({"from": base_url, "to": location})

    return {
        "response_headers": response_headers or {},
        "security_headers": security_headers,
        "final_url": final_url,
        "redirect_chain": redirect_chain
    }


# ============================================================
# GROUP 7 — LINK EXTRACTION & REL ANALYSIS
# (Fix 4: URL normalization, dedup, orphan hardening)
# ============================================================

def _extract_link_intelligence(soup: BeautifulSoup, seo_data: dict, base_url: str) -> dict:
    """Extract hardened link analysis with dedup and normalization."""
    page_domain = urlparse(base_url).netloc.lower().replace("www.", "")
    normalized_page_url = _normalize_link_url(base_url, base_url)

    links = []
    total_internal = 0
    total_external = 0
    dom_index = 0
    orphan_candidates = []
    pagination_detected = False
    noindex_detected = False

    # Initialize counters for unique href tracking
    unique_internal = 0
    unique_external = 0

    # Check for noindex in meta robots
    meta_tags = seo_data.get("meta_tags", {})
    robots_content = meta_tags.get("robots", [])
    if robots_content:
        robots_str = " ".join(robots_content).lower()
        if "noindex" in robots_str:
            noindex_detected = True

    # Check rel=next/prev for pagination
    for link_tag in soup.find_all("link", rel=True):
        rel_vals = link_tag.get("rel", [])
        if isinstance(rel_vals, list):
            rel_str = " ".join(rel_vals).lower()
        else:
            rel_str = str(rel_vals).lower()
        if "next" in rel_str or "prev" in rel_str:
            pagination_detected = True
            break

    # ── Fix 4: URL-only deduplication ──
    # Key = normalized_href only (anchor text variations handled separately)
    seen_hrefs = set()
    href_data = {}  # Store anchor variations and counts for each href

    # Collect nav-link hrefs for orphan exclusion
    nav_hrefs = set()
    for nav in soup.find_all(["nav", "header"]):
        for a in nav.find_all("a", href=True):
            nav_hrefs.add(_normalize_link_url(a.get("href", "").strip(), base_url))

    # Extract all <a> tags
    all_a_tags = soup.find_all("a", href=True)
    
    for a_tag in all_a_tags:
        href = a_tag.get("href", "").strip()
        
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue

        anchor_text = a_tag.get_text(strip=True)[:200]

        # Normalize URL
        normalized_href = _normalize_link_url(href, base_url)

        # Dedup check by href only
        if normalized_href in seen_hrefs:
            # Update existing href data with anchor variation
            if normalized_href in href_data:
                existing_data = href_data[normalized_href]
                if anchor_text not in existing_data["anchor_variations"]:
                    existing_data["anchor_variations"].append(anchor_text)
                existing_data["occurrence_count"] += 1
            continue
        
        seen_hrefs.add(normalized_href)
        
        # Resolve to absolute URL
        abs_url = href if href.startswith("http") else urljoin(base_url, href)

        # Rel attributes
        rel_attrs = a_tag.get("rel", [])
        if isinstance(rel_attrs, str):
            rel_attrs = [rel_attrs]
        rel_attrs = [r.lower() for r in rel_attrs]

        # Classify
        try:
            link_domain = urlparse(abs_url).netloc.lower().replace("www.", "")
        except Exception:
            link_domain = ""

        is_internal = link_domain.endswith(page_domain)
        is_external = not is_internal and bool(link_domain)
        is_nofollow = "nofollow" in rel_attrs

        # Don't add individual link entry here - will build from href_data
        # Don't count here either - will count from unique href_data

        # ── Fix 4: Hardened orphan detection ──
        # Only internal, non-empty-anchor, non-homepage, non-nav-duplicate
        if is_internal and not anchor_text.strip():
            if normalized_href != normalized_page_url:  # Exclude homepage self-links
                if normalized_href not in nav_hrefs:  # Exclude nav duplicates
                    orphan_candidates.append(href)

        # Store href data
        href_data[normalized_href] = {
            "anchor_variations": [anchor_text],
            "occurrence_count": 1,
            "first_dom_index": dom_index,
            "href": href,
            "rel_attributes": rel_attrs,
            "is_internal": is_internal,
            "is_external": is_external,
            "is_nofollow": is_nofollow
        }

        dom_index += 1

    # Build final links list from href_data (unique hrefs only)
    for normalized_href, data in href_data.items():
        links.append({
            "href": data["href"],
            "anchor_text": data["anchor_variations"][0] if data["anchor_variations"] else "",  # Primary anchor
            "anchor_variations": data["anchor_variations"],
            "occurrence_count": data["occurrence_count"],
            "rel_attributes": data["rel_attributes"],
            "is_internal": data["is_internal"],
            "is_external": data["is_external"],
            "is_nofollow": data["is_nofollow"],
            "dom_index": data["first_dom_index"]
        })
        
        if data["is_internal"]:
            unique_internal += 1
        elif data["is_external"]:
            unique_external += 1

    return {
        "links": links,
        "total_internal_links": unique_internal,
        "total_external_links": unique_external,
        "orphan_candidates": orphan_candidates,
        "pagination_detected": pagination_detected,
        "noindex_detected": noindex_detected
    }
