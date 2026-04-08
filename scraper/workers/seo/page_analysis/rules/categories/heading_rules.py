"""
Heading SEO Rules (Rules 84–121, 141, 229)

All rules related to H1–H6 heading validation: count, length,
keyword placement, duplicates, readability, and accessibility.
"""

import re
from ..base_seo_rule import BaseSEORuleV2
from ..seo_rule_utils import _keyword_from_context


# ── Helpers ───────────────────────────────────────────────────

GENERIC_PHRASES = {
    "home", "homepage", "welcome", "page", "untitled",
    "about", "about us", "services", "contact",
    "read more", "learn more", "click here",
}

FILLER_WORDS = {
    "best", "top", "amazing", "awesome", "incredible",
    "ultimate", "ever", "guaranteed",
}

CLICKBAIT_WORDS = {
    "you won't believe", "shocking", "mind-blowing",
    "secret", "hack", "insane", "jaw-dropping",
}


def _get_headings_by_tag(headings, tag):
    return [h for h in headings if h.get("tag") == tag]


def _heading_texts(headings, tag):
    return [h.get("text", "").strip() for h in headings if h.get("tag") == tag]


# _keyword_from_context imported from seo_rule_utils


def _secondary_keyword(normalized):
    meta_tags = normalized.get("meta_tags", {})
    keywords_list = meta_tags.get("keywords", [])
    if keywords_list and isinstance(keywords_list, list) and keywords_list[0]:
        kw = keywords_list[0]
        if isinstance(kw, str):
            parts = [k.strip().lower() for k in kw.split(",") if k.strip()]
            if len(parts) >= 2:
                return parts[1]
    return ""


# ═══════════════════════════════════════════════════════════════
# H1 RULES (Rules 84–93)
# ═══════════════════════════════════════════════════════════════

class H1ExactlyOneRule(BaseSEORuleV2):
    rule_id = "H1_EXACTLY_ONE"
    rule_no = 84
    category = "Headings"
    severity = "high"
    description = "Exactly one H1 per page"

    def evaluate(self, normalized, job_id, project_id, url):
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        if len(h1s) != 1:
            return [self.create_issue(
                job_id, project_id, url,
                f"Page has {len(h1s)} H1 tags (expected exactly 1)",
                len(h1s), "Exactly 1 H1 tag",
                data_key="headings", data_path="h1"
            )]
        return []


class H1LengthRule(BaseSEORuleV2):
    rule_id = "H1_LENGTH"
    rule_no = 85
    category = "Headings"
    severity = "high"
    description = "H1 length should be 20–70 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        for h in h1s:
            text = h.get("text", "").strip()
            if text:
                length = len(text)
                if length < 20 or length > 70:
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"H1 length is {length} characters (should be 20–70)",
                        length, "20–70 characters",
                        data_key="headings", data_path="h1"
                    )]
        return []


class H1ContainsKeywordRule(BaseSEORuleV2):
    rule_id = "H1_CONTAINS_KEYWORD"
    rule_no = 90
    category = "Headings"
    severity = "high"
    description = "H1 must contain primary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        for h in h1s:
            text = h.get("text", "").strip()
            if text and keyword.lower() not in text.lower():
                return [self.create_issue(
                    job_id, project_id, url,
                    "H1 does not contain primary keyword",
                    text, f"Include keyword: {keyword}",
                    data_key="headings", data_path="h1"
                )]
        return []


class H1KeywordAtBeginningRule(BaseSEORuleV2):
    rule_id = "H1_KEYWORD_BEGINNING"
    rule_no = 87
    category = "Headings"
    severity = "high"
    description = "Primary keyword should be at beginning of H1"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        for h in h1s:
            text = h.get("text", "").strip()
            if text and keyword.lower() in text.lower():
                pos = text.lower().find(keyword.lower())
                if pos > len(text) * 0.3:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Keyword appears late in H1",
                        f"Position {pos}", "Keyword at beginning",
                        data_key="headings", data_path="h1"
                    )]
        return []


# Rule 88 (H1_ALL_CAPS) removed — covered by rule 111 (HEADING_ALL_CAPS)


# Rule 89 (H1_EMPTY) removed — covered by rule 108 (HEADING_NO_EMPTY)


# Rule 90 (H1_IS_LINK) removed — covered by rule 110 (HEADING_NOT_LINK)


# Rule 91 (H1_CONTAINS_HTML) removed — covered by rule 109 (HEADING_NO_HTML)


class H1ReadabilityRule(BaseSEORuleV2):
    rule_id = "H1_READABILITY"
    rule_no = 92
    category = "Headings"
    severity = "medium"
    description = "H1 should be readable (≤10 words)"

    def evaluate(self, normalized, job_id, project_id, url):
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        for h in h1s:
            text = h.get("text", "").strip()
            if text:
                wc = len(text.split())
                if wc > 10:
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"H1 has {wc} words (recommend ≤10)",
                        str(wc), "≤10 words",
                        data_key="headings", data_path="h1"
                    )]
        return []


# Rule 93 (H1_CLICKBAIT_FILLER) removed — covered by rule 121 (HEADING_CLICKBAIT_FILLER)


# ═══════════════════════════════════════════════════════════════
# H2 RULES (Rules 94–98)
# ═══════════════════════════════════════════════════════════════

class H2CountRule(BaseSEORuleV2):
    rule_id = "H2_COUNT"
    rule_no = 94
    category = "Headings"
    severity = "high"
    description = "H2 count should be reasonable (0–10)"

    def evaluate(self, normalized, job_id, project_id, url):
        h2s = _get_headings_by_tag(normalized.get("headings", []), "h2")
        if len(h2s) > 10:
            return [self.create_issue(
                job_id, project_id, url,
                f"Too many H2 tags ({len(h2s)})",
                len(h2s), "0–10 H2 tags",
                data_key="headings", data_path="h2"
            )]
        return []


class H2ContainsKeywordRule(BaseSEORuleV2):
    rule_id = "H2_CONTAINS_KEYWORD"
    rule_no = 95
    category = "Headings"
    severity = "high"
    description = "H2 should contain secondary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _secondary_keyword(normalized)
        if not keyword:
            keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        h2_texts = _heading_texts(normalized.get("headings", []), "h2")
        if h2_texts and not any(keyword.lower() in t.lower() for t in h2_texts):
            return [self.create_issue(
                job_id, project_id, url,
                "No H2 contains secondary/primary keyword",
                f"{len(h2_texts)} H2s checked", f"Include: {keyword}",
                data_key="headings", data_path="h2"
            )]
        return []


class H2LengthRule(BaseSEORuleV2):
    rule_id = "H2_LENGTH"
    rule_no = 96
    category = "Headings"
    severity = "medium"
    description = "H2 length should be 10–80 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        issues = []
        h2s = _get_headings_by_tag(normalized.get("headings", []), "h2")
        for h in h2s:
            text = h.get("text", "").strip()
            if text:
                length = len(text)
                if length < 10 or length > 80:
                    issues.append(self.create_issue(
                        job_id, project_id, url,
                        f"H2 length is {length} chars (should be 10–80)",
                        text[:60], "10–80 characters",
                        data_key="headings", data_path="h2"
                    ))
                    break  # report once
        return issues


class H2EmptyRule(BaseSEORuleV2):
    rule_id = "H2_EMPTY"
    rule_no = 97
    category = "Headings"
    severity = "high"
    description = "H2 should not be empty"

    def evaluate(self, normalized, job_id, project_id, url):
        h2s = _get_headings_by_tag(normalized.get("headings", []), "h2")
        empty_count = sum(1 for h in h2s if not h.get("text", "").strip())
        if empty_count > 0:
            return [self.create_issue(
                job_id, project_id, url,
                f"{empty_count} empty H2 tag(s) found",
                empty_count, "All H2 tags should have text",
                data_key="headings", data_path="h2"
            )]
        return []


class H2DuplicateRule(BaseSEORuleV2):
    rule_id = "H2_DUPLICATE"
    rule_no = 98
    category = "Headings"
    severity = "high"
    description = "H2 tags should be unique"

    def evaluate(self, normalized, job_id, project_id, url):
        texts = [t.lower() for t in _heading_texts(normalized.get("headings", []), "h2") if t]
        seen = set()
        dupes = []
        for t in texts:
            if t in seen:
                dupes.append(t)
            seen.add(t)
        if dupes:
            return [self.create_issue(
                job_id, project_id, url,
                "Duplicate H2 tags found",
                ", ".join(dupes[:3]), "H2 tags should be unique",
                data_key="headings", data_path="h2"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# H3 RULES (Rules 99–101)
# ═══════════════════════════════════════════════════════════════

class H3CountRule(BaseSEORuleV2):
    rule_id = "H3_COUNT"
    rule_no = 99
    category = "Headings"
    severity = "medium"
    description = "H3 count should be reasonable (0–15)"

    def evaluate(self, normalized, job_id, project_id, url):
        h3s = _get_headings_by_tag(normalized.get("headings", []), "h3")
        if len(h3s) > 15:
            return [self.create_issue(
                job_id, project_id, url,
                f"Too many H3 tags ({len(h3s)})",
                len(h3s), "0–15 H3 tags",
                data_key="headings", data_path="h3"
            )]
        return []


class H3LengthRule(BaseSEORuleV2):
    rule_id = "H3_LENGTH"
    rule_no = 100
    category = "Headings"
    severity = "medium"
    description = "H3 length should be 8–70 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        h3s = _get_headings_by_tag(normalized.get("headings", []), "h3")
        for h in h3s:
            text = h.get("text", "").strip()
            if text and (len(text) < 8 or len(text) > 70):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"H3 length is {len(text)} chars (should be 8–70)",
                    text[:60], "8–70 characters",
                    data_key="headings", data_path="h3"
                )]
        return []


class H3DuplicateRule(BaseSEORuleV2):
    rule_id = "H3_DUPLICATE"
    rule_no = 101
    category = "Headings"
    severity = "high"
    description = "H3 tags should be unique"

    def evaluate(self, normalized, job_id, project_id, url):
        texts = [t.lower() for t in _heading_texts(normalized.get("headings", []), "h3") if t]
        seen = set()
        dupes = []
        for t in texts:
            if t in seen:
                dupes.append(t)
            seen.add(t)
        if dupes:
            return [self.create_issue(
                job_id, project_id, url,
                "Duplicate H3 tags found",
                ", ".join(dupes[:3]), "H3 tags should be unique",
                data_key="headings", data_path="h3"
            )]
        return []


# ═══════════════════════════════════════════════════════════════
# H4–H6 RULES (Rules 102–107)
# ═══════════════════════════════════════════════════════════════

class H4CountRule(BaseSEORuleV2):
    rule_id = "H4_COUNT"
    rule_no = 102
    category = "Headings"
    severity = "medium"
    description = "H4 count should be reasonable (0–20)"

    def evaluate(self, normalized, job_id, project_id, url):
        h4s = _get_headings_by_tag(normalized.get("headings", []), "h4")
        if len(h4s) > 20:
            return [self.create_issue(job_id, project_id, url,
                f"Too many H4 tags ({len(h4s)})", len(h4s), "0–20",
                data_key="headings", data_path="h4")]
        return []


class H4LengthRule(BaseSEORuleV2):
    rule_id = "H4_LENGTH"
    rule_no = 103
    category = "Headings"
    severity = "medium"
    description = "H4 length should be 6–60 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in _get_headings_by_tag(normalized.get("headings", []), "h4"):
            text = h.get("text", "").strip()
            if text and (len(text) < 6 or len(text) > 60):
                return [self.create_issue(job_id, project_id, url,
                    f"H4 length is {len(text)} chars (6–60)", text[:60], "6–60 characters",
                    data_key="headings", data_path="h4")]
        return []


class H5CountRule(BaseSEORuleV2):
    rule_id = "H5_COUNT"
    rule_no = 104
    category = "Headings"
    severity = "medium"
    description = "H5 count should be reasonable (0–25)"

    def evaluate(self, normalized, job_id, project_id, url):
        h5s = _get_headings_by_tag(normalized.get("headings", []), "h5")
        if len(h5s) > 25:
            return [self.create_issue(job_id, project_id, url,
                f"Too many H5 tags ({len(h5s)})", len(h5s), "0–25",
                data_key="headings", data_path="h5")]
        return []


class H5LengthRule(BaseSEORuleV2):
    rule_id = "H5_LENGTH"
    rule_no = 105
    category = "Headings"
    severity = "medium"
    description = "H5 length should be 6–60 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in _get_headings_by_tag(normalized.get("headings", []), "h5"):
            text = h.get("text", "").strip()
            if text and (len(text) < 6 or len(text) > 60):
                return [self.create_issue(job_id, project_id, url,
                    f"H5 length is {len(text)} chars (6–60)", text[:60], "6–60 characters",
                    data_key="headings", data_path="h5")]
        return []


class H6CountRule(BaseSEORuleV2):
    rule_id = "H6_COUNT"
    rule_no = 106
    category = "Headings"
    severity = "medium"
    description = "H6 count should be reasonable (0–30)"

    def evaluate(self, normalized, job_id, project_id, url):
        h6s = _get_headings_by_tag(normalized.get("headings", []), "h6")
        if len(h6s) > 30:
            return [self.create_issue(job_id, project_id, url,
                f"Too many H6 tags ({len(h6s)})", len(h6s), "0–30",
                data_key="headings", data_path="h6")]
        return []


class H6LengthRule(BaseSEORuleV2):
    rule_id = "H6_LENGTH"
    rule_no = 107
    category = "Headings"
    severity = "medium"
    description = "H6 length should be 6–50 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in _get_headings_by_tag(normalized.get("headings", []), "h6"):
            text = h.get("text", "").strip()
            if text and (len(text) < 6 or len(text) > 50):
                return [self.create_issue(job_id, project_id, url,
                    f"H6 length is {len(text)} chars (6–50)", text[:60], "6–50 characters",
                    data_key="headings", data_path="h6")]
        return []


# ═══════════════════════════════════════════════════════════════
# ALL HEADINGS CROSS-CUTTING (Rules 108–121, 141, 229)
# ═══════════════════════════════════════════════════════════════

class HeadingNoEmptyRule(BaseSEORuleV2):
    rule_id = "HEADING_NO_EMPTY"
    rule_no = 108
    category = "Headings"
    severity = "high"
    description = "No empty headings (H1–H6)"

    def evaluate(self, normalized, job_id, project_id, url):
        headings = normalized.get("headings", [])
        empty = [h.get("tag", "?") for h in headings if not h.get("text", "").strip()]
        if empty:
            return [self.create_issue(
                job_id, project_id, url,
                f"{len(empty)} empty heading(s) found",
                ", ".join(empty[:5]), "All headings should have text",
                data_key="headings"
            )]
        return []


class HeadingNoHtmlRule(BaseSEORuleV2):
    rule_id = "HEADING_NO_HTML"
    rule_no = 109
    category = "Headings"
    severity = "high"
    description = "No HTML tags inside any heading"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "")
            if text and re.search(r'<[^>]+>', text):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{h.get('tag', '?')} contains HTML tags",
                    text[:60], "Remove HTML from headings",
                    data_key="headings"
                )]
        return []


class HeadingNotLinkRule(BaseSEORuleV2):
    rule_id = "HEADING_NOT_LINK"
    rule_no = 110
    category = "Headings"
    severity = "medium"
    description = "Headings should not be links"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "").strip()
            if text and text.lower().startswith(("http://", "https://", "www.")):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{h.get('tag', '?')} appears to be a link",
                    text[:60], "Use descriptive text, not URLs",
                    data_key="headings"
                )]
        return []


class HeadingAllCapsRule(BaseSEORuleV2):
    rule_id = "HEADING_ALL_CAPS"
    rule_no = 111
    category = "Headings"
    severity = "medium"
    description = "Avoid ALL CAPS in any heading"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "").strip()
            if text and text == text.upper() and len(text) > 3 and any(c.isalpha() for c in text):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{h.get('tag', '?')} is all uppercase: {text[:40]}",
                    text[:60], "Use sentence case",
                    data_key="headings"
                )]
        return []


class HeadingExcessivePunctuationRule(BaseSEORuleV2):
    rule_id = "HEADING_EXCESSIVE_PUNCTUATION"
    rule_no = 112
    category = "Headings"
    severity = "high"
    description = "Avoid excessive punctuation in headings"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "")
            if text and re.search(r'[!?]{2,}', text):
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{h.get('tag', '?')} has excessive punctuation",
                    text[:60], "Avoid repeated !! or ??",
                    data_key="headings"
                )]
        return []


class HeadingGenericPhraseRule(BaseSEORuleV2):
    rule_id = "HEADING_GENERIC_PHRASE"
    rule_no = 113
    category = "Headings"
    severity = "high"
    description = "Avoid generic phrases in headings"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "").strip().lower()
            if text and text in GENERIC_PHRASES:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{h.get('tag', '?')} uses generic phrase: {text}",
                    text, "Use specific, descriptive headings",
                    data_key="headings"
                )]
        return []


class HeadingContainsKeywordRule(BaseSEORuleV2):
    rule_id = "HEADING_CONTAINS_KEYWORD"
    rule_no = 114
    category = "Headings"
    severity = "high"
    description = "Headings should include primary/secondary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        headings = normalized.get("headings", [])
        if not headings:
            return []
        has_keyword = any(
            keyword.lower() in h.get("text", "").lower()
            for h in headings if h.get("text", "").strip()
        )
        if not has_keyword:
            return [self.create_issue(
                job_id, project_id, url,
                "No heading contains primary keyword",
                f"{len(headings)} headings checked", f"Include: {keyword}",
                data_key="headings"
            )]
        return []


# DISABLED: Cannot be implemented with current normalized data structure.
# Rule is unregistered. See seo_rule_engine.py.
# To re-enable: scraper must provide raw HTML heading position data.
class HeadingKeywordEarlyRule(BaseSEORuleV2):
    rule_id = "HEADING_KEYWORD_EARLY"
    rule_no = 115
    category = "Headings"
    severity = "medium"
    description = "Keyword should appear early in headings"

    def evaluate(self, normalized, job_id, project_id, url):
        # Covered by H1 keyword position rule (87) — skip for all headings
        return []


class HeadingKeywordStuffingRule(BaseSEORuleV2):
    rule_id = "HEADING_KEYWORD_STUFFING"
    rule_no = 116
    category = "Headings"
    severity = "high"
    description = "Avoid keyword stuffing in headings"

    def evaluate(self, normalized, job_id, project_id, url):
        keyword = _keyword_from_context(normalized)
        if not keyword or len(keyword) < 3:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        headings = normalized.get("headings", [])
        total_count = sum(
            h.get("text", "").lower().count(keyword.lower())
            for h in headings
        )
        if total_count >= 6:
            return [self.create_issue(
                job_id, project_id, url,
                f"Keyword '{keyword}' appears {total_count} times across headings",
                total_count, "Avoid excessive keyword repetition",
                data_key="headings"
            )]
        return []


class HeadingVoiceSearchRule(BaseSEORuleV2):
    rule_id = "HEADING_VOICE_SEARCH"
    rule_no = 117
    category = "Headings"
    severity = "info"
    description = "Optimize headings for voice search"

    def evaluate(self, normalized, job_id, project_id, url):
        # Check if any H2/H3 uses question format
        question_starters = ("how", "what", "why", "when", "where", "which", "who", "can", "does", "is")
        headings = normalized.get("headings", [])
        h2_h3 = [h for h in headings if h.get("tag") in ("h2", "h3")]
        if h2_h3:
            has_question = any(
                h.get("text", "").strip().lower().startswith(question_starters)
                for h in h2_h3
            )
            if not has_question:
                return [self.create_issue(
                    job_id, project_id, url,
                    "No H2/H3 uses question format for voice search",
                    f"{len(h2_h3)} H2/H3 headings", "Use question format (How, What, Why…)",
                    data_key="headings"
                )]
        return []


class HeadingUniqueRule(BaseSEORuleV2):
    rule_id = "HEADING_UNIQUE"
    rule_no = 118
    category = "Headings"
    severity = "high"
    description = "All headings should be unique on page"

    def evaluate(self, normalized, job_id, project_id, url):
        headings = normalized.get("headings", [])
        texts = [h.get("text", "").strip().lower() for h in headings if h.get("text", "").strip()]
        seen = set()
        dupes = []
        for t in texts:
            if t in seen:
                dupes.append(t)
            seen.add(t)
        if dupes:
            return [self.create_issue(
                job_id, project_id, url,
                f"{len(dupes)} duplicate heading(s) found",
                ", ".join(list(set(dupes))[:3]), "All headings should be unique",
                data_key="headings"
            )]
        return []


class HeadingReadableRule(BaseSEORuleV2):
    rule_id = "HEADING_READABLE"
    rule_no = 119
    category = "Headings"
    severity = "medium"
    description = "Headings should be readable and scannable"

    def evaluate(self, normalized, job_id, project_id, url):
        limits = {"h1": 10, "h2": 12, "h3": 12, "h4": 10, "h5": 10, "h6": 10}
        for h in normalized.get("headings", []):
            tag = h.get("tag", "")
            text = h.get("text", "").strip()
            limit = limits.get(tag, 12)
            if text and len(text.split()) > limit:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"{tag} has {len(text.split())} words (recommend ≤{limit})",
                    text[:60], f"≤{limit} words for {tag}",
                    data_key="headings"
                )]
        return []


class HeadingSentenceCaseRule(BaseSEORuleV2):
    rule_id = "HEADING_SENTENCE_CASE"
    rule_no = 120
    category = "Headings"
    severity = "medium"
    description = "Headings should maintain sentence case"

    def evaluate(self, normalized, job_id, project_id, url):
        # Check if heading is all lowercase (weird) or all uppercase (caught in 111)
        for h in normalized.get("headings", []):
            text = h.get("text", "").strip()
            if text and len(text) > 3:
                # All lowercase is unusual
                if text == text.lower() and any(c.isalpha() for c in text):
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"{h.get('tag', '?')} is all lowercase: {text[:40]}",
                        text[:60], "Use sentence case or title case",
                        data_key="headings"
                    )]
        return []


class HeadingClickbaitFillerRule(BaseSEORuleV2):
    rule_id = "HEADING_CLICKBAIT_FILLER"
    rule_no = 121
    category = "Headings"
    severity = "high"
    description = "Headings should avoid clickbait and filler words"

    def evaluate(self, normalized, job_id, project_id, url):
        for h in normalized.get("headings", []):
            text = h.get("text", "").strip().lower()
            if text:
                for phrase in CLICKBAIT_WORDS:
                    if phrase in text:
                        return [self.create_issue(
                            job_id, project_id, url,
                            f"{h.get('tag', '?')} contains clickbait language",
                            text[:60], f"Remove: '{phrase}'",
                            data_key="headings"
                        )]
        return []


class H1DiffersFromTitleRule(BaseSEORuleV2):
    rule_id = "H1_DIFFERS_FROM_TITLE"
    rule_no = 141
    category = "Headings"
    severity = "medium"
    description = "H1 should differ from title tag (not identical)"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "").strip().lower()
        h1s = _get_headings_by_tag(normalized.get("headings", []), "h1")
        if title and h1s:
            h1_text = h1s[0].get("text", "").strip().lower()
            if h1_text and h1_text == title:
                return [self.create_issue(
                    job_id, project_id, url,
                    "H1 is identical to title tag",
                    h1_text[:60], "H1 and title should differ slightly",
                    data_key="headings", data_path="h1"
                )]
        return []


class HeadingHierarchyLogicalRule(BaseSEORuleV2):
    rule_id = "HEADING_HIERARCHY_LOGICAL"
    rule_no = 229
    category = "Headings"
    severity = "high"
    description = "Heading hierarchy should be logical (H1→H2→H3)"

    def evaluate(self, normalized, job_id, project_id, url):
        headings = normalized.get("headings", [])
        if not headings:
            return []
        levels = [h.get("level", 0) for h in headings if h.get("level", 0) > 0]
        for i in range(1, len(levels)):
            if levels[i] - levels[i - 1] > 1:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"Heading hierarchy skips levels: H{levels[i-1]} → H{levels[i]}",
                    f"H{levels[i-1]} to H{levels[i]}", "Sequential (H1→H2→H3)",
                    data_key="headings"
                )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_heading_rules(registry):
    """Register all Heading rules"""
    # H1 (84–93)
    registry.register(H1ExactlyOneRule())
    registry.register(H1LengthRule())
    registry.register(H1ContainsKeywordRule())
    registry.register(H1KeywordAtBeginningRule())
    # Rules 88, 89, 90, 91, 93 removed — covered by cross-heading rules 108–121
    registry.register(H1ReadabilityRule())
    # H2 (94–98)
    registry.register(H2CountRule())
    registry.register(H2ContainsKeywordRule())
    registry.register(H2LengthRule())
    registry.register(H2EmptyRule())
    registry.register(H2DuplicateRule())
    # H3 (99–101)
    registry.register(H3CountRule())
    registry.register(H3LengthRule())
    registry.register(H3DuplicateRule())
    # H4–H6 (102–107)
    registry.register(H4CountRule())
    registry.register(H4LengthRule())
    registry.register(H5CountRule())
    registry.register(H5LengthRule())
    registry.register(H6CountRule())
    registry.register(H6LengthRule())
    # Cross-cutting (108–121, 141, 229)
    registry.register(HeadingNoEmptyRule())
    registry.register(HeadingNoHtmlRule())
    registry.register(HeadingNotLinkRule())
    registry.register(HeadingAllCapsRule())
    registry.register(HeadingExcessivePunctuationRule())
    registry.register(HeadingGenericPhraseRule())
    registry.register(HeadingContainsKeywordRule())
    # registry.register(HeadingKeywordEarlyRule())  # DISABLED: rule_no 115
    registry.register(HeadingKeywordStuffingRule())
    registry.register(HeadingVoiceSearchRule())
    registry.register(HeadingUniqueRule())
    registry.register(HeadingReadableRule())
    registry.register(HeadingSentenceCaseRule())
    registry.register(HeadingClickbaitFillerRule())
    registry.register(H1DiffersFromTitleRule())
    registry.register(HeadingHierarchyLogicalRule())
