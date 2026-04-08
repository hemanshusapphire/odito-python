"""
Title Tag SEO Rules (Rules 1–20)

All rules related to <title> tag validation, keyword placement,
readability, and quality checks.
"""

import re
from ..base_seo_rule import BaseSEORuleV2
from ..utils import safe_str
from ..seo_rule_utils import _keyword_from_context


# ── Helpers ───────────────────────────────────────────────────

GENERIC_TITLE_PHRASES = {
    "home", "homepage", "welcome", "page", "untitled",
    "new page", "test", "default", "main page",
}

FILLER_WORDS = {
    "best", "top", "amazing", "awesome", "incredible",
    "ultimate", "ever", "guaranteed", "unbelievable",
}

CLICKBAIT_WORDS = {
    "you won't believe", "shocking", "mind-blowing",
    "secret", "hack", "insane", "jaw-dropping",
    "what happens next", "number one",
}


def _word_count(text):
    return len(text.split()) if text else 0


# _keyword_from_context imported from seo_rule_utils


# ── Rule 1: Title must not be empty ──────────────────────────

class TitleEmptyRule(BaseSEORuleV2):
    rule_id = "TITLE_EMPTY"
    rule_no = 1
    category = "Title Tag"
    severity = "high"
    description = "Title tag must not be empty"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("title"):
            return [self.create_issue(
                job_id, project_id, url,
                "Title tag is empty or missing",
                None, "A descriptive page title",
                data_key="title"
            )]
        return []


# ── Rule 2: Exactly one title tag per page ───────────────────

# DISABLED: Cannot be implemented with current normalized data structure.
# Rule is unregistered. See seo_rule_engine.py.
# To re-enable: scraper must provide raw HTML title tag count.
class TitleMultipleRule(BaseSEORuleV2):
    rule_id = "TITLE_MULTIPLE"
    rule_no = 2
    category = "Title Tag"
    severity = "high"
    description = "Exactly one title tag per page"

    def evaluate(self, normalized, job_id, project_id, url):
        # This requires raw HTML count — not available in normalized data.
        # If page has a title, we assume one. This is a best-effort check.
        # The scraper extracts only the first title, so we cannot detect >1.
        return []


# ── Rule 3: Title length 10–70 characters ────────────────────

class TitleLengthRule(BaseSEORuleV2):
    rule_id = "TITLE_LENGTH"
    rule_no = 3
    category = "Title Tag"
    severity = "high"
    description = "Title length should be between 10 and 70 characters"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if not title:
            return []  # Rule 1 covers empty
        length = len(title)
        if length < 10 or length > 70:
            return [self.create_issue(
                job_id, project_id, url,
                f"Title length is {length} characters (should be 10–70)",
                length, "10–70 characters",
                data_key="title"
            )]
        return []


# ── Rule 4: No single-word title ─────────────────────────────

class TitleSingleWordRule(BaseSEORuleV2):
    rule_id = "TITLE_SINGLE_WORD"
    rule_no = 4
    category = "Title Tag"
    severity = "high"
    description = "Title should not be a single word"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title and _word_count(title) == 1:
            return [self.create_issue(
                job_id, project_id, url,
                "Title is a single word",
                title, "Multi-word descriptive title",
                data_key="title"
            )]
        return []


# ── Rule 5: No all-uppercase title ───────────────────────────

class TitleAllCapsRule(BaseSEORuleV2):
    rule_id = "TITLE_ALL_CAPS"
    rule_no = 5
    category = "Title Tag"
    severity = "medium"
    description = "Title should not be all uppercase"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title and title == title.upper() and any(c.isalpha() for c in title):
            return [self.create_issue(
                job_id, project_id, url,
                "Title is all uppercase",
                title, "Use sentence case or title case",
                data_key="title"
            )]
        return []


# ── Rule 6: No excessive punctuation ─────────────────────────

class TitleExcessivePunctuationRule(BaseSEORuleV2):
    rule_id = "TITLE_EXCESSIVE_PUNCTUATION"
    rule_no = 6
    category = "Title Tag"
    severity = "high"
    description = "Title should not contain excessive punctuation"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title and re.search(r'[!?]{2,}', title):
            return [self.create_issue(
                job_id, project_id, url,
                "Title contains excessive punctuation",
                title, "Avoid repeated !! or ??",
                data_key="title"
            )]
        return []


# ── Rule 7: No leading/trailing separators ────────────────────

class TitleLeadingTrailingSeparatorRule(BaseSEORuleV2):
    rule_id = "TITLE_LEADING_TRAILING_SEPARATOR"
    rule_no = 7
    category = "Title Tag"
    severity = "medium"
    description = "Title should not start or end with separators"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            stripped = title.strip()
            if stripped.startswith("-") or stripped.endswith("-") or \
               stripped.startswith("|") or stripped.endswith("|") or \
               stripped.startswith("—") or stripped.endswith("—"):
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title starts or ends with a separator",
                    title, "Remove leading/trailing separators (-, |, —)",
                    data_key="title"
                )]
        return []


# ── Rule 8: Avoid generic phrases ─────────────────────────────

class TitleGenericPhraseRule(BaseSEORuleV2):
    rule_id = "TITLE_GENERIC_PHRASE"
    rule_no = 8
    category = "Title Tag"
    severity = "high"
    description = "Title should not use generic phrases"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            title_lower = title.strip().lower()
            if title_lower in GENERIC_TITLE_PHRASES:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title uses a generic phrase",
                    title, "Use a specific, descriptive title",
                    data_key="title"
                )]
        return []


# ── Rule 9: Avoid unnecessary numbers ─────────────────────────

class TitleUnnecessaryNumbersRule(BaseSEORuleV2):
    rule_id = "TITLE_UNNECESSARY_NUMBERS"
    rule_no = 9
    category = "Title Tag"
    severity = "medium"
    description = "Avoid unnecessary numbers in title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            # Flag if title is mostly numbers (>50% digits)
            digit_count = sum(1 for c in title if c.isdigit())
            if digit_count > 0 and digit_count / len(title) > 0.5:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title contains excessive numbers",
                    title, "Minimize unnecessary numbers in title",
                    data_key="title"
                )]
        return []


# ── Rule 10: Must include primary keyword ─────────────────────

class TitleMissingKeywordRule(BaseSEORuleV2):
    rule_id = "TITLE_MISSING_KEYWORD"
    rule_no = 10
    category = "Title Tag"
    severity = "high"
    description = "Title must include primary keyword"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if not title:
            return []
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        if keyword.lower() not in title.lower():
            return [self.create_issue(
                job_id, project_id, url,
                "Title does not contain primary keyword",
                title, f"Include keyword: {keyword}",
                data_key="title"
            )]
        return []


# ── Rule 11: Keyword should appear early ──────────────────────

class TitleKeywordPositionRule(BaseSEORuleV2):
    rule_id = "TITLE_KEYWORD_POSITION"
    rule_no = 11
    category = "Title Tag"
    severity = "high"
    description = "Primary keyword should appear early in title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if not title:
            return []
        keyword = _keyword_from_context(normalized)
        if not keyword:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        pos = title.lower().find(keyword.lower())
        if pos > 0:  # keyword present but not at start
            # Warn if keyword doesn't start within first 30% of title
            if pos > len(title) * 0.3:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Primary keyword appears late in title",
                    f"Position {pos} of {len(title)}",
                    "Keyword should appear early (position 0)",
                    data_key="title"
                )]
        return []


# ── Rule 12: Avoid keyword stuffing ───────────────────────────

class TitleKeywordStuffingRule(BaseSEORuleV2):
    rule_id = "TITLE_KEYWORD_STUFFING"
    rule_no = 12
    category = "Title Tag"
    severity = "high"
    description = "Avoid keyword stuffing in title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if not title:
            return []
        keyword = _keyword_from_context(normalized)
        if not keyword or len(keyword) < 3:
            return []  # KEYWORD_UNAVAILABLE: issue skipped — no target keyword configured
        count = title.lower().count(keyword.lower())
        if count >= 3:
            return [self.create_issue(
                job_id, project_id, url,
                "Title contains keyword stuffing",
                f"'{keyword}' appears {count} times",
                "Use keyword 1–2 times maximum",
                data_key="title"
            )]
        return []


# ── Rule 13: Optimize for voice search ────────────────────────

class TitleVoiceSearchRule(BaseSEORuleV2):
    rule_id = "TITLE_VOICE_SEARCH"
    rule_no = 13
    category = "Title Tag"
    severity = "info"
    description = "Title could be optimized for voice search (question format)"

    def evaluate(self, normalized, job_id, project_id, url):
        # This is a suggestion/warning — we note if the title is NOT
        # in question form, which would be better for voice search.
        # Only warn on informational pages (detected by content length).
        title = normalized.get("title", "")
        content_text = normalized.get("content_text", "")
        if not title:
            return []
        word_count = _word_count(content_text)
        # Only check for informational pages (>500 words)
        if word_count > 500:
            question_starters = ("how", "what", "why", "when", "where", "which", "who", "can", "does", "is")
            if not title.lower().strip().startswith(question_starters):
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title could be optimized for voice search",
                    title, "Consider question format (How, What, Why…)",
                    data_key="title"
                )]
        return []


# ── Rule 14: Avoid vague titles ───────────────────────────────

class TitleVagueRule(BaseSEORuleV2):
    rule_id = "TITLE_VAGUE"
    rule_no = 14
    category = "Title Tag"
    severity = "high"
    description = "Avoid vague, non-descriptive titles"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if not title:
            return []
        vague_patterns = [
            "click here", "read more", "learn more",
            "about us", "our services", "contact us",
            "info", "details", "overview",
        ]
        title_lower = title.strip().lower()
        for pattern in vague_patterns:
            if title_lower == pattern:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title is vague and non-descriptive",
                    title, "Use a specific, keyword-rich title",
                    data_key="title"
                )]
        return []


# ── Rule 15: Title should align with meta title (OG proxy) ───

class TitleMetaAlignmentRule(BaseSEORuleV2):
    rule_id = "TITLE_META_ALIGNMENT"
    rule_no = 15
    category = "Title Tag"
    severity = "medium"
    description = "Title should align with OG title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = safe_str(normalized.get("title", ""))
        og_title = safe_str(normalized.get("og_tags", {}).get("og:title", ""))
        if not title or not og_title:
            return []
        # Simple similarity: check if they share >70% of words
        title_words = set(title.lower().split())
        og_words = set(og_title.lower().split())
        if not title_words or not og_words:
            return []
        intersection = title_words & og_words
        similarity = len(intersection) / max(len(title_words), len(og_words))
        if similarity < 0.7:
            return [self.create_issue(
                job_id, project_id, url,
                "Title and OG title have low similarity",
                f"{round(similarity * 100)}% overlap",
                "≥70% word overlap expected",
                data_key="title"
            )]
        return []


# ── Rule 16: Readable & scannable (≤10 words) ────────────────

class TitleReadabilityRule(BaseSEORuleV2):
    rule_id = "TITLE_READABILITY"
    rule_no = 16
    category = "Title Tag"
    severity = "medium"
    description = "Title should be readable and scannable (≤10 words)"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            wc = _word_count(title)
            if wc > 10:
                return [self.create_issue(
                    job_id, project_id, url,
                    f"Title has {wc} words (recommend ≤10)",
                    str(wc), "≤10 words for readability",
                    data_key="title"
                )]
        return []


# ── Rule 17: Avoid clickbait language ─────────────────────────

class TitleClickbaitRule(BaseSEORuleV2):
    rule_id = "TITLE_CLICKBAIT"
    rule_no = 17
    category = "Title Tag"
    severity = "high"
    description = "Avoid clickbait language in title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            title_lower = title.lower()
            for phrase in CLICKBAIT_WORDS:
                if phrase in title_lower:
                    return [self.create_issue(
                        job_id, project_id, url,
                        "Title contains clickbait language",
                        title, f"Remove clickbait phrase: '{phrase}'",
                        data_key="title"
                    )]
        return []


# ── Rule 18: Avoid filler words ───────────────────────────────

class TitleFillerWordsRule(BaseSEORuleV2):
    rule_id = "TITLE_FILLER_WORDS"
    rule_no = 18
    category = "Title Tag"
    severity = "medium"
    description = "Avoid filler words in title"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            title_words = set(title.lower().split())
            found_fillers = title_words & FILLER_WORDS
            if found_fillers:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title contains filler words",
                    ", ".join(found_fillers), "Remove filler words for clarity",
                    data_key="title"
                )]
        return []


# ── Rule 19: Title should not contain HTML tags ───────────────

class TitleContainsHtmlRule(BaseSEORuleV2):
    rule_id = "TITLE_CONTAINS_HTML"
    rule_no = 19
    category = "Title Tag"
    severity = "high"
    description = "Title should not contain HTML tags"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title and re.search(r'<[^>]+>', title):
            return [self.create_issue(
                job_id, project_id, url,
                "Title contains HTML tags",
                title, "Remove HTML tags from title",
                data_key="title"
            )]
        return []


# ── Rule 20: Title should not be a link ───────────────────────

class TitleIsLinkRule(BaseSEORuleV2):
    rule_id = "TITLE_IS_LINK"
    rule_no = 20
    category = "Title Tag"
    severity = "high"
    description = "Title should not look like a URL/link"

    def evaluate(self, normalized, job_id, project_id, url):
        title = normalized.get("title", "")
        if title:
            stripped = title.strip().lower()
            if stripped.startswith(("http://", "https://", "www.")):
                return [self.create_issue(
                    job_id, project_id, url,
                    "Title appears to be a URL",
                    title, "Use descriptive text, not a URL",
                    data_key="title"
                )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_title_rules(registry):
    """Register all Title Tag category rules"""
    registry.register(TitleEmptyRule())
    # registry.register(TitleMultipleRule())  # DISABLED: rule_no 2
    registry.register(TitleLengthRule())
    registry.register(TitleSingleWordRule())
    registry.register(TitleAllCapsRule())
    registry.register(TitleExcessivePunctuationRule())
    registry.register(TitleLeadingTrailingSeparatorRule())
    registry.register(TitleGenericPhraseRule())
    registry.register(TitleUnnecessaryNumbersRule())
    registry.register(TitleMissingKeywordRule())
    registry.register(TitleKeywordPositionRule())
    registry.register(TitleKeywordStuffingRule())
    registry.register(TitleVoiceSearchRule())
    registry.register(TitleVagueRule())
    registry.register(TitleMetaAlignmentRule())
    registry.register(TitleReadabilityRule())
    registry.register(TitleClickbaitRule())
    registry.register(TitleFillerWordsRule())
    registry.register(TitleContainsHtmlRule())
    registry.register(TitleIsLinkRule())
