"""
AEO — Answer Readiness card rules (3 rules).

AEO-046  Direct Answer In First 60 Words
AEO-050  No Long Intro Before Answer
AEO-053  No AI Filler Phrases
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAEO046DirectAnswer(BaseRule):
    RULE_ID  = "AEO-046"
    HUB, CARD, SEVERITY = "aeo", "answer_readiness", "critical"
    ISSUE_TITLE       = "No Direct Answer in First 60 Words"
    ISSUE_DESCRIPTION = "This page does not deliver a direct answer within the first 60 words of body content. AI extraction systems are unlikely to select it for answer-layer citations."
    RECOMMENDATION    = "Restructure the opening paragraph to lead with the key answer or definition. Move all context and background after the first 2-3 substantive sentences."
    EXPECTED_IMPACT   = "critical"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        content = page.get("content", {})
        has_answer   = bool(content.get("has_direct_answer", False))
        first_60     = content.get("first_60_words", "")
        if has_answer:
            return self._pass({
                "has_direct_answer": True,
                "first_60_words_preview": first_60[:120],
            })
        return self._fail({
            "has_direct_answer": False,
            "first_60_words_preview": first_60[:120],
            "first_answer_word_position": content.get("intro_word_count", 0),
        })


class RuleAEO050NoLongIntro(BaseRule):
    RULE_ID  = "AEO-050"
    HUB, CARD, SEVERITY = "aeo", "answer_readiness", "high"
    ISSUE_TITLE       = "Answer Buried Behind Long Introduction"
    ISSUE_DESCRIPTION = "The page opens with more than 100 words of introductory content before delivering any substantive answer. AI systems weight early content most heavily for extraction."
    RECOMMENDATION    = "Move the key answer or primary claim to the top of the page. Context, background, and brand narrative should follow the answer, not precede it."
    EXPECTED_IMPACT   = "high"
    _MAX_INTRO = 100

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        content     = page.get("content", {})
        intro_words = int(content.get("intro_word_count", 0))
        preview     = content.get("first_100_words", "")
        if intro_words <= self._MAX_INTRO:
            return self._pass({"intro_word_count": intro_words, "threshold": self._MAX_INTRO})
        return self._fail({
            "intro_word_count": intro_words,
            "threshold": self._MAX_INTRO,
            "intro_preview": preview[:120],
        })


class RuleAEO053NoFillerPhrases(BaseRule):
    RULE_ID  = "AEO-053"
    HUB, CARD, SEVERITY = "aeo", "answer_readiness", "medium"
    ISSUE_TITLE       = "AI Filler Phrases Detected"
    ISSUE_DESCRIPTION = "AI filler phrases were found in this page's content. These phrases signal padding and low-quality writing to AI content evaluators, reducing citation likelihood."
    RECOMMENDATION    = "Remove each flagged filler phrase and replace with direct, specific statements. The evidence list contains the exact phrases found."
    EXPECTED_IMPACT   = "medium"
    _MAX_FILLERS = 2

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        filler = page.get("content", {}).get("filler_phrases", {})
        count  = int(filler.get("count", 0))
        found  = filler.get("found", [])
        if count <= self._MAX_FILLERS:
            return self._pass({"filler_count": count, "threshold": self._MAX_FILLERS})
        return self._fail({
            "filler_count": count,
            "threshold": self._MAX_FILLERS,
            "examples": found[:5],
        })


RULES = [
    RuleAEO046DirectAnswer(),
    RuleAEO050NoLongIntro(),
    RuleAEO053NoFillerPhrases(),
]
