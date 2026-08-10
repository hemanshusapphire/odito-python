"""
AISO — Coverage card rules (10 rules).

AISO-CV1   XML Sitemap Exists
AISO-CV2   Internal Linking Present
AISO-CV3   Minimum Content Depth (utility-page aware)
AISO-CV4   No Orphan Pages
AISO-CV5   Shallow Click Depth
AISO-CV6   Structured Data Type Breadth
AISO-CV7   FAQ / HowTo Schema Coverage
AISO-CV8   Sitemap Breadth Sanity
AISO-CV9   Site-Wide Reachability Ratio
AISO-CV10  URL Parameter Hygiene
"""

from typing import Any
from ...base import BaseRule, RuleResult


class RuleAISOCV1XmlSitemap(BaseRule):
    RULE_ID  = "AISO-CV1"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "XML Sitemap Missing"
    ISSUE_DESCRIPTION = "No XML sitemap was found. AI crawlers and search engines rely on sitemaps to discover and prioritise your content."
    RECOMMENDATION    = "Create an XML sitemap at /sitemap.xml. Reference it in robots.txt with a Sitemap: directive and submit it in Google Search Console."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        sitemap = page.get("technical", {}).get("sitemap", {})
        exists  = bool(sitemap.get("exists", False))
        if exists:
            return self._pass({"sitemap_exists": True, "url": sitemap.get("url")})
        return self._fail({"sitemap_exists": False})


class RuleAISOCV2InternalLinking(BaseRule):
    RULE_ID  = "AISO-CV2"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Insufficient Internal Linking"
    ISSUE_DESCRIPTION = "This page has fewer than 3 internal links, limiting AI crawlers' ability to discover related content and understand topical structure."
    RECOMMENDATION    = "Add at least 3 contextually relevant internal links to related pages on your site."
    EXPECTED_IMPACT   = "medium"
    _MIN = 3

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        count = int(page.get("content", {}).get("internal_link_count", 0))
        if count >= self._MIN:
            return self._pass({"internal_link_count": count, "threshold": self._MIN})
        return self._fail({"internal_link_count": count, "threshold": self._MIN})


class RuleAISOCV3ContentDepth(BaseRule):
    """
    Utility-page aware (added in Coverage V2): pages whose purpose is not to
    carry substantive body content are exempt from the word-count floor
    rather than being unfairly flagged thin.

    Page types use the canonical set from rules/page_type_matrix.py (verified
    against shared/page_type_detector.py): "contact" and "legal" are the real
    values that correspond to contact/privacy/terms/cookie-policy pages —
    there is no dedicated "login" page type in this system, so a page like
    that is classified "generic" and evaluated normally like any other
    unclassified page (no principled way to exempt it without adding a new
    page-type detector, which is out of scope here).
    """
    RULE_ID  = "AISO-CV3"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Insufficient Content Depth"
    ISSUE_DESCRIPTION = "Page body content is under 300 words — too thin for AI systems to extract substantive answers or reliable citations."
    RECOMMENDATION    = "Expand the page to at least 300 words of substantive, topic-relevant body content."
    EXPECTED_IMPACT   = "medium"
    _MIN = 300
    _EXCLUDED_PAGE_TYPES = frozenset({"contact", "legal"})

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        word_count = int(page.get("content", {}).get("word_count", 0))
        page_type = (page.get("page_type") or "generic").strip().lower()

        if page_type in self._EXCLUDED_PAGE_TYPES:
            return self._pass({
                "word_count": word_count,
                "threshold":  self._MIN,
                "excluded_page_type": page_type,
            })

        if word_count >= self._MIN:
            return self._pass({"word_count": word_count, "threshold": self._MIN})
        return self._fail({"word_count": word_count, "threshold": self._MIN})


class RuleAISOCV4NoOrphanPages(BaseRule):
    RULE_ID  = "AISO-CV4"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Page Is Orphaned"
    ISSUE_DESCRIPTION = "No other page on the site links to this page internally, making it unreachable via normal crawling or site navigation."
    RECOMMENDATION    = "Add at least one internal link to this page from a relevant hub, category, or navigation page."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        site_structure = page.get("site_structure") or {}
        if "is_orphan" not in site_structure:
            return self._skip("No crawl graph data available for this page")
        if not site_structure["is_orphan"]:
            return self._pass({"is_orphan": False})
        return self._fail({"is_orphan": True})


class RuleAISOCV5ShallowClickDepth(BaseRule):
    RULE_ID  = "AISO-CV5"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "Page Is Buried Too Deep In Site Structure"
    ISSUE_DESCRIPTION = "This page is more than 3 clicks from the homepage, making it less likely to be discovered and prioritised by AI crawlers."
    RECOMMENDATION    = "Add internal links that bring this page within 3 clicks of the homepage, e.g. from a category or hub page."
    EXPECTED_IMPACT   = "medium"
    _MAX_DEPTH = 3

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        site_structure = page.get("site_structure") or {}
        depth = site_structure.get("click_depth")
        if depth is None:
            return self._skip("No crawl graph data available for this page")
        if depth <= self._MAX_DEPTH:
            return self._pass({"click_depth": depth, "threshold": self._MAX_DEPTH})
        return self._fail({"click_depth": depth, "threshold": self._MAX_DEPTH})


class RuleAISOCV6SchemaTypeBreadth(BaseRule):
    RULE_ID  = "AISO-CV6"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "No Structured Data Types Present"
    ISSUE_DESCRIPTION = "This page has no Schema.org structured data, limiting how AI systems can parse and understand its content."
    RECOMMENDATION    = "Add at least one relevant Schema.org type (e.g. Organization, Article, Product, LocalBusiness) as JSON-LD."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        types_present = (page.get("schema") or {}).get("types_present") or []
        count = len(types_present)
        if count >= 1:
            return self._pass({"types_present": types_present, "count": count})
        return self._fail({"types_present": [], "count": 0})


class RuleAISOCV7FaqHowtoCoverage(BaseRule):
    RULE_ID  = "AISO-CV7"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    ISSUE_TITLE       = "No FAQ or HowTo Schema Present"
    ISSUE_DESCRIPTION = "This page has neither FAQPage nor HowTo structured data, missing an opportunity for AI systems to extract direct, citable answers."
    RECOMMENDATION    = "Where content includes Q&A or step-by-step instructions, mark it up with FAQPage or HowTo schema."
    EXPECTED_IMPACT   = "medium"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        schema = page.get("schema") or {}
        faq_pairs = schema.get("faq_schema_pairs") or []
        howto = schema.get("howto") or {}
        has_faq = len(faq_pairs) > 0
        has_howto = bool(howto.get("present"))
        if has_faq or has_howto:
            return self._pass({"faq_present": has_faq, "howto_present": has_howto})
        return self._fail({"faq_present": False, "howto_present": False})


class RuleAISOCV8SitemapBreadthSanity(BaseRule):
    RULE_ID  = "AISO-CV8"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    SCOPE    = "domain"
    ISSUE_TITLE       = "Sitemap Exists But Is Effectively Empty"
    ISSUE_DESCRIPTION = "The XML sitemap exists but lists fewer than 2 URLs, providing little practical discovery benefit despite being present."
    RECOMMENDATION    = "Regenerate the sitemap to include all indexable pages, not just the homepage."
    EXPECTED_IMPACT   = "medium"
    _MIN_URLS = 2

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        sitemap = (page.get("technical") or {}).get("sitemap") or {}
        exists = bool(sitemap.get("exists"))
        url_count = sitemap.get("url_count")

        if not exists:
            return self._skip("No sitemap present — covered by AISO-CV1")
        if url_count is None:
            return self._skip("Sitemap URL count not available")
        if url_count >= self._MIN_URLS:
            return self._pass({"url_count": url_count, "threshold": self._MIN_URLS})
        return self._fail({"url_count": url_count, "threshold": self._MIN_URLS})


class RuleAISOCV9ReachabilityRatio(BaseRule):
    RULE_ID  = "AISO-CV9"
    HUB, CARD, SEVERITY = "aiso", "coverage", "medium"
    SCOPE    = "domain"
    ISSUE_TITLE       = "Low Site-Wide URL Reachability"
    ISSUE_DESCRIPTION = "Fewer than 85% of discovered URLs on this site are actually reachable (HTTP 200/301/302), meaning a meaningful share of claimed content is broken or unreachable to crawlers."
    RECOMMENDATION    = "Review the URL qualification report for this project and fix or remove URLs that are failing, redirecting incorrectly, or timing out."
    EXPECTED_IMPACT   = "medium"
    _MIN_RATIO = 0.85

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        reach = (page.get("technical") or {}).get("reachability") or {}
        discovered = reach.get("discovered")
        qualified = reach.get("qualified")

        if not discovered:
            return self._skip("No URL qualification data available")

        ratio = (qualified or 0) / discovered
        evidence = {
            "discovered": discovered,
            "qualified":  qualified,
            "ratio":      round(ratio, 4),
            "threshold":  self._MIN_RATIO,
        }
        if ratio >= self._MIN_RATIO:
            return self._pass(evidence)
        return self._fail(evidence)


class RuleAISOCV10UrlParameterHygiene(BaseRule):
    RULE_ID  = "AISO-CV10"
    HUB, CARD, SEVERITY = "aiso", "coverage", "low"
    ISSUE_TITLE       = "URL Contains Parameters or Double Slashes"
    ISSUE_DESCRIPTION = "This page's URL contains query parameters or double slashes, which can create duplicate-content variants and dilute AI citation signal."
    RECOMMENDATION    = "Use clean, static URLs. Canonicalize or redirect parameterized/double-slash variants to a single canonical URL."
    EXPECTED_IMPACT   = "low"

    def evaluate(self, page: dict[str, Any]) -> RuleResult:
        hygiene = (page.get("technical") or {}).get("url_hygiene") or {}
        has_parameters = bool(hygiene.get("has_parameters", False))
        has_double_slash = bool(hygiene.get("has_double_slash", False))

        if not has_parameters and not has_double_slash:
            return self._pass({"has_parameters": False, "has_double_slash": False})
        return self._fail({
            "has_parameters":   has_parameters,
            "parameter_count":  hygiene.get("parameter_count", 0),
            "has_double_slash": has_double_slash,
        })


RULES = [
    RuleAISOCV1XmlSitemap(),
    RuleAISOCV2InternalLinking(),
    RuleAISOCV3ContentDepth(),
    RuleAISOCV4NoOrphanPages(),
    RuleAISOCV5ShallowClickDepth(),
    RuleAISOCV6SchemaTypeBreadth(),
    RuleAISOCV7FaqHowtoCoverage(),
    RuleAISOCV8SitemapBreadthSanity(),
    RuleAISOCV9ReachabilityRatio(),
    RuleAISOCV10UrlParameterHygiene(),
]
