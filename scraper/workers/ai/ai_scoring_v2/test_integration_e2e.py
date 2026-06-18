"""
End-to-end integration test for the refactored pipeline:

    raw_html -> extract_normalized_signals -> to_legacy_aliases (page_data)
             -> ScoringEngine.score_page -> build_issues

Run from python_workers/:
    python scraper/workers/ai/ai_scoring_v2/test_integration_e2e.py

Verifies the extraction->scoring->issue contract holds: the normalized aliases
feed the real scoring rules without error, scores are produced, and rich issues
(not "Rule X scored Y") come out the other end.
"""
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
sys.path.insert(0, ROOT)

from scraper.workers.ai.ai_visibility.normalized import extract_normalized_signals
from scraper.workers.ai.ai_scoring_v2.rule_registry import RuleRegistry
from scraper.workers.ai.ai_scoring_v2.scoring_engine import ScoringEngine
from scraper.workers.ai.ai_scoring_v2.categories.ai_impact import register_ai_impact_rules
from scraper.workers.ai.ai_scoring_v2.categories.citation_probability import register_citation_probability_rules
from scraper.workers.ai.ai_scoring_v2.categories.llm_readiness import register_llm_readiness_rules
from scraper.workers.ai.ai_scoring_v2.categories.aeo_score import register_aeo_score_rules
from scraper.workers.ai.ai_scoring_v2.categories.topical_authority import register_topical_authority_rules
from scraper.workers.ai.ai_scoring_v2.categories.voice_intent import register_voice_intent_rules
from scraper.workers.ai.ai_scoring_v2.issue_engine import build_issues

HTML = """
<!DOCTYPE html><html lang="en"><head><title>Acme Plumbing Services</title>
<meta name="description" content="Acme provides reliable plumbing services across the metro area.">
<link rel="canonical" href="https://acme.com/services">
<script type="application/ld+json">{"@context":"https://schema.org","@graph":[
 {"@type":"Organization","name":"Acme Inc","url":"https://acme.com","sameAs":["https://twitter.com/acme"]},
 {"@type":"Service","name":"Plumbing"}]}</script></head>
<body>
<header class="elementor-location-header"><div class="elementor-nav-menu">
  <a href="/">Home</a><a href="/services">Services</a><a href="/about">About</a><a href="/contact">Contact</a>
</div></header>
<main><h1>Plumbing Services</h1>
<section><h2>Emergency Plumbing</h2><p>%s</p>
  <p>We provide <a href="/services/leaks">leak repair services</a>,
  <a href="/services/drains">drain cleaning services</a> and
  <a href="/services/heating">heating system services</a>.</p></section>
<section><h2>What areas do you serve?</h2><p>We serve the greater metro area with same-day service.</p></section>
</main>
<footer class="site-footer"><a href="/privacy">Privacy</a></footer>
</body></html>
""" % (" ".join(["Acme Inc delivers reliable plumbing services."] * 70))


class _Job:
    projectId = "507f1f77bcf86cd799439011"


def run():
    results = []

    def chk(c, m):
        results.append((bool(c), m))

    # 1. Extraction -> aliases
    signals = extract_normalized_signals(HTML, "https://acme.com/services")
    page_data = signals.to_legacy_aliases()
    page_data["url"] = "https://acme.com/services"
    page_data["normalized_signals"] = signals.to_dict()["normalized_signals"]
    chk(page_data["content_metrics"]["word_count"] > 300, "extraction: word_count carried into aliases")
    chk(page_data["page_type_properties"]["detected_type"] == "Service", "extraction: page type Service")

    # 2. Register rules + score
    registry = RuleRegistry()
    for reg in (register_ai_impact_rules, register_citation_probability_rules,
                register_llm_readiness_rules, register_aeo_score_rules,
                register_topical_authority_rules, register_voice_intent_rules):
        reg(registry)
    engine = ScoringEngine(registry)

    page_score = engine.score_page(page_data)
    chk(0 <= page_score["page_ai_score"] <= 100, f"scoring: final score in range ({page_score['page_ai_score']})")
    chk(len(page_score["category_scores"]) == 6, "scoring: 6 category scores")
    chk(len(page_score["rule_breakdown"]) > 0, "scoring: rule_breakdown produced in-memory")

    # 3. Build issues (rich)
    rule_lookup = registry.get_rule if hasattr(registry, "get_rule") else (lambda r: None)

    def safe_lookup(rid):
        try:
            return registry.get_rule(rid)
        except Exception:
            return None

    issues = build_issues(page_score["rule_breakdown"], page_data, _Job.projectId,
                          page_data["url"], safe_lookup, str)
    chk(len(issues) >= 1, f"issues: produced ({len(issues)})")
    if issues:
        sample = issues[0]
        chk("Rule " not in sample["title"], "issues: no 'Rule X scored Y' titles")
        chk(bool(sample["recommendation"]), "issues: recommendation present")
        chk(sample["severity"] in ("critical", "high", "medium", "low"), "issues: valid severity")
        chk(sample["message"] == sample["title"], "issues: message alias == title")
        chk("page_type" in sample["evidence"], "issues: evidence includes page_type")

    # all failing rules must be genuine (score < threshold), none for passing rules
    for iss in issues:
        chk(iss["rule_score"] < 75, f"issues: {iss['rule_id']} is genuinely failing ({iss['rule_score']})")

    passed = sum(1 for ok, _ in results if ok)
    print(f"\nE2E INTEGRATION — {passed}/{len(results)} passed")
    for ok, m in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {m}")
    print(f"\nSample issues ({min(len(issues),3)} of {len(issues)}):")
    for iss in issues[:3]:
        print(f"  - [{iss['severity']}] {iss['title']}  ({iss['rule_id']})")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(run())
