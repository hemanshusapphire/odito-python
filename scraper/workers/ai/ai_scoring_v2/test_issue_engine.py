"""Quick verification of the issue + severity engine. Run: python test_issue_engine.py"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from issue_engine import build_issues, compute_severity, should_create_issue


class _Rule:
    def __init__(self, is_required, description):
        self.is_required = is_required
        self.description = description


_RULES = {
    "schema_valid_jsonld": _Rule(True, "Schema valid JSON-LD"),
    "faq_section_5_to_10_questions": _Rule(False, "FAQ section 5-10 questions"),
    "service_pages_800_words": _Rule(False, "Service pages 800+ words"),
    "geo_coordinates_in_schema": _Rule(False, "GeoCoordinates in schema"),
    "primary_organization_schema": _Rule(True, "Primary Organization schema"),
}


def _lookup(rid):
    return _RULES.get(rid)


def run():
    results = []

    def chk(c, m):
        results.append((bool(c), m))

    # context-aware severity
    chk(compute_severity("schema_valid_jsonld", 10, True, "Service") == "critical",
        "schema_valid_jsonld failing -> critical")
    chk(compute_severity("faq_section_5_to_10_questions", 0, False, "Homepage") == "medium",
        "no FAQ on Homepage -> medium")
    chk(compute_severity("faq_section_5_to_10_questions", 0, False, "Article") == "low",
        "no FAQ on Article -> low")
    chk(compute_severity("service_pages_800_words", 10, False, "Service") == "high",
        "thin Service page -> high")
    chk(compute_severity("primary_organization_schema", 20, True, "Homepage") == "critical",
        "missing Organization -> critical")

    # should_create_issue gate
    chk(should_create_issue(50, True) is True, "required score 50 -> issue")
    chk(should_create_issue(50, False) is False, "optional score 50 -> no issue")
    chk(should_create_issue(90, True) is False, "required score 90 -> no issue")

    # build_issues on a homepage
    rule_breakdown = [
        {"rule_id": "schema_valid_jsonld", "category": "ai_impact", "score": 10},
        {"rule_id": "faq_section_5_to_10_questions", "category": "aeo_score", "score": 0},
        {"rule_id": "geo_coordinates_in_schema", "category": "llm_readiness", "score": 80},  # passes -> excluded
    ]
    page_data = {
        "normalized_signals": {"page_type": {"detected_type": "Homepage"}},
        "content_metrics": {"word_count": 120, "avg_paragraph_length": 200},
        "faq_metrics": {"question_count": 0},
    }
    issues = build_issues(rule_breakdown, page_data, "proj123", "https://x.com/", _lookup, str)

    chk(len(issues) == 2, f"2 issues (passed rule excluded) got {len(issues)}")
    by_id = {i["rule_id"]: i for i in issues}
    chk("geo_coordinates_in_schema" not in by_id, "passed rule not stored")

    schema_issue = by_id["schema_valid_jsonld"]
    chk(schema_issue["title"] == "Invalid JSON-LD structured data", "rich title (not 'Rule X scored Y')")
    chk("Rule " not in schema_issue["title"], "no 'Rule X' text in title")
    chk(schema_issue["severity"] == "critical", "schema issue critical")
    chk(schema_issue["status"] == "failed", "schema issue status failed")
    chk(bool(schema_issue["recommendation"]), "recommendation present")
    chk(schema_issue["message"] == schema_issue["title"], "message alias == title")
    chk(schema_issue["rule_score"] == 10, "rule_score alias present")
    chk(isinstance(schema_issue["evidence"], dict), "evidence dict present")
    chk(schema_issue["confidence"] == 92, "required confidence 92")

    faq_issue = by_id["faq_section_5_to_10_questions"]
    chk(faq_issue["severity"] == "medium", "FAQ on homepage medium")
    chk(faq_issue["detected_value"] == 0, f"FAQ detected_value from signals (got {faq_issue['detected_value']})")
    chk(faq_issue["expected_value"] == "5–10 question FAQ", "FAQ expected_value")

    passed = sum(1 for ok, _ in results if ok)
    print(f"\nISSUE ENGINE VALIDATION — {passed}/{len(results)} passed")
    for ok, m in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {m}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(run())
