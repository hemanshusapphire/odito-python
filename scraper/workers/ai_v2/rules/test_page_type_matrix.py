"""
Regression coverage for should_skip()'s generic-page gating bug.

Root cause: SKIP_MATRIX explicitly lists "generic" in the skip set for
GEO-S1 through GEO-S5 (the schema-coverage rules — Service/Article/FAQPage/
Product/Organization), but the gate only ever consulted SKIP_MATRIX when
`page_type != "generic"`. A generic page therefore fell straight through to
the LocalBusiness-only fallback (which doesn't apply to S1-S5), so these
five rules ran un-gated on every generic page — confirmed against real
production data: on one real audited project, GEO-S4 (Product schema,
applicable only to 'product' pages) had 118 FAIL issues recorded while 0
pages were actually typed 'product' in that same job's ai_pages, because
~118 'generic'-typed pages were incorrectly evaluated and failed.

`page_type` on ai_pages comes from `seo_page_data.page_type`
(see extractor.py) and is "generic" whenever that upstream field is
absent/unrecognized — routine, not a rare edge case.

Run from python_workers/:
    python -m unittest scraper.workers.ai_v2.rules.test_page_type_matrix -v
"""

import unittest

from scraper.workers.ai_v2.rules.page_type_matrix import (
    should_skip,
    SKIP_MATRIX,
    GEO_LOCALBUSINESS_RULES,
)

GEO_SCHEMA_RULES = ["GEO-S1", "GEO-S2", "GEO-S3", "GEO-S4", "GEO-S5"]

# Each rule's one genuinely-applicable page type (the type NOT in its skip set).
APPLICABLE_TYPE = {
    "GEO-S1": "service",
    "GEO-S2": "article",   # 'blog' is the other applicable type
    "GEO-S3": "faq",
    "GEO-S4": "product",
    "GEO-S5": "about",
}


class TestGeoSchemaRulesSkipGenericPages(unittest.TestCase):
    """The bug: these 5 rules must skip 'generic' pages, and previously didn't."""

    def test_each_geo_schema_rule_skips_a_generic_page(self):
        for rule_id in GEO_SCHEMA_RULES:
            with self.subTest(rule_id=rule_id):
                skip, reason = should_skip(rule_id, "generic", page={"schema": {}})
                self.assertTrue(skip, f"{rule_id} must skip a generic page")
                self.assertIn("generic", reason.lower())

    def test_each_geo_schema_rule_still_evaluates_its_own_applicable_type(self):
        # The fix must not accidentally over-skip the rule's real target type.
        for rule_id, page_type in APPLICABLE_TYPE.items():
            with self.subTest(rule_id=rule_id, page_type=page_type):
                skip, _ = should_skip(rule_id, page_type, page={"schema": {}})
                self.assertFalse(skip, f"{rule_id} must still evaluate on '{page_type}' pages")

    def test_geo_s2_also_evaluates_blog_pages(self):
        skip, _ = should_skip("GEO-S2", "blog", page={"schema": {}})
        self.assertFalse(skip)

    def test_each_geo_schema_rule_still_skips_an_unrelated_named_type(self):
        # e.g. GEO-S4 (Product) must still skip a 'contact' page, same as before the fix.
        for rule_id in GEO_SCHEMA_RULES:
            with self.subTest(rule_id=rule_id):
                skip, _ = should_skip(rule_id, "contact", page={"schema": {}})
                self.assertTrue(skip)


class TestLocalBusinessRulesUnaffectedByFix(unittest.TestCase):
    """These rules' skip sets never included "generic" — behavior must be identical."""

    def test_lb_rule_on_generic_page_without_local_business_schema_skips(self):
        for rule_id in sorted(GEO_LOCALBUSINESS_RULES):
            with self.subTest(rule_id=rule_id):
                skip, reason = should_skip(
                    rule_id, "generic", page={"schema": {"local_business": {"present": False}}}
                )
                self.assertTrue(skip)
                self.assertIn("localbusiness", reason.lower())

    def test_lb_rule_on_generic_page_with_local_business_schema_evaluates(self):
        for rule_id in sorted(GEO_LOCALBUSINESS_RULES):
            with self.subTest(rule_id=rule_id):
                skip, _ = should_skip(
                    rule_id, "generic", page={"schema": {"local_business": {"present": True}}}
                )
                self.assertFalse(skip)

    def test_lb_rule_on_its_applicable_named_type_evaluates(self):
        skip, _ = should_skip("GEO-059", "homepage", page={"schema": {}})
        self.assertFalse(skip)


class TestDomainScopeRulesBypassPageTypeGate(unittest.TestCase):
    def test_domain_scope_never_skipped_by_page_type(self):
        skip, reason = should_skip("GEO-065", "generic", page={}, scope="domain")
        self.assertFalse(skip)
        self.assertEqual(reason, "")


if __name__ == "__main__":
    unittest.main()
