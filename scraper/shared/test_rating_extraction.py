"""
Tests for visible rating extraction (rating_extraction.py).

Stdlib unittest only. Run from python_workers/:
    python -m unittest scraper.shared.test_rating_extraction -v
"""

import unittest

from bs4 import BeautifulSoup

from scraper.shared.rating_extraction import extract_rating_signals
from scraper.shared.enhanced_seo_extraction import extract_enhanced_seo_signals


def cands(html):
    return extract_rating_signals(BeautifulSoup(html, "html.parser"))["rating_candidates"]


class Extracts(unittest.TestCase):
    def test_slash_scale_with_review_count(self):
        (c,) = cands("<div><span>Rated 4.8/5 based on 127 reviews</span></div>")
        self.assertEqual((c["ratingValue"], c["bestRating"], c["reviewCount"]), (4.8, 5, 127))
        self.assertNotIn("ratingCount", c)
        self.assertEqual(c["source"], "text")
        self.assertIn("127 reviews", c["evidence"])

    def test_out_of_form_and_thousands_separator(self):
        (c,) = cands("<p>4.9 out of 5 from 1,204 verified customer reviews</p>")
        self.assertEqual((c["ratingValue"], c["bestRating"], c["reviewCount"]), (4.9, 5, 1204))

    def test_non_five_scales(self):
        (c,) = cands("<p>Score 92/100 (48 reviews)</p>")
        self.assertEqual((c["ratingValue"], c["bestRating"], c["reviewCount"]), (92, 100, 48))

    def test_stars_vocabulary_implies_five_scale_and_ratings_count_is_ratingCount(self):
        (c,) = cands("<div>4.7 stars · 310 ratings</div>")
        self.assertEqual((c["ratingValue"], c["bestRating"], c["ratingCount"]), (4.7, 5, 310))
        self.assertNotIn("reviewCount", c)

    def test_value_and_count_in_sibling_elements_use_the_smallest_common_element(self):
        (c,) = cands('<section><div class="r"><b>4.6/5</b><span>(89 reviews)</span></div><p>Unrelated long text about our services and how we work.</p></section>')
        self.assertEqual((c["ratingValue"], c["reviewCount"]), (4.6, 89))
        self.assertLessEqual(len(c["evidence"]), 40)

    def test_the_same_widget_rendered_twice_is_one_candidate(self):
        html = "<div>4.8/5 · 127 reviews</div><div class='m'>4.8/5 · 127 reviews</div>"
        self.assertEqual(len(cands(html)), 1)


class Refuses(unittest.TestCase):
    def test_rating_without_a_count_is_not_a_candidate(self):
        self.assertEqual(cands("<p>Rated 4.9/5 by our clients</p>"), [])

    def test_count_without_a_rating_is_not_a_candidate(self):
        self.assertEqual(cands("<p>Read our 120 reviews from happy customers</p>"), [])

    def test_approximate_counts_are_rejected(self):
        self.assertEqual(cands("<p>4.9/5 from 120+ reviews</p>"), [])
        self.assertEqual(cands("<p>4.9/5 from over 120 reviews</p>"), [])
        self.assertEqual(cands("<p>4.9/5 from more than 120 reviews</p>"), [])

    def test_two_conflicting_ratings_in_one_block_are_ambiguous(self):
        self.assertEqual(cands("<p>4.9/5 on Google and 4.2/5 on Yelp from 88 reviews</p>"), [])

    def test_two_counts_in_one_block_are_ambiguous(self):
        self.assertEqual(cands("<p>4.9/5 from 88 reviews and 12 ratings</p>"), [])

    def test_out_of_range_or_nonsense_values(self):
        self.assertEqual(cands("<p>7.5/5 from 88 reviews</p>"), [])
        self.assertEqual(cands("<p>0/5 from 88 reviews</p>"), [])

    def test_dates_and_fractions_are_not_ratings(self):
        self.assertEqual(cands("<p>Updated 5/5/2024 · 88 reviews</p>"), [])

    def test_prose_about_stars_without_a_rating_scale(self):
        self.assertEqual(cands("<p>We have five stars of service. See our 30 reviews.</p>"), [])

    def test_page_without_ratings(self):
        self.assertEqual(cands("<html><body><h1>Welcome</h1><p>Nothing here.</p></body></html>"), [])

    def test_scripts_are_not_read(self):
        self.assertEqual(cands("<script>var t='4.8/5 from 90 reviews'</script>"), [])


class Microdata(unittest.TestCase):
    def test_existing_microdata_aggregate_rating_is_reported_separately(self):
        html = """
        <div itemscope itemtype="https://schema.org/Service">
          <div itemprop="aggregateRating" itemscope itemtype="https://schema.org/AggregateRating">
            <span itemprop="ratingValue">4.5</span>/<span itemprop="bestRating">5</span>
            <meta itemprop="reviewCount" content="33"/>
          </div>
        </div>"""
        r = extract_rating_signals(BeautifulSoup(html, "html.parser"))
        self.assertEqual(r["microdata_aggregate_rating"], {"ratingValue": "4.5", "reviewCount": "33", "ratingCount": None, "bestRating": "5"})

    def test_no_microdata(self):
        self.assertIsNone(extract_rating_signals(BeautifulSoup("<p>x</p>", "html.parser"))["microdata_aggregate_rating"])


class Integration(unittest.TestCase):
    def test_enhanced_signals_expose_rating_signals_with_extracted_marker(self):
        soup = BeautifulSoup("<div>4.8/5 · 127 reviews</div>", "html.parser")
        out = extract_enhanced_seo_signals(soup, "", "https://x.example/", json_ld_entries=[])
        signals = out["rating_signals"]
        self.assertTrue(signals["rating_extracted"])
        self.assertEqual(signals["rating_candidate_count"], 1)
        self.assertEqual(signals["rating_candidates"][0]["reviewCount"], 127)
        # Existing FAQ signals are untouched.
        self.assertIn("faq_howto_signals", out)


if __name__ == "__main__":
    unittest.main()
