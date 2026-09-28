"""
Tests for FAQ Q/A pair extraction (faq_pair_extraction.py).

Stdlib unittest only, matching this repo's convention. No network, no Mongo.

Run from python_workers/:
    python -m unittest scraper.shared.test_faq_pair_extraction -v
"""

import unittest

from bs4 import BeautifulSoup

from scraper.shared.faq_pair_extraction import extract_faq_pairs
from scraper.shared.enhanced_seo_extraction import extract_faq_howto_signals


def pairs(html):
    return extract_faq_pairs(BeautifulSoup(html, "html.parser"))["pairs"]


def qa(html):
    return [(p["question"], p["answer"]) for p in pairs(html)]


class HeadingPairs(unittest.TestCase):
    def test_question_headings_with_paragraph_answers(self):
        html = """
        <h2>Frequently Asked Questions</h2>
        <h3>What is SEO?</h3><p>SEO is the practice of improving search visibility.</p>
        <h3>How long does it take?</h3><p>Usually three to six months.</p><p>Results vary.</p>
        <h2>Contact us</h2><p>Reach out any time.</p>
        """
        self.assertEqual(qa(html), [
            ("What is SEO?", "SEO is the practice of improving search visibility."),
            ("How long does it take?", "Usually three to six months. Results vary."),
        ])

    def test_answer_stops_at_next_heading_and_keeps_full_text(self):
        long_answer = "Word " * 200
        html = f"<h3>Is it long?</h3><p>{long_answer}</p><h3>Next?</h3><p>Short answer here.</p>"
        result = qa(html)
        self.assertEqual(result[0][1], long_answer.strip())  # never truncated
        self.assertEqual(len(result), 2)

    def test_marketing_headings_are_not_questions(self):
        html = "<h2>How We Work</h2><p>We work hard every day.</p><h2>Why Choose Us</h2><p>Because we are good.</p>"
        self.assertEqual(qa(html), [])

    def test_question_start_words_count_only_inside_faq_section(self):
        html = """
        <h2>FAQ</h2>
        <h3>How do I sign up</h3><p>Click the sign up button.</p>
        """
        self.assertEqual(qa(html), [("How do I sign up", "Click the sign up button.")])

    def test_heading_without_answer_is_dropped(self):
        self.assertEqual(qa("<h3>Anyone home?</h3><h3>Second?</h3>"), [])

    def test_wrapped_heading_answer_in_sibling_wrapper(self):
        html = '<div class="q"><h3>Do you ship abroad?</h3></div><div class="a"><p>Yes, worldwide.</p></div>'
        self.assertEqual(qa(html), [("Do you ship abroad?", "Yes, worldwide.")])


class StructuralPairs(unittest.TestCase):
    def test_details_summary(self):
        html = "<details><summary>Can I cancel?</summary><p>Yes, anytime.</p></details>"
        self.assertEqual(qa(html), [("Can I cancel?", "Yes, anytime.")])

    def test_details_without_question_outside_faq_is_ignored(self):
        html = "<details><summary>Read more</summary><p>Hidden text.</p></details>"
        self.assertEqual(qa(html), [])

    def test_details_any_text_inside_faq_container(self):
        html = '<div class="faq-list"><details><summary>Pricing</summary><p>From $10.</p></details></div>'
        self.assertEqual(qa(html), [("Pricing", "From $10.")])

    def test_definition_list(self):
        html = "<dl><dt>What is X?</dt><dd>X is a thing.</dd><dt>What is Y?</dt><dd>Y is another.</dd></dl>"
        self.assertEqual(qa(html), [("What is X?", "X is a thing."), ("What is Y?", "Y is another.")])

    def test_aria_accordion(self):
        html = """
        <div class="faq"><button aria-controls="p1" aria-expanded="false">Where are you based?</button>
        <div id="p1"><p>Mumbai, India.</p></div></div>
        """
        self.assertEqual(qa(html), [("Where are you based?", "Mumbai, India.")])

    def test_nav_toggle_with_aria_controls_is_not_a_question(self):
        html = '<nav><button aria-controls="menu">Menu</button><ul id="menu"><li>Home</li></ul></nav>'
        self.assertEqual(qa(html), [])

    def test_elementor_style_class_pairs(self):
        html = """
        <section class="elementor-widget-faq">
          <div class="elementor-tab-title">Is there a free trial?</div>
          <div class="elementor-tab-content"><p>Yes, 14 days.</p></div>
        </section>
        """
        self.assertEqual(qa(html), [("Is there a free trial?", "Yes, 14 days.")])


class Hygiene(unittest.TestCase):
    def test_text_is_cleaned_not_reworded(self):
        html = "<h3>What&nbsp;is   <em>AEO</em>?</h3><p>It is <strong>answer</strong> engine optimization,\n  yes.</p>"
        (q, a), = qa(html)
        self.assertEqual(q, "What is AEO?")
        self.assertEqual(a, "It is answer engine optimization, yes.")

    def test_inline_markup_does_not_insert_spaces_before_punctuation(self):
        (q, a), = qa("<h3>Why?</h3><p>Because <b>reasons</b>, obviously.</p>")
        self.assertEqual(a, "Because reasons, obviously.")

    def test_duplicates_collapse_and_order_is_document_order(self):
        html = """
        <h3>B question?</h3><p>Answer B.</p>
        <details><summary>A question?</summary>Answer A.</details>
        <h3>b question</h3><p>Duplicate.</p>
        """
        self.assertEqual([p["question"] for p in pairs(html)], ["B question?", "A question?"])

    def test_scripts_and_styles_are_not_answer_text(self):
        (q, a), = qa("<h3>Ok?</h3><p>Fine.<script>alert(1)</script><style>p{}</style></p>")
        self.assertEqual(a, "Fine.")

    def test_pair_cap(self):
        html = "".join(f"<h3>Question number {i}?</h3><p>Answer {i}.</p>" for i in range(80))
        self.assertEqual(len(pairs(html)), 50)

    def test_empty_page(self):
        self.assertEqual(pairs("<html><body><p>Nothing here.</p></body></html>"), [])


class SignalsIntegration(unittest.TestCase):
    def test_faq_howto_signals_exposes_pairs_and_marker(self):
        soup = BeautifulSoup("<h2>FAQ</h2><h3>Is it good?</h3><p>Yes, very good indeed.</p>", "html.parser")
        signals = extract_faq_howto_signals(soup, json_ld_entries=[])
        self.assertTrue(signals["faq_pairs_extracted"])
        self.assertEqual(signals["faq_pair_count"], 1)
        self.assertEqual(signals["faq_pairs"][0]["question"], "Is it good?")
        # Pre-existing counters are untouched.
        self.assertGreaterEqual(signals["faq_section_count"], 1)
        self.assertFalse(signals["faq_schema_present"])


if __name__ == "__main__":
    unittest.main()
