"""
Validation harness for the normalized extraction architecture.

Run:  python -m scraper.workers.ai.ai_visibility.normalized.validation_harness
(from python_workers/)  — or directly: python validation_harness.py

Tests representative site types and asserts the root-cause fixes hold:
 * shared DOM is never mutated (the original destructive-decompose bug)
 * Elementor div-based nav is detected AND its links counted (consistency)
 * content_depth_score > 0 on a large page
 * FAQ requires genuine Q&A, not just the word "FAQ"
 * topic clusters are not inflated by mere presence of internal links
 * legacy alias keys exist for backward compatibility
"""

from __future__ import annotations

import sys
import os

# Allow running as a plain script.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")))

try:
    from scraper.workers.ai.ai_visibility.normalized import extract_normalized_signals, RenderedDOM
except ImportError:
    sys.path.insert(0, os.path.dirname(__file__))
    from normalizer import extract_normalized_signals
    from rendered_dom import RenderedDOM


# ── fixtures ─────────────────────────────────────────────────────────────────
ELEMENTOR_WP = """
<!DOCTYPE html><html lang="en"><head><title>Acme Services</title>
<meta name="description" content="Acme provides plumbing services">
<link rel="canonical" href="https://acme.com/services">
<script type="application/ld+json">{"@context":"https://schema.org","@graph":[
 {"@type":"Organization","name":"Acme Inc","sameAs":["https://twitter.com/acme"]},
 {"@type":"Service","name":"Plumbing"}]}</script></head>
<body>
<div class="elementor-location-header"><div class="elementor-nav-menu">
  <a href="/">Home</a><a href="/services">Services</a><a href="/about">About</a><a href="/contact">Contact</a>
</div></div>
<main><h1>Plumbing Services</h1>
<section><h2>Emergency Plumbing</h2><p>%s</p>
  <p>We also fix <a href="/services/leaks">leak repair services</a> and offer
  <a href="/services/drains">drain cleaning solutions</a> plus
  <a href="/services/heating">heating system services</a>.</p></section>
<section><h2>What areas do you serve?</h2><p>We serve the greater metro area with same-day service for all plumbing emergencies and repairs.</p></section>
<details><summary>Do you offer warranties?</summary><p>Yes, all work is guaranteed for one year.</p></details>
</main>
<footer class="site-footer"><a href="/privacy">Privacy</a><a href="/terms">Terms</a></footer>
</body></html>
""" % (" ".join(["Acme Inc delivers reliable plumbing."] * 60))

NEXTJS_SSR = """
<!DOCTYPE html><html lang="en"><head><title>Blog Post</title>
<script id="__NEXT_DATA__" type="application/json">{"props":{}}</script>
<script type="application/ld+json">{"@type":"Article","headline":"Guide","author":{"@type":"Person","name":"Jane"}}</script>
</head><body><div id="__next">
<nav><a href="/">Home</a><a href="/blog">Blog</a></nav>
<article><h1>The Complete Guide</h1><p>%s</p></article>
</div></body></html>
""" % (" ".join(["Detailed content here."] * 120))

REACT_CSR_LIGHT = """
<!DOCTYPE html><html><head><title>App</title></head>
<body><div id="root"></div><script src="/bundle.js"></script></body></html>
"""

SHOPIFY = """
<!DOCTYPE html><html lang="en"><head><title>Product - Store</title>
<script type="application/ld+json">{"@type":"Product","name":"Cool Shirt","offers":{"@type":"Offer","price":"20"}}</script>
</head><body>
<header class="site-header"><nav class="navbar"><a href="/">Home</a><a href="/products">Products</a><a href="/cart">Cart</a></nav></header>
<main><h1>Cool Shirt</h1><p>A great shirt. Buy now.</p>
<img src="/shirt.webp" loading="lazy" width="400" height="400" alt="Cool Shirt">
<img src="/a.webp" loading="lazy" alt="a"><img src="/b.webp" loading="lazy" alt="b"><img src="/c.webp" loading="lazy" alt="c">
</main></body></html>
"""

STATIC_HTML = """
<!DOCTYPE html><html lang="en"><head><title>About FAQ</title></head>
<body><h1>About Us</h1>
<p>We mention FAQ in passing but have no real questions here. Just a paragraph.</p>
<a href="/page1">link</a><a href="https://external.com">ext</a>
</body></html>
"""


def _assert(cond, msg, results):
    results.append((bool(cond), msg))


def run():
    results = []

    # 1. Elementor: div-based nav detected + links counted consistently.
    s = extract_normalized_signals(ELEMENTOR_WP, "https://acme.com/services")
    _assert(s.navigation.has_navigation, "Elementor: nav detected", results)
    _assert(s.navigation.nav_link_count == 4, f"Elementor: 4 nav links (got {s.navigation.nav_link_count})", results)
    _assert(s.navigation.has_footer, "Elementor: footer detected", results)
    _assert(s.content.content_depth_score > 0, f"Elementor: depth>0 (got {s.content.content_depth_score})", results)
    _assert(s.content.substantial_sections >= 1, f"Elementor: substantial sections (got {s.content.substantial_sections})", results)
    _assert(s.faq.detected, "Elementor: FAQ detected via real Q&A", results)
    _assert(s.faq.source in ("visible", "both"), f"Elementor: FAQ source visible (got {s.faq.source})", results)
    _assert(s.links.has_topic_clusters, f"Elementor: topic clusters (got {s.links.topic_cluster_links})", results)
    _assert(s.entities.primary_entity == "Acme Inc", f"Elementor: primary entity (got {s.entities.primary_entity!r})", results)
    _assert(s.entities.schema_vs_html_consistent, "Elementor: entity consistent with HTML", results)

    # CRITICAL regression: master DOM not mutated — nav still present after extraction.
    dom = RenderedDOM(ELEMENTOR_WP, "https://acme.com/services")
    _ = dom.content_text()  # triggers content clone + chrome strip
    _ = dom.content_paragraphs()
    _assert(dom.select('[class*="elementor-nav-menu"]'), "No-mutation: nav survives content extraction", results)
    _assert(dom.find_all("footer"), "No-mutation: footer survives content extraction", results)

    # 2. Next.js SSR article.
    s2 = extract_normalized_signals(NEXTJS_SSR, "https://site.com/blog/guide")
    _assert(s2.navigation.has_navigation, "Next.js: semantic nav detected", results)
    _assert(s2.page_type.detected_type == "Article", f"Next.js: Article type (got {s2.page_type.detected_type})", results)
    _assert(s2.content.word_count > 100, f"Next.js: word count (got {s2.content.word_count})", results)

    # 3. React CSR light shell — should not crash, low content, no fake nav.
    s3 = extract_normalized_signals(REACT_CSR_LIGHT, "https://app.com")
    _assert(not s3.navigation.has_navigation, "React shell: no fake nav", results)
    _assert(s3.content.thin_content, "React shell: thin content flagged", results)

    # 4. Shopify product with genuine lazy loading.
    s4 = extract_normalized_signals(SHOPIFY, "https://store.com/products/shirt")
    _assert(s4.page_type.detected_type == "Product", f"Shopify: Product type (got {s4.page_type.detected_type})", results)
    _assert(s4.navigation.has_navigation, "Shopify: navbar detected", results)
    _assert(s4.media.lazy_loading_genuine, f"Shopify: genuine lazy loading (lazy={s4.media.images_lazy}/{s4.media.images_total})", results)
    _assert(s4.media.images_modern_format >= 4, f"Shopify: webp detected (got {s4.media.images_modern_format})", results)

    # 5. Static HTML — FAQ word present but no real Q&A => NOT detected (no false positive).
    s5 = extract_normalized_signals(STATIC_HTML, "https://static.com/about")
    _assert(not s5.faq.detected, "Static: FAQ NOT falsely detected", results)
    _assert(not s5.links.has_topic_clusters, "Static: no topic-cluster inflation", results)
    _assert(s5.links.internal >= 1 and s5.links.external >= 1, "Static: link classification", results)

    # 6. Legacy alias contract present.
    aliases = s.to_legacy_aliases()
    for key in ("content_metrics", "heading_metrics", "faq_metrics", "entity_metrics",
                "unified_entity_graph", "structured_data", "page_type_properties",
                "has_navigation", "paragraph_count", "word_count"):
        _assert(key in aliases, f"Alias: {key} present", results)
    _assert(aliases["faq_metrics"]["faq_detected"] == s.faq.detected, "Alias: faq_detected mapped", results)
    _assert(aliases["entity_metrics"]["entity_per_1000_words"] == s.entities.entity_per_1000_words,
            "Alias: entity_per_1000_words mapped", results)

    # ── report ──
    passed = sum(1 for ok, _ in results if ok)
    print(f"\n{'='*64}\nNORMALIZED EXTRACTION VALIDATION  —  {passed}/{len(results)} passed\n{'='*64}")
    for ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {msg}")
    failed = [m for ok, m in results if not ok]
    if failed:
        print(f"\n{len(failed)} FAILED")
        return 1
    print("\nALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(run())
