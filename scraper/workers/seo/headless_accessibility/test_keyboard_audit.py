"""Tests for the v2 keyboard/focus audit (keyboard_audit.py + a11y_audit.js).

Stdlib unittest only. Two layers:

  * Pure decision logic (focus-indicator verdicts, trap analysis/classification,
    colour maths) — no browser.
  * Browser behaviour — real headless Chromium against small fixture pages
    (auto-skipped when Playwright/Chromium is unavailable).

Run from python_workers/:
    python -m unittest scraper.workers.seo.headless_accessibility.test_keyboard_audit -v
"""

import unittest

from scraper.workers.seo.headless_accessibility import keyboard_audit as ka
from scraper.workers.seo.headless_accessibility.keyboard_audit import (
    analyze_traversal,
    classify_trap,
    contrast_ratio,
    evaluate_focus_indicator,
    parse_color,
)

NONE_STYLE = {
    "outlineStyle": "none", "outlineWidth": "0px", "outlineColor": "rgb(0, 0, 0)", "outlineOffset": "0px",
    "boxShadow": "none", "border": ["0px none rgb(0, 0, 0)"] * 4, "backgroundColor": "rgba(0, 0, 0, 0)",
    "backgroundImage": "none", "color": "rgb(0, 0, 238)", "textDecorationLine": "none", "textDecorationColor": "rgb(0, 0, 238)",
    "transform": "none", "filter": "none", "opacity": "1", "before": None, "after": None,
}


def style(**over):
    s = dict(NONE_STYLE)
    s.update(over)
    return s


# ═══════════════════════════════════════════════════════════════════════════
# Pure logic
# ═══════════════════════════════════════════════════════════════════════════

class ColorMathTests(unittest.TestCase):
    def test_parse_rgb_and_rgba(self):
        self.assertEqual(parse_color("rgb(1, 2, 3)"), (1.0, 2.0, 3.0, 1.0))
        self.assertEqual(parse_color("rgba(1, 2, 3, 0.5)"), (1.0, 2.0, 3.0, 0.5))
        self.assertEqual(parse_color("rgba(0, 0, 0, 0)")[3], 0.0)

    def test_unparseable_is_none(self):
        for v in (None, "", "transparent", "red", 5):
            self.assertIsNone(parse_color(v))

    def test_contrast_ratio_known_values(self):
        self.assertEqual(contrast_ratio(parse_color("rgb(0,0,0)"), parse_color("rgb(255,255,255)")), 21.0)
        self.assertEqual(contrast_ratio(parse_color("rgb(255,255,255)"), parse_color("rgb(255,255,255)")), 1.0)

    def test_alpha_is_composited_over_background(self):
        # 50% black over white behaves like mid grey (ratio well below 21:1)
        self.assertLess(contrast_ratio(parse_color("rgba(0,0,0,0.5)"), parse_color("rgb(255,255,255)")), 6)


class FocusIndicatorTests(unittest.TestCase):
    def test_no_change_on_focus_is_missing(self):
        r = evaluate_focus_indicator(style(), style())
        self.assertEqual(r["status"], "missing")
        self.assertEqual(r["signals"], [])

    def test_outline_none_alone_is_not_a_failure_when_a_box_shadow_appears(self):
        after = style(boxShadow="rgb(0, 95, 204) 0px 0px 0px 3px")
        r = evaluate_focus_indicator(style(), after)
        self.assertEqual(r["status"], "present")
        self.assertIn("box-shadow", r["signals"])

    def test_outline_appears(self):
        after = style(outlineStyle="solid", outlineWidth="2px", outlineColor="rgb(0, 95, 204)")
        r = evaluate_focus_indicator(style(), after)
        self.assertEqual((r["status"], r["signals"]), ("present", ["outline"]))

    def test_chrome_default_focus_ring_counts(self):
        after = style(outlineStyle="auto", outlineWidth="1px", outlineColor="rgb(16, 16, 16)")
        self.assertEqual(evaluate_focus_indicator(style(), after)["status"], "present")

    def test_permanent_outline_that_does_not_change_is_not_a_focus_indicator(self):
        same = style(outlineStyle="solid", outlineWidth="2px", outlineColor="rgb(0, 95, 204)")
        self.assertEqual(evaluate_focus_indicator(same, dict(same))["status"], "missing")

    def test_border_colour_change(self):
        before = style(border=["1px solid rgb(200, 200, 200)"] * 4)
        after = style(border=["1px solid rgb(0, 95, 204)"] * 4)
        self.assertIn("border", evaluate_focus_indicator(before, after)["signals"])

    def test_background_colour_change(self):
        r = evaluate_focus_indicator(style(), style(backgroundColor="rgb(255, 255, 0)"))
        self.assertEqual((r["status"], r["signals"]), ("present", ["background"]))

    def test_text_colour_change_and_underline_and_transform(self):
        self.assertIn("color", evaluate_focus_indicator(style(), style(color="rgb(200, 0, 0)"))["signals"])
        self.assertIn("text-decoration", evaluate_focus_indicator(style(), style(textDecorationLine="underline"))["signals"])
        self.assertIn("transform", evaluate_focus_indicator(style(), style(transform="matrix(1.05, 0, 0, 1.05, 0, 0)"))["signals"])

    def test_pseudo_element_change(self):
        after = style(after={"bg": "rgb(0, 95, 204)", "border": "0px none", "shadow": "none", "outline": "", "opacity": "1", "transform": "none", "w": "100%", "h": "2px"})
        self.assertIn("pseudo-element", evaluate_focus_indicator(style(), after)["signals"])

    def test_transparent_or_zero_size_shadow_is_not_an_indicator(self):
        after = style(boxShadow="rgba(0, 0, 0, 0) 0px 0px 0px 0px")
        self.assertEqual(evaluate_focus_indicator(style(), after)["status"], "missing")

    def test_low_contrast_outline_only_is_weak(self):
        after = style(outlineStyle="solid", outlineWidth="2px", outlineColor="rgb(250, 250, 250)")
        r = evaluate_focus_indicator(style(), after, background="rgb(255, 255, 255)")
        self.assertEqual(r["status"], "weak")
        self.assertEqual(r["reason"], "low_contrast")
        self.assertLess(r["contrastRatio"], 3)

    def test_high_contrast_outline_is_present_and_a_low_contrast_outline_with_a_background_change_is_present(self):
        good = style(outlineStyle="solid", outlineWidth="2px", outlineColor="rgb(0, 0, 0)")
        self.assertEqual(evaluate_focus_indicator(style(), good, background="rgb(255, 255, 255)")["status"], "present")
        mixed = style(outlineStyle="solid", outlineWidth="2px", outlineColor="rgb(250, 250, 250)", backgroundColor="rgb(255, 255, 0)")
        self.assertEqual(evaluate_focus_indicator(style(), mixed, background="rgb(255, 255, 255)")["status"], "present")

    def test_not_visible_and_unknown(self):
        self.assertEqual(evaluate_focus_indicator(style(), style(), visible=False)["status"], "not_visible")
        self.assertEqual(evaluate_focus_indicator(None, style())["status"], "unknown")
        self.assertEqual(evaluate_focus_indicator(style(), None)["status"], "unknown")


def steps(*ids, body=False):
    out = [{"kind": "element", "id": i} for i in ids]
    if body:
        out.append({"kind": "body"})
    return out


class TraversalTests(unittest.TestCase):
    def test_normal_page_ending_on_body_is_not_a_trap(self):
        r = analyze_traversal(steps(1, 2, 3, 4, 5, body=True), {1, 2, 3, 4, 5})
        self.assertFalse(r["detected"])
        self.assertTrue(r["exited"])

    def test_v1_regression_three_different_links_without_ids_are_not_a_trap(self):
        """v1 compared tag+id ('a' == 'a') and called five consecutive id-less links a focus trap."""
        r = analyze_traversal(steps(11, 12, 13, 14, 15, body=True), {11, 12, 13, 14, 15})
        self.assertFalse(r["detected"])

    def test_same_element_three_times_in_a_row_is_stuck(self):
        r = analyze_traversal(steps(1, 2, 2, 2), {1, 2, 3})
        self.assertTrue(r["detected"])
        self.assertEqual((r["type"], r["elementIds"]), ("stuck", [2]))

    def test_two_consecutive_repeats_are_tolerated(self):
        self.assertFalse(analyze_traversal(steps(1, 2, 2, 3, body=True), {1, 2, 3})["detected"])

    def test_mid_sequence_revisit_is_a_cycle_and_names_the_loop(self):
        r = analyze_traversal(steps(1, 2, 3, 4, 5, 3), {1, 2, 3, 4, 5, 6, 7})
        self.assertTrue(r["detected"])
        self.assertEqual(r["type"], "cycle")
        self.assertEqual(r["elementIds"], [3, 4, 5])
        self.assertEqual(r["cycleLength"], 3)

    def test_wrap_back_to_the_first_element_after_covering_the_page_is_normal(self):
        r = analyze_traversal(steps(1, 2, 3, 4, 1), {1, 2, 3, 4})
        self.assertFalse(r["detected"])
        self.assertTrue(r["wrapped"])

    def test_wrap_to_first_after_only_a_small_loop_with_many_unvisited_is_a_trap(self):
        r = analyze_traversal(steps(1, 2, 1), set(range(1, 21)))
        self.assertTrue(r["detected"])
        self.assertEqual(r["elementIds"], [1, 2])

    def test_return_to_first_with_even_one_unvisited_visible_element_is_a_trap(self):
        r = analyze_traversal(steps(1, 2, 1), {1, 2, 3})
        self.assertTrue(r["detected"])
        self.assertEqual(r["elementIds"], [1, 2])

    def test_a_loop_that_covers_every_focusable_element_is_treated_as_a_wrap(self):
        r = analyze_traversal(steps(1, 2, 3, 1), {1, 2, 3})
        self.assertFalse(r["detected"])
        self.assertTrue(r["wrapped"])

    def test_iframe_and_media_hosts_repeating_are_not_stuck(self):
        seq = [{"kind": "element", "id": 1}, {"kind": "iframe", "id": 2}, {"kind": "iframe", "id": 2}, {"kind": "iframe", "id": 2}, {"kind": "element", "id": 3}, {"kind": "body"}]
        self.assertFalse(analyze_traversal(seq, {1, 2, 3})["detected"])
        media = [{"kind": "media", "id": 5}] * 4 + [{"kind": "body"}]
        self.assertFalse(analyze_traversal(media, {5})["detected"])

    def test_leading_body_steps_before_any_element_are_ignored(self):
        r = analyze_traversal([{"kind": "body"}, {"kind": "element", "id": 1}, {"kind": "element", "id": 2}, {"kind": "body"}], {1, 2})
        self.assertFalse(r["detected"])
        self.assertTrue(r["exited"])

    def test_truncated_traversal_is_reported_and_is_not_a_trap(self):
        r = analyze_traversal(steps(1, 2, 3), set(range(1, 100)), truncated=True)
        self.assertFalse(r["detected"])
        self.assertTrue(r["truncated"])

    def test_empty(self):
        r = analyze_traversal([], set())
        self.assertFalse(r["detected"])
        self.assertIsNone(r["firstId"])


class TrapClassificationTests(unittest.TestCase):
    ANALYSIS = {"detected": True, "type": "cycle", "cycleLength": 3, "elementIds": [1, 2, 3]}

    def probe(self, **over):
        p = {"selector": "div.modal", "tag": "div", "role": "dialog", "ariaModal": True, "isDialogLike": True, "isWidgetLike": False,
             "visible": True, "hasCloseControl": True, "closeControl": {"selector": "button.close", "name": "Close"}, "focusableInside": 3}
        p.update(over)
        return p

    def test_no_trap_detected(self):
        self.assertEqual(classify_trap({"detected": False})["verdict"], "none")

    def test_visible_modal_with_close_button_is_intentional(self):
        r = classify_trap(self.ANALYSIS, self.probe())
        self.assertEqual(r["verdict"], "intentional_modal")
        self.assertTrue(r["intentional"])

    def test_visible_modal_that_closes_on_escape_is_intentional_even_without_a_close_button(self):
        r = classify_trap(self.ANALYSIS, self.probe(hasCloseControl=False, closeControl=None), {"closed": True})
        self.assertEqual(r["verdict"], "intentional_modal")
        self.assertTrue(r["container"]["escapeCloses"])

    def test_visible_modal_with_no_way_out_is_a_failure(self):
        r = classify_trap(self.ANALYSIS, self.probe(hasCloseControl=False, closeControl=None), {"closed": False})
        self.assertEqual(r["verdict"], "modal_without_exit")
        self.assertFalse(r["intentional"])

    def test_popup_widget_with_close_is_intentional(self):
        r = classify_trap(self.ANALYSIS, self.probe(isDialogLike=False, ariaModal=False, role=None, isWidgetLike=True))
        self.assertEqual(r["verdict"], "intentional_widget")

    def test_hidden_container_trap_is_a_failure(self):
        r = classify_trap(self.ANALYSIS, self.probe(visible=False))
        self.assertEqual(r["verdict"], "hidden_container_trap")
        self.assertFalse(r["intentional"])
        self.assertIn("not visible", r["suspectedCause"])

    def test_trap_in_a_plain_container_is_unintended(self):
        r = classify_trap(self.ANALYSIS, self.probe(isDialogLike=False, ariaModal=False, role=None, isWidgetLike=False, hasCloseControl=False))
        self.assertEqual(r["verdict"], "unintended")
        self.assertFalse(r["intentional"])

    def test_stuck_focus_names_the_key_handler_cause(self):
        r = classify_trap({"detected": True, "type": "stuck", "cycleLength": 1, "elementIds": [2]}, self.probe(isDialogLike=False, ariaModal=False, role=None, isWidgetLike=False))
        self.assertIn("intercepting Tab", r["suspectedCause"])

    def test_unidentifiable_container_is_unintended_and_says_so(self):
        r = classify_trap(self.ANALYSIS, None)
        self.assertEqual(r["verdict"], "unintended")
        self.assertIsNone(r["container"])

    def test_focus_return_to_trigger_is_never_claimed_because_it_is_not_tested(self):
        self.assertIsNone(classify_trap(self.ANALYSIS, self.probe())["focusReturnsToTrigger"])


# ═══════════════════════════════════════════════════════════════════════════
# Real Chromium
# ═══════════════════════════════════════════════════════════════════════════

try:
    from playwright.async_api import async_playwright
    _HAVE_PLAYWRIGHT = True
except Exception:  # pragma: no cover
    _HAVE_PLAYWRIGHT = False


@unittest.skipUnless(_HAVE_PLAYWRIGHT, "playwright not installed")
class BrowserAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._pw = await async_playwright().start()
        try:
            self.browser = await self._pw.chromium.launch(headless=True, args=["--no-sandbox"])
        except Exception as exc:  # browser binary missing
            await self._pw.stop()
            self.skipTest(f"chromium unavailable: {exc}")
        self.context = await self.browser.new_context(viewport={"width": 1200, "height": 800})
        self.page = await self.context.new_page()

    async def asyncTearDown(self):
        await self.browser.close()
        await self._pw.stop()

    async def audit(self, html, **kw):
        await self.page.set_content(html)
        return await ka.run_keyboard_audit(self.page, **kw)

    # ── focus indicators ────────────────────────────────────────────────────
    async def test_outline_removed_and_nothing_else_is_reported_with_exact_elements(self):
        r = await self.audit("""
            <style>a,button{outline:none}</style>
            <nav><a href="/about">About</a><a href="/blog">Blog</a></nav>
            <button class="cta primary">Get Started</button>""")
        missing = r["affected_elements"]["missing_focus_indicator"]
        self.assertEqual(r["missing_focus_outline"], 3)
        self.assertEqual({(e["tag"], e["accessibleName"]) for e in missing}, {("a", "About"), ("a", "Blog"), ("button", "Get Started")})
        self.assertTrue(all(e["selectorUnique"] for e in missing))
        self.assertEqual({e["role"] for e in missing}, {"link", "button"})
        self.assertFalse(r["focus_trap_detected"])

    async def test_box_shadow_focus_style_with_outline_none_is_not_flagged(self):
        r = await self.audit("""
            <style>a{outline:none} a:focus-visible{box-shadow:0 0 0 3px #005fcc}</style>
            <a href="/a">A</a><a href="/b">B</a>""")
        self.assertEqual(r["missing_focus_outline"], 0)
        self.assertEqual([x["status"] for x in r["element_results"]], ["present", "present"])

    async def test_background_change_only_is_accepted(self):
        r = await self.audit("<style>a{outline:none} a:focus{background:#ff0}</style><a href='/a'>A</a>")
        self.assertEqual(r["missing_focus_outline"], 0)

    async def test_browser_default_focus_ring_is_accepted(self):
        r = await self.audit("<a href='/a'>A</a><button>B</button>")
        self.assertEqual(r["missing_focus_outline"], 0)

    async def test_transitioned_outline_is_waited_for(self):
        r = await self.audit("""
            <style>a{outline:2px solid transparent;transition:outline-color .25s} a:focus-visible{outline-color:#000}</style>
            <a href="/a">A</a>""")
        self.assertEqual(r["missing_focus_outline"], 0)

    async def test_low_contrast_indicator_is_reported_as_weak_not_missing(self):
        r = await self.audit("<style>body{background:#fff}a{outline:none} a:focus{outline:2px solid #fafafa}</style><a href='/a'>A</a>")
        self.assertEqual(r["missing_focus_outline"], 0)
        self.assertEqual(len(r["affected_elements"]["weak_focus_indicator"]), 1)

    async def test_the_suppressing_css_rule_is_named(self):
        r = await self.audit("<style>a{outline:0}</style><a href='/a'>A</a>")
        fi = r["affected_elements"]["missing_focus_indicator"][0]["focusIndicator"]
        self.assertEqual(fi["suppressingRule"]["selector"], "a")
        self.assertFalse(fi["focusRuleFound"])

    # ── selectors / names / metadata ────────────────────────────────────────
    async def test_duplicate_looking_elements_get_selectors_that_resolve_to_exactly_that_element(self):
        html = "<style>a{outline:none}</style>" + "".join(f"<div><a href='/x' class='btn'>Same</a></div>" for _ in range(3))
        r = await self.audit(html)
        for e in r["affected_elements"]["missing_focus_indicator"]:
            self.assertTrue(e["selectorUnique"], e["selector"])
            self.assertEqual(await self.page.locator(e["selector"]).count(), 1, e["selector"])
        self.assertEqual(len({e["selector"] for e in r["affected_elements"]["missing_focus_indicator"]}), 3)

    async def test_dynamic_generated_ids_and_classes_are_not_used_in_selectors(self):
        r = await self.audit("""<style>a{outline:none}</style>
            <a id="radix-:r1a2:" class="css-1a2b3c4 sc-9f8e7d6 nav-link" href="/p">Pricing</a>""")
        sel = r["affected_elements"]["missing_focus_indicator"][0]["selector"]
        self.assertNotIn("radix", sel)
        self.assertNotIn("css-1a2b3c4", sel)
        self.assertNotIn("sc-9f8e7d6", sel)
        self.assertEqual(await self.page.locator(sel).count(), 1)

    async def test_elements_without_ids_or_classes_still_get_a_unique_path(self):
        r = await self.audit("<style>a{outline:none}</style><ul><li><a href='#'>One</a></li><li><a href='#'>Two</a></li></ul>")
        sels = [e["selector"] for e in r["affected_elements"]["missing_focus_indicator"]]
        self.assertEqual(len(set(sels)), 2)
        for sel in sels:
            self.assertEqual(await self.page.locator(sel).count(), 1)

    async def test_accessible_name_sources(self):
        r = await self.audit("""<style>*{outline:none}</style>
            <button aria-label="Open menu">☰</button>
            <span id="lbl">Search site</span><input aria-labelledby="lbl" type="text">
            <label for="em">Email address</label><input id="em" type="email">
            <a href="/x"><img src="x.png" alt="Company logo"></a>
            <input type="submit" value="Send it">
            <a href="/t">Visible text</a>
            <button title="Only a title"></button>""")
        names = [e["accessibleName"] for e in r["affected_elements"]["missing_focus_indicator"]]
        self.assertEqual(names, ["Open menu", "Search site", "Email address", "Company logo", "Send it", "Visible text", "Only a title"])

    async def test_metadata_fields_are_present_and_compact(self):
        r = await self.audit("<style>a{outline:none}</style><nav><a class='nav-link' href='/about-naxonify' tabindex='0' aria-label='About Naxonify'>About</a></nav>")
        e = r["affected_elements"]["missing_focus_indicator"][0]
        self.assertEqual((e["tag"], e["role"], e["href"], e["tabindex"], e["ariaLabel"]), ("a", "link", "/about-naxonify", "0", "About Naxonify"))
        self.assertIn("nav-link", e["classes"])
        self.assertEqual(e["container"]["role"], "nav")
        self.assertTrue(e["visible"])
        self.assertGreater(e["rect"]["w"], 0)
        self.assertEqual(e["focusIndicator"]["before"]["outline"], "none")
        self.assertNotIn("<a", str(e))  # no raw HTML

    async def test_hidden_disabled_and_negative_tabindex_elements_are_not_audited(self):
        r = await self.audit("""<style>*{outline:none}</style>
            <button style="display:none">hidden</button>
            <button disabled>disabled</button>
            <a href="/skip" tabindex="-1">programmatic only</a>
            <a href="/real">Real</a>""")
        self.assertEqual([x["tab"] if False else x["name"] for x in r["element_results"]], ["Real"])

    # ── traversal / traps ───────────────────────────────────────────────────
    async def test_a_normal_page_has_no_trap_and_records_the_focus_sequence(self):
        r = await self.audit("<a href='/1'>One</a><a href='/2'>Two</a><a href='/3'>Three</a><a href='/4'>Four</a>")
        self.assertFalse(r["focus_trap_detected"])
        self.assertTrue(r["traversal"]["completed"])
        seq = [s for s in r["focus_sequence"] if s["kind"] != "body"]
        self.assertEqual([s["accessibleName"] for s in seq], ["One", "Two", "Three", "Four"])
        self.assertEqual(seq[0]["from"], "document start")
        self.assertEqual(seq[1]["from"], seq[0]["to"])

    async def test_ten_id_less_links_are_not_a_trap(self):
        """The exact v1 false positive."""
        r = await self.audit("".join(f"<a href='/{i}'>Link {i}</a>" for i in range(10)))
        self.assertFalse(r["focus_trap_detected"])
        self.assertFalse(r["trap_details"]["detected"])

    async def test_a_scripted_tab_trap_in_a_plain_container_is_an_unintended_failure(self):
        r = await self.audit("""
            <a href="/before">Before</a>
            <div id="widget"><button id="b1">First</button><button id="b2">Second</button></div>
            <a href="/after">After</a>
            <script>
              const bs=[document.getElementById('b1'),document.getElementById('b2')];
              document.getElementById('b1').addEventListener('focus',()=>{});
              document.addEventListener('keydown',e=>{ if(e.key==='Tab' && bs.includes(document.activeElement)){
                e.preventDefault(); (document.activeElement===bs[0]?bs[1]:bs[0]).focus(); }});
            </script>""")
        t = r["trap_details"]
        self.assertTrue(t["detected"])
        self.assertFalse(t["intentional"])
        self.assertEqual(t["verdict"], "unintended")
        self.assertTrue(r["focus_trap_detected"])
        self.assertEqual(t["container"]["selector"], "#widget")
        self.assertEqual({c["accessibleName"] for c in t["cycle"]}, {"First", "Second"})
        self.assertEqual(t["firstElement"]["accessibleName"], "Before")

    async def test_a_visible_modal_dialog_with_a_close_button_is_an_intentional_trap_not_a_failure(self):
        r = await self.audit("""
            <a href="/bg">Behind</a>
            <div role="dialog" aria-modal="true" id="dlg" style="position:fixed;top:50px;left:50px;background:#fff;padding:20px">
              <button id="ok">OK</button><button id="close" aria-label="Close dialog">×</button>
            </div>
            <script>
              const items=[document.getElementById('ok'),document.getElementById('close')];
              document.addEventListener('keydown',e=>{ if(e.key==='Tab'){ e.preventDefault();
                const i=items.indexOf(document.activeElement); items[(i+1)%items.length].focus(); }});
              document.getElementById('ok').focus();
            </script>""")
        t = r["trap_details"]
        self.assertTrue(t["detected"])
        self.assertEqual(t["verdict"], "intentional_modal")
        self.assertTrue(t["intentional"])
        self.assertFalse(r["focus_trap_detected"], "an intentional modal trap must not be reported as a failure")
        self.assertEqual(t["container"]["role"], "dialog")
        self.assertTrue(t["container"]["hasCloseControl"])

    async def test_a_visible_modal_with_no_way_out_is_a_failure(self):
        r = await self.audit("""
            <a href="/bg">Behind</a>
            <div role="dialog" aria-modal="true" id="dlg" style="position:fixed;top:50px;left:50px;background:#fff;padding:20px">
              <button id="a">Accept</button><button id="b">Continue</button>
            </div>
            <script>
              const items=[document.getElementById('a'),document.getElementById('b')];
              document.addEventListener('keydown',e=>{ if(e.key==='Tab'){ e.preventDefault();
                const i=items.indexOf(document.activeElement); items[(i+1)%items.length].focus(); }});
              document.getElementById('a').focus();
            </script>""")
        self.assertEqual(r["trap_details"]["verdict"], "modal_without_exit")
        self.assertTrue(r["focus_trap_detected"])
        self.assertFalse(r["trap_details"]["container"]["escapeCloses"])

    async def test_a_modal_that_closes_on_escape_is_intentional_even_without_a_close_button(self):
        r = await self.audit("""
            <a href="/bg">Behind</a>
            <div role="dialog" aria-modal="true" id="dlg" style="position:fixed;top:50px;left:50px;background:#fff;padding:20px">
              <button id="a">Yes</button><button id="b">No</button>
            </div>
            <script>
              const items=[document.getElementById('a'),document.getElementById('b')];
              document.addEventListener('keydown',e=>{
                if(e.key==='Escape'){ document.getElementById('dlg').style.display='none'; return; }
                if(e.key==='Tab'){ e.preventDefault(); const i=items.indexOf(document.activeElement); items[(i+1)%items.length].focus(); }});
              document.getElementById('a').focus();
            </script>""")
        self.assertEqual(r["trap_details"]["verdict"], "intentional_modal")
        self.assertTrue(r["trap_details"]["container"]["escapeCloses"])
        self.assertFalse(r["focus_trap_detected"])

    async def test_media_and_iframe_hosts_do_not_look_like_a_trap(self):
        r = await self.audit("""<a href="/a">A</a>
            <video controls width="200" height="100"></video>
            <iframe srcdoc="<a href='/in1'>in1</a><a href='/in2'>in2</a>" width="200" height="80"></iframe>
            <a href="/z">Z</a>""")
        self.assertFalse(r["focus_trap_detected"])

    async def test_radio_group_members_are_not_reported_as_unreachable(self):
        r = await self.audit("""<a href="/x">x</a>
            <input type="radio" name="g" value="1"><input type="radio" name="g" value="2" checked><input type="radio" name="g" value="3">""")
        self.assertEqual(r["unreachable_elements"], 0)
        self.assertEqual(r["focusable_total"], 2)

    # ── unreachable ─────────────────────────────────────────────────────────
    async def test_unreachable_is_only_reported_after_a_completed_traversal_and_is_never_body_presses(self):
        r = await self.audit("<a href='/only'>Only one link</a>")
        self.assertEqual(r["unreachable_elements"], 0, "v1 counted the Tab press that left the page as 'unreachable'")

    # ── shadow DOM / dynamic / responsive ───────────────────────────────────
    async def test_open_shadow_dom_controls_are_audited(self):
        r = await self.audit("""<style>button{outline:none}</style><my-el></my-el>
            <script>const h=document.querySelector('my-el'); const s=h.attachShadow({mode:'open'});
              s.innerHTML='<style>button{outline:none}</style><button>Inside shadow</button>';</script>""")
        inner = [e for e in r["affected_elements"]["missing_focus_indicator"] if e["accessibleName"] == "Inside shadow"]
        self.assertEqual(len(inner), 1)
        self.assertTrue(inner[0]["inShadowDom"])

    async def test_an_element_inserted_after_the_baseline_is_unknown_not_a_false_failure(self):
        r = await self.audit("""<style>*{outline:none}</style><a id="first" href="/1">First</a>
            <script>document.getElementById('first').addEventListener('focus',()=>{
              if(document.getElementById('late')) return;
              const b=document.createElement('button'); b.id='late'; b.textContent='Late'; document.body.appendChild(b); });</script>""")
        late = [x for x in r["element_results"] if x["name"] == "Late"]
        self.assertEqual([x["status"] for x in late], ["unknown"])
        self.assertNotIn("Late", [e["accessibleName"] for e in r["affected_elements"]["missing_focus_indicator"]])

    async def test_a_mobile_viewport_only_audits_what_is_visible_there(self):
        html = """<style>a,button{outline:none} .desktop{display:block} .burger{display:none}
                  @media (max-width:600px){.desktop{display:none}.burger{display:block}}</style>
                  <a class="desktop" href="/a">Desktop link</a><button class="burger">Menu</button>"""
        await self.page.set_viewport_size({"width": 375, "height": 700})
        r = await self.audit(html)
        self.assertEqual([x["name"] for x in r["element_results"]], ["Menu"])

    async def test_offscreen_elements_that_take_focus_are_listed_as_not_visible_not_as_missing(self):
        r = await self.audit("<style>a{outline:none} .sr{position:absolute;left:-9999px}</style><a class='sr' href='/skip'>Skip</a><a href='/x'>Visible</a>")
        self.assertEqual([e["accessibleName"] for e in r["affected_elements"]["focus_not_visible"]], ["Skip"])
        self.assertEqual([e["accessibleName"] for e in r["affected_elements"]["missing_focus_indicator"]], ["Visible"])

    async def test_the_effective_background_skips_transparent_layers_and_composites_translucent_ones(self):
        r = await self.audit("""<style>a{outline:none}</style>
            <div style="background:rgb(15,22,40)"><div style="background:rgba(255,255,255,0)"><a href="/1">On navy through a transparent-white layer</a></div></div>
            <div style="background:rgb(1,97,244)"><a href="/2" style="background:rgba(255,255,255,0)">On blue</a></div>
            <div style="background:#fff"><a href="/3">On white</a></div>
            <div style="background:rgb(0,0,0)"><div style="background:rgba(255,255,255,0.5)"><a href="/4">Translucent over black</a></div></div>""")
        bgs = {e["accessibleName"]: e["computed"]["effectiveBackground"] for e in r["affected_elements"]["missing_focus_indicator"]}
        self.assertEqual(bgs["On navy through a transparent-white layer"], "rgb(15, 22, 40)")
        self.assertEqual(bgs["On blue"], "rgb(1, 97, 244)")
        self.assertEqual(bgs["On white"], "rgb(255, 255, 255)")
        self.assertEqual(bgs["Translucent over black"], "rgb(128, 128, 128)")

    async def test_small_click_targets_carry_selector_and_name(self):
        r = await self.audit("<a href='/x' style='display:inline-block;width:10px;height:10px'>x</a>")
        self.assertEqual(r["small_click_targets"], 1)
        self.assertEqual(r["small_click_targets_list"][0]["accessibleName"], "x")

    # ── technology ──────────────────────────────────────────────────────────
    async def test_divi_wordpress_is_detected_with_the_theme(self):
        r = await self.audit("""<html><head><meta name="generator" content="Divi Child Theme v.1.0.0">
            <link rel="stylesheet" href="https://example.com/wp-content/themes/divi-child/style.css"></head>
            <body class="et_divi_theme"><div class="et_pb_row"><a href="/x">x</a></div></body></html>""")
        t = r["technology"]
        self.assertEqual((t["cms"], t["builder"], t["theme"]), ("WordPress", "Divi", "divi-child"))

    async def test_tailwind_and_bootstrap_detection(self):
        tw = await self.audit("<body>" + "".join("<div class='px-4 py-2 md:flex hover:bg-blue-500'>x</div>" for _ in range(20)) + "<a href='/x'>x</a></body>")
        self.assertEqual(tw["technology"]["cssFramework"], "Tailwind")
        bs = await self.audit("<body class='x'><div class='container-fluid'><a class='btn btn-primary' href='/x'>x</a></div></body>")
        self.assertEqual(bs["technology"]["cssFramework"], "Bootstrap")

    async def test_result_is_json_serializable_and_small(self):
        import json
        r = await self.audit("<style>a{outline:none}</style>" + "".join(f"<a href='/{i}'>Link {i}</a>" for i in range(40)))
        blob = json.dumps(r)
        self.assertLess(len(blob), 200_000)
        self.assertEqual(r["audit_version"], 2)
        self.assertLessEqual(len(r["affected_elements"]["missing_focus_indicator"]), ka.MAX_LISTED_ELEMENTS)
        self.assertEqual(r["affected_elements"]["missing_focus_indicator_total"], 40)

    async def test_the_tab_budget_marks_truncation_instead_of_guessing(self):
        r = await self.audit("".join(f"<a href='/{i}'>Link {i}</a>" for i in range(30)), max_tabs=5)
        self.assertTrue(r["traversal"]["truncated"])
        self.assertEqual(r["unreachable_elements"], 0)
        self.assertFalse(r["focus_trap_detected"])


if __name__ == "__main__":
    unittest.main()
