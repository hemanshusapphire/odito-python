"""
Keyboard / focus accessibility audit (audit_version 2).

WHY THIS REPLACES THE OLD SIMULATION
------------------------------------
The v1 check pressed Tab at most 10 times and recorded only *counters*:

  * ``missing_focus_outline`` counted Tab stops whose computed outline was
    "none" — ignoring box-shadow/border/background/... focus styling, recording
    no element, and capped by the number of Tab presses (so "10 elements" was
    just "10 presses").
  * ``focus_trap_detected`` compared ``tag + id``; any three consecutive links
    without an id looked like "the same element focused three times".
  * ``unreachable_elements`` counted Tab presses that landed on <body> (which
    is simply what happens at the end of the page).

v2 records the exact elements and decides from real evidence:

  * every Tab stop is identified by a stable element id (not tag+id);
  * the focus indicator is judged by comparing the element's computed style
    UNFOCUSED vs FOCUSED (outline, box-shadow, border, background, colour,
    text-decoration, transform, filter, opacity, ::before/::after);
  * a trap is a real cycle/stuck focus, and is classified as intentional (a
    visible modal/dialog/popup that can be closed) or a failure.

Division of labour: ``a11y_audit.js`` (in the page) only collects facts;
everything below the "pure decision logic" banner is plain Python operating on
JSON-serializable data, so it is unit-tested without a browser.
"""

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

AUDIT_VERSION = 2
AUDIT_METHOD = "playwright_tab_traversal+computed_style_diff"

MAX_TAB_STOPS = int(os.environ.get("A11Y_MAX_TAB_STOPS", "100"))
AUDIT_BUDGET_S = float(os.environ.get("A11Y_AUDIT_BUDGET_S", "30"))
MAX_LISTED_ELEMENTS = 50

# Tags where several Tab stops legitimately report the SAME host element
# (an iframe's / media player's inner controls) — never evidence of a trap.
MULTI_STOP_TAGS = {"iframe", "video", "audio", "object", "embed"}

_AUDIT_JS_PATH = Path(__file__).resolve().parent / "a11y_audit.js"
_AUDIT_JS = None


def _load_audit_js() -> str:
    global _AUDIT_JS
    if _AUDIT_JS is None:
        _AUDIT_JS = _AUDIT_JS_PATH.read_text(encoding="utf-8")
    return _AUDIT_JS


# ═══════════════════════════════════════════════════════════════════════════
# Pure decision logic (no browser)
# ═══════════════════════════════════════════════════════════════════════════

_COLOR_RE = re.compile(r"rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+%?))?\s*\)")


def parse_color(value):
    """'rgb(1, 2, 3)' / 'rgba(1, 2, 3, .5)' -> (r, g, b, a) or None."""
    if not isinstance(value, str):
        return None
    m = _COLOR_RE.search(value)
    if not m:
        return None
    a = m.group(4)
    if a is None:
        alpha = 1.0
    elif a.endswith("%"):
        alpha = float(a[:-1]) / 100.0
    else:
        alpha = float(a)
    return (float(m.group(1)), float(m.group(2)), float(m.group(3)), alpha)


def _luminance(rgb):
    def chan(c):
        c = c / 255.0
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    r, g, b = rgb[:3]
    return 0.2126 * chan(r) + 0.7152 * chan(g) + 0.0722 * chan(b)


def contrast_ratio(fg, bg):
    """WCAG contrast ratio between two parsed colors (alpha of fg composited on bg)."""
    if fg is None or bg is None:
        return None
    a = fg[3]
    blended = tuple(fg[i] * a + bg[i] * (1 - a) for i in range(3))
    l1, l2 = _luminance(blended), _luminance(bg)
    hi, lo = max(l1, l2), min(l1, l2)
    return round((hi + 0.05) / (lo + 0.05), 2)


def _px(value):
    try:
        return float(str(value).replace("px", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _color_differs(a, b):
    ca, cb = parse_color(a), parse_color(b)
    if ca is None or cb is None:
        return (a or "") != (b or "")
    return any(abs(ca[i] - cb[i]) > 2 for i in range(3)) or abs(ca[3] - cb[3]) > 0.02


def _outline_visible(s):
    if not s:
        return False
    style = (s.get("outlineStyle") or "none").lower()
    if style in ("none", "hidden"):
        return False
    if _px(s.get("outlineWidth")) <= 0:
        return False
    c = parse_color(s.get("outlineColor"))
    return c is None or c[3] > 0.02


def _outline_signature(s):
    return (s.get("outlineStyle"), s.get("outlineWidth"), s.get("outlineColor"), s.get("outlineOffset")) if s else None


def _shadow_visible(value):
    if not value or value == "none":
        return False
    for chunk in re.split(r",(?![^(]*\))", value):
        c = parse_color(chunk)
        nums = [abs(float(n)) for n in re.findall(r"(-?[\d.]+)px", chunk)]
        if (c is None or c[3] > 0.02) and any(n > 0 for n in nums):
            return True
    return False


def _border_visible(sides):
    for side in sides or []:
        parts = str(side).split(" ", 2)
        if len(parts) < 3:
            continue
        width, style, color = _px(parts[0]), parts[1].lower(), parse_color(parts[2])
        if width > 0 and style not in ("none", "hidden") and (color is None or color[3] > 0.02):
            return True
    return False


def _first_color(value):
    m = _COLOR_RE.search(value or "")
    return parse_color(m.group(0)) if m else None


def compact_style(s):
    """Small, human-readable focus-relevant style summary for storage/UI."""
    if not s:
        return None
    outline = "none" if not _outline_visible(s) else f"{s.get('outlineWidth')} {s.get('outlineStyle')} {s.get('outlineColor')}"
    border_sides = s.get("border") or []
    return {
        "outline": outline,
        "outlineWidth": s.get("outlineWidth"),
        "outlineStyle": s.get("outlineStyle"),
        "outlineColor": s.get("outlineColor"),
        "boxShadow": s.get("boxShadow") or "none",
        "border": border_sides[0] if border_sides else None,
        "backgroundColor": s.get("backgroundColor"),
        "color": s.get("color"),
        "textDecoration": s.get("textDecorationLine"),
    }


def evaluate_focus_indicator(before, after, visible=True, background=None):
    """Decide whether an element shows a visible focus indicator.

    An element is NOT failed merely because ``outline: none`` — a valid
    indicator may be a box-shadow, border, background/colour change, underline,
    transform, or a ::before/::after change. What matters is a meaningful
    visual difference between the unfocused and focused computed styles.

    Returns {status, reason, signals, contrastRatio?}:
      present      a visible difference exists
      weak         the only difference is an outline/shadow/border whose colour
                   has < 3:1 contrast against the background
      missing      nothing visibly changes on focus
      not_visible  the focused element is not rendered on screen
      unknown      no unfocused baseline / focused snapshot to compare
    """
    if not visible:
        return {"status": "not_visible", "reason": "element_not_rendered", "signals": []}
    if not before or not after:
        return {"status": "unknown", "reason": "no_baseline", "signals": []}

    signals = []
    color_signals = {}   # signal -> parsed color, for the contrast check

    if _outline_visible(after) and (not _outline_visible(before) or _outline_signature(before) != _outline_signature(after)):
        signals.append("outline")
        color_signals["outline"] = parse_color(after.get("outlineColor"))

    if _shadow_visible(after.get("boxShadow")) and after.get("boxShadow") != before.get("boxShadow"):
        signals.append("box-shadow")
        color_signals["box-shadow"] = _first_color(after.get("boxShadow"))

    if after.get("border") != before.get("border") and _border_visible(after.get("border")):
        signals.append("border")
        color_signals["border"] = parse_color(str((after.get("border") or [""])[0]).split(" ", 2)[-1])

    if _color_differs(before.get("backgroundColor"), after.get("backgroundColor")) or before.get("backgroundImage") != after.get("backgroundImage"):
        signals.append("background")
    if _color_differs(before.get("color"), after.get("color")):
        signals.append("color")
    if after.get("textDecorationLine") != before.get("textDecorationLine") and (after.get("textDecorationLine") or "none") != "none":
        signals.append("text-decoration")
    if after.get("transform") != before.get("transform"):
        signals.append("transform")
    if after.get("filter") != before.get("filter"):
        signals.append("filter")
    if abs(float(after.get("opacity") or 1) - float(before.get("opacity") or 1)) > 0.05:
        signals.append("opacity")
    if after.get("before") != before.get("before") or after.get("after") != before.get("after"):
        signals.append("pseudo-element")

    if not signals:
        return {"status": "missing", "reason": "no_visual_change_on_focus", "signals": []}

    # A colour-only outline/shadow/border that blends into the background is a weak
    # indicator (WCAG 1.4.11 / 2.4.11 want >= 3:1). Anything else that changed
    # (background, underline, transform...) is accepted as present.
    only_edge = all(s in color_signals for s in signals)
    if only_edge:
        bg = parse_color(background) or (255.0, 255.0, 255.0, 1.0)
        ratios = [contrast_ratio(color_signals[s], bg) for s in signals if color_signals.get(s) is not None]
        if ratios and all(r is not None and r < 3.0 for r in ratios):
            return {"status": "weak", "reason": "low_contrast", "signals": signals, "contrastRatio": min(ratios)}

    return {"status": "present", "reason": None, "signals": signals}


def analyze_traversal(steps, visible_ids, truncated=False):
    """Analyse an ordered list of Tab stops for focus traps.

    ``steps``: [{"kind": "element"|"iframe"|"media"|"body", "id": int}], in order.
    ``visible_ids``: ids of the page's visible tabbable elements (baseline).

    Detects, by element IDENTITY (never tag+id):
      * stuck  — the same element focused on 3+ consecutive Tab presses;
      * cycle  — focus revisits an element without ever leaving the loop:
                 * back to a mid-sequence element (elements before it can never
                   be reached again), or
                 * back to the FIRST element after visiting only a small part of
                   the visible page (a normal end-of-page wrap covers the page).
    Reaching <body> after at least one element is the normal end of the page.
    """
    visible_ids = set(visible_ids or [])
    seq, kinds = [], {}
    exited = False
    for s in steps:
        if s.get("kind") == "body":
            if seq:
                exited = True
                break
            continue
        seq.append(s["id"])
        kinds[s["id"]] = s.get("kind", "element")

    result = {
        "detected": False, "type": None, "elementIds": [], "cycleLength": 0,
        "sequenceLength": len(seq), "exited": exited, "wrapped": False, "truncated": bool(truncated),
        "firstId": seq[0] if seq else None, "lastId": seq[-1] if seq else None,
    }

    run = 1
    for i in range(1, len(seq)):
        if seq[i] == seq[i - 1] and kinds.get(seq[i]) == "element":
            run += 1
            if run >= 3:
                result.update(detected=True, type="stuck", elementIds=[seq[i]], cycleLength=1)
                return result
        else:
            run = 1

    # A Tab press that leaves focus where it was once or twice (an iframe/media
    # host, a momentary handler) is not movement — collapse consecutive repeats so
    # only real revisits after moving elsewhere count as a cycle.
    path = [e for k, e in enumerate(seq) if k == 0 or e != seq[k - 1]]

    first_seen = {}
    for i, eid in enumerate(path):
        if eid not in first_seen:
            first_seen[eid] = i
            continue
        j = first_seen[eid]
        cycle = path[j:i]
        if j == 0:
            # Back at the first element WITHOUT ever reaching <body>/browser UI.
            # If everything visible was covered this is the normal end-of-page
            # wrap; if visible elements were never reached, focus is confined to
            # the loop (e.g. a modal that never lets focus reach the page behind).
            # Limitation: a loop covering EVERY focusable element is
            # indistinguishable from a wrap by identity alone.
            unvisited = visible_ids - set(path)
            if unvisited and len(cycle) < len(visible_ids):
                result.update(detected=True, type="cycle", elementIds=list(dict.fromkeys(cycle)), cycleLength=len(cycle))
            else:
                result["wrapped"] = True
        else:
            result.update(detected=True, type="cycle", elementIds=list(dict.fromkeys(cycle)), cycleLength=len(cycle))
        return result
    return result


def classify_trap(analysis, probe=None, escape=None):
    """Decide whether a detected trap is intentional or a failure.

    Legitimate focus traps exist in modal dialogs, drawers, popovers, menus and
    command palettes — an intentional modal trap is NOT an accessibility failure.
    ``probe``: facts about the container (from ``probeContainer``); ``escape``:
    {"closed": bool} after pressing Escape, or None if not tested.
    """
    if not analysis.get("detected"):
        return {"detected": False, "intentional": False, "verdict": "none"}

    probe = probe or {}
    dialog_like = bool(probe.get("isDialogLike") or probe.get("ariaModal") or probe.get("role") in ("dialog", "alertdialog"))
    widget_like = bool(probe.get("isWidgetLike"))
    visible = bool(probe.get("visible"))
    has_close = bool(probe.get("hasCloseControl"))
    escape_closes = None if escape is None else bool(escape.get("closed"))
    can_leave = has_close or bool(escape_closes)

    if not probe:
        verdict, cause = "unintended", "focus is confined to a set of elements but the container could not be identified"
    elif dialog_like and visible:
        if can_leave:
            verdict, cause = "intentional_modal", "focus is trapped inside a visible modal dialog that can be closed — expected behaviour while it is open"
        else:
            verdict, cause = "modal_without_exit", "focus is trapped inside a visible dialog that has no close control and does not close on Escape"
    elif widget_like and visible and can_leave:
        verdict, cause = "intentional_widget", "focus is trapped inside a visible popup/drawer that can be closed"
    elif not visible:
        verdict, cause = "hidden_container_trap", "focus cycles inside a container that is not visible — a closed drawer/popup is still in the tab order"
    elif analysis.get("type") == "stuck":
        verdict, cause = "unintended", "Tab does not move focus off this element — a key handler or script is intercepting Tab"
    else:
        verdict, cause = "unintended", "focus cycles among a small set of elements in a container that is not a modal dialog"

    intentional = verdict.startswith("intentional")
    return {
        "detected": True,
        "intentional": intentional,
        "verdict": verdict,
        "suspectedCause": cause,
        "container": {
            "selector": probe.get("selector"),
            "tag": probe.get("tag"),
            "role": probe.get("role"),
            "ariaModal": bool(probe.get("ariaModal")),
            "visible": visible,
            "isDialogLike": dialog_like,
            "hasCloseControl": has_close,
            "closeControl": probe.get("closeControl"),
            "escapeCloses": escape_closes,
            "focusableInside": probe.get("focusableInside"),
        } if probe else None,
        "trapType": analysis.get("type"),
        "cycleLength": analysis.get("cycleLength"),
        # Whether focus returns to the trigger after closing can only be tested by
        # opening the component, which this audit never does (it does not click).
        "focusReturnsToTrigger": None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Browser orchestration
# ═══════════════════════════════════════════════════════════════════════════

def _element_record(rec, indicator):
    """Compact, storable record of one Tab stop (no raw HTML)."""
    m = rec["meta"]
    out = {k: m.get(k) for k in (
        "tag", "role", "accessibleName", "text", "ariaLabel", "ariaLabelledby", "elementId", "classes", "href",
        "name", "type", "tabindex", "selector", "selectorUnique", "domPath", "rect", "visible", "disabled",
        "naturallyFocusable", "container", "inShadowDom",
    )}
    if m.get("xpath"):
        out["xpath"] = m["xpath"]
    if m.get("hiddenReason"):
        out["hiddenReason"] = m["hiddenReason"]
    out["focusIndicator"] = {
        **indicator,
        "before": compact_style(rec.get("before")),
        "after": compact_style(rec.get("after")),
        "focusRuleFound": rec.get("focusRuleFound"),
        "focusRules": rec.get("focusRules") or [],
        "suppressingRule": rec.get("suppressingRule"),
    }
    out["computed"] = {"color": (rec.get("after") or {}).get("color"), "backgroundColor": (rec.get("after") or {}).get("backgroundColor"), "effectiveBackground": rec.get("background")}
    return out


async def run_keyboard_audit(page, max_tabs=None, budget_s=None):
    """Tab through the page and return the v2 ``keyboard_analysis`` dict."""
    max_tabs = max_tabs or MAX_TAB_STOPS
    budget_s = budget_s or AUDIT_BUDGET_S
    started = time.monotonic()
    tested_at = datetime.now(timezone.utc).isoformat()

    await page.evaluate(_load_audit_js())
    transitions_off = await page.evaluate("() => window.__odA11y.noTransitions()")
    baseline = await page.evaluate("() => window.__odA11y.baseline(500)")
    visible_ids = {i for i, vis in baseline["ids"] if vis}

    # Start from the top of the document with nothing focused.
    await page.evaluate("() => { const a = document.activeElement; if (a && a.blur) a.blur(); window.scrollTo(0, 0); }")

    steps, records, order = [], {}, []
    truncated = False
    for _ in range(max_tabs):
        if time.monotonic() - started > budget_s:
            truncated = True
            break
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(40)
        try:
            rec = await page.evaluate("() => window.__odA11y.collect()")
        except Exception:
            rec = {"focused": False, "kind": "body"}

        if not rec.get("focused"):
            steps.append({"kind": "body"})
            if order:
                break  # left the page: end of a normal traversal
            continue

        eid = rec["meta"]["id"]
        tag = rec["meta"]["tag"]
        kind = "iframe" if tag == "iframe" else ("media" if tag in MULTI_STOP_TAGS else "element")
        steps.append({"kind": kind, "id": eid})
        if eid not in records:
            records[eid] = rec
            order.append(eid)
        partial = analyze_traversal(steps, visible_ids)
        if partial["detected"] or partial["wrapped"]:
            break
    else:
        truncated = True

    analysis = analyze_traversal(steps, visible_ids, truncated=truncated)
    completed = analysis["exited"] or analysis["wrapped"]

    # ── Per-element focus-indicator verdicts ────────────────────────────────
    element_results, missing, weak, not_visible, small_targets = [], [], [], [], []
    for eid in order:
        rec = records[eid]
        m = rec["meta"]
        indicator = evaluate_focus_indicator(rec.get("before"), rec.get("after"), visible=m.get("visible", True), background=rec.get("background"))
        stored = _element_record(rec, indicator)
        element_results.append({"selector": m["selector"], "tag": m["tag"], "name": m.get("accessibleName"), "status": indicator["status"], "selectorUnique": m.get("selectorUnique")})
        if indicator["status"] == "missing":
            missing.append(stored)
        elif indicator["status"] == "weak":
            weak.append(stored)
        elif indicator["status"] == "not_visible":
            not_visible.append(stored)
        w, h = (m.get("rect") or {}).get("w", 0), (m.get("rect") or {}).get("h", 0)
        if m.get("visible") and ((0 < w < 24) or (0 < h < 24)):
            small_targets.append({"selector": m["selector"], "width": w, "height": h, "tag": m["tag"], "accessibleName": m.get("accessibleName"), "role": m.get("role")})

    # ── Unreachable: visible tabbable elements a COMPLETED traversal never focused
    unreachable = []
    if completed and not analysis["detected"]:
        for eid in sorted(visible_ids - set(order))[:25]:
            d = await page.evaluate("(i) => window.__odA11y.describeById(i)", eid)
            if d:
                unreachable.append({k: d.get(k) for k in ("tag", "role", "accessibleName", "selector", "selectorUnique", "domPath", "rect", "container")})

    # ── Focus trap classification ───────────────────────────────────────────
    trap = {"detected": False, "intentional": False, "verdict": "none"}
    if analysis["detected"]:
        probe = await page.evaluate("(ids) => window.__odA11y.probeContainer(ids)", analysis["elementIds"])
        escape = None
        if probe and probe.get("visible") and (probe.get("isDialogLike") or probe.get("isWidgetLike")):
            try:
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(250)
                state = await page.evaluate("(i) => window.__odA11y.probeState(i)", probe["nodeId"])
                escape = {"closed": (not state.get("present")) or (not state.get("visible"))}
            except Exception:
                escape = None
        trap = classify_trap(analysis, probe, escape)
        trap["firstElement"] = (_brief(records.get(analysis["firstId"])) if analysis.get("firstId") else None)
        trap["lastElement"] = (_brief(records.get(analysis["lastId"])) if analysis.get("lastId") else None)
        trap["cycle"] = [_brief(records.get(i)) for i in analysis["elementIds"][:20] if records.get(i)]
        trap["triggeringElement"] = _brief(records.get(analysis["elementIds"][0])) if analysis["elementIds"] and records.get(analysis["elementIds"][0]) else None

    technology = {}
    try:
        technology = await page.evaluate("() => window.__odA11y.technology()")
    except Exception:
        technology = {}

    prev = "document start"
    sequence = []
    for n, s in enumerate(steps, start=1):
        if s["kind"] == "body":
            sequence.append({"step": n, "from": prev, "to": "browser UI / end of page", "kind": "body"})
            break
        m = records[s["id"]]["meta"]
        label = m["selector"]
        sequence.append({
            "step": n, "from": prev, "to": label, "kind": s["kind"], "tag": m["tag"], "role": m.get("role"),
            "accessibleName": m.get("accessibleName"), "container": (m.get("container") or {}).get("selector"),
        })
        prev = label

    return {
        "audit_version": AUDIT_VERSION,
        "audit_method": AUDIT_METHOD,
        "tested_at": tested_at,
        "keyboard_navigation_checked": True,
        # Legacy summary fields (same names/meaning the rules already read):
        "focus_trap_detected": bool(trap.get("detected")) and not trap.get("intentional"),
        "unreachable_elements": len(unreachable),
        "small_click_targets": len(small_targets),
        "small_click_targets_list": small_targets[:MAX_LISTED_ELEMENTS],
        "missing_focus_outline": len(missing),
        "total_tab_presses": len(steps),
        "detected_focusable_elements": baseline["total"],
        "focus_order": [
            {"tag": records[s["id"]]["meta"]["tag"], "id": records[s["id"]]["meta"].get("elementId") or "", "selector": records[s["id"]]["meta"]["selector"]}
            for s in steps if s["kind"] != "body"
        ],
        # v2 detail:
        "focusable_total": baseline["total"],
        "focusable_visible": baseline["visible"],
        "tab_stops_visited": len(order),
        "traversal": {
            "completed": bool(completed), "exited": analysis["exited"], "wrapped": analysis["wrapped"],
            "truncated": bool(truncated), "max_tab_stops": max_tabs, "duration_ms": int((time.monotonic() - started) * 1000),
        },
        "affected_elements": {
            "missing_focus_indicator": missing[:MAX_LISTED_ELEMENTS],
            "missing_focus_indicator_total": len(missing),
            "weak_focus_indicator": weak[:MAX_LISTED_ELEMENTS],
            "focus_not_visible": not_visible[:MAX_LISTED_ELEMENTS],
            "unreachable": unreachable,
        },
        "element_results": element_results,
        "focus_sequence": sequence,
        "trap_details": trap,
        "technology": technology,
        "transitions_disabled_for_audit": bool(transitions_off),
        "css_rules_unavailable": bool(await page.evaluate("() => window.__odA11y.crossOriginSheets()")),
    }


def _brief(rec):
    if not rec:
        return None
    m = rec["meta"]
    return {"tag": m["tag"], "role": m.get("role"), "accessibleName": m.get("accessibleName"), "selector": m["selector"], "selectorUnique": m.get("selectorUnique")}
