/*
 * In-page keyboard/focus audit helper for the HEADLESS_ACCESSIBILITY worker.
 *
 * Injected once per page (see keyboard_audit.py). It only COLLECTS facts from
 * the live DOM — element identity, a stable selector, accessible name, computed
 * styles before/after focus, matching CSS rules, containers. Every verdict
 * (does this element have a visible focus indicator? is this a focus trap? is
 * the trap intentional?) is made in Python (keyboard_audit.py) from these
 * plain, JSON-serializable facts so it can be unit-tested without a browser.
 *
 * Nothing here mutates the page except moving keyboard focus / scrolling (which
 * the Tab traversal does anyway). No DOM attributes are added; element identity
 * is kept in a WeakMap. No raw HTML is returned — only compact structured data.
 */
(() => {
  if (window.__odA11y) return;

  const MAX_TEXT = 80;
  const MAX_CLASSES = 6;
  const MAX_RULES = 6000;

  const S = {
    ids: new WeakMap(),
    els: [],
    base: new WeakMap(),
    rules: null,
    crossOriginSheets: 0,
    transitionsOff: false,
  };

  const clip = (s, n) => {
    s = (s == null ? '' : String(s)).replace(/\s+/g, ' ').trim();
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
  };
  const esc = (s) => (window.CSS && CSS.escape ? CSS.escape(s) : String(s).replace(/[^a-zA-Z0-9_-]/g, '\\$&'));

  function idOf(el) {
    let id = S.ids.get(el);
    if (!id) {
      id = S.els.length + 1;
      S.ids.set(el, id);
      S.els.push(el);
    }
    return id;
  }

  // ── active element (through open shadow roots) ────────────────────────────
  function deepActive() {
    let a = document.activeElement;
    while (a && a.shadowRoot && a.shadowRoot.activeElement) a = a.shadowRoot.activeElement;
    return a;
  }

  // ── visibility ────────────────────────────────────────────────────────────
  function visibility(el) {
    const r = el.getBoundingClientRect();
    const cs = getComputedStyle(el);
    if (cs.display === 'none') return { visible: false, why: 'display_none' };
    if (cs.visibility === 'hidden' || cs.visibility === 'collapse') return { visible: false, why: 'visibility_hidden' };
    if (el.checkVisibility && !el.checkVisibility({ checkOpacity: true, checkVisibilityCSS: true })) {
      return { visible: false, why: 'not_rendered' };
    }
    if (r.width <= 0 || r.height <= 0) return { visible: false, why: 'zero_size' };
    const docW = Math.max(document.documentElement.scrollWidth, window.innerWidth);
    const docH = Math.max(document.documentElement.scrollHeight, window.innerHeight);
    const ax = r.left + window.scrollX;
    const ay = r.top + window.scrollY;
    if (ax + r.width <= 0 || ay + r.height <= 0 || ax >= docW + 1 || ay >= docH + 1) {
      return { visible: false, why: 'offscreen' };
    }
    return { visible: true, why: null };
  }

  // ── computed style snapshots (only what can signal a focus change) ────────
  function pseudo(el, which) {
    const cs = getComputedStyle(el, which);
    if (!cs || !cs.content || cs.content === 'none' || cs.content === 'normal') return null;
    return {
      bg: cs.backgroundColor,
      border: `${cs.borderTopWidth} ${cs.borderTopStyle} ${cs.borderTopColor}`,
      shadow: cs.boxShadow,
      outline: `${cs.outlineWidth} ${cs.outlineStyle} ${cs.outlineColor}`,
      opacity: cs.opacity,
      transform: cs.transform,
      w: cs.width,
      h: cs.height,
    };
  }

  function styleSnap(el) {
    const cs = getComputedStyle(el);
    return {
      outlineStyle: cs.outlineStyle,
      outlineWidth: cs.outlineWidth,
      outlineColor: cs.outlineColor,
      outlineOffset: cs.outlineOffset,
      boxShadow: cs.boxShadow,
      border: [
        `${cs.borderTopWidth} ${cs.borderTopStyle} ${cs.borderTopColor}`,
        `${cs.borderRightWidth} ${cs.borderRightStyle} ${cs.borderRightColor}`,
        `${cs.borderBottomWidth} ${cs.borderBottomStyle} ${cs.borderBottomColor}`,
        `${cs.borderLeftWidth} ${cs.borderLeftStyle} ${cs.borderLeftColor}`,
      ],
      backgroundColor: cs.backgroundColor,
      backgroundImage: clip(cs.backgroundImage, 120),
      color: cs.color,
      textDecorationLine: cs.textDecorationLine,
      textDecorationColor: cs.textDecorationColor,
      transform: cs.transform,
      filter: cs.filter,
      opacity: cs.opacity,
      before: pseudo(el, '::before'),
      after: pseudo(el, '::after'),
    };
  }

  // The colour actually behind an element: walk up compositing every translucent
  // background layer until an opaque one (or the white page) is reached. A layer
  // like rgba(255,255,255,0) is TRANSPARENT and must not be mistaken for white.
  function effectiveBackground(el) {
    const layers = [];
    for (let n = el; n && n.nodeType === 1; n = n.parentElement) {
      const m = /rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+%?))?\s*\)/.exec(getComputedStyle(n).backgroundColor || '');
      if (!m) continue;
      const a = m[4] === undefined ? 1 : (m[4].endsWith('%') ? parseFloat(m[4]) / 100 : parseFloat(m[4]));
      if (a <= 0) continue;
      layers.push([+m[1], +m[2], +m[3], a]);
      if (a >= 0.99) break;
    }
    let [r, g, b] = [255, 255, 255];
    for (let i = layers.length - 1; i >= 0; i -= 1) {
      const [lr, lg, lb, la] = layers[i];
      r = lr * la + r * (1 - la);
      g = lg * la + g * (1 - la);
      b = lb * la + b * (1 - la);
    }
    return `rgb(${Math.round(r)}, ${Math.round(g)}, ${Math.round(b)})`;
  }

  function maxTransitionMs(el) {
    const cs = getComputedStyle(el);
    const toMs = (v) => v.split(',').map((x) => (x.trim().endsWith('ms') ? parseFloat(x) : parseFloat(x) * 1000) || 0);
    const d = toMs(cs.transitionDuration || '0s');
    const l = toMs(cs.transitionDelay || '0s');
    let m = 0;
    for (let i = 0; i < d.length; i += 1) m = Math.max(m, d[i] + (l[i % l.length] || 0));
    return m;
  }

  // ── selectors ─────────────────────────────────────────────────────────────
  const DYNAMIC_ID = /(\d{4,})|^(:r|radix-|headlessui-|react-|ember|__|ui-id-|mui-)/i;
  const DYNAMIC_CLASS = /^(css-|sc-|jsx-|styled-|_[a-z0-9]{5,}$|[a-z]+_[a-f0-9]{5,}$)|[0-9a-f]{7,}|^\d/i;

  function rootOf(el) {
    return el.getRootNode && el.getRootNode() instanceof ShadowRoot ? el.getRootNode() : document;
  }
  function isUnique(sel, el) {
    try {
      const found = rootOf(el).querySelectorAll(sel);
      return found.length === 1 && found[0] === el;
    } catch (e) {
      return false;
    }
  }
  function stableClasses(el) {
    const raw = typeof el.className === 'string' ? el.className : (el.getAttribute && el.getAttribute('class')) || '';
    return raw.split(/\s+/).filter((c) => c && !DYNAMIC_CLASS.test(c) && c.length <= 40);
  }
  function ownSelectors(el) {
    const tag = el.tagName.toLowerCase();
    const out = [];
    if (el.id && !DYNAMIC_ID.test(el.id)) out.push('#' + esc(el.id));
    for (const attr of ['data-testid', 'data-test', 'data-cy', 'data-id']) {
      const v = el.getAttribute(attr);
      if (v) out.push(`${tag}[${attr}="${v.replace(/"/g, '\\"')}"]`);
    }
    const href = el.getAttribute('href');
    if (tag === 'a' && href) out.push(`a[href="${href.replace(/"/g, '\\"')}"]`);
    const aria = el.getAttribute('aria-label');
    if (aria) out.push(`${tag}[aria-label="${aria.replace(/"/g, '\\"')}"]`);
    const name = el.getAttribute('name');
    if (name) out.push(`${tag}[name="${name.replace(/"/g, '\\"')}"]`);
    const classes = stableClasses(el);
    if (classes.length) {
      out.push(tag + classes.slice(0, 3).map((c) => '.' + esc(c)).join(''));
      out.push(tag + classes.slice(0, 1).map((c) => '.' + esc(c)).join(''));
    }
    return out;
  }
  function nth(el) {
    const tag = el.tagName;
    let i = 1;
    for (let s = el.previousElementSibling; s; s = s.previousElementSibling) if (s.tagName === tag) i += 1;
    const total = el.parentElement ? Array.from(el.parentElement.children).filter((c) => c.tagName === tag).length : 1;
    return total > 1 ? `${el.tagName.toLowerCase()}:nth-of-type(${i})` : el.tagName.toLowerCase();
  }
  function cssSelector(el) {
    for (const sel of ownSelectors(el)) if (isUnique(sel, el)) return { selector: sel, unique: true };
    // Anchor on the nearest ancestor that has a unique own selector, then walk down.
    const parts = [];
    let node = el;
    for (let depth = 0; node && node.nodeType === 1 && depth < 8; depth += 1) {
      const own = ownSelectors(node).find((s) => isUnique(s, node));
      if (own && node !== el) {
        parts.unshift(own);
        const sel = parts.join(' > ');
        if (isUnique(sel, el)) return { selector: sel, unique: true };
        break;
      }
      parts.unshift(nth(node));
      const sel = parts.join(' > ');
      if (isUnique(sel, el)) return { selector: sel, unique: true };
      if (node.parentElement === null || node === document.body) break;
      node = node.parentElement;
    }
    const fallback = parts.join(' > ');
    return { selector: fallback || el.tagName.toLowerCase(), unique: isUnique(fallback, el) };
  }
  function xpath(el) {
    const segs = [];
    for (let n = el; n && n.nodeType === 1; n = n.parentElement) {
      let i = 1;
      for (let s = n.previousElementSibling; s; s = s.previousElementSibling) if (s.tagName === n.tagName) i += 1;
      segs.unshift(`${n.tagName.toLowerCase()}[${i}]`);
      if (n === document.documentElement) break;
    }
    return '/' + segs.join('/');
  }
  function domPath(el) {
    const segs = [];
    let n = el;
    for (let d = 0; n && n.nodeType === 1 && d < 6; d += 1) {
      let s = n.tagName.toLowerCase();
      if (n.id && !DYNAMIC_ID.test(n.id)) s += '#' + n.id;
      else {
        const c = stableClasses(n)[0];
        if (c) s += '.' + c;
      }
      segs.unshift(s);
      if (n === document.body) break;
      n = n.parentElement;
    }
    return segs.join(' > ');
  }

  // ── role / accessible name ────────────────────────────────────────────────
  const IMPLICIT_ROLE = {
    a: (el) => (el.hasAttribute('href') ? 'link' : null),
    button: () => 'button',
    select: () => 'combobox',
    textarea: () => 'textbox',
    summary: () => 'button',
    input: (el) => {
      const t = (el.getAttribute('type') || 'text').toLowerCase();
      if (['button', 'submit', 'reset', 'image'].includes(t)) return 'button';
      if (t === 'checkbox') return 'checkbox';
      if (t === 'radio') return 'radio';
      if (t === 'range') return 'slider';
      if (t === 'search') return 'searchbox';
      return 'textbox';
    },
    iframe: () => 'iframe',
  };
  function roleOf(el) {
    const explicit = el.getAttribute('role');
    if (explicit) return explicit.split(/\s+/)[0];
    const fn = IMPLICIT_ROLE[el.tagName.toLowerCase()];
    return fn ? fn(el) : null;
  }
  function textOf(el) {
    return clip(el.innerText || el.textContent || '', MAX_TEXT);
  }
  function accessibleName(el) {
    const lb = el.getAttribute('aria-labelledby');
    if (lb) {
      const t = lb.split(/\s+/).map((id) => {
        const n = document.getElementById(id);
        return n ? n.textContent : '';
      }).join(' ');
      if (clip(t, MAX_TEXT)) return clip(t, MAX_TEXT);
    }
    const al = el.getAttribute('aria-label');
    if (al && al.trim()) return clip(al, MAX_TEXT);
    if (el.labels && el.labels.length) {
      const t = clip(Array.from(el.labels).map((l) => l.textContent).join(' '), MAX_TEXT);
      if (t) return t;
    }
    const tag = el.tagName.toLowerCase();
    if (tag === 'input') {
      const type = (el.getAttribute('type') || '').toLowerCase();
      if (['button', 'submit', 'reset'].includes(type) && el.value) return clip(el.value, MAX_TEXT);
      if (type === 'image' && el.alt) return clip(el.alt, MAX_TEXT);
    }
    const text = textOf(el);
    if (text) return text;
    const img = el.querySelector && el.querySelector('img[alt]');
    if (img && img.getAttribute('alt').trim()) return clip(img.getAttribute('alt'), MAX_TEXT);
    const svgTitle = el.querySelector && el.querySelector('svg title');
    if (svgTitle && svgTitle.textContent.trim()) return clip(svgTitle.textContent, MAX_TEXT);
    if (el.title) return clip(el.title, MAX_TEXT);
    if (el.placeholder) return clip(el.placeholder, MAX_TEXT);
    return '';
  }

  // ── CSS rules that mention focus / remove outlines ────────────────────────
  const FOCUS_PSEUDO = /:focus-visible|:focus-within|:focus/g;
  function walkRules(rules, href, out) {
    for (const rule of Array.from(rules)) {
      if (out.length >= MAX_RULES) return;
      if (rule.type === 1 && rule.selectorText) {
        const sel = rule.selectorText;
        const hasFocus = /:focus/.test(sel);
        const st = rule.style;
        const outline = (st.outline || '') + ' ' + (st.outlineStyle || '') + ' ' + (st.outlineWidth || '');
        const outlineNone = /(^|\s)(none|0|0px)(\s|$)/.test(outline.trim()) && !/auto|solid|dashed|dotted|double/.test(outline);
        if (hasFocus || outlineNone) {
          const declares = [];
          if (st.outline || st.outlineStyle || st.outlineWidth) declares.push('outline');
          if (st.boxShadow) declares.push('box-shadow');
          if (st.border || st.borderColor || st.borderWidth) declares.push('border');
          if (st.background || st.backgroundColor) declares.push('background');
          if (st.color) declares.push('color');
          if (st.textDecoration) declares.push('text-decoration');
          out.push({ sel, hasFocus, outlineNone, declares, href });
        }
      } else if (rule.cssRules && (rule.type === 4 || rule.type === 12)) {
        walkRules(rule.cssRules, href, out);
      }
    }
  }
  function rules() {
    if (S.rules) return S.rules;
    const out = [];
    for (const sheet of Array.from(document.styleSheets)) {
      let cssRules = null;
      try { cssRules = sheet.cssRules; } catch (e) { S.crossOriginSheets += 1; continue; }
      if (cssRules) walkRules(cssRules, sheet.href || 'inline <style>', out);
    }
    S.rules = out;
    return out;
  }
  function matchesRule(el, selText) {
    for (const part of selText.split(',')) {
      const stripped = part.replace(FOCUS_PSEUDO, '').trim() || '*';
      if (/::?(before|after|placeholder|selection|marker)/.test(stripped)) continue;
      try { if (el.matches(stripped)) return true; } catch (e) { /* unsupported selector */ }
    }
    return false;
  }
  function ruleInfo(el) {
    const focusRules = [];
    let suppressing = null;
    for (const r of rules()) {
      if (!matchesRule(el, r.sel)) continue;
      if (r.hasFocus && r.declares.length && focusRules.length < 3) focusRules.push({ selector: clip(r.sel, 120), stylesheet: r.href, declares: r.declares });
      if (r.outlineNone && !suppressing) suppressing = { selector: clip(r.sel, 120), stylesheet: r.href, onFocus: r.hasFocus };
    }
    return { focusRuleFound: focusRules.length > 0, focusRules, suppressingRule: suppressing };
  }

  // ── containers ────────────────────────────────────────────────────────────
  const DIALOG_SEL = 'dialog,[role="dialog"],[role="alertdialog"],[aria-modal="true"]';
  const LANDMARK_SEL = 'nav,header,footer,main,aside,form,[role="navigation"],[role="banner"],[role="search"],[role="contentinfo"],[role="menu"],[role="menubar"]';
  const WIDGET_SEL = '[class*="modal"],[class*="popup"],[class*="pum-"],[class*="drawer"],[class*="offcanvas"],[class*="off-canvas"],[class*="mobile-menu"],[class*="lightbox"],[class*="overlay"]';

  function containerOf(el) {
    const c = el.closest(`${DIALOG_SEL},${LANDMARK_SEL},${WIDGET_SEL}`);
    if (!c) return null;
    return { selector: cssSelector(c).selector, role: roleOf(c) || c.tagName.toLowerCase() };
  }

  function describe(el) {
    const tag = el.tagName.toLowerCase();
    const sel = cssSelector(el);
    const vis = visibility(el);
    const r = el.getBoundingClientRect();
    const tabindexAttr = el.getAttribute('tabindex');
    const inShadow = el.getRootNode && el.getRootNode() instanceof ShadowRoot;
    const href = el.getAttribute('href');
    const out = {
      id: idOf(el),
      tag,
      role: roleOf(el),
      accessibleName: accessibleName(el),
      text: textOf(el),
      ariaLabel: el.getAttribute('aria-label') || null,
      ariaLabelledby: el.getAttribute('aria-labelledby') || null,
      elementId: el.id || null,
      classes: stableClasses(el).slice(0, MAX_CLASSES),
      href: href ? clip(href, 200) : null,
      name: el.getAttribute('name') || null,
      type: el.getAttribute('type') || null,
      tabindex: tabindexAttr === null ? null : tabindexAttr,
      selector: sel.selector,
      selectorUnique: sel.unique,
      domPath: domPath(el),
      rect: {
        x: Math.round(r.left + window.scrollX),
        y: Math.round(r.top + window.scrollY),
        w: Math.round(r.width),
        h: Math.round(r.height),
      },
      visible: vis.visible,
      hiddenReason: vis.why,
      disabled: !!el.disabled || el.getAttribute('aria-disabled') === 'true',
      naturallyFocusable: /^(a|area|button|input|select|textarea|summary|iframe)$/.test(tag) && (tag !== 'a' || el.hasAttribute('href')),
      container: containerOf(el),
      inShadowDom: !!inShadow,
    };
    if (!sel.unique) out.xpath = xpath(el);
    return out;
  }

  // ── tabbable set + baseline (unfocused) styles ────────────────────────────
  const TABBABLE_SEL = [
    'a[href]', 'area[href]', 'button', 'input:not([type="hidden"])', 'select', 'textarea', 'summary',
    'iframe', '[tabindex]', '[contenteditable=""]', '[contenteditable="true"]', 'audio[controls]', 'video[controls]',
  ].join(',');

  // querySelectorAll that also descends into OPEN shadow roots.
  function deepAll(root, sel, out) {
    root.querySelectorAll(sel).forEach((e) => out.push(e));
    root.querySelectorAll('*').forEach((e) => {
      if (e.shadowRoot) deepAll(e.shadowRoot, sel, out);
    });
    return out;
  }

  function tabbable() {
    const seenRadioGroups = new Set();
    return deepAll(document, TABBABLE_SEL, []).filter((el) => {
      if (el.disabled) return false;
      const ti = el.getAttribute('tabindex');
      if (ti !== null && parseInt(ti, 10) < 0) return false;
      if (el.closest('[inert]')) return false;
      // A radio group is ONE tab stop (the checked radio, else the first) — the other
      // members are reached with the arrow keys, not Tab, so they are not "unreachable".
      if (el.tagName === 'INPUT' && el.type === 'radio' && el.name) {
        const key = (el.form ? 'f' : 'd') + '|' + el.name;
        const group = deepAll(el.getRootNode(), `input[type="radio"][name="${el.name.replace(/"/g, '\\"')}"]`, []);
        if (group.some((r) => r.checked) ? !el.checked : el !== group[0]) return false;
        if (seenRadioGroups.has(key)) return false;
        seenRadioGroups.add(key);
      }
      return true;
    });
  }

  function baseline(max) {
    const all = tabbable().slice(0, max || 500);
    let visible = 0;
    const ids = [];
    for (const el of all) {
      S.base.set(el, styleSnap(el));
      const vis = visibility(el).visible;
      if (vis) visible += 1;
      ids.push([idOf(el), vis ? 1 : 0]);
    }
    return {
      total: all.length,
      visible,
      ids,
      documentTitle: clip(document.title, 120),
    };
  }

  // ── one traversal step ────────────────────────────────────────────────────
  async function collect() {
    const el = deepActive();
    if (!el || el === document.body || el === document.documentElement) {
      return { focused: false, kind: 'body' };
    }
    // With transitions disabled (see noTransitions) styles are final immediately;
    // otherwise wait out this element's own transition so we read the END state.
    const wait = S.transitionsOff ? 0 : Math.min(maxTransitionMs(el) + 30, 350);
    if (wait > 30) await new Promise((r) => setTimeout(r, wait));
    const meta = describe(el);
    const before = S.base.get(el) || null;
    const after = styleSnap(el);
    const info = ruleInfo(el);
    return {
      focused: true,
      kind: el.tagName.toLowerCase() === 'iframe' ? 'iframe' : 'element',
      meta,
      before,
      after,
      background: effectiveBackground(el),
      wasInBaseline: !!before,
      ...info,
    };
  }

  // ── trap container probing ────────────────────────────────────────────────
  function commonAncestor(els) {
    if (!els.length) return null;
    let a = els[0];
    while (a) {
      if (els.every((e) => a.contains(e))) return a;
      a = a.parentElement;
    }
    return document.body;
  }

  const CLOSE_RE = /\b(close|dismiss|cancel|no thanks|not now|got it|×|✕|x)\b/i;
  function containerState(node) {
    if (!node || !node.isConnected) return { present: false, visible: false };
    return { present: true, visible: visibility(node).visible };
  }

  // Facts about the container that holds a (suspected) trap. `ids` are element ids
  // from S.ids.
  function probeContainer(ids) {
    const els = ids.map((i) => S.els[i - 1]).filter(Boolean);
    if (!els.length) return null;
    const lca = commonAncestor(els);
    const dialog = lca && (lca.closest ? lca.closest(DIALOG_SEL) : null);
    const widget = lca && lca.closest ? lca.closest(WIDGET_SEL) : null;
    const node = dialog || (lca && lca.nodeType === 1 ? lca : null) || widget;
    const holder = dialog || widget || node;
    if (!holder) return null;
    const cs = getComputedStyle(holder);
    const closeCandidates = Array.from(holder.querySelectorAll('button,[role="button"],a,input[type="button"]')).filter((b) => {
      const label = `${b.getAttribute('aria-label') || ''} ${b.textContent || ''} ${b.getAttribute('title') || ''} ${typeof b.className === 'string' ? b.className : ''}`;
      return CLOSE_RE.test(label) || /close|dismiss/i.test(typeof b.className === 'string' ? b.className : '');
    });
    const closeEl = closeCandidates[0] || null;
    return {
      nodeId: idOf(holder),
      selector: cssSelector(holder).selector,
      tag: holder.tagName.toLowerCase(),
      role: roleOf(holder) || (holder.tagName.toLowerCase() === 'dialog' ? 'dialog' : null),
      ariaModal: holder.getAttribute('aria-modal') === 'true',
      isDialogLike: !!dialog,
      isWidgetLike: !!widget,
      visible: visibility(holder).visible,
      position: cs.position,
      zIndex: cs.zIndex,
      hasCloseControl: !!closeEl,
      closeControl: closeEl ? { selector: cssSelector(closeEl).selector, name: accessibleName(closeEl) } : null,
      focusableInside: Array.from(holder.querySelectorAll(TABBABLE_SEL)).filter((e) => visibility(e).visible).length,
    };
  }

  function probeState(nodeId) {
    const node = S.els[nodeId - 1];
    const active = deepActive();
    return {
      ...containerState(node),
      activeInside: !!(node && active && node.contains(active)),
      activeTag: active ? active.tagName.toLowerCase() : null,
    };
  }

  // ── technology hints ──────────────────────────────────────────────────────
  function technology() {
    const sig = [];
    const html = document.documentElement;
    const generator = (document.querySelector('meta[name="generator"]') || {}).content || '';
    const body = document.body;
    const bodyClass = body ? (typeof body.className === 'string' ? body.className : '') : '';
    const hrefs = Array.from(document.querySelectorAll('link[rel="stylesheet"],script[src]')).map((n) => n.href || n.src || '').join(' ');
    const themeMatch = hrefs.match(/\/wp-content\/themes\/([^/]+)\//);
    const t = { cms: null, builder: null, theme: themeMatch ? themeMatch[1] : null, cssFramework: null, jsFramework: null, generator: clip(generator, 60) || null, signals: sig };

    if (/WordPress/i.test(generator) || /wp-content|wp-includes/.test(hrefs) || /wp-(admin|json)/.test(hrefs) || /\bwp-/.test(bodyClass)) { t.cms = 'WordPress'; sig.push('wp-content assets'); }
    if (document.querySelector('[class*="et_pb_"],.et-l,#et-boc') || /\bet_divi|\bet-db\b|\bet_pb_/.test(bodyClass)) { t.builder = 'Divi'; sig.push('et_pb_* classes'); }
    else if (document.querySelector('.elementor,[data-elementor-type]')) { t.builder = 'Elementor'; sig.push('.elementor'); }
    else if (document.querySelector('.vc_row,.wpb_wrapper')) { t.builder = 'WPBakery'; sig.push('.vc_row'); }
    else if (document.querySelector('.fl-builder-content')) { t.builder = 'Beaver Builder'; }
    else if (document.querySelector('[class*="wp-block-"]')) { t.builder = 'Gutenberg'; sig.push('wp-block-* classes'); }
    if (/wix\.com|wixstatic/.test(hrefs)) t.cms = t.cms || 'Wix';
    if (/squarespace/i.test(generator + hrefs)) t.cms = t.cms || 'Squarespace';
    if (/shopify/i.test(hrefs) || window.Shopify) t.cms = t.cms || 'Shopify';
    if (/webflow/i.test(hrefs + generator)) t.cms = t.cms || 'Webflow';

    if (window.__NEXT_DATA__ || document.getElementById('__next')) { t.jsFramework = 'Next.js'; sig.push('__next'); }
    else if (window.__NUXT__ || document.getElementById('__nuxt')) t.jsFramework = 'Nuxt';
    else if (document.querySelector('[data-reactroot],[data-reactid]')) t.jsFramework = 'React';
    else if (document.querySelector('[data-v-app],[data-v-]') || window.__VUE__) t.jsFramework = 'Vue';
    else if (window.ng || document.querySelector('[ng-version]')) t.jsFramework = 'Angular';

    const sample = Array.from(document.querySelectorAll('body *')).slice(0, 600);
    let tw = 0;
    for (const n of sample) {
      const c = typeof n.className === 'string' ? n.className : '';
      if (/\b(sm|md|lg|xl|hover|focus|dark):[a-z-]+/.test(c) || /\b(px|py|mx|my|mt|mb|gap|space-x|space-y)-\d/.test(c) || /\bbg-[a-z]+-\d{2,3}\b/.test(c)) tw += 1;
    }
    if (tw >= 15) { t.cssFramework = 'Tailwind'; sig.push('tailwind-style utility classes'); }
    else if (/bootstrap/i.test(hrefs) || document.querySelector('.btn.btn-primary,.navbar-expand-lg,.container-fluid')) { t.cssFramework = 'Bootstrap'; sig.push('bootstrap classes/assets'); }
    return t;
  }

  // Focus styling is usually animated (transition: all .4s). Turning transitions off
  // for the audit makes the computed style the FINAL style at once, so every Tab
  // stop is read in ~ms instead of waiting out each transition. A constructable
  // stylesheet is used because it is not blocked by a page's style-src CSP. The
  // page is a throwaway audit context; animations are left untouched.
  function noTransitions() {
    try {
      const sheet = new CSSStyleSheet();
      sheet.replaceSync('*,*::before,*::after{transition:none!important}');
      document.adoptedStyleSheets = [...document.adoptedStyleSheets, sheet];
      S.transitionsOff = true;
    } catch (e) {
      S.transitionsOff = false;
    }
    return S.transitionsOff;
  }

  Object.defineProperty(window, '__odA11y', {
    value: { noTransitions, baseline, collect, probeContainer, probeState, technology, describeById: (i) => (S.els[i - 1] ? describe(S.els[i - 1]) : null), crossOriginSheets: () => S.crossOriginSheets },
    enumerable: false,
    configurable: true,
  });
})();
