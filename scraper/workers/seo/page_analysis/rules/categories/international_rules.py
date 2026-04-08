"""
International SEO Rules (Rules 66–70, 126–127)

Hreflang validation: presence, self-referencing, x-default,
valid language codes, valid URLs.
"""

import re
from urllib.parse import urlparse
from ..base_seo_rule import BaseSEORuleV2


# ── Helpers ───────────────────────────────────────────────────

ISO_639_1_CODES = {
    "aa", "ab", "ae", "af", "ak", "am", "an", "ar", "as", "av",
    "ay", "az", "ba", "be", "bg", "bh", "bi", "bm", "bn", "bo",
    "br", "bs", "ca", "ce", "ch", "co", "cr", "cs", "cu", "cv",
    "cy", "da", "de", "dv", "dz", "ee", "el", "en", "eo", "es",
    "et", "eu", "fa", "ff", "fi", "fj", "fo", "fr", "fy", "ga",
    "gd", "gl", "gn", "gu", "gv", "ha", "he", "hi", "ho", "hr",
    "ht", "hu", "hy", "hz", "ia", "id", "ie", "ig", "ii", "ik",
    "io", "is", "it", "iu", "ja", "jv", "ka", "kg", "ki", "kj",
    "kk", "kl", "km", "kn", "ko", "kr", "ks", "ku", "kv", "kw",
    "ky", "la", "lb", "lg", "li", "ln", "lo", "lt", "lu", "lv",
    "mg", "mh", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my",
    "na", "nb", "nd", "ne", "ng", "nl", "nn", "no", "nr", "nv",
    "ny", "oc", "oj", "om", "or", "os", "pa", "pi", "pl", "ps",
    "pt", "qu", "rm", "rn", "ro", "ru", "rw", "sa", "sc", "sd",
    "se", "sg", "si", "sk", "sl", "sm", "sn", "so", "sq", "sr",
    "ss", "st", "su", "sv", "sw", "ta", "te", "tg", "th", "ti",
    "tk", "tl", "tn", "to", "tr", "ts", "tt", "tw", "ty", "ug",
    "uk", "ur", "uz", "ve", "vi", "vo", "wa", "wo", "xh", "yi",
    "yo", "za", "zh", "zu",
}


def _is_valid_url(url_str):
    if not url_str:
        return False
    try:
        parsed = urlparse(str(url_str).strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
# HREFLANG RULES (Rules 66–70, 126–127)
# ═══════════════════════════════════════════════════════════════

class HreflangPresentRule(BaseSEORuleV2):
    rule_id = "HREFLANG_PRESENT"
    rule_no = 66
    category = "International"
    severity = "info"
    description = "Hreflang tags should be present for multilingual sites"

    def evaluate(self, normalized, job_id, project_id, url):
        if not normalized.get("hreflangs"):
            return [self.create_issue(
                job_id, project_id, url,
                "Hreflang tags are missing",
                "None", "Add hreflang for multilingual content",
                data_key="hreflangs"
            )]
        return []


class HreflangSelfReferenceRule(BaseSEORuleV2):
    rule_id = "HREFLANG_SELF_REFERENCE"
    rule_no = 67
    category = "International"
    severity = "high"
    description = "Self-referencing hreflang URL should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        if not hreflangs:
            return []
        page_url = normalized.get("url", "").rstrip("/").lower()
        has_self = any(
            h.get("href", "").rstrip("/").lower() == page_url
            for h in hreflangs if isinstance(h, dict)
        )
        if not has_self:
            return [self.create_issue(
                job_id, project_id, url,
                "No self-referencing hreflang URL found",
                page_url, "Add hreflang pointing to this page",
                data_key="hreflangs"
            )]
        return []


class HreflangXDefaultRule(BaseSEORuleV2):
    rule_id = "HREFLANG_X_DEFAULT"
    rule_no = 68
    category = "International"
    severity = "high"
    description = "x-default hreflang should be present"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        if not hreflangs:
            return []
        has_xdefault = any(
            h.get("hreflang", "").lower() == "x-default"
            for h in hreflangs if isinstance(h, dict)
        )
        if not has_xdefault:
            return [self.create_issue(
                job_id, project_id, url,
                "x-default hreflang is missing",
                "None", "Add x-default hreflang",
                data_key="hreflangs"
            )]
        return []


class HreflangValidLangCodeRule(BaseSEORuleV2):
    rule_id = "HREFLANG_VALID_LANG_CODE"
    rule_no = 69
    category = "International"
    severity = "high"
    description = "Hreflang must use valid language code or x-default"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        for h in hreflangs:
            if isinstance(h, dict):
                lang = h.get("hreflang", "").strip().lower()
                if lang == "x-default":
                    continue
                primary = lang.split("-")[0]
                if primary not in ISO_639_1_CODES:
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"Invalid hreflang code: {lang}",
                        lang, "Use valid ISO 639-1 code or x-default",
                        data_key="hreflangs"
                    )]
        return []


class HreflangValidUrlRule(BaseSEORuleV2):
    rule_id = "HREFLANG_VALID_URL"
    rule_no = 70
    category = "International"
    severity = "high"
    description = "Each hreflang must have a valid URL"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        for h in hreflangs:
            if isinstance(h, dict):
                href = h.get("href", "")
                if href and not _is_valid_url(href):
                    return [self.create_issue(
                        job_id, project_id, url,
                        f"Invalid hreflang URL: {href}",
                        href[:80], "Use valid absolute URL",
                        data_key="hreflangs"
                    )]
        return []


class HreflangIncludesTargetLangsRule(BaseSEORuleV2):
    rule_id = "HREFLANG_INCLUDES_TARGET_LANGS"
    rule_no = 126
    category = "International"
    severity = "medium"
    description = "Hreflang should include target languages"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        if hreflangs:
            langs = [h.get("hreflang", "") for h in hreflangs if isinstance(h, dict)]
            non_default = [l for l in langs if l.lower() != "x-default"]
            if len(non_default) < 1:
                return [self.create_issue(
                    job_id, project_id, url,
                    "Hreflang present but no target languages declared",
                    "0 target langs", "Add language-specific hreflang tags",
                    data_key="hreflangs"
                )]
        return []


class HreflangAllValidRule(BaseSEORuleV2):
    rule_id = "HREFLANG_ALL_VALID"
    rule_no = 127
    category = "International"
    severity = "high"
    description = "All hreflang tags must be valid"

    def evaluate(self, normalized, job_id, project_id, url):
        hreflangs = normalized.get("hreflangs", [])
        invalid_count = 0
        for h in hreflangs:
            if isinstance(h, dict):
                if not h.get("hreflang") or not h.get("href"):
                    invalid_count += 1
        if invalid_count > 0:
            return [self.create_issue(
                job_id, project_id, url,
                f"{invalid_count} invalid hreflang tag(s)",
                invalid_count, "Each hreflang must have both lang and href",
                data_key="hreflangs"
            )]
        return []


class MultilingualSupportRule(BaseSEORuleV2):
    rule_id = "MULTILINGUAL_SUPPORT"
    rule_no = 147
    category = "International"
    severity = "medium"
    description = "Page should support multilingual (hreflang + lang)"

    def evaluate(self, normalized, job_id, project_id, url):
        has_lang = bool(normalized.get("html_lang"))
        has_hreflang = bool(normalized.get("hreflangs"))
        if not has_lang and not has_hreflang:
            return [self.create_issue(
                job_id, project_id, url,
                "Page has no language signals (no lang attribute, no hreflang)",
                "None", "Add html lang and/or hreflang tags",
                data_key="html_lang"
            )]
        return []


# ── Registration ──────────────────────────────────────────────

def register_international_rules(registry):
    """Register all International rules"""
    registry.register(HreflangPresentRule())
    registry.register(HreflangSelfReferenceRule())
    registry.register(HreflangXDefaultRule())
    registry.register(HreflangValidLangCodeRule())
    registry.register(HreflangValidUrlRule())
    registry.register(HreflangIncludesTargetLangsRule())
    registry.register(HreflangAllValidRule())
    registry.register(MultilingualSupportRule())
