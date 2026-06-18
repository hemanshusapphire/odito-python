"""
Extraction: crawlability signals.

Reads domain_technical_reports (populated by TECHNICAL_DOMAIN worker) to
determine which AI bots are blocked in robots.txt, and whether llms.txt
exists and is accurate.

All output is stored under ai_pages["crawlability"].
"""

from typing import Any


# Bots the V2 rules care about — name matches robots.txt User-agent token.
_BOTS = {
    "gptbot":           "gptbot_blocked",
    "perplexitybot":    "perplexitybot_blocked",
    "claudebot":        "claudebot_blocked",
    "google-extended":  "google_extended_blocked",
    "bingbot":          "bingbot_blocked",
}

# Bots for which we detect an explicit (non-wildcard) Allow directive.
_EXPLICIT_ALLOW_BOTS = {
    "gptbot":        "gptbot_explicit_allow",
    "claudebot":     "claudebot_explicit_allow",
    "perplexitybot": "perplexitybot_explicit_allow",
}


def _is_explicitly_allowed(agent_token: str, robots_rules: list[dict]) -> bool:
    """
    Return True only when the agent has its own named stanza that contains
    an Allow directive covering / (i.e. Allow: / or Allow: with empty path).

    Wildcard (*) entries are intentionally ignored — this function signals
    that a site has made an affirmative, bot-specific declaration of support.
    """
    agent_lower = agent_token.lower()
    for entry in robots_rules:
        if entry.get("user_agent", "").lower() != agent_lower:
            continue
        for directive in entry.get("rules", []):
            if directive.get("type", "").lower() == "allow" and directive.get("path", "") in ("/", ""):
                return True
    return False


def _is_blocked(agent_token: str, robots_rules: list[dict]) -> bool:
    """
    Return True if the given user-agent is blocked (Disallow: /) in the
    parsed robots_rules list, either directly or via a wildcard block with
    no explicit Allow override.

    robots_rules is expected to be a list of dicts:
        [{"user_agent": "GPTBot", "rules": [{"type": "disallow", "path": "/"}]}, ...]
    """
    agent_lower = agent_token.lower()
    wildcard_blocked = False
    agent_blocked = False
    agent_allowed = False

    for entry in robots_rules:
        ua = entry.get("user_agent", "").lower()
        directives = entry.get("rules", [])

        is_wildcard = ua == "*"
        is_match = ua == agent_lower

        if not (is_wildcard or is_match):
            continue

        for directive in directives:
            dtype = directive.get("type", "").lower()
            path = directive.get("path", "")
            if path not in ("/", ""):
                continue
            if dtype == "disallow":
                if is_wildcard:
                    wildcard_blocked = True
                else:
                    agent_blocked = True
            elif dtype == "allow":
                if is_match:
                    agent_allowed = True

    if agent_blocked and not agent_allowed:
        return True
    if wildcard_blocked and not agent_allowed and not agent_blocked:
        return True
    return False


def extract_crawlability(domain_report: dict[str, Any]) -> dict[str, Any]:
    """
    Extract all crawlability signals from the domain technical report.

    Args:
        domain_report: document from domain_technical_reports collection.

    Returns:
        Crawlability sub-document for ai_pages.
    """
    robots_rules: list = domain_report.get("robots_rules", [])

    bots = {}
    for token, field in _BOTS.items():
        bots[field] = _is_blocked(token, robots_rules)
    for token, field in _EXPLICIT_ALLOW_BOTS.items():
        bots[field] = _is_explicitly_allowed(token, robots_rules)

    llms = domain_report.get("llms_txt", {}) or {}
    llms_exists  = bool(llms.get("exists", False))
    llms_urls    = llms.get("urls_listed", []) or []
    llms_broken  = llms.get("broken_urls", [])  or []
    llms_malform = bool(llms.get("malformed", False))

    return {
        "robots": {
            **bots,
            "raw_rules_count": len(robots_rules),
        },
        "llms_txt": {
            "exists":      llms_exists,
            "location":    llms.get("location"),
            "status_code": llms.get("status_code"),
            "byte_size":   llms.get("byte_size"),
            "url_count":   len(llms_urls),
            "broken_urls": llms_broken,
            "malformed":   llms_malform,
            "is_accurate": (
                llms_exists
                and not llms_broken
                and not llms_malform
            ),
        },
    }
