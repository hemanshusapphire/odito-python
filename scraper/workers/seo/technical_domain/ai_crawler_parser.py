"""
AI Crawler Robots.txt Parser  —  extraction only, no scoring.

Parses robots.txt content into normalized per-bot accessibility signals.
Scoring (signal quality → numeric score) is performed exclusively by the
AI Visibility Worker.

Accessibility logic
-------------------
- A bot is BLOCKED when it has an explicit Disallow: / in its own section
  and no Allow: / override in the same section.
- A bot is EXPLICITLY ALLOWED when its own section contains Allow: / (or
  Allow: /*) with no root-level Disallow that overrides it.
- A bot that has an explicit section with only path-specific disallows is
  accessible (its section does not block the root).
- A bot with NO explicit section inherits the wildcard (User-agent: *)
  rules. inheritsWildcard is set to True.
- When robots.txt does not exist, all bots are treated as inheriting an
  implicit allow-all wildcard.

Bug fixes applied (vs previous version)
-----------------------------------------
1. Multiple consecutive User-agent: lines are now grouped together before
   the Disallow/Allow rules that follow them, matching the robots.txt spec.
2. Empty Disallow: (no path) means "allow everything" — it is no longer
   treated as a root block.
3. Disallow: /* is now recognised as a root block.
4. ClaudeBot and anthropic-ai are tracked as independent signals with no
   cross-contamination between the two keys.
"""

from __future__ import annotations

import re
from typing import Dict, List, Tuple


# Each key maps to the SINGLE User-agent string it watches.
# ClaudeBot and anthropicAi are deliberately separated to prevent
# an explicit block of one from masking a block of the other.
_BOTS: Dict[str, str] = {
    "gptbot":         "GPTBot",
    "claudebot":      "ClaudeBot",
    "deepseek":       "DeepSeekBot",
    "perplexitybot":  "PerplexityBot",
    "googleExtended": "Google-Extended",
}

# Pre-compiled case-insensitive pattern per bot key.
_BOT_PATTERNS: Dict[str, re.Pattern] = {
    key: re.compile(rf"^{re.escape(ua)}$", re.IGNORECASE)
    for key, ua in _BOTS.items()
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _blocks_root(disallow_paths: List[str]) -> bool:
    """
    True when the Disallow list contains an entry that blocks root access.

    Recognised root-blocking patterns:
      Disallow: /       — standard full-site block
      Disallow: /*      — Google-extended wildcard full-site block

    NOT treated as root block:
      Disallow:         — empty value means "allow all" per spec
      Disallow: /wp-admin/  — path-specific, does not block root
    """
    for path in disallow_paths:
        stripped = path.strip()
        if stripped in ("/", "/*"):
            return True
    return False


def _allows_root(allow_paths: List[str]) -> bool:
    """True if an Allow entry explicitly re-opens root access."""
    for path in allow_paths:
        if path.strip() in ("/", "/*"):
            return True
    return False


def _parse_sections(content: str) -> Tuple[
    List[str], List[str],                        # wildcard disallows / allows
    Dict[str, List[str]], Dict[str, List[str]],  # ua_disallows / ua_allows
]:
    """
    Parse robots.txt into per-section Disallow/Allow path lists.

    Fix 1 — consecutive User-agent lines:
        Multiple consecutive User-agent: lines before any Disallow/Allow
        share the same rule block.  current_agents APPENDS on each
        User-agent: line and resets only on blank lines.

    Returns
    -------
    wildcard_disallows  Disallow paths under User-agent: *
    wildcard_allows     Allow paths under User-agent: *
    ua_disallows        {ua_lower: [disallow_paths]} for named agents
    ua_allows           {ua_lower: [allow_paths]} for named agents
    """
    wildcard_disallows: List[str] = []
    wildcard_allows: List[str] = []
    ua_disallows: Dict[str, List[str]] = {}
    ua_allows: Dict[str, List[str]] = {}

    current_agents: List[str] = []
    # Track whether the current agent block has received any Disallow/Allow rules.
    # When a new User-agent: line arrives AFTER rules, it starts a fresh block
    # rather than appending to the existing one.
    has_rules = False

    for raw_line in content.splitlines():
        line = raw_line.strip()

        # Strip inline comments (# character in path is not valid in robots.txt)
        if "#" in line:
            line = line[:line.index("#")].strip()

        # Blank line closes the current block
        if not line:
            current_agents = []
            has_rules = False
            continue

        lower = line.lower()

        if lower.startswith("user-agent:"):
            agent = line.split(":", 1)[1].strip().lower()
            if has_rules:
                # Previous block had rules — this UA starts a NEW block
                # (handles malformed robots.txt with no blank line between records)
                current_agents = [agent]
                has_rules = False
            else:
                # FIX 1: Consecutive UA lines before any rules → group together
                current_agents.append(agent)

        elif lower.startswith("disallow:"):
            path = line.split(":", 1)[1].strip()
            # FIX 2: Empty Disallow: means "allow all" — skip entirely
            if path == "":
                continue
            has_rules = True
            for agent in current_agents:
                if agent == "*":
                    wildcard_disallows.append(path)
                else:
                    ua_disallows.setdefault(agent, []).append(path)

        elif lower.startswith("allow:"):
            path = line.split(":", 1)[1].strip()
            if path == "":
                continue
            has_rules = True
            for agent in current_agents:
                if agent == "*":
                    wildcard_allows.append(path)
                else:
                    ua_allows.setdefault(agent, []).append(path)

    return wildcard_disallows, wildcard_allows, ua_disallows, ua_allows


def _bot_signal(
    key: str,
    pattern: re.Pattern,
    wildcard_disallows: List[str],
    wildcard_allows: List[str],
    ua_disallows: Dict[str, List[str]],
    ua_allows: Dict[str, List[str]],
) -> Dict:
    """
    Compute the normalised accessibility signal for a single bot.

    FIX 4 — independent per-bot signals:
        Each bot key matches ONLY its own UA string (pattern).  There is no
        cross-pollination between claudebot and anthropicAi.
    """
    # Collect explicit Disallow/Allow paths for this bot's UA
    explicit_disallows: List[str] = []
    explicit_allows: List[str] = []
    mentioned = False

    for ua_lower, paths in ua_disallows.items():
        if pattern.match(ua_lower):
            mentioned = True
            explicit_disallows.extend(paths)

    for ua_lower, paths in ua_allows.items():
        if pattern.match(ua_lower):
            mentioned = True
            explicit_allows.extend(paths)

    explicitly_blocked = _blocks_root(explicit_disallows) and not _allows_root(explicit_allows)
    explicitly_allowed = _allows_root(explicit_allows) and not explicitly_blocked

    # Wildcard inheritance applies only when the bot has NO explicit section
    inherits_wildcard = not mentioned

    if explicitly_blocked:
        accessible = False
    elif explicitly_allowed:
        accessible = True
    elif mentioned:
        # Has an explicit section but it only restricts specific sub-paths
        accessible = True
    else:
        # Inherits wildcard — check if wildcard blocks root
        wildcard_blocks_root = _blocks_root(wildcard_disallows) and not _allows_root(wildcard_allows)
        accessible = not wildcard_blocks_root

    return {
        "mentioned":         mentioned,
        "explicitlyAllowed": explicitly_allowed,
        "explicitlyBlocked": explicitly_blocked,
        "inheritsWildcard":  inherits_wildcard,
        "accessible":        accessible,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def parse_ai_crawler_signals(robots_content: str, robots_exists: bool) -> Dict:
    """
    Parse robots.txt into normalised AI crawler accessibility signals.

    Parameters
    ----------
    robots_content  Raw text of robots.txt (empty string when unavailable).
    robots_exists   True when robots.txt was reachable (HTTP 200).

    Returns
    -------
    Dict keyed by bot identifier (gptbot, claudebot, anthropicAi,
    perplexitybot, googleExtended).  Each value is the signal dict
    produced by _bot_signal().
    """
    if not robots_exists or not robots_content:
        # No robots.txt → implicit allow-all for all bots
        return {
            key: {
                "mentioned":         False,
                "explicitlyAllowed": False,
                "explicitlyBlocked": False,
                "inheritsWildcard":  True,
                "accessible":        True,
            }
            for key in _BOTS
        }

    wildcard_disallows, wildcard_allows, ua_disallows, ua_allows = _parse_sections(robots_content)

    return {
        key: _bot_signal(
            key,
            _BOT_PATTERNS[key],
            wildcard_disallows,
            wildcard_allows,
            ua_disallows,
            ua_allows,
        )
        for key in _BOTS
    }
