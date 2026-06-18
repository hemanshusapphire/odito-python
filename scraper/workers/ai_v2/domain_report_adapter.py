"""
Adapter: V1 domain_technical_reports → V2 domain_report format.

V1 stores (from technical_domain/worker.py):
    robotsContent:    str   raw robots.txt text
    robotsExists:     bool
    sitemapExists:    bool
    sitemapContent:   str
    llmsTxt:          {exists, valid, hasContent, contentLength, content,
                       hasAllow, hasDisallow, mentionedBots, status}
    aiCrawlerSignals: {gptbot: {...}, claudebot: {...}, ...}

V2 extraction layer expects:
    robots_rules:     list[{user_agent: str, rules: [{type: str, path: str}]}]
    llms_txt:         {exists, urls_listed, broken_urls, malformed,
                       location, status_code, byte_size}
    sitemap:          {exists: bool, url: str|None}
"""

import sys, os
from typing import Any
from bson import ObjectId

_PW_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _PW_ROOT not in sys.path:
    sys.path.insert(0, _PW_ROOT)

from db import domain_technical_reports


def _parse_robots_rules(robots_content: str) -> list[dict]:
    """
    Parse raw robots.txt string into V2 robots_rules format:
        [{"user_agent": str, "rules": [{"type": "allow"|"disallow", "path": str}]}]

    Handles the standard robots.txt block structure:
        User-agent: GPTBot
        Disallow: /
    """
    if not robots_content:
        return []

    groups: list[dict] = []
    current_agents: list[str] = []
    current_rules: list[dict] = []

    def _flush() -> None:
        if current_agents and current_rules:
            for ua in current_agents:
                groups.append({"user_agent": ua, "rules": list(current_rules)})

    for raw_line in robots_content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue

        key, _, val = line.partition(":")
        key = key.strip().lower()
        val = val.split("#")[0].strip()  # strip inline comments

        if key == "user-agent":
            if current_rules:
                _flush()
                current_agents = []
                current_rules = []
            current_agents.append(val)
        elif key == "disallow":
            current_rules.append({"type": "disallow", "path": val})
        elif key == "allow":
            current_rules.append({"type": "allow", "path": val})

    _flush()
    return groups


def build_v2_domain_report(project_id: Any) -> dict[str, Any]:
    """
    Fetch the V1 domain_technical_reports document and translate it into the
    dict shape that V2 extraction layer expects.

    Returns an empty-signal dict (all rules evaluate to defaults) when no
    document exists for this project.
    """
    pid = ObjectId(project_id) if not isinstance(project_id, ObjectId) else project_id
    doc = domain_technical_reports.find_one({"projectId": pid}) or {}

    # robots_rules — parse raw robots.txt content into V2 list format
    robots_content: str = doc.get("robotsContent", "") or ""
    robots_rules = _parse_robots_rules(robots_content)

    # llms_txt — translate V1 llmsTxt structure to V2 expectations
    llms_v1: dict = doc.get("llmsTxt", {}) or {}
    llms_txt: dict[str, Any] = {
        "exists":      bool(llms_v1.get("exists", False)),
        # V1 "valid" means the file is well-formed; absence of validity = malformed
        "malformed":   bool(llms_v1.get("exists") and not llms_v1.get("valid", True)),
        # V1 doesn't track individual URL entries inside llms.txt
        "urls_listed": [],
        "broken_urls": [],
        # V1 doesn't record HTTP-level metadata for llms.txt fetch
        "location":    None,
        "status_code": None,
        "byte_size":   llms_v1.get("contentLength"),
    }

    # sitemap
    sitemap: dict[str, Any] = {
        "exists": bool(doc.get("sitemapExists", False)),
        # V1 doesn't reliably store the sitemap URL
        "url":    None,
    }

    return {
        "robots_rules":  robots_rules,
        "robots_exists": bool(doc.get("robotsExists", False)),
        "llms_txt":      llms_txt,
        "sitemap":       sitemap,
    }
