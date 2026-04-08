"""
Content Rules (Legacy — slimmed down)

Title and heading rules have been moved to title_rules.py and heading_rules.py.
Content quality rules moved to general_rules.py.

This file is kept intentionally empty for backward compatibility.
All content rules now live in separate dedicated files.
"""

# No rules to register — content rules distributed across:
# - title_rules.py (rules 1–20)
# - meta_rules.py (rules 21–42)
# - heading_rules.py (rules 84–121, 141, 229)
# - general_rules.py (rules 184–188, 197, 199–201, 208–209)


def register_content_rules(registry):
    """No-op: content rules moved to dedicated files."""
    pass
