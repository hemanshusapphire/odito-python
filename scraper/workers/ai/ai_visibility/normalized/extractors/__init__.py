"""Per-metric extractors. One extractor function per metric, semantic validation,
no shallow existence checks. All consume the immutable RenderedDOM (and, where
useful, the already-parsed JSON-LD graph) — never re-parsing the page."""
