"""
Tests for response-integrity validation (forensic fix, 2026-08-11).

Root cause under test: krishnaeyecentre.com and vasaneye.com respond with
Content-Encoding: br. Without the `brotli` package, requests/urllib3 has no
decoder for it and silently returns the still-compressed bytes; fetcher.py's
own raw.decode(..., errors="replace") then force-decodes that binary as text,
producing a U+FFFD-dense string that BeautifulSoup parses without raising —
yielding a document with extraction_status="SUCCESS" but empty title/meta/
canonical/images/links. Confirmed live against production for both domains,
and reproduced here with genuine gzip/deflate/brotli compression (not
simulated) so the fix is verified against real codecs, not just strings.

Stdlib unittest only, matching this repo's existing convention (see
page_scraping/test_page_scraping_upsert.py). No live network, no live Mongo —
compressed bytes are constructed in-process and fed through a real
urllib3.HTTPResponse so genuine decompression code runs, but no socket is
opened.

Run from python_workers/:
    python -m unittest scraper.shared.test_fetcher_integrity -v
"""

import gzip
import io
import unittest
import zlib
from unittest import mock

import requests
import urllib3

from scraper.shared import fetcher
from scraper.shared import orchestrator


# ---------------------------------------------------------------------------
# Real HTML fixtures
# ---------------------------------------------------------------------------
VALID_HTML = """<!DOCTYPE html>
<html lang="en-US">
<head>
  <title>Posterior Uveitis - Krishna Eye Centre</title>
  <meta name="description" content="Learn about posterior uveitis treatment.">
  <link rel="canonical" href="https://krishnaeyecentre.com/knowledge-centre/posterior-uveitis">
</head>
<body>
  <nav><a href="/">Home</a><a href="/contact">Contact</a></nav>
  <h1>Posterior Uveitis</h1>
  <p>Posterior uveitis is inflammation of the back part of the eye.</p>
  <img src="/eye.jpg" alt="Eye diagram">
</body>
</html>"""

# A minimal-but-legitimate page — must NOT be rejected (no title/meta/canonical).
MINIMAL_VALID_HTML = "<html><body><p>Just some text.</p></body></html>"


def _corrupt_binary_as_text(raw: bytes) -> str:
    """Reproduce exactly what fetcher.py's own decode step does to
    still-compressed bytes: force-decode as UTF-8 with errors='replace'."""
    return raw.decode("utf-8", errors="replace")


class ValidateHtmlIntegrityTests(unittest.TestCase):
    """Case A / detection logic — the core validation function in isolation."""

    def test_valid_html_passes(self):
        is_valid, diag = fetcher.validate_html_integrity(VALID_HTML)
        self.assertTrue(is_valid)
        self.assertIsNone(diag["reason"])
        self.assertEqual(diag["replacement_char_count"], 0)

    def test_minimal_html_without_title_meta_canonical_still_passes(self):
        # Explicit requirement: never reject a legitimate minimal page just
        # because it lacks title/meta/canonical/images/links.
        is_valid, diag = fetcher.validate_html_integrity(MINIMAL_VALID_HTML)
        self.assertTrue(is_valid)

    def test_empty_content_fails(self):
        is_valid, diag = fetcher.validate_html_integrity("")
        self.assertFalse(is_valid)
        self.assertEqual(diag["reason"], "EMPTY_CONTENT")

    def test_gzip_bytes_forced_as_text_is_detected_as_corrupted(self):
        # Reproduces the exact production mechanism with a genuinely
        # gzip-compressed payload that never got decompressed.
        compressed = gzip.compress(VALID_HTML.encode("utf-8"))
        corrupted = _corrupt_binary_as_text(compressed)
        is_valid, diag = fetcher.validate_html_integrity(corrupted)
        self.assertFalse(is_valid)
        self.assertIn(diag["reason"], ("HIGH_REPLACEMENT_CHAR_DENSITY", "NO_HTML_STRUCTURE_MARKER"))

    def test_brotli_bytes_forced_as_text_is_detected_as_corrupted(self):
        try:
            import brotli
        except ImportError:
            self.skipTest("brotli not installed in this environment")
        compressed = brotli.compress(VALID_HTML.encode("utf-8"))
        corrupted = _corrupt_binary_as_text(compressed)
        is_valid, diag = fetcher.validate_html_integrity(corrupted)
        self.assertFalse(is_valid)

    def test_real_production_fingerprint_is_detected(self):
        # First bytes captured live from krishnaeyecentre.com's actual
        # Content-Encoding: br response (pre-decompression), reproduced here
        # verbatim as a regression anchor for this exact incident.
        raw = bytes.fromhex("83ff1f0080aaaaaaeaff1e9793b96695")
        corrupted = _corrupt_binary_as_text(raw)
        is_valid, diag = fetcher.validate_html_integrity(corrupted)
        self.assertFalse(is_valid)


class DownloadStreamingDecompressionTests(unittest.TestCase):
    """Case B — genuine gzip/deflate/brotli round-trip through real urllib3
    decompression (not mocked), proving the fix doesn't require any manual
    decompression code: once the codec is available, requests/urllib3
    already handles it correctly end to end."""

    def _fake_response(self, body: bytes, content_encoding: "str | None"):
        headers = {"Content-Type": "text/html; charset=UTF-8"}
        if content_encoding:
            headers["Content-Encoding"] = content_encoding
        http_resp = urllib3.HTTPResponse(
            body=io.BytesIO(body),
            headers=headers,
            status=200,
            preload_content=False,
        )
        resp = requests.Response()
        resp.status_code = 200
        resp.raw = http_resp
        resp.url = "https://example.test/page"
        resp.headers = requests.structures.CaseInsensitiveDict(headers)
        # Real requests derives this from the Content-Type charset param when
        # the response is constructed by the transport adapter; our manually
        # built fixture must do the same so fetcher.py's
        # `res.encoding or res.apparent_encoding` doesn't fall through to
        # apparent_encoding (which re-reads .content, already consumed by
        # iter_content() by that point).
        resp.encoding = requests.utils.get_encoding_from_headers(resp.headers) or "utf-8"
        return resp

    def test_gzip_decompresses_to_valid_html(self):
        compressed = gzip.compress(VALID_HTML.encode("utf-8"))
        fake_resp = self._fake_response(compressed, "gzip")
        with mock.patch.object(fetcher, "_session") as mock_session:
            mock_session.get.return_value = fake_resp
            res, html = fetcher._download_html_streaming(
                "https://example.test/page", {}, timeout=10, max_bytes=10_000_000
            )
        self.assertIn("<!DOCTYPE html>", html)
        is_valid, _ = fetcher.validate_html_integrity(html)
        self.assertTrue(is_valid)

    def test_deflate_decompresses_to_valid_html(self):
        compressed = zlib.compress(VALID_HTML.encode("utf-8"))
        fake_resp = self._fake_response(compressed, "deflate")
        with mock.patch.object(fetcher, "_session") as mock_session:
            mock_session.get.return_value = fake_resp
            res, html = fetcher._download_html_streaming(
                "https://example.test/page", {}, timeout=10, max_bytes=10_000_000
            )
        self.assertIn("<!DOCTYPE html>", html)
        is_valid, _ = fetcher.validate_html_integrity(html)
        self.assertTrue(is_valid)

    def test_brotli_decompresses_to_valid_html_when_package_available(self):
        try:
            import brotli
        except ImportError:
            self.skipTest("brotli not installed in this environment")
        compressed = brotli.compress(VALID_HTML.encode("utf-8"))
        fake_resp = self._fake_response(compressed, "br")
        with mock.patch.object(fetcher, "_session") as mock_session:
            mock_session.get.return_value = fake_resp
            res, html = fetcher._download_html_streaming(
                "https://example.test/page", {}, timeout=10, max_bytes=10_000_000
            )
        self.assertIn("<!DOCTYPE html>", html)
        is_valid, _ = fetcher.validate_html_integrity(html)
        self.assertTrue(is_valid)


class FetchHtmlIntegrationTests(unittest.TestCase):
    """Case C end to end — fetch_html() must never return corrupted content
    as if it were a successful fetch."""

    def test_corrupted_content_with_no_browser_available_raises(self):
        compressed = gzip.compress(VALID_HTML.encode("utf-8"))
        corrupted_html = _corrupt_binary_as_text(compressed)

        fake_res = mock.Mock()
        fake_res.url = "https://example.test/page"
        fake_res.status_code = 200
        fake_res.headers = {"Content-Encoding": "br", "Content-Type": "text/html"}

        with mock.patch.object(fetcher, "_download_html_streaming", return_value=(fake_res, corrupted_html)), \
             mock.patch.object(fetcher, "PLAYWRIGHT_AVAILABLE", False), \
             mock.patch.object(fetcher, "SELENIUM_AVAILABLE", False), \
             mock.patch.object(fetcher, "_is_js_cached", return_value=None):
            with self.assertRaises(fetcher.InvalidHtmlContentError) as ctx:
                fetcher.fetch_html("https://example.test/page", timeout=5)

        diag = ctx.exception.diagnostics
        self.assertEqual(diag["content_encoding"], "br")
        self.assertEqual(diag["http_status"], 200)
        self.assertIn(diag["reason"], ("HIGH_REPLACEMENT_CHAR_DENSITY", "NO_HTML_STRUCTURE_MARKER"))

    def test_corrupted_content_recovers_via_browser_fallback(self):
        compressed = gzip.compress(VALID_HTML.encode("utf-8"))
        corrupted_html = _corrupt_binary_as_text(compressed)

        fake_res = mock.Mock()
        fake_res.url = "https://example.test/page"
        fake_res.status_code = 200
        fake_res.headers = {"Content-Encoding": "br", "Content-Type": "text/html"}

        with mock.patch.object(fetcher, "_download_html_streaming", return_value=(fake_res, corrupted_html)), \
             mock.patch.object(fetcher, "PLAYWRIGHT_AVAILABLE", True), \
             mock.patch.object(fetcher, "fetch_html_playwright", return_value=(VALID_HTML, 200, 500, {}, "https://example.test/page")), \
             mock.patch.object(fetcher, "_is_js_cached", return_value=None):
            html, status, rt, headers, final_url = fetcher.fetch_html("https://example.test/page", timeout=5)

        self.assertIn("<!DOCTYPE html>", html)
        is_valid, _ = fetcher.validate_html_integrity(html)
        self.assertTrue(is_valid)


class ExtractionStatusSemanticsTests(unittest.TestCase):
    """Case A/C at the orchestrator layer — extraction_status must reflect
    real structural validity, not merely 'no exception was raised'."""

    def test_valid_html_yields_success_with_real_fields(self):
        result = orchestrator.extract_comprehensive_seo_data(
            VALID_HTML, "https://krishnaeyecentre.com/knowledge-centre/posterior-uveitis"
        )
        self.assertEqual(result["extraction_status"], "SUCCESS")
        self.assertEqual(result["title"], "Posterior Uveitis - Krishna Eye Centre")
        self.assertEqual(
            result["canonical"],
            "https://krishnaeyecentre.com/knowledge-centre/posterior-uveitis",
        )
        self.assertTrue(result["meta_tags"])

    def test_corrupted_html_yields_failed_not_success(self):
        compressed = gzip.compress(VALID_HTML.encode("utf-8"))
        corrupted_html = _corrupt_binary_as_text(compressed)

        result = orchestrator.extract_comprehensive_seo_data(
            corrupted_html, "https://krishnaeyecentre.com/knowledge-centre/posterior-uveitis"
        )
        self.assertEqual(result["extraction_status"], "FAILED")
        self.assertIn("extraction_failure_reason", result)
        # Must short-circuit before running the real extraction pipeline —
        # no title/meta_tags/canonical keys should even be present.
        self.assertNotIn("title", result)
        self.assertNotIn("meta_tags", result)

    def test_classify_fetch_exception_reports_invalid_html_response(self):
        exc = fetcher.InvalidHtmlContentError(
            "test",
            diagnostics={
                "reason": "HIGH_REPLACEMENT_CHAR_DENSITY",
                "content_encoding": "br",
                "content_type": "text/html; charset=UTF-8",
                "replacement_char_ratio": 0.44,
                "http_status": 200,
            },
        )
        classified = orchestrator._classify_fetch_exception(exc)
        self.assertEqual(classified["failure_type"], "INVALID_HTML_RESPONSE")
        self.assertIn("br", classified["error_message"])
        self.assertIn("HIGH_REPLACEMENT_CHAR_DENSITY", classified["error_message"])


if __name__ == "__main__":
    unittest.main()
