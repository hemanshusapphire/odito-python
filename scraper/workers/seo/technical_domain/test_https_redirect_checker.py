"""
Tests for https_redirect_checker.py.

Covers the redirect scenarios the SSL/technical audit pipeline depends on to
pick the right hostname for the TLS check (see worker.py Step 4-5, which
feeds `final_url` from this module into ssl_checker.check_ssl_certificate).
A wrong redirect classification here doesn't just mislabel the "HTTPS
Redirect" check -- it can point the SSL check at the wrong hostname too.

Run from python_workers/:
    python -m unittest scraper.workers.seo.technical_domain.test_https_redirect_checker -v
"""

import unittest
from unittest import mock

import requests

from scraper.workers.seo.technical_domain import https_redirect_checker as hrc


def _resp(status_code, location=None):
    r = mock.MagicMock(spec=requests.Response)
    r.status_code = status_code
    r.headers = {"Location": location} if location else {}
    return r


class HttpToHttpsRedirectTests(unittest.TestCase):
    def test_http_redirects_directly_to_https_same_host(self):
        """D. HTTP -> HTTPS redirect (same host)."""
        responses = [
            _resp(301, "https://example.com/"),
            _resp(200),
        ]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("example.com")

        self.assertTrue(result["https_redirect"])
        self.assertEqual(result["final_url"], "https://example.com/")
        self.assertEqual(result["redirect_chain"], ["http://example.com", "https://example.com/"])

    def test_https_redirect_to_www(self):
        """E. HTTPS -> www redirect (apex http -> https www, one hop)."""
        responses = [
            _resp(301, "https://www.example.com/"),
            _resp(200),
        ]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("example.com")

        self.assertTrue(result["https_redirect"])
        self.assertEqual(result["final_url"], "https://www.example.com/")

    def test_www_redirects_to_non_www(self):
        """F. www -> non-www redirect."""
        responses = [
            _resp(301, "https://example.com/"),
            _resp(200),
        ]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("www.example.com")

        self.assertTrue(result["https_redirect"])
        self.assertEqual(result["final_url"], "https://example.com/")

    def test_multi_hop_redirect_http_to_https_www(self):
        """Real-world shape reproduced from sapphiredigitalagency.com:
        http://apex -> https://apex -> https://www.apex (two hops)."""
        responses = [
            _resp(301, "https://sapphiredigitalagency.com/"),
            _resp(301, "https://www.sapphiredigitalagency.com/"),
            _resp(200),
        ]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("sapphiredigitalagency.com")

        self.assertTrue(result["https_redirect"])
        self.assertEqual(result["final_url"], "https://www.sapphiredigitalagency.com/")
        self.assertEqual(len(result["redirect_chain"]), 3)

    def test_no_redirect_stays_http_is_not_marked_as_https_redirect(self):
        responses = [_resp(200)]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("plain-http-only.example.com")

        self.assertFalse(result["https_redirect"])
        self.assertEqual(result["final_url"], "http://plain-http-only.example.com")

    def test_redirect_to_different_domain_is_not_marked_as_https_redirect(self):
        responses = [
            _resp(301, "https://totally-different-domain.com/"),
            _resp(200),
        ]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("example.com")

        self.assertFalse(result["https_redirect"])
        self.assertEqual(result["final_url"], "https://totally-different-domain.com/")


class BlockedOrUnreachableTests(unittest.TestCase):
    def test_blocked_crawler_403_does_not_crash_and_reports_no_redirect(self):
        """I. Site blocks the crawler user-agent -- must not be mistaken for
        an SSL failure; check_https_redirect degrades gracefully and
        worker.py falls back to the base domain for the SSL check itself."""
        responses = [_resp(403)]
        with mock.patch.object(requests, "get", side_effect=responses):
            result = hrc.check_https_redirect("blocked.example.com")

        self.assertFalse(result["https_redirect"])
        self.assertEqual(result["final_url"], "http://blocked.example.com")

    def test_dns_failure_returns_safe_default(self):
        with mock.patch.object(
            requests, "get",
            side_effect=requests.exceptions.ConnectionError("Name or service not known"),
        ):
            result = hrc.check_https_redirect("does-not-resolve.invalid")

        self.assertFalse(result["https_redirect"])
        self.assertIsNone(result["final_url"])
        self.assertEqual(result["redirect_chain"], [])

    def test_timeout_returns_safe_default(self):
        with mock.patch.object(requests, "get", side_effect=requests.exceptions.Timeout("timed out")):
            result = hrc.check_https_redirect("slow.example.com")

        self.assertFalse(result["https_redirect"])
        self.assertIsNone(result["final_url"])

    def test_empty_hostname_returns_safe_default(self):
        result = hrc.check_https_redirect("")
        self.assertFalse(result["https_redirect"])
        self.assertIsNone(result["final_url"])


if __name__ == "__main__":
    unittest.main()
