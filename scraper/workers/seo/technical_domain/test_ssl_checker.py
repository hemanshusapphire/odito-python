"""
Tests for ssl_checker.py.

Root cause under test (2026-09): check_ssl_certificate() imported the
`cryptography` package but that package was never declared in
requirements.txt / installed in the worker's environment. When missing, the
function silently returned ssl_valid=False for EVERY hostname on every
audit -- including sites with a perfectly valid, browser-trusted
certificate. That is what produced the "SSL certificate not found or
invalid" CRITICAL false positive reported for sapphiredigitalagency.com
(and, in fact, every project, since the bug was environmental, not
domain-specific).

Where practical these tests spin up a real local TLS server presenting a
genuine, freshly-generated certificate and drive it through the exact same
socket + ssl.SSLContext handshake code that production uses (SNI, hostname
matching, chain-of-trust validation) -- rather than mocking Python's ssl
internals -- so a refactor that breaks real validation would break these
tests too. `socket.create_connection` is patched only to redirect the
(hostname, port) DNS-resolution step at 127.0.0.1, while the hostname
string itself still flows into `wrap_socket(server_hostname=...)` for a
real SNI + certificate-hostname check.

Run from python_workers/:
    python -m unittest scraper.workers.seo.technical_domain.test_ssl_checker -v
"""

import datetime
import os
import socket
import ssl
import tempfile
import threading
import unittest
from unittest import mock

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from scraper.workers.seo.technical_domain import ssl_checker


# ---------------------------------------------------------------------------
# Certificate + local TLS server helpers
# ---------------------------------------------------------------------------

def _generate_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _make_cert(common_name, sans, not_before, not_after, key=None):
    """Self-signed certificate. Used both as the "server" cert and, when
    also loaded into the client's trust store, as its own trust anchor --
    the standard way to stand up a trusted local TLS test server."""
    key = key or _generate_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
    )
    if sans:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(s) for s in sans]),
            critical=False,
        )
    cert = builder.sign(key, hashes.SHA256())
    return cert, key


def _pem_files(cert, key):
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    cert_fd, cert_path = tempfile.mkstemp(suffix=".crt.pem")
    os.write(cert_fd, cert_pem)
    os.close(cert_fd)
    key_fd, key_path = tempfile.mkstemp(suffix=".key.pem")
    os.write(key_fd, key_pem)
    os.close(key_fd)
    return cert_path, key_path


class _LocalTLSServer:
    """A minimal local TLS server presenting a given cert/key, so client
    tests exercise a real handshake instead of a mocked one."""

    def __init__(self, cert, key):
        self._cert_path, self._key_path = _pem_files(cert, key)
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(self._cert_path, self._key_path)

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(5)
        self._sock.settimeout(0.5)
        self.port = self._sock.getsockname()[1]

        self._stop = False
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while not self._stop:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                with self._context.wrap_socket(conn, server_side=True) as tls_conn:
                    try:
                        tls_conn.recv(1)
                    except Exception:
                        pass
            except Exception:
                pass  # handshake failures are expected in several tests

    def close(self):
        self._stop = True
        self._thread.join(timeout=2)
        self._sock.close()
        os.unlink(self._cert_path)
        os.unlink(self._key_path)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _redirect_to(port):
    """Patch socket.create_connection so ssl_checker connects to the local
    test server at 127.0.0.1:port regardless of the hostname it was asked
    to resolve -- the hostname itself still reaches wrap_socket() for a
    real SNI + certificate-hostname check."""
    real_create_connection = socket.create_connection

    def _patched(address, timeout=None, *args, **kwargs):
        _hostname, _port = address
        return real_create_connection(("127.0.0.1", port), timeout=timeout)

    return mock.patch.object(ssl_checker.socket, "create_connection", side_effect=_patched)


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


# ---------------------------------------------------------------------------
# A. Valid HTTPS certificate / near-expiry warning path
# ---------------------------------------------------------------------------

class ValidCertificateTests(unittest.TestCase):
    def test_valid_trusted_certificate_reports_VALID(self):
        cert, key = _make_cert(
            "test.local", ["test.local"],
            _now() - datetime.timedelta(days=1),
            _now() + datetime.timedelta(days=90),
        )
        cert_path, _ = _pem_files(cert, key)
        with _LocalTLSServer(cert, key) as server:
            with _redirect_to(server.port), mock.patch.object(ssl_checker.certifi, "where", return_value=cert_path):
                result = ssl_checker.check_ssl_certificate("test.local", 443)

        os.unlink(cert_path)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_VALID)
        self.assertTrue(result["ssl_valid"])
        self.assertIsNotNone(result["ssl_expiry_date"])
        self.assertGreater(result["ssl_days_remaining"], 80)
        self.assertIsNotNone(result["ssl_details"])
        self.assertTrue(result["ssl_details"]["authorized"])
        self.assertEqual(result["ssl_details"]["tls_version"], "TLSv1.3")

    def test_valid_certificate_via_san_not_cn(self):
        """Hostname matches a SAN entry that isn't the certificate's CN --
        must still validate (this is how sapphiredigitalagency.com's real
        certificate works: CN=sapphiredigitalagency.com, SAN includes
        www.sapphiredigitalagency.com)."""
        cert, key = _make_cert(
            "test.local", ["test.local", "www.test.local"],
            _now() - datetime.timedelta(days=1),
            _now() + datetime.timedelta(days=90),
        )
        cert_path, _ = _pem_files(cert, key)
        with _LocalTLSServer(cert, key) as server:
            with _redirect_to(server.port), mock.patch.object(ssl_checker.certifi, "where", return_value=cert_path):
                result = ssl_checker.check_ssl_certificate("www.test.local", 443)

        os.unlink(cert_path)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_VALID)


# ---------------------------------------------------------------------------
# B. Expired certificate
# ---------------------------------------------------------------------------

class ExpiredCertificateTests(unittest.TestCase):
    def test_expired_certificate_reports_EXPIRED_CERTIFICATE(self):
        cert, key = _make_cert(
            "test.local", ["test.local"],
            _now() - datetime.timedelta(days=400),
            _now() - datetime.timedelta(days=30),
        )
        cert_path, _ = _pem_files(cert, key)
        with _LocalTLSServer(cert, key) as server:
            with _redirect_to(server.port), mock.patch.object(ssl_checker.certifi, "where", return_value=cert_path):
                result = ssl_checker.check_ssl_certificate("test.local", 443)

        os.unlink(cert_path)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_EXPIRED_CERTIFICATE)
        self.assertFalse(result["ssl_valid"])
        self.assertIn("expired", result["ssl_message"].lower())
        self.assertIsNotNone(result["ssl_expiry_date"])  # diagnostic fetch still reports the real date


# ---------------------------------------------------------------------------
# C. Hostname mismatch
# ---------------------------------------------------------------------------

class HostnameMismatchTests(unittest.TestCase):
    def test_hostname_not_in_san_reports_HOSTNAME_MISMATCH(self):
        cert, key = _make_cert(
            "test.local", ["test.local"],  # no SAN for other-name.local
            _now() - datetime.timedelta(days=1),
            _now() + datetime.timedelta(days=90),
        )
        cert_path, _ = _pem_files(cert, key)
        with _LocalTLSServer(cert, key) as server:
            with _redirect_to(server.port), mock.patch.object(ssl_checker.certifi, "where", return_value=cert_path):
                result = ssl_checker.check_ssl_certificate("other-name.local", 443)

        os.unlink(cert_path)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_HOSTNAME_MISMATCH)
        self.assertFalse(result["ssl_valid"])
        self.assertIn("test.local", result["ssl_message"])


# ---------------------------------------------------------------------------
# Certificate chain error (self-signed / untrusted issuer)
# ---------------------------------------------------------------------------

class ChainErrorTests(unittest.TestCase):
    def test_untrusted_self_signed_certificate_reports_CHAIN_ERROR(self):
        cert, key = _make_cert(
            "test.local", ["test.local"],
            _now() - datetime.timedelta(days=1),
            _now() + datetime.timedelta(days=90),
        )
        with _LocalTLSServer(cert, key) as server:
            # Deliberately do NOT trust this cert (don't patch certifi) --
            # the real certifi bundle has no idea about our ad hoc CA, so
            # the handshake must fail on chain-of-trust grounds.
            with _redirect_to(server.port):
                result = ssl_checker.check_ssl_certificate("test.local", 443)

        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_CHAIN_ERROR)
        self.assertFalse(result["ssl_valid"])


# ---------------------------------------------------------------------------
# G. DNS failure / H. Timeout / NO_HTTPS / TLS_CONNECTION_ERROR / BLOCKED
# ---------------------------------------------------------------------------

class ConnectionFailureClassificationTests(unittest.TestCase):
    def test_dns_failure_reports_DNS_ERROR(self):
        with mock.patch.object(
            ssl_checker.socket, "create_connection",
            side_effect=socket.gaierror("Name or service not known"),
        ):
            result = ssl_checker.check_ssl_certificate("does-not-resolve.invalid", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_DNS_ERROR)
        self.assertFalse(result["ssl_valid"])

    def test_timeout_reports_TIMEOUT(self):
        with mock.patch.object(
            ssl_checker.socket, "create_connection",
            side_effect=socket.timeout("timed out"),
        ):
            result = ssl_checker.check_ssl_certificate("slow.test.local", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_TIMEOUT)
        self.assertFalse(result["ssl_valid"])

    def test_connection_refused_reports_NO_HTTPS(self):
        with mock.patch.object(
            ssl_checker.socket, "create_connection",
            side_effect=ConnectionRefusedError("refused"),
        ):
            result = ssl_checker.check_ssl_certificate("no-https.test.local", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_NO_HTTPS)
        self.assertFalse(result["ssl_valid"])

    def test_generic_tls_handshake_failure_reports_TLS_CONNECTION_ERROR(self):
        fake_sock = mock.MagicMock()
        with mock.patch.object(ssl_checker.socket, "create_connection") as create_conn:
            create_conn.return_value.__enter__.return_value = fake_sock
            with mock.patch.object(
                ssl.SSLContext, "wrap_socket",
                side_effect=ssl.SSLError("SSLV3_ALERT_HANDSHAKE_FAILURE"),
            ):
                result = ssl_checker.check_ssl_certificate("handshake-fails.test.local", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_TLS_CONNECTION_ERROR)
        self.assertFalse(result["ssl_valid"])

    def test_connection_reset_during_handshake_reports_BLOCKED(self):
        fake_sock = mock.MagicMock()
        with mock.patch.object(ssl_checker.socket, "create_connection") as create_conn:
            create_conn.return_value.__enter__.return_value = fake_sock
            with mock.patch.object(
                ssl.SSLContext, "wrap_socket",
                side_effect=ConnectionResetError("Connection reset by peer"),
            ):
                result = ssl_checker.check_ssl_certificate("blocked.test.local", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_BLOCKED)
        self.assertFalse(result["ssl_valid"])


# ---------------------------------------------------------------------------
# Root-cause regression: missing `cryptography` dependency must never
# silently report a definitive pass/fail.
# ---------------------------------------------------------------------------

class MissingDependencyRegressionTests(unittest.TestCase):
    def test_missing_cryptography_reports_UNKNOWN_not_a_false_critical(self):
        with mock.patch.object(ssl_checker, "CRYPTOGRAPHY_AVAILABLE", False):
            result = ssl_checker.check_ssl_certificate("sapphiredigitalagency.com", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_UNKNOWN)
        self.assertFalse(result["ssl_valid"])
        self.assertIn("missing server dependency", result["ssl_message"])

    def test_empty_hostname_reports_UNKNOWN(self):
        result = ssl_checker.check_ssl_certificate("", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_UNKNOWN)
        self.assertFalse(result["ssl_valid"])


# ---------------------------------------------------------------------------
# Live smoke test against the real reported domain. Skipped by default
# (network-dependent); run explicitly with RUN_LIVE_SSL_TESTS=1 to confirm
# the fix against production, matching how this bug was first diagnosed.
# ---------------------------------------------------------------------------

@unittest.skipUnless(
    os.environ.get("RUN_LIVE_SSL_TESTS") == "1",
    "set RUN_LIVE_SSL_TESTS=1 to run the live network check against the real domain",
)
class LiveSapphireDigitalAgencyTests(unittest.TestCase):
    def test_www_subdomain_is_valid(self):
        result = ssl_checker.check_ssl_certificate("www.sapphiredigitalagency.com", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_VALID)
        self.assertTrue(result["ssl_valid"])

    def test_apex_domain_is_valid(self):
        result = ssl_checker.check_ssl_certificate("sapphiredigitalagency.com", 443)
        self.assertEqual(result["ssl_status"], ssl_checker.STATUS_VALID)
        self.assertTrue(result["ssl_valid"])


if __name__ == "__main__":
    unittest.main()
