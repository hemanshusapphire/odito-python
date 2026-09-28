"""SSL Certificate checker for domain-level technical data collection.

Performs a real, verifying TLS handshake against the target hostname (SNI,
hostname/SAN matching, chain-of-trust validation against the certifi CA
bundle) and differentiates every failure mode instead of collapsing
everything into a single "invalid" bucket.

Root-cause note (2026-09): a previous version of this module made
certificate validation depend on the `cryptography` package but never
declared it in requirements.txt. When that package was missing at runtime,
`check_ssl_certificate()` silently returned `ssl_valid=False` for every
hostname on every audit -- including sites with a perfectly valid,
browser-trusted certificate -- which is what produced the "SSL certificate
not found or invalid" CRITICAL false positive. See requirements.txt.
"""

import datetime
import socket
import ssl

import certifi

try:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.x509.oid import NameOID
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

# Status vocabulary. technicalChecks.service.js (Node) maps these onto the
# OK / Warning / Critical severities shown in the UI -- keep values in sync
# with that mapping if you add new statuses here.
STATUS_VALID = "VALID"
STATUS_INVALID_CERTIFICATE = "INVALID_CERTIFICATE"
STATUS_EXPIRED_CERTIFICATE = "EXPIRED_CERTIFICATE"
STATUS_HOSTNAME_MISMATCH = "HOSTNAME_MISMATCH"
STATUS_CHAIN_ERROR = "CERTIFICATE_CHAIN_ERROR"
STATUS_NO_HTTPS = "NO_HTTPS"
STATUS_TLS_CONNECTION_ERROR = "TLS_CONNECTION_ERROR"
STATUS_DNS_ERROR = "DNS_ERROR"
STATUS_TIMEOUT = "TIMEOUT"
STATUS_BLOCKED = "BLOCKED"
STATUS_UNKNOWN = "UNKNOWN"

CONNECT_TIMEOUT_SECONDS = 10


def _empty_result(status: str, message: str) -> dict:
    return {
        "ssl_status": status,
        "ssl_valid": status == STATUS_VALID,
        "ssl_expiry_date": None,
        "ssl_days_remaining": None,
        "ssl_message": message,
        "ssl_details": None,
    }


def _name_to_str(name) -> str:
    try:
        attrs = name.get_attributes_for_oid(NameOID.COMMON_NAME)
        if attrs:
            return attrs[0].value
    except Exception:
        pass
    try:
        return name.rfc4514_string()
    except Exception:
        return str(name)


def _der_to_details(der_cert: bytes, tls_version: str) -> dict:
    """Parse a DER certificate into human-readable details via `cryptography`."""
    cert = x509.load_der_x509_certificate(der_cert, default_backend())

    not_after = cert.not_valid_after_utc if hasattr(cert, "not_valid_after_utc") else cert.not_valid_after
    not_before = cert.not_valid_before_utc if hasattr(cert, "not_valid_before_utc") else cert.not_valid_before
    if not_after.tzinfo is None:
        not_after = not_after.replace(tzinfo=datetime.timezone.utc)
    if not_before.tzinfo is None:
        not_before = not_before.replace(tzinfo=datetime.timezone.utc)

    try:
        san_ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        san_names = san_ext.value.get_values_for_type(x509.DNSName)
    except x509.ExtensionNotFound:
        san_names = []

    now = datetime.datetime.now(datetime.timezone.utc)

    return {
        "not_before": not_before,
        "not_after": not_after,
        "days_remaining": (not_after - now).days,
        "subject": _name_to_str(cert.subject),
        "issuer": _name_to_str(cert.issuer),
        "san": san_names,
        "fingerprint_sha1": cert.fingerprint(hashes.SHA1()).hex(),
        "fingerprint_sha256": cert.fingerprint(hashes.SHA256()).hex(),
        "tls_version": tls_version,
    }


def _fetch_raw_certificate(hostname: str, port: int, timeout: int):
    """Open a TLS connection WITHOUT verification, purely to retrieve the
    certificate the server actually presented, for diagnostics when the
    verifying handshake fails (e.g. to report *why* -- expired vs hostname
    mismatch vs untrusted issuer -- with real dates/names)."""
    context = ssl._create_unverified_context()
    with socket.create_connection((hostname, port), timeout=timeout) as sock:
        with context.wrap_socket(sock, server_hostname=hostname) as ssock:
            der_cert = ssock.getpeercert(binary_form=True)
            tls_version = ssock.version()
    return der_cert, tls_version


def check_ssl_certificate(hostname: str, port: int = 443) -> dict:
    """
    Validate the TLS certificate actually presented by `hostname:port`.

    Performs a certificate-verifying TLS handshake with SNI set to
    `hostname` (chain trust via the certifi CA bundle, hostname/SAN
    matching, expiry enforced by the handshake itself), then classifies
    every failure mode so callers never have to guess why a check failed.

    Args:
        hostname: Hostname only (e.g., "example.com"), no scheme/path.
        port: TCP port to connect to (default 443).

    Returns:
        dict:
            ssl_status:          one of the STATUS_* constants above
            ssl_valid:           True only when ssl_status == VALID
            ssl_expiry_date:     ISO-8601 UTC string, or None
            ssl_days_remaining:  int, or None
            ssl_message:         human-readable explanation
            ssl_details:         dict (subject/issuer/san/fingerprints/tls_version/
                                  authorized/authorization_error), or None
    """
    if not hostname:
        print(f"[SSL ERROR] Invalid hostname | hostname={hostname}")
        return _empty_result(STATUS_UNKNOWN, "No hostname provided for SSL check")

    if not CRYPTOGRAPHY_AVAILABLE:
        # Hard dependency. Without it we cannot inspect the certificate, so
        # reporting VALID or INVALID would both be guesses -- surface this as
        # an audit-infrastructure problem instead of defaulting to "invalid"
        # (that default is exactly what caused the false-positive CRITICAL
        # this module was rewritten to fix).
        print(f"[SSL ERROR] cryptography library not available | hostname={hostname}")
        return _empty_result(STATUS_UNKNOWN, "SSL verification unavailable (missing server dependency)")

    verified_context = ssl.create_default_context(cafile=certifi.where())

    try:
        with socket.create_connection((hostname, port), timeout=CONNECT_TIMEOUT_SECONDS) as sock:
            with verified_context.wrap_socket(sock, server_hostname=hostname) as ssock:
                der_cert = ssock.getpeercert(binary_form=True)
                tls_version = ssock.version()
                details = _der_to_details(der_cert, tls_version)

                message = f"SSL certificate is valid (expires {details['not_after'].strftime('%Y-%m-%d')})"
                print(
                    f"[SSL OK] hostname={hostname} | issuer={details['issuer']} | "
                    f"expiry={details['not_after']} | days_remaining={details['days_remaining']} | "
                    f"tls={tls_version}"
                )

                return {
                    "ssl_status": STATUS_VALID,
                    "ssl_valid": True,
                    "ssl_expiry_date": details["not_after"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "ssl_days_remaining": details["days_remaining"],
                    "ssl_message": message,
                    "ssl_details": {
                        "subject": details["subject"],
                        "issuer": details["issuer"],
                        "san": details["san"],
                        "fingerprint_sha1": details["fingerprint_sha1"],
                        "fingerprint_sha256": details["fingerprint_sha256"],
                        "tls_version": details["tls_version"],
                        "authorized": True,
                        "authorization_error": None,
                    },
                }

    except ssl.SSLCertVerificationError as e:
        # Must be caught before the generic ssl.SSLError branch below --
        # SSLCertVerificationError is a subclass of SSLError.
        reason = (getattr(e, "verify_message", "") or str(e)).lower()
        print(f"[SSL ERROR] Certificate verification failed | hostname={hostname} | reason={reason}")

        # Fetch the raw (unverified) cert so we can report *why*, with real
        # dates/names, rather than just OpenSSL's error string.
        diag_details = None
        try:
            der_cert, tls_version = _fetch_raw_certificate(hostname, port, CONNECT_TIMEOUT_SECONDS)
            diag_details = _der_to_details(der_cert, tls_version)
        except Exception as diag_error:
            print(f"[SSL ERROR] Diagnostic fetch failed | hostname={hostname} | error={str(diag_error)}")

        if any(k in reason for k in ("hostname mismatch", "doesn't match", "certificate is not valid for")):
            status = STATUS_HOSTNAME_MISMATCH
            san_list = ", ".join(diag_details["san"]) if diag_details and diag_details["san"] else "unknown"
            message = f"Certificate hostname does not match {hostname} (certificate is valid for: {san_list})"
        elif "expired" in reason:
            status = STATUS_EXPIRED_CERTIFICATE
            expiry_str = diag_details["not_after"].strftime("%Y-%m-%d") if diag_details else "an earlier date"
            message = f"Certificate expired on {expiry_str}"
        elif any(k in reason for k in (
            "unable to get local issuer certificate",
            "unable to get issuer certificate",
            "self signed certificate",
            "self-signed certificate",
            "certificate chain",
            "unable to verify the first certificate",
        )):
            status = STATUS_CHAIN_ERROR
            issuer_str = diag_details["issuer"] if diag_details else "an untrusted issuer"
            message = f"Certificate chain could not be verified (issuer: {issuer_str})"
        else:
            status = STATUS_INVALID_CERTIFICATE
            message = f"Certificate verification failed: {reason or str(e)}"

        result = _empty_result(status, message)
        if diag_details:
            result["ssl_expiry_date"] = diag_details["not_after"].strftime("%Y-%m-%dT%H:%M:%SZ")
            result["ssl_days_remaining"] = diag_details["days_remaining"]
            result["ssl_details"] = {
                "subject": diag_details["subject"],
                "issuer": diag_details["issuer"],
                "san": diag_details["san"],
                "fingerprint_sha1": diag_details["fingerprint_sha1"],
                "fingerprint_sha256": diag_details["fingerprint_sha256"],
                "tls_version": diag_details["tls_version"],
                "authorized": False,
                "authorization_error": reason,
            }
        return result

    except socket.gaierror as e:
        print(f"[SSL ERROR] DNS resolution failed | hostname={hostname} | error={str(e)}")
        return _empty_result(STATUS_DNS_ERROR, f"Could not resolve hostname {hostname} (DNS lookup failed)")

    except (socket.timeout, TimeoutError):
        print(f"[SSL ERROR] Connection timeout | hostname={hostname}")
        return _empty_result(STATUS_TIMEOUT, f"Connection to {hostname}:{port} timed out")

    except ConnectionRefusedError:
        print(f"[SSL ERROR] Connection refused | hostname={hostname}")
        return _empty_result(STATUS_NO_HTTPS, f"No service listening on {hostname}:{port} (HTTPS unavailable)")

    except (ssl.SSLError, OSError) as e:
        msg = str(e).lower()
        if any(k in msg for k in ("forcibly closed", "connection reset", "eof occurred", "connection aborted")):
            print(f"[SSL ERROR] Connection blocked/reset during handshake | hostname={hostname} | error={str(e)}")
            return _empty_result(
                STATUS_BLOCKED,
                f"Connection to {hostname} was reset during the TLS handshake (possibly blocked)"
            )
        print(f"[SSL ERROR] TLS connection error | hostname={hostname} | error={str(e)}")
        return _empty_result(STATUS_TLS_CONNECTION_ERROR, f"TLS connection error: {str(e)}")

    except Exception as e:
        print(f"[SSL ERROR] Unexpected error | hostname={hostname} | error_type={type(e).__name__} | error={str(e)}")
        return _empty_result(STATUS_UNKNOWN, f"Unexpected error checking SSL certificate: {str(e)}")
