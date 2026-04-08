"""SSL Certificate checker for domain-level technical data collection."""

import ssl
import socket
import datetime
from urllib.parse import urlparse

# Import cryptography library for robust certificate parsing
try:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False


def check_ssl_certificate(hostname: str) -> dict:
    """
    Check SSL certificate for the given hostname.
    
    Args:
        hostname: Hostname only (e.g., "example.com")
        
    Returns:
        dict with keys: ssl_valid, ssl_expiry_date, ssl_days_remaining
    """
    result = {
        "ssl_valid": False,
        "ssl_expiry_date": None,
        "ssl_days_remaining": None
    }
    
    if not hostname:
        print(f"[SSL ERROR] Invalid hostname | hostname={hostname}")
        return result
    
    if not CRYPTOGRAPHY_AVAILABLE:
        print(f"[SSL ERROR] Cryptography library not available | hostname={hostname}")
        return result
    
    try:
        # Create SSL context
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        # Open TLS socket connection to hostname:443
        with socket.create_connection((hostname, 443), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                # If we can establish TLS connection, SSL is valid
                result["ssl_valid"] = True
                
                # Extract certificate expiry date using cryptography library
                try:
                    # Get DER certificate and parse with cryptography
                    der_cert = ssock.getpeercert(binary_form=True)
                    cert = x509.load_der_x509_certificate(der_cert, default_backend())
                    
                    # Extract expiry date (notAfter field)
                    not_after = cert.not_valid_after_utc if hasattr(cert, 'not_valid_after_utc') else cert.not_valid_after
                    
                    # Convert to ISO 8601 format with UTC timezone
                    iso_expiry_date = not_after.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    # Calculate remaining days (timezone-safe)
                    now = datetime.datetime.now(datetime.timezone.utc)
                    if hasattr(not_after, 'tzinfo') and not_after.tzinfo:
                        # Certificate has timezone info
                        days_remaining = (not_after - now).days
                    else:
                        # Certificate is naive, assume UTC
                        not_after_utc = not_after.replace(tzinfo=datetime.timezone.utc)
                        days_remaining = (not_after_utc - now).days
                    
                    # Update result with real certificate data
                    result.update({
                        "ssl_expiry_date": iso_expiry_date,
                        "ssl_days_remaining": days_remaining
                    })
                    
                    print(f"✅ SSL certificate parsed successfully | hostname={hostname} | expiry={iso_expiry_date} | days_remaining={days_remaining}")
                    
                except Exception as cert_parse_error:
                    print(f"[SSL ERROR] Certificate parsing failed | hostname={hostname} | error_type={type(cert_parse_error).__name__} | error={str(cert_parse_error)}")
                    # Keep ssl_valid=True since connection worked, but no expiry data
                
    except socket.gaierror as e:
        print(f"[SSL ERROR] DNS resolution failed | hostname={hostname} | error={str(e)}")
    except socket.timeout:
        print(f"[SSL ERROR] Connection timeout | hostname={hostname}")
    except socket.error as e:
        print(f"[SSL ERROR] Connection error | hostname={hostname} | error={str(e)}")
    except ssl.SSLError as e:
        print(f"[SSL ERROR] SSL handshake failed | hostname={hostname} | error={str(e)}")
    except Exception as e:
        print(f"[SSL ERROR] Unexpected error | hostname={hostname} | error_type={type(e).__name__} | error={str(e)}")
    
    return result
