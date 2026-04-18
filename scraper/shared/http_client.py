"""Shared HTTP client for connection pooling and reuse."""

import asyncio
import logging
import httpx
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Global client variable and event loop tracking
_http_client: Optional[httpx.AsyncClient] = None
_event_loop_id: Optional[int] = None


def get_http_client() -> httpx.AsyncClient:
    """
    Get or create an HTTP client for the current event loop.
    This prevents event loop binding issues by recreating the client if the event loop has changed.
    """
    global _http_client, _event_loop_id

    try:
        current_loop = asyncio.get_running_loop()
        current_loop_id = id(current_loop)
    except RuntimeError:
        # No running loop, create new client
        current_loop_id = None

    # Recreate client if event loop has changed or client doesn't exist
    if _http_client is None or _event_loop_id != current_loop_id:
        # Close old client if it exists and loop is different
        if _http_client is not None and _event_loop_id != current_loop_id:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Can't close in running loop, just replace
                    _http_client = None
                else:
                    loop.run_until_complete(_http_client.aclose())
            except:
                _http_client = None

        _http_client = httpx.AsyncClient(
            timeout=30,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20
            )
        )
        _event_loop_id = current_loop_id

    return _http_client


async def close_http_client():
    """Close the shared HTTP client. Call this on application shutdown."""
    global _http_client, _event_loop_id
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
        _event_loop_id = None


async def async_get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
    """
    Async GET request with retry logic.

    Args:
        url: Request URL
        params: Query parameters
        **kwargs: Additional arguments passed to httpx.get

    Returns:
        httpx.Response object

    Raises:
        httpx.HTTPError: If request fails after retries
    """
    max_retries = 3
    base_delay = 1.0
    client = get_http_client()

    for attempt in range(max_retries):
        try:
            response = await client.get(url, params=params, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPError as e:
            if attempt == max_retries - 1:
                logger.error(f"GET {url} failed after {max_retries} attempts: {e}")
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"GET {url} failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
            await asyncio.sleep(delay)


async def async_post(url: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
    """
    Async POST request with retry logic.

    Args:
        url: Request URL
        json: JSON payload
        **kwargs: Additional arguments passed to httpx.post

    Returns:
        httpx.Response object

    Raises:
        httpx.HTTPError: If request fails after retries
    """
    max_retries = 3
    base_delay = 1.0
    client = get_http_client()

    for attempt in range(max_retries):
        try:
            response = await client.post(url, json=json, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPError as e:
            if attempt == max_retries - 1:
                logger.error(f"POST {url} failed after {max_retries} attempts: {e}")
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"POST {url} failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
            await asyncio.sleep(delay)
