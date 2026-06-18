"""
URL_QUALIFICATION worker — Probes discovered URLs and emits a deterministic
canonical URL list consumed by all downstream parallel workers.

Responsibilities:
  1. Read all discovered URLs from seo_internal_links for the project
  2. Sort by type priority: main(0) > service(1) > blog(2) > other(99)
  3. Health-check each URL with a concurrent HEAD request (8 s timeout)
  4. Classify results:
       qualified     — HTTP 200, 301, 302
       low_priority  — HTTP 4xx, 5xx  (usable but deprioritised)
       rejected      — DNS failure, SSL failure, connection refused, timeout
  5. Emit canonicalUrls: qualified first (priority order), then low_priority,
     capped at CANONICAL_CAP (50). Rejected URLs are excluded.
  6. Write per-URL probe records to seo_audit_url_pool for audit diagnostics.

Pool sizing rationale:
  - Crawler typically discovers 70-80 URLs per site.
  - CANDIDATE_POOL_SIZE=75 probes the full realistic discovery set.
  - With ~15% expected failure rate, 50 qualified URLs guarantees 25+ successes.
"""

import os
import asyncio
import httpx
import requests
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel
from bson.objectid import ObjectId

from scraper.shared.url_selector import get_candidate_pool
from db import seo_audit_url_pool


# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
PROBE_CONCURRENCY = 20       # simultaneous HEAD requests per job
PROBE_TIMEOUT_S = 8          # per-URL hard timeout
CANDIDATE_POOL_SIZE = 75     # max URLs read from seo_internal_links
CANONICAL_CAP = 50           # max entries in canonicalUrls output
QUALIFIED_STATUSES = {200, 301, 302}


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------
class UrlQualificationJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to TECHNICAL_DOMAIN job


# ---------------------------------------------------------------------------
# HTTP probing
# ---------------------------------------------------------------------------
async def _probe_url(url: str, client: httpx.AsyncClient) -> dict:
    """HEAD-probe a single URL; return a classification dict."""
    try:
        resp = await client.head(url, follow_redirects=True, timeout=PROBE_TIMEOUT_S)
        status = resp.status_code
        qualified = status in QUALIFIED_STATUSES
        return {
            "url": url,
            "status_code": status,
            "qualified": qualified,
            "reject_reason": None,
            "low_priority": not qualified,
        }
    except httpx.TimeoutException:
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": "timeout", "low_priority": False,
        }
    except httpx.ConnectError as exc:
        s = str(exc).lower()
        reason = "dns_failure" if ("getaddrinfo" in s or "name or service" in s) else "connection_refused"
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": reason, "low_priority": False,
        }
    except Exception as exc:
        s = str(exc).lower()
        reason = "ssl_failure" if ("ssl" in s or "certificate" in s) else f"error:{type(exc).__name__}"
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": reason, "low_priority": False,
        }


async def _run_probes(
    urls: List[str], job_id: str, project_id: str
) -> List[dict]:
    """Probe all URLs under a semaphore; persist results to seo_audit_url_pool."""
    sem = asyncio.Semaphore(PROBE_CONCURRENCY)

    async def bounded(url: str, client: httpx.AsyncClient) -> dict:
        async with sem:
            return await _probe_url(url, client)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; OditoQualifier/1.0)"},
        timeout=httpx.Timeout(PROBE_TIMEOUT_S + 2.0),
    ) as client:
        results = await asyncio.gather(
            *[bounded(u, client) for u in urls]
        )

    # Persist audit trail (non-fatal)
    now = datetime.now(timezone.utc)
    job_oid = ObjectId(job_id)
    proj_oid = ObjectId(project_id)
    pool_docs = [
        {
            "job_id": job_oid,
            "project_id": proj_oid,
            "url": r["url"],
            "qualified": r["qualified"],
            "status_code": r["status_code"],
            "reject_reason": r.get("reject_reason"),
            "low_priority": r.get("low_priority", False),
            "probed_at": now,
        }
        for r in results
    ]
    if pool_docs:
        try:
            seo_audit_url_pool.insert_many(pool_docs, ordered=False)
        except Exception as exc:
            print(f"[URL_QUAL] seo_audit_url_pool write failed (non-fatal): {exc}")

    return list(results)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
async def execute_url_qualification(job: UrlQualificationJob) -> dict:
    """Execute URL qualification: probe → classify → emit canonicalUrls."""
    job_id = job.jobId
    project_id = job.projectId
    start = datetime.now(timezone.utc)

    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        raise Exception("NODE_BACKEND_URL is required")

    print(f"[URL_QUAL] Starting | jobId={job_id} | projectId={project_id}")

    # Step 1: Build candidate pool (deterministically sorted by type priority)
    candidates = get_candidate_pool(project_id, max_size=CANDIDATE_POOL_SIZE)
    discovered_count = len(candidates)
    print(f"[URL_QUAL] Candidate pool: {discovered_count} URLs")

    if not candidates:
        result_data = {
            "canonicalUrls": [],
            "discoveredUrls": 0,
            "candidateUrls": 0,
            "qualifiedUrls": 0,
            "lowPriorityUrls": 0,
            "rejectedUrls": 0,
            "canonicalCount": 0,
            "duration_ms": 0,
        }
        _send_completion(job_id, node_backend_url, result_data)
        return {"status": "completed", "jobId": job_id, "canonicalUrls": []}

    candidate_urls = [c["url"] for c in candidates]

    # Step 2: Probe all candidates concurrently
    print(
        f"[URL_QUAL] Probing {len(candidate_urls)} URLs "
        f"| concurrency={PROBE_CONCURRENCY} | timeout={PROBE_TIMEOUT_S}s"
    )
    probe_results = await _run_probes(candidate_urls, job_id, project_id)
    probe_map = {r["url"]: r for r in probe_results}

    # Step 3: Assemble canonicalUrls in deterministic order
    # candidates list already encodes type-priority order; preserve it.
    qualified: List[str] = []
    low_priority: List[str] = []
    rejected = 0

    for candidate in candidates:
        url = candidate["url"]
        probe = probe_map.get(url, {})
        if probe.get("qualified"):
            qualified.append(url)
        elif probe.get("low_priority"):
            low_priority.append(url)
        else:
            rejected += 1

    canonical_urls = (qualified + low_priority)[:CANONICAL_CAP]

    duration_ms = int(
        (datetime.now(timezone.utc) - start).total_seconds() * 1000
    )

    result_data = {
        "canonicalUrls": canonical_urls,
        "discoveredUrls": discovered_count,
        "candidateUrls": len(candidate_urls),
        "qualifiedUrls": len(qualified),
        "lowPriorityUrls": len(low_priority),
        "rejectedUrls": rejected,
        "canonicalCount": len(canonical_urls),
        "duration_ms": duration_ms,
    }

    print(
        f"[URL_QUAL] Complete | qualified={len(qualified)} "
        f"| lowPriority={len(low_priority)} | rejected={rejected} "
        f"| canonical={len(canonical_urls)} | duration={duration_ms}ms"
    )

    _send_completion(job_id, node_backend_url, result_data)

    return {
        "status": "completed",
        "jobId": job_id,
        "canonicalUrls": canonical_urls,
        "result_data": result_data,
    }


def _send_completion(job_id: str, node_backend_url: str, result_data: dict):
    """POST completion callback to Node.js (synchronous, fire-and-forget on error)."""
    try:
        url = f"{node_backend_url}/api/jobs/{job_id}/complete"
        resp = requests.post(
            url,
            json={"stats": result_data, "result_data": result_data},
            timeout=30,
        )
        resp.raise_for_status()
        print(f"[URL_QUAL] Completion callback sent | jobId={job_id}")
    except Exception as exc:
        print(f"[URL_QUAL] Completion callback failed (non-fatal) | jobId={job_id} | error={exc}")
