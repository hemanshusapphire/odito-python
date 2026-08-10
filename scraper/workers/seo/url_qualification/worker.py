"""
URL_QUALIFICATION worker — Probes discovered URLs and emits a deterministic
canonical URL list consumed by all downstream parallel workers.

Responsibilities:
  1. Read all discovered URLs from seo_internal_links for the project
  2. Sort by type priority: main(0) > service(1) > blog(2) > other(99)
  3. Health-check each URL with a concurrent HEAD request (8 s timeout). If
     the HEAD times out, retry once with GET before giving up — some
     origins (WordPress/shared-hosting under concurrent load) stall on
     HEAD specifically while GET succeeds normally (forensic audit,
     2026-07-06). Only a HEAD timeout followed by a GET timeout/error is
     still classified as a genuine failure.
  4. Classify results:
       qualified     — HTTP 200, 301, 302 (from HEAD, or from the GET retry)
       low_priority  — HTTP 4xx, 5xx  (usable but deprioritised)
       rejected      — DNS failure, SSL failure, connection refused, timeout
                       (HEAD+GET both failed)
  5. Starvation recovery: if timeouts (not real 4xx/5xx/DNS/SSL failures)
     wipe out most of the usable pool, re-probe just the timed-out URLs once
     more at reduced concurrency and an extended timeout before finalizing.
     See STARVATION_* constants below for the exact trigger and rationale.
     Complementary to the per-URL GET retry above: this operates at the
     batch level for URLs where even the immediate GET retry didn't recover.
  6. Emit canonicalUrls: qualified first (priority order), then low_priority,
     capped at CANONICAL_CAP (50). Rejected URLs are excluded.
  7. Write per-URL probe records to seo_audit_url_pool for audit diagnostics
     (each record tagged with probe_phase: "initial" or "retry", plus
     response_time_ms, final_url, redirect_count, probe_method_used, and
     error_message for full forensic traceability).

Pool sizing rationale:
  - CANDIDATE_POOL_SIZE bounds the candidate pool read from seo_internal_links
    and probed here. It must cover every realistic single-site discovery count
    (sites with 1000+, even 10,000+, discovered URLs are expected) so the URL
    Selection screen can show the user everything LINK_DISCOVERY found, not a
    truncated subset — the URL Selection flow itself has no cap of its own
    (see odito_backend urlSelectionController.js). This constant is
    intentionally NOT unbounded — it still protects a single audit run from a
    truly pathological outlier (e.g. a 50k+-URL sitemap) turning into an
    unbounded probing job — but its default is set high enough that no
    realistic site's discovery count truncates here. Configurable via the
    CANDIDATE_POOL_SIZE env var if a deployment needs a different ceiling.
  - CANONICAL_CAP=50 is unrelated to the above: it only bounds the legacy
    auto-chunk canonicalUrls fan-out used when a project has
    require_url_selection=false (URL Selection workflow skipped). It has no
    effect on what the URL Selection screen shows or what the user can
    approve — that path uses the user's own selected_urls list (no
    platform-wide cap there either), not this field.
"""

import os
import time
import asyncio
import httpx
import requests
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel
from bson.objectid import ObjectId

from scraper.shared.url_selector import get_candidate_pool
from scraper.shared.utils import normalize_url
from db import seo_audit_url_pool


# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
PROBE_CONCURRENCY = 10       # simultaneous HEAD requests per job — lowered
                             # from 20 (forensic audit, 2026-07-06): two
                             # independent real-site incidents
                             # (sapphiredigitalconnect.com, asgeyehospital.com)
                             # both showed false "timeout" classifications
                             # caused by 20 simultaneous HEAD requests
                             # saturating a shared-hosting/WordPress PHP-FPM
                             # worker pool, on sites that respond in well
                             # under 1s to an isolated request. Every URL
                             # probed in one job is the same origin, so this
                             # concurrency figure IS the per-host concurrency.
                             # 10 meaningfully reduces saturation risk while
                             # only ~doubling worst-case batch time (see the
                             # GET-retry-on-timeout fallback below for the
                             # complementary per-URL fix).
PROBE_TIMEOUT_S = 8          # per-URL hard timeout (initial pass)
CANDIDATE_POOL_SIZE = int(os.environ.get("CANDIDATE_POOL_SIZE", "20000"))
                             # max URLs read from seo_internal_links — high
                             # enough to cover real-world discovery counts
                             # (sites with 5,000-10,000+ URLs exist) so the
                             # URL Selection screen can show everything
                             # discovered with no truncation; still a bound,
                             # not unlimited, to protect against a truly
                             # pathological outlier site (e.g. a 50k+-URL
                             # sitemap) turning into an unbounded probing job.
                             # Env-configurable rather than a second hardcoded
                             # constant to bump later.
CANONICAL_CAP = 50           # max entries in canonicalUrls output — only
                             # affects the legacy auto-chunk path (see
                             # docstring above), not the URL Selection screen
QUALIFIED_STATUSES = {200, 301, 302}

# ---------------------------------------------------------------------------
# Starvation recovery tuning
#
# Evidence (forensic audit, 2026-07-06): a confirmed-up, confirmed-healthy
# site (sapphiredigitalconnect.com) lost 22 of 25 candidate URLs to
# reject_reason="timeout" under PROBE_CONCURRENCY=20 concurrent HEAD probes
# at PROBE_TIMEOUT_S=8s, while single isolated requests to the same URLs
# completed in 3.8-9.8s. This is a slow-origin-server pattern, not a dead
# site — real 404/DNS/SSL/connection failures are unaffected by concurrency
# and are excluded from this recovery path entirely; only genuine timeouts
# are retried.
#
# STARVATION_MIN_TIMEOUTS / STARVATION_POOL_FLOOR jointly gate the retry so
# it only fires for a real starvation pattern (many timeouts, most of the
# pool lost), not for a handful of flaky URLs on an otherwise healthy site.
# RETRY_PROBE_CONCURRENCY/RETRY_PROBE_TIMEOUT_S relieve pressure on the slow
# origin (fewer simultaneous requests) and allow for its observed latency
# (~2x the initial timeout, comfortably above the 9.8s worst case seen).
# MAX_RETRY_URLS bounds worst-case added duration so one slow site can never
# make this job run unboundedly long.
# ---------------------------------------------------------------------------
STARVATION_MIN_TIMEOUTS = 5      # ignore noise from a handful of flaky URLs
STARVATION_POOL_FLOOR = 0.5      # retry only if usable pool < 50% of candidates
RETRY_PROBE_CONCURRENCY = 5      # reduced concurrency: relieve pressure on the slow origin
RETRY_PROBE_TIMEOUT_S = 15       # ~2x initial timeout; above observed worst-case (9.8s)
MAX_RETRY_URLS = 25              # bound worst-case added duration


# ---------------------------------------------------------------------------
# Explainability layer (observability only — does not affect qualified/
# low_priority classification, which is decided entirely by QUALIFIED_STATUSES
# and the try/except control flow in _probe_url below, unchanged).
#
# Every non-qualifying probe outcome is mapped onto exactly one of these
# reason codes, so reject_reason is never left null. STALE_SITEMAP_URL is a
# presentation-layer refinement of HTTP_404 (see _build_qualification_details)
# for the specific, common, real case of a sitemap advertising a page that no
# longer exists — the rejection is still driven by the 404, never by the URL
# having come from a sitemap.
# ---------------------------------------------------------------------------
REASON_TIMEOUT = "TIMEOUT"
REASON_DNS_FAILURE = "DNS_FAILURE"
REASON_SSL_ERROR = "SSL_ERROR"
REASON_CONNECTION_REFUSED = "CONNECTION_REFUSED"
REASON_TOO_MANY_REDIRECTS = "TOO_MANY_REDIRECTS"
REASON_INVALID_URL = "INVALID_URL"
REASON_HEAD_FAILED_GET_FAILED = "HEAD_FAILED_GET_FAILED"
REASON_STALE_SITEMAP_URL = "STALE_SITEMAP_URL"
REASON_UNKNOWN = "UNKNOWN"


def _reason_code_for_status(status_code: int) -> str:
    """Generic HTTP_<code> reason code for any non-qualifying status (404,
    410, 500, 503, 403, ...) — covers the full range without hardcoding every
    individual status.
    """
    return f"HTTP_{status_code}"


def _explanation_for(reason_code: str, status_code: Optional[int] = None) -> str:
    """Human-readable explanation for a reason code. Pure presentation text —
    never used in any qualification decision.
    """
    if reason_code == REASON_STALE_SITEMAP_URL:
        return ("This URL was discovered inside the website sitemap but "
                "currently returns HTTP 404.")
    if reason_code.startswith("HTTP_"):
        return f"The origin server returned HTTP {status_code}."
    if reason_code == REASON_TIMEOUT:
        return ("The origin server did not respond within the probe timeout "
                "window (HEAD and the GET retry both timed out).")
    if reason_code == REASON_DNS_FAILURE:
        return "The URL's hostname could not be resolved (DNS lookup failed)."
    if reason_code == REASON_SSL_ERROR:
        return "The TLS/SSL handshake failed while connecting to the origin server."
    if reason_code == REASON_CONNECTION_REFUSED:
        return "The origin server refused the connection."
    if reason_code == REASON_TOO_MANY_REDIRECTS:
        return "The URL redirected more times than the probe's redirect limit allows."
    if reason_code == REASON_INVALID_URL:
        return "The URL could not be parsed or is not a valid HTTP(S) URL."
    if reason_code == REASON_HEAD_FAILED_GET_FAILED:
        return "Both the HEAD request and the GET retry failed."
    return "The URL could not be qualified for an unrecognized reason."


def _build_qualification_details(probe: dict, source_info: dict) -> dict:
    """Assemble the structured, explainable qualification_details object for
    one URL. Pure aggregation of data _probe_url()/_run_probes() already
    computed plus discovery provenance already stored in seo_internal_links —
    does not itself evaluate or alter the qualified/low_priority decision.
    """
    qualified = bool(probe.get("qualified"))
    low_priority = bool(probe.get("low_priority"))
    decision = "qualified" if qualified else ("low_priority" if low_priority else "rejected")

    status_code = probe.get("status_code")
    reason_code = probe.get("reason_code") or (None if qualified else REASON_UNKNOWN)

    # Task 4: stale-sitemap refinement — only changes the EXPLANATION, never
    # the decision. The rejection is still, and only ever, because of the 404.
    source_sitemap = source_info.get("sourceSitemap")
    if not qualified and status_code == 404 and source_sitemap:
        reason_code = REASON_STALE_SITEMAP_URL

    explanation = _explanation_for(reason_code, status_code) if reason_code else None

    return {
        "decision": decision,
        "reason_code": reason_code,
        "explanation": explanation,
        "source": {
            "discovered_from": "sitemap" if source_sitemap else ("html_link" if source_info.get("sourceUrl") else None),
            "sitemap": source_sitemap,
            "source_page": source_info.get("sourceUrl"),
        },
        "probe": {
            "method": probe.get("probe_method_used"),
            "final_status": status_code,
            "redirected": bool(probe.get("redirect_count")),
            "final_url": probe.get("final_url"),
            "response_time_ms": probe.get("response_time_ms"),
        },
        "evaluated_at": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------
class UrlQualificationJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    sourceJobId: str  # Reference to LINK_DISCOVERY job
    canonicalHost: Optional[str] = None  # Host resolved by LINK_DISCOVERY's redirect handling


# ---------------------------------------------------------------------------
# HTTP probing
# ---------------------------------------------------------------------------
async def _probe_url(url: str, client: httpx.AsyncClient, timeout_s: float = PROBE_TIMEOUT_S) -> dict:
    """HEAD-probe a single URL; return a classification dict with full probe
    diagnostics (response_time_ms, final_url, redirect_count,
    probe_method_used, error_message).

    GET-retry-on-timeout (forensic audit, 2026-07-06): a HEAD timeout is not
    always a dead/slow URL — some origins (WordPress/shared-hosting under
    concurrent load, specifically) stall on HEAD while answering GET
    normally and quickly. If the HEAD times out, retry once with GET before
    giving up; if the GET resolves to a qualifying status (200/301/302,
    same QUALIFIED_STATUSES used everywhere else), the URL is qualified.
    Only a HEAD timeout followed by a GET timeout (or GET error) is still
    classified as reject_reason="timeout" / the original error reason.
    """
    start = time.monotonic()

    try:
        resp = await client.head(url, follow_redirects=True, timeout=timeout_s)
        status = resp.status_code
        qualified = status in QUALIFIED_STATUSES
        reason_code = None if qualified else _reason_code_for_status(status)
        return {
            "url": url,
            "status_code": status,
            "qualified": qualified,
            "reject_reason": reason_code,
            "reason_code": reason_code,
            "low_priority": not qualified,
            "response_time_ms": int((time.monotonic() - start) * 1000),
            "final_url": str(resp.url),
            "redirect_count": len(resp.history),
            "probe_method_used": "HEAD",
            "error_message": None,
        }
    except httpx.TooManyRedirects as exc:
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": REASON_TOO_MANY_REDIRECTS, "reason_code": REASON_TOO_MANY_REDIRECTS,
            "low_priority": False,
            "response_time_ms": int((time.monotonic() - start) * 1000),
            "final_url": None, "redirect_count": 0,
            "probe_method_used": "HEAD",
            "error_message": str(exc),
        }
    except httpx.InvalidURL as exc:
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": REASON_INVALID_URL, "reason_code": REASON_INVALID_URL,
            "low_priority": False,
            "response_time_ms": int((time.monotonic() - start) * 1000),
            "final_url": None, "redirect_count": 0,
            "probe_method_used": "HEAD",
            "error_message": str(exc),
        }
    except httpx.TimeoutException:
        try:
            get_resp = await client.get(url, follow_redirects=True, timeout=timeout_s)
            status = get_resp.status_code
            qualified = status in QUALIFIED_STATUSES
            reason_code = None if qualified else _reason_code_for_status(status)
            return {
                "url": url,
                "status_code": status,
                "qualified": qualified,
                "reject_reason": reason_code,
                "reason_code": reason_code,
                "low_priority": not qualified,
                "response_time_ms": int((time.monotonic() - start) * 1000),
                "final_url": str(get_resp.url),
                "redirect_count": len(get_resp.history),
                "probe_method_used": "HEAD+GET",
                "error_message": None,
            }
        except httpx.TimeoutException:
            return {
                "url": url, "status_code": None, "qualified": False,
                "reject_reason": REASON_TIMEOUT, "reason_code": REASON_TIMEOUT,
                "low_priority": False,
                "response_time_ms": int((time.monotonic() - start) * 1000),
                "final_url": None, "redirect_count": 0,
                "probe_method_used": "HEAD+GET",
                "error_message": "HEAD and GET retry both timed out",
            }
        except Exception as exc:
            s = str(exc).lower()
            reason = REASON_SSL_ERROR if ("ssl" in s or "certificate" in s) else REASON_HEAD_FAILED_GET_FAILED
            return {
                "url": url, "status_code": None, "qualified": False,
                "reject_reason": reason, "reason_code": reason,
                "low_priority": False,
                "response_time_ms": int((time.monotonic() - start) * 1000),
                "final_url": None, "redirect_count": 0,
                "probe_method_used": "HEAD+GET",
                "error_message": str(exc),
            }
    except httpx.ConnectError as exc:
        s = str(exc).lower()
        if "getaddrinfo" in s or "name or service" in s:
            reason = REASON_DNS_FAILURE
        elif "ssl" in s or "certificate" in s:
            reason = REASON_SSL_ERROR
        else:
            reason = REASON_CONNECTION_REFUSED
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": reason, "reason_code": reason,
            "low_priority": False,
            "response_time_ms": int((time.monotonic() - start) * 1000),
            "final_url": None, "redirect_count": 0,
            "probe_method_used": "HEAD",
            "error_message": str(exc),
        }
    except Exception as exc:
        s = str(exc).lower()
        reason = REASON_SSL_ERROR if ("ssl" in s or "certificate" in s) else REASON_UNKNOWN
        return {
            "url": url, "status_code": None, "qualified": False,
            "reject_reason": reason, "reason_code": reason,
            "low_priority": False,
            "response_time_ms": int((time.monotonic() - start) * 1000),
            "final_url": None, "redirect_count": 0,
            "probe_method_used": "HEAD",
            "error_message": str(exc),
        }


async def _run_probes(
    urls: List[str], job_id: str, project_id: str,
    concurrency: int = PROBE_CONCURRENCY,
    timeout_s: float = PROBE_TIMEOUT_S,
    phase: str = "initial",
    url_source_map: Optional[dict] = None,
) -> List[dict]:
    """Probe all URLs under a semaphore; persist results to seo_audit_url_pool.

    concurrency/timeout_s/phase let the starvation-recovery retry pass reuse
    this exact function with different (gentler, more patient) parameters
    instead of duplicating the probe/persist logic.

    url_source_map (url -> {"sourceUrl":..., "sourceSitemap":..., "type":...})
    carries discovery provenance from seo_internal_links through to the
    qualification_details written per URL — purely additive metadata, does
    not affect probing or classification.
    """
    sem = asyncio.Semaphore(concurrency)
    url_source_map = url_source_map or {}

    async def bounded(url: str, client: httpx.AsyncClient) -> dict:
        async with sem:
            return await _probe_url(url, client, timeout_s)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; OditoQualifier/1.0)"},
        timeout=httpx.Timeout(timeout_s + 2.0),
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
            "probe_phase": phase,
            "probed_at": now,
            "response_time_ms": r.get("response_time_ms"),
            "final_url": r.get("final_url"),
            "redirect_count": r.get("redirect_count"),
            "probe_method_used": r.get("probe_method_used"),
            "error_message": r.get("error_message"),
            "qualification_details": _build_qualification_details(
                r, url_source_map.get(r["url"], {})
            ),
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

    # Step 1b: Fold host aliases (www vs non-www, or any other alias of the
    # same registrable domain) onto the canonical host LINK_DISCOVERY already
    # resolved for this audit run, then re-dedupe. This is defense-in-depth:
    # link_discovery.py already normalizes onto canonical_host before writing
    # to seo_internal_links, but older/pre-fix data (or any URL that slipped
    # through some other path) is corrected here too, before probing,
    # qualification, or canonicalUrls generation ever see it. `candidates` is
    # already priority-sorted; the first occurrence of a folded duplicate
    # wins, preserving that determinism. No-ops entirely if canonicalHost
    # wasn't provided (e.g. a project audited before this fix existed).
    duplicates_folded = 0
    if job.canonicalHost:
        seen_urls = set()
        deduped_candidates = []
        for c in candidates:
            folded_url = normalize_url(c["url"], canonical_host=job.canonicalHost)
            if folded_url in seen_urls:
                duplicates_folded += 1
                continue
            seen_urls.add(folded_url)
            deduped_candidates.append({**c, "url": folded_url})
        candidates = deduped_candidates

    discovered_count = len(candidates)
    print(
        f"[URL_QUAL] Candidate pool: {discovered_count} URLs"
        + (f" | {duplicates_folded} host-alias duplicates folded onto {job.canonicalHost}" if duplicates_folded else "")
    )

    if not candidates:
        result_data = {
            "canonicalUrls": [],
            "discoveredUrls": 0,
            "candidateUrls": 0,
            "qualifiedUrls": 0,
            "lowPriorityUrls": 0,
            "rejectedUrls": 0,
            "canonicalCount": 0,
            "starvationDetected": False,
            "retriedUrls": 0,
            "recoveredUrls": 0,
            "duplicatesFolded": 0,
            "duration_ms": 0,
        }
        _patch_reachability_stats(project_id, node_backend_url, result_data)
        _send_completion(job_id, node_backend_url, result_data)
        return {"status": "completed", "jobId": job_id, "canonicalUrls": []}

    candidate_urls = [c["url"] for c in candidates]

    # Discovery provenance per URL (sourceUrl/sourceSitemap/type from
    # seo_internal_links, threaded through get_candidate_pool) — used only to
    # enrich qualification_details' explanation, never to affect probing or
    # classification.
    url_source_map = {
        c["url"]: {
            "sourceUrl": c.get("sourceUrl"),
            "sourceSitemap": c.get("sourceSitemap"),
            "type": c.get("type"),
        }
        for c in candidates
    }

    # Step 2: Probe all candidates concurrently
    print(
        f"[URL_QUAL] Probing {len(candidate_urls)} URLs "
        f"| concurrency={PROBE_CONCURRENCY} | timeout={PROBE_TIMEOUT_S}s"
    )
    probe_results = await _run_probes(candidate_urls, job_id, project_id, url_source_map=url_source_map)
    probe_map = {r["url"]: r for r in probe_results}

    # Step 2b: Starvation detection + bounded retry.
    #
    # Only genuine timeouts are retried — real 404/5xx (low_priority) and
    # DNS/SSL/connection failures are final answers and are never touched
    # here (Case: a site with 22 real 404s must behave identically to
    # today; only a site that's losing its pool to unanswered requests
    # under concurrency gets a second, gentler look).
    timed_out_urls = [
        u for u in candidate_urls if probe_map.get(u, {}).get("reject_reason") == "timeout"
    ]
    usable_count = sum(
        1 for r in probe_map.values() if r.get("qualified") or r.get("low_priority")
    )
    starvation_detected = (
        len(timed_out_urls) >= STARVATION_MIN_TIMEOUTS
        and usable_count < len(candidate_urls) * STARVATION_POOL_FLOOR
    )

    retried_count = 0
    recovered_count = 0

    if starvation_detected:
        retry_targets = timed_out_urls[:MAX_RETRY_URLS]
        retried_count = len(retry_targets)
        print(
            f"[URL_QUAL] Starvation detected | timeouts={len(timed_out_urls)} "
            f"| usable={usable_count}/{len(candidate_urls)} "
            f"| retrying {retried_count} URLs at concurrency={RETRY_PROBE_CONCURRENCY} "
            f"timeout={RETRY_PROBE_TIMEOUT_S}s"
        )
        retry_results = await _run_probes(
            retry_targets, job_id, project_id,
            concurrency=RETRY_PROBE_CONCURRENCY,
            timeout_s=RETRY_PROBE_TIMEOUT_S,
            phase="retry",
            url_source_map=url_source_map,
        )
        for r in retry_results:
            if r.get("qualified") or r.get("low_priority"):
                recovered_count += 1
            probe_map[r["url"]] = r  # retry outcome is authoritative

        print(f"[URL_QUAL] Retry complete | recovered={recovered_count}/{retried_count}")

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
        "duplicatesFolded": duplicates_folded,
        "starvationDetected": starvation_detected,
        "retriedUrls": retried_count,
        "recoveredUrls": recovered_count,
        "duration_ms": duration_ms,
    }

    print(
        f"[URL_QUAL] Complete | qualified={len(qualified)} "
        f"| lowPriority={len(low_priority)} | rejected={rejected} "
        f"| canonical={len(canonical_urls)} | starvation={starvation_detected} "
        f"| recovered={recovered_count}/{retried_count} | duration={duration_ms}ms"
    )

    _patch_reachability_stats(project_id, node_backend_url, result_data)
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


def _patch_reachability_stats(project_id: str, node_backend_url: str, result_data: dict):
    """
    Persist reachability stats onto domain_technical_reports (non-fatal).

    Powers the AISO Coverage card's Site-Wide Reachability Ratio rule
    (AISO-CV9). Reuses the same endpoint TECHNICAL_DOMAIN writes the full
    report to, but omits `domain` — the handler treats that as a
    patch-only request and only updates an EXISTING report, never creates
    one (this job has no domain value to satisfy that required field).
    If TECHNICAL_DOMAIN hasn't run yet for this project, the patch is a
    harmless no-op and reachability simply stays unavailable until the
    next run — AISO-CV9 already SKIPs cleanly when this data is absent.
    """
    try:
        url = f"{node_backend_url}/api/jobs/domain-technical-report"
        resp = requests.post(
            url,
            json={
                "projectId": project_id,
                "discoveredUrls": result_data.get("discoveredUrls", 0),
                "qualifiedUrls": result_data.get("qualifiedUrls", 0),
                "lowPriorityUrls": result_data.get("lowPriorityUrls", 0),
                "rejectedUrls": result_data.get("rejectedUrls", 0),
            },
            timeout=10,
        )
        print(f"[URL_QUAL] Reachability stats patch | status={resp.status_code}")
    except Exception as exc:
        print(f"[URL_QUAL] Reachability stats patch failed (non-fatal) | error={exc}")
