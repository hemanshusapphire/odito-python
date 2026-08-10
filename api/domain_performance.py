"""Domain performance analysis API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from bson.objectid import ObjectId
from datetime import datetime
import os
import time
import requests
from urllib.parse import urlparse

from dotenv import load_dotenv
load_dotenv()

router = APIRouter()

# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------
_RETRY_DELAYS = [0, 30, 60]          # seconds before each attempt (3 total)
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503}
_PSI_TIMEOUT = 120                   # seconds per individual PSI request


class DomainPerformanceJob(BaseModel):
    jobId: str
    projectId: str
    userId: str
    main_url: str


# ---------------------------------------------------------------------------
# Node.js callback helpers
# ---------------------------------------------------------------------------

def _node_url():
    url = os.environ.get("NODE_BACKEND_URL", "")
    if not url:
        raise RuntimeError("NODE_BACKEND_URL not set")
    return url


def _emit_ws_event(project_id: str, event: str, payload: dict):
    """Fire a WebSocket event via the Node.js SSE/socket bridge (best-effort)."""
    try:
        requests.post(
            f"{_node_url()}/api/pagespeed/ws-event",
            json={"projectId": project_id, "event": event, "payload": payload},
            timeout=5,
        )
    except Exception as e:
        print(f"[WS] Failed to emit {event}: {e}")


def _mark_job_failed(job_id: str, error: str):
    try:
        requests.post(
            f"{_node_url()}/api/jobs/{job_id}/fail",
            json={"error": error},
            timeout=10,
        )
    except Exception as e:
        print(f"[CALLBACK] Failed to mark job failed | jobId={job_id} | {e}")


def _mark_job_complete(job_id: str, stats: dict):
    try:
        requests.post(
            f"{_node_url()}/api/jobs/{job_id}/complete",
            json={"stats": stats, "result_data": stats},
            timeout=10,
        )
    except Exception as e:
        print(f"[CALLBACK] Failed to mark job complete | jobId={job_id} | {e}")


# ---------------------------------------------------------------------------
# FastAPI route
# ---------------------------------------------------------------------------

@router.post("/jobs/domain-performance")
def handle_domain_performance(job: DomainPerformanceJob):
    """Handle DOMAIN_PERFORMANCE job dispatched from Node.js."""
    print(f"[DEBUG] DOMAIN_PERFORMANCE endpoint called | jobId={job.jobId} | projectId={job.projectId}")

    from main import completed_jobs, completed_jobs_lock
    from db import db

    try:
        with completed_jobs_lock:
            if job.jobId in completed_jobs:
                print(f"[INFO] Skipping already completed DOMAIN_PERFORMANCE job | jobId={job.jobId}")
                return {"status": "already_completed", "jobId": job.jobId}

        print(f"[WORKER] DOMAIN_PERFORMANCE started | jobId={job.jobId}")
        result = execute_domain_performance_logic(job)

        with completed_jobs_lock:
            completed_jobs.add(job.jobId)

        return {"status": "accepted", "jobId": job.jobId, "message": "DOMAIN_PERFORMANCE job accepted and processing"}

    except Exception as e:
        print(f"[ERROR] DOMAIN_PERFORMANCE handler failed | jobId={job.jobId} | reason=\"{e}\"")
        return {"status": "error", "jobId": job.jobId, "error": str(e)}


# ---------------------------------------------------------------------------
# Score validation
# ---------------------------------------------------------------------------

def validate_scores(metrics, device_type):
    if "error" in metrics:
        return
    zero_fields = [f for f in ["performance_score", "accessibility_score", "best_practices_score", "seo_score"]
                   if metrics.get(f, 0) == 0]
    if zero_fields:
        print(f"[WARNING] {device_type} zero scores: {zero_fields}")


# ---------------------------------------------------------------------------
# PSI fetch with exponential backoff retry
# ---------------------------------------------------------------------------

def _fetch_psi_with_retry(main_url: str, strategy: str, api_key: str, job_id: str) -> dict:
    """
    Call the Google PageSpeed Insights API with up to 3 attempts.

    Retry on: 429, 500, 502, 503, connection errors, timeouts.
    Do NOT retry: 400, 401, 403, 404, invalid URL errors.

    Returns metrics dict on success, or raises on permanent failure.
    """
    last_error = None
    api_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {
        "url": main_url,
        "strategy": strategy,
        "key": api_key,
        "category": ["performance", "accessibility", "best-practices", "seo"],
    }

    for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
        if delay > 0:
            print(f"[RETRY] {strategy} attempt {attempt} | waiting {delay}s | jobId={job_id}")
            time.sleep(delay)

        print(f"[PSI] {strategy} attempt {attempt}/{len(_RETRY_DELAYS)} | jobId={job_id}")
        try:
            response = requests.get(api_url, params=params, timeout=_PSI_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                metrics = extract_metrics(data)
                metrics["_attempt"] = attempt
                return metrics

            if response.status_code in _RETRYABLE_STATUS_CODES:
                last_error = f"HTTP {response.status_code}"
                print(f"[RETRY] {strategy} retryable error | status={response.status_code} | attempt={attempt}")
                continue

            # Non-retryable HTTP error (400, 401, 403, 404 …)
            body = response.text[:300]
            raise RuntimeError(f"PSI non-retryable error {response.status_code}: {body}")

        except requests.Timeout:
            last_error = f"Timeout after {_PSI_TIMEOUT}s"
            print(f"[RETRY] {strategy} timed out | attempt={attempt}")

        except requests.ConnectionError as ce:
            last_error = f"Connection error: {ce}"
            print(f"[RETRY] {strategy} connection error | attempt={attempt} | {ce}")

    raise RuntimeError(f"{strategy} PSI failed after {len(_RETRY_DELAYS)} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Core execution logic
# ---------------------------------------------------------------------------

def execute_domain_performance_logic(job: DomainPerformanceJob):
    """Execute domain-level PageSpeed analysis for both mobile and desktop."""
    start_ts = time.time()
    print(f"[DOMAIN_PERFORMANCE] Starting analysis for {job.main_url}")

    from db import db, seo_domain_performance, jobs

    api_key = os.getenv("PSI_API_KEY")
    if not api_key or api_key == "your_actual_api_key_here":
        raise ValueError("PSI_API_KEY not configured")

    print(f"[DOMAIN_PERFORMANCE] PSI key: {api_key[:6]}***")

    # Normalise URL
    main_url = job.main_url.strip()
    if not main_url.startswith(("http://", "https://")):
        main_url = "https://" + main_url
    if main_url.endswith("/") and main_url.count("/") > 3:
        main_url = main_url.rstrip("/")

    parsed = urlparse(main_url)
    domain = parsed.netloc
    if not domain:
        raise ValueError(f"Invalid URL: {main_url}")

    print(f"[DOMAIN_PERFORMANCE] Validated URL: {main_url} | domain: {domain}")

    # Emit started WS event
    _emit_ws_event(job.projectId, "pagespeed:started", {
        "jobId": job.jobId,
        "domain": domain,
        "timestamp": datetime.utcnow().isoformat(),
    })

    # ------------------------------------------------------------------
    # Fetch mobile metrics
    # ------------------------------------------------------------------
    mobile_metrics = None
    mobile_status = "completed"
    mobile_error = None
    mobile_attempts = 0

    try:
        mobile_metrics = _fetch_psi_with_retry(main_url, "mobile", api_key, job.jobId)
        mobile_attempts = mobile_metrics.pop("_attempt", 1)
        print(f"[DOMAIN_PERFORMANCE] Mobile OK | score={mobile_metrics.get('performance_score')} | attempts={mobile_attempts}")
        validate_scores(mobile_metrics, "mobile")
    except Exception as e:
        mobile_status = "failed"
        mobile_error = str(e)
        print(f"[ERROR] Mobile failed | {e}")

    # ------------------------------------------------------------------
    # Fetch desktop metrics
    # ------------------------------------------------------------------
    desktop_metrics = None
    desktop_status = "completed"
    desktop_error = None
    desktop_attempts = 0

    try:
        desktop_metrics = _fetch_psi_with_retry(main_url, "desktop", api_key, job.jobId)
        desktop_attempts = desktop_metrics.pop("_attempt", 1)
        print(f"[DOMAIN_PERFORMANCE] Desktop OK | score={desktop_metrics.get('performance_score')} | attempts={desktop_attempts}")
        validate_scores(desktop_metrics, "desktop")
    except Exception as e:
        desktop_status = "failed"
        desktop_error = str(e)
        print(f"[ERROR] Desktop failed | {e}")

    # ------------------------------------------------------------------
    # Both failed → abort without touching existing data
    # ------------------------------------------------------------------
    if mobile_status == "failed" and desktop_status == "failed":
        combined_error = f"mobile: {mobile_error} | desktop: {desktop_error}"
        duration_ms = int((time.time() - start_ts) * 1000)

        # Update only the scan metadata; preserve previous mobile/desktop data
        seo_domain_performance.update_one(
            {"project_id": ObjectId(job.projectId)},
            {
                "$set": {
                    "scan_status": "failed",
                    "last_run_at": datetime.utcnow(),
                    "last_error": combined_error,
                    "mobile_status": "failed",
                    "desktop_status": "failed",
                    "mobile_error": mobile_error,
                    "desktop_error": desktop_error,
                    "retry_count": mobile_attempts + desktop_attempts,
                    "duration_ms": duration_ms,
                    "updated_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

        jobs.update_one(
            {"_id": ObjectId(job.jobId)},
            {"$set": {"status": "failed", "completed_at": datetime.utcnow(),
                      "error_message": combined_error}},
        )

        _emit_ws_event(job.projectId, "pagespeed:failed", {
            "jobId": job.jobId,
            "error": combined_error,
            "timestamp": datetime.utcnow().isoformat(),
        })
        _mark_job_failed(job.jobId, combined_error)

        raise RuntimeError(combined_error)

    # ------------------------------------------------------------------
    # At least one succeeded — build partial-safe $set payload
    # ------------------------------------------------------------------
    duration_ms = int((time.time() - start_ts) * 1000)
    overall_status = "completed" if (mobile_status == "completed" and desktop_status == "completed") else "partial_success"

    # Base metadata — always written
    update_fields = {
        "project_id": ObjectId(job.projectId),
        "domain": domain,
        "scan_status": overall_status,
        "last_run_at": datetime.utcnow(),
        "last_error": None,
        "mobile_status": mobile_status,
        "desktop_status": desktop_status,
        "mobile_error": mobile_error,
        "desktop_error": desktop_error,
        "retry_count": mobile_attempts + desktop_attempts,
        "duration_ms": duration_ms,
        "job_id": job.jobId,
        "updated_at": datetime.utcnow(),
    }

    # Only write device data when that device succeeded
    if mobile_status == "completed" and mobile_metrics is not None:
        update_fields["mobile"] = mobile_metrics
        update_fields["last_successful_run_at"] = datetime.utcnow()
        update_fields["tested_at"] = datetime.utcnow()
    if desktop_status == "completed" and desktop_metrics is not None:
        update_fields["desktop"] = desktop_metrics
        update_fields["last_successful_run_at"] = datetime.utcnow()
        update_fields["tested_at"] = datetime.utcnow()

    seo_domain_performance.update_one(
        {"project_id": ObjectId(job.projectId)},
        {"$set": update_fields},
        upsert=True,
    )

    print(
        f"[DOMAIN_PERFORMANCE] Saved | domain={domain} | status={overall_status} "
        f"| mobile_score={mobile_metrics.get('performance_score') if mobile_metrics else 'N/A'} "
        f"| desktop_score={desktop_metrics.get('performance_score') if desktop_metrics else 'N/A'} "
        f"| duration={duration_ms}ms"
    )

    # Update job document
    jobs.update_one(
        {"_id": ObjectId(job.jobId)},
        {
            "$set": {
                "status": "completed",
                "completed_at": datetime.utcnow(),
                "result_data": {
                    "domain": domain,
                    "scan_status": overall_status,
                    "mobile_score": mobile_metrics.get("performance_score") if mobile_metrics else None,
                    "desktop_score": desktop_metrics.get("performance_score") if desktop_metrics else None,
                    "mobile_status": mobile_status,
                    "desktop_status": desktop_status,
                    "duration_ms": duration_ms,
                },
            }
        },
    )

    # Emit completion WS event
    _emit_ws_event(job.projectId, "pagespeed:completed", {
        "jobId": job.jobId,
        "status": overall_status,
        "mobileScore": mobile_metrics.get("performance_score") if mobile_metrics else None,
        "desktopScore": desktop_metrics.get("performance_score") if desktop_metrics else None,
        "mobileStatus": mobile_status,
        "desktopStatus": desktop_status,
        "timestamp": datetime.utcnow().isoformat(),
    })
    _mark_job_complete(job.jobId, {
        "domain": domain,
        "scan_status": overall_status,
        "mobile_score": mobile_metrics.get("performance_score") if mobile_metrics else None,
        "desktop_score": desktop_metrics.get("performance_score") if desktop_metrics else None,
    })

    return {
        "domain": domain,
        "mobile_metrics": mobile_metrics,
        "desktop_metrics": desktop_metrics,
        "scan_status": overall_status,
        "duration_ms": duration_ms,
    }


# ---------------------------------------------------------------------------
# Metric extraction helpers (unchanged API surface)
# ---------------------------------------------------------------------------

def extract_metrics(pagespeed_data):
    metrics = {}
    try:
        lr = pagespeed_data["lighthouseResult"]
        categories = lr["categories"]
        audits = lr["audits"]

        metrics["performance_score"]    = round((categories.get("performance",    {}).get("score", 0) or 0) * 100)
        metrics["accessibility_score"]  = round((categories.get("accessibility",  {}).get("score", 0) or 0) * 100)
        metrics["best_practices_score"] = round((categories.get("best-practices", {}).get("score", 0) or 0) * 100)
        metrics["seo_score"]            = round((categories.get("seo",            {}).get("score", 0) or 0) * 100)
        metrics["categories_debug"]     = list(categories.keys())

        metrics["metrics"] = {}

        _cwv = {
            "fcp":          "first-contentful-paint",
            "lcp":          "largest-contentful-paint",
            "cls":          "cumulative-layout-shift",
            "tbt":          "total-blocking-time",
            "speed_index":  "speed-index",
            "tti":          "interactive",
        }
        for key, audit_id in _cwv.items():
            if audit_id in audits:
                a = audits[audit_id]
                raw = a.get("numericValue", 0) or 0
                # CLS is unit-less; others convert ms→s except TBT which stays ms
                if key in ("cls", "tbt"):
                    value = raw
                else:
                    value = raw / 1000
                metrics["metrics"][key] = {
                    "value": value,
                    "unit": "score" if key == "cls" else ("ms" if key == "tbt" else "s"),
                    "display_value": a.get("displayValue", "N/A"),
                }
                # Backward-compat flat fields
                metrics[key] = metrics["metrics"][key]

        metrics["opportunities"] = extract_opportunities(audits)
        metrics["diagnostics"]   = extract_diagnostics(audits)

    except Exception as e:
        print(f"[ERROR] extract_metrics failed: {e}")
        metrics["error"] = str(e)

    return metrics


def extract_opportunities(audits):
    keys = [
        "render-blocking-resources", "unused-javascript", "uses-responsive-images",
        "uses-text-compression", "uses-long-cache-ttl", "uses-webp-images",
        "modern-image-formats", "efficient-animated-images", "offscreen-images",
        "properly-size-images", "unused-css-rules", "legacy-javascript",
    ]
    result = []
    for k in keys:
        if k in audits:
            a = audits[k]
            if a.get("score") is not None and a.get("score") < 1:
                result.append({
                    "id": k,
                    "title": a.get("title", k),
                    "description": a.get("description", ""),
                    "score": a.get("score", 0),
                    "displayValue": a.get("displayValue", "N/A"),
                    "numericValue": a.get("numericValue", 0),
                    "numericUnit": a.get("numericUnit", ""),
                    "details": a.get("details", {}),
                })
    return result


def extract_diagnostics(audits):
    keys = [
        "mainthread-work-breakdown", "bootup-time", "network-requests",
        "resource-summary", "layout-shift-elements", "cumulative-layout-shift",
        "largest-contentful-paint", "first-contentful-paint", "speed-index",
        "interactive", "total-blocking-time", "server-response-time",
    ]
    result = []
    for k in keys:
        if k in audits:
            a = audits[k]
            result.append({
                "id": k,
                "title": a.get("title", k),
                "description": a.get("description", ""),
                "score": a.get("score", 0),
                "displayValue": a.get("displayValue", "N/A"),
                "numericValue": a.get("numericValue", 0),
                "numericUnit": a.get("numericUnit", ""),
                "details": a.get("details", {}),
            })
    return result
