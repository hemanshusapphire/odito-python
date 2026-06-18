"""
Enterprise-grade performance profiling utilities for Odito workers.

Provides high-resolution timing, memory snapshots, and structured performance
reporting for worker pipelines. Zero-overhead when disabled.

Usage:
    from scraper.shared.perf_tracker import PerformanceTracker, WorkerTimingContext

    tracker = PerformanceTracker("HEADLESS_A11Y", job_id="abc123")
    with tracker.stage("browser_launch"):
        browser = await pw.chromium.launch(...)

    tracker.log_summary()

Environment:
    PERF_PROFILING=1  — Enable profiling (default: 1)
"""

import os
import time
import json
import statistics
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROFILING_ENABLED = os.environ.get("PERF_PROFILING", "1") == "1"

# Optional memory tracking — only import if needed
_tracemalloc_available = False
try:
    import tracemalloc
    _tracemalloc_available = True
except ImportError:
    pass


# ---------------------------------------------------------------------------
# StageProfiler — context manager for timing individual stages
# ---------------------------------------------------------------------------
class StageProfiler:
    """Context manager that records high-resolution timing for a named stage.

    Usage:
        with StageProfiler("axe_injection") as sp:
            await page.evaluate(axe_script)
        print(sp.elapsed_ms)
    """

    __slots__ = ("name", "start_time", "end_time", "elapsed_ms", "metadata")

    def __init__(self, name: str, **metadata):
        self.name = name
        self.start_time = 0.0
        self.end_time = 0.0
        self.elapsed_ms = 0.0
        self.metadata = metadata

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.elapsed_ms = round((self.end_time - self.start_time) * 1000, 2)
        return False  # Don't suppress exceptions


# ---------------------------------------------------------------------------
# PerformanceTracker — aggregates stage timings for a single unit of work
# ---------------------------------------------------------------------------
class PerformanceTracker:
    """High-resolution performance tracker for a worker or URL processing unit.

    Records stage-by-stage timing, optional memory snapshots, and produces
    structured performance reports.

    Args:
        worker_name: Identifier for the worker type (e.g., "HEADLESS_A11Y")
        job_id: Job identifier for correlation
        url: Optional URL being processed (for per-URL tracking)
        enable_memory: Whether to capture memory snapshots (default: False)
    """

    def __init__(
        self,
        worker_name: str,
        job_id: str = "",
        url: str = "",
        enable_memory: bool = False,
    ):
        self.worker_name = worker_name
        self.job_id = job_id
        self.url = url
        self.enabled = PROFILING_ENABLED
        self.enable_memory = enable_memory and _tracemalloc_available

        self._stages: list[dict] = []
        self._start_time = time.perf_counter()
        self._memory_snapshots: list[dict] = []

        if self.enable_memory and not tracemalloc.is_tracing():
            tracemalloc.start()

    @contextmanager
    def stage(self, name: str, **metadata):
        """Context manager to time a named stage.

        Args:
            name: Stage name (e.g., "browser_launch", "axe_injection")
            **metadata: Additional key-value pairs to include in the stage record
        """
        if not self.enabled:
            yield
            return

        profiler = StageProfiler(name, **metadata)
        with profiler:
            yield profiler

        stage_record = {
            "stage": name,
            "duration_ms": profiler.elapsed_ms,
        }
        if metadata:
            stage_record["metadata"] = metadata

        self._stages.append(stage_record)

    def record_stage(self, name: str, duration_ms: float, **metadata):
        """Manually record a stage timing (for cases where context manager isn't suitable)."""
        if not self.enabled:
            return
        stage_record = {
            "stage": name,
            "duration_ms": round(duration_ms, 2),
        }
        if metadata:
            stage_record["metadata"] = metadata
        self._stages.append(stage_record)

    def snapshot_memory(self, label: str = ""):
        """Capture current memory usage snapshot."""
        if not self.enable_memory:
            return

        current, peak = tracemalloc.get_traced_memory()
        self._memory_snapshots.append({
            "label": label,
            "current_mb": round(current / 1024 / 1024, 2),
            "peak_mb": round(peak / 1024 / 1024, 2),
            "timestamp_ms": round((time.perf_counter() - self._start_time) * 1000, 2),
        })

    def get_total_ms(self) -> float:
        """Get total elapsed time since tracker creation."""
        return round((time.perf_counter() - self._start_time) * 1000, 2)

    def get_stage_durations(self) -> dict[str, float]:
        """Get a mapping of stage name -> duration_ms."""
        return {s["stage"]: s["duration_ms"] for s in self._stages}

    def to_dict(self) -> dict:
        """Export full performance report as a dictionary."""
        total_ms = self.get_total_ms()
        report = {
            "worker": self.worker_name,
            "job_id": self.job_id,
            "total_ms": total_ms,
            "stages": self._stages,
        }
        if self.url:
            report["url"] = self.url
        if self._memory_snapshots:
            report["memory"] = self._memory_snapshots
        return report

    def log_summary(self, prefix: str = ""):
        """Print structured one-line performance summary."""
        if not self.enabled:
            return

        total_ms = self.get_total_ms()
        stage_summary = " | ".join(
            f"{s['stage']}={s['duration_ms']:.0f}ms" for s in self._stages
        )
        url_part = f" | url={self.url}" if self.url else ""

        tag = f"[PERF:{self.worker_name}]"
        if prefix:
            tag = f"[PERF:{prefix}]"

        print(
            f"{tag} total={total_ms:.0f}ms{url_part} | {stage_summary}"
            f" | jobId={self.job_id}"
        )


# ---------------------------------------------------------------------------
# WorkerTimingContext — aggregates per-URL timings across a full job
# ---------------------------------------------------------------------------
class WorkerTimingContext:
    """Aggregates PerformanceTracker results across multiple URLs in a job.

    Computes summary statistics (mean, median, p95, max) per stage.

    Usage:
        ctx = WorkerTimingContext("HEADLESS_A11Y", job_id="abc123")
        for url in urls:
            tracker = ctx.url_tracker(url)
            with tracker.stage("navigation"):
                ...
            ctx.complete_url(tracker)
        ctx.log_job_summary()
    """

    def __init__(self, worker_name: str, job_id: str = ""):
        self.worker_name = worker_name
        self.job_id = job_id
        self.enabled = PROFILING_ENABLED
        self._url_reports: list[dict] = []
        self._job_start = time.perf_counter()

    def url_tracker(self, url: str, enable_memory: bool = False) -> PerformanceTracker:
        """Create a new PerformanceTracker for a specific URL."""
        return PerformanceTracker(
            self.worker_name,
            job_id=self.job_id,
            url=url,
            enable_memory=enable_memory,
        )

    def complete_url(self, tracker: PerformanceTracker):
        """Record a completed URL's performance data."""
        if not self.enabled:
            return
        self._url_reports.append(tracker.to_dict())

    def get_stage_statistics(self) -> dict[str, dict]:
        """Compute per-stage statistics across all URLs.

        Returns:
            Dict mapping stage name to {mean, median, p95, max, min, count}
        """
        if not self._url_reports:
            return {}

        # Collect all durations per stage
        stage_durations: dict[str, list[float]] = {}
        for report in self._url_reports:
            for stage in report.get("stages", []):
                name = stage["stage"]
                if name not in stage_durations:
                    stage_durations[name] = []
                stage_durations[name].append(stage["duration_ms"])

        # Compute statistics
        stats = {}
        for name, durations in stage_durations.items():
            sorted_d = sorted(durations)
            count = len(sorted_d)
            p95_idx = max(0, int(count * 0.95) - 1)

            stats[name] = {
                "count": count,
                "mean_ms": round(statistics.mean(sorted_d), 1),
                "median_ms": round(statistics.median(sorted_d), 1),
                "p95_ms": round(sorted_d[p95_idx], 1),
                "max_ms": round(max(sorted_d), 1),
                "min_ms": round(min(sorted_d), 1),
                "total_ms": round(sum(sorted_d), 1),
            }

        return stats

    def get_job_summary(self) -> dict:
        """Get full job performance summary."""
        total_ms = round((time.perf_counter() - self._job_start) * 1000, 2)
        url_times = [r["total_ms"] for r in self._url_reports]

        summary = {
            "worker": self.worker_name,
            "job_id": self.job_id,
            "total_job_ms": total_ms,
            "urls_processed": len(self._url_reports),
            "stage_statistics": self.get_stage_statistics(),
        }

        if url_times:
            summary["url_timing"] = {
                "mean_ms": round(statistics.mean(url_times), 1),
                "median_ms": round(statistics.median(url_times), 1),
                "max_ms": round(max(url_times), 1),
                "min_ms": round(min(url_times), 1),
            }

        return summary

    def log_job_summary(self):
        """Print structured job-level performance summary."""
        if not self.enabled:
            return

        summary = self.get_job_summary()
        total_ms = summary["total_job_ms"]
        url_count = summary["urls_processed"]

        print(f"\n{'='*72}")
        print(f"[PERF:{self.worker_name}] JOB PERFORMANCE SUMMARY")
        print(f"{'='*72}")
        print(f"  Job ID:        {self.job_id}")
        print(f"  Total Time:    {total_ms:.0f}ms ({total_ms/1000:.1f}s)")
        print(f"  URLs Processed: {url_count}")

        if "url_timing" in summary:
            ut = summary["url_timing"]
            print(f"  Avg URL Time:  {ut['mean_ms']:.0f}ms")
            print(f"  Max URL Time:  {ut['max_ms']:.0f}ms")

        stage_stats = summary.get("stage_statistics", {})
        if stage_stats:
            print(f"\n  {'Stage':<28} {'Mean':>8} {'P50':>8} {'P95':>8} {'Max':>8} {'Total':>10}")
            print(f"  {'-'*28} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
            for stage, s in stage_stats.items():
                print(
                    f"  {stage:<28} {s['mean_ms']:>7.0f}ms {s['median_ms']:>7.0f}ms "
                    f"{s['p95_ms']:>7.0f}ms {s['max_ms']:>7.0f}ms {s['total_ms']:>9.0f}ms"
                )

        print(f"{'='*72}\n")
