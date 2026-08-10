"""P0-004: seo_page_issues lifecycle-field backfill migration.

Populates the six P0-003 lifecycle fields (status, dedup_key,
first_detected_at, last_verified_at, fix_count, regression_count) on every
pre-existing seo_page_issues document that is missing them, preparing the
collection for the unique dedup_key index created later in P0-005.

The dedup_key is computed by IMPORTING the shared P0-003 helper
(scraper/workers/seo/page_analysis/rules/issue_identity.py) — the hash
algorithm is deliberately NOT reimplemented here, so backfilled keys are
byte-identical to the keys freshly-written issues receive.

Operational pattern (this repo has no migration framework — one-off scripts
are executed manually, mirroring odito_backend's seed scripts):

    cd python_workers

    # Dry run (default): read-only report, zero writes
    python -m scripts.backfill_seo_page_issues_lifecycle

    # Apply: batched backfill of missing fields only
    python -m scripts.backfill_seo_page_issues_lifecycle --apply

Design guarantees (frozen P0-004 requirements):
- Dry run performs zero writes and reports scanned / requiring-update /
  already-compliant counts plus duplicate dedup_key candidate groups.
- Apply mode only ever $sets fields that are MISSING on a given document —
  populated values are never overwritten (per-document $set construction,
  plus a dedup_key-$exists:false guard in every UpdateOne filter).
- Resume-safe by field existence, not progress files: an interrupted run's
  next invocation naturally selects only still-unprocessed documents.
- Idempotent: a second run after completion matches zero documents and
  performs zero writes.
- Duplicate detection REPORTS ONLY (group key mirrors the hash inputs:
  projectId, page_url, issue_code, data_path-normalized-to-""). Resolution
  is a later, human-reviewed step before P0-005 — never automatic here.
"""

import argparse
from datetime import datetime

from pymongo import UpdateOne

from scraper.workers.seo.page_analysis.rules.issue_identity import compute_dedup_key


LIFECYCLE_FIELDS = [
    "status",
    "dedup_key",
    "first_detected_at",
    "last_verified_at",
    "fix_count",
    "regression_count",
]

# A document needs backfill iff dedup_key is absent. dedup_key is the one
# field that can only come from this migration or the P0-003 factories (which
# always write all six together), so its absence is the authoritative
# "not yet processed" marker — and the natural resume filter.
MISSING_FILTER = {"dedup_key": {"$exists": False}}

# Only these fields are needed to compute the backfill for one document.
PROJECTION = {
    "_id": 1,
    "projectId": 1,
    "page_url": 1,
    "issue_code": 1,
    "data_path": 1,
    "created_at": 1,
    # The six lifecycle fields, so per-document $set can skip any that are
    # (partially) populated and must never be overwritten.
    **{field: 1 for field in LIFECYCLE_FIELDS},
}

DEFAULT_BATCH_SIZE = 1000
DUPLICATE_SAMPLE_LIMIT = 10


def _build_missing_field_set(doc):
    """The $set for one document: ONLY the lifecycle fields it is missing.

    Raises ValueError (propagated from compute_dedup_key) if the document
    lacks a usable identity — callers count it as invalid and skip it.
    """
    now = datetime.utcnow()
    # Mirror P0-003's factory semantics exactly: a new issue's
    # first_detected_at equals its created_at. For pre-existing documents the
    # moment of first detection IS the original created_at when present.
    detected_at = doc.get("created_at") or now

    full_set = {
        "status": "open",
        "dedup_key": compute_dedup_key(
            doc.get("projectId"),
            doc.get("page_url"),
            doc.get("issue_code"),
            doc.get("data_path"),
        ),
        "first_detected_at": detected_at,
        "last_verified_at": None,
        "fix_count": 0,
        "regression_count": 0,
    }

    return {field: value for field, value in full_set.items() if field not in doc}


def find_duplicate_candidates(collection, sample_limit=DUPLICATE_SAMPLE_LIMIT):
    """Report-only duplicate detection across the WHOLE collection.

    Groups by the exact dedup_key hash inputs — (projectId, page_url,
    issue_code, data_path normalized to "") — because these are the tuples
    P0-005's unique index will reject if left duplicated. Covers compliant
    and non-compliant documents alike: freshly-written P0-003 documents can
    collide with pre-existing rows for the same logical issue.
    """
    pipeline = [
        {
            "$group": {
                "_id": {
                    "projectId": "$projectId",
                    "page_url": "$page_url",
                    "issue_code": "$issue_code",
                    "data_path": {"$ifNull": ["$data_path", ""]},
                },
                "count": {"$sum": 1},
                "ids": {"$push": "$_id"},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
    ]

    group_count = 0
    duplicate_document_count = 0
    samples = []

    for group in collection.aggregate(pipeline, allowDiskUse=True):
        group_count += 1
        duplicate_document_count += group["count"]
        if len(samples) < sample_limit:
            samples.append({
                "key": group["_id"],
                "count": group["count"],
                "ids": [str(doc_id) for doc_id in group["ids"][:5]],
            })

    return {
        "duplicate_group_count": group_count,
        "duplicate_document_count": duplicate_document_count,
        "sample_groups": samples,
    }


def run_migration(collection, apply=False, batch_size=DEFAULT_BATCH_SIZE):
    """Execute one migration pass. Returns the summary dict.

    collection is injectable for tests; production main() passes the real
    seo_page_issues collection.
    """
    mode = "APPLY" if apply else "DRY_RUN"
    started_at = datetime.utcnow()
    print(f"[BACKFILL] seo_page_issues lifecycle backfill | mode={mode} | batch_size={batch_size}")

    total_documents = collection.count_documents({})
    requiring_update = collection.count_documents(MISSING_FILTER)
    already_compliant = total_documents - requiring_update

    summary = {
        "mode": mode,
        "documents_scanned": total_documents,
        "documents_requiring_update": requiring_update,
        "documents_already_compliant": already_compliant,
        "documents_updated": 0,
        "documents_invalid_skipped": 0,
        "invalid_sample_ids": [],
        "batches_executed": 0,
    }

    if apply:
        batch_ops = []

        def _flush():
            if not batch_ops:
                return
            result = collection.bulk_write(list(batch_ops), ordered=False)
            summary["documents_updated"] += result.modified_count
            summary["batches_executed"] += 1
            batch_ops.clear()
            print(f"[BACKFILL] batch flushed | updated_so_far={summary['documents_updated']}")

        for doc in collection.find(MISSING_FILTER, PROJECTION):
            try:
                missing_fields = _build_missing_field_set(doc)
            except ValueError as invalid_error:
                # A document without a usable identity (missing/empty
                # page_url or issue_code) cannot receive a dedup_key. Skip
                # and report — never crash the whole run, never write a
                # garbage key. These must be inspected before P0-005.
                summary["documents_invalid_skipped"] += 1
                if len(summary["invalid_sample_ids"]) < DUPLICATE_SAMPLE_LIMIT:
                    summary["invalid_sample_ids"].append(str(doc.get("_id")))
                print(f"[BACKFILL] invalid document skipped | _id={doc.get('_id')} | reason=\"{invalid_error}\"")
                continue

            if not missing_fields:
                continue

            batch_ops.append(UpdateOne(
                # dedup_key existence re-checked at write time: if a
                # concurrent run (or a P0-003 write racing an unlikely
                # same-_id path) already populated it, this op is a no-op —
                # apply is idempotent even under concurrency.
                {"_id": doc["_id"], "dedup_key": {"$exists": False}},
                {"$set": missing_fields},
            ))

            if len(batch_ops) >= batch_size:
                _flush()

        _flush()
    else:
        summary["duplicates"] = find_duplicate_candidates(collection)

    summary["duration_seconds"] = (datetime.utcnow() - started_at).total_seconds()

    print(f"[BACKFILL] complete | mode={mode}")
    print(f"[BACKFILL]   documents_scanned            = {summary['documents_scanned']}")
    print(f"[BACKFILL]   documents_requiring_update   = {summary['documents_requiring_update']}")
    print(f"[BACKFILL]   documents_already_compliant  = {summary['documents_already_compliant']}")
    if apply:
        print(f"[BACKFILL]   documents_updated            = {summary['documents_updated']}")
        print(f"[BACKFILL]   documents_invalid_skipped    = {summary['documents_invalid_skipped']}")
        print(f"[BACKFILL]   batches_executed             = {summary['batches_executed']}")
    else:
        duplicates = summary["duplicates"]
        print(f"[BACKFILL]   duplicate_group_count        = {duplicates['duplicate_group_count']}")
        print(f"[BACKFILL]   duplicate_document_count     = {duplicates['duplicate_document_count']}")
        for sample in duplicates["sample_groups"]:
            print(f"[BACKFILL]     duplicate group: {sample['key']} | count={sample['count']} | ids={sample['ids']}")
        if duplicates["duplicate_group_count"] > 0:
            print("[BACKFILL]   WARNING: duplicates MUST be reviewed and resolved before P0-005's unique index.")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Backfill P0-003 lifecycle fields onto pre-existing seo_page_issues documents."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform the batched backfill. Without this flag the script is a read-only dry run.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"UpdateOne ops per bulk_write flush (default {DEFAULT_BATCH_SIZE}).",
    )
    args = parser.parse_args()

    # Imported here, not at module top: db connects to Mongo at import time,
    # and tests exercise run_migration with an injected fake collection.
    from db import seo_page_issues

    run_migration(seo_page_issues, apply=args.apply, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
