#!/usr/bin/env python3
"""
Backfill page_context for existing seo_page_data records.

Populates the structured page_context field (framework, cms, pageType) for
all documents that were created before the context enrichment layer was
deployed, so they benefit from context-aware recommendations without needing
a full re-crawl.

Usage
-----
  cd python_workers
  python scripts/backfill_page_context.py [--project-id <id>] [--dry-run] [--batch 50]

Options
  --project-id  Scope to a single project (default: all projects)
  --dry-run     Print detections without writing to MongoDB
  --batch       Documents per batch (default: 100, max: 500)
  --limit       Max documents to process (default: unlimited)

Strategy
--------
  1. Query seo_page_data where page_context is missing
  2. For each document:
     a. Run enrich_from_page_data() using raw_html, title, meta_tags, etc.
     b. Update the document with the structured page_context
     c. Also update flat framework / cms / page_type fields for compat
  3. Log results
  4. No HTTP requests — pure offline processing of stored data

Performance
-----------
  ~200-500 docs/sec depending on HTML size.
  A project with 1 000 pages takes roughly 5-30 seconds.
  Documents without raw_html rely on URL/title/schema detection only.
"""

import argparse
import sys
import os
import time
from typing import Optional

# Allow running from the python_workers directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson.objectid import ObjectId
from pymongo import UpdateOne

from db import seo_page_data as page_data_col
from shared.context_enrichment import enrich_from_page_data


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill page_context for seo_page_data")
    parser.add_argument("--project-id", help="Scope to one project ID")
    parser.add_argument("--dry-run",    action="store_true", help="Preview without writing")
    parser.add_argument("--batch",      type=int, default=100, help="Batch size (max 500)")
    parser.add_argument("--limit",      type=int, default=0,   help="Max documents (0 = unlimited)")
    return parser.parse_args()


def build_query(project_id: Optional[str]) -> dict:
    """Build MongoDB query for documents missing page_context."""
    query = {
        "$or": [
            {"page_context": {"$exists": False}},
            {"page_context": None},
        ]
    }
    if project_id:
        query["projectId"] = ObjectId(project_id)
    return query


def process_batch(docs: list, dry_run: bool) -> dict:
    """Process a batch of documents and write updates."""
    stats = {"processed": 0, "updated": 0, "errors": 0, "skipped": 0}
    ops = []

    for doc in docs:
        stats["processed"] += 1
        url = doc.get("url") or doc.get("page_url", "")
        if not url:
            stats["skipped"] += 1
            continue

        try:
            page_context = enrich_from_page_data(doc, url)

            fw   = page_context.get("framework", {})
            cms  = page_context.get("cms")
            pt   = page_context.get("pageType", {})

            update_fields = {
                "page_context":          page_context,
                "framework":             fw.get("key", "generic"),
                "cms":                   cms.get("name") if cms else None,
                "page_type":             pt.get("name", "Generic"),
                "page_type_confidence":  pt.get("confidence", 0),
            }

            if dry_run:
                print(
                    f"  [DRY-RUN] {url[:80]} | "
                    f"fw={fw.get('name')} ({fw.get('confidence')}%) | "
                    f"cms={cms.get('name') if cms else 'none'} | "
                    f"type={pt.get('name')} ({pt.get('confidence')}%)"
                )
            else:
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}))

            stats["updated"] += 1

        except Exception as e:
            print(f"  [ERROR] {url}: {e}")
            stats["errors"] += 1

    if ops and not dry_run:
        result = page_data_col.bulk_write(ops, ordered=False)
        # bulk_write result has modified_count
        _ = result

    return stats


def run_backfill(project_id: Optional[str], dry_run: bool, batch_size: int, limit: int):
    query   = build_query(project_id)
    total   = page_data_col.count_documents(query)
    to_do   = min(total, limit) if limit > 0 else total

    scope = f"project={project_id}" if project_id else "all projects"
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Backfill page_context | {scope}")
    print(f"  Documents to process: {to_do:,} of {total:,} missing page_context")
    print(f"  Batch size: {batch_size} | Limit: {limit or 'unlimited'}")
    print()

    if to_do == 0:
        print("Nothing to do — all documents already have page_context.")
        return

    # Projection: include fields used by context_enrichment
    projection = {
        "_id": 1, "url": 1,
        "raw_html": 1,
        "title": 1,
        "meta_tags": 1,
        "structured_data": 1,
        "content": 1,
        "headings": 1,
    }

    cursor = page_data_col.find(query, projection).batch_size(batch_size)

    totals  = {"processed": 0, "updated": 0, "errors": 0, "skipped": 0}
    done    = 0
    start   = time.time()

    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            stats = process_batch(batch, dry_run)
            for k in totals:
                totals[k] += stats[k]
            done += len(batch)
            batch = []

            elapsed = time.time() - start
            rate    = done / elapsed if elapsed > 0 else 0
            pct     = (done / to_do * 100) if to_do > 0 else 100
            print(
                f"  Progress: {done:,}/{to_do:,} ({pct:.1f}%) | "
                f"{rate:.0f} docs/s | "
                f"errors={totals['errors']}"
            )

        if limit > 0 and done >= limit:
            break

    # Flush remaining
    if batch:
        stats = process_batch(batch, dry_run)
        for k in totals:
            totals[k] += stats[k]
        done += len(batch)

    elapsed = time.time() - start
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Backfill complete")
    print(f"  Processed : {totals['processed']:,}")
    print(f"  Updated   : {totals['updated']:,}")
    print(f"  Errors    : {totals['errors']:,}")
    print(f"  Skipped   : {totals['skipped']:,} (missing URL)")
    print(f"  Time      : {elapsed:.1f}s ({totals['processed']/elapsed:.0f} docs/s)")


if __name__ == "__main__":
    args = parse_args()
    batch_size = min(max(args.batch, 1), 500)
    run_backfill(
        project_id=args.project_id,
        dry_run=args.dry_run,
        batch_size=batch_size,
        limit=args.limit,
    )
