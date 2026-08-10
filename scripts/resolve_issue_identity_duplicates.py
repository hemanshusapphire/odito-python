"""Pre-P0-005 one-off: resolve historic seo_page_issues identity duplicates.

The P0-004 dry-run found 216 duplicate identity groups (project, page_url,
issue_code, data_path), all pairwise:

- 215 groups: titlePixelLengthRule emitted desktop AND mobile findings with
  the same data_path ("title_pixel_width"). The rule has been fixed to write
  .desktop/.mobile suffixes going forward; this script retags the HISTORIC
  documents the same way, reconstructing the device from each document's
  detected_value prefix ("Desktop title width: ..." / "Mobile title width:
  ..."). Zero data loss — both findings survive with distinct identities.

- 1 group: FormLabelsRule emitted two findings sharing data_path
  ("axeViolations"). Historic docs don't store the axe violation id, so the
  suffix is not reconstructible: keep the first-inserted document, back up
  and delete the other (full audit re-creates issues on every run, so the
  loss is one transient row that the next audit rewrites with the fixed
  rule's distinct data_paths).

Deleted documents are copied to `seo_page_issues_removed_duplicates` before
deletion — restorable via insert back into seo_page_issues.

Usage (from python_workers/):
    python -m scripts.resolve_issue_identity_duplicates            # dry run
    python -m scripts.resolve_issue_identity_duplicates --apply
"""

import argparse

from pymongo import UpdateOne

DESKTOP_PATH = "title_pixel_width.desktop"
MOBILE_PATH = "title_pixel_width.mobile"


def _duplicate_groups(collection):
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
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def resolve(collection, backup_collection, apply=False):
    mode = "APPLY" if apply else "DRY_RUN"
    print(f"[RESOLVE] seo_page_issues duplicate resolution | mode={mode}")

    groups = _duplicate_groups(collection)
    summary = {
        "mode": mode,
        "groups_found": len(groups),
        "title_pixel_retagged": 0,
        "kept_first_deleted": 0,
        "unresolvable": 0,
        "unresolvable_groups": [],
    }

    retag_ops = []
    delete_ids = []

    for group in groups:
        ids = group["ids"]
        docs = list(collection.find({"_id": {"$in": ids}}))

        if group["_id"]["issue_code"] == "title_pixel_length":
            desktop = [d for d in docs if str(d.get("detected_value", "")).startswith("Desktop")]
            mobile = [d for d in docs if str(d.get("detected_value", "")).startswith("Mobile")]
            if len(desktop) == 1 and len(mobile) == 1 and len(docs) == 2:
                retag_ops.append(UpdateOne({"_id": desktop[0]["_id"]}, {"$set": {"data_path": DESKTOP_PATH}}))
                retag_ops.append(UpdateOne({"_id": mobile[0]["_id"]}, {"$set": {"data_path": MOBILE_PATH}}))
                summary["title_pixel_retagged"] += 2
                continue
            # Unexpected shape (e.g. two Desktop docs) — fall through to
            # keep-first below rather than guessing.

        # Generic fallback: keep the first-inserted (lowest _id), back up and
        # delete the rest.
        docs_sorted = sorted(docs, key=lambda d: str(d["_id"]))
        for doc in docs_sorted[1:]:
            delete_ids.append(doc["_id"])
        summary["kept_first_deleted"] += len(docs_sorted) - 1
        if group["_id"]["issue_code"] != "form_labels" and group["_id"]["issue_code"] != "title_pixel_length":
            summary["unresolvable"] += 1
            summary["unresolvable_groups"].append(group["_id"])

    print(f"[RESOLVE]   groups_found          = {summary['groups_found']}")
    print(f"[RESOLVE]   title_pixel_retagged  = {summary['title_pixel_retagged']} docs")
    print(f"[RESOLVE]   kept_first_deleted    = {summary['kept_first_deleted']} docs")

    if apply:
        if retag_ops:
            result = collection.bulk_write(retag_ops, ordered=False)
            print(f"[RESOLVE]   retag applied: modified={result.modified_count}")
        if delete_ids:
            to_backup = list(collection.find({"_id": {"$in": delete_ids}}))
            if to_backup:
                backup_collection.insert_many(to_backup)
                print(f"[RESOLVE]   backed up {len(to_backup)} doc(s) to {backup_collection.name}")
            delete_result = collection.delete_many({"_id": {"$in": delete_ids}})
            print(f"[RESOLVE]   deleted {delete_result.deleted_count} duplicate doc(s)")

        remaining = _duplicate_groups(collection)
        print(f"[RESOLVE]   remaining duplicate groups after apply = {len(remaining)}")
        summary["remaining_groups_after_apply"] = len(remaining)
    else:
        print(f"[RESOLVE]   (dry run — no writes; --apply to execute)")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Resolve historic seo_page_issues identity duplicates (pre-P0-005).")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    from db import seo_page_issues, db as database
    backup = database["seo_page_issues_removed_duplicates"]
    resolve(seo_page_issues, backup, apply=args.apply)


if __name__ == "__main__":
    main()
