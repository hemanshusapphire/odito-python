"""Issue lifecycle identity + transitions (P0-003, P3-002).

Single source of truth for the six lifecycle metadata fields stamped onto
every newly-created seo_page_issues document, the deterministic dedup_key
that is the collection's unique upsert key (P0-005), and — as of P3-002 —
the OPEN/RESOLVED/REOPENED transition logic applied after each fresh
PAGE_ANALYSIS pass.

reconcile_issue_lifecycle() takes a `collection` argument (never imports
`db` directly) so this module stays dependency-free at import time — both
issue factories (BaseSEORuleV2.create_issue, the live path used by every
rule, and page_analysis.create_issue, legacy) import compute_dedup_key/
lifecycle_fields from here without any circular-import risk; only the
caller that actually has a live Mongo collection touches Mongo.
"""

import hashlib


def compute_dedup_key(project_id, page_url, issue_code, data_path=None):
    """Deterministic, stable identity hash for one issue occurrence.

    Exact hashing inputs (frozen FES contract):

        sha256( project_id + "|" + page_url + "|" + issue_code + "|" + data_path )

    - project_id is coerced with str() (accepts a hex string or a bson
      ObjectId — both yield the identical 24-char hex form).
    - data_path of None is normalized to "" (many rules legitimately emit
      issues with no data_path; the key must still be stable for them).
    - "|" field delimiter prevents cross-field ambiguity (e.g. page_url
      "a" + issue_code "bc" colliding with "ab" + "c").
    - UTF-8 encoding; hex digest output.
    - No salt, no timestamps, no randomness: same inputs always produce
      the same output, across processes and across time.

    Raises ValueError on unusable input — an issue document without a valid
    identity must fail loudly at creation, never be written with a garbage
    key that later corrupts upsert behavior.
    """
    if project_id is None or str(project_id) == "":
        raise ValueError("compute_dedup_key: project_id is required")
    if not page_url or not isinstance(page_url, str):
        raise ValueError("compute_dedup_key: page_url must be a non-empty string")
    if not issue_code or not isinstance(issue_code, str):
        raise ValueError("compute_dedup_key: issue_code must be a non-empty string")

    normalized_data_path = "" if data_path is None else str(data_path)

    try:
        material = f"{str(project_id)}|{page_url}|{issue_code}|{normalized_data_path}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()
    except Exception as hash_error:
        raise ValueError(
            f"compute_dedup_key: hash generation failed for issue_code={issue_code!r}: {hash_error}"
        ) from hash_error


def lifecycle_fields(project_id, page_url, issue_code, data_path, detected_at):
    """The six P0-003 fields, ready to merge into a new issue document.

    detected_at is passed in (not computed here) so first_detected_at is
    exactly the same timestamp as the document's created_at — one clock
    read per document, no drift between the two fields.

    last_verified_at is None at creation: nothing has verified this issue
    yet; it is stamped by verification logic in a later task, never here.
    """
    return {
        "status": "open",
        "dedup_key": compute_dedup_key(project_id, page_url, issue_code, data_path),
        "first_detected_at": detected_at,
        "last_verified_at": None,
        "fix_count": 0,
        "regression_count": 0,
    }


def reconcile_issue_lifecycle(collection, project_id, page_url, current_dedup_keys, verified_at):
    """Transition seo_page_issues status for ONE page after a fresh
    PAGE_ANALYSIS pass (P3-002).

    Must be called once per page that was ACTUALLY just re-analyzed —
    current_dedup_keys (the dedup_keys of every issue this run detected for
    that exact page_url) is treated as authoritative for "what's present on
    this page right now". Never call this for a page whose analysis failed
    or was skipped: an empty/partial current_dedup_keys from a page that
    was never actually re-checked would wrongly resolve issues that are
    still there.

    Transitions (never deletes a document; first_detected_at is untouched —
    it is stamped once at creation by lifecycle_fields() and never
    revisited here):

    - OPEN + still detected      -> stays OPEN, last_verified_at refreshed.
    - OPEN + NOT detected        -> RESOLVED (status, resolved_at,
      last_verified_at, fix_count += 1).
    - RESOLVED + detected again  -> REOPENED, i.e. status back to "open"
      (regression_count += 1, last_verified_at refreshed). resolved_at is
      left as-is — a record of the last resolution — until it resolves
      again.

    current_dedup_keys may be empty (a page with zero issues detected this
    run) — every previously-open issue for that page resolves, which is
    exactly the case the plain `status: "open"` filter (no dedup_key
    clause) below already covers.

    Returns a dict of the three update_many results for logging/testing:
    {"reopened": n, "still_open_refreshed": n, "resolved": n}.
    """
    # Local import: keeps compute_dedup_key/lifecycle_fields importable with
    # zero non-stdlib dependencies (this function is the only one that
    # actually touches a live Mongo collection, so it's the only one that
    # needs bson).
    from bson.objectid import ObjectId
    project_oid = project_id if isinstance(project_id, ObjectId) else ObjectId(project_id)
    keys = list(current_dedup_keys or [])

    reopened_result = still_open_result = resolved_result = None

    if keys:
        # RESOLVED, detected again -> REOPENED. Matches only status:
        # "resolved" docs, so this can never touch a doc the two updates
        # below also match (those only ever match status: "open").
        reopened_result = collection.update_many(
            {
                "projectId": project_oid,
                "page_url": page_url,
                "status": "resolved",
                "dedup_key": {"$in": keys},
            },
            {
                "$set": {"status": "open", "last_verified_at": verified_at},
                "$inc": {"regression_count": 1},
            },
        )

        # Still OPEN, detected again -> unchanged, just refresh the
        # verification timestamp.
        still_open_result = collection.update_many(
            {
                "projectId": project_oid,
                "page_url": page_url,
                "status": "open",
                "dedup_key": {"$in": keys},
            },
            {"$set": {"last_verified_at": verified_at}},
        )

    # OPEN, NOT detected this run -> RESOLVED. No dedup_key clause at all
    # when keys is empty — every currently-open issue on this page resolves.
    resolve_filter = {
        "projectId": project_oid,
        "page_url": page_url,
        "status": "open",
    }
    if keys:
        resolve_filter["dedup_key"] = {"$nin": keys}
    resolved_result = collection.update_many(
        resolve_filter,
        {
            "$set": {"status": "resolved", "resolved_at": verified_at, "last_verified_at": verified_at},
            "$inc": {"fix_count": 1},
        },
    )

    return {
        "reopened": reopened_result.modified_count if reopened_result else 0,
        "still_open_refreshed": still_open_result.modified_count if still_open_result else 0,
        "resolved": resolved_result.modified_count if resolved_result else 0,
    }
