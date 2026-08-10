"""SEO scoring worker with dynamic schema discovery and normalization."""

from datetime import datetime
from typing import Dict, Any, List
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from bson import ObjectId
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from scraper.shared.utils import send_completion_callback, send_failure_callback

# Import is_job_cancelled from the correct location
def is_job_cancelled(job_id: str) -> bool:
    """Check if a job has been cancelled"""
    try:
        from main import cancelled_jobs, cancelled_jobs_lock
        with cancelled_jobs_lock:
            return job_id in cancelled_jobs
    except Exception:
        # If import fails, assume not cancelled (fail-safe)
        return False

def get_db_connection():
    """Get MongoDB connection"""
    try:
        # Use the same connection as db.py for consistency
        import db
        return db.db
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        raise

def discover_collection_schema(db, collection_name: str) -> Dict[str, Any]:
    """Discover schema from a collection by sampling documents.

    Returns {} when the collection is empty so callers can skip it in field detection.
    """
    try:
        collection = db[collection_name]
        sample = collection.find_one()

        if not sample:
            print(f"[SCHEMA] {collection_name} is empty")
            return {}  # empty dict = empty collection; callers must check with `if schema`

        top_level_keys = list(sample.keys())
        print(f"[SCHEMA] {collection_name} top-level keys: {top_level_keys}")

        nested_structure = {}
        for key, value in sample.items():
            if isinstance(value, dict):
                nested_structure[key] = list(value.keys())

        return {
            "sample": sample,
            "top_level_keys": top_level_keys,
            "nested_structure": nested_structure
        }

    except Exception as e:
        print(f"[ERROR] Schema discovery failed for {collection_name}: {e}")
        return {}

def detect_project_field(schemas: Dict[str, Any]) -> str:
    """Detect the project identifier field from non-empty collections only.

    seo_page_performance is optional — it may be empty when SEO_SCORING runs.
    Requiring it in an intersection causes a guaranteed failure on fresh audits.
    We resolve by voting: the field that appears in the most non-empty collections wins.
    Tie-break order matches the priority list.
    """
    possible_fields = ["projectId", "project_id", "seo_project_id", "projectid"]

    # Only vote from collections that actually have documents
    required_collections = ["seo_page_data", "seo_page_issues"]
    optional_collections = ["seo_page_performance"]

    for field in possible_fields:
        # Must be present in all required (non-empty) collections
        found_in_required = all(
            field in schemas.get(col, {}).get("top_level_keys", [])
            for col in required_collections
            if schemas.get(col)  # skip if schema is empty dict (empty collection)
        )
        if found_in_required:
            print(f"[SCHEMA] Detected project field: {field}")
            return field

    print(f"[ERROR] No common project field found across collections")
    return None

def detect_url_field(schemas: Dict[str, Any]) -> Dict[str, str]:
    """Detect the URL field in page data and issues collections (can be different)"""
    possible_fields = ["url", "page_url", "pageUrl", "page", "link_url"]
    
    data_schema = schemas.get("seo_page_data", {})
    issues_schema = schemas.get("seo_page_issues", {})
    data_keys = data_schema.get("top_level_keys", [])
    issues_keys = issues_schema.get("top_level_keys", [])
    
    # Find URL field in page data
    data_url_field = None
    for field in possible_fields:
        if field in data_keys:
            data_url_field = field
            break
    
    # Find URL field in issues
    issues_url_field = None
    for field in possible_fields:
        if field in issues_keys:
            issues_url_field = field
            break
    
    if not data_url_field or not issues_url_field:
        print(f"[ERROR] URL field not found - data_field={data_url_field}, issues_field={issues_url_field}")
        return None
    
    result = {
        "data": data_url_field,
        "issues": issues_url_field
    }
    
    print(f"[SCHEMA] Detected URL fields: {result}")
    return result

def detect_severity_field(schemas: Dict[str, Any]) -> str:
    """Detect the severity field in issues collection"""
    possible_fields = ["severity", "level", "priority", "type", "issue_type"]
    
    issues_schema = schemas.get("seo_page_issues", {})
    top_level_keys = issues_schema.get("top_level_keys", [])
    
    for field in possible_fields:
        if field in top_level_keys:
            print(f"[SCHEMA] Detected severity field: {field}")
            return field
    
    print(f"[ERROR] No severity field found in issues collection")
    return None

def detect_rule_field(schemas: Dict[str, Any]) -> str:
    """Detect the rule identifier field in issues collection"""
    possible_fields = ["rule_id", "issue_code", "code", "rule", "rule_code", "issue_id"]
    
    issues_schema = schemas.get("seo_page_issues", {})
    top_level_keys = issues_schema.get("top_level_keys", [])
    
    for field in possible_fields:
        if field in top_level_keys:
            print(f"[SCHEMA] Detected rule field: {field}")
            return field
    
    print(f"[ERROR] No rule field found in issues collection")
    return None

def build_canonical_field_mapping(schemas: Dict[str, Any]) -> Dict[str, str]:
    """Build mapping from canonical names to actual field names"""
    mapping = {}
    
    # Detect each field
    project_field = detect_project_field(schemas)
    url_fields = detect_url_field(schemas)
    severity_field = detect_severity_field(schemas)
    rule_field = detect_rule_field(schemas)
    
    if not all([project_field, url_fields, severity_field, rule_field]):
        print(f"[ERROR] Schema detection incomplete - missing required fields")
        return None
    
    mapping = {
        "project": project_field,
        "url_data": url_fields["data"],      # URL field in page data
        "url_issues": url_fields["issues"],  # URL field in issues
        "severity": severity_field,
        "rule": rule_field
    }
    
    print(f"[SCHEMA] Canonical field mapping: {mapping}")
    return mapping

def normalize_severity(severity_value: Any) -> str:
    """Normalize severity to standard values"""
    if not severity_value:
        return "low"
    
    severity_str = str(severity_value).lower()
    
    if severity_str in ["high", "critical", "severe", "error"]:
        return "high"
    elif severity_str in ["medium", "moderate", "warning", "warn"]:
        return "medium"
    else:
        return "low"

def calculate_page_score(high_issues: int, medium_issues: int, low_issues: int, word_count: int = 300) -> float:
    """Calculate realistic page score using 3-2-1 weighted scoring with controlled penalties"""
    import math
    
    # Step 1: DAMAGE CALCULATION (STRICT 3-2-1)
    damage = (high_issues * 3) + (medium_issues * 2) + (low_issues * 1)
    
    # Step 2: NORMALIZATION
    complexity = max(1, word_count / 300)
    normalized_damage = damage / complexity
    
    # Step 3: CONTROLLED PENALTY (diminishing scaling to prevent collapse)
    penalty = math.log1p(normalized_damage) * 15
    
    # Step 4: FINAL SCORE
    score = 100 - penalty
    
    # Step 5: SAFE CLAMP
    final_score = max(25, min(100, score))
    
    return round(final_score, 2)

def page_grade(score: float) -> str:
    """Convert score to letter grade like SEMrush"""
    if score >= 90: return "A"
    if score >= 75: return "B"
    if score >= 60: return "C"
    if score >= 40: return "D"
    return "F"

def website_grade(score: float) -> str:
    """Convert website score to letter grade like SEMrush"""
    if score >= 90: return "A"
    if score >= 75: return "B"
    if score >= 60: return "C"
    if score >= 40: return "D"
    return "F"

def detect_project_field_from_page_scores(db) -> str:
    """Detect the project identifier field already used in seo_page_scores.

    F4-016: PROJECT_SEO_AGGREGATION reads/writes only seo_page_scores and
    seoprojects — it has no need for the full schema discovery
    execute_seo_scoring_logic runs against seo_page_data/seo_page_issues
    (which may not even be freshly populated at aggregation time for a
    Verification Batch). Whatever field name SEO_SCORING already wrote each
    score_doc's project identifier under is sampled directly instead.
    """
    possible_fields = ["projectId", "project_id", "seo_project_id", "projectid"]
    sample = db.seo_page_scores.find_one()
    if not sample:
        return None
    for field in possible_fields:
        if field in sample:
            return field
    return None

def compute_and_persist_website_score(db, project_field: str, project_object_id, job_id: str) -> float:
    """Website-level SEO score aggregation (SEMrush-style).

    Extracted verbatim from execute_seo_scoring_logic's own former Phase 7
    (F4-016) so it can be invoked exactly once per Verification Batch via
    PROJECT_SEO_AGGREGATION, instead of once per page. Reads ALL
    seo_page_scores for the project — deliberately not scoped to job_id, for
    the same reason project_aggregator.aggregate_project reads all
    ai_scores: a job_id-scoped read would only reflect whichever job most
    recently touched each page, not the whole project's current state.
    """
    page_scores = list(
        db.seo_page_scores.find(
            {project_field: project_object_id},
            {"page_score": 1}
        )
    )

    if not page_scores:
        print(f"[WEBSITE] No page scores found for website calculation | jobId={job_id}")
        return 0

    website_score = round(
        sum(p["page_score"] for p in page_scores) / len(page_scores),
        2
    )
    website_grade_letter = website_grade(website_score)

    print(f"[WEBSITE] Website score calculated | score={website_score} | grade={website_grade_letter} | jobId={job_id}")

    project_check = db.seoprojects.find_one({"_id": project_object_id})
    if not project_check:
        print(f"[ERROR] Project NOT found | project_object_id={project_object_id}")

    update_result = db.seoprojects.update_one(
        {"_id": project_object_id},
        {
            "$set": {
                "website_score": round(website_score, 2),
                "website_grade": website_grade_letter,
                "pages_scored": len(page_scores),
                "last_scored_at": datetime.utcnow(),
                "scoring_version": "1.1"
            }
        }
    )

    print(f"[PROJECT] Website metrics write result | matched={update_result.matched_count} | modified={update_result.modified_count} | jobId={job_id}")

    if update_result.matched_count == 0:
        print(f"[ERROR] No project document found for update | project_object_id={project_object_id}")
    elif update_result.modified_count == 0:
        print(f"[WARN] Project found but not modified (values may be the same)")
    else:
        print(f"[SUCCESS] Project updated with website metrics | jobId={job_id}")

    return website_score

def execute_project_seo_aggregation_logic(job):
    """PROJECT_SEO_AGGREGATION (F4-016) — website-level SEO scoring, run
    exactly once per Verification Batch by chainingEngine's barrier, instead
    of once per URL. Reuses compute_and_persist_website_score unchanged; the
    only new logic here is locating the project-field name already used in
    seo_page_scores and reporting completion/failure back to Node.
    """
    try:
        print(f"[WORKER] PROJECT_SEO_AGGREGATION started | jobId={job.jobId} | projectId={job.projectId}")

        if not ObjectId.is_valid(job.projectId):
            raise ValueError(f"Invalid projectId: {job.projectId}")
        project_object_id = ObjectId(job.projectId)

        db = get_db_connection()

        project_field = detect_project_field_from_page_scores(db)
        if not project_field:
            error_msg = f"No seo_page_scores found for project {job.projectId} — cannot aggregate"
            print(f"[ERROR] {error_msg}")
            send_failure_callback(job.jobId, error_msg)
            return

        website_score = compute_and_persist_website_score(db, project_field, project_object_id, job.jobId)

        stats = {"websiteScore": round(website_score, 2)}
        send_completion_callback(job.jobId, stats)
        print(f"[WORKER] PROJECT_SEO_AGGREGATION completed | jobId={job.jobId} | website_score={website_score:.2f}")

    except Exception as e:
        error_msg = f"Project SEO aggregation failed: {str(e)}"
        print(f"[ERROR] {error_msg}")
        import traceback
        traceback.print_exc()
        send_failure_callback(job.jobId, error_msg)

def execute_seo_scoring_logic(job):
    """Execute SEO scoring job logic with schema discovery"""
    try:
        print(f"[WORKER] SEO_SCORING started | jobId={job.jobId}")
        
        # Check for cancellation
        if is_job_cancelled(job.jobId):
            print(f"[WORKER] SEO_SCORING cancelled | jobId={job.jobId}")
            return
        
        # SAFETY CHECK: Validate and convert projectId to ObjectId
        if not ObjectId.is_valid(job.projectId):
            raise ValueError(f"Invalid projectId: {job.projectId}")
        
        project_object_id = ObjectId(job.projectId)
        
        db = get_db_connection()
        
        # PHASE 1: SCHEMA DISCOVERY
        print(f"[SCHEMA] Starting schema discovery...")
        
        schemas = {}
        for collection_name in ["seo_page_data", "seo_page_issues", "seo_page_performance"]:
            schemas[collection_name] = discover_collection_schema(db, collection_name)
        
        # PHASE 2: BUILD CANONICAL FIELD MAPPING
        field_mapping = build_canonical_field_mapping(schemas)
        
        if not field_mapping:
            error_msg = "Schema discovery failed - cannot proceed with scoring"
            print(f"[ERROR] {error_msg}")
            send_failure_callback(job.jobId, error_msg)
            return
        
        # PHASE 3: DATA COLLECTION
        project_field = field_mapping["project"]
        
        # Check total pages for project first
        total_pages_count = db.seo_page_data.count_documents({project_field: project_object_id})
        
        # Use ONLY fields that definitely exist - minimal filtering for now
        page_query = {
            project_field: project_object_id,
            "scrape_status": "SUCCESS"   # ✅ correct field - minimal filtering
        }

        # P2-002: optional URL-scope filter. Applied here (not appended to
        # total_pages_count above, which is a separate, already-unused
        # diagnostic count of every page in the project regardless of scope
        # or scrape_status — left untouched, out of scope for "page
        # selection"). Uses field_mapping["url_data"] — the SAME
        # schema-discovered field name the rest of this function already
        # uses to identify a page's URL (see url_data_field below) — not a
        # hardcoded "url" key, so this stays consistent with this worker's
        # existing schema-adaptive design. Empty/absent list is treated as
        # "no filter", matching every sibling worker's convention.
        url_filter = getattr(job, 'urls', None)
        if url_filter:
            page_query[field_mapping["url_data"]] = {"$in": url_filter}

        # Check filtered pages count
        filtered_pages_count = db.seo_page_data.count_documents(page_query)
        
        pages = list(db.seo_page_data.find(page_query))
        issues = list(db.seo_page_issues.find({project_field: project_object_id}))
        
        print(f"[INFO] Found {len(pages)} pages and {len(issues)} issues | jobId={job.jobId}")
        
        if len(pages) == 0:
            error_msg = f"No pages found for project {job.projectId} in seo_page_data after filtering"
            print(f"[ERROR] {error_msg}")
            send_failure_callback(job.jobId, error_msg)
            return
        
        # PHASE 4: PAGE-WISE ISSUE GROUPING
        url_issues_field = field_mapping["url_issues"]
        issues_by_url = {}
        
        for issue in issues:
            page_url = issue.get(url_issues_field)
            if page_url:
                if page_url not in issues_by_url:
                    issues_by_url[page_url] = []
                issues_by_url[page_url].append(issue)
        
        print(f"[INFO] Grouped issues for {len(issues_by_url)} pages | jobId={job.jobId}")
        
        # PHASE 5: SCORING
        page_scores = []
        total_score = 0
        pages_scored = 0
        url_data_field = field_mapping["url_data"]
        
        for page in pages:
            # Check cancellation periodically
            if is_job_cancelled(job.jobId):
                print(f"[WORKER] SEO_SCORING cancelled during scoring | jobId={job.jobId}")
                return
            
            page_url = page.get(url_data_field)
            if not page_url:
                continue
            
            page_issues = issues_by_url.get(page_url, [])
            
            # Count issues by severity
            high_issues = len([i for i in page_issues if normalize_severity(i.get(field_mapping["severity"])) == "high"])
            medium_issues = len([i for i in page_issues if normalize_severity(i.get(field_mapping["severity"])) == "medium"])
            low_issues = len([i for i in page_issues if normalize_severity(i.get(field_mapping["severity"])) == "low"])
            
            # Get word count from page data (normalize content size)
            word_count = page.get("content", {}).get("word_count", 300)
            
            # Calculate realistic page score
            page_score = calculate_page_score(high_issues, medium_issues, low_issues, word_count)
            page_grade_letter = page_grade(page_score)
            
            # Create score document
            score_doc = {
                project_field: project_object_id,
                "page_url": page_url,  # ✅ Use page_url to match unique index
                "page_score": page_score,
                "page_grade": page_grade_letter,  # ✅ Add SEMrush-style grade
                "issues_count": len(page_issues),
                "high_issues_count": high_issues,  # ✅ Use pre-calculated
                "medium_issues_count": medium_issues,  # ✅ Use pre-calculated
                "low_issues_count": low_issues,  # ✅ Use pre-calculated
                "jobId": job.jobId,
                "scored_at": datetime.utcnow(),
                "sourceJobId": job.sourceJobId,
                "scoring_version": "1.1"  # ✅ Updated version
            }
            
            page_scores.append(score_doc)
            total_score += page_score
            pages_scored += 1
        
        # Calculate website score
        website_score = total_score / pages_scored if pages_scored > 0 else 0
        
        print(f"[INFO] Scored {pages_scored} pages, website score: {website_score:.2f} | jobId={job.jobId}")
        
        # PHASE 6: STORAGE
        if page_scores:
            # Bulk upsert to avoid duplicates using PyMongo UpdateOne class
            bulk_ops = []
            for score_doc in page_scores:
                # Defensive guard: ensure page_url exists
                if not score_doc.get("page_url"):
                    print(f"[ERROR] Missing page_url in score document | jobId={job.jobId}")
                    continue
                
                bulk_ops.append(
                    UpdateOne(
                        {
                            project_field: project_object_id,
                            "page_url": score_doc["page_url"]  # ✅ Use page_url to match unique index
                        },
                        {
                            "$set": score_doc
                        },
                        upsert=True
                    )
                )
            
            if bulk_ops:  # Only proceed if we have valid operations
                try:
                    result = db.seo_page_scores.bulk_write(bulk_ops, ordered=False)
                    print(f"[STORAGE] Bulk upsert result | matched={result.matched_count} | upserted={result.upserted_count} | jobId={job.jobId}")
                except BulkWriteError as e:
                    print(f"[WARN] Bulk write warning: {e.details}")
                    print(f"[STORAGE] Partial result | modified={e.details.get('nModified', 0)} | jobId={job.jobId}")
            else:
                print(f"[STORAGE] No valid score documents to store | jobId={job.jobId}")
        
        # PHASE 7: WEBSITE-LEVEL SCORING (SEMrush-style)
        #
        # F4-016: for a batched Verification run (job.batchId set), this
        # project-wide recomputation is deferred to the PROJECT_SEO_AGGREGATION
        # job — fired exactly once per batch by chainingEngine's barrier —
        # instead of running once per page here. compute_and_persist_website_score
        # is unchanged either way; only this call site decides whether THIS
        # job also runs it inline. Full Audit / legacy verification (no
        # batchId) are byte-identical to before.
        is_batched = bool(getattr(job, 'batchId', None))
        if pages_scored > 0 and not is_batched:
            print(f"[WEBSITE] Calculating website-level score | pages_scored={pages_scored} | jobId={job.jobId}")
            website_score = compute_and_persist_website_score(db, project_field, project_object_id, job.jobId)
        elif is_batched:
            website_score = 0
            print(f"[WEBSITE] Batched run — deferring website-level scoring to PROJECT_SEO_AGGREGATION | jobId={job.jobId}")
        else:
            website_score = 0
            print(f"[WEBSITE] No pages scored - skipping website calculation | jobId={job.jobId}")
        
        # Send completion callback
        stats = {
            "pagesScored": pages_scored,
            "websiteScore": round(website_score, 2),
            "totalIssues": len(issues),
            "pagesWithIssues": len(issues_by_url)
        }
        
        send_completion_callback(job.jobId, stats)
        print(f"[WORKER] SEO_SCORING completed | jobId={job.jobId} | pages_scored={pages_scored} | website_score={website_score:.2f}")
        
    except Exception as e:
        error_msg = f"SEO scoring failed: {str(e)}"
        print(f"[ERROR] {error_msg}")
        import traceback
        traceback.print_exc()
        send_failure_callback(job.jobId, error_msg)
