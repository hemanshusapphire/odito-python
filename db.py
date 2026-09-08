from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import os
from dotenv import load_dotenv
from env_config import get_config

# Load environment variables from .env file
load_dotenv()

# connect to MongoDB using same URI as backend
config = get_config()
MONGO_URI = config.get('database.uri')
print(f"[DB DEBUG] Mongo URI: {MONGO_URI}")

client = MongoClient(MONGO_URI, maxPoolSize=100, minPoolSize=10)

# Use database name from config (already properly extracted)
db_name = config.get('database.db_name')
print(f"[DB DEBUG] Database name from config: {db_name}")

# Fallback extraction only if config fails (should never happen)
if not db_name:
    print("[DB DEBUG] Config failed, extracting from URI as fallback")
    if "?" in MONGO_URI:
        uri_without_query = MONGO_URI.split("?")[0]
        if "/" in uri_without_query:
            path_parts = uri_without_query.split("/")
            if len(path_parts) > 3 and path_parts[3]:
                db_name = path_parts[3]
            else:
                db_name = "odito_dev"  # Fixed: Use dev default
        else:
            db_name = "odito_dev"
    else:
        db_name = "odito_dev"

db = client[db_name]

print(f"[DB DEBUG] Final database name: {db_name}")
print(f"[DB DEBUG] Database object: {db}")
print(f"🔗 Connected to MongoDB database: {db_name}")

# collections for link discovery results
seo_internal_links = db["seo_internal_links"]
seo_external_links = db["seo_external_links"]
seo_social_links = db["seo_social_links"]

# collection for screenshot metadata
seo_first_snapshot = db["seo_first_snapshot"]

# collection for main URL homepage snapshots
seo_mainurl_snapshot = db["seo_mainurl_snapshot"]

# collections for page scraping and analysis results
seo_page_data = db["seo_page_data"]

# collection for crawl graph analysis results
seo_crawl_graph = db["seo_crawl_graph"]
seo_page_issues = db["seo_page_issues"]

# collection for headless accessibility data
seo_headless_data = db["seo_headless_data"]

# collection for performance analysis results
seo_page_performance = db["seo_page_performance"]

# collection for SEO scoring results
seo_page_scores = db["seo_page_scores"]

# collection for projects (for website-level scoring)
seoprojects = db["seoprojects"]

# collection for jobs (for job status updates)
jobs = db["jobs"]

# collection for domain-level performance analysis
seo_domain_performance = db["seo_domain_performance"]

# collection for SEO page analysis summaries
seo_page_summary = db["seo_page_summary"]

# collection for domain-level technical data (robots.txt, sitemap.xml)
domain_technical_reports = db["domain_technical_reports"]

# collection for SEO ranking results (onboarding)
seo_rankings = db["seo_rankings"]

# Create unique index to prevent duplicate performance records
# Ensures one record per (projectId, page_url, device_type)
try:
    seo_page_performance.create_index(
        [("projectId", 1), ("page_url", 1), ("device_type", 1)],
        unique=True,
        name="unique_project_page_device"
    )
    print("✅ Created unique index on seo_page_performance (projectId, page_url, device_type)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_page_performance already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_performance: {e}")

# Create unique index to prevent duplicate scoring records
# Ensures one record per (projectId, page_url)
try:
    seo_page_scores.create_index(
        [("projectId", 1), ("page_url", 1)],
        unique=True,
        name="unique_project_page_score"
    )
    print("✅ Created unique index on seo_page_scores (projectId, page_url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_page_scores already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_scores: {e}")

# Create indexes for export performance - critical for PDF export aggregations
try:
    seo_page_data.create_index([("projectId", 1)])
    print("✅ Created index on seo_page_data (projectId)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_data already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_data: {e}")

# Index for page_type filtering — enables direct queries like
#   seo_page_data.find({ projectId, page_type: "Service" })
try:
    seo_page_data.create_index(
        [("projectId", 1), ("page_type", 1)],
        name="project_page_type"
    )
    print("✅ Created index on seo_page_data (projectId, page_type)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_data (page_type) already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_data (page_type): {e}")

# Unique index (projectId, url) — the sole guard against duplicate SUCCESS
# documents when a whole PAGE_SCRAPING chunk job is retried after a partial
# crash. resetProjectCrawlData() already clears every seo_page_data document
# for a project at the start of each new audit run, so at any moment all
# documents for one projectId belong to the current run only — no run_id
# field is needed here for this constraint to be correct. page_scraping.py's
# insert_many is the only writer to this collection in the whole codebase
# and is paired with duplicate-key-tolerant error handling for this index.
try:
    seo_page_data.create_index(
        [("projectId", 1), ("url", 1)],
        unique=True,
        name="unique_project_url"
    )
    print("✅ Created unique index on seo_page_data (projectId, url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_page_data (projectId, url) already exists")
    else:
        print(f"⚠️ Failed to create unique index on seo_page_data (projectId, url): {e}")

try:
    seo_page_issues.create_index([("projectId", 1)])
    print("✅ Created index on seo_page_issues (projectId)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_issues already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_issues: {e}")

# Phase 3 hardening: odito_backend's TaskVerificationService queries
# {projectId, status:'open'} on every single verification pass (project
# recrawl or single-URL re-verification) to determine which Optimization
# Center tasks are still unresolved — the projectId-only index above leaves
# the status filter unindexed. This compound index covers that exact query
# shape (and remains a valid prefix-match for the projectId-only case too).
try:
    seo_page_issues.create_index([("projectId", 1), ("status", 1)], name="project_status")
    print("✅ Created index on seo_page_issues (projectId, status)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_issues (projectId, status) already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_issues (projectId, status): {e}")

# Unique index on dedup_key (P0-005) — converts seo_page_issues from
# insert-only to upsert-capable. dedup_key (P0-003: sha256 of
# project|url|issue_code|data_path, stamped by both issue factories and
# backfilled by P0-004) is the issue's identity; this index enforces one
# document per identity and guards the upsert write path in
# page_analysis.upsert_issues against concurrent-insert races.
#
# PRECONDITION: 100% dedup_key coverage + zero duplicate identities
# (P0-004 backfill + duplicate resolution must have run). If a duplicate
# still exists, this build FAILS — the loud error below is intentional and
# means the P0-004 dry-run report must be re-run and resolved. The upsert
# write path remains functionally correct without the index (filter-based),
# so a failed build here degrades race-safety/speed, not correctness.
try:
    seo_page_issues.create_index(
        [("dedup_key", 1)],
        unique=True,
        name="unique_dedup_key"
    )
    print("✅ Created unique index on seo_page_issues (dedup_key)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_page_issues (dedup_key) already exists")
    else:
        print(f"⚠️ Failed to create unique index on seo_page_issues (dedup_key): {e}")
        print("⚠️ ACTION REQUIRED: run `python -m scripts.backfill_seo_page_issues_lifecycle` (dry run) and resolve any missing/duplicate dedup_keys before this index can build.")

# Create unique index for domain technical reports (one per project)
try:
    domain_technical_reports.create_index(
        [("projectId", 1)],
        unique=True,
        name="unique_project_domain_report"
    )
    print("✅ Created unique index on domain_technical_reports (projectId)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on domain_technical_reports already exists")
    else:
        print(f"⚠️ Failed to create index on domain_technical_reports: {e}")

# Create unique index for headless accessibility data (one per project + URL)
try:
    seo_headless_data.create_index(
        [("projectId", 1), ("url", 1)],
        unique=True,
        name="unique_project_url_accessibility"
    )
    print("✅ Created unique index on seo_headless_data (projectId, url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_headless_data already exists")
    else:
        print(f"⚠️ Failed to create index on seo_headless_data: {e}")

# Create unique index for domain performance data (one per project)
try:
    seo_domain_performance.create_index(
        [("project_id", 1)],
        unique=True,
        name="unique_project_domain_performance"
    )
    print("✅ Created unique index on seo_domain_performance (project_id)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_domain_performance already exists")
    else:
        print(f"⚠️ Failed to create index on seo_domain_performance: {e}")

# Note: Python workers do NOT create projects or jobs
# They only write link discovery results to the collections above

# Create unique index for crawl graph data (one per project + URL)
try:
    seo_crawl_graph.create_index(
        [("projectId", 1), ("url", 1)],
        unique=True,
        name="unique_project_url_crawl_graph"
    )
    print("✅ Created unique index on seo_crawl_graph (projectId, url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_crawl_graph already exists")
    else:
        print(f"⚠️ Failed to create index on seo_crawl_graph: {e}")

# Create index for SEO page summaries (for efficient querying)
try:
    seo_page_summary.create_index(
        [("projectId", 1), ("seo_jobId", 1), ("page_url", 1)],
        name="project_job_page_summary"
    )
    print("✅ Created index on seo_page_summary (projectId, seo_jobId, page_url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_summary already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_summary: {e}")

# ── URL Qualification audit trail ─────────────────────────────────────────────
# Stores one probe record per URL per job run. Used for diagnostics and to
# understand qualification decisions (qualified / low_priority / rejected).
# Retention: TTL 30 days — audit trail only, not needed long-term.
# Storage estimate @ 100k audits/month × avg 50 URLs/audit = 5M docs/month;
# each doc ~300 B → ~1.5 GB/month raw, ~50 MB after 30-day TTL expiry.
seo_audit_url_pool = db["seo_audit_url_pool"]

try:
    # Lookup: find all probe results for a specific job
    seo_audit_url_pool.create_index(
        [("job_id", 1)],
        name="job_id_lookup"
    )
    # Lookup: find all audits that probed a specific URL for a project
    seo_audit_url_pool.create_index(
        [("project_id", 1), ("url", 1)],
        name="project_url_lookup"
    )
    # TTL: automatically delete records older than 30 days
    seo_audit_url_pool.create_index(
        [("probed_at", 1)],
        expireAfterSeconds=30 * 24 * 60 * 60,  # 30 days
        name="ttl_30d"
    )
    print("✅ Created indexes on seo_audit_url_pool")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Indexes on seo_audit_url_pool already exist")
    else:
        print(f"⚠️ Failed to create indexes on seo_audit_url_pool: {e}")

# Persistent PAGE_SCRAPING failure tracking — one document per URL that
# scrape_single_url() could not turn into a seo_page_data record (timeout,
# DNS/SSL/connection error, non-200 status, empty response, extraction
# failure, render failure, or an unexpected exception). Purely additive
# observability: nothing reads from this collection to make a pipeline
# decision, and nothing existing writes to it except the new failure-path
# code in page_scraping.py.
seo_page_failures = db["seo_page_failures"]

try:
    # Lookup: every failure for one audit run (the primary reporting query)
    seo_page_failures.create_index(
        [("project_id", 1), ("run_id", 1)],
        name="project_run_lookup"
    )
    # Breakdown query: count by failure_type within a run
    seo_page_failures.create_index(
        [("project_id", 1), ("run_id", 1), ("failure_type", 1)],
        name="project_run_failure_type_lookup"
    )
    # Paginated/sorted report query (GET /projects/:id/page-failures): lets
    # Mongo satisfy the {project_id, run_id} filter AND the failed_at sort
    # entirely from the index — no in-memory sort stage — at the 10k+
    # failures-in-one-run scale the reporting API must stay performant at.
    seo_page_failures.create_index(
        [("project_id", 1), ("run_id", 1), ("failed_at", -1)],
        name="project_run_recent_lookup"
    )
    # Per-URL forensic lookup across runs (mirrors seo_audit_url_pool's
    # project_url_lookup index) — "has this URL ever failed, and how".
    seo_page_failures.create_index(
        [("project_id", 1), ("url", 1)],
        name="project_url_lookup"
    )
    # TTL: automatically delete records older than 90 days — audit trail
    # only, longer retention than seo_audit_url_pool since these are rarer
    # and more valuable for forensic investigation.
    seo_page_failures.create_index(
        [("failed_at", 1)],
        expireAfterSeconds=90 * 24 * 60 * 60,  # 90 days
        name="ttl_90d"
    )
    print("✅ Created indexes on seo_page_failures")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Indexes on seo_page_failures already exist")
    else:
        print(f"⚠️ Failed to create indexes on seo_page_failures: {e}")

# ── AI Intelligence V2 collections ──────────────────────────────────────────
ai_pages     = db["ai_pages"]
ai_scores    = db["ai_scores"]
ai_issues    = db["ai_issues"]
ai_projects  = db["ai_projects"]

try:
    # ai_pages: one extraction doc per (project, job, url)
    ai_pages.create_index([("project_id", 1), ("job_id", 1), ("url", 1)], unique=True, name="v2_unique_page")
    ai_pages.create_index([("project_id", 1)], name="v2_pages_by_project")
    ai_pages.create_index([("job_id", 1)],     name="v2_pages_by_job")
    print("✅ Created indexes on ai_pages")
except Exception as e:
    print("✅ ai_pages indexes exist" if "already exists" in str(e) else f"⚠️ ai_pages index error: {e}")

try:
    # ai_scores: one score doc per (project, url). job_id is kept as a plain
    # field (provenance — "which job last scored this page") but is NOT part
    # of the identity, so a future single-URL verification run updates the
    # same page's score doc in place instead of creating a duplicate keyed
    # by its own new job_id. Previously unique on (project_id, job_id, url).
    try:
        ai_scores.create_index([("project_id", 1), ("url", 1)], unique=True, name="v2_unique_score")
    except Exception as key_conflict:
        # Old index (project_id, job_id, url) still holds this name — replace it.
        if "already exists" not in str(key_conflict):
            ai_scores.drop_index("v2_unique_score")
            ai_scores.create_index([("project_id", 1), ("url", 1)], unique=True, name="v2_unique_score")
    ai_scores.create_index([("page_id", 1)],   name="v2_score_by_page")
    ai_scores.create_index([("job_id", 1)],    name="v2_scores_by_job")
    print("✅ Created indexes on ai_scores")
except Exception as e:
    print("✅ ai_scores indexes exist" if "already exists" in str(e) else f"⚠️ ai_scores index error: {e}")

try:
    # ai_issues: one doc per failed rule per page — supports severity + hub/card drill-down
    ai_issues.create_index([("project_id", 1), ("severity", 1)],        name="v2_issues_by_severity")
    ai_issues.create_index([("project_id", 1), ("url", 1)],             name="v2_issues_by_url")
    ai_issues.create_index([("project_id", 1), ("hub", 1), ("card", 1)], name="v2_issues_by_card")
    ai_issues.create_index([("score_id", 1)],                            name="v2_issues_by_score")
    ai_issues.create_index([("job_id", 1)],                              name="v2_issues_by_job")
    ai_issues.create_index([("created_at", 1)], expireAfterSeconds=90 * 24 * 60 * 60, name="v2_issues_ttl_90d")
    print("✅ Created indexes on ai_issues")
except Exception as e:
    print("✅ ai_issues indexes exist" if "already exists" in str(e) else f"⚠️ ai_issues index error: {e}")

try:
    # ai_projects: one CURRENT aggregate per project. job_id is kept as a
    # plain field (provenance — "which job last computed this") but is NOT
    # part of the identity: every Node-side reader (aiHubController,
    # AuditHistoryService, AiHubSnapshotService, V2IssueExtractor,
    # contextExtractor) already queries by project_id alone, sorted by
    # computed_at desc, taking the latest doc — never by job_id. Keeping
    # job_id in the unique index let a single-URL verification run's own
    # job_id create a brand-new "latest" doc that silently replaced the
    # project-wide aggregate. Previously unique on (project_id, job_id).
    try:
        ai_projects.create_index([("project_id", 1)], unique=True, name="v2_unique_project_job")
    except Exception as key_conflict:
        if "already exists" not in str(key_conflict):
            ai_projects.drop_index("v2_unique_project_job")
            ai_projects.create_index([("project_id", 1)], unique=True, name="v2_unique_project_job")
    ai_projects.create_index([("project_id", 1), ("computed_at", -1)], name="v2_project_latest")
    print("✅ Created indexes on ai_projects")
except Exception as e:
    print("✅ ai_projects indexes exist" if "already exists" in str(e) else f"⚠️ ai_projects index error: {e}")

