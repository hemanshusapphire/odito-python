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

client = MongoClient(MONGO_URI)

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

# collection for AI visibility analysis
seo_ai_visibility = db["seo_ai_visibility"]

# collection for AI visibility internal links (discovery)
seo_ai_internal_links = db["seo_ai_internal_links"]

# collection for AI visibility projects
seo_ai_visibility_project = db["seo_ai_visibility_project"]


# collection for AI visibility page scores
seo_ai_page_scores = db["seo_ai_page_scores"]

# collection for AI visibility issues (derived from rule_breakdown)
seo_ai_visibility_issues = db["seo_ai_visibility_issues"]

# collection for SEO page analysis summaries
seo_page_summary = db["seo_page_summary"]

# collection for domain-level technical data (robots.txt, sitemap.xml)
domain_technical_reports = db["domain_technical_reports"]

# collections for keyword research results
seo_keyword_research = db["seo_keyword_research"]
seo_keyword_opportunities = db["seo_keyword_opportunities"]

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

try:
    seo_page_issues.create_index([("projectId", 1)])
    print("✅ Created index on seo_page_issues (projectId)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Index on seo_page_issues already exists")
    else:
        print(f"⚠️ Failed to create index on seo_page_issues: {e}")

# Create unique index for AI visibility page scores (mirroring SEO structure)
try:
    # Drop old index if it exists to prevent conflicts
    try:
        seo_ai_page_scores.drop_index("unique_project_ai_page_score")
        print("🔄 Dropped old unique_project_ai_page_score index")
    except:
        pass  # Index doesn't exist, that's fine
    
    # Create new index with correct field name for consistency with issues collection
    seo_ai_page_scores.create_index(
        [("projectId", 1), ("page_url", 1)],  # 🔥 PHASE 4: Use page_url to match issues collection
        unique=True,
        name="unique_project_ai_page_score"
    )
    print("✅ Created unique index on seo_ai_page_scores (projectId, page_url)")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_ai_page_scores already exists")
    else:
        print(f"⚠️ Failed to create index on seo_ai_page_scores: {e}")


# Create indexes for AI page scores
try:
    seo_ai_page_scores.create_index([("projectId", 1), ("page_url", 1)], unique=True)  # 🔥 FIXED: Use page_url not url
    seo_ai_page_scores.create_index([("projectId", 1), ("overall_page_score", -1)])
    seo_ai_page_scores.create_index([("projectId", 1), ("blocking", 1)])
    seo_ai_page_scores.create_index([("ai_jobId", 1)])
    print("✅ Created indexes on seo_ai_page_scores")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Indexes on seo_ai_page_scores already exist")
    else:
        print(f"⚠️ Failed to create indexes on seo_ai_page_scores: {e}")

# ==================== AI PIPELINE UNIQUE INDEXES ====================
# CRITICAL: Prevent duplicate AI visibility records per (projectId, url)
try:
    seo_ai_visibility.create_index(
        [("projectId", 1), ("url", 1)],
        unique=True,
        name="unique_project_url_visibility"
    )
    print("✅ Created unique index on seo_ai_visibility (projectId, url)")
    
    # Check for existing duplicates
    duplicate_pipeline = [
        {"$group": {"_id": {"projectId": "$projectId", "url": "$url"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(seo_ai_visibility.aggregate(duplicate_pipeline))
    if duplicates:
        print(f"⚠️ WARNING: Found {len(duplicates)} duplicate AI visibility records")
        print("   Run manual cleanup if needed: seo_ai_visibility.deleteMany(duplicates)")
    else:
        print("✅ No duplicate AI visibility records found")
        
except Exception as e:
    if "already exists" in str(e):
        print("✅ Unique index on seo_ai_visibility already exists")
    else:
        print(f"⚠️ Failed to create index on seo_ai_visibility: {e}")

print("✅ AI Pipeline unique indexes creation completed")

# Create index for AI visibility issues (for efficient querying and cleanup)
try:
    seo_ai_visibility_issues.create_index([("projectId", 1), ("page_url", 1)])
    seo_ai_visibility_issues.create_index([("projectId", 1), ("severity", 1)])
    seo_ai_visibility_issues.create_index([("category", 1)])
    seo_ai_visibility_issues.create_index([("created_at", -1)])
    print("✅ Created indexes on seo_ai_visibility_issues")
except Exception as e:
    if "already exists" in str(e):
        print("✅ Indexes on seo_ai_visibility_issues already exist")
    else:
        print(f"⚠️ Failed to create indexes on seo_ai_visibility_issues: {e}")

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

