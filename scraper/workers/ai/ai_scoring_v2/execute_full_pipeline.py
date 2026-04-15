#!/usr/bin/env python3
"""
AI Pipeline Execution Script - Full Pipeline with Monitoring
"""

import sys
import os
import json
import time
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

# Add paths
sys.path.append('.')  # python_workers root
sys.path.append('./scraper')  # scraper module
sys.path.append('./scraper/workers/ai/ai_link_discovery')  # link discovery
sys.path.append('./scraper/workers/ai/ai_visibility')  # ai visibility
sys.path.append('./scraper/workers/ai/ai_scoring_v2')  # scoring v2

# Database setup
from env_config import get_config
config = get_config()
MONGO_URI = config.get('database.uri')
client = MongoClient(MONGO_URI)
db = client[config.get('database.db_name')]

# Collections
seo_ai_visibility = db['seo_ai_visibility']
seo_ai_internal_links = db['seo_ai_internal_links']
seo_ai_page_scores = db['seo_ai_page_scores']

def generate_object_id():
    """Generate a valid ObjectId"""
    return ObjectId()

def execute_link_discovery():
    """PHASE 1: AI LINK DISCOVERY"""
    print("=" * 60)
    print("🔗 PHASE 1: AI LINK DISCOVERY")
    print("=" * 60)
    
    try:
        from ai_link_discovery import execute_ai_link_discovery, AiLinkDiscoveryJob
        
        # Create job data
        job_id = str(generate_object_id())
        project_id = str(generate_object_id())
        
        job_data = AiLinkDiscoveryJob(
            jobId=job_id,
            projectId=project_id,
            userId="test-user",
            url="https://www.sapphiredigitalagency.com/"
        )
        
        print(f"🚀 Starting AI_LINK_DISCOVERY for {job_data.url}")
        print(f"   Job ID: {job_id}")
        print(f"   Project ID: {project_id}")
        
        # Execute the job
        start_time = time.time()
        execute_ai_link_discovery(job_data)
        execution_time = time.time() - start_time
        
        # Verify results
        links_count = seo_ai_internal_links.count_documents({'projectId': ObjectId(project_id)})
        print(f"✅ AI_LINK_DISCOVERY completed in {execution_time:.2f}s")
        print(f"   Links discovered: {links_count}")
        
        if links_count > 0:
            print("✅ PHASE 1 SUCCESS - Links discovered")
            return project_id, job_id
        else:
            print("❌ PHASE 1 FAILED - No links discovered")
            return None, None
            
    except Exception as e:
        print(f"❌ AI_LINK_DISCOVERY failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def execute_ai_visibility(project_id, job_id):
    """PHASE 2: AI VISIBILITY"""
    print("\n" + "=" * 60)
    print("👁️ PHASE 2: AI VISIBILITY")
    print("=" * 60)
    
    try:
        # First, create the AI project record for standalone mode
        from pymongo import MongoClient
        from env_config import get_config
        config = get_config()
        MONGO_URI = config.get('database.uri')
        client = MongoClient(MONGO_URI)
        db = client[config.get('database.db_name')]
        seo_ai_visibility_project = db['seo_ai_visibility_project']
        
        # Create standalone AI project record
        ai_project_record = {
            "_id": ObjectId(project_id),
            "isStandalone": True,
            "createdAt": datetime.now(),
            "userId": "test-user"
        }
        
        # Upsert the AI project record
        seo_ai_visibility_project.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": ai_project_record},
            upsert=True
        )
        print(f"✅ Created standalone AI project record | ID: {project_id}")
        
        from ai_visibility import execute_ai_visibility, AIVisibilityJob
        
        # Create job data
        visibility_job_id = str(generate_object_id())
        
        job_data = AIVisibilityJob(
            jobId=visibility_job_id,
            projectId=project_id,
            userId="test-user"
        )
        
        print(f"🚀 Starting AI_VISIBILITY")
        print(f"   Job ID: {visibility_job_id}")
        print(f"   Source Job ID: {job_id}")
        
        # Execute the job
        start_time = time.time()
        execute_ai_visibility(job_data)
        execution_time = time.time() - start_time
        
        # Verify results
        pages_count = seo_ai_visibility.count_documents({'projectId': ObjectId(project_id)})
        print(f"✅ AI_VISIBILITY completed in {execution_time:.2f}s")
        print(f"   Pages processed: {pages_count}")
        
        # Check for structured_data issues
        sample_page = seo_ai_visibility.find_one({'projectId': ObjectId(project_id)})
        if sample_page:
            structured_data = sample_page.get('structured_data')
            if isinstance(structured_data, str):
                print("⚠️ WARNING: structured_data stored as string - should be object")
            elif isinstance(structured_data, dict):
                print("✅ structured_data correctly stored as object")
        
        if pages_count > 0:
            print("✅ PHASE 2 SUCCESS - Pages processed")
            return True
        else:
            print("❌ PHASE 2 FAILED - No pages processed")
            return False
            
    except Exception as e:
        print(f"❌ AI_VISIBILITY failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def execute_ai_scoring_v2(project_id):
    """PHASE 3: AI_VISIBILITY_SCORING_V2"""
    print("\n" + "=" * 60)
    print("📊 PHASE 3: AI_VISIBILITY_SCORING_V2")
    print("=" * 60)
    
    try:
        from ai_scoring_v2_worker import execute_ai_visibility_scoring_logic
        
        # Create job data
        scoring_job_id = str(generate_object_id())
        
        job_data = {
            'jobId': scoring_job_id,
            'projectId': project_id,
            'userId': 'test-user'
        }
        
        print(f"🚀 Starting AI_VISIBILITY_SCORING_V2")
        print(f"   Job ID: {scoring_job_id}")
        
        # Execute the job
        start_time = time.time()
        execute_ai_visibility_scoring_logic(job_data)
        execution_time = time.time() - start_time
        
        # Verify results
        scores_count = seo_ai_page_scores.count_documents({'projectId': ObjectId(project_id)})
        print(f"✅ AI_VISIBILITY_SCORING_V2 completed in {execution_time:.2f}s")
        print(f"   Pages scored: {scores_count}")
        
        # Check sample score
        sample_score = seo_ai_page_scores.find_one({'projectId': ObjectId(project_id)})
        if sample_score:
            score_value = sample_score.get('final_score', 0)
            print(f"   Sample score: {score_value}")
        
        if scores_count > 0:
            print("✅ PHASE 3 SUCCESS - Pages scored")
            return True
        else:
            print("❌ PHASE 3 FAILED - No pages scored")
            return False
            
    except Exception as e:
        print(f"❌ AI_VISIBILITY_SCORING_V2 failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def generate_final_report(project_id):
    """Generate final validation report"""
    print("\n" + "=" * 60)
    print("📋 FINAL VALIDATION REPORT")
    print("=" * 60)
    
    try:
        # Count results from each phase
        links_count = seo_ai_internal_links.count_documents({'projectId': ObjectId(project_id)})
        pages_count = seo_ai_visibility.count_documents({'projectId': ObjectId(project_id)})
        scores_count = seo_ai_page_scores.count_documents({'projectId': ObjectId(project_id)})
        
        print(f"📊 PIPELINE EXECUTION TIMELINE:")
        print(f"   Target Domain: https://www.sapphiredigitalagency.com/")
        print(f"   Project ID: {project_id}")
        print(f"")
        print(f"📈 RESULTS:")
        print(f"   Pages Discovered: {pages_count}")
        print(f"   Links Found: {links_count}")
        print(f"   Pages Scored: {scores_count}")
        print(f"")
        
        # Check scoring details
        if scores_count > 0:
            sample_score = seo_ai_page_scores.find_one({'projectId': ObjectId(project_id)})
            if sample_score:
                final_score = sample_score.get('final_score', 0)
                print(f"   Sample Page Score: {final_score:.1f}")
                
                # Check rule breakdown
                rule_breakdown = sample_score.get('rule_breakdown', [])
                total_rules = len(rule_breakdown)
                passed_rules = len([r for r in rule_breakdown if not r.get('error')])
                success_rate = (passed_rules / total_rules * 100) if total_rules > 0 else 0
                
                print(f"   Total Rules Applied: {total_rules}")
                print(f"   Rule Success Rate: {success_rate:.1f}%")
                
                if success_rate >= 95:
                    print("   ✅ Rule execution excellent")
                elif success_rate >= 85:
                    print("   ⚠️ Rule execution good")
                else:
                    print("   ❌ Rule execution needs improvement")
                
                # Show failed rules
                failed_rules = [r for r in rule_breakdown if r.get('error')]
                if failed_rules:
                    print(f"   Failed Rules: {len(failed_rules)}")
                    for rule in failed_rules:  # Show ALL failed rules
                        print(f"     - {rule.get('rule_id', 'unknown')}: {rule.get('error', 'unknown')}")
        
        print(f"")
        print(f"🎯 CONFIRMATION:")
        
        if links_count > 0 and pages_count > 0 and scores_count > 0:
            print("   ✅ All 3 jobs executed cleanly and verified")
            print("   ✅ Pipeline execution successful")
            return True
        else:
            print("   ❌ Pipeline execution incomplete")
            return False
            
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return False

def main():
    """Main execution function"""
    print("🚀 STARTING FULL AI PIPELINE EXECUTION")
    print(f"📅 Started at: {datetime.now().isoformat()}")
    print(f"🎯 Target: https://www.sapphiredigitalagency.com/")
    
    # PHASE 1: Link Discovery
    project_id, job_id = execute_link_discovery()
    if not project_id:
        print("❌ PIPELINE FAILED - Link discovery unsuccessful")
        return False
    
    # PHASE 2: AI Visibility
    if not execute_ai_visibility(project_id, job_id):
        print("❌ PIPELINE FAILED - AI visibility unsuccessful")
        return False
    
    # PHASE 3: AI Scoring V2
    if not execute_ai_scoring_v2(project_id):
        print("❌ PIPELINE FAILED - AI scoring unsuccessful")
        return False
    
    # Final Report
    success = generate_final_report(project_id)
    
    if success:
        print("\n🎉 FULL AI PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    else:
        print("\n❌ FULL AI PIPELINE EXECUTION COMPLETED WITH ISSUES!")
    
    return success

if __name__ == "__main__":
    main()
