"""Screenshot capture functionality with duplicate detection."""

# 🚨 SCREENSHOT FUNCTIONALITY DISABLED FOR PERFORMANCE
SCREENSHOT_FUNCTIONALITY_DISABLED = True

import os
import hashlib
import threading
from datetime import datetime
from typing import Optional, Tuple
from pathlib import Path

# Optional Selenium import
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Local imports
from config.config import USER_AGENTS
from db import seo_first_snapshot, seo_mainurl_snapshot
from bson.objectid import ObjectId

# File extensions to skip for screenshots
SKIP_EXTENSIONS = (".xml", ".pdf", ".zip", ".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".ico", ".svg", ".webp")

# 🔑 ABSOLUTE base path - where Express serves from (production-safe)
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # python_workers → shared → scraper → python_workers → Odito
BACKEND_STORAGE = PROJECT_ROOT / "odito_backend" / "storage" / "screenshots"

# 🔍 DEBUG: Print runtime identity
print(f"� Screenshot storage root: {BACKEND_STORAGE}")

# Global registry for job-scoped screenshot tracking
SCREENSHOT_REGISTRY = {}
SCREENSHOT_LOCK = threading.Lock()

def clear_screenshot_registry():
    """Clear the screenshot registry at the start of each PAGE_SCRAPING job."""
    if SCREENSHOT_FUNCTIONALITY_DISABLED:
        return
    global SCREENSHOT_REGISTRY
    with SCREENSHOT_LOCK:
        SCREENSHOT_REGISTRY = {}

def should_skip_url(url: str) -> bool:
    """Check if URL should be skipped based on file extension."""
    # Extract the path part of the URL
    from urllib.parse import urlparse
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    # Skip if path ends with any of the skip extensions
    return any(path.endswith(ext) for ext in SKIP_EXTENSIONS)

def store_screenshot_metadata(job_id: str, project_id: str, requested_url: str, final_url: str, 
                            canonical_url: Optional[str], dom_hash: str, scroll_height: int, 
                            screenshot_path: str):
    """Store screenshot metadata in seo_first_snapshot collection."""
    try:
        # Convert absolute path to relative path for static serving (same as homepage)
        relative_path = os.path.relpath(screenshot_path, PROJECT_ROOT / "odito_backend")
        # Normalize path separators to forward slashes for URLs
        relative_path = relative_path.replace(os.sep, '/')
        
        # Store in seo_first_snapshot collection
        seo_first_snapshot.insert_one({
            "project_id": ObjectId(project_id),
            "job_id": ObjectId(job_id),
            "url": requested_url,
            "final_url": final_url,
            "canonical_url": canonical_url,
            "dom_hash": dom_hash,
            "scroll_height": scroll_height,
            "screenshot_path": relative_path,
            "status": "captured",
        })
    except Exception as e:
        print(f"⚠️ Failed to store screenshot metadata: {str(e)}")
        # Don't raise - metadata storage is best-effort

def get_canonical_url(driver) -> Optional[str]:
    """Extract canonical URL from page if present."""
    try:
        canonical_link = driver.find_element("css selector", 'link[rel="canonical"]')
        return canonical_link.get_attribute("href")
    except Exception:
        return None

def compute_dom_fingerprint(driver) -> Tuple[str, int]:
    """Compute DOM fingerprint and scroll height for duplicate detection."""
    try:
        # Wait a moment for page to render
        import time
        time.sleep(0.5)
        
        # Get DOM content for hashing
        dom_content = driver.execute_script("return document.body ? document.body.innerHTML.substring(0, 5000) : ''")
        if not dom_content:
            dom_content = ""
        dom_hash = hashlib.md5(dom_content.encode('utf-8')).hexdigest()
        
        # Get scroll height
        scroll_height = driver.execute_script("return document.body ? document.body.scrollHeight : 0")
        
        return dom_hash, scroll_height
    except Exception as e:
        print(f"⚠️ DOM fingerprinting failed: {str(e)}")
        # Return unique values to avoid false duplicates
        import uuid
        return str(uuid.uuid4()), 1

def is_duplicate_page(dom_hash: str, canonical: Optional[str], final_url: str) -> Optional[str]:
    """Check if page is duplicate using registry and return existing screenshot path."""
    with SCREENSHOT_LOCK:
        for existing_hash, page_data in SCREENSHOT_REGISTRY.items():
            # Check for exact match on DOM hash (primary indicator)
            if existing_hash == dom_hash:
                return page_data.get("screenshot_path")
            
            # Check for exact final URL match (redirect handling)
            if page_data.get("final_url") == final_url:
                return page_data.get("screenshot_path")
            
            # Check for exact canonical URL match (if both have canonical)
            if canonical and page_data.get("canonical") == canonical:
                return page_data.get("screenshot_path")
    return None

def capture_full_page_screenshot(driver, output_path: str) -> Optional[str]:
    """Capture full-page screenshot and save to disk."""
    try:
        # Wait for page to be fully loaded
        driver.execute_script("return document.readyState === 'complete'")
        
        # Get page dimensions with fallbacks
        try:
            total_width = driver.execute_script("return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth, document.body.offsetWidth, document.documentElement.offsetWidth, document.documentElement.clientWidth)")
            total_height = driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, document.body.offsetHeight, document.documentElement.offsetHeight, document.documentElement.clientHeight)")
            
            # Ensure minimum dimensions
            total_width = max(total_width, 1024)
            total_height = max(total_height, 768)
            
        except Exception:
            # Fallback to standard viewport size
            total_width = 1920
            total_height = 1080
        
        # Set viewport to full page size
        driver.set_window_size(total_width, total_height)
        
        # Wait a moment for resize to take effect
        import time
        time.sleep(0.2)
        
        # Capture screenshot
        driver.save_screenshot(output_path)
        
        # Verify screenshot was created
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return output_path
        else:
            print(f"⚠️ Screenshot file not created or empty: {output_path}")
            return None
        
    except Exception as e:
        print(f"⚠️ Screenshot capture failed: {str(e)}")
        return None

def take_page_screenshot(url: str, job_id: str = None, project_id: str = None) -> Optional[str]:
    """
    Take a full-page screenshot for a URL with duplicate detection.
    Returns screenshot path or None if duplicate/failed/skipped.
    """
    if SCREENSHOT_FUNCTIONALITY_DISABLED:
        print(f"📸 Screenshot DISABLED for {url}")
        return None
    # Skip non-HTML resources early
    if should_skip_url(url):
        print(f"📸 Screenshot decision for {url}")
        print(f"  Action: SKIPPED_NON_HTML (file extension)")
        return None
    
    if not SELENIUM_AVAILABLE:
        print("⚠️ Selenium not available for screenshots")
        return None
    
    driver = None
    try:
        # Setup Chrome options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={USER_AGENTS[0]}")
        
        # Create driver instance
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        
        # Navigate to URL
        driver.get(url)
        
        # Get final URL after redirects
        final_url = driver.current_url
        
        # Extract canonical URL
        canonical_url = get_canonical_url(driver)
        
        # Compute DOM fingerprint
        dom_hash, scroll_height = compute_dom_fingerprint(driver)
        
        # Check for duplicates
        existing_screenshot = is_duplicate_page(dom_hash, canonical_url, final_url)
        if existing_screenshot:
            print(f"📸 Screenshot decision for {url}")
            print(f"  Requested URL: {url}")
            print(f"  Final URL: {final_url}")
            print(f"  Canonical: {canonical_url}")
            print(f"  DOM hash: {dom_hash}")
            print(f"  Scroll height: {scroll_height}")
            print(f"  Action: SKIPPED_DUPLICATE")
            
            # Store metadata for duplicate screenshot if job info provided
            if job_id and project_id:
                store_screenshot_metadata(job_id, project_id, url, final_url, canonical_url, 
                                        dom_hash, scroll_height, existing_screenshot)
            
            return existing_screenshot
        
        # Generate screenshot filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"screenshot_{dom_hash[:8]}_{timestamp}.png"
        
        # Ensure internal pages screenshots directory exists
        internal_dir = BACKEND_STORAGE / "internalpages"
        internal_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = str(internal_dir / filename)
        
        # Capture full-page screenshot (no deduplication for homepage)
        captured_path = capture_full_page_screenshot(driver, screenshot_path)
        
        if captured_path:
            # Convert absolute path to relative path for static serving (same as homepage)
            relative_path = os.path.relpath(captured_path, PROJECT_ROOT / "odito_backend")
            # Normalize path separators to forward slashes for URLs
            relative_path = relative_path.replace(os.sep, '/')
            
            # Register in global registry
            with SCREENSHOT_LOCK:
                SCREENSHOT_REGISTRY[dom_hash] = {
                    "final_url": final_url,
                    "canonical": canonical_url,
                    "scroll_height": scroll_height,
                    "screenshot_path": relative_path
                }
            
            # Store metadata in database if job info provided
            if job_id and project_id:
                store_screenshot_metadata(job_id, project_id, url, final_url, canonical_url, 
                                        dom_hash, scroll_height, captured_path)
            
            print(f"📸 Screenshot decision for {url}")
            print(f"  Requested URL: {url}")
            print(f"  Final URL: {final_url}")
            print(f"  Canonical: {canonical_url}")
            print(f"  DOM hash: {dom_hash}")
            print(f"  Scroll height: {scroll_height}")
            print(f"  Action: CAPTURED")
            
            return captured_path
        else:
            print(f"📸 Screenshot decision for {url}")
            print(f"  Requested URL: {url}")
            print(f"  Final URL: {final_url}")
            print(f"  Canonical: {canonical_url}")
            print(f"  DOM hash: {dom_hash}")
            print(f"  Scroll height: {scroll_height}")
            print(f"  Action: SKIPPED_FAILED")
            return None
            
    except Exception as e:
        print(f"⚠️ Screenshot process failed for {url}: {str(e)}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

def capture_homepage_screenshot(url: str, job_id: str, project_id: str) -> dict:
    """
    Capture homepage screenshot for LINK_DISCOVERY job.
    Stores result in seo_mainurl_snapshot collection.
    Failure-safe: continues link discovery even if screenshot fails.
    
    Returns dict with status and metadata for logging.
    """
    if SCREENSHOT_FUNCTIONALITY_DISABLED:
        print(f"📸 Homepage screenshot DISABLED for {url}")
        return {
            "status": "disabled",
            "error": "Screenshot functionality disabled"
        }
    # Skip non-HTML resources early
    if should_skip_url(url):
        return {
            "status": "skipped",
            "error": "Non-HTML resource (file extension)"
        }
    
    if not SELENIUM_AVAILABLE:
        return {
            "status": "failed", 
            "error": "Selenium not available"
        }
    
    driver = None
    try:
        # Setup Chrome options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={USER_AGENTS[0]}")
        
        # Create driver instance
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        
        # Navigate to URL
        driver.get(url)
        
        # Get final URL after redirects
        final_url = driver.current_url
        
        # Extract canonical URL
        canonical_url = get_canonical_url(driver)
        
        # Compute DOM fingerprint
        dom_hash, scroll_height = compute_dom_fingerprint(driver)
        
        # Generate screenshot filename (unique for homepage snapshots)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"homepage_{job_id[:8]}_{timestamp}.png"
        
        # Ensure homepage screenshots directory exists
        homepage_dir = BACKEND_STORAGE / "homepage"
        homepage_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = str(homepage_dir / filename)
        
        # Capture full-page screenshot (no deduplication for homepage)
        captured_path = capture_full_page_screenshot(driver, screenshot_path)
        
        if captured_path:
            # Convert absolute path to relative path for static serving
            relative_path = os.path.relpath(captured_path, PROJECT_ROOT / "odito_backend")
            # Normalize path separators to forward slashes for URLs
            relative_path = relative_path.replace(os.sep, '/')
            
            # Store in seo_mainurl_snapshot collection
            snapshot_doc = {
                "project_id": ObjectId(project_id),
                "job_id": ObjectId(job_id),
                "url": url,
                "final_url": final_url,
                "canonical_url": canonical_url,
                "screenshot_path": relative_path,
                "dom_hash": dom_hash,
                "scroll_height": scroll_height,
                "status": "captured",
                "error": None,
                "captured_at": datetime.utcnow()
            }
            
            seo_mainurl_snapshot.insert_one(snapshot_doc)
            
            return {
                "status": "captured",
                "screenshot_path": relative_path,
                "final_url": final_url,
                "canonical_url": canonical_url,
                "dom_hash": dom_hash,
                "scroll_height": scroll_height
            }
        else:
            # Store failure record
            snapshot_doc = {
                "project_id": ObjectId(project_id),
                "job_id": ObjectId(job_id),
                "url": url,
                "final_url": final_url if 'final_url' in locals() else url,
                "canonical_url": canonical_url if 'canonical_url' in locals() else None,
                "screenshot_path": None,
                "dom_hash": dom_hash if 'dom_hash' in locals() else None,
                "scroll_height": scroll_height if 'scroll_height' in locals() else None,
                "status": "failed",
                "error": "Screenshot capture failed",
                "captured_at": datetime.utcnow()
            }
            
            seo_mainurl_snapshot.insert_one(snapshot_doc)
            
            return {
                "status": "failed",
                "error": "Screenshot capture failed"
            }
            
    except Exception as e:
        # Store failure record even on exception
        try:
            snapshot_doc = {
                "project_id": ObjectId(project_id),
                "job_id": ObjectId(job_id),
                "url": url,
                "final_url": driver.current_url if driver else url,
                "canonical_url": get_canonical_url(driver) if driver else None,
                "screenshot_path": None,
                "dom_hash": None,
                "scroll_height": None,
                "status": "failed",
                "error": str(e),
                "captured_at": datetime.utcnow()
            }
            
            seo_mainurl_snapshot.insert_one(snapshot_doc)
        except Exception:
            pass  # Even metadata storage can fail
        
        return {
            "status": "failed",
            "error": str(e)
        }
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
