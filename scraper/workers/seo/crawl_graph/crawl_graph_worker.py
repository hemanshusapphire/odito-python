"""
CRAWL_GRAPH worker — In-memory link graph analysis.

Computes link graph metrics from seo_page_data documents:
  - Inbound/outbound link counts per URL
  - Orphan page detection (zero inbound, non-homepage)
  - Click depth from homepage via BFS
  - NO HTTP requests — pure computation on stored data

Performance: O(V + E) where V = URLs, E = total internal links.
Target: < 30 seconds for 100 URLs.
"""

import os
import traceback
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from collections import deque

from bson.objectid import ObjectId

from db import seo_page_data, seo_crawl_graph
from scraper.shared.utils import normalize_url


def _identify_homepage(urls: list) -> str | None:
    """
    Identify the homepage from a list of URLs.
    Homepage is the URL whose path is "/" (root).
    If multiple roots exist (e.g. www vs non-www), pick the first match.
    """
    for url in urls:
        try:
            parsed = urlparse(url)
            path = parsed.path.rstrip("/")
            if path == "" or path == "/":
                return url
        except Exception:
            continue
    return None


def _compute_crawl_graph(project_id: str, job_id: str) -> dict:
    """
    Core graph computation — zero HTTP, pure in-memory analysis.

    Returns dict with:
      - documents: list of crawl graph records ready for insert
      - stats: summary statistics
    """
    # 1. Fetch all page data for this project (url + internal_links only)
    cursor = seo_page_data.find(
        {"projectId": ObjectId(project_id)},
        {"url": 1, "internal_links": 1, "_id": 0}
    )

    pages = list(cursor)

    if not pages:
        return {"documents": [], "stats": {"totalUrls": 0, "orphanPages": 0}}

    # 2. Build adjacency map: url -> [outbound links]
    adjacency = {}
    all_urls = set()

    for page in pages:
        url = page.get("url", "")
        if not url:
            continue
        internal_links = page.get("internal_links") or []
        adjacency[url] = internal_links
        all_urls.add(url)

    if not all_urls:
        return {"documents": [], "stats": {"totalUrls": 0, "orphanPages": 0}}

    # 3. Build inbound count map
    inbound_count = {url: 0 for url in all_urls}
    for source_url, targets in adjacency.items():
        for target in targets:
            if target in inbound_count:
                inbound_count[target] += 1

    # 4. Identify homepage
    homepage = _identify_homepage(list(all_urls))

    # 5. BFS from homepage for click depth
    click_depth = {}
    if homepage and homepage in all_urls:
        # BFS traversal
        visited = set()
        queue = deque()
        queue.append((homepage, 0))
        visited.add(homepage)

        while queue:
            current_url, depth = queue.popleft()
            click_depth[current_url] = depth

            for linked_url in adjacency.get(current_url, []):
                if linked_url in all_urls and linked_url not in visited:
                    visited.add(linked_url)
                    queue.append((linked_url, depth + 1))

    # 6. Build output documents
    analyzed_at = datetime.now(timezone.utc)
    documents = []
    orphan_count = 0

    for url in all_urls:
        outbound = len(adjacency.get(url, []))
        inbound = inbound_count.get(url, 0)
        is_homepage = (url == homepage)
        is_orphan = (inbound == 0 and not is_homepage)
        depth = click_depth.get(url, -1)  # -1 = unreachable from homepage

        if is_orphan:
            orphan_count += 1

        documents.append({
            "projectId": ObjectId(project_id),
            "jobId": ObjectId(job_id),
            "url": url,
            "inboundLinks": inbound,
            "outboundLinks": outbound,
            "clickDepthFromHomepage": depth,
            "isOrphan": is_orphan,
            "analyzedAt": analyzed_at
        })

    stats = {
        "totalUrls": len(all_urls),
        "orphanPages": orphan_count,
        "homepageIdentified": homepage is not None,
        "maxDepth": max(click_depth.values()) if click_depth else 0,
        "unreachablePages": sum(1 for url in all_urls if url not in click_depth)
    }

    return {"documents": documents, "stats": stats}


def execute_crawl_graph(job) -> dict:
    """
    Execute CRAWL_GRAPH job.

    Args:
        job: Pydantic model with jobId, projectId, userId, sourceJobId

    Returns:
        dict with status and results
    """
    job_id = job.jobId
    project_id = job.projectId
    # Validate required environment variables
    node_backend_url = os.environ.get("NODE_BACKEND_URL")
    if not node_backend_url:
        raise Exception("NODE_BACKEND_URL is required")

    print(f"[CRAWL_GRAPH] Starting | jobId={job_id} | projectId={project_id}")

    try:
        # Compute graph metrics (pure in-memory, zero HTTP)
        result = _compute_crawl_graph(project_id, job_id)
        documents = result["documents"]
        stats = result["stats"]

        print(f"[CRAWL_GRAPH] Graph computed | totalUrls={stats['totalUrls']} | orphans={stats['orphanPages']} | maxDepth={stats.get('maxDepth', 0)}")

        # Store results in seo_crawl_graph collection
        if documents:
            # Clean up any previous crawl graph data for this project
            seo_crawl_graph.delete_many({"projectId": ObjectId(project_id)})
            seo_crawl_graph.insert_many(documents, ordered=False)
            print(f"[CRAWL_GRAPH] Stored {len(documents)} graph records | projectId={project_id}")

        # Report completion to Node.js
        try:
            complete_url = f"{node_backend_url}/api/jobs/{job_id}/complete"
            complete_payload = {
                "stats": stats,
                "result_data": stats
            }
            response = requests.post(complete_url, json=complete_payload, timeout=10)
            response.raise_for_status()
            print(f"[CRAWL_GRAPH] Completion reported | jobId={job_id}")
        except requests.exceptions.Timeout:
            print(f"[CRAWL_GRAPH] Completion notification timeout (ignored) | jobId={job_id}")
        except Exception as cb_err:
            print(f"[CRAWL_GRAPH] Failed to report completion (job still succeeded) | jobId={job_id} | error={cb_err}")

        return {
            "status": "completed",
            "jobId": job_id,
            **stats
        }

    except Exception as e:
        print(f"❌ [CRAWL_GRAPH] Worker failed | jobId={job_id} | error={str(e)}")
        traceback.print_exc()

        # Report failure to Node.js — DO NOT block pipeline
        try:
            fail_url = f"{node_backend_url}/api/jobs/{job_id}/fail"
            requests.post(
                fail_url,
                json={"error": str(e), "stats": {}},
                timeout=10
            )
            print(f"[CRAWL_GRAPH] Failure reported | jobId={job_id}")
        except Exception as fail_err:
            print(f"⚠️ [CRAWL_GRAPH] Failed to report failure | error={str(fail_err)}")

        # Return graceful failure — DO NOT raise, DO NOT block pipeline
        return {
            "status": "failed_gracefully",
            "jobId": job_id,
            "error": str(e)
        }
