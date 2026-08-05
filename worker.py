# worker.py
import os
import asyncio
import aiohttp
import json
import argparse
from typing import List, Dict, Any

API = "https://api.github.com"
PER_TOKEN_CONCURRENCY = 3
HF_SPACE_URL = os.environ.get("HF_SPACE_URL", "http://localhost:7860")

# Directories to completely ignore when counting files
EXCLUDED_DIRS = {".github", ".venv", "node_modules", "dist", "build", "target", "__pycache__", ".idea", ".vscode", "venv", "env"}
# File extensions to ignore (compiled/built artifacts)
EXCLUDED_EXTS = {".pyc", ".pyo", ".so", ".dll", ".exe", ".class", ".o", ".obj", ".jar", ".war"}

def is_excluded(path: str) -> bool:
    """Check if a file path falls into excluded directories or extensions."""
    parts = path.lower().split("/")
    for part in parts[:-1]:
        if part in EXCLUDED_DIRS:
            return True
    if "." in parts[-1]:
        ext = "." + parts[-1].rsplit(".", 1)[-1]
        if ext in EXCLUDED_EXTS:
            return True
    return False

def load_tokens() -> list[str]:
    multi = os.environ.get("GITHUB_TOKENS", "").strip()
    if multi:
        return [t.strip() for t in multi.split(",") if t.strip()]
    one = (os.environ.get("GITHUB_TOKEN") or "").strip()
    return [one] if one else []

def auth_headers(token: str) -> dict:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "python-dataset-pipeline",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

class RateLimiter:
    def __init__(self):
        self._reset_at = 0.0
        self._lock = asyncio.Lock()

    async def wait_if_needed(self):
        async with self._lock:
            wait = self._reset_at - asyncio.get_event_loop().time()
        if wait > 0:
            await asyncio.sleep(min(wait, 120) + 1)

    def note_response(self, status: int, headers) -> bool:
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        if status == 403 and remaining == "0" and reset:
            self._reset_at = max(self._reset_at, float(reset))
            return True
        if status == 403 and "retry-after" in {k.lower() for k in headers.keys()}:
            retry_after = float(headers.get("Retry-After", 5))
            self._reset_at = max(self._reset_at, asyncio.get_event_loop().time() + retry_after)
            return True
        if status in (502, 503, 504):
            self._reset_at = max(self._reset_at, asyncio.get_event_loop().time() + 3)
            return True
        if remaining is not None:
            try:
                rem = int(remaining)
                if rem <= 3 and reset:
                    self._reset_at = max(self._reset_at, float(reset))
            except ValueError:
                pass
        return False

async def http_get(session, limiter, sem, url, max_retries=8):
    for attempt in range(max_retries):
        await limiter.wait_if_needed()
        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    body = await r.read()
                    should_retry = limiter.note_response(r.status, r.headers)
                    if should_retry:
                        continue
                    return r.status, body
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(2 ** min(attempt, 5))
    return None, None

def python_pct(langs: dict) -> float:
    if not langs: return 0.0
    t = sum(langs.values())
    return (100.0 * langs.get("Python", 0) / t) if t else 0.0

async def fetch_repo(session, limiter, sem, rid: int):
    status, body = await http_get(session, limiter, sem, f"{API}/repositories/{rid}")
    if status is None: return "error", None
    if status in (404, 451): return "not_found", None
    if status != 200: return "error", None
    try: return "ok", json.loads(body)
    except: return "error", None

async def fetch_langs(session, limiter, sem, rid: int) -> dict:
    status, body = await http_get(session, limiter, sem, f"{API}/repositories/{rid}/languages")
    if status != 200 or body is None: return {}
    try: return json.loads(body) or {}
    except: return {}

async def fetch_and_count_files(session, limiter, sem, full_name: str, branch: str) -> int | None:
    """Fetches the Git Tree and counts valid files, ignoring built/junk folders."""
    if not full_name or not branch:
        return None
        
    status, body = await http_get(session, limiter, sem, f"{API}/repos/{full_name}/git/trees/{branch}?recursive=1")
    if status != 200 or body is None:
        return None
        
    try:
        tree_data = json.loads(body)
        # If the tree is truncated by GitHub, it has > 100,000 files. We can just return a huge number.
        if tree_data.get("truncated"):
            return 999999 
            
        count = 0
        for item in tree_data.get("tree", []):
            if item.get("type") == "blob" and not is_excluded(item.get("path", "")):
                count += 1
        return count
    except:
        return None

def usable_canonical(j: dict, allow_archived: bool) -> tuple[bool, str]:
    if j.get("fork"): return False, "still_fork"
    if j.get("disabled"): return False, "disabled"
    if not allow_archived and j.get("archived"): return False, "archived"
    if not j.get("full_name"): return False, "no_name"
    return True, "ok"

def upstream_ids(fork_j: dict) -> list[int]:
    out = []
    for key in ("source", "parent"):
        block = fork_j.get(key) or {}
        uid = block.get("id")
        if uid is not None:
            uid = int(uid)
            if uid not in out: out.append(uid)
    return out

async def process_repo_id(rid, session, limiter, sem, results: Dict[str, list], allow_archived: bool):
    st, j = await fetch_repo(session, limiter, sem, rid)
    if st != "ok" or not j:
        results["errors"].append({"repo_id": rid, "error": st})
        return

    if not j.get("fork"):
        ok, reason = usable_canonical(j, allow_archived)
        if not ok:
            results["errors"].append({"repo_id": rid, "error": reason})
            return
        
        # Fetch file count (does NOT filter, just records the number)
        file_count = await fetch_and_count_files(session, limiter, sem, j.get("full_name"), j.get("default_branch"))
        if file_count is None:
            results["errors"].append({"repo_id": rid, "error": "tree_fetch_failed"})
            return

        langs = await fetch_langs(session, limiter, sem, rid)
        total = sum(langs.values()) if langs else 0
        py = langs.get("Python", 0) if langs else 0
        
        results["metadata"].append({
            "repo_id": rid,
            "full_name": j.get("full_name"),
            "html_url": j.get("html_url"),
            "description": j.get("description"),
            "default_branch": j.get("default_branch"),
            "size_kb": j.get("size"),
            "stars": j.get("stargazers_count"),
            "forks_count": j.get("forks_count"),
            "open_issues": j.get("open_issues_count"),
            "license": (j.get("license") or {}).get("spdx_id"),
            "archived": 1 if j.get("archived") else 0,
            "created_at": j.get("created_at"),
            "updated_at": j.get("updated_at"),
            "pushed_at": j.get("pushed_at"),
            "languages_json": json.dumps(langs),
            "python_bytes": py,
            "total_lang_bytes": total,
            "python_pct": round(python_pct(langs), 2),
            "file_count": file_count, # Saved to DB for later filtering
            "resolved_from_fork_id": None
        })
        return

    # Fork logic
    rec = {
        "fork_repo_id": rid,
        "fork_full_name": j.get("full_name"),
        "fork_html_url": j.get("html_url"),
        "parent_id": (j.get("parent") or {}).get("id"),
        "parent_full_name": (j.get("parent") or {}).get("full_name"),
        "source_id": (j.get("source") or {}).get("id"),
        "source_full_name": (j.get("source") or {}).get("full_name"),
        "resolved_canonical_id": None,
        "resolution": "pending"
    }
    
    uids = upstream_ids(j)
    if not uids:
        rec["resolution"] = "no_parent_or_source"
        results["forks"].append(rec)
        return

    for uid in uids:
        ust, uj = await fetch_repo(session, limiter, sem, uid)
        if ust != "ok" or not uj: continue
        
        ok, reason = usable_canonical(uj, allow_archived)
        if not ok:
            rec["resolution"] = f"upstream_{uid}_{reason}"
            continue
            
        # Fetch file count for upstream repo
        file_count = await fetch_and_count_files(session, limiter, sem, uj.get("full_name"), uj.get("default_branch"))
        if file_count is None:
            rec["resolution"] = f"upstream_{uid}_tree_fetch_failed"
            continue

        langs = await fetch_langs(session, limiter, sem, uid)
        total = sum(langs.values()) if langs else 0
        py = langs.get("Python", 0) if langs else 0
        
        results["metadata"].append({
            "repo_id": uid,
            "full_name": uj.get("full_name"),
            "html_url": uj.get("html_url"),
            "description": uj.get("description"),
            "default_branch": uj.get("default_branch"),
            "size_kb": uj.get("size"),
            "stars": uj.get("stargazers_count"),
            "forks_count": uj.get("forks_count"),
            "open_issues": uj.get("open_issues_count"),
            "license": (uj.get("license") or {}).get("spdx_id"),
            "archived": 1 if uj.get("archived") else 0,
            "created_at": uj.get("created_at"),
            "updated_at": uj.get("updated_at"),
            "pushed_at": uj.get("pushed_at"),
            "languages_json": json.dumps(langs),
            "python_bytes": py,
            "total_lang_bytes": total,
            "python_pct": round(python_pct(langs), 2),
            "file_count": file_count,
            "resolved_from_fork_id": rid
        })
        
        rec["resolved_canonical_id"] = uid
        rec["resolution"] = "saved_upstream"
        results["forks"].append(rec)
        return

    if rec["resolution"] == "pending":
        rec["resolution"] = "upstream_unavailable"
    results["forks"].append(rec)

async def token_worker(token: str, ids: list[int], results: Dict[str, list], allow_archived: bool):
    limiter = RateLimiter()
    sem = asyncio.Semaphore(PER_TOKEN_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=PER_TOKEN_CONCURRENCY)
    
    async with aiohttp.ClientSession(headers=auth_headers(token), connector=connector) as session:
        async def run_one(rid):
            try:
                await process_repo_id(rid, session, limiter, sem, results, allow_archived)
            except Exception as e:
                results["errors"].append({"repo_id": rid, "error": str(e)[:500]})

        await asyncio.gather(*(run_one(rid) for rid in ids))

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--allow-archived", action="store_true")
    args = ap.parse_args()

    tokens = load_tokens()
    if not tokens:
        print("WARNING: no tokens -- 1 anonymous worker (60 req/hr)")
        tokens = [""]
    else:
        print(f"Tokens = {len(tokens)}, {PER_TOKEN_CONCURRENCY} concurrent req/token")

    async with aiohttp.ClientSession() as http_session:
        print(f"Fetching jobs from {HF_SPACE_URL}/get-jobs...")
        async with http_session.get(f"{HF_SPACE_URL}/get-jobs") as resp:
            data = await resp.json()
            ids = data.get("repo_ids", [])

    if not ids:
        print("No jobs received from server. Exiting.")
        return

    print(f"Received {len(ids)} jobs. Starting scraping...")

    chunk_size = len(ids) // len(tokens) if tokens else len(ids)
    id_chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    results = {"metadata": [], "forks": [], "errors": []}
    
    await asyncio.gather(
        *(token_worker(tokens[i], chunk, results, args.allow_archived) for i, chunk in enumerate(id_chunks) if chunk)
    )

    print(f"Scraping complete. Metadata: {len(results['metadata'])}, Forks: {len(results['forks'])}, Errors: {len(results['errors'])}")
    
    print("Posting work back to HF Space...")
    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(f"{HF_SPACE_URL}/post-work", json=results) as resp:
            post_resp = await resp.json()
            print(f"Post-work response: {post_resp}")

if __name__ == "__main__":
    asyncio.run(main())
