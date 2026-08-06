# worker.py
import os
import time
import asyncio
import aiohttp
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

API = "https://api.github.com/graphql"
HF_SPACE_URL = os.environ.get("HF_SPACE_URL", "http://localhost:7860")

MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", str(5 * 3600 + 30 * 60)))
BATCH_SIZE = 100

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def load_tokens() -> list[str]:
    multi = os.environ.get("GITHUB_TOKENS", "").strip()
    if multi:
        return [t.strip() for t in multi.split(",") if t.strip()]
    return []

def auth_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "python-dataset-pipeline"
    }

def escape_graphql_string(s: str) -> str:
    if not s: return ""
    return s.replace("\\", "\\\\").replace("\"", "\\\"")

def build_graphql_query(jobs: List[Dict[str, Any]]) -> str:
    aliases = []
    for i, job in enumerate(jobs):
        path = job.get("repo_path", "")
        if "/" not in path:
            aliases.append(f'repo{i}: repository(owner: "invalid", name: "invalid") {{ nameWithOwner }}')
            continue

        owner, name = path.split("/", 1)
        owner_esc = escape_graphql_string(owner)
        name_esc = escape_graphql_string(name)

        aliases.append(f'''
        repo{i}: repository(owner: "{owner_esc}", name: "{name_esc}") {{
            description
            homepageUrl
            stargazerCount
            forkCount
            watchers {{ totalCount }}
            isArchived
            isDisabled
            isFork
            parent {{ databaseId nameWithOwner }}
            pushedAt
            isTemplate
            primaryLanguage {{ name }}
        }}
        ''')

    aliases.append("rateLimit { cost remaining resetAt }")
    return "query { " + " ".join(aliases) + " }"

def repo_data_to_metadata_row(rid: int, repo_data: dict) -> dict:
    parent_info = repo_data.get("parent") or {}
    return {
        "repo_id": rid,
        "description": repo_data.get("description"),
        "homepage_url": repo_data.get("homepageUrl"),
        "stars": repo_data.get("stargazerCount"),
        "forks_count": repo_data.get("forkCount"),
        "watchers_count": (repo_data.get("watchers") or {}).get("totalCount"),
        "archived": 1 if repo_data.get("isArchived") else 0,
        "disabled": 1 if repo_data.get("isDisabled") else 0,
        "is_fork": 1 if repo_data.get("isFork") else 0,
        "resolved_parent_id": parent_info.get("databaseId"),
        "resolved_parent_path": parent_info.get("nameWithOwner"),
        "pushed_at": repo_data.get("pushedAt"),
        "is_template": 1 if repo_data.get("isTemplate") else 0,
        "primary_language": (repo_data.get("primaryLanguage") or {}).get("name"),
    }

class RunStats:
    def __init__(self):
        self.batches_done = 0
        self.jobs_ok = 0
        self.jobs_error = 0
        self.not_found = 0
        self.http_errors = 0
        self.exceptions = 0
        self.rate_limited = 0
        self.forks_resolved = 0

    def line(self) -> str:
        total = self.jobs_ok + self.jobs_error
        rate = (100.0 * self.jobs_ok / total) if total else 0.0
        return (f"[{ts()} progress] batches={self.batches_done} jobs_ok={self.jobs_ok} "
                f"jobs_error={self.jobs_error} ({rate:.1f}% ok) "
                f"forks_found={self.forks_resolved} http_err={self.http_errors} "
                f"exceptions={self.exceptions} rate_limited={self.rate_limited}")

async def process_batch(jobs: List[Dict[str, Any]], token: str, results: Dict[str, list],
                        session: aiohttp.ClientSession, stats: RunStats, token_label: str):
    if not jobs:
        return

    query = build_graphql_query(jobs)
    headers = auth_headers(token)
    max_retries = 5

    for attempt in range(max_retries):
        try:
            async with session.post(API, json={"query": query}, headers=headers, timeout=30) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except Exception:
                        raise Exception("Invalid JSON response")
                    
                    if data is None:
                        raise Exception("Response JSON is None")
                        
                    graph_data = data.get("data", {}) or {} 
                    
                    if "rateLimit" in graph_data:
                        rl = graph_data["rateLimit"]
                        remaining = rl.get("remaining", 5000)
                        reset_at = rl.get("resetAt")
                        print(f"[{ts()} {token_label}] Cost: {rl.get('cost')} | Rem: {remaining}/5000 | Resets: {reset_at}")
                        
                        if remaining < 150 and reset_at:
                            try:
                                reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                                sleep_seconds = max(int((reset_dt - datetime.now(timezone.utc)).total_seconds()) + 5, 10)
                                print(f"[{ts()} {token_label}] WARNING: Token low ({remaining} rem). Proactively sleeping for {sleep_seconds}s until reset.")
                                await asyncio.sleep(sleep_seconds)
                            except Exception:
                                await asyncio.sleep(60)

                    if "errors" in data:
                        batch_not_found = 0
                        if graph_data:
                            for i, job in enumerate(jobs):
                                rid = job["repo_id"]
                                repo_data = graph_data.get(f"repo{i}")
                                if repo_data is None:
                                    results["errors"].append({"repo_id": rid, "error": "not_found_or_deleted"})
                                    batch_not_found += 1
                                    continue
                                
                                if repo_data.get("isFork") and repo_data.get("parent"):
                                    stats.forks_resolved += 1
                                    results["errors"].append({"repo_id": rid, "error": "fork_resolved_to_parent"})
                                else:
                                    results["metadata"].append(repo_data_to_metadata_row(rid, repo_data))

                            recovered = len(jobs) - batch_not_found
                            stats.jobs_ok += recovered
                            stats.jobs_error += batch_not_found
                            stats.not_found += batch_not_found
                        else:
                            for job in jobs:
                                results["errors"].append({"repo_id": job["repo_id"], "error": "graphql_no_data"})
                            stats.jobs_error += len(jobs)
                        
                        stats.batches_done += 1
                        return

                    batch_ok = 0
                    batch_missing = 0
                    for i, job in enumerate(jobs):
                        rid = job["repo_id"]
                        repo_data = graph_data.get(f"repo{i}")
                        if repo_data is None:
                            results["errors"].append({"repo_id": rid, "error": "not_found_or_deleted"})
                            batch_missing += 1
                            continue
                        
                        if repo_data.get("isFork") and repo_data.get("parent"):
                            stats.forks_resolved += 1
                            results["errors"].append({"repo_id": rid, "error": "fork_resolved_to_parent"})
                        else:
                            results["metadata"].append(repo_data_to_metadata_row(rid, repo_data))
                            batch_ok += 1

                    stats.jobs_ok += batch_ok
                    stats.jobs_error += batch_missing
                    stats.not_found += batch_missing

                    stats.batches_done += 1
                    return

                elif response.status in (403, 429):
                    stats.rate_limited += 1
                    body = await response.text()
                    print(f"[{ts()} {token_label}] HTTP {response.status} (Attempt {attempt+1}/{max_retries}): {body[:200]}")
                    await asyncio.sleep(60)
                    continue

                elif response.status in (502, 503, 504):
                    print(f"[{ts()} {token_label}] HTTP {response.status}. Retrying... (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(3)
                    continue

                else:
                    body = await response.text()
                    print(f"[{ts()} {token_label}] HTTP {response.status}: {body[:200]}")
                    for job in jobs:
                        results["errors"].append({"repo_id": job["repo_id"], "error": f"http_{response.status}"})
                    stats.jobs_error += len(jobs)
                    stats.http_errors += 1
                    return

        except Exception as e:
            print(f"[{ts()} {token_label}] exception: {e!r}. Retrying... (Attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(3)
            continue

    print(f"[{ts()} {token_label}] Max retries exceeded for batch. Marking as error.")
    for job in jobs:
        results["errors"].append({"repo_id": job["repo_id"], "error": "max_retries_exceeded"})
    stats.jobs_error += len(jobs)
    stats.exceptions += 1

async def token_worker(token: str, token_label: str, queue: asyncio.Queue,
                        results: Dict[str, list], stats: RunStats, github_session: aiohttp.ClientSession):
    
    while True:
        batch = await queue.get()
        if batch is None:
            queue.task_done()
            break
            
        await process_batch(batch, token, results, github_session, stats, token_label)
        # Strict 2-second delay to guarantee no secondary rate limits
        await asyncio.sleep(2)
        queue.task_done()

async def fetch_jobs(http_session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    for attempt in range(5):
        try:
            async with http_session.get(f"{HF_SPACE_URL}/get-jobs", timeout=30) as resp:
                data = await resp.json()
                jobs = data.get("jobs", [])
                if jobs:
                    return jobs
                print(f"[{ts()} fetch] No jobs available (attempt {attempt+1}/5), retrying in 15s...")
        except Exception as e:
            print(f"[{ts()} fetch] Error contacting HF Space: {e}, retrying in 15s...")
        await asyncio.sleep(15)
    return []

async def post_work(http_session: aiohttp.ClientSession, results: Dict[str, list]):
    try:
        async with http_session.post(f"{HF_SPACE_URL}/post-work", json=results, timeout=60) as resp:
            post_resp = await resp.json()
            print(f"[{ts()} post] Posted: {post_resp}")
    except Exception as e:
        print(f"[{ts()} post] Failed to post work: {e}")

async def run_one_round(tokens: List[str], jobs: List[Dict[str, Any]], stats: RunStats, github_session: aiohttp.ClientSession) -> Dict[str, list]:
    queue = asyncio.Queue()
    
    for i in range(0, len(jobs), BATCH_SIZE):
        await queue.put(jobs[i:i + BATCH_SIZE])
    for _ in tokens:
        await queue.put(None)

    results = {"metadata": [], "errors": []}
    workers = [
        asyncio.create_task(token_worker(t, f"tok{idx+1}", queue, results, stats, github_session))
        for idx, t in enumerate(tokens)
    ]
    
    await queue.join()
    await asyncio.gather(*workers)
    return results

async def main():
    tokens = load_tokens()
    if not tokens:
        print("WARNING: no tokens provided. Set GITHUB_TOKENS secret.")
        return

    print(f"[{ts()} init] Loaded {len(tokens)} token(s). Strict 1 req per token / 2s delay. Batch size {BATCH_SIZE}. Max runtime {MAX_RUNTIME_SECONDS}s")

    stats = RunStats()
    start = time.monotonic()
    round_num = 0

    connector = aiohttp.TCPConnector(limit=100)
    
    async with aiohttp.ClientSession() as hf_session, aiohttp.ClientSession(connector=connector) as github_session:
        background_uploads = []
        
        while True:
            elapsed = time.monotonic() - start
            if elapsed > MAX_RUNTIME_SECONDS:
                break

            round_num += 1
            jobs = await fetch_jobs(hf_session)
            if not jobs:
                break

            print(f"\n[{ts()} system] === Round {round_num} starting === Received {len(jobs)} jobs. ===")
            
            round_start = time.monotonic()
            results = await run_one_round(tokens, jobs, stats, github_session)
            round_duration = time.monotonic() - round_start
            
            rate = len(jobs) / round_duration if round_duration > 0 else 0
            print(f"[{ts()} system] === Round {round_num} finished in {round_duration:.1f}s ({rate:.1f} repos/sec) ===")
            
            background_uploads.append(asyncio.create_task(post_work(hf_session, results)))
            print(stats.line())

        if background_uploads:
            await asyncio.gather(*background_uploads)

if __name__ == "__main__":
    asyncio.run(main())
