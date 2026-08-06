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
CONCURRENCY_PER_TOKEN = int(os.environ.get("CONCURRENCY_PER_TOKEN", "3"))
# Reverted to 100. GitHub will 502/504 if you go higher than 100 aliases.
BATCH_SIZE = int(os.environ.get("GRAPHQL_BATCH_SIZE", "100")) 

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
    if not s:
        return ""
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

        # Reduced languages to 5 to lower query complexity and prevent 502s
        aliases.append(f'''
        repo{i}: repository(owner: "{owner_esc}", name: "{name_esc}") {{
            nameWithOwner
            description
            stargazerCount
            forkCount
            issues(states: [OPEN]) {{ totalCount }}
            licenseInfo {{ spdxId }}
            isArchived
            createdAt
            updatedAt
            pushedAt
            languages(first: 5, orderBy: {{field: SIZE, direction: DESC}}) {{
                totalSize
                edges {{ node {{ name }} size }}
            }}
        }}
        ''')

    aliases.append("rateLimit { cost remaining resetAt }")
    return "query { " + " ".join(aliases) + " }"

def extract_lang_data(lang_graphql: dict) -> dict:
    if not lang_graphql:
        return {}, 0, 0, 0.0
    total_size = lang_graphql.get("totalSize", 0)
    edges = lang_graphql.get("edges", [])
    langs = {e["node"]["name"]: e["size"] for e in edges if e and e.get("node")}

    py_bytes = langs.get("Python", 0)
    pct = round((100.0 * py_bytes / total_size), 2) if total_size > 0 else 0.0
    return langs, py_bytes, total_size, pct

def repo_data_to_metadata_row(rid: int, repo_data: dict) -> dict:
    langs, py_bytes, total_bytes, pct = extract_lang_data(repo_data.get("languages"))
    return {
        "repo_id": rid,
        "full_name": repo_data.get("nameWithOwner"),
        "description": repo_data.get("description"),
        "stars": repo_data.get("stargazerCount"),
        "forks_count": repo_data.get("forkCount"),
        "open_issues": repo_data.get("issues", {}).get("totalCount", 0),
        "license": (repo_data.get("licenseInfo") or {}).get("spdxId"),
        "archived": 1 if repo_data.get("isArchived") else 0,
        "created_at": repo_data.get("createdAt"),
        "updated_at": repo_data.get("updatedAt"),
        "pushed_at": repo_data.get("pushedAt"),
        "languages_json": json.dumps(langs),
        "python_bytes": py_bytes,
        "total_lang_bytes": total_bytes,
        "python_pct": pct,
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

    def line(self) -> str:
        total = self.jobs_ok + self.jobs_error
        rate = (100.0 * self.jobs_ok / total) if total else 0.0
        return (f"[progress] batches={self.batches_done} jobs_ok={self.jobs_ok} "
                f"jobs_error={self.jobs_error} ({rate:.1f}% ok) "
                f"not_found={self.not_found} http_err={self.http_errors} "
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
                        # GitHub returned a 200 OK but with invalid JSON (rare, but happens)
                        raise Exception("Invalid JSON response")
                    
                    if data is None:
                        raise Exception("Response JSON is None")
                        
                    graph_data = data.get("data", {}) or {} 
                    
                    # Safe rate limit logging & automatic proactive sleep guard
                    if "rateLimit" in graph_data:
                        rl = graph_data["rateLimit"]
                        remaining = rl.get("remaining", 5000)
                        reset_at = rl.get("resetAt")
                        print(f"[{token_label}] Cost: {rl.get('cost')} | Rem: {remaining}/5000 | Resets: {reset_at}")
                        
                        # Proactive Rate-Limit Guard
                        if remaining < 150 and reset_at:
                            try:
                                reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                                sleep_seconds = max(int((reset_dt - datetime.now(timezone.utc)).total_seconds()) + 5, 10)
                                print(f"[{token_label}] WARNING: Token low ({remaining} rem). Proactively sleeping for {sleep_seconds}s until reset.")
                                await asyncio.sleep(sleep_seconds)
                            except Exception:
                                await asyncio.sleep(60)

                    # Handle case where GraphQL returned partial data + an errors array
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
                                results["metadata"].append(repo_data_to_metadata_row(rid, repo_data))

                            recovered = len(jobs) - batch_not_found
                            stats.jobs_ok += recovered
                            stats.jobs_error += batch_not_found
                            stats.not_found += batch_not_found
                            if recovered < len(jobs) * 0.8:
                                sample = str(data["errors"])[:300]
                                print(f"[{token_label}] low recovery {recovered}/{len(jobs)} - sample: {sample}")
                        else:
                            for job in jobs:
                                results["errors"].append({"repo_id": job["repo_id"], "error": "graphql_no_data"})
                            stats.jobs_error += len(jobs)
                            sample = str(data["errors"])[:300]
                            print(f"[{token_label}] batch failed entirely (no data) - sample: {sample}")
                        
                        stats.batches_done += 1
                        if stats.batches_done % 25 == 0:
                            print(stats.line())
                        return

                    # Handle standard clean 200 OK responses with no errors
                    batch_ok = 0
                    batch_missing = 0
                    for i, job in enumerate(jobs):
                        rid = job["repo_id"]
                        repo_data = graph_data.get(f"repo{i}")
                        if repo_data is None:
                            results["errors"].append({"repo_id": rid, "error": "not_found_or_deleted"})
                            batch_missing += 1
                            continue
                        results["metadata"].append(repo_data_to_metadata_row(rid, repo_data))
                        batch_ok += 1
                    stats.jobs_ok += batch_ok
                    stats.jobs_error += batch_missing
                    stats.not_found += batch_missing

                    stats.batches_done += 1
                    if stats.batches_done % 25 == 0:
                        print(stats.line())
                    return

                elif response.status in (403, 429):
                    stats.rate_limited += 1
                    print(f"[{token_label}] rate limited (HTTP {response.status}), sleeping 60s (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(60)
                    continue

                elif response.status in (502, 503, 504):
                    print(f"[{token_label}] HTTP {response.status}. Retrying... (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(3)
                    continue

                else:
                    body = await response.text()
                    print(f"[{token_label}] HTTP {response.status}: {body[:200]}")
                    for job in jobs:
                        results["errors"].append({"repo_id": job["repo_id"], "error": f"http_{response.status}"})
                    stats.jobs_error += len(jobs)
                    stats.http_errors += 1
                    stats.batches_done += 1
                    if stats.batches_done % 25 == 0:
                        print(stats.line())
                    return

        except Exception as e:
            print(f"[{token_label}] exception: {e!r}. Retrying... (Attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(3)
            continue

    print(f"[{token_label}] Max retries exceeded for batch. Marking as error.")
    for job in jobs:
        results["errors"].append({"repo_id": job["repo_id"], "error": "max_retries_exceeded"})
    stats.jobs_error += len(jobs)
    stats.exceptions += 1
    
    stats.batches_done += 1
    if stats.batches_done % 25 == 0:
        print(stats.line())

async def token_worker(token: str, token_label: str, queue: asyncio.Queue,
                        results: Dict[str, list], stats: RunStats):
    sem = asyncio.Semaphore(CONCURRENCY_PER_TOKEN)

    async def run_one(batch, session):
        async with sem:
            await process_batch(batch, token, results, session, stats, token_label)

    async with aiohttp.ClientSession() as session:
        tasks = []
        while True:
            batch = await queue.get()
            if batch is None:
                queue.task_done()
                break
            tasks.append(asyncio.create_task(run_one(batch, session)))
            queue.task_done()
        if tasks:
            await asyncio.gather(*tasks)

async def fetch_jobs(http_session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    for attempt in range(5):
        try:
            async with http_session.get(f"{HF_SPACE_URL}/get-jobs", timeout=30) as resp:
                data = await resp.json()
                jobs = data.get("jobs", [])
                if jobs:
                    return jobs
                print(f"No jobs available (attempt {attempt+1}/5), retrying in 15s...")
        except Exception as e:
            print(f"Error contacting HF Space: {e}, retrying in 15s...")
        await asyncio.sleep(15)
    return []

async def post_work(http_session: aiohttp.ClientSession, results: Dict[str, list]):
    try:
        async with http_session.post(f"{HF_SPACE_URL}/post-work", json=results, timeout=60) as resp:
            post_resp = await resp.json()
            print(f"Posted: {post_resp}")
    except Exception as e:
        print(f"Failed to post work: {e}")

async def run_one_round(tokens: List[str], jobs: List[Dict[str, Any]], stats: RunStats) -> Dict[str, list]:
    queue = asyncio.Queue()
    for i in range(0, len(jobs), BATCH_SIZE):
        await queue.put(jobs[i:i + BATCH_SIZE])
    for _ in tokens:
        await queue.put(None)

    results = {"metadata": [], "errors": []}
    workers = [
        asyncio.create_task(token_worker(t, f"tok{idx+1}", queue, results, stats))
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

    print(f"Loaded {len(tokens)} token(s), {CONCURRENCY_PER_TOKEN} concurrent requests/token, "
          f"batch size {BATCH_SIZE}, max runtime {MAX_RUNTIME_SECONDS}s")

    stats = RunStats()
    start = time.monotonic()
    round_num = 0

    async with aiohttp.ClientSession() as http_session:
        while True:
            elapsed = time.monotonic() - start
            if elapsed > MAX_RUNTIME_SECONDS:
                print(f"Runtime budget reached ({elapsed:.0f}s), stopping.")
                break

            round_num += 1
            jobs = await fetch_jobs(http_session)
            if not jobs:
                print("No jobs received after retries. Queue is likely empty. Exiting.")
                break

            print(f"[round {round_num}] received {len(jobs)} jobs")
            results = await run_one_round(tokens, jobs, stats)
            await post_work(http_session, results)
            print(stats.line())

    total = stats.jobs_ok + stats.jobs_error
    print(f"=== Run complete: {total} jobs processed | "
          f"{stats.jobs_ok} ok | {stats.jobs_error} error "
          f"({stats.not_found} not_found, {stats.http_errors} http_err batches, "
          f"{stats.exceptions} exceptions, {stats.rate_limited} rate_limit hits) ===")

if __name__ == "__main__":
    asyncio.run(main())
