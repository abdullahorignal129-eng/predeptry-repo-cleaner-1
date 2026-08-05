import os
import asyncio
import aiohttp
import json
from typing import List, Dict, Any

API = "https://api.github.com/graphql"
HF_SPACE_URL = os.environ.get("HF_SPACE_URL", "http://localhost:7860")

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

        # FIXED: issues(states: [OPEN]) instead of issues(states: OPEN)
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
            languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
                totalSize
                edges {{ node {{ name }} size }}
            }}
        }}
        ''')

    return "query { " + " ".join(aliases) + " }"

def extract_lang_data(lang_graphql: dict) -> dict:
    if not lang_graphql: return {}, 0, 0, 0.0
    total_size = lang_graphql.get("totalSize", 0)
    edges = lang_graphql.get("edges", [])
    langs = {e["node"]["name"]: e["size"] for e in edges if e and e.get("node")}

    py_bytes = langs.get("Python", 0)
    pct = round((100.0 * py_bytes / total_size), 2) if total_size > 0 else 0.0
    return langs, py_bytes, total_size, pct

async def process_batch(jobs: List[Dict[str, Any]], token: str, results: Dict[str, list], session: aiohttp.ClientSession):
    if not jobs: return

    query = build_graphql_query(jobs)
    headers = auth_headers(token)

    try:
        async with session.post(API, json={"query": query}, headers=headers, timeout=30) as response:
            if response.status == 200:
                data = await response.json()

                if "errors" in data:
                    err_msg = str(data["errors"])[:1000]
                    # DEBUG: print the actual GraphQL error so it's visible in CI logs
                    print(f"[GraphQL ERROR] batch of {len(jobs)} jobs, token {token[:4]}...: {err_msg}")

                    graph_data = data.get("data", {})
                    if graph_data:
                        # Partial data may still be present alongside errors.
                        # Salvage whatever aliases came back non-null instead of
                        # discarding the entire batch.
                        recovered = 0
                        for i, job in enumerate(jobs):
                            rid = job["repo_id"]
                            repo_data = graph_data.get(f"repo{i}")
                            if repo_data is None:
                                results["errors"].append({"repo_id": rid, "error": f"graphql_null: {err_msg[:150]}"})
                                continue

                            langs, py_bytes, total_bytes, pct = extract_lang_data(repo_data.get("languages"))
                            results["metadata"].append({
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
                                "python_pct": pct
                            })
                            recovered += 1
                        print(f"[GraphQL ERROR] recovered {recovered}/{len(jobs)} despite errors block")
                    else:
                        # No usable data at all — genuinely fail the whole batch
                        for job in jobs:
                            results["errors"].append({"repo_id": job["repo_id"], "error": f"graphql: {err_msg}"})
                    return

                graph_data = data.get("data", {})

                for i, job in enumerate(jobs):
                    rid = job["repo_id"]
                    repo_data = graph_data.get(f"repo{i}")

                    if repo_data is None:
                        results["errors"].append({"repo_id": rid, "error": "not_found_or_deleted"})
                        continue

                    langs, py_bytes, total_bytes, pct = extract_lang_data(repo_data.get("languages"))

                    results["metadata"].append({
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
                        "python_pct": pct
                    })

            elif response.status in [403, 429]:
                print(f"Token {token[:4]}... rate limited (HTTP {response.status}). Sleeping 60s.")
                await asyncio.sleep(60)
                await process_batch(jobs, token, results, session)
            else:
                err = await response.text()
                # DEBUG: print the actual HTTP error body
                print(f"[HTTP ERROR] status={response.status} token={token[:4]}... body={err[:500]}")
                for job in jobs:
                    results["errors"].append({"repo_id": job["repo_id"], "error": f"http_{response.status}"})

    except Exception as e:
        print(f"[EXCEPTION] batch of {len(jobs)} jobs, token {token[:4]}...: {e!r}")
        for job in jobs:
            results["errors"].append({"repo_id": job["repo_id"], "error": str(e)[:100]})

async def token_worker(token: str, queue: asyncio.Queue, results: Dict[str, list]):
    sem = asyncio.Semaphore(2)
    async with aiohttp.ClientSession() as session:
        while True:
            batch = await queue.get()
            if batch is None:
                queue.task_done()
                break
            async with sem:
                await process_batch(batch, token, results, session)
            queue.task_done()

async def main():
    tokens = load_tokens()
    if not tokens:
        print("WARNING: no tokens provided. Set GITHUB_TOKENS secret.")
        return

    print(f"Loaded {len(tokens)} tokens. 2 concurrent requests per token active.")

    async with aiohttp.ClientSession() as http_session:
        jobs = []
        retries = 0
        while retries < 5:
            print(f"Fetching jobs from {HF_SPACE_URL}/get-jobs...")
            try:
                async with http_session.get(f"{HF_SPACE_URL}/get-jobs", timeout=30) as resp:
                    data = await resp.json()
                    jobs = data.get("jobs", [])
                    if jobs:
                        break
                    print(f"No jobs available right now. Retrying in 15 seconds... (Attempt {retries+1}/5)")
            except Exception as e:
                print(f"Error contacting HF Space: {e}. Retrying in 15s...")

            await asyncio.sleep(15)
            retries += 1

    if not jobs:
        print("No jobs received after 5 attempts. Exiting.")
        return

    print(f"Received {len(jobs)} jobs. Batching into 100s for GraphQL...")

    # DEBUG: show a sample of the first job so you can eyeball repo_path format
    print(f"[DEBUG] Sample job[0]: {jobs[0]}")

    batch_size = 100
    queue = asyncio.Queue()
    for i in range(0, len(jobs), batch_size):
        await queue.put(jobs[i:i + batch_size])

    for _ in tokens:
        await queue.put(None)

    results = {"metadata": [], "errors": []}
    workers = [asyncio.create_task(token_worker(t, queue, results)) for t in tokens]

    await queue.join()
    for w in workers:
        w.cancel()

    print(f"Scraping complete. Metadata: {len(results['metadata'])}, Errors: {len(results['errors'])}")

    # DEBUG: show up to 3 sample errors so the real cause is visible in CI logs
    if results["errors"]:
        print("[DEBUG] Sample errors:")
        for e in results["errors"][:3]:
            print(f"  {e}")

    print("Posting work back to HF Space...")
    async with aiohttp.ClientSession() as http_session:
        try:
            async with http_session.post(f"{HF_SPACE_URL}/post-work", json=results, timeout=60) as resp:
                post_resp = await resp.json()
                print(f"Post-work response: {post_resp}")
        except Exception as e:
            print(f"Failed to post work: {e}")

if __name__ == "__main__":
    asyncio.run(main())
