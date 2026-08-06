# worker.py
import os
import time
import asyncio
import aiohttp
import json
import random
from datetime import datetime, timezone
from typing import List, Dict, Any, Set

API = "https://api.github.com/graphql"
HF_SPACE_URL = os.environ.get("HF_SPACE_URL", "http://localhost:7860")

MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", str(5 * 3600 + 30 * 60)))

# --- Verified against GitHub's official docs (docs.github.com, checked live) ---
#
# THE "100 REPOS PER REQUEST" MYTH: correct, there's no such cap. first/last
# 1-100 applies to PAGINATED CONNECTIONS, not aliased single-object lookups
# like `repo0: repository(owner:,name:)`. Real ceilings:
#   - 500,000 total nodes per call (nowhere near it here)
#   - a 10-SECOND SERVER-SIDE TIMEOUT per call, after which GitHub kills the
#     request server-side. CONFIRMED FROM DOCS: this timeout comes back to
#     you as HTTP 502 *or* 504 - both mean the same thing, "your query took
#     too long". This was previously mishandled in this file: 502/504 were
#     treated as a random transient fault (fixed sleep, retry same size,
#     sizer never told) instead of the timeout signal they actually are.
#     That's the root cause of the storm in the last run - the adaptive
#     sizer never got a chance to shrink because its main feedback path
#     (asyncio.TimeoutError) rarely fired; GitHub was returning 502 well
#     before the client-side timeout ever tripped.
#
# SECONDARY RATE LIMIT:
#   - max 100 concurrent requests, shared across REST+GraphQL, GLOBAL to your
#     account/token - not per-token multiplied. 3 tokens x concurrency=1 is
#     nowhere near this, so it does not bind here.
#   - max 2,000 GraphQL points/minute; simple non-mutation queries are cheap
#     in points, so this rarely binds either.
#   - max 60 seconds of GraphQL CPU-time per 60 real seconds, PER TOKEN.
#     GitHub says to estimate this via response time. At batch=200 with
#     responses regularly running 9-10s, a single token can burn nearly its
#     *entire* per-minute CPU budget on ~6 requests. This is the real
#     throughput ceiling at this batch size, not concurrency.
#   - GitHub's own guidance: "Make requests serially instead of concurrently."
#     -> don't raise CONCURRENCY_PER_TOKEN as a way to get more throughput;
#     it fights the same per-token CPU-time budget instead of helping.
#
# CONCLUSION: throughput comes from staying comfortably under the 10s wall
# (which also keeps CPU-time-per-minute low, since fast responses cost less
# of that budget) and running enough tokens in parallel, not from bigger
# batches or higher concurrency. The empirical evidence in the log
# (batch=200 -> 9-10s, repeated 502s and RESOURCE_LIMITS_EXCEEDED) shows 200
# is already past the sweet spot for this exact field selection.
CONCURRENCY_PER_TOKEN = int(os.environ.get("CONCURRENCY_PER_TOKEN", "1"))

# Starting size lowered from 200 -> MIN, given the log shows 200 already
# consistently hits the wall for this query shape. The adaptive sizer will
# climb back up on its own if a run's data happens to be cheaper, but no
# longer opens every round with a burst of avoidable timeouts.
BATCH_SIZE = int(os.environ.get("GRAPHQL_BATCH_SIZE", "130"))
MIN_BATCH_SIZE = int(os.environ.get("GRAPHQL_MIN_BATCH_SIZE", "80"))
# Ceiling trimmed from 250 -> 180: the log shows 200 already over the line,
# so leaving headroom above it just invites another timeout storm before the
# sizer notices. Still adaptive, still free to sit below this.
MAX_BATCH_SIZE = int(os.environ.get("GRAPHQL_MAX_BATCH_SIZE", "180"))

# Server timeout is a hard 10s (confirmed in GitHub docs). We target well
# under that so a batch that's a bit slower than usual doesn't get
# server-killed and penalized.
TARGET_RESPONSE_SECONDS = float(os.environ.get("TARGET_RESPONSE_SECONDS", "10"))
HARD_SERVER_TIMEOUT_SECONDS = 10.0

# Minimum gap between the *start* of consecutive requests from the same token,
# shared across that token's concurrent slots via TokenPacer. With
# CONCURRENCY_PER_TOKEN=1 this is simply the pause between successive requests.
MIN_REQUEST_INTERVAL = float(os.environ.get("MIN_REQUEST_INTERVAL", "1.0"))

# GraphQL error "type" values that mean the repo genuinely does not resolve
# (deleted/renamed/never existed) - these are the ONLY ones we treat as a
# permanent not_found. Anything else (RESOURCE_LIMITS_EXCEEDED, TIMEOUT,
# SERVICE_UNAVAILABLE, etc.) means "GitHub couldn't compute this one right
# now", which is not evidence the repo is missing, so it gets requeued
# instead of mislabeled.
NOT_FOUND_ERROR_TYPES: Set[str] = {"NOT_FOUND"}

def ts() -> str:
    """Returns current time as HH:MM:SS for logging."""
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
    if not s:
        return ""
    return s.replace("\\", "\\\\").replace("\"", "\\\"")

def build_graphql_query(jobs: List[Dict[str, Any]]) -> str:
    """
    Requests the full set of fields GitHub's repository object exposes that are
    cheap (no extra pagination cost) to fetch alongside what you already had.
    Removed: openIssues/issue count (per your request - you don't want it).
    Added: everything else useful that comes "for free" on the repository node
    so you don't have to make a second pass later for it.

    NOTE: field selection here is intentionally untouched. See the summary in
    chat for which fields are the likely cost drivers if you ever want to trim.
    """
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
            nameWithOwner
            description
            homepageUrl
            stargazerCount
            forkCount
            watchers {{ totalCount }}
            licenseInfo {{ spdxId name }}
            isArchived
            isDisabled
            isFork
            isPrivate
            isTemplate
            diskUsage
            primaryLanguage {{ name }}
            createdAt
            updatedAt
            pushedAt
            defaultBranchRef {{ name }}
            repositoryTopics(first: 10) {{ nodes {{ topic {{ name }} }} }}
            languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
                totalSize
                edges {{ node {{ name }} size }}
            }}
        }}
        ''')

    aliases.append("rateLimit { cost remaining resetAt }")
    return "query { " + " ".join(aliases) + " }"

def extract_lang_data(lang_graphql: dict):
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
    topics = [
        n["topic"]["name"]
        for n in (repo_data.get("repositoryTopics") or {}).get("nodes", [])
        if n and n.get("topic")
    ]
    return {
        "repo_id": rid,
        "full_name": repo_data.get("nameWithOwner"),
        "description": repo_data.get("description"),
        "homepage_url": repo_data.get("homepageUrl"),
        "stars": repo_data.get("stargazerCount"),
        "forks_count": repo_data.get("forkCount"),
        "watchers_count": (repo_data.get("watchers") or {}).get("totalCount"),
        "license": (repo_data.get("licenseInfo") or {}).get("spdxId"),
        "license_name": (repo_data.get("licenseInfo") or {}).get("name"),
        "archived": 1 if repo_data.get("isArchived") else 0,
        "disabled": 1 if repo_data.get("isDisabled") else 0,
        "is_fork": 1 if repo_data.get("isFork") else 0,
        "is_private": 1 if repo_data.get("isPrivate") else 0,
        "is_template": 1 if repo_data.get("isTemplate") else 0,
        "disk_usage_kb": repo_data.get("diskUsage"),
        "primary_language": (repo_data.get("primaryLanguage") or {}).get("name"),
        "default_branch": (repo_data.get("defaultBranchRef") or {}).get("name"),
        "topics_json": json.dumps(topics),
        "created_at": repo_data.get("createdAt"),
        "updated_at": repo_data.get("updatedAt"),
        "pushed_at": repo_data.get("pushedAt"),
        "languages_json": json.dumps(langs),
        "python_bytes": py_bytes,
        "total_lang_bytes": total_bytes,
        "python_pct": pct,
    }

def classify_alias_errors(errors: list) -> Dict[str, Set[str]]:
    """
    Maps alias (e.g. 'repo37') -> set of GraphQL error 'type' values reported
    against that alias. Lets us tell "this repo genuinely doesn't exist"
    (NOT_FOUND) apart from "GitHub couldn't finish computing this one this
    round" (RESOURCE_LIMITS_EXCEEDED and everything else), which needs a
    retry rather than a permanent not_found label.
    """
    by_alias: Dict[str, Set[str]] = {}
    for err in errors or []:
        path = err.get("path") or []
        if not path:
            continue
        alias = str(path[0])
        by_alias.setdefault(alias, set()).add(err.get("type", "UNKNOWN"))
    return by_alias

class RunStats:
    def __init__(self):
        self.batches_done = 0
        self.jobs_ok = 0
        self.jobs_error = 0
        self.not_found = 0
        self.http_errors = 0
        self.exceptions = 0
        self.rate_limited = 0
        self.requeued = 0

    def line(self) -> str:
        total = self.jobs_ok + self.jobs_error
        rate = (100.0 * self.jobs_ok / total) if total else 0.0
        return (f"[{ts()} progress] batches={self.batches_done} jobs_ok={self.jobs_ok} "
                f"jobs_error={self.jobs_error} ({rate:.1f}% ok) "
                f"not_found={self.not_found} http_err={self.http_errors} "
                f"exceptions={self.exceptions} rate_limited={self.rate_limited} "
                f"requeued={self.requeued}")


class TokenPacer:
    """
    Enforces a minimum gap between request *starts* for a single token, and
    tracks a per-token secondary-limit 'penalty' backoff that grows the more
    that token gets 403'd, and decays over time when requests succeed.
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0
        self.penalty = 0.0  # extra seconds added to backoff, grows on repeated 403s

    async def wait_turn(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
            # jitter avoids every concurrent slot waking at the exact same instant
            self._next_allowed = max(time.monotonic(), self._next_allowed) + MIN_REQUEST_INTERVAL + random.uniform(0, 0.3)

    async def register_secondary_limit_hit(self, base_sleep: float):
        async with self._lock:
            self.penalty = min(self.penalty * 1.8 + base_sleep, 600)  # cap at 10 min
            self._next_allowed = time.monotonic() + self.penalty
            print(f"[{ts()}] secondary-limit penalty now {self.penalty:.0f}s for this token")

    def register_success(self):
        # slowly forgive penalty on sustained success
        self.penalty = max(0.0, self.penalty * 0.95)


class AdaptiveBatchSize:
    """
    GitHub doesn't publish the per-alias server cost of our query shape, and
    the thing that actually kills an oversized batch is the undocumented-until-
    hit 10-second server timeout - not a repo-count limit. So instead of
    guessing a fixed BATCH_SIZE, we measure real response latency per request
    (INCLUDING the 502/504 timeout responses - see process_batch) and adjust
    the shared target batch size up/down to converge on the largest batch
    that reliably finishes well under 10s.
    Shared across all tokens so they converge on the same safe ceiling together.
    """
    def __init__(self, start: int, min_size: int, max_size: int, target_seconds: float):
        self._lock = asyncio.Lock()
        self.current = start
        self.min_size = min_size
        self.max_size = max_size
        self.target_seconds = target_seconds

    async def record(self, batch_len: int, elapsed: float, timed_out: bool):
        async with self._lock:
            if timed_out or elapsed > HARD_SERVER_TIMEOUT_SECONDS * 0.8:
                # too close to the real 10s wall - back off hard
                self.current = max(self.min_size, int(self.current * 0.6))
                print(f"[{ts()} adaptive] batch too slow ({elapsed:.1f}s for {batch_len}) -> shrinking to {self.current}")
            elif elapsed < self.target_seconds * 0.5 and self.current < self.max_size:
                # comfortably fast - nudge up to find the real ceiling
                self.current = min(self.max_size, int(self.current * 1.15) + 1)

    def get(self) -> int:
        return self.current


async def process_batch(jobs: List[Dict[str, Any]], token: str, results: Dict[str, list],
                        session: aiohttp.ClientSession, stats: RunStats, token_label: str,
                        pacer: TokenPacer, sizer: "AdaptiveBatchSize | None" = None) -> List[Dict[str, Any]]:
    """
    Returns a list of jobs that need to be retried - either because the
    batch as a whole timed out (client-side, or server-side via 502/504), or
    because some aliases in it came back with a non-NOT_FOUND GraphQL error
    (e.g. RESOURCE_LIMITS_EXCEEDED) that isn't evidence the repo is missing.
    Keeps this function's side effects limited to `results`/`stats`.
    """
    if not jobs:
        return []

    query = build_graphql_query(jobs)
    headers = auth_headers(token)
    max_retries = 6
    # Client-side timeout set just above GitHub's real 10s server-side timeout
    # so we detect the server killing the request rather than timing out
    # first ourselves and masking the signal the adaptive sizer needs.
    client_timeout_seconds = HARD_SERVER_TIMEOUT_SECONDS + 3

    for attempt in range(max_retries):
        await pacer.wait_turn()
        req_start = time.monotonic()
        try:
            async with session.post(API, json={"query": query}, headers=headers, timeout=client_timeout_seconds) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except Exception:
                        raise Exception("Invalid JSON response")

                    if data is None:
                        raise Exception("Response JSON is None")

                    pacer.register_success()
                    elapsed = time.monotonic() - req_start

                    graph_data = data.get("data")
                    if graph_data is None:
                        # GitHub occasionally returns HTTP 200 with a null/malformed
                        # body during backend instability (often alongside upstream
                        # 502s). This is not a "batch too big" signal - it's a
                        # transient server fault - so we retry without feeding it
                        # to the adaptive sizer.
                        print(f"[{ts()} {token_label}] HTTP 200 but null/empty data body "
                              f"(Attempt {attempt+1}/{max_retries}) - transient GitHub fault, retrying")
                        await asyncio.sleep(3 + random.uniform(0, 2))
                        continue

                    if sizer is not None:
                        await sizer.record(len(jobs), elapsed, timed_out=False)

                    if "rateLimit" in graph_data and graph_data["rateLimit"] is not None:
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
                    elif "rateLimit" in graph_data:
                        print(f"[{ts()} {token_label}] rateLimit was null in response (likely degraded server), skipping rate check this round")

                    if "errors" in data:
                        alias_errors = classify_alias_errors(data["errors"])
                        retry_jobs: List[Dict[str, Any]] = []

                        if graph_data:
                            batch_not_found = 0
                            for i, job in enumerate(jobs):
                                rid = job["repo_id"]
                                alias = f"repo{i}"
                                repo_data = graph_data.get(alias)
                                if repo_data is not None:
                                    results["metadata"].append(repo_data_to_metadata_row(rid, repo_data))
                                    continue

                                types = alias_errors.get(alias, set())
                                if types and types.issubset(NOT_FOUND_ERROR_TYPES):
                                    # genuinely doesn't resolve - permanent
                                    results["errors"].append({"repo_id": rid, "error": "not_found_or_deleted"})
                                    batch_not_found += 1
                                else:
                                    # RESOURCE_LIMITS_EXCEEDED or anything else -
                                    # not evidence the repo is missing, retry it
                                    retry_jobs.append(job)

                            recovered = len(jobs) - batch_not_found - len(retry_jobs)
                            stats.jobs_ok += recovered
                            stats.jobs_error += batch_not_found
                            stats.not_found += batch_not_found
                            stats.requeued += len(retry_jobs)
                            if recovered < len(jobs) * 0.8:
                                sample = str(data["errors"])[:300]
                                print(f"[{ts()} {token_label}] low recovery {recovered}/{len(jobs)} ok, "
                                      f"{batch_not_found} not_found, {len(retry_jobs)} requeued - sample: {sample}")

                            if retry_jobs and sizer is not None:
                                # A batch this size couldn't be fully computed -
                                # same posture as a near-timeout, so the sizer
                                # should treat it that way even though the HTTP
                                # status itself was 200.
                                await sizer.record(len(jobs), HARD_SERVER_TIMEOUT_SECONDS * 0.85, timed_out=False)
                        else:
                            retry_jobs = list(jobs)
                            stats.requeued += len(retry_jobs)
                            sample = str(data["errors"])[:300]
                            print(f"[{ts()} {token_label}] batch failed entirely (no data) - sample: {sample} - requeuing")

                        stats.batches_done += 1
                        if stats.batches_done % 25 == 0:
                            print(stats.line())
                        return retry_jobs

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
                    return []

                elif response.status in (403, 429):
                    stats.rate_limited += 1
                    body = await response.text()
                    is_secondary = "secondary rate limit" in body.lower() or "abuse" in body.lower()
                    print(f"[{ts()} {token_label}] HTTP {response.status} (Attempt {attempt+1}/{max_retries}) "
                          f"{'[SECONDARY]' if is_secondary else '[PRIMARY/ABUSE]'}: {body[:200]}")

                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        base_sleep = int(retry_after) + 2
                    else:
                        base_sleep = min(30 * (2 ** attempt), 300)

                    if is_secondary:
                        await pacer.register_secondary_limit_hit(base_sleep)
                    else:
                        await asyncio.sleep(base_sleep)

                    continue

                elif response.status in (502, 504):
                    # CONFIRMED FROM GITHUB DOCS: 502/504 on this endpoint means
                    # your query exceeded the 10s server-side timeout - it is
                    # the SAME signal as asyncio.TimeoutError below, not a
                    # generic transient fault. Route it through the identical
                    # path: tell the sizer, and if the batch is big enough hand
                    # it back to be retried smaller instead of resubmitting the
                    # same oversized query and getting the same result.
                    elapsed = time.monotonic() - req_start
                    stats.exceptions += 1
                    if sizer is not None:
                        await sizer.record(len(jobs), elapsed, timed_out=True)
                    print(f"[{ts()} {token_label}] HTTP {response.status} after {elapsed:.1f}s for {len(jobs)} "
                          f"repos - GitHub's 10s server timeout (Attempt {attempt+1}/{max_retries})")
                    if len(jobs) > MIN_BATCH_SIZE * 2 and attempt < max_retries - 1:
                        stats.requeued += len(jobs)
                        return jobs
                    await asyncio.sleep(3 + random.uniform(0, 2))
                    continue

                elif response.status == 503:
                    # Not documented as the 10s-timeout signal - plain
                    # service-unavailable, keep as a simple transient retry.
                    print(f"[{ts()} {token_label}] HTTP 503. Retrying... (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(3 + random.uniform(0, 2))
                    continue

                else:
                    body = await response.text()
                    print(f"[{ts()} {token_label}] HTTP {response.status}: {body[:200]}")
                    for job in jobs:
                        results["errors"].append({"repo_id": job["repo_id"], "error": f"http_{response.status}"})
                    stats.jobs_error += len(jobs)
                    stats.http_errors += 1
                    stats.batches_done += 1
                    if stats.batches_done % 25 == 0:
                        print(stats.line())
                    return []

        except asyncio.TimeoutError:
            stats.exceptions += 1
            elapsed = time.monotonic() - req_start
            if sizer is not None:
                await sizer.record(len(jobs), elapsed, timed_out=True)
            print(f"[{ts()} {token_label}] timeout on batch of {len(jobs)} after {elapsed:.1f}s "
                  f"(Attempt {attempt+1}/{max_retries})")
            if len(jobs) > MIN_BATCH_SIZE * 2 and attempt < max_retries - 1:
                stats.requeued += len(jobs)
                return jobs
            await asyncio.sleep(3 + random.uniform(0, 2))
            continue

        except Exception as e:
            print(f"[{ts()} {token_label}] exception: {e!r}. Retrying... (Attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(3 + random.uniform(0, 2))
            continue

    print(f"[{ts()} {token_label}] Max retries exceeded for batch. Marking as error.")
    for job in jobs:
        results["errors"].append({"repo_id": job["repo_id"], "error": "max_retries_exceeded"})
    stats.jobs_error += len(jobs)
    stats.exceptions += 1

    stats.batches_done += 1
    if stats.batches_done % 25 == 0:
        print(stats.line())
    return []


async def token_worker(token: str, token_label: str, job_pool: List[Dict[str, Any]],
                        pool_lock: asyncio.Lock, pool_index: Dict[str, int],
                        results: Dict[str, list], stats: RunStats, github_session: aiohttp.ClientSession,
                        sizer: AdaptiveBatchSize, requeue_bin: List[Dict[str, Any]]):
    """
    Pulls dynamically-sized chunks from a shared job pool (list + index cursor)
    instead of a pre-sliced queue, so a shrink/grow from the adaptive sizer
    takes effect on the very next pull - not just next round. requeue_bin
    collects jobs from timed-out AND resource-limited batches; their split
    children go straight back through the shared pool via a second pass.
    """
    sem = asyncio.Semaphore(CONCURRENCY_PER_TOKEN)
    pacer = TokenPacer()

    async def take_chunk() -> List[Dict[str, Any]]:
        async with pool_lock:
            start = pool_index["i"]
            if start >= len(job_pool):
                return []
            size = sizer.get()
            end = min(start + size, len(job_pool))
            pool_index["i"] = end
            return job_pool[start:end]

    async def run_one(batch):
        async with sem:
            leftover = await process_batch(batch, token, results, github_session, stats, token_label, pacer, sizer=sizer)
            if leftover:
                requeue_bin.extend(leftover)

    tasks = []
    while True:
        batch = await take_chunk()
        if not batch:
            break
        tasks.append(asyncio.create_task(run_one(batch)))
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


async def run_one_round(tokens: List[str], jobs: List[Dict[str, Any]], stats: RunStats,
                         github_session: aiohttp.ClientSession, sizer: AdaptiveBatchSize) -> Dict[str, list]:
    """
    Runs one pass over `jobs`. Uses a shared pool (list + index cursor) so all
    tokens pull dynamically-sized chunks based on the live AdaptiveBatchSize,
    rather than a queue pre-sliced at a fixed BATCH_SIZE. Any jobs that timed
    out (client-side, or via 502/504) or came back RESOURCE_LIMITS_EXCEEDED
    mid-round are swept into a second pass at whatever (now-shrunk) batch size
    the sizer has converged to, until nothing is left or we give up.
    """
    results = {"metadata": [], "errors": []}
    remaining = list(jobs)
    max_sweeps = 4  # bounds worst-case: a pathological repo shouldn't loop forever

    for sweep in range(max_sweeps):
        if not remaining:
            break
        if sweep > 0:
            print(f"[{ts()} system] requeue sweep {sweep}: {len(remaining)} jobs left over "
                  f"from timeouts/resource limits")

        pool_lock = asyncio.Lock()
        pool_index = {"i": 0}
        requeue_bin: List[Dict[str, Any]] = []

        workers = [
            asyncio.create_task(
                token_worker(t, f"tok{idx+1}", remaining, pool_lock, pool_index,
                             results, stats, github_session, sizer, requeue_bin)
            )
            for idx, t in enumerate(tokens)
        ]
        await asyncio.gather(*workers)
        remaining = requeue_bin

    if remaining:
        print(f"[{ts()} system] {len(remaining)} jobs still unresolved after {max_sweeps} sweeps, marking as error.")
        for job in remaining:
            results["errors"].append({"repo_id": job["repo_id"], "error": "unresolved_after_max_sweeps"})
        stats.jobs_error += len(remaining)

    return results


async def main():
    tokens = load_tokens()
    if not tokens:
        print("WARNING: no tokens provided. Set GITHUB_TOKENS secret.")
        return

    print(f"[{ts()} init] Loaded {len(tokens)} token(s), {CONCURRENCY_PER_TOKEN} concurrent requests/token, "
          f"starting batch size {BATCH_SIZE} (adaptive, {MIN_BATCH_SIZE}-{MAX_BATCH_SIZE}), "
          f"min interval {MIN_REQUEST_INTERVAL}s/req, max runtime {MAX_RUNTIME_SECONDS}s")

    stats = RunStats()
    start = time.monotonic()
    round_num = 0
    # One shared sizer across the whole run - all tokens converge on the same
    # empirically-safe batch size together, and it persists across rounds so
    # each new round starts from what was already learned.
    sizer = AdaptiveBatchSize(BATCH_SIZE, MIN_BATCH_SIZE, MAX_BATCH_SIZE, TARGET_RESPONSE_SECONDS)

    connector = aiohttp.TCPConnector(limit=100)

    async with aiohttp.ClientSession() as hf_session, aiohttp.ClientSession(connector=connector) as github_session:
        background_uploads = []

        while True:
            elapsed = time.monotonic() - start
            if elapsed > MAX_RUNTIME_SECONDS:
                print(f"[{ts()} system] Runtime budget reached ({elapsed:.0f}s), stopping.")
                break

            round_num += 1
            jobs = await fetch_jobs(hf_session)
            if not jobs:
                print(f"[{ts()} system] No jobs received after retries. Queue is likely empty. Exiting.")
                break

            print(f"\n[{ts()} system] === Round {round_num} starting === Received {len(jobs)} jobs. "
                  f"Current adaptive batch size: {sizer.get()}. Total elapsed: {elapsed:.0f}s ===")

            round_start = time.monotonic()
            results = await run_one_round(tokens, jobs, stats, github_session, sizer)
            round_duration = time.monotonic() - round_start

            rate = len(jobs) / round_duration if round_duration > 0 else 0

            print(f"[{ts()} system] === Round {round_num} finished in {round_duration:.1f}s ({rate:.1f} repos/sec) ===")

            background_uploads.append(asyncio.create_task(post_work(hf_session, results)))
            print(stats.line())

            remaining_budget = MAX_RUNTIME_SECONDS - (time.monotonic() - start)
            print(f"[{ts()} system] Runtime budget remaining: {remaining_budget:.0f}s\n")

        if background_uploads:
            print(f"[{ts()} system] Waiting for final background uploads to complete...")
            await asyncio.gather(*background_uploads)

    total = stats.jobs_ok + stats.jobs_error
    print(f"[{ts()} system] === Run complete: {total} jobs processed | "
          f"{stats.jobs_ok} ok | {stats.jobs_error} error "
          f"({stats.not_found} not_found, {stats.http_errors} http_err batches, "
          f"{stats.exceptions} exceptions, {stats.rate_limited} rate_limit hits, "
          f"{stats.requeued} requeues) ===")

if __name__ == "__main__":
    asyncio.run(main())
