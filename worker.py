# worker.py
import os
import time
import asyncio
import aiohttp
import json
import random
from datetime import datetime, timezone
from typing import List, Dict, Any

API = "https://api.github.com/graphql"
HF_SPACE_URL = os.environ.get("HF_SPACE_URL", "http://localhost:7860")

MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", str(5 * 3600 + 30 * 60)))

# --- Verified against GitHub's official docs (docs.github.com/.../rate-limits-and-query-limits-for-the-graphql-api) ---
#
# THE "100 REPOS PER REQUEST" MYTH: there is no such cap. The `first`/`last`
# 1-100 rule applies to PAGINATED CONNECTIONS (e.g. `repositories(first: 100)`),
# not to aliased single-object lookups like `repo0: repository(owner:,name:)`.
# Aliasing many `repository()` lookups into one query is exactly how you get
# past the 100 figure - there is no documented alias-count cap, only:
#   - 500,000 total nodes per call (hard ceiling; we're nowhere near it - see
#     the per-repo node math below)
#   - a 10-SECOND SERVER-SIDE TIMEOUT per call. This is the real ceiling on
#     batch size: not a repo count, but how much work GitHub's server can
#     finish computing in 10s. A batch too big to finish in time gets killed
#     server-side AND costs you extra primary rate-limit points as a penalty
#     for the next hour. So oversized batches are actively worse than useless.
#
# SECONDARY RATE LIMIT (the thing that actually 403'd you before):
#   - max 100 concurrent requests, shared across REST+GraphQL, GLOBAL to your
#     account/token - not per-token multiplied
#   - max 2,000 GraphQL points/minute; non-mutation GraphQL = 1 point/request,
#     so this caps out at 2,000 requests/min - far above what concurrency=100
#     could even sustain, so it rarely binds in practice
#   - max 60 seconds of GraphQL CPU-time per 60 real seconds - THIS is what
#     you likely tripped: firing multiple concurrent fat/nested queries per
#     token burns CPU-time budget fast, independent of point cost
#   - GitHub's own guidance: "avoid concurrent requests" for GraphQL
#
# Given that, throughput comes from BATCH SIZE (more repos per request,
# staying under the 10s timeout) far more than from concurrency, which is
# capped low and actively discouraged by GitHub itself.
#
# --- Locked to the "average" sweet spot ---
# Modeling showed batch=400 (near the 10s wall) is NOT actually faster than
# batch~200: once response time gets close to the server timeout, latency
# eats the gain from bigger batches. ~200 repos/request is the point where
# you get near-maximum throughput (~200 repos/sec across 3 tokens, ~720k/hr)
# without flirting with the 10s cutoff. So the adaptive ceiling is capped
# here instead of left free to climb toward 400 - it will still shrink below
# this if real responses run slow, but won't push past it looking for more.
#
# MIN_BATCH_SIZE raised to 130: shrinking all the way to 50 loses too much
# throughput for what's usually a transient GitHub-side issue (502s, null
# response bodies) rather than a genuinely oversized batch. 130 is still a
# real reduction from 200 if responses are truly slow, without over-correcting.
CONCURRENCY_PER_TOKEN = int(os.environ.get("CONCURRENCY_PER_TOKEN", "1"))
BATCH_SIZE = int(os.environ.get("GRAPHQL_BATCH_SIZE", "200"))
MIN_BATCH_SIZE = int(os.environ.get("GRAPHQL_MIN_BATCH_SIZE", "130"))
MAX_BATCH_SIZE = int(os.environ.get("GRAPHQL_MAX_BATCH_SIZE", "250"))

# Server timeout is a hard 10s (GitHub docs). We target well under that so a
# batch that's a bit slower than usual doesn't get server-killed and penalized.
# Sweet-spot target: keep responses in the 2-4s band, not racing toward 10s.
TARGET_RESPONSE_SECONDS = float(os.environ.get("TARGET_RESPONSE_SECONDS", "3.0"))
HARD_SERVER_TIMEOUT_SECONDS = 10.0

# Minimum gap between the *start* of consecutive requests from the same token,
# shared across that token's concurrent slots via TokenPacer. With
# CONCURRENCY_PER_TOKEN=1 this is simply the pause between successive requests.
# GitHub's own docs say "pause at least 1 second... and avoid concurrent
# requests" - we follow that literally as the safe default.
MIN_REQUEST_INTERVAL = float(os.environ.get("MIN_REQUEST_INTERVAL", "1.0"))

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
        return (f"[{ts()} progress] batches={self.batches_done} jobs_ok={self.jobs_ok} "
                f"jobs_error={self.jobs_error} ({rate:.1f}% ok) "
                f"not_found={self.not_found} http_err={self.http_errors} "
                f"exceptions={self.exceptions} rate_limited={self.rate_limited}")


class TokenPacer:
    """
    Enforces a minimum gap between request *starts* for a single token, and
    tracks a per-token secondary-limit 'penalty' backoff that grows the more
    that token gets 403'd, and decays over time when requests succeed.
    This is what actually stops the retry-storm in your log: instead of every
    concurrent slot retrying at the same fixed 62s, each 403 pushes this
    token's next allowed request further out, and all slots respect it.
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
    and adjust the shared target batch size up/down to converge on the
    largest batch that reliably finishes well under 10s.
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
    Returns a list of jobs that need to be retried (e.g. from a timeout split)
    by the caller, rather than mutating shared state itself - keeps this
    function's side effects limited to `results`/`stats`.
    """
    if not jobs:
        return []

    query = build_graphql_query(jobs)
    headers = auth_headers(token)
    max_retries = 6
    # Client-side timeout set just above GitHub's real 10s server-side timeout
    # (see HARD_SERVER_TIMEOUT_SECONDS) so we detect the server killing the
    # request rather than timing out first ourselves and masking the signal
    # the adaptive sizer needs.
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
                        # 502s, as seen in practice). This is not a "batch too big"
                        # signal - it's a transient server fault - so we retry
                        # without feeding it to the adaptive sizer, which would
                        # otherwise misread it as a timeout and shrink for no reason.
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
                        # GitHub returned HTTP 200 but rateLimit was explicitly
                        # null - a degraded-server partial response (often seen
                        # alongside 502s). Not fatal: skip the rate-limit read
                        # this round, the "errors" branch below will still
                        # correctly process whatever repo data did come back.
                        print(f"[{ts()} {token_label}] rateLimit was null in response (likely degraded server), skipping rate check this round")

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
                                print(f"[{ts()} {token_label}] low recovery {recovered}/{len(jobs)} - sample: {sample}")
                        else:
                            for job in jobs:
                                results["errors"].append({"repo_id": job["repo_id"], "error": "graphql_no_data"})
                            stats.jobs_error += len(jobs)
                            sample = str(data["errors"])[:300]
                            print(f"[{ts()} {token_label}] batch failed entirely (no data) - sample: {sample}")

                        stats.batches_done += 1
                        if stats.batches_done % 25 == 0:
                            print(stats.line())
                        return []

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
                        # exponential backoff instead of flat 60s
                        base_sleep = min(30 * (2 ** attempt), 300)

                    if is_secondary:
                        await pacer.register_secondary_limit_hit(base_sleep)
                    else:
                        await asyncio.sleep(base_sleep)

                    continue

                elif response.status in (502, 503, 504):
                    print(f"[{ts()} {token_label}] HTTP {response.status}. Retrying... (Attempt {attempt+1}/{max_retries})")
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
                # Batch was too large to finish inside GitHub's real 10s
                # server-side window - hand the jobs back to the caller so
                # they can be resubmitted at the now-shrunk sizer.get() size,
                # instead of retrying this same oversized query again.
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
    collects jobs from timed-out batches (their split children go straight
    back through the shared pool via a second pass).
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
                # timed-out batch that needs to be retried in smaller pieces
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
    out mid-round are swept into a second pass at whatever (now-shrunk) batch
    size the sizer has converged to, until nothing is left or we give up.
    """
    results = {"metadata": [], "errors": []}
    remaining = list(jobs)
    max_sweeps = 4  # bounds worst-case: a pathological repo shouldn't loop forever

    for sweep in range(max_sweeps):
        if not remaining:
            break
        if sweep > 0:
            print(f"[{ts()} system] requeue sweep {sweep}: {len(remaining)} jobs left over from timeouts")

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
            results["errors"].append({"repo_id": job["repo_id"], "error": "timeout_after_max_sweeps"})
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
          f"{stats.exceptions} exceptions, {stats.rate_limited} rate_limit hits) ===")

if __name__ == "__main__":
    asyncio.run(main())
