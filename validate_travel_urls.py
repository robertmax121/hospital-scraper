"""
Validate every active travel_jobs URL by HEAD-request, deactivate the 4xx.

Designed to run on its own Railway cron (every ~4-6 hours) so the table
stays clean between full scrapes. Listings Vivian removes mid-day get
flagged as inactive within hours instead of waiting until the next
nightly run.

Idempotent. Safe to interrupt and restart.

2026-10-07 (push 10), after the 10-06 run analysis: the active rows are
paged by id cursor (the 20,000-id windows were cut to PostgREST's 1,000-row
cap, and 4,070 of 66,228 rows were never fetched); Nomad and AMN are on the
allowlist (36,242 rows, 55% of the board, were rejected before any request
and reported as indeterminate); rows off the allowlist are counted and
skipped before a task exists; and the checks run 120 in flight, interleaved
by host, so the whole board (~66k HEADs at the observed ~1.4 s each) fits in
about 12 minutes instead of ~23 for 28k.
"""
import asyncio
import logging
import os
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import aiohttp

logger = logging.getLogger(__name__)

# 2026-10-07 (push 10): 30 in flight did ~22 rows/s (1,250 s for the 28k
# Vivian/Aya rows). With Nomad and AMN the board is ~66k rows; at 120 in
# flight, spread over four hosts by _interleave_by_host and capped per host
# by the connector, that is ~88 rows/s, about 12-13 minutes.
CONCURRENCY = 120
PER_HOST = CONCURRENCY // 2
TIMEOUT_SEC = 10
PAGE_SIZE = 1000
DEACT_BATCH = 500
BAD_CODES = {404, 410}              # treat these as definitively dead
INDETERMINATE_CODES = {0, 408, 429, 500, 502, 503, 504}  # transient; leave alone
USER_AGENT = "Mozilla/5.0 (compatible; WaypointURLValidator/1.0)"

# SSRF guard. The `url` field on travel_jobs comes from scraped agency
# data — if a row gets poisoned we don't want to issue requests to
# arbitrary hosts (cloud metadata, internal Supabase endpoints, etc.).
# Any host not on this list (or any URL not over plain https) is skipped.
# 2026-10-07 (push 10): Nomad (23,341 active rows on 10-06, six agencies)
# and AMN (12,901) added; until then every one of their rows was reported
# as indeterminate without a request.
ALLOWED_HOSTS = {
    "vivian.com",
    "www.vivian.com",
    "ayahealthcare.com",
    "www.ayahealthcare.com",
    "nomadhealth.com",
    "www.nomadhealth.com",
    "amnhealthcare.com",
    "www.amnhealthcare.com",
}


def _is_allowed_url(url: str) -> bool:
    """Reject URLs whose scheme isn't https or whose host isn't in the
    allowlist. Used as the SSRF gate before every outbound HEAD."""
    if not url or not isinstance(url, str):
        return False
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme != "https":
        return False
    host = (p.hostname or "").lower()
    return host in ALLOWED_HOSTS


def _split_allowed(rows: list[dict]) -> tuple[list[dict], Counter]:
    """Rows whose URL passes the allowlist, and a Counter of the hosts of
    the rows that do not (2026-10-07, push 10): the skipped rows never get a
    task, so they are reported on their own line instead of swelling the
    indeterminate count."""
    allowed, skipped = [], Counter()
    for r in rows:
        url = r.get("url", "")
        if _is_allowed_url(url):
            allowed.append(r)
        else:
            try:
                skipped[(urlparse(url).hostname or "").lower() or "(no host)"] += 1
            except Exception:
                skipped["(bad url)"] += 1
    return allowed, skipped


def _interleave_by_host(rows: list[dict]) -> list[dict]:
    """Round-robin the rows across their hosts (2026-10-07, push 10). Ids
    are assigned per agency in upsert order, so the id-ordered fetch comes
    back in long single-host runs; checked in that order, 120 in flight would
    all land on one host at a time. Interleaved, the four hosts share the
    concurrency and the per-host connector cap holds."""
    by_host: dict[str, list[dict]] = {}
    for r in rows:
        by_host.setdefault((urlparse(r.get("url", "")).hostname or "").lower(), []).append(r)
    out: list[dict] = []
    queues = [iter(v) for v in by_host.values()]
    while queues:
        alive = []
        for q in queues:
            r = next(q, None)
            if r is not None:
                out.append(r)
                alive.append(q)
        queues = alive
    return out


def _env() -> tuple[str, str]:
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = (
        os.environ.get("SUPABASE_KEY", "")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    )
    if not sb_url or not sb_key:
        raise SystemExit("SUPABASE_URL/_KEY not set")
    return sb_url, sb_key


async def _fetch_active(session: aiohttp.ClientSession, sb_url: str, sb_key: str) -> list[dict]:
    """Collect is_active=true rows by id cursor, return [{id, url}, ...].

    Rewritten 2026-07-20: the old offset-Range pagination
    (`order=id` + `Range: frm-to`) re-scans from the top of the table on
    every page and started hitting the 8s statement timeout (HTTP 500)
    once the table passed ~250k rows, silently validating 0 rows. Windowed
    PK-range scans are bounded per statement regardless of table size, and
    each window retries so a transient 500 can't zero the run.

    2026-10-07 (push 10): the 20,000-id windows asked for limit=20000, but
    PostgREST answers at most 1,000 rows a request, so a window with more
    active rows than that was cut and nothing said so (8 windows, 4,070 of
    66,228 rows on 10-06). Now the same cursor walk Layer 4 uses: id >
    last, ordered by id, PAGE_SIZE a page, until an empty page (not a short
    one, so a server cap below PAGE_SIZE cannot end the walk early). A page
    that fails after its retries ends the walk with a WARNING; the rows
    fetched so far are still checked (deactivation is per row)."""
    headers = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}"}

    async def _get_json(url: str, tries: int = 4):
        for attempt in range(1, tries + 1):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status in (200, 206):
                        return await r.json()
                    logger.warning(f"fetch page: HTTP {r.status} (attempt {attempt})")
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"fetch page: {e} (attempt {attempt})")
            await asyncio.sleep(3 * attempt)
        return None

    rows: list[dict] = []
    last_id = 0
    while True:
        chunk = await _get_json(
            f"{sb_url}/rest/v1/travel_jobs?select=id,url"
            f"&is_active=eq.true&id=gt.{last_id}&order=id.asc&limit={PAGE_SIZE}"
        )
        if chunk is None:
            logger.warning(f"fetch page after id {last_id}: gave up after retries; "
                           f"checking the {len(rows):,} rows fetched so far")
            break
        if not chunk:
            break
        rows.extend(chunk)
        last_id = chunk[-1]["id"]
    return rows


async def _head_check(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                      row: dict) -> tuple[int, int]:
    """HEAD-check one URL. Returns (id, code) where code=0 means network
    error or SSRF-rejected (URL not on the allowlist).

    Redirects are NOT auto-followed. After HEAD, if the response is a
    3xx, we re-validate the Location header against the allowlist before
    issuing the follow-up request — that closes the redirect-to-internal
    SSRF path the previous version had.
    """
    if not _is_allowed_url(row.get("url", "")):
        return row["id"], 0
    async with sem:
        try:
            async with session.head(
                row["url"],
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC),
            ) as r:
                # If it's a redirect, validate the target host before
                # following — bounce off-allowlist redirects to "indeterminate".
                # 2026-10-07 (push 10): the Location is resolved against the
                # row's URL first: AMN answers every HEAD with a 301 to the
                # same path plus a trailing slash, as a relative Location,
                # which the allowlist (https + host) rejected as it stood.
                if 300 <= r.status < 400:
                    loc = urljoin(row["url"], r.headers.get("Location", ""))
                    if not _is_allowed_url(loc):
                        return row["id"], 0
                    async with session.head(
                        loc, allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC),
                    ) as r2:
                        return row["id"], r2.status
                return row["id"], r.status
        except (asyncio.TimeoutError, aiohttp.ClientError):
            return row["id"], 0


async def _deactivate_ids(session: aiohttp.ClientSession, sb_url: str,
                          sb_key: str, ids: list[int]) -> int:
    """Batch-deactivate by ID. Returns rows successfully PATCHed."""
    if not ids:
        return 0
    headers = {
        "apikey": sb_key,
        "Authorization": f"Bearer {sb_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    sent = 0
    for i in range(0, len(ids), DEACT_BATCH):
        chunk = ids[i:i + DEACT_BATCH]
        ids_csv = ",".join(str(x) for x in chunk)
        url = f"{sb_url}/rest/v1/travel_jobs?id=in.({ids_csv})"
        body = b'{"is_active":false}'
        async with session.patch(url, headers=headers, data=body) as r:
            if r.status in (200, 204):
                sent += len(chunk)
            else:
                logger.warning(
                    f"deactivate batch starting at {i}: HTTP {r.status} "
                    f"-- {(await r.text())[:200]}"
                )
    return sent



# 2026-10-07 (push 10 review): a host-wide blip (CDN or WAF answering 404 to
# every HEAD for a while) must not retire a whole agency's board in one
# night. A host whose dead share passes MAX_DEAD_SHARE with more than
# MAX_DEAD_MIN dead rows keeps its rows and logs an ALARM instead; the next
# night re-checks them. Mirrors the front-pool checker's guard.
MAX_DEAD_SHARE = 0.25
MAX_DEAD_MIN = 500


def _guard_mass_retire(bad: list, host_of: dict) -> tuple:
    """Split the dead ids into (retire, held_by_host) by the per-host guard."""
    totals: dict = {}
    for h in host_of.values():
        totals[h] = totals.get(h, 0) + 1
    dead: dict = {}
    for rid in bad:
        h = host_of.get(rid, '?')
        dead[h] = dead.get(h, 0) + 1
    held = {h: n for h, n in dead.items()
            if n > MAX_DEAD_MIN and n / max(totals.get(h, n), 1) > MAX_DEAD_SHARE}
    retire = [rid for rid in bad if host_of.get(rid, '?') not in held]
    return retire, held

async def main() -> None:
    sb_url, sb_key = _env()
    started = datetime.now(timezone.utc)
    logger.info(f"validate_travel_urls: starting at {started.isoformat()}")

    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2, limit_per_host=PER_HOST)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout,
                                     connector=connector) as session:
        fetched = await _fetch_active(session, sb_url, sb_key)
        # 2026-10-07 (push 10): rows off the allowlist get no task and their
        # own count; the rest are interleaved by host for the shared pool.
        rows, skipped = _split_allowed(fetched)
        logger.info(f"validating {len(rows):,} active rows "
                    f"({len(fetched):,} fetched, {sum(skipped.values()):,} skipped: host not allowed)")
        if skipped:
            logger.info(f"  skipped (host not allowed): {dict(skipped.most_common(6))}")
        if not rows:
            return
        rows = _interleave_by_host(rows)

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [asyncio.create_task(_head_check(session, sem, r)) for r in rows]

        bad: list[int] = []
        ok = 0
        indet = 0
        done = 0
        for fut in asyncio.as_completed(tasks):
            rid, code = await fut
            if code in BAD_CODES:
                bad.append(rid)
            elif 200 <= code < 400:
                ok += 1
            else:
                indet += 1
            done += 1
            if done % 5000 == 0:
                logger.info(
                    f"  progress: {done:,}/{len(rows):,}  "
                    f"ok={ok} bad={len(bad)} indet={indet}"
                )

        logger.info(
            f"validation complete: ok={ok} bad={len(bad)} indet={indet} "
            f"(of {len(rows):,})"
        )
        host_of = {r["id"]: (urlparse(r.get("url", "")).hostname or "").lower() for r in rows}
        bad, held = _guard_mass_retire(bad, host_of)
        for h, n in held.items():
            logger.warning(f"  ALARM travel validator: {h}: {n:,} of its rows answered 404/410 "
                           f"(over {int(MAX_DEAD_SHARE * 100)}%); looks like a blocker, not expiry; nothing retired for this host")
        deact = await _deactivate_ids(session, sb_url, sb_key, bad)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info(f"deactivated {deact:,} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(main())
