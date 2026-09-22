"""
Validate the apply links behind the front page's card pool (2026-09-22).

The front page and /api/front-pool deal cards from per-state pools: active
rows with a posted wage and apply_verified=true, newest first, 400 per state,
plus a national pool ordered by description length (app/page.js). Both
predicates are platform-level judgements (scraper._apply_verified_for trusts
a domain, not a URL) and say nothing about one link: the owner clicked five
front-page cards on 2026-09-22 and four were dead.

This pass fetches exactly the rows those pools serve, asks each apply URL
whether it still answers, retires the ones that are definitively gone (404 or
410, confirmed by a second request two seconds later) and stamps
last_dead_check_at on every row that answered, so the front page only deals
links that were live the night before. Runs as STEP 2e of the nightly, after
the sign-on pass.

Non-fatal and bounded: a 15-minute deadline, and it refuses to retire more
than MAX_DEAD_SHARE of the rows it checked (a network fault on the runner
must not read as ten thousand dead hospitals) or more than half of any one
system's checked rows (a site that answers 404 to a bot is a blocker, not a
mass expiry; it is logged as an ALARM and left alone).

Outbound requests go only to public http(s) hosts: no IP literals outside
the global ranges, no localhost, nothing under our own infrastructure's
suffixes, and every redirect hop is re-checked before it is followed.
"""
import asyncio
import ipaddress
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import aiohttp

logger = logging.getLogger(__name__)

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]
POOL_PER_STATE = 400            # FRONT_PAGE_POOL in the site's lib/front-page.js
CONCURRENCY = 32
TIMEOUT_SEC = 12
DEADLINE_SEC = 15 * 60
PATCH_BATCH = 300
DEAD_CODES = {404, 410}
MAX_DEAD_SHARE = 0.05           # retire nothing if more than 5% of checked rows look dead
SYSTEM_MAX_DEAD_SHARE = 0.5     # skip a system whose checked rows are half dead ...
SYSTEM_MIN_DEAD = 10            # ... once it has at least this many dead
MAX_HOPS = 4
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 WaypointLinkCheck/1.0")
BLOCKED_SUFFIXES = (".local", ".internal", ".localhost", "supabase.co", "supabase.in",
                    "railway.app", "railway.internal", "vercel.app", "waypointrecruit.com")


def _public_http_url(url) -> bool:
    """The outbound gate: http(s), a real public host, never our own."""
    if not url or not isinstance(url, str):
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https"):
        return False
    host = (p.hostname or "").lower().rstrip(".")
    if not host or host == "localhost" or host.endswith(BLOCKED_SUFFIXES):
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return "." in host
    return ip.is_global


def _env() -> tuple:
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not sb_url or not sb_key:
        raise SystemExit("SUPABASE_URL/_KEY not set")
    return sb_url, sb_key


async def _get_json(session, url, headers, tries=3):
    for attempt in range(1, tries + 1):
        try:
            async with session.get(url, headers=headers) as r:
                if r.status in (200, 206):
                    return await r.json()
                logger.warning(f"  pool fetch: HTTP {r.status} (attempt {attempt})")
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logger.warning(f"  pool fetch: {e} (attempt {attempt})")
        await asyncio.sleep(2 * attempt)
    return None


async def _fetch_pool(session, sb_url, headers) -> list:
    """The union of every state pool and the national pool, as the site
    queries them (app/api/front-pool/route.js, app/page.js): id, url, system."""
    base = (f"{sb_url}/rest/v1/hospital_jobs?select=id,url,hospital_system"
            f"&is_active=eq.true&city=neq.&wage_min=not.is.null&apply_verified=eq.true")
    rows = {}
    for st in STATES:
        chunk = await _get_json(
            session,
            f"{base}&state=eq.{st}&order=apply_verified.desc,scraped_at.desc&limit={POOL_PER_STATE}",
            headers)
        for r in chunk or []:
            rows[r["id"]] = r
        await asyncio.sleep(0.05)
    national = await _get_json(
        session,
        f"{base}&state=neq.&order=apply_verified.desc,desc_len.desc.nullslast&limit={POOL_PER_STATE}",
        headers)
    for r in national or []:
        rows[r["id"]] = r
    return list(rows.values())


async def _probe(session, url, method):
    async with session.request(method, url, allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC)) as r:
        return r.status, r.headers.get("Location", "")


async def _status(session, url) -> int:
    """Final status after up to MAX_HOPS gated redirects. HEAD first; many
    applicant systems refuse HEAD, so a refusal is asked again as GET (the
    body is never read). 0 means no answer, or a hop the gate refused."""
    cur = url
    for _ in range(MAX_HOPS + 1):
        if not _public_http_url(cur):
            return 0
        try:
            st, loc = await _probe(session, cur, "HEAD")
            if st in (400, 403, 405, 501) or st >= 500:
                st, loc = await _probe(session, cur, "GET")
        except (asyncio.TimeoutError, aiohttp.ClientError, ValueError):
            return 0
        if 300 <= st < 400 and loc:
            cur = urljoin(cur, loc)
            continue
        return st
    return 0


async def _check(session, sem, row) -> tuple:
    """(id, system, final status). A dead answer is confirmed once more
    before it counts, two seconds later."""
    async with sem:
        st = await _status(session, row.get("url", ""))
        if st in DEAD_CODES:
            await asyncio.sleep(2)
            again = await _status(session, row.get("url", ""))
            if again not in DEAD_CODES:
                st = again
    return row["id"], row.get("hospital_system") or "", st


async def _patch(session, sb_url, headers, ids, body) -> int:
    sent = 0
    hdrs = dict(headers, **{"Content-Type": "application/json", "Prefer": "return=minimal"})
    for i in range(0, len(ids), PATCH_BATCH):
        chunk = ids[i:i + PATCH_BATCH]
        csv = ",".join(str(x) for x in chunk)
        try:
            async with session.patch(f"{sb_url}/rest/v1/hospital_jobs?id=in.({csv})",
                                     headers=hdrs, json=body) as r:
                if r.status in (200, 204):
                    sent += len(chunk)
                else:
                    logger.warning(f"  patch batch at {i}: HTTP {r.status} {(await r.text())[:160]}")
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logger.warning(f"  patch batch at {i}: {e}")
    return sent


async def main() -> dict:
    sb_url, sb_key = _env()
    started = time.monotonic()
    sb_headers = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}"}
    summary = {"checked": 0, "live": 0, "dead": 0, "indeterminate": 0,
               "retired": 0, "stamped": 0, "unchecked": 0}
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*;q=0.8"},
                                     timeout=aiohttp.ClientTimeout(total=30),
                                     connector=connector) as session:
        rows = await _fetch_pool(session, sb_url, sb_headers)
        logger.info(f"  front-pool link check: {len(rows):,} rows ({len(STATES)} state pools + national)")
        if not rows:
            return summary

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [asyncio.create_task(_check(session, sem, r)) for r in rows]
        budget = max(30.0, DEADLINE_SEC - (time.monotonic() - started))
        done, pending = await asyncio.wait(tasks, timeout=budget)
        for t in pending:
            t.cancel()
        if pending:
            logger.warning(f"  deadline reached: {len(pending):,} rows unchecked; they keep their status")
        summary["unchecked"] = len(pending)

        live, dead, by_sys = [], [], {}
        for t in done:
            if t.cancelled() or t.exception() is not None:
                continue
            rid, system, st = t.result()
            summary["checked"] += 1
            d = by_sys.setdefault(system, [0, 0])
            d[0] += 1
            if st in DEAD_CODES:
                dead.append((rid, system))
                d[1] += 1
            elif 200 <= st < 300:
                live.append(rid)
            else:
                summary["indeterminate"] += 1
        summary["live"], summary["dead"] = len(live), len(dead)

        blocked = {s for s, (n, k) in by_sys.items()
                   if k >= SYSTEM_MIN_DEAD and k / max(n, 1) >= SYSTEM_MAX_DEAD_SHARE}
        for s in sorted(blocked):
            n, k = by_sys[s]
            logger.warning(f"  ALARM front-pool: {s}: {k}/{n} checked links answered 404/410; "
                           f"looks like a blocker, not expiry; nothing retired for this system")
        worst = sorted(((k, n, s) for s, (n, k) in by_sys.items() if k), reverse=True)[:8]
        for k, n, s in worst:
            logger.info(f"  dead links: {s}: {k}/{n}")

        retire = [rid for rid, s in dead if s not in blocked]
        if summary["checked"] and len(retire) > MAX_DEAD_SHARE * summary["checked"]:
            logger.warning(f"  ALARM front-pool: {len(retire):,} of {summary['checked']:,} checked links dead "
                           f"(over {MAX_DEAD_SHARE:.0%}); retiring nothing this run")
            retire = []

        now_iso = datetime.now(timezone.utc).isoformat()
        if retire:
            summary["retired"] = await _patch(session, sb_url, sb_headers, retire,
                                              {"is_active": False, "last_dead_check_at": now_iso})
        if live:
            summary["stamped"] = await _patch(session, sb_url, sb_headers, live,
                                              {"last_dead_check_at": now_iso})
    elapsed = time.monotonic() - started
    logger.info(f"  front-pool link check done in {elapsed:.0f}s: {summary}")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(main())
