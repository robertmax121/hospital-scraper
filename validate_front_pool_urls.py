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

2026-10-07 (push 10), after the 10-06 run analysis:
  * a HEAD 404/410 is asked again with GET before it counts: Paylocity
    answers HEAD with 404 and GET with 200 on live postings, which read as
    the Brattleboro Retreat / Grace Cottage ALARMs and retired six live
    Spanish Peaks / Community First / Altus rows a night;
  * the pool query carries id.desc as its last tie-breaker: every row of a
    run shares one scraped_at, so the site's order (apply_verified desc,
    scraped_at desc, limit 400 per state) falls back to physical row order,
    and each stamp moved a checked row out of the pool (TX 27/400 checked).
    The site's app/api/front-pool/route.js (and app/page.js) need the same
    tie-breaker before the two pools coincide; this script does not touch
    the site;
  * 100-row PATCHes that split and retry on a 57014 statement timeout
    (900 live rows went unstamped at 300 a batch);
  * pm.healthcaresource.com /cs/<tenant>#/job/<id> links are unverifiable
    (the job id is in the fragment, which never reaches the server, so every
    one answers 200) and are counted, not requested;
  * the indeterminate rows are logged by final status.

2026-10-27 (Bounce QA re-analysis, item 4): job pages people actually open.
UF Health 29002321 (chaplain, Jacksonville) was live with Apply while its
employer page answered 404, and had never been checked: the pools above hold
only rows with a posted wage (and the 400 newest per state), and nothing else
fetches a staff job's link. Layer 4 and the sweep retire only rows the crawl
stops listing. So the job pages opened in the last LANDED_DAYS days
(waypoint_events page views, bots excluded by the stored flag; about 260
distinct active jobs a week on 2026-10-26) are checked too, first, with the
same rules: two 404/410 answers, the per-system ALARM, the 5% cap, the
HealthcareSource skip. A failure to read them never stops the pool check.
A retired row whose feed still lists it is set active again by the next
upsert (every listed row is sent is_active=true); this pass then retires it
again that night while it keeps its landings.
"""
import asyncio
import ipaddress
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse, urljoin

import aiohttp

SUPABASE_UA = "waypoint-scraper/1.0 (+https://jobs.waypointrecruit.com)"

import site_revalidate
import sb_keys

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
# 2026-10-07 (push 10): 300-row PATCHes of last_dead_check_at (an indexed
# column, so no HOT update: 32 index entries a row) hit the role's 8 s
# statement timeout three times on 10-06; 100 a batch, split and retried.
PATCH_BATCH = 100
PATCH_MIN_BATCH = 25
PATCH_RETRY_PAUSES = (2, 5)     # seconds, before each retry of a PATCH_MIN_BATCH piece
DEAD_CODES = {404, 410}
# HEAD answers asked again with GET before they count (the body is never
# read). 400/403/405/501 and 5xx are refusals of the method; 404/410 are
# asked again because Paylocity (and others) answer HEAD with 404 on a
# live posting (2026-10-07, push 10).
GET_AGAIN_CODES = {400, 403, 404, 405, 410, 501}
# A redirect onto a platform's "job not found" page is the posting's absence,
# not a live page. Paylocity answers HEAD with 404 on every posting; GET
# answers 200 on a live one and 302 -> /Recruiting/Jobs/JobNotFound (which
# then answers 200) on a closed one, so following that hop would stamp a
# closed job live (probed 2026-10-07, push 10). Path prefixes, lower case.
DEAD_REDIRECT_PATHS = ("/recruiting/jobs/jobnotfound",)
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


def _unverifiable(url) -> str:
    """Why no request can tell whether this link is live, or '' when one
    can. pm.healthcaresource.com serves /cs/<tenant>#/job/<id>: the job id
    sits in the fragment, which the browser never sends, so the server
    answers 200 for a posting that is gone (749 such rows were stamped live
    on 10-06). These rows are counted and left alone, neither stamped nor
    retired (2026-10-07, push 10)."""
    if not url or not isinstance(url, str):
        return ""
    try:
        p = urlparse(url)
    except Exception:
        return ""
    host = (p.hostname or "").lower()
    if host == "pm.healthcaresource.com" and p.path.startswith("/cs/") and p.fragment.startswith("/job"):
        return "fragment route"
    return ""


def _dead_redirect(url) -> bool:
    """True when a redirect target is a platform's job-not-found page
    (DEAD_REDIRECT_PATHS), which counts as a 404 for the posting."""
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        return False
    return any(path.startswith(p) for p in DEAD_REDIRECT_PATHS)


def _env() -> tuple:
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = sb_keys.supabase_key()
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


# The site's pool order plus id.desc (2026-10-07, push 10). Every row of a
# run shares one scraped_at, so without a tie-breaker Postgres returns the
# 400 in physical order, which a stamp (a non-HOT update) changes: 6,817 of
# the 13,990 rows stamped on 10-06 had left the pool by morning. The site's
# app/api/front-pool/route.js and app/page.js must order the same way for
# the checked rows to be the served rows; until then the two can differ on
# ties. The national pool ties on desc_len too (bodies cut at the old cap).
STATE_POOL_ORDER = "apply_verified.desc,scraped_at.desc,id.desc"
NATIONAL_POOL_ORDER = "apply_verified.desc,desc_len.desc.nullslast,id.desc"


async def _fetch_pool(session, sb_url, headers) -> list:
    """The union of every state pool and the national pool, as the site
    queries them (app/api/front-pool/route.js, app/page.js): id, url, system."""
    base = (f"{sb_url}/rest/v1/hospital_jobs?select=id,url,hospital_system"
            f"&is_active=eq.true&city=neq.&wage_min=not.is.null&apply_verified=eq.true")
    rows = {}
    for st in STATES:
        chunk = await _get_json(
            session,
            f"{base}&state=eq.{st}&order={STATE_POOL_ORDER}&limit={POOL_PER_STATE}",
            headers)
        for r in chunk or []:
            rows[r["id"]] = r
        await asyncio.sleep(0.05)
    national = await _get_json(
        session,
        f"{base}&state=neq.&order={NATIONAL_POOL_ORDER}&limit={POOL_PER_STATE}",
        headers)
    for r in national or []:
        rows[r["id"]] = r
    return list(rows.values())


# 2026-10-27 (Bounce QA re-analysis, item 4): the job pages people opened.
# The site logs a "pageview" event per page (waypoint_events, page_path
# "/jobs/<slug>-<id>", sometimes with a query string such as Reddit's click
# id); ua_is_bot is stamped at insert. Newest first, LANDED_MAX_ROWS distinct
# active jobs at most (about 260 a week on 2026-10-26, so the cap only bites
# on a traffic spike), read in pages by an id cursor.
LANDED_DAYS = 14
LANDED_MAX_ROWS = 3000
LANDED_EVENTS_PAGE = 1000
LANDED_EVENTS_MAX_PAGES = 40
LANDED_ROWS_CHUNK = 150         # ids per hospital_jobs read (URL length)
_JOB_PAGE_ID_RX = re.compile(r"^/jobs/[^/?#]+-(\d+)/?(?:[?#].*)?$")


def _landed_job_id(page_path):
    """hospital_jobs.id of a /jobs/<slug>-<id> page path, else None."""
    m = _JOB_PAGE_ID_RX.match(str(page_path or ""))
    return int(m.group(1)) if m else None


async def _fetch_landed(session, sb_url, headers, now=None) -> list:
    """Active hospital_jobs rows (id, url, system) whose job page was opened
    in the last LANDED_DAYS days, most recently opened first."""
    since = ((now or datetime.now(timezone.utc)) - timedelta(days=LANDED_DAYS)).isoformat()
    ids: list = []
    seen: set = set()
    cursor = None
    for _ in range(LANDED_EVENTS_MAX_PAGES):
        url = (f"{sb_url}/rest/v1/waypoint_events?select=id,page_path"
               f"&event_type=eq.pageview&created_at=gte.{quote(since)}&ua_is_bot=not.is.true"
               f"&page_path=like./jobs/*"
               + (f"&id=lt.{cursor}" if cursor is not None else "")
               + f"&order=id.desc&limit={LANDED_EVENTS_PAGE}")
        page = await _get_json(session, url, headers)
        if not page:
            break
        for ev in page:
            jid = _landed_job_id(ev.get("page_path"))
            if jid is not None and jid not in seen:
                seen.add(jid)
                ids.append(jid)
        cursor = page[-1].get("id")
        if len(page) < LANDED_EVENTS_PAGE or cursor is None or len(ids) >= LANDED_MAX_ROWS * 2:
            break
    rows = {}
    for i in range(0, len(ids), LANDED_ROWS_CHUNK):
        chunk = ids[i:i + LANDED_ROWS_CHUNK]
        got = await _get_json(
            session,
            f"{sb_url}/rest/v1/hospital_jobs?select=id,url,hospital_system"
            f"&is_active=eq.true&id=in.({','.join(str(x) for x in chunk)})",
            headers)
        for r in got or []:
            rows[r["id"]] = r
    return [rows[j] for j in ids if j in rows][:LANDED_MAX_ROWS]


async def _probe(session, url, method):
    async with session.request(method, url, allow_redirects=False,
                               timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC)) as r:
        return r.status, r.headers.get("Location", "")


async def _status(session, url) -> int:
    """Final status after up to MAX_HOPS gated redirects. HEAD first; many
    applicant systems refuse HEAD, so a refusal is asked again as GET (the
    body is never read), and so is a HEAD 404/410, since Paylocity answers
    HEAD with 404 on a live posting: only a row that looks dead pays the
    extra GET (2026-10-07, push 10). 0 means no answer, or a hop the gate
    refused."""
    cur = url
    for _ in range(MAX_HOPS + 1):
        if not _public_http_url(cur):
            return 0
        try:
            st, loc = await _probe(session, cur, "HEAD")
            if st in GET_AGAIN_CODES or st >= 500:
                st, loc = await _probe(session, cur, "GET")
        except (asyncio.TimeoutError, aiohttp.ClientError, ValueError):
            return 0
        if 300 <= st < 400 and loc:
            cur = urljoin(cur, loc)
            if _dead_redirect(cur):
                return 404
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


async def _patch(session, sb_url, headers, ids, body, landed: list | None = None) -> int:
    """PATCH body onto ids in PATCH_BATCH-row statements. 2026-10-07
    (push 10): a batch the database cannot finish (57014 statement timeout,
    any 5xx, 408/429, a network fault) is split in half down to
    PATCH_MIN_BATCH and the pieces are retried with backoff, the
    scraper._post_hospital_rows pattern; a piece that still fails is logged
    and skipped, and the loop continues. Returns rows landed; `landed`
    (2026-10-15) collects their ids."""
    hdrs = dict(headers, **{"Content-Type": "application/json", "Prefer": "return=minimal"})

    async def _send_once(chunk) -> tuple:
        """One PATCH. Returns (ok, retryable, detail)."""
        csv = ",".join(str(x) for x in chunk)
        try:
            async with session.patch(f"{sb_url}/rest/v1/hospital_jobs?id=in.({csv})",
                                     headers=hdrs, json=body) as r:
                if r.status in (200, 204):
                    return True, False, ""
                text = (await r.text())[:160]
                retryable = r.status >= 500 or r.status in (408, 429) or "57014" in text
                return False, retryable, f"HTTP {r.status} {text}"
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            return False, True, f"{type(e).__name__}: {e}"

    def _done(chunk) -> int:
        if landed is not None:
            landed.extend(chunk)
        return len(chunk)

    async def _send(chunk, where) -> int:
        ok, retryable, detail = await _send_once(chunk)
        if ok:
            return _done(chunk)
        if len(chunk) > PATCH_MIN_BATCH:
            logger.warning(f"  patch {where} ({len(chunk)} rows): {detail}; splitting in half")
            if retryable:
                await asyncio.sleep(PATCH_RETRY_PAUSES[0])
            mid = len(chunk) // 2
            return await _send(chunk[:mid], where + "a") + await _send(chunk[mid:], where + "b")
        for pause in (PATCH_RETRY_PAUSES if retryable else ()):
            await asyncio.sleep(pause)
            ok, retryable, detail = await _send_once(chunk)
            if ok:
                return _done(chunk)
            if not retryable:
                break
        logger.warning(f"  patch {where} ({len(chunk)} rows) FAILED, skipped: {detail}")
        return 0

    sent = 0
    for i in range(0, len(ids), PATCH_BATCH):
        sent += await _send(ids[i:i + PATCH_BATCH], f"batch at {i}")
    return sent


def _tally(results) -> tuple:
    """Sort (id, system, status) results into live ids, dead (id, system)
    pairs, per-system [checked, dead] counts and a Counter of the final
    status of every indeterminate row (0 = no answer or a hop the gate
    refused). Pure; main() logs the Counter (2026-10-07, push 10)."""
    live, dead, by_sys, indet = [], [], {}, Counter()
    for rid, system, st in results:
        d = by_sys.setdefault(system, [0, 0])
        d[0] += 1
        if st in DEAD_CODES:
            dead.append((rid, system))
            d[1] += 1
        elif 200 <= st < 300:
            live.append(rid)
        else:
            indet[st] += 1
    return live, dead, by_sys, indet


async def main() -> dict:
    sb_url, sb_key = _env()
    started = time.monotonic()
    # Supabase refuses a secret key (sb_secret_) sent with a browser User-Agent, and this
    # session's default User-Agent is a Chrome string: name the caller on every Supabase call.
    sb_headers = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}", "User-Agent": SUPABASE_UA}
    summary = {"checked": 0, "live": 0, "dead": 0, "indeterminate": 0,
               "retired": 0, "stamped": 0, "unchecked": 0, "unverifiable": 0,
               "landed": 0, "landed_extra": 0}
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*;q=0.8"},
                                     timeout=aiohttp.ClientTimeout(total=30),
                                     connector=connector) as session:
        pool = await _fetch_pool(session, sb_url, sb_headers)
        # 2026-10-27: the job pages people opened, checked first (a deadline
        # cuts the tail of the list). A failure to read them is logged and
        # the pool check goes on as before.
        landed: list = []
        try:
            landed = await _fetch_landed(session, sb_url, sb_headers)
        except Exception as e:
            logger.warning(f"  landed job pages not read (non-fatal; the pool check goes on): {e}")
        pool_ids = {r["id"] for r in pool}
        extra = [r for r in landed if r["id"] not in pool_ids]
        summary["landed"], summary["landed_extra"] = len(landed), len(extra)
        rows = extra + pool
        logger.info(f"  front-pool link check: {len(rows):,} rows ({len(STATES)} state pools + national, "
                    f"plus {len(extra):,} opened job pages outside them; {len(landed):,} opened in {LANDED_DAYS} days)")
        if not rows:
            return summary
        # 2026-10-07 (push 10): links no request can verify are counted and
        # left alone (no stamp, no retirement) instead of reading as live.
        checkable = [r for r in rows if not _unverifiable(r.get("url", ""))]
        summary["unverifiable"] = len(rows) - len(checkable)
        if summary["unverifiable"]:
            logger.info(f"  front-pool: {summary['unverifiable']:,} rows unverifiable by request "
                        f"(HealthcareSource fragment routes); not stamped, not retired")
        if not checkable:
            return summary

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [asyncio.create_task(_check(session, sem, r)) for r in checkable]
        budget = max(30.0, DEADLINE_SEC - (time.monotonic() - started))
        done, pending = await asyncio.wait(tasks, timeout=budget)
        for t in pending:
            t.cancel()
        if pending:
            logger.warning(f"  deadline reached: {len(pending):,} rows unchecked; they keep their status")
        summary["unchecked"] = len(pending)

        results = [t.result() for t in done if not t.cancelled() and t.exception() is None]
        live, dead, by_sys, indet = _tally(results)
        summary["checked"] = len(results)
        summary["indeterminate"] = sum(indet.values())
        summary["live"], summary["dead"] = len(live), len(dead)
        if indet:
            logger.info(f"  indeterminate by final status (0 = no answer / gate): "
                        f"{dict(indet.most_common())}")

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
            # 2026-10-15: the retired ids are noted for the site's page purge
            # (site_revalidate; the nightly flushes after this pass, STEP 2f).
            retired_ids: list = []
            summary["retired"] = await _patch(session, sb_url, sb_headers, retire,
                                              {"is_active": False, "last_dead_check_at": now_iso},
                                              landed=retired_ids)
            site_revalidate.note_retired(retired_ids, "front-pool")
        if live:
            summary["stamped"] = await _patch(session, sb_url, sb_headers, live,
                                              {"last_dead_check_at": now_iso})
    elapsed = time.monotonic() - started
    logger.info(f"  front-pool link check done in {elapsed:.0f}s: {summary}")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(main())
    site_revalidate.flush("front-pool")
