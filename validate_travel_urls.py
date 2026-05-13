"""
Validate every active travel_jobs URL by HEAD-request, deactivate the 4xx.

Designed to run on its own Railway cron (every ~4-6 hours) so the table
stays clean between full scrapes. Listings Vivian removes mid-day get
flagged as inactive within hours instead of waiting until the next
nightly run.

Idempotent. Safe to interrupt and restart.
"""
import asyncio
import logging
import os
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

CONCURRENCY = 30
TIMEOUT_SEC = 10
PAGE_SIZE = 1000
DEACT_BATCH = 500
BAD_CODES = {404, 410}              # treat these as definitively dead
INDETERMINATE_CODES = {0, 408, 429, 500, 502, 503, 504}  # transient; leave alone
USER_AGENT = "Mozilla/5.0 (compatible; WaypointURLValidator/1.0)"

# SSRF guard. The `url` field on travel_jobs comes from scraped Vivian/Aya
# data — if a row gets poisoned we don't want to issue requests to
# arbitrary hosts (cloud metadata, internal Supabase endpoints, etc.).
# Any host not on this list (or any URL not over plain https) is skipped.
ALLOWED_HOSTS = {
    "vivian.com",
    "www.vivian.com",
    "ayahealthcare.com",
    "www.ayahealthcare.com",
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
    """Page through is_active=true rows, return [{id, url}, ...]."""
    rows: list[dict] = []
    page = 0
    while True:
        frm = page * PAGE_SIZE
        to = frm + PAGE_SIZE - 1
        url = f"{sb_url}/rest/v1/travel_jobs?select=id,url&is_active=eq.true&order=id"
        headers = {
            "apikey": sb_key,
            "Authorization": f"Bearer {sb_key}",
            "Range": f"{frm}-{to}",
        }
        async with session.get(url, headers=headers) as r:
            if r.status not in (200, 206):
                logger.error(f"fetch active rows: HTTP {r.status}")
                return rows
            chunk = await r.json()
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < PAGE_SIZE:
                break
            page += 1
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
                if 300 <= r.status < 400:
                    loc = r.headers.get("Location", "")
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


async def main() -> None:
    sb_url, sb_key = _env()
    started = datetime.now(timezone.utc)
    logger.info(f"validate_travel_urls: starting at {started.isoformat()}")

    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout,
                                     connector=connector) as session:
        rows = await _fetch_active(session, sb_url, sb_key)
        logger.info(f"validating {len(rows):,} active rows")
        if not rows:
            return

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
        deact = await _deactivate_ids(session, sb_url, sb_key, bad)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info(f"deactivated {deact:,} rows in {elapsed:.1f}s")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(main())
