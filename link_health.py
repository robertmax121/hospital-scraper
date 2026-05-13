"""
Direct link-health check for hospital_jobs apply URLs.

Why this exists
---------------
The diff-based deactivation in database.mark_inactive_jobs is a proxy: it
flags a job inactive when the hospital's scraper stops returning that job.
But proxies are leaky:

  - A hospital may dead-link a posting (return 404 on the apply URL) while
    still listing the job_id in their search API for another day or two.
  - A hospital's scraper module on our side may be broken, in which case
    the diff pass falls back to age-based, which takes 14 days to flag
    anything.
  - Some ATSes (UKG, Workday) return 200 OK with a "this posting is no
    longer available" page for retired jobs — the scraper happily ingests
    the URL, but a user clicking it sees a dead page.

This module does direct HEAD requests against every active job's apply URL
and flags is_active=false only on definitive 404 / 410 responses. Other
failure modes (timeouts, 5xx, DNS errors, 403 blocked-by-bot) are logged
but NOT deactivated — they're frequently transient or false positives.

Designed to run AFTER scheduler.py's mark_inactive_jobs. Idempotent.

CLI usage:
    python link_health.py                       # scan all active rows
    python link_health.py --max-checks 5000     # cap the scan
    python link_health.py --stale-days 2        # only rows not seen lately
    python link_health.py --dry-run             # report, don't deactivate
"""

import argparse
import logging
import os
import sys
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from supabase import create_client, Client

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
TIMEOUT = 8           # seconds per HEAD
WORKERS = 50          # concurrent HEAD requests
PAGE = 1000           # PostgREST page size
DEAD_STATUS = {404, 410}


def _client() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = (os.environ.get("SUPABASE_KEY", "")
           or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
    if not url or not key:
        raise ValueError("SUPABASE_URL / SUPABASE_KEY env vars must be set")
    return create_client(url, key)


def _fetch_active(db: Client, max_rows: Optional[int] = None,
                  stale_days: Optional[int] = None) -> Iterator[dict]:
    """Yield {id, url, hospital_system, scraped_at} for active rows with a URL.

    Args:
        max_rows:   stop after this many rows are yielded (default: no cap)
        stale_days: if set, only yield rows whose scraped_at is older than
                    NOW() - stale_days days. Useful for nightly runs where
                    you don't want to re-HEAD rows the scraper just saw.
    """
    offset = 0
    yielded = 0
    cutoff_iso = None
    if stale_days is not None:
        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(days=stale_days)).isoformat()

    while True:
        try:
            q = (db.table("hospital_jobs")
                   .select("id,url,hospital_system,scraped_at")
                   .eq("is_active", True)
                   .order("scraped_at", desc=False))  # oldest first → highest dead-link risk
            if cutoff_iso is not None:
                q = q.lt("scraped_at", cutoff_iso)
            resp = q.range(offset, offset + PAGE - 1).execute()
            rows = resp.data or []
        except Exception as e:
            logger.warning(f"Link health fetch (offset={offset}): {e}")
            return
        for r in rows:
            if not r.get("url"):
                continue
            yield r
            yielded += 1
            if max_rows and yielded >= max_rows:
                return
        if len(rows) < PAGE:
            return
        offset += PAGE


def _check_one(job: dict) -> tuple[int, Optional[int], Optional[str]]:
    """HEAD an apply URL with redirect-follow.

    Returns: (row_id, status_code or None, error_kind or None)

    - status_code present → server responded (we still consider 404/410
      dead even after redirects)
    - status_code None, error_kind set → transport-level failure
    """
    url = job["url"]
    req = urllib.request.Request(url, method="HEAD",
                                 headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return job["id"], resp.status, None
    except urllib.error.HTTPError as e:
        return job["id"], e.code, None
    except urllib.error.URLError as e:
        reason = e.reason
        kind = type(reason).__name__ if not isinstance(reason, str) else "URLError"
        # Treat socket timeouts under URLError as "timeout" for clearer logs
        if "timed out" in str(reason).lower() or kind == "timeout":
            kind = "timeout"
        return job["id"], None, kind
    except Exception as e:
        return job["id"], None, type(e).__name__


def run(max_rows: Optional[int] = None,
        stale_days: Optional[int] = None,
        dry_run: bool = False,
        log_progress: bool = True) -> dict:
    """Scan active hospital_jobs URLs and deactivate confirmed-dead ones.

    Returns:
        Stats dict:
          checked:       number of URLs HEAD'd
          dead:          404 / 410 responses (these get deactivated)
          blocked:       403 (kept active — ATS bot-blocked us, URL may still work)
          server_err:    5xx (kept active — transient)
          redirected:    3xx that resolved to a final 2xx
          live:          2xx
          timeouts:      no response within timeout (kept active)
          errors:        DNS / connection refused / other transport (kept active)
          deactivated:   rows successfully flagged is_active=false
    """
    db = _client()

    logger.info(
        f"Link health: fetching active rows "
        f"(stale_days={stale_days}, max_rows={max_rows})..."
    )
    jobs = list(_fetch_active(db, max_rows=max_rows, stale_days=stale_days))
    n_total = len(jobs)
    logger.info(f"Link health: scanning {n_total:,} URLs with {WORKERS} workers")

    counts = {
        "checked": n_total,
        "dead": 0,
        "blocked": 0,
        "server_err": 0,
        "redirected": 0,
        "live": 0,
        "timeouts": 0,
        "errors": 0,
        "deactivated": 0,
    }
    dead_ids: list[int] = []

    if n_total == 0:
        logger.info(f"Link health: nothing to scan — {counts}")
        return counts

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_check_one, j) for j in jobs]
        done = 0
        for f in as_completed(futs):
            id_, status, err = f.result()
            done += 1
            if log_progress and done % 5000 == 0:
                logger.info(
                    f"  link-health progress: {done:,}/{n_total:,} "
                    f"(dead so far: {len(dead_ids):,})"
                )
            if status in DEAD_STATUS:
                dead_ids.append(id_)
                counts["dead"] += 1
            elif status == 403:
                counts["blocked"] += 1
            elif status and status >= 500:
                counts["server_err"] += 1
            elif status and 300 <= status < 400:
                # urllib follows redirects → 3xx here means redirect chain
                # didn't terminate in a 2xx. Treat as ambiguous (not dead).
                counts["redirected"] += 1
            elif status and 200 <= status < 300:
                counts["live"] += 1
            elif err == "timeout":
                counts["timeouts"] += 1
            elif err is not None:
                counts["errors"] += 1

    # Deactivate confirmed-dead in batches
    if dead_ids and not dry_run:
        logger.info(
            f"Link health: deactivating {len(dead_ids):,} confirmed-dead "
            f"(HTTP 404 / 410) rows..."
        )
        BATCH = 500
        for i in range(0, len(dead_ids), BATCH):
            chunk = dead_ids[i:i + BATCH]
            try:
                (db.table("hospital_jobs")
                   .update({"is_active": False})
                   .in_("id", chunk)
                   .execute())
                counts["deactivated"] += len(chunk)
            except Exception as e:
                logger.warning(f"Link health: deactivation chunk {i//BATCH}: {e}")
    elif dead_ids and dry_run:
        logger.info(
            f"Link health (DRY RUN): would deactivate "
            f"{len(dead_ids):,} confirmed-dead (404/410) rows"
        )

    logger.info(f"Link health summary: {counts}")
    return counts


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--max-checks", type=int, default=None,
                        help="Cap on URLs to check (default: all active)")
    parser.add_argument("--stale-days", type=int, default=None,
                        help="Only check rows scraped > N days ago")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't deactivate, just report counts")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        stats = run(max_rows=args.max_checks,
                    stale_days=args.stale_days,
                    dry_run=args.dry_run)
    except KeyboardInterrupt:
        logger.warning("Link health: interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Link health: fatal {type(e).__name__}: {e}")
        sys.exit(1)

    print(stats)


if __name__ == "__main__":
    _cli()
