"""
Direct link-health check for hospital_jobs apply URLs.

What this catches (v2)
----------------------

For each active job's apply URL we run an escalating check:

  1. **HEAD with default UA**
     - 404 / 410 → deactivate immediately
     - 403       → retry with full browser UA + headers (some ATSes block
                   any non-browser request indiscriminately). If browser
                   retry returns 404/410, deactivate. If still 403,
                   classify as "blocked" and leave alone — the URL very
                   likely still works in a real browser.
     - 5xx / timeout / DNS / connection refused → "transient" — increment
                   dead_check_failures column; deactivate after 3
                   consecutive runs report this URL as transient.

  2. **Redirect-to-listing check**
     After HEAD follows redirects, if the final URL no longer contains
     the row's job_id (and the original URL did), or if the final URL
     matches a generic listing-page pattern, treat as dead — the ATS
     bounced us off the specific posting to the search page, which is
     ATS-speak for "this job no longer exists."

  3. **GET with browser UA + body-text dead-phrase scan**
     For URLs that returned 2xx on HEAD, follow up with a GET (real
     browser headers) and grep the response body for known dead-listing
     phrases ("no longer available", "position has been filled",
     "this requisition is no longer", etc.). Catches the case where the
     ATS returns 200 but renders a "this posting is closed" page.

     Workday URLs (myworkdayjobs.com) are EXCLUDED from this pass because
     Workday is a JS SPA — the response body is the same 100KB shell
     regardless of whether the job_id exists. Workday dead jobs are
     handled by the scraper-diff + age-based sweep in
     database.mark_inactive_jobs.

What we deliberately don't catch
--------------------------------
  - 403 that survives browser-UA retry. Almost always bot-rate-limit,
    not a dead URL. Logged as "blocked".
  - Truly intermittent 5xx that resolves on retry. Multi-run confirmation
    via dead_check_failures handles this — three strikes deactivates.
  - Workday SPA "this job is no longer accepting applications" pages.
    Would need a Playwright probe or Workday cxs API call; deferred.

Persistence
-----------
This module uses two columns on hospital_jobs:
  - dead_check_failures (int, default 0): consecutive transient failures.
  - last_dead_check_at  (timestamptz):    when we last verified the URL.

These are upserted at the end of each run as part of bulk batches.

CLI usage:
    python link_health.py                       # full scan of active rows
    python link_health.py --max-checks 5000     # cap the scan
    python link_health.py --stale-days 2        # only rows scraped >2d ago
    python link_health.py --skip-body           # HEAD-only, no GET pass
    python link_health.py --dry-run             # report only, no writes
"""

import argparse
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional
from urllib.parse import urlparse

import httpx
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# ── Tuning knobs ────────────────────────────────────────────────────────────
TIMEOUT_HEAD = 8         # seconds per HEAD
TIMEOUT_GET  = 12        # seconds per GET (body fetch)
WORKERS      = 40        # concurrent requests (lowered from v1's 50 because
                         # v2 does up to 3 requests per URL)
PAGE         = 1000      # PostgREST page size
FAILURE_DEACTIVATE_THRESHOLD = 3   # consecutive transient failures → deactivate

DEAD_STATUS = {404, 410}

DEFAULT_UA = "Mozilla/5.0 (compatible; WaypointRecruitLinkAudit/2.0)"
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
BROWSER_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

# ── Dead-phrase regex ──────────────────────────────────────────────────────
# Compiled once, case-insensitive. Tuned to ATS-specific copy where possible.
_DEAD_PHRASES = [
    r"no longer available",
    r"no longer accepting applications?",
    r"is no longer accepting",
    r"position has been filled",
    r"this position is no longer",
    r"job has been filled",
    r"this job has been filled",
    r"job has expired",
    r"posting has expired",
    r"this posting has expired",
    r"this requisition is no longer",
    r"the job you are looking for is no longer",
    r"the job you have selected is (?:no longer|not currently)",
    r"we['’]re sorry,? (?:but )?the job",
    r"we are sorry,? but the job",
    r"job is no longer available",
    r"this opportunity is no longer",
    r"currently not accepting applications",
    r"job not found",
    r"requisition (?:has been|is no longer)",
    r"this job is closed",
    r"the position you (?:are looking for|requested) is no longer",
    r"the job posting (?:you have selected )?has expired",
    r"we could not find the job you (?:were|are) looking for",
    r"sorry, this job is no longer",
    r"job not available",
    r"no longer exists",
]
DEAD_PHRASE_RE = re.compile("|".join(_DEAD_PHRASES), re.I)

# ── Redirect-to-listing patterns ───────────────────────────────────────────
# A final URL matching one of these (i.e. urllib followed redirects and
# landed here) means the ATS bounced us off the specific posting onto a
# generic search/index page.
_LISTING_PATTERNS = [
    r"/search/?(?:\?|$)",
    r"/search/jobs/?(?:\?|$)",
    r"/jobs/?(?:\?|$)",
    r"/browse/?(?:\?|$)",
    r"/listings/?(?:\?|$)",
    r"/careers/?(?:\?|$)",       # ATS root, no job id
    r"/results/?(?:\?|$)",
    # SmartRecruiters generic landing
    r"smartrecruiters\.com/[^/]+/?(?:\?|$)",
]
LISTING_RE = re.compile("|".join(_LISTING_PATTERNS), re.I)


def _is_workday_url(url: str) -> bool:
    """Workday SPA — HEAD/GET can't detect a dead job."""
    return ".myworkdayjobs.com" in (url or "")


def _looks_like_listing(original_url: str, final_url: str, job_id: str) -> bool:
    """True if final_url looks like a generic listing/search page rather
    than the specific job posting.

    Heuristics:
      - Original URL contained the job_id but final URL does NOT — the ATS
        redirected us away from the specific posting.
      - Final URL matches a generic-listing-page regex.
      - Final URL is just a bare ATS host (no path).
    """
    if not final_url:
        return False
    try:
        op = urlparse(original_url)
        fp = urlparse(final_url)
    except Exception:
        return False

    # Job_id was in original URL but stripped from final → likely redirected to listing
    if job_id and len(str(job_id)) >= 4:
        jid = str(job_id)
        if jid in original_url and jid not in final_url:
            return True

    # Bare ATS host (no path or just /) and host differs from origin
    if not fp.path or fp.path == "/":
        # If host also differs (origin → some-other-host.com) treat as redirect-away
        if fp.netloc and fp.netloc != op.netloc:
            return True

    # Generic listing path pattern
    if LISTING_RE.search(fp.path or ""):
        return True

    return False


# ── Supabase glue ──────────────────────────────────────────────────────────

def _client() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = (os.environ.get("SUPABASE_KEY", "")
           or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
    if not url or not key:
        raise ValueError("SUPABASE_URL / SUPABASE_KEY env vars must be set")
    return create_client(url, key)


def _fetch_active(db: Client, max_rows: Optional[int] = None,
                  stale_days: Optional[int] = None) -> list[dict]:
    """Return all active rows with URLs, paginating PostgREST."""
    out: list[dict] = []
    offset = 0
    cutoff_iso = None
    if stale_days is not None:
        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(days=stale_days)).isoformat()
    while True:
        try:
            q = (db.table("hospital_jobs")
                   .select("id,url,job_id,hospital_system,scraped_at,dead_check_failures")
                   .eq("is_active", True)
                   .order("scraped_at", desc=False))
            if cutoff_iso is not None:
                q = q.lt("scraped_at", cutoff_iso)
            resp = q.range(offset, offset + PAGE - 1).execute()
            rows = resp.data or []
        except Exception as e:
            logger.warning(f"Link health fetch (offset={offset}): {e}")
            break
        for r in rows:
            if not r.get("url"):
                continue
            out.append(r)
            if max_rows and len(out) >= max_rows:
                return out
        if len(rows) < PAGE:
            break
        offset += PAGE
    return out


# ── Per-URL check ──────────────────────────────────────────────────────────

# Verdict types: "dead", "blocked", "transient", "live"
# A dead verdict carries a "reason" string for logging.

def _check_one(job: dict, skip_body: bool = False) -> dict:
    """Run the escalating check on one URL. Returns:

      {"id": int, "verdict": str, "reason": str, "status": int|None,
       "final_url": str|None}
    """
    url = job["url"]
    job_id = job.get("job_id") or ""
    result = {"id": job["id"], "verdict": "live", "reason": "",
              "status": None, "final_url": None}

    with httpx.Client(timeout=TIMEOUT_HEAD, follow_redirects=True,
                      headers={"User-Agent": DEFAULT_UA}) as c:
        # ── Step 1: HEAD with default UA ──────────────────────────────
        try:
            r = c.head(url)
            result["status"] = r.status_code
            result["final_url"] = str(r.url)
        except httpx.TimeoutException:
            result["verdict"] = "transient"
            result["reason"] = "timeout"
            return result
        except (httpx.ConnectError, httpx.RemoteProtocolError) as e:
            result["verdict"] = "transient"
            result["reason"] = type(e).__name__
            return result
        except Exception as e:
            result["verdict"] = "transient"
            result["reason"] = type(e).__name__
            return result

        # 404 / 410 → dead immediately
        if r.status_code in DEAD_STATUS:
            result["verdict"] = "dead"
            result["reason"] = f"HTTP {r.status_code}"
            return result

        # 403 → retry with browser UA
        if r.status_code == 403:
            try:
                r2 = httpx.head(url, timeout=TIMEOUT_HEAD,
                                follow_redirects=True,
                                headers=BROWSER_HEADERS)
                result["status"] = r2.status_code
                result["final_url"] = str(r2.url)
                if r2.status_code in DEAD_STATUS:
                    result["verdict"] = "dead"
                    result["reason"] = f"HTTP {r2.status_code} (browser retry)"
                    return result
                if r2.status_code == 403:
                    result["verdict"] = "blocked"
                    return result
                # else: browser retry succeeded → fall through to other checks
                r = r2
            except Exception:
                result["verdict"] = "blocked"
                return result

        # 5xx → transient
        if r.status_code >= 500:
            result["verdict"] = "transient"
            result["reason"] = f"HTTP {r.status_code}"
            return result

        # 3xx that didn't terminate in 2xx (very rare with follow_redirects=True)
        if 300 <= r.status_code < 400:
            result["verdict"] = "transient"
            result["reason"] = f"HTTP {r.status_code}"
            return result

        # ── Step 2: redirect-to-listing check ─────────────────────────
        if _looks_like_listing(url, str(r.url), job_id):
            result["verdict"] = "dead"
            result["reason"] = f"redirect-to-listing → {r.url}"
            return result

        # ── Step 3: GET + body-text dead-phrase scan ──────────────────
        # Skip for Workday SPAs and when --skip-body is set
        if skip_body or _is_workday_url(url):
            return result  # verdict stays "live"

        # Only relevant for 2xx responses
        if not (200 <= r.status_code < 300):
            return result  # verdict stays "live" — we already passed dead checks

        try:
            g = httpx.get(url, timeout=TIMEOUT_GET, follow_redirects=True,
                          headers=BROWSER_HEADERS)
            result["status"] = g.status_code
            result["final_url"] = str(g.url)
            if g.status_code in DEAD_STATUS:
                result["verdict"] = "dead"
                result["reason"] = f"HTTP {g.status_code} (on GET)"
                return result
            if _looks_like_listing(url, str(g.url), job_id):
                result["verdict"] = "dead"
                result["reason"] = f"redirect-to-listing on GET → {g.url}"
                return result
            # Body phrase check (cap body to first 200KB to control memory)
            body_text = g.text[:200_000]
            m = DEAD_PHRASE_RE.search(body_text)
            if m:
                result["verdict"] = "dead"
                result["reason"] = f"body phrase: '{m.group(0)[:60]}'"
                return result
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError):
            # GET failed but HEAD succeeded → treat as live (HEAD is good enough signal)
            pass
        except Exception:
            pass

    return result


# ── Bulk DB updates ────────────────────────────────────────────────────────

def _apply_results(db: Client, results: list[dict],
                   row_by_id: dict[int, dict], dry_run: bool) -> dict:
    """Translate per-URL verdicts into bulk DB updates.

    Updates done:
      - dead             → is_active=false, dead_check_failures=0, last_dead_check_at=now
      - transient & prev_failures+1 >= THRESHOLD → same as dead
      - transient        → dead_check_failures += 1, last_dead_check_at=now
      - live / blocked   → dead_check_failures=0, last_dead_check_at=now
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    counts = {"dead": 0, "transient_deact": 0,
              "transient_pending": 0, "live": 0, "blocked": 0}
    dead_ids: list[int] = []
    live_ids: list[int] = []
    blocked_ids: list[int] = []
    # transient: group by NEW failure count so we can batch updates
    transient_by_new_count: dict[int, list[int]] = {}

    for r in results:
        rid = r["id"]
        verdict = r["verdict"]
        if verdict == "dead":
            dead_ids.append(rid)
            counts["dead"] += 1
        elif verdict == "live":
            live_ids.append(rid)
            counts["live"] += 1
        elif verdict == "blocked":
            blocked_ids.append(rid)
            counts["blocked"] += 1
        elif verdict == "transient":
            prev = int(row_by_id[rid].get("dead_check_failures") or 0)
            new = prev + 1
            if new >= FAILURE_DEACTIVATE_THRESHOLD:
                dead_ids.append(rid)
                counts["transient_deact"] += 1
            else:
                transient_by_new_count.setdefault(new, []).append(rid)
                counts["transient_pending"] += 1

    if dry_run:
        logger.info(f"[dry-run] would dead-deactivate {len(dead_ids):,} rows; "
                    f"reset {len(live_ids):,} live; mark "
                    f"{counts['transient_pending']:,} transient")
        return counts

    # Apply
    def _batch_update(ids: list[int], patch: dict, label: str):
        if not ids:
            return
        for i in range(0, len(ids), 500):
            chunk = ids[i:i+500]
            try:
                (db.table("hospital_jobs").update(patch)
                   .in_("id", chunk).execute())
            except Exception as e:
                logger.warning(f"Update {label} chunk {i//500}: {e}")

    # Dead — set is_active=false, reset counters
    _batch_update(dead_ids, {
        "is_active": False,
        "dead_check_failures": 0,
        "last_dead_check_at": now_iso,
    }, "dead")

    # Live & blocked — reset failures, stamp last check
    _batch_update(live_ids + blocked_ids, {
        "dead_check_failures": 0,
        "last_dead_check_at": now_iso,
    }, "live+blocked")

    # Transient (not yet at threshold) — set incremented failure count
    for new_count, ids in transient_by_new_count.items():
        _batch_update(ids, {
            "dead_check_failures": new_count,
            "last_dead_check_at": now_iso,
        }, f"transient(failures={new_count})")

    return counts


# ── Entry points ───────────────────────────────────────────────────────────

def run(max_rows: Optional[int] = None,
        stale_days: Optional[int] = None,
        dry_run: bool = False,
        skip_body: bool = False,
        log_progress: bool = True) -> dict:
    """Scan active hospital_jobs URLs; deactivate confirmed-dead.

    Returns a stats dict — see _apply_results for keys, plus per-verdict
    counts from the scan phase.
    """
    db = _client()

    logger.info(
        f"Link health v2: fetching active rows "
        f"(stale_days={stale_days}, max_rows={max_rows}, skip_body={skip_body})..."
    )
    rows = _fetch_active(db, max_rows=max_rows, stale_days=stale_days)
    n_total = len(rows)
    row_by_id = {r["id"]: r for r in rows}
    logger.info(f"Link health v2: scanning {n_total:,} URLs with {WORKERS} workers")

    if n_total == 0:
        return {"checked": 0, "deactivated": 0}

    results: list[dict] = []
    dead_so_far = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(_check_one, r, skip_body) for r in rows]
        done = 0
        for f in as_completed(futs):
            res = f.result()
            results.append(res)
            done += 1
            if res["verdict"] == "dead":
                dead_so_far += 1
            if log_progress and done % 5000 == 0:
                logger.info(
                    f"  link-health progress: {done:,}/{n_total:,} "
                    f"(dead so far: {dead_so_far:,})"
                )

    # Tally raw verdicts
    raw = {"dead": 0, "transient": 0, "live": 0, "blocked": 0}
    sample_dead: list[dict] = []
    for r in results:
        raw[r["verdict"]] += 1
        if r["verdict"] == "dead" and len(sample_dead) < 5:
            sample_dead.append(r)

    logger.info(
        f"Link health v2 raw verdicts: "
        f"dead={raw['dead']:,} transient={raw['transient']:,} "
        f"live={raw['live']:,} blocked={raw['blocked']:,}"
    )
    if sample_dead:
        logger.info("Sample dead URLs (first 5):")
        for s in sample_dead:
            logger.info(f"  id={s['id']} reason={s['reason']!r} final={s['final_url']}")

    # Bulk-apply
    updates = _apply_results(db, results, row_by_id, dry_run)

    summary = {**raw, **updates, "checked": n_total}
    logger.info(f"Link health v2 summary: {summary}")
    return summary


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Hospital job link-health checker v2")
    parser.add_argument("--max-checks", type=int, default=None,
                        help="Cap on URLs to check (default: all active)")
    parser.add_argument("--stale-days", type=int, default=None,
                        help="Only check rows scraped > N days ago")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write to DB, just report counts")
    parser.add_argument("--skip-body", action="store_true",
                        help="HEAD only; skip the GET+body-text pass")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        stats = run(max_rows=args.max_checks,
                    stale_days=args.stale_days,
                    dry_run=args.dry_run,
                    skip_body=args.skip_body)
    except KeyboardInterrupt:
        logger.warning("Link health: interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Link health: fatal {type(e).__name__}: {e}")
        sys.exit(1)

    print(stats)


if __name__ == "__main__":
    _cli()
