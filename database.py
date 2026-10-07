"""
Database — Supabase
Handles upsert, deactivation (Layer 4: multi-run miss confirmation),
and stats queries.
"""

import os
import logging
from datetime import datetime, timezone
from supabase import create_client, Client
from retire_guard import (BACKSTOP_DAYS, plan_layer4, load_yield_history, save_yield_history,
                          zero_yield_alarms)

logger = logging.getLogger(__name__)

# ── Schema. Run once in Supabase SQL editor; subsequent runs are no-ops. ────
SETUP_SQL = """
CREATE TABLE IF NOT EXISTS hospital_jobs (
    id              BIGSERIAL PRIMARY KEY,
    job_id          TEXT NOT NULL,
    hospital_system TEXT NOT NULL,
    hospital_name   TEXT,
    title           TEXT,
    location        TEXT,
    city            TEXT,
    state           TEXT,
    specialty       TEXT,
    job_type        TEXT,
    url             TEXT,
    posted_date     TEXT,
    description     TEXT,
    ats_platform    TEXT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT TRUE,
    consecutive_scrape_misses INT NOT NULL DEFAULT 0,
    UNIQUE(job_id, hospital_system)
);
CREATE INDEX IF NOT EXISTS idx_state      ON hospital_jobs(state);
CREATE INDEX IF NOT EXISTS idx_specialty  ON hospital_jobs(specialty);
CREATE INDEX IF NOT EXISTS idx_system     ON hospital_jobs(hospital_system);
CREATE INDEX IF NOT EXISTS idx_active     ON hospital_jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_ats        ON hospital_jobs(ats_platform);
CREATE INDEX IF NOT EXISTS idx_scrape_misses
    ON hospital_jobs(consecutive_scrape_misses) WHERE is_active = true;
"""

# PostgREST returns at most this many rows per request unless overridden via
# Range headers (which supabase-py's .range(a, b) sets). Pagination is
# explicit everywhere we'd otherwise be silently truncated.
PAGE = 1000

# Layer 4 threshold. A row must be missed by `MISS_THRESHOLD` consecutive
# scrape runs before we deactivate it. This is the tolerance for a single
# bad scrape — a network glitch, an ATS rate-limit, a transient module bug.
# At 3 with a nightly cron, worst-case lag is ~2 extra days on board for a
# genuinely-dead job (acceptable). False-deactivations from one-off scraper
# flakiness become near-zero.
MISS_THRESHOLD = 3


def client() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_KEY environment variables")
    return create_client(url, key)


def service_client() -> Client:
    """Client for privileged writes to RLS-protected tables (currently
    site_stats). Uses the service-role key, which bypasses row-level security.

    Falls back to SUPABASE_KEY only so local/dev runs don't crash outright —
    but in production that fallback is the anon key, which RLS will reject for
    these tables (error 42501). The site_stats counter only updates when
    SUPABASE_SERVICE_ROLE_KEY is present in the environment.
    """
    url = os.environ.get("SUPABASE_URL", "")
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
           or os.environ.get("SUPABASE_KEY", ""))
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
    # 2026-09-21: refresh_hospital_sitemap_cohort runs for minutes on 240k
    # rows and timed out at the client default twice; give the privileged
    # client a long read timeout (older supabase-py without ClientOptions
    # keeps the default).
    try:
        from supabase.lib.client_options import ClientOptions
        return create_client(url, key, options=ClientOptions(postgrest_client_timeout=300))
    except Exception:
        return create_client(url, key)


def upsert_jobs(jobs: list[dict]) -> dict:
    """Upsert jobs by (job_id, hospital_system). On conflict, refreshes the
    row's mutable fields (including scraped_at). consecutive_scrape_misses
    is NOT reset here — mark_inactive_jobs handles that explicitly for
    found rows so the reset is symmetric with the miss-increment.

    2026-09-24: the nightly no longer calls this. scheduler.run() used to
    re-send every row scrape() had already upserted through
    scraper._upsert_hospital_jobs_to_supabase (the same dicts, already
    aliased and stamped), doubling the write load on a 32-index table. That
    function now dedupes, splits and retries itself, and scrape() re-sends
    only the rows it could not land (scraper.retry_failed_hospital_upsert).
    """
    db = client()
    # Dedupe on the conflict key BEFORE batching (2026-08-21). Two sources
    # can emit the same (job_id, hospital_system) — e.g. Northwell's three
    # Oracle portals aliased to one system name — and Postgres raises 21000
    # ("ON CONFLICT DO UPDATE cannot affect row a second time") when both
    # land in one statement, losing the whole 100-row batch. Last one wins,
    # but prefer a row that carries a description over one that doesn't.
    by_key = {}
    for j in jobs:
        key = (j.get("hospital_system"), j.get("job_id"))
        prev = by_key.get(key)
        if prev is not None and (prev.get("description") or "") and not (j.get("description") or ""):
            continue
        by_key[key] = j
    if len(by_key) < len(jobs):
        logger.info(f"upsert_jobs: deduped {len(jobs) - len(by_key)} duplicate (system, job_id) rows before batching")
    jobs = list(by_key.values())

    inserted, errors = 0, 0
    for i in range(0, len(jobs), 100):
        batch = jobs[i:i+100]
        try:
            (db.table("hospital_jobs")
               .upsert(batch, on_conflict="job_id,hospital_system")
               .execute())
            inserted += len(batch)
        except Exception as e:
            logger.error(f"Batch {i//100} upsert error: {e}")
            errors += len(batch)
    return {"inserted": inserted, "errors": errors}


def mark_inactive_jobs(current_jobs: list[dict],
                       miss_threshold: int = MISS_THRESHOLD,
                       exclude_systems: set[str] | None = None,
                       complete_systems: set[str] | None = None) -> dict:
    """Layer 4 deactivation: multi-run miss confirmation.

    2026-10-07 (push 10): `complete_systems` (canonical labels, from
    scraper.COMPLETE_SYSTEMS) read their whole board tonight, so the yield
    guard is bypassed for them and their unseen rows count as usual. The
    guard's zero-yield streaks are kept in state/layer4_yields.json and a
    system dark two runs in a row is logged as an ALARM (retire_guard.
    zero_yield_alarms). BACKSTOP_EXEMPT systems keep their unseen rows frozen
    past the backstop while guarded.

    2026-09-16: `exclude_systems` (canonical hospital_system labels) are left
    untouched: no miss bump, no reset, no deactivation. The per-system sweep
    already skips PARTIAL_SYSTEMS (a system whose crawl came back partial or
    empty), but this pass did not, so a system the nightly cannot crawl at
    all decayed here instead: HCA Healthcare is crawled from a residential IP
    by hca_local_push.py (Cloudflare 403s the Railway egress), and the 16,968
    HCA rows it maintains were reaching miss_threshold three nights after
    every local push (4,717 deactivated on 2026-09-16 alone).

    2026-09-24: yield guard (retire_guard.py, the same numbers as the
    sweep's guard). A system whose run yielded 0 rows, or under 80% of its
    active rows when it has 20+, does not have its unseen rows bumped: they
    keep their miss count. Seen rows are still reset. A row nobody has seen
    for BACKSTOP_DAYS (scraped_at) loses that protection and is counted as
    usual, so a system that is really gone still retires. On 09-24, 17
    systems (4,279 rows, 26 CMS hospitals) had answered nothing and were one
    or two runs from retirement although their boards were live.

    Per active row each scrape run:
      - Row's (system, job_id) IS in this scrape → reset misses to 0.
      - Row's (system, job_id) NOT in this scrape → increment misses
        (unless the system is yield-guarded and the row was seen within
        BACKSTOP_DAYS).
      - On increment, if misses >= miss_threshold → deactivate.

    This replaces the previous strict-diff-on-first-miss approach. The
    earlier version was correct in spirit (the scraper is the source of
    truth for what the hospital is currently listing) but too punitive
    in practice: one flaky run could deactivate thousands of legitimately
    live jobs, which then take days to re-appear as scrapers find them
    again. By requiring N consecutive misses we tolerate a single bad
    scrape per row without losing it from the board.

    Pagination of the active-row fetch is explicit. The 2026-05-12
    nightly's "98 stale" result was actually a 1000-row PostgREST page
    cap silently truncating the diff — fixed here too.

    Args:
        current_jobs:   every row this scrape run produced. We diff their
                        (hospital_system, job_id) tuples against active.
        miss_threshold: number of consecutive misses required to
                        deactivate. Default 3.

    Returns:
        Stats dict with:
            deactivated:      rows that reached miss_threshold this run
            misses_pending:   rows whose miss count rose but isn't yet
                              at threshold
            reset_to_found:   rows confirmed present in this scrape
            current_keys:     count of (system, job_id) tuples in scrape
            active_before:    active rows before the pass
            guarded_systems:  systems the yield guard held tonight
            guard_held_rows:  their unseen rows kept at their miss count
            guard_backstop_rows: their unseen rows past the backstop
    """
    db = client()

    # Build canonical "what's in this run" key set.
    current_keys: set[tuple[str, str]] = set()
    for j in current_jobs:
        s = j.get("hospital_system")
        k = j.get("job_id")
        if s and k:
            current_keys.add((s, str(k)))
    logger.info(f"  Layer 4: {len(current_keys):,} keys in current scrape")

    # Page through ALL active rows using CURSOR pagination on id.
    # Previously used .range(offset, offset+PAGE-1) which broke when
    # PostgREST returned 999 rows for a Range request of 0-999: the
    # `len(rows) < PAGE` termination check fired on the first page and
    # the loop exited after fetching ~1.4% of the table. Cursor
    # pagination (WHERE id > last_seen ORDER BY id LIMIT PAGE) doesn't
    # care about the per-request size — it just keeps advancing until
    # an empty page comes back.
    active: list[dict] = []
    last_id = 0
    fetch_failed = False
    while True:
        # Retry each page: right after the nightly's ~115k-row upsert wave the
        # first page reliably times out (seen 2026-07-29, "read operation
        # timed out"), and the old single-try + break turned the whole pass
        # into a silent no-op on an "empty" table.
        rows = None
        for attempt in range(4):
            try:
                resp = (db.table("hospital_jobs")
                          .select("id,job_id,hospital_system,consecutive_scrape_misses,scraped_at")
                          .eq("is_active", True)
                          .gt("id", last_id)
                          .order("id", desc=False)
                          .limit(PAGE)
                          .execute())
                rows = resp.data or []
                break
            except Exception as e:
                logger.warning(f"  Fetch active page (last_id={last_id}, attempt {attempt + 1}/4): {e}")
                import time as _t
                _t.sleep(3 * (attempt + 1))
        if rows is None:
            fetch_failed = True
            break
        if not rows:
            break
        active.extend(rows)
        last_id = rows[-1]["id"]
    if fetch_failed:
        # Refuse to run the pass on a partial/empty picture of the table —
        # incrementing misses against rows we failed to page over would be
        # wrong, and pretending the table is empty hides the failure.
        logger.error(f"  Layer 4 ABORTED: active-row fetch failed after retries "
                     f"({len(active):,} rows fetched before failure). "
                     f"No misses bumped, nothing deactivated this run.")
        return {"deactivated": 0, "misses_pending": 0, "reset_to_found": 0,
                "current_keys": len(current_keys), "active_before": len(active),
                "aborted": True}
    logger.info(f"  Layer 4: {len(active):,} active rows in DB")

    # Categorize each active row (pure logic in retire_guard.plan_layer4):
    # seen -> reset to 0; unseen -> bump, retire at the threshold; rows of
    # exclude_systems untouched; unseen rows of a yield-guarded system (0
    # rows tonight, or under 80% of 20+ active rows) keep their count unless
    # nobody has seen them for BACKSTOP_DAYS.
    plan = plan_layer4(active, current_keys, miss_threshold, exclude_systems,
                       complete_systems=complete_systems)
    found_ids: list[int] = plan["found_ids"]
    deactivate_ids: list[int] = plan["deactivate_ids"]
    bump_by_new_count: dict[int, list[int]] = plan["bump_by_new_count"]
    excluded_n = plan["excluded_rows"]
    guarded = plan["guarded"]
    bypassed = plan["complete_bypassed"]
    for system in sorted(bypassed, key=lambda s: -bypassed[s]["active"]):
        c = bypassed[system]
        logger.info(f"  LAYER4 complete crawl {system}: {c['reason']} {c['yield']}/{c['active']} "
                    f"(yield/active) but the adapter read the whole board; unseen rows counted as usual")
    for system in sorted(guarded, key=lambda s: -guarded[s]["active"]):
        g = guarded[system]
        logger.warning(f"  LAYER4 GUARD {system}: {g['reason']} {g['yield']}/{g['active']} "
                       f"(yield/active); {g['frozen']} unseen rows keep their miss count, "
                       f"{g['backstop']} past the {BACKSTOP_DAYS}-day backstop counted as usual"
                       + (f", {g['exempt']} past it kept frozen (BACKSTOP_EXEMPT)" if g.get("exempt") else ""))
    if guarded:
        logger.warning(f"  Layer 4 yield guard: {len(guarded)} system(s), "
                       f"{plan['frozen_rows']:,} rows held, {plan['backstop_rows']:,} past the backstop")

    # 2026-10-07 (push 10): ALARM on a system that is zero-yield two runs in a
    # row (or whose newest row is older than the previous run). Bookkeeping
    # only; it never changes the plan and never fails the pass.
    alarms: list[dict] = []
    try:
        now = datetime.now(timezone.utc)
        history, alarms = zero_yield_alarms(guarded, load_yield_history(), now)
        if not save_yield_history(history):
            logger.warning("  Layer 4: could not save the zero-yield streaks (state/layer4_yields.json); "
                           "streaks restart next run")
        for a in alarms:
            age = f"{a['age_h']} h ago" if a["age_h"] is not None else "unknown"
            logger.warning(f"  ALARM LAYER4 {a['system']}: zero yield {a['runs']} run(s) in a row, "
                           f"{a['active']} active rows, newest row stamped {a['newest_seen']} ({age}); "
                           f"the adapter is broken or the employer left this board")
    except Exception as e:
        logger.warning(f"  Layer 4 zero-yield alarm bookkeeping failed (non-fatal): {e}")

    # Helper for batched updates.
    def _batch_update(ids: list[int], patch: dict, label: str) -> int:
        n = 0
        for i in range(0, len(ids), 500):
            chunk = ids[i:i+500]
            try:
                (db.table("hospital_jobs").update(patch)
                   .in_("id", chunk).execute())
                n += len(chunk)
            except Exception as e:
                logger.warning(f"  Update {label} chunk {i//500}: {e}")
        return n

    # Apply: reset found, deactivate threshold-hit, bump others.
    reset_n = _batch_update(found_ids, {"consecutive_scrape_misses": 0}, "reset-found")
    deact_n = _batch_update(deactivate_ids,
                            {"is_active": False, "consecutive_scrape_misses": 0},
                            "deactivate-at-threshold")
    bump_n = 0
    for new_count, ids in bump_by_new_count.items():
        bump_n += _batch_update(ids,
                                {"consecutive_scrape_misses": new_count},
                                f"bump-to-{new_count}")

    summary = {
        "deactivated":     deact_n,
        "misses_pending":  bump_n,
        "reset_to_found":  reset_n,
        "current_keys":    len(current_keys),
        "active_before":   len(active),
        "excluded_rows":   excluded_n,
        "excluded_systems": sorted(exclude_systems or []),
        "guarded_systems": sorted(guarded),
        "guard_held_rows": plan["frozen_rows"],
        "guard_backstop_rows": plan["backstop_rows"],
        "complete_bypassed": sorted(bypassed),
        "zero_yield_alarms": [a["system"] for a in alarms],
    }
    logger.info(f"Layer 4 deactivation: {summary}")
    return summary


def active_total_after_passes(layer4: dict | None, front_pool: dict | None) -> int | None:
    """The active hospital_jobs count the nightly already knows, with no
    scan (2026-10-07, push 10): Layer 4 paged every active row
    (active_before) and retired `deactivated` of them, and the front-pool
    check retired `retired` more; nothing else in the run flips is_active
    after Layer 4. On 10-06: 350,430 - 228 - 31 = 350,171, the live count.
    None when Layer 4 aborted (its page is partial) or a figure is missing,
    which the caller prints as "unknown", never as 0."""
    if not layer4 or layer4.get("aborted"):
        return None
    before = layer4.get("active_before")
    deact = layer4.get("deactivated")
    if not isinstance(before, int) or not isinstance(deact, int):
        return None
    retired = (front_pool or {}).get("retired") or 0
    if not isinstance(retired, int):
        return None
    return before - deact - retired


def _count_active_by_cursor() -> int | None:
    """Tally active rows by id cursor, the Layer 4 walk, with the same
    per-page retries and on the 300 s service client (2026-10-07, push 10:
    the plain client's ~5 s read timeout killed the third page on 10-04 and
    10-06, with no retry, and one exception ended the whole function).
    Only the fallback for a standalone call or an aborted Layer 4; the
    nightly passes the arithmetic total. None when a page fails after its
    retries."""
    try:
        db = service_client()
    except Exception as e:
        logger.warning(f"get_stats: no client for the fallback count ({e})")
        return None
    total = 0
    last_id = 0
    while True:
        rows = None
        for attempt in range(4):
            try:
                resp = (db.table("hospital_jobs")
                          .select("id")
                          .eq("is_active", True)
                          .gt("id", last_id)
                          .order("id", desc=False)
                          .limit(PAGE)
                          .execute())
                rows = resp.data or []
                break
            except Exception as e:
                logger.warning(f"get_stats: count page (last_id={last_id}, attempt {attempt + 1}/4): {e}")
                import time as _t
                _t.sleep(3 * (attempt + 1))
        if rows is None:
            logger.error(f"get_stats: fallback count failed after retries ({total:,} rows tallied before)")
            return None
        if not rows:
            return total
        total += len(rows)
        last_id = rows[-1]["id"]


def _write_site_stats(total: int) -> bool:
    """Persist the count to the single-row site_stats summary table. The
    public website reads THIS one row for its hero-pill count instead of
    running a live exact COUNT (which times out at this table size).

    site_stats has RLS enabled with a public-read-only policy, so the write
    MUST go through the service-role key (service_client), which bypasses
    RLS. The plain client() is rejected here (42501), which is exactly how
    this counter silently went stale before."""
    if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        logger.warning(
            "SUPABASE_SERVICE_ROLE_KEY not set; the site_stats write will be "
            "rejected by RLS and the homepage job counter will NOT update. "
            "Add it to the cron environment (Supabase > Settings > API > "
            "service_role key).")
    try:
        (service_client().table("site_stats")
           .upsert({"id": 1,
                    "total_active_jobs": total,
                    "updated_at": datetime.now().isoformat()})
           .execute())
        logger.info(f"site_stats updated: total_active_jobs={total:,}")
        return True
    except Exception as e:
        logger.warning(f"site_stats write failed (non-fatal): {e}")
        return False


def _refresh_sitemap_cohort() -> None:
    """Sticky sitemap cohort refresh (2026-08-24): admit tonight's new
    quality-bar qualifiers, evict only deactivated jobs. Kills the
    quality-gate flapping that swung GSC "known pages" by thousands per
    day. Non-fatal; the website's sitemap RPC reads the table.

    2026-10-07 (push 10): runs on its own, not inside the count's try
    block. Note that through PostgREST the call runs under the
    authenticator role's 8 s statement_timeout (the function's own "set
    local statement_timeout" cannot re-arm a timer that is already
    running), and the refresh has taken 105-115 s since late September, so
    this call has failed every night since 09-26 and will go on failing
    until the function is cheaper; pg_cron job 4 (sql/push10_cohort_cron.sql
    moves it to 05:30 UTC with a 600 s timeout) is the refresh of record."""
    try:
        # Railway's older supabase-py requires the params argument
        # explicitly; the no-arg form raised "Client.rpc() missing 1
        # required positional argument: 'params'" every night, so the
        # cohort never admitted new jobs after the 8/24 seed (confirmed
        # 2026-08-26: cohort_joined_tonight=0 until a manual refresh).
        res = service_client().rpc("refresh_hospital_sitemap_cohort", {}).execute()
        logger.info(f"hospital_sitemap_cohort refreshed: {res.data}")
    except Exception as e:
        logger.warning(f"hospital_sitemap_cohort refresh failed (non-fatal; pg_cron job 4 is the "
                       f"refresh of record): {e}")


def get_stats(total_active: int | None = None) -> dict:
    """The active hospital_jobs count, written to site_stats id=1.

    2026-10-07 (push 10): the nightly passes the total it already knows
    (active_total_after_passes: Layer 4's active_before minus its
    deactivations minus the front-pool retirements), so no statement scans
    the table. The id-cursor walk that used to run here (each page a pkey
    scan filtering is_active through the sparse low-id region, 3.3 s a page
    at idle, on a client with a ~5 s read timeout and no retry) timed out on
    10-04 and 10-06, printed "Active jobs in DB: 0", left site_stats stale
    and skipped the cohort refresh. It remains only as the fallback for a
    standalone call or an aborted Layer 4, with retries, on the service
    client. The site_stats write and the sitemap-cohort RPC each run on
    their own, so neither depends on the count any more.

    Returns {"total_active_jobs": int or None, ...}; None means unknown,
    and the caller prints "unknown", never 0.
    """
    total = total_active
    if total is None:
        total = _count_active_by_cursor()
    if total is None:
        logger.error("get_stats: active count unknown (no run arithmetic and the fallback count failed); "
                     "site_stats id=1 left as it was")
    else:
        _write_site_stats(total)
    _refresh_sitemap_cohort()
    return {"total_active_jobs": total,
            "last_updated": datetime.now().isoformat()}


# ── Run observability: public.scraper_runs (2026-10-07, push 10) ────────────
# One row a nightly: inserted when the run starts, updated when it ends,
# through the service client (the table has RLS on and no policies, so only
# service_role and postgres can write it). sql/push10_scraper_runs.sql adds
# the run_* columns to the table that exists (it has ats_platform NOT NULL
# and a status CHECK: running / success / partial / failed / blocked).
# Non-fatal throughout: a run without a record still runs.
RUN_RECORD_PLATFORM = "nightly"


def start_run_record(run_day: int, started_at_iso: str) -> int | None:
    """Insert tonight's scraper_runs row; returns its id, None on failure."""
    try:
        res = (service_client().table("scraper_runs")
                 .insert({"ats_platform": RUN_RECORD_PLATFORM,
                          "status": "running",
                          "started_at": started_at_iso,
                          "run_started_at": started_at_iso,
                          "run_day": run_day})
                 .execute())
        rows = res.data or []
        rid = rows[0].get("id") if rows and isinstance(rows[0], dict) else None
        if rid is None:
            logger.warning("scraper_runs: insert returned no id (non-fatal)")
            return None
        logger.info(f"scraper_runs: row {rid} started (run_day {run_day})")
        return int(rid)
    except Exception as e:
        logger.warning(f"scraper_runs insert failed (non-fatal): {e}")
        return None


def finish_run_record(run_id: int | None, status: str, finished_at_iso: str,
                      rows_upserted: int | None = None, deactivated: int | None = None,
                      field_priced: int | None = None, text_priced: int | None = None,
                      notes: str | None = None, duration_ms: int | None = None,
                      jobs_seen: int | None = None, meta: dict | None = None) -> bool:
    """Close tonight's scraper_runs row. status is one of the table's CHECK
    values (success / partial / failed / blocked). None fields are left as
    they are. Returns True when the update landed."""
    if run_id is None:
        return False
    if status not in ("running", "success", "partial", "failed", "blocked"):
        status = "partial"
    patch = {"status": status,
             "finished_at": finished_at_iso,
             "run_finished_at": finished_at_iso}
    for k, v in (("rows_upserted", rows_upserted), ("deactivated", deactivated),
                 ("field_priced", field_priced), ("text_priced", text_priced),
                 ("notes", notes), ("duration_ms", duration_ms), ("jobs_seen", jobs_seen),
                 ("jobs_deactivated", deactivated), ("meta", meta)):
        if v is not None:
            patch[k] = v
    try:
        (service_client().table("scraper_runs").update(patch).eq("id", run_id).execute())
        logger.info(f"scraper_runs: row {run_id} finished ({status})")
        return True
    except Exception as e:
        logger.warning(f"scraper_runs update failed (non-fatal): {e}")
        return False
