"""
Database — Supabase
Handles upsert, deactivation (Layer 4: multi-run miss confirmation),
and stats queries.
"""

import os
import logging
from datetime import datetime
from supabase import create_client, Client
from retire_guard import BACKSTOP_DAYS, plan_layer4

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
    function now dedupes, splits and retries itself.
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
                       exclude_systems: set[str] | None = None) -> dict:
    """Layer 4 deactivation: multi-run miss confirmation.

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
    plan = plan_layer4(active, current_keys, miss_threshold, exclude_systems)
    found_ids: list[int] = plan["found_ids"]
    deactivate_ids: list[int] = plan["deactivate_ids"]
    bump_by_new_count: dict[int, list[int]] = plan["bump_by_new_count"]
    excluded_n = plan["excluded_rows"]
    guarded = plan["guarded"]
    for system in sorted(guarded, key=lambda s: -guarded[s]["active"]):
        g = guarded[system]
        logger.warning(f"  LAYER4 GUARD {system}: {g['reason']} {g['yield']}/{g['active']} "
                       f"(yield/active); {g['frozen']} unseen rows keep their miss count, "
                       f"{g['backstop']} past the {BACKSTOP_DAYS}-day backstop counted as usual")
    if guarded:
        logger.warning(f"  Layer 4 yield guard: {len(guarded)} system(s), "
                       f"{plan['frozen_rows']:,} rows held, {plan['backstop_rows']:,} past the backstop")

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
    }
    logger.info(f"Layer 4 deactivation: {summary}")
    return summary


def get_stats() -> dict:
    """Count active jobs.

    A single `SELECT id ... count=exact WHERE is_active=true` forces Postgres
    to scan the whole active set in one statement, which now exceeds the role's
    statement_timeout (error 57014 / HTTP 500) and returned a misleading 0 to
    the website's job counter. Instead we CURSOR-paginate on id — the same
    pattern Layer 4 uses to walk the table without timing out — and tally the
    rows. Each page is a small indexed range scan with a LIMIT, so no single
    statement can blow the timeout regardless of table size.
    """
    db = client()
    try:
        total = 0
        last_id = 0
        while True:
            resp = (db.table("hospital_jobs")
                      .select("id")
                      .eq("is_active", True)
                      .gt("id", last_id)
                      .order("id", desc=False)
                      .limit(PAGE)
                      .execute())
            rows = resp.data or []
            if not rows:
                break
            total += len(rows)
            last_id = rows[-1]["id"]

        # Persist to the single-row site_stats summary table. The public
        # website reads THIS one row for its hero-pill count instead of
        # running a live exact COUNT (which times out at this table size).
        #
        # site_stats has RLS enabled with a public-read-only policy, so the
        # write MUST go through the service-role key (service_client), which
        # bypasses RLS. The plain client() is the anon key — it can write the
        # RLS-off hospital_jobs table fine but is rejected here (42501), which
        # is exactly how this counter silently went stale before.
        if not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
            logger.warning(
                "SUPABASE_SERVICE_ROLE_KEY not set — the site_stats write will be "
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
        except Exception as e:
            logger.warning(f"site_stats write failed (non-fatal): {e}")

        # Sticky sitemap cohort refresh (2026-08-24): admit tonight's new
        # quality-bar qualifiers, evict only deactivated jobs. Kills the
        # quality-gate flapping that swung GSC "known pages" by thousands
        # per day. Non-fatal; the website's sitemap RPC reads the table.
        try:
            # Railway's older supabase-py requires the params argument
            # explicitly; the no-arg form raised "Client.rpc() missing 1
            # required positional argument: 'params'" every night, so the
            # cohort never admitted new jobs after the 8/24 seed (confirmed
            # 2026-08-26: cohort_joined_tonight=0 until a manual refresh).
            res = service_client().rpc("refresh_hospital_sitemap_cohort", {}).execute()
            logger.info(f"hospital_sitemap_cohort refreshed: {res.data}")
        except Exception as e:
            logger.warning(f"hospital_sitemap_cohort refresh failed (non-fatal): {e}")

        return {"total_active_jobs": total,
                "last_updated": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"get_stats error: {e}")
        return {}
