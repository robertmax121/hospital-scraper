"""
Nightly Scheduler — Lean Build
Scrapes all hospital systems → deduplicates → pushes to Supabase.
No email. No alerts. Maximum performance.

Cron: 0 20 * * *  (8 PM nightly)
"""

import logging
import os
from datetime import datetime, timezone
from scraper import (scrape, PARTIAL_SYSTEMS, COMPLETE_SYSTEMS, HOSPITAL_SYSTEM_ALIASES,
                     LAST_UPSERT_FAILED, LAST_RUN_COUNTS, proxies, fix_run_day)
from database import (mark_inactive_jobs, get_stats, active_total_after_passes,
                      start_run_record, finish_run_record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/run_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# 2026-10-07 (push 10): log hygiene. aiohttp 3.9.3 logs "Can not load
# response cookies: Illegal key 'BIGipServer...'" through 'aiohttp.client'
# for every pm.healthcaresource.com answer (an F5 cookie name with '/' and
# '+', which http.cookies rejects): 811 of the 1,000 lines Railway keeps
# were that one warning on 10-06, and the adapter stop lines fell off the
# tail. supabase-py's httpx client logs every PostgREST call at INFO (96-588
# lines a run). Neither says anything about the run.
def _drop_cookie_noise(record: logging.LogRecord) -> bool:
    try:
        return not record.getMessage().startswith("Can not load response cookies")
    except Exception:
        return True


def quiet_library_loggers() -> None:
    logging.getLogger("aiohttp.client").addFilter(_drop_cookie_noise)
    logging.getLogger("httpx").setLevel(logging.WARNING)


quiet_library_loggers()


def _priced_counts(jobs: list[dict]) -> tuple[int, int]:
    """Rows of tonight's upsert payload priced from a structured pay field
    and from the body text (posting_facts.pay_src, push 9). Rows whose
    stored body was kept (blank payload) count under neither, so this is
    what the run read, not the board's priced total (2026-10-07, push 10)."""
    field = text = 0
    for j in jobs:
        src = ((j.get("posting_facts") or {}).get("pay_src")) if isinstance(j, dict) else None
        if src == "field":
            field += 1
        elif src == "text":
            text += 1
    return field, text


def run():
    os.makedirs("logs", exist_ok=True)
    start = datetime.now()
    # 2026-10-07 (push 10): one run day for the whole run (scrape() keeps
    # this value for the pay and refresh slots), and a scraper_runs row
    # opened now and closed at the end (non-fatal either way).
    run_day = fix_run_day()
    started_iso = datetime.now(timezone.utc).isoformat()
    run_id = start_run_record(run_day, started_iso)

    logger.info("=" * 55)
    logger.info(f"  NIGHTLY SCRAPE — {start.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 55)

    # ── Step 1: Scrape everything ─────────────────────────────────
    logger.info("\n[ STEP 1 ] Scraping all hospital systems...")
    jobs = scrape()

    if not jobs:
        logger.error("Zero jobs returned — aborting.")
        finish_run_record(run_id, "failed", datetime.now(timezone.utc).isoformat(),
                          rows_upserted=0, notes="zero jobs returned by scrape(); aborted before Layer 4",
                          duration_ms=int((datetime.now() - start).total_seconds() * 1000))
        return

    if proxies.proxies:
        logger.info(f"  Proxy pool: {len(proxies.proxies)} loaded, {len(proxies.retired)} retired, "
                    f"{proxies.fallbacks} proxied requests retried direct")

    # ── Step 2: Layer 4 (the rows are already in the database) ────
    # 2026-09-24: this step used to call database.upsert_jobs(jobs) first.
    # scrape() had already upserted these exact rows through
    # scraper._upsert_hospital_jobs_to_supabase: the same dicts, aliased and
    # stamped in place, on the same conflict key. The second pass re-sent
    # every row (~225k) in 100-row batches each night, doubling the write
    # load on a 32-index table whose 8 s statement timeout is what failed the
    # first pass, and its continue-on-error loop hid those failures. The
    # first pass now dedupes, splits failed batches, retries and continues,
    # pauses and probes through a database brownout (abandoning only after
    # 10 minutes without a write), and scrape() re-sends the rows that did
    # not land once more after the travel flow (retry_failed_hospital_upsert),
    # with no sweep. Their systems stay unswept tonight either way.
    logger.info(f"\n[ STEP 2 ] {len(jobs):,} jobs were upserted by scrape(); running Layer 4...")
    if LAST_UPSERT_FAILED:
        logger.warning(f"  {len(LAST_UPSERT_FAILED):,} hospital rows did not land after the retry; "
                       f"their systems were not swept (Layer 4 still resets their seen rows)")

    # Layer 4: multi-run miss confirmation before deactivation.
    # A row needs to miss MISS_THRESHOLD consecutive scrapes before going
    # is_active=false. mark_inactive_jobs handles the counter bookkeeping.
    # 2026-09-16: systems whose crawl was partial or skipped tonight are
    # exempt from the miss count (the per-system sweep already exempted them;
    # this pass did not, and it was the engine deactivating HCA's rows).
    # HCA is maintained by hca_local_push.py from a residential IP unless the
    # nightly is explicitly told to crawl it (HCA_NIGHTLY=1).
    # 2026-09-24: on top of these explicit exemptions, mark_inactive_jobs now
    # applies the sweep's yield guard to every system (retire_guard.py): a
    # system that yielded 0 rows, or under 80% of 20+ active rows, keeps its
    # unseen rows' miss counts, with a 7-day scraped_at backstop.
    layer4_exempt = {HOSPITAL_SYSTEM_ALIASES.get(s, s) for s in PARTIAL_SYSTEMS}
    if os.environ.get("HCA_NIGHTLY") != "1":
        layer4_exempt.add("HCA Healthcare")
    # 2026-10-07 (push 10): adapters that read their whole board
    # (COMPLETE_SYSTEMS) bypass the yield guard; a partial report wins.
    layer4_complete = {HOSPITAL_SYSTEM_ALIASES.get(s, s) for s in COMPLETE_SYSTEMS} - layer4_exempt
    deact_stats = mark_inactive_jobs(jobs, exclude_systems=layer4_exempt, complete_systems=layer4_complete)
    logger.info(f"  Layer 4 deactivation: {deact_stats}")

    # ── Step 2b: travel URL liveness sweep (added 2026-07-20) ─────
    # Travel contracts churn fast — a same-day sample showed ~20% of
    # fresh-scraped listings already 404/410 by evening. HEAD-validate every
    # active travel URL and deactivate the definitively dead so the /travel
    # board doesn't serve broken apply links between scrapes. Non-fatal.
    try:
        import asyncio as _asyncio
        from validate_travel_urls import main as _validate_travel
        logger.info("\n[ STEP 2b ] Validating travel job URLs...")
        _asyncio.run(_validate_travel())
    except Exception as e:
        logger.warning(f"travel URL validation failed (non-fatal): {e}")

    # ── Step 2c: re-count travel AFTER validation (added 2026-07-29) ──
    # The travel counter used to run inside the travel upsert, i.e. BEFORE
    # the validator above deactivates dead links — so even a successful write
    # was stale by a couple thousand rows. Re-running it here makes
    # site_stats id=2 the final, post-sweep number the homepage should show.
    try:
        from scraper import update_travel_site_stats
        logger.info("\n[ STEP 2c ] Recounting travel jobs for site_stats...")
        update_travel_site_stats()
    except Exception as e:
        logger.warning(f"travel site_stats recount failed (non-fatal): {e}")

    # ── Step 2d: sign-on bonus flag pass (added 2026-08-08) ───────
    # Flags active hospital_jobs mentioning a sign-on/signing bonus in the
    # title or description; the website renders the bonus pill from it.
    # 2026-10-07 (push 10): has_signon now rides on the upsert (normalize_job
    # computes it with the body in hand, and the enrichment trigger ORs it
    # with the stored value), so this call is a no-op unless asked for a
    # full backfill (flag_signon_jobs(full_pass=True)). Non-fatal.
    try:
        from scraper import flag_signon_jobs
        logger.info("\n[ STEP 2d ] Flagging sign-on bonus jobs...")
        flag_signon_jobs()
    except Exception as e:
        logger.warning(f"sign-on flag pass failed (non-fatal): {e}")

    # ── Step 2e: front-page link check (added 2026-09-22) ─────────
    # The front page and /api/front-pool deal cards from per-state pools
    # (verified platform + posted wage); apply_verified is a judgement about
    # a platform, not a link, and four of the five cards the owner clicked
    # that day were dead. validate_front_pool_urls HEAD-checks exactly the
    # rows those pools serve, retires the confirmed 404/410s and stamps
    # last_dead_check_at on the rest. Bounded to 15 minutes, refuses a mass
    # retirement, non-fatal.
    front_summary: dict = {}
    try:
        import asyncio as _asyncio
        from validate_front_pool_urls import main as _validate_front
        logger.info("\n[ STEP 2e ] Checking front-page pool links...")
        front_summary = _asyncio.run(_validate_front()) or {}
    except Exception as e:
        logger.warning(f"front-pool link check failed (non-fatal): {e}")

    # ── Step 3: Summary ───────────────────────────────────────────
    # 2026-10-07 (push 10): the active total is arithmetic on what the run
    # already knows (Layer 4 active_before minus its deactivations minus the
    # front-pool retirements), no table scan; a count that cannot be made
    # prints "unknown", never 0.
    total_active = active_total_after_passes(deact_stats, front_summary)
    stats = get_stats(total_active)
    elapsed = (datetime.now() - start).seconds
    n = stats.get("total_active_jobs")
    shown = f"{n:,}" if isinstance(n, int) else "unknown"

    logger.info(f"\n{'─'*55}")
    logger.info(f"  Active jobs in DB:  {shown}")
    logger.info(f"  Runtime:            {elapsed}s")
    logger.info(f"  Completed:          {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"{'─'*55}\n")

    # Close the scraper_runs row (2026-10-07, push 10).
    field_priced, text_priced = _priced_counts(jobs)
    sweep_deact = LAST_RUN_COUNTS.get("sweep_deactivated") or 0
    layer4_deact = deact_stats.get("deactivated") or 0
    front_retired = front_summary.get("retired") or 0
    status = "partial" if (LAST_UPSERT_FAILED or deact_stats.get("aborted")) else "success"
    notes = (f"rows in payload {len(jobs):,}; upsert landed {LAST_RUN_COUNTS.get('upsert_sent') or 0:,}, "
             f"not landed {len(LAST_UPSERT_FAILED):,}; deactivated: sweep {sweep_deact:,}, "
             f"layer4 {layer4_deact:,}, front-pool {front_retired:,}; layer4 guarded "
             f"{len(deact_stats.get('guarded_systems') or [])} system(s)"
             f"{' (ABORTED)' if deact_stats.get('aborted') else ''}; partial adapters "
             f"{len(PARTIAL_SYSTEMS)}; active after run {shown}")
    finish_run_record(run_id, status, datetime.now(timezone.utc).isoformat(),
                      rows_upserted=LAST_RUN_COUNTS.get("upsert_sent"),
                      deactivated=sweep_deact + layer4_deact + front_retired,
                      field_priced=field_priced, text_priced=text_priced,
                      notes=notes, duration_ms=int((datetime.now() - start).total_seconds() * 1000),
                      jobs_seen=len(jobs),
                      meta={"layer4": {k: v for k, v in deact_stats.items()
                                       if k not in ("excluded_systems", "guarded_systems")},
                            "front_pool": front_summary,
                            "partial_systems": sorted(PARTIAL_SYSTEMS)[:50],
                            "guarded_systems": (deact_stats.get("guarded_systems") or [])[:50]})


if __name__ == "__main__":
    run()
