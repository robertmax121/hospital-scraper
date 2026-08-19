"""
Nightly Scheduler — Lean Build
Scrapes all hospital systems → deduplicates → pushes to Supabase.
No email. No alerts. Maximum performance.

Cron: 0 20 * * *  (8 PM nightly)
"""

import logging
import os
from datetime import datetime
from scraper import scrape
from database import upsert_jobs, mark_inactive_jobs, get_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/run_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def run():
    os.makedirs("logs", exist_ok=True)
    start = datetime.now()

    logger.info("=" * 55)
    logger.info(f"  NIGHTLY SCRAPE — {start.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 55)

    # ── Step 1: Scrape everything ─────────────────────────────────
    logger.info("\n[ STEP 1 ] Scraping all hospital systems...")
    jobs = scrape()

    if not jobs:
        logger.error("Zero jobs returned — aborting.")
        return

    # ── Step 2: Push to database ──────────────────────────────────
    logger.info(f"\n[ STEP 2 ] Pushing {len(jobs):,} jobs to Supabase...")
    result = upsert_jobs(jobs)
    logger.info(f"  Result: {result}")

    # Layer 4: multi-run miss confirmation before deactivation.
    # A row needs to miss MISS_THRESHOLD consecutive scrapes before going
    # is_active=false. mark_inactive_jobs handles the counter bookkeeping.
    deact_stats = mark_inactive_jobs(jobs)
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
    # Runs after the description-fetch phase so late-arriving descriptions
    # get flagged the same night. Non-fatal.
    try:
        from scraper import flag_signon_jobs
        logger.info("\n[ STEP 2d ] Flagging sign-on bonus jobs...")
        flag_signon_jobs()
    except Exception as e:
        logger.warning(f"sign-on flag pass failed (non-fatal): {e}")

    # ── Step 3: Summary ───────────────────────────────────────────
    stats = get_stats()
    elapsed = (datetime.now() - start).seconds

    logger.info(f"\n{'─'*55}")
    logger.info(f"  Active jobs in DB:  {stats.get('total_active_jobs', 0):,}")
    logger.info(f"  Runtime:            {elapsed}s")
    logger.info(f"  Completed:          {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"{'─'*55}\n")


if __name__ == "__main__":
    run()
