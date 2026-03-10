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

    # Mark jobs no longer found as inactive
    current_keys = {f"{j['hospital_system']}::{j['job_id']}" for j in jobs}
    deactivated = mark_inactive_jobs(current_keys)
    logger.info(f"  Deactivated {deactivated} stale listings")

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
