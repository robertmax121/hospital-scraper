"""
Nightly Scheduler — Lean Build
Scrapes all hospital systems → deduplicates → pushes to Supabase →
hybrid-deactivates stale → direct link-health-checks every active URL.
No email. No alerts. Maximum performance.

Cron: 0 20 * * *  (8 PM nightly)
"""

import logging
import os
from datetime import datetime
from scraper import scrape
from database import upsert_jobs, mark_inactive_jobs, get_stats
import link_health

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

    # ── Step 2.5: Hybrid deactivation ─────────────────────────────
    # Per-system diff for healthy systems + age-based sweep for the rest.
    # See database.mark_inactive_jobs docstring for the rationale.
    logger.info("\n[ STEP 2.5 ] Hybrid deactivation pass...")
    try:
        deact_stats = mark_inactive_jobs(jobs)
        logger.info(f"  Deactivation: {deact_stats}")
    except Exception as e:
        logger.warning(f"  Hybrid deactivation failed (non-fatal): {e}")

    # ── Step 3: Direct link-health check ──────────────────────────
    # Ground-truth dead-link detection. HEADs every active URL; flags
    # is_active=false on definitive 404/410. Other failure modes (timeouts,
    # 5xx, 403, DNS errors) are NOT deactivated — they're usually transient
    # or bot-blocked rather than truly dead.
    #
    # On a ~70K-row table with 50 workers this takes roughly 10-15 minutes.
    # Wrapped in try/except so a transport issue can't break the cron.
    logger.info("\n[ STEP 3 ] Direct link-health check (HEAD scan)...")
    try:
        link_stats = link_health.run()
        logger.info(f"  Link health: {link_stats}")
    except Exception as e:
        logger.warning(f"  Link health check failed (non-fatal): {e}")

    # ── Step 4: Summary ───────────────────────────────────────────
    stats = get_stats()
    elapsed = (datetime.now() - start).seconds

    logger.info(f"\n{'─'*55}")
    logger.info(f"  Active jobs in DB:  {stats.get('total_active_jobs', 0):,}")
    logger.info(f"  Runtime:            {elapsed}s")
    logger.info(f"  Completed:          {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"{'─'*55}\n")


if __name__ == "__main__":
    run()
