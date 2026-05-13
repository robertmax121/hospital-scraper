"""
Database — Supabase
Handles upsert, deactivation, and stats queries.
"""

import os
import logging
from datetime import datetime
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# ── Run this SQL once in your Supabase SQL editor ─────────────────────────────
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
    dead_check_failures INT NOT NULL DEFAULT 0,
    last_dead_check_at  TIMESTAMPTZ,
    UNIQUE(job_id, hospital_system)
);
CREATE INDEX IF NOT EXISTS idx_state      ON hospital_jobs(state);
CREATE INDEX IF NOT EXISTS idx_specialty  ON hospital_jobs(specialty);
CREATE INDEX IF NOT EXISTS idx_system     ON hospital_jobs(hospital_system);
CREATE INDEX IF NOT EXISTS idx_active     ON hospital_jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_ats        ON hospital_jobs(ats_platform);
CREATE INDEX IF NOT EXISTS idx_dead_check_at ON hospital_jobs(last_dead_check_at);
"""

# PostgREST returns at most this many rows per request unless overridden via
# Range headers. supabase-py's `.range(a, b)` sets those headers for us, so
# we paginate explicitly anywhere we'd otherwise be silently truncated.
PAGE = 1000


def client() -> Client:
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_KEY environment variables")
    return create_client(url, key)


def upsert_jobs(jobs: list[dict]) -> dict:
    db = client()
    inserted, errors = 0, 0
    for i in range(0, len(jobs), 100):
        batch = jobs[i:i+100]
        try:
            db.table("hospital_jobs").upsert(batch, on_conflict="job_id,hospital_system").execute()
            inserted += len(batch)
        except Exception as e:
            logger.error(f"Batch {i//100} upsert error: {e}")
            errors += len(batch)
    return {"inserted": inserted, "errors": errors}


def mark_inactive_jobs(current_jobs: list[dict]) -> dict:
    """Strict-diff deactivation.

    The only signal we trust is: did this scrape run produce this row?

      - Row's (job_id, hospital_system) IS in the current scrape → keep active.
      - Row's (job_id, hospital_system) is NOT in the current scrape → deactivate.

    No DEACT_MIN guard, no age-based fallback, no per-system carve-outs.
    Earlier iterations had both — the reasoning being "protect a system's
    inventory from a broken/transient scraper module." That was wrong: if
    a module returns zero, those URLs ARE dead from the user's perspective
    (the hospital's ATS hasn't confirmed them in this run). Better to show
    an empty system for a night than show a system with dead apply links.
    If the module comes back, those jobs come back next run.

    The replaced implementation also had a silent 1000-row pagination cap
    on the active-rows fetch, which is why the 2026-05-12 nightly only
    deactivated 98 rows out of a true ~36K stale backlog. This pass
    paginates explicitly.

    URL-resolves-on-HEAD verification is link_health.py's job — separate.

    Args:
        current_jobs: every job dict this scrape produced. We diff their
                      (hospital_system, job_id) tuples against the active
                      set in the DB.

    Returns:
        Stats dict: deactivated, current_keys, active_before.
    """
    db = client()

    # Build the canonical "what's in this run" key set.
    current_keys: set[tuple[str, str]] = set()
    for j in current_jobs:
        s = j.get("hospital_system")
        k = j.get("job_id")
        if s and k:
            current_keys.add((s, str(k)))
    logger.info(f"  Strict-diff: {len(current_keys):,} keys in current scrape")

    # Page through ALL active rows.
    active: list[dict] = []
    offset = 0
    while True:
        try:
            resp = (db.table("hospital_jobs")
                      .select("id,job_id,hospital_system")
                      .eq("is_active", True)
                      .range(offset, offset + PAGE - 1)
                      .execute())
            rows = resp.data or []
        except Exception as e:
            logger.warning(f"  Fetch active page (offset={offset}): {e}")
            break
        active.extend(rows)
        if len(rows) < PAGE:
            break
        offset += PAGE
    logger.info(f"  Strict-diff: {len(active):,} active rows in DB")

    # Diff in memory.
    stale_ids = [
        r["id"] for r in active
        if (r["hospital_system"], str(r["job_id"])) not in current_keys
    ]

    # Batch-deactivate.
    deactivated = 0
    for i in range(0, len(stale_ids), 500):
        chunk = stale_ids[i:i+500]
        try:
            (db.table("hospital_jobs")
               .update({"is_active": False})
               .in_("id", chunk)
               .execute())
            deactivated += len(chunk)
        except Exception as e:
            logger.warning(f"  Deactivate chunk {i//500}: {e}")

    summary = {
        "deactivated":   deactivated,
        "current_keys":  len(current_keys),
        "active_before": len(active),
    }
    logger.info(f"Strict-diff deactivation: {summary}")
    return summary


def get_stats() -> dict:
    db = client()
    try:
        result = db.table("hospital_jobs").select("id", count="exact").eq("is_active", True).execute()
        return {"total_active_jobs": result.count, "last_updated": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"get_stats error: {e}")
        return {}
