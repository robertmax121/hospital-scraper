"""
Database — Supabase
Handles upsert, deactivation, and stats queries.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
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
    UNIQUE(job_id, hospital_system)
);
CREATE INDEX IF NOT EXISTS idx_state      ON hospital_jobs(state);
CREATE INDEX IF NOT EXISTS idx_specialty  ON hospital_jobs(specialty);
CREATE INDEX IF NOT EXISTS idx_system     ON hospital_jobs(hospital_system);
CREATE INDEX IF NOT EXISTS idx_active     ON hospital_jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_ats        ON hospital_jobs(ats_platform);
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


def mark_inactive_jobs(current_jobs: list[dict], deact_min: int = 10,
                       age_days: int = 14) -> dict:
    """Hybrid deactivation. Replaces the previous set-diff implementation,
    which (a) silently truncated to 1000 rows because it never paginated and
    (b) was too aggressive — a broken scraper module returning zero rows
    would wipe that system's entire inventory.

    Two passes:

      A. **Diff-based deactivation, per healthy system.** A system is
         "healthy" this run if it produced >= `deact_min` rows. For each
         such system, fetch ALL active rows (paginated), diff against the
         job_ids in this run, mark the leftover stale.

      B. **Age-based sweep across the whole table.** Any active row whose
         scraped_at hasn't been refreshed in > `age_days` days gets marked
         inactive — regardless of which system it belongs to. This is the
         backstop for systems whose scraper module is broken (Healthcare-
         Source, Tenet, iCIMS modern, etc.) — those rows never get
         refreshed by the diff pass because the scraper produces zero
         current jobs for them.

    Note: this does NOT verify the apply URL actually resolves. That's the
    link_health module's job, which runs as a separate step.

    Args:
        current_jobs: the list of job dicts this scrape produced. We use
                      it to build per-system key sets and identify which
                      systems counted as "healthy."
        deact_min:    minimum rows-per-system to enable diff-based
                      deactivation for that system. Below this, only the
                      age-based sweep can deactivate the system's rows.
        age_days:     scraped_at age threshold for the age-based sweep.

    Returns:
        Stats dict.
    """
    db = client()

    # Bucket current scrape by system
    by_system: dict[str, set[str]] = {}
    for j in current_jobs:
        s = j.get("hospital_system")
        k = j.get("job_id")
        if not s or not k:
            continue
        by_system.setdefault(s, set()).add(str(k))

    healthy = sorted(s for s, ks in by_system.items() if len(ks) >= deact_min)
    skipped = sorted((s, len(ks)) for s, ks in by_system.items() if len(ks) < deact_min)
    if skipped:
        sample = skipped[:8]
        more = f" (+{len(skipped) - 8} more)" if len(skipped) > 8 else ""
        logger.info(f"  Diff-deactivation skip (below DEACT_MIN={deact_min}): {sample}{more}")

    # ── Pass A: Diff-based deactivation, per healthy system ───────────────
    diff_deact = 0
    for system in healthy:
        current_keys = by_system[system]
        existing: set[str] = set()
        offset = 0
        while True:
            try:
                resp = (db.table("hospital_jobs")
                          .select("job_id")
                          .eq("is_active", True)
                          .eq("hospital_system", system)
                          .range(offset, offset + PAGE - 1)
                          .execute())
                rows = resp.data or []
            except Exception as e:
                logger.warning(f"  Fetch active for {system}: {e}")
                rows = []
            existing.update(str(r["job_id"]) for r in rows)
            if len(rows) < PAGE:
                break
            offset += PAGE
        stale = list(existing - current_keys)
        if not stale:
            continue
        for i in range(0, len(stale), 500):
            chunk = stale[i:i+500]
            try:
                (db.table("hospital_jobs")
                   .update({"is_active": False})
                   .eq("hospital_system", system)
                   .in_("job_id", chunk)
                   .execute())
                diff_deact += len(chunk)
            except Exception as e:
                logger.warning(f"  Deactivate {system} chunk {i//500}: {e}")
        logger.info(f"  Deactivated {len(stale):,} stale rows for {system}")

    # ── Pass B: Age-based sweep (catches broken-module systems) ───────────
    cutoff = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    age_ids: list[int] = []
    offset = 0
    while True:
        try:
            resp = (db.table("hospital_jobs")
                      .select("id")
                      .eq("is_active", True)
                      .lt("scraped_at", cutoff)
                      .range(offset, offset + PAGE - 1)
                      .execute())
            rows = resp.data or []
        except Exception as e:
            logger.warning(f"  Fetch age-stale rows: {e}")
            rows = []
        age_ids.extend(r["id"] for r in rows)
        if len(rows) < PAGE:
            break
        offset += PAGE
    age_deact = 0
    for i in range(0, len(age_ids), 500):
        chunk = age_ids[i:i+500]
        try:
            db.table("hospital_jobs").update({"is_active": False}).in_("id", chunk).execute()
            age_deact += len(chunk)
        except Exception as e:
            logger.warning(f"  Age-deactivate batch {i//500}: {e}")
    if age_deact:
        logger.info(f"  Age-deactivated {age_deact:,} rows older than {age_days} days")

    summary = {
        "diff_deactivated": diff_deact,
        "age_deactivated":  age_deact,
        "healthy_systems":  len(healthy),
        "broken_systems":   len(skipped),
    }
    logger.info(f"Hybrid deactivation: {summary}")
    return summary


def get_stats() -> dict:
    db = client()
    try:
        result = db.table("hospital_jobs").select("id", count="exact").eq("is_active", True).execute()
        return {"total_active_jobs": result.count, "last_updated": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"get_stats error: {e}")
        return {}
