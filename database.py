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
    UNIQUE(job_id, hospital_system)
);
CREATE INDEX IF NOT EXISTS idx_state      ON hospital_jobs(state);
CREATE INDEX IF NOT EXISTS idx_specialty  ON hospital_jobs(specialty);
CREATE INDEX IF NOT EXISTS idx_system     ON hospital_jobs(hospital_system);
CREATE INDEX IF NOT EXISTS idx_active     ON hospital_jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_ats        ON hospital_jobs(ats_platform);
"""


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


def mark_inactive_jobs(current_keys: set[str]) -> int:
    db = client()
    try:
        result = db.table("hospital_jobs").select("job_id,hospital_system").eq("is_active", True).execute()
        stale = {f"{r['hospital_system']}::{r['job_id']}" for r in result.data} - current_keys
        for key in stale:
            system, job_id = key.split("::", 1)
            db.table("hospital_jobs").update({"is_active": False}).eq("hospital_system", system).eq("job_id", job_id).execute()
        return len(stale)
    except Exception as e:
        logger.error(f"mark_inactive error: {e}")
        return 0


def get_stats() -> dict:
    db = client()
    try:
        result = db.table("hospital_jobs").select("id", count="exact").eq("is_active", True).execute()
        return {"total_active_jobs": result.count, "last_updated": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"get_stats error: {e}")
        return {}
