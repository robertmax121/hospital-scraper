"""
Google Indexing API publisher — nightly companion to the scraper.

Submits quality-cohort job URLs (is_active + apply_verified + description
>= 200 chars — the exact set app/jobs/[slug]/page.js renders index,follow)
to Google's Indexing API, newest-scraped first, and URL_DELETED
notifications for previously-submitted jobs that have since gone inactive.

Quota: Google's default approved quota is 200 publish requests/day. We spend
up to PUBLISH_CAP on updates and DELETE_CAP on removals; 429 stops the run
gracefully (tomorrow's run resumes where we left off — submission state is
tracked in hospital_jobs.indexing_submitted_at, added 2026-07-27).

Auth: service-account JSON key (GOOGLE_INDEXING_KEY env var, falling back to
the shared-folder key). The service account must be a GSC Owner of
jobs.waypointrecruit.com and the Web Search Indexing API must be enabled in
its project.

Run: python indexing_publisher.py            (scheduled daily on the office PC)
     python indexing_publisher.py --cap 5    (small test run)
"""
import argparse
import json
import logging
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("indexing_publisher")

SITE = "https://jobs.waypointrecruit.com"
PUBLISH_CAP = 180
DELETE_CAP = 15
PAGE = 500

DEFAULT_KEY_PATHS = [
    os.environ.get("GOOGLE_INDEXING_KEY", ""),
    r"C:\Users\rober\OneDrive\claud_outputinput\waypoint-indexing-503717-1119cd82261c.json",
]
ENV_LOCAL = r"C:\Users\rober\OneDrive\claud_outputinput\.env.local"


def load_env():
    env = dict(os.environ)
    if os.path.exists(ENV_LOCAL):
        for line in open(ENV_LOCAL, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env.setdefault(k.strip(), v.strip())
    return env


def slugify(s: str) -> str:
    # MUST mirror lib/slugify.js exactly — the published URL has to equal the
    # canonical slug the site 301-enforces, or Google gets redirect chains.
    return re.sub(r"^-+|-+$", "", re.sub(r"[^a-z0-9]+", "-", (s or "").lower()))


def make_slug(job: dict) -> str:
    parts = [slugify(p) for p in [job.get("title"), job.get("city"), job.get("state")] if p]
    parts.append(str(job["id"]))
    return "-".join(parts)


def sb(env, path, method="GET", body=None, prefer=None):
    key = env.get("SUPABASE_SERVICE_ROLE_KEY") or env.get("SUPABASE_KEY")
    url = f"{env.get('SUPABASE_URL', 'https://tlzxajgonuevbqaelhvf.supabase.co').rstrip('/')}/rest/v1/{path}"
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def google_session():
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
    key_path = next((p for p in DEFAULT_KEY_PATHS if p and os.path.exists(p)), None)
    if not key_path:
        logger.error("no service-account key found (set GOOGLE_INDEXING_KEY)")
        sys.exit(1)
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/indexing"])
    return AuthorizedSession(creds)


def notify(sess, url, kind):
    """Returns 'ok', 'quota', or 'error'."""
    r = sess.post("https://indexing.googleapis.com/v3/urlNotifications:publish",
                  json={"url": url, "type": kind})
    if r.status_code == 200:
        return "ok"
    if r.status_code == 429:
        logger.warning("quota exhausted (429) — stopping for today")
        return "quota"
    logger.warning(f"{kind} {url} -> HTTP {r.status_code}: {r.text[:200]}")
    return "error"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=int, default=PUBLISH_CAP, help="max URL_UPDATED submissions this run")
    args = ap.parse_args()

    env = load_env()
    sess = google_session()
    now = datetime.now(timezone.utc).isoformat()

    # Abort the whole run after this many consecutive failures — a broken
    # precondition (API disabled, missing GSC ownership) fails EVERY request
    # identically, and retrying 16k queue entries against it just hammers
    # Google with 403s (exactly what the first test run did before this guard).
    MAX_CONSECUTIVE_ERRORS = 5
    consecutive = 0
    deleted = 0
    published = 0

    # ── URL_DELETED for previously-submitted jobs that died ────────────────
    dead = sb(env, "hospital_jobs?select=id,title,city,state"
                   "&is_active=eq.false&indexing_submitted_at=not.is.null"
                   f"&limit={DELETE_CAP}")
    for job in dead or []:
        verdict = notify(sess, f"{SITE}/jobs/{make_slug(job)}", "URL_DELETED")
        if verdict == "quota":
            return summary(published, deleted)
        if verdict == "ok":
            consecutive = 0
            sb(env, f"hospital_jobs?id=eq.{job['id']}", method="PATCH",
               body={"indexing_submitted_at": None}, prefer="return=minimal")
            deleted += 1
        else:
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_ERRORS:
                logger.error("aborting: %d consecutive failures — check API enabled + GSC ownership", consecutive)
                return summary(published, deleted)

    # ── URL_UPDATED for the freshest never-submitted quality jobs ──────────
    # One batch per run: exactly `cap` candidates, attempted once each.
    # Failures are not retried within the run — the queue re-serves them
    # tomorrow (indexing_submitted_at stays NULL).
    batch = sb(env, "hospital_jobs?select=id,title,city,state"
                    "&is_active=eq.true&apply_verified=eq.true&desc_len=gte.200"
                    "&indexing_submitted_at=is.null"
                    "&order=scraped_at.desc"
                    f"&limit={args.cap}")
    for job in batch or []:
        verdict = notify(sess, f"{SITE}/jobs/{make_slug(job)}", "URL_UPDATED")
        if verdict == "quota":
            return summary(published, deleted)
        if verdict == "ok":
            consecutive = 0
            sb(env, f"hospital_jobs?id=eq.{job['id']}", method="PATCH",
               body={"indexing_submitted_at": now}, prefer="return=minimal")
            published += 1
        else:
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_ERRORS:
                logger.error("aborting: %d consecutive failures — check API enabled + GSC ownership", consecutive)
                return summary(published, deleted)
    return summary(published, deleted)


def summary(published, deleted):
    logger.info(f"DONE: published={published} deleted={deleted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
