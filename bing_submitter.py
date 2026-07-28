"""
Bing URL Submission API publisher — daily companion to indexing_publisher.py.

Submits quality-cohort job URLs (same bar as the Google publisher: is_active +
apply_verified + description >= 200 chars) to Bing Webmaster Tools' URL
Submission API. Bing's daily quota is adaptive and typically far more generous
than Google's 200 — we ask the API for today's remaining quota and submit up
to that many, in batches of up to 500.

This feeds Bing organic AND the surfaces built on Bing's index: ChatGPT
search, Copilot, DuckDuckGo, Ecosia.

State: hospital_jobs.bing_submitted_at (column + queue index, 2026-07-28).
Auth: BING_WMT_API_KEY in the shared-folder .env.local.

Run: python bing_submitter.py            (scheduled daily with the Google run)
     python bing_submitter.py --cap 20   (small test)
"""
import argparse
import json
import logging
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bing_submitter")

SITE = "https://jobs.waypointrecruit.com"
BATCH = 500
SAFETY_CAP = 4000          # never spend the entire adaptive quota in one day
ENV_LOCAL = r"C:\Users\rober\OneDrive\claud_outputinput\.env.local"
API = "https://ssl.bing.com/webmaster/api.svc/json"


def load_env():
    env = dict(os.environ)
    if os.path.exists(ENV_LOCAL):
        for line in open(ENV_LOCAL, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env.setdefault(k.strip(), v.strip())
    return env


def slugify(s):
    return re.sub(r"^-+|-+$", "", re.sub(r"[^a-z0-9]+", "-", (s or "").lower()))


def make_slug(job):
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


def bing(env, endpoint, payload=None):
    key = env.get("BING_WMT_API_KEY")
    if not key:
        logger.error("BING_WMT_API_KEY not set in %s", ENV_LOCAL)
        sys.exit(1)
    url = f"{API}/{endpoint}?apikey={key}"
    if payload is None:
        url += f"&siteUrl={urllib.parse.quote(SITE, safe='')}"
        req = urllib.request.Request(url, headers={"User-Agent": "waypoint-bing-submitter/1.0"})
    else:
        req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json; charset=utf-8",
                                              "User-Agent": "waypoint-bing-submitter/1.0"},
                                     method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read() or b"{}")


import urllib.parse  # noqa: E402  (used in bing())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cap", type=int, default=SAFETY_CAP)
    args = ap.parse_args()
    env = load_env()
    now = datetime.now(timezone.utc).isoformat()

    # Ask Bing how much quota we actually have today.
    try:
        q = bing(env, "GetUrlSubmissionQuota")
        daily = (q.get("d") or {}).get("DailyQuota", 0)
        logger.info("Bing daily quota remaining: %s", daily)
    except urllib.error.HTTPError as e:
        logger.error("quota check failed: HTTP %s %s", e.code, e.read().decode()[:300])
        sys.exit(1)
    budget = max(0, min(args.cap, int(daily)))
    if budget == 0:
        logger.info("no quota remaining today - done")
        return 0

    submitted = 0
    while submitted < budget:
        take = min(BATCH, budget - submitted)
        batch = sb(env, "hospital_jobs?select=id,title,city,state"
                        "&is_active=eq.true&apply_verified=eq.true&desc_len=gte.200"
                        "&bing_submitted_at=is.null"
                        "&order=scraped_at.desc"
                        f"&limit={take}")
        if not batch:
            logger.info("queue empty")
            break
        urls = [f"{SITE}/jobs/{make_slug(j)}" for j in batch]
        try:
            bing(env, "SubmitUrlBatch", {"siteUrl": SITE, "urlList": urls})
        except urllib.error.HTTPError as e:
            logger.error("batch submit failed: HTTP %s %s", e.code, e.read().decode()[:300])
            break
        ids = ",".join(str(j["id"]) for j in batch)
        sb(env, f"hospital_jobs?id=in.({ids})", method="PATCH",
           body={"bing_submitted_at": now}, prefer="return=minimal")
        submitted += len(batch)
        logger.info("submitted %d/%d", submitted, budget)
        if len(batch) < take:
            break

    logger.info("DONE: bing_submitted=%d", submitted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
