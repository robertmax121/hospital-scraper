"""
HCA local push — run HCA's browserless crawl from a RESIDENTIAL IP and upsert
the results to Supabase through the same pipeline the nightly uses.

WHY THIS EXISTS: Cloudflare on careers.hcahealthcare.com admits the Firefox
TLS fingerprint from residential IPs (this machine) but 403s Railway's
datacenter IP, so the nightly can only cover HCA once the webshare residential
proxy account has bandwidth. Until then, run this manually from home:

    python hca_local_push.py

Takes ~15-25 min (23 states, ~16k jobs, polite pacing). Writes to the real
hospital_jobs table with the standard per-system sweep, so re-running it also
retires HCA listings that disappeared since the last push. The nightly's
sweep guard (scraper.py, 2026-07-29) keeps a blocked Railway run from
deactivating what this pushes.
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Env: prefer the shell / repo .env; fall back to the shared-folder .env.local
# (maps SUPABASE_SERVICE_ROLE_KEY -> SUPABASE_KEY for the writer).
SHARED_ENV = r"C:\Users\rober\OneDrive\claud_outputinput\.env.local"
if not os.environ.get("SUPABASE_KEY") and os.path.exists(SHARED_ENV):
    shared = {}
    for line in open(SHARED_ENV, encoding="utf-8"):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            shared[k.strip()] = v.strip()
    os.environ.setdefault("SUPABASE_URL", shared.get("SUPABASE_URL",
                          "https://tlzxajgonuevbqaelhvf.supabase.co"))
    if shared.get("SUPABASE_SERVICE_ROLE_KEY"):
        os.environ.setdefault("SUPABASE_KEY", shared["SUPABASE_SERVICE_ROLE_KEY"])

import scraper  # noqa: E402  (imports after env setup on purpose)

# This machine's own IP is residential — that's the whole point of running
# here. Disable the (currently 402-exhausted) webshare pool so the crawl
# doesn't route through dead proxies.
scraper.proxies.proxies = []


def main() -> None:
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    jobs = asyncio.run(scraper.run_hca(None))
    if len(jobs) < 1000:
        print(f"ABORT: only {len(jobs)} HCA jobs scraped (expected ~16k). "
              f"Not pushing — check the log lines above (403 = Cloudflare).")
        return

    # Same dedupe + normalize as scrape()'s tail.
    seen, rows = set(), []
    for j in jobs:
        key = f"{j.ats_platform}::{j.hospital_system}::{j.job_id}"
        if key not in seen and j.job_id and j.title:
            seen.add(key)
            rows.append(scraper.normalize_job(j))

    print(f"Pushing {len(rows):,} HCA rows to Supabase "
          f"(upsert + per-system sweep, run_started={run_started_iso})...")
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, run_started_iso)
    print(f"DONE: {sent:,} rows sent. TX facilities now live on the board.")


if __name__ == "__main__":
    main()
