"""
HCA local push — run HCA's browserless crawl from a RESIDENTIAL IP and upsert
the results to Supabase through the same pipeline the nightly uses.

WHY THIS EXISTS: Cloudflare on careers.hcahealthcare.com admits the Firefox
TLS fingerprint from residential IPs (this machine) but 403s / rate-limits
Railway's datacenter IP, so the nightly covers only the slices that slip
through (3,368 rows on 2026-09-10 at page boundaries of 1000 / 500 / 500 /
500, against 16,943 unique ids from a home IP the same day). Until the
webshare residential pool is funded, run this from home, by hand or from a
nightly scheduled task (see reports/S-scraper-2.md for the one-line
schtasks command):

    python hca_local_push.py

Takes ~13-25 min (24 states, ~17k jobs, polite pacing; 16,941 in 752 s on 2026-09-10). Writes to the real
hospital_jobs table with the standard per-system sweep, so re-running it also
retires HCA listings that disappeared since the last push. The nightly's
sweep guard (scraper.py, 2026-07-29) and the PARTIAL_SYSTEMS flag
(2026-09-10) keep a blocked Railway run from deactivating what this pushes.
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Env: prefer the shell / repo .env; fall back to the shared-folder .env.local
# (maps SUPABASE_SERVICE_ROLE_KEY -> SUPABASE_KEY for the writer). 2026-09-10:
# the shared folder lives under a different user name on each of the two
# boxes, so every known location is tried.
SHARED_ENV_CANDIDATES = [
    os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", ".env.local"),
    r"C:\Users\rober\OneDrive\claud_outputinput\.env.local",
    r"C:\Users\19403\OneDrive\claud_outputinput\.env.local",
]
if not os.environ.get("SUPABASE_KEY"):
    for shared_env in SHARED_ENV_CANDIDATES:
        if not os.path.exists(shared_env):
            continue
        shared = {}
        for line in open(shared_env, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                shared[k.strip()] = v.strip()
        os.environ.setdefault("SUPABASE_URL", shared.get("SUPABASE_URL",
                              "https://tlzxajgonuevbqaelhvf.supabase.co"))
        if shared.get("SUPABASE_SERVICE_ROLE_KEY"):
            os.environ.setdefault("SUPABASE_KEY", shared["SUPABASE_SERVICE_ROLE_KEY"])
        break

import scraper  # noqa: E402  (imports after env setup on purpose)

# This machine's own IP is residential — that's the whole point of running
# here. Disable the (currently 402-exhausted) webshare pool so the crawl
# doesn't route through dead proxies.
scraper.proxies.proxies = []


def main() -> None:
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    jobs = asyncio.run(scraper.run_hca(None))
    if len(jobs) < 1000:
        print(f"ABORT: only {len(jobs)} HCA jobs scraped (expected ~17k). "
              f"Not pushing — check the log lines above (403 = Cloudflare).")
        return
    if "HCA Healthcare" in scraper.PARTIAL_SYSTEMS:
        # 2026-09-10: a slice that never finished means this is not the
        # inventory; the upsert still lands the rows but skips the sweep.
        print("NOTE: run_hca reported a PARTIAL crawl; rows will be upserted, nothing swept.")

    # Same dedupe + normalize as scrape()'s tail (finalize_jobs, 2026-09-10),
    # including the CMS blank-state fill, which needs the credentials above.
    scraper.load_cms_lookup()
    rows = scraper.finalize_jobs(jobs)

    print(f"Pushing {len(rows):,} HCA rows to Supabase "
          f"(upsert + per-system sweep, run_started={run_started_iso})...")
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, run_started_iso)
    print(f"DONE: {sent:,} rows sent across {len({r['state'] for r in rows if r.get('state')})} states.")


if __name__ == "__main__":
    main()
