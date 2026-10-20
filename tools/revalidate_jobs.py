"""
Purge the site's cached job pages for hospital_jobs ids (2026-10-15).

Run it after retirement SQL applied by hand (sql/150b, verify_stale's sql/115
kind, a URL rewrite such as sql/150): the nightly purges what IT retires
(site_revalidate, scheduler STEP 2f), but rows flipped or rewritten by SQL keep
their cached page, Apply button and link for up to ~12 h otherwise.

It POSTs the ids to the site's /api/revalidate/jobs in batches of 500 with
Authorization: Bearer <CRON_SECRET> (the site's cron credential), through the
same code the nightly uses (site_revalidate.post_ids). Idempotent: a repeat
only makes each page's next visit re-read its row.

Ids, any mix (deduplicated, order kept):
    python -I tools/revalidate_jobs.py 28140695 28140696
    python -I tools/revalidate_jobs.py --ids 28140695,28140696
    python -I tools/revalidate_jobs.py --file ids.txt         (any separators)
    some_command | python -I tools/revalidate_jobs.py --file -
    python -I tools/revalidate_jobs.py --sql "select id from public.retired_closed_backup_20261015
        where reason like 'hca stale 150b%' and flipped_at is not null"
--sql reads through the read-only wp.sql helper (claud_outputinput/tools/wp.py),
data project only, and refuses to start between 23:30 and 05:30 UTC.

Release steps of 2026-10-15 (run once each, after the site endpoint is live):
    # the owner's example plus the hand retirements of the day (145, 146, 146c;
    # the backup table exists only once 146 or 146c is applied, else just the id):
    python -I tools/revalidate_jobs.py 28140695 --sql "select id from
        public.retired_closed_backup_20261015 where flipped_at is not null"
    # Norton's new Apply links (after the first nightly on JIBE_POSTING_URL, or
    # after sql/150, whichever moved them; nothing purges a changed URL by itself):
    python -I tools/revalidate_jobs.py --sql "select id from public.hospital_jobs
        where hospital_system = 'Norton Healthcare' and is_active"


Credential: CRON_SECRET from the environment, or --env-file PATH (a dotenv
file; only CRON_SECRET and SITE_URL are read from it, nothing is printed).
--site URL (or SITE_URL) points at a preview deployment; https only.
--batch is at most 500 ids per POST (review 10-15: the site looks ids up in
200-id chunks with an 8 s abort each and purges nothing if a call outlives
its 60 s maxDuration, so a 2,000-id call could time out on a slow database).
--dry-run shows what would be sent and sends nothing.

Exit: 0 every id sent (or none to send), 1 some ids not sent, 2 not run
(no credential, bad arguments, quiet window).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)

import site_revalidate  # noqa: E402

WP_DIRS = [
    os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", "tools"),
    r"C:\Users\19403\OneDrive\claud_outputinput\tools",
    r"C:\Users\rober\OneDrive\claud_outputinput\tools",
]
QUIET_UTC = (23 * 60 + 30, 5 * 60 + 30)   # --sql refuses to start in this window (UTC minutes)
MAX_BATCH = 500   # ids per POST; see --batch above
_ID_RX = re.compile(r"\d+")
_READ_ONLY_RX = re.compile(r"^\s*(select|with)\b", re.I)
_WRITE_RX = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call|do)\b", re.I)


def ids_from_text(text: str) -> list[int]:
    """Every run of digits in text, as ints, deduplicated in order."""
    out, seen = [], set()
    for m in _ID_RX.finditer(text or ""):
        i = int(m.group(0))
        if i > 0 and i not in seen:
            seen.add(i)
            out.append(i)
    return out


def read_env_file(path: str) -> dict:
    """REVALIDATE_SECRET, CRON_SECRET and SITE_URL from a dotenv file (quotes stripped)."""
    out = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k.startswith("export "):
                k = k[len("export "):].strip()
            if k in (site_revalidate.SECRET_ENV, site_revalidate.FALLBACK_SECRET_ENV, site_revalidate.SITE_ENV):
                out[k] = v.strip().strip('"').strip("'")
    return out


def in_quiet_window(now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    m = now.hour * 60 + now.minute
    lo, hi = QUIET_UTC
    return m >= lo or m < hi


def ids_from_sql(query: str, now: datetime | None = None) -> list[int]:
    if not _READ_ONLY_RX.match(query or "") or _WRITE_RX.search(query or ""):
        raise SystemExit("--sql takes one read-only SELECT (or WITH ... SELECT) that returns an id column")
    if in_quiet_window(now):
        raise SystemExit("--sql: no database reads between 23:30 and 05:30 UTC")
    wp = None
    for d in WP_DIRS:
        if os.path.exists(os.path.join(d, "wp.py")):
            sys.path.insert(0, d)
            import wp  # noqa: E402
            break
    if wp is None:
        raise SystemExit("wp.py (the read-only SQL helper) was not found in " + " / ".join(WP_DIRS))
    try:
        rows = wp.sql(query) or []
    except Exception as e:     # a missing table (SQL not applied yet), a syntax error, ...
        raise SystemExit(f"--sql failed, nothing sent: {str(e)[:300]}")
    out, seen = [], set()
    for r in rows:
        v = r.get("id") if isinstance(r, dict) else None
        if v is None and isinstance(r, dict) and len(r) == 1:
            v = next(iter(r.values()))
        try:
            i = int(v)
        except (TypeError, ValueError):
            continue
        if i > 0 and i not in seen:
            seen.add(i)
            out.append(i)
    return out


def _parse_args(argv):
    p = argparse.ArgumentParser(description="Purge the site's cached job pages for hospital_jobs ids.")
    p.add_argument("ids", nargs="*", help="hospital_jobs ids")
    p.add_argument("--ids", dest="ids_csv", action="append", default=[], help="comma-separated ids")
    p.add_argument("--file", help="a file of ids, any separators; - for stdin")
    p.add_argument("--sql", help="a read-only SELECT returning an id column (wp.sql)")
    p.add_argument("--env-file", help="dotenv file to read REVALIDATE_SECRET or CRON_SECRET (and SITE_URL) from")
    p.add_argument("--site", help="site origin (default SITE_URL or https://jobs.waypointrecruit.com)")
    p.add_argument("--batch", type=int, default=site_revalidate.BATCH, help=f"ids per POST (max {MAX_BATCH})")
    p.add_argument("--max-ids", type=int, default=100000, help="refuse more ids than this")
    p.add_argument("--dry-run", action="store_true", help="show what would be sent; send nothing")
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _parse_args(argv)
    text = " ".join(args.ids) + " " + " ".join(args.ids_csv)
    if args.file:
        text += " " + (sys.stdin.read() if args.file == "-" else open(args.file, encoding="utf-8").read())
    ids = ids_from_text(text)
    if args.sql:
        seen = set(ids)
        ids += [i for i in ids_from_sql(args.sql) if i not in seen]
    if not ids:
        print("No ids given; nothing to purge.")
        return 0
    if len(ids) > args.max_ids:
        print(f"REFUSED: {len(ids):,} ids is over --max-ids {args.max_ids:,}")
        return 2
    batch = max(1, min(int(args.batch), MAX_BATCH))
    secret = site_revalidate._env_secret()
    site = args.site
    if args.env_file:
        env = read_env_file(args.env_file)
        secret = secret or (env.get(site_revalidate.SECRET_ENV, "").strip()
                            or env.get(site_revalidate.FALLBACK_SECRET_ENV, "").strip())
        site = site or env.get(site_revalidate.SITE_ENV)
    base = site_revalidate._site_base(site)
    print(f"{len(ids):,} id(s) -> {(base or '(refused origin)') + site_revalidate.ENDPOINT_PATH} "
          f"in {-(-len(ids) // batch)} batch(es) of up to {batch}; first: {ids[:5]}")
    if args.dry_run:
        print("DRY RUN: nothing sent."
              + ("" if secret else f" ({site_revalidate.SECRET_ENV} is not set; a real run would skip.)"))
        return 0
    if not secret:
        print(f"NOT RUN: {site_revalidate.SECRET_ENV} is not set. Export it (the value of the site's "
              f"{site_revalidate.SECRET_ENV} on Vercel) or pass --env-file PATH.")
        return 2
    out = site_revalidate.post_ids(ids, secret=secret, site=site, batch=batch, label="manual",
                                   max_ids=len(ids), deadline_sec=max(site_revalidate.DEADLINE_SEC,
                                                                      len(ids) / batch * 10))
    print(json.dumps(out, sort_keys=True))
    if out.get("skipped"):
        return 2
    return 0 if out.get("sent", 0) >= len(ids) else 1


if __name__ == "__main__":
    raise SystemExit(main())
