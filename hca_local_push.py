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

    python hca_local_push.py                                         # crawl, save, push
    python hca_local_push.py --replay logs/hca_rows_20260923.json    # push a saved crawl
    python hca_local_push.py --replay logs/hca_rows_20260923.json --check   # validate only

Takes ~13-25 min (24 states, ~17k jobs, polite pacing; 16,941 in 752 s on 2026-09-10). Writes to the real
hospital_jobs table with the standard per-system sweep, so re-running it also
retires HCA listings that disappeared since the last push. The nightly's
sweep guard (scraper.py, 2026-07-29) and the PARTIAL_SYSTEMS flag
(2026-09-10) keep a blocked Railway run from deactivating what this pushes.

2026-09-24 — SAVED ROWS AND --replay. The 09-23 run crawled 17,153 jobs and
fetched 16,868 descriptions (2.5 hours), then the upsert stopped at its first
failed batch (rows 6,500-6,999: HTTP 500, 57014 statement timeout) and
10,653 rows never landed. Every crawl now saves its finalized rows, after the
detail pass and BEFORE the upsert, to logs/hca_rows_<YYYYMMDD>.json (the
local date the run started, like logs/run_<date>.log; the newest
HCA_ROWS_KEEP files, default 7, are kept; 0 keeps them all). --replay <file> upserts a saved
file through the same shared upsert without crawling. A replay stamps the
rows with the crawl's own run start, so the sweep retires exactly what that
crawl did not see; a file saved from a PARTIAL crawl is never swept, and a
file older than REPLAY_MAX_AGE_HOURS is refused unless --allow-stale, which
re-sends the rows without a sweep.
"""
import argparse
import asyncio
import json
import os
import re
import time
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# Env: prefer the shell / repo .env; fall back to the shared-folder .env.local
# (maps SUPABASE_SERVICE_ROLE_KEY -> SUPABASE_KEY for the writer). 2026-09-10:
# the shared folder lives under a different user name on each of the two
# boxes, so every known location is tried.
SHARED_ENV_CANDIDATES = [
    os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", ".env.local"),
    r"C:\Users\rober\OneDrive\claud_outputinput\.env.local",
    r"C:\Users\19403\OneDrive\claud_outputinput\.env.local",
]


def _load_shared_env() -> None:
    """2026-09-24: called from main() rather than at import, so importing this
    module (the tests do) never picks up write credentials. scraper reads the
    SUPABASE_* variables when it calls out, not at import, so the order is safe."""
    if os.environ.get("SUPABASE_KEY"):
        return
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


import scraper  # noqa: E402

HCA_SYSTEM = "HCA Healthcare"
LOGS_DIR = os.path.join(HERE, "logs")
ROWS_FORMAT = "hca_rows/1"
ROWS_FILE_RE = re.compile(r"^hca_rows_\d{8}\.json$")
ROWS_KEEP = int(os.environ.get("HCA_ROWS_KEEP", "7"))
REPLAY_MAX_AGE_HOURS = 48
# Upsert order: these states first, the rest in crawl order. On 09-23 the
# upsert stopped after 6,500 rows in crawl order (CO, CA, ...), so Texas's
# 4,473 rows were among the ones that never landed.
PUSH_FIRST_STATES = ("TX",)


class ReplayError(ValueError):
    """A saved rows file that must not be pushed."""


def _hca_detail_pass(jobs) -> None:
    import random
    from concurrent.futures import ThreadPoolExecutor
    budget = int(os.environ.get("HCA_DESC_MAX_PER_RUN", "2000"))
    workers = int(os.environ.get("HCA_DESC_WORKERS", "4"))
    if budget <= 0:
        return
    try:
        from curl_cffi import requests as _cr
    except ImportError:
        print("curl_cffi not installed — skipping the HCA detail pass")
        return
    cands = [j for j in jobs if (j.url or "").startswith("http") and len((j.description or "").strip()) < scraper.DETAIL_MIN_CHARS]
    random.shuffle(cands)
    cands.sort(key=lambda j: 0 if (j.state or "").strip().upper() in scraper.DETAIL_PRIORITY_STATES else 1)
    pending = cands[:budget]
    if not pending:
        return
    filled = 0

    def one(job):
        nonlocal filled
        try:
            r = _cr.get(job.url, impersonate="chrome", timeout=25)
            if r.status_code != 200:
                return
            posting = scraper._jobposting_from_html(r.text)
            if posting and scraper._apply_posting(job, posting):
                filled += 1
        except Exception:
            return
        time.sleep(random.uniform(0.2, 0.6))

    print(f"HCA detail pass: {len(pending)} of {len(cands)} pages without a description (budget {budget}, {workers} workers)")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, pending))
    print(f"HCA detail pass: {filled} descriptions landed")


# ── Saved rows (2026-09-24) ────────────────────────────────────────────────
def rows_file_path(run_date: str, logs_dir: str = LOGS_DIR) -> str:
    return os.path.join(logs_dir, f"hca_rows_{run_date}.json")


def _prune_rows_files(logs_dir: str, keep: int) -> None:
    """Keep the newest `keep` hca_rows_<date>.json files (about 100 MB each)."""
    if keep <= 0:
        return
    try:
        names = sorted((n for n in os.listdir(logs_dir) if ROWS_FILE_RE.match(n)), reverse=True)
        for n in names[keep:]:
            os.remove(os.path.join(logs_dir, n))
            print(f"Removed old rows file {n} (keeping the newest {keep})")
    except OSError as e:
        print(f"WARNING: could not prune old rows files in {logs_dir} ({e})")


def save_rows(rows: list[dict], path: str, run_started_iso: str,
              partial: bool, failed_slices: list[str]) -> str | None:
    """Write the finalized rows before the upsert, atomically (tmp + rename).
    Never fatal: a save that fails is reported and the push goes ahead."""
    payload = {
        "format": ROWS_FORMAT,
        "hospital_system": HCA_SYSTEM,
        "run_started_iso": run_started_iso,
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partial": bool(partial),
        "failed_slices": list(failed_slices),
        "row_count": len(rows),
        "rows": rows,
    }
    tmp = path + ".tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), default=str)
        os.replace(tmp, path)
    except Exception as e:
        print(f"WARNING: could not save the rows to {path} ({e}); pushing anyway, "
              f"but a push that fails part way cannot be replayed")
        return None
    print(f"Saved {len(rows):,} finalized rows to {path}")
    _prune_rows_files(os.path.dirname(path) or ".", ROWS_KEEP)
    return path


def load_rows_file(path: str, now: datetime | None = None) -> dict:
    """Read and validate a saved rows file. Raises ReplayError for anything
    that must not reach the database: another format, a truncated file, rows
    that are not HCA rows with a job_id and title, or a missing run start.
    Rows repeating a job_id are dropped (one upsert batch cannot touch the
    same row twice). The age policy is replay()'s, from the returned
    age_hours."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise ReplayError(f"{path}: no such file")
    except (OSError, ValueError) as e:
        raise ReplayError(f"{path}: unreadable ({e})")
    if not isinstance(data, dict) or data.get("format") != ROWS_FORMAT:
        got = data.get("format") if isinstance(data, dict) else type(data).__name__
        raise ReplayError(f"{path}: not an HCA rows file (format {got!r}, expected {ROWS_FORMAT!r})")
    hca_names = {HCA_SYSTEM, scraper.HOSPITAL_SYSTEM_ALIASES.get(HCA_SYSTEM, HCA_SYSTEM)}
    if data.get("hospital_system") not in hca_names:
        raise ReplayError(f"{path}: saved for {data.get('hospital_system')!r}, not {HCA_SYSTEM!r}")
    rows = data.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReplayError(f"{path}: no rows")
    if data.get("row_count") != len(rows):
        raise ReplayError(f"{path}: header says {data.get('row_count')} rows but the file holds "
                          f"{len(rows)} (truncated or edited)")
    bad = [r for r in rows
           if not isinstance(r, dict) or r.get("hospital_system") not in hca_names
           or not str(r.get("job_id") or "").strip() or not str(r.get("title") or "").strip()]
    if bad:
        first = bad[0] if isinstance(bad[0], dict) else {}
        raise ReplayError(f"{path}: {len(bad)} row(s) are not HCA rows with a job_id and title "
                          f"(first: system={first.get('hospital_system')!r} job_id={first.get('job_id')!r})")
    try:
        started = datetime.fromisoformat(str(data.get("run_started_iso") or "").replace("Z", "+00:00"))
    except ValueError:
        raise ReplayError(f"{path}: bad run_started_iso {data.get('run_started_iso')!r}")
    if started.tzinfo is None:
        raise ReplayError(f"{path}: run_started_iso {data.get('run_started_iso')!r} has no time zone")
    seen: set[str] = set()
    unique: list[dict] = []
    for r in rows:
        jid = str(r["job_id"])
        if jid in seen:
            continue
        seen.add(jid)
        unique.append(r)
    now = now or datetime.now(timezone.utc)
    return {
        "rows": unique,
        "run_started_iso": data["run_started_iso"],
        "partial": bool(data.get("partial")),
        "failed_slices": list(data.get("failed_slices") or []),
        "age_hours": (now - started).total_seconds() / 3600,
        "dupes": len(rows) - len(unique),
    }


# ── Push ───────────────────────────────────────────────────────────────────
def _push_order(rows: list[dict]) -> list[dict]:
    """Stable: PUSH_FIRST_STATES first, everything else in its crawl order."""
    rank = {s: i for i, s in enumerate(PUSH_FIRST_STATES)}
    return sorted(rows, key=lambda r: rank.get((r.get("state") or "").strip().upper(), len(rank)))


def push_rows(rows: list[dict], run_started_iso: str, saved_path: str | None = None) -> int:
    """Upsert through the shared scraper._upsert_hospital_jobs_to_supabase
    (the same retry/split path and per-system sweep as the nightly). Returns
    the number of rows sent."""
    rows = _push_order(rows)
    n_states = len({r["state"] for r in rows if r.get("state")})
    print(f"Pushing {len(rows):,} HCA rows ({n_states} states, {'/'.join(PUSH_FIRST_STATES)} first) "
          f"to Supabase (upsert + per-system sweep, run_started={run_started_iso})...")
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, run_started_iso)
    print(f"DONE: {sent:,} of {len(rows):,} rows sent.")
    if sent < len(rows):
        how = (f"python hca_local_push.py --replay {saved_path}" if saved_path
               else "no saved file; re-run the crawl")
        print(f"INCOMPLETE: {len(rows) - sent:,} rows did not land and HCA was not swept. "
              f"Re-send without crawling: {how}")
    return sent


def replay(path: str, allow_stale: bool = False, check_only: bool = False) -> int:
    """Upsert a saved rows file without crawling. Returns an exit code:
    0 = every row sent (or --check passed), 1 = the upsert was incomplete.
    Raises ReplayError when the file must not be pushed."""
    data = load_rows_file(path)
    rows = data["rows"]
    bodies = sum(1 for r in rows if len((r.get("description") or "").strip()) >= scraper.DETAIL_MIN_CHARS)
    states: dict[str, int] = {}
    for r in rows:
        st = (r.get("state") or "").strip().upper() or "??"
        states[st] = states.get(st, 0) + 1
    top = ", ".join(f"{s} {n:,}" for s, n in sorted(states.items(), key=lambda kv: -kv[1])[:8])
    print(f"Replay {path}: {len(rows):,} rows, {bodies:,} with a full description, "
          f"{len(states)} states ({top}); crawled {data['run_started_iso']} "
          f"({data['age_hours']:.1f} h ago)"
          + (f"; {data['dupes']} repeated job ids dropped" if data["dupes"] else "")
          + ("; PARTIAL crawl " + str(data["failed_slices"]) if data["partial"] else ""))
    stale = data["age_hours"] > REPLAY_MAX_AGE_HOURS
    if check_only:
        print("CHECK ONLY: nothing sent."
              + (f" The file is older than {REPLAY_MAX_AGE_HOURS} h; a replay needs --allow-stale." if stale else ""))
        return 0
    if stale and not allow_stale:
        raise ReplayError(f"{path} is {data['age_hours']:.0f} h old (limit {REPLAY_MAX_AGE_HOURS} h): its rows "
                          f"would re-open listings that closed since. Re-crawl, or pass --allow-stale "
                          f"to re-send them without a sweep")
    if data["partial"]:
        scraper.PARTIAL_SYSTEMS.add(HCA_SYSTEM)
        print("NOTE: the saved crawl was PARTIAL; rows will be upserted, nothing swept.")
    elif stale:
        scraper.PARTIAL_SYSTEMS.add(HCA_SYSTEM)
        print("NOTE: --allow-stale; rows will be upserted, nothing swept.")
    sent = push_rows(rows, data["run_started_iso"], saved_path=path)
    return 0 if sent >= len(rows) else 1


def crawl_and_push() -> int:
    run_date = datetime.now().strftime("%Y%m%d")
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    jobs = asyncio.run(scraper.run_hca(None))
    if len(jobs) < 1000:
        print(f"ABORT: only {len(jobs)} HCA jobs scraped (expected ~17k). "
              f"Not pushing — check the log lines above (403 = Cloudflare).")
        return 2
    partial = HCA_SYSTEM in scraper.PARTIAL_SYSTEMS
    failed = list(scraper._HCA_FAILED_SLICES)
    if partial:
        # 2026-09-10: a slice that never finished means this is not the
        # inventory; the upsert still lands the rows but skips the sweep.
        print(f"NOTE: run_hca reported a PARTIAL crawl ({len(failed)} slice(s) did not finish: "
              f"{failed}); rows will be upserted, nothing swept.")

    # Same dedupe + normalize as scrape()'s tail (finalize_jobs, 2026-09-10),
    # including the CMS blank-state fill, which needs the credentials above.
    scraper.load_cms_lookup()
    # 2026-09-21 (owner, job page v2): HCA's list carries no posting body, and
    # Railway cannot read the job pages (Cloudflare). This machine can: fetch a
    # budget of job pages and take the JSON-LD JobPosting (description,
    # employment type, posted date). The enrichment trigger keeps what lands.
    _hca_detail_pass(jobs)

    rows = scraper.finalize_jobs(jobs)
    saved = save_rows(rows, rows_file_path(run_date), run_started_iso, partial, failed)
    sent = push_rows(rows, run_started_iso, saved_path=saved)
    return 0 if sent >= len(rows) else 1


def _parse_args(argv):
    p = argparse.ArgumentParser(description="HCA crawl from a residential IP, pushed to Supabase.")
    p.add_argument("--replay", metavar="FILE",
                   help="upsert a saved logs/hca_rows_<date>.json without crawling")
    p.add_argument("--check", action="store_true",
                   help="with --replay: validate and summarize the file, send nothing")
    p.add_argument("--allow-stale", action="store_true",
                   help=f"with --replay: accept a file older than {REPLAY_MAX_AGE_HOURS} h "
                        f"(rows re-sent, nothing swept)")
    args = p.parse_args(argv)
    if (args.check or args.allow_stale) and not args.replay:
        p.error("--check and --allow-stale go with --replay FILE")
    return args


def main(argv=None) -> int:
    args = _parse_args(argv)
    try:
        sys.stdout.reconfigure(line_buffering=True)   # keep print() in order with the log lines
    except Exception:
        pass
    if args.replay:
        if not args.check:
            _load_shared_env()
        try:
            return replay(args.replay, allow_stale=args.allow_stale, check_only=args.check)
        except ReplayError as e:
            print(f"REPLAY REFUSED: {e}")
            return 2
    _load_shared_env()
    # This machine's own IP is residential — that's the whole point of running
    # here. Disable the (currently 402-exhausted) webshare pool so the crawl
    # doesn't route through dead proxies.
    scraper.proxies.proxies = []
    return crawl_and_push()


if __name__ == "__main__":
    raise SystemExit(main())
