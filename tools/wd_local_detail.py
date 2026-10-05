"""
wd_local_detail.py - one-off local pass over the Workday no-description backlog
(plan of record 2026-10-04, item 8b).

WHY: 42,314 active Workday rows (74% of every row with no description) have
never had a successful CXS detail fetch. The endpoint answers this machine
(3 of 3 probed rows: 200 with a 3,507 / 15,450 / 8,048-character
jobDescription), so this script reads the CXS detail for every active Workday
row whose stored body is under 200 characters and upserts what lands through
the nightly's own pipeline:

    row url -> scraper._workday_cxs_url (the /wday/cxs/{tenant}/ form)
            -> GET, 16 in flight overall, 4 per host, 0.15-0.45 s pause
            -> scraper._apply_wd_posting_info (body, timeType, ISO startDate)
            -> scraper.finalize_jobs (normalize, facts, pay)
            -> scraper._upsert_hospital_jobs_to_supabase

Modelled on hca_local_push._hca_detail_pass. The enrichment trigger keeps
everything a row already holds that this pass does not improve.

SAFETY
  * Nothing is written without --write. --dry-run reads candidates through the
    read-only wp.sql helper (SELECT-guarded), fetches the details, prints counts
    per tenant and three sample bodies, and never loads write credentials.
  * Every system in a write batch is put in scraper.PARTIAL_SYSTEMS, so the
    upsert's per-system sweep never runs: this pass sees only the rows without
    a body, and a sweep would retire every other row of the tenant.
  * The upsert stamps scraped_at (and is_active) on the rows it sends, which
    would shield those rows from a nightly sweep running at the same time. The
    write mode therefore refuses to start between 23:30 and 06:00 UTC (the
    nightly starts at 00:09) and must not run while a deploy-triggered Railway
    run is in flight.
  * Rows are read and pushed in chunks (--chunk, default 2,000), so a stop part
    way keeps what landed; re-running skips rows that now hold a body.

USAGE (owner's go required for --write)
    python tools/wd_local_detail.py --dry-run --limit 60 --every 101
    python tools/wd_local_detail.py --write [--limit N] [--chunk 2000] [--system "Trinity Health"]
"""
import argparse
import collections
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlsplit

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)

WP_DIRS = [
    os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", "tools"),
    r"C:\Users\19403\OneDrive\claud_outputinput\tools",
    r"C:\Users\rober\OneDrive\claud_outputinput\tools",
]
IN_FLIGHT = 16           # requests in flight overall
PER_HOST = 4             # requests in flight per Workday host
PAUSE = (0.15, 0.45)     # seconds, held inside the host gate after each request
TIMEOUT = 25
QUIET_UTC = (23 * 60 + 30, 6 * 60)   # write mode refuses to start between 23:30 and 06:00 UTC
COLUMNS = ("id,hospital_system,hospital_name,job_id,title,location,city,state,specialty,job_type,url,"
           "posted_date,description,ats_platform,wage_min,wage_max,wage_unit")


def _wp():
    for d in WP_DIRS:
        if os.path.exists(os.path.join(d, "wp.py")):
            sys.path.insert(0, d)
            import wp  # noqa: E402
            return wp
    raise SystemExit("wp.py (the read-only SQL helper) was not found in " + " / ".join(WP_DIRS))


def _q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def candidate_sql(last_id: int, page: int, system: str = "", every: int = 1) -> str:
    """Active Workday rows whose stored body is under 200 characters, keyset-
    paged on id. Light on the database: no description scan, an id-ordered
    page of short rows."""
    sys_sql = f" and hospital_system = {_q(system)}" if system else ""
    if every > 1:
        sys_sql += f" and id % {int(every)} = 0"
    return (f"select {COLUMNS} from public.hospital_jobs"
            f" where is_active and ats_platform = 'Workday' and coalesce(desc_len, 0) < 200"
            f"{sys_sql} and id > {int(last_id)} order by id limit {int(page)}")


def job_from_row(S, r: dict):
    """A stored hospital_jobs row -> scraper.Job, so the nightly's own
    normalize / facts / upsert path handles it unchanged."""
    def num(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    return S.Job(
        title=r.get("title") or "", hospital_system=r.get("hospital_system") or "",
        hospital_name=r.get("hospital_name") or "", city=r.get("city") or "",
        state=r.get("state") or "", location=r.get("location") or "",
        specialty=r.get("specialty") or "", job_type=r.get("job_type") or "",
        url=r.get("url") or "", job_id=str(r.get("job_id") or ""),
        posted_date=r.get("posted_date") or "", description=r.get("description") or "",
        ats_platform=r.get("ats_platform") or "Workday",
        wage_min=num(r.get("wage_min")), wage_max=num(r.get("wage_max")), wage_unit=r.get("wage_unit"),
    )


def round_robin_by_host(items, host_of):
    """Interleave hosts so the 16 workers spread over tenants instead of
    queueing four at a time behind one host."""
    by = collections.OrderedDict()
    for it in items:
        by.setdefault(host_of(it), []).append(it)
    out, queues = [], list(by.values())
    while queues:
        nxt = []
        for qq in queues:
            out.append(qq.pop(0))
            if qq:
                nxt.append(qq)
        queues = nxt
    return out


class Fetcher:
    """CXS detail GETs: IN_FLIGHT overall, PER_HOST per host, a pause inside
    the host gate after each request (the nightly's Workday pacing)."""

    def __init__(self, get=None, pause=PAUSE, per_host=PER_HOST):
        self.get = get or self._default_get()
        self.pause = pause
        self.per_host = per_host
        self._gates, self._lock = {}, threading.Lock()

    @staticmethod
    def _default_get():
        try:
            from curl_cffi import requests as cr

            def get(url):
                r = cr.get(url, impersonate="chrome", timeout=TIMEOUT,
                           headers={"Accept": "application/json", "Accept-Language": "en-US,en;q=0.9"})
                return r.status_code, (r.json() if r.status_code == 200 else None)
        except ImportError:
            import urllib.request
            import urllib.error

            def get(url):
                rq = urllib.request.Request(url, headers={"Accept": "application/json",
                                                          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                try:
                    with urllib.request.urlopen(rq, timeout=TIMEOUT) as resp:
                        return resp.status, json.loads(resp.read().decode("utf-8", "replace"))
                except urllib.error.HTTPError as e:
                    return e.code, None
        return get

    def _gate(self, host):
        with self._lock:
            g = self._gates.get(host)
            if g is None:
                g = self._gates[host] = threading.Semaphore(self.per_host)
            return g

    def fetch(self, cxs_url):
        """(status, jobPostingInfo or None). Never raises."""
        host = (urlsplit(cxs_url).hostname or "").lower()
        with self._gate(host):
            try:
                status, data = self.get(cxs_url)
            except Exception as e:
                status, data = type(e).__name__, None
            if self.pause:
                time.sleep(random.uniform(*self.pause))
        info = (data or {}).get("jobPostingInfo") if isinstance(data, dict) else None
        return status, (info if isinstance(info, dict) else None)


def detail_pass(S, rows, fetcher, workers=IN_FLIGHT):
    """Fetch the CXS detail for each row. Returns (changed Jobs, stats,
    per-system Counter, samples). A Job is 'changed' when the posting info
    gave it a body (_apply_wd_posting_info True), an ISO date or a type."""
    stats = collections.Counter()
    per_sys = collections.defaultdict(collections.Counter)
    samples, changed = [], []
    lock = threading.Lock()
    work = []
    for r in rows:
        cxs = S._workday_cxs_url(r.get("url") or "")
        if not cxs:
            stats["url is not a Workday job URL"] += 1
            per_sys[r.get("hospital_system")]["no cxs url"] += 1
            continue
        work.append((r, cxs))
    work = round_robin_by_host(work, lambda t: (urlsplit(t[1]).hostname or "").lower())

    def one(item):
        r, cxs = item
        job = job_from_row(S, r)
        status, info = fetcher.fetch(cxs)
        system = r.get("hospital_system")
        was_date, was_type = job.posted_date, job.job_type
        body = S._apply_wd_posting_info(job, info) if info is not None else False
        with lock:
            stats["fetched"] += 1
            per_sys[system]["fetched"] += 1
            stats[f"HTTP {status}"] += 1
            if status != 200:
                per_sys[system][f"HTTP {status}"] += 1
            if body:
                stats["descriptions"] += 1
                per_sys[system]["descriptions"] += 1
                if len(samples) < 3 and system not in {s["system"] for s in samples}:
                    samples.append({"system": system, "id": r.get("id"), "title": job.title,
                                    "url": cxs, "chars": len(job.description), "body": job.description})
            if job.posted_date != was_date:
                stats["ISO dates"] += 1
            if job.job_type != was_type:
                stats["employment types"] += 1
            if body or job.posted_date != was_date or job.job_type != was_type:
                changed.append(job)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        list(ex.map(one, work))
    return changed, stats, per_sys, samples


def in_quiet_window(now=None) -> bool:
    now = now or datetime.now(timezone.utc)
    m = now.hour * 60 + now.minute
    lo, hi = QUIET_UTC
    return m >= lo or m < hi


def push(S, jobs) -> int:
    """Upsert through the nightly pipeline with every system marked PARTIAL,
    so no per-system sweep runs. Returns rows landed."""
    if not jobs:
        return 0
    rows = S.finalize_jobs(jobs)
    systems = {r.get("hospital_system") for r in rows if r.get("hospital_system")}
    S.PARTIAL_SYSTEMS.update(systems)
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    sent = S._upsert_hospital_jobs_to_supabase(rows, run_started_iso)
    if S.LAST_UPSERT_FAILED:
        sent += S.retry_failed_hospital_upsert()
    return sent


def _print_report(stats, per_sys, samples, S, show_samples=True):
    print("\n== totals ==")
    for k, v in sorted(stats.items(), key=lambda kv: -kv[1]):
        print(f"  {k}: {v:,}")
    print("\n== per system (fetched -> descriptions; non-200s) ==")
    for s, c in sorted(per_sys.items(), key=lambda kv: -kv[1]["fetched"]):
        bad = ", ".join(f"{k} {v}" for k, v in c.items() if k.startswith("HTTP") or k == "no cxs url")
        print(f"  {s}: {c['fetched']:,} -> {c['descriptions']:,}" + (f"; {bad}" if bad else ""))
    if show_samples:
        print("\n== sample bodies ==")
        for smp in samples:
            facts = S.posting_facts_for(smp["body"], None, smp["title"]) or {}
            keys = [k for k in ("shift", "schedule", "hours", "certs", "education", "experience", "benefits") if facts.get(k)]
            print(f"-- {smp['system']} | id {smp['id']} | {smp['title']} | {smp['chars']:,} chars | facts: {', '.join(keys) or 'none'}")
            print("   " + smp["url"])
            print("   " + smp["body"][:700].replace("\n", " / ") + (" ..." if smp["chars"] > 700 else ""))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Workday no-description backlog: local CXS detail pass.")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="fetch and report; write nothing")
    mode.add_argument("--write", action="store_true", help="upsert what lands (owner's go only)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N candidate rows (0 = all)")
    ap.add_argument("--chunk", type=int, default=2000, help="rows read, fetched and pushed per round")
    ap.add_argument("--system", default="", help="only this hospital_system")
    ap.add_argument("--workers", type=int, default=IN_FLIGHT)
    ap.add_argument("--every", type=int, default=1, help="only rows whose id is a multiple of K (a spread sample)")
    a = ap.parse_args(argv)
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    if a.dry_run:
        for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
            os.environ.pop(k, None)
    else:
        if in_quiet_window():
            print("REFUSED: 23:30-06:00 UTC is the nightly window; an upsert now would shield rows from its sweep.")
            return 2
        import hca_local_push
        hca_local_push._load_shared_env()
        if not os.environ.get("SUPABASE_KEY"):
            print("REFUSED: no SUPABASE_KEY for the write")
            return 2
    wp = _wp()
    import scraper as S
    S.proxies.proxies = []          # this machine's own IP is the point of running here

    limit = a.limit if a.limit > 0 else (60 if a.dry_run else 0)
    fetcher = Fetcher()
    total_stats = collections.Counter()
    total_sys = collections.defaultdict(collections.Counter)
    all_samples, last, read, landed, t0 = [], 0, 0, 0, time.time()
    while True:
        page = a.chunk if not limit else min(a.chunk, limit - read)
        if page <= 0:
            break
        rows = wp.sql(candidate_sql(last, page, a.system, a.every), timeout=300)
        if not rows:
            break
        last = rows[-1]["id"]
        read += len(rows)
        changed, stats, per_sys, samples = detail_pass(S, rows, fetcher, a.workers)
        total_stats.update(stats)
        for s, c in per_sys.items():
            total_sys[s].update(c)
        for smp in samples:
            if len(all_samples) < 3 and smp["system"] not in {x["system"] for x in all_samples}:
                all_samples.append(smp)
        if a.write:
            landed += push(S, changed)
        print(f"  read {read:,} (last id {last}); this round {stats['fetched']:,} fetched -> "
              f"{stats['descriptions']:,} descriptions; {'landed ' + format(landed, ',') if a.write else 'dry run, nothing written'}; "
              f"{time.time() - t0:.0f}s")
    _print_report(total_stats, total_sys, all_samples, S)
    if a.dry_run:
        print(f"\nDRY RUN: {read:,} candidate rows read, nothing written.")
    else:
        print(f"\nWRITE: {landed:,} rows landed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
