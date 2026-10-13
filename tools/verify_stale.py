"""
verify_stale.py - check stale active rows against the employer's own board.

WHY (2026-10-12): the owner found NewYork-Presbyterian Apply links landing on
dead Workday pages (job 34128047, req 00887615: closed on NYP's board, active
here, last scraped 10-01). The per-system sweep holds NYP every night because
the retire guard compares tonight's yield (316) with ALL active rows (488),
and the closed rows it keeps are what hold the ratio under 0.80. This script
asks each employer board directly whether each stale posting still exists, so
the closed ones can be retired by an owner-run SQL file instead of waiting on
the Layer 4 backstop.

VERDICTS (one per row, written to <out-dir>/<platform>.csv)
  CLOSED   a definite closed signal from the employer's board:
             Workday   the CXS detail answers 404/410 or 403 S22 ("permission
                       denied", what nyp.wd1 serves for a closed posting) AND a
                       CXS search for the
                       requisition id finds no posting with that id (both
                       requests made with Origin/Referer, like the scraper);
                       a tenant whose /jobs search does not answer gets
                       UNKNOWN for every row (canary), never CLOSED.
             Oracle    recruitingCEJobRequisitionDetails ById answers 200 with
                       items = [].
             others    HTTP 404/410 on the job page, a redirect that drops the
                       job id and lands on a search/listing page, or an
                       explicit "no longer available" text with no posting
                       markup (JSON-LD JobPosting / apply button) on the page.
  OPEN     the board still serves the posting (stale here = a scraper gap).
  UNKNOWN  anything else: 403, 5xx, timeouts, 429 after backoff, login or bot
           walls, pages that say neither. UNKNOWN is never retired.

Only the Workday checker was validated live (2026-10-12, NYP / Sutter /
Carilion / John Muir). The others encode each platform's documented closed
signal conservatively; spot-check a few CLOSED rows before trusting a new
platform's CSV.

POLITENESS
  * at most 2 requests in flight per host, a 0.5-1.0 s pause inside the host
    gate after every request
  * 429 / 503 with Retry-After: back off (Retry-After or 10 s, 30 s, 90 s),
    three tries, then UNKNOWN; three 429s in a row on one host pause that host
    for 5 minutes
  * --max-requests caps the whole run (default 3000); --stop-at (UTC HH:MM,
    default 23:15) stops issuing requests so nothing runs into the nightly
    (00:09 UTC); the script refuses to start between 23:15 and 05:30 UTC

RESUMABLE
  Each platform CSV is appended row by row. On start, ids already in the CSV
  with CLOSED or OPEN are skipped (UNKNOWN ones too unless --retry-unknown).

DATABASE
  Read-only. Rows come from --rows-file (JSON list or CSV from an earlier
  read) or from --from-db, which reads through the SELECT-guarded wp.sql
  helper with a 30 s statement_timeout and refuses to run 23:30-05:30 UTC.
  `sql` mode writes the owner-run retire / restore files from the CSVs; it
  never connects to the database.

USAGE
  python tools/verify_stale.py check --rows-file rows.json --system "NewYork-Presbyterian"
  python tools/verify_stale.py check --from-db --platform Workday --system "Sutter Health" --days 3
  python tools/verify_stale.py sql --out-sql 115_retire_verified_closed.sql \
         --out-restore 115r_retire_verified_closed.sql --stale-before "2026-10-12 21:00+00"
"""
import argparse
import collections
import csv
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlsplit, unquote

HERE = os.path.dirname(os.path.abspath(__file__))
WP_DIRS = [
    os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", "tools"),
    r"C:\Users\19403\OneDrive\claud_outputinput\tools",
    r"C:\Users\rober\OneDrive\claud_outputinput\tools",
]
DEFAULT_OUT = os.path.join(os.path.expanduser("~"), "OneDrive", "claud_outputinput", "stale_verify_2026-10-12")

PER_HOST = 2
PAUSE = (0.5, 1.0)
TIMEOUT = 25
BACKOFF = (10, 30, 90)
HOST_COOLDOWN = 300
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
CSV_FIELDS = ["id", "hospital_system", "ats_platform", "job_id", "url", "scraped_at", "misses",
              "verdict", "http_status", "evidence", "checked_at"]

CLOSED_PHRASES = re.compile(
    r"(no longer (?:available|accepting applications|open|active|posted)"
    r"|job (?:has )?expired|posting (?:has )?expired|position has been filled"
    r"|this (?:job|position|posting|requisition) (?:is|has been) (?:closed|filled|removed)"
    r"|job (?:you are|you're) looking for (?:is|was|could not|cannot|can't)"
    r"|job not found|job-not-found|jobnotfound|requisition (?:is )?(?:closed|not found))", re.I)
POSTING_MARKUP = re.compile(r'"@type"\s*:\s*"JobPosting"|apply now|apply for this job|>\s*apply\s*<', re.I)
SEARCH_PATH = re.compile(r"(search|results|/jobs/?$|/careers/?$|/job-search|/joblist|/openings/?$|^/?$)", re.I)
WALL_PATH = re.compile(r"(login|signin|sign-in|captcha|challenge|access-denied|blocked)", re.I)


def utcnow():
    return datetime.now(timezone.utc)


def in_quiet(now, start_hm, end_hm):
    m = now.hour * 60 + now.minute
    s = start_hm[0] * 60 + start_hm[1]
    e = end_hm[0] * 60 + end_hm[1]
    return m >= s or m < e if s > e else s <= m < e


def slug(platform):
    return re.sub(r"[^a-z0-9]+", "_", (platform or "unknown").lower()).strip("_") or "unknown"


# --------------------------------------------------------------------- HTTP
class Budget:
    def __init__(self, max_requests, stop_at):
        self.max = max_requests
        self.used = 0
        self.stop_at = stop_at
        self.lock = threading.Lock()

    def take(self):
        with self.lock:
            if self.used >= self.max:
                return False
            if self.stop_at and utcnow() >= self.stop_at:
                return False
            self.used += 1
            return True


class Http:
    """curl_cffi Chrome impersonation; per-host gate of PER_HOST with a pause
    inside it; 429/503 backoff; a global request budget and stop time."""

    class OutOfBudget(Exception):
        pass

    def __init__(self, budget, per_host=PER_HOST, pause=PAUSE):
        from curl_cffi import requests as cr
        self.cr = cr
        self.budget = budget
        self.per_host = per_host
        self.pause = pause
        self.gates, self.lock = {}, threading.Lock()
        self.cool_until = collections.defaultdict(float)
        self.streak_429 = collections.Counter()
        self.by_host = collections.Counter()

    def _gate(self, host):
        with self.lock:
            g = self.gates.get(host)
            if g is None:
                g = self.gates[host] = threading.Semaphore(self.per_host)
            return g

    def request(self, method, url, *, headers=None, json_body=None, verify=True, allow_redirects=True):
        """Returns (status or exception name, response or None)."""
        host = (urlsplit(url).hostname or "").lower()
        h = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
        h.update(headers or {})
        for attempt in range(len(BACKOFF) + 1):
            wait = self.cool_until[host] - time.time()
            if wait > 0:
                time.sleep(wait)
            with self._gate(host):
                if not self.budget.take():
                    raise Http.OutOfBudget()
                self.by_host[host] += 1
                try:
                    r = self.cr.request(method, url, headers=h, json=json_body, timeout=TIMEOUT,
                                        impersonate="chrome", verify=verify,
                                        allow_redirects=allow_redirects, max_redirects=8)
                    status = r.status_code
                except Exception as e:  # noqa: BLE001 - any transport failure is UNKNOWN
                    r, status = None, type(e).__name__
                time.sleep(random.uniform(*self.pause))
            if status in (429, 503) and r is not None:
                with self.lock:
                    self.streak_429[host] += 1
                    if self.streak_429[host] >= 3:
                        self.cool_until[host] = time.time() + HOST_COOLDOWN
                if attempt < len(BACKOFF):
                    ra = (r.headers.get("Retry-After") or "").strip()
                    delay = int(ra) if ra.isdigit() and int(ra) <= 600 else BACKOFF[attempt]
                    time.sleep(delay)
                    continue
            else:
                with self.lock:
                    self.streak_429[host] = 0
            return status, r
        return status, r


# ----------------------------------------------------------------- Workday
_WD_JOBS = re.compile(
    r"^https://([^./]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?([^/?#]+)(/job/[^?#]+)")
_WD_SITE = re.compile(
    r"^https://(wd\d+)\.myworkdaysite\.com/(?:[a-z]{2}-[A-Z]{2}/)?recruiting/([^/?#]+)/([^/?#]+)(/job/[^?#]+)")
_WD_REQ = re.compile(r"^[A-Za-z]{0,8}[-_]?\d[\w-]{2,}$")


def workday_parts(url):
    """(host, tenant, site, job path, cxs detail url, cxs search url, referer) or None."""
    u = re.sub(r"/apply(?:/[^/?#]*)?/?(?=$|\?)", "", url or "")
    m = _WD_JOBS.match(u)
    if m:
        tenant, wd, site, path = m.groups()
        host = f"{tenant}.{wd}.myworkdayjobs.com"
        base = f"https://{host}/wday/cxs/{tenant}/{site}"
        return host, tenant, site, path, base + path, base + "/jobs", f"https://{host}/{site}"
    m = _WD_SITE.match(u)
    if m:
        wd, tenant, site, path = m.groups()
        host = f"{wd}.myworkdaysite.com"
        base = f"https://{host}/wday/cxs/{tenant}/{site}"
        return host, tenant, site, path, base + path, base + "/jobs", f"https://{host}/recruiting/{tenant}/{site}"
    return None


class WorkdayChecker:
    def __init__(self, http):
        self.http = http
        self.canary = {}
        self.lock = threading.Lock()

    def _hdr(self, host, referer, post=False):
        h = {"Accept": "application/json", "Origin": f"https://{host}", "Referer": referer}
        if post:
            h["Content-Type"] = "application/json"
        return h

    def _search(self, host, search_url, referer, text, limit=20):
        body = {"limit": limit, "offset": 0, "searchText": text, "appliedFacets": {}}
        st, r = self.http.request("POST", search_url, headers=self._hdr(host, referer, True),
                                  json_body=body, verify=False)
        if st != 200 or r is None:
            return st, None
        try:
            return st, r.json()
        except Exception:  # noqa: BLE001
            return "bad-json", None

    def board_ok(self, host, search_url, referer):
        """Canary, once per site: the /jobs search answers 200 with a total > 0."""
        key = search_url
        with self.lock:
            if key in self.canary:
                return self.canary[key]
            self.canary[key] = None
        st, data = self._search(host, search_url, referer, "", limit=1)
        ok = (st == 200 and isinstance(data, dict) and (data.get("total") or 0) > 0,
              f"board total {data.get('total') if isinstance(data, dict) else st}")
        with self.lock:
            self.canary[key] = ok
        return ok

    def check(self, row):
        parts = workday_parts(row.get("url"))
        if not parts:
            return "UNKNOWN", "", "url is not a Workday job url"
        host, tenant, site, path, detail, search, referer = parts
        ok = None
        while ok is None:
            ok = self.board_ok(host, search, referer)
            if ok is None:
                time.sleep(0.5)
        if not ok[0]:
            return "UNKNOWN", "", "board canary failed (" + ok[1] + ")"
        st, r = self.http.request("GET", detail, headers=self._hdr(host, referer + path), verify=False)
        if st == 200 and r is not None:
            try:
                info = (r.json() or {}).get("jobPostingInfo") or {}
            except Exception:  # noqa: BLE001
                info = {}
            if info:
                if info.get("canApply") is False:
                    return "UNKNOWN", st, "detail 200 but canApply=false"
                return "OPEN", st, "detail 200 " + str(info.get("jobReqId") or info.get("id") or "")[:40]
            return "UNKNOWN", st, "detail 200 without jobPostingInfo"
        # A closed posting's detail answers 404/410, or (measured on nyp.wd1
        # 2026-10-12) 403 {"errorCode":"S22","message":"permission denied"}
        # while the same host serves open postings 200 under the same headers.
        # Either way the requisition search below must agree before CLOSED.
        s22 = False
        if st == 403 and r is not None:
            try:
                body = r.json() or {}
                s22 = body.get("errorCode") == "S22" or "permission denied" in str(body.get("message", "")).lower()
            except Exception:  # noqa: BLE001
                s22 = False
        if st not in (404, 410) and not s22:
            return "UNKNOWN", st, "detail answered " + str(st)
        if s22:
            st = "403-S22"
        jid = str(row.get("job_id") or "").strip()
        tail = unquote(path.rsplit("/", 1)[-1])
        if not _WD_REQ.match(jid):
            m = re.search(r"_([A-Za-z]{0,8}[-_]?\d[\w-]{2,})$", tail)
            jid = m.group(1) if m else ""
        if not jid:
            return "UNKNOWN", st, f"detail {st}; no requisition id to search"
        sst, data = self._search(host, search, referer, jid)
        if sst != 200 or not isinstance(data, dict):
            return "UNKNOWN", st, f"detail {st}; search answered {sst}"
        posts = data.get("jobPostings") or []
        for p in posts:
            bf = [str(b).strip() for b in (p.get("bulletFields") or [])]
            ext = p.get("externalPath") or ""
            if jid in bf or re.search("_" + re.escape(jid) + r"(?:-\d+)?$", ext) or ext.rsplit("/", 1)[-1] == tail:
                return "OPEN", st, f"detail {st} but search finds it at {ext[:120]} (url moved)"
        total = data.get("total") or 0
        if total > len(posts):
            return "UNKNOWN", st, f"detail {st}; search total {total}, id not on first page"
        return "CLOSED", st, f"detail {st}; search '{jid}' total {total}, no match"


# ------------------------------------------------------------------ Oracle
_ORA = re.compile(r"^https://([^/]+)/hcmUI/CandidateExperience/[a-z]{2}(?:-[A-Z]{2})?/sites/([^/]+)/(?:job|requisitions/preview)/(\d+)", re.I)


class OracleChecker:
    def __init__(self, http):
        self.http = http

    def check(self, row):
        m = _ORA.match(row.get("url") or "")
        if not m:
            return GenericChecker(self.http).check(row)
        host, site, rid = m.groups()
        api = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
               f"?expand=all&onlyData=true&finder=ById;Id=%22{rid}%22,siteNumber={site}")
        st, r = self.http.request("GET", api, headers={"Accept": "application/json"})
        if st != 200 or r is None:
            return "UNKNOWN", st, "requisition api answered " + str(st)
        try:
            items = (r.json() or {}).get("items")
        except Exception:  # noqa: BLE001
            return "UNKNOWN", st, "requisition api bad json"
        if items == []:
            return "CLOSED", st, f"requisition api ById {rid}: items []"
        if items and str(items[0].get("Id") or "") == rid:
            return "OPEN", st, f"requisition api ById {rid}: found"
        return "UNKNOWN", st, "requisition api: unexpected shape"


# ----------------------------------------------- page checkers (HTML boards)
def _id_tokens(row):
    toks = set()
    jid = str(row.get("job_id") or "").strip()
    if jid and len(jid) >= 4:
        toks.add(jid.lower())
    path = urlsplit(row.get("url") or "").path
    for t in re.findall(r"\d{4,}", path):
        toks.add(t)
    return toks


class GenericChecker:
    """HTML job page: 404/410, a redirect to a listing that drops the job id, or
    an explicit closed text on a page without posting markup = CLOSED."""

    PLATFORM_OPEN = {
        "phenom": re.compile(r'"jobDetail"\s*:\s*\{\s*"status"\s*:\s*200', re.I),
        "icims": re.compile(r"iCIMS_JobContent|iCIMS_JobHeaderGroup|iCIMS_Header", re.I),
        "talentbrew": re.compile(r"ajd_|job-description|data-job-id", re.I),
    }
    PLATFORM_CLOSED = {
        "phenom": re.compile(r'"jobDetail"\s*:\s*\{\s*"status"\s*:\s*(?:404|410)', re.I),
    }

    def __init__(self, http, platform=""):
        self.http = http
        self.platform = slug(platform)

    def check(self, row):
        url = row.get("url") or ""
        if not url.startswith("http"):
            return "UNKNOWN", "", "no url"
        st, r = self.http.request("GET", url, headers={"Accept": "text/html,application/xhtml+xml"})
        if st in (404, 410):
            return "CLOSED", st, f"job page {st}"
        if st != 200 or r is None:
            return "UNKNOWN", st, f"job page answered {st}"
        final = str(r.url or url)
        fpath = urlsplit(final).path
        toks = _id_tokens(row)
        moved = final.rstrip("/") != url.rstrip("/")
        id_in_final = any(t in final.lower() for t in toks) if toks else True
        if WALL_PATH.search(fpath):
            return "UNKNOWN", st, "redirected to a login/bot wall " + final[:120]
        if moved and toks and not id_in_final and SEARCH_PATH.search(fpath):
            return "CLOSED", st, "redirected to listing " + final[:120]
        text = r.text or ""
        pc = self.PLATFORM_CLOSED.get(self.platform)
        if pc and pc.search(text):
            return "CLOSED", st, "platform closed flag in page data"
        po = self.PLATFORM_OPEN.get(self.platform)
        has_markup = bool(POSTING_MARKUP.search(text)) or bool(po and po.search(text))
        id_in_page = any(t in text.lower() for t in toks) if toks else False
        cm = CLOSED_PHRASES.search(text)
        if cm and not has_markup:
            return "CLOSED", st, "closed text: " + cm.group(0)[:60]
        if has_markup and id_in_page and not cm:
            return "OPEN", st, "posting markup with job id"
        return "UNKNOWN", st, "page says neither" + (" (closed text beside posting markup)" if cm else "")


def checker_for(platform, http, cache={}):  # noqa: B006 - per-run cache by design
    key = slug(platform)
    if key not in cache:
        if key == "workday":
            cache[key] = WorkdayChecker(http)
        elif key in ("oracle_hcm", "oracle"):
            cache[key] = OracleChecker(http)
        else:
            cache[key] = GenericChecker(http, platform)
    return cache[key]


# ------------------------------------------------------------------- rows
def wp_module():
    for d in WP_DIRS:
        if os.path.exists(os.path.join(d, "wp.py")):
            sys.path.insert(0, d)
            import wp  # noqa: E402
            return wp
    raise SystemExit("wp.py (read-only SQL helper) not found")


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def rows_from_db(args):
    if in_quiet(utcnow(), (23, 30), (5, 30)):
        raise SystemExit("refusing a database read between 23:30 and 05:30 UTC (nightly run)")
    wp = wp_module()
    where = ["is_active", f"scraped_at < now() - interval '{int(args.days)} days'"]
    if args.platform:
        where.append("ats_platform = " + q(args.platform))
    if args.system:
        where.append("hospital_system in (" + ",".join(q(s) for s in args.system) + ")")
    sql = ("select set_config('statement_timeout', '30s', false); "
           "select id, hospital_system, ats_platform, job_id, url, scraped_at, "
           "consecutive_scrape_misses as misses from public.hospital_jobs where "
           + " and ".join(where) + f" order by scraped_at, id limit {int(args.limit or 5000)}")
    return [r for r in wp.sql(sql) if "id" in r]


def rows_from_file(path):
    if path.lower().endswith(".json"):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def done_ids(out_dir, platform, retry_unknown):
    p = os.path.join(out_dir, slug(platform) + ".csv")
    done = set()
    if os.path.exists(p):
        with open(p, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                if r.get("verdict") in ("CLOSED", "OPEN") or (r.get("verdict") == "UNKNOWN" and not retry_unknown):
                    done.add(str(r.get("id")))
    return done


def latest_verdicts(out_dir):
    """id -> last row written, across every platform CSV in out_dir."""
    out = {}
    for name in sorted(os.listdir(out_dir)):
        if not name.endswith(".csv"):
            continue
        with open(os.path.join(out_dir, name), encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                out[str(r["id"])] = r
    return out


# ------------------------------------------------------------------ check
def run_check(args):
    now = utcnow()
    if in_quiet(now, (23, 15), (5, 30)):
        raise SystemExit("refusing to start between 23:15 and 05:30 UTC (nightly window)")
    hh, mm = (int(x) for x in args.stop_at.split(":"))
    stop_at = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if stop_at <= now:
        raise SystemExit(f"--stop-at {args.stop_at} UTC has already passed")
    os.makedirs(args.out_dir, exist_ok=True)

    rows = rows_from_file(args.rows_file) if args.rows_file else rows_from_db(args)
    if args.system:
        rows = [r for r in rows if r.get("hospital_system") in set(args.system)]
    if args.platform:
        rows = [r for r in rows if slug(r.get("ats_platform")) == slug(args.platform)]
    by_plat = collections.defaultdict(list)
    for r in rows:
        by_plat[r.get("ats_platform") or "unknown"].append(r)
    todo = []
    for plat, rs in by_plat.items():
        done = done_ids(args.out_dir, plat, args.retry_unknown)
        todo += [r for r in rs if str(r.get("id")) not in done]
    if args.limit:
        todo = todo[: args.limit]
    # round-robin by host so the per-host gates all stay busy
    by_host = collections.OrderedDict()
    for r in todo:
        by_host.setdefault((urlsplit(r.get("url") or "").hostname or ""), []).append(r)
    order, queues = [], list(by_host.values())
    while queues:
        nxt = []
        for qq in queues:
            order.append(qq.pop(0))
            if qq:
                nxt.append(qq)
        queues = nxt
    print(f"{len(rows)} rows read; {len(order)} to check on {len(by_host)} hosts; "
          f"budget {args.max_requests} requests; stop at {stop_at:%H:%M} UTC", flush=True)

    http = Http(Budget(args.max_requests, stop_at))
    writers, files, wlock = {}, {}, threading.Lock()
    stats = collections.Counter()
    per_sys = collections.defaultdict(collections.Counter)
    stopped = threading.Event()

    def write(row, verdict, status, evidence):
        plat = row.get("ats_platform") or "unknown"
        with wlock:
            if plat not in writers:
                p = os.path.join(args.out_dir, slug(plat) + ".csv")
                new = not os.path.exists(p)
                files[plat] = open(p, "a", encoding="utf-8", newline="")
                writers[plat] = csv.DictWriter(files[plat], fieldnames=CSV_FIELDS)
                if new:
                    writers[plat].writeheader()
            writers[plat].writerow({
                "id": row.get("id"), "hospital_system": row.get("hospital_system"), "ats_platform": plat,
                "job_id": row.get("job_id"), "url": row.get("url"), "scraped_at": row.get("scraped_at"),
                "misses": row.get("misses", row.get("consecutive_scrape_misses")),
                "verdict": verdict, "http_status": status, "evidence": evidence,
                "checked_at": utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            })
            files[plat].flush()
            stats[verdict] += 1
            per_sys[row.get("hospital_system")][verdict] += 1
            n = sum(stats.values())
            if n % 50 == 0:
                print(f"  {n} checked {dict(stats)} requests {http.budget.used}", flush=True)

    def one(row):
        if stopped.is_set():
            return
        try:
            verdict, status, ev = checker_for(row.get("ats_platform"), http).check(row)
        except Http.OutOfBudget:
            stopped.set()
            return
        except Exception as e:  # noqa: BLE001
            verdict, status, ev = "UNKNOWN", "", "checker error " + type(e).__name__
        write(row, verdict, status, ev)

    workers = max(2, min(args.workers, 2 * len(by_host)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, order))
    for f in files.values():
        f.close()
    print(f"done: {dict(stats)}; requests {http.budget.used}"
          + ("; STOPPED at the request budget or stop time" if stopped.is_set() else ""), flush=True)
    for s, c in sorted(per_sys.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"  {s}: {dict(c)}")
    print("requests by host: " + ", ".join(f"{h} {n}" for h, n in http.by_host.most_common()))


# -------------------------------------------------------------------- sql
def run_sql(args):
    verdicts = latest_verdicts(args.out_dir)
    closed = sorted(int(i) for i, r in verdicts.items() if r.get("verdict") == "CLOSED")
    if args.system:
        keep = set(args.system)
        closed = sorted(int(i) for i, r in verdicts.items()
                        if r.get("verdict") == "CLOSED" and r.get("hospital_system") in keep)
    by_sys = collections.Counter(verdicts[str(i)]["hospital_system"] for i in closed)
    batches = [closed[i:i + args.batch] for i in range(0, len(closed), args.batch)]
    guard = f"is_active and scraped_at < {q(args.stale_before)}"
    tbl = "public.retired_closed_backup_20261012"
    lines = [
        "-- 115_retire_verified_closed.sql  (generated by hospital-scraper tools/verify_stale.py sql)",
        "-- DATA project (tlzxajgonuevbqaelhvf) only. OWNER-RUN. Never between 23:30 and 05:30 UTC.",
        f"-- Retires {len(closed)} active rows whose posting the employer's own board confirmed CLOSED",
        f"-- (CSVs in {args.out_dir}). UNKNOWN and OPEN rows are not touched.",
        "-- Per system: " + "; ".join(f"{s} {n}" for s, n in by_sys.most_common()),
        f"-- Guard on every statement: {guard}  (a row re-stamped by a later crawl is left alone).",
        "-- Undo: 115r_retire_verified_closed.sql restores is_active from the backup table.",
        "",
        "-- 1. Save (id, is_active, scraped_at) of the rows to retire.",
        f"set lock_timeout = '3s'; create table if not exists {tbl} (id bigint primary key, is_active boolean not null, "
        "scraped_at timestamptz, saved_at timestamptz not null default now());",
    ]
    for b in batches:
        lines.append(f"set lock_timeout = '3s'; insert into {tbl} (id, is_active, scraped_at) "
                     f"select id, is_active, scraped_at from public.hospital_jobs where {guard} "
                     f"and id in ({','.join(map(str, b))}) on conflict (id) do nothing;")
    lines += ["", f"-- 2. Retire them ({len(batches)} statement(s) of at most {args.batch} ids)."]
    for b in batches:
        lines.append(f"set lock_timeout = '3s'; update public.hospital_jobs set is_active = false "
                     f"where {guard} and id in ({','.join(map(str, b))});")
    lines += ["", "-- 3. Check: expect 0 rows still active among the backed-up ids.",
              f"select count(*) as still_active from public.hospital_jobs h join {tbl} b using (id) where h.is_active;", ""]
    with open(args.out_sql, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    rlines = [
        "-- 115r_retire_verified_closed.sql  (undo of 115_retire_verified_closed.sql)",
        "-- DATA project (tlzxajgonuevbqaelhvf) only. OWNER-RUN. Never between 23:30 and 05:30 UTC.",
        f"-- Puts is_active back from {tbl} for rows 115 retired that no crawl has re-stamped since",
        f"-- (scraped_at still before {args.stale_before}). The backup table is kept; drop it by hand later.",
        "",
        f"set lock_timeout = '3s'; update public.hospital_jobs h set is_active = b.is_active from {tbl} b "
        "where h.id = b.id and h.is_active = false and b.is_active = true "
        f"and h.scraped_at < {q(args.stale_before)};",
        "",
        f"select count(*) as restored_active from public.hospital_jobs h join {tbl} b using (id) where h.is_active;",
        "",
    ]
    with open(args.out_restore, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(rlines))
    print(f"wrote {args.out_sql}: {len(closed)} CLOSED ids in {len(batches)} batches; {dict(by_sys)}")
    print(f"wrote {args.out_restore}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="verify stale rows against the employer boards")
    c.add_argument("--rows-file", help="JSON list or CSV of rows (id, hospital_system, ats_platform, job_id, url, ...)")
    c.add_argument("--from-db", action="store_true", help="read rows through wp.sql (read-only)")
    c.add_argument("--system", action="append", help="hospital_system (repeatable)")
    c.add_argument("--platform", help="ats_platform, ex: Workday")
    c.add_argument("--days", type=int, default=3, help="--from-db: stale when scraped_at older than this")
    c.add_argument("--limit", type=int, default=0)
    c.add_argument("--out-dir", default=DEFAULT_OUT)
    c.add_argument("--max-requests", type=int, default=3000)
    c.add_argument("--stop-at", default="23:15", help="UTC HH:MM; no request starts after it")
    c.add_argument("--workers", type=int, default=8)
    c.add_argument("--retry-unknown", action="store_true")
    s = sub.add_parser("sql", help="write the owner-run retire + restore SQL from the CSVs")
    s.add_argument("--out-dir", default=DEFAULT_OUT)
    s.add_argument("--out-sql", required=True)
    s.add_argument("--out-restore", required=True)
    s.add_argument("--stale-before", required=True, help="timestamptz; only rows last scraped before it are touched")
    s.add_argument("--system", action="append")
    s.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args()
    if args.cmd == "check":
        if not args.rows_file and not args.from_db:
            ap.error("check needs --rows-file or --from-db")
        run_check(args)
    else:
        if args.batch > 1000:
            ap.error("--batch is at most 1000")
        run_sql(args)


if __name__ == "__main__":
    main()
