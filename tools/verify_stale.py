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
                       items = [], asked by the whole id ("2026-464305"; the
                       10-14 fix: the id used to stop at the hyphen).
             HCA       (careers.hcahealthcare.com, Talemetry; curl_cffi
                       impersonate="firefox", no proxy) the job page answers
                       301 to /jobs?job_not_found=true, or 200 with "This job
                       posting is no longer active." and no JSON-LD
                       JobPosting, on two separate fetches (the second must
                       read CLOSED too, else UNKNOWN). Only those two signals
                       were measured live; a 404/410 never was, so it is
                       UNKNOWN here. OPEN = 200 with a JobPosting.
             PeopleSoft (NYC Health + Hospitals, careers.nychhc.org) the
                       posting url (Page=HRS_APP_JBPST_FL&JobOpeningId=N) is
                       sent to the job list (Page=HRS_APP_SCHJOB_FL, "N jobs
                       found") on two separate fetches. OPEN = the posting
                       page echoes Job ID N with its HRS_SCH_PSTDSC body.
             iCIMS     the job page (in_iframe=1) answers 404/410 AND carries
                       "does not exist or is no longer open". OPEN = 200 with
                       a JSON-LD JobPosting and no job cards.
             Phenom    the row's url is a Phenom job page on the site's own
                       host ({base}/.../job/{jobId}), the tenant's jobDetail
                       widget (POST {base}/widgets, by that jobId) answers
                       hits 0 AND that job page answers 404/410. A row whose
                       id comes from job_id instead (Workday, Infor or other
                       apply links) is never CLOSED by the Phenom board, only
                       OPEN or UNKNOWN. OPEN = the widget returns the job. A
                       row whose url is a Workday job page is asked of
                       Workday first (rules above); only when Workday says
                       UNKNOWN is the Phenom board asked. A row whose url is
                       an Oracle requisition (Ascension) is CLOSED when
                       Oracle's ById answers items [] AND, past both
                       canaries, the widget answers hits 0 for its job_id.
             SmartRecruiters (jobs.smartrecruiters.com/{org}/{id}; measured
                       2026-10-14, Northwestern) the public posting API
                       (api.smartrecruiters.com/v1/companies/{org}/postings/
                       {id}) answers 200 with "active": false. OPEN = active
                       true and its applyUrl names the same id.
             Jibe      (hosts of scraper.py JIBE_SITES; UHS rows are stored as
                       iCIMS) the job page answers 500 "Oops! An error has
                       occurred." on two separate fetches AND the front's
                       /api/jobs?keywords={req} finds no job with that req_id.
                       OPEN = 200 with a JobPosting naming the req_id.
             others    HTTP 404/410 on the job page, a redirect that drops the
                       job id and lands on a search/listing page, or an
                       explicit "no longer available" text with no posting
                       markup (JSON-LD JobPosting / apply button) on the page.
  OPEN     the board still serves the posting (stale here = a scraper gap).
  MOVED    (SmartRecruiters) the old id still answers, but its apply link
           names a new id: the board re-published the posting. Not closed;
           `sql` never retires it. The row is a re-keyed duplicate when the
           new id is an active row too (the duplicate cleanup's call).
  UNKNOWN  anything else: 403, 5xx, timeouts, 429 after backoff, login or bot
           walls, pages that say neither. UNKNOWN is never retired.

CANARY (one per site, before any row of that site is judged)
  Workday: the board's /jobs search answers with a total > 0. HCA, PeopleSoft,
  iCIMS, Phenom, SmartRecruiters and Jibe: a posting the board lists right
  now (first card of HCA's Colorado search, first row of the PeopleSoft job
  list, first link of the iCIMS portal search, first jobId on the Phenom
  search-results page, first posting of the SmartRecruiters company list,
  first job of the Jibe front's /api/jobs) must
  read OPEN through the same requests the rows go through (Phenom: and its
  job page must answer 200). If it does not, every row of that site is
  UNKNOWN ("board canary failed (...)"), never CLOSED.
  HCA re-runs its canary before every 500th row; when one fails mid-run,
  every remaining HCA row is UNKNOWN without a request ("... site aborted"),
  and a CLOSED still in flight at that moment is written UNKNOWN.
  Phenom adds a ROW canary per site and system: the board canary's jobId
  comes from the search page, not from the ids our rows store, so it cannot
  catch a system whose stored ids are not the widget's jobId (every open row
  would read hits 0). The most recently scraped row of that system in the
  input export (rows whose url is a Phenom job page on the site first, the
  only rows that can be CLOSED) must read OPEN through the same widget call,
  else every row of that system is UNKNOWN ("row canary failed (...)"). A
  legitimately closed newest row therefore holds the whole system back;
  that is the intended trade (UNKNOWN is never retired).

VALIDATION
  Workday live 2026-10-12 (NYP / Sutter / Carilion / John Muir). HCA,
  PeopleSoft (NYC H+H), iCIMS (Prime, Legacy, Select Medical, Emory) and
  Phenom (Jackson Health, UVA Health) live 2026-10-13 01:00-01:20 UTC, a
  handful of open and closed postings each (signals and ids in the comments
  above each checker; recorded pages in tests/fixtures/verify_stale). Oracle
  and the generic page checker encode each platform's documented closed
  signal conservatively; spot-check a few CLOSED rows before trusting a new
  platform's CSV.

POLITENESS
  * at most PER_HOST = 2 requests in flight per host (--per-host, 1..2), and
    a pause of P..2P s inside the host gate after every request (--pause P,
    default 0.5, so 0.5-1.0 s). --workers is the thread count across ALL
    hosts: any value of 2 or more still sends 2 at a time to one host, so
    lowering --workers does not slow HCA (the run's biggest host).
  * 429 / 503 with Retry-After: back off (Retry-After or 10 s, 30 s, 90 s),
    three tries, then UNKNOWN; three 429s in a row on one host pause that host
    for 5 minutes
  * IF A BOARD ANSWERS 429 (the progress lines show UNKNOWN climbing, the
    CSV evidence says "answered 429"): stop the run (Ctrl+C, or close the
    window; the CSV keeps every row already written) and start it again with
      --per-host 1 --pause 2 --retry-unknown
    which sends one request at a time to each host with a 2-4 s rest after
    each, and re-asks the rows the 429s left UNKNOWN. A higher --pause (up to
    30) slows it further. Lowering --workers is not the knob.
  * --max-requests caps the whole run (default 3000); --stop-at (UTC HH:MM,
    default 23:15) stops issuing requests so nothing runs into the nightly
    (00:09 UTC); the script refuses to start between 23:15 and 05:30 UTC

SHIPPING
  This file ships in the same batched hospital-scraper push as A1 and A2,
  never in a push of its own: every push to the scraper repo redeploys
  Railway, and a redeploy kills an in-flight nightly run (00:09-05:15 UTC).
  The checks themselves run from a local checkout and need no deploy.

RESUMABLE
  Each platform CSV is appended row by row. On start, ids already in the CSV
  with CLOSED or OPEN are skipped (UNKNOWN ones too unless --retry-unknown).

DATABASE
  Read-only. Rows come from --rows-file (JSON list or CSV from an earlier
  read) or from --from-db, which reads through the SELECT-guarded wp.sql
  helper with a 30 s statement_timeout and refuses to run 23:30-05:30 UTC.
  `sql` mode writes the owner-run retire / restore files from the CSVs; it
  never connects to the database.

HCA / NYC H+H / PHENOM / iCIMS RUN (2026-10-13; lead, after 05:30 UTC)
  1. Export the candidates, read-only, through wp.sql (one statement; save
     the JSON it prints as rows_2026-10-13.json):
       select id, hospital_system, ats_platform, job_id, url, scraped_at, consecutive_scrape_misses as misses from public.hospital_jobs where is_active and scraped_at < now() - interval '4 days' and (hospital_system in ('HCA Healthcare', 'NYC Health + Hospitals') or ats_platform in ('Phenom', 'iCIMS')) order by hospital_system, scraped_at, id;
     Note the export time (UTC): it is the --stale-before of step 3.
  2. Check (about 1 request a row, 2 for a CLOSED HCA, PeopleSoft or Phenom
     row, plus a few per canary: HCA re-runs its canary every 500 rows; HCA
     alone is several thousand rows, so raise the budget):
       python tools/verify_stale.py check --rows-file rows_2026-10-13.json --out-dir <shared>/stale_verify_2026-10-13 --max-requests 20000
  3. Write the retire + rollback files in db_write format:
       python tools/verify_stale.py sql --db-write --out-dir <shared>/stale_verify_2026-10-13 --backup-table retired_closed_backup_20261013 --stale-before "<export time>+00" --out-sql NNN_retire_verified_closed_more.sql --out-restore NNNr_retire_verified_closed_more.sql
     which writes, for each batch of at most 1000 CLOSED ids (<T> =
     public.retired_closed_backup_20261013, <S> = the export time):
       NNN:
         set lock_timeout = '3s'; create table if not exists <T> (id bigint primary key, is_active boolean not null, scraped_at timestamptz, saved_at timestamptz not null default now());
         set lock_timeout = '3s'; insert into <T> (id, is_active, scraped_at) select id, is_active, scraped_at from public.hospital_jobs where is_active and scraped_at < '<S>' and id in (<ids>) on conflict (id) do nothing;
         set lock_timeout = '3s'; update public.hospital_jobs set is_active = false where is_active and scraped_at < '<S>' and id in (<ids>);
         select count(*) as still_active from public.hospital_jobs h join <T> b using (id) where h.is_active;
       NNNr:
         set lock_timeout = '3s'; update public.hospital_jobs h set is_active = b.is_active from <T> b where h.id = b.id and h.is_active = false and b.is_active = true and h.scraped_at < '<S>';
         select count(*) as restored_active from public.hospital_jobs h join <T> b using (id) where h.is_active;
     (the same shape as 115 / 115r, with a backup table of its own so NNNr
     cannot restore the rows 115 retired). A row any crawl re-stamps after
     the export (the 04:00 UTC HCA task included) fails the scraped_at guard
     and is left alone.

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

    def request(self, method, url, *, headers=None, json_body=None, verify=True, allow_redirects=True,
                impersonate="chrome", native_ua=False):
        """Returns (status or exception name, response or None).

        native_ua=True leaves the User-Agent to curl_cffi's impersonation
        profile. The fixed Chrome 124 UA below rides on a newer Chrome TLS
        handshake, a mismatch some bot walls answer: iCIMS portals return
        405 to it and 200 without it (2026-10-13), and a Chrome UA on
        HCA's Firefox handshake would be worse. The checkers added on
        2026-10-13 all pass native_ua=True; Workday, Oracle and the generic
        page checker keep the UA they were validated with."""
        host = (urlsplit(url).hostname or "").lower()
        h = {"Accept-Language": "en-US,en;q=0.9"}
        if not native_ua and impersonate == "chrome":
            h["User-Agent"] = UA
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
                                        impersonate=impersonate, verify=verify,
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
# 2026-10-14: the requisition id keeps its hyphens. Ascension's newer
# requisitions are numbered "2026-464305" (scraper fix b25710a, 10-09); the
# old "(\d+)" stopped at the hyphen, asked ById "2026", got items [] and
# called four Ascension rows CLOSED on their year alone. The id now runs to
# the end of its path segment.
_ORA = re.compile(r"^https://([^/]+)/hcmUI/CandidateExperience/[a-z]{2}(?:-[A-Z]{2})?/sites/([^/]+)/"
                  r"(?:job|requisitions/preview)/([0-9A-Za-z][0-9A-Za-z-]*)(?=$|[/?#])", re.I)


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
    ck = (key, id(http))
    if ck not in cache:
        if key == "workday":
            cache[ck] = WorkdayChecker(http)
        elif key in ("oracle_hcm", "oracle"):
            cache[ck] = OracleChecker(http)
        elif key == "phenom":
            cache[ck] = PhenomChecker(http, workday=checker_for("Workday", http))
        elif key == "peoplesoft":
            cache[ck] = PeopleSoftChecker(http)
        elif key == "icims":
            cache[ck] = IcimsChecker(http)
        elif key == "hca":
            cache[ck] = HcaChecker(http)
        elif key == "smartrecruiters":
            cache[ck] = SmartRecruitersChecker(http)
        elif key == "jibe":
            cache[ck] = JibeChecker(http)
        else:
            cache[ck] = GenericChecker(http, platform)
    return cache[ck]


def checker_for_row(row, http):
    """The checker for one row: the url decides where it names a board this
    script reads (HCA's Talemetry site, a PeopleSoft posting, a
    SmartRecruiters posting, a Jibe front, an iCIMS portal, an Oracle
    requisition), else the row's ats_platform. Phenom rows stay with the
    Phenom checker, which sends Workday- and Oracle-linked rows on to those
    checkers first. Jibe fronts come before iCIMS: the scraper stores their
    rows as ats_platform iCIMS (UHS)."""
    url = row.get("url") or ""
    plat = slug(row.get("ats_platform"))
    if _host(url) == HCA_HOST:
        return checker_for("hca", http)
    if plat == "phenom":
        return checker_for("Phenom", http)
    if _PS_POSTING_URL.match(url):
        return checker_for("PeopleSoft", http)
    if _SR_URL.match(url):
        return checker_for("SmartRecruiters", http)
    if _host(url) in jibe_hosts():
        return checker_for("Jibe", http)
    if plat == "icims" or _ICIMS_URL.match(url):
        return checker_for("iCIMS", http)
    if _ORA.match(url):
        return checker_for("Oracle", http)
    return checker_for(row.get("ats_platform"), http)


# ------------------------------------------------------- per-site canary
class Canary:
    """One check per site key, shared by every worker thread: the first
    caller runs probe() (-> (ok, note)), the rest wait for its answer. The
    rule (2026-10-13): a posting the board itself lists right now must read
    OPEN through the very path the rows go through, or every row on that site
    is UNKNOWN. A probe cut short by the request budget is not cached."""

    def __init__(self):
        self.lock = threading.Lock()
        self.events, self.results = {}, {}

    def get(self, key, probe):
        with self.lock:
            ev = self.events.get(key)
            owner = ev is None
            if owner:
                ev = self.events[key] = threading.Event()
        if owner:
            try:
                res = probe()
            except Http.OutOfBudget:
                res = None
            except Exception as e:  # noqa: BLE001
                res = (False, "canary error " + type(e).__name__)
            with self.lock:
                self.results[key] = res
                if res is None:
                    del self.events[key]
            ev.set()
        else:
            ev.wait()
        res = self.results.get(key)
        if res is None:
            raise Http.OutOfBudget()
        return res


class NativeUA:
    """An Http whose requests leave the User-Agent to the TLS profile (see
    Http.request); every checker added on 2026-10-13 talks through one."""

    def __init__(self, http):
        self.http = http

    def request(self, method, url, **kw):
        kw.setdefault("native_ua", True)
        return self.http.request(method, url, **kw)


def _host(url):
    try:
        return (urlsplit(url or "").hostname or "").lower()
    except ValueError:
        return ""


def _has_jobposting(text):
    return bool(re.search(r'"@type"\s*:\s*"JobPosting"', text or ""))


_REDIRECTS = (301, 302, 303, 307, 308)


# --------------------------------------------- HCA Healthcare (Talemetry)
# Measured live 2026-10-13 (curl_cffi impersonate="firefox", no proxy, from a
# residential line): an open posting answers 200 with a JSON-LD JobPosting
# and its "Job ID: NNNNNNN" line. A closed one answers either 200 with
# <p class="neu-text--h3 ...">This job posting is no longer active.</p> and
# NO JobPosting (17200000, Charleston Pharmacy Tech), or 301 to
# /jobs?job_not_found=true once Talemetry has dropped it (16263000, 16800000,
# 17500000, 17700000, 99999999). Cloudflare blocks come back as 403 or a
# "Just a moment" interstitial: UNKNOWN. No 404/410 was seen, so neither is
# a closed signal here (UNKNOWN), and CLOSED needs a second fetch that agrees.
HCA_HOST = "careers.hcahealthcare.com"
HCA_IMPERSONATE = "firefox"
HCA_CANARY_LIST = f"https://{HCA_HOST}/search/jobs/in/co-colorado?q=&page=1&per_page=20"
_HCA_JOB_URL = re.compile(r"^https://careers\.hcahealthcare\.com/jobs/(\d+)(?:-[^/?#]*)?/?(?:[?#].*)?$")
_HCA_CARD_LINK = re.compile(r'href="(https://careers\.hcahealthcare\.com/jobs/(\d+)-[^"]+)"')
_HCA_INACTIVE = re.compile(r"This job posting is no longer active", re.I)
_HCA_CHALLENGE = re.compile(r"Just a moment|cf-chl", re.I)


class HcaChecker:
    """CLOSED only on the two signals measured live (the 301 to
    job_not_found, the 'no longer active' page without a JobPosting), and
    only when a second fetch of the same url reads CLOSED too. A 404/410 was
    never observed on this board, so it is UNKNOWN. The board canary runs
    before the first row and again before every CANARY_EVERY-th row; once one
    fails, every remaining HCA row is UNKNOWN without a request, and a CLOSED
    still in flight when it failed is written UNKNOWN."""

    CANARY_EVERY = 500

    def __init__(self, http, canary=None):
        self.http = NativeUA(http)
        self.canary = canary or Canary()
        self.lock = threading.Lock()
        self.rows_seen = 0
        self.dead = ""          # set once a canary fails: why

    def _get(self, url):
        return self.http.request("GET", url, headers={"Accept": "text/html,application/xhtml+xml"},
                                 allow_redirects=False, impersonate=HCA_IMPERSONATE)

    def read(self, url, job_id):
        """One fetch of the job page (one same-id slug redirect followed)."""
        st, r = self._get(url)
        if st in _REDIRECTS and r is not None:
            loc = r.headers.get("location") or ""
            if "job_not_found" in loc:
                return "CLOSED", st, "redirect to /jobs?job_not_found=true"
            # a changed slug moves the posting: follow one hop on the same id
            m = _HCA_JOB_URL.match(loc)
            if not (m and m.group(1) == job_id):
                return "UNKNOWN", st, "redirect to " + loc[:120]
            st, r = self._get(loc)
            if st in _REDIRECTS and r is not None:
                loc = r.headers.get("location") or ""
                if "job_not_found" in loc:
                    return "CLOSED", st, "redirect to /jobs?job_not_found=true"
                return "UNKNOWN", st, "second redirect to " + loc[:120]
        if st in (404, 410):
            return "UNKNOWN", st, f"job page {st} (never observed on this board; not a closed signal)"
        if st != 200 or r is None:
            return "UNKNOWN", st, f"job page answered {st}"
        text = r.text or ""
        posting = _has_jobposting(text)
        if _HCA_CHALLENGE.search(text) and not posting:
            return "UNKNOWN", st, "Cloudflare interstitial"
        inactive = bool(_HCA_INACTIVE.search(text))
        if inactive and not posting:
            return "CLOSED", st, "page says 'This job posting is no longer active.'"
        if posting and not inactive and f"/jobs/{job_id}" in text:
            return "OPEN", st, "JobPosting on the page"
        return "UNKNOWN", st, "page says neither" + (" (inactive text beside a JobPosting)" if inactive else "")

    def verdict(self, url, job_id):
        v, st, ev = self.read(url, job_id)
        if v != "CLOSED":
            return v, st, ev
        v2, st2, ev2 = self.read(url, job_id)
        if v2 == "CLOSED":
            return "CLOSED", st2, f"{ev}; second fetch agrees ({ev2})"
        return "UNKNOWN", st2, f"first fetch: {ev}; second fetch read {v2} ({ev2})"

    def _probe(self):
        st, r = self.http.request("GET", HCA_CANARY_LIST, headers={"Accept": "text/html"},
                                  impersonate=HCA_IMPERSONATE)
        if st != 200 or r is None:
            return False, f"listing answered {st}"
        m = _HCA_CARD_LINK.search(r.text or "")
        if not m:
            return False, "listing has no job cards"
        v, _, ev = self.verdict(m.group(1), m.group(2))
        return v == "OPEN", f"listed job {m.group(2)} read {v} ({ev})"

    def check(self, row):
        url = (row.get("url") or "").strip()
        m = _HCA_JOB_URL.match(url)
        if not m:
            return "UNKNOWN", "", "url is not an HCA job url"
        with self.lock:
            if self.dead:
                return "UNKNOWN", "", self.dead
            n = self.rows_seen
            self.rows_seen += 1
        block = n // self.CANARY_EVERY
        ok, note = self.canary.get((HCA_HOST, block), self._probe)
        if not ok:
            why = "board canary failed (" + note + ")"
            if block:
                why = f"board canary failed at row {block * self.CANARY_EVERY} ({note}); site aborted"
            with self.lock:
                self.dead = self.dead or why
            return "UNKNOWN", "", why
        v, st, ev = self.verdict(url, m.group(1))
        with self.lock:
            dead = self.dead
        if dead and v == "CLOSED":
            return "UNKNOWN", st, ev + "; not trusted: " + dead
        return v, st, ev


# ------------------------------------------- PeopleSoft (NYC H+H, Queen's)
# Measured live 2026-10-13 on careers.nychhc.org: every posting url first
# answers 302 to itself plus "&" (the site's cookie handshake). An open
# posting then serves Page=HRS_APP_JBPST_FL with
# <span ... id='HRS_SCH_WRK2_HRS_JOB_OPENING_ID' >140671</span> and the
# HRS_SCH_PSTDSC body; a closed or unknown opening (130000, 138000, 9999999)
# is sent on by a second 302 to the job list, Page=HRS_APP_SCHJOB_FL
# ("1819 jobs found"). CLOSED needs that landing on two separate fetches, so
# a one-off session hiccup cannot retire an open posting.
_PS_POSTING_URL = re.compile(
    r"^(https://[^?#]+\.GBL)\?(?=.*\bPage=HRS_APP_JBPST_FL\b)(?=.*\bJobOpeningId=(\d+))", re.I)
_PS_LIST_PAGE = "?Page=HRS_APP_SCHJOB_FL&Action=U"
_PS_LIST_ID = re.compile(r"HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID\$\d+' >(\d+)<")
_PS_JOBS_FOUND = re.compile(r"\d[\d,]*\s*(?:<[^>]+>\s*)*jobs found", re.I)


def ps_posting_url(base, jid):
    return f"{base}?Page=HRS_APP_JBPST_FL&Action=U&FOCUS=Applicant&SiteId=1&JobOpeningId={jid}&PostingSeq=1"


class PeopleSoftChecker:
    def __init__(self, http, canary=None):
        self.http = NativeUA(http)
        self.canary = canary or Canary()

    def _read(self, url, jid):
        """('open' | 'list' | 'other', status, final url)."""
        st, r = self.http.request("GET", url, headers={"Accept": "text/html,application/xhtml+xml"})
        if st != 200 or r is None:
            return "other", st, ""
        final, text = str(r.url or url), r.text or ""
        if re.search(r"id='HRS_SCH_WRK2_HRS_JOB_OPENING_ID'\s*>\s*" + re.escape(jid) + r"\s*<", text) \
                and "HRS_SCH_PSTDSC" in text:
            return "open", st, final
        if re.search(r"[?&]Page=HRS_APP_SCHJOB_FL\b", final, re.I) and _PS_JOBS_FOUND.search(text) \
                and "HRS_SCH_PSTDSC" not in text:
            return "list", st, final
        return "other", st, final

    def verdict(self, url, jid):
        kind, st, final = self._read(url, jid)
        if kind == "open":
            return "OPEN", st, f"posting page shows Job ID {jid}"
        if kind != "list":
            return "UNKNOWN", st, f"posting page answered {st}" + (" at ..." + final[-60:] if final else "")
        kind2, st2, _ = self._read(url, jid)
        if kind2 == "list":
            return "CLOSED", st2, "posting url sent to the job list on two fetches (Page=HRS_APP_SCHJOB_FL)"
        return "UNKNOWN", st2, f"first fetch landed on the job list, the second read {kind2}"

    def _probe(self, base):
        st, r = self.http.request("GET", base + _PS_LIST_PAGE, headers={"Accept": "text/html"})
        if st != 200 or r is None:
            return False, f"job list answered {st}"
        m = _PS_LIST_ID.search(r.text or "")
        if not m:
            return False, "job list has no rows"
        v, _, ev = self.verdict(ps_posting_url(base, m.group(1)), m.group(1))
        return v == "OPEN", f"listed opening {m.group(1)} read {v} ({ev})"

    def check(self, row):
        url = (row.get("url") or "").strip()
        m = _PS_POSTING_URL.match(url)
        if not m:
            return "UNKNOWN", "", "url is not a PeopleSoft posting url"
        base, jid = m.group(1), m.group(2)
        ok, note = self.canary.get(base, lambda: self._probe(base))
        if not ok:
            return "UNKNOWN", "", "board canary failed (" + note + ")"
        return self.verdict(url, jid)


# ------------------------------------------------------------------ iCIMS
# Measured live 2026-10-13 (Prime, Legacy, Select Medical, Emory), on the
# job page with in_iframe=1 as the scraper reads it: an open posting answers
# 200 with a JSON-LD JobPosting and the iCIMS_JobHeaderGroup; a closed or
# unknown id answers 410 with <div class="iCIMS_Message iCIMS_ErrorMessage
# ...">Error: The job that you were looking for either does not exist or is
# no longer open.</div> above the portal's current job cards. CLOSED needs
# the 404/410 AND that message; a 410 without it is UNKNOWN.
_ICIMS_URL = re.compile(r"^https?://([a-z0-9-]+\.icims\.com)/jobs/(\d+)(/[^?#]*)?", re.I)
_ICIMS_GONE = re.compile(r"does not exist or is no longer open", re.I)
_ICIMS_LIST_LINK = re.compile(r"https?://[a-z0-9-]+\.icims\.com/jobs/(\d+)/[^\"'?#\s<>]*/job", re.I)


def icims_page_url(url):
    """The in_iframe=1 job page for an iCIMS job or apply url, or ""."""
    m = _ICIMS_URL.match(url or "")
    if not m:
        return ""
    host, jid, rest = m.group(1).lower(), m.group(2), (m.group(3) or "").rstrip("/")
    path = f"/jobs/{jid}{rest}" if re.fullmatch(r"(?:/[^/]+)?/job", rest) else f"/jobs/{jid}/job"
    return f"https://{host}{path}?in_iframe=1"


class IcimsChecker:
    def __init__(self, http, canary=None):
        self.http = NativeUA(http)
        self.canary = canary or Canary()

    def verdict(self, page, jid):
        st, r = self.http.request("GET", page, headers={"Accept": "text/html,application/xhtml+xml"})
        if r is None or not isinstance(st, int):
            return "UNKNOWN", st, f"job page answered {st}"
        text = r.text or ""
        gone = bool(_ICIMS_GONE.search(text))
        posting = _has_jobposting(text)
        if st in (404, 410):
            if gone and not posting:
                return "CLOSED", st, f"job page {st}: 'does not exist or is no longer open'"
            return "UNKNOWN", st, f"job page {st} without the iCIMS closed message"
        if st != 200:
            return "UNKNOWN", st, f"job page answered {st}"
        if _host(str(r.url or page)) != _host(page):
            return "UNKNOWN", st, "redirected off the portal to " + str(r.url)[:120]
        if gone and not posting:
            return "CLOSED", st, "page says 'does not exist or is no longer open'"
        if posting and not gone and "iCIMS_JobCardItem" not in text and f"/jobs/{jid}/" in (text + str(r.url or page)):
            return "OPEN", st, "JobPosting on the page"
        return "UNKNOWN", st, "page says neither"

    def _probe(self, host):
        st, r = self.http.request("GET", f"https://{host}/jobs/search?ss=1&in_iframe=1",
                                  headers={"Accept": "text/html"})
        if st != 200 or r is None:
            return False, f"job search answered {st}"
        m = _ICIMS_LIST_LINK.search(r.text or "")
        if not m:
            return False, "job search lists no jobs"
        v, _, ev = self.verdict(icims_page_url(m.group(0)), m.group(1))
        return v == "OPEN", f"listed job {m.group(1)} read {v} ({ev})"

    def check(self, row):
        page = icims_page_url((row.get("url") or "").strip())
        if not page:
            return "UNKNOWN", "", "url is not an iCIMS job url"
        host, jid = _host(page), _ICIMS_URL.match(page).group(2)
        ok, note = self.canary.get(host, lambda: self._probe(host))
        if not ok:
            return "UNKNOWN", "", "board canary failed (" + note + ")"
        return self.verdict(page, jid)


# ----------------------------------------------------------------- Phenom
# Measured live 2026-10-13 (Jackson Health, UVA Health): the tenant's
# jobDetail widget (POST {base}/widgets, the call the scraper's detail pass
# makes) answers {"jobDetail": {"status": 200, "hits": 1, "data": {"job":
# {"jobId": ...}}}} for a listed posting and "hits": 0, "data": {} for one
# the board no longer has; the job page {base}/us/en/job/{jobId} answers 200
# with a JSON-LD JobPosting, or HTTP 410 (Jackson 205000 .. 222000 and 1;
# UVA 1). CLOSED needs both (widget hits 0 AND page 404/410), on a row whose
# url is that Phenom job page (review 2026-10-13: a stored job_id that is not
# the widget's jobId reads hits 0 for an open posting; see the row canary in
# PhenomChecker). Rows whose url
# is a Workday job page (DaVita, Bon Secours Mercy, Corewell, SSM, Cone ...:
# the scraper stores the Workday page) go to the Workday checker, the
# employer's own ATS. Crawls of several Phenom tenants look truncated, so a
# stale Phenom row is not presumed closed; OPEN rows are expected.
_PHENOM_JOB_PATH = re.compile(r"/(?:[a-z]{2}/[a-z]{2}/)?job/([^/?#]+)", re.I)
_PHENOM_SEQNO = re.compile(r"(?:EXTERNAL|INTERNAL)[A-Z]{4}$")
_PHENOM_LIST_ID = re.compile(r'"jobId"\s*:\s*"([^"]+)"')
_ATS_HOSTS = re.compile(r"(myworkdayjobs|myworkdaysite|icims|oraclecloud|taleo|smartrecruiters|inforcloudsuite"
                        r"|healthcaresource|successfactors|ultipro|ukg|paylocity|jobvite|brassring)\.", re.I)


def _scraper_literal(name, _cache={}):  # noqa: B006 - parsed once per process
    """A module-level literal of scraper.py (PHENOM_ORGS, PHENOM_BARE_JOB_PATH),
    read with ast rather than by importing scraper, whose import opens a log
    file and reads credentials. None when it cannot be read."""
    if name not in _cache:
        import ast
        val = None
        try:
            with open(os.path.join(os.path.dirname(HERE), "scraper.py"), encoding="utf-8") as f:
                tree = ast.parse(f.read())
            for node in tree.body:
                if isinstance(node, ast.Assign) and len(node.targets) == 1:
                    tgt = node.targets[0]
                elif isinstance(node, ast.AnnAssign):
                    tgt = node.target
                else:
                    continue
                if isinstance(tgt, ast.Name) and tgt.id == name and node.value is not None:
                    val = ast.literal_eval(node.value)
                    break
        except (OSError, SyntaxError, ValueError):
            val = None
        _cache[name] = val
    return _cache[name]


def phenom_base(row):
    """The Phenom career site a row belongs to: PHENOM_ORGS[system], else the
    row's own host when its url is a Phenom job page (BSW). "" if neither.
    2026-10-14: a database export carries the aliased name (scraper emits
    "Ascension Health", HOSPITAL_SYSTEM_ALIASES stores "Ascension"), so the
    emit name that aliases to the row's system is looked up too."""
    orgs = _scraper_literal("PHENOM_ORGS") or {}
    system = row.get("hospital_system") or ""
    base = orgs.get(system)
    if not base:
        aliases = _scraper_literal("HOSPITAL_SYSTEM_ALIASES") or {}
        base = next((orgs[e] for e, canon in aliases.items() if canon == system and e in orgs), "")
    if base:
        return base.rstrip("/")
    url = row.get("url") or ""
    if _PHENOM_JOB_PATH.search(urlsplit(url).path or "") and not _ATS_HOSTS.search(_host(url)):
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}"
    return ""


def _scraped_ts(v):
    """scraped_at (ISO text from wp.sql or a CSV) as epoch seconds; 0 if unreadable."""
    s = str(v or "").strip().replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return 0.0
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def _id_num(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


class PhenomChecker:
    """Two canaries per Phenom site before any row is judged:
      board  the first jobId on the search-results page reads OPEN (widget
             finds it) and its job page answers 200;
      row    the most recently scraped row of that system in the input export
             (prime(rows); rows whose url is a Phenom job page on the site
             come first, since only they can be judged CLOSED) reads OPEN
             through the same widget call, by the id the row itself carries.
             The board canary uses an id from the search page, so it cannot
             tell when the ids this system's rows store are not the widget's
             jobId (every open row would then read hits 0). A checker that
             was never primed fails this canary.
    CLOSED also needs the row's url to be a Phenom /job/{id} page on the
    site's own host: a row whose id comes from job_id (Workday, Infor or other
    apply links) can read OPEN or UNKNOWN through the widget, never CLOSED.
    One exception (2026-10-14): a row whose url is an Oracle requisition
    (Ascension's Phenom front of Oracle Recruiting) is CLOSED when Oracle's
    ById answers items [] for the full id AND, past both canaries, the
    Phenom jobDetail widget answers hits 0 for the row's job_id: two boards
    agreeing, as Workday's detail and search must."""

    def __init__(self, http, workday=None, canary=None, oracle=None):
        self.http = NativeUA(http)
        self.workday = workday or WorkdayChecker(http)
        self.oracle = oracle or OracleChecker(http)
        self.canary = canary or Canary()
        self.site_rows = None   # (base, hospital_system) -> the export row the row canary reads

    @staticmethod
    def row_id(row, base):
        """(Phenom jobId for the row, True when it comes from a Phenom job url
        on base's host) or ("", False)."""
        url = (row.get("url") or "").strip()
        m = _PHENOM_JOB_PATH.search(urlsplit(url).path or "") if _host(url) == _host(base) else None
        wid = unquote(m.group(1)) if m else str(row.get("job_id") or "").strip()
        if not wid or _PHENOM_SEQNO.search(wid):
            return "", False
        return wid, bool(m)

    def prime(self, rows):
        """Pick each (site, system)'s row canary from the input export."""
        best = {}
        for row in rows:
            if slug(row.get("ats_platform")) != "phenom" or _host(row.get("url")) == HCA_HOST:
                continue
            base = phenom_base(row)
            if not base:
                continue
            wid, on_site = self.row_id(row, base)
            if not wid:
                continue
            key = (base, row.get("hospital_system") or "")
            rank = (on_site, _scraped_ts(row.get("scraped_at")), _id_num(row.get("id")))
            if key not in best or rank > best[key][0]:
                best[key] = (rank, row)
        self.site_rows = {k: r for k, (_, r) in best.items()}
        return self.site_rows

    def _row_probe(self, base, system):
        if self.site_rows is None:
            return False, "no input export given for the row canary"
        row = self.site_rows.get((base, system))
        if row is None:
            return False, "no row of this system in the export to test"
        wid, on_site = self.row_id(row, base)
        v, _, ev = self.verdict(base, system, wid, (row.get("url") or "").strip() if on_site else "",
                                closable=on_site)
        return v == "OPEN", (f"most recently scraped row id {row.get('id')} (jobId {wid}, scraped "
                             f"{row.get('scraped_at')}) read {v} ({ev})")

    def _widget(self, base, wid):
        body = {"lang": "en_us", "deviceType": "desktop", "country": "us", "pageName": "job-page",
                "ddoKey": "jobDetail", "jobId": str(wid)}
        st, r = self.http.request("POST", base + "/widgets", json_body=body,
                                  headers={"Accept": "application/json", "Content-Type": "application/json"})
        if st != 200 or r is None:
            return st, None
        try:
            jd = (r.json() or {}).get("jobDetail")
        except Exception:  # noqa: BLE001
            return "bad-json", None
        if not isinstance(jd, dict) or jd.get("status") != 200 or not isinstance(jd.get("hits"), int):
            return "bad-shape", None
        job = (jd.get("data") or {}).get("job") or {}
        return st, (jd["hits"], str(job.get("jobId") or ""))

    @staticmethod
    def page_url(base, system, wid, url=""):
        if url and _host(url) == _host(base) and _PHENOM_JOB_PATH.search(urlsplit(url).path or ""):
            return url
        bare = _scraper_literal("PHENOM_BARE_JOB_PATH") or set()
        return f"{base}/job/{wid}" if system in bare else f"{base}/us/en/job/{wid}"

    def _page(self, page):
        return self.http.request("GET", page, headers={"Accept": "text/html,application/xhtml+xml"})

    def verdict(self, base, system, wid, url="", closable=False):
        """closable: the row's url is a Phenom job page on base's host (its
        id came from that url). Without it a widget miss is UNKNOWN and the
        job page is not fetched."""
        wst, w = self._widget(base, wid)
        if w is None:
            return "UNKNOWN", wst, f"jobDetail widget answered {wst}"
        hits, got = w
        if hits >= 1 and got == wid:
            return "OPEN", wst, f"jobDetail widget finds {wid}"
        if hits != 0:
            return "UNKNOWN", wst, f"jobDetail widget hits {hits}, jobId {got[:40]!r}"
        if not closable:
            return "UNKNOWN", wst, (f"jobDetail widget hits 0 for {wid}, but the row's url is not a Phenom "
                                    f"job page on {_host(base)}; not judged")
        page = self.page_url(base, system, wid, url)
        pst, r = self._page(page)
        if pst in (404, 410) and r is not None and _host(str(r.url or page)) == _host(page):
            return "CLOSED", pst, f"jobDetail widget hits 0 and job page {pst}"
        return "UNKNOWN", pst, f"jobDetail widget hits 0 but job page answered {pst}"

    def _probe(self, base, system):
        for path in ("/us/en/search-results", "/search-results", ""):
            st, r = self.http.request("GET", base + path, headers={"Accept": "text/html"})
            if st != 200 or r is None:
                continue
            ids = [i for i in _PHENOM_LIST_ID.findall(r.text or "") if not _PHENOM_SEQNO.search(i)]
            if not ids:
                continue
            wid = ids[0]
            v, _, ev = self.verdict(base, system, wid)
            if v != "OPEN":
                return False, f"listed job {wid} read {v} ({ev})"
            pst, _ = self._page(self.page_url(base, system, wid))
            if pst != 200:
                return False, f"listed job {wid}: the widget finds it but its job page answered {pst}"
            return True, f"listed job {wid} OPEN, job page 200"
        return False, "no listed job found on the search page"

    def check(self, row):
        url = (row.get("url") or "").strip()
        pre = ""
        if workday_parts(url):
            # The Workday posting decides when it can; when it cannot (UVA's
            # CXS search reports total 0, so its canary fails) the Phenom
            # board is asked the same question under its own rules.
            v = self.workday.check(row)
            if v[0] != "UNKNOWN":
                return v
            pre = "workday: " + str(v[2])[:80] + "; phenom: "
        oracle_closed = ""
        if _ORA.match(url):
            # Ascension: Oracle's answer is OPEN on its own; its items []
            # needs the Phenom board's hits 0 below before it is CLOSED.
            v = self.oracle.check(row)
            if v[0] == "OPEN":
                return v
            if v[0] == "CLOSED":
                oracle_closed = str(v[2])
            pre = "oracle: " + str(v[2])[:80] + "; phenom: "
        base = phenom_base(row)
        if not base:
            return "UNKNOWN", "", pre + "no Phenom site known for this system"
        wid, on_site = self.row_id(row, base)
        if not wid:
            return "UNKNOWN", "", pre + "no Phenom jobId on the row"
        system = row.get("hospital_system") or ""
        ok, note = self.canary.get(base, lambda: self._probe(base, system))
        if not ok:
            return "UNKNOWN", "", pre + "board canary failed (" + note + ")"
        ok, note = self.canary.get(("row", base, system), lambda: self._row_probe(base, system))
        if not ok:
            return "UNKNOWN", "", pre + "row canary failed (" + note + ")"
        if oracle_closed:
            wst, w = self._widget(base, wid)
            if w is None:
                return "UNKNOWN", wst, pre + f"jobDetail widget answered {wst}"
            if w[0] == 0:
                return "CLOSED", wst, f"{oracle_closed}; Phenom jobDetail widget hits 0 for {wid}"
            return "UNKNOWN", wst, pre + f"jobDetail widget hits {w[0]}, jobId {w[1][:40]!r}"
        v, st, ev = self.verdict(base, system, wid, url if on_site else "", closable=on_site)
        return v, st, pre + ev


# ------------------------------------------------------- SmartRecruiters
# Measured live 2026-10-14 (Northwestern Medicine, api.smartrecruiters.com,
# public, no key): GET /v1/companies/{org}/postings/{id} answers 200 for an
# old id too. A closed posting carries "active": false (744000150699610; its
# job page says "Sorry, this job has expired"); a live one "active": true.
# Northwestern re-published about 1,340 postings under new ids on 10-13: the
# old id still answers active true, but its applyUrl names the NEW id
# (744000150157032 -> 744000154749068, same refNumber 210742). That row is
# not closed, it is a re-keyed copy: verdict MOVED, never retired by `sql`
# (the duplicate cleanup decides it against the new id's row). An unknown id
# answers 404, never measured for a real posting: UNKNOWN.
SR_API = "https://api.smartrecruiters.com/v1/companies"
_SR_URL = re.compile(r"^https://jobs\.smartrecruiters\.com/([^/?#]+)/(\d{6,})(?:[-/?#].*)?$", re.I)
_SR_APPLY_ID = re.compile(r"^https://jobs\.smartrecruiters\.com/[^/?#]+/(\d{6,})", re.I)


class SmartRecruitersChecker:
    def __init__(self, http, canary=None):
        self.http = NativeUA(http)
        self.canary = canary or Canary()

    def _detail(self, org, pid):
        st, r = self.http.request("GET", f"{SR_API}/{org}/postings/{pid}", headers={"Accept": "application/json"})
        if st != 200 or r is None:
            return st, None
        try:
            d = r.json()
        except Exception:  # noqa: BLE001
            return "bad-json", None
        if not isinstance(d, dict) or str(d.get("id") or "") != pid or not isinstance(d.get("active"), bool):
            return "bad-shape", None
        return st, d

    def verdict(self, org, pid):
        st, d = self._detail(org, pid)
        if d is None:
            return "UNKNOWN", st, f"posting api answered {st}"
        ref = str(d.get("refNumber") or "")
        tag = f" (refNumber {ref})" if ref else ""
        if d["active"] is False:
            return "CLOSED", st, "posting api active=false" + tag
        m = _SR_APPLY_ID.match(str(d.get("applyUrl") or ""))
        if m and m.group(1) != pid:
            return "MOVED", st, f"posting api active=true; apply link moved to id {m.group(1)}{tag}"
        return "OPEN", st, "posting api active=true" + tag

    def _probe(self, org):
        st, r = self.http.request("GET", f"{SR_API}/{org}/postings?limit=1&offset=0",
                                  headers={"Accept": "application/json"})
        if st != 200 or r is None:
            return False, f"postings list answered {st}"
        try:
            d = r.json() or {}
            first = str(((d.get("content") or [{}])[0] or {}).get("id") or "")
        except Exception:  # noqa: BLE001
            return False, "postings list bad json"
        if not (d.get("totalFound") or 0) > 0 or not first:
            return False, f"postings list total {d.get('totalFound')}"
        v, _, ev = self.verdict(org, first)
        return v == "OPEN", f"listed posting {first} read {v} ({ev})"

    def check(self, row):
        m = _SR_URL.match((row.get("url") or "").strip())
        if not m:
            return "UNKNOWN", "", "url is not a SmartRecruiters posting url"
        org, pid = m.group(1).lower(), m.group(2)
        ok, note = self.canary.get(("sr", org), lambda: self._probe(org))
        if not ok:
            return "UNKNOWN", "", "board canary failed (" + note + ")"
        return self.verdict(org, pid)


# -------------------------------------------------------------------- Jibe
# Measured live 2026-10-14 on jobs.uhsinc.com (Universal Health Services'
# Jibe front of iCIMS; the scraper stores these rows as ats_platform iCIMS):
# an open posting's page (/jobs/{req}, redirected to /careers/jobs/{req})
# answers 200 with a JSON-LD JobPosting and "req_id":"{req}" in its page
# data; a closed one (371160, 369234; 999999999 alike) answers HTTP 500 with
# the 275-byte "Oops! An error has occurred." page, and the front's own
# search, GET {base}/api/jobs?keywords={req}, finds no job with that req_id
# (totalCount 0; 375157 -> totalCount 1, req_id 375157). Every page carries
# the site's NO_JOBS_404 text ("may be no longer available"), so the generic
# checker read all 30 sampled UHS rows "page says neither". CLOSED needs the
# Oops page on two separate fetches AND a search that does not find the
# req_id. A 500 is also what a broken server says, hence the search.
_JIBE_JOB_URL = re.compile(r"^(https://[^/?#]+)/(?:careers/)?jobs/([0-9A-Za-z][0-9A-Za-z-]*)(?:[/?#].*)?$")
_JIBE_OOPS = re.compile(r"<title>\s*Oops! An error has occurred\.\s*</title>", re.I)


def jibe_hosts(_cache={}):  # noqa: B006 - parsed once per process
    """Hosts of scraper.py's JIBE_SITES (read with ast, not imported)."""
    if "hosts" not in _cache:
        sites = _scraper_literal("JIBE_SITES") or {}
        _cache["hosts"] = frozenset(_host(u) for u in sites.values() if isinstance(u, str) and _host(u))
    return _cache["hosts"]


class JibeChecker:
    def __init__(self, http, canary=None):
        self.http = NativeUA(http)
        self.canary = canary or Canary()

    def _page(self, url, jid):
        """('open' | 'oops' | 'other', status)."""
        st, r = self.http.request("GET", url, headers={"Accept": "text/html,application/xhtml+xml"})
        text = (r.text or "") if r is not None else ""
        if st == 200 and _has_jobposting(text) and re.search(r'"(?:req_id|slug)"\s*:\s*"' + re.escape(jid) + '"', text):
            return "open", st
        if st == 500 and _JIBE_OOPS.search(text) and not _has_jobposting(text):
            return "oops", st
        return "other", st

    def _search(self, base, jid):
        """(status, total, [req_id, ...]) of the front's own search for jid."""
        st, r = self.http.request("GET", f"{base}/api/jobs?page=1&limit=10&keywords={jid}",
                                  headers={"Accept": "application/json"})
        if st != 200 or r is None:
            return st, None, []
        try:
            d = r.json() or {}
            ids = [str(((w or {}).get("data") or w or {}).get("req_id") or "") for w in (d.get("jobs") or [])]
            return st, int(d.get("totalCount") or 0), ids
        except Exception:  # noqa: BLE001
            return "bad-json", None, []

    def verdict(self, base, url, jid):
        kind, st = self._page(url, jid)
        if kind == "open":
            return "OPEN", st, "job page 200 with a JobPosting for this req_id"
        if kind != "oops":
            return "UNKNOWN", st, f"job page answered {st}"
        sst, total, ids = self._search(base, jid)
        if total is None:
            return "UNKNOWN", st, f"job page 500 'Oops'; search answered {sst}"
        if jid in ids:
            return "UNKNOWN", st, f"job page 500 'Oops' but the search finds req {jid}"
        if total > len(ids):
            return "UNKNOWN", st, f"job page 500 'Oops'; search total {total}, req not on the first page"
        kind2, st2 = self._page(url, jid)
        if kind2 != "oops":
            return "UNKNOWN", st2, f"first fetch 500 'Oops', the second read {kind2} ({st2})"
        return "CLOSED", st2, f"job page 500 'Oops' on two fetches; search '{jid}' total {total}, no match"

    def _probe(self, base):
        sst, total, ids = self._search(base, "")
        if not total or not ids or not ids[0]:
            return False, f"job search answered {sst}, total {total}"
        v, _, ev = self.verdict(base, f"{base}/jobs/{ids[0]}?lang=en-us", ids[0])
        return v == "OPEN", f"listed req {ids[0]} read {v} ({ev})"

    def check(self, row):
        url = (row.get("url") or "").strip()
        m = _JIBE_JOB_URL.match(url)
        if not m or _host(url) not in jibe_hosts():
            return "UNKNOWN", "", "url is not a Jibe job url"
        base, jid = m.group(1), m.group(2)
        ok, note = self.canary.get(("jibe", base), lambda: self._probe(base))
        if not ok:
            return "UNKNOWN", "", "board canary failed (" + note + ")"
        return self.verdict(base, url, jid)


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
                if r.get("verdict") in ("CLOSED", "OPEN", "MOVED") or (r.get("verdict") == "UNKNOWN" and not retry_unknown):
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

    rows = all_rows = rows_from_file(args.rows_file) if args.rows_file else rows_from_db(args)
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

    http = Http(Budget(args.max_requests, stop_at), per_host=args.per_host, pause=(args.pause, 2 * args.pause))
    # Phenom row canaries come from the whole export (resumed or filtered
    # runs pick the same row).
    checker_for("Phenom", http).prime(all_rows)
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
            verdict, status, ev = checker_for_row(row, http).check(row)
        except Http.OutOfBudget:
            stopped.set()
            return
        except Exception as e:  # noqa: BLE001
            verdict, status, ev = "UNKNOWN", "", "checker error " + type(e).__name__
        write(row, verdict, status, ev)

    workers = max(2, min(args.workers, 2 * len(by_host)))
    ex = ThreadPoolExecutor(max_workers=workers)
    try:
        list(ex.map(one, order))
    except KeyboardInterrupt:
        # Without this the pool's shutdown would work through every queued
        # row before exiting; the docs tell the operator Ctrl+C stops a run.
        stopped.set()
        print("interrupted: finishing the requests in flight, starting no new row", flush=True)
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
    for f in files.values():
        f.close()
    print(f"done: {dict(stats)}; requests {http.budget.used}"
          + ("; STOPPED (request budget, stop time or Ctrl+C)" if stopped.is_set() else ""), flush=True)
    for s, c in sorted(per_sys.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"  {s}: {dict(c)}")
    print("requests by host: " + ", ".join(f"{h} {n}" for h, n in http.by_host.most_common()))


# -------------------------------------------------------------------- sql
def run_sql(args):
    """Owner-run retire + restore files from the CSVs in --out-dir. With
    --db-write both files are in the db_write format (no comment or blank
    lines, one statement per line, every locking statement prefixed with
    set lock_timeout = '3s';); without it they carry the explanatory header
    that 115 / 115r had. Each run names its own backup table (--backup-table,
    default retired_closed_backup_<UTC date>) so a rollback restores only the
    rows its own retire file saved."""
    verdicts = latest_verdicts(args.out_dir)
    keep = set(args.system or [])
    closed = sorted(int(i) for i, r in verdicts.items()
                    if r.get("verdict") == "CLOSED" and (not keep or r.get("hospital_system") in keep))
    by_sys = collections.Counter(verdicts[str(i)]["hospital_system"] for i in closed)
    batches = [closed[i:i + args.batch] for i in range(0, len(closed), args.batch)]
    guard = f"is_active and scraped_at < {q(args.stale_before)}"
    tbl = "public." + args.backup_table
    name, rname = os.path.basename(args.out_sql), os.path.basename(args.out_restore)
    lines = [
        f"-- {name}  (generated by hospital-scraper tools/verify_stale.py sql)",
        "-- DATA project (tlzxajgonuevbqaelhvf) only. OWNER-RUN. Never between 23:30 and 05:30 UTC.",
        f"-- Retires {len(closed)} active rows whose posting the employer's own board confirmed CLOSED",
        f"-- (CSVs in {args.out_dir}). UNKNOWN and OPEN rows are not touched.",
        "-- Per system: " + "; ".join(f"{s} {n}" for s, n in by_sys.most_common()),
        f"-- Guard on every statement: {guard}  (a row re-stamped by a later crawl is left alone).",
        f"-- Undo: {rname} restores is_active from the backup table.",
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
    rlines = [
        f"-- {rname}  (undo of {name})",
        "-- DATA project (tlzxajgonuevbqaelhvf) only. OWNER-RUN. Never between 23:30 and 05:30 UTC.",
        f"-- Puts is_active back from {tbl} for rows {name} retired that no crawl has re-stamped since",
        f"-- (scraped_at still before {args.stale_before}). The backup table is kept; drop it by hand later.",
        "",
        f"set lock_timeout = '3s'; update public.hospital_jobs h set is_active = b.is_active from {tbl} b "
        "where h.id = b.id and h.is_active = false and b.is_active = true "
        f"and h.scraped_at < {q(args.stale_before)};",
        "",
        f"select count(*) as restored_active from public.hospital_jobs h join {tbl} b using (id) where h.is_active;",
        "",
    ]
    if args.db_write:
        lines = [x for x in lines if x and not x.startswith("--")] + [""]
        rlines = [x for x in rlines if x and not x.startswith("--")] + [""]
    with open(args.out_sql, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    with open(args.out_restore, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(rlines))
    print(f"wrote {args.out_sql}: {len(closed)} CLOSED ids in {len(batches)} batches; {dict(by_sys)}")
    print(f"wrote {args.out_restore} (backup table {tbl})")


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
    c.add_argument("--workers", type=int, default=8,
                   help="threads across all hosts; does NOT slow one host (see --per-host / --pause)")
    c.add_argument("--per-host", type=int, default=PER_HOST,
                   help=f"requests in flight per host, 1..{PER_HOST} (default {PER_HOST}); 1 if a board answers 429")
    c.add_argument("--pause", type=float, default=PAUSE[0],
                   help=f"seconds each host slot rests after a request: a random P..2P (default {PAUSE[0]})")
    c.add_argument("--retry-unknown", action="store_true")
    s = sub.add_parser("sql", help="write the owner-run retire + restore SQL from the CSVs")
    s.add_argument("--out-dir", default=DEFAULT_OUT)
    s.add_argument("--out-sql", required=True)
    s.add_argument("--out-restore", required=True)
    s.add_argument("--stale-before", required=True, help="timestamptz; only rows last scraped before it are touched")
    s.add_argument("--system", action="append")
    s.add_argument("--batch", type=int, default=1000)
    s.add_argument("--backup-table", default="retired_closed_backup_" + utcnow().strftime("%Y%m%d"),
                   help="table in schema public that saves the retired rows (default: by UTC date)")
    s.add_argument("--db-write", action="store_true",
                   help="db_write format: no comment or blank lines, one statement per line")
    args = ap.parse_args()
    if args.cmd == "check":
        if not args.rows_file and not args.from_db:
            ap.error("check needs --rows-file or --from-db")
        if not 1 <= args.per_host <= PER_HOST:
            ap.error(f"--per-host is 1..{PER_HOST}")
        if not 0.5 <= args.pause <= 30:
            ap.error("--pause is 0.5..30 seconds")
        run_check(args)
    else:
        if args.batch > 1000:
            ap.error("--batch is at most 1000")
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", args.backup_table):
            ap.error("--backup-table must be a plain lower-case table name")
        run_sql(args)


if __name__ == "__main__":
    main()
