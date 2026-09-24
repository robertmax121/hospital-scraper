"""
Hospital Job Scraper — Maximum Coverage Build
Fixed API endpoints + verbose error logging to diagnose 0-job issues.
"""

import asyncio
import aiohttp
import html as htmllib
import json
import logging
import random
import re
from html import unescape as _html_unescape   # 2026-09-10: Paycor / TaleoBE / HCTS parsers
import time
import os
import weakref
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional
from city_utils import clean_city
from specialty_canon import canonical_specialty

# curl_cffi provides browser-grade TLS fingerprints (Chrome/Firefox
# impersonation). Required by the HCA / Houston Methodist / Oceans adapters:
# Cloudflare on careers.hcahealthcare.com blocks aiohttp's TLS outright but
# accepts a Firefox fingerprint (verified 2026-07-28), and Workday CXS
# sometimes 403s non-browser TLS. Optional import so every other adapter
# still runs if the wheel is missing.
try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/run_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Proxy rotation ─────────────────────────────────────────────────────────
# 2026-09-17: failures before a pool address is retired for the run. The first
# run through the Webshare pool (09-16 22:27 UTC) showed a slice of the 100
# addresses 403'd by the Jibe fronts while the rest passed; three strikes
# takes a blocked address out instead of letting it keep costing retries.
PROXY_STRIKES = int(os.getenv("PROXY_STRIKES", "3"))


class ProxyRotator:
    def __init__(self):
        self._bad: dict[str, int] = {}
        self.retired: set[str] = set()
        self.fallbacks = 0
        proxy_file = os.environ.get("PROXY_FILE", "proxies.txt")
        if os.path.exists(proxy_file):
            with open(proxy_file) as f:
                self.proxies = [line.strip().rstrip(",") for line in f if line.strip().rstrip(",")]
        else:
            raw = os.environ.get("PROXY_LIST", "")
            self.proxies = [p.strip().rstrip(",") for p in re.split(r"[,\n]+", raw) if p.strip().rstrip(",")]
        # 2026-09-16: Webshare showed ZERO requests in 30 days on a live plan:
        # proxies.txt is untracked and PROXY_LIST is unset on Railway, so every
        # adapter ran direct. With WEBSHARE_API_KEY set, the current direct-mode
        # list is fetched at startup, so a rotated pool never goes stale.
        if not self.proxies and os.environ.get("WEBSHARE_API_KEY"):
            self.proxies = self._webshare_list(os.environ["WEBSHARE_API_KEY"])
        self._i = 0
        if self.proxies:
            logger.info(f"  Proxies loaded: {len(self.proxies)} available")
        else:
            logger.warning("  No proxies configured — running without proxies")

    @staticmethod
    def _webshare_list(api_key: str) -> list:
        """host:port:user:pass entries from the Webshare API (direct mode)."""
        try:
            import urllib.request
            import json as _json
            hdr = {"Authorization": "Token " + api_key}
            def _get(url):
                req_ = urllib.request.Request(url, headers=hdr)
                with urllib.request.urlopen(req_, timeout=30) as r:
                    return _json.loads(r.read().decode())
            cfg = _get("https://proxy.webshare.io/api/v2/proxy/config/")
            out, url = [], "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page_size=100"
            while url and len(out) < 500:
                page = _get(url)
                for item in page.get("results", []):
                    if item.get("valid", True):
                        out.append(f"{item['proxy_address']}:{item['port']}:{cfg['username']}:{cfg['password']}")
                url = page.get("next")
            logger.info(f"  Webshare: {len(out)} proxies fetched")
            return out
        except Exception as e:
            logger.warning(f"  Webshare list fetch failed ({e}); running without proxies")
            return []

    @staticmethod
    def _fmt(p: str) -> str:
        parts = p.split(":")
        # Support both host:port:user:pass and user:pass@host:port formats
        if len(parts) == 4:
            return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        return f"http://{p}"

    def get(self) -> Optional[str]:
        """Next live address in rotation; None (direct) when the pool is empty
        or every address has been retired this run."""
        if not self.proxies:
            return None
        for _ in range(len(self.proxies)):
            p = self.proxies[self._i % len(self.proxies)]
            self._i += 1
            url = self._fmt(p)
            if url not in self.retired:
                return url
        return None

    def mark_bad(self, proxy_url: Optional[str], why: str = "") -> None:
        """Count a failed proxied request; PROXY_STRIKES of them retire the
        address for the rest of the run."""
        if not proxy_url:
            return
        self.fallbacks += 1
        n = self._bad.get(proxy_url, 0) + 1
        self._bad[proxy_url] = n
        if n == PROXY_STRIKES and proxy_url not in self.retired:
            self.retired.add(proxy_url)
            live = sum(1 for p in self.proxies if self._fmt(p) not in self.retired)
            host = proxy_url.rsplit("@", 1)[-1]
            logger.info(f"  Proxy retired after {n} failures ({why}): {host}; {live} live")

proxies = ProxyRotator()

# ── Job dataclass ──────────────────────────────────────────────────────────
@dataclass
class Job:
    title: str
    hospital_system: str
    hospital_name: str
    city: str
    state: str
    location: str
    specialty: str
    job_type: str
    url: str
    job_id: str
    posted_date: str
    description: str
    ats_platform: str
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    # Posted pay (2026-08-21): set by adapters with STRUCTURED salary fields
    # (USAJobs PositionRemuneration, Lever salaryRange). When left None,
    # normalize_job falls back to regex extraction from title+description.
    wage_min: float | None = None
    wage_max: float | None = None
    wage_unit: str | None = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

async def jitter(): await asyncio.sleep(random.uniform(0.8, 2.5))
# 2026-07-31: cap raised 500 -> 8000. At 500 chars a description was cut
# mid-word, usually before the job-specific text even started, so postings from
# the same system shared a byte-identical boilerplate preamble — duplicate
# primary content across indexed pages. It also kept descriptions short enough
# that many fell under the desc_len >= 200 bar the sitemap and the page-level
# indexability rule both use.
#
# Measured impact is bounded: only 5,246 of 137,995 active jobs were actually
# at the cap (22,135 were already under 500 naturally, and 110,582 have NO
# description at all — those need per-job detail fetches, a separate fix).
#
# 8000 is well past a typical posting (~2-6k after tag stripping) and costs
# nothing structurally: description is TEXT, and Postgres TOASTs + compresses
# anything over ~2KB. Est. +150-200MB against a 796MB database.
_BLOCK_TAG_RX = re.compile(
    r"</?(?:p|div|br|li|ul|ol|h[1-6]|tr|table|thead|tbody|section|article|header|footer|blockquote|pre|dl|dt|dd|hr)\b[^>]*>",
    re.I)


def strip_html(s):
    s = s or ""
    # Greenhouse's boards API returns `content` HTML-ENTITY-ESCAPED
    # ("&lt;div class=&quot;…&quot;&gt;"), so the tag regex below saw no tags
    # and job pages rendered literal markup (2026-08-25, One Medical /
    # Silver Spring example). Decode twice (their payloads carry &amp;nbsp;
    # style double escapes) only when the text has escaped tags and no real
    # ones — plain descriptions never hit this branch.
    if "<" not in s and "&lt;" in s:
        s = htmllib.unescape(htmllib.unescape(s))
    # 2026-09-22: tags used to vanish without a separator, so every HTML body
    # (Workday / TalentBrew / HCA JSON-LD, Oracle, Phenom, Findly) arrived as
    # one glued run ("InsurancePaid Time Off", "Mon-FriLocation") and the
    # entities stayed ("License&nbsp;"). The site's schedule chips and
    # qualification lines were missing or wrong because of it. Block tags now
    # end a line, inline tags become a space, entities decode, and only runs
    # of spaces collapse; the line breaks are what the parsers key on.
    if "<" in s:
        s = _BLOCK_TAG_RX.sub("\n", s)
        s = re.sub(r"<[^>]+>", " ", s)
    if "&" in s:
        s = htmllib.unescape(s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()[:8000]


# ── Workday job descriptions (2026-08-03) ─────────────────────────────────
# Workday's LIST endpoint returns an empty jobDescription, which is why all
# ~45k Workday rows have no description and are therefore ineligible for the
# sitemap (app/sitemap.js tier 3 requires >= 200 chars). The per-job DETAIL
# endpoint carries it — and also `startDate`, a real ISO date that replaces
# the unsortable relative `postedOn` label ("Posted 3 Days Ago"). One request
# per job fixes both problems at once.
#
# OFF BY DEFAULT — set WD_FETCH_DESCRIPTIONS=1 to enable. When on, this adds
# ONE HTTP request per job that lacks a description, so it is capped and
# throttled rather than let loose on 45k jobs:
#   WD_DESC_MAX_PER_RUN  hard ceiling per scrape (default 500 = a canary batch)
#   WD_DESC_CONCURRENCY  parallel detail fetches (default 4)
# Raise MAX_PER_RUN only after watching a full run: 45k detail hits in one
# night is exactly the pattern that gets a tenant to rate-limit us.
# 2026-09-22 (owner: fill the job page boxes): ON by default. Workday is 38%
# of active rows and only a quarter of them had a body; the per-job endpoint
# also carries timeType (employment type) and startDate (ISO posted date).
# Budget 6,000 a night across all tenants, at most budget/DETAIL_TENANT_SHARE
# per tenant, 4 in flight per tenant and WD_DESC_GLOBAL_CONCURRENCY in flight
# overall (the ~99 tenants list in parallel, so a per-tenant limit alone
# would allow hundreds of simultaneous detail requests).
WD_FETCH_DESCRIPTIONS = os.getenv("WD_FETCH_DESCRIPTIONS", "1") == "1"
WD_DESC_MAX_PER_RUN   = int(os.getenv("WD_DESC_MAX_PER_RUN", "6000"))
WD_DESC_CONCURRENCY   = int(os.getenv("WD_DESC_CONCURRENCY", "4"))
WD_DESC_GLOBAL_CONCURRENCY = int(os.getenv("WD_DESC_GLOBAL_CONCURRENCY", "12"))
_wd_detail_gate = None   # asyncio.Semaphore, created per run in run_workday()


class _DescBudget:
    """GLOBAL cap on detail fetches for a whole run.

    scrape_workday() runs once per tenant and there are ~99 of them, so a
    per-tenant cap of 500 would authorise ~49,500 requests a night — the exact
    opposite of the intended canary. This budget is shared across every tenant
    so WD_DESC_MAX_PER_RUN means what it says. asyncio is single-threaded and
    take() has no await inside it, so a plain int needs no lock.
    """
    def __init__(self, total):
        self.remaining = max(0, total)
        self.spent = 0

    def take(self, n):
        n = max(0, min(n, self.remaining))
        self.remaining -= n
        self.spent += n
        return n


WD_DESC_BUDGET = _DescBudget(WD_DESC_MAX_PER_RUN)

# Aya description pass (2026-08-04) — same containment contract as Workday's:
# off by default, run-wide budget, throttled. Aya's per-job JSON endpoint is
# ~6KB so the budget can be generous once the canary run looks right.
AYA_FETCH_DESCRIPTIONS = os.getenv("AYA_FETCH_DESCRIPTIONS", "0") == "1"
AYA_DESC_MAX_PER_RUN   = int(os.getenv("AYA_DESC_MAX_PER_RUN", "500"))
AYA_DESC_CONCURRENCY   = int(os.getenv("AYA_DESC_CONCURRENCY", "4"))
AYA_DESC_BUDGET        = _DescBudget(AYA_DESC_MAX_PER_RUN)

# ── Detail passes for the platforms whose list endpoints carry no posting body
# (2026-09-21, owner: the v2 job page needs the description, employment type,
# schedule, benefits and bonus amounts, and 90k Oracle / Phenom / TalentBrew /
# HCA rows had none). Same containment contract as the Workday pass: one
# run-wide budget per platform, shuffled with transparency states first, and
# the enrichment trigger keeps what a night fetched. On by default; DETAIL_FETCH=0
# turns every pass off.
DETAIL_FETCH            = os.getenv("DETAIL_FETCH", "1") == "1"
DETAIL_CONCURRENCY      = int(os.getenv("DETAIL_CONCURRENCY", "4"))
ORACLE_DESC_MAX_PER_RUN = int(os.getenv("ORACLE_DESC_MAX_PER_RUN", "8000"))   # 2026-09-22: 3,000 -> 8,000 after a clean first night
TB_DESC_MAX_PER_RUN     = int(os.getenv("TB_DESC_MAX_PER_RUN", "2500"))
PHENOM_DESC_MAX_PER_RUN = int(os.getenv("PHENOM_DESC_MAX_PER_RUN", "2500"))
DETAIL_TENANT_SHARE     = int(os.getenv("DETAIL_TENANT_SHARE", "4"))   # one tenant takes at most budget/SHARE per run
ORACLE_DESC_BUDGET      = _DescBudget(ORACLE_DESC_MAX_PER_RUN)
TB_DESC_BUDGET          = _DescBudget(TB_DESC_MAX_PER_RUN)
PHENOM_DESC_BUDGET      = _DescBudget(PHENOM_DESC_MAX_PER_RUN)
DETAIL_PRIORITY_STATES  = {"CA", "CO", "CT", "DC", "HI", "IL", "MD", "MN", "NJ", "NY", "VT", "WA"}
DETAIL_MIN_CHARS        = int(os.getenv("DETAIL_MIN_CHARS", "1500"))  # shorter than this = a teaser, worth a detail fetch

# Hard stop for Workday list pagination. Replaces the old `offset >= total`
# break, which truncated six large tenants at exactly 2,000 jobs because
# Workday caps the REPORTED total at 2000 while still serving results beyond it
# (see the note at the end-of-pagination check in scrape_workday). 20,000 =
# 1,000 pages at LIMIT=20; no health system has that many open reqs, so hitting
# this means something is wrong and the log line will say so.
WD_MAX_OFFSET = int(os.getenv("WD_MAX_OFFSET", "20000"))



# ── Direct-request retry + per-host pacing (2026-09-24, push 1) ─────────────
# Railway runs with no proxy pool, and a direct request used to get exactly one
# attempt: a 429 or a 5xx on the first page of a tenant returned it empty, and
# three empty nights retire every row it had (Connally, Odessa, Crane, Lubbock
# Heart, Shamrock, Nacogdoches and Rolling Plains all answer when fetched one at
# a time, and each came back 0 on a Railway run between 09-22 and 09-24).
# A direct request that answers one of DIRECT_RETRY_STATUSES is now retried up
# to HTTP_RETRIES times: after the server's Retry-After when it sends one (a
# wait longer than HTTP_RETRY_AFTER_CAP is not retried at all), otherwise after
# exponential backoff with jitter (1-2 s, 3-6 s, 9-18 s at the default base).
# Two per-host limits keep a dead host from eating the run: a host whose last
# HTTP_RETRY_HOST_TRIP requests all failed after their retries gets no more
# retries until one of its requests succeeds, and one host never sleeps more
# than HTTP_RETRY_HOST_SLEEP_CAP seconds in retries per run. Transport errors
# and timeouts are not retried here (they raise to the adapter as before).
HTTP_RETRIES               = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_RETRY_BASE            = float(os.getenv("HTTP_RETRY_BASE", "2"))
HTTP_RETRY_AFTER_CAP       = float(os.getenv("HTTP_RETRY_AFTER_CAP", "30"))
HTTP_RETRY_HOST_SLEEP_CAP  = float(os.getenv("HTTP_RETRY_HOST_SLEEP_CAP", "240"))
HTTP_RETRY_HOST_TRIP       = int(os.getenv("HTTP_RETRY_HOST_TRIP", "3"))
# Tenants of one runner that share a host are scraped at most this many at a
# time (run_paycom, run_paylocity, run_ukg, run_adp, run_healthcaresource and
# run_kronos used to gather every tenant against one host at once, which is
# the burst Paylocity answers with 429), with a short pause before the next
# tenant on that host starts.
HOST_CONCURRENCY           = int(os.getenv("HOST_CONCURRENCY", "2"))
HOST_TENANT_SPACING        = (0.5, 1.5)


class _PacingState:
    """Per-event-loop retry bookkeeping. scrape() runs the hospital crawl and
    the travel crawl in two asyncio.run() calls, and the tests run many, so
    the state lives with the loop instead of the module and every run starts
    clean."""
    def __init__(self):
        self.slept: dict[str, float] = {}       # host -> seconds slept in retries
        self.fail_streak: dict[str, int] = {}   # host -> requests in a row that failed after retries
        self.retries: dict[str, int] = {}       # host -> retry attempts made
        self.recovered: dict[str, int] = {}     # host -> requests that succeeded on a retry
        self.gave_up: dict[str, int] = {}       # host -> requests returned still failing


_PACING_BY_LOOP = weakref.WeakKeyDictionary()
_PACING_NO_LOOP = _PacingState()


def _pacing() -> _PacingState:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return _PACING_NO_LOOP
    st = _PACING_BY_LOOP.get(loop)
    if st is None:
        st = _PACING_BY_LOOP[loop] = _PacingState()
    return st


def _url_host(url) -> str:
    try:
        return (urlsplit(str(url)).hostname or "").lower()
    except Exception:
        return ""


def _retry_after_seconds(resp) -> Optional[float]:
    """Retry-After as seconds (delta-seconds or an HTTP date); None when absent
    or unreadable."""
    hdrs = getattr(resp, "headers", None)
    raw = hdrs.get("Retry-After") if hdrs is not None else None
    if raw is None:
        return None
    raw = str(raw).strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


async def _retry_sleep(seconds: float) -> None:
    """Indirection so the tests can record the waits instead of sleeping."""
    await asyncio.sleep(seconds)


def _retry_summary() -> str:
    st = _pacing()
    if not st.retries:
        return "HTTP retries: none"
    top = sorted(st.retries.items(), key=lambda kv: -kv[1])[:8]
    return (f"HTTP retries: {sum(st.retries.values())} on {len(st.retries)} hosts; "
            f"{sum(st.recovered.values())} requests recovered, {sum(st.gave_up.values())} still failing; "
            + ", ".join(f"{h} {n}" for h, n in top))


class _FallbackResponse:
    """Wrapper so we can use 'async with' syntax with fallback logic."""
    def __init__(self, session, method, url, proxy, kwargs):
        self._s = session
        self._method = method
        self._url = url
        self._proxy = proxy
        self._kw = kwargs
        self._ctx = None

    # Statuses a proxied attempt retries direct. 402 = Webshare "Payment
    # Required" (the pool out of paid bandwidth, 2026-07-28); 407 = proxy auth;
    # 403 / 429 = the target blocks or throttles that address (the 09-16 run:
    # part of the Webshare pool is 403'd by the Jibe fronts, and every adapter
    # stops paging at its first failed page, so Novant came back 101 of 1,688
    # and UHS 1,140 of 4,834); 5xx / 52x = the proxy or an edge in front of
    # the target. A direct retry costs one request; a broken page costs the
    # rest of the tenant.
    PROXY_RETRY_STATUSES = frozenset({402, 403, 407, 429, 500, 502, 503, 504,
                                      520, 521, 522, 523, 524, 525, 526, 530})
    # Statuses a DIRECT attempt retries after a wait (2026-09-24): throttling
    # and transient server or edge failures. 403 and 404 are answers, not
    # hiccups; 525 / 526 / 530 are TLS or DNS misconfiguration that a retry
    # will not fix.
    DIRECT_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504,
                                       520, 521, 522, 523, 524})

    async def __aenter__(self):
        fn = getattr(self._s, self._method)
        if self._proxy:
            try:
                self._ctx = fn(self._url, proxy=self._proxy, **self._kw)
                r = await self._ctx.__aenter__()
            except Exception as e:
                # Refused, reset, timed out, proxy auth: whatever the proxied
                # attempt did, the direct attempt is the one that counts. The
                # old rule only fell back on 502 / 407 / 402 text matches, so a
                # timed-out proxy ended the tenant's pagination.
                proxies.mark_bad(self._proxy, type(e).__name__)
            else:
                if r.status not in self.PROXY_RETRY_STATUSES:
                    return r
                proxies.mark_bad(self._proxy, f"HTTP {r.status}")
                await self._ctx.__aexit__(None, None, None)
        return await self._direct(fn)

    async def _direct(self, fn):
        """The direct attempt, retried on DIRECT_RETRY_STATUSES within the
        per-host limits above. Returns the last response either way, so the
        adapter sees the same status it always did when retries run out."""
        host = _url_host(self._url)
        st = _pacing()
        attempt = 0
        while True:
            self._ctx = fn(self._url, **self._kw)
            r = await self._ctx.__aenter__()
            if r.status not in self.DIRECT_RETRY_STATUSES:
                st.fail_streak[host] = 0
                if attempt:
                    st.recovered[host] = st.recovered.get(host, 0) + 1
                return r
            delay = self._retry_delay(r, attempt, host, st)
            if delay is None:
                st.gave_up[host] = st.gave_up.get(host, 0) + 1
                streak = st.fail_streak[host] = st.fail_streak.get(host, 0) + 1
                if streak == HTTP_RETRY_HOST_TRIP:
                    logger.info(f"HTTP retry: {host} failed {streak} requests in a row after retries; "
                                f"no more retries on it until one of its requests succeeds")
                return r
            logger.info(f"HTTP {r.status} from {host}: retry {attempt + 1}/{HTTP_RETRIES} in {delay:.1f}s")
            st.slept[host] = st.slept.get(host, 0.0) + delay
            st.retries[host] = st.retries.get(host, 0) + 1
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None
            await _retry_sleep(delay)
            attempt += 1

    @staticmethod
    def _retry_delay(r, attempt: int, host: str, st: "_PacingState") -> Optional[float]:
        """Seconds to wait before the next attempt, or None for no retry."""
        if attempt >= HTTP_RETRIES:
            return None
        if st.fail_streak.get(host, 0) >= HTTP_RETRY_HOST_TRIP:
            return None
        ra = _retry_after_seconds(r)
        if ra is not None:
            if ra > HTTP_RETRY_AFTER_CAP:
                return None      # asked to wait longer than a run can afford: do not hit it early
            delay = ra + random.uniform(0.2, 1.0)
        else:
            d = HTTP_RETRY_BASE * (3 ** attempt)
            delay = random.uniform(d / 2, d)
        if st.slept.get(host, 0.0) + delay > HTTP_RETRY_HOST_SLEEP_CAP:
            return None
        return delay

    async def __aexit__(self, *args):
        if self._ctx:
            await self._ctx.__aexit__(*args)


async def _gather_by_host(session, items, scrape, host_of) -> list:
    """asyncio.gather over (system, config) tenants with at most
    HOST_CONCURRENCY of them in flight per host, and a short pause before the
    next tenant on that host starts. Tenants on different hosts still run in
    parallel. Same result shape as gather(..., return_exceptions=True)."""
    gates: dict[str, asyncio.Semaphore] = {}
    limit = max(1, HOST_CONCURRENCY)

    async def one(system, cfg):
        try:
            host = host_of(cfg) or ""
        except Exception:
            host = ""
        gate = gates.get(host)
        if gate is None:
            gate = gates[host] = asyncio.Semaphore(limit)
        async with gate:
            result = await scrape(session, system, cfg)
            await asyncio.sleep(random.uniform(*HOST_TENANT_SPACING))
            return result

    return await asyncio.gather(*[one(s, c) for s, c in items], return_exceptions=True)


def req(session, method, url, **kwargs):
    """Drop-in for 'async with session.get/post(...)' with proxy fallback."""
    proxy = kwargs.pop("proxy", None)
    return _FallbackResponse(session, method, url, proxy, kwargs)



# ── Transparency states first (2026-09-10, S-scraper-2) ──────────────────
# Pay-transparency states carry the rows the board sells best (posted pay on
# the card), so every per-state or per-org crawl starts there: the HCA state
# loop, the NeoGov agencies, the UKG / ADP / Kronos configs that carry a
# state, and the Workday tenant order (also the order the WD_DESC_BUDGET
# detail fetches are spent in). Ordering is the whole mechanism: nothing is
# skipped, but when a per-run cap or a Cloudflare rate-limit bites mid-run,
# it bites the non-transparency states. Documented in reports/S-scraper-2.md.
TRANSPARENCY_STATES = ("CO", "WA", "CA", "NY", "IL", "MN", "MD", "HI", "DC", "NJ", "MA", "VT")
_TRANSPARENCY_RANK = {s: i for i, s in enumerate(TRANSPARENCY_STATES)}


def priority_states_first(items, state_of):
    """Stable sort: TRANSPARENCY_STATES order first, everything else after in
    its original order. state_of(item) returns a 2-letter code or ""."""
    def rank(it):
        try:
            st = (state_of(it) or "").strip().upper()
        except Exception:
            st = ""
        return _TRANSPARENCY_RANK.get(st, len(TRANSPARENCY_STATES))
    return sorted(items, key=rank)


# ── Partial-run protection (2026-09-10, S-scraper-2) ────────────────────────
# An adapter that KNOWS it did not finish (HCA with a state slice that failed
# every retry, for example) adds its emit-name here and the upsert's
# per-system sweep skips that system tonight regardless of the 25% ratio
# guard. The ratio guard alone lets a half-crawl of a 16k-row system retire
# the other half: 3,368 HCA rows on 2026-09-10 were the survivors of exactly
# that (per-state counts of 1000 / 500 / 500 / 500 = page boundaries).
PARTIAL_SYSTEMS: set[str] = set()

# ── Cross-tenant dedupe at the source (2026-09-10, S-scraper-2) ────────────
# Two ATS tenants of one system can list the same posting (CityMD's two
# Workday entries doubled 608 rows before the second entry was removed).
# Systems mapped to one family are deduped in finalize_jobs: a row whose
# (family, title, facility, city, state) was already emitted by a DIFFERENT
# tenant is dropped; the same key from the same tenant is kept (two reqs for
# one role at one site are two postings). Keys are the scraper-side emit
# names, before HOSPITAL_SYSTEM_ALIASES. Opt-in on purpose: only families
# known to cross-list belong here.
SYSTEM_FAMILIES = {
    "CityMD":                     "CityMD",
    "Summit Health (CityMD)":     "CityMD",
    "Summit Health (Physicians)": "CityMD",
}

# ── CMS blank-state fill (2026-09-10, S-scraper-2) ──────────────────────────
# 24,158 active rows (14.4%) had no state on 2026-09-10, in 85 (system,
# hospital_name) groups whose hospital_name is the system itself (WVU
# Medicine 3,183, Trinity Health 2,671, ...). The September backfill filled
# what it could from the CMS `hospitals` table once and the nightly upsert
# blanked it again, so the same lookup now runs inside normalize_job: a
# (normalised) facility name that is unique in CMS gives city and state, and
# the adapter's raw location text gets the same lookup because Workday's
# locationsText is often a facility name that clean_city (rightly) refuses as
# a city. Nothing is guessed from the system alone: CMS hospital_system is
# far too sparse to call a system single-state (Trinity Health has 5 tagged
# rows there, all MI).
_CMS_LOOKUP: dict | None = None      # normalised name -> (city, state)
_CMS_FILLS = {"n": 0}


def _cms_norm(name) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bmed\b", "medical", s)
    s = re.sub(r"\bctr\b", "center", s)
    s = re.sub(r"\bhosp\b", "hospital", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\b(the|inc|llc)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def set_cms_lookup(rows) -> int:
    """Build the lookup from hospitals rows ({hospital_name, city, state}).
    Names shared by rows in different places are dropped (87 of 5,295 CMS
    names, e.g. Memorial Hospital). Returns the usable count."""
    global _CMS_LOOKUP
    seen, ambiguous = {}, set()
    for r in rows or []:
        k = _cms_norm(r.get("hospital_name"))
        st = (r.get("state") or "").strip().upper()
        if not k or len(st) != 2:
            continue
        v = ((r.get("city") or "").strip(), st)
        if k in seen and seen[k] != v:
            ambiguous.add(k)
        seen.setdefault(k, v)
    for k in ambiguous:
        seen.pop(k, None)
    _CMS_LOOKUP = seen
    _CMS_FILLS["n"] = 0
    return len(seen)


def load_cms_lookup() -> int:
    """One read-only PostgREST pass over the CMS hospitals table per run (same
    SUPABASE_URL / key the upsert uses). Without credentials the lookup stays
    empty and normalize_job fills nothing, so dry runs behave as before."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = (os.environ.get("SUPABASE_KEY", "")
              or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
    if not sb_url or not sb_key:
        set_cms_lookup([])
        logger.info("CMS lookup: SUPABASE_URL/SUPABASE_KEY not set — blank-state fill off")
        return 0
    import urllib.request as _urlreq
    rows, off, page = [], 0, 1000
    try:
        while True:
            u = (f"{sb_url.rstrip('/')}/rest/v1/hospitals?select=hospital_name,city,state"
                 f"&order=id&limit={page}&offset={off}")
            rq = _urlreq.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
            with _urlreq.urlopen(rq, timeout=30) as resp:
                chunk = json.loads(resp.read().decode())
            rows.extend(chunk)
            if len(chunk) < page:
                break
            off += page
    except Exception as e:
        logger.warning(f"CMS lookup: load failed ({e}); blank-state fill off this run")
    n = set_cms_lookup(rows)
    logger.info(f"CMS lookup: {len(rows)} hospitals, {n} unique names")
    return n


def cms_location_for(*names):
    """(city, state) for the first name that is a unique CMS facility, trying
    the head of "Facility - Department" / "Facility, Unit" too; else None."""
    if not _CMS_LOOKUP:
        return None
    for n in names:
        n = (n or "").strip()
        if not n:
            continue
        hit = _CMS_LOOKUP.get(_cms_norm(n))
        if hit:
            return hit
        head = re.split(r"\s[-|/]\s|,", n, maxsplit=1)[0].strip()
        if head and head != n:
            hit = _CMS_LOOKUP.get(_cms_norm(head))
            if hit:
                return hit
    return None


# ══════════════════════════════════════════════════════════════════════════
#  WORKDAY
#  Format: "System Name": (tenant, wd_num, career_site_name)
#  Find these by visiting: https://tenant.wd5.myworkdayjobs.com/
# ══════════════════════════════════════════════════════════════════════════
# ── hospital_system aliases ────────────────────────────────────────────────
# Scraper-side emit names that don't exact-match the hospital_wages canonical
# names. Applied just before upserting to Supabase so the wage-join works
# without any downstream code changes.
#
# Source of truth for canonical names: hos1master.xlsx (the curated wage
# data). Verified 2026-05-26 against the wage_join_gap.txt inspection.
# Initial pass: +25,108 jobs gained wage match, 60.0% -> 90.7% coverage.
HOSPITAL_SYSTEM_ALIASES = {
    # Round 1 (2026-05-26): the 16 big ones (60.0% -> 90.7% wage coverage)
    "CommonSpirit Health":         "CommonSpirit",
    "Community Health Systems":    "CHS",
    "Intermountain Health (IMH)":  "Intermountain Healthcare",
    # Prisma runs two Workday sites (corporate + providers); merge both under
    # one board-facing system name (2026-08-04).
    "Prisma Health (Providers)":   "Prisma Health",
    "Northwell Health (CX_1)":     "Northwell Health",
    "Northwell Health (CX_3)":     "Northwell Health",
    "Ascension Health":            "Ascension",
    "Saint Luke's Health System":  "St. Luke's Health System",
    "Bon Secours Mercy":           "Bon Secours Mercy Health",
    "Memorial Healthcare System":  "Memorial Health System",
    "Vanderbilt (VUMC)":           "Vanderbilt",
    "Montefiore Health":           "Montefiore",
    "Cone Health":                 "ConeHealth",
    "Baptist Health (FL)":         "Baptist Health South Florida",
    # 2026-05-27: catch any stragglers labeled with the ambiguous "Baptist
    # Health", canonicalize them to the disambiguated KY/IN label. Applied at
    # upsert time so the next scrape cycle self-heals any legacy rows.
    # 2026-09-24: the "Billings Clinic" -> KY/IN alias is gone: Billings
    # Clinic (MT/WY) is scraped under its own name now (CSOD_ORGS), and the
    # alias would have filed every Billings row under Baptist KY/IN.
    "Baptist Health":              "Baptist Health (KY/IN)",
    "Samaritan Health NY":         "Samaritan Health",
    "Texas Health Resources":      "Texas Health",
    "Freeman Health System":       "Freeman Health",
    "Spartanburg Regional":        "Spartanburg Regional Healthcare",
    "CentraCare":                  "CentraCare Health",
    # Round 2 (2026-05-26): smaller wins after the big-ones canonicalization
    "HSHS Hospitals":              "HSHS",
    "MultiCare Health":            "MultiCare",
    "Erlanger Health System":      "Erlanger",
    "Guthrie Health":              "Guthrie",
    "Southwest Health":            "Southwest Healthcare",
    # 2026-09-10 (S-scraper-2): CityMD's two Workday entries pointed at ONE
    # site (shm.wd5 Summit_CityMD / summit_citymd) and doubled every row (608
    # under each name on 2026-09-10). The second entry is gone from
    # WORKDAY_TENANTS; this alias folds any straggler onto the CityMD row so
    # the (job_id, hospital_system) upsert key merges instead of duplicating.
    "Summit Health (CityMD)":      "CityMD",
    # Wooster's second ADP career center (Bloomington Medical Services) is the
    # hospital's physician group; one board-facing system (2026-09-10).
    "Wooster Community Hospital (BMS)": "Wooster Community Hospital",
}


################################################################################
# JOB-TYPE CLASSIFIER (added 2026-05-27)
#
# Most hospital ATSs do not expose a clean job-type field. Of 70K active
# rows snapshotted on the day this was written:
#   - 40K had job_type = NULL/empty
#   - ~6K had wage-range strings ("$31.53 - $52.24") landed there by upstream
#     scraper bugs
#   - the rest split among "full time", "part-time", "PRN", "regular", and
#     dozens of system-specific variants
#
# This classifier normalizes all of that into seven canonical buckets:
#   travel           - explicit travel-RN/nurse postings
#   per_diem         - PRN, casual, relief, on-call, variable-time
#   temporary        - temp, seasonal, interim
#   part_time        - explicit PT or <20-hour or "20-39" labels
#   full_time        - explicit FT or "regular" or "benefit-eligible"
#   resident_intern  - residency / fellowship / intern training positions
#   standard         - hospital staff positions with no signal; conventionally
#                      full-time in practice but we don't claim that without
#                      proof. ~53% of rows fall here today.
#
# The result is written to hospital_jobs.derived_job_type at upsert time.
# Mirror this function's logic if you change the SQL backfill query, or
# vice versa — they must stay in sync.
################################################################################

def derive_job_type(title, raw_job_type):
    """Classify a job into one of seven canonical buckets.

    Lowercases + substring-matches against `raw_job_type` first (the ATS-
    reported value, which may be noisy), then falls back to title keywords.
    Returns one of the seven bucket strings above; never None.
    """
    t  = (title or '').lower()
    jt = (raw_job_type or '').lower()

    # Travel — exclusive top priority since "Travel RN PRN" should not
    # match PRN first.
    if ('travel rn' in t or 'travel nurse' in t
            or 'travel allied' in t or 'travel tech' in t):
        return 'travel'

    # Per-Diem / PRN — match on either source.
    PD_KW = ('prn', 'per diem', 'per-diem', 'casual', 'relief',
             'variable time', 'on call', 'on-call', 'pool')
    if any(k in jt for k in PD_KW):
        return 'per_diem'
    if any(k in t  for k in ('prn', 'per diem', 'per-diem', 'on call')):
        return 'per_diem'

    # Temporary / Seasonal / Contract
    TEMP_KW = ('temporary', 'temp', 'seasonal', 'interim')
    if any(k in jt for k in TEMP_KW):
        return 'temporary'
    if 'temporary' in t or 'seasonal' in t:
        return 'temporary'

    # Part-Time
    PT_KW = ('part time', 'part-time', 'less than 20', '(20-39)', 'limited benefits')
    if any(k in jt for k in PT_KW) or 'part time' in t or 'part-time' in t:
        return 'part_time'

    # Full-Time / "Regular" / Benefit-Eligible
    FT_KW = ('full time', 'full-time', 'regular', 'benefit eligible',
             'benefits eligible', '40 hours', '(40 hours/week)')
    if any(k in jt for k in FT_KW) or 'full time' in t or 'full-time' in t:
        return 'full_time'

    # Residency / Fellowship / Intern training positions
    if ('resident' in t or 'fellowship' in t or ' fellow ' in t
            or 'fellow,' in t or 'intern' in t):
        return 'resident_intern'

    # Honest residual: hospital staff job with no schedule signal. The vast
    # majority of these are FT in reality, but we don't have proof from the
    # ATS, so we don't claim it. The dashboard and PDF report call this
    # bucket "Standard staff (unsignaled)" so users understand the gap.
    return 'standard'


WORKDAY_TENANTS = {
    # 2026-09-22 Texas resume: UMC Health System, Lubbock (422 beds). 131
    # postings in the dry run, bodies and dates from the detail pass; the
    # board carries no state, see SYSTEM_LOCATION_DEFAULTS / WD_FACILITY_MAP.
    "UMC Health System":         ("umchealthsystem",    "1",   "External"),
    # Teladoc Health (2026-09-10 T2 telehealth batch): the wd1 link on their
    # careers page returns HTTP 422; the live tenant is wd503, site
    # teladochealth_is_hiring (43 postings, 21 US, 8 US clinical on
    # 2026-09-09). Brand anchor, small board; the 1099 THMG network is
    # recruited off-ATS. Non-US rows are dropped by apply_employer_rules.
    "Teladoc Health":            ("teladoc",            "503", "teladochealth_is_hiring"),
    # ── Kaiser Permanente removed 2026-05-26 ──
    # The kaiserpermanente.wd5.myworkdayjobs.com tenant now returns
    # HTTP 422 for cxs POST and 500 + maintenance-page redirect for
    # the public site URL. Kaiser is on TalentBrew now (company 641
    # at www.kaiserpermanentejobs.org) but the JSON /results endpoint
    # returns 0 jobs without a working session warmup. Replaced with a
    # dedicated HTML-pagination adapter (scrape_kaiser_html) which
    # parses /search-jobs?p=N directly and works without proxies.
    # Advocate Health (Advocate Aurora + Atrium) — Workday tenant 'aah'.
    # Validated 2026-06-18: aah.wd5 / site "External" returns total ~2,000.
    "Advocate Health":           ("aah",                "5",  "External"),
    # 2026-09-24 configs: Allegheny Health Network posts on the Highmark
    # Health tenant (Pittsburgh, Natrona Heights, Erie); Beth Israel Lahey on
    # its own (facility names such as Anna Jaques and Lahey in locationsText).
    "Allegheny Health Network":  ("highmarkhealth",     "1",  "highmark"),
    "Beth Israel Lahey Health":  ("bilh",               "1",  "External"),
    # ── 2026-09-10 Texas block C (Y-texas-build): four children's tenants.
    # Site names read from each careers page's "view openings" link on
    # 2026-09-10; row counts from the same day's dry run are in
    # reports/Y-texas-build.md. Shriners is a wd12 tenant like Houston
    # Methodist; if the edge 403s aiohttp it needs the curl_cffi path.
    "Cook Children's":                  ("cookchildrens",     "1",  "Cook_Childrens_Careers"),
    "Driscoll Children's Hospital":     ("driscoll",          "1",  "DHS"),
    "Shriners Children's":              ("shrinerschildrens", "12", "Shriners"),
    "Texas Scottish Rite for Children": ("src",               "1",  "scottishriteforchildren"),
    # Providence moved off Workday to Oracle HCM (see ORACLE_ORGS). The old
    # providence.wd5 / Providence_External tenant returns 0 now. Removed 2026-06-18.
    "Banner Health":             ("bannerhealth",       "108","Careers"),
    # ── 2026-08-04 cleanup: this block of six had returned HTTP 422 on every
    # run since it was added — the tenant identifiers were guesses that never
    # validated. Audit findings, entry by entry:
    #   Northwell Health      -> NOT Workday (jobs.northwell.edu, own portal).
    #   Novant Health         -> NOT Workday (iCIMS: easyapply-novanthealth.icims.com).
    #   UC Health (Colorado)  -> NOT Workday (Radancy: careers.uchealth.org).
    #     All three removed — each needs its own non-Workday adapter (future
    #     expansion targets, Northwell alone is ~NY's largest employer).
    #   Intermountain Health  -> removed: broken DUPLICATE. The working entry
    #     is "Intermountain Health (IMH)" (imh/wd108) further down, whose rows
    #     land as "Intermountain Healthcare" via HOSPITAL_SYSTEM_ALIASES —
    #     1,354 active jobs, never actually missing.
    #   Prisma / Geisinger    -> wrong tenant coordinates; fixed below and
    #     validated live 2026-08-04 (1,632 + 585 + 1,473 jobs on the CXS API).
    "Prisma Health":             ("prismahealth",       "5",  "PrismaHealthCorporate"),
    "Prisma Health (Providers)": ("prismahealth",       "5",  "PrismaHealthProviders"),
    "Geisinger":                 ("geisinger",          "5",  "GeisingerExternal"),
    # Summit BHC — behavioral health, ~35 facilities (2026-08-04 psych-gap
    # sweep; the CMS analysis put psychiatric coverage at 41.7%). Tenant from
    # summitbhc.com/careers, validated live: total=293.
    "Summit BHC":                ("summitbhc",          "1",  "Summit_BHC"),
    # 2026-05-29: old tenant (sanfordhealth/Sanford_Health) now returns HTTP 422 —
    # dead. Live tenant is sanford.wd5/SanfordHealth (total=2000, Fargo/Sioux Falls/
    # Mandan geography). This is the full Sanford system INCLUDING Good Samaritan
    # Society (senior living/SNF), so no separate Good Sam entry is needed.
    "Sanford Health":            ("sanford",            "5",  "SanfordHealth"),
    # SSM Health, Mercy Health, Henry Ford removed 2026-08-28: dead tenants;
    # re-homed (SSM -> Phenom; Mercy -> Bon Secours Mercy Health Phenom;
    # Henry Ford -> SmartRecruiters). See the resurrection block below.
    "Carilion Clinic":           ("carilionclinic",     "12", "External_Careers"),  # 732, validated 2026-08-28
    # DaVita Workday entry removed 2026-08-28: HTTP 404 (dead tenant); the
    # 3,441 banked DaVita rows flow from PHENOM_ORGS (careers.davita.com).
    # ── 2026-08-28 non-acute Bucket A (scraper-audit expansion). Every tenant
    # below validated live with a PLAIN client this session (the wd12 pair
    # too — Houston Methodist's 403 was tenant-specific, not a wd12 rule):
    # Duly 364 · LifeStance 113 · CityMD 581 · SummitPhysicians 128 ·
    # Fresenius 2000-capped (global tenant; check first run for non-US rows) ·
    # Sunrise 1,928 · GoHealth 489.
    "Duly Health and Care":      ("dulyhealthandcare",  "1",  "Duly"),
    "LifeStance Health":         ("lifestance",         "5",  "Careers"),
    # "Summit Health (CityMD)" (shm.wd5 Summit_CityMD) removed 2026-09-10
    # (S-scraper-2): same site as the "CityMD" entry below (summit_citymd),
    # every row landed twice. sql/16_scraper_dedupe_cleanup.sql retires the
    # 608 rows it left under the old name.
    "Summit Health (Physicians)":("shm",                "5",  "SummitHealthPhysicians"),
    "Fresenius Medical Care":    ("freseniusmedicalcare","3", "fme"),
    "Sunrise Senior Living":     ("sunriseseniorliving","12", "SUNRISE_EXT_CAREERS"),
    "GoHealth Urgent Care":      ("gohealthuc",         "12", "External"),
    # Houston Methodist — REMOVED 2026-07-28. Tenant moved wd1 -> wd12 and the
    # external site is now "GTI"; the old wd1/HoustonMethodist_External CXS
    # returns HTTP 422 (this adapter never wrote a single row). Workday's wd12
    # edge also 403s non-browser TLS, so it now has a dedicated curl_cffi
    # adapter: run_houston_methodist().
    # ── 2026-08-28 DARK-SYSTEMS RESURRECTION (scraper-audit expansion) ──────
    # The completeness probe found this whole block returning HTTP 422/404
    # with ZERO rows banked — tenant coordinates guessed long ago and never
    # validated, silently dead for months. Every entry below was re-discovered
    # and validated live this session (CXS total in the trailing comment).
    # Systems that MIGRATED PLATFORMS are removed here and re-homed:
    #   SSM Health -> Phenom (PHENOM_ORGS, jobs.ssmhealth.com, ~1,616)
    #   Mercy Health / Bon Secours -> consolidated Bon Secours Mercy Health
    #     (Phenom careers.bsmhealth.org, already banking ~2,034)
    #   Henry Ford Health -> SmartRecruiters (SMARTRECRUITERS_ORGS, ~1,689)
    #   Indiana University Health -> Oracle (ORACLE_ORGS ekcm/CX, ~1,069)
    #   Inova Health -> Oracle (ORACLE_ORGS elar/CX_1, ~682)
    #   WellSpan Health -> Oracle (ORACLE_ORGS fa-evzu/CX_1, ~1,049)
    #   Fairview / OSF / WakeMed -> Jibe (JIBE_SITES, ~1,262/1,411/565)
    #   Hackensack Meridian -> TalentBrew (TALENTBREW_ORGS, ~1,665)
    #   Adventist Health -> Oracle: the existing ecvz/CX_1 entry mislabeled
    #     "Cape Cod Healthcare" IS Adventist (1,421 banked rows sit in
    #     CA/HI/OR); renamed in ORACLE_ORGS + needs a one-shot DB relabel.
    #   Dignity Health -> no standalone board; it is the industry facet of
    #     the CommonSpirit TalentBrew scrape (already banking ~5,494).
    #   CommonSpirit Workday tenant dead -> covered by TalentBrew adapter.
    #   Piedmont -> classic iCIMS (careers-piedmont.icims.com, ~1,810);
    #   UnityPoint -> LiquidCompass API (~1,573); UTSW -> Taleo REST (~574);
    #   McLaren -> SelectMinds HTML (~1,094); UNC -> Talemetry jobs.json
    #     (~2,007, needs firefox TLS); MaineHealth -> Talemetry jobs.json
    #     (~1,107, firefox TLS); RWJBarnabas -> symplr JobseekerSearchAPI
    #     (~2,073). These six need adapter work: wave 3, see the audit doc.
    "NewYork-Presbyterian":      ("nyp",                "1",  "nypcareers"),               # 385
    "Ochsner Health":            ("ochsner",            "1",  "Ochsner"),                  # 1,903
    "Parkland Health":           ("parklandhospital",   "12", "Parkland_Careers"),         # 349
    "Sharp HealthCare":          ("sharp",              "1",  "External"),
    "Sutter Health":             ("sutterhealth",       "1",  "SH"),                       # 1,206
    "VCU Health":                ("vcuhealth",          "1",  "VCUHealth_careers"),        # 386
    "Wellstar Health":           ("wellstar",           "1",  "wellstarcareers"),          # 816
    "Wellstar Health (Providers)": ("wellstar",         "1",  "wellstarprovidercareers"),  # 199
    "Memorial Hermann":          ("memorialhermann",    "5",  "External"),                 # 598
    "OhioHealth":                ("ohiohealth",         "5",  "OhioHealthJobs"),           # 1,034
    "Tufts Medicine":            ("tuftsmedicine",      "1",  "Jobs"),                     # 490
    "Virtua Health":             ("virtua",             "1",  "Virtua_Health_External_Career_Site"), # 739
    "Essentia Health":           ("essentiahealth",     "1",  "Essentia_Health"),          # 791
    # ── Confirmed from direct URL verification ──
    "BestCare Health":           ("bestcare", "1", "bestcare"),
    "Bronson Healthcare":        ("bronsonhg", "1", "newhires"),
    # ── Added from scraper1.xlsx confirmed URLs ──
    "Albany Med":                ("albanymed", "5", "Albany_Med"),
    "Allina Health":             ("allina", "5", "External"),
    "Avera Health":              ("avera", "5", "avera-careers"),
    # 2026-05-27 CORRECTION: the "bhs" tenant is Baptist Health (KY/IN) —
    # the Louisville-based system, NOT Billings Clinic (Montana). All 1,079
    # jobs at this tenant were being mislabeled as Billings Clinic — URLs
    # like bhs.wd1.myworkdayjobs.com plus titles like "Director, Baptist
    # Health Medical Group" + KY/IN locations confirm. Real Billings Clinic
    # does not appear to be on Workday.
    #
    # 2026-05-27 (updated): use explicit "(KY/IN)" region tag to disambiguate
    # from the FOUR other Baptist Health systems in the dossier:
    #   - Baptist Health (KY/IN)       — Louisville, this entry
    #   - Baptist Health South Florida — Miami area, via Phenom
    #   - Baptist Health System (TX)   — San Antonio, via Taleo
    #   - Valley Baptist Health System — Rio Grande Valley TX, via Taleo
    #   - Baptist Memorial (TN)        — Memphis (not yet scraped)
    # Same system is also scraped via Phenom at jobs.baptisthealthcareers.com
    # (entry below in PHENOM_ORGS uses identical name; upsert dedups on
    # (job_id, hospital_system) so cross-scraper overlap collapses cleanly).
    "Baptist Health (KY/IN)":    ("bhs", "1", "careers"),
    "Bozeman Health":            ("bozemanhealth", "1", "BozemanHealthCareers"),
    "Broadlawns Medical Center": ("broadlawns",            "501","Broadlawns_Careers"),
    "Cape Fear Valley Health":   ("capefearvalley", "1", "CFV"),
    "Capital Health":            ("capitalhealth", "1", "CapitalHealthCareers"),
    "Enloe Health":              ("enloe", "12", "EnloeHealth"),
    "Freeman Health System":     ("freemanhealth", "1", "jointeamfreeman"),
    "Great River Health":        ("greatriverhealth", "5", "External"),
    "Halifax Health":            ("halifaxhealth", "12", "HalifaxHealth"),
    "Healogics":                 ("healogics", "5", "healogics"),
    "Hendricks Regional Health": ("hendricks", "1", "Hendricks_External_Career_Site"),
    "Houston Healthcare":        ("hhc", "5", "HHC"),  # Corrected from "Hartford HealthCare" — tenant 'hhc' is Houston Healthcare (GA); Hartford uses Phenom scraper below
    "HRHS":                      ("hrhs", "1", "Careers"),
    "HSHS Hospitals":            ("hshs", "1", "hshscareers"),
    "Intermountain Health (IMH)":("imh",                   "108","IntermountainCareers"),
    "Jefferson Health":          ("jeffersonhealth", "5", "ThomasJeffersonExternal"),
    "John Muir Health":          ("jmh", "5", "JohnMuirHealthCareers"),
    "Jupiter Medical Center":    ("jupitermed", "1", "External"),
    "Kaweah Health":             ("kaweahhealth", "1", "Careers"),
    "LMH Health":                ("lmh", "1", "LMHjobs"),
    "Logan Health":              ("loganhealth", "1", "Logan_Careers"),
    "Maine General Health":      ("mainegeneral", "5", "MaineGeneralCareers"),
    "Mary Washington Healthcare":("marywashingtonhealthcare","5","Externalcareers"),
    "Mass General Brigham":      ("massgeneralbrigham", "1", "MGBExternal"),
    "Memorial Healthcare System":("memorialhealthcare", "1", "MHS_Careers"),
    "Methodist Le Bonheur":      ("methodisthealth", "5", "MLH"),
    "Methodist Health System TX":("methodisthealthsystem", "1", "MHS_Careers"),
    "Montefiore Health":         ("montefiore", "12", "MMC"),
    "Monument Health":           ("monumenthealth", "1", "Goldcareers"),
    "MultiCare Health":          ("multicare", "1", "multicare"),
    "Northeast Georgia Health":  ("nghs", "1", "External"),
    "Endeavor Health":           ("nshs", "1", "ns-eeh"),  # Renamed: NorthShore + Edward-Elmhurst merged into Endeavor Health (2024). Old key was "North Shore Health System".
    # 2026-09-16 (NY coverage): this tenant is United Health Services of
    # Binghamton (rows: Johnson City, Binghamton, Norwich, Walton), not NYU
    # Langone, which runs SilkRoad OpenHire (no adapter yet). Relabelled; the
    # 405 rows under the old name deactivate as the new ones land.
    "United Health Services":    ("nyuhs", "12", "nyuhscareers1"),
    # 2026-09-16 (NY coverage): fingerprinted live from the careers pages.
    "Rochester Regional Health": ("rrhs", "5", "RRH"),
    "Richmond University Medical Center": ("rumcsi", "5", "RUMC"),
    # 2026-09-17 (coverage lever 3): fingerprinted from the careers sites and
    # probed live the same day (counts = Workday "total"; 2000 = the window,
    # recovered by the facet slicing below).
    "Sentara Healthcare":        ("sentara", "1", "scs"),                          # 2000+, VA/NC
    "St. Luke's University Health Network": ("sluhn", "1", "SLUHN"),               # 964, PA/NJ
    "Memorial Healthcare System": ("memorialhealthcare", "1", "mhs_careers"),      # 568, Hollywood FL
    "HonorHealth":               ("honorhealth", "12", "honorhealth_careers"),     # 521, AZ
    "CoxHealth":                 ("coxhealth", "5", "coxhealth_external"),         # 491, Springfield MO
    "The University of Kansas Health System": ("kansashealthsystem", "1", "careers"),  # 1,042
    "Saint Luke's Health System": ("saintlukes", "1", "saintlukeshealthcareers"),  # 565, Kansas City
    "Stanford Health Care":      ("stanfordmedicine", "115", "SHC_External_Career_Site"),  # 391
    "Owensboro Health":          ("owensborohealth", "1", "owensborohealth"),
    "Phelps Health":             ("phelpshealth", "5", "Phelps"),
    "Pullman Regional Hospital": ("pullmanregionalhospital","1", "Careers"),
    "Riverside Health System":   ("rivhs", "1", "Non-ProviderRHS"),
    "University of Rochester":   ("rochester", "5", "UR_Staff"),
    # wd115 is the real shard (unusual high number); wd1 422'd forever.
    "Saint Francis Health":      ("saintfrancis", "115", "External"),  # 536, validated 2026-08-28
    "Saint Luke's Health System":("saintlukes", "1", "saintlukeshealthcareers"),
    "Salinas Valley Health":     ("salinasvalleyhealth", "5", "SalinasValleyHealth"),
    "Samaritan Health NY":       ("samaritanhealth", "12", "shsny"),
    "Sarah Bush Lincoln Health": ("sarahbush", "1", "SarahBush"),
    "St. Francis Medical Center":("sfmc", "1", "SFHS"),
    "Southern Illinois Health":  ("sih", "5", "SIH_External"),
    "Silver Cross Hospital":     ("silvercross", "5", "SilverCrossCareers"),
    "Stormont Vail Health":      ("stormontvail", "1", "SVH"),
    "Sturdy Memorial Hospital":  ("sturdymemorial", "5", "Sturdy"),
    "Tidelands Health":          ("tidelandshealth", "12", "Tidelands"),
    "UMass Memorial Health":     ("ummh", "1", "Careers"),
    "UofL Health":               ("uoflhealth", "1", "UofLHealthCareers"),
    "Vanderbilt (VUMC)":         ("vumc", "1", "vumccareers"),
    "West Tennessee Healthcare": ("wth",                   "501","WTH"),  # Corrected: Wheaton Franciscan ceased to exist in 2016 (acquired by Ascension); tenant 'wth' is West Tennessee Healthcare in Jackson, TN
    "WVU Medicine":              ("wvumedicine", "1", "WVUH"),
    # ── Added from scraper1.xlsx expansion ──
    "UW Medicine":               ("uw", "5", "UWHires"),
    # ── Added 2026-05-06: acute-care expansion (verified via careers-page HTML) ──
    "Trinity Health":            ("trinityhealth", "1", "Jobs"),
    "Cleveland Clinic":          ("ccf",           "1", "ClevelandClinicCareers"),
    # ── Added 2026-05-13: post-acute expansion Phase 1 (verified via careers-page HTML) ──
    # Fresenius Medical Care: ~2,500 US dialysis centers. Largest US dialysis operator
    # alongside DaVita. Confirmed via redirect from jobs.fmcna.com → careers homepage
    # → Workday "Returning Applicants" link points to wd3 tenant.
    "Fresenius Medical Care":    ("freseniusmedicalcare", "3", "fme"),
    # ProMedica: 10 hospitals across 9 states (Toledo, OH HQ). Workday confirmed via
    # web search — promedica.wd12.myworkdayjobs.com/External_Careers shows 439 jobs.
    "ProMedica":                 ("promedica", "12", "External_Careers"),
    # ── Added 2026-05-26: Phase 2 non-acute expansion (verified Workday cxs 200) ──
    # GoHealth Urgent Care: ~250 urgent care centers across the US.
    # Endpoint validated 2026-05-26: gohealthuc.wd12/external returns total=412.
    "GoHealth Urgent Care":      ("gohealthuc", "12", "external"),
    # CityMD (Summit Health parent SHM): ~150 urgent care centers in NY/NJ/CT.
    # Endpoint validated 2026-05-26: shm.wd5/summit_citymd returns total=548.
    "CityMD":                    ("shm", "5", "summit_citymd"),
    # Compassus: hospice + palliative care + home health across ~200 locations.
    # Endpoint validated 2026-05-26: hospicecom.wd5/Compassus returns total=1076.
    "Compassus":                 ("hospicecom", "5", "Compassus"),
    # ── Added 2026-05-29: Phase 3 non-acute expansion (verified Workday cxs 200) ──
    # All endpoints validated live 2026-05-29 via probe_ats.py (Origin/Referer
    # headers required; reads data["total"]).
    # Elara Caring: home health + hospice + personal care, ~200 locations.
    "Elara Caring":              ("elara", "5", "External"),            # 937 jobs, home-health
    # Option Care Health: nation's largest independent home/alternate-site infusion provider.
    "Option Care Health":        ("optioncare", "1", "OptionCare"),     # 292 jobs, home-infusion
    # LifeStance Health: ~700 outpatient mental-health centers.
    "LifeStance Health":         ("lifestance", "5", "Careers"),        # 152 jobs, behavioral
    # WellNow Urgent Care: ~190 urgent care centers (shares Aspen Dental's Workday tenant).
    "WellNow Urgent Care":       ("aspendental", "1", "WellNowUrgentCareCareers"),  # 186 jobs, urgent-care
    # SimonMed Imaging: ~170 outpatient imaging centers.
    "SimonMed Imaging":          ("sim", "3", "External"),              # 98 jobs, imaging
    # Akumin: outpatient imaging + oncology, ~130 centers.
    "Akumin":                    ("akumincorp", "5", "akumincareers"),  # 260 jobs, imaging
}

# Generic fallback site names to try when the specific one fails
CAREER_SITE_FALLBACKS = [
    "External_Career_Site",
    "External",
    "Careers",
    "careers",
    "ExternalCareers",
    "External_Careers",
]



##############################################################################
#  LOCATION LOOKUP TABLES
#  Two-tier fallback applied in normalize_job() when city/state is blank
#  or unparseable from the ATS response.
#
#  Tier 1 — FACILITY_LOCATION_MAP: specific hospital/campus name → (city, state)
#  Tier 2 — SYSTEM_LOCATION_DEFAULTS: hospital system → (city, state)
#            Used when the specific facility isn't in Tier 1.
##############################################################################

FACILITY_LOCATION_MAP: dict[str, tuple[str, str]] = {
    # ── Memorial Hermann ──────────────────────────────────────────────────
    "memorial hermann texas medical center": ("Houston", "TX"),
    "memorial hermann memorial city medical center": ("Houston", "TX"),
    "memorial hermann greater heights hospital": ("Houston", "TX"),
    "memorial hermann southwest hospital": ("Houston", "TX"),
    "memorial hermann southeast hospital": ("Houston", "TX"),
    "memorial hermann sugar land hospital": ("Sugar Land", "TX"),
    "memorial hermann pearland hospital": ("Pearland", "TX"),
    "memorial hermann katy hospital": ("Katy", "TX"),
    "memorial hermann northeast hospital": ("Humble", "TX"),
    "memorial hermann the woodlands medical center": ("The Woodlands", "TX"),
    "memorial hermann rehabilitation hospital - katy": ("Katy", "TX"),
    "memorial hermann surgical hospital": ("Houston", "TX"),
    "tirr memorial hermann": ("Houston", "TX"),
    "memorial hermann medical group": ("Houston", "TX"),
    "memorial hermann": ("Houston", "TX"),
    # ── CHRISTUS Health ───────────────────────────────────────────────────
    "christus system office": ("Irving", "TX"),
    "christus ministry system office": ("Irving", "TX"),
    "christus health ark-la-tex": ("Texarkana", "TX"),
    "christus spohn health system": ("Corpus Christi", "TX"),
    "christus spohn hospital corpus christi - shoreline": ("Corpus Christi", "TX"),
    "christus spohn hospital corpus christi - south": ("Corpus Christi", "TX"),
    "christus spohn hospital alice": ("Alice", "TX"),
    "christus spohn hospital beeville": ("Beeville", "TX"),
    "christus spohn hospital kleberg": ("Kingsville", "TX"),
    "christus spohn hospital kenedy": ("Kenedy", "TX"),
    "christus good shepherd health system": ("Longview", "TX"),
    "christus good shepherd medical center - longview": ("Longview", "TX"),
    "christus good shepherd medical center - marshall": ("Marshall", "TX"),
    "christus mother frances hospital - tyler": ("Tyler", "TX"),
    "christus mother frances hospital - jacksonville": ("Jacksonville", "TX"),
    "christus mother frances hospital - winnsboro": ("Winnsboro", "TX"),
    "christus mother frances hospital - sulphur springs": ("Sulphur Springs", "TX"),
    "christus southeast texas health system": ("Beaumont", "TX"),
    "christus southeast texas - st. elizabeth": ("Beaumont", "TX"),
    "christus southeast texas - jasper memorial": ("Jasper", "TX"),
    "christus santa rosa health system": ("San Antonio", "TX"),
    "christus santa rosa hospital - medical center": ("San Antonio", "TX"),
    "christus santa rosa hospital - alamo heights": ("San Antonio", "TX"),
    "christus santa rosa hospital - new braunfels": ("New Braunfels", "TX"),
    "christus santa rosa hospital - westover hills": ("San Antonio", "TX"),
    "christus santa rosa hospital - kyle": ("Kyle", "TX"),
    "christus trinity mother frances": ("Tyler", "TX"),
    "christus muguerza": ("Monterrey", "TX"),
    "christus health shreveport-bossier": ("Shreveport", "LA"),
    "christus health shreveport": ("Shreveport", "LA"),
    "christus schumpert health system": ("Shreveport", "LA"),
    "christus dubuis hospital": ("Houston", "TX"),
    "christus st. vincent regional medical center": ("Santa Fe", "NM"),
    "christus st. vincent": ("Santa Fe", "NM"),
    "christus highlands medical center": ("Sulphur Springs", "TX"),
    "christus continuing care": ("Irving", "TX"),
    "christus children's": ("San Antonio", "TX"),
    "christus children's hospital": ("San Antonio", "TX"),
    # ── Houston Methodist ─────────────────────────────────────────────────
    "houston methodist hospital": ("Houston", "TX"),
    "houston methodist san jacinto hospital": ("Baytown", "TX"),
    "houston methodist west hospital": ("Houston", "TX"),
    "houston methodist willowbrook hospital": ("Houston", "TX"),
    "houston methodist sugar land hospital": ("Sugar Land", "TX"),
    "houston methodist st. john hospital": ("Nassau Bay", "TX"),
    "houston methodist clear lake hospital": ("Nassau Bay", "TX"),
    "houston methodist baytown hospital": ("Baytown", "TX"),
    "houston methodist the woodlands hospital": ("The Woodlands", "TX"),
    # ── Baylor Scott & White ──────────────────────────────────────────────
    "baylor university medical center": ("Dallas", "TX"),
    "baylor scott & white medical center - temple": ("Temple", "TX"),
    "baylor scott & white medical center - waco": ("Waco", "TX"),
    "baylor scott & white medical center - round rock": ("Round Rock", "TX"),
    "baylor scott & white medical center - mckinney": ("McKinney", "TX"),
    "baylor scott & white medical center - plano": ("Plano", "TX"),
    "baylor scott & white all saints medical center": ("Fort Worth", "TX"),
    "baylor scott & white medical center - irving": ("Irving", "TX"),
    "baylor scott & white medical center - hillcrest": ("Waco", "TX"),
    # ── Cleveland Clinic ──────────────────────────────────────────────────
    "cleveland clinic main campus": ("Cleveland", "OH"),
    "cleveland clinic akron general": ("Akron", "OH"),
    "cleveland clinic florida": ("Weston", "FL"),
    "cleveland clinic abu dhabi": ("Abu Dhabi", "AE"),
    "cleveland clinic london": ("London", ""),
    "cleveland clinic avon hospital": ("Avon", "OH"),
    "cleveland clinic marymount hospital": ("Garfield Heights", "OH"),
    "cleveland clinic hillcrest hospital": ("Mayfield Heights", "OH"),
    "cleveland clinic fairview hospital": ("Cleveland", "OH"),
    "cleveland clinic medina hospital": ("Medina", "OH"),
    "cleveland clinic union hospital": ("Dover", "OH"),
    # ── Mayo Clinic ───────────────────────────────────────────────────────
    "mayo clinic - rochester": ("Rochester", "MN"),
    "mayo clinic rochester": ("Rochester", "MN"),
    "mayo clinic - phoenix": ("Phoenix", "AZ"),
    "mayo clinic - scottsdale": ("Scottsdale", "AZ"),
    "mayo clinic - jacksonville": ("Jacksonville", "FL"),
    "mayo clinic florida": ("Jacksonville", "FL"),
    "mayo clinic arizona": ("Phoenix", "AZ"),
    "mayo clinic health system": ("Rochester", "MN"),
    # ── HCA Healthcare ────────────────────────────────────────────────────
    "hca houston healthcare": ("Houston", "TX"),
    "hca florida": ("Nashville", "TN"),
    "las vegas": ("Las Vegas", "NV"),
    # ── Parkland Health ───────────────────────────────────────────────────
    "parkland memorial hospital": ("Dallas", "TX"),
    "parkland health": ("Dallas", "TX"),
    # ── UT Southwestern ───────────────────────────────────────────────────
    "ut southwestern medical center": ("Dallas", "TX"),
    "university of texas southwestern medical center": ("Dallas", "TX"),
    # ── Montefiore ────────────────────────────────────────────────────────
    "montefiore medical center": ("Bronx", "NY"),
    "montefiore einstein": ("Bronx", "NY"),
    "montefiore nyack": ("Nyack", "NY"),
    "montefiore new rochelle": ("New Rochelle", "NY"),
    "montefiore mount vernon": ("Mount Vernon", "NY"),
    # ── NewYork-Presbyterian ──────────────────────────────────────────────
    "newyork-presbyterian hospital": ("New York", "NY"),
    "newyork-presbyterian/weill cornell": ("New York", "NY"),
    "newyork-presbyterian/columbia": ("New York", "NY"),
    "newyork-presbyterian brooklyn methodist": ("Brooklyn", "NY"),
    "newyork-presbyterian queens": ("Flushing", "NY"),
    "newyork-presbyterian lower manhattan": ("New York", "NY"),
    "newyork-presbyterian hudson valley": ("Cortlandt Manor", "NY"),
    # ── Thomas Jefferson / Jefferson Health ───────────────────────────────
    "thomas jefferson university hospital": ("Philadelphia", "PA"),
    "jefferson hospital": ("Philadelphia", "PA"),
    "jefferson cherry hill hospital": ("Cherry Hill", "NJ"),
    "jefferson stratford hospital": ("Stratford", "NJ"),
    "jefferson abington hospital": ("Abington", "PA"),
    "jefferson torresdale hospital": ("Philadelphia", "PA"),
    # ── Mass General Brigham ──────────────────────────────────────────────
    "massachusetts general hospital": ("Boston", "MA"),
    "brigham and women's hospital": ("Boston", "MA"),
    "newton-wellesley hospital": ("Newton", "MA"),
    "north shore medical center": ("Salem", "MA"),
    "mclean hospital": ("Belmont", "MA"),
    "spaulding rehabilitation": ("Boston", "MA"),
    "martha's vineyard hospital": ("Oak Bluffs", "MA"),
    "nantucket cottage hospital": ("Nantucket", "MA"),
    "faulkner hospital": ("Boston", "MA"),
    # ── Vanderbilt Health ─────────────────────────────────────────────────
    "vanderbilt university medical center": ("Nashville", "TN"),
    "vanderbilt wilson county hospital": ("Lebanon", "TN"),
    "vanderbilt health one hundred oaks": ("Nashville", "TN"),
    # ── Ochsner Health ────────────────────────────────────────────────────
    "ochsner medical center": ("New Orleans", "LA"),
    "ochsner medical center - west bank": ("Gretna", "LA"),
    "ochsner medical center - kenner": ("Kenner", "LA"),
    "ochsner medical center - north shore": ("Slidell", "LA"),
    "ochsner medical center - baton rouge": ("Baton Rouge", "LA"),
    "ochsner lafayette general": ("Lafayette", "LA"),
    "ochsner medical center - shreveport": ("Shreveport", "LA"),
    # ── UNC Health ────────────────────────────────────────────────────────
    "unc hospitals": ("Chapel Hill", "NC"),
    "unc rex healthcare": ("Raleigh", "NC"),
    "unc nash health care": ("Rocky Mount", "NC"),
    "unc lenoir health care": ("Kinston", "NC"),
    "chatham hospital": ("Siler City", "NC"),
    "caldwell memorial hospital": ("Lenoir", "NC"),
    # ── Intermountain Healthcare ──────────────────────────────────────────
    "intermountain medical center": ("Murray", "UT"),
    "primary children's hospital": ("Salt Lake City", "UT"),
    "ldsh hospital": ("Salt Lake City", "UT"),
    "lds hospital": ("Salt Lake City", "UT"),
    "intermountain health": ("Salt Lake City", "UT"),
    # ── Additional single-city systems ────────────────────────────────────
    "university of texas medical branch": ("Galveston", "TX"),
    "utmb health": ("Galveston", "TX"),
    "harris health system": ("Houston", "TX"),
    "ben taub hospital": ("Houston", "TX"),
    "lww": ("Houston", "TX"),
}

# Normalize all keys to lowercase for matching
FACILITY_LOCATION_MAP = {k.lower(): v for k, v in FACILITY_LOCATION_MAP.items()}

# 2026-09-17 (blank states): systems whose board writes a bare city with no
# state, where the system default would be wrong for part of the rows
# (ProMedica is Toledo OH but Monroe / Adrian are MI; Legacy is Portland OR but
# Vancouver is WA; Saint Luke's KC straddles the state line). Checked before
# SYSTEM_LOCATION_DEFAULTS; keys are lowercased system and city.
SYSTEM_CITY_STATE: dict[str, dict[str, str]] = {
    "promedica": {"monroe": "MI", "adrian": "MI", "tecumseh": "MI", "lambertville": "MI",
                  "coldwater": "MI", "hillsdale": "MI", "dundee": "MI"},
    "legacy health": {"vancouver": "WA"},
    "st. luke's health system": {"overland park": "KS", "iola": "KS", "lenexa": "KS",
                                 "shawnee": "KS", "leawood": "KS", "olathe": "KS"},
}

# System-level fallback — used when facility lookup fails
# Multi-state systems use primary HQ market as default
SYSTEM_LOCATION_DEFAULTS: dict[str, tuple[str, str]] = {
    # 2026-09-22 Texas resume: UMC Lubbock's Workday board writes only the
    # campus ("UMC Main Campus", "Health & Wellness Hospital"); every site is in Lubbock.
    "umc health system":          ("Lubbock",          "TX"),
    "university health (san antonio)": ("San Antonio", "TX"),   # 2026-09-22: TalentBrew cards carry no location
    # 2026-09-24: UTHealth Houston's Phenom board writes "Texas Medical
    # Center-Houston" (blanked by clean_city) or just "Texas" on about 70%
    # of its 452 rows; the campus is in Houston.
    "uthealth houston":           ("Houston",           "TX"),
    # 2026-09-24: BILH's corporate and multi-site rows ("2 Locations",
    # "Beth Israel Lahey Health") after WD_FACILITY_MAP and the tenant default.
    "beth israel lahey health":   ("Boston",            "MA"),
    # 2026-09-17 (blank states): single-market systems whose boards carry no
    # location at all (iCIMS card lists at Covenant / OHSU, Workday tenants
    # with facility names). Both the adapter label and the canonical label.
    "wvu medicine":               ("Morgantown",        "WV"),
    "multicare":                  ("Tacoma",            "WA"),
    "multicare health":           ("Tacoma",            "WA"),
    "covenant health":            ("Knoxville",         "TN"),
    "ohsu":                       ("Portland",          "OR"),
    "kettering health":           ("Kettering",         "OH"),
    "legacy health":              ("Portland",          "OR"),
    "promedica":                  ("Toledo",            "OH"),
    "university of rochester":    ("Rochester",         "NY"),
    "saint francis health":       ("Tulsa",             "OK"),
    "freeman health":             ("Joplin",            "MO"),
    "freeman health system":      ("Joplin",            "MO"),
    "samaritan health":           ("Watertown",         "NY"),
    "samaritan health ny":        ("Watertown",         "NY"),
    "duly health and care":       ("Downers Grove",     "IL"),
    "wellstar health (providers)": ("Marietta",         "GA"),
    "st. luke's health system":   ("Kansas City",       "MO"),
    # 2026-09-17 (coverage lever 3): single-market systems added the same day.
    "trihealth":                  ("Cincinnati",        "OH"),
    "uc health":                  ("Cincinnati",        "OH"),
    "loma linda university health": ("Loma Linda",      "CA"),
    "uw health":                  ("Madison",           "WI"),
    "baptist memorial health care": ("Memphis",         "TN"),
    "franciscan missionaries of our lady health system": ("Baton Rouge", "LA"),
    "honorhealth":                ("Scottsdale",        "AZ"),
    "stanford health care":       ("Palo Alto",         "CA"),
    "memorial healthcare system": ("Hollywood",         "FL"),
    "renown health":              ("Reno",              "NV"),
    "norton healthcare":          ("Louisville",        "KY"),
    "tower health":               ("West Reading",      "PA"),
    "orlando health":             ("Orlando",           "FL"),
    "sarasota memorial health care system": ("Sarasota", "FL"),
    "lcmc health":                ("New Orleans",       "LA"),
    "parkview health":            ("Fort Wayne",        "IN"),
    "coxhealth":                  ("Springfield",       "MO"),
    "saint luke's health system": ("Kansas City",       "MO"),
    "the university of kansas health system": ("Kansas City", "KS"),
    "st. luke's university health network": ("Bethlehem", "PA"),
    "sentara healthcare":         ("Norfolk",           "VA"),
    "lee health":                 ("Fort Myers",        "FL"),
    "baycare":                    ("Clearwater",        "FL"),
    "penn state health":          ("Hershey",           "PA"),
    "aspirus health":             ("Wausau",            "WI"),
    # Workday tenants
    "kaiser permanente":          ("Oakland",          "CA"),
    "providence health":          ("Renton",           "WA"),
    "banner health":              ("Phoenix",           "AZ"),
    "northwell health":           ("New Hyde Park",     "NY"),
    "intermountain health":       ("Salt Lake City",    "UT"),
    "intermountain healthcare":   ("Salt Lake City",    "UT"),
    "uc health (colorado)":       ("Aurora",            "CO"),
    "novant health":              ("Winston-Salem",     "NC"),
    "prisma health":              ("Greenville",        "SC"),
    "geisinger":                  ("Danville",          "PA"),
    "sanford health":             ("Sioux Falls",       "SD"),
    "ssm health":                 ("St. Louis",         "MO"),
    "mercy health":               ("Chesterfield",      "MO"),
    "carilion clinic":            ("Roanoke",           "VA"),
    "davita":                     ("Denver",            "CO"),
    "henry ford health":          ("Detroit",           "MI"),
    "houston methodist":          ("Houston",           "TX"),
    "indiana university health":  ("Indianapolis",      "IN"),
    "inova health":               ("Falls Church",      "VA"),
    "newyork-presbyterian":       ("New York",          "NY"),
    "ochsner health":             ("New Orleans",       "LA"),
    "parkland health":            ("Dallas",            "TX"),
    "piedmont healthcare":        ("Atlanta",           "GA"),
    "rwjbarnabas health":         ("West Orange",       "NJ"),
    "sharp healthcare":           ("San Diego",         "CA"),
    "sutter health":              ("Sacramento",        "CA"),
    "unc health":                 ("Chapel Hill",       "NC"),
    "unitypoint health":          ("West Des Moines",   "IA"),
    "ut southwestern medical":    ("Dallas",            "TX"),
    "vcu health":                 ("Richmond",          "VA"),
    "wakemed":                    ("Raleigh",           "NC"),
    "wellstar health":            ("Marietta",          "GA"),
    "memorial hermann":           ("Houston",           "TX"),
    "ohiohealth":                 ("Columbus",          "OH"),
    "wellspan health":            ("York",              "PA"),
    "hackensack meridian":        ("Edison",            "NJ"),
    "mainehealth":                ("Portland",          "ME"),
    "mclaren health care":        ("Grand Blanc",       "MI"),
    "osf healthcare":             ("Peoria",            "IL"),
    "tufts medicine":             ("Boston",            "MA"),
    "virtua health":              ("Marlton",           "NJ"),
    "adventist health":           ("Roseville",         "CA"),
    "dignity health":             ("San Francisco",     "CA"),
    "bon secours":                ("Richmond",          "VA"),
    "essentia health":             ("Duluth",            "MN"),
    "fairview health":            ("Minneapolis",       "MN"),
    "bestcare health":            ("Bend",              "OR"),
    "bronson healthcare":         ("Kalamazoo",         "MI"),
    "albany med health system":   ("Albany",            "NY"),
    "allina health":              ("Minneapolis",       "MN"),
    "avera":                      ("Sioux Falls",       "SD"),
    "bjc healthcare":             ("St. Louis",         "MO"),
    "baptist health":             ("Louisville",        "KY"),
    "cape fear valley health":    ("Fayetteville",      "NC"),
    "capital health":             ("Pennington",        "NJ"),
    "endeavor health":            ("Evanston",          "IL"),
    "freeman health":             ("Joplin",            "MO"),
    "great river health":         ("West Burlington",   "IA"),
    "hshs":                       ("Springfield",       "IL"),
    "halifax health":             ("Daytona Beach",     "FL"),
    "healogics":                  ("Jacksonville",      "FL"),
    "houston healthcare":         ("Warner Robins",     "GA"),
    "jefferson health":           ("Philadelphia",      "PA"),
    "john muir health":           ("Walnut Creek",      "CA"),
    "logan health":               ("Kalispell",         "MT"),
    "mainegeneral health":        ("Augusta",           "ME"),
    "mary washington healthcare": ("Fredericksburg",    "VA"),
    "mass general brigham":       ("Boston",            "MA"),
    "memorial health system":     ("Savannah",          "GA"),
    "methodist health system":    ("Dallas",            "TX"),
    "methodist le bonheur":       ("Memphis",           "TN"),
    "montefiore":                 ("Bronx",             "NY"),
    "united health services":     ("Binghamton",        "NY"),
    "rochester regional health":  ("Rochester",         "NY"),
    "richmond university medical center": ("Staten Island", "NY"),
    "monument health":            ("Rapid City",        "SD"),
    "multicare":                  ("Tacoma",            "WA"),
    "northeast georgia medical center": ("Gainesville", "GA"),
    "phelps health":              ("Rolla",             "MO"),
    "riverside health":           ("Newport News",      "VA"),
    "sih":                        ("Carbondale",        "IL"),
    "saint francis health system":("Tulsa",             "OK"),
    "tidelands health":           ("Murrells Inlet",    "SC"),
    "uhs":                        ("King of Prussia",   "PA"),
    "umass memorial health":      ("Worcester",         "MA"),
    "university of rochester medicine": ("Rochester",   "NY"),
    "uofl health":                ("Louisville",        "KY"),
    "vanderbilt health":          ("Nashville",         "TN"),
    "sentara healthcare":         ("Norfolk",           "VA"),
    "advocate health":            ("Charlotte",         "NC"),
    "west tennessee healthcare":  ("Jackson",           "TN"),
    "bozeman health":             ("Bozeman",           "MT"),
    "broadlawns medical center":  ("Des Moines",        "IA"),
    "hendricks regional health":  ("Danville",          "IN"),
    "harrison health":             ("Bremerton",         "WA"),
    "jupiter medical center":     ("Jupiter",           "FL"),
    "kaweah health":              ("Visalia",           "CA"),
    "lawrence memorial hospital": ("Lawrence",          "KS"),
    "owensboro health":           ("Owensboro",         "KY"),
    "salinas valley health":      ("Salinas",           "CA"),
    "samaritan health":           ("Watertown",         "NY"),
    "sarah bush lincoln health":  ("Mattoon",           "IL"),
    "saint francis medical center":("Cape Girardeau",   "MO"),
    "silver cross hospital":      ("New Lenox",         "IL"),
    "stormont vail health":       ("Topeka",            "KS"),
    "sturdy memorial hospital":   ("Attleboro",         "MA"),
    # SmartRecruiters
    "davita":                     ("Denver",            "CO"),
    "northwestern medicine":      ("Chicago",           "IL"),
    "healthpartners":             ("St. Paul",          "MN"),
    "envision healthcare":        ("Nashville",         "TN"),
    "amerihealth caritas":        ("Philadelphia",      "PA"),
    "chenmed":                    ("Miami",             "FL"),
    "alignment healthcare":       ("Orange",            "CA"),
    "kindred healthcare":         ("Louisville",        "KY"),
    "acadia healthcare":          ("Franklin",          "TN"),
    "surgery partners":           ("Nashville",         "TN"),
    # Playwright
    "mayo clinic":                ("Rochester",         "MN"),
    "christus health":            ("Irving",            "TX"),
    "baylor scott & white":       ("Dallas",            "TX"),
    "hca healthcare":             ("Nashville",         "TN"),
    "cleveland clinic":           ("Cleveland",         "OH"),
    "mymichigan health":          ("Midland",           "MI"),
    # CommonSpirit — REMOVED 2026-07-28. The old ("Chicago", "IL") last-resort
    # default was actively harmful: TalentBrew cities missing from
    # COMMONSPIRIT_CITY_STATE (Lufkin, Lake Jackson, Livingston, The Woodlands,
    # San Augustine — all the CHI St. Luke's TX towns) were being saved with
    # state=IL, hiding them from Texas searches/hubs. scrape_talentbrew now
    # reads "City, ST" straight off each result card, so the default would
    # only ever fire on a parse regression — better to save state="" and
    # surface the bug than to mislabel Texas jobs as Illinois.
    # Greenhouse
    "davita":                     ("Denver",            "CO"),
    # AdventHealth — Findly Google CTS (added 2026-04-24)
    "adventhealth":               ("Altamonte Springs", "FL"),
}

# Normalize system keys to lowercase
SYSTEM_LOCATION_DEFAULTS = {k.lower(): v for k, v in SYSTEM_LOCATION_DEFAULTS.items()}

# Systems where we ALWAYS override city/state regardless of what the ATS returns.
# Use sparingly — only when the ATS consistently returns wrong/campus-name data
# and every job is definitively in one location.
FORCE_LOCATION_OVERRIDE: dict[str, tuple[str, str]] = {
    # Systems where ALL jobs are in one metro — always override ATS data
    # 2026-09-10 (Y-texas-build): hospital-midlandhealth.icims.com cards carry no location field.
    "Midland Health": ("Midland", "TX"),
    "memorial hermann":                    ("Houston",      "TX"),
    "methodist health system":             ("Dallas",       "TX"),
    "methodist le bonheur":                ("Memphis",      "TN"),
    "northeast georgia medical center":    ("Gainesville",  "GA"),
    "cape fear valley health":             ("Fayetteville", "NC"),
    "broadlawns medical center":           ("Des Moines",   "IA"),
    "jupiter medical center":              ("Jupiter",      "FL"),
    "phelps health":                       ("Rolla",        "MO"),
    "sturdy memorial hospital":            ("Attleboro",    "MA"),
    "freeman health":                      ("Joplin",       "MO"),
    "sih":                                 ("Carbondale",   "IL"),
    "harrison health":                     ("Bremerton",    "WA"),
    "kaweah health":                       ("Visalia",      "CA"),
    "silver cross hospital":               ("New Lenox",    "IL"),
    "tidelands health":                    ("Murrells Inlet","SC"),
    "salinas valley health":               ("Salinas",      "CA"),
    "bozeman health":                      ("Bozeman",      "MT"),
    "logan health":                        ("Kalispell",    "MT"),
    "great river health":                  ("West Burlington","IA"),
    "halifax health":                      ("Daytona Beach","FL"),
    "mary washington healthcare":          ("Fredericksburg","VA"),
    "saint francis health system":         ("Tulsa",        "OK"),
    "saint francis medical center":        ("Cape Girardeau","MO"),
    "lawrence memorial hospital":          ("Lawrence",     "KS"),
}
FORCE_LOCATION_OVERRIDE = {k.lower(): v for k, v in FORCE_LOCATION_OVERRIDE.items()}


_NOT_CITY_WORDS = re.compile(
    r"\b(hospital|hospitals|medical|center|centre|clinic|clinics|campus|health|healthcare|regional|"
    r"building|bldg|office|offices|suite|ste|floor|tower|institute|services?|department|dept|"
    r"remote|corporate|plaza|park|complex|facility|system)\b", re.I)


_STREET_THEN_CITY_RE = re.compile(
    r"\d{1,6}\s+(?:[NSEW]\.?\s+)?[A-Za-z0-9 .'-]*?\b(?:Ave|Avenue|St|Street|Rd|Road|Blvd|Boulevard|Dr|Drive|Hwy|Highway|"
    r"Pkwy|Parkway|Way|Ln|Lane|Pl|Place|Ct|Court|Cir|Circle|Trl|Trail|Ter|Terrace)\.?,?\s+([A-Z][A-Za-z .'-]{2,30})$")


def _cityish(part: str) -> bool:
    """A comma-separated part that could be a city name: short, no digits,
    none of the facility words."""
    p = (part or "").strip()
    return bool(p) and len(p.split()) <= 4 and not any(ch.isdigit() for ch in p) \
        and " - " not in p and not _NOT_CITY_WORDS.search(p)


def parse_city_state(loc_str: str) -> tuple[str, str]:
    """
    Extract (city, state) from a location string robustly.
    Handles:
      - "City, ST"
      - "City, ST, United States"
      - "ST, City"  (CHRISTUS-style reversed)
      - Full state names from SmartRecruiters ("Chicago, Illinois")
      - Single-segment strings ("Remote")
    Returns 2-char state code where possible.
    """
    STATE_ABBR = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
        "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
        "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA",
        "kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
        "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
        "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH",
        "new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC",
        "north dakota":"ND","ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA",
        "rhode island":"RI","south carolina":"SC","south dakota":"SD","tennessee":"TN",
        "texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
        "west virginia":"WV","wisconsin":"WI","wyoming":"WY","district of columbia":"DC",
        "puerto rico":"PR","guam":"GU","virgin islands":"VI",
    }
    JUNK = {"united states","us","usa","canada","remote","united kingdom","uk",""}
    if not loc_str:
        return "", ""
    _codes = set(STATE_ABBR.values())
    s = str(loc_str).strip()
    # 2026-09-17 (blank states): Workday tenants also write "Rochester - NY",
    # "Dallas - TX (LBJ FWY)", "MOUNT PLEASANT (SC)" and "Remote Work - New
    # York". Rewrite those to "City, ST" before the comma split.
    if "," not in s:
        m = re.match(r"^(.*?\S)\s*[-\u2013]\s*([A-Za-z]{2})\b\s*(?:\(.*\))?\s*$", s)
        if m and m.group(2).upper() in _codes:
            s = f"{m.group(1)}, {m.group(2).upper()}"
        else:
            m = re.match(r"^(.*?\S)\s*\(([A-Za-z]{2})\)\s*$", s)
            if m and m.group(2).upper() in _codes:
                s = f"{m.group(1)}, {m.group(2).upper()}"
            else:
                m = re.match(r"^(.*?\S)\s*[-\u2013]\s*([A-Za-z ]{4,20}?)\s*$", s)
                if m and m.group(2).strip().lower() in STATE_ABBR:
                    s = f"{m.group(1)}, {m.group(2).strip()}"
    parts = [p.strip() for p in s.split(",")]
    # 2026-09-17: "Kennebunk, ME - Huntington Common" (Sunrise): a part that
    # starts with a state code and a dash is the state.
    parts = [(m.group(1).upper() if (m := re.match(r"^([A-Za-z]{2})\s*[-\u2013]\s*\S.*$", p)) and m.group(1).upper() in _codes else p)
             for p in parts]
    # Strip trailing zip codes from each part (e.g. "TX  75039" → "TX", "Irving TX 75039" → "Irving TX")
    import re as _re
    parts = [_re.sub(r'\s+\d{5}(-\d{4})?$', '', p).strip() for p in parts]
    # Remove segments that are purely numeric (zip-only segments)
    parts = [p for p in parts if not p.isdigit()]
    # Mark remote explicitly before stripping junk
    is_remote = any(p.lower() == "remote" for p in parts)
    parts = [p for p in parts if p.lower() not in JUNK]
    if not parts:
        return ("Remote", "") if is_remote else ("", "")

    # Find 2-char alpha state code anywhere in parts
    state = next((p for p in parts if len(p) == 2 and p.isalpha()), "")

    # If no 2-char code, check for full state name
    if not state:
        for p in parts:
            abbr = STATE_ABBR.get(p.lower(), "")
            if abbr:
                state = abbr
                break

    # Determine city: if first part IS the state code → reversed format
    if parts and len(parts[0]) == 2 and parts[0].isalpha() and parts[0].upper() == state:
        city = parts[1] if len(parts) > 1 else ""
    else:
        # Remove state/country parts to get city. 2026-09-17: prefer the part
        # that looks like a city ("MercyOne North Iowa Medical Center - East
        # Campus, Mason City, Iowa" -> Mason City), and take the tail of a
        # "Facility - City" part ("Saint Francis Hospital - Hartford, CT").
        cands = [p for p in parts
                 if p != state
                 and p.lower() not in JUNK
                 and not STATE_ABBR.get(p.lower(), "")]
        city = next((p for p in cands if _cityish(p)), cands[0] if cands else parts[0])
        if " - " in city:
            tail = city.rsplit(" - ", 1)[1].strip()
            if tail and len(tail.split()) <= 4 and not any(ch.isdigit() for ch in tail):
                city = tail
        if any(ch.isdigit() for ch in city):
            m = _STREET_THEN_CITY_RE.search(city)
            if m:
                city = m.group(1).strip()

    return city.strip(), state.upper() if state else ""


async def _workday_fetch_details(session, working_url, targets, system):
    """Fill description + ISO posted_date from Workday's per-job DETAIL endpoint.

    `targets` is [(Job, externalPath), ...]. Mutates the Job objects in place.
    Never raises and never aborts the scrape: a failed detail fetch simply
    leaves that job exactly as the list endpoint returned it, which is the
    current behaviour for every Workday job anyway. Worst case is no change.
    """
    # Detail URL = the CXS site base (list URL minus its trailing "/jobs")
    # plus the job's externalPath, which already begins with "/job/".
    base = working_url[:-len("/jobs")] if working_url.endswith("/jobs") else working_url

    # 2026-09-22: a teaser under DETAIL_MIN_CHARS is a candidate too (the
    # list endpoint sometimes carries a stub), and one tenant may take at most
    # budget/DETAIL_TENANT_SHARE so the first tenants to finish listing do not
    # drain the night's allowance.
    candidates = [(j, p) for j, p in targets
                  if p and len((j.description or "").strip()) < DETAIL_MIN_CHARS]
    # SHUFFLE (2026-08-05): the scrape can't see which rows already carry a
    # DB-side description (the preserve_scraped_enrichment trigger keeps those
    # safe), so without shuffling the budget re-fetched the SAME first-N jobs
    # every night and coverage never accumulated past one night's budget
    # (measured: 4,931 -> 4,935 across a full run). Random order makes each
    # night enrich a fresh slice; the trigger makes it stick.
    random.shuffle(candidates)
    # TRANSPARENCY-STATE PRIORITY (2026-08-21, Robert): jobs in states with
    # mandatory pay disclosure get their descriptions fetched first — those
    # descriptions carry posted wage ranges 60-80% of the time vs ~20%
    # elsewhere, so each budget slot yields 3-4x the wage pills AND the
    # indexable description either way. Stable sort preserves the shuffle
    # within each group, so coverage still accumulates across nights.
    TRANSPARENCY_STATES = {"CA", "CO", "CT", "DC", "HI", "IL", "MD", "MN", "NJ", "NY", "VT", "WA"}
    candidates.sort(key=lambda t: 0 if (t[0].state or "").strip().upper() in TRANSPARENCY_STATES else 1)
    # Draw from the RUN-WIDE budget, not a per-tenant one. Whichever tenants
    # finish their listing pass first get the allowance; later tenants simply
    # get none this run and pick it up on a subsequent night.
    cap = max(50, (WD_DESC_BUDGET.remaining + WD_DESC_BUDGET.spent) // max(1, DETAIL_TENANT_SHARE))
    allowed = WD_DESC_BUDGET.take(min(len(candidates), cap))
    pending = candidates[:allowed]
    if not pending:
        return

    sem = asyncio.Semaphore(WD_DESC_CONCURRENCY)
    gate = _wd_detail_gate or asyncio.Semaphore(WD_DESC_GLOBAL_CONCURRENCY)
    filled = dated = typed = 0

    async def one(job, path):
        nonlocal filled, dated, typed
        url = base + (path if path.startswith("/") else "/" + path)
        async with sem, gate:
            try:
                async with req(session, "get", url, headers=HEADERS, ssl=False,
                               proxy=proxies.get(),
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        return
                    data = await r.json()
            except Exception:
                return
            info = (data or {}).get("jobPostingInfo") or {}
            desc = strip_html(str(info.get("jobDescription") or ""))
            # Only accept a description that clears the sitemap bar and beats
            # what the list gave us; a shorter one adds storage and churn.
            if len(desc) >= 200 and len(desc) > len((job.description or "").strip()):
                job.description = desc
                filled += 1
            start = str(info.get("startDate") or "")[:10]
            if re.match(r"^\d{4}-\d{2}-\d{2}$", start):
                job.posted_date = start
                dated += 1
            # 2026-09-22: employment type from the detail when the list had none.
            tt = str(info.get("timeType") or "").strip()
            if tt and not (job.job_type or "").strip():
                job.job_type = tt
                typed += 1
            # Throttle inside the semaphore so this genuinely paces requests
            # rather than just staggering their completion.
            await asyncio.sleep(random.uniform(0.15, 0.45))

    await asyncio.gather(*[one(j, p) for j, p in pending], return_exceptions=True)
    logger.info(f"  Workday {system}: details {len(pending)} fetched -> "
                f"{filled} descriptions, {dated} ISO dates, {typed} employment types")


# ── Detail helpers shared by the Oracle / TalentBrew / Phenom / HCA passes ──
_LD_JSON_RX = re.compile(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _jobposting_from_html(html: str):
    """The JSON-LD JobPosting on a job page (Workday job pages, TalentBrew,
    HCA's Talemetry pages all carry one), or None."""
    for m in _LD_JSON_RX.finditer(html or ""):
        try:
            j = json.loads(m.group(1).strip())
        except Exception:
            continue
        cands = j if isinstance(j, list) else [j]
        for c in cands:
            if not isinstance(c, dict):
                continue
            if c.get("@type") == "JobPosting":
                return c
            for g in (c.get("@graph") or []):
                if isinstance(g, dict) and g.get("@type") == "JobPosting":
                    return g
    return None


def _apply_posting(job, posting: dict) -> bool:
    """Copy a JSON-LD JobPosting onto a Job: description (+ qualifications),
    employment type, posted date, structured pay. Fills blanks only; True when
    a description of 200+ characters landed."""
    desc = strip_html(str(posting.get("description") or "")).strip()
    quals = strip_html(str(posting.get("qualifications") or "")).strip()
    if quals and quals[:120] not in desc:
        desc = f"{desc}\n\nQualifications\n{quals}".strip()
    ok = False
    if len(desc) >= 200 and len(desc) > len((job.description or "").strip()):
        job.description = desc
        ok = True
    et = posting.get("employmentType")
    if isinstance(et, list):
        et = et[0] if et else None
    if et and not (job.job_type or "").strip():
        job.job_type = str(et)
    dp = str(posting.get("datePosted") or "")[:10]
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", dp)
    if m and not (job.posted_date or "").strip():
        job.posted_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    bs = posting.get("baseSalary")
    if isinstance(bs, dict) and job.wage_min is None:
        val = bs.get("value") if isinstance(bs.get("value"), dict) else bs
        try:
            lo = float(val.get("minValue")) if val.get("minValue") is not None else None
            hi = float(val.get("maxValue")) if val.get("maxValue") is not None else lo
            if lo is None and val.get("value") is not None:
                lo = hi = float(val.get("value"))
        except (TypeError, ValueError, AttributeError):
            lo = hi = None
        got = _wage_pair(lo, hi) if lo and hi else None
        if got:
            job.wage_min, job.wage_max, job.wage_unit = got
    return ok


async def _fetch_html(session, url: str, timeout: int = 20) -> str:
    try:
        async with req(session, "get", url, headers={**HEADERS, "Accept": "text/html,*/*"},
                       ssl=False, proxy=proxies.get(),
                       timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                return ""
            return await r.text()
    except Exception:
        return ""


async def _jsonld_detail(session, job) -> bool:
    """Job page -> JSON-LD JobPosting -> Job fields."""
    posting = _jobposting_from_html(await _fetch_html(session, job.url))
    return _apply_posting(job, posting) if posting else False


async def _detail_pass(session, system: str, jobs: list, budget, fetch_one, label: str, skip=None, share: int | None = None) -> None:
    """Shared driver: rows without a full body, shuffled, transparency states
    first, at most a fair share of the platform budget per tenant, fetched
    under a semaphore with a small pause. `skip(job)` excludes rows whose page
    is known to carry nothing (Taleo, iCIMS behind a Phenom front) so they
    cost no budget. Never raises."""
    cands = [j for j in jobs if (j.url or "").startswith("http")
             and len((j.description or "").strip()) < DETAIL_MIN_CHARS
             and not (skip and skip(j))]
    if not cands:
        return
    random.shuffle(cands)
    cands.sort(key=lambda j: 0 if (j.state or "").strip().upper() in DETAIL_PRIORITY_STATES else 1)
    cap = share if share else max(50, (budget.remaining + budget.spent) // max(1, DETAIL_TENANT_SHARE))
    allowed = budget.take(min(len(cands), cap))
    pending = cands[:allowed]
    if not pending:
        return
    sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
    filled = 0

    async def one(job):
        nonlocal filled
        async with sem:
            try:
                if await fetch_one(job):
                    filled += 1
            except Exception:
                pass
            await asyncio.sleep(random.uniform(0.15, 0.45))

    await asyncio.gather(*[one(j) for j in pending], return_exceptions=True)
    logger.info(f"  {label} {system}: details {len(pending)} fetched -> {filled} descriptions; "
                f"{len(cands) - len(pending)} left for another night; budget left {budget.remaining}")


def _oracle_posting_text(it: dict) -> tuple[str, str, str]:
    """(description, job_type, posted_date) from an Oracle requisition detail.
    The schedule line comes first so the facts extractor sees hours and shift."""
    sched = str(it.get("JobSchedule") or "").strip()
    shift = str(it.get("JobShift") or "").strip()
    hours = it.get("WorkHours")
    days = str(it.get("WorkDays") or "").strip()
    head = " · ".join(x for x in (sched, shift, f"{hours} hours per week" if hours else "", days) if x)
    parts = [f"Schedule: {head}"] if head else []
    for k in ("ExternalDescriptionStr", "ExternalResponsibilitiesStr", "ExternalQualificationsStr"):
        v = strip_html(str(it.get(k) or "")).strip()
        if v:
            parts.append(v)
    start = str(it.get("ExternalPostedStartDate") or "")[:10]
    return "\n\n".join(parts), sched, (start if re.match(r"^\d{4}-\d{2}-\d{2}$", start) else "")


async def _oracle_detail(session, base_url: str, site_number: str, job) -> bool:
    api = f"{base_url}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
    params = {"expand": "all", "onlyData": "true",
              "finder": f'ById;Id="{job.job_id}",siteNumber={site_number}'}
    async with req(session, "get", api, params=params,
                   headers={**HEADERS, "Accept": "application/json", "REST-Framework-Version": "4"},
                   ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
        if r.status != 200:
            return False
        data = await r.json(content_type=None)
    items = (data or {}).get("items") or []
    if not items:
        return False
    desc, sched, start = _oracle_posting_text(items[0])
    ok = False
    if len(desc) >= 200 and len(desc) > len((job.description or "").strip()):
        job.description = desc
        ok = True
    if sched and not (job.job_type or "").strip():
        job.job_type = sched
    if start and not (job.posted_date or "").strip():
        job.posted_date = start
    return ok


def _phenom_posting_text(jd: dict) -> tuple[str, str, str]:
    """(description, job_type, posted_date) from a Phenom widgets jobDetail."""
    desc = strip_html(str(jd.get("description") or "")).strip()
    shift = str(jd.get("shift") or "").strip()
    if shift and shift.lower() not in desc.lower()[:400]:
        desc = f"Schedule: {shift}\n\n{desc}"
    jt = str(jd.get("type") or jd.get("jobType") or "").strip()
    created = str(jd.get("dateCreated") or jd.get("postedDate") or "")[:10]
    return desc, jt, (created if re.match(r"^\d{4}-\d{2}-\d{2}$", created) else "")


_ORACLE_PREVIEW_RE = re.compile(
    r"(https://[^/]+\.oraclecloud\.com/hcmUI/CandidateExperience/[a-z]{2}/sites/[^/]+)/jobs/preview/([0-9A-Za-z]+)")


async def _phenom_detail(session, base_url: str, job) -> bool:
    """Phenom rows point at three kinds of page: the Phenom job page (legacy
    tenants answer the widgets jobDetail call), a Workday job page (Corewell,
    SSM, LCMC: the Phenom front is a skin) or Taleo (nothing to read). Try the
    widget, then the page's JSON-LD."""
    if "/job/" in (job.url or "") and "myworkdayjobs" not in job.url and "taleo" not in job.url:
        try:
            body = {"lang": "en_us", "deviceType": "desktop", "country": "us", "pageName": "job-page",
                    "ddoKey": "jobDetail", "jobId": str(job.job_id)}
            async with req(session, "post", f"{base_url}/widgets", json=body,
                           headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json"},
                           ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200 and "json" in (r.headers.get("content-type") or ""):
                    data = await r.json(content_type=None)
                    jd = (((data or {}).get("jobDetail") or {}).get("data") or {}).get("job") or {}
                    if jd:
                        desc, jt, created = _phenom_posting_text(jd)
                        ok = False
                        if len(desc) >= 200 and len(desc) > len((job.description or "").strip()):
                            job.description = desc
                            ok = True
                        if jt and not (job.job_type or "").strip():
                            job.job_type = jt
                        if created and not (job.posted_date or "").strip():
                            job.posted_date = created
                        # 2026-09-24: Oracle-backed tenants (Ascension) carry
                        # structured pay here; 0.0 means "not stated".
                        if job.wage_min is None:
                            try:
                                got = _wage_pair(float(jd.get("minSalaryNew") or 0) or None,
                                                 float(jd.get("maxSalaryNew") or 0) or None)
                            except (TypeError, ValueError):
                                got = None
                            if got:
                                job.wage_min, job.wage_max, job.wage_unit = got
                        if ok:
                            return True
        except Exception:
            pass
    if "taleo" in (job.url or ""):
        return False
    return await _jsonld_detail(session, job)


# 2026-09-16 (NY coverage): Montefiore's Workday tenant lists postings by street
# address, so parse_city_state() found no city and no state on all 444 rows.
# Address prefix -> (facility marketing name, city); every unmatched address in
# the tally was in the Bronx, so that is the fallback city for this tenant.
MONTEFIORE_ADDR_MAP = [
    ("111 East 210", ("Montefiore Medical Center, Moses Campus", "Bronx")),
    ("110 East 210", ("Montefiore Medical Center, Moses Campus", "Bronx")),
    ("1825 Eastchester", ("Montefiore Medical Center, Einstein Campus", "Bronx")),
    ("1695 Eastchester", ("Montefiore Medical Center, Einstein Campus", "Bronx")),
    ("1621 Eastchester", ("Montefiore Medical Center, Einstein Campus", "Bronx")),
    ("600 East 233", ("Montefiore Medical Center, Wakefield Campus", "Bronx")),
    ("3415 Bainbridge", ("Children's Hospital at Montefiore", "Bronx")),
    ("3400 Bainbridge", ("Children's Hospital at Montefiore", "Bronx")),
    ("1250 Waters", ("Montefiore Hutchinson Campus", "Bronx")),
    ("1510 Waters", ("Montefiore Hutchinson Campus", "Bronx")),
    ("1300 Morris Park", ("Albert Einstein College of Medicine", "Bronx")),
    ("555 South Broadway", ("Montefiore, Tarrytown", "Tarrytown")),
    ("555 Taxter", ("Montefiore, Elmsford", "Elmsford")),
    ("3 Odell", ("Montefiore, Yonkers", "Yonkers")),
    ("200 Corporate", ("Montefiore, Yonkers", "Yonkers")),
    ("100 Corporate", ("Montefiore, Yonkers", "Yonkers")),
    ("6 Executive", ("Montefiore, Yonkers", "Yonkers")),
    ("4 Executive", ("Montefiore, Yonkers", "Yonkers")),
    ("440 White Plains", ("Montefiore, Eastchester", "Eastchester")),
]


def _montefiore_loc(loc: str, city: str, state: str) -> tuple[str, str, str]:
    """(hospital_name, city, state) for a Montefiore locationsText."""
    text = (loc or "").strip()
    for prefix, (name, c) in MONTEFIORE_ADDR_MAP:
        if text.startswith(prefix):
            return name, c, "NY"
    return "Montefiore", (city or "Bronx"), "NY"


# A Workday bulletField that is a requisition number: optional letter prefix,
# optional separator, 3+ digits, optional suffix, no spaces.
_WD_REQ_ID_RE = re.compile(r"^[A-Za-z]{0,12}[-_ ]?\d{3,}[A-Za-z0-9._-]*$")

# 2026-09-17 (blank states): tenants whose locationsText is a facility name
# ("Ruby Memorial Hospital (WVUH)", "Strong Memorial Hospital", "SJHSYR-
# MAINCAMPUS") or nothing at all. 25,882 active rows carried no state on
# 2026-09-17, WVU 3,140 and Trinity 2,656 of them. Keyed by the WORKDAY_TENANTS
# label; inner keys are the locationsText lowercased, whitespace collapsed,
# trailing "(CODE)" stripped; a key also matches as a prefix. Value =
# (facility label or None to keep the system name, city, state).
WD_FACILITY_MAP: dict[str, dict[str, tuple[str | None, str, str]]] = {
    # 2026-09-22 Texas resume: the two campuses that are hospitals take the
    # CMS names; clinics and offices keep the system name (SYSTEM_LOCATION_DEFAULTS gives Lubbock TX).
    "UMC Health System": {
        "umc main campus": ("University Medical Center", "Lubbock", "TX"),
        "health & wellness hospital": ("UMC Health & Wellness Hospital", "Lubbock", "TX"),
    },
    "WVU Medicine": {
        "ruby memorial hospital": ("J.W. Ruby Memorial Hospital", "Morgantown", "WV"),
        "wvu medicine golisano children's hospital": ("WVU Medicine Children's Hospital", "Morgantown", "WV"),
        "wvu medicine children's hospital": ("WVU Medicine Children's Hospital", "Morgantown", "WV"),
        "berkeley medical center": ("Berkeley Medical Center", "Martinsburg", "WV"),
        "united hospital center": ("United Hospital Center", "Bridgeport", "WV"),
        "wheeling hospital": ("Wheeling Hospital", "Wheeling", "WV"),
        "thomas memorial hospital": ("Thomas Memorial Hospital", "South Charleston", "WV"),
        "uniontown hospital": ("Uniontown Hospital", "Uniontown", "PA"),
        "camden clark": ("Camden Clark Medical Center", "Parkersburg", "WV"),
        "weirton medical center": ("Weirton Medical Center", "Weirton", "WV"),
        "wmch medical office building weirton": ("Weirton Medical Center", "Weirton", "WV"),
        "princeton community hospital": ("Princeton Community Hospital", "Princeton", "WV"),
        "potomac valley hospital": ("Potomac Valley Hospital", "Keyser", "WV"),
        "st. joseph's hospital": ("St. Joseph's Hospital", "Buckhannon", "WV"),
        "garrett regional medical center": ("Garrett Regional Medical Center", "Oakland", "MD"),
        "jefferson medical center": ("Jefferson Medical Center", "Ranson", "WV"),
        "reynolds memorial hospital": ("Reynolds Memorial Hospital", "Glen Dale", "WV"),
        "fairmont medical center": ("Fairmont Medical Center", "Fairmont", "WV"),
        "grant memorial hospital": ("Grant Memorial Hospital", "Petersburg", "WV"),
        "jackson general hospital": ("Jackson General Hospital", "Ripley", "WV"),
        "summersville regional medical center": ("Summersville Regional Medical Center", "Summersville", "WV"),
        "saint francis hospital": ("Saint Francis Hospital", "Charleston", "WV"),
        "braxton county memorial hospital": ("Braxton County Memorial Hospital", "Gassaway", "WV"),
        "wetzel county hospital": ("Wetzel County Hospital", "New Martinsville", "WV"),
        "harrison community hospital": ("Harrison Community Hospital", "Cadiz", "OH"),
        "barnesville hospital": ("Barnesville Hospital", "Barnesville", "OH"),
        "bluefield behavioral health center": ("Bluefield Behavioral Health Center", "Bluefield", "WV"),
        "mary babb randolph cancer center": ("Mary Babb Randolph Cancer Center", "Morgantown", "WV"),
        "rockefeller neuroscience institute": ("Rockefeller Neuroscience Institute", "Morgantown", "WV"),
        "wvu medicine eye institute": ("WVU Medicine Eye Institute", "Morgantown", "WV"),
        "healthy minds clarksburg": (None, "Clarksburg", "WV"),
        "southpointe clinic": (None, "Canonsburg", "PA"),
        "operations support center": (None, "Morgantown", "WV"),
        "physician office center": (None, "Morgantown", "WV"),
        "health sciences center": (None, "Morgantown", "WV"),
        "healthy minds morgantown": (None, "Morgantown", "WV"),
        "information technology center": (None, "Morgantown", "WV"),
        "suncrest towne centre": (None, "Morgantown", "WV"),
        "university town center": (None, "Morgantown", "WV"),
    },
    "University of Rochester": {
        "strong memorial hospital": ("Strong Memorial Hospital", "Rochester", "NY"),
        "golisano children's hospital": ("Golisano Children's Hospital", "Rochester", "NY"),
        "james p. wilmot cancer center": ("Wilmot Cancer Institute", "Rochester", "NY"),
        "wilmot": ("Wilmot Cancer Institute", "Rochester", "NY"),
        "highland hospital": ("Highland Hospital", "Rochester", "NY"),
        "strong west": ("Strong West", "Brockport", "NY"),
        "eastman dental": ("Eastman Institute for Oral Health", "Rochester", "NY"),
        "f.f. thompson": ("F.F. Thompson Hospital", "Canandaigua", "NY"),
        "thompson health": ("F.F. Thompson Hospital", "Canandaigua", "NY"),
        "noyes": ("Noyes Health", "Dansville", "NY"),
        "jones memorial": ("Jones Memorial Hospital", "Wellsville", "NY"),
        "st. james": ("St. James Hospital", "Hornell", "NY"),
        "school of medicine and dentistry": (None, "Rochester", "NY"),
    },
    "Trinity Health": {
        "sjhsyr": ("St. Joseph's Health Hospital", "Syracuse", "NY"),
        "loyola medicine - loyola university medical center": ("Loyola University Medical Center", "Maywood", "IL"),
        "loyola medicine - macneal hospital": ("MacNeal Hospital", "Berwyn", "IL"),
        "loyola medicine - gottlieb memorial hospital": ("Gottlieb Memorial Hospital", "Melrose Park", "IL"),
        "loyola medicine": ("Loyola Medicine", "Maywood", "IL"),
        "mmcia": ("MercyOne Des Moines Medical Center", "Des Moines", "IA"),
        "mneia": ("MercyOne Waterloo Medical Center", "Waterloo", "IA"),
        "moq": ("MercyOne Quad Cities", "Bettendorf", "IA"),
        "mount carmel east": ("Mount Carmel East", "Columbus", "OH"),
        "mount carmel st. ann's": ("Mount Carmel St. Ann's", "Westerville", "OH"),
        "mount carmel grove city": ("Mount Carmel Grove City", "Grove City", "OH"),
        "mount carmel": ("Mount Carmel Health", "Columbus", "OH"),
        "mercy catholic medical center - mercy fitzgerald campus": ("Mercy Fitzgerald Hospital", "Darby", "PA"),
        "langhorne": ("St. Mary Medical Center", "Langhorne", "PA"),
        "thiha": ("Trinity Health IHA Medical Group", "Ann Arbor", "MI"),
    },
    "Intermountain Health (IMH)": {
        "intermountain medical center": ("Intermountain Medical Center", "Murray", "UT"),
        "alta view hospital": ("Alta View Hospital", "Sandy", "UT"),
        "riverton hospital": ("Riverton Hospital", "Riverton", "UT"),
        "american fork hospital": ("American Fork Hospital", "American Fork", "UT"),
        "layton hospital": ("Layton Hospital", "Layton", "UT"),
        "park city hospital": ("Park City Hospital", "Park City", "UT"),
        "cedar city hospital": ("Cedar City Hospital", "Cedar City", "UT"),
        "spanish fork hospital": ("Spanish Fork Hospital", "Spanish Fork", "UT"),
        "orem community hospital": ("Orem Community Hospital", "Orem", "UT"),
        "bear river valley hospital": ("Bear River Valley Hospital", "Tremonton", "UT"),
        "cassia regional hospital": ("Cassia Regional Hospital", "Burley", "ID"),
        "sevier valley hospital": ("Sevier Valley Hospital", "Richfield", "UT"),
        "sanpete valley hospital": ("Sanpete Valley Hospital", "Mount Pleasant", "UT"),
        "heber valley hospital": ("Heber Valley Hospital", "Heber City", "UT"),
        "garfield memorial hospital": ("Garfield Memorial Hospital", "Panguitch", "UT"),
        "st. james healthcare": ("St. James Healthcare", "Butte", "MT"),
        "platte valley medical center": ("Platte Valley Medical Center", "Brighton", "CO"),
        "primary childrens hospital": ("Primary Children's Hospital", "Salt Lake City", "UT"),
        "primary children's hospital": ("Primary Children's Hospital", "Salt Lake City", "UT"),
        "primary childrens at lehi": ("Primary Children's Hospital Lehi", "Lehi", "UT"),
        "primary childrens at taylorsville": ("Primary Children's Hospital", "Taylorsville", "UT"),
        "lds hospital": ("LDS Hospital", "Salt Lake City", "UT"),
        "utah valley hospital": ("Utah Valley Hospital", "Provo", "UT"),
        "mckay-dee hospital": ("McKay-Dee Hospital", "Ogden", "UT"),
        "logan regional hospital": ("Logan Regional Hospital", "Logan", "UT"),
        "st. george regional hospital": ("St. George Regional Hospital", "St. George", "UT"),
        "saint joseph hospital": ("Saint Joseph Hospital", "Denver", "CO"),
        "st. mary's medical center": ("St. Mary's Medical Center", "Grand Junction", "CO"),
        "good samaritan medical center": ("Good Samaritan Medical Center", "Lafayette", "CO"),
        "lutheran medical center": ("Lutheran Medical Center", "Wheat Ridge", "CO"),
        "st. vincent regional hospital": ("St. Vincent Regional Hospital", "Billings", "MT"),
        "holy rosary healthcare": ("Holy Rosary Healthcare", "Miles City", "MT"),
        "peaks regional office": (None, "Broomfield", "CO"),
        "nevada central office": (None, "Las Vegas", "NV"),
        "home services - salt lake city": (None, "Salt Lake City", "UT"),
        "valley center tower": (None, "Salt Lake City", "UT"),
    },
    "Saint Francis Health": {
        "saint francis eufaula": ("Saint Francis Hospital Eufaula", "Eufaula", "OK"),
        "saint francis muskogee": ("Saint Francis Hospital Muskogee", "Muskogee", "OK"),
        "saint francis vinita": ("Saint Francis Hospital Vinita", "Vinita", "OK"),
        "saint francis south": ("Saint Francis Hospital South", "Tulsa", "OK"),
        "natalie building": (None, "Tulsa", "OK"),
        "warren building": (None, "Tulsa", "OK"),
        "blandine building": (None, "Tulsa", "OK"),
        "tower": (None, "Tulsa", "OK"),
    },
    "Freeman Health System": {
        "freeman fort scott": ("Freeman Fort Scott", "Fort Scott", "KS"),
        "freeman neosho": ("Freeman Neosho Hospital", "Neosho", "MO"),
    },
    # 2026-09-24 configs: BILH's locationsText is the facility ("Anna Jaques
    # Hospital", "Lahey Clinic, Peabody") with no state, read from the tenant's
    # locations facet (160 values). The hospitals take their own names and
    # towns; Exeter and the "Core" practices are New Hampshire; everything
    # else falls to the tenant default (Boston MA).
    "Beth Israel Lahey Health": {
        "beth israel deaconess medical center": ("Beth Israel Deaconess Medical Center", "Boston", "MA"),
        "bidmc east campus": ("Beth Israel Deaconess Medical Center", "Boston", "MA"),
        "bidmc west campus": ("Beth Israel Deaconess Medical Center", "Boston", "MA"),
        "beth israel deaconess hospital milton": ("Beth Israel Deaconess Hospital-Milton", "Milton", "MA"),
        "beth israel deaconess hospital needham": ("Beth Israel Deaconess Hospital-Needham", "Needham", "MA"),
        "beth israel deaconess hospital plymouth": ("Beth Israel Deaconess Hospital-Plymouth", "Plymouth", "MA"),
        "bid plymouth hospital": ("Beth Israel Deaconess Hospital-Plymouth", "Plymouth", "MA"),
        "lahey hospital and medical center": ("Lahey Hospital & Medical Center", "Burlington", "MA"),
        "lahey clinic": ("Lahey Hospital & Medical Center", "Burlington", "MA"),
        "lahey medical center peabody": ("Lahey Medical Center, Peabody", "Peabody", "MA"),
        "lahey medical center": ("Lahey Hospital & Medical Center", "Burlington", "MA"),
        "lahey med. ctr.": ("Lahey Hospital & Medical Center", "Burlington", "MA"),
        "addison gilbert hospital": ("Addison Gilbert Hospital", "Gloucester", "MA"),
        "anna jaques hospital": ("Anna Jaques Hospital", "Newburyport", "MA"),
        "bayridge hospital": ("BayRidge Hospital", "Lynn", "MA"),
        "beverly hospital": ("Beverly Hospital", "Beverly", "MA"),
        "mount auburn hospital": ("Mount Auburn Hospital", "Cambridge", "MA"),
        "new england baptist hospital": ("New England Baptist Hospital", "Boston", "MA"),
        "nebh hospital": ("New England Baptist Hospital", "Boston", "MA"),
        "winchester hospital": ("Winchester Hospital", "Winchester", "MA"),
        "exeter hospital": ("Exeter Hospital", "Exeter", "NH"),
        "core ": (None, "Exeter", "NH"),
        "bidhc salem nh": (None, "Salem", "NH"),
        "bidhc seabrook": (None, "Seabrook", "NH"),
        "beth israel lahey health": (None, "Boston", "MA"),
    },
}

# Single-market tenants: rows still without a state after the map and the
# facet pass take the tenant's home market. Multi-state tenants (Trinity,
# Intermountain, Sunrise) are deliberately absent: a wrong state is worse than
# none, and the state facet covers them where the tenant exposes one.
WD_TENANT_DEFAULT: dict[str, tuple[str, str]] = {
    "UMC Health System": ("Lubbock", "TX"),   # 2026-09-22 Texas resume: every UMC site is in Lubbock
    "WVU Medicine":            ("Morgantown", "WV"),
    "University of Rochester": ("Rochester", "NY"),
    "MultiCare Health":        ("Tacoma", "WA"),
    "Saint Francis Health":    ("Tulsa", "OK"),
    "Freeman Health System":   ("Joplin", "MO"),
    "Samaritan Health NY":     ("Watertown", "NY"),
    "Duly Health and Care":    ("Downers Grove", "IL"),
    "Wellstar Health (Providers)": ("Marietta", "GA"),
    "Allegheny Health Network": ("Pittsburgh", "PA"),   # 2026-09-24: "51 Locations"-style rows
    "Beth Israel Lahey Health": ("Boston", "MA"),       # 2026-09-24: clinics missing from WD_FACILITY_MAP
}

# 2026-09-24: tenants whose locationsText leads with "City ST" and no comma
# between them ("Pittsburgh PA, 15212, 320 E N Ave."). parse_city_state
# reads "Pittsburgh PA" as the city, so the AHN dry run came back with 1,833
# of 1,896 states blank. Scoped to the listed tenants so no other board's
# parse changes.
WD_CITY_SPACE_STATE = {"Allegheny Health Network"}
_WD_CITY_SPACE_ST_RE = re.compile(r"^([A-Za-z][A-Za-z .'-]*?[A-Za-z.])\s+([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?$")
_WD_MULTI_LOC_RE = re.compile(r"^\d+\s+Locations?$", re.I)
_WD_REMOTE_CITY_RE = re.compile(r"^(?:working at home|work from|remote)\b", re.I)


def _wd_city_space_state(loc: str, city: str, state: str) -> tuple[str, str]:
    """(city, state) for a locationsText that leads with "City ST"; "N
    Locations" clears the city so the tenant default fills it, and a remote
    row ("Working at Home - Ohio", "Work From Anywhere") keeps its state but
    no town."""
    text = (loc or "").strip()
    if not state:
        m = _WD_CITY_SPACE_ST_RE.match(text.split(",")[0].strip())
        if m and m.group(2) in _STATE_CODE_BY_NAME.values():
            return m.group(1).strip(), m.group(2)
        if _WD_MULTI_LOC_RE.match(text):
            return "", ""
    if _WD_REMOTE_CITY_RE.match(city or ""):
        return "", state
    return city, state


_STATE_CODE_BY_NAME = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}


def _wd_facility(system: str, loc: str, city: str, state: str) -> tuple[str | None, str, str]:
    """(facility or None, city, state) from WD_FACILITY_MAP for one posting.
    Parsed city/state win; the map only fills what is blank."""
    fmap = WD_FACILITY_MAP.get(system)
    if not fmap:
        return None, city, state
    key = re.sub(r"\s*\([A-Za-z0-9&/ .'-]{1,14}\)\s*$", "", loc or "")
    key = re.sub(r"\s+", " ", key).strip().lower()
    if not key:
        return None, city, state
    hit = fmap.get(key)
    if hit is None:
        # prefix, then suffix ("Intermountain Health Alta View Hospital"),
        # longest key first so "mount carmel east" beats "mount carmel".
        for k in sorted(fmap, key=len, reverse=True):
            if key.startswith(k) or key.endswith(" " + k):
                hit = fmap[k]
                break
    if hit is None:
        return None, city, state
    facility, fc, fs = hit
    # 2026-09-24: with no comma and no state, parse_city_state hands the whole
    # facility string back as the "city" ("Anna Jaques Hospital"). When the
    # map knows the town, the town wins over that echo.
    if fc and not state and re.sub(r"\s+", " ", (city or "")).strip().lower() in (key, re.sub(r"\s+", " ", loc or "").strip().lower()):
        city = ""
    return facility, (city or fc), (state or fs)


WD_FACET_BUDGET = int(os.getenv("WD_FACET_BUDGET", "600"))   # requests per tenant for the location passes
WD_FACET_WINDOW = 2000   # Workday serves at most 2,000 results per filter


def _wd_facet_groups(data: dict) -> list[tuple[str, str, list]]:
    """(facetParameter, group label, values) for every top-level facet and
    every nested group inside locationMainGroup (Workday nests Country /
    State / City / Locations there: MultiCare's `locations` values read
    "Auburn, Washington", Intermountain's locationHierarchy2 is the city and
    locationRegionStateProvince the state, Trinity's state facet is top-level)."""
    out = []
    for f in data.get("facets") or []:
        p = f.get("facetParameter") or ""
        vals = f.get("values") or []
        if vals and isinstance(vals[0], dict) and vals[0].get("values"):
            for g in vals:
                gp, gv = g.get("facetParameter") or "", g.get("values") or []
                if gp and gv:
                    out.append((gp, g.get("descriptor") or "", gv))
        elif vals:
            out.append((p, "", vals))
    return out


async def _wd_facet_pass(session, working_url: str, param: str, values: list, budget: list) -> dict[str, str]:
    """externalPath -> value descriptor for one facet parameter, paging each
    value (20 a page) until the tenant budget runs out."""
    out: dict[str, str] = {}
    for v in values:
        desc = (v.get("descriptor") or "").strip()
        vid = v.get("id")
        if not desc or not vid or not (v.get("count") or 0):
            continue
        offset = 0
        while offset < WD_FACET_WINDOW and budget[0] > 0:
            budget[0] -= 1
            try:
                async with req(session, "post", working_url,
                               json={"limit": 20, "offset": offset, "searchText": "",
                                     "appliedFacets": {param: [vid]}},
                               headers={**HEADERS, "Content-Type": "application/json"},
                               ssl=False, proxy=proxies.get(),
                               timeout=aiohttp.ClientTimeout(total=25)) as r:
                    if r.status != 200:
                        break
                    page = await r.json()
            except Exception:
                break
            posts = page.get("jobPostings") or []
            for pst in posts:
                ext = pst.get("externalPath")
                if ext:
                    out.setdefault(ext, desc)
            if len(posts) < 20:
                break
            offset += 20
            await jitter()
    return out


async def _wd_location_facets(session, working_url: str, system: str) -> tuple[dict[str, str], dict[str, str]]:
    """(externalPath -> city, externalPath -> state) from the tenant's location
    facets. Prefers a "City, State" locations group (one pass); otherwise a
    state group plus a city group. Empty dicts when the tenant has neither."""
    try:
        async with req(session, "post", working_url,
                       json={"limit": 1, "offset": 0, "searchText": "", "appliedFacets": {}},
                       headers={**HEADERS, "Content-Type": "application/json"},
                       ssl=False, proxy=proxies.get(),
                       timeout=aiohttp.ClientTimeout(total=25)) as r:
            data = await r.json() if r.status == 200 else {}
    except Exception:
        return {}, {}
    groups = _wd_facet_groups(data)

    def _desc(v):
        return (v.get("descriptor") or "").strip()

    def _is_state_group(vals):
        named = sum(1 for v in vals if _desc(v).lower() in _STATE_CODE_BY_NAME)
        return named >= max(1, len(vals) // 2)

    def _is_city_state_group(vals):
        parsed = sum(1 for v in vals if all(parse_city_state(_desc(v))))
        return parsed >= max(1, (len(vals) * 2) // 3)

    budget = [WD_FACET_BUDGET]
    path_city: dict[str, str] = {}
    path_state: dict[str, str] = {}
    locs = next(((p, v) for p, _, v in groups if p == "locations" and _is_city_state_group(v)), None)
    if locs:
        by_path = await _wd_facet_pass(session, working_url, locs[0], locs[1], budget)
        for ext, desc in by_path.items():
            c, st = parse_city_state(desc)
            if st:
                path_state[ext] = st
            if c:
                path_city[ext] = c
        logger.info(f"  Workday {system}: locations facet located {len(path_state):,} postings ({len(locs[1])} values)")
        return path_city, path_state
    state_g = next(((p, v) for p, _, v in groups
                    if re.search(r"state|province|region", p, re.I) and _is_state_group(v)), None)
    if state_g:
        by_path = await _wd_facet_pass(session, working_url, state_g[0], state_g[1], budget)
        for ext, desc in by_path.items():
            code = _STATE_CODE_BY_NAME.get(desc.lower())
            if code:
                path_state[ext] = code
        city_g = next(((p, v) for p, label, v in groups
                       if p == "locationHierarchy2" or label.strip().lower() == "city"), None)
        if city_g and budget[0] > 0:
            by_path = await _wd_facet_pass(session, working_url, city_g[0], city_g[1], budget)
            for ext, desc in by_path.items():
                if _cityish(desc):
                    path_city[ext] = desc
        logger.info(f"  Workday {system}: state facet '{state_g[0]}' located {len(path_state):,} postings"
                    f" ({len(state_g[1])} states); city facet {len(path_city):,}; budget left {budget[0]}")
    return path_city, path_state


def _wd_apply_locations(jobs: list, ext_by_job: dict, path_city: dict, path_state: dict,
                        default: tuple | None) -> tuple[int, int]:
    """Fill blank states (and blank cities) from the facet maps, then the
    tenant default for whatever is still without a state.
    Returns (filled_by_facet, filled_by_default)."""
    by_facet = by_default = 0
    for jb in jobs:
        ext = ext_by_job.get(id(jb), "")
        if not (jb.city or "").strip() and path_city.get(ext):
            jb.city = path_city[ext]
        if (jb.state or "").strip():
            continue
        st = path_state.get(ext, "") if path_state else ""
        if st:
            jb.state = st
            by_facet += 1
        elif default:
            jb.city = jb.city or default[0]
            jb.state = default[1]
            by_default += 1
    return by_facet, by_default


def _wd_apply_states(jobs: list, ext_by_job: dict, path_state: dict, default: tuple | None) -> tuple[int, int]:
    return _wd_apply_locations(jobs, ext_by_job, {}, path_state, default)


async def _wd_fill_states(session, working_url: str, system: str, jobs: list, detail_targets: list) -> None:
    """Blank-state recovery for one tenant: location facets, then tenant default."""
    blank = sum(1 for jb in jobs if not (jb.state or "").strip())
    if not blank:
        return
    path_city: dict[str, str] = {}
    path_state: dict[str, str] = {}
    if blank >= max(20, len(jobs) // 5):
        try:
            path_city, path_state = await _wd_location_facets(session, working_url, system)
        except Exception as e:
            logger.info(f"Workday {system}: location facet pass failed ({e})")
    ext_by_job = {id(j): ext for j, ext in detail_targets}
    by_facet, by_default = _wd_apply_locations(jobs, ext_by_job, path_city, path_state, WD_TENANT_DEFAULT.get(system))
    still = sum(1 for jb in jobs if not (jb.state or "").strip())
    logger.info(f"  Workday {system}: blank states {blank:,} -> facet {by_facet:,}, default {by_default:,}, still blank {still:,}")


async def scrape_workday(session: aiohttp.ClientSession, system: str, tenant_data: tuple) -> list[Job]:
    tenant, wd_num, primary_site = tenant_data
    jobs = []
    # (Job, externalPath) pairs, so the optional detail pass can rebuild each
    # job's CXS detail URL. externalPath is not carried on the Job dataclass.
    detail_targets = []

    # Use the confirmed URL directly — no probe loop
    working_url = f"https://{tenant}.wd{wd_num}.myworkdayjobs.com/wday/cxs/{tenant}/{primary_site}/jobs"
    logger.info(f"Workday {system}: using {working_url}")
    LIMIT = 20
    # Workday's CXS search serves at most ~2,000 UNIQUE results per query —
    # measured 2026-08-04: pages past offset 2000 return 200 OK with full
    # pages that are DUPLICATES of earlier rows (100 distinct ids across 200
    # sampled rows spanning offsets 0-3040). The 08-04 morning fix removed the
    # `offset >= total` break believing results continued past 2,000; they
    # respond, but they repeat. Pagination alone can NEVER see past the
    # window — big tenants need the facet-sliced pass below.
    WD_RESULT_WINDOW = 2000
    # Dedupe within the tenant across the wrap-around AND across facet slices.
    # Also protects the upsert: two identical (job_id, system) rows in one
    # batch would make ON CONFLICT error out ("cannot affect row a second
    # time"), failing the whole 500-row chunk.
    seen_ids = set()

    async def _crawl(applied_facets=None):
        """One paginated sweep; parses into jobs/detail_targets via seen_ids.
        Returns True if it hit the 2,000-result window (i.e. likely truncated)."""
        offset = 0
        # 2026-05-14 quirk preserved: some tenants report the true total only
        # on page 1 and 0 afterwards. Kept for logging; never bounds the loop.
        initial_total = None
        while True:
            try:
                body = {"limit": LIMIT, "offset": offset, "searchText": "",
                        "locations": [], "categories": []}
                if applied_facets:
                    body["appliedFacets"] = applied_facets
                async with req(session, "post", working_url, json=body,
                    headers={**HEADERS, "Content-Type": "application/json"},
                    ssl=False, proxy=proxies.get(),
                    timeout=aiohttp.ClientTimeout(total=25)) as r:
                    if r.status != 200:
                        return False
                    data = await r.json()
                listings = data.get("jobPostings", [])
                if not listings:
                    return False
                page_total = data.get("total", 0) or 0
                if initial_total is None and page_total > 0:
                    initial_total = page_total
                for j in listings:
                    loc = j.get("locationsText", "")
                    _city, _state = parse_city_state(loc)
                    if system in WD_CITY_SPACE_STATE:
                        _city, _state = _wd_city_space_state(loc, _city, _state)
                    _facility = system
                    if tenant == "montefiore":
                        _facility, _city, _state = _montefiore_loc(loc, _city, _state)
                    elif system in WD_FACILITY_MAP:
                        _fac, _city, _state = _wd_facility(system, loc, _city, _state)
                        if _fac:
                            _facility = _fac
                    # job_id (2026-05-29): first digit-bearing bulletField is the
                    # req number (some tenants put the state in [0]); fall back to
                    # the always-unique externalPath.
                    _bf = j.get("bulletFields") or []
                    _jid = next((str(b).strip() for b in _bf if _WD_REQ_ID_RE.match(str(b).strip())), "")
                    if not _jid:
                        # 2026-09-17: only a bullet shaped like a req number
                        # ("JR11450", "R-53741", "JobReq0059731", "202500779")
                        # may be the id; an address or "Posted 30+ Days Ago"
                        # bullet used to be picked because it carried a digit,
                        # collapsing whole tenants (HonorHealth 521 -> 135).
                        _jid = j.get("externalPath", "") or (j.get("title", "") + loc)
                    if _jid in seen_ids:
                        continue
                    seen_ids.add(_jid)
                    jobs.append(Job(
                        title=j.get("title", ""),
                        hospital_system=system,
                        hospital_name=_facility,
                        city=_city,
                        state=_state,
                        location=loc,
                        specialty=(j.get("categories") or [{}])[0].get("name", ""),
                        job_type=j.get("timeType", ""),
                        # 2026-07-01: externalPath often already starts "/job/";
                        # collapse "/job//job/" or ~84% of apply links break.
                        url=(working_url.replace("/wday/cxs/"+tenant+"/","/").replace("/jobs","")
                             + "/job/" + j.get("externalPath","")).replace("/job//job/", "/job/"),
                        job_id=_jid,
                        posted_date=j.get("postedOn", ""),
                        description=strip_html(str(j.get("jobDescription", ""))),
                        ats_platform="Workday",
                    ))
                    _ext = j.get("externalPath", "")
                    if _ext:
                        detail_targets.append((jobs[-1], _ext))
                offset += LIMIT
                if len(listings) < LIMIT:
                    return False           # short page — genuine end
                if offset >= WD_RESULT_WINDOW:
                    return True            # hit the window — truncated
                await jitter()
            except Exception as e:
                logger.info(f"Workday {system}: {e}")
                return False

    hit_window = await _crawl()

    # ── Facet-sliced recovery (2026-08-04) ────────────────────────────────
    # Only fires when the plain sweep filled the whole 2,000-result window,
    # which means the tenant almost certainly has more. Re-crawl one facet
    # value at a time; each slice gets its own 2,000-row window, and seen_ids
    # collapses the overlap. Advocate measured ~5,098 real openings behind a
    # "total" of 2,000. Facet preference: jobFamilyGroup splits finest
    # (25 values, largest 1,951 at Advocate); timeType (2 values) is the
    # last-resort coarse split.
    if hit_window:
        try:
            async with req(session, "post", working_url,
                json={"limit": 1, "offset": 0, "searchText": "", "appliedFacets": {}},
                headers={**HEADERS, "Content-Type": "application/json"},
                ssl=False, proxy=proxies.get(),
                timeout=aiohttp.ClientTimeout(total=25)) as r:
                facet_data = await r.json() if r.status == 200 else {}
        except Exception:
            facet_data = {}
        facets = {f.get("facetParameter"): [v.get("id") for v in (f.get("values") or []) if v.get("id")]
                  for f in (facet_data.get("facets") or [])}
        slice_param = next((p for p in ("jobFamilyGroup", "jobFamily", "timeType")
                            if facets.get(p)), None)
        if slice_param:
            before = len(jobs)
            truncated_slices = 0
            for vid in facets[slice_param]:
                if await _crawl({slice_param: [vid]}):
                    truncated_slices += 1
                await jitter()
            logger.info(f"  Workday {system}: window hit — facet-sliced by {slice_param} "
                        f"({len(facets[slice_param])} slices) recovered {len(jobs)-before} more jobs"
                        + (f"; {truncated_slices} slices ALSO hit the window (still truncated)"
                           if truncated_slices else ""))
        else:
            logger.info(f"Workday {system}: hit the 2,000 window but no usable facet to slice by "
                        f"— inventory beyond 2,000 is unreachable for this tenant")

    # ── Blank-state recovery (2026-09-17): state facet, then tenant default.
    try:
        await _wd_fill_states(session, working_url, system, jobs, detail_targets)
    except Exception as e:
        logger.info(f"Workday {system}: blank-state recovery failed ({e})")

    # Optional second pass — no-op unless WD_FETCH_DESCRIPTIONS=1.
    if WD_FETCH_DESCRIPTIONS and detail_targets:
        try:
            await _workday_fetch_details(session, working_url, detail_targets, system)
        except Exception as e:
            # Descriptions are a bonus; never let them cost us the listings.
            logger.info(f"Workday {system}: detail pass failed ({e}) — keeping list data")
    return jobs

async def run_workday(session) -> list[Job]:
    global _wd_detail_gate
    _wd_detail_gate = asyncio.Semaphore(WD_DESC_GLOBAL_CONCURRENCY)
    logger.info(f"Workday: scraping {len(WORKDAY_TENANTS)} systems... "
                f"(detail budget {WD_DESC_BUDGET.remaining:,}, {'on' if WD_FETCH_DESCRIPTIONS else 'off'})")
    # 2026-09-10 (S-scraper-2): transparency-state tenants first (state from
    # SYSTEM_LOCATION_DEFAULTS; tenants without one keep their order, last).
    # gather() starts tasks in this order, so it is also the order the shared
    # WD_DESC_BUDGET is spent in.
    tenants = priority_states_first(
        list(WORKDAY_TENANTS.items()),
        lambda kv: (SYSTEM_LOCATION_DEFAULTS.get(kv[0].lower()) or ("", ""))[1])
    results = await asyncio.gather(
        *[scrape_workday(session, s, t) for s, t in tenants],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Workday: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  TALEO — Fixed endpoint
# ══════════════════════════════════════════════════════════════════════════
# Removed DNS-dead orgs: hcahealthcare, tenethealth, lifepointhealth, chscare,
#   christushealth, selectmedical, shrinershospitals, nhccare, teamhealth, encompasshealth
# Adding orgs with confirmed *.taleo.net DNS resolution:
TALEO_ORGS: dict[str, str] = {
    # ─────────────────────────────────────────────────────────────────────────
    # All Taleo healthcare tenants are dead as of April 2026.
    #
    # April 27 run results:
    #   DNS-dead (Cannot connect to host):
    #     hcahealthcare, tenethealth, lifepointhealth, chscare, selectmedical,
    #     shrinershospitals, encompasshealth, nhccare, teamhealth
    #   HTTP 404 (host alive, API removed):
    #     erlanger, tgh, capecodhc, hcmc
    #
    # Kept the dict empty (rather than removing) so future Taleo additions can
    # be tested. Run is short-circuited in run_all() — no gather call wasted.
    # ─────────────────────────────────────────────────────────────────────────
}

async def scrape_taleo(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    # Updated Taleo endpoint pattern
    base_url = f"https://{org}.taleo.net"
    try:
        # First get the company code
        async with session.get(
            f"{base_url}/careersection/rest/jobboard/renderRequisitionList",
            params={"lang": "en", "organization": org, "pageNo": 1, "pageSize": 25,
                    "sortField": "POSTING_DATE", "sortDirection": "DESC"},
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Taleo {system}: HTTP {r.status}")
                return []
            data = await r.json(content_type=None)
    except Exception as e:
        logger.info(f"Taleo {system}: {e}")
        return []

    page = 1
    while True:
        try:
            async with session.get(
                f"{base_url}/careersection/rest/jobboard/renderRequisitionList",
                params={"lang": "en", "organization": org, "pageNo": page, "pageSize": 25,
                        "sortField": "POSTING_DATE", "sortDirection": "DESC"},
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200: break
                data = await r.json(content_type=None)
            reqs = data.get("requisitionList", [])
            if not reqs: break
            for j in reqs:
                _tcity = j.get("city", "")
                _tstate = j.get("state", "")
                # Taleo state can be full name ("Texas") — normalize to 2-char
                _, state = parse_city_state(f"{_tcity}, {_tstate}")
                city  = _tcity
                state = state or _tstate
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("organizationName", system),
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=j.get("jobField", ""),
                    job_type=j.get("jobType", ""),
                    url=f"{base_url}/careersection/2/jobdetail.ftl?job={j.get('contestNo','')}",
                    job_id=str(j.get("contestNo", "")),
                    posted_date=j.get("postingDate", ""),
                    description=strip_html(j.get("jobDescription", "")),
                    ats_platform="Taleo",
                ))
            if len(reqs) < 25: break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Taleo {system} page {page}: {e}")
            break
    return jobs

async def run_taleo(session) -> list[Job]:
    if not TALEO_ORGS:
        return []
    logger.info(f"Taleo: scraping {len(TALEO_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_taleo(session, s, o) for s, o in TALEO_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Taleo: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  iCIMS — Correct per-org subdomain API
#  Each org has its own career portal domain backed by iCIMS.
#  The JSON search endpoint: GET /jobs/search?mode=json returns structured data.
#  org_data = full domain of the career portal (no protocol)
# ══════════════════════════════════════════════════════════════════════════
ICIMS_ORGS = {
    # Format: "System": "subdomain.icims.com"
    # All domains verified to use .icims.com subdomain format for JSON API access
    # REMOVED (wrong platform): UPMC (Taleo), Sentara (Workday), Advocate Aurora (Workday),
    #   Northwestern Medicine (SmartRecruiters), HealthPartners (SmartRecruiters)
    # 2026-09-16 (NY coverage): fingerprinted live from the careers pages.
    "Catholic Health (Long Island)": "careers-chsli.icims.com",
    # Garnet, Bassett and Maimonides (added here 2026-09-16) moved to
    # JIBE_SITES the same day: their icims.com portals redirect to Jibe
    # fronts (careers.garnethealth.org, jobs.bassett.org, careers.maimo.org)
    # and the classic portal answers with an empty page or a login gate.
    # MedStar Health moved to JIBE_SITES 2026-09-24: careers.medstarhealth.org
    # is a Jibe front and this entry never wrote a row.
    "Kettering Health":       "careers-ketteringhealth.icims.com",
    "Loma Linda University":  "careers-lluh.icims.com",
    # "Texas Health Resources" moved to FINDLY_CWS_ORGS — uses Findly/m-cloud.io, not iCIMS
    # "Cone Health" REMOVED 2026-05-29: HAR analysis proved careers.conehealth.com is
    #   Phenom (org code CHPCHVUS, POST /widgets), NOT iCIMS. Already in PHENOM_ORGS;
    #   the iCIMS entry only ever returned 0. See note in PHENOM_ORGS.
    "Monument Health":        "careers-monument.icims.com",
    "Owensboro Health":       "careers-owensborohealth.icims.com",
    "Stormont Vail":          "careers-stormontvail.icims.com",
    # ── From URL spreadsheet ──
    "Appalachian Regional Healthcare":  "careers-arh.icims.com",
    "Prime Healthcare":                 "careers-primehealthcare.icims.com",
    "Midland Health":                   "hospital-midlandhealth.icims.com",
    # ── 2026-09-10 Texas block C: Ardent's referrals-only portal never
    # yielded a row; Ardent moved to JIBE_SITES on 2026-09-22.
    # ── 2026-09-22 Texas resume: OakBend (Richmond TX, 158 beds), card-list
    # portal, 168 jobs in the dry run (166 TX, 91 bodies).
    "OakBend Medical Center":           "careers-obmc.icims.com",
    "Covenant Health":                  "careers-covenanthealth.icims.com",
    "Providence Health & Services":     "careers-hub-phs.icims.com",
    "Tri-City Medical Center":          "careers-tricitymed.icims.com",
    "Emory Healthcare":                 "ehccareers-emory.icims.com",
    "St. Luke's Health System":         "careers-slhs.icims.com",
    "Methodist Hospitals":              "careers-methodisthospitals.icims.com",
    "Central Maine Healthcare":         "careers-centralmainehealthcare-ph.icims.com",
    "Tuality Healthcare":               "careers-tuality.icims.com",
    "Legacy Health":                    "careers-lhs.icims.com",
    "OHSU":                             "careersat-ohsu.icims.com",
    # ── Added 2026-05-06: acute-care expansion ──
    # NOTE 2026-05-08: Ascension was ICIMS_ORGS but HAR analysis showed the
    # listings actually come from Phenom (POST jobs.ascension.org/widgets);
    # iCIMS is only used for the apply submit page. Moved to PHENOM_ORGS.
    # MyMichigan: small (~6 hospitals) but fills the empty MI coverage hole.
    "MyMichigan Health":                "careers-mymichigan.icims.com",
    # ── Added 2026-05-06: Home Health / Hospice expansion ──
    # Amedisys: ~530 home-health and hospice locations across the US.
    # Apply links resolve to careersen-amedisys.icims.com (verified via careers
    # page HTML). Same iCIMS pattern as Cone Health etc.
    "Amedisys":                         "careersen-amedisys.icims.com",
    # ── Added 2026-05-13: post-acute expansion Phase 1 (verified) ──
    # Select Medical: nation's largest post-acute care operator. ~100 LTAC
    # hospitals (Select Specialty Hospitals) + outpatient rehab. Confirmed
    # iCIMS tenant via web search — careers.selectmedical.com is a marketing
    # shell that funnels apply clicks to jobs-selectmedicalcorp.icims.com.
    "Select Medical":                   "jobs-selectmedicalcorp.icims.com",
    # Genesis HealthCare: 250+ skilled nursing facilities. Confirmed via
    # genesiscareers.jobs → "Returning Candidate Login" points to careers-genesishcc.icims.com.
    "Genesis HealthCare":               "careers-genesishcc.icims.com",
}


# ── TALENTBREW ─────────────────────────────────────────────────────────────────
# TalentBrew career sites — HTML results endpoint, paginated
TALENTBREW_ORGS = {
    # Format: "System": ("base_url", records_per_page)
    "CommonSpirit Health":      ("https://www.commonspirit.careers/search-jobs", 100),
    # Methodist Healthcare (HCA San Antonio) — REMOVED 2026-07-28. joinmethodist.com
    # is Talemetry, not TalentBrew — the /results endpoint never existed there and
    # this entry never wrote a row. Methodist SA facilities (Methodist Hospital
    # Stone Oak etc.) are covered by the rebuilt run_hca() master crawl.
    # ScionHealth — confirmed TalentBrew (company 40922, tbcdn.talentbrew.com)
    # 61 long-term acute care + 15 community hospitals across 26 states
    "ScionHealth":              ("https://jobs.scionhealth.com/search-jobs", 25),
    # 2026-09-22 Texas resume: University Health, San Antonio (657 beds, the
    # county system). Company 43277; generic module names, h2 cards, pay in
    # the card, no location (SYSTEM_LOCATION_DEFAULTS gives San Antonio TX).
    # 1,031 jobs on the board on 2026-09-22.
    "University Health (San Antonio)": ("https://careers.universityhealth.com/search-jobs", 15),
    # Kaiser Permanente moved to dedicated scrape_kaiser_html adapter
    # 2026-05-26 — the session-based /results JSON endpoint was returning
    # 0 jobs even with warmup. Direct HTML pagination at /search-jobs?p=N
    # works reliably (15 jobs/page, ~34 pages, 510 jobs total).
    # NewYork-Presbyterian REMOVED 2026-08-28: this TalentBrew entry banked 0
    # (same session issue as Kaiser); NYP now flows from its validated
    # Workday tenant (nyp.wd1/nypcareers) in WORKDAY_TENANTS instead.
    # Hackensack Meridian (2026-08-28 resurrection): TalentBrew front over
    # iCIMS; results endpoint validated live, ~1,665 jobs, 15/page.
    "Hackensack Meridian Health": ("https://jobs.hackensackmeridianhealth.org/search-jobs", 15),
    # Mayo Clinic — REMOVED 2026-06-18. This TalentBrew/HTML scrape of
    # jobs.mayoclinic.org returned only ~14 jobs because Mayo migrated to
    # Oracle HCM. Now scraped via ORACLE_ORGS (fa-euwp-saasfaprod1 / Mayo-US,
    # ~1,318 jobs).
    # NOTE (2026-05-29): Enhabit Home Health (TalentBrew company 39891) was here
    # but its /results JSON endpoint returns hasContent:false even after the geo
    # warmup, so the generic adapter yielded 0. Moved to a dedicated HTML
    # adapter (scrape_enhabit_html / run_enhabit) that paginates /search-jobs?p=N.
}



##############################################################################
#  COMMONSPIRIT HEALTH — city slug → state lookup
#  CommonSpirit operates in 21 states. The TalentBrew URL contains city but
#  no state. This map resolves the city slug to a state code.
#  Source: CommonSpirit facility directory (commonspirit.org/locations)
##############################################################################
COMMONSPIRIT_CITY_STATE: dict[str, str] = {
    # Arizona
    "phoenix": "AZ", "chandler": "AZ", "mesa": "AZ", "tempe": "AZ",
    "scottsdale": "AZ", "flagstaff": "AZ", "prescott": "AZ",
    "prescott-valley": "AZ", "sun-city": "AZ", "casa-grande": "AZ",
    "globe": "AZ", "show-low": "AZ", "sierra-vista": "AZ",
    "bullhead-city": "AZ", "lake-havasu-city": "AZ", "kingman": "AZ",
    "parker": "AZ", "wickenburg": "AZ", "yuma": "AZ", "nogales": "AZ",
    "tucson": "AZ", "laveen": "AZ", "gilbert": "AZ", "peoria": "AZ",
    "surprise": "AZ", "glendale": "AZ", "goodyear": "AZ",
    # California
    "bakersfield": "CA", "fresno": "CA", "stockton": "CA",
    "modesto": "CA", "sacramento": "CA", "santa-rosa": "CA",
    "san-jose": "CA", "san-francisco": "CA", "oakland": "CA",
    "redding": "CA", "eureka": "CA", "gilroy": "CA", "hollister": "CA",
    "morgan-hill": "CA", "merced": "CA", "turlock": "CA",
    "los-gatos": "CA", "santa-cruz": "CA", "watsonville": "CA",
    "monterey": "CA", "san-luis-obispo": "CA", "santa-barbara": "CA",
    "ventura": "CA", "oxnard": "CA", "long-beach": "CA",
    "los-angeles": "CA", "burlingame": "CA", "daly-city": "CA",
    "hayward": "CA", "fremont": "CA", "san-leandro": "CA",
    "castro-valley": "CA", "livermore": "CA", "pleasanton": "CA",
    "walnut-creek": "CA", "concord": "CA", "antioch": "CA",
    "pittsburg": "CA", "vallejo": "CA", "napa": "CA", "petaluma": "CA",
    "santa-monica": "CA", "torrance": "CA", "garden-grove": "CA",
    "anaheim": "CA", "corona": "CA", "riverside": "CA",
    "san-bernardino": "CA", "fontana": "CA", "ontario": "CA",
    "rancho-cucamonga": "CA", "palm-springs": "CA", "visalia": "CA",
    "porterville": "CA", "hanford": "CA", "tulare": "CA",
    "woodland": "CA", "chico": "CA", "marysville": "CA",
    # Colorado
    "colorado-springs": "CO", "pueblo": "CO", "denver": "CO",
    "canon-city": "CO", "woodland-park": "CO", "aurora": "CO",
    "colorado-city": "CO",
    # Illinois
    "chicago": "IL", "joliet": "IL", "aurora": "IL", "bolingbrook": "IL",
    "romeoville": "IL", "channahon": "IL", "waukegan": "IL", "elgin": "IL",
    "urbana": "IL", "champaign": "IL", "danville": "IL", "kankakee": "IL",
    "pontiac": "IL", "springfield": "IL", "decatur": "IL",
    "peoria": "IL", "bloomington": "IL", "rockford": "IL",
    "ottawa": "IL", "streator": "IL", "peru": "IL",
    # Indiana
    "hammond": "IN", "munster": "IN", "dyer": "IN", "valparaiso": "IN",
    "crown-point": "IN", "merrillville": "IN", "michigan-city": "IN",
    "la-porte": "IN", "hobart": "IN", "portage": "IN",
    "east-chicago": "IN", "gary": "IN",
    # Iowa
    "iowa-city": "IA", "cedar-rapids": "IA", "davenport": "IA",
    "dubuque": "IA", "waterloo": "IA",
    # Kansas
    "wichita": "KS", "chanute": "KS", "pittsburg": "KS",
    # Kentucky
    "lexington": "KY", "corbin": "KY",
    # Minnesota
    "saint-paul": "MN", "st-paul": "MN", "crookston": "MN",
    "minneapolis": "MN",
    # Montana
    "missoula": "MT", "helena": "MT", "great-falls": "MT",
    "butte": "MT", "billings": "MT", "kalispell": "MT",
    "bozeman": "MT", "miles-city": "MT", "glendive": "MT",
    "havre": "MT", "polson": "MT",
    # Nebraska
    "omaha": "NE", "lincoln": "NE", "hastings": "NE", "kearney": "NE",
    "norfolk": "NE", "mccook": "NE", "alliance": "NE",
    "papillion": "NE", "bellevue": "NE", "grand-island": "NE",
    "north-platte": "NE", "columbus": "NE", "fremont": "NE",
    "york": "NE", "beatrice": "NE",
    # Nevada
    "las-vegas": "NV", "henderson": "NV", "north-las-vegas": "NV",
    "reno": "NV",
    # North Dakota
    "bismarck": "ND", "fargo": "ND", "grand-forks": "ND",
    "minot": "ND", "jamestown": "ND", "devils-lake": "ND",
    "dickinson": "ND", "williston": "ND",
    # Oregon
    "portland": "OR", "eugene": "OR", "bend": "OR", "salem": "OR",
    "corvallis": "OR", "grants-pass": "OR", "medford": "OR",
    "roseburg": "OR", "coos-bay": "OR", "north-bend": "OR",
    "ashland": "OR", "klamath-falls": "OR", "la-grande": "OR",
    "pendleton": "OR", "the-dalles": "OR", "hood-river": "OR",
    # South Dakota
    "sioux-falls": "SD", "aberdeen": "SD", "huron": "SD",
    "watertown": "SD", "mitchell": "SD", "pierre": "SD",
    "yankton": "SD", "vermillion": "SD", "rapid-city": "SD",
    # Tennessee
    "memphis": "TN",
    # Texas
    "houston": "TX", "san-antonio": "TX", "corpus-christi": "TX",
    "victoria": "TX", "laredo": "TX", "waco": "TX",
    # Washington
    "yakima": "WA", "kennewick": "WA", "spokane": "WA",
    "richland": "WA", "walla-walla": "WA", "colville": "WA",
    "omak": "WA", "bridgeport": "WA", "brewster": "WA",
    "prosser": "WA", "sunnyside": "WA", "grandview": "WA",
    "othello": "WA", "pasco": "WA", "moses-lake": "WA",
    "wenatchee": "WA", "ellensburg": "WA",
    # Wisconsin
    "la-crosse": "WA", "neillsville": "WI", "monroe": "WI",
    "sparta": "WI", "onalaska": "WI",
    # Arkansas
    "harrison": "AR",
}
# Also accept the title-cased city name (from .replace("-"," ").title())
_cs_extra = {}
for k, v in COMMONSPIRIT_CITY_STATE.items():
    _cs_extra[k.replace("-", " ").title().lower()] = v
COMMONSPIRIT_CITY_STATE.update(_cs_extra)


async def scrape_talentbrew(session: aiohttp.ClientSession, system: str, base_url: str, rpp: int = 100) -> list[Job]:
    """Scrape a TalentBrew career site via their paginated results endpoint.
    Includes robust retry logic with exponential backoff + proxy rotation for
    connection drops (the CommonSpirit server intermittently drops the TCP
    connection mid-session, typically around page 14 of 48).

    Some TalentBrew tenants (Kaiser, NYP) require a session warmup before the
    /results endpoint returns content. We do a no-op landing GET + the
    SetSearchRequestGeoLocation POST upfront — harmless for tenants that
    don't need it (CommonSpirit, Methodist, ScionHealth).
    """
    jobs = []
    page = 1
    results_url = base_url.rstrip("/") + "/results"
    MAX_RETRIES = 10         # max retries per page before giving up on that page
    BASE_BACKOFF = 3.0       # seconds — doubles each retry

    # ── Session warmup — required by Kaiser / NYP (Apr 29 2026 HAR pattern) ──
    # Sending null/empty geo causes their /results endpoint to respond with
    # `hasContent:false` and a 61-byte shell. The fix (verified live against
    # Kaiser 2026-04-29) is to POST the full location object with a real
    # lat/lon. Chicago coords are a generic safe default — every TalentBrew
    # tenant we've tested replies with full results once a valid location is set.
    _warmup_body = json.dumps({
        "IsInitialRequest": False, "k": None, "kt": 0,
        "l": "Chicago, IL", "lt": 4,
        "lp": "6252001-4896861-4888671-4887398",
        "ac": None, "alp": None, "alt": 0, "r": None, "p": None,
        "lat": 41.8781, "lon": -87.6298,
        "orgIds": None, "shouldRefocusElement": False,
    }).encode("utf-8")
    try:
        async with req(session, "get", base_url,
                       headers={**HEADERS, "Accept": "text/html,*/*"},
                       proxy=proxies.get(), ssl=False,
                       timeout=aiohttp.ClientTimeout(total=20)) as r0:
            await r0.read()  # consume body to fully establish session
        async with req(session, "post",
                       base_url.rstrip("/") + "/SetSearchRequestGeoLocation",
                       data=_warmup_body,
                       headers={**HEADERS,
                                "X-Requested-With": "XMLHttpRequest",
                                "Accept": "application/json",
                                "Content-Type": "application/json",
                                "Origin": base_url.rstrip("/").rsplit("/", 1)[0]
                                          if "/" in base_url else base_url,
                                "Referer": base_url},
                       proxy=proxies.get(), ssl=False,
                       timeout=aiohttp.ClientTimeout(total=15)) as r0:
            await r0.read()
    except Exception as e:
        logger.info(f"TalentBrew {system}: warmup failed (non-fatal): {e}")

    # 2026-09-22 (University Health San Antonio): some TalentBrew sites name
    # their modules generically; the "Section 6" names return hasJobs=true
    # with an empty results fragment. Page 1 is retried with the generic
    # names before giving up.
    module_sets = [("Section 6 - Search Results List", "Section 6 - Search Filters"),
                   ("Search Results", "Search Filters")]
    mod_idx = 0
    retry_modules = False
    while True:
        params = {
            "ActiveFacetID": "0",
            "CurrentPage": str(page),
            "RecordsPerPage": str(rpp),
            "TotalContentResults": "",
            "Distance": "50",
            "RadiusUnitType": "0",
            "Keywords": "",
            "Location": "",
            "ShowRadius": "False",
            "IsPagination": "True" if page > 1 else "False",
            "CustomFacetName": "",
            "FacetTerm": "",
            "FacetType": "0",
            "SearchResultsModuleName": module_sets[mod_idx][0],
            "SearchFiltersModuleName": module_sets[mod_idx][1],
            "SortCriteria": "0",
            "SortDirection": "0",
            "PostalCode": "",
            "TotalContentPages": "0",
            "SearchType": "5",
            "ResultsType": "0",
            "fc": "", "fl": "", "fcf": "", "afc": "", "afl": "", "afcf": "",
        }

        attempt = 0
        page_succeeded = False

        while attempt <= MAX_RETRIES:
            try:
                async with req(session, "get", results_url, params=params,
                               headers={**HEADERS, "X-Requested-With": "XMLHttpRequest",
                                        "Accept": "text/html,*/*"},
                               proxy=proxies.get(),
                               timeout=aiohttp.ClientTimeout(total=90)) as r:
                    if r.status != 200:
                        logger.info(f"TalentBrew {system}: HTTP {r.status} on page {page}, retry {attempt}/{MAX_RETRIES}")
                        raise Exception(f"HTTP {r.status}")  # trigger retry logic
                    html = await r.text()

                # Parse response — JSON envelope wrapping HTML fragment
                try:
                    data = json.loads(html)
                    results_html = data.get("results", "")
                    has_jobs = data.get("hasJobs", False)
                except Exception:
                    results_html = html
                    has_jobs = True

                if not has_jobs or not results_html:
                    if page == 1 and mod_idx == 0 and not jobs:
                        logger.info(f"TalentBrew {system}: empty with the Section 6 module names; retrying page 1 with the generic names")
                        mod_idx, retry_modules = 1, True
                        break
                    logger.info(f"TalentBrew {system}: hasJobs={has_jobs}, empty on page {page} — done")
                    return jobs

                # ── Card-level parse (2026-07-28 rewrite) ─────────────────
                # Each result card carries everything we need — no more city-slug
                # map, no more hardcoded domain:
                #   <a class="search-results-list__job-link" href="/job/..."
                #      data-job-id="98433922176">Medical Surgical Nurse</a>
                #   <li class="... job-department"> Med/Surg </li>
                #   <li class="... job-facility"> St Luke&#x27;s Health - Memorial - Lufkin </li>
                #   <li class="... job-location"> Lufkin, TX </li>
                # Verified across all 5,319 CommonSpirit cards: zero missing a
                # 2-letter state, zero missing a facility element.
                origin_m = re.match(r"https?://[^/]+", base_url)
                origin = origin_m.group(0) if origin_m else "https://www.commonspirit.careers"

                # 2026-09-22 (University Health San Antonio): the other card
                # shape puts the title in <h2>, the facility in a
                # job-companyName span, the pay in a job-salary span, and no
                # location at all (the city is the URL slug; the state comes
                # from SYSTEM_LOCATION_DEFAULTS at normalize time).
                card_matches = re.finditer(
                    r'href="(/job/[^"]+)"[^>]*data-job-id="(\d+)"[^>]*>\s*(?:<h2>)?([^<]*)'
                    r'(.*?)(?=<a class="search-results-list__job-link"|<li>\s*<a href="/job/|\Z)',
                    results_html, re.S
                )
                seen = set()
                for cm in card_matches:
                    url_path, job_id, title, tail = cm.groups()
                    if job_id in seen:
                        continue
                    seen.add(job_id)
                    title = htmllib.unescape(title).strip()
                    if not title:
                        continue

                    fac_m = (re.search(r'job-facility">\s*([^<]*?)\s*</li>', tail)
                             or re.search(r'job-companyName">\s*([^<]*?)\s*</span>', tail))
                    loc_m = re.search(r'job-location">\s*([^<]*?)\s*</(?:li|span)>', tail)
                    sal_m = re.search(r'job-salary">\s*\$?\s*([\d,]+(?:\.\d+)?)\s*(?:-|–|—|to)\s*\$?\s*([\d,]+(?:\.\d+)?)', tail)
                    card_wage = _wage_pair(_wage_num(sal_m.group(1)), _wage_num(sal_m.group(2))) if sal_m else None
                    facility = htmllib.unescape(fac_m.group(1)).strip() if fac_m else ""
                    # Corporate/remote roles carry the generic system name
                    if not facility or facility.lower() == system.lower():
                        facility = system

                    # Location is "City, ST" or "City, County, ST" — state is
                    # ALWAYS the last comma segment (344/5319 cards are 3-part,
                    # e.g. "Bryan, Brazos, TX"; a naive 2-split would store the
                    # county as the state).
                    city_name, city_state = "", ""
                    if loc_m:
                        loc = htmllib.unescape(loc_m.group(1)).strip()
                        parts = [p.strip() for p in loc.split(",") if p.strip()]
                        if parts:
                            city_name = parts[0]
                            last = parts[-1]
                            if len(last) == 2 and last.isalpha():
                                city_state = last.upper()
                    if not city_name:
                        # Fall back to the URL slug for city
                        slug_m = re.match(r"/job/([^/]+)/", url_path)
                        if slug_m:
                            city_name = slug_m.group(1).replace("-", " ").title()
                    if not city_state:
                        # Legacy slug map as last resort (kept for parse regressions)
                        city_state = COMMONSPIRIT_CITY_STATE.get(city_name.lower().replace(" ", "-"), "")

                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=facility,
                        city=city_name,
                        state=city_state,
                        location=f"{city_name}, {city_state}" if city_state else city_name,
                        specialty="",
                        job_type="",
                        url=f"{origin}{url_path}",
                        job_id=job_id,
                        posted_date="",
                        description="",
                        ats_platform="TalentBrew",
                        wage_min=card_wage[0] if card_wage else None,
                        wage_max=card_wage[1] if card_wage else None,
                        wage_unit=card_wage[2] if card_wage else None,
                    ))

                if not seen:
                    if page == 1 and mod_idx == 0 and not jobs:
                        logger.info(f"TalentBrew {system}: no cards with the Section 6 module names; retrying page 1 with the generic names")
                        mod_idx, retry_modules = 1, True
                        break
                    logger.info(f"TalentBrew {system}: no job cards on page {page} — done")
                    return jobs

                logger.info(f"TalentBrew {system}: page {page} → {len(seen)} jobs (total so far: {len(jobs)})")
                page_succeeded = True

                if len(seen) < rpp:
                    return jobs  # last page — we're done
                break  # success — move to next page

            except Exception as e:
                err_str = str(e).lower()
                attempt += 1
                # Retryable: any TCP/SSL connection failure, timeout, or incomplete read
                is_retryable = any(kw in err_str for kw in [
                    "connect", "timeout", "payload", "incomplete",
                    "reset", "broken pipe", "eof", "ssl", "timed out"
                ])
                if is_retryable and attempt <= MAX_RETRIES:
                    backoff = BASE_BACKOFF * (2 ** (attempt - 1))  # 2, 4, 8, 16, 32, 64s
                    logger.info(f"TalentBrew {system}: page {page} connection error ({e}) — retry {attempt}/{MAX_RETRIES} in {backoff:.0f}s (new proxy)")
                    await asyncio.sleep(backoff)
                    # proxies.get() will automatically rotate to next proxy on next call
                else:
                    logger.info(f"TalentBrew {system}: page {page} failed after {MAX_RETRIES} retries — stopping at {len(jobs)} jobs")
                    return jobs  # give up on this system

        if retry_modules:
            retry_modules = False
            continue                      # same page, generic module names

        if not page_succeeded:
            return jobs

        page += 1
        await jitter()

    return jobs


async def run_talentbrew(session: aiohttp.ClientSession) -> list[Job]:
    logger.info(f"TalentBrew: scraping {len(TALENTBREW_ORGS)} systems...")
    async def _one(sys, url, rpp):
        jobs = await scrape_talentbrew(session, sys, url, rpp)
        # 2026-09-21: the detail pass lives here because scrape_talentbrew
        # returns from inside its paging loop. Job pages carry a JSON-LD
        # JobPosting with the body, employment type, date and often pay.
        if DETAIL_FETCH and jobs:
            try:
                await _detail_pass(session, sys, jobs, TB_DESC_BUDGET,
                                   lambda j: _jsonld_detail(session, j), "TalentBrew")
            except Exception as e:
                logger.info(f"TalentBrew {sys}: detail pass failed ({e})")
        return jobs
    tasks = [_one(sys, url, rpp) for sys, (url, rpp) in TALENTBREW_ORGS.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_jobs = []
    total = 0
    for (sys, _), result in zip(TALENTBREW_ORGS.items(), results):
        if isinstance(result, Exception):
            logger.info(f"  TalentBrew {sys}: ERROR {result}")
        else:
            logger.info(f"  TalentBrew {sys}: {len(result)} jobs")
            total += len(result)
            all_jobs.extend(result)
    logger.info(f"  TalentBrew total: {total} jobs")
    return all_jobs


# ── Kaiser Permanente (dedicated HTML adapter) ────────────────────────────
# Kaiser's job board is at www.kaiserpermanentejobs.org and uses TalentBrew
# (company 641) for both rendering and behind-the-scenes data. The standard
# TalentBrew /results JSON endpoint requires a session warmup that's flaky.
# We bypass it entirely by paginating /search-jobs?p={N} and parsing the
# rendered HTML directly - works without proxies, returns 15 jobs per page,
# total ~34 pages for ~510 jobs as of 2026-05-26.
KAISER_BASE = "https://www.kaiserpermanentejobs.org"
KAISER_JOB_PATTERN = re.compile(r'href="(/job/([^/]+)/([^/]+)/641/(\d+))"')
# Title extraction: the <a href> is followed by a <span class="job-title">title</span>
# Quick fallback: derive from URL slug if span lookup fails.
KAISER_TITLE_NEAR_HREF = re.compile(
    r'href="[^"]*?/641/(\d+)"[^>]*>(?:\s*<[^>]+>)*\s*([^<\n]{3,150}?)\s*<'
)
# City -> state for Kaiser locations. Built from observed locations + the
# CommonSpirit map; Kaiser is heaviest in CA, with Mid-Atlantic, NW, and HI.
KAISER_CITY_STATE = {
    # California (overlap with CommonSpirit map but added for safety)
    "union-city": "CA", "pleasanton": "CA", "vista": "CA", "downey": "CA",
    "san-diego": "CA", "anaheim": "CA", "irvine": "CA", "santa-clara": "CA",
    "sunnyside": "WA", "panorama-city": "CA", "fontana": "CA",
    "south-san-francisco": "CA", "harbor-city": "CA", "west-los-angeles": "CA",
    "baldwin-park": "CA", "woodland-hills": "CA", "antioch": "CA",
    "redwood-city": "CA", "vallejo": "CA", "fremont": "CA", "fresno": "CA",
    "roseville": "CA", "sacramento": "CA", "san-jose": "CA",
    "santa-rosa": "CA", "san-rafael": "CA", "richmond": "CA",
    "oakland": "CA", "south-sacramento": "CA", "modesto": "CA",
    "stockton": "CA", "manteca": "CA", "tracy": "CA",
    # Mid-Atlantic (Mid-Atlantic permanente group)
    "rockville": "MD", "gaithersburg": "MD", "silver-spring": "MD",
    "largo": "MD", "kensington": "MD", "manassas": "VA",
    "tysons-corner": "VA", "fairfax": "VA", "alexandria": "VA",
    "burke": "VA", "reston": "VA", "springfield": "VA",
    "halethorpe": "MD", "frederick": "MD", "camp-springs": "MD",
    # Northwest
    "portland": "OR", "salem": "OR", "longview": "WA", "vancouver": "WA",
    # Colorado
    "denver": "CO", "lakewood": "CO", "westminster": "CO", "aurora": "CO",
    "lone-tree": "CO", "loveland": "CO", "wheat-ridge": "CO",
    # Hawaii
    "honolulu": "HI", "wailuku": "HI", "lihue": "HI",
    # Georgia
    "atlanta": "GA", "duluth": "GA", "stockbridge": "GA", "lawrenceville": "GA",
    # Washington state (Group Health legacy)
    "seattle": "WA", "renton": "WA", "redmond": "WA", "tacoma": "WA",
    "olympia": "WA", "everett": "WA", "bellevue": "WA",
}


async def scrape_kaiser_html(session: aiohttp.ClientSession) -> list[Job]:
    """Paginate Kaiser's /search-jobs?p=N and parse jobs out of the rendered HTML."""
    SYSTEM = "Kaiser Permanente"
    MAX_PAGES = 100                  # 510 jobs / 15 per page = 34, but real total ~900+; cap high
    EXPECTED_PER_PAGE = 15
    jobs: list[Job] = []
    seen_ids: set[str] = set()
    empty_pages_in_a_row = 0
    for page in range(1, MAX_PAGES + 1):
        url = f"{KAISER_BASE}/search-jobs?p={page}"
        try:
            async with req(session, "get", url,
                           headers={**HEADERS, "Accept": "text/html,*/*"},
                           timeout=aiohttp.ClientTimeout(total=45)) as r:
                if r.status != 200:
                    logger.info(f"Kaiser: page {page} HTTP {r.status} - stopping")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"Kaiser: page {page} fetch error: {e} - stopping")
            break

        # Extract job links + IDs
        matches = KAISER_JOB_PATTERN.findall(html)
        # Build job_id -> title lookup from the heading-after-href pattern
        title_map = {jid: t.strip() for jid, t in KAISER_TITLE_NEAR_HREF.findall(html)}

        new_this_page = 0
        for url_path, city, title_slug, job_id in matches:
            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            new_this_page += 1
            # Prefer the actual heading title; fall back to slug-derived
            title = title_map.get(job_id) or title_slug.replace("-", " ").title()
            city_name = city.replace("-", " ").title()
            state = KAISER_CITY_STATE.get(city.lower(), "")
            jobs.append(Job(
                title=title,
                hospital_system=SYSTEM,
                hospital_name=SYSTEM,
                city=city_name,
                state=state,
                location=f"{city_name}, {state}" if state else city_name,
                specialty="",
                job_type="",
                url=f"{KAISER_BASE}{url_path}",
                job_id=job_id,
                posted_date="",
                description="",
                ats_platform="TalentBrew",   # underlying ATS - matches reality
            ))

        logger.info(f"Kaiser: page {page} -> {new_this_page} new jobs (total: {len(jobs)})")

        # End conditions:
        #   - page yielded 0 new IDs twice in a row (end-of-results)
        #   - page yielded fewer than half the expected page size (likely last page)
        if new_this_page == 0:
            empty_pages_in_a_row += 1
            if empty_pages_in_a_row >= 2:
                logger.info(f"Kaiser: 2 empty pages in a row - done at page {page}")
                break
        else:
            empty_pages_in_a_row = 0
        if new_this_page < EXPECTED_PER_PAGE // 2 and page > 5:
            logger.info(f"Kaiser: partial page {page} ({new_this_page} jobs) - done")
            break

        await jitter()

    logger.info(f"  Kaiser Permanente: {len(jobs)} jobs")
    return jobs


async def run_kaiser(session: aiohttp.ClientSession) -> list[Job]:
    return await scrape_kaiser_html(session)


# ── UnitedHealth Group TalentBrew (dedicated HTML adapter) ──────────────────
# UHG runs careers.unitedhealthgroup.com on TalentBrew company 34088. Single
# tenant hosts MULTIPLE sub-brands: UnitedHealth Group, Optum, LHC Group
# (home health/hospice, ~32K employees), MedExpress (urgent care), Surgical
# Care Affiliates, Naviguard. ~5,800 jobs as of 2026-05-27.
#
# v1 (this implementation): label every row as "UnitedHealth Group" with
# ats_platform=TalentBrew. Sub-brand splitting via title-keyword classifier
# is a v2 refinement — most jobs don't expose their sub-brand in the
# search-result HTML, so v1 captures volume; v2 captures attribution.
#
# Same pagination pattern as Kaiser: ?p={N}, 15 jobs/page, plain HTML.
# No proxy needed (tested 2026-05-27, direct fetch works).
UHG_BASE = "https://careers.unitedhealthgroup.com"
UHG_JOB_PATTERN = re.compile(r'href="(/job/([^/]+)/([^/]+)/34088/(\d+))"')
UHG_TITLE_NEAR_HREF = re.compile(
    r'href="[^"]*?/34088/(\d+)"[^>]*>(?:\s*<[^>]+>)*\s*([^<\n]{3,150}?)\s*<'
)
# Sub-brand classifier — applied during v2; v1 labels everything UHG.
# Keys are substrings to check in title (lowercase); values are the
# sub-brand to credit. First-match-wins on the order below.
UHG_SUBBRAND_HINTS = [
    ('atrius',     'Optum'),
    ('optum',      'Optum'),
    ('medexpress', 'MedExpress'),
    ('hospice',    'LHC Group'),
    ('home health','LHC Group'),
    ('home-health','LHC Group'),
    ('lhc',        'LHC Group'),
    ('sca ',       'Surgical Care Affiliates'),
    ('surgery center', 'Surgical Care Affiliates'),
    ('naviguard',  'Naviguard'),
]
# US city → state lookup — minimal seed (TalentBrew search URLs include
# city as a slug). When unmapped, state is left blank; the public-board
# search still works from city/title. Expand this dict as gaps surface in
# the logs.
UHG_CITY_STATE_SEED = {
    # Major UHG/Optum hubs
    "eden-prairie": "MN", "minneaponew-york": "NY", "minneapolis": "MN",
    "san-antonio": "TX", "phoenix": "AZ", "san-diego": "CA",
    "boston": "MA", "atlanta": "GA", "charlotte": "NC",
    "chicago": "IL", "denver": "CO", "indianapolis": "IN",
    "irvine": "CA", "los-angeles": "CA", "miami": "FL",
    "houston": "TX", "dallas": "TX", "austin": "TX",
    "tampa": "FL", "orlando": "FL", "philadelphia": "PA",
    "pittsburgh": "PA", "seattle": "WA", "portland": "OR",
    "st-louis": "MO", "kansas-city": "MO", "nashville": "TN",
    "raleigh": "NC", "tucson": "AZ", "salt-lake-city": "UT",
    "everett": "WA", "chelmsford": "MA", "little-rock": "AR",
    # LHC Group footprint (home health/hospice — heavy in South + Midwest)
    "lafayette": "LA", "baton-rouge": "LA", "shreveport": "LA",
    "jackson": "MS", "memphis": "TN", "knoxville": "TN",
    "louisville": "KY", "lexington": "KY", "cincinnati": "OH",
}


async def scrape_uhg_talentbrew(session: aiohttp.ClientSession) -> list[Job]:
    """Paginate UHG's /search-jobs?p=N and parse jobs from the rendered HTML.

    Identical shape to the Kaiser adapter. 5,800+ jobs across UHG sub-brands
    (Optum, LHC Group, MedExpress, etc.) all parented under "UnitedHealth Group"
    in v1. Set the env var UHG_SPLIT_SUBBRANDS=1 to enable title-keyword
    sub-brand attribution (v2; experimental).
    """
    SYSTEM = "UnitedHealth Group"
    MAX_PAGES = 500                  # 5,800 jobs / 15 per page = 387 + safety
    EXPECTED_PER_PAGE = 15
    SPLIT = os.environ.get("UHG_SPLIT_SUBBRANDS") == "1"

    jobs: list[Job] = []
    seen_ids: set[str] = set()
    empty_pages_in_a_row = 0
    subbrand_counts = {}
    for page in range(1, MAX_PAGES + 1):
        url = f"{UHG_BASE}/search-jobs?p={page}"
        try:
            async with req(session, "get", url,
                           headers={**HEADERS, "Accept": "text/html,*/*"},
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    logger.info(f"UHG: page {page} HTTP {r.status} — stopping")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"UHG: page {page} fetch error: {e} — stopping")
            break

        matches = UHG_JOB_PATTERN.findall(html)
        title_map = {jid: t.strip() for jid, t in UHG_TITLE_NEAR_HREF.findall(html)}

        new_this_page = 0
        for url_path, city, title_slug, job_id in matches:
            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            new_this_page += 1
            title = title_map.get(job_id) or title_slug.replace("-", " ").title()
            city_name = city.replace("-", " ").title()
            state = UHG_CITY_STATE_SEED.get(city.lower(), "")
            # Sub-brand attribution (v2 only). Default is parent UHG label.
            system_label = SYSTEM
            if SPLIT:
                tl = title.lower()
                for needle, brand in UHG_SUBBRAND_HINTS:
                    if needle in tl:
                        system_label = brand
                        break
                subbrand_counts[system_label] = subbrand_counts.get(system_label, 0) + 1

            jobs.append(Job(
                title=title,
                hospital_system=system_label,
                hospital_name=system_label,
                city=city_name,
                state=state,
                location=f"{city_name}, {state}" if state else city_name,
                specialty="",
                job_type="",
                url=f"{UHG_BASE}{url_path}",
                job_id=job_id,
                posted_date="",
                description="",
                ats_platform="TalentBrew",
            ))

        logger.info(f"UHG: page {page} -> {new_this_page} new jobs (total: {len(jobs)})")

        # Same end conditions as Kaiser:
        #   - two consecutive empty pages = end of results
        #   - <half a page worth and we're past page 5 = partial last page
        if new_this_page == 0:
            empty_pages_in_a_row += 1
            if empty_pages_in_a_row >= 2:
                logger.info(f"UHG: 2 empty pages in a row - done at page {page}")
                break
        else:
            empty_pages_in_a_row = 0
        if new_this_page < EXPECTED_PER_PAGE // 2 and page > 5:
            logger.info(f"UHG: partial page {page} ({new_this_page} jobs) - done")
            break

        await jitter()

    if SPLIT and subbrand_counts:
        logger.info(f"  UHG sub-brand split: {subbrand_counts}")
    logger.info(f"  UnitedHealth Group (TalentBrew 34088): {len(jobs)} jobs")
    return jobs


async def run_uhg(session: aiohttp.ClientSession) -> list[Job]:
    return await scrape_uhg_talentbrew(session)



# ── Enhabit Home Health & Hospice (dedicated HTML adapter) ──────────────────
# Enhabit runs careers.enhabit.com on TalentBrew company 39891. The standard
# /results JSON endpoint replies hasContent:false / 61-byte shell even after
# the Chicago geo warmup (verified 2026-05-29) — so the generic TalentBrew
# adapter returned 0 for Enhabit. The rendered /search-jobs?p=N HTML, however,
# embeds the job links directly (~16/page, ~1,621 jobs / company id 39891 in
# the URL path), so we paginate the HTML like Kaiser/UHG.
ENHABIT_BASE = "https://careers.enhabit.com"
ENHABIT_JOB_PATTERN = re.compile(r'href="(/job/([^/]+)/([^/]+)/39891/(\d+))"')
ENHABIT_TITLE_NEAR_HREF = re.compile(
    r'href="[^"]*?/39891/(\d+)"[^>]*>(?:\s*<[^>]+>)*\s*([^<\n]{3,150}?)\s*<'
)
# City → state seed (home-health/hospice footprint, heaviest in TX/KS/VA/AZ).
# Unmapped cities leave state blank; the public board still searches by city.
ENHABIT_CITY_STATE_SEED = {
    "hutchinson": "KS", "south-hutchinson": "KS", "wichita": "KS",
    "virginia-beach": "VA", "norfolk": "VA", "richmond": "VA",
    "el-paso": "TX", "mesa": "AZ", "phoenix": "AZ", "tucson": "AZ",
    "dallas": "TX", "fort-worth": "TX", "houston": "TX", "austin": "TX",
    "san-antonio": "TX", "plano": "TX", "arlington": "TX",
    "oklahoma-city": "OK", "tulsa": "OK", "little-rock": "AR",
    "memphis": "TN", "nashville": "TN", "knoxville": "TN",
    "birmingham": "AL", "jackson": "MS", "baton-rouge": "LA",
    "denver": "CO", "colorado-springs": "CO", "boise": "ID",
    "salt-lake-city": "UT", "albuquerque": "NM", "las-vegas": "NV",
}


async def scrape_enhabit_html(session: aiohttp.ClientSession) -> list[Job]:
    """Paginate Enhabit's /search-jobs?p=N and parse jobs from rendered HTML."""
    SYSTEM = "Enhabit Home Health"
    MAX_PAGES = 200                  # ~1,621 jobs / 16 per page ≈ 102 + safety
    EXPECTED_PER_PAGE = 16
    jobs: list[Job] = []
    seen_ids: set[str] = set()
    empty_pages_in_a_row = 0
    for page in range(1, MAX_PAGES + 1):
        url = f"{ENHABIT_BASE}/search-jobs?p={page}"
        try:
            async with req(session, "get", url,
                           headers={**HEADERS, "Accept": "text/html,*/*"},
                           proxy=proxies.get(),
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    logger.info(f"Enhabit: page {page} HTTP {r.status} — stopping")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"Enhabit: page {page} fetch error: {e} — stopping")
            break

        matches = ENHABIT_JOB_PATTERN.findall(html)
        title_map = {jid: t.strip() for jid, t in ENHABIT_TITLE_NEAR_HREF.findall(html)}

        new_this_page = 0
        for url_path, city, title_slug, job_id in matches:
            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            new_this_page += 1
            title = title_map.get(job_id) or title_slug.replace("-", " ").title()
            city_name = city.replace("-", " ").title()
            state = ENHABIT_CITY_STATE_SEED.get(city.lower(), "")
            jobs.append(Job(
                title=title,
                hospital_system=SYSTEM,
                hospital_name=SYSTEM,
                city=city_name,
                state=state,
                location=f"{city_name}, {state}" if state else city_name,
                specialty="",
                job_type="",
                url=f"{ENHABIT_BASE}{url_path}",
                job_id=job_id,
                posted_date="",
                description="",
                ats_platform="TalentBrew",
            ))

        logger.info(f"Enhabit: page {page} -> {new_this_page} new jobs (total: {len(jobs)})")

        if new_this_page == 0:
            empty_pages_in_a_row += 1
            if empty_pages_in_a_row >= 2:
                logger.info(f"Enhabit: 2 empty pages in a row - done at page {page}")
                break
        else:
            empty_pages_in_a_row = 0
        if new_this_page < EXPECTED_PER_PAGE // 2 and page > 5:
            logger.info(f"Enhabit: partial page {page} ({new_this_page} jobs) - done")
            break

        await jitter()

    logger.info(f"  Enhabit Home Health (TalentBrew 39891): {len(jobs)} jobs")
    return jobs


async def run_enhabit(session: aiohttp.ClientSession) -> list[Job]:
    return await scrape_enhabit_html(session)


# ── Maxim Healthcare Services (dedicated HTML adapter) ──────────────────────
# Maxim runs careers.maximhealthcare.com on TalentBrew company 49382. Same
# story as Enhabit: the /results JSON markup didn't parse, but the rendered
# /search-jobs?p=N HTML embeds the job links directly (~19/page, count ~1,826,
# company id 49382 in the URL path). Home health / pediatric homecare /
# staffing, nationwide. Verified 2026-05-29.
MAXIM_BASE = "https://careers.maximhealthcare.com"
MAXIM_JOB_PATTERN = re.compile(r'href="(/job/([^/]+)/([^/]+)/49382/(\d+))"')
MAXIM_TITLE_NEAR_HREF = re.compile(
    r'href="[^"]*?/49382/(\d+)"[^>]*>(?:\s*<[^>]+>)*\s*([^<\n]{3,150}?)\s*<'
)
MAXIM_CITY_STATE_SEED = {
    "zanesville": "OH", "columbus": "OH", "cleveland": "OH", "cincinnati": "OH",
    "roanoke": "VA", "richmond": "VA", "virginia-beach": "VA", "rainelle": "WV",
    "charleston": "WV", "morgantown": "WV", "pittsburgh": "PA", "philadelphia": "PA",
    "baltimore": "MD", "rockville": "MD", "columbia": "MD", "washington": "DC",
    "atlanta": "GA", "charlotte": "NC", "raleigh": "NC", "tampa": "FL",
    "orlando": "FL", "miami": "FL", "jacksonville": "FL", "houston": "TX",
    "dallas": "TX", "san-antonio": "TX", "austin": "TX", "chicago": "IL",
    "detroit": "MI", "boston": "MA", "newark": "NJ", "los-angeles": "CA",
    "san-diego": "CA", "sacramento": "CA", "phoenix": "AZ", "denver": "CO",
}


async def scrape_maxim_html(session: aiohttp.ClientSession) -> list[Job]:
    """Paginate Maxim's /search-jobs?p=N and parse jobs from rendered HTML."""
    SYSTEM = "Maxim Healthcare"
    MAX_PAGES = 250                  # ~1,826 jobs / ~14 unique per page ≈ 130 + safety
    EXPECTED_PER_PAGE = 19
    jobs: list[Job] = []
    seen_ids: set[str] = set()
    empty_pages_in_a_row = 0
    for page in range(1, MAX_PAGES + 1):
        url = f"{MAXIM_BASE}/search-jobs?p={page}"
        try:
            async with req(session, "get", url,
                           headers={**HEADERS, "Accept": "text/html,*/*"},
                           proxy=proxies.get(),
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    logger.info(f"Maxim: page {page} HTTP {r.status} — stopping")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"Maxim: page {page} fetch error: {e} — stopping")
            break

        matches = MAXIM_JOB_PATTERN.findall(html)
        title_map = {jid: t.strip() for jid, t in MAXIM_TITLE_NEAR_HREF.findall(html)}

        new_this_page = 0
        for url_path, city, title_slug, job_id in matches:
            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            new_this_page += 1
            title = title_map.get(job_id) or title_slug.replace("-", " ").title()
            city_name = city.replace("-", " ").title()
            state = MAXIM_CITY_STATE_SEED.get(city.lower(), "")
            jobs.append(Job(
                title=title,
                hospital_system=SYSTEM,
                hospital_name=SYSTEM,
                city=city_name,
                state=state,
                location=f"{city_name}, {state}" if state else city_name,
                specialty="",
                job_type="",
                url=f"{MAXIM_BASE}{url_path}",
                job_id=job_id,
                posted_date="",
                description="",
                ats_platform="TalentBrew",
            ))

        logger.info(f"Maxim: page {page} -> {new_this_page} new jobs (total: {len(jobs)})")

        if new_this_page == 0:
            empty_pages_in_a_row += 1
            if empty_pages_in_a_row >= 2:
                logger.info(f"Maxim: 2 empty pages in a row - done at page {page}")
                break
        else:
            empty_pages_in_a_row = 0
        if new_this_page < EXPECTED_PER_PAGE // 3 and page > 5:
            logger.info(f"Maxim: partial page {page} ({new_this_page} jobs) - done")
            break

        await jitter()

    logger.info(f"  Maxim Healthcare (TalentBrew 49382): {len(jobs)} jobs")
    return jobs


async def run_maxim(session: aiohttp.ClientSession) -> list[Job]:
    return await scrape_maxim_html(session)


# ── 2026-09-10 (Y-texas-build): iCIMS card-list portals ──────────────────
# careers-primehealthcare.icims.com and hospital-midlandhealth.icims.com
# answer the classic /jobs/search?mode=json request with HTTP 200 and the
# portal's HTML card list (<li class="iCIMS_JobCardItem">, 50 a page, pr=N
# paging). The classic path only parsed JSON or data-id attributes, so both
# tenants (configured since May) returned 0 rows every night; Prime alone is
# 5 Texas hospitals. The card carries Facility, title, description, the deep
# link and a <dl> of fields (Job Locations "US-TX-Weslaco").
_ICIMS_CARD_RE = re.compile(r'<li class="iCIMS_JobCardItem">(.*?)</li>', re.DOTALL)
_ICIMS_FIELD_RE = re.compile(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", re.DOTALL)


def _icims_card_location(loc: str) -> tuple[str, str]:
    m = re.match(r"^\s*US-([A-Z]{2})-(.+?)\s*$", loc or "")
    if m:
        return m.group(2).strip(), m.group(1)
    return parse_city_state(loc or "")


# 2026-09-16: card header location ("<span class="sr-only field-label">
# Location</span> <span > US-NY-West Islip</span>"), used by the Catholic
# Health (Long Island) portal, whose <dl> holds only Category / Schedule /
# Shift / FTE / Department.
_ICIMS_HEADER_LOC_RE = re.compile(
    r'field-label">\s*(?:Job\s+)?Locations?\s*</span>\s*<span[^>]*>(.*?)</span>', re.DOTALL)

# 2026-09-17 (blank states): card-list portals with a Facility field but no
# location (Covenant Health, Knoxville): facility substring -> (city, state).
ICIMS_FACILITY_LOC: dict[str, tuple[tuple[str, tuple[str, str]], ...]] = {
    "Covenant Health": (
        ("fort sanders", ("Knoxville", "TN")), ("parkwest", ("Knoxville", "TN")),
        ("methodist medical center", ("Oak Ridge", "TN")), ("leconte", ("Sevierville", "TN")),
        ("morristown", ("Morristown", "TN")), ("roane", ("Harriman", "TN")),
        ("cumberland", ("Crossville", "TN")), ("claiborne", ("Tazewell", "TN")),
        ("fort loudoun", ("Lenoir City", "TN")), ("peninsula", ("Louisville", "TN")),
        ("thompson", ("Knoxville", "TN")), ("covenant", ("Knoxville", "TN")),
    ),
}

# Card-list portals with no Facility field: city -> campus, so the row names
# the hospital the CMS table knows rather than the network.
ICIMS_CITY_FACILITY = {
    "Catholic Health (Long Island)": {
        "West Islip":       "Good Samaritan University Hospital",
        "Rockville Centre": "Mercy Hospital",
        "Smithtown":        "St. Catherine of Siena Hospital",
        "Port Jefferson":   "St. Charles Hospital",
        "Roslyn":           "St. Francis Hospital & Heart Center",
        "Bethpage":         "St. Joseph Hospital",
    },
}


def _parse_icims_cards(text: str, system: str, domain: str) -> list[Job]:
    import html as _html  # card text carries entities (&rsquo;) that strip_html leaves in place
    jobs = []
    for card in _ICIMS_CARD_RE.findall(text):
        m = re.search(r'href="(https?://[^"]+/jobs/(\d+)/[^"]*?)"', card)
        t = re.search(r"<h3[^>]*>(.*?)</h3>", card, re.DOTALL)
        if not m or not t:
            continue
        title = strip_html(t.group(1)).strip()
        if not title:
            continue
        url = m.group(1).replace("&amp;", "&").split("?")[0]
        fac = re.search(r'field-label">\s*Facility\s*</span>\s*<span[^>]*>(.*?)</span>', card, re.DOTALL)
        facility = strip_html(fac.group(1)).strip() if fac else ""
        fields = {strip_html(k).strip().lower(): strip_html(v).strip() for k, v in _ICIMS_FIELD_RE.findall(card)}
        if not facility:
            facility = fields.get("facility", "")   # Covenant Health keeps it in the <dl>
        loc = next((v for k, v in fields.items() if "location" in k), "")
        if not loc:
            hl = _ICIMS_HEADER_LOC_RE.search(card)
            loc = strip_html(hl.group(1)).strip() if hl else ""
        city, state = _icims_card_location(loc)
        if not facility:
            facility = ICIMS_CITY_FACILITY.get(system, {}).get(city, "")
        if not state and facility:
            fl = facility.lower()
            for sub, (fc, fs) in ICIMS_FACILITY_LOC.get(system, ()):
                if sub in fl:
                    city, state = (city or fc), fs
                    break
        d = re.search(r'class="[^"]*\bdescription\b[^"]*"[^>]*>(.*?)</div>', card, re.DOTALL)
        jobs.append(Job(
            title=title, hospital_system=system, hospital_name=facility or system,
            city=city, state=state, location=loc,
            specialty=fields.get("category", ""),
            job_type=fields.get("position type", fields.get("type", "")),
            url=url, job_id=m.group(2),
            posted_date=fields.get("posted date", "")[:10],
            description=_html.unescape(strip_html(d.group(1))).strip() if d else "",
            ats_platform="iCIMS",
        ))
    return jobs


async def _scrape_icims_cards(session: aiohttp.ClientSession, system: str, domain: str, first_text: str) -> list[Job]:
    """Page 0 is the classic response already in hand; pr=1.. until a page
    adds no new ids (200-page cap = 10,000 jobs)."""
    jobs = _parse_icims_cards(first_text, system, domain)
    seen = {j.job_id for j in jobs}
    page = 1
    while jobs and page < 200:
        await jitter()
        try:
            async with req(session, "get", f"https://{domain}/jobs/search",
                           params={"ss": "1", "pr": str(page), "in_iframe": "1", "searchRelation": "keyword_all"},
                           headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
                           proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    break
                text = await r.text()
        except Exception as e:
            logger.info(f"iCIMS {system} cards page {page}: {e}")
            break
        new = [j for j in _parse_icims_cards(text, system, domain) if j.job_id not in seen]
        if not new:
            break
        jobs.extend(new)
        seen.update(j.job_id for j in new)
        page += 1
    logger.info(f"  iCIMS {system}: {len(jobs)} jobs (card list, {page} pages)")
    return jobs


async def _scrape_icims_modern(session: aiohttp.ClientSession, system: str, domain: str) -> list[Job]:
    """Handles newer iCIMS portals that use JavaScript-rendered search pages.
    Fetches the search results page and extracts job data from embedded JSON
    or structured HTML attributes."""
    import json as _json
    jobs = []
    base_url = f"https://{domain}"
    # Modern iCIMS search URL — pr=1 triggers paginated results
    url = f"{base_url}/jobs/search"
    page = 1
    while True:
        try:
            async with req(session, "get", url,
                params={"ss": "1", "pr": str(page), "searchCategory": "", "searchLocation": "", "searchKeyword": ""},
                headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"iCIMS modern {system}: HTTP {r.status}")
                    break
                text = await r.text()

            # Pattern 1: JSON blob embedded in page
            m = re.search(r'icims\.data\s*=\s*(\{.*?"jobs"\s*:\s*\[.*?\].*?\});', text, re.DOTALL)
            if not m:
                m = re.search(r'window\.__ICIMS_DATA__\s*=\s*(\{.*?\});', text, re.DOTALL)
            if m:
                try:
                    data = _json.loads(m.group(1))
                    raw = data.get("jobs", data.get("searchResults", []))
                    if not raw:
                        break
                    for j in raw:
                        loc = j.get("joblocation", j.get("location", ""))
                        _city, _state = parse_city_state(str(loc))
                        jid = str(j.get("jobid", j.get("id", "")))
                        jobs.append(Job(
                            title=j.get("jobtitle", j.get("title", "")),
                            hospital_system=system, hospital_name=j.get("jobcompany", system),
                            city=_city, state=_state, location=str(loc),
                            specialty=j.get("jobcategory", ""), job_type=j.get("jobtype", ""),
                            url=j.get("detailUrl", f"{base_url}/jobs/{jid}/job"),
                            job_id=jid,
                            posted_date=str(j.get("postdate", ""))[:10],
                            description=strip_html(j.get("jobdescription", "")),
                            ats_platform="iCIMS",
                        ))
                    if len(raw) < 25:
                        break
                    page += 1
                    await jitter()
                    continue
                except Exception as e:
                    logger.info(f"iCIMS modern {system}: JSON parse error {e}")

            # Pattern 2: HTML data attributes
            found = re.findall(
                r'data-id="(\d+)"[^>]*data-title="([^"]+)"[^>]*data-location="([^"]*)"',
                text
            )
            if found:
                for jid, title, loc in found:
                    _city, _state = parse_city_state(loc)
                    jobs.append(Job(
                        title=title, hospital_system=system, hospital_name=system,
                        city=_city, state=_state, location=loc,
                        specialty="", job_type="",
                        url=f"{base_url}/jobs/{jid}/job",
                        job_id=jid, posted_date="", description="", ats_platform="iCIMS",
                    ))
                # HTML results are not paginated — check for next page link
                if 'class="iCIMS_Pager"' in text and f'pr={page+1}' in text:
                    page += 1
                    await jitter()
                    continue
            break
        except Exception as e:
            logger.info(f"iCIMS modern {system}: {e}")
            break
    logger.info(f"iCIMS modern {system}: {len(jobs)} jobs")
    return jobs


async def scrape_icims(session: aiohttp.ClientSession, system: str, domain: str) -> list[Job]:
    jobs = []
    base_url = f"https://{domain}"

    # iCIMS has two JSON API patterns depending on portal version:
    # 1. Classic: /jobs/search?mode=json&ss=1&p_startrow=N  (older portals)
    # 2. Modern:  /jobs/search?ss=1&pr=1&searchCategory=&searchLocation=&searchKeyword=  (newer, returns HTML with embedded JSON)
    # Try classic JSON first, fall through to HTML parsing if it fails.

    url = f"{base_url}/jobs/search"
    offset = 0
    while True:
        try:
            async with req(session, "get",
                url,
                params={
                    "ss": "1",
                    "searchKeyword": "",
                    "searchLocation": "",
                    "mode": "json",
                    "iis": "Job+Board",
                    "in_iframe": "1",
                    "p_startrow": offset,
                },
                headers={**HEADERS, "Accept": "application/json, text/html"}, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 404:
                    # Classic JSON API not available — try modern HTML+embedded JSON endpoint
                    logger.info(f"iCIMS {system}: classic API 404, trying modern endpoint")
                    jobs = await _scrape_icims_modern(session, system, domain)
                    return jobs
                if r.status != 200:
                    logger.info(f"iCIMS {system}: HTTP {r.status}")
                    break
                ct = r.headers.get("content-type", "")
                if "json" in ct:
                    data = await r.json(content_type=None)
                    listings = data.get("jobs", data.get("searchResults", []))
                    if not listings:
                        break
                    for j in listings:
                        loc = j.get("joblocation", "") or j.get("location", "")
                        _city, _state = parse_city_state(str(loc))
                        jid = str(j.get("jobid", j.get("id", "")))
                        jobs.append(Job(
                            title=j.get("jobtitle", j.get("title", "")),
                            hospital_system=system,
                            hospital_name=j.get("jobcompany", system),
                            city=_city, state=_state,
                            location=str(loc),
                            specialty=j.get("jobcategory", ""),
                            job_type=j.get("jobtype", ""),
                            url=j.get("detailUrl", f"https://{domain}/jobs/{jid}/job"),
                            job_id=jid,
                            posted_date=str(j.get("postdate", ""))[:10],
                            description=strip_html(j.get("jobdescription", "")),
                            ats_platform="iCIMS",
                        ))
                    if len(listings) < 25:
                        break
                    offset += 25
                else:
                    # HTML fallback — parse structured data from page
                    text = await r.text()
                    # 2026-09-10 (Y-texas-build): card-list portals (see _parse_icims_cards).
                    if "iCIMS_JobCardItem" in text:
                        jobs = await _scrape_icims_cards(session, system, domain, text)
                        break
                    found = re.findall(
                        r'data-id="(\d+)"[^>]*data-title="([^"]+)"[^>]*data-location="([^"]*)"',
                        text
                    )
                    if not found:
                        # Try JSON embedded in page
                        m = re.search(r'window\.__ICIMS_DATA__\s*=\s*(\{.*?\});', text, re.DOTALL)
                        if m:
                            try:
                                import json
                                page_data = json.loads(m.group(1))
                                found_json = page_data.get("jobs", [])
                                for j in found_json:
                                    loc = j.get("location", "")
                                    _city, _state = parse_city_state(loc)
                                    jid = str(j.get("id", ""))
                                    jobs.append(Job(
                                        title=j.get("title", ""),
                                        hospital_system=system, hospital_name=system,
                                        city=_city, state=_state,
                                        location=loc, specialty="", job_type="",
                                        url=f"https://{domain}/jobs/{jid}/job",
                                        job_id=jid, posted_date="", description="",
                                        ats_platform="iCIMS",
                                    ))
                            except: pass
                        break
                    for jid, title, loc in found:
                        _city, _state = parse_city_state(loc)
                        jobs.append(Job(
                            title=title, hospital_system=system, hospital_name=system,
                            city=_city, state=_state,
                            location=loc, specialty="", job_type="",
                            url=f"https://{domain}/jobs/{jid}/job",
                            job_id=jid, posted_date="", description="", ats_platform="iCIMS",
                        ))
                    break
                await jitter()
        except Exception as e:
            logger.info(f"iCIMS {system}: {e}")
            break
    return jobs

async def run_icims(session) -> list[Job]:
    logger.info(f"iCIMS: scraping {len(ICIMS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_icims(session, s, o) for s, o in ICIMS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  iCIMS: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  JIBE (iCIMS Talent Cloud front) — {careers-site}/api/jobs  (added 2026-08-04)
#
#  Distinct from the classic *.icims.com portals scrape_icims() handles: Jibe
#  sites serve a clean JSON API on the branded careers domain itself.
#    GET {base}/api/jobs?page=N&limit=100   -> { jobs: [{data: {...}}], totalCount }
#  limit=100 is honored (default is 10), and the payload includes the FULL job
#  description in-feed (Amedisys sample: 7,206 chars) — no per-job detail
#  fetches needed, which also means these rows clear the sitemap's 200-char
#  indexability bar on day one.
#
#  URL: meta_data.canonical_url is the public posting page (validated live —
#  /jobs/{req_id} 302s to it). Do NOT use apply_url: it points at the iCIMS
#  /login gate, which is exactly the broken-apply-link shape the QA guardrails
#  exist to prevent.
# ══════════════════════════════════════════════════════════════════════════

JIBE_SITES = {
    # Both validated live 2026-08-04: Amedisys totalCount=1175 (home health /
    # hospice — the largest missing non-acute operator), Novant totalCount=1639.
    "Amedisys":      "https://careers.amedisys.com",
    "Novant Health": "https://jobs.novanthealth.org",
    # SNF expansion (2026-08-04): found via the CMS nursing-home chain
    # analysis — Trilogy runs 124 SNF/AL campuses across IN/OH/KY/MI and its
    # careers site is the same Jibe surface. Validated live: totalCount=1560,
    # 7k-char descriptions in-feed.
    "Trilogy Health Services": "https://jobs.trilogyhs.com",
    # ── 2026-08-28 non-acute Bucket A: all four /api/jobs endpoints validated
    # live this session with a plain client (US Renal 436, AccentCare 1,344,
    # RadNet 1,083, Fast Pace 430; full descriptions in-feed as usual).
    "US Renal Care":           "https://careers.usrenalcare.com",
    "AccentCare":              "https://careers.accentcare.com",
    "RadNet":                  "https://careers.radnet.com",
    "Fast Pace Health":        "https://talent.fastpacehealth.com",
    # ── 2026-08-28 dark-systems resurrection: three majors whose Workday
    # tenants were dead turned out to run Jibe fronts; /api/jobs validated
    # live (Fairview 1,262 as M Health Fairview; OSF 1,411; WakeMed 565).
    "Fairview Health":         "https://careers.fairview.org",
    # ── 2026-09-10 Texas block C (Y-texas-build): careers-uhsinc.icims.com
    # answers every search with a redirect script to jobs.uhsinc.com, which
    # is a Jibe front. Validated through scrape_jibe on 2026-09-10: 4,823
    # rows, 831 TX, full descriptions in-feed. The feed carries no facility
    # label (every row is "Universal Health Services"), so the 19 Texas UHS
    # hospitals are linked through hospital_cms_alias (sql/17).
    "Universal Health Services": "https://jobs.uhsinc.com",
    "OSF HealthCare":          "https://www.osfcareers.org",
    "WakeMed":                 "https://jobs.wakemed.org",
    # ── 2026-09-16 New York coverage: the icims.com portals we first
    # configured redirect here. Validated live: Garnet 112 (Middletown +
    # Catskills campuses), Maimonides 175, Bassett 336 (Cooperstown, Fox,
    # O'Connor, Little Falls, Cobleskill). location_name carries the
    # facility, so these three take it as hospital_name (JIBE_FACILITY_NAME).
    "Garnet Health":             "https://careers.garnethealth.org",
    "Maimonides Health":         "https://careers.maimo.org",
    "Bassett Healthcare Network": "https://jobs.bassett.org",
    # ── 2026-09-17 coverage lever 3: /api/jobs validated live (totalCount).
    "Mercy":                     "https://careers.mercy.net",              # 2,193; Chesterfield MO, 66 uncovered CMS hospitals
    "Orlando Health":            "https://careers.orlandohealth.com",      # 1,149
    "UF Health":                 "https://jobs.ufhealth.org",              # 1,249
    "BJC HealthCare":            "https://jobs.bjc.org",                   # 1,191 (Saint Luke's KC rows included since the 2024 merger)
    "Norton Healthcare":         "https://nortonhealthcare.jibeapply.com", # 723, Louisville
    "Sarasota Memorial Health Care System": "https://careers.smh.com",     # 381
    "Tower Health":              "https://www.towercareers.org",           # 348, Reading PA
    # ── 2026-09-22 Texas resume: the referrals-only iCIMS portal never
    # yielded a row; jobs.ardenthealth.com is a Jibe front (validated live:
    # 1,538 jobs, 502 TX / 349 OK / 283 NM / 144 KS / 118 ID / 116 NJ, 7k-char
    # bodies). location_name is the facility ("UT Health Tyler", "BAPTIST
    # CAMPUS", "Seton Harker Heights"), linked to CMS through sql/45.
    "Ardent Health":             "https://jobs.ardenthealth.com",
    # ── 2026-09-24 configs: /api/jobs totalCount probed live (Piedmont 1,727
    # GA; MedStar 1,270 MD/DC, replacing a dead iCIMS entry; UCI 431 CA).
    "Piedmont Healthcare":       "https://join.piedmont.org",
    "MedStar Health":            "https://careers.medstarhealth.org",
    "UCI Health":                "https://jobs.uci.edu",
}

# Jibe feeds whose location_name is a facility (not a street or a region):
# rows take it as hospital_name so the CMS coverage count and the job page
# see the campus, not the network.
JIBE_FACILITY_NAME = {"Garnet Health", "Maimonides Health", "Bassett Healthcare Network",
                      "Mercy", "Orlando Health", "UF Health", "BJC HealthCare", "Norton Healthcare",
                      "Sarasota Memorial Health Care System", "Tower Health", "Ardent Health"}

async def scrape_jibe(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    jobs: list[Job] = []
    page, total = 1, None
    while page <= 60:  # 60 x 100 = 6,000/site ceiling; both sites are well under
        try:
            async with req(session, "get", f"{base_url}/api/jobs",
                           params={"page": str(page), "limit": "100"},
                           headers={**HEADERS, "Accept": "application/json"},
                           ssl=False, proxy=proxies.get(),
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Jibe {system}: HTTP {r.status} at page {page}")
                    break
                data = await r.json(content_type=None)
        except Exception as e:
            logger.info(f"Jibe {system}: page {page} error: {e}")
            break
        items = data.get("jobs") or []
        if total is None:
            total = data.get("totalCount") or 0
        if not items:
            break
        row_errors = 0
        first_err = None
        for wrap in items:
            try:
                j = (wrap or {}).get("data") or wrap
                rid = str(j.get("req_id") or "").strip()
                title = (j.get("title") or "").strip()
                if not rid or not title:
                    continue
                # `state` arrives as a FULL name ("Maryland"); parse_city_state
                # owns the full-name -> 2-letter mapping (its STATE_ABBR table
                # is LOCAL to that function — referencing it from here was a
                # NameError that a bare except silently ate on every row,
                # yielding "0 jobs (site reports 1175)" on the first test run).
                city, st = parse_city_state(
                    f"{(j.get('city') or '').strip()}, {(j.get('state') or '').strip()}")
                meta = j.get("meta_data") or {}
                url = (meta.get("canonical_url")
                       or f"{base_url}/jobs/{rid}")
                cat = j.get("category")
                if isinstance(cat, list):
                    cat = cat[0] if cat else None
                posted = str(j.get("posted_date") or "")[:10]
                facility = (j.get("location_name") or "").strip()
                jobs.append(Job(
                    title=title,
                    hospital_system=system,
                    hospital_name=facility if (system in JIBE_FACILITY_NAME and facility) else system,
                    city=city,
                    state=st,
                    location=", ".join(p for p in (city, st) if p),
                    specialty=str(cat) if cat else "",
                    job_type=(j.get("employment_type") or ""),
                    url=str(url),
                    job_id=rid,
                    posted_date=posted,
                    description=strip_html(str(j.get("description") or "")),
                    ats_platform="iCIMS",
                ))
            except Exception as e:
                # Count and report instead of swallowing: a structural bug
                # (wrong field, missing name) fails EVERY row identically, and
                # a silent continue turns that into "0 jobs" with no clue.
                row_errors += 1
                if first_err is None:
                    first_err = repr(e)
                continue
        if row_errors:
            logger.info(f"Jibe {system}: page {page}: {row_errors} row errors (first: {first_err})")
        if len(items) < 100 or (total and page * 100 >= total):
            break
        page += 1
        await jitter()
    # Dedupe on job_id — Jibe repeats a req across category pages occasionally.
    seen, uniq = set(), []
    for jb in jobs:
        if jb.job_id in seen:
            continue
        seen.add(jb.job_id)
        uniq.append(jb)
    logger.info(f"  Jibe {system}: {len(uniq):,} jobs (site reports {total})")
    return uniq

async def run_jibe(session) -> list[Job]:
    logger.info(f"Jibe: scraping {len(JIBE_SITES)} systems...")
    results = await asyncio.gather(
        *[scrape_jibe(session, s, b) for s, b in JIBE_SITES.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Jibe: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  FINDLY CWS (Careers Widget Service) — jobsapi-internal.m-cloud.io
#
#  Findly is a career-site aggregator that fronts ATS backends (most commonly
#  Taleo) with a clean JSON API. Sites typically have URLs like jobs.{hospital}.org
#  and embed a cws_opts JavaScript config with the org ID.
#
#  Endpoint (confirmed from Texas Health HAR capture, 2026-04):
#    GET https://jobsapi-internal.m-cloud.io/api/job?callback=CWS.jobs.jobCallback
#        &Organization={org_id}&facet[]=ats_portalid:{portal_id}
#        &Limit=100&offset={offset}&sortfield=open_date&sortorder=descending
#
#  Response is JSONP-wrapped: CWS.jobs.jobCallback({ totalHits, queryResult:[...] });
#  Each queryResult item has: id, title, primary_city, primary_state, open_date,
#  description, url, primary_category, brand, shift, job_type, etc.
#
#  No auth required, no cookies, no proxies needed — clean public API.
#  API accepts Limit up to 100 (faster than the website's default of 10).
#
#  Format: "System": (org_id, portal_id)
# ══════════════════════════════════════════════════════════════════════════
FINDLY_CWS_ORGS = {
    # Confirmed from HAR capture of jobs.texashealth.org
    "Texas Health Resources": ("2277", "TexasHealth-Taleo-External"),
    # Add more orgs here as they're discovered. Discovery process:
    #   1. Visit jobs.{hospital}.org/listjobs/ (or similar careers page)
    #   2. View source → find cws_opts JavaScript var
    #   3. Read "org" value and the ats_portalid facet used in their API calls
}


async def scrape_findly(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    """Scrape a Findly CWS career portal. Clean JSONP API, paginated."""
    import re as _re
    org_id, portal_id = org_data
    jobs: list[Job] = []
    offset = 1  # Findly uses 1-indexed offset
    limit = 100  # API max; website uses 10 but the endpoint accepts up to 100
    base_api = "https://jobsapi-internal.m-cloud.io/api/job"

    while True:
        params = {
            "callback": "CWS.jobs.jobCallback",
            "sortfield": "open_date",
            "sortorder": "descending",
            "facet[]": f"ats_portalid:{portal_id}",
            "Limit": str(limit),
            "Organization": org_id,
            "offset": str(offset),
            "useBooleanKeywordSearch": "true",
        }
        try:
            async with req(session, "get", base_api, params=params,
                headers={**HEADERS, "Accept": "*/*", "Referer": f"https://jobsapi-internal.m-cloud.io/"},
                ssl=False, proxy=proxies.get(),
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Findly {system}: HTTP {r.status} at offset {offset}")
                    break
                body = await r.text()
        except Exception as e:
            logger.info(f"Findly {system}: {e}")
            break

        # Strip JSONP wrapper: CWS.jobs.jobCallback({...});
        m = _re.match(r'[^(]*\((.*)\);?\s*$', body, _re.DOTALL)
        inner = m.group(1) if m else body
        try:
            data = json.loads(inner)
        except Exception as e:
            logger.info(f"Findly {system}: JSON parse error: {e}")
            break

        items = data.get("queryResult", []) or []
        total = data.get("totalHits", 0)

        if not items:
            break

        for j in items:
            title = j.get("title", "") or ""
            city = j.get("primary_city", "") or ""
            state = j.get("primary_state", "") or ""
            ref = j.get("ref", "") or str(j.get("id", ""))
            url = j.get("url") or j.get("seo_url") or ""
            open_date = j.get("open_date", "") or ""
            brand = j.get("brand", "") or system  # e.g., "Texas Health HEB"
            jobs.append(Job(
                title=title,
                hospital_system=system,
                hospital_name=brand if brand else system,
                city=city,
                state=state,
                location=f"{city}, {state}".strip(", "),
                specialty=j.get("primary_category", "") or j.get("parent_category", ""),
                job_type=j.get("employment_type", "") or j.get("job_type", ""),
                url=url,
                job_id=ref,
                posted_date=str(open_date)[:10] if open_date else "",
                description=strip_html(j.get("description", "") or ""),
                ats_platform="Findly",
            ))

        offset += limit
        if offset > total:
            break
        await jitter()

    logger.info(f"  Findly {system}: {len(jobs)} jobs")
    return jobs


async def run_findly(session) -> list[Job]:
    logger.info(f"Findly: scraping {len(FINDLY_CWS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_findly(session, s, o) for s, o in FINDLY_CWS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Findly total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  FINDLY GOOGLE CTS — jobsapi-google.m-cloud.io  (NEW — added 2026-04-24)
#
#  Findly's newer backend, built on Google Cloud Talent Solution. Different
#  endpoint, identifier format, and response shape from legacy Findly CWS.
#  AdventHealth runs on this backend; other large systems (Corewell, Baptist
#  South FL candidates, etc.) are likely candidates — check the careers page
#  Network tab for calls to jobsapi-google.m-cloud.io.
#
#  Endpoint (confirmed from jobs.adventhealth.com HAR capture, 2026-04-24):
#    GET https://jobsapi-google.m-cloud.io/api/job/search
#        ?callback=CWS.jobs.jobCallback
#        &companyName=companies/{uuid}                      ← Google CTS identifier
#        &customAttributeFilter=(ats_portalid="X" OR ats_portalid="Y")
#        &pageSize=100&offset={n}
#        &orderBy=posting_publish_time desc
#
#  Response is JSONP-wrapped with totalHits, nextPageToken, searchResults[]:
#    {
#      "totalHits": 4559,
#      "nextPageToken": "...",
#      "searchResults": [
#         { "job": { "title", "ref", "id", "primary_city", "primary_state",
#                    "primary_zip", "primary_country", "description",
#                    "company_name", "primary_category", "ats_portalid", ...}}
#      ]
#    }
#
#  No auth, cookies, or proxies required — clean public JSON.
#
#  Discovery path for new orgs:
#    1. Visit /job-search-results/ (or similar) on the careers domain
#    2. Open DevTools Network, filter for "jobsapi-google"
#    3. From any /api/job/search request, extract:
#         - companyName UUID (e.g. companies/657741e2-...)
#         - ats_portalid values from the customAttributeFilter
#         - the base careers site URL for constructing apply URLs
#
#  Format: "System": (company_uuid, [portal_id, ...], "https://jobs.{domain}")
# ══════════════════════════════════════════════════════════════════════════
FINDLY_GOOGLE_ORGS = {
    "AdventHealth": (
        "657741e2-bfab-4de3-a2e1-660a06974a62",
        # "Manual Postings" portal removed 2026-05-12. Audit found every
        # /job/{numeric}/ URL it produced returns 404 — AdventHealth retired
        # the legacy URL path during their Workday migration. Workday-Mulesoft
        # alone serves the live R-prefixed URLs that resolve.
        ["AdventHealth-Workday-Mulesoft"],
        "https://jobs.adventhealth.com",
    ),
    # 2026-09-10 (Z-texas-acute-D): jobs.utsouthwestern.edu is the same
    # WordPress "cws" plugin (cws_opts.api = jobsapi-google.m-cloud.io,
    # org companies/06f73e7c-...). The unfiltered search answered 839 jobs;
    # an empty portal list means "no customAttributeFilter" (see below).
    # utsw.taleo.net itself is Taleo Enterprise (careersection/2); its REST
    # renderRequisitionList 404s, so this front is the source.
    "UT Southwestern Medical Center": (
        "06f73e7c-038e-4e98-9022-c43f1967ae9c",
        [],
        "https://jobs.utsouthwestern.edu",
    ),
    # ── 2026-09-24 configs: the same WordPress cws plugin (org id read from
    # each site's cws_opts). UPMC's Phenom entry never wrote a row; its Taleo
    # external section redirects to this site. Totals probed live: UPMC
    # 2,857 (PA/MD/NY), University Hospitals 1,229 (OH), UCLA Health 717 (CA).
    "UPMC": (
        "4c0b87d3-a9b3-4243-b9c7-2ad12c533ab3",
        [],
        "https://careers.upmc.com",
    ),
    "University Hospitals": (
        "d424c10c-7f7e-4b63-8d01-0c03089366bb",
        [],
        "https://careers.uhhospitals.org",
    ),
    "UCLA Health": (
        "616bdbc8-cf62-4430-b498-12e49fc71b12",
        [],
        "https://www.uclahealthcareers.org",
    ),
}


async def scrape_findly_google(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    """Scrape a Findly CWS career portal on the Google CTS backend.
    Differs from scrape_findly in endpoint, identifier format, and response shape.

    Routed through DIRECT session (no proxy) — the API is public and the
    response payload (~95KB JSON) is large enough that residential proxies
    routinely truncate it mid-body with "Response payload is not completed".
    Same treatment as Texas Health legacy Findly.
    """
    import re as _re
    company_uuid, portal_ids, base_site = org_data
    jobs: list[Job] = []

    # Build ats_portalid filter — quoted OR chain across all portals
    filter_str = " OR ".join(f'ats_portalid="{p}"' for p in portal_ids)
    attr_filter = f"({filter_str})"

    api = "https://jobsapi-google.m-cloud.io/api/job/search"
    page_size = 100
    offset = 0
    next_page_token: Optional[str] = None

    # Google CTS allows offset-based pagination up to ~5000; beyond that (rare for a
    # single system), we fall through to pageToken-based pagination.
    pages_fetched = 0
    while True:
        params = {
            "callback": "CWS.jobs.jobCallback",
            "pageSize": str(page_size),
            "companyName": f"companies/{company_uuid}",
            **({"customAttributeFilter": attr_filter} if portal_ids else {}),  # 2026-09-10: none for UTSW
            "orderBy": "posting_publish_time desc",
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        else:
            params["offset"] = str(offset)

        # Retry-with-backoff: residential connectivity to m-cloud.io occasionally
        # truncates large JSON payloads. Three tries with exponential backoff.
        body = None
        for attempt in range(3):
            try:
                async with session.get(
                    api, params=params,
                    headers={**HEADERS, "Accept": "*/*",
                             "Referer": f"{base_site}/job-search-results/"},
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as r:
                    if r.status != 200:
                        logger.info(f"FindlyGoogle {system}: HTTP {r.status} at offset {offset}")
                        body = None
                        break
                    body = await r.text()
                    break  # success — exit retry loop
            except Exception as e:
                wait = 2 ** attempt
                logger.info(f"FindlyGoogle {system}: offset {offset} attempt {attempt+1}/3 ({e}) — retry in {wait}s")
                await asyncio.sleep(wait)
        if body is None:
            logger.info(f"FindlyGoogle {system}: failed after 3 attempts at offset {offset} — stopping at {len(jobs)} jobs")
            break

        # Strip JSONP wrapper: CWS.jobs.jobCallback({...});
        m = _re.match(r'[^(]*\((.*)\);?\s*$', body, _re.DOTALL)
        inner = m.group(1) if m else body
        try:
            data = json.loads(inner)
        except Exception as e:
            logger.info(f"FindlyGoogle {system}: JSON parse error: {e} (body len={len(body)})")
            break

        results = data.get("searchResults", []) or []
        total = data.get("totalHits", 0)

        if pages_fetched == 0:
            logger.info(f"FindlyGoogle {system}: totalHits={total}")

        if not results:
            break

        for r_item in results:
            j = r_item.get("job", {}) or {}
            if not isinstance(j, dict):
                continue
            title = j.get("title", "") or ""
            city = j.get("primary_city", "") or ""
            state = j.get("primary_state", "") or ""
            ref = j.get("ref", "") or str(j.get("id", "") or "")
            brand = j.get("company_name", "") or system
            category = j.get("primary_category", "") or ""
            description = j.get("description", "") or ""
            posted_raw = j.get("posting_publish_time", "") or j.get("open_date", "") or ""
            posted = str(posted_raw)[:10] if posted_raw else ""
            # 2026-07-01: AdventHealth retired the /job/{ref}/ path — the
            # link audit found 100% of those URLs return 404. The Google CTS
            # payload already carries the real URLs: `url` is the canonical
            # Findly job page (numeric id + slug), `seo_url` is the direct
            # Workday apply URL. Both validated 200. Prefer the canonical
            # branded `url`, fall back to seo_url, then the legacy pattern.
            url = (j.get("url") or j.get("seo_url")
                   or (f"{base_site}/job/{ref}/" if ref else base_site))

            jobs.append(Job(
                title=title,
                hospital_system=system,
                hospital_name=brand if brand else system,
                city=city,
                state=state,
                location=f"{city}, {state}".strip(", "),
                specialty=category,
                job_type=j.get("employment_type", "") or j.get("job_type", ""),
                url=url,
                job_id=str(j.get("id", "") or ref),
                posted_date=posted,
                description=strip_html(description),
                ats_platform="Findly-Google",
            ))

        pages_fetched += 1
        next_page_token = data.get("nextPageToken")
        offset += page_size

        # Stop conditions:
        #  - fewer results than page_size → last page
        #  - offset ≥ totalHits with no pageToken → done
        #  - safety cap to prevent infinite loop
        if len(results) < page_size:
            break
        if not next_page_token and offset >= total:
            break
        if offset > 20000:
            logger.info(f"FindlyGoogle {system}: hit safety cap at offset {offset}")
            break
        await jitter()

    logger.info(f"  FindlyGoogle {system}: {len(jobs)} jobs")
    return jobs


async def run_findly_google(session) -> list[Job]:
    logger.info(f"FindlyGoogle: scraping {len(FINDLY_GOOGLE_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_findly_google(session, s, o) for s, o in FINDLY_GOOGLE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  FindlyGoogle total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  MAJOR HEALTH SYSTEM CAREER PORTALS (formerly "SuccessFactors")
#  These orgs use custom career portals — scraped via Playwright
#  They are added to CUSTOM_SITES in run_playwright_scrapers()
# ══════════════════════════════════════════════════════════════════════════
SUCCESSFACTORS_ORGS: dict = {}  # Handled via Playwright — see CUSTOM_SITES

async def scrape_successfactors(session, system, org_data) -> list[Job]:
    return []  # These orgs scraped via Playwright

async def run_successfactors(session) -> list[Job]:
    return []  # No-op — these orgs handled by Playwright




# ══════════════════════════════════════════════════════════════════════════
#  GREENHOUSE — Public API (no proxy needed, very reliable)
# ══════════════════════════════════════════════════════════════════════════
GREENHOUSE_ORGS = {
    "One Medical":                 "onemedical",   # ✅ ~269 jobs (April 2026)
    # BAYADA Home Health Care (2026-08-28 non-acute Bucket A): boards-api
    # validated live this session, 2,571 jobs. Their branded site 403s plain
    # clients but the Greenhouse API is open.
    "BAYADA Home Health Care":     "bayada",
    # ── 2026-09-10 telehealth batch (T2). Every token confirmed via boards-api
    # on 2026-09-09 (HTTP 200; whole-board total, then the clinical slice from
    # reports/W-telehealth-targets.md). All are tagged employer_type=telehealth
    # through TELEHEALTH_SYSTEMS and pass the clinical-title and 1099 gates
    # (apply_employer_rules), so only the clinical slice lands.
    "Charlie Health":    "charliehealth",       # 261 total, ~46 clinical: W2 therapists by state, pay posted
    "Equum Medical":     "equummedical",        # 8 total, 5 clinical: tele-ICU, tele-psych, virtual nursing
    "Cadence":           "cadencehealth",       # 15 total, 11 clinical: remote RN/LPN/NP, pay posted
    "Boulder Care":      "bouldercare",         # 18 total, ~11 clinical: telehealth NP by state, pay posted
    "Hazel Health":      "hazel",               # 18 total, ~17 clinical: part-time virtual BH by state
    "Galileo":           "galileo",             # 13 total, ~7 clinical: remote NP by state, TX physicians
    "Ophelia":           "ophelia",             # 12 total, 6 clinical: NP/PA by state, no pay posted
    "Hicuity Health":    "hicuityhealth",       # 7 total, 2 clinical: tele-ICU program LVNs
    "Workit Health":     "workithealth",        # 8 total, 3 clinical
    "Bicycle Health":    "bicyclehealth",       # 11 total, ~3 clinical
    "Spring Health":     "springhealth66",      # 76 total, 5 clinical (engineering board; the gate drops the rest)
    # 1099-only boards, included so the entries exist if the 1099 gate is ever
    # relaxed; with the gate on they contribute 0 rows:
    "Octave":            "octave",              # 29 total, 24 clinical, all 1099
    "Headspace":         "headspaceproviders",  # 7 total, 6 clinical, all 1099
    # ── Removed 2026-04-27: all returned HTTP 404 in production ──
    # "Carbon Health":             "carbonhealth",        # 404
    # "Included Health":           "includedhealth",      # 404
    # "Osmind":                    "osmind",              # 404
    # "Alto Pharmacy":             "alto",                # 404
    # "Brightspring Health":       "brightspringhealth",  # 404
    # "Aveanna Healthcare":        "aveanna",             # 404
    # "BrightSpring":              "brightspring",        # 404
    # "Pediatrix Medical Group":   "pediatrix",           # 404
    # "RadNet":                    "radnet",              # 404
    # Re-add only after confirming via curl https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
}

# 2026-09-22 (Charlie Health lesson): every Greenhouse row was stamped
# "Full-time" whatever the posting said (Charlie Health hires the same role
# full- or part-time). The type now comes from a metadata field when the
# board has one, else the title and the body's labelled lines (normalize_job),
# else stays blank; and a board that publishes pay_input_ranges gives the
# structured figure that always beats regex extraction.
def _greenhouse_job_type(j) -> str:
    for m in j.get("metadata") or []:
        name, value = str(m.get("name") or ""), m.get("value")
        if isinstance(value, str) and value.strip() and re.search(r"employment|job type|time type|schedule|hours type", name, re.I):
            return value.strip()
    return ""


def _greenhouse_pay(j):
    for r in j.get("pay_input_ranges") or []:
        if (r.get("currency_type") or "USD") != "USD":
            continue
        lo, hi = r.get("min_cents"), r.get("max_cents")
        if lo is None or hi is None:
            continue
        got = _wage_pair(lo / 100.0, hi / 100.0)
        if got:
            return got
    return (None, None, None)


async def scrape_greenhouse(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with req(session, "get",
            f"https://boards-api.greenhouse.io/v1/boards/{org}/jobs?content=true&pay_transparency=true",
            headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Greenhouse {system}: HTTP {r.status}")
                return []
            data = await r.json()
        jobs = []
        for j in data.get("jobs", []):
            loc = j.get("location", {}).get("name", "")
            _city, _state = parse_city_state(loc)
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=system,
                city=_city,
                state=_state,
                location=loc,
                specialty=next((d["name"] for d in j.get("departments", []) if d.get("name")), ""),
                job_type=_greenhouse_job_type(j),
                url=j.get("absolute_url", ""),
                job_id=str(j.get("id", "")),
                posted_date=j.get("updated_at", "")[:10],
                description=strip_html(j.get("content", "")),
                ats_platform="Greenhouse",
                wage_min=_greenhouse_pay(j)[0], wage_max=_greenhouse_pay(j)[1], wage_unit=_greenhouse_pay(j)[2],
            ))
        return jobs
    except Exception as e:
        logger.info(f"Greenhouse {system}: {e}")
        return []

async def run_greenhouse(session) -> list[Job]:
    logger.info(f"Greenhouse: scraping {len(GREENHOUSE_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_greenhouse(session, s, o) for s, o in GREENHOUSE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Greenhouse: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  SMARTRECRUITERS
# ══════════════════════════════════════════════════════════════════════════
SMARTRECRUITERS_ORGS = {
    # IDs = company slug from jobs.smartrecruiters.com/{slug}
    "DaVita":               "DaVita",
    "Northwestern Medicine": "northwesternmedicine",
    "HealthPartners":       "HealthPartners1",
    "Envision Healthcare":  "EnvisionHealthcare",
    "AmeriHealth Caritas":  "AmeriHealthCaritas",
    "ChenMed":              "ChenMed",
    "Alignment Healthcare": "AlignmentHealthcare",
    # Added verified SR orgs:
    "Kindred Healthcare":   "KindredatHome",
    "Acadia Healthcare":    "AcadiaHealthcare",
    "Surgery Partners":     "SurgeryPartners",
    # Henry Ford moved off its dead Workday tenant to SmartRecruiters
    # (2026-08-28 resurrection; totalFound=1,689 validated live).
    "Henry Ford Health":    "HenryFordHealth1",
    # IORA Health removed — acquired by One Medical (Amazon)
    # University of Maryland Medical System moved to PHENOM_ORGS 2026-09-24:
    # this company page lists 0 jobs; careers.umms.org is Phenom.
    # 2026-09-24 configs: Munson Healthcare (Traverse City MI), totalFound 660;
    # its Phenom entry never wrote a row.
    "Munson Healthcare":    "MunsonHealthcare1",
    # ── Added 2026-05-29: Phase 3 non-acute expansion (verified SR API 200) ──
    # totalFound validated live 2026-05-29 via probe_ats.py.
    "US Physical Therapy":  "usphysicaltherapy2",   # 1,075 jobs, outpatient PT (~600 clinics)
    "Atria Senior Living":  "AtriaGroupLLC",         # 966 jobs, senior living (~200 communities)
}

async def scrape_smartrecruiters(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs, offset = [], 0
    while True:
        try:
            async with req(session, "get",
                f"https://api.smartrecruiters.com/v1/companies/{org}/postings",
                params={"limit": 100, "offset": offset},
                headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"SmartRecruiters {system}: HTTP {r.status}")
                    break
                data = await r.json()
            listings = data.get("content", [])
            if not listings: break
            for j in listings:
                loc_d  = j.get("location", {})
                city   = loc_d.get("city", "")
                # region is often a full state name ("Illinois") — normalize it
                _, _state = parse_city_state(f"{city}, {loc_d.get('region','')}")
                state  = _state or loc_d.get("region", "")
                jobs.append(Job(
                    title=j.get("name", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=j.get("department", {}).get("label", ""),
                    job_type=j.get("typeOfEmployment", {}).get("label", ""),
                    url=f"https://jobs.smartrecruiters.com/{org}/{j.get('id','')}",
                    job_id=str(j.get("id", "")),
                    posted_date=j.get("releasedDate", "")[:10],
                    description="",
                    ats_platform="SmartRecruiters",
                ))
            offset += 100
            if offset >= data.get("totalFound", 0): break
            await jitter()
        except Exception as e:
            logger.info(f"SmartRecruiters {system}: {e}")
            break
    return jobs

async def run_smartrecruiters(session) -> list[Job]:
    logger.info(f"SmartRecruiters: scraping {len(SMARTRECRUITERS_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_smartrecruiters(session, s, o) for s, o in SMARTRECRUITERS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  SmartRecruiters: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  CONCENTRA  (Sitecore SXA Search)
# ══════════════════════════════════════════════════════════════════════════
# Concentra runs ~520 occupational/urgent-care clinics. Its career site is NOT
# iCIMS — it's a Sitecore SXA "search results" controller. The public JSON API:
#   GET https://www.concentra.com//sxa/search/results/
#       ?s={SCOPE}|{SCOPE}&itemid={ITEMID}&sig=careers&v={VARIANT}
#       &e={offset}&p={pageSize}&g=&o=&q=
# Pagination semantics (verified live 2026-05-29): e = OFFSET (start index),
# p = PAGE SIZE; the server returns items[e : e+p]. Total is in "Count".
# Each result's job fields live in the `Html` blob; `Url`/`Id` are clean.
CONCENTRA_SCOPE   = "{E6F6B861-8426-447A-A003-80760B98B375}"
CONCENTRA_ITEMID  = "{A9A7A019-FD55-4B3A-8BF9-6B439042625B}"
CONCENTRA_VARIANT = "{6DE7E335-E365-482E-B2E4-3E28CE99D128}"
CONCENTRA_PAGE    = 500   # 500 verified safe; server caps very large p
CONCENTRA_BASE    = "https://www.concentra.com"

_CONCENTRA_TITLE_RE    = re.compile(r'<a[^>]*\btitle="([^"]+)"', re.I)
_CONCENTRA_LOCATION_RE = re.compile(r'field-location">([^<]+)<', re.I)
_CONCENTRA_CATEGORY_RE = re.compile(r'field-category[^"]*">([^<]+)<', re.I)
_CONCENTRA_JOBID_RE    = re.compile(r'/(\d+)/?$')

async def scrape_concentra(session: aiohttp.ClientSession) -> list[Job]:
    jobs: list[Job] = []
    offset, total = 0, None
    headers = {
        **HEADERS,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{CONCENTRA_BASE}/careers/career-search/",
    }
    while True:
        params = {
            "s": f"{CONCENTRA_SCOPE}|{CONCENTRA_SCOPE}",
            "itemid": CONCENTRA_ITEMID, "sig": "careers",
            "g": "", "o": "", "q": "",
            "e": str(offset), "p": str(CONCENTRA_PAGE),
            "v": CONCENTRA_VARIANT,
        }
        try:
            async with req(session, "get",
                f"{CONCENTRA_BASE}//sxa/search/results/",
                params=params, headers=headers, ssl=False, proxy=proxies.get(),
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Concentra: HTTP {r.status} at offset {offset}")
                    break
                data = await r.json(content_type=None)
        except Exception as e:
            logger.info(f"Concentra: {e} at offset {offset}")
            break

        if total is None:
            total = data.get("Count", 0) or 0
        results = data.get("Results") or []
        if not results:
            break

        for it in results:
            html = it.get("Html", "") or ""
            mt = _CONCENTRA_TITLE_RE.search(html)
            ml = _CONCENTRA_LOCATION_RE.search(html)
            mc = _CONCENTRA_CATEGORY_RE.search(html)
            title = (mt.group(1) if mt else "").strip()
            if not title:
                continue
            loc_txt = (ml.group(1) if ml else "").strip()
            city, state = parse_city_state(loc_txt)
            specialty = (mc.group(1) if mc else "").replace("-", " ").strip()
            path = it.get("Url", "") or ""
            url = f"{CONCENTRA_BASE}{path}" if path.startswith("/") else path
            mj = _CONCENTRA_JOBID_RE.search(path.rstrip("/") + "/")
            job_id = (mj.group(1) if mj else "") or str(it.get("Id", ""))
            jobs.append(Job(
                title=title,
                hospital_system="Concentra",
                hospital_name="Concentra",
                city=city, state=state, location=loc_txt,
                specialty=specialty, job_type="",
                url=url, job_id=str(job_id),
                posted_date="", description="",
                ats_platform="Concentra",
            ))

        # NOTE: Concentra returns SHORT pages (e.g. 496 for a 500-window) because
        # some index entries are filtered server-side. Do NOT break on a short
        # page — advance by the full window and stop only when offset >= total
        # (or an empty page). Verified 2026-05-29: 496+500+263 = 1,259 jobs.
        offset += CONCENTRA_PAGE
        if total and offset >= total:
            break
        await jitter()
    return jobs

async def run_concentra(session) -> list[Job]:
    logger.info("Concentra: scraping Sitecore SXA career search...")
    jobs = await scrape_concentra(session)
    logger.info(f"  Concentra: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  LEVER
# ══════════════════════════════════════════════════════════════════════════
LEVER_ORGS = {
    # Verified working Lever org IDs (slug from jobs.lever.co/{slug})
    "Tempus AI":            "tempus-ai",
    "Nuvation Bio":         "nuvation-bio",
    # Removed (404): cityblock-health, nomi-health, calibrate
    # Removed 2026-09-10 (HTTP 404 on 2026-09-09): "Brightside Health": "brightside"
    # (placeholder board; clinician recruiting is off-ATS), "Hims & Hers":
    # "hims-hers-1" and "SonderMind": "SonderMind" (both moved to Ashby; see
    # ASHBY_ORGS). No Lever row had ever landed in hospital_jobs.
    # ── 2026-09-10 telehealth batch (T2). Slugs confirmed via
    # api.lever.co/v0/postings on 2026-09-09. Tagged employer_type=telehealth;
    # the clinical-title, 1099 and non-US gates in apply_employer_rules apply.
    "Included Health":   "includedhealth",      # 85 total, ~66 clinical: W2 virtual primary care NP/MD,
                                                # RN care managers. Moved off Greenhouse (that token has
                                                # 404ed since 2026-04). Pay not posted; salaryRange on 4 rows.
    "Ro":                "ro",                  # 47 total, 8 clinical: salaried virtual physicians, seasonal RN
    "Curai Health":      "curai",               # 8 total, 6 clinical: virtual primary care physicians by shift
    "Lyra Health":       "lyrahealth",          # 558 total (484 US), ~397 clinical, ~95% 1099; the 1099 gate
                                                # keeps the ~20 W2 rows (crisis, DBT, telehealth physician)
}

async def scrape_lever(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with req(session, "get",
            f"https://api.lever.co/v0/postings/{org}?mode=json",
            headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Lever {system}: HTTP {r.status}")
                return []
            listings = await r.json()
        jobs = []
        for j in (listings if isinstance(listings, list) else []):
            loc = j.get("categories", {}).get("location", "")
            # 2026-09-10: telehealth boards (Lyra) carry non-US postings and
            # normalize_job only blanks the state, it never drops the row; so
            # drop them here on Lever's own country field.
            if system in TELEHEALTH_SYSTEMS:
                _country = str(j.get("country") or "").strip().upper()
                if _country and _country not in ("US", "USA", "UNITED STATES"):
                    continue
            _city, _state = parse_city_state(loc)
            # Structured salary (2026-08-21): Lever exposes salaryRange
            # {min, max, interval} when the org publishes it. interval is
            # e.g. "per-year-salary" / "per-hour-wage".
            sr = j.get("salaryRange") or {}
            _ival = str(sr.get("interval") or "").lower()
            _unit = "year" if "year" in _ival else ("hour" if "hour" in _ival else None)
            _w = _wage_pair(_wage_num(str(sr.get("min") or "")),
                            _wage_num(str(sr.get("max") or ""))) if _unit else None
            if _w and _w[2] != _unit:
                _w = None
            jobs.append(Job(
                title=j.get("text", ""),
                hospital_system=system,
                hospital_name=system,
                city=_city,
                state=_state,
                location=loc,
                specialty=j.get("categories", {}).get("department", ""),
                job_type=j.get("categories", {}).get("commitment", ""),
                url=j.get("hostedUrl", ""),
                job_id=j.get("id", ""),
                posted_date=str(j.get("createdAt", ""))[:10],
                description=strip_html(j.get("descriptionPlain", "")),
                ats_platform="Lever",
                wage_min=_w[0] if _w else None,
                wage_max=_w[1] if _w else None,
                wage_unit=_w[2] if _w else None,
            ))
        return jobs
    except Exception as e:
        logger.info(f"Lever {system}: {e}")
        return []

async def run_lever(session) -> list[Job]:
    logger.info(f"Lever: scraping {len(LEVER_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_lever(session, s, o) for s, o in LEVER_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Lever: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  ASHBY: public posting API (no key); one GET returns the whole board.
#  2026-09-10 (T2 telehealth batch): the scraper had no Ashby adapter and the
#  two largest W2 telehealth boards (Talkiatry, SonderMind) live here. Field
#  mapping confirmed against the talkiatry feed on 2026-09-09
#  (reports/W-telehealth-scraper-plan.md, section 3c). Counts in the comments
#  are that day's whole-board totals and the clinical slice that survives the
#  telehealth gates (apply_employer_rules, below).
# ══════════════════════════════════════════════════════════════════════════
ASHBY_ORGS = {
    "Talkiatry":   "talkiatry",        # 186 total, ~155 clinical, 100% remote, one posting per
                                       # state; compensationTierSummary on 185 of 186
    "SonderMind":  "sondermind",       # 135 total, ~132 clinical; pay is in the description text
                                       # (0 of 135 structured), so normalize_job's regex does it
    "Brightline":  "hellobrightline",  # 10 total, ~4 clinical (NY, WI)
    "Wheel":       "wheel",            # 5 total, ~3 clinical: multi-state telemedicine physicians
    # Read but not added (0 or 1 clinical on 2026-09-09): rula, grow-therapy,
    # virtahealth, sesame, hinge-health. hims-and-hers: 13 clinical, all
    # in-person pharmacists.
}

# Ashby reports employmentType in camel case; derive_job_type keys on the
# spaced words, so map them.
_ASHBY_EMPLOYMENT = {"fulltime": "Full time", "parttime": "Part time",
                     "contract": "Contract", "temporary": "Temporary", "intern": "Intern"}
_ASHBY_COMP_RX = re.compile(
    r"\$\s*(\d[\d,]*(?:\.\d+)?)\s*([Kk])?\s*(?:-|\u2013|\u2014|to)\s*\$?\s*(\d[\d,]*(?:\.\d+)?)\s*([Kk])?")


def _ashby_wage(summary):
    """(lo, hi, unit) from Ashby's compensationTierSummary, else None so that
    normalize_job falls back to the description regex (the SonderMind case).
    Shapes seen live: "$230K - $260K + Offers Bonus", "$100 - $120 per hour",
    "$85,000 - $95,000", null. The plausibility band (_wage_pair) and the
    wording must agree; "$50 - $60" with no unit stays None."""
    if not summary:
        return None
    s = str(summary)
    m = _ASHBY_COMP_RX.search(s)
    if not m:
        return None
    lo, hi = _wage_num(m.group(1)), _wage_num(m.group(3))
    if lo is None or hi is None:
        return None
    if m.group(2):
        lo *= 1000
    if m.group(4):
        hi *= 1000
    pair = _wage_pair(lo, hi)
    if not pair:
        return None
    hourly = bool(re.search(r"per\s+hour|/\s*hr\b|hourly|an\s+hour", s, re.I))
    return pair if (pair[2] == "hour") == hourly else None


async def scrape_ashby(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with req(session, "get",
            f"https://api.ashbyhq.com/posting-api/job-board/{org}?includeCompensation=true",
            headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Ashby {system}: HTTP {r.status}")
                return []
            data = await r.json()
        jobs = []
        for j in data.get("jobs", []):
            if j.get("isListed") is False:
                continue
            addr = ((j.get("address") or {}).get("postalAddress") or {})
            _city  = (addr.get("addressLocality") or "").strip()
            _state = (addr.get("addressRegion") or "").strip()
            loc = j.get("location") or ""
            if not (_city or _state):
                _city, _state = parse_city_state(loc)
            elif len(_state) > 2:
                # addressRegion is sometimes the full state name
                _state = parse_city_state(f"{_city}, {_state}")[1] or _state
            comp = j.get("compensation") or {}
            _w = _ashby_wage(comp.get("compensationTierSummary"))
            et = re.sub(r"[^a-z]", "", str(j.get("employmentType") or "").lower())
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=system,
                city=_city,
                state=_state,
                location=loc,
                specialty=j.get("department") or j.get("team") or "",
                job_type=_ASHBY_EMPLOYMENT.get(et, str(j.get("employmentType") or "")),
                url=j.get("jobUrl", "") or j.get("applyUrl", ""),
                job_id=str(j.get("id", "")),
                posted_date=str(j.get("publishedAt") or "")[:10],
                description=strip_html(j.get("descriptionHtml") or j.get("descriptionPlain") or ""),
                ats_platform="Ashby",
                wage_min=_w[0] if _w else None,
                wage_max=_w[1] if _w else None,
                wage_unit=_w[2] if _w else None,
            ))
        return jobs
    except Exception as e:
        logger.info(f"Ashby {system}: {e}")
        return []


async def run_ashby(session) -> list[Job]:
    logger.info(f"Ashby: scraping {len(ASHBY_ORGS)} orgs...")
    jobs: list[Job] = []
    # One request per org, sequential with jitter (scrape_ashby never raises).
    for s, o in ASHBY_ORGS.items():
        jobs.extend(await scrape_ashby(session, s, o))
        await jitter()
    logger.info(f"  Ashby: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  CAREERPLUG: franchise job boards, plain HTML, /jobs?page=N pagination.
#  2026-09-10 (T2 urgent-care batch): American Family Care (~250 clinics per
#  afcurgentcare.com and Orbital 2026-06; 26 states) posts franchise-clinic
#  jobs on one shared CareerPlug board (27 pages on 2026-09-10). Corporate
#  roles sit on HRMDirect and physicians go through AHR Staffing; neither is
#  scraped. List-only like the UHG / Kaiser HTML adapters: no description,
#  posted_date empty, the CareerPlug job page is the apply URL.
# ══════════════════════════════════════════════════════════════════════════
CAREERPLUG_ORGS = {
    "American Family Care": "american-family-care-careers",
}
# 2026-09-15 (owner backlog): a budgeted detail pass. The job link 302s to
# /jobs/<id>/apps/new, whose <div class="job-description-container"> holds the
# posting (benefits list, duties, requirements). CP_DESC_BUDGET fetches per
# run, newest first; the DB trigger preserve_scraped_enrichment keeps a
# description once a row has one, so the whole board fills in over a few
# nights without re-fetching.
CP_DESC_BUDGET = 300
_CP_DESC_RX = re.compile(r'<div[^>]+class="[^"]*job-description-container[^"]*"[^>]*>(.*?)<(?:form|div class="apply-page|div class="account_description)', re.S | re.I)


async def _careerplug_detail(session, url: str) -> str:
    """Description text from a CareerPlug apply page, or '' on any failure."""
    try:
        async with req(session, "get", url, headers={**HEADERS, "Accept": "text/html,*/*"},
                       ssl=False, proxy=proxies.get(), allow_redirects=True,
                       timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                return ""
            html = await r.text()
    except Exception:
        return ""
    m = _CP_DESC_RX.search(html)
    if not m:
        return ""
    text = strip_html(m.group(1))
    return text if len(text) >= 80 else ""
_CP_LINK_RX = re.compile(
    r'<a\b[^>]*href="(?:https?://[^"/]+)?/jobs/(\d+)(?:[?#][^"]*)?"[^>]*>(.*?)</a>', re.S | re.I)
# "Knoxville, TN 37919" / "Fort Worth, TX": 1-4 capitalised words before the
# comma; all-caps tokens (PRN, NP) cannot start a word so titles do not bleed in.
_CP_LOC_RX = re.compile(r"((?:[A-Z][a-z][A-Za-z.'\-]*\s){0,3}[A-Z][a-z][A-Za-z.'\-]*),\s*([A-Z]{2})\b(?:\s+\d{5})?")


_CP_CARD_RX = re.compile(
    r"^(?P<title>.*?)\s*Location:\s*(?P<st>[A-Z]{2})-(?P<city>.+?)(?:-(?P<zip>\d{5}))?"
    r"(?:\s+Post Date:\s*(?P<mm>\d{2})-(?P<dd>\d{2})-(?P<yy>\d{2}))?\s*$", re.S)


def _cp_text(fragment: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", fragment or "")).strip()


def _cp_parse_card(text: str, tail_text: str):
    """(title, city, state, posted_date) from a CareerPlug card. AFC's anchor
    text reads "NP/PA PRN Location: TN-Powell-37849 Post Date: 09-10-26"
    (seen 2026-09-10); other boards put "City, ST" after the title, inside
    or after the anchor, so those two shapes are the fallbacks."""
    m = _CP_CARD_RX.match(text)
    if m and m.group("st") in _US_STATE_SET:
        posted = f"20{m.group('yy')}-{m.group('mm')}-{m.group('dd')}" if m.group("yy") else ""
        return (m.group("title").strip(" -|,\u2022"), m.group("city").replace("-", " ").strip(),
                m.group("st"), posted)
    lm = _CP_LOC_RX.search(text)
    if lm and lm.group(2) in _US_STATE_SET:
        return (text[:lm.start()].strip(" -|,\u2022"), lm.group(1).strip(), lm.group(2), "")
    lm = _CP_LOC_RX.search(tail_text)
    if lm and lm.group(2) in _US_STATE_SET:
        return (text, lm.group(1).strip(), lm.group(2), "")
    return (text, "", "", "")


async def scrape_careerplug(session: aiohttp.ClientSession, system: str, slug: str) -> list[Job]:
    jobs: list[Job] = []
    seen: set[str] = set()
    base = f"https://{slug}.careerplug.com"
    for page in range(1, 80):
        url = f"{base}/jobs?page={page}"
        try:
            async with req(session, "get", url, headers={**HEADERS, "Accept": "text/html,*/*"},
                           ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status != 200:
                    logger.info(f"CareerPlug {system}: page {page} HTTP {r.status}; stopping")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"CareerPlug {system}: page {page} {e}; stopping")
            break
        found = list(_CP_LINK_RX.finditer(html))
        new = 0
        for k, m in enumerate(found):
            jid = m.group(1)
            if jid in seen:
                continue
            text = _cp_text(m.group(2))
            if not text:
                continue
            tail_end = found[k + 1].start() if k + 1 < len(found) else m.end() + 800
            title, city, state, posted = _cp_parse_card(text, _cp_text(html[m.end():tail_end]))
            if not title:
                continue
            seen.add(jid)
            new += 1
            jobs.append(Job(
                title=title,
                hospital_system=system,
                hospital_name=system,
                city=city,
                state=state,
                location=f"{city}, {state}" if city and state else (state or city),
                specialty="",
                job_type="",
                url=f"{base}/jobs/{jid}",
                job_id=jid,
                posted_date=posted,
                description="",
                ats_platform="CareerPlug",
            ))
        if page == 1 and not found:
            i = html.find("/jobs/")
            logger.info(f"CareerPlug {system}: no rows parsed on page 1 "
                        f"(len {len(html)}, '/jobs/' x{html.count('/jobs/')}); "
                        f"snippet {html[max(0, i - 300):i + 300]!r}")
        if new == 0:
            break
        await jitter()
    # 2026-09-15: detail pass within the budget (newest posted first).
    spent = 0
    for j in sorted(jobs, key=lambda x: x.posted_date or "", reverse=True):
        if spent >= CP_DESC_BUDGET:
            break
        await jitter()
        j.description = await _careerplug_detail(session, j.url)
        spent += 1
    logger.info(f"  CareerPlug {system}: {len(jobs)} jobs, {sum(1 for j in jobs if j.description)} with descriptions ({spent} fetched)")
    return jobs


async def run_careerplug(session) -> list[Job]:
    logger.info(f"CareerPlug: scraping {len(CAREERPLUG_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_careerplug(session, s, o) for s, o in CAREERPLUG_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  CareerPlug: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  EMPLOYER CLASS (hospital_jobs.employer_type), 2026-09-10 (T2 batch)
#  Stamped per hospital_system on every upsert so the column always reflects
#  this config, never a stale manual value; None = hospital or unclassified.
#  The column and its CHECK were applied on the DATA project on 2026-09-10;
#  the CHECK allows 'hospital', 'telehealth', 'urgent_care', 'freestanding_er',
#  'snf', 'home_health', 'dialysis', 'other'. Any other value makes PostgREST
#  reject the whole 500-row chunk, so add DB values before scraper values.
# ══════════════════════════════════════════════════════════════════════════
TELEHEALTH_SYSTEMS = {"Talkiatry", "SonderMind", "Brightline", "Wheel", "Included Health",
                      "Ro", "Curai Health", "Lyra Health", "Charlie Health", "Equum Medical",
                      "Cadence", "Boulder Care", "Hazel Health", "Galileo", "Ophelia",
                      "Hicuity Health", "Workit Health", "Bicycle Health", "Spring Health",
                      "Octave", "Headspace", "Teladoc Health"}
# Urgent-care operators already crawled by their own adapters (active rows on
# 2026-09-10: Concentra 1,280; CityMD 608 under each of its two names; GoHealth
# 494; Fast Pace 450; WellNow 228) plus the T2 additions.
# 2026-09-14 (W-urgentcare) adds NextCare (ADP CX, 104 rows), FastMed Urgent
# Care (ADP WorkforceNow, 41 rows) and Patient First (own WordPress REST, 223
# rows, parsed end to end but shipped OFF — see the Patient First section).
URGENT_CARE_SYSTEMS = {"Concentra", "CityMD", "Summit Health (CityMD)", "GoHealth Urgent Care",
                       "WellNow Urgent Care", "Fast Pace Health", "American Family Care",
                       "NextCare", "FastMed Urgent Care", "Patient First"}
# None scraped yet: the Texas freestanding-ER chains have no readable ATS
# (see reports/T2-telehealth-urgentcare.md); HCA bought 11 SignatureCare ERs
# in 2026 and those ride inside the HCA crawl unlabelled.
FREESTANDING_ER_SYSTEMS: set[str] = set()
# Sub-brands that ride inside a bigger system's crawl and only show in
# hospital_name: HCA's CareNow (121 active rows on 2026-09-10) and MD Now (19).
# Matched as a hospital_name prefix. MedExpress is NOT reachable this way: the
# UHG TalentBrew crawl cannot tell sub-brands apart and its keyword search
# pages are JS-only (checked 2026-09-10).
#
# 2026-09-14 (W-urgentcare): MedExpress needs no adapter at all — the BRAND is
# gone. optum.com serves medexpress.com/careers as a page titled "MedExpress
# Closed in Most States | Request Medical Records", and the UHG sitemap
# (careers.unitedhealthgroup.com/sitemap.xml, 5,519 job URLs, the whole
# board and robots-allowed unlike /search-jobs/) contains zero MedExpress
# URLs and zero MedExpress facets under custom_fields.brand / .entity /
# .division / .uhgoptumsubbrands. The 21 urgent-care job URLs on that board
# belong to Optum NY, Atrius, Reliant ReadyMed and Colonial Healthcare, and
# already ride inside run_uhg. Do not spend Playwright time on this.
URGENT_CARE_NAME_PREFIXES = ("CareNow", "MD Now")


def employer_type_for(system, name=""):
    """Coarse employer class for hospital_jobs.employer_type; None means
    hospital or unclassified. Keyed on the post-alias hospital_system first,
    then on hospital_name prefixes for sub-brands inside a bigger crawl."""
    if system in TELEHEALTH_SYSTEMS:
        return "telehealth"
    if system in URGENT_CARE_SYSTEMS:
        return "urgent_care"
    if system in FREESTANDING_ER_SYSTEMS:
        return "freestanding_er"
    if (name or "").strip().startswith(URGENT_CARE_NAME_PREFIXES):
        return "urgent_care"
    return None


# ── Telehealth row rules (2026-09-10, T2) ───────────────────────────────────
# A telehealth company's board is mostly corporate (Spring Health 5 clinical
# of 76) or 1099 (Lyra ~95%). Two gates, applied only to TELEHEALTH_SYSTEMS,
# keep the board clinical and W2: the clinical-title gate and the 1099 gate.
# derive_job_type has no contractor bucket, so a 1099 posting would surface
# as a W2-looking job with a pay pill; keep the gate on until a contract_1099
# bucket and a card label ship together (owner's call).
TELEHEALTH_DROP_1099 = True
# Title-based on purpose: Ashby employmentType is "Contract" on boards the
# targets report counts as W2 (SonderMind), so the field is not trusted yet.
_RX_1099 = re.compile(r"\b1099\b|independent contractor|\bcontract(?:or)?\b", re.I)
_CLINICAL_WORD_RX = re.compile(
    r"\b(nurse|nursing|physician|doctor|psychiatrist|psychiatry|intensivist|hospitalist|"
    r"cardiologist|neurologist|therapist|therapy|psychologist|counselor|clinician|pharmacist|"
    r"respiratory|dietitian|dietician|nutritionist|medical assistant|health coach|care manager|"
    r"medical director|clinical director|social worker|practitioner|prescriber)\b", re.I)
_CLINICAL_ABBR_RX = re.compile(
    r"\b(RN|LPN|LVN|NP|PA|APRN|PMHNP|FNP|DNP|MD|DO|RT|RD|RDN|LCSW|LPC|LPCC|LMFT|LMHC|LCPC|"
    r"LICSW|LISW|BCBA|CRNA|PsyD)\b")
_CORPORATE_RX = re.compile(
    r"\b(engineer\w*|software|developer|sales|marketing|recruit\w*|finance|financial|accountant|"
    r"accounting|product|design\w*|analyst|analytics|partnership\w*|legal|people|payroll|"
    r"data scientist|growth|account executive|customer success|copywriter|brand|communications|"
    r"revenue cycle|billing|talent acquisition|human resources|procurement|devops|infrastructure|"
    r"operations|relations|credentialing|onboarding|scheduler|enrollment|business)\b", re.I)
# Non-US rows on telehealth boards (Lyra, Teladoc: Spain, Canada). Workday and
# Ashby carry the country only in the location text.
_NON_US_RX = re.compile(
    r"\b(canada|spain|india|united kingdom|england|scotland|ireland|mexico|philippines|germany|"
    r"france|australia|brazil|colombia|argentina|chile|portugal|netherlands|israel|singapore|"
    r"japan|poland|toronto|montreal|ontario|british columbia|barcelona|madrid|london|"
    r"dublin|bangalore|bengaluru|hyderabad|pune|mumbai|chennai|manila|sydney)\b", re.I)
# Workday job paths start with the country ("/job/ES-Barcelona/...", US rows
# are "/job/USA---Any-Location..."), and Teladoc's Spanish titles carry no
# country in the location text at all.
_NON_US_URL_RX = re.compile(r"myworkdayjobs\.com/[^/]+/job/(?!US)[A-Z]{2,3}(?:-|/)")
_NON_US_TITLE_RX = re.compile(r"campa[n\u00f1]a|espa[n\u00f1]a|\bESP\b|\bCanad[a\u00e1]\b|\bUK\b")

# Per-state remote postings name the state in the title ("Psychiatrist -
# Texas", "Virtual Nurse Practitioner - CA License") and often carry no
# address, so normalize_job would leave state blank and the row would never
# reach /jobs/state/tx. Same map as parse_city_state's, kept module-level.
_US_STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}
_US_STATE_SET = set(_US_STATE_CODES.values())
# "(Philadelphia, PA)" / "- MD": a state code after a comma, dash, paren or
# slash is a location, not a credential; stripped before the abbr gate.
_STATE_TOKEN_RX = re.compile(r"[,(\-\u2013\u2014/]\s*(?:" + "|".join(sorted(_US_STATE_SET)) + r")\b")
_STATE_NAME_RX = re.compile(
    r"\b(" + "|".join(sorted((re.escape(n) for n in _US_STATE_CODES), key=len, reverse=True)) + r")\b", re.I)
_DC_RX = re.compile(r"\bwashington,?\s*d\.?\s*c\.?\b", re.I)
# Two-letter codes only in unambiguous positions: "(TX)", "CA License" /
# "CA-licensed", or after a dash / pipe / comma at the end of the title.
_STATE_CODE_RX = re.compile(
    r"\(\s*([A-Z]{2})\s*\)"
    r"|\b([A-Z]{2})(?=\s*[-\u2013\u2014]?\s*(?:License|Licensed|Licensure|Lic\b))"
    r"|[-\u2013\u2014|,/]\s*([A-Z]{2})\s*$")
# Codes that double as credentials (PA, MD) or roles (MA, CT): accept them
# only next to "License".
_STATE_CODE_LICENSE_ONLY = {"PA", "MD", "MA", "CT"}


def _state_from_title(title: str) -> str:
    """Two-letter state from a posting title, or "". Full names win over
    codes; the first match wins on multi-state titles ("Registered Dietitian -
    Pennsylvania / New Jersey" gives PA), which the report notes."""
    t = title or ""
    if _DC_RX.search(t):
        return "DC"
    m = _STATE_NAME_RX.search(t)
    if m:
        return _US_STATE_CODES[m.group(1).lower()]
    for m in _STATE_CODE_RX.finditer(t):
        paren, lic, tail = m.group(1), m.group(2), m.group(3)
        code = paren or lic or tail
        if code not in _US_STATE_SET:
            continue
        if code in _STATE_CODE_LICENSE_ONLY and not lic:
            continue
        return code
    return ""


def apply_employer_rules(job: Job):
    """Telehealth-only row rules, run in run_all before normalize_job. Returns
    the job (state possibly filled from the title) or None to drop it. Rows
    from every other system pass through untouched."""
    if job.hospital_system not in TELEHEALTH_SYSTEMS:
        return job
    t = job.title or ""
    if TELEHEALTH_DROP_1099 and _RX_1099.search(t):
        return None
    if _CORPORATE_RX.search(t):
        return None
    if not (_CLINICAL_WORD_RX.search(t) or _CLINICAL_ABBR_RX.search(_STATE_TOKEN_RX.sub("", t))):
        return None
    if (_NON_US_RX.search(f"{job.location or ''} {job.city or ''}")
            or _NON_US_URL_RX.search(job.url or "") or _NON_US_TITLE_RX.search(t)):
        return None
    if (job.city or "").strip().lower() == "remote":
        job.city = ""
    if not (job.state or "").strip():
        job.state = _state_from_title(t)
    return job


# ══════════════════════════════════════════════════════════════════════════
#  USAJOBS — Free public API
# ══════════════════════════════════════════════════════════════════════════
async def run_usajobs(session) -> list[Job]:
    logger.info("USAJOBS: scraping VA + federal hospitals...")
    jobs = []
    MEDICAL_SERIES = "0600;0601;0602;0610;0620;0630;0640;0645;0646;0647;0648;0649;0660;0670;0675"
    ORGS = [
        ("VA Hospitals",            "VATA"),
        ("Indian Health Service",   "HE38"),
        ("Military Health System",  "DD"),
        ("NIH Clinical Center",     "HE06"),
    ]
    usajobs_key = os.environ.get("USAJOBS_API_KEY", "")
    usajobs_email = os.environ.get("USAJOBS_EMAIL", "")
    usajobs_headers = {
        **HEADERS,
        "Host": "data.usajobs.gov",
        "User-Agent": usajobs_email or "hospitalJobScraper@example.com",
        "Authorization-Key": usajobs_key,
    }
    for system_name, org_code in ORGS:
        try:
            async with session.get(
                "https://data.usajobs.gov/api/search",
                params={"Organization": org_code, "ResultsPerPage": 500, "JobCategoryCode": MEDICAL_SERIES},
                headers=usajobs_headers,
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 401:
                    logger.info(f"USAJOBS: 401 — set USAJOBS_API_KEY and USAJOBS_EMAIL env vars (free at usajobs.gov/Applicant/ProfileDashboard/Home)")
                    break
                if r.status != 200:
                    logger.info(f"USAJOBS {system_name}: HTTP {r.status}")
                    continue
                data = await r.json()
            for item in data.get("SearchResult", {}).get("SearchResultItems", []):
                m = item.get("MatchedObjectDescriptor", {})
                loc = (m.get("PositionLocation") or [{}])[0]
                city  = loc.get("CityName", "")
                state = loc.get("CountrySubDivisionCode", "")
                # Federal postings ALWAYS carry an exact pay range (2026-08-21).
                # RateIntervalCode: PA = Per Annum, PH = Per Hour; anything
                # else (per-day/bi-weekly oddities) is left for the regex path.
                rem = (m.get("PositionRemuneration") or [{}])[0]
                _wmin = _wage_num(str(rem.get("MinimumRange") or ""))
                _wmax = _wage_num(str(rem.get("MaximumRange") or ""))
                _code = str(rem.get("RateIntervalCode") or "").upper()
                _unit = {"PA": "year", "PH": "hour"}.get(_code)
                _w = _wage_pair(_wmin, _wmax) if _unit else None
                if _w and _w[2] != _unit:
                    _w = None
                jobs.append(Job(
                    title=m.get("PositionTitle", ""),
                    hospital_system=system_name,
                    hospital_name=m.get("OrganizationName", system_name),
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=(m.get("JobCategory") or [{}])[0].get("Name", ""),
                    job_type=(m.get("PositionSchedule") or [{}])[0].get("Name", ""),
                    url=m.get("PositionURI", ""),
                    job_id=m.get("PositionID", ""),
                    posted_date=m.get("PublicationStartDate", "")[:10],
                    description=m.get("QualificationSummary", "")[:500],
                    ats_platform="USAJOBS",
                    wage_min=_w[0] if _w else None,
                    wage_max=_w[1] if _w else None,
                    wage_unit=_w[2] if _w else None,
                ))
            await jitter()
        except Exception as e:
            logger.info(f"USAJOBS {system_name}: {e}")

    logger.info(f"  USAJOBS: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PLAYWRIGHT
# ══════════════════════════════════════════════════════════════════════════
##############################################################################
#  PHENOM PEOPLE — CommonSpirit, Baptist Health, Corewell, etc.
#  Phenom renders jobs via JS — no public REST API accessible without auth.
#  These orgs are scraped via Playwright (see CUSTOM_SITES below).
##############################################################################
# Phenom org codes from CDN URLs (cdn.phenompeople.com/CareerConnectResources/{ORG_CODE}/...)
# Used to build the direct Phenom backend API URL as first probe attempt.
PHENOM_ORG_CODES = {
    "Ascension Health":  "AHEAHUUS",   # confirmed from cdn.phenompeople.com/CareerConnectResources/AHEAHUUS/
    "Corewell Health":   "SPHEUS",      # confirmed from cdn.phenompeople.com/CareerConnectResources/SPHEUS/
    "Temple Health":     "TUHTUHUS",   # confirmed from widgets intercept refNum
    "DaVita":            "DAVIUS",     # confirmed from careers.davita.com 2026-05-27
    "UTHealth Houston":  "UHHUHHUS",   # 2026-09-24, widgets refNum on careers.uth.tmc.edu
    "University of Maryland Medical System": "UOJUOMUS",   # 2026-09-24, widgets refNum on careers.umms.org
}

PHENOM_ORGS = {
    # CommonSpirit moved to TalentBrew — see run_talentbrew
    # Baylor Scott & White moved to Playwright — session-based Phenom
    # 2026-05-27: renamed from "Baptist Health" → "Baptist Health (KY/IN)"
    # to disambiguate from FL/TX/AR/TN systems. Same canonical name as the
    # Workday entry above; cross-scraper job overlap dedups on the unique
    # (job_id, hospital_system) constraint.
    "Baptist Health (KY/IN)":       "https://jobs.baptisthealthcareers.com",
    # 2026-05-27: DaVita added (Phase 1C). Probed careers.davita.com,
    # confirmed Phenom hosting with org code DAVIUS. Dialysis market
    # leader, ~2,800 centers nationwide — expected +3-5K jobs.
    "DaVita":                       "https://careers.davita.com",
    # ── 2026-09-10 Texas block C (Y-texas-build). Both validated through
    # scrape_phenom on 2026-09-10 (widgets/refineSearch path): Children's
    # Health (Dallas + Plano, refNum CHHEUS) 173 rows, all TX, apply links
    # are Infor deep links; Hendrick Health (Abilene + Brownwood, refNum
    # HHSHHSUS) 353 rows, all TX, apply links go to HealthcareSource.
    "Children's Health":            "https://jobsearch.childrens.com",
    "Hendrick Health":              "https://careers.hendrickhealth.org",
    # 2026-09-17 (coverage lever 3): search-results pages report 1,979 and 840 hits.
    "Corewell Health":              "https://careers.corewellhealth.org",
    "LCMC Health":                  "https://careers.lcmchealth.org",
    "Bryan Health":                 "https://careers.bryanhealth.com",
    "PeaceHealth":                  "https://careers.peacehealth.org",
    "Roper St. Francis Healthcare": "https://careers.rsfh.com",
    "ScionHealth":                  "https://jobs.scionhealth.com",
    "Temple Health":                "https://careers.templehealth.org",
    # Atrium Health is on Coveo (not Phenom) — handled by run_atrium() below.
    # "Atrium Health":              "https://careers.atriumhealth.org",
    "ECU Health":                   "https://careers.ecuhealth.org",
    "Penn Medicine":                "https://careers.pennmedicine.org",
    # ── Added from scraper1.xlsx expansion ──
    "Bon Secours Mercy":            "https://careers.bsmhealth.org",
    "Hoag Health":                  "https://careers.hhsys.org",
    "Spartanburg Regional":         "https://careers.spartanburgregional.com",
    "Duke Health":                  "https://careers.dukehealth.org",
    "Cone Health":                  "https://careers.conehealth.com",
    "Hartford HealthCare":          "https://www.hhccareers.org",
    # SSM moved off its dead Workday tenant to Phenom (2026-08-28
    # resurrection; totalHits=1,616 validated live, refNum SHWSHLUS).
    "SSM Health":                   "https://jobs.ssmhealth.com",
    "Baptist Health (FL)":          "https://careers.baptisthealth.net",
    "Jackson Health System":        "https://jobs.jacksonhealth.org",
    "Children's Healthcare ATL":    "https://careers.choa.org",
    "Franciscan Health":            "https://jobs.franciscanhealth.org",
    "CentraCare":                   "https://jobs.centracare.com",
    "Children's Minnesota":         "https://careers.childrensmn.org",
    "St. Charles Health":           "https://careers.stcharleshealthcare.org",
    # ── Added 2026-05-08 from HAR analysis ──
    # Ascension's careers site is Phenom-hosted with iCIMS only handling the
    # final apply step — the listings come from POST jobs.ascension.org/widgets,
    # confirmed via HAR. Tenant code AHEAHUUS already in PHENOM_ORG_CODES.
    # Was previously routed to Playwright (CUSTOM_SITES) which returned 0 jobs.
    # ~140 hospitals expected.
    "Ascension Health":             "https://jobs.ascension.org",
    # ── Added 2026-05-29: non-acute expansion (Phenom detected via landing page) ──
    # PruittHealth — SNF + home health + hospice across the Southeast (~180 locations).
    # careers.pruitthealth.com is Phenom (/us/en path). Adapter discovers pageId from HTML.
    "PruittHealth":                 "https://careers.pruitthealth.com",
    # ── Added 2026-08-04: behavioral-health expansion ──
    # Acadia Healthcare — ~250 behavioral facilities nationwide. Landing page
    # carries the full Phenom fingerprint (phenompeople.com CDN, window.phApp,
    # /us/en path), verified live. Apply step is iCIMS but listings come from
    # the Phenom widgets API like Ascension's.
    "Acadia Healthcare":            "https://www.acadiacareers.com",
    # ── 2026-09-24 configs. UTHealth Houston (refNum UHHUHHUS) posts Harris
    # County Psychiatric Center jobs; it lives on careers.uth.tmc.edu, not
    # go.uth.edu. UMMS moved here from SmartRecruiters (that company page
    # shows 0 jobs; careers.umms.org is Phenom, refNum UOJUOMUS; its apply
    # links are SmartRecruiters postings). Dry run: UTHealth 452 rows, all
    # TX; UMMS 1,563, MD.
    # Munson (now SmartRecruiters) and UPMC (now Findly-Google) left this
    # table the same day: neither Phenom entry ever wrote a row.
    "UTHealth Houston":             "https://careers.uth.tmc.edu",
    "University of Maryland Medical System": "https://careers.umms.org",
    # HCA Healthcare — REMOVED 2026-07-28. It was never Phenom (that 2026-06-18
    # web-research note was wrong): careers.hcahealthcare.com is Talemetry, and
    # this entry just burned a nightly 403. Covered by the rebuilt run_hca().
}

async def scrape_phenom(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    """Scrape a Phenom People career site.

    Three-phase probe strategy:
      Phase 0 — Establish session cookies by visiting career page
      Phase 1 — Try direct REST API endpoints (works for legacy Phenom like Bryan Health)
      Phase 2 — Try /widgets endpoint with search payloads (modern Phenom with JWT)
      Phase 3 — Fetch all jobs from whichever endpoint worked

    Key fix: Probe now rejects endpoints returning data=null, which was causing
    all non-Bryan Phenom orgs to silently return 0 jobs.
    """
    jobs = []

    # ── Probe helper ──────────────────────────────────────────────────────
    def _probe_has_job_data(data: dict) -> bool:
        """Does this response actually contain extractable job listings?"""
        # Reject explicit null data (modern Phenom without auth)
        if "data" in data and data["data"] is None:
            return False
        # Reject non-zero errorCode (0 is valid/success, don't treat as error)
        ec = data.get("errorCode")
        if ec is not None and ec != 0 and ec != "0" and ec != "":
            return False
        if data.get("error"):
            return False
        # Check known job-list keys
        for key in ("jobs", "requisitions", "results", "entries", "jobPostings", "items"):
            val = data.get(key)
            if isinstance(val, list) and val:
                return True
        # Check nested: data.jobs, data.entries, etc.
        # `recommendedJobs` is the recommendationJobsBrowsingHistory payload key
        # confirmed via Duke HAR — must be in this list or the probe rejects valid data.
        data_val = data.get("data")
        if isinstance(data_val, dict):
            for key in ("jobs", "entries", "results", "requisitions", "recommendedJobs"):
                if isinstance(data_val.get(key), list) and data_val[key]:
                    return True
        if isinstance(data_val, list) and data_val:
            return True
        # Elasticsearch hits.hits
        hits = data.get("hits")
        if isinstance(hits, dict) and isinstance(hits.get("hits"), list) and hits["hits"]:
            return True
        # Phenom widgets direct hits list (modern shape, sibling of totalHits)
        if isinstance(hits, list) and hits and isinstance(hits[0], dict):
            return True
        # Bryan Health style: total_entries > 0
        if data.get("total_entries", 0) > 0:
            return True
        # NOTE: we do NOT accept bare totalHits > 0 — Phenom's `refineSearch`
        # widget returns totalHits populated with aggregation counts but
        # WITHOUT actual job listings. Only `latestJobs` and `jobSearch` return
        # real data. Requiring an actual list above ensures we skip refineSearch
        # and try the next payload.
        return False

    # ── Phase 0: Establish session cookies ────────────────────────────────
    for cookie_url in [f"{base_url}/us/en/search-results", base_url]:
        try:
            async with session.get(
                cookie_url,
                headers={**HEADERS, "Accept": "text/html"},
                proxy=proxies.get(), ssl=False,
                timeout=aiohttp.ClientTimeout(total=15),
                allow_redirects=True,
            ) as r:
                if r.status == 200:
                    break
        except Exception:
            continue

    # ── Phase 1: Probe direct API endpoints ───────────────────────────────
    org_code = PHENOM_ORG_CODES.get(system, "")
    endpoints = []
    if org_code:
        endpoints.append(f"https://api.phenompeople.com/CareerConnectResources/{org_code}/jobs/search")
    endpoints += [
        f"{base_url}/api/jobs",
        f"{base_url}/api/search/jobs",
        f"{base_url}/search/jobs",
        f"{base_url}/en/search-results",
    ]

    api_url = None
    use_post = False
    widget_payload_template = None   # set only if widgets endpoint works
    widget_response_key = None       # nested key to unwrap in widget response
    probe_headers = {
        **HEADERS,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": base_url,
        "Referer": f"{base_url}/us/en/search-results",
    }

    for ep in endpoints:
        is_cdn = "api.phenompeople.com" in ep
        for method in ("post", "get"):
            try:
                if method == "post":
                    req_kwargs = {"json": {"from": 0, "size": 10, "language": "en_US",
                                           "query": "", "location": ""}}
                else:
                    params = (
                        {"from": 0, "size": 10, "language": "en_US"}
                        if is_cdn
                        else {"start": 0, "num": 10, "from": 0, "size": 10, "language": "en_US"}
                    )
                    req_kwargs = {"params": params}

                async with getattr(session, method)(
                    ep, **req_kwargs,
                    headers=probe_headers,
                    proxy=proxies.get(), ssl=False,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status == 200 and "json" in r.headers.get("content-type", ""):
                        probe_data = await r.json(content_type=None)
                        if _probe_has_job_data(probe_data):
                            api_url = ep
                            use_post = (method == "post")
                            break
                        else:
                            logger.info(f"Phenom {system}: {ep} [{method}] → no job data (keys={list(probe_data.keys())[:6]})")
            except Exception as e:
                logger.info(f"Phenom {system}: probe {ep} [{method}] → {e}")
        if api_url:
            break

    # ── Phase 1.5: HTML metadata discovery (BSW pattern, Apr 29 2026) ─────
    # Modern Phenom orgs (Duke, UPMC, Hartford, BSW, Bon Secours, Hoag, etc.)
    # require the full canonical refineSearch payload with the org-specific
    # pageId embedded in the body. The pageId is unique per careers site
    # (page3 for Duke, page5 for Hartford, page12 for Bon Secours, etc.) and
    # is exposed as JSON in the search-results HTML. Fetching it once gives
    # us the right pageId without hardcoding a per-org table.
    discovered_page_id = None
    discovered_ref_num = None
    discovered_site_type = "external"
    if not api_url:
        # Different Phenom orgs use different URL paths for the search-results
        # page. Try common variants in order. First successful 200 wins.
        html_probe_urls = [
            f"{base_url}/us/en/search-results",
            f"{base_url}/search-results",
            f"{base_url}/jobs/search",
            f"{base_url}/careers/search",
            base_url,                          # corporate landing — last resort
        ]
        for probe_url in html_probe_urls:
            try:
                async with session.get(
                    probe_url,
                    headers={**HEADERS,
                        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                                       "Chrome/130.0.0.0 Safari/537.36"),
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    },
                    proxy=proxies.get(), ssl=False,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status != 200:
                        continue
                    html = await r.text()
                    mp = re.search(r'"pageId"\s*:\s*"([^"]+)"', html)
                    mr = re.search(r'"refNum"\s*:\s*"([^"]+)"', html)
                    ms = re.search(r'"siteType"\s*:\s*"([^"]+)"', html)
                    if mp:
                        discovered_page_id = mp.group(1)
                        if mr: discovered_ref_num = mr.group(1)
                        if ms: discovered_site_type = ms.group(1)
                        break
            except Exception as e:
                logger.info(f"Phenom {system}: HTML probe {probe_url} error: {e}")

    if discovered_page_id:
        logger.info(f"Phenom {system}: discovered pageId={discovered_page_id} "
                    f"refNum={discovered_ref_num} siteType={discovered_site_type}")

    # ── Phase 2: Widgets endpoint fallback ────────────────────────────────
    if not api_url:
        widgets_url = f"{base_url}/widgets"
        # refNum is the org code Phenom uses internally. Prefer the value
        # discovered from HTML (most accurate), then PHENOM_ORG_CODES table,
        # then the CDN-derived org_code.
        ref_num = (discovered_ref_num or PHENOM_ORG_CODES.get(system) or org_code or "").upper()

        widget_payloads = [
            # ── refineSearch (canonical, BSW pattern Apr 29 2026) ────────────
            # Only included when the HTML probe discovered a pageId. This is
            # the working payload shape verified live against BSW (1,935 jobs)
            # and Duke (854 jobs). Returns hits=0 with the wrong pageId, so
            # we ONLY include this when discovered_page_id is set.
            *([{
                "lang": "en_us", "deviceType": "desktop", "country": "us",
                "pageName": "search-results",
                "ddoKey": "refineSearch",
                "sortBy": "", "subsearch": "", "from": 0, "irs": False,
                "jobs": True, "counts": True,
                "all_fields": ["category", "jobFunction", "JobLevel0Code",
                               "state", "city", "type", "jobShift"],
                "size": 50, "clearAll": False, "jdsource": "facets",
                "isSliderEnable": False,
                "pageId": discovered_page_id,
                "siteType": discovered_site_type,
                "keywords": "", "global": True,
                "selected_fields": {}, "locationData": {},
            }] if discovered_page_id else []),
            # latestJobs — returns recent listings on landing/search pages.
            # Confirmed to return real data on Jackson Health, Spartanburg, etc.
            {
                "lang": "en_us", "deviceType": "desktop", "country": "us",
                "pageName": "search-results",
                "refNum": ref_num,
                "ddoKey": "latestJobs",
                "from": 0, "size": 50, "sortBy": "",
            },
            # jobSearch — older widget format
            {
                "lang": "en_us", "deviceType": "desktop",
                "refNum": ref_num,
                "ddoKey": "jobSearch",
                "from": 0, "size": 50, "query": "",
            },
            # recommendationJobsBrowsingHistory — undocumented but confirmed
            # via HAR analysis (Duke Health) to return full job objects with
            # title, reqId, cityState, postedDate, multi_category, etc.
            # Use this when latestJobs/jobSearch/refineSearch yield no data
            # (modern Phenom orgs like Duke, UPMC, Atrium, Hartford that gate
            # the standard search endpoints behind auth).
            {
                "keywords": None, "categories": None, "jobsViewed": None,
                "jobsApplied": None, "locations": None, "types": [],
                "userProfile": None, "landingPages": None, "department": "",
                "recoSize": 200,                # try to pull as many as Phenom allows
                "lang": "en_us", "deviceType": "desktop", "country": "us",
                "pageName": "search-results",
                "refNum": ref_num,
                "siteType": "external", "pageId": "page3",
                "ddoKey": "recommendationJobsBrowsingHistory",
            },
            # refineSearch — faceted aggregation endpoint. Sometimes returns
            # hits on certain Phenom variants. Last in chain.
            {
                "lang": "en_us", "deviceType": "desktop", "country": "us",
                "pageName": "search-results",
                "refNum": ref_num,
                "ddoKey": "refineSearch",
                "sortBy": "", "from": 0, "size": 50, "query": "",
                "locations": [], "postedDateRange": "", "searchType": "search",
            },
        ]

        for payload in widget_payloads:
            ddo_key = payload.get("ddoKey", "unknown")
            try:
                async with session.post(
                    widgets_url,
                    json=payload,
                    headers={
                        **HEADERS,
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Origin": base_url,
                        "Referer": f"{base_url}/us/en/search-results",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                    proxy=proxies.get(), ssl=False,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as r:
                    if r.status != 200 or "json" not in r.headers.get("content-type", ""):
                        continue
                    probe_data = await r.json(content_type=None)

                logger.info(f"Phenom {system}: widgets/{ddo_key} → keys={list(probe_data.keys())[:8]}")

                # Check top level
                if _probe_has_job_data(probe_data):
                    api_url = widgets_url
                    use_post = True
                    widget_payload_template = payload.copy()
                    logger.info(f"Phenom {system}: widgets/{ddo_key} has job data!")
                    break

                # Check nested under ddoKey name (widgets batch responses).
                # `recommendationJobsBrowsingHistory` wraps jobs at .data.recommendedJobs
                # so we also need the unwrap to drill that deep.
                for nested_key in (ddo_key, "recommendationJobsBrowsingHistory",
                                    "refineSearch", "latestJobs", "jobSearch"):
                    nested = probe_data.get(nested_key)
                    if isinstance(nested, dict):
                        if _probe_has_job_data(nested):
                            api_url = widgets_url
                            use_post = True
                            widget_payload_template = payload.copy()
                            widget_response_key = nested_key
                            logger.info(f"Phenom {system}: widgets/{nested_key} has nested job data!")
                            break
                        # Diagnostic — log inner shape when nested key matches but no job data.
                        # This surfaces whether the API requires auth (data:null) or returns
                        # an empty result (data.recommendedJobs:[]) or has a different schema.
                        if nested_key == ddo_key:
                            inner_keys = list(nested.keys())[:8]
                            data_val = nested.get("data")
                            data_shape = (
                                "null" if data_val is None
                                else f"dict(keys={list(data_val.keys())[:8]})" if isinstance(data_val, dict)
                                else f"list(len={len(data_val)})" if isinstance(data_val, list)
                                else type(data_val).__name__
                            )
                            hits_v = nested.get("hits")
                            total_v = nested.get("totalHits") or nested.get("total")
                            err = nested.get("errorCode") or nested.get("errorMsg")
                            logger.info(f"Phenom {system}: {ddo_key} inner: keys={inner_keys}, "
                                        f"data={data_shape}, hits={hits_v}, total={total_v}, err={err}")
                if api_url:
                    break

            except Exception as e:
                logger.info(f"Phenom {system}: widgets/{ddo_key} probe error: {e}")

    if not api_url:
        logger.info(f"Phenom {system}: no API endpoint found")
        return []

    # ── Phase 3: Fetch all jobs ───────────────────────────────────────────
    is_widgets = widget_payload_template is not None
    logger.info(f"Phenom {system}: using {api_url} [{'POST' if use_post else 'GET'}]{' (widgets)' if is_widgets else ''}")

    offset = 0
    fetch_headers = {
        **HEADERS,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": base_url,
        "Referer": f"{base_url}/us/en/search-results",
    }
    if is_widgets:
        fetch_headers["X-Requested-With"] = "XMLHttpRequest"

    while True:
        try:
            is_cdn = "api.phenompeople.com" in api_url

            if is_widgets:
                payload = widget_payload_template.copy()
                payload["from"] = offset
                payload["size"] = 50
                fetch_kwargs = {"json": payload}
                http_method = session.post
            elif use_post:
                fetch_kwargs = {"json": {"from": offset, "size": 50, "language": "en_US",
                                         "query": "", "location": ""}}
                http_method = session.post
            else:
                fetch_params = (
                    {"from": offset, "size": 50, "language": "en_US"}
                    if is_cdn
                    else {"start": offset, "num": 50, "size": 50, "from": offset, "language": "en_US"}
                )
                fetch_kwargs = {"params": fetch_params}
                http_method = session.get

            async with http_method(
                api_url, **fetch_kwargs,
                headers=fetch_headers,
                proxy=proxies.get(), ssl=False,
                timeout=aiohttp.ClientTimeout(total=25),
            ) as r:
                if r.status != 200:
                    break
                data = await r.json(content_type=None)

            # Unwrap nested widget response if needed
            if widget_response_key and isinstance(data.get(widget_response_key), dict):
                data = data[widget_response_key]

            if offset == 0:
                logger.info(f"Phenom {system}: response keys={list(data.keys())[:8]}")

            # --- Extract listings ---
            def _extract_listings(d):
                for key in ("jobs", "requisitions", "results", "entries",
                            "recommendedJobs"):
                    v = d.get(key)
                    if isinstance(v, list) and v:
                        return v
                hits = d.get("hits")
                # Phenom widgets modern shape: `hits` is a direct list, with `totalHits` as sibling
                if isinstance(hits, list) and hits:
                    return hits
                # Phenom legacy / ES-style: {"hits": {"hits": [...]}}
                if isinstance(hits, dict):
                    inner = hits.get("hits")
                    if isinstance(inner, list) and inner:
                        return inner
                data_val = d.get("data")
                if isinstance(data_val, dict):
                    # recommendationJobsBrowsingHistory: jobs are at .data.recommendedJobs
                    sub = (data_val.get("jobs") or data_val.get("entries") or
                            data_val.get("results") or data_val.get("recommendedJobs"))
                    if isinstance(sub, list) and sub:
                        return sub
                if isinstance(data_val, list) and data_val:
                    return data_val
                return []

            raw = _extract_listings(data)
            listings = [j for j in raw if isinstance(j, dict)]
            if not listings:
                if offset == 0:
                    logger.info(f"Phenom {system}: no listings at offset {offset} — keys={list(data.keys())[:8]}, data_val={str(data.get('data', ''))[:150]}")
                break

            for j in listings:
                doc = j.get("_source", j)
                # Location: prefer explicit city/state, then cityState (combined string
                # that recommendationJobsBrowsingHistory returns, e.g. "Durham, North Carolina"),
                # then multi_location (array).
                _raw_city  = doc.get("city", "")
                _raw_state = doc.get("state", "") or doc.get("stateCode", "")
                city_state = doc.get("cityState", "")
                multi_loc = doc.get("multi_location") or doc.get("locations") or []
                if isinstance(multi_loc, list) and multi_loc:
                    loc = ", ".join(str(x) for x in multi_loc)
                elif city_state:
                    loc = city_state
                else:
                    loc = doc.get("location", "") or _raw_city
                if not (_raw_city or _raw_state) and city_state:
                    parts = [p.strip() for p in city_state.split(",")]
                    if len(parts) >= 2:
                        _raw_city = parts[0]
                        _raw_state = parts[-1]
                city, state = (
                    parse_city_state(f"{_raw_city}, {_raw_state}")
                    if (_raw_city or _raw_state)
                    else (_raw_city, _raw_state)
                )
                city  = city  or _raw_city
                state = state or _raw_state
                title = doc.get("title", "") or doc.get("jobTitle", "") or doc.get("name", "")
                # reqId is what recommendationJobsBrowsingHistory uses
                job_id = str(
                    doc.get("id", "") or doc.get("jobId", "") or
                    doc.get("requisitionId", "") or doc.get("reqId", "") or
                    j.get("_id", "")
                )
                url = (
                    doc.get("applyUrl", "") or doc.get("jobUrl", "") or
                    doc.get("url", "") or f"{base_url}/job/{job_id}"
                )
                # 2026-09-24: Ascension's Phenom front (Oracle Recruiting behind
                # it) hands out Oracle's e-mail apply step
                # (.../sites/CX_1/jobs/preview/{id}/easy-apply/email). Store the
                # canonical Oracle job page instead: it is a real posting page,
                # and its "/job/" lets _phenom_detail call the jobDetail widget.
                _orc = _ORACLE_PREVIEW_RE.match(url)
                if _orc:
                    url = f"{_orc.group(1)}/job/{_orc.group(2)}"
                # multi_category is an array on recommendationJobsBrowsingHistory
                multi_cat = doc.get("multi_category") or []
                specialty_val = (
                    (multi_cat[0] if isinstance(multi_cat, list) and multi_cat else "") or
                    doc.get("category", "") or doc.get("jobCategory", "") or
                    doc.get("department", "")
                )
                if title and job_id:
                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=doc.get("facility", "") or doc.get("company", "") or system,
                        city=city, state=state,
                        location=loc or f"{city}, {state}",
                        specialty=specialty_val,
                        job_type=(
                            doc.get("employmentType", "") or doc.get("jobType", "") or
                            doc.get("type", "")
                        ),
                        url=url,
                        job_id=job_id,
                        posted_date=str(
                            doc.get("postedDate", "") or doc.get("datePosted", "") or
                            doc.get("postDate", "")
                        )[:10],
                        description=strip_html(str(
                            doc.get("description", "") or doc.get("shortDescription", "") or
                            doc.get("descriptionTeaser", "")
                        )),
                        ats_platform="Phenom",
                    ))

            total = (
                data.get("total") or data.get("count") or data.get("total_entries") or
                data.get("totalCount") or data.get("totalHits") or
                (data.get("hits", {}) or {}).get("total", {}).get("value") or
                len(listings)
            )
            if isinstance(total, dict):
                total = total.get("value", len(listings))
            offset += 50
            if offset >= int(total) or len(listings) < 50:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Phenom {system}: {e}")
            break

    if DETAIL_FETCH and jobs:
        try:
            await _detail_pass(session, system, jobs, PHENOM_DESC_BUDGET,
                               lambda j: _phenom_detail(session, base_url, j), "Phenom",
                               skip=lambda j: bool(re.search(r"taleo\.net|icims\.com", j.url or "")))
        except Exception as e:
            logger.info(f"Phenom {system}: detail pass failed ({e})")
    logger.info(f"  Phenom {system}: {len(jobs)} jobs")
    return jobs

#
# ── Baylor Scott & White (BSW) — dedicated Phenom refineSearch handler ──────
# Verified Apr 29 2026: refineSearch endpoint returns the full 1,935-job inventory
# when called with the correct payload shape (page14-ds, siteType=external, plus
# the full-required field set). Replaces the old Playwright handler which only
# captured ~58 jobs from the first-page render.
#
# Endpoint:    POST https://jobs.bswhealth.com/widgets
# Payload:    { "ddoKey":"refineSearch", "pageId":"page14-ds",
#              "siteType":"external", "from":N, "size":100, ... }
# Response:   { "refineSearch": { "totalHits": N, "data": { "jobs": [...] } } }
#
# Each job exposes: jobId, jobSeqNo, title, companyName, workLocation, city,
# state (full name), country, postalCode, category, multi_category_array, type,
# postedDate, dateCreated, applyUrl, externalApply, jobShift,
# location (e.g. "Waxahachie, Texas, United States"), descriptionTeaser.
async def run_bsw(session) -> list[Job]:
    logger.info("BSW: scraping Baylor Scott & White via Phenom refineSearch...")
    base_url   = "https://jobs.bswhealth.com"
    widgets    = f"{base_url}/widgets"
    page_size  = 100
    out: list[Job] = []
    headers = {
        **HEADERS,
        "Accept":           "application/json",
        "Content-Type":     "application/json",
        "Origin":           base_url,
        "Referer":          f"{base_url}/us/en/search-results",
        "X-Requested-With": "XMLHttpRequest",
    }

    def _payload(offset: int) -> dict:
        return {
            "lang": "en_us", "deviceType": "desktop", "country": "us",
            "pageName": "search-results", "ddoKey": "refineSearch",
            "sortBy": "", "subsearch": "", "from": offset, "irs": False,
            "jobs": True, "counts": True,
            "all_fields": ["category", "jobFunction", "JobLevel0Code",
                           "state", "city", "type", "jobShift"],
            "size": page_size, "clearAll": False, "jdsource": "facets",
            "isSliderEnable": False, "pageId": "page14-ds",
            "siteType": "external", "keywords": "", "global": True,
            "selected_fields": {}, "locationData": {},
        }

    total_hits = None
    offset = 0
    pages_fetched = 0
    consecutive_empty = 0
    while True:
        try:
            async with session.post(
                widgets, json=_payload(offset),
                headers=headers, ssl=False,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    logger.info(f"BSW: HTTP {r.status} at offset={offset}")
                    break
                data = await r.json(content_type=None)
        except Exception as e:
            logger.info(f"BSW: exception at offset={offset}: {e}")
            break

        inner = data.get("refineSearch") or {}
        if total_hits is None:
            total_hits = inner.get("totalHits") or 0
            logger.info(f"BSW: totalHits={total_hits}")
        listings = (inner.get("data") or {}).get("jobs") or []
        if not listings:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            offset += page_size
            await jitter()
            continue
        consecutive_empty = 0

        for j in listings:
            try:
                job_seq   = j.get("jobSeqNo") or j.get("jobId") or ""
                if not job_seq:
                    continue
                title     = j.get("title", "")
                if not title:
                    continue
                # state arrives as full name ("Texas") — convert to 2-letter code.
                full_loc  = j.get("location") or j.get("cityStateCountry") or ""
                _city, _state = parse_city_state(full_loc) if full_loc else ("", "")
                city  = _city  or j.get("city") or ""
                state = _state or ""
                if not state and j.get("state"):
                    # Fall back: pass "<state>" through parse_city_state for name → abbr
                    _, state = parse_city_state(j["state"])
                # Apply URL: prefer the BSW careers-site detail page over the
                # Taleo apply URL, so users land on a real job description page
                # we can also link to from analytics.
                apply_url = j.get("applyUrl") or ""
                detail_url = f"{base_url}/us/en/job/{j.get('jobId') or ''}"
                # employmentType: 'Full Time' → 'Full-time' (canonical UI form)
                jt = (j.get("type") or "").strip()
                if jt.lower() == "full time": jt = "Full-time"
                elif jt.lower() == "part time": jt = "Part-time"
                # Specialty: prefer the first multi_category_array entry
                multi_cat = j.get("multi_category_array") or []
                category_val = ""
                if isinstance(multi_cat, list) and multi_cat and isinstance(multi_cat[0], dict):
                    category_val = multi_cat[0].get("category") or ""
                if not category_val:
                    category_val = j.get("category") or ""

                out.append(Job(
                    title=title,
                    hospital_system="Baylor Scott & White",
                    hospital_name=j.get("workLocation") or "Baylor Scott & White",
                    city=city, state=state,
                    location=j.get("cityStateCountry") or full_loc,
                    specialty=category_val,
                    job_type=jt,
                    url=detail_url if j.get("jobId") else apply_url,
                    job_id=str(job_seq),
                    posted_date=str(j.get("postedDate") or j.get("dateCreated") or "")[:10],
                    description=strip_html(str(j.get("descriptionTeaser") or "")),
                    ats_platform="Phenom",
                ))
            except Exception as e:
                logger.info(f"BSW: row parse error: {e}")
                continue

        pages_fetched += 1
        offset += page_size
        if total_hits and offset >= total_hits:
            break
        # Safety cap: ~30 pages × 100 = 3,000 jobs; well above the 1,935 inventory
        if pages_fetched >= 30:
            break
        await jitter()

    logger.info(f"  BSW: {len(out):,} jobs (totalHits={total_hits})")
    return out


async def run_phenom(session) -> list[Job]:
    logger.info(f"Phenom: scraping {len(PHENOM_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_phenom(session, s, u) for s, u in PHENOM_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Phenom total: {len(jobs):,} jobs")
    return jobs





##############################################################################
#  ADP WORKFORCE NOW — public career-center JSON (rebuilt 2026-09-10, S-scraper-2)
#  The earlier build posted to .../mdf/recruitment/json/jobPosting, which is
#  not an ADP endpoint: twelve placeholder orgs and not one ADP row ever
#  reached hospital_jobs. The career-center page
#  (recruitment.html?cid=...&ccId=...) loads its list from
#    GET /mascsr/default/careercenter/public/events/staffing/v1/job-requisitions
#        ?cid=<cid>&ccId=<ccId>&timeStamp=<ms>&lang=en_US&locale=en_US&$top=20&$skip=0
#    -> {"jobRequisitions": [...], "meta": {"totalNumber": N}}
#  cid identifies the client, ccId the career center INSIDE the client
#  (Wooster runs two: hospital positions and Bloomington Medical Services).
#  Format: "System": (cid, ccId, state[, hospital_name]) | [tuples] | bare cid
##############################################################################
ADP_ORGS = {
    # 2026-09-10 (S-scraper-2): contract client, both career centers linked
    # from woosterhospital.org/careers ("WCH Positions" / "BMS Positions").
    "Wooster Community Hospital":
        ("7889f006-4c3f-49f9-931b-a7ffd827148d", "9200389848466_2", "OH", "Wooster Community Hospital"),
    "Wooster Community Hospital (BMS)":
        ("7889f006-4c3f-49f9-931b-a7ffd827148d", "9200402128985_2", "OH", "Bloomington Medical Services"),
    # 2026-09-22 Texas resume: Breckenridge TX critical-access hospital, 32 jobs in the dry run.
    "Stephens Memorial Hospital":
        ("76d82eed-ed91-46db-82b7-9e829fb2467b", "19000101_000001", "TX", "Stephens Memorial Hospital"),
    # 2026-09-10 (Z-texas-acute-D): Texas block D. legenthealth.com/careers links
    # one Workforce Now career center for all three Legent hospitals (Plano,
    # Grapevine, San Antonio); the facility comes from requisitionLocations,
    # so hospital_name is left blank here. ppgh.com/careers/current-openings
    # links its own center (page 403s WebFetch; read through curl_cffi).
    "Legent Health":
        ("f159ab44-676f-4e8e-aee4-ed91de0cda16", "19000101_000001", "TX"),
    "Palo Pinto General Hospital":
        ("c66c3f90-0bcf-49e7-a8ce-6eb2974431ee", "19000101_000001", "TX", "Palo Pinto General Hospital"),
    # 2026-09-24 Texas configs: one career center for Nexus Health Systems'
    # children's hospitals (Healthbridge Houston, Nexus Dallas) and its
    # Shenandoah campus; hospital_name left blank so the facility comes from
    # requisitionLocations, as for Legent. 109 rows in the dry run, all TX
    # (Houston 29, Dallas 26, Shenandoah 24, Conroe 14, San Antonio 12).
    "Nexus Health Systems":
        ("be1c5b46-8cdd-4d8c-8447-b37057486176", "19000101_000001", "TX"),
    # 2026-09-14 (W-urgentcare): FastMed's ATS was unidentified in the T2
    # report. It is a plain WorkforceNow career center — fastmed.com/careers
    # embeds the recruitment.html link carrying this cid — so no new adapter
    # was needed. Verified live through scrape_adp 2026-09-14: board total 40,
    # 41 rows parsed, all 41 with a city and a 2-letter state, all NC (FastMed
    # left AZ/TX/NC-outside markets; the chain is North Carolina only now).
    # Rows are list-only, no description, like every other ADP board here.
    # hospital_name is left blank on purpose: nameCode.shortName here
    # is just " Cary, NC, US", whose head is the city, so _adp_job falls back
    # to the system name instead of storing a city as a facility.
    "FastMed Urgent Care":
        ("05f4cb80-7271-43f3-b774-34a057858613", "19000101_000001", "NC"),
    # Legacy cids from scraper1.xlsx (system names were never learned because
    # the old endpoint never answered). Kept so the rebuilt adapter can say
    # in the log which of them are live boards; rename once a run shows them.
    "ADP Health System 1":  "152f13f3-9efa-4e16-9a69-bb7500136904",
    "ADP Health System 2":  "542f7b59-1156-4a17-a729-f8cd9337acf6",
    "ADP Health System 3":  "af93ba9c-e8c7-4a6f-ade3-711614110405",
    "ADP Health System 4":  "77e754a7-66ab-427f-ae54-31edee4e9bf6",
    "ADP Health System 5":  "86be0242-2e9b-4a21-9dac-6ef6b31fbbee",
    "ADP Health System 6":  "171c7aca-96cb-44e7-95db-7545554c14e8",
    "ADP Health System 7":  "c155faa0-8c71-47b0-bbaa-2b7939324014",
    "ADP Health System 8":  "a074e043-a14e-4f2d-8cf7-bee3e0a7ac61",
    "ADP Health System 9":  "1a214979-2739-4245-a1d1-38dc8531018f",
    "ADP Health System 10": "5ffc5741-7db3-4aa8-a16a-e19abed9677e",
    "ADP Health System 11": "58af5ddf-316e-4ac8-bc2f-471750cda3c7",
    "ADP Health System 12": "bb661c48-7edc-400c-adfb-40f8f7743374",
}
_ADP_API = "https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions"
_ADP_PORTAL = "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html"
_ADP_PAGE = 20
_ADP_POSITION = {"F": "Full time", "P": "Part time", "T": "Temporary", "C": "Contract"}


def _adp_centers(value):
    """Normalise an ADP_ORGS value to [(cid, ccId, state, hospital_name)]."""
    if isinstance(value, str):
        return [(value, "19000101_000001", "", "")]
    if isinstance(value, tuple):
        value = [value]
    return [(v[0], v[1], v[2] if len(v) > 2 else "", v[3] if len(v) > 3 else "") for v in value]


def _adp_job(j: dict, system: str, cid: str, cc_id: str, default_state: str = "",
             hospital_name: str = ""):
    """One jobRequisitions item -> Job, or None when it has no id / title."""
    item_id = str(j.get("itemID") or j.get("itemId") or j.get("requisitionID") or j.get("id") or "").strip()
    title = str(j.get("requisitionTitle") or j.get("jobTitle") or j.get("title") or "").strip()
    if not item_id or not title:
        return None
    locs = j.get("requisitionLocations") or []
    loc0 = locs[0] if locs and isinstance(locs[0], dict) else {}
    addr = loc0.get("address") or {}
    city = str(addr.get("cityName") or "").strip()
    st = str((addr.get("countrySubdivisionLevel1") or {}).get("codeValue") or "").strip()
    if not (city or st):
        nc = loc0.get("nameCode") or {}
        city, st = parse_city_state(str(nc.get("shortName") or nc.get("longName") or ""))
    if len(st) > 2:
        st = parse_city_state(f"{city}, {st}")[1] or st
    st = (st or default_state).upper()
    # Live shape (Wooster, 2026-09-10): postDate at the top level, JobClass /
    # SalaryType in customFieldGroup, payGradeRange.min/maximumRate.amountValue
    # as structured pay (16.21 - 20.92 + SalaryType "Hourly"), workLevelCode
    # for the schedule ("Casual"), and the facility as the head of
    # requisitionLocations[].nameCode.shortName ("Wooster Community Hospital,
    # Wooster, OH, US"). No description in the list payload (list-only rows).
    posted = str(j.get("postDate") or "")[:10]
    if not posted:
        for pi in j.get("postingInstructions") or []:
            posted = str(pi.get("postingDate") or "")[:10]
            if posted:
                break
    cfg = j.get("customFieldGroup") or {}
    category, salary_type = "", ""
    for f in cfg.get("stringFields") or []:
        code = str((f.get("nameCode") or {}).get("codeValue") or "")
        if code == "JobClass" or "categor" in code.lower():
            category = str(f.get("stringValue") or "")
            break
    for f in cfg.get("codeFields") or []:
        if str((f.get("nameCode") or {}).get("codeValue") or "") == "SalaryType":
            salary_type = str(f.get("shortName") or f.get("codeValue") or "")
    pg = j.get("payGradeRange") or {}
    lo = (pg.get("minimumRate") or {}).get("amountValue")
    hi = (pg.get("maximumRate") or {}).get("amountValue")
    wage = None
    if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
        pair = _wage_pair(float(lo), float(hi))
        stl = salary_type.lower()
        hourly = stl.startswith("hour") or stl == "hr"
        annual = "annual" in stl or "year" in stl or stl in ("sa", "yr")
        if pair and ((hourly and pair[2] == "hour") or (annual and pair[2] == "year") or not stl):
            wage = pair
    if not hospital_name:
        head = str(((loc0.get("nameCode") or {}).get("shortName") or "")).split(",")[0].strip()
        if head and head.lower() != city.lower() and not re.search(r"\d", head):
            hospital_name = head
    ptype = str((j.get("workLevelCode") or {}).get("shortName") or j.get("positionType") or "").strip()
    return Job(
        title=title,
        hospital_system=system,
        hospital_name=hospital_name or system,
        city=city, state=st,
        location=f"{city}, {st}".strip(", "),
        specialty=category,
        job_type=_ADP_POSITION.get(ptype.upper(), ptype),
        url=f"{_ADP_PORTAL}?cid={cid}&ccId={cc_id}&lang=en_US&selectedMenuKey=CareerCenter&jobId={item_id}",
        job_id=item_id,
        posted_date=posted,
        description=strip_html(j.get("jobDescription") or j.get("description") or ""),
        ats_platform="ADP",
        wage_min=wage[0] if wage else None,
        wage_max=wage[1] if wage else None,
        wage_unit=wage[2] if wage else None,
    )


async def scrape_adp(session: aiohttp.ClientSession, system: str, value) -> list[Job]:
    jobs: list[Job] = []
    totals: list[int] = []
    for cid, cc_id, default_state, hospital_name in _adp_centers(value):
        skip, total = 0, None
        while True:
            try:
                async with req(session, "get", _ADP_API,
                    params={"cid": cid, "ccId": cc_id, "timeStamp": str(int(time.time() * 1000)),
                            "lang": "en_US", "locale": "en_US",
                            "$top": str(_ADP_PAGE), "$skip": str(skip)},
                    headers={**HEADERS,
                             "Referer": f"{_ADP_PORTAL}?cid={cid}&ccId={cc_id}&lang=en_US",
                             "Accept": "application/json, text/plain, */*"},
                    ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                    if r.status != 200:
                        logger.info(f"ADP {system}: HTTP {r.status} (cid {cid[:8]}…, ccId {cc_id})")
                        break
                    data = await r.json(content_type=None)
                items = (data or {}).get("jobRequisitions") or []
                if total is None:
                    total = int(((data or {}).get("meta") or {}).get("totalNumber") or 0)
                for j in items:
                    job = _adp_job(j, system, cid, cc_id, default_state, hospital_name)
                    if job:
                        jobs.append(job)
                # The API answered 19 to $top=20 on every board in the
                # 2026-09-10 dry run, so a short page is not the end: page by
                # what was received until meta.totalNumber is reached.
                skip += len(items)
                if not items or (total and skip >= total):
                    break
                await jitter()
            except Exception as e:
                logger.info(f"ADP {system}: {e}")
                break
        totals.append(total or 0)
    logger.info(f"  ADP {system}: {len(jobs)} jobs (board totals {totals})")
    return jobs


async def run_adp(session) -> list[Job]:
    logger.info(f"ADP: scraping {len(ADP_ORGS)} systems...")
    orgs = priority_states_first(list(ADP_ORGS.items()),
                                 lambda kv: _adp_centers(kv[1])[0][2])
    # 2026-09-24: every tenant is on workforcenow.adp.com; HOST_CONCURRENCY at a time.
    results = await _gather_by_host(session, orgs, scrape_adp, lambda v: _url_host(_ADP_API))
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  ADP total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  ADP RECRUITMENT MANAGEMENT "CX" (myjobs.adp.com) — 2026-09-14, W-urgentcare
#
#  NOT the WorkforceNow career center scrape_adp() reads; a different ADP
#  product with a different API. nextcare.com/careers 301s to
#  myjobs.adp.com/nextcare/cx, an Angular app. The T2 report stopped at the
#  SiteMinder bounce (/cx/staffing/v2/job-applicant 302s to
#  workforcenow/login.html) and the privacy-statement modal. Neither is on the
#  data path: the modal gates the APPLY flow in the browser only, and the job
#  list comes from my.adp.com behind two PUBLIC request headers.
#
#  Two GETs, no cookie and no session:
#    1. myjobs.adp.com/public/staffing/v1/career-site/<domain>
#       -> {"orgoid": "...", "myJobsToken": "..."}  (both public; ADP rotates
#          the token, so it is fetched per run and never hardcoded)
#    2. my.adp.com/.../v1/job-requisitions with orgoid + myjobstoken as
#       REQUEST HEADERS. Without orgoid the API answers HTTP 400
#       {"message":"Missing orgoid header"}; without $select the payload
#       carries no description and no posting date.
#  `count` is the true total, $skip pages, and $top=100 was served in full.
#  The one trap is Accept-Language; see the note in scrape_adp_cx.
#  Verified live through this adapter 2026-09-14: NextCare count 104, 104/104
#  rows with a city and a 2-letter state, 104/104 with a description over 200
#  chars and a postingDate, 10 states (AZ CO KS MO NC NE OK TX VA WY).
##############################################################################
ADPCX_ORGS = {
    # "System": ("career-site domain", default_state)
    # NextCare: ~170 urgent-care clinics in 11 states (nextcare.com "170+").
    "NextCare": ("nextcare", ""),
    # 2026-09-24 Texas configs: Hunt Regional Healthcare (Greenville TX),
    # myjobs.adp.com/huntregional; 114 rows in the dry run, all with bodies.
    "Hunt Regional Healthcare": ("huntregional", "TX"),
}
_ADPCX_SITE    = "https://myjobs.adp.com/public/staffing/v1/career-site/{domain}"
_ADPCX_API     = "https://my.adp.com/myadp_prefix/mycareer/public/staffing/v1/job-requisitions"
_ADPCX_DETAILS = "https://myjobs.adp.com/{domain}/cx/job-details?reqId={req}"
# Field list copied from the app's own XHR. publishedJobTitle is the candidate-
# facing title; jobTitle is the internal one (identical on NextCare today).
_ADPCX_SELECT = ("reqId,jobTitle,publishedJobTitle,type,jobDescription,jobQualifications,"
                 "workLocations,workLevelCode,clientRequisitionID,postingDate,"
                 "requisitionLocations")
_ADPCX_PAGE = 100
_ADPCX_MAX_PAGES = 40        # 4,000 reqs; a bigger board means something is wrong


def _adpcx_job(j: dict, system: str, domain: str, default_state: str = "") -> Job | None:
    """One jobRequisitions item -> Job, or None when it has no id / title or
    is not a US posting."""
    req_id = str(j.get("reqId") or "").strip()
    title = str(j.get("publishedJobTitle") or j.get("jobTitle") or "").strip()
    if not req_id or not title:
        return None
    locs = j.get("requisitionLocations") or []
    loc0 = locs[0] if locs and isinstance(locs[0], dict) else {}
    addr = loc0.get("address") or {}
    country = str((addr.get("country") or {}).get("codeValue") or "").upper()
    if country and country not in ("USA", "US"):
        return None
    city = str(addr.get("cityName") or "").strip()
    st = str((addr.get("countrySubdivisionLevel1") or {}).get("codeValue") or "").strip()
    if not (city and st):
        # Every NextCare row carried both on 2026-09-14; this is the degrade
        # path for a board that does not. The clinician titles read
        # "… | Cedar Park, TX", so the tail after the last pipe is a location.
        c2, s2 = parse_city_state(title.rsplit("|", 1)[-1] if "|" in title else "")
        city, st = city or c2, st or s2
    if len(st) > 2:
        st = parse_city_state(f"{city}, {st}")[1] or ""
    st = (st or default_state).upper()
    # NextCare appends the location to the candidate-facing title
    # ("Physician Assistant or Nurse Practitioner | Cedar Park, TX"). The card
    # already carries city and state, so the suffix is noise in the headline
    # and in search. Trimmed only when the tail really is this row's location
    # — never blindly, because a pipe can carry a credential list.
    if "|" in title:
        head, tail = title.rsplit("|", 1)
        _c, _s = parse_city_state(tail.strip())
        if _s and _s.upper() == st and head.strip():
            title = head.strip()
    return Job(
        title=title,
        hospital_system=system,
        hospital_name=system,
        city=city,
        state=st,
        location=f"{city}, {st}".strip(", "),
        specialty="",
        job_type=derive_job_type(title, str(j.get("workLevelCode") or "")),
        url=_ADPCX_DETAILS.format(domain=domain, req=req_id),
        job_id=req_id,
        posted_date=str(j.get("postingDate") or "")[:10],
        description=strip_html(j.get("jobDescription") or ""),
        ats_platform="ADP CX",
    )


async def _adpcx_credentials(session: aiohttp.ClientSession, domain: str) -> tuple[str, str]:
    """(orgoid, myJobsToken) from the public career-site config. ("", "") when
    the config cannot be read — the caller then skips the board rather than
    hammering the API with headers it knows are missing."""
    try:
        async with req(session, "get", _ADPCX_SITE.format(domain=domain),
                       headers={**HEADERS, "Accept": "application/json"},
                       ssl=False, proxy=proxies.get(),
                       timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"ADP CX {domain}: career-site HTTP {r.status}")
                return "", ""
            cfg = await r.json(content_type=None)
    except Exception as e:
        logger.info(f"ADP CX {domain}: career-site {e}")
        return "", ""
    return str((cfg or {}).get("orgoid") or ""), str((cfg or {}).get("myJobsToken") or "")


async def scrape_adp_cx(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    domain, default_state = (org_data if isinstance(org_data, tuple) else (org_data, ""))
    orgoid, token = await _adpcx_credentials(session, domain)
    if not (orgoid and token):
        logger.info(f"  ADP CX {system}: no orgoid/token — skipped")
        return []
    # Accept-Language is NOT cosmetic here and must stay exactly "en-US".
    # The API matches the header against the posting's locale as a literal
    # string, so anything else answers HTTP 200 with {"count":0} and no
    # error: HEADERS' own "en-US,en;q=0.9" (a normal browser q-list), "en",
    # "en_US" and "*" all return 0, while "en-US" and no header at all
    # return the full board. Measured on NextCare 2026-09-14; this silent
    # zero is why the first cut of this adapter banked nothing.
    headers = {**HEADERS, "Accept": "application/json, text/plain, */*",
               "Accept-Language": "en-US",
               "Referer": f"https://myjobs.adp.com/{domain}/cx/job-listing",
               "orgoid": orgoid, "myjobstoken": token}
    jobs: list[Job] = []
    total = None
    skip = 0
    for _ in range(_ADPCX_MAX_PAGES):
        try:
            async with req(session, "get", _ADPCX_API,
                           params={"$select": _ADPCX_SELECT, "$top": str(_ADPCX_PAGE),
                                   "$skip": str(skip), "radius": "25", "$filter": "",
                                   "tz": "America/Chicago"},
                           headers=headers, ssl=False, proxy=proxies.get(),
                           timeout=aiohttp.ClientTimeout(total=90)) as r:
                if r.status != 200:
                    logger.info(f"ADP CX {system}: HTTP {r.status} at $skip={skip}")
                    break
                data = await r.json(content_type=None)
        except Exception as e:
            logger.info(f"ADP CX {system}: {e} at $skip={skip}")
            break
        items = (data or {}).get("jobRequisitions") or []
        if total is None:
            total = int((data or {}).get("count") or 0)
        for j in items:
            job = _adpcx_job(j, system, domain, default_state)
            if job:
                jobs.append(job)
        skip += len(items)
        if not items or (total and skip >= total):
            break
        await jitter()
    if not total:
        # A live urgent-care board is never empty; count 0 means the request
        # was shaped wrong (see the Accept-Language note above), not that the
        # employer stopped hiring. Loud, but still no exception: one bad board
        # must not take the run down.
        logger.warning(f"  ADP CX {system}: board count 0 — request rejected silently, check headers")
    logger.info(f"  ADP CX {system}: {len(jobs)} jobs (board count {total})")
    return jobs


async def run_adp_cx(session) -> list[Job]:
    logger.info(f"ADP CX: scraping {len(ADPCX_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_adp_cx(session, s, v) for s, v in ADPCX_ORGS.items()],
        return_exceptions=True
    )
    for (s, _), r in zip(ADPCX_ORGS.items(), results):
        if isinstance(r, Exception):
            logger.info(f"  ADP CX {s}: ERROR {r}")
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  ADP CX total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  PATIENT FIRST — own-domain WordPress board, 2026-09-14, W-urgentcare
#
#  78 medical centers across VA / MD / NJ / PA; 223 open posts on 2026-09-14.
#  Worth having: the physician and physician-extender posts carry a POSTED PAY
#  BAND in the body ("$129,000 - $286,000 a year", "$27 - $37 an hour"), which
#  is the inventory this board sells.
#
#  SOURCE IS THE SITE'S OWN WORDPRESS REST API, not the 23 listing pages:
#    /wp-json/wp/v2/job?per_page=100&page=N      223 posts, 3 requests
#      -> id, title.rendered, link, date, content.rendered (full description,
#         2.5k to 8k chars), and the term ids for location / job-type
#    /wp-json/wp/v2/location?per_page=100&page=N 256 terms, 3 requests
#      -> term id to "Aberdeen, MD" / "Abington, PA" (a few are bare centre
#         names like "Bayview"; those fall back to the state in the title)
#    /wp-json/wp/v2/center?per_page=100         79 centres, 1 request
#      -> acf.city / acf.stateCode, which is what gives the bare-centre-name
#         rows a real location (72% of rows placed without it, 97% with it)
#  Seven requests instead of 23 HTML pages, and full descriptions instead of
#  excerpts. A job page past the end answers 400, which the fetch reads as
#  empty, so the pager cannot run away.
#  The card R-number (R20203553) is NOT in the REST payload — it is themed in
#  from a private ACF field — so job_id is the WordPress post id, which is the
#  stable key the site itself uses in class_list and never changes.
#
#  OFF BY DEFAULT, and it is not a bug. EVERY patientfirst.com URL — the
#  listing, the REST API, even /robots.txt — answers HTTP 403 with a
#  "Checking your browser..." JavaScript challenge (a8c-cdn; it sets a short
#  _hcc cookie). aiohttp cannot pass it and neither can curl_cffi with a
#  Chrome TLS fingerprint; both were tried again on 2026-09-14 and got the
#  same 6,877-byte challenge page every time, before and after a homepage
#  visit. An ordinary browser runs the site's own script and loads the page
#  normally, so the fetch below is the repo's existing rendered-page route:
#  Chromium navigates the careers page, the page clears itself, and the REST
#  calls then run inside that cleared session with page.evaluate + fetch()
#  — exactly what run_atrium() does for Cloudflare-fronted Coveo.
#
#  No challenge solver was written and none is wanted: whether to run a
#  browser at a site that fronts itself this way is Robert's call, not the
#  scraper's, so the adapter ships disabled. robots.txt cannot be read to
#  check policy (it 403s too). Set PATIENT_FIRST_ENABLED=1 to turn it on.
#  See reports/W-urgentcare-adapters.md.
##############################################################################
PATIENT_FIRST_BASE = "https://www.patientfirst.com"
PATIENT_FIRST_CAREERS = PATIENT_FIRST_BASE + "/careers/medical-center-administrative-opportunities/"
PATIENT_FIRST_ENABLED = os.getenv("PATIENT_FIRST_ENABLED", "0") == "1"
# 100 posts per page; 223 live on 2026-09-14, so 3 pages. The cap is the
# runaway guard, the same shape as _ADPCX_MAX_PAGES.
PATIENT_FIRST_MAX_PAGES = int(os.getenv("PATIENT_FIRST_MAX_PAGES", "4"))
_PF_JOB_API = "/wp-json/wp/v2/job?per_page=100&page={page}"
_PF_LOC_API = "/wp-json/wp/v2/location?per_page=100&page={page}"
# The centre post type (79 live) is how the bare-centre-name location terms
# get a city and a state: acf.city / acf.stateCode per centre, keyed by the
# centre title ("Midlothian" -> Richmond, VA). One request.
_PF_CENTER_API = "/wp-json/wp/v2/center?per_page=100"
_PF_JOBTYPE = {"full time": "Full time", "part time": "Part time",
               "per diem": "PRN", "prn": "PRN", "temporary": "Temporary"}


def _pf_text(fragment: str) -> str:
    """Rendered WordPress HTML -> flat text. Never raises on None."""
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))).strip()


def _pf_job(j: dict, locations: dict, job_types: dict, centers: dict,
            system: str = "Patient First") -> Job | None:
    """One wp/v2/job item -> Job, or None when it has no id, title or link."""
    post_id = str(j.get("id") or "").strip()
    title = _pf_text(((j.get("title") or {}) or {}).get("rendered") or "")
    url = str(j.get("link") or "").strip()
    if not (post_id and title and url):
        return None
    term_ids = j.get("location") or []
    term = locations.get(term_ids[0], "") if term_ids else ""
    city, state = parse_city_state(term) if term else ("", "")
    if state not in _US_STATE_SET:
        # Bare centre name ("Bayview", "Indian River"): not a city/state pair.
        # The centre post type carries the real address, so look it up there
        # first; failing that take the state from the title, which on this
        # board often names it ("Virginia Part Time Physician Extender").
        city, state = centers.get(term.strip().lower(), ("", ""))
        if state not in _US_STATE_SET:
            city, state = "", _state_from_title(title)
    raw_type = ""
    for t in (j.get("job-type") or []):
        raw_type = _PF_JOBTYPE.get((job_types.get(t, "") or "").strip().lower(), "")
        if raw_type:
            break
    return Job(
        title=title,
        hospital_system=system,
        hospital_name=f"{system} - {term}" if term and not city else system,
        city=city,
        state=state,
        location=f"{city}, {state}" if city and state else (city or state),
        specialty="",
        job_type=derive_job_type(title, raw_type),
        url=url,
        job_id=post_id,
        posted_date=str(j.get("date") or "")[:10],
        description=_pf_text(((j.get("content") or {}) or {}).get("rendered") or ""),
        ats_platform="WordPress",
    )


async def _pf_fetch_json(paths: list[str]) -> dict[str, list]:
    """Fetch REST paths inside a rendered patientfirst.com session.

    Mirrors run_atrium(): Chromium loads one real page so the edge challenge
    clears itself, then the JSON calls ride that session's cookies through
    page.evaluate + fetch(). Returns {path: parsed list}; a path that fails is
    simply absent, and the whole function returns {} rather than raising, so a
    dead browser or a changed page can never take the run down.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Patient First: playwright not installed — skipping")
        return {}
    pw_proxy = None
    proxy_url = proxies.get()
    if proxy_url:
        m = re.match(r"https?://([^:]+):([^@]+)@([^:]+):(\d+)", proxy_url)
        if m:
            pw_proxy = {"server": f"http://{m.group(3)}:{m.group(4)}",
                        "username": m.group(1), "password": m.group(2)}
    out: dict[str, list] = {}
    try:
        async with async_playwright() as pw:
            launch_kwargs = dict(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-blink-features=AutomationControlled",
                      "--disable-dev-shm-usage"],
            )
            if pw_proxy:
                launch_kwargs["proxy"] = pw_proxy
            browser = await pw.chromium.launch(**launch_kwargs)
            try:
                ctx = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent=HEADERS["User-Agent"],
                    locale="en-US", timezone_id="America/New_York",
                )
                page = await ctx.new_page()
                await page.goto(PATIENT_FIRST_CAREERS, wait_until="domcontentloaded", timeout=60000)
                cleared = False
                for _ in range(15):
                    content = await page.content()
                    if "fl-post-feed-post" in content:
                        cleared = True
                        break
                    await asyncio.sleep(2)
                if not cleared:
                    logger.warning("Patient First: careers page never rendered — challenge held or page changed")
                    return {}
                bodies = await page.evaluate(
                    """async (paths) => {
                        const one = async (p) => {
                            try {
                                const r = await fetch(p, { credentials: 'include',
                                                           headers: { 'Accept': 'application/json' } });
                                if (!r.ok) return '';
                                return await r.text();
                            } catch (e) { return ''; }
                        };
                        const out = [];
                        for (const p of paths) { out.push(await one(p)); }
                        return out;
                    }""",
                    paths,
                )
            finally:
                await browser.close()
    except Exception as e:
        logger.warning(f"Patient First: rendered fetch failed ({e})")
        return {}
    for path, body in zip(paths, bodies or []):
        if not body:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        if isinstance(data, list):
            out[path] = data
    return out


async def scrape_patient_first(session: aiohttp.ClientSession) -> list[Job]:
    system = "Patient First"
    # One browser session covers both taxonomies and every job page; the
    # request list is built up front so the page is opened exactly once.
    paths = [_PF_LOC_API.format(page=p) for p in range(1, 4)] + [_PF_CENTER_API]
    paths += [_PF_JOB_API.format(page=p) for p in range(1, PATIENT_FIRST_MAX_PAGES + 1)]
    payloads = await _pf_fetch_json(paths)
    if not payloads:
        logger.info(f"  {system}: 0 jobs (no payload)")
        return []
    locations: dict[int, str] = {}
    job_types: dict[int, str] = {}
    for p in paths[:3]:
        for t in payloads.get(p, []):
            if isinstance(t, dict) and t.get("id"):
                locations[t["id"]] = _pf_text(t.get("name") or "")
    centers: dict[str, tuple[str, str]] = {}
    for c in payloads.get(_PF_CENTER_API, []):
        if not isinstance(c, dict):
            continue
        acf = c.get("acf") if isinstance(c.get("acf"), dict) else {}
        name = _pf_text(((c.get("title") or {}) or {}).get("rendered") or "")
        city = str(acf.get("city") or "").strip()
        st = str(acf.get("stateCode") or "").strip().upper()
        if name and city and st in _US_STATE_SET:
            centers[name.lower()] = (clean_city(city), st)
            # A few location terms are the centre's CITY rather than the
            # centre's name ("Fredericksburg"), so index both. setdefault
            # keeps the centre-name key authoritative when they collide.
            centers.setdefault(city.strip().lower(), (clean_city(city), st))
    # job-type terms are few and ride inside the posts' _embedded only when
    # asked for; the names are stable, so map the ids from the class_list the
    # REST payload already carries ("job-type-part-time").
    jobs: list[Job] = []
    seen: set[str] = set()
    for p in paths[4:]:
        items = payloads.get(p, [])
        if not items:
            break
        for it in items:
            if not isinstance(it, dict):
                continue
            for cls in (it.get("class_list") or []):
                if str(cls).startswith("job-type-"):
                    slug = str(cls)[len("job-type-"):].replace("-", " ")
                    for tid in (it.get("job-type") or []):
                        job_types.setdefault(tid, slug)
            job = _pf_job(it, locations, job_types, centers, system)
            if not job or job.job_id in seen:
                continue
            seen.add(job.job_id)
            jobs.append(job)
    logger.info(f"  {system}: {len(jobs)} jobs ({len(locations)} location terms, {len(centers)} centres)")
    return jobs


async def run_patient_first(session) -> list[Job]:
    if not PATIENT_FIRST_ENABLED:
        logger.info("Patient First: disabled (PATIENT_FIRST_ENABLED != 1) — "
                    "patientfirst.com answers 403 behind a JS challenge on every "
                    "URL, so it needs the rendered fetch; turning that on is the "
                    "owner's call. See reports/W-urgentcare-adapters.md")
        return []
    return await scrape_patient_first(session)


##############################################################################
#  SELECTMINDS / ORACLE RECRUITING — used by Henry Ford Health
#  SelectMinds exposes a public JSON search API
##############################################################################
SELECTMINDS_ORGS = {
    # Henry Ford removed 2026-08-28: banked 0 here; HF is on SmartRecruiters
    # now (SMARTRECRUITERS_ORGS "HenryFordHealth1", validated 1,689 jobs).
    # McLaren Health Care is a wave-3 SelectMinds candidate (front-end HTML
    # at careers.mclaren.org, ~1,094 jobs; their AJAX endpoint 403s without
    # a browser session — see the 2026-08-28 audit doc before adding).
}

async def scrape_selectminds(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    # SelectMinds public API endpoint pattern
    base = f"https://{org}.referrals.selectminds.com"
    api_url = f"{base}/api/jobs/search"
    page = 1
    while True:
        try:
            async with req(session, "get",
                api_url,
                params={"page": page, "per_page": 25, "keywords": ""},
                headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"}, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    # Try alternate endpoint
                    async with req(session, "get",
                        f"{base}/jobs/search",
                        params={"page": page, "per_page": 25},
                        headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
                    ) as r2:
                        if r2.status != 200:
                            logger.info(f"SelectMinds {system}: HTTP {r.status}")
                            break
                        data = await r2.json(content_type=None)
                else:
                    data = await r.json(content_type=None)

            listings = data.get("jobs", data.get("results", []))
            if not listings:
                break

            for j in listings:
                loc = j.get("location", "")
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("department", system),
                    city=_city, state=_state,
                    location=loc,
                    specialty=j.get("category", ""),
                    job_type=j.get("employment_type", ""),
                    url=j.get("url", f"{base}/jobs/{j.get('id','')}"),
                    job_id=str(j.get("id", "")),
                    posted_date=str(j.get("created_at", ""))[:10],
                    description=strip_html(j.get("description", "")),
                    ats_platform="SelectMinds",
                ))

            if len(listings) < 25:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"SelectMinds {system}: {e}")
            break

    logger.info(f"  SelectMinds {system}: {len(jobs)} jobs")
    return jobs

async def run_selectminds(session) -> list[Job]:
    logger.info(f"SelectMinds: scraping {len(SELECTMINDS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_selectminds(session, s, o) for s, o in SELECTMINDS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  SelectMinds total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  RECRUITING.COM — used by STB Careers
#  Has a simple public JSON API
##############################################################################
RECRUITINGCOM_ORGS = {
    "STB Careers": "stbcareers",
}

async def scrape_recruitingcom(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    api_url = f"https://{org}.recruiting.com/api/v1/jobs"
    page = 1
    while True:
        try:
            async with req(session, "get",
                api_url,
                params={"page": page, "per_page": 50},
                headers={
                    **HEADERS,
                    "Referer": f"https://{org}.recruiting.com/",
                    "Origin": f"https://{org}.recruiting.com",
                    "Accept": "application/json",
                }, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    logger.info(f"Recruiting.com {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)

            listings = data if isinstance(data, list) else data.get("jobs", data.get("data", []))
            if not listings:
                break

            for j in listings:
                loc = j.get("location", "") or j.get("city", "")
                _city, _state = parse_city_state(str(loc))
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=_city, state=_state,
                    location=str(loc),
                    specialty=j.get("department", "") or j.get("category", ""),
                    job_type=j.get("employment_type", "") or j.get("type", ""),
                    url=j.get("url", f"https://{org}.recruiting.com/jobs/{j.get('id','')}"),
                    job_id=str(j.get("id", "")),
                    posted_date=str(j.get("posted_at", j.get("created_at", "")))[:10],
                    description=strip_html(j.get("description", "")),
                    ats_platform="Recruiting.com",
                ))

            if len(listings) < 50:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Recruiting.com {system}: {e}")
            break

    logger.info(f"  Recruiting.com {system}: {len(jobs)} jobs")
    return jobs

async def run_recruitingcom(session) -> list[Job]:
    logger.info(f"Recruiting.com: scraping {len(RECRUITINGCOM_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_recruitingcom(session, s, o) for s, o in RECRUITINGCOM_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Recruiting.com total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  INFOR CLOUDSUITE HCM — used by Faith Regional Health Services
#  Public job board with REST API
##############################################################################
INFOR_ORGS = {
    # Format: "System": ("css-subdomain", "hr_org")
    "Faith Regional Health":       ("css-faithregional-prd",             "100"),
    # 2026-09-17 (coverage lever 3): css host and HROrganization from the careers pages.
    "BayCare":                     ("css-baycarehs-prd",                 "1"),      # 16 FL hospitals
    "Aspirus Health":              ("css-aspirus-prd",                   "10"),     # WI/MI
    "Penn State Health":           ("css-pennstatehealth-prd",           "PSH"),
    "Lee Health":                  ("css-leememorial-prd",               "1000"),   # Fort Myers FL
    "CAMC":                        ("css-camc-prd",                      "CAMC"),
    "Vandalia Health":             ("css-camc-prd",                      "CAMC"),   # Vandalia rebranded from CAMC — same system
    "Ballad Health":               ("css-balladhealth-prd",              "1"),
    "PH Healthcare":               ("css-phhealthcare-prd",              "1"),
    "Carson Tahoe Health":         ("css-carsontahoehs-prd",             "1"),
    "Middlesex Health":            ("css-middlesex-prd",                 "1"),
    "Bay Health":                  ("css-bayhealth-prd",                 "1"),
    "BayCare Health System":       ("css-baycarehs-prd",                 "1"),
    "Lakeland Regional Health":    ("css-lakelandrmc-prd",               "LRH"),
    "Tift Regional Health":        ("css-tiftregional-prd",              "1"),
    "Eastern Maine Health":        ("css-emh-prd",                       "1"),
    "Maury Regional Health":       ("css-mauryregionalhos-prd",          "MR"),
    "Skagit Regional Health":      ("css-mnc4u622l854lnnt-prd",          "1"),
    "DHR Health":                  ("css-pf7dmpe5vb7ydcw4-prd",          "1"),
}

async def scrape_infor(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    org_id, hr_org = org_data
    jobs = []

    # Infor CloudSuite HCM — Lawson CandidateSelfService JSON API
    base = f"https://{org_id}.inforcloudsuite.com"

    # Try multiple endpoint patterns
    endpoints = [
        # Newer OData v1
        (f"{base}/hcm/v1/Jobs", {
            "csk.JobBoard": "EXTERNAL",
            "csk.HROrganization": hr_org,
            "$format": "json",
            "$top": 100,
        }),
        # Lawson CandidateSelfService with JSON output
        (f"{base}/hcm/CandidateSelfService/controller.servlet", {
            "context.session.key.HROrganization": hr_org,
            "context.session.key.JobBoard": "EXTERNAL",
            "context.dataarea": "hcm",
            "dataarea": "lmghr",
            "JobPost": "1",
            "format": "json",
        }),
        # Alternative OData path
        (f"{base}/hcm/Jobs/page/JobsSearchPage", {
            "csk.JobBoard": "EXTERNAL",
            "csk.HROrganization": hr_org,
            "$format": "json",
            "$top": 100,
        }),
    ]

    data = None
    working_url = None
    last_status = None
    for json_api, params in endpoints:
        try:
            async with req(session, "get",
                json_api,
                params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                last_status = r.status
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    if "json" in ct:
                        data = await r.json(content_type=None)
                        working_url = json_api
                        break
                    else:
                        # Got HTML back — this endpoint doesn't return JSON
                        continue
                # Don't log intermediate failures — final state logged below
        except Exception:
            # Suppress per-endpoint exceptions; final summary line below
            continue

    if not data:
        # Single line per org instead of 3+ — Infor as a platform is not currently
        # producing data, so until we get a working endpoint pattern from a HAR,
        # this is the cleanest way to keep run logs readable.
        logger.info(f"Infor {system}: no JSON endpoint (last={last_status})")
        return []

    logger.info(f"Infor {system}: using {working_url}")
    try:
        listings = data.get("value", data.get("d", {}).get("results", data.get("jobs", [])))
        for j in listings:
            _icity = j.get("City", "")
            _istate = j.get("State", "") or j.get("StateProvince", "")
            _, state = parse_city_state(f"{_icity}, {_istate}")
            city  = _icity
            state = state or _istate
            jobs.append(Job(
                title=j.get("JobTitle", j.get("Title", "")),
                hospital_system=system,
                hospital_name=j.get("Organization", system),
                city=city, state=state,
                location=f"{city}, {state}",
                specialty=j.get("JobCategory", ""),
                job_type=j.get("EmploymentType", ""),
                url=base_url,
                job_id=str(j.get("RequisitionId", j.get("JobId", ""))),
                posted_date=str(j.get("PostingDate", ""))[:10],
                description=strip_html(j.get("JobDescription", "")),
                ats_platform="Infor",
            ))
    except Exception as e:
        logger.info(f"Infor {system}: {e}")

    logger.info(f"  Infor {system}: {len(jobs)} jobs")
    return jobs

async def run_infor(session) -> list[Job]:
    logger.info(f"Infor: scraping {len(INFOR_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_infor(session, s, o) for s, o in INFOR_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Infor total: {len(jobs):,} jobs")
    return jobs



##############################################################################
#  UKG PRO / ULTIPRO — GUID-based job board API
#  Format: ("base_url", "guid")
##############################################################################
UKG_ORGS = {
    # Relabelled 2026-09-10: this board (cchsconnect / COL1053CCHD) is Columbia
    # County Health System, Dayton WA (14 rows, all WA, in the dry run), not
    # Catawba Valley (NC). The adapter had never written a row, so no old rows
    # carry the wrong name.
    "Columbia County Health System": ("https://cchsconnect.rec.pro.ukg.net/COL1053CCHD", "c6df4630-7da9-4627-af22-819e939d86fa", "WA"),
    "Augusta University Health":    ("https://recruiting.ultipro.com/AUG1000AUG",        "02a29cd6-e7aa-4501-96be-6336647e3184"),
    "Cape Regional Health":         ("https://crhukg.rec.pro.ukg.net/CHE1503CHPE",       "09584b08-b32f-4882-8c7b-223bbd8e3851"),
    "Northwest Medical Center":     ("https://nwmedicalctr.rec.pro.ukg.net/NOR1080NWMC", "f22ba272-5440-48f3-9f0f-84f6f384d461"),
    "Guadalupe Regional Medical":   ("https://grmedcenter.rec.pro.ukg.net/GUA1500GDRM",  "42079bd4-4198-48a9-b64a-26c8b01496d6"),
    # ── 2026-09-22 Texas resume (dry-run counts): Surgery Partners' shared
    # SUR1004SRGY board hosts two Texas hospitals under their own guids;
    # Odessa Regional lives on recruiting2.ultipro.com (recruiting.ultipro.com
    # answers 404 for it). Paris Regional's ACC1011RRCM board 404s on both
    # hosts; that hospital is covered through the Lifepoint alias instead.
    "Lubbock Heart & Surgical Hospital": ("https://recruiting.ultipro.com/SUR1004SRGY",   "b20e37c3-0c12-41c6-a16d-381589bf39b0", "TX"),   # 15
    "The Physicians Centre Hospital":    ("https://recruiting.ultipro.com/SUR1004SRGY",   "67c3533b-0e5c-446c-8a5c-28a8d13de3df", "TX"),   # 17, Bryan
    "Odessa Regional Medical Center":    ("https://recruiting2.ultipro.com/QHC1000QHCS",  "3734377e-6308-45d0-b97a-c6f17d82c5e2", "TX"),   # 71
    # 2026-09-24 Texas configs, dry-run counts in the comments (all TX, with
    # bodies). Big Bend has its own board on the QHC tenant; the old
    # "Quorum Health" board (c304f8f7) answered 0 and was removed.
    "North Texas Medical Center":        ("https://chc1996.rec.pro.ukg.net/BAP1004BHST",  "552e4b54-4518-49d6-8877-99604ad0c8cc", "TX"),   # 27, Gainesville
    "South Texas Spine & Surgical Hospital": ("https://recruiting.ultipro.com/SUR1004SRGY", "ea3a4692-77b7-4cea-96ad-69072d0a99f4", "TX"),  # 9, San Antonio
    "Goodall-Witcher Healthcare":        ("https://recruiting2.ultipro.com/GOO1036GDWH",  "4a69c263-bfc3-4e3e-b725-bbb1535d1cff", "TX"),   # 10, Clifton
    "Big Bend Regional Medical Center":  ("https://recruiting2.ultipro.com/QHC1000QHCS",  "b3394170-e409-475c-a2dd-95d53697602f", "TX"),   # 20, Alpine
    "Granite Hills Medical":        ("https://recruiting.ultipro.com/GRE1050GNHP",       "2b67ecb4-00fb-4863-931a-7bf0ebcb493a"),
    "Medical Associates":           ("https://recruiting.ultipro.com/MEA1004MEVM",       "d561e1d3-aa5e-4c1b-bcf5-5319c6abdcac"),
    "Excela Health":                ("https://recruiting.ultipro.com/EXC1005EXCEH",      "a00363e2-39d4-4408-a790-fbd62f4846d8"),
    "Heritage Health":              ("https://recruiting.ultipro.com/HER1004HERIT",      "68189271-8c8d-4634-bb50-bd2edf375278"),
    "Alliant Health":               ("https://recruiting.ultipro.com/ALL1034ABHC",       "ad28382f-2fcd-4cbb-bb18-24dd71b05bce"),
    "Erie County Medical Center":   ("https://ecmc462.rec.pro.ukg.net/ERI1003ECMC",      "4d1858fb-5b2a-499b-a320-4f1f4e5bcb06"),
    "Wyoming County Community":     ("https://recruiting.ultipro.com/WYC1000WHMC",       "5e6bf310-55e9-45dd-8252-85e4c670f433"),
    "Deaconess Health":             ("https://deaconess.rec.pro.ukg.net/DEA1005DEAC",    "a1f943e7-8d4d-4348-bf5e-4664f78d3abb"),
    # NHC — National HealthCare Corporation, 69 SNFs across the Southeast
    # (2026-08-04 SNF expansion, from the CMS chain analysis). Coordinates
    # pulled from nhccare.com/careers; validated live: totalCount=1092.
    "NHC":                          ("https://recruiting2.ultipro.com/NAT1059NHTH",      "02b4dc60-27be-428b-aa7b-4f7b89a29f7a"),
    "North Mississippi Medical":    ("https://recruiting.ultipro.com/NOR1041NAHO",       "84528182-2cf7-4f42-b7ca-dbb54c6f1c10"),
    "Kern Medical":                 ("https://recruiting.ultipro.com/KER1002KERN",       "e74fb506-5af0-e4c1-999e-64d5e8414cb0"),
    "Grinnell Regional Medical":    ("https://recruiting.ultipro.com/GRI1004GHSC",       "f5d979ef-386f-4469-8178-a3801183d063"),
    "Columbia Regional Medical":    ("https://recruiting.ultipro.com/COL1042CRME",       "5ac3f35f-7e01-49ff-ad53-0acc27b4cee7"),
    "Crisp Regional Health":        ("https://recruiting.ultipro.com/CRI1005CRISP",      "c74342f0-7984-4858-8545-16e720353c82"),
    "South Georgia Health":         ("https://sghsukg.rec.pro.ukg.net/SOU1076SOUG",      "2de20fad-cb3f-4525-87cd-7bd1d3c2a720"),
    "Murray-Calloway County":       ("https://murray.rec.pro.ukg.net/MUR1004MCCH",       "78a4032f-cda1-471d-86f1-9e64991ed7d2"),
    "TJ Regional Health":           ("https://tjregional.rec.pro.ukg.net/TJS1500TJSC",   "a4b9e606-5dc1-4c8c-ba68-83fd41e97ade"),
    "Lakewood Health":              ("https://recruiting2.ultipro.com/SKY1006LAKES",     "9dcd58e9-9155-4226-9b21-f476fcd1d29b"),
    # ── 2026-09-10 (S-scraper-2): contract clients on UKG Pro Recruiting. An
    # optional third element is the state (run order + default when the
    # opportunity carries none). Springhill's board was found by web search
    # (the hospital's own careers page 403s every crawler, WebFetch included).
    "Springhill Medical Center":    ("https://springhill.rec.pro.ukg.net/SPR1500SHSL", "a6346066-8a35-4fb0-b665-fc48587cb154", "AL"),
}

_UKG_PAGE = 50


def _ukg_job(j: dict, system: str, base_url: str, guid: str, default_state: str = "") -> Job:
    """One LoadSearchResults opportunity -> Job (hoisted 2026-09-10 so the
    fixture test covers it). UKG Pro Recruiting answers in PascalCase (Id,
    Title, Locations[].Address.City / .State.Code, PostedDate, FullTime,
    JobCategoryName, BriefDescription); the camelCase keys the earlier build
    guessed at are still read second. The detail deep link is
    /JobBoard/<guid>/OpportunityDetail?opportunityId=<Id>; the old
    "?detail=" form opened the board's front page."""
    def g(*keys, default=""):
        for k in keys:
            v = j.get(k)
            if v not in (None, ""):
                return v
        return default
    locs = g("Locations", "locations", default=[]) or []
    loc0 = locs[0] if locs and isinstance(locs[0], dict) else {}
    addr = loc0.get("Address") or loc0.get("address") or {}
    city = str(addr.get("City") or addr.get("city") or g("city", "City") or "").strip()
    st = addr.get("State") or addr.get("state") or g("state", "State") or ""
    if isinstance(st, dict):
        st = st.get("Code") or st.get("code") or st.get("Name") or st.get("name") or ""
    st = str(st).strip()
    if len(st) > 2:
        st = parse_city_state(f"{city}, {st}")[1] or st
    if not city and not st:
        city, st = parse_city_state(str(g("location", "Location", "formattedLocation")))
    st = (st or default_state).upper()
    facility = str(loc0.get("LocalizedName") or loc0.get("LocalizedDescription")
                   or loc0.get("Name") or loc0.get("name") or "").strip()
    if (not facility or "," in facility or re.search(r"\d", facility)
            or facility.lower() in _US_STATE_CODES or facility.upper() in _US_STATE_CODES.values()):
        facility = system          # "City, ST", street addresses and region names ("Alabama") are not facilities
    opp_id = str(g("Id", "id", "opportunityId", "OpportunityId")).strip()
    full = j.get("FullTime")
    job_type = str(g("employmentType", "workHours") or
                   ("Full time" if full is True else "Part time" if full is False else ""))
    return Job(
        title=str(g("Title", "title")).strip(),
        hospital_system=system,
        hospital_name=facility,
        city=city, state=st,
        location=f"{city}, {st}".strip(", "),
        specialty=str(g("JobCategoryName", "jobCategory", "category")),
        job_type=job_type,
        url=f"{base_url}/JobBoard/{guid}/OpportunityDetail?opportunityId={opp_id}",
        job_id=opp_id,
        posted_date=str(g("PostedDate", "postedDate"))[:10],
        description=strip_html(g("BriefDescription", "shortDescription", "description")),
        ats_platform="UKG",
    )


async def scrape_ukg(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    base_url, guid = org_data[0], org_data[1]
    default_state = org_data[2] if len(org_data) > 2 else ""
    jobs = []
    # Confirmed endpoint from network intercept on Deaconess
    api = f"{base_url}/JobBoard/{guid}/JobBoardView/LoadSearchResults"
    offset = 0
    while True:
        try:
            payload = {
                "opportunitySearch": {
                    "Top": _UKG_PAGE,
                    "Skip": offset,
                    "QueryString": "",
                    "OrderBy": [{"Value": "postedDateDesc", "PropertyName": "PostedDate", "Ascending": False}],
                    "Filters": [],
                },
                "deviceType": "desktop",
                "recommendationSettings": {},
            }
            async with req(session, "post", api,
                json=payload,
                headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json",
                         "Referer": f"{base_url}/JobBoard/{guid}/", "X-Requested-With": "XMLHttpRequest"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"UKG {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            # Response: {"opportunities": [...], "totalCount": N}
            items = data.get("opportunities", data.get("Opportunities", [])) if isinstance(data, dict) else []
            if not items:
                break
            for j in items:
                job = _ukg_job(j, system, base_url, guid, default_state)
                if job.job_id and job.title:
                    jobs.append(job)
            total = int(data.get("totalCount", data.get("total", data.get("Total", 0))) or 0)
            offset += _UKG_PAGE
            if offset >= total or len(items) < _UKG_PAGE:
                break
            await jitter()
        except Exception as e:
            logger.info(f"UKG {system}: {e}")
            break
    logger.info(f"  UKG {system}: {len(jobs)} jobs")
    return jobs

async def run_ukg(session) -> list[Job]:
    logger.info(f"UKG: scraping {len(UKG_ORGS)} systems...")
    orgs = priority_states_first(list(UKG_ORGS.items()),
                                 lambda kv: kv[1][2] if len(kv[1]) > 2 else "")
    # 2026-09-24: most boards share recruiting.ultipro.com / recruiting2.ultipro.com;
    # HOST_CONCURRENCY at a time per host.
    results = await _gather_by_host(session, orgs, scrape_ukg, lambda o: _url_host(o[0]))
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  UKG total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  ORACLE HCM CLOUD — REST search endpoint
#  Format: ("base_url",)  — base includes full path up to /sites/{site}
##############################################################################
ORACLE_ORGS = {
    # Format: "System": ("https://{oracle-subdomain}.oraclecloud.com", "siteNumber")
    # siteNumber extracted from original career site URLs (/sites/{siteNumber})
    # API: GET {base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions
    #      with finder=findReqs;siteNumber={siteNumber},limit=N,offset=N
    # Northwell Health (2026-08-04). Was misconfigured as a Workday tenant and
    # 422'd on every run since May — jobs.northwell.edu is actually an Oracle
    # Cloud front (host + siteNumber pulled from the landing page's deeplink,
    # validated live: TotalJobsCount=1286 on this endpoint). NY's largest
    # private employer; expect this to grow as their sites are enumerated.
    "Northwell Health":          ("https://eppr.fa.us2.oraclecloud.com",                      "CX_2"),
    # Northwell runs several CE sites on the same instance (enumerated
    # 2026-08-04: CX_1=367, CX_2=1291, CX_3=1370 — pools overlap heavily).
    # Both alias back to "Northwell Health" and the (job_id, hospital_system)
    # unique key collapses cross-site duplicates on upsert.
    "Northwell Health (CX_1)":   ("https://eppr.fa.us2.oraclecloud.com",                      "CX_1"),
    "Northwell Health (CX_3)":   ("https://eppr.fa.us2.oraclecloud.com",                      "CX_3"),
    "Jackson Hospital":          ("https://ejid.fa.us6.oraclecloud.com",                      "CX_1001"),
    # 2026-09-16 (NY coverage): careers.mountsinai.org fronts this instance;
    # TotalJobsCount 1,770 on 2026-09-16, New York NY primary locations.
    "Mount Sinai Health System": ("https://ejis.fa.us6.oraclecloud.com",                      "CX_1"),
    # 2026-09-17 (coverage lever 3): TotalJobsCount probed live the same day.
    "Baptist Memorial Health Care": ("https://fa-ewpe-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_1"),   # 1,601; TN/MS/AR
    "TriHealth":                 ("https://fa-evly-saasfaprod1.fa.ocs.oraclecloud.com",           "CX_1"),   # 525; Cincinnati
    "Loma Linda University Health": ("https://egln.fa.us2.oraclecloud.com",                      "CX_1"),   # 459
    "UW Health":                 ("https://eimy.fa.us6.oraclecloud.com",                          "CX_1"),   # 940; Madison WI
    "Franciscan Missionaries of Our Lady Health System": ("https://eqtm.fa.us2.oraclecloud.com",  "CX_1"),   # 834; LA/MS
    "UC Health":                 ("https://eswt.fa.us6.oraclecloud.com",                          "CX_1"),   # 426; Cincinnati
    # ── 2026-09-10 Texas block C (Y-texas-build): site CX_1 on all three,
    # validated through scrape_oracle on 2026-09-10 (Texas Children's 412
    # rows, 98% TX, Houston + Austin; United Regional 79, Wichita Falls;
    # UT Health San Antonio 221, San Antonio). No structured pay on any.
    "Texas Children's":                   ("https://eohh.fa.us2.oraclecloud.com",                 "CX_1"),
    "United Regional Health Care System": ("https://iaoxqy.fa.ocs.oraclecloud.com",               "CX_1"),
    "UT Health San Antonio":              ("https://fa-eomf-saasfaprod1.fa.ocs.oraclecloud.com",  "CX_1"),
    # elar/CX_1 was mislabeled "Erlanger Health System" and never banked a
    # row under that name; careers.inova.org redirect confirms it is INOVA
    # (validated 2026-08-28, TotalJobsCount=682).
    "Inova Health System":       ("https://elar.fa.us2.oraclecloud.com",                      "CX_1"),
    "EvergreenHealth":           ("https://erym.fa.us6.oraclecloud.com",                      "CX_1"),
    "Valley Health (NV)":        ("https://fa-eveq-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_1"),
    "Mount Nittany Health":      ("https://mnh-ibosjb.fa.ocs.oraclecloud.com",               "MountNittanyHealthCareers"),
    # ertr/CX_3001 was labelled "Trinity Health (Oregon)" (aliased to Trinity
    # Health), but every row it wrote is in Oklahoma: it is INTEGRIS Health
    # (673 active OK rows on 2026-09-24, Oklahoma City, Edmond, Enid...).
    # Renamed 2026-09-24; the old rows need a one-shot relabel (post-push SQL).
    "INTEGRIS Health":           ("https://ertr.fa.us2.oraclecloud.com",                      "CX_3001"),
    # 2026-09-24 configs: UCSF Health, TotalJobsCount 820 probed live.
    "UCSF Health":               ("https://iazuqy.fa.ocs.oraclecloud.com",                    "CX_1"),
    "Memorial Hospital":         ("https://wearememorial-ibrkjb.fa.ocs.oraclecloud.com",      "Careers"),
    # ecvz/CX_1 was mislabeled "Cape Cod Healthcare" — its 1,421 banked rows
    # sit in CA/HI/OR and careers.adventisthealth.org redirects here: this is
    # ADVENTIST HEALTH (validated 2026-08-28, TotalJobsCount=1,430). Renamed;
    # the old Cape Cod rows need a one-shot DB relabel (post-push step).
    "Adventist Health":          ("https://ecvz.fa.us2.oraclecloud.com",                      "CX_1"),
    "Flagler Health":            ("https://erou.fa.us2.oraclecloud.com",                      "CX_1"),
    "Eastern Connecticut Health":("https://eglz.fa.us2.oraclecloud.com",                      "CX"),
    "Guthrie Health":            ("https://elfw.fa.us2.oraclecloud.com",                      "CX_1001"),  # confirmed
    "Valley Children's":         ("https://epyz.fa.us2.oraclecloud.com",                      "CX_1"),
    "Southwest Health":          ("https://fa-exgl-saasfaprod1.fa.ocs.oraclecloud.com",       "JoinOurTeam"),
    "HealthPartners":            ("https://fa-etnv-saasfaprod1.fa.ocs.oraclecloud.com",       "healthpartners"),
    "United Regional":           ("https://erqh.fa.us2.oraclecloud.com",                      "CX_1001"),
    "Unknown (fa-eyip)":         ("https://fa-eyip-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_4001"),
    # ── Added 2026-05-13: post-acute expansion Phase 1 (verified) ──
    # VITAS Healthcare: largest US hospice operator (~30K patients/day).
    # Confirmed via redirect from www.vitas.com/careers → ejrz.fa.us2.oraclecloud.com.
    "VITAS Healthcare":          ("https://ejrz.fa.us2.oraclecloud.com",                      "CX_5001"),
    # Brookdale Senior Living: ~650 senior living + memory care + SNF communities.
    # Confirmed via careers.brookdale.com — "Search Jobs" buttons point to ibmwjb.fa.ocs.
    "Brookdale Senior Living":   ("https://ibmwjb.fa.ocs.oraclecloud.com",                    "CX_1"),
    # ── 2026-08-28 dark-systems resurrection: both migrated OFF dead Workday
    # tenants onto Oracle; validated live via recruitingCEJobRequisitions.
    "Indiana University Health": ("https://ekcm.fa.us6.oraclecloud.com",                      "CX"),      # 1,069
    "WellSpan Health":           ("https://fa-evzu-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_1"),    # 1,049
    # ── Added 2026-05-26: Phase 2 non-acute expansion (verified Oracle HCM 200) ──
    # Encompass Health: ~160 inpatient rehab hospitals across the US. Confirmed
    # via careers.encompasshealth.com job listing - all 'Apply' URLs route through
    # ibwsjb.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX/job/...
    # Endpoint validated 2026-05-26: ibwsjb + siteNumber=CX returns
    # TotalJobsCount=2157 with full requisitionList.items[] populated.
    "Encompass Health":          ("https://ibwsjb.fa.ocs.oraclecloud.com",                    "CX"),
    # ── Added 2026-05-29: Phase 3 non-acute expansion (verified Oracle HCM 200) ──
    # Lifepoint Health (behavioral + community hospitals + rehab). Endpoint
    # validated 2026-05-29: ibnjjb + siteNumber=CX_1 returns TotalJobsCount=3814.
    "Lifepoint Health":          ("https://ibnjjb.fa.ocs.oraclecloud.com",                    "CX_1"),
    # ── Added 2026-06-18: top missing acute-care systems on Oracle HCM (validated) ──
    # Providence: migrated off Workday to Oracle. evac/CX_1 → TotalJobsCount=1,877.
    "Providence Health":         ("https://evac.fa.us2.oraclecloud.com",                      "CX_1"),
    # Tenet Healthcare: eodr/CX_1001 → TotalJobsCount=2,363.
    "Tenet Healthcare":          ("https://eodr.fa.us2.oraclecloud.com",                      "CX_1001"),
    # Mayo Clinic: was a broken TalentBrew HTML scrape (~14 jobs); Mayo runs on
    # Oracle HCM now. fa-euwp-saasfaprod1/Mayo-US → TotalJobsCount=1,318.
    "Mayo Clinic":               ("https://fa-euwp-saasfaprod1.fa.ocs.oraclecloud.com",       "Mayo-US"),
}

# 2026-09-16 (NY coverage): Mount Sinai site names in requisition titles ->
# (facility, city). Order matters: "Mount Sinai Hospital" must not swallow
# "Mount Sinai Hospital of Queens"-style titles, so the specific sites go first.
MOUNT_SINAI_SITES = [
    (r"south nassau|\bMSSN\b",                 ("Mount Sinai South Nassau", "Oceanside")),
    (r"mount sinai queens|\bMSQ\b",            ("Mount Sinai Queens", "Queens")),
    (r"mount sinai brooklyn|\bMSB\b",          ("Mount Sinai Brooklyn", "Brooklyn")),
    (r"morningside|\bMSM\b",                   ("Mount Sinai Morningside", "New York")),
    (r"mount sinai west|\bMSW\b",              ("Mount Sinai West", "New York")),
    (r"beth israel|\bMSBI\b",                  ("Mount Sinai Beth Israel", "New York")),
    (r"kravis",                                ("Kravis Children's Hospital at Mount Sinai", "New York")),
    (r"eye and ear|\bNYEE\b",                  ("New York Eye and Ear Infirmary of Mount Sinai", "New York")),
    (r"union square",                          ("Mount Sinai Union Square", "New York")),
    (r"chelsea",                               ("Mount Sinai Chelsea", "New York")),
    (r"icahn",                                 ("Icahn School of Medicine at Mount Sinai", "New York")),
    (r"at home",                               ("Mount Sinai at Home", "New York")),
    (r"mount sinai doctors",                   ("Mount Sinai Doctors", "New York")),
    (r"mount sinai hospital|\bMSH\b|the mount sinai\b", ("The Mount Sinai Hospital", "New York")),
]
_MOUNT_SINAI_SITES_RE = [(re.compile(pat, re.I), val) for pat, val in MOUNT_SINAI_SITES]


def _mount_sinai_loc(title: str, city: str, state: str) -> tuple[str, str, str]:
    """(hospital_name, city, state) for a Mount Sinai requisition.

    A located row keeps its city and state and only gains the facility; an
    unlocated one takes the site's city; a row with no site and no location
    defaults to New York, NY."""
    facility = "Mount Sinai Health System"
    site_city = ""
    for rx, (name, c) in _MOUNT_SINAI_SITES_RE:
        if rx.search(title or ""):
            facility, site_city = name, c
            break
    if state:
        # Located rows keep their location; a New York row with no city takes
        # the site's city; a New Jersey row with no city is left alone.
        return facility, (city or (site_city if state == "NY" else "")), state
    return facility, (site_city or "New York"), "NY"


async def scrape_oracle(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    base_url, site_number = org_data
    jobs = []
    # Oracle HCM recruiting API. Confirmed shape (2026-05-14, VITAS):
    #   {
    #     "items": [ <single SearchResult metadata wrapper> ],   <-- top-level
    #     "count": 1, "hasMore": false, "limit": ..., "offset": ...
    #   }
    # The SearchResult wrapper holds the actual jobs nested:
    #   items[0] = {
    #     "TotalJobsCount": <int>,
    #     "Limit": <int>, "Offset": <int>,
    #     "requisitionList": {
    #       "items": [ <-- THESE are the actual jobs (Title, Id, etc.) -->,
    #                  ... up to `limit` rows ... ],
    #       "count": <int>, "hasMore": <bool>
    #     }
    #   }
    # The previous version of this scraper iterated `data.items` directly,
    # treating the SearchResult wrapper itself as a job — which is why every
    # Oracle tenant except one (HealthPartners, by accident) returned 0 jobs.
    # Fix: dive two levels deeper to data.items[0].requisitionList.items.
    api = f"{base_url}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    offset = 0
    limit  = 25
    while True:
        try:
            # Bug fix (2026-05-29): the top-level limit/offset query params
            # paginate the OUTER resourcecollection — which only ever holds a
            # single SearchResult wrapper — so they were a no-op. Every Oracle
            # tenant was therefore stuck on page 1 (exactly 25 jobs each, e.g.
            # Encompass 25/2168, Brookdale 25/2096, Lifepoint 26/3790). The
            # real pagination knobs live INSIDE the finder predicate. Passing
            # limit/offset in BOTH places paginates correctly and is verified
            # safe against the previously-"working" tenants.
            params = {
                "finder":  (f"findReqs;siteNumber={site_number},sortBy=POSTING_DATES_DESC,"
                            f"limit={limit},offset={offset}"),
                "expand":  "requisitionList.workLocation,requisitionList.secondaryLocations",
                "limit":   limit,
                "offset":  offset,
                "totalResults": "true",
            }
            async with req(session, "get", api, params=params,
                headers={**HEADERS,
                         "Accept": "application/vnd.oracle.adf.resourcecollection+json",
                         "REST-Framework-Version": "4"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Oracle {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            search_results = data.get("items", [])
            if not search_results:
                break
            search_result = search_results[0]
            total = int(search_result.get("TotalJobsCount", 0) or 0)
            req_list_wrapper = search_result.get("requisitionList") or {}
            page_items = req_list_wrapper.get("items", []) if isinstance(req_list_wrapper, dict) else []
            if not page_items:
                break
            for j in page_items:
                loc = j.get("PrimaryLocation", j.get("primaryLocation", ""))
                if isinstance(loc, dict):
                    loc = loc.get("Name", loc.get("name", ""))
                _city, _state = parse_city_state(str(loc))
                _facility = system
                if system == "Mount Sinai Health System":
                    _facility, _city, _state = _mount_sinai_loc(j.get("Title", j.get("title", "")), _city, _state)
                func = j.get("JobFunction", j.get("jobFunction", ""))
                if isinstance(func, dict):
                    func = func.get("Name", func.get("name", ""))
                jobs.append(Job(
                    title=j.get("Title", j.get("title", "")),
                    hospital_system=system,
                    hospital_name=_facility,
                    city=_city, state=_state, location=str(loc),
                    specialty=str(func) if func else "",
                    job_type=j.get("WorkHours", j.get("workHours", "")) or "",
                    # 2026-07-01: candidate URL must be singular "/job/{id}".
                    # The plural "/jobs/{id}" renders Oracle's "Page not found"
                    # page (a live render test confirmed this across 8 tenants),
                    # so ~22k Oracle apply links were dead. Singular resolves.
                    url=f"{base_url}/hcmUI/CandidateExperience/en/sites/{site_number}/job/{j.get('Id', j.get('id', ''))}",
                    job_id=str(j.get("Id", j.get("id", j.get("RequisitionNumber", "")))),
                    posted_date=str(j.get("PostedDate", j.get("postedDate", "")))[:10],
                    description="",
                    ats_platform="Oracle HCM",
                ))
            # Pagination: Oracle's top-level limit/offset paginate the
            # requisitionList contents. Stop when we've fetched the reported
            # total, or when the page came back short.
            offset += limit
            if total and offset >= total:
                break
            if len(page_items) < limit:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Oracle {system}: {e}")
            break
    if DETAIL_FETCH and jobs:
        try:
            await _detail_pass(session, system, jobs, ORACLE_DESC_BUDGET,
                               lambda j: _oracle_detail(session, base_url, site_number, j), "Oracle")
        except Exception as e:
            logger.info(f"Oracle {system}: detail pass failed ({e})")
    logger.info(f"  Oracle {system}: {len(jobs)} jobs")
    return jobs

async def run_oracle(session) -> list[Job]:
    logger.info(f"Oracle HCM: scraping {len(ORACLE_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_oracle(session, s, o) for s, o in ORACLE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Oracle HCM total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  HEALTHCARESOURCE PM — hospital-specific tenant API
#  Format: ("tenant_slug",)
##############################################################################
HEALTHCARESOURCE_ORGS = {
    "Ellis Medicine":           "ellishospital",   # 2026-09-16 (NY coverage), pm.healthcaresource.com/cs/ellishospital
    "Parkview Health":          "pvh",             # 2026-09-17 (lever 3), Fort Wayne IN
    "Renown Health":            "renownhealth",    # 2026-09-17 (lever 3), Reno NV
    "Central Valley Medical":   "centralvalleymedicalcenter",
    "RMCM":                     "rmcm",
    "CRMC Health":              "crmchealth",
    "AnMed Health":             "anmed",
    "CaroMont Health":          "caromont",
    "Randolph Health":          "randolph",
    "Scotland Health":          "scotland",
    "Carteret Health Care":     "carteret",
    "Stillwater Medical":       "stillwater",
    "Crouse Health":            "crouse",
    "York Hospital":            "yorkhospital",
    "Lake Regional Health":     "lakeregional",
    "Liberty Hospital":         "liberty",
    "Forrest Health":           "forresthealth",
    "MRHC":                     "mrhc",
    "Brattleboro Memorial":     "bch",
    "Waterbury Hospital":       "waterbury",
    "ECHN":                     "echn",
    "Archbold Medical":         "archbold",
    "Kootenai Health":          "kootenai",
    "Community Memorial":       "comhs",
    "Union Hospital":           "unionhospital",
    "Hays Medical Center":      "haysmed",
    "CHC Healthcare":           "chc",
    "Lawrence General":         "lawrence",
    "Holyoke Health":           "Holyokehealth",
    "Sarasota Memorial":        "smh",
    # 2026-09-10 (Z-texas-acute-D): Texas block D, keys read from the /cs/<tenant>
    # links on shannonhealth.com/careers and mchodessa.com/careers.
    "Shannon Medical Center":   "shannonhealth",
    "Medical Center Health System (Odessa)": "medicalcenterhealth",
}

def _dig(d, *path, default=""):
    """Nested dict read: _dig(src, "jobLocation", "address", "addressLocality")."""
    cur = d
    for p in path:
        if isinstance(cur, list):
            cur = cur[0] if cur else None
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if isinstance(cur, list):
        cur = cur[0] if cur else None
    return default if cur in (None, "") else cur


def _hcs_job(hit: dict, system: str, tenant: str) -> Job | None:
    """One Elasticsearch hit from JobseekerSearchAPI -> Job (2026-09-10).
    Field names follow the schema.org-style document the client bundle
    filters on (jobLocation.address.addressLocalityRegion, userArea.
    jobPostingID); every read has a plain-key fallback."""
    src_ = hit.get("_source") if isinstance(hit.get("_source"), dict) else hit
    job_id = str(_dig(src_, "userArea", "jobPostingID") or src_.get("jobPostingID") or src_.get("requisitionId")
                 or src_.get("jobId") or src_.get("id") or hit.get("_id") or "").strip()
    title = str(src_.get("title") or src_.get("jobTitle") or _dig(src_, "userArea", "title") or "").strip()
    if not job_id or not title:
        return None
    city = str(_dig(src_, "jobLocation", "address", "addressLocality") or src_.get("city") or "").strip()
    st = str(_dig(src_, "jobLocation", "address", "addressRegion") or src_.get("state") or "").strip()
    if not (city and st):
        c2, s2 = parse_city_state(str(_dig(src_, "jobLocation", "address", "addressLocalityRegion") or src_.get("location") or ""))
        city, st = city or c2, st or s2
    if len(st) > 2:
        st = parse_city_state(f"{city}, {st}")[1] or st
    # 2026-09-10: hiringOrganization.subOrganization.name is the facility
    # ("Shannon Medical Center", "Medical Center Hospital", both exact CMS
    # names); .name is the system ("Shannon Health"); a further nested
    # subOrganization is the department ("LD ROOMS", skipped).
    sub = str(_dig(src_, "hiringOrganization", "subOrganization", "name") or "").strip()
    facility = sub if sub and not sub.isupper() and not re.search(r"\d", sub) else ""
    facility = facility or str(_dig(src_, "jobLocation", "name") or _dig(src_, "hiringOrganization", "name")
                               or src_.get("facilityName") or src_.get("facility") or "").strip()
    return Job(
        title=title,
        hospital_system=system,
        hospital_name=facility or system,
        city=city, state=st.upper(),
        location=f"{city}, {st.upper()}".strip(", "),
        specialty=str(src_.get("occupationalCategory") or src_.get("category") or _dig(src_, "userArea", "category") or ""),
        job_type=str(src_.get("employmentType") or _dig(src_, "userArea", "employmentType") or ""),
        url=f"https://pm.healthcaresource.com/cs/{tenant}#/job/{job_id}",
        job_id=job_id,
        posted_date=str(src_.get("datePosted") or src_.get("postedDate") or "")[:10],
        description=strip_html(str(src_.get("description") or "")),
        ats_platform="HealthcareSource",
    )


# 2026-09-10 (Z-texas-acute-D): the endpoint is an Elasticsearch proxy. GET
# answers 405; the earlier minimal POST body {"size","from"} answered 500
# ("Cannot perform runtime binding on a null reference") for every tenant,
# so this adapter had never returned a row. The client bundle (CS/build/
# client.bundle.js, esQueryGenerator) posts a bool query and passes the page
# size as a query-string parameter (searchEndpoint + "?size=N").
_HCS_PAGE = 50
_HCS_BODY = {"query": {"bool": {"must": {"match_all": {}}}}}   # 2026-09-10: "*" alone matches nothing


async def scrape_healthcaresource(session: aiohttp.ClientSession, system: str, tenant: str) -> list[Job]:
    jobs: list[Job] = []
    api = f"https://pm.healthcaresource.com/JobseekerSearchAPI/{tenant}/api/Search"
    offset = 0
    while True:
        try:
            async with req(session, "post", api,
                params={"size": _HCS_PAGE, "from": offset},
                json=_HCS_BODY,
                headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json; charset=utf-8",
                         "Referer": f"https://pm.healthcaresource.com/cs/{tenant}"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"HealthcareSource {system}: HTTP {r.status} {(await r.text())[:120]}")
                    break
                data = await r.json(content_type=None)
            hits = (data or {}).get("hits") or {}
            items = hits.get("hits") or []
            if not items:
                break
            for h in items:
                job = _hcs_job(h, system, tenant)
                if job:
                    jobs.append(job)
            total = hits.get("total")
            total = int((total or {}).get("value", 0)) if isinstance(total, dict) else int(total or 0)
            offset += len(items)
            if offset >= total or len(items) < _HCS_PAGE:
                break
            await jitter()
        except Exception as e:
            logger.info(f"HealthcareSource {system}: {e}")
            break
    logger.info(f"  HealthcareSource {system}: {len(jobs)} jobs")
    return jobs

async def run_healthcaresource(session) -> list[Job]:
    logger.info(f"HealthcareSource: scraping {len(HEALTHCARESOURCE_ORGS)} systems...")
    # 2026-09-24: every tenant is on pm.healthcaresource.com; HOST_CONCURRENCY at a time.
    results = await _gather_by_host(session, list(HEALTHCARESOURCE_ORGS.items()),
                                    scrape_healthcaresource, lambda t: "pm.healthcaresource.com")
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  HealthcareSource total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  TENET HEALTH — custom career site with JSON search API
##############################################################################
TENET_BRANDS = {
    "Baptist Health System (TX)":   "Baptist Health System",
    "Valley Baptist Health System": "Valley Baptist Health System",
    "The hospitals of Providence":  "The hospitals of Providence",
    "Pittsburgh-area facilities":   "Pittsburgh",
    "Detroit Medical Center":       "Detroit Medical Center",
}

async def scrape_tenet(session: aiohttp.ClientSession, system: str, brand: str) -> list[Job]:
    jobs = []
    # Must use POST — the URL-encoded JSON filter exceeds aiohttp's 8190-byte header limit as GET params
    api = "https://jobs.tenethealth.com/search-jobs/results"
    offset = 0
    while True:
        try:
            payload = {
                "orgIds": "30315",
                "ascf": [{"key": "custom_fields.CustomBrand", "value": brand}],
                "from": offset, "num": 25,
            }
            async with req(session, "post", api,
                json=payload,
                headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Tenet {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            items = data.get("eagerLoadRefineSearch", {}).get("data", {}).get("jobs", [])
            if not items:
                break
            for j in items:
                loc = j.get("jobLocation", j.get("Location", ""))
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system="Tenet Health",
                    hospital_name=system,
                    city=_city, state=_state, location=loc,
                    specialty=j.get("industry", ""),
                    job_type=j.get("jobType", ""),
                    url=f"https://jobs.tenethealth.com/{j.get('canonicalPositionUrl','')}",
                    job_id=str(j.get("jobId", "")),
                    posted_date=str(j.get("postedDate", ""))[:10],
                    description="",
                    ats_platform="Tenet",
                ))
            total = data.get("eagerLoadRefineSearch", {}).get("data", {}).get("totalJobsCount", 0)
            offset += 25
            if offset >= total:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Tenet {system}: {e}")
            break
    logger.info(f"  Tenet {system}: {len(jobs)} jobs")
    return jobs

async def run_tenet(session) -> list[Job]:
    logger.info(f"Tenet: scraping {len(TENET_BRANDS)} brands...")
    results = await asyncio.gather(
        *[scrape_tenet(session, s, b) for s, b in TENET_BRANDS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Tenet total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  TRINITY HEALTH — Jibe-based career portal with JSON search API
##############################################################################
TRINITY_ORGS = {
    "St. Peter's Health Partners":  "https://jobs.trinity-health.org/stpetershealthpartners",
    "Loyola Medicine":              "https://jobs.trinity-health.org/loyolamedicine",
    "Saint Alphonsus":              "https://jobs.trinity-health.org/saintalphonsus",
    "MercyOne":                     "https://jobs.trinity-health.org/mercyone",
    "Holy Cross Health":            "https://jobs.trinity-health.org/holycrosshealth",
}

async def scrape_trinity(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    jobs = []
    # Trinity/Jibe career portals use GET /search-results?m=3&pg=N&pgcnt=N
    # The ?m=3 parameter appears to be required (sort mode).
    # Add Accept: application/json to request JSON response instead of HTML.
    api  = f"{base_url}/search-results"
    page = 1
    while True:
        try:
            params = {"m": "3", "pg": page, "pgcnt": 25}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json, text/javascript, */*"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Trinity {system}: HTTP {r.status}")
                    break
                ct = r.headers.get("content-type", "")
                if "json" not in ct:
                    # Some Jibe sites return HTML — check for JSON in body anyway
                    text = await r.text()
                    try:
                        import json as _json
                        data = _json.loads(text)
                    except Exception:
                        logger.info(f"Trinity {system}: non-JSON response (HTML page)")
                        break
                else:
                    data = await r.json(content_type=None)

            # Jibe response keys vary by version
            items = (data.get("jobs") or
                     data.get("requisitionList") or
                     data.get("results") or [])
            if not items:
                logger.info(f"Trinity {system}: empty — keys={list(data.keys())[:8]}")
                break
            for j in items:
                loc = j.get("location", j.get("primaryLocation",
                            j.get("jobLocation", "")))
                if isinstance(loc, dict):
                    loc = loc.get("name", loc.get("Name", ""))
                _city, _state = parse_city_state(str(loc))
                jobs.append(Job(
                    title=j.get("title", j.get("Title", "")),
                    hospital_system="Trinity Health",
                    hospital_name=system,
                    city=_city, state=_state, location=str(loc),
                    specialty=j.get("category", j.get("jobFunction", "")),
                    job_type=j.get("type", j.get("workHours", "")),
                    url=j.get("applyUrl", j.get("detailUrl",
                              f"{base_url}/jobs/{j.get('id', j.get('jobId', ''))}")),
                    job_id=str(j.get("id", j.get("jobId", j.get("Id", "")))),
                    posted_date=str(j.get("postedDate", j.get("PostedDate", "")))[:10],
                    description="",
                    ats_platform="Jibe",
                ))
            total = (data.get("totalJobsCount") or
                     data.get("total") or
                     data.get("count") or 0)
            if page * 25 >= total or len(items) < 25:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Trinity {system}: {e}")
            break
    logger.info(f"  Trinity {system}: {len(jobs)} jobs")
    return jobs

async def run_trinity(session) -> list[Job]:
    logger.info(f"Trinity Health: scraping {len(TRINITY_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_trinity(session, s, u) for s, u in TRINITY_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Trinity total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  UHS INC — custom career site, brand-filtered
##############################################################################
UHS_BRANDS = {
    "South Texas Health System":       "south-texas-health-system",
    "Texoma Medical Center":           "texoma-medical-center",
    "Aiken Regional Medical":          "aiken-regional-medical-centers",
    "St. Mary's Regional Medical":     "st-marys-regional-medical-center",
    "Northern Nevada Health System":   "the-northern-nevada-health-system",
    "Valley Health System (NV)":       "the-valley-health-system",
    "Southwest Healthcare":            "southwest-healthcare",
    "Wellington Regional Medical":     "wellington-regional-medical-center",
}

async def scrape_uhs(session: aiohttp.ClientSession, system: str, brand: str) -> list[Job]:
    jobs = []
    api = f"https://jobs.uhsinc.com/{brand}/jobs-data"
    page = 1
    while True:
        try:
            params = {"page": page, "pageSize": 25}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    break
                data = await r.json(content_type=None)
            items = data.get("jobs", data.get("results", []))
            if not items:
                break
            for j in items:
                loc = j.get("location", j.get("jobLocation", ""))
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system="UHS",
                    hospital_name=system,
                    city=_city, state=_state, location=loc,
                    specialty=j.get("category", ""),
                    job_type=j.get("employmentType", ""),
                    url=f"https://jobs.uhsinc.com/{brand}/jobs/{j.get('id','')}",
                    job_id=str(j.get("id", j.get("jobId", ""))),
                    posted_date=str(j.get("postedDate", ""))[:10],
                    description="",
                    ats_platform="UHS",
                ))
            total = data.get("total", data.get("totalCount", 0))
            if page * 25 >= total:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"UHS {system}: {e}")
            break
    logger.info(f"  UHS {system}: {len(jobs)} jobs")
    return jobs

async def run_uhs(session) -> list[Job]:
    logger.info(f"UHS: scraping {len(UHS_BRANDS)} brands...")
    results = await asyncio.gather(
        *[scrape_uhs(session, s, b) for s, b in UHS_BRANDS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  UHS total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  LIFEPOINT HEALTH — brand-filtered subdomain job listings
##############################################################################
##############################################################################
#  LIFEPOINT HEALTH — moved to Playwright (site rebuilt on WordPress 2025)
#  Old /brand/jobs-data API is dead. Now scraped via run_playwright_scrapers.
##############################################################################
async def run_lifepoint(session) -> list[Job]:
    # LifePoint is now handled by Playwright — this stub keeps run_all() intact
    return []


##############################################################################
#  KRONOS (Legacy Workforce Ready) — mykronos.com career portal
#  Format: "System": ("subdomain", "company_id")
#  API: GET /ta/rest/ui/recruitment/companies/|{id}/job-requisitions
##############################################################################
KRONOS_ORGS = {
    "Astria Health":    ("prd01-hcm01.prd", "6110092"),
    "ArnotHealth":      ("prd01-hcm01.npr", "6012355"),
    "Ridgeview":        ("prd01-hcm01.prd", "6104389"),
    # ── Added from scraper1.xlsx expansion ──
    "Kronos Hospital 2": ("prd01-hcm01.prd", "6059921"),
    "Kronos Hospital 3": ("prd01-hcm01.prd", "6142380"),
    # 2026-09-10 (S-scraper-2): contract client. magruderhospital.com/about-us/
    # careers links prd01-hcm01.npr.mykronos.com/ta/6070232.careers, i.e. UKG
    # Ready (this adapter), not UKG Dimensions; the optional third element is
    # the state (run order + default).
    "Magruder Hospital": ("prd01-hcm01.npr", "6070232", "OH"),
    # 2026-09-10 (Z-texas-acute-D): wghospital.com links secure7.saashr.com/ta/
    # 6215251.careers, the same UKG Ready REST API on the older saashr.com
    # host; a value with a full host name is used as-is. 12 rows, all Vernon
    # TX, with base_pay_from/frequency (mapped to posted pay below).
    "Wilbarger General Hospital": ("secure7.saashr.com", "6215251", "TX"),
    # 2026-09-24 Texas configs: Coryell Health (Gatesville), same UKG Ready API.
    "Coryell Health": ("secure6.saashr.com", "6034202", "TX"),   # 20 (Gatesville 16, Waco 4)
}


def _kronos_wage(j: dict):
    """base_pay_from / base_pay_to + base_pay_frequency -> (min, max, unit)
    or None. Frequency HOUR / YEAR must agree with the magnitude check in
    _wage_pair (2026-09-10)."""
    lo, hi = j.get("base_pay_from"), j.get("base_pay_to")
    if not isinstance(lo, (int, float)):
        return None
    if not isinstance(hi, (int, float)):
        hi = lo
    pair = _wage_pair(float(lo), float(hi))
    freq = str(j.get("base_pay_frequency") or "").upper()
    if not pair:
        return None
    if freq.startswith("HOUR") and pair[2] != "hour":
        return None
    if freq.startswith("YEAR") and pair[2] != "year":
        return None
    return pair

def _kronos_base(subdomain: str) -> str:
    # 2026-09-10: a value carrying a full host ("secure7.saashr.com") is used
    # as-is; the short form still maps to <subdomain>.mykronos.com.
    return f"https://{subdomain}" if subdomain.endswith((".com", ".net")) else f"https://{subdomain}.mykronos.com"


# 2026-09-24: UKG Ready's `offset` parameter is a 1-based PAGE NUMBER, not a
# row offset. The loop used to add `size` to it, so the second request asked
# for page 21, got an empty page, and every tenant stopped at its first 20
# rows (09-24 run: Ridgeview 20 of 119, Kronos Hospital 3 20 of 97, Magruder
# 20 of 40). offset=1&size=100 answers 100 rows, so pages are 100 wide and the
# page number goes up by one; _paging.total ends the loop.
_KRONOS_PAGE = 100
_KRONOS_MAX_PAGES = 50


async def scrape_kronos(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    subdomain, company_id = org_data[0], org_data[1]
    default_state = org_data[2] if len(org_data) > 2 else ""
    jobs  = []
    base  = _kronos_base(subdomain)
    api   = f"{base}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions"
    page   = 1                 # sent as "offset": a page number (see above)
    size   = _KRONOS_PAGE
    seen_ids: set[str] = set()
    while True:
        try:
            params = {"offset": page, "size": size, "sort": "desc",
                      "ein_id": "", "lang": "en-US", "_": int(time.time()*1000)}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Kronos {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            # 2026-09-10 (S-scraper-2): UKG Ready answers {"job_requisitions":
            # [...]} (Magruder dry run); the keys guessed before never matched,
            # so this adapter had never returned a row for any org.
            items = data if isinstance(data, list) else (
                data.get("job_requisitions") or data.get("requisitions") or data.get("jobs") or [])
            new = [j for j in items if str(j.get("id", "")) not in seen_ids]
            if not items or not new:
                break      # the endpoint ignores offset/size and repeats: stop on no new ids
            for j in new:
                seen_ids.add(str(j.get("id", "")))
                loc  = j.get("location", {}) or {}
                city  = loc.get("city", "") or ""
                state = (loc.get("state", "") or default_state)
                cats  = j.get("job_categories", []) or []
                wage  = _kronos_wage(j)
                jobs.append(Job(
                    title=j.get("job_title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=city, state=state,
                    location=f"{city}, {state}".strip(", "),
                    specialty=cats[0] if cats else "",
                    # 2026-09-10: employee_type is the schedule; base_pay_frequency
                    # ("YEAR") was being stored as the job type.
                    job_type=str(((j.get("employee_type") or {}).get("name") if isinstance(j.get("employee_type"), dict)
                                  else j.get("employee_type")) or j.get("employment_type") or ""),
                    url=f"{base}/ta/{company_id}.careers?CareersSearch=&ShowJob={j.get('id', '')}&lang=en-US",
                    job_id=str(j.get("id", "")),
                    posted_date="",
                    description=strip_html(j.get("job_description") or ""),
                    ats_platform="Kronos",
                    wage_min=wage[0] if wage else None,
                    wage_max=wage[1] if wage else None,
                    wage_unit=wage[2] if wage else None,
                ))
            total = 0
            if isinstance(data, dict):
                try:
                    total = int((data.get("_paging") or {}).get("total") or 0)
                except (TypeError, ValueError, AttributeError):
                    total = 0
            if total:
                if page * size >= total or len(seen_ids) >= total:
                    break
            elif len(items) < size:
                break
            if page >= _KRONOS_MAX_PAGES:
                logger.info(f"Kronos {system}: stopped at page {page} ({len(seen_ids)} of {total or '?'} rows)")
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Kronos {system}: {e}")
            break
    logger.info(f"  Kronos {system}: {len(jobs)} jobs")
    return jobs

async def run_kronos(session) -> list[Job]:
    logger.info(f"Kronos: scraping {len(KRONOS_ORGS)} systems...")
    # 2026-09-24: HOST_CONCURRENCY tenants at a time per UKG Ready host.
    results = await _gather_by_host(
        session,
        priority_states_first(list(KRONOS_ORGS.items()), lambda kv: kv[1][2] if len(kv[1]) > 2 else ""),
        scrape_kronos, lambda o: _url_host(_kronos_base(o[0])))
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Kronos total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  THE APPLICANT MANAGER (theapplicantmanager.com) — small independent
#  hospitals. Added 2026-09-15 for Bayou Bend Health System (Franklin, LA),
#  a showcase client. Server-rendered HTML: the careers page lists one
#  <a class="pos_title_list" href="jobs?pos=<id>"> per opening, title with
#  "(City, ST)" appended, an optional one-line blurb in the next <p>; the
#  detail page carries the text in <div class="job_listing"> and the schedule
#  in <p class="small_font">. No JSON, no pagination, no auth.
##############################################################################
TAM_ORGS = {
    # system: (company code, default city, default state)
    "Bayou Bend Health System": ("fr", "Franklin", "LA"),
}
_TAM_BASE = "https://theapplicantmanager.com"
_TAM_LINK_RE = re.compile(
    r'<a[^>]+class="pos_title_list"[^>]+href="jobs\?pos=([A-Za-z0-9]+)[^"]*"[^>]*>(.*?)</a>\s*</p>\s*(?:<p>(.*?)</p>)?',
    re.S)
_TAM_LOC_RE = re.compile(r'\s*\(([^()]+?),\s*([A-Za-z]{2})\)\s*$')
_TAM_EEO_RE = re.compile(r'Equal Employment Opportunity Statement.*', re.S | re.I)


def _tam_split_title(raw: str, default_city: str, default_state: str) -> tuple[str, str, str]:
    """'Medical Assistant (MA) (Franklin, LA)' -> title, city, state."""
    t = strip_html(raw).strip()
    m = _TAM_LOC_RE.search(t)
    if m:
        return t[:m.start()].strip(), m.group(1).strip(), m.group(2).upper()
    return t, default_city, default_state


async def scrape_tam(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    code, default_city, default_state = org_data
    jobs: list[Job] = []
    list_url = f"{_TAM_BASE}/careers?co={code}"
    try:
        async with req(session, "get", list_url, headers=HEADERS, ssl=False,
                       proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"TAM {system}: HTTP {r.status}")
                return jobs
            text = await r.text()
    except Exception as e:
        logger.info(f"TAM {system}: {e}")
        return jobs
    seen: set[str] = set()
    for pos, raw_title, blurb in _TAM_LINK_RE.findall(text):
        if pos in seen:
            continue
        seen.add(pos)
        title, city, state = _tam_split_title(raw_title, default_city, default_state)
        if not title or title.lower().startswith("general application"):
            continue      # the evergreen "General Application" is not an opening
        url = f"{_TAM_BASE}/jobs?pos={pos}"
        description = strip_html(blurb or "").strip()
        job_type = ""
        try:
            await jitter()
            async with req(session, "get", url, headers=HEADERS, ssl=False,
                           proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r2:
                if r2.status == 200:
                    d = await r2.text()
                    m = re.search(r'<div class="job_listing">(.*?)<div class="line_div', d, re.S)
                    if m:
                        body = m.group(1)
                        jt = re.search(r'<p class="small_font">(.*?)</p>', body, re.S)
                        if jt:
                            # "Department: 006 - ICU<br/>non-management position<br/>full time position"
                            sched = re.search(r'(full[ -]?time|part[ -]?time|per diem|prn|temporary|seasonal)', strip_html(jt.group(1)), re.I)
                            job_type = sched.group(1).title().replace("Prn", "PRN") if sched else ""
                        full = _TAM_EEO_RE.sub("", strip_html(body)).strip()
                        if len(full) > len(description):
                            description = full
        except Exception as e:
            logger.info(f"TAM {system} {pos}: {e}")
        jobs.append(Job(
            title=title,
            hospital_system=system,
            hospital_name=system,
            city=city, state=state,
            location=f"{city}, {state}".strip(", "),
            specialty="",
            job_type=job_type,
            url=url,
            job_id=pos,
            posted_date="",
            description=description,
            ats_platform="ApplicantManager",
        ))
    logger.info(f"  TAM {system}: {len(jobs)} jobs")
    return jobs


async def run_tam(session) -> list[Job]:
    logger.info(f"TAM: scraping {len(TAM_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_tam(session, s, o) for s, o in
          priority_states_first(list(TAM_ORGS.items()), lambda kv: kv[1][2])],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  TAM total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  NEOGOV / GOVERNMENTJOBS.COM — county and public-district hospitals
#  (added 2026-09-10, S-scraper-2). The careers page is an MVC app whose
#  list is a plain GET partial (captured from the page's own XHR):
#    GET https://www.governmentjobs.com/careers/home/index
#        ?agency=<agency>&sort=PostingDate&isDescendingSort=true&page=N
#  -> HTML with <li class="list-item" data-job-id="..."> cards, 10 a page:
#  <a class="item-details-link" data-department-name="..." href="/careers/
#  <agency>/jobs/<id>/<slug>">, a "Full time - $x - $y Annually" line,
#  "Category:", "Department:", sometimes "Location:", a snippet, and
#  <span id="job-postings-number">N</span> for the total. County boards mix
#  every department, so each agency carries a department regex and rows from
#  other departments are dropped. City / state default to the hospital's CMS
#  city / state; a card "Location:" wins when present. List-only (no
#  description body, relative posted date), like the UHG / Kaiser adapters.
#  Format: "System": (agency, state, CMS city, hospital_name, department regex | None)
##############################################################################
NEOGOV_AGENCIES = {
    # Seeded from CMS ownership "Government - Local / Hospital District" in the
    # top coverage states; California first (transparency state, 64 government
    # acute hospitals in CMS). Department strings are what each board used on
    # 2026-09-10 (per-agency dry run in reports/S-scraper-2.md).
    "Los Angeles County Department of Health Services":
        ("lacounty", "CA", "", "Los Angeles County Department of Health Services", r"^HEALTH SERVICES$"),
    "Riverside University Health System":
        ("riverside", "CA", "Moreno Valley", "Riverside University Health System - Medical Center", r"^RUHS"),
    "Arrowhead Regional Medical Center":
        ("sanbernardino", "CA", "Colton", "Arrowhead Regional Medical Center", r"Arrowhead"),
    "Ventura County Medical Center":
        ("ventura", "CA", "Ventura", "Ventura County Medical Center", r"^Health Care Agency"),
    "Contra Costa Regional Medical Center":
        ("contracosta", "CA", "Martinez", "Contra Costa Regional Medical Center", r"^Health Services"),
    "Natividad Medical Center":
        ("montereycounty", "CA", "Salinas", "Natividad Medical Center", r"Natividad"),
    # Santa Clara Valley Healthcare: Valley Medical Center plus the Regional
    # Medical Center of San Jose the county took over in 2025; both San Jose.
    "Santa Clara Valley Healthcare":
        ("santaclara", "CA", "San Jose", "Santa Clara Valley Healthcare", r"^Santa Clara Valley Health"),
    "San Mateo Medical Center":
        ("sanmateo", "CA", "San Mateo", "San Mateo Medical Center", r"^San Mateo Medical Center"),
    "University Medical Center of Southern Nevada":
        ("umcsn", "NV", "Las Vegas", "University Medical Center of Southern Nevada", None),
}
NEOGOV_MAX_PAGES = int(os.getenv("NEOGOV_MAX_PAGES", "80"))   # 10 cards a page
_NG_INDEX = "https://www.governmentjobs.com/careers/home/index"
_NG_LINK_RX = re.compile(
    r'class="item-details-link"[^>]*href="(/careers/[^"]+/jobs/(\d+)/[^"]*)"[^>]*>(.*?)</a>', re.S)
_NG_DEPT_RX = re.compile(r'data-department-name="([^"]*)"')
_NG_META_RX = re.compile(r'<ul class="list-meta">(.*?)</ul>', re.S)
_NG_LI_RX = re.compile(r'<li[^>]*>(.*?)</li>', re.S)
_NG_ENTRY_RX = re.compile(r'<div class="list-entry">(.*?)</div>', re.S)
_NG_TOTAL_RX = re.compile(r'id="job-postings-number">\s*([\d,]+)')
_NG_SALARY_RX = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*-\s*\$\s*([\d,]+(?:\.\d+)?)\s*(Annually|Hourly|Monthly|Biweekly|Weekly|Daily)?', re.I)


def _ng_text(s):
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def _parse_neogov_page(page_html, system, agency, state, city, hospital_name, dept_rx=None):
    """(jobs, total, cards_on_page) from one list partial. dept_rx (compiled)
    keeps only the hospital departments of a county-wide board."""
    total_m = _NG_TOTAL_RX.search(page_html or "")
    total = int(total_m.group(1).replace(",", "")) if total_m else 0
    jobs, cards = [], 0
    for chunk in (page_html or "").split('<li class="list-item" data-job-id="')[1:]:
        cards += 1
        lm = _NG_LINK_RX.search(chunk)
        if not lm:
            continue
        path, job_id, title = lm.group(1), lm.group(2), _ng_text(lm.group(3))
        dm = _NG_DEPT_RX.search(chunk)
        dept = _ng_text(dm.group(1)) if dm else ""
        if dept_rx and not dept_rx.search(dept):
            continue
        mm = _NG_META_RX.search(chunk)
        meta = [_ng_text(x) for x in _NG_LI_RX.findall(mm.group(1))] if mm else []
        job_type, salary, category, loc_text = "", "", "", ""
        for m in meta:
            low = m.lower()
            if not m:
                continue
            if low.startswith("category:"):
                category = m.split(":", 1)[1].strip()
            elif low.startswith("location:"):
                loc_text = m.split(":", 1)[1].strip()
            elif low.startswith(("department:", "exam type:", "closing:", "job number:")):
                continue
            elif "$" in m:
                head, _, tail = m.partition("$")
                job_type = head.strip(" -").strip() or job_type
                salary = "$" + tail.strip()
            elif not job_type:
                job_type = m
        wage = None
        sm = _NG_SALARY_RX.search(salary)
        if sm:
            unit = (sm.group(3) or "").lower()
            pair = _wage_pair(_wage_num(sm.group(1)), _wage_num(sm.group(2)))
            if pair and ((unit == "annually" and pair[2] == "year") or (unit == "hourly" and pair[2] == "hour")):
                wage = pair
        c, s = parse_city_state(loc_text) if loc_text else ("", "")
        c = c or city
        s = (s or state).upper()
        em = _NG_ENTRY_RX.search(chunk)
        snippet = _ng_text(em.group(1)) if em else ""
        desc = (f"Salary: {salary}. " if salary else "") + snippet
        jobs.append(Job(
            title=title,
            hospital_system=system,
            hospital_name=hospital_name or system,
            city=c, state=s,
            location=f"{c}, {s}".strip(", "),
            specialty=category,
            job_type=job_type,
            url=f"https://www.governmentjobs.com{path}",
            job_id=job_id,
            posted_date="",
            description=desc[:8000],
            ats_platform="NeoGov",
            wage_min=wage[0] if wage else None,
            wage_max=wage[1] if wage else None,
            wage_unit=wage[2] if wage else None,
        ))
    return jobs, total, cards


async def _neogov_fetch_page(session, agency: str, page: int) -> tuple[int, str]:
    async with req(session, "get", _NG_INDEX,
        params={"agency": agency, "sort": "PostingDate", "isDescendingSort": "true", "page": str(page)},
        headers={**HEADERS, "Accept": "text/html, */*; q=0.01", "X-Requested-With": "XMLHttpRequest",
                 "Referer": f"https://www.governmentjobs.com/careers/{agency}"},
        ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
        return r.status, (await r.text() if r.status == 200 else "")


async def scrape_neogov(session: aiohttp.ClientSession, system: str, cfg: tuple) -> list[Job]:
    agency, state, city, hospital_name, dept_pat = cfg
    dept_rx = re.compile(dept_pat, re.I) if dept_pat else None
    jobs: list[Job] = []
    page, total = 1, 0
    while True:
        try:
            status, html_ = await _neogov_fetch_page(session, agency, page)
            if status != 200:
                logger.info(f"NeoGov {system}: HTTP {status} (agency {agency}, page {page})")
                break
            got, total, cards = _parse_neogov_page(html_, system, agency, state, city, hospital_name, dept_rx)
            jobs.extend(got)
            if not cards or page * 10 >= total or page >= NEOGOV_MAX_PAGES:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"NeoGov {system}: {e}")
            break
    logger.info(f"  NeoGov {system}: {len(jobs)} jobs kept of {total} on the {agency} board")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PRELOAD-STATE career sites (2026-09-22, Texas resume). jobs.harrishealth.org
#  renders each listing page with
#    window.__PRELOAD_STATE__ = {"jobSearch": {"totalJob": 475, "jobs": [...]}}
#  ten jobs a page at /jobs/page/N, each with the description, the facility
#  (locations[0].streetAddress: "Ben Taub Hospital"), city/state, the
#  employment type and the PeopleSoft apply link (the PeopleSoft portal
#  itself needs a session; this front does not).
# ══════════════════════════════════════════════════════════════════════════
PRELOAD_SITES = {
    # "System": (base url, default state)
    "Harris Health System": ("https://jobs.harrishealth.org", "TX"),   # 475 jobs on 2026-09-22; Houston, 595 beds (CMS 450289)
}
_PRELOAD_RX = re.compile(r"window\.__PRELOAD_STATE__\s*=\s*")
_PRELOAD_FACILITY_RX = re.compile(r"hospital|medical center|health center|clinic|campus|institute|pavilion|center\b", re.I)


def _preload_state(html: str):
    m = _PRELOAD_RX.search(html)
    if not m:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, m.end())
        return obj
    except Exception:
        return None


async def scrape_preload(session: aiohttp.ClientSession, system: str, cfg: tuple) -> list[Job]:
    base, default_state = cfg
    jobs: list[Job] = []
    page, total, per = 1, None, None
    while page <= 200:
        try:
            async with req(session, "get", f"{base}/jobs/page/{page}", headers=HEADERS, ssl=False,
                           proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Preload {system}: HTTP {r.status} at page {page}")
                    break
                html = await r.text()
        except Exception as e:
            logger.info(f"Preload {system}: page {page} error: {e}")
            break
        search = ((_preload_state(html) or {}).get("jobSearch")) or {}
        items = search.get("jobs") or []
        if total is None:
            total = int(search.get("totalJob") or 0)
        if not items:
            break
        per = per or len(items)
        for j in items:
            try:
                title = (j.get("title") or "").strip()
                rid = str(j.get("requisitionID") or j.get("uniqueID") or j.get("sourceID") or "").strip()
                if not title or not rid:
                    continue
                loc = ((j.get("locations") or [None])[0]) or {}
                facility = (loc.get("streetAddress") or loc.get("locationName") or "").strip()
                city = (loc.get("city") or "").strip()
                st = (loc.get("stateAbbr") or "").strip().upper() or default_state
                et = j.get("employmentType")
                et = et[0] if isinstance(et, list) and et else (et or "")
                orig = (j.get("originalURL") or "").strip()
                url = f"{base}/{orig.lstrip('/')}" if orig else str(j.get("applyURL") or "")
                jobs.append(Job(
                    title=title,
                    hospital_system=system,
                    hospital_name=facility if (facility and _PRELOAD_FACILITY_RX.search(facility)) else system,
                    city=city,
                    state=st,
                    location=", ".join(p for p in (city, st) if p),
                    specialty="",
                    job_type=str(et),
                    url=url,
                    job_id=rid,
                    posted_date=str(j.get("postedDate") or j.get("datePosted") or j.get("postingDate") or "")[:10],
                    description=strip_html(str(j.get("description") or "")),
                    ats_platform="PreloadState",
                ))
            except Exception as e:
                logger.info(f"Preload {system}: row error {e!r}")
        if total and per and page * per >= total:
            break
        page += 1
        await jitter()
    seen, uniq = set(), []
    for jb in jobs:
        if jb.job_id in seen:
            continue
        seen.add(jb.job_id)
        uniq.append(jb)
    logger.info(f"  Preload {system}: {len(uniq):,} jobs (site reports {total})")
    return uniq


async def run_preload(session) -> list[Job]:
    async def _one(sys, cfg):
        jobs = await scrape_preload(session, sys, cfg)
        # The listing's description field is empty; the job page carries a
        # JSON-LD JobPosting (4k-char body, datePosted, employmentType).
        # Same detail pass and budget as TalentBrew.
        if DETAIL_FETCH and jobs:
            try:
                await _detail_pass(session, sys, jobs, TB_DESC_BUDGET,
                                   lambda j: _jsonld_detail(session, j), "Preload")
            except Exception as e:
                logger.info(f"Preload {sys}: detail pass failed ({e})")
        return jobs
    results = await asyncio.gather(*[_one(s, c) for s, c in PRELOAD_SITES.items()], return_exceptions=True)
    out = [j for r in results if isinstance(r, list) for j in r]
    for (s, _), r in zip(PRELOAD_SITES.items(), results):
        if isinstance(r, Exception):
            logger.info(f"  Preload {s}: ERROR {r}")
    logger.info(f"  Preload total: {len(out):,} jobs")
    return out


async def run_neogov(session) -> list[Job]:
    logger.info(f"NeoGov: scraping {len(NEOGOV_AGENCIES)} agencies (transparency states first)...")
    jobs: list[Job] = []
    # Sequential on purpose: one agency at a time, jitter between pages, so a
    # county board never sees more than one request in flight from us.
    for system, cfg in priority_states_first(list(NEOGOV_AGENCIES.items()), lambda kv: kv[1][1]):
        try:
            jobs.extend(await scrape_neogov(session, system, cfg))
        except Exception as e:
            logger.info(f"NeoGov {system}: {e}")
        await jitter()
    logger.info(f"  NeoGov total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  APPLICANTPRO — applicantpro.com career portal
#  API: GET https://{subdomain}.applicantpro.com/core/jobs/{site_id}
#  Returns JSON array of job objects
##############################################################################
APPLICANTPRO_ORGS = {
    "Cayuga Health":        ("cayugahealthsystem", "17888"),
    "Cascade Medical":      ("cascademedicalcenter", ""),
    "Jefferson Healthcare": ("jeffersonhealthcare", ""),
    # 2026-09-10 (Z-texas-acute-D): the board is a Vue app now; its JobListings
    # component reads /core/jobs/<domainId>?getParams=<json> (domainId from the
    # page's bootstrapVue config; a bare /core/jobs/<id> answers a PHP error).
    "Elite Hospital Kingwood": ("elitekingwood", "9514"),
}
_APPLICANTPRO_PARAMS = {"cityUrl": "", "countryAbbreviation": "", "stateAbbreviation": "", "isInternal": 0}

async def scrape_applicantpro(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    subdomain, site_id = org_data
    jobs = []
    # If site_id known, use direct endpoint; otherwise fetch from /jobs to get site_id
    if not site_id:
        # Try to find site_id from the jobs listing page
        try:
            async with req(session, "get",
                f"https://{subdomain}.applicantpro.com/jobs/",
                headers=HEADERS, ssl=False, proxy=proxies.get(),
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    text = await r.text()
                    m = re.search(r'/core/jobs/(\d+)', text) or re.search(r'domainId\s*:\s*(\d+)', text)
                    if m:
                        site_id = m.group(1)
        except Exception as e:
            logger.info(f"ApplicantPro {system}: site_id discovery failed: {e}")
            return []

    if not site_id:
        logger.info(f"ApplicantPro {system}: could not determine site_id")
        return []

    try:
        api = f"https://{subdomain}.applicantpro.com/core/jobs/{site_id}"
        async with req(session, "get", api, params={"getParams": json.dumps(_APPLICANTPRO_PARAMS)},
            headers={**HEADERS, "Accept": "application/json"},
            ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"ApplicantPro {system}: HTTP {r.status}")
                return []
            data = await r.json(content_type=None)
        # 2026-09-10: {"success": true, "data": {"jobs": [...]}} on the Vue boards
        items = data if isinstance(data, list) else (
            (data.get("data") or {}).get("jobs") if isinstance(data.get("data"), dict) else data.get("jobs", [])) or []
        for j in items:
            city  = j.get("city", "")
            state = j.get("abbreviation") or j.get("state") or j.get("stateAbbreviation") or ""
            if not state:
                state = parse_city_state(str(j.get("title", "")))[1]      # "... - Kingwood, Texas"
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=system,      # 2026-09-10: "subdomain" is a slug, not a facility
                city=city, state=state,
                location=f"{city}, {state}".strip(", "),
                specialty=j.get("classification", j.get("jobCategory", "")),
                job_type=j.get("employmentType", ""),
                url=f"https://{subdomain}.applicantpro.com/jobs/{j.get('id', '')}.html",
                job_id=str(j.get("id", "")),
                posted_date=str(j.get("startDateRef", ""))[:10],
                description="",
                ats_platform="ApplicantPro",
            ))
    except Exception as e:
        logger.info(f"ApplicantPro {system}: {e}")

    logger.info(f"  ApplicantPro {system}: {len(jobs)} jobs")
    return jobs

async def run_applicantpro(session) -> list[Job]:
    logger.info(f"ApplicantPro: scraping {len(APPLICANTPRO_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_applicantpro(session, s, o) for s, o in APPLICANTPRO_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  ApplicantPro total: {len(jobs):,} jobs")
    return jobs


async def run_playwright_scrapers() -> list[Job]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping custom sites")
        return []

    logger.info("Playwright: scraping JS-heavy custom sites...")
    jobs = []

    CUSTOM_SITES = [
        # PRODUCING JOBS — keep these
        # Mayo Clinic removed 2026-06-18 — migrated to Oracle HCM (see ORACLE_ORGS).
        ("CHRISTUS Health",               "https://careers.christushealth.org/job-search"),
        # Baylor Scott & White — moved to dedicated run_bsw() handler (Apr 29 2026)
        # using Phenom refineSearch endpoint. The Playwright route only captured
        # ~58 jobs from the first-page render; the new handler returns the full
        # 1,935-job inventory via paginated /widgets calls.
        # ("Baylor Scott & White",          "https://jobs.bswhealth.com/us/en/search-results"),
        ("MyMichigan Health",             "https://careers.mymichigan.org/jobs"),
        # LARGE SYSTEMS — Phenom via Playwright (proxy-free)
        # NOTE: HCA Healthcare is handled by dedicated run_hca() — do NOT add here
        # Ascension Health moved to PHENOM_ORGS (2026-05-08) — was returning 0
        # jobs here; the Phenom widgets path produces full inventory.
        # Cleveland Clinic is in WORKDAY_TENANTS (ccf.wd1) — Playwright is a
        # fallback; left here in case Workday route flakes out.
        ("Cleveland Clinic",              "https://jobs.clevelandclinic.org/search/"),
        # Methodist Healthcare (joinmethodist.com) — REMOVED 2026-07-28. HCA
        # affiliate; its jobs come through the rebuilt run_hca() master crawl.
        # LIFEPOINT — rebuilt on WordPress 2025
        ("LifePoint Health",              "https://jobs.lifepointhealth.net/jobs/"),
        # CUSTOM ATS
        ("MUSC Health",                   "https://musc.career-pages.com/jobs/search"),
        ("University of Vermont Health",  "https://www.uvmhealthnetworkcareers.org/jobs/"),
    ]

    # Deduplicate by system name (Cleveland Clinic listed twice above)
    seen_systems = set()
    CUSTOM_SITES = [(name, url) for name, url in CUSTOM_SITES
                    if name not in seen_systems and not seen_systems.add(name)]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=[
            "--no-sandbox", "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ])

        for system_name, url in CUSTOM_SITES:
            try:
                ctx = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                    locale="en-US",
                )
                await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>false})")
                page = await ctx.new_page()

                captured = []
                async def capture(response, _sn=system_name):
                    url_l = response.url.lower()
                    if any(x in url_l for x in [
                        "api/jobs", "search-jobs", "careers/search", "job-search",
                        "jobpostings", "/jobs?", "requisitions", "positions",
                        "api/search", "job_search", "jobsearch", "joblist",
                        "/search/", "apply/v2", "talentbrew", "tb_ajax",
                        "findly", "job-search-results/results",
                        "career-pages.com", "uvmhealthnetwork", "widgets",
                        "wp-json", "lifepointhealth.net/jobs",
                        "phenompeople", "bswhealth",
                    ]):
                        try:
                            ct = response.headers.get("content-type", "")
                            if "json" in ct:
                                d = await response.json()
                                if isinstance(d, dict):
                                    # Handle Elasticsearch nested hits: {"hits": {"hits": [...], "total": N}}
                                    if isinstance(d.get("hits"), dict) and isinstance(d["hits"].get("hits"), list):
                                        raw_hits = d["hits"]["hits"]
                                        unwrapped = [h.get("_source", h) for h in raw_hits if isinstance(h, dict)]
                                        captured.extend(unwrapped)
                                    else:
                                        # Handle Phenom widgets nested response:
                                        # {"data": {"jobs": [...], "count": N}, "reqData": {...}}
                                        # or {"data": {"requisitions": [...]}}
                                        data_val = d.get("data")
                                        if isinstance(data_val, dict):
                                            for inner_key in ("jobs", "requisitions", "results", "postings", "items"):
                                                inner = data_val.get(inner_key)
                                                if isinstance(inner, list) and inner:
                                                    captured.extend(inner)
                                                    break
                                        for key in ("jobs", "jobPostings", "results", "requisitions",
                                                    "postings", "items", "hits"):
                                            val = d.get(key)
                                            if isinstance(val, list) and val:
                                                unwrapped = [j.get("_source", j) if isinstance(j, dict) and "_source" in j else j for j in val]
                                                captured.extend(unwrapped)
                                                break
                                        # data is a flat list
                                        if isinstance(data_val, list) and data_val:
                                            captured.extend(data_val)
                                elif isinstance(d, list) and d:
                                    # Unwrap _source if ES-style hits
                                    unwrapped = [j.get("_source", j) if isinstance(j, dict) and "_source" in j else j for j in d]
                                    captured.extend(unwrapped)
                        except: pass
                page.on("response", capture)

                # Wait strategy: networkidle is the gold-standard but heavy sites
                # (BSW Phenom, Mayo Clinic) hang waiting for tracking pixels and
                # never go idle within 30s. domcontentloaded lets us proceed once
                # the HTML is parsed, and our scroll + sleep loop handles JS hydration.
                slow_sites = ("bswhealth.com", "jobs.mayoclinic.org")
                _wait = "domcontentloaded" if any(s in url for s in slow_sites) else "networkidle"
                await page.goto(url, wait_until=_wait, timeout=60000)
                await asyncio.sleep(random.uniform(2, 4))

                bsw_site = "bswhealth.com" in url

                # BSW Health (Phenom) — click the search submit button to trigger job API call
                if bsw_site:
                    try:
                        await page.wait_for_selector("[data-ph-at-id='globalsearch-button']", timeout=10000)
                        btn = await page.query_selector("[data-ph-at-id='globalsearch-button']")
                        if btn:
                            await btn.click()
                            await asyncio.sleep(8)  # Wait for API response
                            logger.info("BSW: clicked search button")
                        else:
                            logger.info("BSW: search button not found")
                    except Exception as e:
                        logger.info(f"BSW search trigger: {e}")

                for _ in range(4):
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)

                for j in captured:
                    if not isinstance(j, dict):
                        continue
                    title = j.get("title", j.get("jobTitle", j.get("name", j.get("positionTitle", ""))))
                    # Try many possible location field names; CHRISTUS uses various structures
                    loc = (j.get("location") or j.get("city") or j.get("locationsText") or
                           j.get("primaryLocation") or j.get("address") or
                           j.get("locationName") or j.get("jobLocation") or "")
                    if isinstance(loc, dict):
                        # Handle many possible dict shapes from different ATS platforms
                        loc_city  = (loc.get("city") or loc.get("cityName") or loc.get("municipality") or
                                     loc.get("addressLocality") or loc.get("name") or "")
                        loc_state = (loc.get("stateCode") or loc.get("state") or loc.get("region") or
                                     loc.get("countrySubdivisionCode") or loc.get("addressRegion") or "")
                        loc = f"{loc_city}, {loc_state}" if loc_city or loc_state else ""
                    elif isinstance(loc, list):
                        loc = ", ".join(str(x) for x in loc[:2])
                    _city, _state = parse_city_state(str(loc))
                    job_id = str(j.get("id", j.get("jobId", j.get("requisitionId", j.get("externalId", "")))))
                    if not job_id:
                        job_id = f"{system_name}_{title}_{loc}"[:80]
                    # hospital_name: prefer bu (CHRISTUS business unit) > facility > department > system
                    _hosp_name = (j.get("bu") or j.get("facility") or j.get("department") or system_name)
                    if title:
                        jobs.append(Job(
                            title=str(title), hospital_system=system_name,
                            hospital_name=_hosp_name,
                            city=_city,
                            state=_state,
                            location=str(loc), specialty=j.get("category", j.get("jobCategory", "")),
                            job_type=j.get("employmentType", j.get("jobType", "")),
                            url=str(j.get("url", j.get("applyUrl", j.get("canonicalPositionUrl", url)))),
                            job_id=job_id,
                            posted_date=str(j.get("datePosted", j.get("postedOn", j.get("postingDate", ""))))[:10],
                            description=strip_html(str(j.get("description", j.get("shortDescription", "")))),
                            ats_platform="Custom",
                        ))

                # DOM fallback if no API responses captured
                if not [j for j in jobs if j.hospital_system == system_name]:
                    cards = await page.query_selector_all(
                        "[data-job-id],[data-testid='job-card'],.job-card,.job-listing,"
                        ".search-result-item,li.job,.job-result,article.job,[class*='JobCard'],"
                        "[class*='job-item'],[class*='career-item']"
                    )
                    for card in cards[:200]:
                        try:
                            t = await card.query_selector(
                                "h2,h3,h4,.job-title,[data-testid='job-title'],"
                                "[class*='title'],[class*='Title']"
                            )
                            a = await card.query_selector("a[href]")
                            l = await card.query_selector(
                                ".location,.job-location,[data-testid='location'],"
                                "[class*='location'],[class*='Location']"
                            )
                            title_txt = (await t.inner_text()).strip() if t else ""
                            href      = await a.get_attribute("href") if a else ""
                            loc_txt   = (await l.inner_text()).strip() if l else ""
                            if title_txt:
                                p = [x.strip() for x in loc_txt.split(",")]
                                jobs.append(Job(
                                    title=title_txt, hospital_system=system_name,
                                    hospital_name=system_name,
                                    city=p[0] if p else "", state=p[-1] if len(p)>1 else "",
                                    location=loc_txt, specialty="", job_type="",
                                    url=f"{url.rstrip('/')}{href}" if href and href.startswith("/") else href or url,
                                    job_id=href.split("/")[-1] if href else title_txt[:60],
                                    posted_date="", description="", ats_platform="Custom",
                                ))
                        except: continue

                await ctx.close()
                count = len([j for j in jobs if j.hospital_system == system_name])
                logger.info(f"  {system_name}: {count} jobs")
                await asyncio.sleep(random.uniform(3, 5))

            except Exception as e:
                logger.error(f"Playwright {system_name}: {e}")

        await browser.close()

    logger.info(f"  Playwright total: {len(jobs):,} jobs")
    return jobs



# ══════════════════════════════════════════════════════════════════════════
#  CORNERSTONE ON DEMAND (CSOD)
#  JPS Health Network (Fort Worth, TX)
# ══════════════════════════════════════════════════════════════════════════
CSOD_ORGS = {
    "JPS Health Network": ("https://jpshealthnet.csod.com", "4"),
    # ── Added from scraper1.xlsx expansion ──
    "Singing River Health System": ("https://singingriverhealthsystem.csod.com", "1"),
    # 2026-09-24 configs: Billings Clinic (MT/WY), career site 1.
    "Billings Clinic": ("https://billingsclinic.csod.com", "1"),
}

def _csod_job(rq: dict, system: str, base: str, site_id: str, corp: str) -> Job | None:
    """One career-site search requisition -> Job (2026-09-10). Shape (JPS):
    requisitionId, displayJobTitle, postingEffectiveDate "9/10/2026",
    locations[{city, state, country}]. List-only rows (no description)."""
    rid = str(rq.get("requisitionId") or rq.get("id") or "").strip()
    title = str(rq.get("displayJobTitle") or rq.get("title") or "").strip()
    if not rid or not title:
        return None
    loc0 = (rq.get("locations") or [{}])[0] or {}
    city = str(loc0.get("city") or "").strip()
    if city.isupper():
        city = city.title()              # "FORT WORTH" alongside "Fort Worth" (JPS)
    st = str(loc0.get("state") or "").strip().upper()
    posted = ""
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", str(rq.get("postingEffectiveDate") or ""))
    if m:
        posted = f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return Job(
        title=title, hospital_system=system, hospital_name=system,
        city=city, state=st, location=f"{city}, {st}".strip(", "),
        specialty="", job_type="",
        url=f"{base}/ux/ats/careersite/{site_id}/requisition/{rid}?c={corp}",
        job_id=rid, posted_date=posted, description="", ats_platform="CSOD",
    )


_CSOD_PAGE = 25


async def scrape_csod(session: aiohttp.ClientSession, system: str, base: str, site_id: str) -> list[Job]:
    # 2026-09-10 (Z-texas-acute-D): /ux/ats/careersite/<id>/jobs was never an
    # endpoint (it 302s to a SAML login without the c=<corp> parameter), so
    # this adapter had returned 0 rows since it was added. The career-site
    # home page embeds csod.context.token (a JWT); the search API is
    # POST /services/x/career-site/v1/search with that token as Bearer and
    # cultureName in the body (400 "CultureName field is required" without
    # it). JPS: 209 requisitions on 2026-09-10.
    jobs: list[Job] = []
    corp = re.sub(r"^https?://", "", base).split("/")[0].split(".")[0]
    try:
        async with req(session, "get", f"{base}/ux/ats/careersite/{site_id}/home", params={"c": corp},
                       headers=HEADERS, ssl=False, proxy=proxies.get(),
                       timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"CSOD {system}: home HTTP {r.status}")
                return []
            home = await r.text()
        m = re.search(r'"token"\s*:\s*"([^"]+)"', home)
        if not m:
            logger.info(f"CSOD {system}: no csod.context.token on the home page")
            return []
        token = m.group(1)
        page = 1
        while True:
            body = {"careerSiteId": int(site_id), "careerSitePageId": int(site_id), "pageNumber": page,
                    "pageSize": _CSOD_PAGE, "cultureId": 1, "cultureName": "en-US", "searchText": "",
                    "cities": [], "countryCodes": [], "cultures": [], "customFieldCheckboxKeys": [],
                    "customFieldDropdowns": [], "customFieldRadios": [], "placeID": "",
                    "postingsWithinDays": None, "radius": None, "searchResultsSortingOption": 0, "states": []}
            async with req(session, "post", f"{base}/services/x/career-site/v1/search", json=body,
                           headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json",
                                    "Authorization": f"Bearer {token}", "X-Requested-With": "XMLHttpRequest",
                                    "Referer": f"{base}/ux/ats/careersite/{site_id}/home?c={corp}"},
                           ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"CSOD {system}: search HTTP {r.status} {(await r.text())[:120]}")
                    break
                data = await r.json(content_type=None)
            d = (data or {}).get("data") or {}
            items = d.get("requisitions") or []
            if not items:
                break
            for rq in items:
                job = _csod_job(rq, system, base, site_id, corp)
                if job:
                    jobs.append(job)
            total = int(d.get("totalCount") or 0)
            if page * _CSOD_PAGE >= total or len(items) < _CSOD_PAGE or page >= 80:
                break
            page += 1
            await jitter()
    except Exception as e:
        logger.info(f"CSOD {system}: {e}")
    logger.info(f"  CSOD {system}: {len(jobs)} jobs")
    return jobs

async def run_csod(session) -> list[Job]:
    logger.info(f"CSOD: scraping {len(CSOD_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_csod(session, s, b, i) for s, (b, i) in CSOD_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  CSOD total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PAYCOM — Small Texas hospitals
# ══════════════════════════════════════════════════════════════════════════
PAYCOM_ORGS = {
    "Connally Memorial Medical Center": "772E59A3981B29A14463EC6C3223083C",
    # ── 2026-09-10 Texas block D (Y-texas-build): client keys read from each
    # hospital's careers page. The adapter was rebuilt the same day against
    # the SPA portal's API (see scrape_paycom).
    "Childress Regional Medical Center": "B47435CED3FD56BC6C37E9204206C989",
    "El Paso Children's Hospital":       "2B54F2860AD440DB8B3B7C9BE2122564",
    "Nacogdoches Memorial Hospital":     "C4638CB50E7EB9BDFE01AC5E31D77604",
    "Rolling Plains Memorial Hospital":  "1C3BB3931F4EB94FBCE852E05A8DEDA5",
    "SUN Behavioral Houston":            "292ACA5AA961D98C89091BAB7A81FFE5",
    # ── 2026-09-22 Texas resume: client keys from the fingerprint pass
    # (data/fingerprints/20260917-0420), each validated in a dry run with
    # full bodies in-feed. Family Hospital Systems' key serves Brushy Creek
    # (Round Rock) and its other campuses, 44 TX rows.
    "Uvalde Memorial Hospital":          "4F9268B125C765E56921BD62380F6FDA",   # 16
    "Electra Memorial Hospital":         "CBCA8A5823AC214457E7B0DDADBFF53F",   # 30
    "Iraan General Hospital":            "97DBDEC2FD8ADF5E89319516BC5CB310",   # 10
    "Fisher County Hospital District":   "D1D2477089224081140D652FE53B44BA",   # 4
    "Sweeny Community Hospital":         "B0F026CC4B9A9CD4D87975E3BBD9A6A1",   # 12
    "Reagan Memorial Hospital":          "D4C72F026DE938686F749E27665CEBE1",   # 5
    "Family Hospital Systems":           "D9DFA45B3E3394DFD6AF110856BDF669",   # 56 (Brushy Creek Family Hospital + sister campuses)
    "Hemphill County Hospital":          "BA0F97E1F0BBEA0363815A42D822FDF2",   # 20
    # 2026-09-24 Texas configs.
    "Texas Institute for Surgery":       "6613B1554FEB2852B28AB89172680E60",   # 8, Dallas
    # ── Added from scraper1.xlsx expansion ──
    "Paycom Hospital 2": "4863CB61AD1B2555F37E9E5884626947",
    "Paycom Hospital 3": "C48961799EBD231096CE8423D325C34C",
    "Paycom Hospital 4": "0FD7E535C5AC57A6144B389ACAA1998B",
    "Paycom Hospital 5": "8236C138F02B1587E10CAE245C2E6EE6",
    "Paycom Hospital 6": "BA896DB60A5046DD23CC67AB5801923F",
}

_PAYCOM_LIST = "https://www.paycomonline.net/v4/ats/web.php/jobs"
_PAYCOM_PAGE = 50
_PAYCOM_DETAIL_MAX = int(os.getenv("PAYCOM_DETAIL_MAX", "300"))   # detail fetches per client key per run
_PAYCOM_FILTERS = {"distanceFrom": 0, "workEnvironments": [], "positionTypes": [], "educationLevels": [],
                   "categories": [], "travelTypes": [], "shiftTypes": [], "otherFilters": [],
                   "keywordSearchText": "", "location": "", "sortOption": "N"}


def _paycom_clean(desc: str) -> str:
    """Detail descriptions arrive as HTML whose inner tags are entity-escaped
    again (&lt;span style=...&gt;): unescape until stable, then strip (2026-09-10)."""
    for _ in range(3):
        u = _html_unescape(desc)
        if u == desc:
            break
        desc = u
    return re.sub(r"<[^>]*$", "", re.sub(r"<[^>]+>", " ", strip_html(desc))).strip()   # also a dangling open tag


def _paycom_job(prev: dict, detail: dict | None, system: str, client_key: str, default_state: str = "TX") -> Job | None:
    """One job-posting preview (+ optional job-postings/<id> detail) -> Job.
    Preview: jobId, jobTitle, positionType, remoteType, locations ("Main 499 -
    Floresville, TX 78114" / "Childress, TX 79201"), description (150-char
    teaser), postedOn, isHotJob. Detail (jobPosting): city, location,
    salaryRange, description... The SPA route for a posting is
    /portal/<key>/jobs/<jobId> (the classic ViewJobDetails URL 302s into
    that portal). 2026-09-10."""
    jid = str(prev.get("jobId") or "").strip()
    title = str(prev.get("jobTitle") or "").strip()
    if not jid or not title:
        return None
    det = (detail or {}).get("jobPosting") if isinstance(detail, dict) else None
    det = det if isinstance(det, dict) else (detail if isinstance(detail, dict) else {})
    loc_text = str(det.get("location") or prev.get("locations") or "")
    city = str(det.get("city") or "").strip()
    st = str(det.get("state") or "").strip()
    m = re.search(r"([A-Za-z .'-]+?),\s*([A-Z]{2})\b(?:\s+\d{5})?\s*$", loc_text)
    if m:
        city = city or m.group(1).split(" - ")[-1].strip()
        st = st or m.group(2)
    st = (st or default_state).upper()
    desc = str(det.get("description") or det.get("jobDescription") or prev.get("description") or "")
    pay = str(det.get("salaryRange") or "").strip()
    if pay:
        desc = f"Pay: {pay}\n\n{desc}"
    posted = str(det.get("postedOn") or det.get("postedDate") or prev.get("postedOn") or "")[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", posted):
        posted = ""                      # "1 day ago" style strings are not dates
    if city.isupper():
        city = city.title()              # "SWEETWATER" (Rolling Plains)
    return Job(
        title=title, hospital_system=system, hospital_name=system,
        city=city, state=st, location=f"{city}, {st}".strip(", "),
        specialty="", job_type=str(prev.get("positionType") or det.get("positionType") or ""),
        url=f"https://www.paycomonline.net/v4/ats/web.php/portal/{client_key}/jobs/{jid}",
        job_id=jid, posted_date=posted, description=_paycom_clean(desc), ats_platform="Paycom",
    )


async def scrape_paycom(session: aiohttp.ClientSession, system: str, client_key: str) -> list[Job]:
    # 2026-09-10 (Z-texas-acute-D): rebuilt. web.php/jobs?clientkey= is an SPA
    # now; the same URL with Accept: application/json returns the portal's
    # libConfig with a sessionJWT and atsPortalMantleServiceUrl. The SPA
    # chunk (career-portal/main/index-sprawl-*.js) posts {skip, take,
    # filtersForQuery} to api/ats/job-posting-previews/search with the JWT
    # as Bearer and reads api/ats/job-postings/<jobId> for the detail.
    # Connally 54 / Childress 14 previews on 2026-09-10.
    jobs: list[Job] = []
    try:
        async with req(session, "get", _PAYCOM_LIST, params={"clientkey": client_key},
                       headers={**HEADERS, "Accept": "application/json"},
                       ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"Paycom {system}: HTTP {r.status}")
                return []
            cfg = await r.json(content_type=None)
        jwt = (cfg or {}).get("sessionJWT") or ""
        base = str(((cfg or {}).get("libConfig") or {}).get("atsPortalMantleServiceUrl") or "").rstrip("/")
        if not jwt or not base:
            logger.info(f"Paycom {system}: libConfig without sessionJWT / service url")
            return []
        hdr = {**HEADERS, "Accept": "application/json", "Content-Type": "application/json",
               "Authorization": f"Bearer {jwt}", "Locale": "en-US", "Translation-Highlights": "false",
               "Origin": "https://www.paycomonline.net",
               "Referer": f"{_PAYCOM_LIST}?clientkey={client_key}"}
        previews: list[dict] = []
        skip = 0
        while True:
            async with req(session, "post", f"{base}/api/ats/job-posting-previews/search",
                           json={"skip": skip, "take": _PAYCOM_PAGE, "filtersForQuery": dict(_PAYCOM_FILTERS)},
                           headers=hdr, ssl=False, proxy=proxies.get(),
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Paycom {system}: search HTTP {r.status} {(await r.text())[:120]}")
                    break
                data = await r.json(content_type=None)
            items = (data or {}).get("jobPostingPreviews") or []
            previews.extend(items)
            total = int((data or {}).get("jobPostingPreviewsCount") or 0)
            skip += len(items)
            if not items or skip >= total or len(items) < _PAYCOM_PAGE:
                break
            await jitter()
        for n, prev in enumerate(previews):
            detail = None
            if n < _PAYCOM_DETAIL_MAX and prev.get("jobId"):
                try:
                    async with req(session, "get", f"{base}/api/ats/job-postings/{prev['jobId']}",
                                   headers=hdr, ssl=False, proxy=proxies.get(),
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                        if r.status == 200:
                            detail = await r.json(content_type=None)
                    await asyncio.sleep(random.uniform(0.3, 0.9))
                except Exception as e:
                    logger.info(f"Paycom {system}: detail {prev.get('jobId')}: {e}")
            job = _paycom_job(prev, detail, system, client_key)
            if job:
                jobs.append(job)
    except Exception as e:
        logger.info(f"Paycom {system}: {e}")
    logger.info(f"  Paycom {system}: {len(jobs)} jobs")
    return jobs

async def run_paycom(session) -> list[Job]:
    logger.info(f"Paycom: scraping {len(PAYCOM_ORGS)} systems...")
    # 2026-09-24: every client key is on paycomonline.net; HOST_CONCURRENCY at a
    # time instead of all of them at once.
    results = await _gather_by_host(session, list(PAYCOM_ORGS.items()), scrape_paycom,
                                    lambda k: _url_host(_PAYCOM_LIST))
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Paycom total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PAYCOR — Texas rural hospitals (Townsen, Coon, Hereford)
# ══════════════════════════════════════════════════════════════════════════
PAYCOR_ORGS = {
    # Titus Regional removed 2026-09-24: its board reads "no open jobs" since
    # CHRISTUS took the hospital over (CHRISTUS Mount Pleasant, covered by alias).
    # 2026-09-24 Texas configs: Hereford's careers page embeds its clientId
    # through a newton.newtonsoftware.com iframe.
    "Coon Memorial Hospital":           "8a7883c69297218301929c1072ca01b5",   # 22, Dalhart
    "Hereford Regional Medical Center": "8a7883c65f99acc5015fc5bb4d5073ef",   # 15, Hereford
    # 2026-09-10 (Z-texas-acute-D): townsenmemorial.com/careers links this
    # clientId ("Click To See Our Career Opportunities").
    "Townsen Memorial Hospital": "8a7883d090b87b970190e27961ce11c4",
    # ── Added from scraper1.xlsx expansion ──
    "Paycor Hospital 2": "8a7883d07725ca8701773c07f64d08fa",
}

_PAYCOR_HOME = "https://recruitingbypaycor.com/career/CareerHome.action"
_PAYCOR_ROW_RE = re.compile(
    r'<a[^>]+href="([^"]*JobIntroduction\.action\?[^"]*\bid=([0-9a-f]+)[^"]*)"[^>]*>\s*(.*?)\s*</a>'
    r'(?:(?!gnewtonCareerGroupJobTitleClass).)*?gnewtonCareerGroupJobDescriptionClass"\s*>\s*(.*?)\s*</div>',
    re.S)
_PAYCOR_HEAD_RE = re.compile(r'gnewtonCareerGroupHeaderClass"\s*>\s*(.*?)\s*</div>', re.S)


def _parse_paycor_home(text: str, system: str, client_id: str, default_state: str = "TX") -> list[Job]:
    """CareerHome.action is a server-rendered Newton (gnewton) table: a
    department header ("Emergency Room - Humble"), then rows with the title
    link (JobIntroduction.action?clientId=&id=<hex>) and a description line
    "Hospital - Humble, TX" (facility - city, state). 2026-09-10."""
    jobs: list[Job] = []
    # walk header by header so the department is known for each row
    heads = list(_PAYCOR_HEAD_RE.finditer(text))
    for n, h in enumerate(heads):
        seg = text[h.end(): heads[n + 1].start() if n + 1 < len(heads) else len(text)]
        dept = _html_unescape(re.sub(r"<[^>]+>", "", h.group(1))).strip()
        dept = dept.split(" - ")[0].strip()
        for m in _PAYCOR_ROW_RE.finditer(seg):
            url, jid, title, line = m.group(1), m.group(2), m.group(3), m.group(4)
            title = _html_unescape(re.sub(r"<[^>]+>", "", title)).strip()
            line = _html_unescape(re.sub(r"<[^>]+>", "", line)).strip()
            facility, _, loc = line.rpartition(" - ")
            city, st = parse_city_state(loc or line)
            if not title or not jid:
                continue
            jobs.append(Job(
                title=title, hospital_system=system, hospital_name=system,
                city=city, state=(st or default_state).upper(),
                location=f"{city}, {(st or default_state).upper()}".strip(", "),
                specialty=dept, job_type="",
                url=_html_unescape(url), job_id=jid, posted_date="", description="",
                ats_platform="Paycor",
            ))
    return jobs


async def scrape_paycor(session: aiohttp.ClientSession, system: str, client_id: str) -> list[Job]:
    # 2026-09-10 (Z-texas-acute-D): the guessed CareerJobSearch.action JSON
    # endpoint 404s; the board is server-rendered HTML (all jobs on the home
    # page, no paging), parsed by _parse_paycor_home. Titus and "Paycor
    # Hospital 2" had returned 0 rows since they were added.
    try:
        async with req(session, "get", _PAYCOR_HOME, params={"clientId": client_id},
                       headers={**HEADERS, "Accept": "text/html,*/*"},
                       ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"Paycor {system}: HTTP {r.status}")
                return []
            text = await r.text()
    except Exception as e:
        logger.info(f"Paycor {system}: {e}")
        return []
    jobs = _parse_paycor_home(text, system, client_id)
    logger.info(f"  Paycor {system}: {len(jobs)} jobs")
    return jobs

async def run_paycor(session) -> list[Job]:
    logger.info(f"Paycor: scraping {len(PAYCOR_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_paycor(session, s, c) for s, c in PAYCOR_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Paycor total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PAYLOCITY — recruiting.paylocity.com/recruiting/jobs/All/<guid>/<org>
#  2026-09-10 (Y-texas-build): the listing page embeds `window.pageData =
#  {...}` whose Jobs[] carry JobId, JobTitle, LocationName, PublishedDate,
#  Description (HTML) and JobLocation{City, State, Zip}. The v2 feed
#  (/recruiting/v2/api/feed/jobs/<guid>) answers {"jobs": []} for these orgs,
#  so the page is the source. Deep link: /Recruiting/Jobs/Details/<JobId>.
# ══════════════════════════════════════════════════════════════════════════
PAYLOCITY_ORGS = {
    # Format: "System": (company guid, org slug, default state)
    # White Rock Medical Center (Dallas): 18 rows on 2026-09-10, all Dallas TX.
    "White Rock Medical Center": ("2bd01e6d-d70f-4766-9be9-6f4c349647bc", "White-Rock-Medical-Center", "TX"),
    # 2026-09-10 (Z-texas-acute-D): eastlandmemorial.com's employment page
    # links this board under "Current Job Openings".
    "Eastland Memorial Hospital": ("c1f736ba-2df6-4f47-95f4-117e45c82e0d", "Eastland-Memorial-Hospital-District", "TX"),
    # ── 2026-09-22 Texas resume: twelve rural districts from the fingerprint
    # pass, each validated in a dry run (counts in the comments). Paylocity
    # answers HTTP 429 to a burst of requests; the runner's jitter spaces them.
    # Altus Community Healthcare's board covers its Lumberton hospital and
    # its free-standing ER sites.
    "Coleman County Medical Center":    ("59046f49-fc75-436f-bc94-1ec0a76d1d2b", "Coleman-County-Medical-Center", "TX"),        # 12
    "Sabine County Hospital":           ("fa086daf-eefa-4e0d-9672-4bc0f1bca7c8", "Sabine-County-Hospital", "TX"),               # 8
    "Shamrock General Hospital":        ("19e0f8e8-d462-4685-87f4-50d63482b5f7", "Shamrock-General-Hospital", "TX"),            # 7
    "Lynn County Hospital District":    ("decdd05f-efa3-4ad9-80c5-8a7dbd93d653", "Lynn-County-Hospital-District", "TX"),        # 7
    "Stonewall Memorial Hospital":      ("5a6d7eef-c623-48b5-ba82-ecd26847b3ca", "Stonewall-Memorial-Hospital-District", "TX"), # 15
    "Martin County Hospital District":  ("4a44721e-de82-4004-b17f-bb712eee0534", "Martin-County-Hospital-District", "TX"),      # 17
    "Crane County Hospital District":   ("edfc210c-c5b1-44c5-83a0-804263517d79", "Crane-County-Hospital-District", "TX"),       # 10
    "Kimble Hospital":                  ("1bcf0c7d-43e3-4c68-89fc-d02935d1b57e", "Kimble-Hospital", "TX"),                      # 9
    "Culberson Hospital":               ("1f59bb01-19cc-4592-9fe8-4a17210e69cd", "Culberson-Hospital", "TX"),                   # 8
    "Schleicher County Medical Center": ("a6bb2c61-8ed5-4bd9-99d7-8aa25c93983f", "Schleicher-County-Medical-Center", "TX"),     # 5
    "Altus Community Healthcare":       ("071ad9aa-36cc-453e-baad-b9aed49da904", "Altus-Community-Healthcare", "TX"),           # 86
    "Graham Regional Medical Center":   ("b5e3394e-0c3f-46b1-831d-54975f36e6aa", "Graham-Hospital-District", "TX"),             # 22
    # 2026-09-24 Texas configs.
    "Reeves Regional Health":           ("107b349f-7b56-40e0-9ffc-18913ca2343c", "Reeves-Regional-Health", "TX"),               # 24, Pecos
    "Muleshoe Area Medical Center":     ("31daf266-5e69-4889-9acf-8a5cd298b4b0", "Muleshoe-Area-Hospital-District", "TX"),      # 10, Muleshoe
}
_PAYLOCITY_PAGEDATA_RE = re.compile(r"window\.pageData\s*=\s*(\{)")


def _parse_paylocity_page(text: str, system: str, guid: str, org: str, default_state: str) -> list[Job]:
    m = _PAYLOCITY_PAGEDATA_RE.search(text)
    if not m:
        return []
    data, _end = json.JSONDecoder().raw_decode(text, m.start(1))
    jobs = []
    for j in data.get("Jobs") or []:
        jid = j.get("JobId")
        title = (j.get("JobTitle") or "").strip()
        if not jid or not title or j.get("IsInternal"):
            continue
        loc = j.get("JobLocation") or {}
        city = (loc.get("City") or j.get("LocationName") or "").strip()
        state = (loc.get("State") or "").strip().upper() or default_state
        jobs.append(Job(
            title=title, hospital_system=system, hospital_name=system,
            city=city, state=state, location=f"{city}, {state}".strip(", "),
            specialty=j.get("HiringDepartment") or "", job_type="",
            url=f"https://recruiting.paylocity.com/Recruiting/Jobs/Details/{jid}",
            job_id=str(jid), posted_date=str(j.get("PublishedDate") or "")[:10],
            description=strip_html(j.get("Description") or "").strip(),
            ats_platform="Paylocity",
        ))
    return jobs


async def scrape_paylocity(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    guid, org, default_state = org_data
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{guid}/{org}"
    try:
        async with req(session, "get", url, headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
                       proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"Paylocity {system}: HTTP {r.status}")
                return []
            text = await r.text()
    except Exception as e:
        logger.info(f"Paylocity {system}: {e}")
        return []
    jobs = _parse_paylocity_page(text, system, guid, org, default_state)
    logger.info(f"  Paylocity {system}: {len(jobs)} jobs")
    return jobs


async def run_paylocity(session) -> list[Job]:
    if not PAYLOCITY_ORGS:
        return []
    logger.info(f"Paylocity: scraping {len(PAYLOCITY_ORGS)} systems...")
    ordered = priority_states_first(list(PAYLOCITY_ORGS.items()), lambda kv: kv[1][2])
    # 2026-09-24: Paylocity answers 429 to a burst, and this used to request
    # every board at once; HOST_CONCURRENCY at a time, spaced, and a 429 is
    # retried after its Retry-After (see _FallbackResponse).
    results = await _gather_by_host(session, ordered, scrape_paylocity,
                                    lambda cfg: "recruiting.paylocity.com")
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Paylocity total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  WORKABLE — apply.workable.com/<account>/ (added 2026-09-10, Z-texas-acute-D)
#  POST https://apply.workable.com/api/v3/accounts/<account>/jobs with
#  {query, location, department, worktype, remote} -> {total, results[],
#  nextPage}; the next page is requested with "token": nextPage in the body
#  (a ?token= query parameter repeats page 1). Items: id, shortcode, title,
#  remote, location{city, region}, published, type ("full"/"part"),
#  department[], workplace. Deep link: /<account>/j/<shortcode>/.
# ══════════════════════════════════════════════════════════════════════════
WORKABLE_ORGS = {
    # Format: "System": (account slug, default state)
    # Huntsville Memorial Hospital: 59 jobs on 2026-09-10, all Huntsville TX.
    "Huntsville Memorial Hospital": ("huntsville-memorial-hospital", "TX"),
    # 2026-09-22 Texas resume (dry-run counts): both boards answer the v3 API.
    "Houston Behavioral Healthcare Hospital": ("houston-behavioral-healthcare-hospital", "TX"),   # 11
    "Yoakum Community Hospital":              ("yoakum-community", "TX"),                         # 10
    # 2026-09-24 Texas configs: three Signature Healthcare behavioral hospitals.
    "San Antonio Behavioral Healthcare Hospital": ("sanantoniobehavioral", "TX"),                     # 22
    "Georgetown Behavioral Health Institute":     ("georgetown-behavioral-health-institute", "TX"),   # 28
    "Dallas Behavioral Healthcare Hospital":      ("dbhh", "TX"),                                     # 26, De Soto
}
_WORKABLE_TYPES = {"full": "Full time", "part": "Part time", "contract": "Contract", "temporary": "Temporary"}


def _workable_job(j: dict, system: str, slug: str, default_state: str) -> Job | None:
    code = str(j.get("shortcode") or "").strip()
    title = str(j.get("title") or "").strip()
    if not code or not title or j.get("isInternal"):
        return None
    loc = j.get("location") or {}
    city = str(loc.get("city") or "").strip()
    region = str(loc.get("region") or "").strip()
    st = parse_city_state(f"{city}, {region}")[1] if region else ""
    st = (st or (region if len(region) == 2 else "") or default_state).upper()
    dept = j.get("department") or []
    return Job(
        title=title, hospital_system=system, hospital_name=system,
        city=city, state=st, location=f"{city}, {st}".strip(", "),
        specialty=str(dept[-1] if isinstance(dept, list) and dept else dept or ""),
        job_type=_WORKABLE_TYPES.get(str(j.get("type") or "").lower(), str(j.get("type") or "")),
        url=f"https://apply.workable.com/{slug}/j/{code}/",
        job_id=code, posted_date=str(j.get("published") or "")[:10], description="",
        ats_platform="Workable",
    )


async def scrape_workable(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    slug, default_state = org_data
    jobs: list[Job] = []
    api = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    body = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
    token, pages = None, 0
    while True:
        try:
            payload = {**body, **({"token": token} if token else {})}
            async with req(session, "post", api, json=payload,
                           headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json",
                                    "Referer": f"https://apply.workable.com/{slug}/"},
                           ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Workable {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
        except Exception as e:
            logger.info(f"Workable {system}: {e}")
            break
        items = (data or {}).get("results") or []
        for j in items:
            job = _workable_job(j, system, slug, default_state)
            if job:
                jobs.append(job)
        token = (data or {}).get("nextPage")
        pages += 1
        if not items or not token or pages >= 60:
            break
        await jitter()
    logger.info(f"  Workable {system}: {len(jobs)} jobs")
    return jobs


async def run_workable(session) -> list[Job]:
    if not WORKABLE_ORGS:
        return []
    logger.info(f"Workable: scraping {len(WORKABLE_ORGS)} systems...")
    ordered = priority_states_first(list(WORKABLE_ORGS.items()), lambda kv: kv[1][1])
    results = await asyncio.gather(*[scrape_workable(session, s_, cfg) for s_, cfg in ordered], return_exceptions=True)
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Workable total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  TALEO BUSINESS EDITION — <host>.tbe.taleo.net/<host>NN/ats/careers/v2/
#  (added 2026-09-10, Z-texas-acute-D). The v2 searchResults page shows ten
#  jobs and pages through a session-bound "next&rowFrom=" link, but the
#  board publishes an RSS feed of every open position:
#    <base>/ats/servlet/Rss?org=<ORG>&cws=<N>&WebPage=SRCHR_V2&WebVersion=0&_rss_version=2
#  with taleo:reqId / department / locationCity / locationState and the
#  full taleo:html-description (Baptist SE Texas: 140 items, 1.1 MB).
#  Deep link: <base>/ats/careers/v2/viewRequisition?org=&cws=&rid=<reqId>.
# ══════════════════════════════════════════════════════════════════════════
TALEO_BE_ORGS = {
    # Format: "System": (base up to the instance path, org code, cws number, default state)
    # bhset.net/careers links phf02/ats/careers/v2/jobSearch?act=redirectCwsV2&cws=38&org=BHST.
    "Baptist Hospitals of Southeast Texas": ("https://phf.tbe.taleo.net/phf02", "BHST", "38", "TX"),
}
_TBE_NS = {"taleo": "urn:TBERss"}


def _parse_taleo_be_rss(xml_text: str, system: str, base: str, org: str, cws: str, default_state: str) -> list[Job]:
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    jobs: list[Job] = []
    root = ET.fromstring(xml_text)
    for it in root.iter("item"):
        def t(tag, ns=None):
            el = it.find(f"taleo:{tag}", _TBE_NS) if ns else it.find(tag)
            return (el.text or "").strip() if el is not None and el.text else ""
        rid = t("reqId", ns=True) or (re.search(r"rid=(\d+)", t("link") or "") or [None, ""])[1]
        title = t("title")
        if not rid or not title:
            continue
        city = t("locationCity", ns=True)
        st = t("locationState", ns=True)
        if len(st) > 2:
            st = parse_city_state(f"{city}, {st}")[1] or st
        if not (city or st):
            city, st = parse_city_state(t("location", ns=True))
        posted = ""
        try:
            posted = parsedate_to_datetime(t("pubDate")).strftime("%Y-%m-%d")
        except Exception:
            pass
        desc = t("html-description", ns=True) or t("description")
        jobs.append(Job(
            title=title, hospital_system=system, hospital_name=system,
            city=city, state=(st or default_state).upper(),
            location=f"{city}, {(st or default_state).upper()}".strip(", "),
            specialty=t("department", ns=True), job_type="",
            url=f"{base}/ats/careers/v2/viewRequisition?org={org}&cws={cws}&rid={rid}",
            job_id=rid, posted_date=posted, description=strip_html(_html_unescape(desc)),
            ats_platform="TaleoBE",
        ))
    return jobs


async def scrape_taleo_be(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    base, org, cws, default_state = org_data
    try:
        async with req(session, "get", f"{base}/ats/servlet/Rss",
                       params={"org": org, "cws": cws, "WebPage": "SRCHR_V2", "WebVersion": "0", "_rss_version": "2"},
                       headers={**HEADERS, "Accept": "application/rss+xml,text/xml,*/*"},
                       ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=90)) as r:
            if r.status != 200:
                logger.info(f"TaleoBE {system}: HTTP {r.status}")
                return []
            text = await r.text()
    except Exception as e:
        logger.info(f"TaleoBE {system}: {e}")
        return []
    try:
        jobs = _parse_taleo_be_rss(text, system, base, org, cws, default_state)
    except Exception as e:
        logger.info(f"TaleoBE {system}: RSS parse {e}")
        return []
    logger.info(f"  TaleoBE {system}: {len(jobs)} jobs")
    return jobs


async def run_taleo_be(session) -> list[Job]:
    if not TALEO_BE_ORGS:
        return []
    logger.info(f"TaleoBE: scraping {len(TALEO_BE_ORGS)} systems...")
    ordered = priority_states_first(list(TALEO_BE_ORGS.items()), lambda kv: kv[1][3])
    results = await asyncio.gather(*[scrape_taleo_be(session, s_, cfg) for s_, cfg in ordered], return_exceptions=True)
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  TaleoBE total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  HCTS PORTALS — <sub>.hctsportals.com (HealthcareSource's "Healthcare
#  Talent Source" career sites; added 2026-09-10, Z-texas-acute-D). Server-
#  rendered list at /jobs/search?page=N, 25 cards a page (<div class=
#  "jobs-section__item ...">) carrying the title link (/jobs/<id>-<slug>),
#  a Location line, Date Posted and a Facility line. UMC El Paso: 8 pages.
# ══════════════════════════════════════════════════════════════════════════
HCTS_PORTALS = {
    # Format: "System": (subdomain, default state)
    "University Medical Center of El Paso": ("umcelpasocareers", "TX"),
}
HCTS_MAX_PAGES = int(os.getenv("HCTS_MAX_PAGES", "40"))
_HCTS_ITEM_RE = re.compile(r'class="jobs-section__item[\s"]')
_HCTS_TITLE_RE = re.compile(r'<h2>\s*<a[^>]+href="([^"]*?/jobs/(\d+)[^"]*)"[^>]*>(.*?)</a>', re.S)


def _hcts_field(seg: str, label: str) -> str:
    m = re.search(r'title="\s*' + re.escape(label) + r'\s*"[^>]*>\s*</i>\s*</span>?\s*(?:&nbsp;)?\s*([^<]+)', seg)
    if not m:
        m = re.search(r'title="\s*' + re.escape(label) + r'\s*"[^>]*>\s*</i>\s*(?:&nbsp;)?\s*([^<]+)', seg)
    return _html_unescape(m.group(1)).strip() if m else ""


def _parse_hcts_page(text: str, system: str, sub: str, default_state: str) -> list[Job]:
    jobs: list[Job] = []
    starts = [m.start() for m in _HCTS_ITEM_RE.finditer(text)]
    for n, s in enumerate(starts):
        seg = text[s: starts[n + 1] if n + 1 < len(starts) else len(text)]
        m = _HCTS_TITLE_RE.search(seg)
        if not m:
            continue
        url, jid, title = m.group(1), m.group(2), _html_unescape(re.sub(r"<[^>]+>", "", m.group(3))).strip()
        if url.startswith("/"):
            url = f"https://{sub}.hctsportals.com{url}"
        loc = _hcts_field(seg, "Location")
        city, st = parse_city_state(re.sub(r",\s*United States\s*$", "", loc))
        posted = ""
        d = _hcts_field(seg, "Date Posted") or _hcts_field(seg, "Date Updated")
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
            try:
                posted = datetime.strptime(d, fmt).strftime("%Y-%m-%d")
                break
            except Exception:
                continue
        facility = _hcts_field(seg, "Facility")
        jobs.append(Job(
            title=title, hospital_system=system, hospital_name=facility or system,
            city=city, state=(st or default_state).upper(),
            location=f"{city}, {(st or default_state).upper()}".strip(", "),
            specialty="", job_type="", url=url, job_id=jid, posted_date=posted, description="",
            ats_platform="HCTS",
        ))
    return jobs


async def scrape_hcts(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    sub, default_state = org_data
    jobs: list[Job] = []
    seen: set[str] = set()
    for page in range(1, HCTS_MAX_PAGES + 1):
        try:
            async with req(session, "get", f"https://{sub}.hctsportals.com/jobs/search", params={"page": page},
                           headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
                           ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status != 200:
                    logger.info(f"HCTS {system}: HTTP {r.status} on page {page}")
                    break
                text = await r.text()
        except Exception as e:
            logger.info(f"HCTS {system}: page {page}: {e}")
            break
        batch = [j for j in _parse_hcts_page(text, system, sub, default_state) if j.job_id not in seen]
        if not batch:
            break
        seen.update(j.job_id for j in batch)
        jobs.extend(batch)
        await jitter()
    logger.info(f"  HCTS {system}: {len(jobs)} jobs")
    return jobs


async def run_hcts(session) -> list[Job]:
    if not HCTS_PORTALS:
        return []
    logger.info(f"HCTS: scraping {len(HCTS_PORTALS)} portals...")
    ordered = priority_states_first(list(HCTS_PORTALS.items()), lambda kv: kv[1][1])
    results = await asyncio.gather(*[scrape_hcts(session, s_, cfg) for s_, cfg in ordered], return_exceptions=True)
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  HCTS total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  HCA HEALTHCARE — browserless Talemetry crawl via curl_cffi (rebuilt 2026-07-28)
#
#  History: careers.hcahealthcare.com sits behind Cloudflare. aiohttp got 403
#  everywhere (Apr 2026), and the Playwright/patchright build that replaced it
#  never cleared the challenge on Railway either — HCA wrote 25 rows on
#  2026-04-27 and nothing since. The block turned out to be TLS-fingerprint-
#  specific: a Chrome-profile TLS handshake gets 403, but a Firefox
#  fingerprint sails through (verified 3/3 fresh sessions, 2026-07-28).
#  curl_cffi impersonate="firefox" + plain GETs is all it takes — no browser,
#  no proxy.
#
#  Shape: GET /search/jobs/in/{state-slug}?q=&page=N&per_page=500 (HTML).
#  The unsegmented search silently truncates at 10,000 rows (page 21 @500
#  returns nothing and reports total 0), so we crawl per state — the largest
#  (FL, ~4.6k) is comfortably under the cap; a per-state crawl was proven to
#  collect exactly the site-reported total (16,263 unique ids on 2026-07-28).
#  ~16.3k jobs / ~186 hospitals / 23 states in ~40 requests. Each card
#  carries the real facility name ("Medical City Plano"), a "City, ST"
#  location, title, absolute URL, and the stable Talemetry job id.
# ══════════════════════════════════════════════════════════════════════════

HCA_BASE = "https://careers.hcahealthcare.com"
# Active-state slugs from the site's State facet (2026-07-28; audited
# 2026-09-10, S-scraper-2: ms-mississippi removed — HCA has no Mississippi
# facilities and the site answered that slug with the NATIONAL search, 10,000
# rows of other states; _hca_fetch_slice now catches that on page 1).
# It is the FLOOR: run_hca also reads the live State facet and crawls any
# slug it finds that is missing here (logged), so a new HCA state cannot go
# uncrawled. Run order is set by priority_states_first (CO, CA first).
HCA_STATE_SLUGS = [
    "ak-alaska", "ar-arkansas", "ca-california", "co-colorado", "fl-florida",
    "ga-georgia", "id-idaho", "ks-kansas", "ky-kentucky", "la-louisiana",
    "mo-missouri", "nc-north-carolina", "nh-new-hampshire", "nm-new-mexico",
    "nv-nevada", "oh-ohio", "ok-oklahoma", "sc-south-carolina", "tn-tennessee",
    "tx-texas", "ut-utah", "va-virginia", "wi-wisconsin", "wy-wyoming",
]
HCA_PAGE_SIZE = 500
HCA_PAGE_CAP = 20                 # 20 x 500 = the site's 10,000-row search ceiling
HCA_IMPERSONATE = os.getenv("HCA_IMPERSONATE", "firefox")   # TLS profile; see _curl_fetch
# Overflow slicing, only for a state that fills HCA_PAGE_CAP full pages (the
# 10k ceiling truncated it): the same state is re-crawled per keyword and
# unioned by job id. No state is near it today (FL, the largest, ~4.6k), so
# this is the guard rail for the day one is.
HCA_OVERFLOW_QUERIES = ["nurse", "rn", "tech", "technologist", "therapist", "physician",
                        "assistant", "coordinator", "manager", "specialist", "pharmac",
                        "surgical", "patient", "clinical"]
_HCA_FACET_RE = re.compile(r'/search/jobs/in/([a-z]{2}-[a-z-]+)')
_HCA_FAILED_SLICES: list[str] = []

_HCA_ANCHOR_RE = re.compile(
    r'<a class="neu-link" href="(https://careers\.hcahealthcare\.com/jobs/(\d+)-[^"]+)"[^>]*>([^<]+)</a>'
)
_HCA_FACILITY_RE = re.compile(r'<div class="neu-text--caption">([^<]+)</div>')
_HCA_LOCATION_RE = re.compile(
    r'<div class="neu-text--caption neu-margin--bottom-10">(.*?)</div>', re.DOTALL
)
_HCA_TYPE_RE = re.compile(r'>work</i>\s*([^<&]+)')
# 2026-09-24: the results header ("Showing 1-500 of 4561 results"); the site's
# own count for the slice, checked against what the pages actually served.
_HCA_TOTAL_RE = re.compile(r'\bof\s+([\d,]+)\s+results?\b', re.I)
_HCA_CARD_MARK = "jobs-section__item-outer"   # one per card; _parse_hca_cards splits on it
HCA_SHORTFALL_TOL = 0.03   # a finished slice may trail the listed total by max(10, 3%)


def _hca_total(page_html: str) -> int | None:
    """The 'of N results' figure on a search page, or None when absent."""
    m = _HCA_TOTAL_RE.search(page_html or "")
    return int(m.group(1).replace(",", "")) if m else None


def _hca_is_challenge(page_html: str) -> bool:
    """A Cloudflare interstitial served with HTTP 200. 2026-09-24: NOT the
    'challenge-platform' substring: every normal page loads
    /cdn-cgi/challenge-platform/scripts/jsd/main.js (Cloudflare's bot-detection
    beacon), so that test flagged every empty past-the-end page as a challenge
    and all 25 slices of the 09-23 home run as PARTIAL. A real block comes back
    as a 403, which _curl_fetch already raises."""
    return "Just a moment" in page_html or "cf-chl" in page_html


def _hca_shortfall(got: int, total: int | None) -> bool:
    """True when the site listed clearly more results than the slice served."""
    return bool(total) and got + max(10, int(total * HCA_SHORTFALL_TOL)) < total


def _parse_hca_cards(page_html: str) -> list[Job]:
    """Parse one HCA search-results page by splitting on the card container.

    Card DOM — note facility/location captions come BEFORE the title anchor
    (the pre-rebuild parser looked after the anchor and would have shifted
    every card's facility onto its neighbor):
        <div class="neu-text--caption">Medical City Plano</div>
        <div class="neu-text--caption neu-margin--bottom-10"> Plano, TX, United States </div>
        <h2 class="neu-text--h6"><a class="neu-link" href=".../jobs/17676170-slug">Title</a></h2>
        ... work</i> Full-time ...
    The plain-caption regex cannot match the location div (different class
    attribute), so first-match-per-chunk is safe for the facility.
    """
    jobs: list[Job] = []
    for chunk in page_html.split("jobs-section__item-outer")[1:]:
        am = _HCA_ANCHOR_RE.search(chunk)
        if not am:
            continue
        url, job_id, title = am.group(1), am.group(2), htmllib.unescape(am.group(3)).strip()

        fac_m = _HCA_FACILITY_RE.search(chunk)
        facility = htmllib.unescape(fac_m.group(1)).strip() if fac_m else "HCA Healthcare"

        city = state = ""
        loc_m = _HCA_LOCATION_RE.search(chunk)
        if loc_m:
            loc_raw = re.sub(r"<[^>]+>", "", loc_m.group(1))
            loc_raw = re.sub(r"\s+", " ", loc_raw).strip().strip(",")
            parts = [p.strip() for p in loc_raw.split(",") if p.strip()]
            if parts:
                city = parts[0]
            for p in parts[1:]:
                if len(p) == 2 and p.isalpha():
                    state = p.upper()
                    break

        jt_m = _HCA_TYPE_RE.search(chunk)
        job_type = jt_m.group(1).strip().rstrip("&").strip() if jt_m else ""

        jobs.append(Job(
            title=title,
            hospital_system="HCA Healthcare",
            hospital_name=facility,
            city=city, state=state,
            location=f"{city}, {state}" if city and state else city or state,
            specialty="", job_type=job_type,
            url=url,
            job_id=str(job_id),
            posted_date="",
            description="",
            ats_platform="Talemetry",
        ))
    return jobs


def _curl_fetch(method: str, url: str, impersonate: str, timeout: int = 60, **kw):
    """One curl_cffi fetch honoring the house proxy policy: webshare pool
    FIRST, direct connection as the BACKUP — the same order the aiohttp
    adapters get from req()/_FallbackResponse.

    Rotates to the next pool entry on every call (proxies.get()). A proxied
    attempt that errors or returns non-200 (402 = pool out of bandwidth,
    407/5xx = proxy trouble) falls straight through to one direct attempt.
    Raises the last failure if both paths fail; caller owns retries/backoff.

    Why HCA needs the pool specifically: Cloudflare on careers.hcahealthcare.com
    blocks by TLS fingerprint AND IP reputation — the Firefox handshake passes
    from residential IPs (webshare pool, home connections; verified 200/99
    cards 2026-07-29) but is 403'd from Railway's datacenter IP (the
    2026-07-29 nightly pulled 2 of ~16,000 jobs going direct).
    """
    proxy = proxies.get()  # None when no pool is configured
    paths = ([{"http": proxy, "https": proxy}, None] if proxy else [None])
    last_exc = None
    for proxy_cfg in paths:
        try:
            s = curl_requests.Session(impersonate=impersonate)
            if proxy_cfg:
                s.proxies = proxy_cfg
            r = getattr(s, method)(url, timeout=timeout, **kw)
            if r.status_code == 200:
                return r
            last_exc = RuntimeError(
                f"HTTP {r.status_code} {'via proxy' if proxy_cfg else 'direct'}")
        except Exception as e:
            last_exc = e
    raise last_exc


def _hca_is_state_slug(slug: str) -> bool:
    """"co-colorado" yes; "el-paso" / "st-petersburg" no. The facet regex also
    matches the site's city links (2026-09-10 dry run), and a city slug costs
    a full national-search page before the page-1 guard rejects it."""
    code, _, name = slug.partition("-")
    return _US_STATE_CODES.get(name.replace("-", " ")) == code.upper()


def _hca_discover_state_slugs() -> list[str]:
    """Static list plus whatever the live State facet lists, transparency
    states first. The static list alone when the facet page cannot be read
    (logged, never fatal)."""
    slugs = list(HCA_STATE_SLUGS)
    try:
        r = _curl_fetch("get", f"{HCA_BASE}/search/jobs", HCA_IMPERSONATE, timeout=60,
                        params={"q": ""})
        live = sorted(x for x in set(_HCA_FACET_RE.findall(r.text)) if _hca_is_state_slug(x))
        new = [s for s in live if s not in slugs]
        if new:
            logger.info(f"  HCA: State facet lists {len(new)} slug(s) missing from "
                        f"HCA_STATE_SLUGS {new} — crawling them too; add them to the list")
            slugs.extend(new)
    except Exception as e:
        logger.info(f"  HCA: State facet unreadable ({e}); using the static list")
    return priority_states_first(slugs, lambda s: s[:2])


def _hca_fetch_slice(slug: str, q: str = "") -> tuple[list[Job], bool, bool]:
    """Blocking crawl of one state (optionally one keyword inside it).
    Returns (jobs, finished, capped): finished is False when a page failed
    every retry, came back as a Cloudflare challenge, or the listing ended
    well short of the site's own 'of N results' total; capped is True when
    the slice filled HCA_PAGE_CAP full pages (the 10k ceiling; the caller
    re-slices by keyword). The slice ends on the first page holding fewer
    than HCA_PAGE_SIZE cards."""
    out: list[Job] = []
    page, errors, last_err, total = 1, 0, None, None
    label = f"{slug}?q={q}" if q else slug
    while True:
        try:
            r = _curl_fetch(
                "get", f"{HCA_BASE}/search/jobs/in/{slug}", HCA_IMPERSONATE, timeout=90,
                params={"q": q, "page": str(page), "per_page": str(HCA_PAGE_SIZE)},
            )
            cards = _parse_hca_cards(r.text)
        except Exception as e:
            last_err = e
            errors += 1
            if errors >= 4:
                # Each _curl_fetch already tried proxy AND direct, so 4 loop
                # errors = up to 8 failed attempts. Surface WHY: 402 = pool
                # out of bandwidth, 403 = Cloudflare IP block, else network.
                logger.info(f"  HCA {label}: page {page} failed {errors}x (last: {last_err}) "
                            f"— stopping at {len(out)} jobs; slice marked PARTIAL")
                return out, False, False
            time.sleep(min(30, 3 * 2 ** errors))   # 6, 12, 24 s: outlasts a WAF burst window
            continue
        errors = 0
        total = total or _hca_total(r.text)
        if not cards:
            if _hca_is_challenge(r.text):
                # 2026-09-16: a Cloudflare challenge page, not an empty state.
                logger.info(f"  HCA {label}: page {page} is a Cloudflare challenge; slice marked PARTIAL")
                return out, False, False
            if _hca_shortfall(len(out), total):
                logger.info(f"  HCA {label}: page {page} came back empty at {len(out)} of the {total} "
                            f"results the site lists; slice marked PARTIAL")
                return out, False, False
            # Past the end (the inventory was an exact multiple of the page size).
            return out, True, False
        if page == 1 and not q:
            # 2026-09-10 (S-scraper-2 dry run): a slug the site does not know
            # is NOT an empty page. ms-mississippi (HCA left the state) came
            # back as the unsegmented national search: 10,000 rows of other
            # states over 716 s. Recognise that on page 1 and stop.
            st = slug[:2].upper()
            matched = sum(1 for c in cards if c.state == st)
            if matched < len(cards) / 2:
                logger.info(f"  HCA {slug}: not a State facet value — page 1 is the national "
                            f"search ({matched}/{len(cards)} cards in {st}); skipping this slug "
                            f"(remove it from HCA_STATE_SLUGS)")
                return [], True, False
        out.extend(cards)
        # 2026-09-24: a page with fewer than a full page of cards is the last
        # one; finish here instead of requesting the empty page past the end.
        # Counted on the raw card containers, not the parsed jobs, so a card
        # the parser skips cannot end a slice early. The site's own total is
        # the cross-check: a short page well below it is a truncated crawl
        # (per_page capped, parser regression), and a truncated slice must
        # not finish, or the sweep would retire the pages it never read.
        if r.text.count(_HCA_CARD_MARK) < HCA_PAGE_SIZE:
            if _hca_shortfall(len(out), total):
                logger.info(f"  HCA {label}: page {page} ended the listing at {len(out)} of the {total} "
                            f"results the site lists; slice marked PARTIAL")
                return out, False, False
            return out, True, False
        if page >= HCA_PAGE_CAP:
            return out, True, True
        page += 1
        time.sleep(0.6)  # polite pacing — keep the WAF happy


def _hca_fetch_state(slug: str) -> list[Job]:
    """Per-state crawl — runs in a worker thread via asyncio.to_thread.
    Records unfinished slices in _HCA_FAILED_SLICES (run_hca turns that into
    PARTIAL_SYSTEMS so the sweep leaves HCA alone that night)."""
    jobs, finished, capped = _hca_fetch_slice(slug)
    if not finished:
        _HCA_FAILED_SLICES.append(slug)
    if capped:
        logger.warning(f"  HCA {slug}: filled {HCA_PAGE_CAP} pages — the {HCA_PAGE_CAP * HCA_PAGE_SIZE:,}-row "
                       f"search ceiling truncated it; re-crawling by keyword")
        ids = {j.job_id for j in jobs}
        for q in HCA_OVERFLOW_QUERIES:
            extra, ok, _ = _hca_fetch_slice(slug, q)
            if not ok:
                _HCA_FAILED_SLICES.append(f"{slug}?q={q}")
            for j in extra:
                if j.job_id not in ids:
                    ids.add(j.job_id)
                    jobs.append(j)
            time.sleep(0.6)
        logger.info(f"  HCA {slug}: {len(jobs):,} jobs after keyword slices")
    return jobs


async def run_hca(session: aiohttp.ClientSession) -> list[Job]:
    """HCA Healthcare — every division from the master site, no browser needed."""
    if curl_requests is None:
        logger.warning("HCA Healthcare: curl_cffi not installed — skipping")
        return []
    # 2026-09-16: on Railway (datacenter address) Cloudflare admits one page
    # per state and refuses FL / TX / GA outright, so the nightly's partial
    # crawl is never the inventory. When that partial yield crossed 25% of a
    # shrinking inventory it passed the sweep guard and deactivated the rest:
    # 16,941 rows from the 09-10 home run decayed to 2,367 by 09-15. HCA is
    # therefore LOCAL-ONLY: hca_local_push.py (a residential address) owns the
    # crawl and its own sweep; the nightly skips the crawl and marks HCA
    # PARTIAL so the sweep leaves its rows alone. HCA_NIGHTLY=1 re-enables it.
    if os.environ.get("RAILWAY_ENVIRONMENT") and os.environ.get("HCA_NIGHTLY") != "1":
        PARTIAL_SYSTEMS.add("HCA Healthcare")
        logger.warning("HCA Healthcare: local-only (Railway address is Cloudflare-blocked); "
                       "crawl skipped, sweep skipped; run hca_local_push.py from a home address")
        return []
    _HCA_FAILED_SLICES.clear()
    slugs = await asyncio.to_thread(_hca_discover_state_slugs)
    logger.info(f"HCA Healthcare: browserless crawl, {len(slugs)} states, transparency states "
                f"first ({HCA_IMPERSONATE} TLS)...")

    all_jobs: list[Job] = []
    per_state: dict[str, int] = {}
    BATCH = 4  # states in flight at once ≈ 2 req/s peak across threads
    for i in range(0, len(slugs), BATCH):
        batch = slugs[i:i + BATCH]
        results = await asyncio.gather(
            *[asyncio.to_thread(_hca_fetch_state, s) for s in batch],
            return_exceptions=True,
        )
        for slug, res in zip(batch, results):
            if isinstance(res, Exception):
                logger.info(f"  HCA {slug}: ERROR {res}")
                _HCA_FAILED_SLICES.append(slug)
            else:
                logger.info(f"  HCA {slug}: {len(res)} jobs")
                per_state[slug[:2].upper()] = len(res)
                all_jobs.extend(res)

    seen: set[str] = set()
    unique: list[Job] = []
    for j in all_jobs:
        if j.job_id in seen:
            continue
        seen.add(j.job_id)
        unique.append(j)
    dupes = len(all_jobs) - len(unique)
    logger.info(f"  HCA Healthcare TOTAL: {len(unique):,} jobs across {len(per_state)} states"
                + (f" ({dupes} cross-state dupes removed)" if dupes else ""))
    if _HCA_FAILED_SLICES:
        # 2026-09-10: a slice that never finished means the crawl is not the
        # inventory. Say so and keep the sweep off HCA tonight (PARTIAL_SYSTEMS).
        PARTIAL_SYSTEMS.add("HCA Healthcare")
        logger.warning(f"  HCA Healthcare: PARTIAL run — {len(_HCA_FAILED_SLICES)} slice(s) did not "
                       f"finish {_HCA_FAILED_SLICES}; the per-system sweep skips HCA this run "
                       f"(403 = Cloudflare IP block: fund the residential pool or run "
                       f"hca_local_push.py from a home IP)")
    return unique


# ══════════════════════════════════════════════════════════════════════════
#  HOUSTON METHODIST — Workday CXS on wd12/GTI via curl_cffi (added 2026-07-28)
#
#  The old wd1 tenant (HoustonMethodist_External) returns HTTP 422 — the
#  tenant moved to wd12 and the external career site is now named "GTI"
#  (houstonmethodistcareers.org links only to .../en-US/GTI/...). Quirks that
#  keep it out of the generic Workday adapter: the CXS rejects limit > 20
#  with HTTP 400, and the wd12 edge 403s non-browser TLS, so this uses
#  curl_cffi Chrome impersonation in a worker thread. 1,414 jobs at build
#  time, all Houston-metro TX. locationsText is a building-level string
#  ("HM Willowbrook - Main Hospital Building"); the campus prefix maps to the
#  marketing facility name + city below.
# ══════════════════════════════════════════════════════════════════════════

HM_CXS_URL = "https://houstonmethodist.wd12.myworkdayjobs.com/wday/cxs/houstonmethodist/GTI/jobs"
HM_PUBLIC_BASE = "https://houstonmethodist.wd12.myworkdayjobs.com/en-US/GTI"
# locationsText prefix -> (facility marketing name, city). Longest prefixes
# are matched in list order. CMS lists Clear Lake's city as Nassau Bay.
HM_CAMPUS_MAP = [
    ("HM Baytown",              ("Houston Methodist Baytown Hospital", "Baytown")),
    ("HM Sugar Land",           ("Houston Methodist Sugar Land Hospital", "Sugar Land")),
    ("HM The Woodlands",        ("Houston Methodist The Woodlands Hospital", "The Woodlands")),
    ("HM Clear Lake",           ("Houston Methodist Clear Lake Hospital", "Nassau Bay")),
    ("HM Willowbrook",          ("Houston Methodist Willowbrook Hospital", "Houston")),
    ("HM Cypress",              ("Houston Methodist Cypress Hospital", "Cypress")),
    ("HM West",                 ("Houston Methodist West Hospital", "Houston")),
    ("HM Texas Medical Center", ("Houston Methodist Hospital", "Houston")),
    ("HM Continuing Care",      ("Houston Methodist Continuing Care Hospital", "Katy")),
]


def _hm_fetch_all() -> list[Job]:
    """Blocking full pull (~71 requests at limit=20) — runs in a worker thread.
    Webshare-pool-first / direct-backup via _curl_fetch (house policy)."""
    jobs: list[Job] = []
    offset, total, errors = 0, None, 0
    while total is None or offset < total:
        try:
            r = _curl_fetch(
                "post", HM_CXS_URL, "chrome", timeout=45,
                json={"limit": 20, "offset": offset, "searchText": "", "appliedFacets": {}},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            data = r.json()
        except Exception as e:
            errors += 1
            if errors >= 3:
                logger.info(f"  Houston Methodist: offset {offset} failed 3x ({e}) — stopping at {len(jobs)}")
                return jobs
            time.sleep(3 * errors)
            continue
        errors = 0
        total = data.get("total", 0)
        postings = data.get("jobPostings", [])
        if not postings:
            break
        for p in postings:
            loc_text = p.get("locationsText") or ""  # null on system-wide postings
            facility, city = "Houston Methodist", "Houston"
            for prefix, (fac, cty) in HM_CAMPUS_MAP:
                if loc_text.startswith(prefix):
                    facility, city = fac, cty
                    break
            path = p.get("externalPath") or ""
            bullets = p.get("bulletFields") or []
            job_id = bullets[0] if bullets else path.rsplit("_", 1)[-1]
            if not path or not job_id:
                continue
            jobs.append(Job(
                title=(p.get("title") or "").strip(),
                hospital_system="Houston Methodist",
                hospital_name=facility,
                city=city, state="TX",
                location=f"{city}, TX",
                specialty="", job_type="",
                url=f"{HM_PUBLIC_BASE}{path}",
                job_id=str(job_id),
                posted_date=p.get("postedOn") or "",
                description="",
                ats_platform="Workday",
            ))
        offset += 20
        time.sleep(0.4)
    return jobs


async def run_houston_methodist() -> list[Job]:
    if curl_requests is None:
        logger.warning("Houston Methodist: curl_cffi not installed — skipping")
        return []
    logger.info("Houston Methodist: Workday CXS wd12/GTI (curl_cffi)...")
    try:
        jobs = await asyncio.to_thread(_hm_fetch_all)
    except Exception as e:
        logger.info(f"  Houston Methodist: ERROR {e}")
        return []
    logger.info(f"  Houston Methodist: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  OCEANS HEALTHCARE — custom in-house job board (added 2026-07-28)
#
#  Behavioral-health chain (Oceans Behavioral Hospitals + Haven Behavioral,
#  acquired 2024): ~33 facilities across TX/LA/MS/PA/NM/OH/OK/AZ/ID, incl.
#  10 Texas psychiatric hospitals (Midland's is listed as "of the Permian
#  Basin"). No commercial ATS — a self-hosted ASP.NET MVC board at
#  oceansjobboard.com (careers.oceansjobboard.com is only a Weebly shell).
#  Recipe: GET /jobs embeds the first 25 jobs + FilterGroups in a Vue data
#  blob; POST /jobs/LoadMoreSearchCallback pages ~26 at a time, echoing the
#  server's FilterGroups back each round. ~328 jobs / 13 requests at build
#  time. Soft 404s: dead pages return HTTP 200 with a tiny "Page Not Found"
#  body — never trust status alone on the detail pages.
# ══════════════════════════════════════════════════════════════════════════

OCEANS_BASE = "https://oceansjobboard.com"
_OCEANS_DATA_START = re.compile(r'data:\s*(\{"FilterGroups")')


def _oceans_parse_blob(text: str) -> dict:
    """2026-09-10 (Y-texas-build): the board's Vue block gained a `computed:`
    member between `data:` and `mounted:`, so the old non-greedy regex
    (`data: {...}, mounted:`) captured the data object PLUS the computed block
    and json.loads raised "Extra data"; run_oceans logged the error and
    returned 0 rows every night since (734 rows in the table, none active,
    10 Texas psychiatric hospitals dark). Decode exactly one JSON object from
    the first `data: {"FilterGroups"` on, whatever follows it."""
    m = _OCEANS_DATA_START.search(text)
    if not m:
        raise RuntimeError("embedded Vue data blob not found (board redesigned?)")
    blob, _end = json.JSONDecoder().raw_decode(text, m.start(1))
    return blob


def _oceans_job(rec: dict) -> Optional[Job]:
    title = (rec.get("Title") or "").strip()
    job_no = rec.get("JobNumber")
    url_path = rec.get("Url") or ""
    if not title or not job_no or not url_path:
        return None
    # /job-detail/{JobNumber}/{yyyy-MM-dd} — the date segment is the post date
    dm = re.search(r"/(\d{4}-\d{2}-\d{2})$", url_path)
    city = (rec.get("City") or "").strip()
    state = (rec.get("State") or "").strip()
    return Job(
        title=title,
        hospital_system="Oceans Healthcare",
        hospital_name=(rec.get("LocationName") or "Oceans Healthcare").strip(),
        city=city, state=state,
        location=f"{city}, {state}" if city and state else city or state,
        specialty="",
        job_type="",
        url=f"{OCEANS_BASE}{url_path}",
        job_id=str(job_no),
        posted_date=dm.group(1) if dm else "",
        description="",
        ats_platform="OceansJobBoard",
    )


def _oceans_fetch_all() -> list[Job]:
    """Blocking full pull (~13 requests) — runs in a worker thread.
    The board is stateless (no cookies/CSRF — proven), so each request can go
    through _curl_fetch's webshare-first / direct-backup path independently."""
    r = _curl_fetch("get", f"{OCEANS_BASE}/jobs", "chrome", timeout=45)
    blob = _oceans_parse_blob(r.text)
    records = list(blob.get("Jobs") or [])
    filter_groups = blob.get("FilterGroups") or []
    has_more = bool(blob.get("HasMore"))

    guard = 0  # board is ~13 pages today; 60 caps a runaway HasMore loop
    while has_more and guard < 60:
        guard += 1
        try:
            rr = _curl_fetch(
                "post", f"{OCEANS_BASE}/jobs/LoadMoreSearchCallback", "chrome", timeout=45,
                json={"FilterGroups": filter_groups, "CurrentResultCount": len(records)},
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            logger.info(f"  Oceans: LoadMore failed ({e}) — stopping at {len(records)}")
            break
        d = rr.json()
        page_jobs = d.get("Jobs") or []
        if not page_jobs:
            break
        records.extend(page_jobs)
        filter_groups = d.get("FilterGroups") or filter_groups
        has_more = bool(d.get("HasMore"))
        time.sleep(0.4)

    out: list[Job] = []
    seen: set[str] = set()
    for rec in records:
        j = _oceans_job(rec)
        if j and j.job_id not in seen:
            seen.add(j.job_id)
            out.append(j)
    return out


async def run_oceans() -> list[Job]:
    if curl_requests is None:
        logger.warning("Oceans Healthcare: curl_cffi not installed — skipping")
        return []
    logger.info("Oceans Healthcare: crawling oceansjobboard.com...")
    try:
        jobs = await asyncio.to_thread(_oceans_fetch_all)
    except Exception as e:
        logger.info(f"  Oceans Healthcare: ERROR {e}")
        return []
    logger.info(f"  Oceans Healthcare: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  COMMUNITY HEALTH SYSTEMS (CHS) — WordPress WPJobBoard
# ══════════════════════════════════════════════════════════════════════════
async def run_chs(session: aiohttp.ClientSession) -> list[Job]:
    jobs = []
    LIMIT  = 60
    offset = 0

    CHS_HEADERS = {
        **HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.careershealthcare.com/job/",
        "Origin": "https://www.careershealthcare.com",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    logger.info("CHS: starting WPJobBoard scrape...")

    # Primary endpoint discovered via HAR Apr 29 2026:
    # api.careershealthcare.com/job/wp_job_grid returns {"payload":[<html>,...]}
    # Each payload entry is a self-contained HTML job card.
    probes = [
        ("get",  "https://api.careershealthcare.com/job/wp_job_grid",
         {"limit": LIMIT, "order-by": "title", "offset": 0, "_directory": 1}),
        # Legacy fallbacks
        ("get",  "https://www.careershealthcare.com/wp_job_grid",
         {"limit": LIMIT, "order-by": "title", "offset": 0, "_directory": 120}),
        ("post", "https://www.careershealthcare.com/wp-admin/admin-ajax.php",
         {"action": "wp_job_grid", "limit": LIMIT, "order-by": "title", "offset": 0, "_directory": 120}),
        ("get",  "https://www.careershealthcare.com/wp-admin/admin-ajax.php",
         {"action": "wp_job_grid", "limit": LIMIT, "order-by": "title", "offset": 0, "_directory": 120}),
        ("post", "https://www.careershealthcare.com/wp-admin/admin-ajax.php",
         {"action": "wp_job_grid", "limit": LIMIT, "order-by": "title", "offset": 0, "_directory": 1}),
    ]

    endpoint = None
    for method, probe_url, base_params in probes:
        try:
            fn = getattr(session, method)
            kw = {"params" if method == "get" else "data": base_params}
            async with fn(probe_url, **kw, headers=CHS_HEADERS,
                          timeout=aiohttp.ClientTimeout(total=20)) as r:
                logger.info(f"  CHS probe {probe_url} [{method.upper()}]: HTTP {r.status}")
                if r.status == 200:
                    text = await r.text()
                    logger.info(f"  CHS probe response preview: {text[:200]}")
                    if text.strip().startswith("{") or text.strip().startswith("["):
                        endpoint = (method, probe_url, base_params)
                        logger.info(f"  CHS: endpoint confirmed")
                        break
        except Exception as ex:
            logger.info(f"  CHS probe {probe_url}: {ex}")

    if not endpoint:
        logger.warning("CHS: could not find working endpoint — skipping")
        return []

    method, probe_url, base_params = endpoint
    while True:
        params = {**base_params, "offset": offset}
        kw = {"params" if method == "get" else "data": params}
        try:
            fn = getattr(session, method)
            async with fn(probe_url, **kw, headers=CHS_HEADERS,
                          timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    break
                text = await r.text()
                if not text.strip():
                    break
                try:
                    import json as _j
                    data = _j.loads(text)
                except:
                    break
                # Two response shapes possible:
                #   1. New: {"payload":[<html_card>, ...], "meta":{...}}  (api subdomain)
                #   2. Legacy: list of {job_title, job_city, ...} or {jobs:[...]}
                entries = []
                payload_html = None
                if isinstance(data, dict) and isinstance(data.get("payload"), list):
                    payload_html = data["payload"]
                else:
                    entries = data if isinstance(data, list) else (
                        data.get("jobs") or data.get("data") or []
                    )

                # Shape 1 — parse HTML cards (api.careershealthcare.com)
                if payload_html is not None:
                    if not payload_html:
                        break
                    parsed_count = 0
                    for card in payload_html:
                        if not isinstance(card, str):
                            continue
                        m_id    = re.search(r'data-id="(\d+)"', card)
                        m_hosp  = re.search(r'<h3>([^<]+)</h3>', card)
                        m_loc   = re.search(r'<h5 class="job-location">([^<]+)</h5>', card)
                        m_title = re.search(r'<h5 class="job-title">\s*<a [^>]*href="([^"]+)"[^>]*>([^<]+)</a>', card)
                        m_shift = re.search(r'<h6 class="job-shift">\s*([^<]+?)\s*</h6>', card)
                        if not (m_id and m_title): continue
                        jid = m_id.group(1)
                        jurl = m_title.group(1).strip()
                        title_t = re.sub(r'\s+', ' ', m_title.group(2)).strip()
                        hosp = (m_hosp.group(1).strip() if m_hosp else "Community Health Systems").replace("&#039;", "'").replace("&amp;", "&")
                        loc_str = m_loc.group(1).strip() if m_loc else ""
                        city, state = parse_city_state(loc_str)
                        jtype = (m_shift.group(1).strip() if m_shift else "")
                        jobs.append(Job(
                            title=title_t, hospital_system="Community Health Systems",
                            hospital_name=hosp, city=city, state=state,
                            location=loc_str or f"{city}, {state}".strip(", "),
                            specialty="", job_type=jtype, url=jurl, job_id=jid,
                            posted_date="",
                            description="",
                            ats_platform="WPJobBoard",
                        ))
                        parsed_count += 1
                    logger.info(f"  CHS offset {offset}: {parsed_count} jobs (total: {len(jobs)})")
                    if parsed_count < LIMIT:
                        break
                    offset += LIMIT
                    await jitter()
                    continue

                # Shape 2 — legacy object-list response
                if not entries:
                    break
                for j in entries:
                    title = j.get("job_title") or j.get("title") or ""
                    city  = j.get("job_city")  or j.get("city")  or ""
                    state = j.get("job_state") or j.get("state") or ""
                    loc   = j.get("job_location") or j.get("location") or f"{city}, {state}".strip(", ")
                    jid   = str(j.get("job_id") or j.get("id") or "")
                    jurl  = j.get("job_url") or j.get("url") or f"https://www.careershealthcare.com/job/{jid}"
                    hosp  = j.get("job_company") or j.get("company") or "Community Health Systems"
                    jtype = j.get("job_type") or j.get("employment_type") or ""
                    if not title or not jid:
                        continue
                    if not city or not state:
                        parts = [p.strip() for p in loc.split(",")]
                        if len(parts) >= 2:
                            city  = city  or parts[0]
                            state = state or parts[-1].strip().upper()[:2]
                    jobs.append(Job(
                        title=title, hospital_system="Community Health Systems",
                        hospital_name=hosp, city=city, state=state,
                        location=f"{city}, {state}" if city and state else loc,
                        specialty="", job_type=jtype, url=jurl, job_id=jid,
                        posted_date=j.get("job_date") or j.get("date") or "",
                        description=strip_html(j.get("job_description") or j.get("description") or ""),
                        ats_platform="WPJobBoard",
                    ))
                logger.info(f"  CHS offset {offset}: {len(entries)} jobs (total: {len(jobs)})")
                if len(entries) < LIMIT:
                    break
                offset += LIMIT
                await jitter()
        except Exception as e:
            logger.error(f"CHS offset {offset}: {e}")
            break

    logger.info(f"  CHS: {len(jobs):,} total jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  Atrium Health  —  Coveo-backed careers site, HTML pagination
# ══════════════════════════════════════════════════════════════════════════
# Atrium uses careers.atriumhealth.org with Coveo Search rendering job results
# directly into HTML on each page (25 jobs per page, ~119 pages total ≈ 2,975
# listings). Cloudflare-protected — needs residential proxy + browser headers.
#
# Pagination URL pattern (from sitestats tracker in HAR):
#   https://careers.atriumhealth.org/search/jobs?page=N&q=&location=
ATRIUM_BASE = "https://careers.atriumhealth.org"

# Pre-compiled parser: each result is a div.ihrecord.CoveoResult with id="R…",
# wrapping a CoveoResultLink anchor (title + href to /jobs/{ID}-{slug}) and a
# page-description div carrying location text ("City, ST, United States").
_ATRIUM_RESULT_RE = re.compile(
    r'<div class="ihrecord CoveoResult"[^>]*id="R(\d+)"[^>]*>'
    r'(?:[\s\S]{0,2500}?)<a class="CoveoResultLink" href="(https://careers\.atriumhealth\.org/jobs/[^"]+)">'
    r'([^<]+)</a>'
    r'(?:[\s\S]{0,1200}?)<div class="page-description">([\s\S]{0,500}?)</div>',
    re.IGNORECASE,
)


def _parse_atrium_html(html: str, page: int) -> tuple[list[Job], int]:
    """Parse one Atrium results page. Returns (jobs, max_page_seen).
    max_page_seen is the largest page number referenced in pagination links —
    used to know when to stop iterating."""
    out: list[Job] = []
    for r_id, href, title, desc in _ATRIUM_RESULT_RE.findall(html):
        title = re.sub(r'&amp;', '&', title).strip()
        if not title or not r_id:
            continue
        # Location text looks like:  "                    Charlotte,
        #                                NC,
        #                                United States"
        loc_clean = re.sub(r'\s+', ' ', desc).strip().rstrip(',').strip()
        # Strip ", United States" suffix
        loc_clean = re.sub(r',?\s*United States\s*$', '', loc_clean, flags=re.IGNORECASE).strip()
        # Pull state from "City, ST"
        m = re.search(r',\s*([A-Z]{2})\s*,?\s*$', loc_clean)
        state = m.group(1) if m else ""
        city = loc_clean.rsplit(",", 1)[0].strip() if state else loc_clean
        out.append(Job(
            title=title,
            hospital_system="Atrium Health",
            hospital_name="Atrium Health",
            city=city, state=state,
            location=f"{city}, {state}" if state else city,
            specialty="",
            job_type="",
            url=href,
            job_id=str(r_id),
            posted_date="",
            description="",
            ats_platform="Coveo",
        ))
    # Detect max page index referenced in pagination links
    page_nums = re.findall(r'href="[^"]*page=(\d+)[^"]*"', html)
    max_page = max((int(p) for p in page_nums), default=page)
    return out, max_page


async def run_atrium(session: aiohttp.ClientSession) -> list[Job]:
    """Atrium uses Cloudflare-fronted Coveo HTML. aiohttp + Webshare residential
    consistently 403s. Browser-based Patchright/Playwright path mirrors HCA:
       1. Launch Chromium through residential proxy
       2. Navigate page 1 (Cloudflare clears, sets cf_clearance)
       3. Use page.evaluate() with parallel fetch() to grab pages 2..N inside
          the cleared session (cookies ride along)
       4. Parse each HTML body with the existing _parse_atrium_html regex
    """
    # Try Patchright first, fall back to Playwright
    async_playwright = None
    using_patchright = False
    try:
        from patchright.async_api import async_playwright as _pw
        async_playwright = _pw; using_patchright = True
        logger.info("Atrium Health: Patchright detected — using anti-detection fork")
    except ImportError:
        try:
            from playwright.async_api import async_playwright as _pw
            async_playwright = _pw
            logger.info("Atrium Health: using vanilla Playwright")
        except ImportError:
            logger.warning("Atrium Health: neither patchright nor playwright installed — skipping")
            return []

    # Residential proxy (Atrium WAF flags datacenter IPs, same as HCA)
    pw_proxy = None
    proxy_url = proxies.get()
    if proxy_url:
        m = re.match(r"https?://([^:]+):([^@]+)@([^:]+):(\d+)", proxy_url)
        if m:
            pw_proxy = {"server": f"http://{m.group(3)}:{m.group(4)}",
                        "username": m.group(1), "password": m.group(2)}
            logger.info(f"Atrium Health: using residential proxy {m.group(3)}:{m.group(4)}")

    logger.info("Atrium Health: launching Chromium for Coveo HTML scrape...")
    all_jobs: list[Job] = []

    async with async_playwright() as pw:
        launch_kwargs = dict(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled",
                  "--disable-dev-shm-usage"],
        )
        if pw_proxy: launch_kwargs["proxy"] = pw_proxy
        browser = await pw.chromium.launch(**launch_kwargs)
        try:
            ctx = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="en-US", timezone_id="America/New_York",
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>false});"
                "window.chrome = window.chrome || {runtime: {}};"
            )
            page = await ctx.new_page()

            # Page 1
            page1_url = f"{ATRIUM_BASE}/search/jobs?page=1&q=&location="
            try:
                await page.goto(page1_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                logger.warning(f"Atrium: page 1 navigation failed ({e})")
                await browser.close(); return []

            # Wait for the page to fully render (Coveo hydrates client-side after the
            # raw HTML loads). The CoveoResultLink class signals jobs are present.
            cleared = False
            for _ in range(15):
                content = await page.content()
                if "CoveoResultLink" in content:
                    cleared = True; break
                if "Just a moment" in content or "Cloudflare" in content:
                    await asyncio.sleep(2)
                else:
                    await asyncio.sleep(1)

            if not cleared:
                logger.warning("Atrium Health: page 1 did not render Coveo results — Cloudflare blocked or page changed")
                await browser.close(); return []

            html1 = await page.content()
            p1_jobs, max_page = _parse_atrium_html(html1, 1)
            all_jobs.extend(p1_jobs)
            page_cap = min(max_page, 200)
            logger.info(f"  Atrium: page 1 → {len(p1_jobs)} jobs, sweeping through page {page_cap}")

            # Pages 2..N via in-page parallel fetch() — cleared session cookies
            # ride along automatically with credentials:'include'.
            BATCH = 6   # Atrium more sensitive to parallelism than HCA
            for batch_start in range(2, page_cap + 1, BATCH):
                batch_pages = list(range(batch_start, min(batch_start + BATCH, page_cap + 1)))
                try:
                    htmls = await page.evaluate(
                        """async (pageNums) => {
                            const fetchOne = async (p) => {
                                try {
                                    const r = await fetch(
                                        '/search/jobs?page=' + p + '&q=&location=',
                                        { credentials: 'include',
                                          headers: { 'Accept': 'text/html,application/xhtml+xml' } }
                                    );
                                    if (!r.ok) return '';
                                    return await r.text();
                                } catch (e) { return ''; }
                            };
                            return await Promise.all(pageNums.map(fetchOne));
                        }""",
                        batch_pages,
                    )
                    for p, html in zip(batch_pages, htmls):
                        if not html or len(html) < 5000: continue
                        pj, _ = _parse_atrium_html(html, p)
                        all_jobs.extend(pj)
                except Exception as e:
                    logger.info(f"Atrium: batch starting page {batch_start} failed: {e}")

                if (batch_start - 2) % (BATCH * 5) == 0:
                    logger.info(f"  Atrium: through page {min(batch_start + BATCH - 1, page_cap)}/{page_cap}, "
                                f"{len(all_jobs):,} jobs so far")
                await asyncio.sleep(0.4)

        finally:
            try: await browser.close()
            except Exception: pass

    # Dedupe within run on job_id
    seen, uniq = set(), []
    for j in all_jobs:
        if j.job_id in seen: continue
        seen.add(j.job_id); uniq.append(j)
    logger.info(f"  Atrium Health: {len(uniq):,} jobs")
    return uniq


# ══════════════════════════════════════════════════════════════════════════
#  TRAVEL JOBS  —  separate product, separate Supabase table (travel_jobs)
# ══════════════════════════════════════════════════════════════════════════
# Travel nursing jobs live in their own bucket. They're scraped from staffing
# agencies (Vivian Health is the largest aggregator; it republishes Aya, AMN,
# Trusted, and many smaller agencies). Schema mirrors hospital_jobs but
# `hospital_system` becomes `agency_name` and wages are weekly contract rates
# rather than salary ranges.
#
# Output: writes `travel_jobs_latest.json` AND, if SUPABASE_URL/_KEY are set
# in env, upserts directly to the `travel_jobs` table (keyed on
# `(agency_name, agency_job_id)`).

@dataclass
class TravelJob:
    agency_name:        str
    agency_job_id:      str
    title:              str
    specialty:          str | None      = None
    city:               str | None      = None
    state:              str | None      = None
    location:           str | None      = None
    weekly_pay_numeric: float | None    = None
    weekly_pay_display: str | None      = None
    hourly_rate_numeric: float | None   = None
    housing_stipend:    float | None    = None
    contract_weeks:     int | None      = None
    hours_per_week:     int | None      = None
    shift:              str | None      = None
    start_date:         str | None      = None
    hospital_facility:  str | None      = None
    description:        str | None      = None
    url:                str | None      = None
    posted_date:        str | None      = None


def _coerce_money(v) -> float | None:
    """Turn things like '$2,400' or 2400.0 or '2400' into a float; else None."""
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("$", "").replace(",", "").replace("/wk", "").replace("/week", "")
    s = re.sub(r"[^\d\.]", "", s)
    try: return float(s) if s else None
    except: return None


def _classify_travel_specialty(title: str | None, raw_specialty: str | None) -> str | None:
    """Canonical specialty for a travel contract.

    Rewritten 2026-07-30. It used to return the agency's own string untouched
    whenever one existed, which is why travel_jobs held 588 distinct specialty
    values ("PT Outpatient", "PT Inpatient Rehab", "PT SNF", "CVOR", "CVOR
    Technologist"…) and the board's specialty filter matched almost none of
    them. Same canonicaliser as the hospital side now, so a travel PT contract
    and a staff PT job land in the same bucket. Unmappable agency values are
    preserved, not nulled — measured 2026-07-30, this leaves travel at 0%
    uncategorised (unchanged) while collapsing 588 values to ~140.
    """
    return canonical_specialty(title, raw_specialty)


# ── Vivian Health ─────────────────────────────────────────────────────────
# Vivian's frontend hits Algolia DIRECTLY (the /api/self/* path is gated by
# session auth — the public Algolia search-only key is what unauthenticated
# browsers use). App ID and key extracted from the browser HAR; the API key
# is search-only, safe to embed.
VIVIAN_ALGOLIA_APP_ID  = "Q86HQHHJLB"
VIVIAN_ALGOLIA_API_KEY = "d303713dffdc1b685b8993b09665717d"  # rotated key, refreshed 2026-06-18 (old 1e4ad0… returned 403; app id unchanged)
VIVIAN_ALGOLIA_HOST    = f"https://{VIVIAN_ALGOLIA_APP_ID.lower()}-dsn.algolia.net"
VIVIAN_INDEX           = "searchable-jobs-prod"
VIVIAN_HITS_PER_PAGE   = 250          # Algolia hard cap
VIVIAN_MAX_PAGES       = 100          # 25,000 per employmentType
VIVIAN_EMPLOYMENT_TYPES = ["Travel", "Permanent", "Local Contract", "Per Diem / PRN"]

import urllib.parse as _vivian_urllib_parse  # used inside scrape_vivian_page

async def scrape_vivian_page(session: aiohttp.ClientSession,
                              employment_type: str, page: int) -> tuple[list[TravelJob], int, bool]:
    """Fetch one Vivian page from Algolia. Returns (jobs, total, has_more)."""
    inner_params = _vivian_urllib_parse.urlencode({
        "hitsPerPage":  VIVIAN_HITS_PER_PAGE,
        "page":         page,
        "filters":      'origin:"platform" OR origin:"vms" OR origin:"scraped"',
        "facetFilters": json.dumps([[f"employmentType:{employment_type}"]]),
    })
    body = {
        "requests": [{
            "indexName": VIVIAN_INDEX,
            "params":    inner_params,
        }]
    }
    url = (f"{VIVIAN_ALGOLIA_HOST}/1/indexes/*/queries"
           f"?x-algolia-api-key={VIVIAN_ALGOLIA_API_KEY}"
           f"&x-algolia-application-id={VIVIAN_ALGOLIA_APP_ID}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer":      "https://www.vivian.com/",
        "Origin":       "https://www.vivian.com",
    }
    try:
        # Note: `req()` does getattr(session, method) — aiohttp's method names
        # are lowercase ("post"), not uppercase, or it raises AttributeError.
        async with req(session, "post", url, json=body, headers=headers,
                       timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"Vivian {employment_type} page {page}: HTTP {r.status}")
                return [], 0, False
            data = await r.json(content_type=None)
    except Exception as e:
        logger.info(f"Vivian {employment_type} page {page}: {e}")
        return [], 0, False

    results = data.get("results") or []
    if not results or not isinstance(results, list):
        if page == 0:
            logger.info(f"Vivian {employment_type}: unexpected shape, keys={list(data.keys())[:8]}")
        return [], 0, False
    first = results[0] if isinstance(results[0], dict) else {}
    hits = first.get("hits") or []
    total = first.get("nbHits") or 0
    n_pages = first.get("nbPages") or 0

    out: list[TravelJob] = []
    for j in hits:
        try:
            jid = str(j.get("objectID") or "")
            if not jid: continue
            # Vivian's Algolia index sometimes returns several objectIDs for
            # the same underlying job, distinguished by a 32-char hex hash on
            # a stable parent ID (e.g. "scraped::9069_2413897-fbc9...").
            # Strip the hash so the (agency_name, agency_job_id) UNIQUE
            # constraint dedupes these phantom variants — otherwise we
            # accumulate hundreds of rows for one real job.
            stable_jid = re.sub(r"-[a-f0-9]{32}$", "", jid)
            # Title — Vivian doesn't expose `title`; nested under `titles.{simple|verbose|gpt}`
            titles = j.get("titles") or {}
            if isinstance(titles, dict):
                title = (titles.get("simple") or titles.get("verbose") or
                         titles.get("gpt") or titles.get("seo") or "")
            else:
                title = str(titles or "")
            agency = j.get("employerName") or j.get("agencyName") or "Vivian Listing"

            # Pay — `pay` is a nested dict with display.full + min/max + period
            pay = j.get("pay") or {}
            pay_min = pay.get("minRate")
            pay_max = pay.get("maxRate")
            pay_period = pay.get("period", "")  # "week", "hour", "year"
            pay_display_obj = pay.get("display") or {}
            pay_display = pay_display_obj.get("full") if isinstance(pay_display_obj, dict) else None

            wp_num = wp_display = hourly_num = None
            if pay_period == "week":
                wp_num = float(pay_max) if pay_max else (float(pay_min) if pay_min else None)
                wp_display = pay_display
            elif pay_period == "hour":
                hourly_num = float(pay_max) if pay_max else (float(pay_min) if pay_min else None)
                wp_display = pay_display  # display still useful even when hourly
            else:
                wp_display = pay_display

            # Contract length — string like "12 weeks"; pull the leading int
            cl = j.get("contractLengthWeeks")
            duration = None
            if isinstance(cl, (int, float)):
                duration = int(cl)
            elif isinstance(cl, str):
                m = re.match(r"(\d+)", cl)
                if m: duration = int(m.group(1))

            # Shift
            shift_val = j.get("shift")
            if isinstance(shift_val, list): shift_val = ", ".join(str(s) for s in shift_val)

            # Location — `location` is an array: [display, stateCode, stateName, "Compact States"]
            loc_arr = j.get("location") or []
            loc_display = j.get("locationDisplay") or (
                loc_arr[0] if isinstance(loc_arr, list) and loc_arr else "")
            state_code = ""
            if isinstance(loc_arr, list) and len(loc_arr) > 1:
                cand = loc_arr[1]
                if isinstance(cand, str) and len(cand) == 2 and cand.isalpha():
                    state_code = cand.upper()
            # Parse city from "Pittsburgh, Pennsylvania"
            city = ""
            if loc_display and "," in loc_display:
                city = loc_display.split(",", 1)[0].strip()

            # Specialty
            specialty_raw = None
            sn = j.get("specialtyNames")
            if isinstance(sn, list) and sn: specialty_raw = sn[0]
            elif isinstance(sn, str): specialty_raw = sn
            specialty = _classify_travel_specialty(title, specialty_raw)

            # Vivian serves canonical job pages at /jobs/<slug>/ (plural).
            # When jobDetailsSlug is missing, falling back to the raw
            # objectID produced URLs like /jobs/platform::jp-57754308/
            # which always 404 — so skip the row instead of shipping a
            # broken Apply link. (~65% of historical inserts were this
            # case; sampling showed 77% of those URLs returned 404.)
            slug = j.get("jobDetailsSlug")
            if not slug or "::" in slug:
                continue
            url_field = f"https://www.vivian.com/jobs/{slug}/"

            out.append(TravelJob(
                agency_name=agency,
                agency_job_id=f"vivian:{stable_jid}",
                title=title,
                specialty=specialty,
                city=city, state=state_code,
                location=loc_display,
                weekly_pay_numeric=wp_num,
                weekly_pay_display=wp_display,
                hourly_rate_numeric=hourly_num,
                housing_stipend=None,    # Vivian doesn't expose stipend separately in this index
                contract_weeks=duration,
                hours_per_week=None,
                shift=shift_val,
                start_date=j.get("startDateDisplay") or j.get("startMonth"),
                hospital_facility=j.get("facilityName"),
                description=strip_html(j.get("description") or j.get("searchDescription") or ""),
                url=url_field,
                posted_date=j.get("createdAtDisplay"),
            ))
        except Exception as e:
            logger.info(f"Vivian parse error: {e}")
            continue
    has_more = (page + 1) < n_pages and len(hits) >= VIVIAN_HITS_PER_PAGE
    return out, total, has_more


async def run_vivian(session: aiohttp.ClientSession) -> list[TravelJob]:
    logger.info("Vivian Health: starting scrape (travel jobs)")
    all_jobs: list[TravelJob] = []
    for employment_type in VIVIAN_EMPLOYMENT_TYPES:
        for page in range(0, VIVIAN_MAX_PAGES):
            jobs, total, has_more = await scrape_vivian_page(session, employment_type, page)
            all_jobs.extend(jobs)
            if page == 0:
                logger.info(f"  Vivian {employment_type}: total={total:,}, page 0: {len(jobs)} jobs")
            if not has_more or not jobs:
                logger.info(f"  Vivian {employment_type}: stopped at page {page+1} ({len(all_jobs):,} so far)")
                break
            await asyncio.sleep(0.3)
        else:
            logger.info(f"  Vivian {employment_type}: hit page cap {VIVIAN_MAX_PAGES}")
    # Dedupe within Vivian on agency_job_id
    seen, uniq = set(), []
    for j in all_jobs:
        if j.agency_job_id in seen: continue
        seen.add(j.agency_job_id); uniq.append(j)
    logger.info(f"Vivian Health: {len(uniq):,} unique travel listings")
    return uniq


# ── Aya Healthcare ────────────────────────────────────────────────────────
# Aya runs a clean public JSON API (api.ayahealthcare.com) that returns the
# same listings as their /healthcare-jobs/ page. No auth, no cookies, no
# CSRF. Cloudflare in front issues a __cf_bm soft cookie but doesn't
# challenge straight requests with a real UA + Origin header.
#
# Total travel inventory observed: ~7,900 jobs across all professions.
# Pagination is offset/limit; API accepts limit=250 (verified). 32 pages
# × 250 = full catalog in ~30 sec.
#
# employmentTypeCodes mapping from response.employmentTypeCount:
#   1 Permanent · 2 TravelOrContract · 3 PerDiem · 5/6 LocumTenens
# We pull code 2 (Travel) here. Permanent/PerDiem are deliberately skipped
# — those belong on the hospital side, not the travel page.
AYA_API_BASE = "https://api.ayahealthcare.com/AyaHealthcareWeb/job/search"
AYA_PAGE_SIZE = 250
AYA_MAX_PAGES = 50            # ceiling — current inventory needs ~32

async def scrape_aya_page(session: aiohttp.ClientSession, offset: int) -> tuple[list[TravelJob], int]:
    """Fetch one Aya page. Returns (jobs, total_count)."""
    params = {
        "employmentTypeCodes": "2",
        "includeRelatedSpecialties": "true",
        "useCityLatLong": "true",
        "limit":  str(AYA_PAGE_SIZE),
        "offset": str(offset),
    }
    headers = {
        "Origin":     "https://www.ayahealthcare.com",
        "Referer":    "https://www.ayahealthcare.com/healthcare-jobs/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept":     "application/json, text/plain, */*",
    }
    try:
        async with session.get(AYA_API_BASE, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                logger.info(f"Aya: HTTP {r.status} at offset {offset}")
                return [], 0
            data = await r.json()
    except Exception as e:
        logger.info(f"Aya: page error at offset {offset}: {e}")
        return [], 0

    items = data.get("items") or []
    total = int(data.get("count") or 0)
    out: list[TravelJob] = []
    for j in items:
        try:
            jid = j.get("jobID")
            if jid is None: continue
            # Pay: prefer weeklyPayLow/High; fall back to regularPayLow/High;
            # then to alternatePayLow/High; then parse payRate.value.
            wp_low  = _coerce_money(j.get("weeklyPayLow"))  or _coerce_money(j.get("regularPayLow"))  or _coerce_money(j.get("alternatePayLow"))
            wp_high = _coerce_money(j.get("weeklyPayHigh")) or _coerce_money(j.get("regularPayHigh")) or _coerce_money(j.get("alternatePayHigh"))
            pay_display = None
            pr = j.get("payRate") or {}
            if isinstance(pr, dict) and pr.get("value"):
                pay_display = str(pr["value"]).strip()
            elif wp_low and wp_high:
                pay_display = f"${wp_low:,.0f}–${wp_high:,.0f}/wk"
            elif wp_low:
                pay_display = f"${wp_low:,.0f}/wk"

            city = (j.get("city") or "").strip() or None
            st   = (j.get("stateAbbrev") or "").strip() or None
            location = ", ".join(p for p in (city, st) if p) or None

            prof = (j.get("professionText") or "").strip()
            expert = (j.get("expertiseText") or "").strip()
            # Aya's profession/expertise often duplicate for non-RN roles
            # (e.g. Perfusionist - Perfusionist). Dedup case-insensitively.
            if prof and expert and prof.lower() == expert.lower():
                title = prof
            else:
                title = " - ".join(p for p in (prof, expert) if p) or "Travel Contract"

            # contract_weeks: `duration` is in weeks per Aya's UI
            cw = j.get("duration")
            try: cw = int(cw) if cw is not None else None
            except: cw = None

            # hours_per_week: shifts (per week) × hours (per shift)
            hpw = None
            try:
                shifts = j.get("shifts")
                hours  = j.get("hours")
                if shifts is not None and hours is not None:
                    hpw = int(round(float(shifts) * float(hours)))
            except: pass

            shift_text = j.get("shiftText") or j.get("longShift") or None
            start_disp = j.get("startDateDisplay") or j.get("startDate")

            out.append(TravelJob(
                agency_name        = "Aya Healthcare",
                agency_job_id      = str(jid),
                title              = title,
                specialty          = _classify_travel_specialty(title, expert) or expert or None,
                city               = city,
                state              = st,
                location           = location,
                weekly_pay_numeric = wp_low,           # match Vivian convention: low end is the sortable number
                weekly_pay_display = pay_display,
                hourly_rate_numeric= None,
                housing_stipend    = None,
                contract_weeks     = cw,
                hours_per_week     = hpw,
                shift              = shift_text,
                start_date         = start_disp,
                hospital_facility  = (j.get("facilityName") or "").strip() or None,
                description        = None,
                url                = f"https://www.ayahealthcare.com/travel-nursing-job/{jid}",
                posted_date        = j.get("posted") or j.get("enteredTime"),
            ))
        except Exception as e:
            logger.info(f"Aya parse error: {e}")
            continue
    return out, total


async def run_aya(session: aiohttp.ClientSession) -> list[TravelJob]:
    logger.info("Aya Healthcare: starting scrape (travel jobs)")
    all_jobs: list[TravelJob] = []
    total = 0
    for page in range(0, AYA_MAX_PAGES):
        offset = page * AYA_PAGE_SIZE
        jobs, total = await scrape_aya_page(session, offset)
        if page == 0:
            logger.info(f"  Aya Healthcare: total={total:,}, page 0: {len(jobs)} jobs")
        if not jobs:
            logger.info(f"  Aya Healthcare: stopped at page {page+1} ({len(all_jobs):,} so far)")
            break
        all_jobs.extend(jobs)
        if (page + 1) * AYA_PAGE_SIZE >= total:
            logger.info(f"  Aya Healthcare: reached total at page {page+1} ({len(all_jobs):,} jobs)")
            break
        await asyncio.sleep(0.25)         # well under the 2000/30s rate limit
    else:
        logger.info(f"  Aya Healthcare: hit page cap {AYA_MAX_PAGES}")
    # Dedupe within Aya on agency_job_id
    seen, uniq = set(), []
    for j in all_jobs:
        if j.agency_job_id in seen: continue
        seen.add(j.agency_job_id); uniq.append(j)
    logger.info(f"Aya Healthcare: {len(uniq):,} unique travel listings")

    # Optional detail pass — no-op unless AYA_FETCH_DESCRIPTIONS=1.
    if AYA_FETCH_DESCRIPTIONS and uniq:
        try:
            await _aya_fetch_details(session, uniq)
        except Exception as e:
            logger.info(f"Aya: detail pass failed ({e}) — keeping list data")
    return uniq


async def _aya_fetch_details(session: aiohttp.ClientSession, jobs: list) -> None:
    """Fill descriptions from Aya's per-job JSON endpoint (added 2026-08-04).

    The search API returns NO description text (probed: `details` is null on
    every item), but GET api.ayahealthcare.com/AyaHealthcareWeb/job/{id} is a
    public ~6KB JSON document whose `jobDescription` runs 1-4k chars of real
    per-job text — measured 3,788 chars on the probe job. 6KB x 10.8k jobs is
    ~65MB for a full backfill, so this is cheap even through proxies (the
    page-scrape alternative was 181KB per job).

    Same containment contract as the Workday pass: flag-gated, budgeted per
    run via AYA_DESC_BUDGET, failures leave the job exactly as the list
    returned it, and only text clearing the 200-char sitemap bar is kept.
    """
    headers = {
        "Origin":     "https://www.ayahealthcare.com",
        "Referer":    "https://www.ayahealthcare.com/healthcare-jobs/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept":     "application/json, text/plain, */*",
    }
    candidates = [j for j in jobs if not (j.description or "").strip() and j.agency_job_id]
    # Shuffle for the same reason as the Workday pass: the DB-side trigger
    # preserves previously fetched descriptions, so a random slice per night
    # accumulates coverage instead of re-fetching the same 500 forever.
    random.shuffle(candidates)
    allowed = AYA_DESC_BUDGET.take(len(candidates))
    pending = candidates[:allowed]
    if not pending:
        return

    sem = asyncio.Semaphore(AYA_DESC_CONCURRENCY)
    filled = 0

    async def one(job):
        nonlocal filled
        url = f"https://api.ayahealthcare.com/AyaHealthcareWeb/job/{job.agency_job_id}"
        async with sem:
            try:
                async with req(session, "get", url, headers=headers, ssl=False,
                               proxy=proxies.get(),
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        return
                    data = await r.json(content_type=None)
            except Exception:
                return
            desc = strip_html(str((data or {}).get("jobDescription") or ""))
            if len(desc) >= 200:
                job.description = desc
                filled += 1
            await asyncio.sleep(random.uniform(0.15, 0.4))

    await asyncio.gather(*[one(j) for j in pending], return_exceptions=True)
    logger.info(f"  Aya: details {len(pending)} fetched -> {filled} descriptions")


# ══════════════════════════════════════════════════════════════════════════
#  NOMAD HEALTH — public Elasticsearch JSON API (added 2026-07-29)
#
#  nomadhealth.com's own UI XHR (/api/jobposts/jobpost_search/) 401s anon
#  callers, but the sibling GET /api/v1/jobposts/search/ is fully public (no
#  auth/cookie/CSRF). It defaults to discipline=nurse, so all 10 disciplines
#  must be iterated to get the whole board (~16.7k travel/contract jobs; every
#  listing is travel). per_page is snake_case (camelCase silently caps at 10).
#  Behind Cloudflare -> curl_cffi via _curl_fetch (webshare-first). Job-page
#  URLs are long SEO slugs, so we join the trailing job CODE against the job
#  sitemap once for real apply links.
# ══════════════════════════════════════════════════════════════════════════

NOMAD_SEARCH = "https://nomadhealth.com/api/v1/jobposts/search/"
NOMAD_DISCIPLINES = [
    "nurse", "cath_lab_technologist", "lab_technician", "occupational_therapist",
    "physical_therapist", "radiology_technologist", "respiratory_therapist",
    "ultrasound_technologist", "speech_language_pathologist", "surgical_technologist",
]


def _nomad_url_map() -> dict:
    """Build {job_code: canonical_url} from Nomad's job sitemap chunks (~18 of
    1,000 URLs each). One-time cost; gives real SEO apply links instead of the
    non-reconstructable slug. Empty dict on failure (adapter falls back to the
    detail-API URL, which still carries the nomadhealth.com domain)."""
    out = {}
    for i in range(0, 30):  # 18 observed; stop on first missing/empty chunk
        try:
            r = _curl_fetch("get", f"https://nomadhealth.com/sitemap-jobs-chunk/{i}.xml",
                            "chrome", timeout=60)
        except Exception:
            break
        locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
        if not locs:
            break
        for u in locs:
            code = u.rstrip("/").rsplit("/", 1)[-1]
            if code:
                out[code] = u
    return out


def _nomad_map(rec: dict, url_map: dict) -> Optional[TravelJob]:
    jid = rec.get("id")
    title = (rec.get("title") or "").strip()
    if not jid or not title:
        return None
    agency = rec.get("agency") or {}
    fac = rec.get("facility") or {}
    city = fac.get("city")
    state = fac.get("state")
    specs = rec.get("specializations") or []
    specialty = ", ".join(s.get("name", "") for s in specs if s.get("name")) or None
    wpay = rec.get("weekly_gross_compensation")
    m = re.search(r"\$[\d,]+/wk", title)
    wp_display = m.group(0) if m else (f"${wpay:,.0f}/wk" if wpay else None)
    hourly = rec.get("pay_rate") if rec.get("pay_rate_period") == "hour" else None
    hpw = None
    mm = re.match(r"(\d+)x(\d+)", rec.get("shift_hours_and_days") or "")
    if mm:
        hpw = int(mm.group(1)) * int(mm.group(2))
    shift_types = rec.get("shift_types") or []
    shift = ", ".join(shift_types) if shift_types else (rec.get("shift_hours_and_days") or None)
    code = rec.get("code")
    url = (url_map.get(code)
           or (f"https://nomadhealth.com/api/jobposts/code/{code}" if code else "https://nomadhealth.com/jobs"))
    return TravelJob(
        agency_name=agency.get("name") or "Nomad Health",
        agency_job_id=f"nomad-{jid}",
        title=title,
        specialty=specialty,
        city=city, state=state,
        location=f"{city}, {state}" if city and state else (city or state),
        weekly_pay_numeric=float(wpay) if wpay else None,
        weekly_pay_display=wp_display,
        hourly_rate_numeric=float(hourly) if hourly else None,
        housing_stipend=None,
        contract_weeks=rec.get("contract_length"),
        hours_per_week=hpw,
        shift=shift,
        start_date=rec.get("start_date"),
        hospital_facility=fac.get("name"),
        description=None,
        url=url,
        posted_date=rec.get("date_last_published"),
    )


def _nomad_fetch_all() -> list[TravelJob]:
    """Blocking full pull — runs in a worker thread. Webshare-first via _curl_fetch."""
    url_map = _nomad_url_map()
    logger.info(f"  Nomad Health: sitemap URL map has {len(url_map):,} codes")
    seen: dict = {}
    for disc in NOMAD_DISCIPLINES:
        page, errors = 1, 0
        while page <= 80:  # per-discipline guard (nurse ~13 pages at per_page=1000)
            try:
                r = _curl_fetch("get", NOMAD_SEARCH, "chrome", timeout=90,
                                params={"discipline": disc, "per_page": 1000, "page": page},
                                headers={"Accept": "application/json"})
                data = r.json()
            except Exception as e:
                errors += 1
                if errors >= 3:
                    logger.info(f"  Nomad {disc}: page {page} failed 3x ({e}) — moving on")
                    break
                time.sleep(2 * errors)
                continue
            errors = 0
            posts = data.get("jobposts") or []
            for rec in posts:
                rid = rec.get("id")
                if rid:
                    seen[rid] = rec
            pag = data.get("pagination") or {}
            if not posts or not pag.get("has_next"):
                break
            page += 1
            time.sleep(0.3)
    out: list[TravelJob] = []
    for rec in seen.values():
        tj = _nomad_map(rec, url_map)
        if tj:
            out.append(tj)
    return out


async def run_nomad() -> list[TravelJob]:
    if curl_requests is None:
        logger.warning("Nomad Health: curl_cffi not installed — skipping")
        return []
    logger.info("Nomad Health: public JSON API, 10 disciplines (curl_cffi)...")
    try:
        jobs = await asyncio.to_thread(_nomad_fetch_all)
    except Exception as e:
        logger.info(f"  Nomad Health: ERROR {e}")
        return []
    logger.info(f"  Nomad Health: {len(jobs):,} travel listings")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  AMN HEALTHCARE — public "ONE" Azure JSON API (added 2026-07-29)
#
#  The largest US travel staffing agency. Custom "ONE" search SPA backed by a
#  fully anonymous Azure APIM JSON API (no auth/key/cookie). GET /JobSearch
#  with Filters=JobType:Travel returns ~10.8k travel nursing+allied jobs.
#  pageSize hard-caps at 100. Not Cloudflare/fingerprint-gated, but routed
#  through _curl_fetch anyway to honor webshare-first. Real hospital name is
#  withheld (organization.name always blank) -> facilityType category used.
#  Apply URL is built from the SPA's own GetJobSlug() algorithm (verified 200).
# ══════════════════════════════════════════════════════════════════════════

AMN_API = "https://api.amnhealthcare.io/ONEAmnJobSearch/v1/JobSearch"


def _amn_slug(j: dict) -> str:
    ds = j.get("disciplineSpecialty") or {}
    disc = ds.get("disciplineName") or ""
    spec = ds.get("specialtyName") or ""
    s = disc + ("-" if spec else "") + spec
    s = s.replace(" - ", "-").replace(" ", "-").replace("/", "-").replace("&", "")
    jid = j.get("jobID")
    city = (j.get("city") or {}).get("name")
    st = (j.get("state") or {}).get("abbrev")
    if city and st:
        tail = f"{jid}/{city.replace(' - ', '-').replace(' ', '-').replace('&', '')}-{st}-{s}/"
    elif st:
        tail = f"{jid}/{st}-{s}/"
    else:
        tail = f"{jid}/{s}/"
    return ("https://www.amnhealthcare.com/job-details/" + tail).lower()


def _amn_map(j: dict) -> Optional[TravelJob]:
    jid = j.get("jobID")
    title = (j.get("jobTitle") or "").strip()
    if not jid or not title:
        return None
    pr = j.get("payRate") or {}
    ds = j.get("disciplineSpecialty") or {}
    org = j.get("organization") or {}
    city = (j.get("city") or {}).get("name")
    st = (j.get("state") or {}).get("abbrev")
    mn, mx = pr.get("minPayRate"), pr.get("maxPayRate")
    weekly = float(mx or mn) if (pr.get("payRateType") == "Weekly" and (mx or mn)) else None
    display = f"${mn}-${mx}/{pr.get('payRateTypeAbbrev')}" if (mn or mx) else None
    return TravelJob(
        agency_name="AMN Healthcare",
        agency_job_id=f"amn-{jid}",
        title=title,
        specialty=ds.get("specialtyName") or ds.get("disciplineName"),
        city=city, state=st,
        location=f"{city}, {st}" if city and st else (city or st),
        weekly_pay_numeric=weekly,
        weekly_pay_display=display,
        hourly_rate_numeric=None,
        housing_stipend=None,
        contract_weeks=j.get("durationInt"),
        hours_per_week=j.get("hoursPerWeek"),
        shift=j.get("shift"),
        start_date=j.get("startDate"),
        hospital_facility=(org.get("name") or j.get("facilityType")),
        description=j.get("descriptionLong"),
        url=_amn_slug(j),
        posted_date=j.get("datePosted"),
    )


def _amn_fetch_all() -> list[TravelJob]:
    """Blocking full pull (~109 pages at pageSize=100) — runs in a worker thread."""
    out: list[TravelJob] = []
    seen: set = set()
    page, errors = 1, 0
    while page <= 300:  # ~109 expected; guard against runaway
        try:
            r = _curl_fetch("get", AMN_API, "chrome", timeout=60,
                            params={"pageNumber": page, "pageSize": 100,
                                    "sortOrder": "relevance", "Filters": "JobType:Travel"},
                            headers={"Accept": "application/json"})
            jobs = (r.json() or {}).get("jobs") or []
        except Exception as e:
            errors += 1
            if errors >= 3:
                logger.info(f"  AMN: page {page} failed 3x ({e}) — stopping at {len(out)}")
                break
            time.sleep(2 * errors)
            continue
        errors = 0
        if not jobs:
            break
        for j in jobs:
            jid = j.get("jobID")
            if jid and jid not in seen:
                seen.add(jid)
                tj = _amn_map(j)
                if tj:
                    out.append(tj)
        page += 1
        time.sleep(0.3)
    return out


async def run_amn() -> list[TravelJob]:
    if curl_requests is None:
        logger.warning("AMN Healthcare: curl_cffi not installed — skipping")
        return []
    logger.info("AMN Healthcare: public ONE Azure JSON API (curl_cffi)...")
    try:
        jobs = await asyncio.to_thread(_amn_fetch_all)
    except Exception as e:
        logger.info(f"  AMN Healthcare: ERROR {e}")
        return []
    logger.info(f"  AMN Healthcare: {len(jobs):,} travel listings")
    return jobs


# ── Travel jobs runner + Supabase upsert ──────────────────────────────────
async def run_all_travel() -> list[dict]:
    start = datetime.now()
    proxy_connector = aiohttp.TCPConnector(limit=20, ssl=False)
    direct_connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=proxy_connector, headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as proxy_session, \
               aiohttp.ClientSession(connector=direct_connector, headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as direct_session:
        results = await asyncio.gather(
            run_vivian(direct_session),    # Vivian doesn't need proxy; Cloudflare on their end is mild
            run_aya(direct_session),       # Aya — clean public JSON, no proxy needed
            run_nomad(),                   # Nomad Health — public JSON API via _curl_fetch (webshare-first)
            run_amn(),                     # AMN Healthcare — public Azure JSON API via _curl_fetch (webshare-first)
            return_exceptions=True,
        )
    all_travel: list[TravelJob] = []
    for r in results:
        if isinstance(r, list):
            all_travel.extend(r)
    # Convert to dicts. We stamp scraped_at to the current run's start time on
    # every row so that the PostgREST upsert refreshes it on conflict — that
    # gives us a reliable "last seen" signal for the post-upsert deactivation
    # pass below.
    run_started_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    rows = []
    for j in all_travel:
        d = asdict(j)
        d["is_active"] = True
        d["scraped_at"] = run_started_at_iso
        # Same city sanitation as the hospital pipeline: extract the real city
        # from embedded facility/address junk, blank it when unrecoverable.
        if d.get("city"):
            d["city"] = clean_city(d["city"])
        rows.append(d)
    elapsed = (datetime.now() - start).seconds
    logger.info("=" * 55)
    logger.info(f"  TRAVEL JOBS:        {len(rows):,}")
    logger.info(f"  AGENCIES:           {len({r['agency_name'] for r in rows if r.get('agency_name')})}")
    logger.info(f"  RUNTIME (travel):   {elapsed}s")
    logger.info("=" * 55)
    return rows


def _upsert_travel_jobs_to_supabase(rows: list[dict]) -> int:
    """Upsert travel rows into Supabase. Returns count upserted, or 0 on failure
    or if env vars are missing. Falls back to JSON dump in either case.

    After a successful upsert, performs a deactivation pass: any row whose
    url matches a domain we scraped this run AND whose scraped_at is older
    than this run's start gets is_active=false. That keeps the table from
    growing unboundedly with listings the source has removed."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not sb_url or not sb_key:
        logger.info("Travel upsert: SUPABASE_URL/SUPABASE_KEY not set — JSON dump only")
        return 0
    if not rows:
        logger.info("Travel upsert: no rows to send")
        return 0
    import urllib.request as _urlreq, urllib.error as _urlerr
    url = f"{sb_url.rstrip('/')}/rest/v1/travel_jobs?on_conflict=agency_name,agency_job_id"
    headers = {
        "apikey":        sb_key,
        "Authorization": f"Bearer {sb_key}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }
    # Batch loop rewritten 2026-08-06. The old version BROKE on the first
    # failed batch: on 2026-08-05 batch 8 of ~120 errored, only 3,500 rows
    # got fresh scraped_at, and the deactivation sweep below then wiped the
    # 63,631 rows whose upsert never ran. Now each batch retries 3x with
    # backoff; a permanently-failed batch is skipped (not fatal) and, most
    # importantly, ANY permanent failure disables this run's sweep.
    BATCH = 500
    sent = 0
    failed_batches = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        body = json.dumps(chunk).encode()
        ok = False
        for attempt in range(3):
            rq = _urlreq.Request(url, data=body, headers=headers, method="POST")
            try:
                with _urlreq.urlopen(rq, timeout=60) as resp:
                    _ = resp.read()
                ok = True
                break
            except _urlerr.HTTPError as e:
                err_body = e.read().decode()[:500]
                logger.warning(f"Travel upsert batch {i} attempt {attempt + 1}: HTTP {e.code} — {err_body}")
            except Exception as e:
                logger.warning(f"Travel upsert batch {i} attempt {attempt + 1}: {e}")
            time.sleep(5 * (attempt + 1))
        if ok:
            sent += len(chunk)
        else:
            failed_batches += 1
    logger.info(f"Travel upsert: {sent}/{len(rows)} rows sent to Supabase ({failed_batches} batches permanently failed)")

    # ── Deactivation pass (rewritten 2026-07-20) ───────────────────────
    # The previous implementation issued ONE global PATCH over every active
    # row older than this run. At ~170k active rows that statement blew the
    # 8s statement timeout, the HTTPError was logged-and-swallowed, and stale
    # rows snowballed (121k rows >30 days stale by the time this was caught —
    # the /travel board's broken links). Now: per-source, id-WINDOWED patches
    # — every statement is a small PK-range update that cannot time out.
    # A source is swept only when it produced >= DEACT_MIN rows this run, so
    # a partial outage can't wipe a healthy source's inventory (previously a
    # single short source skipped the sweep for ALL sources — the other half
    # of the snowball).
    # Each source is swept independently, matched by its url domain. New
    # sources MUST be listed here or their removed listings never deactivate
    # (the 2026-07-20 travel-purge bug). nomadhealth.com + amnhealthcare.com
    # added 2026-07-29.
    KNOWN_DOMAINS = ("vivian.com", "ayahealthcare.com", "nomadhealth.com", "amnhealthcare.com")
    DEACT_MIN     = 100
    WINDOW        = 5000
    if sent == 0:
        return sent
    # HARD GUARD (2026-08-06): a partial upsert must NEVER trigger the sweep.
    # Rows whose batch failed keep their old scraped_at, so sweeping now would
    # deactivate live listings wholesale (the 2026-08-05 incident: 63,631
    # wrongly-deactivated rows). One extra day of stale rows is harmless —
    # the URL validator still runs — so skip and let tomorrow's run true up.
    if failed_batches > 0:
        logger.warning(
            f"Travel deactivate: SKIPPING sweep — {failed_batches} upsert batch(es) "
            f"permanently failed ({sent}/{len(rows)} rows landed). Sweeping after a "
            f"partial upsert would deactivate live listings."
        )
        update_travel_site_stats()
        return sent
    run_started_iso = min(
        (r.get("scraped_at") for r in rows if r.get("scraped_at")),
        default=None,
    )
    if not run_started_iso:
        logger.info("Travel deactivate: no scraped_at on rows; skipping")
        return sent
    domain_counts: dict[str, int] = {d: 0 for d in KNOWN_DOMAINS}
    for r in rows:
        u = (r.get("url") or "").lower()
        for d in KNOWN_DOMAINS:
            if d in u:
                domain_counts[d] += 1
                break
    sweep = [d for d, n in domain_counts.items() if n >= DEACT_MIN]
    short = [d for d, n in domain_counts.items() if n < DEACT_MIN]
    if short:
        logger.warning(
            f"Travel deactivate: NOT sweeping {short} — below DEACT_MIN={DEACT_MIN} "
            f"(counts={domain_counts}); healthy sources still sweep"
        )
    from urllib.parse import quote as _q
    total_deact = 0
    failed_windows = 0
    if sweep:
        # max id for windowing (PK-indexed, fast)
        try:
            rq = _urlreq.Request(
                f"{sb_url.rstrip('/')}/rest/v1/travel_jobs?select=id&order=id.desc&limit=1",
                headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
            with _urlreq.urlopen(rq, timeout=30) as resp:
                _maxrows = json.loads(resp.read())
            max_id = _maxrows[0]["id"] if _maxrows else 0
        except Exception as e:
            logger.warning(f"Travel deactivate: max-id lookup failed ({e}); skipping sweep")
            max_id = 0
        body = json.dumps({"is_active": False}).encode()
        patch_headers = {
            "apikey":        sb_key,
            "Authorization": f"Bearer {sb_key}",
            "Content-Type":  "application/json",
            "Prefer":        "return=minimal,count=exact",
        }
        for domain in sweep:
            for lo in range(0, max_id + WINDOW, WINDOW):
                purl = (
                    f"{sb_url.rstrip('/')}/rest/v1/travel_jobs?is_active=eq.true"
                    f"&scraped_at=lt.{_q(run_started_iso)}"
                    f"&url=like.{_q('*' + domain + '*')}"
                    f"&id=gte.{lo}&id=lt.{lo + WINDOW}"
                )
                try:
                    rq = _urlreq.Request(purl, data=body, headers=patch_headers, method="PATCH")
                    with _urlreq.urlopen(rq, timeout=60) as resp:
                        cr = resp.headers.get("Content-Range", "")
                        n = cr.split("/")[-1]
                        if n.isdigit():
                            total_deact += int(n)
                except Exception as e:
                    failed_windows += 1
                    if failed_windows <= 3:
                        logger.warning(f"Travel deactivate window {domain} id>={lo}: {e}")
        logger.info(
            f"Travel deactivate: {total_deact:,} rows swept across {len(sweep)} source(s) "
            f"({failed_windows} failed windows, counts={domain_counts})"
        )

    # Travel count → site_stats id=2. Written again at the very end of the
    # scheduler run (after the URL validator deactivates dead links) so the
    # homepage shows the final number, not a mid-run one.
    update_travel_site_stats()
    return sent


def update_travel_site_stats() -> int:
    """Write the live travel-jobs count to site_stats id=2. Returns the count.

    The homepage's "Open Roles" figure is site_stats id=1 (hospital) + id=2
    (travel), so if this write fails the site silently advertises a stale
    number.

    Rewritten 2026-07-29 — it HAD been failing silently. The old version
    counted by paging every active row 1,000 at a time; that was ~26 requests
    when it shipped, but travel inventory doubled to 52k (Nomad + AMN) and the
    loop became ~53 sequential round-trips, long enough to fail and get
    swallowed by the surrounding try/except. Result: id=2 froze at 26,444 from
    2026-07-27 while the table actually held 52,641 — the homepage was
    understating travel inventory by half. A single head request with
    count=exact does the same job in one round-trip.
    """
    import urllib.request as _urlreq
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = (os.environ.get("SUPABASE_KEY", "")
              or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
    stats_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "") or sb_key
    if not sb_url or not sb_key:
        logger.info("Travel site_stats: SUPABASE_URL/KEY not set — skipping")
        return 0
    def _head_count(extra: str = "") -> Optional[int]:
        """One count=exact head request. None on failure."""
        try:
            rq = _urlreq.Request(
                f"{sb_url.rstrip('/')}/rest/v1/travel_jobs"
                f"?select=id&is_active=eq.true{extra}&limit=1",
                headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}",
                         "Prefer": "count=exact", "Range": "0-0"})
            with _urlreq.urlopen(rq, timeout=90) as resp:
                cr = resp.headers.get("Content-Range", "")
                resp.read()
            tail = cr.split("/")[-1] if "/" in cr else ""
            return int(tail) if tail.isdigit() else None
        except Exception:
            return None

    # Fast path: whole-table exact count, retried. It usually succeeds in well
    # under a second, but it is NOT reliable on its own — travel_jobs is ~1.95M
    # rows with no index on is_active, and the same query was observed
    # succeeding and then 500ing (57014 statement timeout) minutes apart on
    # 2026-07-29. A single un-retried attempt is precisely how this counter
    # went stale for two days in the first place.
    travel_total = None
    for attempt in range(4):
        travel_total = _head_count()
        if travel_total is not None:
            break
        time.sleep(2 * (attempt + 1))

    # Slow path: count in id windows. Each window is a small indexed range that
    # cannot hit the statement timeout. ~79 requests / ~77s at current size —
    # slow, but it always returns a number, and a correct slow count beats a
    # homepage that silently advertises half the inventory.
    if travel_total is None:
        logger.info("Travel site_stats: exact count kept timing out — falling back to id windows")
        try:
            rq = _urlreq.Request(
                f"{sb_url.rstrip('/')}/rest/v1/travel_jobs?select=id&order=id.desc&limit=1",
                headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
            with _urlreq.urlopen(rq, timeout=30) as resp:
                rows = json.loads(resp.read())
            max_id = rows[0]["id"] if rows else 0
        except Exception as e:
            logger.warning(f"Travel site_stats: max-id lookup failed ({e}) — not writing")
            return 0
        WINDOW = 25000
        total, failed, lo = 0, 0, 0
        while lo <= max_id:
            n = _head_count(f"&id=gte.{lo}&id=lt.{lo + WINDOW}")
            if n is None:
                failed += 1
            else:
                total += n
            lo += WINDOW
        if failed:
            # A partial sum would UNDERSTATE the count — the exact failure mode
            # we're fixing. Refuse rather than write a number we know is short.
            logger.warning(f"Travel site_stats: {failed} windows failed — not writing a partial count")
            return 0
        travel_total = total

    if travel_total is None:
        logger.warning("Travel site_stats: could not determine count — not writing")
        return 0

    try:
        stats_body = json.dumps({
            "id": 2,
            "total_active_jobs": travel_total,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).encode()
        rq = _urlreq.Request(
            f"{sb_url.rstrip('/')}/rest/v1/site_stats?on_conflict=id",
            data=stats_body,
            headers={"apikey": stats_key, "Authorization": f"Bearer {stats_key}",
                     "Content-Type": "application/json",
                     "Prefer": "resolution=merge-duplicates,return=minimal"},
            method="POST")
        with _urlreq.urlopen(rq, timeout=30) as resp:
            resp.read()
        logger.info(f"site_stats id=2 (travel) updated: {travel_total:,} active")
        _refresh_travel_sitemap_cohort(sb_url, stats_key)
        return travel_total
    except Exception as e:
        logger.warning(f"Travel site_stats write failed (non-fatal): {e}")
        return 0


def flag_signon_jobs() -> int:
    """Set has_signon=true on active hospital_jobs whose title or description
    mentions a sign-on / signing bonus (2026-08-08, Robert — feeds the solid
    yellow bonus pill on the job cards).

    Runs nightly as scheduler Step 2d so rows whose description only arrives
    later via the detail-fetch budget still get flagged. Title matches on a
    bare "sign-on"; descriptions require bonus/incentive proximity so ATS
    boilerplate like "sign on to your account" doesn't false-positive.
    Id-windowed PATCHes (the travel-deactivate pattern) so no statement can
    time out. Never un-flags: the enrichment trigger means a description can
    only gain mentions, and a title mention is stable for the row's life."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = os.environ.get("SUPABASE_KEY", "") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not sb_url or not sb_key:
        logger.info("flag_signon: SUPABASE_URL/SUPABASE_KEY not set; skipping")
        return 0
    import urllib.request as _urlreq
    from urllib.parse import quote as _q
    try:
        rq = _urlreq.Request(
            f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id&order=id.desc&limit=1",
            headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
        with _urlreq.urlopen(rq, timeout=30) as resp:
            rows = json.loads(resp.read())
        max_id = rows[0]["id"] if rows else 0
    except Exception as e:
        logger.warning(f"flag_signon: max-id lookup failed ({e}); skipping")
        return 0
    # NO parentheses in these regexes: PostgREST's or=() parser consumes a
    # decoded ")" as the group terminator, Postgres then gets an unbalanced
    # regex and every window 400s (2026-08-21 run: 3,962 failed windows,
    # error 2201B "parentheses () not balanced"). Values are additionally
    # double-quoted inside or=() per PostgREST quoting rules.
    TITLE_RX = "sign[- ]?on|signing bonus"
    DESC_RX = "sign[- ]?on bonus|sign[- ]?on incentive|signing bonus"
    body = json.dumps({"has_signon": True}).encode()
    patch_headers = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}",
                     "Content-Type": "application/json",
                     "Prefer": "return=minimal,count=exact"}
    WINDOW = 5000
    total = 0
    failed = 0
    for lo in range(0, max_id + WINDOW, WINDOW):
        purl = (
            f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?is_active=eq.true"
            f"&has_signon=eq.false"
            f"&or=(title.imatch.%22{_q(TITLE_RX)}%22,description.imatch.%22{_q(DESC_RX)}%22)"
            f"&id=gte.{lo}&id=lt.{lo + WINDOW}"
        )
        try:
            rq = _urlreq.Request(purl, data=body, headers=patch_headers, method="PATCH")
            with _urlreq.urlopen(rq, timeout=60) as resp:
                cr = resp.headers.get("Content-Range", "")
                n = cr.split("/")[-1]
                if n.isdigit():
                    total += int(n)
        except Exception as e:
            failed += 1
            if failed <= 3:
                logger.warning(f"flag_signon window id>={lo}: {e}")
    logger.info(f"flag_signon: {total:,} rows newly flagged ({failed} failed windows)")
    return total


def _refresh_travel_sitemap_cohort(sb_url: str, sb_key: str) -> None:
    """Rebuild the travel_sitemap_cohort snapshot table (added 2026-08-04).

    app/sitemap.js tier 4 on the website reads the travel quality cohort
    (active + description >= 200 chars) from this slim table instead of
    filtering travel_jobs live: the live char_length() filter cannot survive
    the 8s statement timeout while a Vercel build hammers the DB with ~185
    concurrent page renders. The snapshot only has to be as fresh as the data,
    and the data only changes when THIS scraper runs — so refreshing it here,
    right after site_stats, keeps the sitemap exactly one scrape behind
    reality, same as everything else on the site.

    Non-fatal by design: a failed refresh leaves yesterday's snapshot, which
    is a slightly stale sitemap — strictly better than a missing one.
    """
    import urllib.request as _urlreq
    for attempt in range(4):
        try:
            rq = _urlreq.Request(
                f"{sb_url.rstrip('/')}/rest/v1/rpc/refresh_travel_sitemap_cohort",
                data=b"{}",
                headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}",
                         "Content-Type": "application/json"},
                method="POST")
            with _urlreq.urlopen(rq, timeout=150) as resp:
                n = resp.read().decode().strip()
            logger.info(f"travel_sitemap_cohort refreshed: {n} rows")
            return
        except Exception as e:
            if attempt == 3:
                logger.warning(f"travel_sitemap_cohort refresh failed after 4 tries (non-fatal): {e}")
            else:
                time.sleep(5 * (attempt + 1))


# ── Apply-link QA guardrails (added 2026-07-01) ───────────────────────────
# Root-caused three "job exists but the apply link is broken" bugs — Workday
# /job//job/ (Sign-In page), Oracle /jobs/{id} (Page-not-found), AdventHealth
# /job/R- (404) — that together broke ~61% of apply links while every job still
# looked fine (the scraper only checked "did we get jobs", never "does the link
# resolve"). Three guardrails so a template break can't silently ship again:
#   _sanitize_apply_url  - defensively repairs known-bad URL shapes on write
#   _apply_verified_for  - domain-based verified flag that drives board ranking
#   _qa_link_gate        - structural check per platform; alarms on regression
from urllib.parse import urlsplit as _urlsplit

# Opaque SPA platforms whose apply links can't be verified from outside without
# their own API (generic shell, no JSON-LD, redirect-on-live). Ranked to the
# back of the board via apply_verified=false until per-platform verification.
#
# 2026-08-28 EMPIRICAL RE-VERIFICATION (browser-grade curl_cffi requests, 620
# sampled links spanning every platform then on this list): 618/620 resolved
# to live postings; the only 2 dead were ordinary expired Ascension reqs, not
# platform breakage. The blanket distrust was denying ~15,600 active jobs
# their apply_verified flag (and the desc>=200 subset its place in the SEO
# quality cohort) for no reason. All proven platforms removed; only the one
# platform too small to draw in the sample (22 jobs) stays until checked.
OPAQUE_APPLY_DOMAINS = {
    "kontactintelligence.com",
}

def _reg_domain(url: str) -> str:
    try:
        host = _urlsplit(url).netloc.lower().split(":")[0].split(".")
        return ".".join(host[-2:]) if len(host) >= 2 else ".".join(host)
    except Exception:
        return ""

def _sanitize_apply_url(url: str) -> str:
    """Defensively repair known-bad apply-URL shapes on write, so a regressed
    adapter can't ship a link that 404s / lands on Sign-In. Idempotent."""
    if not url or not url.startswith(("http://", "https://")):
        return url
    # Workday: externalPath already begins with /job/, so a prepended /job/
    # produces /job//job/ -> Sign-In page. Collapse to canonical single slash.
    url = url.replace("/job//job/", "/job/")
    # Oracle HCM: candidate URL must be singular /sites/{site}/job/{id};
    # plural /jobs/{id} renders "Page not found". Leave /jobs/preview/{id}.
    url = re.sub(r"(/sites/[^/]+)/jobs/(?!preview/)([0-9A-Za-z])", r"\1/job/\2", url)
    return url

def _apply_verified_for(url: str) -> bool:
    """True for platforms whose apply links we trust; False for the opaque
    tranche (ranked to the back of the board until we can verify them)."""
    return _reg_domain(url) not in OPAQUE_APPLY_DOMAINS

def _structural_url_ok(url: str, platform: str) -> tuple[bool, str]:
    """Does the apply URL match its platform's canonical shape? Catches the
    template breaks behind the three broken-link bugs, and novel breaks that
    deviate from the expected pattern."""
    if not url or not url.startswith(("http://", "https://")):
        return False, "empty_or_relative"
    low = url.lower()
    if "myworkdayjobs.com" in low or "myworkdaysite.com" in low:
        if "/job//job/" in url:
            return False, "workday_doubled_slash"
        if "/job/" not in url:
            return False, "workday_no_job_path"
        return True, ""
    if "oraclecloud.com" in low:
        if re.search(r"/sites/[^/]+/jobs/(?!preview/)[0-9A-Za-z]", url):
            return False, "oracle_plural_jobs"
        return True, ""
    if "jobsapi-google" in low or "adventhealth.com" in low:
        if re.search(r"/job/R-", url):
            return False, "findly_retired_R_path"
        return True, ""
    try:
        p = _urlsplit(url)
        if not p.path or p.path == "/":
            return False, "no_path"
    except Exception:
        return False, "unparseable"
    return True, ""

def _qa_link_gate(rows: list[dict], run_started_iso: str, sb_url: str, sb_key: str) -> None:
    """Post-scrape resolvability gate: structurally validate every apply URL per
    platform, log per-platform malformed-rates, ALARM on regressions, and record
    to qa_link_audit for monitoring. Non-fatal — never blocks a run."""
    from collections import Counter
    by: dict[str, dict] = {}
    for r in rows:
        plat = r.get("ats_platform") or "unknown"
        ok, reason = _structural_url_ok(r.get("url", ""), plat)
        d = by.setdefault(plat, {"total": 0, "bad": 0, "reasons": Counter()})
        d["total"] += 1
        if not ok:
            d["bad"] += 1
            d["reasons"][reason] += 1
    THRESH = 0.05
    audit_rows, any_alarm = [], False
    for plat, d in sorted(by.items()):
        rate = d["bad"] / d["total"] if d["total"] else 0.0
        alarm = rate > THRESH and d["total"] >= 20
        any_alarm = any_alarm or alarm
        msg = f"QA link gate [{plat}]: {d['bad']}/{d['total']} malformed ({rate:.1%})"
        if d["reasons"]:
            msg += f" {dict(d['reasons'])}"
        logger.warning("  ALARM " + msg) if alarm else logger.info("  " + msg)
        audit_rows.append({"run_at": run_started_iso, "platform": plat,
                           "total": d["total"], "structural_bad": d["bad"],
                           "bad_rate": round(rate, 4), "alarm": alarm})
    if any_alarm:
        logger.warning(f"QA link gate: a platform exceeded the {THRESH:.0%} malformed-URL "
                       "threshold — a scraper adapter likely regressed; investigate.")
    if sb_url and sb_key and audit_rows:
        try:
            import urllib.request as _u
            rq = _u.Request(f"{sb_url.rstrip('/')}/rest/v1/qa_link_audit",
                            data=json.dumps(audit_rows).encode(),
                            headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}",
                                     "Content-Type": "application/json", "Prefer": "return=minimal"},
                            method="POST")
            with _u.urlopen(rq, timeout=30) as resp:
                resp.read()
        except Exception as e:
            logger.info(f"QA link gate: audit insert skipped ({e})")


# ── Hospital upsert + deactivation pass (added 2026-05-12) ─────────────────
# Mirrors _upsert_travel_jobs_to_supabase. The hospital pipeline previously
# had no deactivation step, so when a hospital filled or removed a posting it
# would sit in our DB with is_active=true forever — causing dead-link rates
# of 70%+ on AdventHealth, CommonSpirit, and others.
#
# This function:
#   1. Upserts every row in the current run with scraped_at=run_started_iso,
#      refreshing the timestamp on conflict.
#   2. Per-system deactivation: any row whose hospital_system appeared in
#      THIS run (≥ DEACT_MIN rows) AND whose scraped_at < run_started_iso
#      gets is_active=false. Systems that produced fewer than DEACT_MIN rows
#      this run get skipped — that protects them from being wiped on a bad
#      run (e.g. when an ATS migrates or the proxy chain glitches).
#   2026-09-24: batches of 100 that split, retry and continue; a system is
#      swept only if every one of its rows landed, and only if it passes the
#      yield guard shared with Layer 4 (retire_guard.py). Returns rows landed.
def _upsert_hospital_jobs_to_supabase(rows: list[dict], run_started_iso: str) -> int:
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = (os.environ.get("SUPABASE_KEY", "")
              or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))
    if not sb_url or not sb_key:
        logger.info("Hospital upsert: SUPABASE_URL/SUPABASE_KEY not set — no-op")
        return 0
    if not rows:
        logger.info("Hospital upsert: no rows to send")
        return 0

    import urllib.request as _urlreq, urllib.error as _urlerr
    from urllib.parse import quote as _q

    # 1. Upsert. Stamp scraped_at to this run's start so on-conflict merge
    #    refreshes it — that's what makes the deactivation pass below
    #    correctly distinguish fresh rows from stale.
    #
    # ALIAS CANONICALIZATION (added 2026-05-26): rewrite hospital_system
    # to the canonical name used in hospital_wages so the wage-join works.
    # See HOSPITAL_SYSTEM_ALIASES at the top of this file.
    # JOB-TYPE DERIVATION (added 2026-05-27): populate derived_job_type from
    # the noisy raw job_type + title. See derive_job_type() at top of file.
    # ~67% of hospital ATSs do not expose a clean job-type field; the rest
    # bleed wage strings or schedule prose into the column. The classifier
    # buckets into: travel, per_diem, temporary, part_time, full_time,
    # resident_intern, standard (= unsignaled hospital staff).
    alias_hits = 0
    jt_buckets = {}
    for r in rows:
        r["scraped_at"] = run_started_iso
        r["is_active"]  = True
        # QA guardrails: repair any known-bad apply-URL shape, then set the
        # verified flag that ranks working links first on the board.
        r["url"] = _sanitize_apply_url(r.get("url", ""))
        r["apply_verified"] = _apply_verified_for(r.get("url", ""))
        sys_name = r.get("hospital_system")
        if sys_name and sys_name in HOSPITAL_SYSTEM_ALIASES:
            r["hospital_system"] = HOSPITAL_SYSTEM_ALIASES[sys_name]
            alias_hits += 1
        # Job-type classification — uses raw job_type (which may be noise)
        # + title as fallback signal. Result lands in derived_job_type so
        # the read side can ignore the original column without losing info.
        djt = derive_job_type(r.get("title"), r.get("job_type"))
        r["derived_job_type"] = djt
        jt_buckets[djt] = jt_buckets.get(djt, 0) + 1
        # 2026-09-10 (T2): employer class (telehealth / urgent_care / ...);
        # None for hospitals. Every row carries the key because PostgREST bulk
        # upserts need uniform keys, and the DB CHECK on employer_type lists
        # the allowed values (see employer_type_for).
        r["employer_type"] = employer_type_for(r.get("hospital_system"), r.get("hospital_name"))
    if alias_hits:
        logger.info(f"Hospital upsert: canonicalized {alias_hits} rows via HOSPITAL_SYSTEM_ALIASES")
    if jt_buckets:
        top = sorted(jt_buckets.items(), key=lambda kv: -kv[1])
        logger.info(f"Hospital upsert: derived_job_type buckets = {dict(top)}")

    # QA link gate — structural resolvability check on the final (sanitized)
    # URLs. Runs before the write so a regression is logged even if the upsert
    # later fails. Non-fatal.
    _qa_link_gate(rows, run_started_iso, sb_url, sb_key)

    url = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs"
           f"?on_conflict=job_id,hospital_system")
    headers = {
        "apikey":        sb_key,
        "Authorization": f"Bearer {sb_key}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }
    # Dedupe on the conflict key before batching (moved here from
    # database.upsert_jobs on 2026-09-24, when scheduler stopped re-sending
    # every row through it). finalize_jobs dedupes on (ats, system, job_id),
    # so two ATS entries aliased to one system can still emit one key twice,
    # and Postgres rejects the whole statement (21000, "ON CONFLICT DO UPDATE
    # cannot affect row a second time"). Last one wins, but a row with a
    # description beats one without.
    by_key: dict[tuple, dict] = {}
    for r in rows:
        k = (r.get("hospital_system"), str(r.get("job_id")))
        prev = by_key.get(k)
        if prev is not None and (prev.get("description") or "") and not (r.get("description") or ""):
            continue
        by_key[k] = r
    if len(by_key) < len(rows):
        logger.info(f"Hospital upsert: deduped {len(rows) - len(by_key)} duplicate (system, job_id) rows")
        rows = list(by_key.values())

    # 2026-09-24: resilient batches. 500-row statements hit the role's 8 s
    # statement_timeout (57014) on a 32-index table, and the loop used to stop
    # at the first failure: the 09-24 HCA local push landed 6,500 of 17,153
    # rows, and an incomplete nightly upsert skips the whole sweep (about 18k
    # unseen rows of healthy systems were still live on 09-24). Now: 100-row
    # batches; a failed batch is split in half (100, 50, 25) and the 25-row
    # pieces are retried
    # with backoff when the error is load-shaped (5xx, 57014, 408/429, network);
    # a batch that still fails is recorded and the loop CONTINUES. A batch that
    # had to be split halves the size of the batches after it (down to 25) and
    # 20 clean batches double it back, so a database under pressure is not
    # sent 100-row statements it cannot finish, 2,000 times a night. Only when
    # UPSERT_BREAKER_ROWS rows in a row fail to land (the database is down, or
    # every row is rejected: several minutes of failures) does the loop stop.
    # Retrying is safe: the upsert is idempotent (merge-duplicates on
    # job_id, hospital_system).
    BATCH, MIN_BATCH = 100, 25
    RETRY_PAUSES = (2, 5)      # seconds, before each retry of a MIN_BATCH piece
    UPSERT_BREAKER_ROWS = 300
    failed_rows: list[dict] = []
    splits = [0]

    def _post(chunk: list[dict]) -> tuple[bool, bool, str]:
        """One POST. Returns (ok, retryable, detail)."""
        rq = _urlreq.Request(url, data=json.dumps(chunk).encode(), headers=headers, method="POST")
        try:
            with _urlreq.urlopen(rq, timeout=60) as resp:
                resp.read()
            return True, False, ""
        except _urlerr.HTTPError as e:
            try:
                err = e.read().decode()[:300]
            except Exception:
                err = ""
            retryable = e.code >= 500 or e.code in (408, 429) or "57014" in err
            return False, retryable, f"HTTP {e.code} {err}"
        except Exception as e:   # URLError, socket timeout, connection reset
            return False, True, f"{type(e).__name__}: {e}"

    def _send(chunk: list[dict], label: str) -> int:
        """Land as much of chunk as possible; returns rows landed. Rows that
        never land go to failed_rows."""
        ok, retryable, detail = _post(chunk)
        if ok:
            return len(chunk)
        if len(chunk) > MIN_BATCH:
            splits[0] += 1
            logger.warning(f"Hospital upsert {label} ({len(chunk)} rows): {detail}; splitting in half")
            if retryable:
                time.sleep(RETRY_PAUSES[0])
            mid = len(chunk) // 2
            return _send(chunk[:mid], label + "a") + _send(chunk[mid:], label + "b")
        for pause in (RETRY_PAUSES if retryable else ()):
            time.sleep(pause)
            ok, retryable, detail = _post(chunk)
            if ok:
                return len(chunk)
            if not retryable:
                break
        logger.warning(f"Hospital upsert {label} ({len(chunk)} rows) FAILED, continuing: {detail}")
        failed_rows.extend(chunk)
        return 0

    sent = 0
    size, clean, dead_rows = BATCH, 0, 0
    i = 0
    while i < len(rows):
        chunk = rows[i:i + size]
        before = splits[0]
        n = _send(chunk, f"batch {i}")
        sent += n
        i += len(chunk)
        if splits[0] > before:
            clean = 0
            if size > MIN_BATCH:
                size = max(MIN_BATCH, size // 2)
                logger.warning(f"Hospital upsert: batch size down to {size}")
        elif n == len(chunk):
            clean += 1
            if size < BATCH and clean >= 20:
                size, clean = min(BATCH, size * 2), 0
        else:
            clean = 0
        dead_rows = dead_rows + len(chunk) if n == 0 else 0
        if dead_rows >= UPSERT_BREAKER_ROWS:
            rest = rows[i:]
            failed_rows.extend(rest)
            logger.error(f"Hospital upsert STOPPED: {dead_rows} rows in a row failed to land "
                         f"(through row {i:,}); {len(rest):,} rows not attempted")
            break
    logger.info(f"Hospital upsert: {sent}/{len(rows)} rows sent")
    if sent == 0:
        return 0

    # 2026-09-22: while the database was timing out, the sweep ran against the
    # FULL row list after an incomplete upsert, and every system whose rows
    # were never re-stamped lost its whole inventory (22,689 rows). The rule
    # is now per system (2026-09-24): a system with ANY row that did not land
    # is not swept tonight; a system whose every row landed is swept as usual.
    failed_systems = {r.get("hospital_system") for r in failed_rows if r.get("hospital_system")}
    if failed_systems:
        fs = sorted(failed_systems)
        more = f" (+{len(fs) - 20} more)" if len(fs) > 20 else ""
        logger.error(f"Hospital deactivate SKIPPED for {len(fs)} system(s) whose upsert did not fully land "
                     f"({len(failed_rows):,} rows): {fs[:20]}{more}")

    # 2. Per-system deactivation pass.
    DEACT_MIN = 10
    system_counts: dict[str, int] = {}
    for r in rows:
        s = r.get("hospital_system")
        if s:
            system_counts[s] = system_counts.get(s, 0) + 1
    # 2026-09-10 (S-scraper-2): an adapter that reported a partial run
    # (PARTIAL_SYSTEMS, post-alias names) is never swept that night.
    partial = {HOSPITAL_SYSTEM_ALIASES.get(s, s) for s in PARTIAL_SYSTEMS}
    if partial:
        logger.warning(f"Hospital deactivate skip (adapter reported a PARTIAL run): {sorted(partial)}")
    safe_systems = sorted(s for s, n in system_counts.items()
                          if n >= DEACT_MIN and s not in partial and s not in failed_systems)
    skipped      = sorted((s, n) for s, n in system_counts.items() if 0 < n < DEACT_MIN)
    if skipped:
        sk = skipped[:8]
        more = f" (+{len(skipped) - 8} more)" if len(skipped) > 8 else ""
        logger.info(f"Hospital deactivate skip (below DEACT_MIN={DEACT_MIN}): {sk}{more}")

    # 2b. Proportional sweep guard (added 2026-07-29). DEACT_MIN alone is a
    # thin shield: the 2026-07-29 Railway run pulled 2 of ~16k HCA jobs
    # (Cloudflare IP block) — had it pulled 10+, the sweep would have
    # deactivated the system's entire healthy inventory. Before sweeping,
    # compare this run's yield against the system's currently-active rows;
    # a large system suddenly yielding under a quarter of its inventory is a
    # broken/blocked adapter, not a hiring freeze. Skip it loudly and let a
    # healthy future run resume sweeping. Count failures fall through to the
    # old behavior (sweep) so a flaky count can't disable cleanup globally.
    # 2026-09-22: three changes after the 22,689-row sweep. A count that
    # fails now protects the system (it used to "sweep as usual", so a busy
    # database turned every guard off at once); the guard covers systems
    # from 20 active rows up, not 200; and a run must yield at least 80% of
    # a system's active rows before the rest is retired, not 25%: a half
    # crawl of a 16k-row system is a blocked adapter, not 8,000 closed jobs.
    # 2026-09-24: the thresholds and the test live in retire_guard.py, shared
    # with Layer 4 (database.mark_inactive_jobs), so the two retirement passes
    # cannot drift apart. A count whose total cannot be read also protects.
    from retire_guard import GUARD_RATIO, guard_reason
    guarded: list[str] = []
    for system in list(safe_systems):
        try:
            curl_ = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs"
                     f"?select=id&is_active=eq.true&hospital_system=eq.{_q(system)}&limit=1")
            crq = _urlreq.Request(curl_, headers={"apikey": sb_key,
                                                  "Authorization": f"Bearer {sb_key}",
                                                  "Prefer": "count=exact",
                                                  "Range": "0-0"})
            with _urlreq.urlopen(crq, timeout=30) as resp:
                cr = resp.headers.get("Content-Range", "")
            total = cr.split("/")[-1] if "/" in cr else ""
            if not total.isdigit():
                raise ValueError(f"unreadable Content-Range {cr!r}")
            active_n = int(total)
        except Exception as e:
            safe_systems.remove(system)
            guarded.append(system)
            logger.warning(f"SWEEP GUARD: NOT sweeping {system} — active-count failed ({e}); inventory preserved")
            continue
        reason = guard_reason(active_n, system_counts[system])
        if reason:
            safe_systems.remove(system)
            guarded.append(system)
            logger.warning(
                f"SWEEP GUARD: NOT sweeping {system} — {reason}: run yielded {system_counts[system]} rows "
                f"vs {active_n} active in DB (<{int(GUARD_RATIO*100)}%). Adapter likely "
                f"broken/blocked; inventory preserved.")
    if guarded:
        logger.warning(f"Sweep guard protected {len(guarded)} system(s): {guarded}")

    patch_headers = {
        "apikey":        sb_key,
        "Authorization": f"Bearer {sb_key}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal,count=exact",
    }
    body = json.dumps({"is_active": False}).encode()
    total_deactivated = 0
    for system in safe_systems:
        purl = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs"
                f"?is_active=eq.true"
                f"&hospital_system=eq.{_q(system)}"
                f"&scraped_at=lt.{_q(run_started_iso)}")
        rq = _urlreq.Request(purl, data=body, headers=patch_headers, method="PATCH")
        try:
            with _urlreq.urlopen(rq, timeout=60) as resp:
                cr = resp.headers.get("Content-Range", "")
                n  = int(cr.split("/")[-1]) if "/" in cr and cr.split("/")[-1].isdigit() else 0
                if n:
                    logger.info(f"  Deactivated {n} stale rows for {system}")
                    total_deactivated += n
        except _urlerr.HTTPError as e:
            try: err = e.read().decode()[:300]
            except Exception: err = ""
            logger.warning(f"Deactivate {system}: HTTP {e.code} — {err}")
        except Exception as e:
            logger.warning(f"Deactivate {system}: {e}")
    logger.info(f"Hospital deactivation pass: {total_deactivated} rows across "
                f"{len(safe_systems)} systems (skipped {len(skipped)})")
    return sent


def scrape_travel() -> list[dict]:
    os.makedirs("logs", exist_ok=True)
    return asyncio.run(run_all_travel())


# ══════════════════════════════════════════════════════════════════════════
#  MASTER RUNNER
# ══════════════════════════════════════════════════════════════════════════
# ── Normalization helpers (hoisted from run_all 2026-07-29 so partial-push
#    scripts like hca_local_push.py reuse the exact same pipeline) ──────────
SPECIALTY_MAP = {
    "ICU / Critical Care": ["icu", "intensive care", "critical care", "micu", "sicu", "cvicu", "neuro icu", "picu", "cardiac icu", "ccu", "coronary care", "trauma icu", "burn icu"],
    "Emergency / Trauma": ["emergency department", "emergency room", "emergency care", "emergency medicine", " ed rn", " er rn", "ecc ", "trauma nurse", "trauma rn", " er nurse", "emergency nurse"],
    "Labor & Delivery": ["labor and delivery", "labor & delivery", "l&d", "ldrp", "ldrpn", "obstetric", "ob nurse", "ob rn", "mother baby", "antepartum", "postpartum", "maternal", "perinatal", "birth center", "women and infant", "women & infant"],
    "Med / Surg": ["med surg", "med-surg", "medsurg", "medical surgical", "medical-surgical", "acute care", "acute medsurg", "telemetry", "tele rn", "tele nurse", "imc"],
    "Operating Room / Surgery": ["operating room", " or rn", "or nurse", "perioperative", "surgical services", "surgery rn", "surgery nurse", "circulator", "scrub nurse", "pacu", "post anesthesia", "pre-op", "pre operative", "preoperative", "post-op", "post operative", "postoperative", "ambulatory surgery"],
    "Cardiac / Cardiovascular": ["cardiac", "cardiology", "cardiovascular", "cath lab", "catheterization", "cardiothoracic", "electrophysiology", "ep lab", "echocardiogram", "echo tech", "cardiac rehab", "heart failure"],
    "Oncology": ["oncology", "cancer", "chemo", "chemotherapy", "hematology", "infusion", "radiation therapy", "radiation therapist", "radiation oncology"],
    "Pediatrics": ["pediatric", "peds ", "pedi ", "neonatal", "nicu", "newborn", "pediatrician", "children", "child life"],
    "Behavioral Health / Psych": ["behavioral health", "behavioral medicine", "psychiatric", "psych ", "mental health", "addiction", "substance abuse", "detox", "counselor", "behavioral counselor"],
    "Home Health": ["home health", "home care", "visiting nurse", "home hospice"],
    "Wound Care / Dialysis": ["wound care", "ostomy", "dialysis", "hemodialysis", "renal", "nephrology"],
    "CRNA / Anesthesia": ["crna", "certified registered nurse anesthetist", "anesthesia", "anesthesiologist"],
    "Travel Nursing": ["travel nurse", "travel rn", "travel assignment", "travel contract", "13-week", "13 week"],
    "Nurse Practitioner / PA": ["nurse practitioner", " np ", "np-", " pa ", "pa-", "physician assistant", "advanced practice", "aprn", "acnp", "fnp", "agacnp", "np/pa", "np-pa"],
    "Float Pool / General RN": ["float pool", "float rn", "staff nurse", "staff rn", "registered nurse", " rn ", "clinical nurse", "nurse resident", "nurse residency", "nurse extern", "nurse intern", "nurse manager", "charge nurse", "nursing supervisor", "nursing assistant", "nurse aide", "cna ", "licensed practical nurse", "lpn ", " lvn ", "licensed vocational nurse"],
    "Radiology / Imaging": ["radiology", "radiolog", "radiologic", "x-ray", "xray", "mri", "magnetic resonance", "ct tech", "ct scan", "computed tomography", "ultrasound", "sonograph", "mammograph", "nuclear medicine", "nuclear med", "fluoroscopy", "interventional radiology", "imaging tech", "dosimetrist"],
    "Respiratory Therapy": ["respiratory therapist", "respiratory therapy", "rrt", "crt ", "pulmonary", "ventilator"],
    "Physical / Occupational Therapy": ["physical therapist", "physical therapy", " pt ", "occupational therapist", "occupational therapy", " ot ", "speech patholog", "speech therapist", "speech language", " slp ", "rehab therapist", "rehabilitation", "athletic trainer"],
    "Pharmacy": ["pharmacist", "pharmacy technician", "pharmacy tech", "clinical pharmacist", "pharmacy manager"],
    "Laboratory": ["laboratory", "lab technician", "lab tech", "lab scientist", "clinical laboratory", "medical laboratory", "phlebotomist", "phlebotomy", "blood bank", "histolog", "histotech", "cytotech", "patholog", "microbiology", "lab assistant", " mlt ", " mls ", "medical technologist", "med technologist"],
    "Surgical Tech": ["surgical technologist", "surgical tech", "scrub tech", "cst ", "sterile processing", "central sterile"],
    "EMS / Paramedic": ["paramedic", "emt ", "emergency medical tech", "ems ", "ambulance", "flight medic"],
    "Physician": ["physician", " md ", " do ", "hospitalist", "intensivist", "neonatologist", "cardiologist", "neurologist", "oncologist", "radiologist", "anesthesiologist", "surgeon", "psychiatrist", "pulmonologist", "gastroenterologist", "nephrologist", "endocrinologist", "rheumatologist", "urologist", "orthopedic", "ophthalmologist", "dermatologist", "pathologist", "emergency medicine physician", "family medicine", "internal medicine", "primary care", "physiatrist"],
    "Healthcare Administration": ["director", "administrator", "chief ", " vp ", "vice president", "manager ", "supervisor", "coordinator", "case manager", "care manager", "utilization management", "quality management", "compliance", "revenue cycle", "coding", "billing", "health information", "medical records", "hr business", "human resources", "accounts payable", "accounts receivable", "enrollment representative", "enrollment specialist"],
    "Support Staff": ["patient transporter", "patient care tech", "patient care assistant", "unit secretary", "unit clerk", "medical assistant", "patient registrar", "patient access", "admitting", "scheduling", "front desk", "receptionist", "food service", "dietary", "housekeeping", "environmental services", "evs ", "security officer", "security guard", "groundskeeper", "maintenance", "supply chain", "driver ", "chaplain", "office assistant", "registrar"],
}

def classify_title(title: str):
    if not title:
        return None
    t = f" {title.lower()} "
    for specialty, keywords in SPECIALTY_MAP.items():
        for kw in keywords:
            if kw in t:
                return specialty
    return None

def _loc_text(v) -> str:
    """Coerce an adapter's location-ish value to a stripped string. Dicts
    (Ashby / ADP / UKG style {name, city, state, ...}) become their most
    specific text; lists join with ', '; None becomes ''."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        for k in ("name", "locationName", "city", "text", "label", "value", "address"):
            t = v.get(k)
            if isinstance(t, str) and t.strip():
                return t.strip()
        parts = [str(x).strip() for x in v.values() if isinstance(x, (str, int)) and str(x).strip()]
        return ", ".join(parts)
    if isinstance(v, (list, tuple)):
        return ", ".join(_loc_text(x) for x in v if _loc_text(x))
    return str(v).strip()


def normalize_job(j: Job) -> dict:
    """Standardize location fields before writing to Supabase.
    - city/state cleaned and trimmed
    - If city matches hospital_name (or hospital_system), blank the city
    - location always built as 'City, ST' from clean city + state
    """
    d = asdict(j)

    # Sanitize/extract the city up front: turns "2 Locations", street
    # addresses, and pipe/newline facility blocks into a real city or "".
    # Anything blanked here is refilled by the FACILITY/SYSTEM location
    # fallback below. (see city_utils.clean_city)
    # 2026-09-10: keep the adapter's raw city / location text for the CMS
    # facility lookup below (clean_city blanks facility names, correctly).
    # 2026-09-10 (hotfix): two manual runs died here with "'dict' object has
    # no attribute 'strip'": a new adapter handed a location OBJECT through.
    # Every text field is coerced now, so one adapter's shape can never
    # abort finalize_jobs for the whole run again.
    raw_city = _loc_text(d.get("city"))
    raw_loc  = _loc_text(d.get("location"))
    city  = clean_city(raw_city.strip(",").strip())
    state = _loc_text(d.get("state")).upper()
    for _k in ("title", "hospital_name", "hospital_system", "url", "description", "specialty", "job_type"):
        if isinstance(d.get(_k), (dict, list)):
            d[_k] = _loc_text(d.get(_k))

    # Force override — always wins regardless of scraped data
    _sys_key = (d.get("hospital_system") or "").strip().lower()
    if _sys_key in FORCE_LOCATION_OVERRIDE:
        city, state = FORCE_LOCATION_OVERRIDE[_sys_key]

    # Keep only the 2-char state code if state is noisy (e.g. "TX, United States" or "United States")
    COUNTRY_JUNK = {"united states", "us", "usa", "canada", "united kingdom", "uk"}
    if state and (len(state) > 2 or state.lower() in COUNTRY_JUNK):
        parts = [p.strip() for p in state.split(",")]
        state = next((p for p in parts if len(p) == 2 and p.isalpha()), "")
        if not state:
            # Try pulling state from raw location string instead
            raw_loc = (d.get("location") or "").upper()
            loc_parts = [p.strip() for p in raw_loc.split(",")]
            state = next((p for p in loc_parts if len(p) == 2 and p.isalpha()), "")

    # Blank city only if it is an exact match for the hospital/system name
    # (Workday previously put loc string as hospital_name — now fixed upstream)
    hosp_name   = (d.get("hospital_name")   or "").strip().lower()
    hosp_system = (d.get("hospital_system") or "").strip().lower()
    city_lower  = city.lower()
    if city_lower and (city_lower == hosp_name or city_lower == hosp_system):
        city = ""
    # 2026-09-24: a "city" that is only its own state's name ("Texas, TX" on
    # 82 UTHealth Houston rows) is a region, not a city; the fallback below
    # refills it. New York, NY is a real city and stays.
    if city_lower and state and state != "NY" and _STATE_CODE_BY_NAME.get(city_lower) == state:
        city = ""

    # 2026-09-17: bare city with a known state for this system (ProMedica's
    # Michigan towns, Legacy's Vancouver WA) before the system default.
    if city and not state:
        state = SYSTEM_CITY_STATE.get(hosp_system, {}).get(city_lower, "")

    # Location lookup fallback — fires when city or state still missing
    if not city or not state:
        lookup = FACILITY_LOCATION_MAP.get(hosp_name) or SYSTEM_LOCATION_DEFAULTS.get(hosp_system)
        if lookup:
            fallback_city, fallback_state = lookup
            if not city:
                city = fallback_city
            if not state:
                state = fallback_state

    # CMS facility lookup (2026-09-10, S-scraper-2): a unique CMS facility
    # name in hospital_name or in the raw location text fills state (and city
    # when blank). Nothing system-level; see cms_location_for.
    if not state:
        hit = cms_location_for(d.get("hospital_name"), raw_loc, raw_city)
        if hit:
            if not city:
                city = hit[0]
            state = hit[1]
            _CMS_FILLS["n"] += 1

    # Build canonical location: "City, ST" — blank if both missing
    if city and state:
        location = f"{city}, {state}"
    elif state:
        location = state
    elif city:
        location = city
    else:
        location = ""

    d["city"]     = city
    d["state"]    = state
    d["location"] = location

    # Canonical specialty (2026-07-30). This USED to be
    #   if not d.get("specialty"): d["specialty"] = classify_title(...)
    # i.e. whatever string the ATS supplied won and was stored verbatim, which
    # left 214 distinct values in hospital_jobs — five spellings of "Advanced
    # Practice", seven of "Administrative", plus "Rehabilitation Services",
    # "Physical Therapist Assistant" and a long tail nobody could filter by.
    # A user searching "physical therapist" got 5,746 hits while the specialty
    # filter returned a fraction of them.
    #
    # Now every row is canonicalised: title first (profession before setting,
    # so "Physical Therapist - Inpatient - Acute Care" is Physical Therapy, not
    # Med / Surg), then the ATS string through an alias table, and if neither
    # resolves the original value is kept rather than nulled — so this can
    # never make a row worse. See specialty_canon.py.
    d["specialty"] = canonical_specialty(d.get("title", ""), d.get("specialty"))

    # Employment type (2026-09-21, job page v2): one vocabulary for the chip
    # ("FULL_TIME", "Full-time", "Regular Full time" -> "Full time"); a blank
    # ATS value falls back to the title. The enrichment trigger keeps a
    # stored value when a list-only night sends a blank.
    d["job_type"] = canonical_job_type(d.get("job_type"), d.get("title", ""))
    if not d["job_type"]:
        d["job_type"] = job_type_from_text(d.get("description"))

    # Posted-wage (2026-08-08; adapter-first 2026-08-21): a structured salary
    # field set by the adapter (USAJobs, Lever) always wins; otherwise regex
    # extraction from the posting text. NULLs never clobber a prior value
    # (enrichment trigger).
    if d.get("wage_min") is None:
        wage = extract_posted_wage(f"{d.get('title') or ''}\n{d.get('description') or ''}", d.get("job_type"))
        d["wage_min"], d["wage_max"], d["wage_unit"] = wage if wage else (None, None, None)

    # Requirements chips (2026-08-24): certs/education/shift/experience from
    # the posting text. Null when nothing found; the enrichment trigger
    # preserves a stored value across list-only upserts.
    d["posting_facts"] = extract_posting_facts(d.get("description"), d.get("job_type"))

    return d


# ── Posted-wage extraction (2026-08-08) ────────────────────────────────────
# Regex over text the scrape already holds — zero extra requests. Guards:
# ignore dollar figures near bonus/sign-on/relocation/stipend language, and
# sanity-bound hourly 12-250 and annual 25k-900k so "$401k match" noise and
# job-ID-like numbers can't become a wage. Hourly ranges win over annual
# when both appear (healthcare postings quote hourly for the roles we pill).
# 2026-09-22 (Charlie Health lesson, hospital_jobs 26658803): the old guard
# vetoed any figure with a bonus word within 60 characters before or 20 after,
# across line breaks. "Full-Time Salary: (base + bonus) $70,000-$80,000" died
# on "bonus", "Part-Time Rate: $54-$66/hour" died on the "Signing Bonuses!"
# line under it, and the page showed a BLS estimate under "The posting lists
# no pay". The veto now stays inside the figure's own clause, treats "base +
# bonus" / "plus bonus" / "bonus eligible" as a description of the pay rather
# than a priced bonus, and lets a unit or pay word right after the figure
# ("/hour", "per year", "salary") settle it. See _wage_is_noise.
_WAGE_NOISE_WORDS = r"sign[- ]?on|signing|bonus(?:es)?|relocation|retention|referral|differential|stipend|reimburse\w*|incentives?"
_WAGE_NEAR_NOISE = re.compile(_WAGE_NOISE_WORDS, re.I)
_WAGE_NOISE_HEAD_RX = re.compile(r"(?:^|[^A-Za-z$])(" + _WAGE_NOISE_WORDS + r")\b(?:\s+bonus(?:es)?)?([^$\n;]{0,50})$", re.I)
_WAGE_NOISE_TAIL_RX = re.compile(r"^([^$\n;]{0,40}?)\b(" + _WAGE_NOISE_WORDS + r")\b", re.I)
_WAGE_NOISE_LABEL_RX = re.compile(r"(?:^|[^A-Za-z$])(" + _WAGE_NOISE_WORDS + r")\b(?:\s+bonus(?:es)?)?\s*(?::|-|–|\bof\b|\bup\s+to\b|\bis\b)", re.I)
# Words that make a bonus part of the pay description rather than a priced
# extra: "(base + bonus) $70,000", "$70,000 plus bonus", "$70,000, bonus eligible".
_WAGE_COMPONENT_WORD = re.compile(r"^(?:bonus(?:es)?|incentives?|differentials?|stipends?)$", re.I)
_WAGE_COMPOSITION_RX = re.compile(
    r"(?:\+|plus|and|&|including|incl\.?|inclusive of|with|or)\s*(?:an?\s+|the\s+)?"
    r"(?:annual|quarterly|monthly|performance|potential|productivity|generous|possible)?\s*$", re.I)
_WAGE_BONUS_DESCR_RX = re.compile(r"\s*(?:eligib|potential|opportunit|program|structure|plan|available)", re.I)
_WAGE_JOINER_RX = re.compile(r"^[\s,.]*(?:\+|(?:plus|and|&|with|including|incl\.?|in addition to|or)\b)", re.I)
_WAGE_NEXT_FIGURE_RX = re.compile(r"(?:\s+bonus(?:es)?)?\s*[:\-!*]*\s*(?:of|up to|is|=|starting at)?\s*\$", re.I)
_WAGE_UNIT_RX = re.compile(
    r"^\s*(?:/\s*(?:hour|hr|year|yr|annum)|per\s+(?:hour|year|annum)|an?\s+(?:hour|year)|hourly|annually|annual|salary|base)\b", re.I)
# A line that names one employment type before its figure ("Full-Time
# Salary: $70,000-$80,000", "Part-Time: Minimum 12 hours/week") is scored
# against the row's job_type: the matching line wins, the other loses, and
# with no job_type the full-time line is the headline. Wordy forms only
# ("PT" is also physical therapy).
_WAGE_TYPE_LABELS = (
    ("Full time", re.compile(r"full[\s-]*time", re.I)),
    ("Part time", re.compile(r"part[\s-]*time", re.I)),
    ("Per diem",  re.compile(r"per[\s-]*diem|\bPRN\b|as[\s-]needed", re.I)),
)


def _line_type(t, pos):
    """The employment type the line names before position pos, or None when
    it names none or more than one ("part-time or full-time")."""
    head = t[t.rfind("\n", 0, pos) + 1:pos][-80:]
    hits = [label for label, rx in _WAGE_TYPE_LABELS if rx.search(head)]
    return hits[0] if len(hits) == 1 else None


def _type_score(label, job_type):
    if not label:
        return 0
    if job_type:
        return 4 if label == job_type else -4
    return 2 if label == "Full time" else 0


def _wage_is_noise(t, start, end):
    """True when the dollar figure at t[start:end] prices a bonus, relocation
    package, stipend or reimbursement rather than the pay. Decided inside the
    figure's own clause only: no line break or sentence end is crossed, the
    words between two figures belong to the earlier one, a component word
    joined to the pay ("base + bonus", "plus bonus", "bonus eligible") is a
    description, and a unit or pay word right after the figure makes it pay."""
    # Clause bounds: a line break, a semicolon, a sentence end before a
    # capital, or a flattened body's glued boundary ("27.25This position",
    # "experience)Sign-on Bonus").
    _clause = r"[\n;]|(?<=[a-z0-9)])[.!?](?=\s+[A-Z]|\s*$)|(?<=[a-z0-9)])(?=[A-Z][a-z])"
    after = re.split(_clause, t[end - 1:end + 80])[0][1:]      # one char of context so a glued boundary at the figure's end still splits
    before = re.split(_clause, t[max(0, start - 100):start])[-1]
    # An aside in parentheses before the figure is dropped, so "Retention
    # Bonus (beginning at the completion of the 2nd year): up to $30,000"
    # still reads as a bonus (Flagler Health, 2026-09-22).
    before = re.sub(r"\([^()]{0,80}\)", " ", before)
    if "$" in before:
        before = before.rsplit("$", 1)[-1]
        if re.match(r"\s*\d", before):
            # The words after an earlier figure belong to it ("$5,000 sign-on
            # bonus and $70,000"), unless a bonus word in them heads this
            # figure with a label marker ("$30,000 Retention Bonus: up to $30,000").
            m2 = _WAGE_NOISE_LABEL_RX.search(before)
            before = before[m2.start():] if m2 else ""
    m = _WAGE_NOISE_HEAD_RX.search(before)
    if m and not _PAY_WORDS.search(m.group(2)):
        if not (_WAGE_COMPONENT_WORD.match(m.group(1)) and _WAGE_COMPOSITION_RX.search(before[:m.start(1)])):
            return True
    if _WAGE_PAY_LABEL_BEFORE_RX.search(before) or _WAGE_UNIT_RX.match(after):
        return False
    # Flattened bodies (Select Medical) glue the next label onto the figure:
    # "$35.67 - $48.00 (based on experience)Sign-on Bonus: $10,000". An aside
    # in parentheses is skipped unless it names the figure ("(sign-on
    # bonus)"); a joining word means the bonus word is an extra, not the
    # figure's identity ("$55/hr + shift differential"); and a bonus word
    # that heads its own dollar figure belongs to that one.
    tail = after
    while True:
        pm = re.match(r"[\s,.]*\(([^)]{0,60})\)", tail)
        if not pm or _WAGE_NEAR_NOISE.match(pm.group(1).strip()):
            break
        tail = tail[pm.end():]
    if _WAGE_JOINER_RX.match(tail):
        return False
    m = _WAGE_NOISE_TAIL_RX.match(tail)
    if m:
        gap, word, rest = m.group(1), m.group(2), tail[m.end(2):]
        if _PAY_WORDS.search(gap) or _WAGE_NEXT_FIGURE_RX.match(rest):
            return False
        if _WAGE_COMPONENT_WORD.match(word) and (_WAGE_COMPOSITION_RX.search(gap) or _WAGE_BONUS_DESCR_RX.match(rest)):
            return False
        return True
    return False
# 2026-09-22: a "k" suffix counts ("$95k - $110k", "$45k annually").
# Three decimals too: Sharp HealthCare's Workday bodies write "Hourly Pay
# Range (Minimum - Midpoint - Maximum):$83.970 - $108.360 - $121.360", and
# the third figure (the maximum) is read by the range loop.
_WAGE_RANGE_RX = re.compile(
    r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,3})?)\s*(k\b)?\s*(?:-|–|—|to|through)\s*\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,3})?)\s*(k\b)?", re.I)
_WAGE_THIRD_RX = re.compile(r"\s*(?:-|–|—)\s*\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,3})?)\b")
# County boards (NeoGov) quote a month or a pay period: "$9,404.43 -
# $10,893.28 Biweekly", "$6,500 per month". Converted to a year.
_WAGE_PERIOD_RX = re.compile(r"\s*(?:(per\s+month|monthly|/\s*mo(?:nth)?\b|a\s+month)|(bi-?weekly|per\s+pay\s+period|every\s+(?:two|2)\s+weeks|per\s+(?:two|2)\s+weeks))", re.I)   # used with .match(t, pos): no ^ (it would anchor to the string start)


def _period_scale(t, pos):
    m = _WAGE_PERIOD_RX.match(t, pos)
    return (12 if m.group(1) else 26) if m else 1
# A pay label right before the figure settles it as pay whatever follows
# ("Pay range: $26.18 - $33.51 Relief Differential - 15%", St. Charles).
_WAGE_PAY_LABEL_BEFORE_RX = re.compile(
    r"(?:salary|pay|compensation|wage|rate|guarantee)\s*(?:range|rate|scale)?\s*(?:\([^()]*\))?\s*(?:of|at|is|:|-|–|from|starting at|starts at|up to)?\s*(?:up to\s+)?$", re.I)
_WAGE_SINGLE_RX = re.compile(
    r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)\s*(k\b)?\s*(?:per\s+hour|/\s*hr\b|/\s*hour(?:ly)?|hourly|an\s+hour|per\s+year|/\s*yr\b|annually|per\s+annum|a\s+year|"
    r"per\s+month|monthly|/\s*mo\b|a\s+month|bi-?weekly|per\s+pay\s+period|every\s+(?:two|2)\s+weeks)", re.I)
# A pay label right before a bare figure ("Income Guarantee at $354,000",
# "Salary: $85,000", "Pay rate $42") is a single figure with no unit; the
# band decides hourly or annual. Not when a range follows (the range rule
# owns it). Flagler Health's physician postings, 2026-09-22.
_WAGE_LABELLED_RX = re.compile(
    r"(?:salary|compensation|pay(?:\s+rate)?|wage|income\s+guarantee|guarantee(?:d)?(?:\s+(?:annual\s+)?(?:salary|income|base|minimum))?|"
    r"base(?:\s+salary|\s+pay)?|earnings?|rate)\s*(?:of|at|is|:|-|–|starts?\s+at|from|starting\s+at)?\s*(?:up\s+to\s+)?"
    r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)(?![\d,])\s*(k\b)?(?!\s*(?:-|–|—|to|through)\s*\$?\s*\d)", re.I)
# (?![\d,]) after the figure: without it "Salary: $12,500 - $15,000" backtracked
# to "$12" once the range lookahead failed, and stored $12/hr (backfill sample, 2026-09-22).


def _amt(num, k):
    v = _wage_num(num)
    return v * 1000 if (v is not None and k) else v
# Dollar-less annual ranges (2026-08-25): One Medical et al. write "The base
# salary range for this role is 253,200 - 302,700" with no $. Gated hard:
# "salary/pay/compensation range" wording within the same sentence, both
# numbers comma-grouped (so "range is 3 - 5 years" can never match), and
# _wage_pair's 25k-900k annual band still applies.
_WAGE_BARE_RANGE_RX = re.compile(
    r"(?:salary|pay|compensation)\s+range[^.\n$]{0,60}?"
    r"(\d{2,3},\d{3}(?:\.\d{1,2})?)\s*(?:-|–|—|to|through)\s*(\d{2,3},\d{3}(?:\.\d{1,2})?)", re.I)


def _wage_num(s):
    try:
        return float(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _wage_pair(lo, hi):
    """(lo, hi, unit) when the pair lands in a plausible band, else None."""
    if lo is None or hi is None:
        return None
    if lo > hi:
        lo, hi = hi, lo
    if 12 <= lo <= 350 and 12 <= hi <= 350:     # 350: contract therapists and physicians quote "up to $296 per hour" (SonderMind, 2026-09-22)
        return (lo, hi, "hour")
    if 25000 <= lo <= 900000 and 25000 <= hi <= 900000:
        return (lo, hi, "year")
    return None


# ── Posting-facts extraction (2026-08-24, Robert-approved chips) ────────────
# Closed-vocabulary extraction of certifications / education / shift /
# experience from posting text, with required-vs-preferred classified per
# sentence ("prefer" anywhere in the sentence => pref). Stored as ONE jsonb
# column (posting_facts) and rendered as the Requirements chip row in the
# job page key-facts box. Same honesty contract as the wage extractor:
# nothing extractable => null => the row doesn't render.
_FACT_CERTS = [
    ("BLS",   r"\bBLS\b|basic life support"),
    ("ACLS",  r"\bACLS\b|advanced cardiac life support|advanced cardiovascular life support"),
    ("PALS",  r"\bPALS\b|pediatric advanced life support"),
    ("NRP",   r"\bNRP\b|neonatal resuscitation"),
    ("TNCC",  r"\bTNCC\b"),
    ("CCRN",  r"\bCCRN\b"),
    ("CNOR",  r"\bCNOR\b"),
    ("CEN",   r"\bCEN\b"),
    ("CPR",   r"\bCPR\b"),
    ("ARRT",  r"\bARRT\b"),
    ("CST",   r"\bCST\b|certified surgical technologist"),
    ("RRT",   r"\bRRT\b|registered respiratory therapist"),
    ("NIHSS", r"\bNIHSS\b"),
    ("RN license",      r"\bRN license\b|registered nurse licen|current.{0,20}\bRN\b.{0,20}licen|licensure as a registered nurse"),
    ("Compact license", r"compact (?:state )?licen|multistate licen|\beNLC\b|\bNLC\b"),
    ("LPN license",     r"\bLPN licen|licensed practical nurse licen"),
]
_FACT_EDU = [
    ("BSN",       r"\bBSN\b|bachelor(?:'s|’s)? (?:of science )?(?:degree )?in nursing|baccalaureate.{0,15}nursing"),
    ("ADN/ASN",   r"\bADN\b|\bASN\b|associate(?:'s|’s)? degree in nursing|associate degree nursing"),
    ("MSN",       r"\bMSN\b|master(?:'s|’s)? (?:of science )?in nursing"),
    ("DNP",       r"\bDNP\b"),
    ("Nursing diploma", r"diploma (?:in|of) nursing|nursing diploma"),
    # 2026-09-22 (scoreboard): the list was nursing-only, so therapists, techs,
    # NPs and office roles got no education chip although the body named the
    # degree. Generic degrees come after the nursing ones; two chips at most.
    ("Doctorate",         r"doctora(?:te|l)\b|\bPhD\b|\bPharmD\b|\bDPT\b|\bPsyD\b|\bAuD\b|\bDScPT\b"),
    ("Master's degree",   r"master(?:'s|’s)?\s*(?:degree|of\s+[A-Za-z]|in\s+[A-Za-z]|[\[(]|(?:required|preferred|or|from)\b)|\bMHA\b|\bMPH\b|\bMBA\b|\bMSW\b"),
    ("Bachelor's degree", r"bachelor(?:'s|’s)?\s*(?:degree|of\s+[A-Za-z]|in\s+[A-Za-z]|[\[(]|(?:required|preferred|or|from)\b)|baccalaureate degree|\bB\.?S\.?\s+(?:degree|in\s)|\bB\.?A\.?\s+(?:degree|in\s)"),
    ("Associate degree",  r"associate(?:'s|’s)?\s*(?:degree|of\s+(?:applied\s+)?science|in\s+[A-Za-z]|[\[(]|(?:required|preferred)\b)|associate(?:'s|’s)\s+(?:or|from)\b|\bA\.?A\.?S\.?\s+degree"),
    ("HS diploma/GED",    r"high school (?:diploma|grad|graduate|equivalen)|\bH\.?S\.?\s+diploma|\bGED\b"),
]
# 2026-09-22 (scoreboard): "Full-Time Nights", "Shift: Nights", "Nights
# 6:45pm - 7:15am" and a p.m.-to-a.m. span are night shifts; the mirror
# forms are days. "may include nights and weekends" is not a shift.
_FACT_SHIFT = [
    ("Nights",   r"\bnight shift\b|\b7p\s*-?\s*7a\b|overnight|(?:(?:full|part)[\s-]*time|relief|\bprn|per diem)\s*[-–,]?\s*nights?\b|\bshift:\s*nights?\b|\bnights?\s*\(?\s*\d{1,2}(?::\d{2})?\s*[ap]\.?m|\b\d{1,2}(?::\d{2})?\s*p\.?m?\.?\s*(?:-|–|—|to)\s*\d{1,2}(?::\d{2})?\s*a\.?m\b"),
    ("Days",     r"\bday shift\b|\b7a\s*-?\s*7p\b|(?:(?:full|part)[\s-]*time|relief|\bprn|per diem)\s*[-–,]?\s*days?\b|\bshift:\s*days?\b|\bdays?\s*\(?\s*\d{1,2}(?::\d{2})?\s*[ap]\.?m|\b\d{1,2}(?::\d{2})?\s*a\.?m?\.?\s*(?:-|–|—|to)\s*\d{1,2}(?::\d{2})?\s*p\.?m\b"),
    ("Evenings", r"\bevening shift\b|\bevenings\b|(?:(?:full|part)[\s-]*time|relief|\bprn|per diem)\s*[-–,]?\s*evenings?\b|\bshift:\s*evenings?\b"),
    ("Rotating", r"\brotating shift|shift rotation"),
    ("Weekends", r"\bweekend(?:s| option| program| coverage| shifts?| rotation)\b|every other weekend"),
    ("3x12s",    r"\b3\s*x\s*12|three 12s|\b12[- ]hour shifts"),
    ("PRN",      r"\bPRN\b|per diem"),
]
_FACT_EXP_WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
                   "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
_FACT_EXP_RX = re.compile(
    r"(?:minimum of\s+|at least\s+)?(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s*"
    r"(?:\+|or more|plus)?\s*(?:-|to\s+)?\s*(\d+)?\s*years?(?:'|s)?\s*(?:of\s+)?"
    r"(?:[a-z ]{0,25}?)(experience|clinical|nursing|\bRN\b|acute care|bedside)", re.I)


# 2026-09-21 (owner, job page v2): schedule summary, hours per week, sign-on
# and relocation amounts, and a short benefits list, all from the posting text.
_DAY = r"(mon(?:day)?|tue(?:s|sday)?|wed(?:nesday)?|thu(?:r|rs|rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)"
_FACT_DAYS_RX = re.compile(_DAY + r"\s*(?:-|–|—|to|through|thru)\s*" + _DAY + r"\b", re.I)
_FACT_HOURS_RX = re.compile(r"\b(\d{1,2}(?:\.\d)?)\s*(?:hours?|hrs?)\s*(?:per|/|a|each)\s*(?:week|wk)\b|\b(\d{1,2})\s*(?:hours?|hrs?)/(?:week|wk)\b", re.I)
_FACT_SHIFTLEN_RX = re.compile(r"\b([2-6])\s*[x×]\s*(8|10|12)\b|\b(two|three|four|five|2|3|4|5)\s+(8|10|12)[- ]hour", re.I)
_NUM_WORDS = {"two": 2, "three": 3, "four": 4, "five": 5}
# The amount that sits right before the keyword wins ("$10,000 sign-on bonus
# ... and a $3,000 relocation package"), then an amount after it ("up to $15k").
_BONUS_RXS = [
    re.compile(r"\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k)?([^$\n.]{0,40}?)(?:sign[- ]?on|signing)", re.I),
    re.compile(r"(?:sign[- ]?on|signing)(?:\s+bonus)?([^$\n.]{0,60}?)\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k)?\b", re.I),
]
_RELO_RXS = [
    re.compile(r"\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k)?([^$\n.]{0,40}?)relocation", re.I),
    re.compile(r"relocation(?:\s+(?:bonus|assistance|package|allowance))?([^$\n.]{0,60}?)\$\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k)?\b", re.I),
]
_BENEFIT_ITEMS = [
    ("Medical, dental and vision", r"\b(?:medical|health)\b[^.]{0,40}\bdental\b|\bdental\b[^.]{0,30}\bvision\b"),
    ("Benefits from day one",      r"benefits?[^.]{0,20}(?:from|starting|on|beginning)\s+day\s+one|day[- ]one benefits|benefits? start(?:ing)? (?:on )?(?:your )?first day"),
    ("401(k) with match",          r"401\s?\(?k\)?[^.]{0,50}match|matching 401"),
    ("401(k)",                     r"401\s?\(?k\)?"),
    ("403(b)",                     r"403\s?\(?b\)?"),
    ("Paid time off",              r"paid time off|\bPTO\b|paid vacation"),
    ("Tuition assistance",         r"tuition (?:assistance|reimbursement|support)|student loan"),
    ("Paid parental leave",        r"parental leave|paid maternity|paid family leave"),
    ("Retirement plan",            r"\bpension\b|retirement (?:plan|savings)"),
    ("Life and disability insurance", r"life insurance|disability insurance|short[- ]term disability|long[- ]term disability"),
    ("Shift differentials",        r"shift differential|differential pay"),
    ("Wellness and mental health resources", r"wellness program|employee assistance program|\bEAP\b|mental health (?:resources|support|benefits)"),
    ("Childcare support",          r"child ?care"),
    ("Continuing education",       r"continuing education|\bCEU|professional development|\bCME\b"),
    # 2026-09-22 (Charlie Health lesson): items behavioral-health and clinic
    # employers list that the hospital vocabulary had no label for.
    ("Malpractice insurance",       r"malpractice|liability (?:insurance|coverage)"),
    ("License reimbursement",      r"licens\w*\s+(?:fees?\s+)?(?:reimburse|paid for)|reimburse\w*\s+(?:for\s+)?(?:new\s+|state\s+)?licens"),
    ("Wellness stipend",           r"wellness (?:stipend|allowance|reimbursement)"),
    ("Flexible scheduling",        r"flexible schedul|flexible hours|self[- ]scheduling"),
    ("Employee discounts",         r"employee discount|discount program"),
]
_BENEFIT_RXS = [(label, re.compile(rx, re.I)) for label, rx in _BENEFIT_ITEMS]


def _money_amount(num: str, k: str):
    try:
        v = float(num.replace(",", ""))
    except (TypeError, ValueError):
        return None
    if k:
        v *= 1000
    return int(v) if 500 <= v <= 100000 else None


_PAY_WORDS = re.compile(r"salary|compensation|\bpay\b|\bbase\b|annual|per year|/yr|per hour|/hr|hourly|wage|rate", re.I)


def _first_amount(text: str, rxs) -> int | None:
    """First plausible amount; the gap between the figure and the keyword
    (group 3, when the regex captures it) must not describe pay."""
    for rx in rxs:
        for m in rx.finditer(text):
            g = m.groups()
            if g[0] is not None and not re.match(r"^[\d,.]+$", g[0] or ""):
                gap, num, k = g[0], g[1], g[2]          # keyword first: (gap, amount, k)
            else:
                num, k, gap = g[0], g[1], (g[2] if len(g) > 2 else "")   # amount first: (amount, k, gap)
            if gap and _PAY_WORDS.search(gap):
                continue
            v = _money_amount(num, k)
            if v:
                return v
    return None


def _day3(d: str) -> str:
    return d[:3].title()


def _hours_for_type(t, job_type):
    """Hours per week; when the posting lists hours per employment type
    ("Part-Time: Minimum 12 hours/week" / "Full-Time: 40 hours/week") the
    line matching job_type wins, else the full-time line, else the first."""
    best = None
    for m in _FACT_HOURS_RX.finditer(t):
        try:
            h = float(m.group(1) or m.group(2))
        except (TypeError, ValueError):
            continue
        if not (4 <= h <= 84):
            continue
        score = _type_score(_line_type(t, m.start()), job_type)
        if best is None or score > best[0]:
            best = (score, h)
    return best[1] if best else None


def extract_schedule(text: str, shift_labels, job_type=None) -> tuple:
    """("Fri–Sun · 3×12h · Days", 36) style summary and hours per week."""
    t = text or ""
    pieces = []
    m = _FACT_DAYS_RX.search(t)
    if m:
        pieces.append(f"{_day3(m.group(1))}–{_day3(m.group(2))}")
    m = _FACT_SHIFTLEN_RX.search(t)
    if m:
        n = m.group(1) or m.group(3)
        n = _NUM_WORDS.get(str(n).lower(), n)
        length = m.group(2) or m.group(4)
        pieces.append(f"{n}×{length}h")
    hours = _hours_for_type(t, job_type)
    if hours is not None and not pieces:
        pieces.append(f"{int(hours) if hours == int(hours) else hours} hrs/wk")
    for lab in (shift_labels or []):
        if lab in ("Days", "Nights", "Evenings", "Rotating") and lab not in pieces:
            pieces.append(lab)
            break
    summary = " · ".join(pieces)[:48] if pieces else None
    return summary, (int(hours) if hours is not None and hours == int(hours) else hours)


def extract_benefits(text: str) -> list:
    """Closed-vocabulary benefit labels, eight at most (the page shows the
    first few; raised from five on 2026-09-22 so the stored row keeps more)."""
    t = (text or "")[:20000]
    out = []
    for label, rx in _BENEFIT_RXS:
        if rx.search(t):
            if label == "401(k)" and any(o.startswith("401(k)") for o in out):
                continue
            out.append(label)
        if len(out) == 8:
            break
    return out


# 2026-09-22 (Charlie Health lesson): the closed vocabulary caught five of a
# posting's eleven listed benefits and renamed the rest ("24/7 Employee
# Assistance Program" became "Wellness and mental health resources"). When a
# posting has its own "Benefits" list, keep those lines verbatim as well:
# the short lines under a benefits heading, until the next heading, a prose
# line or a colon-terminated label. Pay lines (a "$") are not benefits.
_SIGNON_MENTION_RX = re.compile(r"sign[- ]?(?:on|ing)\s+bonus", re.I)
_BENEFIT_HEAD_RX = re.compile(
    r"^\s*(?:(?:our|the|your|employee|full)\s+)?(?:benefits?(?:\s+(?:and|&)\s+perks)?|perks(?:\s+(?:and|&)\s+benefits)?|"
    r"what we offer|benefits?\s+(?:include|package|highlights|summary|offered)|compensation\s+(?:and|&)\s+benefits|"
    r"total rewards|why work (?:with|for) us)\s*[:!]?\s*$", re.I)
_BENEFIT_STOP_RX = re.compile(
    r"^\s*(?:about\b|responsibilit|qualifications?\b|requirements?\b|duties\b|what you|who you|position\b|"
    r"job (?:summary|description|duties)|education\b|experience\b|schedule\b|compensation\b|pay\b|salary\b|"
    r"equal opportunity|eeo\b|the (?:role|position)\b|apply\b|how to apply)", re.I)


def extract_benefit_lines(text: str) -> list:
    lines = (text or "")[:20000].split("\n")
    for i, l in enumerate(lines):
        if not _BENEFIT_HEAD_RX.match(l):
            continue
        out = []
        for raw in lines[i + 1:]:
            s = re.sub(r"^[\s•\-\*·–—>]+", "", raw).strip()
            if not s:
                continue
            if s.endswith(":") or len(s) > 90 or _BENEFIT_STOP_RX.match(s) or _BENEFIT_HEAD_RX.match(s):
                break
            if "$" in s or len(s) < 4:
                continue
            out.append(s.rstrip(".;, "))
            if len(out) == 12:
                break
        if len(out) >= 2:
            return out
    return []


_JOB_TYPE_CANON = [
    ("Full time", re.compile(r"full[\s_-]*time|\bFT\b|regular full|^full$", re.I)),
    ("Part time", re.compile(r"part[\s_-]*time|\bPT\b|^part$", re.I)),
    ("Per diem",  re.compile(r"per[\s_-]*diem|\bPRN\b|as[\s_-]needed|casual|\bpool\b|resource|flex", re.I)),
    ("Travel",    re.compile(r"\btravel", re.I)),
    ("Contract",  re.compile(r"contract|temporary|\btemp\b|seasonal|locum", re.I)),
    ("Intern",    re.compile(r"intern|residen|fellow|student|apprentice", re.I)),
    ("Volunteer", re.compile(r"volunteer", re.I)),
]


def canonical_job_type(raw, title: str = "") -> str:
    """"FULL_TIME", "Full-time", "Regular Full time" -> "Full time"; a blank
    value falls back to the title ("RN, PRN Nights" -> "Per diem"); anything
    unrecognised is kept as the ATS wrote it."""
    raw = (raw or "").strip()
    for label, rx in _JOB_TYPE_CANON:
        if raw and rx.search(raw):
            return label
    # 2026-09-22: AdventHealth's Findly feed puts the pay range in job_type
    # ("$16.58 - $26.53") and it was kept verbatim; every AdventHealth page
    # showed pay text as its employment type. Anything with a digit or a
    # symbol, or longer than a type name, is treated as blank (title fallback).
    if raw and (len(raw) > 24 or not re.fullmatch(r"[A-Za-z][A-Za-z /&-]*", raw)):
        raw = ""
    if raw and raw.lower() in ("regular", "standard", "employee", "staff"):
        raw = ""
    if not raw:
        for label, rx in _JOB_TYPE_CANON[:3]:
            if rx.search(title or ""):
                return label
        return ""
    return raw[:40]


# 2026-09-22 (owner win 4): six rows in ten carry no employment type from
# their feed, yet the body says it on a labelled line ("Job Type: Full-time",
# "Schedule: Full time", "Status: PRN", "Employment Type: Part Time"). Only
# labelled lines count, and a line naming two types ("Full-time or Part-time")
# is skipped; prose like "full-time employees receive" never qualifies.
_JOB_TYPE_LINE_RX = re.compile(
    r"(?im)^[ \t]*(?:job[ ]?type|employment[ ]?(?:type|status)|position[ ]?(?:type|status)|work[ ]?(?:type|schedule|status)|"
    r"schedule(?:d)?(?:[ ]hours)?|status|fte[ ]status|hours[ ]type|time[ ]type|shift[ ]type|category)"
    r"(?:[ \t]*[:\-\u2013][ \t]*|[ \t]+(?=(?:full|part|prn|per[ \t-]?diem)\b))([^\n]{2,60})$")   # "Schedule Full-time" needs no colon


def job_type_from_text(text) -> str:
    t = str(text or "")[:6000]
    for m in _JOB_TYPE_LINE_RX.finditer(t):
        value = m.group(1).strip()
        hits = [label for label, rx in _JOB_TYPE_CANON if rx.search(value)]
        # "Schedule Full-time Flexible availability": "flex" is a per-diem cue
        # in an ATS field, not in a sentence that names another type.
        if "Per diem" in hits and len(set(hits)) > 1 and not re.search(r"per[\s_-]*diem|\bPRN\b|as[\s_-]needed", value, re.I):
            hits = [h for h in hits if h != "Per diem"]
        if len(set(hits)) == 1:
            return hits[0]
    return ""


def extract_posting_facts(text, job_type=None):
    """{'certs': [[label, pref_bool]...], 'education': [...], 'shift': [...],
    'experience': [label, pref_bool] | None, 'schedule': str | None,
    'hours': number | None, 'signon': int | None, 'signon_offered': bool,
    'relocation': int | None, 'benefits': [str...], 'benefit_lines': [str...]}
    or None when nothing found. job_type (canonical) picks between figures a
    posting lists per employment type."""
    if not text:
        return None
    # Dotted abbreviations would split a sentence in two ("H.S. Diploma",
    # "B.S. in", "Ph.D."), so they lose their dots first (2026-09-22).
    t = re.sub(r"\b([A-Za-z])\.([A-Za-z])\.(?=[\s,;:)]|$)", r"\1\2", text[:12000])
    t = re.sub(r"\bPh\.D\.", "PhD", t)
    t = re.sub(r"([.!?;])(?=[A-Z(])", r"\1 ", t)
    sents = re.split(r"(?<=[.!?;])\s+", t)
    out = {"certs": [], "education": [], "shift": [], "experience": None}
    seen = set()
    edu_pos = {}
    offset = 0
    for s in sents:
        offset = t.find(s, offset)
        pref = bool(re.search(r"prefer", s, re.I))

        def item_pref(m):
            # "[Required] Associate [Preferred] Bachelor's [Preferred]": the
            # marker after the item decides; otherwise the sentence does,
            # unless "required" follows the item (2026-09-22).
            after = s[m.end():m.end() + 40]
            if re.search(r"prefer", after, re.I):
                return True
            return pref and not re.search(r"requir", after, re.I)

        for label, rx in _FACT_CERTS:
            if label not in seen:
                m = re.search(rx, s, re.I)
                if m:
                    seen.add(label)
                    out["certs"].append([label, item_pref(m)])
        for label, rx in _FACT_EDU:
            k = "e:" + label
            if k not in seen:
                m = re.search(rx, s, re.I)
                if m:
                    seen.add(k)
                    out["education"].append([label, item_pref(m)])
                    edu_pos[label] = offset + m.start()
        for label, rx in _FACT_SHIFT:
            k = "s:" + label
            if k not in seen and re.search(rx, s, re.I):
                seen.add(k)
                out["shift"].append([label, pref])
        if out["experience"] is None:
            m = _FACT_EXP_RX.search(s)
            if m:
                lo = _FACT_EXP_WORDS.get(m.group(1).lower())
                if lo is None:
                    try:
                        lo = int(m.group(1))
                    except ValueError:
                        lo = None
                if lo is not None and 0 < lo <= 15:
                    hi = m.group(2)
                    out["experience"] = [f"{lo}-{hi} years" if hi else f"{lo}+ years", pref]
    # BLS implies CPR — drop the redundant chip.
    if any(c[0] == "BLS" for c in out["certs"]):
        out["certs"] = [c for c in out["certs"] if c[0] != "CPR"]
    out["certs"] = out["certs"][:5]
    out["education"] = sorted(out["education"], key=lambda e: edu_pos.get(e[0], 0))[:2]   # the posting's own order, so the required level leads
    out["shift"] = out["shift"][:2]
    # 2026-09-21: schedule summary, hours, bonus amounts, benefits.
    out["schedule"], out["hours"] = extract_schedule(t, [x[0] for x in out["shift"]], job_type)
    out["signon"] = _first_amount(t, _BONUS_RXS)
    # 2026-09-22: "Signing Bonuses!" / "sign-on bonus available" with no
    # amount is still a fact worth a pill; the site shows it without a figure.
    out["signon_offered"] = bool(out["signon"] is None and _SIGNON_MENTION_RX.search(t))
    out["relocation"] = _first_amount(t, _RELO_RXS)
    out["benefits"] = extract_benefits(t)
    out["benefit_lines"] = extract_benefit_lines(text)
    if not (out["certs"] or out["education"] or out["shift"] or out["experience"]
            or out["schedule"] or out["hours"] or out["signon"] or out["signon_offered"]
            or out["relocation"] or out["benefits"] or out["benefit_lines"]):
        return None
    return out


def extract_posted_wage(text, job_type=None):
    """Best-effort (min, max, unit) from posting text, or None.

    Every dollar figure that survives _wage_is_noise is a candidate. A dollar
    range outranks a bare "salary range 70,000 - 80,000", which outranks a
    single figure; hourly outranks annual at the same rank (healthcare quotes
    hourly for the roles we pill); and when a posting prices several
    employment types ("Full-Time Salary: ... / Part-Time Rate: ...") the line
    matching job_type wins, or the full-time line when job_type is unknown.
    Earlier text breaks ties."""
    if not text:
        return None
    t = text[:12000]
    cands = []

    def consider(m, got, rank):
        if got:
            score = rank + (1 if got[2] == "hour" else 0) + _type_score(_line_type(t, m.start()), job_type)
            cands.append((score, -m.start(), got))          # ties: the earlier figure in the text

    for m in _WAGE_RANGE_RX.finditer(t):
        if _wage_is_noise(t, m.start(), m.end()):
            continue
        k = m.group(2) or m.group(4)                          # "$70-80K": one suffix for both ends
        lo, hi = _amt(m.group(1), k), _amt(m.group(3), k)
        m3 = _WAGE_THIRD_RX.match(t, m.end())                 # "min - midpoint - max": the maximum is the third figure
        end = m.end()
        if m3 and hi is not None:
            third = _amt(m3.group(1), k)
            if third is not None and third > hi:
                hi, end = third, m3.end()
        scale = _period_scale(t, end)                         # "... Biweekly" / "... per month" -> a year
        if scale != 1 and lo is not None and hi is not None:
            lo, hi = lo * scale, hi * scale
        consider(m, _wage_pair(lo, hi), 6)
    for m in _WAGE_BARE_RANGE_RX.finditer(t):
        if _wage_is_noise(t, m.start(), m.end()):
            continue
        got = _wage_pair(_wage_num(m.group(1)), _wage_num(m.group(2)))
        if got and got[2] == "year":
            consider(m, got, 3)
    for m in _WAGE_SINGLE_RX.finditer(t):
        if _wage_is_noise(t, m.start(), m.end()):
            continue
        v = _amt(m.group(1), m.group(2))
        tail = m.group(0).lower()
        if v is not None and re.search(r"month|/\s*mo\b", tail):
            v, unit = v * 12, "year"
        elif v is not None and re.search(r"weekly|pay period|two weeks|2 weeks", tail):
            v, unit = v * 26, "year"
        else:
            unit = "hour" if re.search(r"hour|hr", tail) else "year"
        got = _wage_pair(v, v)
        if got and got[2] == unit:
            consider(m, (v, v, unit), 0)
    for m in _WAGE_LABELLED_RX.finditer(t):
        fig = t.rfind("$", m.start(), m.start(1))
        if fig < 0 or _wage_is_noise(t, fig, m.end()):
            continue
        v = _amt(m.group(1), m.group(2))
        got = _wage_pair(v, v)
        if got:
            consider(m, got, -1)                              # below a figure that carries its own unit
    if not cands:
        return None
    cands.sort(key=lambda c: (-c[0], -c[1]))
    return cands[0][2]


def finalize_jobs(all_jobs: list) -> list[dict]:
    """The tail every push shares (run_all and hca_local_push.py), hoisted
    2026-09-10 (S-scraper-2): exact dedupe on (ats, post-alias system,
    job_id), the telehealth rules, the cross-tenant family dedupe
    (SYSTEM_FAMILIES) and normalize_job (which does the CMS fill)."""
    seen, unique = set(), []
    family_seen: dict[tuple, str] = {}
    family_dropped = 0
    for job in all_jobs:
        if not job.job_id or not job.title:
            continue
        canon = HOSPITAL_SYSTEM_ALIASES.get(job.hospital_system, job.hospital_system)
        key = f"{job.ats_platform}::{canon}::{job.job_id}"
        if key in seen:
            continue
        seen.add(key)
        # 2026-09-10 (T2): telehealth gates + state-from-title before
        # normalize_job; None means the row is dropped.
        job = apply_employer_rules(job)
        if job is None:
            continue
        row = normalize_job(job)
        fam = SYSTEM_FAMILIES.get(job.hospital_system)
        if fam:
            fac = "" if job.hospital_name in SYSTEM_FAMILIES else _cms_norm(row.get("hospital_name"))
            fkey = (fam, (row.get("title") or "").strip().lower(), fac,
                    (row.get("city") or "").strip().lower(), row.get("state") or "")
            src = family_seen.get(fkey)
            if src is not None and src != job.hospital_system:
                family_dropped += 1
                continue
            family_seen.setdefault(fkey, job.hospital_system)
        unique.append(row)
    if family_dropped:
        logger.info(f"Cross-tenant dedupe: dropped {family_dropped} rows listed twice by one family (SYSTEM_FAMILIES)")
    if _CMS_FILLS["n"]:
        logger.info(f"CMS lookup: filled state on {_CMS_FILLS['n']} rows")
    return unique


async def run_all() -> list[dict]:
    start = datetime.now()
    # 2026-09-10: CMS facility lookup for the blank-state fill (read-only,
    # no-op without credentials).
    await asyncio.to_thread(load_cms_lookup)
    # Two sessions: one with ssl=False for proxy-routed scrapers,
    # one with normal SSL for scrapers that connect directly (Taleo, SF, etc.)
    proxy_connector  = aiohttp.TCPConnector(limit=30, ssl=False)
    direct_connector = aiohttp.TCPConnector(limit=30)

    # max_line_size raised to 64 KB — Tenet's Set-Cookie headers exceed the 8 KB default
    async with aiohttp.ClientSession(connector=proxy_connector,  headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as proxy_session, \
               aiohttp.ClientSession(connector=direct_connector, headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as direct_session:
        ats_results = await asyncio.gather(
            run_workday(proxy_session),
            run_taleo(direct_session),       # direct — no ssl=False
            run_icims(proxy_session),
            run_jibe(proxy_session),         # Jibe/iCIMS Talent Cloud JSON API — Amedisys + Novant (added 2026-08-04)
            run_findly(proxy_session),           # Findly CWS legacy (Texas Health)
            run_findly_google(direct_session),   # Findly CWS Google CTS (AdventHealth) — direct (no proxy) for large JSON payloads
            run_greenhouse(proxy_session),
            run_smartrecruiters(proxy_session),
            run_concentra(proxy_session),    # Concentra — Sitecore SXA search (~1,260 jobs)
            run_lever(proxy_session),
            run_ashby(proxy_session),        # Ashby posting API: Talkiatry, SonderMind, Brightline, Wheel (added 2026-09-10)
            run_careerplug(proxy_session),   # CareerPlug HTML board: American Family Care franchise clinics (added 2026-09-10)
            run_usajobs(direct_session),
            run_adp(proxy_session),
            run_adp_cx(proxy_session),       # ADP Recruitment Management CX (myjobs.adp.com): NextCare urgent care (added 2026-09-14)
            run_patient_first(proxy_session),# Patient First WordPress REST via a rendered page: OFF by default, owner's call (added 2026-09-14)
            run_selectminds(proxy_session),
            run_recruitingcom(proxy_session),
            run_infor(proxy_session),
            run_phenom(proxy_session),
            run_bsw(direct_session),         # Baylor Scott & White — Phenom refineSearch direct API (Apr 29 2026)
            run_talentbrew(proxy_session),
            run_kaiser(direct_session),  # Kaiser Permanente — TalentBrew company 641, HTML pagination, direct (no proxy) needed since pages are ~1.9MB
            run_uhg(direct_session),     # UnitedHealth Group (Optum, LHC, MedExpress) — TalentBrew company 34088, 5,800+ jobs, ~7MB pages, direct fetch works
            run_enhabit(direct_session), # Enhabit Home Health — TalentBrew company 39891, HTML pagination (/results JSON is empty), ~1,621 jobs
            run_maxim(direct_session),   # Maxim Healthcare — TalentBrew company 49382, HTML pagination, ~1,826 jobs
            # ── New platforms from URL spreadsheet ──
            run_ukg(proxy_session),
            run_oracle(proxy_session),
            run_healthcaresource(proxy_session),
            run_tenet(proxy_session),
            run_trinity(proxy_session),
            run_uhs(proxy_session),
            run_lifepoint(proxy_session),
            run_kronos(proxy_session),
            run_neogov(proxy_session),  # NeoGov / governmentjobs.com county hospital boards (added 2026-09-10)
            run_applicantpro(proxy_session),
            run_csod(proxy_session),
            run_paycom(proxy_session),
            run_paycor(proxy_session),
            run_paylocity(proxy_session),  # Paylocity pageData boards: White Rock Medical Center (added 2026-09-10)
            run_workable(proxy_session),   # Workable v3 accounts API: Huntsville Memorial (added 2026-09-10)
            run_taleo_be(proxy_session),   # Taleo Business Edition RSS: Baptist SE Texas (added 2026-09-10)
            run_hcts(proxy_session),       # hctsportals.com HTML list: UMC El Paso (added 2026-09-10)
            run_tam(proxy_session),        # The Applicant Manager HTML board: Bayou Bend Health System (added 2026-09-15)
            run_preload(proxy_session),    # window.__PRELOAD_STATE__ career sites: Harris Health System (added 2026-09-22)
            run_hca(direct_session),    # HCA Healthcare — browserless per-state crawl via curl_cffi Firefox TLS (rebuilt 2026-07-28)
            run_houston_methodist(),    # Workday wd12/GTI — curl_cffi; wd12 edge 403s non-browser TLS (added 2026-07-28)
            run_oceans(),               # Oceans Behavioral — custom board at oceansjobboard.com via curl_cffi (added 2026-07-28)
            run_chs(proxy_session),
            run_atrium(proxy_session),  # Atrium Health — Coveo HTML pagination via residential proxy
            return_exceptions=True,
        )

    logger.info(_retry_summary())

    pw_jobs = await run_playwright_scrapers()

    all_jobs: list[Job] = pw_jobs[:]
    for r in ats_results:
        if isinstance(r, list):
            all_jobs.extend(r)

    unique = finalize_jobs(all_jobs)

    elapsed = (datetime.now() - start).seconds
    logger.info("=" * 55)
    logger.info(f"  TOTAL UNIQUE JOBS:  {len(unique):,}")
    logger.info(f"  SYSTEMS COVERED:    {len({j['hospital_system'] for j in unique})}")
    logger.info(f"  STATES COVERED:     {len({j['state'] for j in unique if j['state']})}")
    logger.info(f"  RUNTIME:            {elapsed}s")
    logger.info("=" * 55)
    return unique


def scrape() -> list[dict]:
    """Public entry point — scrapes hospital jobs AND travel jobs.

    Returns the hospital-jobs list (the caller's existing pipeline pushes it
    to Supabase `hospital_jobs`).  Travel jobs are written DIRECTLY to the
    `travel_jobs` Supabase table here — this avoids requiring any change to
    the existing Railway runner script. The travel scrape is wrapped in
    try/except so it can never break the hospital-jobs nightly run.
    """
    os.makedirs("logs", exist_ok=True)

    # Stamp this run's start so the upsert + deactivation pass agree on
    # "what was scraped this run vs what's stale". Same pattern as travel.
    run_started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    hospital_jobs = asyncio.run(run_all())

    # ── Hospital upsert + deactivation pass (added 2026-05-12) ────────────
    # Mirrors the travel side-flow. Without this, jobs that disappear from
    # a hospital's ATS stay is_active=true in our DB forever and accumulate
    # as dead links. The deactivation pass is per-system with a DEACT_MIN
    # safety threshold — a broken scraper module (Ascension, Hoag) can't
    # wipe its inventory by producing zero rows.
    try:
        _upsert_hospital_jobs_to_supabase(hospital_jobs, run_started_iso)
    except Exception as e:
        logger.warning(f"Hospital upsert/deactivation failed (non-fatal): {e}")

    # ── Travel jobs side-flow (separate table, self-contained) ────────────
    try:
        logger.info("[ TRAVEL ] Starting travel-jobs scrape (separate flow)...")
        travel_rows = asyncio.run(run_all_travel())
        # Save JSON dump for diagnostics + Railway log archival
        try:
            with open("travel_jobs_latest.json", "w") as f:
                json.dump(travel_rows, f, indent=2)
        except Exception as _je:
            logger.info(f"Travel: JSON dump failed (non-fatal): {_je}")
        # Direct upsert to Supabase if env vars present
        _upsert_travel_jobs_to_supabase(travel_rows)
    except Exception as e:
        logger.warning(f"Travel jobs scrape failed (non-fatal): {e}")

    return hospital_jobs


if __name__ == "__main__":
    jobs = scrape()
    with open("jobs_latest.json", "w") as f:
        json.dump(jobs, f, indent=2)
    print(f"Saved {len(jobs):,} jobs to jobs_latest.json")
