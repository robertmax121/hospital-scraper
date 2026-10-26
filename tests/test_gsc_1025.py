# 2026-10-25, Search Console fix plan, fix 3 (scraper part).
#
# Fix 3: Aya returned 0 rows every night from 10-20 15:01 UTC to 10-25
# (scraper_runs.meta.travel_sweep: yield 0, held below DEACT_MIN), so 10,001
# contracts kept their 10-19 stamp and stayed live. The adapter now retries a
# refused page through curl_cffi, follows the advertised count past the old
# 50-page ceiling, and records why a crawl is incomplete; an adapter that
# raises is logged and recorded; and the travel sweep retires the rows of a
# held source that nobody has seen for TRAVEL_STALE_DAYS over
# TRAVEL_STALE_MIN_RUNS runs (the stale backstop).
#
# Fakes only; no network, no database (Kaiser's fix 11: test_kaiser_state_1025.py).
import asyncio
import io
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest

import scheduler
import scraper
from conftest import FIXTURES

RUN = "2026-10-26T01:20:00.000000Z"


@pytest.fixture(autouse=True)
def _clean_travel_state(monkeypatch):
    scraper.TRAVEL_INCOMPLETE.clear()
    scraper.TRAVEL_FETCH_NOTES.clear()
    scraper.LAST_RUN_COUNTS.pop("travel_sweep", None)

    async def _nosleep(*_a, **_k):
        return None
    monkeypatch.setattr(scraper.asyncio, "sleep", _nosleep)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    yield
    scraper.TRAVEL_INCOMPLETE.clear()
    scraper.TRAVEL_FETCH_NOTES.clear()
    scraper.LAST_RUN_COUNTS.pop("travel_sweep", None)


# ── Aya adapter ──────────────────────────────────────────────────────────

def _aya_fixture():
    with open(os.path.join(FIXTURES, "gsc1025", "aya_search.json"), encoding="utf-8") as f:
        return json.load(f)


def _aya_catalog(n):
    tpl = _aya_fixture()["items"][1]
    return [{**tpl, "jobID": 9_000_000 + i} for i in range(n)]


class _AioResp:
    def __init__(self, status, data):
        self.status, self._data = status, data

    async def json(self, content_type="application/json"):
        if isinstance(self._data, Exception):
            raise self._data
        return self._data

    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


class FakeAya:
    """Aya's search API on both paths. direct: status code for the aiohttp
    path (200 serves the catalog). curl: True serves the catalog, an int is
    an HTTP failure. fail_offsets: offsets that fail on every path."""

    def __init__(self, monkeypatch, n=600, count=None, direct=200, curl=True, fail_offsets=()):
        self.items = _aya_catalog(n)
        self.count = n if count is None else count
        self.direct, self.curl, self.fail_offsets = direct, curl, set(fail_offsets)
        self.calls = []                     # ("direct"|"curl", offset)
        monkeypatch.setattr(scraper, "curl_requests", object())
        monkeypatch.setattr(scraper, "_curl_fetch", self._curl_fetch)

    def _page(self, params):
        off, lim = int(params["offset"]), int(params["limit"])
        return {"items": self.items[off:off + lim], "count": self.count}

    def get(self, url, params=None, headers=None, timeout=None):
        off = int(params["offset"])
        self.calls.append(("direct", off))
        assert url == scraper.AYA_API_BASE and headers["Origin"] == "https://www.ayahealthcare.com"
        if off in self.fail_offsets or self.direct != 200:
            return _AioResp(self.direct if self.direct != 200 else 503, {"error": "refused"})
        return _AioResp(200, self._page(params))

    def _curl_fetch(self, method, url, impersonate, timeout=60, params=None, headers=None):
        off = int(params["offset"])
        self.calls.append(("curl", off))
        assert method == "get" and impersonate == "chrome" and headers["Referer"].startswith("https://www.ayahealthcare.com/")
        if off in self.fail_offsets or self.curl is not True:
            raise RuntimeError(f"HTTP {self.curl if self.curl is not True else 503} direct")

        class R:
            def __init__(s, d): s._d = d
            def json(s): return s._d
        return R(self._page(params))


def _run_aya(fake):
    return asyncio.run(scraper.run_aya(fake))


def test_aya_fixture_parses_to_travel_rows():
    jobs, total = scraper._aya_parse(_aya_fixture())
    assert total == 15_257 and len(jobs) == 3
    j = jobs[1]
    assert j.agency_name == "Aya Healthcare" and j.agency_job_id == "3521596"
    assert (j.city, j.state, j.location) == ("Anchorage", "AK", "Anchorage, AK")
    assert j.title == "Registered Nurse - Labor and Delivery"
    assert j.weekly_pay_numeric == 4566.52 and j.weekly_pay_display == "$4566.52 to $4751.88 weekly"
    assert j.contract_weeks == 13 and j.hours_per_week == 48
    assert j.url == "https://www.ayahealthcare.com/travel-nursing-job/3521596"
    assert jobs[0].title == "Perfusionist"


def test_aya_healthy_direct_crawl_is_complete(monkeypatch):
    fake = FakeAya(monkeypatch, n=600)
    jobs = _run_aya(fake)
    assert len(jobs) == 600
    assert {c[0] for c in fake.calls} == {"direct"}
    assert scraper.TRAVEL_INCOMPLETE == {} and scraper.TRAVEL_FETCH_NOTES == {}


def test_aya_refused_direct_is_read_through_curl_and_stays_there(monkeypatch, caplog):
    fake = FakeAya(monkeypatch, n=600, direct=403)
    with caplog.at_level(logging.INFO):
        jobs = _run_aya(fake)
    assert len(jobs) == 600
    # page 0: direct refused once, curl answers; pages 1-2 go straight to curl
    assert fake.calls == [("direct", 0), ("curl", 0), ("curl", 250), ("curl", 500)]
    assert scraper.TRAVEL_INCOMPLETE == {}
    assert scraper.TRAVEL_FETCH_NOTES["ayahealthcare.com"] == (
        "read through curl_cffi from offset 0; direct read refused (direct HTTP 403)")
    assert any("600 unique travel listings of 600 advertised (read curl_cffi)" in m for m in caplog.messages)


def test_aya_refused_on_every_path_yields_nothing_and_says_why(monkeypatch):
    fake = FakeAya(monkeypatch, n=600, direct=403, curl=403)
    assert _run_aya(fake) == []
    assert len(fake.calls) == 2 * scraper.AYA_PAGE_ATTEMPTS == 6
    why = scraper.TRAVEL_INCOMPLETE["ayahealthcare.com"]
    assert why == ("page 0 failed (direct HTTP 403; curl_cffi RuntimeError: HTTP 403 direct); "
                   "0 of 0 advertised")


def test_aya_reads_past_the_old_50_page_ceiling(monkeypatch):
    # 10-18 / 10-19: 12,500 and 12,499 rows, the 50 x 250 ceiling, of ~15,000.
    fake = FakeAya(monkeypatch, n=15_257)
    jobs = _run_aya(fake)
    assert len(jobs) == 15_257
    assert len(fake.calls) == 62
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_aya_page_ceiling_marks_the_crawl_incomplete(monkeypatch):
    monkeypatch.setattr(scraper, "AYA_MAX_PAGES", 2)
    fake = FakeAya(monkeypatch, n=600)
    assert len(_run_aya(fake)) == 500
    assert scraper.TRAVEL_INCOMPLETE["ayahealthcare.com"] == "hit the 2-page ceiling (500 of 600)"


def test_aya_failed_page_mid_crawl_holds_even_when_most_rows_came(monkeypatch):
    fake = FakeAya(monkeypatch, n=5_000, fail_offsets={4_750})
    jobs = _run_aya(fake)
    assert len(jobs) == 4_750                      # 95% read, but a page is known unread
    why = scraper.TRAVEL_INCOMPLETE["ayahealthcare.com"]
    assert why.startswith("page 19 failed (direct HTTP 503; curl_cffi RuntimeError") and why.endswith(
        "4,750 of 5,000 advertised")


def test_aya_short_list_against_the_count_is_incomplete(monkeypatch):
    fake = FakeAya(monkeypatch, n=400, count=1_000)   # list ends at 400 of an advertised 1,000
    assert len(_run_aya(fake)) == 400
    assert scraper.TRAVEL_INCOMPLETE["ayahealthcare.com"] == "page 2 came back empty; 400 of 1,000 advertised"


def test_aya_last_page_shrinking_within_tolerance_is_complete(monkeypatch):
    fake = FakeAya(monkeypatch, n=990, count=1_000)   # 99%: contracts closed while paging
    assert len(_run_aya(fake)) == 990
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_an_adapter_that_raises_is_logged_and_held(monkeypatch, caplog):
    async def ok_vivian(_s): return []
    async def boom(_s): raise ValueError("bad payload")
    async def empty(): return []
    monkeypatch.setattr(scraper, "run_vivian", ok_vivian)
    monkeypatch.setattr(scraper, "run_aya", boom)
    monkeypatch.setattr(scraper, "run_nomad", empty)
    monkeypatch.setattr(scraper, "run_amn", empty)
    with caplog.at_level(logging.WARNING):
        rows = asyncio.run(scraper.run_all_travel())
    assert rows == []
    assert scraper.TRAVEL_INCOMPLETE == {"ayahealthcare.com": "adapter raised ValueError: bad payload"}
    assert scraper.TRAVEL_FETCH_NOTES == {"ayahealthcare.com": "adapter raised ValueError: bad payload"}
    assert any("Travel: ayahealthcare.com adapter raised ValueError" in m for m in caplog.messages)


# ── Stale backstop ───────────────────────────────────────────────────────

def _ts(s):
    return scraper._travel_iso_utc(s)


class FakeDB:
    """travel_jobs + scraper_runs over PostgREST.
    table: id -> [url, scraped_at, is_active]; history: scraper_runs rows
    (newest first) as {"started_at", "travel"}; baselines: domain -> count
    of active rows at PREV for the sweep guard (as in test_travel_sweep_guard)."""

    PREV = "2026-10-25T01:20:00.000000+00:00"

    def __init__(self, monkeypatch, table, history, baselines=None, history_fails=False):
        self.table, self.history, self.baselines = table, history, baselines or {}
        self.history_fails = history_fails
        self.log = []
        monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setattr(urllib.request, "urlopen", self.urlopen)
        monkeypatch.setattr(scraper, "update_travel_site_stats", lambda: 0)

    def urlopen(self, rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)
        path = urllib.parse.urlsplit(u).path
        if m == "POST":
            for r in json.loads(rq.data.decode()):
                self.log.append(("upsert", r["agency_job_id"]))
            return _UResp()
        if path.endswith("/scraper_runs"):
            self.log.append(("history", u))
            if self.history_fails:
                raise urllib.error.HTTPError(u, 500, "err", {}, io.BytesIO(b"timeout"))
            assert q["ats_platform"] == ["eq.nightly"] and q["order"] == ["started_at.desc"]
            assert q["select"] == ["started_at,travel:meta->travel_sweep"]
            before = _ts(q["started_at"][0][3:])
            rows = [h for h in self.history if _ts(h["started_at"]) < before]
            return _UResp(body=json.dumps(rows[:int(q["limit"][0])]).encode())
        if m == "PATCH" and "id" in q and q["id"][0].startswith("in.("):
            cut = _ts(q["scraped_at"][0][3:])
            ids = [int(x) for x in q["id"][0][4:-1].split(",")]
            n = 0
            for i in ids:
                row = self.table.get(i)
                if row and row[2] and _ts(row[1]) < cut:
                    row[2] = False
                    n += 1
            self.log.append(("stale_patch", tuple(ids)))
            return _UResp(f"*/{n}")
        if m == "PATCH":
            self.log.append(("sweep_patch", q["url"][0][5:].strip("*")))
            return _UResp("*/0")
        if "order" in q and q["order"] == ["id.desc"]:
            return _UResp(body=b'[{"id": 4000}]')
        if q.get("select") == ["id"] and q.get("order") == ["id.asc"]:
            dom = q["url"][0][5:].strip("*")
            cut = _ts(q["scraped_at"][0][3:])
            last = int(q["id"][0][3:])
            lim = int(q["limit"][0])
            ids = sorted(i for i, (url, st, act) in self.table.items()
                         if act and dom in url and _ts(st) < cut and i > last)[:lim]
            self.log.append(("stale_read", dom))
            return _UResp(body=json.dumps([{"id": i} for i in ids]).encode())
        if q.get("order") == ["scraped_at.desc"]:
            dom = q["url"][0][5:].strip("*")
            b = self.baselines.get(dom, 0)
            return _UResp(body=b"[]" if not b else json.dumps([{"scraped_at": self.PREV}]).encode())
        if "scraped_at" in q and q["scraped_at"][0].startswith("eq."):
            dom = q["url"][0][5:].strip("*")
            return _UResp(f"0-0/{self.baselines.get(dom, 0)}")
        raise AssertionError(f"unexpected {m} {u}")

    def kinds(self, k):
        return [e for e in self.log if e[0] == k]


class _UResp:
    def __init__(self, content_range="", body=b""):
        self.headers = {"Content-Range": content_range}
        self._body = body

    def read(self): return self._body
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _hist(*starts, skipped=()):
    return [{"started_at": s, "travel": ({"yield": {}, "skipped": "x"} if s in skipped else {"yield": {}})}
            for s in starts]


NIGHTLY = _hist("2026-10-25T00:02:13.42008+00:00", "2026-10-24T00:05:03.129893+00:00",
                "2026-10-23T00:03:14.315367+00:00", "2026-10-22T00:03:03.897345+00:00")


def _vivian_rows(n):
    return [{"agency_name": "Vivian", "agency_job_id": f"v{i}", "title": "RN",
             "url": f"https://www.vivian.com/job/{i}/", "is_active": True, "scraped_at": RUN}
            for i in range(n)]


def _aya_table(n, stamp="2026-10-19T01:18:04.233143+00:00", start=1):
    return {start + i: [f"https://www.ayahealthcare.com/travel-nursing-job/{i}", stamp, True] for i in range(n)}


def test_cutoff_is_the_older_of_three_days_and_two_earlier_runs(monkeypatch):
    FakeDB(monkeypatch, {}, NIGHTLY)
    cut, why = scraper._travel_stale_cutoff("https://fake.invalid", "k", RUN)
    # nightly runs: 3 days before 10-26 01:20 is 10-23 01:20, older than the
    # 10-24 00:05 start of the second earlier run, so the age rule decides.
    assert (cut, why) == ("2026-10-23T01:20:00.000000+00:00", "")


def test_after_an_outage_the_run_count_decides(monkeypatch):
    # Last runs 10-16 and 10-15; tonight (10-26) is the first night back.
    FakeDB(monkeypatch, {}, _hist("2026-10-16T00:02:00+00:00", "2026-10-15T00:02:00+00:00"))
    cut, _ = scraper._travel_stale_cutoff("https://fake.invalid", "k", RUN)
    assert cut == "2026-10-15T00:02:00.000000+00:00"   # rows seen on 10-16 stay


def test_skipped_and_unclosed_runs_do_not_count(monkeypatch):
    hist = ([{"started_at": "2026-10-26T00:02:00+00:00", "travel": None}]       # tonight's open row
            + _hist("2026-10-25T00:02:00+00:00", "2026-10-24T00:02:00+00:00", "2026-10-23T00:02:00+00:00",
                    skipped=("2026-10-24T00:02:00+00:00",)))
    FakeDB(monkeypatch, {}, hist)
    cut, _ = scraper._travel_stale_cutoff("https://fake.invalid", "k", RUN)
    assert cut == "2026-10-23T00:02:00.000000+00:00"


def test_too_little_history_or_an_unreadable_one_holds(monkeypatch):
    FakeDB(monkeypatch, {}, _hist("2026-10-25T00:02:00+00:00"))
    assert scraper._travel_stale_cutoff("https://fake.invalid", "k", RUN) == (
        None, "1 earlier travel run(s) on record, needs 2")
    FakeDB(monkeypatch, {}, NIGHTLY, history_fails=True)
    cut, why = scraper._travel_stale_cutoff("https://fake.invalid", "k", RUN)
    assert cut is None and why.startswith("run history unreadable (HTTPError")


def test_the_aya_case_held_source_loses_rows_unseen_since_10_19(monkeypatch, caplog):
    table = _aya_table(2_345)
    table[99_001] = ["https://www.ayahealthcare.com/travel-nursing-job/new", "2026-10-24T01:20:00+00:00", True]
    table[99_002] = ["https://www.vivian.com/job/old", "2026-10-19T01:18:04+00:00", True]   # swept source
    db = FakeDB(monkeypatch, table, NIGHTLY, baselines={"vivian.com": 1_000})
    with caplog.at_level(logging.INFO):
        sent = scraper._upsert_travel_jobs_to_supabase(_vivian_rows(1_000))
    assert sent == 1_000
    ts = scraper.LAST_RUN_COUNTS["travel_sweep"]
    assert ts["swept"] == ["vivian.com"]
    assert ts["held"]["ayahealthcare.com"] == "yield 0 below DEACT_MIN=100"
    assert ts["stale"] == {"cutoff": "2026-10-23T01:20:00.000000+00:00",
                           "retired": {"ayahealthcare.com": 2_345}, "total": 2_345}
    assert sum(1 for i, r in table.items() if "aya" in r[0] and not r[2]) == 2_345
    assert table[99_001][2] is True                       # seen two days ago: stays
    assert table[99_002][2] is True                       # vivian.com is the sweep's, not the backstop's
    # ids read in pages of 1,000 and retired 200 at a time
    assert len(db.kinds("stale_patch")) == 12
    assert all(len(e[1]) <= scraper.TRAVEL_STALE_PATCH for e in db.kinds("stale_patch"))
    assert {e[1] for e in db.kinds("stale_read")} == {"ayahealthcare.com", "nomadhealth.com", "amnhealthcare.com"}
    json.dumps(ts)
    assert scheduler.travel_sweep_notes(scraper.LAST_RUN_COUNTS) == (
        "; travel stale backstop retired 2,345 (ayahealthcare.com 2,345)")
    assert any("Travel stale backstop ayahealthcare.com: 2,345 of 2,345 active rows" in m for m in caplog.messages)


def test_one_failed_night_after_an_outage_retires_nothing(monkeypatch):
    table = _aya_table(500, stamp="2026-10-16T01:18:00+00:00")
    db = FakeDB(monkeypatch, table, _hist("2026-10-16T00:02:00+00:00", "2026-10-15T00:02:00+00:00"),
                baselines={"vivian.com": 1_000})
    scraper._upsert_travel_jobs_to_supabase(_vivian_rows(1_000))
    assert all(r[2] for r in table.values())
    assert db.kinds("stale_patch") == []
    assert scraper.LAST_RUN_COUNTS["travel_sweep"]["stale"]["total"] == 0


def test_backstop_holds_when_history_is_unreadable(monkeypatch):
    table = _aya_table(50)
    db = FakeDB(monkeypatch, table, NIGHTLY, baselines={"vivian.com": 1_000}, history_fails=True)
    scraper._upsert_travel_jobs_to_supabase(_vivian_rows(1_000))
    assert all(r[2] for r in table.values()) and db.kinds("stale_read") == []
    stale = scraper.LAST_RUN_COUNTS["travel_sweep"]["stale"]
    assert stale["held"].startswith("run history unreadable")


def test_no_backstop_when_the_upsert_did_not_fully_land(monkeypatch):
    table = _aya_table(50)
    db = FakeDB(monkeypatch, table, NIGHTLY, baselines={"vivian.com": 1_000})
    real = db.urlopen

    def failing_post(rq, timeout=None):
        if rq.get_method() == "POST" and json.loads(rq.data.decode())[0]["agency_job_id"] == "v500":
            raise urllib.error.HTTPError(rq.full_url, 500, "err", {}, io.BytesIO(b"x"))
        return real(rq, timeout)
    monkeypatch.setattr(urllib.request, "urlopen", failing_post)
    scraper._upsert_travel_jobs_to_supabase(_vivian_rows(1_000))
    assert all(r[2] for r in table.values())
    assert db.kinds("history") == [] and "stale" not in scraper.LAST_RUN_COUNTS["travel_sweep"]


def test_quiet_source_with_a_reason_shows_in_the_notes():
    counts = {"travel_sweep": {"yield": {"vivian.com": 40_000, "ayahealthcare.com": 0, "nomadhealth.com": 0},
                               "held": {"ayahealthcare.com": "yield 0 below DEACT_MIN=100",
                                        "nomadhealth.com": "yield 0 below DEACT_MIN=100"},
                               "incomplete": {"ayahealthcare.com": "page 0 failed (direct HTTP 403); 0 of 0 advertised"},
                               "stale": {"cutoff": "x", "retired": {"ayahealthcare.com": 0}, "total": 0}}}
    assert scheduler.travel_sweep_notes(counts) == (
        "; travel source returned nothing ayahealthcare.com (page 0 failed (direct HTTP 403); 0 of 0 advertised)")
