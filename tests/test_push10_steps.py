"""Push 10 / steps (2026-10-07): the post-scrape passes and run hygiene.
No network, no database: the two validators are driven with fake aiohttp
sessions, database.get_stats with a fake service client."""
import asyncio
import logging

import pytest

import database
import scraper as S
import validate_front_pool_urls as F
import validate_travel_urls as T


# ── fakes ──────────────────────────────────────────────────────────────────

class _Resp:
    def __init__(self, status, text="", headers=None, data=None):
        self.status, self._text, self.headers, self._data = status, text, headers or {}, data

    async def text(self):
        return self._text

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    """Answers by (method, url) from a script; records every call."""
    def __init__(self, script):
        self.script, self.calls = script, []

    def _answer(self, method, url, **kw):
        self.calls.append((method, url, kw))
        ans = self.script(method, url, kw)
        if isinstance(ans, Exception):
            raise ans
        return ans

    def request(self, method, url, **kw):
        return self._answer(method.upper(), url, **kw)

    def get(self, url, **kw):
        return self._answer("GET", url, **kw)

    def head(self, url, **kw):
        return self._answer("HEAD", url, **kw)

    def patch(self, url, **kw):
        return self._answer("PATCH", url, **kw)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fast_sleep(monkeypatch):
    """asyncio.sleep and time.sleep take no time (the retry backoffs)."""
    import time as _time
    orig = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda *_a, **_k: orig(0))
    monkeypatch.setattr(_time, "sleep", lambda *_a, **_k: None)


# ── item 1: front-pool checker ─────────────────────────────────────────────

def test_head_404_is_asked_again_with_get_and_a_200_wins(fast_sleep):
    url = "https://recruiting.paylocity.com/Recruiting/Jobs/Details/4455803"

    def script(method, u, kw):
        return _Resp(404) if method == "HEAD" else _Resp(200)
    s = _Session(script)
    assert _run(F._status(s, url)) == 200
    assert [m for m, _u, _k in s.calls] == ["HEAD", "GET"]


def test_head_404_confirmed_by_get_404_is_dead():
    s = _Session(lambda method, u, kw: _Resp(404))
    assert _run(F._status(s, "https://jobs.example.com/x/1")) == 404
    assert [m for m, _u, _k in s.calls] == ["HEAD", "GET"]


def test_head_200_pays_no_get():
    s = _Session(lambda method, u, kw: _Resp(200))
    assert _run(F._status(s, "https://jobs.example.com/x/1")) == 200
    assert [m for m, _u, _k in s.calls] == ["HEAD"]


def test_get_again_codes_cover_the_paylocity_case():
    assert {404, 410, 403, 405} <= F.GET_AGAIN_CODES


def test_redirect_to_job_not_found_is_dead(fast_sleep):
    """Paylocity on a closed posting: HEAD 404, GET 302 -> JobNotFound (200)."""
    url = "https://recruiting.paylocity.com/Recruiting/Jobs/Details/1"

    def script(method, u, kw):
        if method == "HEAD":
            return _Resp(404)
        if u == url:
            return _Resp(302, headers={"Location": "/Recruiting/Jobs/JobNotFound"})
        return _Resp(200)
    s = _Session(script)
    assert _run(F._status(s, url)) == 404
    assert [m for m, _u, _k in s.calls] == ["HEAD", "GET"]     # the not-found page is never fetched
    assert F._dead_redirect("https://recruiting.paylocity.com/Recruiting/Jobs/JobNotFound?x=1")
    assert not F._dead_redirect("https://recruiting.paylocity.com/Recruiting/Jobs/Details/2")


def test_other_redirects_are_still_followed():
    url = "https://jobs.example.com/old/1"

    def script(method, u, kw):
        if u == url:
            return _Resp(301, headers={"Location": "https://jobs.example.com/new/1"})
        return _Resp(200)
    s = _Session(script)
    assert _run(F._status(s, url)) == 200
    assert [u for _m, u, _k in s.calls] == [url, "https://jobs.example.com/new/1"]


def test_healthcaresource_fragment_routes_are_unverifiable():
    assert F._unverifiable("https://pm.healthcaresource.com/cs/lompocvmc#/job/4530") == "fragment route"
    assert F._unverifiable("https://pm.healthcaresource.com/cs/rwjbh#/job/12345?x=1") == "fragment route"
    assert F._unverifiable("https://recruiting.paylocity.com/Recruiting/Jobs/Details/4455803") == ""
    assert F._unverifiable("https://pm.healthcaresource.com/cs/pvh") == ""      # the board, not a job
    assert F._unverifiable(None) == "" and F._unverifiable("") == ""


def test_pool_queries_end_in_id_desc():
    assert F.STATE_POOL_ORDER == "apply_verified.desc,scraped_at.desc,id.desc"
    assert F.NATIONAL_POOL_ORDER.endswith(",id.desc")


def test_tally_counts_indeterminate_by_status():
    results = [(1, "A", 200), (2, "A", 404), (3, "B", 403), (4, "B", 0), (5, "B", 0), (6, "C", 301)]
    live, dead, by_sys, indet = F._tally(results)
    assert live == [1] and dead == [(2, "A")]
    assert by_sys == {"A": [2, 1], "B": [3, 0], "C": [1, 0]}
    assert dict(indet) == {403: 1, 0: 2, 301: 1}


def test_patch_splits_on_57014_and_lands_every_row(fast_sleep):
    assert F.PATCH_BATCH == 100

    def script(method, url, kw):
        n = url.split("id=in.(")[1].rstrip(")").count(",") + 1
        if n > F.PATCH_MIN_BATCH:
            return _Resp(500, text='{"code":"57014","message":"canceling statement due to statement timeout"}')
        return _Resp(204)
    s = _Session(script)
    ids = list(range(1, 251))
    sent = _run(F._patch(s, "https://db.example", {"apikey": "k"}, ids, {"last_dead_check_at": "now"}))
    assert sent == 250
    sizes = [u.split("id=in.(")[1].rstrip(")").count(",") + 1 for _m, u, _k in s.calls]
    assert max(sizes) == 100 and {100, 50, 25} <= set(sizes)
    assert all(sz in (100, 50, 25) for sz in sizes)


def test_patch_skips_a_piece_that_keeps_failing_and_continues(fast_sleep):
    def script(method, url, kw):
        ids = url.split("id=in.(")[1].rstrip(")").split(",")
        return _Resp(500, text="57014") if "7" in ids else _Resp(204)
    s = _Session(script)
    sent = _run(F._patch(s, "https://db.example", {}, list(range(1, 201)), {"x": 1}))
    # one 25-row piece (ids 1-25, holding id 7) is skipped after its retries
    assert sent == 175


def test_patch_does_not_retry_a_rejected_statement(fast_sleep):
    s = _Session(lambda m, u, kw: _Resp(400, text='{"code":"22P02"}'))
    sent = _run(F._patch(s, "https://db.example", {}, list(range(1, 11)), {"x": 1}))
    assert sent == 0 and len(s.calls) == 1


# ── item 2: travel validator ───────────────────────────────────────────────

def test_travel_fetch_pages_by_id_cursor_past_the_1000_row_cap(fast_sleep):
    # 2,133 active rows inside one 20,000-id window: the old window fetch
    # returned 1,000 of them; the cursor returns all.
    table = [{"id": 1_880_000 + i, "url": f"https://www.vivian.com/jobs/{i}"} for i in range(2133)]

    def script(method, url, kw):
        assert "id=gt." in url and "order=id.asc" in url and f"limit={T.PAGE_SIZE}" in url
        last = int(url.split("id=gt.")[1].split("&")[0])
        page = [r for r in table if r["id"] > last][:T.PAGE_SIZE]
        return _Resp(200, data=page)
    s = _Session(script)
    rows = _run(T._fetch_active(s, "https://db.example", "k"))
    assert len(rows) == 2133 and rows[-1]["id"] == table[-1]["id"]
    assert len(s.calls) == 4          # 1000, 1000, 133, then the empty page ends it


def test_travel_fetch_keeps_rows_fetched_before_a_dead_page(fast_sleep):
    calls = {"n": 0}

    def script(method, url, kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp(200, data=[{"id": i, "url": "https://www.vivian.com/jobs/x"} for i in range(1, 1001)])
        return _Resp(500, text="57014")
    rows = _run(T._fetch_active(_Session(script), "https://db.example", "k"))
    assert len(rows) == 1000


def test_travel_allowlist_has_nomad_and_amn():
    assert {"nomadhealth.com", "www.amnhealthcare.com"} <= T.ALLOWED_HOSTS
    assert T._is_allowed_url("https://nomadhealth.com/jobs/abc")
    assert T._is_allowed_url("https://www.amnhealthcare.com/jobs/travel-nursing/x/123")
    assert not T._is_allowed_url("https://169.254.169.254/latest/meta-data")
    assert not T._is_allowed_url("http://www.vivian.com/jobs/x")


def test_travel_head_check_resolves_a_relative_redirect(fast_sleep):
    """AMN: HEAD 301 with Location '/job-details/<id>/<slug>/' (relative)."""
    url = "https://www.amnhealthcare.com/job-details/3597735/rapid-city-sd-rrt"
    target = url + "/"

    def script(method, u, kw):
        assert method == "HEAD"
        if u == url:
            return _Resp(301, headers={"Location": "/job-details/3597735/rapid-city-sd-rrt/"})
        return _Resp(200) if u == target else _Resp(500)
    s = _Session(script)
    rid, code = _run(T._head_check(s, asyncio.Semaphore(2), {"id": 9, "url": url}))
    assert (rid, code) == (9, 200)
    assert [u for _m, u, _k in s.calls] == [url, target]


def test_travel_head_check_bounces_an_off_list_redirect(fast_sleep):
    s = _Session(lambda m, u, kw: _Resp(302, headers={"Location": "https://evil.example/x"}))
    rid, code = _run(T._head_check(s, asyncio.Semaphore(2), {"id": 3, "url": "https://www.vivian.com/jobs/a"}))
    assert (rid, code) == (3, 0) and len(s.calls) == 1


def test_travel_rows_off_the_allowlist_are_split_out_and_counted():
    rows = [{"id": 1, "url": "https://www.vivian.com/jobs/a"},
            {"id": 2, "url": "https://jobs.example.org/b"},
            {"id": 3, "url": "https://jobs.example.org/c"},
            {"id": 4, "url": ""}]
    allowed, skipped = T._split_allowed(rows)
    assert [r["id"] for r in allowed] == [1]
    assert dict(skipped) == {"jobs.example.org": 2, "(no host)": 1}


def test_travel_rows_are_interleaved_by_host():
    rows = ([{"id": i, "url": f"https://www.vivian.com/jobs/{i}"} for i in range(3)]
            + [{"id": 10 + i, "url": f"https://nomadhealth.com/jobs/{i}"} for i in range(2)]
            + [{"id": 20, "url": "https://www.amnhealthcare.com/jobs/0"}])
    out = [r["id"] for r in T._interleave_by_host(rows)]
    assert out == [0, 10, 20, 1, 11, 2]
    assert T.CONCURRENCY >= 100 and T.PER_HOST <= T.CONCURRENCY


# ── item 3: the active total without a scan ────────────────────────────────

def test_active_total_is_layer4_arithmetic():
    layer4 = {"active_before": 350430, "deactivated": 228}
    assert database.active_total_after_passes(layer4, {"retired": 31}) == 350171
    assert database.active_total_after_passes(layer4, {}) == 350430 - 228
    assert database.active_total_after_passes(layer4, None) == 350430 - 228


def test_active_total_is_unknown_when_layer4_aborted_or_incomplete():
    assert database.active_total_after_passes({"active_before": 5, "deactivated": 0, "aborted": True}, {}) is None
    assert database.active_total_after_passes({}, {}) is None
    assert database.active_total_after_passes(None, {"retired": 1}) is None
    assert database.active_total_after_passes({"active_before": "350430", "deactivated": 0}, {}) is None


class _FakeTable:
    def __init__(self, log, name, fail_select=False):
        self.log, self.name, self.fail_select = log, name, fail_select
        self._op = None

    def upsert(self, body):
        self.log.append((self.name, "upsert", body)); self._op = "w"; return self

    def insert(self, body):
        self.log.append((self.name, "insert", body)); self._op = "i"; return self

    def update(self, body):
        self.log.append((self.name, "update", body)); self._op = "u"; return self

    def eq(self, col, val):
        self.log.append((self.name, "eq", (col, val))); return self

    def select(self, *_a, **_k):
        self._op = "s"; return self

    def gt(self, *_a): return self
    def order(self, *_a, **_k): return self
    def limit(self, *_a): return self

    def execute(self):
        if self._op == "s" and self.fail_select:
            raise TimeoutError("The read operation timed out")
        class R: pass
        r = R()
        r.data = [{"id": 77}] if self._op == "i" else []
        return r


class _FakeClient:
    def __init__(self, log, fail_select=False):
        self.log, self.fail_select = log, fail_select

    def table(self, name):
        return _FakeTable(self.log, name, self.fail_select)

    def rpc(self, name, params):
        self.log.append(("rpc", name, params))
        class Q:
            def execute(_s):
                class R: data = {"added": 1}
                return R()
        return Q()


def test_get_stats_writes_the_given_total_and_refreshes_the_cohort(monkeypatch):
    log = []
    monkeypatch.setattr(database, "service_client", lambda: _FakeClient(log))
    monkeypatch.setattr(database, "client", lambda: (_ for _ in ()).throw(AssertionError("no scan")))
    out = database.get_stats(350171)
    assert out["total_active_jobs"] == 350171
    writes = [b for t, op, b in log if t == "site_stats" and op == "upsert"]
    assert writes and writes[0]["id"] == 1 and writes[0]["total_active_jobs"] == 350171
    assert ("rpc", "refresh_hospital_sitemap_cohort", {}) in log


def test_get_stats_unknown_total_writes_nothing_but_still_calls_the_cohort(monkeypatch, fast_sleep):
    log = []
    monkeypatch.setattr(database, "service_client", lambda: _FakeClient(log, fail_select=True))
    out = database.get_stats(None)
    assert out["total_active_jobs"] is None
    assert not [1 for t, op, _b in log if t == "site_stats"]
    assert ("rpc", "refresh_hospital_sitemap_cohort", {}) in log


def test_get_stats_cohort_failure_does_not_hide_the_total(monkeypatch):
    class _Broken(_FakeClient):
        def rpc(self, *_a):
            raise RuntimeError("57014 statement timeout")
    log = []
    monkeypatch.setattr(database, "service_client", lambda: _Broken(log))
    assert database.get_stats(123)["total_active_jobs"] == 123
    assert [1 for t, op, _b in log if t == "site_stats" and op == "upsert"]


# ── item 6: the scraper_runs record ────────────────────────────────────────

def test_run_record_start_and_finish(monkeypatch):
    log = []
    monkeypatch.setattr(database, "service_client", lambda: _FakeClient(log))
    rid = database.start_run_record(739500, "2026-10-07T00:09:00+00:00")
    assert rid == 77
    ins = [b for t, op, b in log if t == "scraper_runs" and op == "insert"][0]
    assert ins["status"] == "running" and ins["ats_platform"] == "nightly" and ins["run_day"] == 739500
    assert ins["run_started_at"] == ins["started_at"]
    ok = database.finish_run_record(rid, "success", "2026-10-07T03:40:00+00:00",
                                    rows_upserted=312466, deactivated=9562,
                                    field_priced=21303, text_priced=24595, notes="n")
    assert ok
    upd = [b for t, op, b in log if t == "scraper_runs" and op == "update"][0]
    assert upd["status"] == "success" and upd["rows_upserted"] == 312466
    assert upd["deactivated"] == 9562 == upd["jobs_deactivated"]
    assert upd["run_finished_at"] == upd["finished_at"]
    assert ("scraper_runs", "eq", ("id", 77)) in log


def test_run_record_is_non_fatal_without_a_client(monkeypatch):
    monkeypatch.setattr(database, "service_client", lambda: (_ for _ in ()).throw(ValueError("no key")))
    assert database.start_run_record(1, "x") is None
    assert database.finish_run_record(None, "success", "x") is False
    assert database.finish_run_record(5, "success", "x") is False


def test_finish_run_record_only_sends_check_values(monkeypatch):
    log = []
    monkeypatch.setattr(database, "service_client", lambda: _FakeClient(log))
    database.finish_run_record(1, "bogus", "x")
    assert [b for t, op, b in log if op == "update"][0]["status"] == "partial"


def test_scheduler_priced_counts_and_log_filters():
    import scheduler
    jobs = [{"posting_facts": {"pay_src": "field"}}, {"posting_facts": {"pay_src": "text"}},
            {"posting_facts": {"v": 4}}, {"posting_facts": None}, {}]
    assert scheduler._priced_counts(jobs) == (1, 1)
    rec = logging.LogRecord("aiohttp.client", logging.WARNING, __file__, 1,
                            "Can not load response cookies: Illegal key 'BIGipServerT8l62Llkf6/Rq+nWxHobug'",
                            None, None)
    keep = logging.LogRecord("aiohttp.client", logging.WARNING, __file__, 1, "something else", None, None)
    assert scheduler._drop_cookie_noise(rec) is False and scheduler._drop_cookie_noise(keep) is True
    assert any(f is scheduler._drop_cookie_noise for f in logging.getLogger("aiohttp.client").filters)
    assert logging.getLogger("httpx").level == logging.WARNING


# ── item 5: has_signon rides on the upsert ─────────────────────────────────

@pytest.mark.parametrize("title, body, want", [
    ("RN - $10,000 Sign-On Bonus", "", True),
    ("Registered Nurse (Signing Bonus)", "", True),
    ("RN Sign On Bonus Eligible", "", True),
    ("Registered Nurse", "We offer a $5,000 sign-on bonus for this role.", True),
    ("Registered Nurse", "Sign-On Incentive available", True),
    ("Registered Nurse", "Please sign on to your account to apply.", False),
    ("Registered Nurse", "Bonus structure discussed at interview.", False),
    ("", "", False),
    (None, None, False),
])
def test_has_signon_for_matches_the_sql_pass_rules(title, body, want):
    assert S.has_signon_for(title, body) is want


def test_normalize_job_sends_has_signon_on_every_row():
    def job(title, desc):
        return S.Job(title=title, hospital_system="Test Health", hospital_name="Test Hospital",
                     city="Austin", state="TX", location="Austin, TX", specialty="", job_type="",
                     url="https://jobs.example.com/1", job_id="1", posted_date="",
                     description=desc, ats_platform="Workday")
    assert S.normalize_job(job("RN", "Full description with a $3,000 sign-on bonus."))["has_signon"] is True
    assert S.normalize_job(job("RN", ""))["has_signon"] is False
    assert "has_signon" in S.normalize_job(job("Tech", "No bonus here."))


def test_flag_signon_jobs_is_a_no_op_unless_asked(monkeypatch):
    import urllib.request
    monkeypatch.setenv("SUPABASE_URL", "https://db.example")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    attempted = []

    def _urlopen(*a, **k):
        attempted.append(a[0])
        raise OSError("no network in tests")
    monkeypatch.setattr(urllib.request, "urlopen", _urlopen)
    assert S.flag_signon_jobs() == 0
    assert attempted == []                      # the nightly call sends nothing
    assert S.flag_signon_jobs(full_pass=True) == 0
    assert len(attempted) == 1                  # the backfill still starts with the max-id lookup


# ── item 6: one run day per run; BayCare off the re-read list ──────────────

def test_run_day_is_fixed_once_and_read_by_both_slots(monkeypatch):
    S.reset_run_day()
    try:
        assert S.fix_run_day(739000) == 739000
        assert S.fix_run_day(739001) == 739000          # the first call wins
        assert S._run_day() == 739000
        monkeypatch.setattr(S, "PAY_REREAD_DAYS", 7)
        monkeypatch.setattr(S, "DETAIL_REFRESH_DAYS", 7)
        for canon, jid in (("Tenet Healthcare", "123"), ("Mayo Clinic", "9")):
            assert S._pay_slot(canon, jid) == S._pay_slot(canon, jid, 739000)
            assert S._refresh_slot(canon, jid) == S._refresh_slot(canon, jid, 739000)
        # a day passed explicitly overrides the fixed one
        hits = {S._refresh_slot("Tenet Healthcare", "123", d) for d in range(739000, 739007)}
        assert hits == {True, False}
    finally:
        S.reset_run_day()
    assert S._RUN_DAY[0] is None
    assert isinstance(S._run_day(), int)


def test_baycare_left_the_pay_reread_list():
    assert "BayCare" not in S.PAY_REREAD_SYSTEMS
    assert "Tenet Healthcare" in S.PAY_REREAD_SYSTEMS and "Brookdale Senior Living" in S.PAY_REREAD_SYSTEMS


def test_last_run_counts_exist_for_the_run_record():
    assert set(S.LAST_RUN_COUNTS) >= {"upsert_sent", "sweep_deactivated"}
