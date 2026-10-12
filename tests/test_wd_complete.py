"""Workday complete-crawl reports and per-system complete rules (2026-10-12).

NewYork-Presbyterian's Workday board (total 323) was crawled in full every
night, but the yield guard compared the crawl (316 rows) with 488 active rows
and held the sweep, so 172 closed postings kept their dead Apply links.
scrape_workday now reports a complete crawl (report_complete) when its plain
crawl reached the last page with no error and no window hit, and a system fed
by two emit-names is complete only when both are (complete_canonical).
No network, no database."""
import asyncio
import urllib.error
import urllib.parse
import urllib.request

import retire_guard
import scraper


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status = body, status

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, body, status=200, exc=None):
        self.resp, self.exc = _Resp(body, status), exc

    async def __aenter__(self):
        if self.exc:
            raise self.exc
        return self.resp

    async def __aexit__(self, *a):
        return False


def _posting(n, req=None):
    rid = req if req is not None else n
    return {"title": f"Patient Registrar {n}", "externalPath": f"/job/Queens-NY/Registrar_{n:08d}",
            "locationsText": "Flushing, NY", "postedOn": "Posted Today", "bulletFields": [f"{rid:08d}"]}


def _board(postings, total=None, fail_at=None, status=500, exc=None):
    """A fake cxs endpoint over `postings` (20 a page). total: the advertised
    total (default len). fail_at: the offset that answers `status` (or raises
    `exc`)."""
    total = len(postings) if total is None else total

    def answer(body):
        off = body.get("offset", 0)
        if fail_at is not None and off == fail_at and body.get("limit") != 1:
            return _Ctx({}, status=status, exc=exc)
        if body.get("limit") == 1:
            return _Ctx({"total": total, "jobPostings": postings[:1], "facets": []})
        return _Ctx({"total": total if off == 0 else 0, "jobPostings": postings[off:off + 20]})
    return answer


def _run(monkeypatch, answer, system="NewYork-Presbyterian"):
    def req(session, method, url, **kw):
        return answer(kw.get("json") or {})

    async def no_wait():
        return None

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    return asyncio.run(scraper.scrape_workday(None, system, ("nyp", "1", "nypcareers")))


# -- 1. scrape_workday reports a complete crawl -------------------------------

def test_a_whole_board_is_reported_complete(monkeypatch):
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(323)]))
    assert len(jobs) == 323
    assert "NewYork-Presbyterian" in scraper.COMPLETE_SYSTEMS
    assert "NewYork-Presbyterian" in scraper.LISTED_SYSTEMS


def test_postings_collapsed_by_the_req_id_dedupe_still_count_as_listed(monkeypatch):
    # NYP 10-12: 316 rows from a board of 323. Seven postings repeat an
    # earlier requisition number; the pages still listed all 323.
    posts = [_posting(i) for i in range(316)] + [_posting(316 + i, req=i) for i in range(7)]
    jobs = _run(monkeypatch, _board(posts))
    assert len(jobs) == 316
    assert "NewYork-Presbyterian" in scraper.COMPLETE_SYSTEMS


def test_a_board_whose_last_page_is_full_ends_on_an_empty_page(monkeypatch):
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(40)]))
    assert len(jobs) == 40 and "NewYork-Presbyterian" in scraper.COMPLETE_SYSTEMS


def test_an_http_error_mid_crawl_is_not_complete(monkeypatch):
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(323)], fail_at=160))
    assert len(jobs) == 160
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS
    assert "NewYork-Presbyterian" in scraper.LISTED_SYSTEMS


def test_an_exception_mid_crawl_is_not_complete(monkeypatch):
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(323)], fail_at=300,
                                    exc=asyncio.TimeoutError()))
    assert len(jobs) == 300
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS


def test_pages_short_of_the_advertised_total_are_not_complete(monkeypatch):
    # The pages ended at 323 but the tenant advertised 400: 323 < 98% of 400.
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(323)], total=400))
    assert len(jobs) == 323 and "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS


def test_no_advertised_total_or_an_empty_board_is_not_complete(monkeypatch):
    _run(monkeypatch, _board([_posting(i) for i in range(30)], total=0))
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS
    _run(monkeypatch, _board([]))
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS


def test_a_crawl_that_fills_the_window_is_never_complete(monkeypatch):
    # Over 2,000 postings: the plain crawl fills the window and the facet
    # pass cannot slice (no facets), so the crawl is truncated.
    jobs = _run(monkeypatch, _board([_posting(i) for i in range(2100)], total=2000))
    assert len(jobs) == 2000
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS


def test_a_partial_report_wins_over_a_complete_crawl(monkeypatch):
    scraper.PARTIAL_SYSTEMS.add("NewYork-Presbyterian")
    _run(monkeypatch, _board([_posting(i) for i in range(50)]))
    assert "NewYork-Presbyterian" not in scraper.COMPLETE_SYSTEMS


# -- 2. complete_canonical: every feed of a system must be complete -----------

def test_complete_canonical_needs_every_emit_name(monkeypatch):
    monkeypatch.setitem(scraper.HOSPITAL_SYSTEM_ALIASES, "Two Tenant Board (Providers)", "Two Tenant Board")
    scraper.LISTED_SYSTEMS.update({"Two Tenant Board", "Two Tenant Board (Providers)", "Solo Board"})
    scraper.COMPLETE_SYSTEMS.update({"Two Tenant Board", "Solo Board"})
    assert scraper.complete_canonical() == {"Solo Board"}          # the second tenant did not finish
    scraper.COMPLETE_SYSTEMS.add("Two Tenant Board (Providers)")
    assert scraper.complete_canonical() == {"Solo Board", "Two Tenant Board"}
    scraper.PARTIAL_SYSTEMS.add("Two Tenant Board (Providers)")     # a partial report wins (post-alias)
    assert scraper.complete_canonical() == {"Solo Board"}
    assert scraper.complete_canonical({"Solo Board"}) == set()      # exclude (Layer 4's exempt set)


def test_a_tenant_that_failed_with_no_rows_still_holds_its_system(monkeypatch):
    monkeypatch.setitem(scraper.HOSPITAL_SYSTEM_ALIASES, "Two Tenant Board (Providers)", "Two Tenant Board")
    _run(monkeypatch, _board([_posting(i) for i in range(40)]), system="Two Tenant Board")
    _run(monkeypatch, _board([_posting(i) for i in range(40)], fail_at=0), system="Two Tenant Board (Providers)")
    assert "Two Tenant Board" in scraper.COMPLETE_SYSTEMS
    assert "Two Tenant Board" not in scraper.complete_canonical()


# -- 3. The sweep: a complete Workday system is swept on its own --------------

class _UResp:
    def __init__(self, content_range=""):
        self.headers = {"Content-Range": content_range}

    def read(self): return b""
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _row(system, i):
    return {"hospital_system": system, "job_id": str(i), "title": "Patient Registrar", "hospital_name": system,
            "url": f"https://example.org/jobs/{i}", "job_type": "", "description": "", "state": "NY",
            "city": "Flushing"}


def _fake_db(monkeypatch, active_by_system, broken=()):
    patches = []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u:
            return _UResp()
        if m == "POST":
            if any(b.encode() in (rq.data or b"") for b in broken):
                raise urllib.error.HTTPError(u, 500, "boom", {}, None)
            return _UResp()
        system = urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0])
        if m == "GET":
            return _UResp(f"0-0/{active_by_system[system]}")
        patches.append(system)
        return _UResp("*/0")
    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    return patches


def test_nyp_shape_is_swept_once_workday_reports_complete(monkeypatch):
    # 316 rows tonight against 488 active: 65%, held by the 80% guard before.
    patches = _fake_db(monkeypatch, {"NewYork-Presbyterian": 488})
    rows = [_row("NewYork-Presbyterian", i) for i in range(316)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T00:09:00.000000Z")
    assert patches == []                                     # no claim: guarded, as before
    _run(monkeypatch, _board([_posting(i) for i in range(316)] + [_posting(316 + i, req=i) for i in range(7)]))
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T00:09:00.000000Z")
    assert patches == ["NewYork-Presbyterian"]


def test_a_complete_system_is_swept_when_another_systems_batch_failed(monkeypatch):
    # 30 + 30 rows: whichever system the upsert sends first, the halves of
    # the split batch are one system each; every batch with a Broken Board
    # row fails. Whole Board: 30 of 40 active (75%), held without its claim.
    patches = _fake_db(monkeypatch, {"Whole Board": 40, "Broken Board": 25}, broken=("Broken Board",))
    scraper.LISTED_SYSTEMS.update({"Whole Board", "Broken Board"})
    scraper.COMPLETE_SYSTEMS.update({"Whole Board", "Broken Board"})
    rows = [_row("Whole Board", i) for i in range(30)] + [_row("Broken Board", 1000 + i) for i in range(30)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T00:09:00.000000Z")
    assert patches == ["Whole Board"]                        # the failed system is never swept


def test_the_half_of_active_floor_still_holds_a_complete_claim(monkeypatch):
    patches = _fake_db(monkeypatch, {"NewYork-Presbyterian": 700})
    _run(monkeypatch, _board([_posting(i) for i in range(300)]))
    rows = [_row("NewYork-Presbyterian", i) for i in range(300)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T00:09:00.000000Z")
    assert patches == []                                     # 300 of 700 active: under half, not believed


# -- 4. Layer 4 counts a complete system's unseen rows as usual ---------------

def test_layer4_counts_a_complete_workday_systems_unseen_rows():
    active = [{"id": i, "job_id": str(i), "hospital_system": "NewYork-Presbyterian",
               "consecutive_scrape_misses": 2, "scraped_at": "2026-10-11T00:09:00+00:00"} for i in range(488)]
    keys = {("NewYork-Presbyterian", str(i)) for i in range(316)}
    held = retire_guard.plan_layer4(active, keys, 3)
    assert held["deactivate_ids"] == [] and "NewYork-Presbyterian" in held["guarded"]
    plan = retire_guard.plan_layer4(active, keys, 3, complete_systems={"NewYork-Presbyterian"})
    assert len(plan["deactivate_ids"]) == 172 and "NewYork-Presbyterian" in plan["complete_bypassed"]
