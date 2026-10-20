"""Workday complete-crawl reports and per-system complete rules (2026-10-12).

NewYork-Presbyterian's Workday board (total 323) was crawled in full every
night, but the yield guard compared the crawl (316 rows) with 488 active rows
and held the sweep, so 172 closed postings kept their dead Apply links.
scrape_workday now reports a complete crawl (report_complete) when its plain
crawl reached the last page with no error and no window hit, and a system fed
by two emit-names is complete only when both are (complete_canonical).
Review (2026-10-12): every configured feed of a system (each adapter config
that emits it, each alias key) must have run, returned rows and claimed a
whole crawl; the Workday claim is recorded only as scrape_workday returns.
No network, no database."""
import asyncio
import pathlib
from datetime import datetime, timedelta, timezone
import re
import urllib.error
import urllib.parse
import urllib.request

import pytest

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
    scraper.EMITTED_SYSTEMS.update({"Two Tenant Board", "Two Tenant Board (Providers)", "Solo Board"})
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
    # 2026-10-20: two days before now, not a fixed date: plan_layer4's 7-day
    # backstop retires rows last seen more than a week ago whatever the
    # guard says, so the fixed 2026-10-11 stamp made this fail after 10-18.
    seen = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    active = [{"id": i, "job_id": str(i), "hospital_system": "NewYork-Presbyterian",
               "consecutive_scrape_misses": 2, "scraped_at": seen} for i in range(488)]
    keys = {("NewYork-Presbyterian", str(i)) for i in range(316)}
    held = retire_guard.plan_layer4(active, keys, 3)
    assert held["deactivate_ids"] == [] and "NewYork-Presbyterian" in held["guarded"]
    plan = retire_guard.plan_layer4(active, keys, 3, complete_systems={"NewYork-Presbyterian"})
    assert len(plan["deactivate_ids"]) == 172 and "NewYork-Presbyterian" in plan["complete_bypassed"]


# -- 5. Every configured feed of a system must claim (review 2026-10-12) ------

NYP = "NewYork-Presbyterian"


def _nyp_complete(monkeypatch):
    """NYP's Workday tenant crawls its whole board: 316 rows of 323 listed."""
    _run(monkeypatch, _board([_posting(i) for i in range(316)] + [_posting(316 + i, req=i) for i in range(7)]))
    assert NYP in scraper.COMPLETE_SYSTEMS and ("WORKDAY_TENANTS", NYP) in scraper.COMPLETE_FEEDS


def test_a_workday_only_system_is_complete(monkeypatch):
    # NYP has one feed in the real configs: its Workday tenant.
    feeds = scraper.configured_feed_map()
    assert {e for e in feeds if scraper.HOSPITAL_SYSTEM_ALIASES.get(e, e) == NYP} == {NYP}
    assert feeds[NYP] == {"WORKDAY_TENANTS"}
    _nyp_complete(monkeypatch)
    assert scraper.complete_canonical() == {NYP}


def test_a_silent_non_workday_sibling_with_no_rows_holds_the_system(monkeypatch):
    # Another adapter emits the same name; it ran tonight with 0 rows and no
    # claim. The Workday claim alone does not make the system complete, and
    # the sweep keeps the yield guard (316 rows vs 488 active: held).
    monkeypatch.setitem(scraper.ICIMS_ORGS, NYP, "careers-nyp.icims.com")
    _nyp_complete(monkeypatch)
    assert NYP not in scraper.complete_canonical()
    patches = _fake_db(monkeypatch, {NYP: 488})
    scraper._upsert_hospital_jobs_to_supabase([_row(NYP, i) for i in range(316)], "2026-10-13T00:09:00.000000Z")
    assert patches == []


def test_a_silent_alias_sibling_holds_the_system(monkeypatch):
    # A HOSPITAL_SYSTEM_ALIASES key that maps to NYP is a feed of it, listed
    # in a config or not; with no rows and no claim tonight it holds NYP.
    monkeypatch.setitem(scraper.HOSPITAL_SYSTEM_ALIASES, "NYP (Physicians)", NYP)
    _nyp_complete(monkeypatch)
    assert NYP not in scraper.complete_canonical()
    monkeypatch.setitem(scraper.HEALTHCARESOURCE_ORGS, "NYP (Physicians)", "nypmd")
    assert NYP not in scraper.complete_canonical()


def test_both_feeds_complete_make_the_system_complete(monkeypatch):
    # A sibling under its own (aliased) name, and one under the same name.
    monkeypatch.setitem(scraper.HOSPITAL_SYSTEM_ALIASES, "NYP (Physicians)", NYP)
    monkeypatch.setitem(scraper.HEALTHCARESOURCE_ORGS, "NYP (Physicians)", "nypmd")
    monkeypatch.setitem(scraper.PHENOM_ORGS, NYP, "https://careers.nyp.org")
    assert scraper.report_complete(NYP, 400, 400, feed="PHENOM_ORGS")   # the Phenom sibling, before Workday
    _nyp_complete(monkeypatch)
    assert NYP not in scraper.complete_canonical()                       # the Physicians feed has not run
    assert scraper.report_complete("NYP (Physicians)", 50, 50, feed="HEALTHCARESOURCE_ORGS")
    assert NYP not in scraper.complete_canonical()                       # claimed, but returned no rows
    scraper.EMITTED_SYSTEMS.add("NYP (Physicians)")
    assert scraper.complete_canonical() == {NYP}
    patches = _fake_db(monkeypatch, {NYP: 488})
    rows = [_row(NYP, i) for i in range(300)] + [_row("NYP (Physicians)", 9000 + i) for i in range(16)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T00:09:00.000000Z")
    assert patches == [NYP]                                              # swept as a complete system


def test_a_same_name_sibling_that_failed_its_claim_holds_the_system(monkeypatch):
    monkeypatch.setitem(scraper.PHENOM_ORGS, NYP, "https://careers.nyp.org")
    _nyp_complete(monkeypatch)
    assert not scraper.report_complete(NYP, 100, 400, feed="PHENOM_ORGS")  # Phenom listed a quarter
    assert NYP not in scraper.complete_canonical()


def test_real_systems_with_a_non_workday_sibling_are_held(monkeypatch):
    # Cleveland Clinic (Playwright fallback), Trinity Health (run_trinity),
    # Monument Health (iCIMS) and Owensboro Health (iCIMS) each have a second
    # adapter emitting the Workday tenant's name.
    for system in ("Cleveland Clinic", "Trinity Health", "Monument Health", "Owensboro Health"):
        assert "WORKDAY_TENANTS" in scraper.configured_feed_map()[system]
        _run(monkeypatch, _board([_posting(i) for i in range(40)]), system=system)
        assert system in scraper.COMPLETE_SYSTEMS
        assert system not in scraper.complete_canonical()


def test_a_workday_tenant_with_no_rows_claims_nothing(monkeypatch):
    # The whole board is listed, but the statewide-board title filter
    # (WD_TITLE_FACILITY) keeps none of its 30 postings: no rows, no claim.
    monkeypatch.setitem(scraper.WD_TITLE_FACILITY, NYP, ())
    monkeypatch.setattr(scraper, "_wd_title_facility", lambda system, title: None)
    assert _run(monkeypatch, _board([_posting(i) for i in range(30)])) == []
    assert NYP not in scraper.COMPLETE_SYSTEMS and NYP not in scraper.EMITTED_SYSTEMS
    assert NYP not in scraper.complete_canonical()


# -- 6. The Workday claim is recorded only as scrape_workday returns ---------

def test_an_exception_after_the_crawl_leaves_the_system_not_complete(monkeypatch):
    _nyp_complete(monkeypatch)                                 # last night's claim, same process

    async def cancelled(*a, **k):
        raise asyncio.CancelledError()
    monkeypatch.setattr(scraper, "_wd_fill_states", cancelled)
    with pytest.raises(asyncio.CancelledError):
        _run(monkeypatch, _board([_posting(i) for i in range(323)]))
    assert NYP not in scraper.COMPLETE_SYSTEMS
    assert ("WORKDAY_TENANTS", NYP) not in scraper.COMPLETE_FEEDS
    assert NYP not in scraper.complete_canonical()


# -- 7. The feed registry covers every adapter config ------------------------

_CONFIG_NAME = re.compile(r"_(TENANTS|ORGS|SITES|BOARDS|BRANDS|AGENCIES|PORTALS)$")
_NOT_FEEDS = {"MOUNT_SINAI_SITES"}      # Mount Sinai facility patterns, not boards


def test_every_adapter_config_is_a_feed_config():
    configs = {n for n, v in vars(scraper).items()
               if _CONFIG_NAME.search(n) and isinstance(v, (dict, list, tuple))}
    assert configs - _NOT_FEEDS <= set(scraper.FEED_CONFIGS)
    assert set(scraper.FEED_CONFIGS) <= configs


def test_every_literal_emit_name_is_a_configured_feed():
    src = pathlib.Path(scraper.__file__).read_text(encoding="utf-8")
    literals = set(re.findall(r'hospital_system="([^"]+)"', src))
    literals |= set(re.findall(r'^\s*(?:[A-Z_]*SYSTEM|system)\s*=\s*"([^"]+)"', src, re.M))
    assert literals and literals <= set(scraper.configured_feed_map())


def test_every_complete_claim_names_a_configured_feed():
    src = pathlib.Path(scraper.__file__).read_text(encoding="utf-8")
    calls = re.findall(r'(?<!def )report_complete\((?:[^()]|\([^()]*\))*\)', src)
    claimed = {m.group(1) for c in calls for m in [re.search(r'feed="([^"]+)"', c)] if m}
    assert len(calls) == 15 and all('feed="' in c for c in calls)       # every claim names its feed
    known = set().union(*scraper.configured_feed_map().values())
    # 2026-10-14 (fix 11): Greenhouse, ApplicantPro, UKG, NeoGov, TalentBrew
    # and SelectMinds claim against the board's own count too.
    # 2026-10-15: the Jibe feeds (totalCount), Oracle HCM (TotalJobsCount)
    # and SmartRecruiters (totalFound) against their own counts.
    assert claimed == {"WORKDAY_TENANTS", "PHENOM_ORGS", "HEALTHCARESOURCE_ORGS", "TALEMETRY_SITES",
                       "run_uhg", "run_chs", "GREENHOUSE_ORGS", "APPLICANTPRO_ORGS", "UKG_ORGS",
                       "NEOGOV_AGENCIES", "TALENTBREW_ORGS", "SELECTMINDS_ORGS", "JIBE_SITES",
                       "ORACLE_ORGS", "SMARTRECRUITERS_ORGS"}
    assert claimed <= known
