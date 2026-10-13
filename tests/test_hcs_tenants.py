"""HealthcareSource tenants that went dark (2026-10-13).

The 10-13 12:00 UTC run listed 0 rows for two HealthcareSource tenants:

  Sarasota Memorial (smh): every request answers HTTP 500
    {"Errors": ["Alias for clientId 4723 does not exist."]}. Sarasota Memorial
    moved to iCIMS; careers.smh.com is the Jibe front already configured as
    JIBE_SITES "Sarasota Memorial Health Care System" (397 live on 10-13).
  Liberty Hospital (liberty): every page answers HTTP 500 "Value cannot be
    null. Parameter name: source". Liberty Hospital is part of The University
    of Kansas Health System; libertyhospital.org's "View our openings" goes to
    the kansashealthsystem wd1 Workday tenant we already read.

These tests pin the config change, the adapter's handling of both answers
(fixtures are the live bodies), the location of Liberty's facility-only
Workday sites (fixture: postings read live on 10-13), and how the sweep and
Layer 4 treat a tenant that lists nothing or skips a page. No network, no
database: scraper.req and urllib.request.urlopen are fakes.
"""
import asyncio
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

import retire_guard
import scraper

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "hcs_tenants")
TUKHS = "The University of Kansas Health System"


def _fixture(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        if isinstance(self.r, Exception):
            raise self.r
        return self.r

    async def __aexit__(self, *a):
        return False


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)


def _hcs_fake(monkeypatch, responder):
    calls = []

    def fake_req(session, method, url, **kw):
        calls.append((url, kw["params"]["from"], kw["json"]))
        return _Ctx(responder(kw["params"]["from"], kw["json"]))
    monkeypatch.setattr(scraper, "req", fake_req)
    return calls


def _hit(i):
    return {"_id": f"t_{i}", "_source": {"title": f"RN {i}", "datePosted": "2026-10-01T00:00:00Z",
                                         "jobLocation": {"address": {"addressLocality": "Canton",
                                                                     "addressRegion": "OH"}},
                                         "userArea": {"jobPostingID": str(i)}}}


# -- 1. Config: the dead tenants are gone, their boards are read elsewhere ----

def test_the_two_dead_tenants_are_not_configured():
    assert "Sarasota Memorial" not in scraper.HEALTHCARESOURCE_ORGS
    assert "Liberty Hospital" not in scraper.HEALTHCARESOURCE_ORGS
    assert not {"smh", "liberty"} & {t.lower() for t in scraper.HEALTHCARESOURCE_ORGS.values()}


def test_their_jobs_come_from_the_boards_they_moved_to():
    # Sarasota Memorial: iCIMS, through its Jibe front (facility = location_name).
    assert scraper.JIBE_SITES["Sarasota Memorial Health Care System"] == "https://careers.smh.com"
    assert "Sarasota Memorial Health Care System" in scraper.JIBE_FACILITY_NAME
    # Liberty Hospital: The University of Kansas Health System's Workday.
    assert scraper.WORKDAY_TENANTS[TUKHS] == ("kansashealthsystem", "1", "careers")
    feeds = scraper.configured_feed_map()
    assert feeds["Sarasota Memorial Health Care System"] == {"JIBE_SITES"}
    assert "Sarasota Memorial" not in feeds and "Liberty Hospital" not in feeds


# -- 2. The adapter on the two live answers -----------------------------------

def test_a_removed_tenant_stops_after_one_request(monkeypatch, caplog):
    gone = _fixture("smh_alias_gone.json")
    calls = _hcs_fake(monkeypatch, lambda frm, body: _R(500, gone))
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Gone Tenant", "smh"))
    assert jobs == []
    assert len(calls) == 1                                   # no unsorted retry, no paging on
    assert calls[0][0] == "https://pm.healthcaresource.com/JobseekerSearchAPI/smh/api/Search"
    assert any("TENANT GONE" in m and "Alias for clientId 4723 does not exist" in m for m in caplog.messages)
    # Nothing listed: no partial flag (the zero-yield guard and the backstop
    # decide), no complete claim.
    assert "Gone Tenant" not in scraper.PARTIAL_SYSTEMS
    assert "Gone Tenant" not in scraper.COMPLETE_SYSTEMS


def test_a_tenant_that_fails_every_page_lists_nothing(monkeypatch, caplog):
    # The 10-13 log: "page at offset 100 failed (HTTP 500 ... Value cannot be
    # null. Parameter name: source)" and "0 rows listed".
    null_source = _fixture("liberty_null_source.json")
    calls = _hcs_fake(monkeypatch, lambda frm, body: _R(500, null_source))
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Broken Tenant", "liberty"))
    assert jobs == []
    assert [c[1] for c in calls] == [0, 0, 50, 100] and "sort" not in calls[1][2]
    assert any("page at offset 100 failed (RuntimeError: HTTP 500" in m and "Parameter name: source" in m
               for m in caplog.messages)
    assert any("stopping at offset 100 after 3 consecutive page failures; 0 rows listed" in m
               for m in caplog.messages)
    assert not any("TENANT GONE" in m for m in caplog.messages)
    assert "Broken Tenant" not in scraper.PARTIAL_SYSTEMS
    assert "Broken Tenant" not in scraper.COMPLETE_SYSTEMS


def test_a_skipped_page_makes_the_crawl_partial(monkeypatch, caplog):
    monkeypatch.setattr(scraper, "_HCS_PAGE", 5)
    null_source = _fixture("liberty_null_source.json")

    def responder(frm, body):
        if frm == 10:
            return _R(500, null_source)
        return _R(200, {"hits": {"total": {"value": 50}, "hits": [_hit(frm + i) for i in range(5)]}})
    _hcs_fake(monkeypatch, responder)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Aultman Health Foundation", "aultman"))
    assert len(jobs) == 45 and "10" not in {j.job_id for j in jobs}
    assert "Aultman Health Foundation" in scraper.PARTIAL_SYSTEMS
    assert "Aultman Health Foundation" not in scraper.COMPLETE_SYSTEMS
    assert any("Aultman Health Foundation: PARTIAL, 1 page(s) skipped, 45 listed of 50 advertised" in m
               for m in caplog.messages)


def test_a_whole_crawl_is_not_partial(monkeypatch):
    monkeypatch.setattr(scraper, "_HCS_PAGE", 5)

    def responder(frm, body):
        return _R(200, {"hits": {"total": {"value": 10}, "hits": [_hit(frm + i) for i in range(5)] if frm < 10 else []}})
    _hcs_fake(monkeypatch, responder)
    jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Aultman Health Foundation", "aultman"))
    assert len(jobs) == 10
    assert "Aultman Health Foundation" not in scraper.PARTIAL_SYSTEMS
    assert "Aultman Health Foundation" in scraper.COMPLETE_SYSTEMS


# -- 3. The sweep and Layer 4 on these outages --------------------------------

class _UResp:
    def __init__(self, content_range=""):
        self.headers = {"Content-Range": content_range}

    def read(self): return b""
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _row(system, i):
    return {"hospital_system": system, "job_id": str(i), "title": "Registered Nurse", "hospital_name": system,
            "url": f"https://example.org/jobs/{i}", "job_type": "", "description": "", "state": "OH",
            "city": "Canton"}


def _fake_db(monkeypatch, active_by_system):
    patches = []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u or m == "POST":
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


def test_the_sweep_never_touches_a_tenant_that_listed_nothing(monkeypatch):
    # A dark tenant has no row in tonight's list, so it is not in the sweep's
    # system counts at all; a healthy system next to it is swept as usual.
    patches = _fake_db(monkeypatch, {"Healthy Board": 40, "Dark Tenant": 78})
    rows = [_row("Healthy Board", i) for i in range(40)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T12:00:00.000000Z")
    assert patches == ["Healthy Board"]


def test_the_sweep_holds_a_tenant_with_a_skipped_page(monkeypatch):
    # 45 of 50 active is above GUARD_RATIO, so only the partial flag keeps the
    # sweep off the 5 live rows on the page that failed.
    monkeypatch.setattr(scraper, "_HCS_PAGE", 5)

    def responder(frm, body):
        if frm == 10:
            return _R(500, _fixture("liberty_null_source.json"))
        return _R(200, {"hits": {"total": {"value": 50}, "hits": [_hit(frm + i) for i in range(5)]}})
    _hcs_fake(monkeypatch, responder)
    jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Aultman Health Foundation", "aultman"))
    patches = _fake_db(monkeypatch, {"Aultman Health Foundation": 50})
    rows = [_row("Aultman Health Foundation", j.job_id) for j in jobs]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T12:00:00.000000Z")
    assert patches == []
    scraper.PARTIAL_SYSTEMS.clear()                          # what the sweep did before this fix
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-13T12:00:00.000000Z")
    assert patches == ["Aultman Health Foundation"]


def test_layer4_freezes_a_dark_tenant_until_the_backstop():
    now = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
    recent = (now - timedelta(days=1)).isoformat()
    old = (now - timedelta(days=retire_guard.BACKSTOP_DAYS + 1)).isoformat()
    rows = ([{"id": i, "job_id": str(i), "hospital_system": "Dark Tenant", "consecutive_scrape_misses": 2,
              "scraped_at": recent} for i in range(10)]
            + [{"id": 100 + i, "job_id": str(100 + i), "hospital_system": "Dark Tenant",
                "consecutive_scrape_misses": 2, "scraped_at": old} for i in range(5)])
    assert retire_guard.guard_reason(15, 0) == "zero-yield"
    plan = retire_guard.plan_layer4(rows, set(), miss_threshold=3, now=now)
    assert plan["guarded"]["Dark Tenant"]["reason"] == "zero-yield"
    assert plan["frozen_rows"] == 10                         # seen within 7 days: held
    assert sorted(plan["deactivate_ids"]) == [100, 101, 102, 103, 104]   # past the backstop: retired


# -- 4. Liberty Hospital's rows on the Kansas Workday board -------------------

def _fake_wd_board(monkeypatch, postings):
    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("appliedFacets") or body.get("limit") == 1 or body.get("offset", 0):
            return _Ctx(_R(200, {"total": 0, "jobPostings": [], "facets": []}))
        return _Ctx(_R(200, {"total": len(postings), "jobPostings": [dict(p) for p in postings]}))
    monkeypatch.setattr(scraper, "req", req)


def test_liberty_hospital_sites_are_located_in_missouri(monkeypatch):
    postings = _fixture("tukhs_liberty_postings.json")["jobPostings"]
    _fake_wd_board(monkeypatch, postings)
    jobs = asyncio.run(scraper.scrape_workday(None, TUKHS, scraper.WORKDAY_TENANTS[TUKHS]))
    rows = scraper.finalize_jobs(jobs)
    got = {r["job_id"]: (r["hospital_name"], r["city"], r["state"]) for r in rows}
    assert len(got) == len(postings) == 12
    for rid in ("R-45937", "R-42558", "R-51069", "R-53774", "R-48097", "R-49813"):
        assert got[rid] == ("Liberty Hospital", "Liberty", "MO")              # "Liberty Hospital"
    assert got["R-58326"] == (TUKHS, "Liberty", "MO")                         # Sports Medicine, Blue Jay Dr
    assert got["R-56321"] == got["R-53612"] == (TUKHS, "Kansas City", "MO")   # Urgent Care Shoal Creek
    assert got["R-52114"] == got["R-57029"] == (TUKHS, "Liberty", "MO")       # "Liberty, MO"
    assert got["R-56851"] == (TUKHS, "Kansas City", "KS")                     # untouched
    assert all(r["url"].startswith("https://kansashealthsystem.wd1.myworkdayjobs.com/") for r in rows)
    assert {r["hospital_system"] for r in rows} == {TUKHS}


def test_the_facility_map_needs_the_whole_site_name():
    f = scraper._wd_facility
    assert f(TUKHS, "Liberty Hospital", "Liberty Hospital", "") == ("Liberty Hospital", "Liberty", "MO")
    assert f(TUKHS, "Liberty Hospital Urgent Care Shoal Creek", "", "") == (None, "Kansas City", "MO")
    assert f(TUKHS, "Liberty, MO", "Liberty", "MO") == (None, "Liberty", "MO")
    assert f(TUKHS, "Olathe Hospital", "Olathe Hospital", "") == (None, "Olathe Hospital", "")
