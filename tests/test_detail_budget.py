"""Detail-pass budget (2026-09-24): known bodies cost nothing, every tenant
gets a fair share whenever it finishes listing, sibling sites fetch a
requisition once. No network, no database."""
import asyncio
import io
import json

import pytest

import scraper
from scraper import Job


def _job(i, system="T", desc="", state="", posted="", url=None):
    return Job(title=f"RN {i}", hospital_system=system, hospital_name=system, city="", state=state,
               location="", specialty="", job_type="", url=url or f"https://x/{system}/job/{i}",
               job_id=str(i), posted_date=posted, description=desc, ats_platform="Oracle HCM")


@pytest.fixture(autouse=True)
def _no_known_bodies(monkeypatch):
    scraper.set_known_bodies([])
    # The pass pauses 0.15-0.45 s after each fetch; not here.
    monkeypatch.setattr(scraper.random, "uniform", lambda a, b: 0.0)
    yield
    scraper.set_known_bodies([])


def test_water_fill_is_max_min_fair():
    assert scraper._water_fill({"a": 10, "b": 500, "c": 500}, 610) == {"a": 10, "b": 300, "c": 300}
    assert scraper._water_fill({"a": 10, "b": 20}, 1000) == {"a": 10, "b": 20}
    assert scraper._water_fill({"a": 100, "b": 100}, 101) == {"a": 50, "b": 51}
    assert scraper._water_fill({"a": 5}, 0) == {"a": 0}


def test_posted_age_and_rank():
    assert scraper._posted_age_days("Posted Today") == 0
    assert scraper._posted_age_days("Posted Yesterday") == 1
    assert scraper._posted_age_days("Posted 30+ Days Ago") == 30
    assert scraper._posted_age_days("") is None
    assert scraper._posted_age_days("2000-01-01") > 9000
    ny_old = _job(1, state="NY", posted="2020-01-01")
    fl_new = _job(2, state="FL", posted="Posted Today")
    fl_undated = _job(3, state="FL")
    order = sorted([fl_undated, fl_new, ny_old], key=scraper._detail_rank)
    assert [j.job_id for j in order] == ["1", "2", "3"]


def test_known_bodies_cost_nothing_and_keep_the_stored_body():
    scraper.set_known_bodies([
        {"hospital_system": "Guthrie", "job_id": "1", "desc_len": 2400},   # full body, aliased system
        {"hospital_system": "Guthrie", "job_id": "2", "desc_len": 900},    # detail body, list gives nothing
        {"hospital_system": "Guthrie", "job_id": "3", "desc_len": 600},    # teaser: the list sends the same
        {"hospital_system": "Guthrie", "job_id": "4", "desc_len": 150},    # not a body
    ])
    teaser = "Short teaser. " * 43                                          # 602 characters
    jobs = [_job(1, "Guthrie Health", desc=teaser), _job(2, "Guthrie Health"),
            _job(3, "Guthrie Health", desc=teaser), _job(4, "Guthrie Health"), _job(5, "Guthrie Health")]
    b = scraper._DescBudget(100)
    cands, known, dup = scraper._detail_candidates("Guthrie Health", jobs, b)
    assert sorted(j.job_id for j in cands) == ["3", "4", "5"]
    assert known == 2 and dup == 0
    # The list's shorter copy is dropped so the enrichment trigger keeps the stored body.
    assert jobs[0].description == "" and jobs[1].description == ""
    assert jobs[2].description == teaser


def test_old_first_come_rule_starved_the_slow_tenants():
    """The bug: tenants that finish listing first take the whole budget."""
    b = scraper._DescBudget(1000)
    got = {}

    async def tenant(name, delay, need):
        await asyncio.sleep(delay)                     # listing time
        jobs = [_job(i, name) for i in range(need)]
        seen = []

        async def fetch(j):
            seen.append(j.job_id)
            return True
        await scraper._detail_pass(None, name, jobs, b, fetch, "Test")
        got[name] = len(seen)

    async def main():
        await asyncio.gather(*[tenant(f"small{i}", 0.001 * i, 300) for i in range(4)],
                             tenant("VITAS", 0.05, 1000), tenant("Lifepoint", 0.08, 4000))

    asyncio.run(main())
    assert got["VITAS"] == 0 and got["Lifepoint"] == 0
    assert sum(got.values()) == 1000


def test_fair_share_reaches_every_tenant(monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_TENANT_MAX", 1500)
    b = scraper._DescBudget(1200)
    names = [f"small{i}" for i in range(4)] + ["VITAS", "Lifepoint"]
    needs = {"small0": 10, "small1": 20, "small2": 30, "small3": 40, "VITAS": 1000, "Lifepoint": 4000}
    delays = {"small0": 0.0, "small1": 0.001, "small2": 0.002, "small3": 0.003, "VITAS": 0.05, "Lifepoint": 0.08}
    got = {}

    async def scrape(name):
        await asyncio.sleep(delays[name])
        jobs = [_job(i, name) for i in range(needs[name])]
        seen = []

        async def fetch(j):
            seen.append(j.job_id)
            return True
        await scraper._detail_pass(None, name, jobs, b, fetch, "Test")
        got[name] = len(seen)
        return jobs

    async def main():
        b.expect(names)
        await asyncio.gather(*[scraper._tenant_reporting(b, n, scrape(n)) for n in names])

    asyncio.run(main())
    # Small tenants get everything they need; the two slow ones split the rest evenly.
    assert [got[f"small{i}"] for i in range(4)] == [10, 20, 30, 40]
    assert got["VITAS"] == 550 and got["Lifepoint"] == 550
    assert b.spent == 1200 and b.remaining == 0


def test_tenant_max_caps_one_host(monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_TENANT_MAX", 300)
    b = scraper._DescBudget(10000)
    got = {}

    async def scrape(name, need):
        jobs = [_job(i, name) for i in range(need)]
        seen = []

        async def fetch(j):
            seen.append(j.job_id)
            return True
        await scraper._detail_pass(None, name, jobs, b, fetch, "Test")
        got[name] = len(seen)

    async def main():
        b.expect(["A", "B"])
        await asyncio.gather(scraper._tenant_reporting(b, "A", scrape("A", 5000)),
                             scraper._tenant_reporting(b, "B", scrape("B", 50)))

    asyncio.run(main())
    assert got == {"A": 300, "B": 50}
    assert b.spent == 350


def test_a_failed_tenant_does_not_hold_the_barrier():
    b = scraper._DescBudget(200)
    got = {}

    async def broken():
        raise RuntimeError("list endpoint 500")

    async def good():
        jobs = [_job(i, "G") for i in range(500)]
        seen = []

        async def fetch(j):
            seen.append(j.job_id)
            return True
        await scraper._detail_pass(None, "G", jobs, b, fetch, "Test")
        got["G"] = len(seen)

    async def main():
        b.expect(["G", "Broken"])
        await asyncio.wait_for(asyncio.gather(scraper._tenant_reporting(b, "Broken", broken()),
                                              scraper._tenant_reporting(b, "G", good()),
                                              return_exceptions=True), timeout=5)

    asyncio.run(main())
    assert got["G"] == 200


def test_barrier_timeout_holds_a_share_for_the_slow_tenant(monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_BARRIER_WAIT", 0.05)
    b = scraper._DescBudget(900)            # floor 300 each
    got = {}

    async def scrape(name, delay, need):
        await asyncio.sleep(delay)
        jobs = [_job(i, name) for i in range(need)]
        seen = []

        async def fetch(j):
            seen.append(j.job_id)
            return True
        await scraper._detail_pass(None, name, jobs, b, fetch, "Test")
        got[name] = len(seen)

    async def main():
        b.expect(["tiny", "big", "slow"])
        await asyncio.gather(scraper._tenant_reporting(b, "tiny", scrape("tiny", 0, 0)),
                             scraper._tenant_reporting(b, "big", scrape("big", 0, 5000)),
                             scraper._tenant_reporting(b, "slow", scrape("slow", 0.3, 5000)))

    asyncio.run(main())
    # tiny's unused floor (300) is split with the still-listing tenant held in reserve.
    assert got["big"] == 450 and got["slow"] == 450


def test_sibling_sites_fetch_a_requisition_once_and_the_body_survives_dedupe():
    b = scraper._DescBudget(1000)
    fetched = []
    desc = "Full posting body. " * 100

    async def fetch(j):
        fetched.append((j.hospital_system, j.job_id))
        j.description = desc
        return True

    cx2 = [_job(i, "Northwell Health") for i in range(0, 60)]
    cx1 = [_job(i, "Northwell Health (CX_1)") for i in range(40, 80)]

    async def main():
        b.expect(["Northwell Health", "Northwell Health (CX_1)"])
        await asyncio.gather(
            scraper._tenant_reporting(b, "Northwell Health",
                                      scraper._detail_pass(None, "Northwell Health", cx2, b, fetch, "Test")),
            scraper._tenant_reporting(b, "Northwell Health (CX_1)",
                                      scraper._detail_pass(None, "Northwell Health (CX_1)", cx1, b, fetch, "Test")))

    asyncio.run(main())
    ids = [jid for _, jid in fetched]
    assert len(ids) == len(set(ids)) == 80
    # finalize_jobs keeps whichever copy carries the body.
    rows = scraper.finalize_jobs(cx2 + cx1)
    by_id = {r["job_id"]: r for r in rows}
    assert len(by_id) == 80 and all(len(r["description"]) > 1000 for r in by_id.values())


def test_load_known_bodies_pages_by_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    pages = [
        [{"id": 5, "hospital_system": "VITAS Healthcare", "job_id": "40788", "desc_len": 2712},
         {"id": 9, "hospital_system": "Guthrie", "job_id": "7", "desc_len": 150}],
        [{"id": 12, "hospital_system": "Guthrie", "job_id": "8", "desc_len": 1600}],
        [],
    ]
    urls = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(rq, timeout=0):
        urls.append(rq.full_url)
        return _Resp(json.dumps(pages[len(urls) - 1]).encode())

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert scraper.load_known_bodies() == 2
    assert "id=gt.0" in urls[0] and "id=gt.9" in urls[1] and "id=gt.12" in urls[2]
    assert all("desc_len=gte.200" in u and "is_active=is.true" in u and "%22Oracle%20HCM%22" in u for u in urls)
    assert scraper._KNOWN_BODIES[("VITAS Healthcare", "40788")] == 2712


def test_load_known_bodies_without_credentials(monkeypatch):
    for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert scraper.load_known_bodies() == 0
    assert scraper._KNOWN_BODIES == {}


def test_budget_defaults_2026_09_24():
    assert scraper.ORACLE_DESC_MAX_PER_RUN >= 10000 or scraper.os.getenv("ORACLE_DESC_MAX_PER_RUN")
    assert scraper.WD_DESC_MAX_PER_RUN >= 10000 or scraper.os.getenv("WD_DESC_MAX_PER_RUN")
    assert scraper.DETAIL_TENANT_MAX >= 500
