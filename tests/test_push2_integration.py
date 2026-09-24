"""Push 2 integration (2026-09-24): the detail passes push2/wiring added share
push2/budget's rules (known bodies cost nothing, a registered fair split) and
push2/facts' 12,000-character body. No network, no database."""
import asyncio

import pytest

import scraper
from scraper import Job


def _job(i, system="S", desc="", platform="SmartRecruiters"):
    return Job(title=f"RN {i}", hospital_system=system, hospital_name=system, city="", state="",
               location="", specialty="", job_type="", url=f"https://h/{system}/{i}",
               job_id=str(i), posted_date="", description=desc, ats_platform=platform)


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)   # see test_known_body_refresh.py
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    yield
    scraper.set_known_bodies([])


def test_known_body_load_covers_every_wired_platform():
    for plat in ("Custom", "SmartRecruiters", "ADP", "Paycor", "Paylocity", "Workable",
                 "TalentBrew", "Workday", "Oracle HCM", "Phenom"):
        assert plat in scraper.KNOWN_BODY_PLATFORMS


def test_by_system_pass_skips_known_bodies():
    scraper.set_known_bodies([{"hospital_system": "Henry Ford Health", "job_id": str(i), "desc_len": 2400}
                              for i in range(3)])
    jobs = [_job(i, "Henry Ford Health") for i in range(5)]
    fetched = []

    async def fetch(job):
        fetched.append(job.job_id)
        return True
    budget = scraper._DescBudget(100)
    asyncio.run(scraper._detail_passes_by_system(None, jobs, budget, fetch, "SmartRecruiters"))
    assert sorted(fetched) == ["3", "4"] and budget.spent == 2


def test_by_system_pass_splits_what_small_tenants_leave():
    # Old rule: at most max(50, 100 // 4) = 50 each, first come. Now: a floor
    # of 33 each, the small tenant takes 5, and its 28 go to the two big ones.
    jobs = ([_job(i, "Small") for i in range(5)]
            + [_job(i, "BigA") for i in range(200)] + [_job(i, "BigB") for i in range(200)])
    got = {}

    async def fetch(job):
        got[job.hospital_system] = got.get(job.hospital_system, 0) + 1
        return True
    budget = scraper._DescBudget(100)
    asyncio.run(scraper._detail_passes_by_system(None, jobs, budget, fetch, "Test", in_flight=3))
    assert got["Small"] == 5 and got["BigA"] + got["BigB"] == 95
    assert min(got["BigA"], got["BigB"]) >= 47 and budget.remaining == 0
    assert budget.expected == {"Small", "BigA", "BigB"}


def test_by_system_pass_failure_still_reports_the_tenant(monkeypatch):
    real = scraper._detail_pass

    async def flaky(session, system, rows, budget, fetch_one, label, skip=None, share=None):
        if system == "Broken":
            raise RuntimeError("boom")
        return await real(session, system, rows, budget, fetch_one, label, skip=skip, share=share)
    monkeypatch.setattr(scraper, "_detail_pass", flaky)
    monkeypatch.setattr(scraper, "DETAIL_BARRIER_WAIT", 0.01)
    jobs = [_job(i, "Broken") for i in range(3)] + [_job(i, "Fine") for i in range(80)]
    fetched = []

    async def fetch(job):
        fetched.append(job.hospital_system)
        return True
    budget = scraper._DescBudget(60)
    asyncio.run(scraper._detail_passes_by_system(None, jobs, budget, fetch, "Test"))
    # the broken tenant's floor (30) goes to the one that wants more
    assert fetched.count("Fine") == 60 and "Broken" in budget.leftover


def test_wired_bodies_keep_12000_characters():
    long = "<p>" + ("Registered nurse duties and qualifications. " * 400) + "</p>"
    txt = scraper._sr_posting_text({"jobAd": {"sections": {"jobDescription": {"text": long},
                                                           "qualifications": {"text": long}}}})
    assert 8000 < len(txt) <= 12000
    assert len(scraper.strip_html("x" * 20000)) == 12000
