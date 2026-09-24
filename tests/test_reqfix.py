"""push3/reqfix (2026-09-24): University Health TalentBrew page detail,
requirement false positives found in the 40-posting hand check, one-line
bodies of the still-low systems, and the small misfiles (Licensures, VITAS
"equivalent experience or licensure", Phenom schedule line inside 12,000)."""
import asyncio
import os

import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


class _R:
    def __init__(self, body):
        self.status, self._body = 200, body

    async def text(self):
        return self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def test_university_health_page_without_jsonld(monkeypatch):
    html = open(os.path.join(FIX, "uh_tb_page_100998069312.html"), encoding="utf-8").read()
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(html)))
    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: asyncio.sleep(0, result=html))
    url = "https://careers.universityhealth.com/job/san-antonio/med-surg-technician/43277/100998069312"
    old = _job(hospital_system="University Health (San Antonio)", url=url, job_id="100998069312")
    assert asyncio.run(scraper._jsonld_detail(None, old)) is False          # the old pass: nothing
    new = _job(hospital_system="University Health (San Antonio)", url=url, job_id="100998069312")
    assert asyncio.run(scraper._tb_page_detail(None, new)) is True
    assert len(new.description) >= 500
    assert "high school diploma" in new.description.lower()
    rq = scraper.extract_requirements(new.description)
    assert rq["qualifications"]["required"] and rq["education"]


def test_university_health_uses_tb_page_detail(monkeypatch):
    assert "University Health (San Antonio)" in scraper.TB_PAGE_DETAIL_ORGS
    used = []

    async def fake_scrape(session, sys, url, rpp):
        return [_job(hospital_system=sys, url=f"https://x/{sys}", job_id=sys)]

    async def fake_pass(session, system, jobs, budget, fetch_one, label, **kw):
        assert budget is scraper.TB_DESC_BUDGET
        await fetch_one(jobs[0])

    async def page(session, job):
        used.append(("page", job.hospital_system))
        return True

    async def jsonld(session, job):
        used.append(("jsonld", job.hospital_system))
        return True
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    monkeypatch.setattr(scraper, "scrape_talentbrew", fake_scrape)
    monkeypatch.setattr(scraper, "_detail_pass", fake_pass)
    monkeypatch.setattr(scraper, "_tb_page_detail", page)
    monkeypatch.setattr(scraper, "_jsonld_detail", jsonld)
    asyncio.run(scraper.run_talentbrew(None))
    assert ("page", "University Health (San Antonio)") in used
    assert ("jsonld", "ScionHealth") in used
    assert ("jsonld", "University Health (San Antonio)") not in used
