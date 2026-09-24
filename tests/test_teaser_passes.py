"""Push 3 teaser passes (2026-09-24): iCIMS job page JSON-LD (in_iframe=1),
UKG OpportunityDetail JSON, NeoGov job page JSON-LD, CareerPlug on the budget
framework, Paycom's qualifications field. Bodies under tests/fixtures/teasers
were fetched live on 2026-09-24 and trimmed to the part the parser reads.
No network: `req` is replaced by a fake."""
import asyncio
import json
import os

import pytest

import scraper
from scraper import Job

from conftest import FIXTURES

TD = os.path.join(FIXTURES, "teasers")


def _read(name):
    with open(os.path.join(TD, name), encoding="utf-8") as f:
        return f.read()


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://careers-x.icims.com/jobs/1/rn/job", job_id="1",
                posted_date="", description="", ats_platform="iCIMS")
    base.update(kw)
    return Job(**base)


def _reqs(job):
    row = scraper.normalize_job(job)
    return ((row.get("posting_facts") or {}).get("requirements")) or {}


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {"content-type": "text/html"}

    async def text(self):
        return self._body

    async def json(self, content_type=None):
        return json.loads(self._body)


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _serve(monkeypatch, routes):
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append(url)
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(_R(status, body))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return seen


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    async def no_sleep(*a, **k):
        return None
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "jitter", no_sleep)
    scraper.set_known_bodies([])
    for b in (scraper.ICIMS_DESC_BUDGET, scraper.UKG_DESC_BUDGET, scraper.NEOGOV_DESC_BUDGET,
              scraper.CAREERPLUG_DESC_BUDGET):
        b.__init__(b.total)
    yield
    scraper.set_known_bodies([])


# ── iCIMS ────────────────────────────────────────────────────────────────
def test_icims_detail_url_adds_in_iframe_once():
    u = "https://jobs-selectmedicalcorp.icims.com/jobs/377704/physical-therapist/job"
    assert scraper._icims_detail_url(u) == u + "?in_iframe=1"
    assert scraper._icims_detail_url(u + "?mode=job") == u + "?mode=job&in_iframe=1"
    assert scraper._icims_detail_url(u + "?in_iframe=1") == u + "?in_iframe=1"


@pytest.mark.parametrize("fixture,min_len,lic,cert", [
    ("icims_selectmedical_372249.html", 1800, "Valid State RN License", "BLS is required at hire."),
    ("icims_prime_278990.html", 4200, "registration with State Board", "BCLS"),
    ("icims_covenant_76032.html", 2100, "Tennessee State medical technologist licensure", "ASCP"),
])
def test_icims_page_fills_body_and_requirements(fixture, min_len, lic, cert):
    job = _job(description="Card teaser. " * 40)          # ~520 characters, like the list
    assert _reqs(_job(description=job.description)).get("licensure", []) == []
    assert scraper._icims_apply_page(job, _read(fixture))
    assert len(job.description) >= min_len
    assert "Qualifications" in job.description
    rq = _reqs(job)
    assert any(lic in x[0] for x in rq["licensure"])
    assert any(cert in x[0] for x in rq["certifications"])
    assert job.posted_date.startswith("2026-")
    assert job.job_type == ""                               # OTHER / CONTRACTOR are not taken


def test_icims_page_fills_a_blank_state_only():
    job = _job(description="")
    scraper._icims_apply_page(job, _read("icims_covenant_76032.html"))
    assert (job.city, job.state) == ("Lenoir City", "TN")
    kept = _job(city="Knoxville", state="TN")
    scraper._icims_apply_page(kept, _read("icims_prime_278990.html"))
    assert (kept.city, kept.state) == ("Knoxville", "TN")


def test_icims_page_without_jsonld_changes_nothing():
    job = _job(description="teaser", job_type="Full Time")
    assert not scraper._icims_apply_page(job, "<html>Human Verification</html>")
    assert (job.description, job.job_type) == ("teaser", "Full Time")


def test_icims_tenant_pass_fetches_iframe_page_and_keeps_failed_rows(monkeypatch):
    jobs = [_job(job_id=str(i), url=f"https://careers-x.icims.com/jobs/{i}/rn/job",
                 description="teaser " * 30) for i in range(1, 5)]

    async def fake_list(session, system, domain):
        return list(jobs)
    monkeypatch.setattr(scraper, "scrape_icims", fake_list)
    page = _read("icims_prime_278990.html")
    seen = _serve(monkeypatch, [("/jobs/1/", 200, page), ("/jobs/2/", 200, page),
                                ("/jobs/3/", 405, "<html>challenge</html>")])   # 4: 404
    budget = scraper.ICIMS_DESC_BUDGET
    budget.__init__(100)
    budget.expect(["X"])

    async def go():
        fetch = scraper._host_gated(lambda j: scraper._icims_detail(None, j), 6)
        return await scraper._tenant_reporting(budget, "X", scraper._icims_tenant(None, "X", "careers-x.icims.com", fetch))
    out = asyncio.run(go())
    assert len(out) == 4                                    # no listed row dropped
    assert all("in_iframe=1" in u for u in seen) and len(seen) == 4
    filled = {j.job_id for j in out if len(j.description) > 4000}
    assert filled == {"1", "2"}
    assert {j.job_id: j.description for j in out if j.job_id in ("3", "4")} == \
        {"3": "teaser " * 30, "4": "teaser " * 30}
    assert budget.spent == 4


def test_icims_known_body_is_not_refetched(monkeypatch):
    scraper.set_known_bodies([{"hospital_system": "X", "job_id": "1", "desc_len": 4287}])
    jobs = [_job(job_id="1", description="teaser " * 30), _job(job_id="2", url="https://careers-x.icims.com/jobs/2/rn/job",
                                                                description="teaser " * 30)]
    seen = _serve(monkeypatch, [("/jobs/", 200, _read("icims_prime_278990.html"))])
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    asyncio.run(scraper._detail_pass(None, "X", jobs, scraper.ICIMS_DESC_BUDGET,
                                     lambda j: scraper._icims_detail(None, j), "iCIMS"))
    assert len(seen) == 1 and "/jobs/2/" in seen[0]
    assert jobs[0].description == ""                        # blank: the trigger keeps the stored body


def test_icims_platform_in_known_bodies():
    for plat in ("iCIMS", "UKG", "NeoGov", "CareerPlug"):
        assert plat in scraper.KNOWN_BODY_PLATFORMS


# ── UKG ──────────────────────────────────────────────────────────────────
def test_ukg_detail_json_fills_body_type_and_date():
    job = _job(ats_platform="UKG", description="Monitors and cleans equipment.", job_type="")
    d = scraper._ukg_detail_data(_read("ukg_cape_regional.html"))
    assert d and d["Id"]
    assert scraper._ukg_apply_detail(job, d)
    assert len(job.description) > 2000
    assert job.job_type == "Part time"
    assert job.posted_date == "2026-09-09"
    rq = _reqs(job)
    assert any("High school diploma" in x[0] for x in rq["education"])
    assert rq["certifications"]


def test_ukg_short_posting_keeps_the_list_text():
    listed = "x" * 401
    job = _job(ats_platform="UKG", description=listed)
    assert not scraper._ukg_apply_detail(job, scraper._ukg_detail_data(_read("ukg_granite_hills_short.html")))
    assert job.description == listed


def test_ukg_criteria_and_pay_range():
    d = {"Description": "<p>" + "Duties. " * 40 + "</p>", "FullTime": True, "PayRangeVisible": True,
         "PayRange": {"PayRangeMinimum": 31.5, "PayRangeMaximum": 44.0},
         "LicenseAndCertificationCriteria": [{"LicenseAndCertificationName": "Registered Nurse (RN)", "Required": True}],
         "EducationCriteria": [{"EducationLevelDescription": "Bachelor's Degree", "Required": False}],
         "SkillCriteria": [{"SkillName": "Epic", "MinimumScaleValueDescription": "Expert", "Required": False}]}
    job = _job(ats_platform="UKG", job_type="")
    assert scraper._ukg_apply_detail(job, d)
    assert "Licenses and Certifications\nRegistered Nurse (RN) (required)" in job.description
    assert "Education\nBachelor's Degree (preferred)" in job.description
    assert "Skills\nEpic (preferred)" in job.description
    assert (job.wage_min, job.wage_max, job.wage_unit) == (31.5, 44.0, "hour")
    assert job.job_type == "Full time"
    assert scraper._ukg_detail_data("<html>no opportunity</html>") is None


def test_ukg_runner_runs_the_budgeted_pass(monkeypatch):
    async def fake_scrape(session, system, org):
        return [_job(hospital_system=system, ats_platform="UKG", job_id=system[:3] + "1",
                     url=f"{org[0]}/JobBoard/{org[1]}/OpportunityDetail?opportunityId=1", description="teaser")]
    monkeypatch.setattr(scraper, "scrape_ukg", fake_scrape)
    monkeypatch.setattr(scraper, "UKG_ORGS", {"Cape Regional Health": ("https://crhukg.rec.pro.ukg.net/CHE1503CHPE", "g1"),
                                              "Excela Health": ("https://recruiting.ultipro.com/EXC1005EXCEH", "g2")})
    seen = _serve(monkeypatch, [("OpportunityDetail", 200, _read("ukg_cape_regional.html"))])
    out = asyncio.run(scraper.run_ukg(None))
    assert len(out) == 2 and len(seen) == 2
    assert all(len(j.description) > 2000 for j in out)


# ── host gate ────────────────────────────────────────────────────────────
def test_host_gate_limits_per_host_and_total():
    live = {"a": 0, "b": 0, "c": 0}
    peak = {"a": 0, "b": 0, "c": 0, "all": 0}

    async def fetch(job):
        h = job.url.split("/")[2][0]
        live[h] += 1
        peak[h] = max(peak[h], live[h])
        peak["all"] = max(peak["all"], sum(live.values()))
        await _real_sleep(0.01)
        live[h] -= 1
        return True
    gated = scraper._host_gated(fetch, 3, per_host=2, pause=(0, 0))
    jobs = [_job(url=f"https://{h}.example.com/jobs/{i}", hospital_system=h) for h in "abc" for i in range(6)]

    async def go():
        await asyncio.gather(*[gated(j) for j in jobs])
    asyncio.run(go())
    assert max(peak[h] for h in "abc") <= 2 and peak["all"] <= 3


_real_sleep = asyncio.sleep


def test_host_gate_breaker_stops_a_dead_tenant(monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_BREAKER_TRIES", 5)
    calls = []

    async def fetch(job):
        calls.append(job.hospital_system)
        return job.hospital_system == "ok"
    gated = scraper._host_gated(fetch, 1, pause=(0, 0))

    async def go():
        for i in range(12):
            await gated(_job(hospital_system="dead", job_id=str(i)))
            await gated(_job(hospital_system="ok", job_id=str(i)))
    asyncio.run(go())
    assert calls.count("dead") == 5 and calls.count("ok") == 12


# ── NeoGov ───────────────────────────────────────────────────────────────
def test_neogov_jsonld_full_bulletin():
    job = _job(ats_platform="NeoGov", url="https://www.governmentjobs.com/careers/riverside/jobs/5479054/x",
               description="Salary: $45.75 - $80.24 Hourly. excerpt " * 18, job_type="Full Time")
    posting = scraper._jobposting_from_html(_read("neogov_riverside_5479054.html"))
    assert scraper._apply_posting(job, posting)
    assert len(job.description) > 9000
    assert job.posted_date == "2026-09-16"
    assert (job.wage_min, job.wage_max, job.wage_unit) == (45.75, 80.24, "hour")
    rq = _reqs(job)
    assert any("Registered Nurse in the State of California" in x[0] for x in rq["licensure"])
    assert len(rq["qualifications"]["required"]) >= 10


def test_neogov_runner_pass_one_in_flight(monkeypatch):
    async def fake_scrape(session, system, cfg):
        return [_job(hospital_system=system, ats_platform="NeoGov", job_id=f"{system[:2]}{i}",
                     url=f"https://www.governmentjobs.com/careers/{cfg[0]}/jobs/{i}/x", description="excerpt") for i in range(3)]
    monkeypatch.setattr(scraper, "scrape_neogov", fake_scrape)
    monkeypatch.setattr(scraper, "NEOGOV_AGENCIES", {"Riverside University Health System": ("riverside", "CA", "", "R", None)})
    seen = _serve(monkeypatch, [("/jobs/", 200, _read("neogov_riverside_5479054.html"))])
    out = asyncio.run(scraper.run_neogov(None))
    assert len(out) == 3 and len(seen) == 3
    assert all(len(j.description) > 9000 for j in out)


# ── CareerPlug ───────────────────────────────────────────────────────────
def test_careerplug_pass_skips_known_bodies_and_uses_budget(monkeypatch):
    jobs = [_job(hospital_system="American Family Care", ats_platform="CareerPlug", job_id=str(i),
                 url=f"https://american-family-care-careers.careerplug.com/jobs/{i}", description="") for i in range(6)]

    async def fake_list(session, system, slug):
        return list(jobs)
    monkeypatch.setattr(scraper, "scrape_careerplug", fake_list)
    fetched = []

    async def fake_detail(session, url):
        fetched.append(url.rsplit("/", 1)[1])
        return "Responsibilities\n" + "Treat patients. " * 30
    monkeypatch.setattr(scraper, "_careerplug_detail", fake_detail)
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    scraper.set_known_bodies([{"hospital_system": "American Family Care", "job_id": "0", "desc_len": 2600},
                              {"hospital_system": "American Family Care", "job_id": "1", "desc_len": 5499}])
    scraper.CAREERPLUG_DESC_BUDGET.__init__(3)
    out = asyncio.run(scraper.run_careerplug(None))
    assert len(out) == 6
    assert len(fetched) == 3 and not {"0", "1"} & set(fetched)
    assert sum(1 for j in out if j.description.startswith("Responsibilities")) == 3


# ── Paycom ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize("jid,lic,edu,shift", [
    ("335044", "Current North Carolina RN License.", None, "Shift: Night"),
    ("318211", None, "First year of a two-year nursing program", "Shift: Rotating"),
])
def test_paycom_detail_keeps_the_qualifications_field(jid, lic, edu, shift):
    det = json.loads(_read(f"paycom_detail_{jid}.json"))
    jp = det.get("jobPosting", det)
    duty_only = scraper._paycom_clean(jp["description"])
    job = scraper._paycom_job({"jobId": jid, "jobTitle": "RN"}, det, "Paycom Hospital 2", "K")
    assert len(job.description) > len(duty_only) + 300
    assert "Qualifications" in job.description and shift in job.description
    rq = _reqs(job)
    if lic:
        assert any(lic in x[0] for x in rq["licensure"])
    if edu:
        assert any(edu in x[0] for x in rq["education"])
    assert rq["certifications"]


def test_paycom_without_detail_is_unchanged():
    job = scraper._paycom_job({"jobId": "9", "jobTitle": "RN", "description": "Short preview text."}, None, "S", "K")
    assert job.description == "Short preview text."
