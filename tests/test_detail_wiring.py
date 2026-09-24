"""Detail passes for the runners that listed titles only (2026-09-24):
Kaiser, UHG, Enhabit, Maxim (TalentBrew job pages), CHRISTUS (JSON-LD),
Houston Methodist (Workday CXS), SmartRecruiters, ADP, Paycor, Paylocity and
Workable. No network: `req` and `_curl_fetch` are replaced by fakes."""
import asyncio
import json

import pytest

import scraper
from scraper import Job


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {"content-type": "text/html" if isinstance(body, str) else "application/json"}

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _serve(monkeypatch, routes):
    """Route req() by URL substring; returns the list of (method, url, params)."""
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append((method, url, kw.get("params")))
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(_R(status, body))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return seen


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    """No real pauses, detail passes on, fresh budgets."""
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    for name in ("TB_PAGE", "CUSTOM", "SR", "ADP", "PAYCOR", "PAYLOCITY", "WORKABLE", "WD"):
        monkeypatch.setattr(scraper, f"{name}_DESC_BUDGET", scraper._DescBudget(1000))


BODY = "The Registered Nurse assesses, plans and evaluates care for assigned patients. " * 4

KAISER_PAGE = '''<html><head><script type="application/ld+json">
{"@context":"http://schema.org","@type":"JobPosting","datePosted":"2026-9-21","employmentType":"Standard",
 "description":"<p>Job Summary:</p><p>%s</p>","qualifications":"<p>Basic Qualifications:</p><ul><li>BLS required.</li></ul>",
 "jobLocation":[{"@type":"Place","address":{"addressLocality":"Walnut Creek","addressRegion":"CA","addressCountry":"US"}}]}
</script></head><body>
<div class="ats-description"><div><b>Job Summary:</b></div><div>%s</div></div>
<div class="ats-extras">
  <span class="job-info"><strong>Primary Location:</strong> California,Walnut Creek,Walnut Creek Hospital</span>
  <span class="job-info"><strong>Scheduled Weekly Hours:</strong> 28</span>
  <span class="job-info"><strong>Shift:</strong> Evening </span>
  <span class="job-info"><strong>Job Schedule:</strong> Part-time</span>
  <span class="job-info"><strong>Pay Range:</strong> $89.75 - $106.61 / hour</span>
  <span class="job-info-descriptor">Kaiser Permanente strives to offer a market competitive total rewards package.</span>
</div></body></html>''' % (BODY, BODY)

MAXIM_PAGE = '''<html><body><dl>
<div><dt>Date posted</dt><dd class="job-description__desc-detail job-detail-date-posted">08/26/2026</dd></div>
<div><dt>Location</dt><dd class="job-description__desc-detail job-detail-location">Grove City, Ohio</dd></div>
</dl>
<div class="ats-description"><div><span>Hourly Pay: LPN $25 - $32</span></div>
<div><ul><li>%s</li></ul></div><div><b>Requirements:</b></div><div><ul><li>Current LPN license.</li></ul></div></div>
<div class="footer">Not part of the posting</div></body></html>''' % BODY


def test_tb_page_kaiser_extras_schedule_and_pay(monkeypatch):
    _serve(monkeypatch, [("kaiserpermanentejobs.org", 200, KAISER_PAGE)])
    j = _job(url="https://www.kaiserpermanentejobs.org/job/walnut-creek/staff-nurse-ii/641/1", state="CA")
    assert asyncio.run(scraper._tb_page_detail(None, j)) is True
    assert j.job_type == "Part-time"               # the page's Job Schedule, not JSON-LD "Standard"
    assert j.posted_date == "2026-09-21"
    assert "BLS required" in j.description and "Registered Nurse assesses" in j.description
    assert "Pay Range: $89.75 - $106.61 / hour" in j.description
    assert "Shift: Evening" in j.description and "Scheduled Weekly Hours: 28" in j.description
    assert "Primary Location" not in j.description and "total rewards" not in j.description
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (89.75, 106.61, "hour")


def test_tb_page_maxim_ats_block_date_and_state(monkeypatch):
    _serve(monkeypatch, [("maximhealthcare.com", 200, MAXIM_PAGE)])
    j = _job(url="https://careers.maximhealthcare.com/job/grove-city/lpn/49382/1", city="Grove City",
             location="Grove City")
    assert asyncio.run(scraper._tb_page_detail(None, j)) is True
    assert "Hourly Pay: LPN $25 - $32" in j.description and "Current LPN license" in j.description
    assert "Not part of the posting" not in j.description   # the nesting-aware block ends at its own </div>
    assert j.posted_date == "2026-08-26"
    assert (j.state, j.location) == ("OH", "Grove City, OH")


def test_fill_state_rules():
    j = _job(city="Idabel")
    assert scraper._fill_state(j, "Idabel", "Oklahoma", "United States") is True and j.state == "OK"
    kept = _job(state="TX", city="Austin")
    assert scraper._fill_state(kept, "Tulsa", "OK", "US") is False and kept.state == "TX"
    foreign = _job(city="Hyderabad")
    assert scraper._fill_state(foreign, "Hyderabad", "Telangana", "India") is False and foreign.state == ""
    assert scraper._fill_state(_job(), "Somewhere", "Ontario", "") is False


def test_sr_detail_sections_and_type(monkeypatch):
    payload = {"typeOfEmployment": {"label": "Part-time"}, "releasedDate": "2026-08-20T20:12:12.762Z",
               "jobAd": {"sections": {
                   "companyDescription": {"title": "Company Description", "text": "<p>We are a health system.</p>"},
                   "jobDescription": {"title": "Job Description", "text": "<p>Shift: 6 PM - 2:30 AM</p><p>%s</p>" % BODY},
                   "qualifications": {"title": "Qualifications", "text": "<p>Requires a high school diploma or GED.</p>"},
                   "additionalInformation": {"title": "Additional Information", "text": ""}}}}
    seen = _serve(monkeypatch, [("api.smartrecruiters.com", 200, payload)])
    j = _job(url="https://jobs.smartrecruiters.com/HenryFordHealth1/3743990014723486", job_id="3743990014723486")
    assert asyncio.run(scraper._sr_detail(None, j)) is True
    assert seen[0][1] == "https://api.smartrecruiters.com/v1/companies/HenryFordHealth1/postings/3743990014723486"
    assert j.description.startswith("Job Description\nShift: 6 PM")
    assert j.description.index("Qualifications") < j.description.index("Company Description")
    assert j.job_type == "Part-time" and j.posted_date == "2026-08-20"


def test_adp_detail_uses_stored_career_center_ids(monkeypatch):
    seen = _serve(monkeypatch, [("job-requisitions/9200965483764_1", 200,
                                 {"requisitionDescription": "<p>%s</p>" % BODY, "postDate": "2026-05-12T16:10:00.000-04:00"})])
    j = _job(url=f"{scraper._ADP_PORTAL}?cid=76d82eed&ccId=19000101_000001&lang=en_US"
                 f"&selectedMenuKey=CareerCenter&jobId=9200965483764_1", job_id="9200965483764_1")
    assert asyncio.run(scraper._adp_detail(None, j)) is True
    method, url, params = seen[0]
    assert url == f"{scraper._ADP_API}/9200965483764_1"
    assert params["cid"] == "76d82eed" and params["ccId"] == "19000101_000001"
    assert "Registered Nurse assesses" in j.description and j.posted_date == "2026-05-12"


PAYCOR_PAGE = '''<table id="gnewtonJobDescription"><tr>
<td id="gnewtonJobDescriptionText">
<div><b>Townsen Memorial is</b> looking for a PRN Sterile Processing Technician!</div>
<div><table><tr><td>%s</td></tr></table></div><br/>
</td></tr><tr><td id="gnewtonJobDescriptionBtn"><input type="button" value="Apply"/></td></tr></table>''' % BODY


def test_paycor_detail_block(monkeypatch):
    _serve(monkeypatch, [("JobIntroduction.action", 200, PAYCOR_PAGE)])
    j = _job(url="https://recruitingbypaycor.com/career/JobIntroduction.action?clientId=a&id=b&source=&lang=en")
    assert asyncio.run(scraper._paycor_detail(None, j)) is True
    assert j.description.startswith("Townsen Memorial is looking for a PRN")
    assert "Registered Nurse assesses" in j.description and "Apply" not in j.description and "<td" not in j.description


def test_workable_detail_joins_requirements_and_benefits(monkeypatch):
    seen = _serve(monkeypatch, [("api/v2/accounts/huntsville-memorial-hospital/jobs/9D49579863", 200,
                                 {"description": "<p>%s</p>" % BODY, "requirements": "<ul><li>Texas RN license</li></ul>",
                                  "benefits": "<p>Medical, dental and vision</p>"})])
    j = _job(url="https://apply.workable.com/huntsville-memorial-hospital/j/9D49579863/")
    assert asyncio.run(scraper._workable_detail(None, j)) is True
    assert "Requirements\nTexas RN license" in j.description and "Benefits\nMedical, dental and vision" in j.description
    assert seen[0][1].endswith("/api/v2/accounts/huntsville-memorial-hospital/jobs/9D49579863")


def test_hm_detail_sync_iso_date_and_type(monkeypatch):
    calls = []

    class _Resp:
        def json(self):
            return {"jobPostingInfo": {"jobDescription": "<p>%s</p>" % BODY, "startDate": "2026-08-24",
                                       "timeType": "Full time"}}

    def fake_curl(method, url, impersonate, timeout=60, **kw):
        calls.append((method, url))
        return _Resp()
    monkeypatch.setattr(scraper, "_curl_fetch", fake_curl)
    path = "/job/HM-West---Main-Hospital-Building/Sr-Surgical-Technologist_JR-13515-2"
    j = _job(url=scraper.HM_PUBLIC_BASE + path, posted_date="Posted 30+ Days Ago")
    assert scraper._hm_detail_sync(j) is True
    assert calls == [("get", "https://houstonmethodist.wd12.myworkdayjobs.com/wday/cxs/houstonmethodist/GTI" + path)]
    assert j.posted_date == "2026-08-24" and j.job_type == "Full time"
    assert scraper._hm_detail_sync(_job(url="https://elsewhere/job/1")) is False


@pytest.mark.parametrize("runner,lister,system", [
    ("run_kaiser", "scrape_kaiser_html", "Kaiser Permanente"),
    ("run_uhg", "scrape_uhg_talentbrew", "UnitedHealth Group"),
    ("run_enhabit", "scrape_enhabit_html", "Enhabit Home Health"),
    ("run_maxim", "scrape_maxim_html", "Maxim Healthcare"),
])
def test_talentbrew_html_runners_run_the_page_pass(monkeypatch, runner, lister, system):
    rows = [_job(hospital_system=system, url=f"https://t/job/c/t/1/{i}", job_id=str(i)) for i in range(5)]

    async def fake_list(session):
        return rows
    fetched = []

    async def fake_detail(session, job):
        fetched.append(job.job_id)
        job.description = BODY
        return True
    monkeypatch.setattr(scraper, lister, fake_list)
    monkeypatch.setattr(scraper, "_tb_page_detail", fake_detail)
    monkeypatch.setattr(scraper, "TB_PAGE_DESC_BUDGET", scraper._DescBudget(3))
    out = asyncio.run(getattr(scraper, runner)(None))
    assert out is rows and len(fetched) == 3            # the budget caps the pass
    assert scraper.TB_PAGE_DESC_BUDGET.remaining == 0


def test_houston_methodist_runner_spends_the_workday_budget(monkeypatch):
    rows = [_job(hospital_system="Houston Methodist", url=scraper.HM_PUBLIC_BASE + f"/job/x_JR-{i}", job_id=f"JR-{i}")
            for i in range(4)]
    monkeypatch.setattr(scraper, "curl_requests", object())
    monkeypatch.setattr(scraper, "_hm_fetch_all", lambda: rows)
    fetched = []

    def fake_detail(job):
        fetched.append(job.job_id)
        return True
    monkeypatch.setattr(scraper, "_hm_detail_sync", fake_detail)
    assert asyncio.run(scraper.run_houston_methodist()) is rows
    assert sorted(fetched) == [f"JR-{i}" for i in range(4)]
    assert scraper.WD_DESC_BUDGET.spent == 4


def test_smartrecruiters_runner_passes_per_tenant(monkeypatch):
    monkeypatch.setattr(scraper, "SMARTRECRUITERS_ORGS", {"A": "a", "B": "b"})

    async def fake_list(session, system, org):
        return [_job(hospital_system=system, url=f"https://jobs.smartrecruiters.com/{org}/{i}", job_id=f"{org}{i}")
                for i in range(3)]
    fetched = []

    async def fake_detail(session, job):
        fetched.append(job.job_id)
        return True
    monkeypatch.setattr(scraper, "scrape_smartrecruiters", fake_list)
    monkeypatch.setattr(scraper, "_sr_detail", fake_detail)
    jobs = asyncio.run(scraper.run_smartrecruiters(None))
    assert len(jobs) == 6 and sorted(fetched) == ["a0", "a1", "a2", "b0", "b1", "b2"]


def test_by_system_gate_limits_requests_in_flight(monkeypatch):
    live, peak, fetched = 0, 0, []

    async def fetch(job):
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        for _ in range(3):
            await asyncio.sleep(0)
        live -= 1
        fetched.append(job.job_id)
        return True
    jobs = [_job(hospital_system=f"S{t}", url=f"https://h/{t}/{i}", job_id=f"{t}-{i}") for t in range(4) for i in range(5)]
    budget = scraper._DescBudget(200)
    asyncio.run(scraper._detail_passes_by_system(None, jobs, budget, fetch, "Test", in_flight=2))
    assert len(fetched) == 20 and peak <= 2
    # the fair-share cap still applies per tenant: budget/DETAIL_TENANT_SHARE, at least 50
    small = scraper._DescBudget(8)
    got = []

    async def fetch2(job):
        got.append(job.hospital_system)
        return True
    asyncio.run(scraper._detail_passes_by_system(None, jobs, small, fetch2, "Test", in_flight=2))
    assert len(got) == 8 and small.remaining == 0


def test_playwright_christus_pass_skips_search_page_rows(monkeypatch):
    fetched = []

    async def fake_jsonld(session, job):
        fetched.append(job.url)
        return True
    monkeypatch.setattr(scraper, "_jsonld_detail", fake_jsonld)
    rows = [_job(hospital_system="CHRISTUS Health", url="https://careers.christushealth.org/opportunity/rn-1", job_id="1"),
            _job(hospital_system="CHRISTUS Health", url="https://careers.christushealth.org/job-search", job_id="2"),
            _job(hospital_system="MUSC Health", url="https://musc.career-pages.com/jobs/3", job_id="3")]
    asyncio.run(scraper._playwright_detail_passes(rows))
    assert fetched == ["https://careers.christushealth.org/opportunity/rn-1"]


def test_new_budgets_exist():
    for name in ("TB_PAGE", "CUSTOM", "SR", "ADP", "PAYCOR", "PAYLOCITY", "WORKABLE"):
        assert getattr(scraper, f"{name}_DESC_MAX_PER_RUN") > 0
