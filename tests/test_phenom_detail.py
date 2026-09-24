"""Phenom detail pass (2026-09-24, push 3).

Phenom stored teasers (25,684 active rows, median body 320 characters):
the jobDetail widget was only called for rows whose URL held "/job/", so
Jackson Health (Infor), Hendrick (HealthcareSource), Children's Health (Infor
short URLs) and CentraCare (Oracle preview links) got "0 descriptions";
DaVita's Workday rows read JSON-LD off the Workday apply step; BSW had no
detail pass. Saved bodies in tests/fixtures/phenom were fetched live on
2026-09-24 (widget jobDetail JSON trimmed to the fields the pass reads, and
one Workday CXS detail). No network: `req` is replaced by a fake."""
import asyncio
import json
import pathlib

import pytest

import scraper
from scraper import Job

FIX = pathlib.Path(__file__).parent / "fixtures" / "phenom"


def fx(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def _job(**kw):
    base = dict(title="RN", hospital_system="Jackson Health System", hospital_name="Jackson", city="Miami",
                state="FL", location="Miami, FL", specialty="", job_type="", url="https://x/apply",
                job_id="1", posted_date="", description="Teaser. " * 30, ats_platform="Phenom")
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
        if isinstance(self.r, Exception):
            raise self.r
        return self.r

    async def __aexit__(self, *a):
        return False


def _serve(monkeypatch, routes):
    """Route req() by URL substring; returns the list of (method, url, json)."""
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append((method, url, kw.get("json")))
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(body if isinstance(body, Exception) else _R(status, body))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return seen


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    monkeypatch.setattr(scraper, "PHENOM_DESC_BUDGET", scraper._DescBudget(1000))
    scraper.set_known_bodies([])


# ── URLs ────────────────────────────────────────────────────────────────────
DAVITA_APPLY = ("https://davita.wd1.myworkdayjobs.com/DKC_External/job/03055---Escondido-Dialysis/"
                "Clinical-Coordinator---Registered-Nurse_R0475128/apply")
BSMH_APPLY = ("https://wd5.myworkdaysite.com/recruiting/easyservice/MercyHealthCareers/job/Warren-OH/"
              "Chaplain-1---Pastoral-Care---St-Joseph-Warren-Hospital_R286671/apply")


def test_workday_job_url_strips_apply_step():
    assert scraper._workday_job_url(DAVITA_APPLY) == DAVITA_APPLY[:-len("/apply")]
    assert scraper._workday_job_url(BSMH_APPLY) == BSMH_APPLY[:-len("/apply")]
    assert scraper._workday_job_url(DAVITA_APPLY + "/applyManually") == DAVITA_APPLY[:-len("/apply")]
    assert scraper._workday_job_url(DAVITA_APPLY + "?source=Phenom") == DAVITA_APPLY[:-len("/apply")] + "?source=Phenom"
    # job page already, or not Workday: unchanged
    page = DAVITA_APPLY[:-len("/apply")]
    assert scraper._workday_job_url(page) == page
    for u in ("https://www.hhccareers.org/job/26160761", "https://baptisthlth.taleo.net/careersection/2/jobapply.ftl?job=1",
              "https://elyb.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs/preview/231954/apply/email",
              "https://davita.wd1.myworkdayjobs.com/DKC_External/apply", "", None):
        assert scraper._workday_job_url(u) == (u or "")


def test_workday_cxs_url_both_host_kinds():
    assert scraper._workday_cxs_url(DAVITA_APPLY) == (
        "https://davita.wd1.myworkdayjobs.com/wday/cxs/davita/DKC_External/job/03055---Escondido-Dialysis/"
        "Clinical-Coordinator---Registered-Nurse_R0475128")
    assert scraper._workday_cxs_url(BSMH_APPLY) == (
        "https://wd5.myworkdaysite.com/wday/cxs/easyservice/MercyHealthCareers/job/Warren-OH/"
        "Chaplain-1---Pastoral-Care---St-Joseph-Warren-Hospital_R286671")
    assert scraper._workday_cxs_url(
        "https://ssmh.wd115.myworkdayjobs.com/en-US/ssmhealth/job/MO-X/RN_R1/apply").endswith(
        "/wday/cxs/ssmh/ssmhealth/job/MO-X/RN_R1")
    assert scraper._workday_cxs_url("https://www.hhccareers.org/job/26160761") == ""
    assert scraper._workday_cxs_url("") == ""


def test_phenom_url_job_id():
    assert scraper._phenom_url_job_id("https://jobs.bswhealth.com/us/en/job/26007805") == "26007805"
    assert scraper._phenom_url_job_id("https://jobs.bswhealth.com/us/en/job/26007805/rn-icu?x=1") == "26007805"
    assert scraper._phenom_url_job_id("https://pm.healthcaresource.com/cs/ehendrick/#/preApply/1") == ""


# ── body text ───────────────────────────────────────────────────────────────
def test_posting_text_strips_shift_label_and_adds_missing_quals():
    jd = fx("widget_jackson_223645.json")["jobDetail"]["data"]["job"]
    desc, jt, created = scraper._phenom_posting_text(jd)
    assert len(desc) > 5000
    assert "Shift: Shift:" not in desc
    assert desc.rstrip().endswith("Schedule: Day Job") or "Day Job" in desc[:400]
    assert created == jd["dateCreated"][:10]
    # a separate qualifications field the description lacks is appended once
    extra = {"description": "<p>" + "Provide care. " * 20 + "</p>",
             "qualifications": "<ul><li>Current BLS certification required.</li><li>Florida RN license.</li></ul>"}
    d2, _, _ = scraper._phenom_posting_text(extra)
    assert d2.count("Qualifications") == 1 and "BLS certification" in d2
    # already held: not duplicated
    d3, _, _ = scraper._phenom_posting_text({"description": d2, "qualifications": extra["qualifications"]})
    assert d3.count("Current BLS certification required.") == 1


# ── routing ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("name,jid,url,base,min_len", [
    ("widget_jackson_223645.json", "223645",
     "https://css-f76fc6enwqtkl6kq-prd.inforcloudsuite.com:443/hcm/Jobs/navigation/JobPosting[JobPostingSet](JHS,223645,1)"
     ".JobPostingDisplayNav?csk.HROrganization=JHS&csk.JobBoard=EXTERNAL", "https://jobs.jacksonhealth.org", 5000),
    ("widget_hendrick_35517.json", "35517", "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25344",
     "https://careers.hendrickhealth.org", 500),     # the whole posting is short
    ("widget_childrens_32260.json", "32260", "https://gen-childrens-prd.inforcloudsuite.com/hcm/xmlhttp/shorturl.do?key=ZGJ",
     "https://jobsearch.childrens.com", 4000),
    ("widget_centracare_232098.json", "232098",
     "https://elyb.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs/preview/232098/apply/email",
     "https://jobs.centracare.com", 1500),
])
def test_widget_called_whatever_the_apply_link(monkeypatch, name, jid, url, base, min_len):
    """The four tenants that returned "0 descriptions" on 09-22."""
    seen = _serve(monkeypatch, [(base + "/widgets", 200, fx(name))])
    j = _job(job_id=jid, url=url)
    assert asyncio.run(scraper._phenom_detail(None, base, j)) is True
    assert len(j.description) >= min_len
    assert seen[0][0] == "post" and seen[0][2]["jobId"] == jid and seen[0][2]["ddoKey"] == "jobDetail"
    assert len(seen) == 1                      # no JSON-LD read of a foreign apply page


def test_workday_row_reads_cxs_first(monkeypatch):
    seen = _serve(monkeypatch, [("/wday/cxs/davita/DKC_External/job/", 200, fx("cxs_davita_R0475128.json")),
                                ("careers.davita.com/widgets", 500, "")])
    j = _job(hospital_system="DaVita", job_id="R0475128", url=scraper._workday_job_url(DAVITA_APPLY),
             job_type="", posted_date="")
    assert asyncio.run(scraper._phenom_detail(None, "https://careers.davita.com", j)) is True
    assert len(j.description) > 2500
    assert j.job_type == "Full time" and j.posted_date
    assert [s[1] for s in seen] == [scraper._workday_cxs_url(DAVITA_APPLY)]


def test_workday_row_falls_back_to_widget_on_403(monkeypatch):
    seen = _serve(monkeypatch, [("/wday/cxs/", 403, {"errorCode": "S22"}),
                                ("careers.davita.com/widgets", 200, fx("widget_jackson_223645.json"))])
    j = _job(hospital_system="DaVita", job_id="R0475128", url=DAVITA_APPLY)
    assert asyncio.run(scraper._phenom_detail(None, "https://careers.davita.com", j)) is True
    assert len(j.description) > 5000
    assert [s[0] for s in seen] == ["get", "post"]


def test_failed_fetch_leaves_row_as_listed(monkeypatch):
    _serve(monkeypatch, [("/wday/cxs/", 0, RuntimeError("reset")), ("/widgets", 0, asyncio.TimeoutError())])
    j = _job(hospital_system="DaVita", job_id="R1", url=DAVITA_APPLY, job_type="Full-time", posted_date="2026-09-01")
    before = (j.description, j.job_type, j.posted_date, j.url)
    assert asyncio.run(scraper._phenom_detail(None, "https://careers.davita.com", j)) is False
    assert (j.description, j.job_type, j.posted_date, j.url) == before


def test_detail_pass_never_drops_rows(monkeypatch):
    """A pass where every fetch fails keeps every listed row, unchanged."""
    _serve(monkeypatch, [("/widgets", 0, RuntimeError("down"))])
    jobs = [_job(job_id=str(i), url=f"https://x.example/apply/{i}") for i in range(5)]
    snap = [(j.job_id, j.description) for j in jobs]
    asyncio.run(scraper._detail_pass(None, "Jackson Health System", jobs, scraper.PHENOM_DESC_BUDGET,
                                     lambda j: scraper._phenom_detail(None, "https://jobs.jacksonhealth.org", j),
                                     "Phenom"))
    assert [(j.job_id, j.description) for j in jobs] == snap


def test_bsw_uses_job_id_from_url(monkeypatch):
    seen = _serve(monkeypatch, [("jobs.bswhealth.com/widgets", 200, fx("widget_jackson_223645.json"))])
    j = _job(hospital_system="Baylor Scott & White", job_id="BSWHUS26007805EXTERNALENUS",
             url="https://jobs.bswhealth.com/us/en/job/26007805")
    ok = asyncio.run(scraper._phenom_detail(None, "https://jobs.bswhealth.com", j,
                                            widget_id=scraper._phenom_url_job_id(j.url)))
    assert ok and seen[0][2]["jobId"] == "26007805"


def test_known_body_skipped_at_no_cost(monkeypatch):
    seen = _serve(monkeypatch, [("/widgets", 200, fx("widget_jackson_223645.json"))])
    scraper.set_known_bodies([{"hospital_system": "Jackson Health System", "job_id": "1", "desc_len": 6000}])
    jobs = [_job(job_id="1"), _job(job_id="2")]
    asyncio.run(scraper._detail_pass(None, "Jackson Health System", jobs, scraper.PHENOM_DESC_BUDGET,
                                     lambda j: scraper._phenom_detail(None, "https://jobs.jacksonhealth.org", j),
                                     "Phenom"))
    assert [s[2]["jobId"] for s in seen] == ["2"]
    assert scraper.PHENOM_DESC_BUDGET.spent == 1
    scraper.set_known_bodies([])


# ── same path as every body: finalize_jobs -> posting_facts["requirements"] ─
@pytest.mark.parametrize("name,fields", [
    ("widget_jackson_223645.json", ("qualifications", "licensure")),
    ("widget_hendrick_35517.json", ("qualifications",)),
    ("widget_childrens_32260.json", ("qualifications",)),
    ("cxs_davita_R0475128.json", ("qualifications", "certifications", "licensure", "education")),
])
def test_fetched_body_reaches_requirements(monkeypatch, name, fields):
    data = fx(name)
    j = _job(job_id="9")
    if "jobPostingInfo" in data:
        assert scraper._apply_wd_posting_info(j, data["jobPostingInfo"])
    else:
        j.description = scraper._phenom_posting_text(data["jobDetail"]["data"]["job"])[0]
    rows = scraper.finalize_jobs([j])
    rq = rows[0]["posting_facts"]["requirements"]
    teaser = scraper.extract_requirements("Teaser. " * 30)
    for f in fields:
        got = rq[f]["required"] + rq[f]["preferred"] if f == "qualifications" else rq[f]
        was = teaser[f]["required"] + teaser[f]["preferred"] if f == "qualifications" else teaser[f]
        assert got and not was, f
