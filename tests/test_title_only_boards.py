"""Detail passes for the title-only boards nobody had covered (2026-09-24,
push 3): HealthcareSource (body already in the search hit), CHS WPJobBoard,
Oceans and ApplicantPro (job-page JSON-LD), Concentra (microdata), HCTS (job
page), Kronos / UKG Ready and CSOD (per-requisition JSON). Fixtures under
tests/fixtures/nobody/ are live pages and payloads fetched 2026-09-24,
trimmed. No network: `_fetch_html`, `_curl_html` and `req` are replaced."""
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


def _counts(text):
    r = scraper.extract_requirements(text)
    q = r["qualifications"]
    return len(q["required"]), len(q["preferred"]), len(r["certifications"]), len(r["licensure"]), len(r["education"])


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {}
        self.cookies = {}

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
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append((method, url, kw.get("params"), kw.get("headers") or {}))
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(_R(status, body))
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
    for name in ("CHS", "CONCENTRA", "OCEANS", "APPLICANTPRO", "CSOD", "HCTS", "KRONOS"):
        monkeypatch.setattr(scraper, f"{name}_DESC_BUDGET", scraper._DescBudget(1000))
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


# ── HealthcareSource: the body is in the search hit ─────────────────────────
def test_hcs_hit_body_comes_from_job_summary_display(fixture_text):
    d = json.loads(fixture_text("nobody/hcs_pvh_90962.json"))
    j = scraper._hcs_job(d["hits"]["hits"][0], "Parkview Health", "pvh")
    assert j and j.job_id == "90962"
    assert len(j.description) >= 1500
    assert "Master Degree in Social Work required" in j.description
    assert "#" not in j.description.split("Schedule:")[0]          # not the "#"-mangled jobSummary
    assert j.description.rstrip().endswith("Schedule: Days shift · 8:00am-8:00pm")
    assert _counts(j.description)[3] >= 1                         # "Must have current Indiana LSW or LCSW license."
    row = scraper.finalize_jobs([j])[0]
    assert row["posting_facts"]["requirements"]["licensure"]


def test_hcs_hit_without_summary_stays_blank():
    hit = {"_id": "1", "_source": {"title": "RN", "userArea": {"jobPostingID": "5", "shift": "Days"},
                                    "jobLocation": {"address": {"addressLocality": "Reno", "addressRegion": "NV"}}}}
    j = scraper._hcs_job(hit, "Renown Health", "renownhealth")
    assert j.description == ""                                     # no lone "Schedule:" line


def test_hcs_recorded_fixture_now_has_bodies(fixture_text):
    d = json.loads(fixture_text("healthcaresource_search.json"))
    jobs = [scraper._hcs_job(h, "Shannon Medical Center", "shannonhealth") for h in d["hits"]["hits"]]
    assert all(len(j.description) >= 200 for j in jobs)


# ── JSON-LD boards: CHS, Oceans, ApplicantPro ───────────────────────────────
@pytest.mark.parametrize("name,min_len", [("chs_painter_143276.html", 1500),
                                          ("oceans_20443.html", 1500),
                                          ("applicantpro_4147596.html", 1000)])
def test_jsonld_board_pages_give_a_body(fixture_text, monkeypatch, name, min_len):
    html = fixture_text(f"nobody/{name}")

    async def fake_fetch(session, url, timeout=20):
        return html
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    j = _job(url="https://example/job/1")
    assert asyncio.run(scraper._jsonld_board_detail(None, j)) is True
    assert len(j.description) >= min_len
    assert j.posted_date and len(j.posted_date) == 10


def test_oceans_experience_requirements_are_folded_in(fixture_text):
    posting = scraper._jobposting_from_html(fixture_text("nobody/oceans_20443.html"))
    plain = _job()
    scraper._apply_posting(plain, posting)
    assert "unrestricted state nursing license" not in plain.description   # only in experienceRequirements
    j = _job()
    assert scraper._apply_posting(j, scraper._posting_with_requirements(posting))
    assert "Qualifications\nExperience\nMSN preferred" in j.description
    req, pref, cert, lic, edu = _counts(j.description)
    assert req + pref >= 1 and lic >= 1


def test_posting_with_requirements_does_not_repeat_text():
    p = {"description": "<p>Duties.</p><p>BLS required.</p>", "experienceRequirements": "BLS required.",
         "educationRequirements": {"@type": "EducationalOccupationalCredential", "credentialCategory": "bachelor degree"}}
    out = scraper._posting_with_requirements(p)
    assert out["qualifications"] == "Education\nbachelor degree"
    assert scraper._posting_with_requirements({"description": "x"}) == {"description": "x"}


# ── Concentra: microdata page, every section ────────────────────────────────
def test_concentra_page_gives_overview_and_qualifications(fixture_text):
    body, et, posted = scraper._concentra_posting(fixture_text("nobody/concentra_350203.html"))
    assert body.startswith("Bonus Potential!")
    assert "Qualifications" in body and "Bachelor’s Degree from an accredited Physical Therapy program" in body
    assert "Read more" not in body
    assert et == "Full Time" and posted == "2026-07-16"
    req, pref, cert, lic, edu = _counts(body)
    assert req >= 5 and pref >= 1 and edu >= 1


def test_concentra_detail_prefers_curl_and_falls_back(fixture_text, monkeypatch):
    html = fixture_text("nobody/concentra_350203.html")
    calls = []

    async def fake_curl(url, impersonate="chrome", timeout=25):
        calls.append("curl")
        return ""                                   # challenge / no curl_cffi

    async def fake_fetch(session, url, timeout=20):
        calls.append("aiohttp")
        return html
    monkeypatch.setattr(scraper, "_curl_html", fake_curl)
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    j = _job(url="https://www.concentra.com/careers/job-search/tx/temple/physical-therapist/350203/")
    assert asyncio.run(scraper._concentra_detail(None, j)) is True
    assert calls == ["curl", "aiohttp"] and j.job_type == "Full Time" and j.posted_date == "2026-07-16"


# ── HCTS: job page body + sidebar ───────────────────────────────────────────
def test_hcts_page_body_and_fields(fixture_text, monkeypatch):
    html = fixture_text("nobody/hcts_2229509.html")
    body, fields = scraper._hcts_posting(html)
    assert body.startswith("Job Summary") and "Bachelor degree in Nursing is required" in body
    assert fields["Schedule - Shift - Hours"] == "PRN - Nights - 7pm-7am"
    assert fields["Job Category"] == "Nursing"

    async def fake_fetch(session, url, timeout=20):
        return html
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    j = _job(url="https://umcelpasocareers.hctsportals.com/jobs/2229509-rn-prn", job_id="2229509")
    assert asyncio.run(scraper._hcts_detail(None, j)) is True
    assert j.description.rstrip().endswith("Schedule: PRN - Nights - 7pm-7am") and j.specialty == "Nursing"
    req, pref, cert, lic, edu = _counts(j.description)
    assert edu >= 1 and cert + lic >= 1


# ── Kronos / UKG Ready ──────────────────────────────────────────────────────
def test_kronos_detail_url_and_text(fixture_text, monkeypatch):
    j = _job(url="https://prd01-hcm01.prd.mykronos.com/ta/6059921.careers?CareersSearch=&ShowJob=2151532804&lang=en-US",
             job_id="2151532804", description="x" * 256)
    url = scraper._kronos_detail_url(j)
    assert url == "https://prd01-hcm01.prd.mykronos.com/ta/rest/ui/recruitment/companies/%7C6059921/job-requisitions/2151532804"
    assert scraper._kronos_detail_url(_job(url="https://x/careers")) == ""
    d = json.loads(fixture_text("nobody/kronos_detail.json"))
    seen = _serve(monkeypatch, [("job-requisitions/2151532804", 200, d)])
    assert asyncio.run(scraper._kronos_detail(None, j)) is True
    assert seen[0][2] == {"lang": "en-US"}
    assert len(j.description) > 1500
    assert scraper.strip_html(d["job_requirement"])[:60] in j.description
    assert sum(_counts(j.description)) >= 3


# ── CSOD ────────────────────────────────────────────────────────────────────
def test_csod_detail_uses_the_home_token_and_cookies(fixture_text, monkeypatch):
    d = json.loads(fixture_text("nobody/csod_jobdetails_30638.json"))
    seen = _serve(monkeypatch, [("/requisitions/30638/jobDetails", 200, d)])
    j = _job(hospital_system="JPS Health Network", job_id="30638")
    monkeypatch.setitem(scraper._CSOD_CTX, "JPS Health Network", ("https://jpshealthnet.csod.com", "TOKEN", "cscx=abc"))
    assert asyncio.run(scraper._csod_detail(None, j)) is True
    hdrs = seen[0][3]
    assert hdrs["Authorization"] == "Bearer TOKEN" and hdrs["Cookie"] == "cscx=abc"
    assert j.description.startswith("Job Summary: The Rad Technologist I CT PRN") and j.posted_date == "2026-09-18"
    other = _job(hospital_system="Unknown CSOD", job_id="1")
    assert asyncio.run(scraper._csod_detail(None, other)) is False and other.description == ""


# ── The pass itself: budget, known bodies, failures never drop a row ────────
def _rows(n, system="Community Health Systems"):
    return [_job(hospital_system=system, job_id=str(i), url=f"https://www.careershealthcare.com/job/{i}")
            for i in range(n)]


def test_failing_fetches_keep_every_listed_row():
    jobs = _rows(5)

    async def boom(job):
        raise RuntimeError("network down")

    asyncio.run(scraper._board_detail_passes(None, jobs, scraper.CHS_DESC_BUDGET, boom, "CHS"))
    assert len(jobs) == 5 and all(j.description == "" and j.title == "RN" for j in jobs)


def test_pass_driver_error_is_contained(monkeypatch):
    async def broken(*a, **k):
        raise RuntimeError("driver bug")
    monkeypatch.setattr(scraper, "_detail_passes_by_system", broken)
    jobs = _rows(3)
    asyncio.run(scraper._board_detail_passes(None, jobs, scraper.CHS_DESC_BUDGET, None, "CHS"))
    assert len(jobs) == 3


def test_budget_caps_fetches_and_known_bodies_are_skipped(monkeypatch):
    monkeypatch.setattr(scraper, "CHS_DESC_BUDGET", scraper._DescBudget(3))
    scraper.set_known_bodies([{"hospital_system": "CHS", "job_id": "0", "desc_len": 4000}])
    jobs = _rows(6)
    fetched = []

    async def fetch(job):
        fetched.append(job.job_id)
        job.description = "Body " * 100
        return True

    asyncio.run(scraper._board_detail_passes(None, jobs, scraper.CHS_DESC_BUDGET, fetch, "CHS"))
    assert len(fetched) == 3 and "0" not in fetched                 # CHS canonical key, known body skipped
    assert scraper.CHS_DESC_BUDGET.remaining == 0


def test_new_platforms_are_known_body_platforms():
    for p in ("WPJobBoard", "Concentra", "OceansJobBoard", "ApplicantPro", "CSOD", "HCTS", "Kronos"):
        assert p in scraper.KNOWN_BODY_PLATFORMS


def test_runners_wire_their_passes(monkeypatch):
    """run_hcts / run_kronos / run_concentra hand their rows to the pass with
    the board's budget; the rows come back whatever the pass does."""
    calls = []

    async def fake_passes(session, jobs, budget, fetch_one, label, in_flight=2):
        calls.append((label, budget, len(jobs)))
        raise RuntimeError("pass failed")
    monkeypatch.setattr(scraper, "_detail_passes_by_system", fake_passes)

    async def fake_hcts(session, system, cfg):
        return _rows(2, system)

    async def fake_kronos(session, system, cfg):
        return _rows(3, system)

    async def fake_conc(session):
        return _rows(4, "Concentra")
    monkeypatch.setattr(scraper, "scrape_hcts", fake_hcts)
    monkeypatch.setattr(scraper, "scrape_kronos", fake_kronos)
    monkeypatch.setattr(scraper, "scrape_concentra", fake_conc)
    monkeypatch.setattr(scraper, "HCTS_PORTALS", {"UMC El Paso": ("umcelpasocareers", "TX")})
    monkeypatch.setattr(scraper, "KRONOS_ORGS", {"Magruder Hospital": ("secure7.saashr.com", "1", "OH")})
    assert len(asyncio.run(scraper.run_hcts(None))) == 2
    assert len(asyncio.run(scraper.run_kronos(None))) == 3
    assert len(asyncio.run(scraper.run_concentra(None))) == 4
    assert [(c[0], c[2]) for c in calls] == [("HCTS", 2), ("Kronos", 3), ("Concentra", 4)]
    assert calls[0][1] is scraper.HCTS_DESC_BUDGET and calls[2][1] is scraper.CONCENTRA_DESC_BUDGET


def test_concentra_list_403_retries_through_curl(monkeypatch):
    """A 403 challenge on the SXA search API is retried once through
    curl_cffi; the rows parse as before. Anything else still ends the list."""
    page = {"Count": 1, "Results": [{"Url": "/careers/job-search/tx/temple/physical-therapist/350203/", "Id": "x",
                                     "Html": '<a href="#" title="Physical Therapist">x</a>'
                                             '<div class="field-location">Temple, TX</div>'}]}
    _serve(monkeypatch, [("/sxa/search/results/", 403, "Just a moment...")])
    asked = []

    async def fake_curl_page(params):
        asked.append(params["e"])
        return page
    monkeypatch.setattr(scraper, "_concentra_curl_page", fake_curl_page)
    jobs = asyncio.run(scraper.scrape_concentra(None))
    assert asked == ["0"] and len(jobs) == 1
    assert jobs[0].job_id == "350203" and jobs[0].state == "TX" and jobs[0].title == "Physical Therapist"

    async def no_curl(params):
        return None
    monkeypatch.setattr(scraper, "_concentra_curl_page", no_curl)
    assert asyncio.run(scraper.scrape_concentra(None)) == []
    _serve(monkeypatch, [("/sxa/search/results/", 500, "")])
    monkeypatch.setattr(scraper, "_concentra_curl_page", fake_curl_page)
    asked.clear()
    assert asyncio.run(scraper.scrape_concentra(None)) == [] and asked == []
