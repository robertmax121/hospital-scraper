"""Coverage stage 4 (2026-10-25): Taleo Enterprise career sections (REST and
FTL kinds), the SelectMinds front for McLaren (company line, custom base URL,
job-page bodies), ApplicantStack, JazzHR and JobAps. Every fixture is a
trimmed live response of 10-25 (tests/fixtures/stage4_1025); no network: the
page getters are replaced."""
import asyncio
import json
import os

import scraper

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "stage4_1025")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


# ── Taleo: locations ────────────────────────────────────────────────────────

def test_taleo_location_rest_and_ftl_shapes():
    loc = scraper.taleo_location
    assert loc("United States-Florida-Tampa", "FL") == ("", "Tampa", "FL")
    assert loc('["US-NY-Stony Brook"]', "NY") == ("", "Stony Brook", "NY")
    assert loc("Florida-Brooksville-TGH Brooksville", "FL") == ("TGH Brooksville", "Brooksville", "FL")
    assert loc("US-NC-Winston-Salem", "NC")[1:] == ("Winston Salem", "NC")      # a hyphenated town kept whole
    assert loc("Erlanger Baroness Hospital Chattanooga, TN", "TN") == ("Erlanger Baroness Hospital", "Chattanooga", "TN")
    assert loc("Erlanger Western Carolina Hospital Murphy, NC", "TN") == ("Erlanger Western Carolina Hospital", "Murphy", "NC")
    assert loc("Erlanger East Chattanooga, TN", "TN") == ("Erlanger East", "Chattanooga", "TN")
    assert loc("Bledsoe Pikeville, TN", "TN") == ("Bledsoe", "Pikeville", "TN")
    assert loc("Chattanooga, TN", "TN") == ("", "Chattanooga", "TN")
    assert loc("", "TN") == ("", "", "TN")
    assert loc("Some Hospital Center, TX", "TN")[1] == ""                       # "Center" is never the town


def test_taleo_hospital_rules():
    h = scraper._taleo_hospital
    assert h("Erlanger Health System", "Children's Hospital", "Chattanooga") == "Children's Hospital at Erlanger"
    assert h("Erlanger Health System", "Erlanger Baroness Hospital", "Chattanooga") == "Erlanger Baroness Hospital"
    assert h("Erlanger Health System", "Bledsoe", "Pikeville") == "Erlanger Bledsoe Hospital"
    assert h("Erlanger Health System", "Physician Practices", "Chattanooga") == "Erlanger Health System"
    assert h("Tampa General Hospital", "", "Tampa") == "Tampa General Hospital"
    assert h("Tampa General Hospital", "", "Brooksville") == "Tampa General Hospital Brooksville"
    assert h("Tampa General Hospital", "", "Boca Grande") == "Tampa General Hospital"     # the system name
    assert h("Stony Brook Medicine", "", "Stony Brook") == "Stony Brook University Hospital"
    assert h("Stony Brook Medicine", "", "Southampton") == "Stony Brook Southampton Hospital"
    assert h("Stony Brook Medicine", "", "Commack") == "Stony Brook Medicine"


# ── Taleo: REST lists ───────────────────────────────────────────────────────

def test_taleo_rest_rows():
    data = json.loads(_read("taleo_rest_tgh.json"))
    jobs = scraper._taleo_rest_jobs(data, "Tampa General Hospital", "tgh.taleo.net", "ex", "FL")
    assert [j.job_id for j in jobs] == ["2600045W", "260003RN", "2600043U"]
    j = jobs[0]
    assert j.title == "Clinical Nurse - Labor & Delivery"
    assert (j.hospital_name, j.city, j.state, j.location) == ("Tampa General Hospital", "Tampa", "FL", "Tampa, FL")
    assert j.url == "https://tgh.taleo.net/careersection/ex/jobdetail.ftl?job=2600045W&lang=en"
    assert j.posted_date == "2026-10-25" and j.ats_platform == "Taleo" and j.description == ""
    sbu = scraper._taleo_rest_jobs(json.loads(_read("taleo_rest_sbu.json")), "Stony Brook Medicine",
                                   "stonybrooku.taleo.net", "2", "NY")
    assert sbu[0].hospital_name == "Stony Brook University Hospital" and sbu[0].city == "Stony Brook"


def _taleo_fake(monkeypatch, answers):
    """answers: callables or values by (method, url-suffix); records calls."""
    calls = []

    async def fake_get(session, url, system, method="get", **kw):
        calls.append((method, url, kw.get("json") or kw.get("data")))
        for (m, suffix), ans in answers.items():
            if m == method and url.split("?")[0].endswith(suffix):
                out = ans(kw) if callable(ans) else ans
                return (200, out) if isinstance(out, str) else out
        return 404, ""

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "_taleo_get", fake_get)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    return calls


def test_scrape_taleo_rest_pages_and_claims_complete(monkeypatch):
    data = json.loads(_read("taleo_rest_tgh.json"))
    pages = {1: data["requisitionList"][:2], 2: data["requisitionList"][2:]}

    def page(kw):
        n = kw["json"]["pageNo"]
        return json.dumps({"requisitionList": pages.get(n, []),
                           "pagingData": {"currentPageNo": n, "pageSize": 2, "totalCount": 3}})
    calls = _taleo_fake(monkeypatch, {("get", "/jobsearch.ftl"): _read("taleo_tgh_search.html"),
                                      ("post", "/rest/jobboard/searchjobs"): page})
    jobs = asyncio.run(scraper.scrape_taleo(None, "Tampa General Hospital", ("tgh.taleo.net", "ex", "FL")))
    assert [j.job_id for j in jobs] == ["2600045W", "260003RN", "2600043U"]
    assert calls[1][1] == "https://tgh.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=101430233"
    assert [c[2]["pageNo"] for c in calls[1:]] == [1, 2]
    assert "Tampa General Hospital" in scraper.COMPLETE_SYSTEMS
    assert ("TALEO_ORGS", "Tampa General Hospital") in scraper.COMPLETE_FEEDS


def test_scrape_taleo_rest_failed_page_is_partial(monkeypatch):
    data = json.loads(_read("taleo_rest_tgh.json"))

    def page(kw):
        if kw["json"]["pageNo"] == 2:
            return 503, ""
        return json.dumps({"requisitionList": data["requisitionList"][:2],
                           "pagingData": {"pageSize": 2, "totalCount": 3}})
    _taleo_fake(monkeypatch, {("get", "/jobsearch.ftl"): _read("taleo_tgh_search.html"),
                              ("post", "/rest/jobboard/searchjobs"): page})
    jobs = asyncio.run(scraper.scrape_taleo(None, "Tampa General Hospital", ("tgh.taleo.net", "ex", "FL")))
    assert len(jobs) == 2
    assert "Tampa General Hospital" in scraper.PARTIAL_SYSTEMS
    assert "Tampa General Hospital" not in scraper.COMPLETE_SYSTEMS


def test_scrape_taleo_unavailable_section(monkeypatch):
    page = "<html><head><title>Career Section Unavailable</title></head><body></body></html>"
    calls = _taleo_fake(monkeypatch, {("get", "/jobsearch.ftl"): page})
    assert asyncio.run(scraper.scrape_taleo(None, "McLaren", ("mclarenhlth.taleo.net", "2", "MI"))) == []
    assert len(calls) == 1


# ── Taleo: FTL lists (Erlanger) ─────────────────────────────────────────────

def test_taleo_ftl_first_page_and_pager_answer():
    html = _read("taleo_ftl_erlanger_search.html")
    toks = scraper._taleo_history(html)
    assert scraper._taleo_ftl_total(html, toks) == 442
    p1 = scraper._taleo_ftl_jobs(toks, "Erlanger Health System", "erlanger.taleo.net", "ehs_external", "TN")
    assert len(p1) == 10
    j = p1[0]
    assert (j.job_id, j.hospital_name, j.city, j.state) == ("48194", "Children's Hospital at Erlanger", "Chattanooga", "TN")
    assert j.title == "Surgical Technologist - Children's Hospital - Surgery - Full-time"      # %27 decoded
    assert j.url == "https://erlanger.taleo.net/careersection/ehs_external/jobdetail.ftl?job=48194&lang=en"
    p2 = scraper._taleo_ftl_jobs(scraper._taleo_tokens(_read("taleo_ftl_erlanger_p2.txt")),
                                 "Erlanger Health System", "erlanger.taleo.net", "ehs_external", "TN")
    assert len(p2) == 10 and not {j.job_id for j in p1} & {j.job_id for j in p2}
    murphy = next(j for j in p2 if j.job_id == "48627")
    assert (murphy.hospital_name, murphy.city, murphy.state) == ("Erlanger Western Carolina Hospital", "Murphy", "NC")
    east = next(j for j in p2 if j.job_id == "48622")
    assert (east.hospital_name, east.city) == ("Erlanger East Hospital", "Chattanooga")


def test_scrape_taleo_ftl_pages_through_the_ajax_pager(monkeypatch):
    html = _read("taleo_ftl_erlanger_search.html").replace('value="442"', 'value="20"')
    calls = _taleo_fake(monkeypatch, {("get", "/jobsearch.ftl"): html,
                                      ("post", "/jobsearch.ajax"): _read("taleo_ftl_erlanger_p2.txt")})
    jobs = asyncio.run(scraper.scrape_taleo(None, "Erlanger Health System",
                                            ("erlanger.taleo.net", "ehs_external", "TN")))
    assert len(jobs) == 20
    form = calls[1][2]
    assert form["rlPager.currentPage"] == "2" and form["csrftoken"]
    assert form["ftlinterfaceid"] == "requisitionListInterface" and form["ftlcompid"] == "rlPager"
    assert "Erlanger Health System" in scraper.COMPLETE_SYSTEMS


# ── Taleo: job pages ────────────────────────────────────────────────────────

def test_taleo_posting_body_pay_and_schedule():
    p = scraper.taleo_posting(_read("taleo_detail_sbu.html"))
    assert p["contest"] == "2603976" and p["title"].startswith("Patient Access Team Lead")
    assert p["pay"] == "$55,945 - $68,197 / year" and p["type"] == "Full-time" and p["shift"] == "Day Shift"
    assert p["org"] == "Stony Brook University Hospital"
    assert p["body"].endswith("Pay: $55,945 - $68,197 / year\nSchedule: Full-time · Day Shift")
    assert "<" not in p["body"][:500] and len(p["body"]) > 3000
    assert scraper.extract_posted_wage(p["body"], "Full-time")[:2] == (55945.0, 68197.0)
    e = scraper.taleo_posting(_read("taleo_detail_erlanger.html"))
    assert e["contest"] == "47095" and e["type"] == "Full-time" and "$10k Sign on bonus" in e["body"]
    assert e["body"].count("Job Summary:") == 1               # the description is sent twice, kept once
    t = scraper.taleo_posting(_read("taleo_detail_tgh.html"))
    assert t["shift"] == "Night Job" and "Graduate of an accredited School of Nursing" in t["body"]
    assert scraper.taleo_posting("<html></html>")["body"] == ""


def test_taleo_detail_fills_the_row_and_skips_another_postings_page(monkeypatch):
    page = _read("taleo_detail_sbu.html")

    async def fake_fetch(session, url, timeout=20):
        return page
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    job = scraper._taleo_job("Stony Brook Medicine", "stonybrooku.taleo.net", "2", "NY", "2603976",
                             "Patient Access Team Lead", "US-NY-Stony Brook")
    assert asyncio.run(scraper._taleo_detail(None, job)) is True
    assert job.job_type == "Full-time" and "Pay: $55,945" in job.description
    other = scraper._taleo_job("Stony Brook Medicine", "stonybrooku.taleo.net", "2", "NY", "9999999",
                               "Other", "US-NY-Stony Brook")
    assert asyncio.run(scraper._taleo_detail(None, other)) is False and other.description == ""


# ── SelectMinds: McLaren ────────────────────────────────────────────────────

def test_selectminds_mclaren_cards_name_the_hospital():
    jobs = scraper._parse_selectminds_page(_read("selectminds_mclaren_search.html"), "McLaren Health Care",
                                           "https://careers.mclaren.org")
    assert [(j.job_id, j.hospital_name, j.city, j.state) for j in jobs] == [
        ("39176", "McLaren Flint", "Flint", "MI"),
        ("39071", "McLaren Lapeer Region", "Lapeer", "MI"),
        ("38584", "McLaren Port Huron", "Port Huron", "MI")]
    assert jobs[0].url == "https://careers.mclaren.org/jobs/grounds-mechanic-39176"
    # another tenant's cards keep the system name (the company line is McLaren's rule only)
    other = scraper._parse_selectminds_page(_read("selectminds_mclaren_search.html"), "Main Line Health",
                                            "https://careers.mclaren.org")
    assert {j.hospital_name for j in other} == {"Main Line Health"}


def test_selectminds_company_names_spelled_out_and_extra_locations_dropped():
    card = _read("selectminds_mclaren_search.html")
    card = card.replace('<p class="jlr_company">McLaren Flint</p>', '<p class="jlr_company">Barbara Ann Karmanos Cancer Cn</p>')
    card = card.replace("Michigan, Flint", "Michigan, Detroit and 1 additional location", 1)
    j = scraper._parse_selectminds_page(card, "McLaren Health Care", "https://careers.mclaren.org")[0]
    assert (j.hospital_name, j.city, j.state) == ("Barbara Ann Karmanos Cancer Institute", "Detroit", "MI")


def test_selectminds_custom_base_url(monkeypatch):
    asked = []

    async def fake_get(session, system, url, params=None):
        asked.append(url)
        if url.endswith("/jobs/search/"):
            return 200, _read("selectminds_mclaren_search.html").replace('class="ghost">129.0', 'class="ghost">1.0'), url + "5932375"
        return 404, "", url
    monkeypatch.setattr(scraper, "_selectminds_get", fake_get)
    jobs = asyncio.run(scraper.scrape_selectminds(None, "McLaren Health Care", "https://careers.mclaren.org"))
    assert asked[0] == "https://careers.mclaren.org/jobs/search/" and len(jobs) == 3
    assert scraper.SELECTMINDS_ORGS["McLaren Health Care"] == "https://careers.mclaren.org"
    assert scraper.SELECTMINDS_MAX_PAGES >= 130          # McLaren lists 129 pages of 10


def test_selectminds_job_page_body_requisition_and_company(monkeypatch):
    page = _read("selectminds_mclaren_job.html")
    body, company = scraper.selectminds_posting(page)
    assert company == "McLaren Flint"
    assert body.startswith("Department:") and "Qualifications:" in body and "Rate of Pay: $19.95" in body

    async def fake_get(session, system, url, params=None):
        return 200, page, url
    monkeypatch.setattr(scraper, "_selectminds_get", fake_get)
    job = scraper.Job(title="Grounds Mechanic", hospital_system="McLaren Health Care", hospital_name="McLaren Health Care",
                      city="Flint", state="MI", location="Flint, MI", specialty="", job_type="",
                      url="https://careers.mclaren.org/jobs/grounds-mechanic-39176", job_id="39176",
                      posted_date="", description="Department: Plant Operations...", ats_platform="SelectMinds")
    assert asyncio.run(scraper._selectminds_detail(None, job)) is True
    assert job.req_id == "26011476" and job.hospital_name == "McLaren Flint" and len(job.description) > 500
    job.job_id, job.description = "11111", ""
    assert asyncio.run(scraper._selectminds_detail(None, job)) is False      # another posting's page


def test_selectminds_body_pass_tenants_skip_the_separate_requisition_pass():
    assert {"McLaren Health Care", "Main Line Health", "University of Iowa Hospitals and Clinics"} <= scraper.SELECTMINDS_BODY_PASS
    assert "SelectMinds" in scraper.KNOWN_BODY_PLATFORMS


# ── ApplicantStack ──────────────────────────────────────────────────────────

def test_applicantstack_rows_towns_sites_and_clinics():
    d = scraper._parse_applicantstack(_read("applicantstack_delta.html"), "Delta Health System", "MS")
    assert [(j.job_id, j.hospital_name, j.city) for j in d] == [
        ("a2evjac05tas", "Delta Health System - The Medical Center", "Greenville"),
        ("a2evjac0dyxa", "Delta Health System - The Medical Center", "Greenville"),
        ("a2evjacaf253", "Delta Health System", "Clarksdale")]           # the oncology clinic, not a hospital
    assert d[0].url == "https://deltaregional.applicantstack.com/x/detail/a2evjac05tas"
    assert d[0].job_type == "Full Time (Nights)" and d[0].specialty == "2 South" and d[0].ats_platform == "ApplicantStack"
    n = scraper._parse_applicantstack(_read("applicantstack_nrmc.html"), "Nevada Regional Medical Center", "MO")
    assert {(j.hospital_name, j.city, j.state) for j in n} == {("Nevada Regional Medical Center", "Nevada", "MO")}
    w = scraper._parse_applicantstack(_read("applicantstack_wcch.html"), "West Calcasieu Cameron Hospital", "LA")
    assert [(j.hospital_name, j.city, j.state) for j in w] == [
        ("West Calcasieu Cameron Hospital", "Sulphur", "LA"), ("Dynamic Dimensions West", "Sulphur", "LA"),
        ("Dynamic Dimensions East", "Moss Bluff", "LA"), ("Community Clinic", "Sulphur", "LA")]


def test_applicantstack_detail_reads_the_jsonld(monkeypatch):
    async def fake_fetch(session, url, timeout=20):
        return _read("applicantstack_detail.html")
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    job = scraper._parse_applicantstack(_read("applicantstack_delta.html"), "Delta Health System", "MS")[1]
    assert asyncio.run(scraper._applicantstack_detail(None, job)) in (True, False)
    assert "Registered Nurse" in job.description and job.posted_date == "2026-07-27"


# ── JazzHR ──────────────────────────────────────────────────────────────────

def test_jazzhr_rows_and_hospital_towns():
    p = scraper._parse_jazzhr(_read("jazzhr_perimeter.html"), "Perimeter Healthcare", "TX")
    got = {(j.city, j.state): j.hospital_name for j in p}
    assert got[("West Memphis", "AR")] == "Perimeter Behavioral Hospital of West Memphis"
    assert got[("Garland", "TX")] == "Perimeter Behavioral Hospital of Dallas"
    assert got[("Jackson", "TN")] == "Perimeter Behavioral Center of Jackson"
    assert got[("Forrest City", "AR")] == "Perimeter Healthcare"          # no Perimeter hospital there
    j = p[0]
    assert j.url.startswith("https://perimeterhealthcare.applytojob.com/apply/") and j.job_id in j.url
    e = scraper._parse_jazzhr(_read("jazzhr_exceptional.html"), "Exceptional Healthcare", "TX")
    yuma = [j for j in e if j.state == "AZ" and j.city == "Yuma"]
    assert len(yuma) == 2 and {j.hospital_name for j in yuma} == {"Exceptional Community Hospital Yuma"}   # "YUMA, AZ" too
    assert {j.hospital_name for j in e if j.city == "Rapid City"} == {"Exceptional Healthcare"}


def test_jazzhr_detail_reads_the_jsonld(monkeypatch):
    async def fake_fetch(session, url, timeout=20):
        return _read("jazzhr_job.html")
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    job = scraper._parse_jazzhr(_read("jazzhr_exceptional.html"), "Exceptional Healthcare", "TX")[0]
    assert asyncio.run(scraper._jazzhr_detail(None, job)) is True
    assert "Radiologic Technology" in job.description and job.job_type == "PART_TIME"
    assert job.posted_date == "2026-09-22"


# ── JobAps ──────────────────────────────────────────────────────────────────

def test_jobaps_md_rows_public_sections_hospitals_and_salary():
    cfg = scraper.JOBAPS_SITES["Maryland Department of Health"]
    jobs = scraper._parse_jobaps(_read("jobaps_md.html"), "Maryland Department of Health", cfg)
    named = [j for j in jobs if j.hospital_name]
    assert {(j.hospital_name, j.city) for j in named} == {("Springfield Hospital Center", "Sykesville"),
                                                          ("Spring Grove Hospital Center", "Catonsville")}
    j = next(j for j in named if j.job_id == "26-004563-0001")
    assert j.title == "LINEN SERVICE WORKER *REPOST*" and j.state == "MD"
    assert j.url == "https://www.jobapscloud.com/MD/sup/bulpreview.asp?b=&R1=26&R2=004563&R3=0001"
    assert j.description.startswith("Salary: $38,582.00 - $60,852.00/ year")
    assert not any(j.title.startswith("DJS") for j in jobs)              # other agencies are not read
    assert "Current State Employees" in _read("jobaps_md.html")
    public = scraper._jobaps_public(_read("jobaps_md.html"))
    assert "Current State Employees" not in public and "Internship Opportunities" not in public


def test_jobaps_bulletin_body_and_salary_line():
    body, header = scraper.jobaps_bulletin(_read("jobaps_md_bulletin.html"))
    assert body.startswith("GRADE") and "Springfield Hospital Center, Sykesville" in body
    assert body.endswith("Salary: $106,087.00 - $167,304.00/year") and "<div" not in body
    assert "26-000351-0002" in header
    body, _ = scraper.jobaps_bulletin(_read("jobaps_ct_bulletin_cvh.html"))
    assert "Connecticut Valley Hospital" in body and body.endswith("Salary: $95,809 - $126,746/year")
    assert "Â" not in body
    assert scraper.jobaps_bulletin("<html></html>") == ("", "")


def test_scrape_jobaps_ct_reads_bulletins_and_keeps_one_hospital_rows(monkeypatch):
    board = _read("jobaps_ct.html")
    cvh = _read("jobaps_ct_bulletin_cvh.html")
    two = cvh.replace("Connecticut Valley Hospital (CVH)", "Connecticut Valley Hospital (CVH) or Connecticut Mental Health Center")

    async def fake_get(session, url, label, system, timeout=40):
        if url.endswith("/CT/"):
            return 200, board
        if "R2=5729HN" in url:                 # Middletown: one hospital named
            return 200, cvh
        if "R2=2351FX" in url:                 # Norwich: CVH named, but the row is in Norwich
            return 200, cvh
        return 200, two                        # two hospitals named

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "_board_get", fake_get)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_jobaps(None, "Connecticut DMHAS", scraper.JOBAPS_SITES["Connecticut DMHAS"]))
    assert [(j.job_id, j.hospital_name, j.city) for j in jobs] == [
        ("261005-5729HN-001", "Connecticut Valley Hospital", "Middletown")]
    assert jobs[0].description.endswith("Salary: $95,809 - $126,746/year")
    assert not any("Transportation" in j.title for j in jobs)


# ── Registry ────────────────────────────────────────────────────────────────

def test_new_configs_are_feeds_and_runners_are_wired():
    for name in ("APPLICANTSTACK_ORGS", "JAZZHR_ORGS", "JOBAPS_SITES", "TALEO_ORGS"):
        assert name in scraper.FEED_CONFIGS
    for plat in ("Taleo", "SelectMinds", "ApplicantStack", "JazzHR", "JobAps"):
        assert plat in scraper.KNOWN_BODY_PLATFORMS
    import inspect
    src = inspect.getsource(scraper.run_all)
    for runner in ("run_taleo(", "run_applicantstack(", "run_jazzhr(", "run_jobaps(", "run_selectminds("):
        assert runner in src
    feeds = scraper.configured_feed_map()
    assert "TALEO_ORGS" in feeds["Tampa General Hospital"] and "JAZZHR_ORGS" in feeds["Perimeter Healthcare"]


# ── 10-25 discovered tenants ────────────────────────────────────────────────

def test_applicantstack_columns_salary_slugs_and_default_town():
    b = scraper._parse_applicantstack(_read("applicantstack_bearlake.html"), "Bear Lake Memorial Hospital", "ID")
    assert b[0].description == "Pay: Minimum starting wage of $14.25, increases based on qualifications and experience"
    assert b[0].job_type == "Full Time/Regular Part Time/PRN" and b[0].specialty == "Skilled Nursing Facility"
    assert b[1].description == ""                                            # "N/A" is no pay
    assert (b[2].city, b[2].state) == ("Garden City", "UT")
    r = scraper._parse_applicantstack(_read("applicantstack_river.html"), "River Hospital", "NY", "Alexandria Bay")
    assert [(j.city, j.state, j.job_type) for j in r] == [("Alexandria Bay", "NY", "Full-Time · Days")] * 2
    assert (r[0].wage_min, r[0].wage_max, r[0].wage_unit) == (19.0, 21.0, "hour")
    assert (r[1].wage_min, r[1].wage_max, r[1].wage_unit) == (250000.0, 325000.0, "year")
    assert r[1].specialty == "RFHC"
    s = scraper._parse_applicantstack(_read("applicantstack_salem.html"), "Salem Regional Medical Center", "OH")
    assert s[0].job_id == "advanced-practice-provider-app-pain-management"
    assert s[0].url == "https://salemregional.applicantstack.com/x/detail/advanced-practice-provider-app-pain-management"
    assert scraper._as_wage("Commensurate with experience") is None and scraper._as_wage("$27.00 - $29.00") == (27.0, 29.0, "hour")
    assert scraper.APPLICANTSTACK_ORGS["River Hospital"] == ("riverhospital", "NY", "Alexandria Bay")


def test_applicantstack_detail_keeps_the_salary_column(monkeypatch):
    async def fake_fetch(session, url, timeout=20):
        return _read("applicantstack_detail.html")
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    job = scraper._parse_applicantstack(_read("applicantstack_bearlake.html"), "Bear Lake Memorial Hospital", "ID")[0]
    job.description = "Pay: Minimum starting wage of $14.25"
    assert asyncio.run(scraper._applicantstack_detail(None, job)) is True
    assert job.description.startswith("Delta Health System is seeking")
    assert job.description.endswith("\n\nPay: Minimum starting wage of $14.25")


def test_board_general_applications_are_not_openings():
    jobs = scraper._parse_jazzhr(_read("jazzhr_franklin.html"), "Franklin Medical Center", "LA")
    assert [j.title for j in jobs] == ["Admissions Clerk- PRN"]
    assert jobs[0].hospital_name == "Franklin Medical Center" and jobs[0].city == "Winnsboro"
    assert scraper._taleo_job("Tampa General Hospital", "tgh.taleo.net", "ex", "FL", "26000ZZ",
                              "General Application - Nursing", "United States-Florida-Tampa") is None


def test_taleo_be_department_names_the_hospital():
    jobs = scraper._parse_taleo_be_rss(_read("taleo_be_bvhs_rss.xml"), "Blanchard Valley Health System",
                                       "https://phg.tbe.taleo.net/phg02", "BVHS", "40", "OH")
    assert [(j.hospital_name, j.city) for j in jobs] == [
        ("Bluffton Hospital", "Bluffton"), ("Blanchard Valley Hospital", "Findlay"),
        ("Blanchard Valley Health System", "Findlay"), ("Blanchard Valley Health System", "Findlay")]
    assert jobs[0].url.startswith("https://phg.tbe.taleo.net/phg02/ats/careers/v2/viewRequisition?org=BVHS&cws=40&rid=")
    assert all(len(j.description) > 500 for j in jobs)
    assert scraper.TALEO_BE_ORGS["Blanchard Valley Health System"] == ("https://phg.tbe.taleo.net/phg02", "BVHS", "40", "OH")


def test_stony_brook_sections_file_under_one_system():
    assert scraper.HOSPITAL_SYSTEM_ALIASES["Stony Brook Southampton Hospital"] == "Stony Brook Medicine"
    assert scraper.HOSPITAL_SYSTEM_ALIASES["Stony Brook Eastern Long Island Hospital"] == "Stony Brook Medicine"
    assert scraper._taleo_hospital("Stony Brook Southampton Hospital", "", "Southampton") == "Stony Brook Southampton Hospital"
    assert scraper._taleo_hospital("Stony Brook Eastern Long Island Hospital", "", "Greenport") == "Stony Brook Eastern Long Island Hospital"
