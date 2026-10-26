"""Apply links that land on a sign-in page (2026-10-27).

Owner, 10-26: "UT Health Houston jobs are not linking to the direct
application page", with the link a visitor reached: Taleo's sign-in page
(/careersection/iam/accessmanagement/login.jsf?redirectionURI=...jobapply.ftl...).

Phenom's applyUrl for the Taleo-backed tenants is Taleo's apply step,
jobapply.ftl: UTHealth Houston (431 active rows on 10-26, no career section
in the path) and Children's Minnesota (154, sections "ex" and "physician").
Opened cold, uth.taleo.net answers its Privacy Agreement and childrensmn a
script that goes straight to login.jsf. jobdetail.ftl?job=<id> under the
same section is the public posting (fetched on 10-26 for all four forms:
each page carried the job's title). A link with no section keeps none:
Taleo 302s /careersection/jobdetail.ftl?job=<id> to the section the job is
posted in, and on uth.taleo.net that differs by job (staff "ex", faculty
"ex2"; "ex" calls a faculty job no longer available).

No network, no database."""
import asyncio
import copy
import json
import os

import pytest

import scraper
from conftest import FIXTURES

UTH_APPLY = "https://uth.taleo.net/careersection/jobapply.ftl?job=26000260"
UTH_PAGE = "https://uth.taleo.net/careersection/jobdetail.ftl?job=26000260"
UTH_EX_PAGE = "https://uth.taleo.net/careersection/ex/jobdetail.ftl?job=26000260"
UTH_FACULTY_APPLY = "https://uth.taleo.net/careersection/jobapply.ftl?job=260001OI"
UTH_FACULTY_PAGE = "https://uth.taleo.net/careersection/jobdetail.ftl?job=260001OI"     # 302 -> /ex2/
CMN_EX_APPLY = "https://childrensmn.taleo.net/careersection/ex/jobapply.ftl?lang=en&job=260271K6"
CMN_EX_PAGE = "https://childrensmn.taleo.net/careersection/ex/jobdetail.ftl?job=260271K6&lang=en"
CMN_DOC_APPLY = "https://childrensmn.taleo.net/careersection/physician/jobapply.ftl?lang=en&job=260270PQ"
CMN_DOC_PAGE = "https://childrensmn.taleo.net/careersection/physician/jobdetail.ftl?job=260270PQ&lang=en"
# The link the owner reached (the site had added its utm tags to UTH_APPLY,
# and Taleo had moved it under its "ex" section on the way to sign-in).
OWNER_LOGIN = (
    "https://uth.taleo.net/careersection/iam/accessmanagement/login.jsf?redirectionURI=https%3A%2F%2Futh.taleo.net"
    "%2Fcareersection%2Fex%2Fjobapply.ftl%3Fjob%3D26000260%26utm_source%3Dwaypoint_recruit%26utm_medium%3Djob_board"
    "%26utm_campaign%3Duthealth_houston_26000260%26utm_content%3Dorganic&TARGET=https%3A%2F%2Futh.taleo.net"
    "%2Fcareersection%2Fex%2Fjobapply.ftl%3Fjob%3D26000260%26utm_source%3Dwaypoint_recruit%26utm_medium%3Djob_board"
    "%26utm_campaign%3Duthealth_houston_26000260%26utm_content%3Dorganic")


# ── the rewrite ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("apply_url, page", [
    (UTH_APPLY, UTH_PAGE),                    # no section: Taleo routes the job page to the job's own
    (UTH_FACULTY_APPLY, UTH_FACULTY_PAGE),    # ... "ex" for staff, "ex2" for faculty
    (CMN_EX_APPLY, CMN_EX_PAGE),
    (CMN_DOC_APPLY, CMN_DOC_PAGE),
    (OWNER_LOGIN, UTH_EX_PAGE),               # the sign-in page wrapping an apply step (Taleo had added "ex")
    ("https://uth.taleo.net:443/careersection/ex/jobapply.ftl?job=26000260", UTH_EX_PAGE),
    ("https://uth.taleo.net/careersection/ex/jobapply.ftl?job=26000260", UTH_EX_PAGE),
    # BSW's Phenom applyUrl shape (fixture reqid_1014/phenom): csNo and portal dropped
    ("https://bswhealth.taleo.net/careersection/ex/jobapply.ftl?job=26018384&lang=en&csNo=2&portal=101430233",
     "https://bswhealth.taleo.net/careersection/ex/jobdetail.ftl?job=26018384&lang=en"),
    ("https://upmcjobs.taleo.net/careersection/UPMC_External/jobapply.ftl?lang=en&job=260001S0",
     "https://upmcjobs.taleo.net/careersection/UPMC_External/jobdetail.ftl?job=260001S0&lang=en"),
    ("https://example.taleo.net/careersection/jobapply.ftl?job=123&lang=en",
     "https://example.taleo.net/careersection/jobdetail.ftl?job=123&lang=en"),
])
def test_taleo_apply_step_becomes_the_job_page(apply_url, page):
    assert scraper._taleo_job_url(apply_url) == page
    assert scraper._taleo_job_url(page) == page                  # idempotent
    assert scraper._sanitize_apply_url(apply_url) == page        # every row, on write
    assert scraper._sanitize_apply_url(page) == page


@pytest.mark.parametrize("url", [
    UTH_PAGE, CMN_EX_PAGE,
    "https://example.taleo.net/careersection/2/jobapply.ftl?lang=en",            # no job number: nothing to point at
    "https://example.taleo.net/careersection/iam/accessmanagement/login.jsf?lang=en",
    "https://phg.tbe.taleo.net/phg04/ats/careers/v2/viewRequisition?org=COMACOUN&cws=37&rid=7325",  # Taleo Business Edition
    "https://davita.wd1.myworkdayjobs.com/DKC_External/job/03055---Escondido-Dialysis/RN_R0475128",
    "https://careers-arh.icims.com/jobs/39813/clinic-lpn/job",
    "https://jobs.example.org/careersection/2/jobapply.ftl?job=1",               # not a taleo.net host
    "", None,
])
def test_anything_else_is_left_alone(url):
    assert scraper._taleo_job_url(url) == (url or "")


def test_sanitize_keeps_its_older_repairs():
    assert scraper._sanitize_apply_url("https://x.wd1.myworkdayjobs.com/s/job//job/A_1") == "https://x.wd1.myworkdayjobs.com/s/job/A_1"
    assert scraper._sanitize_apply_url(
        "https://e.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX/jobs/123") == (
        "https://e.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX/job/123")


def test_every_taleo_apply_link_in_the_fixtures_keeps_its_job_number():
    import re
    from urllib.parse import parse_qs, urlsplit
    seen = 0
    for root, _d, files in os.walk(FIXTURES):
        for name in files:
            with open(os.path.join(root, name), encoding="utf-8", errors="replace") as f:
                text = f.read()
            for u in set(re.findall(r'https://[a-z0-9-]+\.taleo\.net/careersection/[^"\\\s]*jobapply\.ftl\?[^"\\\s]+', text)):
                out = scraper._taleo_job_url(u)
                assert "/jobdetail.ftl?" in out and "jobapply" not in out
                assert urlsplit(out).path.split("/")[2] == urlsplit(u).path.split("/")[2]          # same section
                assert parse_qs(urlsplit(out).query)["job"] == parse_qs(urlsplit(u).query)["job"]
                seen += 1
    assert seen >= 18      # Phenom BSW, Findly-Google Scripps / UH / UPMC / UTSW


def test_the_hash_moves_with_the_url():
    """Rows written by the old code hash the apply link; the fixed code's
    payload hashes the job page, so each such row is rewritten once."""
    row = {"hospital_system": "UTHealth Houston", "job_id": "26000260", "title": "Electrician II (Facilities)",
           "url": UTH_APPLY, "posting_facts": {"h": "x"}}
    fixed = dict(row, url=scraper._sanitize_apply_url(row["url"]))
    assert scraper.content_hash_for(row) != scraper.content_hash_for(fixed)


# ── the Phenom adapter ─────────────────────────────────────────────────────

class _R:
    def __init__(self, status, body):
        self.status, self._body, self.url = status, body, ""
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


_PHENOM_HTML = ('<script>phApp.ddo = {}; phApp.pageInfo = {"refNum":"UHHUHHUS","siteType":"external",'
                '"rootDomain":"https://careers.example.org","pageId":"page24"};</script>')


@pytest.fixture
def phenom_board(monkeypatch):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    with open(os.path.join(FIXTURES, "caps1014", "phenom_davita_refine_located.json"), encoding="utf-8") as f:
        tmpl = json.load(f)["refineSearch"]["data"]["jobs"][0]
    links = [(UTH_APPLY, "26000260"), (CMN_EX_APPLY, "260271K6"), (CMN_DOC_APPLY, "260270PQ"),
             ("https://davita.wd1.myworkdayjobs.com/DKC_External/job/01601---Hastings-Dialysis/PCT_R0480722/apply", "R0480722")]
    jobs = []
    for url, jid in links:
        j = copy.deepcopy(tmpl)
        j.update(jobId=jid, reqId=jid, applyUrl=url, title=f"Job {jid}")
        jobs.append(j)
    base = "https://careers.example.org"

    def fake_req(session, method, url, **kw):
        if method == "post" and url == f"{base}/widgets" and (kw.get("json") or {}).get("ddoKey") == "refineSearch":
            return _Ctx(_R(200, {"refineSearch": {"status": 200, "hits": len(jobs), "totalHits": len(jobs),
                                                  "data": {"jobs": jobs}}}))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, _PHENOM_HTML))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return base


def test_phenom_stores_the_taleo_job_page(phenom_board):
    jobs = asyncio.run(scraper.scrape_phenom(None, "UTHealth Houston", phenom_board))
    got = {j.job_id: j.url for j in jobs}
    assert got["26000260"] == UTH_PAGE
    assert got["260271K6"] == CMN_EX_PAGE
    assert got["260270PQ"] == CMN_DOC_PAGE
    # the Workday apply step is still cut back to the job page, as before
    assert got["R0480722"] == "https://davita.wd1.myworkdayjobs.com/DKC_External/job/01601---Hastings-Dialysis/PCT_R0480722"
    assert not any("jobapply" in u for u in got.values())


# ── Cornerstone (CSOD): the requisition link without /home/ ────────────────
# Fetched 10-26 on all three live tenants: .../careersite/<id>/requisition/<rid>
# 302s to /common/restrictedarea.aspx and on to the employee sign-in (SAML
# single sign-on for Billings Clinic and JPS, a password page for UI Health);
# .../careersite/<id>/home/requisition/<rid> is the public career-site page.

@pytest.mark.parametrize("old, new", [
    ("https://billingsclinic.csod.com/ux/ats/careersite/1/requisition/12946?c=billingsclinic",
     "https://billingsclinic.csod.com/ux/ats/careersite/1/home/requisition/12946?c=billingsclinic"),
    ("https://jpshealthnet.csod.com/ux/ats/careersite/4/requisition/30966?c=jpshealthnet",
     "https://jpshealthnet.csod.com/ux/ats/careersite/4/home/requisition/30966?c=jpshealthnet"),
    ("https://uic.csod.com/ux/ats/careersite/2/requisition/20701?c=uic",
     "https://uic.csod.com/ux/ats/careersite/2/home/requisition/20701?c=uic"),
])
def test_csod_requisition_link_gets_the_home_step(old, new):
    assert scraper._csod_job_url(old) == new
    assert scraper._csod_job_url(new) == new
    assert scraper._sanitize_apply_url(old) == new
    assert scraper._sanitize_apply_url(new) == new


@pytest.mark.parametrize("url", [
    "https://jpshealthnet.csod.com/ux/ats/careersite/4/home?c=jpshealthnet",
    "https://jpshealthnet.csod.com/ux/ats/careersite/4/home/requisition/30966?c=jpshealthnet",
    "https://jpshealthnet.csod.com/ux/ats/careersite/4/requisition/30966/apply?c=jpshealthnet",
    "https://example.org/ux/ats/careersite/4/requisition/30966?c=x",
    "", None,
])
def test_csod_other_links_are_left_alone(url):
    assert scraper._csod_job_url(url) == (url or "")


def test_csod_adapter_writes_the_career_site_link(fixture_text):
    d = json.loads(fixture_text("csod_search.json"))
    for rq in d["data"]["requisitions"]:
        j = scraper._csod_job(rq, "Billings Clinic", "https://billingsclinic.csod.com", "1", "billingsclinic")
        assert j.url == f"https://billingsclinic.csod.com/ux/ats/careersite/1/home/requisition/{j.job_id}?c=billingsclinic"
        assert scraper._structural_url_ok(j.url, "CSOD") == (True, "")


# ── UAB: iCIMS's candidate sign-in step from the Phenom board ──────────────

UAB_LOGIN = "https://careers-uabmedicine.icims.com/jobs/8020/rn---registered-nurse---adult-psych---multiple-shifts/login"


def test_icims_login_step_shape():
    rx = scraper._ICIMS_LOGIN_STEP_RX
    assert rx.match(UAB_LOGIN) and rx.match(UAB_LOGIN + "/") and rx.match(UAB_LOGIN + "?mode=apply")
    for u in (UAB_LOGIN[:-len("login")] + "job",
              "https://careers-uabmedicine.icims.com/jobs/8020/login-coordinator/job",
              "https://careers.uabmedicine.org/us/en/job/8020",
              "https://jobs.example.org/jobs/8020/x/login"):
        assert not rx.match(u)


def test_phenom_icims_login_step_becomes_the_phenom_job_page(monkeypatch):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    with open(os.path.join(FIXTURES, "caps1014", "phenom_davita_refine_located.json"), encoding="utf-8") as f:
        tmpl = json.load(f)["refineSearch"]["data"]["jobs"][0]
    jobs = []
    for jid, url in (("8020", UAB_LOGIN),
                     ("8021", "https://careers-uabmedicine.icims.com/jobs/8021/lpn/job"),
                     ("8022", "")):
        j = copy.deepcopy(tmpl)
        j.update(jobId=jid, reqId=jid, applyUrl=url, title=f"Job {jid}")
        jobs.append(j)
    base = "https://careers.uabmedicine.org"

    def fake_req(session, method, url, **kw):
        if method == "post" and url == f"{base}/widgets" and (kw.get("json") or {}).get("ddoKey") == "refineSearch":
            return _Ctx(_R(200, {"refineSearch": {"status": 200, "hits": 3, "totalHits": 3, "data": {"jobs": jobs}}}))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, _PHENOM_HTML))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    got = {j.job_id: j.url for j in asyncio.run(scraper.scrape_phenom(None, "UAB Health System", base))}
    assert got["8020"] == "https://careers.uabmedicine.org/us/en/job/8020"
    assert got["8021"] == "https://careers-uabmedicine.icims.com/jobs/8021/lpn/job"     # a job page stays
    assert got["8022"] == "https://careers.uabmedicine.org/us/en/job/8022"              # no link: as before


# ── the QA link gate names the sign-in steps ───────────────────────────────

@pytest.mark.parametrize("url", [UTH_APPLY, OWNER_LOGIN, CMN_DOC_APPLY, UAB_LOGIN,
                                 "https://uic.csod.com/ux/ats/careersite/2/requisition/20701?c=uic"])
def test_gate_flags_a_sign_in_step(url):
    assert scraper._structural_url_ok(url, "Phenom") == (False, "sign_in_step")


@pytest.mark.parametrize("url", [UTH_PAGE, CMN_EX_PAGE, "https://uic.csod.com/ux/ats/careersite/2/home/requisition/20701?c=uic",
                                 "https://careers.uabmedicine.org/us/en/job/8020",
                                 "https://careers-arh.icims.com/jobs/39813/clinic-lpn/job"])
def test_gate_passes_the_job_pages(url):
    assert scraper._structural_url_ok(url, "Phenom") == (True, "")
