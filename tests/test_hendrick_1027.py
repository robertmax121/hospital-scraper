"""Hendrick Health Apply links: the HealthcareSource employee question (2026-10-27).

Owner, 10-26: "Need to fix hendrick too." Hendrick Health's Phenom board
(careers.hendrickhealth.org) hands out HealthcareSource's #/preApply/<id> as
applyUrl (373 active rows on 10-26,
https://pm.healthcaresource.com/cs/ehendrick/#/preApply/<id>). That route is
the first step of an application: the tenant's "Employee Verification"
screen ("Are you a current employee of Hendrick Health? ...", the page's
applyFlow.applyShield config) with Next and Back, and no job description.
#/job/<id> on the same tenant is the public posting with its Apply button.
Both routes read the posting by the same id (the app's route table,
'/job/:id' and '/preApply/:id'; WebApi.getJob ->
JobseekerAPI/Site/ehendrick/api/v2/JobPostingV2?id=<id>, no sign-in).
Fetched on 10-26 for 14 Hendrick ids: 11 returned the stored title, 3 were
closed on HealthcareSource (404), which no link can change.

No other system stored a preApply link (11,914 active HealthcareSource rows
read 10-26). Only the route word changes.

No network, no database."""
import asyncio
import copy
import json
import os

import pytest

import scraper
import validate_front_pool_urls
from conftest import FIXTURES

HD_PRE = "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25609"
HD_JOB = "https://pm.healthcaresource.com/cs/ehendrick/#/job/25609"


# ── the rewrite ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("pre, job", [
    (HD_PRE, HD_JOB),                                                          # the stored shape
    ("https://pm.healthcaresource.com/cs/ehendrick#/preApply/25609",           # no slash before "#"
     "https://pm.healthcaresource.com/cs/ehendrick#/job/25609"),
    ("https://pm.healthcaresource.com/cs/ehendrick/?utm_source=waypoint_recruit&utm_medium=job_board#/preApply/25609",
     "https://pm.healthcaresource.com/cs/ehendrick/?utm_source=waypoint_recruit&utm_medium=job_board#/job/25609"),
    ("https://pm.healthcaresource.com/cs/ehendrick?x=1#/preApply/25609",       # query, no slash
     "https://pm.healthcaresource.com/cs/ehendrick?x=1#/job/25609"),
    ("https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25609?source=Indeed",   # the route's own query
     "https://pm.healthcaresource.com/cs/ehendrick/#/job/25609?source=Indeed"),
    ("https://pm.healthcaresource.com/cs/ehendrick/?a=1#/preApply/25609?source=Indeed&jobdetailspage=x",
     "https://pm.healthcaresource.com/cs/ehendrick/?a=1#/job/25609?source=Indeed&jobdetailspage=x"),
    ("https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25609/",         # a trailing slash is dropped
     HD_JOB),
    ("https://pm.healthcaresource.com/CS/bryan/#/PreApply/301",                # any tenant, any case
     "https://pm.healthcaresource.com/CS/bryan/#/job/301"),
    ("http://pm.healthcaresource.com/cs/lawrence#/preapply/15691",
     "http://pm.healthcaresource.com/cs/lawrence#/job/15691"),
    ("https://jobs.healthcaresource.com/cs/x/#/preApply/7",
     "https://jobs.healthcaresource.com/cs/x/#/job/7"),
])
def test_preapply_becomes_the_job_page(pre, job):
    assert scraper._hcs_job_url(pre) == job
    assert scraper._hcs_job_url(job) == job                      # idempotent
    assert scraper._sanitize_apply_url(pre) == job               # every row, on write
    assert scraper._sanitize_apply_url(job) == job
    assert scraper._sanitize_apply_url(scraper._sanitize_apply_url(pre)) == job


@pytest.mark.parametrize("url", [
    HD_JOB,
    "https://pm.healthcaresource.com/cs/lawrence#/job/15691",                  # the adapter's own links
    "https://pm.healthcaresource.com/cs/rwjbarnabashealth_i#/job/136024",
    "https://pm.healthcaresource.com/cs/ehendrick/",                           # the board
    "https://pm.healthcaresource.com/cs/ehendrick/#/search",
    "https://pm.healthcaresource.com/cs/ehendrick/#/apply?JobId=25609",        # not this route
    "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/",                # no id
    "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/abc",             # not a posting id
    "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25609/extra",     # an unknown shape is left as it is
    "https://pm.healthcaresource.com/JobseekerSearchAPI/ehendrick/api/Search",
    "https://pm.healthcaresource.com/CareerSite/ehendrick/#/preApply/1",       # not a /cs/ site
    "https://careers.hendrickhealth.org/us/en/job/35808",
    "https://example.org/cs/ehendrick/#/preApply/25609",                       # not a healthcaresource.com host
    "https://healthcaresource.com.example.org/cs/x/#/preApply/1",
    "https://lifecare.hcshiring.com/jobs/12345",
    "https://samhealthjobs.hctsportals.com/jobs/2149125-radiologic-tech",
    "https://uth.taleo.net/careersection/jobdetail.ftl?job=26000260",
    "https://davita.wd1.myworkdayjobs.com/DKC_External/job/03055---Escondido-Dialysis/RN_R0475128",
    "", None,
])
def test_anything_else_is_left_alone(url):
    assert scraper._hcs_job_url(url) == (url or "")
    if url:
        assert scraper._sanitize_apply_url(url) == url


def test_sanitize_keeps_its_older_repairs():
    assert scraper._sanitize_apply_url("https://uth.taleo.net/careersection/jobapply.ftl?job=26000260") == (
        "https://uth.taleo.net/careersection/jobdetail.ftl?job=26000260")
    assert scraper._sanitize_apply_url("https://uic.csod.com/ux/ats/careersite/2/requisition/20701?c=uic") == (
        "https://uic.csod.com/ux/ats/careersite/2/home/requisition/20701?c=uic")
    assert scraper._sanitize_apply_url("https://x.wd1.myworkdayjobs.com/s/job//job/A_1") == (
        "https://x.wd1.myworkdayjobs.com/s/job/A_1")


def test_the_fixture_link_from_hendricks_phenom_widget():
    with open(os.path.join(FIXTURES, "phenom", "widget_hendrick_35517.json"), encoding="utf-8") as f:
        text = f.read()
    assert "https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25344" in text
    assert scraper._sanitize_apply_url("https://pm.healthcaresource.com/cs/ehendrick/#/preApply/25344") == (
        "https://pm.healthcaresource.com/cs/ehendrick/#/job/25344")


# ── the content hash ───────────────────────────────────────────────────────

def _row(url):
    return {"hospital_system": "Hendrick Health", "job_id": "35808", "title": "Scheduling Rep, Hendrick Urgent Care Ambler",
            "url": url, "apply_verified": True, "ats_platform": "Phenom", "posting_facts": {"h": "x"}}


def test_the_hash_moves_with_the_link():
    """A Hendrick row written by the old code hashes the preApply link; the
    fixed code's payload hashes the job page, so each such row is rewritten
    once, and only the link differs between the two payloads."""
    old = _row(HD_PRE)
    new = dict(old, url=scraper._sanitize_apply_url(old["url"]))
    assert new["url"] == HD_JOB
    assert scraper.content_hash_for(old) != scraper.content_hash_for(new)
    assert {k for k in old if old[k] != new[k]} == {"url"}
    assert scraper._apply_verified_for(HD_PRE) == scraper._apply_verified_for(HD_JOB)    # same flag either way


@pytest.mark.parametrize("url", [
    "https://pm.healthcaresource.com/cs/lawrence#/job/15691",
    "https://careers.hendrickhealth.org/us/en/job/35808",
    "https://uth.taleo.net/careersection/jobdetail.ftl?job=26000260",
])
def test_the_hash_of_any_other_row_does_not_move(url):
    row = _row(url)
    assert scraper.content_hash_for(dict(row, url=scraper._sanitize_apply_url(row["url"]))) == scraper.content_hash_for(row)


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


_PHENOM_HTML = ('<script>phApp.ddo = {}; phApp.pageInfo = {"refNum":"HHSHHSUS","siteType":"external",'
                '"rootDomain":"https://careers.hendrickhealth.org","pageId":"page24"};</script>')


def test_phenom_stores_the_hendrick_job_page(monkeypatch):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    with open(os.path.join(FIXTURES, "caps1014", "phenom_davita_refine_located.json"), encoding="utf-8") as f:
        tmpl = json.load(f)["refineSearch"]["data"]["jobs"][0]
    jobs = []
    for jid, url in (("35808", HD_PRE),
                     ("35807", "https://pm.healthcaresource.com/cs/ehendrick/#/job/25608"),
                     ("35806", "")):
        j = copy.deepcopy(tmpl)
        j.update(jobId=jid, reqId=jid, applyUrl=url, title=f"Job {jid}")
        jobs.append(j)
    base = "https://careers.hendrickhealth.org"

    def fake_req(session, method, url, **kw):
        if method == "post" and url == f"{base}/widgets" and (kw.get("json") or {}).get("ddoKey") == "refineSearch":
            return _Ctx(_R(200, {"refineSearch": {"status": 200, "hits": 3, "totalHits": 3, "data": {"jobs": jobs}}}))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, _PHENOM_HTML))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    got = {j.job_id: j.url for j in asyncio.run(scraper.scrape_phenom(None, "Hendrick Health", base))}
    assert got["35808"] == HD_JOB
    assert got["35807"] == "https://pm.healthcaresource.com/cs/ehendrick/#/job/25608"     # a job page stays
    assert got["35806"] == "https://careers.hendrickhealth.org/us/en/job/35806"           # no link: as before
    assert not any("preApply" in u for u in got.values())


# ── the QA link gate and the nightly link check ────────────────────────────

def test_gate_names_the_employee_question():
    assert scraper._structural_url_ok(HD_PRE, "Phenom") == (False, "apply_first_step")
    assert scraper._structural_url_ok(HD_JOB, "Phenom") == (True, "")
    assert scraper._structural_url_ok("https://pm.healthcaresource.com/cs/lawrence#/job/15691", "HealthcareSource") == (True, "")


def test_front_pool_check_treats_the_new_link_like_every_healthcaresource_job_page():
    # the posting id is in the fragment, which never reaches the server: the
    # nightly link check counts these rows and leaves them alone, as it does
    # for the 11,541 rows the HealthcareSource adapter writes
    assert validate_front_pool_urls._unverifiable(HD_JOB) == "fragment route"
    assert validate_front_pool_urls._unverifiable("https://pm.healthcaresource.com/cs/lawrence#/job/15691") == "fragment route"
