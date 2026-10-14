# 2026-10-14 (owner: "ucla health is scraping wrong job id's"): UCLA Health's
# requisition number. job_id is the Findly posting id in the
# uclahealthcareers.org URL (23914219, the owner's example); the page prints
# "Job #33102", its Refer A Friend link is EmpRef/?ReqNo=33102, and the
# search payload carries the same number as job.ref (job.clientid is null on
# every UCLA row). Stored as posting_facts["req_id"] through the same
# plumbing as AdventHealth; job_id, url and the upsert key are unchanged.
# Fixtures are real 2026-10-14 snippets (tests/fixtures/findly_req/): five
# jobs of the UCLA search payload (the example job whole, the other bodies cut
# at a paragraph end) and excerpts of the owner's example page.
import asyncio
import dataclasses
import json
import re

import scraper

UCLA = scraper.FINDLY_GOOGLE_ORGS["UCLA Health"]

# job_id -> the number the job page prints as "Job #"
PRINTED = {
    "23914219": "33102",     # the owner's example (our job 32693903)
    "24007335": "33449",
    "24007005": "33535",
    "24007006": "33542",
    "24007009": "33727",
}


class _R:
    status = 200

    def __init__(self, text):
        self._t = text

    async def text(self):
        return self._t


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, body):
        self.body = body
        self.calls = 0

    def get(self, url, **kw):
        self.calls += 1
        return _Ctx(_R(self.body))


def _scrape(fixture_text, system="UCLA Health", org=UCLA):
    session = _Session(fixture_text("findly_req/ucla_search.txt"))
    jobs = asyncio.run(scraper.scrape_findly_google(session, system, org))
    assert session.calls == 1
    return jobs


def _payload_jobs(fixture_text):
    body = fixture_text("findly_req/ucla_search.txt")
    data = json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))
    return [r["job"] for r in data["searchResults"]]


def test_search_rows_carry_the_printed_job_number(fixture_text):
    jobs = _scrape(fixture_text)
    assert [(j.job_id, j.req_id) for j in jobs] == list(PRINTED.items())
    ex = next(j for j in jobs if j.job_id == "23914219")
    # job_id and url stay the Findly posting id and page
    assert ex.url.endswith("/job/23914219/lvn-supervisor-plastic-reconstructive-surgery-westwood-los-angeles-ca/")
    assert ex.ats_platform == "Findly-Google" and ex.hospital_system == "UCLA Health"


def test_the_payload_has_ref_and_no_clientid(fixture_text):
    for j in _payload_jobs(fixture_text):
        assert j["clientid"] is None
        assert scraper.findly_google_req_id(j, "ref") == PRINTED[str(j["id"])]
        # the default key (AdventHealth's clientid) finds nothing on a UCLA row
        assert scraper.findly_google_req_id(j) == ""


def test_the_job_page_prints_ref_as_its_job_number(fixture_text):
    page = fixture_text("findly_req/ucla_job_23914219.html")
    assert "UCLA Health Careers - Job #33102 - Los Angeles, CA</title>" in page
    assert 'href="https://hshr.mednet.ucla.edu/EmpRef/?ReqNo=33102"' in page
    assert '"id":23914219,' in page and '"ref":"33102"' in page
    # Avature's own page id (JobDetail/33188) is not the job number
    assert "avature.net/careers/JobDetail/33188" in page
    payload = next(j for j in _payload_jobs(fixture_text) if str(j["id"]) == "23914219")
    assert payload["ref"] == "33102"


def test_odd_ref_gives_blank():
    f = scraper.findly_google_req_id
    assert f({"ref": "33102"}, "ref") == "33102"
    assert f({"ref": " 33102 "}, "ref") == "33102"
    assert f({"ref": ""}, "ref") == ""
    assert f({"ref": None}, "ref") == ""
    assert f({"ref": "see posting"}, "ref") == ""
    assert f({"ref": "1" * 30}, "ref") == ""
    assert f({"clientid": "33102"}, "ref") == ""
    assert f(None, "ref") == ""


def test_other_findly_google_tenants_are_untouched(fixture_text):
    # 2026-10-14: UPMC and University Hospitals now read ref too
    # (tests/test_reqid_1014_findly_google.py); a tenant with no entry in
    # FINDLY_GOOGLE_REQ_ID still gets no number.
    jobs = _scrape(fixture_text, "Some Other Findly Tenant", scraper.FINDLY_GOOGLE_ORGS["UPMC"])
    assert len(jobs) == 5 and all(j.req_id == "" for j in jobs)


def test_normalize_job_sends_it_as_posting_facts_req_id(fixture_text):
    j = next(x for x in _scrape(fixture_text) if x.job_id == "23914219")
    d = scraper.normalize_job(j)
    assert "req_id" not in d
    assert d["job_id"] == "23914219" and d["url"] == j.url
    assert d["posting_facts"]["req_id"] == "33102"
    # the facts hash moves with the number, so stored UCLA facts are rebuilt once
    plain = scraper.normalize_job(dataclasses.replace(j, req_id=""))
    assert plain["posting_facts"]["h"] != d["posting_facts"]["h"]
    assert "req_id" not in plain["posting_facts"]
