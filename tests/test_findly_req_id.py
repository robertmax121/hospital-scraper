# 2026-10-12: AdventHealth requisition number. job_id is the Findly posting
# id in the jobs.adventhealth.com URL (23744623); the job page prints
# "Req #: R-0379591". The Findly Google search payload carries it as
# job.clientid; job.ref (and the page's JSON-LD identifier.value) carries a
# per-location "-N" suffix on requisitions posted to several locations, so it
# is not read. Stored as posting_facts["req_id"] through the HCA plumbing;
# job_id, url and the upsert key are unchanged.
# Fixtures are real 2026-10-12 snippets (tests/fixtures/findly_req/): five
# jobs of the AdventHealth search payload (bodies cut at a paragraph end) and
# excerpts of four of their job pages.
import asyncio
import dataclasses
import json
import re

import pytest

import scraper

ADVENT = scraper.FINDLY_GOOGLE_ORGS["AdventHealth"]

# job_id -> the number the job page prints under "Req #"
PRINTED = {
    "23998822": "R-0407018",
    "23999932": "R-0407667",     # ref "R-0407667-1": one location of a multi-location requisition
    "23744623": "R-0379591",     # the owner's example (our job 19868876)
    "22822074": "R-316207",      # ref "R-316207-28"; the older six-digit R number
    "22955020": "315099",        # pre-R numeric requisition, kept as printed
}
PAGES = ["23744623", "23999932", "22822074", "22955020"]


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


def _scrape(fixture_text, system="AdventHealth", org=ADVENT):
    session = _Session(fixture_text("findly_req/adventhealth_search.txt"))
    jobs = asyncio.run(scraper.scrape_findly_google(session, system, org))
    assert session.calls == 1
    return jobs


def _payload_jobs(fixture_text):
    body = fixture_text("findly_req/adventhealth_search.txt")
    data = json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))
    return [r["job"] for r in data["searchResults"]]


def test_search_rows_carry_the_printed_requisition(fixture_text):
    jobs = _scrape(fixture_text)
    assert [(j.job_id, j.req_id) for j in jobs] == list(PRINTED.items())
    ex = next(j for j in jobs if j.job_id == "23744623")
    # job_id and url stay the Findly posting id and page
    assert ex.url == ("https://jobs.adventhealth.com/job/23744623/registered-nurse-rn-endoscopy-"
                      "sign-on-and-relocation-available-kissimmee-fl/")
    assert ex.ats_platform == "Findly-Google" and ex.hospital_system == "AdventHealth"


@pytest.mark.parametrize("job_id", PAGES)
def test_the_job_page_prints_clientid_not_ref(fixture_text, job_id):
    page = fixture_text(f"findly_req/adventhealth_job_{job_id}.html")
    want = PRINTED[job_id]
    assert re.search(r"<strong>Req #:</strong>\s*" + re.escape(want) + r"\s*</li>", page)
    assert f'<span id="gtm-jobdetail-idSC" style="display:none;">{want}</span>' in page
    assert f'"clientid":"{want}"' in page and f'"id":{job_id},' in page
    payload = next(j for j in _payload_jobs(fixture_text) if str(j["id"]) == job_id)
    assert scraper.findly_google_req_id(payload) == want
    # the JSON-LD identifier repeats ref, suffix included, so it is not the source
    ref = re.search(r'"ref":"([^"]*)"', page).group(1)
    assert ref == payload["ref"]
    assert scraper._ld_identifier_value(scraper._jobposting_from_html(page)) == ref


def test_a_multi_location_ref_suffix_is_not_the_requisition(fixture_text):
    by_id = {str(j["id"]): j for j in _payload_jobs(fixture_text)}
    assert by_id["23999932"]["ref"] == "R-0407667-1"
    assert by_id["22822074"]["ref"] == "R-316207-28"
    assert scraper.findly_google_req_id(by_id["23999932"]) == "R-0407667"
    assert scraper.findly_google_req_id(by_id["22822074"]) == "R-316207"


def test_blank_or_odd_clientid_gives_blank():
    f = scraper.findly_google_req_id
    assert f({"clientid": "R-0379591", "ref": "R-0379591"}) == "R-0379591"
    assert f({"clientid": " R-0379591 "}) == "R-0379591"
    assert f({"ref": "R-0379591"}) == ""                    # ref alone is never read
    assert f({"clientid": None, "ref": "R-0407667-1"}) == ""
    assert f({"clientid": ""}) == ""
    assert f({"clientid": "see posting"}) == ""
    assert f({"clientid": "R-" + "1" * 30}) == ""
    assert f(None) == "" and f("R-0379591") == ""


def test_other_findly_google_tenants_are_untouched(fixture_text):
    # 2026-10-14: UPMC, University Hospitals, UT Southwestern and Scripps read
    # ref (tests/test_reqid_1014_findly_google.py); AdventHealth keeps clientid.
    assert scraper.FINDLY_GOOGLE_REQ_ID == {"AdventHealth": "clientid", "UCLA Health": "ref",
                                            "UPMC": "ref", "University Hospitals": "ref",
                                            "UT Southwestern Medical Center": "ref", "Scripps Health": "ref"}
    jobs = _scrape(fixture_text, "Some Other Findly Tenant", scraper.FINDLY_GOOGLE_ORGS["UPMC"])
    assert len(jobs) == 5 and all(j.req_id == "" for j in jobs)


def test_normalize_job_sends_it_as_posting_facts_req_id(fixture_text):
    j = next(x for x in _scrape(fixture_text) if x.job_id == "23744623")
    assert len(j.description) >= 200
    d = scraper.normalize_job(j)
    assert "req_id" not in d
    assert d["job_id"] == "23744623" and d["url"] == j.url
    assert d["posting_facts"]["req_id"] == "R-0379591"
    # the facts hash moves with the number, so stored AdventHealth facts are rebuilt once
    plain = scraper.normalize_job(dataclasses.replace(j, req_id=""))
    assert plain["posting_facts"]["h"] != d["posting_facts"]["h"]
    assert "req_id" not in plain["posting_facts"]
