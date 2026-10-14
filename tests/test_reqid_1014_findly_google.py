# 2026-10-14 (owner: "we need to fix those now"): the job number UPMC,
# University Hospitals, UT Southwestern and Scripps print on their job pages
# is the Findly search payload's job.ref. job_id stays the Findly posting id
# in the page URL; the number travels as posting_facts["req_id"] through the
# AdventHealth / UCLA plumbing (FINDLY_GOOGLE_REQ_ID, findly_google_req_id).
# Fixtures (tests/fixtures/reqid_1014/findly_google/) are real 2026-10-14
# snippets: the search payload's job objects for the pages checked that day
# (bodies cut at a paragraph end, recruiter fields and e-mail addresses
# redacted) and excerpts of each job page (title, the printed label, the
# JSON-LD identifier).
import asyncio
import dataclasses
import json
import re

import pytest

import scraper

TENANTS = {
    # system: (fixture slug, {job_id: number the page prints}, label as printed)
    "UPMC": ("upmc", {"24007855": "8457814813", "23971655": "2600023K", "23758364": "260001S0"},
             "<strong>Job ID:</strong> {}"),
    "University Hospitals": ("uh", {"23842240": "260008MP", "23856269": "260008QX", "23455340": "25000645"},
                             "<b>Position #:</b> {}"),
    "UT Southwestern Medical Center": ("utsw", {"23943857": "978816", "24007454": "260000KR",
                                                "23891515": "968980", "24007415": "980455"},
                                       '<span>Job Number:</span> <span id="gtm-jobdetail-id">{}</span>'),
    "Scripps Health": ("scripps", {"24008661": "262005926", "23911683": "262005506", "22560864": "252005698"},
                       "<strong>Job ID:</strong> {}"),
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


def _payload(fixture_text, slug):
    return fixture_text(f"reqid_1014/findly_google/{slug}_search.txt")


def _payload_jobs(fixture_text, slug):
    body = _payload(fixture_text, slug)
    data = json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))
    return [r["job"] for r in data["searchResults"]]


def _scrape(fixture_text, system):
    slug = TENANTS[system][0]
    session = _Session(_payload(fixture_text, slug))
    jobs = asyncio.run(scraper.scrape_findly_google(session, system, scraper.FINDLY_GOOGLE_ORGS[system]))
    assert session.calls == 1
    return jobs


def test_the_four_tenants_read_ref():
    for system in TENANTS:
        assert scraper.FINDLY_GOOGLE_REQ_ID[system] == "ref"
    # the earlier tenants are unchanged
    assert scraper.FINDLY_GOOGLE_REQ_ID["AdventHealth"] == "clientid"
    assert scraper.FINDLY_GOOGLE_REQ_ID["UCLA Health"] == "ref"


@pytest.mark.parametrize("system", list(TENANTS))
def test_search_rows_carry_the_number_the_page_prints(fixture_text, system):
    slug, printed, label = TENANTS[system]
    jobs = _scrape(fixture_text, system)
    assert {j.job_id: j.req_id for j in jobs} == printed
    for j in jobs:
        assert j.ats_platform == "Findly-Google" and j.hospital_system == system
        # job_id stays the Findly posting id; the url is the payload's own
        assert j.req_id != j.job_id
        page = fixture_text(f"reqid_1014/findly_google/{slug}_job_{j.job_id}.html")
        assert label.format(j.req_id) in page
        assert re.search(r'"value":\s*"%s"' % re.escape(j.req_id), page)   # JSON-LD identifier agrees


@pytest.mark.parametrize("system", list(TENANTS))
def test_no_ref_carries_a_per_location_suffix(fixture_text, system):
    for j in _payload_jobs(fixture_text, TENANTS[system][0]):
        assert not re.search(r"-\d+$", j["ref"])
        assert scraper.findly_google_req_id(j, "ref") == j["ref"]


def test_utsw_faculty_row_reads_ref_not_the_interfolio_clientid(fixture_text):
    raw = next(j for j in _payload_jobs(fixture_text, "utsw") if j["id"] == 24007415)
    assert raw["clientid"] == "195567" and raw["ref"] == "980455"
    assert raw["url"] == "https://apply.interfolio.com/195567"
    job = next(j for j in _scrape(fixture_text, "UT Southwestern Medical Center") if j.job_id == "24007415")
    assert job.req_id == "980455"
    assert job.url == "https://apply.interfolio.com/195567"      # the stored Apply link is unchanged
    page = fixture_text("reqid_1014/findly_google/utsw_job_24007415.html")
    assert '<span id="gtm-jobdetail-id">980455</span>' in page and "195567" not in page.split("-->", 1)[1]


def test_normalize_job_rebuilds_the_facts_with_the_number(fixture_text):
    scraper.set_known_bodies([])
    job = next(j for j in _scrape(fixture_text, "UPMC") if j.job_id == "24007855")
    d = scraper.normalize_job(job)
    assert "req_id" not in d and d["job_id"] == "24007855" and d["url"] == job.url
    assert d["posting_facts"]["req_id"] == "8457814813"
    plain = scraper.normalize_job(dataclasses.replace(job, req_id=""))
    assert plain["posting_facts"]["h"] != d["posting_facts"]["h"]


def test_a_row_whose_facts_are_null_still_sends_the_number(fixture_text, monkeypatch):
    """A teaser with no facts (posting_facts_for -> None) used to drop the
    number unless REQ_ID_ONLY_FACTS=1; Findly-Google now sends the
    {"req_id"}-only object, which the live trigger merges into the stored facts."""
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    scraper.set_known_bodies([])
    job = next(j for j in _scrape(fixture_text, "Scripps Health") if j.job_id == "24008661")
    teaser = dataclasses.replace(job, description="", title="Care Team Assistant", job_type="")
    d = scraper.normalize_job(teaser)
    assert d["posting_facts"] == {"req_id": "262005926"}
    # a platform outside REQ_ID_ONLY_PLATFORMS keeps the old null
    other = scraper.normalize_job(dataclasses.replace(teaser, ats_platform="Findly"))
    assert other["posting_facts"] is None
