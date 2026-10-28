# 2026-10-28 (owner: "yes, change it to that"): UT Southwestern's rows showed
# "University Of Texas Southwestern Medical Cent (The" as the employer on cards
# and job pages: the Findly Google search payload's company_name, the legal
# name cut at 50 characters with the article moved to the end. Every UTSW row
# now carries "UT Southwestern Medical Center", the name its own job pages
# print (FINDLY_GOOGLE_NAME). Fixtures are the real 2026-10-14 snippets of
# tests/fixtures/reqid_1014/findly_google/ (search payload and job pages).
import asyncio
import dataclasses
import json
import re

import pytest

import scraper

UTSW = "UT Southwestern Medical Center"
CUT = "University Of Texas Southwestern Medical Cent (The"


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

    def get(self, url, **kw):
        return _Ctx(_R(self.body))


def _body(fixture_text, slug="utsw"):
    return fixture_text(f"reqid_1014/findly_google/{slug}_search.txt")


def _payload_jobs(fixture_text, slug="utsw"):
    data = json.loads(re.match(r"[^(]*\((.*)\);?\s*$", _body(fixture_text, slug), re.S).group(1))
    return [r["job"] for r in data["searchResults"]]


def _scrape(fixture_text, system=UTSW, slug="utsw"):
    org = scraper.FINDLY_GOOGLE_ORGS.get(system) or scraper.FINDLY_GOOGLE_ORGS[UTSW]
    return asyncio.run(scraper.scrape_findly_google(_Session(_body(fixture_text, slug)), system, org))


def test_the_payload_names_utsw_by_its_cut_legal_name(fixture_text):
    raw = _payload_jobs(fixture_text)
    assert len(raw) == 4
    assert {j["company_name"] for j in raw} == {CUT} and len(CUT) == 50
    # nothing below the system: brand is a department code, business_unit empty
    assert all(not j.get("business_unit") for j in raw)


def test_every_utsw_row_carries_the_name_its_pages_print(fixture_text):
    jobs = _scrape(fixture_text)
    assert len(jobs) == 4
    for j in jobs:
        assert (j.hospital_name, j.hospital_system) == (UTSW, UTSW)
        page = fixture_text(f"reqid_1014/findly_google/utsw_job_{j.job_id}.html")
        assert f" job at {UTSW}, Dallas, TX</title>" in page
        assert "Southwestern Medical Cent " not in page and "(The" not in page
    # the faculty row (Interfolio Apply link) too
    assert any(j.url.startswith("https://apply.interfolio.com/") for j in jobs)


def test_normalize_job_keeps_the_name_and_dallas(fixture_text):
    scraper.set_known_bodies([])
    for j in _scrape(fixture_text):
        d = scraper.normalize_job(j)
        assert d["hospital_name"] == UTSW and d["hospital_system"] == UTSW
        assert (d["city"], d["state"]) == ("Dallas", "TX")


def test_the_new_name_changes_the_content_hash(fixture_text):
    # A stored row written with the cut name has a different hash, so the
    # first nightly after the push rewrites it in full with the new name.
    scraper.set_known_bodies([])
    j = _scrape(fixture_text)[0]
    new = scraper.normalize_job(j)
    old = scraper.normalize_job(dataclasses.replace(j, hospital_name=CUT))
    assert old["hospital_name"] == CUT
    assert scraper.content_hash_for(new) != scraper.content_hash_for(old)


def test_only_utsw_is_renamed(fixture_text):
    assert scraper.FINDLY_GOOGLE_NAME == {UTSW: UTSW}
    assert UTSW not in scraper.FINDLY_GOOGLE_FACILITY
    # the same payload under a tenant in neither map keeps company_name
    assert {j.hospital_name for j in _scrape(fixture_text, "Some Other Findly Tenant")} == {CUT}


@pytest.mark.parametrize("system,slug,name", [
    ("University Hospitals", "uh", "University Hospitals (SP)"),
    ("Scripps Health", "scripps", "Scripps Health"),
])
def test_other_tenants_keep_company_name(fixture_text, system, slug, name):
    jobs = _scrape(fixture_text, system, slug)
    assert jobs and {j.hospital_name for j in jobs} == {name}
