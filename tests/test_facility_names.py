"""2026-10-15 (wages, facility): the actual hospital on postings that ten
adapters filed under the system's name (facility_names, Wages-accuracy-audit
root cause 2 / item 12a).

Fixtures (tests/fixtures/facility/) are real samples read 2026-10-15, cut
short (bodies to a few hundred characters, recruiter fields dropped):
  upmc_search.txt        UPMC's Findly search payload (business_unit, brand)
  novant_jobs.json       Novant's Jibe /api/jobs rows (location_name)
  guthrie_page.json      Guthrie's Oracle list page (workLocation)
  tenet_page.json        Tenet's Oracle list page (workLocation)
  bhsf_jobs.json         Baptist Health South Florida's Phenom rows (brand)
  multicare_list.json    MultiCare's Workday list page (bulletFields)
  vumc_details.json      Vanderbilt Workday details (jobRequisitionLocation)
  corewell_details.json  Corewell Workday details (jobRequisitionLocation, body)
  stored_bodies.json     stored Ascension / MGB / Corewell / Vanderbilt bodies
No network, no database."""
import asyncio
import json
import os
import re

import aiohttp
import pytest
from aiohttp import web

import facility_names as F
import scraper
from scraper import Job
from test_list_resilience import _Redirect, _fast, _no_proxies, _serve
from test_wd_tenant_facets import _fake_req

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read() if name.endswith(".txt") else json.load(f)


@pytest.fixture(autouse=True)
def _no_known_facilities():
    F.set_known_facilities([])
    yield
    F.set_known_facilities([])


def _job(system, name, title="RN", city="", state="", description="", job_id="1", **kw):
    return Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="", url="https://x/1",
               job_id=job_id, posted_date="", description=description, ats_platform=kw.get("ats", "Workday"))


# ── The tables ──────────────────────────────────────────────────────────────

def test_every_system_and_its_stored_label():
    assert F.FACILITY_SYSTEMS == {"UPMC", "Novant Health", "Guthrie Health", "Tenet Healthcare", "Baptist Health (FL)",
                                  "MultiCare Health", "Mass General Brigham", "Corewell Health", "Vanderbilt (VUMC)",
                                  "Ascension Health"}
    for emit, label in (("Ascension Health", "Ascension"), ("Guthrie Health", "Guthrie"),
                        ("MultiCare Health", "MultiCare"), ("Vanderbilt (VUMC)", "Vanderbilt"),
                        ("Baptist Health (FL)", "Baptist Health South Florida")):
        assert scraper.HOSPITAL_SYSTEM_ALIASES[emit] == label == F.spec_for(emit).db_label
    assert set(F.FALLBACK_DB_LABELS) == {"Ascension", "Mass General Brigham", "Corewell Health", "Vanderbilt"}


@pytest.mark.parametrize("system", sorted(F.FACILITY_SYSTEMS))
def test_every_proper_name_resolves_to_itself_with_its_town(system):
    spec = F.spec_for(system)
    for name, (town, st) in spec.hospitals.items():
        assert F.resolve(system, name, town, st) == (name, town, st)
        assert F.is_hospital_name(system, name) and F.canonical_name(system, name.upper()) == name
        assert len(st) == 2 and town
        # without a state only a name that is no brand of several hospitals
        assert F.resolve(system, name) in ((name, town, st), None)


# The proper names that need the posting's state or town (their bare form is
# a brand of several hospitals); every other proper name resolves anywhere.
AMBIGUOUS_WITHOUT_STATE = {
    "Ascension St. Francis Hospital", "Ascension St. Joseph Hospital",
    "Ascension Mercy Hospital", "Ascension Franklin Hospital", "Ascension Providence",
}


def test_which_proper_names_need_a_state():
    got = {name for s in sorted(F.FACILITY_SYSTEMS) for name in F.spec_for(s).hospitals
           if F.resolve(s, name) is None}
    assert got == AMBIGUOUS_WITHOUT_STATE


def test_a_hospital_of_one_system_is_not_one_of_another():
    assert F.resolve("UPMC", "Massachusetts General Hospital") is None
    assert F.resolve("Mass General Brigham", "UPMC Presbyterian") is None
    assert F.resolve("Some Other System", "UPMC Presbyterian") is None


def test_text_is_cleaned_before_lookup():
    assert F.clean_text("Columbia St. Mary\u00e2\u0080\u0099s") == "Columbia St. Mary's"
    assert F.fkey("The General Hospital Corporation") == "general hospital"
    assert F.fkey("Saint Thomas  West") == F.fkey("St. Thomas West") == "st thomas west"


# ── UPMC: Findly business_unit, refined by brand ────────────────────────────

UPMC_EXPECT = {
    ("UPMC Presbyterian", "UPMC Presbyterian"): "UPMC Presbyterian",
    ("UPMC Susquehanna Williamsport", "UPMC Susquehanna Soldiers+Sail"): "UPMC Wellsboro",
    ("UPMC Susquehanna Williamsport", "UPMC Susquehanna Williamsport"): "UPMC Williamsport",
    ("UPMC Susquehanna Williamsport", "Muncy Valley Long Term Care"): "",
    ("UPMC Children's Hospital of Pittsburgh", "Children's Community Pediatrics"): "",
    ("UPMC Children's Hospital of Pittsburgh", "UPMC Children's Hospital of Pittsburgh"): "UPMC Children's Hospital of Pittsburgh",
    ("University of Pittsburgh Physicians", "University of Pittsburgh Physicians"): "",
    ("UPMC Passavant", "UPMC Passavant Cranberry"): "UPMC Passavant Cranberry",
    ("UPMC Memorial", "UPMC Pinnacle Memorial"): "UPMC Memorial",
    ("UPMC Western Maryland", "UPMC Western Maryland"): "UPMC Western Maryland",
    ("Hospital", "UPMC Chautauqua at WCA"): "UPMC Chautauqua",
    ("Corporate Finance", "Corporate Revenue Cycle"): "",
    ("UPMC Altoona", "UPMC Altoona Reg Health Srvcs"): "",
    ("UPMC Bedford Memorial", "UPMC Bedford Memorial"): "UPMC Bedford",
}


def _upmc_jobs():
    body = _load("upmc_search.txt")
    return [r["job"] for r in json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))["searchResults"]]


def test_upmc_fixture_covers_every_case():
    assert {(j["business_unit"], j["brand"]) for j in _upmc_jobs()} == set(UPMC_EXPECT)


@pytest.mark.parametrize("bu,brand", list(UPMC_EXPECT))
def test_upmc_facility(bu, brand):
    assert F.upmc_facility(brand, bu) == UPMC_EXPECT[(bu, brand)]


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


def test_scrape_findly_google_stores_the_upmc_hospital():
    raw = {str(j["id"]): (j["business_unit"], j["brand"]) for j in _upmc_jobs()}
    jobs = asyncio.run(scraper.scrape_findly_google(_Session(_load("upmc_search.txt")), "UPMC",
                                                    scraper.FINDLY_GOOGLE_ORGS["UPMC"]))
    assert len(jobs) == len(UPMC_EXPECT)
    for j in jobs:
        # 2026-10-29 (owner decision 2): a row that names no hospital takes the
        # system name, no longer company_name ("UPMC - Pittsburgh Medical Center")
        want = UPMC_EXPECT[raw[j.job_id]] or "UPMC"
        assert j.hospital_name == want and j.hospital_system == "UPMC"
        assert j.req_id                                                          # the 10-14 job number is untouched


def test_upmc_unit_in_another_state_names_no_hospital():
    # Review 2026-10-15, live: "Behavioral Health Navigator I - UPMC Western Maryland" in Cumberland, MD
    # (and "- UPMC Chautauqua" in Jamestown, NY) sit in UPMC Western Psychiatric Hospital's unit.
    wph = "UPMC Western Psychiatric Hospital"
    assert F.upmc_facility(wph, wph) == wph
    assert F.upmc_facility(wph, wph, "Pittsburgh", "PA") == wph
    assert F.upmc_facility(wph, wph, "Cumberland", "MD") == ""
    assert F.upmc_facility(wph, wph, "Jamestown", "NY") == ""
    assert F.upmc_facility("UPMC Western Maryland", "UPMC Western Maryland", "Cumberland", "MD") == "UPMC Western Maryland"
    body = _load("upmc_search.txt")
    payload = json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))
    j = dict(payload["searchResults"][0]["job"], business_unit=wph, brand=wph, primary_city="Cumberland",
             primary_state="MD", title="Behavioral Health Navigator I - UPMC Western Maryland")
    payload["searchResults"] = [dict(payload["searchResults"][0], job=j)]
    payload["totalHits"] = 1
    text = "cb(" + json.dumps(payload) + ");"
    jobs = asyncio.run(scraper.scrape_findly_google(_Session(text), "UPMC", scraper.FINDLY_GOOGLE_ORGS["UPMC"]))
    assert [(x.hospital_name, x.city, x.state) for x in jobs] == [("UPMC", "Cumberland", "MD")]   # 2026-10-29: system name


def test_findly_google_facility_tenants():
    # 2026-10-29: AdventHealth, Scripps, University Hospitals and UCLA Health
    # joined UPMC (facilities_findly.py, tests/test_findly_facility_1029.py)
    assert set(scraper.FINDLY_GOOGLE_FACILITY) == {"UPMC", "AdventHealth", "Scripps Health",
                                                  "University Hospitals", "UCLA Health"}


# ── Novant: Jibe location_name ──────────────────────────────────────────────

NOVANT_EXPECT = {
    "PHO - Presbyterian Medical Center": "Novant Health Presbyterian Medical Center",
    "NHRMC - New Hanover Regional Medical Center": "Novant Health New Hanover Regional Medical Center",
    "SCR-HHH1 - Hilton Head Hospital": "Novant Health Hilton Head Hospital",
    "Hilton Head Hospital": "Novant Health Hilton Head Hospital",
    "STK-LBCH - NH Community Hospital - Stokes": "Novant Health Community Hospital - Stokes",
    "PENDERMEMORIAL - Pender Memorial Hospital": "Novant Health Pender Medical Center",
    "CIG - Cancer Institute": "",
    "SCM - Scotts Hill Medical Center": "",           # a medical office park, not a hospital
    "NC-REMOTE - North Carolina Remote": "",
    "Charlotte Region": "",
}


def test_novant_fixture_covers_every_case():
    assert {r["data"]["location_name"] for r in _load("novant_jobs.json")["jobs"]} == set(NOVANT_EXPECT)


@pytest.mark.parametrize("loc", list(NOVANT_EXPECT))
def test_novant_facility(loc):
    assert F.novant_facility(loc) == NOVANT_EXPECT[loc]


def test_scrape_jibe_stores_the_novant_hospital(monkeypatch):
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    page = _load("novant_jobs.json")

    async def jobs_api(request):
        return web.json_response(page if request.query.get("page") == "1" else {"jobs": [], "totalCount": 0})

    async def go():
        runner, base = await _serve([("GET", "/api/jobs", jobs_api)])
        try:
            async with aiohttp.ClientSession() as s:
                return await scraper.scrape_jibe(_Redirect(s, base), "Novant Health", scraper.JIBE_SITES["Novant Health"])
        finally:
            await runner.cleanup()
    rows = asyncio.run(go())
    loc = {r["data"]["req_id"]: r["data"]["location_name"] for r in page["jobs"]}
    assert len(rows) == len(loc)
    for r in rows:
        assert r.hospital_name == (NOVANT_EXPECT[loc[r.job_id]] or "Novant Health")
        assert r.hospital_system == "Novant Health"


def test_other_jibe_tenants_unchanged():
    row = {"location_name": "Kent Hospital, 455 Toll Gate Rd, Warwick", "tags2": ["Kent Hospital"]}
    assert scraper._jibe_facility("Care New England Health System", row) == "Kent Hospital"
    assert scraper._jibe_facility("Mercy", {"location_name": "Mercy Hospital St. Louis"}) == "Mercy Hospital St. Louis"
    assert scraper._jibe_facility("Yale New Haven Health System", {"location_name": "CT06510"}) == \
        "Yale New Haven Health System"


# ── Guthrie and Tenet: Oracle work location ─────────────────────────────────

ORACLE_EXPECT = {
    "Guthrie Health": {
        "HCM - Robert Packer Hospital": "Guthrie Robert Packer Hospital",
        "HCM - TGC Guthrie Lourdes Hospital": "Guthrie Lourdes Hospital",
        "HCM - Corning Hospital": "Guthrie Corning Hospital",
        "HCM - Robert Packer Hospital at Towanda Campus": "Guthrie Robert Packer Hospital Towanda Campus",
        "HCM - TGC Guthrie Cortland Med Center": "Guthrie Cortland Medical Center",
        "HCM - GMG Cortland Medical Center": "",     # the medical group's office
        "HCM - The Guthrie Clinic": "",
    },
    "Tenet Healthcare": {
        "Rock Hill Hospital, Piedmont Medical Network": "Piedmont Medical Center",
        "St. Marys Medical Center": "St. Mary's Medical Center",
        "Thop Memorial Campus": "The Hospitals of Providence Memorial Campus",
        "Saint Francis Hospital-Bartlett": "Saint Francis Hospital - Bartlett",
        "Hdmc Holdings, LLC DBA Hi-Desert Medical Center": "Hi-Desert Medical Center",
        # DMC's recruiting address: its physician postings are mostly at Children's (review 10-15)
        "3990 John R. Street, Detroit, Mi 48201-2018": "",
        "Metrowest Medical Center - Framingham Union Campus": "Framingham Union Hospital",
        "Oced - Good Samaritan Medical Center": "",   # an off-campus ED
        "BHS Phys Network": "",
        "Healthlink (Fitness Center)": "",
    },
}
ORACLE_FIX = {"Guthrie Health": "guthrie_page.json", "Tenet Healthcare": "tenet_page.json"}


@pytest.mark.parametrize("system", list(ORACLE_EXPECT))
def test_oracle_work_location_facility(system):
    items = _load(ORACLE_FIX[system])["items"][0]["requisitionList"]["items"]
    got = {j["workLocation"]["items"][0]["LocationName"]: scraper._oracle_work_location_facility(system, j) for j in items}
    assert got == ORACLE_EXPECT[system]


@pytest.mark.parametrize("system", list(ORACLE_EXPECT))
def test_scrape_oracle_stores_the_hospital(monkeypatch, system):
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    page = _load(ORACLE_FIX[system])

    async def reqs(request):
        return web.json_response(page)

    async def go():
        runner, base = await _serve([("GET", "/hcmRestApi/resources/latest/recruitingCEJobRequisitions", reqs)])
        try:
            async with aiohttp.ClientSession() as s:
                return await scraper.scrape_oracle(_Redirect(s, base), system, scraper.ORACLE_ORGS[system])
        finally:
            await runner.cleanup()
    rows = asyncio.run(go())
    loc = {j["Id"]: j["workLocation"]["items"][0]["LocationName"] for j in page["items"][0]["requisitionList"]["items"]}
    assert len(rows) == len(loc)
    for r in rows:
        assert r.hospital_name == (ORACLE_EXPECT[system][loc[r.job_id]] or system)
        assert r.hospital_system == system


def test_other_oracle_tenants_keep_the_system():
    j = {"Id": "1", "Title": "RN", "workLocation": {"items": [{"LocationName": "Delray Medical Center"}]}}
    # 2026-10-29: the facilities_oracle_other tenants joined (each with its own table)
    assert "Mayo Clinic" not in scraper.ORACLE_LOCATION_FACILITY
    import facilities_oracle_other
    assert scraper.ORACLE_LOCATION_FACILITY == {"Guthrie Health", "Tenet Healthcare"} | facilities_oracle_other.ORACLE_SYSTEMS
    assert scraper._oracle_work_location_facility("Tenet Healthcare", j) == "Delray Medical Center"


def test_nothing_is_guessed():
    # an unlisted hospital, a free-standing ED, a department, a holding company
    for raw in ("Sierra Vista Regional Medical Center", "San Antonio - FS Hospital",
                "Palm Beach Gardens Medical Center - Cardiac Rehab Dept", "NME Hospitals Inc (NME)"):
        assert F.list_facility("Tenet Healthcare", raw) == ""
    assert F.list_facility("Guthrie Health", "HCM - Some New Hospital") == ""


def test_a_brand_in_a_town_without_its_hospital_names_none():
    r = F.resolve
    assert r("Ascension Health", "Ascension St. Vincent", "Evansville", "IN")[0] == "Ascension St. Vincent Evansville"
    assert r("Ascension Health", "St. Vincent Hospital", "Avon", "IN") is None
    assert r("Ascension Health", "Ascension Via Christi Hospital", "Manhattan", "KS")[0] == \
        "Ascension Via Christi Hospital Manhattan"
    assert r("Ascension Health", "Ascension Via Christi Hospital", "Wichita", "KS") is None   # four hospitals there
    assert r("Ascension Health", "Ascension Saint Thomas", "Nashville", "TN") is None          # Midtown or West
    assert r("Ascension Health", "Ascension Saint Thomas Hospital", "Murfreesboro", "TN")[0] == \
        "Ascension Saint Thomas Rutherford"
    assert r("Ascension Health", "St. Mary's Medical Center", "Evansville", "IN")[0] == "Ascension St. Vincent Evansville"
    assert r("Ascension Health", "St. Francis Hospital", "Milwaukee", "WI")[0] == "Ascension St. Francis Hospital"
    assert r("Ascension Health", "St. Francis Hospital", "Wichita", "KS")[0] == "Ascension Via Christi St. Francis"
    assert r("Ascension Health", "Ascension St. Francis Hospital", "Milwaukee", "WI")[0] == "Ascension St. Francis Hospital"
    # no state: Milwaukee's name is also how a Wichita posting could spell its own
    assert r("Ascension Health", "Ascension St. Francis Hospital", "", "") is None


def test_a_hospital_is_never_taken_from_another_state():
    # Review 2026-10-15: Chicago postings print "Facility: Ascension Saint Joseph Hospital"
    # (1303184, 1316322, 1325997; 2025-425507 with no town), which fkey spells like
    # Milwaukee's "Ascension St. Joseph Hospital".
    r = F.resolve
    chi = ("Ascension Saint Joseph - Chicago", "Chicago", "IL")
    assert r("Ascension Health", "Ascension Saint Joseph Hospital", "Chicago", "IL") == chi
    assert r("Ascension Health", "Saint Joseph Hospital", "Chicago", "IL") == chi
    assert r("Ascension Health", "Saint Joseph Hospital", "Joliet", "IL")[0] == "Ascension Saint Joseph - Joliet"
    assert r("Ascension Health", "Ascension Saint Joseph Hospital", "Joliet", "IL")[0] == "Ascension Saint Joseph - Joliet"
    # Illinois with no town, or a third Illinois town: Chicago or Joliet, so none
    assert r("Ascension Health", "Ascension Saint Joseph Hospital", "", "IL") is None
    assert r("Ascension Health", "St. Joseph Hospital", "Elk Grove Village", "IL") is None
    # no town and no state at all (2025-425507): Milwaukee is not guessed
    assert r("Ascension Health", "Ascension Saint Joseph Hospital", "", "") is None
    # Milwaukee's own postings keep it
    assert r("Ascension Health", "Ascension St. Joseph Hospital", "Milwaukee", "WI") == \
        ("Ascension St. Joseph Hospital", "Milwaukee", "WI")
    assert r("Ascension Health", "St. Joseph Hospital", "Milwaukee", "WI")[0] == "Ascension St. Joseph Hospital"
    # a proper name of one state on a posting in another names none
    assert r("Ascension Health", "Ascension Sacred Heart Bay", "Austin", "TX") is None
    assert r("Ascension Health", "St. Vincent", "Salem", "OR") is None          # Salem, IN is the hospital's
    assert F.list_facility("Tenet Healthcare", "Delray Medical Center", "Dallas", "TX") == "Delray Medical Center"  # head office
    assert F.list_facility("Tenet Healthcare", "Delray Medical Center", "El Paso", "TX") == ""
    assert F.list_facility("Guthrie Health", "HCM - TGC Guthrie Lourdes Hospital", "Binghamton", "NY") == \
        "Guthrie Lourdes Hospital"
    assert F.list_facility("Guthrie Health", "HCM - TGC Guthrie Lourdes Hospital", "Sayre", "PA") == \
        "Guthrie Lourdes Hospital"                                                # head office
    assert F.list_facility("Guthrie Health", "HCM - TGC Guthrie Lourdes Hospital", "Towanda", "PA") == ""
    # Vanderbilt's unit codes: Tennessee only
    assert F.text_facility("Vanderbilt (VUMC)", "", "Registered Nurse, ED - VUH", "Bowling Green", "KY") is None
    assert F.text_facility("Vanderbilt (VUMC)", "", "Registered Nurse, ED - VUH", "Nashville", "TN")[0] == \
        "Vanderbilt University Medical Center"


def test_stored_chicago_rows_through_apply_facility():
    body = "Location: Chicago, IL\n\nFacility: Ascension Saint Joseph Hospital\n\n" + "Provides care. " * 80
    job = _job("Ascension Health", "Ascension Health", city="Chicago", state="IL", description=body, job_id="1325997")
    assert F.apply_facility(job) == "text"
    assert (job.hospital_name, job.city, job.state) == ("Ascension Saint Joseph - Chicago", "Chicago", "IL")
    job = _job("Ascension Health", "Ascension Health", description=body, job_id="2025-425507")
    assert F.apply_facility(job) == "none"
    assert (job.hospital_name, job.city, job.state, job.location) == ("Ascension Health", "", "", "")


def test_a_proper_name_with_hospital_added():
    r = F.resolve
    assert r("Ascension Health", "Ascension Saint Thomas Stones River Hospital")[0] == "Ascension Saint Thomas Stones River"
    assert r("Ascension Health", "Ascension Sacred Heart Gulf Hospital")[0] == "Ascension Sacred Heart Gulf"
    assert r("Ascension Health", "Ascension Providence Hospital", "Waco", "TX")[0] == "Ascension Providence"


# ── Baptist Health South Florida: Phenom brand ──────────────────────────────

def _bhsf_expect(doc):
    brand, title = doc["brand"], doc["title"]
    if brand == "Bethesda Hospital, Inc.":
        return "Bethesda Hospital West" if "Bethesda West" in title else "Bethesda Hospital East"
    return {"Boca Raton Regional Hospital": "Boca Raton Regional Hospital",
            "Baptist Hospital of Miami": "Baptist Hospital of Miami",
            "Fishermen's Community Hospital": "Fishermen's Community Hospital"}.get(brand, "")


def test_bhsf_brand():
    docs = _load("bhsf_jobs.json")
    assert {d["brand"] for d in docs} >= {"Bethesda Hospital, Inc.", "Corporate", "Baptist Health Medical Group"}
    assert any("Bethesda West" in d["title"] for d in docs) and any("Bethesda East" in d["title"] for d in docs)
    for d in docs:
        got = scraper._phenom_list_facility("Baptist Health (FL)", d, d["city"], "FL", d["title"])
        assert got == _bhsf_expect(d), d["title"]
        assert scraper._phenom_list_facility("Corewell Health", d, d["city"], "FL", d["title"]) == ""


# ── MultiCare: Workday bulletFields ─────────────────────────────────────────

MULTICARE_EXPECT = {
    "Covington Medical Center": "MultiCare Covington Medical Center",
    "Yakima Memorial Hospital": "MultiCare Yakima Memorial Hospital",
    "Mary Bridge Children's Hospital & Health Network": "MultiCare Mary Bridge Children's Hospital",
    "Good Samaritan Hospital": "MultiCare Good Samaritan Hospital",
    "Valley Hospital": "MultiCare Valley Hospital",
    "MultiCare Health System": "",
    "Indigo Urgent Care": "",
}


def test_scrape_workday_stores_the_multicare_entity(monkeypatch):
    page = _load("multicare_list.json")
    _fake_req(monkeypatch, lambda b: page if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(page["jobPostings"]), "jobPostings": []})
    jobs = asyncio.run(scraper.scrape_workday(None, "MultiCare Health", scraper.WORKDAY_TENANTS["MultiCare Health"]))
    entity = {p["bulletFields"][1]: p["bulletFields"][2] for p in page["jobPostings"]}
    assert set(entity.values()) == set(MULTICARE_EXPECT)
    assert len(jobs) == len(entity)
    for j in jobs:
        assert j.hospital_name == (MULTICARE_EXPECT[entity[j.job_id]] or "MultiCare Health")


# ── Vanderbilt and Corewell: Workday jobRequisitionLocation ─────────────────

VUMC_EXPECT = {
    "VANDERBILT BEDFORD HOSPITAL (LOC02075)": "Vanderbilt Bedford Hospital",
    "VANDERBILT WILSON COUNTY HOSPITAL (LOC02029)": "Vanderbilt Wilson County Hospital",
    "VANDERBILT HOSPITAL (LOC00133)": "Vanderbilt University Medical Center",
    "MONROE CARELL JR CHILDREN'S HOSPITAL (LOC00245)": "Monroe Carell Jr. Children's Hospital at Vanderbilt",
    "VANDERBILT HOSPITAL CLARKSVILLE (LOC00604)": "Vanderbilt Clarksville Hospital",
    "VANDERBILT TULLAHOMA-HARTON HOSPITAL (LOC02068)": "Vanderbilt Tullahoma-Harton Hospital",
    "VANDERBILT INGRAM CANCER CENTER CLARKSVILLE (LOC09001)": "",
    "ONE HUNDRED OAKS (LOC00400)": "",
    "MED CTR EAST SOUTH TOWER (LOC00255)": "",
    "PRESTON RESEARCH BLDG (LOC00215)": "",
    "VCCOC (LOC00574)": "",
}
COREWELL_EXPECT = {
    "SITE - Royal Oak Hospital - 3601 W 13 Mile Rd - Royal Oak": "Corewell Health William Beaumont University Hospital",
    "SITE - Blodgett Hospital - 1840 Wealthy St SE- Grand Rapids": "Corewell Health Blodgett Hospital",
    "SITE - Lakeland Hospital St Joseph - 1234 Napier Ave - St Joseph": "Corewell Health Lakeland Hospital, St. Joseph",
    "SITE - Troy Hospital - 44201 Dequindre Rd - Troy": "Corewell Health Beaumont Troy Hospital",
    "SITE - Greenville Hospital - 615 S Bower - Greenville": "Corewell Health Greenville Hospital",
    "SITE - Medical Office Building West/East - 3535 W 13 Mile Rd - Royal Oak": "",
    "SITE - 9340 Telegraph Rd - Taylor": "",
    "SITE - Hospice & Palliative Care Royalton - 4025 Health Park Ln - St Joseph": "",
}


@pytest.mark.parametrize("system,fixture,expect,label", [
    ("Vanderbilt (VUMC)", "vumc_details.json", VUMC_EXPECT, "Vanderbilt (VUMC)"),
    ("Corewell Health", "corewell_details.json", COREWELL_EXPECT, "Corewell Health"),
])
def test_workday_detail_location_names_the_hospital(system, fixture, expect, label):
    seen_desc = set()
    for d in _load(fixture):
        info = d["jobPostingInfo"]
        desc = info["jobRequisitionLocation"]["descriptor"]
        if desc not in expect:
            continue
        seen_desc.add(desc)
        job = _job(system, label, title=info["title"])
        scraper._apply_wd_posting_info(job, info)
        assert job.hospital_name == (expect[desc] or label), desc
        assert getattr(job, F.SEEN_ATTR, False) is (not expect[desc])
    assert seen_desc == set(expect)


@pytest.mark.parametrize("phenom_name", [
    "Corewell Health Helen DeVos Children’s Hospital",     # a hospital the requisition puts at Butterworth
    "Corewell Health East Medical Group",                    # a group the requisition puts at Beaumont University
    "Corewell Health Children’s",
])
def test_a_list_facility_is_not_overridden_by_the_detail(phenom_name):
    # Review 2026-10-15: about 8 Corewell rows alternated night to night between the Phenom
    # list's facility (read every night) and the requisition location (read on detail nights).
    # The list's own facility now holds on every night; the detail fills only label rows.
    info = {"jobRequisitionLocation": {"descriptor": "SITE - Butterworth Hospital - 100 Michigan St NE - Grand Rapids"}}
    detail_night = _job("Corewell Health", phenom_name, city="Grand Rapids", state="MI", job_id="R1", ats="Phenom")
    scraper._apply_wd_posting_info(detail_night, info)
    assert detail_night.hospital_name == phenom_name and not getattr(detail_night, F.SEEN_ATTR, False)
    other_night = _job("Corewell Health", phenom_name, city="Grand Rapids", state="MI", job_id="R1", ats="Phenom")
    F.apply_facility(detail_night)
    F.apply_facility(other_night)
    assert detail_night.hospital_name == other_night.hospital_name
    # a label row still takes the requisition's hospital
    label_row = _job("Corewell Health", "Corewell Health", city="Grand Rapids", state="MI", job_id="R2", ats="Phenom")
    scraper._apply_wd_posting_info(label_row, info)
    assert label_row.hospital_name == "Corewell Health Butterworth Hospital"


def test_detail_location_is_read_only_for_the_configured_tenants():
    info = {"jobRequisitionLocation": {"descriptor": "VANDERBILT HOSPITAL (LOC00133)"}, "jobDescription": ""}
    job = _job("Some Workday Tenant", "Some Workday Tenant")
    scraper._apply_wd_posting_info(job, info)
    assert job.hospital_name == "Some Workday Tenant" and not getattr(job, F.SEEN_ATTR, False)


def test_vanderbilt_title_and_unit_codes():
    t = F.text_facility
    assert t("Vanderbilt (VUMC)", "", "Registered Nurse, NICU, Night Shift, TempForce - VCH")[0] == \
        "Vanderbilt Clarksville Hospital"
    assert t("Vanderbilt (VUMC)", "", "Registered Nurse, EP Lab, Day Shift - VUH")[0] == \
        "Vanderbilt University Medical Center"
    assert t("Vanderbilt (VUMC)", "", "PRN Nurse Practitioner/Physician Assistant - VTHH/VBCH")[0] == \
        "Vanderbilt Tullahoma-Harton Hospital"
    assert t("Vanderbilt (VUMC)", "", "Psych RN, Dual Diagnosis, Evenings -VPH")[0] == "Vanderbilt Psychiatric Hospital"
    assert t("Vanderbilt (VUMC)", "", "Behavioral Health Specialist - VBH") is None   # VBH is also Behavioral Health
    assert t("Vanderbilt (VUMC)", "", "Medical Assistant - Spine Clinic") is None


# ── Bodies: Ascension "Facility:", MGB "Site:", Corewell SITE, VUMC Organization ──

BODY_EXPECT = {
    "1321457": ("Ascension Sacred Heart Bay", "Panama City", "FL"),
    "1266905": ("Ascension St. Vincent Evansville", "Evansville", "IN"),     # brand + the posting's town
    "1320365": None,                                                         # "... Evansville Urology": a clinic
    "1278388": ("Ascension Saint Agnes Hospital", "Baltimore", "MD"),         # "Facility Name:"
    "1268194": ("Ascension Columbia St. Mary's Hospital Milwaukee", "Milwaukee", "WI"),   # mojibake, brand + town
    "1261358": ("Ascension Via Christi St. Francis", "Wichita", "KS"),
    "1291130": None,                                                         # "Multiple Ascension sites"
    "1280409": None,                                                         # five hospitals in one line
    "10919": None,                                                           # "Ascension Medical Group"
    "1252495": None,                                                         # no Facility line
    "2026-465182": None,                                                     # Henry Ford's, not listed: no guess
    "14664": None,                                                           # brand, remote: no town
    "RQ3298059": ("Massachusetts General Hospital", "Boston", "MA"),
    "RQ4042518": ("Salem Hospital", "Salem", "MA"),
    "RQ4008046": ("Spaulding Hospital Cambridge", "Cambridge", "MA"),
    "RQ4042851": None,                                                       # Spaulding corporation, Brighton
    "RQ4026771": None,                                                       # Community Physicians
    "RQ4055639": ("Wentworth-Douglass Hospital", "Dover", "NH"),
    "R151254": ("Corewell Health Dearborn Hospital", "Dearborn", "MI"),
    "R162905": ("Corewell Health Helen DeVos Children's Hospital", "Grand Rapids", "MI"),
    "R183768": ("Corewell Health Farmington Hills Hospital", "Farmington Hills", "MI"),
    "R214775": None,                                                         # Corewell Health Place (offices)
    "R127804": ("Corewell Health Big Rapids Hospital", "Big Rapids", "MI"),
    "R-58502": ("Vanderbilt Clarksville Hospital", "Clarksville", "TN"),     # "Organization: VCH ..."
    "R-55323": ("Monroe Carell Jr. Children's Hospital at Vanderbilt", "Nashville", "TN"),
    "R-62833": None,
}
BODY_SYSTEM = {"asc": "Ascension Health", "mgb": "Mass General Brigham", "cw": "Corewell Health", "vu": "Vanderbilt (VUMC)"}


def _stored():
    return _load("stored_bodies.json")


def test_stored_body_fixture_covers_every_case():
    assert {r["job_id"] for r in _stored()} == set(BODY_EXPECT)


@pytest.mark.parametrize("row", _load("stored_bodies.json"), ids=lambda r: r["job_id"])
def test_body_names_the_hospital(row):
    system = BODY_SYSTEM[row["k"]]
    got = F.text_facility(system, row["d"], row["title"], row["city"], row["state"])
    assert got == BODY_EXPECT[row["job_id"]]


@pytest.mark.parametrize("row", _load("stored_bodies.json"), ids=lambda r: r["job_id"])
def test_apply_facility_on_a_fetched_body(row):
    system = BODY_SYSTEM[row["k"]]
    label = F.spec_for(system).system
    job = _job(system, label, title=row["title"], city=row["city"], state=row["state"],
               description=row["d"] + "\n" + "x" * 1000, job_id=row["job_id"])
    how = F.apply_facility(job)
    want = BODY_EXPECT[row["job_id"]]
    assert job.hospital_system == system
    if want:
        assert how == "text" and job.hospital_name == want[0]
        assert (job.city, job.state) == (row["city"], row["state"])   # the posting's own town stays
    else:
        assert how == "none" and job.hospital_name == label


# ── The stored hospital on nights without a body ────────────────────────────

def _known(rows):
    return F.set_known_facilities([{"hospital_system": s, "job_id": j, "hospital_name": n} for s, j, n in rows])


def test_stored_hospital_is_kept_when_tonight_has_no_body():
    assert _known([("Ascension", "1321457", "Ascension Sacred Heart Bay"),
                   ("Mass General Brigham", "RQ1", "Salem Hospital"),
                   ("Ascension", "9", "Ascension Health"),                 # the label: not a hospital
                   ("Ascension", "10", "Some Clinic"),                     # not one of Ascension's hospitals
                   ("UPMC", "11", "UPMC Presbyterian")]) == 2               # a list system: never kept
    job = _job("Ascension Health", "Ascension Health", city="Panama City", state="FL", job_id="1321457")
    assert F.apply_facility(job) == "kept" and job.hospital_name == "Ascension Sacred Heart Bay"
    job = _job("Mass General Brigham", "Mass General Brigham", city="Salem", state="MA", job_id="RQ1")
    assert F.apply_facility(job) == "kept" and job.hospital_name == "Salem Hospital"
    job = _job("Ascension Health", "Ascension Health", job_id="10")
    assert F.apply_facility(job) == "none" and job.hospital_name == "Ascension Health"


def test_stored_hospital_is_not_kept_over_tonights_own_answer():
    _known([("Ascension", "1", "Ascension Sacred Heart Bay"), ("Vanderbilt", "2", "Vanderbilt Wilson County Hospital")])
    # tonight's body names no hospital
    job = _job("Ascension Health", "Ascension Health", description="Facility: Multiple Ascension sites\n" + "x" * 1200,
               job_id="1")
    assert F.apply_facility(job) == "none" and job.hospital_name == "Ascension Health"
    # tonight's body names another hospital
    job = _job("Ascension Health", "Ascension Health", description="Facility: Ascension Sacred Heart Gulf\n" + "x" * 1200,
               job_id="1")
    assert F.apply_facility(job) == "text" and job.hospital_name == "Ascension Sacred Heart Gulf"
    # tonight's detail said "an office building"
    job = _job("Vanderbilt (VUMC)", "Vanderbilt (VUMC)", job_id="2")
    scraper._apply_wd_posting_info(job, {"jobRequisitionLocation": {"descriptor": "ONE HUNDRED OAKS (LOC00400)"}})
    assert F.apply_facility(job) == "none" and job.hospital_name == "Vanderbilt (VUMC)"


def test_load_known_facilities_without_credentials_is_empty(monkeypatch):
    _known([("Ascension", "1", "Ascension Sacred Heart Bay")])
    for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert F.load_known_facilities() == 0
    assert F.known_facility("Ascension Health", "1") == ""


class _Resp:
    def __init__(self, body):
        self.body = body

    def read(self):
        return json.dumps(self.body).encode()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_rest(monkeypatch, by_system, fail=()):
    """A PostgREST stand-in: rows per system, keyset-paged on id."""
    import urllib.parse
    import urllib.request
    urls = []

    def fake_urlopen(rq, timeout=None):
        urls.append(rq.full_url)
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(rq.full_url).query)
        label = q["hospital_system"][0].split(".", 1)[1]
        if label in fail:
            raise OSError("statement timeout")
        last, lim = int(q["id"][0].split(".", 1)[1]), int(q["limit"][0])
        rows = sorted((r for r in by_system.get(label, []) if r["id"] > last), key=lambda r: r["id"])
        return _Resp(rows[:lim])

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(F.time, "sleep", lambda s: None)
    monkeypatch.setattr(F, "KNOWN_FACILITY_PAGE", 2)
    monkeypatch.setenv("SUPABASE_URL", "https://db.example")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    return urls


def _row(i, system, jid, name):
    return {"id": i, "hospital_system": system, "job_id": jid, "hospital_name": name}


def test_load_known_facilities_pages_each_system_on_its_own(monkeypatch):
    # Review 2026-10-15: one pass per system (hospital_system=eq.), not one in.() pass walked in
    # primary-key order (10 s cold, over PostgREST's 8 s timeout).
    urls = _fake_rest(monkeypatch, {
        "Ascension": [_row(1, "Ascension", "a", "Ascension Sacred Heart Bay"),
                      _row(5, "Ascension", "a2", "Ascension Health"),
                      _row(9, "Ascension", "a3", "Ascension St. Joseph Hospital")],
        "Corewell Health": [_row(2, "Corewell Health", "b", "Corewell Health")],
        "Vanderbilt": [_row(3, "Vanderbilt", "c", "Vanderbilt Bedford Hospital")],
        "Mass General Brigham": [_row(4, "Mass General Brigham", "d", "Salem Hospital")],
    })
    assert F.load_known_facilities() == 4
    assert F.known_facility("Vanderbilt (VUMC)", "c") == "Vanderbilt Bedford Hospital"
    assert F.known_facility("Ascension Health", "a3") == "Ascension St. Joseph Hospital"   # a proper name, any state
    assert F.known_facility("Ascension Health", "a2") == ""
    assert all("hospital_system=eq." in u and "in.(" not in u and "order=id.asc" in u for u in urls)
    asc = [u for u in urls if "eq.Ascension&" in u]
    assert ["id=gt.0" in asc[0], "id=gt.5" in asc[1], len(asc)] == [True, True, 2]
    assert any("eq.Mass%20General%20Brigham&" in u for u in urls)


def test_load_known_facilities_keeps_the_systems_that_loaded(monkeypatch):
    _fake_rest(monkeypatch, {"Ascension": [_row(1, "Ascension", "a", "Ascension Sacred Heart Bay")],
                             "Vanderbilt": [_row(3, "Vanderbilt", "c", "Vanderbilt Bedford Hospital")]},
               fail=("Vanderbilt",))
    assert F.load_known_facilities() == 1
    assert F.known_facility("Ascension Health", "a") == "Ascension Sacred Heart Bay"
    assert F.known_facility("Vanderbilt (VUMC)", "c") == ""


def test_stored_hospital_in_another_state_is_not_kept():
    _known([("Ascension", "1", "Ascension St. Joseph Hospital"), ("Ascension", "2", "Ascension St. Joseph Hospital")])
    job = _job("Ascension Health", "Ascension Health", city="Chicago", state="IL", job_id="1")
    assert F.apply_facility(job) == "none" and job.hospital_name == "Ascension Health"
    job = _job("Ascension Health", "Ascension Health", city="Milwaukee", state="WI", job_id="2")
    assert F.apply_facility(job) == "kept" and job.hospital_name == "Ascension St. Joseph Hospital"


def test_load_known_facilities_pages_the_stored_rows(monkeypatch):
    pages = [[{"id": 1, "hospital_system": "Ascension", "job_id": "a", "hospital_name": "Ascension Sacred Heart Bay"},
              {"id": 2, "hospital_system": "Ascension", "job_id": "b", "hospital_name": "Ascension Health"}],
             [{"id": 3, "hospital_system": "Ascension", "job_id": "c", "hospital_name": "Ascension Saint Agnes Hospital"}]]
    urls = []

    def fake_urlopen(rq, timeout=None):
        urls.append(rq.full_url)
        if "eq.Ascension&" not in rq.full_url:
            return _Resp([])
        return _Resp(pages.pop(0) if pages else [])

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(F, "KNOWN_FACILITY_PAGE", 2)
    monkeypatch.setenv("SUPABASE_URL", "https://db.example")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    assert F.load_known_facilities() == 2
    assert F.known_facility("Ascension Health", "c") == "Ascension Saint Agnes Hospital"
    asc = [u for u in urls if "eq.Ascension&" in u]
    assert "id=gt.0" in asc[0] and "id=gt.2" in asc[1] and len(asc) == 2
    assert len(urls) == 2 + 3                       # the other three systems: one empty page each


# ── Towns: blank or head office only ────────────────────────────────────────

def test_head_office_and_blank_towns_take_the_hospitals():
    job = _job("UPMC", "UPMC Altoona", city="Pittsburgh", state="PA")
    assert F.apply_facility(job) == "list" and (job.city, job.state, job.location) == ("Altoona", "PA", "Altoona, PA")
    job = _job("Tenet Healthcare", "Delray Medical Center")
    F.apply_facility(job)
    assert (job.city, job.state) == ("Delray Beach", "FL")
    job = _job("MultiCare Health", "MultiCare Yakima Memorial Hospital", city="Tacoma", state="WA")
    F.apply_facility(job)
    assert job.city == "Yakima"
    # a posting with its own town keeps it
    job = _job("UPMC", "UPMC Altoona", city="Hollidaysburg", state="PA")
    F.apply_facility(job)
    assert (job.city, job.state) == ("Hollidaysburg", "PA")
    # a blank town in another state is not filled from a hospital elsewhere
    job = _job("Tenet Healthcare", "Delray Medical Center", state="TX")
    F.apply_facility(job)
    assert (job.city, job.state) == ("", "TX")


def test_names_the_adapters_already_give_are_canonicalised_or_left():
    job = _job("Corewell Health", "Corewell Health Helen DeVos Children\u2019s Hospital", city="Grand Rapids", state="MI")
    assert F.apply_facility(job) == "list"
    assert job.hospital_name == "Corewell Health Helen DeVos Children's Hospital"
    job = _job("Corewell Health", "Corewell Health West Medical Group", city="Grand Rapids", state="MI")
    assert F.apply_facility(job) == "other" and job.hospital_name == "Corewell Health West Medical Group"


def test_other_systems_are_never_touched():
    job = _job("Advocate Health", "Advocate Health", description="Facility: Ascension Sacred Heart Bay\n" + "x" * 1200)
    assert F.apply_facility(job) == "" and job.hospital_name == "Advocate Health"


# ── Through finalize_jobs (the upsert payload) ──────────────────────────────

def test_finalize_jobs_payload(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    _known([("Mass General Brigham", "RQ9", "Brigham and Women's Hospital")])
    rows = scraper.finalize_jobs([
        _job("Ascension Health", "Ascension Health", title="RN ICU", city="Panama City", state="FL",
             description="Your future role at a glance\n\nLocation: Panama City, FL\n\nFacility: Sacred Heart Bay Hospital\n\n"
                         + "Provides care. " * 80, job_id="A1", ats="Phenom"),
        _job("Mass General Brigham", "Mass General Brigham", title="RN", city="Boston", state="MA", job_id="RQ9"),
        _job("Novant Health", "Novant Health Rowan Medical Center", title="RN", city="Winston-Salem", state="NC",
             job_id="N1", ats="iCIMS"),
        _job("UPMC", "UPMC - Pittsburgh Medical Center", title="Analyst", city="Pittsburgh", state="PA", job_id="U1",
             ats="Findly-Google"),
    ])
    by = {r["job_id"]: r for r in rows}
    assert by["A1"]["hospital_name"] == "Ascension Sacred Heart Bay" and by["A1"]["hospital_system"] == "Ascension Health"
    assert by["RQ9"]["hospital_name"] == "Brigham and Women's Hospital"
    assert (by["N1"]["hospital_name"], by["N1"]["city"], by["N1"]["location"]) == \
        ("Novant Health Rowan Medical Center", "Salisbury", "Salisbury, NC")
    assert by["U1"]["hospital_name"] == "UPMC - Pittsburgh Medical Center" and by["U1"]["city"] == "Pittsburgh"
