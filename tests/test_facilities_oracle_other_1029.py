"""2026-10-29 (facility scale): hospitals of the Oracle HCM, feed-code and
own-site systems (facilities_oracle_other).

Fixtures (tests/fixtures/facility_oracle_other_1029/), real, read 2026-10-29:
  oracle_list_items.json      one list requisition per tenant, as
                              recruitingCEJobRequisitions served it (work
                              location fields kept, the rest dropped)
  oracle_work_locations.json  each tenant's workLocationsFacet: every work
                              location spelling the board uses, with counts
  stored_rows.json            stored hospital_jobs values: Jackson / NYU /
                              CHS feed names with their titles and towns, and
                              IU Health / TriHealth labelled body lines (which
                              this family does not read)
No network, no database."""
import asyncio
import json
import os
import re

import aiohttp
import pytest
from aiohttp import web

import facilities_oracle_other as M
import facility_names as F
import scraper
from scraper import Job
from test_list_resilience import _Redirect, _fast, _no_proxies, _serve

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_oracle_other_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def _no_known_facilities():
    F.set_known_facilities([])
    yield
    F.set_known_facilities([])


def _job(system, name, title="RN", city="", state="", description="", job_id="1"):
    return Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="", url="https://x/1",
               job_id=job_id, posted_date="", description=description, ats_platform="Oracle HCM")


# ── The tables ──────────────────────────────────────────────────────────────

def test_every_table_is_whole():
    for system, t in M.SYSTEMS.items():
        for name, (town, st, src) in t["hospitals"].items():
            assert name.strip() == name and "—" not in name and "–" not in name, (system, name)
            assert town and re.fullmatch(r"[A-Z]{2}", st), (system, name)
            assert re.match(r"(cms \d{5,6}|address \S|board: )", src), (system, name, src)
        for raw, target in t["sites"].items():
            for name in (target.values() if isinstance(target, dict) else (target,)):
                assert name in t["hospitals"], (system, raw, name)
        for raw, (name, town, st, src) in M.entries(system).items():
            assert name, (system, raw)


def test_specs_are_registered_without_changing_the_ten():
    assert len(F.FACILITY_SYSTEMS) == 10 and len(F.FALLBACK_DB_LABELS) == 4
    for system, t in M.SYSTEMS.items():
        if not t["hospitals"]:
            continue
        for name in (system, t["db"], *t["emit"]):
            assert F.spec_for(name) is F.spec_for(system), name
    # list-value Specs only: no body is read, nothing stored is kept
    assert all(not spec.fallback for spec in F.registered())
    assert len(F.registered()) == sum(1 for t in M.SYSTEMS.values() if t["hospitals"])


def test_oracle_systems_join_the_location_hook():
    assert {"Guthrie Health", "Tenet Healthcare"} <= scraper.ORACLE_LOCATION_FACILITY
    assert M.ORACLE_SYSTEMS <= scraper.ORACLE_LOCATION_FACILITY
    assert {"Northwell Health (CX_1)", "Northwell Health (CX_3)"} <= M.ORACLE_SYSTEMS
    for s in M.ORACLE_SYSTEMS:
        assert s in scraper.ORACLE_ORGS, s
    # not every Oracle tenant: those without a table keep the system
    assert "Mayo Clinic" not in scraper.ORACLE_LOCATION_FACILITY
    assert "Encompass Health" not in scraper.ORACLE_LOCATION_FACILITY


# ── Oracle work locations, the boards' own spellings ────────────────────────
WL_EXPECT = {
    "Baptist Memorial Health Care": {
        "BMH - Memphis Hospital": "Baptist Memorial Hospital-Memphis",
        "MBMC - Hospital": "Mississippi Baptist Medical Center",
        "BMH - NEA Hospital": "NEA Baptist Memorial Hospital",
        "BMH - Desoto Hospital": "Baptist Memorial Hospital-DeSoto",
        "BAMC-Baptist Anderson Regional Medical Center North": "Baptist Anderson Regional Medical Center",
        "BMHCC Corporate Office": "",
    },
    "Adventist Health": {
        "AHPL - Hospital": "Adventist Health Portland",
        "AHTC - Hospital": "Adventist Health Twin Cities",
        "AHSV - Hospital": "Adventist Health Sierra Vista",
        "AHRS - ONE Adventist Health Way": "",      # the Roseville head office
    },
    "Providence Health": {
        "Sacred Heart Medical Center-Spokane(HR)": "Providence Sacred Heart Medical Center",
        "Swedish First Hill 747 Broadway-Seattle(HR)": "Swedish First Hill",
        "Everett Medical Ctr Colby-Everett(HR)": "Providence Regional Medical Center Everett",
        "Covenant Medical Center(HR)": "Covenant Medical Center",
    },
    "Indiana University Health": {
        "Methodist Hospital": "IU Health Methodist Hospital",
        "Riley Hospital": "Riley Hospital for Children at IU Health",
        "Fort Wayne Hospital": "",                   # not on the CMS list yet: not guessed
        "Gateway Plaza": "",
    },
    "WellSpan Health": {
        "1001 S George St-York Hospital-EMP": "WellSpan York Hospital",
        "283 Butler Rd-Philhaven Mt Gretna-EMP": "WellSpan Philhaven",
        "25 Monument Rd-Apple Hill Medical Center-EMP": "",
    },
    "Franciscan Missionaries of Our Lady Health System": {
        "HR_SDH Main Campus": "St. Dominic Hospital",
        "HR_Lake Childrens Hospital Campus": "Our Lady of the Lake Children's Hospital",
        "HR_FMOLHS Corporate Shared Svcs": "",
    },
    "Atlantic Health System": {
        "100 Madison Ave": "Morristown Medical Center",
        "99 Beauvoir Ave": "Overlook Medical Center",
        "475 South Street": "",                      # the Morristown offices
        "Atlantic Medical Group": "",
    },
    "Lifepoint Health": {
        "UP Health System - Marquette General Hospital": "UP Health System-Marquette",
        "Haywood Regional Medical Center": "Haywood Regional Medical Center",
    },
    "UW Health": {
        "Building: 600 HIGHLAND AVE": "UW Health University Hospital",
        "600 Highland Ave": "UW Health University Hospital",
        "1675 HIGHLAND AVE 7325(AFCH7325-01)": "UW Health American Family Children's Hospital",
        "4602 EASTPARK BLVD 0220(EMH1513-01)": "UW Health East Madison Hospital",
        "Building: 1401 E STATE ST": "UW Health SwedishAmerican Hospital",
        "Building: 4621 EASTPARK BLVD": "",          # the clinic building across the road
        "Building: 707 S University Ave": "",
    },
    "Loma Linda University Health": {
        "East Campus/East Campus Hosp/1flr": "Loma Linda University Medical Center East Campus",
        "Loma Linda University Medical Center - Murrieta/2nd Floor": "Loma Linda University Medical Center - Murrieta",
        "Dennis and Carol Troesh Medical Campus/Tower Base/3rd Floor": "Loma Linda University Medical Center",
        "LLUH 101": "",
    },
    "UCSF Health": {
        "Saint Francis Memorial Hosp - FL   - RM": "Saint Francis Memorial Hospital",
        "BENIOFF CHILDREN'S HOSPITAL OAKLAND": "UCSF Benioff Children's Hospital Oakland",
        "Location Required - FL 1 - RM": "",
    },
    "Northwell Health": {
        "North Shore University Hospitl": "North Shore University Hospital",
        "Southside Hospital": "South Shore University Hospital",
        "1111 Marcus Ave": "",
    },
    "McLeod Health": {
        "Clarendon": "McLeod Health Clarendon",
        "McLeod Pavilion": "",                       # a building: not read as the hospital
    },
    "Texas Children's": {
        "West Tower": "Texas Children's Hospital",
        "Mark Wallace Tower": "",
        "North Austin Campus": "Texas Children's Hospital North Austin",
    },
}


@pytest.mark.parametrize("system", sorted(WL_EXPECT))
def test_work_location_spellings(system):
    wl = {n for n, _ in _load("oracle_work_locations.json")[system]}
    for raw, want in WL_EXPECT[system].items():
        if not raw.startswith(("600 Highland", "1675 HIGHLAND", "4602 EASTPARK")):
            assert raw in wl, (system, raw)      # each is one of the board's own spellings
        assert F.list_facility(system, M.oracle_location(system, raw)) == want, (system, raw)


def test_every_named_spelling_is_a_hospital_of_its_own_table():
    for system, names in _load("oracle_work_locations.json").items():
        for raw, _n in names:
            got = F.list_facility(system, M.oracle_location(system, raw))
            assert got == "" or got in M.SYSTEMS[system]["hospitals"], (system, raw, got)


def test_adventist_codes_the_table_does_not_list_go_by_the_posting_town():
    j = {"workLocation": {"items": [{"LocationName": "AHSO - Hospital", "TownOrCity": "Sonora", "Region2": "CA"}]}}
    assert scraper._oracle_work_location_facility("Adventist Health", j, "Sonora", "CA") == "Adventist Health Sonora"
    # a town with no Adventist hospital, or a clinic of a hospital's code: none
    assert scraper._oracle_work_location_facility("Adventist Health", j, "Fresno", "CA") == ""
    j2 = {"workLocation": {"items": [{"LocationName": "AHCS - Kailua Professional Center I"}]}}
    assert scraper._oracle_work_location_facility("Adventist Health", j2, "Kailua", "HI") == ""


def test_a_hospital_in_another_state_is_never_taken():
    # Covenant Medical Center is Lubbock's; a posting in Washington cannot be it
    assert F.list_facility("Providence Health", "Covenant Medical Center(HR)", "Spokane", "WA") == ""
    assert F.list_facility("Providence Health", "Covenant Medical Center(HR)", "Lubbock", "TX") == "Covenant Medical Center"


# ── The real list rows through scrape_oracle's own helpers ─────────────────
LIST_EXPECT = {
    "Adventist Health": ("Adventist Health Portland", "Portland", "OR"),
    "AtlantiCare": ("AtlantiCare Regional Medical Center, Mainland Campus", "Galloway", "NJ"),
    "Atlantic Health System": ("", "Freehold", "NJ"),
    "Baptist Health Care (Pensacola)": ("Baptist Hospital", "Pensacola", "FL"),
    "Baptist Memorial Health Care": ("Baptist Memorial Hospital-Memphis", "Memphis", "TN"),
    "Centra Health": ("Centra Lynchburg General Hospital", "Lynchburg", "VA"),
    "Indiana University Health": ("IU Health West Hospital", "Avon", "IN"),
    "Inova Health System": ("Inova Fairfax Hospital", "Falls Church", "VA"),
    "Inspira Health Network": ("Inspira Medical Center Mullica Hill", "Mullica Hill", "NJ"),
    "Lifepoint Health": ("", "Fort Worth", "TX"),
    "Loma Linda University Health": ("Loma Linda University Medical Center - Murrieta", "Murrieta", "CA"),
    "McLeod Health": ("McLeod Health Dillon", "Dillon", "SC"),
    # listed at Vacaville; the work location is the Fairfield hospital's own address
    "NorthBay Health": ("NorthBay Medical Center", "Fairfield", "CA"),
    "Northwell Health": ("Syosset Hospital", "Syosset", "NY"),
    "Parkview Health": ("Parkview Regional Medical Center", "Fort Wayne", "IN"),
    "Providence Health": ("Providence Saint Joseph Medical Center", "Burbank", "CA"),
    # PrimaryLocation is "NJ, United States" only: the town comes later, from the table (finalize)
    "St. Joseph's Health": ("St. Joseph's University Medical Center", "", "NJ"),
    "UChicago Medicine": ("University of Chicago Medical Center", "Chicago", "IL"),
    "WellSpan Health": ("", "Chambersburg", "PA"),
}


@pytest.mark.parametrize("system", sorted(LIST_EXPECT))
def test_real_list_rows(system):
    (j,) = _load("oracle_list_items.json")[system]
    _loc, city, state = scraper._oracle_location(j)
    name = scraper._oracle_work_location_facility(system, j, city, state)
    t = M.oracle_site_town(system, j, name)
    if t:
        city, state = t
    assert (name, city, state) == LIST_EXPECT[system]


def _scrape(monkeypatch, system, items):
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    page = {"items": [{"TotalJobsCount": len(items), "requisitionList": {"items": items, "hasMore": False}}]}

    async def reqs(request):
        return web.json_response(page)

    async def go():
        runner, base = await _serve([("GET", "/hcmRestApi/resources/latest/recruitingCEJobRequisitions", reqs)])
        try:
            async with aiohttp.ClientSession() as s:
                return await scraper.scrape_oracle(_Redirect(s, base), system, scraper.ORACLE_ORGS[system])
        finally:
            await runner.cleanup()
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    return asyncio.run(go())


def test_scrape_oracle_end_to_end(monkeypatch):
    lists = _load("oracle_list_items.json")
    (row,) = _scrape(monkeypatch, "NorthBay Health", lists["NorthBay Health"])
    assert (row.hospital_system, row.hospital_name, row.city, row.state) == \
        ("NorthBay Health", "NorthBay Medical Center", "Fairfield", "CA")
    (row,) = _scrape(monkeypatch, "St. Joseph's Health", lists["St. Joseph's Health"])
    assert (row.hospital_name, row.city, row.state) == ("St. Joseph's University Medical Center", "", "NJ")
    # finalize: the named hospital fills the blank town from the table
    assert F.apply_facilities([row]) == {"St. Joseph's Health": {"list": 1}}
    assert (row.hospital_name, row.city, row.state) == ("St. Joseph's University Medical Center", "Paterson", "NJ")
    (row,) = _scrape(monkeypatch, "Atlantic Health System", lists["Atlantic Health System"])
    assert (row.hospital_name, row.city) == ("Atlantic Health System", "Freehold")   # an office: the system


def test_guthrie_and_tenet_unchanged_by_the_town_rule():
    j = {"workLocation": {"items": [{"LocationName": "Delray Medical Center", "TownOrCity": "Delray Beach", "Region2": "FL"}]}}
    assert M.oracle_site_town("Tenet Healthcare", j, "Delray Medical Center") is None


# ── Feed codes: Jackson, NYU, CHS (stored values) ───────────────────────────
JACKSON = {
    "Jackson Main": ("Jackson Memorial Hospital", "Miami"),
    "South": ("Jackson South Medical Center", "Miami"),
    "North": ("Jackson North Medical Center", "North Miami Beach"),
    "Jackson West Medical Ctr": ("Jackson West Medical Center", "Doral"),
    "JM PMC": ("Jackson Memorial Perdue Medical Center", "Cutler Bay"),
    "JM LTC": ("Jackson Memorial Long Term Care Center", "Miami"),
    "Jackson Rehab": ("Christine E. Lynn Rehabilitation Center", "Miami"),
    "Corporate": ("Jackson Health System", "Miami"),
    "Support Svc": ("Jackson Health System", "Miami"),
    "Corrections": ("Jackson Health System", "Miami"),
    "Private Practices": ("Jackson Health System", "Miami"),
    "Miami Transplant Institute": ("Miami Transplant Institute", "Miami"),   # a named center keeps its name
}


def test_jackson_codes():
    rows = [r for r in _load("stored_rows.json")["stored_names"] if r["hospital_system"] == "Jackson Health System"]
    seen = set()
    for r in rows:
        j = _job("Jackson Health System", r["hospital_name"], r["title"], r["city"], r["state"])
        M.rename_jobs([j])
        F.apply_facilities([j])
        if r["hospital_name"] in JACKSON:
            seen.add(r["hospital_name"])
            assert (j.hospital_name, j.city, j.state) == (*JACKSON[r["hospital_name"]], "FL"), r
        elif r["hospital_name"] == "Women Children":
            want = "Holtz Women's Hospital" if re.search(r"women'?s|mother", r["title"], re.I) else "Holtz Children's Hospital"
            assert j.hospital_name == want, r
        elif r["hospital_name"] == "Behavioral Health":
            assert j.hospital_name in ("Jackson Behavioral Health Hospital", "Jackson Health System"), r
        assert j.hospital_system == "Jackson Health System"
    assert seen == set(JACKSON) - {"Miami Transplant Institute"} | ({"Miami Transplant Institute"} & seen)


def test_jackson_behavioral_health_by_title():
    a = _job("Jackson Health System", "Behavioral Health", "Licensed Practical Nurse, Jackson Behavioral Health, Full-Time")
    b = _job("Jackson Health System", "Behavioral Health", "Peer Specialist, Jackson Community Mental Health Center")
    M.rename_jobs([a, b])
    assert (a.hospital_name, b.hospital_name) == ("Jackson Behavioral Health Hospital", "Jackson Health System")


def test_nyu_spellings():
    rows = [r for r in _load("stored_rows.json")["stored_names"] if r["hospital_system"] == "NYU Langone Health"]
    for r in rows:
        j = _job("NYU Langone Health", r["hospital_name"], r["title"], r["city"], r["state"])
        M.rename_jobs([j])
        F.apply_facilities([j])
        assert "—" not in j.hospital_name and "--" not in j.hospital_name
        assert (j.city, j.state) == (r["city"], r["state"])      # the posting's own town stays
        if "Long Island" in r["hospital_name"]:
            assert j.hospital_name == "NYU Langone Hospital - Long Island"
        elif "Suffolk" in r["hospital_name"]:
            assert j.hospital_name == "NYU Langone Hospital - Suffolk"
        elif r["hospital_name"] == "NYU Langone Health":
            assert j.hospital_name == "NYU Langone Health"       # a school or practice: the system


def test_chs_offices_and_hub_tags():
    rows = [r for r in _load("stored_rows.json")["stored_names"] if r["hospital_system"] == "CHS"]
    want = {"CHS Corporate": "Community Health Systems", "HIM Central Services": "Community Health Systems",
            "CHS Shared Business Operations, LLC": "Community Health Systems",
            "Shared Services Center - Nashville": "Community Health Systems",
            "Merit Health Medical Group (hub)": "Merit Health Medical Group",
            "Tennova Medical Group (hub)": "Tennova Medical Group"}
    for r in rows:
        j = _job("Community Health Systems", r["hospital_name"], r["title"], r["city"], r["state"])
        M.rename_jobs([j])
        F.apply_facilities([j])
        assert j.hospital_name == want.get(r["hospital_name"], r["hospital_name"]), r
        assert (j.city, j.state) == (r["city"], r["state"])      # no town is added, blank stays blank


# ── No body is read (the text family's job) ─────────────────────────────────
def test_body_lines_are_not_read():
    # IU Health and TriHealth bodies name hospitals ("Location: Bloomington
    # Hospital"), but a line can name one in another town than the posting;
    # the work location is read instead, and the row is left on the label here.
    n = 0
    for r in _load("stored_rows.json")["stored_body_lines"]:
        j = _job(r["hospital_system"], r["hospital_system"], r["title"], r["city"], r["state"],
                 description="Posting\n" + "\n".join(r["lines"]) + "\n")
        assert F.apply_facility(j) == "none" and j.hospital_name == r["hospital_system"], r
        n += 1
    assert n >= 40


def test_nothing_stored_is_kept_for_these_systems():
    F.set_known_facilities([{"hospital_system": "TriHealth", "job_id": "77", "hospital_name": "Bethesda North Hospital"},
                            {"hospital_system": "Jackson Health System", "job_id": "9",
                             "hospital_name": "Jackson Memorial Hospital"}])
    j = _job("TriHealth", "TriHealth", city="Montgomery", state="OH", job_id="77")
    assert F.apply_facility(j) == "none" and j.hospital_name == "TriHealth"
    j = _job("Jackson Health System", "Jackson Health System", city="Miami", state="FL", job_id="9")
    assert F.apply_facility(j) == "none"


def test_renown_hawaii_pacific_deaconess_names():
    cases = [
        ("Renown Health", "Renown Regional Med Cntr", "Reno", "NV", "Renown Regional Medical Center"),
        ("Renown Health", "Renown S Meadows Med Cntr", "Reno", "NV", "Renown South Meadows Medical Center"),
        ("Renown Health", "Renown Health Physician Servic", "Reno", "NV", "Renown Health"),
        ("Renown Health", "Hometown Health Plan", "Reno", "NV", "Hometown Health Plan"),
        ("Hawaii Pacific Health", "Straub Medical Center, Honolulu, HI", "Honolulu", "HI", "Straub Medical Center"),
        ("Hawaii Pacific Health", "Kauai Medical Clinic, Lihue, HI", "Lihue", "HI", "Kauai Medical Clinic"),
        ("Deaconess Health", "IL Crossroads Hospital", "Mount Vernon", "IL", "Deaconess Illinois Crossroads Hospital"),
        ("Deaconess Health", "Baptist Hlth Madisonville", "Madisonville", "KY", "Baptist Health Deaconess Madisonville"),
        ("Deaconess Health", "Memorial Medical Center", "Jasper", "IN", "Memorial Medical Center"),
    ]
    for system, raw, city, st, want in cases:
        j = _job(system, raw, "RN", city, st)
        M.rename_jobs([j])
        F.apply_facilities([j])
        assert (j.hospital_name, j.city, j.state) == (want, city, st), (system, raw)
