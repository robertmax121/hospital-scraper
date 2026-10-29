"""2026-10-29 (facility scale phase 4, the chains group): the site a posting
of RadNet, US Physical Therapy, American Family Care, NHS Management, GoHealth
Urgent Care, Cantex Continuing Care or Nexion Health works at
(facilities_chains and its hooks in scraper.py). CityMD names no site.

Fixtures (tests/fixtures/facility_chains_1029/), all real, read 2026-10-29:
  radnet_jobs.json          RadNet /api/jobs rows (Jibe), fields verbatim, bodies cut
  usph_postings.json        US Physical Therapy SmartRecruiters postings-list rows, verbatim
  nhs_requisitions.json     NHS Management UKG Ready job-requisitions rows, verbatim
  gohealth_list.json        GoHealth's Workday CXS list page and its location facet values
  cantex_previews.json      Cantex Paycom job-posting previews (teasers cut)
  nexion_requisitions.json  Nexion ADP CX jobRequisitions rows, verbatim
  stored_rows.json          stored AFC, Nexion and Cantex rows (bodies cut), each with the
                            name the change list gives it ("want")
No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_chains as C
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_chains_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


RADNET = _load("radnet_jobs.json")
USPH = _load("usph_postings.json")
NHS = _load("nhs_requisitions.json")
GOHEALTH = _load("gohealth_list.json")
CANTEX = _load("cantex_previews.json")
NEXION = _load("nexion_requisitions.json")
STORED = _load("stored_rows.json")


def _job(system, name=None, city="", state="", description="", job_id="1", title="Registered Nurse",
         ats="CareerPlug"):
    return Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="",
               url="https://example.org/job/1", job_id=str(job_id), posted_date="", description=description,
               ats_platform=ats)


@pytest.fixture(autouse=True)
def _clean_maps():
    C.set_known_facilities([])
    yield
    C.set_known_facilities([])


class _R:
    def __init__(self, payload):
        self.status = 200
        self._p = payload
        self.headers = {"content-type": "application/json"}

    async def json(self, content_type=None):
        return self._p

    async def text(self):
        return json.dumps(self._p)


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait(*a, **k):
    return None


# ══ RadNet ══════════════════════════════════════════════════════════════════
RADNET_WANT = {
    ("NJIN Clifton", "Onsite"): "New Jersey Imaging Network - Clifton",
    ("AIP Medical Arts 102", "Onsite"): "Advanced Radiology - Medical Arts",
    ("MNHTN E 77th Ground Fl", "Onsite"): "Lenox Hill Radiology - Manhattan E 77th",
    ("BK Kern Bahamas", "Onsite"): "Kern Radiology - Bahamas",
    ("Queens Kew Gardens", "Onsite"): "Lenox Hill Radiology - Queens Kew Gardens",
    ("Queens Elmhurst Queens 8812", "Onsite"): "Lenox Hill Radiology - Queens Elmhurst",
    ("VC Rolling Oaks Simi Valley100", "Onsite"): "Rolling Oaks Radiology - Simi Valley",
    ("Central DRI Elk Grove", "Onsite"): "Diagnostic Radiological Imaging Elk Grove",
    ("Central Modesto", "Onsite"): "Modesto Advanced Imaging",
    ("TM Highway 79", "Onsite"): "RadNet - Highway 79",
    ("IE Corona S Main 801 Ste 101", "Onsite"): "RadNet - Corona S Main",
    ("LA Beverly Tower Wilshire", "Onsite"): "RadNet - Beverly Tower Wilshire",
    ("TCIP - Tradition", "Onsite"): "Radiology Imaging Associates - Tradition",
    ("NJIN of West New York", "Onsite"): "New Jersey Imaging Network - West New York",
    ("SFV Tarzana (200)", "Onsite"): "Breastlink - Tarzana",
    ("SFV Tarzana (100)", "Onsite"): "RadNet - Tarzana",
    ("Bronx Market Admin LL2", "Onsite"): "RadNet",          # an office
    ("Quality and Regulatory DH", "Remote"): "RadNet",       # DeepHealth, remote
    ("Remote Work MD", "Remote"): "RadNet",
    ("AZ Breastlink Mobile Mammograp", "Onsite"): "RadNet",  # a mobile unit
    ("RCO Mil", "Remote"): "RadNet",
    ("LB Palo Verde", "Hybrid"): "RadNet",
}


@pytest.mark.parametrize("row", [r for r in RADNET if (r["location_name"], (r.get("tags2") or [""])[0]) in RADNET_WANT],
                         ids=lambda r: f"{r['location_name']}|{(r.get('tags2') or [''])[0]}")
def test_radnet_centre_from_location_name_and_brand(row):
    want = RADNET_WANT[(row["location_name"], (row.get("tags2") or [""])[0])]
    if C.FLOAT_TITLE_RX.search(row.get("title") or ""):
        want = C.RADNET                 # review A 2026-10-29: a float works across the market's centres
    assert C.radnet_site(row)[0] == want
    assert scraper._jibe_facility(C.RADNET, row) == want


def test_radnet_brand_clean_up():
    assert C.radnet_brand("RadNet Bakersfield (Kern-Radiology)") == "Kern Radiology"
    assert C.radnet_brand("RadNet Ventura (Rolling Oaks Radiology)") == "Rolling Oaks Radiology"
    assert C.radnet_brand("RadNet Orange County") == "RadNet"
    assert C.radnet_brand("Radiology Regional Center, P.A") == "Radiology Regional Center"
    assert C.radnet_brand("Stockton Diagnostic Imaging- March Lane and California Street") == "Stockton Diagnostic Imaging"
    assert C.radnet_brand("") == "RadNet"


def test_radnet_site_table_gives_one_brand_per_centre():
    # "VIP Samaritan Ctr 100" carries Valley Radiology on some postings and NorCal on others
    rows = [r for r in RADNET if r["location_name"] == "VIP Samaritan Ctr 100"]
    assert rows
    names = {C.radnet_site(dict(r, tags1=["Valley Radiology"]))[0] for r in rows} | \
            {C.radnet_site(dict(r, tags1=["NorCal Imaging Centers"]))[0] for r in rows}
    assert len(names) == 1


def test_scrape_jibe_stores_radnet_centres(monkeypatch):
    rows = [dict(r) for r in RADNET]
    payload = {"jobs": [{"data": r} for r in rows], "totalCount": len(rows)}
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    jobs = asyncio.run(scraper.scrape_jibe(None, C.RADNET, scraper.JIBE_SITES[C.RADNET]))
    by = {j.job_id: j for j in jobs}
    for r in rows:
        j = by[str(r["req_id"])]
        assert j.hospital_system == C.RADNET
        assert j.hospital_name == C.radnet_site(r)[0]


def test_a_radnet_hook_that_raises_keeps_the_label(monkeypatch):
    def boom(j):
        raise ValueError("unforeseen row")
    monkeypatch.setattr(C, "radnet_site", boom)
    assert scraper._jibe_facility(C.RADNET, RADNET[0]) == C.RADNET


# ══ US Physical Therapy ═════════════════════════════════════════════════════
USPH_WANT = {
    "01344-Wright PT - Central Boise": "Wright Physical Therapy - Central Boise",
    "01306-Wright PT - Twin Falls Ma": "Wright Physical Therapy - Twin Falls Main",
    "01387-Wright PT � Southeast Boi": "Wright Physical Therapy",          # cut, nothing completes it
    "01199-Carolina PT - NE Columbia": "Carolina Physical Therapy & Sports Medicine - NE Columbia",
    "01534-P4 - Southern Rehab Colum": {"P4 Physical Therapy": "P4 Physical Therapy - Southern Rehab Columbus",
                                        "Southern Rehab & Sports Medicine": "Southern Rehab & Sports Medicine - Columbus"},
    "01204-STAR TN - Greenbrier": "STAR Physical Therapy - Greenbrier",
    "507-STAR TN - White Bridge Rd": "STAR Physical Therapy - White Bridge Road",
    "628-STAR TN - CLARKSVILLE N": "STAR Physical Therapy - Clarksville North",
    "454-STAR TN - Clarksville TN": "STAR Physical Therapy - Clarksville",
    "01643-Agility - Max Motion": "Max Motion Physical Therapy",
    "01646-Agility - Sol Oracle": "Sol Physical Therapy - Oracle",
    "827-Xcelerate PT": "Xcelerate Physical Therapy",
    "01523-Elite S&S - Elite S&S": "Elite Spine and Sports Physical Therapy",
    "573-Kingwood - West Woodlands": "West Woodlands Physical Therapy",
    "785-North Lake - Sports Medic": "North Lake Physical Therapy - Sports Medicine Oregon",
    "01858-TPNT - Ft Worth Plvc Hlth": "Therapy Partners of North Texas",
    "01576-Atlas-Highlands/W Denver": "Atlas Physical Therapy - Highlands/W Denver",
    "1015-PTS-Mechanicsville": "Physical Therapy Solutions - Mechanicsville",
    "833-ARC - KCK": "ARC Physical Therapy+ - KCK",
    "01170-Briotix - Prevention": "US Physical Therapy",      # on-site at a client's plant
    "01746-Metro - Homecare": "US Physical Therapy",          # home care
    "999-U.S. PT Management, Ltd": "US Physical Therapy",     # corporate
    "01103-RACVA - CBO": "US Physical Therapy",               # business office
    "446-STAR TN - Athletic Traing": "US Physical Therapy",   # athletic training outreach
    "01773-Metro - Ignite": "US Physical Therapy",
}


def _clinic(p):
    return {f.get("fieldLabel"): f.get("valueLabel") for f in p.get("customField", [])}.get("Clinic")


@pytest.mark.parametrize("posting", USPH, ids=lambda p: f"{_clinic(p)}|{(p.get('location') or {}).get('remote')}")
def test_usph_clinic_from_brand_and_clinic_fields(posting):
    loc = posting.get("location") or {}
    if loc.get("remote") or loc.get("hybrid"):
        assert C.usph_site(posting) == (C.USPH, "remote")
        return
    want = USPH_WANT[_clinic(posting)]
    if isinstance(want, dict):         # one clinic posted under two brands
        want = want[{f.get("fieldLabel"): f.get("valueLabel") for f in posting.get("customField", [])}["Brands"]]
    if C.FLOAT_TITLE_RX.search(posting.get("name") or ""):
        want = C.USPH                   # review A 2026-10-29: a floater covers several clinics
    assert C.usph_site(posting)[0] == want


def test_scrape_smartrecruiters_stores_usph_clinics(monkeypatch):
    payload = {"content": USPH, "totalFound": len(USPH)}
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    jobs = asyncio.run(scraper.scrape_smartrecruiters(None, C.USPH, "usphysicaltherapy2"))
    by = {j.job_id: j for j in jobs}
    assert len(by) == len(USPH)
    for p in USPH:
        assert by[str(p["id"])].hospital_name == C.usph_site(p)[0]
        assert by[str(p["id"])].hospital_system == C.USPH


def test_smartrecruiters_other_systems_are_untouched():
    assert C.smartrecruiters_site("Northwestern Medicine", USPH[0]) is None


# ══ NHS Management ══════════════════════════════════════════════════════════
def test_nhs_home_from_address_line_1():
    got = {(r["location"] or {}).get("address_line_1"): C.kronos_site(C.NHS, r["location"]) for r in NHS}
    assert got["Huntsville Health and Rehab"] == "Huntsville Health and Rehab"
    assert got["Park Manor Health and Rehab"] == "Park Manor Health and Rehab"
    assert got["Aspire Cahaba River"] == "Aspire Cahaba River"
    assert got["3141 Old Columbiana Road"] == ""          # the Birmingham office's street
    assert got["575 Southland Drive"] == ""
    assert C.kronos_site("Magruder Hospital", NHS[0]["location"]) is None


def test_scrape_kronos_stores_nhs_homes(monkeypatch):
    payload = {"job_requisitions": NHS, "_paging": {"offset": 1, "size": 100, "total": len(NHS)}}
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    jobs = asyncio.run(scraper.scrape_kronos(None, C.NHS, scraper.KRONOS_ORGS[C.NHS]))
    by = {j.job_id: j for j in jobs}
    assert len(by) == len(NHS)
    for r in NHS:
        want = C.kronos_site(C.NHS, r["location"]) or C.NHS
        assert by[str(r["id"])].hospital_name == want


# ══ GoHealth Urgent Care ════════════════════════════════════════════════════
def test_gohealth_every_board_descriptor_but_four_names_a_centre():
    left = [d for d in GOHEALTH["primaryLocation"] if not C.gohealth_site(d)]
    assert sorted(left) == ["Hartford - Derby", "Remote", "Remote - New York (Northwell)"]
    assert len(GOHEALTH["primaryLocation"]) - len(left) >= 190


@pytest.mark.parametrize("loc,want", [
    ("Seminary Plaza - Alexandria, VA", ("GoHealth Urgent Care - Seminary Plaza", "Alexandria", "VA")),
    ("Kirkwood - Kirkwood, MO", ("GoHealth Urgent Care - Kirkwood", "Kirkwood", "MO")),
    ("Erie - Summit Towne Centre - Erie, PA", ("GoHealth Urgent Care - Erie Summit Towne Centre", "Erie", "PA")),
    ("Erie - Erie West - Erie, PA", ("GoHealth Urgent Care - Erie West", "Erie", "PA")),
    ("Charleston - Staten Island, NY", ("GoHealth Urgent Care - Charleston", "Staten Island", "NY")),
    ("UPMC - Shadyside", ("GoHealth Urgent Care - Shadyside", "Pittsburgh", "PA")),
    ("Downtown Hartford, CT", ("GoHealth Urgent Care - Downtown Hartford", "Hartford", "CT")),
    ("6 Locations", None),
    ("Remote", None),
    ("Hartford - Derby", None),
])
def test_gohealth_descriptor(loc, want):
    assert C.gohealth_site(loc) == want
    assert scraper.WD_SITE_FACILITY[C.GOHEALTH](loc, "") == want


def test_gohealth_list_rows_multi_location_postings_name_none():
    for j in GOHEALTH["jobPostings"]:
        got = scraper.WD_SITE_FACILITY[C.GOHEALTH](j["locationsText"], j["externalPath"])
        if "Locations" in j["locationsText"]:
            assert got is None
            assert C.gohealth_unknown(j["locationsText"], j["externalPath"]) == ""
        elif C.gohealth_pool(j["externalPath"]):
            assert got is None              # review A 2026-10-29: a regional or float role, no one centre's
        else:
            assert got and got[0].startswith("GoHealth Urgent Care - ")
    assert C.gohealth_unknown("Hartford - Derby") == "Hartford - Derby"


# ══ Cantex Continuing Care ══════════════════════════════════════════════════
def _preview(loc):
    return next(p for p in CANTEX if p["locations"] == loc)


@pytest.mark.parametrize("loc,want", [
    ("Skilled Nursing at The Carlyle at Stonebridge Park - Southlake, TX 76092", "The Carlyle at Stonebridge Park"),
    ("Pivot at The Madison on Marsh - Carrollton, TX 75006", "The Madison on Marsh"),
    ("Skilled Nursing at Woodville Health & Rehabilitation Center - Woodville, TX 75979",
     "Woodville Health and Rehabilitation Center"),
    ("Elysian Hospice - Dallas - Addison, TX 75001", "Elysian Hospice - Dallas"),
    ("TheraCare Home Health - Sherman/Denison (Grayson County) - Denison, TX 75020", "TheraCare Home Health - Sherman/Denison"),
    ("Pharmacare - San Antonio - San Antonio, TX 78229", "Pharmacare - San Antonio"),
    ("Cantex Corporate - Carrollton, TX 75006", ""),
    ("Cantex - Houston  - Stafford , TX 77477", ""),
    ("", ""),
])
def test_cantex_site_from_the_location_value(loc, want):
    assert C.paycom_site(C.CANTEX, loc) == want
    prev = _preview(loc)
    job = scraper._paycom_job(prev, None, C.CANTEX, "K", "TX")
    assert job.hospital_name == (want or C.CANTEX)


def test_cantex_town_stays_the_adapters():
    job = scraper._paycom_job(_preview("Elysian Hospice - Dallas - Addison, TX 75001"), None, C.CANTEX, "K", "TX")
    assert (job.city, job.state) == ("Addison", "TX")


def test_paycom_other_clients_are_untouched():
    assert C.paycom_site("Connally Memorial Medical Center", "Main 499 - Floresville, TX 78114") is None


@pytest.mark.parametrize("row", STORED["cantex"], ids=lambda r: str(r["id"]))
def test_cantex_stored_rows_give_the_change_list_name(row):
    name, _how = C.cantex_stored_site(row["title"], row["description"], row["city"], row["state"])
    if row["want"] != C.CANTEX:
        assert name == row["want"]


# ══ Nexion Health ═══════════════════════════════════════════════════════════
def _nexion_job(item):
    return scraper._adpcx_job(item, C.NEXION, "nexion", "")


def test_nexion_titles_and_entities_name_the_home():
    seen = {}
    for item in NEXION:
        j = _nexion_job(item)
        seen[item["publishedJobTitle"]] = j.hospital_name
    assert seen["Maintenance Director - FT - Sterling Hills Healthcare Center"] == "Sterling Hills Healthcare Center"
    assert seen["CNA: Natchez Rehabilitation and Healthcare Center"] == "Natchez Rehabilitation and Healthcare Center"
    assert seen["Maintenance Assistant (PT): Delt Rehabilitation and Healthcare Center"] == \
        "Delta Rehabilitation and Healthcare Center"
    assert seen["LPN: Yazoo City Rehabilitation and Healthcare Center"] == "Yazoo City Rehabilitation and Healthcare Center"
    assert seen["RN - New Iberia North"] == "New Iberia Manor North"
    # a bare town in the title: the requisition location's entity names the home
    assert seen["Cook - Terrell"] == "Terrell Healthcare Center"
    assert seen["Registered Nurse - Duncanville"] == "Duncanville Healthcare Center"
    assert seen["Admissions Coordinator-LPN Thibodaux"] == "Thibodaux Healthcare"
    # regional and corporate roles keep the system name
    assert seen["Regional Financial Specialist"] == C.NEXION
    assert seen["Staff Accountant"] == C.NEXION


def test_nexion_entity_in_another_town_names_none():
    assert C.nexion_site("Cook", "Dallas", "TX", {"nameCode": {"codeValue": "SDM"}}) == ("", "other town")
    assert C.nexion_title_site("LPN - Cross Timbers", "Terrell", "TX") == ("", "other town")


def test_adpcx_other_boards_are_untouched():
    assert C.adpcx_site("NextCare Urgent Care", "Medical Assistant", "Mesa", "AZ", {}) is None


@pytest.mark.parametrize("row", STORED["nexion"], ids=lambda r: str(r["id"]))
def test_nexion_stored_titles_give_the_change_list_name(row):
    name, _how = C.nexion_title_site(row["title"], row["city"], row["state"])
    assert (name or C.NEXION) == row["want"]


# ══ American Family Care ════════════════════════════════════════════════════
@pytest.mark.parametrize("row", STORED["afc"], ids=lambda r: str(r["id"]))
def test_afc_centre_only_when_the_posting_names_it_at_the_rows_town(row):
    name, _how = C.afc_centre(row["title"], row["description"], row["city"], row["state"])
    assert (name or C.AFC) == row["want"]


def test_afc_examples():
    by = {r["id"]: r for r in STORED["afc"]}
    assert by[26662042]["want"] == "AFC Urgent Care Cicero"                 # "Location: AFC Urgent Care Cicero"
    assert by[26662046]["want"] == "AFC Urgent Care Modesto"                # title "... for AFC Urgent Care - Modesto"
    assert by[26662733]["want"] == "AFC Urgent Care Southwest Austin"       # a part of the row's town
    assert by[26662204]["want"] == "AFC Urgent Care Harper's Point"         # a named spot, no town anywhere
    assert by[26662245]["want"] == C.AFC                                    # "AFC Urgent Care of Louisville" on a Boulder row
    assert by[26662309]["want"] == C.AFC                                    # North Bergen centre on a Lyndhurst row
    assert by[38697184]["want"] == C.AFC                                    # "AFC Urgent Care San Diego" on a Santee row
    assert by[26662058]["want"] == C.AFC                                    # names no centre (6 applies stay)


def test_afc_franchise_group_names_none():
    assert C.afc_centre("Physician", "At AFC Urgent Care Portland/Vancouver, we care", "Portland", "OR") == ("", "none")


def test_afc_stored_centre_kept_on_a_night_without_a_body():
    C.set_known_facilities([{"hospital_system": C.AFC, "job_id": "77", "hospital_name": "AFC Urgent Care Cicero"},
                            {"hospital_system": C.AFC, "job_id": "78", "hospital_name": C.AFC}])
    j = _job(C.AFC, city="Cicero", state="IL", job_id="77", title="Medical Assistant")
    assert C.apply_facility(j) == "kept" and j.hospital_name == "AFC Urgent Care Cicero"
    j2 = _job(C.AFC, city="Cicero", state="IL", job_id="78", title="Medical Assistant")
    assert C.apply_facility(j2) == "none" and j2.hospital_name == C.AFC
    # a night whose body names no centre leaves the label, the stored name is not kept
    j3 = _job(C.AFC, city="Cicero", state="IL", job_id="77", title="Medical Assistant",
              description="Benefits/Perks " * 40)
    assert C.apply_facility(j3) == "none" and j3.hospital_name == C.AFC


def test_afc_pass_leaves_other_systems_and_other_names():
    j = _job("Concentra", city="Cicero", state="IL", title="AFC Urgent Care Cicero")
    assert C.apply_facility(j) == "" and j.hospital_name == "Concentra"
    j2 = _job(C.AFC, name="Somebody Else", city="Cicero", state="IL")
    assert C.apply_facility(j2) == "other" and j2.hospital_name == "Somebody Else"


def test_afc_row_that_raises_keeps_its_name(monkeypatch):
    def boom(*a, **k):
        raise ValueError("unforeseen row")
    monkeypatch.setattr(C, "afc_centre", boom)
    j = _job(C.AFC, city="Cicero", state="IL")
    assert C.apply_facilities([j]) == {C.AFC: {"error": 1}}
    assert j.hospital_name == C.AFC


def test_finalize_names_afc_rows_like_the_change_list():
    jobs = [_job(C.AFC, city=r["city"], state=r["state"], description=r["description"], job_id=r["id"],
                 title=r["title"]) for r in STORED["afc"]]
    out = {int(o["job_id"]): o for o in scraper.finalize_jobs(jobs)}
    for r in STORED["afc"]:
        assert out[r["id"]]["hospital_name"] == r["want"]
        assert out[r["id"]]["hospital_system"] == C.AFC


# ══ CityMD and everything else ══════════════════════════════════════════════
def test_citymd_is_never_renamed():
    for fn in (lambda: C.jibe_site(C.CITYMD, {}), lambda: C.smartrecruiters_site(C.CITYMD, {}),
               lambda: C.kronos_site(C.CITYMD, {}), lambda: C.paycom_site(C.CITYMD, ""),
               lambda: C.adpcx_site(C.CITYMD, "", "", "", {})):
        assert fn() is None
    j = _job(C.CITYMD, city="Florham Park", state="NJ", ats="Workday")
    assert C.apply_facility(j) == "" and j.hospital_name == C.CITYMD
    assert C.CITYMD not in scraper.WD_SITE_FACILITY


def test_other_jibe_systems_are_untouched():
    assert C.jibe_site("Amedisys", RADNET[0]) is None
