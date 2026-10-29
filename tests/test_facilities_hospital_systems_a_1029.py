"""2026-10-29 (facility scale phase 3, hospital systems A): Lifepoint's
body-named sites and the Lifepoint / Providence / Baptist Memorial Oracle
work locations (facilities_hospital_systems_a).

Fixtures (tests/fixtures/facility_hsa_1029/), real, read 2026-10-29:
  lifepoint_rows.json          stored Lifepoint hospital_jobs rows on the
                               system label (title, town, first 1,600
                               characters of the body), one per reading case
  oracle_work_locations.json   each tenant's workLocationsFacet as the
                               tenant served it (overall, per state, and for
                               Seattle and Spokane), with counts
No network, no database."""
import json
import os

import pytest

import facilities_hospital_systems_a as H
import facilities_oracle_other as O
import facility_names as F
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_hsa_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


ROWS = {r["case"]: r for r in _load("lifepoint_rows.json")}
WL = _load("oracle_work_locations.json")


@pytest.fixture(autouse=True)
def _no_known():
    H.set_known_facilities([])
    F.set_known_facilities([])
    yield
    H.set_known_facilities([])


def _job(r=None, *, system=H.LIFEPOINT, name=None, title=None, city=None, state=None, description=None,
         job_id=None):
    r = r or {}
    city = r.get("city", "") if city is None else city
    state = r.get("state", "") if state is None else state
    return Job(title=r.get("title", "RN") if title is None else title, hospital_system=system,
               hospital_name=system if name is None else name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="",
               url=r.get("url", "https://x/1"), job_id=str(r.get("job_id", "1") if job_id is None else job_id),
               posted_date="", description=r.get("description", "") if description is None else description,
               ats_platform="Oracle HCM")


# ── Lifepoint bodies (real stored rows) ─────────────────────────────────────
BODY_EXPECT = {
    "part-of Sovah Danville":        ("Sovah Health - Danville", "Danville", "VA"),
    "lead Your experience matters":  ("Jackson Purchase Medical Center", "Mayfield", "KY"),
    "part-of with beds tail":        ("Paris Regional Medical Center", "Paris", "TX"),
    "operated jointly":              ("Liberty Rehabilitation Hospital", "Liberty Township", "OH"),
    "facility line":                 ("Frye Regional Medical Center", "Hickory", "NC"),
    "generic Sovah at Martinsville": ("Sovah Health - Martinsville", "Martinsville", "VA"),
    "formerly tail":                 ("Centerpoint Health - Winchester", "Winchester", "KY"),
    # one hospital on two campuses: the work location says the same name
    "Starr Etowah campus":           ("Starr Regional Medical Center", "Etowah", "TN"),
    "duke lifepoint":                ("Conemaugh Memorial Medical Center", "Johnstown", "PA"),
    "beds sentence":                 ("Fleming County Hospital", "Flemingsburg", "KY"),
    "opening line":                  ("North Alabama Medical Center", "Florence", "AL"),
    # a named senior-care site keeps its own name, whatever the title says
    "named care site":               ("Starr Regional Health and Rehab", "Etowah", "TN"),
    # stays on "Lifepoint Health" with its own town:
    "partner unit":                  None,   # Lifepoint Rehabilitation inside a Kindred hospital
    "elsewhere":                     None,   # Harris Regional named on a Franklin posting (a clinic there)
    "offsite title":                 None,   # an athletic trainer: not the hospital's floor
    "corporate office":              None,   # Brentwood head office
}


@pytest.mark.parametrize("case", sorted(BODY_EXPECT))
def test_lifepoint_body(case):
    r = ROWS[case]
    got = H.text_facility(H.LIFEPOINT, r["description"], r["title"], r["city"], r["state"])
    assert got == BODY_EXPECT[case]


@pytest.mark.parametrize("case", sorted(BODY_EXPECT))
def test_lifepoint_apply_facility(case):
    r = ROWS[case]
    j = _job(r)
    how = H.apply_facility(j)
    exp = BODY_EXPECT[case]
    if exp is None:
        assert (j.hospital_name, j.city, j.state) == (H.LIFEPOINT, r["city"], r["state"])
        assert how in ("unknown", "elsewhere", "offsite", "none")
    else:
        assert how == "text"
        assert (j.hospital_name, j.city, j.state) == exp
    assert j.hospital_system == H.LIFEPOINT


def test_subjects_cut_lead_and_tail():
    assert H.lifepoint_subject("Your experience matters\n\nSovah Danville is part of Lifepoint Health, a diversified")\
        == "Sovah Danville"
    assert H.lifepoint_subject("Your experience matters Jackson Purchase is part of Lifepoint Health") == \
        "Jackson Purchase"
    assert H.lifepoint_subject("Paris Regional is a 154 bed hospital located in Paris, Texas, and is part of "
                               "Lifepoint Health") == "Paris Regional"
    assert H.lifepoint_subject("Centerpoint - Winchester, formerly Clark Regional Medical Center, is part of "
                               "Lifepoint Health") == "Centerpoint - Winchester"
    # "St." ends no sentence
    assert H.lifepoint_subject("St. Luke's Health-Brazosport Hospital is part of Lifepoint Health") == \
        "St. Luke's Health-Brazosport Hospital"
    assert H.lifepoint_subject("No employer sentence here.") == ""


def test_two_sites_in_one_sentence_name_none():
    body = "Your experience matters\nCenterpoint - Winchester & Centerpoint - Paris are part of Lifepoint Health."
    assert H.text_facility(H.LIFEPOINT, body, "RN", "Winchester", "KY") is None


def test_a_site_in_another_town_is_never_moved():
    body = "Haywood Regional Medical Center is part of Lifepoint Health"
    assert H.text_facility(H.LIFEPOINT, body, "RN", "Clyde", "NC") == ("Haywood Regional Medical Center", "Clyde", "NC")
    assert H.text_facility(H.LIFEPOINT, body, "RN", "Asheville", "NC") is None


def test_clinic_and_remote_titles_keep_the_label():
    body = "Frye Regional Medical Center is part of Lifepoint Health"
    for title in ("Certified Medical Assistant (CMA) - Hickory Family Medicine", "Registered Nurse (RN) FryeCare "
                  "Cardiology", "Remote Coding Specialist", "Front Desk - Urgent Care"):
        assert H.text_facility(H.LIFEPOINT, body, title, "Hickory", "NC") is None, title
    assert H.text_facility(H.LIFEPOINT, body, "Registered Nurse (RN) ICU", "Hickory", "NC") == \
        ("Frye Regional Medical Center", "Hickory", "NC")


# ── Nights without a body ───────────────────────────────────────────────────
def test_no_body_keeps_the_stored_site():
    H.set_known_facilities([
        {"hospital_system": H.LIFEPOINT, "job_id": "K1", "hospital_name": "Sovah Health - Danville"},
        {"hospital_system": H.LIFEPOINT, "job_id": "K2", "hospital_name": "Lifepoint Health"},      # label: not kept
        {"hospital_system": "Other", "job_id": "K3", "hospital_name": "Sovah Health - Danville"},
    ])
    j = _job(city="Danville", state="VA", description="", job_id="K1")
    assert H.apply_facility(j) == "kept" and j.hospital_name == "Sovah Health - Danville"
    j2 = _job(city="Danville", state="VA", description="", job_id="K2")
    assert H.apply_facility(j2) == "none" and j2.hospital_name == H.LIFEPOINT
    # the posting moved to another town: the stored site is not carried there
    j3 = _job(city="Richmond", state="VA", description="", job_id="K1")
    assert H.apply_facility(j3) == "none" and j3.hospital_name == H.LIFEPOINT


def test_a_row_the_work_location_named_is_left_alone():
    r = ROWS["part-of Sovah Danville"]
    j = _job(r, name="Sovah Health - Danville - ARU")
    assert H.apply_facility(j) == "other" and j.hospital_name == "Sovah Health - Danville - ARU"
    assert H.apply_facility(_job(r, system="Providence Health", name="Providence Health")) == ""


def test_load_known_without_credentials(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert H.load_known_facilities() == 0


# ── Oracle work locations (the tenants' own facets) ─────────────────────────
WL_EXPECT = {
    "Lifepoint Health": {
        "Haywood Regional Medical Center": "Haywood Regional Medical Center",          # main's ten, unchanged
        "UP Health System - Marquette General Hospital": "UP Health System-Marquette",
        "Sovah Health - Danville": "Sovah Health - Danville",
        "Sovah Health - Martinsville": "Sovah Health - Martinsville",
        "Twin County Regional Hospital": "Twin County Regional Healthcare",
        "Clinch Valley Medical Center": "Clinch Valley Health",
        "CHC PR & MSA - Centra Health Rehab Hospital - Lynchburg VA": "Centra Rehabilitation Hospital",
        "Centra BH MSA JV": "Centra Behavioral Health Hospital",
        "Sovah Health - Danville - ARU - R362J": "Sovah Health - Danville",
        "Centerpoint Health - Winchester Hospital": "Centerpoint Health - Winchester",
        "Highpoint - Sumner Regional Medical Center": "Highpoint Health - Sumner",
        "Highpoint Health - Winchester with Ascension Saint Thomas": "Highpoint Health - Winchester",
        "Southern TN Regional Health System - Pulaski": "Southern Tennessee Regional Health System - Pulaski",
        "Ascension St. Thomas Rehabilitation Hospital - IRF": "Ascension Saint Thomas Rehabilitation Hospital",
        # a named senior-care site: its own name (decision 2)
        "Starr Regional - Health and Rehab": "Starr Regional Health and Rehab",
        # offices, clinics and partner units keep the system
        "Lifepoint Health - Corporate": "",
        "Sovah Health - Danville Pulmonology": "",
        "Twin County Galax Family Care Center": "",
        "CHC PR & MSA - UofL Health Frazier Rehab Hosp - Brownsboro": "",
    },
    "Providence Health": {
        "Sacred Heart Medical Center-Spokane(HR)": "Providence Sacred Heart Medical Center",   # main's twelve
        "Kadlec Regional Medical Ctr-Richland(HR)": "Kadlec Regional Medical Center",
        "Swedish Edmonds 21601 76th(HR)": "Swedish Edmonds",
        "Swedish Cherry Hill 500 17th-Seattle(HR)": "Swedish Cherry Hill",
        "St Patrick Hospital-Broadway Bldg-Missoula MT(HR)": "Providence St. Patrick Hospital",
        "St Joseph Hospital-Orange(HR)": "Providence St. Joseph Hospital Orange",
        "Providence Hood River Memorial Hosp-Hood River(HR)": "Providence Hood River Memorial Hospital",
        "Covenant Hobbs Hospital-4900 N Lovington Hwy(HR)": "Covenant Health Hobbs Hospital",
        "St Lukes Rehab-Spokane(HR)": "Providence St. Luke's Rehabilitation Medical Center",
        # buildings, clinics, office parks, urgent care
        "Swedish First Hill 1101 Madison-Seattle(HR)": "",
        "Swedish Cherry Hill 550 17th-Seattle(HR)": "",
        "Providence Valley Office Park-Renton(HR)": "",
        "Covenant Medical Grp-Nashville Ave(HR)": "",
        "Covenant Hobbs Urgent Care-5320 Lovington Hwy(HR)": "",
        "Providence Medical Grp AK-Anchorage(HR)": "",
        "PACMED Admin Bh-Seattle(HR)": "",
        "Providence PACE Seattle-Seattle(HR)": "",
        "Mother Gamelin Ctr-Spokane(HR)": "",
    },
    "Baptist Memorial Health Care": {
        "BMH - Memphis Hospital": "Baptist Memorial Hospital-Memphis",                 # main's nine
        "BMH - Women's Hospital": "Baptist Memorial Hospital for Women",
        "BMH - Huntingdon Hospital": "Baptist Memorial Hospital-Carroll County",
        "BMH - Tipton Hospital": "Baptist Memorial Hospital-Tipton",
        "BMH - Oktibbeha County Hospital": "Baptist Memorial Hospital-Oktibbeha County",
        "BMH - Crittenden Hospital": "Baptist Memorial Hospital-Crittenden",
        "BMH - Paragould Hospital": "Baptist Memorial Hospital-Paragould",
        "BMHCC Corporate Office": "",
        "BMG - Central Business Office": "",
        "Baptist Health Sciences University": "",
        "NEA - Clinic": "",
        "NEA - Stadium Clinic": "",
        "BMG - The Medical Group": "",
    },
}


@pytest.mark.parametrize("system", sorted(WL_EXPECT))
def test_work_locations(system):
    seen = {name for name, _n in WL[system]}
    for raw, exp in WL_EXPECT[system].items():
        assert raw in seen, (system, raw)           # the tenant's own spelling
        assert F.list_facility(system, raw, "", "") == exp, (system, raw)


def test_every_facet_value_is_decided():
    """Each work location the facets served is either named or one of the
    offices, clinics and buildings the module leaves on the system."""
    left = {
        "Lifepoint Health": {"Lifepoint Health - Corporate", "Sovah Health - Danville Pulmonology",
                             "Twin County Galax Family Care Center",
                             "CHC PR & MSA - UofL Health Frazier Rehab Hosp - Brownsboro"},
        "Baptist Memorial Health Care": {"BMHCC Corporate Office", "BMG - Central Business Office",
                                         "Baptist Health Sciences University", "BMG - The Medical Group",
                                         "NEA - Clinic", "NEA - Clinics", "NEA - Stadium Clinic",
                                         "NEA - Woodsprings Clinic", "NEA - Cancer Center", "NEA - Paragould Clinic",
                                         "NEA - Manila"},
    }
    for system, names in left.items():
        for raw, _n in WL[system]:
            got = F.list_facility(system, raw, "", "")
            assert bool(got) != (raw in names), (system, raw, got)


def test_rules_reach_hospitals_the_facet_did_not_list_and_never_a_clinic():
    P, B = "Providence Health", "Baptist Memorial Health Care"
    assert F.list_facility(P, "St Jude Medical Center-Fullerton(HR)", "Fullerton", "CA") == \
        "Providence St. Jude Medical Center"
    assert F.list_facility(P, "St Jude Heritage Clinic-Fullerton(HR)", "Fullerton", "CA") == ""
    assert F.list_facility(P, "Mount Carmel Hospital-Colville(HR)", "Colville", "WA") == "Providence Mount Carmel Hospital"
    assert F.list_facility(B, "BMH - Yazoo Hospital", "Yazoo City", "MS") == "Baptist Medical Center Yazoo"
    assert F.list_facility(B, "BMG - Yazoo Clinic", "Yazoo City", "MS") == ""
    assert F.list_facility(B, "BAMC-Baptist Anderson Regional Medical Center South", "Meridian", "MS") == \
        "Baptist Anderson Regional Medical Center"
    # a hospital is never taken from another state (facility_names.resolve)
    assert F.list_facility(B, "BMH - Tipton Hospital", "Oxford", "MS") == ""


def test_generic_spelling_follows_the_campus_town():
    L = "Lifepoint Health"
    assert F.list_facility(L, "Sovah Health", "Martinsville", "VA") == "Sovah Health - Martinsville"
    assert F.list_facility(L, "Sovah Health", "Danville", "VA") == "Sovah Health - Danville"
    assert F.list_facility(L, "Maria Parham", "Louisburg", "NC") == "Maria Parham Franklin"


def test_extend_oracle_keeps_main_and_adds_the_rest():
    for system, extra in H.ORACLE_EXTRA.items():
        t = O.SYSTEMS[system]
        for name in extra["hospitals"]:
            assert name in t["hospitals"], (system, name)
        for raw, target in extra["sites"].items():
            assert t["sites"][raw] == target
    # main's names and towns did not move
    assert O.SYSTEMS["Providence Health"]["hospitals"]["Covenant Medical Center"][:2] == ("Lubbock", "TX")
    assert O.SYSTEMS["Lifepoint Health"]["hospitals"]["UP Health System-Marquette"][:2] == ("Marquette", "MI")
    assert O.SYSTEMS["Baptist Memorial Health Care"]["hospitals"]["Baptist Memorial Hospital-DeSoto"][:2] == \
        ("Southaven", "MS")
    # the Lifepoint text table and the Oracle table write the same names
    for site, _sp in H.LIFEPOINT_SITES:
        assert site.name in O.SYSTEMS["Lifepoint Health"]["hospitals"], site.name


MAIN_LIFEPOINT = {"Haywood Regional Medical Center", "Ottumwa Regional Health Center", "Raleigh General Hospital",
                  "Conemaugh Memorial Medical Center", "Frye Regional Medical Center", "North Alabama Medical Center",
                  "Harris Regional Hospital", "Lake Cumberland Regional Hospital", "Havasu Regional Medical Center",
                  "UP Health System-Marquette"}


def test_extend_oracle_refuses_a_moved_hospital_and_changes_nothing():
    # main's tables as they were before the additions
    fake = {}
    for s, t in O.SYSTEMS.items():
        if s not in H.ORACLE_EXTRA:
            continue
        extra = H.ORACLE_EXTRA[s]
        main_h = {n: v for n, v in t["hospitals"].items() if n not in extra["hospitals"] or n in MAIN_LIFEPOINT}
        main_s = {r: v for r, v in t["sites"].items() if r not in extra["sites"]}
        fake[s] = dict(hospitals=main_h, sites=main_s, rules=tuple(t["rules"])[:len(t["rules"]) - len(extra["rules"])])
    before = json.dumps(fake, sort_keys=True, default=str)
    # Baptist Memorial (checked last) carries a hospital in the wrong town
    fake["Baptist Memorial Health Care"]["hospitals"]["Baptist Memorial Hospital-Tipton"] = ("Memphis", "TN", "cms 0")
    with pytest.raises(ValueError):
        H.extend_oracle(fake)
    del fake["Baptist Memorial Health Care"]["hospitals"]["Baptist Memorial Hospital-Tipton"]
    assert json.dumps(fake, sort_keys=True, default=str) == before       # Lifepoint and Providence untouched
    added = H.extend_oracle(fake)
    main_names = json.loads(before)
    assert added == {s: sum(1 for n in e["hospitals"] if n not in main_names[s]["hospitals"])
                     for s, e in H.ORACLE_EXTRA.items()}
    assert added["Lifepoint Health"] == len(H.LIFEPOINT_SITES) - 10      # main's ten are kept as they were
    assert "Kadlec Regional Medical Center" in fake["Providence Health"]["hospitals"]


def test_list_row_through_scrape_oracle_helpers():
    j = {"Id": "1", "Title": "RN", "PrimaryLocation": "Danville, VA, United States", "PrimaryLocationCountry": "US",
         "workLocation": {"items": [{"LocationName": "Sovah Health - Danville", "TownOrCity": "Danville",
                                     "Region2": "VA", "Country": "US"}]}}
    _loc, city, state = scraper._oracle_location(j)
    name = scraper._oracle_work_location_facility("Lifepoint Health", j, city, state)
    assert (name, O.oracle_site_town("Lifepoint Health", j, name)) == ("Sovah Health - Danville", ("Danville", "VA"))


# ── Through finalize_jobs (the upsert payload) ──────────────────────────────
def test_finalize_jobs_payload(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    H.set_known_facilities([{"hospital_system": H.LIFEPOINT, "job_id": "K9", "hospital_name": "Trios Health"}])
    rows = scraper.finalize_jobs([
        _job(ROWS["part-of Sovah Danville"]),
        _job(ROWS["partner unit"]),
        _job(city="Kennewick", state="WA", description="", job_id="K9", title="Registered Nurse (RN) ICU"),
        _job(ROWS["facility line"], name="Frye Regional Medical Center"),         # the work location named it
    ])
    by = {r["job_id"]: r for r in rows}
    d = by[str(ROWS["part-of Sovah Danville"]["job_id"])]
    assert (d["hospital_name"], d["city"], d["state"], d["hospital_system"]) == \
        ("Sovah Health - Danville", "Danville", "VA", H.LIFEPOINT)
    assert by[str(ROWS["partner unit"]["job_id"])]["hospital_name"] == H.LIFEPOINT
    assert by["K9"]["hospital_name"] == "Trios Health" and by["K9"]["city"] == "Kennewick"
    assert by[str(ROWS["facility line"]["job_id"])]["hospital_name"] == "Frye Regional Medical Center"


# ── Piedmont: the Summerville campus street (facilities_icims) ──────────────
def test_piedmont_summerville_campus_street():
    import facilities_icims as I
    for street in ("2260 Wrightsboro Road", "2260 Wrightsboro Rd"):
        assert I.piedmont_facility({"location_name": street, "city": "Augusta"}) == \
            ("Piedmont Augusta Summerville Campus", "street")
    # the main campus is unchanged, and the street names nothing in another town
    assert I.piedmont_facility({"location_name": "1350 Walton Way", "city": "Augusta"})[0] == "Piedmont Augusta Hospital"
    assert I.piedmont_facility({"location_name": "2260 Wrightsboro Road", "city": "Macon"})[0] == I.PIEDMONT
