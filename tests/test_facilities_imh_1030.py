"""2026-10-30 (facility scale phase 7, the owner: "innermountain needs billings
clinic added"): Intermountain's Workday rows name their clinic, center or
hospital (facilities_imh, registered in facilities_workday and hooked through
WD_SITE_FACILITY); offices, home services, Life Flight bases, multi-site
postings and clinic float pools keep the system label with the town.

Fixtures (tests/fixtures/imh1030/wd_list_pages.json) are real payloads: two
first CXS list pages of imh.wd108 (searchText "Billings" and "Float Pool"),
read 2026-10-30 a few seconds apart, no login, trimmed to the fields
scrape_workday reads. Bodies below are written for the tests (the board's
"Location:" / "Work City:" / "Work State:" layout). No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_imh as I
import facilities_workday as FW
import scraper
from test_wd_tenant_facets import _fake_req

S = I.SYSTEM
FIX = os.path.join(os.path.dirname(__file__), "fixtures", "imh1030")
with open(os.path.join(FIX, "wd_list_pages.json"), encoding="utf-8") as _f:
    PAGES = json.load(_f)["pages"]
URL = "https://imh.wd108.myworkdayjobs.com/IntermountainCareers/job/{}/X_R1"


def _crawl(monkeypatch, key):
    page = PAGES[key]
    _fake_req(monkeypatch, lambda b: page if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(page["jobPostings"]), "jobPostings": []})
    return asyncio.run(scraper.scrape_workday(None, S, scraper.WORKDAY_TENANTS[S]))


def _body(sites, city, state):
    return (f"Job Description:\n\nAbout this role.\n\nLocation:\n{', '.join(sites)}\n\nWork City:\n{city}\n\n"
            f"Work State:\n{state}\n\nScheduled Weekly Hours:\n40\n")


# ── the hook ────────────────────────────────────────────────────────────────

def test_the_resolver_is_hooked_for_both_labels():
    assert scraper.WD_SITE_FACILITY[S] is I.site
    assert scraper.WD_SITE_UNKNOWN[S] is I.unknown
    assert scraper.HOSPITAL_SYSTEM_ALIASES[S] == I.DB_SYSTEM
    assert FW.covers(S) and FW.covers(I.DB_SYSTEM)
    assert len(FW.TABLES[S]) >= 230


def test_the_owner_row_names_intermountains_billings_downtown_clinic():
    # https://jobs.waypointrecruit.com/jobs/patient-service-representative-non-oncology-infusion-center-billings-mt-37866021
    path = "/job/Billings-Downtown-Clinic/Patient-Service-Representative---Non-Oncology-Infusion-Center_R185815"
    want = ("Intermountain Health Billings Downtown Clinic", "Billings", "MT")
    assert I.site("Billings Downtown Clinic", path) == want
    assert I.site("", path) == want
    # never read as the separate system Billings Clinic
    name = want[0]
    assert name.startswith("Intermountain Health ") and not name.lower().startswith("billings clinic")
    assert "billings clinic" not in name.lower()
    # its gastroenterology site is the same clinic
    assert I.site("Billings Downtown Clinic Gastroenterology", "") == want


@pytest.mark.parametrize("loc,seg,want", [
    ("Red Lodge Clinic - Family Medicine", "", ("Intermountain Health Red Lodge Clinic", "Red Lodge", "MT")),
    ("Heights Clinic", "", ("Intermountain Health Heights Clinic", "Billings", "MT")),
    ("Clinic Pediatrics - Tenaya", "", ("Intermountain Health Tenaya Clinic", "Las Vegas", "NV")),
    ("AmberCreek", "", ("Intermountain Health Amber Creek Clinic", "Thornton", "CO")),
    ("Superior Clinic", "", ("Intermountain Health Superior Clinic", "Superior", "CO")),
    ("Intermountain Health Intermountain Medical Center", "", ("Intermountain Medical Center", "Murray", "UT")),
    ("Intermountain Health McKay-Dee Hospital", "", ("McKay-Dee Hospital", "Ogden", "UT")),
    ("Intermountain Health McKay Dee Hospital", "", ("McKay-Dee Hospital", "Ogden", "UT")),
    ("TOSH", "", ("TOSH The Orthopedic Specialty Hospital", "Murray", "UT")),
    ("San Juan Cancer Center JV", "", ("San Juan Cancer Center", "Montrose", "CO")),
    # offices, home services, bases, buildings: the label with the site's town
    ("Peaks Regional Office", "", (S, "Broomfield", "CO")),
    ("Home Services - Salt Lake City", "", (S, "South Jordan", "UT")),
    ("Valley Center Tower", "", (S, "Murray", "UT")),
    ("MGBL West End MOB - Billings", "", (S, "Billings", "MT")),
    ("Billings Medical Group", "", (S, "Billings", "MT")),
    ("Elko Air Base", "", (S, "Elko", "NV")),
    ("SelectHealth - Idaho", "", (S, "Meridian", "ID")),
    ("Nevada Children's Hospital", "", (S, "Las Vegas", "NV")),
    ("St George Regional at 400 E", "", (S, "St George", "UT")),
])
def test_sites(loc, seg, want):
    assert I.site(loc, f"/job/{seg}/X_R1" if seg else "") == want


def test_two_sites_take_the_primary_site_when_it_is_listed():
    loc = "Intermountain Health St Vincent Regional Hospital; Billings Downtown Clinic"
    assert I.site(loc, "/job/Billings-Downtown-Clinic/Cardiology-Electrophysiologist_R78744") == \
        ("Intermountain Health Billings Downtown Clinic", "Billings", "MT")
    assert I.site(loc, "/job/Intermountain-Health-St-Vincent-Regional-Hospital/Physician_R114430") == \
        ("St. Vincent Regional Hospital", "Billings", "MT")
    # a primary that is not one of the two: the label (the adapter fills the town from the bullets)
    assert I.site(loc, "/job/Heights-Clinic/X_R1") == (S, "", "")
    # a primary that is an office: the label in its town
    assert I.site("Elko Air Base; Ely Air Base", "/job/Elko-Air-Base/X_R1") == (S, "Elko", "NV")


def test_three_or_more_sites_keep_the_label_in_the_primary_town():
    loc = "Saint Joseph Hospital; Platte Valley Hospital; Good Samaritan Hospital; Lutheran Medical Center"
    assert I.site(loc, "/job/Lutheran-Medical-Center/CT-Technologist-Float-Pool_R187266") == (S, "Wheat Ridge", "CO")
    assert I.site("3 Locations", "/job/Lutheran-Medical-Center/X_R1") == (S, "Wheat Ridge", "CO")
    # the primary not listed (an edited posting) or unknown: no town of the module's own
    assert I.site(loc, "/job/Intermountain-Health-St-George-Regional-Hospital/X_R1") == (S, "", "")
    assert I.unknown(loc, "/job/Lutheran-Medical-Center/X_R1") == ""


def test_an_unknown_site_is_not_guessed():
    assert I.site("Red Rock Medical Center", "") is None
    assert I.unknown("Red Rock Medical Center", "") == "Red Rock Medical Center"
    assert I.site("Some Office Building", "") is None
    assert I.unknown("Some Office Building", "") == ""


def test_names_are_clean_and_branded():
    exceptions = {"San Juan Cancer Center", "Platte Valley Hospital OB-GYN", "Primary Children's Outpatient Clinic"}
    for name in I.SITE_NAMES:
        assert name == name.strip() and "  " not in name and "(" not in name, name
        assert name.startswith("Intermountain Health ") or name in exceptions, name
        assert "Billings Clinic" not in name, name
    assert not (I.SITE_NAMES & I.HOSPITAL_NAMES)
    for key, (name, city, state, source) in FW.TABLES[S].items():
        assert city and state and source, key
        assert name is None or name in I.SITE_NAMES or name in I.HOSPITAL_NAMES, name


# ── the float / remote rule ─────────────────────────────────────────────────

@pytest.mark.parametrize("title,name,keeps", [
    ("Medical Assistant Float Pool", "Intermountain Health Utah Valley Clinic", True),
    ("Registered Nurse Central Market Clinic Float Pool", "Intermountain Health Salt Lake Clinic", True),
    ("Advanced Practice Provider-PRN Float Covering South Market", "Intermountain Health Mountain Family Health Clinic", True),
    ("Physician OBGYN OB Hospitalist HYBRID", "Intermountain Health Southridge Clinic", True),
    ("Medical Assistant- Salt Lake Clinic- Multiple Specialties", "Intermountain Health Salt Lake Clinic", False),
    ("Medical Assistant Float Pool", "McKay-Dee Hospital", False),           # a hospital's own float pool
    ("Patient Service Representative", "Intermountain Health Billings Downtown Clinic", False),
    # release 7 review: Home InstaCare treats patients in their homes (home-based)
    ("Advanced Practice Provider Home InstaCare", "Intermountain Health Wynn Clinic", True),
    ("Advanced Practice Provider InstaCare", "Intermountain Health Utah Valley InstaCare", False),
])
def test_keep_label(title, name, keeps):
    assert I.keep_label(name, title) is keeps


def test_apply_facilities_puts_a_clinic_float_pool_back_on_the_label():
    base = dict(hospital_system=S, city="Provo", state="UT", location="Utah Valley Clinic", url=URL.format("Utah-Valley-Clinic"),
                job_id="R1", ats_platform="Workday", specialty="", job_type="", posted_date="", description="")
    pool = scraper.Job(title="Medical Assistant Float Pool", hospital_name="Intermountain Health Utah Valley Clinic", **base)
    keep = scraper.Job(title="Medical Assistant", hospital_name="Intermountain Health Utah Valley Clinic",
                       **{**base, "job_id": "R2"})
    other = scraper.Job(title="RN Float Pool", hospital_name="Mercy Hospital", **{**base, "hospital_system": "Mercy",
                                                                                  "job_id": "R3"})
    stats = I.apply_facilities([pool, keep, other])
    assert pool.hospital_name == S and (pool.city, pool.state) == ("Provo", "UT")
    assert keep.hospital_name == "Intermountain Health Utah Valley Clinic"
    assert other.hospital_name == "Mercy Hospital"
    assert stats[S] == {"pool or remote title: label": 1, "clinic or center": 1}


# ── stored rows (the change list) ───────────────────────────────────────────

def test_stored_decision():
    d = I.stored_decision(URL.format("Billings-Downtown-Clinic"), "Patient Service Representative",
                          _body(["Billings Downtown Clinic"], "Billings", "Montana"))
    assert d == ("Intermountain Health Billings Downtown Clinic", "Billings", "MT", "clinic or center")
    # two sites: the primary site (the url's segment)
    d = I.stored_decision(URL.format("Billings-Downtown-Clinic"), "Urologist",
                          _body(["Billings Downtown Clinic", "Intermountain Health St Vincent Regional Hospital"],
                                "Billings", "Montana"))
    assert d[:3] == ("Intermountain Health Billings Downtown Clinic", "Billings", "MT")
    # several sites: the label in the primary site's town
    d = I.stored_decision(URL.format("Intermountain-Health-Intermountain-Medical-Center"), "Nutrition Care Associate",
                          _body(["Intermountain Health Alta View Hospital", "Intermountain Health Intermountain Medical Center",
                                 "Intermountain Health LDS Hospital"], "Murray", "Utah"))
    assert d == (S, "Murray", "UT", "multi-site posting (3 sites)")
    # an edited posting whose primary is no longer listed: the Work City
    d = I.stored_decision(URL.format("Intermountain-Health-St-George-Regional-Hospital"), "RN Travel Team",
                          _body(["Intermountain Health LDS Hospital", "Intermountain Health Park City Hospital",
                                 "Intermountain Health Riverton Hospital"], "Riverton", "Utah"))
    assert d[:3] == (S, "Riverton", "UT")
    # the single site the body names wins over the url's (an edited posting)
    d = I.stored_decision(URL.format("Logan-Dialysis-Center"), "Registered Nurse Dialysis",
                          _body(["Intermountain Health Logan Regional Hospital"], "Logan", "Utah"))
    assert d[:3] == ("Logan Regional Hospital", "Logan", "UT")
    # a body without the block: the url's site
    d = I.stored_decision(URL.format("Intermountain-Health-American-Fork-Hospital"), "MRI Technologist PRN",
                          "Location: American Fork Hospital\n\nAdditional Details: shifts at two sites.")
    assert d[:3] == ("American Fork Hospital", "American Fork", "UT")
    # a clinic float pool
    d = I.stored_decision(URL.format("Utah-Valley-Clinic"), "Medical Assistant Float Pool",
                          _body(["Utah Valley Clinic"], "Provo", "Utah"))
    assert d == (S, "Provo", "UT", "pool, market, remote or hybrid title")
    # a recruiter's free text after "Location:" is not the board's block
    assert I.body_sites("Location:\n1760 N 2200 W\n\nSuite 40\n\nSalt Lake City, 84116\n\nWork City:\nSalt Lake City") == []
    assert I.work_town(_body(["X"], "Saint George", "Utah")) == ("St George", "UT")


# ── through the adapter (real pages) ────────────────────────────────────────

@pytest.mark.parametrize("key,want", [
    ("billings", {
        "R78744": ("Intermountain Health Billings Downtown Clinic", "Billings", "MT"),     # 2 sites, primary the clinic
        "R114430": ("St. Vincent Regional Hospital", "Billings", "MT"),                     # 2 sites, primary the hospital
        "R183224": ("Intermountain Health Heights Clinic", "Billings", "MT"),
        "R185512": ("Intermountain Health Broadwater Clinic", "Billings", "MT"),
        "R187340": ("Intermountain Health Red Lodge Clinic", "Red Lodge", "MT"),
        "R179420": (S, "Billings", "MT"),                                                     # a medical office building
        "R180971": (S, "Billings", "MT"),                                                     # the medical group
        "R177922": ("St. Vincent Regional Hospital", "Billings", "MT"),
        "R187979": ("St. Vincent Regional Hospital", "Billings", "MT"),
    }),
    ("float_pool", {
        "R122337": (S, "St George", "UT"),            # five hospitals: a resource pool (was St. George Regional Hospital)
        "R187266": (S, "Wheat Ridge", "CO"),          # four hospitals (was Lutheran Medical Center, the last one listed)
        "R177971": (S, "Broomfield", "CO"),
        "R106130": (S, "Provo", "UT"),
        "R185904": (S, "Provo", "UT"),                # a clinic float pool
        "R186665": (S, "Salt Lake City", "UT"),       # "Central Market Clinic Float Pool"
        "R181061": ("Intermountain Health Salt Lake Clinic", "Salt Lake City", "UT"),   # "Multiple Specialties"
        "R185827": ("McKay-Dee Hospital", "Ogden", "UT"),                                 # a hospital's own float pool
        "R188239": ("Primary Children's Hospital", "Salt Lake City", "UT"),
        "R186247": (S, "Ogden", "UT"),                # a medical building
    }),
])
def test_real_pages_through_the_adapter_and_finalize(monkeypatch, key, want):
    jobs = _crawl(monkeypatch, key)
    rows = {r["job_id"]: r for r in scraper.finalize_jobs(jobs)}
    for jid, (name, city, state) in want.items():
        d = rows[jid]
        assert (d["hospital_name"], d["city"], d["state"]) == (name, city, state), jid
        assert d["hospital_system"] == S
