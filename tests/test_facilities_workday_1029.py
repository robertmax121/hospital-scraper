"""2026-10-29 (facility scale, Workday family): rows of a multi-hospital
Workday system name their hospital and its real town (facilities_workday,
WD_SITE_FACILITY), and the head-office override no longer moves them.

Fixtures (tests/fixtures/workday1029/wd_list_pages.json) are real payloads:
the first CXS list page (20 postings) of ten tenants, read 2026-10-29 a few
seconds apart, no login, trimmed to the fields scrape_workday reads
(locationsText, externalPath, bulletFields, title, postedOn, timeType).
The stored links below are real rows of hospital_jobs on 2026-10-29.
No network, no database."""
import asyncio
import json
import os
import re

import pytest

import facilities_workday as F
import scraper
from test_wd_tenant_facets import _fake_req

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "workday1029")
with open(os.path.join(FIX, "wd_list_pages.json"), encoding="utf-8") as _f:
    PAGES = json.load(_f)["pages"]


def _crawl(monkeypatch, system, page=None):
    page = page if page is not None else PAGES[system]
    _fake_req(monkeypatch, lambda b: page if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(page["jobPostings"]), "jobPostings": []})
    tenant = tuple(page["tenant"]) if "tenant" in page else scraper.WORKDAY_TENANTS[system]
    return asyncio.run(scraper.scrape_workday(None, system, tenant))


def _rows(monkeypatch, system):
    return {j.job_id: scraper.normalize_job(j) for j in _crawl(monkeypatch, system)}


# ── the tables ──────────────────────────────────────────────────────────────

def test_every_table_is_a_workday_tenant_and_is_hooked():
    assert len(F.TABLES) >= 45
    for system in F.systems():
        assert system in scraper.WORKDAY_TENANTS, system
        assert scraper.WD_SITE_FACILITY[system] is not None
        assert system in scraper.WD_SITE_UNKNOWN
    for db, system in F.ALSO.items():
        assert system in F.TABLES and scraper.HOSPITAL_SYSTEM_ALIASES.get(db, db) in (db, system) or \
            scraper.HOSPITAL_SYSTEM_ALIASES.get(system) == db, (db, system)
    # Advocate keeps its own module; a tenant with no table is not hooked
    assert scraper.WD_SITE_FACILITY["Advocate Health"].__module__ == "advocate_facilities"
    for system in ("Fresenius Medical Care", "CVS Health", "Option Care Health"):
        assert system not in scraper.WD_SITE_FACILITY


def test_table_entries_are_clean():
    states = set(scraper._WD_STATE_CODES)
    for system, tbl in F.TABLES.items():
        entries = list(tbl.items()) + [(k, e) for k, e in F.PREFIXES.get(system, ())]
        for key, (name, city, state, source) in entries:
            assert key and key == re.sub(r"[^a-z0-9]", "", key), (system, key)
            assert source.strip(), (system, key)
            if not city:                       # an explicit "not placed" entry
                assert not state and name is None, (system, key)
                continue
            assert state in states and city == city.strip() and "  " not in city, (system, key, city, state)
            if name is not None:
                assert name == name.strip() and "  " not in name, name
                assert not name.lower().startswith("the "), name
                assert "(" not in name and "|" not in name, name
                assert name not in (system, F.ALSO.get(system)), name
                assert name.lower() != system.lower(), name


def test_one_town_per_hospital_name():
    for system in F.TABLES:
        towns = {}
        entries = list(F.TABLES[system].values()) + [e for _k, e in F.PREFIXES.get(system, ())]
        for name, city, state, _src in entries:
            if name:
                towns.setdefault(name, set()).add((city, state))
        assert {n: t for n, t in towns.items() if len(t) > 1} == {}, system


def test_the_descriptor_and_its_slug_are_one_key():
    """Every single-site posting on the ten pages is placed the same from its
    locationsText as from its externalPath (the stored link's segment)."""
    n = 0
    for system, page in PAGES.items():
        for p in page["jobPostings"]:
            loc, path = p.get("locationsText") or "", p.get("externalPath") or ""
            if re.match(r"^\d+ Locations?$", loc):
                continue
            assert F.facility(system, loc) == F.facility(system, "2 Locations", path), (system, loc, path)
            n += 1
    assert n >= 150


# ── the town rules ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,want", [
    ("Kalispell, Montana", ("Kalispell", "MT")),
    ("Mt. Pleasant, Iowa", ("Mount Pleasant", "IA")),
    ("Mt-Pleasant-Iowa", ("Mount Pleasant", "IA")),          # parse_city_state read ("Iowa", "MT")
    ("Cut-Bank-Montana", ("Cut Bank", "MT")),
    ("West Burlington, Iowa", ("West Burlington", "IA")),
    ("Remote - South Carolina", None),
    ("Saint Alphonsus Medical Center - Nampa, Idaho", None),  # a facility, not a town
    ("Norfolk, VA", None),                                    # a state code: parse_city_state's job
    ("", None),
])
def test_town_state_name(text, want):
    assert F.town_state_name(text) == want


def test_town_lead_and_town_in_site():
    assert F.town_lead("Moorestown - 401 Young Ave", "NJ") == ("Moorestown", "NJ")
    assert F.town_lead("Glassboro", "NJ") == ("Glassboro", "NJ")
    assert F.town_lead("Lippincott - 301 Lippincott Drive", "NJ") is None     # a street, in Marlton
    assert F.town_lead("200 Bowman Drive", "NJ") is None
    assert F.town_in_site("NCO 15 S Mchenry Rd Buffalo Grove", "IL") == ("Buffalo Grove", "IL")
    assert F.town_in_site("NSO 2151 Waukegan Road Bannockburn", "IL") == ("Bannockburn", "IL")
    assert F.town_in_site("NSO 225 N Milwaukee Ave Vernon Hills", "IL") == ("Vernon Hills", "IL")
    assert F.town_in_site("700 W Lincoln", "IL") is None                       # a street in Charleston, IL
    assert F.town_in_site("Allina Health Coon Rapids Clinic", "MN") == ("Coon Rapids", "MN")
    assert F.town_in_site("Battle Creek 4625 Beckley Rd", "MI") == ("Battle Creek", "MI")
    assert F.town_in_site("Portage 6901 Portage Rd", "MI") == ("Portage", "MI")
    assert F.town_in_site("Business Operations Center", "OH") is None


# ── stored links (real rows, 2026-10-29) ────────────────────────────────────

@pytest.mark.parametrize("system,url,want", [
    ("Endeavor Health", "https://nshs.wd1.myworkdayjobs.com/ns-eeh/job/LOH-Naperville-Main-Hospital/RN_R1",
     ("Endeavor Health Linden Oaks Hospital", "Naperville", "IL")),
    ("Endeavor Health", "https://nshs.wd1.myworkdayjobs.com/ns-eeh/job/EDH-MOB-1-Naperville-100-Spalding-Drive/MA_R2",
     ("Endeavor Health Edward Hospital", "Naperville", "IL")),
    ("Endeavor Health", "https://nshs.wd1.myworkdayjobs.com/ns-eeh/job/NCO-15-S-Mchenry-Rd-Buffalo-Grove/MA_R3",
     ("Endeavor Health", "Buffalo Grove", "IL")),
    ("Cape Fear Valley Health", "https://capefearvalley.wd1.myworkdayjobs.com/CFV/job/BJH/RN_R4",
     ("Cape Fear Valley Betsy Johnson Hospital", "Dunn", "NC")),
    ("Trinity Health", "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/MOQ---Genesis-Medical-Center-Silvis/RN_00690001",
     ("Genesis Medical Center Silvis", "Silvis", "IL")),
    ("Trinity Health", "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Mount-Carmel-New-Albany/Echo_00690872",
     ("Mount Carmel New Albany Surgical Hospital", "New Albany", "OH")),
    ("Cleveland Clinic", "https://ccf.wd1.myworkdayjobs.com/ClevelandClinicCareers/job/Hillcrest-Hospital/RN_1",
     ("Cleveland Clinic Hillcrest Hospital", "Mayfield Heights", "OH")),
    ("Memorial Healthcare System", "https://memorialhealthcare.wd1.myworkdayjobs.com/mhs_careers/job/Memorial-Hospital-West/RN_1",
     ("Memorial Hospital West", "Pembroke Pines", "FL")),
    ("Prisma Health (Providers)", "https://prismahealth.wd5.myworkdayjobs.com/PrismaHealthProviders/job/Blount-Memorial-Hospital/MD_1",
     ("Blount Memorial Hospital", "Maryville", "TN")),
    ("Intermountain Health (IMH)", "https://imh.wd108.myworkdayjobs.com/IntermountainCareers/job/St-Marys-Regional-Hospital/RN_R1",
     ("St. Mary's Regional Hospital", "Grand Junction", "CO")),
    ("Sutter Health", "https://sutterhealth.wd1.myworkdayjobs.com/SH/job/San-Francisco/RN_R1",
     ("Sutter Health", "San Francisco", "CA")),
    ("Logan Health", "https://loganhealth.wd1.myworkdayjobs.com/Logan_Careers/job/Cut-Bank-Montana/LPN_Req1",
     ("Logan Health", "Cut Bank", "MT")),
    ("Virtua Health", "https://virtua.wd1.myworkdayjobs.com/Virtua_Health_External_Career_Site/job/Dover---200-Banning-Street/X_R1",
     None),                                                  # listed, never placed: a Dover in another state
    ("MUSC Health", "https://musc.wd1.myworkdayjobs.com/MUSC/job/Charleston/RN_R1", None),   # a town names no hospital
])
def test_stored_links(system, url, want):
    assert F.facility(system, "2 Locations", url) == want


def test_an_unknown_hospital_site_is_reported_not_guessed():
    assert F.facility("Banner Health", "Banner New Tower Med Ctr (1 Main St)") is None
    assert F.unknown_site("Banner Health", "Banner New Tower Med Ctr (1 Main St)") == \
        "Banner New Tower Med Ctr (1 Main St)"
    assert F.unknown_site("Banner Health", "Remote-Arizona") == ""
    assert F.unknown_site("Banner Health", "Banner Desert Med Ctr (1400 S Dobson Rd)") == ""


# ── through the adapter and normalize_job (real pages) ──────────────────────

@pytest.mark.parametrize("system,want", [
    ("Memorial Hermann", {
        "100187838": ("Memorial Hermann Katy Hospital", "Katy", "TX"),          # FORCE_LOCATION_OVERRIDE said Houston
        "100186409": ("Memorial Hermann Northeast Hospital", "Humble", "TX"),
        "100188269": ("Memorial Hermann Sugar Land Hospital", "Sugar Land", "TX"),
        "100187092": ("Children's Memorial Hermann Hospital", "Houston", "TX"),
        "100188169": ("Memorial Hermann", "Houston", "TX"),                     # a medical plaza: as before
    }),
    ("Logan Health", {
        "Req18014": ("Logan Health", "Whitefish", "MT"),                         # was Kalispell (FORCE)
        "Req18117": ("Logan Health", "Conrad", "MT"),
        "Req11262": ("Logan Health", "Kalispell", "MT"),
    }),
    ("Great River Health", {
        "R-09383": ("Great River Health", "Mount Pleasant", "IA"),               # was West Burlington (FORCE)
        "R-08922": ("Great River Health", "West Burlington", "IA"),
    }),
    ("Virtua Health", {
        "R1064472": ("Virtua Voorhees Hospital", "Voorhees", "NJ"),              # was Marlton (tenant default)
        "R1062268": ("Virtua Our Lady of Lourdes Hospital", "Camden", "NJ"),
        "R1064564": ("Virtua Health", "Moorestown", "NJ"),
        "R1064772": ("Virtua Health", "Cherry Hill", "NJ"),
        "R1064369": ("Virtua Health", "Marlton", "NJ"),                          # Lippincott Drive is in Marlton
    }),
    ("NewYork-Presbyterian", {
        "00885428": ("NewYork-Presbyterian/Weill Cornell Medical Center", "New York", "NY"),
        "00889852": ("NewYork-Presbyterian Hudson Valley Hospital", "Cortlandt Manor", "NY"),
        "00889267": ("NewYork-Presbyterian", "Flushing", "NY"),                  # a medical group: the label
    }),
    ("Endeavor Health", {
        "R34777": ("Endeavor Health Northwest Community Hospital", "Arlington Heights", "IL"),
        "R35329": ("Endeavor Health Swedish Hospital", "Chicago", "IL"),        # SWH entity code
        "R894": ("Endeavor Health Edward Hospital", "Naperville", "IL"),        # "3 Locations": the path's primary site
        "R41419": ("Endeavor Health", "Evanston", "IL"),
    }),
    ("Cleveland Clinic", {
        "358080": ("Cleveland Clinic Akron General", "Akron", "OH"),             # was Cleveland
        "353729": ("Cleveland Clinic Mercy Hospital", "Canton", "OH"),
        "358741": ("Cleveland Clinic", "Solon", "OH"),
        "340128": ("Cleveland Clinic", "Palm City", "FL"),                       # WD_FACILITY_MAP, as before
    }),
    ("Sentara Healthcare", {
        "JR-104974": ("Sentara Norfolk General Hospital", "Norfolk", "VA"),
        "JR-104932": ("Sentara Williamsburg Regional Medical Center", "Williamsburg", "VA"),
        "JR-108693": ("Sentara Healthcare", "Norfolk", "VA"),
    }),
    ("Trinity Health", {
        "00701657": ("Saint Francis Hospital", "Hartford", "CT"),
        "00703674": ("MercyOne Dubuque Medical Center", "Dubuque", "IA"),
        "00703685": ("Samaritan Hospital - St. Mary's Campus", "Troy", "NY"),
        "00703686": ("MercyOne Ankeny Clinic", "Ankeny", "IA"),                  # WD_FACILITY_MAP: the clinic (owner 2026-10-29)
    }),
    ("Bronson Healthcare", {
        "JOB49667": ("Bronson Battle Creek Hospital", "Battle Creek", "MI"),     # was Kalamazoo
        "JOB51494": ("Bronson Methodist Hospital", "Kalamazoo", "MI"),
        "JOB50218": ("Bronson Healthcare", "Portage", "MI"),
    }),
])
def test_real_pages_through_the_adapter(monkeypatch, system, want):
    rows = _rows(monkeypatch, system)
    for jid, (name, city, state) in want.items():
        d = rows[jid]
        assert (d["hospital_name"], d["city"], d["state"], d["location"]) == (name, city, state, f"{city}, {state}"), jid
        assert d["hospital_system"] == system


def test_force_override_still_moves_what_the_table_did_not_place():
    base = dict(title="RN", hospital_system="Memorial Hermann", url="https://x/job/a_1", job_id="1",
                ats_platform="Workday", specialty="", job_type="", posted_date="", description="")
    katy = scraper.normalize_job(scraper.Job(**base, hospital_name="Memorial Hermann Katy Hospital",
                                             city="Katy", state="TX", location="Katy Hospital"))
    assert (katy["city"], katy["state"]) == ("Katy", "TX")
    # a place the table does not give still takes the head office
    other = scraper.normalize_job(scraper.Job(**base, hospital_name="Memorial Hermann", city="Pasadena",
                                              state="TX", location="Some Clinic"))
    wrong = scraper.normalize_job(scraper.Job(**base, hospital_name="Memorial Hermann Katy Hospital",
                                              city="Pasadena", state="TX", location="x"))
    assert (wrong["city"], wrong["state"]) == ("Houston", "TX")
    assert (other["city"], other["state"]) == ("Houston", "TX")
    junk = scraper.normalize_job(scraper.Job(**base, hospital_name="Memorial Hermann Katy Hospital",
                                             city="Medical Plaza North", state="", location="x"))
    assert (junk["city"], junk["state"]) == ("Houston", "TX")
    assert F.placed("Logan Health", "Logan Health", "Whitefish", "MT")
    assert not F.placed("Logan Health", "Logan Health", "Spokane", "MT")      # no such Montana town
    assert not F.placed("Fresenius Medical Care", "Fresenius Medical Care", "Denver", "CO")


def test_the_generic_town_state_rule_on_an_untabled_tenant(monkeypatch):
    page = {"total": 3, "jobPostings": [
        {"title": "RN", "externalPath": "/job/Mt-Pleasant-Iowa/RN_R-10001", "locationsText": "Mt-Pleasant-Iowa",
         "bulletFields": ["R-10001"]},
        {"title": "LPN", "externalPath": "/job/Cut-Bank-Montana/LPN_R-10002", "locationsText": "2 Locations",
         "bulletFields": ["R-10002"]},
        {"title": "MA", "externalPath": "/job/Austin-TX/MA_R-10003", "locationsText": "Austin, TX",
         "bulletFields": ["R-10003"]},
    ]}
    jobs = {j.job_id: j for j in _crawl(monkeypatch, "Plain Board", dict(page, tenant=["plain", "5", "External"]))}
    assert (jobs["R-10001"].city, jobs["R-10001"].state) == ("Mount Pleasant", "IA")   # parse_city_state: ("Iowa", "MT")
    assert (jobs["R-10002"].city, jobs["R-10002"].state) == ("Cut Bank", "MT")         # "2 Locations": the primary site
    assert (jobs["R-10003"].city, jobs["R-10003"].state) == ("Austin", "TX")           # untouched
    assert {j.hospital_name for j in jobs.values()} == {"Plain Board"}


def test_a_tenant_without_a_table_keeps_its_rows(monkeypatch):
    page = dict(PAGES["Sentara Healthcare"], tenant=["freseniusmedicalcare", "3", "fme"])
    jobs = _crawl(monkeypatch, "Fresenius Medical Care", page)
    assert jobs and {j.hospital_name for j in jobs} == {"Fresenius Medical Care"}
