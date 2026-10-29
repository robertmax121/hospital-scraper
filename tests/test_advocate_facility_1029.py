"""2026-10-29: Advocate Health rows name their facility and its real town
(advocate_facilities, WD_SITE_FACILITY).

Fixtures (tests/fixtures/advocate1029/) are real payloads read from
aah.wd5.myworkdayjobs.com (site External) on 2026-10-29:
  aah_list_page.json   the first CXS list page (20 postings: locationsText,
                       externalPath, bulletFields; three "2 Locations")
  aah_locations.json   the board's whole "locations" facet that day (680
                       descriptors with their posting counts)
  aah_details.json     five CXS detail payloads (jobPostingInfo.location,
                       additionalLocations, jobRequisitionLocation; bodies
                       cut to 500 characters): R272624 (Advocate Sherman
                       Hospital, the owner's example), two "2 Locations"
                       postings, an Aurora and an Atrium posting
No network, no database."""
import asyncio
import json
import os
import re

import pytest

import advocate_facilities as A
import scraper
from test_wd_tenant_facets import _fake_req

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "advocate1029")
SYSTEM = "Advocate Health"


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


LOCATIONS = _load("aah_locations.json")["values"]
PAGE = _load("aah_list_page.json")
DETAILS = _load("aah_details.json")["details"]
TOWN_TAIL = re.compile(r",\s*([A-Za-z .'-]+),\s*([A-Z]{2})\s*$")
TOWN_HEAD = re.compile(r"^[A-Za-z .'-]+,\s*[A-Z]{2}\s+-\s+")


# ── the table ───────────────────────────────────────────────────────────────

def test_the_system_label_is_the_workday_tenant():
    assert A.SYSTEM == SYSTEM
    assert scraper.WORKDAY_TENANTS[SYSTEM] == ("aah", "5", "External")
    # 2026-10-29 (facility scale): WD_SITE_FACILITY now also holds the other
    # Workday systems' tables (facilities_workday); Advocate's stays its own.
    assert scraper.WD_SITE_FACILITY[SYSTEM] is A.facility
    assert scraper.WD_SITE_UNKNOWN[SYSTEM] is A.unknown_site
    assert SYSTEM not in scraper.WD_FACILITY_MAP     # no map to fall back to for a site the table does not know


def test_table_entries_are_clean():
    states = set(scraper._WD_STATE_CODES)
    for key, (name, city, state, source) in A.FACILITIES.items():
        assert key == re.sub(r"[^a-z0-9]", "", key)
        assert name == name.strip() and "  " not in name and name != SYSTEM
        assert not name.lower().startswith("the "), name
        assert "(" not in name and "use only" not in name.lower(), name
        assert state in states and city and city == city.strip(), (name, city, state)
        assert re.match(r"^(?:cms \d{6}|board(?:; cms \d{6})?|address .+)$", source), source
    for key, (city, state) in A.OFFICES.items():
        assert key not in A.FACILITIES and state in states and city


def test_one_spelling_per_facility_and_one_town_per_name():
    by_name = {}
    for name, city, state, _src in A.FACILITIES.values():
        by_name.setdefault(name, set()).add((city, state))
    assert all(len(v) == 1 for v in by_name.values()), {k: v for k, v in by_name.items() if len(v) > 1}
    # case and punctuation aside, no two names are the same facility
    keys = [re.sub(r"[^a-z0-9]", "", n.lower()) for n in by_name]
    assert len(keys) == len(set(keys))
    assert A.names() == sorted(by_name)


def test_board_town_is_the_descriptor_town():
    """A site whose descriptor names its town keeps the board's own spelling
    ("Winston Salem"), so no row whose town was already right moves."""
    seen = 0
    for v in LOCATIONS:
        d = v["descriptor"]
        hit = A.facility(d)
        m = TOWN_TAIL.search(d)
        if hit and m and hit[0] != SYSTEM:
            seen += 1
            assert (hit[1], hit[2]) == (" ".join(m.group(1).split()), m.group(2)), d
    assert seen == 32       # 2026-10-29: every Atrium / Wake Forest Baptist site but Hugh Chatham


# ── the whole board of 2026-10-29 ──────────────────────────────────────────

def test_every_location_on_the_board_is_known_or_a_town():
    unknown = [v["descriptor"] for v in LOCATIONS if not A.facility(v["descriptor"]) and A.unknown_site(v["descriptor"])]
    assert unknown == []
    named = [v for v in LOCATIONS if not TOWN_HEAD.match(v["descriptor"])]
    places = [v["descriptor"] for v in named if not A.facility(v["descriptor"])]
    # what is left unnamed: Remote and the recruiting areas ("Kenosha County, WI", "Chicago North, IL")
    assert "Remote" in places
    assert all(p == "Remote" or re.search(r",\s*[A-Z]{2},?$", p) for p in places), places
    # every table key is a site the board listed that day
    keys = {A.site_key(v["descriptor"]) for v in LOCATIONS}
    assert set(A.FACILITIES) <= keys and set(A.OFFICES) <= keys


def test_counts_on_the_board():
    fac = sum(v["count"] for v in LOCATIONS if (A.facility(v["descriptor"]) or ("",))[0] not in ("", SYSTEM))
    office = sum(v["count"] for v in LOCATIONS if (A.facility(v["descriptor"]) or ("",))[0] == SYSTEM)
    assert len(LOCATIONS) == 680
    assert (fac, office) == (4135, 35)
    assert len({A.facility(v["descriptor"])[0] for v in LOCATIONS if A.facility(v["descriptor"])} - {SYSTEM}) == 63


def test_town_clinics_keep_the_system_and_their_parse():
    for v in LOCATIONS:
        d = v["descriptor"]
        if TOWN_HEAD.match(d):
            assert A.facility(d) is None, d
            assert A.unknown_site(d) == "", d


# ── the owner's example and the payload shapes ─────────────────────────────

def test_r272624_is_advocate_sherman_hospital_in_elgin():
    info = DETAILS["sherman_R272624"]["jobPostingInfo"]
    assert info["jobReqId"] == "R272624"
    assert info["location"] == "Advocate Sherman Hospital - 1425 N Randall Rd"
    assert "Elgin, IL" in info["jobDescription"]
    path = info["externalUrl"].split("/External", 1)[1]
    want = ("Advocate Sherman Hospital", "Elgin", "IL")
    assert A.facility(info["location"], path) == want
    assert A.facility("", path) == want
    assert A.facility("2 Locations", path) == want


def test_the_path_segment_is_the_primary_location():
    """A "2 Locations" posting names its primary site only in the path; the
    detail payload's location is that site, the others are
    additionalLocations."""
    for k in ("2loc_R276221", "2loc_R282077"):
        info = DETAILS[k]["jobPostingInfo"]
        assert info["additionalLocations"]
        seg = A.path_segment(info["externalUrl"].split("/External", 1)[1])
        assert A.segment_key(seg) == A.site_key(info["location"])
    # R282077's primary is a Libertyville clinic; its second site, Condell, is not where it is filed
    info = DETAILS["2loc_R282077"]["jobPostingInfo"]
    assert info["additionalLocations"] == ["Advocate Condell Medical Center - 801 S Milwaukee Ave"]
    assert A.facility("2 Locations", info["externalUrl"].split("/External", 1)[1]) is None


def test_descriptor_and_segment_agree_on_every_listed_posting():
    for p in PAGE["jobPostings"]:
        loc, ext = p["locationsText"], p["externalPath"]
        if A._N_LOCATIONS_RX.match(loc):
            continue
        assert A.site_key(loc) == A.segment_key(A.path_segment(ext)), (loc, ext)
        assert A.facility(loc, ext) == A.facility("2 Locations", ext) == A.facility("", ext)


@pytest.mark.parametrize("seg,want", [
    ("Advocate-Christ-Hospital--Medical-Center---4440-W-95th-St", ("Advocate Christ Medical Center", "Oak Lawn", "IL")),
    ("Advocate-Childrens-Hospital---Park-Ridge---1675-Dempster-St",
     ("Advocate Children's Hospital Park Ridge", "Park Ridge", "IL")),
    ("Advocate-Childrens-Hospital---4440-W-95th-St", ("Advocate Children's Hospital Oak Lawn", "Oak Lawn", "IL")),
    ("Aurora-Medical-Center-Oshkosh-Fond-du-Lac-Campus---210-Wisconsin-American-Dr",
     ("Aurora Medical Center Fond du Lac", "Fond du Lac", "WI")),
    ("Aurora-Medical-Center-Oshkosh---855-N-Westhaven-Dr", ("Aurora Medical Center Oshkosh", "Oshkosh", "WI")),
    ("Aurora-Medical-Center-Sheboygan-County---3400-Union-Ave",
     ("Aurora Medical Center Sheboygan County", "Sheboygan", "WI")),
    ("Aurora-St-Lukes-Medical-Center---2900-W-Oklahoma-Ave", ("Aurora St. Luke's Medical Center", "Milwaukee", "WI")),
    ("Aurora-St-Lukes-South-Shore---5900-S-Lake-Dr", ("Aurora St. Luke's South Shore", "Cudahy", "WI")),
    ("Atrium-Health-Union-West---1000-Healing-Way-Matthews-NC", ("Atrium Health Union West", "Matthews", "NC")),
    ("Atrium-Health-Union---600-Hospital-Dr-Monroe-NC", ("Atrium Health Union", "Monroe", "NC")),
    ("Atrium-Health-Wake-Forest-Baptist---Medical-Center-Blvd-Winston-Salem-NC",
     ("Atrium Health Wake Forest Baptist Medical Center", "Winston Salem", "NC")),
    ("High-Point-Medical-Center---601-N-Elm-St-High-Point-NC",
     ("Atrium Health Wake Forest Baptist High Point Medical Center", "High Point", "NC")),
    ("Scotland-Memorial-Hospital-Advocate-Health-Use-Only---500-Lauchwood-Dr-Laurinburg-NC",
     ("Scotland Memorial Hospital", "Laurinburg", "NC")),
    ("Hugh-Chatham-Memorial-Hospital---180-Parkwood-Dr", ("Hugh Chatham Memorial Hospital", "Elkin", "NC")),
    ("AAO-Oak-Brook---2025-Windsor-Dr", (SYSTEM, "Oak Brook", "IL")),
    ("AAO-Milwaukee---750-W-Virginia-St", (SYSTEM, "Milwaukee", "WI")),
    ("Charlotte-NC---1237-Harding-Pl", None),
    ("Aurora-IL---1221-N-Highland-Ave", None),                 # the town of Aurora, IL: no facility
    ("Milwaukee-WI---2801-W-Kinnickinnic-River-Pkwy--2801-W-Kinnickinnic-River-Pkwy", None),
    ("Remote", None),
    ("", None),
])
def test_segments(seg, want):
    assert A.facility("2 Locations", f"/job/{seg}/Some-Title_R1" if seg else "") == want


def test_an_unknown_named_site_is_reported_not_guessed():
    d = "Advocate New Pavilion Hospital - 1 Main St"
    assert A.facility(d) is None
    assert A.unknown_site(d) == "Advocate New Pavilion Hospital"
    assert A.unknown_site("2 Locations", "/job/Advocate-New-Pavilion-Hospital---1-Main-St/RN_R1") == \
        "Advocate New Pavilion Hospital"
    for d in ("Chicago, IL - 3000 N Halsted St", "Remote", "Kenosha County, WI", "Chicago Southeast, IL,",
              "Milwaukee, WI - 2801 W Kinnickinnic River Pkwy - 2801 W Kinnickinnic River Pkwy"):
        assert A.unknown_site(d) == "", d
    assert A.unknown_site("2 Locations", "/job/Crystal-Lake-IL---525-E-Congress-Pkwy/MA_R276221") == ""


# ── through the adapter and normalize_job ──────────────────────────────────

def _crawl(monkeypatch, system):
    _fake_req(monkeypatch, lambda b: PAGE if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(PAGE["jobPostings"]), "jobPostings": []})
    return asyncio.run(scraper.scrape_workday(None, system, ("aah", "5", "External")))


def test_scrape_workday_stores_the_facility_and_its_town(monkeypatch):
    jobs = _crawl(monkeypatch, SYSTEM)
    assert len(jobs) == len(PAGE["jobPostings"]) == 20
    by_id = {j.job_id: j for j in jobs}
    want = {
        "R270122": ("Advocate Lutheran General Hospital", "Park Ridge", "IL"),
        "R278719": ("Advocate Condell Medical Center", "Libertyville", "IL"),
        "R282852": ("Advocate Trinity Hospital", "Chicago", "IL"),
        "R284599": ("Aurora St. Luke's Medical Center", "Milwaukee", "WI"),
        "R239684": ("Advocate Children's Hospital Oak Lawn", "Oak Lawn", "IL"),
        "R274192": ("Atrium Health Wake Forest Baptist Medical Center", "Winston Salem", "NC"),
        "R282162": ("Advocate Good Shepherd Hospital", "Barrington", "IL"),
        "R277666": ("Atrium Health Cleveland", "Shelby", "NC"),
        "R284593": ("Aurora West Allis Medical Center", "West Allis", "WI"),
    }
    for jid, (name, city, state) in want.items():
        j = by_id[jid]
        assert (j.hospital_system, j.hospital_name, j.city, j.state) == (SYSTEM, name, city, state), jid
        d = scraper.normalize_job(j)
        assert (d["hospital_system"], d["hospital_name"], d["city"], d["state"], d["location"]) == \
            (SYSTEM, name, city, state, f"{city}, {state}"), jid
    # a "Town, ST - street" clinic keeps the system label and its own town
    for jid, town in (("R249163", ("Chicago", "IL")), ("R274286", ("Chicago", "IL")), ("R210571", ("Macon", "GA"))):
        j = by_id[jid]
        assert (j.hospital_name, j.city, j.state) == (SYSTEM,) + town, jid
    # every named-site posting on the page was named; nothing else was
    named = {p["bulletFields"][0] for p in PAGE["jobPostings"]
             if A.facility(p["locationsText"], p["externalPath"])}
    assert {j.job_id for j in jobs if j.hospital_name != SYSTEM} == named


def test_other_workday_tenants_are_untouched(monkeypatch):
    # 2026-10-29 (facility scale): Sutter Health has a town table now
    # (facilities_workday); a tenant with no table keeps every row on its label.
    jobs = _crawl(monkeypatch, "Fresenius Medical Care")
    assert jobs and {j.hospital_name for j in jobs} == {"Fresenius Medical Care"}
    assert "Fresenius Medical Care" not in scraper.WD_SITE_FACILITY
