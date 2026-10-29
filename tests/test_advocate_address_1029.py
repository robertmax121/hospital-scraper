"""2026-10-29: Advocate Health "Town, ST - street" board sites that are a
hospital's own campus name that hospital (advocate_facilities.ADDRESS_SITES).

The owner, 2026-10-29: "Advocate health is not complete. simply check the
known hospitals and their open roles vs the total open jobs for the system".
After the named-site table, 1,916 active rows still read "Advocate Health":
the board names 597 sites by town and street alone. The 17 whose street
address is the hospital building itself or a building on its campus (on
sourced evidence: campus maps, the hospital's own page, the system's
location pages and job postings, the postings' own text) now name the
hospital; every other address site (clinics, medical groups, offices,
freestanding EDs, hospices, labs, Remote, areas) keeps the system label.

Fixtures (tests/fixtures/advocate1029/), real payloads read from
aah.wd5.myworkdayjobs.com (site External) on 2026-10-29:
  aah_locations.json            the board's whole "locations" facet
  aah_address_sites_lists.json  two CXS list pages filtered by the locations
                                facet (35 postings at 1001 Blythe Blvd, 2801
                                W Kinnickinnic River Pkwy, 1 and 342 Medical
                                Center Blvd, 1237 Harding Pl, 10650 Park Rd,
                                275 Beatty Dr, 1435 N Randall Rd, 200 Medical
                                Park Dr; "2 Locations" / "4 Locations" ones
                                whose primary site is in the path only)
No network, no database."""
import asyncio
import json
import os
import re

import advocate_facilities as A
import scraper
from test_wd_tenant_facets import _fake_req

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "advocate1029")
SYSTEM = "Advocate Health"


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


LOCATIONS = _load("aah_locations.json")["values"]
LISTS = _load("aah_address_sites_lists.json")["pages"]
POSTINGS = [p for page in LISTS for p in page["jobPostings"]]
HEAD = re.compile(r"^([A-Za-z .'-]+),\s*([A-Z]{2})\s+-\s+")
NEW_HOSPITALS = {"Atrium Health Carolinas Rehabilitation": ("Charlotte", "NC"),
                 "Carolinas Rehabilitation Mount Holly": ("Belmont", "NC")}


# ── the table ───────────────────────────────────────────────────────────────

def test_every_entry_is_a_town_street_site_on_the_board_in_its_own_town():
    by_key = {A.address_key(v["descriptor"]): v["descriptor"] for v in LOCATIONS}
    for key, (name, city, state, source) in A.ADDRESS_SITES.items():
        assert key == re.sub(r"[^a-z0-9]", "", key)
        d = by_key[key]                                    # a descriptor the board listed that day
        m = HEAD.match(d)
        assert m, d                                        # a "Town, ST - street" site, not a named one
        assert (city, state) == (" ".join(m.group(1).split()), m.group(2)), d   # the town as given
        assert A.site_key(d) not in A.FACILITIES and A.site_key(d) not in A.OFFICES, d
        assert name != SYSTEM and name == name.strip() and "  " not in name, name
        assert source and source == source.strip(), key


def test_names_are_the_known_hospitals_in_their_own_towns():
    """A campus address names a hospital the named-site table already has,
    in that hospital's town; the two exceptions are rehabilitation hospitals
    the board never names as a site (1100 Blythe Blvd on the CMC campus, 275
    Beatty Dr in Belmont)."""
    known = {n: (c, s) for n, c, s, _src in A.FACILITIES.values()}
    for name, city, state, _src in A.ADDRESS_SITES.values():
        assert known.get(name, NEW_HOSPITALS.get(name)) == (city, state), name
    assert {n for n, *_ in A.ADDRESS_SITES.values()} - set(known) == set(NEW_HOSPITALS)
    assert set(NEW_HOSPITALS) <= set(A.names())


def test_counts_on_the_board_of_2026_10_29():
    campus = [v for v in LOCATIONS if A.address_key(v["descriptor"]) in A.ADDRESS_SITES]
    assert len(campus) == 17
    assert sum(v["count"] for v in campus) == 250
    per = {}
    for v in campus:
        n = A.facility(v["descriptor"])[0]
        per[n] = per.get(n, 0) + v["count"]
    assert per["Atrium Health Carolinas Medical Center"] == 44 + 35 + 31
    assert per["Atrium Health Wake Forest Baptist Medical Center"] == 18 + 12
    assert per["Aurora St. Luke's Medical Center"] == 24


def test_neighbours_and_lookalikes_stay_unnamed():
    """Never a nearby or similar address: the clinic buildings next to a
    campus, other numbers on a hospital's street, and the exact addresses of
    hospitals that are not Advocate's keep the system label."""
    for d in ("Charlotte, NC - 1237 Harding Pl", "Charlotte, NC - 1225 Harding Pl", "Charlotte, NC - 1350 S Kings Dr",
              "Charlotte, NC - 10650 Park Rd", "Charlotte, NC - 10620 Park Rd", "Charlotte, NC - 101 E WT Harris Blvd",
              "Winston Salem, NC - 342 Medical Center Blvd", "Winston Salem, NC - 300 Medical Center Blvd",
              "Winston Salem, NC - 1920 W 1st St", "Park Ridge, IL - 1875 Dempster St", "Park Ridge, IL - 1775 Ballard Rd",
              "Elgin, IL - 1710 N Randall Rd", "Oak Lawn, IL - 4400 W 95th St", "Hazel Crest, IL - 17850 Kedzie Ave",
              "Chicago, IL - 2301 E 93rd St", "Macon, GA - 770 Hemlock St", "Concord, NC - 100 Medical Park Dr",
              "Concord, NC - 401 Medical Park Dr", "Statesville, NC - 557 Brookdale Dr", "Milwaukee, WI - 1525 N 12th St",
              "Mount Pleasant, WI - 8400 Washington Ave", "Charlotte, NC - 4828 Airport Center Pkwy",
              "Oak Brook, IL - 2311 W 22nd St", "Remote"):
        assert A.facility(d) is None, d
        assert A.unknown_site(d) == "", d
    # a town alone, or a street the table knows in another town, is not a campus
    assert A.facility("Charlotte, NC") is None
    assert A.facility("Concord, NC - 1001 Blythe Blvd") is None
    assert A.facility("Charlotte, NC - 1001 Blythe Blvd Suite 2") is None


def test_descriptor_and_path_segment_give_the_same_answer():
    for p in POSTINGS:
        loc, ext = p["locationsText"], p["externalPath"]
        if A._N_LOCATIONS_RX.match(loc):
            continue
        assert A.address_key(loc) == A.address_key("2 Locations", ext), (loc, ext)
        assert A.facility(loc, ext) == A.facility("2 Locations", ext) == A.facility("", ext), (loc, ext)


# ── the real payloads, through the adapter and normalize_job ────────────────

def _crawl(monkeypatch):
    seen, page = set(), []
    for p in POSTINGS:
        if p["bulletFields"][0] not in seen:
            seen.add(p["bulletFields"][0])
            page.append(p)
    data = {"total": len(page), "jobPostings": page}
    _fake_req(monkeypatch, lambda b: data if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(page), "jobPostings": []})
    return page, asyncio.run(scraper.scrape_workday(None, SYSTEM, ("aah", "5", "External")))


def test_scrape_workday_names_the_campus_hospital(monkeypatch):
    page, jobs = _crawl(monkeypatch)
    assert len(jobs) == len(page) == 35
    by_id = {j.job_id: j for j in jobs}
    want = {
        "R279851": ("Carolinas Rehabilitation Mount Holly", "Belmont", "NC"),           # Belmont, NC - 275 Beatty Dr
        "R282627": ("Carolinas Rehabilitation Mount Holly", "Belmont", "NC"),
        "R279091": ("Advocate Sherman Hospital", "Elgin", "IL"),                        # Elgin, IL - 1435 N Randall Rd
        "R272043": ("Atrium Health Cabarrus", "Concord", "NC"),                         # Concord, NC - 200 Medical Park Dr
        "R278944": ("Aurora St. Luke's Medical Center", "Milwaukee", "WI"),             # 2801 W Kinnickinnic River Pkwy
        "R280318": ("Atrium Health Wake Forest Baptist Medical Center", "Winston Salem", "NC"),   # 1 Medical Center Blvd
        "R277215": ("Atrium Health Carolinas Medical Center", "Charlotte", "NC"),       # Charlotte, NC - 1001 Blythe Blvd
        # "2 Locations" / "4 Locations": the primary site is the path's first segment
        "R260624": ("Atrium Health Cabarrus", "Concord", "NC"),
        "R274810": ("Advocate Sherman Hospital", "Elgin", "IL"),
        "R268350": ("Carolinas Rehabilitation Mount Holly", "Belmont", "NC"),
        "R177341": ("Atrium Health Carolinas Rehabilitation", "Charlotte", "NC"),       # /job/Charlotte-NC---1100-Blythe-Blvd/
        "R252652": ("Atrium Health Carolinas Medical Center", "Charlotte", "NC"),
    }
    for jid, (name, city, state) in want.items():
        j = by_id[jid]
        assert (j.hospital_system, j.hospital_name, j.city, j.state) == (SYSTEM, name, city, state), jid
        d = scraper.normalize_job(j)
        assert (d["hospital_system"], d["hospital_name"], d["city"], d["state"], d["location"]) == \
            (SYSTEM, name, city, state, f"{city}, {state}"), jid
    # the neighbours on the same pages keep the system label and their own town
    for jid, town in (("R265637", ("Charlotte", "NC")),      # Charlotte, NC - 1237 Harding Pl
                      ("R280897", ("Charlotte", "NC")),      # Charlotte, NC - 10650 Park Rd
                      ("R269074", ("Winston Salem", "NC")),  # Winston Salem, NC - 342 Medical Center Blvd
                      ("R280107", None)):                    # 6 Locations, primary Remote
        j = by_id[jid]
        assert j.hospital_name == SYSTEM, jid
        if town:
            assert (j.city, j.state) == town, jid
    # every posting the table names was named; nothing else was
    named = {p["bulletFields"][0] for p in page if A.facility(p["locationsText"], p["externalPath"])}
    assert {j.job_id for j in jobs if j.hospital_name != SYSTEM} == named
    assert len(named) == 26      # 35 postings less 1237 Harding Pl (3), 10650 Park Rd (3), 342 Medical Center Blvd (2), Remote (1)


def test_the_town_was_already_the_descriptor_town(monkeypatch):
    """The campus table changes the name only: each posting's town is the
    one its "Town, ST" descriptor (or path segment) already gave."""
    page, jobs = _crawl(monkeypatch)
    by_id = {j.job_id: j for j in jobs}
    for p in page:
        hit = A.ADDRESS_SITES.get(A.address_key(p["locationsText"], p["externalPath"]))
        if not hit:
            continue
        seg = A.path_segment(p["externalPath"])
        m = re.match(r"^(.+?)-([A-Z]{2})---", seg)
        assert m, seg
        assert (hit[1].replace(" ", "-"), hit[2]) == (m.group(1), m.group(2)), seg
        j = by_id[p["bulletFields"][0]]
        assert (j.city, j.state) == (hit[1], hit[2])
