"""2026-10-29: the hospital a posting's own text names, for the systems whose
feed names only the system (facilities_text, the description-text family).

Fixture (tests/fixtures/facility_text/stored_bodies.json): real bodies stored
in hospital_jobs, read 2026-10-29, cut after the line that names the site (a
body whose line sits deep keeps its first 400 characters, "[cut]", then the
lines around it). Each row keeps its stored system, label, town and job id.
No network, no database."""
import json
import os
import re

import pytest

import facilities_text as T
import facility_names as F
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_text")
with open(os.path.join(FIX, "stored_bodies.json"), encoding="utf-8") as _f:
    ROWS = {r["id"]: r for r in json.load(_f)["rows"]}


@pytest.fixture(autouse=True)
def _no_known():
    T.set_known_facilities([])
    T.UNKNOWN.clear()
    yield
    T.set_known_facilities([])
    T.UNKNOWN.clear()


def _job(system, name, city="", state="", description="", job_id="1", title="RN"):
    return Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="", url="https://x/1",
               job_id=job_id, posted_date="", description=description, ats_platform="Infor")


def _row_job(rid):
    r = ROWS[rid]
    return _job(r["hospital_system"], r["hospital_name"], r["city"], r["state"], r["description"], r["job_id"],
                r["title"])


def _read(rid):
    r = ROWS[rid]
    return T.text_facility(r["hospital_system"], r["description"], r["city"], r["state"])


# ── The tables ──────────────────────────────────────────────────────────────

STATES = set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
             "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())


# 2026-10-29 phase 3 B: Workday site hooks on three text systems. Geisinger's
# table names only the descriptors that are a site; Allegheny Health
# Network's names the hospital streets of its "Town ST, zip, street"
# descriptors (its text Spec still reads the rare "Location:" line on a row
# the adapter left); Sutter Health's (facilities_workday.TOWN_SITE) reads only
# the town of a bare-town descriptor, never a hospital.
TEXT_AND_WORKDAY = {"Geisinger", "Allegheny Health Network", "Sutter Health"}


def test_every_system_is_a_scraper_label_with_no_other_facility_hook():
    # 2026-10-29 (integration): facilities_oracle_other registers a Spec for
    # its Oracle tenants (UChicago, UC Health, HealthPartners), which names a
    # row from the Oracle work location at list time. Such a Spec reads no
    # body and keeps no stored name, so the posting text is still read only
    # here, and only on rows the work location left on the label.
    registered = set(map(id, F.registered()))
    for system in T.SYSTEMS:
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(system, system) == system
        if system in F.SPECS:                              # facility_names reads its own systems
            spec = F.SPECS[system]
            assert id(spec) in registered, system
            assert not spec.fallback and F.text_candidates(system, "Location: X\nFacility: Y", "T") == [], system
        assert system not in scraper.WD_SITE_FACILITY or system in TEXT_AND_WORKDAY, system
    # 26 + Geisinger and Sutter Health (facilities_hospital_systems_b, phase 3 B)
    assert len(T.SYSTEMS) == 28


def test_table_entries_are_clean():
    for system, spec in T.SPECS.items():
        names = {}
        for site in spec.names.values():
            assert site.state in STATES and site.city == site.city.strip() and site.city, (system, site)
            assert site.name == site.name.strip() and "  " not in site.name and "," not in site.name, site.name
            assert not site.name.lower().startswith("the "), site.name
            assert re.match(r"^(cms \d{6}|board|address .+)", site.source), site.source
            names.setdefault(site.name, set()).add((site.city, site.state))
            for town, st, cname in site.campuses:
                assert st in STATES and town and (not cname or cname not in spec.table)
        # one town per name
        assert all(len(v) == 1 for v in names.values()), names
        # the system's own label names a site only where the system is named for its flagship
        assert system not in names or system == "Cabell Huntington Hospital"
        for k, site in spec.table.items():
            assert k == T.key(k) and site.name in spec.names
        assert not (set(spec.offices) & set(spec.table)), system


def test_one_spelling_never_names_two_sites():
    with pytest.raises(ValueError):
        T.Spec("X", None, (T.Read("label", "Location", "site"),),
               (T.S("A Hospital", "Town", "TX", "board", "Main"), T.S("B Hospital", "Town", "TX", "board", "main")))


def test_keys_fold_case_punctuation_saint_and_ampersand():
    assert T.key("Wheeler Heart & Vascular") == T.key("WHEELER HEART AND VASCULAR")
    assert T.key("St. Joseph’s Hospital") == T.key("Saint Josephs Hospital") == "st josephs hospital"
    assert T.lead_key("St. Mary's Medical Center is") .startswith(T.lead_key("St. Mary's Medical Center") + " ")


# ── Real stored bodies ──────────────────────────────────────────────────────

@pytest.mark.parametrize("rid, want", [
    (32714935, ("UNC REX Healthcare", "Raleigh", "NC")),            # Entity line, posting in Raleigh
    (32714857, ("Johnston Health Clayton", "Clayton", "NC")),       # a campus town keeps its campus name
    (32715993, ("UNC Rockingham Health Care", "Eden", "NC")),       # at the Chapel Hill default: the table's town
    (32712226, ("Bayhealth Hospital Kent Campus", "Dover", "DE")),
    (32712215, ("Bayhealth Hospital Sussex Campus", "Milford", "DE")),
    (38670742, ("Charlton Memorial Hospital", "Fall River", "MA")),  # stored at New Bedford; the line names Fall River
    (38670465, ("St. Luke's Hospital", "New Bedford", "MA")),       # "...; Includes coverage at Tobey" is a note
    (38670688, ("Tobey Hospital", "Wareham", "MA")),                # "Operating Room - Tobey Hospital - Wareham, MA"
    (35379310, ("Golisano Children's Hospital of Buffalo", "Buffalo", "NY")),
    (35379287, ("Millard Fillmore Suburban Hospital", "Williamsville", "NY")),
    (38783740, ("John H. Stroger Jr. Hospital of Cook County", "Chicago", "IL")),
    (38783748, ("Cermak Health Services", "Chicago", "IL")),
    (35378733, ("Kona Community Hospital", "Kealakekua", "HI")),    # stored at "Big Island"
    (35378785, ("Samuel Mahelona Memorial Hospital", "Kapaa", "HI")),  # stored at "Kauai"
    (28948260, ("Cox Monett Hospital", "Monett", "MO")),            # stored town "Cox Monett"
    (34803504, ("Cape Coral Hospital", "Cape Coral", "FL")),        # at the Fort Myers default
    (38790004, ("NCH Baker Hospital", "Naples", "FL")),             # by its street address
    (38790007, ("NCH North Naples Hospital", "Naples", "FL")),
    (35500181, ("Miami Valley Hospital North", "Dayton", "OH")),
    (26666409, ("Children's Medical Center Dallas", "Dallas", "TX")),  # "Hosp-Dal-C"
    (35439570, ("University of Chicago Medical Center", "Chicago", "IL")),  # "Onsite - Hyde Park"
    (35439847, ("UChicago Medicine Comer Children's Hospital", "Chicago", "IL")),
    (17641199, ("Duke Raleigh Hospital", "Raleigh", "NC")),         # "Duke Raleigh, a Campus of Duke University Hospital"
    (37291655, ("Duke University Hospital", "Durham", "NC")),       # "Duke University Hospital. 2301 Erwin Rd"
    (28943789, ("Monument Health Spearfish Hospital", "Spearfish", "SD")),  # code SPH, at the Rapid City default
    (32705885, ("Winter Haven Hospital", "Winter Haven", "FL")),    # "BayCare Health System, Med Surg Unit I-WHH"
    (32716535, ("MaineHealth Maine Medical Center Biddeford", "Biddeford", "ME")),
    (35364440, ("St. Mary's Medical Center", "Huntington", "WV")),
    (35364464, ("Rivers Health", "Point Pleasant", "WV")),
    (2303627, ("Park Nicollet Methodist Hospital", "Saint Louis Park", "MN")),
    (32706886, ("Aspirus Rhinelander Hospital", "Rhinelander", "WI")),
    (1307578, ("UW Medical Center - Montlake", "Seattle", "WA")),
    (38690292, ("Mount Sinai Hospital", "Chicago", "IL")),          # "Mount Sinai Hospital's Emergency Department"
])
def test_stored_body_names_its_site(rid, want):
    assert _read(rid) == want


@pytest.mark.parametrize("rid, what", [
    (32715018, "elsewhere"),   # UNC Medical Center entity on a Butner posting: a clinic or office of it
    (32714803, "office"),      # "Entity: Shared Services"
    (28948313, "office"),      # CoxHealth "Home Support: 2240 W Sunset St"
    (32709441, "office"),      # Lee: "Lee Health /HealthPark Medical Center/Gulf Coast Medical Center / Cape ..." (several)
    (37733156, "elsewhere"),   # Premier "Facility: Miami Valley Hospital" on a Greenville posting
    (26666382, "unknown"),     # Children's Health "Location: Dallas, TX": a town, no site
    (28943586, "unknown"),     # Monument "MHOSH-Clinic ...": the clinic, not the hospital's code
    (32705337, "elsewhere"),   # BayCare "St. Petersburg, FL - St. Joseph's Hospital / Suncoast ..." in St Petersburg
    (32716457, "office"),      # MaineHealth first line "Medical Group Practices"
    (38690261, "none"),        # Sinai: the "About Sinai Health System" paragraph lists every hospital
])
def test_stored_body_that_names_no_site_leaves_the_label(rid, what):
    r = ROWS[rid]
    assert _read(rid) is None
    j = _row_job(rid)
    assert T.apply_facility(j) == what
    assert (j.hospital_name, j.city, j.state, j.hospital_system) == (r["hospital_name"], r["city"], r["state"],
                                                                     r["hospital_system"])


def test_apply_sets_name_and_town_and_never_the_system():
    j = _row_job(35378733)                     # Hawaii, stored at "Big Island"
    assert T.apply_facility(j) == "text"
    assert (j.hospital_name, j.city, j.state, j.location) == ("Kona Community Hospital", "Kealakekua", "HI",
                                                              "Kealakekua, HI")
    assert j.hospital_system == "Hawaii Health Systems"
    j = _row_job(32714935)                     # town already right: only the name changes
    assert T.apply_facility(j) == "text" and (j.city, j.location) == ("Raleigh", ROWS[32714935]["city"] + ", NC")


def test_adapter_short_names_are_written_clean_town_kept():
    r = next(r for r in ROWS.values() if r["hospital_name"] == "St Josephs")
    j = _row_job(r["id"])
    assert T.apply_facility(j) == "canon"
    assert (j.hospital_name, j.city, j.state) == ("St. Joseph's Hospital", r["city"], r["state"])
    # a name the adapter wrote that no table lists is left as it is
    j = _job("BayCare", "BayCare Sys Office West", "Clearwater", "FL", "Facility: Morton Plant Hospital")
    assert T.apply_facility(j) == "other" and j.hospital_name == "BayCare Sys Office West"


def test_nights_without_a_body_keep_the_stored_site():
    T.set_known_facilities([
        {"hospital_system": "UNC Health", "job_id": "R1", "hospital_name": "UNC REX Healthcare"},
        {"hospital_system": "UNC Health", "job_id": "R2", "hospital_name": "Johnston Health Clayton"},
        {"hospital_system": "UNC Health", "job_id": "R3", "hospital_name": "UNC Health"},          # the label: not kept
        {"hospital_system": "Kaleida Health", "job_id": "R1", "hospital_name": "Some Clinic"},     # not a site
    ])
    assert T.known_facility("UNC Health", "R3") == "" and T.known_facility("Kaleida Health", "R1") == ""
    j = _job("UNC Health", "UNC Health", "Raleigh", "NC", "", "R1")
    assert T.apply_facility(j) == "kept" and j.hospital_name == "UNC REX Healthcare"
    j = _job("UNC Health", "UNC Health", "Clayton", "NC", "", "R2")
    assert T.apply_facility(j) == "kept" and j.hospital_name == "Johnston Health Clayton"
    # the stored site, the posting now in another town: not kept
    j = _job("UNC Health", "UNC Health", "Apex", "NC", "", "R1")
    assert T.apply_facility(j) == "none" and j.hospital_name == "UNC Health"
    # a body read tonight that names no site: the label, not the stored name
    j = _job("UNC Health", "UNC Health", "Raleigh", "NC", "Entity: Shared Services\n" + "Duties. " * 120, "R1")
    assert T.apply_facility(j) == "office" and j.hospital_name == "UNC Health"


def test_load_known_without_credentials_is_empty(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert T.load_known_facilities() == 0


def test_two_sites_in_one_value_name_none():
    spec = T.SPECS["Southcoast Health"]
    assert T.match_value(spec, "Charlton Memorial Hospital - Tobey Hospital") is None
    assert T.match_value(spec, "Tobey Hospital - Wareham, MA; also covers Charlton Memorial Hospital").name == "Tobey Hospital"
    assert T.match_value(T.SPECS["BayCare"], "St. Joseph's Hospital - North").name == "St. Joseph's Hospital-North"


def test_location_of_job_is_not_the_location_line():
    body = "Department : GCH Respiratory Therapy\nLocation of Job : US:NY:Buffalo\nWork Type : Full-Time\n"
    assert T.read_text(T.SPECS["Kaleida Health"], body)[2] == "none"


def test_unknown_values_are_counted_for_the_run_log():
    T.apply_facility(_job("Kaleida Health", "Kaleida Health", "Buffalo", "NY", "Location: Conventus\n" + "x " * 400))
    assert T.UNKNOWN == {"Kaleida Health": {"Conventus": 1}}
    assert "Kaleida Health: Conventus (1)" in T.unknown_summary()


def test_ascension_alias_added():
    hit = F.resolve("Ascension Health", "Seton Medical Center Williamson", "Round Rock", "TX")
    assert hit == ("Ascension Seton Williamson", "Round Rock", "TX")


# ── Through finalize_jobs (the upsert payload) ──────────────────────────────

def test_finalize_jobs_payload(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    T.set_known_facilities([{"hospital_system": "CoxHealth", "job_id": "C9", "hospital_name": "Cox Medical Center Branson"}])
    rows = scraper.finalize_jobs([
        _row_job(35378785),                                                          # Hawaii, "Kauai"
        _row_job(32705885),                                                          # BayCare label row, WHH
        _job("CoxHealth", "CoxHealth", "Branson", "MO", "", "C9", "RN"),             # no body tonight: kept
        _row_job(32714803),                                                          # UNC Shared Services
    ])
    by = {r["job_id"]: r for r in rows}
    k = ROWS[35378785]["job_id"]
    assert (by[k]["hospital_name"], by[k]["city"], by[k]["state"], by[k]["location"], by[k]["hospital_system"]) == \
        ("Samuel Mahelona Memorial Hospital", "Kapaa", "HI", "Kapaa, HI", "Hawaii Health Systems")
    assert by[ROWS[32705885]["job_id"]]["hospital_name"] == "Winter Haven Hospital"
    assert by["C9"]["hospital_name"] == "Cox Medical Center Branson" and by["C9"]["city"] == "Branson"
    assert by[ROWS[32714803]["job_id"]]["hospital_name"] == "UNC Health"
