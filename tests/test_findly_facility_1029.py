# 2026-10-29 (owner: every hospital inside a multi-hospital system named and
# placed correctly; decisions: cards and job pages show the hospital, named
# sites show their own name, generic offices / home-based / remote show the
# system name with the town, never guess a town). The Findly-Google family:
# AdventHealth (job.department read with job.education), Scripps Health
# (job.business_unit), University Hospitals (job.store_id), UCLA Health
# (job.primary_address), UPMC unchanged (facilities_findly.py).
# Fixtures are real search results read 2026-10-29 (tests/fixtures/findly1029/:
# bodies cut at 240 characters, recruiter names replaced) and real stored
# AdventHealth rows (the body's Address block).
import asyncio
import dataclasses
import json
import os
import re

import pytest

import facilities_findly as FF
import scraper

FIX = "findly1029"


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
        self.calls = 0

    def get(self, url, **kw):
        self.calls += 1
        return _Ctx(_R(self.body))


def _payload(fixture_text, name):
    body = fixture_text(f"{FIX}/{name}_search.txt")
    return [r["job"] for r in json.loads(re.match(r"[^(]*\((.*)\);?\s*$", body, re.S).group(1))["searchResults"]]


def _scrape(fixture_text, system, name):
    s = _Session(fixture_text(f"{FIX}/{name}_search.txt"))
    jobs = asyncio.run(scraper.scrape_findly_google(s, system, scraper.FINDLY_GOOGLE_ORGS[system]))
    assert s.calls == 1
    return {j.job_id: j for j in jobs}


# ── AdventHealth ────────────────────────────────────────────────────────────
# job id -> (department, education, expected hospital_name)
AH = {
    "23901841": ("AdventHealth Orlando", "Orlando", "AdventHealth Orlando"),
    "24029548": ("AdventHealth East Orlando", "East Orlando", "AdventHealth East Orlando"),
    "24029419": ("AdventHealth Lake Nona", "Lake Nona", "AdventHealth Lake Nona"),
    "24074083": ("AdventHealth Tampa", "Tampa", "AdventHealth Tampa"),
    "22755068": ("AdventHealth Carrollwood", "Carrollwood", "AdventHealth Carrollwood"),
    "23674869": ("AdventHealth Tampa", "WFD Support", "AdventHealth"),
    "24057742": ("AdventHealth Porter", "Porter", "AdventHealth Porter"),
    "24020027": ("AdventHealth Porter", "RMR Support", "AdventHealth"),
    "23918895": ("AdventHealth Shawnee Mission", "Shawnee Mission", "AdventHealth Shawnee Mission"),
    "23734282": ("AdventHealth Shawnee Mission", "ASC MAR Shawnee Mission", "AdventHealth"),
    "24044797": ("AdventHealth Shawnee Mission", "Mid America Region", "AdventHealth"),
    "24055524": ("AdventHealth Daytona Beach", "East FL Division", "AdventHealth"),
    "24055384": ("UChicago Medicine AdventHealth Hinsdale", "Hinsdale JV", "UChicago Medicine AdventHealth Hinsdale"),
    "22754261": ("UChicago Medicine AdventHealth Hinsdale", "GLR Support JV", "AdventHealth"),
    "24055404": ("Texas Health Huguley Hospital", "Texas Health Huguley JV",
                 "Texas Health Huguley Hospital Fort Worth South"),
    "24074313": ("AdventHealth Millenia ER", "ER", "AdventHealth Millenia ER"),
    "23999207": ("AdventHealth Palm Parkway", "ER", "AdventHealth Palm Parkway"),
    "24026991": ("AdventHealth ER and Urgent Care Southmoor", "ER", "AdventHealth ER and Urgent Care Southmoor"),
    "24074087": ("AdventHealth Medical Group West Florida", "AdventHealth Medical Group", "AdventHealth"),
    "23834826": ("AdventHealth Home Health and Hospice", "AdventHealth Home Health and Hospice", "AdventHealth"),
    "22756072": ("AdventHealth Orlando Support", "Orlando Support", "AdventHealth"),
    "22822103": ("AdventHealth Corporate", None, "AdventHealth"),
    "23524001": ("AdventHealth University", "AdventHealth University", "AdventHealth University"),
    "24054802": ("OSED WFD Auburndale", "OSED WFD Auburndale", "AdventHealth"),
    "23650617": ("AdventHealth Prairie Star", "ASC MAR Prairie Star", "AdventHealth Prairie Star"),
    "23797784": (None, None, "AdventHealth"),
    "23999991": ("AdventHealth Centra Care", "Centra Care Support", "AdventHealth"),
    "24020000": ("AdventHealth Wesley Chapel", "Wesley Chapel", "AdventHealth Wesley Chapel"),
    # the Littleton primary care physician filed under Porter's company with
    # the region's support location: its own job page prints "Facility:
    # AdventHealth Porter", but it is no Porter job (decision 2)
    "24011109": ("AdventHealth Porter", "RMR Support", "AdventHealth"),
}


def test_fixture_is_the_real_payload(fixture_text):
    jobs = _payload(fixture_text, "adventhealth")
    assert {str(j["id"]) for j in jobs} == set(AH)
    for j in jobs:
        dept, edu, _ = AH[str(j["id"])]
        assert (j.get("department"), j.get("education")) == (dept, edu)
        assert j["company_name"] == "Adventist Health System - Florida"     # the old label, every row


def test_adventhealth_rows_name_the_hospital_or_the_system(fixture_text):
    raw = {str(j["id"]): j for j in _payload(fixture_text, "adventhealth")}
    jobs = _scrape(fixture_text, "AdventHealth", "adventhealth")
    assert set(jobs) == set(AH)
    for jid, j in jobs.items():
        assert j.hospital_name == AH[jid][2], jid
        assert j.hospital_system == "AdventHealth"
        # the posting's own town is kept on every row
        assert (j.city, j.state) == (raw[jid]["primary_city"], raw[jid]["primary_state"])
        assert j.location == f"{j.city}, {j.state}"
        assert j.req_id                            # the 10-12 requisition number is untouched
    assert "Adventist Health System - Florida" not in {j.hospital_name for j in jobs.values()}


def test_orlando_and_tampa_hold_several_hospitals(fixture_text):
    jobs = _scrape(fixture_text, "AdventHealth", "adventhealth")
    orlando = {j.hospital_name for j in jobs.values() if j.city == "Orlando" and j.hospital_name != "AdventHealth"}
    assert {"AdventHealth Orlando", "AdventHealth East Orlando", "AdventHealth Lake Nona"} <= orlando
    tampa = {j.hospital_name for j in jobs.values() if j.city == "Tampa" and j.hospital_name != "AdventHealth"}
    assert {"AdventHealth Tampa", "AdventHealth Carrollwood"} <= tampa


def test_support_location_under_a_hospital_company_names_no_hospital():
    base = {"primary_city": "Littleton", "primary_state": "CO"}
    porter = dict(base, department="AdventHealth Porter")
    assert FF.adventhealth(dict(porter, education="Porter"), "Denver", "CO") == ("AdventHealth Porter", "Denver", "CO")
    for edu in ("RMR Support", "Rocky Mountain Region", None, ""):
        assert FF.adventhealth(dict(porter, education=edu), "Littleton", "CO") is None
    # a hospital's own location is matched without case or punctuation
    assert FF.adventhealth({"department": "AdventHealth East Orlando", "education": "east orlando"}, "Orlando", "FL")[0] \
        == "AdventHealth East Orlando"


def test_er_rule_and_unknown_departments():
    er = {"department": "AdventHealth Clermont ER", "education": "ER"}
    assert FF.adventhealth(er, "Clermont", "FL") == ("AdventHealth Clermont ER", "Clermont", "FL")
    assert FF.unknown("AdventHealth", er) == ""
    # a new hospital the table does not list yet: no name, logged
    new = {"department": "AdventHealth Lake Mary", "education": "Lake Mary"}
    assert FF.adventhealth(new, "Lake Mary", "FL") is None
    assert FF.unknown("AdventHealth", new) == "AdventHealth Lake Mary"
    # known non-sites are not logged
    for dept in ("AdventHealth Medical Group Central Florida", "AdventHealth Home Health and Hospice",
                 "OSED WFD Auburndale", "AdventHealth Corporate", "AdventHealth Centra Care"):
        assert FF.unknown("AdventHealth", {"department": dept, "education": "x"}) == ""
        assert FF.adventhealth({"department": dept, "education": "ER"}, "Orlando", "FL") is None


def test_hospital_town_only_when_the_posting_has_none():
    j = {"department": "AdventHealth Gordon", "education": "Gordon"}
    assert FF.adventhealth(j, "Rome", "GA") == ("AdventHealth Gordon", "Rome", "GA")
    assert FF.adventhealth(j, "", "") == ("AdventHealth Gordon", "Calhoun", "GA")


def test_new_names_change_the_content_hash(fixture_text):
    scraper.set_known_bodies([])
    j = _scrape(fixture_text, "AdventHealth", "adventhealth")["23901841"]
    new = scraper.normalize_job(j)
    old = scraper.normalize_job(dataclasses.replace(j, hospital_name="Adventist Health System - Florida"))
    assert new["hospital_name"] == "AdventHealth Orlando" and new["hospital_system"] == "AdventHealth"
    assert scraper.content_hash_for(new) != scraper.content_hash_for(old)


# ── Stored AdventHealth rows (expired rows that carry applies) ──────────────
STORED = {
    3509472: "AdventHealth Porter",                          # 2525 S Downing St, Denver
    10250545: None,                                          # AHMG clinic, 380 Celebration Pl
    17625281: None,                                          # office, 5600 Clearfork Main St
    17881855: "Texas Health Huguley Hospital Fort Worth South",   # 11801 South Fwy, Burleson
    19868876: "AdventHealth Kissimmee",                      # 2450 N Orange Blossom Trl
    23355274: None,                                          # 10961 Narcoossee Rd is not the hospital's street
    26016304: None,                                          # "Director of Home Health" on the Altamonte campus
    29368927: None,                                          # 2501 N Orange Ave
    38427259: "AdventHealth Orlando",                        # 601 E Rollins St
}


def test_stored_rows_by_the_hospitals_own_street(fixture_text):
    rows = json.loads(fixture_text(f"{FIX}/adventhealth_stored_rows.json"))
    assert {r["id"] for r in rows} == set(STORED)
    for r in rows:
        assert r["hospital_name"] == "Adventist Health System - Florida"
        hit = FF.stored_facility("AdventHealth", r)
        assert (hit[0] if hit else None) == STORED[r["id"]], r["id"]
        if hit:
            assert (hit[1], hit[2]) == (r["city"], r["state"])      # the stored town is kept
            assert hit[3].startswith("stored body Address line")
    assert FF.stored_facility("Scripps Health", rows[0]) is None


# ── Scripps Health ─────────────────────────────────────────────────────────
SCRIPPS = {
    "SCRIPPS MEMORIAL HOSP LA JOLLA (300)": "Scripps Memorial Hospital La Jolla",
    "SCRIPPS GREEN HOSP (310)": "Scripps Green Hospital",
    "SCRIPPS MEMORIAL HOSP ENC (320)": "Scripps Memorial Hospital Encinitas",
    "SCRIPPS MERCY HOSP CHULA VISTA (330)": "Scripps Mercy Hospital Chula Vista",
    "SCRIPPS MERCY HOSP SAN DIEGO (350)": "Scripps Mercy Hospital San Diego",
    "SCRIPPS CLIN-TORREY PINES (202)": "Scripps Clinic Torrey Pines",
    "SCF-ANDERSON MEDICAL PAVILION (233)": "Scripps Clinic Anderson Medical Pavilion",
    "SCRIPPS COASTAL MED CNTR-HLLCR (419)": "Scripps Coastal Medical Center Hillcrest",
    "SCRIPPS CORPORATE OFFICE-CP (100)": "Scripps Health",
    "QUALCOMM HEALTH CENTER (401)": "Scripps Health",
    "SCRIPPS COASTAL MED CNTR OBGYN (491)": "Scripps Health",
    "HOSPITAL CENTRALIZED SERVICES (390)": "Scripps Health",
}


def test_scripps_rows_name_the_hospital_or_clinic(fixture_text):
    raw = {str(j["id"]): j for j in _payload(fixture_text, "scripps")}
    assert {j["business_unit"] for j in raw.values()} == set(SCRIPPS)
    jobs = _scrape(fixture_text, "Scripps Health", "scripps")
    for jid, j in jobs.items():
        assert j.hospital_name == SCRIPPS[raw[jid]["business_unit"]], jid
        assert (j.city, j.state) == (raw[jid]["primary_city"], raw[jid]["primary_state"])
    for jj in raw.values():
        assert FF.unknown("Scripps Health", jj) == ""


# ── University Hospitals ───────────────────────────────────────────────────
UH = {
    "Cleveland Medical Center": "UH Cleveland Medical Center",
    "Rainbow Babies & Children's Hospital": "UH Rainbow Babies & Children's Hospital",
    "Lake West Medical Center": "UH Lake West Medical Center",
    "Tripoint Medical Center": "UH TriPoint Medical Center",
    "Geauga Medical Center Campus": "UH Geauga Medical Center",
    "Parma Medical Center": "UH Parma Medical Center",
    "Beachwood Medical Center": "UH Beachwood Medical Center",
    "Conneaut Medical Center": "UH Conneaut Medical Center",
    "Medical Practices / Mgmt Services Org": "University Hospitals",
    "University Hospitals": "University Hospitals",
    "Cleveland - Ohio - United States": "University Hospitals",
    "Home Care Services": "University Hospitals",
}


def test_uh_rows_name_the_facility_or_the_system(fixture_text):
    raw = {str(j["id"]): j for j in _payload(fixture_text, "uh")}
    assert {j["store_id"] for j in raw.values()} == set(UH)
    assert {j["company_name"] for j in raw.values()} == {"University Hospitals (SP)"}
    jobs = _scrape(fixture_text, "University Hospitals", "uh")
    for jid, j in jobs.items():
        assert j.hospital_name == UH[raw[jid]["store_id"]], jid
        assert (j.city, j.state) == (raw[jid]["primary_city"], raw[jid]["primary_state"])
        assert FF.unknown("University Hospitals", raw[jid]) == ""


# ── UCLA Health ────────────────────────────────────────────────────────────
def test_ucla_names_a_hospital_only_at_its_own_street_in_its_own_town(fixture_text):
    raw = {str(j["id"]): j for j in _payload(fixture_text, "ucla")}
    jobs = _scrape(fixture_text, "UCLA Health", "ucla")
    got = {}
    for jid, j in jobs.items():
        street = re.split(r",\s*City:", raw[jid]["primary_address"])[0].strip()
        got[(street, j.city)] = j.hospital_name
        assert (j.city, j.state) == (raw[jid]["primary_city"], raw[jid]["primary_state"])
    assert got == {
        ("757 Westwood Plaza", "Los Angeles"): "Ronald Reagan UCLA Medical Center",
        ("RRUMC 757 Westwood Pl", "Los Angeles"): "Ronald Reagan UCLA Medical Center",
        ("RRUCLAMC, 757 Westwood Plaza", "Los Angeles"): "Ronald Reagan UCLA Medical Center",
        ("757 westwood plaza", "Los Angeles"): "Ronald Reagan UCLA Medical Center",
        ("1250 16th St.", "Santa Monica"): "UCLA Santa Monica Medical Center",
        # the same street written under Los Angeles: no guess, the system
        ("1250 16th Street", "Los Angeles"): "UCLA Health",
        ("7300 Medical Center Dr.", "West Hills"): "UCLA West Valley Medical Center",
        ("760 Westwood Plaza", "Los Angeles"): "UCLA Health",
        ("200 Medical Plaza", "Los Angeles"): "UCLA Health",
        ("10920 Wilshire Blvd", "Los Angeles"): "UCLA Health",
    }


def test_ucla_street_keys():
    k = FF._ucla_street
    assert k("150 Medical Plaza (RRUMC), City: Los Angeles, State: CA") == k("150 UCLA Medical Plaza") \
        == k("150 Medical Plaza-Rm-4230C") == "150 medical"
    assert k("RRUCLAMC, 757 Westwood Plaza, City: Los Angeles") == "757 westwood"
    assert k("") == "" and k(None) == ""
    assert FF.ucla({"primary_address": "150 Medical Plaza (RRUMC), City: Los Angeles"}, "Los Angeles", "CA") \
        == ("Resnick Neuropsychiatric Hospital at UCLA", "Los Angeles", "CA")


# ── Wiring ─────────────────────────────────────────────────────────────────
def test_hooks_and_system_names():
    assert set(scraper.FINDLY_GOOGLE_FACILITY) == {"UPMC", "AdventHealth", "Scripps Health",
                                                  "University Hospitals", "UCLA Health"}
    assert scraper.FINDLY_GOOGLE_NAME == {
        "UT Southwestern Medical Center": "UT Southwestern Medical Center",
        "AdventHealth": "AdventHealth", "University Hospitals": "University Hospitals", "UPMC": "UPMC"}
    for system in scraper.FINDLY_GOOGLE_FACILITY:
        assert system in scraper.FINDLY_GOOGLE_ORGS


def test_every_table_name_is_clean():
    tables = [FF.ADVENTHEALTH_FACILITIES, FF.SCRIPPS_FACILITIES, FF.UH_FACILITIES, FF.UCLA_FACILITIES]
    for t in tables:
        for name, city, st, src in t.values():
            assert name == name.strip() and "  " not in name and "—" not in name
            assert city and re.fullmatch(r"[A-Z]{2}", st) and src


def test_run_log_counts_named_rows_and_unknown_values(fixture_text, caplog):
    jobs = _payload(fixture_text, "adventhealth")
    extra = dict(jobs[0], id=1, department="AdventHealth Lake Mary", education="Lake Mary")
    body = "CWS.jobs.jobCallback(" + json.dumps({"totalHits": len(jobs) + 1,
                                                 "searchResults": [{"job": j} for j in jobs + [extra]]}) + ");"
    with caplog.at_level("INFO", logger="scraper"):
        out = asyncio.run(scraper.scrape_findly_google(_Session(body), "AdventHealth",
                                                       scraper.FINDLY_GOOGLE_ORGS["AdventHealth"]))
    named = sum(1 for j in out if j.hospital_name != "AdventHealth")
    assert named == sum(1 for v in AH.values() if v[2] != "AdventHealth")
    text = caplog.text
    assert f"{named} of {len(out)} rows name their site" in text
    assert "AdventHealth Lake Mary" in text
