"""2026-10-29 (facility scale, phase 5 group C): the hospital or named site on
the label rows of ARH, DCH, Shriners Children's, St. Charles, Children's
Wisconsin, Premier, Hendrick, Sharp, Mary Washington, UW Medicine, CHOP,
Stormont Vail, Cape Cod, Temple, Bellin, Stanford and Norton Sound
(facilities_fac5c).

Fixture (tests/fixtures/facility_fac5c_1029/stored_rows.json): real stored
label rows of hospital_jobs read 2026-10-29 22:45-23:00 UTC (bodies over
2,500 characters cut, their Location / Facility lines kept), each with the
answer of the stored-row change list (facility_scale_2026-10-29/phase5/
changes/c.tsv; null = the row stays on the system name). No network, no
database."""
import inspect
import json
import os

import pytest

import facilities_fac5c as F
import facilities_town as FT
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_fac5c_1029", "stored_rows.json")
with open(FIX, encoding="utf-8") as _f:
    ROWS = json.load(_f)["rows"]


@pytest.fixture(autouse=True)
def _empty_stored():
    FT.set_stored([])
    yield
    FT.set_stored([])


def _job(system, title, city, state, description="", url="", job_id="1", name=None):
    return Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
               location=f"{city}, {state}", specialty="", job_type="", url=url, job_id=str(job_id), posted_date="",
               description=description, ats_platform="X")


def _adapter(j):
    """The adapter's list-time Workday site table (the stored url's site segment)."""
    if j.hospital_system in scraper.WD_SITE_FACILITY and "myworkdayjobs" in j.url:
        hit = scraper.WD_SITE_FACILITY[j.hospital_system]("", j.url)
        if hit:
            j.hospital_name, j.city, j.state = hit
    return j


def _finalize(jobs):
    os.environ["FINALIZE_WORKERS"] = "1"
    return scraper.finalize_jobs(jobs)


BODY = "Job Summary\n" + ("The team member provides care and support to the patients and families we serve. " * 15)


# ── the stored rows come out as the change list writes them ──────────────────
def test_stored_rows_match_the_change_list():
    jobs = [_adapter(_job(r["hospital_system"], r["title"], r["city"], r["state"], r["description"], r["url"],
                          r["job_id"])) for r in ROWS]
    out = {(o["hospital_system"], str(o["job_id"])): o for o in _finalize(jobs)}
    bad = []
    for r in ROWS:
        o = out[(r["hospital_system"], str(r["job_id"]))]
        got = [o["hospital_name"], o.get("city") or "", o.get("state") or ""]
        if r["want"] is None:
            if got[0] != r["hospital_system"]:
                bad.append((r["id"], "label", got))
        elif got != r["want"]:
            bad.append((r["id"], r["want"], got))
        assert o["hospital_system"] == r["hospital_system"]
    assert not bad, bad
    assert sum(1 for r in ROWS if r["want"]) >= 80
    assert sum(1 for r in ROWS if r["want"] is None) >= 40


def test_every_town_table_is_registered():
    for spec in F.TOWN_SPECS:
        assert FT.SPECS[spec.system] is spec
    assert FT.SPECS["Shriners Children's"].town("St. Louis", "MO")[0] == "Shriners Children's St. Louis"
    assert FT.SPECS["Hendrick Health"].shared[(FT.town_key("Abilene"), "TX")] == [
        "Hendrick Medical Center", "Hendrick Medical Center South"]


# ── town tables (facilities_town's rule) ────────────────────────────────────
def test_arh_town_and_office_rows():
    assert FT.decide(F.ARH, F.ARH, "Hazard", "KY", "Registered Nurse", BODY)[:3] == (
        "Hazard ARH Regional Medical Center", "Hazard", "KY")
    assert FT.decide(F.ARH, F.ARH, "US-Hazard", "KY", "Registered Nurse", BODY)[:3] == (
        "Hazard ARH Regional Medical Center", "Hazard", "KY")
    assert FT.decide(F.ARH, F.ARH, "Lexington", "KY", "Registered Nurse", BODY)[3] == "no-town"
    assert FT.decide(F.ARH, F.ARH, "Hazard", "KY", "Medical Assistant", BODY)[0] is None
    assert FT.decide(F.ARH, F.ARH, "Hazard", "KY", "Revenue Cycle Analyst - Remote", BODY)[0] is None


def test_childrens_wisconsin_keeps_milwaukee_and_its_welfare_services():
    assert FT.decide(F.CHW, F.CHW, "Neenah", "WI", "NICU RN,  Full-time Nights - Neenah, WI", BODY)[0] == \
        "Children's Wisconsin Fox Valley Hospital"
    assert FT.decide(F.CHW, F.CHW, "Neenah", "WI", "Child Welfare Caseworker PRN - Casual", BODY)[3] == "care-site"
    assert FT.decide(F.CHW, F.CHW, "Neenah", "WI", "Clinical Education Program Manager-Northeast Wisconsin",
                     BODY)[0] is None
    assert FT.decide(F.CHW, F.CHW, "Milwaukee", "WI", "Registered Nurse - PICU", BODY,
                     "https://cwi.wd1.myworkdayjobs.com/CW_Careers/job/Milwaukee-WI/RN_R1")[3] == "keep"


def test_chop_names_king_of_prussia_and_keeps_other_systems_hospitals():
    assert FT.decide(F.CHOP, F.CHOP, "King of Prussia", "PA", "Clinical Nurse, Emergency Department", BODY)[0] == \
        "CHOP Middleman Family Pavilion"
    assert FT.decide(F.CHOP, F.CHOP, "Philadelphia", "PA", "Clinical Nurse, Emergency Department", BODY)[3] == "keep"
    assert FT.decide(F.CHOP, F.CHOP, "Bryn Mawr", "PA", "Hospitalist - Bryn Mawr", BODY)[0] is None


def test_stormont_vail_flint_hills_and_topeka():
    assert FT.decide(F.STORMONT, F.STORMONT, "Junction City", "KS",
                     "Acute Care RN (Flint Hills) - Medical Nursing - FT - Night", BODY)[0] == \
        "Stormont Vail Health Flint Hills Campus"
    assert FT.decide(F.STORMONT, F.STORMONT, "Junction City", "KS", "Medical Assistant (Flint Hills) - RHC - FT - Day",
                     BODY)[0] is None
    assert FT.decide(F.STORMONT, F.STORMONT, "Topeka", "KS", "Registered Nurse - ICU", BODY,
                     "https://stormontvail.wd1.myworkdayjobs.com/SV/job/Topeka-KS/RN_R1")[3] == "keep"
    assert FT.decide(F.STORMONT, F.STORMONT, "Topeka", "KS", "Registered Nurse - ICU", BODY)[3] == "default"


def test_uw_renton_is_kept():
    assert FT.decide(F.UWM, F.UWM, "Renton", "WA", "Specimen Processing Supervisor, Renton Central Lab: Microbiology",
                     BODY)[3] == "keep"


def test_mary_washington_stafford_keeps_its_town_through_the_head_office_override():
    j = _job(F.MWHC, "Registered Nurse - Medical Surgical", "Stafford", "VA", BODY,
             "https://marywashingtonhealthcare.wd5.myworkdayjobs.com/Externalcareers/job/Stafford/RN_R1", "R1")
    o = _finalize([_adapter(j)])[0]
    assert (o["hospital_name"], o["city"], o["state"]) == ("Stafford Hospital", "Stafford", "VA")


# ── title / body / link readers ─────────────────────────────────────────────
def test_hendrick_north_south():
    assert F.hendrick_read("RN -CCU / ICU - South", BODY, "Abilene", "TX")[0] == "Hendrick Medical Center South"
    assert F.hendrick_read("New Grad RN CCU  ICU South  Aug - Dec Grad", BODY, "Abilene", "TX")[0] == \
        "Hendrick Medical Center South"
    assert F.hendrick_read("Patient Care Tech, C5 PCU Tele – North", BODY, "Abilene", "TX")[0] == \
        "Hendrick Medical Center"
    assert F.hendrick_read("Scheduling Rep, Hendrick Clinic South Internal Med", BODY, "Abilene", "TX") is None
    assert F.hendrick_read("CMA, Hendrick Clinic Southside", BODY, "Abilene", "TX") is None
    assert F.hendrick_read("RN -CCU / ICU - South", BODY, "Brownwood", "TX") is None


def test_premier_titles():
    assert F.premier_read("Completion Tech/Full-Time-Days/ Upper Valley Medical Center", BODY, "Dayton", "OH")[:3] == (
        "Upper Valley Medical Center", "Troy", "OH")
    assert F.premier_read("PROGRAM MEDICAL DIRECTOR - MVH NORTH", BODY, "Dayton", "OH")[0] == \
        "Miami Valley Hospital North"
    assert F.premier_read("CRNA at MIAMI VALLEY HOSPITAL (7am-7pm)", BODY, "Dayton", "OH")[0] == "Miami Valley Hospital"
    # a posting in another town than the hospital it names (a satellite) keeps the label
    assert F.premier_read("CT Tech - Miami Valley Hospital", BODY, "Beavercreek", "OH") is None
    # a "Facility:" line is facilities_text's to read
    assert F.premier_read("RN - MVH", "Facility: Premier System Support\n" + BODY, "Dayton", "OH") is None
    assert F.decide(F.PREMIER, F.PREMIER, "Dayton", "OH", "CRNA or AA at MIAMI VALLEY HOSPITAL (HYBRID)", BODY)[0] is None


def test_temple_campus_words():
    assert F.temple_read("RN - POOL 6A Med Surg Episcopal Campus", BODY, "Philadelphia", "PA")[0] == \
        "Temple University Hospital - Episcopal Campus"
    assert F.temple_read("Registered EEG Technician [F/T, 40hrs | Days]- TUH- Jeanes Hospital", BODY, "Philadelphia",
                         "PA")[0] == "Temple University Hospital - Jeanes Campus"
    assert F.temple_read("RN Float - Jeanes and Episcopal", BODY, "Philadelphia", "PA") is None
    assert F.temple_read("Registered Nurse - TUH Main", BODY, "Philadelphia", "PA") is None


def test_sharp_titles():
    assert F.sharp_read("Nursing Assistant-3N Medical Neuroscience-Sharp Memorial Hospital-Day", BODY, "San Diego",
                        "CA")[0] == "Sharp Memorial Hospital"
    assert F.sharp_read("Surgical Tech I  - Women's Surgery - Sharp Mary Birch - 12 Hour Night", BODY, "San Diego",
                        "CA")[0] == "Sharp Mary Birch Hospital for Women & Newborns"
    assert F.sharp_read("Medical Assistant - Sharp Rees-Stealy La Mesa", BODY, "La Mesa", "CA") is None
    assert F.sharp_read("Pharmacy Technician - Sharp Grossmont Specialty Pharmacy", BODY, "La Mesa", "CA") is None
    assert F.sharp_read("RN - ICU - Sharp Memorial Hospital", BODY, "La Mesa", "CA") is None


def test_uw_titles():
    assert F.uw_read("Registered Nurse - Emergency - Harborview", BODY, "Seattle", "WA")[0] == \
        "Harborview Medical Center"
    assert F.uw_read("Cardiovascular Technologist (CVT) - Cardiac Catheterization Lab - UW Medical Center - Montlake",
                     BODY, "Seattle", "WA")[0] == "UW Medical Center - Montlake"
    assert F.uw_read("MA / Medical Assistant - UW Medicine Primary Care at Ballard", BODY, "Seattle", "WA") is None
    assert F.uw_read("Registered Nurse - UW Medical Center", BODY, "Seattle", "WA") is None


def test_bellin_location_street():
    hosp = "Location: 744 S Webster Ave, Green Bay, WI 54301\n" + BODY
    psych = "Job Specifics: Location: 301 E St Joseph St, Green Bay, WI 54301\n" + BODY
    clinic = "Location: 725 S Webster Ave, Green Bay, WI 54301\n" + BODY
    assert F.bellin_read("Registered Nurse (RN) | Cardiac Unit", hosp, "Green Bay", "WI")[0] == "Bellin Hospital"
    assert F.bellin_read("RN Supervisor | Inpatient Psychiatry", psych, "Green Bay", "WI")[0] == \
        "Bellin Psychiatric Center"
    assert F.bellin_read("Registered Nurse (RN) | Cardiac Unit", clinic, "Green Bay", "WI") is None
    assert F.bellin_read("Certified Medical Assistant (CMA)", hosp, "Green Bay", "WI") is None


def test_stanford_tri_valley_link():
    u = ("https://stanfordmedicine.wd115.myworkdayjobs.com/SHC_External_Career_Site/job/"
         "5555-W-Las-Positas-Blvd---PLEASANTON/Clinical-Nurse_R1")
    assert F.stanford_read("Clinical Nurse II - Med Surg", BODY, "PLEASANTON", "CA", u)[:3] == (
        "Stanford Health Care Tri-Valley", "Pleasanton", "CA")
    assert F.stanford_read("Clinical Nurse II", BODY, "PLEASANTON", "CA", u.replace("5555-W-Las-Positas-Blvd",
                                                                                     "5725-W-Las-Positas-101")) is None
    assert F.decide(F.STANFORD, F.STANFORD, "PALO ALTO", "CA", "Clinical Nurse", BODY,
                    u.replace("5555-W-Las-Positas-Blvd---PLEASANTON", "500P-Hospital---JKLM-Patient---PALO-ALTO"))[0] \
        is None


def test_cape_cod_title_towns_and_named_sites():
    j = _job(F.CAPECOD, "Urgent Care Receptionist (Sandwich)", "Hyannis", "MA", BODY)
    assert F.apply_facility(j) == "town" and (j.hospital_name, j.city) == (F.CAPECOD, "Sandwich")
    j = _job(F.CAPECOD, "Certified Home Health Aide, (Per Diem), VNA of Cape Cod", "Hyannis", "MA", BODY)
    assert F.apply_facility(j) == "title" and j.hospital_name == "VNA of Cape Cod"
    j = _job(F.CAPECOD, "Maintenance Mechanic, JML Care Center", "Hyannis", "MA", BODY)
    F.apply_facility(j)
    assert (j.hospital_name, j.city) == ("JML Care Center", "Falmouth")
    j = _job(F.CAPECOD, "RN, Med Surg", "Hyannis", "MA", BODY)
    assert F.apply_facility(j) == "stamp" and j._town_skip is True
    assert FT.apply_job(j) == "detail-office" and j.hospital_name == F.CAPECOD
    # the stamp's own town named in the title: the town test runs in the reader
    j = _job(F.CAPECOD, "Physical Therapist/PT Clinical Specialist, Inpatient (Hyannis)", "Hyannis", "MA", BODY)
    F.apply_facility(j)
    assert (j.hospital_name, j.city) == ("Cape Cod Hospital", "Hyannis")
    j = _job(F.CAPECOD, "Certified Medical Assistant, Endocrine Center (Hyannis)", "Hyannis", "MA", BODY)
    F.apply_facility(j)
    assert j.hospital_name == F.CAPECOD and FT.apply_job(j) == "detail-office" and j.hospital_name == F.CAPECOD
    assert F.capecod_title_town("Urgent Care Technician (Per Diem, Multiple Locations)") == ""
    assert F.capecod_title_town("Surgical Coordinator, Plastic Surgery (Per Diem) (Masphee)") == "Mashpee"


def test_norton_sound_village_towns():
    assert F.norton_read("Clinician I - Unalakleet", BODY, "Nome", "AK")[:3] == (F.NORTON, "Unalakleet", "AK")
    assert F.norton_read("Clinic Travel Specialist (Relief) - Brevig", BODY, "Nome", "AK")[1] == "Brevig Mission"
    assert F.norton_read("Grants Administrator", BODY, "Nome", "AK") is None
    assert F.norton_read("Patient Accounts Representative I - Finance", BODY, "Nome", "AK") is None
    assert F.norton_read("Clinician I - Unalakleet", BODY, "Anchorage", "AK") is None


# ── safety ──────────────────────────────────────────────────────────────────
def test_body_less_night_keeps_the_stored_decision():
    FT.set_stored([{"hospital_system": F.SHARP, "job_id": "77", "hospital_name": F.SHARP, "city": "San Diego",
                    "state": "CA"},
                   {"hospital_system": F.HENDRICK, "job_id": "78", "hospital_name": "Hendrick Medical Center South",
                    "city": "Abilene", "state": "TX"}])
    j = _job(F.SHARP, "RN - ICU - Sharp Memorial Hospital", "San Diego", "CA", "", job_id="77")
    assert F.apply_facility(j) == "stored" and j.hospital_name == F.SHARP
    assert FT.apply_job(j) == "kept-label" and j.hospital_name == F.SHARP
    j = _job(F.HENDRICK, "RN, 2E Med/Tele", "Abilene", "TX", "", job_id="78")
    assert F.apply_facility(j) == "stored"
    assert FT.apply_job(j) == "kept" and j.hospital_name == "Hendrick Medical Center South"


def test_a_raising_reader_keeps_the_row(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen row")
    monkeypatch.setitem(F.READERS, F.HENDRICK, boom)
    j = _job(F.HENDRICK, "RN -CCU / ICU - South", "Abilene", "TX", BODY)
    stats = F.apply_facilities([j])
    assert stats == {F.HENDRICK: {"error": 1}}
    assert (j.hospital_name, j.city, j.state) == (F.HENDRICK, "Abilene", "TX")


def test_other_systems_and_named_rows_are_untouched():
    j = _job("Banner Health", "RN - ICU - Sharp Memorial Hospital", "San Diego", "CA", BODY)
    assert F.apply_facility(j) == "" and j.hospital_name == "Banner Health"
    j = _job(F.SHARP, "RN - ICU - Sharp Memorial Hospital", "San Diego", "CA", BODY, name="Sharp Mesa Vista Hospital")
    assert F.apply_facility(j) == "" and j.hospital_name == "Sharp Mesa Vista Hospital"


def test_finalize_runs_the_pass_before_the_town_tables():
    src = inspect.getsource(scraper.finalize_jobs)
    i = src.index("facilities_fac5c.apply_facilities")
    assert src.index("facilities_systems_b.apply_facilities") < i < src.index("facilities_town.apply_jobs")
    assert "_facility_pass(\"Phase 5 C\"" in src
