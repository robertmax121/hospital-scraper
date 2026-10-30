"""2026-10-29 (facility scale, phase 5 group A): the hospital or named site on
the rows UT Southwestern, NYU Langone, MaineHealth, University of Iowa
Hospitals and Clinics, WVU Medicine, St. Luke's, UMass Memorial, Novant,
Corewell and Beth Israel Lahey still filed under the system name, the towns a
posting's own text gives a label row, the UMass town table and UCSF Health's
Oracle work locations (facilities_fac5_a).

Fixture (tests/fixtures/facility_fac5a_1029/stored_rows.json): real stored
label rows of hospital_jobs read 2026-10-29 (bodies cut to 9,000 characters
where the cut body gives the same answer), each with the answer of the
stored-row change list (facility_scale_2026-10-29/phase5/changes/a.tsv, the dry
replay through finalize_jobs; null = the row keeps its stored name and town)
and decide()'s own answer ("decide"; the UMass town-table rows are named by
facilities_town, after it). No network, no database."""
import json
import os
import re

import pytest

import facilities_fac5_a as A
import facilities_oracle_other as OO
import facilities_text as T
import facilities_town as W
import facility_names
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_fac5a_1029", "stored_rows.json")
with open(FIX, encoding="utf-8") as _f:
    _DATA = json.load(_f)
ROWS = _DATA["rows"]


@pytest.fixture(autouse=True)
def _empty_stored_maps():
    T.set_known_facilities([])
    W.set_stored([])
    A.set_known_facilities([])
    yield
    T.set_known_facilities([])
    W.set_stored([])
    A.set_known_facilities([])


def _job(r, **kw):
    d = dict(title=r["title"], hospital_system=r["hospital_system"], hospital_name=r["hospital_system"],
             city=r["city"], state=r["state"], location=f"{r['city']}, {r['state']}", specialty="", job_type="",
             url=r["url"], job_id=str(r["job_id"]), posted_date="", description=r["description"], ats_platform="X")
    d.update(kw)
    return Job(**d)


def _row(system, pred=lambda r: True, named=None):
    for r in ROWS:
        if r["hospital_system"] == system and pred(r) and (named is None or bool(r["expect"]) == named):
            return r
    pytest.skip(f"no fixture row for {system}")


# ── The stored rows give the change list's answer ───────────────────────────
@pytest.mark.parametrize("r", ROWS, ids=lambda r: f"{r['hospital_system'][:12]}-{r['id']}")
def test_stored_row_answer(r):
    name, city, state, how = A.decide(r["hospital_system"], r["hospital_system"], r["city"], r["state"],
                                      r["title"], r["description"], r["url"])
    got = [name, city, state] if name else None
    assert got == r["decide"], (r["title"], how)


def test_fixture_covers_every_system_and_both_answers():
    systems = {r["hospital_system"] for r in ROWS}
    assert systems == set(A.SYSTEMS)
    for s in systems:
        if s in (A.BILH,):
            continue
        assert any(r["expect"] for r in ROWS if r["hospital_system"] == s), s
        assert any(not r["expect"] for r in ROWS if r["hospital_system"] == s), s


def _want(r):
    return r["expect"] or [r["hospital_system"], r["city"] or "", r["state"] or ""]


@pytest.mark.parametrize("system", sorted({r["hospital_system"] for r in ROWS}))
def test_finalize_gives_the_change_list_answer(system):
    rows = [r for r in ROWS if r["hospital_system"] == system]
    out = scraper.finalize_jobs([_job(r) for r in rows])
    by = {o["job_id"]: o for o in out}
    for r in rows:
        o = by[str(r["job_id"])]
        assert o["hospital_system"] == r["hospital_system"]
        assert [o["hospital_name"], o.get("city") or "", o.get("state") or ""] == _want(r), r["title"]


def test_every_name_is_a_table_site_with_a_sourced_town():
    for system, sites in A.SITES.items():
        for name, (town, st, src) in sites.items():
            assert name and town and len(st) == 2 and src, (system, name)
            assert re.match(r"(cms \d{6}|address |board)", src), (system, name, src)
            assert name != system
            assert "—" not in name and "–" not in name


# ── Through finalize_jobs (the hook) ────────────────────────────────────────
def test_finalize_on_a_night_without_bodies_keeps_the_stored_answers():
    rows = [r for r in ROWS if r["expect"] and r["expect"][0] != r["hospital_system"]]
    stored = [{"hospital_system": r["hospital_system"], "job_id": r["job_id"], "hospital_name": r["expect"][0],
               "city": r["expect"][1], "state": r["expect"][2]} for r in rows]
    W.set_stored(stored)
    A.set_known_facilities(stored)
    out = scraper.finalize_jobs([_job(r, description="") for r in rows])
    by = {o["job_id"]: o for o in out}
    for r in rows:
        o = by[str(r["job_id"])]
        assert o["hospital_name"] == r["expect"][0], r["title"]


def test_a_row_already_naming_a_site_is_left_alone():
    r = _row(A.SLUHN, named=True)
    assert A.decide(r["hospital_system"], "St. Luke's Hospital - Monroe Campus", r["city"], r["state"],
                    r["title"], r["description"], r["url"])[3] == "other"


def test_other_systems_are_not_touched():
    j = Job(title="RN - ICU", hospital_system="Duke Health", hospital_name="Duke Health", city="Durham", state="NC",
            location="Durham, NC", specialty="", job_type="", url="", job_id="1", posted_date="",
            description="Clements University Hospital", ats_platform="X")
    assert A.apply_facility(j) == ""
    assert j.hospital_name == "Duke Health"


# ── Nights without a body ───────────────────────────────────────────────────
def test_blank_body_keeps_the_stored_site():
    r = _row(A.MAINE, lambda r: bool(r["decide"]), named=True)
    A.set_known_facilities([{"hospital_system": r["hospital_system"], "job_id": r["job_id"],
                             "hospital_name": r["expect"][0], "city": r["expect"][1], "state": r["expect"][2]}])
    j = _job(r, description="")
    assert A.apply_facility(j) == "kept"
    assert [j.hospital_name, j.city, j.state] == r["expect"]


def test_blank_body_without_a_stored_site_keeps_the_label():
    r = _row(A.MAINE, named=True)
    j = _job(r, description="")
    A.apply_facility(j)
    assert j.hospital_name == r["hospital_system"]


def test_blank_body_keeps_a_stored_town_over_a_site_name_in_the_town_field():
    A.set_known_facilities([{"hospital_system": A.WVU, "job_id": "R1", "hospital_name": A.WVU,
                             "city": "Clarksburg", "state": "WV"}])
    j = Job(title="Home Health Aide", hospital_system=A.WVU, hospital_name=A.WVU,
            city="Home Health Davisson Run Road", state="WV", location="", specialty="", job_type="", url="",
            job_id="R1", posted_date="", description="", ats_platform="workday")
    assert A.apply_facility(j) == "kept"
    assert (j.hospital_name, j.city, j.state) == (A.WVU, "Clarksburg", "WV")


def test_stored_site_does_not_follow_a_posting_to_another_real_town():
    A.set_known_facilities([{"hospital_system": A.COREWELL, "job_id": "C1", "hospital_name": "Meijer Heart Center",
                             "city": "Grand Rapids", "state": "MI"}])
    j = Job(title="RN", hospital_system=A.COREWELL, hospital_name=A.COREWELL, city="Holland", state="MI",
            location="Holland, MI", specialty="", job_type="", url="", job_id="C1", posted_date="",
            description="", ats_platform="workday")
    A.apply_facility(j)
    assert (j.hospital_name, j.city) == (A.COREWELL, "Holland")


def test_a_row_that_raises_keeps_its_name(monkeypatch):
    r = _row(A.SLUHN, named=True)
    j = _job(r)

    def boom(*a, **k):
        raise RuntimeError("unforeseen row")
    monkeypatch.setattr(A, "decide", boom)
    stats = A.apply_facilities([j])
    assert j.hospital_name == r["hospital_system"]
    assert stats[r["hospital_system"]]["error"] == 1


# ── Readers ─────────────────────────────────────────────────────────────────
def test_utsw_title_site_line_and_other_employers():
    assert A.utsw_read("Clinical Staff Pharmacist, Acute Care - Zale, Nights", "")[0] == "Zale Lipshy Pavilion"
    assert A.utsw_read("RN Operating Room ENT", "Operating Room RN Circulator\n\nClements University Hospital\n\n"
                       "$10,000 Sign-on Bonus")[0] == "William P. Clements Jr. University Hospital"
    assert A.utsw_read("Oncology RN", "Location : UTSW Simmons Comprehensive Cancer Center - Dallas, TX")[0] == \
        "UT Southwestern Simmons Comprehensive Cancer Center"
    # a faculty posting covering Parkland and Clements names neither
    assert A.utsw_read("Neuro Radiologist", "Division faculty provide services at Parkland Memorial Hospital, "
                       "Clements University Hospital, and their affiliated outpatient clinics.")[0] is None
    # two UTSW sites name none
    assert A.utsw_read("RN Float Pool - ICU", "supporting staffing across Clements University Hospital and "
                       "Zale Lipshy Pavilion")[0] is None
    # a mention that places nothing ("across from")
    assert A.utsw_read("Chief", "across from UTSW's William P. Clements Jr. University Hospital.")[0] is None
    assert A.utsw_read("OB/GYN Maternal-Fetal Medicine Physician (Parkland Hospital)", "")[0] is None


def test_nyu_unit_codes_and_offices():
    assert A.nyu_read("Patient Care Technician-KP 18 Oncology-Day Shift", "")[0] == "Kimmel Pavilion"
    assert A.nyu_read("Secretary II-TH6 OR-Day Shift (35 Hours)", "")[0] == "Tisch Hospital"
    assert A.nyu_read("2026 New Graduate RN Posting - Tisch/Kimmel", "")[0] is None
    assert A.nyu_read("Sec II-Intake/Sched (FGP) - Manhattan, Tisch Ctr Essex Crossing", "")[0] is None
    assert A.nyu_read("Medical Assistant - Manhattan, Langone Orthopedic Center", "")[0] is None
    assert A.nyu_read("New Graduate RN Posting - Orthopedic Hospital", "")[0] == "NYU Langone Orthopedic Hospital"


def test_uihc_main_campus_is_the_system_name_and_sites_and_towns():
    assert A.uihc_read("Staff Nurse", "Location: University Campus")[1] == "main"
    assert A.uihc_read("Staff Nurse", "Location: Downtown Campus - 500 E Market Street, Iowa City, IA")[0] == \
        "University of Iowa Health Care Medical Center Downtown"
    assert A.uihc_read("Staff Nurse", "Location: Medical Center North Liberty")[0] == \
        "University of Iowa Health Care Medical Center North Liberty"
    assert A.uihc_read("Staff Nurse", "Location: University Campus, Downtown Campus, or North Liberty Campus")[1] == \
        "multi"
    name, how, _v, town = A.uihc_read("Medical Assistant", "Location: Coralville, IA (or surround clinics)")
    assert name is None and town == ("Coralville", "IA")
    got = A.decide(A.UIHC, A.UIHC, "Iowa City", "IA", "Medical Assistant",
                   "Location: Bettendorf, IA ( and surround clinics/locations based on clinic needs)")
    assert got[:3] == (A.UIHC, "Bettendorf", "IA")
    assert A.uihc_read("Staff Nurse", "Location: Iowa City, IA and Coralville, IA ( or surround clinics)")[3] is None


def test_wvu_site_segment_and_address_town():
    url = "https://wvumedicine.wd1.myworkdayjobs.com/WVUH/job/Wetzel-County-Hospital-WTZ/X_R1"
    assert A.wvu_read("Part-time, Emergency Medicine Advanced Practice Professional", "", url)[0] == \
        "Wetzel County Hospital"
    url2 = "https://wvumedicine.wd1.myworkdayjobs.com/WVUH/job/Reynolds-Memorial-Hospital-RMH/X_R2"
    assert A.wvu_read("Recruiter - Reynolds/Wetzel", "", url2)[0] is None
    body = "Company:\nTHOM Thomas Hospitals\n\nAddress:\n500 Donnally Street\n\nCharleston\n\nWest Virginia\n"
    assert A.wvu_address_town(body) == ("Charleston", "WV")
    got = A.decide(A.WVU, A.WVU, "Neurodevelopmental Center", "WV", "Therapist", body)
    assert got[:3] == (A.WVU, "Charleston", "WV")
    # a real town that is not the stamp is the row's own
    assert A.decide(A.WVU, A.WVU, "Wheeling", "WV", "Therapist", body)[0] is None


def test_st_lukes_hospital_streets_and_guards():
    u = "https://sluhn.wd1.myworkdayjobs.com/SLUHN/job/Easton-PA---1872-St-Lukes-Blvd/X_R1"
    assert A.sluhn_read("Cath Lab Technologist Full-Time Day Shift", "", u)[0] == "St. Luke's Hospital - Anderson Campus"
    assert A.sluhn_read("Experienced Registered Nurse, Infusion, Opportunities at Select Campuses", "", u)[0] is None
    assert A.sluhn_read("MRI East Region Float Technologist Full-Time", "", u)[0] is None
    assert A.sluhn_read("Phlebotomist, Easton/Palmerton", "", u)[0] is None
    office = "https://sluhn.wd1.myworkdayjobs.com/SLUHN/job/Allentown-PA---1110-American-Parkway/X_R2"
    assert A.sluhn_read("Accountant", "", office)[0] is None
    gsl = "https://sluhn.wd1.myworkdayjobs.com/SLUHN/job/Orwigsburg-PA---100-Paramount-Blvd/X_R3"
    assert A.sluhn_read("Patient Care Assistant (Full-Time, Days)", "", gsl)[0] == "Geisinger St. Luke's Hospital"


def test_umass_nashoba_site_and_facility_line():
    u = "https://ummh.wd1.myworkdayjobs.com/Careers/job/Nashoba-SEF---Groton-MA/Registered-Nurse_R1"
    got = A.decide(A.UMASS, A.UMASS, "Groton", "MA", "Registered Nurse, Nashoba Valley, 24 Hour Days", "", u)
    assert got[:3] == ("UMass Memorial Medical Center at Nashoba Valley", "Groton", "MA")
    body = "Facility: UMass Memorial Health – Harrington Hospital\nSchedule: Days"
    assert A.decide(A.UMASS, A.UMASS, "Southbridge", "MA", "Respiratory Therapist", body)[:3] == \
        ("UMass Memorial Health - Harrington Hospital", "Southbridge", "MA")
    # the same line on a posting in another town is that hospital's clinic or office
    assert A.decide(A.UMASS, A.UMASS, "Charlton", "MA", "Medical Assistant", body)[0] is None


def test_umass_town_table_places_hospital_titles_only():
    spec = W.SPECS[A.UMASS]
    assert spec.town("Milford", "MA")[0] == "Milford Regional Medical Center"
    assert spec.town("Clinton", "MA")[0] == "UMass Memorial Health - HealthAlliance-Clinton Hospital"
    assert spec.town("Worcester", "MA") is None
    base = dict(specialty="", job_type="", url="", posted_date="", ats_platform="workday")
    jobs = [Job(title="Registered Nurse, Med Surg, Marlborough, Nights, 36-Hours", hospital_system=A.UMASS,
                hospital_name=A.UMASS, city="Marlborough", state="MA", location="Marlborough, MA", job_id="M1",
                description="", **base),
            Job(title="Rad Tech, Medway Imaging - 32 hours, Days", hospital_system=A.UMASS, hospital_name=A.UMASS,
                city="Milford", state="MA", location="Milford, MA", job_id="M2", description="", **base),
            Job(title="Phlebotomist/Lab Support Tech - Per Diem, Outpatient Sites", hospital_system=A.UMASS,
                hospital_name=A.UMASS, city="Milford", state="MA", location="Milford, MA", job_id="M3",
                description="", **base),
            Job(title="Registered Nurse, ICU", hospital_system=A.UMASS, hospital_name=A.UMASS, city="Worcester",
                state="MA", location="Worcester, MA", job_id="M4", description="", **base)]
    by = {o["job_id"]: o["hospital_name"] for o in scraper.finalize_jobs(jobs)}
    assert by["M1"] == "UMass Memorial Health - Marlborough Hospital"
    assert by["M2"] == A.UMASS and by["M3"] == A.UMASS and by["M4"] == A.UMASS


def test_corewell_named_sites_and_offices():
    body = "Primary Location\nSITE - Rehab & Nursing Kentridge - 4118 Kalamazoo Ave SE - Grand Rapids\n\nDepartment"
    assert A.corewell_read("CNA", body)[0] == "Rehab and Nursing Center - Kentridge"
    office = "Primary Location\nSITE - Corewell Health Place - 100 Corewell Drive NW - Grand Rapids\n"
    assert A.corewell_read("Analyst", office)[0] is None
    cedar = "About Rehab & Nursing Center – Cedar\nText\nPrimary Location\nSITE - 1226 Cedar St NE - Grand Rapids\n"
    assert A.corewell_read("RN or LPN Long Term Care", cedar)[0] == "Rehab and Nursing Center - Cedar"
    assert A.corewell_read("RN", "Primary Location\nSITE - 1226 Cedar St NE - Grand Rapids\n")[0] is None


def test_bilh_herrick_house_and_boston_neighbourhoods():
    u = "https://bilh.wd1.myworkdayjobs.com/External/job/The-Herrick-House/LPN_R1"
    assert A.decide(A.BILH, A.BILH, "The Herrick House", "MA", "LPN - Night shift", "", u)[:3] == \
        ("The Herrick House", "Beverly", "MA")
    assert A.decide(A.BILH, A.BILH, "Boston", "MA", "Analyst", "Location: Haverhill, MA")[:3] == \
        (A.BILH, "Haverhill", "MA")
    assert A.decide(A.BILH, A.BILH, "Boston", "MA", "Analyst",
                    "Location: Schrafft City Center in Charlestown, MA on-site")[0] is None
    assert A.decide(A.BILH, A.BILH, "Boston", "MA", "Analyst",
                    "Location: Shared between Lynn, Beverly and Newburyport practices.")[0] is None


def test_maine_named_communities():
    assert A.maine_read("CNA - Quarry Hill", "Quarry Hill\nNursing/Clinical Support\n", "Camden", "ME")[0] == \
        "Quarry Hill"
    assert A.maine_read("Director - Thornton Oaks Independent Living Retirement",
                        "Senior Health Center\nManagement\n", "Brunswick", "ME")[0] is None
    assert A.maine_read("LPN - Mid Coast Senior Health Center", "Senior Health Center\nNursing\n",
                        "Brunswick", "ME")[0] == "Mid Coast Senior Health Center"


# ── UCSF Health: the Oracle work locations ──────────────────────────────────
@pytest.mark.parametrize("loc,want", [
    ("Moffitt Hospital - FL 05 - RM M524", A.PARNASSUS),
    ("Long Hospital - FL 07 - RM 701", A.PARNASSUS),
    ("505 PARNASSUS AVE M-487", A.PARNASSUS),
    ("ML RM M39C PHARMACY INPATIENT", A.PARNASSUS),
    ("Mission Bay Hospital - FL 04 - RM 4100", A.MISSION_BAY),
    ("Mt Zion Bldg B - FL 2 - RM 201", A.MOUNT_ZION),
    ("Children's Hospital Oakland - FL 1 - RM 1", "UCSF Benioff Children's Hospital Oakland"),
    ("UCSF Hyde Hospital  Main Bldg 68", "Saint Francis Memorial Hospital"),
    ("UCSF Stanyan Hospital", "St. Mary's Medical Center"),
    ("ACC Building - FL 03 - RM 301", ""),
    ("ML ACC 8FLR RM A808 NEUROSURGERY", ""),
    ("ML OP PHARMACY", ""),
    ("ZSFG Building 5 - FL 2 - RM 1", ""),
    ("Location Required - FL 1 - RM", ""),
    ("Mission Hall Building - FL 05 - RM 5160", ""),
])
def test_ucsf_work_locations(loc, want):
    nm = OO.oracle_location(A.UCSF, loc)
    got = facility_names.list_facility(A.UCSF, nm, "San Francisco", "CA", "Staff Nurse")
    assert (got or "") == want


def test_ucsf_main_names_still_hold():
    for loc, want in (("Saint Francis Memorial Hosp", "Saint Francis Memorial Hospital"),
                      ("Saint Marys Medical Center", "St. Mary's Medical Center")):
        assert facility_names.list_facility(A.UCSF, OO.oracle_location(A.UCSF, loc), "San Francisco", "CA", "") == want


def test_extend_oracle_is_all_or_nothing():
    systems = {A.UCSF: {"hospitals": {"UCSF Medical Center at Mission Bay": ("Oakland", "CA", "x")},
                        "sites": {}, "rules": ()}}
    before = json.dumps(systems, sort_keys=True)
    with pytest.raises(ValueError):
        A.extend_oracle(systems)
    assert json.dumps(systems, sort_keys=True) == before
    systems = {A.UCSF: {"hospitals": {}, "sites": {"Moffitt Hospital": "Somewhere Else"}, "rules": ()}}
    with pytest.raises(ValueError):
        A.extend_oracle(systems)
