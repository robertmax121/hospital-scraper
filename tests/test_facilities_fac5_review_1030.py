"""2026-10-30 (facility release 5, review A): the single-site verdicts that
were wrong (Union Hospital Regional, Rivers Health in Point Pleasant,
Children's National's Rehabilitation and Specialized Care Center, Billings
Clinic's affiliates) and the phase 5 names that went to roles no one site
holds (facilities_fac5_review).

Fixture (tests/fixtures/facility_fac5_review_1030/stored_rows.json): real
stored label rows of hospital_jobs read 2026-10-29 22:00-23:05 UTC (emails,
phone numbers and recruiters' names taken out), each with the answer of the
review's stored-row change list (facility_scale_2026-10-29/phase5/
release5_work/reviewA: ra.tsv names it, removed.tsv takes it out of a phase 5
list; null = the row stays on the system name). A UCSF row the phase 5 list
names from the live Oracle work location starts on that name, as the
adapter writes it. No network, no database."""
import json
import os

import pytest

import facilities_fac5_review as R
import facilities_town as FT
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_fac5_review_1030", "stored_rows.json")
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


# ── the stored rows come out as the review's change list writes them ─────────
def test_stored_rows_match_the_review_lists():
    jobs = [_adapter(_job(r["hospital_system"], r["title"], r["city"], r["state"], r["description"], r["url"],
                          r["job_id"], name=r.get("start_name"))) for r in ROWS]
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
    assert sum(1 for r in ROWS if r["want"] is None) >= 60


# ── single-site verdicts that were wrong ─────────────────────────────────────
def test_union_hospital_regional_by_its_department_code():
    for title in ("RN-UHR EMERGENCY ROOM", "TECH-MRI-REGISTERED-MRI UHR", "HOUSEKEEPER-UHR ENVIRONMENTAL SERVICES"):
        j = _job(R.UNION, title, "TERRE HAUTE", "IN", BODY)
        assert R.apply_facility(j) == "site"
        assert (j.hospital_name, j.city, j.state) == ("Union Hospital Regional", "TERRE HAUTE", "IN")
    for title in ("RN-MEDICAL CARE 4EC", "RN-ONU ORTHO/NEURO/URO 3EA", "THER - REG RESP THR OPT II PRN-RESPIRATORY THERAPY"):
        j = _job(R.UNION, title, "TERRE HAUTE", "IN", BODY)
        assert R.apply_facility(j) == ""
        assert j.hospital_name == R.UNION
    # another town than Terre Haute: never
    assert R.site_for(R.UNION, "RN-UHR EMERGENCY ROOM", BODY, "Clinton", "IN") is None


def test_rivers_health_is_point_pleasants_one_hospital():
    spec = FT.SPECS[R.CABELL]
    assert spec is R.CABELL_TOWNS
    got = FT.decide(R.CABELL, R.CABELL, "Point Pleasant", "WV", "Registered Nurse-ICU-FT", BODY)
    assert got[:3] == ("Rivers Health", "Point Pleasant", "WV")
    # Huntington holds two of the network's hospitals: kept
    assert FT.decide(R.CABELL, R.CABELL, "Huntington", "WV", "Registered Nurse-ICU-FT", BODY)[0] is None
    assert FT.decide(R.CABELL, R.CABELL, "Huntington", "WV", "RN - St. Mary's Medical Center ICU", BODY)[0] is None
    # a clinic's or a business office's posting keeps the label
    assert FT.decide(R.CABELL, R.CABELL, "Point Pleasant", "WV", "Medical Assistant - Family Practice", BODY)[0] is None
    j = _job(R.CABELL, "Deposit Preparer - Full time", "Point Pleasant", "WV", BODY)
    assert R.apply_facility(j) == "office-title" and j._town_skip


def test_childrens_national_rsc_by_its_workday_site():
    url = ("https://childrensnational.wd108.myworkdayjobs.com/CN_Careers/job/Rehabilitation-and-Specialized-Care-"
           "Bunker-Hill-Road/Respiratory-Therapist-II_R-00123")
    j = _job(R.CNH, "Respiratory Therapist II", "Washington", "DC", BODY, url)
    assert R.apply_facility(j) == "site"
    assert j.hospital_name == "Children's National Rehabilitation and Specialized Care Center"
    main = url.replace("Rehabilitation-and-Specialized-Care-Bunker-Hill-Road", "Childrens-National-Hospital-Main-Campus")
    j = _job(R.CNH, "Respiratory Therapist II", "Washington", "DC", BODY, main)
    assert R.apply_facility(j) == "" and j.hospital_name == R.CNH


def test_billings_clinic_affiliates_by_the_titles_own_name():
    cases = [
        ("Radiologic Technologist w/ CT Training - Daniels Memorial Healthcare, Scobey, MT", "Scobey", "MT",
         "Daniels Memorial Healthcare"),
        ("CEO - Vista Plains Health (Harlowton, Montana)", "Harlowton", "MT", "Vista Plains Health"),
        ("Radiologic Technologist - North Big Horn Hospital, Lovell, WY", "Lovell", "WY", "North Big Horn Hospital"),
    ]
    for title, city, st, want in cases:
        j = _job(R.BILLINGS, title, city, st, BODY)
        assert R.apply_facility(j) == "site"
        assert (j.hospital_name, j.city, j.state) == (want, city, st)
    # a posting in another town is not that affiliate's
    assert R.site_for(R.BILLINGS, "RN - Daniels Memorial Healthcare, Scobey, MT", BODY, "Billings", "MT") is None
    # a body that names an affiliate is not enough (the title must)
    j = _job(R.BILLINGS, "RN - ED/Acute Care (Full-time)", "Hardin", "MT", "RN at Big Horn Hospital. " + BODY)
    assert R.site_for(R.BILLINGS, j.title, j.description, j.city, j.state) is None


# ── roles no one site holds keep the system name ─────────────────────────────
@pytest.mark.parametrize("system,title", [
    ("DCH Health System", "Sourcing Specialist"),
    ("DCH Health System", "Supply Chain Analytics Manager"),
    ("DCH Health System", "Nursing Team Leader - Centralized Monitoring"),
    ("Appalachian Regional Healthcare", "Command Center Director"),
    ("Appalachian Regional Healthcare", "System Provider Access Manager"),
    ("Logan Health", "Senior Applications Specialist | Enterprise Resource Planning (ERP)"),
    ("Logan Health", "Performance Improvement Consultant"),
    ("Logan Health", "Durable Medical Equipment (DME) Inventory Specialist | Applied Health Services"),
    ("United Health Services", "LPN Education Incentive Program"),
    ("United Health Services", "Emergency Department Nursing Assistant – Wilson & BGH"),
    ("UCSF Health", "Assistant Unit Director - Pediatric Resource Team (Cross Bay)"),
    ("UCSF Health", "Physician Assistant - PICU (BCH-Mission Bay/Oakland)"),
    ("UCSF Health", "Anesthesia Tech II- (Float Oakland & Walnut Creek)"),
    ("Cincinnati Children's", "RN Clinical Manager –Liberty/Centerville Locations Ambulatory Subspecialty Sites"),
    ("Health First", "Registered Respiratory Therapist | Brevard County, FL"),
    ("Premier Health", "TRAVEL AT HOME-RESP THERP"),
    ("University of Iowa Hospitals and Clinics",
     "Staff Nurse - UI Health Care Iowa City Jefferson Street - Gastroenterology Clinic 50%"),
])
def test_scope_by_title(system, title):
    assert R.scope_by_title(system, title)


@pytest.mark.parametrize("system,title", [
    ("Hendrick Health", "RN, Centralized Support - North"),
    ("Shriners Children's", "Coordinator, Supply Chain"),
    ("UCSF Health", "Physician Assistant - Cardiac Surgery"),
    ("Cincinnati Children's", "Registered Nurse (RN), Psychiatry Float Pool - DECEMBER GRADS WELCOME!"),
    ("United Health Services", "Registered Nurse - Med Surg Neuro - Stepdown"),
    ("Health First", "Registered Respiratory Therapist (Variable Shift) – Holmes Regional"),
])
def test_not_scope_by_title(system, title):
    assert not R.scope_by_title(system, title)


def test_scope_by_body():
    assert R.scope_by_body("TITLE: RN – Float/Resource – Madras and Prineville REPORTS TO POSITION: Department Manager")
    assert R.scope_by_body("This position is required to float between multiple campus locations.")
    assert R.scope_by_body("The Clinical Education Department is a system-wide support service that provides ...")
    assert not R.scope_by_body("fosters the effective integration of efforts with system-wide initiatives.")
    assert not R.scope_by_body("These departments provide system-wide support including, but not limited to ...")
    assert not R.scope_by_body("Associates must be willing to float to other Health First facilities, as needed.")


def test_a_phase_5_name_goes_back_and_an_earlier_name_stays():
    # named by a phase 5 pass tonight (the snapshot had the label): back to the label, town kept
    j = _job("UCSF Health", "Assistant Unit Director - Pediatric Resource Team (Cross Bay)", "San Francisco", "CA", BODY)
    before = R.snapshot([j])
    j.hospital_name = "UCSF Benioff Children's Hospital Oakland"
    assert R.apply_facility(j, before) == "multi-title"
    assert (j.hospital_name, j.city) == ("UCSF Health", "San Francisco")
    # UCSF's three phase 5 building names come from the adapter: also back
    j = _job("UCSF Health", "Anesthesia Tech II- (Float Oakland & Walnut Creek)", "Oakland", "CA", BODY,
             name="UCSF Medical Center at Mission Bay")
    assert R.apply_facility(j, R.snapshot([j])) == "multi-title" and j.hospital_name == "UCSF Health"
    # a name an earlier family gave (not a phase 5 name) is left alone
    j = _job("UCSF Health", "Physician Assistant - PICU (BCH-Mission Bay/Oakland)", "Oakland", "CA", BODY,
             name="UCSF Benioff Children's Hospital Oakland")
    assert R.apply_facility(j, R.snapshot([j])) == ""
    assert j.hospital_name == "UCSF Benioff Children's Hospital Oakland"


def test_town_table_rows_are_told_to_keep_the_label():
    j = _job("St. Charles Health", "RN Resource Float", "Madras", "OR",
             "ST. CHARLES HEALTH SYSTEM JOB DESCRIPTION TITLE: RN – Float/Resource – Madras and Prineville " + BODY)
    assert R.apply_facility(j) == "scope-body" and j._town_skip
    assert FT.apply_job(j) == "detail-office" and j.hospital_name == "St. Charles Health"
    j = _job("St. Charles Health", "RN Preop Holding", "Bend", "OR", BODY)
    assert R.apply_facility(j) == "" and not getattr(j, "_town_skip", False)
    assert FT.apply_job(j) == "town" and j.hospital_name == "St. Charles Bend"


def test_other_systems_are_never_touched():
    for system in ("CommonSpirit Health", "Ascension", "Kaiser Permanente", "Sanford Health"):
        j = _job(system, "Sourcing Specialist - Command Center (Cross Bay)", "Somewhere", "TX", BODY)
        assert R.apply_facility(j) == ""
        assert j.hospital_name == system and not getattr(j, "_town_skip", False)


def test_a_row_the_pass_raises_on_keeps_its_name(monkeypatch):
    j = _job(R.UNION, "RN-UHR EMERGENCY ROOM", "TERRE HAUTE", "IN", BODY)

    def boom(*a, **k):
        raise RuntimeError("x")
    monkeypatch.setattr(R, "site_for", boom)
    stats = R.apply_facilities([j])
    assert stats[R.UNION] == {"error": 1}
    assert (j.hospital_name, j.city, j.state) == (R.UNION, "TERRE HAUTE", "IN")


def test_the_pass_runs_after_phase_5_and_before_the_town_tables():
    import inspect
    src = inspect.getsource(scraper.finalize_jobs)
    snap, a, c, rv, town = (src.index("facilities_fac5_review.snapshot"), src.index('"Phase 5 A"'),
                            src.index('"Phase 5 C"'), src.index('"Phase 5 review"'), src.index('"Town tables"'))
    assert snap < a < c < rv < town
