"""Fix 10 (2026-10-14, owner: "okay those should be properly sorted"): the
physician / CRNA / pharmacist / executive pay floor reads the title's ROLE,
not every high word in it. Titles and pay lines are the real ones from the
10-14 run log and the hospital_jobs rows named beside them. No network, no
database."""
import logging

import pytest

import scraper
from scraper import extract_posted_wage, extract_posting_facts, role_floor_high_title, Job

FLOOR = "under the physician / CRNA / pharmacist / executive floor ($40 an hour, $100,000 a year)"
FILLER = "\n\n" + "The team cares for patients on a busy unit and supports the department. " * 30


def _job(**kw):
    base = dict(title="Registered Nurse", hospital_system="Test System", hospital_name="Test System",
                city="Springfield", state="MO", location="Springfield, MO", specialty="", job_type="",
                url="https://example.org/job/1", job_id="1", posted_date="", description="", ats_platform="Test")
    base.update(kw)
    return Job(**base)


def _rejections(caplog):
    return [r.getMessage() for r in caplog.records if "Pay rejected" in r.getMessage()]


# ── the title's role ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("title", [
    # a physician setting or function, not a physician (10-14 run log)
    "Licensed Practical Nurse Physician Office",                       # Advocate 15267942
    "CMA  - Physician's Office (Mahomet) FT",                          # Gibson 35488263
    "Physician Office Front Desk Float Pool",                          # AdventHealth 681857
    "Physician Office Supervisor",                                     # AdventHealth 12062716
    "Physician Payment Reviewer",                                      # AdventHealth 36288095
    "Licensed Practical Nurse - Pediatrics - Physician Practice",      # Hackensack 36367936
    "Clinical Supervisor - Urgent Care - Physician Practice",          # Hackensack 35421007
    "Patient Service Rep I - Physician Practice Central Ops",          # Northern Light 37597448
    "Radiation Therapist - Radiation Oncology Physician's Clinic - FT",
    "Athletic Trainer / Physician Extender",
    "Receptionist, Physician Group; Days; JS Cardiology",
    "Physician Referral Representative",
    "Director, Physician Network",
    "Nurse Clinician - Carolinas Hospitalist Group - Full Time - AH Union",
    # a setting in a title that names another role (fix 10 review)
    "DIRECTOR-HOSPITALIST PRACTICE",                                   # Forrest 28621071: runs the group's business operations
    "Director Physician Group 2",                                      # FMOL 32118481
    "Coord, Physicians Group Practice - Neuro Surgery (Full-Time, 40 Hours, Day Shift)",   # Queen's 35493316
    "Security Officer - HS - PE Physicians Enterprise StradaBldg - USA Health Physicians Group",
    "Mgr- Physician Practice",                                         # Northern Light 38094470
    "Physician Practice Reps - FT & Flex in Panama City Beach",
    "Registered Dietician Physician Practice",
    "Certified Athletic Trainer (Physician Practice) - Ortho & Sports Medicine Clinic - FT - Day",
    "Physician Practice Controller - Finance",
    "Director Physician Practice Operations",
    "Physician Practice Operations",                                   # a function after a place
    # a labelled location is not a title
    "Site: Mass General Brigham Community Physicians, Inc.",
    "Site: Wentworth-Douglass Physician Corporation",
    # another role
    "Clinical Nurse II:Medical/Surgical Hospitalist (M4)- 36 hrs/wk, Nights",   # Albany Med 34170593
    "Hospitalist APP",
    "Advanced Practice Provider - Hospitalist",
    "Student Registered Nurse Anesthetist - APP Pathway",              # a CRNA student is still a student
    "CRNA Resident",
    "Certified Nurse Midwife OB Hospitalist - Part Time",
    "Pathologists' Asst",
    "Anesthesiologist Asst PRN Pflugerville",
    "House Officer (Limited Permit Physician) - Dept. Medicine",      # Wyckoff 27998233
    "Graduate Pharmacist",                                             # Bassett 28998848
    "PHARMACIST - LICENSE PENDING - PRN",
    # never a high role
    "Pharmacy Technician",
    "Executive Assistant",
    "Executive Assistant to the CEO",
    "Chief of Staff to the CEO",
])
def test_not_the_floors_role(title):
    assert role_floor_high_title(title) is False
    assert scraper.role_floor_reason(title, (20.0, 28.0, "hour")) is None
    assert scraper.role_floor_reason(title, (53601.6, 53601.6, "year")) is None


@pytest.mark.parametrize("title", [
    "OB Hospitalist Full Time",                                        # Luminis 35353358, $19-$23 stays refused
    "Physician - Inpatient Psychiatry- Per Diem  ",                    # Luminis 35353433
    "Hospice Team Physician",
    "Physician Advisor",
    "Physician Reviewer, Select Health",
    "Clinical Physician Executive",
    "Physician Clinic Medical Director",                               # Concentra: a medical director keeps the floor
    "Medical Director - Adult Primary Care - UVA Physicians Group",
    "CARDIOLOGY PHYSICIAN (FT; 40hrs/wk) - Temple Physicians Inc. @ Chestnut Hill Cardiology",
    "Rural Cardiologist Practice with the Oklahoma Heart Institute (Cushing, OK)",
    # a setting alone does not take the floor away (fix 10 review)
    "MD Physician Group",                                              # Wellstar 22077352: a staff pathologist
    "MD Physician Group PRN",                                          # Wellstar 22077358
    "Physician Practice Opportunities",                                # UnityPoint 35495110: recruiting physicians
    "Physician Partner - Dermatology",
    "Locum Physician Group Opportunity",
    "Nocturnist Physician Group",
    "Urgent Care Physician Clinic",
    "HealthLinkNow is currently looking for Psychiatrists interested in providing Telemedicine in California",
    "Physician - Family Medicine - Gerontology | Nursing Homes",
    "CRNA - Main OR",
    "Certified Registered Nurse Anesthetist (CRNA)",                  # a CRNA, not an RN
    "NURSE ANESTHETIST NURSE SR NEX",
    # an advanced practice provider who is a CRNA is a CRNA (fix 10 review)
    "APP - CRNA - Blodgett",                                           # Corewell 29387417
    "APP - Pediatric CRNA - Helen DeVos Children's Hospital",          # Corewell 29387089
    "APP - CRNA | Anesthesia",                                         # Sanford 2633395
    "APP Nurse Anesthetist/UKHC",                                      # UK HealthCare 35499264
    "Advanced Practice Provider - OB CRNA, North",                     # Community Health Network 35498591
    "APRN - CRNA - Main OR",
    "Staff Pharmacist Part Time",                                      # UnitedHealth 37648689: a tech's $16-$29 band
    "Advanced Practice Pharmacist - Specialty Pharmacy",
    "Pharmacy Supervisor - Pharmacist - FT",
    "Chief Executive Officer",
    "VP Operations - NE Florida Region",
    "VP, Regional Nurse Executive",
    "Academic Child and Adolescent Psychiatrist Asst/Assoc/Full Prof | Clinician Educator",
])
def test_the_floors_role(title):
    assert role_floor_high_title(title) is True
    assert scraper.role_floor_reason(title, (19.0, 23.0, "hour")) == FLOOR
    assert scraper.role_floor_reason(title, (55000.0, 75000.0, "year")) == FLOOR
    assert scraper.role_floor_reason(title, (150.0, 200.0, "hour")) is None


def test_a_cna_in_a_physician_office_meets_the_cna_ceiling():
    # the CNA / PCT ceiling is reached once the physician word is a setting
    assert scraper.role_floor_reason("CNA - Physician's Office", (55.0, 60.0, "hour")) == \
        "over the CNA / PCT / EVS / dietary ceiling ($45 an hour)"
    assert scraper.role_floor_reason("CNA - Physician's Office", (18.0, 22.0, "hour")) is None


# ── the text path: the six rows that get their pay back ─────────────────────

@pytest.mark.parametrize("title,line,want", [
    ("Licensed Practical Nurse Physician Office",                                  # Advocate 15267942
     "Schedule Details/Additional Information: Days\nPay Range $23.65 - $35.50\nMajor Responsibilities:",
     (23.65, 35.5, "hour")),
    ("Licensed Practical Nurse - Oncology - Physician Practice",                   # Hackensack 35420694
     "Compensation\nMinimum rate of $29.38 Hourly\nHMH is committed to pay equity", (29.38, 29.38, "hour")),
    ("Clinical Supervisor - Urgent Care - Physician Practice",                     # Hackensack 35421007
     "Compensation\nMinimum rate of $53,601.60 Annually\nHMH is committed to pay equity", (53601.6, 53601.6, "year")),
    ("Patient Service Rep I - Physician Practice Central Ops",                     # Northern Light 37597448
     "Work Schedule: 5:00 AM to 1:30 PM\nSalary Range : $18.25 - $21.45\nExempt: No", (18.25, 21.45, "hour")),
])
def test_setting_titles_keep_their_pay(caplog, title, line, want):
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        assert extract_posted_wage(f"{title}\n{line}") == want
    assert _rejections(caplog) == []


def test_real_physician_placeholders_stay_refused(caplog):
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        assert extract_posted_wage("OB Hospitalist Full Time\nPay Range: $19.00 - $23.00 per hour") is None
        assert extract_posted_wage("Certified Registered Nurse Anesthetist (CRNA)\nPay: $25.00 - $35.00 per hour") is None
    msgs = _rejections(caplog)
    assert len(msgs) == 2 and all(FLOOR in m for m in msgs)
    assert any("'OB Hospitalist Full Time'" in m and "19-23 hour" in m for m in msgs)


def test_an_app_crna_split_salary_stays_refused(caplog):
    # Corewell 29387089 prints "$28 5,000" for $285,000. The CRNA floor is
    # what keeps $28 an hour off the row; "APP" does not lift it.
    title = "APP - Pediatric CRNA - Helen DeVos Children's Hospital"
    body = ("Join our dedicated team of pediatric anesthesia providers at Helen DeVos Children's Hospital.\n\n"
            "Competitive annual base salary of $28 5,000 . Apply today and learn more about our competitive "
            "rates and earning potential opportunities!\n\nCorewell Health is offering a $100,000 starting bonus "
            "for a three-year commitment - with a relocation allowance of up to $12,000.\n\n"
            "CME allowance of $2,500 annually with rollover of unused funds with an annual maximum of $5,000.")
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        assert extract_posted_wage(f"{title}\n{body}") is None
    msgs = _rejections(caplog)
    assert len(msgs) == 1 and FLOOR in msgs[0] and "28-28 hour" in msgs[0]
    d = scraper.normalize_job(_job(title=title, hospital_system="Corewell Health", description=body + FILLER))
    assert d["wage_min"] is None and d["wage_unit"] is None


def test_normalize_job_stores_the_physician_office_lpn_pay():
    j = _job(title="Licensed Practical Nurse Physician Office", hospital_system="Advocate Health",
             description="Pay Range $23.65 - $35.50\nMajor Responsibilities: Assists in the assessment." + FILLER)
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (23.65, 35.5, "hour")
    assert d["posting_facts"]["pay_src"] == "text"


# ── the pay note reads the job's own title, quietly ──────────────────────────

LHC_OPENING = ("Explore opportunities with Elite Hospice, a part of LHC Group, a leading post-acute care partner "
               "for hospitals, physicians and families nationwide. As members of the Optum family of businesses, "
               "we are dedicated to helping people feel their best.")
MGB_OPENING = "Site: Mass General Brigham Community Physicians, Inc."


def test_body_labels_are_not_titles_for_the_pay_note(caplog):
    # UnitedHealth's LHC Group opening and Mass General Brigham's "Site:" line
    # were the first line the "up to" note read as a title: 29 of the 43
    # "Pay rejected" lines of the 10-14 run, all on rows whose stored pay
    # normalize_job read under the real title.
    lhc = LHC_OPENING + "\n\nThe hourly pay for this role will range from $16.00 to $22.60 per hour based on full-time employment." + FILLER
    mgb = MGB_OPENING + "\n\nPay Range\n$22.75 - $24.94/Hourly\n\nGrade\n3" + FILLER
    up_to = LHC_OPENING + "\n\nPay: Up to $22.60 per hour" + FILLER
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        extract_posting_facts(lhc, None, "CNA Hospice PRN")
        extract_posting_facts(mgb, None, "Medical Assistant, Primary Care")
        assert extract_posting_facts(up_to, None, "CNA Hospice PRN")["pay_note"] == "up_to"
        # a refused figure is logged once, by normalize_job's read, not again by the note's
        extract_posting_facts("Pay Range: $19.00 - $23.00 per hour" + FILLER, None, "OB Hospitalist Full Time")
    assert _rejections(caplog) == []


def test_log_rejects_off_is_quiet_but_still_refuses(caplog):
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        assert extract_posted_wage("OB Hospitalist Full Time\nPay Range: $19.00 - $23.00 per hour",
                                   log_rejects=False) is None
    assert _rejections(caplog) == []
