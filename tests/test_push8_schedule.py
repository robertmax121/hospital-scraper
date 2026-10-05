"""Push 8 / schedule (2026-10-04, plan items 1, 3 and 4): shift, hours and
job-type rules. Every text below is copied from a stored hospital_jobs
description (the row id and system are named), trimmed to the lines the rule
reads."""
import os

import pytest

import scraper as S

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "push8")


def _shift(text, title=None):
    f = S.extract_posting_facts(text, None, title) or {}
    return [x[0] for x in f.get("shift") or []]


def _facts(text, job_type=None):
    return S.extract_posting_facts(text, job_type) or {}


# ── item 3: shift ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("text, want", [
    # 29223868 Parkview (HealthcareSource schedule line, plural)
    ("Culture: World-class teamwork, Personalized health journeys for co-workers and family members\n\n"
     "Schedule: Nights shift · 7p-7a", "Nights"),
    # 33778514 Ascension (hyphen)
    ("Department/Specialty: Elmbrook Internal Medicine\n\nSchedule: Day-shift, part-time\n\n"
     "How you’ll make an impact in this role", "Days"),
    # 22076871 Wellstar (label line with no colon)
    ("Work Shift\nDay (United States of America)\n\nJob Summary:\nRegistered Nurse", "Days"),
    # 14749205 Prisma Health
    ("Specialty specific skills\n\nWork Shift\nVariable (United States of America)\n\nLocation\nGHS Eye Institute", "Variable"),
    # 29308365 Jefferson
    ("Other responsibilities may be assigned when circumstances require.\n\nWork Shift\nWorkday Day (United States of America)\n\n"
     "Worker Sub Type\nRegular", "Days"),
    # 14749732 Geisinger ("Rotation")
    ("Location:\nGeisinger Medical Center (GMC)\n\nShift:\nRotation (United States of America)\n\nScheduled Weekly Hours:\n40", "Rotating"),
    # 23658750 Stormont Vail (ordinal)
    ("Position Status:\nFull time\nShift:\nFirst Shift (Days - Less than 12 hours per shift) (United States of America)\n"
     "Hours per week:\n40", "Days"),
    # 35389557 Broward Health
    ("Broward Health Coral Springs\n\nCoral Springs FL\n\nShift: 1st (Days)\n\nFTE: .01 (Pool)", "Days"),
    # 35389464 Broward Health (shift number)
    ("Department: HC for the Homeless\n\nReq 34022\n\nShift: Shift 1\n\nFTE: 1.000000\n\nSummary:", "Days"),
    # 32709205 Lee Health (shift number + narrow no-break spaces)
    ("Department: Emergency Services\n\nWork Type: Full Time\n\nShift: Shift 3/7:00 PM to 7:30 AM\n\n"
     "Minimum to Midpoint Pay Rate: $33.37 - $45.05 / hour", "Nights"),
    # 32709495 Lee Health (seconds in the span)
    ("Department: Rehabilitation Services\n\nWork Type: Full Time\n\nShift: Shift 1/8:00:00 AM to 5:00:00 PM\n\n"
     "Hiring Range: $70,720.00 - $115,252,80 annually", "Days"),
    # 32484008 Jackson Health System (Phenom)
    ("or any other status protected by law.\n\nSchedule: Day Job", "Days"),
    # 34545508 UNC Health (Infor)
    ("Work Assignment Type: Onsite\n\nWork Schedule: Night Job\n\nLocation of Job: US:NC:Chapel Hill", "Nights"),
    # 32709715 Vandalia Health (Infor)
    ("• Basic Life Support (Required) Within 30 days of hire\n\nWork Schedule: Nights\n\nStatus: 72 hrs per pay", "Nights"),
    # 31600284 St. Luke's (24-hour span)
    ("Must be team orientated\n\nThree 12-hour shifts, 0700-1930.\n\nThe position comes with an excellent benefits package.", "Days"),
])
def test_shift_forms(text, want):
    assert _shift(text)[:1] == [want]


def test_rotating_spans_stay_rotating():
    # 27443666 (Nemours, via Valley Children's Oracle tenant)
    text = ("Nemours is seeking a Unit Clerk to join our CICU in Wilmington, DE ! This PART -TIME position consisting "
            "of 48 hours every 2 weeks. Rotating shifts, 0700-1930 and 1900-0730.")
    assert _shift(text)[0] == "Rotating"


@pytest.mark.parametrize("text", [
    # 32705884 BayCare: the legend under the field is no shift of its own
    "Shift 1 = Days, 2 = Evenings, 3 = Nights, 4 = Varies\n\nWeekend Work: Every Other",
    # a year range is no 24-hour span (Baptist Health Phenom boilerplate)
    "one of Fortune's 100 Best Companies to Work For, and in the 2025-2026 U.S. News & World Report Best Hospital Rankings",
    # a heading over prose is no labelled field
    "Shift\nNight differential applies to all hours worked after 7 o'clock for eligible staff.",
    "Shift: 12 hours",
])
def test_not_a_shift(text):
    assert _shift(text) == []


def test_shift_number_in_field_with_legend():
    # 32705884 BayCare: "Shift: Shift 1" with the legend below it -> Days only
    text = ("Status: Full Time, Exempt: No\n\nShift Hours: 6:45AM - 7:15PM\n\nShift: Shift 1\n\n"
            "Shift 1 = Days, 2 = Evenings, 3 = Nights, 4 = Varies\n\nWeekend Work: Every Other")
    assert _shift(text)[0] == "Days"


def test_schedule_line_outranks_prose():
    # Blythedale (iCIMS header "Shift: Day", JSON-LD prose names the
    # differentials for evening and night shifts)
    text = ("Certified nurse's aides are eligible for evening shift, night shift and New York State CNA "
            "certification differentials.\n\nSchedule: Day shift")
    assert _shift(text)[0] == "Days"


def test_variable_reaches_schedule_summary():
    # 32713457 Northern Light Health (Infor)
    f = _facts("Work Type: PRN\n\nHours Per Week: 36.00\n\nWork Schedule: Variable\n\nSalary Range : $65.00-$65.00")
    assert f["shift"][0][0] == "Variable"
    assert f["schedule"] == "36 hrs/wk · Variable"


# ── item 4: hours ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("text, job_type, want", [
    # 17284217 Geisinger (Workday template, label and value on two lines)
    ("Shift:\nNights (United States of America)\n\nScheduled Weekly Hours:\n36\n\nWorker Type:\nRegular", None, 36),
    # 22078018 OhioHealth (glued)
    ("Work Shift:NightScheduled Weekly Hours :36DepartmentPulmonary", None, 36),
    # 32712007 Middlesex Health
    ("Department: Emergency Dept Nursing\n\nHours: 24.00 per week\n\nShift: Days", None, 24),
    # 32713457 Northern Light Health
    ("Work Type: PRN\n\nHours Per Week: 36.00\n\nWork Schedule: Variable", None, 36),
    # 34545508 UNC Health
    ("Standard Hours Per Week: 40.00\n\nSalary Range: $24.98 - $35.91 per hour", None, 40),
    # 33926626 Owensboro Health
    ("Work Hours: 7a-7:30p | Status: Full Time - 0.9 FTE (72 hours per pay period) | Entity: Owensboro Health", None, 36),
    # 24566176 Enloe Health
    ("Days off: Fixed If fixed, days off: Saturday & Sunday\nHours per pay period: 80\n\nEnloe Health is a Level II", None, 40),
    # 32709715 Vandalia Health
    ("Work Schedule: Nights\n\nStatus: 72 hrs per pay\n\nLocation: Memorial Hospital", None, 36),
    # 32707347 Aspirus Health
    ("HOURS: Supplemental 0.2 FTE, 16 Hours Biweekly\n\nExperience/Qualifications", None, 8),
    # 32712323 Bayhealth
    ("Location: Sussex Campus Hospital\n\nStatus: Full Time 72 Hours\n\nShift: Nights", None, 36),
    # 34512519 AdventHealth
    ("Schedule: Full time, 36 hours\n\nShift: Days, 9:00am to 9:00pm", None, 36),
    # 931754 Cone Health (FTE on its own line)
    ("On Call Required\nYes\n\nFTE:\n0.67\n\nJob Type:\nBenefit Eligible (20-39) Hours/Week", "part_time", 27),
    # Montefiore form named in the plan (daily hours x work days)
    ("Scheduled Daily Hours: 7.5 HOURS\nWork Days: MON-FRI", None, 37.5),
    # two decimals in the plain form
    ("Full time, 40.00 hours per week, day shift", None, 40),
])
def test_hours_forms(text, job_type, want):
    assert S._hours_for_type(text, job_type) == want


@pytest.mark.parametrize("text, job_type", [
    # 33709242 Mercy: a benefits threshold
    ("matched retirement plans for team members working 32+ hours per pay period.", None),
    # 28950289 Owensboro: a threshold on a per-diem posting
    ("Status: Per Diem/PRN - 0.0 FTE (less than 40 hours per pay period) | Entity: Owensboro Health", "per_diem"),
    # 19020802 Cone Health: the PRN placeholder FTE
    ("Work Shift :\nFlexible Schedule (United States of America)\n\nFTE:\n0.0003\n\nJob Type:\nRelief (PRN)", "per_diem"),
    # 26727795 AnMed Health: eligibility, not the job's FTE
    ("*Varied benefits packages are available to positions with a 0.6 FTE or higher.", None),
    # DCH (iCIMS header): a range
    ("Position Type: Regular Full-Time (72 to 80 hours bi-weekly)", None),
    # a shift length is no weekly figure
    ("Full Time, 12 hour shifts, nights", None),
    # 26456177 Geisinger: zero scheduled hours
    ("Scheduled Weekly Hours:\n0\n\nWorker Type:\nRegular", None),
])
def test_hours_not_read(text, job_type):
    assert S._hours_for_type(text, job_type) is None


def test_hours_reach_schedule_summary():
    # 33926626 Owensboro Health
    f = _facts("Shift: Days\n\nStatus: Full Time - 0.9 FTE (72 hours per pay period) | Entity: Owensboro Health")
    assert f["hours"] == 36
    assert f["schedule"] == "36 hrs/wk · Days"


# ── Oracle requisition flex fields (items 1 and 4) ────────────────────────

def _oracle_item(**kw):
    it = {"ExternalDescriptionStr": "<p>" + "Provides patient care on the rehabilitation unit. " * 6 + "</p>",
          "JobSchedule": "Full time", "JobShift": "Day", "ExternalPostedStartDate": "2026-10-01"}
    it.update(kw)
    return it


def test_oracle_fte_flex_gives_hours():
    # Lifepoint requisition 358813 (probed 2026-10-04)
    flex = {"items": [{"Prompt": "FTE", "Value": "0.9 - 72 hr/pp (Full Time)"},
                      {"Prompt": "Position Type", "Value": "Primary"}]}
    desc, sched, _ = S._oracle_posting_text(_oracle_item(requisitionFlexFields=flex))
    assert "Schedule: Full time · Day shift · 36 hours per week" in desc
    assert S.extract_posting_facts(desc)["hours"] == 36


def test_oracle_prn_fte_gives_nothing():
    # Lifepoint requisition 358756: "0.001 - PRN/OnCall (PRN)"
    flex = {"items": [{"Prompt": "FTE", "Value": "0.001 - PRN/OnCall (PRN)"}]}
    desc, _, _ = S._oracle_posting_text(_oracle_item(JobSchedule="Part time", JobShift="Weekend", requisitionFlexFields=flex))
    assert "hours per week" not in desc


def test_oracle_flex_job_type_only_when_blank_and_named():
    # Tenet requisition 2603025185: Assignment Category "Part Time 2 - Per Diem"
    flex = {"items": [{"Prompt": "Assignment Category", "Value": "Part Time 2 - Per Diem"}]}
    _, sched, _ = S._oracle_posting_text(_oracle_item(JobSchedule=None, requisitionFlexFields=flex))
    assert sched == "Part Time 2 - Per Diem" and S.derive_job_type("", sched) == "per_diem"
    # Lifepoint Position Type "Primary" names no type
    flex = {"items": [{"Prompt": "Position Type", "Value": "Primary"}]}
    _, sched, _ = S._oracle_posting_text(_oracle_item(JobSchedule=None, requisitionFlexFields=flex))
    assert sched == ""
    # a JobSchedule is never replaced
    flex = {"items": [{"Prompt": "Assignment Category", "Value": "Part Time 2 - Per Diem"}]}
    _, sched, _ = S._oracle_posting_text(_oracle_item(JobSchedule="Part time", requisitionFlexFields=flex))
    assert sched == "Part time"


def test_fte_hours_values():
    # WellSpan "0.9" / "1"; Broward "1.000000"; Lifepoint PRN placeholder
    assert [S._fte_hours(v) for v in ("0.9", "1", "1.000000", "0.001 - PRN/OnCall (PRN)", "80 Hours", None)] == [36, 40, 40, None, None, None]


# ── iCIMS job page header (items 1, 3, 4) ─────────────────────────────────

def _icims_job(job_type=""):
    return S.Job(title="Patient Safety Attendant", hospital_system="Legacy Health", hospital_name="Legacy Health",
                 city="", state="", location="", specialty="", job_type=job_type,
                 url="https://careers-lhs.icims.com/jobs/49315/patient-safety-attendant-eh/job", job_id="49315",
                 posted_date="", description="", ats_platform="iCIMS")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


def test_icims_header_legacy():
    # careers-lhs.icims.com/jobs/49315: Avg Hours Per Week 36, FTE 0.90, Shift Night
    html = _read("icims_legacy_49315.html")
    hdr = S._icims_header(html)
    assert hdr["shift"] == "Night" and hdr["avg hours per week"] == "36" and hdr["fte"] == "0.90"
    job = _icims_job()
    assert S._icims_apply_page(job, html)
    assert "Schedule: Night shift · 36 hours per week" in job.description
    f = S.extract_posting_facts(job.description)
    assert f["shift"][0][0] == "Nights" and f["hours"] == 36


def test_icims_header_huntsville_shift_number_and_position_type():
    # careers-hhsys.icims.com/jobs/66341: Shift "3", Position Type "Regular Full-Time"
    html = _read("icims_huntsville_66341.html")
    job = _icims_job()
    assert S._icims_apply_page(job, html)
    assert "Schedule: Night shift" in job.description
    # the JSON-LD employmentType is "OTHER" (not kept), so the header's
    # Position Type fills the blank job_type
    assert job.job_type == "Regular Full-Time"
    assert S.derive_job_type(job.title, job.job_type) == "full_time"


def test_icims_position_type_never_replaces_a_job_type():
    # Legacy: the JSON-LD employmentType FULL_TIME is kept; a header type
    # would only fill a blank one, and a stored one is never replaced
    html = _read("icims_legacy_49315.html")
    job = _icims_job()
    S._icims_apply_page(job, html)
    assert job.job_type == "FULL_TIME"
    job = _icims_job(job_type="Part time")
    S._icims_apply_page(job, _read("icims_huntsville_66341.html"))
    assert job.job_type == "Part time"


def test_icims_header_day_outranks_body_prose():
    # careers-blythedale.icims.com/jobs/1567: header Shift "Day"; the body
    # names evening / night shift differentials
    job = _icims_job()
    assert S._icims_apply_page(job, _read("icims_blythedale_1567.html"))
    assert S.extract_posting_facts(job.description)["shift"][0][0] == "Days"


def test_icims_search_page_gives_no_header():
    # an expired Prime posting redirects to the search page, whose job cards
    # carry other postings' Shift / Position Type
    assert S._icims_header(_read("icims_prime_search_cards.html")) == {}


@pytest.mark.parametrize("value, want", [
    ("Days", "Day"), ("Night", "Night"), ("First Shift", "Day"), ("3", "Night"), ("Variable", "Variable"),
    ("1st (Days)", "Day"), ("8 Hour", ""), ("Days and Nights Available", ""), ("12 Hour Shift/Nights", ""),
])
def test_icims_shift_word(value, want):
    assert S._icims_shift_word(value) == want


def test_icims_header_hours_from_pay_period_fte():
    # Kettering header FTE "80 Hours Per Pay Period/FTE 1.0", Shift "First Shift"
    assert S._icims_header_lines({"shift": "First Shift", "fte": "80 Hours Per Pay Period/FTE 1.0"}) == \
        "Schedule: Day shift · 40 hours per week"
    # Select Medical "Experience (Years)" "0" adds nothing; "3" adds a line
    assert S._icims_header_lines({"experience (years)": "0"}) == ""
    assert S._icims_header_lines({"experience (years)": "3"}) == "Experience: 3+ years experience"


# ── item 1: derive_job_type ───────────────────────────────────────────────

@pytest.mark.parametrize("raw, want", [
    ("Contract", "temporary"),            # 1,114 active 'standard' rows store this
    ("Locum Tenens", "temporary"),
    ("Contract PRN", "per_diem"),         # per diem still wins
    ("Full time", "full_time"),
    ("Benefit Eligible (20-39) Hours/Week", "part_time"),
    ("Call-in/On-Call", "per_diem"),
])
def test_derive_job_type(raw, want):
    assert S.derive_job_type("Registered Nurse", raw) == want
