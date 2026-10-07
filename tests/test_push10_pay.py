"""Push 10 pay items (2026-10-07; approval_2026-10-07/pay-audit-2026-10-07.md
and approval_2026-10-06/goal-plan-accuracy-descriptions-pay.md). One case
per audit id, replayed on the stored body's own pay lines (tests/fixtures/
push10/pay_snippets.json, trimmed from hospital_jobs rows named in the audit)
as normalize_job passes them ("title\\nbody"), plus the shapes each rule must
keep refusing. No network, no database."""
import json
import logging

import pytest

import scraper
from scraper import extract_posted_wage, Job


@pytest.fixture
def snips(fixture_text):
    return json.loads(fixture_text("push10/pay_snippets.json"))


FILLER = "\n\n" + "The team cares for patients on a busy unit and supports the department. " * 30


def _job(**kw):
    base = dict(title="Registered Nurse", hospital_system="Test System", hospital_name="Test System",
                city="Springfield", state="MO", location="Springfield, MO", specialty="", job_type="",
                url="https://example.org/job/1", job_id="1", posted_date="", description="", ats_platform="Test")
    base.update(kw)
    return Job(**base)


# ── item 2: Sunrise's bare decimal pair ─────────────────────────────────────

def test_sunrise_bare_decimal_pair_after_a_pay_label(snips):
    assert extract_posted_wage(snips["sunrise_newline"]) == (15.5, 19.4, "hour")       # 22069721
    assert extract_posted_wage(snips["sunrise_glued"]) == (17.6, 22.05, "hour")        # 22069456, flattened body
    assert extract_posted_wage("Pay Range: 15.50 - 19.40") == (15.5, 19.4, "hour")
    assert extract_posted_wage("Hourly Rate: 19.00 to 23.80") == (19.0, 23.8, "hour")
    assert extract_posted_wage("Pay Range: 25.00 - 30.00 per visit") == (25.0, 30.0, "visit")


def test_bare_decimal_pair_needs_a_label_decimals_and_the_band():
    assert extract_posted_wage("15.50 - 19.40") is None                               # no label
    assert extract_posted_wage("Pay Range: 3 - 5 years of experience") is None         # no decimals
    assert extract_posted_wage("Pay Range: 0.50 - 1.00 FTE") is None                   # outside the band
    assert extract_posted_wage("Pay Range: 45.50 - 55.00 per year") is None            # the stated unit disagrees
    assert extract_posted_wage("Pay Grade: 10.00 - 12.00") is None                     # a grade, not a pay label
    assert extract_posted_wage("Hourly Wage Estimate: 30.00 - 45.00 / hour") is None   # an estimate label is not a pay label
    assert extract_posted_wage("Shift differential pay rate: 2.50 - 4.00") is None     # a priced extra


# ── item 3: UW Medicine's Minimum / Maximum lines and "annual" ──────────────

def test_uw_minimum_and_maximum_lines_read_as_one_range(snips):
    assert extract_posted_wage(snips["uw_annual"]) == (45288.0, 53400.0, "year")       # 1307444
    assert extract_posted_wage(snips["uw_hourly_flat"]) == (74.4, 106.46, "hour")      # 21523895 (was the minimum alone)
    assert extract_posted_wage(snips["guthrie_minmax"]) == (29.62, 45.02, "hour")      # 13879615 "pay range min $X max $Y"
    assert extract_posted_wage("Minimum Hiring Rate: $20.00 Maximum Hiring Rate: $30.00") == (20.0, 30.0, "hour")
    assert extract_posted_wage("Pay Range Minimum: $45,288.00 annual Pay Range Maximum: $53,400.00 annual (0.5 FTE)") is None   # S8


def test_annual_and_yearly_are_units():
    assert extract_posted_wage("$45,288.00 annual") == (45288.0, 45288.0, "year")
    assert extract_posted_wage("$85,000 yearly") == (85000.0, 85000.0, "year")
    assert extract_posted_wage("$70,000 annual salary") == (70000.0, 70000.0, "year")
    assert extract_posted_wage("$5,000 annual bonus") is None
    assert extract_posted_wage("$25 annual") is None                                   # the unit disagrees with the band


# ── item 4: non-base figures, figure-then-label, placeholders, role floors ──

def test_premiums_differentials_and_loan_figures_are_not_base_pay(snips):
    assert extract_posted_wage(snips["chs_premium"]) is None                            # 31567141 "Weekend Premium Pay $12/hr"
    assert extract_posted_wage(snips["novant_float"]) is None                           # 14793957 "Float pay premiums up to $15/hour"
    assert extract_posted_wage(snips["coxhealth_diff"]) is None                         # 28948478 "$14.50 Float Differential"
    assert extract_posted_wage("$15/hour weekend premium") is None                      # Henry Ford
    assert extract_posted_wage("Tier 4 Pay rate: Experience rate + $15.00 per hour") is None   # South Georgia
    assert extract_posted_wage("401(k) match up to $3,000 per year") is None
    assert extract_posted_wage("Student Loan Repayment: Up to $20,000") is None
    assert extract_posted_wage("Additional pay: $5.00 per hour for nights") is None


def test_the_real_figure_wins_once_the_add_on_is_out(snips):
    # 35209607: "$90/hr above base pay for additional shifts ... an additional
    # $50,000 to $100,000" came before the real salary and won the tie.
    assert extract_posted_wage(snips["denver_additional"], "Full time") == (444600.0, 522700.0, "year")
    # 35282682: the loan-repayment "$75,000 annually" beat the headline.
    assert extract_posted_wage(snips["presbyterian_650k"]) == (650000.0, 650000.0, "year")
    assert extract_posted_wage("$650k Starting Salary") == (650000.0, 650000.0, "year")
    assert extract_posted_wage("$250,000 base salary with production bonus") == (250000.0, 250000.0, "year")


def test_joined_and_composed_figures_keep_reading():
    # the add-on word belongs to the earlier figure when a joining word separates them
    assert extract_posted_wage("$5,000 sign-on bonus and $70,000 annual salary") == (70000.0, 70000.0, "year")
    assert extract_posted_wage("Relocation assistance of $5,000 and a salary of $85,000") == (85000.0, 85000.0, "year")
    assert extract_posted_wage("Base salary plus bonus: $70,000 - $80,000") == (70000.0, 80000.0, "year")
    assert extract_posted_wage("Pay: $30.00 - $40.00 per hour with additional shift differential") == (30.0, 40.0, "hour")
    # the ceilings and the pay-word-before forms the earlier pushes settled
    assert extract_posted_wage("Up to $248 per hour") == (248.0, 248.0, "hour")
    assert extract_posted_wage("Compensation: Up to $47 per hour, based on experience Sign on bonus: $10,000") == (47.0, 47.0, "hour")
    assert extract_posted_wage("Starting rate $21.21/hr") == (21.21, 21.21, "hour")


def test_a_flat_range_under_the_wrong_unit_is_a_placeholder(snips):
    assert extract_posted_wage(snips["medstar_yr"]) is None                             # 33986367 "USD $20.00 - USD $20.00 /Yr."
    assert extract_posted_wage("$45,000 - $45,000 / hour") is None
    assert extract_posted_wage("Pay: $18 - $18 annually") is None
    # a range with two ends keeps the band's unit: Kaiser's template prints
    # hourly figures under "/ year" (6 of 38 Kaiser rows in the push-10 sample)
    assert extract_posted_wage(snips["kaiser_year_range"]) == (52.8, 68.93, "hour")
    # a unit word on the next line is not the figure's unit (UnitedHealth)
    got = extract_posted_wage(snips["uhg_annual_next_line"])
    assert got is not None and got[2] == "hour"


def test_vanderbilt_premium_pay_is_the_rules_known_cost(snips):
    # "*Premium Pay $65" on a per-diem resource pool is the whole rate, but it
    # has the shape of CHS's weekend premium line and the rule cannot tell
    # them apart (1 row in the 1,000-row push-10 regression sample).
    assert extract_posted_wage(snips["vanderbilt_premium_pay"]) is None


def test_role_floors_on_the_text_path(caplog):
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        assert extract_posted_wage("Staff Physician\nPay Range: $19.00 - $23.00 per hour") is None            # Luminis's placeholder band
        assert extract_posted_wage("Physician-OB/GYN\nSalary: $30,937 - $54,155 annually") is None            # University Hospitals
        assert extract_posted_wage("CRNA - Main OR\nPay: $25.00 - $35.00 per hour") is None
        assert extract_posted_wage("CNA - Med Surg\nPay: $55.00 - $60.00 per hour") is None
        # the top of the range reaches the floor: a real range
        assert extract_posted_wage("Psychiatrist (MD/DO)\nSalary: $72,800 - $130,000 annually") == (72800.0, 130000.0, "year")
        assert extract_posted_wage("Clinical Pharmacist\nPay Range: $36.66 - $80.87 per hour") == (36.66, 80.87, "hour")
        # not the role: the floor does not apply
        assert extract_posted_wage("Pharmacist Intern\nPay: $19.00 - $23.00 per hour") == (19.0, 23.0, "hour")
        assert extract_posted_wage("Physician Assistant\nPay: $55.00 - $70.00 per hour") == (55.0, 70.0, "hour")
        assert extract_posted_wage("Lead Physician Compensation Analyst\nSalary: $94,083 - $96,000 annually") == (94083.0, 96000.0, "year")
        assert extract_posted_wage("Speech Language Pathologist\nPay: $35.00 - $38.00 per hour") == (35.0, 38.0, "hour")
        assert extract_posted_wage("CNA - Med Surg\nPay: $18.00 - $22.00 per hour") == (18.0, 22.0, "hour")
        assert extract_posted_wage("RNFA - Surgery (CNA)\nPay: $70.00 - $90.00 per hour") == (70.0, 90.0, "hour")
    msgs = [r.getMessage() for r in caplog.records if "Pay rejected (text)" in r.getMessage()]
    assert len(msgs) == 4
    assert any("'Staff Physician'" in m and "19-23 hour" in m for m in msgs)
    assert any("'CNA - Med Surg'" in m and "55-60 hour" in m for m in msgs)


def test_role_floors_on_the_field_path(caplog):
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        j = _job(title="OB Hospitalist")
        assert scraper.set_field_wage(j, (19.0, 23.0, "hour")) is False and j.wage_min is None     # Luminis 35353358
        j = _job(title="Clinical Pharmacist")
        assert scraper.set_field_wage(j, (45.0, 60.0, "hour")) is True and j.wage_min == 45.0
        # an adapter that assigned the fields itself is caught in normalize_job
        j = _job(title="Pediatric Anesthesiologist", description="Cares for patients." + FILLER)
        j.wage_min, j.wage_max, j.wage_unit = 50000.0, 90000.0, "year"
        d = scraper.normalize_job(j)
        assert d["wage_min"] is None and "pay_src" not in d["posting_facts"]
    msgs = [r.getMessage() for r in caplog.records if "Pay rejected (field)" in r.getMessage()]
    assert len(msgs) == 2 and any("'OB Hospitalist'" in m for m in msgs)


# ── item 1: the posting text's own range over a structured field ────────────

@pytest.mark.parametrize("key,title,field,want,src", [
    ("rwj_text", "Co-occurring Disorder Spec", (29.39, 36.09, "hour"), (38.47, 47.45, "hour"), "text"),      # 35481735
    ("lompoc_text", "Medical Assistant", (14.17, 18.99, "hour"), (20.49, 28.24, "hour"), "text"),           # 35483082
    ("uci_text", "Lead Academic Personnel Department Analyst", (79200.0, 143400.0, "year"), (79200.0, 95250.0, "year"), "text"),   # 33987728
    ("boulder_text", "Medical Assistant - IMA Boulder", (13.66, 20.5, "hour"), (20.76, 31.15, "hour"), "text"),   # 33062004
    ("crouse_text", "Respiratory Therapist", (33.15, 46.69, "hour"), (33.15, 46.69, "hour"), "field"),     # 27464647: within 5%
])
def test_text_over_field_in_normalize_job(snips, key, title, field, want, src):
    j = _job(title=title, description=snips[key] + FILLER)
    scraper.set_field_wage(j, field)
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == want
    assert d["posting_facts"]["pay_src"] == src


def test_pay_text_overrides_rule():
    assert scraper.pay_text_overrides((29.39, 36.09, "hour"), (38.47, 47.45, "hour")) is True
    assert scraper.pay_text_overrides((30.0, 40.0, "hour"), (31.0, 41.0, "hour")) is False     # within 5% on both ends
    assert scraper.pay_text_overrides((30.0, 40.0, "hour"), (30.0, 43.0, "hour")) is True      # one end off by 7.5%
    assert scraper.pay_text_overrides((30.0, 40.0, "hour"), (32.0, 32.0, "hour")) is False     # a single never replaces a range
    assert scraper.pay_text_overrides((30.0, 30.0, "hour"), (32.0, 32.0, "hour")) is True
    assert scraper.pay_text_overrides((30.0, 40.0, "hour"), (70000.0, 80000.0, "year")) is False   # not comparable
    assert scraper.pay_text_overrides((30.0, 40.0, "hour"), None) is False
    assert scraper.pay_text_overrides(None, (30.0, 40.0, "hour")) is False


# ── item 5: HCA's own printed estimate, behind HCA_ESTIMATE_PAY ─────────────

def test_hca_estimate_line(snips, monkeypatch):
    assert scraper._hca_estimate_wage(snips["hca_hourly"]) == (91.36, 137.0, "hour")          # 13439403
    assert scraper._hca_estimate_wage(snips["hca_salary"]) == (82180.8, 115044.8, "year")      # 13439397
    assert scraper._hca_estimate_wage(snips["hca_physician"]) == (400000.0, 450000.0, "year")  # 13439406
    assert scraper._hca_estimate_wage(snips["hca_bad_unit"]) is None                          # 13439407: year figures "/ hour"
    assert scraper._hca_estimate_wage("") is None
    # the generic body rules never read an estimate label
    assert extract_posted_wage(snips["hca_hourly"]) is None
    # normalize_job: HCA rows only, stamped as text, and off with the flag
    j = _job(hospital_system="HCA Healthcare", title="Clinical Nurse Coordinator", description=snips["hca_hourly"] + FILLER)
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (91.36, 137.0, "hour") and d["posting_facts"]["pay_src"] == "text"
    j = _job(hospital_system="Other System", title="Clinical Nurse Coordinator", description=snips["hca_hourly"] + FILLER)
    assert scraper.normalize_job(j)["wage_min"] is None
    j = _job(hospital_system="HCA Healthcare", title="Urology Physician", description=snips["hca_physician"] + FILLER)
    assert scraper.normalize_job(j)["wage_min"] == 400000.0
    monkeypatch.setattr(scraper, "HCA_ESTIMATE_PAY", False)
    j = _job(hospital_system="HCA Healthcare", title="Clinical Nurse Coordinator", description=snips["hca_hourly"] + FILLER)
    assert scraper.normalize_job(j)["wage_min"] is None


# ── item 6: Phenom list pay keys ─────────────────────────────────────────────

def test_phenom_list_pay_keys(fixture_text):
    docs = json.loads(fixture_text("push10/phenom_list_pay.json"))
    assert scraper._phenom_list_pay(docs["umms"]) == tuple(docs["umms_want"])
    assert scraper._phenom_list_pay(docs["umms_zero"]) is None                 # "0.00" placeholder
    assert scraper._phenom_list_pay(docs["childrens"]) == tuple(docs["childrens_want"])
    assert scraper._phenom_list_pay(docs["childrens_zero"]) is None            # "0.0-0.0 ": no posted pay; salaryHourly is derived
    assert scraper._phenom_list_pay(docs["plain"]) is None
    assert scraper._phenom_list_pay(None) is None
    assert scraper._phenom_list_pay({"minimumPay": 0, "maximumPay": 80000}) is None
    assert scraper._phenom_list_pay({"minimumPay": "54,000.00", "maximumPay": "80,000.00"}) == (54000.0, 80000.0, "year")
    assert scraper._phenom_list_pay({"salaryRange": "$33.37 - $50.06 per hour"}) == (33.37, 50.06, "hour")


# ── item 7: AMN's low end, Aya's hourly display ─────────────────────────────

def test_amn_weekly_numeric_is_the_low_end():
    j = {"jobID": 123, "jobTitle": "RN ICU", "payRate": {"minPayRate": 2240, "maxPayRate": 2355, "payRateType": "Weekly", "payRateTypeAbbrev": "wk"},
         "disciplineSpecialty": {"disciplineName": "RN", "specialtyName": "ICU"}, "organization": {}, "city": {"name": "Austin"}, "state": {"abbrev": "TX"}}
    t = scraper._amn_map(j)
    assert t.weekly_pay_numeric == 2240.0 and t.weekly_pay_display == "$2240-$2355/wk"     # travel_jobs 1884989 stored 2355
    j["payRate"] = {"minPayRate": None, "maxPayRate": 2355, "payRateType": "Weekly", "payRateTypeAbbrev": "wk"}
    assert scraper._amn_map(j).weekly_pay_numeric == 2355.0
    j["payRate"] = {"minPayRate": 60, "maxPayRate": 65, "payRateType": "Hourly", "payRateTypeAbbrev": "hr"}
    assert scraper._amn_map(j).weekly_pay_numeric is None


def test_aya_hourly_display_keeps_the_weekly_and_stores_the_rate():
    wp_low, wp_high, disp, hourly = scraper._aya_pay({"weeklyPayLow": 591, "weeklyPayHigh": 591, "payRate": {"value": "$18.00 per hour"}})
    assert (wp_low, wp_high, disp, hourly) == (591.0, 591.0, "$18.00 per hour", 18.0)         # travel_jobs 2258679
    assert scraper._aya_pay({"weeklyPayLow": 2500, "weeklyPayHigh": 2700, "payRate": {"value": "$2,500 - $2,700 weekly"}})[3] is None
    assert scraper._aya_pay({"weeklyPayLow": 2500, "weeklyPayHigh": 2700})[2] == "$2,500–$2,700/wk"
    assert scraper._aya_pay({"regularPayLow": 1800})[0] == 1800.0
