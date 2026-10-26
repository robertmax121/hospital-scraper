"""Posted pay reader, round 2 (Bounce QA re-analysis, 2026-10-26/27): job
pages that say "Pay not listed" although the posting prints the pay. Each
case is a real active row (read-only SQL, 2026-10-26), trimmed to the pay
lines; the extractor is replayed on "title\\nbody" as normalize_job passes
it. The adversarial cases keep bonuses, differentials, $0 lines, prorated
figures and system-wide bands out."""
from scraper import extract_posted_wage


# ── Lakeland Regional Health: minimum and midpoint ─────────────────────────

def test_lakeland_min_mid_hourly():
    # Lakeland 32712711
    body = ("Sr. Refund Account Specialist\nLocation : 1324 Lakeland Hills Blvd Lakeland, FL\n"
            "Pay Rate : Min $18.82 Mid $23.53\n\nPosition Summary\n\nThe Senior Refund Account Specialist ...")
    assert extract_posted_wage(body) == (18.82, 23.53, "hour")


def test_lakeland_min_mid_annual_and_trailing_extra():
    # Lakeland 32712660 (annual) and 32712646 (a weekender add-on after the pair)
    assert extract_posted_wage("Staff Accountant\nPay Rate: Min $45,323.20 Mid $56,659.20\n\nPosition Summary") \
        == (45323.2, 56659.2, "year")
    assert extract_posted_wage("Radiology Tech II\nPay Rate : Min $30.90 Mid $38.61 Lakeland Weekender $7.00\n\nPosition Summary") \
        == (30.9, 38.61, "hour")


def test_rady_minimum_midpoint_maximum_reads_minimum_to_maximum():
    # Rady Children's 38509293
    body = ("Physical Therapist, NICU\nprogram development.\n\nPay Range\nMinimum $47.99 Midpoint $63.60 Maximum $79.20\n\n"
            "Rate of pay is determined by various factors")
    assert extract_posted_wage(body) == (47.99, 79.2, "hour")


def test_min_mid_zero_and_bonus_stay_out():
    # Lakeland's physician postings print "Min $0.00 Mid $0.00" (32712817)
    assert extract_posted_wage("Physician - Specialist - Cardiology\nPay Rate : Min $0.00 Mid $0.00\n\nPosition Summary") is None
    assert extract_posted_wage("RN\nShift differential pay rate: Min $2.00 Mid $3.00") is None


# ── "... for this position / role is $X" ───────────────────────────────────

def test_salary_for_this_position_virtua_maine_hackensack():
    # Virtua 22080035, MaineHealth 32716855, Hackensack Meridian 35421422
    assert extract_posted_wage("Neurocritical Care Nurse Practitioner\nCompensation:\n\nThe starting salary for this position is: "
                               "$163,350.00 (annually).\nThe actual compensation package could vary") == (163350.0, 163350.0, "year")
    assert extract_posted_wage("General Hematologist Oncologist\nThe annual base salary for this position is $520,000.\n\n"
                               "In addition to base pay, MaineHealth offers comprehensive benefits") == (520000.0, 520000.0, "year")
    assert extract_posted_wage("Chief, Wound Center\nThe minimum base salary for this position is $237,959. The posted rate of pay "
                               "in this job posting is a reasonable good-faith estimate") == (237959.0, 237959.0, "year")


def test_prime_compensation_estimate_for_this_role():
    # Prime Healthcare 26512482 and 26511987
    assert extract_posted_wage("Clinical Lab Scientist\nSaint Francis Hospital offers competitive compensation and a reasonable "
                               "compensation estimate for this role is $34.24. The exact starting compensation to be offered "
                               "will be determined") == (34.24, 34.24, "hour")
    assert extract_posted_wage("Certified Phlebotomy Technician (CPT)\nSt. Francis Medical Center offers competitive compensation "
                               "and the current compensation rate for this role is $23.80. Benefits may vary") == (23.8, 23.8, "hour")


def test_hourly_rate_for_this_role_and_colon_form():
    # One Medical 24960528, OSF 22134261
    assert extract_posted_wage("Per Diem Nurse Practitioner\nThe hourly rate for this role is $84.00. Final determination of "
                               "starting pay may vary") == (84.0, 84.0, "hour")
    assert extract_posted_wage("APN Cardiology Fellow\nClinical base hourly pay range for this position: $47.12\n\n"
                               "Range shown should be used as an estimate") == (47.12, 47.12, "hour")


def test_for_this_position_range_is_not_read_as_its_low_end():
    # MaineHealth 32717298: the range is over the hourly band; its low end
    # alone must not be stored. Salinas 28363687: "$56.40. - $65.00".
    assert extract_posted_wage("Anesthesiologist - Per Diem\nThe pay rate for this position is $298.00 to $353.00 per hour.") is None
    assert extract_posted_wage("Talent Acquisition Specialist - Per Diem\nPay Range: The hourly rate for this position is "
                               "$56.40. - $65.00. The range displayed") is None
    # Baylor Scott & White 33003320: an aside between the two ends.
    assert extract_posted_wage("Enrollment Marketing Manager\nThe pay range for this position is $100,318.40 (entry-level "
                               "qualifications) - $155,480 (highly experienced).") is None
    # and a plain range under the same words still reads as a range
    assert extract_posted_wage("Senior Medical Lab Scientist\nThe base compensation range for this role is $32.85- $50") \
        == (32.85, 50.0, "hour")


def test_for_this_position_adversarial():
    # a bonus, a differential, on-call pay: never the pay
    assert extract_posted_wage("RN\nThe sign-on bonus pay for this position is $5,000.") is None
    assert extract_posted_wage("RN\nThe differential rate for this position is $5.00 per hour.") is None
    assert extract_posted_wage("RN\nThe on-call rate for this position is $8.00 per hour.") is None
    # prorated (MaineHealth 32717443), a floor written "+" (Duly 28896488)
    assert extract_posted_wage("Primary Care Physician\nThe annual base salary for this position at a .8 FTE is $206,000.") is None
    assert extract_posted_wage("Gynecologic Oncologist\nThe anticipated base compensation range for this position is $500,000+, "
                               "depending upon the applicant's qualifications") is None
    # an annual label over an hourly-sized figure, per-shift pay, "up to"
    assert extract_posted_wage("Physician\nThe annual salary for this position is $45.00.") is None
    assert extract_posted_wage("Pathology (PRN) - Physician\nThe compensation for this position is $2,200/shift.") is None
    assert extract_posted_wage("Resource Employee\nThe hourly rate for this position is up to $25.00.") is None
    # a typo'd figure is not guessed at (St. Luke's "$115,00")
    assert extract_posted_wage("Psychologist\nThe estimated salary range for this position is: $115,00\n\nPlease complete") is None


# ── Plain ranges ───────────────────────────────────────────────────────────

def test_currency_word_after_the_figure_bilh():
    # Beth Israel Lahey Health 36778099
    body = ("Physician Assistant/Nurse Practitioner, Orthopaedics\nPay Range: $133,120.00 USD - $230,000.00 USD\n\n"
            "The pay range listed for this position is the annual base salary range the organization reasonably and in "
            "good faith expects to pay")
    assert extract_posted_wage(body) == (133120.0, 230000.0, "year")
    assert extract_posted_wage("Nurse Practitioner - Employee Health\nPay Range: $0.00 USD - $0.00 USD\n\nThe pay range listed") is None


def test_minimum_offer_maximum_offer_presbyterian_and_hourly_word_cleveland():
    # Presbyterian 38413599, Cleveland Clinic 1211139
    assert extract_posted_wage("RN - Outpatient Hospice\nCompensation Pay Range:\nMinimum Offer $39.69\nMaximum Offer $58.17\n\n"
                               "Summary:") == (39.69, 58.17, "hour")
    assert extract_posted_wage("Patient Access Account Specialist\nCompensation Pay Range:\nMinimum Offer $0.00\n"
                               "Maximum Offer $0.00\n\nSummary:") is None
    assert extract_posted_wage("MRI Technologist I\nPay Range\n\nMinimum hourly: $35.25\n\nMaximum hourly: $53.00\n\n"
                               "The pay range displayed on this job posting") == (35.25, 53.0, "hour")
    # a relocation or bonus range under the same layout stays out
    assert extract_posted_wage("RN\nRelocation Range:\nMinimum Offer $30,000\nMaximum Offer $50,000") is None


def test_four_decimal_figures():
    # Kern Medical 38502568, Trinity Health 2664171
    assert extract_posted_wage("Surgical Technologist\nCompensation\n\nThe estimated pay for this position is $28.5738 to "
                               "$46.4142 . The rates shown include a 6% premium pay") == (28.5738, 46.4142, "hour")
    assert extract_posted_wage("Billing and Follow-up Representative-I\n100% Work Onsite (Pay Range: $17.1538-$25.7307) "
                               "Performs day-to-day billing") == (17.1538, 25.7307, "hour")


def test_period_typed_for_thousands_comma():
    # UMass Memorial 1303103, UK HealthCare 38409967, BAYADA "$106.000-$125,000"
    assert extract_posted_wage("Internal Medicine Physician\nHiring Range: $250,000 - $300.000\nPlease note") \
        == (250000.0, 300000.0, "year")
    assert extract_posted_wage("Amb/Retail Pharmacist\nSalary Range $114.911-156,062/year\nType of Position") \
        == (114911.0, 156062.0, "year")
    assert extract_posted_wage("Physical Therapist\nBase Pay, depending upon qualifications and location:\n$106.000-$125,000 /year") \
        == (106000.0, 125000.0, "year")
    # Sharp's three-decimal hourly range has no end of 1,000 and reads as before
    assert extract_posted_wage("Advanced Care Technician\nHourly Pay Range (Minimum - Midpoint - Maximum):$25.630 - $25.630 - $28.180") \
        == (25.63, 28.18, "hour")


def test_double_dash_range_osf():
    # OSF HealthCare 22134602
    assert extract_posted_wage("Pediatric Physician in Peoria, IL\nFull time, clinical base salary pay range for this position: "
                               "$250,000--$270,000.\n\nRange shown should be used as an estimate") == (250000.0, 270000.0, "year")


def test_zero_lines_bands_and_bonuses_stay_unpriced():
    # Johns Hopkins 35496066: "$0" ranges stay unpriced
    jh = ("Physical Therapist I\nSalary Range: Minimum $0/hour - Maximum $0/hour. In cases where the range is displayed "
          "as a $0 amount, salary discussions will occur during candidate screening calls")
    assert extract_posted_wage(jh) is None
    assert extract_posted_wage("Medical Director\nPay Range is $1,040.00 to $520,000.00 / annual salary.") is None
    assert extract_posted_wage("Staff Psychologist\nThe salary range for this position is $2,080 to $3,120,000 annually .") is None
    assert extract_posted_wage("Physician\nCompensation Range:\n$1.00 - $1,000,000.00\n\nEmployee Benefits") is None
    assert extract_posted_wage("Coordinator\nPay Range\n$15,080 - $520,000") is None
    assert extract_posted_wage("RN\nSign-on bonus: $5,000 USD - $10,000 USD for eligible new hires") is None
    assert extract_posted_wage("RN\nWeekend premium $38.5000 - $52.0000 per hour") is None
    assert extract_posted_wage("RN\nAn average of $12,000--$14,000 per child per year in tuition assistance") is None


def test_extra_heading_on_the_line_above_a_minimum_maximum_block():
    # the heading names the extra one line above the figures (review case)
    assert extract_posted_wage("RN\nSign-On Bonus Range\n\nMinimum: $5,000\n\nMaximum: $10,000") is None
    assert extract_posted_wage("RN\nRetention Incentive Range:\nMinimum Offer $26,000\nMaximum Offer $40,000") is None
    # a pay heading over the same block still reads (Bon Secours 20960917)
    assert extract_posted_wage("Rehabilitation Technician\nprofessionals.\n\nRange\n\nMinimum :$16.32\n\nMaximum: $23.15\n\n"
                               "Bon Secours is an equal opportunity employer.") == (16.32, 23.15, "hour")


# ── JSON-LD band over levels the body prices one by one ────────────────────

def _jsonld(lo, hi, unit, desc):
    return {"@type": "JobPosting", "description": desc,
            "baseSalary": {"@type": "MonetaryAmount", "currency": "USD",
                           "value": {"@type": "QuantitativeValue", "minValue": lo, "maxValue": hi, "unitText": unit}}}


def _job(**kw):
    from scraper import Job
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="", specialty="",
                job_type="", url="https://www.governmentjobs.com/careers/x/jobs/1/rn", job_id="1", posted_date="",
                description="", ats_platform="NeoGov")
    base.update(kw)
    return Job(**base)


def test_jsonld_band_spanning_the_bodys_levels_stays():
    from scraper import _apply_posting
    # Riverside 26728644: two levels in four decimals under a $45.75 - $80.24 band
    levels = ("Salary Range (Hourly) & Differentials\n\nRegistered Nurse II - MC/CHC - $45.7530 - $70.8141\n\n"
              "Registered Nurse III - MC/CHC - $53.1735 - $80.2448\n\nEvening Shift Differential: $2.50/hour\n\n") * 3
    j = _job(title="Registered Nurse II/III-MC/CHC - Emergency Department")
    _apply_posting(j, _jsonld(45.75, 80.24, "HOUR", levels))
    assert (j.wage_min, j.wage_max, j.wage_unit) == (45.75, 80.24, "hour")
    # Riverside 26728642: a band the body never prints loses to the body's own figure
    other = ("Registered Nurse II - MC/CHC - $43.9933 - $68.0905\n\nRegistered Nurse III - MC/CHC - $51.1284 - $77.1585\n\n") * 3
    j = _job(title="Registered Nurse II/III-MC/CHC - Adult Critical Care Unit")
    _apply_posting(j, _jsonld(45.75, 80.24, "HOUR", other))
    assert j.wage_min is None


def test_period_thousands_figure_is_a_whole_number():
    # UnitedHealth 3549098: 130.151 * 1000 must store 130151, not 130151.00000000001
    assert extract_posted_wage("Home Health Registered Nurse\nPay Range:\nAnnual Target Pay - $100,000 - $130.151\n\nPay is based") \
        == (100000.0, 130151.0, "year")
