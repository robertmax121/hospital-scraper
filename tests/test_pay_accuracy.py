"""Posted-pay accuracy (2026-10-04, push6/pay-accuracy): one case per raw-text
example in the audit's causes.md sections C..K, plus the healthy figures that
must keep reading. The extractor is replayed on title + body exactly as
normalize_job passes it ("title\\nbody")."""
import scraper
from scraper import _apply_posting, _wage_pair, extract_posted_wage, extract_posting_facts, Job


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


# ── healthy figures that must keep working ──────────────────────────────────

def test_healthy_figures_still_read():
    assert extract_posted_wage("Pay : $45.50/hr") == (45.5, 45.5, "hour")
    assert extract_posted_wage("Starting rate $21.21/hr") == (21.21, 21.21, "hour")
    assert extract_posted_wage("$19.55 - $34.25 /Hr") == (19.55, 34.25, "hour")
    assert extract_posted_wage("Hourly rate: $18.00- $25.00 Monthly Incentive Bonus") == (18.0, 25.0, "hour")
    assert extract_posted_wage("Up to $248 per hour") == (248.0, 248.0, "hour")
    assert extract_posted_wage("$117K-$132,600") == (117000.0, 132600.0, "year")
    assert extract_posted_wage("$70-80K annually") == (70000.0, 80000.0, "year")
    assert extract_posted_wage("The salary range is $95k - $110k depending on experience") == (95000.0, 110000.0, "year")
    assert extract_posted_wage("Salary $6,500 per month plus benefits") == (78000.0, 78000.0, "year")
    assert extract_posted_wage("Primary Care Clinic Physician: $9,404.43 - $10,893.28 Biweekly (This amount is prorated") == (244515.18, 283225.28, "year")
    assert extract_posted_wage("Pay rate $42") == (42.0, 42.0, "hour")
    assert extract_posted_wage("Salary: $85,000") == (85000.0, 85000.0, "year")
    assert extract_posted_wage("Compensation: Up to $47 per hour, based on experience Sign on bonus: $10,000") == (47.0, 47.0, "hour")
    assert extract_posted_wage("Relief, Variable Pay range: $26.18 - $33.51 Relief Differential - 15% Swing Shift Differential - $2.50/hr") == (26.18, 33.51, "hour")
    assert extract_posted_wage("$15/hr shift differential on nights") is None


# ── S1 / S2: width test and dead ends (causes C and G) ──────────────────────

def test_wage_pair_width_test_and_visit_band():
    assert _wage_pair(15.0, 130.0) is None                    # AdventHealth StaffFlex
    assert _wage_pair(20.0, 100.0) is None and _wage_pair(20.0, 80.0) is None   # CommonSpirit JSON-LD
    assert _wage_pair(32.0, 125.0) is None and _wage_pair(12.0, 120.0) is None
    assert _wage_pair(100000.0, 400000.0) is None              # HCA "Salary Estimate"
    assert _wage_pair(341.0, 64.09) is None and _wage_pair(340.0, 54.12) is None
    assert _wage_pair(17.13, 216.42) is None
    assert _wage_pair(0.0, 0.0) is None and _wage_pair(0.0, 40.0) is None
    assert _wage_pair(40.0, 120.0) == (40.0, 120.0, "hour")   # exactly 3x stays
    assert _wage_pair(75.0, 70.0) == (70.0, 75.0, "hour")      # a swap inside the width is still swapped
    assert _wage_pair(55.0, 160.0, "visit") == (55.0, 160.0, "visit")
    assert _wage_pair(55.0, 160.0) == (55.0, 160.0, "hour")
    assert _wage_pair(600.0, 700.0, "visit") is None
    assert _wage_pair(5.0, 10.0, "visit") is None


def test_boilerplate_band_dies_and_the_real_figure_in_the_same_body_wins():
    # 30677917 AdventHealth: the template band loses, "Pay : $45.50/hr" reads
    assert extract_posted_wage("Pay Range: $15.00 - $130.00") is None
    body = "CST/Scrub Tech Contract 40hrs\nGuaranteed Hours\n\nSchedule: 40 hours per week\n\nPay : $45.50/hr\n\nPay Range: $15.00 - $130.00\n"
    assert extract_posted_wage(body) == (45.5, 45.5, "hour")
    # 13440262 HCA "Hourly Wage Estimate: $32.00 - $125.00 / hour": neither end survives as a flat figure
    assert extract_posted_wage("Hourly Wage Estimate: $32.00 - $125.00 / hour What's this?") is None
    assert extract_posted_wage("Salary Estimate: $100,000 - $400,000 / year What's this?") is None
    # exactly 3x (Concentra "state range of $30.00 to $90.00") and under 3x (Mount Sinai Flex) are the posting's statement
    assert extract_posted_wage("the state range of $30.00 to $90.00 per hour") == (30.0, 90.0, "hour")
    assert extract_posted_wage("Pay Range: $17.00 - $50.00") == (17.0, 50.0, "hour")
    assert extract_posted_wage("Pay Range: $17.00 - $51.01") is None


def test_swapped_typo_pairs_are_dropped_not_swapped():
    # 26512859 Prime, 13879618 Guthrie, 28128433 Catholic Health LI
    assert extract_posted_wage("$341.00 to $64.09") is None
    prime = ("A reasonable compensation estimate for this role, which includes estimated wages, benefits, "
             "and other forms of compensation, is $341.00 to $64.09. The exact starting compensation to be offered")
    assert extract_posted_wage(prime) is None
    assert extract_posted_wage("The pay range for this position is $340.00 - $54.12 per hour + shift diffs!") is None
    assert extract_posted_wage("Posted Salary Range\nUSD $120.00 - USD $12.00 /Hr.") is None


# ── S3: iCIMS "USD $a - USD $b" and per-end units ───────────────────────────

def test_icims_usd_range_and_per_end_hourly_units():
    # 26517706 Legacy / GoHealth: the whole band is read, found too wide, and the ceiling is not re-stored
    assert extract_posted_wage("Pay Range\nUSD $17.13 - USD $216.42 /Hr.\n\nOur Commitment to Health") is None
    assert extract_posted_wage("Posted Salary Range\nUSD $62.00 - USD $135.00 /Hr.") == (62.0, 135.0, "hour")
    assert extract_posted_wage("Posted Salary Range USD $38.00 - USD $52.00 /Hr.") == (38.0, 52.0, "hour")
    # GoHealth physicians: the floor used to be stored alone
    assert extract_posted_wage("$135/hour - $160/hour") == (135.0, 160.0, "hour")
    assert extract_posted_wage("Pay: $135 per hour to $160 per hour") == (135.0, 160.0, "hour")
    assert extract_posted_wage("$42/hr - $55/hr depending on experience") == (42.0, 55.0, "hour")


# ── S4: per visit / per point / per session (cause E) ───────────────────────

def test_per_visit_rates_store_the_visit_unit_never_hourly():
    assert extract_posted_wage("Pay: $55 to $160 per visit") == (55.0, 160.0, "visit")                       # 22129560 AccentCare
    assert extract_posted_wage("Pay Per Visit based on Experience and Visit Type\n\n$60 -$145 per visit") == (60.0, 145.0, "visit")   # 26699633 NHC
    assert extract_posted_wage("Pay: $70-75 per point, depending on qualifications") == (70.0, 75.0, "visit")   # 32459996 BAYADA
    assert extract_posted_wage("$24.00-$28.00/visit") == (24.0, 28.0, "visit")                                 # Select Medical
    assert extract_posted_wage("Rate: $75 per visit") == (75.0, 75.0, "visit")
    assert extract_posted_wage("$90 a session") == (90.0, 90.0, "visit")
    assert extract_posted_wage("$40 each point") == (40.0, 40.0, "visit")
    # a body that also prints an hourly line stores the hourly one (UnitedHealth)
    uhg = "The hourly range for this role is $28.03 to $54.89 per hour.\nPer visit point rate $45 - $60 per point."
    assert extract_posted_wage(uhg) == (28.03, 54.89, "hour")
    assert extract_posted_wage("Per visit point rate $45 - $60 per point.\nThe hourly range for this role is $28.03 to $54.89 per hour.") == (28.03, 54.89, "hour")


# ── S5: period scale stays in the figure's own clause (cause F) ─────────────

def test_period_scale_never_reads_the_next_heading():
    # 34688605 GoHealth: "Monthly Incentive Bonus" is the next heading, not the unit
    gohealth = "Hourly rate: $18.00- $25.00\n\nMonthly Incentive Bonus: This position offers monthly incentive pay based on performance metrics."
    assert extract_posted_wage(gohealth) == (18.0, 25.0, "hour")
    assert extract_posted_wage("Hourly rate: $18.00- $25.00 Monthly Incentive Bonus") == (18.0, 25.0, "hour")
    assert extract_posted_wage("$18.00 - $25.00 monthly stipend") is None
    # a scaled value is annual or nothing: 12 x $20-$25 cannot land in the hourly band
    assert extract_posted_wage("$20 - $25 per month") is None
    assert extract_posted_wage("$6,500 - $7,200 per month") == (78000.0, 86400.0, "year")


# ── S6: non-pay dollar figures (cause D) ─────────────────────────────────────

def test_non_pay_dollar_figures_are_noise():
    assert extract_posted_wage("WEEKEND SHIFT DIFF IS AN EXTRA $15/HR") is None                      # 26515976 Emory
    emory = "SHIFT: 11 AM-11:30 PM / FULL-TIME / 36 HOURS\n\nLOCATION: EMORY MIDTOWN HOSPITAL\n\nWEEKEND SHIFT DIFF IS AN EXTRA $15/HR\n\nBe inspired."
    assert extract_posted_wage(emory) is None
    assert extract_posted_wage("$185/hr excess hours") is None                                       # 2298194 Jackson
    geisinger = ("Optional overtime opportunities available at $275/hour\n\nCompetitive Compensation\n\n"
                 "Base Salary: $500,000\n\n$560,000+ with experience")                                 # 14750596 Geisinger
    assert extract_posted_wage(geisinger) == (500000.0, 500000.0, "year")
    northwell = ("The following rates are paid for certain administrative tasks:\n\nOrientation: 1/2 Day = $75.00; "
                 "Full day = $150.00; Education/In-services: $40.00 - $150.00; Meetings: $35.00")      # 14842526 Northwell
    assert extract_posted_wage(northwell) is None
    assert extract_posted_wage("Competitive Compensation $100 monthly commute subsidy Excellent Medical") is None   # 26536083 UHS
    assert extract_posted_wage("Extra shift bonus of $10/hr") is None
    assert extract_posted_wage("On-call pay $5/hr while on call") is None
    # a pay word in the gap keeps the figure ("Starting rate $21.21/hr" is pay, not an extra)
    assert extract_posted_wage("Starting rate $21.21/hr") == (21.21, 21.21, "hour")
    assert extract_posted_wage("Base pay $32 - $38 per hour, overtime eligible") == (32.0, 38.0, "hour")
    assert extract_posted_wage("Compensation: $41.00 per hour (competitive shift differentials)") == (41.0, 41.0, "hour")


# ── S7: k-suffix per end (cause K) ───────────────────────────────────────────

def test_k_suffix_applies_per_end():
    amedisys = ("PT Physical Therapist Home Health $15K Sign on Bonus\nFull Time Position\n\n$15K Sign On Bonus\n\n"
                "Attractive pay\n\n$117K-$132,600 Annual Salary (Converting to Per Visit)\n\nEnjoy many perks")     # 27364849
    assert extract_posted_wage(amedisys) == (117000.0, 132600.0, "year")
    assert extract_posted_wage("$117K-$132,600") == (117000.0, 132600.0, "year")
    assert extract_posted_wage("$117,000-$132.6K") == (117000.0, 132600.0, "year")
    assert extract_posted_wage("$95k - $110k") == (95000.0, 110000.0, "year")
    assert extract_posted_wage("$95 - $110k") == (95000.0, 110000.0, "year")
    assert extract_posted_wage("Attractive pay $117K-$132,600 Annual Salary") == (117000.0, 132600.0, "year")
    # the labelled rule cannot backtrack a K away to store $117/hr
    assert scraper._WAGE_LABELLED_RX.search("pay $117K-$132,600") is None
    assert scraper._WAGE_LABELLED_RX.search("pay $117K") is not None
    assert extract_posted_wage("Pay: $20K sign-on, base $20K") is None


# ── S8: FTE / part-time annual figures (cause H) ─────────────────────────────

def test_prorated_annual_figures_are_skipped():
    maine = ("The pay range for this position at a .4 FTE is $47,255 to $61,153 annually based on the "
             "candidate's level of experience.")                                                        # 32717610 MaineHealth
    assert extract_posted_wage(maine) is None
    assert extract_posted_wage("Pay range at 0.75 FTE: $60,000 - $70,000") is None
    assert extract_posted_wage("CRNA - .3 FTE\nPay Range: $70,500 - $83,100") is None                   # 33000689 UMMS (title FTE)
    assert extract_posted_wage("Part time, 18 hours per week, salary range $25,262.64- $38,769.12") is None   # 32713409 Northern Light
    assert extract_posted_wage("Salary: $40,000 for a 20 hrs/week schedule") is None
    assert extract_posted_wage("Salary range is 45,000 - 52,000 at .5 FTE") is None                    # bare range
    # a full-time statement keeps reading
    assert extract_posted_wage("Pay Range (1.0 FTE): $70,500 - $83,100") == (70500.0, 83100.0, "year")
    assert extract_posted_wage("RN Med Surg 1.0 FTE\nPay Range: $70,500 - $83,100") == (70500.0, 83100.0, "year")
    assert extract_posted_wage("Salary: $70,000 per year, 40 hours per week") == (70000.0, 70000.0, "year")
    # hourly figures are never prorated
    assert extract_posted_wage("Part time .5 FTE, pay $32 - $38 per hour") == (32.0, 38.0, "hour")
    flagler = ("Compensation and Benefits: Income Guarantee at $354,000 Full-time: 15 shifts per month - 1 shift: 10 hours "
               "APP Supervision: $12,000 per year per 1.0 FTE APP Signing Bonus: up to $30,000")
    assert extract_posted_wage(flagler, "Full time") == (354000.0, 354000.0, "year")


# ── S9: a role word the title does not name demotes the figure (cause R) ────

def test_role_word_penalty_prefers_the_titles_own_range():
    uhs = ("Nurse Practitioner ( NP ) or Physician Assistant ( PA )  - Walk In Clinic\n"
           "Physician: Annual Compensation Range: $420,000-$480,000\n\n"
           "APP Salary Range $147,000-$171,000 annually")                                               # 28055820 shape
    assert extract_posted_wage(uhs) == (147000.0, 171000.0, "year")
    rn = "Registered Nurse - Med Surg\nLPN Compensation Range: $19.79 - $29.69\nRN Compensation Range: $31.00 - $45.00"
    assert extract_posted_wage(rn) == (31.0, 45.0, "hour")
    # no role word, or the title's own: the earliest range still wins
    assert extract_posted_wage("Registered Nurse - Med Surg\nCompensation Range: $19.79 - $29.69\nCompensation Range: $31.00 - $45.00") == (19.79, 29.69, "hour")
    assert extract_posted_wage("Physician Assistant\nPhysician Assistant pay range $60 - $75 per hour") == (60.0, 75.0, "hour")
    assert scraper._role_words("Nurse Practitioner ( NP ) or Physician Assistant ( PA )", title=True) == {"np", "pa", "app"}
    assert scraper._role_words("Physician: Annual Compensation Range:") == {"physician"}


# ── S10: JSON-LD only when the body yields nothing (cause C) ────────────────

def _posting(lo, hi, unit, desc):
    return {"@type": "JobPosting", "description": desc,
            "baseSalary": {"@type": "MonetaryAmount", "currency": "USD",
                           "value": {"@type": "QuantitativeValue", "minValue": lo, "maxValue": hi, "unitText": unit}}}


def test_jsonld_estimate_band_loses_to_the_body_and_to_the_width_test():
    filler = "As a Travel Surgical Technician you scrub in. " * 8
    # 2287623 CommonSpirit: $20-$100 is wider than 3x and dies in _wage_pair
    j = _job(title="Travel Surgical Technician")
    _apply_posting(j, _posting(20, 100, "HOUR", filler))
    assert (j.wage_min, j.wage_max, j.wage_unit) == (None, None, None)
    # 13440119 HCA: $100,000-$400,000 "Salary Estimate" likewise
    j = _job(title="Internal Medicine Physician")
    _apply_posting(j, _posting(100000, 400000, "YEAR", filler))
    assert j.wage_min is None
    # a plausible JSON-LD pair is taken when the body states no figure
    j = _job(title="Lab Supervisor")
    _apply_posting(j, _posting(52.15, 77.9, "HOUR", filler))
    assert (j.wage_min, j.wage_max, j.wage_unit) == (52.15, 77.9, "hour")
    # and left for the body's own figure when the body states one
    j = _job(title="Lab Supervisor")
    _apply_posting(j, _posting(52.15, 77.9, "HOUR", filler + "Pay: $45.50/hr."))
    assert (j.wage_min, j.wage_max, j.wage_unit) == (None, None, None)
    assert extract_posted_wage(f"{j.title}\n{j.description}") == (45.5, 45.5, "hour")
    # a job that already carries a body figure keeps it over the posting's estimate
    j = _job(title="Lab Supervisor", description="Pay: $45.50/hr. " + filler)
    _apply_posting(j, _posting(20, 60, "HOUR", ""))
    assert j.wage_min is None


# ── S11: "up to" note (cause B) ──────────────────────────────────────────────

def test_up_to_pay_note():
    sonder = "Job Types: Part-time, Contract\nPay: Up to $248 per hour (pay dependent on session type)"   # 26661912 SonderMind
    assert extract_posted_wage(sonder) == (248.0, 248.0, "hour")
    assert extract_posting_facts(sonder)["pay_note"] == "up_to"
    assert extract_posting_facts("Compensation: Up to $41.00/hr. (based on years of experience)")["pay_note"] == "up_to"
    assert extract_posting_facts("Base salary up to $100k with full benefits")["pay_note"] == "up_to"
    f = extract_posting_facts("Pay: $40 - $50 per hour. Sign-on bonus up to $10,000.")
    assert "pay_note" not in f
    f = extract_posting_facts("Pay: $45.50/hr. Relocation up to $5,000.")
    assert "pay_note" not in f
    assert extract_posting_facts("We care for patients at home.") is None


# ── the upsert row: a visit unit and a dropped band pass through normalize_job ─

def test_normalize_job_stores_visit_unit_and_drops_boilerplate_band():
    j = _job(title="Registered Nurse/ RN , Home Health", state="TX",
             description="Position Type:PRN\n\nPay: $55 to $160 per visit\n\nSchedule: M-F 8 to 5")
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (55.0, 160.0, "visit")
    j = _job(title="CST/Scrub Tech Contract 40hrs", state="KS", description="Pay Range: $15.00 - $130.00\n\nGuaranteed hours")
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (None, None, None)
