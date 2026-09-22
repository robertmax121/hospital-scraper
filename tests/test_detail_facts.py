"""Detail passes and posting facts for the job page v2 (2026-09-21)."""
import scraper
from scraper import (_jobposting_from_html, _apply_posting, _oracle_posting_text, _phenom_posting_text,
                     extract_posting_facts, extract_schedule, extract_benefits, canonical_job_type, Job)


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


LD = '''<html><head><script type="application/ld+json">
{"@context":"https://schema.org","@type":"JobPosting","title":"Lab Supervisor","datePosted":"2026-9-16",
 "employmentType":"Full Time","description":"<p>Job Summary and Responsibilities</p><p>%s</p>",
 "qualifications":"<ul><li>Bachelor's degree in clinical laboratory science</li><li>ASCP certification</li></ul>",
 "baseSalary":{"@type":"MonetaryAmount","currency":"USD","value":{"@type":"QuantitativeValue","minValue":52.15,"maxValue":77.9,"unitText":"HOUR"}}}
</script></head><body></body></html>''' % ("As a Lab Supervisor you lead the bench. " * 8)


def test_jsonld_posting_fills_description_type_date_and_pay():
    p = _jobposting_from_html(LD)
    assert p and p["@type"] == "JobPosting"
    j = _job()
    assert _apply_posting(j, p) is True
    assert "Lab Supervisor you lead the bench" in j.description
    assert "Qualifications" in j.description and "ASCP certification" in j.description
    assert j.job_type == "Full Time"
    assert j.posted_date == "2026-09-16"
    assert (j.wage_min, j.wage_max, j.wage_unit) == (52.15, 77.9, "hour")


def test_jsonld_posting_never_overwrites():
    j = _job(description="already here " * 80, job_type="Per diem", posted_date="2026-01-01")
    p = _jobposting_from_html(LD)
    assert _apply_posting(j, p) is False          # a longer stored body stays
    assert j.job_type == "Per diem" and j.posted_date == "2026-01-01"
    teaser = _job(description="short teaser " * 20)   # 260 chars: a fetched full body replaces it
    assert _apply_posting(teaser, p) is True and "lead the bench" in teaser.description


def test_no_jsonld_is_none():
    assert _jobposting_from_html("<html><body>nothing</body></html>") is None


def test_oracle_posting_text():
    it = {"JobSchedule": "Full time", "JobShift": "Shift1 - Day", "WorkHours": 36, "WorkDays": None,
          "ExternalDescriptionStr": "<p>Schedule</p><p>Full Time: 36 hours per week; 3 12 hour shifts</p>",
          "ExternalResponsibilitiesStr": "<p>Essential Functions: obtains vital signs.</p>",
          "ExternalQualificationsStr": "<p>Minimum Education: High School Diploma or GED Required</p>",
          "ExternalPostedStartDate": "2026-09-11T16:48:53+00:00"}
    desc, sched, start = _oracle_posting_text(it)
    assert desc.startswith("Schedule: Full time · Shift1 - Day · 36 hours per week")
    assert "Essential Functions" in desc and "Minimum Education" in desc
    assert sched == "Full time" and start == "2026-09-11"


def test_phenom_posting_text():
    desc, jt, created = _phenom_posting_text({"description": "<p>" + "Care for residents. " * 20 + "</p>", "shift": "Day", "type": "Full-time", "dateCreated": "2026-06-16T09:52:43.962+0000"})
    assert desc.startswith("Schedule: Day") and jt == "Full-time" and created == "2026-06-16"


TEXT = ("Full Time: 36 hours per week; 3 12 hour shifts (7:30 AM - 8:30 pm) with weekend rotation, Friday - Sunday. "
        "$10,000 sign-on bonus for experienced RNs and a $3,000 relocation package. "
        "Benefits from day one: medical, dental and vision coverage, paid time off, 403(b) retirement plan, "
        "4 weeks 100% paid parental leave, and tuition reimbursement. Requires BLS and a current RN license.")


def test_facts_schedule_bonus_benefits():
    f = extract_posting_facts(TEXT)
    assert f["schedule"].startswith("Fri–Sun · 3×12h")
    assert f["hours"] == 36
    assert f["signon"] == 10000 and f["relocation"] == 3000
    assert f["benefits"][:3] == ["Medical, dental and vision", "Benefits from day one", "403(b)"]
    assert "Paid time off" in f["benefits"]
    assert any(c[0] == "BLS" for c in f["certs"])


def test_facts_amounts_need_dollars_and_sanity():
    assert (extract_posting_facts("Sign-on bonus available. Great benefits.") or {}).get("signon") is None
    assert extract_posting_facts("Sign on bonus up to $15k for nights")["signon"] == 15000
    assert (extract_posting_facts("relocation of $250,000 is not a bonus") or {}).get("relocation") is None
    both = extract_posting_facts("$3,000 relocation assistance and a $10,000 sign-on bonus")
    assert (both["signon"], both["relocation"]) == (10000, 3000)


def test_schedule_only_hours():
    summary, hours = extract_schedule("This is a 24 hours per week position.", [])
    assert (summary, hours) == ("24 hrs/wk", 24)
    assert extract_schedule("No schedule words here.", []) == (None, None)


def test_benefits_cap_and_401k_dedupe():
    b = extract_benefits("401(k) with company match, 401(k), PTO, dental and vision, pension, EAP, childcare, tuition reimbursement")
    assert len(b) == 7 and "401(k) with match" in b and "401(k)" not in b
    b = extract_benefits("401(k) match, PTO, dental and vision, pension, EAP, childcare, tuition reimbursement, "
                         "parental leave, life insurance, shift differential, malpractice coverage")
    assert len(b) == 8


def test_job_type_canon():
    assert canonical_job_type("FULL_TIME") == "Full time"
    assert canonical_job_type("Full-time") == "Full time"
    assert canonical_job_type("Regular Full time") == "Full time"
    assert canonical_job_type("PRN/Per Diem") == "Per diem"
    assert canonical_job_type("Part Time") == "Part time"
    assert canonical_job_type("", "RN, PRN Nights") == "Per diem"
    assert canonical_job_type("Regular", "Registered Nurse") == ""
    assert canonical_job_type("Travel Nurse") == "Travel"
    assert canonical_job_type("Fellowship") == "Intern"
    assert canonical_job_type("Weekend Program") == "Weekend Program"


def test_budgets_and_flags_exist():
    assert scraper.DETAIL_FETCH in (True, False)
    for b in (scraper.ORACLE_DESC_BUDGET, scraper.TB_DESC_BUDGET, scraper.PHENOM_DESC_BUDGET):
        assert b.remaining >= 0


def test_bonus_ignores_salary_gaps():
    f = extract_posting_facts("Compensation: Salary up to $100k based on experience. Incentives: Sign on bonus up to $10k and student debt repayment")
    assert f["signon"] == 10000
    f2 = extract_posting_facts("Compensation: starting at $76,000 annual base. Incentives: potential for sign on bonuses")
    assert (f2 or {}).get("signon") is None
    f3 = extract_posting_facts("Physician Gastroenterology: CME allowance, Sign-on bonus - $100K, paid malpractice")
    assert f3["signon"] == 100000


def test_budget_share_and_skip(monkeypatch):
    import asyncio
    b = scraper._DescBudget(100)
    seen = []
    async def fetch(job):
        seen.append(job.url); return True
    jobs = [_job(url=f"https://x/job/{i}", job_id=str(i)) for i in range(80)]
    jobs += [_job(url=f"https://acme.taleo.net/j/{i}", job_id=f"t{i}") for i in range(20)]
    asyncio.run(scraper._detail_pass(None, "T", jobs, b, fetch, "Test", skip=lambda j: "taleo" in j.url, share=30))
    assert len(seen) == 30 and all("taleo" not in u for u in seen)
    assert b.remaining == 70 and b.spent == 30



# ── 2026-09-22: strip_html keeps line structure and decodes entities ─────────
def test_strip_html_block_tags_become_line_breaks():
    from scraper import strip_html
    html = ("<p>Benefits from Day One: Medical, Dental, Vision Insurance</p>"
            "<ul><li>Paid Time Off from Day One</li><li>403-B Retirement Plan</li></ul>"
            "<p>Shift: Full Time Days Mon-Fri</p><p>Location: Providing coverage</p>")
    out = strip_html(html)
    assert "InsurancePaid" not in out
    lines = [l for l in out.splitlines() if l]
    assert lines[1] == "Paid Time Off from Day One"
    assert lines[3] == "Shift: Full Time Days Mon-Fri"
    assert lines[4] == "Location: Providing coverage"


def test_strip_html_decodes_entities_and_keeps_plain_text():
    from scraper import strip_html
    assert strip_html("New York State License&nbsp;<br>Advance certification &amp; ARRT") == "New York State License\nAdvance certification & ARRT"
    assert strip_html("Plain text, no markup.") == "Plain text, no markup."
    assert strip_html("Inline <b>bold</b> word") == "Inline bold word"


def test_canonical_job_type_rejects_pay_text():
    from scraper import canonical_job_type
    assert canonical_job_type("$16.58 - $26.53", "Certified Medical Assistant Float Pool") == "Per diem"
    assert canonical_job_type("$16.58 - $26.53", "Clinic RN") == ""
    assert canonical_job_type("40 hours/week", "ICU RN Full Time Nights") == "Full time"
    assert canonical_job_type("Volunteer", "") == "Volunteer"
    assert canonical_job_type("FULL_TIME", "") == "Full time"



# ── 2026-09-22: employment type from a labelled body line (win 4) ────────────
def test_job_type_from_text_reads_labelled_lines_only():
    from scraper import job_type_from_text
    assert job_type_from_text("Our promise\nSchedule: Full time\nShift: Day") == "Full time"
    assert job_type_from_text("Job Type: Part-Time\nLocation: Tampa") == "Part time"
    assert job_type_from_text("Status: PRN\n") == "Per diem"
    assert job_type_from_text("Employment Type: Full-time or Part-time\n") == ""       # two types: skip
    assert job_type_from_text("Full-time employees receive medical, dental and vision.") == ""
    assert job_type_from_text("") == ""


def test_workday_detail_defaults_are_on():
    import scraper
    assert scraper.WD_FETCH_DESCRIPTIONS is True or scraper.WD_FETCH_DESCRIPTIONS == (scraper.os.getenv("WD_FETCH_DESCRIPTIONS", "1") == "1")
    assert scraper.WD_DESC_MAX_PER_RUN >= 500
    assert scraper.ORACLE_DESC_MAX_PER_RUN >= 3000


# ── 2026-09-22: the Charlie Health lesson (hospital_jobs 26658803) ──────────
# Pay listed per employment type, a "(base + bonus)" note, a "Signing
# Bonuses!" line right under the hourly figure, and an eleven-item benefits
# list: the page showed a BLS estimate, five renamed benefits and 12 hrs/wk.
CHARLIE = """Work Type: 100% Remote (W-2)
We also believe clinicians deserve an exceptional compensation and benefits package.
Compensation

Full-Time Salary: (base + bonus) $70,000-$80,000
Part-Time Rate: $54-$66/hour
Signing Bonuses!

Benefits

401(k) with matching
Medical, dental, and vision insurance
Wellness stipend
Free online CEUs
Malpractice liability insurance
PTO (vacation, sick time, select federal holidays)
Reimbursement for new license applications
Opportunity for cross-licensure sponsorship (if eligible)
Transparent scheduling- know your schedule ahead of time
Dedicated operational, HR, and IT support
24/7 Employee Assistance Program

The Provider Experience at Charlie Health:

Flexibility: Work 100% remote from the comfort of your home - no commute, no problem!

Part-Time: Minimum 12 hours/week; flexible scheduling
Full-Time: 40 hours/week; evening availability required
"""


def test_wage_survives_bonus_words_in_other_clauses():
    from scraper import extract_posted_wage
    assert extract_posted_wage(CHARLIE, "Full time") == (70000.0, 80000.0, "year")
    assert extract_posted_wage(CHARLIE, "Part time") == (54.0, 66.0, "hour")
    assert extract_posted_wage(CHARLIE) == (70000.0, 80000.0, "year")            # unknown type: the full-time headline
    assert extract_posted_wage("Part-Time Rate: $54-$66/hour\nSigning Bonuses!") == (54.0, 66.0, "hour")
    assert extract_posted_wage("Salary: $70,000 per year plus bonus potential") == (70000.0, 70000.0, "year")
    assert extract_posted_wage("Base pay $32 - $38 per hour, bonus eligible") == (32.0, 38.0, "hour")
    assert extract_posted_wage("Pay $30 - $35/hr plus a $2,500 bonus") == (30.0, 35.0, "hour")
    assert extract_posted_wage("$15/hr shift differential on nights; base pay $32 - $38 per hour") == (32.0, 38.0, "hour")
    # Select Medical's flattened bodies glue the bonus label onto the figure
    assert extract_posted_wage("Compensation: $35.67 - $48.00 (based on experience)Sign-on Bonus: $10,000 Select Specialty") == (35.67, 48.0, "hour")
    assert extract_posted_wage("Compensation: Up to $47 per hour, based on experience Sign on bonus: $10,000") == (47.0, 47.0, "hour")
    assert extract_posted_wage("Compensation: Up to $41.00/hr. (based on years of experience)Sign-on Bonus! $5,000") == (41.0, 41.0, "hour")
    assert extract_posted_wage("Compensation: $41.00 per hour (competitive shift differentials)Sign on bonus: $15,000") == (41.0, 41.0, "hour")
    assert extract_posted_wage("Compensation: $52 per hour plus shift differential for weekend shifts Sign on bonus:$5,000") == (52.0, 52.0, "hour")
    assert extract_posted_wage("Compensation:$55/hr + Shift Differential Our inpatient") == (55.0, 55.0, "hour")
    assert extract_posted_wage("Pay Rate $55.67 - 75.63, plus night shift differential**7,500 Sign on Bonus**") == (55.67, 75.63, "hour")
    assert extract_posted_wage("Salary: $24.00/hourly + Mileage Reimbursement Up to 80% travel") == (24.0, 24.0, "hour")
    assert extract_posted_wage("Sign-on bonus: $15,000Compensation: $40.00 to $53.87 per hour + differentials") == (40.0, 53.87, "hour")
    assert extract_posted_wage("Weekend only RNs earn an additional $20/hour for weekend incentive pay in addition to their base rate") is None


def test_wage_labelled_bare_figures_and_k_suffix():
    from scraper import extract_posted_wage
    # Flagler Health physician posting: the guarantee is the pay, the retention
    # bonus with a parenthetical aside is not, and the stored row had the bonus.
    flagler = ("Compensation and Benefits: Income Guarantee at $354,000 Full-time: 15 shifts per month - 1 shift: 10 hours "
               "APP Supervision: $12,000 per year per 1.0 FTE APP Signing Bonus: up to $30,000 Retention Bonus (beginning at the "
               "completion of the 2nd year): up to $30,000 per year Relocation bonus: Up to $10,000 (based on location)")
    assert extract_posted_wage(flagler, "Full time") == (354000.0, 354000.0, "year")
    assert extract_posted_wage("Salary: $85,000") == (85000.0, 85000.0, "year")
    assert extract_posted_wage("Pay rate $42") == (42.0, 42.0, "hour")
    assert extract_posted_wage("The salary range is $95k - $110k depending on experience") == (95000.0, 110000.0, "year")
    assert extract_posted_wage("Base salary up to $100k") == (100000.0, 100000.0, "year")
    assert extract_posted_wage("Earn $45k annually with full benefits") == (45000.0, 45000.0, "year")
    assert extract_posted_wage("Sign-on bonus: $10,000 and a $2,000 referral bonus") is None
    assert extract_posted_wage("Compensation up to $2,000 in referral bonuses") is None
    assert extract_posted_wage("Salary: $100,000 - $120,000 per year") == (100000.0, 120000.0, "year")
    # backfill sample 2026-09-22: a labelled figure must not backtrack to its first digits
    assert extract_posted_wage("Salary: $12,500 - $15,000 annually Schedule: 2-3 hours/wk") is None
    assert extract_posted_wage("Compensation: $53,5000/Yr At NovaCare") is None


def test_scoreboard_patterns_2026_09_22():
    """The stated-but-not-captured list from the 2,860-row scoreboard."""
    from scraper import extract_posted_wage, extract_posting_facts, job_type_from_text
    # Sharp HealthCare (Workday): three decimals, minimum - midpoint - maximum
    sharp = "On-Call Required:NoHourly Pay Range (Minimum - Midpoint - Maximum):$83.970 - $108.360 - $121.360 The stated pay scale reflects the range"
    assert extract_posted_wage(sharp) == (83.97, 121.36, "hour")
    # St. Charles (Phenom): a differential item glued after a labelled range
    assert extract_posted_wage("Relief, Variable Pay range: $26.18 - $33.51 Relief Differential - 15% Swing Shift Differential - $2.50/hr") == (26.18, 33.51, "hour")
    assert extract_posted_wage("Night Differential: $10.00/hr Weekend Differential: $2.00/hr Labor and Delivery Experience Required") is None
    # SonderMind (Ashby): contract therapists above the old $250/hr ceiling
    assert extract_posted_wage("contract position (1099) Pay: up to $296 per hour. Pay rates are based on the provider license type") == (296.0, 296.0, "hour")
    # TriHealth (Oracle): differentials alone are not pay
    assert extract_posted_wage("Night shift nurses receive a $4/hour shift differential plus a $6/hour night shift premium.") is None
    # education beyond nursing
    f = extract_posting_facts("Required Education Bachelor's Degree in finance, accounting, or other related field Experience 2 years")
    assert f["education"][0][0] == "Bachelor's degree"
    assert extract_posting_facts("What you’ll need: H.S. Diploma or Equivalent Required And Completion of approved LMRT training")["education"][0][0] == "HS diploma/GED"
    assert extract_posting_facts("Qualifications Master’s Degree from an Accredited University Licensed as Advance Practice")["education"][0][0] == "Master's degree"
    assert extract_posting_facts("Doctorate degree in physical therapy (DPT) required")["education"][0][0] == "Doctorate"
    assert (extract_posting_facts("Works with associates across the unit; the doctor on call reviews orders.") or {}).get("education", []) == []
    # shifts written without the word "shift"
    assert extract_posting_facts("*** Full- Time Nights; 7pm- 7am; $5000 Sign on Bonus")["shift"][0][0] == "Nights"
    assert extract_posting_facts("Work hours: Full-Time Nights (6:45 PM – 7:15 AM) for 36 hours/week.")["shift"][0][0] == "Nights"
    assert extract_posting_facts("Job Description: Shift: Days Conduct comprehensive assessments")["shift"][0][0] == "Days"
    assert extract_posting_facts("Clinic hours 8:00am - 5:00pm Monday through Friday")["shift"][0][0] == "Days"
    assert all(s[0] != "Nights" for s in (extract_posting_facts("Schedule may include nights and weekends as needed.") or {}).get("shift", []))
    # "Schedule Full-time" with no colon
    assert job_type_from_text("Bilingual preferred\nSchedule Full-time Flexible availability required\n") == "Full time"
    assert job_type_from_text("Full-time, Part-time, and PRN opportunities available.") == ""
    # county boards quote a pay period or a month (NeoGov)
    assert extract_posted_wage("Primary Care Clinic Physician: $9,404.43 - $10,893.28 Biweekly (This amount is prorated") == (244515.18, 283225.28, "year")
    assert extract_posted_wage("Salary $6,500 per month plus benefits") == (78000.0, 78000.0, "year")
    # shift after a comma or a dash
    assert extract_posting_facts("Work hours: Full Time, Nights Department highlights")["shift"][0][0] == "Nights"
    assert extract_posting_facts("Sterile Processing Tech II (Full Time- Nights) Bring your passion")["shift"][0][0] == "Nights"
    assert extract_posting_facts("Relief, Days Pay range: $23.37 - $32.71")["shift"][0][0] == "Days"
    # education written as a list of levels or a fragment
    assert extract_posting_facts("Education: High School Grad or Equiv [Required] Associate [Preferred] Bachelor's [Preferred] Field of Study: N/A")["education"][0][0] == "HS diploma/GED"
    assert extract_posting_facts("Education Associate's from an ARRT accredited program")["education"][0][0] == "Associate degree"


def test_wage_bonus_amounts_still_rejected():
    from scraper import extract_posted_wage
    assert extract_posted_wage("Sign-on bonus: $10,000 for nights") is None
    assert extract_posted_wage("$10,000 sign-on bonus") is None
    assert extract_posted_wage("Eligible for a retention bonus of up to $30,000 over three years") is None
    assert extract_posted_wage("PTO and sign-on bonus up to $30,000") is None
    assert extract_posted_wage("Tuition reimbursement up to $5,250 per year") is None
    assert extract_posted_wage("$15/hr shift differential on nights") is None


def test_hours_and_schedule_follow_the_job_type():
    assert extract_posting_facts(CHARLIE, "Full time")["hours"] == 40
    assert extract_posting_facts(CHARLIE, "Part time")["hours"] == 12
    assert extract_posting_facts(CHARLIE)["hours"] == 40
    assert extract_posting_facts(CHARLIE, "Full time")["schedule"] == "40 hrs/wk"
    assert extract_posting_facts(TEXT)["hours"] == 36                            # a single figure is unchanged


def test_signon_offered_without_amount():
    f = extract_posting_facts(CHARLIE)
    assert f["signon"] is None and f["signon_offered"] is True
    f2 = extract_posting_facts("Sign on bonus up to $15k for nights")
    assert f2["signon"] == 15000 and f2["signon_offered"] is False
    assert extract_posting_facts("Sign-on bonus available. Great benefits.")["signon_offered"] is True
    assert extract_posting_facts("Great benefits and a friendly team.") is None


def test_benefit_lines_verbatim_under_heading():
    from scraper import extract_benefit_lines
    lines = extract_benefit_lines(CHARLIE)
    assert lines[:3] == ["401(k) with matching", "Medical, dental, and vision insurance", "Wellness stipend"]
    assert lines[-1] == "24/7 Employee Assistance Program" and len(lines) == 11
    assert all("$" not in l for l in lines)
    # prose under the heading is not a list; a lone item is not a list
    assert extract_benefit_lines("Benefits\nWe offer a comprehensive package designed to support you and your family through every stage of life and career.") == []
    assert extract_benefit_lines("Benefits:\n- Medical\nResponsibilities:\n- Charting") == []
    assert extract_benefit_lines("What we offer:\n• Medical, dental, vision\n• 403(b) with match.\n• $5,000 sign-on\n\nAbout us\nWe are big.") == ["Medical, dental, vision", "403(b) with match"]
    f = extract_posting_facts(CHARLIE)
    assert f["benefit_lines"] == lines
    assert "Malpractice insurance" in f["benefits"] and "License reimbursement" in f["benefits"] and "Wellness stipend" in f["benefits"]


def test_greenhouse_type_and_pay_fields():
    from scraper import _greenhouse_job_type, _greenhouse_pay
    assert _greenhouse_job_type({"metadata": [{"name": "Employment Type", "value": "Part-time"}]}) == "Part-time"
    assert _greenhouse_job_type({"metadata": [{"name": "Super Region", "value": None}]}) == ""
    assert _greenhouse_job_type({}) == ""
    assert _greenhouse_pay({"pay_input_ranges": [{"min_cents": 7000000, "max_cents": 8000000, "currency_type": "USD"}]}) == (70000.0, 80000.0, "year")
    assert _greenhouse_pay({"pay_input_ranges": [{"min_cents": 5400, "max_cents": 6600, "currency_type": "USD"}]}) == (54.0, 66.0, "hour")
    assert _greenhouse_pay({"pay_input_ranges": [{"min_cents": 100, "max_cents": 200, "currency_type": "USD"}]}) == (None, None, None)
    assert _greenhouse_pay({}) == (None, None, None)
