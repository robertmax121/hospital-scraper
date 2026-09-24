"""2026-09-24 facts audit: bodies from VITAS / Encompass / Lifepoint (Oracle),
Houston Methodist (Workday), SmartRecruiters and Paylocity turned into job-page
facts. Each case below is a pattern the hand-labelled 97-posting sample showed
stated in the text but missed, or captured wrongly, before this change."""
import re
from scraper import (strip_html, _oracle_posting_text, extract_posting_facts, extract_benefits,
                     extract_benefit_lines, job_type_from_text, title_shift)

# The VITAS posting the owner reported (hospital_jobs 2305630, Oracle ejrz CX_5001, job 40788), trimmed.
VITAS = {
    "JobSchedule": "Full time", "JobShift": None, "WorkHours": None, "WorkDays": None,
    "ExternalPostedStartDate": "2026-05-09T13:47:40+00:00",
    "ExternalDescriptionStr": (
        "<div><p><strong>WHO WE ARE</strong></p><p>We are VITAS Healthcare, the nation’s leading end-of-life care provider since 1978.</p>"
        "<p>Salary Range: $34-$42/Hour</p><p><strong>WHAT YOU’LL DO</strong></p>"
        "<p>As a home care registered nurse (RN), you will ensure hospice patients are comfortable and forge compassionate, meaningful connections with the people in their lives.</p>"
        "<p><strong>WHAT’S EXPECTED FROM YOU</strong></p><p><a target=\"_blank\">A VITAS nurse is the end-of-life caregiver everyone deserves. "
        "In addition to having your RN license, at least two years of nursing experience, and reliable transportation, you'll approach your work with the traits that make the VITAS Difference.</a></p>"
        "<p>&nbsp;</p><p>&nbsp;<strong>QUALIFICATIONS</strong></p><ul><li>Currently licensed to practice nursing in the state where the VITAS program is located.</li>"
        "<li>A minimum of two years of nursing experience in hospice, home health, or community health in the last five years.</li>"
        "<li>Equivalent experience or licensure may be considered</li></ul><p>&nbsp;</p>"
        "<p><strong>SPECIAL INSTRUCTIONS TO CANDIDATE</strong></p><ul><li>EOE/AA M/F/D/V</li></ul><div><div><div>&nbsp;</div></div></div></div>"),
    "CorporateDescriptionStr": (
        "<body><p class=\"MsoNormal\">VITAS® Healthcare is the nation’s leading provider of end-of-life care. &nbsp;As a member of the VITAS team, you’ll find fulfillment.</p>"
        "<p>Benefits Include:</p><p>- Competitive compensation&nbsp;<br />- Health, dental, vision, life and disability insurance<br />"
        "- 401(k) plan with numerous investment options and generous company match<br />- Tuition Reimbursement<br />- Paid Time Off<br />"
        "- Employee Assistance Program<b><span><br /></span></b></p>"
        "<p class=\"MsoNormal\"><span>Many of our positions offer the opportunity to work&nbsp;</span><span>day or night shifts, weekdays or weekends.</span></p></body>"),
}


def test_vitas_reported_posting_fills_the_boxes():
    desc, sched, start = _oracle_posting_text(VITAS)
    assert sched == "Full time" and start == "2026-05-09"
    # the posting's own text leads; the schedule closes it; no entity noise
    assert desc.startswith("WHO WE ARE") and desc.endswith("Schedule: Full time")
    assert "&nbsp;" not in desc and re.search(r"QUALIFICATIONS\n+Currently licensed to practice nursing", desc)
    # only the corporate benefits list is taken, never its prose (the "day or
    # night shifts, weekdays or weekends" line would have become a shift chip)
    assert "Benefits\n• Competitive compensation" in desc
    assert "night shifts" not in desc and "people-focused" not in desc
    f = extract_posting_facts(desc, "Full time", "Registered Nurse RN NIGHTS")
    assert f["certs"] == [["RN license", False]]
    assert f["experience"] == ["2+ years", False]
    assert f["shift"] == [["Nights", False]] and f["schedule"] == "Nights"      # from the title
    assert f["benefit_lines"][:2] == ["Competitive compensation", "Health, dental, vision, life and disability insurance"]
    assert "401(k)" in f["benefits"] and "Paid time off" in f["benefits"]


def test_corporate_benefits_only_when_the_posting_has_none():
    own = dict(VITAS, ExternalDescriptionStr="<p>Benefits</p><ul><li>Medical</li><li>Dental</li></ul>" + VITAS["ExternalDescriptionStr"])
    desc, _, _ = _oracle_posting_text(own)
    assert "Competitive compensation" not in desc


def test_oracle_bare_qualifications_part_gets_a_heading():
    desc, _, _ = _oracle_posting_text({"JobSchedule": "Part time", "JobShift": "Night",
                                        "ExternalDescriptionStr": "<p>" + "Cares for patients. " * 12 + "</p>",
                                        "ExternalQualificationsStr": "<ul><li>BLS required</li><li>BSN preferred</li></ul>"})
    assert re.search(r"\n\nQualifications\n+BLS required\n+BSN preferred", desc)
    assert desc.endswith("Schedule: Part time · Night shift")
    f = extract_posting_facts(desc)
    assert f["shift"][0][0] == "Nights" and ["BSN", True] in f["education"]


def test_strip_html_double_entities_bullets_and_split_figures():
    # Houston Methodist Workday: "&amp;#xa;" is a newline two escapes deep
    assert strip_html("Work Shift :&amp;#xa;3 - Night (United States of America)&amp;#xa;Job Category") == \
        "Work Shift :\n3 - Night (United States of America)\nJob Category"
    assert [l for l in strip_html("<ul><li>⦁ CPR certification</li><li>· State licensure</li></ul>").split("\n") if l] == \
        ["• CPR certification", "• State licensure"]
    assert strip_html("<p>Sign-On Bonus: $<span>5,0</span><span>00</span></p>") == "Sign-On Bonus: $5,000"
    assert strip_html("<p>Insurance</p><span>Paid</span> Time Off") == "Insurance\nPaid Time Off"
    assert len(strip_html("x" * 20000)) == 12000


# Houston Methodist's Workday layout (an unpunctuated list under headings).
HM = """QUALIFICATIONS
EDUCATION
Graduate of education program approved by the credentialing body
Bachelor’s degree preferred
EXPERIENCE
Two years nursing experience in a pre-op/PACU, ICU or ED environment
LICENSES AND CERTIFICATIONS
Required
RN - Registered Nurse - Texas State Licensure - Texas Board of Nursing_PSV Compact Licensure – Must obtain permanent Texas license within 60 days and
BLS - Basic Life Support or Instructor (AHA) - American Heart Association and
ACLS - Advanced Cardiac Life Support or Instructor (AHA) - American Heart Association
Preferred
CAPA - Certified Ambulatory Perianesthesia Nurse (ABPANC) and
PALS - Pediatric Advanced Life Support or Instructor (AHA)
SKILLS AND ABILITIES
Uses critical thinking skills
Work Shift :
3 - Night (United States of America)
Job Category :
Clinical"""


def test_heading_context_sets_required_and_preferred():
    f = extract_posting_facts(HM)
    certs = dict((c[0], c[1]) for c in f["certs"])
    assert certs == {"RN license": False, "Compact license": False, "BLS": False, "ACLS": False, "PALS": True}
    assert f["education"] == [["Bachelor's degree", True]]
    assert f["experience"] == ["2+ years", False]
    assert f["shift"][0] == ["Nights", False]          # the two-line Work Shift field


def test_required_preferred_markers_belong_to_their_item():
    f = extract_posting_facts("Requirements: BLS (required), ACLS (preferred)")
    assert dict(map(tuple, f["certs"])) == {"BLS": False, "ACLS": True}
    f = extract_posting_facts("For Nursing, must possess minimum of an Associate Degree in Nursing, RN licensure with BSN preferred.")
    assert ["ADN/ASN", False] in f["education"] and ["BSN", True] in f["education"]
    assert f["certs"] == [["RN license", False]]
    f = extract_posting_facts("Minimum 2 years of nursing experience (hospice, palliative, ER, ICU, home health or case management preferred)")
    assert f["experience"] == ["2+ years", False]
    f = extract_posting_facts("PALS/ACLS/ATLS (BLS preferred)")
    assert dict(map(tuple, f["certs"]))["ACLS"] is False and dict(map(tuple, f["certs"]))["BLS"] is True


def test_degree_and_licence_wordings():
    f = extract_posting_facts("Bachelors in Instructional Design, Education or related field required. Master’s degree preferred.")
    assert f["education"] == [["Bachelor's degree", False], ["Master's degree", True]]
    assert extract_posting_facts("Masters of Social Work required from an accredited school")["education"] == [["Master's degree", False]]
    assert extract_posting_facts("Bachelor's degree in nursing preferred.")["education"] == [["BSN", True]]     # one chip, not BSN + Bachelor's
    assert not (extract_posting_facts("Access Associate (Full-time, Days)") or {}).get("education")
    for t in ("Registered Nurse (RN) with a valid state license", "Current State of Illinois Registered Professional Nurse",
              "RN - Registered Nurse - Texas State Licensure - Texas Board of Nursing"):
        assert extract_posting_facts(t)["certs"][0] == ["RN license", False], t
    assert not (extract_posting_facts("Foreign trained RN who does not hold a Michigan RN license.") or {}).get("certs")
    assert extract_posting_facts("Current licensure by the Michigan State Board of Nursing as a Licensed Practical Nurse.")["certs"] == [["LPN license", False]]
    assert extract_posting_facts("Current Vocational Nurse licensure")["certs"] == [["LPN license", False]]
    assert extract_posting_facts("BCLS - Basic Cardiac Life Support (Required upon hire)")["certs"] == [["BLS", False]]
    # a licence the text names only generically is the title's
    lic = "Currently licensed to practice nursing in the state where the program is located."
    assert extract_posting_facts(lic, None, "Registered Nurse (RN)")["certs"] == [["RN license", False]]
    assert extract_posting_facts(lic, None, "Licensed Practical Nurse (LPN)")["certs"] == [["LPN license", False]]
    assert not (extract_posting_facts(lic, None, "Home Health Aide") or {}).get("certs")


def test_experience_wordings():
    exp = lambda t: (extract_posting_facts(t) or {}).get("experience")
    assert exp("Qualified candidates must have a minimum of (2) years experience in hospice") == ["2+ years", False]
    assert exp("two (2) years of acute care experience") == ["2+ years", False]
    assert exp("3-4 years’ customer service experience a must") == ["3-4 years", False]
    assert exp("At least2years of ultrasound/sonography experience.") == ["2+ years", False]
    assert exp("six months to one-year related experience") == ["1+ years", False]
    assert exp("Twelve months registered nurse experience in a healthcare environment") == ["1+ years", False]
    assert exp("Zero (0) to two (2) years’ experience in a relevant role.") is None
    assert exp("Associate degree or additional two years of experience in lieu of degree. One year of coding experience.") == ["1+ years", False]
    assert exp("2 year / Associate Degree in Nursing, required") is None
    assert exp("Minimum 1–2 years Emergency Medicine or Hospital Medicine experience.") == ["1-2 years", False]


def test_shift_availability_negation_and_fields():
    sh = lambda t, title=None: [s[0] for s in ((extract_posting_facts(t, None, title) or {}).get("shift") or [])]
    assert sh("2. Is willing to work on flexible schedule, i.e., weekends or rotating shifts.") == []
    assert sh("provides shifts during non-business hours such as evenings, weekends and holidays") == []
    assert sh("May be required to work weekdays and/or weekends, evenings and/or night shifts if needed.") == []
    assert sh("Monday–Friday schedule (no weekends or evenings)") == []
    assert sh("Minimum overnight travel (up to 10%)") == []
    assert sh("We offer paid training so nurses feel competent on their first shift.") == []
    assert sh("Available RN Shifts:\n1st, 2nd, or 3rd shift\nPRN, Part-Time, or Full-Time") == []
    assert sh("Shift: 2:00 PM-10:30 PM\nFull time, 40 hours per week.") == ["Evenings"]
    assert sh("Work hours: PRN (As needed) – Monday-Friday 7:00am-5:30pm") == ["Days", "PRN"]
    assert sh("Schedule: Weekend Nights (Saturday, Sunday, Monday - 7:00 pm - 7:30 am)") == ["Nights"]
    # the title's shift, and a time of day ahead of PRN / Weekends
    assert sh("Cares for patients per diem.", "RN Nights (7 on/7off)") == ["Nights", "PRN"]
    assert sh("Cares for patients.", "LVN - Hospital Full-Time Nightshift") == ["Nights"]
    assert title_shift("Registered Nurse (RN) On Call WEEKENDS") == ["Weekends"]


def test_signon_offers_and_non_offers():
    f = lambda t: extract_posting_facts(t) or {}
    nm = ("If sign-on bonus is included in a job posting eligibility is as followed: Internal employees are not eligible for the sign-on bonus. "
          "Sign-on Bonus Eligibility (if sign-on bonus offered for position): rehires are not eligible for the sign on bonus.")
    assert not f(nm).get("signon_offered")
    assert f("Bonus: $10,000 sign‑on bonus + profit sharing")["signon"] == 10000
    assert f("Sign‑On Bonus: $ 5,0 00 About TriHealth")["signon"] == 5000
    assert f("Competitive compensation package, including sign-on and productivity bonuses")["signon_offered"] is True


def test_benefit_vocabulary_ignores_duties_and_demands():
    assert extract_benefits("Must have and maintain transportation with required liability insurance and license.") == []
    assert extract_benefits("Is willing to work on flexible schedule, i.e., weekends.") == []
    assert extract_benefits("Seeks continuing education opportunities to meet those needs.") == []
    assert extract_benefits("Maintain certifications and continuing education requirements needed to perform job.") == []
    assert extract_benefits("c. Professional Development\na. Registered Nurse, RN, develops professional growth goals") == []
    assert extract_benefits("Continuing education opportunities\nMalpractice coverage") == ["Continuing education", "Malpractice insurance"]
    assert "Continuing education" in extract_benefits("Work-Life Balance & Paid Time Off (PTO). Professional Development. For more information")
    assert "Continuing education" in extract_benefits("VITAS supports your professional development with ongoing learning.")


def test_benefit_lines_headings_intros_and_stops():
    enc = ("Starting Perks and Benefits\nAt Encompass Health, we are committed to a caring environment. From day one, you will have access to:\n"
           "Affordable medical, dental, and vision plans.\nGenerous paid time off that accrues over time.\nQualifications\nCPR certification.")
    assert extract_benefit_lines(enc) == ["Affordable medical, dental, and vision plans", "Generous paid time off that accrues over time"]
    uspt = ("What You’ll Get:\nPotential Equity\nClinic mentorship\nExcellent benefits package including:\nHealth, dental, and vision insurance\n401(k)\n"
            "SPECIAL INSTRUCTIONS TO CANDIDATE\nEOE/AA M/F/D/V")
    assert extract_benefit_lines(uspt) == ["Potential Equity", "Clinic mentorship", "Health, dental, and vision insurance", "401(k)"]
    assert extract_benefit_lines("Benefits:\nResponsibilities:\n- Charting\n- Filing") == []


def test_job_type_from_prose():
    assert job_type_from_text("HIRING for a PRN/POOL (as needed) position\nCare for patients.") == "Per diem"
    assert job_type_from_text("This full-time role offers the opportunity to make an impact.") == "Full time"
    assert job_type_from_text("Join us on a PRN basis, where your skills matter.") == "Per diem"
    assert job_type_from_text("HIRING FULL-TIME NIGHTS and PRN all shifts!") == ""
    assert job_type_from_text("Benefits for a full-time position include medical.") == ""
