"""push8/requirements (2026-10-05), plan items 5-7: licence / certification
headings, must-clauses and bare credential lines (5), requirements.experience
and "Under 1 year" (6), the wider benefits rules (7).

Every text below is copied from a stored hospital_jobs description (the row
id is named beside it); only the surrounding lines are trimmed."""
import scraper


def _items(rq, field):
    return [x[0] for x in rq[field]]


# ── plan item 5: headings, must-clauses, bare credential lines ─────────────

def test_upmc_licensure_certifications_and_clearances_heading():
    # UPMC 33727490 / 32690530: the heading used to read as 'stop'
    assert scraper._rq_heading("Licensure, Certifications, and Clearances:")[0] == "lic+cert"
    text = ("ATTENTION: BACHELOR LEVEL TRANSCRIPTS MUST BE ATTACHED WIHT APPLICATION FOR CONSIDERATION.\n\n"
            "Licensure, Certifications, and Clearances:\n\nCardiopulmonary Resuscitation (CPR)\n\n"
            "Basic Life Support (BLS) OR Cardiopulmonary Resuscitation (CPR)\n\n"
            "Comprehensive Crisis Management (CCMC)\n\nAct 31 Child Abuse Reporting with renewal\n\n"
            "Act 33 with renewal\n\nAct 34 with renewal\n\nAct 73 FBI Clearance with renewal\n\n"
            "UPMC is an Equal Opportunity Employer/Disability/Veteran")
    certs = _items(scraper.extract_requirements(text), "certifications")
    assert "Basic Life Support (BLS) OR Cardiopulmonary Resuscitation (CPR)" in certs
    assert "Cardiopulmonary Resuscitation (CPR)" in certs
    # clearances are not certifications
    assert not any(c.startswith("Act ") for c in certs)


def test_geisinger_parenthesised_heading():
    # Geisinger 14750770
    assert scraper._rq_heading("Certification(s) and License(s):")[0] == "lic+cert"
    assert scraper._rq_heading("License(s):")[0] == "lic+cert"
    assert scraper._rq_heading("Certification(s):")[0] == "lic+cert"
    text = ("Education:\nGraduate from Specialty Training Program-Nursing (Required)\n\n"
            "Experience:\nMinimum of 1 year-Nursing (Preferred)\n\n"
            "Certification(s) and License(s):\nBasic Life Support Certification - Default Issuing BodyDefault Issuing Body, "
            "Licensed Practical Nurse - Default Issuing BodyDefault Issuing Body\n\n"
            "Skills:\nCommunication, Computer Literacy, Customer Service, Multitasking, Teamwork\n\n"
            "OUR PURPOSE & VALUES: Everything we do is about caring for our patients, our members, our students, "
            "our Geisinger family and our communities.\n")
    rq = scraper.extract_requirements(text)
    assert _items(rq, "certifications") == ["Basic Life Support Certification, Licensed Practical Nurse"]
    assert _items(rq, "licensure") == ["Basic Life Support Certification, Licensed Practical Nurse"]
    assert _items(rq, "experience") == ["Minimum of 1 year-Nursing (Preferred)"]


def test_cone_listing_heading_is_not_an_item():
    # Cone Health 19885490
    assert scraper._rq_heading("Licensure/Certification/Listing") == ("lic+cert", None, "")
    text = ("Experience\n\nRequired: Completed appropriate residency training (and fellowship training as required).\n\n"
            "Licensure/Certification/Listing\n\nRequired: Fully licensed to practice Medicine in the state of NC.\n")
    rq = scraper.extract_requirements(text)
    assert "Licensure/Certification/Listing" not in _items(rq, "certifications")
    assert "Fully licensed to practice Medicine in the state of NC." in _items(rq, "licensure")


def test_known_labels_keep_their_kind():
    # Methodist 26517106: "Certificate Required" stays a qualifications heading
    assert scraper._rq_heading("Certificate Required")[0] == "qual"
    assert scraper._rq_heading("Required Certifications:") == ("qual", "req", "")    # as before push8
    # VCU Health 29297481
    assert scraper._rq_heading("Licensure, Certification, or Registration Requirements for Hire:")[0] == "lic+cert"


def test_option_care_must_be_licensed_or_registered():
    # Option Care Health 13665839
    text = ("Basic Education and/or Experience Requirements\n\nHigh School Diploma or GED.\n\n"
            "Minimum of 6 months of relevant experience.\n\nMust be licensed or registered (if required by state)\n\n"
            "Basic Qualifications\n\nExperience providing customer service to internal and external customers.\n")
    rq = scraper.extract_requirements(text)
    assert _items(rq, "licensure") == ["Must be licensed or registered (if required by state)"]
    assert "Minimum of 6 months of relevant experience." in _items(rq, "experience")


def test_must_clause_helper():
    assert scraper._rq_must_cred("Must be licensed or registered (if required by state)") == {"licensure"}
    assert scraper._rq_must_cred("Must maintain current BLS certification") == {"certifications"}
    assert scraper._rq_must_cred("Must have a valid driver's license") == set()
    assert scraper._rq_must_cred("Must be able to lift 50 pounds") == set()
    assert scraper._rq_must_cred("The facility must be licensed by the state") == set()


def test_rwjbarnabas_pay_block_first_then_to_be_considered():
    # RWJBarnabas 35482005: "Pay Transparency:" above silenced every line
    text = ("Pay Range: $55.61 - $68.10 per hour\n\nPay Transparency:\n\n"
            "The above reflects the anticipated hourly wage range for this position if hired to work in New Jersey.\n\n"
            "A typical day for a Nuclear Medicine Technologist may include:\n\n"
            "Performs nuclear medicine imaging procedures according to established protocols and physician orders\n\n"
            "Providing a positive, reassuring patient experience is central to your approach\n\n"
            "To be considered for this Nuclear Medicine Technologist opportunity:\n\n"
            "Associate's degree in Nuclear Medicine Technology or completion of an accredited Nuclear Medicine "
            "Technology program\n\nMust hold NMTCB or ARRT (N) certification\n\nMust maintain current BLS certification\n\n"
            "1–3 years of nuclear medicine experience preferred\n\nSchedule: Evening shift\n")
    rq = scraper.extract_requirements(text)
    assert "Must hold NMTCB or ARRT (N) certification" in _items(rq, "certifications")
    assert "Must maintain current BLS certification" in _items(rq, "certifications")
    assert _items(rq, "education")
    assert _items(rq, "experience") == ["1–3 years of nuclear medicine experience preferred"]


def test_rwjbarnabas_to_be_considered_sentence_is_kept_in_a_block():
    # RWJBarnabas 35481772
    text = ("This role might be for you if:\n\n"
            "To be considered for this RN opportunity, you must hold an active New Jersey Registered Nurse license in "
            "good standing and a current American Heart Association BLS certification along with ACLS certification.\n\n"
            "A Bachelor’s degree in Nursing is strongly preferred.\n")
    rq = scraper.extract_requirements(text)
    assert _items(rq, "licensure") and _items(rq, "certifications")
    assert _items(rq, "education") == ["A Bachelor’s degree in Nursing is strongly preferred."]


def test_samaritan_pay_field_above_the_requirements():
    # Samaritan Health 33867584 (no requirements heading at all)
    text = ("Location:\nSamaritan Medical Center\nDepartment:\n01.7350 SMC MEDICAL ONCOLOGY\nPay Range:\n$26.09 - $37.83\n"
            "Care for our community, and your career.\n\n• Graduate from an accredited School of Nursing.\n\n"
            "• Knowledge of new trends and techniques in Nursing.\n\n"
            "• GPN’s must obtain NYS license within 3 months of hire.\n\n"
            "• Current BLS certification required or obtained within 3 months of hire.\n\n"
            "Samaritan is an Affirmative Action/Equal Opportunity Employer.\n")
    rq = scraper.extract_requirements(text)
    assert "GPN’s must obtain NYS license within 3 months of hire." in _items(rq, "licensure")
    assert "Current BLS certification required or obtained within 3 months of hire." in _items(rq, "certifications")
    # pay prose after the pay label stays out
    assert not scraper._rq_after_pay_line("The compensation offered will depend on educational background and experience.")


def test_bare_credential_lines_outside_a_block():
    # York Hospital 27464812 (a bullet list with no heading), UPMC 32690530
    text = ("In order to help us continue to provide exceptional patient experiences, we are looking for a candidate "
            "with the following:\n\n• fluency in diagnostic X-ray is preferred\n\n• BLS Certification\n\n"
            "• Maine RT Licensure\n\nYORK HOSPITAL IS AN EQUAL OPPORTUNITY EMPLOYER.\n")
    rq = scraper.extract_requirements(text)
    assert "BLS Certification" in _items(rq, "certifications")
    assert "Maine RT Licensure" in _items(rq, "licensure")
    assert scraper._rq_bare_types("High School diploma or Equivalent") == {"education"}
    assert scraper._rq_bare_types("NYS license required") == {"licensure"}
    assert scraper._rq_bare_types("Bachelor's Degree") == {"education"}


def test_bare_line_is_never_a_title_or_hospital_prose():
    for s in ("Certified Nursing Assistant", "Licensed Practical Nurse (LPN)", "Registered Nurse",
              "University of Pittsburgh Medical Center", "Magnet-designated hospital with Joint Commission certification",
              "We offer tuition reimbursement for your degree.", "Act 33 with renewal"):
        assert scraper._rq_bare_types(s) == set(), s


def test_experience_as_a_certified_title_is_not_a_certification():
    # Samaritan Health 28896142
    assert "certifications" not in scraper._rq_types(
        "One year or more experience as a Certified Nursing Assistant preferable.")


# ── plan item 6: requirements.experience and "Under 1 year" ────────────────

def test_experience_lines_are_kept_verbatim():
    # HCA 19622111, Samaritan 34391484, BAYADA 30684828
    text = ("Associate or Bachelor’s degree in Nursing from an accredited nursing program\n\n"
            "Registered Nurse License or Graduate Nurse in the State\n\nNo previous experience needed\n\n"
            "Previous hospital RN experience is preferred.\n\nNEW GRADS ENCOURAGED TO APPLY!\n\nBenefits\n\n"
            "We offer a total rewards package to support your health, life, career and retirement.\n")
    exp = _items(scraper.extract_requirements(text), "experience")
    assert exp == ["No previous experience needed", "Previous hospital RN experience is preferred.",
                   "NEW GRADS ENCOURAGED TO APPLY!"]


def test_experience_lines_that_are_not_requirements():
    for s in ("Providing a positive, reassuring patient experience is central to your approach",   # RWJBarnabas 35482005
              "Equivalent education and/or experience may substitute for minimum qualifications",      # UW Medicine 33933806
              "Willingness to become a preceptor after two years of nursing experience",               # 32714175
              "For more than 75 years, Windham Hospital has treated patients",                         # Hartford 13392421
              "Experience the HCA Healthcare difference where colleagues are trusted"):                # HCA 26259923
        assert not scraper._rq_exp_line(s), s
    assert scraper._rq_exp_line("Experience working with children preferred")


def test_under_one_year():
    # The Springs of Mooresville 33695799, plan R7
    assert scraper._experience_from("0-1 Years of relevant experience preferred")[0] == "Under 1 year"
    assert scraper._experience_from("Less than one year of experience")[0] == "Under 1 year"
    assert scraper._experience_from("Experience: 0-1 years")[0] == "Under 1 year"
    assert scraper._experience_from("Experience Required: 2 years")[0] == "2+ years"
    # a ceiling is no minimum (both used to read "2+ years"): 32298492, 26119607
    assert scraper._experience_from("Entry level for RNs with less than 2 years of Nephrology Nursing experience "
                                    "as a Registered Nurse.") is None
    assert scraper._experience_from("Less than 2 years prior relevant experience") is None
    assert scraper._experience_from("2+ years experience required")[0] == "2+ years"


def test_posting_facts_carry_requirements_experience():
    body = "Qualifications\n\nPrevious ICU experience preferred\n\nCurrent BLS required\n\n" + "Duties include patient care. " * 30
    f = scraper.posting_facts_for(body)
    assert f["requirements"]["experience"] == [["Previous ICU experience preferred", True]]


# ── plan item 7: benefits ──────────────────────────────────────────────────

def test_medical_and_dental_within_80_characters():
    # Geisinger 14750917
    t = ("We offer healthcare benefits for full time and part time positions from day one, including vision, dental "
         "and domestic partners.")
    assert "Medical, dental and vision" in scraper.extract_benefits(t)
    # (a guard, not stored text: no sampled body had one) the wider reach
    # never takes a clinic's service list
    assert scraper.extract_benefits("Our community health center offers primary medical care for adults and children "
                                    "in four counties and dental services for the uninsured.") == []


def test_generic_benefits_package_label():
    # Corewell Health 29388810
    t = ("How Corewell Health cares for you\n\nComprehensive benefits package to meet your financial, health, and "
         "work/life balance goals. Learn more here .")
    assert scraper.extract_benefits(t) == ["Benefits package (see posting)"]
    assert scraper.extract_benefits("We offer competitive pay and benefits.") == ["Benefits package (see posting)"]
    assert scraper.extract_benefits("This position is not eligible for full benefits.") == []
    # a specific label wins; the generic one is never added beside it
    assert scraper.extract_benefits("Comprehensive benefits package including 403(b) and paid time off") == \
        ["403(b)", "Paid time off"]
