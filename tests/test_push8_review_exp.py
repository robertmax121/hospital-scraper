"""push8 review (2026-10-05): two reproduced defects in the push 8
requirements rules.

1. requirements.experience filed employer marketing and licence lines that
   only mention new grads (every CoxHealth posting carries "Acknowledged by
   Forbes as one of the Best Employers for New Grads.").
2. _rq_bare_types filed a pay premium line as education (Parkview "BSN
   Premium (If applicable)").

Every text below is copied from a stored hospital_jobs description (the row
id is named beside it); only the surrounding lines are trimmed."""
import scraper


def _items(rq, field):
    return [x[0] for x in rq[field]]


# -- 1. new grads ------------------------------------------------------------

def test_coxhealth_forbes_honour_is_not_experience():
    # CoxHealth 28948449
    text = ("CoxHealth has earned the following honors for workplace excellence:\n\n"
            "Named one of Modern Healthcare’s Best Places to work five times.\n\n"
            "Acknowledged by Forbes as one of the Best Employers for New Grads.\n\n"
            "Healthcare Innovation's Top Companies to Work for in Healthcare (2025).\n\n"
            "Benefits\n\nMedical, Vision, Dental, Retirement with Employer")
    assert _items(scraper.extract_requirements(text), "experience") == []
    assert not scraper._rq_exp_line("Acknowledged by Forbes as one of the Best Employers for New Grads.", outside=True)


def test_uh_why_choose_pitch_is_not_experience():
    # University Hospitals 32693359
    text = ("Harrington Heart & Vascular Institute (Lerner Tower 7)\n\n"
            "Why choose UH as a New Grad RN?\n\nAcademic/Teaching Hospital\n\nNurse focused\n\n"
            "Supportive & education focused culture\n\nMagnet Recognized\n\n"
            "Nurse Residency Program to assist in Transition to Practice\n\nFree parking for full-time caregivers\n")
    assert _items(scraper.extract_requirements(text), "experience") == []


def test_new_grad_only_line_needs_a_welcome_cue():
    # the only experience word is "new grad" and nothing welcomes them
    for s in ("New grads are required to complete Norman Regional's Nurse Residency Program.",   # 35324333
              "New graduates must obtain ARRT-R registry or NMTCB prior to first day of employment.",  # 34414312
              "New Grad Mentorship"):                                                            # 26520662
        assert not scraper._rq_exp_line(s), s
        assert not scraper._rq_exp_line(s, outside=True), s
    # Froedtert 33778344 (under "Additional Preferences:") and Saint Francis
    # 23656513 still state who may apply
    assert scraper._rq_exp_line("New graduate RNs are welcome to apply.")
    assert scraper._rq_exp_line("Not A New Grad Position", outside=True)


def test_new_grad_welcome_kept_in_a_block_and_outside():
    # Froedtert 33778344
    text = ("Education:\n\nDiploma from an accredited school/college of nursing OR Required professional "
            "licensure at time of hire.\n\nAdditional Preferences:\n\nNew graduate RNs are welcome to apply.\n\n"
            "Benefits that help you thrive\n\nComprehensive health coverage: medical, dental, vision\n")
    assert "New graduate RNs are welcome to apply." in _items(scraper.extract_requirements(text), "experience")
    # Saint Francis 23656513
    text = ("Full Time\n\nNights\n\nNot A New Grad Position\n\nPrefer experience in L&D and/or Antepartum\n\n"
            "7p - 7a\n")
    exp = _items(scraper.extract_requirements(text), "experience")
    assert "Not A New Grad Position" in exp
    assert "Prefer experience in L&D and/or Antepartum" in exp


def test_must_submit_instruction_is_not_experience():
    # Northwestern Medicine 30686093
    text = ("Accepted documentation:\n\n"
            "New MA Graduates within six (6) months from date of graduation from an MA Program must submit: "
            "a letter from the school or a letter from the certifying body\n\n"
            "Non-Certified MAs or new graduates past six (6) months from date of graduation from an MA program "
            "must submit: a letter from the certifying body\n")
    assert _items(scraper.extract_requirements(text), "experience") == []


def test_licence_deadline_for_new_grads_is_not_experience_but_licence_with_years_is():
    # HCTS 35490899: a licence deadline for new grads
    s = ("Current unencumbered Oregon license in Radiologic Technology required. If a new graduate, "
         "temporary license upon hire and permanent license within six (6) months")
    assert not scraper._rq_exp_line(s, outside=True)
    # VITAS 2305836: a licence and a years figure; the licence veto does not apply
    text = ("In addition to having your RN license, at least two years of nursing experience, and reliable "
            "transportation, you'll approach your work with the traits that make the VITAS Difference: "
            "Commitment, Compassion, and a Can-do Attitude.\n")
    assert any("at least two years of nursing experience" in x
               for x in _items(scraper.extract_requirements(text), "experience"))
    # Prime Healthcare 35483201: experience, a licence and a welcome on one line
    s = ("One (1) yearclinical experience as a Licensed Vocational Nurse in an acute and/or subacute hospital "
         "environment PREFERRED - New graduates encouraged to apply")
    assert scraper._rq_exp_line(s, outside=True)


def test_title_echo_is_not_experience():
    # Bronson 19385671: the title printed in the body under "Title"
    title = "Clinical Dietitian General Posting-Completing or New Grads Welcome"
    text = ("Location\nBBC Bronson Battle Creek, BMH Bronson Methodist Hospital\n\nTitle\n"
            "Clinical Dietitian General Posting-Completing or New Grads Welcome\n\n"
            "Clinical Dietitian Role Summary\n\nThe Clinical Dietitian is an integral member of the "
            "multidisciplinary healthcare team.\n\nQualifications\n\n"
            "Previous clinical nutrition experience in an acute care, outpatient, or specialty setting.\n")
    assert _items(scraper.extract_requirements(text, title), "experience") == [
        "Previous clinical nutrition experience in an acute care, outpatient, or specialty setting."]
    # without the title the line still reads as a welcome
    assert title in _items(scraper.extract_requirements(text), "experience")


def test_experience_requirements_and_preferences_label_is_not_an_item():
    # iCIMS 22132130: the heading freed by a dropped item must not take its slot
    text = ("Level 4: Nurse practitioner with over 10 years of experience.\n\n"
            "Experience Requirements and Preferences\n\n"
            "Education: Master’s Degree in Nursing (MSN) and/or master’s degree in Physician Assistant Studies\n\n"
            "Experience: At least 1-2 years of experience as a provider in a relevant practice, such as Urgent Care\n")
    assert "Experience Requirements and Preferences" not in _items(scraper.extract_requirements(text), "experience")


# -- 2. pay premium lines ----------------------------------------------------

def test_bsn_premium_is_not_education():
    # Parkview Health 29223868
    text = ("Flex Team members receive $5.00 premium\n\nQualified RN's will receive:\n\nCompetitive Rate of Pay\n\n"
            "Additional Flex Premium\n\nBSN Premium (If applicable)\n\nSign-On Bonus\n\n"
            "Student Loan Repayment- Up to $30,000\n\nTuition Assistance\n\n"
            "Qualifications:\n\nRequires a Diploma in nursing or an ASN/BSN\n")
    edu = _items(scraper.extract_requirements(text), "education")
    assert "BSN Premium (If applicable)" not in edu
    assert "Requires a Diploma in nursing or an ASN/BSN" in edu
    assert scraper._rq_bare_types("BSN Premium (If applicable)") == set()
    # Workday 35282658
    assert scraper._rq_bare_types("Differentials for higher education, certifications, and various lead roles") == set()
    # a bare credential line still reads as one
    assert scraper._rq_bare_types("BSN preferred") == {"education"}
