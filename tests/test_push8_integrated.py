"""push8 integration (2026-10-05): the merged requirements / schedule /
plumbing branches, the one FACTS_VERSION bump (plan item 10) and the fix the
combined replay found. Texts are copied from stored hospital_jobs
descriptions (row id named beside each)."""
import scraper


def test_facts_version_bumped_once_for_push8():
    assert scraper.FACTS_VERSION == 4
    f = scraper.posting_facts_for("Current BLS certification required. " * 60, None, "Registered Nurse")
    assert f["v"] == 4


# Mercy 28999687: a "NOTE:" line under "Certification(s):" used to end the
# lic+cert block and silence every certification under it.
MERCY = ("Minimum Qualifications:\n\n"
         "Education: Graduate of an accredited practical nursing program; Consideration will be given for "
         "comparable education experience and successful licensure.\n\n"
         "Licensure: Is personally responsible for obtaining and maintaining a current LPN license within the "
         "hiring state in which nursing duties are performed and must meet all State Board of Nursing requirements.\n\n"
         "Certification(s):\n\n"
         "NOTE: one or more of the certifications below may be required based on the position/unit hired to, or "
         "acquisition of certification within department required timeframe:\n\n"
         "ACLS (Advanced Cardiac Life Support)\n\n"
         "PALS (Pediatric Advanced Life Support)\n\n"
         "BLS (Basic Life Support) certification through the American Heart Association or successful completion "
         "of course within 30 days of hire.\n\n"
         "Physical Requirements:\n\n"
         "Position requires the ability to push, pull, and/or lift 50 lbs. on a regular basis.\n")


def test_mercy_note_line_keeps_the_certification_block():
    rq = scraper.extract_requirements(MERCY)
    certs = [x[0] for x in rq["certifications"]]
    assert "ACLS (Advanced Cardiac Life Support)" in certs
    assert "PALS (Pediatric Advanced Life Support)" in certs
    assert any(c.startswith("BLS (Basic Life Support)") for c in certs)
    assert not any(c.startswith("NOTE") for c in certs)
    assert rq["licensure"] and rq["licensure"][0][0].startswith("Is personally responsible")


def test_note_line_still_ends_a_block_with_nothing_after_it():
    # a note with no credential after it still closes the block
    text = ("Certification(s):\n\nBLS required.\n\n"
            "NOTE: This position description is not all-inclusive.\n\n"
            "We are proud to serve our community and offer excellent care.\n")
    rq = scraper.extract_requirements(text)
    certs = [x[0] for x in rq["certifications"]]
    assert certs == ["BLS required."]


# Adventist Health 8098720 (Oracle): "Item: Required" table rows. A short
# label before ": Required" / ": Preferred" used to read as a bare mode
# heading, so the RN licence was dropped (facts_backfill dry run, push8/plumbing).
ADVENTIST = ("Job Requirements:\n\nEducation and Work Experience:\n\n"
             "Bachelor’s Degree in nursing or equivalent combination of education/related experience: Required\n\n"
             "Master's Degree: Preferred\n\n"
             "Five years' technical experience: Preferred\n\n"
             "Supervisory experience or demonstrated supervisory skills: Required\n\n"
             "Licenses/Certifications:\n\n"
             "Registered Nurse (RN) licensure in the state of practice: Required\n\n"
             "Cardiopulmonary Resuscitation (CPR) certification or Basic Life Support (BLS) certification from "
             "approved vendor per AH policy: Preferred\n\n"
             "Clinical specialty and/or nursing administration certification: Preferred\n")


def test_adventist_item_required_rows_are_items():
    rq = scraper.extract_requirements(ADVENTIST)
    assert rq["licensure"] == [["Registered Nurse (RN) licensure in the state of practice: Required", False]]
    assert ["Master's Degree: Preferred", True] in rq["education"]
    assert ["Five years' technical experience: Preferred", True] in rq["experience"]
    assert ["Clinical specialty and/or nursing administration certification: Preferred", True] in rq["certifications"]


def test_amedisys_one_plus_year_in_parentheses_is_the_minimum():
    # Amedisys 34746468: "One (1+) year" was unread, so the later "less than
    # 1 year" (an exception clause) became the experience figure.
    text = ("Qualifications\n\n"
            "One (1+) year of clinical experience as a Registered Nurse (RN). If less than 1 year clinical "
            "experience as a RN, candidate must be approved by VP Clinical.*\n\n"
            "Current RN license in state of practice.\n")
    f = scraper.extract_posting_facts(text, "Full time", "Registered Nurse")
    assert f["experience"][0] == "1+ years"


def test_monadnock_benefits_definition_is_not_the_positions_hours():
    # Monadnock Community Hospital 35437993
    text = ("Working Hours:\n\nThis is a 24 hour per week position - night shift\n\n"
            "About Our Benefits:\n\nTuition reimbursement\n\n"
            "*Part time employees are defined as working 22.5 to 35.99 hours per week\n\n"
            "Apply Now! or click the Apply button above\n")
    f = scraper.extract_posting_facts(text, "Part time", "Health Unit Coordinator - Emergency Department - Part Time Nights")
    assert f["hours"] == 24


def test_wellstar_eligible_label_before_hours_still_reads():
    # Wellstar 22076378: "Eligible" belongs to the bonus label, not the hours
    text = ("Work Shift***Sign-On Bonus Eligible***Hours: Full-Time 40 hrs a week (5/8s) Job Summary:"
            "The physical therapist assesses patients.\n")
    f = scraper.extract_posting_facts(text, "Full time", "Physical Therapist")
    assert f["hours"] == 40


def test_bare_flag_labels_still_set_the_mode():
    # "Preferred:" and a label naming nothing stay headings
    assert scraper._rq_heading("Preferred:")[0] == "mode"
    assert scraper._rq_heading("Travel: Required") is None or scraper._rq_heading("Travel: Required")[0] != "licensure"
    assert scraper._rq_heading("RN licensure: Required") is None
