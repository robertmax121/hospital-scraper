"""Push 3 integration fixes to extract_requirements / strip_html (2026-09-24),
each on a saved body. The audit of the push 3 branches found these after two
agents had tuned the same extractor. No network, no database."""
import os
import re

import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _read(*parts):
    with open(os.path.join(FIX, *parts), encoding="utf-8") as f:
        t = f.read()
    return scraper.strip_html(t) if re.search(r"<(?:li|div|p|br|ul|strong)\b", t) else t


def _rq(*parts):
    return scraper.extract_requirements(_read(*parts))


def _quals(rq):
    return rq["qualifications"]["required"] + rq["qualifications"]["preferred"]


def _items(rq, field):
    return [x[0] for x in rq[field]]


BOILER = re.compile(r"(?i)equal (employment )?opportunit|\bEEO\b|search firm|Fortune|all rights reserved|401\s?\(?k|"
                    r"medical, dental|benefit|tuition|paid time off|background screening|https?://|headquartered|"
                    r"no phone calls|subsidiaries")


# (a) the legal / corporate tail never becomes a qualification

def test_uhs_icims_eeo_search_firm_and_benefits_tail_stay_out():
    rq = _rq("integrated", "icims_uhs_eeo_tail_26534632.txt")
    q = _quals(rq)
    assert not [x for x in q if BOILER.search(x)], q
    # the requirements before the tail are all still there
    assert any(x.startswith("High School diploma or equivalent required") for x in q)
    assert "Basic knowledge of IS standards and quality management methods." in q
    assert any("High School diploma" in x for x in _items(rq, "education"))


def test_uhs_icims_fortune_and_headquarters_prose_stay_out():
    rq = _rq("integrated", "icims_uhs_fortune_31454973.txt")
    q = _quals(rq)
    assert not [x for x in q if BOILER.search(x) or re.search(r"Growing steadily|World.s$", x)], q
    assert "Licensed Practical Nurse LPN, licensed in the State of Michigan required." in q
    assert "Previous psychiatric experience preferred." in rq["qualifications"]["preferred"]
    assert any("State of Michigan" in x for x in _items(rq, "licensure"))


def test_uhs_recruitment_scam_notice_stays_out():
    rq = _rq("integrated", "icims_uhs_scam_notice_27661571.txt")
    q = _quals(rq)
    assert not [x for x in q if re.search(r"(?i)scam|beware of anyone", x)], q
    assert any("state and federal regulatory" in x for x in q)
    assert any(x.startswith("Bachelor's Degree required") for x in _items(rq, "education"))


def test_adventhealth_background_screening_notice_and_link_stay_out():
    rq = _rq("integrated", "findly_adventhealth_screening_26016320.txt")
    assert not [x for x in _quals(rq) if re.search(r"(?i)background screening|clearinghouse|https?://", x)]


def test_tail_cut_keeps_the_requirement_before_it_on_the_same_line():
    body = ("Qualifications\n"
            "- Current BLS certification EEO Statement All UHS subsidiaries are committed to providing an environment "
            "of mutual respect where equal employment opportunities are available to all applicants.\n"
            "- Excellent Medical, Dental, Vision and Prescription Drug Plans\n")
    rq = scraper.extract_requirements(body)
    assert rq["qualifications"]["required"] == ["Current BLS certification"]
    assert _items(rq, "certifications") == ["Current BLS certification"]


# (b) licence lines under combined headings

def test_wvu_combined_education_certification_licensure_heading_keeps_the_rn_licence():
    rq = _rq("integrated", "workday_wvu_rn_31907161.txt")
    lic = _items(rq, "licensure")
    assert any(x.startswith("Current Registered Nurse license issued by the state in which services will be provided")
               for x in lic), lic
    # "CORE DUTIES AND RESPONSIBILITIES: The statements ..." ends the block: no duty is a certification
    certs = _items(rq, "certifications")
    assert not [x for x in certs if re.search(r"CORE DUTIES|Prioritizes|Functions as|Advocates|^12\.$|Other duties", x)], certs
    assert "Obtain certification in Basic Life Support within 30 days of hire date." in certs


def test_paycom_licenses_certification_label_files_the_rn_as_licensure():
    rq = _rq("integrated", "paycom_fhs_rn_32150616.txt")
    assert "Registered Nurse in the State." in _items(rq, "licensure")
    assert "Registered Nurse in the State." not in _items(rq, "certifications")
    assert "Current BLS." in _items(rq, "certifications")


# (c) BS / MS "in <field>" are education

def test_bs_and_ms_in_a_field_are_education():
    rq = _rq("license", "phenom_ot_17355703.txt")
    edu = rq["education"]
    assert ["BS in Occupational Therapy", False] in edu
    assert ["MS in Occupational Therapy", True] in edu
    assert _items(rq, "licensure") == ["SC OT License"]
    assert scraper._rq_types("Must have MS in Nursing") == {"education"}
    assert "education" not in scraper._rq_types("Works as in the unit")          # lower-case "as in" is not a degree
    assert "education" not in scraper._rq_types("THIS ROLE IS AS IN THE PAST")   # nor all-caps prose


# (c2) AdventHealth degree levels under an "Education:" heading are education
# (live 3b74770 kept them; the reqfix narrowing of the education block lost
# "Associate" and "Master's" because _RQ_EDU_RX wants "degree/of/in" after them)

def test_adventhealth_associate_under_education_heading_is_education():
    rq = _rq("integrated", "findly_adventhealth_associate_19004264.txt")
    assert rq["education"] == [["Associate [Required]", False]]
    assert "Basic Life Support - CPR Cert (BLS) [Required]" in _items(rq, "certifications")


def test_adventhealth_masters_under_education_heading_is_education():
    rq = _rq("integrated", "findly_adventhealth_masters_17341304.txt")
    assert rq["education"] == [["Master's [Required]", False]]
    assert "Basic Life Support - CPR Cert (BLS) [Required]" in _items(rq, "certifications")


def test_degree_level_line_is_education_only_inside_an_education_block():
    for line, pref in (("Associate [Preferred]", True), ("Associate's [Required]", False), ("Associates [Preferred]", True),
                       ("Master’s [Preferred]", True), ("Masters Social Work required", False),
                       ("Technical/Vocational School [Required]", False)):
        rq = scraper.extract_requirements(f"Education:\n\n{line}\n\nWork Experience:\n\n1+ years [Preferred]")
        assert rq["education"] == [[line, pref]], (line, rq["education"])
    # a role or an experience line under the heading is not a degree
    rq = scraper.extract_requirements("Education:\n\nAssociate Director experience in a hospital [Preferred]")
    assert rq["education"] == []
    # outside an education block a bare level word is not education
    rq = scraper.extract_requirements("Qualifications\nMaster scheduler experience required\nAssociate [Required]")
    assert rq["education"] == []


# (d) "Board Eligible2 years" glue

def test_count_glued_to_the_previous_item_is_split():
    rq = _rq("integrated", "wellstar_board_eligible_22077288.txt")
    assert _items(rq, "licensure") == ["Must be Board Certified/Board Eligible"]
    assert not [x for x in _items(rq, "licensure") + _items(rq, "certifications") if "years" in x]


# (e) "The ideal candidate" is a heading only as the whole label

def test_ideal_candidate_heading_is_the_whole_label_not_a_sentence():
    assert scraper._rq_heading("The Ideal Candidate:")[:2] == ("qual", "pref")
    assert scraper._rq_heading("The ideal candidate will have:")[:2] == ("qual", "pref")
    assert scraper._rq_heading("The ideal candidate has pediatric experience and core values") is None
    assert scraper._RQ_PHRASE_HEAD_RX.match("The ideal candidate has pediatric experience") is None


# (f) words split across inline tags

def test_word_split_across_inline_tags_is_joined():
    html = "<p>Requirements:</p><p><span>Mu</span><span>st be licensed as a Florida Registered Nurse</span></p>"
    assert scraper.strip_html(html) == "Requirements:\n\nMust be licensed as a Florida Registered Nurse"
    # never across cells, block tags, or where a space or a capital sits at the tag
    assert scraper.strip_html("<td>pay</td><td>rate</td>") == "pay rate"
    assert scraper.strip_html("<p>one</p><p>two</p>") == "one\n\ntwo"
    assert scraper.strip_html("<b>Education:</b>Bachelor") == "Education: Bachelor"
    assert scraper.strip_html("<b>Nurse</b> <i>license</i>") == "Nurse license"
    assert scraper.strip_html("<span>RN</span><span>license</span>") == "RN license"
    # the split-figure join from 3b74770 still works
    assert scraper.strip_html("$<span>5,0</span><span>00</span> sign-on") == "$5,000 sign-on"


# outside a block a duty is not a credential (push3/nobody2's Oceans body)

def test_duty_naming_a_licence_outside_a_block_is_not_licensure():
    with open(os.path.join(FIX, "nobody", "oceans_20443.html"), encoding="utf-8") as f:
        posting = scraper._posting_with_requirements(scraper._jobposting_from_html(f.read()))
    rq = scraper.extract_requirements(scraper.strip_html(posting["description"]))
    assert not [x for x in _items(rq, "licensure") + _items(rq, "certifications") if x.startswith("Directs ")]
    body = "About the role\nEnhances clinical skills by maintaining the Physician's certification and state licensure.\n"
    assert scraper.extract_requirements(body)["licensure"] == []
    # a requirement sentence outside a block still counts
    assert scraper.extract_requirements("Must hold a current Texas RN license.")["licensure"]


# a heading label is never an item; a misspelled duties heading still ends the block

def test_heading_label_left_as_an_item_is_dropped():
    body = ("Education/Training Bachelor's degree from an approved program in occupational therapy. Licensure/Certification\n"
            "Licensure/Certification\n"
            "Maintains current Occupational Therapist license in the State of Florida.\n"
            "Required\n")
    rq = scraper.extract_requirements("Qualifications\n" + body)
    everything = _quals(rq) + _items(rq, "certifications") + _items(rq, "licensure") + _items(rq, "education")
    assert "Licensure/Certification" not in everything and "Required" not in everything
    assert any("Occupational Therapist license in the State of Florida" in x for x in _items(rq, "licensure"))


def test_misspelled_responsibilities_heading_ends_the_block():
    body = ("Qualifications\nValid Florida state license.\nBLS/Healthcare Provider certification required.\n"
            "Responsabilities\nApplies principles of radiation safety to minimize exposure to patients, self, and others.\n"
            "Assesses the patient's physical condition and age specific needs.\n")
    rq = scraper.extract_requirements(body)
    assert rq["qualifications"]["required"] == ["Valid Florida state license.", "BLS/Healthcare Provider certification required."]


def test_facts_version_bumped_once_for_push3():
    assert scraper.FACTS_VERSION == 3
