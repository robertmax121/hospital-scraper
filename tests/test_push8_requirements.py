"""push8/requirements (2026-10-05): requirement items cleaned at the source,
new licensure / certification headings and must-clauses, bare credential
lines, requirements.experience, and the wider benefits rules.

Every text below is copied from a stored hospital_jobs description (the row
id is named beside it); only the surrounding lines are trimmed."""
import scraper


def _items(rq, field):
    return [x[0] for x in rq[field]]


# ── (a) item cleanup ───────────────────────────────────────────────────────

def test_zero_width_characters_leave_the_items():
    # HCA Healthcare 13444471 (Workday template with U+200B before items)
    text = ("Qualifications\n\n​​Associate Degree in Nursing or RN Diploma​ – required\n\n"
            "​Currently licensed as a registered professional nurse in the state(s) of practice and/or has an "
            "active compact license, in accordance with law and regulation - required\n\n"
            "​Basic Life Support (BLS) – required\n\n​\n\nBenefits\n")
    rq = scraper.extract_requirements(text)
    every = _items(rq, "education") + _items(rq, "licensure") + _items(rq, "certifications") + \
        rq["qualifications"]["required"] + rq["qualifications"]["preferred"]
    assert every and not any("​" in s for s in every)
    assert "Basic Life Support (BLS) – required" in _items(rq, "certifications")
    assert "Associate Degree in Nursing or RN Diploma – required" in _items(rq, "education")
    assert "" not in rq["qualifications"]["required"]


def test_zero_width_only_line_is_empty():
    assert scraper._rq_clean("​​") == ""
    assert scraper._rq_clean("﻿ ​ ") == ""


def test_no_break_spaces_read_as_spaces():
    # GoHealth Urgent Care 23624701
    s = scraper._rq_item_text("Minimum\xa01 year of full-time experience\xa0as a licensed NP or PA")
    assert s == "Minimum 1 year of full-time experience as a licensed NP or PA"


def test_list_enumerators_are_stripped():
    # Stanford Health Care 28893405, Beth Israel Lahey 32575468, UPMC
    assert scraper._rq_item_text(
        "(c) CRNA - Valid California license to practice as a Certified Registered Nurse Anesthetist as well as "
        "having current national certification.").startswith("CRNA - Valid California license")
    assert scraper._rq_item_text("C. Maintains necessary continuing education requirements for license") \
        == "Maintains necessary continuing education requirements for license"
    assert scraper._rq_item_text("3. 1+ year of experience in Nursing or relevant area.") \
        == "1+ year of experience in Nursing or relevant area."
    assert scraper._rq_item_text("b) 6 months of experience as UPMC Nursing Assistant/CNA") \
        == "6 months of experience as UPMC Nursing Assistant/CNA"
    # never a degree abbreviation or a figure that is not a list number
    assert scraper._rq_item_text("B.S. in Nursing") == "B.S. in Nursing"
    assert scraper._rq_item_text("2. 5 mg dose") == "2. 5 mg dose"


def test_enumerated_item_in_a_block_is_stored_without_its_number():
    # Stanford Health Care 28893405
    text = ("Licenses and Certifications\n\nPA - Physician Assistant State Licensure or\n\n"
            "(c) CRNA - Valid California license to practice as a Certified Registered Nurse Anesthetist as well as "
            "having current national certification.\n\nThese principles apply to all employees:\n")
    lic = _items(scraper.extract_requirements(text), "licensure")
    assert any(s.startswith("CRNA - Valid California license") for s in lic)
    assert not any(s.startswith("(c)") for s in lic)


def test_long_item_is_cut_on_a_word_break():
    # Sutter Health 27259892: stored as "... Drug Enforcement Administration (DEA), Food and Drug Administ"
    line = ("Requires a basic working knowledge of legal requirements and accreditation standards including National "
            "Association of Boards of Pharmacy (NABP), The Joint Commission (TJC), Title XXII, United States Department "
            "of Homeland Security (DHS), Drug Enforcement Administration (DEA), Food and Drug Administration (FDA) and "
            "United States Pharmacopeia (USP).")
    rq = scraper.extract_requirements("Qualifications\n\n" + line + "\n")
    items = rq["qualifications"]["required"] + _items(rq, "licensure")
    assert items
    for s in items:
        assert len(s) <= 300
        assert line.startswith(s)
        nxt = line[len(s):len(s) + 1]
        assert nxt in ("", " ", ","), (s[-30:], nxt)
        assert not s.endswith(("Administ", " and", ","))


def test_cut_helper():
    assert scraper._rq_cut("short") == "short"
    s = scraper._rq_cut("word " * 80)
    assert len(s) <= 300 and s.endswith("word")
    # a 320-character run with no space keeps the hard cut
    assert len(scraper._rq_cut("x" * 320)) == 300


def test_prefix_duplicate_is_skipped():
    # VCU Health 29297481: the same licence twice, once with its deadline
    text = ("Licensure, Certification, or Registration Requirements for Hire:\n"
            "Current licensure with the Virginia State Board of Pharmacy required or obtained within the first 90 days "
            "of employment\n\n"
            "Licensure, Certification, or Registration Requirements for continued employment:\n"
            "Current licensure with the Virginia State Board of Pharmacy\n\n"
            "Experience REQUIRED: N/A\n")
    lic = _items(scraper.extract_requirements(text), "licensure")
    assert lic == ["Current licensure with the Virginia State Board of Pharmacy required or obtained within the first "
                   "90 days of employment"]


def test_prefix_duplicate_longer_replaces_shorter():
    # the shorter form first: the longer one takes its place, once
    text = ("Licensure:\nCurrent licensure with the Virginia State Board of Pharmacy\n"
            "Current licensure with the Virginia State Board of Pharmacy required or obtained within the first 90 days "
            "of employment\n")
    lic = _items(scraper.extract_requirements(text), "licensure")
    assert len(lic) == 1 and lic[0].endswith("90 days of employment")


def test_short_items_are_not_prefix_duplicates():
    # under 20 characters both stay ("BLS" and "BLS required" are separate lines)
    text = "Certifications:\nBLS\nBLS required within 30 days\n"
    certs = _items(scraper.extract_requirements(text), "certifications")
    assert "BLS required within 30 days" in certs


def test_default_issuing_body_is_dropped():
    # Geisinger 14750770
    s = scraper._rq_item_text("Basic Life Support Certification - Default Issuing BodyDefault Issuing Body, "
                              "Licensed Practical Nurse - Default Issuing BodyDefault Issuing Body")
    assert s == "Basic Life Support Certification, Licensed Practical Nurse"
