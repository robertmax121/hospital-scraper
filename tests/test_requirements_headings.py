"""Headings audit 2026-09-24 (owner, after Sentara: "Could there be other
systems who have this similar problem?"). One saved stored body per cause:
a body stored as one line (Akumin), a requirements label whose value is a
sentence (Loma Linda), phrase headings once read as stop headings (HCA
"What qualifications you will need:", RadNet "To ensure success in this
role, you must have:" / "You are:")."""
import pathlib

import scraper

FIX = pathlib.Path(__file__).parent / "fixtures" / "quals"


def rq(name):
    return scraper.extract_requirements((FIX / name).read_text(encoding="utf-8"))


def lines(r):
    return r["qualifications"]["required"] + r["qualifications"]["preferred"]


def no_noise(r):
    for s in lines(r):
        low = s.lower()
        assert "benefit" not in low and "equal opportunity" not in low and "$" not in s, s


def test_one_line_body_is_split_at_its_headings():
    r = rq("wall_akumin_2092824.txt")
    assert any("High School Diploma" in s for s in r["qualifications"]["required"])
    assert any("Associate" in s for s in r["qualifications"]["preferred"])
    assert any("ARRT" in c[0] for c in r["certifications"])
    no_noise(r)


def test_requirements_label_with_a_sentence_value():
    r = rq("label_lomalinda_29194990.txt")
    assert any("Minimum one year of clerical experience" in s for s in lines(r))
    assert any("Knowledge of medical terminology" in s for s in lines(r))
    assert any("Associate's Degree" in e[0] for e in r["education"])
    no_noise(r)


def test_what_qualifications_you_will_need_opens_a_block():
    r = rq("phrase_hca_13439426.txt")
    assert any("Registered Nurse license" in s for s in r["qualifications"]["required"])
    assert any("2 years of experience" in s for s in r["qualifications"]["required"])
    assert any("Bachelors in nursing" in s for s in r["qualifications"]["preferred"])
    assert not any("380+ bed" in s or "Level II Trauma" in s for s in lines(r))
    no_noise(r)


def test_you_must_have_and_you_are_open_a_block():
    r = rq("phrase_radnet_22131037.txt")
    assert any("State License in Diagnostic Radiologic Technology" in s for s in lines(r))
    assert any("ACLS" in c[0] for c in r["certifications"])
    assert any("ARRT" in s for s in r["qualifications"]["preferred"])
    no_noise(r)


def test_duty_and_pay_lines_are_not_headings():
    for s in ("Follow dietary modifications and special meal requirements",
              "Knowledge of third-party reimbursement programs and requirements",
              "This range is an estimate, based on potential employee qualifications: education, experience",
              "Weekend Requirements:", "Physical Requirements:", "Travel Requirements:"):
        h = scraper._rq_heading(s)
        assert h is None or h[0] == "stop", (s, h)
    for s in ("What qualifications you will need:", "Here’s what you need:", "You are:", "About You",
              "Required Criteria", "Candidate Qualifications", "a. Education:",
              "Education, Licenses, and Experiences Required for this Role:"):
        h = scraper._rq_heading(s)
        assert h and h[0] != "stop", (s, h)


def test_line_broken_bodies_are_not_re_split():
    t = "Qualifications:\nBSN required\nBLS required\nACLS preferred\nBenefits:\nMedical"
    assert scraper._rq_unwall(t) == t
