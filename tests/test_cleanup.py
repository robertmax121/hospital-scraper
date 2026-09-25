"""push4/cleanup (2026-09-25): leftovers in the requirements reader found by
the push 3 hand checks, each on a saved body (tests/fixtures/cleanup; the
Kronos, HCTS, HealthcareSource and WPJobBoard bodies were fetched from the
employer pages by the detail passes that go live with 0ff68dc, the rest are
stored bodies), and the one Workday CXS jobPostingInfo reader."""
import os

import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "cleanup")


def _rq(name, text=None):
    if text is None:
        with open(os.path.join(FIX, name), encoding="utf-8") as f:
            text = f.read()
    return scraper.extract_requirements(text)


def _q(rq):
    return rq["qualifications"]["required"] + rq["qualifications"]["preferred"]


def _f(rq, field):
    return [x[0] for x in rq[field]]


def _any(lines, needle):
    return any(needle.lower() in x.lower() for x in lines)


def test_iu_health_closing_paragraph_is_not_education():
    rq = _rq("iu_30748888.txt")
    assert not _any(_f(rq, "education"), "unlike any other")
    assert not _any(_q(rq), "unlike any other") and not _any(_q(rq), "Indiana’s largest employers")
    assert _f(rq, "education") == ["Bachelor's Degree is required."]


def test_iu_health_why_join_ends_the_block():
    rq = _rq("iu_22199459.txt")
    assert not _any(_f(rq, "certifications"), "largest")
    assert not _any(_q(rq), "largest")
    assert _any(_f(rq, "certifications"), "Basic Life Support (BLS) certification")
    assert _any(_f(rq, "licensure"), "Registered Nurse (RN) license in the state of Indiana")
    assert scraper._rq_heading("Why Join IU Health?") == ("stop", None, "")


def test_hca_basic_cardiac_life_support_and_total_rewards():
    rq = _rq("hca_13441212.txt")
    assert _f(rq, "certifications") == ["Basic Cardiac Life Support must be obtained within 30 days of employment start date"]
    assert not _any(_q(rq), "total rewards")
    assert _f(rq, "education") == ["Registered Nurse Diploma"]


def test_great_river_duty_after_an_adverb_is_not_a_requirement():
    rq = _rq("greatriver_24311892.txt")
    assert not _any(_q(rq), "Proactively monitors patients")
    assert _any(_f(rq, "certifications"), "Certified Nursing Assistant within 120 Days")
    # "Currently ..." is never read as a duty
    assert not scraper._RQ_DUTY_RX.search("Currently supports a valid RN license")
    assert scraper._RQ_DUTY_RX.search("Proactively monitors patients for safety")


def test_chs_facility_marketing_is_not_a_certification():
    rq = _rq("chs_wpjobboard_144487.txt")
    certs = _f(rq, "certifications")
    assert not _any(certs, "community healthcare provider")
    assert certs == ["BCLS - Basic Life Support required", "ACLS - Advanced Cardiac Life Support preferred"]
    assert _any(_f(rq, "licensure"), "LPN - Licensed Practical Nurse")
    # the other CHS template: "What Sets Us Apart" / "... is a trusted healthcare provider ..."
    with open(os.path.join(FIX, "chs_wpjobboard_144487.txt"), encoding="utf-8") as f:
        body = f.read()
    body = body.replace("Western Arizona Regional Medical Center is your community healthcare provider",
                        "What Sets Us Apart\n\nEast Georgia Regional Medical Center is a trusted healthcare provider "
                        "serving the Statesboro community with a commitment to quality, safety, and compassionate care."
                        "\n\nOur collaborative environment supports professional growth.\n\nWestern Arizona Regional "
                        "Medical Center is your community healthcare provider")
    certs = _f(_rq("", body), "certifications")
    assert not _any(certs, "Sets Us Apart") and not _any(certs, "trusted healthcare provider")
    assert not _any(certs, "collaborative environment")


def test_hcts_heading_and_degree_of_motivation():
    rq = _rq("hcts_2041392.txt")
    assert not _any(_f(rq, "education"), "high degree of motivation")
    assert _any(_f(rq, "education"), "Doctor of Medicine Degree")
    assert not _any(_f(rq, "certifications") + _q(rq), "License/Registration/Certification")
    assert _any(_f(rq, "licensure"), "Active Texas Medical License")


def test_kronos_licensure_registry_or_certification_heading():
    rq = _rq("kronos_wilbarger_26728502.txt")
    lic = _f(rq, "licensure")
    assert lic == ["Registered Nurse currently licensed by the State of Texas Board of Nursing."]
    assert not _any(_f(rq, "certifications"), "Registered Nurse currently licensed")
    assert _any(_f(rq, "certifications"), "PALS, ACLS, and TNCC")
    # the same line broken after "by the" is read whole
    with open(os.path.join(FIX, "kronos_wilbarger_26728502.txt"), encoding="utf-8") as f:
        body = f.read().replace("currently licensed by the State of Texas", "currently licensed by the\nState of Texas")
    assert _f(_rq("", body), "licensure") == lic


def test_kronos_arnot_ot_licence_is_licensure_not_education():
    rq = _rq("kronos_arnot_2080678796.txt")
    assert _any(_f(rq, "licensure"), "licensed, or eligible to obtain a license, to practice occupational therapy")
    assert not _any(_f(rq, "education"), "licens")
    assert not _any(_f(rq, "education"), "mandatory education programs")


def test_renown_paramedic_licence_is_licensure():
    rq = _rq("renown_hcs_57674.txt")
    assert _any(_f(rq, "licensure"), "Nationally Registered Paramedic licensure")


def test_orlando_heading_text_is_cut_out_of_the_line():
    for name in ("orlando_29001856.txt", "orlando_29001151.txt"):
        rq = _rq(name)
        every = _q(rq) + _f(rq, "certifications") + _f(rq, "licensure") + _f(rq, "education")
        assert not _any(every, "Licensure/Certification"), name
        assert not _any(every, "Education/Training"), name
    rq = _rq("orlando_29001856.txt")
    assert _any(_f(rq, "certifications"), "Maintains current BLS/HealthCare Provider certification")
    assert _any(_f(rq, "education"), "High school graduate or equivalent")


def test_uhs_link_is_not_glued_onto_a_preference():
    rq = _rq("uhs_26531614.txt")
    assert not any("http" in x or "flclearinghouse" in x for x in _q(rq))
    assert _any(rq["qualifications"]["preferred"], "Prefer one (1) year experience as a Practical Nurse")


def test_salinas_licence_under_education_is_licensure_only():
    rq = _rq("salinas_28363684.txt")
    assert _any(_f(rq, "licensure"), "California Occupational Therapy License")
    assert not _any(_f(rq, "education"), "License")


def test_prisma_licenses_heading_glued_to_its_item():
    rq = _rq("prisma_17028496.txt")
    every = _q(rq) + _f(rq, "certifications") + _f(rq, "licensure")
    assert not _any(every, "LicensesHolds")
    assert _any(_f(rq, "licensure"), "Holds a current RN compact/multistate license")


def test_ardent_recruitment_package_is_not_a_qualification():
    rq = _rq("ardent_32452318.txt")
    assert not _any(_q(rq), "Recruitment Package") and not _any(_q(rq), "Smart Technology")
    assert _q(rq) == ["At least 2 years of experience in Sleep Medicine"]


def test_equum_keeps_ability_to_obtain_medical_license():
    rq = _rq("equum_26658817.txt")
    assert _any(_q(rq), "Ability to obtain a state Medical License")
    assert _any(_f(rq, "licensure"), "Ability to obtain a state Medical License")
    assert not _any(_f(rq, "education"), "Medical License")


def test_uf_health_empty_certification_table():
    rq = _rq("ufhealth_29003292.txt")
    every = _q(rq) + _f(rq, "certifications") + _f(rq, "licensure")
    assert not _any(every, "Required/Preferred")
