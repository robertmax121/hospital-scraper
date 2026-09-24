"""Qualifications / certifications / licensure / education (2026-09-24).

Owner: "sutter health also not scraping qualifications, certifications,
licensure, or education ... all scraper events should be looking for those 4
things." Seven saved bodies, one per source shape: Sutter, Trinity and
Advocate (Workday CXS detail JSON, fetched 2026-09-24), and the stored bodies
of one Oracle, one iCIMS, one Phenom and one TalentBrew posting.

STATED lists what each posting says, read by hand: every item a field should
hold, named by a distinctive piece of its text. The extractor must capture
every stated item and nothing else (never invent), with the right
required / preferred flag where the posting marks one.
"""
import json
import pathlib

import pytest

import scraper

FIX = pathlib.Path(__file__).parent / "fixtures" / "quals"


def body(name: str) -> str:
    p = FIX / name
    if p.suffix == ".json":
        return scraper.strip_html(json.loads(p.read_text(encoding="utf-8"))["jobPostingInfo"]["jobDescription"])
    return p.read_text(encoding="utf-8")


# field -> [(text piece, preferred)], and the qualification lines as counts
STATED = {
    "workday_sutter_R-129343.json": {
        "q_required": 16, "q_preferred": 0,
        "certifications": [("BLS-Basic Life Support", False)],
        "licensure": [],
        "education": [("High School Diploma", False), ("course of study", False), ("on-the-job training", False)],
    },
    # 2026-09-24 (owner): Sentara's "Required at time of hire:" block.
    "workday_sentara_JR-105919.json": {
        "q_required": 4, "q_preferred": 0,
        "certifications": [],
        "licensure": [],
        "education": [("Bachelor level degree", False)],
    },
    "workday_trinity_00694866.json": {
        "q_required": 3, "q_preferred": 0,
        "certifications": [("AHA BLS", False)],
        "licensure": [("PA RN License", False)],
        "education": [("BSN required", False)],
    },
    "workday_advocate_R192999.json": {
        "q_required": 3, "q_preferred": 0,
        "certifications": [("Basic Life Support (BLS)", False)],
        "licensure": [("Registered Nurse license issued", False)],
        "education": [("approved program in nursing", False)],
    },
    "oracle_32523218.txt": {
        "q_required": 3, "q_preferred": 1,
        "certifications": [("BLS certification", False), ("CPN preferred", True)],
        "licensure": [("Florida RN license", False)],
        "education": [("Bachelors in Science of Nursing", False)],
    },
    "icims_32452199.txt": {
        "q_required": 6, "q_preferred": 3,
        "certifications": [("BLS certification", False), ("require ACLS", False),
                           ("oncology certification", False), ("All required certifications", False)],
        "licensure": [("Kansas RN Licensure", False)],
        "education": [("Associate’s degree in nursing", False), ("Bachelor’s degree in nursing", True)],
    },
    "phenom_32485819.txt": {
        "q_required": 5, "q_preferred": 2,
        "certifications": [],
        "licensure": [("Licensed Practical Nurse (LPN) license", False)],
        "education": [("Practical Nursing program", False)],
    },
    "talentbrew_32495146.txt": {
        "q_required": 11, "q_preferred": 4,
        "certifications": [("Basic Life Support (BCLS)", False), ("Advanced Cardiac Life Support", True)],
        "licensure": [("State Licensure and/or Compact", False)],
        "education": [("accredited school of nursing", False), ("Bachelor’s Degree in Nursing", True)],
    },
}


@pytest.mark.parametrize("name", sorted(STATED))
def test_every_stated_item_and_nothing_else(name):
    got = scraper.extract_requirements(body(name))
    want = STATED[name]
    assert len(got["qualifications"]["required"]) == want["q_required"], got["qualifications"]["required"]
    assert len(got["qualifications"]["preferred"]) == want["q_preferred"], got["qualifications"]["preferred"]
    for field in ("certifications", "licensure", "education"):
        items = got[field]
        assert len(items) == len(want[field]), (field, items)
        for piece, pref in want[field]:
            hit = [it for it in items if piece in it[0]]
            assert hit, (field, piece, items)
            assert hit[0][1] is pref, (field, piece, hit[0])


def test_posting_facts_carries_the_four_fields_and_keeps_the_old_keys():
    f = scraper.extract_posting_facts(body("workday_sutter_R-129343.json"), "Part time")
    rq = f["requirements"]
    assert set(rq) == {"qualifications", "certifications", "licensure", "education"}
    assert rq["licensure"] == []                 # the posting states none: empty, not invented
    # backward compatibility: the chips the site renders today are still filled
    assert ["BLS", False] in f["certs"]
    assert f["education"][0][0] == "HS diploma/GED"
    assert f["experience"][0] == "1+ years"


def test_items_are_verbatim_text_from_the_posting():
    for name in STATED:
        text = body(name)
        got = scraper.extract_requirements(text)
        lines = got["qualifications"]["required"] + got["qualifications"]["preferred"]
        lines += [x[0] for f in ("certifications", "licensure", "education") for x in got[f]]
        for line in lines:
            if line.startswith("Ability to "):          # Sutter's stem + bullet, joined
                continue
            assert line[:40] in text, (name, line)


def test_heading_variants():
    text = ("Job Summary\nProvides care.\n\n"
            "MINIMUM QUALIFICATIONS:\nTwo years of acute care experience\n\n"
            "Preferred Qualifications\nCCRN certification\n\n"
            "Licensure/Certification:\nCurrent Texas RN license or multistate (NLC) license\nACLS within 6 months of hire\n\n"
            "Education/Experience: Graduate of an accredited school of nursing\n\n"
            "Knowledge, Skills and Abilities\nStrong communication skills\n\n"
            "Benefits\nTuition reimbursement toward your BSN\nMedical, dental and vision\n")
    got = scraper.extract_requirements(text)
    assert got["qualifications"]["required"][0] == "Two years of acute care experience"
    assert "CCRN certification" in got["qualifications"]["preferred"]
    assert ["CCRN certification", True] in got["certifications"]
    assert got["licensure"] == [["Current Texas RN license or multistate (NLC) license", False]]
    assert ["ACLS within 6 months of hire", False] in got["certifications"]
    assert got["education"] == [["Graduate of an accredited school of nursing", False]]
    assert "Strong communication skills" in got["qualifications"]["required"]
    # the benefits block is not a requirement
    assert not any("Tuition" in x for x in got["qualifications"]["required"] + got["qualifications"]["preferred"])


def test_prose_posting_without_headings():
    text = ("We are hiring a night nurse for our ICU. Candidates must hold a current California RN license. "
            "BLS and ACLS certification required. A BSN is preferred. Our 378 licensed beds serve the valley.")
    got = scraper.extract_requirements(text)
    assert [x[0] for x in got["licensure"]] == ["Candidates must hold a current California RN license."]
    assert got["certifications"] == [["BLS and ACLS certification required.", False]]
    assert got["education"] == [["A BSN is preferred.", True]]


def test_nothing_stated_nothing_stored():
    text = ("About Us\nWe are a community hospital. We offer tuition reimbursement, continuing education "
            "and a sign-on bonus.\n\nResponsibilities\nEducates patients. Performs treatments to level of licensure.\n"
            "Maintains a safe environment.")
    got = scraper.extract_requirements(text)
    assert got == {"qualifications": {"required": [], "preferred": []},
                   "certifications": [], "licensure": [], "education": []}
    assert scraper.extract_requirements("") == got


def test_drivers_license_is_not_licensure():
    got = scraper.extract_requirements("Requirements\nValid driver's license and auto insurance\nCurrent LVN license in Texas")
    assert got["licensure"] == [["Current LVN license in Texas", False]]
    assert len(got["qualifications"]["required"]) == 2


def test_board_eligibility_and_dea_are_licensure():
    got = scraper.extract_requirements(
        "Qualifications\nBoard certified or board eligible in Family Medicine\nActive DEA registration\nMD or DO degree")
    lic = [x[0] for x in got["licensure"]]
    assert "Board certified or board eligible in Family Medicine" in lic
    assert "Active DEA registration" in lic
    assert ["MD or DO degree", False] in got["education"]


def test_workday_budget_clears_the_backlog_in_a_few_nights():
    # 76,879 body-less active Workday rows on 2026-09-24; Trinity has 7,057
    assert scraper.WD_DESC_MAX_PER_RUN * 3 >= 76879 or scraper.os.getenv("WD_DESC_MAX_PER_RUN")
    assert scraper.WD_DESC_BUDGET.tenant_max == scraper.WD_DESC_TENANT_MAX
    assert -(-7057 // scraper.WD_DESC_TENANT_MAX) <= 3
    assert scraper.WD_DESC_CONCURRENCY == 4          # per-host limit unchanged
    b = scraper._DescBudget(26000, 3000)
    b.expect(["big", "small"])
    assert b.floor() == 3000                          # capped by the Workday tenant max, not 1,500
    assert scraper._DescBudget(26000).tenant_max == scraper.DETAIL_TENANT_MAX
