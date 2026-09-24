"""Licensure vocabulary (push3/license, 2026-09-24).

The requirements extractor caught nursing licences and little else: on a
hand-labelled sample of 167 stored full bodies it caught 93 of 133 stated
licences (CNA/EMT state certification 1 of 11, behavioural health 4 of 12,
PT/OT/SLP 7 of 13). Thirteen stored bodies, one per shape that failed or
that must stay out, pin the widened vocabulary:

MUST lists what each posting states as a licence (a distinctive piece of
the text a licensure line must hold); NEVER lists text that must not reach
licensure (duties, a licensed facility, a national registry team line).
"""
import pathlib

import pytest

import scraper

FIX = pathlib.Path(__file__).parent / "fixtures" / "license"

CASES = {
    # Findly-Google "Licenses and Certifications:" list: the RN line is the
    # licence; CCRN / SCRN / RDMS lines stay certifications.
    "findly_google_rn_6456249.txt": (
        ["Registered Nurse (RN) [Required]"],
        ["Critical Care Registered Nurse", "Stroke Certified Registered Nurse", "Registered Diagnostic Medical Sonographer"]),
    # "License/Registration/Certifications" heading (Phenom).
    "phenom_ot_17355703.txt": (["SC OT License"], ["CPR-AHA BLS"]),
    # Washington social-work licences with no "license" word after them.
    "talentbrew_social_worker_7168648.txt": (
        ["Licensed Social Worker Associate Independent Clinical", "Licensed Advanced Social Worker",
         "Licensed Social Worker Associate Advanced", "Licensed Agency Affiliated Counselor"], []),
    "findly_google_pharmacist_8271503.txt": (["Licensed Pharmacist (RPH)"], []),
    # Texas DSHS EMT / paramedic state certification counts; CCMA / CMA stay certifications.
    "oracle_ma_emt_26702419.txt": (
        ["Licensed-Paramedic", "EMT - Emergency Medical Tech Texas Department of State Health Services"],
        ["CCMA - Cert-Cert Clinical MA", "CMA-AAM"]),
    # A body stored as one 4,279-character line.
    "icims_oneline_ot_26531539.txt": (["Licensed by the State of Texas required"], ["Used under license"]),
    "talentbrew_cna_registry_32102370.txt": (["State Nurse Aide Registry"], ["non-licensed workers"]),
    "preloadstate_neurologist_32152316.txt": (
        ["valid Texas license to practice medicine", "Board-certified Adult Neurologist", "Drug Enforcement Agency (DEA)"], []),
    "findly_mammo_facility_licensed_650012.txt": ([], ["licensed as a sub-site"]),
    "oracle_dental_assistant_2304111.txt": (
        ["licensed dental assistant in the state of Minnesota"],
        ["team composed of dentists", "within the scope of practice", "is hiring a Licensed Dental Assistant"]),
    # "Licensure: <value>" is one label line; the duties, EEO and drug-testing
    # lines after it are not licences.
    "applicantmanager_sonographer_27754034.txt": (
        [], ["Exhibits working knowledge", "Refers to physician orders", "Equal Employment", "Drug Testing", "drug free workplace"]),
    "icims_rrt_29000781.txt": (["Respiratory Care Practitioner (RCP) license"], ["Prefer critical care experience"]),
    "workday_np_dea_22066077.txt": (
        ["Current state licensure as PA or Advanced Practice Nurse", "Controlled Substance License and DEA License"], []),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_licensure_lines(name):
    text = (FIX / name).read_text(encoding="utf-8")
    must, never = CASES[name]
    lic = [x[0] for x in scraper.extract_requirements(text)["licensure"]]
    for piece in must:
        assert any(piece.lower() in x.lower() for x in lic), (piece, lic)
    for piece in never:
        assert not any(piece.lower() in x.lower() for x in lic), (piece, lic)


@pytest.mark.parametrize("line", [
    "Current RN license in the state of Texas required",
    "Must have Virginia license or ability to obtain",
    "VA License Speech Language Pathologist",
    "Current NYS LPN License required",
    "Licensed Clinical Social Worker preferred",
    "Independently licensed therapists (LCSW, LMFT, LPC, LMHC or equivalent) required",
    "Active compact multi-state LPN/LVN license required",
    "Board Certified/Eligible in General Surgery required",
    "Current DEA registration required",
    "Valid RN licensures as required by state regulations.",
    "Registration or licensure with state board of pharmacy required",
    "Must be licensed by the Texas Medical Board (TMB)",
    "Current Georgia Paramedic certification required",
    "Current CNA certification listed on the State Nurse Aide Registry",
    "Must be a PSYPACT authorized licensed psychologist",
    "California Pharmacy Technician License required",
    "Eligible for licensure as a Physical Therapist in Ohio",
])
def test_stated_licence_is_licensure(line):
    assert scraper._rq_lic(line), line


@pytest.mark.parametrize("line", [
    "Valid driver's license and reliable transportation required",
    "Our 378 licensed beds serve the region",
    "Must work at the appropriate level of licensure",
    "Tuition reimbursement and continuing education available",
    "License reimbursement and CME allowance",
    "Supervises licensed and unlicensed staff",
    "Notify the physician and/or Licensed Independent Provider (LIP) of abnormal findings.",
    "At least 5 years of clinical experience as a Licensed Practical Nurse is required.",
    "Our facility is licensed as a sub-site of the main hospital.",
    "Complies with DEA regulations for controlled substances.",
    "Pay rates are based on the provider license type and session types.",
])
def test_not_licensure(line):
    assert not scraper._rq_lic(line), line


def test_four_field_format_is_unchanged():
    got = scraper.extract_requirements("Qualifications:\nCurrent Texas RN license required\nBLS required\nBSN preferred")
    assert set(got) == {"qualifications", "certifications", "licensure", "education"}
    assert set(got["qualifications"]) == {"required", "preferred"}
    assert got["licensure"] == [["Current Texas RN license required", False]]
    assert all(isinstance(x, list) and len(x) == 2 for f in ("certifications", "licensure", "education") for x in got[f])
