"""2026-10-29, adversarial review A of the facility release: two kinds of
row the town tables put on a hospital that is not where the posting is.

1. Penn State Health's employer line ("Penn State Health - Hershey Medical
   Center Location: US:PA: State College") names the employing entity, not
   the work site: Hershey Medical Center also employs the medical group's
   practices (State College, Mount Joy, Palmyra ...) and Life Lion EMS. The
   line names the hospital only for a posting in its own town that is no
   clinic's or office's.
2. A title that names a nursing home, a senior residence, a child-care or
   fitness centre, a warehouse or an urgent care keeps the system name, even
   when it also names the town's hospital ("Mon Valley Residence at
   Hilltop").

The postings below are shortened from stored rows of 2026-10-29. No network,
no database."""
import pytest

import facilities_town as T

PSH = "Penn State Health"
HERSHEY = "Penn State Health Milton S. Hershey Medical Center"


@pytest.fixture(autouse=True)
def _no_stored_names():
    T.set_stored([])
    yield
    T.set_stored([])


def _psh_body(town, entity="Hershey Medical Center", extra=""):
    return (f"Penn State Health - {entity} Location: US:PA: {town} Work Type: Full Time FTE: 1.00 "
            f"Shift: Day Hours: 8:00a - 4:30p SUMMARY OF POSITION: {extra} Provides care to patients.")


def test_employer_line_in_another_town_keeps_the_label():
    # 32707884: a medical-group practice in State College, employed by Hershey
    got = T.decide(PSH, PSH, "State College", "PA", "Administrative Associate - Orthopaedics Sports Medicine",
                   _psh_body("State College"))
    assert got == (None, None, None, "entity-elsewhere")


def test_employer_line_at_the_stamped_town_reads_the_town_line():
    # the scraper stamps Hershey; the posting's own town line says Harrisburg
    got = T.decide(PSH, PSH, "Hershey", "PA", "RN Registered Nurse - 401 Division St - Behavioral Health General",
                   _psh_body("Harrisburg"))
    assert got[3] == "entity-elsewhere"
    assert got[0] is None


def test_employer_line_names_the_hospital_in_its_own_town():
    # an emergency department's body speaks of "urgent care": no clinic
    got = T.decide(PSH, PSH, "Hershey", "PA", "RN Registered Nurse - Adult Emergency Department",
                   _psh_body("Hershey", extra="The department provides emergent and urgent care to adults."))
    assert got == (HERSHEY, "Hershey", "PA", "entity")
    got = T.decide(PSH, PSH, "Enola", "PA", "Registered Nurse - Med Surg",
                   _psh_body("Enola", entity="Hampden Medical Center"))
    assert got == ("Penn State Health Hampden Medical Center", "Enola", "PA", "entity")


def test_employer_line_on_a_clinic_posting_keeps_the_label():
    got = T.decide(PSH, PSH, "Hershey", "PA", "Medical Assistant - Orthopaedics Clinic", _psh_body("Hershey"))
    assert got == (None, None, None, "entity-offsite")
    got = T.decide(PSH, PSH, "Hershey", "PA", "Medical Office Associate II - Neurology", _psh_body("Hershey"))
    assert got == (None, None, None, "entity-offsite")


@pytest.mark.parametrize("system,city,state,title", [
    ("Vitruvian Health", "Dalton", "GA", "CNA - Full Time - Nights - Quinton Memorial Health and Rehabilitation"),
    ("Vitruvian Health", "Dalton", "GA", "Wound/Treatment Care Coordinator - Ridgewood"),
    ("Penn Highlands Healthcare", "Monongahela", "PA", "Wait Staff Mon Valley Residence at Hilltop"),
    ("Penn Highlands Healthcare", "St Marys", "PA", "Certified Nurse Aide, Pinecrest Manor"),
    ("Penn Highlands Healthcare", "Brookville", "PA", "Dietary Aide, McKinley Health Center"),
    ("Sanford Health", "Sioux Falls", "SD", "CNA - Full time Days - SF Center"),
    ("Sanford Health", "Bismarck", "ND", "RN - Registered Nurse - FT - Days - Augusta"),
    ("Sanford Health", "Bemidji", "MN", "CNA - Neilson Place - Full Time Days"),
    ("Great River Health", "West Burlington", "IA", "Certified Nurse Assistant (CNA) | 24 Hours per Week | Klein Center"),
    ("Blanchard Valley Health System", "Findlay", "OH", "STNA (Birch) - Full Time, 1st Shift"),
    ("Blanchard Valley Health System", "Findlay", "OH", "Res Care Nurse (Heights)"),
    ("VCU Health", "Richmond", "VA", "Child Care Aide - N-Deck - Rotating"),
    ("Baptist Health (KY/IN)", "Louisville", "KY", "PRN Athletic Trainer"),
    ("Carilion Clinic", "Roanoke", "VA", "WAREHOUSE MATERIALS ASSOCIATE I"),
    ("Valley Health (VA)", "Winchester", "VA", "Radiologic Tech, UCC/OH - Per Diem"),
])
def test_a_site_that_is_no_hospital_keeps_the_label(system, city, state, title):
    spec = T.SPECS[system]
    got = T.decide(system, spec.system, city, state, title, "")
    assert got == (None, None, None, "care-site")


def test_hospital_postings_in_the_same_towns_still_map():
    assert T.decide("Sanford Health", "Sanford Health", "Fargo", "ND",
                    "Registered Nurse - RN - Broadway ICU - Straight Nights", "")[0] == "Sanford Medical Center Fargo"
    assert T.decide("Vitruvian Health", "Vitruvian Health", "Dalton", "GA",
                    "RN - PCU - FT - Nights", "")[0] == "Hamilton Medical Center"
    assert T.decide("Penn Highlands Healthcare", "Penn Highlands Healthcare", "Monongahela", "PA",
                    "Physical Therapist Mon Valley Hospital", "")[0] == "Penn Highlands Mon Valley"


def test_a_hospitals_own_skilled_nursing_unit_is_no_care_site():
    assert not T.CARE_SITE_RX.search("Certified Nursing Assistant-Skilled Nursing Unit")
    assert not T.CARE_SITE_RX.search("Nurse Resident - RN Surgical Ortho")
    assert not T.CARE_SITE_RX.search("RN Residency Program - Med/Surg")


@pytest.mark.parametrize("system,city,state,title", [
    ("Boston Medical Center Health System", "Boston", "MA", "Registered Nurse, Critical Care (All Locations)"),
    ("St. Luke's University Health Network", "Coaldale", "PA",
     "Experienced Registered Nurse, Emergency Department, Opportunities at Multiple Campuses"),
    ("UCLA Health", "Santa Monica", "CA", "Float RN - Oncology, Various Locations"),
])
def test_a_posting_for_several_sites_keeps_the_label(system, city, state, title):
    got = T.decide(system, T.SPECS[system].system, city, state, title, "")
    assert got == (None, None, None, "multi-site")


def test_a_posting_that_names_another_campus_by_its_short_name_keeps_the_label():
    bmc = "Boston Medical Center Health System"
    # Boston is the stamped town: the posting's own location line names it
    where = "Location: Boston, MA\nDepartment: Med/Surg"
    assert T.decide(bmc, bmc, "Boston", "MA", "Certified Nursing Assistant - 32 Hours, Days (BMC South)",
                    where)[3] == "other-hospital"
    assert T.decide(bmc, bmc, "Boston", "MA", "Certified Nursing Assistant - 32 Hours, Days",
                    where)[0] == "Boston Medical Center"
    # the short name never names a hospital by itself; the town still does
    assert T.decide(bmc, bmc, "Brockton", "MA", "Certified Nursing Assistant - 32 Hours, Days (BMC South)",
                    "")[0] == "Boston Medical Center - Brockton"
