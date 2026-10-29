"""2026-10-29 review A of facility release 3 (names and towns), on stored
payloads (tests/fixtures/facility_release3_reviewA_1029/rows.json: the rows as
the database holds them, hospital_name put back on the system label the way
the adapter emits it).

The owner's decision 2: named sites show their own name; generic offices,
home-based and remote jobs, and postings for several sites, show the system
name with the town. The sample found rows the release named anyway:

  - MUSC: the "Entity" line was read case-blind and lazily, so a summary
    saying "MCP is an entity within the Medical University ..." swallowed
    it; a long summary pushed it past 1,500 characters; and the system
    office's cost centers ("SYS - IS Workday") were not read at all.
  - town tables: a location line naming an office building, an operations
    building, an airport base or a nursing home; regional / system resource
    pools.
  - group B: remote radiologists (Geisinger, Jefferson), "Apply here for all
    locations", and Jefferson physician-practice, clinic and office postings
    whose practice sits on a hospital's street.
  - DaVita / Fresenius: Integrated Kidney Care, hybrid-remote, regional,
    traveler and regional-float postings filed under a clinic.
  - BrightSpring and UHG: remote, virtual and field-based titles.

Each case is paired with a control from the same system that stays named.
No network, no database."""
import io
import json
import os
from types import SimpleNamespace

import pytest

import facilities_dialysis as DI
import facilities_home_health as HH
import facilities_hospital_systems_b as HSB
import facilities_leftovers  # noqa: F401  registers the MUSC, Cook, Sentara, Avera tables
import facilities_text as FT
import facilities_town as T
import facilities_uhg as UHG

ROWS = json.load(io.open(os.path.join(os.path.dirname(__file__), "fixtures", "facility_release3_reviewA_1029",
                                      "rows.json"), encoding="utf-8"))


def _row(key):
    return ROWS[key]


def _decide(key):
    r = _row(key)
    return T.decide(r["hospital_system"], r["hospital_system"], r["city"], r["state"], r["title"],
                    r["description"], r["url"])


def _job(key, **over):
    r = dict(_row(key))
    r.update(over)
    return SimpleNamespace(hospital_system=r["hospital_system"], hospital_name=r["hospital_name"],
                           city=(r["city"] or "").strip(), state=(r["state"] or "").strip(),
                           location=r.get("location") or "", title=r["title"] or "",
                           description=r["description"] or "", url=r["url"] or "", job_id=r["job_id"],
                           ats_platform="")


# ── MUSC: entity line and system-office cost centers ────────────────────────
@pytest.mark.parametrize("key, why", [
    ("musc_mcp", "MUSC Community Physicians (an entity within the university) employs the practice"),
    ("musc_univ_long", "the university (Entity line past 1,500 characters)"),
    ("musc_sys", "a system-office cost center (SYS - IS Workday)"),
])
def test_musc_practice_university_and_system_office_keep_the_label(key, why):
    name, _city, _state, how = _decide(key)
    assert name is None, why
    assert how == "entity-office"


def test_musc_hospital_authority_rows_still_take_the_hospital():
    name, city, state, how = _decide("musc_muha_rn")
    assert (name, city, state) == ("MUSC Health University Medical Center", "Charleston", "SC")


def test_musc_entity_line_is_read_whole_and_short():
    rx, values = T.SPECS["MUSC Health"].entity
    m = rx.search(T.clean_text(_row("musc_mcp")["description"]))
    assert m and m.group(1) == "MUSC Community Physicians (MCP)"
    assert T.SPECS["MUSC Health"].entity_chars > len(_row("musc_univ_long")["description"][:1500])


# ── town tables: office buildings, airports, nursing homes, regional pools ──
@pytest.mark.parametrize("key, how", [
    ("cook_rosedale", "offsite"),            # Location: Rosedale Office Building
    ("avera_oahe_manor", "offsite"),         # Location: Avera Oahe Manor (a nursing home)
    ("sentara_regional_pool", "multi-site"),  # Eastern Market Regional Resource Pool (SCH, SOH, SWRMC)
])
def test_town_rows_that_are_no_hospitals(key, how):
    name, _c, _s, got = _decide(key)
    assert name is None
    assert got == how


def test_town_controls_keep_their_hospital():
    assert _decide("cook_medical_center")[0] == "Cook Children's Medical Center"
    # a hospital's own nursing resource pool is that hospital's
    assert _decide("sentara_rmh_pool")[0] == "Sentara RMH Medical Center"


def test_where_not_hospital_reads_location_lines_only():
    assert T.where_not_hospital("Location: Rosedale Office Building\nDepartment: Technology Services")
    assert T.where_not_hospital("Building Location: Essentia Virginia Hospital\nDepartment: 2106640 "
                                "ENVIRONMENTAL SERVICES - VIR SNF")
    assert not T.where_not_hospital("Location: Medical Center - Fort Worth\nDepartment: Maintenance")
    assert not T.where_not_hospital("We also run a retirement community down the road.")


@pytest.mark.parametrize("title", [
    "ICU/Critical Care Registered Nurse- Eastern Market Regional Resource Pool (SCH, SOH,SWRMC)",
    "RN - Toledo Hospital - System Resource Pool - Tier IV - ED/AC",
    "Registered Nurse - Divisional Float Resource (Full Time, Nights) Northeast",
    "RN Division Resource, Full-Time, Nights, Resource RN Regional, Clinical Supervisor",
])
def test_regional_and_system_pools_are_multi_site(title):
    assert T.is_multi_site(title, "")


@pytest.mark.parametrize("title", ["Registered Nurse (RN) Resource Pool", "RN - Float Pool, Jefferson",
                                   "Facility Resource Pool, PRN, Days"])
def test_a_hospitals_own_pool_is_not_multi_site(title):
    assert not T.is_multi_site(title, "")


@pytest.mark.parametrize("title, want", [
    ("Chief Technology Officer", True),
    ("Chief Learning Officer", True),
    ("Regional Chief Operating Officer - Central Region", True),
    ("Associate Chief Medical Information Officer - North Region - Jefferson Abington", True),
    ("Chief Nursing Officer", False),
    ("Chief Executive Officer", False),
    ("Chief Operating Officer, West Penn Hospital (AHN)", False),
])
def test_system_office_executive_titles(title, want):
    assert bool(T.SYSTEM_ROLE_RX.search(title)) is want


# ── group B: remote, all locations, Jefferson practices ─────────────────────
def _geisinger_after_text(key):
    FT.set_known_facilities([])
    job = _job(key)
    FT.apply_facility(job)
    return job


def test_geisinger_remote_radiologist_goes_back_to_the_label():
    job = _geisinger_after_text("geisinger_remote")
    assert job.hospital_name == "Geisinger Medical Center"       # the Location line names GMC
    stats = HSB.apply_after_town([job])
    assert job.hospital_name == "Geisinger"
    assert stats["Geisinger"] == {"label remote": 1}


def test_geisinger_all_locations_posting_goes_back_to_the_label():
    job = _geisinger_after_text("geisinger_all_locations")
    HSB.apply_after_town([job])
    assert job.hospital_name == "Geisinger"


def test_geisinger_hospital_row_keeps_its_hospital():
    job = _geisinger_after_text("geisinger_gswb")
    HSB.apply_after_town([job])
    assert job.hospital_name == "Geisinger South Wilkes-Barre"


def test_not_one_site_reads_negated_remote_as_onsite():
    assert HSB.not_one_site("NP Inbox Management (Hybrid)", "This is not a fully remote position") == ""
    assert HSB.not_one_site("Radiologist", "This is a fully remote position") == "remote"
    assert HSB.not_one_site("RN - Remote Telemetry Unit", "") == ""


def test_jefferson_practice_posting_on_a_hospital_street_keeps_the_label_with_its_town():
    r = _row("jefferson_jup_audiology")
    assert HSB.jefferson_entity(r["description"]) == "Jefferson University Physicians"
    got = HSB.jefferson_decide("Jefferson Health", r["city"], r["state"], r["description"], title=r["title"])
    assert got[:3] == ("Jefferson Health", "Philadelphia", "PA")


def test_jefferson_hospital_entity_still_takes_the_hospital():
    r = _row("jefferson_tjuh_or")
    got = HSB.jefferson_decide("Jefferson Health", r["city"], r["state"], r["description"], title=r["title"])
    assert got[0] == "Thomas Jefferson University Hospital"


def test_jefferson_practice_hospitalist_role_takes_the_hospital():
    r = _row("jefferson_hospital_medicine")
    assert HSB._JEFF_PRACTICE_RX.search(HSB.jefferson_entity(r["description"]))
    got = HSB.jefferson_decide("Jefferson Health", r["city"], r["state"], r["description"], title=r["title"])
    assert got[0] == "Jefferson Methodist Hospital"


@pytest.mark.parametrize("title", ["Generalist Medical Lab Technician/Scientist (Per Diem, Days) - Einstein",
                                   "Part Time RNs - New to Practice Welcomed - All Shifts - New Jersey",
                                   "Pharmacy Informatics Manager - Full Time / Days - Center City",
                                   "Diagnostic Radiologists"])
def test_jefferson_hospital_words_are_no_practice_or_research(title):
    assert not HSB.jefferson_not_a_hospital_job(title, "")


# ── DaVita / Fresenius ──────────────────────────────────────────────────────
def test_davita_hybrid_remote_np_keeps_the_label():
    job = _job("davita_hybrid_np")
    stats = DI.apply_jobs([job])
    assert job.hospital_name == "DaVita"
    assert stats["DaVita"].get("label not one clinic") == 1


def test_davita_clinic_pct_keeps_the_clinic():
    job = _job("davita_pct")
    DI.apply_jobs([job])
    assert job.hospital_name == "DaVita Deltona Dialysis"


def test_fresenius_regional_float_keeps_the_label_and_takes_the_clinic_state():
    job = _job("fresenius_regional_float")
    DI.apply_jobs([job])
    assert job.hospital_name == "Fresenius Medical Care"
    assert job.state == "DC"


def test_fresenius_clinic_manager_keeps_the_clinic():
    job = _job("fresenius_camarillo")
    DI.apply_jobs([job])
    assert job.hospital_name == "Fresenius Kidney Care Camarillo"


@pytest.mark.parametrize("title, body, want", [
    ("Registered Nurse Regional Float", "", True),
    ("RN Case Manager Experienced (IKC-CA)", "", True),
    ("Traveler Registered Nurse", "", True),
    ("Regional Home Manager", "", True),
    ("Float Patient Care Technician", "", False),
    ("Patient Care Technician", "Float to various clinics on an as needed basis.", False),
    ("RN Virtual Hiring Event", "", False),
    ("Registered Nurse", "This is not a fully remote role.", False),
])
def test_not_one_clinic(title, body, want):
    assert DI.not_one_clinic(title, body) is want


# ── BrightSpring, UHG ───────────────────────────────────────────────────────
def test_brightspring_remote_title_keeps_the_label():
    job = _job("brightspring_remote")
    assert HH.apply_facility(job) == "remote"
    assert job.hospital_name == "BrightSpring Health Services"


def test_brightspring_branch_title_keeps_its_company():
    job = _job("brightspring_adoration")
    assert HH.apply_facility(job) == "text"
    assert job.hospital_name == "Adoration Health"


def test_uhg_field_based_title_keeps_the_label():
    r = _row("uhg_field_based")
    assert UHG.uhg_site(r["title"], r["description"]) == ("UnitedHealth Group", "remote")


def test_uhg_agency_row_keeps_its_agency():
    r = _row("uhg_conemaugh")
    assert UHG.uhg_site(r["title"], r["description"])[0] == "Conemaugh Home Health"
