# 2026-10-25 coverage stage 2: config lines on existing adapters for systems
# whose CMS hospitals were uncovered, and the small location / name rules the
# dry runs showed they need. No network, no database.
import pytest

import scraper


# -- every new config is present, once, with the shape its adapter reads -------

NEW = {
    "WORKDAY_TENANTS": {
        "Mount Sinai Medical Center (Florida)": ("msmc", "12", "msmc_careers"),
        "Valley Children's Healthcare": ("valleychildrens", "12", "VCH_External"),
        "Children's National Hospital": ("childrensnational", "108", "CN_Careers"),
        "Overlake Medical Center": ("overlakemedicalcenter", "5", "Overlake_External_Career_Site"),
        "GBMC HealthCare": ("gbmc", "1", "gbmc"),
        "University Health (Kansas City)": ("trumed", "1", "TMC_External_Career_Site"),
        "Holland Hospital": ("hollandhospital", "1", "External"),
        "Olympic Medical Center": ("olympicmedical", "5", "OMC_Careers"),
        "Boys Town": ("boystown", "1", "boystowncareers"),
        "East Tennessee Children's Hospital": ("etch", "5", "ETCH"),
        "OU Health": ("oumedicine", "5", "OUHealthCareers"),
        "Children's Wisconsin": ("cwi", "1", "CW_Careers"),
        "Singing River Health System": ("mysrhs", "108", "SRHS"),
        "Genesis HealthCare System": ("genesishcs", "1", "GenesisHCS"),
        "Eisenhower Health": ("eisenhower", "1", "Eisenhower_Careers"),
        "Reid Health": ("reidhealth", "12", "External_Careers"),
        "Dayton Children's": ("daytonchildrens", "1", "dayton_childrens_career_site"),
    },
    "ICIMS_ORGS": {
        "Cooper University Health Care": "careers-cooperhealth.icims.com",
        "VHC Health": "careers-vhchealth.icims.com",
        "San Antonio Regional Hospital": "careers-sarh.icims.com",
        "Foundation Health Partners": "careers-fhp.icims.com",
        "Heywood Healthcare": "careers-heywood.icims.com",
        "Henry Community Health": "careers-hcmhcares.icims.com",
    },
    "JIBE_SITES": {
        "Riverside Healthcare (Kankakee)": "https://careers.riversidehealthcare.org",
        "FirstHealth of the Carolinas": "https://talent.firsthealth.org",
        "Covenant HealthCare": "https://careers.covenanthealthcare.com",
    },
    "HEALTHCARESOURCE_ORGS": {
        "Pomona Valley Hospital Medical Center": "pvhmc", "Valleywise Health": "maricopa",
        "St. Joseph's/Candler": "sjchs", "TidalHealth": "peninsula", "Sinai Chicago": "sinai",
        "Rutland Regional Medical Center": "rrmc", "Goshen Health": "goshen",
        "Auburn Community Hospital": "auburnhospital", "South County Health": "southcounty",
        "CalvertHealth": "cmh", "Campbell County Health": "cchwyo", "Major Health Partners": "major",
        "National Jewish Health": "national",
    },
    "UKG_ORGS": {
        "Frederick Health": ("https://recruiting.ultipro.com/FRE1010FRHS", "776a147e-2e0b-43d0-844e-bbd49deeb24d", "MD"),
        "Southern Ohio Medical Center": ("https://mysomc.rec.pro.ukg.net/SOU1068SOMC", "1735eb8a-40b2-43d2-89aa-99eea075dc4f", "OH"),
        "Southern Ohio Medical Center (Providers)": ("https://mysomc.rec.pro.ukg.net/SOU1068SOMC", "b22a8c81-102d-44ef-8953-701bcd0e54d4", "OH"),
        "Firelands Health": ("https://recruiting.ultipro.com/FIR1028FRMC", "aa965d38-1509-43d7-9a00-c84a96ce1cb9", "OH"),
        "Episcopal Health Services": ("https://myehs.rec.pro.ukg.net/EPI1010SJEH", "16d4fe52-3a0d-4b07-8565-5038c46b9f53", "NY"),
        "Barton Health": ("https://recruiting2.ultipro.com/BAR1023BTHS", "22edf9a1-6621-4bec-9730-f5384a308c49", "CA"),
        "Witham Health Services": ("https://witham.rec.pro.ukg.net/WIT1500WHMH", "47727cde-3d32-4129-bef1-ef796414651d", "IN"),
        "Island Health": ("https://recruiting2.ultipro.com/ISL1001ISLAN", "e5b91c51-cf06-445e-9aed-84859d230b57", "WA"),
        "Perry County Memorial Hospital": ("https://perrycountymem.rec.pro.ukg.net/PER1500PCMH", "ad3e78fe-7ca1-44dd-9add-84fd268d95b4", "IN"),
        "Tahoe Forest Health System": ("https://recruiting.ultipro.com/TAH1000", "19fd0f5d-3c9e-10c7-8338-910ff12f7559", "CA"),
    },
    "ORACLE_ORGS": {
        "Hurley Medical Center": ("https://fa-evlb-saasfaprod1.fa.ocs.oraclecloud.com", "CX_1"),
        "Waterbury Health": ("https://icjdjb.fa.ocs.oraclecloud.com", "CX_1"),
        "Shore Medical Center": ("https://eiqu.fa.us6.oraclecloud.com", "CX_1"),
        "Children's Nebraska": ("https://egnx.fa.us2.oraclecloud.com", "CX"),
        "Stamford Health": ("https://fa-ewfb-saasfaprod1.fa.ocs.oraclecloud.com", "Careers"),
        "Hoag": ("https://iaucqy.fa.ocs.oraclecloud.com", "CX_1"),
        "AtlantiCare": ("https://ibywjb.fa.ocs.oraclecloud.com", "CX_1"),
    },
    "PHENOM_ORGS": {
        "Lurie Children's": "https://jobs.luriechildrens.org",
        "San Juan Regional Medical Center": "https://careers.sanjuanregional.com",
        "Cheyenne Regional Medical Center": "https://careers.cheyenneregional.org",
        "Cincinnati Children's": "https://jobs.cincinnatichildrens.org",
        "Children's Hospital of Philadelphia": "https://careers.chop.edu",
        "Grady Health System": "https://careers.gradyhealth.org",
        "Seattle Children's": "https://careers.seattlechildrens.org",
    },
    "PAYCOR_ORGS": {"Day Kimball Health": "4028f88b24c330a20124eb33b4ad1653"},
    "INFOR_ORGS": {
        "Akron Children's": ("css-akronchildrens-prd", "1", "EXTERNAL", "OH"),
        "Cambridge Health Alliance": ("css-cambridgehealth-prd", "1000", "EXTERNAL", "MA"),
        "Children's of Alabama": ("css-childrensal-prd", "10", "EXTERNAL", "AL"),
    },
    "TALENTBREW_ORGS": {"Rady Children's Hospital San Diego": ("https://jobs.rchsd.org/search-jobs", 15)},
    "TALEMETRY_SITES": {"EvergreenHealth": "https://careers.evergreenhealth.com"},
    # single-hospital boards from the 10-20 platform pass, re-read 2026-10-25
    "KRONOS_ORGS": {
        "Washington County Hospital and Clinics": ("secure7.saashr.com", "6217537", "IA"),
        "Sharon Regional Medical Center": ("secure7.saashr.com", "6212795", "PA"),
    },
    "ADP_ORGS": {
        "St. Luke Hospital & Living Center": ("58e14ddc-10c8-4aac-a782-4d9fab68932f", "19000101_000001", "KS",
                                              "St. Luke Hospital & Living Center"),
        "Bingham Healthcare": ("1b6bb77f-2903-4807-b0c8-4bfcb7c78404", "19000101_000001", "ID", ""),
    },
    "PAYLOCITY_ORGS": {
        "Penobscot Valley Hospital": ("f677b95e-47db-4c39-b99b-29e5e27ab84c", "Penobscot-Valley-Hospital", "ME"),
        "Munising Memorial Hospital": ("191146fb-2615-4deb-abf9-587139fc7a3f", "Munising-Memorial-Hospital-Association", "MI"),
    },
    "PAYCOM_ORGS": {"AVEM Health Partners": "1C1875CBF90AD0C6A2E08F2A4B01A84C"},
    "APPLICANTPRO_ORGS": {"Southwest Health System": ("shs", ""), "Montrose Regional Health": ("montrosehospital", "")},
}
NEW["PAYCOR_ORGS"].update({"Arkansas Valley Regional Medical Center": "8a7883d09d9307dd019db0b264160476",
                           "Bothwell Regional Health Center": "8a7883c6946708d70194a902a07514b4"})
NEW["UKG_ORGS"]["Memorial Regional Health"] = ("https://recruiting2.ultipro.com/MEM1005MHCR",
                                               "3057c08c-d82e-49ff-aee0-6146eb32163f", "CO")


@pytest.mark.parametrize("dict_name", sorted(NEW))
def test_new_configs_present(dict_name):
    d = getattr(scraper, dict_name)
    for system, cfg in NEW[dict_name].items():
        assert d[system] == cfg, system


@pytest.mark.parametrize("dict_name", sorted(NEW))
def test_no_tenant_is_configured_twice(dict_name):
    d = getattr(scraper, dict_name)
    for system, cfg in NEW[dict_name].items():
        same = [s for s, c in d.items() if c == cfg]
        assert same == [system], (system, same)


def test_new_labels_are_new():
    # A stage-2 label must not be an existing label of another adapter (it
    # would merge two employers) nor be rewritten by HOSPITAL_SYSTEM_ALIASES.
    seen = {}
    for dict_name, entries in NEW.items():
        for system in entries:
            assert system not in seen, (system, seen.get(system), dict_name)
            seen[system] = dict_name
            if system != "Southern Ohio Medical Center (Providers)":
                assert system not in scraper.HOSPITAL_SYSTEM_ALIASES, system
    assert scraper.HOSPITAL_SYSTEM_ALIASES["Southern Ohio Medical Center (Providers)"] == "Southern Ohio Medical Center"


def test_rejected_candidates_stay_out():
    # The University of Tennessee's Oracle site (fa-ewlq) lists the university
    # (Memphis, Knoxville campus, Chattanooga, Martin), not UT Medical Center;
    # Children's of Alabama's HealthcareSource tenant answers 0 jobs; Holy Name's
    # Jobvite pages gave no bodies; Island Health's second board is a subset.
    assert "University of Tennessee Medical Center" not in scraper.ORACLE_ORGS
    assert all("fa-ewlq" not in c[0] for c in scraper.ORACLE_ORGS.values())
    assert "chsys" not in scraper.HEALTHCARESOURCE_ORGS.values()
    assert "Holy Name Medical Center" not in scraper.TALEMETRY_SITES
    assert all("cdaaeaad" not in str(c) for c in scraper.UKG_ORGS.values())


# -- Workday towns and hospital names ---------------------------------------------

def test_workday_tenant_defaults():
    td = scraper.WD_TENANT_DEFAULT
    for system in NEW["WORKDAY_TENANTS"]:
        if system == "Boys Town":      # multi-state youth-care sites: no default town
            assert system not in td
            continue
        assert system in td, system
    assert td["GBMC HealthCare"] == ("Baltimore", "MD")
    assert td["OU Health"] == ("Oklahoma City", "OK")
    assert td["Genesis HealthCare System"] == ("Zanesville", "OH")


@pytest.mark.parametrize("system,loc,want", [
    ("GBMC HealthCare", "GBMC Hospital", ("Greater Baltimore Medical Center", "Baltimore", "MD")),
    ("GBMC HealthCare", "Gilchrist Hunt Valley", (None, "Hunt Valley", "MD")),
    ("University Health (Kansas City)", "University Health Truman Medical Center",
     ("University Health Truman Medical Center", "Kansas City", "MO")),
    ("University Health (Kansas City)", "UH Lakewood Medical Center",
     ("University Health Lakewood Medical Center", "Kansas City", "MO")),
    ("University Health (Kansas City)", "University Health 1 (UH1)", (None, "Kansas City", "MO")),
    ("Children's National Hospital", "Children's National Hospital (Main Campus)",
     ("Children's National Hospital", "Washington", "DC")),
    ("Eisenhower Health", "Eisenhower Medical Center", ("Eisenhower Medical Center", "Rancho Mirage", "CA")),
    ("Dayton Children's", "Dayton Children's - Main Campus", ("Dayton Children's Hospital", "Dayton", "OH")),
    ("Dayton Children's", "Dayton Children's - South Campus", (None, "Springboro", "OH")),
    ("OU Health", "Yukon Facility", (None, "Yukon", "OK")),
    ("Overlake Medical Center", "Lynnwood ED", (None, "Lynnwood", "WA")),
])
def test_workday_facility_map(system, loc, want):
    city, state = scraper.parse_city_state(loc)
    assert scraper._wd_facility(system, loc, city, state) == want


@pytest.mark.parametrize("loc,name,town,st", [
    ("Peak View Behavioral Health", "Peak View Behavioral Health", "Colorado Springs", "CO"),
    ("Eagle View Behavioral Health", "Eagle View Behavioral Health", "Bettendorf", "IA"),
    ("Raleigh Oaks Behavioral Health", "Raleigh Oaks Behavioral Health", "Garner", "NC"),
    ("Peak Behavioral Health", "Peak Behavioral Health", "Santa Teresa", "NM"),
    ("Creekside Behavioral Health", "Creekside Behavioral Health", "Kingsport", "TN"),
    ("Miramont Behavioral Health", "Miramont Behavioral Health", "Middleton", "WI"),
    ("Willow Creek Behavioral Health", "Willow Creek Behavioral Health", "Green Bay", "WI"),
    ("Highland Hospital", "Highland Hospital", "Charleston", "WV"),
    ("The Pavilion at Williamsburg Place", "The Pavilion at Williamsburg Place", "Williamsburg", "VA"),
])
def test_summit_bhc_cms_hospitals_take_their_own_names(loc, name, town, st):
    city, state = scraper.parse_city_state(loc)
    assert scraper._wd_facility("Summit BHC", loc, city, state) == (name, town, st)


@pytest.mark.parametrize("loc,town,st", [
    ("The Farley Center", "Williamsburg", "VA"),
    ("Las Cruces Recovery Center", "Las Cruces", "NM"),
    ("Summit Healthcare Mgmt", "Franklin", "TN"),
])
def test_summit_bhc_other_sites_keep_the_system_name(loc, town, st):
    city, state = scraper.parse_city_state(loc)
    assert scraper._wd_facility("Summit BHC", loc, city, state) == (None, town, st)


def test_summit_names_match_their_cms_rows():
    # by_exact / by_contain in refresh_cms_coverage_snapshot compare these
    # normalised names inside the state.
    norm = scraper._cms_norm
    assert norm("Peak View Behavioral Health") == norm("PEAK VIEW BEHAVIORAL HEALTH")
    assert norm("Highland Hospital") == norm("HIGHLAND HOSPITAL")
    assert norm("The Pavilion at Williamsburg Place") == norm("THE PAVILION AT WILLIAMSBURG PLACE")
    assert norm("Peak Behavioral Health") in norm("PEAK BEHAVIORAL HEALTH SERVICES, LLC")
    assert norm("Willow Creek Behavioral Health") in norm("SBH Green Bay, LLC d/b/a WILLOW CREEK BEHAVIORAL HEALTH")


# -- campus labels and neighbourhood towns (normalize_job) -------------------------

def _row(system, name, city, state, platform="UKG"):
    return scraper.normalize_job(scraper.Job(
        title="RN", hospital_system=system, hospital_name=name, city=city, state=state,
        location=f"{city}, {state}", specialty="", job_type="", url="https://x/1", job_id="1",
        posted_date="", description="", ats_platform=platform))


@pytest.mark.parametrize("system,raw,want", [
    ("Frederick Health", "Main Campus", "Frederick Health Hospital"),
    ("Firelands Health", "Main Campus", "Firelands Regional Medical Center"),
    ("Firelands Health", "Bellevue Hospital", "The Bellevue Hospital"),
    ("Episcopal Health Services", "Hospital", "St. John's Episcopal Hospital"),
    ("Barton Health", "Barton Hospital", "Barton Memorial Hospital"),
    ("Island Health", "Island Hospital-Washingto", "Island Hospital"),
    ("Tahoe Forest Health System", "* Truckee", "Tahoe Forest Hospital"),
    ("Tahoe Forest Health System", "IVCH", "Incline Village Community Hospital"),
    ("Southern Ohio Medical Center", "SOMC", "Southern Ohio Medical Center"),
    ("Southern Ohio Medical Center (Providers)", "Southern Ohio Medical Center (Providers)",
     "Southern Ohio Medical Center"),
    ("Pomona Valley Hospital Medical Center", "PVHMC incl. Offsite Facilities", "Pomona Valley Hospital Medical Center"),
    ("National Jewish Health", "*National Jewish Health - Main Campus", "National Jewish Health"),
    ("Grady Health System", "Main Grady Campus (Downtown Atlanta)", "Grady Memorial Hospital"),
])
def test_campus_labels_take_the_hospital_name(system, raw, want):
    assert _row(system, raw, "Somewhere", "OH")["hospital_name"] == want


def test_campus_label_fix_is_scoped_to_its_system():
    # "Main Campus" on any other board is left alone.
    assert _row("Some Other Health", "Main Campus", "Akron", "OH")["hospital_name"] == "Main Campus"
    assert _row("Frederick Health", "Rose Hill", "Frederick", "MD")["hospital_name"] == "Rose Hill"


def test_lurie_neighbourhoods_are_chicago():
    for town in ("Streeterville", "River North", "Lincoln Park", "Old Town"):
        d = _row("Lurie Children's", "Lurie Children's", town, "IL", "Phenom")
        assert (d["city"], d["state"]) == ("Chicago", "IL"), town
    d = _row("Lurie Children's", "Lurie Children's", "Skokie", "IL", "Phenom")
    assert d["city"] == "Skokie"
    # another system's "Lincoln Park" (a town in MI and NJ) is not touched
    assert _row("Some Other Health", "X Hospital", "Lincoln Park", "MI")["city"] == "Lincoln Park"


def test_evergreen_location_names_take_the_system_name():
    def e(name):
        return {"id": 1, "title": "RN", "location": {"name": name, "locality": name.split(",")[0],
                                                     "region_abbr": "WA"}}
    j = scraper._talemetry_job
    assert j(e("Kirkland, Washington"), "EvergreenHealth", "https://careers.evergreenhealth.com").hospital_name == \
        "EvergreenHealth"
    assert j(e("Monroe, Washington"), "EvergreenHealth", "https://careers.evergreenhealth.com").hospital_name == \
        "EvergreenHealth"


def test_avem_board_defaults_to_oklahoma():
    assert scraper.PAYCOM_DEFAULT_STATE["AVEM Health Partners"] == "OK"
