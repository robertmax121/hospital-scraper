"""cov3/workday-oracle-infor (2026-10-04, coverage round 3): tenant configs for
AHRQ systems with no rows (Workday, Oracle HCM, Infor CloudSuite, Paycom) and
the scraper-side relabels (Humboldt Park Health, Catawba Valley, Bryan Health,
Hoag vs Huntsville). Offline: config shape, the location tables and the pure
row builders; the live totals in the comments were read the same day."""
import inspect
import json
import re

import scraper
from scraper import _wd_facility

WORKDAY = {
    "North Mississippi Medical":      ("nmhs", "108", "NMHS", "MS"),
    "Memorial Health System":         ("memorialhealth", "108", "Memorial_Health_External_Career_Site", "IL"),
    "Valley Health (VA)":             ("valleyhealthlink", "115", "valleyhealthcareers", "VA"),
    "Brown University Health":        ("brownhealth", "12", "External_Careers", "RI"),
    "Keck Medicine of USC":           ("usc", "5", "ExternalUSCCareers", "CA"),
    "Mercyhealth":                    ("mercyhealth", "1", "mercyhealthcareers", "WI"),
    "Benefis Health System":          ("benefis", "1", "BHS", "MT"),
    "Concord Hospital":               ("crhc", "1", "Concord_Careers", "NH"),
    "Nationwide Children's Hospital": ("nationwidechildrens", "5", "NCHCareers", "OH"),
    "SolutionHealth":                 ("solutionhealth", "1", "Careers", "NH"),
    "ChristianaCare":                 ("christianacare", "5", "CCHS", "DE"),
}


def test_workday_tenants_sites_and_default_states():
    for label, (tenant, wd, site, st) in WORKDAY.items():
        assert scraper.WORKDAY_TENANTS[label] == (tenant, wd, site), label
        assert scraper.WD_TENANT_DEFAULT[label][1] == st, label
        assert label not in scraper.HOSPITAL_SYSTEM_ALIASES, label
    # one board per label across the platforms (the push5 rule)
    boards = [scraper.PHENOM_ORGS, scraper.JIBE_SITES, scraper.ICIMS_ORGS, scraper.ORACLE_ORGS,
              scraper.INFOR_ORGS, scraper.UKG_ORGS, scraper.TALEMETRY_SITES, scraper.HCTS_PORTALS]
    for label in WORKDAY:
        assert not any(label in b for b in boards), label


def test_keck_is_cut_to_its_hospital_locations():
    f = scraper.WD_TENANT_FACETS["Keck Medicine of USC"]
    assert set(f) == {"locations"} and len(f["locations"]) == 5
    assert "e4488bbdc40210fcf92084f5fba8f5a6" in f["locations"]   # Health Sciences Campus
    assert "e4488bbdc40210fcf920c28abbb7f5c0" in f["locations"]   # Glendale (Verdugo Hills)
    assert all(re.fullmatch(r"[0-9a-f]{32}", i) for i in f["locations"])
    assert _wd_facility("Keck Medicine of USC", "Glendale, CA", "Glendale", "CA") == ("USC Verdugo Hills Hospital", "Glendale", "CA")
    assert _wd_facility("Keck Medicine of USC", "ARH Arcadia Hospital", "ARH Arcadia Hospital", "") == ("USC Arcadia Hospital", "Arcadia", "CA")
    # the Health Sciences Campus parses on its own and keeps the system name
    assert _wd_facility("Keck Medicine of USC", "Los Angeles, CA - Health Sciences Campus", "Los Angeles", "CA") == (None, "Los Angeles", "CA")


def test_town_facility_locations_take_the_map_town():
    """NMHS writes "Tupelo - North MS Medical Center"; parse_city_state hands the
    facility half back as the city, and that fragment must not survive as a town."""
    city, st = scraper.parse_city_state("Tupelo - North MS Medical Center")
    assert (city, st) == ("North MS Medical Center", "")
    assert _wd_facility("North Mississippi Medical", "Tupelo - North MS Medical Center", city, st) == \
        ("North Mississippi Medical Center", "Tupelo", "MS")
    assert _wd_facility("North Mississippi Medical", "Hamilton, AL - Marion Regional Medical Center",
                        *scraper.parse_city_state("Hamilton, AL - Marion Regional Medical Center")) == \
        ("Marion Regional Medical Center", "Hamilton", "AL")
    assert _wd_facility("North Mississippi Medical", "South Marion Winfield", "South Marion Winfield", "") == (None, "Winfield", "AL")
    assert _wd_facility("North Mississippi Medical", "Tupelo - Pathology", "Pathology", "") == (None, "Tupelo", "MS")
    # a parsed "City, ST" still wins over the map (the existing rule)
    assert _wd_facility("Mercyhealth", "Javon Bea Hospital - Rockford, IL", "Rockford", "IL") == ("Javon Bea Hospital", "Rockford", "IL")
    assert _wd_facility("Mercyhealth", "Javon Bea Hospital - Riverside", "Riverside", "") == ("Javon Bea Hospital", "Rockford", "IL")
    assert _wd_facility("Mercyhealth", "Mercyhealth Hospital and Physician Clinic - Crystal Lake", "Crystal Lake", "") == \
        ("Mercyhealth Hospital and Physician Clinic - Crystal Lake", "Crystal Lake", "IL")
    # the pre-cov3 behaviour for whole-string echoes is unchanged
    assert _wd_facility("Baptist Health (Alabama)", "Prattville Baptist Hospital", "Prattville Baptist Hospital", "") == \
        ("Prattville Baptist Hospital", "Prattville", "AL")


def test_facility_maps_name_the_cms_hospitals():
    cases = [
        ("Memorial Health System", "Decatur Memorial Hospital", ("Decatur Memorial Hospital", "Decatur", "IL")),
        ("Memorial Health System", "MMC", ("Springfield Memorial Hospital", "Springfield", "IL")),
        ("Memorial Health System", "Lincoln Memorial Hospital", ("Abraham Lincoln Memorial Hospital", "Lincoln", "IL")),
        ("Brown University Health", "Rhode Island Hospital", ("Rhode Island Hospital", "Providence", "RI")),
        ("Brown University Health", "Saint Annes Hospital", ("Saint Anne's Hospital", "Fall River", "MA")),
        ("Brown University Health", "Morton Hospital", ("Morton Hospital", "Taunton", "MA")),
        ("SolutionHealth", "Manchester - Elliot Hospital", ("Elliot Hospital", "Manchester", "NH")),
        ("SolutionHealth", "Manchester - 130 Tarrytown Road", (None, "Manchester", "NH")),
        ("ChristianaCare", "Christiana Hospital", ("Christiana Hospital", "Newark", "DE")),
        ("ChristianaCare", "Union Hospital", ("Union Hospital of Cecil County", "Elkton", "MD")),
        ("ChristianaCare", "Elkton Maryland", (None, "Elkton", "MD")),
    ]
    for system, loc, want in cases:
        city, st = scraper.parse_city_state(loc)
        assert _wd_facility(system, loc, city, st) == want, (system, loc)
    # a facility-only row with no map hit still takes the tenant's home market
    class J:
        city = ""; state = ""
    j = J()
    scraper._wd_apply_locations([j], {}, {}, {}, scraper.WD_TENANT_DEFAULT["Brown University Health"])
    assert (j.city, j.state) == ("Providence", "RI")


def test_oracle_sites_and_home_markets():
    for label, host, site, st in (("Mosaic Life Care", "ibcjqy.fa.ocs", "jobsmymlc", "MO"),
                                  ("UChicago Medicine", "fa-etnf-saasfaprod1", "CX_1001", "IL"),
                                  ("Inspira Health Network", "ernh.fa.us2", "CX_1", "NJ")):
        base, s = scraper.ORACLE_ORGS[label]
        assert base.startswith("https://") and host in base and s == site, label
        assert scraper.SYSTEM_LOCATION_DEFAULTS[label.lower()][1] == st, label


def test_catawba_valley_relabel():
    assert "Valley Health (NV)" not in scraper.ORACLE_ORGS
    assert scraper.ORACLE_ORGS["Catawba Valley Health System"] == ("https://fa-eveq-saasfaprod1.fa.ocs.oraclecloud.com", "CX_1")
    assert scraper.SYSTEM_LOCATION_DEFAULTS["catawba valley health system"] == ("Hickory", "NC")
    assert "Valley Health (VA)" in scraper.WORKDAY_TENANTS


def test_infor_boards():
    assert scraper.INFOR_ORGS["Hawaii Pacific Health"] == ("css-jdzyl6hzmy2pe28d-prd", "10", "EXTERNAL", "HI")
    assert scraper.INFOR_ORGS["Kaleida Health"] == ("css-y9x4ku9mqsygapwx-prd", "1000", "KH-EXTERNAL", "NY")
    assert scraper.INFOR_ORGS["Salem Health"] == ("css-salemhealth-prd", "1", "STAFF_EXTERNAL", "OR")
    assert scraper.INFOR_ORGS["Bellin Health"] == ("css-bellin-prd", "500", "EXTERNAL", "WI")


def _infor_payload(rows):
    return {"dataViewSet": {"data": rows, "pagingInfo": {"hasNext": False}, "pagingUrls": {}}, "status": "COMPLETED"}


def _infor_row(rid, title, sort_loc, loj=""):
    return {"resourceId": f"JobPosting[JobPostingSet](1,{rid},1)",
            "fields": {"Description": {"value": title}, "LocationOfJob": {"value": loj},
                       "LocationOfJobDescriptionForSort": {"value": sort_loc}, "WorkType": {"value": "FT"},
                       "JobRequisition": {"value": rid}, "JobPosting": {"value": 1},
                       "PostingDateRange_prd_Begin": {"value": "20261003"}}}


def test_infor_rows_for_the_new_boards():
    """Rows through the adapter's documented location shapes ("City, ST", a
    "Facility:City:ST" colon value, a blank value taking the tenant default);
    Hawaii Pacific names the hospital ("Wilcox Medical Center, Lihue, HI"), the
    shape its live rows carried on 2026-10-04."""
    rows = scraper._infor_rows(_infor_payload([_infor_row(101, "Registered Nurse", "Buffalo, NY"),
                                               _infor_row(102, "Registered Nurse", "", "Olean General Hospital:Olean:NY"),
                                               _infor_row(103, "Nurse Practitioner", "Bradford, PA")]))
    jobs, _ = scraper._infor_jobs("Kaleida Health", scraper.INFOR_ORGS["Kaleida Health"], rows)
    by = {j.job_id: j for j in jobs}
    assert len(by) == 3
    assert (by["101"].city, by["101"].state) == ("Buffalo", "NY")
    assert (by["103"].city, by["103"].state) == ("Bradford", "PA")
    assert by["101"].url.endswith("?csk.HROrganization=1000&csk.JobBoard=KH-EXTERNAL")
    rows = scraper._infor_rows(_infor_payload([_infor_row(7, "RN - Night Shift | Oconto Emergency Department", "Oconto, WI"),
                                               _infor_row(8, "Service Associate", "")]))
    jobs, _ = scraper._infor_jobs("Bellin Health", scraper.INFOR_ORGS["Bellin Health"], rows)
    by = {j.job_id: j for j in jobs}
    assert (by["7"].city, by["7"].state) == ("Oconto", "WI")
    assert by["8"].state == "WI"                       # tenant default for a blank location
    rows = scraper._infor_rows(_infor_payload([_infor_row(5, "Security Officer", "Wilcox Medical Center, Lihue, HI")]))
    jobs, _ = scraper._infor_jobs("Hawaii Pacific Health", scraper.INFOR_ORGS["Hawaii Pacific Health"], rows)
    assert (jobs[0].city, jobs[0].state) == ("Lihue", "HI")
    assert "Wilcox" in jobs[0].hospital_name


def test_paycom_memorial_health_system_ohio():
    assert scraper.PAYCOM_ORGS["Memorial Health System"] == "D9A1F2007E6D793704B1C60B560F717C"
    assert scraper.PAYCOM_DEFAULT_STATE["Memorial Health System"] == "OH"
    key = scraper.PAYCOM_ORGS["Memorial Health System"]
    st = scraper.PAYCOM_DEFAULT_STATE["Memorial Health System"]
    j = scraper._paycom_job({"jobId": "41", "jobTitle": "RN - ICU", "locations": "Marietta, OH 45750"}, None,
                            "Memorial Health System", key, st)
    assert (j.city, j.state, j.hospital_system) == ("Marietta", "OH", "Memorial Health System")
    j = scraper._paycom_job({"jobId": "42", "jobTitle": "Phlebotomist", "locations": "Parkersburg, WV 26101"}, None,
                            "Memorial Health System", key, st)
    assert (j.city, j.state) == ("Parkersburg", "WV")
    j = scraper._paycom_job({"jobId": "43", "jobTitle": "Cook"}, None, "Memorial Health System", key, st)
    assert j.state == "OH"
    # the label's two boards: Springfield IL on Workday, Marietta OH on Paycom; the FL
    # rows under it come from the "Memorial Healthcare System" Workday tenant via the alias
    assert scraper.WORKDAY_TENANTS["Memorial Health System"][0] == "memorialhealth"
    assert scraper.WORKDAY_TENANTS["Memorial Healthcare System"][0] == "memorialhealthcare"
    assert scraper.HOSPITAL_SYSTEM_ALIASES["Memorial Healthcare System"] == "Memorial Health System"
    assert "memorial health system" not in scraper.SYSTEM_LOCATION_DEFAULTS   # the Savannah GA default is gone


def test_humboldt_park_relabel_and_nmhs_label_moves_to_workday():
    assert "North Mississippi Medical" not in scraper.UKG_ORGS
    base, guid, st = scraper.UKG_ORGS["Humboldt Park Health"]
    assert base.endswith("/NOR1041NAHO") and guid == "84528182-2cf7-4f42-b7ca-dbb54c6f1c10" and st == "IL"
    assert scraper.WORKDAY_TENANTS["North Mississippi Medical"][0] == "nmhs"


def test_bryan_health_default_state():
    assert scraper.PHENOM_ORGS["Bryan Health"] == "https://careers.bryanhealth.com"
    assert scraper.SYSTEM_LOCATION_DEFAULTS["bryan health"] == ("Lincoln", "NE")


def test_hoag_huntsville_mix_up_resolved():
    assert "Hoag Health" not in scraper.PHENOM_ORGS
    assert not any("hhsys.org" in v for v in scraper.PHENOM_ORGS.values())
    assert scraper.ICIMS_ORGS["Huntsville Hospital Health System"] == "careers-hhsys.icims.com"
    assert "Huntsville Hospital Health System" not in scraper.PHENOM_ORGS


def test_wd_facility_source_keeps_the_fragment_rule():
    src = inspect.getsource(scraper._wd_facility)
    assert "echo in full" in src
