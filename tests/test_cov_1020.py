# 2026-10-20 coverage stage 1: config additions on existing adapters and the
# small per-board rules they needed. No network, no database.
import asyncio

import pytest

import scraper


# -- every new config is present, once, with the shape its adapter reads -------

NEW = {
    "HEALTHCARESOURCE_ORGS": {"Bryan Health": "bryan", "Beacon Health System": "beaconhealthsystem",
                              "CharterCARE Health Partners": "chartercare",
                              "St. Bernards Healthcare": "stbernardshealthcare"},
    "PAYCOM_ORGS": {"SUN Behavioral Columbus": "35572DE1BA844B10220FCBED3F0F67E5",
                    "SUN Behavioral Kentucky": "3DDA687F40025B5B102A6B6072E43AE2",
                    "SUN Behavioral Delaware": "57BE0AE6B74A749D1F2C1B103B4DF34D",
                    "Highland District Hospital": "DE6330BB5888C63E02E5ED4D3106FEB3"},
    "PAYCOR_ORGS": {"Wilson Health": "8a7883c67db6c3c2017dbbd3ff28034f"},
    "JIBE_SITES": {"Conway Regional Health System": "https://careers.conwayregional.org"},
    "TALEMETRY_SITES": {"SGMC Health": "https://careers.sgmc.org"},
    "ICIMS_ORGS": {"Baptist Health (Birmingham)": "careers-baptisthealthal.icims.com",
                   "Northern Arizona Healthcare": "careers-nahealth.icims.com",
                   "Community Memorial Healthcare": "careers-cmhshealth.icims.com"},
}


@pytest.mark.parametrize("dict_name", sorted(NEW))
def test_new_configs_present(dict_name):
    d = getattr(scraper, dict_name)
    for system, cfg in NEW[dict_name].items():
        assert d[system] == cfg, system


def test_bryan_left_phenom_for_healthcaresource():
    assert "Bryan Health" not in scraper.PHENOM_ORGS
    assert scraper.HEALTHCARESOURCE_ORGS["Bryan Health"] == "bryan"


def test_workday_tenants_and_their_location_rules():
    wd = scraper.WORKDAY_TENANTS
    assert wd["Boston Medical Center Health System"] == ("bmc", "1", "BMC")
    assert wd["Southeast Alaska Regional Health Consortium"] == ("searhc", "5", "SEARHC")
    assert wd["Arkansas Children's"] == ("archildrens", "1", "External_Career_Site")
    assert wd["Emanate Health"] == ("cvhp", "5", "EmanateHealth")
    assert wd["Rady Children's Health"] == ("rch", "108", "Careers")
    for system in ("Boston Medical Center Health System", "Emanate Health", "Rady Children's Health",
                   "Arkansas Children's"):
        assert system in scraper.WD_TENANT_DEFAULT
    assert scraper.WD_TENANT_DEFAULT["Boston Medical Center Health System"] == ("Boston", "MA")


@pytest.mark.parametrize("system,loc,want", [
    ("Emanate Health", "Emanate Health Inter-Community Hospital - Covina",
     ("Emanate Health Inter-Community Hospital", "Covina", "CA")),
    ("Emanate Health", "Emanate Health Queen of the Valley Hospital - West Covina",
     ("Emanate Health Queen of the Valley Hospital", "West Covina", "CA")),
    ("Rady Children's Health", "Main Campus - Orange", ("Children's Hospital of Orange County", "Orange", "CA")),
    ("Rady Children's Health", "CHOC at Mission Hospital", ("Children's Hospital at Mission", "Mission Viejo", "CA")),
    ("Boston Medical Center Health System", "Brockton - 830 Oak MOB", (None, "Brockton", "MA")),
    ("Boston Medical Center Health System", "960 Massachusetts Ave", (None, "Boston", "MA")),
])
def test_workday_facility_map(system, loc, want):
    city, state = scraper.parse_city_state(loc)
    assert scraper._wd_facility(system, loc, city, state) == want


def test_searhc_state_dash_town():
    city, state = scraper.parse_city_state("AK - Juneau")
    assert state == "AK"
    assert scraper._wd_facility("Southeast Alaska Regional Health Consortium", "AK - Juneau", city, state) == \
        (None, "Juneau", "AK")


def test_arkansas_childrens_town_from_the_path():
    p = scraper._wd_path_location
    assert p("Arkansas Children's", "/job/Little-Rock/RN-II_R0024069") == ("Little Rock", "AR")
    assert p("Arkansas Children's", "/job/Springdale/Patient-Access_R0024404") == ("Springdale", "AR")


# -- Workday: a tenant that repeats pages past its total ------------------------

class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status = body, status

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, body):
        self.resp = _Resp(body)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


def _repeating_board(n_unique=100):
    posts = [{"title": f"RN {i}", "externalPath": f"/job/Orange/RN_R-{i:05d}", "locationsText": "Main Campus - Orange",
              "postedOn": "Posted Today", "bulletFields": [f"R-{i:05d}"]} for i in range(n_unique)]
    calls = {"n": 0}

    def answer(body):
        calls["n"] += 1
        off = body.get("offset", 0)
        page = posts[(off % n_unique):(off % n_unique) + 20]   # past the end, the same postings again
        return _Ctx({"total": n_unique, "jobPostings": page, "facets": []})
    return answer, calls


def _run_wd(monkeypatch, system, answer):
    def req(session, method, url, **kw):
        return answer(kw.get("json") or {})

    async def no_wait():
        return None
    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    return asyncio.run(scraper.scrape_workday(None, system, ("rch", "108", "Careers")))


def test_repeating_tenant_stops_at_the_first_page_of_repeats(monkeypatch):
    answer, calls = _repeating_board()
    jobs = _run_wd(monkeypatch, "Rady Children's Health", answer)
    assert len(jobs) == 100
    assert calls["n"] <= 10                    # 5 pages + the repeat page (+ facet/location probes), not 100+
    assert "Rady Children's Health" in scraper.COMPLETE_SYSTEMS
    assert {j.hospital_name for j in jobs} == {"Children's Hospital of Orange County"}
    assert {(j.city, j.state) for j in jobs} == {("Orange", "CA")}


def test_the_repeat_stop_is_opt_in(monkeypatch):
    monkeypatch.setattr(scraper, "WD_REPEATING_PAGES", set())
    answer, calls = _repeating_board()
    jobs = _run_wd(monkeypatch, "Rady Children's Health", answer)
    assert len(jobs) == 100
    assert calls["n"] >= 100                   # the old behaviour: paged to the 2,000-result window


# -- UKG: title program numbers and town suffixes --------------------------------

def _ukg(title, city, state, name, code="TEL1006", guid="g"):
    j = {"Id": "abc", "Title": title, "Locations": [{"LocalizedName": name,
                                                     "Address": {"City": city, "State": {"Code": state}}}]}
    return scraper._ukg_job(j, "Telecare Corporation" if code == "TEL1006" else "American Health Partners",
                            f"https://recruiting2.ultipro.com/{code}", guid)


def test_telecare_program_number_names_the_hospital_in_its_own_town():
    assert _ukg("Acute RN Supervisor (on call)  - Mental Health 169", "San Leandro", "CA",
                "Telecare Corporation").hospital_name == "Willow Rock Center"
    assert _ukg("Unit Clerk (On Call) - Mental Health 155", "Oakland", "CA",
                "Telecare Corporation").hospital_name == "Telecare Heritage Psychiatric Health Facility"
    assert _ukg("Behavioral Hospital Program Administrator - 194", "Santa Cruz", "CA",
                "Telecare Corporation").hospital_name == "Santa Cruz County Psychiatric Health Facility"


def test_telecare_number_in_another_town_or_another_number_keeps_the_company():
    assert _ukg("Maintenance Assistant - Mental Health 115", "Oakland", "CA",
                "Telecare Corporation").hospital_name == "Telecare Corporation"
    assert _ukg("Clinician - Mental Health 169", "Oakland", "CA",
                "Telecare Corporation").hospital_name == "Telecare Corporation"
    assert _ukg("Recovery Specialist $1690 bonus", "San Leandro", "CA",
                "Telecare Corporation").hospital_name == "Telecare Corporation"


def test_unity_psychiatric_care_takes_its_town():
    j = _ukg("RN", "Clarksville", "TN", "Unity Psychiatric Care", code="TEN1002THMI")
    assert j.hospital_name == "Unity Psychiatric Care - Clarksville"
    assert scraper._cms_norm(j.hospital_name) == scraper._cms_norm("UNITY PSYCHIATRIC CARE-CLARKSVILLE")
    j = _ukg("Analyst", "Franklin", "TN", "American Health Plans", code="TEN1002THMI")
    assert j.hospital_name == "American Health Plans"


def test_allegiance_boards_write_one_label():
    for k in ("Bienville", "Byrd", "Mercy Regional", "Sabine", "DeQuincy"):
        emit = f"Allegiance Health Management ({k})"
        assert emit in scraper.UKG_ORGS
        assert scraper.HOSPITAL_SYSTEM_ALIASES[emit] == "Allegiance Health Management"


def test_a_cross_listed_allegiance_posting_lands_once():
    def job(system, desc):
        return scraper.Job(title="RN", hospital_system=system, hospital_name="Dequincy Memorial", city="DeQuincy",
                           state="LA", location="DeQuincy, LA", specialty="", job_type="", url="https://x/1",
                           job_id="guid-1", posted_date="", description=desc, ats_platform="UKG")
    rows = scraper.finalize_jobs([job("Allegiance Health Management (Mercy Regional)", "short"),
                                  job("Allegiance Health Management (DeQuincy)", "a longer body " * 10)])
    assert len(rows) == 1


# -- Talemetry campus names, iCIMS places, SuccessFactors towns -------------------

def test_sgmc_campus_names():
    def e(name):
        return {"id": 1, "title": "RN", "location": {"name": name, "locality": "Valdosta", "region_abbr": "GA"}}
    j = scraper._talemetry_job
    assert j(e("Main Campus"), "SGMC Health", "https://careers.sgmc.org").hospital_name == "SGMC Health"
    assert j(e("Lanier Campus"), "SGMC Health", "https://careers.sgmc.org").hospital_name == "SGMC Health Lanier"
    assert j(e("Berrien Campus"), "SGMC Health", "https://careers.sgmc.org").hospital_name == "SGMC Berrien Campus"
    assert j(e("EMS Station One"), "SGMC Health", "https://careers.sgmc.org").hospital_name == "SGMC Health"
    # the 10-13 relabel's old label stays out of the feeds (test_name_mismatch_relabels)
    assert "South Georgia Health" not in scraper.TALEMETRY_SITES
    # a site with no map keeps the location name (Asante)
    assert j(e("Asante Ashland"), "Asante Health System", "https://jobs.asante.org").hospital_name == "Asante Ashland"


def test_icims_multi_site_location_takes_the_first_site():
    assert scraper._icims_card_location("US-AZ-Cottonwood | US-AZ-Sedona") == ("Cottonwood", "AZ")
    assert scraper._icims_card_location("US-AZ-Flagstaff") == ("Flagstaff", "AZ")


def _icims_card(loc):
    return ('<li class="iCIMS_JobCardItem"><a href="https://careers-x.icims.com/jobs/1234/rn/job?in_iframe=1">'
            '<h3>RN</h3></a><dl class="iCIMS_JobHeaderGroup"><dt class="iCIMS_JobHeaderField">Location</dt>'
            f'<dd class="iCIMS_JobHeaderData">{loc}</dd></dl></li>')


@pytest.mark.parametrize("system,loc,want", [
    ("Baptist Health (Birmingham)", "Shelby Medical Center, AL", ("Shelby Medical Center", "Alabaster", "AL")),
    ("Baptist Health (Birmingham)", "Walker Medical Center, AL", ("Walker Medical Center", "Jasper", "AL")),
    ("Community Memorial Healthcare", "147 N. Brent Street, CA",
     ("Community Memorial Hospital - Ventura", "Ventura", "CA")),
    ("Community Memorial Healthcare", "1306 Maricopa Highway, CA",
     ("Community Memorial Hospital - Ojai", "Ojai", "CA")),
])
def test_icims_location_places(system, loc, want):
    jobs = scraper._parse_icims_cards(_icims_card(loc), system, "careers-x.icims.com")
    assert len(jobs) == 1
    assert (jobs[0].hospital_name, jobs[0].city, jobs[0].state) == want


def test_menonita_business_unit_names_hospital_and_town():
    p = {"id": "77", "title": "ENFERMERA", "otherValues": [[{"fieldId": "businessUnit_obj",
                                                             "shortVal": "Hospital Menonita Cayey"}]]}
    j = scraper._sf_job(p, "Sistema de Salud Menonita", "https://career8.successfactors.com", "mennoniteg", "PR",
                        "businessUnit_obj")
    assert (j.hospital_name, j.city, j.state) == ("Hospital Menonita Cayey", "Cayey", "PR")


# -- HTML list: Baptist Jacksonville ----------------------------------------------

def test_baptist_jacksonville_cards(fixture_text):
    cfg = scraper.HTML_LIST_SITES["Baptist Health Jacksonville"]
    jobs = scraper._parse_html_list_page(fixture_text("baptistjax_list.html"), "Baptist Health Jacksonville", cfg)
    got = {j.job_id: (j.hospital_name, j.city, j.state) for j in jobs}
    assert got["807362"] == ("Baptist Medical Center Beaches", "Jacksonville Beach", "FL")
    assert got["804597"] == ("Baptist Medical Center Nassau", "Fernandina Beach", "FL")
    assert got["808369"] == ("Baptist IT Center", "Jacksonville", "FL")
    assert got["808502"] == ("Remote - Florida", "", "FL")          # no town is guessed
    assert all(j.url.startswith("https://www.baptistjax.com/about-us/careers/job-listings/") for j in jobs)
    assert {j.job_type for j in jobs} <= {"Full time", "Part time", "PRN", "Per diem", "Temporary"}


def test_pih_single_state_default():
    d = scraper.normalize_job(scraper.Job(title="RN", hospital_system="PIH Health", hospital_name="PIH Health",
                                          city="Downey", state="", location="Downey", specialty="", job_type="",
                                          url="https://jobs.pihhealth.org/job/downey/rn/912/1", job_id="1",
                                          posted_date="", description="", ats_platform="TalentBrew"))
    assert (d["city"], d["state"]) == ("Downey", "CA")
