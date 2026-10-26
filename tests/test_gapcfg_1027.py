# 2026-10-27 RN gap, scraper builder 1 ("configs"): config lines on adapters
# we already run for non-acute employers with no rows (home health, hospice,
# skilled nursing, surgery centers, dialysis, corrections health, payers,
# rehab hospitals and LTACHs), the two dead entries they replace, Fresenius
# Medical Care's US cut and state slices, and the HealthcareSource
# hcshiring.com variant for Life Care Centers of America. No network, no
# database: adapters run against fakes of scraper.req.
#
# Paced no-write dry runs on 10-26 (rows / RN rows by the site's RN rule /
# rows with town and state; detail passes were capped at a sample):
#   CenterWell 1,806 / 331 / 1,762      CVS Health 1,958 / 136 / (state-first rule added after)
#   Elevance Health 112 / 47 / 79       Centene 174 / 60 / 0 (remote, state only)
#   Humana 90 / 25 / 38                 Cigna 133 / 38 / 111
#   Fresenius (TN, CO, NM only) 173 / 66 / 146
#   Genesis HealthCare 3,793 / 474 / 3,673   SCA Health 708 / 310 / 678
#   Vibra Healthcare 308 / 77 / 308     Ernest Health 561 / 84 / 561
#   PAM Health 1,558 / 290 / 1,557      USPI 1,728 / 760 / 1,722
#   Molina Healthcare 390 / 80 / 230    BrightSpring 1,970 / 324 / 1,970
#   Gentiva 773 / 367 / 771             Wellpath 640 / 184 / 640
#   DCI 295 / 142 / 295                 Nobis (7 hospitals) 106 / 19 / 106
#   Life Care Centers (first 60 of 226 pages) 600 / 140 / 594
import asyncio
import json

import pytest

import scraper


# -- every new config is present, once, with the shape its adapter reads -------

NEW = {
    "WORKDAY_TENANTS": {
        "CenterWell": ("humana", "5", "CenterWell_External_Career_Site"),
        "CVS Health": ("cvshealth", "1", "CVS_Health_Careers"),
        "Elevance Health": ("elevancehealth", "1", "ANT"),
        "Centene": ("centene", "5", "Centene_External"),
        "Humana": ("humana", "5", "Humana_External_Career_Site"),
        "Cigna": ("cigna", "5", "cignacareers"),
    },
    "JIBE_SITES": {
        "Genesis HealthCare": "https://www.genesiscareers.jobs",
        "SCA Health": "https://careers.sca.health",
        "Vibra Healthcare": "https://careers.vibrahealthcare.com",
        "Ernest Health": "https://www.ernesthealthcareers.com",
        "PAM Health": "https://pamhealth.jibeapply.com",
    },
    "TALENTBREW_ORGS": {
        "United Surgical Partners International": ("https://careers.uspi.com/search-jobs", 100),
        "Molina Healthcare": ("https://careers.molinahealthcare.com/search-jobs", 100),
    },
    "ICIMS_ORGS": {
        "BrightSpring Health Services": "careers-brightspring.icims.com",
        "Gentiva": "careers-gentivahs.icims.com",
        "Wellpath": "careers-wellpath.icims.com",
        "Dialysis Clinic, Inc.": "careers-dialysisclinic.icims.com",
    },
    "PAYCOR_ORGS": {
        "Bradenton Rehabilitation Hospital": "8a7883c69a5b2f45019a7373f680050b",
        "Cincinnati Rehabilitation Hospital": "8a7883c68cf53477018d1997ca2d18bc",
        "Clarksville Rehabilitation Hospital": "8a7883c69b1375e0019bb7bc32777bc0",
        "Florida Rehabilitation Hospital at Tampa": "8a7883d08fa7a14c018facbde2e800a2",
        "Little Rock Rehabilitation Hospital": "8a7883d0a13c112b01a13c753b510005",
        "Pinellas County Rehabilitation Hospital": "8a7883c69a5b2f45019a738d1581055b",
        "Tucson Rehabilitation Hospital": "8a7883c69520a6c8019585b875912475",
    },
    "HCSHIRING_SITES": {
        "Life Care Centers of America": "lifecarecareers.hcshiring.com",
    },
}
NOBIS = "Nobis Rehabilitation Partners"


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


def test_humana_tenant_runs_two_sites_not_one_twice():
    wd = scraper.WORKDAY_TENANTS
    assert wd["CenterWell"][0] == wd["Humana"][0] == "humana"
    assert wd["CenterWell"][2] != wd["Humana"][2]


def test_new_labels_are_new_and_feed_only_their_own_config():
    # A label must not be a label of another adapter config (it would merge
    # two employers or make one system two feeds) nor be rewritten by an
    # alias, except the Nobis hospital boards, which file under one system.
    feeds = scraper.configured_feed_map()
    seen = {}
    for dict_name, entries in NEW.items():
        for system in entries:
            assert system not in seen, (system, seen.get(system), dict_name)
            seen[system] = dict_name
            assert feeds[system] == {dict_name}, (system, feeds[system])
            if dict_name == "PAYCOR_ORGS":
                assert scraper.HOSPITAL_SYSTEM_ALIASES[system] == NOBIS
            else:
                assert system not in scraper.HOSPITAL_SYSTEM_ALIASES, system
    assert NOBIS not in feeds or feeds[NOBIS] == set()
    # no existing label is a near twin of a new one ("Genesis HealthCare
    # System" is the Zanesville OH hospital system, a different employer)
    assert scraper.WORKDAY_TENANTS["Genesis HealthCare System"] == ("genesishcs", "1", "GenesisHCS")


def test_dead_entries_are_retired():
    # careers-genesishcc.icims.com answers a redirect script to the Jibe
    # front; SmartRecruiters "KindredatHome" lists 0 postings. Neither ever
    # wrote a row.
    assert "Genesis HealthCare" not in scraper.ICIMS_ORGS
    assert "careers-genesishcc.icims.com" not in scraper.ICIMS_ORGS.values()
    assert "Kindred Healthcare" not in scraper.SMARTRECRUITERS_ORGS
    assert "KindredatHome" not in scraper.SMARTRECRUITERS_ORGS.values()


def test_left_out_boards_stay_out():
    # Nobis's corporate board (Allen TX office, remote roles) and its empty
    # "Nobis Hospital Partners, LLC" board; YesCare's iCIMS tenant answers 404.
    assert "8a7883c68cf53477018d19795ce3187a" not in scraper.PAYCOR_ORGS.values()
    assert "8a7883c69cd56968019ce24f4a830219" not in scraper.PAYCOR_ORGS.values()
    assert all("yescare" not in d for d in scraper.ICIMS_ORGS.values())


# -- payers: the clinical families only; Fresenius: the US, sliced by state -------

US = "bc33aa3152ec42d4995f4791a106ed09"


def test_payer_boards_are_cut_to_clinical_families():
    f = scraper.WD_TENANT_FACETS
    assert f["CVS Health"] == {"jobFamilyGroup": ["e65dbadf6a50100168ed7d5902dc0001",
                                                  "e65dbadf6a50100168ed7df44fa00000"]}
    assert f["Elevance Health"] == {"jobFamilyGroup": ["f42bff05a414010057202352de780000"]}
    assert len(f["Centene"]["jobFamilyGroup"]) == 4 and list(f["Centene"]) == ["jobFamilyGroup"]
    assert len(f["Humana"]["jobFamilyGroup"]) == 7 and list(f["Humana"]) == ["jobFamilyGroup"]
    assert f["Cigna"]["Location_Country"] == [US] and len(f["Cigna"]["jobFamilyGroup"]) == 3
    for system in ("CVS Health", "Elevance Health", "Centene", "Humana", "Cigna"):
        ids = [i for vals in f[system].values() for i in vals]
        assert len(ids) == len(set(ids)), system
    # CenterWell is a care provider: its whole board is read
    assert "CenterWell" not in f


def test_fresenius_is_cut_to_the_us_and_sliced_by_state():
    assert scraper.WD_TENANT_FACETS["Fresenius Medical Care"] == {"locationCountry": [US]}
    assert scraper.WD_SLICE_PARAM["Fresenius Medical Care"] == "locationRegionStateProvince"


def _facets(*groups, nested=()):
    out = [{"facetParameter": p, "values": vals} for p, vals in groups]
    if nested:
        out.append({"facetParameter": "locationMainGroup",
                    "values": [{"facetParameter": p, "descriptor": d, "values": vals} for p, d, vals in nested]})
    return {"facets": out}


def _v(i, n=5):
    return {"id": i, "descriptor": i, "count": n}


def test_slice_values_default_order():
    data = _facets(("timeType", [_v("t1")]), ("jobFamilyGroup", [_v("g1"), _v("g2")]))
    assert scraper._wd_slice_values(data, "Any Board") == ("jobFamilyGroup", ["g1", "g2"])


def test_slice_values_skip_a_parameter_the_fixed_facet_sets():
    # CVS's fixed facet is jobFamilyGroup: slicing on it would replace the
    # filter and read the retail families; the next parameter is used.
    data = _facets(("jobFamilyGroup", [_v("g1"), _v("g2")]), ("timeType", [_v("t1"), _v("t2")]))
    assert scraper._wd_slice_values(data, "CVS Health", {"jobFamilyGroup": ["g1"]}) == ("timeType", ["t1", "t2"])
    assert scraper._wd_slice_values(data, "X", {"jobFamilyGroup": ["g1"], "timeType": ["t1"]}) == (None, [])


def test_slice_values_named_parameter_reads_a_nested_group_and_skips_empty_values():
    data = _facets(("jobFamilyGroup", [_v("g1")]),
                   nested=[("locationCountry", "Country", [_v(US, 2794)]),
                           ("locationRegionStateProvince", "Region/State",
                            [_v("tx", 245), _v("oh", 133), _v("nowhere", 0)])])
    got = scraper._wd_slice_values(data, "Fresenius Medical Care", {"locationCountry": [US]})
    assert got == ("locationRegionStateProvince", ["tx", "oh"])


class _Resp:
    status = 200

    def __init__(self, body, text=None):
        self._body = body
        self._text = text

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return self._text if self._text is not None else json.dumps(self._body)


class _Ctx:
    def __init__(self, resp):
        self.resp = resp

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


async def _no_wait(*a, **k):
    return None


def _wd_posting(n, loc="Knoxville, TN"):
    return {"title": f"Registered Nurse {n}", "externalPath": f"/job/Knoxville-TN/RN_R{n:07d}",
            "locationsText": loc, "postedOn": "Posted Today", "bulletFields": [f"R{n:07d}"]}


def test_fresenius_window_hit_is_sliced_by_state_inside_the_us(monkeypatch):
    """2,000-row plain sweep inside the US filter, then one crawl per state
    (not per job family, whose US clinical slice alone overflows)."""
    bodies = []

    def answer(body):
        applied = body.get("appliedFacets") or {}
        if body.get("limit") == 1:
            return _facets(("jobFamilyGroup", [_v("clinical", 2674)]),
                           nested=[("locationRegionStateProvince", "Region/State",
                                    [_v("tn", 91), _v("nm", 49), _v("pr", 0)])])
        st = (applied.get("locationRegionStateProvince") or [None])[0]
        if st == "tn":
            return {"total": 2, "jobPostings": [_wd_posting(9001), _wd_posting(9002)]}
        if st == "nm":
            return {"total": 1, "jobPostings": [_wd_posting(9003, "Albuquerque, NM")]}
        off = body["offset"]
        return {"total": 2000, "jobPostings": [_wd_posting(off + k) for k in range(20)]}

    def req(session, method, url, **kw):
        bodies.append(kw.get("json") or {})
        return _Ctx(_Resp(answer(kw.get("json") or {})))

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    system = "Fresenius Medical Care"
    jobs = asyncio.run(scraper.scrape_workday(None, system, scraper.WORKDAY_TENANTS[system]))
    base = {"locationCountry": [US]}
    sweep = [b for b in bodies if b.get("limit") == 20 and "locationRegionStateProvince" not in (b.get("appliedFacets") or {})]
    slices = [b["appliedFacets"] for b in bodies if "locationRegionStateProvince" in (b.get("appliedFacets") or {})]
    assert len(sweep) == 100 and all(b["appliedFacets"] == base for b in sweep)
    assert slices == [{**base, "locationRegionStateProvince": ["tn"]}, {**base, "locationRegionStateProvince": ["nm"]}]
    assert not any("jobFamilyGroup" in (b.get("appliedFacets") or {}) for b in bodies)
    assert len(jobs) == 2003


# -- Workday: state-first site names (CVS, Elevance) -----------------------------

@pytest.mark.parametrize("text,want", [
    ("IL - Chicago", ("Chicago", "IL")),
    ("TX - Work from home", ("", "TX")),
    ("NY---Work-from-hom", ("", "NY")),
    ("OH-MASON-4361-IRWIN-SIMPSON-RD", ("Mason", "OH")),
    ("IA-URBANDALE-12421-MEREDITH-DR-STE-130", ("Urbandale", "IA")),
    ("IN-INDIANAPOLIS-220-VIRGINIA-AVE", ("Indianapolis", "IN")),
    ("MO - St Louis", ("St. Louis", "MO")),
    ("NY - New York", ("New York", "NY")),
    ("TX - Texas", ("", "TX")),
    ("CA - Remote", ("", "CA")),
    ("Field-Ohio", ("", "")),
    ("Chicago, IL", ("", "")),
])
def test_state_first_site_names(text, want):
    assert scraper._wd_state_first(text) == want


def test_state_first_rule_is_limited_to_its_tenants(monkeypatch):
    assert scraper.WD_STATE_FIRST == frozenset({"CVS Health", "Elevance Health"})

    def run(system, loc, path):
        posting = {"title": "Case Manager RN", "externalPath": path, "locationsText": loc,
                   "postedOn": "Posted Today", "bulletFields": ["R1000001"]}

        def req(session, method, url, **kw):
            body = kw.get("json") or {}
            if body.get("limit") == 1:
                return _Ctx(_Resp({"facets": []}))
            return _Ctx(_Resp({"total": 1, "jobPostings": [posting]}))
        monkeypatch.setattr(scraper, "req", req)
        monkeypatch.setattr(scraper, "jitter", _no_wait)
        monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
        monkeypatch.setitem(scraper.WD_TENANT_FACETS, system, scraper.WD_TENANT_FACETS.get(system, {}))
        j = asyncio.run(scraper.scrape_workday(None, system, ("t", "1", "S")))[0]
        return j.city, j.state

    assert run("CVS Health", "IL - Chicago", "/job/IL---Chicago/Case-Manager-RN_R1000001") == ("Chicago", "IL")
    assert run("Elevance Health", "2 Locations",
               "/job/CO-DENVER-700-BROADWAY/Nurse-Case-Manager-II_JR209803") == ("Denver", "CO")
    # another tenant with the same text keeps the old reading
    city, state = run("Other Board", "IL - Chicago", "/job/IL---Chicago/Case-Manager-RN_R1000001")
    assert city != "Chicago"


# -- Jibe: facility names, posting links, Vibra's staffing rows ---------------------

def _jibe_row(rid, title, **kw):
    d = {"req_id": rid, "slug": rid, "title": title, "city": "Folsom", "state": "California",
         "country": "United States", "description": "<p>" + "Posting text. " * 30 + "</p>"}
    d.update(kw)
    return {"data": d}


def _run_jibe(monkeypatch, system, rows):
    feed = {"totalCount": len(rows), "jobs": rows}

    def req(session, method, url, **kw):
        if url.endswith("/api/jobs"):
            return _Ctx(_Resp(feed))
        return _Ctx(_Resp({}, text=""))
    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    scraper.COMPLETE_SYSTEMS.discard(system)
    scraper.PARTIAL_SYSTEMS.discard(system)
    return asyncio.run(scraper.scrape_jibe(None, system, scraper.JIBE_SITES[system]))


def test_vibra_staffing_placements_are_read_and_left_out(monkeypatch):
    rows = [
        _jibe_row("30326", "RN Registered Nurse Part Time ICU Nights RN", tags7=["Vibra Hospital of Sacramento"]),
        _jibe_row("30314", "Registered Nurse RN Surgical Services Main OR Full Time RN",
                  tags7=["Vibra Travels - Permanent Staffing - San Antonio, TX"], city="San Antonio", state="Texas"),
        _jibe_row("30400", "Registered Nurse", tags7=["Vibra Permanent Staffing - Paducah, KY"],
                  city="Paducah", state="Kentucky"),
        _jibe_row("30500", "Physical Therapist", tags7=["VibraLife of Mechanicsburg"],
                  city="Mechanicsburg", state="Pennsylvania"),
    ]
    jobs = _run_jibe(monkeypatch, "Vibra Healthcare", rows)
    assert {j.job_id: j.hospital_name for j in jobs} == {"30326": "Vibra Hospital of Sacramento",
                                                         "30500": "VibraLife of Mechanicsburg"}
    # every posting was served (two stored, two left out on purpose): complete
    assert "Vibra Healthcare" in scraper.COMPLETE_SYSTEMS
    assert jobs[0].url == "https://careers.vibrahealthcare.com/jobs/30326"


def test_leave_out_rule_touches_no_other_board():
    assert set(scraper.JIBE_LEAVE_OUT) == {"Vibra Healthcare"}
    j = {"tags7": ["Vibra Travels - Permanent Staffing - Kerrville, Tx"], "location_name": "x"}
    assert scraper._jibe_left_out("Vibra Healthcare", j)
    assert not scraper._jibe_left_out("Ernest Health", j)


def test_jibe_facilities_and_links_of_the_new_boards(monkeypatch):
    g = _run_jibe(monkeypatch, "Genesis HealthCare", [
        _jibe_row("56102", "Registered Nurse Supervisor, RN", location_name="Siler City Center",
                  city="Siler City", state="North Carolina")])[0]
    assert (g.hospital_name, g.city, g.state) == ("Siler City Center", "Siler City", "NC")
    assert g.url == "https://www.genesiscareers.jobs/genesishcc/jobs/56102"
    s = _run_jibe(monkeypatch, "SCA Health", [
        _jibe_row("49828", "Registered Nurse Endoscopy II", location_name="MA 50809 Emerson Endo",
                  tags3=["Emerson Endo and Digestive Health Cntr"], city="Concord", state="Massachusetts",
                  meta_data={"canonical_url": "https://careers.sca.health/jobs/49828?lang=en-us"})])[0]
    assert s.hospital_name == "Emerson Endo and Digestive Health Cntr"
    assert s.url == "https://careers.sca.health/jobs/49828?lang=en-us"
    e = _run_jibe(monkeypatch, "Ernest Health", [
        _jibe_row("35659", "Registered Nurse - Full Time Nights", location_name="Bakersfield Rehabilitation Hospital",
                  city="Bakersfield", meta_data={"canonical_url": "https://careers.ernesthealth.com/jobs/35659?lang=en-us"})])[0]
    assert e.hospital_name == "Bakersfield Rehabilitation Hospital"
    assert e.url == "https://www.ernesthealthcareers.com/careers-home/jobs/35659?lang=en-us"
    p = _run_jibe(monkeypatch, "PAM Health", [
        _jibe_row("97034", "Registered Nurse (RN) - FT Nights | Aiken Rehab",
                  location_name="PAM Health Rehabilitation Hospital of Aiken", city="Aiken", state="South Carolina")])[0]
    assert (p.hospital_name, p.state) == ("PAM Health Rehabilitation Hospital of Aiken", "SC")
    assert p.url == "https://pamhealth.jibeapply.com/jobs/97034"


# -- TalentBrew: Molina's <strong> locations -----------------------------------------

@pytest.mark.parametrize("tail,want", [
    ('<span class="job-location"> <strong>Lincoln, NE, United States;</strong> <strong>Lincoln, Nebraska</strong> </span>',
     ("Lincoln", "NE")),
    ('<span class="job-location"> <strong>United States;</strong> </span>', ("", "")),
    ('<span class="job-location"> <strong>NE, United States;</strong> </span>', ("", "NE")),
    ('<span class="job-location">Chattanooga, TN</span>', None),
    ('<span class="job-molina">Molina Healthcare</span>', None),
])
def test_talentbrew_strong_locations(tail, want):
    assert scraper._tb_strong_location(tail) == want


def test_molina_cards_take_town_and_state(monkeypatch):
    cards = (
        '<section id="search-results" data-total-results="2"><ul>'
        '<li> <a href="/job/lincoln/nurse-practitioner/21726/102001544720" data-job-id="102001544720">'
        ' <h2>Nurse Practitioner</h2> <span class="job-molina">Molina Healthcare</span>'
        ' <span class="job-location"> <strong>Lincoln, NE, United States;</strong> <strong>Lincoln, Nebraska</strong> </span>'
        ' <span class="job-date-posted">10/15/2026</span> </a> </li>'
        '<li> <a href="/job/united-states/care-review-clinician-rn-remote/21726/102470290960" data-job-id="102470290960">'
        ' <h2>Care Review Clinician RN (Remote)</h2> <span class="job-molina">Molina Healthcare</span>'
        ' <span class="job-location"> <strong>United States;</strong> </span> </a> </li></ul></section>')

    def req(session, method, url, **kw):
        if url.endswith("/results"):
            return _Ctx(_Resp(None, text=json.dumps({"results": cards, "hasJobs": True})))
        return _Ctx(_Resp(None, text=""))
    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    url, rpp = scraper.TALENTBREW_ORGS["Molina Healthcare"]
    jobs = asyncio.run(scraper.scrape_talentbrew(None, "Molina Healthcare", url, rpp))
    got = {j.job_id: (j.city, j.state) for j in jobs}
    assert got == {"102001544720": ("Lincoln", "NE"), "102470290960": ("", "")}
    assert jobs[0].url == "https://careers.molinahealthcare.com/job/lincoln/nurse-practitioner/21726/102001544720"


# -- Nobis: one system, the hospital as hospital_name ---------------------------------

def test_nobis_boards_file_under_one_system():
    page = ('<div class="gnewtonCareerGroupHeaderClass">Nursing</div>'
            '<div class="gnewtonCareerGroupRowClass"><div class="gnewtonCareerGroupJobTitleClass">'
            '<a href="https://recruitingbypaycor.com/career/JobIntroduction.action?clientId=8a7883c69a5b2f45019a7373f680050b'
            '&amp;id=8a78839ea150ae2901a159d634a703e7&amp;source=&amp;lang=en">Registered Nurse Full Time Days</a></div>'
            '<div class="gnewtonCareerGroupJobDescriptionClass">Bradenton, FL</div></div>')
    jobs = scraper._parse_paycor_home(page, "Bradenton Rehabilitation Hospital", "8a7883c69a5b2f45019a7373f680050b")
    assert [(j.hospital_name, j.city, j.state) for j in jobs] == [("Bradenton Rehabilitation Hospital", "Bradenton", "FL")]
    assert scraper.HOSPITAL_SYSTEM_ALIASES[jobs[0].hospital_system] == NOBIS


# -- HealthcareSource hcshiring.com (Life Care Centers of America) ----------------------

LC = "Life Care Centers of America"
LC_HOST = "lifecarecareers.hcshiring.com"
LC_PAGE = ("<html><head><script>var V = { version: 'cb5572bd', releaseDate: '260723', brand: function() {}};"
           "</script></head><body></body></html>")


def _lc_row(i, title="RN Registered Nurse", **kw):
    d = {"id": f"id{i:04d}", "title": title, "summary": "Position Summary: cut at two hundred characters",
         "isInternalOnly": False, "organization": "Life Care Center of Wichita", "street": "622 N Edgemoor St",
         "city": "Wichita", "state": "KS", "zip": "67208", "onePage": True, "hasOpening": True,
         "lastOpening": "2026-10-22T20:48:26.5067505Z", "validThrough": None, "compensationMax": None,
         "compensationMin": None, "education": None, "experienceYears": None, "googleEmploymentType": None,
         "googleSalaryUnit": None,
         "publicFieldValues": [{"name": "Shift", "value": "Days/Weekends", "standardName": None},
                               {"name": "Employment Status", "value": "PRN", "standardName": "Status"}]}
    d.update(kw)
    return d


def test_hcshiring_row():
    j = scraper._hcshiring_job(_lc_row(1), LC, "https://" + LC_HOST)
    assert (j.title, j.hospital_system, j.hospital_name, j.city, j.state) == (
        "RN Registered Nurse", LC, "Life Care Center of Wichita", "Wichita", "KS")
    assert j.url == "https://lifecarecareers.hcshiring.com/jobs/id0001"
    assert (j.job_id, j.posted_date, j.job_type, j.ats_platform) == ("id0001", "2026-10-22", "Per diem", "HCSHiring")
    assert j.description == ""          # the 200-character summary is not stored
    assert j.wage_min is None
    assert scraper._hcshiring_job({"id": "", "title": "x"}, LC, "https://x") is None


def test_hcshiring_pay():
    assert scraper._hcshiring_pay({"compensationMin": 38.0, "compensationMax": 46.32, "googleSalaryUnit": "HOUR"}) == (38.0, 46.32, "hour")
    assert scraper._hcshiring_pay({"compensationMin": None, "compensationMax": None}) is None


def _fake_lc(monkeypatch, pages, total, fail_on=None):
    calls = []

    def req(session, method, url, **kw):
        p = kw.get("params") or {}
        calls.append((url, dict(p)))
        if url == f"https://{LC_HOST}/jobs":
            return _Ctx(_Resp(None, text=LC_PAGE))
        if url == f"https://{LC_HOST}/cb5572bd/api/jobs":
            n = int(p["page"])
            if fail_on and n == fail_on:
                r = _Resp(None)
                r.status = 500
                return _Ctx(r)
            return _Ctx(_Resp({"jobs": pages[n - 1] if n <= len(pages) else [],
                               "meta": {"page": n, "perPage": 10, "totalJobs": total, "totalPages": len(pages)}}))
        if url == f"https://{LC_HOST}/cb5572bd/api/jobDescriptions":
            return _Ctx(_Resp({"jobDescriptions": {
                "id": p["id"], "description": "<div class='job'><p>" + "Deliver quality nursing care. " * 12 + "</p></div>",
                "fields": [{"id": "Employment Status", "name": "Full-Time"}, {"id": "Shift", "name": "Nights"}],
                "job": {"id": p["id"], "compensationMin": 38.0, "compensationMax": 46.0, "googleSalaryUnit": "HOUR",
                        "googleEmploymentType": "FULL_TIME"}}}))
        raise AssertionError(url)

    async def fetch_html(session, url, timeout=20):
        return LC_PAGE if url == f"https://{LC_HOST}/jobs" else ""
    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "_fetch_html", fetch_html)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    scraper.COMPLETE_SYSTEMS.discard(LC)
    scraper.PARTIAL_SYSTEMS.discard(LC)
    scraper.COMPLETE_FEEDS.discard(("HCSHIRING_SITES", LC))
    return calls


def test_hcshiring_reads_every_page_and_claims_complete(monkeypatch):
    pages = [[_lc_row(i) for i in range(10)], [_lc_row(i) for i in range(10, 20)], [_lc_row(20), _lc_row(5)]]
    calls = _fake_lc(monkeypatch, pages, total=21)
    jobs = asyncio.run(scraper.scrape_hcshiring(None, LC, LC_HOST))
    assert len(jobs) == 21 and len({j.job_id for j in jobs}) == 21          # the repeat on page 3 is dropped
    assert [p["page"] for u, p in calls if u.endswith("/api/jobs")] == ["1", "2", "3"]
    assert (("HCSHIRING_SITES", LC) in scraper.COMPLETE_FEEDS) and LC not in scraper.PARTIAL_SYSTEMS


def test_hcshiring_failed_page_is_partial(monkeypatch):
    pages = [[_lc_row(i) for i in range(10)], [_lc_row(i) for i in range(10, 20)], [_lc_row(20)]]
    _fake_lc(monkeypatch, pages, total=21, fail_on=2)
    jobs = asyncio.run(scraper.scrape_hcshiring(None, LC, LC_HOST))
    assert len(jobs) == 10
    assert LC in scraper.PARTIAL_SYSTEMS and LC not in scraper.COMPLETE_SYSTEMS
    scraper.PARTIAL_SYSTEMS.discard(LC)


def test_hcshiring_page_ceiling_is_partial(monkeypatch):
    pages = [[_lc_row(i + 10 * n) for i in range(10)] for n in range(3)]
    _fake_lc(monkeypatch, pages, total=30)
    monkeypatch.setattr(scraper, "HCSHIRING_MAX_PAGES", 2)
    jobs = asyncio.run(scraper.scrape_hcshiring(None, LC, LC_HOST))
    assert len(jobs) == 20 and LC in scraper.PARTIAL_SYSTEMS
    scraper.PARTIAL_SYSTEMS.discard(LC)


def test_hcshiring_no_version_lists_nothing(monkeypatch):
    _fake_lc(monkeypatch, [[_lc_row(1)]], total=1)

    async def fetch_html(session, url, timeout=20):
        return "<html>Just a moment...</html>"
    monkeypatch.setattr(scraper, "_fetch_html", fetch_html)
    assert asyncio.run(scraper.scrape_hcshiring(None, LC, LC_HOST)) == []


def test_hcshiring_detail_fills_body_type_shift_and_pay(monkeypatch):
    _fake_lc(monkeypatch, [[_lc_row(1, googleEmploymentType=None, publicFieldValues=[])]], total=1)
    jobs = asyncio.run(scraper.scrape_hcshiring(None, LC, LC_HOST))
    j = jobs[0]
    assert j.job_type == ""
    assert asyncio.run(scraper._hcshiring_detail(None, j)) is True
    assert j.description.startswith("Deliver quality nursing care.") and "Schedule: Night" in j.description
    assert (j.job_type, j.wage_min, j.wage_max, j.wage_unit) == ("Full time", 38.0, 46.0, "hour")


def test_hcshiring_is_wired_into_the_run():
    import inspect
    assert "HCSHIRING_SITES" in scraper.FEED_CONFIGS
    assert "HCSHiring" in scraper.KNOWN_BODY_PLATFORMS
    assert "run_hcshiring(proxy_session)" in inspect.getsource(scraper.run_all)
    assert scraper.configured_feed_map()[LC] == {"HCSHIRING_SITES"}
