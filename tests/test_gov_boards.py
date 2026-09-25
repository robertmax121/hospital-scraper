# 2026-09-25 (push 5 / gov): state government boards. Fixtures were captured
# on 2026-09-25 through the adapters' own requests and trimmed: seven rows of
# the NY State Jobs vacancy table, three tiles of the Texas HHS search page
# (tile layout) and two rows of the Florida search page (table layout).
import asyncio
import re

import scraper


# ── NY State Jobs ─────────────────────────────────────────────────────────
def test_ny_rows_keep_only_named_cms_hospitals(fixture_text):
    jobs = scraper._parse_ny_statejobs(fixture_text("ny_statejobs_table.html"))
    names = sorted(j.hospital_name for j in jobs)
    # The MTA row and the Central New York Psychiatric Center row (no CMS
    # hospital) are dropped.
    assert names == sorted([
        "Manhattan Psychiatric Center", "Helen Hayes Hospital", "New York City Children's Center",
        "Rockland Children's Psychiatric Center", "Mid-Hudson Forensic Psychiatric Center"])
    for j in jobs:
        assert j.state == "NY" and j.ats_platform == "NYStateJobs"
        assert j.url == f"https://statejobs.ny.gov/public/vacancyDetailsView.cfm?id={j.job_id}"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date)
        assert "&#x" not in j.title and "<" not in j.description


def test_ny_row_maps_facility_city_and_system(fixture_text):
    jobs = {j.job_id: j for j in scraper._parse_ny_statejobs(fixture_text("ny_statejobs_table.html"))}
    m = jobs["225726"]
    assert m.hospital_system == scraper.NY_STATEJOBS_SYSTEM and m.city == "New York"
    assert m.title.startswith("Health Information Management Technician 2 (NY HELPS)")
    assert m.posted_date == "2026-09-25" and "Apply by 2026-10-09" in m.description
    hh = jobs["225291"]          # facility named in the agency column only
    assert hh.hospital_name == "Helen Hayes Hospital" and hh.city == "West Haverstraw"
    assert hh.hospital_system == scraper.NY_STATEJOBS_DOH_SYSTEM


def test_ny_rockland_childrens_is_not_rockland_psychiatric():
    hay = "Nurse 2, Rockland Children's Psychiatric Center | Mental Health, Office of"
    hits = [name for rx, name, _, _ in scraper._NY_FACILITY_RX if rx.search(hay)]
    assert hits[0] == "Rockland Children's Psychiatric Center"
    assert "Rockland Psychiatric Center" not in hits


def test_ny_facility_spelling_variants():
    for hay, want in (("Clerk, St.Lawrence Psychiatric Center", "St. Lawrence Psychiatric Center"),
                      ("Clerk, St. Lawerence Psychiatric Center", "St. Lawrence Psychiatric Center"),
                      ("Clerk, Capital Distric Psychiatric Center", "Capital District Psychiatric Center"),
                      ("Aide, Western New York Children Psychiatric Center", "Western New York Children's Psychiatric Center"),
                      ("RN, Hudson Forensic Psychiatric Center", "Mid-Hudson Forensic Psychiatric Center")):
        hit = next(name for rx, name, _, _ in scraper._NY_FACILITY_RX if rx.search(hay))
        assert hit == want, hay


def test_ny_date_forms():
    assert scraper._ny_date("09/25/26") == "2026-09-25"
    assert scraper._ny_date("1/5/2027") == "2027-01-05"
    assert scraper._ny_date("13/40/26") == "" and scraper._ny_date("") == ""


def test_ny_empty_page_is_empty():
    assert scraper._parse_ny_statejobs("<html>maintenance</html>") == []


# ── SuccessFactors RMK boards ─────────────────────────────────────────────
def test_rmk_tile_layout_parses(fixture_text):
    tiles = scraper._parse_rmk_tiles(fixture_text("sf_rmk_tiles.html"), "https://careers.hhs.texas.gov/hhscjobs")
    assert len(tiles) == 3
    for t in tiles:
        assert t["url"].startswith("https://careers.hhs.texas.gov/hhscjobs/job/") and t["job_id"] in t["url"]
        assert t["city"] == "Austin" and t["state"] == "TX"      # the value div, not the aria label
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", t["posted"]) and t["title"]


def test_rmk_table_layout_parses(fixture_text):
    tiles = scraper._parse_rmk_tiles(fixture_text("sf_rmk_table.html"), "https://jobs.myflorida.com")
    assert len(tiles) == 2
    for t in tiles:
        assert t["url"].startswith("https://jobs.myflorida.com/job/") and t["job_id"] in t["url"]
        assert t["city"] == "Macclenny" and t["state"] == "FL"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", t["posted"])


def test_rmk_jobs_keep_only_the_hospitals_city():
    tiles = [{"job_id": "1", "url": "u1", "title": "RN", "city": "Austin", "state": "TX", "posted": ""},
             {"job_id": "2", "url": "u2", "title": "RN", "city": "Kerrville", "state": "TX", "posted": ""},
             {"job_id": "3", "url": "u3", "title": "RN", "city": "Austin", "state": "AR", "posted": ""}]
    jobs = scraper._rmk_jobs(tiles, "Texas Health and Human Services", "TX", "Austin State Hospital", ("Austin",))
    assert [j.job_id for j in jobs] == ["1"]
    j = jobs[0]
    assert j.hospital_name == "Austin State Hospital" and j.location == "Austin, TX"
    assert j.ats_platform == "SuccessFactorsRMK"


def test_rmk_scrape_pages_until_short_page_and_dedupes(monkeypatch):
    calls = []

    class _Resp:
        status = 200

        def __init__(self, text):
            self._t = text

        async def text(self):
            return self._t

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    def tile(i, city):
        return (f'<li class="job-tile job-id-{i} x" data-url="/b/job/X/{i}/">'
                f'<a class="jobTitle-link" href="/b/job/X/{i}/">Nurse {i}</a>'
                f'<div id="job-{i}-desktop-section-location-value">{city}, TX </div></li>')

    def fake_req(session, method, url, **kw):
        start = int(kw["params"]["startrow"])
        calls.append((kw["params"]["q"], start))
        if start == 0:
            body = "".join(tile(n, "RUSK") for n in range(25))
        else:
            body = tile(100, "RUSK") + tile(101, "TYLER") + tile(3, "RUSK")
        return _Resp('<ul id="job-tile-list">' + body + "</ul>")

    async def no_sleep(*a, **k):
        return None

    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    cfg = ("https://example.org/b", "TX", (("Rusk State Hospital", "Rusk State Hospital", ("Rusk",)),))
    jobs = asyncio.run(scraper.scrape_sf_rmk(None, "Texas Health and Human Services", cfg))
    assert calls == [('"Rusk State Hospital"', 0), ('"Rusk State Hospital"', 25)]
    assert len(jobs) == 26 and {j.hospital_name for j in jobs} == {"Rusk State Hospital"}
    assert all(j.url.startswith("https://example.org/b/job/") for j in jobs)


def test_rmk_boards_config_shape():
    for system, (base, state, facilities) in scraper.SF_RMK_BOARDS.items():
        assert base.startswith("https://") and not base.endswith("/") and len(state) == 2
        for phrase, hospital, cities in facilities:
            assert phrase and hospital and isinstance(cities, tuple) and cities
    assert scraper.SF_RMK_PAGE_GAP[0] >= 1.0      # one request a second per board at most


def test_new_runners_are_wired_into_run_all():
    import inspect
    src = inspect.getsource(scraper.run_all)
    assert "run_ny_statejobs(" in src and "run_sf_rmk(" in src


def test_ny_curly_apostrophe_matches():
    hay = "Nutrition Services Administrator 1, Rockland Children\u2019s Psychiatric Center, P28031"
    hit = next(name for rx, name, _, _ in scraper._NY_FACILITY_RX if rx.search(hay))
    assert hit == "Rockland Children's Psychiatric Center"


# ── Public hospital authorities / state university hospitals (configs) ────
def test_gov_configs_present_and_shaped():
    wd = scraper.WORKDAY_TENANTS
    for system, tenant in (("Denver Health", "denverhealth"), ("Palomar Health", "palomarhealth"),
                           ("Phoebe Putney Health", "phoebehealth"), ("Baptist Health (Alabama)", "baptistfirst"),
                           ("University of Mississippi Medical Center", "ummc")):
        t, wdn, site = wd[system]
        assert t == tenant and wdn.isdigit() and site
    assert scraper.WD_TENANT_DEFAULT["Denver Health"] == ("Denver", "CO")
    for system in ("UVA Health", "MU Health Care", "Broward Health"):
        assert scraper.PHENOM_ORGS[system].startswith("https://careers.")
    assert scraper.JIBE_SITES["Tanner Health"] == "https://careers.tanner.org"
    assert scraper.CSOD_ORGS["UI Health"] == ("https://uic.csod.com", "2")
    assert scraper.JIBE_SITES["USA Health"] == "https://careers.usahealthsystem.com" and "USA Health" not in scraper.ICIMS_ORGS
    base, state, fac = scraper.SF_RMK_BOARDS["Arkansas Department of Human Services"]
    assert base == "https://arcareers.arkansas.gov" and state == "AR"
    assert fac == (("Arkansas State Hospital", "Arkansas State Hospital", ("Little Rock",)),)


def test_new_labels_do_not_collide_with_existing_systems():
    new = {"Denver Health", "Palomar Health", "Phoebe Putney Health", "Baptist Health (Alabama)",
           "University of Mississippi Medical Center", "UVA Health", "MU Health Care", "Broward Health",
           "Tanner Health", "UI Health", "USA Health", "Arkansas Department of Human Services",
           "DCH Health System", "Southeast Health (Dothan)", "MarinHealth", "Norman Regional Health System",
           "Regional One Health", "East Alabama Health", "Georgia DBHDD", "Oregon State Hospital"}
    boards = [scraper.WORKDAY_TENANTS, scraper.PHENOM_ORGS, scraper.JIBE_SITES, scraper.CSOD_ORGS,
              scraper.ICIMS_ORGS, scraper.SF_RMK_BOARDS, scraper.NEOGOV_AGENCIES, scraper.HCTS_PORTALS,
              scraper.PAYCOM_ORGS]
    for label in new:
        assert sum(label in b for b in boards) == 1, label


def test_uva_drop_rule_keeps_health_entities():
    rx = scraper.PHENOM_DROP_EMPLOYERS["UVA Health"]
    assert rx.search("The Rector & Visitors of the University of Virginia")
    assert rx.search("The University of Virginia's College at Wise")
    for keep in ("UVA Medical Center", "UVA Community Health", "University of Virginia Physicians Group", ""):
        assert not rx.search(keep), keep
    assert "MU Health Care" not in scraper.PHENOM_DROP_EMPLOYERS


def test_georgia_title_rule_keeps_only_named_state_hospitals():
    f = scraper._wd_title_facility
    assert f("Georgia DBHDD", "Food Service Worker Lead - East Central Regional Hospital") == (
        "East Central Regional Hospital", "Augusta", "GA")
    assert f("Georgia DBHDD", "Health Aide, Gracewood Campus, East Central Regional Hospital")[0] == "East Central Regional Hospital"
    assert f("Georgia DBHDD", "Dentist - ECRH - Mobile Unit")[0] == "East Central Regional Hospital"
    assert f("Georgia DBHDD", "Psychiatrist - GRHA") == ("Georgia Regional Hospital at Atlanta", "Decatur", "GA")
    assert f("Georgia DBHDD", "Hospital Chaplain - GRHS, Savannah, GA")[0] == "Georgia Regional Hospital at Savannah"
    assert f("Georgia DBHDD", "RN - West Central Georgia Regional Hospital")[1] == "Columbus"
    # community programs, HQ, and Central State Hospital (no CMS target) drop
    for t in ("Behavioral Health Counselor- Community Integration Home- Columbus", "NTP Surveyor- Atlanta",
              "Clinical Director - Psychiatrist - Central State Hospital", "Charge Nurse", "Grhapple"):
        assert f("Georgia DBHDD", t) is None, t
    assert f("Denver Health", "Psychiatrist - GRHA") is None      # rule is scoped to its board


def test_statewide_tenants_are_cut_by_facets():
    assert scraper.WD_TENANT_FACETS["Georgia DBHDD"] == {"hiringCompany": ["3f907d8292e51000cddbc10cc6a80000"]}
    assert len(scraper.WD_TENANT_FACETS["Oregon State Hospital"]["locations"]) == 2
    assert scraper._wd_facility("Oregon State Hospital", "Junction City | OHA | Oregon State Hospital",
                                "Junction City | OHA | Oregon State Hospital", "") == (
        "Oregon State Hospital", "Junction City", "OR")


def test_campus_boards_map_to_cms_hospitals():
    fac = scraper._wd_facility
    assert fac("Baptist Health (Alabama)", "Prattville Baptist Hospital", "Prattville Baptist Hospital", "") == (
        "Prattville Baptist Hospital", "Prattville", "AL")
    assert fac("Baptist Health (Alabama)", "Baptist Medical Center East", "", "")[0] == "Baptist Medical Center East"
    assert fac("Phoebe Putney Health", "Sumter Campus", "Sumter Campus", "") == ("Phoebe Sumter Medical Center", "Americus", "GA")
    assert fac("Phoebe Putney Health", "Phoebe North Campus", "", "")[0] == "Phoebe Putney Memorial Hospital"
    assert fac("Phoebe Putney Health", "Albany Meredyth", "Albany Meredyth", "")[0] is None
    for system in ("Baptist Health (Alabama)", "Phoebe Putney Health", "Palomar Health", "Southeast Health (Dothan)",
                   "MarinHealth", "University of Mississippi Medical Center", "Denver Health"):
        assert len(scraper.WD_TENANT_DEFAULT[system]) == 2


def test_hcts_h4_card_parses():
    # East Alabama Health card shape (alabamahealth.hctsportals.com, 2026-09-25), trimmed.
    seg = ('<div class="jobs-section__item p-3"><div class="row"><div class="col-12">'
           '<h4><a href="https://alabamahealth.hctsportals.com/jobs/2199490-rn-cardiac-cath-lab">RN - CARDIAC CATH LAB</a>'
           '</h4></div></div><div class="row"><div class="col-xs-12 col-sm-6">'
           '<i class="fas fa-map-marker hide-for-large text-muted" aria-hidden="true" data-toggle="tooltip" '
           'data-placement="top" title="Location"></i>&nbsp;\n      OPELIKA, AL, United States\n   </div></div></div>')
    jobs = scraper._parse_hcts_page(seg, "East Alabama Health", "alabamahealth", "AL")
    assert len(jobs) == 1
    j = jobs[0]
    assert j.job_id == "2199490" and j.title == "RN - CARDIAC CATH LAB" and j.state == "AL"
    assert j.city.lower() == "opelika"
    assert j.url == "https://alabamahealth.hctsportals.com/jobs/2199490-rn-cardiac-cath-lab"
    assert scraper.HCTS_PORTALS["East Alabama Health"] == ("alabamahealth", "AL")


def test_ohio_state_buildings_take_columbus():
    fac = scraper._wd_facility
    s = "Ohio State Wexner Medical Center"
    assert fac(s, "University Hospital - Doan Hall (0089)", "University Hospital - Doan Hall", "") == (
        "Ohio State University Hospital", "Columbus", "OH")
    assert fac(s, "James Cancer Hospital (0375)", "James Cancer Hospital (0375)", "")[1:] == ("Columbus", "OH")
    assert fac(s, "Medical Center Campus", "Medical Center Campus", "") == (None, "Columbus", "OH")
    assert len(scraper.WD_TENANT_FACETS[s]["locations"]) == 14
