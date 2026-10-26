"""RN gap fixes (2026-10-27): the sitemap career-site adapter (Aveanna
Healthcare), NEOGOV state and county boards with nurse jobs only, and the
Frontline AppliTrack school district boards (school nurses). Every fixture is
a trimmed public response of 10-26 (tests/fixtures/gap_1027); no network:
the page getters are replaced."""
import asyncio
import os
import pathlib
import re

import pytest

import scraper

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "gap_1027")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


@pytest.fixture(autouse=True)
def _clean_claims():
    for s in (scraper.COMPLETE_SYSTEMS, scraper.PARTIAL_SYSTEMS, scraper.PARTIAL_BACKSTOP_SYSTEMS):
        s.clear()
    scraper.COMPLETE_FEEDS.clear()
    yield
    for s in (scraper.COMPLETE_SYSTEMS, scraper.PARTIAL_SYSTEMS, scraper.PARTIAL_BACKSTOP_SYSTEMS):
        s.clear()
    scraper.COMPLETE_FEEDS.clear()


# ── Nurse titles ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("title,want", [
    ("Licensed Practical Nurse - E9", True),
    ("Registered Nurse Manager 13", True),
    ("School Nurse (Updated)", True),
    ("Nurse Specialist (Two Positions)", True),
    ("2026/27 NURSE Substitute", True),
    ("Director of Nursing", True),
    ("Psychiatric Nurse Practitioner", True),
    ("RN - Campus", True),
    ("Department Technician E9 - Nursing Staff Coordinator", False),
    ("Nursery Technician (Forest Technician)", False),
    ("Certified Nursing Assistant", False),
    ("Nurse Aide", False),
    ("Health Science Teacher - Nursing", False),
    ("TCHR-CTE HEALTH SCIENCES- RN", False),
    ("Clinic Assistant", False),
    ("Bus Driver", False),
])
def test_nurse_role_titles(title, want):
    assert scraper.is_nurse_role(title) is want


def test_internal_only_postings_are_not_open_jobs():
    assert scraper.INTERNAL_ONLY_RX.search("Registered Nurse-ACE (Internal Only)")
    assert scraper.INTERNAL_ONLY_RX.search("School Nurse - Internal Applicants Only")
    assert not scraper.INTERNAL_ONLY_RX.search("Registered Nurse (RN)")


# ── Sitemap career site (Aveanna) ───────────────────────────────────────────

def test_sitemap_lists_job_urls_only():
    listed = scraper.parse_sitemap_jobs(_read("aveanna_sitemap.xml"))
    assert len(listed) == 4                                   # the two category pages are left out
    assert listed["167564"] == "https://jobs.aveanna.com/job/167564/home-health-registered-nurse-rn-part-time-nursing-us-wi-madison/"


def test_list_card_gives_the_current_title_town_and_job_ref():
    jobs, total = scraper.parse_sitemap_list_page(_read("aveanna_list.html"), "jobs.aveanna.com", "Aveanna Healthcare")
    assert total == 1268 and len(jobs) == 4
    by_id = {j.job_id: j for j in jobs}
    j = by_id["167564"]
    assert j.title == "Admissions Home Health Registered Nurse RN Part Time"     # the slug says otherwise
    assert (j.city, j.state, j.location) == ("Madison", "WI", "Madison, WI")
    assert j.req_id == "217880" and j.specialty == "Nursing" and j.ats_platform == "SitemapBoard"
    assert j.url == "https://jobs.aveanna.com/job/167564/home-health-registered-nurse-rn-part-time-nursing-us-wi-madison/"
    # retitled since it was first posted: the card's title, never the slug's
    moved = by_id["163784"]
    assert moved.title == "Adult RN - Low Acuity Days Full Time" and "high-acuity" in moved.url
    assert (moved.city, moved.state) == ("Moody", "TX")


def test_job_page_gives_body_type_date_and_the_posted_pay():
    page = _read("aveanna_job.html")
    job = scraper._sm_job_from_page(page, "https://jobs.aveanna.com/job/167564/x/", "167564", "Aveanna Healthcare")
    assert job.title == "Admissions Home Health Registered Nurse RN Part Time"
    assert (job.city, job.state, job.req_id) == ("Madison", "WI", "217880")
    assert job.job_type == "PART_TIME" and job.posted_date == "2026-09-15"
    assert (job.wage_min, job.wage_max, job.wage_unit) == (41.0, 50.0, "hour")
    assert job.description.endswith("Salary: $41.00 - $50.00 per hour") and len(job.description) >= 200
    assert scraper._sm_job_from_page("<html>gone</html>", "u", "1", "Aveanna Healthcare") is None


def _site(monkeypatch, pages):
    """pages: url -> (status, text); every other URL answers 404."""
    asked = []

    async def fake_get(session, url, system, timeout=60):
        asked.append(url)
        return pages.get(url, (404, ""))
    monkeypatch.setattr(scraper, "_sm_get", fake_get)
    return asked


BASE = "https://jobs.aveanna.com"


def _list_with(ids_titles, total):
    cards = "".join(
        f'<div><a href="/job/{i}/slug-{i}/"><h3>{t}</h3></a><dl><dt>Job Ref:</dt><dd>9{i}</dd><br>'
        f'<dt>Location:</dt><dd>Austin, \nTX 78701\n</dd><br><dt>Category:</dt><dd>Nursing</dd></dl></div>'
        for i, t in ids_titles)
    return f"<h2>Found <strong>{total}</strong> jobs</h2>{cards}"


def _sitemap(ids):
    urls = "".join(f"<url><loc>{BASE}/job/{i}/slug-{i}/</loc></url>" for i in ids)
    return f'<?xml version="1.0"?><urlset>{urls}</urlset>'


def test_every_sitemap_job_accounted_for_is_a_complete_crawl(monkeypatch):
    pages = {
        f"{BASE}/sitemap.xml": (200, _sitemap(["1", "2", "3", "4"])),
        f"{BASE}/jobs/": (200, _list_with([("1", "RN A"), ("2", "RN B")], 3)),
        f"{BASE}/jobs/?page_jobs=2": (200, _list_with([("3", "RN C")], 3)),
        # job 4 moved off the list while it was read: its own page has it
        f"{BASE}/job/4/slug-4/": (200, _read("aveanna_job.html")),
    }
    asked = _site(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_sitemap_site(None, "Aveanna Healthcare", ("jobs.aveanna.com", "/jobs/")))
    assert sorted(j.job_id for j in jobs) == ["1", "2", "3", "4"]
    assert "Aveanna Healthcare" in scraper.COMPLETE_SYSTEMS
    assert ("SITEMAP_SITES", "Aveanna Healthcare") in scraper.COMPLETE_FEEDS
    assert asked.count(f"{BASE}/sitemap.xml") == 1 and f"{BASE}/jobs/?page_jobs=3" not in asked


def test_a_closed_job_page_still_counts_as_accounted(monkeypatch):
    pages = {
        f"{BASE}/sitemap.xml": (200, _sitemap(["1", "2"])),
        f"{BASE}/jobs/": (200, _list_with([("1", "RN A")], 1)),
        # job 2's page answers 404: closed after the sitemap was written
    }
    _site(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_sitemap_site(None, "Aveanna Healthcare", ("jobs.aveanna.com", "/jobs/")))
    assert [j.job_id for j in jobs] == ["1"]
    assert "Aveanna Healthcare" in scraper.COMPLETE_SYSTEMS and "Aveanna Healthcare" not in scraper.PARTIAL_SYSTEMS


def test_a_failed_list_page_is_partial(monkeypatch):
    pages = {
        f"{BASE}/sitemap.xml": (200, _sitemap(["1", "2", "3"])),
        f"{BASE}/jobs/": (200, _list_with([("1", "RN A"), ("2", "RN B")], 3)),
        f"{BASE}/jobs/?page_jobs=2": (503, ""),
        f"{BASE}/job/3/slug-3/": (200, _read("aveanna_job.html")),
    }
    _site(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_sitemap_site(None, "Aveanna Healthcare", ("jobs.aveanna.com", "/jobs/")))
    assert len(jobs) == 3
    assert "Aveanna Healthcare" in scraper.PARTIAL_SYSTEMS and "Aveanna Healthcare" not in scraper.COMPLETE_SYSTEMS


def test_more_unlisted_jobs_than_the_gap_cap_is_partial(monkeypatch):
    monkeypatch.setattr(scraper, "SITEMAP_GAP_MAX", 1)
    pages = {
        f"{BASE}/sitemap.xml": (200, _sitemap(["1", "2", "3"])),
        f"{BASE}/jobs/": (200, _list_with([("1", "RN A")], 1)),
        f"{BASE}/job/2/slug-2/": (200, _read("aveanna_job.html")),
    }
    asked = _site(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_sitemap_site(None, "Aveanna Healthcare", ("jobs.aveanna.com", "/jobs/")))
    assert sorted(j.job_id for j in jobs) == ["1", "2"]
    assert f"{BASE}/job/3/slug-3/" not in asked
    assert "Aveanna Healthcare" in scraper.PARTIAL_SYSTEMS and "Aveanna Healthcare" not in scraper.COMPLETE_SYSTEMS


def test_no_sitemap_reads_nothing(monkeypatch):
    asked = _site(monkeypatch, {f"{BASE}/sitemap.xml": (503, "")})
    assert asyncio.run(scraper.scrape_sitemap_site(None, "Aveanna Healthcare", ("jobs.aveanna.com", "/jobs/"))) == []
    assert asked == [f"{BASE}/sitemap.xml"] and "Aveanna Healthcare" not in scraper.COMPLETE_SYSTEMS


def test_detail_fills_body_and_pay(monkeypatch):
    _site(monkeypatch, {"https://jobs.aveanna.com/job/167564/x/": (200, _read("aveanna_job.html"))})
    job = scraper.Job(title="RN", hospital_system="Aveanna Healthcare", hospital_name="Aveanna Healthcare",
                      city="Madison", state="WI", location="Madison, WI", specialty="Nursing", job_type="",
                      url="https://jobs.aveanna.com/job/167564/x/", job_id="167564", posted_date="",
                      description="", ats_platform="SitemapBoard")
    assert asyncio.run(scraper._sitemap_detail(None, job)) is True
    assert job.wage_unit == "hour" and job.posted_date == "2026-09-15" and len(job.description) >= 200


# ── NEOGOV state and county boards ──────────────────────────────────────────

def _ng(name, system, agency, state, city=""):
    cfg = scraper.NEOGOV_AGENCIES[system]
    return scraper._parse_neogov_page(_read(name), system, agency, state, city, "", None, opts=cfg[5],
                                      skip_dept=scraper._ng_sibling_depts(system, agency))


def test_state_board_keeps_nurse_titles_with_their_facility_and_town():
    jobs, total, cards = _ng("neogov_michigan_nurse.html", "State of Michigan", "michigan", "MI")
    assert total == 39 and cards == 3
    assert [j.title for j in jobs] == ["Licensed Practical Nurse - E9", "Registered Nurse Manager 13"]
    lpn, rn = jobs
    assert (lpn.hospital_system, lpn.hospital_name) == ("State of Michigan", "Kalamazoo Psychiatric Hospital")
    assert (lpn.city, lpn.state) == ("Kalamazoo", "MI")
    assert lpn.job_type == "Permanent Full Time"
    assert "Biweekly" in lpn.description           # the card's pay line, read by the pay parser downstream
    assert rn.hospital_name == "Huron Valley Correctional Complex" and rn.city == "Ypsilanti"
    assert (rn.wage_min, rn.wage_max, rn.wage_unit) == (42.63, 58.77, "hour")


def test_county_line_is_not_a_town_and_the_title_names_the_facility():
    jobs, total, cards = _ng("neogov_pabureau_nurse.html", "Commonwealth of Pennsylvania", "pabureau", "PA")
    assert cards == 2 and [j.title for j in jobs] == ["Registered Nurse Supervisor - SCI Mercer"]
    j = jobs[0]
    assert j.hospital_name == "SCI Mercer" and j.city == "" and j.state == "PA"      # card says "Mercer County, PA"
    assert (j.wage_min, j.wage_max, j.wage_unit) == (98144.0, 127834.0, "year")


def test_facility_names():
    f = scraper._ng_facility
    assert f("Health and Human Services  - Caro Center", "x") == "Caro Center"
    assert f("MDHHS-Southeast Michigan Psychiatric Hospital – Child and Adolescent", "x") == "Southeast Michigan Psychiatric Hospital"
    assert f("Department of Corrections", "Licensed Practical Nurse - SCI Laurel Highlands (Multiple Vacancies)") == "SCI Laurel Highlands"
    assert f("Department of Human Services", "Registered Nurse - Warren State Hospital (2nd & 3rd Shift)") == "Warren State Hospital"
    assert f("Department of Health", "Community Health Nurse") == ""
    assert f("Health and Human Services - Counties", "Nurse") == ""


def test_departments_another_entry_keeps_are_left_to_it():
    # LA County's HEALTH SERVICES rows belong to the DHS hospital entry
    rx = scraper._ng_sibling_depts("Los Angeles County", "lacounty")
    assert rx.search("HEALTH SERVICES") and not rx.search("PUBLIC HEALTH")
    page = _read("neogov_michigan_nurse.html").replace(
        'data-department-name="Health and Human Services - Kalamazoo Psychiatric Hospital"',
        'data-department-name="HEALTH SERVICES"')
    jobs, _, _ = scraper._parse_neogov_page(page, "Los Angeles County", "lacounty", "CA", "Los Angeles", "", None,
                                            opts=scraper.NG_NURSES, skip_dept=rx)
    assert [j.title for j in jobs] == ["Registered Nurse Manager 13"]
    # the hospital entries read exactly as before (no options, no skip)
    assert all(len(cfg) == 5 for s, cfg in scraper.NEOGOV_AGENCIES.items() if cfg[3])


def test_an_entry_can_leave_a_department_to_another_adapter(monkeypatch):
    # Erie County's ECMC department: ECMC is read from its own UKG board
    page = _read("neogov_michigan_nurse.html").replace(
        'data-department-name="Health and Human Services - Kalamazoo Psychiatric Hospital"',
        'data-department-name="ERIE COUNTY MEDICAL CENTER (ECMC)"').replace(
        'id="job-postings-number">39', 'id="job-postings-number">3')

    async def fetch(session, agency, page_no, keyword=""):
        return 200, page
    monkeypatch.setattr(scraper, "_neogov_fetch_page", fetch)
    jobs = asyncio.run(scraper.scrape_neogov(None, "Erie County, NY", scraper.NEOGOV_AGENCIES["Erie County, NY"]))
    assert [j.title for j in jobs] == ["Registered Nurse Manager 13"]


def test_state_board_reads_the_keyword_search_and_claims_against_its_count(monkeypatch):
    asked = []
    page = _read("neogov_michigan_nurse.html").replace('id="job-postings-number">39', 'id="job-postings-number">3')

    async def fetch(session, agency, page_no, keyword=""):
        asked.append((agency, page_no, keyword))
        return 200, page
    monkeypatch.setattr(scraper, "_neogov_fetch_page", fetch)
    jobs = asyncio.run(scraper.scrape_neogov(None, "State of Michigan", scraper.NEOGOV_AGENCIES["State of Michigan"]))
    assert asked == [("michigan", 1, "nurse")] and len(jobs) == 2
    assert "State of Michigan" in scraper.COMPLETE_SYSTEMS


def test_hospital_entries_still_read_the_whole_board(monkeypatch):
    asked = []

    async def fetch(session, agency, page_no):          # the old three-argument shape still works
        asked.append((agency, page_no))
        return 200, '<span id="job-postings-number">0</span>'
    monkeypatch.setattr(scraper, "_neogov_fetch_page", fetch)
    asyncio.run(scraper.scrape_neogov(None, "Riverside University Health System",
                                      scraper.NEOGOV_AGENCIES["Riverside University Health System"]))
    assert asked == [("riverside", 1)]


def test_new_agency_labels_do_not_collide():
    new = [s for s, c in scraper.NEOGOV_AGENCIES.items() if len(c) > 5]
    assert len(new) >= 25
    others = [scraper.WORKDAY_TENANTS, scraper.ICIMS_ORGS, scraper.SF_RMK_BOARDS, scraper.JOBAPS_SITES,
              scraper.TALENTBREW_ORGS, scraper.ORACLE_ORGS, scraper.PHENOM_ORGS]
    for label in new:
        assert not any(label in b for b in others), label
        opts = scraper.NEOGOV_AGENCIES[label][5]
        assert opts["keyword"] == "nurse" and opts["nurse_only"] and opts["card_town"] and opts["facility"]


# ── AppliTrack school district boards ───────────────────────────────────────

FW = ("fortworth", "TX", "Fort Worth")


def test_district_list_keeps_nurse_postings():
    jobs, on_board, whole = scraper.parse_applitrack_list(_read("applitrack_list.js"), "Fort Worth ISD", FW)
    assert on_board == 3 and whole
    assert [j.title for j in jobs] == ["School Nurse (Updated)", "Nurse Specialist (Two Positions)"]
    j = jobs[0]
    assert (j.job_id, j.posted_date, j.city, j.state) == ("6873", "2025-03-12", "Fort Worth", "TX")
    assert j.hospital_system == j.hospital_name == "Fort Worth ISD" and j.ats_platform == "AppliTrack"
    assert j.url == ("https://www.applitrack.com/fortworth/onlineapp/default.aspx?AppliTrackJobId=6873"
                     "&AppliTrackLayoutMode=detail&AppliTrackViewPosting=1")
    assert j.description == "Location: To Be Determined"
    assert scraper.applitrack_posting_url(j.url) == ("https://www.applitrack.com/fortworth/onlineapp/jobpostings/"
                                                     "Output.asp?AppliTrackJobId=6873&AppliTrackLayoutMode=detail"
                                                     "&AppliTrackViewPosting=1")


def test_internal_only_and_cut_lists():
    page = _read("applitrack_list.js").replace("Nurse Specialist (Two Positions)", "Registered Nurse-ACE (Internal Only)")
    jobs, on_board, whole = scraper.parse_applitrack_list(page, "Fort Worth ISD", FW)
    assert [j.title for j in jobs] == ["School Nurse (Updated)"] and on_board == 3
    cut = _read("applitrack_list.js").split("</tbody>")[0]
    assert scraper.parse_applitrack_list(cut, "Fort Worth ISD", FW)[2] is False
    empty = ("document.write('<div class=label><b>At this time we do not have any openings.</b></div>"
             "<div id=\\'AppliTrackListContent\\'></div><noscript>Powered by Aspex Solutions</noscript>');")
    assert scraper.parse_applitrack_list(empty, "Spring ISD", ("springisd", "TX", "Houston")) == ([], 0, True)


def test_district_host_other_than_www():
    jobs, _, _ = scraper.parse_applitrack_list(_read("applitrack_list.js"), "X ISD", ("x", "TX", "Town", "phl.applitrack.com"))
    assert jobs[0].url.startswith("https://phl.applitrack.com/x/onlineapp/default.aspx?AppliTrackJobId=")


def test_posting_text_type_and_date():
    body, ptype, posted = scraper.applitrack_posting(_read("applitrack_posting.js"))
    assert ptype == "Nurse" and posted == "2025-03-12"
    assert body.startswith("School Nurse") and "Salary Range: $63,000-$75,903" in body
    assert body.endswith("Location: To Be Determined") and "addthis" not in body
    assert scraper.extract_posted_wage(body, "") == (63000.0, 75903.0, "year")
    # a posting that is only an attached file has no text of its own
    body, ptype, posted = scraper.applitrack_posting(_read("applitrack_posting_attachment.js"))
    assert (body, ptype, posted) == ("", "School Nurse", "2026-03-02")


def test_scrape_claims_the_whole_board_and_detail_fills(monkeypatch):
    async def board_get(session, url, label, system, timeout=40):
        if "Output.asp?all=1" in url:
            return 200, _read("applitrack_list.js")
        if "AppliTrackJobId=6873" in url:
            return 200, _read("applitrack_posting.js")
        return 200, _read("applitrack_posting_attachment.js")
    monkeypatch.setattr(scraper, "_board_get", board_get)
    jobs = asyncio.run(scraper.scrape_applitrack(None, "Fort Worth ISD", FW))
    assert len(jobs) == 2 and "Fort Worth ISD" in scraper.COMPLETE_SYSTEMS
    assert ("APPLITRACK_BOARDS", "Fort Worth ISD") in scraper.COMPLETE_FEEDS
    nurse, spec = jobs
    assert asyncio.run(scraper._applitrack_detail(None, nurse)) is True
    assert "Salary Range" in nurse.description and nurse.specialty == "Nurse"
    assert asyncio.run(scraper._applitrack_detail(None, spec)) is False      # attachment only: list teaser kept
    assert spec.description == "Location: HEALTH SERVICES" and spec.specialty == "School Nurse"


# ── Wiring ──────────────────────────────────────────────────────────────────

def test_new_feeds_are_wired():
    assert {"SITEMAP_SITES", "APPLITRACK_BOARDS"} <= set(scraper.FEED_CONFIGS)
    assert {"SitemapBoard", "AppliTrack"} <= set(scraper.KNOWN_BODY_PLATFORMS)
    src = pathlib.Path(scraper.__file__).read_text(encoding="utf-8")
    run_all = src[src.index("async def run_all()"):src.index("def scrape() -> list[dict]:")]
    assert "run_sitemap_sites(direct_session)" in run_all and "run_applitrack(proxy_session)" in run_all
    assert set(scraper.configured_feed_map()) >= set(scraper.SITEMAP_SITES) | set(scraper.APPLITRACK_BOARDS)
    for system, cfg in scraper.APPLITRACK_BOARDS.items():
        assert re.fullmatch(r"[A-Za-z0-9]+", cfg[0]) and re.fullmatch(r"[A-Z]{2}", cfg[1]) and cfg[2], system
