"""Coverage round 3 (2026-10-04, builder B): iCIMS / Jibe / TalentBrew /
Talemetry / Phenom / HCTS / HealthcareSource / ADP CX / SelectMinds / Taleo BE
/ UKG / Findly-Google tenants, the SelectMinds HTML-list rewrite, and the
state fixes for scraped systems (Banner bullet fields, Cleveland Clinic
Florida sites, Northwell's Connecticut towns, Bryan's default). Offline: the
fixtures under tests/fixtures/cov3 were trimmed from that day's probes."""
import asyncio
import json
import os
import re

import scraper
from scraper import Job

from conftest import FIXTURES

COV3 = os.path.join(FIXTURES, "cov3")


def _read(name):
    with open(os.path.join(COV3, name), encoding="utf-8") as f:
        return f.read()


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status = body, status

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return self._body


class _Ctx:
    def __init__(self, body, status=200):
        self.resp = _Resp(body, status)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


def _fake_req(monkeypatch, answer):
    calls = []

    def req(session, method, url, **kw):
        calls.append((method, url, kw.get("params") or {}, kw.get("json") or {}))
        return _Ctx(*answer(method, url, kw))

    async def no_wait():
        return None

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    return calls


# ── HCTS: Samaritan's <h4 style class> card title ───────────────────────────
def test_hcts_samaritan_h4_with_attributes_parses_facility_and_state():
    jobs = scraper._parse_hcts_page(_read("hcts_samaritan_cards.html"), "Samaritan Health Services", "samhealthjobs", "OR")
    assert len(jobs) == 2
    j = jobs[0]
    assert j.title == "STARS Intake Specialist-Certified" and j.job_id == "1986921"
    assert j.url == "https://samhealthjobs.hctsportals.com/jobs/1986921-stars-intake-specialist-certified"
    assert j.hospital_name == "Samaritan Pacific Communities Hospital"
    assert (j.city, j.state) == ("Newport", "OR") and j.ats_platform == "HCTS"
    # The East Alabama (<h4>) and UMC El Paso (<h2>) shapes still match.
    assert scraper._HCTS_TITLE_RE.search('<h4><a href="https://x.hctsportals.com/jobs/1-rn">RN</a></h4>')
    assert scraper._HCTS_TITLE_RE.search('<h2><a href="/jobs/22-rn">RN</a></h2>')


def test_hcts_samaritan_portals_configured_with_oregon_default():
    assert scraper.HCTS_PORTALS["Samaritan Health Services"] == ("samhealthjobs", "OR")
    assert scraper.HCTS_PORTALS["Samaritan Health Services (Clinicians)"] == ("samhealthclinicianjobs", "OR")
    assert scraper.HOSPITAL_SYSTEM_ALIASES["Samaritan Health Services (Clinicians)"] == "Samaritan Health Services"


# ── SelectMinds: HTML job_list_row cards, keep rule, paging ────────────────
def test_selectminds_cards_parse_title_location_category_and_facility():
    base = "https://mainline.referrals.selectminds.com"
    jobs = scraper._parse_selectminds_page(_read("selectminds_mainline_cards.html"), "Main Line Health", base)
    assert [j.job_id for j in jobs] == ["29120", "29106"]
    director, lab = jobs
    assert director.title == "System Director, Real Estate" and (director.city, director.state) == ("Radnor", "PA")
    assert director.hospital_name == "Main Line Health" and director.ats_platform == "SelectMinds"
    assert lab.title == "Lab Assistant, Outreach" and lab.url == f"{base}/jobs/lab-assistant-outreach-29106"
    assert (lab.city, lab.state, lab.location) == ("Wynnewood", "PA", "Wynnewood, PA")
    assert lab.specialty == "Laboratory" and lab.hospital_name == "Mirmont Treatment Center"
    assert lab.description.startswith("Could you be our next Lab Assistant") and "Requisition #: 78981" in lab.description
    assert scraper._parse_selectminds_page("", "Main Line Health", base) == []
    # a teaser that names a hospital gives the row that hospital
    card = ('<div id="job_list_1" class="job_list_row"><p><a href="/jobs/cardiac-sonographer-1" class="job_link">Cardiac Sonographer</a></p>'
            '<p class="jlr_location"><a href="/x" class="location"><span class="loc_icon entypo">&#128269;</span>Paoli, Pennsylvania, United States</a></p>'
            '<p class="jlr_description">Cardiac Sonographer - Paoli Hospital. Sign-on bonus.</p></div>')
    sono = scraper._parse_selectminds_page(card, "Main Line Health", base)[0]
    assert (sono.city, sono.state, sono.hospital_name) == ("Paoli", "PA", "Paoli Hospital")
    assert sono.url == f"{base}/jobs/cardiac-sonographer-1"


def test_selectminds_keep_rule_only_filters_the_iowa_board():
    def job(title, cat="", desc=""):
        return Job(title=title, hospital_system="x", hospital_name="x", city="", state="", location="",
                   specialty=cat, job_type="", url="u", job_id="1", posted_date="", description=desc,
                   ats_platform="SelectMinds")
    ia = "University of Iowa Hospitals and Clinics"
    assert scraper._selectminds_keep(ia, job("Staff Nurse")) is True
    assert scraper._selectminds_keep(ia, job("Custodian", desc="UI Health Care environmental services")) is True
    assert scraper._selectminds_keep(ia, job("Dining Services Associate", cat="Dining")) is False
    assert scraper._selectminds_keep("Main Line Health", job("Dining Services Associate")) is True


def test_selectminds_scrape_pages_latest_jobs_until_no_new_card(monkeypatch):
    page_html = _read("selectminds_mainline_cards.html")

    def answer(method, url, kw):
        assert url.endswith("/latest-jobs")
        return (page_html, 200)   # every page repeats the same two cards

    calls = _fake_req(monkeypatch, answer)
    jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert len(jobs) == 2
    assert [c[2]["page"] for c in calls] == ["1", "2"]       # page 2 added nothing -> stop, no /jobs/search fallback


def test_selectminds_scrape_falls_back_to_jobs_search_when_latest_is_empty(monkeypatch):
    page_html = _read("selectminds_mainline_cards.html")

    def answer(method, url, kw):
        if url.endswith("/latest-jobs"):
            return ("<html></html>", 200)
        return (page_html if kw["params"]["page"] == "1" else "<html></html>", 200)

    calls = _fake_req(monkeypatch, answer)
    jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert len(jobs) == 2
    assert [c[1].rsplit("/", 1)[-1] or "jobs/search" for c in calls][:1] == ["latest-jobs"]
    assert any(c[1].endswith("/jobs/search/") for c in calls)


def test_selectminds_tenants_configured():
    assert scraper.SELECTMINDS_ORGS["Main Line Health"] == "mainline"
    assert scraper.SELECTMINDS_ORGS["University of Iowa Hospitals and Clinics"] == "uiowa"


# ── Taleo BE: ISO "US-AZ" state ─────────────────────────────────────────────
def test_taleo_be_tmc_rss_strips_the_us_prefix():
    jobs = scraper._parse_taleo_be_rss(_read("taleo_be_tmc_rss.xml"), "TMC Healthcare",
                                       "https://phe.tbe.taleo.net/phe01", "TMCAZ", "38", "AZ")
    assert len(jobs) == 2
    assert all(j.state == "AZ" and j.city == "Tucson" for j in jobs)
    assert all(j.url.startswith("https://phe.tbe.taleo.net/phe01/ats/careers/v2/viewRequisition?org=TMCAZ&cws=38&rid=") for j in jobs)
    assert scraper.TALEO_BE_ORGS["TMC Healthcare"] == ("https://phe.tbe.taleo.net/phe01", "TMCAZ", "38", "AZ")


def test_talentbrew_hmh_cards_with_a_save_button_keep_their_titles(monkeypatch):
    """Integrator smoke 2026-10-04: jobs.hackensackmeridianhealth.org answered
    1,695 jobs, 15 cards a page, but every card parsed with an empty title
    (a "Save for Later" <button> sits between the anchor and the <h2>) and
    the tenant kept 0 rows. Two live cards; the Section 6 names answer empty
    so the generic-name retry serves them."""
    html = _read("talentbrew_hmh_cards.html")

    def answer(method, url, kw):
        p = kw.get("params") or {}
        if url.endswith("/results"):
            if p.get("SearchResultsModuleName") == "Search Results":
                return (json.dumps({"results": html, "hasJobs": True, "hasContent": True}), 200)
            return (json.dumps({"results": "", "hasJobs": False, "hasContent": False}), 200)
        return ("", 200)

    calls = _fake_req(monkeypatch, answer)
    jobs = asyncio.run(scraper.scrape_talentbrew(None, "Hackensack Meridian Health",
                                                 "https://jobs.hackensackmeridianhealth.org/search-jobs", 15))
    assert [(j.title, j.hospital_name, j.city, j.state, j.job_id) for j in jobs] == [
        ("Endocrinologist", "HMHMG SPECIALTY CARE 570", "North Bergen", "NJ", "95340279344"),
        ("Endocrinologist", "HACKENSACK UNIV. MEDICAL GROUP", "Hackensack", "NJ", "88618122928"),
    ]
    assert jobs[0].url == "https://jobs.hackensackmeridianhealth.org/job/north-bergen/endocrinologist/19511/95340279344"
    assert [c[2].get("SearchResultsModuleName") for c in calls if c[1].endswith("/results")] == \
        ["Section 6 - Search Results List", "Search Results"]


def test_taleo_be_rss_survives_invalid_xml_character_references():
    """The live TMC feed (2026-10-04 integrator smoke) carries "&#11;" in a
    body: ElementTree raised "reference to invalid character number" and the
    tenant banked 0 of 356 items. Bad references and raw control characters
    are dropped; valid ones (tab, &#65; = "A", &#x2019;) survive."""
    xml = _read("taleo_be_tmc_rss.xml")
    poisoned = re.sub(r"(<item>\s*<title>[^<]*)", lambda m: m.group(1) + " &#11;&#x1;\x0b&#65;&#x2019;", xml, count=1)
    assert "&#11;" in poisoned
    jobs = scraper._parse_taleo_be_rss(poisoned, "TMC Healthcare",
                                       "https://phe.tbe.taleo.net/phe01", "TMCAZ", "38", "AZ")
    assert len(jobs) == 2
    assert jobs[0].title.endswith("A’") and "\x0b" not in jobs[0].title
    assert scraper._xml_sanitize("a\tb&#9;&#1114111;&#1114112;&#xD800;") == "a\tb&#9;&#1114111;"


# ── Jibe: facility from a tags list ─────────────────────────────────────────
def test_jibe_facility_tag_and_site_code_location_name():
    rows = json.loads(_read("jibe_cne_yale_rows.json"))
    assert scraper._jibe_facility("Care New England Health System", rows["cne"]) == "Kent Hospital"
    assert rows["cne"]["location_name"].startswith("Kent Hospital, 455")   # the address form never becomes the name
    # Yale's location_name is a site code; without a tag or JIBE_FACILITY_NAME the system stays.
    assert scraper._jibe_facility("Yale New Haven Health System", rows["yale"]) == "Yale New Haven Health System"
    assert scraper._jibe_facility("Care New England Health System", {"tags2": [], "location_name": "x"}) == "Care New England Health System"
    assert scraper._jibe_facility("Garnet Health", {"location_name": "Garnet Health Medical Center"}) == "Garnet Health Medical Center"
    city, st = scraper.parse_city_state(f"{rows['yale']['city']}, {rows['yale']['state']}")
    assert (city, st) == ("Bridgeport", "CT")


# ── Workday: Banner's bullet fields, the facet group label, Cleveland Clinic FL
def test_wd_bullet_location_reads_state_and_town():
    assert scraper._wd_bullet_location(["R4446997", "Arizona", "Sun City West"]) == ("Sun City West", "AZ")
    assert scraper._wd_bullet_location(["R4452218", "Arizona", "Arizona"]) == ("", "AZ")
    assert scraper._wd_bullet_location(["R4452218", "Colorado", "Greeley"]) == ("Greeley", "CO")
    assert scraper._wd_bullet_location(["355366"]) == ("", "")
    assert scraper._wd_bullet_location([]) == ("", "")
    assert scraper._wd_bullet_location(["R1", "Wyoming", "Posted 30+ Days Ago"]) == ("", "WY")


def test_banner_rows_take_their_bullet_state_not_the_phoenix_default(monkeypatch):
    page = json.loads(_read("workday_banner_page.json"))

    def answer(method, url, kw):
        body = kw.get("json") or {}
        if body.get("limit") == 1:
            return (page, 200)
        if body.get("offset", 0) == 0 and not body.get("appliedFacets"):
            return (page, 200)
        return ({"total": 0, "jobPostings": []}, 200)

    _fake_req(monkeypatch, answer)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    jobs = asyncio.run(scraper.scrape_workday(None, "Banner Health", scraper.WORKDAY_TENANTS["Banner Health"]))
    assert len(jobs) == 3
    by_loc = {j.location: j for j in jobs}
    webb = next(j for loc, j in by_loc.items() if "Del Webb" in loc)
    assert (webb.city, webb.state) == ("Sun City West", "AZ")
    multi = by_loc["2 Locations"]
    assert multi.state == "AZ" and multi.city == ""        # no town in the bullets; the default fills Phoenix later
    assert all(j.state for j in jobs)                        # nothing left for the blank-state pass


def test_wd_facet_group_labelled_state_is_detected(monkeypatch):
    page = json.loads(_read("workday_banner_page.json"))
    groups = scraper._wd_facet_groups(page)
    assert ("locationHierarchy2", "State") in {(p, label) for p, label, _ in groups}
    state_ids = {v["descriptor"]: v["id"] for _, label, vals in groups if label == "State" for v in vals}

    def answer(method, url, kw):
        body = kw.get("json") or {}
        if body.get("limit") == 1:
            return (page, 200)
        facets = body.get("appliedFacets") or {}
        if "locationHierarchy2" in facets:
            vid = facets["locationHierarchy2"][0]
            desc = next(d for d, i in state_ids.items() if i == vid)
            return ({"jobPostings": [{"externalPath": f"/job/{desc}/1"}]}, 200)
        return ({"jobPostings": []}, 200)

    _fake_req(monkeypatch, answer)
    path_city, path_state = asyncio.run(scraper._wd_location_facets(None, "https://wd/jobs", "Banner Health"))
    assert path_state["/job/Colorado/1"] == "CO" and path_state["/job/Arizona/1"] == "AZ"
    assert "Arizona" not in path_city.values()            # the State group is never read as the city group


def test_cleveland_clinic_florida_sites_take_florida():
    f = scraper._wd_facility
    assert f("Cleveland Clinic", "Martin Health North", "Martin Health North", "") == ("Cleveland Clinic Martin North Hospital", "Stuart", "FL")
    assert f("Cleveland Clinic", "Florida Weston Hospital", "Florida Weston Hospital", "") == ("Cleveland Clinic Hospital", "Weston", "FL")
    assert f("Cleveland Clinic", "Indian River Hospital", "Indian River Hospital", "") == ("Cleveland Clinic Indian River Hospital", "Vero Beach", "FL")
    assert f("Cleveland Clinic", "Vero Radiology Associates", "Vero Radiology Associates", "") == (None, "Vero Beach", "FL")
    assert f("Cleveland Clinic", "Cleveland Clinic Nevada", "Cleveland Clinic Nevada", "") == (None, "Las Vegas", "NV")
    # Ohio sites are untouched and keep the Cleveland OH default downstream.
    assert f("Cleveland Clinic", "Mercy Hospital", "Mercy Hospital", "") == (None, "Mercy Hospital", "")


def test_cleveland_clinic_listing_rows_split_ohio_and_florida(monkeypatch):
    page = json.loads(_read("workday_ccf_page.json"))

    def answer(method, url, kw):
        body = kw.get("json") or {}
        if body.get("offset", 0) == 0 and not body.get("appliedFacets") and body.get("limit") != 1:
            return (page, 200)
        return ({"total": 0, "jobPostings": [], "facets": []}, 200)

    _fake_req(monkeypatch, answer)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    jobs = asyncio.run(scraper.scrape_workday(None, "Cleveland Clinic", scraper.WORKDAY_TENANTS["Cleveland Clinic"]))
    by_loc = {j.location: j for j in jobs}
    martin = by_loc["Martin Health North"]
    assert (martin.hospital_name, martin.city, martin.state) == ("Cleveland Clinic Martin North Hospital", "Stuart", "FL")
    d = scraper.normalize_job(by_loc["Mercy Hospital"])
    assert d["state"] == "OH"                                # the tenant default still backs the Ohio sites


# ── normalize: Northwell's Connecticut towns, Bryan's Nebraska default ──────
def _job(system, city="", state="", hospital_name=None, loc=""):
    return Job(title="RN", hospital_system=system, hospital_name=hospital_name or system, city=city, state=state,
               location=loc, specialty="", job_type="", url="https://x/1", job_id="1", posted_date="",
               description="", ats_platform="Oracle HCM")


def test_northwell_nuvance_towns_are_connecticut():
    for label in ("Northwell Health", "Northwell Health (CX_1)"):
        d = scraper.normalize_job(_job(label, city="Danbury", loc="Danbury"))
        assert (d["city"], d["state"]) == ("Danbury", "CT"), label
    d = scraper.normalize_job(_job("Northwell Health", city="Manhasset", loc="Manhasset"))
    assert d["state"] == "NY"
    d = scraper.normalize_job(_job("Northwell Health (CX_3)", city="Brooklyn", loc="Brooklyn"))
    assert d["state"] == "NY"                                # the site labels now reach the NY default too


def test_bryan_health_blank_rows_default_to_lincoln_ne():
    d = scraper.normalize_job(_job("Bryan Health", hospital_name="Bryan Health"))
    assert (d["city"], d["state"]) == ("Lincoln", "NE")
    assert "Bryan Health" in scraper.PHENOM_ORGS


# ── configs: every new tenant once, labelled, with a default state ──────────
NEW_LABELS = {
    "Community Medical Centers": (scraper.ICIMS_ORGS, "CA"),
    "Tallahassee Memorial Healthcare": (scraper.ICIMS_ORGS, "FL"),
    "Yale New Haven Health System": (scraper.JIBE_SITES, "CT"),
    "Care New England Health System": (scraper.JIBE_SITES, "RI"),
    "MemorialCare": (scraper.TALENTBREW_ORGS, "CA"),
    "Hackensack Meridian Health": (scraper.TALENTBREW_ORGS, "NJ"),
    "Luminis Health": (scraper.GREENHOUSE_ORGS, "MD"),
    "LifeBridge Health": (scraper.TALEMETRY_SITES, "MD"),
    "Asante Health System": (scraper.TALEMETRY_SITES, "OR"),
    "UAB Health System": (scraper.PHENOM_ORGS, "AL"),
    "Health First": (scraper.PHENOM_ORGS, "FL"),
    "Samaritan Health Services": (scraper.HCTS_PORTALS, "OR"),
    "Aultman Health Foundation": (scraper.HEALTHCARESOURCE_ORGS, "OH"),
    "Willis Knighton Health System": (scraper.HEALTHCARESOURCE_ORGS, "LA"),
    "Cabell Huntington Hospital": (scraper.ADPCX_ORGS, "WV"),
    "Ephraim Mcdowell Health": (scraper.ADPCX_ORGS, "KY"),
    "Main Line Health": (scraper.SELECTMINDS_ORGS, "PA"),
    "University of Iowa Hospitals and Clinics": (scraper.SELECTMINDS_ORGS, "IA"),
    "TMC Healthcare": (scraper.TALEO_BE_ORGS, "AZ"),
    "Pipeline Health": (scraper.UKG_ORGS, "CA"),
    "Scripps Health": (scraper.FINDLY_GOOGLE_ORGS, "CA"),
}
BOARDS = [scraper.WORKDAY_TENANTS, scraper.PHENOM_ORGS, scraper.JIBE_SITES, scraper.CSOD_ORGS, scraper.ICIMS_ORGS,
          scraper.ORACLE_ORGS, scraper.INFOR_ORGS, scraper.HEALTHCARESOURCE_ORGS, scraper.TALEMETRY_SITES,
          scraper.SF_RMK_BOARDS, scraper.HCTS_PORTALS, scraper.PAYCOM_ORGS, scraper.TALENTBREW_ORGS,
          scraper.GREENHOUSE_ORGS, scraper.ADP_ORGS, scraper.ADPCX_ORGS, scraper.SELECTMINDS_ORGS,
          scraper.TALEO_BE_ORGS, scraper.UKG_ORGS, scraper.FINDLY_CWS_ORGS, scraper.FINDLY_GOOGLE_ORGS,
          scraper.WORKABLE_ORGS, scraper.SMARTRECRUITERS_ORGS]


def test_each_new_label_is_on_exactly_one_board_with_its_home_state():
    for label, (board, st) in NEW_LABELS.items():
        assert label in board, label
        assert sum(label in b for b in BOARDS) == 1, label
        assert label not in scraper.HOSPITAL_SYSTEM_ALIASES, label
        assert scraper.SYSTEM_LOCATION_DEFAULTS[label.lower()][1] == st, label


def test_tenant_lines_carry_the_probed_identifiers():
    assert scraper.ICIMS_ORGS["Community Medical Centers"] == "careers-communitymedical.icims.com"
    assert scraper.ICIMS_ORGS["Tallahassee Memorial Healthcare"] == "careers-tmh.icims.com"
    assert scraper.JIBE_SITES["Yale New Haven Health System"] == "https://jobs.ynhhs.org"
    assert scraper.JIBE_SITES["Care New England Health System"] == "https://careers.carenewengland.org"
    assert scraper.JIBE_FACILITY_TAG["Care New England Health System"] == "tags2"
    assert scraper.TALENTBREW_ORGS["MemorialCare"] == ("https://careers.memorialcare.org/search-jobs", 15)
    assert scraper.TALENTBREW_ORGS["Hackensack Meridian Health"] == ("https://jobs.hackensackmeridianhealth.org/search-jobs", 15)
    assert scraper.GREENHOUSE_ORGS["Luminis Health"] == "luminishealth"
    assert scraper.TALEMETRY_SITES["LifeBridge Health"] == "https://jobs.lifebridgehealth.org"
    assert scraper.TALEMETRY_SITES["Asante Health System"] == "https://jobs.asante.org"
    assert scraper.PHENOM_ORGS["UAB Health System"] == "https://careers.uabmedicine.org"
    assert scraper.PHENOM_ORGS["Health First"] == "https://www.careers.hf.org"
    assert scraper.PHENOM_ORG_CODES["UAB Health System"] == "UHSUHSUS" and scraper.PHENOM_ORG_CODES["Health First"] == "HFDSUS"
    assert scraper.HEALTHCARESOURCE_ORGS["Aultman Health Foundation"] == "aultman"
    assert scraper.HEALTHCARESOURCE_ORGS["Willis Knighton Health System"] == "wkhs"
    assert scraper.ADPCX_ORGS["Cabell Huntington Hospital"] == ("mhnetwork", "WV")
    assert scraper.ADPCX_ORGS["Ephraim Mcdowell Health"] == ("ephraimmcdowell", "KY")
    assert scraper.UKG_ORGS["Pipeline Health"][0] == "https://pipeline.rec.pro.ukg.net/PIP1500PPLN"
    assert scraper.UKG_ORGS["Pipeline Health"][1:] == ("663afd70-7892-48da-a106-a2deaafde171", "CA")
    assert scraper.FINDLY_GOOGLE_ORGS["Scripps Health"] == ("c7ae2ec3-a75f-4fad-89a4-0c5d5f0f5308", [], "https://careers.scripps.org")
    # Community Hospital Corporation's Workable board is corporate only (9 executive postings): not configured.
    assert "Community Hospital Corporation" not in scraper.WORKABLE_ORGS
    # The AHRQ "Marshall Health Network" board is ADP CX, not a WorkforceNow career center.
    assert not any("Marshall" in k or "Cabell" in k for k in scraper.ADP_ORGS)


def test_pipeline_ukg_row_names_the_hospital():
    opp = {"Id": "c77d5c14", "Title": "Director of Risk Management", "FullTime": True,
           "Locations": [{"LocalizedName": "Memorial Hospital of Gardena",
                          "Address": {"City": "Gardena", "State": {"Code": "CA"}}}]}
    j = scraper._ukg_job(opp, "Pipeline Health", *scraper.UKG_ORGS["Pipeline Health"])
    assert (j.hospital_name, j.city, j.state) == ("Memorial Hospital of Gardena", "Gardena", "CA")


def test_adp_cx_mountain_health_row_defaults_to_wv_when_the_address_is_blank():
    j = scraper._adpcx_job({"reqId": "5001229414200", "publishedJobTitle": "LPN II - 3 East", "requisitionLocations": []},
                           "Cabell Huntington Hospital", "mhnetwork", "WV")
    assert j.state == "WV" and j.hospital_system == "Cabell Huntington Hospital"
    assert j.url == "https://myjobs.adp.com/mhnetwork/cx/job-details?reqId=5001229414200"
