# 2026-10-14 (owner: "Yeah we need to fix those now"): the number the employer
# shows people, stored as posting_facts["req_id"] on four more platforms whose
# job_id is a different number:
#   * classic iCIMS portals: the card's <dl class="iCIMS_JobHeaderGroup"> pair
#     ("ID", "Requisition ID", "Job ID", "Req #") or the card header's "ID"
#     span (Catholic Health LI, Dartmouth, Blythedale); job page as fallback;
#   * Jobvite Engage sites (TALEMETRY_SITES): the job page header's "Job ID:" /
#     "Req #:" line, read in the detail pass that already fetches the page;
#   * HCTS portals: the list card's "Req #" (three markups);
#   * SelectMinds: the /latest-jobs card's "Requisition #" (job_external_id).
# Rows of these platforms whose facts are null tonight send a req_id-only
# object (REQ_ID_ONLY_PLATFORMS; the live trigger merges it, sql/113), so the
# number lands whether or not a body came with it. job_id, url and the upsert
# key are unchanged. Fixtures are real 2026-10-14 snippets
# (tests/fixtures/reqid_1014/), each card / page value checked against the
# live job page the same day.
import asyncio
import dataclasses
import json
import os
import re

import pytest

import scraper

D = "reqid_1014"


@pytest.fixture(autouse=True)
def _no_known_bodies(monkeypatch):
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


def _stored(job, n=3000, fh=None):
    """A known-body row for `job` as load_known_bodies returns it."""
    return {"hospital_system": scraper._canon_system(job.hospital_system), "job_id": job.job_id,
            "ats_platform": job.ats_platform, "desc_len": n, "wage_min": None,
            "fv": scraper.FACTS_VERSION if fh else None, "fh": fh}


def _list_night(job):
    """What the upsert sends for a listed row whose stored body is longer
    than the list's copy: _keep_stored_body blanks it, then normalize_job."""
    scraper.set_known_bodies([_stored(job)])
    scraper._keep_stored_body(job.hospital_system, job)
    assert job.description == ""
    return scraper.normalize_job(job)


# ── iCIMS ────────────────────────────────────────────────────────────────────

ICIMS_CARDS = {   # fixture -> [(job_id, number the card and the job page print)]
    "ahmc_healthcare": [("28956", "req28956"), ("28944", "req28944")],
    "appalachian_regional_healthcare": [("39808", "2026-39808"), ("39804", "2026-39804")],
    "blythedale_children_s_hospital": [("1578", "2026-1578"), ("1577", "2026-1577")],
    "catholic_health_long_island": [("75455", "2026-75455"), ("75424", "2026-75424")],
    "community_medical_centers": [("52605", "2026-R6612"), ("52619", "2026-R7037")],
    "covenant_health": [("75588", "4625780"), ("76018", "4641215")],
    "dartmouth_health": [("42692", "2026-42692"), ("42649", "2026-42649")],
    "dch_health_system": [("14351", "2026-14351"), ("13960", "2026-13960")],
    "huntsville_hospital_health_system": [("73735", "2026-73735"), ("75947", "2026-75947")],
    "kettering_health": [("63646", "2026-63646"), ("63572", "2026-63572")],
    "legacy_health": [("49582", "26-49582"), ("49581", "26-49581")],
    "methodist_hospitals": [("14036", "2026-14036"), ("14032", "2026-14032")],
    "midland_health": [("8053", "2026-8053"), ("8037", "2026-8037")],
    "neuropsychiatric_hospitals": [("4398", "2026-4398"), ("4396", "2026-4396")],
    "northside_hospital": [("122294", "2026-122294"), ("122198", "2026-122198")],
    "oakbend_medical_center": [("5573", "2026-5573"), ("5572", "2026-5572")],
    "ohsu": [("42134", "2026-42134"), ("42108", "2026-42108")],
    "prime_healthcare": [("285876", "2026-285876"), ("285874", "2026-285874")],
    "tallahassee_memorial_healthcare": [("22255", "2026-22255"), ("22253", "2026-22253")],
    "tuality_healthcare": [("11085", "2026-11085"), ("11082", "2026-11082")],
    # "Job ID" / "Job Number" equal to job_id: nothing to correct
    "select_medical": [("381322", ""), ("386124", "")],
    "emory_healthcare": [("175099", ""), ("175068", "")],
}


@pytest.mark.parametrize("name", sorted(ICIMS_CARDS))
def test_icims_cards_carry_the_portal_number(fixture_text, name):
    jobs = scraper._parse_icims_cards(fixture_text(f"{D}/icims/cards_{name}.html"), "Sys", "x.icims.com")
    assert [(j.job_id, j.req_id) for j in jobs] == ICIMS_CARDS[name]
    for j in jobs:   # job_id and url exactly as before
        assert re.fullmatch(r"https://[^/]+\.icims\.com/jobs/" + j.job_id + r"/.+/job", j.url)


@pytest.mark.parametrize("name,jid,want", [
    ("catholic_health_long_island", "75455", "2026-75455"),   # header "ID" span, no <dl> pair
    ("prime_healthcare", "285876", "2026-285876"),            # <dt>ID</dt>
    ("community_medical_centers", "52605", "2026-R6612"),     # <dt>Requisition ID</dt>
    ("ahmc_healthcare", "28956", "req28956"),
])
def test_icims_job_page_gives_the_same_number(fixture_text, name, jid, want):
    page = fixture_text(f"{D}/icims/job_{name}_{jid}.html")
    assert scraper.icims_req_id(scraper._icims_header(page), page, jid) == want
    card = next(j for j in scraper._parse_icims_cards(fixture_text(f"{D}/icims/cards_{name}.html"), "S", "d")
                if j.job_id == jid)
    assert card.req_id == want
    # the detail pass fills a row whose card gave none, and never overwrites one
    blank = dataclasses.replace(card, req_id="", description="")
    scraper._icims_apply_page(blank, page)
    assert blank.req_id == want
    kept = dataclasses.replace(card, req_id="CARD-1")
    scraper._icims_apply_page(kept, page)
    assert kept.req_id == "CARD-1"


def test_icims_search_page_and_same_number_give_none(fixture_text):
    cards = fixture_text(f"{D}/icims/cards_prime_healthcare.html")
    job = scraper._parse_icims_cards(cards, "Prime Healthcare", "d")[0]
    job.req_id = ""
    scraper._icims_apply_page(job, cards)        # an expired posting redirects to a search page
    assert job.req_id == ""
    assert scraper.icims_req_id({"job id": "386998"}, "", "386998") == ""
    assert scraper.icims_req_id({"id": "not a number"}, "", "1") == ""
    assert scraper.icims_req_id({}, '<span class="sr-only field-label">ID</span><span > 2026-1</span>', "1") == "2026-1"
    assert scraper.icims_req_id({}, '<span class="sr-only field-label">Job ID</span><span > 2026-1</span>', "1") == ""


def test_icims_list_night_sends_the_number_without_the_global_switch(fixture_text):
    """Most nights a classic portal row is not fetched: its teaser is shorter
    than the stored body, so the upsert sends a blank body and null facts.
    The number must still go out, as a req_id-only object the trigger merges."""
    job = scraper._parse_icims_cards(fixture_text(f"{D}/icims/cards_catholic_health_long_island.html"),
                                     "Catholic Health (Long Island)", "careers-chsli.icims.com")[0]
    row = _list_night(job)
    assert row["posting_facts"] == {"req_id": "2026-75455"}
    assert "req_id" not in row and row["job_id"] == "75455"
    assert row["url"] == "https://careers-chsli.icims.com/jobs/75455/director-plant-operations/job"


def test_icims_detail_night_puts_the_number_in_the_full_facts_once(fixture_text):
    job = scraper._parse_icims_cards(fixture_text(f"{D}/icims/cards_catholic_health_long_island.html"),
                                     "Catholic Health (Long Island)", "careers-chsli.icims.com")[0]
    assert scraper._icims_apply_page(job, fixture_text(f"{D}/icims/job_catholic_health_long_island_75455.html"))
    assert len(job.description) >= 200
    # stored facts built before req_id existed: the hash differs, so they are rebuilt with it
    old = scraper.normalize_job(dataclasses.replace(job, req_id=""))["posting_facts"]
    scraper.set_known_bodies([_stored(job, len(job.description), fh=old["h"])])
    row = scraper.normalize_job(dataclasses.replace(job))
    facts = row["posting_facts"]
    assert facts["req_id"] == "2026-75455" and facts["v"] == scraper.FACTS_VERSION and facts["h"] != old["h"]
    # the next night the same body is skipped (null facts: the trigger keeps the stored object)
    scraper.set_known_bodies([_stored(job, len(job.description), fh=facts["h"])])
    assert scraper.normalize_job(dataclasses.replace(job))["posting_facts"] is None


def test_rows_without_a_number_and_other_platforms_are_unchanged(fixture_text):
    same = scraper._parse_icims_cards(fixture_text(f"{D}/icims/cards_select_medical.html"), "Select Medical", "d")[0]
    assert _list_night(same)["posting_facts"] is None      # Select Medical: nothing to send
    other = scraper.Job(title="RN", hospital_system="HCA Healthcare", hospital_name="H", city="", state="TX",
                        location="", specialty="", job_type="", url="https://x/jobs/1", job_id="1",
                        posted_date="", description="", ats_platform="Talemetry", req_id="4981331")
    assert scraper.normalize_job(other)["posting_facts"] is None   # still behind REQ_ID_ONLY_FACTS
    assert scraper.REQ_ID_ONLY_PLATFORMS == frozenset({"iCIMS", "Jobvite", "HCTS", "SelectMinds"})


# ── Jobvite Engage (TALEMETRY_SITES, ats_platform "Jobvite") ─────────────────

JOBVITE_PAGES = {   # fixture -> number the header prints
    "penn_medicine_job_18360735": "347741",          # <strong>Job ID:</strong> 347741<br />
    "penn_medicine_job_18359263": "348578",
    "asante_health_system_job_18360384": "27555",    # <strong>Req #:</strong> 27555<br
    "asante_health_system_job_18230356": "25526",
    "uchealth_job_18360497": "435091",               # <strong>Job ID</strong>: 435091 <br>
    "uchealth_job_18360496": "433345",
    "peacehealth_job_18360469": "143880",            # plain "Job ID: 143880<br>"
    "peacehealth_job_18359324": "142243",
    "lifebridge_health_job_18360846": "3905",        # plain "Job ID: 3905<br />"
    "lifebridge_health_job_18360847": "3916",
}


@pytest.mark.parametrize("name", sorted(JOBVITE_PAGES))
def test_jobvite_header_number(fixture_text, name):
    page = fixture_text(f"{D}/jobvite/{name}.html")
    assert scraper.jobvite_req_id_from_page(page, name.rsplit("_", 1)[1]) == JOBVITE_PAGES[name]
    # not the JSON-LD / apply-settings form ("1000-INFOR-435091", "PH-INFOR-143880")
    assert "INFOR" not in scraper.jobvite_req_id_from_page(page)


def test_jobvite_number_is_not_in_the_list_payload(fixture_text):
    for site in ("penn_medicine", "uchealth", "peacehealth", "lifebridge_health", "asante_health_system"):
        data = json.loads(fixture_text(f"{D}/jobvite/{site}_search.json"))
        for e in data["entries"]:
            assert set(e) == {"id", "talemetry_job_id", "permalink", "title", "location"}
            assert scraper._talemetry_job(e, "S", "https://x").req_id == ""


def test_jobvite_two_numbers_or_none_give_none():
    assert scraper.jobvite_req_id_from_page("Job ID: 1<br>Req #: 2<br>") == ""
    assert scraper.jobvite_req_id_from_page("Job ID: 1<br>Job ID: 1<br>") == "1"
    assert scraper.jobvite_req_id_from_page("Job ID: 18360497<br>", "18360497") == ""
    assert scraper.jobvite_req_id_from_page("<label>Job ID Number</label>") == ""
    assert scraper.jobvite_req_id_from_page("") == ""


def test_jobvite_detail_pass_reads_it_and_the_row_sends_it(fixture_text, monkeypatch):
    page = fixture_text(f"{D}/jobvite/penn_medicine_job_18360735.html")

    async def fake_curl(url, imp=None, *a, **k):
        assert url == "https://careers.pennmedicine.org/jobs/18360735-manager-of-operations-practice-support-ops"
        return page

    monkeypatch.setattr(scraper, "_curl_html", fake_curl)
    entry = json.loads(fixture_text(f"{D}/jobvite/penn_medicine_search.json"))["entries"][0]
    job = scraper._talemetry_job(entry, "Penn Medicine", "https://careers.pennmedicine.org")
    asyncio.run(scraper._talemetry_detail(None, job))
    assert job.req_id == "347741" and job.job_id == "18360735"
    # the excerpt has no JSON-LD body: facts are null, the number still goes out
    row = scraper.normalize_job(job)
    assert row["posting_facts"] == {"req_id": "347741"} and row["job_id"] == "18360735"
    # with a body in hand it rides on the full facts object
    job.description = "Manager of Operations, Practice Support, Penn Medicine. " * 40
    facts = scraper.normalize_job(job)["posting_facts"]
    assert facts["req_id"] == "347741" and facts["v"] == scraper.FACTS_VERSION


# ── HCTS ─────────────────────────────────────────────────────────────────────

HCTS = {   # fixture -> (sub, state, [(job_id, Req #)])
    "university_medical_center_of_el_paso": ("umcelpasocareers", "TX",          # icon title, bare number
                                             [("1809610", "26610"), ("1904700", "26742"), ("2005138", "28497")]),
    "samaritan_health_services": ("samhealthjobs", "OR",                         # icon title, "Req #: 72680"
                                  [("1986921", "72680"), ("2005382", "73049"), ("2049633", "74265")]),
    "samaritan_health_services_clinicians": ("samhealthclinicianjobs", "OR",
                                             [("1060928", "1668"), ("1416411", "1818"), ("1593683", "1904")]),
    "east_alabama_health": ("alabamahealth", "AL",                               # no icon, "Req #:</span> &nbsp;20093"
                            [("1236220", "20093"), ("1989345", "26280"), ("1989358", "26281")]),
}


@pytest.mark.parametrize("name", sorted(HCTS))
def test_hcts_cards_carry_req_number(fixture_text, name):
    sub, st, want = HCTS[name]
    jobs = scraper._parse_hcts_page(fixture_text(f"{D}/hcts/cards_{name}.html"), "Sys", sub, st)
    assert [(j.job_id, j.req_id) for j in jobs] == want
    for j in jobs:
        assert j.url.startswith(f"https://{sub}.hctsportals.com/jobs/{j.job_id}-")


@pytest.mark.parametrize("name", sorted(HCTS))
def test_hcts_job_page_matches_the_card(fixture_text, name, monkeypatch):
    sub, st, want = HCTS[name]
    for jid, req in want[:2]:
        page = fixture_text(f"{D}/hcts/{name}_job_{jid}.html")
        assert scraper.hcts_req_id(page, jid) == req

        async def fake_fetch(session, url, timeout=20, _p=page):
            return _p

        monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
        job = scraper.Job(title="T", hospital_system="Sys", hospital_name="Sys", city="", state=st, location="",
                          specialty="", job_type="", url=f"https://{sub}.hctsportals.com/jobs/{jid}-x",
                          job_id=jid, posted_date="", description="", ats_platform="HCTS")
        asyncio.run(scraper._hcts_detail(None, job))
        assert job.req_id == req
        job.req_id = "CARD"
        asyncio.run(scraper._hcts_detail(None, job))
        assert job.req_id == "CARD"


def test_hcts_older_fixtures_and_odd_markup():
    old = open(os.path.join(os.path.dirname(__file__), "fixtures", "hcts_search.html"), encoding="utf-8").read()
    assert all(re.fullmatch(r"\d+", j.req_id) and j.req_id != j.job_id
               for j in scraper._parse_hcts_page(old, "University Medical Center of El Paso", "umcelpasocareers", "TX"))
    assert scraper.hcts_req_id('<span>Req #:</span> &nbsp;1<span>Req #:</span> 2') == ""   # two numbers
    assert scraper.hcts_req_id('<span>Req #:</span> </div>') == ""
    assert scraper.hcts_req_id("") == ""


def test_hcts_list_night_sends_the_number(fixture_text):
    job = scraper._parse_hcts_page(fixture_text(f"{D}/hcts/cards_samaritan_health_services_clinicians.html"),
                                   "Samaritan Health Services (Clinicians)", "samhealthclinicianjobs", "OR")[0]
    job.description = "Outpatient psychiatrist, list teaser"
    row = _list_night(job)
    assert row["posting_facts"] == {"req_id": "1668"} and row["job_id"] == "1060928"


# ── SelectMinds ──────────────────────────────────────────────────────────────

def test_selectminds_latest_jobs_card_number(fixture_text):
    base = "https://mainline.referrals.selectminds.com"
    jobs = scraper._parse_selectminds_page(fixture_text("cov3/selectminds_mainline_cards.html"), "Main Line Health", base)
    assert [(j.job_id, j.req_id) for j in jobs] == [("29120", "82959"), ("29106", "78981")]
    assert jobs[0].description.endswith("Requisition #: 82959")   # the teaser line stays as before
    row = scraper.normalize_job(dataclasses.replace(jobs[0]))
    assert row["posting_facts"] is not None and row["posting_facts"]["req_id"] == "82959"
    assert row["job_id"] == "29120"
    # SelectMinds is not a known-body platform: the teaser goes out every night,
    # and whether or not it yields facts the number rides along
    facts = scraper.normalize_job(jobs[1])["posting_facts"]
    assert facts is not None and facts["req_id"] == "78981"


def test_selectminds_search_pages_print_no_number(fixture_text):
    """Since fix 11 the crawl reads /jobs/search/<id>/pageN, whose cards carry
    no job_external_id on either board; the job page has it (excerpts), but
    SelectMinds has no detail pass, so these rows get none."""
    for org, system, want in (("mainline", "Main Line Health", ["81154", "81600"]),
                              ("uiowa", "University of Iowa Hospitals and Clinics", ["26005431", "26005437"])):
        cards = scraper._parse_selectminds_page(fixture_text(f"{D}/selectminds/{org}_search_cards.html"), system,
                                                f"https://{org}.referrals.selectminds.com")
        assert len(cards) == 2 and all(j.req_id == "" for j in cards)
        pages = [fixture_text(f"{D}/selectminds/{org}_job_{j.job_id}.html") for j in cards]
        assert [scraper._SM_REQ_RE.search(p).group(1) for p in pages] == want
