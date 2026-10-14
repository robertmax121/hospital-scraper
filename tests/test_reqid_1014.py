# 2026-10-14 (owner: "Yeah we need to fix those now"): the job number the
# employer's page shows, stored as posting_facts["req_id"] where our job_id is
# a different value: Workday (jobPostingInfo.jobReqId on the CXS detail the
# Workday pass reads), Baylor Scott & White (Phenom refineSearch reqId/jobId,
# every row every night), CSOD (jobDetails data.ref, JPS + UI Health),
# SuccessFactors RMK (the board's labelled header line), Paycor (the page's
# "Job Id:" cell) and Rush (FXRecruiter feed "Ref:"). job_id, url and the
# (job_id, hospital_system) upsert key never change. A row whose facts go out
# null on a night (no body in hand) still sends the number, as a
# req_id-only object the live enrichment trigger merges (REQ_ID_ONLY_PLATFORMS).
# Fixtures are real 2026-10-14 responses, trimmed, contacts redacted
# (tests/fixtures/reqid_1014/<platform>/).
import asyncio
import dataclasses
import json
import re

import pytest

import scraper
from scraper import Job

FX = "reqid_1014/"


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    """No pauses, no known bodies, fresh detail budgets, the env switch off
    (the per-platform set is what sends the req_id-only object)."""
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    for name in ("WD", "CSOD", "SF_RMK", "PAYCOR"):
        monkeypatch.setattr(scraper, f"{name}_DESC_BUDGET", scraper._DescBudget(1000))
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


def _json(fixture_text, name):
    return json.loads(fixture_text(FX + name))


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {"content-type": "application/json"}

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _serve(monkeypatch, routes):
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append(url)
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(_R(status, body))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return seen


def _serving(page):
    async def fetch(*a, **k):
        return page
    return fetch


def _known(job, desc_len=4000, fh=None, **extra):
    """The known-body row the run-start load gives for a stored posting
    (pass rq=... for the req_id the run-start load reports)."""
    return {"hospital_system": job.hospital_system, "job_id": job.job_id, "ats_platform": job.ats_platform,
            "desc_len": desc_len, "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": fh, **extra}


# ── Workday ──────────────────────────────────────────────────────────────
WD_CASES = [
    ("wvu.json", "JR26-47425"),
    ("monument.json", "27_322"),                 # path suffix "_27_322-2" is a copy suffix
    ("marinhealth.json", "14976 - AC307"),       # path suffix "_JR102021" is not the number
    ("kaweah.json", "R26_0000002352"),
    ("samaritan.json", "02.6025.0414.5"),
    # job_id "14468" is Rochester's ZIP bullet: the row stands for whichever
    # posting comes first for that ZIP, so the detail page's number would
    # drift from the row's url; Rochester's number comes from the list
    # (test_rochester_number_comes_from_the_list_item_that_gives_the_url)
    ("rochester_numeric.json", ""),
    ("rochester_same.json", ""),                 # job_id already is the printed number
    ("prisma_blank.json", ""),                   # the payload carries none
]


@pytest.mark.parametrize("name,want", WD_CASES)
def test_workday_req_id_from_the_cxs_detail(fixture_text, name, want):
    d = _json(fixture_text, "workday/" + name)
    assert scraper.wd_req_id(d["jobPostingInfo"], d["_meta"]["our_job_id"]) == want


@pytest.mark.parametrize("raw,want", [
    ("JR26-47425", "JR26-47425"), (" 27_322 ", "27_322"), ("14976 - AC307", "14976 - AC307"),
    ("02.6025.0414.5", "02.6025.0414.5"), ("", ""), (None, ""), ("Registered Nurse", ""),
    ("14976  -  AC307", ""), ("https://x.myworkdayjobs.com/job/1", ""), ("R" + "1" * 40, ""),
    ("JR1\nJR2", ""),
])
def test_workday_req_value(raw, want):
    assert scraper._wd_req_value(raw) == want


def _wd_target(fixture_text, name, description=""):
    d = _json(fixture_text, "workday/" + name)
    meta = d["_meta"]
    path = meta["our_job_id"] if meta["our_job_id"].startswith("/") else "/job/" + meta["url"].split("/job/", 1)[1]
    job = Job(title=d["jobPostingInfo"]["title"], hospital_system=meta["system"], hospital_name=meta["system"],
              city="", state="", location="", specialty="", job_type="", url=meta["url"],
              job_id=meta["our_job_id"], posted_date="Posted Yesterday", description=description,
              ats_platform="Workday")
    return d, job, path


def test_workday_detail_pass_stores_the_number_and_keeps_job_id(fixture_text, monkeypatch):
    d, job, path = _wd_target(fixture_text, "marinhealth.json")
    seen = _serve(monkeypatch, [("/wday/cxs/", 200, d)])
    working = "https://mymarinhealth.wd5.myworkdayjobs.com/wday/cxs/mymarinhealth/MHCareers/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, path)], "MarinHealth"))
    assert seen == ["https://mymarinhealth.wd5.myworkdayjobs.com/wday/cxs/mymarinhealth/MHCareers" + path]
    assert job.req_id == "14976 - AC307"
    assert len(job.description) >= 200
    row = scraper.normalize_job(job)
    assert "req_id" not in row
    assert row["job_id"] == d["_meta"]["our_job_id"] and row["url"] == d["_meta"]["url"]
    assert row["posting_facts"]["req_id"] == "14976 - AC307"
    assert row["posting_facts"]["v"] == scraper.FACTS_VERSION


def test_workday_number_reaches_the_row_when_no_body_lands(fixture_text, monkeypatch):
    """A fetch whose body is too short to keep still read the number; the row
    sends it as a req_id-only object (the trigger merges it)."""
    d, job, path = _wd_target(fixture_text, "wvu.json")
    d = json.loads(json.dumps(d))
    d["jobPostingInfo"]["jobDescription"] = "<p>Short.</p>"
    _serve(monkeypatch, [("/wday/cxs/", 200, d)])
    working = "https://wvumedicine.wd1.myworkdayjobs.com/wday/cxs/wvumedicine/WVUH/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, path)], "WVU Medicine"))
    assert job.description == "" and job.req_id == "JR26-47425"
    row = scraper.normalize_job(job)
    assert row["posting_facts"] == {"req_id": "JR26-47425"}


def test_workday_rows_the_pass_does_not_fetch_send_nothing_new(fixture_text, monkeypatch):
    """A stored body the pass skips tonight: no request, no number, null facts
    exactly as before (the number arrives on the night the page is read)."""
    d, job, path = _wd_target(fixture_text, "wvu.json")
    scraper.set_known_bodies([_known(job)])
    monkeypatch.setattr(scraper, "_refresh_slot", lambda *a, **k: False)
    seen = _serve(monkeypatch, [("/wday/cxs/", 200, d)])
    working = "https://wvumedicine.wd1.myworkdayjobs.com/wday/cxs/wvumedicine/WVUH/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, path)], "WVU Medicine"))
    assert seen == [] and job.req_id == ""
    assert scraper.normalize_job(job)["posting_facts"] is None


# ── Baylor Scott & White (Phenom refineSearch) ──────────────────────────
class _BswSession:
    def __init__(self, payload):
        self.payload, self.calls = payload, []

    def post(self, url, json=None, **kw):
        self.calls.append(json.get("from"))
        body = self.payload if json.get("from") == 0 else {"refineSearch": {"totalHits": 2258, "data": {"jobs": []}}}
        return _Ctx(_R(200, body))


def _bsw_jobs(fixture_text, monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    sess = _BswSession(_json(fixture_text, "phenom/bsw_refinesearch.json"))
    return asyncio.run(scraper._scrape_bsw(sess))


def test_bsw_listing_carries_the_number_on_every_row(fixture_text, monkeypatch):
    jobs = _bsw_jobs(fixture_text, monkeypatch)
    assert [(j.job_id, j.req_id) for j in jobs] == [
        ("BSWHUS26018384EXTERNALENUS", "26018384"),
        ("BSWHUS26015583EXTERNALENUS", "26015583"),
        ("BSWHUS24004886EXTERNALENUS", "24004886"),
    ]
    assert jobs[0].url == "https://jobs.bswhealth.com/us/en/job/26018384"   # unchanged
    # the job page's JSON-LD identifier is the same number
    posting = scraper._jobposting_from_html(fixture_text(FX + "phenom/bsw_job_26018384_ld.html"))
    assert posting["identifier"]["value"] == jobs[0].req_id


def test_bsw_req_id_rules():
    assert scraper.bsw_req_id({"reqId": "26018384", "jobId": "26018384"}, "BSWHUS26018384EXTERNALENUS") == "26018384"
    assert scraper.bsw_req_id({"jobId": "26018384"}, "BSWHUS26018384EXTERNALENUS") == "26018384"
    assert scraper.bsw_req_id({"reqId": "26018384"}, "26018384") == ""
    assert scraper.bsw_req_id({}, "x") == "" and scraper.bsw_req_id(None, "x") == ""


def test_bsw_number_reaches_every_row_through_normalize_job(fixture_text, monkeypatch):
    jobs = _bsw_jobs(fixture_text, monkeypatch)
    teaser_row = scraper.normalize_job(dataclasses.replace(jobs[0]))
    # the listing teaser is a body (200+ characters): facts built, number on them
    assert teaser_row["posting_facts"]["req_id"] == "26018384"
    # a stored body tonight: the teaser is dropped (known-body skip), facts go
    # out null, and the number still goes, as a req_id-only object
    stored = dataclasses.replace(jobs[0])
    scraper.set_known_bodies([_known(stored)])
    scraper._keep_stored_body(stored.hospital_system, stored)
    assert stored.description == ""
    row = scraper.normalize_job(stored)
    assert row["posting_facts"] == {"req_id": "26018384"}
    assert row["job_id"] == "BSWHUS26018384EXTERNALENUS" and "req_id" not in row


def test_stored_facts_without_the_number_are_rebuilt_once_then_skipped(fixture_text, monkeypatch):
    """The facts skip must not keep an object written before req_id existed."""
    jobs = _bsw_jobs(fixture_text, monkeypatch)
    job = dataclasses.replace(jobs[0], description="Access Services Representative. " * 60)
    old_h = scraper.normalize_job(dataclasses.replace(job, req_id=""))["posting_facts"]["h"]
    scraper.set_known_bodies([_known(job, len(job.description), old_h)])
    row = scraper.normalize_job(job)
    assert row["posting_facts"]["req_id"] == "26018384" and row["posting_facts"]["h"] != old_h
    scraper.set_known_bodies([_known(job, len(job.description), row["posting_facts"]["h"])])
    assert scraper.normalize_job(job)["posting_facts"] is None    # the stored object keeps the number


def test_a_req_id_only_row_is_written_once_then_hash_skipped(fixture_text, monkeypatch):
    """content_hash: the req_id-only object changes the row's hash once (so
    the upsert writes it and the trigger merges it), then stays the same."""
    jobs = _bsw_jobs(fixture_text, monkeypatch)
    job = dataclasses.replace(jobs[0], description="")
    scraper.set_known_bodies([_known(job, 4000, "a1b2c3d4e5f6")])
    before = scraper.normalize_job(dataclasses.replace(job, req_id=""))
    tonight = scraper.normalize_job(job)
    again = scraper.normalize_job(job)
    assert before["posting_facts"] is None and tonight["posting_facts"] == {"req_id": "26018384"}
    assert scraper.content_hash_for(tonight) != scraper.content_hash_for(before)
    assert scraper.content_hash_for(tonight) == scraper.content_hash_for(again)


def test_other_rows_of_these_platforms_are_unchanged():
    """No number read: null facts stay null, as before (no empty objects)."""
    for plat in sorted(scraper.REQ_ID_ONLY_PLATFORMS):
        j = Job(title="RN", hospital_system="Other", hospital_name="Other", city="", state="", location="",
                specialty="", job_type="", url="https://x/1", job_id="1", posted_date="", description="",
                ats_platform=plat)
        assert scraper.normalize_job(j)["posting_facts"] is None
    # a platform outside the set keeps the switch's rule (HCA: off by default)
    j = Job(title="RN", hospital_system="HCA Healthcare", hospital_name="X", city="", state="", location="",
            specialty="", job_type="", url="https://x/1", job_id="1", posted_date="", description="",
            ats_platform="Talemetry", req_id="4981331")
    assert scraper.normalize_job(j)["posting_facts"] is None


# ── CSOD ────────────────────────────────────────────────────────────────
def _csod_job(system, jid):
    return Job(title="X", hospital_system=system, hospital_name=system, city="", state="", location="",
               specialty="", job_type="", url=f"https://x.csod.com/ux/ats/careersite/4/requisition/{jid}?c=x",
               job_id=jid, posted_date="", description="", ats_platform="CSOD")


@pytest.mark.parametrize("name,system,jid,want", [
    ("jps_30426.json", "JPS Health Network", "30426", "46031"),
    ("jps_30790.json", "JPS Health Network", "30790", "46562"),
    ("uihealth_20550.json", "UI Health", "20550", "1042207"),
    ("billings_12479.json", "Billings Clinic", "12479", ""),     # prints "req12479": the same number
])
def test_csod_detail_reads_data_ref(fixture_text, monkeypatch, name, system, jid, want):
    payload = _json(fixture_text, "csod/" + name)
    _serve(monkeypatch, [(f"/requisitions/{jid}/jobDetails", 200, payload)])
    monkeypatch.setitem(scraper._CSOD_CTX, system, ("https://x.csod.com", "tok", ""))
    job = _csod_job(system, jid)
    landed = asyncio.run(scraper._csod_detail(None, job))
    assert job.req_id == want and job.job_id == jid
    row = scraper.normalize_job(job)
    if want:
        assert landed and row["posting_facts"]["req_id"] == want
    else:
        assert "req_id" not in (row["posting_facts"] or {})


def test_csod_number_without_a_body_still_goes(fixture_text, monkeypatch):
    payload = _json(fixture_text, "csod/jps_30426.json")
    payload["data"]["externalDescription"] = "<b>Short</b>"
    _serve(monkeypatch, [("/jobDetails", 200, payload)])
    monkeypatch.setitem(scraper._CSOD_CTX, "JPS Health Network", ("https://x.csod.com", "tok", ""))
    job = _csod_job("JPS Health Network", "30426")
    assert asyncio.run(scraper._csod_detail(None, job)) is False
    assert scraper.normalize_job(job)["posting_facts"] == {"req_id": "46031"}


# ── SuccessFactors RMK ──────────────────────────────────────────────────
@pytest.mark.parametrize("name,system,jid,want", [
    ("tx_1439766300.html", "Texas Health and Human Services", "1439766300", "22217"),
    ("fl_1381376900.html", "Florida Department of Children and Families", "1381376900", "873803"),
    ("il_1437961500.html", "Illinois Department of Human Services", "1437961500", "58304"),
    ("il_1401947500.html", "Illinois Department of Human Services", "1401947500", "56838"),   # zero-width space before the label
])
def test_rmk_detail_reads_the_boards_label(fixture_text, monkeypatch, name, system, jid, want):
    page = fixture_text(FX + "rmk/" + name)
    monkeypatch.setattr(scraper, "_fetch_html", _serving(page))
    job = Job(title="X", hospital_system=system, hospital_name=system, city="", state="", location="",
              specialty="", job_type="", url=f"https://board/job/X/{jid}/", job_id=jid, posted_date="",
              description="", ats_platform="SuccessFactorsRMK")
    assert asyncio.run(scraper._rmk_detail(None, job)) is True
    assert job.req_id == want
    row = scraper.normalize_job(job)
    assert row["posting_facts"]["req_id"] == want and row["job_id"] == jid


def test_rmk_florida_reads_the_requisition_not_the_position_number(fixture_text):
    body, _ = scraper._rmk_posting(fixture_text(FX + "rmk/fl_1381376900.html"))
    assert "Position Number: 60010695" in body
    assert scraper.rmk_req_id("Florida Department of Children and Families", body) == "873803"


def test_rmk_arkansas_position_number_from_a_stored_body(fixture_text):
    body = fixture_text(FX + "rmk/ar_1406755800_stored_body.txt")
    assert scraper.rmk_req_id("Arkansas Department of Human Services", body, "1406755800") == "22150905"
    assert scraper.rmk_req_id("Unknown Board", body) == ""
    assert scraper.rmk_req_id("Arkansas Department of Human Services", "") == ""


# ── Paycor ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("name,system,want", [
    ("townsen_tmscw1013.html", "Townsen Memorial Hospital", "TMSCW1013"),
    ("coon_218.html", "Coon Memorial Hospital", "218"),
    # review 2026-10-14: the cell is in the HTML but the board's own CSS hides
    # it, so people never see a number there and none is stored
    ("insight_2713.html", "Insight Health", ""),
    ("hereford_431.html", "Hereford Regional Medical Center", ""),
])
def test_paycor_detail_reads_the_job_id_cell(fixture_text, monkeypatch, name, system, want):
    page = fixture_text(FX + "paycor/" + name)
    monkeypatch.setattr(scraper, "_fetch_html", _serving(page))
    hexid = "8a78839ea13353c501a1367d3cb31d54"
    job = Job(title="X", hospital_system=system, hospital_name=system, city="", state="TX", location="",
              specialty="", job_type="", url="https://recruitingbypaycor.com/career/JobIntroduction.action?id=" + hexid,
              job_id=hexid, posted_date="", description="", ats_platform="Paycor")
    assert asyncio.run(scraper._paycor_detail(None, job)) is True
    assert job.req_id == want and job.job_id == hexid
    assert "Job Id" not in job.description
    facts = scraper.normalize_job(job)["posting_facts"]
    if want:
        assert facts["req_id"] == want
    else:
        assert "req_id" not in facts


def test_paycor_hidden_cell_rules(fixture_text):
    cell = '<td id="gnewtonJobID"><b>Job Id:</b>&nbsp; 2713</td>'
    assert scraper._paycor_id_hidden(fixture_text(FX + "paycor/insight_2713.html"))
    assert scraper._paycor_id_hidden(fixture_text(FX + "paycor/hereford_431.html"))
    assert not scraper._paycor_id_hidden(fixture_text(FX + "paycor/townsen_tmscw1013.html"))
    assert not scraper._paycor_id_hidden(fixture_text(FX + "paycor/coon_218.html"))
    # another cell hidden, or the id cell styled but shown: the number stays
    assert scraper.paycor_req_id("<style>#gnewtonJobOpening {display:none}</style>" + cell) == "2713"
    assert scraper.paycor_req_id("<style>#gnewtonJobIDX {display:none}</style>" + cell) == "2713"
    assert scraper.paycor_req_id("<style>#gnewtonJobID {font-weight: bold}</style>" + cell) == "2713"
    # hidden by a <style> rule or by the cell's own style attribute
    assert scraper.paycor_req_id("<style>td#gnewtonJobID{display:none}</style>" + cell) == ""
    assert scraper.paycor_req_id('<td id="gnewtonJobID" style="display: none"><b>Job Id:</b> 2713</td>') == ""


def test_paycor_req_id_edge_cases():
    assert scraper.paycor_req_id("") == ""
    assert scraper.paycor_req_id('<td id="gnewtonJobID"><b>Job Id:</b>&nbsp; </td>') == ""
    assert scraper.paycor_req_id('<td id="gnewtonJobID"><b>Job Id:</b>&nbsp; 2713</td>', "2713") == ""


# ── Rush (FXRecruiter feed) ─────────────────────────────────────────────
def test_rush_feed_ref_is_the_number_the_page_prints(fixture_text):
    jobs = scraper._parse_rss_list(fixture_text(FX + "fxrecruiter/rush_feed.xml"), "Rush",
                                   scraper.HTML_LIST_SITES["Rush"])
    assert [(j.job_id, j.req_id) for j in jobs] == [
        ("13191", "31067"), ("13185", "30554"), ("10930", "21729rxv_7_1")]
    assert all(j.ats_platform == "FXRecruiter" for j in jobs)
    assert jobs[0].url.endswith("/clinical-pharmacy-specialist-medication-safety/13191")   # unchanged
    page = fixture_text(FX + "fxrecruiter/rush_job_13191_head.html")
    shown = re.search(r'fa-suitcase"></i></div>\s*<div class="job-text">\s*([^<]+?)\s*</div>', page).group(1)
    assert shown == jobs[0].req_id


def test_rush_number_reaches_a_stored_row(fixture_text):
    jobs = scraper._parse_rss_list(fixture_text(FX + "fxrecruiter/rush_feed.xml"), "Rush",
                                   scraper.HTML_LIST_SITES["Rush"])
    scraper.set_known_bodies([_known(jobs[2])])
    row = scraper.normalize_job(jobs[2])
    assert row["posting_facts"] == {"req_id": "21729rxv_7_1"} and row["job_id"] == "10930"


def test_html_list_req_value():
    assert scraper._hl_req_value("31067") == "31067"
    assert scraper._hl_req_value("21729rxv_7_1") == "21729rxv_7_1"
    assert scraper._hl_req_value("Chicago") == "" and scraper._hl_req_value("") == ""
    # a site without the field: no req_id, as before
    j = scraper._hl_make_job("X", {"id": r"/(\d+)$"}, "https://x.org/jobs/5", "RN", {})
    assert j.job_id == "5" and j.req_id == ""


# ── Re-read of the stored bodies still without a number (REQ_ID_REREAD_WINDOW) ──
# Review 2026-10-14: not tied to the push night. Inside the window a stored
# body whose posting_facts carry no req_id (the run-start load's rq) is read
# on its night in every REQ_ID_REREAD_CYCLE; it drops out once its number is
# stored, and a fetch that failed is tried again a cycle later.
def _day(s):
    from datetime import datetime
    return datetime.strptime(s, "%Y-%m-%d").toordinal()


def _wd_job(system, jid):
    return Job(title="RN", hospital_system=system, hospital_name=system, city="", state="", location="",
               specialty="", job_type="", url="https://x.myworkdayjobs.com/en-US/X" + jid, job_id=jid,
               posted_date="", description="", ats_platform="Workday")


def _forced(job):
    return scraper._known_kind(job.hospital_system, job) == "force"


def _first_forced_night(monkeypatch, job):
    a = _day(scraper.REQ_ID_REREAD_WINDOW[0])
    for x in range(a, a + scraper.REQ_ID_REREAD_CYCLE):
        monkeypatch.setattr(scraper, "_run_day", lambda x=x: x)
        if _forced(job):
            return x
    raise AssertionError("never forced")


def test_reread_set_lists_only_tenants_whose_page_shows_a_number():
    a, b = (_day(x) for x in scraper.REQ_ID_REREAD_WINDOW)
    assert b - a + 1 >= 2 * scraper.REQ_ID_REREAD_CYCLE      # a late push still gets a full cycle
    assert b <= _day("2026-10-31")                            # it ends; drop it on the push after
    for s in scraper.REQ_ID_REREAD_SYSTEMS:
        assert scraper._canon_system(s) == s
        assert s in scraper.CSOD_REQ_ID or s in scraper.SF_RMK_REQ_LABEL or s in scraper.PAYCOR_ORGS, s
        assert s not in scraper.FORCED_REFRESH
    # list-source tenants, a tenant whose job_id is the number, and the Paycor
    # boards that hide the cell are never re-read for it (Workday rows are
    # chosen by their path key, not by tenant)
    for s in ("Baylor Scott & White", "Rush", "Billings Clinic", "Insight Health",
              "Hereford Regional Medical Center", "WVU Medicine", "Rochester Regional Health"):
        assert s not in scraper.REQ_ID_REREAD_SYSTEMS


def test_each_row_is_read_once_in_any_cycle_of_nights_however_late_the_push(monkeypatch):
    ids = [f"/job/Town/Role_JR26-{n}" for n in range(3220)]          # WVU Medicine's size
    jobs = [_wd_job("WVU Medicine", j) for j in ids]
    scraper.set_known_bodies([_known(j, rq=None) for j in jobs])
    a, b = (_day(x) for x in scraper.REQ_ID_REREAD_WINDOW)
    n = scraper.REQ_ID_REREAD_CYCLE
    per_night = {}
    for d in range(a - 2, b + 3):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        per_night[d] = {j.job_id for j in jobs if _forced(j)}
    assert not per_night[a - 1] and not per_night[b + 1]
    # a push landing on any night up to the window's last n nights reads
    # every row exactly once in the n nights after it
    for push in range(a, b - n + 2):
        hits = dict.fromkeys(ids, 0)
        for d in range(push, push + n):
            for j in per_night[d]:
                hits[j] += 1
        assert set(hits.values()) == {1}, push
    biggest = max(len(v) for v in per_night.values())
    assert biggest <= scraper.WD_DESC_TENANT_MAX                  # one tenant's nightly cap
    assert biggest < 2 * len(ids) // n                            # about a fifth a night


def test_rows_with_their_number_or_without_one_to_get_are_not_reread(monkeypatch):
    a = _day(scraper.REQ_ID_REREAD_WINDOW[0])
    path = _wd_job("Montefiore", "/job/Bronx-NY/Registered-Nurse_R1")
    stored = _wd_job("WVU Medicine", "/job/Morgantown/RN_JR26-1")
    bullet = _wd_job("Rochester Regional Health", "14468")           # its number comes from the list
    bswrow = dataclasses.replace(path, hospital_system="Baylor Scott & White", job_id="BSWHUS1EXTERNALENUS",
                                 ats_platform="Phenom")
    billings = dataclasses.replace(path, hospital_system="Billings Clinic", job_id="12479", ats_platform="CSOD")
    insight = dataclasses.replace(path, hospital_system="Insight Health", job_id="8a78839e", ats_platform="Paycor")
    jps = dataclasses.replace(path, hospital_system="JPS Health Network", job_id="30426", ats_platform="CSOD")
    rows = [path, bullet, bswrow, billings, insight, jps, stored]
    scraper.set_known_bodies([_known(j, rq=None) for j in rows[:-1]] + [_known(stored, rq="JR26-1")])
    hits = [0] * len(rows)
    for d in range(a, a + scraper.REQ_ID_REREAD_CYCLE):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        for k, j in enumerate(rows):
            hits[k] += _forced(j)
    assert hits == [1, 0, 0, 0, 0, 1, 0]
    # a map built without the rq key (tests, other callers) never forces a read
    scraper.set_known_bodies([_known(path), _known(jps)])
    for d in range(a, a + scraper.REQ_ID_REREAD_CYCLE):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        assert not _forced(path) and not _forced(jps)


def test_a_failed_read_is_tried_again_a_cycle_later_and_a_stored_number_ends_it(monkeypatch):
    job = _wd_job("WVU Medicine", "/job/Morgantown/RN_JR26-7")
    scraper.set_known_bodies([_known(job, rq=None)])
    a, b = (_day(x) for x in scraper.REQ_ID_REREAD_WINDOW)
    nights = []
    for d in range(a, b + 1):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        if _forced(job):
            nights.append(d)
    n = scraper.REQ_ID_REREAD_CYCLE
    assert len(nights) >= 2 and all(y - x == n for x, y in zip(nights, nights[1:]))
    scraper.set_known_bodies([_known(job, rq="JR26-7")])                # the number landed
    for d in nights:
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        assert not _forced(job)


def test_the_run_start_load_reports_the_stored_number():
    j = _wd_job("WVU Medicine", "/job/Morgantown/RN_JR26-9")
    scraper.set_known_bodies([_known(j, rq="JR26-9")])
    assert scraper._KNOWN_REQ_READ[0] and ("WVU Medicine", j.job_id) in scraper._KNOWN_REQ
    scraper.set_known_bodies([_known(j, rq=" ")])
    assert scraper._KNOWN_REQ_READ[0] and not scraper._KNOWN_REQ
    scraper.set_known_bodies([_known(j)])
    assert not scraper._KNOWN_REQ_READ[0]
    with open(scraper.__file__, encoding="utf-8") as f:
        assert "rq:posting_facts-%3E%3Ereq_id" in f.read()


def test_a_forced_workday_row_is_fetched_and_gets_its_number(fixture_text, monkeypatch):
    d, job, path = _wd_target(fixture_text, "monument.json")
    scraper.set_known_bodies([_known(job, rq=None)])
    monkeypatch.setattr(scraper, "_refresh_slot", lambda *a, **k: False)
    night = _first_forced_night(monkeypatch, job)
    monkeypatch.setattr(scraper, "_run_day", lambda: night)
    assert scraper._known_kind("Monument Health", job) == "force"
    seen = _serve(monkeypatch, [("/wday/cxs/", 200, d)])
    working = "https://monumenthealth.wd1.myworkdayjobs.com/wday/cxs/monumenthealth/Goldcareers/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, path)], "Monument Health"))
    assert len(seen) == 1 and job.req_id == "27_322"
    row = scraper.normalize_job(job)
    assert row["posting_facts"]["req_id"] == "27_322" and row["job_id"] == d["_meta"]["our_job_id"]


def test_a_forced_csod_row_is_a_detail_candidate(monkeypatch):
    job = _csod_job("JPS Health Network", "30790")
    scraper.set_known_bodies([_known(job, rq=None)])
    monkeypatch.setattr(scraper, "_refresh_slot", lambda *a, **k: False)
    night = _first_forced_night(monkeypatch, job)
    monkeypatch.setattr(scraper, "_run_day", lambda: night)
    held = []
    cands, known, dup = scraper._detail_candidates("JPS Health Network", [job], scraper.CSOD_DESC_BUDGET, held=held)
    assert cands == [job] and known == 0 and [k for _, _, k in held] == ["force"]
    monkeypatch.setattr(scraper, "_run_day", lambda: night + 1)          # not its night: skipped as before
    cands, known, dup = scraper._detail_candidates("JPS Health Network", [job], scraper.CSOD_DESC_BUDGET, held=[])
    assert cands == [] and known == 1


# ── Rochester Regional (Workday list bullet) ────────────────────────────
def _rochester_list(fixture_text, monkeypatch, drop=()):
    page = _json(fixture_text, "workday/rochester_list.json")
    page["jobPostings"] = [p for k, p in enumerate(page["jobPostings"]) if k not in drop]
    page["total"] = len(page["jobPostings"])

    def fake_req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("offset", 0) == 0 and not body.get("appliedFacets"):
            return _Ctx(_R(200, page))
        return _Ctx(_R(200, {"total": 0, "jobPostings": []}))
    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    return asyncio.run(scraper.scrape_workday(None, "Rochester Regional Health",
                                              scraper.WORKDAY_TENANTS["Rochester Regional Health"]))


def test_rochester_number_comes_from_the_list_item_that_gives_the_url(fixture_text, monkeypatch):
    jobs = _rochester_list(fixture_text, monkeypatch)
    got = [(j.job_id, j.req_id, j.url.rsplit("_REQ_", 1)[-1].split("-")[0]) for j in jobs]
    # the ZIP keys the row (pre-existing; three Monroe Ave postings share 14607)
    assert got == [("14624", "REQ_237858", "237858"), ("14607", "REQ_238008", "238008"),
                   ("14534", "REQ_194790", "194790"), ("14513", "REQ_241533", "241533")]
    # the page's JSON-LD identifier is the bullet (checked live 2026-10-14)
    meta = _json(fixture_text, "workday/rochester_list.json")["_meta"]
    assert jobs[0].req_id in meta["page_identifier"]
    # another night the ZIP's first posting is another one: the number follows the url
    later = _rochester_list(fixture_text, monkeypatch, drop={1})
    row = next(j for j in later if j.job_id == "14607")
    assert row.req_id == "REQ_240770" and row.url.endswith("_REQ_240770")
    assert row.req_id in meta["page_identifier"]


def test_rochester_number_reaches_a_stored_row_every_night(fixture_text, monkeypatch):
    jobs = _rochester_list(fixture_text, monkeypatch)
    job = jobs[1]
    scraper.set_known_bodies([_known(job)])
    scraper._keep_stored_body(job.hospital_system, job)
    row = scraper.normalize_job(job)
    assert row["posting_facts"] == {"req_id": "REQ_238008"}
    assert row["job_id"] == "14607" and "req_id" not in row


def test_wd_list_req_id_rules():
    roch = "Rochester Regional Health"
    assert scraper.wd_list_req_id(roch, ["Site", "14624", "REQ_237858", "Rochester"], "14624") == "REQ_237858"
    assert scraper.wd_list_req_id(roch, ["Site", "REQ_223614"], "REQ_223614") == ""      # already the job_id
    assert scraper.wd_list_req_id(roch, ["Site", "14624"], "14624") == ""
    assert scraper.wd_list_req_id(roch, ["REQ_EG_17"], "/job/Pulmonary_REQ_EG_17") == ""  # path rows: detail
    assert scraper.wd_list_req_id("WVU Medicine", ["14624", "REQ_237858"], "14624") == ""   # other tenants
    # the detail reader leaves bullet-keyed rows alone
    assert scraper.wd_req_id({"jobReqId": "REQ_243958"}, "14468") == ""
    assert scraper.wd_req_id({"jobReqId": "REQ_EG_17"}, "/job/Pulmonary_REQ_EG_17") == "REQ_EG_17"
