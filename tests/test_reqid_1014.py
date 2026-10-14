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


def _known(job, desc_len=4000, fh=None):
    """The known-body row the run-start load gives for a stored posting."""
    return {"hospital_system": job.hospital_system, "job_id": job.job_id, "ats_platform": job.ats_platform,
            "desc_len": desc_len, "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": fh}


# ── Workday ──────────────────────────────────────────────────────────────
WD_CASES = [
    ("wvu.json", "JR26-47425"),
    ("monument.json", "27_322"),                 # path suffix "_27_322-2" is a copy suffix
    ("marinhealth.json", "14976 - AC307"),       # path suffix "_JR102021" is not the number
    ("kaweah.json", "R26_0000002352"),
    ("samaritan.json", "02.6025.0414.5"),
    ("rochester_numeric.json", "REQ_243958"),    # job_id "14468" came from a bullet that is not the req
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
    ("insight_2713.html", "Insight Health", "2713"),     # the cell is in the HTML, hidden by Insight's CSS
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
    assert scraper.normalize_job(job)["posting_facts"]["req_id"] == want


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


# ── One-time re-read of the stored bodies (REQ_ID_REREAD_WINDOW) ────────
def test_reread_window_lists_only_tenants_with_a_detail_reader():
    from datetime import datetime
    lo, hi = (datetime.strptime(x, "%Y-%m-%d") for x in scraper.REQ_ID_REREAD_WINDOW)
    assert 1 <= (hi - lo).days + 1 <= 7
    for s in scraper.REQ_ID_REREAD_SYSTEMS:
        assert scraper._canon_system(s) == s
        assert scraper.FORCED_REFRESH[s] == scraper.REQ_ID_REREAD_WINDOW
        assert (s in scraper.WORKDAY_TENANTS or s in scraper.CSOD_REQ_ID
                or s in scraper.SF_RMK_REQ_LABEL or s in scraper.PAYCOR_ORGS), s
    # list-source tenants need no re-read; the big tenants with a handful of rows are left out
    for s in ("Baylor Scott & White", "Rush", "Billings Clinic", "Prisma Health", "Montefiore"):
        assert s not in scraper.FORCED_REFRESH


def test_each_stored_body_is_read_once_inside_the_window(monkeypatch):
    from datetime import datetime
    a = datetime.strptime(scraper.REQ_ID_REREAD_WINDOW[0], "%Y-%m-%d").toordinal()
    b = datetime.strptime(scraper.REQ_ID_REREAD_WINDOW[1], "%Y-%m-%d").toordinal()
    ids = [f"/job/Town/Role_JR26-{n}" for n in range(3220)]          # WVU Medicine's size
    hits, per_night = dict.fromkeys(ids, 0), {}
    for d in range(a - 2, b + 3):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: d)
        n = sum(scraper._force_slot("WVU Medicine", j) for j in ids)
        per_night[d] = n
        for j in ids:
            hits[j] += scraper._force_slot("WVU Medicine", j)
    assert set(hits.values()) == {1}
    assert per_night[a - 1] == per_night[b + 1] == 0
    assert max(per_night.values()) <= scraper.WD_DESC_TENANT_MAX          # one tenant's nightly cap


def test_a_forced_workday_row_is_fetched_and_gets_its_number(fixture_text, monkeypatch):
    d, job, path = _wd_target(fixture_text, "monument.json")
    scraper.set_known_bodies([_known(job)])
    monkeypatch.setattr(scraper, "_refresh_slot", lambda *a, **k: False)
    monkeypatch.setattr(scraper, "_force_slot", lambda canon, jid: canon == "Monument Health")
    assert scraper._known_kind("Monument Health", job) == "force"
    seen = _serve(monkeypatch, [("/wday/cxs/", 200, d)])
    working = "https://monumenthealth.wd1.myworkdayjobs.com/wday/cxs/monumenthealth/Goldcareers/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, path)], "Monument Health"))
    assert len(seen) == 1 and job.req_id == "27_322"
    row = scraper.normalize_job(job)
    assert row["posting_facts"]["req_id"] == "27_322" and row["job_id"] == d["_meta"]["our_job_id"]
