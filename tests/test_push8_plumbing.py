"""Push 8 plumbing (2026-10-05): first_seen in the upsert, the placeholder
tenants' real names, the known-body platforms and detail budgets of plan
item 9, and tools/wd_local_detail.py (plan item 8). No database, no network.

The posting texts are real stored bodies (hospital_jobs, read-only, 10-05):
Trinity Health 34721827 (Workday, Saint Agnes Fresno) and Paycom Hospital 2
28624936 (Onslow, a physician posting stored as TX)."""
import io
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

import pytest

import scraper

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools"))
import wd_local_detail as W  # noqa: E402

TRINITY_BODY = """Employment Type:
Part time
Shift:

Description:

Summary:

This position assembles and delivers meal trays and snacks to patients. The position interacts with nursing personnel, patients, and visitors to assure quality service to patients during meal service. This position may be responsible for visiting patients to get meal selections for the day. This position completes assigned tasks in the kitchen related to patient care.

Requirements:

1. High school diploma or equivalent is required.

2. Must have excellent customer service skills and perform with a sense of urgency.

3. Previous food handling is preferred.

4. Some computer skills are helpful.

Pay Range: $23.00 - $28.64"""
TRINITY_URL = ("https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Saint-Agnes-Medical-Center---Fresno-California/"
               "Nutrition-Services-Assistant-Extra-on-Call_00698608")
ONSLOW_BODY = ("Onslow Ambulatory Services is seeking a Gastroenterologist to join our multidisciplinary practice. "
               "The candidate will join the medical team which includes both physicians and advanced practice providers.\n\n"
               "BC/BE\n\nFellowship training – Gastroenterology\n\nCompetitive Compensation\n\n"
               "Qualifications\n\nNC Medical License\n\nERCP capable\n\nShift: Varies")


# ── first_seen ─────────────────────────────────────────────────────────────

class _Resp:
    def __init__(self, headers=None):
        self.headers = headers or {}
    def read(self): return b"[]"
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _row(system, i):
    return {"hospital_system": system, "job_id": str(i), "title": "Nutrition Services Assistant",
            "hospital_name": system, "url": f"https://example.org/jobs/{i}", "job_type": "",
            "description": "", "state": "CA", "city": "Fresno"}


def _fake_db(monkeypatch, column_exists):
    posts, probes = [], []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u:
            return _Resp()
        if m == "GET" and "select=first_seen" in u:
            probes.append(u)
            if column_exists:
                return _Resp()
            raise urllib.error.HTTPError(u, 400, "bad", {}, io.BytesIO(
                b'{"code":"42703","message":"column hospital_jobs.first_seen does not exist"}'))
        if m == "POST":
            posts.append(json.loads(rq.data.decode()))
            return _Resp()
        if m == "GET":
            return _Resp({"Content-Range": "0-0/0"})
        return _Resp({"Content-Range": "*/0"})

    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    monkeypatch.setattr(scraper, "_HJ_FIRST_SEEN", {})
    return posts, probes


def test_upsert_sends_first_seen_once_the_column_exists(monkeypatch):
    posts, probes = _fake_db(monkeypatch, column_exists=True)
    run = "2026-10-05T00:09:00.000000Z"
    scraper._upsert_hospital_jobs_to_supabase([_row("Trinity Health", i) for i in range(12)], run)
    landed = [r for b in posts for r in b]
    assert len(landed) == 12
    assert all(r["first_seen"] == run == r["scraped_at"] for r in landed)
    # the probe is one read-only select, cached for the rest of the process
    scraper._upsert_hospital_jobs_to_supabase([_row("Trinity Health", i) for i in range(12)], run)
    assert len(probes) == 1


def test_upsert_leaves_first_seen_out_before_67b(monkeypatch):
    # PostgREST rejects a batch naming an unknown column: no key at all, and a
    # stale key on a re-sent dict is dropped too.
    posts, probes = _fake_db(monkeypatch, column_exists=False)
    rows = [_row("Trinity Health", i) for i in range(12)]
    rows[0]["first_seen"] = "2026-10-04T00:09:00Z"
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-05T00:09:00.000000Z")
    landed = [r for b in posts for r in b]
    assert len(landed) == 12 and not any("first_seen" in r for r in landed)
    assert scraper._HJ_FIRST_SEEN == {"ok": False}


def test_a_failed_probe_is_not_cached(monkeypatch):
    calls = []

    def urlopen(rq, timeout=None):
        calls.append(rq.full_url)
        raise OSError("connection reset")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper, "_HJ_FIRST_SEEN", {})
    assert scraper._hospital_jobs_has_first_seen("https://fake.invalid", "k") is False
    assert scraper._HJ_FIRST_SEEN == {}            # asked again next run, not pinned off


# ── placeholder tenants ────────────────────────────────────────────────────

PLACEHOLDER = re.compile(r"^(paycom|paycor|adp|ukg|kronos|workday|icims) (hospital|health system)\s*\d+$", re.I)


def test_no_tenant_is_written_under_a_placeholder_name():
    for cfg in (scraper.ADP_ORGS, scraper.PAYCOM_ORGS, scraper.PAYCOR_ORGS, scraper.KRONOS_ORGS):
        assert not [k for k in cfg if PLACEHOLDER.match(k)]


@pytest.mark.parametrize("label,cid,state", [
    ("Regional West Health Services", "152f13f3-9efa-4e16-9a69-bb7500136904", "NE"),
    ("Jefferson Regional Medical Center", "542f7b59-1156-4a17-a729-f8cd9337acf6", "AR"),
    ("Unity Health", "af93ba9c-e8c7-4a6f-ade3-711614110405", "AR"),
    ("Valley Health Systems (WV)", "77e754a7-66ab-427f-ae54-31edee4e9bf6", "WV"),
    ("Columbus Regional Healthcare System", "86be0242-2e9b-4a21-9dac-6ef6b31fbbee", "NC"),
    ("Missouri Delta Medical Center", "171c7aca-96cb-44e7-95db-7545554c14e8", "MO"),
    ("South Central Regional Medical Center", "c155faa0-8c71-47b0-bbaa-2b7939324014", "MS"),
    ("Touchette Regional Hospital", "a074e043-a14e-4f2d-8cf7-bee3e0a7ac61", "IL"),
    ("KPC Health", "1a214979-2739-4245-a1d1-38dc8531018f", "CA"),
    ("Larkin Community Hospital", "5ffc5741-7db3-4aa8-a16a-e19abed9677e", "FL"),
    ("Union General Health System", "58af5ddf-316e-4ac8-bc2f-471750cda3c7", "GA"),
    ("Hudson Regional Health", "bb661c48-7edc-400c-adfb-40f8f7743374", "NJ"),
])
def test_adp_boards_keep_their_cid_under_the_real_name(label, cid, state):
    (c, cc, st, name), = scraper._adp_centers(scraper.ADP_ORGS[label])
    assert (c, cc, st, name) == (cid, "19000101_000001", state, "")
    # A requisition with no location falls back to the label and its state
    # (it used to read "ADP Health System N", state blank).
    job = scraper._adp_job({"itemID": "9201", "requisitionTitle": "Registered Nurse"}, label, c, cc, st, name)
    assert (job.hospital_system, job.hospital_name, job.state) == (label, label, state)


def test_onslow_physician_posting_is_nc_not_tx():
    # Paycom Hospital 2 28624936: the preview names no location, so the
    # adapter's TX default stored it as Texas.
    key = scraper.PAYCOM_ORGS["Onslow Memorial Hospital"]
    assert key == "4863CB61AD1B2555F37E9E5884626947"
    job = scraper._paycom_job({"jobId": "47973", "jobTitle": "Gastroenterologist (47973)"},
                              {"jobPosting": {"description": ONSLOW_BODY}}, "Onslow Memorial Hospital", key,
                              scraper.PAYCOM_DEFAULT_STATE.get("Onslow Memorial Hospital", "TX"))
    assert (job.state, job.hospital_name) == ("NC", "Onslow Memorial Hospital")


@pytest.mark.parametrize("label,key,state", [
    ("Duncan Regional Hospital", "C48961799EBD231096CE8423D325C34C", "OK"),
    ("Oneida Health", "0FD7E535C5AC57A6144B389ACAA1998B", "NY"),
    ("Anderson Hospital", "8236C138F02B1587E10CAE245C2E6EE6", "IL"),
    ("Cuyuna Regional Medical Center", "BA896DB60A5046DD23CC67AB5801923F", "MN"),
])
def test_paycom_boards_relabelled(label, key, state):
    assert scraper.PAYCOM_ORGS[label] == key and scraper.PAYCOM_DEFAULT_STATE[label] == state


def test_kronos_and_paycor_boards_relabelled():
    assert scraper.KRONOS_ORGS["Northern Regional Hospital"] == ("prd01-hcm01.prd", "6059921", "NC")
    assert scraper.KRONOS_ORGS["Pikeville Medical Center"] == ("prd01-hcm01.prd", "6142380", "KY")
    assert scraper.PAYCOR_ORGS["Insight Health"] == "8a7883d07725ca8701773c07f64d08fa"


# ── plan item 9: known bodies and budgets ──────────────────────────────────

@pytest.mark.parametrize("plat", ["PeopleSoft", "SuccessFactors", "Talemetry", "Eightfold",
                                  "Avature", "Drupal", "FXRecruiter", "PeopleAdmin", "WordPress"])
def test_cov3_passes_skip_rows_that_already_hold_a_body(plat):
    assert plat in scraper.KNOWN_BODY_PLATFORMS


def test_peoplesoft_pass_skips_a_stored_body(monkeypatch):
    # NYC H+H 10-04: the pass re-read the bodies it had stored instead of the
    # 1,706 rows with none. A stored body is now free; the empty row is a candidate.
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    base = "https://careers.nychhc.org/psc/hrtam/EMPLOYEE/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
    jobs = [scraper.Job(title=t, hospital_system="NYC Health + Hospitals", hospital_name="Elmhurst Hospital Center",
                        city="Elmhurst", state="NY", location="Elmhurst, NY", specialty="", job_type="",
                        url=f"{base}?Page=HRS_APP_JBPST_FL&Action=U&FOCUS=Applicant&SiteId=1&JobOpeningId={i}&PostingSeq=1",
                        job_id=str(i), posted_date="2026-09-01", description="", ats_platform="PeopleSoft")
            for i, t in ((139013, "Staff Nurse"), (138888, "Clerical Associate"))]
    scraper.set_known_bodies([{"hospital_system": "NYC Health + Hospitals", "job_id": "139013",
                               "desc_len": 6058, "fv": "3"}])
    try:
        budget = scraper._DescBudget(10)
        cands, known, dup = scraper._detail_candidates("NYC Health + Hospitals", jobs, budget)
        assert [j.job_id for j in cands] == ["138888"] and known == 1
    finally:
        scraper.set_known_bodies([])


def test_detail_budgets_raised(monkeypatch):
    for k in ("TB_DESC_MAX_PER_RUN", "PEOPLESOFT_DESC_MAX_PER_RUN"):
        monkeypatch.delenv(k, raising=False)
    src = io.open(scraper.__file__, encoding="utf-8").read()
    assert 'os.getenv("TB_DESC_MAX_PER_RUN", "6000")' in src
    assert 'os.getenv("PEOPLESOFT_DESC_MAX_PER_RUN", "2000")' in src


# ── tools/wd_local_detail.py ───────────────────────────────────────────────

def _stored_row(i=34721827, body=""):
    return {"id": i, "hospital_system": "Trinity Health", "hospital_name": "Saint Agnes Medical Center",
            "job_id": "00698608", "title": "Nutrition Services Assistant Extra on Call",
            "location": "Fresno, CA", "city": "Fresno", "state": "CA", "specialty": "", "job_type": "",
            "url": TRINITY_URL, "posted_date": "Posted 30+ Days Ago", "description": body,
            "ats_platform": "Workday", "wage_min": None, "wage_max": None, "wage_unit": None}


def test_candidate_sql_is_a_light_keyset_select():
    q = W.candidate_sql(34000000, 2000, "Trinity Health", every=101)
    assert q.startswith("select ") and "description" in q
    for frag in ("ats_platform = 'Workday'", "coalesce(desc_len, 0) < 200", "is_active",
                 "id > 34000000", "order by id limit 2000", "hospital_system = 'Trinity Health'", "id % 101 = 0"):
        assert frag in q
    assert not re.search(r"\b(insert|update|delete)\b", q, re.I)


def test_detail_pass_applies_the_cxs_posting_info():
    seen = []

    def get(url):
        seen.append(url)
        return 200, {"jobPostingInfo": {"jobDescription": TRINITY_BODY.replace("\n", "<br>"),
                                        "timeType": "Part time", "startDate": "2026-10-02"}}
    f = W.Fetcher(get=get, pause=None)
    changed, stats, per_sys, samples = W.detail_pass(scraper, [_stored_row()], f, workers=2)
    assert seen == ["https://trinityhealth.wd1.myworkdayjobs.com/wday/cxs/trinityhealth/Jobs/job/"
                    "Saint-Agnes-Medical-Center---Fresno-California/Nutrition-Services-Assistant-Extra-on-Call_00698608"]
    (job,) = changed
    assert "High school diploma or equivalent is required." in job.description
    assert (job.posted_date, job.job_type) == ("2026-10-02", "Part time")
    assert stats["descriptions"] == 1 and stats["HTTP 200"] == 1 and per_sys["Trinity Health"]["descriptions"] == 1
    assert samples and samples[0]["system"] == "Trinity Health"
    # the nightly's own normalize gives the row its facts from the new body
    (row,) = scraper.finalize_jobs(changed)
    assert row["posting_facts"] and row["posting_facts"]["requirements"]["education"]


def test_detail_pass_leaves_failures_and_non_workday_rows_alone():
    f = W.Fetcher(get=lambda url: (429, None), pause=None)
    bad = dict(_stored_row(2), url="https://example.org/job/1")
    changed, stats, per_sys, _s = W.detail_pass(scraper, [_stored_row(), bad], f, workers=2)
    assert changed == [] and stats["HTTP 429"] == 1 and stats["url is not a Workday job URL"] == 1


def test_round_robin_spreads_hosts():
    items = ["a1", "a2", "a3", "b1", "c1", "c2"]
    assert W.round_robin_by_host(items, lambda s: s[0]) == ["a1", "b1", "c1", "a2", "c2", "a3"]


def test_push_never_sweeps(monkeypatch):
    got = {}

    def upsert(rows, run_started_iso):
        got["rows"], got["partial"] = rows, set(scraper.PARTIAL_SYSTEMS)
        return len(rows)
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", upsert)
    monkeypatch.setattr(scraper, "PARTIAL_SYSTEMS", set())
    monkeypatch.setattr(scraper, "LAST_UPSERT_FAILED", [])
    job = W.job_from_row(scraper, _stored_row(body=TRINITY_BODY))
    assert W.push(scraper, [job]) == 1
    assert "Trinity Health" in got["partial"]          # the per-system sweep is skipped
    assert got["rows"][0]["description"].startswith("Employment Type:")


@pytest.mark.parametrize("hhmm,quiet", [((23, 29), False), ((23, 30), True), ((0, 9), True),
                                         ((5, 59), True), ((6, 0), False), ((14, 0), False)])
def test_write_mode_quiet_window(hhmm, quiet):
    from datetime import datetime, timezone
    assert W.in_quiet_window(datetime(2026, 10, 5, *hhmm, tzinfo=timezone.utc)) is quiet


def test_write_needs_an_explicit_mode():
    with pytest.raises(SystemExit):
        W.main([])
