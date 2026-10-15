# 2026-10-14 (Rochester Regional re-key): a Workday list item is keyed by the
# requisition bullet that is the posting's own id, not by a ZIP code that
# happens to match _WD_REQ_ID_RE first (wd_list_job_id). Rochester's bullets
# are [site, ZIP, "REQ_237858", town]; the ZIP keyed the row and seen_ids
# folded 1,331 live postings into 53 rows. Fixtures are real CXS list pages
# fetched 2026-10-14 (tests/fixtures/reqid_1014/workday/): Rochester's page 1
# plus every posting with an unusual bullet layout, and Altru's physician
# rows, whose five-digit bullet is the only one shaped like a requisition.
import asyncio
import json

import pytest

import scraper

FX = "reqid_1014/workday/"
ROCH = "Rochester Regional Health"


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {"content-type": "application/json"}

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _crawl(fixture_text, monkeypatch, name, system):
    page = json.loads(fixture_text(FX + name))

    def fake_req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("offset", 0) == 0 and not body.get("appliedFacets"):
            return _Ctx(_R(200, page))
        return _Ctx(_R(200, {"total": 0, "jobPostings": []}))
    monkeypatch.setattr(scraper, "req", fake_req)
    return page, asyncio.run(scraper.scrape_workday(None, system, scraper.WORKDAY_TENANTS[system]))


def _old_key(p):
    """The picker before the re-key: the first bullet shaped like a
    requisition, else the externalPath."""
    bf = [str(b).strip() for b in (p.get("bulletFields") or [])]
    return next((b for b in bf if scraper._WD_REQ_ID_RE.match(b)), "") or p["externalPath"]


def test_every_rochester_posting_is_its_own_row(fixture_text, monkeypatch):
    page, jobs = _crawl(fixture_text, monkeypatch, "rochester_list_full_p1_eg.json", ROCH)
    posts = page["jobPostings"]
    # the old picker folded these 32 postings into 12 ZIP rows
    assert len({_old_key(p) for p in posts}) == page["_meta"]["old_picker_rows"] == 12
    assert len(jobs) == len(posts) == 32
    assert len({j.job_id for j in jobs}) == 32
    assert not [j.job_id for j in jobs if j.job_id.isdigit()]                 # no ZIP keys a row
    for p, j in zip(posts, jobs):
        own = next(b for b in p["bulletFields"] if b.startswith("REQ_"))
        # the one exception keeps today's path key (a lone REQ_EG_17 bullet)
        assert j.job_id == (p["externalPath"] if p["bulletFields"] == ["REQ_EG_17"] else own), p
        assert j.url.endswith(p["externalPath"].split("/")[-1])
        assert j.req_id == ""                       # job_id is the number; nothing else to store


def test_rochester_physician_postings_keep_their_eg_numbers(fixture_text, monkeypatch):
    # REQ_EG_* does not match _WD_REQ_ID_RE; with a ZIP beside it the ZIP
    # used to key the row (seven RGH physician postings on 14621)
    _, jobs = _crawl(fixture_text, monkeypatch, "rochester_list_full_p1_eg.json", ROCH)
    ids = {j.job_id for j in jobs}
    assert {"REQ_EG_93", "REQ_EG_25", "REQ_EG_15", "REQ_EG_20", "REQ_EG_14", "REQ_EG_16",
            "REQ_EG_70", "REQ_EG_18"} <= ids
    assert "14621" not in ids and "14618" not in ids


def test_rochester_rows_live_today_keep_their_key(fixture_text, monkeypatch):
    # the five active rows not keyed by a ZIP on 2026-10-14 (wp.sql read)
    _, jobs = _crawl(fixture_text, monkeypatch, "rochester_list_full_p1_eg.json", ROCH)
    ids = {j.job_id for j in jobs}
    assert {"/job/Pulmonary-Medicine-Physician_REQ_EG_17", "REQ_199863", "REQ_199875", "REQ_223614"} <= ids
    # REQ_243317 (page 19): its second bullet is a UK postcode
    bf = ["ACM - Remote - UK", "YO10 4DZ", "REQ_243317", "York"]
    path = "/job/ACM---Remote---UK/Clinical-Trials-Scientific-Affairs-Supervisor_REQ_243317-2"
    assert scraper.wd_list_job_id(bf, path) == "REQ_243317"


def test_altru_is_unchanged(fixture_text, monkeypatch):
    page, jobs = _crawl(fixture_text, monkeypatch, "altru_list.json", "Altru Health System")
    assert [j.job_id for j in jobs] == [_old_key(p) for p in page["jobPostings"]]
    assert {"30005", "30019", "30032", "28258", "R6177"} == {j.job_id for j in jobs}


@pytest.mark.parametrize("bullets,path,want", [
    # Rochester (real items)
    (["ACM - Remote", "14624", "REQ_237858", "Rochester"],
     "/job/ACM---Remote/Clinical-Trials-Project-Manager_REQ_237858-1", "REQ_237858"),
    (["Rochester General Hospital (RGH)", "14621", "REQ_EG_93", "Rochester"],
     "/job/Rochester-General-Hospital-RGH/Rheumatologist_REQ_EG_93", "REQ_EG_93"),
    (["REQ_223614"], "/job/Surgical-Technologist-Internship_REQ_223614", "REQ_223614"),
    (["REQ_EG_17"], "/job/Pulmonary-Medicine-Physician_REQ_EG_17", ""),          # path key, as today
    # a ZIP with no bullet that is the posting's id keys the row as before
    (["Site", "14624", "Rochester"], "/job/Site/Nurse_REQ_1", "14624"),
    # a five-digit requisition that is the posting's id stays (Altru, Albany Med)
    (["North Dakota", "28258"], "/job/Grand-Forks-ND/Registered-Nurse-Residency-Program-New-Grads_28258", "28258"),
    (["12345", "R-99999"], "/job/Albany-NY/RN_12345", "12345"),
    (["North Dakota", "30005"], "/job/Grand-Forks-ND/Cardiology---GF_R5", "30005"),
    # the id is the path's last segment, not a town segment that carries the ZIP
    (["Site", "14624", "REQ_1001"], "/job/Rochester-14624/Nurse_REQ_1001", "REQ_1001"),
    # a bullet with a space is a place, never the id; a ZIP inside a longer number is not the id
    (["14624", "REQ 1001"], "/job/Site/Nurse_REQ 1001", "14624"),
    (["14624", "REQ_214624"], "/job/Site/Nurse_REQ_214624", "REQ_214624"),
    # an escaped path still names the id
    (["Site", "14624", "REQ_1002"], "/job/Site/Nurse%20Aide_REQ_1002", "REQ_1002"),
    # tenants without a five-digit first bullet are untouched
    (["R4446997", "Arizona", "Sun City West"], "/job/Banner/RN_R4446997", "R4446997"),
    (["Days", "JR11450"], "/job/X/RN_JR99999", "JR11450"),
    ([], "/job/X/RN_R1", ""),
])
def test_wd_list_job_id_rules(bullets, path, want):
    assert scraper.wd_list_job_id(bullets, path) == want
