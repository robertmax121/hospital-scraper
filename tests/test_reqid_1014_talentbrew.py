# 2026-10-14 (owner: "we need to fix those now"): why most TalentBrew rows
# still had no requisition number two days after tb_req_id_from_page landed
# (ad6fddd), and the fix.
#   * Only UHG's search cards print the number. Every other tenant gets it
#     from the job page, and the detail pass skips a row whose body is stored
#     (all 22,285 active rows were listed on 10-14; 4,321 had a number).
#     Inside REQ_REREAD_WINDOW such a row is a detail candidate again ("req")
#     when its stored facts have no req_id, inside the budget its pass
#     already has.
#   * A row with no body in hand tonight (UHG's card number, most nights)
#     sent null facts, so the number was dropped unless REQ_ID_ONLY_FACTS=1.
#     TalentBrew is in REQ_ID_ONLY_PLATFORMS: the {"req_id"}-only object goes
#     out and the live trigger merges it.
# Fixtures (tests/fixtures/reqid_1014/talentbrew/) are real 2026-10-14
# excerpts of two job pages read that day: the requisition metas, the
# visible label and the JSON-LD JobPosting with its body cut at a paragraph.
import asyncio
import dataclasses
import io
import json
from datetime import date

import pytest

import scraper

DAY_IN = date(2026, 10, 15).toordinal()
DAY_LAST = date(2026, 10, 28).toordinal()
DAY_OUT = date(2026, 10, 29).toordinal()
CS_JOB = "97195133472"          # CommonSpirit, page prints "Requisition ID 2026-480318"
KP_JOB = "100095074928"         # Kaiser, page prints "Job Number 1436888"


@pytest.fixture(autouse=True)
def _reset_known():
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


def _stored(jid, rq="__absent__", system="CommonSpirit", platform="TalentBrew", n=3200, fh="aaaaaaaaaaaa"):
    r = {"hospital_system": system, "job_id": jid, "ats_platform": platform, "desc_len": n,
         "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": fh}
    if rq != "__absent__":
        r["rq"] = rq
    return r


def _job(jid, system="CommonSpirit Health", **kw):
    base = dict(title="Surgical Tech", hospital_system=system, hospital_name="CHI Health",
                city="Omaha", state="NE", location="Omaha, NE", specialty="", job_type="",
                url=f"https://www.commonspirit.careers/job/omaha/surgical-tech/35300/{jid}",
                job_id=jid, posted_date="", description="", ats_platform="TalentBrew")
    base.update(kw)
    return scraper.Job(**base)


def test_known_body_load_marks_talentbrew_bodies_stored_without_a_number():
    scraper.set_known_bodies([
        _stored("1", rq=None),                         # no req_id in the stored facts
        _stored("2", rq="2026-480318"),                # has one
        _stored("3"),                                  # an older load without the rq column
        _stored("4", rq=None, system="Jackson Health System", platform="Phenom"),
        _stored("5", rq=None, n=150),                  # not a body
        _stored("6", rq="  "),                         # blank counts as none
    ])
    assert scraper._KNOWN_NOREQ == {("CommonSpirit", "1"), ("CommonSpirit", "6")}


def test_kind_is_req_only_inside_the_window_and_only_without_a_list_number(monkeypatch):
    scraper.set_known_bodies([_stored("1", rq=None), _stored("2", rq="2026-1")])
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    assert scraper._known_kind("CommonSpirit Health", _job("1")) == "req"
    assert scraper._known_kind("CommonSpirit Health", _job("2")) == "known"
    # a number the list gave (UHG's card) needs no page read
    assert scraper._known_kind("CommonSpirit Health", _job("1", req_id="2026-480318")) == "known"
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_LAST])
    assert scraper._known_kind("CommonSpirit Health", _job("1")) == "req"
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_OUT])
    assert scraper._known_kind("CommonSpirit Health", _job("1")) == "known"
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    monkeypatch.setattr(scraper, "REQ_REREAD_WINDOW", ("bad", "2026-10-28"))
    assert scraper._known_kind("CommonSpirit Health", _job("1")) == "known"


def test_other_platforms_never_get_the_req_kind(monkeypatch):
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    scraper.set_known_bodies([_stored("9", rq=None, system="Jackson Health System", platform="Phenom")])
    j = _job("9", system="Jackson Health System", ats_platform="Phenom")
    assert scraper._known_kind("Jackson Health System", j) == "known"


def test_candidates_put_req_rows_after_the_rows_with_no_body(monkeypatch):
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    scraper.set_known_bodies([_stored("1", rq=None), _stored("2", rq="2026-1")])
    jobs = [_job("1"), _job("2"), _job("0")]          # "0": no stored body
    held = []
    cands, known, dup = scraper._detail_candidates("CommonSpirit Health", jobs, scraper._DescBudget(100), held=held)
    assert [j.job_id for j in cands] == ["0", "1"]
    assert [(it.job_id, before, kind) for it, before, kind in held] == [("1", "", "req")]
    assert known == 1 and dup == 0
    assert "1 stored bodies queued for their requisition number" in scraper._held_note(held)
    # outside the window the same rows cost nothing, as before
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_OUT])
    held = []
    cands, known, _ = scraper._detail_candidates("CommonSpirit Health", [_job("1"), _job("2"), _job("0")],
                                                 scraper._DescBudget(100), held=held)
    assert [j.job_id for j in cands] == ["0"] and known == 2 and held == []


def test_detail_pass_reads_the_page_and_the_facts_carry_the_number(fixture_text, monkeypatch):
    page = fixture_text(f"reqid_1014/talentbrew/commonspirit_{CS_JOB}.html")
    assert '<b>Requisition ID</b> 2026-480318</span>' in page
    fetched = []

    async def fake_fetch_html(session, url, timeout=20):
        fetched.append(url)
        return page

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    scraper.set_known_bodies([_stored(CS_JOB, rq=None)])
    job = _job(CS_JOB)
    budget = scraper._DescBudget(10)
    asyncio.run(scraper._detail_pass(None, "CommonSpirit Health", [job], budget,
                                     lambda j: scraper._jsonld_detail(None, j), "TalentBrew"))
    assert len(fetched) == 1 and budget.spent == 1
    assert job.req_id == "2026-480318"
    assert job.job_id == CS_JOB and job.url.endswith(f"/35300/{CS_JOB}")     # unchanged
    assert len(job.description) >= 200
    d = scraper.normalize_job(job)
    assert d["posting_facts"]["req_id"] == "2026-480318"
    assert d["posting_facts"]["h"] != "aaaaaaaaaaaa"                       # rebuilt, not skipped
    assert "req_id" not in d


def test_outside_the_window_the_stored_row_is_not_read(fixture_text, monkeypatch):
    fetched = []

    async def fake_fetch_html(session, url, timeout=20):
        fetched.append(url)
        return ""

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_OUT])
    scraper.set_known_bodies([_stored(CS_JOB, rq=None)])
    job = _job(CS_JOB)
    asyncio.run(scraper._detail_pass(None, "CommonSpirit Health", [job], scraper._DescBudget(10),
                                     lambda j: scraper._jsonld_detail(None, j), "TalentBrew"))
    assert fetched == [] and job.req_id == "" and job.description == ""


def test_a_failed_read_keeps_the_stored_body(monkeypatch):
    async def fake_fetch_html(session, url, timeout=20):
        return ""                                   # a 404 / timeout answers ""

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    scraper.set_known_bodies([_stored(CS_JOB, rq=None)])
    job = _job(CS_JOB, description="Surgical Tech teaser")
    asyncio.run(scraper._detail_pass(None, "CommonSpirit Health", [job], scraper._DescBudget(10),
                                     lambda j: scraper._jsonld_detail(None, j), "TalentBrew"))
    assert job.description == "" and job.req_id == ""          # blank: the trigger keeps the stored body
    assert scraper.normalize_job(job)["posting_facts"] is None


class _R:
    status = 200

    def __init__(self, text):
        self._t = text

    async def text(self):
        return self._t


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def test_kaiser_page_runner_reads_it_on_a_req_reread(fixture_text, monkeypatch):
    page = fixture_text(f"reqid_1014/talentbrew/kaiser_{KP_JOB}.html")
    assert '<b> Job Number</b> 1436888</span>' in page
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(page)))
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    scraper.set_known_bodies([_stored(KP_JOB, rq=None, system="Kaiser Permanente")])
    job = _job(KP_JOB, system="Kaiser Permanente", city="Lancaster", state="CA",
               url=f"https://www.kaiserpermanentejobs.org/job/lancaster/respiratory-care-practitioner-ii-cpft-certification-on-call/641/{KP_JOB}")
    asyncio.run(scraper._detail_pass(None, "Kaiser Permanente", [job], scraper._DescBudget(10),
                                     lambda j: scraper._tb_page_detail(None, j), "TalentBrew page"))
    assert job.req_id == "1436888" and job.job_id == KP_JOB
    assert scraper.normalize_job(job)["posting_facts"]["req_id"] == "1436888"


def test_uhg_card_number_goes_out_on_a_night_its_body_is_not_read(monkeypatch):
    """UHG's card gives the number on every row; a stored body is not read
    again (no "req" re-read: the card had it), its facts go out null, and
    the number used to be dropped (680 of 5,446 UHG rows had it on 10-14)."""
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    monkeypatch.setattr(scraper, "_RUN_DAY", [DAY_IN])
    scraper.set_known_bodies([_stored("101871518032", rq=None, system="UnitedHealth Group")])
    j = _job("101871518032", system="UnitedHealth Group", req_id="2386735",
             url="https://careers.unitedhealthgroup.com/job/katy/x/34088/101871518032")
    assert scraper._known_kind("UnitedHealth Group", j) == "known"
    scraper._keep_stored_body("UnitedHealth Group", j)
    d = scraper.normalize_job(j)
    assert d["posting_facts"] == {"req_id": "2386735"}
    assert d["job_id"] == "101871518032" and "req_id" not in d
    # a platform outside REQ_ID_ONLY_PLATFORMS still sends null
    other = scraper.normalize_job(dataclasses.replace(j, ats_platform="Phenom", hospital_system="Jackson Health System"))
    assert other["posting_facts"] is None


def test_the_load_asks_for_the_stored_number(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    pages = [[{"id": 3, "hospital_system": "CommonSpirit", "job_id": CS_JOB, "ats_platform": "TalentBrew",
               "desc_len": 3200, "wage_min": None, "fv": "4", "fh": "aaaaaaaaaaaa", "rq": None},
              {"id": 4, "hospital_system": "Kaiser Permanente", "job_id": KP_JOB, "ats_platform": "TalentBrew",
               "desc_len": 3000, "wage_min": None, "fv": "4", "fh": "bbbbbbbbbbbb", "rq": "1436888"}], []]
    urls = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(rq, timeout=0):
        urls.append(rq.full_url)
        return _Resp(json.dumps(pages[len(urls) - 1]).encode())

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert scraper.load_known_bodies() == 2
    assert all("rq:posting_facts-%3E%3Ereq_id" in u for u in urls)
    assert scraper._KNOWN_NOREQ == {("CommonSpirit", CS_JOB)}
