# 2026-10-14 (owner: "we need to fix those now"): CHS (WPJobBoard) requisition
# number. job_id is the WordPress card's data-id (145981); the job page prints
# CHS's Oracle requisition number above the title (span.requisition-number
# "#167235", JSON-LD identifier.value "167235"). Every card of the
# wp_job_grid payload the crawl already reads carries it in its Apply link
# (.../CandidateExperience/en/sites/CX_1/job/167235), so every listed row gets
# it with no extra request. Fixtures (tests/fixtures/reqid_1014/wpjobboard/)
# are real 2026-10-14 snippets: three cards of the payload, each card exactly
# as served, and excerpts of their three job pages.
import asyncio
import json

import scraper

CARDS = {"145981": "167235", "135518": "155930", "144247": "164711"}


def _grid(fixture_text):
    return fixture_text("reqid_1014/wpjobboard/chs_grid.json")


def test_each_card_gives_the_number_its_page_prints(fixture_text):
    cards = json.loads(_grid(fixture_text))["payload"]
    for card in cards:
        did = next(d for d in CARDS if f'data-id="{d}"' in card)
        assert scraper.chs_req_id_from_card(card) == CARDS[did]
        page = fixture_text(f"reqid_1014/wpjobboard/chs_job_{did}.html")
        assert f'title="Requisition Number">#{CARDS[did]}</span>' in page
        assert f'"value":"{CARDS[did]}"' in page


def test_card_without_an_oracle_apply_link_gives_blank():
    f = scraper.chs_req_id_from_card
    assert f("") == "" and f(None) == ""
    assert f('<div class="chs-job" data-id="1"><a href="https://www.careershealthcare.com/job/x">Apply</a></div>') == ""
    # a Learn More link to the CHS page is never read as the number
    assert f('<a href="https://www.careershealthcare.com/job/hospital/fl/naples/1-west-registered-nurse">Learn More</a>') == ""
    assert f('<a href="https://fa-evxo-saasfaprod1.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/167235" '
             'class="qbutton">Apply</a>') == "167235"


class _R:
    def __init__(self, text, status=200):
        self._t, self.status = text, status

    async def text(self):
        return self._t


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _run_chs(fixture_text, monkeypatch):
    grid = _grid(fixture_text)
    empty = json.dumps({"payload": [], "meta": {"total": 2571}})

    def fake_req(session, method, url, **kw):
        params = kw.get("params") or kw.get("data") or {}
        return _Ctx(_R(grid if int(params.get("offset", 0)) == 0 else empty))

    async def no_jitter():
        return None

    async def no_detail(*a, **k):
        return None

    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "_board_detail_passes", no_detail)
    return asyncio.run(scraper.run_chs(None))


def test_run_chs_sets_req_id_on_every_card(fixture_text, monkeypatch):
    jobs = _run_chs(fixture_text, monkeypatch)
    assert {j.job_id: j.req_id for j in jobs} == CARDS
    j = next(x for x in jobs if x.job_id == "145981")
    # job_id and url are the card's data-id and page, as before
    assert j.url == "https://www.careershealthcare.com/job/hospital/fl/naples/1-west-registered-nurse"
    assert j.ats_platform == "WPJobBoard" and j.hospital_system == "Community Health Systems"


def test_a_list_only_night_still_sends_the_number(fixture_text, monkeypatch):
    """The list has no body: a row the detail pass does not read tonight
    (most of them: CHS_DESC_BUDGET is 1,500 of ~2,500) has null facts. The
    {"req_id"}-only object goes out anyway (REQ_ID_ONLY_PLATFORMS); the live
    trigger merges it into the stored facts."""
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    jobs = _run_chs(fixture_text, monkeypatch)
    scraper.set_known_bodies([{"hospital_system": "CHS", "job_id": "145981", "ats_platform": "WPJobBoard",
                               "desc_len": 4200, "fv": scraper.FACTS_VERSION, "fh": "0123456789ab"}])
    try:
        j = next(x for x in jobs if x.job_id == "145981")
        scraper._keep_stored_body("Community Health Systems", j)
        assert j.description == ""
        d = scraper.normalize_job(j)
        assert d["posting_facts"] == {"req_id": "167235"}
        assert "req_id" not in d and d["job_id"] == "145981"
    finally:
        scraper.set_known_bodies([])


def test_a_row_read_by_the_detail_pass_rebuilds_its_facts_with_the_number(fixture_text, monkeypatch):
    jobs = _run_chs(fixture_text, monkeypatch)
    scraper.set_known_bodies([])
    j = next(x for x in jobs if x.job_id == "135518")
    j.description = ("The Physical Therapist evaluates patients and plans, delivers and documents treatment. " * 25)
    d = scraper.normalize_job(j)
    assert d["posting_facts"]["req_id"] == "155930" and d["posting_facts"]["h"]


def test_the_number_adds_no_nightly_write(fixture_text, monkeypatch):
    """Content hash across nights for a row the detail pass read: the night
    after the fetch is a full write anyway (the body goes from the fetched
    text to the blank _keep_stored_body sends), with or without the
    {"req_id"}-only object; every night after it hashes alike, so the row
    is stamped, not rewritten."""
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    jobs = _run_chs(fixture_text, monkeypatch)
    body = "The Physical Therapist evaluates patients and plans, delivers and documents treatment. " * 25

    def night(desc, platforms):
        monkeypatch.setattr(scraper, "REQ_ID_ONLY_PLATFORMS", platforms)
        j = next(x for x in jobs if x.job_id == "135518")
        j.description = desc
        if not desc:
            scraper._keep_stored_body("Community Health Systems", j)
        d = scraper.normalize_job(j)
        return d, scraper.content_hash_for(d)

    with_set = scraper.REQ_ID_ONLY_PLATFORMS
    try:
        scraper.set_known_bodies([])
        a, ha = night(body, with_set)                       # the fetch night
        pf = a["posting_facts"]
        scraper.set_known_bodies([{"hospital_system": "CHS", "job_id": "135518",
                                   "ats_platform": "WPJobBoard", "desc_len": len(body),
                                   "fv": pf["v"], "fh": pf["h"], "rq": pf["req_id"]}])
        b, hb = night("", with_set)
        c, hc = night("", with_set)
        assert b["posting_facts"] == {"req_id": "155930"} and hb != ha and hc == hb
        b0, hb0 = night("", with_set - {"WPJobBoard"})       # as before 10-14
        assert b0["posting_facts"] is None and hb0 != ha
        assert scraper._facts_token(b0) == ["h", pf["h"]]                # same facts token as the fetch night
    finally:
        scraper.set_known_bodies([])
