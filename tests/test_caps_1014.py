"""Fix 11 (2026-10-14): held systems whose crawls came back short.

The 10-14 run's yield guard held 25 systems (10,479 rows), so their closed
postings were never retired. Per adapter:

  Phenom      refineSearch ordered these tenants by the caller's IP; every page
              left through another proxy, so a crawl sampled ~1 - 1/e of the
              board (DaVita 2,102 of 3,333). Fixed location, 500-row pages.
  Talemetry   the crawl ended at the first page the shared Cloudflare edge
              refused (Penn 400 of 1,770, UCHealth 500 of 1,341). per_page 1000,
              failed pages skipped and retried after a cool-down, TLS profile
              rotation on 403, ties recovered in title order, PARTIAL if unread.
  SelectMinds ?page=N is ignored; the search pages by path (Main Line 10 of 346).
  Greenhouse / ApplicantPro / UKG / NeoGov / TalentBrew: complete claims
              against the board's own count, so a board read whole is swept even
              when its stale rows hold the yield under 80%.

Fixtures in tests/fixtures/caps1014 are live responses of 2026-10-14, trimmed.
No network, no database."""
import asyncio
import copy
import json
import logging
import os
import random
import re

import pytest

import scraper
from conftest import FIXTURES

CAPS = os.path.join(FIXTURES, "caps1014")


def _read(name):
    with open(os.path.join(CAPS, name), encoding="utf-8") as f:
        return f.read()


def _json(name):
    return json.loads(_read(name))


class _R:
    """aiohttp-like response: status, headers, url, async text() / json()."""

    def __init__(self, status, body, url=""):
        self.status, self._body, self.url = status, body, url
        self.headers = {"content-type": "text/html" if isinstance(body, str) else "application/json"}

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        if isinstance(self.r, Exception):
            raise self.r
        return self.r

    async def __aexit__(self, *a):
        return False


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    async def no_jitter():
        return None

    async def no_sleep(_s):
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "_retry_sleep", no_sleep)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)


# ══ 1. Phenom: fixed location, 500-row pages ══════════════════════════════

def test_live_davita_pages_show_the_ip_order_and_the_fixed_location():
    """The two live answers of 10-14 (from a Texas address): with no location
    the board opens on Texas clinics; with PHENOM_FIXED_LOCATION it opens on
    the ones nearest the centre of the US, and the total is the same."""
    plain = _json("phenom_davita_refine_noloc.json")["refineSearch"]
    fixed = _json("phenom_davita_refine_located.json")["refineSearch"]
    assert plain["totalHits"] == fixed["totalHits"] == 3346
    assert [j["state"] for j in plain["data"]["jobs"]] == ["Texas", "Texas", "Texas"]
    assert [j["state"] for j in fixed["data"]["jobs"]] == ["Nebraska", "Nebraska", "Kansas"]
    assert fixed["data"]["locationData"] == scraper.PHENOM_FIXED_LOCATION
    assert plain["data"]["locationData"] == {}


_PHENOM_HTML = ('<script>phApp.ddo = {}; phApp.pageInfo = {"refNum":"DAVIUS","siteType":"external",'
                '"rootDomain":"https://careers.davita.com","pageId":"page24"};</script>')


class _PhenomBoard:
    """A refineSearch tenant: n jobs (the live DaVita job shape), at most
    max_size a page. With no location the order follows the caller, a
    different one on every request (the proxy pool); a location gives one
    fixed order. fail_offsets answer HTTP 500; located_total, when set, is
    what a located search reports (a tenant that would filter by it)."""

    def __init__(self, n, max_size=500, rotate=True, fail_offsets=(), located_total=None):
        tmpl = _json("phenom_davita_refine_located.json")["refineSearch"]["data"]["jobs"][0]
        self.jobs = []
        for i in range(n):
            j = dict(tmpl)
            j.update(jobId=f"R{i:07d}", reqId=f"R{i:07d}", title=f"Patient Care Technician {i}")
            self.jobs.append(j)
        self.max_size, self.rotate = max_size, rotate
        self.fail_offsets, self.located_total = set(fail_offsets), located_total
        self.calls = []

    def order(self, located):
        if located or not self.rotate:
            return self.jobs
        out = list(self.jobs)
        random.Random(len(self.calls)).shuffle(out)
        return out

    def answer(self, body):
        self.calls.append(copy.deepcopy(body))
        located = bool((body.get("locationData") or {}).get("latitude"))
        frm, size = int(body.get("from") or 0), min(int(body.get("size") or 10), self.max_size)
        if frm in self.fail_offsets and len(self.calls) > 1:
            return _R(500, "upstream")
        board = self.order(located)
        if located and self.located_total is not None:
            board = board[:self.located_total]
        page = board[frm:frm + size]
        return _R(200, {"refineSearch": {"status": 200, "hits": len(page), "totalHits": len(board),
                                         "data": {"jobs": page}}})


def _phenom_fake(monkeypatch, board, base="https://careers.example.org"):
    def fake_req(session, method, url, **kw):
        if method == "post" and url == f"{base}/widgets" and (kw.get("json") or {}).get("ddoKey") == "refineSearch":
            return _Ctx(board.answer(kw["json"]))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, _PHENOM_HTML))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)


def _list_calls(board):
    return board.calls[1:]          # calls[0] is the probe (size 50, no location)


def test_phenom_crawl_with_a_fixed_location_lists_every_job(monkeypatch):
    board = _PhenomBoard(1234)
    _phenom_fake(monkeypatch, board)
    jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len({j.job_id for j in jobs}) == 1234 == len(jobs)
    calls = _list_calls(board)
    assert [c["from"] for c in calls] == [0, 500, 1000]
    assert all(c["size"] == scraper.PHENOM_PAGE_SIZE == 500 for c in calls)
    assert all(c["locationData"] == scraper.PHENOM_FIXED_LOCATION for c in calls)
    assert board.calls[0]["locationData"] == {} and board.calls[0]["size"] == 50
    assert "Test Phenom" in scraper.COMPLETE_SYSTEMS and "Test Phenom" not in scraper.PARTIAL_SYSTEMS


def test_the_board_model_reproduces_the_10_14_shortfall_without_a_location():
    """Control: 50-row pages with no location, each from another address, the
    way the 10-14 run read DaVita: about 1 - 1/e of the board."""
    board = _PhenomBoard(3333)
    seen = set()
    for frm in range(0, 3333, 50):
        r = board.answer({"from": frm, "size": 50, "locationData": {}})
        seen.update(j["jobId"] for j in r._body["refineSearch"]["data"]["jobs"])
    assert 0.55 * 3333 < len(seen) < 0.70 * 3333          # 2,102 of 3,333 on 10-14


def test_phenom_one_page_board_is_a_complete_crawl(monkeypatch):
    # Roper (327) and Temple (449) fit in one 500-row page: totalHits equals the
    # page, which the old "total larger than the page" rule never believed.
    board = _PhenomBoard(327)
    _phenom_fake(monkeypatch, board)
    jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len(jobs) == 327 and len(_list_calls(board)) == 1
    assert "Test Phenom" in scraper.COMPLETE_SYSTEMS


def test_phenom_pages_by_what_a_clamping_site_serves(monkeypatch):
    board = _PhenomBoard(250, max_size=100)
    _phenom_fake(monkeypatch, board)
    jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len({j.job_id for j in jobs}) == 250
    assert [c["from"] for c in _list_calls(board)] == [0, 100, 200]
    assert "Test Phenom" in scraper.COMPLETE_SYSTEMS


def test_phenom_drops_the_location_when_it_would_filter_the_board(monkeypatch, caplog):
    board = _PhenomBoard(700, rotate=False, located_total=650)
    _phenom_fake(monkeypatch, board)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len({j.job_id for j in jobs}) == 700
    calls = _list_calls(board)
    assert calls[0]["locationData"] == scraper.PHENOM_FIXED_LOCATION
    assert all(c["locationData"] == {} for c in calls[1:]) and [c["from"] for c in calls[1:]] == [0, 500]
    assert any("the located search reports 650 of 700 jobs; paging without the fixed location" in m
               for m in caplog.messages)
    assert "Test Phenom" in scraper.COMPLETE_SYSTEMS


def test_phenom_skipped_page_makes_the_crawl_partial(monkeypatch, caplog):
    board = _PhenomBoard(1400, fail_offsets={500})
    _phenom_fake(monkeypatch, board)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len(jobs) == 900
    assert [c["from"] for c in _list_calls(board)] == [0, 500, 1000]
    assert "Test Phenom" in scraper.PARTIAL_SYSTEMS and "Test Phenom" not in scraper.COMPLETE_SYSTEMS
    assert "Test Phenom" in scraper.PARTIAL_BACKSTOP_SYSTEMS        # Layer 4 keeps the 7-day backstop
    assert any("Phenom Test Phenom: PARTIAL, 1 page(s) skipped, 900 listed of 1400 advertised" in m
               for m in caplog.messages)


# ══ 2. Talemetry (Jobvite Engage search.json) ═══════════════════════════════

_TAL_ENTRY = _json("talemetry_asante_search_p1.json")["entries"][0]


def _tal_entries(ids):
    return [{**_TAL_ENTRY, "id": str(i), "talemetry_job_id": str(i), "permalink": f"job-{i}"} for i in ids]


class _CurlResp:
    status_code = 200

    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def test_live_asante_entry_maps_to_a_row():
    page = _json("talemetry_asante_search_p1.json")
    assert page["total_entries"] == 257 and len(page["entries"]) == 3
    job = scraper._talemetry_job(page["entries"][0], "Asante Health System", "https://jobs.asante.org")
    assert (job.job_id, job.city, job.state, job.hospital_name) == (
        "18230356", "Medford", "OR", "Asante Rogue Regnl Med Center")
    assert job.url == "https://jobs.asante.org/jobs/18230356-patient-safety-monitor-arrmc-resource-pool"


def test_talemetry_reads_a_board_that_serves_per_page_1000_in_one_request(monkeypatch):
    calls = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        calls.append(dict(kw["params"]))
        n = min(int(kw["params"]["per_page"]), 257)
        return _CurlResp({"current_page": 1, "per_page": int(kw["params"]["per_page"]),
                          "total_entries": 257, "entries": _tal_entries(range(n))})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "Asante Health System", "https://jobs.asante.org"))
    assert len(jobs) == 257 and calls == [{"page": "1", "per_page": "1000"}]
    assert "Asante Health System" in scraper.COMPLETE_SYSTEMS


def test_talemetry_403_moves_to_the_next_tls_profile_and_keeps_it(monkeypatch):
    seen = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["page"])
        seen.append((page, impersonate))
        if impersonate == "chrome":
            raise RuntimeError("HTTP 403 direct")      # careers.uchealth.org to chrome150, 10-14
        return _CurlResp({"total_entries": 230, "entries": _tal_entries(range((page - 1) * 100, min(page * 100, 230)))})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "UCHealth", "https://careers.uchealth.org"))
    assert len(jobs) == 230
    assert seen == [(1, "chrome"), (1, "chrome131"), (2, "chrome131"), (3, "chrome131")]
    assert "UCHealth" in scraper.COMPLETE_SYSTEMS


def test_talemetry_failed_page_read_after_the_cool_down_is_a_complete_crawl(monkeypatch):
    waits, asked = [], []

    async def fake_sleep(seconds):
        waits.append(seconds)

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["page"])
        asked.append(page)
        if page == 5 and scraper.TALEMETRY_COOLDOWN_S not in waits:
            raise RuntimeError("HTTP 429 via proxy")
        return _CurlResp({"total_entries": 1770, "entries": _tal_entries(range((page - 1) * 100, min(page * 100, 1770)))})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "_retry_sleep", fake_sleep)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "Penn Medicine", "https://careers.pennmedicine.org"))
    assert len({j.job_id for j in jobs}) == 1770           # 400 on the 10-14 run
    assert asked == [1, 2, 3, 4, 5, 5, 5] + list(range(6, 19)) + [5]
    assert waits == [3.0, 6.0, scraper.TALEMETRY_COOLDOWN_S]
    assert "Penn Medicine" in scraper.COMPLETE_SYSTEMS and "Penn Medicine" not in scraper.PARTIAL_SYSTEMS


def test_talemetry_page_one_refused_reads_the_board_again_after_the_cool_down(monkeypatch):
    waits, asked = [], []

    async def fake_sleep(seconds):
        waits.append(seconds)

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["page"])
        asked.append(page)
        if scraper.TALEMETRY_COOLDOWN_S not in waits:
            raise RuntimeError("HTTP 403 direct")      # LifeBridge: 0 rows on 10-14
        return _CurlResp({"total_entries": 577, "entries": _tal_entries(range((page - 1) * 100, min(page * 100, 577)))})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "_retry_sleep", fake_sleep)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "LifeBridge Health", "https://jobs.lifebridgehealth.org"))
    assert len(jobs) == 577
    tries = scraper.TALEMETRY_PAGE_RETRIES + 1
    assert asked == [1] * tries + [2] * tries + [3] * tries + [1, 2, 3, 4, 5, 6]
    assert "LifeBridge Health" in scraper.COMPLETE_SYSTEMS


def test_talemetry_ids_lost_to_ties_at_page_boundaries_are_found_in_title_order(monkeypatch):
    """LifeBridge, live 10-14: 577 entries over 6 pages held 7 ids twice
    (198/200, 199/201, 298/301, ...), so 7 postings were never listed."""
    default = list(range(577))
    for a, b in ((99, 100), (199, 200)):                  # two ties swap across a boundary between requests
        default[a], default[b] = default[b], default[a]
    asked = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        p = kw["params"]
        page = int(p["page"])
        asked.append((page, p.get("sort_by")))
        if p.get("sort_by") == scraper.TALEMETRY_RESORT:
            order = sorted(range(577), key=lambda i: f"{i:05d}"[::-1])
        else:
            order = default if page % 2 == 0 else list(range(577))
        return _CurlResp({"total_entries": 577, "entries": _tal_entries(order[(page - 1) * 100:page * 100])})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "LifeBridge Health", "https://jobs.lifebridgehealth.org"))
    assert len({j.job_id for j in jobs}) == 577
    assert [a for a in asked if a[1] is None] == [(p, None) for p in range(1, 7)]
    assert any(s == scraper.TALEMETRY_RESORT for _, s in asked)
    assert "LifeBridge Health" in scraper.COMPLETE_SYSTEMS


def test_run_talemetry_lists_every_site_before_any_detail_pass(monkeypatch):
    events = []

    async def fake_scrape(session, system, base):
        events.append(("list", system))
        await asyncio.sleep(0)
        return [scraper._talemetry_job(_tal_entries([1])[0], system, base)]

    async def fake_detail(session, system, jobs, budget, fetch_one, label, **kw):
        events.append(("detail", system))
    monkeypatch.setattr(scraper, "scrape_talemetry", fake_scrape)
    monkeypatch.setattr(scraper, "_detail_pass", fake_detail)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    monkeypatch.setattr(scraper, "TALEMETRY_DESC_BUDGET", scraper._DescBudget(100))
    jobs = asyncio.run(scraper.run_talemetry(None))
    sites = list(scraper.TALEMETRY_SITES)
    assert len(jobs) == len(sites)
    assert events[:len(sites)] == [("list", s) for s in sites]
    assert sorted(events[len(sites):]) == sorted(("detail", s) for s in sites)


# ══ 3. SelectMinds: the search pages by path ══════════════════════════════

_SM_BASE = "https://mainline.referrals.selectminds.com"
_SM_SEARCH = f"{_SM_BASE}/jobs/search/4365639"


def _sm_fake(monkeypatch, pages, latest=None):
    """pages: {1: html, 2: html, ...} for /jobs/search/ (page 1, redirected to
    the search id) and /jobs/search/<id>/page<N>; an int answers that status."""
    asked = []

    def fake_req(session, method, url, **kw):
        asked.append(url)
        if url == f"{_SM_BASE}/jobs/search/":
            body, final = pages.get(1, ""), _SM_SEARCH
        elif url.startswith(_SM_SEARCH + "/page"):
            body, final = pages.get(int(url.rsplit("page", 1)[1]), pages.get(1, "")), url
        elif url == f"{_SM_BASE}/latest-jobs":
            body, final = latest or "", url
        else:
            body, final = 404, url
        if isinstance(body, int):
            return _Ctx(_R(body, "", final))
        return _Ctx(_R(200, body, final))
    monkeypatch.setattr(scraper, "req", fake_req)
    return asked


def _sm_board(total, pages):
    p1, p2 = _read("selectminds_mainline_search_p1.html"), _read("selectminds_mainline_search_p2.html")
    fix = lambda h: (h.replace('class="total_results">346<', f'class="total_results">{total}<')
                      .replace('class="ghost">35.0<', f'class="ghost">{pages}.0<'))
    return fix(p1), fix(p2)


def test_selectminds_search_cards_parse_location_and_category():
    jobs = scraper._parse_selectminds_page(_read("selectminds_mainline_search_p1.html"), "Main Line Health", _SM_BASE)
    assert [(j.job_id, j.title, j.city, j.state, j.specialty) for j in jobs] == [
        ("28195", "Medical Assistant", "Glen Mills", "PA", "Medical Assistant"),
        ("28090", jobs[1].title, jobs[1].city, "PA", jobs[1].specialty)]
    assert all(j.city and j.specialty for j in jobs)
    assert jobs[0].url == f"{_SM_BASE}/jobs/medical-assistant-28195"


def test_selectminds_pages_the_search_by_path_and_claims_complete(monkeypatch):
    p1, p2 = _sm_board(4, 2)
    asked = _sm_fake(monkeypatch, {1: p1, 2: p2})
    jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert [j.job_id for j in jobs] == ["28195", "28090", "29180", "28869"]
    assert asked == [f"{_SM_BASE}/jobs/search/", f"{_SM_SEARCH}/page2"]
    assert "Main Line Health" in scraper.COMPLETE_SYSTEMS


def test_selectminds_page_that_wraps_to_page_one_ends_the_crawl(monkeypatch, caplog):
    p1, p2 = _read("selectminds_mainline_search_p1.html"), _read("selectminds_mainline_search_p2.html")
    asked = _sm_fake(monkeypatch, {1: p1, 2: p2})          # 346 advertised, 35 pages; page 3 repeats page 1
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert len(jobs) == 4 and asked[-1] == f"{_SM_SEARCH}/page3"
    assert "Main Line Health" not in scraper.COMPLETE_SYSTEMS
    assert any("SelectMinds Main Line Health: 4 of 346 cards read" in m for m in caplog.messages)


def test_selectminds_failed_page_makes_the_crawl_partial(monkeypatch):
    p1, p2 = _sm_board(6, 3)
    _sm_fake(monkeypatch, {1: p1, 2: 503, 3: p2})
    jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert len(jobs) == 4
    assert "Main Line Health" in scraper.PARTIAL_SYSTEMS and "Main Line Health" not in scraper.COMPLETE_SYSTEMS
    assert "Main Line Health" in scraper.PARTIAL_BACKSTOP_SYSTEMS


def test_selectminds_falls_back_to_latest_jobs_when_the_search_has_no_card(monkeypatch):
    latest = open(os.path.join(FIXTURES, "cov3", "selectminds_mainline_cards.html"), encoding="utf-8").read()
    asked = _sm_fake(monkeypatch, {1: "<html></html>"}, latest=latest)
    jobs = asyncio.run(scraper.scrape_selectminds(None, "Main Line Health", "mainline"))
    assert [j.job_id for j in jobs] == ["29120", "29106"]
    assert asked == [f"{_SM_BASE}/jobs/search/", f"{_SM_BASE}/latest-jobs"]
    assert "Main Line Health" not in scraper.COMPLETE_SYSTEMS


# ══ 4. Complete claims: Greenhouse, ApplicantPro, UKG, NeoGov, TalentBrew ═══

def _one_answer(monkeypatch, body, status=200):
    asked = []

    def fake_req(session, method, url, **kw):
        asked.append((method, url, kw))
        return _Ctx(_R(status, body(len(asked), kw) if callable(body) else body))
    monkeypatch.setattr(scraper, "req", fake_req)
    return asked


def test_greenhouse_board_read_whole_is_complete(monkeypatch):
    live = _json("greenhouse_onemedical_jobs.json")          # meta.total 396, two of its jobs
    _one_answer(monkeypatch, live)
    jobs = asyncio.run(scraper.scrape_greenhouse(None, "One Medical", "onemedical"))
    assert len(jobs) == 2 and "One Medical" not in scraper.COMPLETE_SYSTEMS
    whole = {**live, "meta": {"total": 2}}
    _one_answer(monkeypatch, whole)
    asyncio.run(scraper.scrape_greenhouse(None, "One Medical", "onemedical"))
    assert "One Medical" in scraper.COMPLETE_SYSTEMS


def test_applicantpro_board_read_whole_is_complete(monkeypatch):
    live = _json("applicantpro_cayuga_jobs.json")             # jobCount 76, two of its jobs
    _one_answer(monkeypatch, live)
    jobs = asyncio.run(scraper.scrape_applicantpro(None, "Cayuga Health", ("cayugahealthsystem", "17888")))
    assert len(jobs) == 2 and "Cayuga Health" not in scraper.COMPLETE_SYSTEMS
    whole = copy.deepcopy(live)
    whole["data"]["jobCount"] = 2
    _one_answer(monkeypatch, whole)
    asyncio.run(scraper.scrape_applicantpro(None, "Cayuga Health", ("cayugahealthsystem", "17888")))
    assert "Cayuga Health" in scraper.COMPLETE_SYSTEMS


def _ukg_items(n):
    tmpl = json.load(open(os.path.join(FIXTURES, "ukg_search.json"), encoding="utf-8"))["opportunities"][0]
    return [{**tmpl, "Id": f"opp-{i}", "Title": f"RN {i}"} for i in range(n)]


def test_ukg_board_read_whole_is_complete(monkeypatch):
    data = json.load(open(os.path.join(FIXTURES, "ukg_search.json"), encoding="utf-8"))   # totalCount 2
    _one_answer(monkeypatch, data)
    cfg = scraper.UKG_ORGS["North Carolina Specialty Hospital"]
    jobs = asyncio.run(scraper.scrape_ukg(None, "North Carolina Specialty Hospital", cfg))
    assert len(jobs) == 2 and "North Carolina Specialty Hospital" in scraper.COMPLETE_SYSTEMS


def test_ukg_page_that_fails_after_the_first_makes_the_crawl_partial(monkeypatch):
    asked = []

    def fake_req(session, method, url, **kw):
        asked.append(kw["json"]["opportunitySearch"]["Skip"])
        if len(asked) == 1:
            return _Ctx(_R(200, {"opportunities": _ukg_items(scraper._UKG_PAGE), "totalCount": 120}))
        return _Ctx(_R(503, "busy"))
    monkeypatch.setattr(scraper, "req", fake_req)
    jobs = asyncio.run(scraper.scrape_ukg(None, "Test UKG", ("https://recruiting.ultipro.com/X", "g", "NC")))
    assert len(jobs) == scraper._UKG_PAGE and asked == [0, scraper._UKG_PAGE]
    assert "Test UKG" in scraper.PARTIAL_SYSTEMS and "Test UKG" not in scraper.COMPLETE_SYSTEMS
    assert "Test UKG" in scraper.PARTIAL_BACKSTOP_SYSTEMS


def test_neogov_board_read_whole_is_complete_and_a_failed_page_partial(monkeypatch):
    html = open(os.path.join(FIXTURES, "neogov_page.html"), encoding="utf-8").read()   # 72 advertised, 2 cards
    whole = re.sub(r'(id="job-postings-number">)\s*72', r"\g<1>2", html)
    cfg = ("riverside", "CA", "Moreno Valley", "Riverside University Health System - Medical Center", None)

    async def one_page(session, agency, page):
        return 200, whole
    monkeypatch.setattr(scraper, "_neogov_fetch_page", one_page)
    jobs = asyncio.run(scraper.scrape_neogov(None, "Riverside University Health System", cfg))
    assert len(jobs) == 2 and "Riverside University Health System" in scraper.COMPLETE_SYSTEMS

    scraper.COMPLETE_SYSTEMS.clear()

    async def second_fails(session, agency, page):
        return (200, html) if page == 1 else (503, "")
    monkeypatch.setattr(scraper, "_neogov_fetch_page", second_fails)
    asyncio.run(scraper.scrape_neogov(None, "Riverside University Health System", cfg))
    assert "Riverside University Health System" in scraper.PARTIAL_SYSTEMS
    assert "Riverside University Health System" in scraper.PARTIAL_BACKSTOP_SYSTEMS
    assert "Riverside University Health System" not in scraper.COMPLETE_SYSTEMS


def _tb_fake(monkeypatch, pages):
    """pages: {page: html fragment or Exception} for the /results endpoint."""
    asked = []

    def fake_req(session, method, url, **kw):
        if url.endswith("/results"):
            p = int(kw["params"]["CurrentPage"])
            asked.append(p)
            body = pages.get(p, "")
            if isinstance(body, Exception):
                return _Ctx(body)
            return _Ctx(_R(200, json.dumps({"results": body, "hasJobs": bool(body), "hasContent": True})))
        return _Ctx(_R(200, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return asked


def test_talentbrew_board_read_whole_is_complete(monkeypatch):
    html = open(os.path.join(FIXTURES, "cov3", "talentbrew_hmh_cards.html"), encoding="utf-8").read()
    assert 'data-total-results="1695"' in html
    _tb_fake(monkeypatch, {1: html})
    jobs = asyncio.run(scraper.scrape_talentbrew(None, "Hackensack Meridian Health",
                                                 "https://jobs.hackensackmeridianhealth.org/search-jobs", 100))
    assert len(jobs) == 2 and "Hackensack Meridian Health" not in scraper.COMPLETE_SYSTEMS   # 2 of 1,695
    _tb_fake(monkeypatch, {1: html.replace('data-total-results="1695"', 'data-total-results="2"')})
    asyncio.run(scraper.scrape_talentbrew(None, "Hackensack Meridian Health",
                                          "https://jobs.hackensackmeridianhealth.org/search-jobs", 100))
    assert "Hackensack Meridian Health" in scraper.COMPLETE_SYSTEMS


def test_talentbrew_crawl_that_stops_on_a_failed_page_is_partial(monkeypatch):
    html = open(os.path.join(FIXTURES, "cov3", "talentbrew_hmh_cards.html"), encoding="utf-8").read()
    asked = _tb_fake(monkeypatch, {1: html, 2: RuntimeError("HTTP 403")})
    jobs = asyncio.run(scraper.scrape_talentbrew(None, "Hackensack Meridian Health",
                                                 "https://jobs.hackensackmeridianhealth.org/search-jobs", 2))
    assert len(jobs) == 2 and asked == [1, 2]
    assert "Hackensack Meridian Health" in scraper.PARTIAL_SYSTEMS
    assert "Hackensack Meridian Health" in scraper.PARTIAL_BACKSTOP_SYSTEMS
    assert "Hackensack Meridian Health" not in scraper.COMPLETE_SYSTEMS


# ══ 5. Review fixes (2026-10-14) ═══════════════════════════════════════════
# a) report_partial keeps Layer 4's 7-day backstop. These six adapters used to
#    report nothing and the yield guard (backstop included) held them; as plain
#    PARTIAL_SYSTEMS entries a board refused every night (Penn Medicine and
#    UCHealth from Railway) would never retire a closed row.

import database        # noqa: E402
import retire_guard    # noqa: E402
from datetime import datetime, timedelta, timezone   # noqa: E402

_NOW = datetime(2026, 10, 15, 2, 0, tzinfo=timezone.utc)


def _l4_rows(system, n, start_id=1, misses=0, age_days=1):
    ts = (_NOW - timedelta(days=age_days)).isoformat()
    return [{"id": start_id + i, "job_id": f"{system}-{start_id + i}", "hospital_system": system,
             "consecutive_scrape_misses": misses, "scraped_at": ts} for i in range(n)]


def test_report_partial_is_guarded_with_the_backstop_and_other_partials_stay_excluded():
    scraper.report_partial("Penn Medicine")
    scraper.PARTIAL_SYSTEMS.add("NYC Health + Hospitals")      # PeopleSoft's 500-row cap, as before
    exclude, guard = scraper.layer4_partial_sets()
    assert guard == {"Penn Medicine"}
    assert exclude == {"NYC Health + Hospitals", "HCA Healthcare"}
    assert scraper.layer4_partial_sets(hca_nightly=True)[0] == {"NYC Health + Hospitals"}
    # still not swept tonight and never a complete crawl
    assert "Penn Medicine" in scraper.PARTIAL_SYSTEMS
    assert not scraper.report_complete("Penn Medicine", 1770, 1770, feed="TALEMETRY_SITES")
    # alias keys map to the canonical name; a full exclusion of the same system wins
    key, canon = next((k, v) for k, v in scraper.HOSPITAL_SYSTEM_ALIASES.items() if k != v)
    scraper.report_partial(key)
    assert canon in scraper.layer4_partial_sets()[1]
    scraper.PARTIAL_SYSTEMS.add(canon)
    exclude, guard = scraper.layer4_partial_sets()
    assert canon in exclude and canon not in guard


def test_layer4_partial_crawl_freezes_recent_rows_and_retires_past_the_backstop():
    """A Talemetry night that read 90 of 100 rows with pages unread: above the
    80% ratio, so only the partial report guards it. Unseen rows seen within
    7 days keep their count; rows nobody has seen for 9 days retire."""
    seen = _l4_rows("Penn Medicine", 90)
    recent = _l4_rows("Penn Medicine", 6, start_id=91, misses=2)
    stale = _l4_rows("Penn Medicine", 4, start_id=97, misses=2, age_days=9)
    active = seen + recent + stale
    keys = {("Penn Medicine", r["job_id"]) for r in seen}
    plan = retire_guard.plan_layer4(active, keys, 3, now=_NOW, partial_systems={"Penn Medicine"})
    g = plan["guarded"]["Penn Medicine"]
    assert (g["reason"], g["frozen"], g["backstop"]) == ("partial crawl", 6, 4)
    assert sorted(plan["deactivate_ids"]) == [97, 98, 99, 100]
    # no report: 90% is healthy and every unseen row counts (the sweep's rule too)
    plain = retire_guard.plan_layer4(active, keys, 3, now=_NOW)
    assert plain["guarded"] == {} and len(plain["deactivate_ids"]) == 10
    # an exclusion (what a PARTIAL_SYSTEMS entry meant): nothing retires, ever
    frozen = retire_guard.plan_layer4(active, keys, 3, now=_NOW, exclude_systems={"Penn Medicine"})
    assert frozen["deactivate_ids"] == [] and frozen["excluded_rows"] == 100
    # under the ratio the ratio's own reason is kept (the zero-yield ALARM reads it)
    dark = retire_guard.plan_layer4(active, set(), 3, now=_NOW, partial_systems={"Penn Medicine"})
    assert dark["guarded"]["Penn Medicine"]["reason"] == "zero-yield"
    # a partial report outranks a complete claim
    both = retire_guard.plan_layer4(active, keys, 3, now=_NOW, partial_systems={"Penn Medicine"},
                                    complete_systems={"Penn Medicine"})
    assert both["complete_bypassed"] == {} and "Penn Medicine" in both["guarded"]


def test_mark_inactive_jobs_passes_the_partial_systems_to_the_plan(monkeypatch, caplog):
    rows = _l4_rows("UCHealth", 18) + _l4_rows("UCHealth", 2, start_id=19, misses=2, age_days=9)
    updates = []

    class _Q:
        def __init__(self, rows):
            self._rows, self._patch = rows, None

        def select(self, *_a, **_k): return self
        def eq(self, *_a): return self

        def gt(self, _col, last_id):
            self._rows = [r for r in self._rows if r["id"] > last_id]
            return self

        def order(self, *_a, **_k): return self
        def limit(self, _n): return self

        def update(self, patch):
            self._patch = patch
            return self

        def in_(self, _col, ids):
            updates.append((self._patch, list(ids)))
            return self

        def execute(self):
            class R: pass
            r = R(); r.data = list(self._rows) if self._patch is None else []
            return r

    class _DB:
        def table(self, _name): return _Q(rows)
    monkeypatch.setattr(database, "client", lambda: _DB())
    seen = [{"hospital_system": "UCHealth", "job_id": f"UCHealth-{i}"} for i in range(1, 17)]   # 16 of 20
    with caplog.at_level(logging.WARNING):
        out = database.mark_inactive_jobs(seen, miss_threshold=3, exclude_systems=set(),
                                          partial_systems={"UCHealth"})
    assert out["guarded_systems"] == ["UCHealth"] and out["deactivated"] == 2
    assert [ids for p, ids in updates if p.get("is_active") is False] == [[19, 20]]
    assert any("LAYER4 GUARD UCHealth: partial crawl 16/20" in m for m in caplog.messages)

