"""Push 10 (2026-10-07): list loops that stopped early, and the Layer 4 safety
additions (retire_guard.py).

The 10-06 run held 15,649 rows behind the yield guard: UHG's TalentBrew crawl
ended at its first bad page (477 of 6,315), ten Phenom tenants and four
HealthcareSource tenants came back partial, CHS stopped at exactly 14 pages,
the Talemetry sites stopped after page 1. These tests drive the REAL adapters
against fakes of req() / _curl_fetch (no network, no database) and check that
a failed page is skipped, that the crawl ends on the site's advertised total,
that the stop reason is logged at WARNING, and that a complete crawl is
reported. The Layer 4 parts are pure (retire_guard) or use a fake client."""
import asyncio
import io
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

import retire_guard
import scraper
from retire_guard import load_yield_history, plan_layer4, save_yield_history, zero_yield_alarms


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
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


# ── 1. UnitedHealth Group (TalentBrew HTML) ─────────────────────────────────

def _uhg_page(ids, total=None, pages=None):
    head = (f'<section data-total-results="{total}" data-total-pages="{pages}" data-records-per-page="15">'
            if total is not None else "<section>")
    links = "".join(f'<a href="/job/eden-prairie/rn-{i}/34088/{i}">RN {i}</a>' for i in ids)
    return head + links + "</section>"


def _uhg_fake(monkeypatch, pages):
    """pages: {page_number: _R or Exception}; returns the pages requested."""
    asked = []

    def fake_req(session, method, url, **kw):
        p = int(url.split("p=")[1])
        asked.append(p)
        return _Ctx(pages.get(p, _R(200, _uhg_page([]))))
    monkeypatch.setattr(scraper, "req", fake_req)
    return asked


def test_uhg_skips_a_failed_page_and_ends_on_the_advertised_total(monkeypatch, caplog):
    pages = {1: _R(200, _uhg_page([1, 2], 8, 4)), 2: _R(503, "busy"),
             3: _R(200, _uhg_page([5, 6], 8, 4)), 4: _R(200, _uhg_page([7, 8], 8, 4)),
             5: _R(200, _uhg_page([9, 10], 8, 4))}
    asked = _uhg_fake(monkeypatch, pages)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert [j.job_id for j in jobs] == ["1", "2", "5", "6", "7", "8"]
    assert asked == [1, 2, 3, 4]                       # 4 pages advertised: page 5 never asked
    assert any("page 2 failed (HTTP 503)" in m for m in caplog.messages)
    assert any("UHG: stopped: advertised 4 pages read; 6 jobs listed of 8 advertised" in m
               for m in caplog.messages)                # short of the total: at WARNING
    assert "UnitedHealth Group" not in scraper.COMPLETE_SYSTEMS


def test_uhg_stops_after_three_consecutive_failures_with_the_reason(monkeypatch, caplog):
    pages = {1: _R(200, _uhg_page([1], 100, 7)), 2: asyncio.TimeoutError(),
             3: _R(429, ""), 4: RuntimeError("boom"), 5: _R(200, _uhg_page([5], 100, 7))}
    asked = _uhg_fake(monkeypatch, pages)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert [j.job_id for j in jobs] == ["1"] and asked == [1, 2, 3, 4]
    assert any("3 consecutive page failures, last RuntimeError: boom on page 4" in m for m in caplog.messages)


def test_uhg_thin_page_after_page_five_no_longer_ends_the_crawl(monkeypatch):
    # Seven pages advertised; page 6 carries one new id, which the old
    # "fewer than half a page after page 5" rule took as the end of the board.
    pages = {p: _R(200, _uhg_page(range(p * 10, p * 10 + (1 if p == 6 else 2)), 13, 7)) for p in range(1, 8)}
    asked = _uhg_fake(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert asked == list(range(1, 8)) and len(jobs) == 13
    assert "UnitedHealth Group" in scraper.COMPLETE_SYSTEMS


def test_uhg_without_an_advertised_total_ends_on_two_empty_pages(monkeypatch):
    asked = _uhg_fake(monkeypatch, {1: _R(200, _uhg_page([1, 2])), 2: _R(200, _uhg_page([3]))})
    jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert len(jobs) == 3 and asked == [1, 2, 3, 4]
    assert "UnitedHealth Group" not in scraper.COMPLETE_SYSTEMS


def test_uhg_live_page_shape_is_read(fixture_text):
    # The attributes the live page carried on 2026-10-07 (uhg_probe).
    html = ('<section id="search-results" data-total-results="5573" data-total-job-results="5573" '
            'data-total-pages="372" data-current-page="1" data-records-per-page="15">')
    assert scraper.UHG_TOTAL_RX.search(html).group(1) == "5573"
    assert scraper.UHG_PAGES_RX.search(html).group(1) == "372"


# ── 2. Phenom list loop: a failed page is skipped, not the end of the crawl ─

def _phenom_job(i):
    return {"id": f"J{i}", "title": f"RN {i}", "city": "Denver", "state": "CO",
            "applyUrl": f"https://careers.example.org/job/J{i}"}


def _phenom_fake(monkeypatch, page_results, total):
    """page_results: {offset: _R or Exception} for the /api/jobs POST list
    pages; the probe (size 10) always answers with jobs. Everything else 404s."""
    offsets = []

    def fake_req(session, method, url, **kw):
        body = kw.get("json") or {}
        if url.endswith("/api/jobs") and method == "post":
            if body.get("size") == 10:
                return _Ctx(_R(200, {"jobs": [_phenom_job(i) for i in range(10)], "total": total}))
            offsets.append(body["from"])
            return _Ctx(page_results.get(body["from"], _R(200, {"jobs": [], "total": total})))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return offsets


def _phenom_page(start, n, total):
    return _R(200, {"jobs": [_phenom_job(i) for i in range(start, start + n)], "total": total})


def test_phenom_skips_a_failed_page_and_keeps_paging(monkeypatch, caplog):
    offsets = _phenom_fake(monkeypatch, {0: _phenom_page(0, 50, 150), 50: _R(500, "upstream"),
                                         100: _phenom_page(100, 50, 150)}, 150)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert offsets == [0, 50, 100] and len(jobs) == 100
    assert any("Phenom Test Phenom: page at offset 50 failed (RuntimeError: HTTP 500)" in m for m in caplog.messages)
    assert any("listed 100 of 150 advertised" in m for m in caplog.messages)
    assert "Test Phenom" not in scraper.COMPLETE_SYSTEMS


def test_phenom_stops_after_three_consecutive_failures(monkeypatch, caplog):
    offsets = _phenom_fake(monkeypatch, {0: _phenom_page(0, 50, 300), 50: asyncio.TimeoutError(),
                                         100: _R(429, ""), 150: _R(502, ""), 200: _phenom_page(200, 50, 300)}, 300)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert offsets == [0, 50, 100, 150] and len(jobs) == 50
    assert any("stopping at offset 150 after 3 consecutive page failures; 50 rows listed of 300 advertised" in m
               for m in caplog.messages)


def test_phenom_complete_crawl_is_reported(monkeypatch):
    _phenom_fake(monkeypatch, {0: _phenom_page(0, 50, 80), 50: _phenom_page(50, 30, 80)}, 80)
    jobs = asyncio.run(scraper.scrape_phenom(None, "Test Phenom", "https://careers.example.org"))
    assert len(jobs) == 80 and "Test Phenom" in scraper.COMPLETE_SYSTEMS


# ── 3. CHS (WPJobBoard cards) ───────────────────────────────────────────────

def _card(i, broken=False):
    title = "" if broken else f'<h5 class="job-title"><a href="https://www.careershealthcare.com/job/{i}">RN {i}</a></h5>'
    return (f'<div class="chs-job" data-id="{i}"> <h3>Physicians Regional</h3> <h6 class="job-shift"> Full Time </h6> '
            f'<h5 class="job-location">Naples, FL</h5> {title}</div>')


def _chs_fake(monkeypatch, pages, total=5):
    """pages: {offset: _R or Exception} for api.careershealthcare.com; the
    probe uses offset 0 too, and any other offset answers an empty payload
    with meta.total = total. Returns the offsets requested."""
    asked = []

    async def nop(*a, **k):
        return None
    monkeypatch.setattr(scraper, "_board_detail_passes", nop)

    def fake_req(session, method, url, **kw):
        off = kw["params"]["offset"]
        asked.append(off)
        if "api.careershealthcare.com" not in url:
            return _Ctx(_R(404, ""))
        return _Ctx(pages.get(off, _R(200, {"payload": [], "meta": {"total": total}})))
    monkeypatch.setattr(scraper, "req", fake_req)
    return asked


def test_chs_advances_by_cards_returned_stops_on_empty_payload_and_warns_on_unparsed(monkeypatch, caplog):
    pages = {0: _R(200, {"payload": [_card(1), _card(2), _card(3, broken=True)], "meta": {"total": 5}}),
             3: _R(200, {"payload": [_card(4), _card(5)], "meta": {"total": 5}})}
    asked = _chs_fake(monkeypatch, pages)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.run_chs(None))
    assert asked == [0, 0, 3, 5]                       # probe, then pages; the unparsed card still counts
    assert [j.job_id for j in jobs] == ["1", "2", "4", "5"]
    assert any("CHS: offset 0: 1 of 3 cards unparsed" in m for m in caplog.messages)
    assert any("CHS: stopped: empty payload at offset 5; 4 jobs listed of 5 advertised" in m
               for m in caplog.messages)
    assert "Community Health Systems" not in scraper.COMPLETE_SYSTEMS


def test_chs_skips_a_failed_page_and_reports_a_complete_crawl(monkeypatch, caplog):
    pages = {0: _R(200, {"payload": [_card(1), _card(2)], "meta": {"total": 4}}),
             2: _R(502, "bad gateway"),
             62: _R(200, {"payload": [_card(3), _card(4)], "meta": {"total": 4}})}
    asked = _chs_fake(monkeypatch, pages, total=4)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.run_chs(None))
    assert asked == [0, 0, 2, 62, 64]                  # a failed page is skipped by LIMIT (60)
    assert [j.job_id for j in jobs] == ["1", "2", "3", "4"]
    assert any("CHS: offset 2 failed (RuntimeError: HTTP 502)" in m for m in caplog.messages)
    assert "Community Health Systems" in scraper.COMPLETE_SYSTEMS


def test_chs_stops_after_three_consecutive_failures(monkeypatch, caplog):
    pages = {0: _R(200, {"payload": [_card(1)], "meta": {"total": 300}}),
             1: asyncio.TimeoutError(), 61: _R(503, ""), 121: _R(500, ""),
             181: _R(200, {"payload": [_card(9)], "meta": {"total": 300}})}
    asked = _chs_fake(monkeypatch, pages, total=300)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.run_chs(None))
    assert asked == [0, 0, 1, 61, 121] and [j.job_id for j in jobs] == ["1"]
    assert any("CHS: stopped: 3 consecutive page failures, last at offset 121; 1 jobs listed of 300 advertised" in m
               for m in caplog.messages)


# ── 4. Talemetry / Jobvite Engage: retry with backoff, status in the log ────

ENTRY = {"id": "1", "permalink": "rn", "title": "RN",
         "location": {"locality": "Aurora", "region_abbr": "CO", "name": "UCHealth Anschutz"}}


class _Resp:
    def __init__(self, payload):
        self._p = payload
        self.status_code = 200

    def json(self):
        return self._p


def _entries(start, n):
    return [{**ENTRY, "id": str(start + i), "permalink": f"job-{start + i}"} for i in range(n)]


def test_talemetry_retries_a_failed_page_with_backoff_then_continues(monkeypatch, caplog):
    attempts, waits = [], []
    pages = {1: _entries(0, 100), 2: _entries(100, 100), 3: _entries(200, 30)}

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["page"])
        attempts.append(page)
        if page == 2 and attempts.count(2) <= 2:
            raise RuntimeError("HTTP 403 direct")
        return _Resp({"total_entries": 230, "entries": pages[page]})

    async def fake_sleep(seconds):
        waits.append(seconds)
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "_retry_sleep", fake_sleep)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_talemetry(None, "UCHealth", "https://careers.uchealth.org"))
    assert len(jobs) == 230 and attempts == [1, 2, 2, 2, 3]
    assert waits == [3.0, 6.0]
    assert any("Talemetry UCHealth: page 2 failed (HTTP 403); retry 1/2 in 3s" in m for m in caplog.messages)
    assert "UCHealth" in scraper.COMPLETE_SYSTEMS


def test_talemetry_reports_site_total_vs_fetched_when_it_stops_early(monkeypatch, caplog):
    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        if kw["params"]["page"] == "2":
            raise RuntimeError("HTTP 403 direct")
        return _Resp({"total_entries": 1296, "entries": _entries(0, 100)})
    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_talemetry(None, "UCHealth", "https://careers.uchealth.org"))
    assert len(jobs) == 100
    assert any("Talemetry UCHealth: 100 of site total 1296 fetched; stopped early: page 2 failed 3 times (HTTP 403)"
               in m for m in caplog.messages)
    assert "UCHealth" not in scraper.COMPLETE_SYSTEMS


# ── 5. HealthcareSource: stable sort, dedupe, skip a failed page ────────────

def _hit(i, date="2026-10-01T00:00:00Z"):
    return {"_id": f"t_{i}", "_source": {"title": f"RN {i}", "datePosted": date,
                                         "jobLocation": {"address": {"addressLocality": "Salina", "addressRegion": "KS"}},
                                         "userArea": {"jobPostingID": str(i)}}}


def _hcs_fake(monkeypatch, responder):
    calls = []

    def fake_req(session, method, url, **kw):
        calls.append((kw["params"]["from"], kw["json"]))
        return _Ctx(responder(kw["params"]["from"], kw["json"]))
    monkeypatch.setattr(scraper, "req", fake_req)
    return calls


def test_hcs_sends_a_stable_sort_dedupes_and_pages_to_the_total(monkeypatch):
    monkeypatch.setattr(scraper, "_HCS_PAGE", 2)

    def responder(frm, body):
        assert body["sort"] == [{"datePosted": {"order": "desc"}}, "_doc"]
        pages = {0: [_hit(1), _hit(2)], 2: [_hit(2), _hit(3)], 4: [_hit(4)]}
        return _R(200, {"hits": {"total": {"value": 5, "relation": "eq"}, "hits": pages.get(frm, [])}})
    calls = _hcs_fake(monkeypatch, responder)
    jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Salina Regional Health Center", "srhc"))
    assert [j.job_id for j in jobs] == ["1", "2", "3", "4"]        # the repeated hit 2 is deduped
    assert [c[0] for c in calls] == [0, 2, 4]
    assert "must" in scraper._HCS_BODY["query"]["bool"]
    assert "Salina Regional Health Center" not in scraper.COMPLETE_SYSTEMS   # 4 of 5


def test_hcs_retries_unsorted_on_a_500_and_skips_a_failed_page(monkeypatch, caplog):
    monkeypatch.setattr(scraper, "_HCS_PAGE", 2)

    def responder(frm, body):
        if "sort" in body:
            return _R(500, "Cannot perform runtime binding on a null reference")
        if frm == 2:
            return asyncio.TimeoutError()
        pages = {0: [_hit(1), _hit(2)], 4: [_hit(5), _hit(6)]}
        return _R(200, {"hits": {"total": {"value": 6}, "hits": pages.get(frm, [])}})
    calls = _hcs_fake(monkeypatch, responder)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_healthcaresource(None, "Salina Regional Health Center", "srhc"))
    assert [c[0] for c in calls] == [0, 0, 2, 4] and "sort" not in calls[1][1]
    assert [j.job_id for j in jobs] == ["1", "2", "5", "6"]
    assert any("retrying this tenant unsorted" in m for m in caplog.messages)
    assert any("HealthcareSource Salina Regional Health Center: page at offset 2 failed (TimeoutError" in m
               for m in caplog.messages)
    assert "Salina Regional Health Center" not in scraper.COMPLETE_SYSTEMS   # 4 listed of 6 advertised


def test_hcs_stops_after_three_consecutive_failures(monkeypatch, caplog):
    monkeypatch.setattr(scraper, "_HCS_PAGE", 2)

    def responder(frm, body):
        if frm == 0:
            return _R(200, {"hits": {"total": {"value": 20}, "hits": [_hit(1), _hit(2)]}})
        return _R(503, "")
    calls = _hcs_fake(monkeypatch, responder)
    with caplog.at_level(logging.WARNING):
        jobs = asyncio.run(scraper.scrape_healthcaresource(None, "CRMC Health", "crmchealth"))
    assert [c[0] for c in calls] == [0, 2, 4, 6] and len(jobs) == 2
    assert any("stopping at offset 6 after 3 consecutive page failures; 2 rows listed of 20 advertised" in m
               for m in caplog.messages)


# ── 6. Layer 4 safety: complete crawls, the UHG backstop exemption, ALARMs ──

NOW = datetime(2026, 10, 7, 4, 0, tzinfo=timezone.utc)
FRESH = (NOW - timedelta(hours=20)).isoformat()
STALE = (NOW - timedelta(days=8)).isoformat()


def _rows(system, n, misses=0, scraped_at=FRESH, start_id=1):
    return [{"id": start_id + i, "job_id": f"{system}-{start_id + i}", "hospital_system": system,
             "consecutive_scrape_misses": misses, "scraped_at": scraped_at} for i in range(n)]


def _keys(system, ids):
    return {(system, f"{system}-{i}") for i in ids}


def test_report_complete_rules():
    assert scraper.report_complete("A", 98, 100) and "A" in scraper.COMPLETE_SYSTEMS
    assert not scraper.report_complete("A", 97, 100) and "A" not in scraper.COMPLETE_SYSTEMS
    assert not scraper.report_complete("B", 10, None) and not scraper.report_complete("B", 10, 0)
    assert not scraper.report_complete("B", 10, "n/a")
    scraper.PARTIAL_SYSTEMS.add("C")
    assert not scraper.report_complete("C", 100, 100) and "C" not in scraper.COMPLETE_SYSTEMS


def test_complete_crawl_bypasses_the_yield_guard():
    active = _rows("Prime Healthcare", 100, misses=2)
    keys = _keys("Prime Healthcare", range(1, 71))                 # 70%: guarded without the report
    plan = plan_layer4(active, keys, 3, now=NOW)
    assert "Prime Healthcare" in plan["guarded"] and plan["deactivate_ids"] == []
    plan = plan_layer4(active, keys, 3, now=NOW, complete_systems={"Prime Healthcare"})
    assert plan["guarded"] == {}
    assert plan["complete_bypassed"]["Prime Healthcare"] == {"reason": "partial-yield", "active": 100, "yield": 70}
    assert sorted(plan["deactivate_ids"]) == list(range(71, 101))   # closed postings retire as usual


def test_backstop_exempt_uhg_keeps_rows_frozen_past_the_backstop_while_guarded():
    assert "UnitedHealth Group" in retire_guard.BACKSTOP_EXEMPT
    active = (_rows("UnitedHealth Group", 50, misses=2, scraped_at=STALE)
              + _rows("UnitedHealth Group", 10, misses=0, start_id=51))
    plan = plan_layer4(active, _keys("UnitedHealth Group", range(51, 61)), 3, now=NOW)   # 10 of 60
    g = plan["guarded"]["UnitedHealth Group"]
    assert g["reason"] == "partial-yield" and g["frozen"] == 50 and g["exempt"] == 50 and g["backstop"] == 0
    assert plan["deactivate_ids"] == [] and plan["backstop_rows"] == 0
    # Another dark system past the backstop still retires.
    plan2 = plan_layer4(_rows("Gone Health", 30, misses=2, scraped_at=STALE), set(), 3, now=NOW)
    assert len(plan2["deactivate_ids"]) == 30
    # Once UHG yields 80% again the guard, and with it the exemption, no longer apply.
    plan3 = plan_layer4(active, _keys("UnitedHealth Group", range(1, 56)), 3, now=NOW)
    assert plan3["guarded"] == {} and plan3["bump_by_new_count"] == {1: list(range(56, 61))}


def test_zero_yield_alarm_after_two_runs_in_a_row():
    plan = plan_layer4(_rows("Parkview Health", 20), set(), 3, now=NOW)
    g = plan["guarded"]
    assert g["Parkview Health"]["newest_seen"] == NOW - timedelta(hours=20)
    hist, alarms = zero_yield_alarms(g, {}, NOW)
    assert hist["zero_yield_streak"] == {"Parkview Health": 1} and alarms == []
    hist2, alarms2 = zero_yield_alarms(g, hist, NOW)
    assert hist2["zero_yield_streak"] == {"Parkview Health": 2}
    assert [(a["system"], a["runs"], a["active"]) for a in alarms2] == [("Parkview Health", 2, 20)]
    # A system with some yield tonight drops out of the streak.
    plan_half = plan_layer4(_rows("Parkview Health", 20), _keys("Parkview Health", [1]), 3, now=NOW)
    hist3, alarms3 = zero_yield_alarms(plan_half["guarded"], hist2, NOW)
    assert hist3["zero_yield_streak"] == {} and alarms3 == []


def test_zero_yield_alarm_from_a_stale_newest_row_without_history():
    # The streak file does not survive a redeploy: rows last stamped 40 h ago
    # mean the previous run did not see them either.
    old = (NOW - timedelta(hours=40)).isoformat()
    plan = plan_layer4(_rows("Dark Board", 5, scraped_at=old), set(), 3, now=NOW)
    _hist, alarms = zero_yield_alarms(plan["guarded"], {"zero_yield_streak": "garbage"}, NOW)
    assert len(alarms) == 1 and alarms[0]["runs"] == 1 and alarms[0]["age_h"] == 40.0
    assert alarms[0]["newest_seen"] == (NOW - timedelta(hours=40)).isoformat()


def test_yield_history_round_trip_and_unwritable_path(tmp_path):
    path = str(tmp_path / "state" / "layer4_yields.json")
    assert load_yield_history(path) == {}
    hist = {"run": NOW.isoformat(), "zero_yield_streak": {"A": 2}}
    assert save_yield_history(hist, path) and load_yield_history(path) == hist
    blocker = tmp_path / "file"
    blocker.write_text("x")
    assert save_yield_history(hist, str(blocker / "layer4_yields.json")) is False
    assert save_yield_history(hist) and load_yield_history() == hist      # conftest's temp default path


def test_mark_inactive_jobs_alarms_on_the_second_dark_run_and_accepts_complete_systems(monkeypatch, caplog):
    import database
    fresh = (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat()
    active_rows = ([{"id": i, "job_id": f"d{i}", "hospital_system": "Dark Board",
                     "consecutive_scrape_misses": 2, "scraped_at": fresh} for i in range(1, 31)]
                   + [{"id": 100 + i, "job_id": f"h{i}", "hospital_system": "Whole Board",
                       "consecutive_scrape_misses": 2, "scraped_at": fresh} for i in range(20)])
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
        def table(self, _name): return _Q(active_rows)
    monkeypatch.setattr(database, "client", lambda: _DB())
    save_yield_history({"run": "yesterday", "zero_yield_streak": {"Dark Board": 1}})
    seen = [{"hospital_system": "Whole Board", "job_id": f"h{i}"} for i in range(10)]   # 50% of 20: guarded unless complete
    with caplog.at_level(logging.INFO):
        out = database.mark_inactive_jobs(seen, miss_threshold=3, exclude_systems=set(),
                                          complete_systems={"Whole Board"})
    assert out["guarded_systems"] == ["Dark Board"] and out["zero_yield_alarms"] == ["Dark Board"]
    assert out["complete_bypassed"] == ["Whole Board"] and out["deactivated"] == 10
    assert any("ALARM LAYER4 Dark Board: zero yield 2 run(s) in a row, 30 active rows" in m for m in caplog.messages)
    assert any("LAYER4 complete crawl Whole Board: partial-yield 10/20" in m for m in caplog.messages)
    assert load_yield_history()["zero_yield_streak"] == {"Dark Board": 2}
    assert [ids for p, ids in updates if p.get("is_active") is False] == [list(range(110, 120))]


# ── sweep guard bypass for a complete crawl (fake urlopen, as in test_retire_guard) ──

class _UResp:
    def __init__(self, content_range=""):
        self.headers = {"Content-Range": content_range}

    def read(self): return b""
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _job(system, i):
    return {"hospital_system": system, "job_id": str(i), "title": "Registered Nurse", "hospital_name": system,
            "url": f"https://example.org/jobs/{i}", "job_type": "", "description": "", "state": "TX", "city": "Odessa"}


def test_sweep_guard_is_bypassed_for_a_complete_crawl_unless_partial(monkeypatch):
    patches = []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u or m == "POST":
            return _UResp()
        if m == "GET":
            return _UResp("0-0/100")          # the database holds 100 active rows: 30 is under 80%
        patches.append(urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0]))
        return _UResp("*/0")
    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    rows = [_job("Whole Board", i) for i in range(30)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-07T00:09:00.000000Z")
    assert patches == []                                   # guarded, as before
    scraper.COMPLETE_SYSTEMS.add("Whole Board")
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-07T00:09:00.000000Z")
    assert patches == ["Whole Board"]                      # complete: swept as usual
    scraper.PARTIAL_SYSTEMS.add("Whole Board")
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-07T00:09:00.000000Z")
    assert patches == ["Whole Board"]                      # a partial report wins


# ── 7. Config: relabels and removals ────────────────────────────────────────

def test_push10_config_relabels_and_removals():
    assert scraper.HEALTHCARESOURCE_ORGS["Boulder Community Health"] == "bch"
    assert "Brattleboro Memorial" not in scraper.HEALTHCARESOURCE_ORGS
    assert "Parkview Health" not in scraper.HEALTHCARESOURCE_ORGS
    assert scraper.ORACLE_ORGS["Parkview Health"] == ("https://parkview-ibyyjb.fa.ocs.oraclecloud.com", "CX_1")
    assert scraper.ORACLE_ORGS["Atlantic Health System"] == ("https://erqh.fa.us2.oraclecloud.com", "CX_1001")
    assert scraper.ORACLE_ORGS["Cottage Health"] == ("https://eglz.fa.us2.oraclecloud.com", "CX")
    assert scraper.ORACLE_ORGS["United Regional Health Care System"][0].startswith("https://iaoxqy")   # Wichita Falls TX
    for old in ("United Regional", "Eastern Connecticut Health"):
        assert old not in scraper.ORACLE_ORGS and old not in scraper.PAY_REREAD_SYSTEMS
    assert {"Atlantic Health System", "Cottage Health"} <= scraper.PAY_REREAD_SYSTEMS
    assert "Atria Senior Living" not in scraper.SMARTRECRUITERS_ORGS
    for d in (scraper.PAYCOR_ORGS, scraper.KRONOS_ORGS):
        assert not any(k.startswith(("Paycor Hospital", "Kronos Hospital")) for k in d)
