# 2026-10-15 (owner, on Amedisys job 56815 in Winter Haven FL: "whats the req
# id and why is it still on our site?"). Three changes, pinned here:
#   1. retirement on the FIRST miss when the employer's feed came back complete
#      (retire_guard.RETIRE_FIRST_MISS_WHEN_COMPLETE, plan_layer4's
#      first_miss_complete, database.mark_inactive_jobs), with partial,
#      capped and flaky feeds keeping the 3-miss count;
#   2. the Jibe feeds (Amedisys, Novant, ...) claim a complete crawl against
#      their totalCount, and a page that fails mid-crawl reports PARTIAL (the
#      sweep used to retire the unread pages' rows when the read pages came to
#      80% of the active rows);
#   3. SelectMinds reads the requisition number from the job page.
# No network, no database.
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

import retire_guard
import scraper
from retire_guard import plan_layer4

NOW = datetime(2026, 10, 16, 2, 0, tzinfo=timezone.utc)
FRESH = (NOW - timedelta(days=1)).isoformat()


def _rows(system, n, misses=0, start_id=1, scraped_at=FRESH):
    return [{"id": start_id + i, "job_id": str(start_id + i), "hospital_system": system,
             "consecutive_scrape_misses": misses, "scraped_at": scraped_at} for i in range(n)]


def _keys(system, ids):
    return {(system, str(i)) for i in ids}


# ── 1. first-miss retirement for complete feeds ─────────────────────────────

def test_setting_is_on_by_default_and_named():
    assert retire_guard.RETIRE_FIRST_MISS_WHEN_COMPLETE is True


def test_complete_feed_retires_unseen_rows_on_the_first_miss():
    """Amedisys tonight: the feed served its whole totalCount; the 18 postings
    it no longer lists go at once, not after three nights."""
    active = _rows("Amedisys", 1229)
    keys = _keys("Amedisys", range(1, 1212))                # 1,211 seen, 18 unseen
    plan = plan_layer4(active, keys, 3, now=NOW, complete_systems={"Amedisys"}, first_miss_complete=True)
    assert sorted(plan["deactivate_ids"]) == list(range(1212, 1230))
    assert plan["first_miss"]["Amedisys"] == {"active": 1229, "yield": 1211, "retired": 18}
    assert plan["first_miss_rows"] == 18 and plan["bump_by_new_count"] == {} and plan["guarded"] == {}
    # the same night without the setting: the 3-miss count, as before
    old = plan_layer4(active, keys, 3, now=NOW, complete_systems={"Amedisys"})
    assert old["deactivate_ids"] == [] and old["bump_by_new_count"] == {1: list(range(1212, 1230))}
    assert old["first_miss"] == {} and old["first_miss_rows"] == 0


def test_small_complete_board_the_sweep_skips_retires_at_once():
    """A board under the sweep's DEACT_MIN (10 rows tonight) is only ever
    retired by Layer 4: three nights for a closed posting, one now."""
    active = _rows("Tanner Health", 8, misses=0)
    plan = plan_layer4(active, _keys("Tanner Health", range(1, 7)), 3, now=NOW,
                       complete_systems={"Tanner Health"}, first_miss_complete=True)
    assert sorted(plan["deactivate_ids"]) == [7, 8]


def test_feeds_without_a_complete_claim_keep_the_three_miss_rule():
    """Partial, capped or flaky: no complete claim, no first miss."""
    active = _rows("Capped Board", 100) + _rows("Flaky Board", 100, start_id=101)
    keys = _keys("Capped Board", range(1, 91)) | _keys("Flaky Board", range(101, 191))
    plan = plan_layer4(active, keys, 3, now=NOW, complete_systems=set(), first_miss_complete=True)
    assert plan["first_miss"] == {} and plan["deactivate_ids"] == []
    assert sorted(plan["bump_by_new_count"][1]) == list(range(91, 101)) + list(range(191, 201))


def test_a_partial_report_beats_a_complete_claim():
    active = _rows("Penn Medicine", 100)
    plan = plan_layer4(active, _keys("Penn Medicine", range(1, 91)), 3, now=NOW,
                       complete_systems={"Penn Medicine"}, partial_systems={"Penn Medicine"},
                       first_miss_complete=True)
    assert plan["first_miss"] == {} and plan["deactivate_ids"] == []
    assert plan["guarded"]["Penn Medicine"]["reason"] == "partial crawl"


def test_a_complete_claim_under_half_of_active_never_wipes_the_system():
    """The guard that stops a bad run from wiping a system: a 'complete' feed
    that returned under COMPLETE_MIN_ACTIVE_RATIO of the active rows (a
    filtered page read as the whole board) retires nothing; its rows are held
    as before."""
    active = _rows("UnitedHealth Group", 1000)
    plan = plan_layer4(active, _keys("UnitedHealth Group", range(1, 300)), 3, now=NOW,
                       complete_systems={"UnitedHealth Group"}, first_miss_complete=True)
    assert plan["first_miss"] == {} and plan["deactivate_ids"] == []
    assert "complete claim rejected" in plan["guarded"]["UnitedHealth Group"]["reason"]


def test_excluded_systems_stay_untouched():
    active = _rows("HCA Healthcare", 50)
    plan = plan_layer4(active, _keys("HCA Healthcare", range(1, 41)), 3, now=NOW,
                       exclude_systems={"HCA Healthcare"}, complete_systems={"HCA Healthcare"},
                       first_miss_complete=True)
    assert plan["excluded_rows"] == 50 and plan["deactivate_ids"] == [] and plan["first_miss"] == {}


def test_seen_rows_are_still_reset_on_a_complete_night():
    active = _rows("Amedisys", 20, misses=1)
    plan = plan_layer4(active, _keys("Amedisys", range(1, 19)), 3, now=NOW,
                       complete_systems={"Amedisys"}, first_miss_complete=True)
    assert sorted(plan["found_ids"]) == list(range(1, 19)) and sorted(plan["deactivate_ids"]) == [19, 20]


def _fake_layer4_db(monkeypatch, active_rows):
    import database
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
            class R:
                pass
            r = R()
            r.data = list(self._rows) if self._patch is None else []
            return r

    class _DB:
        def table(self, _name): return _Q(active_rows)

    monkeypatch.setattr(database, "client", lambda: _DB())
    return database, updates


def test_mark_inactive_jobs_reads_the_setting(monkeypatch):
    fresh = datetime.now(timezone.utc).isoformat()
    active = [{"id": i, "job_id": str(i), "hospital_system": "Amedisys", "consecutive_scrape_misses": 0,
               "scraped_at": fresh} for i in range(1, 21)]
    seen = [{"hospital_system": "Amedisys", "job_id": str(i)} for i in range(1, 19)]
    database, updates = _fake_layer4_db(monkeypatch, active)
    out = database.mark_inactive_jobs(seen, complete_systems={"Amedisys"})
    assert out["first_miss_on"] is True and out["first_miss_retired"] == 2
    assert out["first_miss_systems"] == ["Amedisys"] and out["deactivated"] == 2
    assert [ids for p, ids in updates if p.get("is_active") is False] == [[19, 20]]

    monkeypatch.setattr(retire_guard, "RETIRE_FIRST_MISS_WHEN_COMPLETE", False)
    database, updates = _fake_layer4_db(monkeypatch, active)
    out = database.mark_inactive_jobs(seen, complete_systems={"Amedisys"})
    assert out["first_miss_on"] is False and out["deactivated"] == 0 and out["misses_pending"] == 2


# ── 2. Jibe: complete claims and partial reports ────────────────────────────

class _R:
    def __init__(self, body, status=200):
        self.status, self._body = status, body

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return json.dumps(self._body)


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        if isinstance(self.r, Exception):
            raise self.r
        return self.r

    async def __aexit__(self, *a):
        return False


def _jibe_item(rid, slug=None, title="Registered Nurse"):
    return {"data": {"req_id": rid, "slug": slug if slug is not None else rid, "title": title,
                     "city": "Winter Haven", "state": "Florida", "description": "<p>Home health RN.</p>",
                     "meta_data": {"canonical_url": f"https://careers.amedisys.com/jobs/{slug or rid}?lang=en-us"}}}


def _jibe_board(monkeypatch, pages, total, fail_page=None):
    """pages: list of item lists; fail_page: 1-based page answering HTTP 500."""
    calls = []

    def fake_req(session, method, url, **kw):
        page = int((kw.get("params") or {}).get("page", "1"))
        calls.append(page)
        if fail_page == page:
            return _Ctx(_R({}, status=500))
        items = pages[page - 1] if page <= len(pages) else []
        return _Ctx(_R({"jobs": items, "totalCount": total}))
    monkeypatch.setattr(scraper, "req", fake_req)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    return calls


def _pages(n_items, start=50000, per=100, no_rid=()):
    items = [_jibe_item("" if i in no_rid else str(start + i), slug=str(start + i)) for i in range(n_items)]
    return [items[i:i + per] for i in range(0, n_items, per)]


def test_jibe_whole_board_is_a_complete_claim(monkeypatch):
    _jibe_board(monkeypatch, _pages(250), 250)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Amedisys", "https://careers.amedisys.com"))
    assert len(jobs) == 250 and all(j.req_id == j.job_id for j in jobs)
    assert "Amedisys" in scraper.COMPLETE_SYSTEMS and ("JIBE_SITES", "Amedisys") in scraper.COMPLETE_FEEDS
    assert "Amedisys" not in scraper.PARTIAL_SYSTEMS
    assert "Amedisys" in scraper.complete_canonical(emitted={"Amedisys"})


def test_jibe_rows_without_req_id_still_count_as_served(monkeypatch):
    """UHS: provider postings carry a slug and no req_id; the adapter skips
    them, but they were read, so the board is still complete."""
    _jibe_board(monkeypatch, _pages(200, no_rid={3, 50, 150}), 200)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Universal Health Services", "https://jobs.uhsinc.com"))
    assert len(jobs) == 197
    assert "Universal Health Services" in scraper.COMPLETE_SYSTEMS


def test_jibe_page_failing_mid_crawl_is_partial(monkeypatch):
    """Pages 1-9 of 10 read (90% of the active rows): before 2026-10-15 the
    sweep retired page 10's live rows that night. Now PARTIAL, no claim."""
    calls = _jibe_board(monkeypatch, _pages(1000), 1000, fail_page=10)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Novant Health", "https://jobs.novanthealth.org"))
    assert len(jobs) == 900 and calls == list(range(1, 11))
    assert "Novant Health" in scraper.PARTIAL_SYSTEMS and "Novant Health" in scraper.PARTIAL_BACKSTOP_SYSTEMS
    assert "Novant Health" not in scraper.COMPLETE_SYSTEMS


def test_jibe_page_ceiling_is_partial(monkeypatch):
    monkeypatch.setattr(scraper, "JIBE_MAX_PAGES", 3)
    calls = _jibe_board(monkeypatch, _pages(450), 450)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Universal Health Services", "https://jobs.uhsinc.com"))
    assert len(jobs) == 300 and calls == [1, 2, 3]
    assert "Universal Health Services" in scraper.PARTIAL_SYSTEMS
    assert "Universal Health Services" not in scraper.COMPLETE_SYSTEMS


def test_jibe_board_that_answers_nothing_reports_nothing(monkeypatch):
    _jibe_board(monkeypatch, _pages(100), 100, fail_page=1)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Amedisys", "https://careers.amedisys.com"))
    assert jobs == [] and "Amedisys" not in scraper.PARTIAL_SYSTEMS and "Amedisys" not in scraper.COMPLETE_SYSTEMS


def test_jibe_short_of_the_total_is_not_complete(monkeypatch):
    """The feed ended early with no error (fewer rows than totalCount): no
    claim, so the yield guard and the 3-miss count decide, as before."""
    _jibe_board(monkeypatch, _pages(95), 100)
    asyncio.run(scraper.scrape_jibe(None, "Amedisys", "https://careers.amedisys.com"))
    assert "Amedisys" not in scraper.COMPLETE_SYSTEMS and "Amedisys" not in scraper.PARTIAL_SYSTEMS


# ── 3. SelectMinds: the number from the job page ────────────────────────────

FX = "reqfix_1015/"


@pytest.mark.parametrize("name,job_id,number", [
    ("selectminds_mainline_28195.html", "28195", "81154"),     # its Similar Listings print 2600007B
    ("selectminds_mainline_28762.html", "28762", "80031"),     # its Similar Listings print 78482
    ("selectminds_uiowa_50311.html", "50311", "26005003"),
])
def test_selectminds_page_number(fixture_text, name, job_id, number):
    html = fixture_text(FX + name)
    assert scraper.selectminds_req_id_from_page(html, job_id) == number


def test_selectminds_page_number_refusals(fixture_text):
    html = fixture_text(FX + "selectminds_mainline_28195.html")
    assert scraper.selectminds_req_id_from_page(html, "99999") == ""              # another posting's page
    assert scraper.selectminds_req_id_from_page(html.replace('value="81154"', 'value="81155"'), "28195") == ""
    assert scraper.selectminds_req_id_from_page("<html>No such job</html>", "28195") == ""
    assert scraper.selectminds_req_id_from_page("", "28195") == ""


def _sm_job(jid, url_tail):
    return scraper.Job(title="Medical Assistant", hospital_system="Main Line Health", hospital_name="Main Line Health",
                       city="Glen Mills", state="PA", location="Glen Mills, PA", specialty="Medical Assistant",
                       job_type="", url=f"https://mainline.referrals.selectminds.com/jobs/{url_tail}",
                       job_id=jid, posted_date="", description="Could you be our next Medical Assistant?",
                       ats_platform="SelectMinds")


def test_selectminds_req_pass_reads_unknown_rows_within_the_cap(fixture_text, monkeypatch):
    pages = {"medical-assistant-28195": fixture_text(FX + "selectminds_mainline_28195.html"),
             "ct-scan-technologist-28762": fixture_text(FX + "selectminds_mainline_28762.html")}
    asked = []

    async def fake_get(session, system, url, params=None):
        tail = url.rsplit("/", 1)[1]
        asked.append(tail)
        return (200, pages[tail], url) if tail in pages else (404, "", url)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "_selectminds_get", fake_get)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "_stored_req_job_ids", lambda system: {"28762"})   # already stored
    monkeypatch.setattr(scraper, "SELECTMINDS_REQ_MAX_PER_TENANT", 2)
    jobs = [_sm_job("28195", "medical-assistant-28195"), _sm_job("28762", "ct-scan-technologist-28762"),
            _sm_job("30001", "gone-30001"), _sm_job("30002", "late-30002")]
    got = asyncio.run(scraper._selectminds_req_pass(None, "Main Line Health", jobs))
    assert asked == ["medical-assistant-28195", "gone-30001"]          # stored one skipped, cap of 2
    assert got == 1 and [j.req_id for j in jobs] == ["81154", "", "", ""]
    row = scraper.normalize_job(jobs[0])
    assert row["posting_facts"] == {"req_id": "81154"} and "req_id" not in row
    assert "SelectMinds" in scraper.REQ_ID_ONLY_PLATFORMS


def test_selectminds_req_pass_off_at_zero(monkeypatch):
    monkeypatch.setattr(scraper, "SELECTMINDS_REQ_MAX_PER_TENANT", 0)

    async def boom(*a, **k):
        raise AssertionError("no page read")
    monkeypatch.setattr(scraper, "_selectminds_get", boom)
    assert asyncio.run(scraper._selectminds_req_pass(None, "Main Line Health",
                                                     [_sm_job("28195", "medical-assistant-28195")])) == 0


def test_stored_req_job_ids_without_credentials_is_none(monkeypatch):
    for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert scraper._stored_req_job_ids("Main Line Health") is None


# ── 2b. Oracle HCM and SmartRecruiters: complete claims against their counts ─

def _oracle_board(monkeypatch, total, served, fail_at=None, non_us=()):
    def fake_req(session, method, url, **kw):
        finder = (kw.get("params") or {}).get("finder", "")
        off = int(finder.rsplit("offset=", 1)[1])
        if fail_at is not None and off >= fail_at:
            return _Ctx(_R({}, status=503))
        items = []
        for i in range(off, min(off + 25, served)):
            country = "CA" if i in non_us else "US"
            items.append({"Id": str(300000 + i), "Title": f"RN {i}", "PrimaryLocation": "Dallas, TX, United States",
                          "PrimaryLocationCountry": country})
        return _Ctx(_R({"items": [{"TotalJobsCount": total, "requisitionList": {"items": items}}]}))
    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)


def test_oracle_whole_board_is_complete(monkeypatch):
    _oracle_board(monkeypatch, 60, 60)
    jobs = asyncio.run(scraper.scrape_oracle(None, "Tenet Healthcare", ("https://x.oraclecloud.com", "CX_1")))
    assert len(jobs) == 60
    assert ("ORACLE_ORGS", "Tenet Healthcare") in scraper.COMPLETE_FEEDS


def test_oracle_cut_short_is_not_complete(monkeypatch):
    _oracle_board(monkeypatch, 100, 100, fail_at=75)
    jobs = asyncio.run(scraper.scrape_oracle(None, "Tenet Healthcare", ("https://x.oraclecloud.com", "CX_1")))
    assert len(jobs) == 75 and "Tenet Healthcare" not in scraper.COMPLETE_SYSTEMS


def _sr_board(monkeypatch, total, served, fail_at=None):
    def fake_req(session, method, url, **kw):
        off = int((kw.get("params") or {}).get("offset", 0))
        if fail_at is not None and off >= fail_at:
            return _Ctx(_R({}, status=500))
        items = [{"id": str(700 + i), "name": f"RN {i}", "location": {"city": "Chicago", "region": "IL"}}
                 for i in range(off, min(off + 100, served))]
        return _Ctx(_R({"content": items, "totalFound": total}))
    monkeypatch.setattr(scraper, "req", fake_req)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)


def test_smartrecruiters_whole_board_is_complete(monkeypatch):
    _sr_board(monkeypatch, 250, 250)
    jobs = asyncio.run(scraper.scrape_smartrecruiters(None, "Northwestern Medicine", "NorthwesternMedicine"))
    assert len(jobs) == 250 and ("SMARTRECRUITERS_ORGS", "Northwestern Medicine") in scraper.COMPLETE_FEEDS


def test_smartrecruiters_cut_short_is_not_complete(monkeypatch):
    _sr_board(monkeypatch, 250, 250, fail_at=200)
    jobs = asyncio.run(scraper.scrape_smartrecruiters(None, "Northwestern Medicine", "NorthwesternMedicine"))
    assert len(jobs) == 200 and "Northwestern Medicine" not in scraper.COMPLETE_SYSTEMS


def test_oracle_non_us_requisitions_count_as_served(monkeypatch):
    """Non-US requisitions are read and left out; the board is still whole."""
    _oracle_board(monkeypatch, 50, 50, non_us={3, 40})
    jobs = asyncio.run(scraper.scrape_oracle(None, "Tenet Healthcare", ("https://x.oraclecloud.com", "CX_1")))
    assert len(jobs) == 48 and "Tenet Healthcare" in scraper.COMPLETE_SYSTEMS
