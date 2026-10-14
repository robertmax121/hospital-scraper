# 2026-10-13: Vivian paging. The 00:04 UTC run stopped Travel, Local Contract
# and Per Diem at page 5 because a full Algolia page of hits without a
# jobDetailsSlug (a run of unprocessed records now ranked right after the
# first 1,000 hits) parsed to zero jobs and the loop read that as the end.
# The fixture tests/fixtures/vivian/travel_slug_pair.json is a live 10-13 hit
# pair (descriptions shortened): same rawHash, one with a job page slug, one
# without. Everything else is a fake Algolia endpoint; no network.
import asyncio
import json
import logging
import urllib.parse

import pytest

import scraper


def _hit(i, slug=True, et="Travel"):
    return {
        "objectID": f"platform::780-{i}",
        "jobDetailsSlug": f"Ab{i:06d}-travel-icu-rn-nashville-tn" if slug else None,
        "titles": {"simple": "ICU RN", "verbose": "Travel ICU RN"},
        "employerName": "Cross Country Nurses",
        "pay": {"minRate": 2400, "maxRate": 2600, "period": "week",
                "display": {"full": "$2,400 - $2,600 / week"}},
        "contractLengthWeeks": "13 weeks",
        "shift": ["Nights"],
        "location": ["Nashville, Tennessee", "TN", "Tennessee", "Compact States"],
        "locationDisplay": "Nashville, TN",
        "specialtyNames": ["ICU - Intensive Care Unit"],
        "startDateDisplay": "Nov 2",
        "employmentType": et,
        "description": "<p>13 week ICU contract.</p>",
        "createdAtDisplay": 1788845780,
    }


def _page(n_slugged, n_slugless, start, served_hpp, nb_pages=1513, nb_hits=387649):
    hits = [_hit(start + k) for k in range(n_slugged)]
    hits += [_hit(start + n_slugged + k, slug=False) for k in range(n_slugless)]
    return {"results": [{"hits": hits, "nbHits": nb_hits, "nbPages": nb_pages,
                         "hitsPerPage": served_hpp, "page": 0}]}


def _empty(nb_hits, served_hpp=10):
    return {"results": [{"hits": [], "nbHits": nb_hits, "nbPages": 0, "hitsPerPage": served_hpp}]}


class _Resp:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def json(self, content_type=None):
        return self._payload


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


class FakeAlgolia:
    """Serves pages[(employmentType, page)] -> payload | int (HTTP status) |
    a list of those, one per request (the last one repeats). A missing page
    is an empty result with nbHits 0 (past the end)."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []          # (employmentType, page, params dict)

    def post(self, url, json=None, headers=None, **kw):
        params = dict(urllib.parse.parse_qsl(json["requests"][0]["params"]))
        et = __import__("json").loads(params["facetFilters"])[0][0].split(":", 1)[1]
        page = int(params["page"])
        self.calls.append((et, page, params))
        got = self.pages.get((et, page))
        if isinstance(got, list):
            got = got.pop(0) if len(got) > 1 else got[0]
        if isinstance(got, int):
            return _Ctx(_Resp(got, {}))
        if got is None:
            got = {"results": [{"hits": [], "nbHits": 0, "nbPages": 0,
                                "hitsPerPage": int(params["hitsPerPage"])}]}
        return _Ctx(_Resp(200, got))


@pytest.fixture
def small_pages(monkeypatch):
    """10-hit pages, no sleeps, Travel only unless a test widens it."""
    monkeypatch.setattr(scraper, "VIVIAN_HITS_PER_PAGE", 10)
    monkeypatch.setattr(scraper, "VIVIAN_PAGE_SLEEP", 0)
    monkeypatch.setattr(scraper, "VIVIAN_EMPLOYMENT_TYPES", ["Travel"])
    monkeypatch.setattr(scraper, "VIVIAN_RETRY_SLEEP", (0, 0))
    monkeypatch.setattr(scraper, "_retry_sleep", _no_sleep, raising=False)


@pytest.fixture(autouse=True)
def _no_incomplete_flags():
    scraper.TRAVEL_INCOMPLETE.clear()
    yield
    scraper.TRAVEL_INCOMPLETE.clear()


async def _no_sleep(*_a, **_k):
    return None


def _run(session):
    return asyncio.run(scraper.run_vivian(session))


def test_full_slugless_pages_do_not_end_the_crawl(small_pages, caplog):
    # The 10-13 shape: page 0 mixed, then whole pages with no slug, then mixed
    # again, then a short last page.
    pages = {
        ("Travel", 0): _page(4, 6, 0, 10),
        ("Travel", 1): _page(0, 10, 100, 10),
        ("Travel", 2): _page(0, 10, 200, 10),
        ("Travel", 3): _page(3, 7, 300, 10),
        ("Travel", 4): _page(2, 1, 400, 10),        # short page: the end
    }
    s = FakeAlgolia(pages)
    with caplog.at_level(logging.INFO):
        jobs = _run(s)
    assert [p for _, p, _ in s.calls] == [0, 1, 2, 3, 4]
    assert len(jobs) == 9
    assert {j.agency_job_id for j in jobs} >= {"vivian:platform::780-300", "vivian:platform::780-400"}
    assert any("Vivian Travel: 9 jobs from 43 hits; 2 page(s) had no job pages; stopped: last page 4 (3 hits)"
               in m for m in caplog.messages)


def test_old_stop_rule_would_have_stopped_at_the_first_slugless_page(small_pages):
    # Guard against a regression to `if not has_more or not jobs: break`:
    # a full page that parses to nothing still reports has_more.
    s = FakeAlgolia({("Travel", 1): _page(0, 10, 100, 10)})
    jobs, total, has_more, n_hits = asyncio.run(scraper.scrape_vivian_page(s, "Travel", 1))
    assert jobs == [] and n_hits == 10 and has_more is True


def test_nbpages_estimate_does_not_end_the_crawl(small_pages):
    # nbHits/nbPages are estimates (exhaustive.nbHits=false); live, Permanent
    # said 12 pages on page 0 and still served full pages at page 24.
    pages = {("Travel", p): _page(5, 5, p * 100, 10, nb_pages=1) for p in range(3)}
    s = FakeAlgolia(pages)
    jobs = _run(s)
    assert len(jobs) == 15
    assert [p for _, p, _ in s.calls] == [0, 1, 2, 3]     # page 3 is empty: the end


def test_a_lowered_page_size_still_pages_through(small_pages):
    # If Algolia serves fewer hits per page than asked (hitsPerPage in the
    # response), a page of that size is still a full page.
    pages = {("Travel", 0): _page(4, 0, 0, 4), ("Travel", 1): _page(4, 0, 100, 4),
             ("Travel", 2): _page(1, 0, 200, 4)}
    s = FakeAlgolia(pages)
    assert len(_run(s)) == 9
    assert [p for _, p, _ in s.calls] == [0, 1, 2]


def test_a_page_that_fails_twice_ends_the_type_and_flags_vivian(small_pages, caplog):
    pages = {("Travel", 0): _page(5, 5, 0, 10), ("Travel", 1): 403}
    s = FakeAlgolia(pages)
    with caplog.at_level(logging.INFO):
        jobs = _run(s)
    assert len(jobs) == 5
    assert [p for _, p, _ in s.calls] == [0, 1, 1]          # one retry
    assert any("stopped: request failed on page 1 (after a retry)" in m for m in caplog.messages)
    assert scraper.TRAVEL_INCOMPLETE == {"vivian.com": "Travel: request failed on page 1 (after a retry)"}


def test_a_failed_page_is_retried_once_and_the_crawl_goes_on(small_pages):
    # 403 is not retried inside req(), so this is the adapter's own retry.
    pages = {("Travel", 0): _page(5, 5, 0, 10, nb_hits=22),
             ("Travel", 1): [403, _page(4, 6, 100, 10, nb_hits=22)],
             ("Travel", 2): _page(2, 0, 200, 10, nb_hits=22)}
    s = FakeAlgolia(pages)
    jobs = _run(s)
    assert [p for _, p, _ in s.calls] == [0, 1, 1, 2]
    assert len(jobs) == 11 and scraper.TRAVEL_INCOMPLETE == {}


def test_an_empty_deep_page_is_re_requested_once_while_nbhits_is_far_past(small_pages):
    # Reachable depth moved between requests on 10-13 (page 100 full at
    # 11:45, 64k-70k at 12:13): one empty answer is asked again.
    pages = {("Travel", 0): _page(5, 5, 0, 10, nb_hits=22),
             ("Travel", 1): [_empty(387649), _page(3, 7, 100, 10, nb_hits=22)],
             ("Travel", 2): _page(2, 0, 200, 10, nb_hits=22)}
    s = FakeAlgolia(pages)
    jobs = _run(s)
    assert [p for _, p, _ in s.calls] == [0, 1, 1, 2]
    assert len(jobs) == 10


def test_an_empty_page_at_the_real_end_is_not_re_requested(small_pages):
    pages = {("Travel", 0): _page(5, 5, 0, 10, nb_hits=20),
             ("Travel", 1): _page(4, 6, 100, 10, nb_hits=20)}
    s = FakeAlgolia(pages)                                   # page 2: empty, nbHits 0
    _run(s)
    assert [p for _, p, _ in s.calls] == [0, 1, 2]
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_a_travel_collapse_flags_vivian_even_when_other_types_are_full(small_pages, monkeypatch, caplog):
    # The review case: Travel back to page 0 only while the other types are
    # healthy; the domain total alone would clear the 70% guard.
    monkeypatch.setattr(scraper, "VIVIAN_EMPLOYMENT_TYPES", ["Travel", "Local Contract"])
    pages = {("Travel", 0): _page(4, 6, 0, 10), ("Travel", 1): _empty(387649),
             ("Local Contract", 0): _page(9, 1, 1000, 10, nb_hits=15),
             ("Local Contract", 1): _page(5, 0, 1100, 10, nb_hits=15)}
    s = FakeAlgolia(pages)
    with caplog.at_level(logging.WARNING):
        jobs = _run(s)
    assert [(e, p) for e, p, _ in s.calls] == [("Travel", 0), ("Travel", 1), ("Travel", 1),
                                               ("Local Contract", 0), ("Local Contract", 1)]
    assert len(jobs) == 18
    assert scraper.TRAVEL_INCOMPLETE == {
        "vivian.com": "Travel: ended at page 1 with 4 rows (floor 2,000) from 10 of ~387,649 hits"}
    assert any("crawl INCOMPLETE" in m for m in caplog.messages)


def test_a_short_crawl_above_its_floor_or_near_its_total_is_complete(small_pages, monkeypatch):
    # nbHits far past the scan but the rows clear the floor: complete.
    monkeypatch.setattr(scraper, "VIVIAN_MIN_ROWS", {"Travel": 4})
    _run(FakeAlgolia({("Travel", 0): _page(4, 6, 0, 10), ("Travel", 1): _empty(387649)}))
    assert scraper.TRAVEL_INCOMPLETE == {}
    # Few rows, but the type is small: nbHits is within 5x of the scan.
    monkeypatch.setattr(scraper, "VIVIAN_MIN_ROWS", {"Travel": 2000})
    _run(FakeAlgolia({("Travel", 0): _page(4, 6, 0, 10, nb_hits=40),
                      ("Travel", 1): _page(1, 0, 100, 10, nb_hits=40)}))
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_stopping_on_the_job_page_budget_is_complete(small_pages, monkeypatch):
    monkeypatch.setattr(scraper, "VIVIAN_JOB_PAGES", {"Travel": 2})
    pages = {("Travel", p): _page(2, 8, p * 100, 10) for p in range(10)}
    _run(FakeAlgolia(pages))
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_a_new_run_clears_an_old_flag(small_pages):
    scraper.TRAVEL_INCOMPLETE["vivian.com"] = "stale"
    _run(FakeAlgolia({("Travel", 0): _page(4, 0, 0, 10, nb_hits=4)}))
    assert scraper.TRAVEL_INCOMPLETE == {}


def test_job_page_budget_counts_only_pages_with_jobs(small_pages, monkeypatch, caplog):
    monkeypatch.setattr(scraper, "VIVIAN_JOB_PAGES", {"Travel": 2})
    pages = {("Travel", p): (_page(0, 10, p * 100, 10) if p in (1, 2, 3) else _page(2, 8, p * 100, 10))
             for p in range(10)}
    s = FakeAlgolia(pages)
    with caplog.at_level(logging.INFO):
        jobs = _run(s)
    # page 0 (jobs), 1-3 (slugless, free), 4 (jobs) -> budget of 2 reached
    assert [p for _, p, _ in s.calls] == [0, 1, 2, 3, 4]
    assert len(jobs) == 4
    assert any("stopped: 2 pages with jobs" in m for m in caplog.messages)


def test_page_ceiling_bounds_requests_through_a_long_slugless_run(small_pages, monkeypatch, caplog):
    monkeypatch.setattr(scraper, "VIVIAN_MAX_PAGES", {"Travel": 6})
    pages = {("Travel", p): _page(0, 10, p * 100, 10) for p in range(50)}
    pages[("Travel", 0)] = _page(3, 7, 0, 10)
    s = FakeAlgolia(pages)
    with caplog.at_level(logging.INFO):
        jobs = _run(s)
    assert len(s.calls) == 6 and len(jobs) == 3
    assert any("5 page(s) had no job pages; stopped: page ceiling 6" in m for m in caplog.messages)
    assert scraper.TRAVEL_INCOMPLETE["vivian.com"].startswith("Travel: hit the 6-page ceiling")


def test_every_type_is_crawled_with_its_own_budget(small_pages, monkeypatch):
    monkeypatch.setattr(scraper, "VIVIAN_EMPLOYMENT_TYPES", ["Travel", "Local Contract"])
    pages = {
        ("Travel", 0): _page(2, 8, 0, 10), ("Travel", 1): _page(0, 10, 100, 10),
        ("Travel", 2): _page(1, 0, 200, 10),
        ("Local Contract", 0): _page(3, 7, 1000, 10), ("Local Contract", 1): _page(0, 10, 1100, 10),
        ("Local Contract", 2): _page(4, 6, 1200, 10),
    }
    s = FakeAlgolia(pages)
    jobs = _run(s)
    assert [(e, p) for e, p, _ in s.calls] == [
        ("Travel", 0), ("Travel", 1), ("Travel", 2),
        ("Local Contract", 0), ("Local Contract", 1), ("Local Contract", 2), ("Local Contract", 3)]
    assert len(jobs) == 10


def test_request_asks_for_1000_hits_and_only_the_parsed_attributes():
    s = FakeAlgolia({})
    asyncio.run(scraper.scrape_vivian_page(s, "Local Contract", 7))
    (_, page, params), = s.calls
    assert page == 7
    assert scraper.VIVIAN_HITS_PER_PAGE == 1000 and params["hitsPerPage"] == "1000"
    assert json.loads(params["facetFilters"]) == [["employmentType:Local Contract"]]
    assert params["filters"] == 'origin:"platform" OR origin:"vms" OR origin:"scraped"'
    assert json.loads(params["attributesToRetrieve"]) == scraper.VIVIAN_ATTRIBUTES
    assert params["attributesToHighlight"] == "[]" and params["attributesToSnippet"] == "[]"


def test_budgets_stay_within_a_few_hundred_requests():
    ceiling = sum(scraper.VIVIAN_MAX_PAGES.get(t, scraper.VIVIAN_MAX_PAGES_DEFAULT)
                  for t in scraper.VIVIAN_EMPLOYMENT_TYPES)
    # 2026-10-14 (owner OK): Travel's ceiling is 450 so a deep night ends on
    # Algolia's first empty page (~151-175 pages today), not on the ceiling;
    # the real request count is that depth, the ceiling only a backstop.
    assert ceiling <= 600
    for t in scraper.VIVIAN_EMPLOYMENT_TYPES:
        assert (scraper.VIVIAN_JOB_PAGES.get(t, scraper.VIVIAN_JOB_PAGES_DEFAULT)
                <= scraper.VIVIAN_MAX_PAGES.get(t, scraper.VIVIAN_MAX_PAGES_DEFAULT))
    # Travel's job pages cover more than the old 25,000-hit window, and its
    # ceiling leaves room for a long slugless run on top (30k+ hits on 10-13).
    assert scraper.VIVIAN_JOB_PAGES["Travel"] * scraper.VIVIAN_HITS_PER_PAGE >= 25_000
    assert scraper.VIVIAN_MAX_PAGES["Travel"] >= scraper.VIVIAN_JOB_PAGES["Travel"] + 40


def test_live_pair_keeps_the_slugged_hit_only(fixture_text):
    payload = json.loads(fixture_text("vivian/travel_slug_pair.json"))
    s = FakeAlgolia({("Travel", 0): payload})
    jobs, total, has_more, n_hits = asyncio.run(scraper.scrape_vivian_page(s, "Travel", 0))
    assert n_hits == 2 and total == 378073
    assert has_more is False                         # 2 hits on a 250-hit page: short
    (j,) = jobs
    assert j.agency_job_id == "vivian:platform::780-1310211"
    assert j.url == "https://www.vivian.com/jobs/bBxgnA8p-travel-perfusionist-allied-health-professional-nashville-tn/"
    assert j.agency_name == "Cross Country Allied" and j.title == "Perfusionist"
    assert (j.city, j.state, j.weekly_pay_numeric, j.contract_weeks) == ("Nashville", "TN", 7615.0, 26)


def test_attribute_list_covers_everything_the_parser_reads(fixture_text):
    payload = json.loads(fixture_text("vivian/travel_slug_pair.json"))
    full = FakeAlgolia({("Travel", 0): payload})
    trimmed_payload = json.loads(json.dumps(payload))
    keep = set(scraper.VIVIAN_ATTRIBUTES)
    trimmed_payload["results"][0]["hits"] = [{k: v for k, v in h.items() if k in keep}
                                             for h in trimmed_payload["results"][0]["hits"]]
    trimmed = FakeAlgolia({("Travel", 0): trimmed_payload})
    a = asyncio.run(scraper.scrape_vivian_page(full, "Travel", 0))[0]
    b = asyncio.run(scraper.scrape_vivian_page(trimmed, "Travel", 0))[0]
    assert a == b and len(a) == 1
