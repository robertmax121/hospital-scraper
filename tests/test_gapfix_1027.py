"""Gap fixes to sources we already read (2026-10-27).

1. UHS (Jibe): /api/jobs repeats one page past ~5,100 rows, so a board past
   JIBE_DEEP_WINDOW is read state by state.
2. Trinity Health (Workday): a facet slice past the 2,000-row window is split
   again (Nursing Group by timeType).
3. Dead feeds: Compassus (new Workday site), NYU Langone (Findly CWS),
   Temple Health (SilkRoad OpenHire).
4. HCA local push: the detail pass stops before the scheduled task's limit.
5. Select Medical (iCIMS): the job page's Company field is the facility;
   "Page N of N" completes a card crawl; an AWS WAF answer is PARTIAL.
No network, no database."""
import asyncio
import json
import sys
import time
import types
import zlib

import pytest

import scraper
import hca_local_push as hlp

FX = "gapfix1027/"


# ── shared fakes ─────────────────────────────────────────────────────────────
class _R:
    def __init__(self, body=None, status=200, text=None, headers=None):
        self.status, self._body, self._text = status, body, text
        self.headers = headers or {}

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return self._text if self._text is not None else json.dumps(self._body)

    async def read(self):
        return (await self.text()).encode("utf-8")


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        if isinstance(self.r, Exception):
            raise self.r
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


@pytest.fixture
def quiet(monkeypatch):
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    yield


# ══ 1. UHS: the Jibe deep window ═════════════════════════════════════════════
def _jibe_row(rid, state):
    return {"data": {"req_id": str(rid), "slug": str(rid), "title": "Registered Nurse", "city": "Austin",
                     "state": state, "description": "<p>RN.</p>",
                     "meta_data": {"canonical_url": f"https://jobs.uhsinc.com/jobs/{rid}"}}}


def _deep_board(monkeypatch, rows, window_pages=51, fail=None):
    """A Jibe board that serves `rows` like jobs.uhsinc.com: the whole board
    repeats page `window_pages` for every page past it; ?state= filters
    exactly. fail: (state, page) that answers HTTP 500."""
    calls = []

    def fake_req(session, method, url, **kw):
        prm = kw.get("params") or {}
        page, st = int(prm.get("page", "1")), prm.get("state")
        calls.append((st, page))
        if fail == (st, page):
            return _Ctx(_R({}, status=500))
        listing = [r for r in rows if st is None or r["data"]["state"] == st]
        p = min(page, window_pages) if st is None else page
        return _Ctx(_R({"jobs": listing[(p - 1) * 100:p * 100], "totalCount": len(listing)}))
    monkeypatch.setattr(scraper, "req", fake_req)
    return calls


def _uhs_rows(n_by_state):
    rows, rid = [], 300000
    for st, n in n_by_state.items():
        for _ in range(n):
            rows.append(_jibe_row(rid, st))
            rid += 1
    # interleave states the way the live feed does (not sorted by state)
    return sorted(rows, key=lambda r: zlib.crc32(r["data"]["req_id"].encode()))


UHS = "Universal Health Services"


def test_uhs_shape_board_is_read_by_state_and_is_complete(monkeypatch, quiet):
    rows = _uhs_rows({"Texas": 1060, "California": 540, "Florida": 650, "Nevada": 520, "Pennsylvania": 380,
                      "Washington, DC": 316, "Virginia": 178, "West Virginia": 7, "South Carolina": 262,
                      "Georgia": 204, "Michigan": 190, "Illinois": 119, "Tennessee": 109, "Ohio": 1489})
    assert len(rows) == 6024 > scraper.JIBE_DEEP_WINDOW
    calls = _deep_board(monkeypatch, rows)
    jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    assert len(jobs) == 6024 and len({j.job_id for j in jobs}) == 6024
    assert UHS in scraper.COMPLETE_SYSTEMS and UHS not in scraper.PARTIAL_SYSTEMS
    assert calls[0] == (None, 1) and all(st is not None for st, _ in calls[1:])   # one unfiltered page only
    named_first = [st for st, p in calls[1:] if p == 1][:3]
    assert set(named_first) <= {r["data"]["state"] for r in rows}                 # page-1 states go first
    assert "Washington, DC" in {st for st, _ in calls}
    assert {j.state for j in jobs} >= {"TX", "DC", "WV", "VA"}


def test_the_old_unfiltered_read_of_that_board_could_not_reach_its_tail(monkeypatch, quiet):
    # The 10-26 failure, kept as a regression: under the window the pages are
    # read straight through, and a page that repeats rows already read is
    # never taken for the board's end.
    rows = _uhs_rows({"Texas": 450})
    _deep_board(monkeypatch, rows, window_pages=3)        # pages 4.. repeat page 3
    monkeypatch.setattr(scraper, "JIBE_DEEP_WINDOW", 10000)
    jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    assert len(jobs) == 300
    assert UHS in scraper.PARTIAL_SYSTEMS and UHS not in scraper.COMPLETE_SYSTEMS


def test_a_state_page_that_fails_makes_the_sliced_read_partial(monkeypatch, quiet):
    rows = _uhs_rows({"Texas": 3100, "Florida": 2500})
    _deep_board(monkeypatch, rows, fail=("Texas", 7))
    jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    assert len(jobs) == 5600 - (3100 - 600)
    assert UHS in scraper.PARTIAL_SYSTEMS and UHS not in scraper.COMPLETE_SYSTEMS


def test_postings_in_no_listed_state_keep_the_sliced_read_partial(monkeypatch, quiet):
    rows = _uhs_rows({"Texas": 4000, "Florida": 1000, "": 300})
    _deep_board(monkeypatch, rows)
    jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    assert len(jobs) == 5000 + sum(1 for r in rows[:100] if r["data"]["state"] == "")
    assert UHS in scraper.PARTIAL_SYSTEMS and UHS not in scraper.COMPLETE_SYSTEMS


def test_a_posting_in_two_states_is_one_row(monkeypatch, quiet):
    rows = _uhs_rows({"Texas": 2600, "Oklahoma": 2600})
    dup = json.loads(json.dumps(rows[5]))
    dup["data"]["state"] = "Oklahoma" if rows[5]["data"]["state"] == "Texas" else "Texas"
    _deep_board(monkeypatch, rows + [dup])
    jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    assert len(jobs) == 5200 and UHS in scraper.COMPLETE_SYSTEMS


def test_state_names_cover_the_states_dc_and_puerto_rico():
    names = scraper.JIBE_STATE_NAMES
    assert len(names) == 53 and {"Texas", "West Virginia", "Washington, DC", "District of Columbia",
                                 "Puerto Rico"} <= names


# ══ 2. Trinity Health: Workday slices past the window ════════════════════════
NURS, SUPPORT = "g-nurs", "g-support"
FT, PT = "t-ft", "t-pt"
IA, MI = "s-ia", "s-mi"


def _wd_post(n, fam, tt, st):
    return {"title": f"Registered Nurse {n}", "externalPath": f"/job/Des-Moines-Iowa/RN_{n:08d}",
            "locationsText": "Des Moines, Iowa", "postedOn": "Posted Today", "bulletFields": [f"{n:08d}"],
            "_f": fam, "_t": tt, "_s": st}


def _trinity_board(n_nurs_ft=60, n_nurs_pt=25, n_support=30):
    posts, n = [], 0
    for fam, tt, k in ((NURS, FT, n_nurs_ft), (NURS, PT, n_nurs_pt), (SUPPORT, FT, n_support)):
        for i in range(k):
            posts.append(_wd_post(n, fam, tt, IA if i % 3 else MI))
            n += 1
    return posts


def _wd_fake(monkeypatch, posts, window):
    bodies = []
    monkeypatch.setattr(scraper, "WD_FACET_WINDOW", window)

    def facets_for(sub):
        def vals(key, ids):
            return [{"id": i, "descriptor": i, "count": sum(1 for p in sub if p[key] == i)} for i in ids]
        return [{"facetParameter": "jobFamilyGroup", "values": vals("_f", (NURS, SUPPORT))},
                {"facetParameter": "Location_Region_State_Province", "values": vals("_s", (IA, MI))},
                {"facetParameter": "timeType", "values": vals("_t", (FT, PT))}]

    def answer(body):
        applied = body.get("appliedFacets") or {}
        keymap = {"jobFamilyGroup": "_f", "timeType": "_t", "Location_Region_State_Province": "_s"}
        sub = [p for p in posts if all(p[keymap[k]] in v for k, v in applied.items())]
        if body.get("limit") == 1:
            return {"total": min(len(sub), window), "jobPostings": sub[:1], "facets": facets_for(sub)}
        off = body["offset"]
        page = sub[off:off + 20] if off < window else sub[window - 20:window]    # Workday repeats past the window
        return {"total": min(len(sub), window) if off == 0 else 0, "jobPostings": page}

    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        bodies.append(body)
        return _Ctx(_R(answer(body)))
    monkeypatch.setattr(scraper, "req", req)
    return bodies


def test_split_facet_prefers_a_covering_fitting_facet_with_fewest_values():
    data = {"facets": [
        {"facetParameter": "jobFamilyGroup", "values": [{"id": "a", "count": 2299}]},
        {"facetParameter": "Location_Region_State_Province",
         "values": [{"id": f"s{i}", "count": 2357 // 18} for i in range(18)]},
        {"facetParameter": "timeType", "values": [{"id": "ft", "count": 1586}, {"id": "pt", "count": 713}]},
        {"facetParameter": "workerSubType", "values": [{"id": "r", "count": 1843}, {"id": "c", "count": 453},
                                                       {"id": "t", "count": 2}, {"id": "x", "count": 1}]},
    ]}
    param, values, covers = scraper._wd_split_facet(data, {"jobFamilyGroup": ["a"]}, 2299)
    assert param == "timeType" and values == [("ft", 1586), ("pt", 713)] and covers
    # already applied, single-valued or too many values: never used
    assert scraper._wd_split_facet(data, {"jobFamilyGroup": ["a"], "timeType": ["ft"]}, 1586)[0] == "workerSubType"
    many = {"facets": [{"facetParameter": "locations", "values": [{"id": str(i), "count": 5} for i in range(200)]}]}
    assert scraper._wd_split_facet(many, {}, 1000) is None


def test_a_family_past_the_window_is_split_and_every_posting_read(monkeypatch, quiet):
    # Trinity 10-26 in miniature (window 40): Nursing 85 > 40 is never crawled
    # whole; it is split (by state here: 57 + 28, two values like timeType)
    # and the 57 split again by timeType.
    posts = _trinity_board()
    bodies = _wd_fake(monkeypatch, posts, window=40)
    jobs = asyncio.run(scraper.scrape_workday(None, "Trinity Health", ("trinityhealth", "1", "Jobs")))
    assert len(jobs) == len(posts) == 115
    nursing_whole = [b for b in bodies if b.get("limit") == 20
                     and (b.get("appliedFacets") or {}) == {"jobFamilyGroup": [NURS]}]
    assert nursing_whole == []
    split = {tuple(sorted((b.get("appliedFacets") or {}).keys())) for b in bodies if b.get("limit") == 20}
    assert ("Location_Region_State_Province", "jobFamilyGroup") in split
    assert ("Location_Region_State_Province", "jobFamilyGroup", "timeType") in split
    assert "Trinity Health" not in scraper.COMPLETE_SYSTEMS        # a sliced crawl never claims


def test_slices_under_the_window_are_crawled_as_before(monkeypatch, quiet):
    posts = _trinity_board(n_nurs_ft=20, n_nurs_pt=10, n_support=15)
    bodies = _wd_fake(monkeypatch, posts, window=40)
    jobs = asyncio.run(scraper.scrape_workday(None, "Trinity Health", ("trinityhealth", "1", "Jobs")))
    assert len(jobs) == 45
    # the plain crawl fills the window (45 > 40): one probe for the family
    # counts, and both families (30 and 15) are crawled whole, unsplit
    assert len([b for b in bodies if b.get("limit") == 1]) == 1
    fams = [b["appliedFacets"] for b in bodies if b.get("limit") == 20 and b.get("appliedFacets")]
    assert all(list(f) == ["jobFamilyGroup"] for f in fams)


def test_a_slice_with_no_usable_split_keeps_its_window(monkeypatch, quiet, caplog):
    posts = _trinity_board(n_nurs_ft=85, n_nurs_pt=0, n_support=10)
    for p in posts:
        p["_s"] = IA                                                   # one state, one time type: nothing to split by
    _wd_fake(monkeypatch, posts, window=40)
    with caplog.at_level("INFO", logger=scraper.logger.name):
        jobs = asyncio.run(scraper.scrape_workday(None, "Trinity Health", ("trinityhealth", "1", "Jobs")))
    assert len(jobs) == 40 + 10
    assert any("ALSO hit the window" in r.getMessage() for r in caplog.records)


def test_the_workday_window_is_one_constant():
    assert scraper.WD_FACET_WINDOW == 2000 and scraper.WD_SUBSLICE_DEPTH >= 2


# ══ 3. Dead feeds ════════════════════════════════════════════════════════════
def test_compassus_reads_its_new_workday_site():
    assert scraper.WORKDAY_TENANTS["Compassus"] == ("hospicecom", "5", "compassuscareers")


def _cws(items, total):
    return "CWS.jobs.jobCallback(" + json.dumps({"totalHits": total, "queryResult": items}) + ");"


def _nyu_item(i, dept="NYU Langone Hospital - Brooklyn", salary="$106,916.04-$138,356.04/year"):
    return {"id": 24050000 + i, "ref": f"1168{i:03d}_RR00125{i:03d}", "title": "Registered Nurse",
            "primary_city": "Brooklyn", "primary_state": "NY", "brand": "Brooklyn", "department": dept,
            "url": f"http://jobs.nyulangone.org/job/{24050000 + i}/registered-nurse-brooklyn-ny/",
            "open_date": "2026-10-23T21:02:56.907Z", "employment_type": "Full-Time/Regular",
            "salary": salary, "description": "<p>" + "Provides nursing care. " * 80 + "</p>"}


def _findly_fake(monkeypatch, items, total, fail_offset=None):
    def req(session, method, url, **kw):
        off = int((kw.get("params") or {}).get("offset", "1"))
        if off == fail_offset:
            return _Ctx(_R(status=500, text=""))
        return _Ctx(_R(text=_cws(items[off - 1:off - 1 + 100], total)))
    monkeypatch.setattr(scraper, "req", req)


def test_nyu_langone_rows_take_the_hospital_pay_and_https_link(monkeypatch, quiet):
    items = [_nyu_item(0), _nyu_item(1, dept="NYU Grossman School of Medicine"),
             _nyu_item(2, dept="NYU Langone Hospital—Suffolk", salary="")]
    _findly_fake(monkeypatch, items, 3)
    cfg = scraper.FINDLY_CWS_ORGS["NYU Langone Health"]
    assert cfg == ("1637", "Silkroad~Silkroad_Winthrop")
    jobs = asyncio.run(scraper.scrape_findly(None, "NYU Langone Health", cfg))
    assert [j.hospital_name for j in jobs] == ["NYU Langone Hospital - Brooklyn", "NYU Langone Health",
                                               "NYU Langone Hospital—Suffolk"]
    assert (jobs[0].wage_min, jobs[0].wage_max, jobs[0].wage_unit) == (106916.04, 138356.04, "year")
    assert jobs[2].wage_min is None
    assert all(j.url.startswith("https://jobs.nyulangone.org/job/") for j in jobs)
    assert jobs[0].job_id == "1168000_RR00125000" and jobs[0].state == "NY"
    assert "NYU Langone Health" in scraper.COMPLETE_SYSTEMS
    assert ("FINDLY_CWS_ORGS", "NYU Langone Health") in scraper.COMPLETE_FEEDS


def test_texas_health_keeps_its_brand_and_a_failed_page_is_partial(monkeypatch, quiet):
    items = [dict(_nyu_item(i), brand="Texas Health HEB", url=f"https://jobs.texashealth.org/job/{i}")
             for i in range(150)]
    _findly_fake(monkeypatch, items, 150, fail_offset=101)
    jobs = asyncio.run(scraper.scrape_findly(None, "Texas Health Resources", ("2277", "TexasHealth-Taleo-External")))
    assert len(jobs) == 100 and all(j.hospital_name == "Texas Health HEB" for j in jobs)
    assert all(j.wage_min is None for j in jobs)                     # no pay key for Texas Health
    assert "Texas Health Resources" in scraper.PARTIAL_SYSTEMS
    assert "Texas Health Resources" not in scraper.COMPLETE_SYSTEMS


def test_silkroad_list_page(fixture_text):
    html = fixture_text(FX + "silkroad_temple_list_p1.html")
    assert scraper.silkroad_pager(html) == (1, 18)
    jobs = scraper._parse_silkroad(html, "Temple Health", scraper.SILKROAD_SITES["Temple Health"])
    assert len(jobs) == 25 and len({j.job_id for j in jobs}) == 25
    j = jobs[0]
    assert (j.job_id, j.title, j.city, j.state, j.ats_platform) == (
        "1069", "Associate Application Analyst-Epic Willow", "Philadelphia", "PA", "SilkRoad")
    assert j.url == "https://jobs.silkroad.com/templehealth/Careers/jobs/1069"
    assert j.hospital_system == j.hospital_name == "Temple Health"


def test_silkroad_card_with_no_us_state_takes_the_default():
    card = ('<a id="Jobs_PagedJobList_Job-1063" class="sr-panel" href="/templehealth/Careers/jobs/1063">'
            '<div class="sr-panel__title" id="t">SR Coding Specialist</div>'
            '<div class="sr-panel__location" id="l"><span class="sr-panel__meta">NA, NA</span></div></a>')
    j = scraper._parse_silkroad(card, "Temple Health", ("templehealth", "Careers", "PA"))[0]
    assert (j.city, j.state) == ("", "PA")


def test_silkroad_job_page(fixture_text):
    html = fixture_text(FX + "silkroad_temple_job_1066.html")
    f = scraper.silkroad_job_page(html)
    assert f["tracking code"] == "270026" and f["category"] == "Nursing" and f["schedule"] == "Full-Time"
    job = scraper.Job(title="Nursing Assistant", hospital_system="Temple Health", hospital_name="Temple Health",
                      city="Philadelphia", state="PA", location="Philadelphia, PA", specialty="", job_type="",
                      url="https://jobs.silkroad.com/templehealth/Careers/jobs/1066", job_id="1066",
                      posted_date="", description="", ats_platform="SilkRoad")
    assert scraper._silkroad_apply_page(job, html)
    assert job.req_id == "270026" and job.specialty == "Nursing" and job.job_type == "Full-Time"
    assert job.description.startswith("Assists the RN by performing patient care")
    assert "Schedule: Full-Time · Night Flex · 36 hours per week" in job.description
    assert not scraper._silkroad_apply_page(job, "<html><body>Temple Health Jobs</body></html>")


def _silkroad_fake(monkeypatch, pages, fail_page=None):
    """pages: list of lists of posting ids; each page prints "Page i of N"."""
    n = len(pages)

    def page_html(i):
        cards = "".join(f'<a id="Jobs_PagedJobList_Job-{p}" class="sr-panel" href="/templehealth/Careers/jobs/{p}">'
                        f'<div class="sr-panel__title">RN {p}</div><div class="sr-panel__location">'
                        f'<span class="sr-panel__meta">Philadelphia, PA</span></div></a>' for p in pages[i - 1])
        return f"<html><body>{cards}<div class=\"sr-pagination__pager\">Page {i} of {n}</div></body></html>"

    def req(session, method, url, **kw):
        i = int(url.rsplit("page=", 1)[1])
        if i == fail_page:
            return _Ctx(_R(status=503, text=""))
        return _Ctx(_R(text=page_html(i)))
    monkeypatch.setattr(scraper, "req", req)


def test_silkroad_whole_board_is_complete(monkeypatch, quiet):
    _silkroad_fake(monkeypatch, [list(range(1000 + 25 * k, 1025 + 25 * k)) for k in range(3)])
    jobs = asyncio.run(scraper.scrape_silkroad(None, "Temple Health", scraper.SILKROAD_SITES["Temple Health"]))
    assert len(jobs) == 75 and "Temple Health" in scraper.COMPLETE_SYSTEMS
    assert ("SILKROAD_SITES", "Temple Health") in scraper.COMPLETE_FEEDS


def test_silkroad_failed_page_is_partial_and_a_moved_list_claims_nothing(monkeypatch, quiet):
    _silkroad_fake(monkeypatch, [list(range(1000 + 25 * k, 1025 + 25 * k)) for k in range(3)], fail_page=2)
    jobs = asyncio.run(scraper.scrape_silkroad(None, "Temple Health", scraper.SILKROAD_SITES["Temple Health"]))
    assert len(jobs) == 25 and "Temple Health" in scraper.PARTIAL_SYSTEMS
    scraper.PARTIAL_SYSTEMS.clear()
    _silkroad_fake(monkeypatch, [[1, 2, 3], [3, 4, 5], [6]])          # posting 3 listed twice
    jobs = asyncio.run(scraper.scrape_silkroad(None, "Temple Health", scraper.SILKROAD_SITES["Temple Health"]))
    assert len(jobs) == 6
    assert "Temple Health" not in scraper.COMPLETE_SYSTEMS and "Temple Health" not in scraper.PARTIAL_SYSTEMS


def test_silkroad_is_wired_and_temple_left_phenom():
    assert "SILKROAD_SITES" in scraper.FEED_CONFIGS
    assert "SilkRoad" in scraper.KNOWN_BODY_PLATFORMS and "SilkRoad" in scraper.REQ_ID_ONLY_PLATFORMS
    assert "Temple Health" not in scraper.PHENOM_ORGS and "Temple Health" not in scraper.PHENOM_ORG_CODES
    assert scraper.configured_feed_map()["Temple Health"] == {"SILKROAD_SITES"}
    src = open(scraper.__file__, encoding="utf-8").read()
    assert "run_silkroad(proxy_session)" in src


# ══ 4. HCA: the detail pass ends before the task's limit ════════════════════
def _hca_jobs(n):
    return [scraper.Job(title="RN", hospital_system="HCA Healthcare", hospital_name="HCA Healthcare",
                        city="Plano", state="TX", location="Plano, TX", specialty="", job_type="",
                        url=f"https://careers.hcahealthcare.com/jobs/{i}-rn", job_id=str(i), posted_date="",
                        description="", ats_platform="Talemetry") for i in range(n)]


def _fake_curl(monkeypatch, fetched):
    page = ('<script type="application/ld+json">' + json.dumps(
        {"@type": "JobPosting", "description": "Bedside nursing. " * 40}) + "</script>")

    def get(url, **kw):
        fetched.append(url)
        return types.SimpleNamespace(status_code=200, text=page)
    fake = types.ModuleType("curl_cffi")
    fake.requests = types.SimpleNamespace(get=get)
    monkeypatch.setitem(sys.modules, "curl_cffi", fake)
    monkeypatch.setattr(hlp.time, "sleep", lambda s: None)


def test_hca_detail_pass_starts_no_page_past_the_deadline(monkeypatch, capsys):
    fetched = []
    _fake_curl(monkeypatch, fetched)
    monkeypatch.setenv("HCA_DESC_MAX_PER_RUN", "50")
    monkeypatch.setattr(hlp, "HCA_DETAIL_DEADLINE_MIN", 120)
    monkeypatch.setattr(hlp, "_RUN_START", [time.monotonic() - 121 * 60])   # the run began 121 min ago
    jobs = _hca_jobs(10)
    hlp._hca_detail_pass(jobs)
    assert fetched == [] and all(not j.description for j in jobs)
    assert "10 pages left for the next run" in capsys.readouterr().out


def test_hca_detail_pass_inside_the_deadline_reads_pages(monkeypatch):
    fetched = []
    _fake_curl(monkeypatch, fetched)
    monkeypatch.setenv("HCA_DESC_MAX_PER_RUN", "50")
    monkeypatch.setattr(hlp, "_RUN_START", [time.monotonic()])
    jobs = _hca_jobs(6)
    hlp._hca_detail_pass(jobs)
    assert len(fetched) == 6 and all(len(j.description) > 200 for j in jobs)
    monkeypatch.setattr(hlp, "HCA_DETAIL_DEADLINE_MIN", 0)                  # 0 = no limit
    monkeypatch.setattr(hlp, "_RUN_START", [time.monotonic() - 10 * 3600])
    assert hlp._detail_deadline() is None


def test_crawl_and_push_starts_the_clock(monkeypatch):
    monkeypatch.setattr(hlp, "_RUN_START", [None])
    monkeypatch.setattr(scraper, "run_hca", lambda session: asyncio.sleep(0, result=[]))
    assert hlp.crawl_and_push() == 2                                         # nothing scraped: abort
    assert hlp._RUN_START[0] is not None and hlp._detail_deadline() > time.monotonic()


# ══ 5. Select Medical (iCIMS) ═══════════════════════════════════════════════
SEL = "Select Medical"


def _icims_page(company="Select Specialty Hospital - Knoxville"):
    hdr = ('<dl class="iCIMS_JobHeaderGroup">'
           '<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Job ID</dt>'
           '<dd class="iCIMS_JobHeaderData"><span>382344</span></dd></div>'
           + (f'<div class="iCIMS_JobHeaderTag"><dt class="iCIMS_JobHeaderField">Company</dt>'
              f'<dd class="iCIMS_JobHeaderData"><span>\n{company}</span></dd></div>' if company else "")
           + '</dl>')
    ld = json.dumps({"@context": "http://schema.org", "@type": "JobPosting", "title": "Telemetry Technician",
                     "description": "<p>" + "Monitors cardiac rhythms. " * 80 + "</p>",
                     "datePosted": "2026-10-20T04:00:00.000Z",
                     "jobLocation": {"address": {"addressLocality": "Knoxville", "addressRegion": "TN"}}})
    return f'<html><body>{hdr}<script type="application/ld+json">{ld}</script></body></html>'


def _sel_job(jid="382344", name=SEL, system=SEL):
    return scraper.Job(title="Telemetry Technician - FT Nights", hospital_system=system, hospital_name=name,
                       city="Knoxville", state="TN", location="US-TN-Knoxville", specialty="", job_type="",
                       url=f"https://jobs-selectmedicalcorp.icims.com/jobs/{jid}/telemetry-technician/job",
                       job_id=jid, posted_date="", description="", ats_platform="iCIMS")


def test_the_company_field_becomes_the_facility_on_select_only():
    job = _sel_job()
    assert scraper._icims_apply_page(job, _icims_page())
    assert job.hospital_name == "Select Specialty Hospital - Knoxville" and job.hospital_system == SEL
    other = _sel_job(system="Prime Healthcare", name="Prime Healthcare")
    scraper._icims_apply_page(other, _icims_page("Prime Healthcare Services - Garden Grove LLC"))
    assert other.hospital_name == "Prime Healthcare"
    blank = _sel_job()
    scraper._icims_apply_page(blank, _icims_page(company=""))
    assert blank.hospital_name == SEL
    assert scraper.icims_company({"company": "  Select Physical   Therapy "}) == "Select Physical Therapy"
    assert scraper.icims_company({"company": "N/A"}) == "" and scraper.icims_company({}) == ""


def test_a_waf_page_names_no_facility():
    job = _sel_job()
    waf = ("<html><head><title>Human Verification</title><script>window.awsWafCookieDomainList = [];"
           "</script></head><body><div id='captcha-container'></div></body></html>")
    assert not scraper._icims_apply_page(job, waf) and job.hospital_name == SEL
    assert scraper.is_waf_challenge(405, "") and scraper.is_waf_challenge(200, waf)
    assert not scraper.is_waf_challenge(200, _icims_page())


@pytest.fixture
def stored(monkeypatch):
    def install(rows):
        scraper.set_stored_companies(rows)
    yield install
    scraper.set_stored_companies([])


def test_list_rows_keep_the_stored_facility(stored):
    stored([{"hospital_system": SEL, "job_id": "1", "hospital_name": "Select Specialty Hospital - Knoxville"},
            {"hospital_system": SEL, "job_id": "2", "hospital_name": SEL},
            {"hospital_system": "Prime Healthcare", "job_id": "3", "hospital_name": "X"}])
    a, b, c = _sel_job("1"), _sel_job("2"), _sel_job("9")
    for j in (a, b, c):
        scraper._keep_stored_company(j)
    assert (a.hospital_name, b.hospital_name, c.hospital_name) == ("Select Specialty Hospital - Knoxville", SEL, SEL)
    own = _sel_job("1", name="Kessler Institute")       # a list that names one keeps its own
    scraper._keep_stored_company(own)
    assert own.hospital_name == "Kessler Institute"
    assert ("Prime Healthcare", "3") not in scraper._STORED_COMPANY


def _known(jid, n=3000):
    return {"hospital_system": SEL, "job_id": jid, "desc_len": n, "fv": scraper.FACTS_VERSION,
            "ats_platform": "iCIMS"}


def _slot_ids(day, k=40):
    n = scraper.ICIMS_FACILITY_REREAD_CYCLE
    on = [str(i) for i in range(1000, 1400) if zlib.crc32(f"company|{SEL}|{i}".encode()) % n == day % n]
    off = [str(i) for i in range(1000, 1400) if zlib.crc32(f"company|{SEL}|{i}".encode()) % n != day % n]
    return on[:k], off[:k]


def test_stored_rows_on_the_label_are_read_once_a_cycle_for_their_company(stored, monkeypatch):
    day = scraper.datetime(2026, 11, 2).toordinal()
    monkeypatch.setattr(scraper, "_RUN_DAY", [day])
    on, off = _slot_ids(day)
    named = on[0]
    stored([{"hospital_system": SEL, "job_id": j, "hospital_name": SEL} for j in on[1:] + off]
           + [{"hospital_system": SEL, "job_id": named, "hospital_name": "Select Specialty Hospital - Knoxville"}])
    scraper.set_known_bodies([_known(j) for j in on + off])
    try:
        assert scraper._known_kind(SEL, _sel_job(on[1])) == "facility"
        assert scraper._known_kind(SEL, _sel_job(off[0])) == "known"
        assert scraper._known_kind(SEL, _sel_job(named)) == "known"        # facility already stored
        budget = scraper._DescBudget(1000)
        held = []
        jobs = [_sel_job(j) for j in on + off]
        cands, known, dup = scraper._detail_candidates(SEL, jobs, budget, held=held)
        assert {j.job_id for j in cands} == set(on[1:])
        assert {k for _, _, k in held if k == "facility"} == {"facility"} and len(held) == len(on) - 1
        monkeypatch.setattr(scraper, "_RUN_DAY", [scraper.datetime(2027, 1, 2).toordinal()])
        assert not scraper._company_reread_slot((SEL, on[1]))             # past ICIMS_FACILITY_REREAD_UNTIL
    finally:
        scraper.set_known_bodies([])


def test_no_stored_map_no_reread(stored):
    stored([])
    scraper.set_known_bodies([_known("1234")])
    try:
        assert scraper._known_kind(SEL, _sel_job("1234")) in ("known", "stale")
    finally:
        scraper.set_known_bodies([])


def _card(jid, title="Physical Therapist"):
    return (f'<li class="iCIMS_JobCardItem"><a href="https://jobs-selectmedicalcorp.icims.com/jobs/{jid}/pt/job'
            f'?in_iframe=1"><h3>{title}</h3></a><dl class="iCIMS_JobHeaderGroup"><dt class="iCIMS_JobHeaderField">'
            f'Job Locations</dt><dd class="iCIMS_JobHeaderData">US-TN-Knoxville</dd></dl></li>')


def _card_page(ids, page, of):
    pager = f'<h2 class="iCIMS_SubHeader iCIMS_SubHeader_Jobs">Search Results\nPage {page} of {of} </h2>' if of else ""
    return f"<html><body>{pager}<ul>{''.join(_card(i) for i in ids)}</ul></body></html>"


def _icims_fake(monkeypatch, pages, of=True, answer=None):
    """pages[0] is the first (classic) response; pr=k serves pages[k].
    answer(k) -> (status, text) overrides a page."""
    n = len(pages)

    def req(session, method, url, **kw):
        k = int((kw.get("params") or {}).get("pr", "0"))
        if answer and answer(k):
            st, text = answer(k)
            return _Ctx(_R(status=st, text=text, headers={"content-type": "text/html"}))
        ids = pages[k] if k < n else pages[-1]
        return _Ctx(_R(text=_card_page(ids, min(k, n - 1) + 1, n if of else None),
                       headers={"content-type": "text/html; charset=utf-8"}))
    monkeypatch.setattr(scraper, "req", req)


def _pages_of(n_pages, per=50):
    return [[str(370000 + p * per + i) for i in range(per)] for p in range(n_pages)]


def test_an_icims_card_crawl_that_reaches_page_n_of_n_is_complete(monkeypatch, quiet):
    _icims_fake(monkeypatch, _pages_of(4))
    jobs = asyncio.run(scraper.scrape_icims(None, SEL, "jobs-selectmedicalcorp.icims.com"))
    assert len(jobs) == 200
    assert SEL in scraper.COMPLETE_SYSTEMS and ("ICIMS_ORGS", SEL) in scraper.COMPLETE_FEEDS
    assert SEL not in scraper.PARTIAL_SYSTEMS


def test_a_waf_captcha_mid_crawl_is_partial(monkeypatch, quiet):
    waf = "<html><head><title>Human Verification</title></head><body></body></html>"
    _icims_fake(monkeypatch, _pages_of(4), answer=lambda k: (405, waf) if k == 2 else None)
    jobs = asyncio.run(scraper.scrape_icims(None, SEL, "jobs-selectmedicalcorp.icims.com"))
    assert len(jobs) == 100
    assert SEL in scraper.PARTIAL_SYSTEMS and SEL not in scraper.COMPLETE_SYSTEMS


def test_a_failed_page_before_page_n_is_partial_and_a_moved_list_claims_nothing(monkeypatch, quiet):
    _icims_fake(monkeypatch, _pages_of(4), answer=lambda k: (502, "") if k == 3 else None)
    jobs = asyncio.run(scraper.scrape_icims(None, SEL, "jobs-selectmedicalcorp.icims.com"))
    assert len(jobs) == 150 and SEL in scraper.PARTIAL_SYSTEMS
    scraper.PARTIAL_SYSTEMS.clear()
    pages = _pages_of(3)
    pages[2] = pages[1][-5:] + pages[2][5:]                       # 5 postings shifted onto the next page
    _icims_fake(monkeypatch, pages)
    jobs = asyncio.run(scraper.scrape_icims(None, SEL, "jobs-selectmedicalcorp.icims.com"))
    assert len(jobs) == 145
    assert SEL not in scraper.COMPLETE_SYSTEMS and SEL not in scraper.PARTIAL_SYSTEMS


def test_a_portal_without_a_pager_keeps_the_old_rule(monkeypatch, quiet):
    _icims_fake(monkeypatch, _pages_of(3), of=False, answer=lambda k: (500, "") if k == 2 else None)
    jobs = asyncio.run(scraper.scrape_icims(None, "Covenant Health", "careers-covenanthealth.icims.com"))
    assert len(jobs) == 100
    assert "Covenant Health" not in scraper.PARTIAL_SYSTEMS and "Covenant Health" not in scraper.COMPLETE_SYSTEMS


def test_a_waf_answer_to_the_first_request_is_partial(monkeypatch, quiet):
    _icims_fake(monkeypatch, _pages_of(2), answer=lambda k: (405, "<title>Human Verification</title>"))
    jobs = asyncio.run(scraper.scrape_icims(None, SEL, "jobs-selectmedicalcorp.icims.com"))
    assert jobs == [] and SEL in scraper.PARTIAL_SYSTEMS


def test_icims_pager_reads_the_search_header():
    assert scraper.icims_pager("<h2>Search Results\nPage 1 of 73 \n</h2>") == (1, 73)
    assert scraper.icims_pager("<html>no pager</html>") is None
