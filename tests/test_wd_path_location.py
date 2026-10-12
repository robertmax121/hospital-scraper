"""Workday rows located from externalPath (WD_PATH_LOCATION, 2026-10-12).

Option Care Health's multi-site postings read "2 Locations" in
locationsText and a few sites carry no state ("Tampa", "Woodlands AIC"),
so 42 of 286 live postings left the adapter with no city and no state and
the board (which shows only rows with both) hid them. The externalPath's
first segment names the primary site; these tests pin the parse, the
tenant map, the adapter hook and its scope. No network: scraper.req is
replaced by a fake that answers from the postings below (taken from the
live board on 2026-10-12).
"""
import asyncio

import scraper

OC = "Option Care Health"

POSTINGS = [
    {"title": "Territory Sales Manager", "locationsText": "3 Locations",
     "externalPath": "/job/Hayward-CA/Territory-Sales-Manager_R37386-1", "bulletFields": ["R37386"]},
    {"title": "Home Infusion RN - Camp Hill", "locationsText": "2 Locations",
     "externalPath": "/job/Camp-Hill-PA/Home-Infusion-Nurse---Camp-Hill_R37229", "bulletFields": ["R37229"]},
    {"title": "Home Infusion Nurse - Aurora / Lakewood", "locationsText": "3 Locations",
     "externalPath": "/job/Englewood-Colorado/Home-Infusion-Nurse---Aurora---Lakewood_R36870", "bulletFields": ["R36870"]},
    {"title": "Compounding Pharmacy Technician I", "locationsText": "2 Locations",
     "externalPath": "/job/Tampa/Compounding-Pharmacy-Technician-I_R37387", "bulletFields": ["R37387"]},
    {"title": "Infusion Nurse - Murray", "locationsText": "Murray",
     "externalPath": "/job/Murray/Infusion-Nurse---Murray_R36297", "bulletFields": ["R36297"]},
    {"title": "Infusion Nurse - Woodlands", "locationsText": "Woodlands AIC",
     "externalPath": "/job/Woodlands-AIC/Infusion-Nurse---Woodlands_R37173", "bulletFields": ["R37173"]},
    {"title": "Clinical Pharmacist", "locationsText": "Intramed Plus Columbia",
     "externalPath": "/job/Intramed-Plus-Columbia/Clinical-Pharmacist_R36874", "bulletFields": ["R36874"]},
    {"title": "Benefits Verification Specialist", "locationsText": "Phoenix, AZ",
     "externalPath": "/job/Phoenix-AZ/Benefits-Verification_R37455", "bulletFields": ["R37455"]},
]


class _Resp:
    status = 200

    def __init__(self, body):
        self._body = body

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, body):
        self.resp = _Resp(body)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


def _fake_board(monkeypatch, postings):
    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("appliedFacets") or body.get("limit") == 1 or body.get("offset", 0):
            return _Ctx({"total": 0, "jobPostings": [], "facets": []})
        return _Ctx({"total": len(postings), "jobPostings": [dict(p) for p in postings]})

    async def no_wait():
        return None

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)


def test_path_segment_city_and_state_code():
    f = scraper._wd_path_location
    assert f(OC, "/job/Hayward-CA/Territory-Sales-Manager_R37386-1") == ("Hayward", "CA")
    assert f(OC, "/job/Salt-Lake-City-UT/Intake-Specialist_R36634") == ("Salt Lake City", "UT")
    assert f(OC, "job/New-Bedford-MA/Home-Infusion-Nurse_R37238") == ("New Bedford", "MA")


def test_path_segment_with_a_state_name():
    f = scraper._wd_path_location
    assert f(OC, "/job/Englewood-Colorado/Home-Infusion-Nurse_R36870") == ("Englewood", "CO")
    assert f(OC, "/job/Charleston-West-Virginia/RN_R1") == ("Charleston", "WV")
    assert f(OC, "/job/New-York-New-York/RN_R1") == ("New York", "NY")


def test_stateless_site_names_use_the_tenant_map():
    f = scraper._wd_path_location
    assert f(OC, "/job/Tampa/Warehouse-Coordinator_R35862") == ("Tampa", "FL")
    assert f(OC, "/job/Murray/Infusion-Nurse---Murray_R36297") == ("Murray", "UT")
    assert f(OC, "/job/Woodlands-AIC/Infusion-Nurse---Woodlands_R37173") == ("Shenandoah", "TX")
    assert f(OC, "/job/Intramed-Plus-Columbia/Clinical-Pharmacist_R36874") == ("West Columbia", "SC")
    assert f(OC, "/job/Intramed-Plus-Charleston/Compounding-Pharmacy-Technician-II_R36410") == ("Mount Pleasant", "SC")


def test_segments_that_name_no_state_give_nothing():
    f = scraper._wd_path_location
    assert f(OC, "/job/Somewhere-Unknown/RN_R1") == ("", "")
    assert f(OC, "/job/Remote/RN_R1") == ("", "")
    assert f(OC, "/job/Kansas/RN_R1") == ("", "")          # a state alone names no town
    assert f(OC, "/job/Main-Campus-or/RN_R1") == ("", "")   # lower-case two letters are not a code
    assert f(OC, "/job/Main-Campus-XX/RN_R1") == ("", "")
    assert f(OC, "") == ("", "")
    assert f(OC, "/details/Tampa/RN_R1") == ("", "")
    # The site map belongs to its tenant only.
    assert f("Some Other Board", "/job/Tampa/RN_R1") == ("", "")


def test_option_care_multi_site_rows_are_located(monkeypatch):
    _fake_board(monkeypatch, POSTINGS)
    jobs = asyncio.run(scraper.scrape_workday(None, OC, scraper.WORKDAY_TENANTS[OC]))
    got = {j.job_id: (j.city, j.state) for j in jobs}
    assert got == {
        "R37386": ("Hayward", "CA"),
        "R37229": ("Camp Hill", "PA"),
        "R36870": ("Englewood", "CO"),
        "R37387": ("Tampa", "FL"),
        "R36297": ("Murray", "UT"),
        "R37173": ("Shenandoah", "TX"),
        "R36874": ("West Columbia", "SC"),
        "R37455": ("Phoenix", "AZ"),          # a parsed "City, ST" is untouched
    }
    rows = scraper.finalize_jobs(jobs)
    assert len(rows) == len(POSTINGS)
    # Every row now meets the board's rule (city and state both set).
    assert all(r["city"] and r["state"] for r in rows)
    assert {r["location"] for r in rows if r["job_id"] == "R37386"} == {"Hayward, CA"}


def test_a_parsed_state_is_never_replaced(monkeypatch):
    # A posting whose locationsText already gave a state keeps it even when
    # the path names another site.
    p = {"title": "RN", "locationsText": "Itasca, IL",
         "externalPath": "/job/Hayward-CA/RN_R1", "bulletFields": ["R1"]}
    _fake_board(monkeypatch, [p])
    jobs = asyncio.run(scraper.scrape_workday(None, OC, scraper.WORKDAY_TENANTS[OC]))
    assert [(j.city, j.state) for j in jobs] == [("Itasca", "IL")]


def test_unlisted_tenant_reads_the_path_after_its_own_parse(monkeypatch):
    # 2026-10-12 (extended to every tenant): a tenant not in WD_PATH_LOCATION
    # now takes the path's site in _wd_fill_states, after the facet pass, and
    # "N Locations" never survives as a city.
    assert "Plain Board" not in scraper.WD_PATH_LOCATION
    _fake_board(monkeypatch, POSTINGS[:2])
    jobs = asyncio.run(scraper.scrape_workday(None, "Plain Board", ("plain", "5", "External")))
    assert [(j.city, j.state) for j in jobs] == [("Hayward", "CA"), ("Camp Hill", "PA")]
