"""2026-10-30 (facility scale round 2, CHRISTUS group): CHRISTUS rows filed
under a regional entity take the hospital at their feed street address
(facilities_christus), wired in run_playwright_scrapers.

Fixture (tests/fixtures/christus1030/feed_entries.json): 45 entries of the
career site's own feed (POST /job-search/jobs, read 2026-10-30), the fields
the adapter reads, as served; "_case" is the expectation. Job titles, units
and street addresses only: no person's name, e-mail or phone.
No network, no database, no browser (the Playwright test drives a fake one)."""
import asyncio
import json
import os
import sys
import types

import pytest

import facilities_christus as C
import facilities_fac7_variants as V
import scraper
from conftest import FIXTURES

with open(os.path.join(FIXTURES, "christus1030", "feed_entries.json"), encoding="utf-8") as _f:
    ENTRIES = json.load(_f)["entries"]
MULTI = C.multi_location_ids(ENTRIES)


def _expect(e):
    case = e["_case"]
    if case == "named: feed placeholder at Westover Hills":
        return "CHRISTUS Santa Rosa Hospital - Westover Hills"
    return case.split(": ", 1)[1] if case.startswith("named: ") and case.split(": ", 1)[1] in C.NAMES else None


def _named(e):
    got = C.feed_site(e["bu"], e["locationList"], e["title"], str(e["jobId"]) in MULTI)
    return got[0] if got else None


# ── The real payload ────────────────────────────────────────────────────────

@pytest.mark.parametrize("e", [x for x in ENTRIES if x["_case"].startswith("named: ")
                               and x["_case"].split(": ", 1)[1] in C.NAMES], ids=lambda e: e["url"][-40:])
def test_feed_rows_at_a_hospital_street_take_the_hospital(e):
    assert _named(e) == _expect(e)


@pytest.mark.parametrize("e", [x for x in ENTRIES if x["_case"].startswith("kept: ")], ids=lambda e: e["_case"])
def test_feed_rows_that_keep_their_unit(e):
    assert _named(e) is None


def test_every_table_site_has_a_real_posting():
    named = {_expect(e) for e in ENTRIES if _expect(e)}
    assert named == set(C.NAMES)


def test_feed_placeholder_at_a_hospital_street():
    e = next(x for x in ENTRIES if x["_case"] == "named: feed placeholder at Westover Hills")
    assert e["bu"] == "N/A"
    assert C.feed_site(e["bu"], e["locationList"], e["title"]) == (
        "CHRISTUS Santa Rosa Hospital - Westover Hills", "San Antonio", "TX")


def test_the_town_is_the_streets():
    for e in ENTRIES:
        got = C.feed_site(e["bu"], e["locationList"], e["title"], str(e["jobId"]) in MULTI)
        if got:
            street, city, st = C.parse_address(e["locationList"])
            assert (got[1], got[2]) == (city, st)


# ── Streets ─────────────────────────────────────────────────────────────────

def test_parse_address_reads_the_feed_shape():
    assert C.parse_address("600 Elizabeth Street, Corpus Christi, TX\u00a0\u00a078404") == (
        "600 Elizabeth Street", "Corpus Christi", "TX")
    assert C.parse_address("105-I Medical Plaza, Sulphur Springs, TX  75482") == (
        "105-I Medical Plaza", "Sulphur Springs", "TX")
    for bad in ("N/A", "", None, "TX, Tyler", "Tyler, TX"):
        assert C.parse_address(bad) is None


@pytest.mark.parametrize("a,b", [
    ("1453 E Bert Kouns Industrial Loop", "1453 East Bert Kouns Industrial Loop"),
    ("455 St Michael's Drive", "455 St Michaels Dr."),
    ("1311 E General Cavazos Blvd", "1311 East General Cavazos Boulevard"),
    ("524 Dr Michael DeBakey Drive", "524 Dr. Michael Debakey Dr"),
])
def test_street_spellings_read_alike(a, b):
    assert C.street_key(a) == C.street_key(b)


def test_a_neighbouring_street_number_is_not_the_hospital():
    assert C.site_at("2600 St Michael Drive, Texarkana, TX  75503").name == "CHRISTUS St. Michael Hospital - Texarkana"
    for other in ("2604 St Michael Drive, Texarkana, TX  75503", "2400 St Michael Drive, Texarkana, TX  75503",
                  "445 St Michaels Drive, Santa Fe, NM  87505", "5802 Saratoga Boulevard, Corpus Christi, TX  78414",
                  "600 Elizabeth Street, Corpus Christi, NM  78404"):
        assert C.site_at(other) is None


def test_the_table_is_consistent():
    keys = {}
    for s in C.SITES:
        assert s.streets and s.source
        for st in s.streets:
            k = C.address_key(st, s.city, s.state)
            assert keys.setdefault(k, s.name) == s.name
        # a name the site prints as a site (it says what it is) or the CMS name
        assert "Hospital" in s.name or "Medical Center" in s.name or "Health Care Center" in s.name \
            or s.name == "CHRISTUS Southeast Texas St. Elizabeth" or s.name == "CHRISTUS Southern New Mexico"
    assert len(C.NAMES) == len(C.SITES)


# ── Who keeps the unit ──────────────────────────────────────────────────────

def test_only_the_regional_units_take_a_hospital():
    street = "800 East Dawson Street, Tyler, TX  75701"
    for bu in ("CHRISTUS Trinity Mother Frances Health System", "CHRISTUS Health", "N/A", "", "CHRISTUS Spohn Health System"):
        assert C.feed_site(bu, street, "Registered Nurse - ICU")[0] == "CHRISTUS Mother Frances Hospital - Tyler"
    for bu in ("CHRISTUS Ministry System Office", "CHRISTUS Children\u2019s", "CHRISTUS Ambulatory Centers",
               "CHRISTUS HomeCare", "CHRISTUS Trinity Clinic"):
        assert C.feed_site(bu, street, "Registered Nurse - ICU") is None


def test_the_clinic_group_only_where_its_own_page_named_the_hospital():
    assert C.feed_site("CHRISTUS Trinity Clinic", "703 South Fleishel Avenue, Tyler, TX  75701",
                       "Registered Nurse Clinic - Cardiology")[0] == "CHRISTUS Louis and Peaches Owen Heart Hospital"
    assert C.feed_site("CHRISTUS Trinity Clinic", "1311 E General Cavazos Boulevard, Kingsville, TX  78363",
                       "Physician - General Surgery") is None


@pytest.mark.parametrize("title,why", [
    ("Regional Director Supply Chain - Materials Management Logistics", "regional or system role"),
    ("Radiology Technologist - Regional Outpt Tyler FT", "regional or system role"),
    ("System Director, Managed Care", "regional or system role"),
    ("Chief Information Officer", "regional or system role"),
    ("LVN, Licensed Vocational Nurse - Remote Patient Monitoring", "remote"),
    ("Registered Nurse - All Locations", "multi-site"),
    ("Application System Analyst II - Cadence", "system office function"),
    ("Specialty Coder Senior - Multi Specialty", "system office function"),
    ("CHRISTUS Health Regional Compliance & Privacy Director", "regional or system role"),
])
def test_titles_that_keep_the_unit(title, why):
    assert C.keeps_label(title) == why
    assert C.feed_site("CHRISTUS Spohn Health System", "600 Elizabeth Street, Corpus Christi, TX  78404", title) is None


@pytest.mark.parametrize("title", [
    "Registered Nurse Float II - Nursing Float Pool",           # the hospital's own float pool
    "CHRISTUS DOD SkillBridge Program - Spohn Hospital Corpus Christi Shoreline",
    "RN, Registered Nurse - 6N Cardiovascular IMC",
    "Clinical Nurse - Nursing Float Pool",
    "Registered Nurse - Emergency Department",
    "Advanced Practice Provider - Hospitalist",
])
def test_hospital_titles_take_the_hospital(title):
    assert C.keeps_label(title) == ""
    assert C.feed_site("CHRISTUS Spohn Health System", "600 Elizabeth Street, Corpus Christi, TX  78404",
                       title)[0] == "CHRISTUS Spohn Hospital Corpus Christi - Shoreline"


def test_a_posting_at_several_streets_keeps_the_unit():
    assert {"377892", "209272"} <= MULTI
    for e in ENTRIES:
        if str(e["jobId"]) in ("377892", "209272"):
            assert _named(e) is None
    one = [{"jobId": "1", "locationList": "600 Elizabeth Street, Corpus Christi, TX 78404"},
           {"jobId": "1", "locationList": "600 Elizabeth St., Corpus Christi, TX 78404"},     # one street, twice
           {"jobId": "2", "locationList": "600 Elizabeth Street, Corpus Christi, TX 78404"},
           {"jobId": "2", "locationList": "4200 Nelson Road, Lake Charles, LA 70605"},
           "not a dict", None, {"jobId": None}]
    assert C.multi_location_ids(one) == frozenset({"2"})


def test_feed_site_never_needs_more_than_strings():
    assert C.feed_site(None, None, None) is None
    assert C.feed_site("CHRISTUS Spohn Health System", 12345, None) is None


# ── The stored rows (the change list) decide as the nightly does ────────────

def test_stored_change_matches_the_nightly():
    for e in ENTRIES:
        multi = str(e["jobId"]) in MULTI
        row = {"hospital_system": "CHRISTUS Health", "hospital_name": e["bu"], "title": e["title"]}
        got = C.stored_change(row, e["locationList"], multi)
        want = C.feed_site(e["bu"], e["locationList"], e["title"], multi)
        if want is None or want[0] == e["bu"]:
            assert got is None
        else:
            assert got[:3] == want and "feed street" in got[3]
    assert C.stored_change({"hospital_system": "Other", "hospital_name": "CHRISTUS Health", "title": "RN"},
                           "600 Elizabeth Street, Corpus Christi, TX 78404") is None


# ── The scraper: the hook, its guard, and finalize_jobs keeps the name ──────

def test_adapter_calls_the_hook_through_fac_hook():
    src = open(scraper.__file__, encoding="utf-8").read()
    i = src.index("async def run_playwright_scrapers(")
    j = src.index("async def _playwright_detail_passes(", i)
    body = src[i:j]
    assert '_fac_hook("CHRISTUS street addresses", facilities_christus.feed_site' in body
    assert '_fac_hook("CHRISTUS multi-location postings",' in body
    assert body.index("_cx_multi = _fac_hook(") < body.index("for j in captured:")
    assert body.index('facilities_christus.feed_site') < body.index("jobs.append(Job(")


def test_a_raising_hook_leaves_the_unit(monkeypatch):
    def boom(*a, **k):
        raise ValueError("unforeseen row")
    assert scraper._fac_hook("CHRISTUS street addresses", boom, "x", "y", "z", False, default=None) is None


def test_finalize_jobs_keeps_the_hospital_and_the_placeholder_rule():
    def job(name, jid, city="Corpus Christi"):
        return scraper.Job(title="Registered Nurse - ICU", hospital_system="CHRISTUS Health", hospital_name=name,
                           city=city, state="TX", location=f"TX, {city}", specialty="", job_type="",
                           url=f"https://careers.christushealth.org/opportunity/rn-{jid}", job_id=jid, posted_date="",
                           description="", ats_platform="Custom")
    rows = scraper.finalize_jobs([job("CHRISTUS Spohn Hospital Corpus Christi - Shoreline", "C1"),
                                  job("N/A", "C2"), job("CHRISTUS Spohn Health System", "C3")])
    got = {r["job_id"]: (r["hospital_name"], r["city"], r["state"]) for r in rows}
    assert got == {"C1": ("CHRISTUS Spohn Hospital Corpus Christi - Shoreline", "Corpus Christi", "TX"),
                   "C2": ("CHRISTUS Health", "Corpus Christi", "TX"),
                   "C3": ("CHRISTUS Spohn Health System", "Corpus Christi", "TX")}
    assert V.is_placeholder("N/A")


class _Resp:
    def __init__(self, url, data):
        self.url = url
        self.headers = {"content-type": "application/json"}
        self._data = data

    async def json(self):
        return self._data


class _Page:
    def __init__(self, data):
        self._data = data
        self._cb = None

    def on(self, event, cb):
        self._cb = cb

    async def goto(self, url, **kw):
        await self._cb(_Resp("https://careers.christushealth.org/job-search/jobs", self._data))

    async def evaluate(self, *a, **k):
        return None

    async def wait_for_selector(self, *a, **k):
        return None

    async def query_selector(self, *a, **k):
        return None

    async def query_selector_all(self, *a, **k):
        return []


class _Ctx:
    def __init__(self, data):
        self._data = data

    async def add_init_script(self, *a, **k):
        return None

    async def new_page(self):
        return _Page(self._data)

    async def close(self):
        return None


class _Browser:
    def __init__(self, data):
        self._data = data

    async def new_context(self, **kw):
        return _Ctx(self._data)

    async def close(self):
        return None


def test_playwright_route_names_hospitals_from_the_real_feed(monkeypatch):
    """run_playwright_scrapers over the fixture's entries as the feed's one
    JSON response (the shape /job-search/jobs serves): every expectation holds
    on the Jobs the adapter builds."""
    payload = {"query": "", "jobs": [{k: v for k, v in e.items() if k != "_case"} for e in ENTRIES],
               "count": len(ENTRIES)}

    class _Chromium:
        async def launch(self, **kw):
            return _Browser(payload)

    class _PW:
        chromium = _Chromium()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    fake = types.ModuleType("playwright.async_api")
    fake.async_playwright = lambda: _PW()
    monkeypatch.setitem(sys.modules, "playwright", types.ModuleType("playwright"))
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake)
    monkeypatch.setattr(scraper, "PLAYWRIGHT_SITES", [("CHRISTUS Health", "https://careers.christushealth.org/job-search")])

    async def no_sleep(*a, **k):
        return None

    async def no_detail(jobs):
        return None
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(scraper, "_playwright_detail_passes", no_detail)
    C.STATS.clear()
    jobs = asyncio.run(scraper.run_playwright_scrapers())
    assert len(jobs) == len(ENTRIES)
    by_url = {j.url: j for j in jobs}
    for e in ENTRIES:
        j = by_url[e["url"]]
        want = _expect(e)
        if want:
            site = next(s for s in C.SITES if s.name == want)
            assert (j.hospital_name, j.city, j.state) == (want, site.city, site.state), e["_case"]
        else:
            assert j.hospital_name == (e["bu"] or "CHRISTUS Health"), e["_case"]
        assert j.hospital_system == "CHRISTUS Health" and j.job_id == str(e["jobId"])
    assert not C.STATS      # logged and cleared after the tenant
