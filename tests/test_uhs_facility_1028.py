"""UHS facility names (2026-10-28, owner: "yes show facility names on UHS cards").

jobs.uhsinc.com's /api/jobs names each posting's facility in tags4; a UHS
row's hospital_name is that facility (uhs_facilities.facility), else the
stored name, else the one facility the posting's opening names, else the
system. hospital_system never changes. No network, no database."""
import asyncio
import json

import pytest

import scraper
import uhs_facilities as U

UHS = "Universal Health Services"


# ── uhs_facilities.facility ──────────────────────────────────────────────────
@pytest.mark.parametrize("tag, want", [
    (["Manatee Memorial Hospital"], "Manatee Memorial Hospital"),
    ("Millwood Hospital", "Millwood Hospital"),
    (["STHS McAllen"], "South Texas Health System McAllen"),
    (["STHS Edinburg"], "South Texas Health System Edinburg"),
    (["STHS Heart"], "South Texas Health System Heart"),
    (["Doctors Hospital Of Laredo"], "Doctors Hospital of Laredo"),     # case variant: the board's majority spelling
    (["Prairie St Johns"], "Prairie St. John's"),
    (["Hri Hospital"], "HRI Hospital"),
    (["St Marys Regional Medical Center"], "St. Mary's Regional Medical Center"),
    (["South Texas Health System - Clinics"], "South Texas Health System Clinics"),
    (["NNHS - ER at Spanish Springs"], "Northern Nevada Medical Center"),
    (["The Northern Nevada Health System"], "Northern Nevada Health System"),
    (["Valley Health System Consolidated Services"], "Valley Health System"),
    (["Southwest Healthcare Rancho Springs Hospital"], "Rancho Springs Medical Center"),
    (["The George Washington University Hospital"], "The George Washington University Hospital"),
    (["The Brook Hospital - Dupont"], "The Brook Hospital - Dupont"),
    # a facility the table does not know yet is still a facility, as written
    (["Brand New Behavioral Hospital"], "Brand New Behavioral Hospital"),
    (["NEW RIVER BEHAVIORAL HOSPITAL"], "New River Behavioral Hospital"),
])
def test_a_tag_names_one_clean_facility(tag, want):
    assert U.facility(tag) == want


@pytest.mark.parametrize("tag", [
    ["UHS of Delaware, Inc. - Corporate Office"], ["UHS of Delaware, Inc. - Corporate Office - Remote"],
    ["Atlantic Region CBO"], ["Western CBO"], ["Georgia Business Center"], ["The National Call Center"],
    ["Behavioral Health Shared Services Group"], ["N/A"], ["Universal Health Services"], ["UHS"],
    [], [""], None, "", ["The Brook Dupont And Kmi"],
])
def test_an_office_or_a_blank_names_no_facility(tag):
    assert U.facility(tag) == ""


def test_is_facility_reads_a_stored_name():
    assert U.is_facility("Manatee Memorial Hospital")
    assert not U.is_facility("Universal Health Services")
    assert not U.is_facility("")
    assert not U.is_facility("Western CBO")


# ── uhs_facilities.text_facility ─────────────────────────────────────────────
def test_the_posting_opening_names_its_facility():
    millwood = ("Responsibilities Who We Are: Millwood Hospital (a UHS Facility) is a 134-bed mental health "
                "facility that provides inpatient and outpatient mental health ... Universal Health Services, Inc.")
    assert U.text_facility(millwood) == "Millwood Hospital"
    assert U.text_facility("<p>About <b>Manatee Memorial Hospital</b> (MMH): Manatee Memorial Hospital in "
                           "Bradenton, Florida ...</p>") == "Manatee Memorial Hospital"
    # how postings write it: "Summerlin Hospital is a 496-bed hospital"
    assert U.text_facility("Summerlin Hospital is a 496-bed hospital featuring emergency services") == \
        "Summerlin Hospital Medical Center"
    # the longest name wins: never "Valley Hospital" inside "Spring Valley Hospital Medical Center"
    assert U.text_facility("Spring Valley Hospital Medical Center is hiring") == "Spring Valley Hospital Medical Center"


def test_text_never_guesses():
    # two facilities: none (strict)
    assert U.text_facility("RESOURCE RN POOL FOR HARTGROVE BEHAVIORAL HEALTH SYSTEM AND GARFIELD PARK "
                           "BEHAVIORAL HOSPITAL") == ""
    assert U.text_facility("RESOURCE RN POOL FOR HARTGROVE BEHAVIORAL HEALTH SYSTEM AND GARFIELD PARK "
                           "BEHAVIORAL HOSPITAL", strict=False) == "Hartgrove Behavioral Health System"
    # a market's name is never taken from text ("a member of The Valley Health System")
    assert U.text_facility("The Valley Health System has expanded into an integrated health network") == ""
    assert U.text_facility("Headquartered in King of Prussia, PA, Universal Health Services, Inc.") == ""
    assert U.text_facility("") == "" and U.text_facility(None) == ""
    # past the opening, a posting names its sister facilities: not read
    assert U.text_facility("x " * 400 + "Millwood Hospital") == ""


# ── scraper.uhs_jibe_facility ────────────────────────────────────────────────
def test_tag_then_stored_then_text_then_system():
    f = scraper.uhs_jibe_facility
    scraper.set_stored_uhs_names([
        {"hospital_system": UHS, "job_id": "370001", "hospital_name": "Kingwood Pines Hospital"},
        {"hospital_system": UHS, "job_id": "370002", "hospital_name": "Universal Health Services"},
        {"hospital_system": "Acadia Healthcare", "job_id": "370003", "hospital_name": "Rolling Hills Hospital"},
    ])
    try:
        assert scraper._STORED_UHS == {"370001": "Kingwood Pines Hospital"}
        # 1. the tag wins over everything
        assert f({"req_id": "370001", "tags4": ["STHS McAllen"]}) == ("South Texas Health System McAllen", "tag")
        # an office tag keeps the system, even with a stored facility or a facility in the text
        assert f({"req_id": "370001", "tags4": ["Western CBO"],
                  "description": "Millwood Hospital (a UHS Facility)"}) == (UHS, "office")
        # 2. no tag: the stored facility
        assert f({"req_id": "370001", "tags4": [], "description": "Millwood Hospital (a UHS Facility)"}) == (
            "Kingwood Pines Hospital", "stored")
        # 3. no tag, nothing stored: the posting's opening
        assert f({"req_id": "370002", "description": "<p>Who We Are: Millwood Hospital (a UHS Facility)</p>"}) == (
            "Millwood Hospital", "text")
        # 4. the system
        assert f({"req_id": "370004", "description": "<p>RN.</p>"}) == (UHS, "system")
        assert f(None) == (UHS, "system")
        # _jibe_facility goes through it for UHS only
        assert scraper._jibe_facility(UHS, {"tags4": ["Millwood Hospital"]}) == "Millwood Hospital"
        assert scraper._jibe_facility("Yale New Haven Health System", {"tags4": ["Millwood Hospital"],
                                                                       "location_name": "CT06510"}) == \
            "Yale New Haven Health System"
    finally:
        scraper.set_stored_uhs_names([])


def test_stored_uhs_names_load_nothing_without_credentials(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert scraper.load_stored_uhs_names() == 0
    assert scraper._STORED_UHS == {}


# ── scrape_jibe on a UHS-shaped board ────────────────────────────────────────
class _R:
    def __init__(self, body=None, status=200):
        self.status, self._body = status, body

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


def _row(rid, tag, desc="<p>RN.</p>", city="Bradenton", state="Florida"):
    d = {"req_id": str(rid), "slug": str(rid), "title": "Registered Nurse", "city": city, "state": state,
         "description": desc, "meta_data": {"canonical_url": f"https://jobs.uhsinc.com/jobs/{rid}"}}
    if tag is not None:
        d["tags4"] = tag
    return {"data": d}


def test_scrape_jibe_stores_the_uhs_facility_and_keeps_the_system(monkeypatch, caplog):
    rows = [
        _row(1, ["Manatee Memorial Hospital"]),
        _row(2, ["STHS Edinburg"], city="EDINBURG", state="Texas"),
        _row(3, ["UHS of Delaware, Inc. - Corporate Office"], city="King of Prussia", state="Pennsylvania"),
        _row(4, [], desc="<p>Who We Are: Millwood Hospital (a UHS Facility) is a 134-bed ...</p>",
             city="Arlington", state="Texas"),
        _row(5, None),
    ]

    def fake_req(session, method, url, **kw):
        page = int((kw.get("params") or {}).get("page", "1"))
        return _Ctx(_R({"jobs": rows if page == 1 else [], "totalCount": len(rows)}))

    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    scraper.set_stored_uhs_names([])
    with caplog.at_level("INFO"):
        jobs = asyncio.run(scraper.scrape_jibe(None, UHS, "https://jobs.uhsinc.com"))
    by = {j.job_id: j for j in jobs}
    assert {j.hospital_system for j in jobs} == {UHS}
    assert by["1"].hospital_name == "Manatee Memorial Hospital"
    assert by["2"].hospital_name == "South Texas Health System Edinburg"
    assert by["3"].hospital_name == UHS
    assert by["4"].hospital_name == "Millwood Hospital"
    assert by["5"].hospital_name == UHS
    assert "facility by office 1, system 1, tag 2, text 1" in caplog.text


def test_other_jibe_systems_are_untouched():
    assert scraper._jibe_facility("Care New England Health System", {"tags2": ["Kent Hospital"]}) == "Kent Hospital"
    assert scraper._jibe_facility("Garnet Health", {"location_name": "Garnet Health Medical Center"}) == \
        "Garnet Health Medical Center"
    # UHS's tag is registered as the others' are (tags4, the Facility filter); its
    # value goes through uhs_jibe_facility, never the generic first-entry rule
    assert scraper.JIBE_FACILITY_TAG[UHS] == U.FACILITY_TAG == "tags4"
    assert UHS not in scraper.JIBE_FACILITY_NAME
    assert scraper._jibe_facility(UHS, {"tags4": ["STHS McAllen"]}) == "South Texas Health System McAllen"
    assert scraper._jibe_facility(UHS, {"tags4": ["Western CBO"]}) == UHS


def test_the_table_has_one_spelling_per_facility():
    keys = [U.fkey(n) for n in U.CANONICAL]
    assert len(keys) == len(set(keys))
    # every rename lands on a canonical name (or none)
    assert all(v == "" or U.fkey(v) in set(keys) for v in U.RENAMES.values())
    assert len(U._RENAME_BY_KEY) == len(U.RENAMES)
    assert all(U.fkey(v) in set(keys) for v in U.TEXT_ALIASES.values())
    assert json.dumps(U.CANONICAL).isascii()
