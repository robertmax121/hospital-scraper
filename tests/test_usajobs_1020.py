"""USAJOBS coverage (2026-10-20, "that's it for the VA jobs?"). No network,
no database: the search API needs a key we do not hold locally, so the
payloads are built here in the shape of developer.usajobs.gov's
api-reference/get-api-search (SearchResult.SearchResultItems[].
MatchedObjectDescriptor.PositionLocation[]: LocationName, CountryCode,
CountrySubDivisionCode, CityName) and the public code lists
(data.usajobs.gov/api/codelist/agencysubelements, /occupationalseries,
/countrysubdivisions), fetched 2026-10-20.

1. Org codes: Indian Health Service is HE37 and NIH HE38 (the adapter read
   HE38 for IHS and HE06, no such code, for NIH).
2. Series: every series of the 0600 group plus psychology, social work and
   (VA only) the 0101 peer support specialists, searched in blocks of 15.
3. Locations: one row per US town an announcement lists (states, DC, PR,
   GU, VI), the first town keeping the PositionID, the others PositionID +
   town slug; remote / negotiable entries are no town; foreign entries are
   no row; at most USAJOBS_LOCATION_CAP towns.
4. The same-requisition rule (S1) keeps each town and merges a true
   same-town repeat.
"""
import asyncio
import dataclasses
import logging

import pytest

import scraper


# ── 1. Org codes ─────────────────────────────────────────────────────────────

def test_usajobs_org_codes_are_the_agency_subelement_codes():
    assert dict(scraper.USAJOBS_ORGS) == {
        "VA Hospitals": "VATA",              # Veterans Health Administration
        "Indian Health Service": "HE37",     # Indian Health Service
        "Military Health System": "DD",      # Department of Defense
        "NIH Clinical Center": "HE38",       # National Institutes of Health
    }


# ── 2. Series ────────────────────────────────────────────────────────────────

# Every series of job family 0600 in data.usajobs.gov/api/codelist/
# occupationalseries on 2026-10-20 (none disabled; "0600" itself is not a
# series in the list).
CODELIST_0600_GROUP = (
    "0601 0602 0603 0605 0610 0620 0621 0622 0625 0630 0631 0633 0635 0636 0637 0638 0639 0640 "
    "0642 0644 0645 0646 0647 0648 0649 0650 0651 0660 0661 0662 0665 0667 0668 0669 0670 0671 "
    "0672 0673 0675 0679 0680 0681 0682 0683 0685 0688 0690 0696 0698 0699").split()
OLD_SERIES = "0600;0601;0602;0610;0620;0630;0640;0645;0646;0647;0648;0649;0660;0670;0675".split(";")


def test_usajobs_series_cover_the_0600_group_and_the_hospital_series_outside_it():
    series = scraper.USAJOBS_SERIES
    assert len(series) == len(set(series)) and all(len(c) == 4 and c.isdigit() for c in series)
    assert set(CODELIST_0600_GROUP) <= set(series)
    assert set(OLD_SERIES) <= set(series)                     # nothing the 10-20 run read is lost
    assert {"0180", "0185", "0101"} <= set(series)            # psychology, social work, peer support


def test_usajobs_series_queries_split_into_blocks_and_keep_0101_to_the_va():
    for org in ("VATA", "HE37", "DD", "HE38"):
        queries = scraper.usajobs_series_queries(org)
        codes = [c for q in queries for c in q.split(";")]
        assert all(len(q.split(";")) <= scraper.USAJOBS_SERIES_PER_SEARCH for q in queries)
        assert len(codes) == len(set(codes))
        want = set(scraper.USAJOBS_SERIES) - ({"0101"} if org != "VATA" else set())
        assert set(codes) == want
    assert len(scraper.usajobs_series_queries("VATA")) == 4   # 56 codes, 15 a search


# ── 3. Locations ─────────────────────────────────────────────────────────────

def _loc(city, state, country="United States", name=None):
    cn = f"{city}, {state}" if state else city
    return {"LocationName": name or cn, "CountryCode": country, "CountrySubDivisionCode": state,
            "CityName": name or cn, "Longitude": -95.36, "Latitude": 29.76}


ANYWHERE = {"LocationName": "Anywhere in the U.S. (remote job)", "CountryCode": "United States",
            "CountrySubDivisionCode": "", "CityName": "Anywhere in the U.S. (remote job)"}
LANDSTUHL = {"LocationName": "Landstuhl, Germany", "CountryCode": "Germany",
             "CountrySubDivisionCode": "Rheinland-Pfalz", "CityName": "Landstuhl, Germany"}


def _item(pid, title, locs, sub="Michael E. DeBakey VA Medical Center", lo=87457, hi=153766,
          control="889100100", series=("Nurse", "0610"), org=None):
    return {"MatchedObjectId": control, "MatchedObjectDescriptor": {
        "PositionID": pid, "PositionTitle": title,
        "PositionURI": f"https://www.usajobs.gov:443/job/{control}",
        "PositionLocationDisplay": "Multiple Locations" if len(locs) > 1 else (locs[0]["CityName"] if locs else ""),
        "PositionLocation": locs,
        "OrganizationName": org or ("Military Treatment Facilities under DHA" if "DHA" in sub
                                    else "Veterans Health Administration"),
        "DepartmentName": "Department of Defense" if "DHA" in sub else "Department of Veterans Affairs",
        "JobCategory": [{"Name": series[0], "Code": series[1]}],
        "PositionSchedule": [{"Name": "Full-time", "Code": "1"}],
        "QualificationSummary": "Basic requirements: graduate of a school of professional nursing.",
        "PositionRemuneration": [{"MinimumRange": str(lo), "MaximumRange": str(hi), "RateIntervalCode": "PA",
                                  "Description": "Per Year"}],
        "PublicationStartDate": "2026-10-16T00:00:00.0000",
        "UserArea": {"Details": {"JobSummary": "", "SubAgencyName": sub, "LowGrade": "01", "HighGrade": "03"}},
    }}


HOUSTON_3 = _item("CBTH-13094001-27-RS", "Registered Nurse - Primary Care (PACT)",
                  [_loc("Houston", "Texas"), _loc("Conroe", "Texas"), _loc("Lake Jackson", "Texas")])


def test_single_location_keeps_the_position_id_as_before():
    item = _item("CBTF-13083981-27-AP", "Diagnostic Radiologic Technologist", [_loc("Baltimore", "Maryland")],
                 sub="Baltimore VA Medical Center")
    rows = scraper._usajobs_jobs(item, "VA Hospitals")
    assert len(rows) == 1
    j = rows[0]
    assert (j.job_id, j.req_id, j.city, j.state, j.location) == (
        "CBTF-13083981-27-AP", "", "Baltimore", "MD", "Baltimore, MD")
    # 2026-10-27: a medical center with a name of its own keeps it
    # (va_facilities.HOSPITALS); the VA Maryland Health Care System has three
    assert j.hospital_name == "Baltimore VA Medical Center"
    first = scraper._usajobs_job(item, "VA Hospitals")
    assert dataclasses.replace(first, scraped_at=j.scraped_at) == j


def test_three_locations_are_three_rows_with_stable_ids():
    rows = scraper._usajobs_jobs(HOUSTON_3, "VA Hospitals")
    assert [(j.job_id, j.city, j.state) for j in rows] == [
        ("CBTH-13094001-27-RS", "Houston", "TX"),
        ("CBTH-13094001-27-RS-conroe-tx", "Conroe", "TX"),
        ("CBTH-13094001-27-RS-lake-jackson-tx", "Lake Jackson", "TX"),
    ]
    # the further towns carry the announcement number as their requisition
    assert [j.req_id for j in rows] == ["", "CBTH-13094001-27-RS", "CBTH-13094001-27-RS"]
    # pay, title, url, dates, description and facility are the announcement's
    for field in ("title", "url", "posted_date", "description", "wage_min", "wage_max", "wage_unit",
                  "hospital_name", "specialty", "job_type", "ats_platform", "hospital_system"):
        assert len({getattr(j, field) for j in rows}) == 1, field
    assert (rows[0].wage_min, rows[0].wage_max, rows[0].wage_unit) == (41.91, 73.68, "hour")
    assert rows[0].hospital_name == "Michael E. DeBakey VA Medical Center"


def test_ids_are_the_same_every_night_and_do_not_depend_on_the_other_towns():
    again = scraper._usajobs_jobs(HOUSTON_3, "VA Hospitals")
    assert [j.job_id for j in again] == [j.job_id for j in scraper._usajobs_jobs(HOUSTON_3, "VA Hospitals")]
    d = HOUSTON_3["MatchedObjectDescriptor"]
    reordered = _item(d["PositionID"], d["PositionTitle"],
                      [_loc("Houston", "Texas"), _loc("Lake Jackson", "Texas"), _loc("Beaumont", "Texas"),
                       _loc("Conroe", "Texas")])
    ids = {j.city: j.job_id for j in scraper._usajobs_jobs(reordered, "VA Hospitals")}
    assert ids["Conroe"] == "CBTH-13094001-27-RS-conroe-tx"
    assert ids["Lake Jackson"] == "CBTH-13094001-27-RS-lake-jackson-tx"
    assert ids["Houston"] == "CBTH-13094001-27-RS"


def test_anywhere_in_the_us_alone_is_one_row_with_no_town_as_before():
    item = _item("CBSW-13089255-27-YW", "Physician - Diagnostic Tele-Radiologist", [ANYWHERE],
                 sub="Veterans Health Administration")
    rows = scraper._usajobs_jobs(item, "VA Hospitals")
    assert [(j.job_id, j.city, j.state, j.location) for j in rows] == [("CBSW-13089255-27-YW", "", "", "")]


def test_anywhere_beside_named_towns_is_skipped():
    item = _item("CBTE-13090000-27-JF", "Physician (Radiology)",
                 [ANYWHERE, _loc("Washington", "District of Columbia"), _loc("Baltimore", "Maryland")])
    rows = scraper._usajobs_jobs(item, "VA Hospitals")
    assert [(j.job_id, j.city, j.state) for j in rows] == [
        ("CBTE-13090000-27-JF", "Washington", "DC"),
        ("CBTE-13090000-27-JF-baltimore-md", "Baltimore", "MD"),
    ]


def test_a_hundred_locations_are_cut_to_the_cap_and_counted():
    states = sorted(scraper._STATE_CODE_BY_NAME.items())               # 51 (name, code) incl. DC
    towns = [(t, name) for name, _ in states for t in ("Springfield", "Franklin")][:100]
    item = _item("HSJT-27-13095000-DHA", "Medical Support Assistant (OA)",
                 [_loc(t, name.title()) for t, name in towns], sub="Military Treatment Facilities under DHA",
                 lo=40332, hi=52435, series=("Medical Support Assistance", "0679"))
    stats = {}
    rows = scraper._usajobs_jobs(item, "Military Health System", stats)
    assert len(towns) == 100
    assert len(rows) == scraper.USAJOBS_LOCATION_CAP == 60
    assert stats == {"capped": 1}
    assert rows[0].job_id == "HSJT-27-13095000-DHA"
    assert len({j.job_id for j in rows}) == 60
    assert all(j.job_id.startswith("HSJT-27-13095000-DHA-") for j in rows[1:])


def test_puerto_rico_and_guam_are_us_towns():
    item = _item("CDAA-13092000-27-MR", "Staff Nurse", [_loc("San Juan", "Puerto Rico"), _loc("Ponce", "Puerto Rico"),
                                                        _loc("Mayaguez", "Puerto Rico")],
                 sub="VA Caribbean Healthcare System")
    rows = scraper._usajobs_jobs(item, "VA Hospitals")
    assert [(j.job_id, j.city, j.state) for j in rows] == [
        ("CDAA-13092000-27-MR", "San Juan", "PR"),
        ("CDAA-13092000-27-MR-ponce-pr", "Ponce", "PR"),
        ("CDAA-13092000-27-MR-mayaguez-pr", "Mayaguez", "PR"),
    ]
    assert all(scraper.non_us_reason(j.location, j.city, j.state, j.url) is None for j in rows)
    guam = _item("HSJG-27-13093000-DHA", "Practical Nurse",
                 [{"LocationName": "Hagatna, Guam", "CountryCode": "Guam", "CountrySubDivisionCode": "",
                   "CityName": "Hagatna, Guam"}], sub="Military Treatment Facilities under DHA")
    assert [(j.city, j.state) for j in scraper._usajobs_jobs(guam, "Military Health System")] == [("Hagatna", "GU")]


def test_foreign_locations_are_no_row():
    stats = {}
    only = _item("HSJH-27-13091018-DHA", "Supervisory Physician (Emergency Medicine)", [LANDSTUHL],
                 sub="Military Treatment Facilities under DHA")
    assert scraper._usajobs_jobs(only, "Military Health System", stats) == []
    assert scraper._usajobs_job(only, "Military Health System") is None
    assert stats == {"foreign": 1}
    mixed = _item("HSJH-27-13091019-DHA", "Practical Nurse (Family Medicine)",
                  [LANDSTUHL, _loc("Bethesda", "Maryland")], sub="Military Treatment Facilities under DHA")
    rows = scraper._usajobs_jobs(mixed, "Military Health System")
    assert [(j.job_id, j.city, j.state) for j in rows] == [("HSJH-27-13091019-DHA", "Bethesda", "MD")]


def test_military_post_codes_are_not_states():
    assert scraper._usajobs_state("AP") == ""
    assert scraper._usajobs_state("AE") == ""
    assert scraper._usajobs_state("Armed Forces Pacific") == ""
    assert scraper._usajobs_state("GU") == "GU" and scraper._usajobs_state("dc") == "DC"


def test_one_town_listed_twice_is_one_row():
    item = _item("CBTH-13094002-27-RS", "Registered Nurse - Emergency Department",
                 [_loc("Houston", "Texas", name="Michael E. DeBakey VA Medical Center, Houston, Texas"),
                  _loc("Houston", "Texas"), _loc("Conroe", "Texas")], sub="Veterans Health Administration")
    rows = scraper._usajobs_jobs(item, "VA Hospitals")
    assert [(j.job_id, j.city) for j in rows] == [("CBTH-13094002-27-RS", "Houston"),
                                                  ("CBTH-13094002-27-RS-conroe-tx", "Conroe")]


def test_a_facility_named_ahead_of_the_town_names_that_rows_hospital():
    item = _item("HSJT-26-13073023-DHA", "Medical Instrument Technician (Polysomnography)",
                 [_loc("Jacksonville", "Florida", name="Naval Hospital Jacksonville, Jacksonville, Florida"),
                  _loc("Kings Bay", "Georgia"),
                  _loc("San Diego", "California", name="Point Loma Complex, San Diego, California")],
                 sub="Military Treatment Facilities under DHA")
    rows = scraper._usajobs_jobs(item, "Military Health System")
    assert [(j.city, j.state, j.hospital_name) for j in rows] == [
        ("Jacksonville", "FL", "Naval Hospital Jacksonville"),
        # 2026-10-30 (round 2, mhs): a row whose posting names no military treatment
        # facility of its place shows the system, not the agency label (facilities_mhs)
        ("Kings Bay", "GA", "Military Health System"),
        ("San Diego", "CA", "Military Health System"),       # a complex is not a hospital
    ]
    assert rows[1].job_id == "HSJT-26-13073023-DHA-kings-bay-ga"


def test_a_building_written_as_the_town_gives_way_to_a_named_town_of_its_state():
    item = _item("HSJT-26-13073024-DHA", "Practical Nurse",
                 [{"LocationName": "Naval Hospital Jacksonville, Florida", "CountryCode": "United States",
                   "CountrySubDivisionCode": "Florida", "CityName": "Naval Hospital Jacksonville, Florida"},
                  _loc("Jacksonville", "Florida")], sub="Military Treatment Facilities under DHA")
    rows = scraper._usajobs_jobs(item, "Military Health System")
    assert [(j.job_id, j.city, j.state) for j in rows] == [("HSJT-26-13073024-DHA", "Jacksonville", "FL")]
    alone = _item("HSJT-26-13073025-DHA", "Practical Nurse", item["MatchedObjectDescriptor"]["PositionLocation"][:1],
                  sub="Military Treatment Facilities under DHA")
    rows = scraper._usajobs_jobs(alone, "Military Health System")
    assert [(j.job_id, j.city, j.state, j.hospital_name) for j in rows] == [
        ("HSJT-26-13073025-DHA", "", "FL", "Naval Hospital Jacksonville")]


# ── 4. The same-requisition rule (S1) ───────────────────────────────────────

def test_s1_keeps_every_town_of_one_announcement():
    rows = scraper.finalize_jobs(scraper._usajobs_jobs(HOUSTON_3, "VA Hospitals"))
    assert sorted((r["job_id"], r["city"], r["state"]) for r in rows) == [
        ("CBTH-13094001-27-RS", "Houston", "TX"),
        ("CBTH-13094001-27-RS-conroe-tx", "Conroe", "TX"),
        ("CBTH-13094001-27-RS-lake-jackson-tx", "Lake Jackson", "TX"),
    ]


def test_s1_merges_a_true_same_town_repeat_and_keeps_other_announcements():
    rows = scraper._usajobs_jobs(HOUSTON_3, "VA Hospitals")
    # one announcement's Houston row under a second posting id
    repeat = dataclasses.replace(rows[0], job_id="CBTH-13094001-27-RS-houston-tx", req_id="CBTH-13094001-27-RS")
    # another announcement, same title, same town: a second opening, kept
    other = scraper._usajobs_jobs(_item("CBTH-13094777-27-RS", "Registered Nurse - Primary Care (PACT)",
                                        [_loc("Houston", "Texas")], control="889100777"), "VA Hospitals")
    out = scraper.finalize_jobs(rows + [repeat] + other)
    houston = sorted(r["job_id"] for r in out if r["city"] == "Houston")
    assert houston == ["CBTH-13094001-27-RS", "CBTH-13094777-27-RS"]
    assert len(out) == 4


# ── run_usajobs end to end ───────────────────────────────────────────────────

@pytest.fixture
def usajobs_multi(monkeypatch):
    calls = []
    states = sorted(scraper._STATE_CODE_BY_NAME.items())
    big = _item("HSJT-27-13095000-DHA", "Medical Support Assistant (OA)",
                [_loc(t, name.title()) for name, _ in states for t in ("Springfield", "Franklin")][:100],
                sub="Veterans Health Administration", control="889100200")
    remote = _item("CBSW-13089255-27-YW", "Physician - Diagnostic Tele-Radiologist", [ANYWHERE],
                   sub="Veterans Health Administration", control="889100300")
    page = {"SearchResult": {"SearchResultCount": 3, "SearchResultCountAll": 3,
                             "SearchResultItems": [HOUSTON_3, big, remote],
                             "UserArea": {"NumberOfPages": "1", "IsRadialSearch": False}}}
    empty = {"SearchResult": {"SearchResultCount": 0, "SearchResultCountAll": 0, "SearchResultItems": [],
                              "UserArea": {"NumberOfPages": "0"}}}

    async def fake_page(session, params, headers):
        calls.append((params["Organization"], params["JobCategoryCode"], params["Page"]))
        return 200, (page if params["Organization"] == "VATA" else empty)

    async def no_wait(*a, **k):
        return None

    monkeypatch.setenv("USAJOBS_API_KEY", "test-key")
    monkeypatch.setenv("USAJOBS_EMAIL", "test@example.org")
    monkeypatch.setattr(scraper, "_usajobs_page", fake_page)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    return calls


def test_run_usajobs_emits_a_row_per_town_once_across_the_series_searches(usajobs_multi, caplog):
    with caplog.at_level(logging.INFO, logger=scraper.logger.name):
        jobs = asyncio.run(scraper.run_usajobs(None))
    va = [j for j in jobs if j.hospital_system == "VA Hospitals"]
    assert len(va) == 3 + 60 + 1                     # every search returns the same three announcements
    assert len({j.job_id for j in va}) == len(va)
    assert "VA Hospitals" not in scraper.PARTIAL_SYSTEMS
    va_calls = [c for c in usajobs_multi if c[0] == "VATA"]
    assert [c[1] for c in va_calls] == scraper.usajobs_series_queries("VATA")
    assert any("0101" in c[1] for c in va_calls)
    assert not any("0101" in c[1] for c in usajobs_multi if c[0] != "VATA")
    log = caplog.text
    assert "USAJOBS VA Hospitals: 64 jobs from 3 announcements" in log
    assert "1 listing more than 60 towns cut to 60" in log
