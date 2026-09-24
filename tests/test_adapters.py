# 2026-09-10 (S-scraper-2): one recorded fixture per adapter touched in this
# batch (NeoGov, ADP, UKG, HCA). Everything here is offline: the fixtures under
# tests/fixtures were captured on 2026-09-10 through the adapters' own
# functions and trimmed to two or three items.
import json
import os
import re

import scraper

from conftest import FIXTURES


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


# ── NeoGov ────────────────────────────────────────────────────────────────
def test_neogov_page_parses_cards_total_and_defaults(fixture_text):
    html = fixture_text("neogov_page.html")
    jobs, total, cards = scraper._parse_neogov_page(
        html, "Test County Hospital", "testagency", "CA", "Martinez", "Test County Hospital", None)
    assert cards == 2 and len(jobs) == 2
    assert total >= 2
    for j in jobs:
        assert j.ats_platform == "NeoGov"
        assert re.fullmatch(r"\d+", j.job_id)
        assert j.url.startswith("https://www.governmentjobs.com/careers/") and f"/jobs/{j.job_id}/" in j.url
        assert j.title and j.hospital_name == "Test County Hospital"
        assert j.state == "CA"                       # config state unless the card says otherwise
        assert (j.wage_min is None) == (j.wage_unit is None)
        if j.wage_unit:
            assert j.wage_unit in ("hour", "year") and j.wage_min <= j.wage_max


def test_neogov_department_regex_filters_rows(fixture_text):
    html = fixture_text("neogov_page.html")
    none = scraper._parse_neogov_page(html, "x", "a", "CA", "", "x", re.compile(r"^no such department$", re.I))
    assert none[0] == [] and none[2] == 2            # cards still counted, rows dropped
    every = scraper._parse_neogov_page(html, "x", "a", "CA", "", "x", re.compile(r".", re.I))
    assert len(every[0]) == 2


def test_neogov_agencies_are_ordered_transparency_states_first():
    order = scraper.priority_states_first(list(scraper.NEOGOV_AGENCIES.items()), lambda kv: kv[1][1])
    states = [cfg[1] for _, cfg in order]
    first_non_ca = next((i for i, s in enumerate(states) if s != "CA"), len(states))
    assert all(s == "CA" for s in states[:first_non_ca]) and "CA" not in states[first_non_ca:]


# ── ADP Workforce Now ─────────────────────────────────────────────────────
def test_adp_requisition_maps_location_pay_and_deep_link():
    data = _load("adp_requisitions.json")
    items = data["jobRequisitions"]
    assert items and data["meta"]["totalNumber"] >= len(items)
    job = scraper._adp_job(items[0], "Wooster Community Hospital", "cid-1", "cc-1", "OH", "Wooster Community Hospital")
    assert job is not None and job.ats_platform == "ADP"
    assert job.job_id == items[0]["itemID"]
    assert job.url.startswith(scraper._ADP_PORTAL) and f"jobId={job.job_id}" in job.url and "ccId=cc-1" in job.url
    assert job.city == "Wooster" and job.state == "OH"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", job.posted_date)
    pg = items[0].get("payGradeRange") or {}
    if pg.get("minimumRate") and pg.get("maximumRate"):
        assert job.wage_min == pg["minimumRate"]["amountValue"]
        assert job.wage_max == pg["maximumRate"]["amountValue"]
        assert job.wage_unit in ("hour", "year")


def test_adp_requisition_without_id_is_dropped():
    assert scraper._adp_job({"requisitionTitle": "RN"}, "S", "c", "cc") is None
    assert scraper._adp_job({"itemID": "1"}, "S", "c", "cc") is None


def test_adp_centers_accept_every_config_shape():
    assert scraper._adp_centers("abc") == [("abc", "19000101_000001", "", "")]
    assert scraper._adp_centers(("abc", "cc", "OH")) == [("abc", "cc", "OH", "")]
    assert scraper._adp_centers([("a", "c1", "OH", "H1"), ("a", "c2")]) == [("a", "c1", "OH", "H1"), ("a", "c2", "", "")]


# ── UKG Pro Recruiting ────────────────────────────────────────────────────
def test_ukg_opportunity_maps_pascal_case_fields():
    data = _load("ukg_search.json")
    item = data["opportunities"][0]
    job = scraper._ukg_job(item, "Deaconess Health", "https://deaconess.rec.pro.ukg.net/DEA1005DEAC", "guid-1", "IN")
    assert job.ats_platform == "UKG" and job.title == item["Title"]
    assert job.job_id == item["Id"]
    assert job.url == "https://deaconess.rec.pro.ukg.net/DEA1005DEAC/JobBoard/guid-1/OpportunityDetail?opportunityId=" + item["Id"]
    addr = item["Locations"][0]["Address"]
    assert job.city == addr["City"] and job.state == addr["State"]["Code"]
    assert job.posted_date == item["PostedDate"][:10]
    assert job.job_type == ("Full time" if item["FullTime"] else "Part time")
    assert job.specialty == item["JobCategoryName"]


def test_ukg_opportunity_falls_back_to_default_state_and_system_name():
    job = scraper._ukg_job({"Id": "x", "Title": "RN", "FullTime": False}, "Sys", "https://b/ORG", "g", "AL")
    assert job.state == "AL" and job.city == "" and job.hospital_name == "Sys" and job.job_type == "Part time"


# ── HCA (Talemetry HTML) ──────────────────────────────────────────────────
def test_hca_cards_parse_from_recorded_page(fixture_text):
    cards = scraper._parse_hca_cards(fixture_text("hca_cards.html"))
    assert len(cards) == 3
    for c in cards:
        assert c.hospital_system == "HCA Healthcare" and c.ats_platform == "Talemetry"
        assert c.state == "AK" and c.city and c.hospital_name
        assert c.url.startswith("https://careers.hcahealthcare.com/jobs/") and c.job_id.isdigit()


class _Page:
    def __init__(self, text):
        self.text = text


def test_hca_slice_stops_on_the_national_search_fallback(fixture_text, monkeypatch):
    page = fixture_text("hca_cards.html")          # Alaska cards
    served = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        served.append((url, kw.get("params", {}).get("page")))
        return _Page(page if kw.get("params", {}).get("page") == "1" else "")

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)
    # A slug the site does not know answers with other states' cards: stop.
    assert scraper._hca_fetch_slice("ms-mississippi") == ([], True, False)
    # The real state finishes on its short first page (3 cards of a 500-card
    # page); since 2026-09-24 no request goes to the empty page past the end.
    jobs, finished, capped = scraper._hca_fetch_slice("ak-alaska")
    assert len(jobs) == 3 and finished and not capped
    assert [p for _, p in served] == ["1", "1"]
