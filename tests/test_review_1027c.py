"""Review 2026-10-27 (release 1027b): two fixes found in the live dry runs.

1. Day rates are never stored as hourly pay. "Salary: $125 per day" (an
   AppliTrack substitute school nurse), "Daily rate: $150" and "$150 - $175
   per day" were read as $125, $150 and $150-$175 an hour. "Per diem" (the
   PRN job type) and a hospitalist's "Day rate $150.02 / Night rate $189.50"
   (the day shift's hourly rate) keep their hourly reading.
2. Elevance's Workday locationsText "AL-HUNTSVILLE, 900 BOB WALLACE AVE SW,
   STE 122" stored the street as the town (21 of 115 rows in the dry run);
   the state-first rule now reads Huntsville.
"""
import asyncio
import os
import sys

for _k in [k for k in os.environ if k.startswith(("SUPABASE", "RAILWAY"))]:
    del os.environ[_k]
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

import scraper


@pytest.mark.parametrize("text", [
    "Salary: $125 per day",
    "Daily rate: $150",
    "$150 - $175 per day",
    "$125/day",
    "Substitute Nurse $140 a day",
    "Daily Rate of pay for substitutes: $100",
    "Pay: $165.00 per work day",
])
def test_day_rate_is_never_hourly(text):
    assert scraper.extract_posted_wage(text) is None


@pytest.mark.parametrize("text,want", [
    ("Pay: $32.50 per hour", (32.5, 32.5, "hour")),
    ("$45/hr per diem", (45.0, 45.0, "hour")),
    ("Per Diem Rate: $45.00", (45.0, 45.0, "hour")),
    ("$45.00 - $60.00 per hour, day shift", (45.0, 60.0, "hour")),
    ("Day shift: $45 per hour", (45.0, 45.0, "hour")),
    ("$40 per hour (day shift)", (40.0, 40.0, "hour")),
    ("Starting at $38.50/hr. Daily pay available", (38.5, 38.5, "hour")),
    ("$58,000 - $62,000 per year", (58000.0, 62000.0, "year")),
])
def test_hourly_and_annual_pay_unchanged(text, want):
    assert scraper.extract_posted_wage(text) == want


def test_hospitalist_day_rate_label_is_the_day_shift_hourly_rate():
    body = "Hospitalist Physician\nOverview\n\n7 on, 7 off\n\nEpic EMR\n\nDay rate $150.02\n\nNight rate $189.50\n"
    assert scraper.extract_posted_wage(body) == (150.02, 150.02, "hour")


@pytest.mark.parametrize("text,want", [
    ("AL-HUNTSVILLE, 900 BOB WALLACE AVE SW, STE 122", ("Huntsville", "AL")),
    ("KY-LOUISVILLE, 3195 TERRA CROSSING BLVD, STE 202, 204, 300", ("Louisville", "KY")),
    ("TX-RICHARDSON, 1748 N GREENVILLE AVE", ("Richardson", "TX")),
    ("IL - Chicago", ("Chicago", "IL")),
    ("OH-MASON-4361-IRWIN-SIMPSON-RD", ("Mason", "OH")),
])
def test_state_first_town_stops_at_the_comma(text, want):
    assert scraper._wd_state_first(text) == want


class _Resp:
    def __init__(self, data):
        self.status = 200
        self._data = data

    async def json(self, content_type=None):
        return self._data


class _Ctx:
    def __init__(self, resp):
        self.resp = resp

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


@pytest.mark.parametrize("system,loc,want", [
    ("Elevance Health", "AL-HUNTSVILLE, 900 BOB WALLACE AVE SW, STE 122", ("Huntsville", "AL")),
    ("Elevance Health", "KY-LOUISVILLE, 3195 TERRA CROSSING BLVD, STE 202", ("Louisville", "KY")),
    ("CVS Health", "IL - Chicago", ("Chicago", "IL")),
])
def test_workday_row_never_takes_the_street_as_its_town(monkeypatch, system, loc, want):
    posting = {"title": "Nurse Case Manager II", "externalPath": "/job/X/Nurse-Case-Manager-II_JR1",
               "locationsText": loc, "postedOn": "Posted Today", "bulletFields": ["JR1"]}

    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("limit") == 1:
            return _Ctx(_Resp({"facets": []}))
        return _Ctx(_Resp({"total": 1, "jobPostings": [posting]}))

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    jobs = asyncio.run(scraper.scrape_workday(None, system, ("t", "1", "S")))
    assert (jobs[0].city, jobs[0].state) == want
