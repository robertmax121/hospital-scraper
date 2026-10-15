"""Pay parsing and coverage fixes (2026-10-15, wage accuracy: audit section 2
root cause 2 and section 3 item 12). Fixtures in tests/fixtures/pay1015 are
trimmed from the employers' live job pages, fetched 2026-10-15. No network,
no database.

1. Misreads: an add-on (premium, differential, call pay, a New York State
   hiring-rate increment) never becomes base pay, and a figure split across
   HTML tags ("$28</span><span>5,000") is one figure.
2. Single-number ranges: the Jobvite Engage JSON-LD repair kept a literal
   backslash before "$" (UCHealth, PeaceHealth, Penn Medicine), and Bassett's
   iCIMS template puts "USD" before each labelled end. Hackensack, Franciscan,
   Great River and Montefiore really post one number; they stay single.
3. Pay-law states: CT, VA and ME in TRANSPARENCY_STATES; MA, VA and ME in
   DETAIL_PRIORITY_STATES (order only).
4. USAJOBS: every results page, the hiring facility as hospital_name, and
   annual federal pay stored hourly at 2,087 hours.
"""
import asyncio
import json
import os

import pytest

import scraper
from scraper import Job, extract_posted_wage, strip_html

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "pay1015")
FILLER = "\n\n" + "The team cares for patients on a busy unit and supports the department. " * 30


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


def _wd_desc(name):
    return strip_html(json.loads(_read(name))["jobPostingInfo"]["jobDescription"])


def _job(**kw):
    base = dict(title="Registered Nurse", hospital_system="Test System", hospital_name="Test System",
                city="Springfield", state="MO", location="Springfield, MO", specialty="", job_type="",
                url="https://example.org/job/1", job_id="1", posted_date="", description="", ats_platform="Test")
    base.update(kw)
    return Job(**base)


def _pay(d):
    return d["wage_min"], d["wage_max"], d["wage_unit"]


# ── 1. misreads ──────────────────────────────────────────────────────────────

def test_corewell_crna_salary_split_across_spans_is_one_figure():
    # Corewell 29387089 stored $28/hr: the page prints "$28</span><span>5,000".
    body = _wd_desc("corewell_R177903_detail.json")
    assert "base salary of $285,000" in body
    title = "APP - Pediatric CRNA - Helen DeVos Children's Hospital"
    d = scraper.normalize_job(_job(title=title, hospital_system="Corewell Health", description=body))
    assert _pay(d) == (285000.0, 285000.0, "year")
    assert d["posting_facts"]["pay_src"] == "text"


@pytest.mark.parametrize("html, text", [
    ("<p>Salary of $28</span><span>5,000</span><span>.</span></p>", "Salary of $285,000 ."),
    ("<p>Salary of $<b>2</b><span>85,000</span></p>", "Salary of $285,000"),
    ("<p>Up to $1<span>5,000</span> sign-on</p>", "Up to $15,000 sign-on"),
    # two whole figures side by side stay apart
    ("<p>$25<span>38,000</span></p>", "$25 38,000"),
    ("<p>$25.00<span>38,500</span></p>", "$25.00 38,500"),
    ("<p>$28<span>5,0000</span></p>", "$28 5,0000"),
])
def test_split_lead_group_joins_only_a_three_digit_group(html, text):
    assert strip_html(html) == text


def test_novant_float_premium_is_not_base_pay():
    # Novant 235815 (hospital_jobs 14793957) stored $15.00 - $15.00 an hour
    # from "Float pay premiums up to $15/hour".
    body = strip_html(_read("novant_235815_why.html"))
    assert "Float pay premiums up to $15/hour" in body
    title = "Registered Nurse - Float Pool | Critical Care"
    assert extract_posted_wage(f"{title}\n{body}") is None
    d = scraper.normalize_job(_job(title=title, hospital_system="Novant Health", description=body + FILLER))
    assert d["wage_min"] is None and d["wage_unit"] is None


@pytest.mark.parametrize("line", [
    # stored as base pay on active rows on 2026-10-15 (hospital_jobs ids)
    "Why Join Us?\n\nFloat pay premiums up to $15/hour\n\nNight shift and weekend differentials",   # Novant 14793957
    "Benefits:\n\nWeekend Premium Pay $12/hr\n\nShift Differential $4/hr",                          # CHS 36447829
    "Career Ladder Bonus eligible up to $5000\n\n$14.50 Float Differential\n\n$5 Shift Differential",  # CoxHealth 28948478
    "Tier 4\nPay rate: Experience rate + $15.00 per hour (plus any applicable shift differentials)",  # SE Georgia 26700273
    "Guarantee of 40 hours/week pay.\n\nPremium Pay rate of $200/hour for any hours over 40 in a work week.",  # CHRISTUS 20569
    "For the weekend shifts, you are paid a $20/hr WST premium on top of your base rate.",          # Spartanburg 24060966
    "24/7 staffing options with base rate + $15/hr enhanced pay.",                                  # Union Hospital 28622394
    "Weekend only RNs earn an additional $20/hour for weekend incentive pay in addition to base.",   # UofL 2869262
    "Current benefits include:\n\nShift Differentials up to $14 per hour!!!",                        # Acadia 32487368
    "Shift: Friday - Sunday, 7pm - 7:30am\n\n$20/hour Weekend Option Premium!",                      # Mercy 28999789
    "$40k Sign-On Bonus\n\n$15K Incentive\n\n$185/hr excess hours",                                  # Centra 2298194
    "Premium Pay Rates:\n$15/hour premium on all hours worked.\n$1.25/hour mileage",                 # Sanford 28076038
    "All qualified hours on the weekend are eligible for a $15/hour weekend premium",                # Henry Ford 34526950
    "Physicians at eligible sites may qualify for up to $75,000 in tax-free loan repayment",         # Presbyterian 35283122
    "Call: $250 per point, max call cap $45, 000.00",                                                # Intermountain 8687958
    "On-call pay of $5/hr for every hour on call",
])
def test_add_on_amounts_never_become_base_pay(line):
    assert extract_posted_wage("Registered Nurse\n" + line) is None


@pytest.mark.parametrize("line, want", [
    # the same words around a figure that IS the pay
    ("Salary Range: $4.00 / Hour On Call - $40.00 / Hour Active Pay", (40.0, 40.0, "hour")),        # Sanford 27785121
    ("Schedule: PRN - Work schedule is primarily weekend on-call Compensation: $42.00 per hour", (42.0, 42.0, "hour")),  # Select 37968721
    ("On-call pay of $5/hr. Pay range: $40.00 - $50.00 per hour", (40.0, 50.0, "hour")),
    ("No call required. Salary: $300,000 per year", (300000.0, 300000.0, "year")),
    ("Pay Range: $26.18 - $33.51 Relief Differential - 15%", (26.18, 33.51, "hour")),
])
def test_pay_next_to_add_on_words_is_still_read(line, want):
    assert extract_posted_wage("Registered Nurse\n" + line) == want


def test_intermountain_call_pay_does_not_outrank_the_base_salary():
    body = _wd_desc("intermountain_R144295_detail.json")
    title = "Family Medicine Residency Faculty Physician-Grand Junction"
    assert extract_posted_wage(f"{title}\n{body}") == (221000.0, 221000.0, "year")


def test_nys_increase_hiring_rate_is_an_add_on_and_the_salary_field_stays():
    # OMH 35485301: the field reads $210,226; the body's "$30,771" is an add-on
    # and, read as pay, would have replaced the field (pay_text_overrides).
    html = _read("nysomh_225533_pay.html")
    body = strip_html(html)
    title = "Dentist 2 (NY HELPS), St. Lawrence Psychiatric Center, P28013"
    assert extract_posted_wage(f"{title}\n{body}") is None
    fields = [("Salary Range", "From $210226 to $210226 Annually")]
    got = scraper.field_wage_from_labels(fields)
    assert got == (210226.0, 210226.0, "year")
    j = _job(title=title, hospital_system="New York State Office of Mental Health", description=body + FILLER,
             wage_min=got[0], wage_max=got[1], wage_unit=got[2])
    d = scraper.normalize_job(j)
    assert _pay(d) == (210226.0, 210226.0, "year") and d["posting_facts"]["pay_src"] == "field"
    # the Kirby posting's own salary sentence is still the pay
    kirby = ("The salary of $145,563 includes a Geographic Pay Differential ($20,000), an Increased Hiring Rate "
             "($25,077), and a Downstate Adjustment ($4,150).")
    assert extract_posted_wage("Licensed Psychologist\n" + kirby) == (145563.0, 145563.0, "year")


# ── 2. single-number ranges ──────────────────────────────────────────────────

@pytest.mark.parametrize("name, title, want", [
    ("uchealth_18229356_ld.html", "Emergency Department RN", (35.29, 54.71, "hour")),
    ("peacehealth_17899069_ld.html", "Infection Preventionist", (48.52, 72.78, "hour")),
    ("pennmedicine_18314473_ld.html", "Registered Nurse (Outpatient Infusion) - Full Time (Day Shift)",
     (49.96, 68.99, "hour")),
])
def test_jobvite_engage_ranges_survive_the_json_ld_repair(name, title, want):
    html = _read(name)
    assert scraper._jobposting_from_html(html) is None            # the page's JSON-LD is invalid JSON
    posting = scraper._talemetry_posting(html)
    desc = strip_html(posting["description"])
    assert "\\$" not in desc and "\\" not in desc
    d = scraper.normalize_job(_job(title=title, description=desc))
    assert _pay(d) == want


@pytest.mark.parametrize("stored, want", [
    # bodies stored before the repair keep their backslashes until re-read
    ("Pay: \\$35.29 - \\$54.71 / hour. Pay is dependent on applicant's relevant experience", (35.29, 54.71, "hour")),
    ("Pay Range: \\$48.52 \u2013 \\$72.78/hour", (48.52, 72.78, "hour")),
    ("Salary Range:\n\n\\$49.96 - \\$68.99/Hourly", (49.96, 68.99, "hour")),
    ("Salary Range:\n\n\\$7.25 - \\$120.00/Hourly", None),          # Penn's placeholder band stays too wide
    ("Pay Range: $$45.00 - $$60.00/hour", (45.0, 60.0, "hour")),
])
def test_escaped_or_doubled_dollar_signs_read_as_one(stored, want):
    assert extract_posted_wage("Registered Nurse\n" + stored) == want


def test_talemetry_repair_keeps_valid_escapes_and_drops_stray_ones():
    raw = '{"@type": "JobPosting", "title": "A \\"q\\" \\u00e9", "description": "a\\nb \\$5 \\- c \\\\$ d <\\/p>"}'
    page = f'<script type="application/ld+json">{raw}</script>'
    posting = scraper._talemetry_posting(page)
    assert posting["title"] == 'A "q" \u00e9'
    assert posting["description"] == "a\nb $5 - c \\$ d </p>"


def test_bassett_usd_minimum_and_maximum_lines_are_one_range():
    body = strip_html(_read("bassett_16400_pay.html"))
    assert "Pay Range Minimum:\n\nUSD $19.12/Hr." in body
    assert extract_posted_wage("Nursing Assistant III, Per Diem, O'Connor Hospital\n" + body) == (19.12, 28.68, "hour")
    annual = "Pay Range Minimum:\n\nUSD $93,194.92/Yr.\nPay Range Maximum:\n\nUSD $139,792.38/Yr."
    assert extract_posted_wage("Clinic Nursing Supervisor\n" + annual) == (93194.92, 139792.38, "year")


def test_sources_that_post_one_number_stay_single():
    # Hackensack: "Minimum rate of" (the NJ disclosure of the minimum base pay)
    hmh = strip_html(_read("hackensack_98503713440_comp.html"))
    assert extract_posted_wage("Registered Nurse - Med/Surg/Tele Float Pool - F/T Nights\n" + hmh) == \
        (45.14, 45.14, "hour")
    # Franciscan "Minimum Start Pay", Great River "Minimum Hiring Wage: From",
    # Montefiore "Hourly Rate" (1199 union rate): one figure on the page.
    assert extract_posted_wage("Registered Nurse\n" + _wd_desc("franciscan_R-132922_detail.json")) == \
        (34.0, 34.0, "hour")
    assert extract_posted_wage("Registered Nurse (RN) | 36 hours per week | Medical Surgical Unit\n"
                               + _wd_desc("greatriver_R-09079_detail.json")) == (35.79, 35.79, "hour")
    assert extract_posted_wage("CARDIOVASCULAR TECHNOLOGIST\n" + _wd_desc("montefiore_219715_detail.json")) == \
        (45.44, 45.44, "hour")


# ── 3. pay-law states ────────────────────────────────────────────────────────

def test_newer_pay_law_states_are_in_both_lists():
    older = ("CO", "WA", "CA", "NY", "IL", "MN", "MD", "HI", "DC", "NJ", "MA", "VT")
    assert scraper.TRANSPARENCY_STATES[:12] == older
    assert scraper.TRANSPARENCY_STATES[12:] == ("CT", "VA", "ME")
    assert {"MA", "VA", "ME", "CT"} <= scraper.DETAIL_PRIORITY_STATES
    items = ["tx-texas", "va-virginia", "me-maine", "co-colorado", "ct-connecticut", "fl-florida"]
    assert scraper.priority_states_first(items, lambda s: s[:2]) == \
        ["co-colorado", "ct-connecticut", "va-virginia", "me-maine", "tx-texas", "fl-florida"]


def test_detail_rank_puts_ma_va_me_rows_first():
    rows = [_job(state="TX", posted_date="2026-10-14"), _job(state="VA", posted_date="2026-09-01"),
            _job(state="MA"), _job(state="ME", posted_date="2026-10-01")]
    ranked = sorted(rows, key=scraper._detail_rank)
    assert [r.state for r in ranked] == ["ME", "VA", "MA", "TX"]


# ── 4. USAJOBS ───────────────────────────────────────────────────────────────
# Shape: developer.usajobs.gov/api-reference/get-api-search (SearchResult,
# UserArea.NumberOfPages, UserArea.Details.SubAgencyName). Values: the live
# usajobs.gov pages of these announcements on 2026-10-15 (hiring
# organization, pay, location). The search API needs a key we do not hold
# locally, so the payloads are built here rather than recorded.

def _usaj_item(pid, title, sub, city, state, lo, hi, code="PA", summary=""):
    return {"MatchedObjectId": pid, "MatchedObjectDescriptor": {
        "PositionID": pid, "PositionTitle": title,
        "PositionURI": f"https://www.usajobs.gov:443/GetJob/ViewDetails/{pid}",
        "PositionLocation": [{"LocationName": f"{city}, {state}", "CountryCode": "United States",
                              "CountrySubDivisionCode": state, "CityName": f"{city}, {state}"}],
        "OrganizationName": "Veterans Health Administration", "DepartmentName": "Department of Veterans Affairs",
        "JobCategory": [{"Name": "Nurse", "Code": "0610"}],
        "PositionSchedule": [{"Name": "Full-time", "Code": "1"}],
        "QualificationSummary": "Basic requirements: graduate of a school of professional nursing.",
        "PositionRemuneration": [{"MinimumRange": str(lo), "MaximumRange": str(hi), "RateIntervalCode": code,
                                  "Description": "Per Year" if code == "PA" else "Per Hour"}],
        "PublicationStartDate": "2026-10-01T00:00:00.0000",
        "UserArea": {"Details": {"JobSummary": summary, "SubAgencyName": sub, "LowGrade": "01", "HighGrade": "03"}},
    }}


USAJ_PAGES = {
    1: {"SearchResult": {"SearchResultCount": 3, "SearchResultCountAll": 5, "SearchResultItems": [
        _usaj_item("864236800", "Registered Nurse Community Living Center", "Cincinnati VHA Medical Center",
                   "Cincinnati", "Ohio", 87457, 153766),
        _usaj_item("871468000", "Inpatient Staff Registered Nurse (Med/Surg)",
                   "Veterans' Health Administration/Oklahoma City VA Health Care System/Nursing/Acute Care",
                   "Oklahoma City", "Oklahoma", 75016, 139502),
        _usaj_item("866093500", "Registered Nurse", "North Florida/South Georgia Healthcare System",
                   "Gainesville", "Florida", 73743, 130215),
    ], "UserArea": {"NumberOfPages": "2", "IsRadialSearch": False}}},
    2: {"SearchResult": {"SearchResultCount": 2, "SearchResultCountAll": 5, "SearchResultItems": [
        _usaj_item("879972400", "Registered Nurse - Open Continuous", "Veterans Health Administration",
                   "Houston", "Texas", 66680, 167227,
                   summary="This announcement may be used to fill vacancies at all locations covered by the "
                           "Michael E. DeBakey VA Medical Center"),
        _usaj_item("887880600", "Outpatient Staff RN", "Veterans Health Administration", "Baltimore", "Maryland",
                   78368, 153501, summary="The incumbent will be working at the Baltimore VA Medical Center "
                                          "located in Baltimore, Maryland as an Outpatient Staff RN."),
    ], "UserArea": {"NumberOfPages": "2", "IsRadialSearch": False}}},
}


@pytest.fixture
def usajobs_api(monkeypatch):
    calls = []

    async def fake_page(session, params, headers):
        calls.append((params["Organization"], params["Page"], params["ResultsPerPage"]))
        assert headers["Authorization-Key"] == "test-key"
        if params["Organization"] != "VATA":
            return 200, {"SearchResult": {"SearchResultCount": 0, "SearchResultCountAll": 0,
                                          "SearchResultItems": [], "UserArea": {"NumberOfPages": "0"}}}
        return 200, USAJ_PAGES[params["Page"]]

    async def no_wait(*a, **k):
        return None

    monkeypatch.setenv("USAJOBS_API_KEY", "test-key")
    monkeypatch.setenv("USAJOBS_EMAIL", "test@example.org")
    monkeypatch.setattr(scraper, "_usajobs_page", fake_page)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    return calls


def test_usajobs_reads_every_page_names_the_facility_and_stores_hourly(usajobs_api):
    jobs = asyncio.run(scraper.run_usajobs(None))
    assert [c for c in usajobs_api if c[0] == "VATA"] == [("VATA", 1, 500), ("VATA", 2, 500)]
    va = {j.job_id: j for j in jobs if j.hospital_system == "VA Hospitals"}
    assert len(va) == 5
    assert {jid: j.hospital_name for jid, j in va.items()} == {
        "864236800": "Cincinnati VHA Medical Center",
        "871468000": "Oklahoma City VA Health Care System",
        "866093500": "North Florida/South Georgia Healthcare System",
        "879972400": "Michael E. DeBakey VA Medical Center",
        "887880600": "Baltimore VA Medical Center",
    }
    c = va["864236800"]
    assert (c.city, c.state, c.location) == ("Cincinnati", "OH", "Cincinnati, OH")
    # $87,457 - $153,766 a year at OPM's 2,087 hours
    assert (c.wage_min, c.wage_max, c.wage_unit) == (41.91, 73.68, "hour")
    assert "VA Hospitals" not in scraper.PARTIAL_SYSTEMS          # every page read
    d = scraper.normalize_job(c)
    assert _pay(d) == (41.91, 73.68, "hour") and d["state"] == "OH"


def test_usajobs_page_failure_keeps_rows_and_marks_the_crawl_partial(usajobs_api, monkeypatch):
    async def flaky(session, params, headers):
        if params["Page"] == 2:
            return 503, None
        return 200, USAJ_PAGES[1]
    monkeypatch.setattr(scraper, "_usajobs_page", flaky)
    jobs = asyncio.run(scraper.run_usajobs(None))
    assert len([j for j in jobs if j.hospital_system == "VA Hospitals"]) == 3
    assert "VA Hospitals" in scraper.PARTIAL_SYSTEMS              # page 2 unread: not swept tonight
    assert "VA Hospitals" not in scraper.COMPLETE_SYSTEMS


def test_usajobs_without_a_key_sends_nothing(monkeypatch):
    monkeypatch.delenv("USAJOBS_API_KEY", raising=False)

    async def boom(*a, **k):
        raise AssertionError("no request without a key")
    monkeypatch.setattr(scraper, "_usajobs_page", boom)
    assert asyncio.run(scraper.run_usajobs(None)) == []


def test_usajobs_401_stops_every_org(usajobs_api, monkeypatch):
    seen = []

    async def unauthorized(session, params, headers):
        seen.append(params["Organization"])
        return 401, None
    monkeypatch.setattr(scraper, "_usajobs_page", unauthorized)
    assert asyncio.run(scraper.run_usajobs(None)) == []
    assert seen == ["VATA"]


@pytest.mark.parametrize("rem, want", [
    ({"MinimumRange": "87457", "MaximumRange": "153766", "RateIntervalCode": "PA"}, (41.91, 73.68, "hour")),
    ({"MinimumRange": "103409.0", "MaximumRange": "134435.0", "RateIntervalCode": "PA"}, (49.55, 64.42, "hour")),
    ({"MinimumRange": "28.50", "MaximumRange": "36.10", "RateIntervalCode": "PH"}, (28.5, 36.1, "hour")),
    ({"MinimumRange": "450", "MaximumRange": "600", "RateIntervalCode": "PD"}, None),     # per day
    ({"MinimumRange": "0", "MaximumRange": "0", "RateIntervalCode": "WC"}, None),          # without compensation
    ({"MinimumRange": "20000", "MaximumRange": "24000", "RateIntervalCode": "PA"}, None),  # under the hourly band
    ({"MinimumRange": "", "MaximumRange": "153766", "RateIntervalCode": "PA"}, None),
])
def test_usajobs_pay_converts_annual_at_2087_hours(rem, want):
    assert scraper.usajobs_pay(rem) == want


@pytest.mark.parametrize("sub, summary, want", [
    ("Cincinnati VHA Medical Center", "", "Cincinnati VHA Medical Center"),
    ("Veterans' Health Administration/Oklahoma City VA Health Care System/Nursing/Acute Care", "",
     "Oklahoma City VA Health Care System"),
    ("North Florida/South Georgia Healthcare System", "", "North Florida/South Georgia Healthcare System"),
    ("Veterans Health Administration", "The Ralph H. Johnson VA Medical Center is seeking a nurse.",
     "Ralph H. Johnson VA Medical Center"),
    ("Veterans Health Administration", "Join VA Western Colorado Health Care System in Grand Junction.",
     "VA Western Colorado Health Care System"),
    ("Veterans Health Administration", "Positions at North Florida/South Georgia Veterans Health System sites.",
     "North Florida/South Georgia Veterans Health System"),
    ("Veterans Health Administration", "Work at a VA Medical Center near you.", "Veterans Health Administration"),
    ("", "", "Veterans Health Administration"),
])
def test_usajobs_facility_names(sub, summary, want):
    m = {"OrganizationName": "Veterans Health Administration",
         "UserArea": {"Details": {"SubAgencyName": sub, "JobSummary": summary}}}
    assert scraper.usajobs_facility(m, "VA Hospitals") == want
