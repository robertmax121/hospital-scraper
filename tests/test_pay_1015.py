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
5. Review fixes: an add-on range is refused whole (never its high end
   alone), labels that describe the pay are not add-ons, the orientation
   rate is not the pay, a field spanning the levels the body prices stays,
   and the USAJOBS city / territory / facility edge cases.
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
    # 2026-10-25 (Bounce QA): the body's own comma-less "Salary Range From
    # $210226 to $210226 Annually" line is read now, the same figure as the
    # field; the $30,771 add-on is still never the pay.
    assert extract_posted_wage(f"{title}\n{body}") == (210226.0, 210226.0, "year")
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
    # 2026-10-20: one search per block of series (usajobs_series_queries), each read to its last page
    queries = scraper.usajobs_series_queries("VATA")
    assert [c for c in usajobs_api if c[0] == "VATA"] == [("VATA", p, 500) for _ in queries for p in (1, 2)]
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


# ── 5. review fixes (2026-10-15) ─────────────────────────────────────────────
# Each body line is quoted from a stored hospital_jobs row (id in the comment)
# or from the review's own cases.

@pytest.mark.parametrize("title, line", [
    # a range that prices an add-on is one add-on: its high end is not the pay
    ("Registered Nurse", "Weekend premium $38.50 - $52.00 per hour"),
    ("Registered Nurse", "Location adjustment: $62.00 - $75.00 per hour"),
    ("Registered Nurse", "Shift differential $3.00 - $5.00 per hour"),
    ("Registered Nurse", "Sign-on Bonus Pay: $5,000"),
    ("Registered Nurse", "Shift Differential Pay Rate: $3/hr"),
    # Penn 37067651: the orientation rate under a pay label, beside a placeholder band
    ("PRN Home Health Physical Therapist -Days-Princeton",
     "Salary: \\$26/Hr Orientation, Paid Per Visit\n\nSalary Range:\n\n\\$7.25 - \\$120.00/Hourly"),
    ("Registered Nurse", "Pay Rate: $30.00/hr (orientation)"),
])
def test_add_on_ranges_and_orientation_rates_store_nothing(title, line):
    assert extract_posted_wage(f"{title}\n{line}") is None


@pytest.mark.parametrize("title, line, want", [
    # the same label words describing the pay itself
    ("Registered Nurse", "Employment type: On Call $38.50 - $52.00 per hour", (38.5, 52.0, "hour")),
    ("Registered Nurse", "The pay range, with location adjustment, is $80,000 - $110,000 annually",
     (80000.0, 110000.0, "year")),
    ("Nurse Practitioner", "Pay range after geographic adjustment $120,000 - $150,000", (120000.0, 150000.0, "year")),
    ("Nurse Practitioner", "Salary: Increased Hiring Rate $150,000 - $175,000", (150000.0, 175000.0, "year")),
    # a pay label after the words of an earlier figure or an aside heads its own figure
    ("Registered Nurse RN FT ICU Nights", "$20,000 Sign on Bonus- Pay range: $51.19 - $63.65/Hr\n\nKindred Hospital",
     (51.19, 63.65, "hour")),                                                                    # ScionHealth 32101894
    ("Registered Nurse (RN) - Emergency Department",
     "** Relocation Provided if not local **Compensation range: $44.92-$70.08/hour based on...",
     (44.92, 70.08, "hour")),                                                                    # Main Line 38277794
    ("Partial Hospitalization Clinician",
     "**Currently offering up to a $5k Signing Bonus for this role** Pay: $28.00-$38.00/hour Schedule/Location: Full Time",
     (28.0, 38.0, "hour")),                                                                      # UHS 26531686
    # an aside or a joiner after a range's own unit
    ("Admissions RN - Nights", "Salary range\n\n$41 - $50 / hour (Including shift differential)\n\nBENEFITS:",
     (41.0, 50.0, "hour")),                                                                      # Acadia 34836759
    ("Weekend Home Health Visits Registered Nurse",
     "Base Pay, depending on qualifications\n\n$60.00 - $65.00 per point Plus Weekend Differential",
     (60.0, 65.0, "visit")),                                                                     # BAYADA 38060992
    ("RN New Grad Residency",
     "Graduated pay scale during nurse residency program from $28-30/ hour with various incentives",
     (28.0, 30.0, "hour")),                                                                      # BAYADA 36615018
    ("Registered Nurse", "$30 - $40 per hour, shift differential", (30.0, 40.0, "hour")),    # as before
    # orientation words that do not name the figure
    ("Registered Nurse - Part Time Weekends",
     "Attractive pay\n\n$36.00 to $40.00 per hour for Orientation and will then convert to a per point pay "
     "system with a base rate of $48.00 to $50.00 per point.", (36.0, 40.0, "hour")),             # Amedisys 14792756
    ("Registered Nurse", "Pay rate: $45.00/hr after orientation", (45.0, 45.0, "hour")),
])
def test_pay_described_by_add_on_words_is_read_whole(title, line, want):
    assert extract_posted_wage(f"{title}\n{line}") == want


ASANTE_PHARM = ("Pharmacy Technician 1 and 2 positions. Starting wages for each level are noted below.\n\n"
                "Starting Wage:\n\nPharmacy Tech 1: \\$21.15-\\$29.09 per hour, depending on experience\n\n"
                "Pharmacy Tech 2: \\$22.53-\\$30.98 per hour, depending on experience")     # Asante 36434610


def test_field_spanning_the_levels_the_body_prices_stays():
    # Asante's header spans Tech 1 and Tech 2 (21.15-30.98); the text rule
    # read Tech 1 alone once "\$" parsed.
    assert extract_posted_wage("Pharmacy Technician\n" + ASANTE_PHARM) == (21.15, 29.09, "hour")
    field, text = (21.15, 30.98, "hour"), (21.15, 29.09, "hour")
    assert scraper.pay_text_overrides(field, text) is True
    assert scraper.pay_text_overrides(field, text, "Pharmacy Technician\n" + ASANTE_PHARM) is False
    j = _job(title="Pharmacy Technician", hospital_system="Asante Health System", description=ASANTE_PHARM + FILLER,
             wage_min=21.15, wage_max=30.98, wage_unit="hour")
    d = scraper.normalize_job(j)
    assert _pay(d) == (21.15, 30.98, "hour") and d["posting_facts"]["pay_src"] == "field"


@pytest.mark.parametrize("title, body, field, want", [
    # a field figure the body never prints: the text wins, as push 10 decided
    ("Registered Nurse RRMC (Ashland-Endoscopy)", "Salary Range: \\$53.99 - \\$72.47\n\nShift Differentials:",
     (53.99, 77.79, "hour"), (53.99, 72.47, "hour")),                                           # Asante 35489890
    # a field that does not hold the text's range (another role's band) is replaced
    ("Registered Nurse (RN) - Cove's Edge",
     "The pay range for this position for an LPN is $27.27 to $36.88 per hour.\n\n"
     "The pay range for this position for an RN is $31.79 to $43.02 per hour.",
     (27.27, 36.88, "hour"), (31.79, 43.02, "hour")),                                           # MaineHealth 32717085
])
def test_text_still_replaces_a_field_the_body_disagrees_with(title, body, field, want):
    j = _job(title=title, description=body + FILLER, wage_min=field[0], wage_max=field[1], wage_unit=field[2])
    d = scraper.normalize_job(j)
    assert _pay(d) == want and d["posting_facts"]["pay_src"] == "text"


@pytest.mark.parametrize("name, want", [
    ("Cincinnati, Ohio", "Cincinnati"),
    ("Point Loma Complex, San Diego, California", "San Diego"),      # the API docs' own example
    ("Washington, District of Columbia", "Washington"),
    ("Anywhere in the U.S. (remote job)", "Anywhere in the U.S. (remote job)"),
    ("", ""),
])
def test_usajobs_city_is_the_part_before_the_state(name, want):
    assert scraper._usajobs_city(name) == want


@pytest.mark.parametrize("code, want", [
    ("Ohio", "OH"), ("District of Columbia", "DC"), ("TX", "TX"),
    ("Puerto Rico", "PR"), ("Guam", "GU"), ("Virgin Islands", "VI"), ("U.S. Virgin Islands", "VI"),
    ("Ontario", ""), ("", ""),
])
def test_usajobs_state_codes_include_the_territories(code, want):
    assert scraper._usajobs_state(code) == want


@pytest.mark.parametrize("summary, want", [
    ("Summary The Lt. Col. Luke Weathers, Jr. VA Medical Center in Memphis is hiring.",
     "Lt. Col. Luke Weathers, Jr. VA Medical Center"),
    ("Join the team at Phoenix Indian Medical Center today.", "Phoenix Indian Medical Center"),
    ("Gallup Indian Medical Center serves the Navajo Nation.", "Gallup Indian Medical Center"),
    ("Work at the Indian Health Service Hospital in your area.", ""),
    ("Join the Jr. VA Medical Center team.", ""),
])
def test_usajobs_facility_names_from_text_edge_cases(summary, want):
    assert scraper._fed_facility_from_text(summary) == want


def test_usajobs_job_location_for_a_complex_and_a_territory():
    item = _usaj_item("880000001", "Registered Nurse", "Veterans Health Administration", "x", "California",
                      87457, 153766)
    loc = item["MatchedObjectDescriptor"]["PositionLocation"][0]
    loc["CityName"] = "Point Loma Complex, San Diego, California"
    j = scraper._usajobs_job(item, "VA Hospitals")
    assert (j.city, j.state, j.location) == ("San Diego", "CA", "San Diego, CA")
    pr = _usaj_item("880000002", "Staff Nurse", "VA Caribbean Healthcare System", "San Juan", "Puerto Rico",
                    70000, 120000)
    j = scraper._usajobs_job(pr, "VA Hospitals")
    assert (j.city, j.state, j.hospital_name) == ("San Juan", "PR", "VA Caribbean Healthcare System")
    assert scraper.non_us_reason(j.location, j.city, j.state, j.url) is None
