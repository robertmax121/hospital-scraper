# 2026-10-05 (push 9, structured pay): offline tests for the structured pay
# readers. Fixtures in tests/fixtures/push9 are trimmed from the pages the
# structured-pay sweep saved read-only that day (approval_2026-10-05/
# structured-pay-sweep.md): only the pay-bearing fields, no contacts and no
# requisition-side fields.
import asyncio
import json

import pytest

import scraper


def _load(fixture_text, name):
    return json.loads(fixture_text(f"push9/{name}"))


def _job(**kw):
    base = dict(title="Registered Nurse", hospital_system="Test System", hospital_name="Test System",
                city="Springfield", state="MO", location="Springfield, MO", specialty="", job_type="",
                url="https://example.org/job/1", job_id="1", posted_date="", description="", ats_platform="Test")
    base.update(kw)
    return scraper.Job(**base)


# ── the shared field reader ────────────────────────────────────────────────

@pytest.mark.parametrize("value,unit,want", [
    ("$17.50 –  $23.88  / hour (Salary or hourly rate is based on job qualifications and relevant work experience)", None, (17.5, 23.88, "hour")),
    ("USD $35.00/Hr.-USD $79.07/Hr.", None, (35.0, 79.07, "hour")),          # Yale: the raw extractor read 35.00 - 35.00
    ("$23.97/Hr. -  $29.96/Hr.", None, (23.97, 29.96, "hour")),              # Trilogy: same
    ("$37.67- $54.42 /  Hourly", None, (37.67, 54.42, "hour")),
    ("$55,536 - $92,560 annual salary with offer based on experience", None, (55536.0, 92560.0, "year")),
    ("$123,427.20 - $172,244.80 (based on a 1.0 FTE)", None, (123427.2, 172244.8, "year")),
    ("Compensation range is $71,510.40 - $107,390.40 / salary. This vacancy is not eligible for sponsorship.", None,
     (71510.4, 107390.4, "year")),
    ("The estimated base pay for this position is $20.41 to $32.97. Additional individual compensation may be "
     "available for this role in the form of bonuses, differentials", None, (20.41, 32.97, "hour")),
    ("93.47-121.11", None, (93.47, 121.11, "hour")),
    ("225,000-250,000", None, (225000.0, 250000.0, "year")),
    ("$54.25 - $69.25", "HOURLY", (54.25, 69.25, "hour")),
    ("13.5800 Through 20.3700", None, (13.58, 20.37, "hour")),
    ("$20.2566 - $23.6974", None, (20.26, 23.7, "hour")),
    ("$ 18.79-28.03 USD", None, (18.79, 28.03, "hour")),
    ("From $42641 to $52413 Annually", None, (42641.0, 52413.0, "year")),   # NYS: no comma group
    ("From $17 to $17 Hourly", None, (17.0, 17.0, "hour")),
    ("$4,094.50 - $5,094.16", "Monthly", (49134.0, 61129.92, "year")),       # TX HHS, unit from "Pay Frequency"
    ("62.33/hr.", None, (62.33, 62.33, "hour")),
    ("16.01 USD per hour", None, (16.01, 16.01, "hour")),                    # BayCare: one rate
])
def test_field_wage_shapes(value, unit, want):
    assert scraper.field_wage(value, unit) == want


@pytest.mark.parametrize("value,unit", [
    ("Min - $0.00 Mid - $0.00 Max - $0.00", None),   # Willis Knighton placeholder
    ("0 - 0  per hour", None), ("0.00 - 0.00 USD", None), ("0 - 0", None),   # Infor: no posted pay
    ("$1 - $1,000,000", None),                        # Findly JSON-LD placeholder band (S1 width)
    ("$15.00 - $130.00", None),                       # S1: a boilerplate band
    ("PG-37", None), ("N29", None),                   # pay grades
    ("Sign On Bonus Eligible-$20,000", None),          # a bonus, not the pay
    ("$5,000 sign-on bonus", None),
    ("$45,000 / hour", None), ("$18.50 annually", None),   # the stated unit disagrees with the band
    ("$70,000 - $80,000 (0.5 FTE)", None),             # S8: prorated annual
    ("To Be Determined", None), ("null", None), ("", None), (None, None),
    ("$9,000 - $10,000", None),                        # neither hourly nor annual band, no unit
    ("$4,094.50 - $5,094.16", None),                   # a month figure without its unit stays unread
])
def test_field_wage_rejects(value, unit):
    assert scraper.field_wage(value, unit) is None


def test_field_pay_label():
    pay = {"Pay Range:": "range", "Salary Range:": "range", "Starting Pay:": "range", "Compensation:": "range",
           "Minimum Hiring Rate": "min", "Maximum Hiring Rate": "max", "Salary Range Minimum:": "min",
           "Salary Range Maximum:": "max", "Hiring Range Minimum and Maximum Per Period": "range",
           "Compensation Detail": "range", "Budgeted Job Salary Range": "range", "Minimum Salary/Range*": "min",
           "Min Salary": "min", "Max Salary": "max", "Minimum Salary (Hourly Rate)": "min",
           "Hourly Salary Range": "range", "Pay": "range", "Salary": "range"}
    for label, side in pay.items():
        assert scraper.field_pay_label(label) == side, label
    for label in ("Payroll Job Title:", "Hours Per Pay Period:", "Hours / Pay Period", "Pay Range Statement",
                  "UKG Pay Rule", "Salary Admin Plan", "Pay Rate Frequency", "Pay Frequency", "Job Grade:",
                  "Salary Grade", "Sign On Bonus", "Weekly Hours:", "Hire In Rate", "Shift Differential",
                  "Job Code and Payroll Title", "Department:", ""):
        assert scraper.field_pay_label(label) is None, label


def test_field_wage_pair_and_labels():
    assert scraper.field_wage_pair("USD $43.79/Hr.", "USD $60.93/Hr.") == (43.79, 60.93, "hour")     # UHS tags7 / tags8
    assert scraper.field_wage_pair("32.2863", "34.2317") == (32.29, 34.23, "hour")                  # Northwell, 4 decimals
    assert scraper.field_wage_pair("91,400.0000", "158,100.0000") == (91400.0, 158100.0, "year")
    assert scraper.field_wage_pair("52.00", "52.00") == (52.0, 52.0, "hour")                        # UChicago single rate
    assert scraper.field_wage_pair("$0.00", "$0.00") is None
    assert scraper.field_wage_pair(None, None) is None
    # a range field outranks a min / max pair; a label's unit applies when the value names none
    assert scraper.field_wage_from_labels([("Minimum Salary (Hourly Rate)", "41.790000"),
                                           ("Maximum Salary (Hourly Rate)", "73.560000"),
                                           ("Salary Admin Plan", "RNS")]) == (41.79, 73.56, "hour")
    assert scraper.field_wage_from_labels([("Hours Per Pay Period:", "64"), ("Pay Range", "$25.00 - $26.37 / hourly")]) \
        == (25.0, 26.37, "hour")
    assert scraper.field_wage_from_labels([("Hourly Salary Range", "55.00 - 77.00")]) == (55.0, 77.0, "hour")
    # the unit a label states must agree with the band: an "hourly" field holding an annual figure is not read
    assert scraper.field_wage_from_labels([("Hourly Salary Range", "55,000 - 77,000")]) is None
    assert scraper.field_wage_from_labels([("Department", "$20 - $30"), ("Pay Range Statement", "$20 - $30")]) is None


# ── Jibe / iCIMS careers fronts: job.tagsN by label (BJC first) ─────────────

def test_jibe_bjc_tags7_pay_range(fixture_text):
    fx = _load(fixture_text, "jibe_tags.json")["BJC HealthCare"]
    labels = scraper._jibe_pay_labels(fx["labels_html"])
    assert labels == {"tags7": "Pay Range:"}
    got = [scraper._jibe_pay(row, labels) for row in fx["rows"]]
    assert got[0] == (17.5, 23.88, "hour")
    assert all(g and g[2] == "hour" for g in got) and len(got) == 3


@pytest.mark.parametrize("tenant,tags,first", [
    ("Universal Health Services", {"tags7": "Minimum Hiring Rate", "tags8": "Maximum Hiring Rate"}, (43.79, 60.93, "hour")),
    ("Yale New Haven Health System", {"tags9": "Salary Range:"}, (35.0, 79.07, "hour")),
    ("Trilogy Health Services", {"tags3": "Starting Pay:"}, None),
    ("Fairview Health", {"tags8": "Compensation:"}, (37.67, 54.42, "hour")),
    ("UCI Health", {"tags2": "Salary Range Minimum:", "tags4": "Salary Range Maximum:"}, None),
])
def test_jibe_pay_labels_by_tenant(fixture_text, tenant, tags, first):
    fx = _load(fixture_text, "jibe_tags.json")[tenant]
    labels = scraper._jibe_pay_labels(fx["labels_html"])
    assert labels == tags
    got = [scraper._jibe_pay(row, labels) for row in fx["rows"]]
    assert any(got), tenant
    assert all(g is None or (g[2] in ("hour", "year") and g[0] <= g[1]) for g in got)
    if first:
        assert got[0] == first


@pytest.mark.parametrize("tenant", ["Tower Health", "Care New England Health System", "Tanner Health", "Novant Health"])
def test_jibe_tags_that_are_not_pay(fixture_text, tenant):
    """Tower's "Hours Per Pay Period:", Care New England's "Weekly Hours:"
    (tags7), Tanner's "Sign On Bonus", Novant's "Job Opening ID" (tags7)."""
    fx = _load(fixture_text, "jibe_tags.json")[tenant]
    labels = scraper._jibe_pay_labels(fx["labels_html"])
    assert labels == {}
    assert all(scraper._jibe_pay(row, labels) is None for row in fx["rows"])


def test_jibe_label_conflict_across_languages():
    html = ('{"JOB_DESCRIPTION":{"TAGS3":"Turno"}} {"JOB_DESCRIPTION":{"TAGS3":"Shift","TAGS7":"Minimum Hiring Rate",'
            '"TAGS8":"Maximum Hiring Rate"}} {"JOB_DESCRIPTION":{"TAGS5":"Salario","TAGS6":"Pay Range:"}}'
            ' {"JOB_DESCRIPTION":{"TAGS6":"Hours Per Pay Period:"}}')
    assert scraper._jibe_pay_labels(html) == {"tags7": "Minimum Hiring Rate", "tags8": "Maximum Hiring Rate"}


def test_scrape_jibe_sets_field_pay(monkeypatch, fixture_text):
    fx = _load(fixture_text, "jibe_tags.json")["BJC HealthCare"]
    rows = [{"data": {"req_id": f"10{i}", "title": "Patient Access Rep", "city": "Saint Louis", "state": "Missouri",
                      "description": "<p>Duties</p>", "meta_data": {"canonical_url": f"https://jobs.bjc.org/jobs/10{i}"},
                      **row}} for i, row in enumerate(fx["rows"])]
    rows.append({"data": {"req_id": "199", "title": "Courier", "city": "Saint Louis", "state": "Missouri",
                          "description": "Pay: $19.00 per hour", "tags2": ["Days"]}})

    class _Resp:
        status = 200

        async def json(self, content_type=None):
            return {"jobs": rows, "totalCount": len(rows)}

    class _Ctx:
        async def __aenter__(self):
            return _Resp()

        async def __aexit__(self, *a):
            return False

    async def fake_html(session, url, timeout=20):
        assert url == "https://jobs.bjc.org/jobs"
        return fx["labels_html"]

    monkeypatch.setattr(scraper, "req", lambda *a, **k: _Ctx())
    monkeypatch.setattr(scraper, "_fetch_html", fake_html)
    jobs = {j.job_id: j for j in asyncio.run(scraper.scrape_jibe(None, "BJC HealthCare", "https://jobs.bjc.org"))}
    assert (jobs["100"].wage_min, jobs["100"].wage_max, jobs["100"].wage_unit) == (17.5, 23.88, "hour")
    assert jobs["199"].wage_min is None        # no pay tag: the body parse decides in normalize_job
    d = scraper.normalize_job(jobs["100"])
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (17.5, 23.88, "hour")
    d2 = scraper.normalize_job(jobs["199"])
    assert (d2["wage_min"], d2["wage_unit"]) == (19.0, "hour")


# ── Oracle HCM requisition flex fields ──────────────────────────────────────

ORACLE_WANT = {
    "TenetHealthcare": [(30.6, 48.8, "hour"), None, None],
    "BrookdaleSeniorLiving": [(26.37, 39.55, "hour"), None, (17.31, 25.96, "hour")],
    "MayoClinic": [(123427.2, 172244.8, "year"), (31.49, 47.25, "hour"), (71510.4, 107390.4, "year")],
    "AdventistHealth": [(20.41, 32.97, "hour"), (58.02, 86.97, "hour"), (25.0, 29.56, "hour")],
    "UWHealth": [(20.92, 29.3, "hour")] * 3,
    "HealthPartners": [(21.8, 28.69, "hour"), (22.3, 31.24, "hour"), (22.3, 31.24, "hour")],
    "UCSFHealth": [(93.47, 121.11, "hour"), (93.47, 121.11, "hour"), (225000.0, 250000.0, "year")],
    "InovaHealthSystem": [(54.25, 69.25, "hour"), (20.3, 33.09, "hour"), (32.3, 52.65, "hour")],
    "InspiraHealthNetwork": [(33.11, 47.28, "hour"), (33.11, 47.28, "hour"), (27.01, 38.58, "hour")],
    "LomaLindaUniversityHealth": [(25.0, 26.37, "hour"), (35.28, 47.45, "hour"), (25.0, 26.37, "hour")],
    "Unknownfaeyip": [(62.33, 62.33, "hour"), (55.0, 77.0, "hour"), (67.76, 94.86, "hour")],
    "EasternConnecticutHealth": [(29.16, 44.47, "hour"), (22.71, 33.18, "hour"), (41.03, 65.7, "hour")],
    "ProvidenceHealth": [(21.16, 32.37, "hour"), (44.35, 68.86, "hour"), (28.11, 43.0, "hour")],
    "NorthwellHealth": [(32.29, 34.23, "hour"), (91400.0, 158100.0, "year"), (41780.0, 64340.0, "year")],
    "CedarsSinai": [(82.0, 92.0, "hour"), (23.18, 34.77, "hour"), (23.18, 34.77, "hour")],
    "UnitedRegional": [(55.62, 55.62, "hour"), (41.79, 73.56, "hour"), (41.79, 73.56, "hour")],
    "UChicagoMedicine": [(52.0, 52.0, "hour"), (56.75, 56.75, "hour"), (56.75, 56.75, "hour")],
}


def test_oracle_flex_pay_by_prompt(fixture_text):
    fx = _load(fixture_text, "oracle_flex.json")
    for tenant, want in ORACLE_WANT.items():
        got = [scraper._oracle_pay(it) for it in fx[tenant]]
        got += [None] * (len(want) - len(got))
        assert got == want, tenant


def test_oracle_flex_without_pay_reads_nothing(fixture_text):
    """Tenants of the sweep whose flex fields carry no pay (Lifepoint,
    WellSpan, INTEGRIS, TriHealth, UC Health, Baptist Pensacola): FTE,
    position type, hours and pay rules are never read as pay."""
    fx = _load(fixture_text, "oracle_flex.json")
    for tenant in set(fx) - set(ORACLE_WANT):
        assert all(scraper._oracle_pay(it) is None for it in fx[tenant]), tenant


def test_oracle_detail_field_first(monkeypatch, fixture_text):
    it = dict(_load(fixture_text, "oracle_flex.json")["HealthPartners"][0])
    it["ExternalDescriptionStr"] = "<p>" + "Care for patients on a busy unit. " * 60 + "Pay: $99.00 per hour</p>"

    class _Resp:
        status = 200

        async def json(self, content_type=None):
            return {"items": [it]}

    class _Ctx:
        async def __aenter__(self):
            return _Resp()

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(scraper, "req", lambda *a, **k: _Ctx())
    job = _job(ats_platform="Oracle HCM")
    assert asyncio.run(scraper._oracle_detail(None, "https://x.fa.oraclecloud.com", "CX_1", job))
    assert (job.wage_min, job.wage_max, job.wage_unit) == (21.8, 28.69, "hour")
    d = scraper.normalize_job(job)
    assert d["wage_min"] == 21.8 and d["posting_facts"]["pay_src"] == "field"


# ── HealthcareSource search hits ────────────────────────────────────────────

def test_hcs_pay_fields(fixture_text):
    fx = _load(fixture_text, "hcs_userarea.json")
    assert scraper._hcs_pay(fx["RenownHealth"][0]) == (18.24, 25.53, "hour")
    assert scraper._hcs_pay(fx["CHCHealthcare"][0]) == (13.58, 20.37, "hour")
    assert scraper._hcs_pay(fx["HolyokeHealth"][0]) == (22.0, 29.25, "hour")
    assert scraper._hcs_pay(fx["PacificaHospitaloftheValley"][2]) == (20.26, 23.7, "hour")
    assert any(scraper._hcs_pay(ua) for ua in fx["LawrenceGeneral"])      # posting field "Salary Range"
    assert all(scraper._hcs_pay(ua) is None for ua in fx["WillisKnightonHealthSystem"])   # $0.00 placeholder


def test_hcs_never_reads_requisition_fields():
    ua = {"salaryRange": "", "customJobPostingFieldValues": {},
          "customRequisitionFieldValues": {"1": [{"name": "Salary Range", "value": "$30.00 - $40.00"}]}}
    assert scraper._hcs_pay(ua) is None
    hit = {"_source": {"title": "RN", "userArea": {"jobPostingID": 7, "salaryRange": "$30.00 - $40.00 hourly",
                                                    "jobSummaryDisplay": "<p>Body</p>"},
                       "jobLocation": {"address": {"addressLocality": "Reno", "addressRegion": "NV"}}}}
    job = scraper._hcs_job(hit, "Renown Health", "renown")
    assert (job.wage_min, job.wage_max, job.wage_unit) == (30.0, 40.0, "hour")


# ── SmartRecruiters ─────────────────────────────────────────────────────────

def test_sr_compensation_and_custom_fields(fixture_text):
    fx = _load(fixture_text, "sr_postings.json")
    assert scraper._sr_pay(fx[0]) == (20.25, 26.33, "hour")
    assert scraper._sr_pay({"customField": fx[1]["customField"]}) == (34.5, 55.2, "hour")   # list rows: custom fields only
    assert scraper._sr_pay({"compensation": {"min": 90000, "max": 120000, "currency": "USD", "period": "YEARLY"}}) \
        == (90000.0, 120000.0, "year")
    assert scraper._sr_pay({"compensation": {"min": 25, "max": 30, "currency": "EUR", "period": "HOURLY"}}) is None
    assert scraper._sr_pay({"compensation": {"min": 25, "max": 30, "currency": "USD", "period": "YEARLY"}}) is None
    assert scraper._sr_pay({}) is None


# ── Findly / Google careers fronts ──────────────────────────────────────────

def test_findly_pay_keys_are_configured_not_guessed():
    assert scraper.FINDLY_GOOGLE_PAY_KEY == {"UPMC": "salary", "University Hospitals": "relocation"}
    assert "UT Southwestern Medical Center" not in scraper.FINDLY_GOOGLE_PAY_KEY   # hidden figure, page says commensurate
    assert scraper.field_wage("$ 138.66-201.05 USD") == (138.66, 201.05, "hour")
    assert scraper.field_wage("$58,614 – $91,666 /year") == (58614.0, 91666.0, "year")


# ── PeopleSoft, iCIMS classic, custom fronts ────────────────────────────────

def test_peoplesoft_pay_fields(fixture_text):
    snips = _load(fixture_text, "page_snippets.json")
    assert scraper._ps_pay(snips["nyc_hh"][0]) == (83295.0, 90000.0, "year")
    assert all(scraper._ps_pay(h) for h in snips["nyc_hh"])
    assert scraper._ps_pay(snips["queens"][0]) == (171662.0, 187473.0, "year")
    # "Hire In Rate" alone is not the posted range
    hire_only = snips["nyc_hh"][0].split("HHC_HRS_JO_WRK_HRS_JO_MIN_RT")[0]
    assert scraper._ps_pay(hire_only) is None


def test_icims_header_salary_range(fixture_text):
    snips = _load(fixture_text, "page_snippets.json")
    got = [scraper.field_wage_from_labels(list(scraper._icims_header(h).items())) for h in snips["ohsu"]]
    assert got == [(55536.0, 92560.0, "year"), (55.24, 92.02, "hour"), (55.24, 92.02, "hour")]


def test_infor_formatted_salary(fixture_text):
    vals = _load(fixture_text, "infor_pay.json")
    got = {v: scraper.field_wage(v) for v in vals}
    assert got["16.01 USD per hour"] == (16.01, 16.01, "hour")
    assert [v for v, g in got.items() if g] == ["16.01 USD per hour"]
    job = _job(ats_platform="Infor")
    scraper._infor_apply_detail(job, {"fields": {
        "_op_FormattedSalaryRangeAmountWithCurrencyCodeAndPayRate_spc_translation_cp_": {"value": "16.01 USD per hour"}}})
    assert (job.wage_min, job.wage_max, job.wage_unit) == (16.01, 16.01, "hour")
    job2 = _job(ats_platform="Infor")
    scraper._infor_apply_detail(job2, {"fields": {
        "_op_FormattedSalaryRangeAmountWithCurrencyCodeAndPayRate_spc_translation_cp_": {"value": "0 - 0  per hour"}}})
    assert job2.wage_min is None


def test_custom_fronts(fixture_text):
    snips = _load(fixture_text, "page_snippets.json")
    # University of Michigan Health: the site's "pay" regex on the Drupal page
    rx = scraper.HTML_LIST_SITES["University of Michigan Health"]["pay"]
    import re
    m = re.search(rx, snips["umich"][0], re.S)
    assert m and scraper.field_wage(scraper._hl_text(m.group(1))) == (93464.0, 135674.0, "year")
    # NY State Jobs vacancy rows
    body, et, pay = scraper._nysj_posting(snips["nys"][0])
    assert pay == (42641.0, 52413.0, "year") and et == "Full-Time"
    # Asante (Jobvite Engage header)
    assert scraper._talemetry_pay(snips["asante"][1]) == (21.86, 28.23, "hour")
    assert scraper._talemetry_pay(snips["asante"][2]) == (73.98, 101.73, "hour")
    # TX HHS (RMK body header, unit from "Pay Frequency")
    html = snips["tx_hhs"][0] + '</span></span></div></div><p class="job-location">Rusk, TX</p>'
    body, pay = scraper._rmk_posting(html)
    assert "Salary Range:" in body and pay == (49134.0, 61129.92, "year")
    # Harris: the list row's custom field
    assert scraper.field_wage_from_labels([("Salary", "$20.87 - $26.03")]) == (20.87, 26.03, "hour")


# ── pay_src in posting_facts, and the pay re-read slot ──────────────────────

def test_pay_src_marks_field_and_text():
    body = "Registered Nurse caring for patients on a busy unit. " * 40
    j = _job(description=body + "\nPay: $40.00 - $50.00 per hour")
    scraper.set_field_wage(j, (35.0, 79.07, "hour"))
    d = scraper.normalize_job(j)
    assert (d["wage_min"], d["wage_max"]) == (35.0, 79.07)          # the field wins over the body
    assert d["posting_facts"]["pay_src"] == "field"
    d2 = scraper.normalize_job(_job(description=body + "\nPay: $40.00 - $50.00 per hour"))
    assert d2["wage_min"] == 40.0 and d2["posting_facts"]["pay_src"] == "text"
    d3 = scraper.normalize_job(_job(description=body))
    assert d3["wage_min"] is None and "pay_src" not in (d3["posting_facts"] or {})
    # a blank body (stored body kept by the trigger) keeps posting_facts null even with a field pay
    j4 = _job(description="")
    scraper.set_field_wage(j4, (20.0, 25.0, "hour"))
    d4 = scraper.normalize_job(j4)
    assert d4["wage_min"] == 20.0 and d4["posting_facts"] is None
    assert "pay_src" not in d4                                       # never a column of its own


def test_set_field_wage_keeps_an_adapter_value():
    j = _job()
    j.wage_min, j.wage_max, j.wage_unit = 30.0, 40.0, "hour"
    assert not scraper.set_field_wage(j, (10.0, 12.0, "hour"))
    assert j.wage_min == 30.0
    assert not scraper.set_field_wage(_job(), None)


def test_pay_reread_kind(monkeypatch):
    sysname = "Tenet Healthcare"
    rows = [{"hospital_system": sysname, "job_id": str(i), "desc_len": 4000, "fv": scraper.FACTS_VERSION,
             "wage_min": None} for i in range(70)]
    rows.append({"hospital_system": sysname, "job_id": "priced", "desc_len": 4000, "fv": scraper.FACTS_VERSION,
                 "wage_min": 30.0})
    rows.append({"hospital_system": "Some Workday System", "job_id": "w1", "desc_len": 4000,
                 "fv": scraper.FACTS_VERSION, "wage_min": None})
    rows.append({"hospital_system": sysname, "job_id": "nokey", "desc_len": 4000, "fv": scraper.FACTS_VERSION})
    try:
        scraper.set_known_bodies(rows)
        # over one PAY_REREAD_DAYS cycle every unpriced stored body is due exactly once
        due = {str(i): 0 for i in range(70)}
        for day in range(scraper.PAY_REREAD_DAYS):
            monkeypatch.setattr(scraper, "_run_day", lambda d=day: 700000 + d)
            kinds = {i: scraper._known_kind(sysname, _job(job_id=i, hospital_system=sysname)) for i in due}
            assert set(kinds.values()) <= {"pay", "known"}
            for i, k in kinds.items():
                due[i] += k == "pay"
        assert set(due.values()) == {1}
        assert scraper._known_kind(sysname, _job(job_id="priced", hospital_system=sysname)) == "known"
        assert scraper._known_kind("Some Workday System", _job(job_id="w1")) == "known"
        assert scraper._known_kind(sysname, _job(job_id="nokey", hospital_system=sysname)) == "known"
        monkeypatch.setattr(scraper, "PAY_REREAD_DAYS", 0)
        assert all(scraper._known_kind(sysname, _job(job_id=str(i))) == "known" for i in range(70))
    finally:
        scraper.set_known_bodies([])


def test_pay_reread_candidates_order(monkeypatch):
    sysname = "Tenet Healthcare"
    monkeypatch.setattr(scraper, "PAY_REREAD_DAYS", 1)          # every unpriced stored body is due tonight
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    rows = [{"hospital_system": sysname, "job_id": f"p{i}", "desc_len": 4000, "fv": scraper.FACTS_VERSION,
             "wage_min": None} for i in range(3)]
    rows += [{"hospital_system": sysname, "job_id": "cut", "desc_len": 7995, "fv": None, "wage_min": None}]
    try:
        scraper.set_known_bodies(rows)
        jobs = [_job(job_id=f"p{i}", hospital_system=sysname) for i in range(3)]
        jobs += [_job(job_id="new1", hospital_system=sysname), _job(job_id="cut", hospital_system=sysname)]
        budget = scraper._DescBudget(100)
        held = []
        cands, known, dup = scraper._detail_candidates(sysname, jobs, budget, held=held)
        ids = [j.job_id for j in cands]
        assert ids[0] == "new1" and ids[-1] == "cut" and set(ids[1:4]) == {"p0", "p1", "p2"}
        assert sorted(k for _, _, k in held) == ["cut", "pay", "pay", "pay"]
        assert "3 unpriced stored bodies queued for their pay field" in scraper._held_note(held)
    finally:
        scraper.set_known_bodies([])


def test_pay_reread_systems_are_detail_sources():
    """Every system on the re-read list is crawled by a detail pass that
    reads a pay field; list-level sources (Jibe, HealthcareSource,
    SmartRecruiters, Findly, Harris) reach every row nightly and are not on it."""
    for s in ("BJC HealthCare", "Universal Health Services", "UPMC", "Northwestern Medicine", "Renown Health",
              "Harris Health System"):
        assert s not in scraper.PAY_REREAD_SYSTEMS
    assert len(scraper.PAY_REREAD_SYSTEMS) == 21
    for p in ("SuccessFactorsRMK", "NYStateJobs"):
        assert p in scraper.KNOWN_BODY_PLATFORMS
