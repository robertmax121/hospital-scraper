"""2026-10-29 (facility scale, phase 4; owner: "trinity health not done either"):
the second look at the systems phase 3 left on the system name
(facilities_trinity_stays): Trinity Health's bare-town, text, campus and site
rows, Sanford's Good Samaritan Society communities, Gentiva's and Elara's
named branches.

Fixtures, no network, no database:
  tests/fixtures/trinitystays1029/stored_rows.json  34 real postings as the
      database held them on 2026-10-29 (title, link, town, the first 6,000
      characters of the body; emails and phone numbers replaced) with what
      the branch writes, or null when the row keeps the system name.
"""
import json
import os

import pytest

import facilities_town as T
import facilities_trinity_stays as F
import facilities_workday as W
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "trinitystays1029", "stored_rows.json")
ROWS = json.load(open(FIX, encoding="utf-8"))["rows"]


@pytest.fixture(autouse=True)
def _no_stored_names():
    T.set_stored([])
    T._STORED_LOADED[0] = True       # no PostgREST read in tests
    yield
    T.set_stored([])
    T._STORED_LOADED[0] = False


def _job(r, name=None):
    return Job(title=r["title"], hospital_system=r["hospital_system"], hospital_name=name or r["hospital_system"],
               city=r["city"], state=r["state"], location=f"{r['city']}, {r['state']}" if r["city"] else r["state"],
               specialty="", job_type="", url=r["url"], job_id=r["job_id"], posted_date="",
               description=r["description"], ats_platform="Workday")


def _decide_row(r):
    """What the scraper writes for one stored label row: the Workday list
    hook's site table (Trinity), the phase 4 pass, then the town tables."""
    job = _job(r)
    if r["hospital_system"] == F.TRINITY:
        site = W.facility(F.TRINITY, "", r["url"])
        if site:
            job.hospital_name, job.city, job.state = site
    F.apply_jobs([job])
    T.apply_jobs([job])
    return job


@pytest.mark.parametrize("r", ROWS, ids=[f"{r['case']}-{r['id']}" for r in ROWS])
def test_stored_rows(r):
    job = _decide_row(r)
    if r["expect"] is None:
        assert job.hospital_name == r["hospital_system"], (job.hospital_name, job.city, job.state)
    else:
        name, city, state = r["expect"]
        assert job.hospital_name == name
        assert (job.state or "") == state
        if city:
            assert job.city == city


def test_every_case_kind_present():
    kinds = {r["case"] for r in ROWS}
    for k in ("trin_town", "trin_text_hc", "trin_text_ga", "trin_campus", "trin_sitetable", "trin_sitename",
              "trin_sitestate", "trin_stay_site", "trin_stay_care", "san_town", "san_named", "san_listed",
              "gentiva", "elara"):
        assert k in kinds


# ── Trinity: site kinds ────────────────────────────────────────────────────
@pytest.mark.parametrize("seg,city,state,want", [
    ("Silver-Spring-Maryland", "Silver Spring", "MD", ("Silver Spring", "MD")),
    ("Davenport-IA", "Davenport", "IA", ("Davenport", "IA")),
    ("Grand-Rapids-MI", "Grand Rapids", "MI", ("Grand Rapids", "MI")),
    ("Langhorne", "Langhorne", "PA", ("Langhorne", "PA")),
    ("Saint-Alphonsus-Clinic---Boise-Idaho", "Boise", "ID", None),
    ("SPNY---63-Shaker-Rd-Albany", "Albany", "NY", None),
    ("Muskegon-Michigan", "Muskegon", "MI", ("Muskegon", "MI")),
])
def test_bare_town(seg, city, state, want):
    assert F.bare_town(seg, city, state) == want


def test_site_kind_region_and_campus():
    base = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/{}/X_001"
    assert F.site_kind(base.format("St-Marys-Health-Care-System-Georgia"), "Athens", "GA") == ("region", "GA")
    kind, info = F.site_kind(base.format("Saint-Joseph-Mercy-Health-System-Hospital-Campus---Brighton-Mi"))
    assert kind == "campus" and info == ("Trinity Health Livingston Hospital", "Brighton", "MI")
    assert F.site_kind(base.format("Saint-Alphonsus-Clinic---Boise-Idaho"), "Boise", "ID")[0] == "site"


def test_text_ignores_a_sentence_naming_two_hospitals():
    d = ("Founded in 1963 by the Sisters of the Holy Cross, Holy Cross Health is a member of Trinity Health. "
         "Holy Cross Hospital, in Silver Spring, is one of the largest hospitals in Maryland, and Holy Cross "
         "Germantown Hospital is the first hospital in the nation built on a community college campus.")
    assert F.named_in_text("Registered Nurse", d, "MD") == []
    d2 = "Holy Cross Germantown Hospital, a member of Trinity Health, is seeking a nurse for its ICU."
    assert F.named_in_text("Registered Nurse", d2, "MD") == ["Holy Cross Germantown Hospital"]


def test_text_reads_only_the_rows_state():
    d = "Holy Cross Hospital, a member of Trinity Health, is seeking an RN for its emergency department."
    assert F.named_in_text("RN", d, "FL") == ["Holy Cross Hospital"]
    assert F.named_in_text("RN", d, "MD") == ["Holy Cross Hospital"]
    assert F.named_in_text("RN", d, "MI") == []


def test_ministry_name_followed_by_a_non_hospital_part_names_no_hospital():
    d = "Increase philanthropic support for the Trinity Health Grand Rapids Foundation and its donors this year."
    assert F.named_in_text("Gift Processor", d, "MI") == []
    d2 = "Join the Trinity Health Grand Rapids team on our 6th floor medical unit as an RN."
    assert F.named_in_text("RN", d2, "MI") == ["Trinity Health Grand Rapids Hospital"]


def test_text_names_the_hospital_in_another_town_keeps_the_label():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Germantown-Maryland/RN_00123"
    d = ("Holy Cross Hospital, a member of Trinity Health, is seeking an experienced operating room nurse "
         "to join its surgical team. " * 3)
    name, city, state, how = F.decide_trinity("Trinity Health", "Germantown", "MD", "Registered Nurse- OR", d, url)
    assert name is None and how == "text-elsewhere"


def test_clinic_site_keeps_the_label_and_skips_the_town_rule():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Saint-Alphonsus-Clinic---Boise-Idaho/RN_001"
    job = Job(title="Registered Nurse - Family Medicine", hospital_system="Trinity Health",
              hospital_name="Trinity Health", city="Boise", state="ID", location="Boise, ID", specialty="",
              job_type="", url=url, job_id="T1", posted_date="", description="x" * 1200, ats_platform="Workday")
    F.apply_jobs([job])
    assert getattr(job, F.SKIP_ATTR, False) is True
    T.apply_jobs([job])
    assert job.hospital_name == "Trinity Health" and job.city == "Boise"


def test_bare_town_hospital_posting_takes_the_towns_hospital():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Muskegon-Michigan/RN_002"
    body = ("Employment Type: Full time Shift: 12 Hour Night Shift Description: Registered Nurse for our "
            "5th floor intensive care unit. Provides direct patient care to critically ill adults. " * 8)
    job = Job(title="Registered Nurse ( RN ) - 5th Floor ICU", hospital_system="Trinity Health",
              hospital_name="Trinity Health", city="Muskegon", state="MI", location="Muskegon, MI", specialty="",
              job_type="", url=url, job_id="T2", posted_date="", description=body, ats_platform="Workday")
    F.apply_jobs([job])
    T.apply_jobs([job])
    assert job.hospital_name == "Trinity Health Muskegon Hospital"


def test_no_body_night_keeps_the_stored_name():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Muskegon-Michigan/RN_003"
    T.set_stored([{"hospital_system": "Trinity Health", "job_id": "T3",
                   "hospital_name": "Trinity Health Muskegon Hospital", "city": "Muskegon", "state": "MI"}])
    job = Job(title="Registered Nurse", hospital_system="Trinity Health", hospital_name="Trinity Health",
              city="Muskegon", state="MI", location="Muskegon, MI", specialty="", job_type="", url=url,
              job_id="T3", posted_date="", description="", ats_platform="Workday")
    assert F.apply_jobs([job]) == {"Trinity Health": {"kept": 1}}
    T.apply_jobs([job])
    assert job.hospital_name == "Trinity Health Muskegon Hospital"


def test_named_site_without_a_town_takes_its_state_only_from_the_posting():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Holy-Cross-Home-Care-and-Hospice/RN_004"
    with_state = "Holy Cross Home Care and Hospice serves families across Montgomery County, Maryland. " * 20
    no_state = "Holy Cross Home Care and Hospice serves families in their homes with skilled nursing care. " * 20
    got = F.decide_trinity("Trinity Health", "", "", "Home Health RN", with_state, url)
    assert got[:3] == ("Holy Cross Home Care and Hospice", "", "MD")
    got = F.decide_trinity("Trinity Health", "", "", "Home Health RN", no_state, url)
    assert got[:3] == ("Holy Cross Home Care and Hospice", "", "")


def test_posting_state_ignores_trinitys_own_livonia_michigan():
    d = ("Holy Cross Health is a member of Trinity Health of Livonia, Michigan. "
         "Our nurses serve patients in Silver Spring, Maryland.")
    assert F.posting_state(d) == "MD"
    assert F.posting_state("We serve New Jersey and North Carolina.") == ""


def test_site_table_additions_place_their_sites():
    assert W.facility("Trinity Health", "", "https://x/Jobs/job/SPNY---855-Rt-146-Clifton-Park/A_1") == \
        ("Trinity Health", "Clifton Park", "NY")
    assert W.facility("Trinity Health", "", "https://x/Jobs/job/Athens-Home-Care--Hospice/A_2") == \
        ("St. Mary's Home Care & Hospice", "Athens", "GA")
    assert W.facility("Trinity Health", "", "https://x/Jobs/job/NESM---Saint-Marys-Hospital/A_3") == \
        ("Saint Mary's Hospital", "Waterbury", "CT")


def test_town_spec_registered_and_livonia_kept():
    spec = T.SPECS["Trinity Health"]
    assert spec.town("Silver Spring", "MD")[0] == "Holy Cross Hospital"
    assert spec.town("Ft. Lauderdale", "FL")[0] == "Holy Cross Hospital"
    assert spec.town("Brighton", "MI")[0] == "Trinity Health Livingston Hospital"
    assert spec.town("Livonia", "MI") is None                       # kept: the system office's town
    assert ("albany", "NY") in spec.shared                          # St. Peter's and Albany Memorial


# ── Sanford: Good Samaritan Society communities ───────────────────────────
def _gss(head, city, state, title="CNA - Full Time"):
    d = ("Sanford Health, the largest rural health system in the United States. Department Details " + head +
         " Summary The nursing assistant provides care. " * 30)
    return F.gss_community(title, d, city, state)


@pytest.mark.parametrize("head,city,state,want", [
    ("Good Samaritan in Tyndall offers community fellowship.", "Tyndall", "SD", "Good Samaritan Society - Tyndall"),
    ("At Villisca Good Samaritan, our vision is to provide quality care.", "Villisca", "IA",
     "Good Samaritan Society - Villisca"),
    ("At Good Samaritan, Atkinson, our vision is to provide quality care.", "Atkinson", "NE",
     "Good Samaritan Society - Atkinson"),
    ("Good Samaritan Society - Mandan Sunset is hiring.", "Mandan", "ND", "Good Samaritan Society - Mandan Sunset"),
    ("Good Samaritan - Bethany is a skilled nursing community.", "Brainerd", "MN", "Good Samaritan Society - Bethany"),
    ("At St. Vincent's, Good Samaritan, Bismarck, ND, our vision is quality.", "Bismarck", "ND",
     "Good Samaritan Society - St. Vincent's"),
])
def test_gss_named(head, city, state, want):
    assert _gss(head, city, state)[0] == want


def test_gss_other_town_or_several_or_none_keeps_the_label():
    assert _gss("Good Samaritan Albion offers care.", "Grand Island", "NE")[0] is None
    assert _gss("Seeking a PT to work with our Good Samaritan locations in Waukon and West Union, IA.",
                "Waukon", "IA")[0] is None
    assert _gss("Good Samaritan is proud to be one of the largest non-profit providers.", "Indianola", "IA")[0] is None
    assert _gss("Good Samaritan in Tyndall offers fellowship.", "Tyndall", "SD",
                title="CNA - Local Traveler")[0] is None


def test_gss_no_body_night_keeps_the_stored_community():
    T.set_stored([{"hospital_system": "Sanford Health", "job_id": "S1",
                   "hospital_name": "Good Samaritan Society - Tyndall", "city": "Tyndall", "state": "SD"}])
    job = Job(title="RN", hospital_system="Sanford Health", hospital_name="Sanford Health", city="Tyndall",
              state="SD", location="Tyndall, SD", specialty="", job_type="", url="https://sanford/x", job_id="S1",
              posted_date="", description="", ats_platform="Workday")
    F.apply_jobs([job])
    T.apply_jobs([job])
    assert job.hospital_name == "Good Samaritan Society - Tyndall"


# ── Gentiva and Elara branches ─────────────────────────────────────────────
def test_branch_names():
    d = ("Location: Heartland Hospice Branch, 3975 Fair Ridge Dr, Suite 125S, Fairfax, VA 22033 Date: November 9th. "
         + "We are hiring RNs. " * 80)
    assert F.branch_name("Gentiva", "RN Hiring Event", d, "Fairfax", "VA") == ("Heartland Hospice - Fairfax", "branch")
    assert F.branch_name("Gentiva", "RN Hiring Event", d, "Reston", "VA")[0] is None
    e = "Branch: Kansas City, Missouri | Urgently Hiring Territory: Kansas City and surrounding areas. " * 15
    assert F.branch_name("Elara Caring", "LPN", e, "Kansas City", "MO") == ("Elara Caring - Kansas City", "branch")


def test_branch_no_body_night_keeps_the_stored_branch():
    T.STORED[("Gentiva", "G1")] = ("Heartland Hospice - Fairfax", "Fairfax", "VA")
    job = Job(title="RN", hospital_system="Gentiva", hospital_name="Gentiva", city="Fairfax", state="VA",
              location="Fairfax, VA", specialty="", job_type="", url="https://g/x", job_id="G1", posted_date="",
              description="", ats_platform="iCIMS")
    assert F.apply_jobs([job]) == {"Gentiva": {"kept": 1}}
    assert job.hospital_name == "Heartland Hospice - Fairfax"


def test_a_row_that_raises_keeps_its_name(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen")
    monkeypatch.setattr(F, "decide_trinity", boom)
    job = Job(title="RN", hospital_system="Trinity Health", hospital_name="Trinity Health", city="Muskegon",
              state="MI", location="Muskegon, MI", specialty="", job_type="", url="https://x/Jobs/job/Muskegon-MI/a",
              job_id="E1", posted_date="", description="y" * 1500, ats_platform="Workday")
    assert F.apply_jobs([job]) == {"Trinity Health": {"error": 1}}
    assert (job.hospital_name, job.city, job.state) == ("Trinity Health", "Muskegon", "MI")


def test_finalize_jobs_runs_the_phase4_pass(monkeypatch):
    monkeypatch.setenv("FINALIZE_WORKERS", "1")
    r = next(x for x in ROWS if x["case"] == "trin_text_hc")
    out = scraper.finalize_jobs([_job(r)])
    assert out and out[0]["hospital_name"] == r["expect"][0]


def test_no_body_new_row_on_a_clinic_or_region_site_keeps_the_label():
    for seg, town, st in (("Saint-Joseph-Mercy-Health-System-Primary-Care---Specialty-Care---Pontiac-Mi", "Pontiac", "MI"),
                          ("St-Marys-Health-Care-System-Georgia", "Athens", "GA")):
        job = Job(title="Area Cleaner", hospital_system="Trinity Health", hospital_name="Trinity Health", city=town,
                  state=st, location=f"{town}, {st}", specialty="", job_type="",
                  url=f"https://x/Jobs/job/{seg}/A_9", job_id="N" + st, posted_date="", description="short teaser",
                  ats_platform="Workday")
        F.apply_jobs([job])
        T.apply_jobs([job])
        assert job.hospital_name == "Trinity Health", seg


def test_no_body_named_site_keeps_its_name_and_fixed_state():
    job = Job(title="Home Care RN", hospital_system="Trinity Health", hospital_name="Trinity Health",
              city="", state="", location="", specialty="", job_type="",
              url="https://x/Jobs/job/Trinity-Health-Of-New-England-At-Home---CT/A_8", job_id="N2", posted_date="",
              description="", ats_platform="Workday")
    assert F.apply_jobs([job]) == {"Trinity Health": {"site-name": 1}}
    assert (job.hospital_name, job.state) == ("Trinity Health Of New England At Home", "CT")


def test_no_body_stored_state_fix_is_kept():
    T.set_stored([{"hospital_system": "Trinity Health", "job_id": "N3", "hospital_name": "Trinity Health",
                   "city": "Albany", "state": "NY"}])
    job = Job(title="Registration Associate", hospital_system="Trinity Health", hospital_name="Trinity Health",
              city="Albany", state="", location="Albany", specialty="", job_type="",
              url="https://x/Jobs/job/SPNY---63-Shaker-Rd-Albany/A_7", job_id="N3", posted_date="", description="",
              ats_platform="Workday")
    F.apply_jobs([job])
    T.apply_jobs([job])
    assert (job.hospital_name, job.city, job.state) == ("Trinity Health", "Albany", "NY")


def test_staffing_center_float_keeps_the_label():
    url = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/Grand-Rapids-Michigan/RN_010"
    body = "Float Registered Nurse for the clinical staffing center, caring for adults on inpatient units. " * 15
    job = Job(title="Float Registered Nurse - Clinical Staffing Center", hospital_system="Trinity Health",
              hospital_name="Trinity Health", city="Grand Rapids", state="MI", location="Grand Rapids, MI",
              specialty="", job_type="", url=url, job_id="P1", posted_date="", description=body,
              ats_platform="Workday")
    assert F.apply_jobs([job]) == {"Trinity Health": {"multi-site": 1}}
    T.apply_jobs([job])
    assert job.hospital_name == "Trinity Health"
