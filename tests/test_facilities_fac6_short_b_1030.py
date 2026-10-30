"""2026-10-30 (facility scale, phase 6 group B): the label rows the 10-30 nightly
left behind on TMC Healthcare, Luminis, UC Health, Loma Linda, LifeBridge,
Legacy, Texas Children's and Mosaic, Health First's feed codes and two Brown
Workday sites (facilities_fac6_short_b, its hooks in scraper.py, the Loma Linda
town table in facilities_town).

Fixture (tests/fixtures/facility_fac6_short_b_1030/rows.json): real stored
label rows read 2026-10-30 after the nightly, and excerpts of that night's list
payloads (TMC's RSS department, Luminis' Greenhouse department, UC Health's
short description, Loma Linda's work location), each with the answer of the
stored-row change list (facility_scale_2026-10-29/phase6/changes/short-b.tsv;
null = the row stays on the system name). E-mail addresses are placeholders and
phone numbers are dropped. No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_fac6_short_b as F
import facilities_town as FT
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_fac6_short_b_1030", "rows.json")
with open(FIX, encoding="utf-8") as _f:
    DATA = json.load(_f)


@pytest.fixture(autouse=True)
def _empty_stored():
    FT.set_stored([])
    yield
    FT.set_stored([])


def _job(system, title, city, state, description="", url="", job_id="1", name=None, location=None):
    return Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
               location=location if location is not None else f"{city}, {state}", specialty="", job_type="",
               url=url, job_id=str(job_id), posted_date="", description=description, ats_platform="X")


def _final(jobs):
    out = scraper.finalize_jobs(jobs)
    return {str(o["job_id"]): [o.get("hospital_name"), o.get("city"), o.get("state")] for o in out}


def _same(got, want, system):
    if want is None:
        return got[0] == system
    return got[0] == want[0] and (got[1] or "").lower() == want[1].lower() and got[2] == want[2]


BODY = "Summary: " + "The team provides safe, high quality care to every patient on the unit. " * 20


# ── the stored-row change list, through finalize_jobs ──────────────────────
def test_body_rows_match_the_change_list():
    rows = DATA["body_rows"]
    assert len(rows) >= 150
    jobs = [_job(r["system"], r["title"], r["city"], r["state"], r["description"], r["url"], r["job_id"])
            for r in rows]
    got = _final(jobs)
    bad = [(r["system"], r["title"], r["answer"], got.get(str(r["job_id"])))
           for r in rows if not _same(got[str(r["job_id"])], r["answer"], r["system"])]
    assert not bad, bad[:5]
    assert sum(1 for r in rows if r["answer"]) >= 80


def test_tmc_rows_match_the_change_list():
    for r in DATA["tmc"]:
        hit = F.taleo_be_facility(F.TMC, r["dept"], r["title"], r["city"], r["state"])
        want = r["answer"]
        assert (list(hit) if hit else None) == want, (r["dept"], r["title"], hit, want)


def test_luminis_rows_match_the_change_list():
    for r in DATA["luminis"]:
        hit = F.greenhouse_facility(F.LUMINIS, r["job"])
        assert (list(hit) if hit else None) == r["answer"], (r["job"]["title"], r["job"]["departments"], hit)


def test_oracle_rows_match_the_change_list():
    for r in DATA["oracle"]:
        j = r["req"]
        parts = [p.strip() for p in str(j.get("PrimaryLocation") or "").split(",")]
        city = parts[0] if len(parts) >= 3 else ""
        hit = F.oracle_facility(r["system"], j, city, "")
        assert (list(hit) if hit else None) == r["answer"], (r["system"], j["Title"], hit, r["answer"])


def test_health_first_codes_match_the_change_list():
    jobs = [_job("Health First", r["title"], r["city"], r["state"], BODY, job_id=r["id"], name=r["hospital_name"])
            for r in DATA["health_first"]]
    got = _final(jobs)
    for r in DATA["health_first"]:
        assert got[str(r["id"])] == r["answer"], (r["hospital_name"], got[str(r["id"])])


# ── TMC: the RSS department ────────────────────────────────────────────────
def test_tmc_departments():
    assert F.tmc_department("634 Med-Surg Overflow", "Registered Nurse - Oncology Unit", "Tucson", "AZ") == \
        ("Tucson Medical Center", "Tucson", "AZ")
    assert F.tmc_department("506 Rincon ED", "Paramedic - TMC Rincon ED", "Tucson", "AZ") == ("TMC Rincon", "Tucson", "AZ")
    # clinics, cancer centres, system pools and offices keep the label
    assert F.tmc_department("329 Cardiology Clinic", "General Cardiologist", "Tucson", "AZ") is None
    assert F.tmc_department("615 Supplemental Nurse Staff", "RN Float Pool I - System Med Surg", "Tucson", "AZ") is None
    assert F.tmc_department("634 Med-Surg Overflow", "Medical Assistant TMCOne", "Tucson", "AZ") is None
    assert F.tmc_department("769 Clinical Pharmacy", "EPIC Systems Analyst", "Tucson", "AZ") is None
    # never outside Tucson, never for another system
    assert F.tmc_department("602 Intensive Care Unit", "Registered Nurse - ICU", "Willcox", "AZ") is None
    assert F.taleo_be_facility("Blanchard Valley Health System", "602 Intensive Care Unit", "RN", "Tucson", "AZ") is None


def test_tmc_rss_through_the_adapter_parser():
    xml = """<?xml version="1.0"?><rss version="2.0" xmlns:taleo="urn:TBERss"><channel>
    <item><title>Registered Nurse - Main ED</title><link>x?rid=1</link><taleo:reqId>1</taleo:reqId>
    <taleo:locationCity>Tucson</taleo:locationCity><taleo:locationState>US-AZ</taleo:locationState>
    <taleo:department>601 Emergency Svcs</taleo:department><description>d</description></item>
    <item><title>Medical Assistant TMCOne La Canada</title><link>x?rid=2</link><taleo:reqId>2</taleo:reqId>
    <taleo:locationCity>Tucson</taleo:locationCity><taleo:locationState>US-AZ</taleo:locationState>
    <taleo:department>379 La Canada Office</taleo:department><description>d</description></item>
    <item><title>Registered Nurse - TMC Rincon Med Surg</title><link>x?rid=3</link><taleo:reqId>3</taleo:reqId>
    <taleo:locationCity>Tucson</taleo:locationCity><taleo:locationState>AZ</taleo:locationState>
    <taleo:department>502 Rincon Med/Surg 2nd Floor</taleo:department><description>d</description></item>
    </channel></rss>"""
    base, org, cws, st = scraper.TALEO_BE_ORGS["TMC Healthcare"]
    jobs = scraper._parse_taleo_be_rss(xml, "TMC Healthcare", base, org, cws, st)
    assert [(j.job_id, j.hospital_name, j.city, j.state) for j in jobs] == [
        ("1", "Tucson Medical Center", "Tucson", "AZ"), ("2", "TMC Healthcare", "Tucson", "AZ"),
        ("3", "TMC Rincon", "Tucson", "AZ")]


# ── Luminis: the Greenhouse department ─────────────────────────────────────
def _gh(dept, loc="Annapolis, MD", offices=("Annapolis, MD",), title="Staff Nurse - ICU", jid=7):
    return {"id": jid, "title": title, "location": {"name": loc}, "offices": [{"name": o, "location": o} for o in offices],
            "departments": [{"name": dept}], "absolute_url": f"https://boards.greenhouse.io/x/jobs/{jid}",
            "updated_at": "2026-10-29T00:00:00-04:00", "metadata": [], "content": ""}


def test_luminis_units():
    A = "Luminis Health Anne Arundel Medical Center"
    D = "Luminis Health Doctors Community Medical Center"
    assert F.luminis_department(_gh("LH Anne Arundel Med Ctr - ICU")) == (A, "Annapolis", "MD")
    assert F.luminis_department(_gh("LH Pathways - Behavioral Health Inpt", title="Treatment Assistant")) == \
        ("Luminis Health Pathways", "Annapolis", "MD")
    # the unit is the employer: a Doctors Community unit posted in Annapolis needs its title to say so
    assert F.luminis_department(_gh("LH Doctors Comm Med Ctr - CT Scan", title="CT Technologist")) is None
    assert F.luminis_department(_gh("LH Doctors Comm Med Ctr - ICU", title="Staff Nurse - ICU @ LHDCMC")) == \
        (D, "Lanham", "MD")
    # a posting located in Lanham with an Annapolis office and an Anne Arundel unit is left as it was
    assert F.luminis_department(_gh("LH Anne Arundel Med Ctr - Respiratory Care Services", loc="Lanham, MD",
                                    title="Respiratory Therapist - Registered")) is None
    # outreach labs, the medical group and clinic titles keep the label
    assert F.luminis_department(_gh("LH Anne Arundel Med Ctr - Conexus Laboratories", title="Phlebotomist")) is None
    assert F.luminis_department(_gh("LH Medical Group - Primary Care", title="Medical Assistant")) is None
    assert F.luminis_department(_gh("LH Anne Arundel Med Ctr - ICU", title="Medical Assistant")) is None


def test_luminis_through_scrape_greenhouse(monkeypatch):
    data = {"jobs": [_gh("LH Anne Arundel Med Ctr - ICU", jid=11),
                     _gh("LH Medical Group - Primary Care", title="Medical Assistant", jid=12)],
            "meta": {"total": 2}}

    class R:
        status = 200

        async def json(self, content_type=None):
            return data

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(scraper, "req", lambda *a, **k: R())
    jobs = asyncio.run(scraper.scrape_greenhouse(None, "Luminis Health", "luminishealth"))
    assert {j.job_id: j.hospital_name for j in jobs} == {
        "11": "Luminis Health Anne Arundel Medical Center", "12": "Luminis Health"}


# ── UC Health and Loma Linda: the Oracle list ─────────────────────────────
def test_uc_health_short_description():
    U, W = "University of Cincinnati Medical Center", "West Chester Hospital"
    assert F.uch_short_location("Location: UC Medical Center\nDepartment: Emergency", "RN", "Cincinnati", "OH") == \
        (U, "Cincinnati", "OH")
    assert F.uch_short_location("Location: UCMC Department: Psychiatry Hours: 40", "RN", "", "") == (U, "Cincinnati", "OH")
    assert F.uch_short_location("Location: West Chester Hospital", "RN", "West Chester", "OH") == (W, "West Chester", "OH")
    # a business centre, a clinic, remote, several sites or a building name no hospital
    for v in ("UC Health Business Center", "UC Health Physicians-WCH North", "UC Health/ Remote (OH, KY, IN)",
              "UCMC & WCH", "UCMC Medical Arts Building", "UC Medical Center, Ridgeway"):
        assert F.uch_short_location(f"Location: {v}", "RN", "Cincinnati", "OH") is None, v
    # the hospital's own town only
    assert F.uch_short_location("Location: University of Cincinnati Medical Center", "RN", "West Chester", "OH") is None


def test_loma_linda_work_locations():
    assert F.llu_work_location("Troesh/Tower Adult/13flr", "RN", "Loma Linda") == \
        ("Loma Linda University Medical Center", "Loma Linda", "CA")
    assert F.llu_work_location("Troesh/Tower Children's/9flr", "RN", "Loma Linda") == \
        ("Loma Linda University Children's Hospital", "Loma Linda", "CA")
    assert F.llu_work_location("Loma Linda University Surgical Hospital/1st Floor", "Telemetry Technician", "Loma Linda") == \
        ("Loma Linda University Surgical Hospital", "Redlands", "CA")
    assert F.llu_work_location("Loma Linda University Behavioral Medicine Center/2nd Floor", "RN", "Redlands") == \
        ("Loma Linda University Behavioral Medicine Center", "Redlands", "CA")
    for v in ("Faculty Medical Clinics", "LLUH 197/3rd Floor", "Drayson Center", "PHARMACY RETAIL MC1110A"):
        assert F.llu_work_location(v, "RN", "Loma Linda") is None, v


def test_loma_linda_redlands_is_shared_now():
    spec = FT.SPECS["Loma Linda University Health"]
    assert (FT.town_key("Redlands"), "CA") in spec.shared
    assert FT.decide("Loma Linda University Health", "Loma Linda University Health", "Redlands", "CA",
                     "Registered Nurse", BODY)[3] == "shared"


def _oracle_resp(items):
    class R:
        status = 200

        async def json(self, content_type=None):
            return {"items": [{"TotalJobsCount": len(items), "requisitionList": {"items": items}}]}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False
    return R()


def test_oracle_hooks_through_scrape_oracle(monkeypatch):
    uc = [{"Id": "1", "Title": "RN", "PrimaryLocation": "Cincinnati, OH, United States", "PrimaryLocationCountry": "US",
           "ShortDescriptionStr": "Location: UC Medical Center\nDepartment: X", "workLocation": {"items": []}},
          {"Id": "2", "Title": "Analyst", "PrimaryLocation": "Cincinnati, OH, United States", "PrimaryLocationCountry": "US",
           "ShortDescriptionStr": "Location: UC Health Business Center", "workLocation": {"items": []}}]
    llu = [{"Id": "3", "Title": "Telemetry Technician", "PrimaryLocation": "Loma Linda, CA, United States",
            "PrimaryLocationCountry": "US", "workLocation": {"items": [
                {"LocationName": "Loma Linda University Surgical Hospital/1st Floor", "Country": "US"}]}}]
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    monkeypatch.setattr(scraper, "req", lambda s, m, url, **k: _oracle_resp(uc if "eswt" in url else llu))
    a = asyncio.run(scraper.scrape_oracle(None, "UC Health", scraper.ORACLE_ORGS["UC Health"]))
    b = asyncio.run(scraper.scrape_oracle(None, "Loma Linda University Health",
                                          scraper.ORACLE_ORGS["Loma Linda University Health"]))
    assert [(j.job_id, j.hospital_name, j.city) for j in a] == [
        ("1", "University of Cincinnati Medical Center", "Cincinnati"), ("2", "UC Health", "Cincinnati")]
    assert [(j.job_id, j.hospital_name, j.city) for j in b] == [("3", "Loma Linda University Surgical Hospital", "Redlands")]


# ── body cues ──────────────────────────────────────────────────────────────
LB_SINAI = BODY + "\nAbout Sinai Hospital\nSinai Hospital is a teaching hospital.\nAbout LifeBridge Health\nWe care."


def test_lifebridge_about_heading():
    S = "Sinai Hospital of Baltimore"
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Registered Nurse - PACU",
                        LB_SINAI)[:3] == (S, "Baltimore", "MD")
    # another town than the hospital's: an off-site practice of its employer
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Owings Mills", "MD", "Registered Nurse",
                        LB_SINAI)[3] == "elsewhere"
    # a clinic, provider-group or outpatient title keeps the label
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Medical Assistant",
                        LB_SINAI)[3] == "offsite"
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Registered Nurse Outpatient Psych",
                        LB_SINAI)[3] == "offsite"
    # a posting that names two of the hospitals is no one hospital's
    two = LB_SINAI + "\nThis position has facilities oversight for Sinai Hospital and Grace Medical Center."
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "AVP - Facilities", two)[3] == "several"
    # no body tonight: left to the town table's stored decision
    assert F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "RN", "short")[3] == "no-body"


def test_legacy_phrases():
    gs = BODY + "\nJoin Our Progressive Care Unit (PCU) Team at Legacy Good Samaritan Medical Center."
    assert F.cue_decide("Legacy Health", "Legacy Health", "PORTLAND", "OR", "RN Staff Nurse - PCU", gs)[0] == \
        "Legacy Good Samaritan Medical Center"
    rch = BODY + "\nAbout Randall Children's Hospital:\nRandall Children's Hospital at Legacy Emanuel is a center for children."
    assert F.cue_decide("Legacy Health", "Legacy Health", "PORTLAND", "OR", "RN Staff Nurse - Women's Services", rch)[0] == \
        "Randall Children's Hospital"
    # the system's inventory sentence names no site
    inv = BODY + "\nLegacy Health includes six hospitals, including Randall Children's Hospital at Legacy Emanuel."
    assert F.cue_decide("Legacy Health", "Legacy Health", "PORTLAND", "OR", "Pharmacist", inv)[3] == "none"
    # a float team across sites
    fl = BODY + "\nThis team floats to Emanuel, Good Samaritan and Mount Hood; join our team at Legacy Emanuel."
    assert F.cue_decide("Legacy Health", "Legacy Health", "PORTLAND", "OR", "RN Resource Team", fl)[3] == "several"


def test_texas_childrens_campuses_and_towns():
    west = BODY + "\nWe are searching for a Director of Respiratory Services at West Campus, someone who leads."
    assert F.cue_decide("Texas Children's", "Texas Children's", "Katy", "TX", "Director I Respiratory", west)[:3] == \
        ("Texas Children's Hospital West Campus", "Houston", "TX")
    assert FT.decide("Texas Children's", "Texas Children's", "Austin", "TX", "Registered Nurse (RN) - PACU", BODY)[:3] == \
        ("Texas Children's Hospital North Austin", "Austin", "TX")
    assert FT.decide("Texas Children's", "Texas Children's", "The Woodlands", "TX", "Staff Pharmacist", BODY)[0] == \
        "Texas Children's Hospital The Woodlands"
    # Houston holds three; an outpatient or urgent care title keeps the label
    assert FT.decide("Texas Children's", "Texas Children's", "Houston", "TX", "Staff Pharmacist", BODY)[3] == "shared"
    assert FT.decide("Texas Children's", "Texas Children's", "The Woodlands", "TX", "Speech Language Pathologist - Outpatient",
                     BODY)[3] == "care-site"
    assert FT.decide("Texas Children's", "Texas Children's", "Austin", "TX", "RN - TC Urgent Care", BODY)[3] == "offsite"


def test_mosaic_town_table():
    assert FT.decide("Mosaic Life Care", "Mosaic Life Care", "Maryville", "MO", "Maryville Registered Nurse - Medical Surgical",
                     BODY)[:3] == ("Mosaic Medical Center - Maryville", "Maryville", "MO")
    # Saint Joseph holds the medical center and the Heartland LTACH (and is the stamp)
    spec = FT.SPECS["Mosaic Life Care"]
    assert (FT.town_key("Saint Joseph"), "MO") in spec.shared and spec.default == ("Saint Joseph", "MO")
    assert FT.decide("Mosaic Life Care", "Mosaic Life Care", "Saint Joseph", "MO", "Registered Nurse - Ortho/Neuro",
                     BODY)[0] is None
    # no town from the requisition (the Saint Joseph stamp comes later): no hospital
    assert FT.decide("Mosaic Life Care", "Mosaic Life Care", "", "MO", "Registered Nurse", BODY)[3] == "no-town"
    assert FT.decide("Mosaic Life Care", "Mosaic Life Care", "Maryville", "MO",
                     "Maryville Mental Health Therapist - Family Life Services (East)", BODY)[3] == "care-site"


# ── Health First codes and Brown sites ─────────────────────────────────────
def test_health_first_codes():
    assert F.health_first_code("HRMC", "Melbourne", "FL") == ("Holmes Regional Medical Center", "Melbourne", "FL")
    assert F.health_first_code("CCH", "Cocoa Beach", "FL") == ("Cape Canaveral Hospital", "Cocoa Beach", "FL")
    assert F.health_first_code("VH", "Brevard County", "FL") == ("Viera Hospital", "Melbourne", "FL")
    assert F.health_first_code("GATEWAY", "Melbourne", "FL") == ("Health First", "Melbourne", "FL")
    assert F.health_first_code("Viera MOB", "Brevard County", "FL") == ("Health First", "Brevard County", "FL")
    assert F.health_first_code("HRMC", "Palm Bay", "FL") == ("Health First", "Palm Bay", "FL")
    assert F.health_first_code("Palm Bay Hospital", "Palm Bay", "FL") is None


def test_brown_eddy_street_sites():
    s = "Brown University Health"
    assert scraper._wd_facility(s, "593 Eddy Street - Hasbro", "", "RI")[0] == "Hasbro Children's Hospital"
    assert scraper._wd_facility(s, "593 Eddy Street - Jane Brown", "", "RI")[0] == "Rhode Island Hospital"
    assert scraper._wd_facility(s, "593 Eddy Street - APC", "", "RI")[0] is None


def test_finalize_runs_the_pass_before_the_town_tables():
    j1 = _job("LifeBridge Health", "Registered Nurse - PACU", "Baltimore", "MD", LB_SINAI, job_id="a")
    j2 = _job("Health First", "CT Technologist", "Melbourne", "FL", BODY, job_id="b", name="HRMC")
    j3 = _job("UC Health", "RN", "Cincinnati", "OH", BODY, job_id="c")      # nothing names it: the label stays
    got = _final([j1, j2, j3])
    assert got["a"][0] == "Sinai Hospital of Baltimore"
    assert got["b"][0] == "Holmes Regional Medical Center"
    assert got["c"][0] == "UC Health"


def test_a_raising_reader_keeps_the_row(monkeypatch):
    def boom(*a, **k):
        raise ValueError("unforeseen")
    monkeypatch.setattr(F, "cue_decide", boom)
    j = _job("LifeBridge Health", "RN", "Baltimore", "MD", LB_SINAI, job_id="z")
    stats = F.apply_facilities([j])
    assert j.hospital_name == "LifeBridge Health" and stats["LifeBridge Health"] == {"error": 1}


# -- release 6 review A (2026-10-30) ------------------------------------------
def test_review_a_luminis_two_towns_and_another_hospital_in_the_title():
    A = "Luminis Health Anne Arundel Medical Center"
    D = "Luminis Health Doctors Community Medical Center"
    # a posting located in both towns names its unit's hospital only when its title does
    assert F.luminis_department(_gh("LH Anne Arundel Med Ctr - HSCRC Nurs Support Grant",
                                    loc="Annapolis, MD; Lanham, MD",
                                    title="Nurse Manager - Clinical Education")) is None
    assert F.luminis_department(_gh("LH Doctors Comm Med Ctr - Environmental Services",
                                    loc="Annapolis, MD; Lanham, MD", offices=("Lanham, MD",),
                                    title="Environmental Attendant - Lead")) is None
    got = F.luminis_department(_gh("LH Doctors Comm Med Ctr - Environmental Services",
                                   loc="Annapolis, MD; Lanham, MD", offices=("Lanham, MD",),
                                   title="Environmental Attendant LHDCMC FT"))
    assert got == (D, "Lanham", "MD")
    # a title naming another hospital than the unit names none
    assert F.luminis_department(_gh("LH McNew Medical Center - Behavioral Health Inpt",
                                    title="Occupational Therapist I - PRN - AAMC")) is None
    assert F.luminis_department(_gh("LH Pathways - Behavioral Health Inpt",
                                    title="Treatment Assistant - Full-time - AAMC")) is None
    got = F.luminis_department(_gh("LH Anne Arundel Med Ctr - Pharmacy - Inpatient",
                                   title="Clinical Staff Pharmacist - Full-time -Evening -AAMC"))
    assert got == (A, "Annapolis", "MD")


def test_review_a_uc_health_and_loma_linda_office_titles():
    U = "University of Cincinnati Medical Center"
    for t in ("Registered Nurse (RN), Ambulatory Surgery Center, Part-time, Shift Varies, ONA",
              "Facilities Compliance Specialist, Full Time, First Shift"):
        assert F.uch_short_location("Location: UC Medical Center", t, "Cincinnati", "OH") is None, t
    got = F.uch_short_location("Location: UC Medical Center", "Registered Nurse (RN), Operating Room, Full-time",
                               "Cincinnati", "OH")
    assert got == (U, "Cincinnati", "OH")
    assert F.llu_work_location("Troesh/Tower Adult/13flr", "Clinic Medical Assistant", "Loma Linda") is None
    got = F.llu_work_location("Troesh/Tower Adult/13flr", "Clinical Nurse B", "Loma Linda")
    assert got[0] == "Loma Linda University Medical Center"


def _lb(*lines):
    return BODY + "".join("\n" + x for x in lines)


def test_review_a_lifebridge_two_hospitals_by_their_short_names():
    grace = _lb("Coordinate and support the EPE staff at Sinai and Grace with all CIA responsibilities.",
                "About Grace Medical Center", "Grace Medical Center is a community hospital.",
                "About LifeBridge Health", "We care.")
    got = F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Crisis Intervention Associate",
                       grace)
    assert got[3] == "several"
    # the words, not the names: still the one hospital
    words = _lb("A grace period applies to the uniform allowance.", "About Grace Medical Center",
                "Grace Medical Center is a community hospital.", "About LifeBridge Health", "We care.")
    got = F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Registered Nurse", words)
    assert got[:3] == ("Grace Medical Center", "Baltimore", "MD")
    got = F.cue_decide("LifeBridge Health", "LifeBridge Health", "Baltimore", "MD", "Registered Nurse - PACU", LB_SINAI)
    assert got[:3] == ("Sinai Hospital of Baltimore", "Baltimore", "MD")
