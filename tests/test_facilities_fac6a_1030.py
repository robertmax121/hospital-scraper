"""2026-10-30 (facility scale phase 6, the nightly shortfall part A):
INTEGRIS lead sentences, Prisma's body work-site line and its Midlands
spellings, Broward's Children's Diagnostic & Treatment Center, Lifepoint
programme units (facilities_fac6a, facilities_leftovers, facilities_oracle_other,
facilities_hospital_systems_a).

Fixtures (tests/fixtures/fac6a1030/), real data read 2026-10-30, cut to the
fields the readers use, any e-mail or phone-like text replaced:
  integris_list.json       INTEGRIS's Oracle list (ertr CX_3001): the work
                           location facet and requisitions whose workLocation
                           child comes back with every field null, with their
                           ShortDescriptionStr
  integris_leads.json      the 655 active INTEGRIS label rows' stored body leads
  prisma_tails.json        the active Prisma label and providers-board rows'
                           stored Location / Facility block
  prisma_detail.json       Prisma Workday detail reads (jobRequisitionLocation)
  broward_first_lines.json the 182 active Broward label rows' first body line
  lifepoint_list.json      400 rows of Lifepoint's Oracle list with their
                           work locations
No network, no database."""
import json
import os

import pytest

import facilities_fac6a as F6
import facilities_hospital_systems_a as A
import facilities_leftovers as LO
import facilities_oracle_other as OO
import facilities_town as TOWN
import facility_names as F
import scraper
from conftest import FIXTURES

FIX = os.path.join(FIXTURES, "fac6a1030")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


INTEGRIS_LIST = _load("integris_list.json")
INTEGRIS_LEADS = _load("integris_leads.json")["rows"]
PRISMA_TAILS = _load("prisma_tails.json")["rows"]
PRISMA_DETAIL = _load("prisma_detail.json")["postings"]
BROWARD = _load("broward_first_lines.json")["rows"]
LIFEPOINT_LIST = _load("lifepoint_list.json")["items"]
PAD = "\n\n" + "Provides patient care and support. " * 40


def _job(system, name, *, city="", state="", description="", title="RN", job_id="J1", url=""):
    return scraper.Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
                       location=f"{city}, {state}" if city else state, specialty="", job_type="", url=url,
                       job_id=job_id, posted_date="", description=description, ats_platform="")


@pytest.fixture(autouse=True)
def _no_stored():
    F6.set_known_facilities([])
    TOWN.set_stored([])
    A.set_known_facilities([])
    yield
    F6.set_known_facilities([])
    TOWN.set_stored([])
    A.set_known_facilities([])


# ── INTEGRIS ────────────────────────────────────────────────────────────────

def test_integris_work_location_is_empty_so_the_list_named_nothing():
    """Why the 10-30 nightly named no INTEGRIS row: the facet lists the work
    locations, every requisition's workLocation child is all null."""
    assert {x["Name"] for x in INTEGRIS_LIST["workLocationsFacet"]} >= {"Baptist Medical Center of OK-HCM"}
    for j in INTEGRIS_LIST["items"]:
        for w in j.get("workLocation") or []:
            assert not w.get("LocationName")
        loc, city, state = scraper._oracle_location(j)
        assert scraper._oracle_work_location_facility("INTEGRIS Health", j, city, state) == ""


def test_integris_list_sentence_names_the_hospital():
    got = {}
    for j in INTEGRIS_LIST["items"]:
        loc, city, state = scraper._oracle_location(j)
        got[str(j["Id"])] = F6.oracle_lead_facility("INTEGRIS Health", j, city, state)
    assert got["117463"] == ("INTEGRIS Health Edmond Hospital", "Edmond", "OK")        # "... at INTEGRIS Health Edmond"
    assert got["117958"] == ("INTEGRIS Health Southwest Medical Center", "Oklahoma City", "OK")
    assert got["117967"][0] == "INTEGRIS Health Canadian Valley Hospital"
    assert got["117139"][0] == "INTEGRIS Heart Hospital"
    assert got["115912"] is None          # "INTEGRIS Health Medical Group Float Pool": a pool, no hospital
    assert got["118021"] is None          # "at INTEGRIS Health": the system itself
    assert got["116290"] is None          # medical group clinic float pool, Enid
    assert F6.oracle_lead_facility("Lifepoint Health", INTEGRIS_LIST["items"][0]) is None


def test_integris_stored_leads_totals():
    named = {}
    for r in INTEGRIS_LEADS:
        got = F6.integris_facility(r["lead"], r["title"], r["city"], r["state"])
        if got:
            named[got[0]] = named.get(got[0], 0) + 1
    assert sum(named.values()) == 361
    assert named["INTEGRIS Health Baptist Medical Center"] == 124
    assert named["INTEGRIS Health Southwest Medical Center"] == 74
    assert named["INTEGRIS Health Edmond Hospital"] == 42
    assert named["INTEGRIS Mental Health Spencer"] == 9


@pytest.mark.parametrize("lead, title, city, want", [
    ("Join our team as a night shift, full-time, Registered Nurse at INTEGRIS Health Edmond, in Edmond, OK.",
     "RN", "Edmond", ("INTEGRIS Health Edmond Hospital", "Edmond", "OK")),
    # the requisition files it at Oklahoma City; the sentence names the site and its town
    ("Join our team as a Day Shift, PRN, Inpatient Rehab Tech at INTEGRIS Health Edmond Hospital in Edmond, OK.",
     "Rehab Tech", "Oklahoma City", ("INTEGRIS Health Edmond Hospital", "Edmond", "OK")),
    # no town on the row: the site's town
    ("Join our team as a day shift, RN, at INTEGRIS Health Baptist Medical Center, in Oklahoma City, OK.",
     "RN", "", ("INTEGRIS Health Baptist Medical Center", "Oklahoma City", "OK")),
    # the system's hospital in a town where it has one
    ("Join our team as a Day shift, Fulltime, Certified Surgical Tech / CST, at INTEGRIS Health Hospital, in Miami, OK.",
     "Surgical Tech", "Miami", ("INTEGRIS Health Miami Hospital", "Miami", "OK")),
    ("Join our team as a PRN, Registered Nurse at INTEGRIS Mental Health in Spencer, OK.",
     "RN", "Spencer", ("INTEGRIS Mental Health Spencer", "Spencer", "OK")),
    ("Join our team as a Registered Nurse at the INTEGRIS Health Ponca City Hospital in Ponca City, OK.",
     "Registered Nurse", "Ponca City", ("INTEGRIS Health Ponca City Hospital", "Ponca City", "OK")),
    # an office role's title (the TOWN family's rules): the system name
    ("Join our team as a Marketing Specialist at the INTEGRIS Health Ponca City Hospital in Ponca City, OK.",
     "Marketing Strategist", "Ponca City", None),
    # several sites, a clinic title, a clinic, the system, a sentence with no "at INTEGRIS"
    ("Join our team as a PRN or FT Outpatient float at INTEGRIS Health Edmond Hospital, INTEGRIS Health Hefner "
     "Pointe, INTEGRIS Health Baptist Medical Center in Oklahoma City.", "Physical Therapist", "Oklahoma City", None),
    ("Join our team as a day shift, full-time, Manager of Clinic Operations at INTEGRIS Health Council Crossing "
     "in Oklahoma City, OK.", "Manager Clinic Operations", "Oklahoma City", None),
    ("Join our team as a day shift, Medical Assistant, at INTEGRIS HEALTH Medical Group Rose Creek Outpatient Clinic"
     " in Edmond, OK.", "Medical Assistant", "Edmond", None),
    ("Join our team as a full-time, Director of Workforce Planning at INTEGRIS Health.", "Director", "Oklahoma City",
     None),
    ("join our team.\n\nWorking PRN at INTEGRIS Health Miami Hospital offers flexible scheduling.", "Tech", "Miami",
     None),
    # a site in another town than the row, the sentence naming no town
    ("Join our team as a RN at INTEGRIS Health Enid Hospital.", "RN", "Woodward", None),
])
def test_integris_lead_cases(lead, title, city, want):
    assert F6.integris_facility(lead, title, city, "OK") == want


def test_integris_table_is_clean():
    for site in F6.INTEGRIS_SPEC.names.values():
        assert site.state == "OK"
        assert site.source.split()[0].rstrip(":") in ("cms", "address", "board"), site
    # one spelling never names two sites (facilities_text.Spec raises), and the
    # oracle table's three names are spelled the same way here
    oo = OO.SYSTEMS["INTEGRIS Health"]["hospitals"]
    for name in oo:
        assert name in F6.INTEGRIS_SPEC.names, name


def test_scrape_oracle_reads_the_sentence_only_after_the_work_location():
    src = open(scraper.__file__, encoding="utf-8").read()
    i = src.index("_facility = _oracle_work_location_facility(system, j, _city, _state) or system")
    j = src.index("facilities_fac6a.oracle_lead_facility", i)
    assert src.index("if _facility == system:", i) < j < src.index("jobs.append(Job(", i)


# ── Prisma ──────────────────────────────────────────────────────────────────

def test_prisma_detail_spellings_name_the_midlands_hospitals():
    want = {
        "R1144947": ("site", "Prisma Health Richland Hospital"),      # "Richland"
        "R1145493": ("site", "Prisma Health Richland Hospital"),
        "R1129982": ("site", "Prisma Health Baptist Hospital"),       # "Baptist"
        "R1142151": ("site", "Prisma Health Baptist Hospital"),
        "R1146054": ("site", "Roger C. Peace Rehabilitation Hospital"),
        "R1147314": ("site", "Marshall I. Pickens Hospital"),
        "R1142803": ("site", "Prisma Health Laurens County Hospital"),
        "R1140827": ("unknown", None),                                # "Cancer Centers - Faris Road": no entry, no office word
        "R1145000": ("office", None),                                 # "Patewood Outpt Ctr/Med Offices"
        "R1129774": ("office", None),                                 # "3209 Colonial Dr Richland"
        "R1145372": ("office", None),                                 # "103 Saluda Ridge Ct W Columbia"
        "R1142291": ("office", None),                                 # "300 Palmetto Health Pky Parkri"
        "R1144759": ("unknown", None),                                # "Richland Chapin"
    }
    seen = set()
    for p in PRISMA_DETAIL:
        jid = p["jobReqId"]
        if jid not in want:
            continue
        verdict, entry = LO.detail_site("Prisma Health", LO.requisition_location(p))
        assert (verdict, entry[0] if entry else None) == want[jid], (jid, p["jobRequisitionLocation"])
        seen.add(jid)
    assert seen == set(want)


def test_prisma_detail_pass_places_columbia_and_keeps_laurens_off_until_the_body():
    by = {p["jobReqId"]: p for p in PRISMA_DETAIL}
    job = _job("Prisma Health", "Prisma Health", city="Columbia", state="SC")
    assert LO.apply_detail(job, by["R1144947"]) == "site"
    assert (job.hospital_name, job.city) == ("Prisma Health Richland Hospital", "Columbia")
    # Laurens: the hospital's own street is in Clinton; the detail pass alone
    # leaves the row ("elsewhere"), the body pass names it in its own town
    job = _job("Prisma Health", "Prisma Health", city="Laurens", state="SC")
    assert LO.apply_detail(job, by["R1142803"]) == "elsewhere"
    assert job.hospital_name == "Prisma Health"


def test_prisma_body_block_matches_the_detail_descriptor():
    """The stored body prints the same work site the detail payload carries."""
    assert F6.prisma_location("Location\nRichland\n\nFacility\n1510 Richland Hospital\n\nDepartment\n15106511 "
                              "Marion Suite") == "Richland"
    assert F6.prisma_location("Location: Greenville\nother text") == ""      # no Facility line after it
    assert F6.prisma_location("no block here") == ""


def test_prisma_stored_tails_totals():
    # 2026-10-30 (release 6 review A): with the title and the Facility cost
    # centre read (facilities_leftovers.prisma_gate) and no town move, 590 +
    # 124 became 501 + 51: the remote, home health, hospice, clinic, practice,
    # Corporate, EMS, two-campus and regional rows keep the system name
    named, by = {}, {}
    for r in PRISMA_TAILS:
        v, placed, site = F6.prisma_site("Prisma Health", r["tail"], r["city"], r["state"], r["title"])
        if v == "site":
            named[r["hospital_name"]] = named.get(r["hospital_name"], 0) + 1
            by[placed[0]] = by.get(placed[0], 0) + 1
            assert (placed[1], placed[2]) == (r["city"], r["state"])      # the body line never moves a town
    assert named == {"Prisma Health": 501, "Prisma Health (Providers)": 51}   # label rows, providers-board rows
    assert by["Prisma Health Greenville Memorial Hospital"] == 224
    assert by["Prisma Health Richland Hospital"] == 163
    assert by["Prisma Health Laurens County Hospital"] == 19
    assert "Prisma Health Oconee Memorial Hospital" not in by       # its 6 were remote, Corporate, EMS, practices


def _prisma_body(location, facility, department="Nursing"):
    return f"Location\n{location}\n\nFacility\n{facility}\n\nDepartment\n{department}" + PAD


@pytest.mark.parametrize("title,location,facility,want", [
    # the 10-30 staging's wrong rows (decision 2: the system name)
    ("Ambulatory Coder III, FT, Days, - Remote", "Oconee Medical Campus", "Corporate", "office"),
    ("Trauma Registry Analyst FT, Day-Remote", "Richland", "Richland Hospital", "office"),
    ("Registered Nurse (RN)-Home Health, PT, Days", "Tuomey", "Home Health - Tuomey", "office"),
    ("Registered Nurse (RN)-Hospice Inpatient, Hospice, FT, Days", "Blount Memorial Hospital",
     "Hospice - Eastern Tennessee", "office"),
    ("Certified Medical Assistant (CMA)-Ambulatory, Orthopedics, FT, Days", "Blount Memorial Hospital",
     "BMPG Joule Street Alcoa", "office"),
    ("System Engineer, FT, Days", "Greenville Memorial Hospital", "Corporate", "office"),
    ("Paramedic, Ambulance Services-Midlands, FT, Variable", "Richland", "Prisma Health EMS", "office"),
    ("Physician- EM Midlands", "Richland", "3135 Emergency Medicine", "office"),         # a regional group provider
    ("Pharmacist, Retail, PT Variable Shifts", "Baptist", "Upstate Pharmacy", "office"),
    ("Physician - OBGYN Hospitalist", "Richland", "OBGYN 2 Med Park 107", "office"),     # a practice building
    # two campuses, or a regional group whose cost centre is not the work site's own
    ("Registered Nurse (RN) Acute Care, Facility Resource Pool, Laurens/Hillcrest", "Laurens County Medical Campus",
     "Laurens County Hospital", "office"),
    ("Physician -Emergency Medicine Midlands", "Richland", "3135 Emergency Medicine", "office"),
    ("Physician - Hospitalist, Midlands", "Richland", "Hospitalists Columbia", "office"),
    ("Physician - Hospitalist, Upstate", "Laurens County Medical Campus", "Greenville Memorial Hospitalists", "office"),
    ("Physician - Hospitalist, Upstate", "Greenville Memorial Hospital", "2186 Greenville Memorial Hospitalists", "site"),
    # a cost centre at another facility than the work site names neither
    ("Visitor Services Representative, PRN, Variable", "Laurens County Medical Campus", "Hillcrest Hospital", "unknown"),
    ("Registered Nurse (RN)-Acute Care, Long Term Acute Care, LTAC", "Baptist", "1850 Prisma Health LTACH - Baptist",
     "unknown"),
    ("Licensed Practical Nurse (LPN)-Acute Care, Behavioral Care", "Richland", "Richland Springs Hospital", "unknown"),
    # the hospital's own floors still name it
    ("Registered Nurse (RN)-Acute Care, Short Stay Surgical, FT, Day", "Greenville Memorial Hospital",
     "Greenville Memorial Hospital", "site"),
    ("Computed Tomography (CT) Technologist Eligible - FT, Weekends", "Baptist", "1520 Baptist Hospital", "site"),
    ("APP - Neuro ICU - Columbia", "5 Medical Park Rd Richland", "3510 Neuro ICU", "site"),
    ("Registered Nurse (RN)-Acute Care, Facility Resource Pool, FT, Nights", "Parkridge",
     "Baptist Parkridge Hospital", "site"),
])
def test_prisma_title_and_cost_centre_gate(title, location, facility, want):
    town = {"Oconee Medical Campus": "Seneca", "Tuomey": "Sumter", "Blount Memorial Hospital": "Maryville",
            "Greenville Memorial Hospital": "Greenville", "Laurens County Medical Campus": "Laurens"}.get(location, "Columbia")
    st = "TN" if town == "Maryville" else "SC"
    assert F6.prisma_site("Prisma Health", _prisma_body(location, facility), town, st, title)[0] == want


def test_prisma_body_line_never_moves_the_town():
    """A row the board filed at the stamped default (Greenville) has a Workday
    location that is no town ("Richland-Saluda-Point" in its link); the body's
    "Richland" may contradict it, so the body pass leaves the row and the
    detail re-read (the location itself) decides."""
    body = _prisma_body("Richland", "Richland Hospital", "Rehabilitative Services Ortho Admin")
    assert F6.prisma_site("Prisma Health", body, "Greenville", "SC", "Patient Transporter")[0] == "elsewhere"
    assert F6.prisma_site("Prisma Health", body, "Columbia", "SC", "Patient Transporter")[:2] == (
        "site", ("Prisma Health Richland Hospital", "Columbia", "SC"))
    row = {"hospital_system": "Prisma Health", "hospital_name": "Prisma Health", "title": "Patient Transporter",
           "city": "Greenville", "state": "SC", "description": body}
    assert F6.stored_change(row) is None


def test_prisma_detail_reread_applies_the_gate_and_other_systems_do_not():
    by = {p["jobReqId"]: p for p in PRISMA_DETAIL}
    # "Baptist" names Baptist Hospital, but a remote or home-health posting keeps the label and skips the town table
    job = _job("Prisma Health", "Prisma Health", city="Columbia", state="SC", title="Patient Financial Specialist - Remote")
    assert LO.apply_detail(job, by["R1129982"]) == "office"
    assert job.hospital_name == "Prisma Health" and getattr(job, LO.SKIP_ATTR, False)
    job = _job("Prisma Health", "Prisma Health", city="Columbia", state="SC", title="HVAC Mechanic",
               description=_prisma_body("Baptist", "1850 Prisma Health LTACH - Baptist"))
    assert LO.apply_detail(job, by["R1129982"]) == "unknown"
    assert job.hospital_name == "Prisma Health"
    job = _job("Prisma Health", "Prisma Health", city="Columbia", state="SC", title="HVAC Mechanic")
    assert LO.apply_detail(job, by["R1129982"]) == "site"
    assert job.hospital_name == "Prisma Health Baptist Hospital"
    # another system of the detail table: the Prisma gate is not read
    other = next(s for s in LO.DETAIL if s != "Prisma Health" and any(v for v in LO.DETAIL[s].values()))
    spelling, entry = next((k, v) for k, v in LO.DETAIL[other].items() if v)
    job = _job(other, LO.T.SPECS[other].system, city=entry[1], state=entry[2], title="Coder - Remote")
    assert LO.apply_detail(job, {"jobRequisitionLocation": {"descriptor": spelling}}) == "site"
    assert job.hospital_name == entry[0]


def test_prisma_laurens_campus_keeps_the_posting_town():
    body = "Location\nLaurens County Medical Campus\n\nFacility\nLaurens County Hospital\n\nDepartment\nED" + PAD
    assert F6.prisma_site("Prisma Health", body, "Laurens", "SC")[:2] == (
        "site", ("Prisma Health Laurens County Hospital", "Laurens", "SC"))
    assert F6.prisma_site("Prisma Health", body, "Clinton", "SC")[1] == (
        "Prisma Health Laurens County Hospital", "Clinton", "SC")
    # another town: no guess
    assert F6.prisma_site("Prisma Health", body, "Simpsonville", "SC")[0] == "elsewhere"


def test_prisma_finalize_names_by_body_and_skips_town_on_offices():
    site_body = "Location\nRichland\n\nFacility\nRichland Hospital\n\nDepartment\nMarion Suite" + PAD
    office_body = "Location\n115 N Sumter St Sumter\n\nFacility\nSumter OB\n\nDepartment\nOB" + PAD
    jobs = [_job("Prisma Health", "Prisma Health", city="Columbia", state="SC", description=site_body, job_id="P1",
                 title="Registered Nurse (RN) - Acute Care, Cardiac Telemetry"),
            _job("Prisma Health", "Prisma Health", city="Sumter", state="SC", description=office_body, job_id="P2",
                 title="Registered Nurse"),
            _job("Prisma Health", "Prisma Health", city="Sumter", state="SC", description="Registered nurse." + PAD,
                 job_id="P3", title="Registered Nurse")]
    out = {o["job_id"]: o for o in scraper.finalize_jobs(jobs)}
    assert out["P1"]["hospital_name"] == "Prisma Health Richland Hospital"
    assert out["P2"]["hospital_name"] == "Prisma Health"                         # an office: the town table leaves it
    assert out["P3"]["hospital_name"] == "Prisma Health Tuomey Hospital"         # no block: the town table as before


# ── Broward ─────────────────────────────────────────────────────────────────

def test_broward_first_line_names_cdtc_only():
    named = [r for r in BROWARD if F6.broward_site("\n" + r["first_line"] + "\n")[0] is not None]
    assert len(named) == 5
    assert {r["first_line"] for r in named} == {"Children's Diagnostic & Treatment Center"}
    for line in ("Broward Health Corporate ISC", "Broward Health Physician Group #31599", "Broward Health Point #31797",
                 "Broward Health Corporate Spectrum - 32235"):
        assert F6.broward_site(line)[0] is None


def test_broward_finalize_and_no_body_keep():
    body = "Children's Diagnostic & Treatment Center\n\nDepartment: CDTC Early Steps" + PAD
    out = scraper.finalize_jobs([_job("Broward Health", "Broward Health", city="Fort Lauderdale", state="FL",
                                      description=body, job_id="B1", title="Early Steps Service Coordinator")])
    assert out[0]["hospital_name"] == "Children's Diagnostic & Treatment Center"
    F6.set_known_facilities([{"hospital_system": "Broward Health", "job_id": "B1",
                              "hospital_name": "Children's Diagnostic & Treatment Center",
                              "city": "Fort Lauderdale", "state": "FL"}])
    out = scraper.finalize_jobs([_job("Broward Health", "Broward Health", city="Fort Lauderdale", state="FL",
                                      job_id="B1", title="Early Steps Service Coordinator")])
    assert out[0]["hospital_name"] == "Children's Diagnostic & Treatment Center"


# ── INTEGRIS through finalize ───────────────────────────────────────────────

def test_integris_finalize_body_then_no_body_night_keeps_the_town():
    lead = ("Join our team as a Day Shift, PRN, Inpatient Rehab Tech at INTEGRIS Health Edmond Hospital in "
            "Edmond, OK.") + PAD
    out = scraper.finalize_jobs([_job("INTEGRIS Health", "INTEGRIS Health", city="Oklahoma City", state="OK",
                                      description=lead, job_id="I1", title="Rehab Tech")])
    assert (out[0]["hospital_name"], out[0]["city"]) == ("INTEGRIS Health Edmond Hospital", "Edmond")
    # a night without a body: the stored name and the stored (corrected) town stand
    F6.set_known_facilities([{"hospital_system": "INTEGRIS Health", "job_id": "I1",
                              "hospital_name": "INTEGRIS Health Edmond Hospital", "city": "Edmond", "state": "OK"}])
    out = scraper.finalize_jobs([_job("INTEGRIS Health", "INTEGRIS Health", city="Oklahoma City", state="OK",
                                      job_id="I1", title="Rehab Tech")])
    assert (out[0]["hospital_name"], out[0]["city"]) == ("INTEGRIS Health Edmond Hospital", "Edmond")
    # a stored name in a town that is not the site's is not carried over
    F6.set_known_facilities([{"hospital_system": "INTEGRIS Health", "job_id": "I1",
                              "hospital_name": "INTEGRIS Health Edmond Hospital", "city": "Tulsa", "state": "OK"}])
    out = scraper.finalize_jobs([_job("INTEGRIS Health", "INTEGRIS Health", city="Oklahoma City", state="OK",
                                      job_id="I1", title="Rehab Tech")])
    assert out[0]["hospital_name"] == "INTEGRIS Health"


def test_a_row_that_raises_keeps_its_name():
    class Boom:
        hospital_system = "INTEGRIS Health"
        hospital_name = "INTEGRIS Health"
        city, state, location = "Edmond", "OK", "Edmond, OK"
        job_id = "X"
        title = "RN"

        @property
        def description(self):
            raise RuntimeError("boom")
    b = Boom()
    assert F6.apply_facilities([b]) == {"INTEGRIS Health": {"error": 1}}
    assert b.hospital_name == "INTEGRIS Health"


# ── Lifepoint ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, town, want", [
    ("CHC PR & MSA - Mercy Rehabilitation Hospital St. Louis", "Chesterfield", "Mercy Rehabilitation Hospital St. Louis"),
    ("National Park Medical Center - R311J", "Hot Springs", "National Park Medical Center"),
    ("Conemaugh Memorial Medical Center - MS - R414J", "Johnstown", "Conemaugh Memorial Medical Center"),
    ("Community Rehabilitation Hospital South - IRF", "Greenwood", "Community Rehabilitation Hospital South"),
    ("Willamette Valley Medical Center - BH MSA", "McMinnville", "Willamette Valley Medical Center"),
    ("VMFH Rehabilitation Hospital - IRF", "Tacoma", "Virginia Mason Franciscan Rehabilitation Hospital"),
    ("Tampa Behavioral BH MSA JV", "Tampa", "TGH Behavioral Health Hospital"),
    ("Atlantic Rehabilitation Institute - IRF", "Madison", "Atlantic Rehabilitation Institute"),
    ("Sovah Health - Danville - ARU - R362J", "Danville", "Sovah Health - Danville"),
    # a partner hospital's unit, an outpatient unit, a clinic, the office: nothing
    ("Kindred Hospital - Havertown - R5694", "Havertown", ""),
    ("St. Mary's Regional Medical Center - OP - R598G", "Russellville", ""),
    ("Mosaic Life Care at St. Joseph - Medical Center - ARU-R407J", "St. Joseph", ""),
    ("Sovah Health - Danville Heart and Vascular Clinic", "Danville", ""),
    ("Lifepoint Health - Corporate", "Brentwood", ""),
    # a hospital's unit in another town keeps the label (EMS in Escanaba)
    ("Marquette General Hospital - EMS", "Escanaba", ""),
])
def test_lifepoint_programme_units(raw, town, want):
    name = OO.oracle_location("Lifepoint Health", raw)
    assert F.list_facility("Lifepoint Health", name, town, "", "Registered Nurse") == want


def test_lifepoint_live_page_names_more_without_new_partners():
    named_head = 0
    for j in LIFEPOINT_LIST:
        loc, city, state = scraper._oracle_location(j)
        fac = scraper._oracle_work_location_facility("Lifepoint Health", j, city, state)
        if fac:
            named_head += 1
            assert "Kindred" not in fac and "Jefferson Regional" not in fac
    assert named_head >= 240


def test_lifepoint_new_body_spellings():
    for subj, town, want in [("Community North Rehabilitation Hospital", "Indianapolis",
                              "Community Rehabilitation Hospital North"),
                             ("Providence Swedish Rehab Hospital", "Lynnwood", "Providence Swedish Rehabilitation Hospital")]:
        body = f"Your experience matters\n\n{subj} is part of Lifepoint Health, a diversified healthcare network." + PAD
        state = A.LIFEPOINT_SPEC.names[want].state
        assert A.text_facility("Lifepoint Health", body, "Registered Nurse", town, state) == (want, town, state)
