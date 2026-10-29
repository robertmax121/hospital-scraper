"""2026-10-29 (facility scale phase 4, systems A): Northern Light's "Position is
located:" sites, KU's body work locations, University of Rochester's real
towns, the Emory / Cooper / UMMC town tables and the Indian Health
Service summary facility (facilities_systems_a).

Fixtures (tests/fixtures/facility_sa_1029/), real, read 2026-10-29:
  stored_rows.json        stored hospital_jobs rows of the 7 systems on the
                          system label (title, town, url, job id, first 2,500
                          characters of the body), one per reading case
  usajobs_summaries.json  seven Indian Health Service announcements' own
                          Summary paragraph (the API's JobSummary) as their
                          USAJOBS job pages served it, with the duty towns
No network, no database."""
import json
import os

import pytest

import facilities_systems_a as A
import facilities_text as T
import facilities_town as TOWN
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_sa_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


ROWS = {r["case"]: r for r in _load("stored_rows.json")}
PAGES = {p["control_number"]: p for p in _load("usajobs_summaries.json")}
ATS = {A.IHS: "USAJOBS", A.EMORY: "iCIMS", A.COOPER: "iCIMS", A.NL: "Infor", A.KU: "Workday",
       A.ROCHESTER: "Workday", A.UMMC: "Workday"}


@pytest.fixture(autouse=True)
def _no_stored():
    A.set_known_facilities([])
    TOWN.set_stored([])
    yield
    A.set_known_facilities([])
    TOWN.set_stored([])


def _job(r, *, name=None, description=None, city=None, title=None):
    s = r["hospital_system"]
    return Job(title=r["title"] if title is None else title, hospital_system=s,
               hospital_name=s if name is None else name,
               city=r["city"] if city is None else city, state=r["state"], location=r["location"],
               specialty="", job_type="", url=r["url"], job_id=str(r["job_id"]), posted_date="",
               description=r["description"] if description is None else description, ats_platform=ATS[s])


def _decide(r, **kw):
    """The branch's two passes, in finalize_jobs' order: this module, then the town tables."""
    j = _job(r, **kw)
    A.apply_facility(j)
    if j.hospital_system in A.TOWN_SYSTEMS and TOWN.SPECS[j.hospital_system].is_label(j.hospital_name):
        TOWN.apply_job(j)
    return j.hospital_name, j.city, j.state


# ── what each real stored row becomes ───────────────────────────────────────
EXPECT = {
    "nl located hospital":          ("Northern Light Maine Coast Hospital", "Ellsworth", "ME"),
    "nl mercy fore river":          ("Northern Light Mercy Hospital", "Portland", "ME"),
    "nl acadia":                    ("Northern Light Acadia Hospital", "Bangor", "ME"),
    "nl hyphen mayo":               ("Northern Light Mayo Hospital", None, "ME"),
    "nl bare town entity":          ("Northern Light Sebasticook Valley Hospital", "Pittsfield", "ME"),
    "nl entity inpatient provider": ("Northern Light Eastern Maine Medical Center", "Bangor", "ME"),
    "nl home care branch":          ("Northern Light Home Care and Hospice Presque Isle", "Presque Isle", "ME"),
    "ku bell hospital":             ("University of Kansas Hospital", "Kansas City", "KS"),
    # the work location is the Olathe hospital; the row sat at the stamped default town
    "ku olathe hospital at default": ("University of Kansas Health System Olathe Hospital", "Olathe", "KS"),
    "ku liberty hospital":          ("Liberty Hospital", "Liberty", "MO"),
    "rochester building town":      ("University of Rochester", "Rochester", "NY"),
    "rochester laser lab":          ("University of Rochester", "Rochester", "NY"),
    "rochester brighton address":   ("University of Rochester", "Brighton", "NY"),
    "emory lithonia":               ("Emory Hillandale Hospital", "Lithonia", "GA"),
    "emory midtown named":          ("Emory University Hospital Midtown", "Atlanta", "GA"),
    "cooper camden":                ("Cooper Norcross University Hospital", "Camden", "NJ"),
    "cooper cape":                  ("Cooper Norcross University Hospital Cape Regional", "Cape May Court House", "NJ"),
    "ummc greenwood":               ("UMMC Greenwood", "Greenwood", "MS"),
    "ummc lexington":               ("UMMC Holmes County", "Lexington", "MS"),
}
# stays on the system label with its own town:
STAYS = (
    "nl office building",        # "Cianchette Professional Blding": a building, not the hospital
    "nl street",                 # "925 Union Street"
    "nl entity group provider",  # an outpatient physician: the medical group practises in clinics
    "nl multi site line",        # three hospitals on one line
    "ku medical pavilion",       # an outpatient building
    "ku multi",                  # two work locations
    "ku no location",            # no work location in the body, and the link names Kansas City only
    "rochester real town",       # a real town is never changed, even where the address says a suburb
    "rochester remote",
    "emory atlanta shared",      # four Emory hospitals in Atlanta and the posting names none
    "emory two hospitals",       # "Warner Robins & Perry"
    "emory remote",
    "cooper camden names cape",  # a Camden row whose title names the Cape May hospital
    "cooper cherry hill",        # a kept town
    "cooper typo town",          # "Capa May Court House": a front-desk role (an office's), so not named
    "ummc jackson",              # the system's name is the Jackson campus's own name
    "ummc grenada clinic",
    "ihs national",              # a 60-town announcement names no facility
)


@pytest.mark.parametrize("case", sorted(EXPECT))
def test_stored_row_named(case):
    r = ROWS[case]
    name, city, state = _decide(r)
    want = EXPECT[case]
    assert name == want[0]
    assert city == (want[1] if want[1] is not None else r["city"])
    assert state == want[2]


@pytest.mark.parametrize("case", STAYS)
def test_stored_row_stays(case):
    r = ROWS[case]
    name, city, state = _decide(r)
    assert name == r["hospital_system"]
    assert (city, state) == (r["city"], r["state"])


def test_every_fixture_case_is_decided():
    assert set(ROWS) == set(EXPECT) | set(STAYS)


# ── Northern Light reading ──────────────────────────────────────────────────
def test_nl_located_line_decides_before_the_entity():
    r = ROWS["nl street"]
    site, kind, what, raw = A.read_northern_light(r["description"])
    assert (site, what, raw) == (None, "office", "925 Union Street")
    # the same body without a located line: the entity line names the hospital
    body = r["description"].replace("Position is located: 925 Union Street", "")
    site, kind, what, raw = A.read_northern_light(body)
    assert kind == "entity" and site is not None


def test_nl_bare_towns_and_placeholders():
    for v in ("Pittsfield, ME", "{ Greenville, Me}", "Unity, ME", "Sangerville, ME", "Pittsfield"):
        assert A._nl_bare_town(v), v
    for v in ("Lafayette Building", "Emergency Med Svcs - Dover-Foxcroft, ME", "925 Union Street", "Riverside",
              "Various locations throughout Maine/remote", "Mercy Westgate"):
        assert not A._nl_bare_town(v), v
    assert A.nl_located("x\n\nPosition is located: { Location}\n") == ""
    assert A.nl_located("no line here") is None
    assert A.nl_located("Position is located: Mayo Hospital- Dover-Foxcroft, ME") == \
        "Mayo Hospital - Dover-Foxcroft, ME"


def test_nl_entity_in_another_town_stays():
    r = ROWS["nl entity inpatient provider"]
    name, city, _st = _decide(r, city="Brewer")
    assert (name, city) == (A.NL, "Brewer")


def test_nl_no_body_keeps_the_stored_site():
    r = ROWS["nl located hospital"]
    A.set_known_facilities([{"hospital_system": A.NL, "job_id": r["job_id"],
                             "hospital_name": "Northern Light Maine Coast Hospital", "city": "Ellsworth", "state": "ME"}])
    assert _decide(r, description="")[0] == "Northern Light Maine Coast Hospital"
    A.set_known_facilities([])
    assert _decide(r, description="")[0] == A.NL


# ── KU reading ──────────────────────────────────────────────────────────────
def test_ku_header_shapes():
    assert A.ku_locations("Position Title\n\nRN\n\nDays - Full Time\n\nBell Hospital\n\nPosition Summary / x") == \
        ["Bell Hospital"]
    assert A.ku_locations("Position Title\n\nRN\n\nOlathe Hospital\n\nPosition Summary / x") == ["Olathe Hospital"]
    assert A.ku_locations("Position Title\n\nRN\n\nNights - Full Time, Nights - PRN\n\nPosition Summary") == []
    assert A.ku_locations("Position Title\n\nMRI Tech\n\nVaries\n\nQuivira Medical Pavilion A 12000 W. 151st St."
                          "\n\nPosition Summary") == ["Quivira Medical Pavilion A 12000 W. 151st St."]
    assert A.ku_locations("no header") is None


def test_ku_office_work_location_stays_and_ku_has_no_town_table():
    r = ROWS["ku medical pavilion"]
    j = _job(r)
    assert A.apply_facility(j) == "office"
    assert j.hospital_name == A.KU
    # a town names nothing for KU: half its work locations are clinics and
    # offices in the hospitals' own towns
    assert A.KU not in TOWN.SPECS
    assert TOWN.apply_job(j) == "not-table"


def test_ku_link_site_for_a_row_the_database_does_not_hold():
    r = ROWS["ku no location"]
    url = r["url"].replace("/job/Kansas-City-KS/", "/job/Olathe-Hospital/")
    assert A.ku_url_site(url).name == "University of Kansas Health System Olathe Hospital"
    assert A.ku_url_site(r["url"]) is None
    assert A.ku_url_site("https://x/careers/job/Liberty-Hospital-Sports-Medicine/Scheduler_R-1") is None
    assert A.ku_url_site("https://x/careers/job/Remote/Coder_R-2") is None
    j = _job(r, description="")
    j.url = url
    assert A.apply_facility(j) == "url"
    # the row sat at the stamped default town: the hospital's own town
    assert (j.hospital_name, j.city, j.state) == ("University of Kansas Health System Olathe Hospital", "Olathe", "KS")


def test_ku_no_body_keeps_a_stored_label_over_the_link():
    r = ROWS["ku no location"]
    A.set_known_facilities([{"hospital_system": A.KU, "job_id": r["job_id"], "hospital_name": A.KU,
                             "city": r["city"], "state": r["state"]}])
    j = _job(r, description="")
    j.url = r["url"].replace("/job/Kansas-City-KS/", "/job/Olathe-Hospital/")
    assert A.apply_facility(j) == "kept-label"
    assert (j.hospital_name, j.city) == (A.KU, r["city"])


def test_ku_no_body_keeps_the_stored_site_and_its_town():
    r = ROWS["ku olathe hospital at default"]
    A.set_known_facilities([{"hospital_system": A.KU, "job_id": r["job_id"],
                             "hospital_name": "University of Kansas Health System Olathe Hospital",
                             "city": "Olathe", "state": "KS"}])
    j = _job(r, description="")
    assert A.apply_facility(j) == "kept"
    assert (j.hospital_name, j.city, j.state) == ("University of Kansas Health System Olathe Hospital",
                                                  "Olathe", "KS")


# ── Rochester towns ─────────────────────────────────────────────────────────
def test_rochester_address_town():
    assert A.rochester_address_town("Job Location (Full Address):\n\n475 Alumni Rd, Rochester, New York, "
                                    "United States of America, 14627\n") == ("Rochester", "NY")
    assert A.rochester_address_town("Job Location (Full Address):\n\nRemote Work - New York, Albany, New York, "
                                    "United States of America, 12224\n") is None
    assert A.rochester_address_town("Job Location (Full Address):\n\nUndetermined, Rochester, New York, "
                                    "United States of America, 14642\n") == ("Rochester", "NY")
    assert A.rochester_address_town("nothing") is None


def test_rochester_no_body_keeps_the_stored_town():
    r = ROWS["rochester building town"]
    A.set_known_facilities([{"hospital_system": A.ROCHESTER, "job_id": r["job_id"], "hospital_name": A.ROCHESTER,
                             "city": "Rochester", "state": "NY"}])
    j = _job(r, description="")
    assert A.apply_facility(j) == "kept"
    assert (j.hospital_name, j.city) == (A.ROCHESTER, "Rochester")


def test_a_named_row_is_left_alone():
    r = ROWS["rochester building town"]
    j = _job(r, name="Strong Memorial Hospital")
    assert A.apply_facility(j) == "other"
    assert j.city == r["city"]


# ── Indian Health Service ───────────────────────────────────────────────────
def _usajobs_item(summary, towns, title="Nurse (Case Management)", pid="IHS-27-NN-1"):
    names = {"NM": "New Mexico", "AZ": "Arizona", "NE": "Nebraska", "MN": "Minnesota", "ND": "North Dakota"}
    return {"MatchedObjectDescriptor": {
        "PositionTitle": title, "PositionID": pid, "PositionURI": "https://www.usajobs.gov/job/1",
        "OrganizationName": "Indian Health Service", "QualificationSummary": "To qualify ...",
        "PositionLocation": [{"LocationName": f"{c}, {names[s]}", "CityName": f"{c}, {names[s]}",
                              "CountryCode": "United States", "CountrySubDivisionCode": names[s]}
                             for c, s in towns],
        "PositionRemuneration": [{"MinimumRange": "70000", "MaximumRange": "90000", "RateIntervalCode": "PA"}],
        "UserArea": {"Details": {"JobSummary": summary, "SubAgencyName": ""}}}}


IHS_EXPECT = {
    "889109800": "Northern Navajo Medical Center",               # "... at the Northern Navajo Medical Center, Shiprock, NM"
    "889970700": "Zuni Comprehensive Community Health Center",   # spelled "Zuni Comprehensive Community Center"
    "890053000": "Chinle Service Unit",
    "883144900": "Great Plains Area Drug Dependency Unit",
    "878596200": None,     # names the Emergency Department only
    "882994100": None,     # "At the Indian Health Service (IHS) ..."
    "889642300": None,     # "one of two Public Health Service hospitals in the Bemidji Program Area"
}


@pytest.mark.parametrize("cn", sorted(IHS_EXPECT))
def test_ihs_summary_facility_real_summaries(cn):
    p = PAGES[cn]
    (city, state), = p["towns"]
    m = _usajobs_item(p["job_summary"], [(city, state)])["MatchedObjectDescriptor"]
    assert A.ihs_summary_facility(m, city, state) == IHS_EXPECT[cn]


def test_ihs_facility_only_for_its_own_town():
    p = PAGES["889109800"]
    m = _usajobs_item(p["job_summary"], [("Shiprock", "NM")])["MatchedObjectDescriptor"]
    assert A.ihs_summary_facility(m, "Kayenta", "AZ") is None
    assert A.ihs_summary_facility(m, "Shiprock", "NM") == "Northern Navajo Medical Center"


def test_ihs_rows_through_the_adapter():
    p = PAGES["889109800"]
    rows = scraper._usajobs_jobs(_usajobs_item(p["job_summary"], [("Shiprock", "NM"), ("Gallup", "NM")]),
                                 "Indian Health Service")
    got = {(j.city, j.hospital_name) for j in rows}
    assert got == {("Shiprock", "Northern Navajo Medical Center"), ("Gallup", "Indian Health Service")}
    # a summary naming no facility keeps the agency on every row
    rows = scraper._usajobs_jobs(_usajobs_item(PAGES["889642300"]["job_summary"], [("Red Lake", "MN")]),
                                 "Indian Health Service")
    assert [j.hospital_name for j in rows] == ["Indian Health Service"]


def test_ihs_hook_that_raises_keeps_the_agency(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen")
    monkeypatch.setattr(A, "ihs_summary_facility", boom)
    p = PAGES["889109800"]
    rows = scraper._usajobs_jobs(_usajobs_item(p["job_summary"], [("Shiprock", "NM")]), "Indian Health Service")
    assert [j.hospital_name for j in rows] == ["Indian Health Service"]


def test_two_ihs_facilities_of_one_town_name_none():
    m = _usajobs_item("Serves the Chinle Comprehensive Health Care Facility and the Pinon Health Center.",
                      [("Chinle", "AZ")])["MatchedObjectDescriptor"]
    assert A.ihs_summary_facility(m, "Chinle", "AZ") == "Chinle Comprehensive Health Care Facility"
    m = _usajobs_item("Located at the Chinle Comprehensive Health Care Facility, Chinle Service Unit.",
                      [("Chinle", "AZ")])["MatchedObjectDescriptor"]
    assert A.ihs_summary_facility(m, "Chinle", "AZ") is None


# ── tables ──────────────────────────────────────────────────────────────────
def test_tables_are_clean():
    for spec in (A.NL_SPEC, A.KU_SPEC):
        for name, site in spec.names.items():
            assert "—" not in name and "–" not in name
            assert site.source.startswith(("cms ", "board", "address "))
    for s in A.TOWN_SYSTEMS:
        spec = TOWN.SPECS[s]
        names = [h[0] for h in spec.hospitals]
        assert len(names) == len(set(names))
        assert s in TOWN.BOILERPLATE, f"{s}: no boilerplate keys"
    assert A.TOWN_SYSTEMS == {A.EMORY, A.COOPER, A.UMMC}
    names = [f[0] for f in A.IHS_FACILITIES]
    assert len(names) == len(set(names))


def test_load_known_without_credentials(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    assert A.load_known_facilities() == 0


def test_a_row_that_raises_keeps_its_name(monkeypatch):
    r = ROWS["nl located hospital"]
    j = _job(r)

    def boom(*a, **k):
        raise RuntimeError("unforeseen")
    monkeypatch.setattr(A, "read_northern_light", boom)
    assert A.apply_facilities([j]) == {A.NL: {"error": 1}}
    assert (j.hospital_name, j.city) == (A.NL, r["city"])


def test_finalize_jobs_payload(monkeypatch):
    """The rows through scraper.finalize_jobs (sequential normalize): the
    pass runs before the town tables and the payload carries the names."""
    monkeypatch.setenv("FINALIZE_WORKERS", "1")
    cases = ("nl located hospital", "ku olathe hospital at default", "ku medical pavilion", "rochester laser lab",
             "cooper cape", "emory atlanta shared", "ummc greenwood")
    out = scraper.finalize_jobs([_job(ROWS[c]) for c in cases])
    got = {(o["hospital_system"], o["job_id"]): (o["hospital_name"], o["city"], o["state"]) for o in out}
    for c in cases:
        r = ROWS[c]
        g = got[(r["hospital_system"], str(r["job_id"]))]
        if c in EXPECT:
            want = EXPECT[c]
            assert g == (want[0], want[1] if want[1] is not None else r["city"], want[2]), c
        else:
            assert g[0] == r["hospital_system"], c
