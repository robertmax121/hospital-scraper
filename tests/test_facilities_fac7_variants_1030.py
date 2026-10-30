"""2026-10-30 (facility scale phase 7, variants group): names that stand for
the system but are not its label (facilities_fac7_variants): placeholders
("N/A"), Ascension's site lines, the HSHS and UVA Health town tables.

Fixture (tests/fixtures/fac7variants1030/ascension_site_lines.json): the
value of every active "Ascension Health" row's first Facility Name / Facility
/ Hospital line on 2026-10-30, with the row's town and the number of rows
(facility names only). Every other case is a posting spelled the way the
stored rows spell it, with no person's name, e-mail or phone.
No network, no database."""
import json
import os
from collections import Counter

import pytest

import facilities_fac7_variants as V
import facilities_town as TOWN
import facility_names as F
import scraper
from conftest import FIXTURES

FIX = os.path.join(FIXTURES, "fac7variants1030")
with open(os.path.join(FIX, "ascension_site_lines.json"), encoding="utf-8") as _f:
    ASC_LINES = json.load(_f)["rows"]
PAD = "\n\n" + "Provides patient care and support to the unit team. " * 40


def _job(system, name, *, city="", state="", description="", title="Registered Nurse", job_id="J1", url=""):
    return scraper.Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
                       location=f"{city}, {state}" if city else state, specialty="", job_type="", url=url,
                       job_id=job_id, posted_date="", description=description, ats_platform="")


def _canon(s):
    return scraper.HOSPITAL_SYSTEM_ALIASES.get(s, s)


@pytest.fixture(autouse=True)
def _no_stored():
    V.set_known_facilities([])
    TOWN.set_stored([])
    F.set_known_facilities([])
    yield
    V.set_known_facilities([])
    TOWN.set_stored([])
    F.set_known_facilities([])


# ── Placeholders ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("value", ["N/A", "n/a", " N / A ", "NA", "Unknown", "none", "TBD", "-", "--", "Not Specified"])
def test_placeholders_name_nothing(value):
    assert V.is_placeholder(value)


@pytest.mark.parametrize("value", ["", None, "Nash General Hospital", "NAMC", "Unknown Hospital", "North Area",
                                   "Remote", "CHRISTUS Health"])
def test_real_names_are_no_placeholders(value):
    assert not V.is_placeholder(value)


def test_a_placeholder_row_takes_the_system_label():
    """CHRISTUS's feed gives "N/A" as the business unit on 232 rows: the row
    is a label row like every other, under the system's own name."""
    jobs = [_job("CHRISTUS Health", "N/A", city="Santa Fe", state="NM"),
            _job("CHRISTUS Health", "CHRISTUS Spohn Health System", city="Alice", state="TX", job_id="J2"),
            _job("Some System", "Unknown", city="Tyler", state="TX", job_id="J3")]
    assert V.normalize_jobs(jobs) == {"CHRISTUS Health": 1, "Some System": 1}
    assert [j.hospital_name for j in jobs] == ["CHRISTUS Health", "CHRISTUS Spohn Health System", "Some System"]
    assert (jobs[0].city, jobs[0].state) == ("Santa Fe", "NM")


def test_stored_placeholder_change():
    row = {"hospital_system": "CHRISTUS Health", "hospital_name": "N/A", "city": "Santa Fe", "state": "NM",
           "description": "Summary:" + PAD}
    new, city, state, src = V.stored_change(row)
    assert (new, city, state) == ("CHRISTUS Health", "Santa Fe", "NM")
    assert "placeholder" in src


# ── HSHS (town table) ───────────────────────────────────────────────────────

HSHS_URL = "https://hshs.wd1.myworkdayjobs.com/hshscareers/job/{}/Registered-Nurse_R1000001"


def test_hshs_label_variant_is_read_as_the_label():
    spec = TOWN.SPECS["HSHS"]
    assert spec.is_label("HSHS Hospitals") and spec.is_label("HSHS") and spec.is_label("N/A")
    assert not spec.is_label("HSHS St. John's Hospital")
    assert TOWN.EMIT["HSHS Hospitals"] is spec
    assert scraper.HOSPITAL_SYSTEM_ALIASES["HSHS Hospitals"] == "HSHS"


def test_hshs_town_gives_its_one_hospital():
    body = "Facility: St. John's Hospital\nDepartment: Cath Lab" + PAD
    got = TOWN.decide("HSHS", "HSHS Hospitals", "Springfield", "IL", "Cardiovascular Invasive Technologist III",
                      body, HSHS_URL.format("Springfield-IL"))
    assert got == ("HSHS St. John's Hospital", "Springfield", "IL", "named")
    got = TOWN.decide("HSHS", "HSHS Hospitals", "O'Fallon", "IL", "Emergency Technician", "Summary" + PAD,
                      HSHS_URL.format("OFallon-IL"))
    assert got == ("HSHS St. Elizabeth's Hospital", "O'Fallon", "IL", "town")


def test_hshs_green_bay_needs_the_posting_to_name_one_of_its_two_hospitals():
    body = "Department: Med Surg Unit\nFacility: St. Mary's Hospital" + PAD
    got = TOWN.decide("HSHS", "HSHS Hospitals", "Green Bay", "WI", "Registered Nurse (RN)-Medical/Surgical",
                      body, HSHS_URL.format("Green-Bay-WI"))
    assert got[:3] == ("HSHS St. Mary's Hospital Medical Center", "Green Bay", "WI")
    got = TOWN.decide("HSHS", "HSHS Hospitals", "Green Bay", "WI", "Registered Nurse (RN)", "Summary" + PAD,
                      HSHS_URL.format("Green-Bay-WI"))
    assert got == (None, None, None, "shared")


def test_hshs_breese_and_highland_keep_their_own_st_josephs():
    body = "Facility: St. Joseph's Hospital" + PAD
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Breese", "IL", "Registered Nurse (RN), Emergency Department",
                       body, HSHS_URL.format("Breese-IL"))[0] == "HSHS St. Joseph's Hospital Breese"
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Highland", "IL", "Registered Nurse (RN), Emergency Department",
                       body, HSHS_URL.format("Highland-IL"))[0] == "HSHS St. Joseph's Hospital Highland"


def test_hshs_clinics_remote_and_closed_towns_keep_the_label():
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Carbondale", "IL",
                       "Medical Assistant - Prairie Cardiovascular Clinic", "Summary" + PAD,
                       HSHS_URL.format("Carbondale-IL"))[3] in ("offsite", "no-town")
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Springfield", "IL", "Medical Assistant I - Family Medicine Clinic",
                       "Summary" + PAD, HSHS_URL.format("Springfield-IL"))[3] == "offsite"
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Springfield", "IL", "Coder II - Remote", "Summary" + PAD,
                       HSHS_URL.format("Springfield-IL"))[3] == "remote"
    # Sacred Heart (Eau Claire) closed in 2024: the town names no hospital
    assert TOWN.decide("HSHS", "HSHS Hospitals", "Eau Claire", "WI", "Registered Nurse", "Summary" + PAD,
                       HSHS_URL.format("Eau-Claire-WI"))[3] == "no-town"


def test_hshs_stamped_springfield_needs_the_posting_to_name_the_town():
    """The scraper stamps Springfield, IL on an HSHS row without a location;
    only a Workday site or a location line that names the town counts."""
    got = TOWN.decide("HSHS", "HSHS Hospitals", "Springfield", "IL", "Registered Nurse", "Summary" + PAD,
                      "https://hshs.wd1.myworkdayjobs.com/hshscareers/job/WWHS-Lab_R1031184")
    assert got == (None, None, None, "default")


def test_hshs_childrens_hospitals_are_named_only_when_the_posting_names_them():
    body = "Facility: St. John's Children's Hospital\nDepartment: Pediatrics" + PAD
    got = TOWN.decide("HSHS", "HSHS Hospitals", "Springfield", "IL", "Registered Nurse (RN) - Pediatrics", body,
                      HSHS_URL.format("Springfield-IL"))
    assert got[0] == "HSHS St. John's Children's Hospital"


def test_hshs_job_at_run_time_and_kept_on_a_night_without_body():
    j = _job("HSHS Hospitals", "HSHS Hospitals", city="Decatur", state="IL", description="Summary" + PAD,
             url=HSHS_URL.format("Decatur-IL"), title="Registered Nurse (RN) - Intensive Care Unit (ICU)")
    assert TOWN.apply_jobs([j], canon=_canon) == {"HSHS": {"town": 1}}
    assert j.hospital_name == "HSHS St. Mary's Hospital"
    TOWN.set_stored([{"hospital_system": "HSHS", "job_id": "J9", "hospital_name": "HSHS St. Mary's Hospital",
                      "city": "Decatur", "state": "IL"}])
    k = _job("HSHS Hospitals", "HSHS Hospitals", city="Decatur", state="IL", description="", job_id="J9",
             url=HSHS_URL.format("Decatur-IL"))
    assert TOWN.apply_job(k, canon=_canon) == "kept" and k.hospital_name == "HSHS St. Mary's Hospital"


def test_hshs_names_agree_with_the_workday_table():
    """The Workday table names four hospital sites; the town table spells
    them the same way."""
    import facilities_workday as WD
    wd = {v[0] for v in WD.TABLES["HSHS Hospitals"].values() if v[0]}
    assert wd <= {h[0] for h in TOWN.SPECS["HSHS"].hospitals}


# ── UVA Health (town table) ─────────────────────────────────────────────────

def test_uva_community_health_rows_take_the_town_hospital():
    got = TOWN.decide("UVA Health", "UVA Community Health", "Manassas", "VA", "Surgical Tech", "Summary" + PAD)
    assert got == ("UVA Health Prince William Medical Center", "Manassas", "VA", "town")
    body = "Location: Prince William Medical Center, 8700 Sudley Road, Manassas, VA" + PAD
    got = TOWN.decide("UVA Health", "UVA Community Health", "Manassas", "VA", "PRN Inpatient Psychiatrist MD/DO",
                      body)
    assert got[:3] == ("UVA Health Prince William Medical Center", "Manassas", "VA")
    assert TOWN.decide("UVA Health", "UVA Community Health", "Culpeper", "VA", "Registered Nurse-Med/Tele",
                       "Summary" + PAD)[0] == "UVA Health Culpeper Medical Center"
    assert TOWN.decide("UVA Health", "UVA Community Health", "Haymarket", "VA", "CT Tech (PRN)",
                       "Summary" + PAD)[0] == "UVA Health Haymarket Medical Center"


def test_uva_clinic_towns_and_charlottesville_keep_the_label():
    assert TOWN.decide("UVA Health", "UVA Community Health", "Gainesville", "VA", "Registered Nurse",
                       "Summary" + PAD)[3] == "no-town"
    assert TOWN.decide("UVA Health", "UVA Community Health", "Charlottesville", "VA", "Registered Nurse",
                       "Summary" + PAD)[3] == "shared"
    assert TOWN.decide("UVA Health", "UVA Community Health", "Manassas", "VA", "Medical Assistant - Primary Care",
                       "Summary" + PAD)[3] == "offsite"
    # a row already on a site is left alone
    assert TOWN.decide("UVA Health", "UVA Medical Center", "Culpeper", "VA", "Registered Nurse",
                       "Summary" + PAD)[3] == "named-already"


# ── Ascension (site lines) ──────────────────────────────────────────────────

def _asc(line, city, state, title="Registered Nurse", name="Ascension Health", job_id="A1"):
    return _job("Ascension Health", name, city=city, state=state, title=title, job_id=job_id,
                description="Your future role at a glance\n\nLocation: x\n\n" + line + PAD)


def test_ascension_hospital_spelled_otherwise():
    j = _asc("Facility: Seton Medical Ctr Austin", "Austin", "TX")
    assert V.apply_facility(j, _canon) == "text"
    assert (j.hospital_name, j.city, j.state) == ("Ascension Seton Medical Center Austin", "Austin", "TX")
    j = _asc("Facility: Ascension Williamson", "Round Rock", "TX")
    assert V.apply_facility(j, _canon) == "text" and j.hospital_name == "Ascension Seton Williamson"


def test_ascension_hospital_line_facility_names_did_not_read():
    j = _asc("Hospital: Ascension Sacred Heart Hospital Pensacola", "Pensacola", "FL")
    assert V.apply_facility(j, _canon) == "text"
    assert j.hospital_name == "Ascension Sacred Heart Pensacola"
    # facility_names itself reads only "Facility:" lines
    k = _asc("Hospital: Ascension Sacred Heart Hospital Pensacola", "Pensacola", "FL")
    assert F.apply_facility(k) == "none" and k.hospital_name == "Ascension Health"


def test_ascension_named_sites_in_their_own_town():
    for line, city, st, want in (
            ("Facility: Ascension Via Christi West ER", "Wichita", "KS", "Ascension Via Christi West ER"),
            ("Facility: Alexian Village of Tennessee", "Signal Mountain", "TN", "Alexian Village of Tennessee"),
            ("Facility Name: Ascension St. Vincent's Primary Care - Nocatee", "Jacksonville", "FL",
             "Ascension St. Vincent's Primary Care - Nocatee"),
            ("Facility: Via Christi, St Peters", "DERBY", "KS", "Ascension Via Christi St. Peter"),
            ("Facility: Saint Francis Place", "Milwaukee", "WI", "Ascension Living St. Francis Place"),
            ("Facility Name: Wamego Health Center - Emergency Department", "Wamego", "KS", "Wamego Health Center")):
        j = _asc(line, city, st)
        assert V.apply_facility(j, _canon) == "text", line
        assert j.hospital_name == want and j.city == city and j.state == st, line


def test_ascension_site_in_another_town_keeps_the_label():
    j = _asc("Facility: Wauwatosa Ambulatory Center", "Brookfield", "WI")
    assert V.apply_facility(j, _canon) == "elsewhere" and j.hospital_name == "Ascension Health"


@pytest.mark.parametrize("line", [
    "Facility: Ascension Medical Group",
    "Facility: Ascension Medical Group, clinic",
    "Facility: Multiple Ascension sites",
    "Facility: Ascension Saint Thomas Midtown, West, Rutherford, River Park and Westlawn",
    "Facility: Elmbrook & St. Joseph Hospitals",
    "Facility: AW Glendale Corporate Office",
    "Facility: Ascension Menasha Midway Office Building",
    "Facility: Saint Thomas Medical Partners Nashville",
    "Facility: Ascension Sacred Heart Outpatient Imaging",
    "Facility: Remote",
    "Facility: Ascension Saint Thomas",
])
def test_ascension_groups_offices_lists_and_services_name_nothing(line):
    j = _asc(line, "Nashville", "TN")
    assert V.apply_facility(j, _canon) in ("unknown", "elsewhere", "none")
    assert j.hospital_name == "Ascension Health"


def test_ascension_no_line_no_body_and_other_rows():
    j = _asc("Department: Skilled Nursing", "Wichita", "KS")
    assert V.apply_facility(j, _canon) == "none"
    k = _job("Ascension Health", "Ascension Health", city="Wichita", state="KS", description="", job_id="A9")
    assert V.apply_facility(k, _canon) == "no-body"
    named = _asc("Facility: Ascension Via Christi West ER", "Wichita", "KS", name="Ascension Via Christi St. Joseph")
    assert V.apply_facility(named, _canon) == "other" and named.hospital_name == "Ascension Via Christi St. Joseph"
    other = _job("Some System", "Some System", city="Wichita", state="KS", description="Facility: X" + PAD)
    assert V.apply_facility(other, _canon) == ""


def test_ascension_stored_site_is_kept_on_a_night_without_body():
    V.set_known_facilities([
        {"hospital_system": "Ascension", "job_id": "A9", "hospital_name": "Ascension Via Christi West ER",
         "city": "Wichita", "state": "KS"},
        {"hospital_system": "Ascension", "job_id": "A8", "hospital_name": "Ascension Via Christi St. Joseph",
         "city": "Wichita", "state": "KS"},          # facility_names' own hospital: its own _KNOWN keeps it
        {"hospital_system": "Ascension", "job_id": "A7", "hospital_name": "Some Clinic", "city": "Wichita",
         "state": "KS"}])
    assert set(V._KNOWN) == {"A9"}
    k = _job("Ascension Health", "Ascension Health", city="Wichita", state="KS", description="", job_id="A9")
    assert V.apply_facility(k, _canon) == "kept" and k.hospital_name == "Ascension Via Christi West ER"


def test_ascension_table_on_the_stored_site_lines():
    """The reviewed table on the 2026-10-30 stored lines: what it names, and
    that every value it names is a value two or more postings used or a
    hospital facility_names lists (a spelling one posting used names a site
    other postings name too: "Ascension Via Christi St. Peter." with its
    period)."""
    named, rows_named = Counter(), 0
    by_value = Counter()
    for x in ASC_LINES:
        by_value[x["value"]] += x["rows"]
    for x in ASC_LINES:
        name, city, st, v = V.ascension_site("Facility: " + x["value"] + PAD, x["city"], x["state"])
        if name:
            named[name] += x["rows"]
            rows_named += x["rows"]
    hospitals = F.spec_for("Ascension").hospitals
    for name, n in named.items():
        assert n >= 2 or name in hospitals, name
    assert sum(by_value.values()) == 989
    assert 290 <= rows_named <= 320, rows_named
    for want in ("Sacred Heart Village", "Ascension Via Christi West ER", "Ascension Sacred Heart Pensacola",
                 "Ascension Seton Cedar Park", "Ascension Living St. Francis Place"):
        assert named[want] >= 4, want
    for never in ("Ascension Medical Group", "Ascension Medical Clinic", "Ascension Saint Thomas", "Remote"):
        assert never not in named


def test_ascension_table_entries_are_clean():
    labels = F.spec_for("Ascension").labels
    for name, site in V.ASCENSION_SPEC.names.items():
        assert name == name.strip() and "  " not in name and name not in labels, name
        assert site.state in {"AL", "DC", "FL", "IL", "IN", "KS", "LA", "MD", "MI", "MO", "OK", "TN", "TX", "WI"}
        assert site.source.split()[0] in ("cms", "board", "facility_names"), site
        if site.source == "facility_names":
            assert F.spec_for("Ascension").hospitals[name] == (site.city, site.state), name


def test_ascension_stored_change():
    row = {"hospital_system": "Ascension", "hospital_name": "Ascension Health", "city": "Avilla", "state": "IN",
           "description": "Your future role at a glance\nFacility: Sacred Heart Village\n" + PAD}
    new, city, st, src = V.stored_change(row)
    assert (new, city, st) == ("Sacred Heart Village", "Avilla", "IN") and "Sacred Heart Village" in src
    row["hospital_name"] = "Ascension Via Christi St. Joseph"
    assert V.stored_change(row) is None


# ── The scraper's order ─────────────────────────────────────────────────────

def test_passes_run_in_order():
    src = open(scraper.__file__, encoding="utf-8").read()
    i = src.index("def finalize_jobs(")
    codes = src.index('_facility_pass("Facility codes"', i)
    ph = src.index("facilities_fac7_variants.normalize_jobs", i)
    fn = src.index("facility_names.apply_facilities(kept)", i)
    f6b = src.index('_facility_pass("Phase 6 B"', i)
    f7 = src.index("facilities_fac7_variants.apply_facilities", i)
    town = src.index('_facility_pass("Town tables"', i)
    assert codes < ph < fn < f6b < f7 < town
    run = src.index("async def run_all(")
    assert src.index("facilities_fac7_variants.load_known_facilities", run) < src.index("facilities_town.load_stored",
                                                                                         run)


def test_finalize_jobs_names_the_variants():
    jobs = [_asc("Facility: Ascension Via Christi West ER", "Wichita", "KS", job_id="F1"),
            _job("HSHS Hospitals", "HSHS Hospitals", city="Effingham", state="IL", job_id="F2",
                 title="Registered Nurse (RN) - Emergency Department", description="Summary" + PAD,
                 url=HSHS_URL.format("Effingham-IL")),
            _job("CHRISTUS Health", "N/A", city="Santa Fe", state="NM", job_id="F3", description="Summary" + PAD)]
    rows = scraper.finalize_jobs(jobs)
    got = {r["job_id"]: r["hospital_name"] for r in rows}
    assert got == {"F1": "Ascension Via Christi West ER", "F2": "HSHS St. Anthony's Memorial Hospital",
                   "F3": "CHRISTUS Health"}
