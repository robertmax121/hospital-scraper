"""2026-10-29 (facility scale phase 3): the business a UnitedHealth Group
posting names (facilities_uhg). careers.unitedhealthgroup.com stamps every
row "UnitedHealth Group"; the posting's opening line names the agency, clinic,
pharmacy or practice. Owner decisions: named sites (named clinics and centers,
agencies' named branches) show their own name; generic offices, home-based
and remote jobs show the system name with the town; never guess a town.

Fixture (tests/fixtures/facility_uhg/stored_bodies.json): real bodies stored
in hospital_jobs, read 2026-10-29, cut to their first 900 characters, with
each row's stored system, label, title, town and job id. No network, no
database."""
import json
import os

import pytest

import facilities_uhg as U
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_uhg")
with open(os.path.join(FIX, "stored_bodies.json"), encoding="utf-8") as _f:
    ROWS = {r["id"]: r for r in json.load(_f)["rows"]}


@pytest.fixture(autouse=True)
def _no_known():
    U.set_known_facilities([])
    U.UNKNOWN.clear()
    yield
    U.set_known_facilities([])
    U.UNKNOWN.clear()


def _job(rid=None, *, title="RN", city="", state="", description="", job_id="1", name=U.SYSTEM,
         system=U.SYSTEM):
    if rid is not None:
        r = ROWS[rid]
        title, city, state, description, job_id, name = (r["title"], r["city"], r["state"], r["description"],
                                                         r["job_id"], r["hospital_name"])
    return Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="",
               url=f"https://careers.unitedhealthgroup.com/job/x/y/34088/{job_id}", job_id=str(job_id),
               posted_date="", description=description, ats_platform="TalentBrew")


# (row id, hospital_name, how, city, state): each read against its posting on 2026-10-29
EXPECTED = [
    # LHC Group agencies: "Explore opportunities with <agency>, a part of LHC Group" (the applied-to rows first)
    (1974348, "Centralia Assured Home Health", "body", "Lake Morton Berrydale", "WA"),
    (3547290, "Elite Home Health", "body", "Katy", "TX"),
    (16071855, "Mederi Caretenders Home Health", "body", "Orlando", "FL"),
    (36686199, "Generations Home Health", "body", "Virginia Beach", "VA"),
    (1825945, "CHRISTUS Hospice St. Frances Cabrini", "body", "Alexandria", "LA"),      # "St." is not a name end
    (3548985, "Assured Home Health", "body", "Redmond", "WA"),                          # "a part of L HC Group"
    (3550127, "Elk Valley", "body", "Spring Hill", "TN"),                               # no service word: table
    (23731895, "LHC HomeCare of South Alabama", "body", "Opp", "AL"),                   # "LHC" in a public name: table
    (3551318, "Caretenders", "body", "Waltham", "MA"),                                  # "CAREtenders" folds
    (15440586, "Caretenders Westborough", "body", "Milford", "MA"),                     # a branch keeps its suffix
    (1973884, "Florida Home Health", "body", "Sebring", "FL"),                          # "Floruda Home Health" typo
    (8090627, "Heart 'n Home Hospice", "body", "La Grande", "OR"),                     # "Heart n Home Hospice"
    (2502406, "Heart 'n Home Hospice", "body", "Bend", "OR"),                          # "Heart N Home Hospice"
    # Amedisys
    (33014770, "Amedisys Hospice", "body", "Lakeland", "FL"),
    (37899039, "Amedisys", "body", "Fresno", "CA"),                                     # "Join Amedisys , one of"
    (38492619, "Amedisys", "body", "Asheboro", "NC"),
    (38557878, "Amedisys", "body", "Washington", "PA"),                                 # Washington, PA is a town
    # medical groups, clinics, centers
    (34851023, "WellMed", "body", "Austin", "TX"),
    (3547353, "WellMed", "body", "Rockport", "TX"),                                     # "A partner of the Optum Care network"
    (38837715, "Kelsey-Seybold Clinic", "body", "Houston", "TX"),
    (1974538, "Optum NY", "body", "Hicksville", "NY"),                                  # "Optum NY, (formerly ...) is seeking"
    (14013094, "Optum Washington", "body", "Marysville", "WA"),                         # "Optum WA, (formerly The Polyclinic)"
    (4675150, "Crystal Run Health", "title", "Middletown", "NY"),                       # the title's group over Optum NY's paragraph
    (3549956, "OptumCare Cancer Care", "body", "Las Vegas", "NV"),
    (4445577, "Encompass Medical Group", "body", "Independence", "MO"),
    (33534546, "American Health Network", "body", "Indianapolis", "IN"),                # "(AHN)" dropped
    # pharmacies, behavioral practices
    (1974023, "Genoa Healthcare", "body", "Saginaw", "MI"),
    (1973479, "CPS", "body", "West Grove", "PA"),
    (3548371, "Springfield Behavioral of Pennsylvania", "body", "Trevose", "PA"),       # no comma before "part of"
    (5359856, "Springfield Behavioral of Pennsylvania", "body", "Trevose", "PA"),
    (3549650, "Snyder Therapeutic Services of Pennsylvania", "body", "Ambler", "PA"),   # "J. Snyder" is one name
    # the title names the group when the body names none (or only UHG corporate)
    (3548676, "Crystal Run Health", "title", "Newburgh", "NY"),
    (37898903, "Kelsey-Seybold Clinic", "title", "Houston", "TX"),                      # no stored body
    (3547581, "Kelsey-Seybold Clinic", "title", "Richmond", "TX"),                      # state filled: Kelsey-Seybold is Texas
    # towns from the posting's own words over a state-wide "town"
    (1825894, "WellMed", "body", "Harlingen", "TX"),                                    # "Texas, TX" -> "... - Harlingen, TX"
    (3550120, "Apex Home Healthcare", "body", "Ormond Beach", "FL"),                    # "Florida, FL" -> "... - Ormond Beach FL"
    (2294390, "Patient Care", "body", "New Jersey", "NJ"),                              # the posting names no town: stays
    # the system name (decision 2)
    (37898799, U.SYSTEM, "none", "Louisville", "KY"),                                   # an office ("Like you, UnitedHealth Group")
    (4674398, U.SYSTEM, "none", "Mount Kisco", "NY"),                                   # "Optum is a global organization"
    (12978899, U.SYSTEM, "remote", "Eden Prairie", "MN"),
    (38492631, U.SYSTEM, "remote", "Las Vegas", "NV"),
    (3548043, U.SYSTEM, "remote", "Houston", "TX"),                                     # Kelsey-Seybold, but remote
    (1825916, U.SYSTEM, "label", "Dillon", "SC"),                                       # HouseCalls: home-based
    (1973848, U.SYSTEM, "label", "Norfolk", "VA"),                                      # Senior Community Care
    (4904814, U.SYSTEM, "label", "Warrenton", "VA"),                                    # "LHC Group" alone: no agency
    (1974053, U.SYSTEM, "label", "Marion", "IN"),                                       # "[agency name]" placeholder
    (1973545, U.SYSTEM, "label", "Lafayette", "LA"),                                    # "Eunice LA PCS": internal code
    (8531318, U.SYSTEM, "label", "Ellsworth", "ME"),                                    # Northern Light Health: another system
    (31147673, U.SYSTEM, "label", "Worcester", "MA"),                                   # "Atrius Health and Reliant ...": two
    (5588875, U.SYSTEM, "none", "New York", "NY"),                                      # New York City is a town
    (1973855, U.SYSTEM, "none", "Makati City", ""),                                     # not a US site
]


@pytest.mark.parametrize("rid,name,how,city,state", EXPECTED, ids=[str(e[0]) for e in EXPECTED])
def test_stored_posting_names_its_site(rid, name, how, city, state):
    r = ROWS[rid]
    assert U.uhg_site(r["title"], r["description"]) == (name, how)
    assert U.place(name, r["title"], r["description"], r["city"], r["state"]) == (city, state)


def test_every_fixture_row_is_checked():
    assert {e[0] for e in EXPECTED} == set(ROWS)


@pytest.mark.parametrize("rid,name,how,city,state", EXPECTED, ids=[str(e[0]) for e in EXPECTED])
def test_apply_facility_writes_name_and_town(rid, name, how, city, state):
    job = _job(rid)
    assert U.apply_facility(job) == how
    assert job.hospital_system == U.SYSTEM
    assert (job.hospital_name, job.city, job.state) == (name, city, state)
    if (city, state) != (ROWS[rid]["city"], ROWS[rid]["state"]):
        assert job.location == f"{city}, {state}"


# ── The tables ──────────────────────────────────────────────────────────────

STATES = set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
             "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())


def test_tables_are_clean():
    for k, b in U.BRANDS.items():
        assert k == U.fold(k) and k
        assert b.kind in (U.SITE, U.CORP, U.LABEL)
        assert (b.name == U.SYSTEM) == (b.kind != U.SITE), b
        assert not b.state or b.state in STATES, b
    for name, st in U.FOOTPRINT.items():
        assert st in STATES and name != U.SYSTEM
    for k, name in U.AGENCIES.items():
        assert name == name.strip() and "  " not in name and name != U.SYSTEM, name
        assert k not in U.BRANDS, name
        assert U._agency_name(name) == name, name          # the table's own spellings stay as written
        assert U.valid_name(name), name


def test_only_this_system_is_touched():
    job = _job(title="RN", city="Katy", state="TX", system="Memorial Hermann", name="Memorial Hermann",
               description="Explore opportunities with Elite Home Health, a part of LHC Group, a leading post-acute")
    assert U.apply_facility(job) == ""
    assert job.hospital_name == "Memorial Hermann"
    assert scraper.HOSPITAL_SYSTEM_ALIASES.get(U.SYSTEM, U.SYSTEM) == U.SYSTEM
    assert scraper.FIXED_FEEDS["run_uhg"] == (U.SYSTEM,)          # the label scrape_uhg_talentbrew stamps


def test_a_name_the_adapter_already_set_is_left_alone():
    job = _job(3547290)
    job.hospital_name = "Elite Home Health"
    assert U.apply_facility(job) == "other"
    assert job.hospital_name == "Elite Home Health"


def test_unknown_agency_in_the_lhc_template_is_named_as_printed():
    body = "Explore opportunities with Prairie Rose Home Health , a part of LHC Group, a leading post-acute care"
    assert U.uhg_site("RN", body) == ("Prairie Rose Home Health", "body")
    # an internal code, a placeholder or a name with no service word is not
    for raw in ("Hammond LA PCS", "[agency name]", "Northshore Group", "Acme LHC MVI"):
        body = f"Explore opportunities with {raw}, a part of LHC Group, a leading post-acute care"
        assert U.uhg_site("RN", body) == (U.SYSTEM, "label"), raw


def test_unknown_name_outside_the_lhc_template_names_nothing_and_is_logged():
    body = "Opportunities with Riverbend Wellness Partners. A career with us means"
    assert U.uhg_site("Medical Assistant", body) == (U.SYSTEM, "none")
    assert U.UNKNOWN == {"Riverbend Wellness Partners": 1}
    assert "Riverbend Wellness Partners (1)" in U.unknown_summary()


def test_the_title_group_wins_over_another_groups_opening_paragraph():
    atrius = ("Explore opportunities at Atrius Health, part of the Optum family of businesses. We're an "
              "innovative health care leader and multi-specialty group practice")
    assert U.uhg_site("Licensed Practical Nurse (LPN) - ENT- Reliant Medical Group", atrius) ==         ("Reliant Medical Group", "title")
    assert U.uhg_site("Radiology Technologist - Atrius Health", atrius) == ("Atrius Health", "body")
    hospice = "Explore opportunities with Amedisys Hospice, a leading post-acute care partner for hospitals"
    assert U.uhg_site("Amedisys Hospice RN", hospice) == ("Amedisys Hospice", "body")     # one group
    # a LABEL opening line is never overridden by the title (the MSO is an office)
    mso = "A partner of the Optum Care network, WellMed Medical Management is seeking an MD/DO physician"
    assert U.uhg_site("Chief Medical Information Officer - WellMed Medical Management - Texas", mso) ==         (U.SYSTEM, "label")
    assert U.title_brand("Chief Medical Information Officer - WellMed Medical Management - Texas") == ""


def test_a_stored_name_comes_before_the_title_on_a_night_without_a_body():
    U.set_known_facilities([_stored("4675150x", "Optum NY", "Middletown", "NY")])
    job = _job(title="MFM Ultrasound Tech - Crystal Run Healthcare - Per Diem", city="Middletown", state="NY",
               job_id="4675150x", description="")
    assert U.apply_facility(job) == "kept"
    assert job.hospital_name == "Optum NY"


def test_a_title_naming_two_groups_names_none():
    assert U.title_brand("Physician - Atrius Health or Reliant Medical Group") == ""
    assert U.title_brand("Physician - Atrius Health") == "Atrius Health"


def test_a_town_is_never_guessed():
    # a state-wide row whose title names no known town stays where it is
    assert U.place(U.SYSTEM, "Hospice Aide - Hidalgo County, TX", "", "Texas", "TX") == ("Texas", "TX")
    # a known town of another state is not taken
    assert U.place(U.SYSTEM, "RN - Ormond Beach FL", "", "Texas", "TX") == ("Texas", "TX")
    # two towns: none
    assert U.place(U.SYSTEM, "RN - Katy, TX or Pearland, TX", "", "Texas", "TX") == ("Texas", "TX")
    # a blank state is filled only from a one-state business
    assert U.place("Amedisys", "RN", "", "Richmond", "") == ("Richmond", "")
    assert U.place("Kelsey-Seybold Clinic", "RN", "", "Richmond", "") == ("Richmond", "TX")
    assert U.is_town("New York", "NY") and U.is_town("Washington", "PA") and U.is_town("Indiana", "PA")
    assert not U.is_town("Texas", "TX") and not U.is_town("United States", "") and not U.is_town("Ohio", "")


# ── Nights without a body (the stored name is kept) ─────────────────────────

def _stored(job_id, name, city="Katy", state="TX", system=U.SYSTEM):
    return {"id": 1, "hospital_system": system, "job_id": job_id, "hospital_name": name, "city": city, "state": state}


def test_stored_name_is_kept_on_a_night_without_a_body():
    r = ROWS[3547290]
    assert U.set_known_facilities([_stored(r["job_id"], "Elite Home Health"),
                                   _stored("999", "Not A Real Unit Inc"),             # not a name this module writes
                                   _stored("998", "Elite Home Health", system="Memorial Hermann")]) == 1
    job = _job(title=r["title"], city="Katy", state="TX", job_id=r["job_id"], description="")
    assert U.apply_facility(job) == "kept"
    assert (job.hospital_name, job.city, job.state) == ("Elite Home Health", "Katy", "TX")


def test_stored_town_is_kept_over_a_state_wide_town_on_a_night_without_a_body():
    r = ROWS[1825894]
    U.set_known_facilities([_stored(r["job_id"], "WellMed", "Harlingen", "TX")])
    job = _job(title="Primary Care Physician", city="Texas", state="TX", job_id=r["job_id"], description="")
    assert U.apply_facility(job) == "kept"
    assert (job.hospital_name, job.city, job.state, job.location) == ("WellMed", "Harlingen", "TX", "Harlingen, TX")


def test_tonights_body_wins_over_the_stored_name():
    r = ROWS[3547290]
    U.set_known_facilities([_stored(r["job_id"], "Caretenders")])
    job = _job(3547290)
    assert U.apply_facility(job) == "body"
    assert job.hospital_name == "Elite Home Health"


def test_a_remote_title_is_never_given_the_stored_name():
    U.set_known_facilities([_stored("77", "Kelsey-Seybold Clinic", "Houston", "TX")])
    job = _job(title="Coding Analyst - Remote", city="Houston", state="TX", job_id="77", description="")
    assert U.apply_facility(job) == "remote"
    assert job.hospital_name == U.SYSTEM


def test_load_without_credentials_keeps_nothing(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    U.set_known_facilities([_stored("1", "Elite Home Health")])
    assert U.load_known_facilities() == 0
    assert U.known_facility("1") is None


# ── finalize_jobs ───────────────────────────────────────────────────────────

def test_finalize_names_uhg_rows_and_leaves_the_rest():
    other = _job(title="Registered Nurse - Medical Surgical", city="Gresham", state="OR", job_id="9201003",
                 system="Example Regional Health", name="Example Regional Health",
                 description="Explore opportunities with Elite Home Health, a part of LHC Group, a leading")
    jobs = [_job(3547290), _job(1825894), _job(37898799), _job(12978899), other]
    rows = {r["job_id"]: r for r in scraper.finalize_jobs(jobs)}
    j = {rid: str(ROWS[rid]["job_id"]) for rid in (3547290, 1825894, 37898799, 12978899)}
    assert (rows[j[3547290]]["hospital_name"], rows[j[3547290]]["city"]) == ("Elite Home Health", "Katy")
    assert (rows[j[1825894]]["hospital_name"], rows[j[1825894]]["city"], rows[j[1825894]]["state"]) == \
        ("WellMed", "Harlingen", "TX")
    assert rows[j[37898799]]["hospital_name"] == U.SYSTEM
    assert rows[j[12978899]]["hospital_name"] == U.SYSTEM
    assert all(r["hospital_system"] == U.SYSTEM for k, r in rows.items() if k != "9201003")
    assert rows["9201003"]["hospital_name"] == "Example Regional Health"


def test_a_uhg_pass_that_raises_costs_only_that_pass(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen row")
    monkeypatch.setattr(U, "apply_facilities", boom)
    rows = scraper.finalize_jobs([_job(3547290), _job(37898799)])
    assert sorted(r["job_id"] for r in rows) == sorted(str(ROWS[i]["job_id"]) for i in (3547290, 37898799))
    assert all(r["hospital_name"] == U.SYSTEM for r in rows)
