"""2026-10-29 (facility scale, release 4, adversarial review A: names and
towns). An 80-row sample of the phase 4 change lists against their stored
postings found rows named for a site the posting does not work at: remote
and hybrid roles, regional leaders, pools for several sites, clinics and
business offices placed at the town's hospital, an off-site practice placed
at the Manhattan hospital by its cost-centre code, a named centre whose town
was the centre's own name. Each row here is a real posting as the database
held it, with what the scraper writes now (null: the system name).

Fixtures, no network, no database:
  tests/fixtures/facility_release4_reviewA_1029/stored_rows.json  35 stored
      postings (body cut at 6,000 characters, Mount Sinai's at 12,000 so the
      closing cost-centre line stays; emails and phone numbers replaced) plus
      8 RadNet, US Physical Therapy and GoHealth feed items.
"""
import json
import os

import pytest

import facilities_chains as C
import facilities_systems_a as A
import facilities_systems_b as B
import facilities_town as T
import facilities_trinity_stays as F
import facilities_workday as W
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_release4_reviewA_1029", "stored_rows.json")
DATA = json.load(open(FIX, encoding="utf-8"))
ROWS = DATA["rows"]
TOWN_SYSTEMS = {"Cooper University Health Care", "Emory Healthcare", "Duke Health", "Cedars-Sinai", "SolutionHealth"}


@pytest.fixture(autouse=True)
def _no_stored_names():
    T.set_stored([])
    T._STORED_LOADED[0] = True       # no PostgREST read in tests
    A.set_known_facilities([])
    B.set_known_facilities([])
    yield
    T.set_stored([])
    T._STORED_LOADED[0] = False


def _job(r, name=None, description=None):
    return Job(title=r["title"], hospital_system=r["hospital_system"], hospital_name=name or r["hospital_system"],
               city=r["city"], state=r["state"], location=f"{r['city']}, {r['state']}" if r["city"] else r["state"],
               specialty="", job_type="", url=r["url"], job_id=r["job_id"], posted_date="",
               description=r["description"] if description is None else description, ats_platform="Workday")


def _written(r):
    """The hospital_name the scraper writes for one stored label row."""
    s, title, desc, url = r["hospital_system"], r["title"], r["description"], r["url"]
    if s == F.TRINITY:
        site = W.facility(s, "", url)
        if site and site[0] != s:
            return None if F.site_table_keep(title, desc) else site[0]
        city, st = (site[1], site[2]) if site else (r["city"], r["state"])
        n, _c, _s, how = F.decide_trinity(s, city, st, title, desc, url)
        if how == "town-rule":
            n, _c, _s, how = T.decide(s, s, city, st, title, desc, url)
        return n if n and n != s else None
    if s in TOWN_SYSTEMS:
        return T.decide(s, s, r["city"], r["state"], title, desc, url)[0]
    if s == B.MOUNT_SINAI:
        n = B.decide(s, s, r["city"], r["state"], title, desc)[0]
        return n if n and n != s else None
    if s == A.KU:
        job = _job(r)
        A.apply_facility(job)
        return job.hospital_name if job.hospital_name != s else None
    raise AssertionError(f"no route for {s}")


@pytest.mark.parametrize("r", ROWS, ids=[f"{r['id']}" for r in ROWS])
def test_stored_row(r):
    assert _written(r) == r["want"], r["why"]


def _row(rid):
    return next(r for r in ROWS if r["id"] == rid)


def test_valley_view_is_the_centre_in_elwyn():
    url = _row(18668071)["url"]
    assert W.facility(F.TRINITY, "", url) == ("Mercy LIFE Valley View", "Elwyn", "PA")


def test_riverside_site_keeps_the_system_name_in_green_island():
    url = _row(1309595)["url"]
    assert W.facility(F.TRINITY, "", url) == (F.TRINITY, "Green Island", "NY")


def test_site_guard_reverts_a_hybrid_row_named_by_the_site_table():
    r = _row(38585691)
    job = _job(r, name="St. Francis Home Care")
    job.city, job.state = "Newark", "DE"
    assert F.apply_jobs([job])[F.TRINITY] == {"site-remote": 1}
    assert (job.hospital_name, job.city, job.state) == (F.TRINITY, "", "DE")
    assert getattr(job, F.SKIP_ATTR, False)


def test_site_guard_leaves_a_site_posting_that_is_not_remote():
    r = _row(38585691)
    body = ("Employment Type:\nFull time\n\nDescription:\nHome Care Registered Nurse at St. Francis Home Care in "
            "Newark, Delaware. Provides primary nursing care to home based care patients as ordered by the physician. "
            "Visits patients in their homes across New Castle County.") * 3
    job = _job(r, name="St. Francis Home Care", description=body)
    job.title = "Homecare Registered Nurse"
    F.apply_jobs([job])
    assert job.hospital_name == "St. Francis Home Care"


def test_site_guard_without_a_body_keeps_the_stored_label():
    r = _row(38585691)
    T.set_stored([{"hospital_system": F.TRINITY, "job_id": r["job_id"], "hospital_name": F.TRINITY,
                   "city": "", "state": "DE"}])
    job = _job(r, name="St. Francis Home Care", description="")
    job.city, job.state = "Newark", "DE"
    F.apply_jobs([job])
    assert (job.hospital_name, job.city, job.state) == (F.TRINITY, "", "DE")
    # a row the database already holds under the site name keeps it
    T.set_stored([{"hospital_system": F.TRINITY, "job_id": r["job_id"], "hospital_name": "St. Francis Home Care",
                   "city": "Newark", "state": "DE"}])
    job = _job(r, name="St. Francis Home Care", description="")
    F.apply_jobs([job])
    assert job.hospital_name == "St. Francis Home Care"


@pytest.mark.parametrize("text,hybrid", [
    ("Note: This is a hybrid position with opportunity to work most of the schedule from home", True),
    ("This position is a hybrid position. Spending 2 days a week in one of the offices", True),
    ("Neuro lab and hybrid Surgical suite", False),
    ("Physician - Hospitalist (Hybrid Shift)", False),
])
def test_trinity_hybrid_words(text, hybrid):
    assert F.is_hybrid("", text) is hybrid


@pytest.mark.parametrize("title,remote", [
    ("Director of Claims & Senior Counsel - Hybrid", True),
    ("Physician - Hospitalist (Hybrid Shift)", False),
    ("MLS Techincal Specialist - Core Lab Hybrid 8HR/10HR", False),
])
def test_ku_hybrid_titles(title, remote):
    assert A.ku_remote(title, "") is remote


@pytest.mark.parametrize("j", DATA["radnet"], ids=[j["title"] for j in DATA["radnet"]])
def test_radnet_float_postings_keep_the_system_name(j):
    assert C.jibe_site(C.RADNET, j) == j["want"]


@pytest.mark.parametrize("p", DATA["usph"], ids=[p["name"] for p in DATA["usph"]])
def test_usph_floater_postings_keep_the_system_name(p):
    assert C.smartrecruiters_site(C.USPH, p) == p["want"]


@pytest.mark.parametrize("j", DATA["gohealth"], ids=[j["title"] for j in DATA["gohealth"]])
def test_gohealth_regional_role_keeps_the_system_name(j):
    got = C.gohealth_site(j["locationsText"], j["externalPath"])
    assert (list(got) if got else None) == j["want"]
