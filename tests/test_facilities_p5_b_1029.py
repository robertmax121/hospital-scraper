"""2026-10-29 (facility scale, phase 5 group B): the hospital or named site on
the label rows of Riverside, Banner, Cleveland Clinic (Workday site tables),
United Health Services, Logan Health, Health First (town tables), Telecare,
Cincinnati Children's, Akron Children's, Mass General Brigham, Logan Health,
Cleveland Clinic Nevada and UTHealth Houston (this module's own pass), in
facilities_p5_b.

Fixture (tests/fixtures/facility_p5b_1029/stored_rows.json): real stored
label rows of hospital_jobs read 2026-10-29 (bodies cut to 2,500 characters
where the cut body gives the same answer, e-mail addresses replaced), each
with the answer of the stored-row change list
(facility_scale_2026-10-29/phase5/changes/b.tsv; null = the row stays on the
system name). No network, no database."""
import json
import os

import pytest

import facilities_p5_b as P
import facilities_text as T
import facilities_town as W
import facilities_workday as WD
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_p5b_1029", "stored_rows.json")
with open(FIX, encoding="utf-8") as _f:
    ROWS = json.load(_f)["rows"]


@pytest.fixture(autouse=True)
def _empty_stored_maps():
    T.set_known_facilities([])
    W.set_stored([])
    P.set_known_facilities([])
    yield
    T.set_known_facilities([])
    W.set_stored([])
    P.set_known_facilities([])


def _job(r, **kw):
    d = dict(title=r["title"], hospital_system=r["hospital_system"], hospital_name=r["hospital_system"],
             city=r["city"] or "", state=r["state"] or "", location=r["location"] or "", specialty="", job_type="",
             url=r["url"], job_id=str(r["job_id"]), posted_date="", description=r["description"],
             ats_platform=r["ats_platform"] or "X")
    d.update(kw)
    return Job(**d)


def _adapter(j):
    """The adapter's list-time Workday site table (the stored url's site segment)."""
    if j.hospital_system in scraper.WD_SITE_FACILITY and "myworkdayjobs" in j.url:
        hit = scraper.WD_SITE_FACILITY[j.hospital_system]("", j.url)
        if hit:
            j.hospital_name, j.city, j.state = hit
    return j


def _finalize(jobs):
    os.environ["FINALIZE_WORKERS"] = "1"
    return scraper.finalize_jobs([_adapter(j) for j in jobs])


def _rows(system, name=None):
    out = [r for r in ROWS if r["hospital_system"] == system]
    if name is not None:
        out = [r for r in out if (r["expect"] or [None])[0] == name]
    return out


# ── The stored rows, end to end ─────────────────────────────────────────────
def test_every_fixture_row_gets_the_change_list_answer():
    out = _finalize([_job(r) for r in ROWS])
    got = {(o["hospital_system"], str(o["job_id"])): (o["hospital_name"], o.get("city") or "", o.get("state") or "")
           for o in out}
    bad = []
    for r in ROWS:
        g = got.get((r["hospital_system"], str(r["job_id"])))
        if r["expect"]:
            if g != tuple(r["expect"]):
                bad.append((r["id"], r["title"], r["expect"], g))
        elif g is None or g[0] != r["hospital_system"]:
            bad.append((r["id"], r["title"], "label", g))
    assert not bad, bad[:10]
    assert sum(1 for r in ROWS if r["expect"]) >= 100


def test_fixture_covers_every_system_this_module_names():
    named = {r["hospital_system"] for r in ROWS if r["expect"]}
    for s in (P.RIVERSIDE, P.BANNER, P.CLEVELAND, P.UHS, P.LOGAN, P.HEALTH_FIRST, P.TELECARE, P.CINCY, P.AKRON,
              P.MGB, P.UTH):
        assert s in named, s


# ── Telecare: the title's program number ────────────────────────────────────
def test_telecare_program_number_names_the_program_in_its_town():
    r = _rows(P.TELECARE, "Villa Fairmont Mental Health Rehabilitation Center")[0]
    assert r["title"].rstrip().endswith("103")
    n, c, s, how = P.decide(P.TELECARE, P.TELECARE, "San Leandro", "CA", r["title"], "", r["url"])
    assert (n, c, s, how) == ("Villa Fairmont Mental Health Rehabilitation Center", "San Leandro", "CA", "named")
    # the same number in another town names nothing (never guess a town)
    assert P.decide(P.TELECARE, P.TELECARE, "Oakland", "CA", r["title"])[3] == "elsewhere"


def test_telecare_field_teams_and_virtual_roles_keep_the_label():
    # 396 is the San Diego Mobile Crisis Response Team (a field team)
    assert P.decide(P.TELECARE, P.TELECARE, "San Diego", "CA", "Clinician II - Mental Health 396")[3] == "field"
    assert P.decide(P.TELECARE, P.TELECARE, "Fontana", "CA",
                    "On-Call LCSW - Virtual - BBS-registered Supervisor - Mental Health 346")[3] == "remote"
    assert P.decide(P.TELECARE, P.TELECARE, "Fontana", "CA", "Regional Director of Nursing")[3] == "none"
    # the adapter's own numbers (155 / 169 / 194) are not this table's
    assert not {"155", "169", "194"} & set(P.TELECARE_PROGRAMS)


# ── Cincinnati Children's: the Workday campus ───────────────────────────────
def test_cincinnati_campus_from_the_workday_site():
    r = _rows(P.CINCY, "Cincinnati Children's Liberty Campus")[0]
    assert "/job/Liberty-Campus/" in r["url"]
    assert P.decide(P.CINCY, P.CINCY, r["city"], r["state"], r["title"], "", r["url"])[:3] == (
        "Cincinnati Children's Liberty Campus", "Liberty Township", "OH")
    burnet = r["url"].replace("/job/Liberty-Campus/", "/job/Burnet-Campus/")
    assert P.decide(P.CINCY, P.CINCY, "Cincinnati", "OH", r["title"], "", burnet)[3] == "none"


# ── Akron Children's: the Mahoning Valley campus ────────────────────────────
def test_akron_mahoning_valley_campus_only_in_boardman():
    r = _rows(P.AKRON, "Akron Children's Hospital Mahoning Valley")[0]
    assert P.decide(P.AKRON, P.AKRON, "Boardman", "OH", r["title"], r["description"])[:3] == (
        "Akron Children's Hospital Mahoning Valley", "Boardman", "OH")
    # an Akron posting that mentions the Beeghly campus is not at it
    assert P.decide(P.AKRON, P.AKRON, "Akron", "OH", r["title"], r["description"])[3] == "elsewhere"
    two = "Our pediatric hospitalists cover the Beeghly and Golisano campuses."
    assert P.decide(P.AKRON, P.AKRON, "Boardman", "OH", "Hospitalist", two)[3] == "two"


# ── Mass General Brigham: the Site: entity ──────────────────────────────────
def test_mgb_site_line_names_the_center():
    r = _rows(P.MGB, "Spaulding Nursing and Therapy Center Brighton")[0]
    assert r["description"].lstrip().startswith("Site: Spaulding Nursing and Therapy Center Brighton")
    assert P.decide(P.MGB, P.MGB, "Brighton", "MA", r["title"], r["description"])[3] == "named"
    office = "Site: Mass General Brigham Incorporated\n\nMass General Brigham relies on ..."
    assert P.decide(P.MGB, P.MGB, "Somerville", "MA", "Analyst", office)[3] == "none"


# ── Logan Health: body sites, the town table, the head-office override ──────
def test_logan_opening_names_the_critical_access_hospital():
    body = ("Join Our Logan Health Chester Facilities Team! " + "x " * 600)
    assert P.decide(P.LOGAN, P.LOGAN, "Chester", "MT", "Facilities Technician | Chester | Full Time", body)[:3] == (
        "Logan Health Chester", "Chester", "MT")
    # an EMS station and a two-town posting are no one hospital's
    assert P.decide(P.LOGAN, P.LOGAN, "Cut Bank", "MT", "Emergency Medical Technician | Cut Bank Station EMS East",
                    "Logan Health Cut Bank " + "x " * 600)[3] == "care-site"


def test_logan_shelby_row_keeps_its_town_through_normalize():
    r = next(x for x in _rows(P.LOGAN) if x["expect"] and x["expect"][1] == "Shelby")
    out = _finalize([_job(r)])
    assert (out[0]["hospital_name"], out[0]["city"], out[0]["state"]) == tuple(r["expect"])
    assert WD.placed(P.LOGAN, "Logan Health Shelby", "Shelby", "MT")
    assert not WD.placed(P.LOGAN, "Logan Health Shelby", "Kalispell", "MT")


def test_logan_town_table_keeps_skilled_nursing_and_regional_roles_on_the_label():
    url = "https://loganhealth.wd1.myworkdayjobs.com/Logan_Careers/job/Kalispell-Montana/RN_Req1"
    body = "Logan Health is a regional health system " * 40
    assert W.decide(P.LOGAN, P.LOGAN, "Kalispell", "MT", "RN | Intensive Care Unit (ICU)", body, url)[0] == \
        "Logan Health Medical Center"
    assert W.decide(P.LOGAN, P.LOGAN, "Kalispell", "MT", "RN | Skilled Nursing | PRN status", body, url)[3] == \
        "care-site"
    assert W.decide(P.LOGAN, P.LOGAN, "Conrad", "MT", "LPN | Outpatient Employee Health Services | Hi-Line Region",
                    body, url.replace("Kalispell", "Conrad"))[3] == "care-site"


# ── Town tables: UHS and Health First ───────────────────────────────────────
def test_uhs_town_table():
    url = "https://nyuhs.wd12.myworkdayjobs.com/nyuhscareers1/job/Johnson-City-NY/RN_R1"
    body = "UHS is a not-for-profit health care system " * 40
    assert W.decide(P.UHS, P.UHS, "Johnson City", "NY", "Registered Nurse - PACU", body, url)[:3] == (
        "UHS Wilson Medical Center", "Johnson City", "NY")
    assert W.decide(P.UHS, P.UHS, "Johnson City", "NY", "LPN - UHS Primary Care Vestal", body, url)[3] in (
        "care-site", "offsite")
    # Binghamton is the stamp: the row is placed only when its own site names Binghamton
    assert W.decide(P.UHS, P.UHS, "Binghamton", "NY", "Staff Nurse", body, url.replace("Johnson-City", "Binghamton"))[0] \
        == "UHS Binghamton General Hospital"
    assert W.decide(P.UHS, P.UHS, "Binghamton", "NY", "Staff Nurse", body, "")[3] == "default"


def test_health_first_town_table():
    body = "Job Requirements POSITION SUMMARY " * 40
    assert W.decide(P.HEALTH_FIRST, P.HEALTH_FIRST, "Palm Bay", "FL", "Registered Nurse (RN) - PCU", body)[0] == \
        "Palm Bay Hospital"
    assert W.decide(P.HEALTH_FIRST, P.HEALTH_FIRST, "Cocoa Beach", "FL", "Monitor Tech - Emergency Department",
                    body)[0] == "Cape Canaveral Hospital"
    # Melbourne holds two hospitals (and is the stamp): only a title naming one
    assert W.decide(P.HEALTH_FIRST, P.HEALTH_FIRST, "Melbourne", "FL", "Registered Nurse - Respiratory PCU",
                    body)[0] is None
    assert W.decide(P.HEALTH_FIRST, P.HEALTH_FIRST, "Palm Bay", "FL", "HFMG Medical Assistant - Family Medicine",
                    body)[0] is None


# ── Workday site tables ─────────────────────────────────────────────────────
def test_workday_sites():
    f = scraper.WD_SITE_FACILITY
    base = "https://{t}.myworkdayjobs.com/{b}/job/{s}/X_R1"
    assert f[P.CLEVELAND]("", base.format(t="ccf.wd1", b="ClevelandClinicCareers",
                                          s="Justin-Rogers-Hosp-Care-Center")) == (
        "Justin Rogers Hospice Care Center", "Akron", "OH")
    assert f[P.BANNER]("", base.format(t="bannerhealth.wd108", b="Careers",
                                       s="Banner-North-Colorado-Medical-Center---Loveland-Campus-2000-N-Boise-Ave"))[0] \
        == "Banner North Colorado Medical Center - Loveland Campus"
    assert f[P.RIVERSIDE]("", base.format(t="rivhs.wd1", b="Non-ProviderRHS",
                                          s="Patriots-Colony---Williamsburg-Virginia")) == (
        "Patriots Colony", "Williamsburg", "VA")
    # office buildings and the multi-specialty centers stay on the label
    for seg in ("Newport-Square-Professional-Center---Newport-News-Virginia",
                "Williamsburg-Healthcare-Center---Williamsburg-Virginia"):
        hit = f[P.RIVERSIDE]("", base.format(t="rivhs.wd1", b="Non-ProviderRHS", s=seg))
        assert hit is None or hit[0] in (None, P.RIVERSIDE), (seg, hit)


# ── UTHealth and Cleveland Clinic Nevada ────────────────────────────────────
def test_uthealth_hcpc_and_lou_ruvo():
    assert P.decide(P.UTH, P.UTH, "Houston", "TX", "Staff Nurse II (HCPC)", "")[:3] == (
        "UTHealth Houston Harris County Psychiatric Center", "Houston", "TX")
    assert P.decide(P.UTH, P.UTH, "Houston", "TX", "Research Coordinator - Neurology", "x" * 1200)[3] == "none"
    url = "https://ccf.wd1.myworkdayjobs.com/ClevelandClinicCareers/job/Cleveland-Clinic-Nevada/X_R1"
    body = "Job Title Clinical Speech Pathologist - Lou Ruvo Center for Brain Health Location Las Vegas"
    assert P.decide(P.CLEVELAND, P.CLEVELAND, "Las Vegas", "NV", "Clinical Speech Pathologist", body, url)[0] == \
        "Cleveland Clinic Lou Ruvo Center for Brain Health"
    assert P.decide(P.CLEVELAND, P.CLEVELAND, "Las Vegas", "NV", "CT Technologist, PRN", "", url)[3] == "none"


# ── Nights without a body, guards ───────────────────────────────────────────
def test_no_body_night_keeps_the_stored_site():
    r = _rows(P.MGB, "Spaulding Nursing and Therapy Center Brighton")[0]
    P.set_known_facilities([{"hospital_system": P.MGB, "job_id": r["job_id"],
                             "hospital_name": "Spaulding Nursing and Therapy Center Brighton",
                             "city": "Brighton", "state": "MA"}])
    j = _job(r, description="")
    assert P.apply_facility(j) == "kept"
    assert (j.hospital_name, j.city, j.state) == ("Spaulding Nursing and Therapy Center Brighton", "Brighton", "MA")
    P.set_known_facilities([])
    j = _job(r, description="")
    assert P.apply_facility(j) == "none" and j.hospital_name == P.MGB


def test_a_row_that_raises_keeps_its_label(monkeypatch):
    r = _rows(P.TELECARE, "Villa Fairmont Mental Health Rehabilitation Center")[0]
    ok, bad = _job(r), _job(r, job_id="x2")
    calls = {"n": 0}
    real = P.decide

    def boom(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("unforeseen")
        return real(*a, **kw)

    monkeypatch.setattr(P, "decide", boom)
    stats = P.apply_facilities([bad, ok])
    assert bad.hospital_name == P.TELECARE and stats[P.TELECARE]["error"] == 1
    assert ok.hospital_name == "Villa Fairmont Mental Health Rehabilitation Center"


def test_registration_is_once_and_hospital_system_is_never_touched():
    for s in (P.UHS, P.LOGAN, P.HEALTH_FIRST):
        assert s in W.SPECS
        with pytest.raises(ValueError):
            P._town_spec(s, [])
    r = _rows(P.TELECARE, "Villa Fairmont Mental Health Rehabilitation Center")[0]
    out = _finalize([_job(r)])
    assert out[0]["hospital_system"] == P.TELECARE


def test_load_without_credentials_is_empty(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert P.load_known_facilities() == 0
