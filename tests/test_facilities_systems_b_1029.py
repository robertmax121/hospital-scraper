"""2026-10-29 (facility scale, phase 4 group B): the hospital or named site on
the rows Vandalia, Innovative Renal Care, UK HealthCare, Mount Sinai, OHSU,
SolutionHealth, Dartmouth Health, Duke Health, Rochester Regional Health and
Genesis HealthCare System still filed under the system name
(facilities_systems_b).

Fixture (tests/fixtures/facility_sysb_1029/stored_rows.json): real stored
label rows of hospital_jobs read 2026-10-29 (some bodies cut around the line
that names the site), each with the answer of the stored-row change list
(facility_scale_2026-10-29/phase4/changes/systems-b.tsv; null = the row stays
on the system name). No network, no database."""
import json
import os
import re

import pytest

import facilities_systems_b as B
import facilities_text as T
import facilities_town as W
import facilities_workday as WD
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_sysb_1029", "stored_rows.json")
with open(FIX, encoding="utf-8") as _f:
    _DATA = json.load(_f)
ROWS = _DATA["rows"]
STATES = set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
             "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())


@pytest.fixture(autouse=True)
def _empty_stored_maps():
    T.set_known_facilities([])
    W.set_stored([])
    B.set_known_facilities([])
    yield
    T.set_known_facilities([])
    W.set_stored([])
    B.set_known_facilities([])


def _job(r, **kw):
    d = dict(title=r["title"], hospital_system=r["hospital_system"], hospital_name=r["hospital_system"],
             city=r["city"], state=r["state"], location=f"{r['city']}, {r['state']}", specialty="", job_type="",
             url=r["url"], job_id=str(r["job_id"]), posted_date="", description=r["description"], ats_platform="X")
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
    return scraper.finalize_jobs(jobs)


# ── the stored rows come out as the change list writes them ──────────────────
def test_stored_rows_match_the_change_list():
    jobs = [_adapter(_job(r)) for r in ROWS]
    out = {(o["hospital_system"], str(o["job_id"])): o for o in _finalize(jobs)}
    bad = []
    for r in ROWS:
        o = out[(r["hospital_system"], str(r["job_id"]))]
        got = [o["hospital_name"], o.get("city") or "", o.get("state") or ""]
        if r["want"] is None:
            if got[0] != r["hospital_system"]:
                bad.append((r["id"], "label", got))
        elif got != r["want"]:
            bad.append((r["id"], r["want"], got))
    assert not bad, bad
    assert sum(1 for r in ROWS if r["want"]) >= 40


def test_every_system_has_named_and_kept_rows_in_the_fixture():
    named = {r["hospital_system"] for r in ROWS if r["want"] and r["want"][0] != r["hospital_system"]}
    kept = {r["hospital_system"] for r in ROWS if r["want"] is None}
    for s in (B.VANDALIA, B.IRC, B.UK, B.MOUNT_SINAI, B.OHSU, B.SOLUTION, B.DARTMOUTH, B.DUKE, B.ROCHESTER, B.GENESIS,
              B.CEDARS, B.FMOL):
        assert s in named, s
        assert s in kept, s


def test_a_night_without_a_body_keeps_the_stored_site_and_town():
    """After the SQL, a night that does not read the body (the scraper sends a
    known body blank) keeps every name and town the change list wrote."""
    stored, jobs = [], []
    for r in ROWS:
        want = r["want"] or [r["hospital_system"], r["city"], r["state"]]
        stored.append({"hospital_system": r["hospital_system"], "job_id": r["job_id"], "hospital_name": want[0],
                       "city": want[1], "state": want[2]})
        jobs.append(_adapter(_job(r, description="")))
    W.set_stored(stored)
    B.set_known_facilities(stored)
    out = {(o["hospital_system"], str(o["job_id"])): o for o in _finalize(jobs)}
    for r in ROWS:
        o = out[(r["hospital_system"], str(r["job_id"]))]
        got = [o["hospital_name"], o.get("city") or "", o.get("state") or ""]
        if r["want"]:
            assert got == r["want"], (r["id"], r["want"], got)
        else:
            assert got[0] == r["hospital_system"], (r["id"], got)


# ── per system ───────────────────────────────────────────────────────────────
def test_vandalia_title_segment_decides_over_the_body_line():
    body = "Job Summary\n" + "x " * 400 + "\nLocation: Memorial Hospital\nLocation of Job: US:WV:Charleston\n"
    n, c, s, how = B.decide(B.VANDALIA, B.VANDALIA, "Charleston", "WV",
                            "Registered Nurse 2 -- 5 Front -- Memorial Hospital", body)
    assert (n, c, s, how) == ("CAMC Memorial Hospital", "Charleston", "WV", "text")
    # an office building in the title wins over a hospital in the body
    n, _c, _s, how = B.decide(B.VANDALIA, B.VANDALIA, "Charleston", "WV",
                              "Coder -- HIM -- Memorial-MSOB", body)
    assert n is None and how == "office"
    # "General-MSOB" is the General campus office building, not CAMC General Hospital
    n, _c, _s, how = B.decide(B.VANDALIA, B.VANDALIA, "Charleston", "WV", "Clerk -- Admitting -- General-MSOB", "")
    assert n is None and how == "office"
    n, _c, _s, _h = B.decide(B.VANDALIA, B.VANDALIA, "Charleston", "WV",
                             "RN -- 7 South -- General Hospital (No Weekends/No Holidays)", "")
    assert n == "CAMC General Hospital"
    # no title segment: the body's Location line
    n, _c, _s, _h = B.decide(B.VANDALIA, B.VANDALIA, "Charleston", "WV", "Registered Nurse", body)
    assert n == "CAMC Memorial Hospital"
    # a hospital line on a posting in another town keeps the label
    n, _c, _s, how = B.decide(B.VANDALIA, B.VANDALIA, "Beckley", "WV", "RN -- ICU -- Memorial Hospital", "")
    assert n is None and how == "elsewhere"


def test_irc_location_line_moves_the_town_the_line_writes():
    body = "Our Values\n" + "y " * 400 + "\nLocation: Keowee Dialysis Center LLC - 537 Highway 123 Seneca SC 29678\n"
    assert B.decide(B.IRC, B.IRC, "Keowee", "SC", "RN", body)[:3] == ("Keowee Dialysis Center", "Seneca", "SC")
    office = "Our Values\n" + "y " * 400 + "\nLocation: Franklin, TN (100% onsite)\n"
    assert B.decide(B.IRC, B.IRC, "Franklin", "TN", "Analyst", office)[0] is None


def test_uk_working_title_and_work_location():
    body = ("Posting Details\nJob Title\nLPN/Hospital/UKHC\nWorking Title\nLPN/Good Sam 4m/7a\nDepartment Name\n"
            "H6120:Critical Care\nWork Location\nLexington, KY\n" + "z " * 400)
    assert B.decide(B.UK, B.UK, "Lexington", "KY", "LPN/Hospital/UKHC", body)[:3] == (
        "UK Good Samaritan Hospital", "Lexington", "KY")
    # Work Location in another town: the label, at that town (the adapter stamps Lexington)
    body2 = body.replace("LPN/Good Sam 4m/7a", "Clinic Nurse").replace("Lexington, KY\n", "Bowling Green, KY\n")
    assert B.decide(B.UK, B.UK, "Lexington", "KY", "Nurse Clinical/UKHC", body2)[:3] == (
        "UK HealthCare", "Bowling Green", "KY")
    # two hospitals named: none
    body3 = body.replace("LPN/Good Sam 4m/7a", "RN/Chandler and Good Sam float")
    assert B.decide(B.UK, B.UK, "Lexington", "KY", "Nurse/UKHC", body3)[0] is None


def test_uk_stored_town_is_kept_on_a_night_without_a_body():
    B.set_known_facilities([{"hospital_system": B.UK, "job_id": "1", "hospital_name": B.UK, "city": "Morehead",
                             "state": "KY"}])
    j = Job(title="Nurse/UKHC", hospital_system=B.UK, hospital_name=B.UK, city="Lexington", state="KY",
            location="Lexington, KY", specialty="", job_type="", url="", job_id="1", posted_date="", description="",
            ats_platform="PeopleAdmin")
    assert B.apply_facility(j) == "kept" and (j.hospital_name, j.city, j.state) == (B.UK, "Morehead", "KY")


def test_mount_sinai_cost_centre_codes():
    tail = "\n" + "w " * 400 + "\n{}\n\nSchedule: Full time"
    msh = tail.format("Non-Bargaining Unit, 813 - Radiology - MSH, Mount Sinai Hospital")
    assert B.decide(B.MOUNT_SINAI, B.MOUNT_SINAI, "New York", "NY", "Technologist", msh)[0] == "The Mount Sinai Hospital"
    for code, ent in (("OFFS", "Mount Sinai Hospital"), ("ISM", "Icahn School of Medicine"),
                      ("QHC", "Queens Hospital Center")):
        b = tail.format(f"1199 Bargaining Unit, 008 - FPA Access Center - {code}, {ent}")
        n, _c, _s, how = B.decide(B.MOUNT_SINAI, B.MOUNT_SINAI, "New York", "NY", "Coordinator", b)
        assert n is None and how == "office", code
    # an entity on a remote posting keeps the label
    n, _c, _s, how = B.decide(B.MOUNT_SINAI, B.MOUNT_SINAI, "New York", "NY", "Coder - Remote", msh)
    assert n is None and how == "remote"
    # the title's "Mt Sinai" spelling
    assert B.decide(B.MOUNT_SINAI, B.MOUNT_SINAI, "New York", "NY",
                    "Per Diem Speech Language Pathologist-Mt Sinai Hospital- Per Diem", "")[0] == "The Mount Sinai Hospital"


def test_ohsu_titles_and_the_doernbecher_section():
    assert B.decide(B.OHSU, B.OHSU, "Portland", "OR", "RN - Doernbecher PICU", "")[0] == \
        "OHSU Doernbecher Children's Hospital"
    body = "Department Overview\nAbout OHSU Doernbecher Children’s Hospital Link https://www.ohsu.edu/doernbecher\n"
    assert B.decide(B.OHSU, B.OHSU, "Portland", "OR", "Operating Room Nurse", body)[0] == \
        "OHSU Doernbecher Children's Hospital"
    # Hillsboro Medical Center on a Portland posting: another town, the label
    assert B.decide(B.OHSU, B.OHSU, "Portland", "OR", "Hospitalist - Hillsboro Medical Center", "")[0] is None


def test_town_tables_registered_and_shared_towns():
    assert W.SPECS[B.SOLUTION].towns[(W.town_key("Manchester"), "NH")][0] == "Elliot Hospital"
    assert W.SPECS[B.SOLUTION].towns[(W.town_key("Nashua"), "NH")][0] == "Southern New Hampshire Medical Center"
    assert set(W.SPECS[B.DARTMOUTH].shared[(W.town_key("Lebanon"), "NH")]) == {
        "Dartmouth Hitchcock Medical Center", "Alice Peck Day Memorial Hospital"}
    assert set(W.SPECS[B.DUKE].shared[(W.town_key("Durham"), "NC")]) == {"Duke University Hospital",
                                                                          "Duke Regional Hospital"}
    assert (W.town_key("Cary"), "NC") not in W.SPECS[B.DUKE].towns
    # an off-site service of the town's hospital keeps the label
    assert W.is_care_site(B.SOLUTION, "Radiology Technologist - Off Site Radiology - Per diem")


def test_workday_tables():
    url = "https://rrhs.wd5.myworkdayjobs.com/en-US/Careers/job/{}/RN_R1"
    assert WD.facility(B.ROCHESTER, "", url.format("Edna-Tina-Wilson-Living-Center")) == (
        "Edna Tina Wilson Living Center", "Rochester", "NY")
    assert WD.facility(B.ROCHESTER, "", url.format("Unity-Living-Center"))[0] == "Unity Living Center"
    assert WD.facility(B.ROCHESTER, "", url.format("Sands-Constellation-Center-for-Critical-Care"))[0] == \
        "Rochester General Hospital"
    # a campus and a home care office stay on the label (phase 2's entries untouched)
    assert WD.facility(B.ROCHESTER, "", url.format("Batavia-Medical-Campus")) == (B.ROCHESTER, "Batavia", "NY")
    gurl = "https://genesishcs.wd1.myworkdayjobs.com/GenesisHCS/job/{}/X_R1"
    assert WD.facility(B.GENESIS, "", gurl.format("Genesis-Primary-Care-Dresden")) == (
        "Genesis Primary Care Dresden", "Dresden", "OH")
    assert WD.facility(B.GENESIS, "", gurl.format("Medical-Arts-Building-I")) is None
    assert WD.facility(B.GENESIS, "", gurl.format("Genesis-Hospital")) == ("Genesis Hospital", "Zanesville", "OH")


def test_tables_are_clean():
    for system, spec in B.SPECS.items():
        for site in spec.names.values():
            assert site.state in STATES and site.city and site.city == site.city.strip(), (system, site)
            assert site.name == site.name.strip() and "  " not in site.name and "," not in site.name, site.name
            assert re.match(r"^(cms \d{6}|board|address .+)", site.source), site.source
    for system in (B.SOLUTION, B.DARTMOUTH, B.DUKE, B.CEDARS, B.FMOL):
        for name, city, state, source in W.SPECS[system].hospitals:
            assert state in STATES and city and re.match(r"^(cms \d{6}|site)", source), (name, source)


def test_hospital_system_is_never_touched_and_other_systems_are_left():
    r = next(x for x in ROWS if x["want"] and x["hospital_system"] == B.VANDALIA)
    j = _job(r)
    B.apply_facility(j)
    assert j.hospital_system == B.VANDALIA
    other = _job(r, hospital_system="Some Other Health", hospital_name="Some Other Health")
    assert B.apply_facility(other) == "" and other.hospital_name == "Some Other Health"
    # a row the adapter already named is left as it is
    named = _job(r, hospital_name="CAMC Teays Valley Hospital")
    assert B.apply_facility(named) == "other" and named.hospital_name == "CAMC Teays Valley Hospital"


def test_a_row_that_raises_keeps_its_name_and_the_pass_goes_on(monkeypatch):
    rows = [x for x in ROWS if x["want"] and x["hospital_system"] == B.VANDALIA][:2]
    jobs = [_job(r) for r in rows]
    real = B.decide

    def boom(system, *a, **kw):
        if a and a[3] == jobs[0].title:
            raise RuntimeError("unforeseen row")
        return real(system, *a, **kw)
    monkeypatch.setattr(B, "decide", boom)
    B.ROW_ERRORS.clear()
    stats = B.apply_facilities(jobs)
    assert jobs[0].hospital_name == B.VANDALIA and (jobs[0].city, jobs[0].state) == (rows[0]["city"], rows[0]["state"])
    assert jobs[1].hospital_name == rows[1]["want"][0]
    assert stats[B.VANDALIA]["error"] == 1 and B.ROW_ERRORS[B.VANDALIA] == 1
    B.ROW_ERRORS.clear()


def test_finalize_runs_the_pass_before_the_town_tables():
    src = open(scraper.__file__, encoding="utf-8").read()
    i_pass = src.index('_facility_pass("Systems B", facilities_systems_b.apply_facilities, kept)')
    i_town = src.index('_facility_pass("Town tables", facilities_town.apply_jobs, kept')
    i_text = src.index('_facility_pass("Text facilities", facilities_text.apply_facilities, kept)')
    assert i_text < i_pass < i_town
    assert "facilities_systems_b.load_known_facilities" in src


def test_cedars_and_fmol_town_tables_use_the_oracle_names():
    import facilities_oracle_other as O
    for system in (B.CEDARS, B.FMOL):
        oracle = O.SYSTEMS[system]["hospitals"]
        for name, city, state, _src in W.SPECS[system].hospitals:
            assert name in oracle and (oracle[name][0], oracle[name][1]) == (city, state), (system, name)
    assert set(W.SPECS[B.FMOL].shared[(W.town_key("Baton Rouge"), "LA")]) == {
        "Our Lady of the Lake Regional Medical Center", "Our Lady of the Lake Children's Hospital"}


def test_oracle_extra_names_northwell_and_parkview_work_locations():
    for system, loc, town, st, want in (
            ("Parkview Health", "Parkview Wabash Hospital", "Wabash", "IN", "Parkview Wabash Hospital"),
            ("Parkview Health", "Parkview Noble Hospital", "Kendallville", "IN", "Parkview Noble Hospital"),
            ("Northwell Health", "Lenox Hill Hosp", "New York", "NY", "Lenox Hill Hospital"),
            ("Northwell Health", "Phelps Memorial", "Sleepy Hollow", "NY", "Phelps Hospital"),
            # the phase 2 entries stand
            ("Parkview Health", "Parkview Regional Medical Ctr", "Fort Wayne", "IN", "Parkview Regional Medical Center")):
        j = {"Title": "RN", "workLocation": {"items": [{"LocationName": loc, "TownOrCity": town, "Region2": st}]}}
        assert scraper._oracle_work_location_facility(system, j, town, st) == want, loc
    # a building of the system stays on the label
    j = {"Title": "RN", "workLocation": {"items": [{"LocationName": "15 Burke Ln", "TownOrCity": "Rye", "Region2": "NY"}]}}
    assert scraper._oracle_work_location_facility("Northwell Health", j, "Rye", "NY") == ""


def test_extend_oracle_is_all_or_nothing():
    import copy
    import facilities_oracle_other as O
    systems = copy.deepcopy({k: O.SYSTEMS[k] for k in B.ORACLE_EXTRA})
    old = copy.deepcopy(B.ORACLE_EXTRA)
    try:
        B.ORACLE_EXTRA["Parkview Health"]["hospitals"]["Parkview Wabash Hospital"] = ("Peru", "IN", "x")
        before = copy.deepcopy(systems)
        with pytest.raises(ValueError):
            B.extend_oracle(systems)
        assert systems == before
    finally:
        B.ORACLE_EXTRA.clear()
        B.ORACLE_EXTRA.update(old)
