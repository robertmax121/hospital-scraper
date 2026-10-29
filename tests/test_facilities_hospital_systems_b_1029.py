"""2026-10-29 (facility scale, phase 3 group B): the hospital on the rows
Geisinger, Sutter, Allegheny Health Network, Jefferson, Ochsner, Kaiser and
Huntsville still filed under the system name (facilities_hospital_systems_b).

Fixture (tests/fixtures/facility_hsb/stored_rows.json): real stored label rows
of hospital_jobs read 2026-10-29 (bodies cut after the line that names the
site), each with the answer of the stored-row change list
(facility_scale_2026-10-29/phase3/changes/hospital-systems-b.tsv; null = the
row stays on the system name), and the locationsText / externalPath of a live
highmarkhealth (AHN) Workday list page read the same day. No network, no
database."""
import json
import os
import re

import pytest

import facilities_hospital_systems_b as B
import facilities_icims as I
import facilities_phenom as P
import facilities_text as T
import facilities_town as W
import facilities_workday as WD
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_hsb", "stored_rows.json")
with open(FIX, encoding="utf-8") as _f:
    _DATA = json.load(_f)
ROWS = {r["id"]: r for r in _DATA["rows"]}
BY_TAG = {(r["hospital_system"], r["tag"]): r for r in _DATA["rows"]}
STATES = set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
             "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())


@pytest.fixture(autouse=True)
def _empty_stored_maps():
    T.set_known_facilities([])
    P.set_stored_names([])
    W.set_stored([])
    yield
    T.set_known_facilities([])
    P.set_stored_names([])
    W.set_stored([])


def _job(r, **kw):
    d = dict(title=r["title"], hospital_system=r["hospital_system"], hospital_name=r["hospital_name"],
             city=r["city"], state=r["state"], location=f"{r['city']}, {r['state']}", specialty="", job_type="",
             url=r["url"], job_id=str(r["job_id"]), posted_date="", description=r["description"], ats_platform="Workday")
    d.update(kw)
    return Job(**d)


def _run(r):
    """The scraper's own order on one stored row: the Workday site table (the
    stored url's site segment), the classic iCIMS title pass (Huntsville),
    then finalize_jobs' Phenom, text, group B titles, town tables and group B
    addresses passes."""
    j = _job(r)
    if r["hospital_system"] in WD.TABLES or WD.covers(r["hospital_system"]):
        site = WD.facility(r["hospital_system"], "", r["url"])
        if site:
            j.hospital_name, j.city, j.state = site
    if r["hospital_system"] == B.HUNTSVILLE:
        hit = I.classic_facility(B.HUNTSVILLE, j.title, j.description, j.city, j.state)
        if hit:
            j.hospital_name, j.city, j.state = hit[:3]
    P.apply_facility(j)
    T.apply_facility(j)
    B.apply_before_town([j])
    W.apply_job(j)
    B.apply_after_town([j])
    return j


# ── Registration ────────────────────────────────────────────────────────────

def test_the_tables_are_registered_in_the_family_modules():
    assert T.SPECS[B.GEISINGER] is B.GEISINGER_SPEC and T.SPECS[B.SUTTER] is B.SUTTER_SPEC
    assert {B.GEISINGER, B.SUTTER} <= T.SYSTEMS
    assert B.AHN in scraper.WD_SITE_FACILITY and B.GEISINGER in scraper.WD_SITE_FACILITY
    assert W.SPECS[B.HUNTSVILLE] is B.HUNTSVILLE_TOWNS
    fam = P.FAMILIES[B.KAISER]
    for spelling, site in B.KAISER_SITES.items():
        assert fam.text[1][P.fkey(spelling)] == site
        assert P.site_by_name(fam, site[0]) == site      # kept on a night without a body
    assert any(hit[0] == "Crestwood Medical Center" for _rx, hit in I.TITLE_TABLES[B.HUNTSVILLE])
    for s in B.SYSTEMS:
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(s, s) == s


def test_registration_is_idempotent_and_never_overrides_a_spec():
    B._register_huntsville()
    assert sum(hit[0] == "Crestwood Medical Center" for _rx, hit in I.TITLE_TABLES[B.HUNTSVILLE]) == 1
    B._register_kaiser()
    other = T.Spec(B.SUTTER, None, (T.Read("label", "Organization", "entity"),), ())
    with pytest.raises(ValueError):
        B._register_text_spec(other)


def test_table_entries_are_clean():
    for name, city, state, _src in [v for v in WD.TABLES[B.AHN].values()] + \
            [v for v in WD.TABLES[B.GEISINGER].values()]:
        assert state in STATES and city and (name is None or name == name.strip())
    for site in B.KAISER_SITES.values():
        assert site[0].startswith("Kaiser Permanente ") and site[2] in STATES and site[1]
        assert "  " not in site[0] and not re.search(r"\b(?:Ofcs|Offcs|Med Off|MOB)\b", site[0]), site[0]
    for hit in B.JEFFERSON_STREETS.values():
        assert hit[2] in ("PA", "NJ") and hit[0] in B.JEFFERSON_NAMES
    for _rx, name, towns, src in B.OCHSNER_TITLE_RULES:
        assert towns and re.match(r"^cms \d{6}", src), name


# ── Real stored rows: the change list's own answers ─────────────────────────

@pytest.mark.parametrize("rid", sorted(ROWS))
def test_stored_row_gets_the_change_list_answer(rid):
    r = ROWS[rid]
    j = _run(r)
    got = [j.hospital_name, j.city, j.state]
    if r["want"] is None:
        assert j.hospital_name == r["hospital_system"], (r["tag"], got)
    else:
        assert got == r["want"], (r["tag"], got)
    assert j.hospital_system == r["hospital_system"]          # never touched


def test_geisinger_names_hospitals_and_named_clinics_never_offices_or_other_systems():
    assert _run(BY_TAG[(B.GEISINGER, "hospital")]).hospital_name in (
        "Geisinger Lewistown Hospital", "Geisinger Wyoming Valley Medical Center")
    assert _run(BY_TAG[(B.GEISINGER, "clinic")]).hospital_name == "Geisinger Primary Care Pottsville"
    for tag in ("remote", "office", "other system", "elsewhere"):
        assert _run(BY_TAG[(B.GEISINGER, tag)]).hospital_name == B.GEISINGER, tag
    # a site line in another town than the posting's own names nothing (no town is guessed)
    r = BY_TAG[(B.GEISINGER, "elsewhere")]
    assert "Geisinger Lewistown Hospital (GLH)" in r["description"] and r["city"] == "State College"


def test_geisinger_site_descriptor_gives_the_town_too():
    j = _run(BY_TAG[(B.GEISINGER, "workday site")])
    assert (j.hospital_name, j.city) == ("Geisinger Medical Center Muncy", "Muncy")
    assert WD.facility(B.GEISINGER, "Danville, PA", "") is None            # a town descriptor: as before
    assert WD.facility(B.GEISINGER, "Geisinger Wyoming Valley Courier Hub", "") == \
        (B.GEISINGER, "Wilkes-Barre", "PA")                                # an office: the label, its town


def test_sutter_entity_names_its_hospital_in_its_own_town_only():
    assert _run(BY_TAG[(B.SUTTER, "campus")]).hospital_name == "Mills-Peninsula Medical Center"   # San Mateo campus
    assert _run(BY_TAG[(B.SUTTER, "foundation")]).hospital_name == B.SUTTER
    j = _run(BY_TAG[(B.SUTTER, "elsewhere")])                              # Sutter Coast entity, Brookings OR row
    assert (j.hospital_name, j.city) == (B.SUTTER, "Brookings")


def test_ahn_live_descriptors_and_their_links_are_one_key():
    for loc, path in _DATA["ahn_live_list"]:
        assert WD.facility(B.AHN, loc, path) == WD.facility(B.AHN, "", path), loc
    got = {loc: WD.facility(B.AHN, loc, path) for loc, path in _DATA["ahn_live_list"]}
    assert got["Pittsburgh PA, 15212, 320 E N Ave."] == ("Allegheny General Hospital", "Pittsburgh", "PA")
    assert got["Natrona Heights PA, 15065, 1301 Carlisle Street"] == \
        ("Allegheny Valley Hospital", "Natrona Heights", "PA")
    assert got["Pittsburgh PA, 15224, West Penn 4800 Friendship Ave"][0] == "West Penn Hospital"
    assert got["Monroeville PA, 15146, 2580 Haymaker Rd"] is None           # the Forbes campus offices
    assert got["Erie PA, 16502, 2315 Myrtle St"] is None
    # a "2 Locations" posting: its link's segment is the primary site
    assert WD.facility(B.AHN, "2 Locations", "/job/Canonsburg-PA-15317-100-Medical-Blvd/x_J1") == \
        ("Canonsburg Hospital", "Canonsburg", "PA")


def test_jefferson_address_line():
    j = _run(BY_TAG[(B.JEFFERSON, "hospital from county")])
    assert (j.hospital_name, j.city, j.state) == ("Jefferson Abington Hospital", "Abington", "PA")
    j = _run(BY_TAG[(B.JEFFERSON, "office town")])                          # county row, office street
    assert (j.hospital_name, j.city, j.state) == (B.JEFFERSON, "Fort Washington", "PA")
    j = _run(BY_TAG[(B.JEFFERSON, "office philadelphia")])                  # a real town is never moved
    assert (j.hospital_name, j.city) == (B.JEFFERSON, "Philadelphia")
    assert _run(BY_TAG[(B.JEFFERSON, "care site")]).hospital_name == B.JEFFERSON     # Willowcrest
    assert B.jefferson_address("x\nPrimary Location Address\n, , ,\n") is None
    assert B.jefferson_street_hit("111 S 9th Street", "Philadelphia", "PA") is None   # not 111 S 11th
    assert B.jefferson_street_hit("1200 Old York Road", "Philadelphia", "PA") is None  # another town's street


def test_jefferson_night_without_a_body_keeps_the_stored_town():
    r = BY_TAG[(B.JEFFERSON, "hospital from county")]
    W.set_stored([{"hospital_system": B.JEFFERSON, "job_id": str(r["job_id"]),
                   "hospital_name": "Jefferson Abington Hospital", "city": "Abington", "state": "PA"}])
    j = _job(r, description="")
    T.apply_facility(j)
    B.apply_before_town([j])
    W.apply_job(j)                     # the town tables keep the stored name, not the town
    assert (j.hospital_name, j.city) == ("Jefferson Abington Hospital", "Montgomery County")
    B.apply_after_town([j])
    assert (j.hospital_name, j.city, j.state) == ("Jefferson Abington Hospital", "Abington", "PA")
    # a stored office town comes back the same way
    r = BY_TAG[(B.JEFFERSON, "office town")]
    W.set_stored([{"hospital_system": B.JEFFERSON, "job_id": str(r["job_id"]),
                   "hospital_name": B.JEFFERSON, "city": "Fort Washington", "state": "PA"}])
    j = _job(r, description="")
    W.apply_job(j)
    B.apply_after_town([j])
    assert (j.hospital_name, j.city) == (B.JEFFERSON, "Fort Washington")


@pytest.mark.parametrize("title, city, want", [
    ("Hybrid OR Interventional Radiologic Specialist - Full Time - Jeff Hwy", "New Orleans", "Ochsner Medical Center"),
    ("RN - Acute Medical Stepdown Unit - OMC, Jeff Hwy - FT - Days", "New Orleans", "Ochsner Medical Center"),
    ("Supv-RN OR/Surgery Charge Nurse Baptist", "New Orleans", "Ochsner Baptist"),
    ("RN- OLGMC 5C Med Renal- Full Time Days", "Lafayette", "Ochsner Lafayette General Medical Center"),
    ("EVS - OLG Ortho", "Lafayette", "Ochsner Lafayette General Orthopaedic Hospital"),
    ("Respiratory Therapist - OUHC Respiratory Therapy - Full Time", "Lafayette",
     "Ochsner University Hospital & Clinics"),
    ("Sonographer - Chabert - full time", "Houma", "Leonard J. Chabert Medical Center"),
    ("RN - Med/Surg - St. Bernard Parish Hospital - Nights", "Chalmette", "St. Bernard Parish Hospital"),
    # none: several campuses, a clinic, a health center, a campus in another town, a remote job
    ("RN - Cath Lab - Baptist/Clearview/Jeff Hwy campuses", "New Orleans", None),
    ("Certified Medical Assistant- OUHC Internal Medicine Clinic GME- Full Time", "Lafayette", None),
    ("LPN- OLG Community Health Center- Full Time", "Lafayette", None),
    ("RN - Med Surg - Jeff Hwy", "Kenner", None),
    ("Coder - Remote - Jeff Hwy", "New Orleans", None),
    ("Physician - Urologic Oncology - Academic", "Shreveport", None),
])
def test_ochsner_titles(title, city, want):
    hit = B.ochsner_title(title, "", city, "LA")
    assert (hit[0] if hit else None) == want, hit


@pytest.mark.parametrize("title, want", [
    ("Env Services Aide-Crestwood Medical Center-FT-2nd shift", "Crestwood Medical Center"),
    ("RN Staff-Crestwood-Surgery-FT-2nd shift", "Crestwood Medical Center"),
    ("RN Staff - Madison Hospital Acute Surgical Care 4th FL - PT - 1st Shift", "Madison Hospital"),
    ("RN Staff- ICU/CCU, PRN, Nights, DMH", "Decatur Morgan Hospital"),
    ("Medical Assistant-Crestwood Family Medicine-FT", None),
    ("RN Staff- ER, Full-Time, Nights", None),
])
def test_huntsville_titles(title, want):
    hit = I.title_facility(B.HUNTSVILLE, title)
    assert (hit[0] if hit else None) == want


def test_huntsville_town_table():
    body = "Overview\n\n" + "Provides care. " * 80
    got = W.decide(B.HUNTSVILLE, B.HUNTSVILLE, "Scottsboro", "AL", "RN Staff - ICU - FT", body, "")
    assert got[:3] == ("Highlands Medical Center", "Scottsboro", "AL")
    assert W.decide(B.HUNTSVILLE, B.HUNTSVILLE, "Huntsville", "AL", "RN Staff - ICU - FT", body, "")[3] == "shared"
    assert W.decide(B.HUNTSVILLE, B.HUNTSVILLE, "Scottsboro", "AL", "CNA - Highlands Care Center - FT", body,
                    "")[3] == "care-site"
    assert W.decide(B.HUNTSVILLE, B.HUNTSVILLE, "Albertville", "AL", "RN - FT", body, "")[3] == "no-town"


def test_kaiser_medical_offices():
    j = _run(BY_TAG[(B.KAISER, "medical office")])
    assert (j.hospital_name, j.city) == ("Kaiser Permanente Sunnybrook Medical Offices", "Clackamas")
    for tag in ("office", "other system", "building"):
        assert _run(BY_TAG[(B.KAISER, tag)]).hospital_name == B.KAISER, tag
    # a posting in another town keeps the label; one at the head-office default takes the site's town
    got = P.resolve_row(B.KAISER, hospital_name=B.KAISER, description="Department: Kona Medical Offices - "
                        "Primary Care - 1208\n" + "x" * 1200, city="Hilo", state="HI")
    assert got is None
    got = P.resolve_row(B.KAISER, hospital_name=B.KAISER, description="Department: Kona Medical Offices - "
                        "Primary Care - 1208\n" + "x" * 1200, city="Oakland", state="CA")
    assert got[:3] == ("Kaiser Permanente Kona Medical Offices", "Kailua Kona", "HI")


# ── Through finalize_jobs (the upsert payload) ──────────────────────────────

def test_finalize_jobs_runs_both_passes(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    rows = [BY_TAG[(B.OCHSNER, "jeff hwy hybrid")], BY_TAG[(B.JEFFERSON, "NJ hospital")],
            BY_TAG[(B.JEFFERSON, "office town")], BY_TAG[(B.GEISINGER, "clinic")], BY_TAG[(B.SUTTER, "hospital")]]
    out = {r["job_id"]: r for r in scraper.finalize_jobs([_job(r) for r in rows])}
    want = {str(r["job_id"]): r["want"] for r in rows}
    for jid, w in want.items():
        got = out[jid]
        assert [got["hospital_name"], got["city"], got["state"]] == w, (jid, got["hospital_name"])
        assert got["location"] == f"{w[1]}, {w[2]}"
    assert {out[j]["hospital_system"] for j in want} == {B.OCHSNER, B.JEFFERSON, B.GEISINGER, B.SUTTER}


def test_a_pass_that_raises_never_stops_the_run(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])

    def boom(jobs):
        raise RuntimeError("x")

    monkeypatch.setattr(B, "apply_before_town", boom)
    monkeypatch.setattr(B, "apply_after_town", boom)
    r = BY_TAG[(B.GEISINGER, "clinic")]
    out = scraper.finalize_jobs([_job(r)])
    assert out and out[0]["hospital_name"] == "Geisinger Primary Care Pottsville"


# ── Adventist Health (Oracle work location, live page) ──────────────────────

@pytest.mark.parametrize("item", _DATA["adventist_live"], ids=lambda i: i["workLocation"]["items"][0]["LocationName"])
def test_adventist_work_location(item):
    import facilities_oracle_other as O
    import facility_names as F
    w = item["workLocation"]["items"][0]
    name = O.oracle_location(B.ADVENTIST, w["LocationName"])
    got = F.list_facility(B.ADVENTIST, name, w["TownOrCity"], w["Region2"], item["Title"])
    assert (got if got and got != B.ADVENTIST else None) == item["want"], (w["LocationName"], got)
    if item["want"]:
        assert O.oracle_site_town(B.ADVENTIST, item, got) == (w["TownOrCity"], w["Region2"])
