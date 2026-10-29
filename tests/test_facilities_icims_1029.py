"""2026-10-29: iCIMS and Jibe rows name their hospital (facilities_icims).

Fixtures (tests/fixtures/icims1029/):
  jibe_rows.json     real /api/jobs list rows read 2026-10-29, one page of
                     100 per board, up to 8 rows per board (one per distinct
                     outcome), fields trimmed, bodies cut to 300 characters;
                     _expect is the name the tables give
  classic_rows.json  stored rows of the classic portals (title, town, the
                     opening of the stored body; Emory's to its Location
                     line), read-only from the database that day
No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_icims as F
import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "icims1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


BOARDS = _load("jibe_rows.json")["boards"]
CLASSIC = _load("classic_rows.json")["systems"]


def _row(system, **want):
    """The fixture row of `system` whose fields equal `want` (a tag list
    field matches its first entry)."""
    for r in BOARDS[system]["rows"]:
        ok = True
        for k, v in want.items():
            got = r.get(k)
            if isinstance(got, list):
                got = got[0] if got else None
            if got != v:
                ok = False
                break
        if ok:
            return r
    raise AssertionError(f"no fixture row {want} for {system}")


def _classic(system, job_id):
    return next(r for r in CLASSIC[system] if str(r["job_id"]) == str(job_id))


# ══ Jibe boards ══════════════════════════════════════════════════════════════
def test_every_board_of_the_family_is_a_configured_jibe_site():
    assert set(F.JIBE_SYSTEMS) == set(BOARDS)
    assert set(F.JIBE_SYSTEMS) <= set(scraper.JIBE_SITES)
    for system, b in BOARDS.items():
        assert scraper.JIBE_SITES[system] == b["base"], system


@pytest.mark.parametrize("system", sorted(BOARDS))
def test_each_fixture_row_gets_its_name_through_the_adapter_hook(system):
    for r in BOARDS[system]["rows"]:
        assert F.jibe_facility(system, r)[0] == r["_expect"]
        assert scraper._jibe_facility(system, r) == r["_expect"], (system, r.get("location_name"))


def test_fairview_reads_tags9_the_sites_own_name():
    S = F.FAIRVIEW
    assert F.jibe_facility(S, _row(S, tags9="Fairview Southdale Hospital")) == ("Fairview Southdale Hospital", "table")
    # the West Bank's West Building is the Riverside campus (same postings' location_name "UMMC West Bank ...")
    # (the values below are the board's own, read 2026-10-29)
    r = {"tags9": ["University of Minnesota Medical Center-West Bank, West Building"],
         "location_name": "UMMC West Bank Park Plaza"}
    assert F.jibe_facility(S, r)[0] == "Fairview Riverside University Hospital"
    assert F.jibe_facility(S, {"tags9": ["Fairview St John's Hospital"]})[0] == "Fairview St. John's Hospital"
    # a named residence or clinic keeps its own name; an office is the system
    assert F.jibe_facility(S, {"tags9": ["Willows Bend"]}) == ("Willows Bend", "site")
    assert F.jibe_facility(S, {"tags9": ["Fairview Apple Valley Clinic"]}) == ("Fairview Apple Valley Clinic", "site")
    assert F.jibe_facility(S, {"tags9": ["Midway Corporate Campus"]}) == (S, "office")
    assert F.jibe_facility(S, {"tags9": ["Fairview offices at 323 Stinson"]}) == (S, "office")
    # no tags9: the abbreviated location_name through its own table, else the system
    assert F.jibe_facility(S, {"location_name": "FV Ridges Hospital"})[0] == "Fairview Ridges Hospital"
    assert F.jibe_facility(S, {"location_name": "UMMC West Bank Prof Bldg"}) == (S, "unknown")


def test_medstar_location_name_then_the_hospital_entity():
    S = F.MEDSTAR
    assert F.jibe_facility(S, {"location_name": "MedStar Washington Hosp Ctr", "tags2": ["MedStar Medical Group"]}) \
        == ("MedStar Washington Hospital Center", "table")
    assert F.jibe_facility(S, {"location_name": "MSMHC-Hospital"})[0] == "MedStar Southern Maryland Hospital Center"
    assert F.jibe_facility(S, {"location_name": "MGUH Radiology New Mexico Ave",
                               "tags2": ["MedStar Georgetown University Hospital"]}) \
        == ("MedStar Georgetown University Hospital", "entity")
    assert F.jibe_facility(S, {"location_name": "Urgent Care-Adams Morgan", "tags2": ["MedStar Health Urgent Care"]})[0] \
        == "MedStar Health Urgent Care - Adams Morgan"
    assert F.jibe_facility(S, {"location_name": "Remote Anywhere US", "tags2": ["MedStar Medical Group"]}) == (S, "office")
    assert F.jibe_facility(S, {"location_name": "MHPT at Dundalk", "tags2": ["MedStar Health Physical Therapy"]}) \
        == (S, "unknown")


def test_mymichigan_medical_centers_and_their_towns():
    S = F.MYMICHIGAN
    assert F.jibe_facility(S, {"location_name": "Medical Center Sault"})[0] == "MyMichigan Medical Center Sault"
    assert F.jibe_town(S, "MyMichigan Medical Center Sault", "", "MI") == ("Sault Ste. Marie", "MI")
    assert F.jibe_town(S, "MyMichigan Medical Center Sault", "Midland", "MI") == ("Midland", "MI")   # never moved
    assert F.jibe_facility(S, {"location_name": "MyMichigan Medical Group"}) == (S, "unknown")


def test_piedmont_street_in_its_own_town():
    S = F.PIEDMONT
    r = _row(S, location_name="1255 Highway 54 West")
    assert F.jibe_facility(S, r) == ("Piedmont Fayette Hospital", "street")
    # the same street in another town is not that hospital
    assert F.jibe_facility(S, {**r, "city": "Atlanta", "street_address": ""})[0] == S
    assert F.jibe_facility(S, {"location_name": "350 Hospital Drive", "city": "Macon"})[0] == "Piedmont Macon Medical Center"
    assert F.jibe_facility(S, {"location_name": "PH Campus 1984 Bldg", "city": "Atlanta"}) \
        == ("Piedmont Atlanta Hospital", "campus")
    assert F.jibe_facility(S, {"location_name": "Remote Worker-In State", "city": "Atlanta"}) == (S, "office")
    # an outpatient street the table does not know: the posting's own "Location:" line
    out = {"location_name": "20 Francis Way", "city": "Sharpsburg",
           "description": "<p>Job Summary</p><p>Location: Piedmont Newnan Hospital</p>"}
    assert F.jibe_facility(S, out) == ("Piedmont Newnan Hospital", "body line")


def test_yale_street_names_the_hospital():
    S = F.YNHHS
    for v, want in [("267 grant st bridgeport, CT 06610", "Bridgeport Hospital"),
                    ("1450 CHAPEL STREET NEW HAVEN CT 06511", "Yale New Haven Hospital - Saint Raphael Campus"),
                    ("20 York street New Haven, 06510", "Yale New Haven Hospital"),
                    ("25 wells street, Westerly Rhode Island  02891", "Westerly Hospital"),
                    ("5520 Park Avenue, Trumbull, CT 06611", S),
                    ("Various locations from Stamford to New Rochelle", S)]:
        assert F.jibe_facility(S, {"tags7": [v], "city": ""})[0] == want, v
    # two sites on one line name none
    assert F._line_hit(S, "25 Wells St, Westerly RI / 46 Boston Post Road, Waterford, CT", "Westerly") is None


def test_osf_tanner_usa_uci_infirmary_obh():
    assert F.jibe_facility(F.OSF, {"location_name": "OSF St.James-JWAlbrechtMC"})[0] \
        == "OSF Saint James - John W. Albrecht Medical Center"
    assert F.jibe_facility(F.OSF, {"location_name": "OSF St. Francis Hospital"})[0] == "OSF St. Francis Hospital"
    assert F.jibe_facility(F.OSF, {"location_name": "OSF HealthCare Min Hdqtrs"}) == (F.OSF, "office")
    assert F.jibe_facility(F.TANNER, {"location_name": "Tanner Hospital Villa Rica-V0001"})[0] == "Tanner Hospital Villa Rica"
    assert F.jibe_facility(F.TANNER, {"location_name": "Tanner Resource Center Sys-S0002"}) == (F.TANNER, "office")
    assert F.jibe_facility(F.TANNER, {"location_name": "Tanner Care Point Sargent-T0062"}) \
        == ("Tanner Care Point Sargent", "site")
    assert F.jibe_facility(F.USA_HEALTH, {"location_name": "University Hospital"})[0] == "USA Health University Hospital"
    assert F.jibe_facility(F.USA_HEALTH, {"location_name": "Freestanding Emergency Department",
                                          "tags1": ["USA Health University Hospital"]}) \
        == ("USA Health University Hospital", "entity")
    assert F.jibe_facility(F.USA_HEALTH, {"location_name": "Technology and Research Park III",
                                          "tags1": ["USA Health Shared Services"]})[0] == F.USA_HEALTH
    assert F.jibe_facility(F.UCI, {"location_name": "Building 1 (MC)-3rd Fl", "street_address": "101 The City Drive"})[0] \
        == "UCI Medical Center"
    # "(MC)" off the City Drive campus is an office building
    assert F.jibe_facility(F.UCI, {"location_name": "Bldg 64 (MC)-Chapman-6th Fl",
                                   "street_address": "3800 W. Chapman Ave."})[0] == F.UCI
    assert F.jibe_facility(F.UCI, {"location_name": "Irvine Hospital - 4th Floor"})[0] == "UCI Health - Irvine"
    assert F.jibe_facility(F.INFIRMARY, {"location_name": "Corp"}) == (F.INFIRMARY, "office")
    assert F.jibe_facility(F.OBH, {"location_name": "Schulman and Schachne Nursing Homae"})[0] \
        == "Schulman and Schachne Nursing Home"


def test_a_board_outside_the_family_is_untouched():
    assert F.jibe_facility("Novant Health", {"location_name": "PHO - Presbyterian Medical Center"}) is None
    assert scraper._jibe_facility("Garnet Health", {"location_name": "Garnet Health Medical Center"}) \
        == "Garnet Health Medical Center"


class _R:
    status = 200

    def __init__(self, data):
        self._d = data

    async def json(self, content_type=None):
        return self._d


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


def test_scrape_jibe_stores_the_hospital_and_keeps_the_feeds_town(monkeypatch):
    S = F.FAIRVIEW
    rows = [dict(r) for r in BOARDS[S]["rows"]]
    rows[0] = {**rows[0], "city": "", "state": "Minnesota"}      # a row the feed gave no town
    assert F.JIBE_HOSPITALS[S].get(rows[0]["_expect"])              # a hospital of the table
    payload = {"jobs": [{"data": r} for r in rows], "totalCount": len(rows)}
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    jobs = asyncio.run(scraper.scrape_jibe(None, S, BOARDS[S]["base"]))
    by = {j.job_id: j for j in jobs}
    for r in rows:
        j = by[str(r["req_id"])]
        assert j.hospital_name == r["_expect"] and j.hospital_system == S
    first = by[str(rows[0]["req_id"])]
    hit = F.JIBE_HOSPITALS[S][first.hospital_name]
    assert (first.city, first.state) == (hit[0], hit[1])            # the hospital's town fills the blank
    for r in rows[1:]:
        assert by[str(r["req_id"])].city.lower() == str(r["city"]).lower()


# ══ Classic portals ══════════════════════════════════════════════════════════
def test_kettering_body_line_names_the_site_and_its_town():
    K = F.KETTERING
    r = _classic(K, 62411) if any(str(x["job_id"]) == "62411" for x in CLASSIC[K]) else CLASSIC[K][0]
    got = F.classic_facility(K, r["title"], r["description"], r["city"], r["state"])
    assert got == ("Kettering Health Main Campus", "Kettering", "OH", "body")
    by = {x["description"].split("\n\n")[1].split(" |")[0]: x for x in CLASSIC[K] if x["description"].startswith("Job")}
    soin = by["Kettering Health Soin"]
    assert F.classic_facility(K, soin["title"], soin["description"], "Kettering", "OH")[:3] \
        == ("Kettering Health Soin", "Beavercreek", "OH")
    # "KH Miamisburg | Full-Time": no town on the line, the hospital's own town
    kh = by["KH Miamisburg"]
    assert F.classic_facility(K, kh["title"], kh["description"], "Kettering", "OH")[:3] \
        == ("Kettering Health Miamisburg", "Miamisburg", "OH")
    # "Every Weekend Commitment" in the town's place is not a town
    wk = next(x for x in CLASSIC[K] if "Every Weekend Commitment" in x["description"][:200])
    assert F.kettering_line(wk["description"])[1] in ("Centerville", "")
    # a physician office: the system, in the line's town
    po = by["Physician Office"]
    assert F.classic_facility(K, po["title"], po["description"], "Kettering", "OH")[:3] \
        == (K, "Beavercreek", "OH")
    # a site named with no town keeps the row's own
    mid = by["KH Middletown"]
    assert F.classic_facility(K, mid["title"], mid["description"], "Kettering", "OH")[:3] \
        == ("Kettering Health Middletown", "Kettering", "OH")
    # no line, no answer
    no = next(x for x in CLASSIC[K] if not x["description"].startswith("Job Details"))
    assert F.classic_facility(K, no["title"], no["description"], no["city"], no["state"]) is None


def test_emory_location_line_then_the_title():
    E = F.EMORY
    rows = CLASSIC[E]
    mid = next(r for r in rows if "EMORY MIDTOWN HOSPITAL" in r["description"])
    assert F.classic_facility(E, mid["title"], mid["description"], mid["city"], mid["state"])[:3] \
        == ("Emory University Hospital Midtown", "Atlanta", "GA")
    ortho = next(r for r in rows if "ORTHO SPINE" in r["description"])
    assert F.classic_facility(E, ortho["title"], ortho["description"], ortho["city"], ortho["state"])[:3] \
        == ("Emory University Orthopaedics & Spine Hospital", "Tucker", "GA")
    clinic = next(r for r in rows if "THE EMORY CLINIC" in r["description"])
    assert F.classic_facility(E, clinic["title"], clinic["description"], clinic["city"], clinic["state"]) is None
    euh = next(r for r in rows if "Emory University Hospital," in r["title"])
    assert F.classic_facility(E, euh["title"], euh["description"], euh["city"], euh["state"])[0] \
        == "Emory University Hospital"
    assert F.classic_facility(E, "", "x\nLocation: Emory University Hospital (Atlanta, GA) Schedule: Full-time\n",
                              "Atlanta", "GA")[0] == "Emory University Hospital"


@pytest.mark.parametrize("title,city,want", [
    ("Supv Laboratory, Marshall Medical Centers North, Full Time, 2nd Shift", "Guntersville",
     "Marshall Medical Center North"),
    ("RN Staff, Marshall Medical Centers, South, ICU, Full-Time, Nights", "Boaz", "Marshall Medical Center South"),
    ("RN Staff, Athens-Limestone, ER, PT-1st Shift", "Athens", "Athens-Limestone Hospital"),
    ("Activities Therapist - Lincoln Nursing Home - FT", "Fayetteville", None),
    ("CLN Ultrasound Technologist - DeKalb Obstetric & Gynecology Clinic - FT - 1st Shift", "Fort Payne", None),
    ("RN Staff - Highlands 4th Floor - FT - 1st Shift", "Scottsboro", "Highlands Medical Center"),
    # the title's hospital is in another town than the row's: two answers, none taken
    ("HVAC Mechanic 2, Marshall Medical Centers South, Full Time, 1st Shift", "Guntersville", None),
])
def test_huntsville_titles(title, city, want):
    got = F.classic_facility("Huntsville Hospital Health System", title, "", city, "AL")
    assert (got[0] if got else None) == want


def test_huntsville_fixture_titles_are_real():
    titles = {r["title"] for r in CLASSIC["Huntsville Hospital Health System"]}
    assert "RN Staff - Highlands 4th Floor - FT - 1st Shift" in titles
    assert "Activities Therapist - Lincoln Nursing Home - FT" in titles


@pytest.mark.parametrize("title,want,town", [
    ("Instrument Tech, Northside Gwinnett", "Northside Hospital Gwinnett", "Lawrenceville"),
    ("Anesthesiologist - Northside Hospital Atlanta, Cherokee and Forsyth", None, None),
    ("Nurse Practitioner - Women's Specialists of Northside Gwinnett", None, None),
    ("Clinical Supervisor - L&D (Cherokee Campus)", "Northside Hospital Cherokee", "Canton"),
])
def test_northside_titles_move_the_row_off_the_atlanta_default(title, want, town):
    got = F.classic_facility("Northside Hospital", title, "", "Atlanta", "GA")
    assert (got[0] if got else None) == want
    if want:
        assert got[1:3] == (town, "GA")


def test_northern_arizona_titles():
    S = "Northern Arizona Healthcare"
    assert F.classic_facility(S, "Patient Safety Attendant - Flagstaff Medical Center", "", "Flagstaff", "AZ")[0] \
        == "Flagstaff Medical Center"
    assert F.classic_facility(S, "Nuclear Medicine Technologist - VVMC", "", "Cottonwood", "AZ")[0] \
        == "Verde Valley Medical Center"
    assert F.classic_facility(S, "Medical Assistant Certified - $1,000 sign-on bonus Verde Valley Medical Center "
                                 "(Cottonwood, Sedona, Camp Verde)", "", "Cottonwood", "AZ") is None


def _job(system, title="RN", body="", city="", state="", name=None, job_id="1"):
    return scraper.Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
                       location=", ".join(p for p in (city, state) if p), specialty="", job_type="",
                       url=f"https://x.icims.com/jobs/{job_id}/rn/job", job_id=job_id, posted_date="",
                       description=body, ats_platform="iCIMS")


def test_apply_classic_keeps_the_stored_site_and_town_on_a_night_with_no_body():
    K = F.KETTERING
    F.set_stored([{"hospital_system": K, "job_id": "7", "hospital_name": "Kettering Health Soin",
                   "city": "Beavercreek", "state": "OH"},
                  {"hospital_system": K, "job_id": "8", "hospital_name": K, "city": "Centerville", "state": "OH"},
                  {"hospital_system": K, "job_id": "9", "hospital_name": K, "city": "Kettering", "state": "OH"}])
    try:
        j7 = _job(K, city="Kettering", state="OH", job_id="7")
        assert F.apply_classic(j7) == "kept"
        assert (j7.hospital_name, j7.city, j7.state) == ("Kettering Health Soin", "Beavercreek", "OH")
        j8 = _job(K, city="Kettering", state="OH", job_id="8")
        assert F.apply_classic(j8) == "kept" and (j8.hospital_name, j8.city) == (K, "Centerville")
        assert F.apply_classic(_job(K, city="Kettering", state="OH", job_id="9")) == "none"   # nothing to carry
        # tonight's body wins over the stored row
        j7b = _job(K, body="Job Details\n\nKettering Health Dayton | Dayton | Full-Time | Days\n\n" + "x" * 300,
                   city="Kettering", state="OH", job_id="7")
        assert F.apply_classic(j7b) == "body" and (j7b.hospital_name, j7b.city) == ("Kettering Health Dayton", "Dayton")
        # a facility the card already named is left as it is
        assert F.apply_classic(_job(K, name="Something Else", job_id="7")) == "other"
        assert F.apply_classic(_job("Covenant Health")) == ""
    finally:
        F.set_stored([])


def test_icims_tenant_applies_the_classic_tables(monkeypatch):
    rows = CLASSIC["Northside Hospital"]

    async def fake_list(session, system, domain):
        return [_job(system, title=r["title"], city="Atlanta", state="GA", job_id=str(r["job_id"])) for r in rows]

    monkeypatch.setattr(scraper, "scrape_icims", fake_list)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    jobs = asyncio.run(scraper._icims_tenant(None, "Northside Hospital", "careers-mynorthsidecareer.icims.com", None))
    by = {j.title: j for j in jobs}
    assert (by["Instrument Tech, Northside Gwinnett"].hospital_name, by["Instrument Tech, Northside Gwinnett"].city) \
        == ("Northside Hospital Gwinnett", "Lawrenceville")
    assert by["Anesthesiologist - Northside Hospital Atlanta, Cherokee and Forsyth"].hospital_name == "Northside Hospital"


def test_load_stored_without_credentials_carries_nothing(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert F.load_stored() == 0 and F._STORED == {}


# ══ Stored rows (the change list) ════════════════════════════════════════════
def test_stored_jibe_rows_take_only_their_own_site_line_or_title():
    S = F.UCI
    # UCI bodies' boilerplate names the Irvine hospital on rows the feed places in Orange: not taken
    assert F.stored_facility(S, {"title": "Clinical Nurse II", "city": "Orange", "state": "CA",
                                 "description": "UCI Health - Irvine opened in 2025 ... our medical center in Orange"}) is None
    assert F.stored_facility(F.PIEDMONT, {"title": "RN", "city": "Fayetteville", "state": "GA",
                                          "description": "About\nLocation: Piedmont Fayette Hospital\nMore"})[0] \
        == "Piedmont Fayette Hospital"
    assert F.stored_facility(F.YNHHS, {"title": "Pharmacy Technician-Bridgeport Hospital", "city": "Bridgeport",
                                       "state": "CT", "description": ""})[0] == "Bridgeport Hospital"
    # the old MyMichigan rows whose stored town still carries the site
    hit = F.stored_facility(F.MYMICHIGAN, {"title": "Phlebotomist", "city": "Location\nMedical Center Saginaw",
                                           "state": "MI", "description": ""})
    assert hit[:3] == ("MyMichigan Medical Center Saginaw", "Saginaw", "MI")


def test_stored_classic_rows_use_the_nightly_rule():
    K = F.KETTERING
    for r in CLASSIC[K]:
        a = F.stored_facility(K, r)
        b = F.classic_facility(K, r["title"], r["description"], r["city"], r["state"])
        assert (a[:3] if a else None) == (b[:3] if b else None)
