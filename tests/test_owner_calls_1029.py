"""2026-10-29: the owner's calls on the facility release ("go with recs").

  1. NYU Langone: "NYU Langone Hospital--Long Island" is the employing entity;
     it names the hospital only on a posting in Mineola, elsewhere the row
     shows "NYU Langone Health" with its own town.
  2. UPMC: a label row whose title names exactly ONE UPMC hospital (in the
     posting's own town, or at the head office default / no town) names it;
     two or more, or another town, keep "UPMC".
  3. BayCare: the Infor list's hospital short name on a clinic row (another
     town, or a site line naming a clinic or an outpatient site) keeps
     "BayCare" with its town; a clinic the posting names in full keeps its
     own name.
  4. Trinity Health (MMCIA): only the hospital's own site keeps "MercyOne Des
     Moines Medical Center"; a clinic site spelled in full shows that clinic,
     every other site "Trinity Health" with its town.

Fixture (tests/fixtures/owner_calls_1029/stored_rows.json): real stored
hospital_jobs rows read 2026-10-29 (BayCare bodies cut a little past their
Facility / Location lines), each with the value the stored-row change list
gives it (want). The BayCare and Trinity real list payloads already in the
repo (tests/fixtures/infor_list.json, tests/fixtures/workday1029) run through
the adapters too. No network, no database."""
import json
import os

import pytest

import facilities_findly
import facilities_oracle_other as O
import facilities_text as T
import facilities_workday as W
import facility_names as F
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "owner_calls_1029")
with open(os.path.join(FIX, "stored_rows.json"), encoding="utf-8") as _f:
    DOC = json.load(_f)


@pytest.fixture(autouse=True)
def _no_known():
    F.set_known_facilities([])
    T.set_known_facilities([])
    yield
    F.set_known_facilities([])
    T.set_known_facilities([])


def _job(system, name, title="RN", city="", state="", description="", job_id="1", url="https://x/1"):
    return Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="", url=url,
               job_id=job_id, posted_date="", description=description, ats_platform="")


# ── 1. NYU Langone ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("r", DOC["nyu"], ids=lambda r: r["city"])
def test_nyu_long_island_stored_rows(r):
    for spelling in (r["stored_name"], "NYU Langone Hospital--Long Island", "NYU Langone Hospital—Long Island"):
        j = _job("NYU Langone Health", spelling, r["title"], r["city"], r["state"])
        O.rename_jobs([j])
        F.apply_facilities([j])
        want = "NYU Langone Hospital - Long Island" if r["city"] == "Mineola" else "NYU Langone Health"
        assert (j.hospital_name, j.city, j.state) == (want, r["city"], r["state"]), (spelling, r)
        assert [j.hospital_name, j.city, j.state] == r["want"]


def test_nyu_rows_cover_mineola_and_the_offices():
    towns = {r["city"] for r in DOC["nyu"]}
    assert "Mineola" in towns and {"Garden City", "Huntington", "Lake Success", "Bethpage"} <= towns


def test_nyu_other_hospitals_and_no_town():
    # the other NYU hospitals are not town-bound (the owner's call is Long Island's)
    for name, city in (("NYU Langone Hospital—Suffolk", "Patchogue"), ("NYU Langone Hospital - Brooklyn", "Brooklyn"),
                       ("NYU Langone Hospital—Suffolk", "Shirley")):
        j = _job("NYU Langone Health", name, city=city, state="NY")
        O.rename_jobs([j])
        F.apply_facilities([j])
        assert j.hospital_name.startswith("NYU Langone Hospital - ") and j.city == city
    # a Long Island posting with no town: the system, and no town is guessed
    j = _job("NYU Langone Health", "NYU Langone Hospital--Long Island")
    O.rename_jobs([j])
    F.apply_facilities([j])
    assert (j.hospital_name, j.city, j.state) == ("NYU Langone Health", "", "")
    # Mineola spelled in upper case, state blank: the hospital
    j = _job("NYU Langone Health", "NYU Langone Hospital--Long Island", city="MINEOLA")
    O.rename_jobs([j])
    assert j.hospital_name == "NYU Langone Hospital - Long Island"


def test_town_only_names_a_listed_hospital():
    with pytest.raises(ValueError):
        O._sys("X Test System", town_only=("Not A Hospital",), hospitals={"A Hospital": ("Town", "TX", "address 1 A St, Town")})
    O.SYSTEMS.pop("X Test System", None)


def test_findly_cws_feed_rows_through_finalize_order():
    # the department value as the Findly CWS feed gives it, then the two
    # finalize passes in their order (rename_jobs, apply_facilities)
    rows = [{"department": "NYU Langone Hospital--Long Island", "primary_city": "Garden City", "primary_state": "NY"},
            {"department": "NYU Langone Hospital--Long Island", "primary_city": "Mineola", "primary_state": "NY"},
            {"department": "NYU Grossman School of Medicine", "primary_city": "New York", "primary_state": "NY"}]
    out = []
    for d in rows:
        j = _job("NYU Langone Health", scraper._findly_cws_facility("NYU Langone Health", d),
                 city=d["primary_city"], state=d["primary_state"])
        O.rename_jobs([j])
        F.apply_facilities([j])
        out.append(j.hospital_name)
    assert out == ["NYU Langone Health", "NYU Langone Hospital - Long Island", "NYU Langone Health"]


# ── 2. UPMC ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("r", DOC["upmc"], ids=lambda r: str(r["id"]))
def test_upmc_stored_label_rows(r):
    site = facilities_findly.upmc({"title": r["title"], "brand": "", "business_unit": ""}, r["city"], r["state"])
    j = _job("UPMC", site[0] if site else "UPMC", r["title"], r["city"], r["state"])
    F.apply_facilities([j])
    assert [j.hospital_name, j.city, j.state] == r["want"], r


def test_upmc_fixture_has_every_kind():
    named = [r for r in DOC["upmc"] if r["want"][0] != "UPMC"]
    kept = [r for r in DOC["upmc"] if r["want"][0] == "UPMC" and F.upmc_title_hospitals(r["title"], r["city"])]
    moved = [r for r in named if r["want"][1] != r["city"]]
    assert len(named) >= 90 and len(kept) == 4 and len(moved) == 2


@pytest.mark.parametrize("title,city,state,want", [
    ("CRNA, UPMC Carlisle", "Carlisle", "PA", "UPMC Carlisle"),
    ("CRNA, UPMC Carlisle", "Pittsburgh", "PA", "UPMC Carlisle"),          # the head office default: the hospital
    ("CRNA, UPMC Carlisle", "", "", "UPMC Carlisle"),
    ("PA or CRNP, UPMC Somerset Ear, Nose, and Throat", "Johnstown", "PA", ""),   # another town: a practice
    ("Speech Language Pathologist (UPMC Altoona and Bedford)", "Altoona", "PA", ""),
    ("Graduate Nurse: UPMC Washington and Greene", "Washington", "PA", ""),
    ("Paramedic | UPMC Cole, Wellsboro, & Williamsport Locations", "Williamsport", "PA", ""),
    ("CRNA Casual, UPMC Community Osteopathic, Harrisburg PA", "Harrisburg", "PA", "UPMC Community Osteopathic"),
    ("CRNA - UPMC Presbyterian Shadyside", "Pittsburgh", "PA", ""),           # two hospitals
    ("RN - UPMC Northwest Primary Care", "Seneca", "PA", ""),                 # a practice, not the hospital
    ("CRNA - UPMC East Side Surgery Center", "Pittsburgh", "PA", ""),
    ("Midwife - Magee Womens Hospital", "Pittsburgh", "PA", "Magee-Womens Hospital of UPMC"),
    ("Neonatal Nurse Practitioner - UPMC Children’s Hospital of Pittsburgh", "Pittsburgh", "PA",
     "UPMC Children's Hospital of Pittsburgh"),
    ("RN - UPMC St Margaret's", "Pittsburgh", "PA", "UPMC St. Margaret"),
    ("Behavioral Health Navigator I - UPMC Western Maryland", "Cumberland", "MD", "UPMC Western Maryland"),
    ("Behavioral Health Navigator I - UPMC Western Maryland", "Cumberland", "PA", ""),   # the hospital is in MD
    ("Medical Assistant - UPMC Blair Medical Associates", "Altoona", "PA", ""),          # no hospital named
    # review 2026-10-29: title-only spellings
    ("Director, Quality Management-UPMC York Memorial", "York", "PA", "UPMC Memorial"),
    ("Director, Quality Management-UPMC York Memorial", "Harrisburg", "PA", ""),         # another town
    ("RN - UPMC York Memorial and Hanover", "York", "PA", ""),                           # two hospitals
    ("Ophthalmic Technician (Children's Hospital of Pittsburgh)", "Pittsburgh", "PA",
     "UPMC Children's Hospital of Pittsburgh"),
    ("Professional Staff Nurse, OP - OB/GYN - Magee Women’s Hospital", "Pittsburgh", "PA",
     "Magee-Womens Hospital of UPMC"),
    ("LPN Lead - UPMC Magee Women Bladder & Pelvic Health", "Lancaster", "PA", ""),      # a practice
])
def test_upmc_title_rule(title, city, state, want):
    assert F.upmc_title_facility(title, city, state) == want


def test_upmc_title_aliases_leave_business_units_alone():
    # the title-only spellings are not business-unit spellings
    assert F.upmc_facility("", "UPMC York Memorial", "York", "PA") == ""
    assert "UPMC York Memorial" not in F._UPMC_ALIASES


def test_upmc_unit_and_brand_still_come_first():
    # a business unit that names a hospital wins over the title
    got = facilities_findly.upmc({"title": "CRNA, UPMC Carlisle", "brand": "UPMC Presbyterian",
                                  "business_unit": "UPMC Presbyterian"}, "Pittsburgh", "PA")
    assert got == ("UPMC Presbyterian", "Pittsburgh", "PA")
    got = facilities_findly.upmc({"title": "CRNA, UPMC Carlisle", "brand": "University of Pittsburgh Physicians",
                                  "business_unit": "University of Pittsburgh Physicians"}, "Carlisle", "PA")
    assert got == ("UPMC Carlisle", "Carlisle", "PA")


# ── 3. BayCare ──────────────────────────────────────────────────────────────

def _bc_job(r, description=None):
    return _job("BayCare", r["adapter_value"], r["title"], r["city"], r["state"],
                r["description"] if description is None else description, r["job_id"])


@pytest.mark.parametrize("r", DOC["baycare"], ids=lambda r: str(r["id"]))
def test_baycare_stored_rows(r):
    j = _bc_job(r)
    T.apply_facility(j)
    assert [j.hospital_name, j.city, j.state] == r["want"], r


@pytest.mark.parametrize("rid,want", [
    (32705479, "BayCare"),                                 # "Largo:Morton Plant", no site line: another town
    (32706101, "BayCare"),                                 # "Location: Largo, FL 33777"
    (32706214, "Morton Plant North Bay Recovery Center"),  # "Facility: North Bay Recovery Center", Lutz
    (32965731, "Morton Plant North Bay Recovery Center"),  # "Behavioral Health- Morton Plant North Bay Recover Center"
    (32706056, "BayCare"),                                 # the recovery center line on a New Port Richey posting
    (32705559, "St. Anthony's Primary Care Pasadena"),
    (37263801, "Bardmoor Outpatient Center"),
    (38612217, "BayCare"),                                 # "Bardmoor Outpt and Surgery - 8787 Bryan Dairy Rd"
    (32706352, "BayCare"),                                 # "Free Standing ER, 102 S. Dale Mabry, Tampa"
    (34799542, "BayCare"),                                 # "BayCare ER (South Tampa)"
    (32706624, "BayCare"),                                 # a system-office Facility line, then "Morton Plant Trinity Outpt"
    (34274079, "Morton Plant Hospital"),                   # "Morton Plant Hospital Axelrod Pavillon - Outpatient Imaging"
    (32706183, "Morton Plant Hospital"),                   # "Morton Plant Hospital Behavioral Health"
    (32705133, "Mease Dunedin Hospital"),                  # "BayCare Health System, CT Scan-MDH"
    (32705137, "Mease Countryside Hospital"),
    # review 2026-10-29: a system-office line with no department code names the site
    (36313627, "BayCare"),                                 # "BayCare Health System, Cheek Powell Wellness Center"
    (32705834, "BayCare"),                                 # "BayCare Health System, Wellness Center- Cheek Powell"
    (38569333, "BayCare"),                                 # "BayCare Health System, Mastectomy - HomeCare"
    (32706115, "BayCare"),                                 # "Home base county: Hillsborough County ..."
])
def test_baycare_named_cases(rid, want):
    r = next(x for x in DOC["baycare"] if x["id"] == rid)
    j = _bc_job(r)
    T.apply_facility(j)
    assert j.hospital_name == want and (j.city, j.state) == (r["city"], r["state"])


def test_baycare_office_line_with_a_department_code_decides_nothing():
    pad = "At BayCare, we are proud to be one of the largest employers in the Tampa Bay area. " * 8
    for line, want in (("BayCare Health System, Outpatient Testing-MPH", "Morton Plant Hospital"),
                       ("BayCare Health System, Home Care Coordination - MPH", "Morton Plant Hospital"),
                       ("BayCare Health System, Trinity Nuc Med-MPM SRVCS", "Morton Plant Hospital"),
                       ("BayCare Health System, Cheek Powell Wellness Center", "BayCare")):
        body = f"{pad}\n\nFacility:\n{line}\n\nLocation: Morton Plant\n\nStatus: Full Time\n"
        j = _job("BayCare", "Morton Plant", city="Clearwater", state="FL", description=body)
        T.apply_facility(j)
        assert (j.hospital_name, j.city) == (want, "Clearwater"), line


def test_baycare_nights_without_a_body_keep_the_stored_decision():
    clinic = next(x for x in DOC["baycare"] if x["id"] == 32706352)      # Free Standing ER, Tampa
    named = next(x for x in DOC["baycare"] if x["id"] == 32706214)       # recovery center, Lutz
    hosp = next(x for x in DOC["baycare"] if x["id"] == 32705137)        # Mease Countryside
    T.set_known_facilities([{"hospital_system": "BayCare", "job_id": clinic["job_id"], "hospital_name": "BayCare"},
                            {"hospital_system": "BayCare", "job_id": named["job_id"],
                             "hospital_name": "Morton Plant North Bay Recovery Center"},
                            {"hospital_system": "BayCare", "job_id": hosp["job_id"],
                             "hospital_name": "Mease Countryside Hospital"}])
    for r, want, how in ((clinic, "BayCare", "kept"), (named, "Morton Plant North Bay Recovery Center", "kept"),
                         (hosp, "Mease Countryside Hospital", "canon")):
        j = _bc_job(r, description="")
        assert T.apply_facility(j) == how and j.hospital_name == want, r["id"]
    # nothing stored: the hospital in its own town, the label elsewhere
    T.set_known_facilities([])
    j = _bc_job(clinic, description="")
    assert T.apply_facility(j) == "canon" and j.hospital_name == "St. Joseph's Hospital"
    j = _job("BayCare", "Morton Plant", city="Largo", state="FL")
    assert T.apply_facility(j) == "elsewhere" and (j.hospital_name, j.city) == ("BayCare", "Largo")


def test_baycare_known_map_keeps_label_rows_for_clinic_systems_only():
    T.set_known_facilities([{"hospital_system": "BayCare", "job_id": "9", "hospital_name": "BayCare"},
                            {"hospital_system": "UNC Health", "job_id": "9", "hospital_name": "UNC Health"}])
    assert T._KNOWN_CANON == {("BayCare", "9"): "BayCare"}
    assert T.known_facility("BayCare", "9") == ""          # the label is no site for the text path


def test_baycare_label_row_names_a_clinic_it_spells():
    r = next(x for x in DOC["baycare"] if x["id"] == 32705633)           # stored "BayCare", "St Anthonys Primary Care Baywalk"
    j = _bc_job(r)
    assert T.apply_facility(j) == "text" and j.hospital_name == "St. Anthony's Primary Care Baywalk"


def test_baycare_real_infor_list_rows_unchanged():
    # tests/fixtures/infor_list.json, read 2026-09-24: "Tampa:St Josephs" and "All Locations"
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "infor_list.json"), encoding="utf-8") as f:
        data = json.load(f)["BayCare"]
    rows = scraper._infor_rows(data)
    jobs, _rid = scraper._infor_jobs("BayCare", scraper.INFOR_ORGS["BayCare"], rows)
    T.apply_facilities(jobs)
    got = {j.job_id: (j.hospital_name, j.city, j.state) for j in jobs}
    assert got["173660"] == ("St. Joseph's Hospital", "Tampa", "FL")
    assert got["174707"][0] == "BayCare"


def test_clinic_table_entries_are_clean():
    spec = T.SPECS["BayCare"]
    assert spec.clinic_names <= set(spec.names)
    for name in spec.clinic_names:
        site = spec.names[name]
        assert site.source == "board" and site.state == "FL"
    # a clinic spelling never names a hospital, and the other systems have no clinic rules
    assert all(s.clinic_rx is None for k, s in T.SPECS.items() if k != "BayCare")


# ── 4. Trinity Health (MMCIA) ───────────────────────────────────────────────

def _trinity_site(url):
    path = "/job/" + url.split("/job/", 1)[1]
    loc = W._site_text("", path)
    city, state = scraper.parse_city_state(loc)
    site = scraper.WD_SITE_FACILITY["Trinity Health"](loc, path)
    if site:
        return site
    fac, city, state = scraper._wd_facility("Trinity Health", loc, city, state)
    return (fac or "Trinity Health", city, state)


@pytest.mark.parametrize("r", DOC["trinity"], ids=lambda r: r["url"].split("/job/")[1].split("/")[0][:40])
def test_trinity_mmcia_stored_rows(r):
    assert list(_trinity_site(r["url"])) == r["want"], r
    assert r["want"][1:] == [r["city"], r["state"]]                     # no town moves


def test_trinity_mmcia_names():
    got = {r["url"].split("/job/")[1].split("/")[0]: r["want"][0] for r in DOC["trinity"]}
    assert got["MMCIA---MercyOne-Clive-Clinic-Central-IA"] == "MercyOne Clive Clinic"
    assert got["MMCIA---MercyOne-Des-Moines-Medical-Plaza-Central-IA"] == "Trinity Health"
    assert got["MMCIA---MercyOne--House-of-Mercy-Central-IA"] == "Trinity Health"
    assert got["MMCIA---MercyOne-IHC-West-University--Central-IA"] == "Trinity Health"
    assert got["MMCIA---MercyOne-Mercy-Fam-Med-Grimes-Central-IA"] == "Trinity Health"     # abbreviated: no name
    assert not any(v == "MercyOne Des Moines Medical Center" for k, v in got.items()
                   if k != "MMCIA---MercyOne-Des-Moines-Medical-Center-Central-IA")
    # the hospital's own site, and the other MMCIA hospitals
    for seg, want in (("MMCIA---MercyOne-Des-Moines-Medical-Center-Central-IA", ("MercyOne Des Moines Medical Center", "Des Moines")),
                      ("MMCIA---MercyOne-West-Des-Moines-Medical-Center-Central-IA", ("MercyOne West Des Moines Medical Center", "West Des Moines")),
                      ("MMCIA---MercyOne-Newton-Medical-Center-Central-IA", ("MercyOne Newton Medical Center", "Newton")),
                      ("MMCIA---MercyOne-Centerville-Medical-Center-Central-IA", ("MercyOne Centerville Medical Center", "Centerville"))):
        assert _trinity_site(f"https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/{seg}/RN_1")[:2] == want


def test_trinity_map_hospitals_match_the_site_table():
    # every MMCIA hospital the map names is the name the site table gives
    table_names = set(W.names("Trinity Health"))
    for k, (name, city, state) in scraper.WD_FACILITY_MAP["Trinity Health"].items():
        if k.startswith("mmcia") and name and not name.endswith("Clinic"):
            assert name in table_names, (k, name)
        if k.startswith("mmcia"):
            assert name != "MercyOne Des Moines Medical Center" or k == "mmcia - mercyone des moines medical center", k


def test_trinity_real_list_page_through_the_adapter(monkeypatch):
    # tests/fixtures/workday1029/wd_list_pages.json (read 2026-10-29): the
    # Ankeny and Prairie Trail clinics through scrape_workday and normalize_job
    from test_facilities_workday_1029 import _rows
    got = {jid: (d["hospital_name"], d["city"], d["state"]) for jid, d in _rows(monkeypatch, "Trinity Health").items()}
    assert got["00703686"] == ("MercyOne Ankeny Clinic", "Ankeny", "IA")
    assert ("MercyOne Prairie Trail Clinic", "Ankeny", "IA") in set(got.values())
    assert not any(v[0] == "MercyOne Des Moines Medical Center" for v in got.values())
