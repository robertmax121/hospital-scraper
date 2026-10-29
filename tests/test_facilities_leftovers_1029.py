"""2026-10-29 (facility scale, phase 3, owner: "i thought you were gonna edit
all the large multihospital systems"): the town-only Workday systems
(facilities_leftovers): MUSC, Sentara, Prisma, Methodist TX, Adventist MD,
Avera, Essentia, Baystate, OU Health, Cook Children's, ProMedica, UofL,
John Muir, Houston Healthcare.

Fixtures, no network, no database:
  tests/fixtures/leftovers1029/stored_rows.json  18 real postings as the
      database held them on 2026-10-29 (title, link, town, the first 1,500
      characters of the body; emails and phone numbers replaced) with the
      town rules' decision.
  DETAIL below: real jobRequisitionLocation values of the CXS detail
      payloads read on 2026-10-29 (the scan's samples and 21 reads of this
      build), with the hospital they name or "office".
"""
import json
import os
import types
import zlib
from datetime import date

import pytest

import facilities_leftovers as L
import facilities_town as T
import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "leftovers1029", "stored_rows.json")
ROWS = json.load(open(FIX, encoding="utf-8"))["rows"]


@pytest.fixture(autouse=True)
def _no_stored_names():
    T.set_stored([])
    T._STORED_LOADED[0] = False
    yield
    T.set_stored([])
    T._STORED_LOADED[0] = False


# (system, row town, jobRequisitionLocation, expected hospital or "office" or "unknown")
DETAIL = [
    ("Methodist Health System TX", "Celina", "MCEL - Methodist Celina Medical Center - MCEL",
     "Methodist Celina Medical Center"),
    ("Methodist Health System TX", "Mansfield", "MMMC  - Methodist Mansfield Medical Center - MMMC",
     "Methodist Mansfield Medical Center"),
    ("Methodist Health System TX", "Richardson", "MRMC - Methodist Richardson Medical Center - MRMC",
     "Methodist Richardson Medical Center"),
    ("Methodist Health System TX", "Dallas", "MCMC - Physician Offices 2 - MMG", "office"),
    ("Methodist Health System TX", "Dallas", "MCMC - Methodist Southwest Medical Center - MCMC", "unknown"),
    ("Baystate Health", "Greenfield", "BFMC Main Campus", "Baystate Franklin Medical Center"),
    ("Baystate Health", "Westfield", "BNH Main Campus", "Baystate Noble Hospital"),
    ("Baystate Health", "Springfield", "BHH Main Campus", "office"),
    ("Cook Children's", "Fort Worth", "Medical Center - Fort Worth", "Cook Children's Medical Center"),
    ("Cook Children's", "Prosper", "Prosper Medical Office Building", "office"),
    ("Essentia Health", "Brainerd", "Brainerd Clinic", "office"),
    ("Essentia Health", "Duluth", "Support Services Building", "office"),
    ("Essentia Health", "Fargo", "32nd Avenue Building", "office"),
    ("OU Health", "Edmond", "Medical Office Building - Edmond", "office"),
    ("OU Health", "Oklahoma City", "Children's Physician Building", "office"),
    ("Prisma Health", "Seneca", "Oconee Medical Campus", "Prisma Health Oconee Memorial Hospital"),
    ("Prisma Health", "Columbia", "1301 Taylor St Baptist", "office"),
    ("Prisma Health", "Sumter", "115 N Sumter St Sumter", "office"),
    ("Prisma Health", "Greenville", "Patewood Memorial Hospital", "Prisma Health Patewood Memorial Hospital"),
    ("Prisma Health", "Greenville", "Patewood Outpt Ctr/Med Offices", "office"),
    ("Avera Health", "Aberdeen", "Avera St Luke's Hospital", "Avera St. Luke's Hospital"),
    ("Avera Health", "Yankton", "Avera Majestic Bluffs/Avera Sister James Care Center", "office"),
    ("Avera Health", "Sioux Falls", "Avera Health", "office"),
    ("Avera Health", "Sioux Falls", "Avera McKennan Hospital", "Avera McKennan Hospital & University Health Center"),
    ("Avera Health", "Sioux Center", "Sioux Center Health", "Sioux Center Health"),
    ("Adventist HealthCare (MD)", "Rockville", "AHC Sports Performance Institute", "office"),
    ("Adventist HealthCare (MD)", "Silver Spring", "Capital Choice Pathology Lab", "office"),
    ("Adventist HealthCare (MD)", "Silver Spring", "AHC - Home Health Silver Spring", "office"),
    ("Adventist HealthCare (MD)", "Rockville", "Adventist Rehabilitation - Rockville", "Adventist HealthCare Rehabilitation"),
    ("Adventist HealthCare (MD)", "Fort Washington", "Fort Washington Medical Center",
     "Adventist HealthCare Fort Washington Medical Center"),
    ("ProMedica", "Monroe", "Monroe Regional Hospital", "ProMedica Monroe Regional Hospital"),
    ("ProMedica", "Toledo", "Toledo Hospital", "ProMedica Toledo Hospital"),
    ("ProMedica", "Adrian", "Hickman Hospital", "ProMedica Charles and Virginia Hickman Hospital"),
    ("UofL Health", "Shepherdsville", "South Hospital - UMC", "UofL Health - South Hospital"),
    ("UofL Health", "Louisville", "JHHS - Jewish Hospital - UMC", "UofL Health - Jewish Hospital"),
    ("MUSC Health", "Charleston", "UNIVERSITY HOSPITAL EXTENSION - 165 ASHLEY AVE", "MUSC Health University Medical Center"),
    ("MUSC Health", "Charleston", "MUSC HEALTH WEST", "office"),
    ("MUSC Health", "Charleston", "MUSC Health Black River Medical Center", "MUSC Health Black River Medical Center"),
    ("Sentara Healthcare", "Harrisonburg", "Sentara RMH Medical Center", "Sentara RMH Medical Center"),
    ("Sentara Healthcare", "Williamsburg", "SMG Family Internal Medicine Williamsburg", "office"),
    ("John Muir Health", "Concord", "2540 East Street - Concord Medical Center (CMC)", "John Muir Medical Center - Concord"),
    ("John Muir Health", "Brentwood", "2400 Balfour Road - Brentwood", "office"),
    ("John Muir Health", "San Ramon", "12677 Alcosta Blvd, San Ramon, CA 94583", "office"),
]


@pytest.mark.parametrize("system,town,jrl,want", DETAIL)
def test_detail_work_site(system, town, jrl, want):
    verdict, entry = L.detail_site(system, jrl)
    if want in ("office", "unknown"):
        assert (verdict, entry) == (want, None)
    else:
        assert verdict == "site" and entry[0] == want


def _job(system, name, city, state, jid="1", desc=""):
    return types.SimpleNamespace(hospital_system=system, hospital_name=name, city=city, state=state,
                                 location=f"{city}, {state}", job_id=jid, description=desc, title="Registered Nurse",
                                 url="")


def _info(jrl):
    return {"jobRequisitionLocation": {"descriptor": jrl}}


def test_a_hospital_work_site_at_the_stamped_default_takes_the_hospitals_own_town():
    # the scan's live reads: Sentara 28944585 and MUSC 35281463 sat at the
    # tenant default (Norfolk, Charleston); Prisma 14748814 at Greenville
    j = _job("Sentara Healthcare", "Sentara Healthcare", "Norfolk", "VA")
    assert L.apply_detail(j, _info("Sentara Northern Virginia Medical Center")) == "site"
    assert (j.hospital_name, j.city, j.state, j.location) == (
        "Sentara Northern Virginia Medical Center", "Woodbridge", "VA", "Woodbridge, VA")
    j = _job("MUSC Health", "MUSC Health", "Charleston", "SC")
    L.apply_detail(j, _info("MUSC Health Black River Medical Center"))
    assert (j.hospital_name, j.city) == ("MUSC Health Black River Medical Center", "Cades")
    j = _job("Prisma Health", "Prisma Health (Providers)", "Greenville", "SC")
    L.apply_detail(j, _info("Blount Memorial Hospital"))
    assert (j.hospital_name, j.city, j.state) == ("Blount Memorial Hospital", "Maryville", "TN")


def test_a_hospital_work_site_in_another_real_town_moves_nothing():
    j = _job("ProMedica", "ProMedica", "Sylvania", "OH")
    assert L.apply_detail(j, _info("Monroe Regional Hospital")) == "elsewhere"
    assert (j.hospital_name, j.city) == ("ProMedica", "Sylvania")


def test_an_office_work_site_keeps_the_label_and_the_town_pass_leaves_it():
    j = _job("Essentia Health", "Essentia Health", "Brainerd", "MN", desc="x" * 1200)
    assert L.apply_detail(j, _info("Brainerd Clinic")) == "office"
    assert getattr(j, L.SKIP_ATTR) is True
    assert T.apply_job(j) == "detail-office"
    assert j.hospital_name == "Essentia Health"
    # without the detail verdict the town rule would have named the hospital
    k = _job("Essentia Health", "Essentia Health", "Brainerd", "MN", desc="x" * 1200)
    assert T.apply_job(k) == "town" and k.hospital_name == "Essentia Health St. Joseph's Medical Center"


def test_a_named_row_and_other_systems_are_left_alone():
    j = _job("Sentara Healthcare", "Sentara Leigh Hospital", "Norfolk", "VA")
    assert L.apply_detail(j, _info("Sentara Norfolk General Hospital")) == "named-already"
    assert j.hospital_name == "Sentara Leigh Hospital"
    other = _job("Advocate Health", "Advocate Health", "Charlotte", "NC")
    assert L.apply_detail(other, _info("Sentara Norfolk General Hospital")) == ""
    assert other.hospital_name == "Advocate Health"
    assert L.apply_detail(_job("MUSC Health", "MUSC Health", "Charleston", "SC"), None) == "none"
    assert L.apply_detail(_job("MUSC Health", "MUSC Health", "Charleston", "SC"), {"jobRequisitionLocation": "x"}) \
        in ("unknown", "office")


def test_the_detail_pass_calls_the_hook():
    j = scraper.Job(title="RN", hospital_name="Methodist Health System TX", hospital_system="Methodist Health System TX",
                    location="Celina, TX", city="Celina", state="TX", url="https://x", job_id="JR1",
                    ats_platform="Workday", specialty="", job_type="", posted_date="", description="")
    scraper._apply_wd_posting_info(j, {"jobDescription": "", "jobRequisitionLocation":
                                       {"descriptor": "MCEL - Methodist Celina Medical Center - MCEL"}})
    assert j.hospital_name == "Methodist Celina Medical Center"


@pytest.mark.parametrize("row", ROWS, ids=[str(r["id"]) for r in ROWS])
def test_stored_rows(row):
    got = T.decide(row["hospital_system"], row["hospital_name"], row["city"], row["state"], row["title"],
                   row["description"], row["url"])
    assert list(got) == row["expect"]


def test_stored_row_decisions_cover_every_behaviour():
    hows = {r["expect"][3] for r in ROWS}
    assert {"town", "named", "entity-office", "shared", "care-site", "offsite"} <= hows


def test_musc_employer_line_keeps_the_university_and_practices():
    body = "Job Description Summary\nx\n\nEntity\nMedical University of South Carolina (MUSC - Univ)\n\nWorker Type\nEmployee"
    assert T.decide("MUSC Health", "MUSC Health", "Charleston", "SC", "Project Manager", body, "")[3] == "entity-office"
    body = body.replace("Medical University of South Carolina (MUSC - Univ)", "Medical University Hospital Authority (MUHA)")
    assert T.decide("MUSC Health", "MUSC Health", "Florence", "SC", "RN - Ortho Unit", body, "")[:3] == (
        "MUSC Health Florence Medical Center", "Florence", "SC")


def test_shared_and_default_towns():
    # Columbia SC holds two MUSC hospitals; Charleston is the stamped default
    # and is placed only when the link names it
    assert T.decide("MUSC Health", "MUSC Health", "Columbia", "SC", "RN", "", "")[3] == "shared"
    assert T.decide("MUSC Health", "MUSC Health", "Charleston", "SC", "RN", "", "")[3] == "default"
    url = "https://musc.wd1.myworkdayjobs.com/MUSC/job/Charleston/RN_R-1"
    assert T.decide("MUSC Health", "MUSC Health", "Charleston", "SC", "RN", "", url)[0] == \
        "MUSC Health University Medical Center"
    # Prisma Greer Memorial is in Greer: a Spartanburg row is not put on it
    assert T.decide("Prisma Health", "Prisma Health", "Spartanburg", "SC", "RN", "", "")[3] == "no-town"
    assert T.decide("Prisma Health", "Prisma Health (Providers)", "Greer", "SC", "Hospitalist", "", "")[0] == \
        "Prisma Health Greer Memorial Hospital"


def test_tables_are_registered_under_the_stored_system_names():
    for system in L.SYSTEMS:
        assert T.SPECS[system].system == system
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(system, system) == system
        assert system in scraper.WORKDAY_TENANTS
        assert L.covers(system) and system in L.DETAIL
    assert L.covers("Prisma Health (Providers)")
    assert not L.covers("Advocate Health") and not L.covers("UPMC")
    for system, tbl in L.DETAIL.items():
        names = T.SPECS[system].table_names
        for v in tbl.values():
            assert v is None or v[0] in names


def _slot_ids(system, day, k=30):
    n = L.DETAIL_REREAD_CYCLE
    ids = [str(i) for i in range(5000, 5600)]
    on = [i for i in ids if zlib.crc32(f"wdsite|{system}|{i}".encode()) % n == day.toordinal() % n]
    off = [i for i in ids if i not in on]
    return on[:k], off[:k]


def test_reread_slot_reads_every_stored_row_once_a_cycle():
    day = date(2026, 11, 2)
    on, off = _slot_ids("Avera Health", day)
    assert not L.reread_slot("Avera Health", on[0], today=day)        # nothing loaded: nothing re-read
    T.set_stored([{"hospital_system": "Avera Health", "job_id": j, "hospital_name": "Avera Health",
                   "city": "Sioux Falls", "state": "SD"} for j in on + off]
                 + [{"hospital_system": "UPMC", "job_id": on[0], "hospital_name": "UPMC", "city": "", "state": ""}])
    assert all(L.reread_slot("Avera Health", j, today=day) for j in on)
    assert not any(L.reread_slot("Avera Health", j, today=day) for j in off)
    assert not L.reread_slot("Avera Health", "999999", today=day)     # not a stored active row
    assert not L.reread_slot("UPMC", on[0], today=day)                # not one of these systems
    assert not L.reread_slot("Avera Health", on[0], today=date(2027, 1, 2))   # past DETAIL_REREAD_UNTIL


def test_known_kind_queues_the_reread(monkeypatch):
    day = date(2026, 11, 3)
    monkeypatch.setattr(scraper, "_RUN_DAY", [day.toordinal()])
    on, off = _slot_ids("Baystate Health", day, k=5)
    T.set_stored([{"hospital_system": "Baystate Health", "job_id": j, "hospital_name": "Baystate Health",
                   "city": "Springfield", "state": "MA"} for j in on + off])
    scraper.set_known_bodies([{"hospital_system": "Baystate Health", "job_id": j, "desc_len": 3000,
                               "fv": scraper.FACTS_VERSION, "ats_platform": "Workday"} for j in on + off])
    try:
        mk = lambda j: scraper.Job(title="RN", hospital_name="Baystate Health", hospital_system="Baystate Health",
                                   location="Springfield, MA", city="Springfield", state="MA", url="https://x",
                                   job_id=j, ats_platform="Workday", specialty="", job_type="", posted_date="",
                                   description="")
        kinds_on = {scraper._known_kind("Baystate Health", mk(j)) for j in on}
        assert kinds_on <= {"facility", "force", "cut"} and "facility" in kinds_on
        assert "facility" not in {scraper._known_kind("Baystate Health", mk(j)) for j in off}
        budget = scraper._DescBudget(1000)
        held = []
        cands, _known, _dup = scraper._detail_candidates("Baystate Health", [mk(j) for j in on + off], budget,
                                                          held=held)
        assert {j.job_id for j in cands} >= {j for j in on if scraper._known_kind("Baystate Health", mk(j)) == "facility"}
        assert {k for _, _, k in held} <= {"facility", "force", "refresh", "stale", "cut", "pay", "req"}
    finally:
        scraper.set_known_bodies([])


# ── Trinity Health: named sites that are no hospital (rule 2) ───────────────
WD = "https://trinityhealth.wd1.myworkdayjobs.com/Jobs/job/{}/Some-Title_00701234"


@pytest.mark.parametrize("seg,want", [
    ("TSLC---Saint-Mary-Home", ("Saint Mary Home", "West Hartford", "CT")),
    ("LIFE-St-Joseph-of-the-Pines", ("LIFE St. Joseph of the Pines", "Southern Pines", "NC")),
    ("Pittsburgh-Mercy---Greentree-Community-Living-Arrangements", ("Pittsburgh Mercy - Green Tree", "Green Tree", "PA")),
    ("Pittsburgh-Mercy---South-9th-Street-Campus", ("Pittsburgh Mercy", "Pittsburgh", "PA")),
    ("Community-Hospice-Albany----Albany-New-York", ("Community Hospice - Albany", "Albany", "NY")),
    ("Eddy-Visiting-Nurse-and-Rehab-Association---Troy-New-York",
     ("Eddy Visiting Nurse and Rehab Association", "Troy", "NY")),
    ("Loyola-Center-for-Health-at-Bur-Ridge", ("Loyola Center for Health at Burr Ridge", "Burr Ridge", "IL")),
    ("The-Glen-at-Hiland-Meadows--Terrace-at-The-Glen---Queensbury-New-York",
     ("Glen at Hiland Meadows", "Queensbury", "NY")),
])
def test_trinity_named_sites(seg, want):
    import facilities_workday as W
    assert W.facility("Trinity Health", "", WD.format(seg)) == want
    assert want[0] in L.TRINITY_SITES


def test_trinity_hospitals_town_sites_and_offices_unchanged():
    import facilities_workday as W
    assert W.facility("Trinity Health", "", WD.format("St-Peters-Hospital---Albany-New-York"))[0] ==         "St. Peter's Hospital"
    assert W.facility("Trinity Health", "", WD.format("Langhorne")) is None
    assert W.facility("Trinity Health", "", WD.format("Pittsburgh-Mercy---Frech-Center")) is None
    assert W.facility("Trinity Health", "", WD.format("Medical-Offices-Plymouth-Medical-Center")) ==         ("Trinity Health", "Plymouth", "IN")           # an office: the system name with its town, as phase 2
    with pytest.raises(ValueError):
        L._named_sites("Trinity Health", "St-Peters-Hospital---Albany-New-York | Some Clinic | Albany | NY | site")
