"""2026-10-29 (facility scale phase 3, ambulatory group): the site a posting
of USPI, Concentra or CVS Health works at (facilities_ambulatory and its
hooks in scraper.py).

Fixtures (tests/fixtures/facility_ambulatory_1029/), all real:
  uspi_page_span.html     a USPI job page's header (job-companylocation
                          "Facility" field) and JSON-LD, read 2026-10-29
  uspi_page_facet.html    the other USPI page template: no Facility field,
                          the value in <html data-facet-id>
  uspi_search_cards.html  three USPI search cards (no facility on a card)
  concentra_results.json  Concentra SXA job-search results, verbatim
  stored_rows.json        stored CVS Health and USPI rows (bodies cut)
No network, no database."""
import asyncio
import json
import os
import re
import zlib

import pytest

import facilities_ambulatory as A
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_ambulatory_1029")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


STORED = json.loads(_read("stored_rows.json"))
CVS_ROWS = {r["id"]: r for r in STORED["cvs"]}
USPI_ROWS = {r["id"]: r for r in STORED["uspi"]}
USPI = A.SYSTEM_USPI
CVS = A.SYSTEM_CVS


def _job(system, name=None, city="", state="", description="", job_id="1", title="Registered Nurse",
         url="https://example.org/job/1", ats="TalentBrew"):
    return Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
               location=", ".join(p for p in (city, state) if p), specialty="", job_type="", url=url,
               job_id=str(job_id), posted_date="", description=description, ats_platform=ats)


def _row_job(r, description=None):
    return _job(r["hospital_system"], r["hospital_name"], r["city"] or "", r["state"] or "",
                r["description"] if description is None else description, r["job_id"], r["title"], r["url"],
                "Workday" if r["hospital_system"] == CVS else "TalentBrew")


@pytest.fixture(autouse=True)
def _clean_maps():
    A.set_known_facilities([])
    scraper.set_stored_companies([])
    yield
    A.set_known_facilities([])
    scraper.set_stored_companies([])


# ══ USPI ════════════════════════════════════════════════════════════════════
def test_the_facility_field_names_the_site():
    assert A.tb_page_facility(USPI, _read("uspi_page_span.html")) == ("Winter Haven Ambulatory Surgical Center", True)


def test_the_other_template_names_it_in_the_facet_attribute():
    assert A.tb_page_facility(USPI, _read("uspi_page_facet.html")) == ("Arizona Specialty Hospital", True)


def test_cards_and_other_systems_name_nothing():
    assert A.tb_page_facility(USPI, _read("uspi_search_cards.html")) == ("", False)
    assert A.tb_page_facility(USPI, "") == ("", False)
    assert A.tb_page_facility("CommonSpirit Health", _read("uspi_page_span.html")) == ("", False)
    assert "job-companylocation" not in _read("uspi_search_cards.html")


def test_facility_values_are_cleaned_and_offices_keep_the_system_name():
    assert A.uspi_code("Winter Haven Ambulatory Surgical Center (10622)") == "10622"
    assert A.uspi_code("Blue Ash Endoscopy Center, LLC (13033-01)") == "13033-01"
    assert A.uspi_code("Gulf Coast Endoscopy Center of Venice, LLC (13030 )") == "13030"
    # table names (by the facility number)
    assert A.uspi_clean("Memorial Hermann Surger Center, Greater Heights (11004)") == \
        "Memorial Hermann Surgery Center, Greater Heights"
    assert A.uspi_clean("Medical Park Tower Surgery Ctr (11802)") == "Medical Park Tower Surgery Center"
    assert A.uspi_clean("Treasure Coast Center for Surgery (TCCFS) (10634)") == "Treasure Coast Center for Surgery"
    assert A.uspi_clean("Texas Surgical Center, an affiliate of Midland Memorial Hospital (13000)") == \
        "Texas Surgical Center"
    assert A.uspi_clean("MODESTO RADIOLOGY IMAGING (07996)") == "Modesto Radiology Imaging"
    # a number the table does not know is cleaned the same way
    assert A.uspi_clean("Lakeside Surgery Ctr, LLC (99991)") == "Lakeside Surgery Center"
    assert A.uspi_clean("THE NEW SURGERY CENTER OF TAMPA (99992)") == "New Surgery Center of Tampa"
    assert A.uspi_clean("Foothill Outpatient SC (99994)") == "Foothill Outpatient Surgery Center"
    # shorthand values carry the public name the postings use
    assert A.uspi_clean("Briarcliff Ambulatory SC (10302)") == "Briarcliff Ambulatory Surgery Center"
    assert A.uspi_clean("Mills Park (10641)") == "AdventHealth Surgery Center Mills Park"
    assert A.uspi_clean("Presbyterian Rust Medical Center (11540)") == "Presbyterian Rust Surgery Center"
    for office in ("USPI Dallas Corporate Office (01007)", "USPI Houston Regional Office (09013)",
                   "Remote - Countrywide (RMT)", "Maryland Float Pool (13893-21)", "Covenant Annapolis MSO, LLC (15076)",
                   "HPI Corporate Office (12554)", "Dallas CSO - Hospital (01072)", "Some Regional Office (99993)", ""):
        assert A.uspi_clean(office) == "", office


def test_the_table_is_clean():
    assert len(A.USPI_TABLE) >= 400
    for code, name in A.USPI_TABLE.items():
        assert code == code.upper() and code.strip() == code
        if name is None:
            continue
        assert name == name.strip() and "  " not in name and "(" not in name, name
        assert not re.search(r"\bLLC\b|\bCtr\b", name) and not name.lower().startswith("the "), name
        assert re.search(r"[a-z]", name), name


def test_the_detail_pass_stores_the_site(monkeypatch):
    pages = {"span": _read("uspi_page_span.html"), "facet": _read("uspi_page_facet.html"),
             "office": _read("uspi_page_span.html").replace("Winter Haven Ambulatory Surgical Center (10622)",
                                                            "USPI Dallas Corporate Office (01007)")}

    async def fake_fetch(session, url, timeout=20):
        return pages[url.rsplit("/", 1)[-1]]

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    a = _job(USPI, url="https://careers.uspi.com/job/span")
    assert asyncio.run(scraper._jsonld_detail(None, a))
    assert a.hospital_name == "Winter Haven Ambulatory Surgical Center" and a.hospital_system == USPI
    assert "Winter Haven Ambulatory Surgery Center" in a.description and a.req_id == "300096825"
    b = _job(USPI, url="https://careers.uspi.com/job/facet")
    asyncio.run(scraper._jsonld_detail(None, b))
    assert b.hospital_name == "Arizona Specialty Hospital"
    c = _job(USPI, name="Winter Haven Ambulatory Surgical Center", url="https://careers.uspi.com/job/office")
    asyncio.run(scraper._jsonld_detail(None, c))
    assert c.hospital_name == USPI                       # the page says office: decision 2
    other = _job("MemorialCare", url="https://careers.uspi.com/job/span")
    asyncio.run(scraper._jsonld_detail(None, other))
    assert other.hospital_name == "MemorialCare"


def test_stored_sites_are_carried_and_label_rows_reread_once_a_cycle(monkeypatch):
    assert USPI in scraper.PAGE_FIELD_FACILITY and "Select Medical" in scraper.PAGE_FIELD_FACILITY
    day = scraper.datetime(2026, 11, 2).toordinal()
    monkeypatch.setattr(scraper, "_RUN_DAY", [day])
    n = scraper.ICIMS_FACILITY_REREAD_CYCLE
    on = [str(i) for i in range(5000, 5400) if zlib.crc32(f"company|{USPI}|{i}".encode()) % n == day % n][:3]
    off = [str(i) for i in range(5000, 5400) if zlib.crc32(f"company|{USPI}|{i}".encode()) % n != day % n][:3]
    scraper.set_stored_companies(
        [{"hospital_system": USPI, "job_id": on[0], "hospital_name": "Seaside Surgery Center"}]
        + [{"hospital_system": USPI, "job_id": j, "hospital_name": USPI} for j in on[1:] + off])
    kept = _job(USPI, job_id=on[0])
    scraper._keep_stored_company(kept)
    assert kept.hospital_name == "Seaside Surgery Center"
    label = _job(USPI, job_id=on[1])
    scraper._keep_stored_company(label)
    assert label.hospital_name == USPI
    assert scraper._company_reread_slot((USPI, on[1])) and not scraper._company_reread_slot((USPI, off[0]))
    assert not scraper._company_reread_slot((USPI, on[0]))          # site already stored
    monkeypatch.setattr(scraper, "_RUN_DAY", [scraper.datetime(2027, 1, 2).toordinal()])
    assert not scraper._company_reread_slot((USPI, on[1]))          # the re-read window is over


def test_the_stored_body_names_one_table_site():
    r = USPI_ROWS
    assert A.uspi_body_facility(r[38797436]["description"]) == "Winter Haven Ambulatory Surgical Center"
    assert A.uspi_body_facility(r[38797730]["description"]) == "Arizona Digestive Institute"
    assert A.uspi_body_facility(r[38797312]["description"]) == ""      # two table sites in the first lines
    assert A.uspi_body_facility(r[38797240]["description"]) == ""      # a corporate posting
    assert A.uspi_body_facility("") == ""
    keys = set(A._USPI_BODY_KEYS)
    assert A._fold("Advanced Surgical Care") not in keys and A._fold("Regional SurgiCenter") not in keys
    assert A._fold("Winter Haven Ambulatory Surgical Center") in keys


# ══ Concentra ═══════════════════════════════════════════════════════════════
CON = json.loads(_read("concentra_results.json"))["results"]


class _Resp:
    def __init__(self, data):
        self.status, self._data = 200, data

    async def json(self, content_type=None):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _concentra_jobs(monkeypatch, results):
    monkeypatch.setattr(scraper, "req", lambda *a, **k: _Resp({"Count": len(results), "Results": results}))

    async def no_wait():
        return None

    monkeypatch.setattr(scraper, "jitter", no_wait)
    return {j.url.rstrip("/").rsplit("/", 1)[-1]: j for j in asyncio.run(scraper.scrape_concentra(None))}


def test_the_board_map_point_names_the_centre(monkeypatch):
    jobs = _concentra_jobs(monkeypatch, CON)
    case = {re.search(r"/(\d+)/?$", r["Url"]).group(1): r["_case"] for r in CON}
    got = {case[jid]: (j.hospital_name, j.city, j.state) for jid, j in jobs.items()}
    assert got["centre, same town"] == ("Concentra Bellevue", "Bellevue", "WA")
    assert got["centre in the neighbouring town (Tanasbourne, Beaverton)"] == ("Concentra Tanasbourne", "Hillsboro", "OR")
    assert got["centre town spelled Ft. Lauderdale"][0] == "Concentra Cypress Creek"
    for c in ("two centres at one point", "onsite title", "central billing office at a centre point",
              "corporate department (Information Systems)", "no centre within 100 km", "telecommute"):
        assert got[c][0] == "Concentra", c
    assert all(j.hospital_system == "Concentra" for j in jobs.values())


def _centre(name):
    return next(c for c in A.CONCENTRA_CENTERS if c[0] == name)


def _offset(lat, lon, metres_north):
    return lat + metres_north / 111195.0, lon


def test_centre_distance_state_and_role_rules():
    name, city, st, lat, lon = _centre("Kansas Avenue")
    assert A.concentra_center(lat, lon, city, st, "Medical Assistant", "Medical-Assistant") == "Concentra Kansas Avenue"
    assert A.concentra_center(*_offset(lat, lon, 240), city, st) == "Concentra Kansas Avenue"
    assert A.concentra_center(*_offset(lat, lon, 260), city, st) == ""                  # over 250 m
    assert A.concentra_center(*_offset(lat, lon, 200), "Overland Park", st) == ""      # other town: 150 m
    assert A.concentra_center(lat, lon, city, "MO") == ""                               # another state
    for title in ("Onsite Athletic Trainer - Part Time", "Athletic Trainer", "EMT or Paramedic - PRN",
                  "Float Center Operation Manager", "Spine Telecommute Medical Review Stream Physician",
                  "Cash Posting Specialist- Hybrid", "Field Sales Account Executive"):
        assert A.concentra_center(lat, lon, city, st, title) == "", title
    for cat in ("Central-Billing-Office", "Medical Review UR", "Certified-Athletic-Trainer", "Information-Systems"):
        assert A.concentra_center(lat, lon, city, st, "Specialist", cat) == "", cat
    for bad in ((None, None), ("", ""), (0, 0), ("x", 1)):
        assert A.concentra_center(*bad, city, st) == ""


def test_the_centre_table_is_clean():
    assert len(A.CONCENTRA_CENTERS) >= 600
    seen = set()
    for name, city, st, lat, lon in A.CONCENTRA_CENTERS:
        assert name == name.strip() and name and city and st in A.US_STATES, (name, city, st)
        assert 17 < lat < 72 and -170 < lon < -64, (name, lat, lon)
        assert (name, city, st) not in seen
        seen.add((name, city, st))


# ══ CVS Health ══════════════════════════════════════════════════════════════
def test_the_posting_text_names_the_business():
    want = {38808883: ("Oak Street Health", ("Philadelphia", "PA")),
            38809273: ("Oak Street Health", None),
            38808388: ("Oak Street Health", ("Center Point", "AL")),
            38808542: ("Oak Street Health", None),
            38808820: ("", None),            # Location: Fully Remote
            38809902: ("", None),            # Signify Health in-home visits
            38809038: ("", None),            # Aetna field case manager
            38808310: ("", None),            # Coram home infusion nurse
            38808761: ("Omnicare", None)}
    for rid, (name, town) in want.items():
        r = CVS_ROWS[rid]
        assert A.cvs_site(r["title"], r["description"])[0] == name, rid
        assert A.cvs_address_town(r["description"]) == town, rid
    assert A.cvs_site(CVS_ROWS[38809902]["title"], CVS_ROWS[38809902]["description"])[1] == "home-based"
    assert A.cvs_site(CVS_ROWS[38808820]["title"], CVS_ROWS[38808820]["description"])[1] == "remote"
    mc = [r for r in CVS_ROWS.values() if r["case"].startswith("MinuteClinic")]
    got = {r["case"]: A.cvs_site(r["title"], r["description"]) for r in mc}
    assert got["MinuteClinic, clinic-based role that mentions virtual visits"] == ("MinuteClinic", "text")
    assert got["MinuteClinic Virtual Care NP (remote)"] == ("", "remote")
    assert got["MinuteClinic mental health therapist, 100% remote telehealth"] == ("", "remote")


def test_a_body_night_names_the_site_and_fills_a_missing_town():
    r = CVS_ROWS[38808883]
    job = _row_job(r)
    assert (job.city, job.state) == ("Roosevelt", "")
    assert A.apply_facility(job) == "text+town"
    assert (job.hospital_name, job.city, job.state, job.location) == \
        ("Oak Street Health", "Philadelphia", "PA", "Philadelphia, PA")
    assert job.hospital_system == CVS
    town_ok = _row_job(CVS_ROWS[38808542])                      # its own town stays
    before = (town_ok.city, town_ok.state)
    A.apply_facility(town_ok)
    assert town_ok.hospital_name == "Oak Street Health" and (town_ok.city, town_ok.state) == before
    home = _row_job(CVS_ROWS[38809902], )
    assert A.apply_facility(home) == "home-based" and home.hospital_name == CVS


def test_a_night_without_the_body_keeps_the_stored_site_and_town():
    A.set_known_facilities([
        {"hospital_system": CVS, "job_id": "R1", "hospital_name": "Oak Street Health", "city": "Philadelphia", "state": "PA"},
        {"hospital_system": CVS, "job_id": "R2", "hospital_name": CVS, "city": "Dallas", "state": "TX"},
        {"hospital_system": CVS, "job_id": "R3", "hospital_name": "Oak Street Health", "city": "", "state": ""},
        {"hospital_system": "Concentra", "job_id": "R1", "hospital_name": "X", "city": "Y", "state": "TX"}])
    a = _job(CVS, city="Roosevelt", job_id="R1", ats="Workday")
    assert A.apply_facility(a) == "kept+town"
    assert (a.hospital_name, a.city, a.state) == ("Oak Street Health", "Philadelphia", "PA")
    b = _job(CVS, city="Chicago", state="IL", job_id="R1", ats="Workday")
    A.apply_facility(b)
    assert (b.hospital_name, b.city, b.state) == ("Oak Street Health", "Chicago", "IL")   # the feed's town wins
    c = _job(CVS, city="Dallas", state="TX", job_id="R2", ats="Workday")
    assert A.apply_facility(c) == "none" and c.hospital_name == CVS
    d = _job(CVS, job_id="R3", ats="Workday")
    assert A.apply_facility(d) == "kept" and (d.city, d.state) == ("", "")
    e = _job("Concentra", job_id="R1")
    assert A.apply_facility(e) == "" and e.hospital_name == "Concentra"
    f = _job(CVS, name="Something Else", job_id="R1", ats="Workday")
    assert A.apply_facility(f) == "other" and f.hospital_name == "Something Else"


def test_finalize_names_the_cvs_site_and_a_failing_pass_costs_only_itself(monkeypatch):
    r = CVS_ROWS[38808883]
    rows = scraper.finalize_jobs([_row_job(r)])
    assert rows and rows[0]["hospital_name"] == "Oak Street Health" and rows[0]["hospital_system"] == CVS
    assert (rows[0]["city"], rows[0]["state"]) == ("Philadelphia", "PA")

    def boom(*a, **k):
        raise RuntimeError("unforeseen row")

    monkeypatch.setattr(A, "apply_facilities", boom)
    rows = scraper.finalize_jobs([_row_job(r)])
    assert [x["job_id"] for x in rows] == [r["job_id"]] and rows[0]["hospital_name"] == CVS


def test_loaders_without_credentials_keep_nothing(monkeypatch):
    for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert A.load_known_facilities() == 0 and A._KNOWN == {}


def test_only_the_three_chains_are_touched():
    assert A.SYSTEMS == {"CVS Health", "United Surgical Partners International", "Concentra"}
    assert A.TB_FACILITY_SYSTEMS == {USPI}
    assert USPI in scraper.TALENTBREW_ORGS and CVS in scraper.WORKDAY_TENANTS
    assert scraper.ICIMS_COMPANY_FACILITY == frozenset({"Select Medical"})
