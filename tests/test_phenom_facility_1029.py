"""2026-10-29: Phenom / TalentBrew rows name their hospital and its own town
(facilities_phenom; the owner: every hospital of a multi-hospital system named
and placed, applies retro-assigned).

Fixtures (tests/fixtures/phenomfac1029/), real data read 2026-10-29:
  list_docs.json     each board's /us/en/search-results page: the eager-loaded
                     refineSearch list (10 listings) and its facets, cut to
                     the fields the hook reads (Hartford businessUnit, UMMS
                     memberOrganization, Spartanburg FacilityName, ECU
                     facility, CentraCare / Children's Minnesota workLocation,
                     Broward and Franciscan address, CHOA and Franciscan
                     Workday links)
  stored_lines.json  read-only from the DATA project: the one line of a
                     stored body the resolver reads (Kaiser Department,
                     Hartford Location Detail, MU Facility, MemorialCare
                     Location; Duke and Children's Health lines, which the
                     text family's module reads) and CHOA / Franciscan stored
                     links, with each row's town
No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_phenom as P
import facility_names as F
import scraper
from conftest import FIXTURES

FIX = os.path.join(FIXTURES, "phenomfac1029")
with open(os.path.join(FIX, "list_docs.json"), encoding="utf-8") as _f:
    BOARDS = json.load(_f)["boards"]
with open(os.path.join(FIX, "stored_lines.json"), encoding="utf-8") as _f:
    STORED = json.load(_f)["rows"]
PAD = "\n\n" + "Provides patient care and support. " * 40


def _city_state(doc):
    c, s = scraper.parse_city_state(f"{doc.get('city', '')}, {doc.get('state', '')}", town_first=True)
    return c or doc.get("city", ""), s or doc.get("state", "")


def _hook(system, doc):
    city, state = _city_state(doc)
    return P.list_facility(system, doc, city, state, scraper._workday_job_url(doc.get("applyUrl") or ""))


def _job(system, name, *, city="", state="", url="", description="", job_id="J1", title="RN"):
    return scraper.Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
                       location=f"{city}, {state}" if city else state, specialty="", job_type="", url=url,
                       job_id=job_id, posted_date="", description=description, ats_platform="Phenom")


# ── the table ───────────────────────────────────────────────────────────────

def test_every_family_system_is_a_scraper_feed():
    feeds = set(scraper.PHENOM_ORGS) | set(scraper.TALENTBREW_ORGS) | {"Kaiser Permanente"}
    assert P.SYSTEMS <= feeds, P.SYSTEMS - feeds
    for fam in P.FAMILY:
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(fam.system, fam.system) == fam.db_label, fam.system
        # no system handled twice (facility_names keeps Ascension, BHSF, Corewell)
        assert fam.system not in F.FACILITY_SYSTEMS


def test_sites_are_clean_and_one_town_per_name():
    states = set(scraper._WD_STATE_CODES)
    for fam in P.FAMILY:
        towns = {}
        for site in fam._all_sites():
            name, town, st, source = site
            assert name == name.strip() and "  " not in name and name not in fam.labels, site
            assert st in states and town == town.strip() and town, site
            assert source.split()[0] in ("cms", "board", "address"), site
            assert "—" not in name
            towns.setdefault(name, set()).add((town, st))
        # the CRM reads a name at 3+ towns as a system label: one town per name
        assert all(len(v) == 1 for v in towns.values()), {k: v for k, v in towns.items() if len(v) > 1}


def test_no_other_systems_hospital_is_named():
    kp = P.fam_for("Kaiser Permanente")
    for raw in ("Virginia Hospital Center - UR-Discharge Planning - 1808", "Holy Cross Hospital - Labor - 1808",
                "Anne Arundel Medical Center - OR - 1808", "Dublin Hospital (Future) - Admin - 0201"):
        assert P.text_site(kp, f"Department: {raw}{PAD}") is None, raw
    choa = P.fam_for("Children's Healthcare ATL")
    assert P.segment_site(choa, "https://choa.wd12.myworkdayjobs.com/externalcareers/job/Emory---Midtown/RN_R-1") is None


# ── list fields, on the real search pages ──────────────────────────────────

def test_hartford_business_unit_names_the_hospital_in_its_town():
    b = BOARDS["Hartford HealthCare"]
    assert {"Hartford Hospital", "Windham Hospital", "Manchester Hospital Inc."} <= set(b["facets"]["businessUnit"])
    got = {(d["businessUnit"], _hook("Hartford HealthCare", d)) for d in b["jobs"]}
    assert ("Manchester Hospital Inc.", ("Manchester Memorial Hospital", "Manchester", "CT")) in got
    assert ("Institute of Living", ("Institute of Living", "Hartford", "CT")) in got
    assert ("St. Vincent's Medical Center", ("St. Vincent's Medical Center", "Bridgeport", "CT")) in got
    # the medical group is not a hospital: the adapter keeps the system label
    assert ("Hartford HealthCare Medical Group Specialists", None) in got


def test_hartford_hospital_outside_its_town_keeps_the_system():
    doc = {"businessUnit": "Hartford Hospital", "city": "Avon", "state": "Connecticut"}
    assert P.list_facility("Hartford HealthCare", doc, "Avon", "CT") == ("", "Avon", "CT")
    # Windham runs under Backus's license: a Backus posting in Willimantic is the Windham campus
    doc = {"businessUnit": "Backus Hospital"}
    assert P.list_facility("Hartford HealthCare", doc, "Willimantic", "CT") == ("Windham Hospital", "Willimantic", "CT")
    assert P.list_facility("Hartford HealthCare", doc, "Norwich", "CT") == ("Backus Hospital", "Norwich", "CT")


def test_every_hartford_business_unit_is_listed_or_the_system():
    fam = P.fam_for("Hartford HealthCare")
    for unit in BOARDS["Hartford HealthCare"]["facets"]["businessUnit"]:
        k = P.fkey(unit)
        is_hospital_word = any(w in unit for w in ("Hospital", "Medical Center", "Institute of Living", "Natchaug",
                                                   "Rockville", "Rushford"))
        if is_hospital_word:
            assert k in fam.units, unit


def test_umms_member_organization():
    b = BOARDS["University of Maryland Medical System"]
    got = [(_city_state(d), d.get("memberOrganization"), _hook("University of Maryland Medical System", d)) for d in b["jobs"]]
    assert any(h == ("University of Maryland Charles Regional Medical Center", "La Plata", "MD") for _, _, h in got)
    # the urgent care entity is not in the table: the adapter's chain keeps the system
    assert all(h is None for _, m, h in got if m == "University Of Maryland Urgent Care")
    fam = P.fam_for("University of Maryland Medical System")
    shore = {"memberOrganization": "University Of Maryland Shore Regional Health"}
    assert P.list_facility(fam.system, shore, "Chestertown", "MD")[0] == \
        "University of Maryland Shore Medical Center at Chestertown"
    assert P.list_facility(fam.system, shore, "Denton", "MD") == ("", "Denton", "MD")      # a clinic town


def test_spartanburg_facility_name_and_ecu_facility():
    got = {(d["FacilityName"], _city_state(d)[0]): _hook("Spartanburg Regional", d)
           for d in BOARDS["Spartanburg Regional"]["jobs"]}
    assert got[("Pelham Medical Center", "Greer")] == ("Pelham Medical Center", "Greer", "SC")
    assert got[("Pelham Medical Center", "Simpsonville")] == ("", "Simpsonville", "SC")
    assert got[("Medical Group of the Carolinas", "Greer")] is None
    ecu = {(d["facility"], _city_state(d)[0]): _hook("ECU Health", d) for d in BOARDS["ECU Health"]["jobs"]}
    assert ecu[("ECU Health Medical Center", "Greenville")] == ("ECU Health Medical Center", "Greenville", "NC")
    assert ecu[("ECU Health Phys Team Member", "Edenton")] == ("", "Edenton", "NC")
    fam = P.fam_for("ECU Health")
    for raw in BOARDS["ECU Health"]["facets"]["facility"]:
        if raw not in ("ECU Health", "ECU Health Roanoke Rapids Endo"):
            assert P.fkey(raw) in fam.units or P.fkey(raw) in fam.offices, raw


def test_worklocation_sites():
    cmn = {d["workLocation"]: _hook("Children's Minnesota", d) for d in BOARDS["Children's Minnesota"]["jobs"]}
    assert cmn["Children's - St. Paul"] == ("Children's Minnesota St. Paul Hospital", "St. Paul", "MN")
    assert cmn["Children's - Minneapolis"] == ("Children's Minnesota Minneapolis Hospital", "Minneapolis", "MN")
    assert cmn["Maple Grove Clinic"] is None
    cc = [(d.get("workLocation"), _hook("CentraCare", d)) for d in BOARDS["CentraCare"]["jobs"]]
    assert ("St Cloud Hospital", ("St. Cloud Hospital", "Saint Cloud", "MN")) in cc
    assert ("CentraCare - Monticello", ("CentraCare - Monticello", "Monticello", "MN")) in cc
    # a clinic's workLocation is not the hospital its entity runs
    assert P.list_facility("CentraCare", {"workLocation": "Northway Clinic", "companyName": "St. Cloud Hospital"},
                           "Saint Cloud", "MN") == ("", "Saint Cloud", "MN")
    assert P.list_facility("CentraCare", {"workLocation": "", "companyName": "St. Cloud Hospital"},
                           "Saint Cloud", "MN") == ("St. Cloud Hospital", "Saint Cloud", "MN")


def test_street_address_names_the_hospital():
    br = [(d.get("address"), _hook("Broward Health", d)) for d in BOARDS["Broward Health"]["jobs"]]
    assert ("1600 S Andrews Avenue", ("Broward Health Medical Center", "Fort Lauderdale", "FL")) in br
    assert ("201 E Sample Road", ("Broward Health North", "Deerfield Beach", "FL")) in br
    assert ("6401 N Federal Highway", ("Broward Health Imperial Point", "Fort Lauderdale", "FL")) in br
    assert all(h is None for a, h in br if not a)
    fh = [(d.get("address"), _hook("Franciscan Health", d)) for d in BOARDS["Franciscan Health"]["jobs"]]
    assert all(h == ("Franciscan Health Indianapolis Orthopedic Hospital", "Carmel", "IN")
               for a, h in fh if a.startswith("10777 Illinois"))
    assert all(h is None for a, h in fh if a.startswith("8345 Wicker"))       # a clinic
    assert P.street_key("Greenville, NC 27834") == ""


def test_choa_workday_link_names_the_site():
    got = {_hook("Children's Healthcare ATL", d) for d in BOARDS["Children's Healthcare ATL"]["jobs"]}
    assert ("Arthur M. Blank Hospital", "Atlanta", "GA") in got
    assert ("Children's Scottish Rite Hospital", "Atlanta", "GA") in got
    # the support center (offices) keeps the system label
    sc = [d for d in BOARDS["Children's Healthcare ATL"]["jobs"] if "Support-Center" in d["applyUrl"]][0]
    assert _hook("Children's Healthcare ATL", sc) is None


def test_other_phenom_tenants_are_untouched():
    doc = BOARDS["Hartford HealthCare"]["jobs"][0]
    assert P.list_facility("Duke Health", doc, "Durham", "NC") is None       # facilities_text.py
    assert P.fam_for("Duke Health") is None and P.fam_for("Children's Health") is None
    assert P.list_facility("DaVita", doc, "Meriden", "CT") is None
    assert P.list_facility("Baptist Health (FL)", {"brand": "Boca Raton Regional Hospital"}, "Boca Raton", "FL") is None


# ── stored lines (the cleanup and the nights a body is read) ───────────────

def _resolve(row, *, fallback=False):
    if P.fam_for(row["system"]) is None:
        return None
    body = (row.get("line", "") + PAD) if row.get("line") is not None else ""
    return P.resolve_row(row["system"], hospital_name=row["system"], url=row.get("url", ""), description=body,
                         city=row["city"], state=row["state"], job_id=row["job_id"], fallback=fallback)


@pytest.mark.parametrize("system,line,city,want", [
    ("Kaiser Permanente", "Department: Sunset Medical Center - Intensive Care Unit- 1 - 0801", "Los Angeles",
     "Kaiser Permanente Los Angeles Medical Center"),
    ("Kaiser Permanente", "Department: Oakland Hospital - HIV Clinic - 0206", "Oakland",
     "Kaiser Permanente Oakland Medical Center"),
    ("Kaiser Permanente", "Department: Walnut Creek Hospital - Anesthesiology-Nurse Anesthes - 0206", "Antioch", None),
    ("Kaiser Permanente", "Department: Maui Lani Medical Offices - After Hours Clinic - 1208", "Wailuku", None),
    ("Hartford HealthCare", "Location Detail: Windham Community Memorial (10183)", "Willimantic", "Windham Hospital"),
    ("Hartford HealthCare", "Location Detail: 79 Jefferson St HH Bliss Wing (10064)", "Hartford", "Hartford Hospital"),
    ("Hartford HealthCare", "Location Detail: 15 Corporate Drive Trumbull (10530)", "Trumbull", None),
    ("Hartford HealthCare", "Location Detail: 85 Seymour Street - HHMOB (10117)", "Hartford", None),
    ("MU Health Care", "Facility : Capital Region Medical Center - 1125 Madison St, Jefferson City, MO",
     "Jefferson City", "Capital Region Medical Center"),
    ("MU Health Care", "Facility : Children’s Hospital – 1021 Hitt St, Columbia, MO", "Columbia",
     "MU Health Care Children's Hospital"),
    ("MU Health Care", "Facility : Madison Street Medical Building - 1125 Madison Street, Jefferson City, MO",
     "Jefferson City", None),
    ("MU Health Care", "Facility : University Hospital – 1 Hospital Drive, Columbia, MO & Capital Region",
     "Columbia", None),
    ("MemorialCare", "Location: Miller Children’s Hospital @ Long Beach Memorial Medical Center", "Long Beach",
     "Miller Children's & Women's Hospital Long Beach"),
    ("MemorialCare", "Location: Saddleback Memorial Medical Center", "Laguna Hills",
     "MemorialCare Saddleback Medical Center"),
    ("MemorialCare", "Location: Long Beach, CA", "Long Beach", None),
])
def test_stored_lines(system, line, city, want):
    row = [r for r in STORED if r["system"] == system and r.get("line") == line and r["city"] == city]
    row = row[0] if row else {"system": system, "line": line, "city": city, "state": "", "job_id": "X"}
    got = _resolve(row)
    assert (got[0] if got else None) == want, (line, got)
    if got:
        assert got[3] == "text"


def test_every_stored_fixture_row_resolves_without_error():
    named = 0
    for row in STORED:
        got = _resolve(row)
        if got:
            named += 1
            assert P.fkey(got[1]) == P.fkey(row["city"]), (row, got)     # never another town
        if row["system"] in ("Duke Health", "Children's Health"):
            assert got is None                                         # facilities_text.py reads these
    assert named >= 30


def test_choa_and_franciscan_stored_links():
    got = {r["url"].split("/job/")[1].split("/")[0]: _resolve(r) for r in STORED
           if r["system"] in ("Children's Healthcare ATL", "Franciscan Health")}
    assert got["Childrens-Hughes-Spalding-Hospital"][:3] == ("Children's Hughes Spalding Hospital", "Atlanta", "GA")
    assert got["Childrens-at-Hudson-Bridge"][:3] == ("Children's at Hudson Bridge", "Stockbridge", "GA")
    assert got["Childrens-Healthcare-of-Atlanta-Support-Center"] is None
    assert got["Scottish-Rite-Medical-Office-Building"] is None
    assert got["IN---Indianapolis"] is None           # a town alone names no hospital
    fh = [v for k, v in got.items() if k.startswith("Franciscan-Health-Indianapolis-Orthopedic")]
    assert fh and fh[0][:3] == ("Franciscan Health Indianapolis Orthopedic Hospital", "Carmel", "IN")


def test_default_town_takes_the_sites_town():
    got = P.resolve_row("Kaiser Permanente", hospital_name="Kaiser Permanente", city="Oakland", state="CA",
                        description="Department: Sunnyside Medical Center - Emergency - 1001" + PAD)
    assert got == ("Kaiser Permanente Sunnyside Medical Center", "Clackamas", "OR", "text")
    got = P.resolve_row("Kaiser Permanente", hospital_name="Kaiser Permanente", city="", state="",
                        description="Department: ZION MEDICAL CENTER - Med/Surg - UNIT 9 - 0801" + PAD)
    assert got == ("Kaiser Permanente Zion Medical Center", "San Diego", "CA", "text")


def test_raw_spellings_are_cleaned_in_their_town():
    r = P.resolve_row("ECU Health", hospital_name="ECUH BEA Campus of ECUH MC", city="Washington", state="NC")
    assert r == ("ECU Health Beaufort Hospital", "Washington", "NC", "list")
    r = P.resolve_row("ECU Health", hospital_name="ECU Health Phys Team Member", city="Kinston", state="NC")
    assert r == ("ECU Health", "Kinston", "NC", "list")
    r = P.resolve_row("LCMC Health", hospital_name="West Jefferson", city="Marrero", state="LA")
    assert r == ("West Jefferson Medical Center", "Marrero", "LA", "list")
    r = P.resolve_row("LCMC Health", hospital_name="University Medical Center", city="Metairie", state="LA")
    assert r == ("LCMC Health", "Metairie", "LA", "list")
    r = P.resolve_row("LCMC Health", hospital_name="LCMC", city="New Orleans", state="LA")
    assert r == ("LCMC Health", "New Orleans", "LA", "list")
    assert P.resolve_row("ECU Health", hospital_name="ECU Health Medical Center", city="Greenville", state="NC")[:3] == \
        ("ECU Health Medical Center", "Greenville", "NC")


def test_stored_name_is_kept_on_nights_without_a_body():
    P.set_stored_names([{"hospital_system": "Kaiser Permanente", "job_id": "K1",
                         "hospital_name": "Kaiser Permanente Fontana Medical Center"},
                        {"hospital_system": "Kaiser Permanente", "job_id": "K2", "hospital_name": "Kaiser Permanente"},
                        {"hospital_system": "MemorialCare", "job_id": "D1", "hospital_name": "Some Clinic"}])
    try:
        assert P.resolve_row("Kaiser Permanente", hospital_name="Kaiser Permanente", city="Fontana", state="CA",
                             description="teaser", job_id="K1") == \
            ("Kaiser Permanente Fontana Medical Center", "Fontana", "CA", "kept")
        assert P.resolve_row("Kaiser Permanente", hospital_name="Kaiser Permanente", city="Fontana", state="CA",
                             description="", job_id="K2") is None
        # a body read tonight that names no site wins over the stored name
        assert P.resolve_row("Kaiser Permanente", hospital_name="Kaiser Permanente", city="Fontana", state="CA",
                             description="Department: Fontana MOB 2 - Pediatrics - 0801" + PAD, job_id="K1") is None
        assert P.stored_name(P.fam_for("MemorialCare"), "D1") == ""          # not one of the table's sites
    finally:
        P.set_stored_names([])


def test_load_without_credentials_is_empty(monkeypatch):
    for k in ("SUPABASE_URL", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert P.load_stored_names() == 0


# ── through the adapter and finalize_jobs ──────────────────────────────────

class _R:
    def __init__(self, status, body):
        self.status = status
        self._body = body
        self.headers = {"content-type": "application/json" if not isinstance(body, str) else "text/html"}

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _run_phenom(monkeypatch, system):
    b = BOARDS[system]
    base = b["base"]
    html = ('<script>phApp.ddo = {}; phApp.pageInfo = {"refNum":"%s","siteType":"external","pageId":"%s"};</script>'
            % (b["refNum"], b["pageId"]))
    jobs = [dict(d) for d in b["jobs"]]

    def fake_req(session, method, url, **kw):
        body = kw.get("json") or {}
        if method == "post" and url == f"{base}/widgets" and body.get("ddoKey") == "refineSearch":
            return _Ctx(_R(200, {"refineSearch": {"status": 200, "hits": len(jobs), "totalHits": len(jobs),
                                                  "data": {"jobs": jobs}}}))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, html))
        return _Ctx(_R(404, ""))

    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    return asyncio.run(scraper.scrape_phenom(None, system, base))


def test_scrape_phenom_hartford(monkeypatch):
    rows = _run_phenom(monkeypatch, "Hartford HealthCare")
    names = {(j.hospital_name, j.city) for j in rows}
    assert ("Manchester Memorial Hospital", "Manchester") in names
    assert ("Institute of Living", "Hartford") in names
    assert ("Hartford HealthCare", "West Hartford") in names
    assert all(j.hospital_system == "Hartford HealthCare" for j in rows)


def test_scrape_phenom_ecu_keeps_unlisted_values_and_cleans_listed_ones(monkeypatch):
    rows = _run_phenom(monkeypatch, "ECU Health")
    names = {(j.hospital_name, j.city) for j in rows}
    assert ("ECU Health Medical Center", "Greenville") in names
    assert ("ECU Health", "Edenton") in names          # "ECU Health Phys Team Member": the system
    assert not any("Phys Team Member" in j.hospital_name for j in rows)


def test_scrape_phenom_choa_and_broward(monkeypatch):
    rows = _run_phenom(monkeypatch, "Children's Healthcare ATL")
    assert {"Arthur M. Blank Hospital", "Children's Scottish Rite Hospital", "Children's Healthcare ATL"} <= \
        {j.hospital_name for j in rows}
    rows = _run_phenom(monkeypatch, "Broward Health")
    assert {"Broward Health Medical Center", "Broward Health North", "Broward Health Imperial Point",
            "Broward Health"} == {j.hospital_name for j in rows}


def test_finalize_jobs_names_text_and_link_rows(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    P.set_stored_names([])
    monkeypatch.setattr(P, "load_stored_names", lambda: 0)
    rows = scraper.finalize_jobs([
        _job("Kaiser Permanente", "Kaiser Permanente", city="Clackamas", state="OR", job_id="K9",
             description="Department: Sunnyside Medical Center - Emergency - 1001" + PAD),
        _job("Children's Healthcare ATL", "Children's Healthcare ATL", city="Atlanta", state="GA", job_id="C9",
             url="https://choa.wd12.myworkdayjobs.com/externalcareers/job/Childrens-Hughes-Spalding-Hospital/RN_R-38546"),
        _job("MU Health Care", "University of Missouri Health Care", city="Jefferson City", state="MO", job_id="M9",
             description="Facility : Capital Region Medical Center - 1125 Madison St, Jefferson City, MO" + PAD),
        _job("PIH Health", "PIH Health", city="Whittier", state="CA", job_id="P9", description="Location: Whittier" + PAD),
    ])
    by = {r["job_id"]: r for r in rows}
    assert by["K9"]["hospital_name"] == "Kaiser Permanente Sunnyside Medical Center"
    assert by["K9"]["hospital_system"] == "Kaiser Permanente"
    assert by["C9"]["hospital_name"] == "Children's Hughes Spalding Hospital"
    assert by["M9"]["hospital_name"] == "Capital Region Medical Center"
    # 2026-10-29 (integration): the Phenom hook names nothing from a town alone;
    # the town family runs last in finalize_jobs (facilities_town) and places a
    # label row at the system's one hospital in its town.
    p9 = _job("PIH Health", "PIH Health", city="Whittier", state="CA", job_id="P9", description="Location: Whittier" + PAD)
    P.apply_facility(p9)
    assert p9.hospital_name == "PIH Health"
    assert by["P9"]["hospital_name"] == "PIH Health Whittier Hospital"


def test_apply_facility_leaves_other_systems_alone():
    job = _job("Advocate Health", "Advocate Health", city="Charlotte", state="NC",
               description="Department: Sunnyside Medical Center - Emergency - 1001" + PAD)
    assert P.apply_facility(job) == "" and job.hospital_name == "Advocate Health"


# ── Ascension: the stored Facility lines the table lacked ──────────────────

@pytest.mark.parametrize("raw,city,state,want", [
    ("Ascension St Vincent Plainfield Micro Hospital", "Plainfield", "IN", "Ascension St. Vincent Plainfield"),
    ("Saint Thomas Heart West", "Nashville", "TN", "Ascension Saint Thomas West"),
    ("Ascension Stress Center", "Indianapolis", "IN", "Ascension St. Vincent Stress Center"),
    # the review's warning: a Facility line can name another system's hospital; no fuzzy match
    ("Waterbruy Hospital", "Waterbury", "CT", None),
    ("Ascension Medical Group", "Tulsa", "OK", None),
    ("Ascension Saint Thomas Midtown, West and Rutherford", "Nashville", "TN", None),
])
def test_ascension_new_spellings(raw, city, state, want):
    hit = F.text_facility("Ascension Health", f"Location: {city}, {state}\nFacility: {raw}\n" + PAD, "RN", city, state)
    assert (hit[0] if hit else None) == want
