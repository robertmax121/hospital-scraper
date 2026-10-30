"""2026-10-30 (facility round 2, the "others" group): AnMed's street
addresses, Spartanburg Regional's and CentraCare's Phenom sites, CommonSpirit
and MultiCare title sites, Erlanger title towns (facilities_r2_others).

Fixtures (tests/fixtures/r2others1030/), read live on 2026-10-30 and cut to
the facility fields (no requisition custom fields, no contacts, no bodies):
  anmed_hits.json       every AnMed HealthcareSource hit (270)
  srhs_docs.json        every Spartanburg Regional Phenom list doc (606)
  centracare_docs.json  every CentraCare Phenom list doc (303)
Titles in the inline cases are the stored postings' own. No person's name,
e-mail or phone. No network, no database."""
import asyncio
import copy
import json
import os
from collections import Counter

import pytest

import facilities_r2_others as R
import facilities_town as TOWN
import facility_names as F
import scraper
from conftest import FIXTURES

FIX = os.path.join(FIXTURES, "r2others1030")


def _load(name, key):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)[key]


ANMED_HITS = _load("anmed_hits.json", "hits")
SRHS_DOCS = _load("srhs_docs.json", "docs")
CC_DOCS = _load("centracare_docs.json", "docs")
PAD = "\n\n" + "Provides patient care and support to the unit team. " * 40


def _job(system, name, *, city="", state="", title="Registered Nurse", job_id="J1", description=PAD):
    return scraper.Job(title=title, hospital_system=system, hospital_name=name, city=city, state=state,
                       location=f"{city}, {state}" if city else state, specialty="", job_type="", url="",
                       job_id=job_id, posted_date="", description=description, ats_platform="")


@pytest.fixture(autouse=True)
def _no_stored():
    TOWN.set_stored([])
    F.set_known_facilities([])
    yield
    TOWN.set_stored([])
    F.set_known_facilities([])


# ── AnMed (HealthcareSource street address + department) ────────────────────

@pytest.mark.parametrize("address, key", [
    ("800 N. Fant Street", "800 fant"), ("800 North Fant St.", "800 fant"), ("800 N Fant St", "800 fant"),
    ("2000 E. Greenville St.", "2000 greenville"), ("2000 East Greenville Street", "2000 greenville"),
    ("1655 E. Greenville St.", "1655 greenville"), ("Highway 24", ""), ("", ""),
])
def test_street_key(address, key):
    assert R.street_key(address) == key


def _anmed(hit):
    return scraper._hcs_job(hit, "AnMed Health", "anmed")


def _by_title(title, dept=None, street=None):
    for h in ANMED_HITS:
        s = h["_source"]
        d = ((s["hiringOrganization"]["subOrganization"].get("subOrganization") or {}).get("name"))
        if s["title"] == title and (dept is None or d == dept) and \
                (street is None or s["jobLocation"]["address"].get("streetAddress") == street):
            return h
    raise AssertionError(f"no fixture hit {title!r} {dept!r}")


@pytest.mark.parametrize("title, dept, want", [
    # units of the main campus (800 N Fant St)
    ("Staff Nurse - Neuro ICU **BONUS ELIGIBLE**", "7 North - Neuro ICU ", "AnMed Medical Center"),
    ("Perfusionist", "CV Surgery", "AnMed Medical Center"),
    ("Registrar - Emergency Business Office", "Emergency Business Office (EBO)", "AnMed Medical Center"),
    ("Speech Language Pathologist - Acute Care", "AnMed Therapy - Acute Care", "AnMed Medical Center"),
    # North Campus departments (2000 E Greenville St)
    ("Staff Nurse - Endoscopy", "NC - Endoscopy", "AnMed North Campus"),
    ("Pharmacy Manager - North Campus", "Infusion Pharmacy", "AnMed North Campus"),
    # the main campus's address on an urgent care in Seneca, a practice, an
    # office, the float team: the label
    ("Radiologic Technologist - Urgent Care", "AnMed Urgent Care - Seneca (5720)", "AnMed"),
    ("Physician", "AnMed Young Adult Primary Care  (5510)", "AnMed"),
    ("Cyber Security Analyst II", "Information Services", "AnMed"),
    ("Staff Nurse - Resource Team (Tier 1) *BONUS ELIGIBLE*", "Resource Team", "AnMed"),
    ("Spanish Interpreter", "Language Services & Collaborative Care", "AnMed"),
    # a practice in a North Campus office building, the cancer centre
    ("Staff Nurse - Physician Practice", "AnMed Urology", "AnMed"),
    ("Radiation Therapist", "CC Radiation Oncology", "AnMed"),
])
def test_anmed_hits(title, dept, want):
    assert _anmed(_by_title(title, dept)).hospital_name == want


def test_anmed_other_addresses_and_named_entities_unchanged():
    """Every hit off the two campus addresses keeps the adapter's own value
    ("AnMed", or the entity's own site: "AnMed Cannon", "AnMed Piedmont")."""
    for h in ANMED_HITS:
        s = h["_source"]
        key = R.street_key(s["jobLocation"]["address"].get("streetAddress"))
        sub = s["hiringOrganization"]["subOrganization"]["name"]
        if key in ("800 fant", "2000 greenville") and sub == "AnMed":
            continue
        assert _anmed(h).hospital_name == sub


def test_anmed_counts_on_the_live_feed():
    got = Counter(_anmed(h).hospital_name for h in ANMED_HITS)
    assert got["AnMed Medical Center"] >= 110 and got["AnMed North Campus"] >= 20
    assert set(got) == {"AnMed Medical Center", "AnMed North Campus", "AnMed", "AnMed Cannon", "AnMed Piedmont"}
    # the town never changes: every hit stays in its own town
    for h in ANMED_HITS:
        j = _anmed(h)
        assert j.city == h["_source"]["jobLocation"]["address"]["addressLocality"].strip()


def test_anmed_posting_for_both_campuses_keeps_the_label():
    site, why = R.anmed_site("AnMed", "NC - Surgery", "2000 East Greenville Street", "Anderson",
                             "Staff Nurse - North Campus Operating Room",
                             "AnMed's Operating Room Department spans both the Main Medical Center and North Campus. "
                             "Travel between AnMed's Main Medical Center and North Campus is required.")
    assert site is None and why == "both-campuses"


def test_anmed_elsewhere_and_other_systems():
    assert R.anmed_site("AnMed", "Surgery", "800 N Fant St", "Clemson", "Staff Nurse")[0] is None
    assert R.hcs_site("CaroMont Health", ANMED_HITS[0], "AnMed") is None
    assert R.hcs_site("AnMed Health", ANMED_HITS[0], "AnMed Cannon") is None


# ── Phenom: Spartanburg Regional and CentraCare (through the adapter) ───────

class _R:
    def __init__(self, status, body):
        self.status, self._body, self.url = status, body, ""
        self.headers = {"content-type": "text/html" if isinstance(body, str) else "application/json"}

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


_HTML = ('<script>phApp.ddo = {}; phApp.pageInfo = {"refNum":"TESTUS","siteType":"external",'
         '"rootDomain":"https://careers.example.org","pageId":"page4"};</script>')


def _board(monkeypatch, docs):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "_retry_sleep", no_wait)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    base = "https://careers.example.org"
    docs = [copy.deepcopy(d) for d in docs]

    def fake_req(session, method, url, **kw):
        body = kw.get("json") or {}
        if method == "post" and url == f"{base}/widgets" and body.get("ddoKey") == "refineSearch":
            frm, size = int(body.get("from") or 0), int(body.get("size") or 50)
            page = docs[frm:frm + size]
            return _Ctx(_R(200, {"refineSearch": {"status": 200, "hits": len(page), "totalHits": len(docs),
                                                  "data": {"jobs": page}}}))
        if method == "get" and url == f"{base}/us/en/search-results":
            return _Ctx(_R(200, _HTML))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return base


def _scrape(monkeypatch, system, docs, hook=True):
    base = _board(monkeypatch, docs)
    with monkeypatch.context() as m:
        if not hook:
            m.setattr(R, "phenom_site", lambda *a, **k: None)
        return {j.job_id: j for j in asyncio.run(scraper.scrape_phenom(None, system, base))}


def test_srhs_feed_changes_only_the_round_2_rows(monkeypatch):
    before = _scrape(monkeypatch, "Spartanburg Regional", SRHS_DOCS, hook=False)
    after = _scrape(monkeypatch, "Spartanburg Regional", SRHS_DOCS)
    assert set(before) == set(after)
    changed = {k: ((before[k].hospital_name, before[k].city), (after[k].hospital_name, after[k].city))
               for k in after if (before[k].hospital_name, before[k].city, before[k].state)
               != (after[k].hospital_name, after[k].city, after[k].state)}
    names = Counter(v[1][0] for v in changed.values())
    assert names["Spartanburg Hospital for Restorative Care"] == 20
    assert names["Surgery Center at Pelham"] == 1
    assert names["Spartanburg Medical Center"] >= 4 and names["Cherokee Medical Center"] >= 1
    # every change starts from the label or from the "South Carolina" town
    for (old_name, old_city), (new_name, new_city) in changed.values():
        assert old_name == "Spartanburg Regional" or old_city == "South Carolina"
        assert new_city != "South Carolina"
    # a town-only fix keeps the label
    assert any(v[1][0] == "Spartanburg Regional" and v[0][1] == "South Carolina" for v in changed.values())


def test_srhs_rules():
    doc = {"FacilityName": "Restorative Care"}
    assert R.phenom_site("Spartanburg Regional", doc, "Spartanburg", "SC") == \
        ("Spartanburg Hospital for Restorative Care", "Spartanburg", "SC")
    assert R.phenom_site("Spartanburg Regional", doc, "Greer", "SC") is None          # elsewhere: phenom decides
    mgc = {"FacilityName": "Medical Group of the Carolinas",
           "multi_location_array": [{"location": "South Carolina, United States"},
                                    {"location": "250 Westmoreland Rd, Greer, SC 29651, USA"}]}
    assert R.phenom_site("Spartanburg Regional", mgc, "South Carolina", "SC") == ("", "Greer", "SC")
    bare = {"FacilityName": "Medical Group of the Carolinas",
            "multi_location_array": [{"location": "South Carolina, United States"}]}
    assert R.phenom_site("Spartanburg Regional", bare, "South Carolina", "SC") is None   # no town: unchanged
    smc = {"FacilityName": "Spartanburg Medical Center",
           "multi_location_array": [{"location": "South Carolina, United States"}]}
    assert R.phenom_site("Spartanburg Regional", smc, "South Carolina", "SC") == \
        ("Spartanburg Medical Center", "Spartanburg", "SC")
    smc_greer = dict(smc, multi_location_array=[{"location": "Greer, SC, USA"}])
    assert R.phenom_site("Spartanburg Regional", smc_greer, "South Carolina", "SC") == ("", "Greer", "SC")
    two = dict(mgc, multi_location_array=[{"location": "Greer, SC, USA"}, {"location": "Gaffney, SC, USA"}])
    assert R.phenom_site("Spartanburg Regional", two, "South Carolina", "SC") is None
    assert R.phenom_site("Other Phenom", doc, "Spartanburg", "SC") is None


def test_centracare_feed_changes_only_willmar_and_redwood(monkeypatch):
    before = _scrape(monkeypatch, "CentraCare", CC_DOCS, hook=False)
    after = _scrape(monkeypatch, "CentraCare", CC_DOCS)
    changed = {k: after[k] for k in after if before[k].hospital_name != after[k].hospital_name}
    got = Counter((j.hospital_name, j.city) for j in changed.values())
    assert set(got) <= {("CentraCare - Rice Memorial Hospital", "Willmar"), ("CentraCare - Redwood", "Redwood Falls")}
    assert got[("CentraCare - Rice Memorial Hospital", "Willmar")] >= 20
    assert all(before[k].hospital_name == "CentraCare" for k in changed)
    assert all((before[k].city, before[k].state) == (after[k].city, after[k].state) for k in after)


@pytest.mark.parametrize("dept, title, want", [
    ("Intensive Care Unit", "Registered Nurse | Intensive Care Unit", True),
    ("Hospital Med Hospitalists", "Hospitalist - Physician - Willmar, MN", True),
    ("Nutrition", "Food Service Worker II", True),
    ("Nutrition", "Food Service Worker - High School", False),
    ("Dental Clinic", "Dentist", False),
    ("Willmar Urgent Care", "Urgent Care - Nurse Practitioner or Physician Assistant - Willmar, MN", False),
    ("Emergency Medical Services", "Paramedic", False),
    ("Eye Clinic", "Ophthalmologist Physician - Willmar, MN", False),
    ("", "Internal Medicine Physician - Willmar, MN", False),
])
def test_centracare_willmar_departments(dept, title, want):
    doc = {"department": dept} if dept else {}
    got = R.phenom_site("CentraCare", doc, "Willmar", "MN", title)
    assert (got == ("CentraCare - Rice Memorial Hospital", "Willmar", "MN")) is want


def test_centracare_rules():
    assert R.phenom_site("CentraCare", {}, "Redwood Falls", "MN", "RN | Med-Surg, OB & ER") == \
        ("CentraCare - Redwood", "Redwood Falls", "MN")
    # a work location decides (facilities_phenom); another town names nothing
    assert R.phenom_site("CentraCare", {"workLocation": "Willmar Skylark Clinic", "department": "Intensive Care Unit"},
                         "Willmar", "MN", "Registered Nurse") is None
    assert R.phenom_site("CentraCare", {"department": "Intensive Care Unit"}, "Saint Cloud", "MN",
                         "Registered Nurse") is None


# ── Title rules (finalize_jobs) ─────────────────────────────────────────────

@pytest.mark.parametrize("system, name, title, city, state, want", [
    ("CommonSpirit Health", "CommonSpirit Health", "Supervisor Patient Access-VMFH St Anne Burien", "Burien", "WA",
     ("St Anne Hospital", "Burien", "WA")),
    ("CommonSpirit Health", "CommonSpirit Health", "Manager Patient Access-StJoseph Mt Sterling", "Mount Sterling",
     "KY", ("Saint Joseph Hospital - Mount Sterling", "Mount Sterling", "KY")),
    ("CommonSpirit Health", "CommonSpirit Health", "Supervisor Patient Access-CHI Mercy Health Roseburg", "Roseburg",
     "OR", ("CHI Mercy Health of Roseburg", "Roseburg", "OR")),
    # two hospitals in one town or several sites: the label
    ("CommonSpirit Health", "CommonSpirit Health", "Supervisor Patient Access-StJoseph Lexington", "Lexington", "KY",
     ("CommonSpirit Health", "Lexington", "KY")),
    ("CommonSpirit Health", "CommonSpirit Health", "Supervisor Patient Access-CHI Hlth CUMCBM-Imman-Lakeside-MHP",
     "Omaha", "NE", ("CommonSpirit Health", "Omaha", "NE")),
    ("CommonSpirit Health", "CommonSpirit Health", "Medical Surgical Travel Nurse", "Lufkin", "TX",
     ("CommonSpirit Health", "Lufkin", "TX")),
    ("MultiCare Health", "MultiCare Health", "Polysomnographic Technologist/Scorer - Auburn Medical Center", "Auburn",
     "WA", ("MultiCare Auburn Medical Center", "Auburn", "WA")),
    ("MultiCare Health", "MultiCare Health", "Unit Based NPD Specialist Auburn/Covington PeriOp", "Auburn", "WA",
     ("MultiCare Health", "Auburn", "WA")),
    ("MultiCare Health", "MultiCare Health", "RN Summit Team", "Auburn", "WA", ("MultiCare Health", "Auburn", "WA")),
    # the title's hospital in another town: nothing named
    ("MultiCare Health", "MultiCare Health", "Polysomnographic Technologist/Scorer - Auburn Medical Center", "Kent",
     "WA", ("MultiCare Health", "Kent", "WA")),
    ("Erlanger Health System", "Erlanger Health System", "NP/PA- Primary Care (Pikeville, TN) Part-Time",
     "Chattanooga", "TN", ("Erlanger Health System", "Pikeville", "TN")),
    ("Erlanger Health System", "Erlanger Health System", "LPN - Family Medicine Ringold, GA - Full-Time",
     "Chattanooga", "TN", ("Erlanger Health System", "Ringgold", "GA")),
    ("Erlanger Health System", "Erlanger Health System", "LPN -Erlanger Medical Group Float Pool TN or GA - PRN",
     "Chattanooga", "TN", ("Erlanger Health System", "Chattanooga", "TN")),
    ("Erlanger Health System", "Erlanger Health System", "LPN Procedure Nurse - Primary Care Cleveland - Full-Time",
     "Chattanooga", "TN", ("Erlanger Health System", "Chattanooga", "TN")),
])
def test_title_rules_through_finalize(system, name, title, city, state, want):
    rows = scraper.finalize_jobs([_job(system, name, city=city, state=state, title=title)])
    assert (rows[0]["hospital_name"], rows[0]["city"], rows[0]["state"]) == want
    assert rows[0]["hospital_system"] in (system, scraper.HOSPITAL_SYSTEM_ALIASES[system])


def test_title_rules_leave_named_rows_and_other_systems():
    jobs = [_job("CommonSpirit Health", "St Anne Hospital", city="Burien", state="WA",
                 title="Supervisor Patient Access-VMFH St Anne Burien"),
            _job("Ascension", "Ascension Health", city="Burien", state="WA",
                 title="Supervisor Patient Access-VMFH St Anne Burien", job_id="J2")]
    assert R.apply_facilities(jobs, canon=lambda s: scraper.HOSPITAL_SYSTEM_ALIASES.get(s, s)) == {}
    assert [j.hospital_name for j in jobs] == ["St Anne Hospital", "Ascension Health"]


def test_a_row_the_rule_raises_on_keeps_its_name(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen row")
    monkeypatch.setattr(R, "title_site", boom)
    j = _job("MultiCare Health", "MultiCare Health", city="Auburn", state="WA",
             title="Polysomnographic Technologist/Scorer - Auburn Medical Center")
    assert R.apply_facilities([j]) == {"MultiCare": {"error": 1}}
    assert (j.hospital_name, j.city) == ("MultiCare Health", "Auburn")


def test_stored_change_matches_the_nightly():
    row = {"hospital_system": "CommonSpirit", "hospital_name": "CommonSpirit Health", "city": "Burien", "state": "WA",
           "title": "Supervisor Patient Access-VMFH St Anne Burien"}
    new, city, state, src = R.stored_change(row)
    assert (new, city, state) == ("St Anne Hospital", "Burien", "WA") and "title" in src
    row = {"hospital_system": "Erlanger", "hospital_name": "Erlanger Health System", "city": "Chattanooga",
           "state": "TN", "title": "NP/PA- Primary Care (Pikeville, TN) Part-Time"}
    assert R.stored_change(row)[:3] == ("Erlanger Health System", "Pikeville", "TN")
    row["hospital_name"] = "Erlanger Baroness Hospital"
    assert R.stored_change(row) is None


# ── The scraper's wiring ────────────────────────────────────────────────────

def test_hooks_and_pass_order():
    src = open(scraper.__file__, encoding="utf-8").read()
    i = src.index("def finalize_jobs(")
    f7 = src.index("facilities_fac7_variants.apply_facilities", i)
    r2 = src.index('_facility_pass("Round 2 titles"', i)
    town = src.index('_facility_pass("Town tables"', i)
    assert f7 < r2 < town
    h = src.index("def _hcs_job(")
    assert h < src.index("facilities_r2_others.hcs_site", h) < src.index("def _hcs_pay(", h)
    p = src.index("async def scrape_phenom(")
    assert p < src.index("facilities_phenom.list_facility(system, doc", p) < \
        src.index("facilities_r2_others.phenom_site", p)
