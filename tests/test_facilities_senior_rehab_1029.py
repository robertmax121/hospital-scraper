"""2026-10-29 (facility scale, phase 3, senior-rehab group): the senior living,
rehab and post-acute chains name their site on every posting
(facilities_senior_rehab).

Fixtures (tests/fixtures/facility_senior_rehab_1029/), real payloads read
2026-10-29, trimmed:
  jibe_rows.json        Signature HealthCARE and Trilogy /api/jobs page 1, one
                        row per distinct location_name, bodies cut to 200
                        characters; Signature's tags3 facet (87 values)
  workday_sunrise.json  Sunrise Senior Living's Workday list page 1
  phenom_pruitt.json    PruittHealth's search page (10 docs) and its
                        orgBusiness facet (296 values)
  oracle_brookdale.json 40 Brookdale requisitions with their work locations
  stored_rows.json      stored rows of Sunrise (one per link site), Trilogy
                        (one per body LOCATION campus), Signature (one per body
                        opening), Life Care Services (two per town) and
                        Encompass Health (one per town); emails and phone
                        numbers replaced
No network, no database."""
import asyncio
import json
import os
import re

import pytest

import facilities_senior_rehab as M
import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "facility_senior_rehab_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


JIBE = _load("jibe_rows.json")
WD = _load("workday_sunrise.json")["jobPostings"]
PH = _load("phenom_pruitt.json")
BKD = _load("oracle_brookdale.json")["requisitions"]
STORED = _load("stored_rows.json")["systems"]


# ══ the systems are configured where their hooks expect them ════════════════
def test_every_chain_is_a_configured_board_of_its_adapter():
    assert M.SUNRISE in scraper.WORKDAY_TENANTS
    assert M.TRILOGY in scraper.JIBE_SITES and M.SIGNATURE in scraper.JIBE_SITES
    assert M.PRUITT in scraper.PHENOM_ORGS
    assert M.BROOKDALE in scraper.ORACLE_ORGS and M.ENCOMPASS in scraper.ORACLE_ORGS
    assert M.LCS in scraper.ORACLE_ORGS
    for s in M.SYSTEMS:
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(s, s) == s      # the emit name is the stored label
    assert scraper.WD_SITE_FACILITY[M.SUNRISE] is M.sunrise_site
    assert scraper.WD_SITE_UNKNOWN[M.SUNRISE] is M.sunrise_unknown
    assert not (M.ORACLE_SYSTEMS & scraper.ORACLE_LOCATION_FACILITY)


# ══ Sunrise (Workday descriptor) ═════════════════════════════════════════════
def test_sunrise_list_rows_name_the_community_and_the_boards_town():
    got = {p["locationsText"]: M.sunrise_site(p["locationsText"], p["externalPath"]) for p in WD}
    assert got["Kennebunk, ME - Huntington Common"] == ("Huntington Common", "Kennebunk", "ME")
    assert got["Charlottesville, VA - The Colonnades"] == ("The Colonnades", "Charlottesville", "VA")
    # a descriptor with no town takes the town of the board's descriptor for the same community
    assert got["Sunrise of Fair Oaks"] == ("Sunrise of Fair Oaks", "Fair Oaks", "CA")
    for loc, hit in got.items():
        assert hit is not None, loc


def test_sunrise_descriptor_shapes():
    S = M.sunrise_site
    assert S("Arlington, MA - Sunrise of Arlington MA") == ("Sunrise of Arlington", "Arlington", "MA")
    assert S("Dublin, CA- Emerald Valley") == ("Emerald Valley", "Dublin", "CA")
    assert S("Emerald Valley") == ("Emerald Valley", "Dublin", "CA")
    assert S("Sunrise of Burlington MA") == ("Sunrise of Burlington", "Burlington", "MA")
    assert S("The Quadrangle") == ("The Quadrangle", "Haverford", "PA")
    # "2 Locations": the link's first segment is the primary descriptor
    assert S("2 Locations", "/job/Winston---Salem-NC---Brighton-Gardens-of-Winston---Salem/Cook_JR1") \
        == ("Brighton Gardens of Winston-Salem", "Winston-Salem", "NC")
    # rule 2: the head office keeps the system name and its town; pools and home care the system name
    assert S("McLean, VA - Community Support Office") == (M.SUNRISE, "McLean", "VA")
    assert S("Boston Metro Area - North Shore") == (M.SUNRISE, "", "")
    assert S("Charlotte, NC - Metro Area") == (M.SUNRISE, "Charlotte", "NC")
    assert S("Levittown, NY - At Home New York") == (M.SUNRISE, "Levittown", "NY")
    # no town anywhere on the board: named, never placed
    assert S("Sunrise of North Severna Park") == ("Sunrise of North Severna Park", "", "")
    # Canada is not listed (non_us_reason drops those postings)
    assert S("Richmond Hill, ON - Sunrise of Richmond Hill") is None
    # a new community in the board's usual shape
    assert S("Fairview, TX - Sunrise of Fairview") == ("Sunrise of Fairview", "Fairview", "TX")
    assert M.sunrise_unknown("Somewhere Else") == "Somewhere Else"
    assert M.sunrise_unknown("Kennebunk, ME - Huntington Common") == ""


def test_scrape_workday_names_sunrise_communities(monkeypatch):
    assert scraper.WD_SITE_FACILITY[M.SUNRISE]("Kennebunk, ME - Huntington Common",
                                               "/job/Kennebunk-ME---Huntington-Common/Cook_JR117402-1") \
        == ("Huntington Common", "Kennebunk", "ME")


# ══ Trilogy and Signature (Jibe location_name) ══════════════════════════════
@pytest.mark.parametrize("value,want", [
    ("The Meadows of Kalida", "The Meadows of Kalida"),
    ("Cedar Creek Health Campus", "Cedar Creek Health Campus"),
    ("Trilogy Health Services", ""),
    ("Trilogy Health Services - Travel", ""),
    ("Great Lakes Division", ""),
    ("Evansville, IN", ""),
    ("Synchrony Rehab -", ""),
    ("Synchrony Rehab at Logan Acres Care Center", ""),
    ("Rehab - Kingston Senior Living of Perrysburg", "Kingston Senior Living of Perrysburg"),
    ("Shelby Farms Senior Living, LLC", "Shelby Farms Senior Living"),
])
def test_trilogy_location_values(value, want):
    assert M.trilogy_name(value) == want


def test_jibe_rows_name_the_campus_or_center():
    for r in JIBE["Trilogy Health Services"]:
        got = scraper._jibe_facility(M.TRILOGY, r)
        if r.get("multipleLocations"):
            assert got == M.TRILOGY
        elif r["location_name"] in ("Trilogy Health Services", "Synchrony Rehab -"):
            assert got == M.TRILOGY
        else:
            assert got == r["location_name"], r["location_name"]
    sig = {r["location_name"]: scraper._jibe_facility(M.SIGNATURE, r) for r in JIBE["Signature HealthCARE"]}
    assert sig["Oakview Nursing & Rehab Center"] == "Oakview Nursing and Rehabilitation Center"
    assert sig["Signature HealthCARE of North Hardin Rehab & Wellness"] \
        == "Signature HealthCARE at North Hardin Rehab & Wellness Center"
    assert sig["Liberty Care & Rehabilitation Cente"] == "Liberty Care and Rehabilitation Center"
    assert sig["Princeton"] == "Princeton Transitional Care & Assisted Living"
    for office in ("Signature HealthCARE, LLC", "Signature HealthCARE KY, Lexington", "Signature HealthCARE TN, Memphis",
                   "Signature Consulting Services LLC"):
        if office in sig:
            assert sig[office] == M.SIGNATURE, office


def test_every_signature_facet_value_is_a_center_or_an_office():
    M.UNKNOWN.clear()
    centers, offices = set(), set()
    for v in JIBE["signature_tags3_facet"]:
        name = M.signature_name(v)
        (centers if name else offices).add(name or v)
    assert not M.UNKNOWN                                     # every value is in the table or reads as an office
    assert len(centers) >= 65
    assert offices == {
        "Signature Consulting Services LLC", "Signature HealthCARE KY, Bowling Green", "Signature HealthCARE KY, Elizabethtown",
        "Signature HealthCARE KY, Georgetown", "Signature HealthCARE KY, Lexington", "Signature HealthCARE KY, Louisville",
        "Signature HealthCARE KY, Paducah", "Signature HealthCARE, LLC", "Signature HealthCARE, LLC - North Carolina Regional",
        "Signature HealthCARE NC, Raleigh", "Signature HealthCARE Owensboro", "Signature HealthCARE TN, Cookeville",
        "Signature HealthCARE TN, Knoxville", "Signature HealthCARE TN, Memphis", "Signature HealthCARE TN, Nashville",
        "Signature HealthCARE TN, Portland", "Signature HealthCARE VA, Norfolk"}


class _R:
    def __init__(self, payload):
        self.status = 200
        self._p = payload
        self.headers = {"content-type": "application/json"}

    async def json(self, content_type=None):
        return self._p

    async def text(self):
        return json.dumps(self._p)


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


def test_scrape_jibe_stores_signature_centers_and_trilogy_campuses(monkeypatch):
    for system in (M.SIGNATURE, M.TRILOGY):
        rows = [dict(r) for r in JIBE[system]]
        payload = {"jobs": [{"data": r} for r in rows], "totalCount": len(rows)}
        monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
        monkeypatch.setattr(scraper, "jitter", _no_wait)
        jobs = asyncio.run(scraper.scrape_jibe(None, system, scraper.JIBE_SITES[system]))
        by = {j.job_id: j for j in jobs}
        assert len(by) == len(rows)
        for r in rows:
            j = by[str(r["req_id"])]
            assert j.hospital_system == system
            assert j.hospital_name == scraper._jibe_facility(system, r)


# ══ PruittHealth (Phenom workLocation) ═══════════════════════════════════════
@pytest.mark.parametrize("value,want", [
    ("PruittHealth - Virginia Park LLC", "PruittHealth - Virginia Park"),
    ("Pruitthealth - Carrollton", "PruittHealth - Carrollton"),
    ("PH @ Home - Albany", "PruittHealth @ Home - Albany"),
    ("PH @ Home Columbus", "PruittHealth @ Home - Columbus"),
    ("PruittHealth @ Home - Rome", "PruittHealth @ Home - Rome"),
    ("PH Hospice Western Tennessee", "PruittHealth Hospice Western Tennessee"),
    ("PruittHealth Hospice Macon", "PruittHealth Hospice Macon"),
    ("PruittPlace - Buckhead LLC", "PruittPlace - Buckhead"),
    ("The Oaks of Athens LLC.", "The Oaks of Athens"),
    ("NCVA Salisbury", "North Carolina State Veterans Home - Salisbury"),
    ("NCSVH - Kernersville", "North Carolina State Veterans Home - Kernersville"),
    ("TS SN Aiken", "PruittHealth - Aiken"),
    ("TS SN Old Capital", "PruittHealth - Old Capitol"),
    ("TS SN Hill Haven Nursing Home", ""),          # a center the board does not list as PruittHealth's
    ("TS OPT Kennesaw", ""),
    ("PH Executive", ""),
    ("PruittHealth Pharmacy Services Atlanta", ""),
    ("PruittHealth Home Health - Home Office", ""),
    ("NORCROSS", ""),
    ("UNITED STATES", ""),
])
def test_pruitt_values(value, want):
    assert M.pruitt_name(value) == want


def test_pruitt_facet_and_list_docs():
    facet = PH["orgBusiness_facet"]
    named = sum(n for v, n in facet.items() if M.pruitt_name(v))
    total = sum(facet.values())
    assert named / total > 0.9                               # 1,319 of 1,407 postings on 2026-10-29
    for d in PH["docs"]:
        got = M.phenom_facility(M.PRUITT, d)
        assert got == (M.pruitt_name(d["workLocation"]) or M.PRUITT)
    assert M.phenom_facility("Hartford HealthCare", PH["docs"][0]) is None


# ══ Brookdale (Oracle work location) ═════════════════════════════════════════
def test_brookdale_work_location_names_the_community_at_its_own_town():
    seen = 0
    for j in BKD:
        loc, city, state = scraper._oracle_location(j)
        name, c, s = M.oracle_facility(M.BROOKDALE, j, city, state)
        wl = j["workLocation"]["items"][0]
        ln = wl["LocationName"]
        if re.match(r"^\d{5}-Brookdale ", ln):
            assert name.startswith("Brookdale ") and "(" not in name and not re.search(r"\s\d$", name)
            assert (c, s) == (M.town_case(wl["TownOrCity"]), wl["Region2"])
            seen += 1
        else:
            assert name == M.BROOKDALE and (c, s) == (city, state)      # an office: the system, the posting's town
    assert seen >= 30
    assert M.brookdale_name("50940-Brookdale University Park 1 (AL)") == "Brookdale University Park"
    assert M.brookdale_name("59000-Nashville Office, TN") == ""
    poulsbo = {"workLocation": {"items": [{"LocationName": "24642-Brookdale Montclair Poulsbo (WA)",
                                           "TownOrCity": "Poulsbo", "Region2": "WA"}]}}
    assert M.oracle_facility(M.BROOKDALE, poulsbo, "Seattle", "WA") == ("Brookdale Montclair Poulsbo", "Poulsbo", "WA")
    assert M.oracle_facility("Lifepoint Health", poulsbo) is None


# ══ the finalize pass: Life Care Services, Encompass, body fallbacks ════════
def _job(system, **kw):
    base = dict(title="RN", hospital_system=system, hospital_name=system, city="", state="", location="",
                specialty="", job_type="", url="https://example.org/j/1", job_id="1", posted_date="",
                description="", ats_platform="")
    base.update(kw)
    return scraper.Job(**base)


def test_lcs_body_names_one_community_in_its_own_town():
    j = _job(M.LCS, city="Lubbock", state="TX", description="Position Title: Cook\n\nWhen you work at Carillon Senior "
             "Living , you'll notice ...")
    assert M.apply_job(j) == "text" and j.hospital_name == "Carillon Senior Living"
    # a short brand spelling names the one community of that brand in the posting's town
    j = _job(M.LCS, city="Carmel", state="IN", description="When you work at Rose Senior Living, you have a front-row seat")
    assert M.apply_job(j) == "text" and j.hospital_name == "Rose Senior Living Carmel"
    # a community of another town is not this posting's
    j = _job(M.LCS, city="Des Moines", state="IA", description="When you work at Carillon Senior Living, ...")
    assert M.apply_job(j) == "" and j.hospital_name == M.LCS
    # two communities of one town named: none
    j = _job(M.LCS, city="Seminole", state="FL", description="Freedom Square of Seminole and Lake Seminole Square ...")
    assert M.apply_job(j) == "" and j.hospital_name == M.LCS
    # no body tonight: the stored name is kept
    M.set_known_facilities([{"hospital_system": M.LCS, "job_id": "77", "hospital_name": "Essex Meadows"}])
    j = _job(M.LCS, city="Essex", state="CT", job_id="77")
    assert M.apply_job(j) == "kept" and j.hospital_name == "Essex Meadows"
    M.set_known_facilities([])


def test_lcs_stored_rows():
    rows = STORED[M.LCS]
    named = [r for r in rows if M.stored_facility(M.LCS, r)]
    assert len(named) / len(rows) > 0.75
    for r in rows:
        got = M.stored_facility(M.LCS, r)
        if got:
            v = M.LCS_COMMUNITIES[got[0]]
            assert (re.sub(r"[^a-z]", "", got[1].lower().replace("saint", "st")),
                    got[2]) == (re.sub(r"[^a-z]", "", v[0].lower().replace("saint", "st")), v[1])
            if r["city"] or r["state"]:
                assert got[1:3] == (r["city"], r["state"])                   # the posting keeps its town
            else:
                assert got[1:3] == (v[0], v[1])                              # no town: the community's own
        if r["city"] == "Des Moines":
            assert got is None or got[0] != M.LCS


def test_encompass_one_hospital_per_town():
    E = M.encompass_hospital
    assert E("Certified Nursing Assistant", "Wesley Chapel", "FL") == "Encompass Health Rehabilitation Hospital of Wesley Chapel"
    assert E("RN", "Augusta", "GA") == "Walton Rehabilitation Hospital"
    assert E("RN", "Winston Salem", "NC") == "Novant Health Rehabilitation Hospital"   # punctuation aside
    assert E("RN", "St. Petersburg", "FL") == "Encompass Health Rehabilitation Hospital of St. Petersburg"
    assert E("RN", "Las Vegas", "NV") == ""                  # two hospitals in town
    assert E("Accountant", "Birmingham", "AL") == ""         # the head office
    assert E("Area Business Development Director", "Richmond", "VA") == ""
    assert E("Regional VP, Business Development", "Fort Myers", "FL") == ""
    assert E("RN", "Somewhere", "ZZ") == "" and E("RN", "", "FL") == ""
    towns = [(M._town_key(t), s) for _n, t, s, _src in M.ENCOMPASS_HOSPITALS]
    assert len(towns) == len(set(towns))
    for name, town, st, src in M.ENCOMPASS_HOSPITALS:
        assert src.startswith(("cms", "rows")), name
        assert "—" not in name and "(" not in name


def test_encompass_stored_rows_and_the_pass():
    rows = STORED[M.ENCOMPASS]
    for r in rows:
        got = M.stored_facility(M.ENCOMPASS, r)
        if got:
            assert got[0] == M.encompass_hospital(r["title"], r["city"], r["state"])
            assert got[1:3] == (r["city"], r["state"])
    j = _job(M.ENCOMPASS, title="Physical Therapist", city="Cookeville", state="TN")
    assert M.apply_jobs([j]) == {M.ENCOMPASS: {"town": 1}}
    assert j.hospital_name == "Encompass Health Rehabilitation Hospital of Cookeville"
    j = _job(M.ENCOMPASS, hospital_name="Somewhere Else", city="Cookeville", state="TN")
    assert M.apply_job(j) == "" and j.hospital_name == "Somewhere Else"     # a named row is left alone


def test_trilogy_and_signature_bodies_name_the_site_when_the_feed_did_not():
    tri = STORED[M.TRILOGY]
    hits = 0
    for r in tri:
        got = M.stored_facility(M.TRILOGY, r)
        assert got is not None
        hits += got[0] != M.TRILOGY
    assert hits / len(tri) > 0.85
    blk = next(r for r in tri if "Cedar Creek Health Campus" in r["description"])
    assert M.trilogy_body_site(blk["description"]) == "Cedar Creek Health Campus"
    sig = STORED[M.SIGNATURE]
    for r in sig:
        got = M.stored_facility(M.SIGNATURE, r)
        assert got is not None, r["description"][:80]
        assert got[1] == M.town_case(r["city"])
    oak = next(r for r in sig if r["description"].startswith("About Us\n\nOakview"))
    assert M.stored_facility(M.SIGNATURE, oak)[0] == "Oakview Nursing and Rehabilitation Center"
    j = _job(M.SIGNATURE, city="SOUTH PITTSBURG", state="TN",
             description="About Us\n\nSignature HealthCARE of South Pittsburg Rehab & Wellness Center is a 120-bed")
    assert M.apply_job(j) == "text"
    assert (j.hospital_name, j.city) == ("Signature HealthCARE of South Pittsburg Rehab & Wellness Center", "South Pittsburg")


def test_sunrise_stored_rows_by_link():
    rows = STORED[M.SUNRISE]
    for r in rows:
        got = M.stored_facility(M.SUNRISE, r)
        assert got is not None, r["url"]
        name, city, state, _src = got
        if name != M.SUNRISE:
            assert name and "---" not in name
    sev = next(r for r in rows if "Sunrise-of-North-Severna-Park" in r["url"])
    assert M.stored_facility(M.SUNRISE, sev)[:3] == ("Sunrise of North Severna Park", "", "")   # never a guessed town


def test_brookdale_and_pruitt_stored_rows_wait_for_the_feed():
    assert M.stored_facility(M.BROOKDALE, {"city": "Plano", "state": "TX", "title": "CNA"}) is None
    assert M.stored_facility(M.PRUITT, {"city": "Aiken", "state": "SC", "title": "RN"}) is None


def test_finalize_runs_the_pass_before_the_town_tables():
    import inspect
    src = inspect.getsource(scraper.finalize_jobs)
    assert src.index("facilities_senior_rehab.apply_jobs") < src.index("facilities_town.apply_jobs")
