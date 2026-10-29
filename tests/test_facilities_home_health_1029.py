"""2026-10-29 (phase 3): named branches of the home health, hospice and private
duty chains (facilities_home_health).

Fixtures (tests/fixtures/home_health_1029/), every one a real payload read
2026-10-29, a few requests per host:
  bayada_departments.json  the BAYADA Greenhouse board's departments that held
                           a job, with their job counts (boards-api /offices)
  amed_jibe.json           Amedisys' first Jibe list page (100 rows, trimmed)
  acc_jibe.json            AccentCare's first Jibe list page (100 rows, trimmed)
  vitas_oracle.json        VITAS' first Oracle requisition page (25, trimmed)
  centerwell_cxs.json      five CenterWell Workday CXS details
  stored_bodies.json       stored hospital_jobs bodies (BrightSpring, Compassus,
                           CenterWell), cut at 700 characters
No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_home_health as H
import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "home_health_1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


BAYADA_DEPTS = _load("bayada_departments.json")
AMED = _load("amed_jibe.json")["rows"]
ACC = _load("acc_jibe.json")["rows"]
VITAS = _load("vitas_oracle.json")["requisitions"]
CW = _load("centerwell_cxs.json")["details"]
BODIES = {r["id"]: r for r in _load("stored_bodies.json")["rows"]}


@pytest.fixture(autouse=True)
def _clean():
    H.set_known_facilities([])
    H.UNKNOWN.clear()
    yield
    H.set_known_facilities([])
    H.UNKNOWN.clear()


def _job(system, name=None, city="", state="", description="", job_id="1", title="RN", ats="T"):
    return Job(title=title, hospital_system=system, hospital_name=system if name is None else name,
               city=city, state=state, location=", ".join(p for p in (city, state) if p), specialty="",
               job_type="", url="https://x/1", job_id=job_id, posted_date="", description=description,
               ats_platform=ats)


def _body_job(rid):
    r = BODIES[rid]
    return _job(r["hospital_system"], r["hospital_name"], r["city"], r["state"], r["description"],
                r["job_id"], r["title"])


# ── BAYADA: the Greenhouse department is the service office ────────────────

def test_bayada_every_department_on_the_board():
    named = {d["name"]: H.bayada_branch([{"name": d["name"]}]) for d in BAYADA_DEPTS["departments"]}
    jobs_named = sum(d["jobs"] for d in BAYADA_DEPTS["departments"] if named[d["name"]])
    assert BAYADA_DEPTS["jobs"] == 2613
    assert sum(1 for v in named.values() if v) == 344 and jobs_named == 1809
    # the owner's own example shape
    assert named["Asheville (ASH) - 84"] == "BAYADA Home Health Care - Asheville"
    assert named["Lehigh Valley Home Health (LVV) - 418"] == "BAYADA Home Health Care - Lehigh Valley Home Health"
    assert named["Asheville Skilled Nursing and Pediatrics (AVL) - 256"] == \
        "BAYADA Home Health Care - Asheville Skilled Nursing and Pediatrics"
    assert named["Indianapolis Skilled  (INP) - 597"] == "BAYADA Home Health Care - Indianapolis Skilled"
    # a joint venture keeps its own name
    assert named["Valley Health at Home by BAYADA (VHJ) - 996"] == "Valley Health at Home by BAYADA"
    assert named["Gloucester Hospice BAYADA at Inspira (NGJ) - 649"] == "Gloucester Hospice BAYADA at Inspira"
    assert named["Dover BAYADA at Bayhealth (Mirror Office) (DBT) - 701"] == "Dover BAYADA at Bayhealth"
    # no service office: division, region, sales, unit, support, closed, mirror of another office
    for n in ("Violet Division (VIO) - 169", "Mountain Laurel Division", "Sequoia Region (SQU) - 692",
              "Moccasin Community Sales (MCL) - 6001", "Skilled Nursing Unit (SNU) - 291",
              "Skilled Nursing HICU Service Office (HIC) - 1450", "Fairfax Home Health (Closed) (FFV) - 728",
              "CMRHC Cape May (Mirror Office) (CMC) - 644", "Office Payroll (OPR) - 585",
              "Acquisition #1 Office (FVV) - 1167", "BAYADA"):
        assert named[n] == "", n


def test_bayada_two_offices_name_none():
    assert H.bayada_branch([{"name": "Millville (MIL) - 52"}, {"name": "Newton (NWT) - 231"}]) == ""
    assert H.bayada_branch([{"name": "Millville (MIL) - 52"}, {"name": "Violet Division (VIO) - 169"}]) == \
        "BAYADA Home Health Care - Millville"
    assert H.bayada_branch([]) == "" and H.bayada_branch(None) == ""


def _serve(monkeypatch, body):
    class _R:
        status = 200
        headers = {"content-type": "application/json"}

        async def json(self, content_type=None):
            return body

    class _Ctx:
        async def __aenter__(self):
            return _R()

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(scraper, "req", lambda *a, **k: _Ctx())


def test_greenhouse_adapter_names_the_office(monkeypatch):
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "push10", "greenhouse_bayada_job.json"),
              encoding="utf-8") as f:
        j = json.load(f)
    assert j["departments"][0]["name"] == "Violet Division (VIO) - 169"     # the real job: a division
    j2 = dict(j, id=1, departments=[{"id": 4078540111, "name": "Millville (MIL) - 52", "child_ids": [],
                                     "parent_id": 4015293002}])
    _serve(monkeypatch, {"jobs": [j, j2], "meta": {"total": 2}})
    jobs = asyncio.run(scraper.scrape_greenhouse(None, "BAYADA Home Health Care", "bayada"))
    by = {x.job_id: x for x in jobs}
    assert by[str(j["id"])].hospital_name == "BAYADA Home Health Care"
    assert by["1"].hospital_name == "BAYADA Home Health Care - Millville"
    assert by["1"].hospital_system == "BAYADA Home Health Care" and by["1"].city == "Sicklerville"
    # another Greenhouse board is untouched
    _serve(monkeypatch, {"jobs": [j2], "meta": {"total": 1}})
    other = asyncio.run(scraper.scrape_greenhouse(None, "One Medical", "onemedical"))
    assert other[0].hospital_name == "One Medical"


# ── Amedisys / AccentCare: the Jibe location_name ──────────────────────────

def test_amedisys_jibe_rows():
    got = {r["location_name"]: scraper._jibe_facility("Amedisys", r) for r in AMED}
    assert sum(1 for v in got.values() if v != "Amedisys") == len(got) - 3
    assert got["2214 Sumter, SC Winyah"] == "Amedisys Home Health - Sumter"
    assert got["5050 Largo, MD-HSP"] == "Amedisys Hospice - Largo"
    assert got["2225 West Columbia, SC"] == "Amedisys Home Health - West Columbia"      # row city Columbia
    assert got["2634 Amedisys Hme Hth of Butler, PA"] == "Amedisys Home Health - Butler"
    assert got["0475 The Village FL HOS CCH"] == "Amedisys Hospice - The Villages"
    assert got["0476 Minneola, FL HOS CCH"] == "Amedisys Hospice - Minneola"            # row city Clermont
    assert got["1307 Lee's Summit, MO"] == "Amedisys Home Health - Lee's Summit"
    # a value without its care-center number names none
    for v in ("Columbia, SC", "Mystic, CT", "New Haven, CT"):
        assert got[v] == "Amedisys"


def test_accentcare_jibe_rows():
    got = {(r["location_name"], r["city"]): scraper._jibe_facility("AccentCare", r) for r in ACC}
    g = {k[0]: v for k, v in got.items()}
    assert sum(1 for v in got.values() if v != "AccentCare") == len(got) - 1
    assert g["Remote - Florida East 090"] == "AccentCare"
    assert g["Fort Worth TX HH"] == "AccentCare Home Health - Fort Worth"
    assert g["Tampa FL HOS"] == "AccentCare Hospice - Tampa"
    assert g["Houston TX North HOS"] == "AccentCare Hospice - Houston North"
    assert g["Austin (South) TX HH"] == "AccentCare Home Health - Austin South"
    assert g["GiddingsTX"] == "AccentCare Home Health - Giddings"
    assert g["MERIDIAN HOSPICE MS"] == "AccentCare Hospice - Meridian"
    assert g["INDIANOLA MS"] == "AccentCare Home Health - Indianola"
    assert g["Mease Manor Continuing Care FL HOS IPC"] == "AccentCare Hospice - Mease Manor Continuing Care"
    assert g["Chattanooga TN PP PCS"] == "AccentCare Personal Care - Chattanooga"
    assert g["Concord CA"] == "AccentCare Personal Care - Concord"                     # tags7, row city Walnut Creek
    assert g["King of Prussia PA HOS"] == "AccentCare Hospice - King of Prussia"
    assert g["St Paul MN HH"] == "AccentCare Home Health - Saint Paul"


def test_jibe_hook_leaves_other_boards_alone():
    r = dict(AMED[0])
    assert scraper._jibe_facility("Some Other Board", r) == "Some Other Board"


# ── VITAS: the Oracle work location is the program ─────────────────────────

def test_vitas_work_locations():
    got = [H.oracle_branch("VITAS Healthcare", j) for j in VITAS]
    names = [w["LocationName"] for j in VITAS for w in (j["workLocation"]["items"] or [{"LocationName": None}])[:1]]
    assert sum(1 for g in got if g) == 17
    pairs = dict(zip(names, got))
    assert pairs["Broward North - Main"] == "VITAS Healthcare - Broward North"
    assert pairs["Lake-Sumter - Main"] == "VITAS Healthcare - Lake-Sumter"
    assert pairs["Brevard - Melbourne"] == "VITAS Healthcare - Brevard"
    assert pairs["Chicagoland Central  - Main"] == "VITAS Healthcare - Chicagoland Central"
    assert pairs["Corporate - Miramar HR & Legal"] == ""
    assert pairs["CCC - C500, CC After-Hours"] == ""
    assert H.oracle_branch("Tenet Healthcare", VITAS[0]) == ""


def test_oracle_adapter_names_the_program(monkeypatch):
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    page = {"items": [{"TotalJobsCount": len(VITAS), "requisitionList": {"items": VITAS}}]}
    _serve(monkeypatch, page)
    jobs = asyncio.run(scraper.scrape_oracle(None, "VITAS Healthcare",
                                             ("https://ejrz.fa.us2.oraclecloud.com", "CX_5001")))
    assert len(jobs) == len(VITAS)
    by = {j.title + "|" + j.city: j.hospital_name for j in jobs}
    assert by["Social Worker - Hospice|Deerfield Beach"] == "VITAS Healthcare - Broward North"
    assert by["Hospice Aide- CNA/HHA|Joliet"] == "VITAS Healthcare"                  # no work location
    assert by["Benefits Analyst 1|Miramar"] == "VITAS Healthcare"                    # corporate
    assert all(j.hospital_system == "VITAS Healthcare" for j in jobs)


# ── CenterWell: the detail's requisition location is the branch ────────────

def test_centerwell_requisition_locations():
    got = {}
    for d in CW:
        info = d["jobPostingInfo"]
        st = info["location"].split(", ")[-1]
        got[info["jobRequisitionLocation"]["descriptor"]] = H.centerwell_branch(
            info["jobRequisitionLocation"]["descriptor"], info["title"], info.get("jobDescription") or "", st)
    assert got == {"CWHH Ballston Lk": "CenterWell Home Health - Ballston Lake",
                   "CON South Daytona": "", "CON Kirby": "",
                   "CWHH Hope": "CenterWell Home Health - Hope",
                   "CW Marietta": "CenterWell Home Health - Marietta"}
    # the descriptor as a Workday path segment ("CWHH-LANCASTER-PA")
    assert H.centerwell_branch("CWHH-LANCASTER-PA", "Registered Nurse, Home Health", "", "PA") == \
        "CenterWell Home Health - Lancaster"
    # "CW" off a home health posting names nothing
    assert H.centerwell_branch("CW West Ashley", "Field Sales Professional", "", "") == ""


def test_centerwell_detail_through_the_workday_pass():
    d = next(x for x in CW if x["jobPostingInfo"]["jobRequisitionLocation"]["descriptor"] == "CWHH Ballston Lk")
    job = _job("CenterWell", city="Clifton Park", state="NY", job_id="R-434763",
               title=d["jobPostingInfo"]["title"], ats="Workday")
    scraper._apply_wd_posting_info(job, d["jobPostingInfo"])
    assert getattr(job, H.REQ_LOC_ATTR) == "CWHH Ballston Lk" and len(job.description) > 600
    assert H.apply_facility(job) == "detail"
    assert job.hospital_name == "CenterWell Home Health - Ballston Lake"
    assert (job.city, job.state, job.hospital_system) == ("Clifton Park", "NY", "CenterWell")
    # another Workday tenant's detail stashes nothing
    other = _job("Compassus", ats="Workday")
    scraper._apply_wd_posting_info(other, d["jobPostingInfo"])
    assert getattr(other, H.REQ_LOC_ATTR, None) is None


def test_centerwell_primary_care_detail_names_nothing_and_drops_a_stored_branch():
    d = next(x for x in CW if x["jobPostingInfo"]["jobRequisitionLocation"]["descriptor"] == "CON Kirby")
    H.set_known_facilities([{"hospital_system": "CenterWell", "job_id": "R-432943",
                             "hospital_name": "CenterWell Home Health - Kirby"}])
    job = _job("CenterWell", city="San Antonio", state="TX", job_id="R-432943", title="Medical Assistant")
    scraper._apply_wd_posting_info(job, d["jobPostingInfo"])
    assert H.apply_facility(job) == "none" and job.hospital_name == "CenterWell"


def test_centerwell_body_line_and_kept_name():
    job = _body_job(38795136)
    assert H.apply_facility(job) == "text" and job.hospital_name == "CenterWell Home Health - Denver"
    # a night without a body or a detail: the stored branch is kept
    H.set_known_facilities([{"hospital_system": "CenterWell", "job_id": "R-9", "hospital_name":
                             "CenterWell Home Health - Hope"}])
    night = _job("CenterWell", city="Hope", state="AR", job_id="R-9", ats="Workday")
    assert H.apply_facility(night) == "kept" and night.hospital_name == "CenterWell Home Health - Hope"
    # a name this module never writes is not kept
    H.set_known_facilities([{"hospital_system": "CenterWell", "job_id": "R-9", "hospital_name": "Hope Clinic"}])
    night = _job("CenterWell", city="Hope", state="AR", job_id="R-9", ats="Workday")
    assert H.apply_facility(night) == "none" and night.hospital_name == "CenterWell"


def test_centerwell_reread_slot_and_known_kind(monkeypatch):
    H.set_known_facilities([{"hospital_system": "CenterWell", "job_id": "R-1", "hospital_name": "CenterWell"},
                            {"hospital_system": "CenterWell", "job_id": "R-2",
                             "hospital_name": "CenterWell Home Health - Hope"}])
    day0 = scraper.datetime(2026, 11, 1).toordinal()
    days = [d for d in range(day0, day0 + H.REREAD_CYCLE) if H.reread_slot("CenterWell", "R-1", d)]
    assert len(days) == 1                                         # one night a cycle
    assert not any(H.reread_slot("CenterWell", "R-2", d) for d in range(day0, day0 + 9))   # already named
    assert not H.reread_slot("BrightSpring Health Services", "R-1", days[0])
    late = scraper.datetime(2026, 11, 21).toordinal()
    assert not any(H.reread_slot("CenterWell", "R-1", d) for d in range(late, late + 9))  # after REREAD_UNTIL
    # the detail pass queues the label row for its branch on that night
    scraper.set_known_bodies([{"hospital_system": "CenterWell", "job_id": "R-1", "desc_len": 6000, "fv": "4",
                               "ats_platform": "Workday"}])
    try:
        monkeypatch.setattr(scraper, "_RUN_DAY", [days[0]])
        monkeypatch.setattr(scraper, "_force_slot", lambda *a, **k: False)
        job = _job("CenterWell", job_id="R-1", ats="Workday")
        assert scraper._known_kind("CenterWell", job) == "facility"
        monkeypatch.setattr(scraper, "_RUN_DAY", [days[0] + 1])
        assert scraper._known_kind("CenterWell", job) != "facility"
    finally:
        scraper.set_known_bodies([])


# ── BrightSpring and Compassus: the company the body names ─────────────────

def test_brightspring_company_lines():
    want = {38779984: "Rehab Without Walls Neuro Rehabilitation", 38779659: "PharMerica",
            38779160: "Adoration Health", 38779604: "Amerita", 38779186: "Embrace Hospice",
            38780418: "Adoration Health",        # teaser "... Adoration Home Health is"
            38779726: "Adoration Health",        # "Adoration Health is expanding in Tarboro, NC!"
            38779537: "Abode Care Partners", 38779934: "PharMerica",
            38780204: "", 38778605: "", 38778618: "", 38779469: "",
            38779041: ""}                        # "Our Company: BrightSpring Health Services"
    for rid, name in want.items():
        assert H.brightspring_brand(BODIES[rid]["description"])[0] == name, rid


def test_brightspring_apply_and_keep():
    job = _body_job(38779186)
    assert H.apply_facility(job) == "text" and job.hospital_name == "Embrace Hospice"
    assert (job.city, job.state) == (BODIES[38779186]["city"], BODIES[38779186]["state"])
    # the system's own line: label, nothing logged
    job = _body_job(38779041)
    assert H.apply_facility(job) == "none" and job.hospital_name == "BrightSpring Health Services"
    assert H.UNKNOWN == {}
    # no body tonight: the stored company is kept; an unknown company is logged, not kept
    H.set_known_facilities([{"hospital_system": "BrightSpring Health Services", "job_id": "B1",
                             "hospital_name": "Haven Hospice"}])
    night = _job("BrightSpring Health Services", city="GAINESVILLE", state="FL", job_id="B1", ats="iCIMS")
    assert H.apply_facility(night) == "kept" and night.hospital_name == "Haven Hospice"
    body = "Our Company\n\nSome New Brand Hospice\n\nOverview\n\n" + "x " * 400
    job = _job("BrightSpring Health Services", job_id="B1", description=body)
    assert H.apply_facility(job) == "none" and job.hospital_name == "BrightSpring Health Services"
    assert H.UNKNOWN == {"BrightSpring Health Services": {"Some New Brand Hospice": 1}}


def test_compassus_company_lines():
    want = {1582898: "Ascension at Home together with Compassus",
            5774177: "Bon Secours Home Care and Hospice by Compassus",
            2670843: "Mercy Health Home Care and Hospice by Compassus",
            2880725: "OhioHealth at Home in partnership with Compassus",
            1582695: "Providence at Home with Compassus",
            1582499: "", 38801506: "", 38802269: ""}
    for rid, name in want.items():
        job = _body_job(rid)
        H.apply_facility(job)
        assert job.hospital_name == (name or "Compassus"), rid


def test_known_names_only_this_module_writes():
    H.set_known_facilities([
        {"hospital_system": "Compassus", "job_id": "1", "hospital_name": "Providence at Home with Compassus"},
        {"hospital_system": "Compassus", "job_id": "2", "hospital_name": "Compassus"},
        {"hospital_system": "BrightSpring Health Services", "job_id": "3", "hospital_name": "Some Clinic"},
        {"hospital_system": "Amedisys", "job_id": "4", "hospital_name": "Amedisys Hospice - Largo"},
        {"hospital_system": "CenterWell", "job_id": "5", "hospital_name": "CenterWell"},
    ])
    assert H.known_facility("Compassus", "1") == "Providence at Home with Compassus"
    assert H.known_facility("Compassus", "2") == "" and H.known_facility("BrightSpring Health Services", "3") == ""
    assert H.known_facility("Amedisys", "4") == ""        # a list chain: its feed names it every night
    assert ("CenterWell", "5") in H._ON_LABEL


def test_load_without_credentials_is_empty(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert H.load_known_facilities() == 0 and H._KNOWN == {} and H._ON_LABEL == set()


def test_other_systems_untouched():
    for system in ("BAYADA Home Health Care", "Amedisys", "Aveanna Healthcare", "Advocate Health"):
        job = _job(system, "X Y", description="Our Company\n\nPharMerica\n\n" + "x " * 400)
        assert H.apply_facility(job) == "" and job.hospital_name == "X Y"
    job = _job("Compassus", "Some Adapter Name", description="Company:\nProvidence at Home with Compassus")
    assert H.apply_facility(job) == "other" and job.hospital_name == "Some Adapter Name"


# ── Elara Caring: a lost town read back from the location text ─────────────

@pytest.mark.parametrize("text,state,want", [
    ("Brooklyn---NY-Bay-Ridge", "NY", ("Brooklyn", "NY")),
    ("Brooklyn - NY Bay Ridge", "NY", ("Brooklyn", "NY")),
    ("Valparaiso---IN-SHH", "IN", ("Valparaiso", "IN")),
    ("New-Britain-CT-W-Main-St", "CT", ("New Britain", "CT")),
    ("OKLAHOMA-CITY---OK-Trospher-Park", "OK", ("Oklahoma City", "OK")),
    ("Copperas-Cove-TX---General-Area", "TX", ("Copperas Cove", "TX")),
    ("Abilene---TX-Hospital", "TX", ("Abilene", "TX")),
    ("Kansas-City-North-Kansas-City", "MO", None),     # no state code: two towns, none taken
    ("Big-Sandy---TX", "TX", None),                     # not a known town: never guessed
    ("Brooklyn---NY-Bay-Ridge", "NJ", None),            # another state than the row's
])
def test_elara_town(text, state, want):
    assert H.elara_town(text, state) == want


ELARA = _load("elara_wd_list.json")


def _crawl_elara(monkeypatch, postings):
    from test_wd_tenant_facets import _fake_req
    page = {"total": len(postings), "jobPostings": postings}
    _fake_req(monkeypatch, lambda b: page if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(postings), "jobPostings": []})
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    return asyncio.run(scraper.scrape_workday(None, "Elara Caring", tuple(ELARA["tenant"])))


def test_elara_multi_site_postings_are_left_to_the_existing_rules(monkeypatch):
    # "10 Locations" postings are never read by elara_town: whatever the
    # tenant rules give them (the primary site's town where facilities_workday
    # reads it, else none, 2026-10-13) is the same with and without the hook.
    jobs = _crawl_elara(monkeypatch, ELARA["jobPostings"])
    by = {j.url.rsplit("/", 1)[-1]: scraper.normalize_job(j) for j in jobs}
    monkeypatch.setattr(H, "WD_TOWN_SYSTEMS", frozenset())
    base = {j.url.rsplit("/", 1)[-1]: scraper.normalize_job(j) for j in _crawl_elara(monkeypatch, ELARA["jobPostings"])}
    multi = [p for p in ELARA["jobPostings"] if p["locationsText"].endswith("Locations")]
    assert len(multi) == 19
    for p in multi:
        k = p["externalPath"].rsplit("/", 1)[-1]
        assert (by[k]["city"], by[k]["state"]) == (base[k]["city"], base[k]["state"]), k
        assert by[k]["state"] == "NY"
    single = by["Home-Health-Aide-HHA_JR-139650"]
    assert (single["city"], single["state"]) == ("Dobbs Ferry", "NY")
    assert all(r["hospital_name"] == "Elara Caring" for r in by.values())


def test_elara_single_site_with_a_suffix_gets_its_town(monkeypatch):
    # the stored link /job/Abilene---TX-Hospital/... is the site "Abilene - TX Hospital"
    base = dict(ELARA["jobPostings"][11])
    posts = [dict(base, locationsText="Abilene - TX Hospital", bulletFields=["Texas", "JR-138100"],
                  externalPath="/job/Abilene---TX-Hospital/Caregiver-Personal-Care-Aide-PCA_JR-138100"),
             dict(base, locationsText="Valparaiso - IN SHH", bulletFields=["Indiana", "JR-139500"],
                  externalPath="/job/Valparaiso---IN-SHH/Physical-Therapist-Assistant_JR-139500"),
             dict(base, locationsText="Big Sandy - TX", bulletFields=["Texas", "JR-140034"],
                  externalPath="/job/Big-Sandy---TX/Caregiver-Personal-Care-Aide-PCA_JR-140034")]
    rows = {j.job_id: scraper.normalize_job(j) for j in _crawl_elara(monkeypatch, posts)}
    assert (rows["JR-138100"]["city"], rows["JR-138100"]["state"]) == ("Abilene", "TX")
    assert (rows["JR-139500"]["city"], rows["JR-139500"]["state"]) == ("Valparaiso", "IN")
    assert rows["JR-140034"]["state"] == "TX"          # the town the list does not know is not guessed here


# ── Through finalize_jobs (the upsert payload) ─────────────────────────────

def test_finalize_jobs_payload(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    scraper.set_known_bodies([])
    H.set_known_facilities([{"hospital_system": "CenterWell", "job_id": "R-9",
                             "hospital_name": "CenterWell Home Health - Hope"}])
    rows = scraper.finalize_jobs([
        _job("CenterWell", city="Hope", state="AR", job_id="R-9", title="Home Health RN", ats="Workday"),
        _body_job(38779186),
        _body_job(1582695),
        _job("BAYADA Home Health Care", "BAYADA Home Health Care - Millville", "Millville", "NJ", job_id="G1",
             title="Home Health Aide", ats="Greenhouse"),
        _job("Aveanna Healthcare", city="Kokomo", state="IN", job_id="167978", title="Registered Nurse (RN)",
             ats="SitemapBoard"),
    ])
    by = {r["job_id"]: r for r in rows}
    assert (by["R-9"]["hospital_name"], by["R-9"]["city"], by["R-9"]["state"]) == \
        ("CenterWell Home Health - Hope", "Hope", "AR")
    assert by[BODIES[38779186]["job_id"]]["hospital_name"] == "Embrace Hospice"
    assert by[BODIES[1582695]["job_id"]]["hospital_name"] == "Providence at Home with Compassus"
    assert by["G1"]["hospital_name"] == "BAYADA Home Health Care - Millville"
    assert by["167978"]["hospital_name"] == "Aveanna Healthcare"
    assert all(by[k]["hospital_system"] for k in by)
