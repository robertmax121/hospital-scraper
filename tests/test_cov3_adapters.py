"""cov3 (2026-10-04): three new adapter families, each from one captured
response. PeopleSoft HCM Fluid (NYC Health + Hospitals, The Queen's Health
Systems), LiquidCompass (UnityPoint Health) and SAP SuccessFactors career4
DWR (Johns Hopkins Health System). No network: the list sessions, the DWR
calls and `_curl_fetch` / `_curl_html` are replaced."""
import asyncio
import inspect
import json
import os

import scraper
from scraper import Job

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
NYC = "https://careers.nychhc.org/psc/hrtam/EMPLOYEE/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"


def _read(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


# ── PeopleSoft ────────────────────────────────────────────────────────────

def test_ps_list_rows_total_and_facets():
    html = _read("peoplesoft_list.html")
    rows = scraper._ps_rows(html)
    assert [r["HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID"] for r in rows] == ["140415", "140398"]
    assert rows[0]["SCH_JOB_TITLE"] == "Assistant Coordinating Manager"
    assert rows[0]["HRS_BU_DESCR"] == "QUEENS" and rows[0]["LOCATION"] == "Queens"
    assert rows[0]["SCH_OPENED"] == "10/02/2026"
    # the banner's count is the real total; the fixture's grid holds 500 rows (the display cap)
    assert scraper._ps_total(html) == (1918, True)
    assert scraper._ps_total("<span class='ps-text' id='x'>no banner</span>") == (0, False)
    assert scraper._ps_total("psc_rowcount' id='win0divHRS_AGNT_RSLT_Irowcnt$0'><span class='ps-text'>500 rows</span>") == (500, False)
    assert scraper._ps_total("psc_rowcount' id='win0divHRS_AGNT_RSLT_Irowcnt$0'><span class='ps-text'>409 rows</span>") == (409, True)
    facets = scraper._ps_facets(html)
    assert [(t, lab) for t, lab, _ in facets] == [("HRS_RECR_LOC_ALL", "Location"), ("HRS_JOB_POSTED_HRS_POSTED_IN", "Job Posted In")]
    top = scraper._ps_partition_nodes(facets[0][2], leaves=False)
    assert [(n["path"], n["count"]) for n in top] == [("Manhattan", 819), ("Bronx", 409)]
    assert top[0]["text"] == "Manhattan (819)"
    leaves = scraper._ps_partition_nodes(facets[1][2], leaves=True)
    assert [(n["path"], n["count"]) for n in leaves] == [("2026/10", 93), ("2026/09", 875), ("2026/08", 399)]
    assert scraper._PS_MORE in html
    hidden = scraper._ps_hidden(html)
    assert hidden["ICSID"] and hidden["ICStateNum"] == "3" and hidden["ICAction"] == "None"


def test_ps_job_facility_city_and_url():
    html = _read("peoplesoft_list.html")
    rows = scraper._ps_rows(html)
    j = scraper._ps_job(rows[0], "NYC Health + Hospitals", NYC, "NY")
    assert j.hospital_system == "NYC Health + Hospitals" and j.hospital_name == "Queens Hospital Center"
    assert (j.city, j.state, j.location) == ("Jamaica", "NY", "Jamaica, NY")
    assert j.posted_date == "2026-10-02" and j.job_id == "140415" and j.ats_platform == "PeopleSoft"
    assert j.url == NYC + "?Page=HRS_APP_JBPST_FL&Action=U&FOCUS=Applicant&SiteId=1&JobOpeningId=140415&PostingSeq=1"
    # NYC: an unmapped business unit is title-cased and the borough becomes the city (Manhattan -> New York)
    r = {"HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID": "7", "SCH_JOB_TITLE": "Clerk", "HRS_BU_DESCR": "MORRISANIA", "LOCATION": "Bronx"}
    j = scraper._ps_job(r, "NYC Health + Hospitals", NYC, "NY")
    assert (j.hospital_name, j.city) == ("Morrisania", "Bronx")
    r["LOCATION"] = "Manhattan"
    assert scraper._ps_job(r, "NYC Health + Hospitals", NYC, "NY").city == "New York"
    # Queen's: the Location column is the facility; the map gives the CMS spelling and the town
    q = {"HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID": "157942", "SCH_JOB_TITLE": "RN - Emergency",
         "LOCATION": "*Queen&#039;s Medical Ctr-Honolulu", "SCH_OPENED": "10/03/2026"}
    j = scraper._ps_job(q, "The Queen's Health Systems", "https://hrweb.queens.org/x.GBL", "HI")
    assert (j.hospital_name, j.city, j.state) == ("The Queens Medical Center", "Honolulu", "HI")
    q["LOCATION"] = "North Hawaii Community Hosp"
    assert scraper._ps_job(q, "The Queen's Health Systems", "https://hrweb.queens.org/x.GBL", "HI").hospital_name == "North Hawaii Community Hospital"
    q["LOCATION"] = "Some New Clinic"
    j = scraper._ps_job(q, "The Queen's Health Systems", "https://hrweb.queens.org/x.GBL", "HI")
    assert (j.hospital_name, j.city, j.state) == ("Some New Clinic", "", "HI")
    assert scraper._ps_job({"HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID": "", "SCH_JOB_TITLE": "x"}, "S", NYC, "NY") is None
    assert scraper._ps_title("SOUTH BROOKLYN HEALTH") == "South Brooklyn Health"
    assert scraper._ps_title("*The Queen's Med Ctr-Kahi") == "The Queen's Med Ctr-Kahi"


def test_ps_posting_body_and_type():
    body, et = scraper._ps_posting(_read("peoplesoft_posting.html"))
    assert et == "Full-Time"
    assert body.startswith("About NYC Health + Hospitals")
    assert "Duties & Responsibilities" in body and len(body) > 2000
    assert "<DIV" not in body and "ps_box-button" not in body   # stops at the Apply button
    job = Job(title="T", hospital_system="NYC Health + Hospitals", hospital_name="Queens Hospital Center", city="Jamaica",
              state="NY", location="Jamaica, NY", specialty="", job_type="", url=NYC + "?Page=HRS_APP_JBPST_FL&JobOpeningId=140415",
              job_id="140415", posted_date="", description="", ats_platform="PeopleSoft")

    async def fake_html(url, impersonate="chrome", timeout=25):
        assert url == job.url and impersonate == "chrome"
        return _read("peoplesoft_posting.html")

    import pytest
    mp = pytest.MonkeyPatch()
    mp.setattr(scraper, "_curl_html", fake_html)
    try:
        assert asyncio.run(scraper._peoplesoft_detail(None, job)) is True
    finally:
        mp.undo()
    assert job.job_type == "Full-Time" and len(job.description) > 2000


class _FakePS:
    """A scripted list session: pages[(selections)] -> [first page, chunk 1, chunk 2 ...]."""
    script: dict = {}
    opened: list = []
    posts: list = []

    def __init__(self, base):
        self.base = base
        self.sel = ()
        self.i = 0
        _FakePS.opened.append(base)

    def first(self):
        return _FakePS.script[self.sel][0]

    def post(self, action, extra=None):
        _FakePS.posts.append((self.sel, action, extra))
        if action == "PTS_TREEFACETCHG":
            self.sel = self.sel + (extra["PTS_TREEFACETCHG"],)
            self.i = 0
            return _FakePS.script[self.sel][0]
        self.i += 1
        pages = _FakePS.script[self.sel]
        return pages[self.i] if self.i < len(pages) else ""


def _page(ids, rowcnt, more=True, banner=None, facets=""):
    rows = "".join(
        f"<li id='HRS_AGNT_RSLT_I$0_row_{n}'><span class='ps_box-value'   id='SCH_JOB_TITLE${n}' >Job {i}</span>"
        f"<span class='ps_box-value'   id='HRS_APP_JBSCH_I_HRS_JOB_OPENING_ID${n}' >{i}</span>"
        f"<span class='ps_box-value'   id='LOCATION${n}' >Bronx</span><span class='ps_box-value'   id='HRS_BU_DESCR${n}' >JACOBI</span></li>"
        for n, i in enumerate(ids))
    head = f"<b>{banner}</b> jobs found. Only the first <b>500</b> jobs can be displayed." if banner else ""
    return (f"{head}{facets}<DIV class='psc_rowcount' id='win0divHRS_AGNT_RSLT_Irowcnt$0'><span class='ps-text'>{rowcnt} rows</span></DIV>"
            f"<ul>{rows}</ul>" + ("<div class='ps_box-more' onclick=\"submitAction_win0(document.win0,'HRS_AGNT_RSLT_I$hdown$0');\">more</div>" if more else ""))


def _tree(tree, label, nodes):
    data = [{"id": f"t{i}", "path": p, "val": p, "text": f"{p} ({c})", "type": "node", "children": ch}
            for i, (p, c, ch) in enumerate(nodes)]
    return (f'<oj-tree-view id="{tree}" aria-label="{label}"><script id="{tree}_script">var {tree}_data= '
            + json.dumps(data) + "</script></oj-tree-view>")


def test_ps_crawl_chunks_one_small_list(monkeypatch):
    _FakePS.script = {(): [_page(range(1, 51), 60), _page(range(1, 61), 60, more=False)]}
    _FakePS.opened, _FakePS.posts = [], []
    monkeypatch.setattr(scraper, "_ps_open", lambda base: (lambda s: (s, s.first()))(_FakePS(base)))
    monkeypatch.setattr(scraper, "_ps_pause", lambda: None)
    rows, complete = scraper._ps_crawl("The Queen's Health Systems", "https://hrweb.queens.org/x.GBL")
    assert len(rows) == 60 and complete
    assert [p[1] for p in _FakePS.posts] == ["HRS_AGNT_RSLT_I$hdown$0"]
    assert _FakePS.opened == ["https://hrweb.queens.org/x.GBL"]


def test_ps_crawl_partitions_a_capped_list_by_location_then_month(monkeypatch):
    loc = _tree("HRS_RECR_LOC_ALL", "Location", [("Manhattan", 600, []), ("Bronx", 30, [])])
    months = _tree("HRS_JOB_POSTED_HRS_POSTED_IN", "Job Posted In",
                   [("2026", 600, [{"id": "m1", "path": "2026/10", "text": "10 (300)", "type": "leaf"},
                                   {"id": "m2", "path": "2026/09", "text": "09 (300)", "type": "leaf"}])])
    man = "HRS_RECR_LOC_ALL.HRS_RECR_LOC_ALL:Manhattan (600)<Manhattan>>"
    bronx = "HRS_RECR_LOC_ALL.HRS_RECR_LOC_ALL:Bronx (30)<Bronx>>"
    m10 = "HRS_JOB_POSTED_HRS_POSTED_IN.HRS_JOB_POSTED_HRS_POSTED_IN:10 (300)<2026/10>>"
    m09 = "HRS_JOB_POSTED_HRS_POSTED_IN.HRS_JOB_POSTED_HRS_POSTED_IN:09 (300)<2026/09>>"
    _FakePS.script = {
        (): [_page(range(1, 51), 500, facets=loc + months)],                      # capped, no banner on the first page
        (man,): [_page(range(1, 51), 500, banner=600, facets=loc + months)],        # still over the cap: cut by month
        (man, m10): [_page(range(1, 51), 100, banner=100), _page(range(1, 101), 100, more=False)],
        (man, m09): [_page(range(1001, 1051), 100, banner=100), _page(range(1001, 1101), 100, more=False)],
        (bronx,): [_page(range(5001, 5031), 30, more=False)],
    }
    _FakePS.opened, _FakePS.posts = [], []
    monkeypatch.setattr(scraper, "_ps_open", lambda base: (lambda s: (s, s.first()))(_FakePS(base)))
    monkeypatch.setattr(scraper, "_ps_pause", lambda: None)
    rows, complete = scraper._ps_crawl("NYC Health + Hospitals", NYC)
    assert len(rows) == 230 and complete
    assert len(_FakePS.opened) == 5                      # the root, Manhattan, its two months, Bronx
    facet_posts = [p[2]["PTS_TREEFACETCHG"] for p in _FakePS.posts if p[1] == "PTS_TREEFACETCHG"]
    assert facet_posts == [man, man, m10, man, m09, bronx]
    assert sum(1 for p in _FakePS.posts if p[1] == "HRS_AGNT_RSLT_I$hdown$0") == 2


def test_scrape_peoplesoft_marks_partial_when_the_site_withholds_rows(monkeypatch):
    _FakePS.script = {(): [_page(range(1, 51), 500, more=False)]}   # capped, no facets to cut by
    _FakePS.opened, _FakePS.posts = [], []
    monkeypatch.setattr(scraper, "_ps_open", lambda base: (lambda s: (s, s.first()))(_FakePS(base)))
    monkeypatch.setattr(scraper, "_ps_pause", lambda: None)
    jobs = asyncio.run(scraper.scrape_peoplesoft(None, "NYC Health + Hospitals", (NYC, "NY")))
    assert len(jobs) == 50 and jobs[0].hospital_name == "Jacobi Medical Center" and jobs[0].state == "NY"
    assert "NYC Health + Hospitals" in scraper.PARTIAL_SYSTEMS

    def boom(base):
        raise RuntimeError("HTTP 403")
    monkeypatch.setattr(scraper, "_ps_open", boom)
    assert asyncio.run(scraper.scrape_peoplesoft(None, "NYC Health + Hospitals", (NYC, "NY"))) == []


# ── LiquidCompass ─────────────────────────────────────────────────────────

def test_liquidcompass_result_to_job():
    d = json.loads(_read("liquidcompass_search.json"))
    assert (d["total_count"], d["page_count"], d["current_page"]) == (1693, 85, 1)
    j = scraper._liquidcompass_job(d["results"][0], "UnityPoint Health", "IA")
    assert j.title == "Patient Access Team Lead" and j.job_id == "52547992"
    assert j.hospital_system == "UnityPoint Health" and j.hospital_name == "UnityPoint Health - Marshalltown"
    assert (j.city, j.state, j.location) == ("Marshalltown", "IA", "Marshalltown, IA")
    assert j.url == "https://careers.unitypoint.org/job/52547992/patient-access-mgr"
    assert j.posted_date == "2026-10-03" and j.ats_platform == "LiquidCompass"
    assert j.job_type == "" and j.description.startswith("Area of Interest: Patient Services")   # "Not Stated" dropped
    k = scraper._liquidcompass_job(d["results"][1], "UnityPoint Health", "IA")
    assert k.hospital_name == "DBQ Internal Medicine Clinic" and k.city == "Dubuque"
    # abbreviations expanded, a blank state takes the tenant default, inactive and id-less rows drop
    r = {"id": 5, "raw_title": "RN", "location_name": "Fort Dodge Trinity Reg Med Ctr", "city": "Fort Dodge", "state": "", "detail_url": "https://careers.unitypoint.org/job/5/rn"}
    j = scraper._liquidcompass_job(r, "UnityPoint Health", "IA")
    assert (j.hospital_name, j.state) == ("Fort Dodge Trinity Regional Medical Center", "IA")
    assert scraper._lc_facility("UnityPoint Health", "Waterloo Allen Hosp") == "Allen Hospital"   # mapped to the CMS name
    assert scraper._liquidcompass_job({**r, "is_active": False}, "UnityPoint Health", "IA") is None
    assert scraper._liquidcompass_job({**r, "id": ""}, "UnityPoint Health", "IA") is None
    assert scraper._liquidcompass_job({**r, "detail_url": ""}, "UnityPoint Health", "IA") is None
    assert scraper._lc_facility("UnityPoint Health", "Grinnell Reg Med Cntr Corp") == "Grinnell Regional Medical Center"


class _Resp:
    def __init__(self, payload):
        self._p = payload
        self.status_code = 200

    def json(self):
        return self._p


def test_scrape_liquidcompass_pages_to_page_count(monkeypatch):
    d = json.loads(_read("liquidcompass_search.json"))
    base = d["results"][0]
    calls = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["paged"])
        calls.append((url, kw["params"]))
        results = [{**base, "id": page * 100 + i, "detail_url": f"https://careers.unitypoint.org/job/{page * 100 + i}/x"} for i in range(20 if page < 3 else 5)]
        return _Resp({"total_count": 45, "page_count": 3, "count": len(results), "current_page": page, "results": results})

    async def no_jitter():
        return None

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_liquidcompass(None, "UnityPoint Health", ("unitypoint", "IA")))
    assert len(jobs) == 45 and len({j.job_id for j in jobs}) == 45
    assert [c[1]["paged"] for c in calls] == ["1", "2", "3"]
    assert calls[0][0] == scraper.LIQUIDCOMPASS_API and calls[0][1]["partner"] == "unitypoint" and calls[0][1]["update_results"] == "true"

    def failing(method, url, impersonate, timeout=60, **kw):
        if kw["params"]["paged"] == "2":
            raise RuntimeError("HTTP 502 direct")
        return fake_fetch(method, url, impersonate, timeout, **kw)
    monkeypatch.setattr(scraper, "_curl_fetch", failing)
    assert len(asyncio.run(scraper.scrape_liquidcompass(None, "UnityPoint Health", ("unitypoint", "IA")))) == 20


# ── SuccessFactors ────────────────────────────────────────────────────────

SEARCH_REPLY = '''throw 'allowScriptTagRemoting is false.';
//#DWR-INSERT
//#DWR-REPLY
var s0={};var s1={};var s2={};var s3={};var s4=[];var s5={};var s6=[];var s7=[];var s8={};var s9={};var s10={};
s0.postingCount="2";
s1.options=s2;s1.postingCount=2;s1.postings=s4;s1.detailURLPrefix="\\/career?career%5fns=job%5flisting&company=SFHUP";
s2.pagination=s3;s2.sortByColumn="JOB_POSTING_DATE";s2.sortOrder="DESC";
s3.currentPage=2;s3.endRow=200;s3.increaseCandSummaryPagination=false;s3.pageSize=100;s3.startRow=101;s3.totalCount=1007;
s4[0]=s5;s4[1]=s9;
s5.corporatePosting=true;s5.id=999001;s5.multiLingualTitles=null;s5.otherValues=s6;s5.postingDate="10\\/02\\/2026";s5.title="Registered Nurse - RN \\"Peds\\" ED";
s6[0]=s7;
s7[0]=s8;s7[1]=s10;
s8.fieldId="filter2";s8.internalVal=null;s8.longVal=null;s8.shortVal="Johns Hopkins All Children\\'s Hospital";
s10.fieldId="filter7";s10.internalVal=null;s10.longVal=null;s10.shortVal="St. Petersburg, FL";
s9.corporatePosting=true;s9.id=1;s9.otherValues=null;s9.postingDate="";s9.title="";

dwr.engine._remoteHandleCallback('0','0',{filters:s0,results:s1});
'''

EXC_REPLY = '''throw 'allowScriptTagRemoting is false.';
//#DWR-INSERT
//#DWR-REPLY
dwr.engine._remoteHandleException('0','0',{"errorType":"SFDWRException","message":"An application error occurred. Please try again later.","timestamp":"2026-10-04T16:03:08.577+0000"});
'''


def test_dwr_parse_initial_reply_fixture():
    d = scraper._dwr_parse(_read("successfactors_dwr.txt"))
    res = scraper._sf_results(d)
    assert res["options"]["pagination"] == {"currentPage": 1, "endRow": 10, "increaseCandSummaryPagination": False,
                                            "pageSize": 10, "startRow": 1, "totalCount": 1007}
    assert res["options"]["sortByColumn"] == "JOB_POSTING_DATE" and len(res["postings"]) == 10
    p = res["postings"][0]
    assert (p["id"], p["title"], p["postingDate"]) == (679292, "New Grad RN - Cardiovascular Progressive Care Unit", "10/04/2026")
    assert p["otherValues"][0][2] == {"fieldId": "filter2", "internalVal": None, "longVal": None, "shortVal": "Johns Hopkins Hospital"}
    assert d["payload"]["filters"]["postingCount"] == "1,007"
    assert d["payload"]["filters"]["userValues"]["picklistSelectedValues"]["customFilter_filter8"] == []
    assert res["detailURLPrefix"].startswith("/career?career%5fns=job%5flisting&company=SFHUP")


def test_dwr_parse_search_reply_and_exception():
    d = scraper._dwr_parse(SEARCH_REPLY)
    res = scraper._sf_results(d)
    assert res["options"]["pagination"]["currentPage"] == 2 and len(res["postings"]) == 2
    assert res["postings"][0]["title"] == 'Registered Nurse - RN "Peds" ED' and res["postings"][0]["postingDate"] == "10/02/2026"
    assert res["postings"][0]["otherValues"][0][1]["shortVal"] == "St. Petersburg, FL"
    assert scraper._sf_results({}) == {} and scraper._sf_results(None) == {}
    import pytest
    with pytest.raises(RuntimeError, match="application error"):
        scraper._dwr_parse(EXC_REPLY)
    with pytest.raises(RuntimeError, match="without a callback"):
        scraper._dwr_parse("var s0={};s0.a=1;")


def test_dwr_call_body_matches_the_engine_format():
    call = scraper._DwrCall("search", "/career?company=SFHUP&_s.crb=ab+c=", "JSID1")
    call.param({"pagination": {"currentPage": 2, "pageSize": 100, "flag": False, "none": None}, "sortOrder": "DESC"})
    body = call.body()
    lines = body.split("\n")
    assert lines[:5] == ["callCount=1", "nextReverseAjaxIndex=0", "c0-scriptName=careerJobSearchControllerProxy", "c0-methodName=search", "c0-id=0"]
    assert "c0-e1=number:2" in lines and "c0-e2=number:100" in lines and "c0-e3=boolean:false" in lines and "c0-e4=null:null" in lines
    assert "c0-e5=Object_Object:{currentPage:reference:c0-e1, pageSize:reference:c0-e2, flag:reference:c0-e3, none:reference:c0-e4}" in lines
    assert "c0-e6=string:DESC" in lines
    assert "c0-param0=Object_Object:{pagination:reference:c0-e5, sortOrder:reference:c0-e6}" in lines
    assert "page=%2Fcareer%3Fcompany%3DSFHUP%26_s.crb%3Dab%2Bc%3D" in lines and "httpSessionId=JSID1" in lines
    assert any(l.startswith("scriptSessionId=" + scraper.SF_SCRIPT_SESSION) and len(l) == len("scriptSessionId=" + scraper.SF_SCRIPT_SESSION) + 3 for l in lines)
    assert lines[-1] == "windowName=" and "batchId=0" in lines
    call2 = scraper._DwrCall("getInitialJobSearchData", "/p", "")
    call2.param({"filterOnly": "", "returnToList": False, "tz": "America/New_York"})
    assert "c0-e3=string:America%2FNew_York" in call2.body().split("\n")


def test_sf_posting_to_job():
    d = scraper._dwr_parse(_read("successfactors_dwr.txt"))
    postings = scraper._sf_results(d)["postings"]
    j = scraper._sf_job(postings[0], "Johns Hopkins Health System", "https://career4.successfactors.com", "SFHUP", "MD", "filter2")
    assert j.title == "New Grad RN - Cardiovascular Progressive Care Unit" and j.job_id == "679292"
    assert j.hospital_system == "Johns Hopkins Health System" and j.hospital_name == "Johns Hopkins Hospital"
    assert (j.city, j.state, j.location) == ("Baltimore", "MD", "Baltimore, MD")
    assert j.job_type == "Full Time" and j.posted_date == "2026-10-04" and j.ats_platform == "SuccessFactors"
    assert j.url == ("https://career4.successfactors.com/career?career_ns=job_listing&company=SFHUP&navBarLevel=JOB_SEARCH"
                     "&rcm_site_locale=en_US&career_job_req_id=679292&selected_lang=en_US")
    names = {scraper._sf_job(p, "JH", "https://h", "SFHUP", "MD", "filter2").hospital_name for p in postings}
    assert {"Johns Hopkins Hospital", "Sibley Memorial Hospital", "Suburban Hospital", "Johns Hopkins Bayview Medical Center"} <= names
    dc = [scraper._sf_job(p, "JH", "https://h", "SFHUP", "MD", "filter2") for p in postings if "Sibley" in str(p)]
    assert dc and (dc[0].city, dc[0].state) == ("Washington", "DC")
    fl = scraper._sf_results(scraper._dwr_parse(SEARCH_REPLY))["postings"]
    j = scraper._sf_job(fl[0], "JH", "https://h", "SFHUP", "MD", "filter2")
    assert (j.hospital_name, j.city, j.state, j.job_type) == ("Johns Hopkins All Children's Hospital", "St. Petersburg", "FL", "")
    assert scraper._sf_job(fl[1], "JH", "https://h", "SFHUP", "MD", "filter2") is None
    # no location value anywhere: the tenant default state, the system as facility
    j = scraper._sf_job({"id": 9, "title": "Cook", "otherValues": [[{"fieldId": "filter1", "shortVal": "Food"}]]}, "JH", "https://h", "SFHUP", "MD", "filter2")
    assert (j.hospital_name, j.city, j.state) == ("JH", "", "MD")
    # the list's 40-character cut of a facility name is restored
    j = scraper._sf_job({"id": 10, "title": "RN", "otherValues": [[{"fieldId": "filter2", "shortVal": "Johns Hopkins Howard County Medical C..."}]]}, "JH", "https://h", "SFHUP", "MD", "filter2")
    assert j.hospital_name == "Johns Hopkins Howard County Medical Center"
    j = scraper._sf_job({"id": 11, "title": "RN", "otherValues": [[{"fieldId": "filter2", "shortVal": "Some Unknown Ambulatory Surgery Cent..."}]]}, "JH", "https://h", "SFHUP", "MD", "filter2")
    assert j.hospital_name == "Some Unknown Ambulatory Surgery Cent"


def test_sf_list_pages_the_session_search(monkeypatch):
    calls = []
    init = scraper._dwr_parse(_read("successfactors_dwr.txt"))
    page2 = scraper._dwr_parse(SEARCH_REPLY)

    def fake_open(host, company):
        assert (host, company) == ("https://career4.successfactors.com", "SFHUP")
        return "SESSION", "crb=", "JSID", "/career?company=SFHUP&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&_s.crb=crb="

    def fake_call(sess, host, crb, jsid, page, method, *params):
        calls.append((method, params))
        if method == "getInitialJobSearchData":
            return init
        n = params[0]["pagination"]["currentPage"]
        if n == 1:
            return {"filters": {}, "results": {"postings": scraper._sf_results(init)["postings"]}}
        if n == 2:
            return page2
        return {"filters": {}, "results": {"postings": []}}

    monkeypatch.setattr(scraper, "_sf_open", fake_open)
    monkeypatch.setattr(scraper, "_sf_call", fake_call)
    monkeypatch.setattr(scraper, "_sf_pause", lambda: None)
    monkeypatch.setattr(scraper, "SF_PAGE_CAP", 3)
    jobs, total = scraper._sf_list("Johns Hopkins Health System", "https://career4.successfactors.com", "SFHUP", "MD", "filter2")
    assert total == 1007 and len(jobs) == 11 and len({j.job_id for j in jobs}) == 11
    assert [c[0] for c in calls] == ["getInitialJobSearchData", "search", "search", "search"]
    assert calls[0][1][0] == {"filterOnly": "", "jobAlertId": "", "returnToList": False, "browserTimeZone": "America/New_York"}
    opts = calls[2][1][0]
    assert opts["pagination"] == {"currentPage": 2, "endRow": 200, "increaseCandSummaryPagination": False, "pageSize": 100, "startRow": 101, "totalCount": 1007}
    assert (opts["sortByColumn"], opts["sortOrder"]) == ("JOB_POSTING_DATE", "DESC")
    # the async wrapper: a short crawl of a big site is PARTIAL; an exception is a logged empty list
    jobs = asyncio.run(scraper.scrape_successfactors(None, "Johns Hopkins Health System",
                                                     ("https://career4.successfactors.com", "SFHUP", "MD", "filter2")))
    assert len(jobs) == 11 and "Johns Hopkins Health System" in scraper.PARTIAL_SYSTEMS

    def boom(host, company):
        raise RuntimeError("HTTP 403 direct")
    monkeypatch.setattr(scraper, "_sf_open", boom)
    assert asyncio.run(scraper.scrape_successfactors(None, "Johns Hopkins Health System",
                                                     ("https://career4.successfactors.com", "SFHUP", "MD", "filter2"))) == []


def test_sf_posting_body(monkeypatch):
    html = ('<html><body><div class="jobtitlediv">RN</div><div class="sfpanel_wrapper"><div class="bd"><div class="content">'
            '<div class="joqReqDescription" tabindex="0" role="note"><div class="externalPosting"><p><strong>Make it Happen at Hopkins</strong></p>'
            + "<p>Provides direct patient care on the unit. </p>" * 12 +
            '</div></div><div class="button_row"><button>Apply</button></div></div></div></div></body></html>')
    body = scraper._sf_posting_body(html)
    assert body.startswith("Make it Happen at Hopkins") and "Apply" not in body and len(body) > 200
    assert scraper._sf_posting_body("") == ""
    job = Job(title="RN", hospital_system="Johns Hopkins Health System", hospital_name="Johns Hopkins Hospital", city="Baltimore",
              state="MD", location="Baltimore, MD", specialty="", job_type="Full Time", url="https://career4.successfactors.com/career?x",
              job_id="1", posted_date="", description="", ats_platform="SuccessFactors")

    async def fake_html(url, impersonate="chrome", timeout=25):
        return html
    monkeypatch.setattr(scraper, "_curl_html", fake_html)
    assert asyncio.run(scraper._successfactors_detail(None, job)) is True and len(job.description) > 200


# ── configs and scheduling ────────────────────────────────────────────────

def test_cov3_configs_labels_and_scheduling():
    assert scraper.PEOPLESOFT_SITES["NYC Health + Hospitals"] == (NYC, "NY")
    assert scraper.PEOPLESOFT_SITES["The Queen's Health Systems"][1] == "HI"
    assert scraper.PEOPLESOFT_SITES["The Queen's Health Systems"][0].startswith("https://hrweb.queens.org/psc/careers/")
    assert scraper.LIQUIDCOMPASS_SITES["UnityPoint Health"] == ("unitypoint", "IA")
    assert scraper.SUCCESSFACTORS_ORGS["Johns Hopkins Health System"] == ("https://career4.successfactors.com", "SFHUP", "MD", "filter2")
    for lab in ("NYC Health + Hospitals", "The Queen's Health Systems", "UnityPoint Health", "Johns Hopkins Health System"):
        assert lab not in scraper.HOSPITAL_SYSTEM_ALIASES
        assert lab.lower() in scraper.SYSTEM_LOCATION_DEFAULTS
    assert scraper.SYSTEM_LOCATION_DEFAULTS["nyc health + hospitals"] == ("New York", "NY")
    assert scraper.SYSTEM_LOCATION_DEFAULTS["the queen's health systems"] == ("Honolulu", "HI")
    src = inspect.getsource(scraper.run_all)
    for runner in ("run_peoplesoft(proxy_session)", "run_liquidcompass(proxy_session)", "run_successfactors(proxy_session)"):
        assert runner in src
    assert "Handled via Playwright" not in inspect.getsource(scraper)
    assert scraper.PEOPLESOFT_DESC_BUDGET.total == scraper.PEOPLESOFT_DESC_MAX_PER_RUN
    assert scraper.SF_DESC_BUDGET.total == scraper.SF_DESC_MAX_PER_RUN
    assert scraper.PEOPLESOFT_LIST_CAP == 500 and scraper.SF_PAGE_SIZE == 100
