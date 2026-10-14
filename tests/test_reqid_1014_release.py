# 2026-10-14 (owner: "Yeah we need to fix those now"): the combined req_id
# release (release/reqid-18-1014 = scraper/reqid-g1..g4-1014; NEOGOV is
# mothballed and not in it). One Job per platform, built from a saved
# fixture through that platform's adapter, then normalize_job: the employer's
# number lands in posting_facts (or the {"req_id"}-only object on a night the
# row's facts are null), job_id and url are the adapter's, and req_id is
# never a column of the row. Plus the integration rules: one union set, both
# re-read windows on one start date, no row queued by both re-reads, and the
# re-reads stay inside the detail budget the pass already has.
import asyncio
import dataclasses
import json
from datetime import date

import pytest

import scraper
from scraper import Job

FX = "reqid_1014/"
RELEASE_PLATFORMS = frozenset({
    "TalentBrew", "WPJobBoard", "Findly-Google", "LiquidCompass",           # group 1
    "iCIMS", "Jobvite", "HCTS", "SelectMinds",                              # group 2
    "HealthcareSource", "UKG", "ADP", "ADP CX",                             # group 3
    "Workday", "Phenom", "CSOD", "SuccessFactorsRMK", "Paycor", "FXRecruiter",   # group 4
})


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    quiet(monkeypatch)
    yield
    scraper.set_known_bodies([])


def quiet(mp):
    """No pauses, no known bodies, fresh detail budgets, the env switch off
    (the per-platform set is what sends the req_id-only object)."""
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)

    async def no_jitter(*a, **k):
        return None
    mp.setattr(scraper.asyncio, "sleep", no_sleep)
    mp.setattr(scraper, "jitter", no_jitter)
    mp.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    mp.setattr(scraper, "DETAIL_FETCH", True)
    for name in ("WD", "CSOD", "SF_RMK", "PAYCOR", "TB", "TALEMETRY"):
        if hasattr(scraper, f"{name}_DESC_BUDGET"):
            mp.setattr(scraper, f"{name}_DESC_BUDGET", scraper._DescBudget(1000))
    scraper.set_known_bodies([])


class _R:
    def __init__(self, body, status=200):
        self.status, self._body = status, body
        self.status_code = status
        self.headers = {"content-type": "application/json"}

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


def _serve(mp, answer):
    """scraper.req -> answer(method, url, kw) as the response body."""
    def fake_req(session, method, url, **kw):
        return _Ctx(_R(answer(method, url, kw)))
    mp.setattr(scraper, "req", fake_req)


def _page(text):
    async def fetch(*a, **k):
        return text
    return fetch


def _one(jobs, jid):
    return next(j for j in jobs if str(j.job_id) == str(jid))


# ── builders: platform -> (Job through the adapter, number the page prints) ──
def b_talentbrew(fx, mp):
    """CommonSpirit: a stored body without a number, inside the window: the
    "req" re-read puts it in the detail pass, which reads the page."""
    jid = "97195133472"
    page = fx(FX + f"talentbrew/commonspirit_{jid}.html")
    mp.setattr(scraper, "_fetch_html", _page(page))
    win = getattr(scraper, "REQ_REREAD_WINDOW", ("2026-10-15", "2026-10-28"))
    mp.setattr(scraper, "_RUN_DAY", [date.fromisoformat(win[0]).toordinal()])
    scraper.set_known_bodies([{"hospital_system": "CommonSpirit", "job_id": jid, "ats_platform": "TalentBrew",
                               "desc_len": 3200, "wage_min": None, "fv": scraper.FACTS_VERSION,
                               "fh": "aaaaaaaaaaaa", "rq": None}])
    job = Job(title="Surgical Tech", hospital_system="CommonSpirit Health", hospital_name="CHI Health",
              city="Omaha", state="NE", location="Omaha, NE", specialty="", job_type="",
              url=f"https://www.commonspirit.careers/job/omaha/surgical-tech/35300/{jid}",
              job_id=jid, posted_date="", description="", ats_platform="TalentBrew")
    asyncio.run(scraper._detail_pass(None, "CommonSpirit Health", [job], scraper._DescBudget(10),
                                     lambda j: scraper._jsonld_detail(None, j), "TalentBrew"))
    return job, "2026-480318"


def b_wpjobboard(fx, mp):
    grid = fx(FX + "wpjobboard/chs_grid.json")
    empty = json.dumps({"payload": [], "meta": {"total": 2571}})

    def answer(method, url, kw):
        params = kw.get("params") or kw.get("data") or {}
        return grid if int(params.get("offset", 0)) == 0 else empty

    async def no_detail(*a, **k):
        return None
    _serve(mp, answer)
    mp.setattr(scraper, "_board_detail_passes", no_detail)
    return _one(asyncio.run(scraper.run_chs(None)), "145981"), "167235"


class _FindlySession:
    def __init__(self, body):
        self.body = body

    def get(self, url, **kw):
        return _Ctx(_R(self.body))


def b_findly_google(fx, mp):
    sess = _FindlySession(fx(FX + "findly_google/upmc_search.txt"))
    jobs = asyncio.run(scraper.scrape_findly_google(sess, "UPMC", scraper.FINDLY_GOOGLE_ORGS["UPMC"]))
    return _one(jobs, "24007855"), "8457814813"


class _SyncResp:
    status_code = 200

    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def b_liquidcompass(fx, mp):
    payload = dict(json.loads(fx(FX + "liquidcompass/unitypoint_search.json")), page_count=1)
    mp.setattr(scraper, "_curl_fetch", lambda *a, **k: _SyncResp(payload))
    jobs = asyncio.run(scraper.scrape_liquidcompass(None, "UnityPoint Health", ("unitypoint", "IA")))
    return _one(jobs, "52810916"), "188593"


def b_icims(fx, mp):
    jobs = scraper._parse_icims_cards(fx(FX + "icims/cards_prime_healthcare.html"), "Prime Healthcare",
                                      "careers-primehealthcare.icims.com")
    return _one(jobs, "285876"), "2026-285876"


def b_jobvite(fx, mp):
    page = fx(FX + "jobvite/penn_medicine_job_18360735.html")

    async def fake_curl(url, imp=None, *a, **k):
        return page
    mp.setattr(scraper, "_curl_html", fake_curl)
    entry = json.loads(fx(FX + "jobvite/penn_medicine_search.json"))["entries"][0]
    job = scraper._talemetry_job(entry, "Penn Medicine", "https://careers.pennmedicine.org")
    asyncio.run(scraper._talemetry_detail(None, job))
    return job, "347741"


def b_hcts(fx, mp):
    jobs = scraper._parse_hcts_page(fx(FX + "hcts/cards_university_medical_center_of_el_paso.html"),
                                    "University Medical Center of El Paso", "umcelpasocareers", "TX")
    return _one(jobs, "1809610"), "26610"


def b_selectminds(fx, mp):
    jobs = scraper._parse_selectminds_page(fx("cov3/selectminds_mainline_cards.html"), "Main Line Health",
                                           "https://mainline.referrals.selectminds.com")
    return _one(jobs, "29120"), "82959"


def b_healthcaresource(fx, mp):
    page = json.loads(fx(FX + "healthcaresource/rwjbarnabas_search.json"))
    _serve(mp, lambda m, u, kw: page)
    jobs = asyncio.run(scraper.scrape_healthcaresource(None, "RWJBarnabas Health",
                                                       scraper.HEALTHCARESOURCE_ORGS["RWJBarnabas Health"]))
    return _one(jobs, "136024"), "0000264102"


def b_ukg(fx, mp):
    page = json.loads(fx(FX + "ukg/nhc_search.json"))
    _serve(mp, lambda m, u, kw: page)
    jobs = asyncio.run(scraper.scrape_ukg(None, "NHC", scraper.UKG_ORGS["NHC"]))
    return _one(jobs, "e6efa8de-a057-460e-b718-01fe8a72bb58"), "BEHAV042110"


def b_adp(fx, mp):
    page = json.loads(fx(FX + "adp/kpc_requisitions.json"))
    _serve(mp, lambda m, u, kw: page)
    jobs = asyncio.run(scraper.scrape_adp(None, "KPC Health",
                                          ("a45074b6-b45e-424e-9e71-c45a4d73c34b", "19000101_000001", "CA")))
    return _one(jobs, "9201508809155_1"), "9218"


def b_adp_cx(fx, mp):
    page = json.loads(fx(FX + "adpcx/huntregional_requisitions.json"))

    def answer(m, u, kw):
        if "/career-site/" in u:
            return {"orgoid": "test-orgoid", "myJobsToken": "test-token"}
        return page
    _serve(mp, answer)
    jobs = asyncio.run(scraper.scrape_adp_cx(None, "Hunt Regional Healthcare",
                                             scraper.ADPCX_ORGS["Hunt Regional Healthcare"]))
    return _one(jobs, "5001231190600"), "26-2694"


def b_workday(fx, mp):
    """MarinHealth, keyed by its externalPath: a stored body without a number
    on its night of the 5-night cycle is read by the Workday detail pass."""
    d = json.loads(fx(FX + "workday/marinhealth.json"))
    meta = d["_meta"]
    job = Job(title=d["jobPostingInfo"]["title"], hospital_system="MarinHealth", hospital_name="MarinHealth",
              city="", state="", location="", specialty="", job_type="", url=meta["url"],
              job_id=meta["our_job_id"], posted_date="Posted Yesterday", description="", ats_platform="Workday")
    scraper.set_known_bodies([{"hospital_system": "MarinHealth", "job_id": job.job_id, "ats_platform": "Workday",
                               "desc_len": 4000, "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": None,
                               "rq": None}])
    win = getattr(scraper, "REQ_ID_REREAD_WINDOW", ("2026-10-15", "2026-10-28"))
    a = date.fromisoformat(win[0]).toordinal()
    night = a
    if hasattr(scraper, "_req_reread_slot"):         # the release; the base has no cycle
        for x in range(a, a + scraper.REQ_ID_REREAD_CYCLE):
            mp.setattr(scraper, "_RUN_DAY", [x])
            if scraper._req_reread_slot(("MarinHealth", job.job_id), job):
                night = x
                break
    mp.setattr(scraper, "_RUN_DAY", [night])
    mp.setattr(scraper, "_refresh_slot", lambda *a, **k: False)
    _serve(mp, lambda m, u, kw: d if "/wday/cxs/" in u else "")
    working = "https://mymarinhealth.wd5.myworkdayjobs.com/wday/cxs/mymarinhealth/MHCareers/jobs"
    asyncio.run(scraper._workday_fetch_details(None, working, [(job, job.job_id)], "MarinHealth"))
    return job, "14976 - AC307"


class _BswSession:
    def __init__(self, payload):
        self.payload = payload

    def post(self, url, json=None, **kw):
        body = self.payload if json.get("from") == 0 else {"refineSearch": {"totalHits": 2258, "data": {"jobs": []}}}
        return _Ctx(_R(body))


def b_phenom(fx, mp):
    mp.setattr(scraper, "DETAIL_FETCH", False)
    jobs = asyncio.run(scraper._scrape_bsw(_BswSession(json.loads(fx(FX + "phenom/bsw_refinesearch.json")))))
    return _one(jobs, "BSWHUS26018384EXTERNALENUS"), "26018384"


def b_csod(fx, mp):
    payload = json.loads(fx(FX + "csod/jps_30790.json"))
    _serve(mp, lambda m, u, kw: payload if "/requisitions/30790/jobDetails" in u else "")
    mp.setitem(scraper._CSOD_CTX, "JPS Health Network", ("https://x.csod.com", "tok", ""))
    job = Job(title="X", hospital_system="JPS Health Network", hospital_name="JPS Health Network", city="",
              state="", location="", specialty="", job_type="",
              url="https://jpshealthnet.csod.com/ux/ats/careersite/4/requisition/30790?c=jpshealthnet",
              job_id="30790", posted_date="", description="", ats_platform="CSOD")
    asyncio.run(scraper._csod_detail(None, job))
    return job, "46562"


def b_rmk(fx, mp):
    mp.setattr(scraper, "_fetch_html", _page(fx(FX + "rmk/tx_1439766300.html")))
    job = Job(title="X", hospital_system="Texas Health and Human Services",
              hospital_name="Texas Health and Human Services", city="", state="TX", location="",
              specialty="", job_type="", url="https://careers.hhs.texas.gov/job/X/1439766300/",
              job_id="1439766300", posted_date="", description="", ats_platform="SuccessFactorsRMK")
    asyncio.run(scraper._rmk_detail(None, job))
    return job, "22217"


def b_paycor(fx, mp):
    mp.setattr(scraper, "_fetch_html", _page(fx(FX + "paycor/townsen_tmscw1013.html")))
    hexid = "8a78839ea13353c501a1367d3cb31d54"
    job = Job(title="X", hospital_system="Townsen Memorial Hospital", hospital_name="Townsen Memorial Hospital",
              city="", state="TX", location="", specialty="", job_type="",
              url="https://recruitingbypaycor.com/career/JobIntroduction.action?id=" + hexid,
              job_id=hexid, posted_date="", description="", ats_platform="Paycor")
    asyncio.run(scraper._paycor_detail(None, job))
    return job, "TMSCW1013"


def b_fxrecruiter(fx, mp):
    jobs = scraper._parse_rss_list(fx(FX + "fxrecruiter/rush_feed.xml"), "Rush", scraper.HTML_LIST_SITES["Rush"])
    return _one(jobs, "13191"), "31067"


BUILDERS = {
    "TalentBrew": (b_talentbrew, "97195133472"),
    "WPJobBoard": (b_wpjobboard, "145981"),
    "Findly-Google": (b_findly_google, "24007855"),
    "LiquidCompass": (b_liquidcompass, "52810916"),
    "iCIMS": (b_icims, "285876"),
    "Jobvite": (b_jobvite, "18360735"),
    "HCTS": (b_hcts, "1809610"),
    "SelectMinds": (b_selectminds, "29120"),
    "HealthcareSource": (b_healthcaresource, "136024"),
    "UKG": (b_ukg, "e6efa8de-a057-460e-b718-01fe8a72bb58"),
    "ADP": (b_adp, "9201508809155_1"),
    "ADP CX": (b_adp_cx, "5001231190600"),
    "Workday": (b_workday, "/job/Greenbrae-CA/Clinical-RN-II--Intensive-Care--Part-Time---36-Hour-Week---Nights_JR102021"),
    "Phenom": (b_phenom, "BSWHUS26018384EXTERNALENUS"),
    "CSOD": (b_csod, "30790"),
    "SuccessFactorsRMK": (b_rmk, "1439766300"),
    "Paycor": (b_paycor, "8a78839ea13353c501a1367d3cb31d54"),
    "FXRecruiter": (b_fxrecruiter, "13191"),
}


def dry_normalize(platform, fx, mp):
    """(job, want, row, list_night_row): the adapter's Job, the number its
    page prints, normalize_job of it, and normalize_job of the same row on a
    night its facts are null (no body in hand)."""
    build, _ = BUILDERS[platform]
    job, want = build(fx, mp)
    row = scraper.normalize_job(dataclasses.replace(job))
    scraper.set_known_bodies([])
    bare = dataclasses.replace(job, description="", title="X", job_type="")
    return job, want, row, scraper.normalize_job(bare)


def test_the_release_covers_exactly_the_18_platforms():
    assert set(BUILDERS) == RELEASE_PLATFORMS and len(RELEASE_PLATFORMS) == 18
    assert scraper.REQ_ID_ONLY_PLATFORMS == RELEASE_PLATFORMS
    assert scraper.REQ_ID_LIST_PLATFORMS <= scraper.REQ_ID_ONLY_PLATFORMS
    assert "NeoGov" not in scraper.REQ_ID_ONLY_PLATFORMS and "NEOGOV" not in scraper.REQ_ID_ONLY_PLATFORMS


@pytest.mark.parametrize("platform", sorted(BUILDERS))
def test_dry_normalize_each_platform(platform, fixture_text, monkeypatch):
    job, want, row, bare = dry_normalize(platform, fixture_text, monkeypatch)
    jid = BUILDERS[platform][1]
    assert job.ats_platform == platform
    assert job.req_id == want and want != job.job_id
    # job_id and url exactly as the adapter built them; req_id is not a column
    assert row["job_id"] == jid and row["url"] == job.url and "req_id" not in row
    assert row["posting_facts"] is not None and row["posting_facts"]["req_id"] == want
    # a night its facts are null: the {"req_id"}-only object, without the switch
    assert bare["posting_facts"] == {"req_id": want}
    assert bare["job_id"] == jid and bare["url"] == job.url and "req_id" not in bare
    print(f"\nDRY {platform:18s} job_id={row['job_id'][:48]:48s} req_id={row['posting_facts']['req_id']!r:16s} "
          f"facts_keys={len(row['posting_facts'])} null-facts-night={bare['posting_facts']}")


# ── the two re-reads together ───────────────────────────────────────────────
def test_both_windows_open_on_one_start_line(monkeypatch):
    # 2026-10-15 when the push lands before 00:00 UTC 2026-10-15, else 2026-10-16
    assert scraper.REQ_ID_WINDOW_START in ("2026-10-15", "2026-10-16")
    assert scraper.REQ_REREAD_WINDOW == scraper.REQ_ID_REREAD_WINDOW == scraper._req_id_window(scraper.REQ_ID_WINDOW_START)
    assert scraper._req_id_window("2026-10-15") == ("2026-10-15", "2026-10-28")      # as set
    # moving the start moves both windows, end included
    assert scraper._req_id_window("2026-10-16") == ("2026-10-16", "2026-10-29")
    assert scraper._req_id_window("2026-10-16", 1) == ("2026-10-16", "2026-10-16")


def _tb_row(jid):
    return {"hospital_system": "CommonSpirit", "job_id": jid, "ats_platform": "TalentBrew", "desc_len": 3200,
            "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": "aaaaaaaaaaaa", "rq": None}


def _wd_row(jid, system="WVU Medicine"):
    return {"hospital_system": system, "job_id": jid, "ats_platform": "Workday", "desc_len": 4000,
            "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": None, "rq": None}


def _job(system, jid, platform):
    return Job(title="RN", hospital_system=system, hospital_name=system, city="", state="", location="",
               specialty="", job_type="", url=f"https://x/{jid}", job_id=jid, posted_date="",
               description="", ats_platform=platform)


def test_no_row_is_queued_by_both_re_reads(monkeypatch):
    """A TalentBrew row is the "req" kind's; the 5-night cycle never takes it,
    even for a tenant that were on REQ_ID_REREAD_SYSTEMS. Every night of the
    window each row gets one kind."""
    monkeypatch.setattr(scraper, "REQ_ID_REREAD_SYSTEMS", scraper.REQ_ID_REREAD_SYSTEMS | {"CommonSpirit"})
    tb = [str(100 + i) for i in range(40)]
    wd = [f"/job/x/{i}" for i in range(40)]
    scraper.set_known_bodies([_tb_row(j) for j in tb] + [_wd_row(j) for j in wd])
    a = date.fromisoformat(scraper.REQ_ID_WINDOW_START).toordinal()
    seen_force = set()
    for night in range(a, a + scraper.REQ_ID_WINDOW_NIGHTS):
        monkeypatch.setattr(scraper, "_RUN_DAY", [night])
        for jid in tb:
            j = _job("CommonSpirit Health", jid, "TalentBrew")
            assert not scraper._req_reread_slot(("CommonSpirit", jid), j)
            assert scraper._known_kind("CommonSpirit Health", j) == "req"
        for jid in wd:
            k = scraper._known_kind("WVU Medicine", _job("WVU Medicine", jid, "Workday"))
            assert k in ("force", "known")
            if k == "force":
                seen_force.add(jid)
        # one detail pass a night: each item at most once in its candidates
        items = [_job("CommonSpirit Health", j, "TalentBrew") for j in tb]
        cands, _, _ = scraper._detail_candidates("CommonSpirit Health", items, scraper._DescBudget(1000), held=[])
        assert len({id(c) for c in cands}) == len(cands) == len(tb)
    assert seen_force == set(wd)          # the cycle reaches every Workday row inside the window


def test_tiers_keep_every_group_in_its_place(monkeypatch):
    """No body first, then force (incl. the 5-night cycle), req (TalentBrew),
    pay, cut, and last the Jobvite rows that already hold a body and a number."""
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    a = date.fromisoformat(scraper.REQ_ID_WINDOW_START).toordinal()
    monkeypatch.setattr(scraper, "_RUN_DAY", [a])
    tb = ["1", "2"]
    scraper.set_known_bodies([_tb_row("1"), dict(_tb_row("2"), desc_len=7995, fv=None)])
    items = [_job("CommonSpirit Health", j, "TalentBrew") for j in tb] + [_job("CommonSpirit Health", "0", "TalentBrew")]
    held = []
    cands, _, _ = scraper._detail_candidates("CommonSpirit Health", items, scraper._DescBudget(100), held=held)
    assert [c.job_id for c in cands] == ["0", "1", "2"]
    assert [(it.job_id, kind) for it, _, kind in held] == [("2", "cut"), ("1", "req")]
    # Jobvite: a row holding a body and a number goes behind one missing the number
    scraper.set_known_bodies([{"hospital_system": "PeaceHealth", "job_id": "9", "ats_platform": "Jobvite",
                               "desc_len": 3000, "rq": "143880"}])
    jv = [_job("PeaceHealth", "9", "Jobvite"), _job("PeaceHealth", "8", "Jobvite")]
    cands, _, _ = scraper._detail_candidates("PeaceHealth", jv, scraper._DescBudget(100), held=[])
    assert [c.job_id for c in cands] == ["8", "9"]


def test_re_reads_stay_inside_the_tenants_budget(monkeypatch):
    """A tenant with 30 rows to re-read for their number and 5 new rows, on a
    budget of 10: 10 fetches, the 5 new rows first."""
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    a = date.fromisoformat(scraper.REQ_ID_WINDOW_START).toordinal()
    monkeypatch.setattr(scraper, "_RUN_DAY", [a])
    old = [str(200 + i) for i in range(30)]
    scraper.set_known_bodies([_tb_row(j) for j in old])
    jobs = [_job("CommonSpirit Health", j, "TalentBrew") for j in old] + \
           [_job("CommonSpirit Health", f"n{i}", "TalentBrew") for i in range(5)]
    fetched = []

    async def fetch_one(j):
        fetched.append(j.job_id)
        return False
    budget = scraper._DescBudget(10)
    asyncio.run(scraper._detail_pass(None, "CommonSpirit Health", jobs, budget, fetch_one, "TalentBrew"))
    assert budget.spent == 10 and len(fetched) == 10
    assert set(fetched[:5]) == {f"n{i}" for i in range(5)}
    assert all(j in old for j in fetched[5:])
