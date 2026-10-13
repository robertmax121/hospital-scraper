# 2026-10-13: tools/verify_stale.py checkers for HCA (Talemetry), PeopleSoft
# (NYC H+H), iCIMS and Phenom, read against pages recorded live on 2026-10-13
# (tests/fixtures/verify_stale, trimmed to the parts the checkers read). No
# network: every request goes to FakeHttp, which fails the test on any
# request it was not told about.
import json
import os
import sys
import threading

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools"))
import verify_stale as vs  # noqa: E402

FIX = os.path.join(REPO, "tests", "fixtures", "verify_stale")


def fx(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


class Resp:
    def __init__(self, status, text="", url="", headers=None):
        self.status_code = status
        self.text = text
        self.url = url
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}

    def json(self):
        return json.loads(self.text)


class FakeHttp:
    """routes: [(method, predicate(url) or url substring, Resp or fn(url, kw) -> Resp)]."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def request(self, method, url, **kw):
        self.calls.append((method, url, kw))
        for m, match, resp in self.routes:
            hit = match(url) if callable(match) else match in url
            if m == method and hit:
                r = resp(url, kw) if callable(resp) else resp
                return r.status_code, r
        raise AssertionError(f"unexpected request {method} {url}")


def urls(http, needle=""):
    return [u for _, u, _ in http.calls if needle in u]


# ------------------------------------------------------------------- HCA
HCA_OPEN = "https://careers.hcahealthcare.com/jobs/17996054-clinical-nurse-coordinator-float-pool"


def hca_row(jid, slug="x"):
    return {"id": 1, "hospital_system": "HCA Healthcare", "ats_platform": "Talemetry",
            "job_id": jid, "url": f"https://careers.hcahealthcare.com/jobs/{jid}-{slug}"}


def hca_routes(extra=()):
    return list(extra) + [
        ("GET", "/search/jobs/in/co-colorado", Resp(200, fx("hca_list_co.html"))),
        ("GET", "/jobs/17996054", Resp(200, fx("hca_open_17996054.html"), HCA_OPEN)),
    ]


def test_hca_open_posting_reads_open_and_uses_firefox_without_redirects():
    http = FakeHttp(hca_routes())
    chk = vs.HcaChecker(http)
    v, st, ev = chk.check({**hca_row("17996054"), "url": HCA_OPEN})
    assert (v, st) == ("OPEN", 200)
    for _, _, kw in http.calls:
        assert kw["impersonate"] == "firefox"
        assert kw["native_ua"] is True
    page_calls = [kw for _, u, kw in http.calls if "/jobs/17996054" in u]
    assert all(kw["allow_redirects"] is False for kw in page_calls)


def test_hca_inactive_page_reads_closed():
    http = FakeHttp(hca_routes([("GET", "/jobs/17200000", Resp(200, fx("hca_inactive_17200000.html")))]))
    v, st, ev = vs.HcaChecker(http).check(hca_row("17200000", "pharmacy-tech"))
    assert (v, st) == ("CLOSED", 200)
    assert "no longer active" in ev


def test_hca_job_not_found_redirect_reads_closed():
    http = FakeHttp(hca_routes([("GET", "/jobs/16263000", Resp(
        301, "", headers={"Location": "https://careers.hcahealthcare.com/jobs?job_not_found=true"}))]))
    assert vs.HcaChecker(http).check(hca_row("16263000"))[0] == "CLOSED"


def test_hca_slug_redirect_is_followed_once_on_the_same_id():
    moved = "https://careers.hcahealthcare.com/jobs/17996054-new-title"
    http = FakeHttp(hca_routes([
        ("GET", lambda u: u.endswith("/jobs/17996054-old-title"), Resp(301, "", headers={"Location": moved})),
        ("GET", lambda u: u == moved, Resp(200, fx("hca_open_17996054.html"), moved)),
    ]))
    assert vs.HcaChecker(http).check(hca_row("17996054", "old-title"))[0] == "OPEN"


def test_hca_redirect_elsewhere_and_blocks_are_unknown():
    http = FakeHttp(hca_routes([
        ("GET", "/jobs/111", Resp(302, "", headers={"Location": "https://careers.hcahealthcare.com/search/jobs"})),
        ("GET", "/jobs/222", Resp(403, "Forbidden")),
        ("GET", "/jobs/333", Resp(200, "<html><title>Just a moment...</title></html>")),
        ("GET", "/jobs/444", Resp(200, "<html>a page with no posting</html>")),
    ]))
    chk = vs.HcaChecker(http)
    for jid in ("111", "222", "333", "444"):
        assert chk.check(hca_row(jid))[0] == "UNKNOWN", jid


def test_hca_canary_failure_makes_every_row_unknown_without_fetching_it():
    http = FakeHttp([("GET", "/search/jobs/in/co-colorado", Resp(403, "blocked"))])
    chk = vs.HcaChecker(http)
    for jid in ("16263000", "17200000"):
        v, st, ev = chk.check(hca_row(jid))
        assert v == "UNKNOWN" and "canary" in ev
    assert urls(http, "/jobs/16263000") == [] and len(http.calls) == 1   # probed once, cached


def test_hca_canary_fails_when_the_listed_job_does_not_read_open():
    http = FakeHttp([
        ("GET", "/search/jobs/in/co-colorado", Resp(200, fx("hca_list_co.html"))),
        ("GET", "/jobs/17996054", Resp(301, "", headers={"Location": "https://careers.hcahealthcare.com/jobs?job_not_found=true"})),
    ])
    v, _, ev = vs.HcaChecker(http).check(hca_row("16263000"))
    assert v == "UNKNOWN" and "canary failed" in ev


# ------------------------------------------------------------ PeopleSoft
PS = "https://careers.nychhc.org/psc/hrtam/EMPLOYEE/HRMS/c/HRS_HRAM_FL.HRS_CG_SEARCH_FL.GBL"
PS_LIST_URL = PS + "?Page=HRS_APP_SCHJOB_FL&Action=U"


def ps_row(jid):
    return {"id": 2, "hospital_system": "NYC Health + Hospitals", "ats_platform": "PeopleSoft",
            "job_id": jid, "url": vs.ps_posting_url(PS, jid)}


def ps_open_page(jid):
    """The recorded open posting (Job ID 140671), for any opening id."""
    return fx("ps_open_140671.html").replace("140671", jid)


def ps_http(open_ids=(), closed_ids=(), flaky_ids=()):
    seen = {}

    def posting(url, kw):
        jid = url.split("JobOpeningId=")[1].split("&")[0]
        seen[jid] = seen.get(jid, 0) + 1
        if jid in open_ids or (jid in flaky_ids and seen[jid] > 1):
            return Resp(200, ps_open_page(jid), url + "&")
        if jid in closed_ids or jid in flaky_ids:
            return Resp(200, fx("ps_list_landing.html"), PS_LIST_URL)
        return Resp(500, "error")
    first_listed = vs._PS_LIST_ID.search(fx("ps_list_landing.html")).group(1)
    return FakeHttp([
        ("GET", "Page=HRS_APP_SCHJOB_FL", Resp(200, fx("ps_list_landing.html"), PS_LIST_URL)),
        ("GET", "Page=HRS_APP_JBPST_FL", posting),
    ]), first_listed


def test_peoplesoft_open_posting_reads_open():
    _, listed = ps_http()
    http, _ = ps_http(open_ids={"140671", listed})
    v, st, ev = vs.PeopleSoftChecker(http).check(ps_row("140671"))
    assert (v, st) == ("OPEN", 200) and "140671" in ev


def test_peoplesoft_list_landing_twice_reads_closed():
    _, listed = ps_http()
    http, _ = ps_http(open_ids={listed}, closed_ids={"130000"})
    v, st, ev = vs.PeopleSoftChecker(http).check(ps_row("130000"))
    assert v == "CLOSED" and "two fetches" in ev
    assert len(urls(http, "JobOpeningId=130000&")) == 2


def test_peoplesoft_one_list_landing_then_the_posting_is_unknown():
    _, listed = ps_http()
    http, _ = ps_http(open_ids={listed}, flaky_ids={"138000"})
    assert vs.PeopleSoftChecker(http).check(ps_row("138000"))[0] == "UNKNOWN"


def test_peoplesoft_canary_failure_is_unknown():
    _, listed = ps_http()
    http, _ = ps_http(closed_ids={listed, "130000"})   # the listed opening does not read OPEN
    v, _, ev = vs.PeopleSoftChecker(http).check(ps_row("130000"))
    assert v == "UNKNOWN" and "canary failed" in ev


def test_peoplesoft_posting_url_parse():
    m = vs._PS_POSTING_URL.match(vs.ps_posting_url(PS, "140671"))
    assert m and m.group(1) == PS and m.group(2) == "140671"
    assert vs._PS_POSTING_URL.match(PS_LIST_URL) is None


# ----------------------------------------------------------------- iCIMS
ICIMS = "https://careers-primehealthcare.icims.com"


def icims_row(jid, path=None):
    return {"id": 3, "hospital_system": "Prime Healthcare", "ats_platform": "iCIMS", "job_id": jid,
            "url": ICIMS + (path or f"/jobs/{jid}/job")}


def icims_http(extra=()):
    return FakeHttp(list(extra) + [
        ("GET", "/jobs/search?ss=1&in_iframe=1", Resp(200, fx("icims_search.html"), ICIMS + "/jobs/search?ss=1&in_iframe=1")),
        ("GET", "/jobs/285412/", lambda u, kw: Resp(200, fx("icims_open_285412.html"), u)),
    ])


def test_icims_page_url():
    assert vs.icims_page_url(ICIMS + "/jobs/285412/urgent-care-advanced-practice-provider/job") == \
        ICIMS + "/jobs/285412/urgent-care-advanced-practice-provider/job?in_iframe=1"
    assert vs.icims_page_url(ICIMS + "/jobs/279952/job?mode=job&iis=Indeed") == ICIMS + "/jobs/279952/job?in_iframe=1"
    assert vs.icims_page_url(ICIMS + "/jobs/279952/nurse/login") == ICIMS + "/jobs/279952/job?in_iframe=1"
    assert vs.icims_page_url("https://example.org/jobs/1/job") == ""


def test_icims_open_posting_reads_open_without_the_fixed_ua():
    http = icims_http()
    v, st, _ = vs.IcimsChecker(http).check(icims_row("285412", "/jobs/285412/urgent-care-advanced-practice-provider/job"))
    assert (v, st) == ("OPEN", 200)
    assert all(kw.get("native_ua") is True for _, _, kw in http.calls)


def test_icims_410_with_the_closed_message_reads_closed():
    http = icims_http([("GET", "/jobs/279952/", Resp(410, fx("icims_410_279952.html")))])
    v, st, ev = vs.IcimsChecker(http).check(icims_row("279952"))
    assert (v, st) == ("CLOSED", 410) and "no longer open" in ev


def test_icims_410_without_the_message_and_405_are_unknown():
    http = icims_http([("GET", "/jobs/111/", Resp(410, "<html>gone</html>")),
                       ("GET", "/jobs/222/", Resp(405, "Method Not Allowed"))])
    chk = vs.IcimsChecker(http)
    assert chk.check(icims_row("111"))[0] == "UNKNOWN"
    assert chk.check(icims_row("222"))[0] == "UNKNOWN"


def test_icims_canary_405_makes_rows_unknown():
    http = FakeHttp([("GET", "/jobs/search", Resp(405, "Method Not Allowed"))])
    v, _, ev = vs.IcimsChecker(http).check(icims_row("279952"))
    assert v == "UNKNOWN" and "canary failed" in ev
    assert urls(http, "/jobs/279952") == []


# ---------------------------------------------------------------- Phenom
UVA = "https://careers.uvahealth.org"
JACKSON = "https://jobs.jacksonhealth.org"


def uva_http(extra=()):
    return FakeHttp(list(extra) + [
        ("GET", "/us/en/search-results", Resp(200, fx("phenom_uva_search.html"), UVA + "/us/en/search-results")),
        ("POST", lambda u: u == UVA + "/widgets", lambda u, kw: Resp(
            200, fx("phenom_widget_uva_R0005339.json") if kw["json_body"]["jobId"] == "R0005339"
            else fx("phenom_widget_empty.json"))),
        ("GET", "/us/en/job/R0005339", lambda u, kw: Resp(200, '<script>{"@type": "JobPosting"}</script>', u)),
        ("GET", "/us/en/job/", lambda u, kw: Resp(410, fx("phenom_page_410.html"), u)),
    ])


def uva_row(jid, url=None):
    return {"id": 4, "hospital_system": "UVA Health", "ats_platform": "Phenom", "job_id": jid,
            "url": url or f"{UVA}/us/en/job/{jid}"}


class StubWorkday:
    def __init__(self, answer):
        self.answer = answer
        self.rows = []

    def check(self, row):
        self.rows.append(row)
        return self.answer


def test_phenom_orgs_are_read_from_scraper_without_importing_it():
    orgs = vs._scraper_literal("PHENOM_ORGS")
    assert orgs["UVA Health"] == UVA and orgs["Jackson Health System"] == JACKSON
    assert "Health First" in vs._scraper_literal("PHENOM_BARE_JOB_PATH")
    assert vs.phenom_base({"hospital_system": "Baylor Scott & White",
                           "url": "https://jobs.bswhealth.com/us/en/job/26007805/rn"}) == "https://jobs.bswhealth.com"
    assert vs.phenom_base({"hospital_system": "Nobody", "url": "https://x.icims.com/jobs/1/job"}) == ""
    assert vs.PhenomChecker.page_url("https://www.careers.hf.org", "Health First", "123") == "https://www.careers.hf.org/job/123"
    assert vs.PhenomChecker.page_url(UVA, "UVA Health", "R1") == UVA + "/us/en/job/R1"


def test_phenom_widget_hit_reads_open():
    http = uva_http()
    v, st, ev = vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0005339"))
    assert (v, st) == ("OPEN", 200) and "R0005339" in ev


def test_phenom_widget_zero_and_page_410_reads_closed():
    http = uva_http()
    v, st, ev = vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0000001"))
    assert (v, st) == ("CLOSED", 410)
    assert "hits 0" in ev


def test_phenom_widget_zero_but_page_200_is_unknown():
    http = uva_http([("GET", "/us/en/job/R0000002", lambda u, kw: Resp(200, "<html></html>", u))])
    assert vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0000002"))[0] == "UNKNOWN"


def test_phenom_widget_hit_on_another_job_is_unknown():
    http = FakeHttp([
        ("GET", "/us/en/search-results", Resp(200, fx("phenom_uva_search.html"))),
        ("POST", UVA + "/widgets", Resp(200, fx("phenom_widget_uva_R0005339.json"))),   # any id -> R0005339
        ("GET", "/us/en/job/R0005339", lambda u, kw: Resp(200, "", u)),
    ])
    v, _, ev = vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0000003"))
    assert v == "UNKNOWN" and "hits 1" in ev


def test_phenom_jackson_row_with_an_infor_apply_link_asks_the_widget_by_job_id():
    http = FakeHttp([
        ("POST", JACKSON + "/widgets", lambda u, kw: Resp(
            200, fx("phenom_widget_open_223645.json") if kw["json_body"]["jobId"] == "223645"
            else fx("phenom_widget_empty.json"))),
        ("GET", "/us/en/job/215000", lambda u, kw: Resp(410, fx("phenom_page_410.html"), u)),
    ])
    chk = vs.PhenomChecker(http, workday=StubWorkday(None))
    chk.canary.get(JACKSON, lambda: (True, "test"))
    row = {"id": 5, "hospital_system": "Jackson Health System", "ats_platform": "Phenom", "job_id": "223645",
           "url": "https://css-f76fc6enwqtkl6kq-prd.inforcloudsuite.com:443/hcm/Jobs/form/x"}
    assert chk.check(row)[0] == "OPEN"
    assert chk.check({**row, "job_id": "215000"})[0] == "CLOSED"
    assert urls(http, "inforcloudsuite") == []


def test_phenom_workday_linked_row_takes_workday_verdict_first():
    wd_url = "https://uva.wd1.myworkdayjobs.com/UVAJobs/job/Charlottesville-VA/Custodial_R0005339"
    http = uva_http()
    stub = StubWorkday(("CLOSED", "403-S22", "detail 403-S22; search no match"))
    assert vs.PhenomChecker(http, workday=stub).check(uva_row("R0005339", wd_url))[0] == "CLOSED"
    assert http.calls == []   # Workday decided; the Phenom board was not asked


def test_phenom_workday_unknown_falls_back_to_the_widget():
    wd_url = "https://uva.wd1.myworkdayjobs.com/UVAJobs/job/Charlottesville-VA/Gone_R0000001"
    stub = StubWorkday(("UNKNOWN", "", "board canary failed (board total 0)"))
    v, st, ev = vs.PhenomChecker(uva_http(), workday=stub).check(uva_row("R0000001", wd_url))
    assert v == "CLOSED" and ev.startswith("workday: board canary failed")


def test_phenom_seqno_job_id_is_unknown():
    row = {"id": 6, "hospital_system": "UVA Health", "ats_platform": "Phenom",
           "job_id": "UOVUNIUSR0005339EXTERNALENUS", "url": "https://example.org/apply"}
    http = FakeHttp([])
    assert vs.PhenomChecker(http, workday=StubWorkday(None)).check(row)[0] == "UNKNOWN"
    assert http.calls == []


def test_phenom_canary_needs_the_listed_jobs_page_to_answer_200():
    http = uva_http([("GET", "/us/en/job/R0005339", lambda u, kw: Resp(410, "", u))])
    v, _, ev = vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0000001"))
    assert v == "UNKNOWN" and "canary failed" in ev


# ------------------------------------------------------- dispatch, canary
def test_checker_for_row_dispatch():
    http = FakeHttp([])
    pick = lambda row: type(vs.checker_for_row(row, http)).__name__  # noqa: E731
    assert pick(hca_row("1")) == "HcaChecker"
    assert pick({**hca_row("1"), "ats_platform": "Custom"}) == "HcaChecker"
    assert pick(ps_row("140671")) == "PeopleSoftChecker"
    assert pick(icims_row("1")) == "IcimsChecker"
    assert pick(uva_row("R1")) == "PhenomChecker"
    assert pick({**uva_row("R1"), "url": "https://careers-acadia.icims.com/jobs/1/job"}) == "PhenomChecker"
    assert pick({"ats_platform": "Workday", "url": "https://nyp.wd1.myworkdayjobs.com/x/job/y_1"}) == "WorkdayChecker"
    assert pick({"ats_platform": "TalentBrew", "url": "https://example.org/job/1"}) == "GenericChecker"
    assert isinstance(vs.checker_for_row(uva_row("R1"), http).workday, vs.WorkdayChecker)


def test_canary_runs_once_across_threads_and_budget_cuts_are_not_cached():
    c = vs.Canary()
    calls = []
    gate = threading.Event()

    def probe():
        calls.append(1)
        gate.wait(2)
        return True, "ok"
    out = []
    ts = [threading.Thread(target=lambda: out.append(c.get("k", probe))) for _ in range(5)]
    for t in ts:
        t.start()
    gate.set()
    for t in ts:
        t.join(5)
    assert len(calls) == 1 and out == [(True, "ok")] * 5

    def broke():
        raise vs.Http.OutOfBudget()
    with pytest.raises(vs.Http.OutOfBudget):
        c.get("b", broke)
    assert c.get("b", lambda: (False, "later")) == (False, "later")


def test_native_ua_wrapper_sets_the_flag_only_when_not_given():
    http = FakeHttp([("GET", "x", Resp(200))])
    vs.NativeUA(http).request("GET", "http://x/")
    vs.NativeUA(http).request("GET", "http://x/", native_ua=False)
    assert [kw["native_ua"] for _, _, kw in http.calls] == [True, False]


# --------------------------------------------------------------- sql mode
def _sql_args(tmp_path, **kw):
    import argparse
    out = tmp_path / "csv"
    out.mkdir()
    with open(out / "talemetry.csv", "w", encoding="utf-8", newline="") as f:
        w = vs.csv.DictWriter(f, fieldnames=vs.CSV_FIELDS)
        w.writeheader()
        for i, v in ((11, "CLOSED"), (12, "OPEN"), (13, "UNKNOWN"), (14, "CLOSED")):
            w.writerow({"id": i, "hospital_system": "HCA Healthcare", "ats_platform": "Talemetry", "verdict": v})
    base = dict(out_dir=str(out), out_sql=str(tmp_path / "119_retire.sql"), out_restore=str(tmp_path / "119r_retire.sql"),
                stale_before="2026-10-13 05:30+00", system=None, batch=1000,
                backup_table="retired_closed_backup_20261013", db_write=True)
    base.update(kw)
    return argparse.Namespace(**base)


def test_sql_db_write_format(tmp_path):
    args = _sql_args(tmp_path)
    vs.run_sql(args)
    sql = open(args.out_sql, encoding="utf-8").read().splitlines()
    rb = open(args.out_restore, encoding="utf-8").read().splitlines()
    for line in sql + rb:
        assert line and not line.startswith("--")
        assert line.startswith("set lock_timeout = '3s'; ") or line.startswith("select ")
    assert len(sql) == 4 and len(rb) == 2
    assert "public.retired_closed_backup_20261013" in sql[0] and "if not exists" in sql[0]
    assert "id in (11,14)" in sql[1] and "on conflict (id) do nothing" in sql[1]
    assert sql[2].startswith("set lock_timeout = '3s'; update public.hospital_jobs set is_active = false "
                             "where is_active and scraped_at < '2026-10-13 05:30+00' and id in (11,14);")
    assert "public.retired_closed_backup_20261013 b" in rb[0] and "h.scraped_at < '2026-10-13 05:30+00'" in rb[0]


def test_sql_commented_format_names_its_own_files(tmp_path):
    args = _sql_args(tmp_path, db_write=False)
    vs.run_sql(args)
    text = open(args.out_sql, encoding="utf-8").read()
    assert text.startswith("-- 119_retire.sql") and "119r_retire.sql" in text and "115" not in text


def test_docstring_carries_the_export_and_retire_sql():
    doc = vs.__doc__
    assert ("select id, hospital_system, ats_platform, job_id, url, scraped_at, consecutive_scrape_misses as misses "
            "from public.hospital_jobs where is_active and scraped_at < now() - interval '4 days' and "
            "(hospital_system in ('HCA Healthcare', 'NYC Health + Hospitals') or ats_platform in ('Phenom', 'iCIMS'))") in doc
    assert "set lock_timeout = '3s'; update public.hospital_jobs set is_active = false" in doc
    assert "on conflict (id) do nothing" in doc and "restored_active" in doc
