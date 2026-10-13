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


HCA_GONE = Resp(301, "", headers={"Location": "https://careers.hcahealthcare.com/jobs?job_not_found=true"})


def test_hca_inactive_page_reads_closed_on_two_fetches():
    http = FakeHttp(hca_routes([("GET", "/jobs/17200000", Resp(200, fx("hca_inactive_17200000.html")))]))
    v, st, ev = vs.HcaChecker(http).check(hca_row("17200000", "pharmacy-tech"))
    assert (v, st) == ("CLOSED", 200)
    assert "no longer active" in ev and "second fetch agrees" in ev
    assert len(urls(http, "/jobs/17200000")) == 2


def test_hca_job_not_found_redirect_reads_closed_on_two_fetches():
    http = FakeHttp(hca_routes([("GET", "/jobs/16263000", HCA_GONE)]))
    assert vs.HcaChecker(http).check(hca_row("16263000"))[0] == "CLOSED"
    assert len(urls(http, "/jobs/16263000")) == 2


def test_hca_closed_then_open_on_the_second_fetch_is_unknown():
    seen = []

    def page(url, kw):
        seen.append(url)
        if len(seen) == 1:
            return HCA_GONE
        return Resp(200, fx("hca_open_17996054.html").replace("17996054", "16263001"), url)
    http = FakeHttp(hca_routes([("GET", "/jobs/16263001", page)]))
    v, _, ev = vs.HcaChecker(http).check(hca_row("16263001"))
    assert v == "UNKNOWN" and "second fetch read OPEN" in ev
    assert len(seen) == 2


def test_hca_closed_then_blocked_on_the_second_fetch_is_unknown():
    answers = [Resp(200, fx("hca_inactive_17200000.html")), Resp(403, "Forbidden")]
    http = FakeHttp(hca_routes([("GET", "/jobs/17200000", lambda u, kw: answers.pop(0))]))
    v, st, ev = vs.HcaChecker(http).check(hca_row("17200000"))
    assert (v, st) == ("UNKNOWN", 403) and "second fetch read UNKNOWN" in ev


def test_hca_404_and_410_are_unknown_not_closed():
    http = FakeHttp(hca_routes([("GET", "/jobs/555", Resp(404, "Not Found")),
                                ("GET", "/jobs/666", Resp(410, "Gone"))]))
    chk = vs.HcaChecker(http)
    for jid, code in (("555", 404), ("666", 410)):
        v, st, ev = chk.check(hca_row(jid))
        assert (v, st) == ("UNKNOWN", code) and "never observed" in ev
        assert len(urls(http, f"/jobs/{jid}")) == 1   # no second fetch for a non-CLOSED read


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
        ("GET", "/jobs/777", Resp(410, "Gone")),
    ]))
    chk = vs.HcaChecker(http)
    for jid in ("111", "222", "333", "444", "777"):
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
        ("GET", "/jobs/17996054", HCA_GONE),
    ])
    v, _, ev = vs.HcaChecker(http).check(hca_row("16263000"))
    assert v == "UNKNOWN" and "canary failed" in ev


def test_hca_canary_reruns_every_n_rows():
    http = FakeHttp(hca_routes([("GET", "/jobs/16263000", HCA_GONE)]))
    chk = vs.HcaChecker(http)
    chk.CANARY_EVERY = 2
    verdicts = [chk.check(hca_row("16263000"))[0] for _ in range(5)]
    assert verdicts == ["CLOSED"] * 5
    assert len(urls(http, "/search/jobs/in/co-colorado")) == 3   # before rows 0, 2 and 4
    assert vs.HcaChecker.CANARY_EVERY == 500


def test_hca_canary_failing_mid_run_aborts_the_rest_of_the_site():
    listings = [Resp(200, fx("hca_list_co.html")), Resp(403, "blocked")]
    http = FakeHttp([
        ("GET", "/search/jobs/in/co-colorado", lambda u, kw: listings.pop(0)),
        ("GET", "/jobs/17996054", Resp(200, fx("hca_open_17996054.html"), HCA_OPEN)),
        ("GET", "/jobs/16263000", HCA_GONE),
    ])
    chk = vs.HcaChecker(http)
    chk.CANARY_EVERY = 2
    out = [chk.check(hca_row("16263000")) for _ in range(5)]
    assert [v for v, _, _ in out] == ["CLOSED", "CLOSED", "UNKNOWN", "UNKNOWN", "UNKNOWN"]
    assert "canary failed at row 2" in out[2][2] and "site aborted" in out[2][2]
    assert all("site aborted" in ev for _, _, ev in out[3:])
    assert len(urls(http, "/jobs/16263000")) == 4                # rows 0 and 1 only, two fetches each
    assert len(urls(http, "/search/jobs/in/co-colorado")) == 2   # no third canary after the abort


def test_hca_closed_in_flight_when_the_canary_fails_is_unknown():
    chk = None

    def page(url, kw):
        chk.dead = "board canary failed at row 500 (listing answered 403); site aborted"
        return HCA_GONE
    http = FakeHttp(hca_routes([("GET", "/jobs/16263000", page)]))
    chk = vs.HcaChecker(http)
    v, _, ev = chk.check(hca_row("16263000"))
    assert v == "UNKNOWN" and "not trusted" in ev


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


def uva_row(jid, url=None, scraped="2026-10-07T03:00:00+00:00", rid=4):
    return {"id": rid, "hospital_system": "UVA Health", "ats_platform": "Phenom", "job_id": jid,
            "url": url or f"{UVA}/us/en/job/{jid}", "scraped_at": scraped}


UVA_NEWEST_OPEN = uva_row("R0005339", scraped="2026-10-08 22:10:00+00", rid=40)


class StubWorkday:
    def __init__(self, answer):
        self.answer = answer
        self.rows = []

    def check(self, row):
        self.rows.append(row)
        return self.answer


def phenom(http, workday=None, rows=(UVA_NEWEST_OPEN,)):
    """A PhenomChecker primed with an export (the row canary's pick)."""
    chk = vs.PhenomChecker(http, workday=workday or StubWorkday(None))
    chk.prime(list(rows))
    return chk


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
    v, st, ev = phenom(http).check(uva_row("R0005339"))
    assert (v, st) == ("OPEN", 200) and "R0005339" in ev


def test_phenom_widget_zero_and_page_410_reads_closed():
    http = uva_http()
    v, st, ev = phenom(http).check(uva_row("R0000001"))
    assert (v, st) == ("CLOSED", 410)
    assert "hits 0" in ev


def test_phenom_widget_zero_but_page_200_is_unknown():
    http = uva_http([("GET", "/us/en/job/R0000002", lambda u, kw: Resp(200, "<html></html>", u))])
    assert phenom(http).check(uva_row("R0000002"))[0] == "UNKNOWN"


def test_phenom_widget_hit_on_another_job_is_unknown():
    http = FakeHttp([
        ("GET", "/us/en/search-results", Resp(200, fx("phenom_uva_search.html"))),
        ("POST", UVA + "/widgets", Resp(200, fx("phenom_widget_uva_R0005339.json"))),   # any id -> R0005339
        ("GET", "/us/en/job/R0005339", lambda u, kw: Resp(200, "", u)),
    ])
    v, _, ev = phenom(http).check(uva_row("R0000003"))
    assert v == "UNKNOWN" and "hits 1" in ev


def jackson_http():
    return FakeHttp([
        ("POST", JACKSON + "/widgets", lambda u, kw: Resp(
            200, fx("phenom_widget_open_223645.json") if kw["json_body"]["jobId"] == "223645"
            else fx("phenom_widget_empty.json"))),
        ("GET", "/us/en/job/", lambda u, kw: Resp(410, fx("phenom_page_410.html"), u)),
    ])


JACKSON_INFOR = {"id": 5, "hospital_system": "Jackson Health System", "ats_platform": "Phenom", "job_id": "223645",
                 "url": "https://css-f76fc6enwqtkl6kq-prd.inforcloudsuite.com:443/hcm/Jobs/form/x",
                 "scraped_at": "2026-10-08T01:00:00+00:00"}


def test_phenom_row_without_a_phenom_url_can_read_open_but_never_closed():
    """Jackson stores Infor apply links: the widget is asked by job_id, and a
    widget miss is UNKNOWN (the stored id is not proven to be the jobId);
    the job page is not even fetched."""
    http = jackson_http()
    chk = phenom(http, rows=[JACKSON_INFOR])
    chk.canary.get(JACKSON, lambda: (True, "test"))   # board canary (search page not recorded)
    assert chk.check(JACKSON_INFOR)[0] == "OPEN"
    v, _, ev = chk.check({**JACKSON_INFOR, "job_id": "215000"})
    assert v == "UNKNOWN" and "not a Phenom job page" in ev
    assert urls(http, "inforcloudsuite") == [] and urls(http, "/job/") == []


def test_phenom_workday_linked_row_takes_workday_verdict_first():
    wd_url = "https://uva.wd1.myworkdayjobs.com/UVAJobs/job/Charlottesville-VA/Custodial_R0005339"
    http = uva_http()
    stub = StubWorkday(("CLOSED", "403-S22", "detail 403-S22; search no match"))
    assert phenom(http, workday=stub).check(uva_row("R0005339", wd_url))[0] == "CLOSED"
    assert http.calls == []   # Workday decided; the Phenom board was not asked


def test_phenom_workday_unknown_falls_back_to_the_widget_but_cannot_close():
    wd_url = "https://uva.wd1.myworkdayjobs.com/UVAJobs/job/Charlottesville-VA/Gone_R0000001"
    stub = StubWorkday(("UNKNOWN", "", "board canary failed (board total 0)"))
    http = uva_http()
    v, st, ev = phenom(http, workday=stub).check(uva_row("R0000001", wd_url))
    assert v == "UNKNOWN" and ev.startswith("workday: board canary failed") and "not a Phenom job page" in ev
    assert urls(http, "/us/en/job/R0000001") == []
    open_url = wd_url.replace("Gone_R0000001", "Custodial_R0005339")
    v, _, ev = phenom(uva_http(), workday=stub).check(uva_row("R0005339", open_url))
    assert v == "OPEN" and ev.startswith("workday: ")


def test_phenom_seqno_job_id_is_unknown():
    row = {"id": 6, "hospital_system": "UVA Health", "ats_platform": "Phenom",
           "job_id": "UOVUNIUSR0005339EXTERNALENUS", "url": "https://example.org/apply"}
    http = FakeHttp([])
    assert phenom(http).check(row)[0] == "UNKNOWN"
    assert http.calls == []


def test_phenom_canary_needs_the_listed_jobs_page_to_answer_200():
    http = uva_http([("GET", "/us/en/job/R0005339", lambda u, kw: Resp(410, "", u))])
    v, _, ev = phenom(http).check(uva_row("R0000001"))
    assert v == "UNKNOWN" and "board canary failed" in ev


def test_phenom_row_canary_catches_stored_ids_that_are_not_the_widget_job_id():
    """The review case: the board canary passes on a search-page jobId
    (R0005339), but this system's rows store another id form (here the bare
    number), so an open posting reads widget hits 0, and its job page 410
    would make it CLOSED. The newest row of the export must read OPEN first;
    it does not, so every row of the system is UNKNOWN."""
    http = uva_http()
    rows = [uva_row("0005339", scraped="2026-10-08T21:00:00+00:00", rid=41),
            uva_row("0000001", scraped="2026-10-02T21:00:00+00:00", rid=42)]
    chk = phenom(http, rows=rows)
    for row in rows:
        v, st, ev = chk.check(row)
        assert v == "UNKNOWN" and "row canary failed" in ev and "id 41" in ev, ev
    widget_ids = [kw["json_body"]["jobId"] for m, _, kw in http.calls if m == "POST"]
    assert widget_ids == ["R0005339", "0005339"]   # board canary, row canary; no row asked after


def test_phenom_unprimed_checker_never_judges():
    http = uva_http()
    v, _, ev = vs.PhenomChecker(http, workday=StubWorkday(None)).check(uva_row("R0000001"))
    assert v == "UNKNOWN" and "row canary failed (no input export" in ev


def test_phenom_row_canary_is_per_system():
    http = uva_http()
    bsw = {"id": 50, "hospital_system": "Baylor Scott & White", "ats_platform": "Phenom", "job_id": "1",
           "url": "https://jobs.bswhealth.com/us/en/job/26007805/rn", "scraped_at": "2026-10-09T00:00:00+00:00"}
    chk = phenom(http, rows=[UVA_NEWEST_OPEN, bsw])
    assert set(chk.site_rows) == {(UVA, "UVA Health"), ("https://jobs.bswhealth.com", "Baylor Scott & White")}
    assert chk.check(uva_row("R0000001"))[0] == "CLOSED"
    assert urls(http, "bswhealth") == []


def test_phenom_prime_prefers_phenom_urls_then_the_newest_scrape():
    wd = "https://uva.wd1.myworkdayjobs.com/UVAJobs/job/X/Y_R0000009"
    rows = [
        uva_row("R0000009", url=wd, scraped="2026-10-09T05:00:00+00:00", rid=1),     # newest, Workday url
        uva_row("R0000010", scraped="2026-10-06 04:00:00+00", rid=2),
        uva_row("R0000011", scraped="2026-10-06T04:30:00.12345+00:00", rid=3),      # newest Phenom url
        uva_row("R0000012", scraped="", rid=99),
        {**uva_row("R0000013", rid=4), "ats_platform": "Workday", "scraped_at": "2026-10-10T00:00:00+00:00"},
        {**uva_row("UOVUNIUSR0000014EXTERNALENUS", url="https://example.org/a", rid=5),
         "scraped_at": "2026-10-10T00:00:00+00:00"},
    ]
    picked = vs.PhenomChecker(FakeHttp([])).prime(rows)
    assert picked[(UVA, "UVA Health")]["id"] == 3
    only_wd = vs.PhenomChecker(FakeHttp([])).prime(rows[:1])
    assert only_wd[(UVA, "UVA Health")]["id"] == 1   # no Phenom url in the export: newest row overall


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


def _check_args(tmp_path, rows, **kw):
    import argparse
    rows_file = tmp_path / "rows.json"
    rows_file.write_text(json.dumps(rows), encoding="utf-8")
    base = dict(rows_file=str(rows_file), from_db=False, system=None, platform=None, days=4, limit=0,
                out_dir=str(tmp_path / "out"), max_requests=100, stop_at="23:15", workers=4,
                per_host=vs.PER_HOST, pause=vs.PAUSE[0], retry_unknown=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _verdicts(out_dir):
    return {k: r["verdict"] for k, r in vs.latest_verdicts(out_dir).items()}


def _fake_run_http(monkeypatch, make_http):
    """run_check end to end on the routes of make_http() (a FakeHttp); the
    clock is set to 12:00 UTC so the nightly-window refusal does not fire."""
    from datetime import datetime, timezone
    made = []

    class RunHttp(FakeHttp):
        OutOfBudget = vs.Http.OutOfBudget

        def __init__(self, budget, per_host=vs.PER_HOST, pause=vs.PAUSE):
            super().__init__(make_http().routes)
            self.budget, self.by_host = budget, vs.collections.Counter()
            made.append((per_host, pause))
    monkeypatch.setattr(vs, "Http", RunHttp)
    monkeypatch.setattr(vs, "utcnow", lambda: datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc))
    return made


def test_run_check_primes_the_phenom_row_canary_from_the_whole_export(tmp_path, monkeypatch):
    made = _fake_run_http(monkeypatch, uva_http)
    rows = [uva_row("R0000001", scraped="2026-10-02T00:00:00+00:00", rid=71), UVA_NEWEST_OPEN]
    args = _check_args(tmp_path, rows, per_host=1, pause=2.0)
    vs.run_check(args)
    assert _verdicts(args.out_dir) == {"71": "CLOSED", "40": "OPEN"}
    assert made == [(1, (2.0, 4.0))]
    # a run limited to the old row alone still reads the newest row as its canary
    again = tmp_path / "again"
    again.mkdir()
    args2 = _check_args(again, rows, system=["UVA Health"], limit=1)
    vs.run_check(args2)
    assert _verdicts(args2.out_dir) == {"71": "CLOSED"}


def test_run_check_ctrl_c_starts_no_new_row(tmp_path, monkeypatch):
    """The docs say Ctrl+C stops a run so it can be restarted slower; the
    pool must not work through the rest of its queue after it."""
    import time as _time

    def slow_open(u, kw):
        _time.sleep(0.05)
        return Resp(200, fx("icims_open_285412.html").replace("285412", u.split("/jobs/")[1].split("/")[0]), u)

    def interrupt(u, kw):
        raise KeyboardInterrupt()

    def make_http():
        return icims_http([("GET", "/jobs/900000/", interrupt), ("GET", "/jobs/9", slow_open)])
    _fake_run_http(monkeypatch, make_http)
    rows = [icims_row("900000")] + [{**icims_row(str(900001 + i)), "id": 100 + i} for i in range(40)]
    args = _check_args(tmp_path, rows)
    vs.run_check(args)
    written = vs.latest_verdicts(args.out_dir) if os.path.isdir(args.out_dir) else {}
    assert len(written) < 10


def test_run_check_row_canary_failure_keeps_every_row_of_the_system_unknown(tmp_path, monkeypatch):
    _fake_run_http(monkeypatch, uva_http)
    rows = [uva_row("0005339", scraped="2026-10-08T21:00:00+00:00", rid=81),
            uva_row("0000001", scraped="2026-10-02T21:00:00+00:00", rid=82)]
    args = _check_args(tmp_path, rows)
    vs.run_check(args)
    assert _verdicts(args.out_dir) == {"81": "UNKNOWN", "82": "UNKNOWN"}


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


def test_docstring_names_the_real_rate_knobs_and_the_shipping_rule():
    doc = vs.__doc__
    assert vs.PER_HOST == 2 and "PER_HOST = 2" in doc
    assert "--per-host 1 --pause 2 --retry-unknown" in doc
    assert "--workers 4" not in doc and "Lowering --workers is not the knob" in doc
    assert "same batched hospital-scraper push as A1 and A2" in doc and "redeploys" in doc


@pytest.mark.parametrize("extra", [["--per-host", "3"], ["--per-host", "0"], ["--pause", "0.1"]])
def test_check_refuses_rate_knobs_out_of_range(monkeypatch, extra):
    monkeypatch.setattr(sys, "argv", ["verify_stale.py", "check", "--rows-file", "rows.json"] + extra)
    monkeypatch.setattr(vs, "run_check", lambda args: pytest.fail("run_check must not start"))
    with pytest.raises(SystemExit) as e:
        vs.main()
    assert e.value.code == 2
