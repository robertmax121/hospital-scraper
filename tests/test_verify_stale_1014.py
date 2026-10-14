# 2026-10-14 (fix 12, stale and duplicate cleanup): tools/verify_stale.py
#   * Oracle asks ById by the WHOLE requisition id. Ascension's "2026-464305"
#     used to be cut to "2026", which answers items [] for any row, and four
#     sampled Ascension rows read CLOSED on their year alone.
#   * Phenom rows whose url is an Oracle requisition (Ascension) are CLOSED
#     only when Oracle's items [] and the Phenom widget's hits 0 agree; the
#     database name ("Ascension") finds its Phenom site through the alias.
#   * SmartRecruiters (Northwestern): the public posting API's "active" flag;
#     an old id whose applyUrl names a new id reads MOVED (re-keyed copy).
#   * Jibe fronts (UHS, stored as ats_platform iCIMS): 500 "Oops" on two
#     fetches plus a search that does not find the req_id.
# Pages and API answers recorded live on 2026-10-14 (trimmed) live in
# tests/fixtures/verify_stale. No network: FakeHttp fails on any request it
# was not told about.
import csv
import json
import os
import sys

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


class OkCanary:
    def get(self, key, probe):
        return True, "test"


# ------------------------------------------------------------------ Oracle
ORA = "https://ibpcjb.fa.ocs.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/"


def ora_row(rid, system="Ascension", platform="Phenom"):
    return {"id": 9, "hospital_system": system, "ats_platform": platform, "job_id": rid, "url": ORA + rid}


def ora_http(found=()):
    def answer(u, kw):
        for rid in found:
            if f"Id=%22{rid}%22," in u:
                return Resp(200, json.dumps({"items": [{"Id": rid, "Title": "Registered Nurse"}]}))
        return Resp(200, json.dumps({"items": []}))
    return answer


def test_oracle_asks_by_the_whole_hyphenated_id():
    http = FakeHttp([("GET", "recruitingCEJobRequisitionDetails", ora_http(found=("2026-444545",)))])
    chk = vs.OracleChecker(http)
    assert chk.check(ora_row("2026-444545", platform="Oracle HCM"))[0] == "OPEN"
    v, _, ev = chk.check(ora_row("2026-464305", platform="Oracle HCM"))
    assert v == "CLOSED" and "2026-464305" in ev
    asked = urls(http, "recruitingCEJobRequisitionDetails")
    assert all("Id=%222026%22" not in u for u in asked)
    assert any("Id=%222026-464305%22,siteNumber=CX_1" in u for u in asked)


def test_oracle_regex_takes_numeric_and_hyphen_ids_to_the_segment_end():
    assert vs._ORA.match(ORA + "1287045").group(3) == "1287045"
    assert vs._ORA.match(ORA + "2026-464305").group(3) == "2026-464305"
    assert vs._ORA.match(ORA + "2026-464305?utm_source=x").group(3) == "2026-464305"
    assert vs._ORA.match(ORA.replace("/job/", "/requisitions/preview/") + "1287045/").group(3) == "1287045"


# ------------------------------------------- Phenom front of Oracle (Ascension)
ASC = "https://jobs.ascension.org"


def widget(hit_ids=()):
    def answer(u, kw):
        wid = kw["json_body"]["jobId"]
        if wid in hit_ids:
            return Resp(200, json.dumps({"jobDetail": {"status": 200, "hits": 1, "data": {"job": {"jobId": wid}}}}))
        return Resp(200, fx("phenom_widget_empty.json"))
    return answer


def asc_checker(http):
    return vs.PhenomChecker(http, workday=None, canary=OkCanary(), oracle=vs.OracleChecker(http))


def test_phenom_base_finds_the_site_through_the_database_alias():
    assert vs.phenom_base(ora_row("1")) == ASC                      # stored "Ascension", emitted "Ascension Health"
    assert vs.phenom_base({"hospital_system": "Ascension Health", "url": ORA + "1"}) == ASC
    assert vs.phenom_base({"hospital_system": "Nobody", "url": ORA + "1"}) == ""


def test_ascension_oracle_open_reads_open_without_the_widget():
    http = FakeHttp([("GET", "recruitingCEJobRequisitionDetails", ora_http(found=("1324797",)))])
    v, _, ev = asc_checker(http).check(ora_row("1324797"))
    assert v == "OPEN" and "found" in ev
    assert urls(http, "/widgets") == []


def test_ascension_oracle_empty_and_widget_zero_reads_closed():
    http = FakeHttp([("GET", "recruitingCEJobRequisitionDetails", ora_http()),
                     ("POST", ASC + "/widgets", widget())])
    v, _, ev = asc_checker(http).check(ora_row("2026-464305"))
    assert v == "CLOSED" and "items []" in ev and "hits 0" in ev


def test_ascension_oracle_empty_but_widget_finds_it_is_unknown():
    http = FakeHttp([("GET", "recruitingCEJobRequisitionDetails", ora_http()),
                     ("POST", ASC + "/widgets", widget(hit_ids=("1298582",)))])
    v, _, ev = asc_checker(http).check(ora_row("1298582"))
    assert v == "UNKNOWN" and "hits 1" in ev


def test_ascension_oracle_empty_with_a_failed_canary_is_unknown():
    http = FakeHttp([("GET", "recruitingCEJobRequisitionDetails", ora_http()),
                     ("GET", ASC, Resp(403, "blocked"))])
    chk = vs.PhenomChecker(http, oracle=vs.OracleChecker(http))
    chk.prime([ora_row("2026-464305")])
    v, _, ev = chk.check(ora_row("2026-464305"))
    assert v == "UNKNOWN" and "canary failed" in ev
    assert urls(http, "/widgets") == []


# --------------------------------------------------------- SmartRecruiters
NW = "https://jobs.smartrecruiters.com/northwesternmedicine/"
SR_API = "https://api.smartrecruiters.com/v1/companies/northwesternmedicine/postings"


def sr_row(pid):
    return {"id": 7, "hospital_system": "Northwestern Medicine", "ats_platform": "SmartRecruiters",
            "job_id": pid, "url": NW + pid}


def sr_http(extra=()):
    return FakeHttp(list(extra) + [
        ("GET", SR_API + "?limit=1", Resp(200, fx("sr_list_northwestern.json"))),
        ("GET", SR_API + "/744000155585608", Resp(200, fx("sr_open_744000155585608.json"))),
        ("GET", SR_API + "/744000155306944", Resp(200, fx("sr_open_744000155306944.json"))),
        ("GET", SR_API + "/744000150699610", Resp(200, fx("sr_closed_744000150699610.json"))),
        ("GET", SR_API + "/744000150157032", Resp(200, fx("sr_moved_744000150157032.json"))),
    ])


def test_sr_active_posting_reads_open():
    v, st, ev = vs.SmartRecruitersChecker(sr_http()).check(sr_row("744000155306944"))
    assert (v, st) == ("OPEN", 200) and "active=true" in ev


def test_sr_inactive_posting_reads_closed():
    v, st, ev = vs.SmartRecruitersChecker(sr_http()).check(sr_row("744000150699610"))
    assert (v, st) == ("CLOSED", 200) and "active=false" in ev and "refNumber 222626" in ev


def test_sr_republished_posting_reads_moved_and_names_the_new_id():
    v, _, ev = vs.SmartRecruitersChecker(sr_http()).check(sr_row("744000150157032"))
    assert v == "MOVED" and "744000154749068" in ev and "refNumber 210742" in ev


def test_sr_404_and_bad_shapes_are_unknown():
    http = sr_http([("GET", SR_API + "/1234567", Resp(404, '{"id": "x", "message": "not found"}')),
                    ("GET", SR_API + "/7654321", Resp(200, '{"id": "7654321"}'))])
    chk = vs.SmartRecruitersChecker(http)
    assert chk.check(sr_row("1234567"))[0] == "UNKNOWN"
    assert chk.check(sr_row("7654321"))[0] == "UNKNOWN"


def test_sr_canary_failure_makes_rows_unknown_without_asking_for_them():
    http = FakeHttp([("GET", SR_API + "?limit=1", Resp(429, "slow down"))])
    v, _, ev = vs.SmartRecruitersChecker(http).check(sr_row("744000150699610"))
    assert v == "UNKNOWN" and "canary failed" in ev
    assert urls(http, "/744000150699610") == []


def test_moved_rows_count_as_done_and_are_never_retired(tmp_path):
    out = tmp_path / "sv"
    out.mkdir()
    with open(out / "smartrecruiters.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=vs.CSV_FIELDS)
        w.writeheader()
        for i, v in ((21, "MOVED"), (22, "CLOSED"), (23, "UNKNOWN")):
            w.writerow({"id": i, "hospital_system": "Northwestern Medicine", "ats_platform": "SmartRecruiters", "verdict": v})
    assert vs.done_ids(str(out), "SmartRecruiters", retry_unknown=True) == {"21", "22"}
    import argparse
    args = argparse.Namespace(out_dir=str(out), out_sql=str(tmp_path / "x.sql"), out_restore=str(tmp_path / "xr.sql"),
                              stale_before="2026-10-14 19:00+00", system=None, batch=1000,
                              backup_table="retired_closed_backup_20261014", db_write=True)
    vs.run_sql(args)
    text = open(args.out_sql, encoding="utf-8").read()
    assert "id in (22)" in text and "21" not in text.replace("20261014", "")


# -------------------------------------------------------------------- Jibe
UHS = "https://jobs.uhsinc.com"


def uhs_row(req):
    return {"id": 8, "hospital_system": "Universal Health Services", "ats_platform": "iCIMS",
            "job_id": req, "url": f"{UHS}/jobs/{req}?lang=en-us"}


def jibe_http(extra=()):
    return FakeHttp(list(extra) + [
        ("GET", "/api/jobs?page=1&limit=10&keywords=375157", Resp(200, fx("jibe_search_375157.json"))),
        ("GET", "/api/jobs?page=1&limit=10&keywords=371160", Resp(200, fx("jibe_search_371160.json"))),
        ("GET", "/api/jobs?page=1&limit=10&keywords=", Resp(200, fx("jibe_list_uhs.json"))),
        ("GET", "/jobs/375384", lambda u, kw: Resp(200, fx("jibe_open_375157.html").replace("375157", "375384"),
                                                   UHS + "/careers/jobs/375384?lang=en-us")),
        ("GET", "/jobs/375157", lambda u, kw: Resp(200, fx("jibe_open_375157.html"), UHS + "/careers/jobs/375157?lang=en-us")),
        ("GET", "/jobs/371160", lambda u, kw: Resp(500, fx("jibe_500_oops.html"), UHS + "/careers/jobs/371160?lang=en-us")),
    ])


def test_jibe_open_posting_reads_open():
    v, st, _ = vs.JibeChecker(jibe_http()).check(uhs_row("375157"))
    assert (v, st) == ("OPEN", 200)


def test_jibe_oops_twice_and_no_search_hit_reads_closed():
    http = jibe_http()
    v, st, ev = vs.JibeChecker(http).check(uhs_row("371160"))
    assert (v, st) == ("CLOSED", 500) and "total 0" in ev
    assert len(urls(http, "/jobs/371160")) == 2


def test_jibe_oops_but_the_search_finds_the_req_is_unknown():
    http = jibe_http([("GET", "/jobs/375157", Resp(500, fx("jibe_500_oops.html")))])
    chk = vs.JibeChecker(http)
    chk.canary = OkCanary()
    v, _, ev = chk.check(uhs_row("375157"))
    assert v == "UNKNOWN" and "search finds" in ev


def test_jibe_oops_then_open_is_unknown():
    pages = iter([Resp(500, fx("jibe_500_oops.html")), Resp(200, fx("jibe_open_375157.html").replace("375157", "371160"))])
    http = jibe_http([("GET", "/jobs/371160", lambda u, kw: next(pages))])
    chk = vs.JibeChecker(http)
    chk.canary = OkCanary()
    assert chk.check(uhs_row("371160"))[0] == "UNKNOWN"


def test_jibe_closed_text_on_every_page_is_not_a_closed_signal():
    """The generic checker read every UHS page 'page says neither': the
    front's NO_JOBS_404 text rides on open pages too."""
    page = fx("jibe_open_375157.html")
    assert "no longer available" in page and vs._has_jobposting(page)


def test_jibe_canary_failure_makes_rows_unknown():
    http = FakeHttp([("GET", "/api/jobs?page=1&limit=10&keywords=", Resp(503, "busy"))])
    v, _, ev = vs.JibeChecker(http).check(uhs_row("371160"))
    assert v == "UNKNOWN" and "canary failed" in ev
    assert urls(http, "/jobs/371160") == []


# --------------------------------------------------------------- dispatch
def test_checker_for_row_dispatch_1014():
    http = FakeHttp([])
    pick = lambda row: type(vs.checker_for_row(row, http)).__name__  # noqa: E731
    assert pick(sr_row("744000150699610")) == "SmartRecruitersChecker"
    assert pick({**sr_row("1"), "ats_platform": "Custom", "url": NW + "744000150699610-medical-assistant"}) \
        == "SmartRecruitersChecker"
    assert pick(uhs_row("371160")) == "JibeChecker"                 # stored as iCIMS, a Jibe front
    assert pick({**uhs_row("1"), "ats_platform": "Jibe"}) == "JibeChecker"
    assert pick({"ats_platform": "iCIMS", "url": "https://careers-primehealthcare.icims.com/jobs/1/job"}) == "IcimsChecker"
    assert pick(ora_row("2026-464305", system="Lifepoint Health", platform="Custom")) == "OracleChecker"
    assert pick(ora_row("2026-464305")) == "PhenomChecker"           # Ascension: Phenom asks Oracle first
    assert "jobs.uhsinc.com" in vs.jibe_hosts()
