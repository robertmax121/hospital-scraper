"""2026-10-13: review of 74b2e3a (Workday externalPath locations), two majors.

1. The listing blanks "N Locations" before the fill, so the tenant default
   handed its head-office town to multi-site rows ("/job/Florence/" on MUSC
   -> Charleston, SC). Such a row now takes the default's state alone unless
   the facets or the path place it (WD_SITE_DEFAULT_CITY tenants excepted).
2. normalize_job's SYSTEM_LOCATION_DEFAULTS / FACILITY_LOCATION_MAP fallback
   paired its town with any state ("New Orleans, MS"); the town now goes only
   with its own state. A path that leads with a spelled town and ends with the
   state ("Bay-St-Louis---Gulf-Coast-Region---Mississippi") gives that town.

No network: scrape_workday runs against a fake req like test_cov3_round3.
"""
import asyncio

import scraper as S
from scraper import Job


class J:
    def __init__(self, city, state, loc=""):
        self.city, self.state, self.location = city, state, loc


def seg(s, spellings=None):
    return S._wd_segment_location(s, spellings)


def _apply(rows, default=None, site_default=False, path_state=None, path_city=None):
    jobs = [J(c, s, loc) for c, s, loc, _ in rows]
    ext = {id(j): e for j, (_, _, _, e) in zip(jobs, rows)}
    S._wd_apply_locations(jobs, ext, path_city or {}, path_state or {}, default, site_default)
    return [(j.city, j.state) for j in jobs]


def _job(system, city="", state="", location="", name=None, job_id="1"):
    return Job(title="Registered Nurse", hospital_system=system, hospital_name=name or system,
               city=city, state=state, location=location, specialty="", job_type="",
               url=f"https://example.org/{job_id}", job_id=job_id, posted_date="",
               description="", ats_platform="Workday")


def _norm(job):
    d = S.normalize_job(job)
    return d["city"], d["state"], d["location"]


# -- major 1: "N Locations" never takes the head-office town ---------------

MUSC = ("Charleston", "SC")
SOLUTION = ("Manchester", "NH")


def test_musc_multi_site_row_named_by_a_bare_town_gets_the_state_alone():
    assert seg("Florence") == ("", "")                     # the path names no state
    assert _apply([("", "", "2 Locations", "/job/Florence/RN_R1")], default=MUSC) == [("", "SC")]
    # the literal parse (as origin/main kept it) gives the same
    assert _apply([("2 Locations", "", "2 Locations", "/job/Florence/RN_R1")], default=MUSC) == [("", "SC")]


def test_solutionhealth_street_path_gets_the_state_alone():
    assert seg("Stratham---118-Portsmouth-Ave") == ("", "")
    out = _apply([("", "", "3 Locations", "/job/Stratham---118-Portsmouth-Ave/LNA_R2")], default=SOLUTION)
    assert out == [("", "NH")]


def test_multi_site_row_still_takes_a_town_the_path_or_facets_name():
    assert _apply([("", "", "2 Locations", "/job/Florence-SC/RN_R3")], default=MUSC) == [("Florence", "SC")]
    out = _apply([("", "", "2 Locations", "/job/Florence/RN_R4")], default=MUSC,
                 path_city={"/job/Florence/RN_R4": "Florence"})
    assert out == [("Florence", "SC")]
    # a located sibling with the same segment spells it too
    rows = [("Florence", "SC", "Florence, SC", "/job/Florence/RN_R5"),
            ("", "", "2 Locations", "/job/Florence/RN_R6")]
    assert _apply(rows, default=MUSC)[1] == ("Florence", "SC")


def test_multi_site_row_whose_path_is_the_default_town_keeps_it():
    # 2026-10-12 sample: MUSC "4 Locations" /job/Charleston/, UAMS /job/Little-Rock/,
    # NMMC /job/Tupelo---Retina-Clinic/
    assert _apply([("", "", "4 Locations", "/job/Charleston/RN_R20")], default=MUSC) == [("Charleston", "SC")]
    assert _apply([("", "", "2 Locations", "/job/Little-Rock/RN_R21")],
                  default=("Little Rock", "AR")) == [("Little Rock", "AR")]
    assert _apply([("", "", "2 Locations", "/job/Tupelo---Retina-Clinic/RN_R22")],
                  default=("Tupelo", "MS")) == [("Tupelo", "MS")]
    # only a whole chunk counts, and never a remote or home segment
    assert not S._wd_path_names_town("/job/North-Kansas-City/X_R1", "Kansas City")
    assert not S._wd_path_names_town("/job/Charleston---Remote/X_R2", "Charleston")
    assert not S._wd_path_names_town("/job/Charleston-Work-From-Home/X_R3", "Charleston")
    assert not S._wd_path_names_town("", "Charleston")
    assert _apply([("", "", "2 Locations", "/job/Charleston---Remote/RN_R23")], default=MUSC) == [("", "SC")]
    assert _apply([("", "", "2 Locations", "/job/UAMS/RN_R24")], default=("Little Rock", "AR")) == [("", "AR")]


def test_single_site_rows_still_take_the_tenant_default_town():
    rows = [("", "", "", "/job/x/RN_R7"),
            ("", "", "Denver Health Main Campus", "/job/Denver-Health-Main-Campus/RN_R8")]
    assert S._wd_segment_location("Denver-Health-Main-Campus") == ("", "")
    jobs = [J(c, s, loc) for c, s, loc, _ in rows]
    ext = {id(j): e for j, (_, _, _, e) in zip(jobs, rows)}
    assert S._wd_apply_locations(jobs, ext, {}, {}, ("Denver", "CO")) == (0, 2)
    assert [(j.city, j.state) for j in jobs] == [("Denver", "CO"), ("Denver", "CO")]
    # a job object without a location attribute (older callers) is a single site
    class Bare:
        city, state = "", ""
    b = Bare()
    S._wd_apply_locations([b], {id(b): "/job/y/R9"}, {}, {}, ("Morgantown", "WV"))
    assert (b.city, b.state) == ("Morgantown", "WV")


def test_site_default_city_tenant_still_fills_its_town_for_multi_site_rows():
    out = _apply([("", "", "2 Locations", "/job/SE-Health-Hospital/RN_R10")],
                 default=("Dothan", "AL"), site_default=True)
    assert out == [("Dothan", "AL")]


def _page(postings):
    return {"total": len(postings), "facets": [], "jobPostings": postings}


def _posting(n, loc, ext):
    return {"title": f"Registered Nurse {n}", "externalPath": ext, "locationsText": loc,
            "bulletFields": [f"R{n:05d}"], "postedOn": "Posted Today", "timeType": "Full time"}


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status = body, status

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, body, status=200):
        self.resp = _Resp(body, status)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


def _run_tenant(monkeypatch, system, postings):
    page = _page(postings)

    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        if body.get("limit") == 1 or (body.get("offset", 0) == 0 and not body.get("appliedFacets")):
            return _Ctx(page)
        return _Ctx({"total": 0, "jobPostings": []})

    async def no_wait():
        return None

    monkeypatch.setattr(S, "req", req)
    monkeypatch.setattr(S, "jitter", no_wait)
    monkeypatch.setattr(S, "WD_FETCH_DESCRIPTIONS", False)
    jobs = asyncio.run(S.scrape_workday(None, system, S.WORKDAY_TENANTS[system]))
    return {j.job_id: _norm(j) for j in jobs}


def test_musc_crawl_end_to_end_keeps_multi_site_rows_off_charleston(monkeypatch):
    assert S.WD_TENANT_DEFAULT["MUSC Health"] == MUSC
    out = _run_tenant(monkeypatch, "MUSC Health", [
        _posting(1, "2 Locations", "/job/Florence/RN_R00001"),
        _posting(2, "Charleston, SC", "/job/Charleston-SC/RN_R00002"),
        _posting(3, "MUSC Health Lancaster Medical Center", "/job/Lancaster/RN_R00003"),
        _posting(6, "4 Locations", "/job/Charleston/RN_R00006"),
    ])
    assert out["R00001"] == ("", "SC", "SC")
    assert out["R00006"] == ("Charleston", "SC", "Charleston, SC")
    assert out["R00002"] == ("Charleston", "SC", "Charleston, SC")
    # a site-named single-location row: the site name blocks the default (no
    # Charleston); since 2026-10-20 (messy locations) normalize_job reads the
    # town the site name carries ("MUSC Health Lancaster Medical Center")
    assert out["R00003"] == ("Lancaster", "SC", "Lancaster, SC")


def test_solutionhealth_crawl_end_to_end(monkeypatch):
    out = _run_tenant(monkeypatch, "SolutionHealth", [
        _posting(4, "3 Locations", "/job/Stratham---118-Portsmouth-Ave/LNA_R00004"),
        _posting(5, "Nashua", "/job/Nashua/RN_R00005"),
    ])
    assert out["R00004"] == ("", "NH", "NH")
    assert out["R00005"] == ("Nashua", "NH", "Nashua, NH")     # WD_FACILITY_MAP town


# -- major 2: the fallback town only goes with its own state ---------------

def test_ochsner_mississippi_row_never_gets_new_orleans():
    assert S.SYSTEM_LOCATION_DEFAULTS["ochsner health"] == ("New Orleans", "LA")
    assert _norm(_job("Ochsner Health", state="MS", location="2 Locations")) == ("", "MS", "MS")
    # a facility-level fallback follows the same rule
    assert _norm(_job("Ochsner Health", state="MS", name="Ochsner Medical Center - Kenner")) == ("", "MS", "MS")


def test_fallback_town_still_fills_a_row_in_its_own_state_or_with_no_state():
    assert _norm(_job("Ochsner Health", state="LA", location="2 Locations")) == \
        ("New Orleans", "LA", "New Orleans, LA")
    assert _norm(_job("Ochsner Health", state="la")) == ("New Orleans", "LA", "New Orleans, LA")
    assert _norm(_job("Ochsner Health")) == ("New Orleans", "LA", "New Orleans, LA")
    assert _norm(_job("Ochsner Health", name="Ochsner Medical Center - Kenner")) == ("Kenner", "LA", "Kenner, LA")
    # a row with a town and no state still takes the fallback state, as before
    assert _norm(_job("Ochsner Health", city="Metairie")) == ("Metairie", "LA", "Metairie, LA")


def test_ochsner_region_paths_end_to_end():
    rows = [("", "", "2 Locations", "/job/Picayune---Gulf-Coast-Region---Mississippi/RN_R11"),
            ("", "", "2 Locations", "/job/Bay-St-Louis---Gulf-Coast-Region---Mississippi/RN_R12"),
            ("", "", "2 Locations", "/job/Covington---Northshore-Region---Louisiana/RN_R13")]
    jobs = [J(c, s, loc) for c, s, loc, _ in rows]
    ext = {id(j): e for j, (_, _, _, e) in zip(jobs, rows)}
    S._wd_apply_locations(jobs, ext, {}, {}, S.WD_TENANT_DEFAULT.get("Ochsner Health"))
    assert [(j.city, j.state) for j in jobs] == [("", "MS"), ("Bay St. Louis", "MS"), ("", "LA")]
    normed = [_norm(_job("Ochsner Health", j.city, j.state, j.location, job_id=str(i)))
              for i, j in enumerate(jobs)]
    assert normed[0] == ("", "MS", "MS")                               # not "New Orleans, MS"
    assert normed[1] == ("Bay St. Louis", "MS", "Bay St. Louis, MS")
    assert normed[2] == ("New Orleans", "LA", "New Orleans, LA")       # same state: unchanged rule


def test_first_chunk_town_when_the_state_chunk_is_last():
    assert seg("Bay-St-Louis---Gulf-Coast-Region---Mississippi") == ("Bay St. Louis", "MS")
    assert seg("Picayune---Gulf-Coast-Region---Mississippi") == ("", "MS")
    assert seg("Meridian---Central-Mississippi-Region---Mississippi") == ("", "MS")
    # the punctuation table is per state
    assert seg("St-Louis---Metro-East-Region---Illinois") == ("", "IL")
    # the tenant's own spelling counts too
    sp = {(S._wd_city_key("Meridian"), "MS"): "Meridian"}
    assert seg("Meridian---Central-Mississippi-Region---Mississippi", sp) == ("Meridian", "MS")
    # remote still gives no town; the chunk before the state still wins
    assert seg("Bay-St-Louis---Remote---Mississippi") == ("", "MS")
    assert seg("Saint-Lukes-Hospital------4401-Wornall-Rd------Kansas-City------MO") == ("Kansas City", "MO")
