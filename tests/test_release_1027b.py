"""Release 1027b integration checks (2026-10-27): the seams between the
merged branches hs/payparse2-1027, hs/gapfix-1027, hs/gapcfg-1027,
hs/gapadp-1027 and hs/gaplt-1027.

1. Workday: the gap configs pick the window-hit slice (WD_SLICE_PARAM, a
   parameter the fixed facet sets is never sliced) and the gap fixes split a
   slice past the window again. Both now run in one path.
2. Every _run_small_board label has a host, and every new detail pass is a
   known body platform.
3. Config dicts: no label repeated inside one literal (beyond the 5 Workday
   repeats already on main), and no board configured under two labels.
No network, no database."""
import ast
import asyncio
import json
import pathlib
import re

import pytest

import scraper


async def _no_wait(*a, **k):
    return None


class _R:
    status = 200

    def __init__(self, body):
        self._body = body

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return json.dumps(self._body)


class _Ctx:
    def __init__(self, resp):
        self.resp = resp

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


# ══ 1. Workday: chosen slice, then split again ═══════════════════════════════
US, DE = "c-us", "c-de"
TX, TN = "s-tx", "s-tn"
FT, PT = "t-ft", "t-pt"
FAM = "g-clinical"
KEYS = {"locationCountry": "_c", "locationRegionStateProvince": "_s", "timeType": "_t", "jobFamilyGroup": "_f"}


def _post(n, country, state, tt):
    return {"title": f"Registered Nurse {n}", "externalPath": f"/job/Austin-TX/RN_R{n:07d}",
            "locationsText": "Austin, TX", "postedOn": "Posted Today", "bulletFields": [f"R{n:07d}"],
            "_c": country, "_s": state, "_t": tt, "_f": FAM}


def _board():
    posts, n = [], 0
    for country, state, tt, k in ((US, TX, FT, 35), (US, TX, PT, 25), (US, TN, FT, 20), (DE, None, FT, 30)):
        for _ in range(k):
            posts.append(_post(n, country, state, tt))
            n += 1
    return posts


def _fake_tenant(monkeypatch, posts, window):
    bodies = []
    monkeypatch.setattr(scraper, "WD_FACET_WINDOW", window)
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)

    def counts(sub, key, ids):
        return [{"id": i, "descriptor": i, "count": sum(1 for p in sub if p[key] == i)} for i in ids]

    def answer(body):
        applied = body.get("appliedFacets") or {}
        sub = [p for p in posts if all(p[KEYS[k]] in v for k, v in applied.items())]
        if body.get("limit") == 1:
            return {"total": min(len(sub), window), "jobPostings": sub[:1], "facets": [
                {"facetParameter": "jobFamilyGroup", "values": counts(sub, "_f", (FAM,))},
                {"facetParameter": "timeType", "values": counts(sub, "_t", (FT, PT))},
                {"facetParameter": "locationMainGroup", "values": [
                    {"facetParameter": "locationCountry", "descriptor": "Country", "values": counts(sub, "_c", (US, DE))},
                    {"facetParameter": "locationRegionStateProvince", "descriptor": "Region/State",
                     "values": counts(sub, "_s", (TX, TN))}]}]}
        off = body["offset"]
        page = sub[off:off + 20] if off < window else sub[window - 20:window]   # Workday repeats past the window
        return {"total": min(len(sub), window) if off == 0 else 0, "jobPostings": page}

    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        bodies.append(body)
        return _Ctx(_R(answer(body)))

    monkeypatch.setattr(scraper, "req", req)
    return bodies


def test_slice_counts_come_from_the_group_the_values_came_from():
    data = {"facets": [
        {"facetParameter": "timeType", "values": [{"id": FT, "count": 7}]},
        {"facetParameter": "locationMainGroup", "values": [
            {"facetParameter": "locationRegionStateProvince", "values": [{"id": TX, "count": 60},
                                                                          {"id": TN, "count": 20}]}]}]}
    assert scraper._wd_slice_counts(data, "locationRegionStateProvince") == {TX: 60, TN: 20}
    assert scraper._wd_slice_counts(data, None) == {}
    assert scraper._wd_slice_counts({}, "timeType") == {}


def test_a_state_slice_past_the_window_is_split_again_inside_the_fixed_facet(monkeypatch):
    # Fresenius in miniature (window 40): the US filter is fixed (DE never
    # read), the window-hit crawl is sliced by state (WD_SLICE_PARAM), Texas
    # (60) is past the window and is split by timeType (35 + 25) instead of
    # being crawled whole; Tennessee (20) is crawled whole.
    system = "Fresenius Medical Care"
    monkeypatch.setitem(scraper.WD_TENANT_FACETS, system, {"locationCountry": [US]})
    monkeypatch.setitem(scraper.WD_SLICE_PARAM, system, "locationRegionStateProvince")
    posts = _board()
    bodies = _fake_tenant(monkeypatch, posts, window=40)
    jobs = asyncio.run(scraper.scrape_workday(None, system, ("fme", "3", "fme")))
    us = [p for p in posts if p["_c"] == US]
    assert len(jobs) == len(us) == 80
    assert all((b.get("appliedFacets") or {}).get("locationCountry") == [US] for b in bodies)
    crawls = [b["appliedFacets"] for b in bodies if b.get("limit") == 20]
    assert {"locationCountry": [US], "locationRegionStateProvince": [TX]} not in crawls     # never crawled whole
    assert {"locationCountry": [US], "locationRegionStateProvince": [TX], "timeType": [FT]} in crawls
    assert {"locationCountry": [US], "locationRegionStateProvince": [TX], "timeType": [PT]} in crawls
    assert {"locationCountry": [US], "locationRegionStateProvince": [TN]} in crawls
    assert not any("jobFamilyGroup" in c for c in crawls)
    assert system not in scraper.COMPLETE_SYSTEMS                     # a sliced crawl never claims


def test_the_fixed_parameter_is_never_the_split(monkeypatch):
    # Same board, but every US posting sits in one state and one time type:
    # the only facet with two values inside the slice would be the fixed
    # country, which is never used, so the slice keeps its window.
    system = "Fresenius Medical Care"
    monkeypatch.setitem(scraper.WD_TENANT_FACETS, system, {"locationCountry": [US]})
    monkeypatch.setitem(scraper.WD_SLICE_PARAM, system, "locationRegionStateProvince")
    posts = [_post(n, US, TX, FT) for n in range(60)] + [_post(100 + n, DE, TX, PT) for n in range(10)]
    bodies = _fake_tenant(monkeypatch, posts, window=40)
    jobs = asyncio.run(scraper.scrape_workday(None, system, ("fme", "3", "fme")))
    assert len(jobs) == 40
    assert all((b.get("appliedFacets") or {}).get("locationCountry") == [US] for b in bodies)


# ══ 2. Small boards and body platforms ═══════════════════════════════════════
class _Stop(Exception):
    pass


@pytest.mark.parametrize("run_name,cfg_name", [
    ("run_applicantstack", "APPLICANTSTACK_ORGS"), ("run_jazzhr", "JAZZHR_ORGS"),
    ("run_jobaps", "JOBAPS_SITES"), ("run_silkroad", "SILKROAD_SITES"),
    ("run_hcshiring", "HCSHIRING_SITES"), ("run_applitrack", "APPLITRACK_BOARDS"),
])
def test_every_small_board_label_has_a_host(monkeypatch, run_name, cfg_name):
    seen = []

    async def fake_gather(session, items, scrape, host_of):
        seen.extend(host_of(cfg) for _s, cfg in items)
        raise _Stop

    monkeypatch.setattr(scraper, "_gather_by_host", fake_gather)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    assert getattr(scraper, cfg_name)
    with pytest.raises(_Stop):
        asyncio.run(getattr(scraper, run_name)(None))
    assert seen and all(isinstance(h, str) and h for h in seen)


def test_new_detail_passes_are_known_body_platforms_and_feeds_are_configured():
    assert {"SilkRoad", "HCSHiring", "SitemapBoard", "AppliTrack"} <= set(scraper.KNOWN_BODY_PLATFORMS)
    assert {"SILKROAD_SITES", "HCSHIRING_SITES", "SITEMAP_SITES", "APPLITRACK_BOARDS"} <= set(scraper.FEED_CONFIGS)
    assert len(set(scraper.FEED_CONFIGS)) == len(scraper.FEED_CONFIGS)
    assert all(hasattr(scraper, f) for f in scraper.FEED_CONFIGS)


# ══ 3. Config dicts ═════════════════════════════════════════════════════════
# Repeated keys already on origin/main cd8c583 (the later line wins).
_MAIN_REPEATS = {("WORKDAY_TENANTS", "LifeStance Health"), ("WORKDAY_TENANTS", "Fresenius Medical Care"),
                 ("WORKDAY_TENANTS", "GoHealth Urgent Care"), ("WORKDAY_TENANTS", "Memorial Healthcare System"),
                 ("WORKDAY_TENANTS", "Saint Luke's Health System")}
_CHECKED = set(scraper.FEED_CONFIGS) | {"WD_TENANT_FACETS", "WD_SLICE_PARAM", "HOSPITAL_SYSTEM_ALIASES",
                                         "JIBE_POSTING_URL", "JIBE_FACILITY_TAG", "JIBE_LEAVE_OUT"}


def test_no_config_literal_repeats_a_label():
    tree = ast.parse(pathlib.Path(scraper.__file__).read_text(encoding="utf-8"))
    repeats = set()
    for node in tree.body:
        target = (node.targets[0] if isinstance(node, ast.Assign) else getattr(node, "target", None))
        name = getattr(target, "id", None)
        if name not in _CHECKED or not isinstance(getattr(node, "value", None), ast.Dict):
            continue
        keys = [k.value for k in node.value.keys if isinstance(k, ast.Constant)]
        repeats |= {(name, k) for k in keys if keys.count(k) > 1}
    assert repeats <= _MAIN_REPEATS, sorted(repeats - _MAIN_REPEATS)


def _fingerprint(feed, label, value):
    def norm(v):
        if isinstance(v, str):
            s = re.sub(r"^https?://(www\.)?", "", v.strip().lower())
            return s.rstrip("/")
        if isinstance(v, (list, tuple)):
            return tuple(norm(x) for x in v)
        if isinstance(v, dict):
            return tuple(sorted((str(k), repr(norm(x))) for k, x in v.items()))
        if isinstance(v, re.Pattern):
            return v.pattern
        return repr(v)
    fp = norm(value)
    if feed == "WORKDAY_TENANTS":
        # tenant + site; a shared tenant read under a fixed facet is its own board
        fp = (value[0].lower(), value[2].lower(), repr(norm(scraper.WD_TENANT_FACETS.get(label) or {})))
    return (feed, fp)


def test_no_board_is_configured_under_two_labels():
    seen = {}
    clashes = []
    for feed in scraper.FEED_CONFIGS:
        cfg = getattr(scraper, feed)
        items = cfg.items() if isinstance(cfg, dict) else ((e[0], e[1:]) for e in cfg)
        for label, value in items:
            fp = _fingerprint(feed, label, value)
            if fp in seen and seen[fp] != label:
                clashes.append((feed, seen[fp], label))
            seen.setdefault(fp, label)
    assert clashes == []
