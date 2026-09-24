"""Fixed Workday facets for shared tenants (WD_TENANT_FACETS, 2026-09-24).
No network: scraper.req is replaced by a fake that records each request body
and answers from a script."""
import asyncio
import logging

import scraper

AHN = "Allegheny Health Network"


class _Resp:
    status = 200

    def __init__(self, body):
        self._body = body

    async def json(self, content_type=None):
        return self._body


class _Ctx:
    def __init__(self, body):
        self.resp = _Resp(body)

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *a):
        return False


def _posting(n, loc="Pittsburgh PA, 15212, 320 E North Ave"):
    return {"title": f"Registered Nurse {n}", "externalPath": f"/job/Pittsburgh-PA/RN_J{n:06d}",
            "locationsText": loc, "postedOn": "Posted Today", "bulletFields": [f"J{n:06d}"]}


def _fake_req(monkeypatch, answer):
    """answer(body) -> response JSON. Returns the list of request bodies."""
    bodies = []

    def req(session, method, url, **kw):
        body = kw.get("json") or {}
        bodies.append(body)
        return _Ctx(answer(body))

    async def no_wait():
        return None

    monkeypatch.setattr(scraper, "req", req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    return bodies


def test_ahn_is_crawled_inside_the_ahn_facility_facet(monkeypatch):
    base = scraper.WD_TENANT_FACETS[AHN]
    assert list(base) == ["locationHierarchy1"] and len(base["locationHierarchy1"]) == 11
    bodies = _fake_req(monkeypatch, lambda b: {"total": 3, "jobPostings": [_posting(i) for i in range(3)]})
    jobs = asyncio.run(scraper.scrape_workday(None, AHN, scraper.WORKDAY_TENANTS[AHN]))
    assert len(jobs) == 3
    assert bodies[0]["appliedFacets"] == base
    assert all(j.hospital_system == AHN and j.city == "Pittsburgh" and j.state == "PA" for j in jobs)


def test_a_tenant_without_a_fixed_facet_sends_none(monkeypatch):
    bodies = _fake_req(monkeypatch, lambda b: {"total": 2, "jobPostings": [_posting(i, "Austin, TX") for i in range(2)]})
    jobs = asyncio.run(scraper.scrape_workday(None, "Plain Board", ("plain", "5", "External")))
    assert len(jobs) == 2
    assert "appliedFacets" not in bodies[0]


def test_the_probe_and_every_slice_keep_the_fixed_facet(monkeypatch):
    base = {"locationHierarchy1": ["fac1", "fac2"]}
    monkeypatch.setitem(scraper.WD_TENANT_FACETS, "Window Board", base)

    def answer(body):
        applied = body.get("appliedFacets") or {}
        if body.get("limit") == 1:
            return {"total": 2100, "jobPostings": [],
                    "facets": [{"facetParameter": "jobFamilyGroup",
                                "values": [{"id": "g1", "descriptor": "Nursing", "count": 1},
                                           {"id": "g2", "descriptor": "Clinical", "count": 1}]}]}
        if "jobFamilyGroup" in applied:
            n = 5000 + int(applied["jobFamilyGroup"][0][1:])
            return {"total": 1, "jobPostings": [_posting(n, "Austin, TX")]}
        off = body["offset"]
        return {"total": 2100, "jobPostings": [_posting(off + k, "Austin, TX") for k in range(20)]}

    bodies = _fake_req(monkeypatch, answer)
    jobs = asyncio.run(scraper.scrape_workday(None, "Window Board", ("window", "5", "External")))
    sweep = [b for b in bodies if b.get("limit") == 20 and "jobFamilyGroup" not in (b.get("appliedFacets") or {})]
    probe = [b for b in bodies if b.get("limit") == 1]
    slices = [b for b in bodies if "jobFamilyGroup" in (b.get("appliedFacets") or {})]
    assert len(sweep) == 100 and all(b["appliedFacets"] == base for b in sweep)   # hit the 2,000 window
    assert len(probe) == 1 and probe[0]["appliedFacets"] == base
    assert [b["appliedFacets"] for b in slices] == [{**base, "jobFamilyGroup": ["g1"]},
                                                    {**base, "jobFamilyGroup": ["g2"]}]
    assert len(jobs) == 2002


def test_a_fixed_facet_tenant_with_zero_rows_warns(monkeypatch, caplog):
    _fake_req(monkeypatch, lambda b: {"total": 0, "jobPostings": []})
    with caplog.at_level(logging.WARNING, logger=scraper.logger.name):
        jobs = asyncio.run(scraper.scrape_workday(None, AHN, scraper.WORKDAY_TENANTS[AHN]))
    assert jobs == []
    assert any("0 rows inside its fixed facet" in r.getMessage() for r in caplog.records)
