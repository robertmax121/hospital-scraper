"""The nightly link check also covers the job pages people opened (Bounce QA
re-analysis, 2026-10-26, item 4). UF Health 29002321 was live with Apply
while its employer page answered 404, and had never been checked: the
front-pool check reads only rows with a posted wage. No network, no
database: fake aiohttp sessions."""
import asyncio
from datetime import datetime, timezone

import validate_front_pool_urls as F


class _Resp:
    def __init__(self, status=200, data=None):
        self.status, self._data = status, data

    async def json(self):
        return self._data

    async def text(self):
        return ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def test_landed_job_id_reads_job_pages_only():
    assert F._landed_job_id("/jobs/chaplain-pastoral-care-full-time-days-jacksonville-fl-29002321") == 29002321
    assert F._landed_job_id("/jobs/rn-icu-austin-tx-38367096?rdt_cid=abc123&utm_source=reddit") == 38367096
    assert F._landed_job_id("/jobs/rn-icu-austin-tx-38367096/") == 38367096
    assert F._landed_job_id("/travel/pediatric-infusion-rn-medford-or-2004904") is None
    assert F._landed_job_id("/jobs/state/tx") is None
    assert F._landed_job_id("/jobs/") is None
    assert F._landed_job_id(None) is None and F._landed_job_id("") is None


def test_fetch_landed_pages_events_newest_first_and_keeps_active_rows():
    # 1,000 events on the first page (a full page asks for the next), then 3.
    first = [{"id": 5000 - i, "page_path": f"/jobs/job-{100 + (i % 7)}"} for i in range(1000)]
    second = [{"id": 3999, "page_path": "/jobs/chaplain-jacksonville-fl-29002321?rdt_cid=x"},
              {"id": 3998, "page_path": "/travel/rn-medford-or-2004904"},
              {"id": 3997, "page_path": "/jobs/closed-job-555"}]
    active = {100, 101, 102, 103, 104, 105, 106, 29002321}       # 555 is no longer active
    calls = []

    class _S:
        def get(self, url, **kw):
            calls.append(url)
            if "/waypoint_events?" in url:
                assert "event_type=eq.pageview" in url and "ua_is_bot=not.is.true" in url
                assert "created_at=gte.2026-10-12T" in url and "page_path=like./jobs/*" in url
                assert "order=id.desc" in url
                return _Resp(200, second if "id=lt.4001" in url else first)
            ids = [int(x) for x in url.split("id=in.(")[1].split(")")[0].split(",")]
            assert "is_active=eq.true" in url
            return _Resp(200, [{"id": i, "url": f"https://jobs.example.org/{i}", "hospital_system": "S"}
                               for i in ids if i in active])

    now = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
    rows = asyncio.run(F._fetch_landed(_S(), "https://db.example", {"User-Agent": "x"}, now=now))
    assert [r["id"] for r in rows] == [100, 101, 102, 103, 104, 105, 106, 29002321]
    assert sum("/waypoint_events?" in u for u in calls) == 2


def _main_with(monkeypatch, pool, landed, dead, landed_raises=False):
    async def fake_pool(session, sb_url, headers):
        return pool

    async def fake_landed(session, sb_url, headers, now=None):
        if landed_raises:
            raise RuntimeError("events table unreadable")
        return landed

    checked = []

    async def fake_check(session, sem, row):
        checked.append(row["id"])
        return row["id"], row["hospital_system"], 404 if row["id"] in dead else 200

    patched = []

    class _Session:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def patch(self, url, **kw):
            patched.append((url, kw.get("json")))
            return _Resp(204)

    monkeypatch.setattr(F, "_env", lambda: ("https://db.example", "k"))
    monkeypatch.setattr(F, "_fetch_pool", fake_pool)
    monkeypatch.setattr(F, "_fetch_landed", fake_landed)
    monkeypatch.setattr(F, "_check", fake_check)
    monkeypatch.setattr(F.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(F.aiohttp, "TCPConnector", lambda **k: None)
    monkeypatch.setattr(F.site_revalidate, "note_retired", lambda ids, src: len(ids or []))
    summary = asyncio.run(F.main())
    retired = sorted(int(x) for u, body in patched if body.get("is_active") is False
                     for x in u.split("id=in.(")[1].rstrip(")").split(","))
    return summary, checked, retired


def _rows(ids, system="Sys"):
    return [{"id": i, "url": f"https://jobs.example.org/{i}", "hospital_system": system} for i in ids]


def test_an_opened_job_page_outside_the_pool_is_checked_first_and_retired_when_dead(monkeypatch):
    pool = _rows(range(1, 41), "Pool Sys")
    landed = _rows([29002321], "UF Health") + _rows([7], "Pool Sys")      # 7 is in the pool too
    summary, checked, retired = _main_with(monkeypatch, pool, landed, dead={29002321})
    assert checked[0] == 29002321                       # opened pages first
    assert checked.count(7) == 1 and len(checked) == 41  # no row checked twice
    assert retired == [29002321]
    assert summary["landed"] == 2 and summary["landed_extra"] == 1
    assert summary["checked"] == 41 and summary["stamped"] == 40 and summary["retired"] == 1


def test_a_failed_read_of_opened_pages_leaves_the_pool_check_as_it_was(monkeypatch):
    pool = _rows(range(1, 41))
    summary, checked, retired = _main_with(monkeypatch, pool, [], dead={13}, landed_raises=True)
    assert sorted(checked) == list(range(1, 41)) and retired == [13]
    assert summary["landed"] == 0 and summary["checked"] == 40


def test_opened_pages_alone_are_checked_when_the_pool_is_empty(monkeypatch):
    landed = _rows([29002321], "UF Health") + _rows(range(1, 40), "Other")
    summary, checked, retired = _main_with(monkeypatch, [], landed, dead={29002321})
    assert len(checked) == 40 and retired == [29002321]


def test_opened_pages_keep_the_mass_retirement_guards(monkeypatch):
    # 12 of 20 opened pages of one system answer 404: a blocker, not expiry
    landed = _rows(range(100, 120), "Blocked Sys")
    summary, checked, retired = _main_with(monkeypatch, _rows(range(1000, 1300)), landed, dead=set(range(100, 112)))
    assert summary["landed_extra"] == 20 and retired == []
