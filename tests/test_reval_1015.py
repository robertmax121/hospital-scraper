# 2026-10-15 (owner, on Amedisys job 56815, hospital_jobs 28140695: a closed
# posting still offered Apply hours after the nightly retired it). Pinned here:
#   1. site_revalidate: the ids each retirement pass flips are POSTed to the
#      site's /api/revalidate/jobs (Authorization: Bearer CRON_SECRET) in
#      batches after the last pass; no secret = one log line, no request; a
#      failure never raises;
#   2. the sweep, Layer 4 and the front-pool check note exactly the ids they
#      flipped; the scheduler flushes once (STEP 2f), hca_local_push after
#      its sweep;
#   3. Norton's links go to the live Jibe host (JIBE_POSTING_URL);
#   4. tools/revalidate_jobs.py, the manual purge after retirement SQL.
# No network, no database.
import asyncio
import importlib.util
import io
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import pytest

import retire_guard
import scraper
import site_revalidate as R

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ── fakes ──────────────────────────────────────────────────────────────────

class _SiteResp:
    def __init__(self, status, body):
        self.status, self._raw = status, json.dumps(body).encode()

    def read(self): return self._raw
    def getcode(self): return self.status
    def __enter__(self): return self
    def __exit__(self, *a): return False


class FakeSite:
    """The site's endpoint: answers by script(call_number, ids) -> status or
    an exception; records every request."""

    def __init__(self, script=None):
        self.calls = []
        self.script = script or (lambda n, ids: 200)

    def __call__(self, rq, timeout=None):
        ids = json.loads(rq.data.decode())["ids"]
        self.calls.append({"url": rq.full_url, "method": rq.get_method(), "ids": ids,
                           "auth": rq.get_header("Authorization"), "ctype": rq.get_header("Content-type")})
        st = self.script(len(self.calls), ids)
        if isinstance(st, Exception):
            raise st
        if st >= 400:
            raise urllib.error.HTTPError(rq.full_url, st, "err", {}, io.BytesIO(b'{"ok":false}'))
        n = len(ids)
        return _SiteResp(st, {"ok": True, "received": n, "accepted": n, "tags_revalidated": n,
                              "paths_revalidated": n, "found": n, "not_found": 0, "inactive": n,
                              "still_active": 0, "lookup_failed_chunks": 0, "ms": 3})


def _no_sleep(_s):
    return None


# ── 1. site_revalidate.post_ids ────────────────────────────────────────────

def test_post_ids_sends_batches_of_500_with_the_cron_bearer():
    site, pauses = FakeSite(), []
    ids = list(range(1, 1235))
    out = R.post_ids(ids, secret="s3cret", opener=site, sleep=pauses.append)
    assert [len(c["ids"]) for c in site.calls] == [500, 500, 234]
    assert {c["url"] for c in site.calls} == {"https://jobs.waypointrecruit.com/api/revalidate/jobs"}
    assert {c["method"] for c in site.calls} == {"POST"} and {c["auth"] for c in site.calls} == {"Bearer s3cret"}
    assert {c["ctype"] for c in site.calls} == {"application/json"}
    assert [i for c in site.calls for i in c["ids"]] == ids
    assert out["sent"] == 1234 and out["ok_batches"] == 3 and out["unsent"] == 0 and out["stopped"] is None
    assert out["tags_revalidated"] == 1234 and out["inactive"] == 1234
    assert pauses == [R.PAUSE_SEC, R.PAUSE_SEC]          # between batches, not before the first


def test_post_ids_reads_the_secret_and_site_from_the_environment(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "from-env")
    monkeypatch.setenv("SITE_URL", "https://preview.example.app/")
    site = FakeSite()
    out = R.post_ids([5, 5, "6", "x", None, -1, 0], opener=site, sleep=_no_sleep)
    assert site.calls[0]["url"] == "https://preview.example.app/api/revalidate/jobs"
    assert site.calls[0]["auth"] == "Bearer from-env" and site.calls[0]["ids"] == [5, 6]
    assert out["ids"] == 2 and out["sent"] == 2


def test_no_secret_logs_one_line_and_sends_nothing(monkeypatch, caplog):
    monkeypatch.delenv("CRON_SECRET", raising=False)

    def refuse(*a, **k):
        raise AssertionError("no request without a secret")
    with caplog.at_level(logging.INFO):
        out = R.post_ids([1, 2, 3], opener=refuse, sleep=_no_sleep)
    assert out["skipped"] == "CRON_SECRET not set" and out["sent"] == 0 and out["unsent"] == 3
    msgs = [r.message for r in caplog.records if "CRON_SECRET" in r.message]
    assert len(msgs) == 1 and "not purged" in msgs[0]


def test_the_secret_goes_only_to_an_https_origin():
    site = FakeSite()
    out = R.post_ids([1], secret="s", site="http://evil.example", opener=site, sleep=_no_sleep)
    assert site.calls == [] and out["skipped"] and out["unsent"] == 1
    out = R.post_ids([1], secret="s", site="http://localhost:3000", opener=site, sleep=_no_sleep)
    assert site.calls[0]["url"] == "http://localhost:3000/api/revalidate/jobs" and out["sent"] == 1


@pytest.mark.parametrize("status,why", [(401, "refused CRON_SECRET"), (403, "refused CRON_SECRET"),
                                        (404, "endpoint not deployed")])
def test_a_refused_credential_or_missing_endpoint_stops_at_once(status, why):
    site = FakeSite(lambda n, ids: status)
    out = R.post_ids(list(range(1, 1500)), secret="s", opener=site, sleep=_no_sleep)
    assert len(site.calls) == 1 and why in out["stopped"]
    assert out["sent"] == 0 and out["unsent"] == 1499 and out["failed_batches"] == 1


def test_a_413_halves_the_batch():
    site = FakeSite(lambda n, ids: 413 if len(ids) > 250 else 200)
    out = R.post_ids(list(range(1, 1001)), secret="s", opener=site, sleep=_no_sleep)
    assert out["sent"] == 1000 and out["unsent"] == 0
    assert sorted({len(c["ids"]) for c in site.calls}) == [250, 500]
    assert sorted(i for c in site.calls if len(c["ids"]) == 250 for i in c["ids"]) == list(range(1, 1001))


def test_server_errors_are_retried_then_three_failed_batches_stop():
    site, pauses = FakeSite(lambda n, ids: 503), []
    out = R.post_ids(list(range(1, 2501)), secret="s", opener=site, sleep=pauses.append)
    # 3 batches tried, each 1 + len(RETRY_PAUSES) times, then the run stops
    assert len(site.calls) == 3 * (1 + len(R.RETRY_PAUSES))
    assert out["failed_batches"] == 3 and out["sent"] == 0 and out["unsent"] == 2500
    assert "3 failed batches" in out["stopped"]
    assert pauses.count(R.RETRY_PAUSES[-1]) == 3


def test_a_retry_that_lands_counts_as_sent():
    site = FakeSite(lambda n, ids: 502 if n == 1 else 200)
    out = R.post_ids(list(range(1, 11)), secret="s", opener=site, sleep=_no_sleep)
    assert len(site.calls) == 2 and out["sent"] == 10 and out["failed_batches"] == 0


def test_network_faults_and_odd_answers_never_raise():
    site = FakeSite(lambda n, ids: ValueError("boom") if n % 2 else urllib.error.URLError("down"))
    out = R.post_ids(list(range(1, 2001)), secret="s", opener=site, sleep=_no_sleep)
    assert out["sent"] == 0 and out["stopped"]

    def weird(rq, timeout=None):
        class _W:
            status = 200
            def read(self): return b"not json"
            def getcode(self): return 200
            def __enter__(self): return self
            def __exit__(self, *a): return False
        return _W()
    out = R.post_ids([1, 2], secret="s", opener=weird, sleep=_no_sleep)
    assert out["sent"] == 2 and out["tags_revalidated"] == 0


def test_cap_and_deadline_bound_a_flush():
    site = FakeSite()
    out = R.post_ids(list(range(1, 1001)), secret="s", opener=site, sleep=_no_sleep, max_ids=600)
    assert out["sent"] == 600 and out["capped"] == 400 and out["unsent"] == 0
    ticks = iter(range(0, 10_000, 200))
    out = R.post_ids(list(range(1, 2001)), secret="s", opener=FakeSite(), sleep=_no_sleep,
                     clock=lambda: next(ticks), deadline_sec=300)
    assert out["stopped"].startswith("deadline") and 0 < out["sent"] < 2000
    assert out["unsent"] == 2000 - out["sent"]


def test_note_retired_dedupes_across_passes_and_flush_clears(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "s")
    assert R.note_retired([1, 2, "3", "x", None, 2], "sweep") == 3
    assert R.note_retired([3, 4], "layer4") == 1
    assert R.note_retired(None, "front-pool") == 0
    assert R.pending() == [1, 2, 3, 4] and R.pending_by_source() == {"sweep": 3, "layer4": 1}
    site = FakeSite()
    out = R.flush("nightly", opener=site, sleep=_no_sleep)
    assert [c["ids"] for c in site.calls] == [[1, 2, 3, 4]]
    assert out["sent"] == 4 and out["by_source"] == {"sweep": 3, "layer4": 1}
    assert R.pending() == []
    assert R.flush("nightly", opener=site, sleep=_no_sleep)["ids"] == 0 and len(site.calls) == 1


def test_flush_forgets_the_ids_even_when_nothing_was_sent(monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)
    R.note_retired([9], "sweep")
    out = R.flush("nightly")
    assert out["skipped"] and R.pending() == []


# ── 2a. the sweep notes the ids its PATCH flipped ──────────────────────────

class _UResp:
    def __init__(self, content_range="", body=b""):
        self.headers = {"Content-Range": content_range}
        self._body = body

    def read(self): return self._body
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _job(system, i):
    return {"hospital_system": system, "job_id": str(i), "title": "Registered Nurse", "hospital_name": system,
            "url": f"https://example.org/jobs/{i}", "job_type": "", "description": "", "state": "TX", "city": "Odessa"}


def _sweep_db(monkeypatch, flipped_by_system, body=True):
    patches = []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u or m == "POST":
            return _UResp()
        system = urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0])
        if m == "GET":
            return _UResp("0-0/40")
        patches.append({"system": system, "url": u, "prefer": rq.get_header("Prefer")})
        ids = flipped_by_system.get(system, [])
        raw = json.dumps([{"id": i} for i in ids]).encode() if body else b""
        return _UResp(f"*/{len(ids)}", raw)
    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    return patches


def test_sweep_asks_for_the_flipped_ids_and_notes_them(monkeypatch):
    patches = _sweep_db(monkeypatch, {"Big Board": [701, 702, 703], "Other Board": [9]})
    rows = [_job("Big Board", i) for i in range(40)] + [_job("Other Board", 1000 + i) for i in range(40)]
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-10-16T00:09:00.000000Z")
    assert sorted(p["system"] for p in patches) == ["Big Board", "Other Board"]
    assert all(p["url"].endswith("&select=id") and "scraped_at=lt." in p["url"] for p in patches)
    assert {p["prefer"] for p in patches} == {"return=representation,count=exact"}
    assert sorted(R.pending()) == [9, 701, 702, 703]
    assert set(R.pending_by_source()) == {"sweep"}
    assert scraper.LAST_RUN_COUNTS["sweep_deactivated"] == 4


def test_sweep_count_still_reads_content_range_when_the_body_is_empty(monkeypatch):
    _sweep_db(monkeypatch, {"Big Board": [1, 2]}, body=False)
    scraper._upsert_hospital_jobs_to_supabase([_job("Big Board", i) for i in range(40)], "2026-10-16T00:09:00.000000Z")
    assert scraper.LAST_RUN_COUNTS["sweep_deactivated"] == 2 and R.pending() == []


def test_patched_ids_reads_only_ids():
    assert scraper._patched_ids(_UResp(body=b'[{"id": 3}, {"id": "4"}, {"x": 1}, {"id": null}]')) == [3, 4]
    assert scraper._patched_ids(_UResp(body=b"")) == [] and scraper._patched_ids(_UResp(body=b"{}")) == []


# ── 2b. Layer 4 notes the ids it retired (only chunks that landed) ─────────

def _layer4_db(monkeypatch, active_rows, fail_deactivate=False):
    import database

    class _Q:
        def __init__(self, rows):
            self._rows, self._patch = rows, None

        def select(self, *_a, **_k): return self
        def eq(self, *_a): return self

        def gt(self, _col, last_id):
            self._rows = [r for r in self._rows if r["id"] > last_id]
            return self

        def order(self, *_a, **_k): return self
        def limit(self, _n): return self

        def update(self, patch):
            self._patch = patch
            return self

        def in_(self, _col, ids):
            return self

        def execute(self):
            if fail_deactivate and self._patch and self._patch.get("is_active") is False:
                raise RuntimeError("57014 statement timeout")

            class _Res:
                pass
            r = _Res()
            r.data = list(self._rows) if self._patch is None else []
            return r

    class _DB:
        def table(self, _name): return _Q(active_rows)

    monkeypatch.setattr(database, "client", lambda: _DB())
    return database


def _active(system, n):
    fresh = datetime.now(timezone.utc).isoformat()
    return [{"id": i, "job_id": str(i), "hospital_system": system, "consecutive_scrape_misses": 0,
             "scraped_at": fresh} for i in range(1, n + 1)]


def test_layer4_first_miss_retirements_are_noted(monkeypatch):
    monkeypatch.setattr(retire_guard, "RETIRE_FIRST_MISS_WHEN_COMPLETE", True)
    database = _layer4_db(monkeypatch, _active("Amedisys", 20))
    seen = [{"hospital_system": "Amedisys", "job_id": str(i)} for i in range(1, 19)]
    out = database.mark_inactive_jobs(seen, complete_systems={"Amedisys"})
    assert out["deactivated"] == 2 and sorted(R.pending()) == [19, 20]
    assert R.pending_by_source() == {"layer4": 2}
    assert "retired_ids" not in out          # the ids stay out of scraper_runs.meta


def test_layer4_notes_nothing_for_a_chunk_that_did_not_land(monkeypatch):
    monkeypatch.setattr(retire_guard, "RETIRE_FIRST_MISS_WHEN_COMPLETE", True)
    database = _layer4_db(monkeypatch, _active("Amedisys", 20), fail_deactivate=True)
    seen = [{"hospital_system": "Amedisys", "job_id": str(i)} for i in range(1, 19)]
    out = database.mark_inactive_jobs(seen, complete_systems={"Amedisys"})
    assert out["deactivated"] == 0 and R.pending() == []


# ── 2c. the front-pool check notes the dead links it retired ───────────────

def test_front_pool_retirements_are_noted(monkeypatch):
    import validate_front_pool_urls as F

    rows = [{"id": i, "url": f"https://jobs.example.org/{i}", "hospital_system": f"Sys {i % 7}"}
            for i in range(1, 101)]
    dead = {13, 77}

    async def fake_fetch(session, sb_url, headers):
        return rows

    async def fake_check(session, sem, row):
        return row["id"], row["hospital_system"], 404 if row["id"] in dead else 200

    patched = []

    class _Resp:
        status = 204
        async def text(self): return ""
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    class _Session:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

        def patch(self, url, **kw):
            patched.append((url, kw.get("json")))
            return _Resp()

    monkeypatch.setattr(F, "_env", lambda: ("https://db.example", "k"))
    monkeypatch.setattr(F, "_fetch_pool", fake_fetch)
    monkeypatch.setattr(F, "_check", fake_check)
    monkeypatch.setattr(F.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(F.aiohttp, "TCPConnector", lambda **k: None)
    summary = asyncio.run(F.main())
    assert summary["retired"] == 2 and summary["stamped"] == 98
    assert sorted(R.pending()) == [13, 77] and R.pending_by_source() == {"front-pool": 2}
    retire_calls = [u for u, body in patched if body.get("is_active") is False]
    assert len(retire_calls) == 1
    assert sorted(retire_calls[0].split("id=in.(")[1].rstrip(")").split(",")) == ["13", "77"]


# ── 2d. the scheduler flushes once, after the last retirement pass ─────────

def test_scheduler_flushes_after_the_front_pool_check(monkeypatch):
    import scheduler
    import validate_front_pool_urls as F
    import validate_travel_urls as T
    order, records = [], {}

    def fake_scrape():
        R.note_retired([501, 502], "sweep")
        return [{"hospital_system": "Amedisys", "job_id": "1"}]

    def fake_mark(jobs, **kw):
        order.append("layer4")
        R.note_retired([502, 503], "layer4")
        return {"deactivated": 2, "active_before": 10}

    async def fake_travel():
        order.append("travel")

    async def fake_front():
        order.append("front")
        R.note_retired([504], "front-pool")
        return {"retired": 1}

    def fake_flush(label="", **kw):
        order.append("flush")
        records["flushed"] = (label, R.pending())
        R.clear()
        return {"ids": 4, "sent": 4, "by_source": {"sweep": 2, "layer4": 1, "front-pool": 1}}

    def fake_finish(run_id, status, finished, **kw):
        records["meta"] = kw.get("meta") or {}
    monkeypatch.setattr(scheduler, "fix_run_day", lambda: "2026-10-16")
    monkeypatch.setattr(scheduler, "start_run_record", lambda *a, **k: None)
    monkeypatch.setattr(scheduler, "finish_run_record", fake_finish)
    monkeypatch.setattr(scheduler, "scrape", fake_scrape)
    monkeypatch.setattr(scheduler, "mark_inactive_jobs", fake_mark)
    monkeypatch.setattr(scheduler, "get_stats", lambda total=None: {"total_active_jobs": total})
    monkeypatch.setattr(T, "main", fake_travel)
    monkeypatch.setattr(F, "main", fake_front)
    monkeypatch.setattr(scraper, "update_travel_site_stats", lambda: 0)
    monkeypatch.setattr(scraper, "flag_signon_jobs", lambda *a, **k: 0)
    monkeypatch.setattr(R, "flush", fake_flush)
    scheduler.run()
    assert order == ["layer4", "travel", "front", "flush"]
    assert records["flushed"] == ("nightly", [501, 502, 503, 504])
    assert records["meta"]["site_revalidate"]["sent"] == 4


def test_scheduler_survives_a_flush_that_raises(monkeypatch):
    import scheduler
    import validate_front_pool_urls as F
    import validate_travel_urls as T
    finished = {}

    async def nop():
        return {}

    def boom(*a, **k):
        raise RuntimeError("site down")
    monkeypatch.setattr(scheduler, "fix_run_day", lambda: "2026-10-16")
    monkeypatch.setattr(scheduler, "start_run_record", lambda *a, **k: None)
    monkeypatch.setattr(scheduler, "finish_run_record", lambda *a, **k: finished.update(k))
    monkeypatch.setattr(scheduler, "scrape", lambda: [{"hospital_system": "A", "job_id": "1"}])
    monkeypatch.setattr(scheduler, "mark_inactive_jobs", lambda jobs, **kw: {"deactivated": 0, "active_before": 1})
    monkeypatch.setattr(scheduler, "get_stats", lambda total=None: {"total_active_jobs": total})
    monkeypatch.setattr(T, "main", nop)
    monkeypatch.setattr(F, "main", nop)
    monkeypatch.setattr(scraper, "update_travel_site_stats", lambda: 0)
    monkeypatch.setattr(scraper, "flag_signon_jobs", lambda *a, **k: 0)
    monkeypatch.setattr(R, "flush", boom)
    scheduler.run()
    assert finished["meta"]["site_revalidate"] == {}


# ── 2e. hca_local_push flushes after its sweep ──────────────────────────────

def test_hca_push_rows_purges_what_its_sweep_retired(monkeypatch):
    import hca_local_push as hlp
    monkeypatch.setenv("CRON_SECRET", "s")
    site = FakeSite()
    monkeypatch.setattr(urllib.request, "urlopen", site)
    monkeypatch.setattr(R.time, "sleep", _no_sleep)

    def fake_upsert(rows, run_started_iso):
        R.note_retired([31, 32], "sweep")
        return len(rows)
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", fake_upsert)
    rows = [dict(_job("HCA Healthcare", i), state="TX") for i in range(3)]
    assert hlp.push_rows(rows, "2026-10-16T04:00:00.000000Z") == 3
    assert [c["ids"] for c in site.calls] == [[31, 32]] and R.pending() == []


def test_hca_shared_env_supplies_the_cron_secret(monkeypatch, tmp_path):
    import hca_local_push as hlp
    env = tmp_path / ".env.local"
    env.write_text("SUPABASE_SERVICE_ROLE_KEY=k\nCRON_SECRET=from-file\n", encoding="utf-8")
    monkeypatch.setattr(hlp, "SHARED_ENV_CANDIDATES", [str(env)])
    monkeypatch.delenv("SUPABASE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("CRON_SECRET", raising=False)
    hlp._load_shared_env()
    assert os.environ["CRON_SECRET"] == "from-file"
    for k in ("SUPABASE_KEY", "SUPABASE_URL", "CRON_SECRET"):
        monkeypatch.delenv(k, raising=False)


# ── 3. Norton: the live Jibe host ───────────────────────────────────────────

class _JR:
    def __init__(self, body):
        self.status, self._body = 200, body

    async def json(self, content_type=None):
        return self._body


class _JCtx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self): return self.r
    async def __aexit__(self, *a): return False


def _jibe_feed(monkeypatch, items):
    def fake_req(session, method, url, **kw):
        page = int((kw.get("params") or {}).get("page", "1"))
        return _JCtx(_JR({"jobs": items if page == 1 else [], "totalCount": len(items)}))

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "jitter", no_jitter)


def _item(rid, canonical):
    return {"data": {"req_id": rid, "slug": rid, "title": "RN, Labor & Delivery, 7p-7a", "city": "Louisville",
                     "state": "Kentucky", "description": "<p>Labor and delivery.</p>", "location_name": "Norton Hospital",
                     "meta_data": {"canonical_url": canonical},
                     "apply_url": f"https://careers-nortonhealthcare.icims.com/jobs/{rid}/login"}}


def test_norton_links_go_to_the_live_jibe_host(monkeypatch):
    _jibe_feed(monkeypatch, [_item(r, f"https://www.nortonhealthcarecareers.com/jobs/{r}?lang=en-us")
                             for r in ("162524", "161744")])
    jobs = asyncio.run(scraper.scrape_jibe(None, "Norton Healthcare", "https://nortonhealthcare.jibeapply.com"))
    assert [j.url for j in jobs] == [
        "https://nortonhealthcare.jibeapply.com/careers-home/jobs/162524?lang=en-us",
        "https://nortonhealthcare.jibeapply.com/careers-home/jobs/161744?lang=en-us"]
    assert all("nortonhealthcarecareers.com" not in j.url and "icims.com" not in j.url for j in jobs)
    assert scraper.JIBE_SITES["Norton Healthcare"] == "https://nortonhealthcare.jibeapply.com"


def test_other_jibe_boards_keep_the_feeds_canonical_url(monkeypatch):
    _jibe_feed(monkeypatch, [_item("57802", "https://careers.amedisys.com/jobs/57802?lang=en-us")])
    jobs = asyncio.run(scraper.scrape_jibe(None, "Amedisys", "https://careers.amedisys.com"))
    assert [j.url for j in jobs] == ["https://careers.amedisys.com/jobs/57802?lang=en-us"]
    assert scraper._jibe_posting_url("Amedisys", "https://careers.amedisys.com", {}, "9") == \
        "https://careers.amedisys.com/jobs/9"
    # a slug that is not a plain number is quoted; a missing slug falls back to req_id
    assert scraper._jibe_posting_url("Norton Healthcare", "https://nortonhealthcare.jibeapply.com/",
                                     {"slug": "a b/c"}, "1") == \
        "https://nortonhealthcare.jibeapply.com/careers-home/jobs/a%20b%2Fc?lang=en-us"
    assert scraper._jibe_posting_url("Norton Healthcare", "https://nortonhealthcare.jibeapply.com",
                                     {}, "77") == "https://nortonhealthcare.jibeapply.com/careers-home/jobs/77?lang=en-us"


# ── 4. tools/revalidate_jobs.py ─────────────────────────────────────────────

def _tool():
    spec = importlib.util.spec_from_file_location("revalidate_jobs_tool",
                                                  os.path.join(REPO, "tools", "revalidate_jobs.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_tool_reads_ids_in_any_shape(tmp_path):
    T = _tool()
    assert T.ids_from_text("28140695, 28140696\n28140695 x7 0 -3") == [28140695, 28140696, 7, 3]
    env = tmp_path / "e.env"
    env.write_text('# c\nOTHER=1\nexport CRON_SECRET="abc"\nSITE_URL=https://x.example\n', encoding="utf-8")
    assert T.read_env_file(str(env)) == {"CRON_SECRET": "abc", "SITE_URL": "https://x.example"}


def test_tool_dry_run_and_missing_secret_send_nothing(monkeypatch, capsys):
    T = _tool()

    def refuse(*a, **k):
        raise AssertionError("nothing may be sent")
    monkeypatch.setattr(R, "post_ids", refuse)
    monkeypatch.delenv("CRON_SECRET", raising=False)
    assert T.main(["--dry-run", "1", "2", "--ids", "3,4"]) == 0
    assert "4 id(s)" in capsys.readouterr().out
    assert T.main(["1"]) == 2
    assert "NOT RUN" in capsys.readouterr().out
    assert T.main([]) == 0


def test_tool_posts_with_an_env_file(monkeypatch, tmp_path, capsys):
    T = _tool()
    monkeypatch.delenv("CRON_SECRET", raising=False)
    env = tmp_path / ".env.local"
    env.write_text("CRON_SECRET=file-secret\n", encoding="utf-8")
    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("\n".join(str(i) for i in range(1, 1201)), encoding="utf-8")
    site = FakeSite()
    monkeypatch.setattr(urllib.request, "urlopen", site)
    monkeypatch.setattr(R.time, "sleep", _no_sleep)
    assert T.main(["--file", str(ids_file), "--env-file", str(env)]) == 0
    assert [len(c["ids"]) for c in site.calls] == [500, 500, 200]
    assert {c["auth"] for c in site.calls} == {"Bearer file-secret"}
    assert "file-secret" not in capsys.readouterr().out
    site.script = lambda n, ids: 401
    assert T.main(["5", "--env-file", str(env)]) == 1


def test_tool_sql_is_read_only_and_kept_out_of_the_quiet_window():
    T = _tool()
    with pytest.raises(SystemExit):
        T.ids_from_sql("delete from public.hospital_jobs")
    with pytest.raises(SystemExit):
        T.ids_from_sql("select id from x; update public.hospital_jobs set is_active = false")
    with pytest.raises(SystemExit):
        T.ids_from_sql("select id from x", now=datetime(2026, 10, 16, 0, 10, tzinfo=timezone.utc))
    assert T.in_quiet_window(datetime(2026, 10, 15, 23, 30, tzinfo=timezone.utc))
    assert T.in_quiet_window(datetime(2026, 10, 16, 5, 29, tzinfo=timezone.utc))
    assert not T.in_quiet_window(datetime(2026, 10, 16, 5, 30, tzinfo=timezone.utc))
    assert not T.in_quiet_window(datetime(2026, 10, 15, 17, 0, tzinfo=timezone.utc))
