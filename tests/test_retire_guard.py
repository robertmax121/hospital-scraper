# 2026-09-24: the retirement guards (retire_guard.py), Layer 4's use of them
# (database.mark_inactive_jobs) and the resilient hospital upsert
# (scraper._upsert_hospital_jobs_to_supabase). No database, no network: the
# guard is pure, and the two wiring tests use a fake client / fake urlopen.
import io
import json
import types
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import retire_guard
from retire_guard import guard_reason, parse_ts, plan_layer4

NOW = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)
FRESH = (NOW - timedelta(days=1)).isoformat()       # seen yesterday
STALE = (NOW - timedelta(days=8)).isoformat()       # past the 7-day backstop


def _rows(system, n, misses=0, scraped_at=FRESH, start_id=1):
    return [{"id": start_id + i, "job_id": f"{system}-{start_id + i}", "hospital_system": system,
             "consecutive_scrape_misses": misses, "scraped_at": scraped_at} for i in range(n)]


def _keys(system, ids):
    """Scrape keys for the rows with these ids (job_id is '<system>-<id>')."""
    return {(system, f"{system}-{i}") for i in ids}


def _bumped(plan):
    return sorted(i for ids in plan["bump_by_new_count"].values() for i in ids)


# ── guard_reason ────────────────────────────────────────────────────────────

def test_guard_reason_thresholds():
    assert retire_guard.GUARD_MIN_ACTIVE == 20 and retire_guard.GUARD_RATIO == 0.80
    assert guard_reason(56, 0) == "zero-yield"
    assert guard_reason(5, 0) == "zero-yield"            # zero yield guards any size
    assert guard_reason(100, 79) == "partial-yield"
    assert guard_reason(100, 80) is None                  # exactly 80% is healthy
    assert guard_reason(20, 15) == "partial-yield"
    assert guard_reason(20, 16) is None
    assert guard_reason(19, 1) is None                    # tiny system: ratio not applied
    assert guard_reason(100, 130) is None                 # new postings outnumber the old
    assert guard_reason(0, 0) is None                     # nothing active, nothing to guard


# ── plan_layer4: the five cases the owner asked for ────────────────────────

def test_zero_yield_system_is_held():
    active = _rows("Connally Memorial Medical Center", 56, misses=1)
    plan = plan_layer4(active, set(), miss_threshold=3, now=NOW)
    assert plan["deactivate_ids"] == [] and _bumped(plan) == []
    g = plan["guarded"]["Connally Memorial Medical Center"]
    assert g["reason"] == "zero-yield" and g["active"] == 56 and g["yield"] == 0
    assert g["frozen"] == 56 and plan["frozen_rows"] == 56


def test_zero_yield_rows_at_two_misses_are_not_retired():
    # The 09-24 state: Nacogdoches / Paycom Hospital 2 rows sat at 2 misses.
    active = _rows("Paycom Hospital 2", 165, misses=2)
    plan = plan_layer4(active, set(), miss_threshold=3, now=NOW)
    assert plan["deactivate_ids"] == []


def test_partial_yield_system_resets_seen_and_holds_unseen():
    active = _rows("Northwestern Medicine", 100, misses=1)
    plan = plan_layer4(active, _keys("Northwestern Medicine", range(1, 51)), miss_threshold=3, now=NOW)
    assert plan["guarded"]["Northwestern Medicine"]["reason"] == "partial-yield"
    assert sorted(plan["found_ids"]) == list(range(1, 51))   # seen tonight: reset to 0
    assert _bumped(plan) == [] and plan["deactivate_ids"] == []
    assert plan["frozen_rows"] == 50


def test_healthy_system_counts_as_usual():
    active = _rows("Trinity Health", 90, misses=0) + _rows("Trinity Health", 10, misses=2, start_id=91)
    # 90 of 100 seen (90%): the 10 unseen rows at 2 misses reach the threshold.
    plan = plan_layer4(active, _keys("Trinity Health", range(1, 91)), miss_threshold=3, now=NOW)
    assert plan["guarded"] == {}
    assert sorted(plan["deactivate_ids"]) == list(range(91, 101))
    assert plan["found_ids"] == []                            # seen rows were already at 0


def test_healthy_system_first_miss_is_bumped():
    active = _rows("Trinity Health", 100, misses=0)
    plan = plan_layer4(active, _keys("Trinity Health", range(1, 96)), miss_threshold=3, now=NOW)
    assert plan["bump_by_new_count"] == {1: list(range(96, 101))}


def test_tiny_system_with_some_yield_counts_as_usual():
    # 5 active, 2 seen (40%) but under GUARD_MIN_ACTIVE: no ratio guard.
    active = _rows("Shamrock General Hospital", 5, misses=2)
    plan = plan_layer4(active, _keys("Shamrock General Hospital", [1, 2]), miss_threshold=3, now=NOW)
    assert plan["guarded"] == {}
    assert sorted(plan["found_ids"]) == [1, 2]
    assert sorted(plan["deactivate_ids"]) == [3, 4, 5]


def test_tiny_system_with_zero_yield_is_held():
    active = _rows("Crane County Hospital District", 10, misses=2)
    plan = plan_layer4(active, set(), miss_threshold=3, now=NOW)
    assert plan["guarded"]["Crane County Hospital District"]["reason"] == "zero-yield"
    assert plan["deactivate_ids"] == [] and _bumped(plan) == []


def test_seven_day_backstop_retires_a_dead_system():
    # A system dark for 8 days: its unseen rows lose the guard and count again.
    active = (_rows("Gone Health", 30, misses=2, scraped_at=STALE)
              + _rows("Gone Health", 20, misses=0, scraped_at=STALE, start_id=31)
              + _rows("Gone Health", 5, misses=0, scraped_at=FRESH, start_id=51))
    plan = plan_layer4(active, set(), miss_threshold=3, now=NOW)
    g = plan["guarded"]["Gone Health"]
    assert g["reason"] == "zero-yield" and g["backstop"] == 50 and g["frozen"] == 5
    assert sorted(plan["deactivate_ids"]) == list(range(1, 31))      # 2 -> 3: retired
    assert plan["bump_by_new_count"] == {1: list(range(31, 51))}      # 0 -> 1
    assert plan["backstop_rows"] == 50 and plan["frozen_rows"] == 5


def test_backstop_boundary_and_unreadable_timestamps():
    six_days = (NOW - timedelta(days=6, hours=23)).isoformat()
    active = (_rows("Edge Health", 1, misses=2, scraped_at=six_days)
              + _rows("Edge Health", 1, misses=2, scraped_at=None, start_id=2)
              + _rows("Edge Health", 1, misses=2, scraped_at="not a date", start_id=3))
    plan = plan_layer4(active, set(), miss_threshold=3, now=NOW)
    # Inside the window, missing or unreadable: all held, none retired.
    assert plan["deactivate_ids"] == [] and plan["frozen_rows"] == 3


def test_explicit_exclusions_are_untouched_even_past_the_backstop():
    active = _rows("HCA Healthcare", 40, misses=2, scraped_at=STALE)
    plan = plan_layer4(active, set(), miss_threshold=3,
                       exclude_systems={"HCA Healthcare"}, now=NOW)
    assert plan["excluded_rows"] == 40
    assert plan["guarded"] == {} and plan["deactivate_ids"] == [] and _bumped(plan) == []


def test_mixed_night_only_the_dark_system_is_held():
    active = (_rows("Odessa Regional Medical Center", 70, misses=1)
              + _rows("Trinity Health", 100, misses=2, start_id=101))
    keys = _keys("Trinity Health", range(101, 200))
    plan = plan_layer4(active, keys, miss_threshold=3, now=NOW)
    assert set(plan["guarded"]) == {"Odessa Regional Medical Center"}
    assert plan["deactivate_ids"] == [200]                     # Trinity's one unseen row
    assert sorted(plan["found_ids"]) == list(range(101, 200))


def test_parse_ts_variants():
    base = datetime(2026, 9, 24, 4, 0, 9, tzinfo=timezone.utc)
    assert parse_ts("2026-09-24T04:00:09+00:00") == base
    assert parse_ts("2026-09-24T04:00:09Z") == base
    assert parse_ts("2026-09-24 04:00:09+00") == base
    assert parse_ts("2026-09-24T04:00:09.12345+00:00") == base.replace(microsecond=123450)
    assert parse_ts("2026-09-24T04:00:09.123456789+0000") == base.replace(microsecond=123456)
    assert parse_ts("2026-09-23T23:00:09-05:00") == base
    assert parse_ts("2026-09-24T04:00:09") == base               # naive = UTC
    assert parse_ts(base) == base
    assert parse_ts(None) is None and parse_ts("") is None and parse_ts("garbage") is None


# ── database.mark_inactive_jobs wiring (fake client) ───────────────────────

def test_mark_inactive_jobs_applies_the_yield_guard(monkeypatch):
    import database

    fresh = datetime.now(timezone.utc).isoformat()
    active_rows = (
        [{"id": i, "job_id": f"d{i}", "hospital_system": "Dark Board",
          "consecutive_scrape_misses": 2, "scraped_at": fresh} for i in range(1, 31)]
        + [{"id": 100 + i, "job_id": f"h{i}", "hospital_system": "Healthy Board",
            "consecutive_scrape_misses": 2, "scraped_at": fresh} for i in range(10)]
    )
    updates, selects = [], []

    class _Q:
        def __init__(self, rows):
            self._rows, self._patch = rows, None
        def select(self, cols, *_a, **_k):
            selects.append(cols)
            return self
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
            updates.append((self._patch, list(ids)))
            return self
        def execute(self):
            class R: pass
            r = R(); r.data = list(self._rows) if self._patch is None else []
            return r

    class _DB:
        def table(self, _name): return _Q(active_rows)

    monkeypatch.setattr(database, "client", lambda: _DB())
    seen = [{"hospital_system": "Healthy Board", "job_id": f"h{i}"} for i in range(9)]
    out = database.mark_inactive_jobs(seen, miss_threshold=3, exclude_systems=set())
    assert "scraped_at" in selects[0]
    assert out["guarded_systems"] == ["Dark Board"] and out["guard_held_rows"] == 30
    touched = {i for _patch, ids in updates for i in ids}
    assert not touched & set(range(1, 31))                    # dark rows untouched
    assert out["deactivated"] == 1 and [ids for p, ids in updates if p.get("is_active") is False] == [[109]]
    assert out["reset_to_found"] == 9


# ── scraper._upsert_hospital_jobs_to_supabase (fake urlopen) ───────────────

class _Resp:
    def __init__(self, content_range=""):
        self.headers = {"Content-Range": content_range}
    def read(self): return b""
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _job(system, i, desc=""):
    return {"hospital_system": system, "job_id": str(i), "title": "Registered Nurse",
            "hospital_name": system, "url": f"https://example.org/jobs/{i}", "job_type": "",
            "description": desc, "state": "TX", "city": "Odessa"}


def _fake_db(monkeypatch, fail, count_header=None):
    """fail(rows) -> None (ok) or (code, body). count_header(system, n) -> the
    Content-Range the active count returns. Returns (posts, patches)."""
    import scraper
    posts, patches, active = [], [], {}

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u:
            return _Resp()
        if m == "POST":
            rows = json.loads(rq.data.decode())
            err = fail(rows)
            if err:
                code, body = err
                raise urllib.error.HTTPError(u, code, "err", {}, io.BytesIO(body.encode()))
            posts.append(rows)
            for r in rows:
                active.setdefault(r["hospital_system"], set()).add(r["job_id"])
            return _Resp()
        system = urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0])
        if m == "GET":
            n = len(active.get(system, ()))
            return _Resp(count_header(system, n) if count_header else f"0-0/{n}")
        patches.append(system)
        return _Resp("*/0")

    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    return posts, patches


def test_upsert_splits_on_57014_continues_and_sweeps_only_landed_systems(monkeypatch):
    import scraper
    timeout = (500, '{"code":"57014","message":"canceling statement due to statement timeout"}')
    # Big Board's statements time out above 25 rows; one Bad Board row never lands.
    rows = ([_job("Big Board", i) for i in range(150)]
            + [_job("Bad Board", i) for i in range(30)]
            + [_job("Small Board", i) for i in range(40)])

    def fail(batch):
        if any(r["hospital_system"] == "Bad Board" and r["job_id"] == "7" for r in batch):
            return timeout
        if len(batch) > 25 and any(r["hospital_system"] == "Big Board" for r in batch):
            return timeout
        return None

    posts, patches = _fake_db(monkeypatch, fail)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    landed = [r for batch in posts for r in batch]
    assert max(len(b) for b in posts) <= 100
    assert sent == len(landed) == 220 - 25              # the 25-row piece holding Bad Board 7
    assert {r["hospital_system"] for r in landed} == {"Big Board", "Bad Board", "Small Board"}
    assert sum(1 for r in landed if r["hospital_system"] == "Big Board") == 150
    assert sum(1 for r in landed if r["hospital_system"] == "Small Board") == 40   # after the failure
    # Bad Board had a row that never landed: not swept. The others were.
    assert "Bad Board" not in patches
    assert set(patches) == {"Big Board", "Small Board"}


def test_upsert_gives_up_on_a_dead_database_and_sweeps_nothing_unlanded(monkeypatch):
    # sleep is a no-op and the real clock barely moves: the probe cap still ends the pause.
    import scraper
    down = (503, "service unavailable")
    rows = [_job("A Board", i) for i in range(100)] + [_job("B Board", i) for i in range(500)]
    posts, patches = _fake_db(monkeypatch, lambda b: down if b[0]["hospital_system"] == "B Board" else None)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    assert sent == 100
    assert patches == ["A Board"]
    assert len(scraper.LAST_UPSERT_FAILED) == 500
    assert {r["hospital_system"] for r in scraper.LAST_UPSERT_FAILED} == {"B Board"}


def test_upsert_dedupes_the_conflict_key_preferring_a_description(monkeypatch):
    import scraper
    rows = [_job("Dup Board", 1, desc="Full posting body"), _job("Dup Board", 1, desc="")]
    rows += [_job("Dup Board", i) for i in range(2, 12)]
    posts, _patches = _fake_db(monkeypatch, lambda b: None)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    landed = [r for batch in posts for r in batch]
    assert sent == 11
    assert [r["description"] for r in landed if r["job_id"] == "1"] == ["Full posting body"]


def test_upsert_shrinks_the_batch_under_pressure_and_grows_it_back(monkeypatch):
    import scraper
    timeout = (500, '{"code":"57014"}')
    rows = [_job("Busy Board", i) for i in range(3000)]
    # The first 300 rows only land in statements of 25; after that the database recovers.
    posts, _p = _fake_db(monkeypatch, lambda b: timeout if len(b) > 25 and int(b[0]["job_id"]) < 300 else None)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    sizes = [len(b) for b in posts]
    assert sent == 3000
    assert sizes[:4] == [25, 25, 25, 25]               # the first 100 split down to 25
    first_50, first_100 = sizes.index(50), sizes.index(100)
    assert sizes[first_50 - 20:first_50] == [25] * 20     # 20 clean batches double it back
    assert sizes[first_100 - 20:first_100] == [50] * 20


def test_sweep_guard_protects_a_system_whose_count_is_unreadable(monkeypatch):
    import scraper
    rows = [_job("Counted Board", i) for i in range(30)] + [_job("Blind Board", i) for i in range(30)]
    _posts, patches = _fake_db(monkeypatch, lambda b: None,
                               count_header=lambda system, n: "" if system == "Blind Board" else f"0-0/{n}")
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    assert patches == ["Counted Board"]


def test_sweep_guard_skips_a_partial_yield_system(monkeypatch):
    import scraper
    rows = [_job("Half Board", i) for i in range(30)]
    # The database already holds 100 active rows of Half Board: 30 is under 80%.
    _posts, patches = _fake_db(monkeypatch, lambda b: None,
                               count_header=lambda system, n: "0-0/100")
    scraper._upsert_hospital_jobs_to_supabase(rows, "2026-09-24T00:09:00.000000Z")
    assert patches == []


# -- time-based breaker and the delayed retry (virtual clock) ----------------

RUN = "2026-09-24T00:09:00.000000Z"


class _Clock:
    """Virtual clock: sleep() advances it, and the fake database charges each
    POST its cost."""
    def __init__(self):
        self.t = 0.0

    def sleep(self, s):
        self.t += s

    def monotonic(self):
        return self.t


def _use_clock(monkeypatch, clock):
    import scraper
    monkeypatch.setattr(scraper, "time", types.SimpleNamespace(sleep=clock.sleep, monotonic=clock.monotonic,
                                                               time=clock.monotonic))


def _brownout_db(monkeypatch, clock, down):
    """Fake hospital_jobs on a virtual clock. down(t) is True while the
    database answers every POST with an instant 503 (0.3 s); a POST that
    lands costs 1 s. Returns a log of landed keys, failed attempts (start
    time, keys) and swept systems."""
    log = {"landed": [], "failed": [], "patches": []}

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        assert u.startswith("https://fake.invalid/"), u
        if "/qa_link_audit" in u:
            return _Resp()
        if m == "POST":
            keys = [(r["hospital_system"], r["job_id"]) for r in json.loads(rq.data.decode())]
            t0 = clock.t
            if down(t0):
                clock.t += 0.3
                log["failed"].append((t0, keys))
                raise urllib.error.HTTPError(u, 503, "err", {}, io.BytesIO(b"upstream connect error"))
            clock.t += 1.0
            log["landed"].extend(keys)
            return _Resp()
        system = urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0])
        if m == "GET":
            return _Resp(f"0-0/{sum(1 for s, _ in log['landed'] if s == system)}")
        log["patches"].append(system)
        return _Resp("*/0")

    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    _use_clock(monkeypatch, clock)
    return log


def _twelve_boards():
    rows = [_job(f"Board {s:02d}", k) for s in range(12) for k in range(500)]
    return rows, {(r["hospital_system"], r["job_id"]) for r in rows}


def _missed():
    import scraper
    return {(r["hospital_system"], r["job_id"]) for r in scraper.LAST_UPSERT_FAILED}


def test_upsert_rides_out_a_two_minute_brownout_and_the_retry_lands_the_rest(monkeypatch):
    import scraper
    clock = _Clock()
    rows, keys = _twelve_boards()
    log = _brownout_db(monkeypatch, clock, down=lambda t: 20 <= t < 140)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, RUN)
    missed = _missed()
    # The breaker paused and probed instead of abandoning the other ~3,700
    # rows: only rows tried inside the outage window are missing.
    in_window = {k for t0, ks in log["failed"] if 20 <= t0 < 140 for k in ks}
    assert missed and missed <= in_window and len(missed) <= 300
    assert sent == len(set(log["landed"])) == len(keys) - len(missed)
    assert set(log["landed"]) | missed == keys
    # A system with a missed row is not swept; every other system is.
    missed_systems = {s for s, _ in missed}
    assert set(log["patches"]) == {f"Board {s:02d}" for s in range(12)} - missed_systems
    # The delayed retry re-sends only the missed rows, lands them, sweeps nothing.
    patches, landed = list(log["patches"]), len(log["landed"])
    assert scraper.retry_failed_hospital_upsert() == len(missed)
    assert scraper.LAST_UPSERT_FAILED == []
    assert set(log["landed"]) == keys and len(log["landed"]) - landed == len(missed)
    assert log["patches"] == patches


def test_upsert_abandons_after_ten_minutes_without_a_write_and_leaves_the_rest_for_the_retry(monkeypatch):
    import scraper
    clock = _Clock()
    rows, keys = _twelve_boards()
    log = _brownout_db(monkeypatch, clock, down=lambda t: t >= 20)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, RUN)
    assert sent == 2000                                   # Boards 00-03 landed before the outage
    missed = _missed()
    assert missed == keys - set(log["landed"]) and len(missed) == 4000
    # It kept probing with one 25-row piece every 90 s and gave up only once
    # 10 minutes had passed since the last write (t = 20 s).
    starts = [t0 for t0, _ in log["failed"]]
    probes = [(t0, ks) for (t0, ks), prev in zip(log["failed"][1:], starts)
              if t0 - prev >= scraper.UPSERT_PROBE_PAUSE_S]
    assert len(probes) >= 5 and all(len(ks) == 25 for _, ks in probes)
    assert (20 + scraper.UPSERT_ABANDON_S <= clock.t
            <= 20 + scraper.UPSERT_ABANDON_S + scraper.UPSERT_PROBE_PAUSE_S + 5)
    # Abandoned systems are never swept.
    assert set(log["patches"]) == {"Board 00", "Board 01", "Board 02", "Board 03"}
    # The retry cannot land them either: they stay missed, and nothing is swept.
    assert scraper.retry_failed_hospital_upsert() == 0
    assert _missed() == missed
    assert set(log["patches"]) == {"Board 00", "Board 01", "Board 02", "Board 03"}


def test_upsert_pauses_on_time_when_slow_failures_have_not_reached_the_row_limit(monkeypatch):
    # 57014 timeouts that take 8 s each: 300 failed rows would take many
    # minutes, so the pause comes from 90 s without a write instead.
    import scraper
    clock = _Clock()
    rows = [_job("Slow Board", i) for i in range(1000)]
    attempts = []

    def urlopen(rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u:
            return _Resp()
        if m == "POST":
            attempts.append(clock.t)
            if 3 <= clock.t < 200:
                clock.t += 8.0
                raise urllib.error.HTTPError(u, 500, "err", {}, io.BytesIO(b'{"code":"57014"}'))
            clock.t += 1.0
            return _Resp()
        return _Resp("0-0/1000" if m == "GET" else "*/0")

    monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
    monkeypatch.setenv("SUPABASE_KEY", "test-key")
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    _use_clock(monkeypatch, clock)
    sent = scraper._upsert_hospital_jobs_to_supabase(rows, RUN)
    gaps = [b - a for a, b in zip(attempts, attempts[1:])]
    assert any(g >= scraper.UPSERT_PROBE_PAUSE_S for g in gaps)        # it paused
    assert 0 < len(scraper.LAST_UPSERT_FAILED) < scraper.UPSERT_BREAKER_ROWS
    assert sent == 1000 - len(scraper.LAST_UPSERT_FAILED)


def test_scrape_retries_the_missed_rows_once_after_the_travel_flow(monkeypatch, tmp_path):
    import scraper
    monkeypatch.chdir(tmp_path)
    calls = []
    state = {"miss": True}

    async def run_all():
        return [_job("A Board", 1)]

    async def run_all_travel():
        calls.append("travel")
        return []

    def upsert(rows, iso):
        calls.append("upsert")
        scraper.LAST_UPSERT_FAILED[:] = rows if state["miss"] else []
        return 0 if state["miss"] else len(rows)

    monkeypatch.setattr(scraper, "run_all", run_all)
    monkeypatch.setattr(scraper, "run_all_travel", run_all_travel)
    monkeypatch.setattr(scraper, "_upsert_travel_jobs_to_supabase", lambda rows: calls.append("travel upsert"))
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", upsert)
    monkeypatch.setattr(scraper, "retry_failed_hospital_upsert", lambda: calls.append("retry") or 1)
    scraper.scrape()
    assert calls == ["upsert", "travel", "travel upsert", "retry"]
    calls.clear()
    state["miss"] = False
    scraper.scrape()
    assert calls == ["upsert", "travel", "travel upsert"]             # nothing missed: no retry
