# 2026-10-13: travel sweep guard. The 00:04 UTC run's Vivian paging broke
# (8,650 rows against ~22,000 the night before), the domain still cleared
# DEACT_MIN=100, and the sweep retired 13,550 open listings. A domain is now
# swept only when this run's yield is at least 70% of its baseline: the
# domain's active rows carrying the latest scraped_at before this run (what
# the previous run returned), read with retries BEFORE the upsert restamps
# them. A baseline of 0, an unreadable baseline, or an adapter that flagged
# its crawl incomplete (TRAVEL_INCOMPLETE) holds the domain. What was held
# goes to LAST_RUN_COUNTS["travel_sweep"] -> scraper_runs.meta.
# Fake PostgREST; no network, no database.
import io
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

import pytest

import scheduler
import scraper

RUN = "2026-10-14T00:04:00.000000Z"
PREV = "2026-10-13T00:04:00.123456+00:00"


class _UResp:
    def __init__(self, content_range="", body=b""):
        self.headers = {"Content-Range": content_range}
        self._body = body

    def read(self): return self._body
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _domain_of(u):
    like = urllib.parse.unquote(u.split("url=like.")[1].split("&")[0])
    return like.strip("*")


def _err(u, code=500):
    return urllib.error.HTTPError(u, code, "err", {}, io.BytesIO(b"statement timeout"))


class FakeTravelDB:
    """travel_jobs over PostgREST. baselines: domain -> int (rows at PREV) |
    0 (no earlier active row) | 'fail' (every read fails) | 'bad' (count
    unreadable) | ('flaky', n) (two failed attempts, then n)."""

    def __init__(self, monkeypatch, baselines, fail_post=None):
        self.baselines = baselines
        self.fail_post = fail_post
        self.log = []      # ("latest"|"count", domain, url) | ("upsert", n) | ("patch", domain)
        self.flaky_left = {}
        monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setattr(urllib.request, "urlopen", self.urlopen)
        monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
        monkeypatch.setattr(scraper, "update_travel_site_stats", lambda: 0)

    def urlopen(self, rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if m == "POST":
            rows = json.loads(rq.data.decode())
            if self.fail_post and self.fail_post(rows):
                raise _err(u)
            self.log.append(("upsert", len(rows)))
            return _UResp()
        if m == "PATCH":
            self.log.append(("patch", _domain_of(u)))
            return _UResp("*/3")
        if "order=id.desc" in u:
            return _UResp(body=b'[{"id": 4000}]')
        kind = "latest" if "order=scraped_at.desc" in u else "count" if "scraped_at=eq." in u else None
        if kind is None:
            raise AssertionError(f"unexpected {m} {u}")
        d = _domain_of(u)
        self.log.append((kind, d, u))
        b = self.baselines.get(d, 0)
        if b == "fail":
            raise _err(u)
        if isinstance(b, tuple):
            left = self.flaky_left.setdefault((d, kind), 2)
            if left:
                self.flaky_left[(d, kind)] = left - 1
                raise _err(u)
            b = b[1]
        if kind == "latest":
            return _UResp(body=b"[]" if b == 0 else json.dumps([{"scraped_at": PREV}]).encode())
        if b == "bad":
            return _UResp("0-0/*")
        return _UResp(f"0-0/{b}")

    def reads(self, kind, domain):
        return [e for e in self.log if e[0] == kind and e[1] == domain]

    def swept(self):
        return sorted({e[1] for e in self.log if e[0] == "patch"})


@pytest.fixture(autouse=True)
def _clean_travel_state():
    scraper.TRAVEL_INCOMPLETE.clear()
    scraper.LAST_RUN_COUNTS.pop("travel_sweep", None)
    yield
    scraper.TRAVEL_INCOMPLETE.clear()
    scraper.LAST_RUN_COUNTS.pop("travel_sweep", None)


def _rows(**per_domain):
    host = {"vivian": "www.vivian.com/jobs", "aya": "www.ayahealthcare.com/travel-nursing-jobs",
            "nomad": "www.nomadhealth.com/jobs", "amn": "www.amnhealthcare.com/job"}
    out = []
    for key, n in per_domain.items():
        for i in range(n):
            out.append({"agency_name": key, "agency_job_id": f"{key}:{i}", "title": "RN",
                        "url": f"https://{host[key]}/{i}/", "is_active": True, "scraped_at": RUN})
    return out


def test_the_10_13_vivian_collapse_is_held_and_healthy_sources_sweep(monkeypatch, caplog):
    db = FakeTravelDB(monkeypatch, {"vivian.com": 22_000, "ayahealthcare.com": 1_050})
    rows = _rows(vivian=8_650, aya=1_000)
    with caplog.at_level(logging.INFO):
        sent = scraper._upsert_travel_jobs_to_supabase(rows)
    assert sent == 9_650
    assert db.swept() == ["ayahealthcare.com"]
    assert any("Travel sweep guard vivian.com: yield 8,650 vs baseline 22,000" in m and "HOLD" in m
               for m in caplog.messages)
    assert any("Travel sweep guard ayahealthcare.com: yield 1,000 vs baseline 1,050" in m and "-> sweep" in m
               for m in caplog.messages)
    assert any("Travel deactivate: NOT sweeping" in m
               and "'vivian.com': 'yield 8,650 is 39% of baseline 22,000 (needs 70%)'" in m
               for m in caplog.messages)


def test_the_baseline_is_the_previous_stamp_read_before_the_upsert(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": 20_000, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    first_upsert = next(i for i, e in enumerate(db.log) if e[0] == "upsert")
    before = db.log[:first_upsert]
    assert {(e[0], e[1]) for e in before} == {("latest", "vivian.com"), ("count", "vivian.com"),
                                              ("latest", "ayahealthcare.com"), ("count", "ayahealthcare.com")}
    assert all(e[0] not in ("latest", "count") for e in db.log[first_upsert:])
    latest = urllib.parse.parse_qs(urllib.parse.urlsplit(db.reads("latest", "vivian.com")[0][2]).query)
    assert latest["is_active"] == ["eq.true"] and latest["scraped_at"] == [f"lt.{RUN}"]
    assert latest["order"] == ["scraped_at.desc"] and latest["limit"] == ["1"]
    count = urllib.parse.parse_qs(urllib.parse.urlsplit(db.reads("count", "vivian.com")[0][2]).query)
    assert count["is_active"] == ["eq.true"] and count["scraped_at"] == [f"eq.{PREV}"]
    assert db.swept() == ["ayahealthcare.com", "vivian.com"]


def test_a_flaky_read_is_retried_and_counts(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": ("flaky", 20_000)})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000))
    assert len(db.reads("latest", "vivian.com")) == 3 and len(db.reads("count", "vivian.com")) == 3
    assert db.swept() == ["vivian.com"]


def test_a_read_that_keeps_failing_holds_only_that_domain(monkeypatch, caplog):
    db = FakeTravelDB(monkeypatch, {"vivian.com": "fail", "ayahealthcare.com": 900})
    with caplog.at_level(logging.INFO):
        scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert len(db.reads("latest", "vivian.com")) == scraper.TRAVEL_BASELINE_ATTEMPTS == 3
    assert db.swept() == ["ayahealthcare.com"]
    assert any("baseline read failed for vivian.com after 3 attempts" in m for m in caplog.messages)
    assert any("Travel sweep guard vivian.com: yield 21,000 vs baseline unreadable" in m and "HOLD" in m
               for m in caplog.messages)


def test_an_unreadable_count_holds_the_domain(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": "bad", "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert db.swept() == ["ayahealthcare.com"]


def test_baseline_zero_holds_the_domain(monkeypatch):
    # No active row of the domain predates this run: nothing to compare to.
    db = FakeTravelDB(monkeypatch, {"vivian.com": 0, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert db.reads("count", "vivian.com") == []
    assert db.swept() == ["ayahealthcare.com"]
    held = scraper.LAST_RUN_COUNTS["travel_sweep"]["held"]
    assert held["vivian.com"] == "yield 21,000; no rows from the previous run (baseline 0)"


def test_deact_min_is_still_a_floor_and_skips_the_read(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"nomadhealth.com": 10, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(nomad=50, aya=950))
    assert db.reads("latest", "nomadhealth.com") == []
    assert db.swept() == ["ayahealthcare.com"]


def test_rows_retired_earlier_do_not_lock_a_source_out(monkeypatch):
    # After a bad night the retired rows are inactive, so the baseline is what
    # the previous run actually returned (8,650 after the 10-13 collapse);
    # a recovered run of 21,000 sweeps again.
    db = FakeTravelDB(monkeypatch, {"vivian.com": 8_650})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000))
    assert db.swept() == ["vivian.com"]


def test_an_incomplete_adapter_crawl_holds_its_domain(monkeypatch, caplog):
    # Travel collapsed while the other Vivian types were full: the domain
    # clears 70% (21,000 vs 22,000) but run_vivian flagged it.
    reason = "Travel: ended at page 1 with 392 rows (floor 2,000) from 1,000 of ~380,000 hits"
    scraper.TRAVEL_INCOMPLETE["vivian.com"] = reason
    db = FakeTravelDB(monkeypatch, {"vivian.com": 22_000, "ayahealthcare.com": 900})
    with caplog.at_level(logging.INFO):
        scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert db.swept() == ["ayahealthcare.com"]
    assert any("Travel sweep guard vivian.com" in m and f"HOLD: adapter crawl incomplete: {reason}" in m
               for m in caplog.messages)


def test_holds_and_baselines_are_recorded_for_scraper_runs(monkeypatch):
    scraper.TRAVEL_INCOMPLETE["vivian.com"] = "Travel: request failed on page 3 (after a retry)"
    FakeTravelDB(monkeypatch, {"vivian.com": 22_000, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    ts = scraper.LAST_RUN_COUNTS["travel_sweep"]
    assert ts["swept"] == ["ayahealthcare.com"] and ts["deactivated"] == 6   # 2 id windows x 3
    assert ts["baseline"] == {"vivian.com": 22_000, "ayahealthcare.com": 900}
    assert ts["baseline_stamp"] == {"vivian.com": PREV, "ayahealthcare.com": PREV}
    assert ts["yield"]["vivian.com"] == 21_000
    assert ts["held"]["vivian.com"] == ("adapter crawl incomplete: "
                                        "Travel: request failed on page 3 (after a retry)")
    assert ts["held"]["nomadhealth.com"] == "yield 0 below DEACT_MIN=100"
    json.dumps(ts)                                         # goes into scraper_runs.meta
    # The run notes name the held source with its reason; empty sources stay in meta.
    assert scheduler.travel_sweep_notes(scraper.LAST_RUN_COUNTS) == (
        "; travel sweep held vivian.com (adapter crawl incomplete: "
        "Travel: request failed on page 3 (after a retry))")
    assert scheduler.travel_sweep_notes({}) == ""


def test_one_failed_batch_among_landed_ones_skips_every_sweep(monkeypatch, caplog):
    # 0 < sent < len(rows): batch 2 of 3 fails all three attempts.
    db = FakeTravelDB(monkeypatch, {"vivian.com": 600, "ayahealthcare.com": 500},
                      fail_post=lambda rows: rows[0]["agency_job_id"] == "vivian:500")
    rows = _rows(vivian=700, aya=500)
    with caplog.at_level(logging.WARNING):
        sent = scraper._upsert_travel_jobs_to_supabase(rows)
    assert 0 < sent < len(rows) and sent == 700
    assert [e for e in db.log if e[0] == "upsert"] == [("upsert", 500), ("upsert", 200)]
    assert db.swept() == []
    assert any("SKIPPING sweep" in m for m in caplog.messages)
    ts = scraper.LAST_RUN_COUNTS["travel_sweep"]
    assert ts["skipped"] == "1 upsert batch(es) failed (700/1,200 rows landed)" and ts["swept"] == []
    assert scheduler.travel_sweep_notes(scraper.LAST_RUN_COUNTS) == (
        "; travel sweep skipped (1 upsert batch(es) failed (700/1,200 rows landed))")


def test_partial_upsert_still_skips_every_sweep(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": 100, "ayahealthcare.com": 100},
                      fail_post=lambda rows: True)
    sent = scraper._upsert_travel_jobs_to_supabase(_rows(vivian=600, aya=200))
    assert sent == 0 and db.swept() == []


@pytest.mark.parametrize("n,b,swept", [
    (700, 1000, True),       # exactly 70%
    (699, 1000, False),
    (150, 0, False),         # no rows from the previous run: held
    (99, 0, False),          # under DEACT_MIN
    (5000, 4000, True),
])
def test_sweep_plan_threshold(n, b, swept):
    sweep, held = scraper._travel_sweep_plan({"vivian.com": n}, {"vivian.com": b}, 100)
    assert (sweep == ["vivian.com"]) is swept
    assert ("vivian.com" in held) is (not swept)


def test_sweep_plan_reasons():
    sweep, held = scraper._travel_sweep_plan(
        {"vivian.com": 5000, "ayahealthcare.com": 500, "nomadhealth.com": 900, "amnhealthcare.com": 300},
        {"vivian.com": None, "ayahealthcare.com": 400, "nomadhealth.com": 0, "amnhealthcare.com": 200},
        100, {"amnhealthcare.com": "AMN: request failed on page 2"})
    assert sweep == ["ayahealthcare.com"]
    assert held == {"vivian.com": "yield 5,000; baseline unreadable",
                    "nomadhealth.com": "yield 900; no rows from the previous run (baseline 0)",
                    "amnhealthcare.com": "adapter crawl incomplete: AMN: request failed on page 2"}
