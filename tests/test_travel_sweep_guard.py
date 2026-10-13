# 2026-10-13: travel sweep guard. The 00:04 UTC run's Vivian paging broke
# (8,650 rows against ~22,000 the night before), the domain still cleared
# DEACT_MIN=100, and the sweep retired 13,550 open listings. A domain is now
# swept only when this run's yield is at least 70% of its baseline: active
# rows last seen by the previous run (scraped_at in the 36 h before this
# run's run_started, this run excluded), read once per domain BEFORE the
# upsert restamps them. Fake PostgREST; no network, no database.
import io
import logging
import urllib.error
import urllib.parse
import urllib.request

import pytest

import scraper

RUN = "2026-10-14T00:04:00.000000Z"


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


class FakeTravelDB:
    """travel_jobs over PostgREST. baselines: domain -> int | 'fail' | 'bad'."""

    def __init__(self, monkeypatch, baselines):
        self.baselines = baselines
        self.log = []            # ("baseline", domain, url) | ("upsert",) | ("patch", domain)
        monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setattr(urllib.request, "urlopen", self.urlopen)
        monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
        monkeypatch.setattr(scraper, "update_travel_site_stats", lambda: 0)

    def urlopen(self, rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if m == "POST":
            self.log.append(("upsert",))
            return _UResp()
        if m == "PATCH":
            self.log.append(("patch", _domain_of(u)))
            return _UResp("*/3")
        if "order=id.desc" in u:
            return _UResp(body=b'[{"id": 4000}]')
        if "scraped_at=gte." in u:
            d = _domain_of(u)
            self.log.append(("baseline", d, u))
            b = self.baselines.get(d, 0)
            if b == "fail":
                raise urllib.error.HTTPError(u, 500, "err", {}, io.BytesIO(b"statement timeout"))
            if b == "bad":
                return _UResp("0-0/*")
            return _UResp(f"0-0/{b}")
        raise AssertionError(f"unexpected {m} {u}")

    def baseline_domains(self):
        return [e[1] for e in self.log if e[0] == "baseline"]

    def swept(self):
        return sorted({e[1] for e in self.log if e[0] == "patch"})


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


def test_baselines_are_read_before_the_upsert_over_the_previous_runs_window(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": 20_000, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    first_upsert = db.log.index(("upsert",))
    assert {e[1] for e in db.log[:first_upsert] if e[0] == "baseline"} == {"vivian.com", "ayahealthcare.com"}
    assert all(e[0] != "baseline" for e in db.log[first_upsert:])
    url = next(e[2] for e in db.log if e[0] == "baseline" and e[1] == "vivian.com")
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    assert q["is_active"] == ["eq.true"]
    assert sorted(q["scraped_at"]) == ["gte.2026-10-12T12:04:00.000000Z", f"lt.{RUN}"]
    assert db.swept() == ["ayahealthcare.com", "vivian.com"]


def test_a_failed_baseline_read_holds_only_that_domain(monkeypatch, caplog):
    db = FakeTravelDB(monkeypatch, {"vivian.com": "fail", "ayahealthcare.com": 900})
    with caplog.at_level(logging.INFO):
        scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert db.swept() == ["ayahealthcare.com"]
    assert any("baseline read failed for vivian.com" in m for m in caplog.messages)
    assert any("Travel sweep guard vivian.com: yield 21,000 vs baseline unreadable" in m and "HOLD" in m
               for m in caplog.messages)


def test_an_unreadable_count_holds_the_domain(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": "bad", "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000, aya=950))
    assert db.swept() == ["ayahealthcare.com"]


def test_deact_min_is_still_a_floor_and_skips_the_read(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"nomadhealth.com": 0, "ayahealthcare.com": 900})
    scraper._upsert_travel_jobs_to_supabase(_rows(nomad=50, aya=950))
    assert "nomadhealth.com" not in db.baseline_domains()
    assert db.swept() == ["ayahealthcare.com"]


def test_rows_retired_earlier_do_not_lock_a_source_out(monkeypatch):
    # After a bad night the retired rows are inactive and outside the window,
    # so the baseline is what the previous run actually saw (here 8,650 after
    # the 10-13 collapse); a recovered run of 21,000 sweeps again.
    db = FakeTravelDB(monkeypatch, {"vivian.com": 8_650})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=21_000))
    assert db.swept() == ["vivian.com"]


def test_partial_upsert_still_skips_every_sweep(monkeypatch):
    db = FakeTravelDB(monkeypatch, {"vivian.com": 100, "ayahealthcare.com": 100})
    real = db.urlopen

    def failing(rq, timeout=None):
        if rq.get_method() == "POST":
            raise urllib.error.HTTPError(rq.full_url, 500, "err", {}, io.BytesIO(b"boom"))
        return real(rq, timeout)
    monkeypatch.setattr(urllib.request, "urlopen", failing)
    rows = _rows(vivian=600, aya=200)
    rows[0]["url"] = "https://www.vivian.com/jobs/0/"
    sent = scraper._upsert_travel_jobs_to_supabase(rows)
    assert sent == 0 and db.swept() == []


@pytest.mark.parametrize("n,b,swept", [
    (700, 1000, True),       # exactly 70%
    (699, 1000, False),
    (150, 0, True),          # no rows in the window: only DEACT_MIN applies
    (99, 0, False),          # under DEACT_MIN
    (5000, 4000, True),
])
def test_sweep_plan_threshold(n, b, swept):
    sweep, held = scraper._travel_sweep_plan({"vivian.com": n}, {"vivian.com": b}, 100)
    assert (sweep == ["vivian.com"]) is swept
    assert ("vivian.com" in held) is (not swept)


def test_sweep_plan_holds_a_missing_baseline():
    sweep, held = scraper._travel_sweep_plan({"vivian.com": 5000, "ayahealthcare.com": 500},
                                             {"vivian.com": None, "ayahealthcare.com": 400}, 100)
    assert sweep == ["ayahealthcare.com"]
    assert held == {"vivian.com": "yield 5,000; baseline unreadable"}


def test_baseline_window_is_the_36_hours_before_run_start():
    assert scraper._travel_baseline_window("2026-10-13T00:04:00.123456Z") == (
        "2026-10-11T12:04:00.123456Z", "2026-10-13T00:04:00.123456Z")
