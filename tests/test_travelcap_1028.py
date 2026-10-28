# 2026-10-28: travel state audit (site travel_state_2026-10-28). The scraper
# half of the fixes:
#   * Vivian is crawled for Travel only (Permanent, Local Contract and Per
#     Diem rows were stored as travel contracts: 17,647 of 101,574);
#   * Nomad and AMN store a canonical specialty (33k contracts never reached
#     a specialty chip), with two guards measured over the stored rows;
#   * a domain whose sweep the yield guard holds still retires the rows whose
#     URL tonight's crawl emitted under another key (database function
#     retire_superseded_travel, staged in the site repo);
#   * after the STEP 2c snapshot rebuild lands, one POST marks the site's
#     travel geo pages stale (/api/revalidate/travel-geo).
# Fakes only: no network, no database.
import io
import json
import logging
import urllib.error
import urllib.request

import pytest

import scheduler
import scraper
import site_revalidate as R


# ── Vivian types ────────────────────────────────────────────────────────────
def test_vivian_is_crawled_for_travel_only():
    assert scraper.VIVIAN_EMPLOYMENT_TYPES == ["Travel"]


# ── Nomad / AMN specialty ───────────────────────────────────────────────────
@pytest.mark.parametrize("title,raw,want", [
    ("Travel Nurse - Emergency Room $1,877/wk", "Emergency Room", "Emergency / Trauma"),
    ("Travel Nurse - Medical-Surgical $1,703/wk", "Medical-Surgical", "Med / Surg"),
    ("Registered Nurse - Telemetry", "Telemetry", "Cardiac / Cardiovascular"),
    ("Travel Nurse - Operating Room $3,079/wk", "Operating Room", "Operating Room / Surgery"),
    ("Physical Therapist - Outpatient", "Outpatient", "Physical Therapy"),
    ("Travel Radiology - Computed Tomography $3,229/wk", "Computed Tomography", "Radiology / Imaging"),
    # a generic title bucket yields to the agency's specific label
    ("Registered Nurse - OR", "OR", "Operating Room / Surgery"),
    # Travel Nursing (the board itself) never replaces the agency's label
    ("Travel Nurse", "General", "General"),
    ("Registered Nurse - Float Pool", "Float", "Float Pool / General RN"),
])
def test_feed_specialty(title, raw, want):
    assert scraper._travel_feed_specialty(title, raw) == want


def test_feed_specialty_never_travel_nursing_when_the_agency_named_something():
    for raw in ("General", "Skilled", "Acute", "School"):
        assert scraper._travel_feed_specialty("Travel Nurse", raw) != "Travel Nursing"


def test_nomad_and_amn_rows_carry_the_canonical_specialty():
    nomad = scraper._nomad_map({
        "id": 77, "title": "Travel Nurse - Emergency Room $1,877/wk", "code": "ABC",
        "agency": {"name": "Nomad Health"}, "facility": {"city": "Duluth", "state": "MN", "name": "St. Luke's"},
        "specializations": [{"name": "Emergency Room"}], "weekly_gross_compensation": 1877,
    }, {"ABC": "https://nomadhealth.com/jobs/ABC"})
    assert nomad.specialty == "Emergency / Trauma"
    amn = scraper._amn_map({
        "jobID": 3600928, "jobTitle": "Registered Nurse - OR", "city": {"name": "Providence"},
        "state": {"abbrev": "RI"}, "disciplineSpecialty": {"specialtyName": "OR", "disciplineName": "RN"},
        "payRate": {"minPayRate": 3361, "maxPayRate": 3361, "payRateType": "Weekly", "payRateTypeAbbrev": "wk"},
    })
    assert amn.specialty == "Operating Room / Surgery"


# ── held domain: superseded rows ────────────────────────────────────────────
class _Resp:
    def __init__(self, body=b"", status=200):
        self._b, self.status, self.headers = body, status, {"Content-Range": ""}
    def read(self): return self._b
    def getcode(self): return self.status
    def __enter__(self): return self
    def __exit__(self, *a): return False


def test_retire_superseded_calls_the_function_once_per_held_domain(monkeypatch):
    calls = []
    def fake(rq, timeout=None):
        calls.append((rq.full_url, rq.get_method(), json.loads(rq.data.decode()), rq.headers.get("Authorization")))
        return _Resp(b"2715")
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    out = scraper._travel_retire_superseded("https://fake.invalid", "k", ["vivian.com"], "2026-10-28T00:03:00Z")
    assert out == {"vivian.com": 2715}
    assert calls == [("https://fake.invalid/rest/v1/rpc/retire_superseded_travel", "POST",
                      {"p_domain": "vivian.com", "p_run_started": "2026-10-28T00:03:00Z"}, "Bearer k")]


def test_retire_superseded_skips_quietly_until_the_function_exists(monkeypatch, caplog):
    def missing(rq, timeout=None):
        raise urllib.error.HTTPError(rq.full_url, 404, "nf", {}, io.BytesIO(b"{}"))
    monkeypatch.setattr(urllib.request, "urlopen", missing)
    with caplog.at_level(logging.INFO):
        out = scraper._travel_retire_superseded("https://fake.invalid", "k", ["vivian.com", "ayahealthcare.com"], "x")
    assert out == {"skipped": "function not applied"}
    assert any("retire_superseded_travel() is not in the database yet" in m for m in caplog.messages)


def test_retire_superseded_never_raises_and_does_nothing_without_domains(monkeypatch):
    def boom(rq, timeout=None):
        raise OSError("network down")
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    assert scraper._travel_retire_superseded("https://fake.invalid", "k", ["vivian.com"], "x") == {"vivian.com": "error: OSError"}
    assert scraper._travel_retire_superseded("https://fake.invalid", "k", [], "x") == {}
    assert scraper._travel_retire_superseded("https://fake.invalid", "k", ["vivian.com"], None) == {}


def test_the_upsert_asks_only_for_held_domains(monkeypatch):
    # the 10-28 night: Vivian held at 68% of its baseline, Aya swept
    from tests.test_travel_sweep_guard import FakeTravelDB, _rows
    FakeTravelDB(monkeypatch, {"vivian.com": 44_609, "ayahealthcare.com": 1_000})
    asked = []
    monkeypatch.setattr(scraper, "_travel_retire_superseded",
                        lambda url, key, domains, started: asked.append(list(domains)) or {d: 7 for d in domains})
    scraper._upsert_travel_jobs_to_supabase(_rows(vivian=30_464, aya=1_000))
    assert asked == [["vivian.com"]]
    assert scraper.LAST_RUN_COUNTS["travel_sweep"]["superseded"] == {"vivian.com": 7}
    assert "vivian.com" in scraper.LAST_RUN_COUNTS["travel_sweep"]["held"]


# ── travel geo revalidation ─────────────────────────────────────────────────
def test_revalidate_travel_geo_posts_once_with_the_scraper_key():
    seen = []
    def site(rq, timeout=None):
        seen.append((rq.full_url, rq.get_method(), rq.headers.get("Authorization")))
        return _Resp(b'{"ok":true}')
    out = R.revalidate_travel_geo(secret="s3cret", opener=site)
    assert out == {"status": 200, "ok": True, "skipped": None}
    assert seen == [("https://jobs.waypointrecruit.com/api/revalidate/travel-geo", "POST", "Bearer s3cret")]


def test_revalidate_travel_geo_without_a_secret_or_on_http_sends_nothing(monkeypatch):
    monkeypatch.delenv("REVALIDATE_SECRET", raising=False)
    monkeypatch.delenv("CRON_SECRET", raising=False)
    def refuse(*a, **k):
        raise AssertionError("must not send")
    assert R.revalidate_travel_geo(opener=refuse)["skipped"] == "REVALIDATE_SECRET not set"
    assert R.revalidate_travel_geo(secret="s", site="http://evil.example", opener=refuse)["skipped"].startswith("site origin refused")


def test_revalidate_travel_geo_404_and_faults_never_raise():
    def nf(rq, timeout=None):
        raise urllib.error.HTTPError(rq.full_url, 404, "nf", {}, io.BytesIO(b""))
    assert R.revalidate_travel_geo(secret="s", opener=nf) == {"status": 404, "ok": False, "skipped": None}
    def down(rq, timeout=None):
        raise OSError("down")
    assert R.revalidate_travel_geo(secret="s", opener=down)["status"] == 0


def test_snapshot_refresh_records_its_outcome(monkeypatch):
    monkeypatch.setattr(urllib.request, "urlopen", lambda rq, timeout=None: _Resp(b"52000"))
    scraper.LAST_RUN_COUNTS.pop("travel_snapshot", None)
    scraper._refresh_travel_sitemap_cohort("https://fake.invalid", "k")
    assert scraper.LAST_RUN_COUNTS["travel_snapshot"] == "refreshed"
    def fail(rq, timeout=None):
        raise OSError("timeout")
    monkeypatch.setattr(urllib.request, "urlopen", fail)
    monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
    scraper._refresh_travel_sitemap_cohort("https://fake.invalid", "k")
    assert scraper.LAST_RUN_COUNTS["travel_snapshot"] == "failed"
    scraper.LAST_RUN_COUNTS.pop("travel_snapshot", None)


def test_step_2c_revalidates_only_after_a_landed_refresh():
    import inspect
    src = inspect.getsource(scheduler.run)
    i = src.index("[ STEP 2c ]")
    block = src[i - 400:i + 900]
    assert '_scraper.LAST_RUN_COUNTS.pop("travel_snapshot", None)' in block
    assert 'if _scraper.LAST_RUN_COUNTS.get("travel_snapshot") == "refreshed":' in block
    assert "site_revalidate.revalidate_travel_geo()" in block
    assert '"travel_geo_revalidate": travel_geo_summary' in src
