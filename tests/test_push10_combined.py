# 2026-10-07 (push 10, combined branch): seams where two push-10 builders'
# changes meet. The pay item 1 counter (_PAY_TEXT_OVER_FIELD, moved inside
# normalize_job) must ride back from the perf item 1 process pool with the
# other two counters, or the per-run "Pay: the posting text's own range
# replaced the structured field" line would always read 0 on a pooled
# night.
import os

import scraper
from scraper import Job

FILLER = "\n\n" + "The team cares for patients on a busy unit and supports the department. " * 30
PAY_BODY = "Pay Range: $38.47 - $47.45 per hour" + FILLER      # disagrees with the field below by far more than 5%
FIELD = (29.39, 36.09, "hour")


def _job(**kw):
    base = dict(title="Registered Nurse", hospital_system="Test System", hospital_name="Test System",
                city="Springfield", state="MO", location="Springfield, MO", specialty="", job_type="",
                url="https://example.org/job/1", job_id="1", posted_date="", description=PAY_BODY,
                ats_platform="Test")
    base.update(kw)
    return Job(**base)


def _flip_jobs(n=4):
    out = []
    for i in range(n):
        j = _job(job_id=str(i), url=f"https://example.org/job/{i}")
        scraper.set_field_wage(j, FIELD)
        out.append(j)
    return out


class _FakePool:
    """An executor whose map() runs the chunks in this process, so the
    4-tuple a worker returns is checked without spawning."""
    def __init__(self):
        self.shut = False

    def map(self, fn, chunks, timeout=None):
        out = []
        for c in chunks:
            # a real worker has its own module counters; keep the parent's untouched
            saved = {k: dict(v) for k, v in (("cms", scraper._CMS_FILLS), ("skips", scraper._FACTS_SKIPS),
                                              ("pay", scraper._PAY_TEXT_OVER_FIELD))}
            out.append(fn(c))
            scraper._CMS_FILLS.update(saved["cms"])
            scraper._FACTS_SKIPS.update(saved["skips"])
            scraper._PAY_TEXT_OVER_FIELD.update(saved["pay"])
        return out

    def shutdown(self, wait=True, cancel_futures=False):
        self.shut = True


def test_a_facts_skipped_row_still_carries_has_signon():
    """steps puts has_signon on every upsert row; perf returns early from
    normalize_job when the stored facts hash matches. The skipped row must
    keep the key, or a PostgREST bulk upsert batch would mix key sets."""
    scraper.set_known_bodies([])
    body = "Sign-on bonus available for this role." + FILLER
    first = scraper.normalize_job(_job(job_id="s1", description=body))
    assert first["has_signon"] is True and first["posting_facts"]["h"]
    scraper.set_known_bodies([{"hospital_system": "Test System", "job_id": "s1", "ats_platform": "Test",
                               "desc_len": len(body), "fv": scraper.FACTS_VERSION, "fh": first["posting_facts"]["h"]}])
    scraper._FACTS_SKIPS["n"] = 0
    second = scraper.normalize_job(_job(job_id="s1", description=body))
    assert second["posting_facts"] is None and scraper._FACTS_SKIPS["n"] == 1
    assert second["has_signon"] is True
    assert set(second) == set(first)
    scraper.set_known_bodies([])


def test_normalize_chunk_returns_the_pay_counter_with_the_other_two():
    scraper.set_known_bodies([])
    rows, fills, skips, flips = scraper._normalize_chunk(_flip_jobs(3))
    assert len(rows) == 3
    assert (fills, skips, flips) == (0, 0, 3)
    assert all((r["wage_min"], r["wage_max"], r["wage_unit"]) == (38.47, 47.45, "hour") for r in rows)
    assert all(r["posting_facts"]["pay_src"] == "text" for r in rows)


def test_normalize_jobs_pool_sums_the_pay_counter(monkeypatch):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)
    pool = _FakePool()
    monkeypatch.setattr(scraper, "_finalize_pool", lambda n: pool)
    scraper._PAY_TEXT_OVER_FIELD["n"] = 0
    scraper._FACTS_SKIPS["n"] = 0
    rows = scraper.normalize_jobs(_flip_jobs(5))
    assert len(rows) == 5 and pool.shut
    assert scraper._PAY_TEXT_OVER_FIELD["n"] == 5
    scraper._PAY_TEXT_OVER_FIELD["n"] = 0


def test_finalize_jobs_logs_the_pay_line_from_a_pooled_run(monkeypatch, caplog):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)
    monkeypatch.setattr(scraper, "_finalize_pool", lambda n: _FakePool())
    scraper._PAY_TEXT_OVER_FIELD["n"] = 0
    with caplog.at_level("INFO"):
        rows = scraper.finalize_jobs(_flip_jobs(4))
    assert len(rows) == 4
    assert any("replaced the structured field on 4 rows" in r.getMessage() for r in caplog.records)
    assert scraper._PAY_TEXT_OVER_FIELD["n"] == 0        # reset after the summary line


def test_real_spawned_pool_moves_the_pay_counter(monkeypatch):
    """The spawned workers import scraper afresh; the flip count still
    comes back (the perf test covers the facts counter the same way)."""
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)
    monkeypatch.delenv("SCRAPER_WORKER", raising=False)
    scraper._PAY_TEXT_OVER_FIELD["n"] = 0
    par = scraper.normalize_jobs(_flip_jobs(4))
    assert "SCRAPER_WORKER" not in os.environ
    assert scraper._PAY_TEXT_OVER_FIELD["n"] == 4
    assert [(r["wage_min"], r["wage_max"], r["wage_unit"]) for r in par] == [(38.47, 47.45, "hour")] * 4
    scraper._PAY_TEXT_OVER_FIELD["n"] = 0
