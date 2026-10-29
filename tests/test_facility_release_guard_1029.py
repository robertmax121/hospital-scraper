"""2026-10-29 (facility release, review B): finalize_jobs' four new facility
passes (facilities_oracle_other.rename_jobs, facilities_phenom,
facilities_text, facilities_town) run over every row of the night. A pass
that raises must cost only that pass, never the run: the rows still
normalize and go to the upsert, with the names they had. No network, no
database."""
import pytest

import facilities_oracle_other
import facilities_phenom
import facilities_text
import facilities_town
import scraper


def _job(job_id, system="Legacy Health", name="Legacy Health", city="Gresham", state="OR"):
    return scraper.Job(title="Registered Nurse - Medical Surgical", hospital_system=system, hospital_name=name,
                       city=city, state=state, location="", specialty="", job_type="",
                       url=f"https://example.org/jobs/{job_id}", job_id=str(job_id), posted_date="",
                       description="", ats_platform="")


def _boom(*a, **k):
    raise RuntimeError("unforeseen row")


PASSES = [
    (facilities_oracle_other, "rename_jobs"),
    (facilities_phenom, "apply_facilities"),
    (facilities_text, "apply_facilities"),
    (facilities_town, "apply_jobs"),
]


@pytest.mark.parametrize("mod,fn", PASSES, ids=[f"{m.__name__}.{f}" for m, f in PASSES])
def test_a_pass_that_raises_costs_only_that_pass(monkeypatch, mod, fn):
    monkeypatch.setattr(mod, fn, _boom)
    rows = scraper.finalize_jobs([_job(9101001), _job(9101002, city="Portland")])
    assert sorted(r["job_id"] for r in rows) == ["9101001", "9101002"]
    assert all(r["hospital_system"] == "Legacy Health" for r in rows)


def test_every_pass_raising_still_returns_every_row(monkeypatch):
    for mod, fn in PASSES:
        monkeypatch.setattr(mod, fn, _boom)
    rows = scraper.finalize_jobs([_job(9101003), _job(9101004, system="UNC Health", name="UNC Health",
                                                       city="Chapel Hill", state="NC")])
    assert sorted(r["job_id"] for r in rows) == ["9101003", "9101004"]


def test_the_guard_passes_stats_through_and_maps_none_to_empty():
    assert scraper._facility_pass("x", lambda jobs: {"A": {"town": 1}}, []) == {"A": {"town": 1}}
    assert scraper._facility_pass("x", lambda jobs: None, []) == {}
    assert scraper._facility_pass("x", _boom, []) == {}
