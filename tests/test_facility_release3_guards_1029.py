"""2026-10-29 review B (facility release 3, safety): a facility hook or pass
that raises on one row nobody foresaw must cost only that row, and that row
keeps the name and town it came with.

  - finalize_jobs' phase 3 passes (facilities_uhg, _dialysis, _home_health,
    _ambulatory, _hospital_systems_a, _hospital_systems_b, _senior_rehab)
    guard each row (_row_guard): the rows before and after a failing row are
    named as usual, the failing row is put back and counts as "error".
  - the adapters' facility hooks go through scraper._fac_hook: a raising hook
    returns its default (the row keeps the label) instead of dropping the row
    from tonight's list (a dropped row reads as gone to the sweep) or ending
    the system's scrape.

No network, no database."""
import re

import pytest

import facilities_ambulatory as AM
import facilities_dialysis as DI
import facilities_home_health as HH
import facilities_hospital_systems_a as HSA
import facilities_hospital_systems_b as HSB
import facilities_senior_rehab as SR
import facilities_uhg as UHG
import scraper
from scraper import Job


def _job(system, job_id, name=None, city="Town", state="TX"):
    return Job(title="Registered Nurse", hospital_system=system, hospital_name=system if name is None else name,
               city=city, state=state, location=f"{city}, {state}", specialty="", job_type="",
               url=f"https://example.invalid/job/{job_id}", job_id=str(job_id), posted_date="",
               description="", ats_platform="T")


def _faulty(bad_id):
    """A per-row function that names every row 'Site <id>' at 'Elsewhere, OK',
    and on row `bad_id` changes the row half way and then raises."""
    seen = []

    def fn(job, *args):
        seen.append(job.job_id)
        job.hospital_name = f"Site {job.job_id}"
        if job.job_id == bad_id:
            raise ValueError("a row nobody foresaw")
        job.city, job.state, job.location = "Elsewhere", "OK", "Elsewhere, OK"
        return "named"
    return fn, seen


# (module, the pass, the per-row function it guards, a system the pass covers)
PASSES = [
    (UHG, "apply_facilities", "apply_facility", UHG.SYSTEM),
    (HH, "apply_facilities", "apply_facility", HH.BAYADA),
    (AM, "apply_facilities", "apply_facility", AM.SYSTEM_CVS),
    (HSA, "apply_facilities", "apply_facility", HSA.LIFEPOINT),
    (SR, "apply_jobs", "apply_job", SR.TRILOGY),
    (DI, "apply_jobs", "_apply_one", DI.DAVITA),
    (HSB, "apply_before_town", "_ochsner_one", HSB.OCHSNER),
    (HSB, "apply_after_town", "_jefferson_one", HSB.JEFFERSON),
]


@pytest.mark.parametrize("mod,pass_name,row_fn,system", PASSES,
                         ids=[f"{m.__name__}.{p}" for m, p, _f, _s in PASSES])
def test_a_failing_row_costs_only_that_row(monkeypatch, mod, pass_name, row_fn, system):
    fn, seen = _faulty("2")
    monkeypatch.setattr(mod, row_fn, fn)
    jobs = [_job(system, i) for i in ("1", "2", "3")]
    stats = getattr(mod, pass_name)(jobs)
    assert seen == ["1", "2", "3"]                      # the pass went on past the failing row
    a, bad, c = jobs
    assert (a.hospital_name, a.city, a.state) == ("Site 1", "Elsewhere", "OK")
    assert (c.hospital_name, c.city, c.state) == ("Site 3", "Elsewhere", "OK")
    # the failing row is put back as it came (its half-written name undone)
    assert (bad.hospital_name, bad.city, bad.state, bad.location) == (system, "Town", "TX", "Town, TX")
    assert stats[system].get("error") == 1
    assert stats[system].get("named") == 2


def test_finalize_keeps_every_row_when_a_pass_row_raises(monkeypatch):
    """End to end: a UHG row whose facility read raises is still in tonight's
    rows, on the label with its own town; the other rows are untouched."""
    real = UHG.apply_facility

    def flaky(job):
        if job.job_id == "900002":
            raise RuntimeError("boom")
        return real(job)
    monkeypatch.setattr(UHG, "apply_facility", flaky)
    UHG.set_known_facilities([])
    jobs = [_job(UHG.SYSTEM, jid, city="Orlando", state="FL") for jid in ("900001", "900002", "900003")]
    jobs.append(_job("Some Other System", "900004", name="Some Other Hospital", city="Austin", state="TX"))
    out = {o["job_id"]: o for o in scraper.finalize_jobs(jobs)}
    assert set(out) == {"900001", "900002", "900003", "900004"}
    assert out["900002"]["hospital_name"] == UHG.SYSTEM
    assert (out["900002"]["city"], out["900002"]["state"]) == ("Orlando", "FL")
    assert out["900004"]["hospital_name"] == "Some Other Hospital"


def test_fac_hook_returns_the_default_and_counts(monkeypatch):
    scraper._FAC_HOOK_ERRORS.clear()

    def boom(*a, **k):
        raise KeyError("x")
    assert scraper._fac_hook("test hook", boom, 1, 2, default=("", False)) == ("", False)
    assert scraper._fac_hook("test hook", boom, default=None) is None
    assert scraper._fac_hook("test hook", lambda x, y=0: x + y, 1, y=2, default=None) == 3
    assert scraper._FAC_HOOK_ERRORS == {"test hook": 2}
    scraper._FAC_HOOK_ERRORS.clear()


def _raise(*a, **k):
    raise TypeError("unexpected payload")


def test_jibe_hooks_that_raise_keep_the_label(monkeypatch):
    monkeypatch.setattr(SR, "jibe_facility", _raise)
    monkeypatch.setattr(HH, "jibe_branch", _raise)
    monkeypatch.setattr(DI, "usrc_facility", _raise)
    monkeypatch.setitem(scraper.JIBE_FACILITY_RESOLVER, DI.USRC, DI.usrc_facility)
    row = {"location_name": "2214 Sumter, SC Winyah", "city": "Sumter"}
    assert scraper._jibe_facility(SR.TRILOGY, row) == SR.TRILOGY
    assert scraper._jibe_facility(HH.AMEDISYS, row) == HH.AMEDISYS
    assert scraper._jibe_facility(DI.USRC, {"location_name": "1207 - Gulfgate Kidney Center TX"}) == DI.USRC
    # a board the phase 3 hooks do not cover is decided as before
    assert scraper._jibe_facility("Garnet Health", {"location_name": "Garnet Health Medical Center"}) \
        == "Garnet Health Medical Center"
    scraper._FAC_HOOK_ERRORS.clear()


def test_reread_slots_that_raise_do_not_end_the_detail_plan(monkeypatch):
    monkeypatch.setattr(HH, "reread_slot", _raise)
    import facilities_leftovers as LO
    monkeypatch.setattr(LO, "reread_slot", _raise)
    job = _job("MUSC Health", "77")
    job.description = ""
    scraper._known_kind("MUSC Health", job)        # no exception
    scraper._known_kind("CenterWell", job)
    scraper._FAC_HOOK_ERRORS.clear()


# every phase 3 adapter hook in scraper.py is called through _fac_hook
HOOKS = [
    "facilities_home_health.reread_slot", "facilities_leftovers.reread_slot",
    "facilities_ambulatory.tb_page_facility", "facilities_home_health.stash_requisition",
    "facilities_home_health.elara_town", "facilities_senior_rehab.jibe_facility",
    "facilities_home_health.jibe_branch", "facilities_home_health.greenhouse_branch",
    "facilities_ambulatory.concentra_center", "facilities_senior_rehab.phenom_facility",
    "facilities_home_health.oracle_branch", "facilities_senior_rehab.oracle_facility",
]


def test_every_phase3_adapter_hook_is_guarded():
    src = open(scraper.__file__, encoding="utf-8").read()
    for h in HOOKS:
        assert re.search(re.escape(h) + r"\s*\(", src) is None, f"{h} is called bare"
        assert re.search(r"_fac_hook\([^()]*?,\s*" + re.escape(h) + r"\s*,", src, re.S), f"{h} not via _fac_hook"
    assert re.search(r"_site = _fac_hook\(f\"\{system\} site table\", WD_SITE_FACILITY\[system\]", src)
    assert re.search(r"_unk = _fac_hook\(f\"\{system\} site table \(unknown\)\", WD_SITE_UNKNOWN\[system\]", src)
