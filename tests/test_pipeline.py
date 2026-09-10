# 2026-09-10 (S-scraper-2): pure pipeline rules — transparency-state ordering,
# cross-tenant dedupe at the source, CMS blank-state fill, partial-run flag.
import asyncio
import json
import os

import pytest

import scraper
from scraper import Job

from conftest import FIXTURES


def _job(system, title, job_id, name=None, city="", state="", location="", ats="Workday"):
    return Job(title=title, hospital_system=system, hospital_name=name or system,
               city=city, state=state, location=location or (f"{city}, {state}".strip(", ")),
               specialty="", job_type="", url=f"https://example.org/{job_id}", job_id=job_id,
               posted_date="", description="", ats_platform=ats)


def _cms_sample():
    with open(os.path.join(FIXTURES, "cms_hospitals_sample.json"), encoding="utf-8") as f:
        return json.load(f)


def test_priority_states_first_keeps_order_inside_and_outside_the_list():
    items = ["tx-texas", "ca-california", "fl-florida", "co-colorado", "wa-washington", "ny-new-york"]
    got = scraper.priority_states_first(items, lambda s: s[:2])
    # CO, WA, CA, NY in TRANSPARENCY_STATES order, then the rest as given.
    assert got == ["co-colorado", "wa-washington", "ca-california", "ny-new-york", "tx-texas", "fl-florida"]
    # Items without a state go last, still in their original order.
    assert scraper.priority_states_first([("a", ""), ("b", "CA"), ("c", None)], lambda kv: kv[1]) == \
        [("b", "CA"), ("a", ""), ("c", None)]


def test_hca_slugs_are_crawled_transparency_states_first():
    ordered = scraper.priority_states_first(scraper.HCA_STATE_SLUGS, lambda s: s[:2])
    assert ordered[:2] == ["co-colorado", "ca-california"]
    assert "ms-mississippi" not in scraper.HCA_STATE_SLUGS      # national-search fallback, 2026-09-10


def test_finalize_drops_cross_tenant_twins_but_keeps_same_tenant_reqs():
    rows = scraper.finalize_jobs([
        _job("CityMD", "APN/PA - Nephrology", "R50334", city="Hartford", state="CT"),
        _job("CityMD", "APN/PA - Nephrology", "R52011", city="Hartford", state="CT"),          # same tenant: kept
        _job("Summit Health (CityMD)", "APN/PA - Nephrology", "R99999", city="Hartford", state="CT"),  # other tenant: twin
        _job("Summit Health (Physicians)", "Cardiologist", "R1", city="Berkeley Heights", state="NJ"),  # no twin: kept
    ])
    keys = [(r["hospital_system"], r["job_id"]) for r in rows]
    assert ("CityMD", "R50334") in keys and ("CityMD", "R52011") in keys
    assert not any(jid == "R99999" for _, jid in keys)
    assert ("Summit Health (Physicians)", "R1") in keys


def test_finalize_merges_the_same_req_under_an_aliased_tenant():
    # Post-alias key: both tenants become "CityMD" at upsert, so one batch
    # must not carry the same (job_id, system) twice.
    rows = scraper.finalize_jobs([
        _job("CityMD", "RN", "R1", city="Bronx", state="NY"),
        _job("Summit Health (CityMD)", "RN", "R1", city="Bronx", state="NY"),
    ])
    assert len(rows) == 1


def test_cms_lookup_fills_unique_facilities_only():
    n = scraper.set_cms_lookup(_cms_sample())
    assert n >= 4
    assert scraper.cms_location_for("Ventura County Medical Center") == ("Ventura", "CA")
    assert scraper.cms_location_for("Arrowhead Regional Medical Center - ICU") == ("Colton", "CA")
    assert scraper.cms_location_for("Memorial Hospital") is None          # shared name: never guessed
    assert scraper.cms_location_for("", None, "No Such Place") is None


def test_normalize_job_fills_state_from_cms_by_name_and_raw_location():
    scraper.set_cms_lookup(_cms_sample())
    by_name = scraper.normalize_job(_job("Some System", "RN", "1", name="Natividad Medical Center"))
    assert (by_name["city"], by_name["state"]) == ("Salinas", "CA")
    by_loc = scraper.normalize_job(_job("Some System", "RN", "2", location="Contra Costa Regional Medical Center"))
    assert by_loc["state"] == "CA" and by_loc["city"] == "Martinez"
    untouched = scraper.normalize_job(_job("Some System", "RN", "3", name="Some System"))
    assert untouched["state"] == ""


def test_run_hca_flags_partial_run_when_a_slice_fails(monkeypatch):
    pytest.importorskip("curl_cffi")   # run_hca returns [] without it, by design
    calls = []

    def fake_slice(slug, q=""):
        calls.append(slug)
        if slug == "co-colorado":
            return [Job("RN", "HCA Healthcare", "HCA HealthONE Aurora", "Aurora", "CO", "Aurora, CO",
                        "", "", "https://careers.hcahealthcare.com/jobs/1-rn", "1", "", "", "Talemetry")], True, False
        return [], False, False   # failed every retry

    monkeypatch.setattr(scraper, "_hca_fetch_slice", fake_slice)
    monkeypatch.setattr(scraper, "_hca_discover_state_slugs", lambda: ["co-colorado", "tx-texas"])
    jobs = asyncio.run(scraper.run_hca(None))
    assert [j.job_id for j in jobs] == ["1"]
    assert "HCA Healthcare" in scraper.PARTIAL_SYSTEMS
    assert calls == ["co-colorado", "tx-texas"]


def test_employer_type_stamp_still_reads_the_alias():
    # The alias fold must not break the T2 urgent-care stamp for CityMD rows.
    assert scraper.employer_type_for(scraper.HOSPITAL_SYSTEM_ALIASES["Summit Health (CityMD)"]) == "urgent_care"
