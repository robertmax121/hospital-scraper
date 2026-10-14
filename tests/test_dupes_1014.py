# 2026-10-14 (fix 12, duplicate postings). Two scraper causes of two active
# rows for one posting, found in the 10-14 duplicate audit:
#  * UVM (HTML list): every list page ends with a "Featured Jobs" sidebar of
#    the three newest postings. Its cards print no Job Ref, so each took its
#    link's site id ("12449") beside the full card's requisition
#    ("R0090954"): one Apply link, two rows, three new pairs a night.
#    tests/fixtures/uvm_list_featured.html is trimmed from the live pages of
#    2026-10-14 (page 8's main list holding 12455 / 12454 with their refs,
#    page 1's sidebar holding 12455 / 12454 / 12453 without).
#  * Wellstar (Workday): wellstarcareers and wellstarprovidercareers both
#    list 14 provider requisitions; the requisition (job_id) now lands once.
import asyncio

import scraper
from scraper import Job

UVM = "University of Vermont Health Network"


def _uvm_pages(fixture_text):
    page = fixture_text("uvm_list_featured.html")
    return {1: page, 2: "<html><body>no cards</body></html>"}


def _run_uvm(monkeypatch, pages):
    async def fake_get(url, accept):
        n = int(url.rsplit("=", 1)[1])
        return pages.get(n, "")

    async def no_jitter(*a, **k):
        return None
    monkeypatch.setattr(scraper, "_hl_get", fake_get)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    return asyncio.run(scraper.scrape_html_list(None, UVM, scraper.HTML_LIST_SITES[UVM]))


def test_uvm_featured_cards_do_not_double_a_listed_posting(monkeypatch, fixture_text):
    jobs = _run_uvm(monkeypatch, _uvm_pages(fixture_text))
    by_id = {j.job_id: j for j in jobs}
    # the full cards keep their requisition ids ...
    assert by_id["R0091096"].url.endswith("/job/12455/mental-health-tech-unit-secretary-inpatient-psychiatry-"
                                          "mental-health-us-vt-burlington-111-colchester-ave/")
    assert "R0091094" in by_id
    # ... and their sidebar twins (same link, link id as job_id) are gone
    assert "12455" not in by_id and "12454" not in by_id
    # a sidebar posting whose full card this run did not list stays (no twin)
    assert "12453" in by_id
    assert len(jobs) == 3
    assert len({j.url for j in jobs}) == 3


def test_uvm_fixture_really_has_the_id_less_sidebar_cards(fixture_text):
    cfg = scraper.HTML_LIST_SITES[UVM]
    raw = scraper._parse_html_list_page(fixture_text("uvm_list_featured.html"), UVM, cfg)
    ids = [j.job_id for j in raw]
    assert ids.count("12455") == 1 and ids.count("R0091096") == 1
    assert len(raw) == 5


def _hl(url, job_id, desc=""):
    return Job(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
               specialty="", job_type="", url=url, job_id=job_id, posted_date="", description=desc,
               ats_platform="Custom")


def test_collapse_never_drops_cards_with_ids_of_their_own():
    cfg = {"fields": {"job_id": r"ref:(\w+)"}, "id": r"/job/(\d+)/"}
    shared = "https://example.org/job/77/apply/"
    jobs = [_hl(shared, "A1"), _hl(shared, "A2"), _hl("https://example.org/job/78/rn/", "78")]
    assert scraper._hl_collapse_same_url(jobs, cfg) == jobs


def test_collapse_is_off_for_sites_without_a_job_id_field():
    cfg = {"fields": {}, "id": r"/job/(\d+)/"}
    u = "https://example.org/job/77/rn/"
    jobs = [_hl(u, "77"), _hl(u, "R9")]
    assert scraper._hl_collapse_same_url(jobs, cfg) == jobs
    assert scraper._hl_collapse_same_url(jobs, {"fields": {"job_id": "x"}, "id": r"/job/(\d+)/"}) == [jobs[1]]


def _wd(system, job_id, title="Physician Advisor"):
    return Job(title=title, hospital_system=system, hospital_name=system, city="Marietta", state="GA",
               location="Marietta, GA", specialty="", job_type="",
               url=f"https://wellstar.wd1.myworkdayjobs.com/{system[:4]}/job/x_{job_id}", job_id=job_id,
               posted_date="", description="", ats_platform="Workday")


def test_wellstar_requisition_on_both_sites_lands_once_under_the_main_site():
    jobs = [
        _wd("Wellstar Health (Providers)", "JR-77942"),       # provider site listed first
        _wd("Wellstar Health", "JR-77942"),
        _wd("Wellstar Health (Providers)", "JR-1", title="Hospitalist"),   # only on the provider site
        _wd("Wellstar Health", "JR-2", title="RN"),
        _wd("Some Other System", "JR-77942"),                 # not in the family
    ]
    out = scraper._drop_req_family_copies(jobs)
    keys = [(j.hospital_system, j.job_id) for j in out]
    assert keys == [("Wellstar Health", "JR-77942"), ("Wellstar Health (Providers)", "JR-1"),
                    ("Wellstar Health", "JR-2"), ("Some Other System", "JR-77942")]


def test_wellstar_titles_that_differ_between_sites_still_collapse_and_shared_titles_do_not():
    out = scraper._drop_req_family_copies([
        _wd("Wellstar Health", "JR-72615", title="Pharmacist - Clinical Specialist"),
        _wd("Wellstar Health (Providers)", "JR-72615", title="Clinical Pharmacy Specialist"),
        _wd("Wellstar Health (Providers)", "JR-80000", title="Pharmacist - Clinical Specialist"),
    ])
    assert [(j.hospital_system, j.job_id) for j in out] == [
        ("Wellstar Health", "JR-72615"), ("Wellstar Health (Providers)", "JR-80000")]


def test_finalize_jobs_applies_the_requisition_family():
    rows = scraper.finalize_jobs([_wd("Wellstar Health (Providers)", "JR-75922"), _wd("Wellstar Health", "JR-75922")])
    assert [(r["hospital_system"], r["job_id"]) for r in rows] == [("Wellstar Health", "JR-75922")]
