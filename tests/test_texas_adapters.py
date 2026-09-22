# 2026-09-10 (Y-texas-build): offline tests for the Texas coverage batch.
# Fixtures were captured on 2026-09-10 through the adapters' own requests
# and trimmed to two or three items: the Oceans /jobs Vue block (with the
# `computed:` member that broke the old regex), a Paylocity listing page's
# window.pageData, and two iCIMS card-list items from the Prime portal.
import re

import scraper


# ── Oceans: one JSON object out of the Vue block ──────────────────────────
def test_oceans_blob_decodes_despite_trailing_computed_block(fixture_text):
    html = fixture_text("oceans_jobs_page.html")
    assert "computed:" in html                       # the shape that raised "Extra data"
    blob = scraper._oceans_parse_blob(html)
    assert blob["HasMore"] is True and len(blob["Jobs"]) == 3
    jobs = [scraper._oceans_job(rec) for rec in blob["Jobs"]]
    assert all(jobs)
    j = jobs[0]
    assert j.ats_platform == "OceansJobBoard" and j.hospital_system == "Oceans Healthcare"
    assert j.url.startswith("https://oceansjobboard.com/job-detail/") and j.job_id == str(blob["Jobs"][0]["JobNumber"])
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date)
    assert j.hospital_name == blob["Jobs"][0]["LocationName"] and len(j.state) == 2


def test_oceans_blob_missing_raises():
    try:
        scraper._oceans_parse_blob("<html>no vue here</html>")
    except RuntimeError as e:
        assert "blob not found" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


# ── Paylocity: window.pageData Jobs[] ─────────────────────────────────────
def test_paylocity_page_maps_jobs_location_and_deep_link(fixture_text):
    html = fixture_text("paylocity_page.html")
    jobs = scraper._parse_paylocity_page(html, "White Rock Medical Center", "guid", "org", "TX")
    assert len(jobs) == 2
    for j in jobs:
        assert j.ats_platform == "Paylocity" and j.hospital_name == "White Rock Medical Center"
        assert j.city == "Dallas" and j.state == "TX"
        assert j.url == f"https://recruiting.paylocity.com/Recruiting/Jobs/Details/{j.job_id}"
        assert re.fullmatch(r"\d+", j.job_id) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date)
        assert j.description and "<" not in j.description


def test_paylocity_page_without_pagedata_is_empty():
    assert scraper._parse_paylocity_page("<html></html>", "x", "g", "o", "TX") == []


def test_paylocity_orgs_carry_a_state_for_priority_ordering():
    for cfg in scraper.PAYLOCITY_ORGS.values():
        assert len(cfg) == 3 and re.fullmatch(r"[0-9a-f-]{36}", cfg[0]) and len(cfg[2]) == 2


# ── iCIMS card-list portals (Prime, Midland, Ardent) ──────────────────────
def test_icims_cards_parse_facility_title_and_deep_link(fixture_text):
    html = fixture_text("icims_cards.html")
    jobs = scraper._parse_icims_cards(html, "Prime Healthcare", "careers-primehealthcare.icims.com")
    assert len(jobs) == 2
    for j in jobs:
        assert j.ats_platform == "iCIMS" and j.hospital_system == "Prime Healthcare"
        assert j.hospital_name and j.hospital_name != "Prime Healthcare"       # Facility header
        assert re.fullmatch(r"https://careers-primehealthcare\.icims\.com/jobs/\d+/[^?]+", j.url)
        assert j.job_id in j.url and j.title.startswith("Registered Nurse")
        assert j.description and "&rsquo;" not in j.description


def test_icims_card_location_reads_us_state_city():
    assert scraper._icims_card_location("US-TX-Weslaco") == ("Weslaco", "TX")
    city, state = scraper._icims_card_location("Weslaco, TX")
    assert state == "TX"


# ── Config shapes for the block-C entries ─────────────────────────────────
def test_texas_block_c_config_entries_exist():
    for name in ("Cook Children's", "Driscoll Children's Hospital", "Shriners Children's", "Texas Scottish Rite for Children"):
        t = scraper.WORKDAY_TENANTS[name]
        assert len(t) == 3 and t[1].isdigit()
    for name in ("Texas Children's", "United Regional Health Care System", "UT Health San Antonio"):
        base, site = scraper.ORACLE_ORGS[name]
        assert base.startswith("https://") and site == "CX_1"
    for name in ("Children's Health", "Hendrick Health"):
        assert scraper.PHENOM_ORGS[name].startswith("https://")
    assert scraper.JIBE_SITES["Universal Health Services"] == "https://jobs.uhsinc.com"
    assert scraper.JIBE_SITES["Ardent Health"] == "https://jobs.ardenthealth.com"   # moved off the referrals-only iCIMS portal 2026-09-22
    for key in scraper.PAYCOM_ORGS.values():
        assert re.fullmatch(r"[0-9A-F]{32}", key)
