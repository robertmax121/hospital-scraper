"""push4/cleanup (2026-09-25): the Workday runner, the Phenom rows that link
to Workday and Houston Methodist read CXS jobPostingInfo through one helper
with one posted_date rule (see _apply_wd_posting_info)."""
import scraper
from scraper import Job


# ── Workday CXS jobPostingInfo: one reader, one posted_date rule ──────────
BODY = "Registered Nurse. " * 20


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="Workday")
    base.update(kw)
    return Job(**base)


def test_wd_posting_info_start_date_replaces_a_relative_label_or_blank():
    info = {"jobDescription": "<p>%s</p>" % BODY, "startDate": "2026-09-20", "timeType": "Full time"}
    for posted in ("Posted 3 Days Ago", "Posted 30+ Days Ago", "Posted Today", ""):
        j = _job(posted_date=posted)
        assert scraper._apply_wd_posting_info(j, info) is True
        assert j.posted_date == "2026-09-20" and j.job_type == "Full time"


def test_wd_posting_info_keeps_an_iso_date_the_list_gave():
    j = _job(posted_date="2026-09-18", job_type="Part time", ats_platform="Phenom")
    assert scraper._apply_wd_posting_info(j, {"jobDescription": BODY, "startDate": "2026-09-20", "timeType": "Full time"})
    assert j.posted_date == "2026-09-18" and j.job_type == "Part time"


def test_wd_posting_info_short_body_and_bad_date():
    j = _job(description=BODY + "more", posted_date="Posted 3 Days Ago")
    assert scraper._apply_wd_posting_info(j, {"jobDescription": "short", "startDate": "20 Sep 2026"}) is False
    assert j.posted_date == "Posted 3 Days Ago" and j.description == BODY + "more"
    assert scraper._apply_wd_posting_info(_job(), None) is False
