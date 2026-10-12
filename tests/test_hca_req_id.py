# 2026-10-12: HCA's public requisition number (owner: "job id's for HCA are
# wrong"). job_id is the Talemetry id in the URL (/jobs/18284308-...), which
# HCA's recruiters cannot look up; the posting prints "Job ID: 4981331". The
# number is read from the search card (every card carries it) and from the job
# page (JSON-LD identifier.value, then the visible line), and stored as
# posting_facts["req_id"]. job_id, url and the upsert key are unchanged.
# Fixtures are real snippets fetched 2026-10-12 (tests/fixtures/hca_req/).
import dataclasses
import re

import pytest

import scraper
import hca_local_push as hlp

CARDS = "hca_req/hca_cards_req.html"
JOB_PAGE = "hca_req/hca_job_18284308.html"


def test_search_cards_carry_the_requisition_number(fixture_text):
    jobs = scraper._parse_hca_cards(fixture_text(CARDS))
    assert [(j.job_id, j.req_id) for j in jobs] == [
        ("18349900", "5045755"),
        ("18349898", "5066993"),
        ("15852740", "J-049490"),     # physician postings print the J- form
    ]
    first = jobs[0]
    # job_id and url are exactly what they were before req_id existed
    assert first.url == "https://careers.hcahealthcare.com/jobs/18349900-pharmacy-technician-instructor"
    assert first.title == "Pharmacy Technician Instructor"
    assert first.hospital_name == "The College of Health Care Professions"
    assert (first.city, first.state, first.job_type) == ("Dallas", "TX", "Full-time")


def test_older_alaska_fixture_cards_also_carry_it(fixture_text):
    jobs = scraper._parse_hca_cards(fixture_text("hca_cards.html"))
    assert len(jobs) == 3
    for j in jobs:
        assert re.fullmatch(r"\d{7}", j.req_id) and j.req_id != j.job_id


def test_a_card_without_the_line_parses_with_a_blank_req_id(fixture_text):
    page = re.sub(r"Job ID:\s*[A-Za-z0-9-]+", "", fixture_text(CARDS))
    jobs = scraper._parse_hca_cards(page)
    assert len(jobs) == 3 and all(j.req_id == "" for j in jobs)


def test_job_page_requisition_number(fixture_text):
    page = fixture_text(JOB_PAGE)
    posting = scraper._jobposting_from_html(page)
    assert posting["identifier"]["value"] == "4981331"
    assert scraper.hca_req_id_from_page(page) == "4981331"
    assert scraper.hca_req_id_from_page(page, posting) == "4981331"
    # without the JSON-LD the visible "Job ID:" line still answers
    no_ld = re.sub(r'<script type="application/ld\+json">.*?</script>', "", page, flags=re.S)
    assert scraper._jobposting_from_html(no_ld) is None
    assert scraper.hca_req_id_from_page(no_ld) == "4981331"
    # JSON-LD wins over the line when both exist
    assert scraper.hca_req_id_from_page(page.replace("Job ID: 4981331", "Job ID: 1111111")) == "4981331"
    assert scraper.hca_req_id_from_page("<html><body>Not found</body></html>") == ""
    assert scraper.hca_req_id_from_page("") == ""


@pytest.mark.parametrize("raw,want", [
    ("4981331", "4981331"), (" J-049490 ", "J-049490"), (4981331, "4981331"),
    ("", ""), (None, ""), ("HCA Healthcare", ""), ("49 81331", ""), ("abc-def", ""),
])
def test_req_value_keeps_only_a_printed_requisition_number(raw, want):
    assert scraper._hca_req_value(raw) == want


def _card_job(fixture_text, with_body):
    job = scraper._parse_hca_cards(fixture_text(CARDS))[0]
    if with_body:
        posting = scraper._jobposting_from_html(fixture_text(JOB_PAGE))
        assert scraper._apply_posting(job, posting)
    return job


def test_normalize_job_puts_req_id_in_posting_facts_not_in_a_column(fixture_text):
    scraper.set_known_bodies([])
    row = scraper.normalize_job(_card_job(fixture_text, with_body=True))
    assert "req_id" not in row                       # not a hospital_jobs column
    assert row["job_id"] == "18349900"               # upsert key unchanged
    assert row["url"].endswith("/jobs/18349900-pharmacy-technician-instructor")
    facts = row["posting_facts"]
    assert facts["req_id"] == "5045755"
    assert facts["v"] == scraper.FACTS_VERSION and re.fullmatch(r"[0-9a-f]{12}", facts["h"])


def test_a_row_with_no_body_sends_null_facts_unless_the_merge_trigger_is_on(fixture_text, monkeypatch):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    row = scraper.normalize_job(_card_job(fixture_text, with_body=False))
    # the live trigger keeps the stored facts over a null; a {"req_id"}-only
    # object would replace them, so none is sent
    assert row["posting_facts"] is None and "req_id" not in row
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", True)
    row = scraper.normalize_job(_card_job(fixture_text, with_body=False))
    assert row["posting_facts"] == {"req_id": "5045755"}


def test_facts_hash_moves_only_for_rows_with_a_req_id():
    body = "Registered Nurse. " * 30
    base = scraper._facts_hash("RN", "Full time", None, body)
    assert scraper._facts_hash("RN", "Full time", None, body, "") == base   # every other row: unchanged
    with_req = scraper._facts_hash("RN", "Full time", None, body, "4981331")
    assert with_req != base
    assert with_req != scraper._facts_hash("RN", "Full time", None, body, "4981332")
    assert scraper._facts_hash("RN", "Full time", None, "short", "4981331") is None


def test_stored_facts_without_req_id_are_rebuilt_once_then_skipped(fixture_text):
    """The facts skip (push 10) must not keep a stored object that predates
    req_id: its hash was computed without one, so the row parses again."""
    scraper.set_known_bodies([])
    job = _card_job(fixture_text, with_body=True)
    plain = scraper.normalize_job(dataclasses.replace(job, req_id=""))
    old_h = plain["posting_facts"]["h"]
    stored = {"hospital_system": "HCA Healthcare", "job_id": "18349900", "ats_platform": "Talemetry",
              "desc_len": len(job.description), "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": old_h}
    scraper.set_known_bodies([stored])
    row = scraper.normalize_job(job)
    assert row["posting_facts"] is not None and row["posting_facts"]["req_id"] == "5045755"
    new_h = row["posting_facts"]["h"]
    assert new_h != old_h
    scraper.set_known_bodies([dict(stored, fh=new_h)])
    scraper._FACTS_SKIPS["n"] = 0
    assert scraper.normalize_job(job)["posting_facts"] is None and scraper._FACTS_SKIPS["n"] == 1
    scraper.set_known_bodies([])


class _R:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


def test_local_detail_pass_reads_req_id_when_the_card_had_none(fixture_text, monkeypatch):
    page = fixture_text(JOB_PAGE)
    import curl_cffi.requests as cr
    monkeypatch.setattr(cr, "get", lambda url, **kw: _R(page))
    monkeypatch.setattr(hlp.time, "sleep", lambda *_: None)
    monkeypatch.setenv("HCA_DESC_WORKERS", "1")
    blank = scraper._parse_hca_cards(fixture_text(CARDS))[0]
    blank.req_id = ""
    carded = scraper._parse_hca_cards(fixture_text(CARDS))[1]
    hlp._hca_detail_pass([blank, carded])
    assert blank.req_id == "4981331" and len(blank.description) >= 200
    assert carded.req_id == "5066993"          # the card's value is never overwritten
