# 2026-10-12: TalentBrew requisition number (owner approved). job_id is
# TalentBrew's own posting id (CommonSpirit 101678237952); the employer page
# prints its own number ("Job ID / Requisition ID 2026-495862"). Every
# TalentBrew job page carries it in <meta name="job-ats-req-id"> (and
# gtm_reqid), equal to the JSON-LD identifier.value; UHG's search cards also
# print it. It is stored as posting_facts["req_id"] through the HCA plumbing.
# job_id, url and the upsert key are unchanged.
# Fixtures are real snippets fetched 2026-10-12 (tests/fixtures/tb_req/), one
# job page per tenant (two for Kaiser: its newer page layout's JSON-LD does
# not parse), plus the first three UHG search cards.
import asyncio
import dataclasses
import re

import pytest

import scraper

PAGES = [
    ("commonspirit", "2026-497242"),       # "Requisition ID" / "Job ID"
    ("uhg", "2386735"),                    # "Requisition number:"
    ("kaiser", "1453546"),                 # "Job Number"
    ("kaiser_ajd", "1453272"),             # JSON-LD does not parse: the meta answers
    ("maxim", "592405"),                   # JSON-LD does not parse: the meta answers
    ("scion", "558281"),                   # "Req ID"
    ("hackensack", "2026-181533"),         # "Requisition #"
    ("university_health", "00070959"),     # leading zeros kept as printed
    ("memorialcare", "MEM009943"),         # the Taleo requisition its Apply link names
    ("enhabit", "2026-159119"),            # "Req ID:"
]
BROKEN_LD = {"kaiser_ajd", "maxim"}


def _page(fixture_text, name):
    return fixture_text(f"tb_req/{name}.html")


@pytest.mark.parametrize("name,want", PAGES)
def test_every_tenant_page_gives_the_printed_number(fixture_text, name, want):
    page = _page(fixture_text, name)
    assert scraper.tb_req_id_from_page(page) == want
    posting = scraper._jobposting_from_html(page)
    if name in BROKEN_LD:
        assert posting is None
    else:
        assert scraper._ld_identifier_value(posting) == want
        assert scraper.tb_req_id_from_page(page, posting) == want
    # the meta and the visible label agree with the JSON-LD on the live page
    assert f'content="{want}"' in page


@pytest.mark.parametrize("name,want", [p for p in PAGES if p[0] not in BROKEN_LD])
def test_json_ld_answers_when_the_meta_is_missing(fixture_text, name, want):
    page = re.sub(r'<meta name="(?:job-ats-req-id|gtm_reqid)"[^>]*>', "", _page(fixture_text, name))
    assert "job-ats-req-id" not in page
    assert scraper.tb_req_id_from_page(page) == want


def test_no_meta_and_no_json_ld_gives_blank(fixture_text):
    page = re.sub(r'<meta name="(?:job-ats-req-id|gtm_reqid)"[^>]*>', "", _page(fixture_text, "maxim"))
    assert scraper.tb_req_id_from_page(page) == ""
    assert scraper.tb_req_id_from_page("") == ""
    assert scraper.tb_req_id_from_page('<meta name="job-ats-req-id" content="">') == ""
    # prose or an over-long token is not a requisition number
    assert scraper.tb_req_id_from_page('<meta name="job-ats-req-id" content="see posting">') == ""
    assert scraper.tb_req_id_from_page('<meta name="job-ats-req-id" content="%s">' % ("1" * 30)) == ""


def test_meta_wins_over_json_ld(fixture_text):
    page = _page(fixture_text, "commonspirit").replace('"value":"2026-497242"', '"value":"9999999"')
    assert scraper.tb_req_id_from_page(page) == "2026-497242"


def _job(**kw):
    base = dict(title="Pharmacy Tech Cert", hospital_system="CommonSpirit Health", hospital_name="CHI St. Gabriel's Health",
                city="Little Falls", state="MN", location="Little Falls, MN", specialty="", job_type="",
                url="https://www.commonspirit.careers/job/little-falls/pharmacy-tech-cert/35300/101869449488",
                job_id="101869449488", posted_date="", description="", ats_platform="TalentBrew")
    base.update(kw)
    return scraper.Job(**base)


def test_jsonld_detail_reads_it_for_talentbrew_rows_only(fixture_text, monkeypatch):
    page = _page(fixture_text, "commonspirit")
    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: asyncio.sleep(0, result=page))
    tb = _job()
    asyncio.run(scraper._jsonld_detail(None, tb))
    assert tb.req_id == "2026-497242"
    assert tb.job_id == "101869449488"                     # job_id and url untouched
    assert tb.url.endswith("/35300/101869449488")
    other = _job(hospital_system="Harris Health System", ats_platform="PreloadState")
    asyncio.run(scraper._jsonld_detail(None, other))
    assert other.req_id == ""                              # the shared reader stays as it was elsewhere


class _R:
    status = 200

    def __init__(self, text):
        self._t = text

    async def text(self):
        return self._t


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def test_tb_page_detail_reads_it_where_the_json_ld_is_broken(fixture_text, monkeypatch):
    page = _page(fixture_text, "maxim")
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(page)))
    j = _job(hospital_system="Maxim Healthcare", job_id="101867886224",
             url="https://careers.maximhealthcare.com/job/arlington/private-duty-lvn-pediatric-and-amp-adult-home-care/49382/101867886224")
    asyncio.run(scraper._tb_page_detail(None, j))
    assert j.req_id == "592405" and j.job_id == "101867886224"


def test_tb_page_detail_keeps_a_card_value(fixture_text, monkeypatch):
    page = _page(fixture_text, "uhg")
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(page)))
    j = _job(hospital_system="UnitedHealth Group", job_id="101871518032", req_id="2386735")
    asyncio.run(scraper._tb_page_detail(None, j))
    assert j.req_id == "2386735"


def test_uhg_search_cards_carry_it(fixture_text, monkeypatch):
    cards = fixture_text("tb_req/uhg_cards.html")
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(cards)))

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert [(j.job_id, j.req_id) for j in jobs] == [
        ("101871518032", "2386735"),
        ("101871518000", "2393501"),
        ("101871517952", "2393388"),
    ]
    first = jobs[0]
    assert first.url == ("https://careers.unitedhealthgroup.com/job/katy/per-diem-ambulatory-service-representative-ii-"
                         "kelsey-seybold-clinic-west-campus/34088/101871518032")
    assert first.title == "Per Diem Ambulatory Service Representative II Kelsey Seybold Clinic-WEST Campus"


def test_uhg_card_without_the_span_parses_with_a_blank_req_id(fixture_text, monkeypatch):
    cards = re.sub(r'<span class="job-id job-info">[^<]*</span>', "", fixture_text("tb_req/uhg_cards.html"))
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(cards)))

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_uhg_talentbrew(None))
    assert len(jobs) == 3 and all(j.req_id == "" for j in jobs)


def test_normalize_job_sends_it_as_posting_facts_req_id():
    body = "The Pharmacy Technician supports the pharmacist in dispensing medications. " * 30
    j = _job(description=body, req_id="2026-497242")
    d = scraper.normalize_job(j)
    assert "req_id" not in d
    assert d["job_id"] == "101869449488" and d["url"] == j.url
    assert d["posting_facts"]["req_id"] == "2026-497242"
    # the facts hash moves with the number, so stored TalentBrew facts are rebuilt once
    plain = scraper.normalize_job(dataclasses.replace(j, req_id=""))
    assert plain["posting_facts"]["h"] != d["posting_facts"]["h"]
    assert "req_id" not in plain["posting_facts"]
