"""Coverage round 3, builder D (2026-10-04): the server-rendered HTML list /
RSS adapter family (HTML_LIST_SITES) and the Eightfold PCSX adapter
(EIGHTFOLD_ORGS). Fixtures are trimmed copies of the live pages fetched on
2026-10-04. No network: `_curl_fetch` is replaced where a crawl runs."""
import asyncio
import inspect
import json
import os
import re

import scraper

R3 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "round3")


def _r3(name):
    with open(os.path.join(R3, name), encoding="utf-8") as f:
        return f.read()


def _site(system):
    return scraper.HTML_LIST_SITES[system]


# ── config sanity ────────────────────────────────────────────────────────

def test_every_html_list_site_is_complete():
    for system, cfg in scraper.HTML_LIST_SITES.items():
        assert cfg.get("url") or cfg.get("urls"), system
        assert re.fullmatch(r"[A-Z]{2}", cfg.get("state", "")), system
        assert cfg.get("platform"), system
        if cfg.get("mode") != "rss":
            assert cfg.get("card") and "(?P<url>" in cfg["title"] and "(?P<title>" in cfg["title"], system
            for tpl in cfg.get("urls") or [cfg["url"]]:
                assert any(k in tpl for k in ("{page}", "{page0}", "{offset}")), (system, tpl)
        # every regex compiles
        for rx in [cfg.get("card"), cfg.get("title"), cfg.get("id"), cfg.get("loc_strip"), cfg.get("title_strip")] \
                + list((cfg.get("fields") or {}).values()) + [p for p, _ in cfg.get("hospital_map") or ()]:
            if rx:
                re.compile(rx)
    assert scraper.SYSTEM_LOCATION_DEFAULTS["st. luke's health system (boise)"] == ("Boise", "ID")
    assert scraper.HOSPITAL_SYSTEM_ALIASES["St. Luke's Health System (Boise)"] == "St. Luke's Health System"
    for system in ("UK HealthCare", "University of Michigan Health", "Rush", "Premier Health"):
        assert system.lower() in scraper.SYSTEM_LOCATION_DEFAULTS, system


def test_new_runners_are_wired_into_run_all_and_uvm_playwright_entry_retired():
    src = inspect.getsource(scraper.run_all)
    assert "run_html_list(proxy_session)" in src and "run_eightfold(proxy_session)" in src
    pw = inspect.getsource(scraper.run_playwright_scrapers)
    live = [l for l in pw.splitlines() if "uvmhealthnetworkcareers.org" in l and not l.strip().startswith("#")]
    assert live == []


# ── one card per site ────────────────────────────────────────────────────

def test_community_health_network_cards():
    jobs = scraper._parse_html_list_page(_r3("chnw_search.html"), "Community Health Network", _site("Community Health Network"))
    assert len(jobs) == 2
    j = jobs[0]
    assert j.title == "Food Service Partner-North" and j.job_id == "2602623"
    assert j.url == "https://www.ecommunity.com/careers/jobs/food-service-partner-north-2602623"
    assert j.hospital_name == "Community Hospital North"
    assert (j.city, j.state, j.location) == ("Indianapolis", "IN", "Indianapolis, IN")
    assert j.specialty == "Food Services & Nutrition" and j.job_type == "Full-time"
    assert j.description.startswith("Shift: Variable") and j.ats_platform == "Drupal"
    assert j.hospital_system == "Community Health Network"


def test_wmchealth_cards_carry_company_city_and_hiring_range():
    jobs = scraper._parse_html_list_page(_r3("wmc_search.html"), "WMCHealth", _site("WMCHealth"))
    assert len(jobs) == 2
    j = jobs[0]
    assert j.title == "Access Center Ambassador" and j.job_id == "48609"
    assert j.url == "https://wmchealthjobs.org/?jobs=access-center-ambassador-48609"
    assert j.hospital_name == "NorthEast Provider Solutions Inc."
    assert (j.city, j.state) == ("Hawthorne", "NY")
    assert j.job_type == "Per Diem" and j.specialty == "Call Center"
    assert "Hiring Range: $21.40 -$26.90/hour" in j.description and "Shift: Variable, Varied" in j.description


def test_uvm_cards_take_partner_hospital_ref_and_city_from_the_address_block():
    jobs = scraper._parse_html_list_page(_r3("uvm_jobs.html"), "University of Vermont Health Network", _site("University of Vermont Health Network"))
    assert len(jobs) == 2
    j = jobs[0]
    assert j.job_id == "R0090575" and j.title.startswith("2027 Nurse Assistant Trainee Program")
    assert j.url.startswith("https://uvmhealthcareers.org/job/12346/")
    assert j.hospital_name == "Central Vermont Medical Center"
    assert (j.city, j.state) == ("Berlin", "VT")
    assert j.job_type == "Various" and j.specialty == "Residency & Trainee"
    assert j.description.startswith("Interested in becoming a Licensed Nursing Assistant")
    # a card without a Job Ref falls back to the path id
    card = _r3("uvm_jobs.html").replace('<span class="job-ref"><dt>Job Ref:</dt><dd>R0090575</dd></span>', "")
    jobs = scraper._parse_html_list_page(card, "University of Vermont Health Network", _site("University of Vermont Health Network"))
    assert jobs[0].job_id == "12346"


def test_umich_rows_keep_health_sites_drop_campus_rows_and_name_the_hospital():
    cfg = _site("University of Michigan Health")
    jobs = scraper._parse_html_list_page(_r3("umich_search.html"), "University of Michigan Health", cfg)
    assert len(jobs) == 2      # the third row is a campus (non-health) row
    j = jobs[0]
    assert j.job_id == "283043" and j.title.startswith("REGISTERED NURSE (University Hospital-7A")
    assert j.url == "https://careers.umich.edu/job_detail/283043/registered-nurse-university-hospital-7a-adult-general-medicinetelemetry-unit"
    assert j.hospital_name == "University Hospital"
    assert (j.city, j.state) == ("Ann Arbor", "MI")
    assert j.posted_date == "2026-10-03" and j.specialty == "MM UH CVC 7A-1"
    assert all(x.hospital_name in ("University Hospital", "C.S. Mott Children's Hospital", "University of Michigan Health") for x in jobs)
    # Kahn pavilion / multiple locations carry no city: the default fills later
    assert scraper._hl_city_state(cfg, "", "", "Kahn Health Care Pavilion") == ("", "MI")
    assert scraper._hl_city_state(cfg, "", "", "Troy Medical Campus") == ("Troy", "MI")


def test_uk_healthcare_rows_requisition_department_and_esh():
    jobs = scraper._parse_html_list_page(_r3("uk_postings.html"), "UK HealthCare", _site("UK HealthCare"))
    assert len(jobs) == 2
    j = jobs[0]
    assert j.job_id == "RE56025" and j.title == "Family Mentor"
    assert j.url == "https://ukjobs.uky.edu/postings/650776"
    assert j.specialty.startswith("8T110:") and j.hospital_name == "UK HealthCare"
    assert (j.city, j.state) == ("", "KY")
    assert j.description.startswith("This position is 100% grant funded")
    esh = jobs[1]
    assert esh.specialty.startswith("H10") and esh.hospital_name == "Eastern State Hospital"
    assert [u for u in _site("UK HealthCare")["urls"]] == [
        "https://ukjobs.uky.edu/postings/search?query=&985%5B%5D=12&page={page}",
        "https://ukjobs.uky.edu/postings/search?query=&985%5B%5D=17&page={page}"]


def test_avature_cards_strip_the_id_from_the_title_and_name_the_hospital():
    cfg = _site("St. Luke's Health System (Boise)")
    jobs = scraper._parse_html_list_page(_r3("avature_search.html"), "St. Luke's Health System (Boise)", cfg)
    assert len(jobs) == 2
    j = jobs[0]
    assert j.title == "Pharmacy Technician 3" and j.job_id == "157365"
    assert j.url.startswith("https://careers.slhs.org/careersmarketplace/JobDetail/")
    assert j.hospital_name == "St. Luke's Magic Valley Medical Center"
    assert (j.city, j.state) == ("Twin Falls", "ID")
    assert j.job_type == "Part-Time" and j.specialty == "Pharmacy Administration System Office"
    assert j.ats_platform == "Avature" and j.hospital_system == "St. Luke's Health System (Boise)"
    assert cfg["url"].endswith("jobOffset={offset}") and cfg["step"] == 6


def test_rush_rss_items():
    jobs = scraper._parse_rss_list(_r3("rush_jobs_rss.xml"), "Rush", _site("Rush"))
    assert len(jobs) == 3
    j = jobs[0]
    assert j.title == "Medical Assistant - Lisle" and j.job_id == "13072"
    assert j.url == "https://rush.fxrecruiter.com/jobs/details/united-states/il/chicago/work-type/medical-assistant-lisle/13072"
    assert (j.city, j.state) == ("Chicago", "IL") and j.posted_date == "2026-10-03"
    assert j.hospital_name == "Rush University Medical Center" and j.ats_platform == "FXRecruiter"
    assert jobs[2].hospital_name == "Rush Oak Park Hospital"
    assert scraper._parse_rss_list("<not xml", "Rush", _site("Rush")) == []


# ── crawl: paging, stop rule, no pages past a failure ────────────────────

def _fake_pages(pages):
    calls = []

    class _R:
        def __init__(self, text):
            self.text = text

    def fake(method, url, impersonate, timeout=60, **kw):
        calls.append(url)
        if url not in pages:
            raise RuntimeError("HTTP 404 direct")
        return _R(pages[url])
    return fake, calls


def test_scrape_html_list_pages_until_no_new_cards(monkeypatch):
    cfg = dict(_site("Community Health Network"))
    page = _r3("chnw_search.html")
    tpl = cfg["url"]
    fake, calls = _fake_pages({tpl.format(page0=0): page,
                               tpl.format(page0=1): page.replace("2602623", "2609999").replace("2603617", "2609998"),
                               tpl.format(page0=2): page})      # only repeats: stop
    monkeypatch.setattr(scraper, "_curl_fetch", fake)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_html_list(None, "Community Health Network", cfg))
    assert [j.job_id for j in jobs] == ["2602623", "2603617", "2609999", "2609998"]
    assert calls == [tpl.format(page0=0), tpl.format(page0=1), tpl.format(page0=2)]


def test_scrape_html_list_offset_paging_and_error_keeps_rows(monkeypatch):
    cfg = dict(_site("St. Luke's Health System (Boise)"))
    page = _r3("avature_search.html")
    tpl = cfg["url"]
    fake, calls = _fake_pages({tpl.format(offset=0): page,
                               tpl.format(offset=6): page.replace("157365", "157999").replace("155259", "155999")})
    monkeypatch.setattr(scraper, "_curl_fetch", fake)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_html_list(None, "St. Luke's Health System (Boise)", cfg))
    assert len(jobs) == 4 and calls[-1] == tpl.format(offset=12)      # offset 12 404s: rows kept


def test_scrape_html_list_crawls_every_url_template(monkeypatch):
    cfg = dict(_site("UK HealthCare"))
    page = _r3("uk_postings.html")
    u12, u17 = cfg["urls"]
    fake, calls = _fake_pages({u12.format(page=1): page, u17.format(page=1): page.replace("650776", "650001").replace("RE56025", "RE56001")})
    monkeypatch.setattr(scraper, "_curl_fetch", fake)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_html_list(None, "UK HealthCare", cfg))
    assert len(jobs) == 3                      # the ESH row repeats on the second search and is kept once
    assert calls == [u12.format(page=1), u12.format(page=2), u17.format(page=1), u17.format(page=2)]


def test_scrape_html_list_rss_mode_is_one_fetch(monkeypatch):
    fake, calls = _fake_pages({_site("Rush")["url"]: _r3("rush_jobs_rss.xml")})
    monkeypatch.setattr(scraper, "_curl_fetch", fake)
    jobs = asyncio.run(scraper.scrape_html_list(None, "Rush", _site("Rush")))
    assert len(jobs) == 3 and calls == [_site("Rush")["url"]]


# ── Eightfold ────────────────────────────────────────────────────────────

def test_eightfold_position_to_job():
    d = json.loads(_r3("eightfold_search.json"))
    p = d["data"]["positions"][0]
    j = scraper._eightfold_job(p, "Premier Health", "https://careers.premierhealth.com", "OH")
    assert j.title == "ASSOC NURSE MGR/ OBSERVATION UNIT" and j.job_id == "104067"
    assert (j.city, j.state, j.location) == ("Dayton", "OH", "Dayton, OH")
    assert j.url == "https://careers.premierhealth.com/careers/job/790304849592"
    assert j.specialty == "Nursing" and j.posted_date == "2026-06-16" and j.ats_platform == "Eightfold"
    assert j.hospital_system == "Premier Health" and j.hospital_name == "Premier Health"
    assert scraper._eightfold_job({"id": 1, "name": ""}, "S", "https://b", "OH") is None
    bare = scraper._eightfold_job({"id": 2, "name": "RN", "standardizedLocations": []}, "S", "https://b", "OH")
    assert (bare.city, bare.state) == ("", "OH") and bare.job_id == "2"


class _Resp:
    def __init__(self, status, text="", payload=None):
        self.status_code, self.text, self._p = status, text, payload

    def json(self):
        return self._p


def test_eightfold_fetch_all_pages_with_csrf_until_count(monkeypatch):
    d = json.loads(_r3("eightfold_search.json"))
    pos = d["data"]["positions"]
    calls = []

    class _S:
        def __init__(self, impersonate):
            self.proxies = None

        def get(self, url, params=None, headers=None, timeout=0):
            calls.append((url, dict(params or {}), dict(headers or {})))
            if url.endswith("/careers"):
                return _Resp(200, '<html><meta name="_csrf" content="tok123"></html>')
            start = int(params["start"])
            page = [dict(p, id=p["id"] + start) for p in pos] if start < 4 else []
            return _Resp(200, payload={"data": {"count": 4, "positions": page}})
    monkeypatch.setattr(scraper, "curl_requests", type("M", (), {"Session": staticmethod(lambda impersonate: _S(impersonate))}))
    monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
    positions, count = scraper._eightfold_fetch_all("https://careers.premierhealth.com", "premierhealth.com", "Premier Health")
    assert count == 4 and len(positions) == 4
    assert calls[0][0] == "https://careers.premierhealth.com/careers"
    assert [c[1].get("start") for c in calls[1:]] == [0, 2]
    assert all(c[2]["X-CSRF-TOKEN"] == "tok123" and c[1]["domain"] == "premierhealth.com" for c in calls[1:])
    assert scraper.EIGHTFOLD_ORGS["Premier Health"] == ("https://careers.premierhealth.com", "premierhealth.com", "OH")
