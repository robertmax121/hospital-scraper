"""push5/systems (2026-09-25, coverage B1): the Jobvite Engage (Talemetry)
career-site adapter and the configs added for AHRQ systems that wrote no
rows. No network: `_curl_fetch` and `_curl_html` are replaced."""
import asyncio
import inspect
import json

import scraper
from scraper import Job


def _job(**kw):
    base = dict(title="RN", hospital_system="PeaceHealth", hospital_name="PeaceHealth", city="", state="",
                location="", specialty="", job_type="", url="https://careers.peacehealth.org/jobs/1-rn",
                job_id="1", posted_date="", description="", ats_platform="Jobvite")
    base.update(kw)
    return Job(**base)


ENTRY = {"id": "18152087", "talemetry_job_id": "18152087", "permalink": "patient-care-assistant",
         "title": "Patient Care Assistant",
         "location": {"street": "12605 E 16th Ave", "locality": "Aurora", "region_abbr": "CO",
                      "postal_code": "80045", "country": "United States", "region_full": "Colorado",
                      "name": "UCHlth Anschutz Inpt Pavilion"}}


def test_talemetry_entry_to_job():
    j = scraper._talemetry_job(ENTRY, "UCHealth", "https://careers.uchealth.org")
    assert j.title == "Patient Care Assistant"
    assert (j.city, j.state, j.location) == ("Aurora", "CO", "Aurora, CO")
    assert j.hospital_system == "UCHealth" and j.hospital_name == "UCHlth Anschutz Inpt Pavilion"
    assert j.url == "https://careers.uchealth.org/jobs/18152087-patient-care-assistant"
    assert j.job_id == "18152087" and j.ats_platform == "Jobvite"


def test_talemetry_entry_without_location_or_bad_state():
    e = {"id": "7", "permalink": "", "title": "Cook", "location": {"locality": "", "region_abbr": "Oregon"}}
    j = scraper._talemetry_job(e, "PeaceHealth", "https://careers.peacehealth.org")
    assert j.state == "" and j.hospital_name == "PeaceHealth"
    assert j.url == "https://careers.peacehealth.org/jobs/7"
    assert scraper._talemetry_job({"id": "", "title": "x"}, "S", "https://b") is None
    assert scraper._talemetry_job({"id": "9", "title": ""}, "S", "https://b") is None


class _Resp:
    def __init__(self, payload):
        self._p = payload
        self.status_code = 200

    def json(self):
        return self._p


def _entries(start, n):
    return [{**ENTRY, "id": str(start + i), "permalink": f"job-{start + i}"} for i in range(n)]


def test_scrape_talemetry_pages_until_total(monkeypatch):
    pages = {1: _entries(0, 100), 2: _entries(100, 100), 3: _entries(200, 30)}
    calls = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        page = int(kw["params"]["page"])
        calls.append((url, page, kw["params"]["per_page"], impersonate))
        return _Resp({"current_page": page, "per_page": 100, "total_entries": 230, "entries": pages[page]})

    async def no_jitter():
        return None

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "UCHealth", "https://careers.uchealth.org/"))
    assert len(jobs) == 230 and len({j.job_id for j in jobs}) == 230
    assert [c[1] for c in calls] == [1, 2, 3]
    assert calls[0][0] == "https://careers.uchealth.org/jobs/search.json"
    assert calls[0][2] == "100" and calls[0][3] == "chrome"


def test_scrape_talemetry_stops_on_error_and_keeps_rows(monkeypatch):
    # 2026-10-07 (push 10): a failed page is retried TALEMETRY_PAGE_RETRIES
    # times with a doubling backoff before the crawl stops; the rows already
    # listed are kept either way.
    attempts = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        attempts.append(kw["params"]["page"])
        if kw["params"]["page"] == "2":
            raise RuntimeError("HTTP 403 direct")
        return _Resp({"total_entries": 500, "entries": _entries(0, 100)})

    async def no_jitter():
        return None
    waits = []

    async def fake_sleep(seconds):
        waits.append(seconds)

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "_retry_sleep", fake_sleep)
    jobs = asyncio.run(scraper.scrape_talemetry(None, "Penn Medicine", "https://careers.pennmedicine.org"))
    assert len(jobs) == 100
    assert attempts == ["1"] + ["2"] * (scraper.TALEMETRY_PAGE_RETRIES + 1)
    assert waits == [scraper.TALEMETRY_RETRY_BASE_S * (2 ** i) for i in range(scraper.TALEMETRY_PAGE_RETRIES)]


def _page(ld_text):
    return f'<html><head><script type="application/ld+json">{ld_text}</script></head><body></body></html>'


BODY = "Sterile processing technician. " * 20


def test_talemetry_posting_valid_and_repaired():
    good = json.dumps({"@type": "JobPosting", "title": "T", "description": BODY, "employmentType": "FULL_TIME"})
    assert scraper._talemetry_posting(_page(good))["title"] == "T"
    # UCHealth / PeaceHealth pages carry a backslash before a hyphen, which json.loads rejects.
    bad = good.replace("Sterile processing", "Sterile \\- processing", 1)
    assert scraper._jobposting_from_html(_page(bad)) is None
    fixed = scraper._talemetry_posting(_page(bad))
    assert fixed is not None and "Sterile \\- processing" in fixed["description"]
    # Legitimate escapes survive the repair untouched.
    esc = json.dumps({"@type": "JobPosting", "title": "A \"quoted\" title", "description": BODY + "\nline"})
    assert scraper._talemetry_posting(_page(esc.replace("Sterile", "St\\-erile", 1)))["title"] == 'A "quoted" title'
    assert scraper._talemetry_posting("") is None


def test_talemetry_detail_fills_body(monkeypatch):
    ld = json.dumps({"@type": "JobPosting", "title": "T", "description": BODY, "datePosted": "2026-09-25",
                     "employmentType": "FULL_TIME"}).replace("Sterile", "Sterile \\-", 1)

    async def fake_html(url, impersonate="chrome", timeout=25):
        assert url == "https://careers.peacehealth.org/jobs/1-rn" and impersonate == "chrome"
        return _page(ld)

    monkeypatch.setattr(scraper, "_curl_html", fake_html)
    job = _job()
    assert asyncio.run(scraper._talemetry_detail(None, job)) is True
    assert len(job.description) >= 200


def test_configs_added_and_dead_entries_retired():
    # 2026-10-04 coverage round 3 added LifeBridge and Asante (test_cov3_round3.py).
    assert {"UCHealth", "PeaceHealth", "Penn Medicine"} <= set(scraper.TALEMETRY_SITES)
    assert "PeaceHealth" not in scraper.PHENOM_ORGS and "Penn Medicine" not in scraper.PHENOM_ORGS
    for lab in ("ThedaCare", "MUSC Health", "Presbyterian Healthcare Services", "St. Elizabeth Healthcare",
                "Baystate Health", "Nebraska Medicine", "Phoebe Putney Health", "Adventist HealthCare (MD)"):
        t = scraper.WORKDAY_TENANTS[lab]
        assert len(t) == 3 and t[1].isdigit(), lab
    # UVA Health runs on Phenom (push5/gov, with its non-health drop rule); its Phenom rows
    # already point at this Oracle tenant, so a second Oracle entry would re-add the dropped rows.
    assert "UVA Health" not in scraper.ORACLE_ORGS
    for lab in ("Cedars-Sinai", "Adena Health"):
        base, site = scraper.ORACLE_ORGS[lab]
        assert base.startswith("https://") and site.startswith("CX_")
    for lab in ("Carle Health", "Tanner Health", "Infirmary Health"):
        assert scraper.JIBE_SITES[lab].startswith("https://")
    for lab in ("Dartmouth Health", "Huntsville Hospital Health System", "Northside Hospital"):
        assert scraper.ICIMS_ORGS[lab].endswith(".icims.com")
    assert scraper.INFOR_ORGS["Hawaii Health Systems"] == ("css-hhsc-prd", "7", "EXTERNAL", "HI")
    assert scraper.INFOR_ORGS["Hawaii Pacific Health"][1:] == ("10", "EXTERNAL", "HI")
    assert scraper.HEALTHCARESOURCE_ORGS["Salina Regional Health Center"] == "srhc"
    # labels must not be rewritten onto another system at upsert time
    for lab in list(scraper.TALEMETRY_SITES) + ["Adventist HealthCare (MD)", "MUSC Health", "Carle Health"]:
        assert lab not in scraper.HOSPITAL_SYSTEM_ALIASES


def test_run_talemetry_is_scheduled():
    src = inspect.getsource(scraper)
    assert "run_talemetry(proxy_session)" in src
    assert '("MUSC Health",                   "https://musc.career-pages.com/jobs/search")' not in src


def test_blank_location_defaults_for_new_systems():
    for lab, st in (("Northside Hospital", "GA"), ("ThedaCare", "WI"), ("Phoebe Putney Health", "GA"),
                    ("Nebraska Medicine", "NE"), ("Dartmouth Health", "NH")):
        assert scraper.SYSTEM_LOCATION_DEFAULTS[lab.lower()][1] == st


def test_push5_lanes_configure_each_new_label_once():
    """push5/gov, push5/systems and push5/standalone merged: no board label may be
    listed on two platforms (duplicate fetches, and a second platform bypasses
    per-platform drop rules such as PHENOM_DROP_EMPLOYERS)."""
    boards = [scraper.WORKDAY_TENANTS, scraper.PHENOM_ORGS, scraper.JIBE_SITES, scraper.CSOD_ORGS,
              scraper.ICIMS_ORGS, scraper.ORACLE_ORGS, scraper.INFOR_ORGS, scraper.HEALTHCARESOURCE_ORGS,
              scraper.TALEMETRY_SITES, scraper.SF_RMK_BOARDS, scraper.HCTS_PORTALS, scraper.PAYCOM_ORGS]
    for label in ("UVA Health", "Tanner Health", "Phoebe Putney Health", "Broward Health",
                  "Ohio State Wexner Medical Center", "Cedars-Sinai", "AHMC Healthcare",
                  "Baptist Health Care (Pensacola)", "Northside Hospital", "RWJBarnabas Health"):
        assert sum(label in b for b in boards) == 1, label


def test_single_state_workday_tenants_default_their_state():
    """Presbyterian (NM), MUSC (SC) and Baystate (MA) are single-state systems; rows the
    board leaves without a state take it, so the 61d aliases can count them."""
    for lab, st in (("Presbyterian Healthcare Services", "NM"), ("MUSC Health", "SC"), ("Baystate Health", "MA")):
        assert lab in scraper.WORKDAY_TENANTS
        assert scraper.WD_TENANT_DEFAULT[lab][1] == st
    class J:
        city = ""; state = ""
    j = J()
    scraper._wd_apply_locations([j], {}, {}, {}, scraper.WD_TENANT_DEFAULT["Presbyterian Healthcare Services"])
    assert (j.city, j.state) == ("Albuquerque", "NM")
