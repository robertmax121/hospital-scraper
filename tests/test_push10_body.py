"""Push 10 (2026-10-07), worktree "body": rich descriptions and fact accuracy.
Items: Rush job-page detail (1), Hackensack's TalentBrew page body through
the Compensation section plus the forced re-read slot (2), detail budgets
and the small body captures: UMich body regex, CHS div.description, ADP CX
jobQualifications, MyMichigan on Jibe with the title cut (3), the 20,000
body cap (4), BAYADA's state from the Greenhouse feed (5), the PRN status
veto, the sign-on amount reader and the Oracle flex shift (6).

Fixtures under tests/fixtures/push10 are trimmed copies of the pages and API
answers fetched read-only on 2026-10-07; the facts texts are copied from
stored hospital_jobs descriptions (the row id is named beside each). No
network, no database."""
import asyncio
import inspect
import json
import os
from datetime import datetime

import pytest

import scraper
from scraper import Job

P10 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "push10")


def _p10(name):
    with open(os.path.join(P10, name), encoding="utf-8") as f:
        return f.read()


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


class _R:
    def __init__(self, status, body):
        self.status, self._body = status, body
        self.headers = {"content-type": "text/html" if isinstance(body, str) else "application/json"}

    async def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)

    async def json(self, content_type=None):
        return json.loads(self._body) if isinstance(self._body, str) else self._body

    async def read(self):
        return b""


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def _serve(monkeypatch, routes):
    """Route req() by URL substring; returns the list of (method, url)."""
    seen = []

    def fake_req(session, method, url, **kw):
        seen.append((method, url))
        for frag, status, body in routes:
            if frag in url:
                return _Ctx(_R(status, body))
        return _Ctx(_R(404, ""))
    monkeypatch.setattr(scraper, "req", fake_req)
    return seen


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    scraper.set_known_bodies([])
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)

    async def no_wait():
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    yield
    scraper.set_known_bodies([])


# ── item 1: Rush (FXRecruiter) job-page detail ───────────────────────────────

def test_rush_detail_page_body_and_pay_range(monkeypatch):
    # 2976: a 2024 CRNA posting, one of the 206 rows with no body on 10-07
    html = _p10("rush_job_2976.html")
    assert "application/ld+json" in html and scraper._jobposting_from_html(html) is None   # the trailing comma
    job = _job(hospital_system="Rush", hospital_name="Rush University Medical Center", state="IL",
               url="https://rush.fxrecruiter.com/jobs/details/united-states/il/chicago/crna-rush-university-medical-center-chicago-downtown/2976",
               job_id="2976", ats_platform="FXRecruiter")
    seen = []

    async def fake_html(u, impersonate="chrome", timeout=25):
        seen.append(u)
        return html
    monkeypatch.setattr(scraper, "_curl_html", fake_html)
    assert asyncio.run(scraper._html_list_detail(None, job)) is True
    assert seen == [job.url]
    assert len(job.description) > 4000 and "<" not in job.description
    # the physician / CRNA template opens with the role and the hospital, not the location lines of 13072
    assert job.description.startswith("Job Description\n\nCertified Registered Nurse Anesthetist (CRNA)\n\nRush University Medical Center")
    assert "Pay Range: $250,000 - $350,000" in job.description
    assert scraper.extract_posted_wage(job.description) == (250000.0, 350000.0, "year")
    d = scraper.normalize_job(job)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (250000.0, 350000.0, "year")
    assert "FXRecruiter" in scraper.KNOWN_BODY_PLATFORMS      # a stored body is not fetched again every night


# ── item 2: Hackensack Meridian through the Compensation section ────────────

def test_hackensack_page_body_reaches_the_compensation_section(monkeypatch):
    assert "Hackensack Meridian Health" in scraper.TB_PAGE_DETAIL_ORGS
    html = _p10("hmh_job_101588192384.html")
    summary = scraper.strip_html(scraper._jobposting_from_html(html)["description"]).strip()
    assert 500 < len(summary) < 1200 and "Compensation" not in summary        # what the rows held
    _serve(monkeypatch, [("hackensackmeridianhealth.org", 200, html)])
    job = _job(hospital_system="Hackensack Meridian Health", hospital_name="Hackensack University Medical Center",
               city="Hackensack", state="NJ", ats_platform="TalentBrew", job_id="101588192384",
               url="https://jobs.hackensackmeridianhealth.org/job/hackensack/cardiovascular-technologist-full-time/19511/101588192384")
    assert asyncio.run(scraper._tb_page_detail(None, job)) is True
    assert len(job.description) > 6000 and "<" not in job.description
    assert job.description.startswith("Overview\n\nOur team members are the heart")
    assert "Compensation\nMinimum rate of $40.28 Hourly" in job.description
    assert "New Jersey Pay Transparency Act" in job.description
    assert scraper.extract_posted_wage(job.description) == (40.28, 40.28, "hour")
    assert job.job_type == "Full Time with Benefits"                         # the JSON-LD employmentType
    d = scraper.normalize_job(job)
    assert (d["wage_min"], d["wage_max"], d["wage_unit"]) == (40.28, 40.28, "hour")
    assert d["state"] == "NJ" and scraper.derive_job_type(job.title, job.job_type) == "full_time"


def test_other_talentbrew_tenants_keep_their_reader():
    # Kaiser / UHG / Enhabit / Maxim run _tb_page_detail through their own
    # HTML-list runners (test_detail_wiring covers the Kaiser page); the
    # /results tenants other than University Health and Hackensack keep the
    # JSON-LD reader.
    for sys in ("CommonSpirit Health", "Kaiser Permanente", "UnitedHealth Group"):
        assert sys not in scraper.TB_PAGE_DETAIL_ORGS
    src = inspect.getsource(scraper.run_talentbrew)
    assert "_tb_page_detail if sys in TB_PAGE_DETAIL_ORGS else _jsonld_detail" in src


def test_forced_refresh_slot_visits_each_stored_body_once_inside_the_window(monkeypatch):
    monkeypatch.setattr(scraper, "FORCED_REFRESH", {"Tenet": ("2026-10-08", "2026-10-14")})
    a = datetime(2026, 10, 8).toordinal()
    hits = {str(i): 0 for i in range(300)}
    for d in range(-3, 10):                                    # three nights before, the window, three after
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: a + d)
        for jid in hits:
            hits[jid] += scraper._force_slot("Tenet", jid)
    assert set(hits.values()) == {1}
    for d in (-1, 7):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: a + d)
        assert not any(scraper._force_slot("Tenet", j) for j in hits)
    monkeypatch.setattr(scraper, "_run_day", lambda: a + 2)
    assert scraper._force_slot("Other", "1") is False
    # an inverted or malformed window never fires
    monkeypatch.setattr(scraper, "FORCED_REFRESH", {"Tenet": ("2026-10-14", "2026-10-08"), "X": ("soon", "later")})
    assert scraper._force_slot("Tenet", "1") is False and scraper._force_slot("X", "1") is False


def test_shipped_forced_refresh_entry_is_hackensack_for_one_week():
    win = scraper.FORCED_REFRESH["Hackensack Meridian Health"]
    lo, hi = (datetime.strptime(x, "%Y-%m-%d") for x in win)
    assert 1 <= (hi - lo).days + 1 <= 14
    assert "Hackensack Meridian Health" in scraper.TB_PAGE_DETAIL_ORGS


def test_forced_rows_queue_after_the_rows_with_no_body_and_keep_the_stored_body(monkeypatch):
    T = "Tenet Healthcare"                                     # on PAY_REREAD_SYSTEMS, so a "pay" row can sit beside the forced ones
    monkeypatch.setattr(scraper, "_force_slot", lambda canon, jid: canon == T and jid in ("1", "2"))
    monkeypatch.setattr(scraper, "_refresh_slot", lambda canon, jid: False)
    monkeypatch.setattr(scraper, "_pay_slot", lambda canon, jid: True)
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    cur = str(scraper.FACTS_VERSION)
    scraper.set_known_bodies([{"hospital_system": T, "job_id": "1", "desc_len": 1800, "fv": cur},
                              {"hospital_system": T, "job_id": "2", "desc_len": 1800, "fv": cur},
                              {"hospital_system": T, "job_id": "3", "desc_len": 1800, "fv": cur},
                              {"hospital_system": T, "job_id": "5", "desc_len": 8000, "fv": None},
                              {"hospital_system": T, "job_id": "6", "desc_len": 1800, "fv": cur, "wage_min": None}])
    jobs = [_job(hospital_system=T, job_id=j, url=f"https://x/{j}", ats_platform="Oracle HCM")
            for j in ("1", "2", "3", "4", "5", "6")]
    kinds = {j.job_id: scraper._known_kind(T, j) for j in jobs}
    assert kinds == {"1": "force", "2": "force", "3": "known", "4": None, "5": "cut", "6": "pay"}
    held = []
    cands, known, dup = scraper._detail_candidates(T, jobs, scraper._DescBudget(100), held=held)
    order = [c.job_id for c in cands]
    assert order[0] == "4" and sorted(order[1:3]) == ["1", "2"] and order[3:] == ["6", "5"]   # no body, forced, pay, cut
    assert known == 1 and dup == 0
    assert sorted((h[0].job_id, h[2]) for h in held) == [("1", "force"), ("2", "force"), ("5", "cut"), ("6", "pay")]
    note = scraper._held_note(held)
    assert "2 stored bodies queued on their forced slot" in note and "1 cut at the old 8,000 cap queued" in note
    scraper._settle_held(T, held)                             # no fetch refilled them
    assert all(j.description == "" for j in jobs)             # the trigger keeps the stored bodies
    # off its slot night the same row is simply known
    monkeypatch.setattr(scraper, "_force_slot", lambda canon, jid: False)
    monkeypatch.setattr(scraper, "_pay_slot", lambda canon, jid: False)
    assert scraper._known_kind(T, jobs[0]) == "known"


# ── item 3: budgets and the small body captures ─────────────────────────────

def test_detail_budgets_raised():
    assert scraper.HTML_LIST_DESC_MAX_PER_RUN == 2500 and scraper.HTML_LIST_DESC_BUDGET.total == 2500
    assert scraper.TB_PAGE_DESC_MAX_PER_RUN == 6000 and scraper.TB_PAGE_DESC_BUDGET.total == 6000


def test_umich_job_page_body_regex(monkeypatch):
    html = _p10("umich_job_280808.html")
    assert "ld+json" not in html                                # the page carries no JobPosting
    job = _job(hospital_system="University of Michigan Health", hospital_name="University Hospital", state="MI",
               ats_platform="Drupal", job_id="280808",
               url="https://careers.umich.edu/job_detail/280808/med-staffcredentialing-specialist")

    async def fake_html(u, impersonate="chrome", timeout=25):
        return html
    monkeypatch.setattr(scraper, "_curl_html", fake_html)
    assert asyncio.run(scraper._html_list_detail(None, job)) is True
    assert len(job.description) > 7000 and "<" not in job.description
    assert job.description.startswith("Mission Statement\n\nMichigan Medicine improves the health")
    assert "Job Summary" in job.description and "Required Qualifications" in job.description
    assert job.description.endswith("including protected veterans and individuals with disabilities.")
    assert "Job Opening ID" not in job.description and "Apply Now" not in job.description


def test_chs_div_description_beats_the_jsonld_intro(monkeypatch):
    html = _p10("chs_job_36448251.html")
    assert len(scraper.strip_html(scraper._jobposting_from_html(html)["description"])) < 400   # the intro only
    calls = []

    async def fake_fetch(session, url, timeout=20):
        calls.append(url)
        return html
    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch)
    job = _job(hospital_system="Community Health Systems", ats_platform="WPJobBoard",
               url="https://www.careershealthcare.com/job/hospital/in/fort-wayne/medical-technologist-nights-weekend-premium")
    assert asyncio.run(scraper._jsonld_board_detail(None, job)) is True
    assert calls == [job.url]                                   # one request, as before
    assert len(job.description) > 3000 and "<" not in job.description
    assert job.description.startswith("Job Description\n\nMedical Technologist - Weekend Premium\n\nFull-Time | 36-hours\n\nShift | Nights")
    assert "Benefits" in job.description and "Health Insurance (Medical, Dental" in job.description
    f = scraper.extract_posting_facts(job.description, "full_time")
    assert f["shift"][0][0] == "Nights"
    # the other title-only boards keep the JSON-LD alone
    other = _job(hospital_system="Oceans Healthcare", ats_platform="OceansJobBoard", url="https://x/1")
    assert asyncio.run(scraper._jsonld_board_detail(None, other)) is True
    assert len(other.description) < 400
    # a page whose JSON-LD already holds the posting: the longer wins either way
    full = _job(hospital_system="Community Health Systems", ats_platform="WPJobBoard", url="https://x/2",
                description="x" * 5000)
    assert asyncio.run(scraper._jsonld_board_detail(None, full)) is False and full.description == "x" * 5000


def test_adpcx_appends_the_qualifications_field():
    j = json.loads(_p10("adpcx_requisition.json"))
    job = scraper._adpcx_job(j, "Cabell Huntington Hospital", "mhnetwork", "WV")
    assert job and (job.city, job.state) == ("Huntington", "WV")
    assert job.description.startswith("St. Mary's Medical Center is currently seeking a full time RN.")
    assert "\n\nQualifications\nCurrent WV RN license\n\nBLS required; ACLS within 6 months\n\n1 year of acute care experience preferred" in job.description
    rq = scraper.extract_requirements(job.description, job.title)
    assert "Current WV RN license" in [x[0] for x in rq["licensure"]]
    # no repeat when the description already carries it; nothing when the field is empty
    j2 = dict(j, jobDescription=j["jobDescription"] + j["jobQualifications"])
    assert scraper._adpcx_job(j2, "Cabell Huntington Hospital", "mhnetwork", "WV").description.count("Current WV RN license") == 1
    j3 = dict(j, jobQualifications=None)
    assert "Qualifications" not in scraper._adpcx_job(j3, "Cabell Huntington Hospital", "mhnetwork", "WV").description


def test_mymichigan_moves_to_the_jibe_feed(monkeypatch):
    assert scraper.JIBE_SITES["MyMichigan Health"] == "https://careers.mymichigan.org"
    assert "MyMichigan Health" not in scraper.ICIMS_ORGS
    pw = inspect.getsource(scraper.run_playwright_scrapers)
    assert [l for l in pw.splitlines() if "careers.mymichigan.org" in l and not l.strip().startswith("#")] == []
    data = json.loads(_p10("jibe_mymichigan_jobs.json"))
    seen = _serve(monkeypatch, [("/api/jobs", 200, data)])

    async def no_html(session, url, timeout=20):
        return ""
    monkeypatch.setattr(scraper, "_fetch_html", no_html)
    jobs = asyncio.run(scraper.scrape_jibe(None, "MyMichigan Health", "https://careers.mymichigan.org"))
    assert [(m, u) for m, u in seen] == [("get", "https://careers.mymichigan.org/api/jobs")]
    assert len(jobs) == 2
    j = jobs[0]
    assert (j.title, j.job_id) == ("Supervisor Phlebotomy", "49124")
    assert (j.city, j.state) == ("Mt. Pleasant", "MI")
    assert j.url == "https://careers.mymichigan.org/jobs/49124?lang=en-us"
    assert j.job_type == "FULL_TIME" and j.posted_date == "2026-10-06"
    assert len(j.description) > 500 and "<" not in j.description and j.description.startswith("Summary\n\nThis position")
    assert j.ats_platform == "iCIMS" and "iCIMS" in scraper.KNOWN_BODY_PLATFORMS
    assert "\n" not in j.title
    d = scraper.normalize_job(j)
    assert d["title"] == "Supervisor Phlebotomy" and scraper.canonical_job_type(j.job_type) == "Full time"


def test_title_never_carries_a_line_break():
    # MyMichigan 35191488 as the Playwright route stored it
    d = scraper.normalize_job(_job(title="Registered Nurse RN - ED\n\nReq ID: 49092", hospital_system="MyMichigan Health",
                                    hospital_name="MyMichigan Health", description="x" * 300))
    assert d["title"] == "Registered Nurse RN - ED"
    d = scraper.normalize_job(_job(title="\n  Unit Assistant\nReq ID: 49141"))
    assert d["title"] == "Unit Assistant"
    assert scraper.normalize_job(_job(title="Plain Title"))["title"] == "Plain Title"


# ── item 4: body cap ─────────────────────────────────────────────────────────

def test_body_cap_is_20000_and_the_facts_window_stays_12000():
    assert len(scraper.strip_html("<p>" + "x" * 25000 + "</p>")) == 20000
    assert len(scraper.strip_html("x" * 15000)) == 15000
    assert scraper._FACTS_WINDOW == 12000
    filler = "The nurse cares for patients on the unit. " * 300           # 12,600 characters
    late = filler + "\n$5,000 sign-on bonus for RNs."
    assert scraper.extract_posting_facts(late, "full_time") is None or scraper.extract_posting_facts(late, "full_time").get("signon") is None
    early = "$5,000 sign-on bonus for RNs.\n" + filler
    assert scraper.extract_posting_facts(early, "full_time")["signon"] == 5000


# ── item 5: BAYADA state from the Greenhouse feed ────────────────────────────

def test_greenhouse_state_from_location_offices_and_metadata(monkeypatch):
    j = json.loads(_p10("greenhouse_bayada_job.json"))
    assert j["location"]["name"].startswith("Sicklerville, NJ 08081 | ")
    assert scraper.parse_city_state(j["location"]["name"]) == ("Sicklerville", "")   # the shape behind 2,172 blank states
    assert scraper._greenhouse_city_state(j) == ("Sicklerville", "NJ")
    # offices decide when the location text names no state (Field offices carry none)
    j2 = {"location": {"name": "Wapwallopen"}, "offices": [{"name": "Field", "location": None}, {"name": "Scranton", "location": "Scranton, PA"}]}
    assert scraper._greenhouse_city_state(j2) == ("Wapwallopen", "PA")
    # then an address-like metadata value; the posting's own city stands (Lebanon is ambiguous)
    j3 = {"location": {"name": "Lebanon"}, "offices": [],
          "metadata": [{"name": "Salary", "value": None}, {"name": "BAYADA Office address", "value": "4300 Haddonfield Rd W Building, Pennsauken, NJ 08109"}]}
    assert scraper._greenhouse_city_state(j3) == ("Lebanon", "NJ")
    j4 = {"location": {"name": "Springfield"}, "metadata": [{"name": "Job Category", "value": "Registered Nurse"}, {"name": "State", "value": "Massachusetts"}]}
    assert scraper._greenhouse_city_state(j4) == ("Springfield", "MA")
    assert scraper._greenhouse_city_state({"location": {"name": "Charlotte, NC 28203"}}) == ("Charlotte", "NC")
    assert scraper._greenhouse_city_state({"location": {"name": "Raleigh"}, "offices": [], "metadata": []}) == ("Raleigh", "")
    assert scraper._greenhouse_city_state({}) == ("", "")
    # through the adapter
    _serve(monkeypatch, [("boards-api.greenhouse.io/v1/boards/bayada/jobs", 200, {"jobs": [j]})])
    jobs = asyncio.run(scraper.scrape_greenhouse(None, "BAYADA Home Health Care", "bayada"))
    assert len(jobs) == 1 and (jobs[0].city, jobs[0].state, jobs[0].job_id) == ("Sicklerville", "NJ", "8869663002")
    d = scraper.normalize_job(jobs[0])
    assert (d["city"], d["state"], d["location"]) == ("Sicklerville", "NJ", "Sicklerville, NJ")


# ── item 6a: PRN status-list veto ────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    # Advocate 34403132 (2,550 rows)
    "Eligibility for programs listed above may depend on your FTE or status (e.g., full-time, part-time, per diem, temporary, etc.); please ask a Recruiter for more information during the interview.",
    # Jefferson 36508543 (812)
    "All colleagues, including those who work less than part-time (including per diem colleagues, adjunct faculty, and Jeff Temps ), have access to medical, dental and vision coverage.",
    # Lifepoint 11663450 (261)
    "Multiple levels of medical, dental and vision coverage - tailored benefit options for part-time and PRN employees, and more.",
    # MaineHealth 32717036 (240)
    "Bonus amount prorated for Part-time hires, per diem hires are ineligible.",
    # Adventist HealthCare 36147483 (186)
    "If the salary range is listed as $0 or if the position is Per Diem (with a fixed rate), salary discussions will take place during the screening.",
    # Norton 29004760 (121)
    "No experience required for full-time or part-time positions. If PRN, must have one year of RN/LPN experience in an inpatient, acute-care setting.",
    # South Central Regional 35996800: a label over a "Full Time" value
    "Full Time/PRN:\n\nFull Time\n\nJob Summary\n\nWe are seeking a friendly, detail-oriented specialist.",
    # BAYADA 22142387
    "Flexible Scheduling\nChoose from full-time, part-time, and PRN opportunities (minimum 4 shifts/month for PRN), with 8-, 10- and 12-hour shifts.",
    # GoHealth 1581685
    "We are considering fulltime, part-time or per diem (PRN) schedules.",
    # Sentara 32322731
    "Flexible Positions to Meet your Work Life Balance: Full Time, Part Time and Flexi/PRN",
])
def test_prn_in_a_status_list_is_not_the_shift(text):
    body = "Registered Nurse for the medical unit. Requires BLS. " + text
    f = scraper.extract_posting_facts(body, "full_time", "Registered Nurse")
    assert not any(s[0] == "PRN" for s in (f or {}).get("shift") or []), text


@pytest.mark.parametrize("text,expect", [
    ("Schedule: PRN · Night shift", {"PRN", "Nights"}),            # the Oracle schedule line
    ("This is a PRN position on the medical unit.", {"PRN"}),
    ("Status: Per Diem\nShift: Nights", {"PRN", "Nights"}),
    ("Per diem, nights.", {"PRN", "Nights"}),                     # (a third tag, 3x12s, would drop: two at most)
    ("Full-time or PRN openings for this unit.", {"PRN"}),         # one other status, no class noun: the tag stands
])
def test_prn_as_the_jobs_own_status_keeps_its_tag(text, expect):
    body = "Registered Nurse for the medical unit. Requires BLS. " + text
    f = scraper.extract_posting_facts(body, "per_diem", "Registered Nurse")
    assert {s[0] for s in f["shift"]} == expect, text


def test_prn_status_option_helper():
    s = "Choose from full-time, part-time, and PRN opportunities (minimum 4 shifts/month for PRN), with 8-hour shifts."
    ms = list(scraper._FACT_SHIFT_C[-1][1].finditer(s))
    assert scraper._FACT_SHIFT_C[-1][0] == "PRN" and len(ms) == 2
    assert all(scraper._prn_is_a_status_option(s, m) for m in ms)   # the second mention sits after the same list
    s2 = "Shift: PRN"
    m = next(scraper._FACT_SHIFT_C[-1][1].finditer(s2))
    assert scraper._prn_is_a_status_option(s2, m) is False


# ── item 6b: sign-on amount reader ──────────────────────────────────────────

def test_signon_amount_with_cents():
    # UPMC 33994051, BAYADA 32459560, Guthrie 24082822, University Hospitals 32692826
    for text, amount in (("This position offers a $1,500.00 sign-on bonus with 1-year work commitment!", 1500),
                         ("$5,000.00 SIGN-ON BONUS!!\n\nMake an Impact as a Full-Time Registered Nurse at BAYADA.", 5000),
                         ("Up to a $15,000.00 Sign on Bonus!\n\nThis position does require travel", 15000),
                         ("$5,000.00 sign-on bonus for eligible external hires", 5000),
                         ("$10,000 sign-on bonus for experienced RNs and a $3,000 relocation package.", 10000)):
        f = scraper.extract_posting_facts(text + "\nRequires BLS.", "full_time")
        assert (f["signon"], f["signon_offered"]) == (amount, False), text
    # a typo'd figure still shows "Offered" (Corewell 29387588)
    f = scraper.extract_posting_facts("Corewell Health is offering up to a $10,00 sign-on bonus for this position!\nRequires BLS.", "full_time")
    assert (f["signon"], f["signon_offered"]) == (None, True)


def test_signon_amount_from_a_labelled_bonus_amount_line():
    # Prime 26512036
    prime = ("Bonus Amount\n$15,000.00\n\nBonus Information\nSign-On Bonus Available for Qualified Candidates\n\n"
             "Job Summary\nThe Registered Nurse provides direct patient care. Requires BLS.")
    f = scraper.extract_posting_facts(prime, "full_time")
    assert (f["signon"], f["signon_offered"]) == (15000, False)
    f = scraper.extract_posting_facts("Sign-On Bonus Amount: $5,000\nWe offer a sign-on bonus to new nurses. Requires BLS.", "full_time")
    assert f["signon"] == 5000
    # a bonus amount with no sign-on offer in the posting is not a sign-on bonus
    f = scraper.extract_posting_facts("Bonus Amount\n$15,000.00\n\nRetention bonus paid after two years of service. Requires BLS.", "full_time")
    assert (f["signon"], f["signon_offered"]) == (None, False)
    # Northwestern's conditional boilerplate still shows nothing, so the labelled figure is not read
    f = scraper.extract_posting_facts("Bonus Amount: $5,000\n\nBenefits: medical, dental and vision.\n\nIf sign-on bonus is included in a job "
                                      "posting, current employees are not eligible for the sign-on bonus. Requires BLS.", "full_time")
    assert (f["signon"], f["signon_offered"]) == (None, False)


# ── item 6c: Oracle flex "Shift" / "Job Shift" and "Assignment Category" ────

def test_oracle_flex_shift_and_assignment_category():
    integris, northwell = json.loads(_p10("oracle_flex_shift.json"))["items"]
    assert integris["JobShift"] is None and integris["JobSchedule"] is None
    assert scraper._oracle_flex(integris)["job shift"] == "Night Job" and scraper._oracle_flex(integris)["assignment category"] == "PRN"
    desc, sched, start = scraper._oracle_posting_text(integris)
    assert sched == "PRN" and scraper.derive_job_type("", sched) == "per_diem"
    assert "\n\nSchedule: PRN · Night shift" in desc
    f = scraper.extract_posting_facts(desc, "per_diem")
    assert [s[0] for s in f["shift"]] == ["Nights", "PRN"]
    assert northwell["JobShift"] is None and scraper._oracle_flex(northwell)["shift"] == "Nights"
    desc, sched, start = scraper._oracle_posting_text(northwell)
    assert sched == "Full time" and "\n\nSchedule: Full time · Nights shift" in desc
    assert scraper.extract_posting_facts(desc, "full_time")["shift"][0][0] == "Nights"
    # the requisition's own JobShift still outranks the flex field
    it = dict(northwell, JobShift="Shift1 - Day")
    d2 = scraper._oracle_posting_text(it)[0]
    assert "Shift1 - Day shift" in d2 and "Nights shift" not in d2
    # no flex shift: the line is as before
    it = dict(northwell, requisitionFlexFields={"items": [{"Prompt": "Schedule", "Value": "Full Time"}]})
    assert scraper._oracle_posting_text(it)[0].endswith("Schedule: Full time")
