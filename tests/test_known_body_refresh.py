"""A known body is not known forever (2026-09-24, review of push 2): stored
bodies built with older facts rules, bodies cut at the old 8,000 cap and a
small nightly share of the rest are read again; posting_facts carry the rules
version; title-only facts never ride on a teaser. No network, no database."""
import asyncio
import io
import json

import pytest

import scraper
from scraper import Job

# the facts version a stored row carries: current rules, and the rules before them
CUR, OLD = str(scraper.FACTS_VERSION), str(scraper.FACTS_VERSION - 1)

TEASER = "Registered Nurse RN NIGHTS. Currently licensed to practice nursing. " * 5     # 339 characters once stripped


def _job(i, system="VITAS Healthcare", desc="", title=None):
    return Job(title=title or f"RN {i}", hospital_system=system, hospital_name=system, city="", state="",
               location="", specialty="", job_type="", url=f"https://x/{system}/job/{i}",
               job_id=str(i), posted_date="", description=desc, ats_platform="Oracle HCM")


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper.random, "uniform", lambda a, b: 0.0)
    real_sleep = asyncio.sleep

    async def no_sleep(*a, **k):
        await real_sleep(0)
    monkeypatch.setattr(scraper.asyncio, "sleep", no_sleep)
    yield
    scraper.set_known_bodies([])


def _known(rows):
    scraper.set_known_bodies([{"hospital_system": s, "job_id": j, "desc_len": n, "fv": v} for s, j, n, v in rows])


def test_known_kind():
    _known([("VITAS Healthcare", "1", 8000, None),     # cut at the old cap, never re-read
            ("VITAS Healthcare", "2", 8000, CUR),      # read under the 12,000 cap: a real 8,000-character body
            ("VITAS Healthcare", "3", 7995, None),     # the sanitizer shaved a few characters off the cut
            ("VITAS Healthcare", "4", 3000, OLD),      # facts from older rules
            ("VITAS Healthcare", "5", 3000, CUR),
            ("VITAS Healthcare", "6", 3000, None),     # unstamped (null facts, or facts from before the stamp)
            ("VITAS Healthcare", "7", 300, None),      # teaser: the list sends as much again
            ("VITAS Healthcare", "8", 900, OLD)])      # a detail body under 1,500 over a 339 teaser: never "stale"
    kinds = {i: scraper._known_kind("VITAS Healthcare", _job(i, desc=TEASER if i in (7, 8) else ""))
             for i in range(1, 10)}
    assert kinds == {1: "cut", 2: "known", 3: "cut", 4: "stale", 5: "known", 6: "known", 7: None, 8: "known", 9: None}
    assert scraper._KNOWN_FACTS_V[("VITAS Healthcare", "4")] == scraper.FACTS_VERSION - 1


def test_frozen_repro_old_cap_row_is_fetched_again_and_settled(monkeypatch):
    """The review's frozen.py: a row stored at 8,000 used to give candidates 0,
    known 1, and was never read again."""
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    _known([("VITAS Healthcare", "40788", 8000, None)])
    j = _job(40788, desc=TEASER, title="Registered Nurse RN NIGHTS")
    new = _job(1)
    held = []
    cands, known, dup = scraper._detail_candidates("VITAS Healthcare", [j, new], scraper._DescBudget(100), held=held)
    assert [c.job_id for c in cands] == ["1", "40788"]        # the row with no body first
    assert known == 0 and dup == 0
    assert j.description == TEASER                             # kept until the fetch has had its chance
    assert [(h[0].job_id, h[2]) for h in held] == [("40788", "cut")]
    scraper._settle_held("VITAS Healthcare", held)             # no fetch refilled it
    assert j.description == ""                                 # the trigger keeps the stored body and facts
    # Without `held` (a caller that cannot settle) the old rule stands.
    j2 = _job(40788, desc=TEASER)
    cands, known, _ = scraper._detail_candidates("VITAS Healthcare", [j2], scraper._DescBudget(100))
    assert cands == [] and known == 1 and j2.description == ""


def test_refresh_share_is_capped_and_goes_first(monkeypatch):
    monkeypatch.setattr(scraper, "_refresh_slot", lambda canon, jid: True)
    rows = [("Tenet", str(i), 3000, CUR) for i in range(100)] + [("Tenet", str(i), 3000, OLD) for i in range(100, 103)]
    _known(rows)
    jobs = [_job(i, "Tenet", desc=TEASER) for i in range(110)]
    b = scraper._DescBudget(4000)
    b.expect(["Tenet", "A", "B", "C"])                         # floor 1,000 -> 5% = 50
    held = []
    cands, known, dup = scraper._detail_candidates("Tenet", jobs, b, held=held)
    assert scraper._refresh_quota(b) == 50
    refresh = cands[:50]
    assert {c.job_id for c in refresh[:3]} == {"100", "101", "102"}     # older facts first
    assert all(int(c.job_id) < 103 for c in refresh)
    assert sorted(int(c.job_id) for c in cands[50:]) == list(range(103, 110))   # then the rows with no body
    assert known == 53 and dup == 0
    assert sum(1 for c in jobs[:103] if c.description == "") == 53           # the rest keep the stored body
    assert len(held) == 50 and sum(1 for h in held if h[2] == "stale") == 3
    assert all(h[0].description == TEASER for h in held)


def test_refresh_share_off_and_slot_off(monkeypatch):
    _known([("Tenet", str(i), 3000, CUR) for i in range(20)])
    jobs = [_job(i, "Tenet") for i in range(20)]
    monkeypatch.setattr(scraper, "_refresh_slot", lambda canon, jid: False)
    held = []
    cands, known, _ = scraper._detail_candidates("Tenet", jobs, scraper._DescBudget(1000), held=held)
    assert cands == [] and known == 20 and held == []
    monkeypatch.setattr(scraper, "_refresh_slot", lambda canon, jid: True)
    monkeypatch.setattr(scraper, "DETAIL_REFRESH_PCT", 0)
    cands, known, _ = scraper._detail_candidates("Tenet", jobs, scraper._DescBudget(1000), held=held)
    assert cands == [] and known == 20 and held == []


def test_slot_visits_every_posting_once_a_cycle(monkeypatch):
    days = scraper.DETAIL_REFRESH_DAYS
    hits = {str(i): 0 for i in range(300)}
    for d in range(days):
        monkeypatch.setattr(scraper, "_run_day", lambda d=d: 739000 + d)
        for jid in hits:
            hits[jid] += scraper._refresh_slot("Tenet", jid)
    assert set(hits.values()) == {1}


def test_detail_pass_settles_what_the_fetch_did_not_refill(monkeypatch):
    monkeypatch.setattr(scraper, "_refresh_slot", lambda canon, jid: True)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    _known([("Mayo Clinic", "1", 3000, CUR), ("Mayo Clinic", "2", 3000, CUR), ("Mayo Clinic", "3", 8000, None)])
    jobs = [_job(i, "Mayo Clinic", desc=TEASER) for i in (1, 2, 3, 4)]
    body = "Qualifications: Current RN license. BLS required. " * 40

    async def fetch(job):
        if job.job_id in ("1", "4"):
            job.description = body
            return True
        return False                                           # 2 and 3: the detail endpoint failed
    b = scraper._DescBudget(400)                               # unregistered: cap 100, quota 5
    asyncio.run(scraper._detail_pass(None, "Mayo Clinic", jobs, b, fetch, "Oracle"))
    got = {j.job_id: j.description for j in jobs}
    assert got["1"] == body and got["4"] == body               # an edited body lands
    assert got["2"] == "" and got["3"] == ""                   # the stored bodies stand


def test_posting_facts_for_stamps_and_guards():
    full = ("QUALIFICATIONS\nCurrently licensed to practice nursing in the state.\n"
            "A minimum of two years of nursing experience.\n") + ("We care for patients at home. " * 60)
    f = scraper.posting_facts_for(full, "Full time", "Registered Nurse RN NIGHTS")
    assert f["v"] == scraper.FACTS_VERSION and f["certs"][0][0] == "RN license" and f["experience"][0] == "2+ years"
    assert f["shift"][0][0] == "Nights"
    # A full body that states nothing: a stamp, so older chips do not survive.
    assert scraper.posting_facts_for("We care for patients at home. " * 60, None, "Clerk") == {"v": scraper.FACTS_VERSION}
    # A teaser whose only fact is the title's shift: null, as before push2/facts.
    thin = "Join our team of caring professionals serving hospice patients in the community. " * 3
    assert scraper.extract_posting_facts(thin, "Full time", "Registered Nurse - Med Surg - Nights") is not None
    assert scraper.posting_facts_for(thin, "Full time", "Registered Nurse - Med Surg - Nights") is None
    # A teaser that states a fact keeps the title's shift with it.
    g = scraper.posting_facts_for(TEASER, "Full time", "Registered Nurse RN NIGHTS")
    assert g["certs"] and g["shift"][0][0] == "Nights" and g["v"] == scraper.FACTS_VERSION
    assert scraper.posting_facts_for("", None, "RN NIGHTS") is None
    d = scraper.normalize_job(_job(1, desc=full, title="Registered Nurse RN NIGHTS"))
    assert d["posting_facts"]["v"] == scraper.FACTS_VERSION


def test_load_known_bodies_reads_the_stamp_and_retries(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
    pages = [[{"id": 5, "hospital_system": "VITAS Healthcare", "job_id": "40788", "desc_len": 2712, "fv": "2"}],
             [{"id": 7, "hospital_system": "Mayo Clinic", "job_id": "9", "desc_len": 8000, "fv": None}],
             []]
    calls = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def flaky_urlopen(rq, timeout=0):
        calls.append(rq.full_url)
        if len(calls) == 2:
            raise TimeoutError("statement timeout")            # page two fails once
        served = len(calls) - 1 - (len(calls) > 2)
        return _Resp(json.dumps(pages[served]).encode())

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", flaky_urlopen)
    assert scraper.load_known_bodies() == 2
    assert "fv:posting_facts-%3E%3Ev" in calls[0]
    assert calls[1] == calls[2]                                # the failed page was asked again
    assert scraper._KNOWN_FACTS_V == {("VITAS Healthcare", "40788"): 2, ("Mayo Clinic", "9"): None}
