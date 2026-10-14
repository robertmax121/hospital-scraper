# 2026-10-14 (review of the req-id builder, finding 1): a Jobvite Engage job
# number is printed only on the job page, read in the detail pass that already
# fetches it. Jobvite is not a known-body platform, so every row is a
# candidate every night, transparency states first, and the 1,500 fetches went
# to the same priority rows night after night (Penn NJ 203 of 216 held a body
# on 10-14, Penn PA 391 of 1,555; PeaceHealth WA 404 of 422, OR/AK 76 of 378).
# Now a row whose stored row already holds a body and a req_id (the known-body
# load's rq, posting_facts->>'req_id') goes to the back of the line; nothing is
# skipped and no fetch is added. Fixtures: the real 2026-10-14 PeaceHealth
# search.json entries (one WA, one OR) and their job-page header excerpts.
import asyncio
import dataclasses
import io
import json

import pytest

import scraper

D = "reqid_1014/jobvite"
BASE = "https://careers.peacehealth.org"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


def _peacehealth(fixture_text):
    """The two real PeaceHealth rows: (WA 18360469, OR 18359324)."""
    entries = json.loads(fixture_text(f"{D}/peacehealth_search.json"))["entries"]
    wa, or_ = (scraper._talemetry_job(e, "PeaceHealth", BASE) for e in entries)
    assert (wa.state, or_.state) == ("WA", "OR") and wa.ats_platform == "Jobvite"
    return wa, or_


def _stored(job, n=3000, rq=None):
    """A known-body load row for `job`, as load_known_bodies returns it."""
    return {"hospital_system": scraper._canon_system(job.hospital_system), "job_id": job.job_id,
            "ats_platform": job.ats_platform, "desc_len": n, "wage_min": None,
            "fv": scraper.FACTS_VERSION, "fh": "0123456789ab", "rq": rq}


def _copy(job, jid):
    return dataclasses.replace(job, job_id=jid, url=f"{BASE}/jobs/{jid}-x")


def test_load_reads_the_stored_number(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "k")
    rows = [
        {"id": 1, "hospital_system": "PeaceHealth", "job_id": "18360469", "ats_platform": "Jobvite",
         "desc_len": 2400, "rq": "143880"},
        {"id": 2, "hospital_system": "PeaceHealth", "job_id": "18359324", "ats_platform": "Jobvite",
         "desc_len": 2400, "rq": None},
        {"id": 3, "hospital_system": "Penn Medicine", "job_id": "18360735", "ats_platform": "Jobvite",
         "desc_len": 150, "rq": "347741"},                 # not a body: not loaded as one
        {"id": 4, "hospital_system": "Legacy Health", "job_id": "49582", "ats_platform": "iCIMS",
         "desc_len": 2400, "rq": "26-49582"},              # its number comes from the list card
    ]
    urls = []

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(rq, timeout=0):
        urls.append(rq.full_url)
        return _Resp(json.dumps(rows if len(urls) == 1 else []).encode())

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    scraper.load_known_bodies()
    assert all("rq:posting_facts-%3E%3Ereq_id" in u and "fh:posting_facts-%3E%3Eh" in u for u in urls)
    assert scraper._KNOWN_HAS_REQ == {("PeaceHealth", "18360469")}
    # Jobvite stays off the known-body map: every row is still a candidate
    assert ("PeaceHealth", "18360469") not in scraper._KNOWN_BODIES
    assert ("Legacy Health", "49582") in scraper._KNOWN_BODIES


def test_rows_with_a_body_and_a_number_go_last(fixture_text):
    wa_done, or_new = _peacehealth(fixture_text)
    wa_no_req = _copy(wa_done, "18360001")      # WA, body stored, no number yet
    wa_new = _copy(wa_done, "18360002")         # WA, nothing stored
    or_no_req = _copy(or_new, "18359001")       # OR, body stored, no number yet
    rows = [wa_done, or_new, wa_no_req, wa_new, or_no_req]
    scraper.set_known_bodies([_stored(wa_done, rq="143880"), _stored(wa_no_req), _stored(or_no_req)])
    held = []
    cands, known, dup = scraper._detail_candidates("PeaceHealth", rows, scraper._DescBudget(1500), held=held)
    # nothing skipped, nothing held: the stored row is fetched last, not dropped
    assert (known, dup, held) == (0, 0, [])
    assert len(cands) == 5 and cands[-1] is wa_done
    # the rest keep the old order: transparency state (WA) first, then OR
    assert {c.job_id for c in cands[:2]} == {"18360001", "18360002"}
    assert {c.job_id for c in cands[2:4]} == {"18359324", "18359001"}
    # the list's copy is untouched (Jobvite sends "" and the trigger keeps the body)
    assert all(c.description == "" for c in cands)


def test_without_a_stored_number_the_order_is_unchanged(fixture_text):
    wa, or_ = _peacehealth(fixture_text)
    scraper.set_known_bodies([_stored(wa)])      # body, no number: as every row was on 10-14
    for _ in range(5):
        cands, _, _ = scraper._detail_candidates("PeaceHealth", [or_, wa], scraper._DescBudget(1500), held=[])
        assert cands == [wa, or_]


def test_other_platforms_ignore_the_stored_number(fixture_text):
    wa, or_ = _peacehealth(fixture_text)
    wa, or_ = (dataclasses.replace(j, ats_platform="Talemetry") for j in (wa, or_))
    scraper.set_known_bodies([dict(_stored(wa, rq="143880"), ats_platform="Jobvite")])
    # a row of another platform under the same key is never sent back
    assert ("PeaceHealth", wa.job_id) in scraper._KNOWN_HAS_REQ
    cands, _, _ = scraper._detail_candidates("PeaceHealth", [or_, wa], scraper._DescBudget(1500), held=[])
    assert cands == [wa, or_]


def test_the_night_spends_its_fetches_on_rows_missing_a_number(fixture_text, monkeypatch):
    """Three fetches for a tenant whose WA rows already hold a body and a
    number: they go to the OR rows, which then send theirs; before, the WA
    rows (transparency state) took them all again."""
    wa, or_ = _peacehealth(fixture_text)
    pages = {or_.url: fixture_text(f"{D}/peacehealth_job_18359324.html"),
             wa.url: fixture_text(f"{D}/peacehealth_job_18360469.html")}
    or_rows = [or_] + [_copy(or_, f"1835900{i}") for i in range(2)]
    wa_rows = [wa] + [_copy(wa, f"1836000{i}") for i in range(3)]
    for j in or_rows[1:] + wa_rows[1:]:
        pages[j.url] = pages[or_.url if j.state == "OR" else wa.url].replace(
            "142243" if j.state == "OR" else "143880", "9" + j.job_id[-4:])
    fetched = []

    async def fake_curl(url, imp=None, *a, **k):
        fetched.append(url)
        return pages[url]

    monkeypatch.setattr(scraper, "_curl_html", fake_curl)

    def night(stored):
        fetched.clear()
        jobs = [dataclasses.replace(j) for j in wa_rows + or_rows]
        scraper.set_known_bodies(stored)
        asyncio.run(scraper._detail_pass(None, "PeaceHealth", jobs, scraper._DescBudget(1500),
                                         lambda j: scraper._talemetry_detail(None, j), "Talemetry", share=3))
        return jobs

    # 10-14: WA bodies stored, no number anywhere: the three fetches go to WA
    night([_stored(j) for j in wa_rows])
    assert len(fetched) == 3 and all(u in {j.url for j in wa_rows} for u in fetched)
    # once the WA rows hold their number, the same three fetches reach OR
    jobs = night([_stored(j, rq="x" + j.job_id[-4:]) for j in wa_rows])
    assert sorted(fetched) == sorted(j.url for j in or_rows)
    sent = {j.job_id: scraper.normalize_job(j)["posting_facts"] for j in jobs if j.state == "OR"}
    assert sent["18359324"] == {"req_id": "142243"}          # the number the live page printed
    assert sent["18359000"] == {"req_id": "99000"} and sent["18359001"] == {"req_id": "99001"}
    # job_id and url are untouched
    assert {j.job_id for j in jobs} == {j.job_id for j in wa_rows + or_rows}
