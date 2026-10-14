# 2026-10-14 (owner: "we need to fix those now"): UnityPoint's job number on
# LiquidCompass. job_id is LiquidCompass's own id (52810916, also in the
# careers.unitypoint.org URL); the posting prints UnityPoint's number,
# "Job ID: 188593" in its attribute list, or on Meriter (Madison) postings the
# BrassRing number as the body's first line ("16903BR"). The v1/search result
# the crawl already reads carries it as ats_id. Fixtures
# (tests/fixtures/reqid_1014/liquidcompass/) are real 2026-10-14 snippets:
# three results of the search payload (the keys the adapter reads, bodies cut)
# and the rendered text of two job pages read in a browser.
import asyncio
import json
import re

import scraper

PRINTED = {"52810916": "188593", "52617561": "188154", "52810673": "16903BR"}


def _results(fixture_text):
    return json.loads(fixture_text("reqid_1014/liquidcompass/unitypoint_search.json"))["results"]


def test_results_carry_the_number_the_posting_prints(fixture_text):
    for r in _results(fixture_text):
        j = scraper._liquidcompass_job(r, "UnityPoint Health", "IA")
        assert j.req_id == PRINTED[j.job_id] == r["ats_id"]
        assert j.job_id == str(r["id"]) and j.url == r["detail_url"]        # unchanged
        assert j.req_id != j.job_id
        # the body the payload carries prints the same number
        body = re.sub(r"<[^>]+>", " ", r["description"])
        assert re.search(r"(Job ID:\s*|^\s*)" + re.escape(j.req_id) + r"\b", body)
        # data.originalObject.id agrees; cp_up_job_id is not the printed number
        assert str(r["data"]["originalObject"]["id"]) == r["ats_id"]
    assert _results(fixture_text)[0]["cp_up_job_id"] == "2026-188593"


def test_rendered_pages_print_it(fixture_text):
    assert "Job ID: 188593" in fixture_text("reqid_1014/liquidcompass/rendered_52810916.txt")
    assert "Job Description 16903BR" in fixture_text("reqid_1014/liquidcompass/rendered_52810673.txt")


def test_odd_ats_id_gives_blank():
    f = scraper.liquidcompass_req_id
    assert f({"ats_id": "16903BR"}) == "16903BR" and f({"ats_id": 188593}) == "188593"
    assert f({"ats_id": ""}) == "" and f({"ats_id": None}) == "" and f({}) == "" and f(None) == ""
    assert f({"ats_id": "see posting"}) == "" and f({"ats_id": "1" * 30}) == ""


class _Resp:
    status_code = 200

    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def test_scrape_sets_it_and_normalize_sends_it(fixture_text, monkeypatch):
    payload = json.loads(fixture_text("reqid_1014/liquidcompass/unitypoint_search.json"))
    payload = dict(payload, page_count=1)
    monkeypatch.setattr(scraper, "_curl_fetch", lambda *a, **k: _Resp(payload))

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    jobs = asyncio.run(scraper.scrape_liquidcompass(None, "UnityPoint Health", ("unitypoint", "IA")))
    assert {j.job_id: j.req_id for j in jobs} == PRINTED
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    for j in jobs:
        d = scraper.normalize_job(j)
        assert "req_id" not in d
        # the payload body is short of DETAIL_MIN_CHARS here: whether facts are
        # read from it or not, the number goes out
        assert d["posting_facts"]["req_id"] == PRINTED[j.job_id]


def test_a_result_with_no_facts_still_sends_the_number(fixture_text, monkeypatch):
    """A search result whose body gives no facts tonight (no description, or
    a teaser such as a Meriter body cut to its first line) has null facts.
    LiquidCompass is on REQ_ID_ONLY_PLATFORMS, so the {"req_id"}-only object
    goes out with REQ_ID_ONLY_FACTS off; the live trigger merges it into the
    stored facts. Without the platform on that set the row would send null
    (the second half), so dropping it from the set at a merge fails here."""
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)
    scraper.set_known_bodies([])
    by_id = {str(r["id"]): r for r in _results(fixture_text)}
    rows = []
    for jid, desc in (("52810916", ""), ("52810673", "16903BR")):
        j = scraper._liquidcompass_job(dict(by_id[jid], description=desc), "UnityPoint Health", "IA")
        assert scraper.posting_facts_for(j.description, j.job_type, j.title) is None
        d = scraper.normalize_job(j)
        assert d["posting_facts"] == {"req_id": PRINTED[jid]}
        assert "req_id" not in d and d["job_id"] == jid and d["url"] == by_id[jid]["detail_url"]
        rows.append(j)
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_PLATFORMS",
                        scraper.REQ_ID_ONLY_PLATFORMS - {"LiquidCompass"})
    for j in rows:
        j.description = "" if j.job_id == "52810916" else "16903BR"
        assert scraper.normalize_job(j)["posting_facts"] is None
