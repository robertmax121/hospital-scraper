# 2026-10-14 (owner: "Yeah we need to fix those now"): the job number the
# employer shows people, for four platforms whose job_id is another number.
#   HealthcareSource  job_id = userArea.jobPostingID (136024); the page prints
#                     "Req # 0000264102" = userArea.requisitionNumber
#   UKG               job_id = the opportunity GUID; the page prints
#                     "Requisition Number: BEHAV042110" = RequisitionNumber
#   ADP WorkforceNow  job_id = itemID (9201508809155_1); the page prints
#                     "Requisition ID: 9218" = clientRequisitionID
#   ADP CX            job_id = reqId (5001231190600); the page prints
#                     "#26-2694" = clientRequisitionID
# Each comes from the list payload the adapter already fetches, travels as
# Job.req_id -> posting_facts["req_id"] (normalize_job), and job_id, url and
# the (job_id, hospital_system) upsert key stay as they were. A row of these
# platforms whose facts are null tonight sends {"req_id": ...}
# (REQ_ID_LIST_PLATFORMS), which the live enrichment trigger merges into the
# stored facts. Fixtures are real 2026-10-14 snippets
# (tests/fixtures/reqid_1014/), the audit's own example postings among them.
import asyncio
import dataclasses
import json
import os
import re

import pytest

import scraper

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "reqid_1014")


def _json(rel):
    with open(os.path.join(FIX, rel), encoding="utf-8") as f:
        return json.load(f)


def _text(rel):
    with open(os.path.join(FIX, rel), encoding="utf-8") as f:
        return f.read()


class _R:
    def __init__(self, body, status=200):
        self.status, self._body = status, body

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return json.dumps(self._body)


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    async def no_wait(*_a):
        return None
    monkeypatch.setattr(scraper, "jitter", no_wait)
    monkeypatch.setattr(scraper, "REQ_ID_ONLY_FACTS", False)   # the global switch stays off
    scraper.set_known_bodies([])
    yield
    scraper.set_known_bodies([])


def _serve(monkeypatch, answer):
    """scraper.req -> answer(method, url) as the JSON body."""
    calls = []

    def fake_req(session, method, url, **kw):
        calls.append((method, url))
        return _Ctx(_R(answer(method, url)))
    monkeypatch.setattr(scraper, "req", fake_req)
    return calls


# ── HealthcareSource ────────────────────────────────────────────────────────
def _hcs(monkeypatch, rel, system):
    page = _json(rel)
    calls = _serve(monkeypatch, lambda m, u: page)
    jobs = asyncio.run(scraper.scrape_healthcaresource(None, system, scraper.HEALTHCARESOURCE_ORGS[system]))
    assert len(calls) == 1                        # the one search request the adapter always made
    return jobs


def test_hcs_rows_carry_the_req_number_the_page_prints(monkeypatch):
    jobs = _hcs(monkeypatch, "healthcaresource/rwjbarnabas_search.json", "RWJBarnabas Health")
    # leading zeros kept exactly as printed
    assert [(j.job_id, j.req_id) for j in jobs] == [("136024", "0000264102"), ("129476", "0000247083"),
                                                    ("138291", "48903")]
    ex = jobs[0]
    assert ex.url == "https://pm.healthcaresource.com/cs/rwjbarnabashealth_i#/job/136024"
    assert ex.ats_platform == "HealthcareSource" and ex.ats_platform in scraper.REQ_ID_LIST_PLATFORMS
    jobs = _hcs(monkeypatch, "healthcaresource/lawrence_search.json", "Lawrence General")
    assert [(j.job_id, j.req_id) for j in jobs] == [("16735", "31844"), ("16736", "31845"), ("16737", "31849")]


def test_hcs_req_number_is_requisitionNumber_not_requisitionID_or_jobCode():
    hit = _json("healthcaresource/rwjbarnabas_search.json")["hits"]["hits"][0]
    ua = hit["_source"]["userArea"]
    assert (ua["jobPostingID"], ua["requisitionNumber"], ua["requisitionID"], ua["jobCode"]) == \
        (136024, "0000264102", 146778, "H0327")
    assert scraper._hcs_job(hit, "RWJBarnabas Health", "rwjbarnabashealth_i").req_id == "0000264102"


def test_hcs_job_page_data_and_display_config_agree(monkeypatch):
    # the job page's own data call (JobPostingV2) returns the same number,
    # and the SPA shows it ("Req # ") when requisitionNumber.display is true
    page = _json("healthcaresource/lawrence_jobpostingv2_16737.json")
    assert page["userArea"]["jobPostingID"] == 16737 and page["userArea"]["requisitionNumber"] == "31849"
    jobs = _hcs(monkeypatch, "healthcaresource/lawrence_search.json", "Lawrence General")
    assert next(j for j in jobs if j.job_id == "16737").req_id == page["userArea"]["requisitionNumber"]
    cfg = _json("healthcaresource/jobdetail_config_default.json")
    assert cfg["jobDetail"]["requisitionNumber"]["display"] is True


def test_hcs_odd_values_keep_their_printed_form_and_labels_are_skipped():
    hits = _json("healthcaresource/odd_values_search.json")["hits"]["hits"]
    got = {}
    for h, tenant in zip(hits, ("comhs", "unionhospital", "renownhealth", "unionhospital")):
        j = scraper._hcs_job(h, "X", tenant)
        got[j.job_id] = (h["_source"]["userArea"]["requisitionNumber"], j.req_id)
    assert got == {
        "40287": ("50055.1", "50055.1"),            # Powers Health prints a dot
        "16271": ("RAD OO1", "RAD OO1"),            # Union Hospital prints a space
        "42398": ("Critical Care", ""),             # Renown evergreen label, no digit
        "13993": ("PH-OT", "PH-OT"),                # Union Hospital prints a code (review 10-14)
    }
    # the shared validator other builders use is unchanged (and would drop them)
    assert scraper._hca_req_value("50055.1") == "" and scraper._hca_req_value("RAD OO1") == ""


@pytest.mark.parametrize("raw,want", [
    ("0000264102", "0000264102"),
    (" 31844 ", "31844"),
    ("RAD  OO1", "RAD OO1"),
    ("47834.1", "47834.1"),
    (31844, "31844"),
    ("", ""),
    (None, ""),
    ("Critical Care", ""),
    ("ExpRN-W&C", ""),
    ("https://pm.healthcaresource.com/1", ""),
    ("1" * 41, ""),
    ("Req <b>1</b>", ""),
])
def test_hcs_req_value(raw, want):
    assert scraper._hcs_req_value(raw) == want


# 2026-10-14 (review): Union Hospital prints a digit-less requisition code on
# 16 of 287 postings ("Req # PH-OT", "Req # UHTHRTOPTIII"; rendered pages read
# 2026-10-14). Kept for that tenant only; Renown's labels and every other
# tenant are unchanged.
@pytest.mark.parametrize("raw,tenant,want", [
    ("PH-OT", "unionhospital", "PH-OT"),
    ("UHTHRTOPTIII", "unionhospital", "UHTHRTOPTIII"),
    ("APP-GC", "unionhospital", "APP-GC"),
    ("PH-PTTHPTORFT", "unionhospital", "PH-PTTHPTORFT"),
    ("EMRACUTECLINICALDOCANALYST", "unionhospital", "EMRACUTECLINICALDOCANALYST"),
    ("30722", "unionhospital", "30722"),
    ("RAD OO1", "unionhospital", "RAD OO1"),
    ("PH-OT", "", ""),                              # default: digit rule as before
    ("PH-OT", "renownhealth", ""),
    ("PICURN", "renownhealth", ""),                 # Renown's labels stay skipped
    ("ICU-RN", "renownhealth", ""),
    ("Critical Care", "unionhospital", ""),         # not an all-capitals code
    ("ExpRN-W&C", "unionhospital", ""),
    ("OP CP", "unionhospital", ""),
    ("ER-RN-", "unionhospital", ""),
    ("ABC", "unionhospital", ""),                   # too short
    ("NONE", "unionhospital", ""),                  # placeholders are not numbers
    ("EVERGREEN", "unionhospital", ""),
    ("A" * 41, "unionhospital", ""),
    ("", "unionhospital", ""),
    (None, "unionhospital", ""),
])
def test_hcs_req_value_union_codes(raw, tenant, want):
    assert scraper._hcs_req_value(raw, tenant) == want


def test_union_rows_carry_the_code_the_page_prints_through_normalize_job(monkeypatch):
    jobs = _hcs(monkeypatch, "healthcaresource/union_codes_search.json", "Union Hospital")
    assert [(j.job_id, j.req_id) for j in jobs] == [
        ("24214", "UHTHRTOPTIII"), ("23359", "EMRACUTECLINICALDOCANALYST"), ("25059", "30722")]
    ex = jobs[0]
    assert ex.url == "https://pm.healthcaresource.com/cs/unionhospital#/job/24214"
    # the 16 rows have a stored body and facts built without the number:
    # req_id joins the facts hash, so the first run rebuilds them with it
    old_h = scraper.normalize_job(dataclasses.replace(ex, req_id=""))["posting_facts"]["h"]
    scraper.set_known_bodies([_stored(ex, old_h)])
    row = scraper.normalize_job(ex)
    assert row["job_id"] == "24214" and row["posting_facts"]["req_id"] == "UHTHRTOPTIII"
    assert row["posting_facts"]["h"] != old_h
    # a night whose facts are null still sends the code alone
    blank = dataclasses.replace(ex, description="")
    scraper.set_known_bodies([])
    assert scraper.normalize_job(blank)["posting_facts"] == {"req_id": "UHTHRTOPTIII"}


# ── UKG ─────────────────────────────────────────────────────────────────────
def _ukg(monkeypatch, rel, system):
    page = _json(rel)
    calls = _serve(monkeypatch, lambda m, u: page)
    jobs = asyncio.run(scraper.scrape_ukg(None, system, scraper.UKG_ORGS[system]))
    assert len(calls) == 1
    return jobs


def test_ukg_rows_carry_the_requisition_number(monkeypatch):
    jobs = _ukg(monkeypatch, "ukg/nhc_search.json", "NHC")
    assert sorted((j.job_id, j.req_id) for j in jobs) == [
        ("83e87c9c-6384-4c38-8bb2-d5c52772961a", "BEHAV042426"),
        ("e6efa8de-a057-460e-b718-01fe8a72bb58", "BEHAV042110")]
    ex = next(j for j in jobs if j.job_id.startswith("e6efa8de"))
    assert ex.url == ("https://recruiting2.ultipro.com/NAT1059NHTH/JobBoard/02b4dc60-27be-428b-aa7b-4f7b89a29f7a/"
                      "OpportunityDetail?opportunityId=e6efa8de-a057-460e-b718-01fe8a72bb58")
    assert ex.ats_platform == "UKG" and ex.ats_platform in scraper.REQ_ID_LIST_PLATFORMS
    jobs = _ukg(monkeypatch, "ukg/deaconess_search.json", "Deaconess Health")
    assert sorted((j.job_id, j.req_id) for j in jobs) == [
        ("14763732-4238-438c-b6ac-1c511e1c60f2", "PHYSI020776"),
        ("ab0da9df-98e7-4d1e-bca1-00a20bc5344d", "FINAN020889")]


def test_ukg_detail_page_prints_the_same_number():
    html = _text("ukg/nhc_detail_e6efa8de.html")
    # the page's "Requisition Number:" line binds the object's RequisitionNumber
    assert 'data-bind="text: RequisitionNumber()" data-automation="requisition-number"' in html
    d = scraper._ukg_detail_data(html)
    item = next(o for o in _json("ukg/nhc_search.json")["opportunities"] if o["Id"] == d["Id"])
    assert d["RequisitionNumber"] == item["RequisitionNumber"] == "BEHAV042110"


# ── ADP WorkforceNow ────────────────────────────────────────────────────────
def _adp(monkeypatch, rel, cid, state):
    page = _json(rel)
    calls = _serve(monkeypatch, lambda m, u: page)
    jobs = asyncio.run(scraper.scrape_adp(None, "Test ADP", (cid, "19000101_000001", state)))
    assert len(calls) == 1
    return jobs


def test_adp_rows_carry_the_requisition_id(monkeypatch):
    jobs = _adp(monkeypatch, "adp/kpc_requisitions.json", "a45074b6-b45e-424e-9e71-c45a4d73c34b", "CA")
    assert sorted((j.job_id, j.req_id) for j in jobs) == [("9201508809155_1", "9218"), ("9201508956568_1", "9219")]
    ex = next(j for j in jobs if j.job_id == "9201508809155_1")
    assert ex.url.endswith("&selectedMenuKey=CareerCenter&jobId=9201508809155_1")
    assert ex.ats_platform == "ADP" and ex.ats_platform in scraper.REQ_ID_LIST_PLATFORMS
    jobs = _adp(monkeypatch, "adp/larkin_requisitions.json", "5ffc5741-7db3-4aa8-a16a-e19abed9677e", "FL")
    assert [(j.job_id, j.req_id) for j in jobs] == [("9201252622316_1", "4664")]


def test_adp_detail_call_returns_the_same_number():
    d = _json("adp/kpc_detail_9201508809155_1.json")
    assert (d["itemID"], d["clientRequisitionID"]) == ("9201508809155_1", "9218")


# ── ADP CX ──────────────────────────────────────────────────────────────────
def _adpcx(monkeypatch, rel, system):
    page = _json(rel)

    def answer(method, url):
        if "/career-site/" in url:
            return {"orgoid": "test-orgoid", "myJobsToken": "test-token"}
        return page
    calls = _serve(monkeypatch, answer)
    jobs = asyncio.run(scraper.scrape_adp_cx(None, system, scraper.ADPCX_ORGS[system]))
    assert len(calls) == 2                        # the config and the one list page, as before
    return jobs


def test_adpcx_rows_carry_the_client_requisition_id(monkeypatch):
    jobs = _adpcx(monkeypatch, "adpcx/huntregional_requisitions.json", "Hunt Regional Healthcare")
    assert sorted((j.job_id, j.req_id) for j in jobs) == [("5001231190600", "26-2694"), ("5001231382806", "26-2695")]
    ex = next(j for j in jobs if j.job_id == "5001231190600")
    assert ex.url == "https://myjobs.adp.com/huntregional/cx/job-details?reqId=5001231190600"
    assert ex.ats_platform == "ADP CX" and ex.ats_platform in scraper.REQ_ID_LIST_PLATFORMS
    d = _json("adpcx/huntregional_detail_5001231190600.json")["jobRequisitions"][0]
    assert (str(d["reqId"]), d["clientRequisitionID"]) == ("5001231190600", ex.req_id)


def test_adpcx_cabell_rows(monkeypatch):
    jobs = _adpcx(monkeypatch, "adpcx/mhnetwork_requisitions.json", "Cabell Huntington Hospital")
    assert sorted((j.job_id, j.req_id) for j in jobs) == [("5001231559700", "16287"), ("5001231580000", "16288")]


# ── normalize_job: every active row gets the number on the next run ────────
def _hcs_job(rel, job_id, system="RWJBarnabas Health"):
    for h in _json(rel)["hits"]["hits"]:
        j = scraper._hcs_job(h, system, scraper.HEALTHCARESOURCE_ORGS.get(system, "x"))
        if j and j.job_id == job_id:
            return j
    raise KeyError(job_id)


def _stored(job, fh, desc_len=None):
    """A known-body row as load_known_bodies hands it to set_known_bodies."""
    return {"hospital_system": job.hospital_system, "job_id": job.job_id, "ats_platform": job.ats_platform,
            "desc_len": len(job.description) if desc_len is None else desc_len, "wage_min": None,
            "fv": scraper.FACTS_VERSION, "fh": fh}


def test_a_row_with_a_body_sends_full_facts_with_req_id():
    job = _hcs_job("healthcaresource/rwjbarnabas_search.json", "136024")
    row = scraper.normalize_job(job)
    assert "req_id" not in row and row["job_id"] == "136024" and row["url"] == job.url
    pf = row["posting_facts"]
    assert pf["req_id"] == "0000264102" and pf["v"] == scraper.FACTS_VERSION and re.fullmatch(r"[0-9a-f]{12}", pf["h"])


def test_stored_facts_from_before_are_rebuilt_once_then_skipped():
    """The facts skip (push 10) cannot keep a stored object built without the
    number: req_id joins the hash, so the first run parses the body again."""
    job = _hcs_job("healthcaresource/rwjbarnabas_search.json", "136024")
    old_h = scraper.normalize_job(dataclasses.replace(job, req_id=""))["posting_facts"]["h"]
    scraper.set_known_bodies([_stored(job, old_h)])
    first = scraper.normalize_job(job)["posting_facts"]
    assert first is not None and first["req_id"] == "0000264102" and first["h"] != old_h
    scraper.set_known_bodies([_stored(job, first["h"])])
    scraper._FACTS_SKIPS["n"] = 0
    assert scraper.normalize_job(job)["posting_facts"] is None and scraper._FACTS_SKIPS["n"] == 1


def test_hcs_row_with_no_body_sends_a_req_id_only_object():
    hit = _json("healthcaresource/crmc_blank_body_search.json")["hits"]["hits"][0]
    job = scraper._hcs_job(hit, "CRMC Health", "crmchealth")
    assert job.description == "" and job.req_id == "7305"
    assert scraper.normalize_job(job)["posting_facts"] == {"req_id": "7305"}


def test_ukg_row_whose_stored_body_is_kept_sends_a_req_id_only_object(monkeypatch):
    """The detail pass skips a stored body and the list's teaser is dropped
    (_keep_stored_body), so the upsert sends a blank body and null facts: the
    number goes out alone and the trigger merges it."""
    jobs = _ukg(monkeypatch, "ukg/nhc_search.json", "NHC")
    job = next(j for j in jobs if j.job_id.startswith("e6efa8de"))
    scraper.set_known_bodies([_stored(job, "0123456789ab", desc_len=3200)])
    assert scraper._body_known(job.hospital_system, job) is True and job.description == ""
    assert scraper.normalize_job(job)["posting_facts"] == {"req_id": "BEHAV042110"}


def test_adp_list_row_sends_a_req_id_only_object(monkeypatch):
    jobs = _adp(monkeypatch, "adp/kpc_requisitions.json", "a45074b6-b45e-424e-9e71-c45a4d73c34b", "CA")
    job = next(j for j in jobs if j.job_id == "9201508809155_1")
    assert job.description == ""                  # the list has no body
    assert scraper.normalize_job(job)["posting_facts"] == {"req_id": "9218"}


def test_adpcx_row_sends_full_facts_with_req_id(monkeypatch):
    jobs = _adpcx(monkeypatch, "adpcx/huntregional_requisitions.json", "Hunt Regional Healthcare")
    job = next(j for j in jobs if j.job_id == "5001231190600")
    pf = scraper.normalize_job(job)["posting_facts"]
    assert pf["req_id"] == "26-2694" and "h" in pf and pf["v"] == scraper.FACTS_VERSION


@pytest.mark.parametrize("platform", ["HealthcareSource", "UKG", "ADP", "ADP CX"])
def test_every_list_platform_takes_the_req_only_path(platform):
    """Merge guard (review 10-14): the parallel req_id branches edit the same
    normalize_job line, so after integration each of these four platforms
    must still send {"req_id"} when its facts are null, with the global
    REQ_ID_ONLY_FACTS switch off."""
    assert platform in scraper.REQ_ID_LIST_PLATFORMS and scraper.REQ_ID_ONLY_FACTS is False
    hit = _json("healthcaresource/crmc_blank_body_search.json")["hits"]["hits"][0]
    job = dataclasses.replace(scraper._hcs_job(hit, "CRMC Health", "crmchealth"), ats_platform=platform)
    assert job.description == "" and job.req_id == "7305"
    assert scraper.normalize_job(job)["posting_facts"] == {"req_id": "7305"}


def test_rows_without_a_number_and_other_platforms_are_unchanged():
    hit = _json("healthcaresource/odd_values_search.json")["hits"]["hits"][2]     # Renown "Critical Care"
    job = dataclasses.replace(scraper._hcs_job(hit, "Renown Health", "renownhealth"), description="")
    assert job.req_id == "" and scraper.normalize_job(job)["posting_facts"] is None
    # another platform with a number and no body keeps the global switch (off)
    other = dataclasses.replace(job, ats_platform="Talemetry", req_id="4981331")
    assert scraper.normalize_job(other)["posting_facts"] is None


# The posting_facts lines of the live preserve_scraped_enrichment() and
# hj_req_id_sync() (read with pg_get_functiondef on 2026-10-14), in Python.
def _live_trigger(old, new):
    if new is None and old is not None:
        new = old
    elif (isinstance(old, dict) and isinstance(new, dict) and "req_id" in new
          and {k: v for k, v in new.items() if k != "req_id"} == {}):
        new = {**old, **new}
    if isinstance(old, dict) and "req_id" in old and isinstance(new, dict) and "req_id" not in new:
        new = {**new, "req_id": old["req_id"]}
    req_col = (str(new.get("req_id") or "").strip() or None) if isinstance(new, dict) else None
    return new, req_col


def test_the_trigger_merges_the_number_into_stored_facts_and_the_column(monkeypatch):
    jobs = _adp(monkeypatch, "adp/kpc_requisitions.json", "a45074b6-b45e-424e-9e71-c45a4d73c34b", "CA")
    job = next(j for j in jobs if j.job_id == "9201508809155_1")
    sent = scraper.normalize_job(job)["posting_facts"]
    stored = {"certs": ["RN"], "education": "BSN", "v": scraper.FACTS_VERSION, "h": "0123456789ab"}
    facts, col = _live_trigger(stored, sent)
    assert facts == {**stored, "req_id": "9218"} and col == "9218"
    # a stored row with no facts at all simply gets the number
    assert _live_trigger(None, sent) == ({"req_id": "9218"}, "9218")
    # a later night's null keeps it
    assert _live_trigger(facts, None) == (facts, "9218")


def test_req_id_only_rows_write_once_then_hash_stable(monkeypatch):
    """Content hash (sql/100): the req_id-only payload differs from the null
    payload the row sent before, so the first run writes it; the same payload
    the next night hashes the same, so it is only stamped seen."""
    jobs = _adp(monkeypatch, "adp/kpc_requisitions.json", "a45074b6-b45e-424e-9e71-c45a4d73c34b", "CA")
    job = next(j for j in jobs if j.job_id == "9201508809155_1")
    night1 = scraper.normalize_job(job)
    before = scraper.normalize_job(dataclasses.replace(job, req_id=""))
    assert before["posting_facts"] is None
    assert scraper.content_hash_for(night1) != scraper.content_hash_for(before)
    assert scraper.content_hash_for(scraper.normalize_job(job)) == scraper.content_hash_for(night1)
