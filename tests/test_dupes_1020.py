# 2026-10-20 (coverage stage 1, duplicate fixes). Three causes of one
# opening on two rows, found in the 10-20 duplicate audit:
#  * S1: one requisition under two posting ids in one place (AdventHealth's
#    Findly-Google ids, HCA's Talemetry ids, Workday's "-1" location suffix);
#  * T1: one Vivian agency contract listed under two categories
#    ("...-3511816" and "...-3511816-therapy");
#  * T2: one AMN contract from AMN's own feed and from Vivian.
import scraper
from scraper import Job, TravelJob


def _job(system, job_id, title="Registered Nurse", city="Orlando", state="FL", desc="x" * 50,
         req_id="", url=None, ats="Findly-Google"):
    return Job(title=title, hospital_system=system, hospital_name=system, city=city, state=state,
               location=f"{city}, {state}", specialty="", job_type="", job_id=job_id, posted_date="",
               url=url or f"https://example.org/job/{job_id}", description=desc, ats_platform=ats,
               req_id=req_id)


# -- S1 -----------------------------------------------------------------------

def test_one_requisition_two_posting_ids_one_city_keeps_one_row():
    rows = scraper.finalize_jobs([
        _job("AdventHealth", "23744623", req_id="R-0379591", desc="short body"),
        _job("AdventHealth", "23744999", req_id="R-0379591", desc="a much longer body " * 20),
    ])
    assert len(rows) == 1
    assert rows[0]["job_id"] == "23744999"                 # the longer body wins


def test_one_requisition_in_two_towns_is_never_collapsed():
    rows = scraper.finalize_jobs([
        _job("AdventHealth", "1", req_id="R316207", city="Orlando"),
        _job("AdventHealth", "2", req_id="R316207", city="Tampa"),
    ])
    assert sorted(r["city"] for r in rows) == ["Orlando", "Tampa"]


def test_different_titles_or_requisitions_stay_separate():
    rows = scraper.finalize_jobs([
        _job("AdventHealth", "1", req_id="R100", title="Registered Nurse"),
        _job("AdventHealth", "2", req_id="R100", title="Registered Nurse - Nights"),
        _job("AdventHealth", "3", req_id="R101", title="Registered Nurse"),
    ])
    assert len(rows) == 3


def test_title_punctuation_and_case_do_not_split_one_requisition():
    rows = scraper.finalize_jobs([
        _job("HCA Healthcare", "18284308", req_id="4981331", title="RN - ICU (Nights)"),
        _job("HCA Healthcare", "18284310", req_id="4981331", title="rn icu nights"),
    ])
    assert len(rows) == 1


def test_workday_location_suffix_is_the_same_requisition():
    rows = scraper.finalize_jobs([
        _job("Prisma Health", "R1114518", ats="Workday", city="Greenville", state="SC"),
        _job("Prisma Health", "R1114518-1", ats="Workday", city="Greenville", state="SC", desc="y" * 400),
    ])
    assert [r["job_id"] for r in rows] == ["R1114518-1"]


def test_workday_hyphenated_requisitions_are_not_cut():
    # "JR-75922" and "JR-75923" are two requisitions; the suffix rule needs a
    # digit before the dash and a one- or two-digit tail.
    assert scraper._req_key(_job("Wellstar Health", "JR-75922", ats="Workday")) == "jr-75922"
    assert scraper._req_key(_job("X", "R-53741", ats="Workday")) == "r-53741"
    assert scraper._req_key(_job("X", "R1114518_2", ats="Workday")) == "r1114518"
    # the suffix rule is Workday's alone: an iCIMS "2026-12" stays whole
    assert scraper._req_key(_job("X", "2026-12", ats="iCIMS")) == "2026-12"


def test_placeholder_requisitions_never_merge_rows():
    rows = scraper.finalize_jobs([
        _job("Some Board", "a1", req_id="0"),
        _job("Some Board", "a2", req_id="0"),
        _job("Some Board", "a3", req_id="N/A"),
        _job("Some Board", "a4", req_id="N/A"),
    ])
    assert len(rows) == 4


def test_rows_without_a_city_are_never_collapsed():
    rows = scraper.finalize_jobs([
        _job("AdventHealth", "1", req_id="R200", city="", state="FL"),
        _job("AdventHealth", "2", req_id="R200", city="", state="FL"),
    ])
    assert len(rows) == 2


def test_two_systems_with_one_requisition_number_stay_separate():
    rows = scraper.finalize_jobs([
        _job("System A", "1", req_id="R300"),
        _job("System B", "2", req_id="R300"),
    ])
    assert len(rows) == 2


def test_apply_verified_copy_beats_a_longer_unverified_one(monkeypatch):
    monkeypatch.setattr(scraper, "OPAQUE_APPLY_DOMAINS", {"opaque.example"})
    rows = scraper.finalize_jobs([
        _job("AdventHealth", "1", req_id="R400", url="https://opaque.example/job/1", desc="z" * 900),
        _job("AdventHealth", "2", req_id="R400", url="https://example.org/job/2", desc="z" * 100),
    ])
    assert [r["job_id"] for r in rows] == ["2"]


def test_one_requisition_on_two_family_career_sites_in_one_town_keeps_one():
    # SYSTEM_REQ_FAMILIES members count as one system here too; the family
    # rule itself (job_id only, any title) still runs first.
    rows = scraper.finalize_jobs([
        _job("Wellstar Health", "JR-1", req_id="JR-1", ats="Workday", city="Marietta", state="GA"),
        _job("Wellstar Health (Providers)", "JR-1", req_id="JR-1", ats="Workday", city="Marietta", state="GA"),
    ])
    assert len(rows) == 1 and rows[0]["hospital_system"] == "Wellstar Health"


# -- T1 / T2 --------------------------------------------------------------------

def _tj(agency, jid, state="MN", pay=2300.0, city="Anoka"):
    return TravelJob(agency_name=agency, agency_job_id=jid, title="SLP", city=city, state=state,
                     weekly_pay_numeric=pay)


def test_vivian_category_copies_of_one_contract_keep_the_plain_id():
    jobs = [_tj("AMN Healthcare Allied", "vivian:platform::773-3511816-therapy"),
            _tj("AMN Healthcare Allied", "vivian:platform::773-3511816"),
            _tj("ESS Clinical", "vivian:platform::18569-4600-therapy"),
            _tj("ESS Clinical", "vivian:platform::18569-4600-school-services")]
    out = scraper._vivian_one_per_contract(jobs)
    ids = [j.agency_job_id for j in out]
    assert ids == ["vivian:platform::773-3511816", "vivian:platform::18569-4600-therapy"]


def test_vivian_contract_rule_is_per_agency_and_state():
    jobs = [_tj("Agency A", "vivian:platform::1-55"), _tj("Agency B", "vivian:platform::1-55-therapy"),
            _tj("Agency A", "vivian:platform::1-55-allied", state="WI")]
    assert len(scraper._vivian_one_per_contract(jobs)) == 3


def test_vivian_copy_with_pay_beats_one_without_when_both_carry_a_suffix():
    jobs = [_tj("X", "vivian:platform::9-77-therapy", pay=None), _tj("X", "vivian:platform::9-77-allied", pay=2000.0)]
    assert [j.agency_job_id for j in scraper._vivian_one_per_contract(jobs)] == ["vivian:platform::9-77-allied"]


def test_direct_amn_row_outranks_the_vivian_copy_in_the_same_state():
    jobs = [_tj("AMN Healthcare", "amn-3511816", pay=2304.0),
            _tj("AMN Healthcare Allied", "vivian:platform::773-3511816", pay=2339.5),
            _tj("AMN Healthcare Revenue Cycle", "vivian:platform::3780-3593573-allied", state="MA"),
            _tj("AMN Healthcare", "amn-3593573", state="MA")]
    out = scraper._drop_vivian_amn_copies(jobs)
    assert [j.agency_job_id for j in out] == ["amn-3511816", "amn-3593573"]


def test_vivian_amn_copy_stays_when_the_direct_feed_lacks_it_or_differs_in_state():
    jobs = [_tj("AMN Healthcare", "amn-1"),
            _tj("AMN Healthcare Allied", "vivian:platform::773-2"),            # no direct row
            _tj("AMN Healthcare Allied", "vivian:platform::773-1", state="WI"),  # direct row is MN
            _tj("Aya Healthcare", "vivian:platform::5-1")]                       # not an AMN label
    out = scraper._drop_vivian_amn_copies(jobs)
    assert len(out) == 4


def test_vivian_amn_copies_all_stay_when_the_direct_feed_failed():
    jobs = [_tj("AMN Healthcare Allied", "vivian:platform::773-1")]
    assert scraper._drop_vivian_amn_copies(jobs) == jobs
