# 2026-09-24: offline tests for the rewritten Infor CloudSuite adapter.
# infor_list.json holds list rows captured read-only from the tenants' public
# boards on 2026-09-24 (JobPosting.SearchForJobsResults), trimmed to the
# fields the adapter reads; infor_detail.json is one JobPostingDisplay form.
import json
import re

import scraper


def _jobs(fixture_text, system):
    payload = json.loads(fixture_text("infor_list.json"))[system]
    jobs, rid_of = scraper._infor_jobs(system, scraper.INFOR_ORGS[system], scraper._infor_rows(payload))
    return {j.job_id: j for j in jobs}, rid_of


def test_infor_config_one_entry_per_board():
    boards = [(v[0], v[1], v[2]) for v in scraper.INFOR_ORGS.values()]
    assert len(boards) == len(set(boards)), "two entries scrape the same board"
    for name, cfg in scraper.INFOR_ORGS.items():
        assert len(cfg) == 4 and re.fullmatch(r"css-[a-z0-9]+-prd", cfg[0]), name
        assert cfg[3] == "" or cfg[3] in scraper._INFOR_STATES, name
        assert name not in scraper.HOSPITAL_SYSTEM_ALIASES, name   # "Baptist Health" is rewritten to KY/IN
    for name in ("UNC Health", "MaineHealth", "Northern Light Health", "Baptist Health (AR)", "DHR Health"):
        assert name in scraper.INFOR_ORGS
    assert scraper.INFOR_ORGS["Northern Light Health"][1] == "10"
    assert scraper.INFOR_ORGS["DHR Health"][0] == "css-pf7dmpe5vb7ydcw4-prd"


def test_infor_ballad_street_addresses_and_title_place(fixture_text):
    jobs, rid_of = _jobs(fixture_text, "Ballad Health")
    j = jobs["40870"]
    assert (j.city, j.state, j.hospital_name) == ("Johnson City", "TN", "Johnson City Medical Center")
    assert j.url == ("https://css-balladhealth-prd.inforcloudsuite.com/hcm/Jobs/navigation/"
                     "JobPosting[JobPostingSet](1,40870,1).JobPostingDisplayNav?csk.HROrganization=1&csk.JobBoard=EXTERNAL")
    assert j.ats_platform == "Infor" and re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date)
    assert (jobs["40699"].city, jobs["40699"].state) == ("Norton", "VA")
    # blank location fields: the title's "- Johnson City, TN" names a city the tenant already has
    assert (jobs["40335"].city, jobs["40335"].state) == ("Johnson City", "TN")
    assert rid_of["40870"] == "JobPosting[JobPostingSet](1,40870,1)"


def test_infor_location_forms(fixture_text):
    expect = {
        "BayCare": {"173660": ("Tampa", "FL", "St Josephs"), "174707": ("", "FL", "BayCare")},
        "Maury Regional Health": {"15944": ("Lewisburg", "TN", "Marshall Medical Center")},
        "Lakeland Regional Health": {"35569": ("Lakeland", "FL", "LRH Medical Center")},
        "Aspirus Health": {"129236": ("Wausau", "WI", "Aspirus Wausau Hospital")},
        "MaineHealth": {"94647": ("North Conway", "NH", "MaineHealth")},
        "UNC Health": {"240950": ("Butner", "NC", "UNC Health")},
        "Vandalia Health": {"80201": ("Ronceverte", "WV", "Greenbrier Valley Medical Center"),
                            "57072": ("Weston", "WV", "Stonewall Jackson Memorial Hospital")},
        "Penn State Health": {"104101": ("Camp Hill", "PA", "Penn State Health")},
        "Baptist Health (AR)": {"64016": ("Fort Smith", "AR", "Baptist Health (AR)")},
    }
    for system, rows in expect.items():
        jobs, _ = _jobs(fixture_text, system)
        for job_id, want in rows.items():
            j = jobs[job_id]
            assert (j.city, j.state, j.hospital_name) == want, (system, job_id)


def test_infor_one_row_per_requisition(fixture_text):
    jobs, _ = _jobs(fixture_text, "Middlesex Health")
    assert list(jobs) == ["5244"]
    assert (jobs["5244"].city, jobs["5244"].state) == ("Middletown", "CT")
    assert "(1,5244,2)" in jobs["5244"].url                  # the newest posting wins


def test_infor_worktype_codes(fixture_text):
    assert scraper._infor_worktype("DAILYBASE") == "Per diem"
    assert scraper._infor_worktype("SUPPLTIER2") == "Per diem"
    assert scraper._infor_worktype("PRO-RATA .9") == ""
    assert scraper._infor_worktype("A") == "" and scraper._infor_worktype("3DWE") == ""
    assert scraper._infor_worktype("FT") == "FT" and scraper._infor_worktype("PART TIME BEN") == "PART TIME BEN"
    jobs, _ = _jobs(fixture_text, "Lakeland Regional Health")
    assert jobs["35569"].job_type == ""                      # "A" is a site code


def test_infor_address_and_colon_parsers():
    assert scraper._infor_address("1 MEDICAL PARK BLVD SUITE 210E BRISTOL TN") == ("Bristol", "TN")
    assert scraper._infor_address("1406 TUSCULUM BLVD MOB 2 GREENEVILLE TN") == ("Greeneville", "TN")
    assert scraper._infor_address("1990 HOLTON AVE BIG STONE GAP VA") == ("Big Stone Gap", "VA")
    assert scraper._infor_address("Tifton, GA") == ("", "")
    assert scraper._infor_colon("US:Florida:Largo:HomeCare Largo") == ("Largo", "FL", "HomeCare Largo")
    assert scraper._infor_colon("WI:Rhinelander:54501:Oneida") == ("Rhinelander", "WI", "")
    assert scraper._infor_colon("Tampa:St Josephs") == ("", "", "")


def test_infor_detail_payload_fills_body_and_type(fixture_text):
    d = json.loads(fixture_text("infor_detail.json"))
    j = scraper.Job(title="Housekeeper", hospital_system="Faith Regional Health", hospital_name="Faith Regional Health",
                    city="Norfolk", state="NE", location="Norfolk, NE", specialty="", job_type="", url="https://x",
                    job_id="2467", posted_date="", description="", ats_platform="Infor")
    assert scraper._infor_apply_detail(j, d) is True
    assert len(j.description) >= 200 and "<" not in j.description
    assert j.job_type == "Part Time No Benefits"
    assert j.wage_min is None                                 # "0 - 0 per hour" is no posted pay
