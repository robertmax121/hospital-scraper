# 2026-09-10 (Z-texas-acute-D): offline tests for the Texas block-D batch.
# Fixtures were captured on 2026-09-10 through curl_cffi / the adapters'
# request shapes and trimmed to one or two items each.
import json
import re

import scraper


def test_paycom_preview_and_detail_map_to_job(fixture_text):
    prev = json.loads(fixture_text("paycom_previews.json"))["jobPostingPreviews"]
    det = json.loads(fixture_text("paycom_detail.json"))
    j = scraper._paycom_job(prev[0], det, "Connally Memorial Medical Center", "772E59A3981B29A14463EC6C3223083C")
    assert j.ats_platform == "Paycom" and j.job_id == str(prev[0]["jobId"])
    assert j.city == "Floresville" and j.state == "TX"
    assert j.url == "https://www.paycomonline.net/v4/ats/web.php/portal/772E59A3981B29A14463EC6C3223083C/jobs/" + j.job_id
    assert j.title == prev[0]["jobTitle"] and "<" not in j.description
    j2 = scraper._paycom_job(prev[1], None, "x", "K")           # preview only
    assert j2 and j2.state == "TX" and j2.description


def test_paycom_search_body_shape():
    assert scraper._PAYCOM_FILTERS["sortOption"] == "N" and "keywordSearchText" in scraper._PAYCOM_FILTERS
    for name, key in scraper.PAYCOM_ORGS.items():
        assert re.fullmatch(r"[0-9A-F]{32}", key), name


def test_workable_v3_items_map_and_page_token(fixture_text):
    d = json.loads(fixture_text("workable_v3.json"))
    jobs = [scraper._workable_job(x, "Huntsville Memorial Hospital", "huntsville-memorial-hospital", "TX") for x in d["results"]]
    assert all(jobs) and d["nextPage"]
    for j in jobs:
        assert j.ats_platform == "Workable" and j.city == "Huntsville" and j.state == "TX"
        assert re.fullmatch(r"https://apply\.workable\.com/huntsville-memorial-hospital/j/[0-9A-F]+/", j.url)
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date) and j.job_type in ("Full time", "Part time", "Contract", "Temporary", "")


def test_csod_requisitions_map_with_deep_link(fixture_text):
    d = json.loads(fixture_text("csod_search.json"))
    jobs = [scraper._csod_job(r, "JPS Health Network", "https://jpshealthnet.csod.com", "4", "jpshealthnet") for r in d["data"]["requisitions"]]
    assert all(jobs)
    for j in jobs:
        assert j.ats_platform == "CSOD" and j.city == "Fort Worth" and j.state == "TX"
        assert j.url == f"https://jpshealthnet.csod.com/ux/ats/careersite/4/requisition/{j.job_id}?c=jpshealthnet"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date)


def test_paycor_home_rows_parse_title_location_department(fixture_text):
    html = fixture_text("paycor_home.html")
    jobs = scraper._parse_paycor_home(html, "Townsen Memorial Hospital", "8a7883d090b87b970190e27961ce11c4")
    assert len(jobs) == 2
    for j in jobs:
        assert j.ats_platform == "Paycor" and j.state == "TX" and j.city == "Humble"
        assert "JobIntroduction.action?clientId=8a7883d090b87b970190e27961ce11c4&id=" + j.job_id in j.url
        assert j.specialty and j.title and "<" not in j.title


def test_taleo_be_rss_items_map(fixture_text):
    xml = fixture_text("taleo_be_rss.xml")
    jobs = scraper._parse_taleo_be_rss(xml, "Baptist Hospitals of Southeast Texas", "https://phf.tbe.taleo.net/phf02", "BHST", "38", "TX")
    assert len(jobs) == 2
    for j in jobs:
        assert j.ats_platform == "TaleoBE" and j.state == "TX" and j.city
        assert j.url == f"https://phf.tbe.taleo.net/phf02/ats/careers/v2/viewRequisition?org=BHST&cws=38&rid={j.job_id}"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date) and len(j.description) > 200 and "<" not in j.description


def test_hcts_cards_parse_title_location_date_facility(fixture_text):
    html = fixture_text("hcts_search.html")
    jobs = scraper._parse_hcts_page(html, "University Medical Center of El Paso", "umcelpasocareers", "TX")
    assert len(jobs) == 2
    for j in jobs:
        assert j.ats_platform == "HCTS" and j.city == "El Paso" and j.state == "TX"
        assert re.fullmatch(r"https://umcelpasocareers\.hctsportals\.com/jobs/\d+-[a-z0-9-]+", j.url) and j.job_id in j.url
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date) and j.hospital_name.startswith("University Medical Center")


def test_kronos_saashr_wage_and_host(fixture_text):
    d = json.loads(fixture_text("kronos_saashr.json"))
    w = scraper._kronos_wage(d["job_requisitions"][0])
    assert w and w[2] == "hour" and w[0] > 10
    cfg = scraper.KRONOS_ORGS["Wilbarger General Hospital"]
    assert cfg[0].endswith(".com") and cfg[1].isdigit() and cfg[2] == "TX"


def test_hcs_hit_reads_nested_and_flat_fields():
    hit = {"_id": "1", "_source": {"title": "RN", "jobLocation": {"address": {"addressLocality": "San Angelo", "addressRegion": "TX"}},
                                    "userArea": {"jobPostingID": "9001"}}}
    j = scraper._hcs_job(hit, "Shannon Medical Center", "shannonhealth")
    assert j and j.job_id == "9001" and j.city == "San Angelo" and j.state == "TX"
    assert j.url == "https://pm.healthcaresource.com/cs/shannonhealth#/job/9001"
    assert scraper._hcs_job({"_source": {"title": "", "id": "2"}}, "x", "t") is None
    assert "must" in scraper._HCS_BODY["query"]["bool"]          # {"match_all": {}}; "*" alone matched nothing


def test_hcs_recorded_hits_parse_facility_city_and_link(fixture_text):
    d = json.loads(fixture_text("healthcaresource_search.json"))
    jobs = [scraper._hcs_job(h, "Shannon Medical Center", "shannonhealth") for h in d["hits"]["hits"]]
    assert len(jobs) == 2 and all(jobs)
    for j in jobs:
        assert j.state == "TX" and j.city and j.job_id.isdigit() and j.url.endswith("#/job/" + j.job_id)
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", j.posted_date) and j.hospital_name.startswith("Shannon")
        assert j.specialty and j.job_type


def test_texas_block_d_config_entries_exist():
    assert scraper.ADP_ORGS["Legent Health"][2] == "TX" and scraper.ADP_ORGS["Palo Pinto General Hospital"][3]
    assert scraper.PAYLOCITY_ORGS["Eastland Memorial Hospital"][2] == "TX"
    assert scraper.FINDLY_GOOGLE_ORGS["UT Southwestern Medical Center"][1] == []
    assert scraper.HEALTHCARESOURCE_ORGS["Shannon Medical Center"] == "shannonhealth"
    assert scraper.PAYCOR_ORGS["Townsen Memorial Hospital"].startswith("8a7883d0")
    assert scraper.WORKABLE_ORGS["Huntsville Memorial Hospital"][0] == "huntsville-memorial-hospital"
    assert scraper.TALEO_BE_ORGS["Baptist Hospitals of Southeast Texas"][1:3] == ("BHST", "38")
    assert scraper.HCTS_PORTALS["University Medical Center of El Paso"][0] == "umcelpasocareers"
