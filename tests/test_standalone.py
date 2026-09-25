"""push5/standalone (B2, 2026-09-25): configs for standalone CMS hospitals found
through Wikidata / the 09-17 fingerprint pass. Offline checks only."""
import inspect
import re

import scraper

STANDALONE = {
    "PAYCOM_ORGS": ["Seneca Healthcare District", "Mayers Memorial Hospital", "LifeStream Behavioral Center",
                    "Valor Health", "Gateways Hospital and Mental Health Center", "Santiam Hospital & Clinics",
                    "Putnam County Hospital", "Gibson Community Hospital"],
    "PAYLOCITY_ORGS": ["Jerold Phelps Community Hospital", "Coalinga Regional Medical Center", "Lake Butler Hospital",
                       "Ellenville Regional Hospital", "Brattleboro Retreat", "Western Missouri Medical Center"],
    "ADP_ORGS": ["Catalina Island Medical Center", "Northern Inyo Hospital", "Chapman Global Medical Center",
                 "Hollywood Presbyterian Medical Center", "Johnson Memorial Hospital"],
    "HEALTHCARESOURCE_ORGS": ["San Gorgonio Memorial Hospital", "Pacifica Hospital of the Valley", "Marion Health",
                              "Mary Greeley Medical Center", "Sheridan Memorial Hospital", "North Star Health Alliance"],
    "UKG_ORGS": ["Monadnock Community Hospital", "Woman's Hospital", "Community Hospital (Grand Junction)",
                 "Evanston Regional Hospital", "Gritman Medical Center"],
    "WORKDAY_TENANTS": ["Hospital for Special Surgery", "Rogers Behavioral Health", "Pine Rest Christian Mental Health Services"],
    "ICIMS_ORGS": ["AHMC Healthcare", "NeuroPsychiatric Hospitals", "Blythedale Children's Hospital"],
    "ORACLE_ORGS": ["Baptist Health Care (Pensacola)", "Health Care District of Palm Beach County"],
    "WORKABLE_ORGS": ["Aurora Vista del Mar Hospital", "Aurora Charter Oak Hospital"],
    "GREENHOUSE_ORGS": ["Lewis County General Hospital"],
    "APPLICANTPRO_ORGS": ["Mile Bluff Medical Center"],
}


def test_every_new_board_is_configured_with_a_location_default():
    for table, labels in STANDALONE.items():
        cfg = getattr(scraper, table)
        for lab in labels:
            assert lab in cfg, (table, lab)
            city, state = scraper.SYSTEM_LOCATION_DEFAULTS[lab.lower()]
            assert city and re.fullmatch(r"[A-Z]{2}", state), lab


def test_config_shapes_match_their_adapters():
    for lab in STANDALONE["PAYLOCITY_ORGS"]:
        guid, org, st = scraper.PAYLOCITY_ORGS[lab]
        assert re.fullmatch(r"[0-9a-f-]{36}", guid) and org and len(st) == 2
    for lab in STANDALONE["ADP_ORGS"]:
        centers = scraper._adp_centers(scraper.ADP_ORGS[lab])
        assert centers[0][2] and centers[0][3] == lab
    for lab in STANDALONE["UKG_ORGS"]:
        base, guid, st = scraper.UKG_ORGS[lab]
        assert re.match(r"https://recruiting2?\.ultipro\.com/[A-Z0-9]+$", base) and len(st) == 2
    for lab in STANDALONE["WORKDAY_TENANTS"]:
        tenant, wd, site = scraper.WORKDAY_TENANTS[lab]
        assert wd.isdigit() and tenant and site
    for lab in STANDALONE["WORKABLE_ORGS"]:
        slug, st = scraper.WORKABLE_ORGS[lab]
        assert slug and len(st) == 2


def test_paycom_boards_outside_texas_default_to_their_own_state():
    assert set(scraper.PAYCOM_DEFAULT_STATE) <= set(scraper.PAYCOM_ORGS)
    assert scraper.PAYCOM_DEFAULT_STATE["Santiam Hospital & Clinics"] == "OR"
    src = inspect.getsource(scraper.scrape_paycom)
    assert "PAYCOM_DEFAULT_STATE.get(system" in src
    # a preview with no state takes the board's state, not TX
    job = scraper._paycom_job({"jobId": "1", "jobTitle": "RN"}, None, "Santiam Hospital & Clinics", "K",
                              scraper.PAYCOM_DEFAULT_STATE["Santiam Hospital & Clinics"])
    assert job.state == "OR"
    # Texas boards keep the old default
    job = scraper._paycom_job({"jobId": "2", "jobTitle": "RN"}, None, "Uvalde Memorial Hospital", "K")
    assert job.state == "TX"


def test_blank_location_rows_fall_back_to_the_hospital():
    j = scraper.Job(title="Registered Nurse", hospital_system="Mary Greeley Medical Center",
                    hospital_name="Mary Greeley Medical Center", city="", state="", location="",
                    specialty="", job_type="", url="https://x/1", job_id="1", posted_date="",
                    description="", ats_platform="HealthcareSource")
    d = scraper.normalize_job(j)
    assert (d["city"], d["state"]) == ("Ames", "IA")


def test_no_duplicate_board_for_existing_configs():
    # Victor Valley rides the existing "ADP Health System 9" cid; Clifton-Fine
    # rides Samaritan; Tidelands Georgetown rides Tidelands Health. None of
    # those boards is configured twice.
    cids = [v if isinstance(v, str) else v[0] for v in scraper.ADP_ORGS.values()]
    assert cids.count("1a214979-2739-4245-a1d1-38dc8531018f") == 1
    tenants = [v[0] for v in scraper.WORKDAY_TENANTS.values()]
    assert tenants.count("samaritanhealth") == 1 and tenants.count("tidelandshealth") == 1
