# 2026-10-27 RN gap, long tail (branch hs/gaplt-1027): config lines found with
# fingerprinter v2 on the 178 hospital systems we did not read and on the
# biggest nursing-home and post-acute chains. Every board was read off the
# employer's own careers page, answered a live list check, and passed a
# no-write dry run through its adapter (rows, rows in the hospital's town,
# RN rows by the site rule). No network, no database.
import re

import pytest

import scraper

NEW = {
    "WORKDAY_TENANTS": {
        "Montage Health": [
            "montagehealth",
            "5",
            "Montage_Health"
        ],
        "Marshall Medical Center": [
            "marshallmedical",
            "1",
            "MMC"
        ],
        "Mercy Medical Center Cedar Rapids": [
            "mercycare",
            "5",
            "Mercy_Cedar_Rapids_Careers"
        ],
        "Boone Hospital Center": [
            "boone",
            "503",
            "Careers"
        ],
        "Mary Lanning Healthcare": [
            "marylanning",
            "1",
            "marylanningcareers"
        ],
        "Great Plains Health": [
            "gphealth",
            "5",
            "GPH"
        ],
        "Golden Valley Memorial Healthcare": [
            "gvmh",
            "504",
            "GVMH"
        ],
        "King's Daughters Medical Center": [
            "univofky",
            "108",
            "UKKD"
        ]
    },
    "ICIMS_ORGS": {
        "St. Luke's Hospital (St. Louis)": "careers-stlukes-stl.icims.com",
        "University Hospital (Newark)": "careers-uhnjcareers.icims.com",
        "St. John's Riverside Hospital": "careers-riversidehealth.icims.com"
    },
    "KRONOS_ORGS": {
        "Baton Rouge General Medical Center": [
            "prd01-hcm01.prd",
            "6042637",
            "LA"
        ],
        "Oaklawn Hospital": [
            "secure7.saashr.com",
            "6155701",
            "MI"
        ],
        "Western Wisconsin Health": [
            "secure3.saashr.com",
            "6109363",
            "WI"
        ],
        "NHS Management": [
            "prd01-hcm01.npr",
            "6148296",
            "AL"
        ]
    },
    "PAYCOM_ORGS": {
        "Washington Regional Medical Center": "8DB2A037AA9570E8305F90774012AD8E",
        "Fairfield Medical Center": "73A000C6C682ED2B2B05CF23D62462E8",
        "Trinity Health (Minot)": "E32A77D88C6BCC7B836ED79F274E9EBF",
        "Bristol Hospital": "4597F1280A2AA5933921106F1AFB8700",
        "Lake Region Healthcare": "1A7B46CA8E301A3037C03B992EDBE9E6",
        "Methodist Rehabilitation Center": "DA601DE5FDD2AF5958CC3B273469A638",
        "Schoolcraft Memorial Hospital": "E693DE00DA22B02A79DB46B631265FE0",
        "St. John's Health (Jackson)": "5164671CDCC11A3BAD413D14974B1AAA",
        "Mountain View Hospital (Idaho Falls)": "2F7486FAE11A9F442FC7841B33514458",
        "Cantex Continuing Care": "5D0D962A20961FDE966647E0CA5D5F18"
    },
    "HEALTHCARESOURCE_ORGS": {
        "Blessing Health System": "blessing",
        "Berkshire Health Systems": "berkshire",
        "Holzer Health System": "holzer",
        "Signature Healthcare Brockton Hospital": "brocktonhospital",
        "Conway Medical Center": "cmcsc",
        "Baxter Health": "baxter",
        "Memorial Healthcare (Owosso)": "mhcc",
        "Emerson Health": "emerson",
        "Ozarks Healthcare": "ozarks",
        "Children's Hospital of The King's Daughters": "kingsdaughters",
        "Morris Hospital": "morrishospital",
        "Citizens Memorial Healthcare": "citizensmemorial",
        "Northfield Hospital + Clinics": "northfieldhospital",
        "St. Clair Health": "stclair"
    },
    "UKG_ORGS": {
        "Licking Memorial Hospital": [
            "https://recruiting.ultipro.com/LIC1000LMHS",
            "3a2849cd-0da6-4a57-813a-80b366fb0172",
            "OH"
        ],
        "Hancock Regional Hospital": [
            "https://hancockhealth.rec.pro.ukg.net/HAN1500HCRH",
            "0d9549f5-8d33-4f24-a8e0-96611223b112",
            "IN"
        ],
        "St. Mary's Healthcare (Amsterdam)": [
            "https://stmary.rec.pro.ukg.net/STM1500STMH",
            "77a8f537-cc67-4dfc-ba9e-fac7c978527f",
            "NY"
        ],
        "Indiana Regional Medical Center": [
            "https://recruiting.ultipro.com/IND1019INDI",
            "2dbb7a3a-54b7-4c82-9c18-a0260627136b",
            "PA"
        ],
        "Fisher-Titus Medical Center": [
            "https://recruiting.ultipro.com/FIS1006FSHR",
            "4f0ef98c-7a2a-435a-b8ae-34f07ba89aae",
            "OH"
        ],
        "Olmsted Medical Center": [
            "https://recruiting2.ultipro.com/OLM1000OMC",
            "8aa650e0-3e57-4e69-ad1e-6503d36d502a",
            "MN"
        ],
        "Schneck Medical Center": [
            "https://schneckmed.rec.pro.ukg.net/JAC1502SCKM",
            "0ecf6482-01c9-4568-bec5-8ea8e3f534a3",
            "IN"
        ],
        "Winona Health": [
            "https://recruiting2.ultipro.com/WIN1020WINO",
            "e8f5c467-8162-4978-9a1e-9dacada74410",
            "MN"
        ],
        "Valley View Hospital": [
            "https://recruiting.ultipro.com/VAL1004VVH",
            "7a5186be-89cf-4eb5-b2e5-869118e3e2f0",
            "CO"
        ],
        "Pella Regional Health Center": [
            "https://pellarhc.rec.pro.ukg.net/PEL1000PELLA",
            "3807457d-d4d9-4311-a73c-bfb456afd44c",
            "IA"
        ],
        "Shenandoah Medical Center": [
            "https://recruiting2.ultipro.com/SHE1012SHEM",
            "bcd5738c-c38c-4642-a992-1887c69cb7aa",
            "IA"
        ],
        "Margaret Mary Health": [
            "https://mmhealth.rec.pro.ukg.net/MAR1052MGMH",
            "8cc2d18e-f33a-4496-825e-4b5c69e3cf1d",
            "IN"
        ],
        "Grande Ronde Hospital": [
            "https://grandehospital.rec.pro.ukg.net/GRA1506GDRH",
            "54d9efc6-9e5b-442a-a6c5-64ad8c352228",
            "OR"
        ],
        "Mobridge Regional Hospital": [
            "https://recruiting2.ultipro.com/MOB1005MOHO",
            "d740b8a2-1c7c-47a6-9027-ec800aed2766",
            "SD"
        ],
        "Tri-State Memorial Hospital": [
            "https://recruiting.ultipro.com/TRI1015",
            "e2afe3d3-239e-a920-52a1-3515af825791",
            "WA"
        ],
        "American Senior Communities": [
            "https://recruiting2.ultipro.com/AME1091ASEN",
            "92ca1d4b-eaf6-482c-82b7-bb3bc300b9dd",
            "IN"
        ],
        "AmSurg": [
            "https://recruiting.ultipro.com/AMS1004AMSRG",
            "94a3d750-7693-4943-8b5d-0a9b17bb4e4f"
        ]
    },
    "APPLICANTPRO_ORGS": {
        "Beloit Health System": [
            "beloithealthsystem",
            "19763"
        ]
    },
    "ADPCX_ORGS": {
        "SBH Health System": [
            "stbarnabascareers",
            "NY"
        ],
        "The Brooklyn Hospital Center": [
            "tbhcareers",
            "NY"
        ],
        "Saint Joseph's Medical Center (Yonkers)": [
            "stjosephsyonkersjobs",
            "NY"
        ],
        "Williamson Health": [
            "williamsonhealthcareers",
            "TN"
        ],
        "Finger Lakes Health": [
            "fingerlakes",
            "NY"
        ],
        "North Country Healthcare": [
            "ncha",
            "NH"
        ],
        "Nexion Health": [
            "nexion",
            ""
        ]
    },
    "TALEO_ORGS": {
        "Cook County Health": [
            "cookcountyil.taleo.net",
            "200",
            "IL"
        ],
        "Hennepin Healthcare": [
            "hcmc.taleo.net",
            "1",
            "MN"
        ],
        "Cape Cod Healthcare": [
            "capecodhc.taleo.net",
            "ex",
            "MA"
        ]
    },
    "SELECTMINDS_ORGS": {
        "UTMB Health": "aa083"
    },
    "JIBE_SITES": {
        "Mercy Medical Center (Baltimore)": "https://careers.mdmercy.com",
        "Confluence Health": "https://careers.confluencehealth.org",
        "Beebe Healthcare": "https://www.beebecareers.org",
        "Holy Redeemer Health System": "https://careers.redeemerhealth.org",
        "Signature HealthCARE": "https://www.signaturehealthcarejobs.com"
    },
    "INFOR_ORGS": {
        "NCH Healthcare System": [
            "css-b5wedk4337b8b9un-prd",
            "1",
            "EXTERNAL",
            "FL"
        ],
        "Columbus Regional Health": [
            "css-g8ancuhyudvlv76g-prd",
            "CRHH",
            "EXTERNAL",
            "IN"
        ],
        "North Oaks Health System": [
            "css-northoakshealth-prd",
            "1000",
            "EXTERNAL",
            "LA"
        ]
    },
    "ORACLE_ORGS": {
        "Life Care Services": [
            "https://eexs.fa.us2.oraclecloud.com",
            "AllJobs"
        ]
    },
    "PHENOM_ORGS": {
        "Innovative Renal Care": "https://careers.innovativerenal.com"
    },
    "ADP_ORGS": {
        "Choice Health at Home": [
            "026d2af3-da01-4dc8-a714-353f3f03daba",
            "19000101_000001",
            "",
            ""
        ]
    }
}

LOCATION = {
    "Montage Health": [
        "Monterey",
        "CA"
    ],
    "Marshall Medical Center": [
        "Placerville",
        "CA"
    ],
    "Mercy Medical Center Cedar Rapids": [
        "Cedar Rapids",
        "IA"
    ],
    "Boone Hospital Center": [
        "Columbia",
        "MO"
    ],
    "Mary Lanning Healthcare": [
        "Hastings",
        "NE"
    ],
    "Great Plains Health": [
        "North Platte",
        "NE"
    ],
    "Golden Valley Memorial Healthcare": [
        "Clinton",
        "MO"
    ],
    "King's Daughters Medical Center": [
        "Ashland",
        "KY"
    ],
    "University Hospital (Newark)": [
        "Newark",
        "NJ"
    ],
    "Baton Rouge General Medical Center": [
        "Baton Rouge",
        "LA"
    ],
    "Oaklawn Hospital": [
        "Marshall",
        "MI"
    ],
    "Western Wisconsin Health": [
        "Baldwin",
        "WI"
    ],
    "Washington Regional Medical Center": [
        "Fayetteville",
        "AR"
    ],
    "Fairfield Medical Center": [
        "Lancaster",
        "OH"
    ],
    "Trinity Health (Minot)": [
        "Minot",
        "ND"
    ],
    "Bristol Hospital": [
        "Bristol",
        "CT"
    ],
    "Lake Region Healthcare": [
        "Fergus Falls",
        "MN"
    ],
    "Methodist Rehabilitation Center": [
        "Jackson",
        "MS"
    ],
    "Schoolcraft Memorial Hospital": [
        "Manistique",
        "MI"
    ],
    "St. John's Health (Jackson)": [
        "Jackson",
        "WY"
    ],
    "Blessing Health System": [
        "Quincy",
        "IL"
    ],
    "Berkshire Health Systems": [
        "Pittsfield",
        "MA"
    ],
    "Holzer Health System": [
        "Gallipolis",
        "OH"
    ],
    "Signature Healthcare Brockton Hospital": [
        "Brockton",
        "MA"
    ],
    "Conway Medical Center": [
        "Conway",
        "SC"
    ],
    "Baxter Health": [
        "Mountain Home",
        "AR"
    ],
    "Memorial Healthcare (Owosso)": [
        "Owosso",
        "MI"
    ],
    "Emerson Health": [
        "Concord",
        "MA"
    ],
    "Ozarks Healthcare": [
        "West Plains",
        "MO"
    ],
    "Children's Hospital of The King's Daughters": [
        "Norfolk",
        "VA"
    ],
    "Morris Hospital": [
        "Morris",
        "IL"
    ],
    "Citizens Memorial Healthcare": [
        "Bolivar",
        "MO"
    ],
    "Northfield Hospital + Clinics": [
        "Northfield",
        "MN"
    ],
    "St. Clair Health": [
        "Pittsburgh",
        "PA"
    ],
    "Licking Memorial Hospital": [
        "Newark",
        "OH"
    ],
    "Hancock Regional Hospital": [
        "Greenfield",
        "IN"
    ],
    "St. Mary's Healthcare (Amsterdam)": [
        "Amsterdam",
        "NY"
    ],
    "Indiana Regional Medical Center": [
        "Indiana",
        "PA"
    ],
    "Fisher-Titus Medical Center": [
        "Norwalk",
        "OH"
    ],
    "Olmsted Medical Center": [
        "Rochester",
        "MN"
    ],
    "Schneck Medical Center": [
        "Seymour",
        "IN"
    ],
    "Winona Health": [
        "Winona",
        "MN"
    ],
    "Valley View Hospital": [
        "Glenwood Springs",
        "CO"
    ],
    "Pella Regional Health Center": [
        "Pella",
        "IA"
    ],
    "Shenandoah Medical Center": [
        "Shenandoah",
        "IA"
    ],
    "Margaret Mary Health": [
        "Batesville",
        "IN"
    ],
    "Grande Ronde Hospital": [
        "La Grande",
        "OR"
    ],
    "Mobridge Regional Hospital": [
        "Mobridge",
        "SD"
    ],
    "Tri-State Memorial Hospital": [
        "Clarkston",
        "WA"
    ],
    "Beloit Health System": [
        "Beloit",
        "WI"
    ],
    "SBH Health System": [
        "Bronx",
        "NY"
    ],
    "The Brooklyn Hospital Center": [
        "Brooklyn",
        "NY"
    ],
    "Saint Joseph's Medical Center (Yonkers)": [
        "Yonkers",
        "NY"
    ],
    "Williamson Health": [
        "Franklin",
        "TN"
    ],
    "Finger Lakes Health": [
        "Geneva",
        "NY"
    ],
    "Cook County Health": [
        "Chicago",
        "IL"
    ],
    "Hennepin Healthcare": [
        "Minneapolis",
        "MN"
    ],
    "Cape Cod Healthcare": [
        "Hyannis",
        "MA"
    ],
    "Mercy Medical Center (Baltimore)": [
        "Baltimore",
        "MD"
    ],
    "Confluence Health": [
        "Wenatchee",
        "WA"
    ],
    "Beebe Healthcare": [
        "Lewes",
        "DE"
    ],
    "Holy Redeemer Health System": [
        "Meadowbrook",
        "PA"
    ],
    "NCH Healthcare System": [
        "Naples",
        "FL"
    ],
    "Columbus Regional Health": [
        "Columbus",
        "IN"
    ],
    "Mountain View Hospital (Idaho Falls)": [
        "Idaho Falls",
        "ID"
    ],
    "North Oaks Health System": [
        "Hammond",
        "LA"
    ]
}

PAYCOM_STATE = {
    "Washington Regional Medical Center": "AR",
    "Fairfield Medical Center": "OH",
    "Trinity Health (Minot)": "ND",
    "Bristol Hospital": "CT",
    "Lake Region Healthcare": "MN",
    "Methodist Rehabilitation Center": "MS",
    "Schoolcraft Memorial Hospital": "MI",
    "St. John's Health (Jackson)": "WY",
    "Mountain View Hospital (Idaho Falls)": "ID",
    "Cantex Continuing Care": "TX"
}

WD_DEFAULT = {
    "Montage Health": [
        "Monterey",
        "CA"
    ],
    "Marshall Medical Center": [
        "Placerville",
        "CA"
    ],
    "Mercy Medical Center Cedar Rapids": [
        "Cedar Rapids",
        "IA"
    ],
    "Boone Hospital Center": [
        "Columbia",
        "MO"
    ],
    "Great Plains Health": [
        "North Platte",
        "NE"
    ],
    "Golden Valley Memorial Healthcare": [
        "Clinton",
        "MO"
    ],
    "King's Daughters Medical Center": [
        "Ashland",
        "KY"
    ]
}


def _norm(v):
    return tuple(v) if isinstance(v, list) else v


@pytest.mark.parametrize("table", sorted(NEW))
def test_new_configs_present(table):
    d = getattr(scraper, table)
    for system, cfg in NEW[table].items():
        assert d[system] == _norm(cfg), (table, system)


def test_location_defaults():
    for system, (city, state) in LOCATION.items():
        assert scraper.SYSTEM_LOCATION_DEFAULTS[system.lower()] == (city, state), system
        assert city and re.fullmatch(r"[A-Z]{2}", state)
    for system, (city, state) in WD_DEFAULT.items():
        assert scraper.WD_TENANT_DEFAULT[system] == (city, state), system
        assert system in scraper.WORKDAY_TENANTS


def test_paycom_boards_default_to_their_state():
    for system, st in PAYCOM_STATE.items():
        assert scraper.PAYCOM_DEFAULT_STATE[system] == st
        job = scraper._paycom_job({"jobId": "1", "jobTitle": "RN"}, None, system, "K",
                                  scraper.PAYCOM_DEFAULT_STATE.get(system, "TX"))
        assert job.state == st


def test_config_shapes_match_their_adapters():
    for system, (guid, slug, st) in NEW.get("PAYLOCITY_ORGS", {}).items():
        assert re.fullmatch(r"[0-9a-f-]{36}", guid) and slug and (st == "" or len(st) == 2), system
    for system, key in NEW.get("PAYCOM_ORGS", {}).items():
        assert re.fullmatch(r"[0-9A-F]{32}", key), system
    for system, v in NEW.get("UKG_ORGS", {}).items():
        base, guid, st = (list(v) + [""])[:3]
        assert re.match(r"https://(recruiting2?\.ultipro\.com|[a-z0-9]+\.rec\.pro\.ukg\.net)/[A-Za-z0-9]+$", base), system
        assert re.fullmatch(r"[0-9a-f-]{36}", guid) and (st == "" or len(st) == 2)
    for system, (sub, company, st) in NEW.get("KRONOS_ORGS", {}).items():
        assert company.isdigit() and len(st) == 2, system
        assert scraper._kronos_base(sub).startswith("https://")
    for system, (tenant, wd, site) in NEW.get("WORKDAY_TENANTS", {}).items():
        assert wd.isdigit() and tenant and site, system
    for system, (dom, st) in NEW.get("ADPCX_ORGS", {}).items():
        assert re.fullmatch(r"[a-z0-9-]+", dom) and (st == "" or len(st) == 2), system
    for system, value in NEW.get("ADP_ORGS", {}).items():
        centers = scraper._adp_centers(_norm(value))
        assert re.fullmatch(r"[0-9a-f-]{36}", centers[0][0]), system
    for system, (host, cs, st) in NEW.get("TALEO_ORGS", {}).items():
        assert host.endswith(".taleo.net") and cs and len(st) == 2, system
    for system, (host, org, board, st) in NEW.get("INFOR_ORGS", {}).items():
        assert re.fullmatch(r"css-[a-z0-9]+-prd", host) and org and board, system
    for system, url in list(NEW.get("JIBE_SITES", {}).items()) + list(NEW.get("TALEMETRY_SITES", {}).items()) \
            + list(NEW.get("PHENOM_ORGS", {}).items()):
        assert url.startswith("https://") and not url.endswith("/"), system
    for system, (base, rpp) in NEW.get("TALENTBREW_ORGS", {}).items():
        assert base.startswith("https://") and base.endswith("/search-jobs") and rpp > 0, system
    for system, dom in NEW.get("ICIMS_ORGS", {}).items():
        assert dom.endswith(".icims.com"), system
    for system, (base, site) in NEW.get("ORACLE_ORGS", {}).items():
        assert re.match(r"https://[a-z0-9.-]+\.oraclecloud\.com$", base) and site, system


def test_no_board_is_configured_twice():
    keys = {
        "PAYCOM_ORGS": lambda v: v, "PAYLOCITY_ORGS": lambda v: v[0], "UKG_ORGS": lambda v: v[1],
        "KRONOS_ORGS": lambda v: v[1], "APPLICANTPRO_ORGS": lambda v: v[0], "HEALTHCARESOURCE_ORGS": lambda v: v,
        "WORKDAY_TENANTS": lambda v: (v[0], v[2]), "ICIMS_ORGS": lambda v: v, "ADPCX_ORGS": lambda v: v[0],
        "JIBE_SITES": lambda v: v, "TALEMETRY_SITES": lambda v: v, "PHENOM_ORGS": lambda v: v,
        "TALENTBREW_ORGS": lambda v: v[0], "TALEO_ORGS": lambda v: (v[0], v[1]), "INFOR_ORGS": lambda v: (v[0], v[1]),
        "ORACLE_ORGS": lambda v: (v[0], v[1]), "SELECTMINDS_ORGS": lambda v: v,
    }
    for table, cfgs in NEW.items():
        key = keys.get(table)
        if not key:
            continue
        allk = [str(key(v)).lower() for v in getattr(scraper, table).values() if isinstance(v, (str, tuple))]
        for system, cfg in cfgs.items():
            assert allk.count(str(key(_norm(cfg))).lower()) == 1, (table, system)


def test_new_labels_are_new_and_not_folded_into_another_system():
    for table, cfgs in NEW.items():
        for system in cfgs:
            assert system not in scraper.HOSPITAL_SYSTEM_ALIASES, system
            # a label lives in one adapter table only
            others = [t for t in NEW if t != table and system in NEW[t]]
            assert not others, (system, others)
    # Trinity Health (Minot ND) is not Trinity Health (Livonia MI)
    assert "Trinity Health (Minot)" in scraper.PAYCOM_ORGS
    assert scraper.HOSPITAL_SYSTEM_ALIASES.get("Trinity Health (Minot)") is None


def test_kings_daughters_sites_take_their_towns():
    # the UKKD site names buildings and practices; the map gives each its town
    assert scraper._wd_facility("King's Daughters Medical Center", "Pavilion", "", "")[1:] == ("Ashland", "KY")
    assert scraper._wd_facility("King's Daughters Medical Center", "Wheelersburg Family Care", "", "")[1:] == ("Wheelersburg", "OH")
    assert scraper.WD_TENANT_DEFAULT["King's Daughters Medical Center"] == ("Ashland", "KY")


def _job(system, city="", state=""):
    return scraper.Job(title="Registered Nurse", hospital_system=system, hospital_name=system, city=city, state=state,
                       location="", specialty="", job_type="", url="https://example.org/1", job_id="1",
                       posted_date="", description="", ats_platform="Taleo")


def test_boards_without_a_town_take_the_hospitals_town():
    # Taleo lists with an empty location column (Cook County, Cape Cod), an
    # Infor board that writes only the state (North Oaks), blank Workday rows
    d = scraper.normalize_job(_job("Cook County Health", state="IL"))
    assert (d["city"], d["state"]) == ("Chicago", "IL")
    d = scraper.normalize_job(_job("North Oaks Health System", state="LA"))
    assert (d["city"], d["state"]) == ("Hammond", "LA")
    d = scraper.normalize_job(_job("Boone Hospital Center"))
    assert (d["city"], d["state"]) == ("Columbia", "MO")


def test_icims_boards_that_read_the_town_from_the_job_page_have_no_default():
    # St. Luke's (Chesterfield, Des Peres, clinics) and St. John's Riverside list
    # no town on the card; the detail pass fills it from the posting (10-27
    # check: Chesterfield, Creve Coeur, Yonkers), so a default would misfile rows
    for system in ("St. Luke's Hospital (St. Louis)", "St. John's Riverside Hospital"):
        assert system.lower() not in scraper.SYSTEM_LOCATION_DEFAULTS, system


def test_iredell_board_stays_under_its_stage3_label():
    # Iredell Health System's Paylocity board is on main already (coverage
    # stage 3) under "Iredell Davis Behavioral Health Hospital"; not added twice
    guid = "e48fa797-ce91-4d50-b81e-d93ea7e1a2e0"
    assert [v[0] for v in scraper.PAYLOCITY_ORGS.values()].count(guid) == 1


def test_shared_tenants_are_not_duplicated():
    # Surgery Partners' whole-company UKG board and Fundamental's all-jobs iCIMS
    # portal repeat boards already configured per hospital; they stay out.
    ukg = [v[1] for v in scraper.UKG_ORGS.values()]
    assert "aa616d8f-f2a8-46c2-8f8e-1ca56e162ffd" not in ukg
    assert "alljobs-fundltc.icims.com" not in scraper.ICIMS_ORGS.values()
