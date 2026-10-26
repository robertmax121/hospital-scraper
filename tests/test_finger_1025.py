# 2026-10-25 coverage stage 3: config lines generated from the v2 fingerprinter
# (fingerprint_hospitals.py) for independent CMS hospitals. Every board was read
# off the hospital's own careers page, answered a live list check, and passed a
# no-write dry run through its adapter with most rows in the hospital's town.
# No network, no database.
import re

import pytest

import scraper

NEW = {
    "UKG_ORGS": {
        "Bartlett Regional Hospital": [
            "https://bartlett.rec.pro.ukg.net/BAR1503BART",
            "28db10a5-eb40-4dac-8d90-5bad1ab11310",
            "AK"
        ],
        "Methodist Family Health": [
            "https://recruiting2.ultipro.com/MET1015MFHE",
            "0e63f212-c96d-4306-8d29-0230529a9841",
            "AR"
        ],
        "The CORE Institute": [
            "https://recruiting.ultipro.com/COR1016COREI",
            "d1445d2b-c378-4a1e-9e18-d5d1a7565517",
            "AZ"
        ],
        "Fort Defiance Indian Hospital": [
            "https://recruiting2.ultipro.com/FOR1019FDIH",
            "7aa05ce1-542f-4ed6-9385-15dab39f6bcd",
            "AZ"
        ],
        "The Healing Place": [
            "https://polarahealth.rec.pro.ukg.net/POL1502POHE",
            "aa390afa-5bde-420a-abbd-8f80071f8c64",
            "AZ"
        ],
        "Sierra View Medical Center": [
            "https://sierraview.rec.pro.ukg.net/SIE1005SVMC",
            "b614f151-a52a-44c5-9d7e-9485ef47f936",
            "CA"
        ],
        "Valley Presbyterian Hospital": [
            "https://valleypres.rec.pro.ukg.net/VAL1505VYPH",
            "6b03c339-10b4-46a7-a81e-6715086d1518",
            "CA"
        ],
        "Animas Surgical Hospital": [
            "https://recruiting.ultipro.com/SUR1004SRGY",
            "53b5dba2-c474-42b0-b1b9-c1724fef1e2d",
            "CO"
        ],
        "Delta County Memorial Hospital": [
            "https://deltahospital.rec.pro.ukg.net/DEL1509DTCM",
            "253d7268-1c54-4c49-b525-f435f3deb9a7",
            "CO"
        ],
        "Pioneers Medical Center": [
            "https://pioneers.rec.pro.ukg.net/PIO1009ERBH",
            "59f03e51-a699-4a1a-bc32-99612a394665",
            "CO"
        ],
        "Family Health West Hospital": [
            "https://recruiting.ultipro.com/FAM1007FHW",
            "cd7d40fb-cb7e-4508-99bc-f7fd7aaa9d32",
            "CO"
        ],
        "Gunnison Valley Health": [
            "https://recruiting2.ultipro.com/GUN1002GNSO",
            "7d509108-fbb4-44e1-823a-c3d39950b43a",
            "CO"
        ],
        "Cass County Memorial Hospital": [
            "https://casscounty.rec.pro.ukg.net/CAS1505CAMH",
            "58a00d6c-4dd7-4d3e-a86f-1748e2bcdeae",
            "IA"
        ],
        "Crawford County Memorial Hospital": [
            "https://recruiting.ultipro.com/CRA1005CCMH",
            "2288cfd6-b30d-ce0b-abed-b3c9da0c8f6c",
            "IA"
        ],
        "Mahaska Health Partnership": [
            "https://mahaskaukg.rec.pro.ukg.net/MAH1000MAHC",
            "0d34e7b0-f433-4e51-a750-b2785087496d",
            "IA"
        ],
        "Montgomery County Memorial Hospital": [
            "https://recruiting2.ultipro.com/MON1027MONC",
            "5ddef1ca-eb1c-4f63-885f-9537416b5a47",
            "IA"
        ],
        "St. Anthony Regional Hospital & Nursing Home": [
            "https://stanthonyhosp.rec.pro.ukg.net/STA1506STAG",
            "da0acbc0-0342-4f09-b331-abc6eb8a4a2e",
            "IA"
        ],
        "Bonner General Hospital": [
            "https://bonner.rec.pro.ukg.net/BON1503BONN",
            "e6a6022a-8366-4829-9d4d-f737baac9b7e",
            "ID"
        ],
        "Northwest Specialty Hospital": [
            "https://recruiting.ultipro.com/SUR1004SRGY",
            "b9809736-4266-441f-b708-7916dceaa58e",
            "ID"
        ],
        "Lawrence County Memorial Hospital": [
            "https://lawrencecomem.rec.pro.ukg.net/LAW1502LCMH",
            "35e673f7-bd10-40a1-8a4b-52e5e6308dac",
            "IL"
        ],
        "Wabash General Hospital": [
            "https://recruiting2.ultipro.com/WAB1000WBGH",
            "33638a3f-c9cc-41d2-8850-be3b44f12bce",
            "IL"
        ],
        "Ascentist Hospital": [
            "https://ascentist.rec.pro.ukg.net/ASC1500AHWS",
            "fe77992b-147f-4e3a-8323-b85161fa9e51",
            "KS"
        ],
        "Kansas Spine & Specialty Hospital": [
            "https://recruiting.ultipro.com/SUR1004SRGY",
            "a1f9eecd-255f-46f2-a5bb-1dd4f95dfd1d",
            "KS"
        ],
        "Kentucky River Medical Center": [
            "https://recruiting2.ultipro.com/QHC1000QHCS",
            "11c5b1ac-b258-4022-99e9-34fd6421b7f9",
            "KY"
        ],
        "Avoyelles Hospital": [
            "https://allegiance.rec.pro.ukg.net/ALL1049AHMI",
            "123dea0c-dfc9-4d8b-8381-1c793572e899",
            "LA"
        ],
        "Lafayette Surgical Specialty Hospital": [
            "https://recruiting.ultipro.com/SUR1004SRGY",
            "90d84020-217c-4b03-a630-2f5fdbf15cd6",
            "LA"
        ],
        "Winn Parish Medical Center": [
            "https://allegiance.rec.pro.ukg.net/ALL1049AHMI",
            "55de8045-0bd5-441f-986b-e1aa833c3b2e",
            "LA"
        ],
        "Hillsdale Hospital": [
            "https://recruiting.ultipro.com/HIL1007HCHC",
            "962acc5c-4fb3-4a63-a646-d84c72c1854b",
            "MI"
        ],
        "Chippewa County Hospital": [
            "https://recruiting2.ultipro.com/CCM1000CCHE",
            "13f19108-3d95-4f30-bec7-7b339ded10dc",
            "MN"
        ],
        "Gillette Children's Specialty Healthcare": [
            "https://recruiting2.ultipro.com/GIL1002GCSH",
            "0a5d892e-feaa-4c51-b816-12c2dbc1167d",
            "MN"
        ],
        "LifeCare Medical Center": [
            "https://recruiting2.ultipro.com/LIF1018LIFEM",
            "31a6af76-ca9c-432e-9e95-17f29684d3fc",
            "MN"
        ],
        "Rainy Lake Medical Center": [
            "https://rainylake.rec.pro.ukg.net/INT1106IFMH",
            "52665fd6-c8a6-4a02-aea1-40cd1d26a576",
            "MN"
        ],
        "Ranken Jordan Pediatric Bridge Hospital": [
            "https://myrankenjordan.rec.pro.ukg.net/RAN1501RJHC",
            "62e8cb2a-5d28-4595-8f07-301316592d12",
            "MO"
        ],
        "York General Health Care Services": [
            "https://recruiting2.ultipro.com/YOR1000YKGH",
            "ad024917-d0b7-4def-b4e3-2241f52e8e89",
            "NE"
        ],
        "Huggins Hospital": [
            "https://hugginshospital.rec.pro.ukg.net/HUG1500HGSP",
            "52664b36-fe16-4498-9f35-dae2a33f3e69",
            "NH"
        ],
        "Bergen New Bridge Medical Center": [
            "https://recruiting.ultipro.com/BER1003BRMC",
            "7362b0d4-9214-406f-86b5-85f44c6f1238",
            "NJ"
        ],
        "Summa Western Reserve Hospital": [
            "https://recruiting.ultipro.com/HRR1000HRBEN",
            "18e0ca46-5746-4f6f-b2dc-3371c31b05fc",
            "OH"
        ],
        "Muscogee Nation Health": [
            "https://mcnhealth.rec.pro.ukg.net/TUL1500TCIC",
            "50c0b8ba-7d79-4679-b7a0-796cce471b9e",
            "OK"
        ],
        "Curry General Hospital": [
            "https://recruiting2.ultipro.com/CUR1005CURY",
            "68f149ac-aaa3-4518-a522-8943df27d2da",
            "OR"
        ],
        "OSS Orthopaedic Hospital": [
            "https://recruiting.ultipro.com/OSS1000OSSH",
            "29d6cd00-310d-59ec-f3dc-321e71ce1992",
            "PA"
        ],
        "Lifescape": [
            "https://lifescape.rec.pro.ukg.net/LIF1501LISC",
            "57efc672-bd5a-4b6f-8356-1025d3d31cd9",
            "SD"
        ],
        "Metro Nashville General Hospital": [
            "https://nashvillemgh.rec.pro.ukg.net/NAS1503NMGH",
            "5f773009-4590-40ec-ad18-e94460aa60bb",
            "TN"
        ],
        "Uintah Basin Medical Center": [
            "https://recruiting2.ultipro.com/UIN1000UIME",
            "85250f2c-5522-4b53-af9c-8f360eafe543",
            "UT"
        ],
        "Newport Community Hospital": [
            "https://nhhs714.rec.pro.ukg.net/PUB1501PHPOC",
            "933897cb-aafa-4dc7-976d-20362229a106",
            "WA"
        ],
        "Fort Memorial Hospital": [
            "https://forthc.rec.pro.ukg.net/FOR1037FMEM",
            "a9ddd9e5-a868-47e7-bdf1-2ba96314b7d8",
            "WI"
        ],
        "North Central Health Care": [
            "https://recruiting2.ultipro.com/NOR1057NCHC",
            "c14353c6-e9aa-4cd1-8454-2266da3cb835",
            "WI"
        ],
        "Oakleaf Surgical Hospital": [
            "https://recruiting.ultipro.com/SUR1004SRGY",
            "8dd2bed0-b6d1-4514-9358-c45d96a5cdca",
            "WI"
        ],
        "Richland Hospital": [
            "https://richland.rec.pro.ukg.net/RIC1500TRHI",
            "d8bbcc76-b530-4c16-9261-a7e0d9fafe98",
            "WI"
        ],
        "Southwest Health Center": [
            "https://southwesthealth.rec.pro.ukg.net/SOU1502SWHC",
            "736c3b2b-32cd-4667-a8ad-dc0869f551f5",
            "WI"
        ],
        "Tamarack Health": [
            "https://tamarackhealth.rec.pro.ukg.net/REG1500REGI",
            "9ad614d0-6df3-4942-bf76-c27dcf6898f9",
            "WI"
        ],
        "Vernon Memorial Hospital": [
            "https://vernonmemorial.rec.pro.ukg.net/VER1502VMHC",
            "0c24f5a0-d512-46a1-8785-50dcb9a9dcd6",
            "WI"
        ]
    },
    "HEALTHCARESOURCE_ORGS": {
        "Central Peninsula General Hospital": "cpgh",
        "Northeast Alabama Regional Medical Center": "narmc",
        "Upson Regional Medical Center": "upson",
        "McDonough District Hospital": "mdh",
        "Samaritan Behavioral Center": "behavioralcenter",
        "Granville Health Systems": "granvillemedical",
        "Boone County Health Center": "boonecohealth",
        "Box Butte General Hospital": "bbgh",
        "Valley County Health System": "valleycounty",
        "Carewell Health Medical Center": "eogh",
        "Holy Cross Medical Center (Taos)": "holycrossmedicalcenter",
        "Gifford Medical Center": "giffordhealthcare",
        "Mason General Hospital & Family of Clinics": "masongeneral",
        "Snoqualmie Valley Hospital": "snoqualmiehospital",
        "Cody Regional Health": "westpark",
        "Hot Springs County Memorial Hospital": "hscmh",
        "Memorial Hospital Sweetwater County": "sweetwatermemorial"
    },
    "ADP_ORGS": {
        "Kanakanak Hospital": [
            "c3cf205d-9677-4dfd-ab98-87a0f91551f4",
            "19000101_000001",
            "AK",
            "Kanakanak Hospital"
        ],
        "Norton Sound Regional Hospital": [
            "95989b7d-f38e-4631-bf2a-c33e57ce2877",
            "19000101_000001",
            "AK",
            "Norton Sound Regional Hospital"
        ],
        "Bibb Medical Center": [
            "12f3ca79-156d-401b-8a3c-b14c48f2fa72",
            "19000101_000001",
            "AL",
            "Bibb Medical Center"
        ],
        "Jack Hughston Memorial Hospital": [
            "50301a42-faf9-4b9b-a81d-76937ffdd9b0",
            "19000101_000001",
            "AL",
            "Jack Hughston Memorial Hospital"
        ],
        "Pioneers Memorial Healthcare District": [
            "c8be8fe7-2331-4233-8d3e-11ebfffeecdb",
            "19000101_000001",
            "CA",
            "Pioneers Memorial Healthcare District"
        ],
        "Jasper Memorial Hospital": [
            "8eff549f-e471-4f6b-aede-36699d203506",
            "19000101_000001",
            "GA",
            "Jasper Memorial Hospital"
        ],
        "Optim Healthcare": [
            "ce221109-aaa3-47b2-aead-38a00b716f6f",
            "19000101_000001",
            "GA",
            {
                "Sylvania": "Optim Medical Center - Screven",
                "Reidsville": "Optim Medical Center - Tattnall"
            }
        ],
        "Crawford Memorial Hospital": [
            "15dc44c7-eab3-4503-a1bc-a66382fbd1f7",
            "19000101_000001",
            "IL",
            "Crawford Memorial Hospital"
        ],
        "Iroquois Memorial Hospital": [
            "841b61c7-a8a1-475e-af6f-db1e8f08c427",
            "19000101_000001",
            "IL",
            "Iroquois Memorial Hospital"
        ],
        "Paris Community Hospital": [
            "6d761223-04f6-4d39-a498-276f6ca9389f",
            "19000101_000001",
            "IL",
            "Paris Community Hospital"
        ],
        "Sparta Community Hospital": [
            "41ccae75-fff1-4864-b23c-aafa64e376b7",
            "19000101_000001",
            "IL",
            "Sparta Community Hospital"
        ],
        "Holton Community Hospital": [
            "c5a636da-e25a-49c2-a8f9-21b2ee0c9cd8",
            "19000101_000001",
            "KS",
            "Holton Community Hospital"
        ],
        "Nemaha Valley Community Hospital": [
            "7a29495a-7f36-4e90-8ab9-7ee15c9413f7",
            "19000101_000001",
            "KS",
            "Nemaha Valley Community Hospital"
        ],
        "Phillips County Hospital": [
            "eb9c6c6e-053d-42fe-9759-55564942334b",
            "19000101_000001",
            "KS",
            "Phillips County Hospital"
        ],
        "Allen Parish Hospital": [
            "11590425-a902-4499-9ca3-b6dbe3e4abcd",
            "131202839_2",
            "LA",
            "Allen Parish Hospital"
        ],
        "Opelousas General Health System": [
            "6587fdce-d7b2-436f-815b-bf16ced46fbc",
            "19000101_000001",
            "LA",
            "Opelousas General Health System"
        ],
        "Straith Hospital for Special Surgery": [
            "5f9e1b86-3b1a-48e4-8a4b-d97d224db29d",
            "19000101_000001",
            "MI",
            "Straith Hospital for Special Surgery"
        ],
        "Riverwood Healthcare Center": [
            "a7d85d0d-fc6b-44ce-9b96-3bcf63b801e7",
            "19000101_000001",
            "MN",
            "Riverwood Healthcare Center"
        ],
        "Mineral Community Hospital": [
            "eefe8bc9-2bd6-4b8a-9c5d-6fec4ea14d79",
            "9200854641636_2",
            "MT",
            "Mineral Community Hospital"
        ],
        "Beatrice Community Hospital & Health Center": [
            "5a89661d-77cb-42c6-8b91-c00a04bec13d",
            "93136854_2053",
            "NE",
            "Beatrice Community Hospital & Health Center"
        ],
        "Jefferson Community Health & Life": [
            "a6184af1-a861-4bc2-8fb9-cbd78567d466",
            "19000101_000001",
            "NE",
            "Jefferson Community Health & Life"
        ],
        "Littleton Regional Healthcare": [
            "957eb50b-516a-435d-a4a7-fb5254f17fc1",
            "19000101_000001",
            "NH",
            "Littleton Regional Healthcare"
        ],
        "Roosevelt General Hospital": [
            "aa878936-c926-4971-b908-999813b98fd9",
            "19000101_000001",
            "NM",
            "Roosevelt General Hospital"
        ],
        "Three Crosses Regional Hospital": [
            "14099341-86e9-40d6-a7a2-6c57c3d581bd",
            "19000101_000001",
            "NM",
            "Three Crosses Regional Hospital"
        ],
        "Wayne Hospital": [
            "49021767-48df-4ef6-8582-d34c836c9fa0",
            "19000101_000001",
            "OH",
            "Wayne Hospital"
        ],
        "McAlester Regional Health Center": [
            "76891674-f7ec-4886-958b-849556f3b6dd",
            "19000101_000001",
            "OK",
            "McAlester Regional Health Center"
        ],
        "Oklahoma Surgical Hospital": [
            "8f6eb758-e42f-4c1c-b16c-25a48cd84b93",
            "19000101_000001",
            "OK",
            "Oklahoma Surgical Hospital"
        ],
        "Surgical Hospital of Oklahoma": [
            "8d443174-4f69-4bc8-abfc-fc444626e916",
            "19000101_000001",
            "OK",
            "Surgical Hospital of Oklahoma"
        ],
        "Wagoner Community Hospital": [
            "d852d76d-3c0a-405c-a524-0b0b0cd9ba85",
            "19000101_000001",
            "OK",
            "Wagoner Community Hospital"
        ],
        "Southern Coos Hospital & Health Center": [
            "b9d52d75-5c86-4df1-b92d-35c8ecc38c21",
            "19000101_000001",
            "OR",
            "Southern Coos Hospital & Health Center"
        ],
        "Macon Community Hospital": [
            "373aac54-6868-45db-acac-7d30007e6054",
            "19000101_000001",
            "TN",
            "Macon Community Hospital"
        ],
        "Blue Mountain Hospital": [
            "a0a9f289-91c0-438e-aa1c-506d2887b887",
            "19000101_000001",
            "UT",
            "Blue Mountain Hospital"
        ],
        "Grant Regional Health Center": [
            "84e68b06-658b-4d37-a6d5-20bbc3463ad9",
            "19000101_000001",
            "WI",
            "Grant Regional Health Center"
        ],
        "Niobrara County Hospital District": [
            "a20cd2ad-2b4a-419e-a049-412195d6d6e8",
            "19000101_000001",
            "WY",
            "Niobrara County Hospital District"
        ],
        "North Platte Valley Medical Center": [
            "b3cdeb5c-bb64-4686-a838-e84608ae0021",
            "19000101_000001",
            "WY",
            "North Platte Valley Medical Center"
        ]
    },
    "KRONOS_ORGS": {
        "South Peninsula Hospital": [
            "secure6.saashr.com",
            "6033684",
            "AK"
        ],
        "Bradley County Medical Center": [
            "secure7.saashr.com",
            "6200563",
            "AR"
        ],
        "Mt. Graham Regional Medical Center": [
            "secure4.saashr.com",
            "6162673",
            "AZ"
        ],
        "San Carlos Apache Healthcare": [
            "secure6.entertimeonline.com",
            "6144973",
            "AZ"
        ],
        "Alhambra Hospital Medical Center": [
            "prd01-hcm01.prd",
            "6004578",
            "CA"
        ],
        "Southern Monterey County Memorial Hospital": [
            "prd01-hcm01.prd",
            "6036332",
            "CA"
        ],
        "Mt. San Rafael Hospital": [
            "secure4.saashr.com",
            "6033795",
            "CO"
        ],
        "St. Vincent General Hospital District": [
            "secure7.saashr.com",
            "6205489",
            "CO"
        ],
        "Cherokee Regional Medical Center": [
            "secure6.saashr.com",
            "6174217",
            "IA"
        ],
        "Clarinda Regional Health Center": [
            "secure4.saashr.com",
            "6172951",
            "IA"
        ],
        "Iowa Specialty Hospital": [
            "prd01-hcm01.prd",
            "6016816",
            "IA"
        ],
        "Myrtue Medical Center": [
            "secure6.saashr.com",
            "6164612",
            "IA"
        ],
        "Caribou Medical Center": [
            "secure7.saashr.com",
            "6006526",
            "ID"
        ],
        "Syringa General Hospital": [
            "secure6.saashr.com",
            "6148416",
            "ID"
        ],
        "Teton Valley Hospital": [
            "secure6.saashr.com",
            "6195100",
            "ID"
        ],
        "Morrison Community Hospital": [
            "secure7.saashr.com",
            "6213465",
            "IL"
        ],
        "Unity Physicians Hospital": [
            "secure7.saashr.com",
            "6137938",
            "IN"
        ],
        "Clara Barton Hospital": [
            "secure4.saashr.com",
            "6012351",
            "KS"
        ],
        "Community HealthCare System": [
            "secure6.saashr.com",
            "6168248",
            "KS"
        ],
        "Community Memorial Healthcare (Marysville)": [
            "secure6.saashr.com",
            "6059803",
            "KS"
        ],
        "Kansas Medical Center": [
            "secure.entertimeonline.com",
            "6079352",
            "KS"
        ],
        "Norton County Hospital": [
            "secure6.saashr.com",
            "6198189",
            "KS"
        ],
        "St. James Parish Hospital": [
            "secure4.saashr.com",
            "6167570",
            "LA"
        ],
        "Northern Maine Medical Center": [
            "secure4.saashr.com",
            "6033683",
            "ME"
        ],
        "Eaton Rapids Medical Center": [
            "secure6.saashr.com",
            "6007842",
            "MI"
        ],
        "River's Edge Hospital & Clinic": [
            "secure3.saashr.com",
            "6114104",
            "MN"
        ],
        "Sleepy Eye Medical Center": [
            "secure6.saashr.com",
            "6192194",
            "MN"
        ],
        "Bates County Memorial Hospital": [
            "secure6.saashr.com",
            "6022998",
            "MO"
        ],
        "King's Daughters Medical Center-Brookhaven": [
            "prd01-hcm01.prd",
            "6052038",
            "MS"
        ],
        "Heart of America Medical Center": [
            "secure4.entertimeonline.com",
            "6012738",
            "ND"
        ],
        "Sakakawea Medical Center - Cah": [
            "secure6.saashr.com",
            "6158386",
            "ND"
        ],
        "Towner County Medical Center": [
            "secure7.saashr.com",
            "6203739",
            "ND"
        ],
        "Antelope Memorial Hospital": [
            "secure7.saashr.com",
            "6168212",
            "NE"
        ],
        "Community Hospital (McCook)": [
            "secure10.saashr.com",
            "6009809",
            "NE"
        ],
        "Morrill County Community Hospital": [
            "secure6.saashr.com",
            "6183103",
            "NE"
        ],
        "Phelps Memorial Health Center": [
            "secure4.saashr.com",
            "6131901",
            "NE"
        ],
        "Providence Medical Center (Wayne)": [
            "secure7.saashr.com",
            "6217024",
            "NE"
        ],
        "Nor-Lea Hospital District": [
            "prd01-hcm01.npr",
            "6163093",
            "NM"
        ],
        "Nathan Littauer Hospital": [
            "secure7.saashr.com",
            "6101323",
            "NY"
        ],
        "Adams County Regional Medical Center": [
            "secure7.saashr.com",
            "6146950",
            "OH"
        ],
        "Crystal Clinic Orthopaedic Center": [
            "secure4.saashr.com",
            "6097121",
            "OH"
        ],
        "McCurtain Memorial Hospital": [
            "secure6.saashr.com",
            "6181684",
            "OK"
        ],
        "StarCare Specialty Health": [
            "secure7.saashr.com",
            "6209217",
            "TX"
        ],
        "Columbia Basin Hospital": [
            "secure6.saashr.com",
            "6194802",
            "WA"
        ],
        "Lake Chelan Community Hospital": [
            "secure6.saashr.com",
            "6171358",
            "WA"
        ],
        "North Valley Hospital": [
            "secure7.saashr.com",
            "6204309",
            "WA"
        ],
        "Othello Community Hospital": [
            "secure6.saashr.com",
            "6154361",
            "WA"
        ],
        "Spooner Health System": [
            "secure6.entertimeonline.com",
            "6148886",
            "WI"
        ]
    },
    "PAYLOCITY_ORGS": {
        "Mizell Memorial Hospital": [
            "e16c4b8e-a600-44e2-94fa-7d7feb7f92a7",
            "Mizell-Memorial-Hospital",
            "AL"
        ],
        "Mississippi County Hospital System": [
            "e487105c-f7b0-4828-8ebe-e6c818334029",
            "Mississippi-County-Hospital-System",
            "AR"
        ],
        "Johnson Regional Medical Center": [
            "3c60e1bb-558b-4b95-88e9-be63335f5446",
            "Johnson-Regional-Medical-Center",
            "AR"
        ],
        "Little Colorado Medical Center": [
            "cf7b76bf-cd6a-4435-9fc1-eb32a8f662c9",
            "Winslow-Memorial-Hospital-Inc",
            "AZ"
        ],
        "Colusa Medical Center": [
            "019e5376-bf48-4554-810e-7f3af37c871f",
            "Colusa-Medical-Center",
            "CA"
        ],
        "Effingham Health System": [
            "39a7f9b4-9481-401a-8a9c-b76710b00d3e",
            "Effingham-Hospital-Inc",
            "GA"
        ],
        "Jenkins County Medical Center": [
            "0b577f6b-dac6-415e-8149-87f9e2534362",
            "Hospital-Authority-of-Jenkins-County",
            "GA"
        ],
        "Miller County Hospital": [
            "28e4090e-8444-4ea6-aeb0-b93afe488607",
            "The-Hospital-Authority-of-Miller-County",
            "GA"
        ],
        "Morgan Medical Center": [
            "1565b57c-7167-42de-9fde-91e19b4ff646",
            "Morgan-Medical",
            "GA"
        ],
        "Franklin County Medical Center": [
            "14d5b25e-11f4-47b7-943e-360c13ead2c5",
            "Franklin-County-Medical-Center",
            "ID"
        ],
        "Minidoka Memorial Hospital": [
            "552af7ef-b4a6-4b52-b6aa-5be315fe7ea3",
            "Minidoka-Memorial-Hospital",
            "ID"
        ],
        "North Canyon Medical Center": [
            "66c10b8a-db4e-403f-9834-34302025568d",
            "North-Canyon-Medical-Center-Inc",
            "ID"
        ],
        "Shoshone Medical Center": [
            "694d6b2d-57c1-41a9-85f7-3e1b70889858",
            "Shoshone-Medical-Center",
            "ID"
        ],
        "Hillsboro Area Hospital": [
            "f4f1113a-c9a7-43ce-939d-c99f3c9b0cca",
            "Hillsboro-Area-Hospital-Inc",
            "IL"
        ],
        "Thomas H Boyd Memorial Hospital": [
            "8b76868b-ecf9-49e8-a7c9-0883a343a0f5",
            "Boyd-Healthcare-Services",
            "IL"
        ],
        "Hospital District #6 Patterson Health Center": [
            "a0f5c18a-eefe-40c6-ab86-103f70c476b1",
            "Hospital-District-No-6-of-Harper-County-Kansas",
            "KS"
        ],
        "Meade District Hospital": [
            "aafeb3c5-f379-4786-bb37-b2936758b014",
            "Meade-District-Hospital",
            "KS"
        ],
        "Pratt Regional Medical Center": [
            "da6d037b-47f6-40d4-af26-cabf7d48794d",
            "Pratt-Regional-Medical-Center",
            "KS"
        ],
        "Scott County Hospital": [
            "89db5ce4-0e71-4c2e-9701-09076b7b8c9c",
            "Scott-County-Hospital",
            "KS"
        ],
        "Summit Surgical Hospital (Hutchinson)": [
            "cf0c3339-a090-4874-b5aa-81d6b9054690",
            "Summit-Surgical",
            "KS"
        ],
        "William Newton Hospital": [
            "433b306b-2a75-46a2-98a5-21d650e0d9ff",
            "William-Newton-Memorial-Hospital",
            "KS"
        ],
        "Bunkie General Hospital": [
            "74221d3c-8e70-4691-b081-dfe109d034bd",
            "Bunkie-General-Hospital",
            "LA"
        ],
        "Spine Hospital of Louisiana": [
            "7f4fdfc8-f174-45fc-9fe6-33c2fd87b9fc",
            "The-Spine-Hospital-of-Louisiana",
            "LA"
        ],
        "Cary Medical Center": [
            "b59b60b1-6ef4-4041-86b3-d80d5a2032ca",
            "City-of-Caribou",
            "ME"
        ],
        "Mount Desert Island Hospital": [
            "5e8f7172-61b6-4e56-b027-61e54072c2ac",
            "MDI-Hospital",
            "ME"
        ],
        "Mahnomen Health Center": [
            "acc41e35-57ca-4cb7-941c-36b1fc03bc2d",
            "Mahnomen-Health-Center",
            "MN"
        ],
        "Ray County Memorial Hospital": [
            "9bc8f447-ed5b-4120-aa3a-8aafe73825be",
            "Ray-County-Memorial-Hospital",
            "MO"
        ],
        "Ste Genevieve County Memorial Hospital": [
            "31d032a6-a176-4d4f-b3a6-24a95cf75bf0",
            "Ste-Genevieve-County-Memorial-Hospital",
            "MO"
        ],
        "South Sunflower County Hospital": [
            "a946e31f-cfab-4a2e-9e01-ce6a5b8ec4df",
            "South-Sunflower-County-Hospital",
            "MS"
        ],
        "Tyler Holmes Memorial Hospital": [
            "852ec141-c970-4799-ad36-64ee1090da31",
            "TYLER-HOLMES-MEMORIAL-HOSPITAL",
            "MS"
        ],
        "Rosebud Health Care Center": [
            "9f012c09-3bd1-464e-b950-d9149e6064b7",
            "Rosebud-Health-Care-Center",
            "MT"
        ],
        "Shodair Children's Hospital": [
            "6d709fd3-93c2-4c32-9639-5764f5025457",
            "Shodair",
            "MT"
        ],
        "Iredell Davis Behavioral Health Hospital": [
            "e48fa797-ce91-4d50-b81e-d93ea7e1a2e0",
            "Iredell-Memorial-Hospital-Incorporated",
            "NC"
        ],
        "Jamestown Regional Medical Center": [
            "e03f1d2e-1508-4cf5-bcfb-c9c733f95645",
            "Current-Opening",
            "ND"
        ],
        "St. Luke's Hospital (Crosby)": [
            "affd69b0-2fdd-40cd-a42c-5e01dbd351e6",
            "ST-LUKES-HOSPITAL",
            "ND"
        ],
        "Chadron Community Hospital and Health Services": [
            "a2c99aaf-68a7-46bc-85ec-efee40bde7d2",
            "Chadron-Community-Hospital-Corp",
            "NE"
        ],
        "Cherry County Hospital": [
            "97f5d188-4d5e-44c6-bc74-b4c22f48b052",
            "Cherry-County-Hospital-and-Clinic--non-main-page",
            "NE"
        ],
        "Kearney County Health Services Hospital": [
            "71a3718b-dc39-404d-bf32-fecc684ea041",
            "Kearney-County-Health-Services",
            "NE"
        ],
        "Kimball Health Services": [
            "390de4a1-fa4d-4201-9787-c1c380027db7",
            "Kimball-Health-Services",
            "NE"
        ],
        "Lexington Regional Health Center": [
            "06d1ca18-3222-497d-8d11-a2e0bef52803",
            "Lexington-Regional-Health-Center",
            "NE"
        ],
        "Memorial Hospital (Aurora)": [
            "27f5bfbf-d165-4fef-8a10-a8d4418a437c",
            "Memorial-Community-Health-Inc",
            "NE"
        ],
        "Webster County Community Hospital": [
            "de550125-e316-4a72-bd3b-65cb526a95f4",
            "Webster-County-Community-Hospital",
            "NE"
        ],
        "Cibola General Hospital": [
            "85d620ee-7141-4a4e-b1c0-c30659a256dc",
            "Cibola-General-Hospital",
            "NM"
        ],
        "Gila Regional Medical Center": [
            "471aa3fa-e6a2-4fad-9dd6-6c3e6badaab5",
            "Gila-Regional-Medical-Center",
            "NM"
        ],
        "Pershing General Hospital": [
            "b241eaf6-5f29-4237-8bd5-0e7ff4bac7be",
            "Pershing-General-Hospital",
            "NV"
        ],
        "Brookhaven Hospital": [
            "3bf03f0d-7c5f-4ed4-aa03-15e65d22ec52",
            "Brookhaven-Hospital-LLC",
            "OK"
        ],
        "Cleveland Area Hospital": [
            "89c1d3c8-d7ff-490a-971e-a53812e68764",
            "Cleveland-Area-Hospital-Holdings-Inc",
            "OK"
        ],
        "Jackson County Memorial Hospital Authority": [
            "58515f1b-667c-4baf-89c7-976246900cff",
            "Jackson-County-Memorial-Hospital-Authority",
            "OK"
        ],
        "Newberry County Memorial Hospital": [
            "354506d3-a1d2-481c-be4c-eb8491aba2c3",
            "NCMH-External-Candidates",
            "SC"
        ],
        "Huron Regional Medical Center": [
            "1d3f71c3-b5df-4036-ba45-2b31254377ef",
            "Huron-Regional-Medical-Center",
            "SD"
        ],
        "Chambers Health": [
            "d9034add-658d-4ed8-a5ae-abda92b03ec6",
            "Chambers-County-Public-Hospital",
            "TX"
        ],
        "Collingsworth General Hospital": [
            "97a8799d-6598-4c36-a03b-02bdbc11efdf",
            "Collingsworth-General-Hospital",
            "TX"
        ],
        "McCamey Hospital": [
            "32c26846-86fa-4fd0-bd45-477a0d9b401e",
            "McCamey-County-Hospital-District",
            "TX"
        ],
        "Kane County Hospital": [
            "c504a6f6-63a7-42b4-9eb5-5738623c4a20",
            "KCH-Career-Page",
            "UT"
        ],
        "Springfield Hospital": [
            "3443aa7a-2142-4d1a-9049-de77615adf16",
            "Springfield-springfield-hospital",
            "VT"
        ],
        "Mid Valley Hospital & Clinic": [
            "506da836-11f1-44d2-8330-e6429ac89854",
            "Mid-Valley-Hospital-Clinic",
            "WA"
        ],
        "Burnett Medical Center": [
            "0b6026c5-6c47-4389-b678-88f03ea5400e",
            "Burnett-Medical-Center-Inc",
            "WI"
        ],
        "Crossing Rivers Health Medical Center": [
            "5cb7fb64-5afc-4b1c-b716-03059330a6f7",
            "Crossing-Rivers-Health",
            "WI"
        ],
        "Star Valley Medical Center": [
            "2ec1c3c1-804e-4a6b-b3c8-e33caafc976e",
            "Star-Valley-Health",
            "WY"
        ]
    },
    "PAYCOM_ORGS": {
        "Mena Regional Health System": "87C5A8C599EA572A944E97C7C5E3797A",
        "North Arkansas Regional Medical Center": "9E3A1AF7E6BF8A4A3F6C8BFEB06A43E2",
        "Destiny Springs Healthcare": "73A6320CD88BEBA536A58136A2C6A58B",
        "La Paz Regional Hospital": "E08C56984DCF6DB08FD1FDD4659BAC14",
        "Sage Memorial Hospital": "778ACFA61C583E24FF4645873660AABF",
        "Healthbridge Children's Hospital - Orange": "EE76B94DD3BD99C8107675BF70BEB20A",
        "Mammoth Hospital": "898BD2386405070392D5E532BA263B62",
        "Modoc Medical Center": "8FD727396F86FD98562693776B93E3CD",
        "Palo Verde Hospital": "6FC9C01C65D06229F59C2EB52E3AFEB5",
        "Kit Carson County Memorial Hospital": "C05E70254DAFB96824DEFC7E4F8E0E49",
        "Lincoln Health Hospital": "3E16471F10EEF7BA8882A6FCAC97D408",
        "Pagosa Springs Medical Center": "FB1A890F98B1D3DDFF7273EC6DDED38E",
        "Connecticut Hospice": "19DBACFCFF3F5B41C4ABD5CDC6C51760",
        "Eastside Psychiatric Hospital": "9B3F69D20DE0FDE45AA00BB5B856F20E",
        "Candler County Hospital": "280C4195E9E620B28CBD7D599AD05159",
        "Emanuel Medical Center (Swainsboro)": "CA238BE253E335ECA168F5E2F7CEE6A2",
        "Evans Memorial Hospital": "104895E0E03480D23E475BECDF071242",
        "Jeff Davis Hospital (Hazlehurst)": "281C91F05C93F85D6DED185D0FEF28CD",
        "Ridgeview Institute Monroe": "4A1F988C60553FB4CF83C34E49D07627",
        "Boundary Community Hospital": "B50EAEF436088595D057E8E5CA8B4E3B",
        "Chicago Behavioral Hospital": "02959EC098A4CC351ED08B9149CF46CC",
        "Lake Behavioral Hospital": "F561F0AF277B12509F18272BA6C32254",
        "Silver Oaks Behavioral Hospital": "D6C9B2B3C32B47F6E1C63393E679852D",
        "Cameron Memorial Community Hospital": "BBF0A55EF3E4C554EB31F2F2675E8E98",
        "Amberwell Health": "F93347EBD424E4EF1897284D2C8D459A",
        "Coffey County Hospital": "B5D072EC86316F93A7DE9836BD2E8D6F",
        "Rice Community Healthcare": "0158F19B25768F89F726A2271A97F47F",
        "Kansas City Orthopaedic Institute": "9D7B03A7BB882E63BA78252C8957ABA0",
        "Manhattan Surgical Hospital": "57FA64DB6304EA7DE9931724ECBB9448",
        "Osborne County Memorial Hospital": "3730077781E48D99F8406F124BFA3992",
        "Caldwell Medical Center": "A0E52233EB22E4770AB4B9AA4AD4F055",
        "Carroll County Memorial Hospital": "398B7BEA524C5E3482733562E35A0FA4",
        "Livingston Hospital and Healthcare Services": "9AB2E051D04E5B26ECEEEFDA9A8CDA12",
        "Ohio County Hospital": "0C5629EF50BFA5365B465DBDC44A0785",
        "Trigg County Hospital": "A578F62423DCFF15464BE89EEEA5336A",
        "Surgical Specialty Center of Baton Rouge": "92A8E0D6D08D9AFDAF2261C2F8158845",
        "Bournewood Hospital": "57361158D3FF01C6C3F0B65B6CADA531",
        "Hospital for Behavioral Medicine": "F61F8FAA56D6F0753BA3190F54D6DF5D",
        "Sheridan Community Hospital": "391B6A97D5A2B77135D1F5D3AF2FDD8C",
        "Glacial Ridge Hospital": "51D11DB8BBDEAACB7A9ECC3C72601106",
        "Lakewood Health System": "2C527ECFE54643820FF338C3FBD8F077",
        "McKenzie County Healthcare Systems": "56DFBC02B9017809529D2909D89CCB36",
        "Brodstone Healthcare": "D47A4863AFD9565358108B0522B621D6",
        "Fillmore County Hospital": "523F46EC6B1864E9E535B93E4995869A",
        "Gordon Memorial Hospital District": "D75E5F777567445E85BBB07AD2BA82A6",
        "Lincoln Surgical Hospital": "E13386DBE120AA3652083E099DC16A10",
        "Syracuse Area Health": "87D86B019F36F98ACE455A331A4E7EEA",
        "Artesia General Hospital": "704B7D084ED420D91018B0DB07C889BE",
        "Brylin Hospitals": "91C4C467CACE0F7C68DD80E0F2B34156",
        "Mary Rutan Hospital": "1D9C49D4E9701353B4212E6BCA729496",
        "Atoka County Medical Center": "F06E7D6A3BA6575D65BA3D8528DBDE6B",
        "The Children's Center Rehabilitation Hospital": "7D4E5D43E0EA275B35368287B41F27C0",
        "Coal County General Hospital": "D9F405A20A0247BF926CDA2982BA9E8F",
        "Stilwell Memorial Hospital": "1AA43CB82BCCBA739287732B3D4ED804",
        "OneCore Health": "AAE93863A9A50E5859B05C34B90D9432",
        "Parkside Psychiatric Hospital": "F24B5DD8B19ACF1D75EC0A35C9EE1469",
        "Pawhuska Hospital": "01B084E1542C6D2CCAAFB97A42DB5BCB",
        "Summit Medical Center (Edmond)": "7C14F2018A1BF09A207B901ABD1BA047",
        "Eagleville Hospital": "3C1E98757786B36A0385B8D23A2BBEB7",
        "Malvern Behavioral Health": "C3D534C5EB67866599DBFB1F7237D5E0",
        "Hamilton General Hospital": "BED56E34E48579D2214701779F45CA51",
        "Coulee Medical Center": "59A052C2F2C7CDFEFBEBCED3F52A43F9",
        "Klickitat Valley Hospital": "9D7E216A94F019B126F0649DF68436DB",
        "Smokey Point Behavioral Hospital": "CA744E5FCDA461D14FBAE63E57D676F6",
        "South Sound Behavioral Hospital": "5EE0810D5ACC09AAE0C5493D654184F7",
        "Willapa Harbor Hospital": "1CE156A1130475F99587970A20A06B83",
        "Boone Memorial Hospital": "5D081DBD4ECDEB7A95B75001A827B58F",
        "Memorial Hospital of Carbon County": "0E1938E34C046A782F7C4035270FA096"
    },
    "APPLICANTPRO_ORGS": {
        "Cobre Valley Regional Medical Center": [
            "cvrmc",
            "11028"
        ],
        "Mille Lacs Health System": [
            "mlhealth",
            "5609"
        ],
        "United Hospital District": [
            "uhd",
            "5031"
        ],
        "Glendive Medical Center": [
            "gmc",
            "20224"
        ],
        "Granite County Medical Center": [
            "granitecountyhospital",
            "185"
        ],
        "Harney District Hospital": [
            "harneydh",
            "11796"
        ],
        "Garfield County Public Hospital District #1": [
            "pomeroymd",
            "4176"
        ]
    },
    "TALEO_BE_ORGS": {
        "Henry Mayo Newhall Hospital": [
            "https://phg.tbe.taleo.net/phg04",
            "HENRYMAYO",
            "40",
            "CA"
        ]
    },
    "WORKDAY_TENANTS": {
        "Aspen Valley Hospital": [
            "aspenvalleyhealth",
            "501",
            "AVH"
        ],
        "Centerstone": [
            "centerstone",
            "5",
            "CenterstoneCareers"
        ],
        "Waverly Health Center": [
            "waverlyhealthcenter",
            "12",
            "WHC"
        ],
        "Wood County Hospital": [
            "woodcountyhospital",
            "503",
            "jobs"
        ],
        "Wyandot Memorial Hospital": [
            "wyandotmemorial",
            "1",
            "WMH_Careers"
        ],
        "Brookings Health System": [
            "avera",
            "5",
            "BHS-careers"
        ],
        "Summit Pacific Medical Center": [
            "spmc",
            "5",
            "summitpacificmedicalcenter"
        ],
        "Sauk Prairie Hospital": [
            "saukprairiehealthcare",
            "12",
            "Sauk_Prairie_Healthcare_Careers"
        ]
    },
    "PAYCOR_ORGS": {
        "Virginia Gay Hospital": "8a7883d08ecaad99018eeac0ae610e93",
        "Hopedale Hospital": "8a7883d09397efbb0193cfba5aa61148",
        "Incompass Healthcare": "8a7883c6876de6fb018775a16c0001d2",
        "Kittson Healthcare": "8a7883c698ed62e70199338b155c0ad7",
        "Blueridge Vista Health and Wellness": "8a7883d09e235989019e427b8067063e",
        "Ohio Valley Surgical Hospital": "8a7883d06dcbf31d016ddb5a316b0f92",
        "Sunrise Vista Health and Wellness": "8a7883d09e235989019e4290a36206a1",
        "Prairie Lakes Healthcare System": "8a7883c6889839e90188ba9ab67308fb"
    },
    "ICIMS_ORGS": {
        "RiverView Health": "careers-riverviewhealth.icims.com",
        "Central Desert Behavioral Health Hospital": "centraldesertbh-careers-fundltc.icims.com",
        "Harmon Hospital": "harmonmedicalrehab-careers-fundltc.icims.com",
        "Oklahoma Heart Hospital": "internal-okheart.icims.com"
    },
    "WORKABLE_ORGS": {
        "Desert Parkway Behavioral Healthcare Hospital": [
            "desert-parkway",
            "NV"
        ]
    },
    "SMARTRECRUITERS_ORGS": {
        "Kittitas Valley Community Hospital": "KittitasValleyHealthcare"
    },
    "HCTS_PORTALS": {
        "WhidbeyHealth Medical Center": [
            "whidbeyhealthcareers",
            "WA"
        ],
        "Froedtert South": [
            "froedertsouthcareers",
            "WI"
        ]
    }
}

LOCATION = {
    "Bartlett Regional Hospital": [
        "Juneau",
        "AK"
    ],
    "Central Peninsula General Hospital": [
        "Soldotna",
        "AK"
    ],
    "Kanakanak Hospital": [
        "Dillingham",
        "AK"
    ],
    "Norton Sound Regional Hospital": [
        "Nome",
        "AK"
    ],
    "South Peninsula Hospital": [
        "Homer",
        "AK"
    ],
    "Bibb Medical Center": [
        "Centreville",
        "AL"
    ],
    "Jack Hughston Memorial Hospital": [
        "Phenix City",
        "AL"
    ],
    "Mizell Memorial Hospital": [
        "Opp",
        "AL"
    ],
    "Northeast Alabama Regional Medical Center": [
        "Anniston",
        "AL"
    ],
    "Bradley County Medical Center": [
        "Warren",
        "AR"
    ],
    "Johnson Regional Medical Center": [
        "Clarksville",
        "AR"
    ],
    "Mena Regional Health System": [
        "Mena",
        "AR"
    ],
    "North Arkansas Regional Medical Center": [
        "Harrison",
        "AR"
    ],
    "Cobre Valley Regional Medical Center": [
        "Globe",
        "AZ"
    ],
    "Destiny Springs Healthcare": [
        "Surprise",
        "AZ"
    ],
    "Fort Defiance Indian Hospital": [
        "Fort Defiance",
        "AZ"
    ],
    "La Paz Regional Hospital": [
        "Parker",
        "AZ"
    ],
    "Little Colorado Medical Center": [
        "Winslow",
        "AZ"
    ],
    "Mt. Graham Regional Medical Center": [
        "Safford",
        "AZ"
    ],
    "Sage Memorial Hospital": [
        "Ganado",
        "AZ"
    ],
    "San Carlos Apache Healthcare": [
        "Peridot",
        "AZ"
    ],
    "Alhambra Hospital Medical Center": [
        "Alhambra",
        "CA"
    ],
    "Colusa Medical Center": [
        "Colusa",
        "CA"
    ],
    "Healthbridge Children's Hospital - Orange": [
        "Orange",
        "CA"
    ],
    "Henry Mayo Newhall Hospital": [
        "Valencia",
        "CA"
    ],
    "Mammoth Hospital": [
        "Mammoth Lakes",
        "CA"
    ],
    "Modoc Medical Center": [
        "Alturas",
        "CA"
    ],
    "Palo Verde Hospital": [
        "Blythe",
        "CA"
    ],
    "Pioneers Memorial Healthcare District": [
        "Brawley",
        "CA"
    ],
    "Sierra View Medical Center": [
        "Porterville",
        "CA"
    ],
    "Southern Monterey County Memorial Hospital": [
        "King City",
        "CA"
    ],
    "Valley Presbyterian Hospital": [
        "Van Nuys",
        "CA"
    ],
    "Animas Surgical Hospital": [
        "Durango",
        "CO"
    ],
    "Aspen Valley Hospital": [
        "Aspen",
        "CO"
    ],
    "Delta County Memorial Hospital": [
        "Delta",
        "CO"
    ],
    "Pioneers Medical Center": [
        "Meeker",
        "CO"
    ],
    "Family Health West Hospital": [
        "Fruita",
        "CO"
    ],
    "Gunnison Valley Health": [
        "Gunnison",
        "CO"
    ],
    "Kit Carson County Memorial Hospital": [
        "Burlington",
        "CO"
    ],
    "Lincoln Health Hospital": [
        "Hugo",
        "CO"
    ],
    "Mt. San Rafael Hospital": [
        "Trinidad",
        "CO"
    ],
    "Pagosa Springs Medical Center": [
        "Pagosa Springs",
        "CO"
    ],
    "St. Vincent General Hospital District": [
        "Leadville",
        "CO"
    ],
    "Connecticut Hospice": [
        "Branford",
        "CT"
    ],
    "Eastside Psychiatric Hospital": [
        "Tallahassee",
        "FL"
    ],
    "Candler County Hospital": [
        "Metter",
        "GA"
    ],
    "Effingham Health System": [
        "Springfield",
        "GA"
    ],
    "Emanuel Medical Center (Swainsboro)": [
        "Swainsboro",
        "GA"
    ],
    "Evans Memorial Hospital": [
        "Claxton",
        "GA"
    ],
    "Jasper Memorial Hospital": [
        "Monticello",
        "GA"
    ],
    "Jeff Davis Hospital (Hazlehurst)": [
        "Hazlehurst",
        "GA"
    ],
    "Jenkins County Medical Center": [
        "Millen",
        "GA"
    ],
    "Miller County Hospital": [
        "Colquitt",
        "GA"
    ],
    "Morgan Medical Center": [
        "Madison",
        "GA"
    ],
    "Ridgeview Institute Monroe": [
        "Monroe",
        "GA"
    ],
    "Upson Regional Medical Center": [
        "Thomaston",
        "GA"
    ],
    "Cass County Memorial Hospital": [
        "Atlantic",
        "IA"
    ],
    "Cherokee Regional Medical Center": [
        "Cherokee",
        "IA"
    ],
    "Clarinda Regional Health Center": [
        "Clarinda",
        "IA"
    ],
    "Crawford County Memorial Hospital": [
        "Denison",
        "IA"
    ],
    "Mahaska Health Partnership": [
        "Oskaloosa",
        "IA"
    ],
    "Montgomery County Memorial Hospital": [
        "Red Oak",
        "IA"
    ],
    "Myrtue Medical Center": [
        "Harlan",
        "IA"
    ],
    "St. Anthony Regional Hospital & Nursing Home": [
        "Carroll",
        "IA"
    ],
    "Virginia Gay Hospital": [
        "Vinton",
        "IA"
    ],
    "Waverly Health Center": [
        "Waverly",
        "IA"
    ],
    "Bonner General Hospital": [
        "Sandpoint",
        "ID"
    ],
    "Boundary Community Hospital": [
        "Bonners Ferry",
        "ID"
    ],
    "Caribou Medical Center": [
        "Soda Springs",
        "ID"
    ],
    "Franklin County Medical Center": [
        "Preston",
        "ID"
    ],
    "Minidoka Memorial Hospital": [
        "Rupert",
        "ID"
    ],
    "North Canyon Medical Center": [
        "Gooding",
        "ID"
    ],
    "Northwest Specialty Hospital": [
        "Post Falls",
        "ID"
    ],
    "Shoshone Medical Center": [
        "Kellogg",
        "ID"
    ],
    "Syringa General Hospital": [
        "Grangeville",
        "ID"
    ],
    "Teton Valley Hospital": [
        "Driggs",
        "ID"
    ],
    "Chicago Behavioral Hospital": [
        "Des Plaines",
        "IL"
    ],
    "Crawford Memorial Hospital": [
        "Robinson",
        "IL"
    ],
    "Hillsboro Area Hospital": [
        "Hillsboro",
        "IL"
    ],
    "Hopedale Hospital": [
        "Hopedale",
        "IL"
    ],
    "Iroquois Memorial Hospital": [
        "Watseka",
        "IL"
    ],
    "Lake Behavioral Hospital": [
        "Waukegan",
        "IL"
    ],
    "Lawrence County Memorial Hospital": [
        "Lawrenceville",
        "IL"
    ],
    "McDonough District Hospital": [
        "Macomb",
        "IL"
    ],
    "Morrison Community Hospital": [
        "Morrison",
        "IL"
    ],
    "Paris Community Hospital": [
        "Paris",
        "IL"
    ],
    "Silver Oaks Behavioral Hospital": [
        "New Lenox",
        "IL"
    ],
    "Sparta Community Hospital": [
        "Sparta",
        "IL"
    ],
    "Thomas H Boyd Memorial Hospital": [
        "Carrollton",
        "IL"
    ],
    "Wabash General Hospital": [
        "Mt Carmel",
        "IL"
    ],
    "Cameron Memorial Community Hospital": [
        "Angola",
        "IN"
    ],
    "Incompass Healthcare": [
        "Lawrenceburg",
        "IN"
    ],
    "Unity Physicians Hospital": [
        "Mishawaka",
        "IN"
    ],
    "Ascentist Hospital": [
        "Leawood",
        "KS"
    ],
    "Clara Barton Hospital": [
        "Hoisington",
        "KS"
    ],
    "Coffey County Hospital": [
        "Burlington",
        "KS"
    ],
    "Community HealthCare System": [
        "Onaga",
        "KS"
    ],
    "Community Memorial Healthcare (Marysville)": [
        "Marysville",
        "KS"
    ],
    "Holton Community Hospital": [
        "Holton",
        "KS"
    ],
    "Rice Community Healthcare": [
        "Lyons",
        "KS"
    ],
    "Hospital District #6 Patterson Health Center": [
        "Anthony",
        "KS"
    ],
    "Kansas City Orthopaedic Institute": [
        "Leawood",
        "KS"
    ],
    "Kansas Medical Center": [
        "Andover",
        "KS"
    ],
    "Kansas Spine & Specialty Hospital": [
        "Wichita",
        "KS"
    ],
    "Manhattan Surgical Hospital": [
        "Manhattan",
        "KS"
    ],
    "Meade District Hospital": [
        "Meade",
        "KS"
    ],
    "Nemaha Valley Community Hospital": [
        "Seneca",
        "KS"
    ],
    "Norton County Hospital": [
        "Norton",
        "KS"
    ],
    "Osborne County Memorial Hospital": [
        "Osborne",
        "KS"
    ],
    "Phillips County Hospital": [
        "Phillipsburg",
        "KS"
    ],
    "Pratt Regional Medical Center": [
        "Pratt",
        "KS"
    ],
    "Scott County Hospital": [
        "Scott City",
        "KS"
    ],
    "Summit Surgical Hospital (Hutchinson)": [
        "Hutchinson",
        "KS"
    ],
    "William Newton Hospital": [
        "Winfield",
        "KS"
    ],
    "Caldwell Medical Center": [
        "Princeton",
        "KY"
    ],
    "Carroll County Memorial Hospital": [
        "Carrollton",
        "KY"
    ],
    "Kentucky River Medical Center": [
        "Jackson",
        "KY"
    ],
    "Livingston Hospital and Healthcare Services": [
        "Salem",
        "KY"
    ],
    "Ohio County Hospital": [
        "Hartford",
        "KY"
    ],
    "Trigg County Hospital": [
        "Cadiz",
        "KY"
    ],
    "Allen Parish Hospital": [
        "Kinder",
        "LA"
    ],
    "Avoyelles Hospital": [
        "Marksville",
        "LA"
    ],
    "Bunkie General Hospital": [
        "Bunkie",
        "LA"
    ],
    "Lafayette Surgical Specialty Hospital": [
        "Lafayette",
        "LA"
    ],
    "Opelousas General Health System": [
        "Opelousas",
        "LA"
    ],
    "Spine Hospital of Louisiana": [
        "Baton Rouge",
        "LA"
    ],
    "St. James Parish Hospital": [
        "Lutcher",
        "LA"
    ],
    "Surgical Specialty Center of Baton Rouge": [
        "Baton Rouge",
        "LA"
    ],
    "Winn Parish Medical Center": [
        "Winnfield",
        "LA"
    ],
    "Bournewood Hospital": [
        "Brookline",
        "MA"
    ],
    "Hospital for Behavioral Medicine": [
        "Worcester",
        "MA"
    ],
    "Cary Medical Center": [
        "Caribou",
        "ME"
    ],
    "Mount Desert Island Hospital": [
        "Bar Harbor",
        "ME"
    ],
    "Northern Maine Medical Center": [
        "Fort Kent",
        "ME"
    ],
    "Eaton Rapids Medical Center": [
        "EATON RAPIDS",
        "MI"
    ],
    "Hillsdale Hospital": [
        "Hillsdale",
        "MI"
    ],
    "Sheridan Community Hospital": [
        "Sheridan",
        "MI"
    ],
    "Straith Hospital for Special Surgery": [
        "Southfield",
        "MI"
    ],
    "Chippewa County Hospital": [
        "Montevideo",
        "MN"
    ],
    "Gillette Children's Specialty Healthcare": [
        "St Paul",
        "MN"
    ],
    "Glacial Ridge Hospital": [
        "Glenwood",
        "MN"
    ],
    "Kittson Healthcare": [
        "Hallock",
        "MN"
    ],
    "Lakewood Health System": [
        "Staples",
        "MN"
    ],
    "LifeCare Medical Center": [
        "Roseau",
        "MN"
    ],
    "Mahnomen Health Center": [
        "Mahnomen",
        "MN"
    ],
    "Mille Lacs Health System": [
        "Onamia",
        "MN"
    ],
    "Rainy Lake Medical Center": [
        "International Falls",
        "MN"
    ],
    "River's Edge Hospital & Clinic": [
        "Saint Peter",
        "MN"
    ],
    "RiverView Health": [
        "Crookston",
        "MN"
    ],
    "Riverwood Healthcare Center": [
        "Aitkin",
        "MN"
    ],
    "Sleepy Eye Medical Center": [
        "Sleepy Eye",
        "MN"
    ],
    "United Hospital District": [
        "Blue Earth",
        "MN"
    ],
    "Bates County Memorial Hospital": [
        "Butler",
        "MO"
    ],
    "Ranken Jordan Pediatric Bridge Hospital": [
        "Maryland Heights",
        "MO"
    ],
    "Ray County Memorial Hospital": [
        "Richmond",
        "MO"
    ],
    "Ste Genevieve County Memorial Hospital": [
        "STE. GENEVIEVE",
        "MO"
    ],
    "King's Daughters Medical Center-Brookhaven": [
        "Brookhaven",
        "MS"
    ],
    "South Sunflower County Hospital": [
        "Indianola",
        "MS"
    ],
    "Tyler Holmes Memorial Hospital": [
        "Winona",
        "MS"
    ],
    "Glendive Medical Center": [
        "Glendive",
        "MT"
    ],
    "Granite County Medical Center": [
        "Philipsburg",
        "MT"
    ],
    "Mineral Community Hospital": [
        "Superior",
        "MT"
    ],
    "Rosebud Health Care Center": [
        "Forsyth",
        "MT"
    ],
    "Shodair Children's Hospital": [
        "Helena",
        "MT"
    ],
    "Granville Health Systems": [
        "Oxford",
        "NC"
    ],
    "Iredell Davis Behavioral Health Hospital": [
        "Statesville",
        "NC"
    ],
    "Heart of America Medical Center": [
        "Rugby",
        "ND"
    ],
    "Jamestown Regional Medical Center": [
        "Jamestown",
        "ND"
    ],
    "McKenzie County Healthcare Systems": [
        "Watford City",
        "ND"
    ],
    "Sakakawea Medical Center - Cah": [
        "HAZEN",
        "ND"
    ],
    "St. Luke's Hospital (Crosby)": [
        "Crosby",
        "ND"
    ],
    "Towner County Medical Center": [
        "Cando",
        "ND"
    ],
    "Antelope Memorial Hospital": [
        "NELIGH",
        "NE"
    ],
    "Beatrice Community Hospital & Health Center": [
        "Beatrice",
        "NE"
    ],
    "Boone County Health Center": [
        "Albion",
        "NE"
    ],
    "Box Butte General Hospital": [
        "Alliance",
        "NE"
    ],
    "Brodstone Healthcare": [
        "Superior",
        "NE"
    ],
    "Chadron Community Hospital and Health Services": [
        "Chadron",
        "NE"
    ],
    "Cherry County Hospital": [
        "Valentine",
        "NE"
    ],
    "Community Hospital (McCook)": [
        "McCook",
        "NE"
    ],
    "Fillmore County Hospital": [
        "Geneva",
        "NE"
    ],
    "Gordon Memorial Hospital District": [
        "Gordon",
        "NE"
    ],
    "Jefferson Community Health & Life": [
        "Fairbury",
        "NE"
    ],
    "Kearney County Health Services Hospital": [
        "Minden",
        "NE"
    ],
    "Kimball Health Services": [
        "Kimball",
        "NE"
    ],
    "Lexington Regional Health Center": [
        "Lexington",
        "NE"
    ],
    "Lincoln Surgical Hospital": [
        "Lincoln",
        "NE"
    ],
    "Memorial Hospital (Aurora)": [
        "Aurora",
        "NE"
    ],
    "Morrill County Community Hospital": [
        "Bridgeport",
        "NE"
    ],
    "Phelps Memorial Health Center": [
        "Holdrege",
        "NE"
    ],
    "Providence Medical Center (Wayne)": [
        "Wayne",
        "NE"
    ],
    "Syracuse Area Health": [
        "Syracuse",
        "NE"
    ],
    "Valley County Health System": [
        "Ord",
        "NE"
    ],
    "Webster County Community Hospital": [
        "Red Cloud",
        "NE"
    ],
    "York General Health Care Services": [
        "York",
        "NE"
    ],
    "Huggins Hospital": [
        "Wolfeboro",
        "NH"
    ],
    "Littleton Regional Healthcare": [
        "Littleton",
        "NH"
    ],
    "Bergen New Bridge Medical Center": [
        "Paramus",
        "NJ"
    ],
    "Carewell Health Medical Center": [
        "East Orange",
        "NJ"
    ],
    "Artesia General Hospital": [
        "Artesia",
        "NM"
    ],
    "Central Desert Behavioral Health Hospital": [
        "Albuquerque",
        "NM"
    ],
    "Cibola General Hospital": [
        "Grants",
        "NM"
    ],
    "Gila Regional Medical Center": [
        "Silver City",
        "NM"
    ],
    "Holy Cross Medical Center (Taos)": [
        "Taos",
        "NM"
    ],
    "Nor-Lea Hospital District": [
        "Lovington",
        "NM"
    ],
    "Roosevelt General Hospital": [
        "Portales",
        "NM"
    ],
    "Three Crosses Regional Hospital": [
        "Las Cruces",
        "NM"
    ],
    "Desert Parkway Behavioral Healthcare Hospital": [
        "Las Vegas",
        "NV"
    ],
    "Harmon Hospital": [
        "Las Vegas",
        "NV"
    ],
    "Pershing General Hospital": [
        "Lovelock",
        "NV"
    ],
    "Brylin Hospitals": [
        "Buffalo",
        "NY"
    ],
    "Nathan Littauer Hospital": [
        "Gloversville",
        "NY"
    ],
    "Adams County Regional Medical Center": [
        "Seaman",
        "OH"
    ],
    "Blueridge Vista Health and Wellness": [
        "Cincinnati",
        "OH"
    ],
    "Mary Rutan Hospital": [
        "Bellefontaine",
        "OH"
    ],
    "Ohio Valley Surgical Hospital": [
        "Springfield",
        "OH"
    ],
    "Summa Western Reserve Hospital": [
        "Cuyahoga Falls",
        "OH"
    ],
    "Sunrise Vista Health and Wellness": [
        "Canton",
        "OH"
    ],
    "Wayne Hospital": [
        "Greenville",
        "OH"
    ],
    "Wood County Hospital": [
        "Bowling Green",
        "OH"
    ],
    "Wyandot Memorial Hospital": [
        "Upper Sandusky",
        "OH"
    ],
    "Atoka County Medical Center": [
        "Atoka",
        "OK"
    ],
    "The Children's Center Rehabilitation Hospital": [
        "Bethany",
        "OK"
    ],
    "Cleveland Area Hospital": [
        "Cleveland",
        "OK"
    ],
    "Coal County General Hospital": [
        "Coalgate",
        "OK"
    ],
    "Jackson County Memorial Hospital Authority": [
        "Altus",
        "OK"
    ],
    "McAlester Regional Health Center": [
        "McAlester",
        "OK"
    ],
    "McCurtain Memorial Hospital": [
        "Idabel",
        "OK"
    ],
    "Stilwell Memorial Hospital": [
        "Stilwell",
        "OK"
    ],
    "Oklahoma Heart Hospital": [
        "Oklahoma City",
        "OK"
    ],
    "Oklahoma Surgical Hospital": [
        "Tulsa",
        "OK"
    ],
    "OneCore Health": [
        "Oklahoma City",
        "OK"
    ],
    "Parkside Psychiatric Hospital": [
        "Tulsa",
        "OK"
    ],
    "Pawhuska Hospital": [
        "Pawhuska",
        "OK"
    ],
    "Summit Medical Center (Edmond)": [
        "Edmond",
        "OK"
    ],
    "Surgical Hospital of Oklahoma": [
        "Oklahoma City",
        "OK"
    ],
    "Wagoner Community Hospital": [
        "Wagoner",
        "OK"
    ],
    "Curry General Hospital": [
        "Gold Beach",
        "OR"
    ],
    "Harney District Hospital": [
        "Burns",
        "OR"
    ],
    "Southern Coos Hospital & Health Center": [
        "Bandon",
        "OR"
    ],
    "Eagleville Hospital": [
        "Eagleville",
        "PA"
    ],
    "OSS Orthopaedic Hospital": [
        "York",
        "PA"
    ],
    "Newberry County Memorial Hospital": [
        "Newberry",
        "SC"
    ],
    "Brookings Health System": [
        "Brookings",
        "SD"
    ],
    "Huron Regional Medical Center": [
        "Huron",
        "SD"
    ],
    "Lifescape": [
        "Sioux Falls",
        "SD"
    ],
    "Prairie Lakes Healthcare System": [
        "Watertown",
        "SD"
    ],
    "Macon Community Hospital": [
        "Lafayette",
        "TN"
    ],
    "Metro Nashville General Hospital": [
        "Nashville",
        "TN"
    ],
    "Collingsworth General Hospital": [
        "Wellington",
        "TX"
    ],
    "Hamilton General Hospital": [
        "Hamilton",
        "TX"
    ],
    "McCamey Hospital": [
        "McCamey",
        "TX"
    ],
    "StarCare Specialty Health": [
        "Lubbock",
        "TX"
    ],
    "Blue Mountain Hospital": [
        "Blanding",
        "UT"
    ],
    "Kane County Hospital": [
        "Kanab",
        "UT"
    ],
    "Uintah Basin Medical Center": [
        "Roosevelt",
        "UT"
    ],
    "Gifford Medical Center": [
        "Randolph",
        "VT"
    ],
    "Springfield Hospital": [
        "Springfield",
        "VT"
    ],
    "Columbia Basin Hospital": [
        "Ephrata",
        "WA"
    ],
    "Coulee Medical Center": [
        "Grand Coulee",
        "WA"
    ],
    "Garfield County Public Hospital District #1": [
        "Pomeroy",
        "WA"
    ],
    "Kittitas Valley Community Hospital": [
        "Ellensburg",
        "WA"
    ],
    "Klickitat Valley Hospital": [
        "Goldendale",
        "WA"
    ],
    "Lake Chelan Community Hospital": [
        "CHELAN",
        "WA"
    ],
    "Mason General Hospital & Family of Clinics": [
        "Shelton",
        "WA"
    ],
    "Mid Valley Hospital & Clinic": [
        "Omak",
        "WA"
    ],
    "Newport Community Hospital": [
        "Newport",
        "WA"
    ],
    "North Valley Hospital": [
        "Tonasket",
        "WA"
    ],
    "Othello Community Hospital": [
        "Othello",
        "WA"
    ],
    "Smokey Point Behavioral Hospital": [
        "Marysville",
        "WA"
    ],
    "Snoqualmie Valley Hospital": [
        "Snoqualmie",
        "WA"
    ],
    "South Sound Behavioral Hospital": [
        "Lacey",
        "WA"
    ],
    "Summit Pacific Medical Center": [
        "Elma",
        "WA"
    ],
    "WhidbeyHealth Medical Center": [
        "Coupeville",
        "WA"
    ],
    "Willapa Harbor Hospital": [
        "South Bend",
        "WA"
    ],
    "Burnett Medical Center": [
        "Grantsburg",
        "WI"
    ],
    "Crossing Rivers Health Medical Center": [
        "Prairie du Chien",
        "WI"
    ],
    "Fort Memorial Hospital": [
        "Fort Atkinson",
        "WI"
    ],
    "Grant Regional Health Center": [
        "Lancaster",
        "WI"
    ],
    "North Central Health Care": [
        "Wausau",
        "WI"
    ],
    "Oakleaf Surgical Hospital": [
        "Altoona",
        "WI"
    ],
    "Richland Hospital": [
        "Richland Center",
        "WI"
    ],
    "Sauk Prairie Hospital": [
        "Prairie du Sac",
        "WI"
    ],
    "Southwest Health Center": [
        "Platteville",
        "WI"
    ],
    "Spooner Health System": [
        "Spooner",
        "WI"
    ],
    "Vernon Memorial Hospital": [
        "Viroqua",
        "WI"
    ],
    "Boone Memorial Hospital": [
        "Madison",
        "WV"
    ],
    "Cody Regional Health": [
        "Cody",
        "WY"
    ],
    "Hot Springs County Memorial Hospital": [
        "Thermopolis",
        "WY"
    ],
    "Memorial Hospital Sweetwater County": [
        "Rock Springs",
        "WY"
    ],
    "Memorial Hospital of Carbon County": [
        "Rawlins",
        "WY"
    ],
    "Niobrara County Hospital District": [
        "Lusk",
        "WY"
    ],
    "North Platte Valley Medical Center": [
        "Saratoga",
        "WY"
    ],
    "Star Valley Medical Center": [
        "Afton",
        "WY"
    ]
}

PAYCOM_STATE = {
    "Mena Regional Health System": "AR",
    "North Arkansas Regional Medical Center": "AR",
    "Destiny Springs Healthcare": "AZ",
    "La Paz Regional Hospital": "AZ",
    "Sage Memorial Hospital": "AZ",
    "Healthbridge Children's Hospital - Orange": "CA",
    "Mammoth Hospital": "CA",
    "Modoc Medical Center": "CA",
    "Palo Verde Hospital": "CA",
    "Kit Carson County Memorial Hospital": "CO",
    "Lincoln Health Hospital": "CO",
    "Pagosa Springs Medical Center": "CO",
    "Connecticut Hospice": "CT",
    "Eastside Psychiatric Hospital": "FL",
    "Candler County Hospital": "GA",
    "Emanuel Medical Center (Swainsboro)": "GA",
    "Evans Memorial Hospital": "GA",
    "Jeff Davis Hospital (Hazlehurst)": "GA",
    "Ridgeview Institute Monroe": "GA",
    "Boundary Community Hospital": "ID",
    "Chicago Behavioral Hospital": "IL",
    "Lake Behavioral Hospital": "IL",
    "Silver Oaks Behavioral Hospital": "IL",
    "Cameron Memorial Community Hospital": "IN",
    "Amberwell Health": "KS",
    "Coffey County Hospital": "KS",
    "Rice Community Healthcare": "KS",
    "Kansas City Orthopaedic Institute": "KS",
    "Manhattan Surgical Hospital": "KS",
    "Osborne County Memorial Hospital": "KS",
    "Caldwell Medical Center": "KY",
    "Carroll County Memorial Hospital": "KY",
    "Livingston Hospital and Healthcare Services": "KY",
    "Ohio County Hospital": "KY",
    "Trigg County Hospital": "KY",
    "Surgical Specialty Center of Baton Rouge": "LA",
    "Bournewood Hospital": "MA",
    "Hospital for Behavioral Medicine": "MA",
    "Sheridan Community Hospital": "MI",
    "Glacial Ridge Hospital": "MN",
    "Lakewood Health System": "MN",
    "McKenzie County Healthcare Systems": "ND",
    "Brodstone Healthcare": "NE",
    "Fillmore County Hospital": "NE",
    "Gordon Memorial Hospital District": "NE",
    "Lincoln Surgical Hospital": "NE",
    "Syracuse Area Health": "NE",
    "Artesia General Hospital": "NM",
    "Brylin Hospitals": "NY",
    "Mary Rutan Hospital": "OH",
    "Atoka County Medical Center": "OK",
    "The Children's Center Rehabilitation Hospital": "OK",
    "Coal County General Hospital": "OK",
    "Stilwell Memorial Hospital": "OK",
    "OneCore Health": "OK",
    "Parkside Psychiatric Hospital": "OK",
    "Pawhuska Hospital": "OK",
    "Summit Medical Center (Edmond)": "OK",
    "Eagleville Hospital": "PA",
    "Malvern Behavioral Health": "PA",
    "Coulee Medical Center": "WA",
    "Klickitat Valley Hospital": "WA",
    "Smokey Point Behavioral Hospital": "WA",
    "South Sound Behavioral Hospital": "WA",
    "Willapa Harbor Hospital": "WA",
    "Boone Memorial Hospital": "WV",
    "Memorial Hospital of Carbon County": "WY"
}


def _norm(v):
    return tuple(v) if isinstance(v, list) else v


@pytest.mark.parametrize("table", sorted(NEW))
def test_new_configs_present_once(table):
    d = getattr(scraper, table)
    for system, cfg in NEW[table].items():
        assert d[system] == _norm(cfg), (table, system)


def test_every_new_board_has_its_town_as_location_default():
    for system, (city, state) in LOCATION.items():
        assert scraper.SYSTEM_LOCATION_DEFAULTS[system.lower()] == (city, state), system
        assert city and re.fullmatch(r"[A-Z]{2}", state)


def test_paycom_boards_outside_texas_default_to_their_state():
    for system, st in PAYCOM_STATE.items():
        assert scraper.PAYCOM_DEFAULT_STATE[system] == st
    for system in NEW.get("PAYCOM_ORGS", {}):
        job = scraper._paycom_job({"jobId": "1", "jobTitle": "RN"}, None, system, "K",
                                  scraper.PAYCOM_DEFAULT_STATE.get(system, "TX"))
        assert job.state == PAYCOM_STATE.get(system, "TX")


def test_config_shapes_match_their_adapters():
    for system, (guid, slug, st) in NEW.get("PAYLOCITY_ORGS", {}).items():
        assert re.fullmatch(r"[0-9a-f-]{36}", guid) and slug and len(st) == 2, system
    for system, key in NEW.get("PAYCOM_ORGS", {}).items():
        assert re.fullmatch(r"[0-9A-F]{32}", key), system
    for system, (base, guid, st) in NEW.get("UKG_ORGS", {}).items():
        assert re.match(r"https://(recruiting2?\.ultipro\.com|[a-z0-9]+\.rec\.pro\.ukg\.net)/[A-Za-z0-9]+$", base), system
        assert re.fullmatch(r"[0-9a-f-]{36}", guid) and len(st) == 2
    for system, (sub, company, st) in NEW.get("KRONOS_ORGS", {}).items():
        assert company.isdigit() and len(st) == 2, system
        assert scraper._kronos_base(sub).startswith("https://")
    for system, value in NEW.get("ADP_ORGS", {}).items():
        centers = scraper._adp_centers(_norm(value))
        assert re.fullmatch(r"[0-9a-f-]{36}", centers[0][0]) and centers[0][2], system
    for system, (sub, dom) in NEW.get("APPLICANTPRO_ORGS", {}).items():
        assert re.fullmatch(r"[a-z0-9-]+", sub) and (dom == "" or dom.isdigit()), system
    for system, (tenant, wd, site) in NEW.get("WORKDAY_TENANTS", {}).items():
        assert wd.isdigit() and tenant and site, system
    for system, (slug, st) in NEW.get("WORKABLE_ORGS", {}).items():
        assert slug and len(st) == 2, system


def test_no_board_is_configured_twice():
    keys = {
        "PAYCOM_ORGS": lambda v: v, "PAYLOCITY_ORGS": lambda v: v[0], "UKG_ORGS": lambda v: v[1],
        "KRONOS_ORGS": lambda v: v[1], "APPLICANTPRO_ORGS": lambda v: v[0], "HEALTHCARESOURCE_ORGS": lambda v: v,
        "WORKDAY_TENANTS": lambda v: (v[0], v[2]), "ICIMS_ORGS": lambda v: v,
    }
    for table, key in keys.items():
        if table not in NEW:
            continue
        # a Workday board is a tenant + site: Avera's tenant also hosts Brookings Health System's own site
        allk = [str(key(v)).lower() for v in getattr(scraper, table).values() if isinstance(v, (str, tuple))]
        for system, cfg in NEW[table].items():
            assert allk.count(str(key(_norm(cfg))).lower()) == 1, (table, system)


def _job(system, city="", state="", loc=""):
    return scraper.Job(title="Registered Nurse", hospital_system=system, hospital_name=system, city=city, state=state,
                       location=loc, specialty="", job_type="", url="https://example.org/1", job_id="1",
                       posted_date="", description="", ats_platform="Workday")


def test_blank_location_rows_take_the_hospitals_town():
    # Wyandot's Workday board writes "Hospital" as the location; Henry Mayo's Taleo feed has no city
    d = scraper.normalize_job(_job("Wyandot Memorial Hospital"))
    assert (d["city"], d["state"]) == ("Upper Sandusky", "OH")
    d = scraper.normalize_job(_job("Henry Mayo Newhall Hospital", state="CA"))
    assert (d["city"], d["state"]) == ("Valencia", "CA")


def test_multi_town_boards_have_no_single_town_default():
    # these boards list several hospitals or towns; a blank row must not be filed in one of them
    for system in ("Muscogee Nation Health", "Tamarack Health", "Optim Healthcare", "Iowa Specialty Hospital",
                   "The CORE Institute", "Centerstone", "Mississippi County Hospital System", "Amberwell Health"):
        assert system.lower() not in scraper.SYSTEM_LOCATION_DEFAULTS, system


def test_optim_names_each_hospital_by_town():
    centers = scraper._adp_centers(scraper.ADP_ORGS["Optim Healthcare"])
    assert centers[0][3] == {"Sylvania": "Optim Medical Center - Screven", "Reidsville": "Optim Medical Center - Tattnall"}


def test_ukg_ready_white_label_host():
    sub, company, st = scraper.KRONOS_ORGS["San Carlos Apache Healthcare"]
    assert scraper._kronos_base(sub) == "https://secure6.entertimeonline.com" and company == "6144973" and st == "AZ"


def test_new_labels_do_not_reuse_an_existing_system_name():
    # generic names carry their town ("Memorial Hospital" is Memorial Health's Oracle label)
    assert "Memorial Hospital (Aurora)" in scraper.PAYLOCITY_ORGS
    assert scraper.ORACLE_ORGS["Memorial Hospital"][0].startswith("https://wearememorial")
    assert "Gunnison Valley Hospital" in scraper.PAYLOCITY_ORGS and "Gunnison Valley Health" in scraper.UKG_ORGS
