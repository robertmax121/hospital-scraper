"""The site on UnitedHealth Group rows (2026-10-29, facility scale phase 3).

The owner, 2026-10-29: "unitedhealth group hospital list is incomplete" and
"i thought you were gonna edit all the large multihospital systems". His
decisions for every system: (1) cards and job pages show the site ("Apply to
<site>"), the system name and logo stay on the job page and the system hub;
(2) named sites (hospitals, named clinics and centers, agencies' named
branches) show their own name; generic offices, home-based and remote jobs
show the system name with the town; never guess a town.

careers.unitedhealthgroup.com (TalentBrew company 34088, scrape_uhg_talentbrew)
stamps every posting "UnitedHealth Group". UHG owns no US hospital; its
postings are for its businesses: home health and hospice agencies (LHC Group's
local agencies, Amedisys), medical groups and clinics (Kelsey-Seybold Clinic,
WellMed, Atrius Health, Reliant Medical Group, Optum NY ...), Genoa Healthcare
and CPS pharmacies, behavioral practices, and the corporate, insurance
(UnitedHealthcare) and technology offices. The list card and the job page give
only the town ("Primary location") and the posting; there is no facility
field (read 2026-10-29: job page meta, JSON-LD hiringOrganization is always
"UnitedHealth Group", gtm_tbcn_location is the town). The posting names the
business in its opening line, in a few fixed templates:

  "Explore opportunities with Centralia Assured Home Health, a part of LHC Group, ..."
  "Explore opportunities with Amedisys Hospice, a leading post-acute ..."
  "Explore opportunities with Kelsey-Seybold Clinic, part of the Optum family ..."
  "Opportunities with Genoa Healthcare. A career with Genoa ..."
  "WellMed, part of the Optum family of businesses, is seeking ..."
  "A partner of the Optum Care network, WellMed Medical Group is seeking ..."
  "Join Amedisys , one of the nation's most trusted ..."
  "Optum NY, (formerly Optum Tri-State NY) is seeking ..."

How a row is named (uhg_site):
  1. remote: a title that says Remote, Telecommute, Work from home, Virtual,
     Home-based or Nationwide keeps the system name (decision 2);
  2. the opening line: the first template in the first 1,500 characters that
     names a business. A business in BRANDS is its clean name (a site), or the
     system name (CORP: UHG, UnitedHealthcare, bare Optum and its corporate
     units, where the title may still name the clinic; LABEL: house calls
     (home-based), "LHC Group" alone (no agency named), another system's own
     name, an internal branch code or a placeholder). A home health / hospice
     agency of the LHC Group template ("<agency>, a part of LHC Group") is its
     own name: the spelling AGENCIES folds it to, else the name as printed when
     it reads as an agency name (AGENCY_WORDS, no internal code, no
     placeholder);
  3. the title: a medical group or clinic in TITLE_BRANDS ("OB/GYN Physician -
     Kelsey-Seybold - Katy", "X-Ray Technologist - Crystal Run Healthcare")
     when the opening line named none (or only a CORP name); a title naming two
     different groups names none;
  4. otherwise the system name, with the row's own town.
A name read from the template is never fuzzy-matched; an unknown name outside
the LHC Group template is counted in the run log (UNKNOWN) and names nothing.

Towns: a row keeps its own town (the posting's Primary location). Two fills,
both from the posting's own words: a blank state when the business works in
one state only (FOOTPRINT: Kelsey-Seybold Clinic is Greater Houston, Texas),
and the town when the stored "town" is a state ("Texas, TX") and the title or
the opening line names a town of that state ("Primary Care Physician - WellMed
Medical Group - Harlingen, TX"). No town is guessed.

Nights without a body: a known body is sent blank (scraper._keep_stored_body)
and the enrichment trigger keeps the stored one; on those nights the name
stored on the row is kept (load_known_facilities), and a town this module
had written over a state-named town too. A night whose own body names
nothing leaves the system name.

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/uhg_work/changes_uhg.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import html as _html
import json
import logging
import os
import re
import time

import sb_keys  # the DATA project key, one reader for the repo
from facility_names import clean_text

logger = logging.getLogger("scraper.facility_uhg")

SYSTEM = "UnitedHealth Group"
SITE, CORP, LABEL = "site", "corp", "label"
HEAD_CHARS = 1500          # the opening line sits in the first paragraph or two
NO_BODY_CHARS = 600        # under this the row carries no body tonight


def fold(s) -> str:
    """Lookup key: case, punctuation, "&"/"and", "'s", "Saint"/"St" and
    "HomeCare"/"Home Care" aside."""
    s = clean_text(_html.unescape(str(s or ""))).lower()
    s = s.replace("&", " and ").replace("®", " ").replace("@", " at ")
    s = re.sub(r"'s\b", "s", s).replace("'", "")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bhome\s+care\b", "homecare", s)
    return " ".join(s.split())


# ── The businesses (opening-line names and their clean public names) ────────
# (clean name, kind, footprint state or "", spellings...). A footprint state
# fills a blank state only (FOOTPRINT); "" = several states.
_BRANDS = (
    # home health and hospice (Amedisys, acquired 2025; branches go by its name)
    ("Amedisys", SITE, "", "Amedisys", "Amedisys Inc", "Amedisys Home Health and Hospice"),
    ("Amedisys Home Health", SITE, "", "Amedisys Home Health"),
    ("Amedisys Hospice", SITE, "", "Amedisys Hospice"),
    # medical groups and clinics (Optum Care Delivery)
    ("Kelsey-Seybold Clinic", SITE, "TX", "Kelsey-Seybold Clinic", "Kelsey-Seybold", "Kelsey Seybold Clinic",
     "Kelsey Seybold Clinics", "Kelsey-Seybold Clinics", "Kelsey Seybold"),
    ("Atrius Health", SITE, "MA", "Atrius Health"),
    ("WellMed", SITE, "", "WellMed", "WellMed Medical Group"),
    ("Reliant Medical Group", SITE, "MA", "Reliant Medical Group", "Reliant", "Reliant Medical Group part of Optum"),
    ("ProHealth Physicians", SITE, "CT", "ProHealth Physicians", "ProHealth Physicians CT"),
    ("Optum NY", SITE, "NY", "Optum NY", "Optum NY/NJ", "Optum Tri-State NY", "Optum in the Tri-State region",
     "Optum New York"),
    ("Optum NJ", SITE, "NJ", "Optum NJ", "Optum New Jersey"),
    ("Optum CA", SITE, "CA", "Optum CA", "Optum California"),
    ("Optum NV", SITE, "NV", "Optum NV", "OptumCare Nevada", "Optum Nevada"),
    ("Optum AZ", SITE, "AZ", "Optum AZ", "Optum Arizona"),
    ("Optum Colorado", SITE, "CO", "Optum Colorado", "OptumCare Colorado", "Optum CO", "Optum in Colorado"),
    ("Optum NM", SITE, "NM", "Optum NM", "Optum New Mexico"),
    ("Optum Washington", SITE, "WA", "Optum Washington", "Optum WA"),
    ("Optum Oregon", SITE, "OR", "Optum Oregon"),
    ("OptumCare Cancer Care", SITE, "NV", "OptumCare Cancer Care", "Optum Cancer Care"),
    ("Optum Care Community Center of Nevada", SITE, "NV", "Optum Care Community Center of Nevada"),
    ("Southwest Medical", SITE, "NV", "Southwest Medical", "Southwest Medical Associates"),
    ("Optum Behavioral Care", SITE, "", "Optum Behavioral Care"),
    ("American Health Network", SITE, "", "American Health Network", "AHN"),
    ("The Corvallis Clinic", SITE, "OR", "The Corvallis Clinic", "Corvallis Clinic"),
    ("Oregon Medical Group", SITE, "OR", "Oregon Medical Group"),
    ("Crystal Run Health", SITE, "NY", "Crystal Run Health", "Crystal Run Healthcare"),
    ("Acton Medical Associates", SITE, "MA", "Acton Medical Associates"),
    ("Colonial Healthcare", SITE, "SC", "Colonial Healthcare"),
    ("Unity Health Network", SITE, "OH", "Unity Health Network"),
    ("Encompass Medical Group", SITE, "", "Encompass Medical Group"),
    # pharmacies
    ("Genoa Healthcare", SITE, "", "Genoa Healthcare", "Genoa"),
    ("CPS", SITE, "", "CPS", "CPS Solutions"),
    ("PharmScript", SITE, "", "PharmScript"),
    # behavioral health practices
    ("Springfield Behavioral of Pennsylvania", SITE, "PA", "Springfield Behavioral of Pennsylvania",
     "Springfield Behavioral of Pennsylvania part of the Optum family of businesses"),
    ("Family Counseling Associates of Massachusetts", SITE, "MA", "Family Counseling Associates of Massachusetts"),
    ("Family Counseling Associates of New Hampshire", SITE, "NH", "Family Counseling Associates of New Hampshire"),
    ("Family Counseling Associates", SITE, "", "Family Counseling Associates"),
    ("Tides of Mind Counseling", SITE, "CT", "Tides of Mind Counseling"),
    ("Carroll Counseling", SITE, "MD", "Carroll Counseling"),
    ("Life Changes Group", SITE, "MA", "Life Changes Group"),
    ("Psychiatry Services of New York", SITE, "NY", "Psychiatry Services of New York"),
    ("Life Solutions", SITE, "", "Life Solutions"),
    ("Comprehensive MedPsych Systems", SITE, "FL", "Comprehensive MedPsych Systems", "Comprehensive Med Psych Systems",
     "CMPS"),
    ("Heron Ridge Associates", SITE, "MI", "Heron Ridge Associates"),
    ("Snyder Therapeutic Services of Pennsylvania", SITE, "PA", "J. Snyder Therapeutic Services of Pennsylvania"),
    ("Capstone Behavioral Health", SITE, "NE", "Capstone Behavioral Health"),
    ("Elevate Mental Health", SITE, "AZ", "Elevate Mental Health"),
    ("CBT Counseling of North Carolina", SITE, "NC", "CBT Counseling of North Carolina"),
    ("Affirmations Psychological Services", SITE, "OH", "Affirmations Psychological Services"),
    ("CARE Counseling", SITE, "MN", "CARE Counseling"),
    ("Perspectives Counseling Centers", SITE, "MI", "Perspectives Counseling Centers"),
    ("Whole Health Solutions", SITE, "", "Whole Health Solutions"),
    ("Urban Counseling Collective", SITE, "OR", "Urban Counseling Collective"),
    ("A Better Way Therapy", SITE, "NE", "A Better Way Therapy"),
    ("Bearden Behavioral Health of Tennessee", SITE, "TN", "Bearden Behavioral Health of Tennessee",
     "Bearden of Tennessee", "Bearden of TN"),
    ("Sundance Behavioral Health", SITE, "UT", "Sundance Behavioral Health"),
    ("Carolina Behavioral Care", SITE, "NC", "Carolina Behavioral Care"),
    ("Tomorrow Begins Today Counseling of Ohio", SITE, "OH", "Tomorrow Begins Today Counseling of Ohio"),
    # the system name, the title may still name the clinic (bare Optum and the corporate units)
    ("UnitedHealth Group", CORP, "", "UnitedHealth Group", "UnitedHealth Group and its affiliated brands",
     "UnitedHealthcare", "UnitedHealthcare Community and State", "UMR", "Optum", "Optum Health", "Optum Care",
     "OptumCare", "Optum Insight", "Optum Rx", "OptumRx", "Optum Tech", "Optum Technology", "Optum Health Technology",
     "Optum Financial", "Optum Bank", "Optum Life Sciences", "Optum Medical Group", "The Optum family of businesses",
     "Optum family of businesses", "ProHealth", "Optum On-Site Solutions", "Optum Infusion", "Optum Home Delivery",
     "Optum Specialty Pharmacy", "Optum Frontier Therapies"),
    # the system name, final: house calls (home-based), a parent with no agency
    # named, another system's own name, an office, two groups at once
    ("UnitedHealth Group", LABEL, "", "Optum Home and Community Care", "Optum Home & Community Care",
     "LHC Group", "LHC", "LHC Group Inc", "Home Office", "WellMed Medical Management",
     "Logistics Health Incorporated", "Logistics Health", "LHI", "Transplant Resource Solutions",
     "Southwest Medical Referral Office", "Atrius Health and Reliant Medical Group",
     "Northern Light Health", "Owensboro Health", "UP Health System", "UP Health", "UP Health Systems",
     "UP Health System Menominee", "UP Health Escanaba", "St. Mary's", "Capital Region", "Eunice", "GPA"),
)


class Brand:
    __slots__ = ("name", "kind", "state")

    def __init__(self, name, kind, state):
        self.name, self.kind, self.state = name, kind, state

    def __repr__(self):
        return f"Brand({self.name!r}, {self.kind!r}, {self.state!r})"


BRANDS: dict[str, Brand] = {}
FOOTPRINT: dict[str, str] = {}
for _name, _kind, _st, *_spellings in _BRANDS:
    _b = Brand(_name if _kind == SITE else SYSTEM, _kind, _st)
    if _kind == SITE and _st:
        FOOTPRINT[_name] = _st
    for _sp in ((_name,) if _kind == SITE else ()) + tuple(_spellings):
        _k = fold(_sp)
        _old = BRANDS.get(_k)
        if _old is not None and (_old.name, _old.kind) != (_b.name, _b.kind):
            raise ValueError(f"spelling {_sp!r} names {_old!r} and {_b!r}")
        BRANDS[_k] = _b


# ── Home health and hospice agencies (the LHC Group template) ───────────────
# Every agency name the stored rows' LHC Group template printed (read
# 2026-10-29, 1,776 active rows, ~390 spellings), folded to one spelling per
# agency: the spelling most rows print, case and punctuation variants and
# typos ("Floruda Home Health", "Wiilcare") onto it. A branch suffix the
# posting prints ("CHRISTUS HomeCare - Tyler", "Mederi Caretenders Viera") is
# that branch's name and stays. An agency the table does not list yet is
# named as printed when it reads as an agency name (_agency_name).
_AGENCIES = (
    # (clean name, spellings that fold elsewhere...)
    ("Aberdeen Assured Home Health",),
    ("Acadian HomeCare",),
    ("Acadian Palliative Care",),
    ("Access Home Health",),
    ("Access Hospice Care",),
    ("Access Personal Care Services",),
    ("ACMC Family Home Health",),
    ("Alabama HomeCare of Fort Payne",),
    ("Alabama HomeCare of Oneonta",),
    ("Alabama Hospice Care",),
    ("Alabama Hospice Care of Birmingham",),
    ("Alabama Hospice Care of East Alabama",),
    ("Alabama Hospice Care of Mobile",),
    ("Alabama Hospice of the Shoals",),
    ("Almost Family",),
    ("Almost Family Personal Care",),
    ("Always Better Care", "Always Better Care Homecare"),
    ("Angels of Mercy Homecare", "Angels of Mercy"),
    ("Angels of Mercy Homecare - Marion",),
    ("Apex Companion Care",),
    ("Apex Health and Rehab Carrollton",),
    ("Apex Health and Rehab Harbison",),
    ("Apex Home Healthcare", "Apex Home Health"),
    ("Archbold Home Health",),
    ("Archbold Home Health in Bainbridge",),
    ("Archbold Home Health Services",),
    ("Archbold Home Health Thomasville",),
    ("Arlington Assured Home Health",),
    ("Assured Home Health",),
    ("Assured Home Health & Hospice",),
    ("Assured Home Health at Moses Lake", "Moses Lake Assured Home Health"),
    ("Assured Home Health of Clinton",),
    ("Assured Home Health of Ponca City",),
    ("Assured Home Health of Woodward",),
    ("Assured Home Health Tucson",),
    ("Assured Hospice",),
    ("Assured Hospice of Moses Lake",),
    ("Assured Hospice of Olympia",),
    ("At Home Health",),
    ("At Home Healthcare",),
    ("At Home Hospice",),
    ("At Home Hospice of Rockwall",),
    ("Athens Limestone HomeCare",),
    ("Atmore Community HomeCare",),
    ("Atrinity Home Health",),
    ("Baptist Home Care",),
    ("Baptist Home Care Southaven",),
    ("Baptist Home Health", "Baptist Home Health Care"),
    ("Baptist Hospice",),
    ("Baptist Memorial Hospice",),
    ("Baptist Memorial Palliative Care in Huntingdon",),
    ("Baptist Reynold's Hospice House",),
    ("Baptist Trinity Home Care",),
    ("Baptist Trinity Home Care & Hospice",),
    ("Baptist Trinity Hospice",),
    ("Beauregard Home Health",),
    ("Better @ Home", "Better @ Home OCA"),
    ("Blossom Ridge Home Health", "Blossom Ridge Home Health Agency"),
    ("Boone Memorial HomeCare",),
    ("Brandywine River Valley Hospice",),
    ("Bunkie HomeCare",),
    ("Cambridge Caretenders Home Health", "Cambridge Caretenders"),
    ("Cambridge Home Health",),
    ("Cape Fear Valley Home Health",),
    ("Cape Fear Valley Hospice",),
    ("Capital Region Home Health", "Capital Regional Home Health"),
    ("Care Partners Home Health",),
    ("Caretenders", "Caretenders - New Albany IN", "Cartenders"),
    ("Caretenders Holyoke",),
    ("Caretenders Home Health",),
    ("Caretenders Hospice",),
    ("Caretenders VNA",),
    ("Caretenders Westborough",),
    ("Caretenders Wilmington",),
    ("Carolina Home Care",),
    ("Casa de la Luz",),
    ("Casa de la Luz East Valley",),
    ("Casa de la Luz Hospice",),
    ("Casa de la Luz Palliative Care",),
    ("Casa de la Paz Hospice",),
    ("Casa de la Paz Hospice of Sierra Vista",),
    ("Central Missouri Home Health",),
    ("Central Missouri Hospice",),
    ("Centralia Assured Home Health",),
    ("CHRISTUS Good Shepherd HomeCare",),
    ("CHRISTUS Good Shepherd HomeCare - Longview",),
    ("CHRISTUS Home Health", "CHRISTUS Home Healh"),
    ("CHRISTUS HomeCare",),
    ("CHRISTUS HomeCare - St. Michael",),
    ("CHRISTUS HomeCare - Tyler",),
    ("Christus Hospice - Central Texas",),
    ("Christus Hospice and Palliative Care",),
    ("CHRISTUS Hospice and Palliative Care SPOHN",),
    ("Christus Hospice Schumpert",),
    ("CHRISTUS Hospice St. Frances Cabrini",),
    ("Christus Palliative Care St. Frances Cabrini",),
    ("CHRISTUS VNA Hospice",),
    ("CHRISTUS VNA Hospice of San Antonio",),
    ("CMH Home Health Care",),
    ("Coastal Home Health",),
    ("Colorado Plains Medical Center Home Care", "Plains Medical Center Home Care"),
    ("Commonwealth Healthcare at Home",),
    ("Commonwealth Home Health",),
    ("Commonwealth Home Health of Berwick",),
    ("Commonwealth Hospice",),
    ("Commonwealth Hospice of Forty Fort", "Commonwealth Hospice Forty Fort"),
    ("Community Home Health",),
    ("Community Hospice",),
    ("Community Loving Care Hospice",),
    ("Conemaugh Home Health",),
    ("Conemaugh Regional Hospice",),
    ("Conemaugh Regional Hospice-Johnstown", "Conemaugh Regional Hospice of Johnstown"),
    ("D.W. McMillan Home Health",),
    ("Deaconess HomeCare",),
    ("Deaconess HomeCare - Selmer",),
    ("Deaconess Hospice",),
    ("Deaconess Hospice of Biloxi",),
    ("Decatur Morgan HomeCare",),
    ("Delhi HomeCare",),
    ("DFW Home Health",),
    ("Egan Hospice",),
    ("Elite Home Health",),
    ("Elite Home Health of Fordyce",),
    ("Elite Home Health of Hot Springs",),
    ("Elite Hospice",),
    ("Elite Hospice of Springdale",),
    ("Elite Palliative Care",),
    ("Elk Valley",),
    ("Extendicare Home Health",),
    ("Extendicare Home Health of West TN",),
    ("Florida Home Health", "Floruda Home Health"),
    ("Gateway Regional Home Health",),
    ("Geisinger Home Health",),
    ("Geisinger Home Health Danville",),
    ("Geisinger Hospice",),
    ("Geisinger Hospice of Danville", "Geisinger Hospice-Danville"),
    ("Geisinger Hospice of Selinsgrove", "Geisinger Hospice-Selinsgrove"),
    ("Geisinger Hospice-Lewistown",),
    ("Generations Home Health",),
    ("Georgia Home Health",),
    ("Grace Hospice",),
    ("Grant Memorial Home Health",),
    ("Grant Memorial Hospice",),
    ("Guardian Home Health",),
    ("Harris Home Health",),
    ("Havasu Regional Home Health", "Havasu Regional Medical Center Home Health"),
    ("Haywood Hospice",),
    ("Heart 'n Home Hospice",),
    ("Heart of Hospice",),
    ("Heart of Hospice Columbia",),
    ("Heart of Hospice Lancaster",),
    ("Heartlite Hospice",),
    ("Heartlite Hospice - Scottsboro",),
    ("HighPoint Homecare", "HighPount Homecare"),
    ("HighPoint Hospice",),
    ("Home Care Plus", "Home Care Plus Inc"),
    ("Home Health of Wilson",),
    ("HomeCall",),
    ("HomeCall Baltimore",),
    ("HomeCare Services of Haywood", "Home Care Services of Haywood Regional", "Home Care Services of Haywood Regional Hospital", "Home Care Services of Haywood Regional Medical Center", "HomeCae services of Haywood"),
    ("HomeChoice Health Services",),
    ("Hospice of Wilson",),
    ("Housecalls Home Health",),
    ("Huntsville Hospital HomeCare",),
    ("Huntsville Hospital HomeCare- Madison",),
    ("Idaho Home Health",),
    ("Idaho Home Health and Hospice",),
    ("Idaho Home Health and Hospice of Treasure Valley",),
    ("IL Home Health",),
    ("Illinois Home Health",),
    ("Indiana HomeCare Northwest",),
    ("Infirmary HomeCare",),
    ("Island Home Care",),
    ("Jeff Davis HomeCare",),
    ("Jefferson HomeCare",),
    ("Keller Home Care",),
    ("La HomeCare",),
    ("Lake Norman Home Health",),
    ("LHC HomeCare of South Alabama",),
    ("LHC-Illinois Home Health Care",),
    ("Lifeline Health Care",),
    ("Lifeline Health Care of Cumberland",),
    ("Lifeline Health Care of McCreary",),
    ("Lifeline Health Care of Taylor",),
    ("Lifeline Health Care of Wayne",),
    ("Lifeline Home Health", "Lifeline Homecare"),
    ("Lifeline Home Health Care",),
    ("Lifeline Home Health of Tompkinsville",),
    ("Lifeline Home Health of Western KY",),
    ("Lifeline of Jackson Purchase Home Health",),
    ("Louisiana Home Health",),
    ("Louisiana HomeCare", "Louisiana HomeCare of BR"),
    ("Louisiana HomeCare Egan",),
    ("Louisiana HomeCare of Miss-Lou",),
    ("Louisiana HomeCare of New Orleans",),
    ("Louisiana HomeCare of Northshore",),
    ("Louisiana HomeCare of Raceland",),
    ("Louisiana HomeCare of River Parishes",),
    ("Louisiana Hospice and Palliative Care",),
    ("Lutheran Health Network Home Health",),
    ("Mederi Caretender's Hospice",),
    ("Mederi Caretenders", "Mederi Care Tenders"),
    ("Mederi Caretenders Clermont",),
    ("Mederi Caretenders Home Health",),
    ("Mederi Caretenders Inverness",),
    ("Mederi Caretenders Viera",),
    ("Mederi Private Care",),
    ("Medical Center Home Health",),
    ("Medical Centers HomeCare",),
    ("Memorial Homecare",),
    ("Memorial White Rose Home Health",),
    ("Memorial White Rose Hospice",),
    ("Memorial White Rose Hospice of York",),
    ("Mississippi HomeCare",),
    ("Mississippi HomeCare of Clinton",),
    ("Mississippi HomeCare of Kosciusko",),
    ("Mississippi HomeCare of Yazoo City",),
    ("Mississippi HomeCare- Starkville",),
    ("Mountaineer Home Care",),
    ("Mountlake Terrace Assured Home Health",),
    ("Ms HomeCare",),
    ("MS HomeCare of Wiggins",),
    ("Munroe Regional HomeCare",),
    ("Nason Hospital Home Health",),
    ("North Idaho Home Health",),
    ("Northern Arizona Home Health",),
    ("Northern Arizona Hospice",),
    ("Northern Arizona Hospice of Cottonwood",),
    ("Northern Arizona Hospice of Flagstaff",),
    ("Norton Home Health",),
    ("Nurse On Call", "Nurse On Call Home Health"),
    ("Nursing Care Home Health",),
    ("Ochsner Home Health of New Orleans",),
    ("Ohio's Choice Home Health",),
    ("Olympia Assured Home Health",),
    ("OMNI Home Care - Richmond",),
    ("Omni Home Health",),
    ("OMNI Homecare",),
    ("Orlando Health Home Care",),
    ("Palliative Care at Heart",),
    ("Parrish Home Health",),
    ("Patient Care", "Patient Care Services"),
    ("Patient Care Hospice",),
    ("Patient Care Hospice of Wayne",),
    ("PCH Home Health - Beckley",),
    ("PCH Home Health - Bluefield",),
    ("Pleasant Valley Home Health",),
    ("Preston Memorial Home Care",),
    ("Preston Memorial Home Health",),
    ("Providence Home Health",),
    ("Puyallup Assured Home Health",),
    ("Red Bud Regional Home Care", "Red Bud Regional Home Care - Red Bud"),
    ("Red Bud Regional Home Health",),
    ("Redmond Assured Home Health",),
    ("Renton Assured Home Health",),
    ("Saint Mary's Homecare", "Sanit Mary's Home Care"),
    ("Saint Mary's Hospice",),
    ("South Arkansas Home Health",),
    ("Southeast Alabama HomeCare",),
    ("Southern Home Care",),
    ("Southern Home Health",),
    ("Southside Regional Home Health",),
    ("Spokane Assured Home Health",),
    ("Spokane Valley Assured Home Health",),
    ("St. Francis Medical Center Home Health",),
    ("St. Landry HomeCare",),
    ("St. Mary's Home Health", "St. Mary's Home Health Service"),
    ("Star Hospice",),
    ("Suncrest Companion Care",),
    ("Suncrest Companion Services",),
    ("Suncrest Home Health",),
    ("Suncrest Home Health - Durham",),
    ("Suncrest Hospice",),
    ("SunCrest OMNI",),
    ("Thomas Home Health",),
    ("Total Care Home Care", "TotalCare"),
    ("TriCounty Home Health", "Tri County Home Health"),
    ("Troy Regional Medical Center Home Health",),
    ("Twin Lakes Home Health",),
    ("Unity Health HomeCare",),
    ("UP Health System Home Care & Hospice",),
    ("UP Health System Home Care & Hospice Portage",),
    ("UP Health System Hospice",),
    ("UP Health Systems Home Care",),
    ("UP Home Health",),
    ("UP Home Health Portage",),
    ("Valley View Home Health",),
    ("Ville Platte Home Health",),
    ("VNA of Maryland",),
    ("VNA of Maryland - Chestertown",),
    ("VNA of Maryland- Easton",),
    ("West Virginia Home Health",),
    ("Willcare", "Wiilcare", "Willcare Home Health"),
    ("Wythe Hospice",),
)

AGENCIES: dict[str, str] = {}
for _name, *_spellings in _AGENCIES:
    for _sp in (_name,) + tuple(_spellings):
        _k = fold(_sp)
        if AGENCIES.get(_k, _name) != _name:
            raise ValueError(f"agency spelling {_sp!r} names {AGENCIES[_k]!r} and {_name!r}")
        if _k in BRANDS:
            raise ValueError(f"agency spelling {_sp!r} is also a brand")
        AGENCIES[_k] = _name

# An agency name names a service ("Elite Home Health", "Heart of Hospice",
# "VNA of Maryland", "Apex Companion Care") ...
AGENCY_WORDS = re.compile(r"home|health|hospice|care|nurs|\bvna\b|palliative|companion|rehab|therap|tenders",
                          re.I)
# ... and is not an internal branch code ("Eunice LA PDN PCS", "Almost Family -
# Marinette PD", "Mt. Vernon LHC MVI", "Sulphur Pediatrics LA"), a placeholder
# ("[agency name]"), the parent alone or a sentence.
AGENCY_NOT = re.compile(r"[\[\]{}<>]|\b(?:PCS|PDN|MVI|LHC)\b|\s[A-Z]{2}$|\bagency name\b|^(?:the|a|an|our|we|this|"
                        r"if|you|your|join|explore)\b|\b(?:opportunit\w*|seeking|hiring|leading|position|team)\b",
                        re.I)


def _agency_name(raw) -> str:
    """The clean name of an LHC Group template agency, or "" (internal code,
    placeholder, no service word, too long)."""
    name = clean_text(_html.unescape(str(raw or ""))).strip(" .,;:-*]")
    k = fold(name)
    if k in AGENCIES:
        return AGENCIES[k]
    if not name or len(name) > 70 or len(name.split()) > 9 or not re.match(r"[A-Z0-9]", name):
        return ""
    if AGENCY_NOT.search(name) or not AGENCY_WORDS.search(name):
        return ""
    return name


# ── Reading the opening line ────────────────────────────────────────────────
_T_EXPLORE = re.compile(r"\bE\s?xplore\s+(?:[A-Za-z&/+-]+\s+){0,4}?opportunities\s+(?:with|at)\s+", re.I)
_T_OPPS = re.compile(r"\bOpportunities\s+(?:with|at)\s+|\bopportunity is with\s+|\bWe are hiring at\s+")
_T_PARTNER = re.compile(r"\bA partner of the Optum Care network,\s*")
_T_JOIN = re.compile(r"\bJoin\s+(?=[A-Z])")
# "L HC Group" is a typo some LHC postings print
_T_PARTOF = re.compile(r"\s*,?\s*\(?(?:\ba\s+)?\bpart of (?:the Optum family|(?:the\s+)?L\s?HC Group\b)")
_T_LEAD = re.compile(r"(?:^|\n|[.!?*]\s+)\s*(?=[A-Z])")
_LHC_AFTER = re.compile(r"\s*,?\s*(?:\ba\s+)?\bpart of (?:the\s+)?L\s?HC Group\b")
_JOIN_AFTER = re.compile(r"\s*,\s*(?:one of|a part of|part of)\b")
# "Optum WA, (formerly The Polyclinic) is seeking"
_LEAD_AFTER = re.compile(r"\s*,?\s*(?:\((?:formerly|previously)[^)]{0,80}\)\s*,?\s*)?(?:is|are)\s+"
                         r"(?:seeking|hiring|looking)\b")
# Where a name ends: a comma, "(", ";", ":", "!", "?", "a part of" / "part of",
# " is " / " are ", "a leading", "one of", "in strategic", a full stop that
# is not an abbreviation's ("St.", "Mt.", "D.W."), or a line break.
_NAME_END = re.compile(r"\s*(?:[,(;:!?\n]|\ba part of\b|\bpart of\b|\bis\b|\bare\b|\ba leading\b|\bone of\b|"
                       r"\bin strategic\b|(?<!\bSt)(?<!\bMt)(?<!\bFt)(?<![A-Z])\.(?=\s|$))")
_NAME_MAX = 90
_SENTENCE_END = re.compile(r"\n|(?<!\b[A-Z])(?<!\bSt)(?<!\bMt)(?<!\bFt)[.!?]\s+|\*\s+")


def head_text(description) -> str:
    """The first HEAD_CHARS of a body, each line cleaned, line breaks kept."""
    lines = []
    for line in str(description or "")[:HEAD_CHARS * 2].splitlines():
        line = clean_text(_html.unescape(line))
        if line:
            lines.append(line)
    return "\n".join(lines)[:HEAD_CHARS]


def _name_at(text, pos):
    """(name that starts at pos, where it ends), or None."""
    m = _NAME_END.search(text, pos, pos + _NAME_MAX + 1)
    if m is None or m.start() <= pos:
        return None
    return text[pos:m.start()].strip(" *-"), m.start()


def candidates(head) -> list:
    """(position, template, raw name, LHC Group template) of every business
    the opening templates name, in text order."""
    out = []
    for tpl, rx in (("explore", _T_EXPLORE), ("opps", _T_OPPS), ("partner", _T_PARTNER), ("join", _T_JOIN)):
        for m in rx.finditer(head):
            got = _name_at(head, m.end())
            if not got:
                continue
            raw, end = got
            if tpl == "join" and not _JOIN_AFTER.match(head, end):
                continue
            out.append((m.start(), tpl, raw, bool(_LHC_AFTER.match(head, end))))
    for m in _T_PARTOF.finditer(head):
        # the name starts after the last line break or sentence end ("J. Snyder"
        # and "St. Mary's" are not sentence ends)
        start = 0
        for b in _SENTENCE_END.finditer(head, 0, m.start()):
            start = b.end()
        # "Oregon Medical Group (OMG), part of ..." -> "Oregon Medical Group"
        raw = re.sub(r"\s*\([^()]{0,40}\)$", "", head[start:m.start()].strip(" *-"))
        if raw and len(raw) <= _NAME_MAX and re.match(r"[A-Z0-9]", raw):
            out.append((start, "partof", raw, bool(_LHC_AFTER.match(head, m.start()))))
    for m in _T_LEAD.finditer(head):
        got = _name_at(head, m.end())
        if got and _LEAD_AFTER.match(head, got[1]):
            out.append((m.end(), "lead", got[0], False))
    out.sort(key=lambda c: (c[0], c[1] != "explore"))
    return out


UNKNOWN: dict = {}       # raw opening-line name -> rows named by no table entry, this run


def resolve(raw, lhc):
    """A raw opening-line name -> Brand (SITE with its clean name, CORP or
    LABEL with the system name), or None (unknown: names nothing)."""
    k = fold(raw)
    b = BRANDS.get(k)
    if b is not None:
        return b
    if k in AGENCIES:
        return Brand(AGENCIES[k], SITE, "")
    if lhc:
        name = _agency_name(raw)
        return Brand(name, SITE, "") if name else Brand(SYSTEM, LABEL, "")
    return None


def body_brand(description):
    """(Brand, raw name) the opening line names, or (None, "")."""
    head = head_text(description)
    unknown = ""
    for _pos, tpl, raw, lhc in candidates(head):
        b = resolve(raw, lhc)
        if b is not None:
            return b, raw
        if tpl != "lead" and not unknown:
            unknown = raw
    if unknown:
        UNKNOWN[unknown[:80]] = UNKNOWN.get(unknown[:80], 0) + 1
    return None, ""


# ── The title ───────────────────────────────────────────────────────────────
# Medical groups and clinics a title names ("OB/GYN Physician - Kelsey-Seybold
# - Katy", "Cardiology APC- WellMed- San Antonio, TX", "X-Ray Technologist -
# Crystal Run Healthcare", "Family Medicine Access APC (NP or PA) | Reliant
# Medical Group"). CareMount Medical is Optum NY's former name (Optum
# Tri-State NY), printed in its titles ("Optum, CareMount Medical").
TITLE_BRANDS = (
    (re.compile(r"\bkelsey[\s-]+seybold\b", re.I), "Kelsey-Seybold Clinic"),
    (re.compile(r"\bwellmed\b(?!\s+medical\s+management)", re.I), "WellMed"),   # the MSO is an office
    (re.compile(r"\batrius\b", re.I), "Atrius Health"),
    (re.compile(r"\breliant medical group\b", re.I), "Reliant Medical Group"),
    (re.compile(r"\boptum\s+ny\b|\bcaremount\b", re.I), "Optum NY"),
    (re.compile(r"\bcrystal run\b", re.I), "Crystal Run Health"),
    (re.compile(r"\bcorvallis clinic\b", re.I), "The Corvallis Clinic"),
    (re.compile(r"\boregon medical group\b", re.I), "Oregon Medical Group"),
    (re.compile(r"\bprohealth physicians\b", re.I), "ProHealth Physicians"),
    (re.compile(r"\bamerican health network\b", re.I), "American Health Network"),
    (re.compile(r"\bgenoa\b", re.I), "Genoa Healthcare"),
    (re.compile(r"\bamedisys\b", re.I), "Amedisys"),
)


def title_brand(title) -> str:
    """The one medical group / clinic the title names, or ""."""
    got = {name for rx, name in TITLE_BRANDS if rx.search(title or "")}
    return got.pop() if len(got) == 1 else ""


# Remote, telecommute and home-based postings show the system name (decision 2).
REMOTE = re.compile(r"\b(?:remote|telecommut\w*|work\s+(?:from|at)\s+home|wfh|home[\s-]based|virtual|nationwide)\b",
                    re.I)


def uhg_site(title, description):
    """(hospital_name, how) for one UHG posting: how is "remote", "body"
    (the opening line named a site), "title", "label" (the opening line named
    the system, a corporate unit, house calls, a parent, another system) or
    "none" (nothing named). Pure: no stored name, no town."""
    if REMOTE.search(title or ""):
        return SYSTEM, "remote"
    b, _raw = body_brand(description)
    t = title_brand(title)
    # The title names the job's own group: it wins over an opening line that
    # names no site, only UHG corporate, or another group (a Reliant Medical
    # Group posting in Worcester that opens with Atrius Health's paragraph; a
    # "Crystal Run Healthcare" title under Optum NY's), never over a line that
    # names house calls, a parent or another system (LABEL).
    if t and (b is None or b.kind == CORP or (b.kind == SITE and not _same_group(b.name, t))):
        return t, "title"
    if b is not None and b.kind == SITE:
        return b.name, "body"
    return SYSTEM, ("label" if b is not None else "none")


def _same_group(a, b) -> bool:
    """"Amedisys Hospice" and "Amedisys" are one group; "Atrius Health" and
    "Reliant Medical Group" are two."""
    a, b = fold(a), fold(b)
    return a.startswith(b) or b.startswith(a)


# ── Towns ───────────────────────────────────────────────────────────────────
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR",
}
STATE_CODES = frozenset(STATE_NAMES.values())
NOT_A_TOWN = frozenset({"", "united states", "usa", "us", "remote", "nationwide"})


def is_town(city, state="") -> bool:
    """A row's city is a town: not blank, not the country, and not its own
    state's name ("Texas, TX" is a state-wide posting; "Washington, PA",
    "Indiana, PA" and New York City are towns)."""
    c = " ".join(str(city or "").lower().split())
    if c in NOT_A_TOWN:
        return False
    code = STATE_NAMES.get(c)
    if code is None:
        return True
    st = (state or "").strip().upper()
    if c == "new york" and st == "NY":
        return True
    return bool(st) and code != st


_STATE_AT = re.compile(r"(?:,\s*|\s+)([A-Z]{2})\b(?![a-z])|,\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b")
_BODY_TOWN = re.compile(r"\b(?:in|located in)\s+(?=[A-Z])")


def _towns_before(text, end_rx=_STATE_AT) -> set:
    """(known spelling, state) for every "<Town>, <ST>" / "<Town> <ST>" /
    "<Town>, <State>" in text: the one to four words before the state that
    city_utils knows as a town of that state (known_towns.json, the CMS
    hospitals' towns, the towns two systems post in), longest first."""
    from city_utils import known_town
    out = set()
    for m in end_rx.finditer(text or ""):
        st = m.group(1) or STATE_NAMES.get((m.group(2) or "").lower(), "")
        if st not in STATE_CODES:
            continue
        words = re.findall(r"[A-Za-z.'\-]+", text[max(0, m.start() - 60):m.start()])
        for n in (4, 3, 2, 1):
            if len(words) < n:
                continue
            cand = " ".join(words[-n:]).strip(".-'")
            if not cand or not cand[0].isupper():
                continue
            got = known_town(cand, st)
            if got:
                out.add((got, st))
                break
    return out


def text_town(title, description, state):
    """(town, state) the title or the opening line names, for a row whose
    stored town is a state (or blank): one known town only, in the row's
    state when it has one. None when the text names none or several."""
    want = (state or "").strip().upper()
    keep = lambda found: {(t, s) for t, s in found if is_town(t, s) and (not want or s == want)}
    found = keep(_towns_before(title))
    if not found:
        head = head_text(description)[:700]
        found = set()
        for m in _BODY_TOWN.finditer(head):
            seg = head[m.end():m.end() + 70]
            sm = _STATE_AT.search(seg)
            if sm:
                found |= _towns_before(seg[:sm.end()])
        found = keep(found)
    return found.pop() if len(found) == 1 else None


def place(name, title, description, city, state):
    """(city, state) for a row of this module: its own town; a blank state
    filled from a one-state business; a state-named town replaced by the one
    town the posting names in that state. Nothing else moves."""
    city, state = (city or "").strip(), (state or "").strip().upper()
    if not is_town(city, state):
        got = text_town(title, description, state or STATE_NAMES.get(city.lower(), ""))
        return got if got else (city, state)
    if not state and FOOTPRINT.get(name):
        return city, FOOTPRINT[name]
    return city, state


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000
_SITE_NAMES = frozenset(b.name for b in BRANDS.values() if b.kind == SITE)


def valid_name(name) -> bool:
    """A stored hospital_name this module could have written."""
    name = clean_text(name)
    if not name or name == SYSTEM:
        return False
    return name in _SITE_NAMES or bool(_agency_name(name))


def set_known_facilities(rows) -> int:
    """{job_id: (stored hospital_name, city, state)} for the active UHG rows
    whose stored name is one this module writes."""
    global _KNOWN
    out = {}
    for r in rows or []:
        if (r.get("hospital_system") or "").strip() != SYSTEM or r.get("job_id") in (None, ""):
            continue
        name = clean_text(r.get("hospital_name"))
        if valid_name(name):
            out[str(r["job_id"])] = (name, (r.get("city") or "").strip(), (r.get("state") or "").strip().upper())
    _KNOWN = out
    return len(out)


def known_facility(job_id):
    return _KNOWN.get(str(job_id))


def load_known_facilities() -> int:
    """Read-only PostgREST pass (the SUPABASE_URL / key the upsert uses) over
    the active UHG rows whose hospital_name is not the system name,
    keyset-paged on id. Without credentials the map stays empty and nothing
    is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows, last = [], 0
    flt = _urlparse.quote(f"eq.{SYSTEM}", safe=".")
    nflt = _urlparse.quote(f"neq.{SYSTEM}", safe=".")
    try:
        while len(rows) < KNOWN_MAX_ROWS:
            u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,city,state"
                 f"&is_active=is.true&hospital_system={flt}&hospital_name={nflt}"
                 f"&id=gt.{last}&order=id.asc&limit={KNOWN_PAGE}")
            rq = _urlreq.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
            chunk = None
            for attempt in range(3):
                try:
                    with _urlreq.urlopen(rq, timeout=60) as resp:
                        chunk = json.loads(resp.read().decode())
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(3 * (attempt + 1))
            if not chunk:
                break
            rows.extend(chunk)
            last = chunk[-1]["id"]
            if len(chunk) < KNOWN_PAGE:
                break
    except Exception as e:
        logger.warning(f"UHG facilities: stored names load stopped after {len(rows):,} rows ({e}); "
                       f"the rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"UHG facilities: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before normalize_job) ──────────────
def apply_facility(job) -> str:
    """Name and place one UHG Job. Returns "body", "title", "kept" (the
    stored name, no body tonight), "remote", "label", "none", "other" (the
    adapter's own name, left as it is) or "" for any other system.
    hospital_system is never changed."""
    if (job.hospital_system or "").strip() != SYSTEM:
        return ""
    cur = clean_text(job.hospital_name)
    if cur and cur != SYSTEM:
        return "other"
    # No body tonight (a known body is sent blank): the name stored on the row
    # (read from its body on an earlier night) comes first, before the title.
    kept = None
    if len((job.description or "").strip()) < NO_BODY_CHARS and not REMOTE.search(job.title or ""):
        kept = known_facility(job.job_id)
    if kept:
        name, how = kept[0], "kept"
        state = (job.state or "").strip().upper()
        # a town this module wrote over a state-named town stays
        if not is_town(job.city, state) and is_town(kept[1], kept[2]) and kept[2] and state in ("", kept[2]):
            _set_town(job, kept[1], kept[2])
    else:
        name, how = uhg_site(job.title, job.description)
    if name != SYSTEM:
        job.hospital_name = name
    _set_town(job, *place(name, job.title, job.description, job.city, job.state))
    return how


def _set_town(job, city, state) -> None:
    if city and state and (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = apply_facility(job)
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats


def unknown_summary(top=6) -> str:
    """The opening-line names no table entry names, most frequent first."""
    vals = sorted(UNKNOWN.items(), key=lambda kv: -kv[1])[:top]
    return "; ".join(f"{v} ({n})" for v, n in vals)
