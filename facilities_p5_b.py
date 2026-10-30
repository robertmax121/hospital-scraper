"""The hospital or named site on the label rows of nineteen more systems (2026-10-29, phase 5, group B).

The owner, 2026-10-29: "i thought you were gonna edit all the large
multihospital systems", "trinity health not done either". His two rules
stand: (1) cards and job pages show the HOSPITAL or named site ("Apply to
<site>"); the system name and logo stay on the job page and the system hub
(hospital_system is never touched here: the upsert key, the hub, the
filters, the logo); (2) named sites (hospitals, named clinics, centers,
agencies' named branches, named communities) show their own name; generic
offices, home-based, remote, multi-site pools and regional roles show the
system name with the town; a town or a site is never guessed (a town-to-site
directory is no evidence: the posting itself or the employer's own job page
must name the site; a town with exactly one hospital of the system takes it
only through the facilities_town rule, never for clinics, offices or remote
postings).

Group B's nineteen systems (stored label rows read 2026-10-29, after the
phase 2, 3 and 4 SQL). Seven are one hospital whose name IS the system name,
or a payer, and need nothing here (Nationwide Children's, Tampa General,
Cabell Huntington, Children's National, Molina, and the main campuses of
Cincinnati Children's and Akron Children's); Baptist Health (KY/IN) and
(AR) and Tenet are already as far as their postings go (facilities_town,
facility_names). This module names the rest:

  Workday site tables (facilities_workday; the adapter's list-time
  WD_SITE_FACILITY reads them every night from the posting's own site):
    Riverside Health System  its named communities (Patriots Colony, Warwick
                        Forest), the Lifelong Health and Rehabilitation
                        centers, the Home Health
                        and Hospice branches, the College of Health Careers,
                        named cancer centers and practices; office buildings,
                        professional centers, MOBs, plazas and the support
                        and corporate offices stay.
    Banner Health       the North Colorado Medical Center Loveland campus
                        (its own site name) and the Banner Alzheimer's
                        Institute sites (Phoenix, Tucson).
    Cleveland Clinic    the Justin Rogers Hospice Care Center, in the town
                        the posting itself writes ("Location Akron").
  Town tables (facilities_town: the one hospital of the system in the
  posting's town; clinics, offices, remote, care sites keep the label):
    United Health Services (NY)  Johnson City, Binghamton, Walton, Norwich
    Logan Health        Kalispell, Whitefish, Shelby, Conrad, Cut Bank, Chester
    Health First        Palm Bay, Cocoa Beach; Melbourne holds Holmes Regional
                        and Viera Hospital (only a title naming one)
  This module's own pass (finalize_jobs, before the town tables):
    Telecare Corporation  the program number every title ends in ("Mental
                        Health 103") names the program the employer's own
                        postings name in their bodies (Villa Fairmont MHRC,
                        La Casa PHF, Fern Lodge, the E&Ts, the CSUs ...), in
                        that program's own town; field teams (ACT, FSP,
                        mobile crisis, outreach) keep the label.
    Cincinnati Children's the posting's Workday site names its campus:
                        Liberty Campus, College Hill Campus (the Burnet
                        campus IS "Cincinnati Children's").
    Akron Children's    the posting names the Mahoning Valley (Beeghly)
                        campus in Boardman.
    Mass General Brigham  the body's "Site:" legal entity names a named
                        center or agency facility_names lacks (Spaulding
                        Nursing and Therapy Center Brighton, VNA & Hospice of
                        Cooley Dickinson, MGH Institute of Health Professions).
    Logan Health        the posting's own opening names a site of the system
                        (Logan Health Chester, Cut Bank, Conrad, Shelby,
                        Whitefish, Logan Health Children's, Logan Health
                        Brendan House), before the town table.
    Cleveland Clinic    a Cleveland Clinic Nevada posting that names the Lou
                        Ruvo Center for Brain Health.
    UTHealth Houston    a posting that names the Harris County Psychiatric
                        Center.

Hooks (scraper.py): imported with the other facility modules (its Workday
and town tables register at import, before scraper.WD_SITE_FACILITY is
built); the run start loads the names stored on this pass's named rows
(load_known_facilities: a night without a body keeps them); finalize_jobs
calls apply_facilities after the phase 4 B pass, before the town tables.
Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase5/b_work/changes.py).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_town
import facilities_workday
import sb_keys  # the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility_p5b")

clean_text = facilities_town.clean_text
name_key = facilities_town.name_key
town_key = facilities_town.town_key

RIVERSIDE = "Riverside Health System"
BANNER = "Banner Health"
CLEVELAND = "Cleveland Clinic"
UHS = "United Health Services"
LOGAN = "Logan Health"
HEALTH_FIRST = "Health First"
TELECARE = "Telecare Corporation"
CINCY = "Cincinnati Children's"
AKRON = "Akron Children's"
MGB = "Mass General Brigham"
UTH = "UTHealth Houston"

NO_BODY_CHARS = facilities_town.NO_BODY_CHARS


# ══════════════════════════════════════════════════════════════════════════
#  1. WORKDAY SITE TABLES (facilities_workday: the adapter's WD_SITE_FACILITY)
# ══════════════════════════════════════════════════════════════════════════
# Riverside Health System (rivhs): "<facility> - <town> Virginia"; the town
# is the site's own (phase 2: "towns right"). Named communities (Patriots
# Colony, Warwick Forest: the system's retirement communities), the named
# nursing and rehabilitation centers, the Home Health and Hospice branches
# (an agency's named branches), the College of Health Careers and the named
# cancer centers and practices take their own names, as the site writes them.
# Office buildings, professional centers and parks, MOBs, plazas, the
# multi-specialty healthcare centers (Williamsburg, Hampton: urology,
# hospice, therapy and imaging practices under one roof), the support-services and corporate offices, town-named urgent care (MD
# Express), wellness (fitness) centers, the child care center and sites the
# row's town does not confirm stay on the label.
facilities_workday._t(RIVERSIDE, """
Patriots-Colony---Williamsburg-Virginia | Patriots Colony | Williamsburg | VA | board (the site names the community and its town)
Warwick-Forest---Newport-News-Virginia | Warwick Forest | Newport News | VA | board
Riverside-Lifelong-Health--Rehabilitation-Warwick-Forest---Newport-News-Virginia | Riverside Lifelong Health and Rehabilitation Warwick Forest | Newport News | VA | board
Riverside-Lifelong-Health-and-Rehabilitation-Smithfield---Smithfield-VA | Riverside Lifelong Health and Rehabilitation Smithfield | Smithfield | VA | board
Riverside-Lifelong-Health-and-Rehabilitation-Mathews---Mathews-Virginia | Riverside Lifelong Health and Rehabilitation Mathews | Mathews | VA | board
Riverside-Lifelong-Health-and-Rehabilitation-Saluda---Saluda-Virginia | Riverside Lifelong Health and Rehabilitation Saluda | Saluda | VA | board
Riverside-Home-Health-and-Hospice--Gloucester-Virginia | Riverside Home Health and Hospice Gloucester | Gloucester | VA | board
Riverside-Home-Health-and-Hospice--Accomac-Virginia | Riverside Home Health and Hospice Accomac | Accomac | VA | board
Riverside-Home-Health-and-Hospice--Warsaw-Virginia | Riverside Home Health and Hospice Warsaw | Warsaw | VA | board
Riverside-Home-Health-and-Hospice---Williamsburg-Virginia | Riverside Home Health and Hospice Williamsburg | Williamsburg | VA | board
Riverside-Home-Health-and-Hospice---Shore---Onley-Virginia | Riverside Home Health and Hospice Shore | Onley | VA | board
Riverside-College-of-Health-Careers---Newport-News-Virginia | Riverside College of Health Careers | Newport News | VA | board
Riverside-Regional-Medical-Center-Cancer-Care-Center---Newport-News-Virginia | Riverside Regional Medical Center Cancer Care Center | Newport News | VA | board
Riverside-Middle-Peninsula-Cancer-Center---Gloucester-Virginia | Riverside Middle Peninsula Cancer Center | Gloucester | VA | board
Cancer-Specialists-of-Tidewater---Chesapeake---Chesapeake-Virginia | Cancer Specialists of Tidewater Chesapeake | Chesapeake | VA | board
Riverside-Cancer-Specialist-of-Tidewater---VA-Beach---Virginia-Beach-Virginia | Cancer Specialists of Tidewater Virginia Beach | Virginia Beach | VA | board
Hampton-Roads-Eye-Associates---Oyster-Point---Newport-News-Virginia | Hampton Roads Eye Associates | Newport News | VA | board
Peninsula-Gastroenterology---Newport-News-Virginia | Peninsula Gastroenterology | Newport News | VA | board
Peninsula-Surgery-Center---Newport-News-Virginia | Peninsula Surgery Center | Newport News | VA | board
Hampton-Roads-Surgical-Specialists---Newport-News-Virginia | Hampton Roads Surgical Specialists | Newport News | VA | board
""")

# Banner Health (bannerhealth.wd108): "<site> <street>". The North Colorado
# Medical Center's Loveland campus (2000 N Boise Ave, Loveland, the former
# McKee Medical Center) by the name its own site gives; the Banner
# Alzheimer's Institute (a named center) at its Phoenix (901 E Willetta St)
# and Tucson (2626 E River Rd) sites. The institutes' executive office (1114
# E McDowell Rd) and research building stay.
facilities_workday._t(BANNER, """
Banner-North-Colorado-Medical-Center---Loveland-Campus-2000-N-Boise-Ave | Banner North Colorado Medical Center - Loveland Campus | Loveland | CO | board (2000 N Boise Ave, Loveland)
Banner-Alzheimers-Institute-901-E-Willetta-St | Banner Alzheimer's Institute | Phoenix | AZ | board (901 E Willetta St, Phoenix)
Banner-Alzheimers-Institute-Tucson-2626-E-River-Rd | Banner Alzheimer's Institute Tucson | Tucson | AZ | board (2626 E River Rd, Tucson)
""")

# Cleveland Clinic (ccf.wd1): every Ohio row took the Cleveland default. The
# hospice's inpatient care center writes its own town on every posting
# ("Location Akron Facility Justin Rogers Hosp Care Center").
facilities_workday._t(CLEVELAND, """
Justin-Rogers-Hosp-Care-Center | Justin Rogers Hospice Care Center | Akron | OH | posting (Location Akron, Facility Justin Rogers Hosp Care Center)
""")


# ══════════════════════════════════════════════════════════════════════════
#  2. TOWN TABLES (facilities_town: the one hospital of the system in a town)
# ══════════════════════════════════════════════════════════════════════════
_h = facilities_town._h


def _town_spec(system, hospitals, **kw):
    if system in facilities_town.SPECS:
        raise ValueError(f"facilities_town already has a town table for {system}")
    return facilities_town._spec(system, hospitals, **kw)


# United Health Services (nyuhs): every site but the hospitals' own is a bare
# town ("Johnson-City-NY"). Wilson and Binghamton General share one licence
# (CMS 330394, United Health Services Hospitals, Inc.), each on its own
# street; Our Lady of Lourdes (Binghamton) is another system's. Binghamton is
# the stamp a row with no town takes (SYSTEM_LOCATION_DEFAULTS): a row there
# is placed only when its own Workday site names Binghamton.
UHS_TOWNS = _town_spec(UHS, [
    _h("UHS Wilson Medical Center", "Johnson City", "NY", "cms 330394 (33-57 Harrison St, Johnson City)"),
    _h("UHS Binghamton General Hospital", "Binghamton", "NY", "cms 330394 (10-42 Mitchell Ave, Binghamton)"),
    _h("UHS Delaware Valley Hospital", "Walton", "NY", "cms 331312"),
    _h("UHS Chenango Memorial Hospital", "Norwich", "NY", "cms 330033"),
], default=("Binghamton", "NY"))

# Logan Health (loganhealth): every site is "Town-Montana"; the adapter puts a
# row at its own town (facilities_workday TOWN_BOARDS), Kalispell is the
# stamp. One hospital per town; the names are the ones the postings write
# ("Logan Health Chester", "Logan Health - Conrad", "Logan Health Cut Bank").
# Logan Health Children's sits inside the Kalispell medical center (named only
# when the posting names it).
LOGAN_TOWNS = _town_spec(LOGAN, [
    _h("Logan Health Medical Center", "Kalispell", "MT", "cms 270051"),
    _h("Logan Health Whitefish", "Whitefish", "MT", "cms 271336"),
    _h("Logan Health Shelby", "Shelby", "MT", "cms 271328"),
    _h("Logan Health Conrad", "Conrad", "MT", "cms 271324 (Pondera Medical Center)"),
    _h("Logan Health Cut Bank", "Cut Bank", "MT", "cms 271337 (Northern Rockies Medical Center)"),
    _h("Logan Health Chester", "Chester", "MT", "cms 271334"),
], default=("Kalispell", "MT"),
    sites=[("Logan Health Children's", "Kalispell", "MT", ("Logan Health Children's", "Logan Health Childrens"))])

# The head-office override (scraper FORCE_LOCATION_OVERRIDE: Kalispell) must
# leave a row the town table or this module's pass put at a Logan site in its
# own town, as it leaves the board's own towns (facilities_workday.placed).
for _h_name, _h_town, _h_st in [h[:3] for h in LOGAN_TOWNS.hospitals] + [x[:3] for x in LOGAN_TOWNS.sites]:
    facilities_workday.PLACED_EXTRA.setdefault(LOGAN, set()).add((_h_name, facilities_workday.key(_h_town), _h_st))

# Health First (Phenom, careers.hf.org): no site field on the stored rows.
# Melbourne holds Holmes Regional and Viera Hospital (CMS writes Viera's
# street as Melbourne): a Melbourne row is named only when its title names
# one. Melbourne is also the stamp. Rockledge (the system's offices; Orlando
# Health Rockledge is another system's) and "Brevard County" hold none.
HEALTH_FIRST_TOWNS = _town_spec(HEALTH_FIRST, [
    _h("Holmes Regional Medical Center", "Melbourne", "FL", "cms 100019"),
    _h("Viera Hospital", "Melbourne", "FL", "cms 100315 (8745 N Wickham Rd)"),
    _h("Palm Bay Hospital", "Palm Bay", "FL", "cms 100316"),
    _h("Cape Canaveral Hospital", "Cocoa Beach", "FL", "cms 100177"),
], default=("Melbourne", "FL"))

# Titles naming a site of the system that is no hospital: Health First's
# medical group (HFMG) and health plans, UHS's senior living and home care,
# Logan Health's Brendan House (a skilled nursing facility in Kalispell).
_CARE_SITES = {
    HEALTH_FIRST: re.compile(r"\b(?:HFMG|Health\s+First\s+Medical\s+Group|Health\s+Plans?|AHP|Personal\s+Care|"
                             r"Hospice|Home\s+Care|Pace)\b", re.I),
    UHS: re.compile(r"\b(?:Ideal\s+(?:Senior|Living|Care)|Senior\s+Living|Home\s+Care|UHS\s+Medical\s+Group|"
                    r"Primary\s+Care)\b", re.I),
    # review 2026-10-29: Kalispell's skilled nursing is Brendan House; a
    # "Hi-Line Region" or mobile coach role, an EMS station and a posting for
    # two towns ("Shelby & Cut Bank Physical Therapy") are no one hospital's
    LOGAN: re.compile(r"\b(?:Brendan\s+House|Skilled\s+Nursing|Hi-?Line|Region(?:al)?|Mobile\s+Coach|EMS|Station)\b|"
                      r"\b(?:Shelby|Cut\s+Bank|Chester|Conrad|Whitefish|Kalispell)\s*(?:&|and|/)\s*"
                      r"(?:Shelby|Cut\s+Bank|Chester|Conrad|Whitefish|Kalispell)\b", re.I),
}
for _sys, _rx in _CARE_SITES.items():
    if _sys in facilities_town.CARE_SITE_BY_SYSTEM:
        raise ValueError(f"facilities_town already has care sites for {_sys}")
    facilities_town.CARE_SITE_BY_SYSTEM[_sys] = _rx


# ══════════════════════════════════════════════════════════════════════════
#  3. THIS MODULE'S OWN PASS (posting text, title and link)
# ══════════════════════════════════════════════════════════════════════════
# A site: (public name, town, state, source).
def _s(name, town, state, source):
    return (name, town, state, source)


# Telecare: program number -> the program, as the employer's own postings
# name it in their bodies (read off the stored rows 2026-10-29), at the town
# every posting of that number carries. Facility programs only: field teams
# (ACT, FSP, mobile crisis, outreach, wraparound) keep the system name with
# their town (decision 2). 155 / 169 / 194 are the adapter's own
# (scraper.UKG_TITLE_FACILITY).
TELECARE_PROGRAMS = {
    "101": _s("Morton Bakar Center", "Hayward", "CA", "posting body (Morton Bakar Center is a Skilled Nursing Facility)"),
    "103": _s("Villa Fairmont Mental Health Rehabilitation Center", "San Leandro", "CA",
              "posting body (Villa Fairmont MHRC is a 96 bed licensed Mental Health Rehabilitation Center)"),
    "105": _s("Sage House", "Redwood City", "CA", "posting body (Sage House is a licensed 16-bed mental health rehab)"),
    "109": _s("La Casa Mental Health Rehabilitation Center", "Long Beach", "CA", "posting body (La Casa MHRC)"),
    "110": _s("La Casa Psychiatric Health Facility", "Long Beach", "CA", "posting body (La Casa PHF)"),
    "115": _s("Gladman Mental Health Rehabilitation Center", "Oakland", "CA",
              "posting body (Gladman is licensed as a 40-bed MHRC)"),
    "116": _s("Garfield Neurobehavioral Center", "Oakland", "CA", "posting body (Garfield Neurobehavioral Center)"),
    "154": _s("Redwood Place", "Castro Valley", "CA", "posting body (Redwood Place is a MHRC)"),
    "173": _s("Sanger Place", "Sanger", "CA", "posting body (Sanger Place MHRC)"),
    "191": _s("Jeremy House", "Stockton", "CA", "posting body (Jeremy House is a 6-bed crisis residential)"),
    "195": _s("Santa Cruz County Crisis Stabilization Program", "Santa Cruz", "CA", "posting body"),
    "201": _s("Recovery Center at Gresham", "Gresham", "OR", "posting body (The Recovery Center at Gresham)"),
    "203": _s("Recovery Center at Woodburn", "Woodburn", "OR", "posting body (The Recovery Center at Woodburn)"),
    "208": _s("Deschutes Recovery Center", "Bend", "OR", "posting body"),
    "209": _s("Multnomah Crisis and Treatment Center", "Portland", "OR", "posting body"),
    "250": _s("Telecare Pierce County Evaluation and Treatment", "Milton", "WA", "posting body (Pierce County E&T)"),
    "253": _s("Telecare North Sound Evaluation and Treatment", "Sedro Woolley", "WA", "posting body"),
    "254": _s("Tumwater Evaluation and Treatment", "Tumwater", "WA", "posting body"),
    "255": _s("Telecare King County Evaluation and Treatment", "Federal Way", "WA", "posting body"),
    "259": _s("Telecare Thurston Mason Evaluation and Treatment", "Olympia", "WA", "posting body (Thurston Mason E&T)"),
    "263": _s("Mark Reed Evaluation and Treatment", "McCleary", "WA", "posting body (Mark Reed E&T center in McCleary WA)"),
    "264": _s("Telecare Shelton Evaluation and Treatment", "Shelton", "WA", "posting body"),
    "265": _s("Shelton Next Steps", "Shelton", "WA", "posting body (Shelton Next Steps, a 16-bed E&T)"),
    "270": _s("Telecare Fern Lodge", "Stanwood", "WA", "posting body (Telecare Fern Lodge Behavioral Wellness program)"),
    "320": _s("Stanislaus County Psychiatric Health Facility", "Ceres", "CA", "posting body"),
    "321": _s("Contra Costa Hope House", "Martinez", "CA", "posting body"),
    "323": _s("Riverside County Crisis Stabilization Unit", "Indio", "CA", "posting body"),
    "325": _s("Telecare El Dorado County Psychiatric Health Facility", "Placerville", "CA", "posting body"),
    "330": _s("Jay Mahler Recovery Center", "San Leandro", "CA", "posting body"),
    "333": _s("Horizon View Mental Health Rehabilitation Center", "Camarillo", "CA", "posting body"),
    "339": _s("Telecare Eastern Kern Crisis Stabilization Unit", "Ridgecrest", "CA", "posting body"),
    "342": _s("Lagos Crisis Residential Facility", "Riverside", "CA", "posting body"),
    "346": _s("Merrill Center Crisis Stabilization Unit", "Fontana", "CA", "posting body"),
    "347": _s("Wellspring Center", "Fontana", "CA", "posting body (a 16-bed crisis residential treatment)"),
    "350": _s("Tree House North", "Anaheim", "CA", "posting body (a short-term crisis residential)"),
    "352": _s("Muriel Wright Recovery Center", "San Jose", "CA", "posting body (Muriel Wright Recovery Center CRT)"),
    "353": _s("Muriel Wright Recovery Center", "San Jose", "CA", "posting body (Muriel Wright Recovery Center SUTS)"),
    "378": _s("Riverside County Mental Health Rehabilitation Center", "Riverside", "CA", "posting body"),
    "502": _s("Recovery Center at Sarpy", "Bellevue", "NE", "posting body (The Recovery Center at Sarpy)"),
    "604": _s("Telecare Citrus House", "Downey", "CA", "posting body (Telecare Citrus House, 7225 Leeds St, Downey)"),
    "606": _s("Telecare Magnolia House", "Los Angeles", "CA", "posting body (Telecare Magnolia House, USC Campus)"),
    "608": _s("Serenity House", "San Mateo", "CA", "posting body (a 13 bed crisis residential)"),
    "612": _s("Desert Riverside Crisis Stabilization Unit", "Palm Springs", "CA", "posting body"),
    "616": _s("Alameda County Youth Crisis Stabilization Unit", "San Leandro", "CA", "posting body (Alameda County Youth CSU)"),
    "626": _s("Telecare Kern Adult Psychiatric Health Facility", "Bakersfield", "CA", "posting body"),
    "632": _s("Telecare MLK Adult Psychiatric Health Facility", "Los Angeles", "CA", "posting body"),
    "643": _s("Kern Youth Crisis Stabilization Unit", "Bakersfield", "CA", "posting body (Kern Youth CSU)"),
    "645": _s("Telecare Wellness Village Mental Health Rehabilitation Center", "Perris", "CA", "posting body"),
}
_TELECARE_NUM_RX = re.compile(r"Mental\s+Health\s*#?\s*(\d{2,4})\s*$", re.I)

# Cincinnati Children's: the posting's Workday site (its link's first path
# segment, stored on the row) names its campus. The Burnet campus is the
# hospital itself ("Cincinnati Children's"); buildings, offices, primary care
# sites, schools and remote stay on the label.
CINCY_SEGMENTS = {
    "libertycampus": _s("Cincinnati Children's Liberty Campus", "Liberty Township", "OH",
                        "board (Workday site Liberty-Campus; 7777 Yankee Rd, Liberty Township)"),
    "collegehillcampus": _s("Cincinnati Children's College Hill Campus", "Cincinnati", "OH",
                            "board (Workday site College-Hill-Campus; 5642 Hamilton Ave, Cincinnati)"),
}

# Akron Children's: the Mahoning Valley (Beeghly) campus in Boardman, when the
# posting itself names it ("Location: Mahoning Valley Campus - Onsite", "the
# Beeghly Emergency Department", "our Beeghly campus ED"). Akron (the main
# campus) IS "Akron Children's".
AKRON_MV = _s("Akron Children's Hospital Mahoning Valley", "Boardman", "OH",
              "posting (Location: Mahoning Valley Campus / Beeghly campus, Boardman)")
_AKRON_MV_RX = re.compile(r"(?i)\bLocation\s*:\s*Mahoning\s+Valley\s+Campus\b|\bBeeghly\s+(?:campus|emergency)\b(?!\s*(?:es\b|and\b))")
_AKRON_TWO_RX = re.compile(r"(?i)\bBeeghly\s+(?:and|&)\s+\w+\s+campuses\b|\bBeeghly\s+campuses\b")

# Mass General Brigham: "Site: <legal entity>" values facility_names lacks
# that are one named center or agency (its hospitals are facility_names').
MGB_SITES = {
    name_key("Spaulding Nursing and Therapy Center Brighton, Inc."): _s(
        "Spaulding Nursing and Therapy Center Brighton", "Brighton", "MA", "posting Site: line (the entity is the center)"),
    name_key("VNA & Hospice of Cooley Dickinson, Inc."): _s(
        "VNA & Hospice of Cooley Dickinson", "Northampton", "MA", "posting Site: line (the agency)"),
    name_key("The MGH Institute of Health Professions, Inc."): _s(
        "MGH Institute of Health Professions", "Charlestown", "MA", "posting Site: line (the school)"),
}
_MGB_SITE_RX = re.compile(r"(?m)^[ \t]*Site[ \t]*:[ \t]*([^\n]{3,120})")

# Logan Health: a site of the system the posting's own opening names. Logan
# Health Medical Center is left to the town table (its name is in the
# system's boilerplate); the others name themselves ("Logan Health Chester is
# seeking ...", "Why Logan Health - Shelby?").
LOGAN_SITES = (
    (re.compile(r"\bLogan\s+Health\s*[-–—]?\s*Chester\b"), "Logan Health Chester"),
    (re.compile(r"\bLogan\s+Health\s*[-–—]?\s*Cut\s+Bank\b"), "Logan Health Cut Bank"),
    (re.compile(r"\bLogan\s+Health\s*[-–—]?\s*Conrad\b"), "Logan Health Conrad"),
    (re.compile(r"\bLogan\s+Health\s*[-–—]?\s*Shelby\b"), "Logan Health Shelby"),
    (re.compile(r"\bLogan\s+Health\s*[-–—]?\s*Whitefish\b"), "Logan Health Whitefish"),
    (re.compile(r"\bLogan\s+Health\s+Children(?:'|’)?s\b"), "Logan Health Children's"),
    (re.compile(r"\bLogan\s+Health\s+Brendan\s+House\b"), "Logan Health Brendan House"),
)
LOGAN_NAMED = {
    "Logan Health Chester": _s("Logan Health Chester", "Chester", "MT", "cms 271334"),
    "Logan Health Cut Bank": _s("Logan Health Cut Bank", "Cut Bank", "MT", "cms 271337"),
    "Logan Health Conrad": _s("Logan Health Conrad", "Conrad", "MT", "cms 271324"),
    "Logan Health Shelby": _s("Logan Health Shelby", "Shelby", "MT", "cms 271328"),
    "Logan Health Whitefish": _s("Logan Health Whitefish", "Whitefish", "MT", "cms 271336"),
    "Logan Health Children's": _s("Logan Health Children's", "Kalispell", "MT", "posting (inside cms 270051's campus)"),
    "Logan Health Brendan House": _s("Logan Health Brendan House", "Kalispell", "MT",
                                     "posting (Logan Health Brendan House, skilled nursing)"),
}
LOGAN_OPENING_CHARS = 400
for _site in LOGAN_NAMED.values():
    facilities_workday.PLACED_EXTRA.setdefault(LOGAN, set()).add((_site[0], facilities_workday.key(_site[1]), _site[2]))

# Cleveland Clinic Nevada (the Workday site) postings that name the Lou Ruvo
# Center for Brain Health (Las Vegas).
CC_RUVO = _s("Cleveland Clinic Lou Ruvo Center for Brain Health", "Las Vegas", "NV",
             "posting (Facility Cleveland Clinic Nevada; At Cleveland Clinic Lou Ruvo Center for Brain Health)")
_CC_RUVO_RX = re.compile(r"(?i)\bLou\s+Ruvo\s+Cent(?:er|re)\b")

# UTHealth Houston (a university: its departments, the medical school and UT
# Physicians keep the system name with their town): a posting that names its
# psychiatric hospital.
UTH_HCPC = _s("UTHealth Houston Harris County Psychiatric Center", "Houston", "TX", "cms 454076")
_UTH_HCPC_RX = re.compile(r"\bHarris\s+County\s+Psychiatric\s+Center\b|\bHCPC\b")

DEFAULTS = {MGB: ("Boston", "MA"), UTH: ("Houston", "TX"), CLEVELAND: ("Cleveland", "OH"), LOGAN: ("Kalispell", "MT")}
SYSTEMS = frozenset({TELECARE, CINCY, AKRON, MGB, LOGAN, CLEVELAND, UTH})
SITE_NAMES = {
    TELECARE: {v[0] for v in TELECARE_PROGRAMS.values()},
    CINCY: {v[0] for v in CINCY_SEGMENTS.values()},
    AKRON: {AKRON_MV[0]},
    MGB: {v[0] for v in MGB_SITES.values()},
    LOGAN: set(LOGAN_NAMED),
    CLEVELAND: {CC_RUVO[0]},
    UTH: {UTH_HCPC[0]},
}
_REMOTE_RX = facilities_town.REMOTE_RX


def _segment(url) -> str:
    m = re.search(r"\.(?:myworkdayjobs|myworkdaysite)\.com/.*?/job/([^/?#]+)/", url or "", re.I)
    return re.sub(r"[^a-z0-9]", "", m.group(1).lower()) if m else ""


def read(system, title, description, url=""):
    """(site or None, what) for one posting of a SYSTEMS system. what:
    "site" (named), "field" (a Telecare field team or a program the table
    does not hold), "none" (the posting names no site), "two" (two sites)."""
    t = clean_text(title)
    body = str(description or "")
    if system == TELECARE:
        m = _TELECARE_NUM_RX.search(t)
        if not m:
            return None, "none"
        site = TELECARE_PROGRAMS.get(m.group(1))
        return (site, "site") if site else (None, "field")
    if system == CINCY:
        site = CINCY_SEGMENTS.get(_segment(url))
        return (site, "site") if site else (None, "none")
    if system == AKRON:
        if _AKRON_TWO_RX.search(body):
            return None, "two"
        return (AKRON_MV, "site") if _AKRON_MV_RX.search(body) else (None, "none")
    if system == MGB:
        m = _MGB_SITE_RX.search(body[:600])
        site = MGB_SITES.get(name_key(m.group(1))) if m else None
        return (site, "site") if site else (None, "none")
    if system == LOGAN:
        opening = clean_text(body[:LOGAN_OPENING_CHARS])
        hits = {n for rx, n in LOGAN_SITES if rx.search(t) or rx.search(opening)}
        if len(hits) > 1:
            return None, "two"
        return (LOGAN_NAMED[hits.pop()], "site") if hits else (None, "none")
    if system == CLEVELAND:
        if _segment(url) != "clevelandclinicnevada":
            return None, "none"
        return (CC_RUVO, "site") if (_CC_RUVO_RX.search(t) or _CC_RUVO_RX.search(body[:3000])) else (None, "none")
    if system == UTH:
        if _UTH_HCPC_RX.search(t) or _UTH_HCPC_RX.search(clean_text(body[:1500])):
            return UTH_HCPC, "site"
        return None, "none"
    return None, "none"


def _at_default(system, city, state) -> bool:
    d = DEFAULTS.get(system)
    return bool(d) and clean_text(city).lower() == d[0].lower() and clean_text(state).upper() == d[1]


def place(system, site, city, state):
    """(name, city, state) for a site, or None. A posting with no town, or at
    the system's stamped default, takes the site's own town (the posting
    names the site); a posting in the site's town keeps its own spelling; a
    posting in another town names no site (never guess a town)."""
    name, town, st = site[0], site[1], site[2]
    c, s = clean_text(city), clean_text(state).upper()
    if not c or _at_default(system, c, s):
        return name, town, st
    if s not in ("", st):
        return None
    if town_key(c) == town_key(town):
        return name, c, st
    return None


def decide(system, hospital_name, city, state, title="", description="", url=""):
    """(hospital_name, city, state, how) for one stored or scraped label row
    of a SYSTEMS system, or (None, None, None, how) when it keeps the label.
    Pure: no stored name. how: "named", "remote", "elsewhere" (the site is in
    another town than the posting), "field", "none", "two", "other"
    (already a site)."""
    system = (system or "").strip()
    if system not in SYSTEMS:
        return None, None, None, ""
    if clean_text(hospital_name) not in ("", system):
        return None, None, None, "other"
    site, what = read(system, title, description, url)
    if site is None:
        return None, None, None, what
    if _REMOTE_RX.search(clean_text(title)):
        return None, None, None, "remote"
    if system == LOGAN and site[0] != "Logan Health Brendan House" and (
            facilities_town.is_care_site(LOGAN, title) or facilities_town.is_multi_site(title, description)):
        return None, None, None, "care-site"
    placed = place(system, site, city, state)
    if placed is None:
        return None, None, None, "elsewhere"
    return placed[0], placed[1], placed[2], "named"


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): (stored hospital_name, city, state)} for
    the stored rows whose name is one of this pass's site names."""
    global _KNOWN
    out = {}
    for r in rows or []:
        system = (r.get("hospital_system") or "").strip()
        name, jid = clean_text(r.get("hospital_name")), r.get("job_id")
        if system not in SYSTEMS or jid in (None, "") or name not in SITE_NAMES.get(system, ()):
            continue
        out[(system, str(jid))] = (name, clean_text(r.get("city")), clean_text(r.get("state")).upper())
    _KNOWN = out
    return len(out)


def known_facility(system, job_id):
    return _KNOWN.get(((system or "").strip(), str(job_id)))


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of each system here whose hospital_name is not the
    system label, keyset-paged on id. Without credentials the map stays
    empty and nothing is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    try:
        sb_key = sb_keys.supabase_key()
    except Exception:
        sb_key = ""
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in sorted(SYSTEMS):
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        nflt = _urlparse.quote(f"neq.{system}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}&hospital_name={nflt}"
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
                got += len(chunk)
                last = chunk[-1]["id"]
                if len(chunk) < KNOWN_PAGE:
                    break
        except Exception as e:
            logger.warning(f"Phase 5 B: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Phase 5 B: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before the town tables) ────────────
def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != (clean_text(job.city), clean_text(job.state).upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    """Store the site on one Job of a SYSTEMS system still on the label.
    Returns decide()'s how, or "kept" (no body tonight: the stored site
    stands), or "" for any other system. hospital_system is never changed."""
    system = (job.hospital_system or "").strip()
    if system not in SYSTEMS:
        return ""
    name, city, state, how = decide(system, job.hospital_name, job.city, job.state, job.title,
                                    job.description, job.url)
    if name:
        _set(job, name, city, state)
        return how
    if how == "none" and len(str(job.description or "").strip()) < NO_BODY_CHARS \
            and system not in (TELECARE, CINCY):
        kept = known_facility(system, job.job_id)
        if kept:
            site = next((s for s in _sites(system) if s[0] == kept[0]), None)
            if site is not None:
                c, s = clean_text(job.city), clean_text(job.state).upper()
                if kept[1] and place(system, site, kept[1], kept[2]) is not None:
                    c, s = kept[1], kept[2]      # the stored decision stands, town included
                placed = place(system, site, c, s)
                if placed is not None:
                    _set(job, placed[0], placed[1], placed[2])
                    return "kept"
    return how


def _sites(system):
    if system == TELECARE:
        return list(TELECARE_PROGRAMS.values())
    if system == CINCY:
        return list(CINCY_SEGMENTS.values())
    if system == AKRON:
        return [AKRON_MV]
    if system == MGB:
        return list(MGB_SITES.values())
    if system == LOGAN:
        return list(LOGAN_NAMED.values())
    if system == CLEVELAND:
        return [CC_RUVO]
    if system == UTH:
        return [UTH_HCPC]
    return []


ROW_ERRORS: dict = {}     # system -> rows apply_facility raised on this run (each kept its name)


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}. A row
    apply_facility raises on keeps the name and town it had (the label) and
    is counted ("error"); the pass goes on to the next row."""
    stats: dict = {}
    for job in jobs:
        before = (job.hospital_name, job.city, job.state, job.location)
        try:
            how = apply_facility(job)
        except Exception as e:
            job.hospital_name, job.city, job.state, job.location = before
            n = ROW_ERRORS.get(job.hospital_system, 0) + 1
            ROW_ERRORS[job.hospital_system] = n
            if n <= 3:
                logger.warning(f"Phase 5 B: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how and how != "other":
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
