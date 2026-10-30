"""Military Health System: the military treatment facility a USAJOBS posting
names (2026-10-30, facility scale round 2, group mhs).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard. Decisions: (1) cards
and job pages show the HOSPITAL or site; hospital_system is never touched;
(2) named sites (hospitals, named clinics and centres) show their own name;
generic offices, home-based and remote jobs, multi-site pools and regional
roles show the system name with the town; a site or a town is never guessed.
Round 2 (2026-10-30): the Military Health System's rows filed under an agency
label are named by the military treatment facility (MTF) the posting names,
never a directorate inside it; postings listing many duty stations keep the
system.

What run_usajobs stored before this module (org code DD, every DoD
component): the facility a PositionLocation names ahead of its town, else the
hiring-organization line (UserArea.Details.SubAgencyName) up to its last
segment holding a hospital word, else the agency (OrganizationName). On
2026-10-30, 978 active rows: 746 on an agency label ("Military Treatment
Facilities under DHA" 299, "U.S. Army Medical Command" 162, "Defense Health
Agency" 149, "Department of the Air Force Headquarters" 101, "Naval Medical
Center" 35) and about 150 on a hiring-organization line read whole, so a
directorate or department rode along ("Womack Army Medical Center;
Directorate of Medicine; Department of Pediatrics", "DHN East Martin Army
Community Hospital Directorate of Nursing Services", "EVANS ARMY COMMUNITY
HOSPITAL", "Army Medical Center William Beaumont"). The agency-label rows
fell to the agency because the hiring-organization line names the MTF in a
form the hospital-word reader does not take ("Reynolds Army Health Clinic",
"Kimbrough Ambulatory Care Center", "60th MDG, Travis AFB", "DHN ATLANTIC MTF
CHERRY POINT ...", "Munson Army Heath Clinic"), or names a market or network
only, or is blank, in which case the JobSummary often names it ("This
position is located at Brooke Army Medical Center in San Antonio, Texas").

mhs_facility(m, city, state, loc_facility, n_towns, current) reads, in order:
  1. the facility the row's own PositionLocation names ahead of its town
     (the row's own place: "Naval Medical Center, San Diego, California"
     names Naval Medical Center San Diego; "Tripler Army Medical Center,
     Hawaii" names Tripler);
  2. the hiring-organization line: an MTF of MTFS it names in the row's own
     town; or, on an announcement of at most SITE_TOWN_CAP towns, the one
     hospital or medical center it names in the row's state (a medical
     center's clinic in a nearby town is its posting: "Madigan Army Medical
     Center, Dir. of Family Medicine" in Olympia, WA), or the one clinic it
     names there when the posting has one duty location and its town reads
     as none ("Kimbrough Ambulatory Care Center, Director of Medicine" at
     "Fort Meade Naval Facilities, Maryland", which clean_city drops);
  3. on an announcement of at most SITE_TOWN_CAP towns, the one MTF the
     JobSummary names in the row's own town ("located at WINN Army Medical
     Hospital, Fort Stewart, GA" in Fort Stewart is Winn Army Community
     Hospital; "located at Fort Hood, Texas" names no MTF: a base or its
     town alone never names one);
  4. none: the system's own name when the stored name is a medical agency
     label (AGENCY_LABELS) or the hiring-organization line names an MTF that
     is not this row's (a many-station posting); otherwise None, so the row
     keeps what run_usajobs read (a non-medical DoD organization that hires
     a nurse or physician, "U.S. Military Entrance Processing Command", keeps
     its name; the site prints the system for it either way).
A name that matches two MTFs of the row's place names none. Matching is by
MTFS' own names and abbreviations (case-insensitive names, case-sensitive
acronyms: "BAMC", "WRNMMC"); a generic Navy form ("Naval Medical Center",
"Naval Hospital", "Naval Health Clinic") with no town after it names the one
facility of that form in the row's own town.

The Air Force names its MTFs by medical group ("22nd Medical Group,
McConnell AFB"); AF_GROUPS lists the groups with their bases, and a group
whose hospital has a name of its own takes it (60th: David Grant USAF Medical
Center; 81st: Keesler Medical Center; 88th: Wright-Patterson Medical Center;
99th: Mike O'Callaghan Military Medical Center). The site prints the system
for a numbered group (a digit or "group" in a name is no site to its rule),
as it does today for the agency label these rows held.

hospital_system, job_id, url and slug are never touched. Shared with the
stored-row change list (claud_outputinput/facility_scale_2026-10-29/round2/
mhs_work/changes_mhs.py), so the database holds what the nightly writes.
"""
from __future__ import annotations

import re
from typing import NamedTuple

import facilities_town as FT
from facility_names import clean_text

MHS = "Military Health System"

# Announcements listing more duty towns than this keep the system unless the
# row's own location line names its facility (owner, 2026-10-30: "USAJOBS
# postings listing many duty stations keep the system"; the IHS rule of
# phase 4 counts five or more as a national announcement).
SITE_TOWN_CAP = 4

# The agency labels that stand for the whole Military Health System (USAJOBS
# agency subelements: data.usajobs.gov/api/codelist/agencysubelements, codes
# DD83, DD60, ARMC, AF00, NV18, AF2Z, AF2F, AF18, ARHS), lower case.
AGENCY_LABELS = frozenset({
    "military treatment facilities under dha",
    "defense health agency",
    "dha",
    "u.s. army medical command",
    "us army medical command",
    "united states army medical command",
    "department of the air force headquarters",
    "naval medical command",
    "air force medical command",
    "air force medical support agency",
    "defense health agency air force element",
    "u.s. army health services command",
})


class MTF(NamedTuple):
    name: str                 # the facility's own name, as the site prints it
    states: frozenset         # two-letter codes
    towns: frozenset          # facilities_town.town_key of the towns its rows carry
    kind: str                 # "hospital" (a medical center or hospital) or "clinic"
    names: tuple              # compiled patterns, case-insensitive
    acronyms: tuple           # compiled patterns, case-sensitive
    bare: object = None       # a generic form that names it only in its own town


_CI = re.I


def _m(name, states, towns, kind, names=(), acronyms=(), bare=None) -> MTF:
    return MTF(name, frozenset(s.upper() for s in states.split()), frozenset(FT.town_key(t) for t in towns), kind,
               tuple(re.compile(p, _CI) for p in names), tuple(re.compile(p) for p in acronyms),
               re.compile(bare, _CI) if bare else None)


# "Medical Center" as postings write it: Medical Center, Med Center, Med Ctr,
# Med Cntr, Medical Centre.
_MC = r"med(?:ical)?\.?\s+(?:center|centre|ctr|cntr)\b"
_NMC = r"\bnaval\s+" + _MC
_NMC_TOWNS = r"(?![\s,.\-\u2013]*(?:portsmouth|san\s+diego|camp\s+lejeune)\b)"
_NH = r"\bnaval\s+hosp(?:ital|\.)?"
_NH_TOWNS = (r"(?![\s,.\-\u2013]*(?:bremerton|camp\s+pendleton|jacksonville|pensacola|beaufort|oak\s+harbor|"
             r"twentynine\s+palms|lemoore)\b)")
_NHC = r"\bnaval\s+health\s+clinic"
_NHC_TOWNS = r"(?![\s,.\-\u2013]*(?:annapolis|corpus\s+christi|new\s+england|cherry\s+point|charleston|quantico|patuxent|hawaii|bangor)\b)"
_SEP = r"[\s,.\-\u2013:;/]*"

MTFS: tuple = (
    # ── Army (Defense Health Agency) medical centers and hospitals ─────────
    _m("Brooke Army Medical Center", "TX", ["Fort Sam Houston", "San Antonio", "JBSA Fort Sam Houston",
                                            "JBSA-Fort Sam Houston", "Joint Base San Antonio"], "hospital",
       [r"\bbrooke\s+army\s+" + _MC, r"\barmy\s+" + _MC + r"\s*brooke\b", r"\bbrooke\s+AMC\b"], [r"\bBAMC\b"]),
    _m("Carl R. Darnall Army Medical Center", "TX", ["Fort Hood", "Fort Cavazos", "Killeen"], "hospital",
       [r"\bdarnall\b"], [r"\bCRDAMC\b"]),
    _m("William Beaumont Army Medical Center", "TX", ["El Paso", "Fort Bliss"], "hospital",
       [r"\bwilliam\s+beaumont\b", r"\bbeaumont\s+army\s+" + _MC], [r"\bWBAMC\b"]),
    _m("Dwight D. Eisenhower Army Medical Center", "GA", ["Fort Gordon", "Fort Eisenhower", "Augusta"], "hospital",
       [r"\beisenhower\b(?:\s*\(\s*DDEAMC\s*\))?\s+army\s+" + _MC, r"\barmy\s+" + _MC + r"\s*eisenhower\b",
        r"\bAMC\s+eisenhower\b", r"\beisenhower\s+AMC\b"], [r"\bDDEAMC\b"]),
    _m("Madigan Army Medical Center", "WA", ["Tacoma", "Joint Base Lewis-McChord", "JBLM", "Fort Lewis",
                                             "Lakewood", "DuPont"], "hospital",
       [r"\bmadigan\b"], [r"\bMAMC\b"]),
    _m("Tripler Army Medical Center", "HI", ["Honolulu", "Tripler Army Medical Center", "Tripler AMC"], "hospital",
       [r"\btripler\b"], [r"\bTAMC\b"]),
    _m("Womack Army Medical Center", "NC", ["Fort Bragg", "Fort Liberty", "Fayetteville"], "hospital",
       [r"\bwomack\b"], [r"\bWAMC\b"]),
    _m("Walter Reed National Military Medical Center", "MD", ["Bethesda"], "hospital",
       [r"\bwalter\s+reed\s+(?:national|nat'?l|bethesda|NMMC)\b", r"\bWR\s+nat(?:ional|'?l)?\.?\s+mil"],
       [r"\bWRNMMC\b"]),
    _m("Alexander T. Augusta Military Medical Center", "VA", ["Fort Belvoir"], "hospital",
       [r"\b(?:alexander\s+t\.?|a\.?\s*t\.?)\s*augusta\s+military\s+" + _MC, r"\bfort\s+belvoir\s+community\s+hospital\b"],
       [r"\bATAMMC\b", r"\bFBCH\b"]),
    _m("Martin Army Community Hospital", "GA", ["Fort Benning", "Fort Moore", "Columbus"], "hospital",
       [r"\bmartin\s+army\s+(?:community\s+)?hospital\b", r"\barmy\s+community\s+hospital\s+martin\b"]),
    _m("Blanchfield Army Community Hospital", "KY TN", ["Fort Campbell"], "hospital",
       [r"\bblanc(?:h)?field\b"]),
    _m("Irwin Army Community Hospital", "KS", ["Fort Riley", "Junction City"], "hospital",
       [r"\birwin\s+army\s+community\s+hos\w*", r"\barmy\s+community\s+hospital\s+irwin\b"], [r"\bIACH\b"]),
    _m("General Leonard Wood Army Community Hospital", "MO", ["Fort Leonard Wood"], "hospital",
       [r"\bleonard\s+wood\s+(?:army\s+)?community\s+hospital\b"], [r"\bGLWACH\b"]),
    _m("Evans Army Community Hospital", "CO", ["Fort Carson", "Colorado Springs"], "hospital",
       [r"\bevans\s+army\b", r"\barmy\s+community\s+hospital\s+evans\b"]),
    _m("Bayne-Jones Army Community Hospital", "LA", ["Fort Polk", "Fort Johnson", "Leesville"], "hospital",
       [r"\bbayne[\s\-]*jones\b"], [r"\bBJACH\b"]),
    _m("Bassett Army Community Hospital", "AK", ["Fort Wainwright", "Fairbanks"], "hospital",
       [r"\bbassett\s+army\b", r"\barmy\s+community\s+hospital\s+bassett\b"]),
    _m("Weed Army Community Hospital", "CA", ["Fort Irwin"], "hospital",
       [r"\bweed\s+army\b", r"\barmy\s+community\s+hospital\s+weed\b"]),
    _m("Winn Army Community Hospital", "GA", ["Fort Stewart", "Hinesville"], "hospital",
       [r"\bwinn\s+army\b", r"\barmy\s+community\s+hospital\s+winn\b"]),
    _m("Keller Army Community Hospital", "NY", ["West Point"], "hospital",
       [r"\bkeller\s+army\b", r"\barmy\s+community\s+hospital\s+keller\b"]),
    # ── Army health centers and clinics ───────────────────────────────────
    _m("Raymond W. Bliss Army Health Center", "AZ", ["Fort Huachuca", "Sierra Vista"], "clinic",
       [r"\braymond\s+w\.?\s+bliss\b", r"\bbliss\s+army\s+(?:health|community)\b"]),
    _m("McDonald Army Health Center", "VA", ["Fort Eustis", "Newport News", "Joint Base Langley-Eustis"], "clinic",
       [r"\bmcdonald\s+army\s+health\b", r"\barmy\s+health\s+cent(?:er|re)\s+mcdonald\b"]),
    _m("Munson Army Health Center", "KS", ["Fort Leavenworth"], "clinic",
       [r"\bmunson\s+army\s+hea?l?th\b", r"\barmy\s+hea?l?th\s+(?:cent(?:er|re)|clinic)\s+munson\b"]),
    _m("Reynolds Army Health Clinic", "OK", ["Fort Sill", "Lawton"], "clinic",
       [r"\breynolds\s+army\b", r"\barmy\s+health\s+clinic\s+reynolds\b"]),
    _m("Guthrie Army Health Clinic", "NY", ["Fort Drum"], "clinic", [r"\bguthrie\b"]),
    _m("Lyster Army Health Clinic", "AL", ["Fort Rucker", "Fort Novosel"], "clinic", [r"\blyster\b"]),
    _m("Kenner Army Health Clinic", "VA", ["Fort Lee", "Fort Gregg-Adams"], "clinic",
       [r"\bkenner\s+army\b", r"\barmy\s+health\s+clinic\s+kenner\b"]),
    _m("Moncrief Army Health Clinic", "SC", ["Fort Jackson"], "clinic", [r"\bmoncrief\b"]),
    _m("Kimbrough Ambulatory Care Center", "MD", ["Fort Meade", "Fort George G. Meade", "Fort Meade Naval Facilities"],
       "clinic", [r"\bkimbrough\b"]),
    _m("Fox Army Health Center", "AL", ["Redstone Arsenal", "Huntsville"], "clinic", [r"\bfox\s+army\s+health\b"]),
    _m("Barquist Army Health Clinic", "MD", ["Fort Detrick", "Frederick"], "clinic", [r"\bbarquist\b"]),
    _m("Ireland Army Health Clinic", "KY", ["Fort Knox"], "clinic", [r"\bireland\s+army\b"]),
    _m("Dunham Army Health Clinic", "PA", ["Carlisle Barracks", "Carlisle"], "clinic", [r"\bdunham\s+(?:u\.?s\.?\s+)?army\b"]),
    _m("Schofield Barracks Health Clinic", "HI", ["Schofield Barracks", "Wahiawa"], "clinic",
       [r"\bschofield\s+barracks\s+health\s+clinic\b"]),
    # ── Navy medical centers and hospitals ────────────────────────────────
    _m("Naval Medical Center San Diego", "CA", ["San Diego"], "hospital",
       [_NMC + _SEP + r"san\s+diego\b"], [r"\bNMCSD\b"], bare=_NMC + _NMC_TOWNS),
    _m("Naval Medical Center Portsmouth", "VA", ["Portsmouth"], "hospital",
       [_NMC + _SEP + r"portsmouth\b"], [r"\bNMCP\b"], bare=_NMC + _NMC_TOWNS),
    _m("Naval Medical Center Camp Lejeune", "NC", ["Camp Lejeune", "Marine Corps Base Camp Lejeune", "Jacksonville"],
       "hospital", [_NMC + _SEP + r"camp\s+lejeune\b", r"\bcamp\s+lejeune\s+" + _MC], [r"\bNMCCL\b"],
       bare=_NMC + _NMC_TOWNS),
    _m("Naval Hospital Camp Pendleton", "CA", ["Camp Pendleton", "Oceanside"], "hospital",
       [_NH + _SEP + r"camp\s+pendleton\b"], [r"\bNHCP\b"], bare=_NH + _NH_TOWNS),
    _m("Naval Hospital Bremerton", "WA", ["Bremerton"], "hospital",
       [_NH + _SEP + r"bremerton\b"], bare=_NH + _NH_TOWNS),
    _m("Naval Hospital Jacksonville", "FL", ["Jacksonville", "Naval Air Station Jacksonville"], "hospital",
       [_NH + _SEP + r"jacksonville\b", r"\bjacksonville\s+naval\s+hospital\b"], bare=_NH + _NH_TOWNS),
    _m("Naval Hospital Pensacola", "FL", ["Pensacola", "Naval Air Station Pensacola"], "hospital",
       [_NH + _SEP + r"pensacola\b"], bare=_NH + _NH_TOWNS),
    _m("Naval Hospital Beaufort", "SC", ["Beaufort"], "hospital",
       [_NH + _SEP + r"beaufort\b"], bare=_NH + _NH_TOWNS),
    _m("Naval Hospital Oak Harbor", "WA", ["Oak Harbor", "Whidbey Island"], "hospital",
       [_NH + _SEP + r"oak\s+harbor\b"], bare=_NH + _NH_TOWNS),
    _m("Robert E. Bush Naval Hospital", "CA", ["Twentynine Palms"], "hospital",
       [r"\brobert\s+e\.?\s+bush\b", _NH + _SEP + r"twentynine\s+palms\b"], bare=_NH + _NH_TOWNS),
    # ── Navy health clinics and branch clinics ─────────────────────────────
    _m("Naval Health Clinic Annapolis", "MD", ["Annapolis"], "clinic",
       [_NHC + _SEP + r"annapolis\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Corpus Christi", "TX", ["Corpus Christi"], "clinic",
       [_NHC + _SEP + r"corpus\s+christi\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic New England", "RI", ["Newport"], "clinic",
       [_NHC + _SEP + r"new\s+england\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Cherry Point", "NC", ["Cherry Point", "Havelock"], "clinic",
       [_NHC + _SEP + r"cherry\s+point\b", r"\bMTF\s+cherry\s+point\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Charleston", "SC", ["Goose Creek", "Charleston"], "clinic",
       [_NHC + _SEP + r"charleston\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Quantico", "VA", ["Quantico"], "clinic",
       [_NHC + _SEP + r"quantico\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Patuxent River", "MD", ["Patuxent River"], "clinic",
       [_NHC + _SEP + r"patuxent\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Health Clinic Hawaii", "HI", ["Pearl Harbor", "Joint Base Pearl Harbor-Hickam"], "clinic",
       [_NHC + _SEP + r"hawaii\b"], bare=_NHC + _NHC_TOWNS),
    _m("Naval Branch Health Clinic Bangor", "WA", ["Bangor", "Silverdale"], "clinic",
       [r"\bnaval\s+(?:branch\s+)?health\s+clinic" + _SEP + r"bangor\b"]),
    _m("Naval Branch Health Clinic Groton", "CT", ["Groton", "Groton Submarine Base"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"groton\b"]),
    _m("Naval Branch Health Clinic Portsmouth", "ME NH", ["Kittery", "Kittery Point", "Portsmouth"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"portsmouth\b"]),
    _m("Naval Branch Health Clinic Dam Neck", "VA", ["Virginia Beach", "Dam Neck"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"dam\s+neck\b"]),
    _m("Naval Branch Health Clinic Yorktown", "VA", ["Yorktown"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"yorktown\b", r"\bMTF\s+yorktown\b"]),
    _m("Naval Branch Health Clinic Little Creek", "VA", ["Little Creek Amphibious Base", "Little Creek",
                                                          "Virginia Beach", "Norfolk"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"little\s+creek\b"]),
    _m("Naval Branch Health Clinic Oceana", "VA", ["Naval Air Station Oceana", "Virginia Beach", "Oceana"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"(?:nas\s+)?oceana\b",
        r"\bnbrhlthclinic\s+(?:nas\s+)?oceana\b"]),
    _m("Naval Branch Health Clinic Philadelphia", "PA", ["Philadelphia"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"philadelphia\b"]),
    _m("Naval Branch Health Clinic Belle Chasse", "LA", ["Belle Chasse"], "clinic",
       [r"\b(?:naval\s+)?(?:branch|nbr)\s+health\s+clinic" + _SEP + r"belle\s+chasse\b"]),
    # ── Air Force hospitals with a name of their own ───────────────────────
    _m("Wilford Hall Ambulatory Surgical Center", "TX", ["Lackland AFB", "JBSA Lackland", "Joint Base San Antonio-Lackland",
                                                          "San Antonio"], "hospital",
       [r"\bwilford\s+hall\b"], [r"\bWHASC\b"]),
)

# Air Force medical groups: number -> (base towns, state). A group whose
# hospital has a name of its own takes that name (AF_GROUP_NAMES).
AF_GROUPS = {
    2: ("LA", ["Barksdale AFB", "Bossier City"]),
    4: ("NC", ["Seymour Johnson AFB", "Goldsboro"]),
    5: ("ND", ["Minot AFB", "Minot"]),
    6: ("FL", ["MacDill AFB", "Tampa"]),
    7: ("TX", ["Dyess AFB", "Abilene"]),
    9: ("CA", ["Beale AFB", "Marysville"]),
    10: ("CO", ["USAF Academy", "United States Air Force Academy", "Colorado Springs"]),
    14: ("MS", ["Columbus AFB", "Columbus"]),
    15: ("HI", ["Hickam AFB", "Joint Base Pearl Harbor-Hickam", "Honolulu"]),
    17: ("TX", ["Goodfellow AFB", "San Angelo"]),
    19: ("AR", ["Little Rock AFB", "Jacksonville"]),
    20: ("SC", ["Shaw AFB", "Sumter"]),
    22: ("KS", ["McConnell AFB", "Wichita"]),
    23: ("GA", ["Moody AFB", "Valdosta"]),
    28: ("SD", ["Ellsworth AFB", "Box Elder", "Rapid City"]),
    42: ("AL", ["Maxwell AFB", "Montgomery"]),
    47: ("TX", ["Laughlin AFB", "Del Rio"]),
    49: ("NM", ["Holloman AFB", "Alamogordo"]),
    55: ("NE", ["Offutt AFB", "Bellevue", "Omaha"]),
    56: ("AZ", ["Luke AFB", "Glendale"]),
    60: ("CA", ["Travis AFB", "Fairfield"]),
    71: ("OK", ["Vance AFB", "Enid"]),
    72: ("OK", ["Tinker AFB", "Oklahoma City"]),
    75: ("UT", ["Hill AFB", "Ogden", "Layton"]),
    78: ("GA", ["Robins AFB", "Warner Robins"]),
    81: ("MS", ["Keesler AFB", "Biloxi"]),
    82: ("TX", ["Sheppard AFB", "Wichita Falls"]),
    87: ("NJ", ["McGuire AFB", "Joint Base McGuire-Dix-Lakehurst", "Fort Dix", "Lakehurst", "Wrightstown"]),
    88: ("OH", ["Wright-Patterson AFB", "Dayton", "Fairborn"]),
    90: ("WY", ["F.E. Warren AFB", "FE Warren AFB", "Cheyenne"]),
    92: ("WA", ["Fairchild AFB", "Spokane"]),
    96: ("FL", ["Eglin AFB", "Valparaiso", "Fort Walton Beach"]),
    97: ("OK", ["Altus AFB", "Altus"]),
    99: ("NV", ["Nellis AFB", "Las Vegas"]),
    319: ("ND", ["Grand Forks AFB", "Grand Forks"]),
    325: ("FL", ["Tyndall AFB", "Panama City"]),
    341: ("MT", ["Malmstrom AFB", "Great Falls"]),
    354: ("AK", ["Eielson AFB", "Fairbanks"]),
    355: ("AZ", ["Davis-Monthan AFB", "Tucson"]),
    366: ("ID", ["Mountain Home AFB", "Mountain Home"]),
    375: ("IL", ["Scott AFB", "Belleville"]),
    377: ("NM", ["Kirtland AFB", "Albuquerque"]),
    412: ("CA", ["Edwards AFB", "Edwards"]),
    436: ("DE", ["Dover AFB", "Dover"]),
    509: ("MO", ["Whiteman AFB", "Knob Noster"]),
    628: ("SC", ["Charleston AFB", "Joint Base Charleston", "North Charleston", "Charleston"]),
    633: ("VA", ["Langley AFB", "Joint Base Langley-Eustis", "Hampton"]),
    673: ("AK", ["Elmendorf AFB", "Joint Base Elmendorf-Richardson", "Anchorage"]),
}
AF_GROUP_NAMES = {
    60: "David Grant USAF Medical Center",
    81: "Keesler Medical Center",
    88: "Wright-Patterson Medical Center",
    99: "Mike O'Callaghan Military Medical Center",
}
# The hospitals of those groups by their own names (and Keesler's group by
# its base's name, "Keesler Medical Group").
_AF_OWN = {
    60: ([r"\bdavid\s+grant\b"], [r"\bDGMC\b"]),
    81: ([r"\bkeesler\s+medical\s+(?:center|centre|group)\b"], []),
    88: ([r"\bwright[\s\-]*patterson\s+medical\s+center\b"], []),
    99: ([r"\bo'?\s*callaghan\b"], []),
}
# "22nd Medical Group", "22d Medical Group", "60th MDG", "22 MDG" (an
# ordinal or MDG is required: "3 medical group practices" names none)
_AF_GROUP_RX = re.compile(r"\b(\d{1,3})(?:\s*(?:st|nd|rd|th|d)\s+(?:medical\s+group|med(?:ical)?\s+grp)|"
                          r"\s*(?:st|nd|rd|th|d)?\s*MDG)\b", re.I)


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"


def _af_mtf(n: int) -> MTF:
    st, towns = AF_GROUPS[n]
    own = _AF_OWN.get(n, ([], []))
    kind = "hospital" if n in AF_GROUP_NAMES else "clinic"
    return _m(AF_GROUP_NAMES.get(n) or f"{_ordinal(n)} Medical Group", st, towns, kind, own[0], own[1])


AF_MTFS = {n: _af_mtf(n) for n in AF_GROUPS}


def _clean(s) -> str:
    return clean_text(str(s or "").replace("\u00ad", "-"))


def find_mtfs(text) -> list:
    """[(MTF, bare)] every MTF the text names, in MTFS order; bare is True
    when only the generic Navy form named it (it then needs the row's own
    town). No text, no hits."""
    t = _clean(text)
    if not t:
        return []
    out, seen = [], set()
    for f in MTFS:
        if any(p.search(t) for p in f.names) or any(p.search(t) for p in f.acronyms):
            out.append((f, False))
            seen.add(f.name)
    for f in AF_MTFS.values():
        if f.name not in seen and (any(p.search(t) for p in f.names) or any(p.search(t) for p in f.acronyms)):
            out.append((f, False))
            seen.add(f.name)
    for g in _AF_GROUP_RX.finditer(t):
        f = AF_MTFS.get(int(g.group(1)))
        if f and f.name not in seen:
            out.append((f, False))
            seen.add(f.name)
    for f in MTFS:
        if f.bare is not None and f.name not in seen and f.bare.search(t):
            out.append((f, True))
    return out


def _here(hits, state: str, tk: str, need_town: bool) -> list:
    """The MTFs of hits in the row's state and, when need_town (or the hit is
    bare), in its own town."""
    out = []
    for f, bare in hits:
        if state not in f.states:
            continue
        if (need_town or bare) and not (tk and tk in f.towns):
            continue
        out.append(f)
    return out


def _one(found: list):
    names = {f.name for f in found}
    return found[0] if len(names) == 1 else None


def is_agency_label(name) -> bool:
    return _clean(name).lower().rstrip(".") in AGENCY_LABELS


def mhs_facility(m: dict, city: str = "", state: str = "", loc_facility: str = "", n_towns: int = 1,
                 current: str = ""):
    """The hospital_name of one Military Health System USAJOBS row (see the
    module note): an MTF's own name, the system's name, or None (keep
    `current`, what run_usajobs read)."""
    m = m or {}
    details = (m.get("UserArea") or {}).get("Details") or {}
    st = str(state or "").strip().upper()
    tk = FT.town_key(city) if city else ""
    n = max(1, int(n_towns or 1))
    # 1. the row's own location line
    hit = _one(_here(find_mtfs(loc_facility), st, tk, need_town=False))
    if hit:
        return hit.name
    # 2. the hiring-organization line
    subs = find_mtfs(details.get("SubAgencyName"))
    if subs:
        hit = _one(_here(subs, st, tk, need_town=True))
        if not hit and n <= SITE_TOWN_CAP:
            hit = _one([f for f in _here(subs, st, tk, need_town=False)
                        if f.kind == "hospital" or (not tk and n == 1)])
        if hit:
            return hit.name
    # 3. the summary, in the row's own town, on a posting of a few towns
    if n <= SITE_TOWN_CAP:
        hit = _one(_here(find_mtfs(details.get("JobSummary")), st, tk, need_town=True))
        if hit:
            return hit.name
    # 4. no MTF of this row. A location line run_usajobs read (a facility no
    # MTF of the table) keeps its name; an agency label, or a hiring line
    # naming another place's MTF, gives the system
    if is_agency_label(current) or not _clean(current) or _clean(current) == MHS:
        return MHS
    if _clean(loc_facility):
        return None
    return MHS if subs else None


def canonical_name(name, city: str = "", state: str = "") -> str | None:
    """The MTF a stored hospital_name names for its row (as mhs_facility reads
    a hiring-organization line of a one-town posting), or None."""
    st = str(state or "").strip().upper()
    tk = FT.town_key(city) if city else ""
    hits = find_mtfs(name)
    hit = _one(_here(hits, st, tk, need_town=True)) or _one([f for f in _here(hits, st, tk, need_town=False)
                                                             if f.kind == "hospital" or not tk])
    return hit.name if hit else None


def all_names() -> list:
    """Every name this module can write (tests, the site check)."""
    return [f.name for f in MTFS] + [f.name for f in AF_MTFS.values()]
