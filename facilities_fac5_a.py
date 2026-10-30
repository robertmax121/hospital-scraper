"""The hospital or named site on the rows twelve more large systems still filed under the system name (2026-10-29, facility scale phase 5, group A).

The owner, 2026-10-29: "i thought you were gonna edit all the large
multihospital systems". His two rules stand: (1) cards and job pages show the
HOSPITAL or named site ("Apply to <site>"); the system name and logo stay on
the job page and the system hub (hospital_system is never touched here: the
upsert key, the hub, the filters, the logo); (2) named sites (hospitals, named
clinics, centers, named communities) show their own name; generic offices,
home-based and remote postings, multi-site pools and regional roles show the
system name with the town; a town or a site is never guessed (the posting
itself, or the employer's own job record, must name it).

What each system's label rows carry, and what this module reads (stored label
rows read 2026-10-29 about 22:00 UTC, after the phase 2, 3 and 4 SQL):

  UT Southwestern Medical Center (Findly): the payload names no site below
        the system. The posting names it: a title ("Clinical Staff Pharmacist,
        Acute Care - Zale", "Accountant II - Texas Behavioral Health Center",
        "Oncology Infusion RN - PRN - SCCC"), a site line of its own
        ("Clements University Hospital", "Location : UTSW Simmons
        Comprehensive Cancer Center - Dallas, TX"), or a sentence placing the
        job at one site ("This position is located at Zale Lipshy Pavilion").
        Parkland, Children's, the VA and John Peter Smith are other employers'
        hospitals the faculty also cover: a posting naming one of them, or two
        UTSW sites, names none. The rest of UTSW (research, faculty, clinics,
        offices) is the medical center itself and keeps the system name.
  NYU Langone Health (Findly): the unit code of the title ("KP 18" Kimmel
        Pavilion, "TH14E" Tisch Hospital), "Orthopedic Hospital", "NYU Langone
        Hospital-Brooklyn", or the hospital's own opening paragraph ("NYU
        Langone Orthopedic Hospital at NYU Langone Health is ..."). The owner's
        call (10-29): off-site offices keep "NYU Langone Health" with their
        town; the Long Island entity names its hospital only in Mineola.
  MaineHealth (Infor): the body's first line is the entity: the named
        long-term care communities (Quarry Hill, Cove's Edge, Chase Point, Mid
        Coast Senior Health Center) by their own names; a hospital's lead
        sentence ("MaineHealth Maine Medical Center Portland, ME is ...") on a
        posting in that hospital's own town that is no clinic's.
  University of Iowa Hospitals and Clinics (SelectMinds): the system name is
        the main hospital's own name (CMS 160058, 200 Hawkins Dr): a University
        Campus posting is correct as it is. The "Location:" line names the
        other sites: Stead Family Children's Hospital, the Downtown campus
        (500 E Market St, the former Mercy Iowa City), the North Liberty
        hospital (701 W Forevergreen Rd); a line naming a town other than the
        Iowa City stamp gives the row that town (the label with its town).
  WVU Medicine (Workday): the site segment names a hospital the phase 2 table
        lacked ("Reynolds-Memorial-Hospital-RMH"); the body's "Address:" block
        gives the town of a row whose town is no town ("Neurodevelopmental
        Center", "WVU Innovations Corporation") or the Morgantown stamp.
  St. Luke's University Health Network (Workday): the site segment is a street
        address; a hospital's own street ("Easton-PA---1872-St-Lukes-Blvd",
        the Anderson Campus) names it. Office and clinic streets keep the
        label, as do a several-site posting, a title naming another campus,
        and a clinic or office title.
  UMass Memorial Health (Workday): the "Facility:" / "Location:" line ("UMass
        Memorial Health - Harrington Hospital"); the Nashoba Valley site
        ("Nashoba-SEF---Groton-MA", UMass Memorial Medical Center at Nashoba
        Valley, the posting's own name); the hospital towns (Milford,
        Leominster, Clinton, Southbridge, Marlborough) through the town tables
        (facilities_town: inpatient and hospital-unit titles only).
  Novant Health (Jibe): a "Facility:" / "Location:" line naming a hospital
        ("Community Hospital of Stokes", "Scotts Hill Medical Center").
  Corewell Health (Workday detail): the body's "Primary Location" SITE line
        naming a named site the facility table does not hold: the rehab and
        nursing centers (Kentridge, Leffingwell, Pine Ridge, Cedar by its own
        heading), Meijer Heart Center, Lemmen-Holton Cancer Pavilion.
  Beth Israel Lahey Health (Workday): The Herrick House (an assisted living
        residence on Beverly Hospital's upper campus, by the posting) and the
        town a "Location:" line names on a row at the Boston stamp.
  UCSF Health (Oracle): the work location (read every night, not stored)
        names the hospital building: Moffitt / Long Hospital (UCSF Helen Diller
        Medical Center at Parnassus Heights), Mission Bay Hospital, Children's
        Hospital Oakland, the Mount Zion hospital buildings; Hyde (900 Hyde St)
        and Stanyan (450 Stanyan St) are the Saint Francis and St. Mary's
        hospitals the table already names (extend_oracle).
  UNC Health: its "Entity:" line names the medical group, the faculty
        physicians or shared services on its label rows (offices, decision 2):
        nothing to build.

Hooks (scraper.py): the run start loads the names and towns stored on these
systems' rows (load_known_facilities: a night without a body keeps them),
finalize_jobs calls apply_facilities after the phase 4 passes, before the
town tables; facilities_oracle_other calls extend_oracle before its tables
are built; the UMass and Novant town tables register in facilities_town at
import. Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase5/a_work/changes.py).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_text as T
import facilities_town
import sb_keys  # the DATA project key, one reader for the repo
from city_utils import known_town

logger = logging.getLogger("scraper.facility_fac5a")

clean_text = T.clean_text
key = T.key

UTSW = "UT Southwestern Medical Center"
NYU = "NYU Langone Health"
MAINE = "MaineHealth"
UIHC = "University of Iowa Hospitals and Clinics"
WVU = "WVU Medicine"
SLUHN = "St. Luke's University Health Network"
UMASS = "UMass Memorial Health"
NOVANT = "Novant Health"
COREWELL = "Corewell Health"
BILH = "Beth Israel Lahey Health"
UCSF = "UCSF Health"

# The town the scraper stamps on a row whose feed gives none
# (scraper.SYSTEM_LOCATION_DEFAULTS / the feed's own default): a row there is
# moved only when the posting itself names its town or its site.
DEFAULT_TOWN = {
    WVU: ("Morgantown", "WV"),
    BILH: ("Boston", "MA"),
    UIHC: ("Iowa City", "IA"),
    SLUHN: ("Bethlehem", "PA"),
    UMASS: ("Worcester", "MA"),
}

# (clean public name) -> (town, state, source of the town)
SITES: dict[str, dict[str, tuple[str, str, str]]] = {
    UTSW: {
        "William P. Clements Jr. University Hospital": ("Dallas", "TX", "cms 450044 (6201 Harry Hines Blvd, Dallas)"),
        "Zale Lipshy Pavilion": ("Dallas", "TX", "address 5151 Harry Hines Blvd, Dallas (cms 450044)"),
        "Texas Behavioral Health Center": ("Dallas", "TX", "board: the posting (the state-funded psychiatric "
                                           "hospital UT Southwestern runs in Dallas)"),
        "UT Southwestern Simmons Comprehensive Cancer Center": ("Dallas", "TX", "board: the posting's Location line "
                                                                "(Dallas, TX)"),
    },
    NYU: {
        "Tisch Hospital": ("New York", "NY", "address 550 First Ave, New York (NYU Langone Hospitals, cms 330214)"),
        "Kimmel Pavilion": ("New York", "NY", "address 424 E 34th St, New York (NYU Langone Hospitals, cms 330214)"),
        "NYU Langone Orthopedic Hospital": ("New York", "NY", "cms 330389 (301 E 17th St, New York)"),
        "NYU Langone Hospital - Brooklyn": ("Brooklyn", "NY", "cms 330306 (150 55th St, Brooklyn)"),
    },
    MAINE: {
        "Quarry Hill": ("Camden", "ME", "board: every posting of it sits in Camden ('Perched atop the seaside "
                        "village of Camden, Maine, Quarry Hill')"),
        "Cove's Edge": ("Damariscotta", "ME", "board: every posting of it sits in Damariscotta"),
        "Chase Point": ("Damariscotta", "ME", "board: 'Located in beautiful Damariscotta, Chase Point'"),
        "Mid Coast Senior Health Center": ("Brunswick", "ME", "board: every posting of it sits in Brunswick"),
    },
    UIHC: {
        "University of Iowa Stead Family Children's Hospital": ("Iowa City", "IA", "address 200 Hawkins Dr, Iowa "
                                                                "City (inside cms 160058)"),
        "University of Iowa Health Care Medical Center Downtown": ("Iowa City", "IA", "address 500 E Market St, "
                                                                   "Iowa City (the posting's own line)"),
        "University of Iowa Health Care Medical Center North Liberty": ("North Liberty", "IA", "address 701 W "
                                                                        "Forevergreen Rd, North Liberty (the "
                                                                        "posting's own line)"),
    },
    WVU: {
        "Reynolds Memorial Hospital": ("Glen Dale", "WV", "cms 510013"),
        "Potomac Valley Hospital": ("Keyser", "WV", "cms 511315"),
        "Jefferson Medical Center": ("Ranson", "WV", "cms 511319"),
        "Harrison Community Hospital": ("Cadiz", "OH", "cms 361311"),
        "Weirton Medical Center": ("Weirton", "WV", "cms 510023"),
        "Wetzel County Hospital": ("New Martinsville", "WV", "cms 510072"),
        "United Hospital Center": ("Bridgeport", "WV", "cms 510006"),
        "Summersville Regional Medical Center": ("Summersville", "WV", "cms 511322"),
        "Uniontown Hospital": ("Uniontown", "PA", "cms 390041"),
        "WVU Medicine Children's Hospital": ("Morgantown", "WV", "address 1 Medical Center Dr, Morgantown"),
    },
    SLUHN: {
        "St. Luke's University Hospital - Bethlehem Campus": ("Bethlehem", "PA", "cms 390049 (801 Ostrum St)"),
        "St. Luke's Hospital - Anderson Campus": ("Easton", "PA", "cms 390326 (1872 St. Luke's Blvd)"),
        "St. Luke's Hospital - Easton Campus": ("Easton", "PA", "cms 390162 (250 S 21st St)"),
        "St. Luke's Hospital - Allentown Campus": ("Allentown", "PA", "address 1736 W Hamilton St, Allentown "
                                                   "(cms 390049 Allentown campus)"),
        "St. Luke's Hospital - Sacred Heart Campus": ("Allentown", "PA", "address 421 W Chew St, Allentown"),
        "St. Luke's Hospital - Grand View Campus": ("Sellersville", "PA", "cms 390057 (700 Lawn Ave)"),
        "St. Luke's Hospital - Upper Bucks Campus": ("Quakertown", "PA", "cms 390035 (3000 St. Luke's Dr, "
                                                     "Route 663 John Fries Hwy)"),
        "St. Luke's Hospital - Monroe Campus": ("Stroudsburg", "PA", "cms 390330 (100 St. Luke's Ln)"),
        "St. Luke's Miners Campus": ("Coaldale", "PA", "cms 390183 (360 W Ruddle St)"),
        "St. Luke's Warren Campus": ("Phillipsburg", "NJ", "cms 310060 (185 Roseberry St)"),
        "St. Luke's Hospital - Carbon Campus": ("Lehighton", "PA", "cms 390335 (500 St. Luke's Dr)"),
        "Geisinger St. Luke's Hospital": ("Orwigsburg", "PA", "cms 390332 (100 Paramount Blvd); the postings' "
                                          "own name"),
    },
    UMASS: {
        "UMass Memorial Health - Harrington Hospital": ("Southbridge", "MA", "cms 220019"),
        "UMass Memorial Health - HealthAlliance-Clinton Hospital": ("Leominster", "MA", "cms 220001 (Leominster "
                                                                    "campus; Clinton campus cms 220058)"),
        "UMass Memorial Health - Marlborough Hospital": ("Marlborough", "MA", "cms 220049"),
        "Milford Regional Medical Center": ("Milford", "MA", "cms 220090"),
        "UMass Memorial Medical Center - Hahnemann Campus": ("Worcester", "MA", "address 281 Lincoln St, Worcester"),
        "UMass Memorial Medical Center at Nashoba Valley": ("Groton", "MA", "board: the site 'Nashoba-SEF---"
                                                            "Groton-MA' and the posting's own name"),
    },
    NOVANT: {
        "Novant Health Community Hospital - Stokes": ("Danbury", "NC", "board: the posting's line "
                                                      "('Location: Danbury, NC')"),
        "Novant Health Scotts Hill Medical Center": ("Wilmington", "NC", "address 109 Scotts Hill Medical Dr, "
                                                     "Wilmington (the posting's own line)"),
        "Novant Health New Hanover Regional Medical Center": ("Wilmington", "NC", "cms 340141"),
    },
    COREWELL: {
        "Rehab and Nursing Center - Kentridge": ("Grand Rapids", "MI", "address 4118 Kalamazoo Ave SE, Grand "
                                                 "Rapids (the SITE line)"),
        "Rehab and Nursing Center - Leffingwell": ("Grand Rapids", "MI", "address 1001 Leffingwell Ave NE, Grand "
                                                   "Rapids (the SITE line)"),
        "Rehab and Nursing Center - Cedar": ("Grand Rapids", "MI", "address 1226 Cedar St NE, Grand Rapids (the "
                                             "SITE line; the posting's own heading names the center)"),
        "Pine Ridge Nursing and Rehab Center": ("Stevensville", "MI", "address 4368 S Cleveland Ave, Stevensville "
                                                "(the SITE line)"),
        "Meijer Heart Center": ("Grand Rapids", "MI", "address 150 Michigan St NE, Grand Rapids (the SITE line)"),
        "Lemmen-Holton Cancer Pavilion": ("Grand Rapids", "MI", "address 145 Michigan St NE, Grand Rapids (the "
                                          "SITE line)"),
    },
    BILH: {
        "The Herrick House": ("Beverly", "MA", "board: the posting ('The Herrick House, an assisted living "
                              "facility on the upper campus of the Beverly Hospital')"),
    },
}
SYSTEMS = frozenset(SITES)

_STATE_CODES = {v: k for k, v in facilities_town._STATE_NAMES.items()}


def _site(system, name):
    """(name, town, state) of a SITES entry."""
    town, st, _src = SITES[system][name]
    return name, town, st


def _one(names):
    names = {n for n in names if n}
    return next(iter(names)) if len(names) == 1 else None


def _same_town(a, b) -> bool:
    return bool(a) and bool(b) and facilities_town.town_key(a) == facilities_town.town_key(b)


def _at_default(system, city, state) -> bool:
    d = DEFAULT_TOWN.get(system)
    return bool(d) and _same_town(city, d[0]) and (state or "").strip().upper() in ("", d[1])


def _is_town(city, state) -> bool:
    return bool((city or "").strip()) and bool(known_town(city, (state or "").strip().upper() or None))


def _guard(title, description, offsite=True):
    """Why a posting is no one site's job whatever its text names, or "":
    a remote or hybrid posting, a posting for several sites, and (offsite)
    a clinic, practice, office or medical-group provider's title
    (facilities_town's own tests)."""
    if facilities_town.is_remote(title, description):
        return "remote"
    if facilities_town.is_multi_site(title, description) or _MULTI_RX.search(clean_text(title)):
        return "multi"
    if facilities_town.is_care_site("", title):
        return "care site"
    if offsite and facilities_town._offsite_by_title(title, description):
        return "office"
    return ""


# Titles of a role that floats across the system's sites or posts for several
# of them ("Opportunities at Select Campuses", "Network Float - Central
# Staffing", "MRI East Region Float Technologist", "Region/Float"): no one
# site's job (review A's rule, 2026-10-29).
_MULTI_RX = re.compile(
    r"\b(?:select|multiple|various|several|all)\s+(?:campuses|locations|sites|hospitals)\b|\bnetwork\s+float\b|"
    r"\bcentral\s+staffing\b|\bregion(?:al)?\s*(?:/|\s)\s*float\b|\b(?:east|west|north|south|central)\s+region\b|"
    r"\bfloat\s+(?:pool|technologist|phlebotomist|nurse|rn|team)\b|\b(?:mri|x-ray|cat\s+scan|ct)\s+float\b|"
    r"\bfloat\s+(?:resource|staff)\b|\bresource\s+pool\b", re.I)


def _lines(description) -> list[str]:
    return [ln.strip() for ln in T.clean_text_lines(description) if ln.strip()]


def _path_segment(url) -> str:
    p = url or ""
    if "/job/" not in p:
        return ""
    m = re.match(r"([^/]+)/", p.split("/job/", 1)[1])
    return m.group(1) if m else ""


# ══════════════════════════════════════════════════════════════════════════
#  UT Southwestern
# ══════════════════════════════════════════════════════════════════════════
_UTSW_CUES = (
    (re.compile(r"\b(?:William\s+P\.?\s+)?Clements(?:\s+Jr\.?)?(?:\s+University)?(?:\s+Hospital)?\b|\bCUH\b", re.I),
     "William P. Clements Jr. University Hospital"),
    (re.compile(r"\bZale(?:[\s-]+Lipshy)?\b", re.I), "Zale Lipshy Pavilion"),
    (re.compile(r"\bTexas\s+Behavioral\s+Health\s+Cent(?:er|re)\b|\bTBHC\b", re.I), "Texas Behavioral Health Center"),
    (re.compile(r"\bSimmons\s+(?:Comprehensive\s+)?Cancer\s+Cent(?:er|re)\b|\bSCCC\b", re.I),
     "UT Southwestern Simmons Comprehensive Cancer Center"),
)
# Other employers' hospitals UTSW's faculty and staff also cover: a posting
# naming one of them is no one UTSW site's.
_UTSW_OTHER_RX = re.compile(
    r"\bParkland\b|\bChildren'?s\s+(?:Medical\s+Cent(?:er|re)|Health)\b|\bVA\s+(?:Medical|North\s+Texas)\b|"
    r"\bVeterans\b|\bJohn\s+Peter\s+Smith\b|\bJPS\b|\bTexas\s+Health\b|\bBaylor\b|\bMethodist\b|\bMedical\s+City\b|"
    r"\bScottish\s+Rite\b|\bPresbyterian\b|\bother\s+(?:locations|sites|hospitals)\b", re.I)
_UTSW_SITE_LINE_RX = re.compile(
    r"^(?:(?:clinic\s+)?location\s*:\s*)?(?:UTSW\s+|UT\s+Southwestern\s+)?([A-Za-z.' -]{5,80}?)"
    r"(?:\s*[-,]\s*Dallas(?:,\s*(?:TX|Texas))?)?\s*$", re.I)
# "at <site>": a sentence that places the job there ("This position is
# located at Zale Lipshy Pavilion", "rotating assignments at Zale Lipshy
# Hospital", "your healthcare career with us at William P. Clements Jr.
# University Hospital"), not one that only mentions it ("across from ...").
_UTSW_AT_RX = re.compile(r"\b(?:at|in)\s+(?:the\s+)?(?:UTSW'?s?\s+|UT\s+Southwestern'?s?\s+)?$", re.I)


def _utsw_sites(text):
    return {n for rx, n in _UTSW_CUES if rx.search(text or "")}


def utsw_read(title, description):
    """(site name or None, how, raw)."""
    t = clean_text(title)
    body = str(description or "")[:12000]
    in_title = _utsw_sites(t)
    if in_title:
        if len(in_title) > 1:
            return None, "multi", t
        if _UTSW_OTHER_RX.search(t):
            return None, "other employer", t
        g = _guard(t, "", offsite=False)
        return (None, g, t) if g else (next(iter(in_title)), "title", t)
    # a line of its own naming one site ("Clements University Hospital",
    # "Location : UTSW Simmons Comprehensive Cancer Center - Dallas, TX")
    line_hits = set()
    for ln in _lines(body):
        if len(ln) > 90:
            continue
        m = _UTSW_SITE_LINE_RX.match(ln)
        if m:
            hit = _utsw_sites(m.group(1))
            if len(hit) == 1 and re.fullmatch(
                    r"(?:William\s+P\.?\s+)?Clements(?:\s+Jr\.?)?\s+University\s+Hospital|Zale\s+Lipshy\s+Pavilion|"
                    r"Texas\s+Behavioral\s+Health\s+Cent(?:er|re)|Simmons\s+(?:Comprehensive\s+)?Cancer\s+Cent(?:er|re)",
                    m.group(1).strip(), re.I):
                line_hits |= hit
    all_hits = _utsw_sites(body)
    if _UTSW_OTHER_RX.search(body):
        return None, "other employer", ""
    if len(all_hits) > 1:
        return None, "multi", ""
    if line_hits:
        g = _guard(t, body, offsite=False)
        return (None, g, "") if g else (next(iter(line_hits)), "site line", "")
    if len(all_hits) == 1:
        name = next(iter(all_hits))
        rx = dict((n, r) for r, n in _UTSW_CUES)[name]
        placed = any(_UTSW_AT_RX.search(body[max(0, m.start() - 40):m.start()]) for m in rx.finditer(body))
        if not placed:
            return None, "mention", ""
        g = _guard(t, body)
        return (None, g, "") if g else (name, "sentence", "")
    return None, "none", ""


# ══════════════════════════════════════════════════════════════════════════
#  NYU Langone
# ══════════════════════════════════════════════════════════════════════════
_NYU_TITLE = (
    (re.compile(r"\bKP\s?\d{1,2}\b|\bKimmel\b", re.I), "Kimmel Pavilion"),
    (re.compile(r"\bTH\s?\d{1,2}\s?(?:[EW]|East|West)?\b"), "Tisch Hospital"),
    (re.compile(r"\bTisch\b(?!\s+(?:Ctr|Cent(?:er|re))\b)", re.I), "Tisch Hospital"),
    (re.compile(r"\bOrthopa?edic\s+Hospital\b", re.I), "NYU Langone Orthopedic Hospital"),
    (re.compile(r"\bNYU\s+Langone\s+Hospital\s*[-–—]+\s*Brooklyn\b", re.I), "NYU Langone Hospital - Brooklyn"),
)
_NYU_LEAD = (
    (re.compile(r"^NYU\s+Langone\s+Orthopa?edic\s+Hospital\b", re.I), "NYU Langone Orthopedic Hospital"),
    (re.compile(r"^NYU\s+Langone\s+Hospital\s*[-–—]+\s*Brooklyn\b", re.I), "NYU Langone Hospital - Brooklyn"),
)


# the Faculty Group Practice's offices and ambulatory centres ("Sec II-Intake/
# Sched (FGP) - Manhattan, Tisch Ctr Essex Crossing"): no hospital (the
# owner's call: NYU Langone Health with the town)
_NYU_OFFICE_RX = re.compile(r"\bFGP\b|\bfaculty\s+group\s+practice\b|\bTisch\s+(?:Ctr|Cent(?:er|re))\b|"
                            r"\bambulatory\b", re.I)


def nyu_read(title, description):
    t = clean_text(title)
    if _NYU_OFFICE_RX.search(t):
        return None, "office", t
    hits = {n for rx, n in _NYU_TITLE if rx.search(t)}
    if len(hits) > 1:
        return None, "multi", t
    if hits:
        g = _guard(t, "", offsite=False)
        return (None, g, t) if g else (next(iter(hits)), "title", t)
    lines = _lines(description)
    if lines:
        for rx, n in _NYU_LEAD:
            if rx.search(lines[0]):
                g = _guard(t, description, offsite=False)
                return (None, g, lines[0][:80]) if g else (n, "lead", lines[0][:80])
    return None, ("none" if not lines else "unknown"), ""


# ══════════════════════════════════════════════════════════════════════════
#  MaineHealth
# ══════════════════════════════════════════════════════════════════════════
_MAINE_FIRST = {
    "quarry hill": "Quarry Hill",
    "coves edge": "Cove's Edge",
    "chase point": "Chase Point",
}
_MAINE_SPEC = T.spec_for(MAINE)
_MAINE_LEAD = T.Read("lead", 2, "entity")


def maine_read(title, description, city="", state=""):
    lines = _lines(description)
    if not lines:
        return None, "none", ""
    first = lines[0].split(" | ")[0].strip()
    fk = facilities_town.name_key(first)
    if fk in _MAINE_FIRST:
        return _MAINE_FIRST[fk], "first line", first
    if fk == "senior health center":
        text = clean_text(title) + " " + " ".join(lines[1:6])
        if re.search(r"\bMid\s*Coast\s+Senior\s+Health\s+Cent(?:er|re)\b", text, re.I) and \
                not re.search(r"\bThornton\s+Oaks\b", text, re.I):
            return "Mid Coast Senior Health Center", "first line", first
        return None, "unknown", first
    # a hospital's own lead sentence, read only where the posting sits in that
    # hospital's own town and is no clinic's, office's or practice's
    if _MAINE_SPEC is None:
        return None, "unknown", first
    for ln in lines[:2]:
        lk = T.lead_key(ln) + " "
        for k, site in _MAINE_SPEC.lead:
            if lk.startswith(k + " "):
                if facilities_town.is_offsite(title, description) or \
                        facilities_town.OFFSITE_RX.search(ln[len(site.name):][:120]):
                    return None, "office", ln[:80]
                g = _guard(title, description, offsite=False)
                if g:
                    return None, g, ln[:80]
                placed = T.place(_MAINE_SPEC, site, "entity", city, state)
                if placed is None:
                    return None, "elsewhere", ln[:80]
                return ("@" + placed[0]), "lead", ln[:80]
    return None, "unknown", first


# ══════════════════════════════════════════════════════════════════════════
#  University of Iowa Hospitals and Clinics
# ══════════════════════════════════════════════════════════════════════════
_UIHC_LOC_RX = re.compile(r"(?m)^[ \t*\-•]*(?:Work\s+)?Location[ \t]*:?[ \t]*(.*)$", re.I)
_UIHC_MAIN_RX = re.compile(
    r"\b(?:university\s+(?:main\s+)?campus|main\s+(?:university\s+)?campus|main\s+campus|ui(?:hc)?\s+main\s+campus|"
    r"ui\s+health\s+care\s+main\s+campus|medical\s+(?:center|campus)\s+university|general\s+hospital|"
    r"main\s+(?:university\s+)?hospital|john\s+pappajohn\s+pavill?ion|\bJPP\b|john\s+colloton\s+pavill?ion|\bJCP\b|"
    r"roy\s+(?:j\.?\s+)?carver\s+pavill?ion|\bRCP\b|boyd\s+tower|pomerantz\s+family\s+pavill?ion|"
    r"200\s+hawkins|university\s+medical\s+cent(?:er|re))\b", re.I)
_UIHC_STEAD_RX = re.compile(r"\bstead\s+family\s+children'?s\b|\bSFCH\b", re.I)
_UIHC_DOWNTOWN_RX = re.compile(r"\bdowntown\s+campus\b|\bmedical\s+(?:center|campus)\s+downtown\b|\b500\s*e\.?\s+market\b|"
                               r"\b500\s+east\s+market\b|\bMCD\b|\bjefferson\s+street\s+plaza\b", re.I)
_UIHC_NL_RX = re.compile(r"\b(?:medical\s+center\s+)?north\s+liberty\s+(?:hospital|campus|medical\s+campus)\b|"
                         r"\bmedical\s+center\s+north\s+liberty\b|\b701\s+w\.?\s+forevergreen\b", re.I)
# a plain town line: "Coralville, IA (or surround clinics ...)", "Bettendorf, IA",
# "Regular salaried position located in North Liberty, IA"
_UIHC_TOWN_RX = re.compile(r"^(?:regular\s+salaried\s+position\s+(?:is\s+)?located\s+in\s+)?"
                           r"([A-Za-z][A-Za-z .'-]{1,30}?),?\s+(?:IA|Iowa)\b\.?\s*(.*)$", re.I)


_UIHC_NAMES = {"stead": "University of Iowa Stead Family Children's Hospital",
               "downtown": "University of Iowa Health Care Medical Center Downtown",
               "nl": "University of Iowa Health Care Medical Center North Liberty"}
_UIHC_NL_TITLE_RX = re.compile(r"\bnorth\s+liberty\s+hospital\b", re.I)


def _uihc_location(description):
    """The value of the body's Location line (the next line when it is
    empty), or None."""
    body = str(description or "")
    m = _UIHC_LOC_RX.search(body)
    if not m:
        return None
    v = m.group(1).strip()
    if not v:
        rest = body[m.end():].lstrip("\r\n \t")
        v = rest.split("\n", 1)[0].strip() if rest else ""
    return clean_text(v)


def uihc_read(title, description):
    """(site name, how, raw, town override): the system label for a main
    campus posting ("main"), a named site, or the label with a town."""
    v = _uihc_location(description)
    t = clean_text(title)
    # a title naming one of the sites ("Staff Nurse - North Liberty Hospital
    # Emergency Department", "... - Downtown Campus") names it, whatever town
    # the Location line gives; the line must not name another site
    tk = [k for k, rx in (("stead", _UIHC_STEAD_RX), ("downtown", _UIHC_DOWNTOWN_RX), ("nl", _UIHC_NL_TITLE_RX))
          if rx.search(t)]
    if len(tk) == 1:
        other = [k for k, rx in (("stead", _UIHC_STEAD_RX), ("downtown", _UIHC_DOWNTOWN_RX), ("nl", _UIHC_NL_RX))
                 if k != tk[0] and rx.search(v or "")]
        if not other and not (v and _UIHC_MAIN_RX.search(v) and tk[0] != "stead"):
            g = _guard(t, "", offsite=False)
            if g:
                return None, g, t, None
            return _UIHC_NAMES[tk[0]], "title", t, None
    if v is None:
        return None, "none", "", None
    if not v:
        return None, "unknown", "", None
    kinds = []
    if _UIHC_STEAD_RX.search(v):
        kinds.append("stead")
    if _UIHC_DOWNTOWN_RX.search(v):
        kinds.append("downtown")
    if _UIHC_NL_RX.search(v):
        kinds.append("nl")
    main = bool(_UIHC_MAIN_RX.search(v))
    if len(kinds) > 1 or (kinds and main and kinds != ["stead"]):
        return None, "multi", v, None
    if kinds:
        g = _guard(title, "", offsite=False)
        if g:
            return None, g, v, None
        return _UIHC_NAMES[kinds[0]], "location line", v, None
    if main:
        return None, "main", v, None
    if re.search(r"\b(?:remote|hybrid)\b", v, re.I):
        return None, "remote", v, None
    m = _UIHC_TOWN_RX.match(v)
    if m:
        town, rest = m.group(1).strip(), m.group(2)
        # two towns ("Iowa City, IA and Coralville, IA", "Iowa City/Corridor");
        # "(and surround clinics/locations ...)" is no second town
        if re.search(r"\b(?:and|&)\s+[A-Z][a-z]+,?\s+(?:IA|Iowa)\b|[A-Z][a-z]+\s*/\s*[A-Z][a-z]+", v):
            return None, "multi", v, None
        kt = known_town(town, "IA")
        if kt:
            return None, "town line", v, (kt, "IA")
    m2 = re.search(r"\b(?:located\s+in|in)\s+(Coralville|North\s+Liberty|Iowa\s+City),?\s+(?:IA|Iowa)\b", v)
    if m2 and not re.search(r"\b(?:and|&|or)\b", v):
        kt = known_town(m2.group(1), "IA")
        if kt:
            return None, "town line", v, (kt, "IA")
    return None, "unknown", v, None


# ══════════════════════════════════════════════════════════════════════════
#  WVU Medicine
# ══════════════════════════════════════════════════════════════════════════
_WVU_SITE_SEGMENTS = {
    "reynolds-memorial-hospital-rmh": "Reynolds Memorial Hospital",
    "potomac-valley-hospital-pvh": "Potomac Valley Hospital",
    "jefferson-medical-center-jmc": "Jefferson Medical Center",
    "harrison-community-hospital-harr": "Harrison Community Hospital",
    "weirton-medical-center-wmch": "Weirton Medical Center",
    "wetzel-county-hospital-wtz": "Wetzel County Hospital",
    "united-hospital-center-uhc": "United Hospital Center",
    "summersville-regional-medical-center-srmc": "Summersville Regional Medical Center",
    "uniontown-hospital-untwn": "Uniontown Hospital",
    "wvu-medicine-golisano-childrens-hospital": "WVU Medicine Children's Hospital",
}
_WVU_ADDR_RX = re.compile(r"(?m)^[ \t]*Address[ \t]*:[ \t]*\n+[ \t]*([^\n]+)\n+[ \t]*([^\n]+)\n+[ \t]*([^\n]+)")


def wvu_address_town(description):
    """(town, ST) of the body's "Address:" block (street / town / state
    name), when the town is a known town of that state; else None."""
    m = _WVU_ADDR_RX.search(str(description or ""))
    if not m:
        return None
    town, st_name = clean_text(m.group(2)), clean_text(m.group(3)).lower()
    st = _STATE_CODES.get(st_name) or (st_name.upper() if len(st_name) == 2 else "")
    if not st or st.upper() not in facilities_town._STATE_NAMES:
        return None
    kt = known_town(town, st.upper())
    return (kt, st.upper()) if kt else None


# Short names and towns of WVU Medicine's hospitals: a title naming two of
# them ("BMC/JMC Anesthesiology", "Medical Oncology - Reynolds/ Wetzel",
# "located in Keyser, WV & Petersburg, WV") is no one hospital's.
_WVU_PLACES = (
    r"\bBMC\b|\bBerkeley\b|\bMartinsburg\b", r"\bJMC\b|\bJefferson\b|\bRanson\b", r"\bRMH\b|\bReynolds\b|\bGlen\s+Dale\b",
    r"\bWTZ\b|\bWetzel\b|\bNew\s+Martinsville\b", r"\bPVH\b|\bPotomac\s+Valley\b|\bKeyser\b", r"\bGrant\b|\bPetersburg\b",
    r"\bHARR\b|\bHarrison\b|\bCadiz\b", r"\bWMCH?\b|\bWeirton\b", r"\bUHC\b|\bUnited\s+Hospital\b|\bBridgeport\b",
    r"\bSRMC\b|\bSummersville\b", r"\bUNTWN\b|\bUniontown\b", r"\bRuby\b|\bMorgantown\b", r"\bWheeling\b",
    r"\bCamden\s+Clark\b|\bParkersburg\b", r"\bThomas\b|\bCharleston\b", r"\bGarrett\b|\bOakland\b",
    r"\bBarnesville\b", r"\bPrinceton\b|\bPCH\b", r"\bFairmont\b", r"\bBraxton\b|\bGassaway\b",
    r"\bJackson\s+General\b|\bRipley\b", r"\bSt\.?\s+Joseph'?s\b|\bBuckhannon\b")
_WVU_PLACE_RXS = tuple(re.compile(x, re.I) for x in _WVU_PLACES)


def wvu_read(title, description, url=""):
    seg = _path_segment(url).lower()
    name = _WVU_SITE_SEGMENTS.get(seg)
    if name:
        t = clean_text(title)
        if sum(1 for rx in _WVU_PLACE_RXS if rx.search(t)) > 1:
            return None, "multi", seg
        if re.search(r"\boutpatient\b|\bclinic\b", t, re.I):
            return None, "office", seg
        g = _guard(t, description)
        return (None, g, seg) if g else (name, "site", seg)
    return None, ("none" if not (description or "").strip() else "office"), seg


# ══════════════════════════════════════════════════════════════════════════
#  St. Luke's University Health Network
# ══════════════════════════════════════════════════════════════════════════
def _seg_key(s) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


_SLUHN_SEGMENTS = {_seg_key(k): v for k, v in {
    "Bethlehem-PA---801-Ostrum-St": "St. Luke's University Hospital - Bethlehem Campus",
    "Easton-PA---1872-St-Lukes-Blvd": "St. Luke's Hospital - Anderson Campus",
    "Easton-PA---250-S-21st-St": "St. Luke's Hospital - Easton Campus",
    "Allentown-PA---1736-Hamilton-St": "St. Luke's Hospital - Allentown Campus",
    "Allentown-PA---1736-W-Hamilton-St": "St. Luke's Hospital - Allentown Campus",
    "Allentown-PA---421-Chew-St": "St. Luke's Hospital - Sacred Heart Campus",
    "Allentown-PA---421-W-Chew-St": "St. Luke's Hospital - Sacred Heart Campus",
    "Allentown-Pa---421-W-Chew-Street": "St. Luke's Hospital - Sacred Heart Campus",
    "Sellersville-PA---700-Lawn-Ave": "St. Luke's Hospital - Grand View Campus",
    "Sellersville-PA---700-Lawn-Avenue": "St. Luke's Hospital - Grand View Campus",
    "Quakertown-PA---3000-John-Fries-Highway": "St. Luke's Hospital - Upper Bucks Campus",
    "Quakertown-PA---3000-St-Lukes-Drive": "St. Luke's Hospital - Upper Bucks Campus",
    "Stroudsburg-PA---100-St-Lukes-Lane": "St. Luke's Hospital - Monroe Campus",
    "Coaldale-PA---360-W-Ruddle-St": "St. Luke's Miners Campus",
    "Phillipsburg-NJ---185-Roseberry-St": "St. Luke's Warren Campus",
    "Lehighton-PA---500-St-Lukes-Drive": "St. Luke's Hospital - Carbon Campus",
    "Orwigsburg-PA---100-Paramount-Blvd": "Geisinger St. Luke's Hospital",
}.items()}
# A title naming a campus or a town of the network: a posting at one hospital's
# street whose title names another place is no one hospital's.
_SLUHN_PLACES = (
    (re.compile(r"\bBethlehem\b|\bFountain\s+Hill\b|\bUniversity\s+Hospital\b", re.I),
     "St. Luke's University Hospital - Bethlehem Campus"),
    (re.compile(r"\bAnderson\b", re.I), "St. Luke's Hospital - Anderson Campus"),
    (re.compile(r"\bEaston\b", re.I), "St. Luke's Hospital - Easton Campus"),
    (re.compile(r"\bAllentown\b", re.I), "St. Luke's Hospital - Allentown Campus"),
    (re.compile(r"\bSacred\s+Heart\b", re.I), "St. Luke's Hospital - Sacred Heart Campus"),
    (re.compile(r"\bGrand\s*View\b|\bSellersville\b", re.I), "St. Luke's Hospital - Grand View Campus"),
    (re.compile(r"\bUpper\s+Bucks\b|\bQuakertown\b", re.I), "St. Luke's Hospital - Upper Bucks Campus"),
    (re.compile(r"\bMonroe\b|\bStroudsburg\b", re.I), "St. Luke's Hospital - Monroe Campus"),
    (re.compile(r"\bMiners\b|\bCoaldale\b", re.I), "St. Luke's Miners Campus"),
    (re.compile(r"\bWarren\b|\bPhillipsburg\b", re.I), "St. Luke's Warren Campus"),
    (re.compile(r"\bCarbon\b|\bLehighton\b", re.I), "St. Luke's Hospital - Carbon Campus"),
    (re.compile(r"\bGeisinger\b|\bGSL\b|\bOrwigsburg\b", re.I), "Geisinger St. Luke's Hospital"),
    (re.compile(r"\bPalmerton\b|\bPottsville\b|\bCare\s*Now\b|\bHazleton\b|\bNazareth\b|\bMacungie\b|"
                r"\bCenter\s+Valley\b|\bTamaqua\b|\bHamburg\b|\bPenn\s+Foundation\b|\bWind\s+Gap\b", re.I), "-"),
)


def sluhn_read(title, description, url=""):
    seg = _path_segment(url)
    name = _SLUHN_SEGMENTS.get(_seg_key(seg))
    if not name:
        return None, ("street" if seg else "none"), seg
    t = clean_text(title)
    others = {n for rx, n in _SLUHN_PLACES if rx.search(t)} - {name}
    if others:
        return None, "other campus", t
    g = _guard(t, description)
    return (None, g, seg) if g else (name, "site", seg)


# ══════════════════════════════════════════════════════════════════════════
#  UMass Memorial, Novant, Corewell, Beth Israel Lahey (site lines)
# ══════════════════════════════════════════════════════════════════════════
_S = T.S
UMASS_SPEC = T.Spec(UMASS, ("Worcester", "MA"), (T.Read("label", r"Facility|Location", "site"),), (
    _S("UMass Memorial Health - Harrington Hospital", "Southbridge", "MA", "cms 220019",
       "UMass Memorial Health – Harrington Hospital", "Harrington Hospital", "UMass Memorial Health Harrington"),
    _S("UMass Memorial Health - HealthAlliance-Clinton Hospital", "Leominster", "MA", "cms 220001",
       "UMass Memorial Health – HealthAlliance-Clinton Hospital", "HealthAlliance-Clinton Hospital",
       "HealthAlliance Clinton Hospital", "Health Alliance Hospital", "HealthAlliance Hospital",
       campuses=(("Clinton", "MA", ""),)),
    _S("UMass Memorial Health - Marlborough Hospital", "Marlborough", "MA", "cms 220049",
       "UMass Memorial-Marlborough Hospital", "Marlborough Hospital", "UMass Memorial Medical Center Marlborough"),
    _S("Milford Regional Medical Center", "Milford", "MA", "cms 220090",
       "UMass Memorial Health – Milford Regional Medical Center"),
    _S("UMass Memorial Medical Center - Hahnemann Campus", "Worcester", "MA", "address 281 Lincoln St, Worcester",
       "UMass Memorial Medical Center – Hahnemann Campus", "Hahnemann Campus"),
), offices=("UMass Memorial Health", "UMass Memorial Health – Community Medical Group", "Milford Regional "
            "Physician Group", "MRPG- UMass Memorial Health", "UMass Memorial Medical Center",
            "UMass Memorial Health – Milford Regional Physician Group",
            "UMass Memorial Health – Tri River Family Health Center"))
_UMASS_NASHOBA_SEG = "nashobasefgrotonma"

NOVANT_SPEC = T.Spec(NOVANT, None, (T.Read("label", r"Facility|Location", "site"),), (
    _S("Novant Health Community Hospital - Stokes", "Danbury", "NC", "board", "Community Hospital of Stokes",
       "Novant Health Community Hospital of Stokes", "Stokes Community Hospital"),
    _S("Novant Health Scotts Hill Medical Center", "Wilmington", "NC", "address 109 Scotts Hill Medical Dr, Wilmington",
       "Scotts Hill Medical Center", "Scott's Hill Medical Center", "Novant Health Scott's Hill Medical Center"),
    _S("Novant Health New Hanover Regional Medical Center", "Wilmington", "NC", "cms 340141",
       "New Hanover Regional Medical Center", "NHRMC"),
))

_COREWELL_SITE_RX = re.compile(r"(?m)^[ \t]*Primary\s+Location[ \t]*:?[ \t]*\n*[ \t]*SITE\s*-\s*([^\n]+)$")
_COREWELL_SITES = (
    (re.compile(r"^Rehab\s*(?:&|and)\s*Nursing(?:\s+Cent(?:er|re))?\s*[-–]?\s*Kentridge\b", re.I),
     "Rehab and Nursing Center - Kentridge"),
    (re.compile(r"^Rehab\s*(?:&|and)\s*Nursing(?:\s+Cent(?:er|re))?\s*[-–]?\s*Leffingwell\b", re.I),
     "Rehab and Nursing Center - Leffingwell"),
    (re.compile(r"^Pine\s+Ridge\s+Nursing\s*(?:&|and)\s*Rehab\b", re.I), "Pine Ridge Nursing and Rehab Center"),
    (re.compile(r"^Meijer\s+Heart\s+Cent(?:er|re)\b", re.I), "Meijer Heart Center"),
    (re.compile(r"^Lemmen[\s-]+Holton\s+Cancer\s+Pavill?ion\b", re.I), "Lemmen-Holton Cancer Pavilion"),
)
_COREWELL_CEDAR_RX = re.compile(r"^1226\s+Cedar\s+St\b", re.I)
_COREWELL_CEDAR_HEAD_RX = re.compile(r"(?m)^[ \t]*About\s+(?:the\s+)?Rehab\s*(?:&|and)\s*Nursing\s+Cent(?:er|re)\s*"
                                     r"[-–]?\s*Cedar\b", re.I)


def corewell_read(title, description):
    m = None
    for m in _COREWELL_SITE_RX.finditer(str(description or "")):
        pass
    if m is None:
        return None, "none", ""
    site = clean_text(m.group(1))
    head = site.split(" - ")[0].strip()
    name = _one(n for rx, n in _COREWELL_SITES if rx.search(head))
    if not name and _COREWELL_CEDAR_RX.search(head) and _COREWELL_CEDAR_HEAD_RX.search(description or ""):
        name = "Rehab and Nursing Center - Cedar"
    if not name:
        return None, "office", site
    g = _guard(title, "", offsite=False)
    return (None, g, site) if g else (name, "site line", site)


_BILH_HERRICK_RX = re.compile(r"\bThe\s+Herrick\s+House\b|\bHerrick\s+House\b", re.I)
_BILH_LOC_RX = re.compile(r"(?m)^[ \t]*Location[ \t]*:[ \t]*([^\n]+)$", re.I)
_BOSTON_PARTS = frozenset(facilities_town.town_key(x) for x in (
    "Boston", "Charlestown", "Dorchester", "Roxbury", "Brighton", "Allston", "Jamaica Plain", "Roslindale",
    "Hyde Park", "West Roxbury", "Mattapan", "South Boston", "East Boston", "Back Bay", "Fenway"))
_BILH_TOWN_RX = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*,?\s+(MA|NH|Massachusetts|New\s+Hampshire)\b")


def bilh_read(title, description, url=""):
    seg = _path_segment(url)
    t = clean_text(title)
    if _seg_key(seg) == "theherrickhouse" or _BILH_HERRICK_RX.search(t) or \
            _BILH_HERRICK_RX.search(str(description or "")[:600]):
        g = _guard(t, "", offsite=False)
        return (None, g, seg, None) if g else ("The Herrick House", "site", seg, None)
    m = _BILH_LOC_RX.search(str(description or ""))
    if not m:
        return None, ("none" if not (description or "").strip() else "office"), seg, None
    v = clean_text(m.group(1))
    if re.search(r"\b(?:shared|between|and|&|or|multiple|various)\b|/", v, re.I):
        return None, "multi", v, None
    towns = {(mm.group(1), mm.group(2)) for mm in _BILH_TOWN_RX.finditer(v)}
    if len(towns) == 1:
        town, st = next(iter(towns))
        st = {"massachusetts": "MA", "new hampshire": "NH"}.get(st.lower(), st.upper())
        if st == "MA" and facilities_town.town_key(town) in _BOSTON_PARTS:
            return None, "own town", v, None
        kt = known_town(town, st)
        if kt:
            return None, "town line", v, (kt, st)
    return None, "office", v, None


# ══════════════════════════════════════════════════════════════════════════
#  The decision
# ══════════════════════════════════════════════════════════════════════════
def decide(system, hospital_name, city, state, title="", description="", url=""):
    """(hospital_name, city, state, how) for one stored or scraped row of a
    SYSTEMS system on the label, or (None, None, None, how) when it keeps the
    label as it is. Pure: no stored name. how: a named site ("title", "site",
    "site line", "location line", "lead", "first line", "sentence"), "town"
    (the label with the posting's own town), or why it keeps the label
    ("office", "remote", "multi", "elsewhere", "other employer", "main",
    "none", "unknown", ...); "other" when the row already names a site."""
    system = (system or "").strip()
    if system not in SYSTEMS:
        return None, None, None, ""
    if clean_text(hospital_name) not in ("", system):
        return None, None, None, "other"
    jc, js = clean_text(city), clean_text(state).upper()
    t = clean_text(title)
    town = None
    name, how = None, "none"
    if system == UTSW:
        name, how, _raw = utsw_read(t, description)
    elif system == NYU:
        name, how, _raw = nyu_read(t, description)
    elif system == MAINE:
        name, how, _raw = maine_read(t, description, jc, js)
        if name and name.startswith("@"):
            # a hospital the facilities_text table names, placed in the row's town
            site = _MAINE_SPEC.names[name[1:]]
            placed = T.place(_MAINE_SPEC, site, "entity", jc, js)
            return placed[0], placed[1], placed[2], how
    elif system == UIHC:
        name, how, _raw, town = uihc_read(t, description)
    elif system == WVU:
        name, how, _raw = wvu_read(t, description, url)
        if not name:
            at = wvu_address_town(description)
            if at and (not _is_town(jc, js) or _at_default(system, jc, js)) and not _same_town(at[0], jc):
                town, how = at, "town line"
    elif system == SLUHN:
        name, how, _raw = sluhn_read(t, description, url)
    elif system == UMASS:
        if _seg_key(_path_segment(url)) == _UMASS_NASHOBA_SEG:
            g = _guard(t, "", offsite=False)
            if g:
                return None, None, None, g
            name, how = "UMass Memorial Medical Center at Nashoba Valley", "site"
        else:
            site, kind, what, raw = T.read_text(UMASS_SPEC, description)
            if site is None:
                return None, None, None, what
            g = _guard(t, description, offsite=False)
            if g:
                return None, None, None, g
            placed = T.place(UMASS_SPEC, site, kind, jc, js, raw)
            if placed is None:
                return None, None, None, "elsewhere"
            return placed[0], placed[1], placed[2], "site line"
    elif system == NOVANT:
        site, kind, what, raw = T.read_text(NOVANT_SPEC, description)
        if site is None:
            return None, None, None, what
        g = _guard(t, description, offsite=False)
        if g:
            return None, None, None, g
        placed = T.place(NOVANT_SPEC, site, kind, jc, js, raw)
        if placed is None:
            return None, None, None, "elsewhere"
        return placed[0], placed[1], placed[2], "site line"
    elif system == COREWELL:
        name, how, _raw = corewell_read(t, description)
    elif system == BILH:
        name, how, _raw, town = bilh_read(t, description, url)
        if town and not (not _is_town(jc, js) or _at_default(system, jc, js)):
            town = None
            how = "own town"
    if name:
        n, ntown, nst = _site(system, name)
        if jc and js == nst and _same_town(jc, ntown):
            return n, jc, js, how
        if not jc or not _is_town(jc, js) or _at_default(system, jc, js) or system in (UTSW, NYU, SLUHN, WVU, UMASS) \
                and how in ("title", "site", "site line"):
            # the posting's own site, at its own town: a site segment or a
            # title naming the hospital is where the job is
            return n, ntown, nst, how
        return None, None, None, "elsewhere"
    if town and not _same_town(town[0], jc):
        return system, town[0], town[1], "town"
    return None, None, None, how


# ── Stored names and towns (nights without a body) ──────────────────────────
def _names(system) -> frozenset:
    """Every name this module writes for a system: its SITES, and the
    hospitals of the facilities_text table its MaineHealth lead read uses
    (with their campus names), the UMass and Novant line tables."""
    names = set(SITES.get(system, {}))
    spec = {MAINE: _MAINE_SPEC, UMASS: UMASS_SPEC, NOVANT: NOVANT_SPEC}.get(system)
    if spec is not None:
        names |= set(spec.names)
    return frozenset(names)


_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 40000


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): (stored hospital_name, city, state)} for
    rows whose stored name is one of this module's site names for that
    system, and label rows of a system whose towns this module fixes (WVU,
    UIHC, Beth Israel Lahey) stored at a known town (the stamped town too:
    a WVU row whose Address says Morgantown keeps Morgantown on a night its
    town field carries a building's name)."""
    global _KNOWN
    out = {}
    for r in rows or []:
        system = (r.get("hospital_system") or "").strip()
        jid = r.get("job_id")
        if system not in SYSTEMS or jid in (None, ""):
            continue
        name = clean_text(r.get("hospital_name"))
        city, st = clean_text(r.get("city")), clean_text(r.get("state")).upper()
        if name in _names(system) or (name == system and system in (WVU, UIHC, BILH) and _is_town(city, st)):
            out[(system, str(jid))] = (name, city, st)
    _KNOWN = out
    return len(out)


def known_facility(system, job_id):
    return _KNOWN.get(((system or "").strip(), str(job_id)))


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of each system here, keyset-paged on id
    (idx_system). Without credentials the map stays empty and nothing is
    kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in sorted(SYSTEMS):
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc&limit={KNOWN_PAGE}")
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
            logger.warning(f"Phase 5 A: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Phase 5 A: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before the town tables) ────────────
def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != (clean_text(job.city), clean_text(job.state).upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    """Store the site (or the posting's own town) on one Job of a SYSTEMS
    system still on the label. Returns decide()'s how, or "kept" (no body
    tonight: the stored site or town stands), or "" for any other system.
    hospital_system is never changed."""
    system = (job.hospital_system or "").strip()
    if system not in SYSTEMS:
        return ""
    name, city, state, how = decide(system, job.hospital_name, job.city, job.state, job.title,
                                    job.description, getattr(job, "url", "") or "")
    if name:
        _set(job, name, city, state)
        return how
    if how != "other" and len((job.description or "").strip()) < T.NO_BODY_CHARS:
        kept = known_facility(system, job.job_id)
        if kept:
            jc, js = clean_text(job.city), clean_text(job.state).upper()
            free = not jc or not _is_town(jc, js) or _at_default(system, jc, js) or (_same_town(jc, kept[1])
                                                                                      and js in ("", kept[2]))
            if free and kept[1]:
                _set(job, kept[0], kept[1], kept[2] or js)
                return "kept"
    return how


ROW_ERRORS: dict = {}     # system -> rows apply_facility raised on this run (each kept its name)


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}. A row
    apply_facility raises on keeps the name and town it had (the label) and
    is counted ("error"); the pass goes on to the next row."""
    stats: dict = {}
    for job in jobs:
        if (getattr(job, "hospital_system", "") or "").strip() not in SYSTEMS:
            continue
        before = (job.hospital_name, job.city, job.state, job.location)
        try:
            how = apply_facility(job)
        except Exception as e:
            job.hospital_name, job.city, job.state, job.location = before
            n = ROW_ERRORS.get(job.hospital_system, 0) + 1
            ROW_ERRORS[job.hospital_system] = n
            if n <= 3:
                logger.warning(f"Phase 5 A: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how and how != "other":
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats


# ══════════════════════════════════════════════════════════════════════════
#  TOWN TABLES (facilities_town: the one hospital of the system in a town)
# ══════════════════════════════════════════════════════════════════════════
_h = facilities_town._h

# UMass Memorial Health: Worcester holds the medical center's three campuses
# (named only by a title or line naming one); Milford, Leominster (and the
# same hospital's Clinton campus), Southbridge and Marlborough one hospital
# each. Webster and Fitchburg are not listed: their sites (Harrington's
# Webster campus, HealthAlliance's Burbank campus) are not inpatient
# hospitals the posting would name. Groton is the Nashoba Valley site, named
# by its own site segment above. The names are the ones this module writes.
UMASS_TOWNS = facilities_town._spec(UMASS, [
    _h("UMass Memorial Medical Center - University Campus", "Worcester", "MA", "cms 220163"),
    _h("UMass Memorial Medical Center - Memorial Campus", "Worcester", "MA", "site (119 Belmont St, Worcester)"),
    _h("UMass Memorial Medical Center - Hahnemann Campus", "Worcester", "MA", "site (281 Lincoln St, Worcester)"),
    _h("Milford Regional Medical Center", "Milford", "MA", "cms 220090"),
    _h("UMass Memorial Health - HealthAlliance-Clinton Hospital", "Leominster", "MA", "cms 220001", "Clinton"),
    _h("UMass Memorial Health - Harrington Hospital", "Southbridge", "MA", "cms 220019"),
    _h("UMass Memorial Health - Marlborough Hospital", "Marlborough", "MA", "cms 220049"),
], default=("Worcester", "MA"))
# A UMass posting whose title says it floats ("(Float)") or names a practice
# group is no one hospital's (decision 2).
facilities_town.CARE_SITE_BY_SYSTEM.setdefault(UMASS, re.compile(
    r"\b(?:float|community\s+medical\s+group|physician\s+group|MRPG|tri\s+river|outpatient|outreach|"
    r"medway|franklin|hopkinton|bellingham|uxbridge|northbridge|whitinsville|webster|fitchburg|charlton|"
    r"imaging\s+cent(?:er|re))\b", re.I))


# ══════════════════════════════════════════════════════════════════════════
#  UCSF HEALTH: ORACLE WORK-LOCATION SPELLINGS (nightly; also the live list)
# ══════════════════════════════════════════════════════════════════════════
# The requisitions' own work locations, read 2026-10-29 (781 live, one list
# pass): the hospital buildings below; the ambulatory building (ACC, 400
# Parnassus), the university's research and office buildings, the outpatient
# centres and Zuckerberg San Francisco General (the city's hospital) keep the
# label. "UCSF Hyde Hospital" (900 Hyde St) and "UCSF Stanyan Hospital" (450
# Stanyan St) are the Saint Francis and St. Mary's hospitals the table names.
PARNASSUS = "UCSF Helen Diller Medical Center at Parnassus Heights"
MISSION_BAY = "UCSF Medical Center at Mission Bay"
MOUNT_ZION = "UCSF Medical Center at Mount Zion"
ORACLE_EXTRA = {
    UCSF: {"hospitals": {
        PARNASSUS: ("San Francisco", "CA", "address 505 Parnassus Ave, San Francisco (cms 050454)"),
        MISSION_BAY: ("San Francisco", "CA", "address 1975 4th St, San Francisco (cms 050454)"),
        MOUNT_ZION: ("San Francisco", "CA", "address 1600 Divisadero St, San Francisco (cms 050454)"),
    }, "sites": {
        "Moffitt Hospital": PARNASSUS,
        "Long Hospital": PARNASSUS,
        "Mission Bay Hospital": MISSION_BAY,
        "Mt Zion Bldg A": MOUNT_ZION,
        "Mt Zion Bldg B": MOUNT_ZION,
        "Mt Zion Bldg D": MOUNT_ZION,
        "Children's Hospital Oakland": "UCSF Benioff Children's Hospital Oakland",
        "UCSF Hyde Hospital Main Bldg 68": "Saint Francis Memorial Hospital",
        "UCSF Hyde Hospital": "Saint Francis Memorial Hospital",
        "UCSF Stanyan Hospital": "St. Mary's Medical Center",
        "UCSF Stanyan Community Hospital Receiving": "St. Mary's Medical Center",
    }, "rules": (
        # Moffitt-Long's own street and its room codes ("505 PARNASSUS AVE
        # M-487", "ML RM M39C PHARMACY INPATIENT"); not the ambulatory building
        # ("ML ACC ...") or the outpatient pharmacy ("ML OP PHARMACY")
        (r"^505\s+parnassus\b(?!.*\bACC\b)|^ML\s+RM\b(?!.*\b(?:ACC|OP)\b)", PARNASSUS),
    )},
}


def extend_oracle(systems: dict) -> dict:
    """Add ORACLE_EXTRA to facilities_oracle_other's SYSTEMS (before its
    _build). All or nothing: a clean name already there must sit in the same
    town, a spelling already there must name the same hospital (ValueError
    otherwise, and `systems` is left as it was). Returns {system: added}."""
    staged, out = {}, {}
    for system, extra in ORACLE_EXTRA.items():
        src = systems.get(system)
        if src is None:
            raise ValueError(f"extend_oracle: {system} is not in facilities_oracle_other")
        hosp, sites = dict(src["hospitals"]), dict(src["sites"])
        rules = list(src.get("rules") or ())
        added = 0
        for name, place in extra["hospitals"].items():
            old = hosp.get(name)
            if old is not None and tuple(old[:2]) != tuple(place[:2]):
                raise ValueError(f"extend_oracle: {system} {name!r} is in {old[0]}, not {place[0]}")
            if old is None:
                hosp[name] = place
                added += 1
        folded = {key(k): v for k, v in sites.items()}
        for raw, target in extra["sites"].items():
            if target not in hosp:
                raise ValueError(f"extend_oracle: {system} {raw!r} names unknown {target!r}")
            prev = folded.get(key(raw))
            if prev is not None and prev != target:
                raise ValueError(f"extend_oracle: {system} {raw!r} already names {prev!r}")
            sites[raw] = target
            folded[key(raw)] = target
        for rx, target in extra.get("rules", ()):
            if target not in hosp:
                raise ValueError(f"extend_oracle: {system} rule names unknown {target!r}")
            rules.append((rx, target))
        staged[system] = (hosp, sites, tuple(rules))
        out[system] = added
    for system, (hosp, sites, rules) in staged.items():
        systems[system].update(hospitals=hosp, sites=sites, rules=rules)
    return out
