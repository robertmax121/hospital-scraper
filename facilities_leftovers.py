"""The hospital on the town-only Workday systems' rows, phase 3 (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard, and the applies are
retro-assigned to the right hospital; then, 17:30 UTC, "i thought you were
gonna edit all the large multihospital systems". His decisions: (1) cards and
job pages show the HOSPITAL ("Apply to <hospital>"); the system name and logo
stay on the job page and the system hub (hospital_system is never touched);
(2) named sites (hospitals, named clinics and centers) show their own name;
generic offices, home-based and remote jobs show the system name with the
town; a town is never guessed.

Phase 2 left these Workday systems on the system name: their Workday site
(locationsText and the link's path segment) is only a town ("Greenville-SC",
"Sioux-Falls-SD", "Charleston"), so facilities_workday had nothing to read.
Each posting still names its site twice where we can use it:

  1. the town. Most of these systems have one hospital per town (Avera St.
     Luke's in Aberdeen, Essentia St. Joseph's in Brainerd, Baystate Franklin
     in Greenfield). Their TownSpecs are registered here into
     facilities_town, so the TOWN family's rules decide exactly as they do for
     the 90 systems phase 2 built: a clinic, an office, a remote posting, a
     nursing home, a medical-group provider or a posting for several sites
     keeps the system name; a town with two of the system's hospitals
     (Norfolk, Columbia, Sioux Falls, Dallas, Toledo, Louisville) places only
     a posting whose title or location line names one; a stamped default town
     places only a posting that names its town. The stored decision stands
     on nights without a body (facilities_town.STORED).
  2. the detail payload's jobRequisitionLocation, the work site itself
     ("MRMC - Methodist Richardson Medical Center - MRMC", "BFMC Main
     Campus", "Oconee Medical Campus", "Brainerd Clinic", "Medical Office
     Building - Edmond"). The Workday detail pass reads it
     (scraper._apply_wd_posting_info -> apply_detail): a site the table lists
     as a hospital names it (in that hospital's own town, or a blank or
     stamped town); a clinic, an office building, a lab, home health or a
     street address keeps the system name, and the town pass is told to
     leave the row alone (SKIP_ATTR), so a clinic is never put on the town's
     hospital; a site the table does not know is left to the town rules.
     Stored bodies are read again for it once per DETAIL_REREAD_CYCLE nights
     until DETAIL_REREAD_UNTIL (reread_slot, kind "facility" in
     scraper._known_kind), so rows in the shared towns get their hospital
     and a town-rule row the work site calls a clinic goes back to the system
     name.

  MUSC Health also prints its employing entity in every body ("Entity /
  Medical University of South Carolina (MUSC - Univ)"): the university, the
  physician practices (UMA, MCP) and Carolina Family Care are no hospital
  (the TownSpec entity line names them as offices); the hospital authority
  (MUHA) runs every MUSC hospital, so it leaves the row to the town rules.

What is never done: hospital_system, job_id, url and slug are never touched;
a town is only written from a table entry's own street address or the
posting's own town; a row that already names a facility is left as it is.

Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/leftovers_work/changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import os
import re
import zlib
from datetime import date, datetime

import facilities_town as T
from facilities_town import _h, clean_text, name_key, town_key

ADDED: list = []          # the TownSpecs this module registers, in order


def _spec(system, hospitals, **kw):
    s = T._spec(system, hospitals, **kw)
    ADDED.append(s)
    return s


def _also_named(spec, name, *spellings) -> None:
    """Spellings a posting's title or location line uses for one hospital of
    a shared town ("Toledo Hospital" for ProMedica Toledo Hospital, "Norfolk
    General"): they name it there, as its full name would (TownSpec.keys and
    named_keys; the TOWN family's _names reads them)."""
    if name not in spec.keys:
        raise ValueError(f"{spec.system}: {name!r} is no hospital of the table")
    for sp in spellings:
        k = name_key(sp)
        spec.keys[name].add(k)
        spec.named_keys[name].add(k)


# ═══════════════════════════════════════════════════════════════════════════
# The TownSpecs. Hospitals: the CMS hospital list in the database
# (hospital_cms_alias -> hospitals, "cms <id>") or the hospital's published
# street address ("site (<street, town>)"); names as the rows of phase 2
# already spell them where a row carries one (MUSC, Sentara, Prisma, ProMedica,
# UofL), else the hospital's public name.
# ═══════════════════════════════════════════════════════════════════════════

# ── MUSC Health (musc.wd1, MUSC): sites are towns; Charleston is also the
# tenant default (WD_TENANT_DEFAULT), placed only when the link names it.
_MUSC = _spec("MUSC Health", [
    _h("MUSC Health University Medical Center", "Charleston", "SC", "cms 420004"),
    _h("MUSC Health East Cooper Medical Center", "Mount Pleasant", "SC", "site (2000 Hospital Dr, Mount Pleasant)"),
    _h("MUSC Health Florence Medical Center", "Florence", "SC", "site (805 Pamplico Hwy, Florence)"),
    _h("MUSC Health Columbia Medical Center Downtown", "Columbia", "SC", "site (2435 Forest Dr, Columbia)"),
    _h("MUSC Health Columbia Medical Center Northeast", "Columbia", "SC",
       "site (120 Gateway Corporate Blvd, Columbia)"),
    _h("MUSC Health Orangeburg Medical Center", "Orangeburg", "SC", "site (3000 St Matthews Rd, Orangeburg)"),
    _h("MUSC Health Lancaster Medical Center", "Lancaster", "SC", "cms 420036"),
    _h("MUSC Health Chester Medical Center", "Chester", "SC", "cms 420019"),
    _h("MUSC Health Kershaw Medical Center", "Camden", "SC", "cms 420048"),
    _h("MUSC Health Marion Medical Center", "Mullins", "SC", "cms 420055"),
    _h("MUSC Health Black River Medical Center", "Cades", "SC", "site (4568 Hemingway Hwy, Cades)"),
    _h("MUSC Health Williamsburg Medical Center", "Kingstree", "SC", "site (500 Nelson Blvd, Kingstree)"),
], default=("Charleston", "SC"), sites=[
    ("Shawn Jenkins Children's Hospital", "Charleston", "SC", ("Shawn Jenkins Children's Hospital",
                                                               "Shawn Jenkins Childrens Hospital")),
], entity=(r"\bEntity\s+(.+?)\s+Worker\s+(?:Type|Sub-Type)\b", {
    "Medical University of South Carolina (MUSC - Univ)": "",
    "University Medical Associates (UMA) Only Employees and Financials": "",
    "University Medical Associates (UMA)": "",
    "MUSC Community Physicians (MCP)": "",
    "Carolina Family Care, Inc. (CFC)": "",
    "MUSC Physicians": "",
}))

# ── Sentara Healthcare (sentara.wd1, scs): town sites; Norfolk is the default.
_SENTARA = _spec("Sentara Healthcare", [
    _h("Sentara Norfolk General Hospital", "Norfolk", "VA", "cms 490007"),
    _h("Sentara Leigh Hospital", "Norfolk", "VA", "cms 490046"),
    _h("Sentara Virginia Beach General Hospital", "Virginia Beach", "VA", "cms 490057"),
    _h("Sentara Princess Anne Hospital", "Virginia Beach", "VA", "cms 490119"),
    _h("Sentara Obici Hospital", "Suffolk", "VA", "cms 490044"),
    _h("Sentara CarePlex Hospital", "Hampton", "VA", "cms 490093"),
    _h("Sentara Williamsburg Regional Medical Center", "Williamsburg", "VA", "cms 490066"),
    _h("Sentara Northern Virginia Medical Center", "Woodbridge", "VA", "cms 490113"),
    _h("Sentara Martha Jefferson Hospital", "Charlottesville", "VA", "cms 490077"),
    _h("Sentara RMH Medical Center", "Harrisonburg", "VA", "cms 490004"),
    _h("Sentara Halifax Regional Hospital", "South Boston", "VA", "cms 490013"),
    _h("Sentara Albemarle Medical Center", "Elizabeth City", "NC", "cms 340109"),
], default=("Norfolk", "VA"))
_also_named(_SENTARA, "Sentara Norfolk General Hospital", "Norfolk General", "SNGH")
_also_named(_SENTARA, "Sentara Leigh Hospital", "Leigh Hospital", "SLH")
_also_named(_SENTARA, "Sentara Virginia Beach General Hospital", "Virginia Beach General", "SVBGH")
_also_named(_SENTARA, "Sentara Princess Anne Hospital", "Princess Anne Hospital", "SPAH")

# ── Prisma Health (prismahealth.wd5, both boards): town sites; Greenville
# is the default. Prisma Health Greer Memorial Hospital is at 830 S Buncombe
# Rd, Greer (the CMS list files it under Spartanburg, the county seat), so
# Spartanburg rows are not put on it.
_PRISMA = _spec("Prisma Health", [
    _h("Prisma Health Greenville Memorial Hospital", "Greenville", "SC", "cms 420078"),
    _h("Prisma Health Patewood Memorial Hospital", "Greenville", "SC", "cms 420102"),
    _h("Prisma Health North Greenville Hospital", "Travelers Rest", "SC", "site (807 N Main St, Travelers Rest)"),
    _h("Prisma Health Hillcrest Hospital", "Simpsonville", "SC", "cms 420037"),
    _h("Prisma Health Greer Memorial Hospital", "Greer", "SC", "site (830 S Buncombe Rd, Greer; cms 420033 files it under Spartanburg)"),
    _h("Prisma Health Baptist Easley Hospital", "Easley", "SC", "cms 420015"),
    _h("Prisma Health Oconee Memorial Hospital", "Seneca", "SC", "cms 420009"),
    _h("Prisma Health Laurens County Hospital", "Clinton", "SC", "cms 420038"),
    _h("Prisma Health Baptist Hospital", "Columbia", "SC", "cms 420086"),
    _h("Prisma Health Baptist Parkridge Hospital", "Columbia", "SC", "cms 420106"),
    _h("Prisma Health Richland Hospital", "Columbia", "SC", "cms 420018"),
    _h("Prisma Health Tuomey Hospital", "Sumter", "SC", "cms 420070"),
    _h("Blount Memorial Hospital", "Maryville", "TN", "cms 440011"),
], emit=("Prisma Health (Providers)",), default=("Greenville", "SC"))
_also_named(_PRISMA, "Prisma Health Greenville Memorial Hospital", "Greenville Memorial Hospital", "GMH")
_also_named(_PRISMA, "Prisma Health Patewood Memorial Hospital", "Patewood Memorial Hospital", "Patewood Hospital")
_also_named(_PRISMA, "Prisma Health Baptist Hospital", "Baptist Hospital")
_also_named(_PRISMA, "Prisma Health Baptist Parkridge Hospital", "Baptist Parkridge", "Parkridge Hospital")
_also_named(_PRISMA, "Prisma Health Richland Hospital", "Richland Hospital")

# ── Methodist Health System TX (methodisthealthsystem.wd1, MHS_Careers):
# "Town-Texas" sites. Dallas holds two hospitals.
_METHODIST = _spec("Methodist Health System TX", [
    _h("Methodist Dallas Medical Center", "Dallas", "TX", "cms 450051"),
    _h("Methodist Charlton Medical Center", "Dallas", "TX", "cms 450723"),
    _h("Methodist Richardson Medical Center", "Richardson", "TX", "cms 450537"),
    _h("Methodist Mansfield Medical Center", "Mansfield", "TX", "cms 670023"),
    _h("Methodist Midlothian Medical Center", "Midlothian", "TX", "cms 670300"),
    _h("Methodist Celina Medical Center", "Celina", "TX", "cms 670340"),
    _h("Methodist Southlake Medical Center", "Southlake", "TX", "cms 670132"),
    _h("Methodist McKinney Hospital", "McKinney", "TX", "cms 670069"),
    _h("Methodist Hospital for Surgery", "Addison", "TX", "cms 670073"),
])
_also_named(_METHODIST, "Methodist Dallas Medical Center", "Methodist Dallas", "MDMC")

# ── Adventist HealthCare (MD) (adventisthealthcare.wd1): "Town-MD" sites;
# Rockville is the default and holds Shady Grove and the rehabilitation
# hospital. Its pathology lab, home health and sports performance sites in
# Silver Spring and Rockville are no hospital (CARE_SITE_BY_SYSTEM below).
_ADVENTIST_MD = _spec("Adventist HealthCare (MD)", [
    _h("Adventist HealthCare Shady Grove Medical Center", "Rockville", "MD", "cms 210057"),
    _h("Adventist HealthCare Rehabilitation", "Rockville", "MD", "site (9909 Medical Center Dr, Rockville)"),
    _h("Adventist HealthCare White Oak Medical Center", "Silver Spring", "MD", "cms 210016"),
    _h("Adventist HealthCare Fort Washington Medical Center", "Fort Washington", "MD", "cms 210060"),
], default=("Rockville", "MD"))
_also_named(_ADVENTIST_MD, "Adventist HealthCare Shady Grove Medical Center", "Shady Grove Medical Center",
            "Shady Grove")
_also_named(_ADVENTIST_MD, "Adventist HealthCare Rehabilitation", "Adventist Rehabilitation",
            "Adventist Rehab")

# ── Avera Health (avera.wd5, avera-careers): "Town-ST" sites (every posting
# names its town; no default is stamped); Sioux Falls holds Avera McKennan
# and the Avera Heart Hospital. Every
# hospital the CMS alias list files under Avera (owned, managed or
# affiliated: their postings run on Avera's board), by its public name.
_AVERA = _spec("Avera Health", [
    _h("Avera McKennan Hospital & University Health Center", "Sioux Falls", "SD", "cms 430016"),
    _h("Avera Heart Hospital of South Dakota", "Sioux Falls", "SD", "cms 430095"),
    _h("Avera St. Luke's Hospital", "Aberdeen", "SD", "cms 430014"),
    _h("Avera Sacred Heart Hospital", "Yankton", "SD", "cms 430012"),
    _h("Avera St. Mary's Hospital", "Pierre", "SD", "cms 430015"),
    _h("Avera Queen of Peace Hospital", "Mitchell", "SD", "cms 431340"),
    _h("Avera Marshall Regional Medical Center", "Marshall", "MN", "cms 241359"),
    _h("Avera St. Anthony's Hospital", "O'Neill", "NE", "cms 281329"),
    _h("Avera Creighton Hospital", "Creighton", "NE", "cms 281331"),
    _h("Avera Gettysburg Hospital", "Gettysburg", "SD", "cms 431302 (Avera Missouri River Health Center)"),
    _h("Avera St. Benedict Health Center", "Parkston", "SD", "cms 431330"),
    _h("Avera Gregory Hospital", "Gregory", "SD", "cms 431338"),
    _h("Avera Tyler Hospital", "Tyler", "MN", "cms 241348"),
    _h("Avera Granite Falls Health Center", "Granite Falls", "MN", "cms 241343"),
    _h("Avera De Smet Memorial Hospital", "De Smet", "SD", "cms 431332"),
    _h("Avera Dells Area Hospital", "Dell Rapids", "SD", "cms 431331"),
    _h("Avera Flandreau Hospital", "Flandreau", "SD", "cms 431310"),
    _h("Avera Hand County Memorial Hospital", "Miller", "SD", "cms 431337"),
    _h("Avera Holy Family Hospital", "Estherville", "IA", "cms 161351"),
    _h("Avera Merrill Pioneer Hospital", "Rock Rapids", "IA", "cms 161321"),
    _h("Avera Weskota Memorial Hospital", "Wessington Springs", "SD", "cms 431324"),
    _h("Avera St. Michael's Hospital", "Tyndall", "SD", "cms 431327"),
    _h("Community Memorial Hospital", "Redfield", "SD", "cms 431316"),
    _h("Eureka Community Health Services", "Eureka", "SD", "cms 431308"),
    _h("Floyd Valley Healthcare", "Le Mars", "IA", "cms 161368"),
    _h("Freeman Regional Health Services", "Freeman", "SD", "cms 431313"),
    _h("Hegg Health Center", "Rock Valley", "IA", "cms 161336"),
    _h("Hendricks Community Hospital", "Hendricks", "MN", "cms 241339"),
    _h("Lakes Regional Healthcare", "Spirit Lake", "IA", "cms 160124"),
    _h("Landmann-Jungman Memorial Hospital", "Scotland", "SD", "cms 431317"),
    _h("Marshall County Healthcare Center", "Britton", "SD", "cms 431312"),
    _h("Osceola Community Hospital", "Sibley", "IA", "cms 161345"),
    _h("Pipestone County Medical Center", "Pipestone", "MN", "cms 241374"),
    _h("Platte Health Center", "Platte", "SD", "cms 431306"),
    _h("Regional Health Services of Howard County", "Cresco", "IA", "cms 161328"),
    _h("Sioux Center Health", "Sioux Center", "IA", "cms 161346"),
    _h("Spencer Hospital", "Spencer", "IA", "cms 160112"),
    _h("Tri Valley Health System", "Cambridge", "NE", "cms 281348"),
    _h("Wagner Community Memorial Hospital", "Wagner", "SD", "cms 431315"),
])
_also_named(_AVERA, "Avera McKennan Hospital & University Health Center", "Avera McKennan Hospital",
            "Avera McKennan", "McKennan")

# ── Essentia Health (essentiahealth.wd1): "Town-ST" sites; Duluth is the
# default and holds two hospitals (St. Mary's and Essentia Health-Duluth).
_ESSENTIA = _spec("Essentia Health", [
    _h("Essentia Health St. Mary's Medical Center", "Duluth", "MN", "cms 240002"),
    _h("Essentia Health-Duluth", "Duluth", "MN", "cms 240019"),
    _h("Essentia Health-Fargo", "Fargo", "ND", "cms 350070"),
    _h("Essentia Health St. Joseph's Medical Center", "Brainerd", "MN", "cms 240075"),
    _h("Essentia Health St. Mary's-Detroit Lakes", "Detroit Lakes", "MN", "cms 240101"),
    _h("Essentia Health-Virginia", "Virginia", "MN", "cms 240084"),
    _h("Essentia Health-Fosston", "Fosston", "MN", "cms 241357"),
    _h("Essentia Health-Deer River", "Deer River", "MN", "cms 241360"),
    _h("Essentia Health Northern Pines", "Aurora", "MN", "cms 241340"),
    _h("Essentia Health Holy Trinity Hospital", "Graceville", "MN", "cms 241321"),
    _h("Essentia Health-Ada", "Ada", "MN", "cms 241313"),
    _h("Essentia Health-Moose Lake", "Moose Lake", "MN", "cms 241350"),
    _h("Essentia Health-Sandstone", "Sandstone", "MN", "cms 241309"),
    _h("Essentia Health St. Mary's Hospital-Superior", "Superior", "WI", "cms 521329"),
], default=("Duluth", "MN"))
_also_named(_ESSENTIA, "Essentia Health St. Mary's Medical Center", "St. Mary's Medical Center")
_also_named(_ESSENTIA, "Essentia Health-Duluth", "Miller-Dwan")

# ── Baystate Health (baystatehealth.wd12): "Town-Massachusetts" sites;
# Springfield is the default and holds Baystate Medical Center (Baystate
# Children's Hospital inside it is named only when the posting names it).
_BAYSTATE = _spec("Baystate Health", [
    _h("Baystate Medical Center", "Springfield", "MA", "cms 220077"),
    _h("Baystate Franklin Medical Center", "Greenfield", "MA", "cms 220016"),
    _h("Baystate Noble Hospital", "Westfield", "MA", "cms 220065"),
    _h("Baystate Wing Hospital", "Palmer", "MA", "cms 220030"),
], default=("Springfield", "MA"), sites=[
    ("Baystate Children's Hospital", "Springfield", "MA", ("Baystate Children's Hospital",
                                                           "Baystate Childrens Hospital")),
])

# ── OU Health (oumedicine.wd5): sites are "Oklahoma-City" and a few towns;
# Oklahoma City is the default and holds the medical center and the
# children's hospital.
_OU = _spec("OU Health", [
    _h("OU Health University of Oklahoma Medical Center", "Oklahoma City", "OK",
       "site (700 NE 13th St, Oklahoma City)"),
    _h("Oklahoma Children's Hospital OU Health", "Oklahoma City", "OK", "site (1200 Everett Dr, Oklahoma City)"),
    _h("OU Health Edmond Medical Center", "Edmond", "OK", "site (1 S Bryant Ave, Edmond)"),
], default=("Oklahoma City", "OK"))
_also_named(_OU, "OU Health University of Oklahoma Medical Center", "University of Oklahoma Medical Center",
            "OU Medical Center", "Presbyterian Tower")
_also_named(_OU, "Oklahoma Children's Hospital OU Health", "Oklahoma Children's Hospital",
            "OU Children's Hospital")

# ── Cook Children's (cookchildrens.wd1): "Town-TX" sites.
_spec("Cook Children's", [
    _h("Cook Children's Medical Center", "Fort Worth", "TX", "cms 453300"),
    _h("Cook Children's Medical Center - Prosper", "Prosper", "TX", "cms 453317"),
])

# ── ProMedica (promedica.wd12): town sites ("Toledo", "Monroe"); Toledo is
# the default and holds Toledo Hospital and the Ebeid children's hospital.
# Its titles name the hospital ("RN - Toledo Hospital - Labor and Delivery").
_PROMEDICA = _spec("ProMedica", [
    _h("ProMedica Toledo Hospital", "Toledo", "OH", "cms 360068"),
    _h("ProMedica Russell J. Ebeid Children's Hospital", "Toledo", "OH", "site (2142 N Cove Blvd, Toledo)"),
    _h("ProMedica Flower Hospital", "Sylvania", "OH", "site (5200 Harroun Rd, Sylvania)"),
    _h("ProMedica Monroe Regional Hospital", "Monroe", "MI", "cms 230099"),
    _h("ProMedica Charles and Virginia Hickman Hospital", "Adrian", "MI", "cms 230005"),
    _h("ProMedica Memorial Hospital", "Fremont", "OH", "cms 360156"),
    _h("ProMedica Defiance Regional Hospital", "Defiance", "OH", "cms 361328"),
    _h("ProMedica Bay Park Hospital", "Oregon", "OH", "cms 360259"),
    _h("ProMedica Fostoria Community Hospital", "Fostoria", "OH", "cms 361318"),
], default=("Toledo", "OH"))
_also_named(_PROMEDICA, "ProMedica Toledo Hospital", "Toledo Hospital")
_also_named(_PROMEDICA, "ProMedica Russell J. Ebeid Children's Hospital", "Ebeid Children's Hospital",
            "Ebeid Childrens Hospital", "Russell J. Ebeid")

# ── UofL Health (uoflhealth.wd1): "Louisville-Kentucky" and a few towns;
# Louisville is the default and holds most of its hospitals.
_UOFL = _spec("UofL Health", [
    _h("UofL Health - UofL Hospital", "Louisville", "KY", "cms 180141"),
    _h("UofL Health - Jewish Hospital", "Louisville", "KY", "site (200 Abraham Flexner Way, Louisville)"),
    _h("UofL Health - Mary & Elizabeth Hospital", "Louisville", "KY", "site (1850 Bluegrass Ave, Louisville)"),
    _h("UofL Health - Peace Hospital", "Louisville", "KY", "site (2020 Newburg Rd, Louisville)"),
    _h("UofL Health - Frazier Rehabilitation Institute", "Louisville", "KY",
       "site (220 Abraham Flexner Way, Louisville)"),
    _h("UofL Health - Brown Cancer Center", "Louisville", "KY", "site (529 S Jackson St, Louisville)"),
    _h("UofL Health - South Hospital", "Shepherdsville", "KY", "site (1903 W Hebron Ln, Shepherdsville)"),
    _h("UofL Health - Shelbyville Hospital", "Shelbyville", "KY", "site (727 Hospital Dr, Shelbyville)"),
], default=("Louisville", "KY"))
_also_named(_UOFL, "UofL Health - UofL Hospital", "UofL Hospital", "University of Louisville Hospital")
_also_named(_UOFL, "UofL Health - Mary & Elizabeth Hospital", "Mary and Elizabeth Hospital",
            "Mary & Elizabeth Hospital")
_also_named(_UOFL, "UofL Health - Frazier Rehabilitation Institute", "Frazier Rehab", "Frazier Rehabilitation")

# ── John Muir Health (jmh.wd5): town sites; Walnut Creek is the default.
_spec("John Muir Health", [
    _h("John Muir Medical Center - Walnut Creek", "Walnut Creek", "CA", "cms 050180"),
    _h("John Muir Medical Center - Concord", "Concord", "CA", "cms 050496"),
    _h("John Muir Behavioral Health Center", "Concord", "CA", "cms 054131"),
], default=("Walnut Creek", "CA"))

# ── Houston Healthcare (hhc.wd5): town sites; Warner Robins is the default.
_spec("Houston Healthcare", [
    _h("Houston Medical Center", "Warner Robins", "GA", "site (1601 Watson Blvd, Warner Robins)"),
    _h("Perry Hospital", "Perry", "GA", "site (1120 Morningside Dr, Perry)"),
], default=("Warner Robins", "GA"))

# The systems' own named sites that are no hospital, by the posting's title
# (facilities_town.is_care_site): the row keeps the system name and its town.
T.CARE_SITE_BY_SYSTEM.update({
    "Adventist HealthCare (MD)": re.compile(
        r"\b(?:Capital\s+Choice|Pathology\s+Lab|Sports\s+Performance|Home\s+Health|Lourie\s+Center)\b", re.I),
    "Avera Health": re.compile(r"\b(?:Prince\s+of\s+Peace|Sister\s+James|Majestic\s+Bluffs|Avera\s+@\s*Home|"
                               r"Avera\s+at\s+Home|eCARE|Care\s+Center)\b", re.I),
    "MUSC Health": re.compile(r"\b(?:MUSC\s+Health\s+West|West\s+Ashley|Nexton|Summerville|North\s+Charleston|"
                              r"Kiawah|Clements\s+Ferry|Rivers\s+Ave(?:nue)?|Carnes\s+Crossroads|Park\s+West|"
                              r"Tidelands|Indian\s+Land|Bluffton|Beaufort|Garners\s+Ferry|FSED)\b", re.I),
})
T.EMIT.update({e: s for s in ADDED for e in s.emit})
SYSTEMS = frozenset(s.system for s in ADDED)


# ═══════════════════════════════════════════════════════════════════════════
# The detail payload: jobRequisitionLocation (the work site).
# Spelling -> (hospital name, city, state), or None for a site that is no
# hospital (an office building, a clinic, home health, a lab). Read off the
# detail payloads of 2026-10-29 (scan/work/live and phase3/leftovers_work/
# live/details_1029.json) and each hospital's own name.
# ═══════════════════════════════════════════════════════════════════════════
DETAIL: dict[str, dict[str, tuple | None]] = {}


def _d(system, rows: str) -> None:
    tbl = DETAIL.setdefault(system, {})
    spec = T.SPECS[system]
    for line in rows.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        site, name = [p.strip() for p in line.split("|")]
        k = name_key(site)
        if name == "-":
            val = None
        else:
            h = spec.names.get(name_key(name)) or next((x[:3] for x in spec.sites if x[0] == name), None)
            if h is None:
                raise ValueError(f"{system}: detail site {site!r} names {name!r}, no hospital of the table")
            val = (h[0], h[1], h[2])
        if k in tbl and tbl[k] != val:
            raise ValueError(f"{system}: detail site {site!r} listed twice")
        tbl[k] = val


_d("MUSC Health", """
MAIN HOSPITAL | MUSC Health University Medical Center
UNIVERSITY HOSPITAL | MUSC Health University Medical Center
UNIVERSITY HOSPITAL EXTENSION | MUSC Health University Medical Center
ASHLEY RIVER TOWER | MUSC Health University Medical Center
MUSC Health University Medical Center | MUSC Health University Medical Center
SHAWN JENKINS CHILDRENS HOSPITAL | Shawn Jenkins Children's Hospital
Shawn Jenkins Children's Hospital | Shawn Jenkins Children's Hospital
MUSC Health East Cooper Medical Center | MUSC Health East Cooper Medical Center
MUSC HEALTH EAST COOPER | MUSC Health East Cooper Medical Center
MUSC Health Florence Medical Center | MUSC Health Florence Medical Center
MUSC Health Columbia Medical Center Downtown | MUSC Health Columbia Medical Center Downtown
MUSC Health Columbia Medical Center Northeast | MUSC Health Columbia Medical Center Northeast
MUSC Health Orangeburg | MUSC Health Orangeburg Medical Center
MUSC Health Orangeburg Medical Center | MUSC Health Orangeburg Medical Center
MUSC Health Lancaster Medical Center | MUSC Health Lancaster Medical Center
MUSC Health Chester Medical Center | MUSC Health Chester Medical Center
MUSC Health Kershaw Medical Center | MUSC Health Kershaw Medical Center
MUSC Health Marion Medical Center | MUSC Health Marion Medical Center
MUSC Health Black River Medical Center | MUSC Health Black River Medical Center
MUSC Health Williamsburg Medical Center | MUSC Health Williamsburg Medical Center
MUSC HEALTH WEST | -
""")
_d("Sentara Healthcare", """
Sentara Norfolk General Hospital | Sentara Norfolk General Hospital
Sentara Leigh Hospital | Sentara Leigh Hospital
Sentara Virginia Beach General Hospital | Sentara Virginia Beach General Hospital
Sentara Princess Anne Hospital | Sentara Princess Anne Hospital
Sentara Obici Hospital | Sentara Obici Hospital
Sentara Louise Obici Hospital | Sentara Obici Hospital
Sentara CarePlex Hospital | Sentara CarePlex Hospital
Sentara Williamsburg Regional Medical Center | Sentara Williamsburg Regional Medical Center
Sentara Northern Virginia Medical Center | Sentara Northern Virginia Medical Center
Sentara Martha Jefferson Hospital | Sentara Martha Jefferson Hospital
Sentara RMH Medical Center | Sentara RMH Medical Center
Sentara Halifax Regional Hospital | Sentara Halifax Regional Hospital
Sentara Albemarle Medical Center | Sentara Albemarle Medical Center
""")
_d("Prisma Health", """
Greenville Memorial Hospital | Prisma Health Greenville Memorial Hospital
Patewood Memorial Hospital | Prisma Health Patewood Memorial Hospital
North Greenville Hospital | Prisma Health North Greenville Hospital
Hillcrest Memorial Hospital | Prisma Health Hillcrest Hospital
Hillcrest Hospital | Prisma Health Hillcrest Hospital
Greer Memorial Hospital | Prisma Health Greer Memorial Hospital
Baptist Easley Hospital | Prisma Health Baptist Easley Hospital
Oconee Memorial Hospital | Prisma Health Oconee Memorial Hospital
Oconee Medical Campus | Prisma Health Oconee Memorial Hospital
Laurens County Hospital | Prisma Health Laurens County Hospital
Baptist Hospital | Prisma Health Baptist Hospital
Baptist Parkridge Hospital | Prisma Health Baptist Parkridge Hospital
Richland Hospital | Prisma Health Richland Hospital
Tuomey Hospital | Prisma Health Tuomey Hospital
Blount Memorial Hospital | Blount Memorial Hospital
""")
_d("Methodist Health System TX", """
Methodist Dallas Medical Center | Methodist Dallas Medical Center
Methodist Charlton Medical Center | Methodist Charlton Medical Center
Methodist Richardson Medical Center | Methodist Richardson Medical Center
Methodist Mansfield Medical Center | Methodist Mansfield Medical Center
Methodist Midlothian Medical Center | Methodist Midlothian Medical Center
Methodist Celina Medical Center | Methodist Celina Medical Center
Methodist Southlake Medical Center | Methodist Southlake Medical Center
Methodist McKinney Hospital | Methodist McKinney Hospital
Methodist Hospital for Surgery | Methodist Hospital for Surgery
""")
_d("Adventist HealthCare (MD)", """
Shady Grove Medical Center | Adventist HealthCare Shady Grove Medical Center
Adventist HealthCare Shady Grove Medical Center | Adventist HealthCare Shady Grove Medical Center
White Oak Medical Center | Adventist HealthCare White Oak Medical Center
Adventist HealthCare White Oak Medical Center | Adventist HealthCare White Oak Medical Center
Fort Washington Medical Center | Adventist HealthCare Fort Washington Medical Center
Adventist Rehabilitation - Rockville | Adventist HealthCare Rehabilitation
Adventist Rehabilitation Rockville | Adventist HealthCare Rehabilitation
AHC Sports Performance Institute | -
Capital Choice Pathology Lab | -
""")
_d("Avera Health", """
Avera McKennan Hospital | Avera McKennan Hospital & University Health Center
Avera Heart Hospital | Avera Heart Hospital of South Dakota
Avera St Luke's Hospital | Avera St. Luke's Hospital
Avera Sacred Heart Hospital | Avera Sacred Heart Hospital
Avera St Mary's Hospital | Avera St. Mary's Hospital
Avera Queen of Peace Hospital | Avera Queen of Peace Hospital
Avera Marshall Regional Medical Center | Avera Marshall Regional Medical Center
Avera St Anthony's Hospital | Avera St. Anthony's Hospital
Avera Creighton Hospital | Avera Creighton Hospital
Avera Gettysburg Hospital | Avera Gettysburg Hospital
Avera St Benedict Health Center | Avera St. Benedict Health Center
Avera Gregory Hospital | Avera Gregory Hospital
Avera Tyler Hospital | Avera Tyler Hospital
Sioux Center Health | Sioux Center Health
Floyd Valley Healthcare | Floyd Valley Healthcare
Lakes Regional Healthcare | Lakes Regional Healthcare
Spencer Hospital | Spencer Hospital
Pipestone County Medical Center | Pipestone County Medical Center
Regional Health Services of Howard County | Regional Health Services of Howard County
Hegg Health Center | Hegg Health Center
Avera Health | -
""")
_d("Essentia Health", """
St. Mary's Medical Center | Essentia Health St. Mary's Medical Center
Essentia Health St. Mary's Medical Center | Essentia Health St. Mary's Medical Center
Essentia Health-Duluth | Essentia Health-Duluth
Essentia Health-Fargo | Essentia Health-Fargo
Fargo Hospital | Essentia Health-Fargo
St. Joseph's Medical Center | Essentia Health St. Joseph's Medical Center
Essentia Health St. Joseph's Medical Center | Essentia Health St. Joseph's Medical Center
St. Mary's-Detroit Lakes | Essentia Health St. Mary's-Detroit Lakes
Essentia Health-Virginia | Essentia Health-Virginia
Essentia Health-Fosston | Essentia Health-Fosston
Essentia Health-Deer River | Essentia Health-Deer River
Essentia Health Northern Pines | Essentia Health Northern Pines
Essentia Health Holy Trinity Hospital | Essentia Health Holy Trinity Hospital
Essentia Health-Ada | Essentia Health-Ada
Essentia Health-Moose Lake | Essentia Health-Moose Lake
Essentia Health-Sandstone | Essentia Health-Sandstone
St. Mary's Hospital-Superior | Essentia Health St. Mary's Hospital-Superior
""")
_d("Baystate Health", """
BMC Main Campus | Baystate Medical Center
Baystate Medical Center | Baystate Medical Center
BFMC Main Campus | Baystate Franklin Medical Center
Baystate Franklin Medical Center | Baystate Franklin Medical Center
BNH Main Campus | Baystate Noble Hospital
Baystate Noble Hospital | Baystate Noble Hospital
BWH Main Campus | Baystate Wing Hospital
BWMC Main Campus | Baystate Wing Hospital
Baystate Wing Hospital | Baystate Wing Hospital
Baystate Children's Hospital | Baystate Children's Hospital
BHH Main Campus | -
""")
_d("OU Health", """
University of Oklahoma Medical Center | OU Health University of Oklahoma Medical Center
OU Health University of Oklahoma Medical Center | OU Health University of Oklahoma Medical Center
Presbyterian Tower | OU Health University of Oklahoma Medical Center
Oklahoma Children's Hospital | Oklahoma Children's Hospital OU Health
Children's Hospital | Oklahoma Children's Hospital OU Health
Edmond Medical Center | OU Health Edmond Medical Center
OU Health Edmond Medical Center | OU Health Edmond Medical Center
""")
_d("Cook Children's", """
Medical Center - Fort Worth | Cook Children's Medical Center
Cook Children's Medical Center | Cook Children's Medical Center
Medical Center - Prosper | Cook Children's Medical Center - Prosper
Cook Children's Medical Center - Prosper | Cook Children's Medical Center - Prosper
""")
_d("ProMedica", """
Toledo Hospital | ProMedica Toledo Hospital
ProMedica Toledo Hospital | ProMedica Toledo Hospital
Ebeid Children's Hospital | ProMedica Russell J. Ebeid Children's Hospital
Russell J. Ebeid Children's Hospital | ProMedica Russell J. Ebeid Children's Hospital
Flower Hospital | ProMedica Flower Hospital
Monroe Regional Hospital | ProMedica Monroe Regional Hospital
Hickman Hospital | ProMedica Charles and Virginia Hickman Hospital
Memorial Hospital | ProMedica Memorial Hospital
Defiance Regional Hospital | ProMedica Defiance Regional Hospital
Bay Park Hospital | ProMedica Bay Park Hospital
Fostoria Community Hospital | ProMedica Fostoria Community Hospital
""")
_d("UofL Health", """
UofL Hospital | UofL Health - UofL Hospital
Jewish Hospital | UofL Health - Jewish Hospital
Mary & Elizabeth Hospital | UofL Health - Mary & Elizabeth Hospital
Mary and Elizabeth Hospital | UofL Health - Mary & Elizabeth Hospital
Peace Hospital | UofL Health - Peace Hospital
Frazier Rehabilitation Institute | UofL Health - Frazier Rehabilitation Institute
Frazier Rehab Institute | UofL Health - Frazier Rehabilitation Institute
Brown Cancer Center | UofL Health - Brown Cancer Center
South Hospital | UofL Health - South Hospital
Shelbyville Hospital | UofL Health - Shelbyville Hospital
""")
_d("John Muir Health", """
Walnut Creek Medical Center | John Muir Medical Center - Walnut Creek
John Muir Medical Center - Walnut Creek | John Muir Medical Center - Walnut Creek
Concord Medical Center | John Muir Medical Center - Concord
John Muir Medical Center - Concord | John Muir Medical Center - Concord
Behavioral Health Center | John Muir Behavioral Health Center
John Muir Behavioral Health Center | John Muir Behavioral Health Center
""")
_d("Houston Healthcare", """
Houston Medical Center | Houston Medical Center
Perry Hospital | Perry Hospital
""")

# A work site that is no hospital: a clinic, an office building, a lab, a
# pharmacy, home health or hospice, a medical group or a street address
# ("1301 Taylor St Baptist", "32nd Avenue Building", "MCMC - Physician
# Offices 2 - MMG", "SMG Family Internal Medicine Williamsburg").
_OFFICE_RX = re.compile(
    r"\b(?:clinics?|building|bldg|offices?|MOB|pavilion|practices?|physicians?|medical\s+group|MMG|SMG|AMG|CMG|"
    r"urgent\s+care|express\s*care|home\s+health|homecare|hospice|lab|laboratory|pharmacy|warehouse|plaza|"
    r"suite|remote|corporate|administration|admin|care\s+center|nursing|senior|wellness|imaging|outpatient|outpt|"
    r"institute|support\s+services|data\s+center|call\s+center|business\s+center|distribution)\b", re.I)
_SEG_RX = re.compile(r"\s+-+\s+|\s*[/()|,;]\s*")
SKIP_ATTR = "_town_skip"     # facilities_town.apply_job leaves a row the work site calls no hospital


def _canon(system) -> str:
    s = (system or "").strip()
    spec = T.SPECS.get(s) or T.EMIT.get(s)
    return spec.system if spec is not None and spec.system in SYSTEMS else ""


def covers(system) -> bool:
    return bool(_canon(system))


def requisition_location(info) -> str:
    loc = (info or {}).get("jobRequisitionLocation") if isinstance(info, dict) else None
    if isinstance(loc, dict):
        return clean_text(loc.get("descriptor") or "")
    return clean_text(loc or "") if isinstance(loc, str) else ""


def detail_site(system, descriptor):
    """(verdict, entry) for one jobRequisitionLocation: ("site", (name, city,
    state)), ("office", None), ("unknown", None) or ("none", None). The whole
    value is read first, then each segment ("MCEL - Methodist Celina Medical
    Center - MCEL", "2540 East Street - Concord Medical Center (CMC)"); two
    different hospitals in one value name none."""
    s = _canon(system)
    d = clean_text(descriptor)
    if not s or not d:
        return "none", None
    tbl = DETAIL.get(s, {})
    k = name_key(d)
    if k in tbl:
        v = tbl[k]
        return ("site", v) if v else ("office", None)
    segs = [x.strip() for x in _SEG_RX.split(d) if x and x.strip()]
    hits = {}
    office = False
    for seg in segs:
        sk = name_key(seg)
        if sk in tbl:
            if tbl[sk] is None:
                office = True
            else:
                hits[tbl[sk][0]] = tbl[sk]
    if len(hits) == 1 and not office:
        return "site", next(iter(hits.values()))
    if hits:
        return "unknown", None          # two hospitals, or a hospital and an office: names none
    if office or _OFFICE_RX.search(d) or re.match(r"^\s*\d", d) or k == name_key(s):
        return "office", None
    return "unknown", None


def place(system, entry, city, state):
    """(name, city, state) for a row of `system` at (city, state) whose work
    site is `entry`, or None. The row keeps its own town when it is the
    hospital's; the hospital's town is written when the row has no town or
    sits at the stamped default; a row elsewhere is not moved (its own town
    and the work site disagree: nothing is guessed)."""
    spec = T.SPECS[_canon(system)]
    name, hcity, hst = entry
    jc, js = clean_text(city), (state or "").strip().upper()
    if jc and js == hst and town_key(jc) == town_key(hcity):
        return name, jc, js
    if not jc or spec.at_default(jc, js):
        return name, hcity, hst
    return None


def apply_detail(job, info) -> str:
    """The detail pass's hook (scraper._apply_wd_posting_info): a row still
    on the system name takes the hospital its jobRequisitionLocation names;
    a work site that is no hospital keeps the system name and tells the town
    pass to leave the row (SKIP_ATTR). Returns the verdict ("site", "office",
    "elsewhere", "unknown", "none", "named-already", or "" for another
    system). Never raises."""
    try:
        s = _canon(getattr(job, "hospital_system", ""))
        if not s:
            return ""
        spec = T.SPECS[s]
        if not spec.is_label(getattr(job, "hospital_name", "")):
            return "named-already"
        verdict, entry = detail_site(s, requisition_location(info))
        if verdict == "site":
            placed = place(s, entry, job.city, job.state)
            if placed is None:
                return "elsewhere"
            name, city, st = placed
            job.hospital_name = name
            if (city, st) != (clean_text(job.city), (job.state or "").strip().upper()):
                job.city, job.state, job.location = city, st, f"{city}, {st}"
            return "site"
        if verdict == "office":
            setattr(job, SKIP_ATTR, True)
        return verdict
    except Exception:
        return "none"


# ── Reading stored bodies again for the work site ───────────────────────────
# Every stored active row of these systems is read once per
# DETAIL_REREAD_CYCLE nights (about 2,500 a night across the 14 boards, on
# each board's own detail budget, after the rows with no body) until
# DETAIL_REREAD_UNTIL: rows in a shared town get their hospital, and a row the
# town rule put on a hospital comes back to the system name when its work
# site is a clinic. New rows are read on their first night anyway.
DETAIL_REREAD_CYCLE = int(os.getenv("FAC_DETAIL_REREAD_CYCLE", "5"))
DETAIL_REREAD_UNTIL = os.getenv("FAC_DETAIL_REREAD_UNTIL", "2026-12-31")


def reread_slot(canon_system, job_id, today=None) -> bool:
    """True when this stored row (scraper._known_kind's key) of one of these
    systems is due tonight for a detail read of its work site. Only rows the
    stored map holds (facilities_town.load_stored: the active rows) qualify,
    so a run without credentials re-reads nothing."""
    s = _canon(canon_system)
    if not s or not T._STORED_LOADED[0] or (s, str(job_id)) not in T.STORED:
        return False
    day = today or date.today()
    try:
        if day > datetime.strptime(DETAIL_REREAD_UNTIL, "%Y-%m-%d").date():
            return False
    except (TypeError, ValueError):
        return False
    n = max(1, DETAIL_REREAD_CYCLE)
    return zlib.crc32(f"wdsite|{s}|{job_id}".encode("utf-8")) % n == day.toordinal() % n


# ═══════════════════════════════════════════════════════════════════════════
# Trinity Health (trinityhealth.wd1): its named sites that are no hospital.
# Phase 2 named Trinity's hospitals off the Workday site (facilities_workday)
# and kept its senior communities, hospice and home-care agencies, PACE
# centres, Pittsburgh Mercy's programmes and Loyola's centres for health on
# the system name. The owner's rule 2 as restated 2026-10-29 ("named sites
# (hospitals, named clinics, centers, agencies' named branches) show their
# own name"): each site below shows its own name, in the town its Workday
# site names (several rows had stored the site's name as their town:
# "Saint Mary Home", "LIFE St. Joseph of the Pines", "Greentree Community
# Living Arrangements"). A site name with branches in several towns carries
# its branch ("Community Hospice - Saratoga", "Pittsburgh Mercy - Carnegie";
# Pittsburgh Mercy's programmes inside Pittsburgh are "Pittsburgh Mercy").
# Generic offices ("Medical Offices", "Professional
# Building", a street address), town-only sites and a site whose town the
# posting does not give (Frech Center, the county home-health offices, LIFE
# at St. Mary) keep the system name.
# ═══════════════════════════════════════════════════════════════════════════
import facilities_workday as _W  # noqa: E402


def _named_sites(system, rows: str) -> None:
    """facilities_workday table entries for a system's named sites, from
    "site | name | city | ST | source" lines. An entry phase 2 left on the
    system name (name None: "Pittsburgh-Mercy---South-9th-Street-Campus")
    takes the site's own name; one that already names another facility is
    an error."""
    tbl = _W.TABLES.setdefault(system, {})
    for line in rows.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        site, name, city, state, source = [p.strip() for p in line.split("|")]
        k = _W.key(site)
        old = tbl.get(k)
        if old is not None and old[0] not in (None, name):
            raise ValueError(f"{system}: {site!r} already names {old[0]!r}")
        tbl[k] = (name, city, state, source)


_named_sites("Trinity Health", """
Community-Hospice-Albany----Albany-New-York | Community Hospice - Albany | Albany | NY | site
Community-Hospice-ColumbiaGreene---Catskill-New-York | Community Hospice - Columbia-Greene | Catskill | NY | site
Community-Hospice-Saratoga---Saratoga-Springs-New-York | Community Hospice - Saratoga | Saratoga Springs | NY | site
Eddy-Visiting-Nurse-and-Rehab-Association---Troy-New-York | Eddy Visiting Nurse and Rehab Association | Troy | NY | site
SPNY---Eddy-VNRA | Eddy Visiting Nurse and Rehab Association | Troy | NY | site
Eddy-Memorial-Geriatric-Center--Terrace---Troy-New-York | Eddy Memorial Geriatric Center | Troy | NY | site
SPNY---Eddy-Memorial-Geriatric | Eddy Memorial Geriatric Center | Troy | NY | site
Eddy-Heritage-House-Nursing--Rehabilitation-Center---Troy-New-York | Eddy Heritage House Nursing and Rehabilitation Center | Troy | NY | site
SPNY---Heritage-House | Eddy Heritage House Nursing and Rehabilitation Center | Troy | NY | site
Eddy-Village-Green-at-Cohoes--Marjorie-Doyle-Rockwell-Center---Cohoes-New-York | Eddy Village Green at Cohoes | Cohoes | NY | site
Terrace-at-Beverwyck--Eddy-Village-Green-at-Beverwyck---Slingerlands-New-York | Eddy Village Green at Beverwyck | Slingerlands | NY | site
SPNY---Beverwyck | Eddy Village Green at Beverwyck | Slingerlands | NY | site
Eddy-Hawthorne-Ridge--Terrace-at-Eddy-Hawthorne-Ridge---East-Greenbush-New-York | Eddy Hawthorne Ridge | East Greenbush | NY | site
Glen-Eddy--Terrace-at-Glen-Eddy---Niskayuna-New-York | Glen Eddy | Niskayuna | NY | site
Our-Lady-of-Mercy-Life-Center---Guilderland-New-York | Our Lady of Mercy Life Center | Guilderland | NY | site
SPNY---Our-Lady-of-Mercy-Life | Our Lady of Mercy Life Center | Guilderland | NY | site
The-Glen-at-Hiland-Meadows--Terrace-at-The-Glen---Queensbury-New-York | Glen at Hiland Meadows | Queensbury | NY | site
Schuyler-Ridge-Residential-Healthcare---Clifton-Park-New-York | Schuyler Ridge Residential Healthcare | Clifton Park | NY | site
St-Joseph-Home-Care--Hospice---Livonia-Mi | St. Joseph Home Care & Hospice | Livonia | MI | site
Mercy-Health-VNS---Muskegon-MI | Mercy Health VNS | Muskegon | MI | site
Glacier-Hills---Ann-Arbor-Mi | Glacier Hills | Ann Arbor | MI | site
Bellbrook---Rochester-Hills-Mi | Bellbrook | Rochester Hills | MI | site
The-Neighborhoods-of-White-Lake---White-Lake-Mi | Neighborhoods of White Lake | White Lake | MI | site
Woodland-Village---Brighton-Mi | Woodland Village | Brighton | MI | site
St-Josephs-Village---Ypsilanti-Mi | St. Joseph's Village | Ypsilanti | MI | site
Maryhaven---Southgate-Mi | Maryhaven | Southgate | MI | site
Fraser-Villa---Fraser-Mi | Fraser Villa | Fraser | MI | site
Marycrest---Livonia-Mi | Marycrest | Livonia | MI | site
SJAGA---Mercy-Care-Chamblee | Mercy Care Chamblee | Chamblee | GA | site
LIFE-St-Joseph-of-the-Pines | LIFE St. Joseph of the Pines | Southern Pines | NC | site
TSLC---Saint-Mary-Home | Saint Mary Home | West Hartford | CT | site
THSC---Marys-Meadow | Mary's Meadow at Providence Place | Holyoke | MA | site
Mercy-Home-Health---North-East-Philadelphia | Mercy Home Health | Philadelphia | PA | site
Mercy-Home-Health---South-Philadelphia | Mercy Home Health | Philadelphia | PA | site
Mercy-LIFE---Philadelphia | Mercy LIFE | Philadelphia | PA | site
Mercy-LIFE---South-Philadelphia | Mercy LIFE | Philadelphia | PA | site
Pittsburgh-Mercy---South-9th-Street-Campus | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Allegheny-Center | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Bethlehem-Haven | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Brighton-Heights-Community-Living-Arrangements | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Brookline-Community-Living-Arrangements | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Brookline-Vocational | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Carlow-University | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Glen-Hazel-Extended-Acute-Care | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Mount-Washington-Community-Living-Arrangements | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---North-Side-Community-Living-Arrangements | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Squirrel-Hill-Community-Living-Arrangements | Pittsburgh Mercy | Pittsburgh | PA | site
Pittsburgh-Mercy---Bethel-Park-Supportive-Housing | Pittsburgh Mercy - Bethel Park | Bethel Park | PA | site
Pittsburgh-Mercy---Brentwood-Community-Living-Arrangements | Pittsburgh Mercy - Brentwood | Brentwood | PA | site
Pittsburgh-Mercy---Carnegie-Community-Living-Arrangements | Pittsburgh Mercy - Carnegie | Carnegie | PA | site
Pittsburgh-Mercy---Carnegie-Supportive-Housing | Pittsburgh Mercy - Carnegie | Carnegie | PA | site
Pittsburgh-Mercy---Castle-Shannon-Community-Living-Arrangements | Pittsburgh Mercy - Castle Shannon | Castle Shannon | PA | site
Pittsburgh-Mercy---Greentree-Community-Living-Arrangements | Pittsburgh Mercy - Green Tree | Green Tree | PA | site
Pittsburgh-Mercy---Munhall-Personal-Care-Home | Pittsburgh Mercy - Munhall | Munhall | PA | site
Pittsburgh-Mercy---West-Mifflin-Personal-Care-Home | Pittsburgh Mercy - West Mifflin | West Mifflin | PA | site
Pittsburgh-Mercy---West-View-Community-Living-Arrangements | Pittsburgh Mercy - West View | West View | PA | site
Pittsburgh-Mercy---Wilkensburg-Personal-Care-Home | Pittsburgh Mercy - Wilkinsburg | Wilkinsburg | PA | site
Loyola-Center-for-Health-at-Bur-Ridge | Loyola Center for Health at Burr Ridge | Burr Ridge | IL | site
LUHS-Burr-Ridge-Medical-Center | Loyola Center for Health at Burr Ridge | Burr Ridge | IL | site
Loyola-Center-for-Health-at-Gottlieb | Loyola Center for Health at Gottlieb | Melrose Park | IL | site
Loyola-Center-for-Health-at-Hickory-Hills | Loyola Center for Health at Hickory Hills | Hickory Hills | IL | site
Loyola-Center-for-Health-at-La-Grange-Park | Loyola Center for Health at La Grange Park | La Grange Park | IL | site
Loyola-Center-for-Health-at-Oak-Park-North | Loyola Center for Health at Oak Park | Oak Park | IL | site
Loyola-Center-for-Health-at-Oakbrook-Terrace-North | Loyola Center for Health at Oakbrook Terrace | Oakbrook Terrace | IL | site
Loyola-Center-for-Health-at-Oakbrook-Terrace-South | Loyola Center for Health at Oakbrook Terrace | Oakbrook Terrace | IL | site
LUHS-Oak-Brook-Terrace-Medical-Center-North | Loyola Center for Health at Oakbrook Terrace | Oakbrook Terrace | IL | site
Loyola-Center-for-Health-at-River-Forest | Loyola Center for Health at River Forest | River Forest | IL | site
""")
TRINITY_SITES = frozenset(v[0] for k, v in _W.TABLES["Trinity Health"].items() if v[3] == "site" and v[0])
