"""Second look at the systems phase 3 left mostly on the system name (2026-10-29, phase 4).

The owner, 2026-10-29: "trinity health not done either" (after phase 3 named
359 of Trinity Health's 2,052 label rows), "unitedhealth group hospital list
is incomplete", "i thought you were gonna edit all the large multihospital
systems". His decisions: (1) cards and job pages show the HOSPITAL or site
("Apply to <site>"); the system name and logo stay on the job page and the
system hub (hospital_system is never touched here); (2) named sites
(hospitals, named clinics, centers, agencies' named branches, named
communities) show their own name; generic offices, home-based and remote
postings show the system name with the town; NEVER guess a town or a site
(a branch directory that maps a town to a branch is no evidence unless the
posting itself or the employer's own job page names that branch).

What this module adds, each piece from the posting's own fields:

TRINITY HEALTH (trinityhealth.wd1, Jobs). Its Workday site (locationsText and
the link's path segment) is the only location field, and the detail work site
is the same descriptor. Phase 2 named the hospitals whose site names them
(facilities_workday), phase 3 the named non-hospital sites
(facilities_leftovers). What was left:
  1. Bare-town sites ("Silver-Spring-Maryland", "Davenport-IA",
     "Muskegon-Michigan", "Langhorne"): 724 rows. Trinity's TownSpec is
     registered into facilities_town here, so the TOWN family's rules decide
     exactly as for the 104 systems phases 2 and 3 built: a posting in a town
     with one Trinity hospital is that hospital's unless it is a clinic's, an
     office's, a medical group's, a home-care, hospice, PACE or senior-living
     posting, remote, or for several sites; a town with two (Albany, Troy,
     Hartford) places only a posting that names one. The stored decision
     stands on nights without a body (facilities_town.STORED).
  2. The posting's own text naming its hospital: "Holy Cross Hospital, a
     member of Trinity Health, is seeking ...", "Location: Trinity Health
     Livingston Hospital (Brighton, MI)", "St Mary's Hospital in Athens".
     Read in the sentences that are the posting's own (not a sentence that
     names two of the system's hospitals, not the system's inventory, not the
     boilerplate), only a hospital of the posting's own state, and only in
     the posting's own town (or, on a region site that names no town, in the
     hospital's town). BODY_NAMES.
  3. Region and campus sites. "St-Marys-Health-Care-System-Georgia" (the
     Athens ministry; the town on those rows is a stamped Athens) and
     "Capital-Region---NYS---Albany-New-York" name no work town: a row there
     is placed only when its text names the hospital (2). The Livingston
     hospital's new Brighton campus ("Opened in 2026, Trinity Health
     Livingston Hospital is a modern acute care facility in Brighton") and
     the Plymouth Medical Center campus site are read like a town of that
     one hospital (1), so their clinics and offices keep the system name.
  4. Sites the Workday table did not list (SITES): office and clinic sites
     whose descriptor names their town keep the system name and get that
     town and state (the blank-state rows: "SPNY---855-Rt-146-Clifton-Park"
     is Clifton Park, NY; St. Peter's Health Partners is New York only), and
     named non-hospital sites (PACE centres, LIFE programmes, home care
     agencies, senior communities) show their own name in the town their
     descriptor or their postings give. A named site whose town no posting
     gives keeps the system name.
  Every other Trinity site (a clinic, an office, a street address, a medical
  group) keeps the system name with its town (SKIP_ATTR stops the town rule).

SANFORD HEALTH. Good Samaritan Society communities are named sites (the
owner, 10-29). Each posting's "Department Details" names its community ("At
Villisca Good Samaritan, our vision ...", "Good Samaritan - Bethany",
"Good Samaritan Society - Mandan Sunset"): the row shows "Good Samaritan
Society - <community>" when that community is in the row's own town (its
name is the town, starts with it, or is listed in GSS_COMMUNITIES for that
town). Multi-location postings, local travelers and home-health agency
postings keep the system name.

HOME HEALTH. Gentiva's "Location: Heartland Hospice Branch, 3975 Fair Ridge
Dr, ..., Fairfax, VA" and Elara Caring's "Branch: Kansas City, Missouri" name
their branch: "<brand> - <town>" when that town is the row's own. Enhabit,
Maxim and Aveanna name no branch in any stored field (phase 3; checked again
10-29): they keep the system name with their town.

What is never done: hospital_system, job_id, url and slug are never touched;
a town is only written from a table entry's own street address, the
posting's own descriptor or the posting's own text; a row that already names
a facility is left as it is.

Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase4/trinity_stays_work/changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import json
import os
import re

import facilities_town as T
import facilities_workday as W
import facilities_leftovers  # noqa: F401  (phase 3's Trinity named sites load first)
from city_utils import known_town
from facilities_town import _h, clean_text, name_key, town_key

TRINITY = "Trinity Health"
SANFORD = "Sanford Health"
GENTIVA = "Gentiva"
ELARA = "Elara Caring"
SKIP_ATTR = "_town_skip"     # facilities_town.apply_job leaves the row on the system name

# ═══════════════════════════════════════════════════════════════════════════
# 1. Trinity's TownSpec: every hospital Trinity owns, with the town of its own
# street address (the CMS list in the database, public.hospitals, "cms <id>";
# names as the stored rows and the registry spell them). Affiliates the AHRQ
# list files under Trinity (MercyOne's managed county hospitals, Rome, Mackinac
# Straits) are not Trinity's employers and are left out.
# ═══════════════════════════════════════════════════════════════════════════
_HOSPITALS = [
    # Trinity Health Of New York
    _h("St. Peter's Hospital", "Albany", "NY", "cms 330057"),
    _h("Albany Memorial Hospital", "Albany", "NY", "site (600 Northern Blvd, Albany)"),
    _h("Samaritan Hospital", "Troy", "NY", "cms 330180"),
    _h("Samaritan Hospital - St. Mary's Campus", "Troy", "NY", "site (1300 Massachusetts Ave, Troy)"),
    _h("Sunnyview Rehabilitation Hospital", "Schenectady", "NY", "cms 330406"),
    _h("St. Joseph's Health Hospital", "Syracuse", "NY", "cms 330140"),
    # Trinity Health Of New England
    _h("Saint Francis Hospital", "Hartford", "CT", "cms 070002"),
    _h("Mount Sinai Rehabilitation Hospital", "Hartford", "CT", "site (490 Blue Hills Ave, Hartford)"),
    _h("Saint Mary's Hospital", "Waterbury", "CT", "cms 070016"),
    _h("Johnson Memorial Hospital", "Stafford Springs", "CT", "cms 070008"),
    _h("Mercy Medical Center", "Springfield", "MA", "cms 220066 (271 Carew St, Springfield)"),
    _h("MiraVista Behavioral Health Center", "Holyoke", "MA", "cms 224046"),
    # Trinity Health Mid-Atlantic
    _h("Saint Francis Healthcare", "Wilmington", "DE", "cms 080003 (7th and Clayton Sts, Wilmington)"),
    _h("Mercy Fitzgerald Hospital", "Darby", "PA", "cms 390156"),
    _h("Nazareth Hospital", "Philadelphia", "PA", "cms 390204"),
    _h("St. Mary Medical Center", "Langhorne", "PA", "cms 390258"),
    # Holy Cross Health (Maryland, Florida)
    _h("Holy Cross Hospital", "Silver Spring", "MD", "cms 210004 (1500 Forest Glen Rd, Silver Spring)"),
    _h("Holy Cross Germantown Hospital", "Germantown", "MD", "cms 210065"),
    _h("Holy Cross Hospital", "Ft. Lauderdale", "FL", "cms 100073"),
    # St. Mary's Health Care System (Georgia)
    _h("St. Mary's Hospital", "Athens", "GA", "cms 110006 (1230 Baxter St, Athens)"),
    _h("St. Mary's Sacred Heart Hospital", "Lavonia", "GA", "site (367 Clear Creek Pkwy, Lavonia)"),
    _h("St. Mary's Good Samaritan Hospital", "Greensboro", "GA", "cms 111329"),
    # MercyOne (Iowa, Illinois)
    _h("MercyOne Des Moines Medical Center", "Des Moines", "IA", "cms 160083"),
    _h("MercyOne West Des Moines Medical Center", "West Des Moines", "IA", "site (1755 59th Pl, West Des Moines)"),
    _h("MercyOne Newton Medical Center", "Newton", "IA", "cms 160032"),
    _h("MercyOne Centerville Medical Center", "Centerville", "IA", "cms 161377"),
    _h("MercyOne Elkader Medical Center", "Elkader", "IA", "cms 161319"),
    _h("Dallas County Hospital", "Perry", "IA", "cms 161322"),
    _h("MercyOne North Iowa Medical Center", "Mason City", "IA", "cms 160064"),
    _h("MercyOne New Hampton Medical Center", "New Hampton", "IA", "site (308 N Maple Ave, New Hampton)"),
    _h("MercyOne Dubuque Medical Center", "Dubuque", "IA", "cms 160069"),
    _h("MercyOne Dyersville Medical Center", "Dyersville", "IA", "site (1111 3rd St SW, Dyersville)"),
    _h("MercyOne Clinton Medical Center", "Clinton", "IA", "cms 160080"),
    _h("MercyOne Waterloo Medical Center", "Waterloo", "IA", "cms 160067"),
    _h("MercyOne Oelwein Medical Center", "Oelwein", "IA", "cms 161338"),
    _h("MercyOne Cedar Falls Medical Center", "Cedar Falls", "IA", "site (515 College St, Cedar Falls)"),
    _h("Genesis Medical Center Davenport", "Davenport", "IA", "cms 160033 (both Davenport campuses)"),
    _h("Genesis Medical Center DeWitt", "DeWitt", "IA", "cms 161313"),
    _h("Genesis Medical Center Silvis", "Silvis", "IL", "site (801 Illini Dr, Silvis)"),
    _h("Genesis Medical Center Aledo", "Aledo", "IL", "cms 141304"),
    # Loyola Medicine
    _h("Loyola University Medical Center", "Maywood", "IL", "cms 140276"),
    _h("MacNeal Hospital", "Berwyn", "IL", "cms 140054"),
    _h("Gottlieb Memorial Hospital", "Melrose Park", "IL", "cms 140008"),
    # Saint Joseph Health System (Indiana)
    _h("Saint Joseph Health System Mishawaka Medical Center", "Mishawaka", "IN", "cms 150012"),
    _h("Saint Joseph Regional Medical Center - Plymouth", "Plymouth", "IN", "cms 150076 (1915 Lake Ave, Plymouth)"),
    # Trinity Health Michigan
    _h("Trinity Health Ann Arbor Hospital", "Ann Arbor", "MI", "cms 230156"),
    _h("Trinity Health Oakland Hospital", "Pontiac", "MI", "cms 230029"),
    _h("Trinity Health Chelsea Hospital", "Chelsea", "MI", "cms 230259"),
    _h("Trinity Health Livonia Hospital", "Livonia", "MI", "cms 230002"),
    # cms 230069 is 620 Byron Rd, Howell; its postings: "Opened in 2026, Trinity
    # Health Livingston Hospital is a modern acute care facility in Brighton"
    _h("Trinity Health Livingston Hospital", "Howell", "MI", "cms 230069 (Brighton campus: its postings)", "Brighton"),
    _h("Trinity Health Grand Rapids Hospital", "Grand Rapids", "MI", "cms 230059"),
    _h("Trinity Health Muskegon Hospital", "Muskegon", "MI", "cms 230066"),
    _h("Trinity Health Shelby Hospital", "Shelby", "MI", "site (72 S State St, Shelby)"),
    _h("Trinity Health Grand Haven Hospital", "Grand Haven", "MI", "cms 230174"),
    # Mount Carmel (Ohio)
    _h("Mount Carmel East", "Columbus", "OH", "cms 360035"),
    _h("Mount Carmel St. Ann's", "Westerville", "OH", "cms 360012"),
    _h("Mount Carmel Grove City", "Grove City", "OH", "site (5300 N Meadows Dr, Grove City)"),
    _h("Mount Carmel New Albany Surgical Hospital", "New Albany", "OH", "cms 360266"),
    _h("Diley Ridge Medical Center", "Canal Winchester", "OH", "cms 360358"),
    _h("Mount Carmel Dublin", "Dublin", "OH", "cms 360375 (4105 Emerald Pkwy, Dublin)"),
    # Saint Alphonsus (Idaho, Oregon), Saint Agnes (California)
    _h("Saint Alphonsus Regional Medical Center", "Boise", "ID", "cms 130007"),
    _h("Saint Alphonsus Medical Center - Nampa", "Nampa", "ID", "cms 130013"),
    _h("Saint Alphonsus Medical Center - Ontario", "Ontario", "OR", "cms 380052"),
    _h("Saint Alphonsus Medical Center - Baker City", "Baker City", "OR", "cms 381315"),
    _h("Saint Agnes Medical Center", "Fresno", "CA", "cms 050093"),
]

SPEC = T._spec(TRINITY, _HOSPITALS, keep={
    # the system office and the IHA medical group's home: the posting is an
    # office's or a practice's far more often than the hospital's
    "Livonia, MI": "Trinity Health's system office (20555 Victor Pkwy) shares the town with the hospital",
})

# Trinity's own non-hospital brands by title or setting: Probility (physical
# therapy clinics), PACE / LIFE programmes, IHA and the medical groups,
# Michigan Heart (cardiology practice), the pain institute, Mercy Care (FQHC).
T.CARE_SITE_BY_SYSTEM[TRINITY] = re.compile(
    r"\b(?:Probility|PACE|LIFE\s+(?:at|St\.?|Center)|Mercy\s+LIFE|IHA|Michigan\s+Heart|Pain\s+(?:Institute|Clinic|Center)|"
    r"Mercy\s+Care|Medical\s+Associates|Physician\s+Partners|THOfNE\s*MG|THOfNEMG|Medical\s+Group|"
    r"Family\s+Health\s+Center|Health\s+Center\s+for\s+Children|Vein\s+Center|Surgical\s+Office|"
    r"Orthopedics\s+Office|Outpatient\s+Office|Infusion\s+Center|Donor|Philanthrop\w*|Foundation)\b")

# Trinity's boilerplate sentences (the "Our Commitment" paragraph, each
# ministry's "About us"): fingerprints read off the stored bodies of its label
# rows on 2026-10-29 (tools/town_boilerplate.py's rule, at least 6 postings),
# beside this module. They join facilities_town's boilerplate for Trinity.
_BOILER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "facilities_trinity_stays_boilerplate.json")
try:
    with open(_BOILER_FILE, encoding="utf-8") as _f:
        _bp = json.load(_f)
    for _sys, _keys in _bp.items():
        T.BOILERPLATE[_sys] = frozenset(T.BOILERPLATE.get(_sys, frozenset()) | set(_keys))
except (OSError, ValueError):
    pass

# ═══════════════════════════════════════════════════════════════════════════
# 2. The posting's own text naming its hospital. Spellings a posting uses,
# each only for its hospital's state. A spelling inside a longer one counts
# once ("Holy Cross Hospital" inside "Holy Cross Germantown Hospital" is not
# read: the spellings are matched longest first and the text consumed).
# ═══════════════════════════════════════════════════════════════════════════
BODY_NAMES: dict[str, tuple[str, str, tuple[str, ...]]] = {}


def _b(name, city, state, *spellings):
    BODY_NAMES[name + "|" + state] = (name, state, tuple(spellings) + (name,))


_b("Holy Cross Hospital", "Silver Spring", "MD", "Holy Cross Hospital")
_b("Holy Cross Germantown Hospital", "Germantown", "MD", "Holy Cross Germantown Hospital", "Holy Cross Germantown")
_b("Holy Cross Hospital", "Ft. Lauderdale", "FL", "Holy Cross Hospital")
_b("St. Mary's Hospital", "Athens", "GA", "St. Mary's Hospital", "St Mary's Hospital", "St Marys Hospital",
   "Saint Mary's Hospital")
_b("St. Mary's Sacred Heart Hospital", "Lavonia", "GA", "St. Mary's Sacred Heart", "St Mary's Sacred Heart")
_b("St. Mary's Good Samaritan Hospital", "Greensboro", "GA", "St. Mary's Good Samaritan", "St Mary's Good Samaritan")
_b("Trinity Health Livingston Hospital", "Howell", "MI", "Trinity Health Livingston")
_b("Trinity Health Ann Arbor Hospital", "Ann Arbor", "MI", "Trinity Health Ann Arbor", "St. Joseph Mercy Ann Arbor",
   "Saint Joseph Mercy Ann Arbor")
_b("Trinity Health Oakland Hospital", "Pontiac", "MI", "Trinity Health Oakland", "Trinity Oakland Hospital",
   "St. Joseph Mercy Oakland", "Saint Joseph Mercy Oakland")
_b("Trinity Health Chelsea Hospital", "Chelsea", "MI", "Trinity Health Chelsea", "Chelsea Hospital")
_b("Trinity Health Livonia Hospital", "Livonia", "MI", "Trinity Health Livonia", "St. Mary Mercy Livonia",
   "St. Mary Mercy Hospital")
_b("Trinity Health Grand Rapids Hospital", "Grand Rapids", "MI", "Trinity Health Grand Rapids",
   "Mercy Health Saint Mary's")
_b("Trinity Health Muskegon Hospital", "Muskegon", "MI", "Trinity Health Muskegon")
_b("Trinity Health Grand Haven Hospital", "Grand Haven", "MI", "Trinity Health Grand Haven",
   "North Ottawa Community Hospital")
_b("Trinity Health Shelby Hospital", "Shelby", "MI", "Trinity Health Shelby", "Mercy Health Lakeshore Campus")
_b("Saint Joseph Health System Mishawaka Medical Center", "Mishawaka", "IN", "Mishawaka Medical Center")
_b("Saint Joseph Regional Medical Center - Plymouth", "Plymouth", "IN", "Plymouth Medical Center")
_b("Loyola University Medical Center", "Maywood", "IL", "Loyola University Medical Center")
_b("MacNeal Hospital", "Berwyn", "IL", "MacNeal Hospital")
_b("Gottlieb Memorial Hospital", "Melrose Park", "IL", "Gottlieb Memorial Hospital", "Gottlieb Hospital")
_b("Genesis Medical Center Silvis", "Silvis", "IL", "MercyOne Genesis Silvis Medical Center",
   "MercyOne Silvis Medical Center", "Genesis Medical Center Silvis", "Genesis Medical Center - Silvis")
_b("Genesis Medical Center Aledo", "Aledo", "IL", "MercyOne Genesis Aledo Medical Center",
   "Genesis Medical Center Aledo", "Genesis Medical Center - Aledo")
_b("Mount Carmel East", "Columbus", "OH", "Mount Carmel East")
_b("Mount Carmel St. Ann's", "Westerville", "OH", "Mount Carmel St. Ann's", "Mount Carmel St Ann's")
_b("Mount Carmel Grove City", "Grove City", "OH", "Mount Carmel Grove City")
_b("Mount Carmel New Albany Surgical Hospital", "New Albany", "OH", "Mount Carmel New Albany")
_b("Diley Ridge Medical Center", "Canal Winchester", "OH", "Diley Ridge Medical Center", "Mount Carmel Diley Ridge")
_b("Mount Carmel Dublin", "Dublin", "OH", "Mount Carmel Dublin")
_b("St. Peter's Hospital", "Albany", "NY", "St. Peter's Hospital", "St Peter's Hospital", "St. Peters Hospital",
   "St Peters Hospital", "Saint Peter's Hospital")
_b("Albany Memorial Hospital", "Albany", "NY", "Albany Memorial Hospital", "Albany Memorial Campus")
_b("Samaritan Hospital", "Troy", "NY", "Samaritan Hospital")
_b("Samaritan Hospital - St. Mary's Campus", "Troy", "NY", "Samaritan Hospital - St. Mary's Campus",
   "Samaritan Hospital St. Mary's Campus", "St. Mary's Campus")
_b("Sunnyview Rehabilitation Hospital", "Schenectady", "NY", "Sunnyview Rehabilitation Hospital", "Sunnyview Hospital")
_b("St. Joseph's Health Hospital", "Syracuse", "NY", "St. Joseph's Health Hospital", "St. Joseph's Hospital Health Center")
_b("Saint Francis Hospital", "Hartford", "CT", "Saint Francis Hospital", "St. Francis Hospital",
   "Saint Francis Hospital and Medical Center")
_b("Mount Sinai Rehabilitation Hospital", "Hartford", "CT", "Mount Sinai Rehabilitation Hospital")
_b("Saint Mary's Hospital", "Waterbury", "CT", "Saint Mary's Hospital", "St. Mary's Hospital", "St Mary's Hospital")
_b("Johnson Memorial Hospital", "Stafford Springs", "CT", "Johnson Memorial Hospital")
_b("Mercy Medical Center", "Springfield", "MA", "Mercy Medical Center")
_b("Saint Francis Healthcare", "Wilmington", "DE", "Saint Francis Hospital", "St. Francis Hospital")
_b("Nazareth Hospital", "Philadelphia", "PA", "Nazareth Hospital")
_b("Mercy Fitzgerald Hospital", "Darby", "PA", "Mercy Fitzgerald Hospital", "Mercy Fitzgerald Campus")
_b("St. Mary Medical Center", "Langhorne", "PA", "St. Mary Medical Center", "St Mary Medical Center")
_b("Saint Alphonsus Regional Medical Center", "Boise", "ID", "Saint Alphonsus Regional Medical Center",
   "Boise Regional Medical Center")
_b("Saint Alphonsus Medical Center - Nampa", "Nampa", "ID", "Saint Alphonsus Medical Center - Nampa",
   "Saint Alphonsus Medical Center Nampa", "Nampa Medical Center")
_b("Saint Alphonsus Medical Center - Ontario", "Ontario", "OR", "Saint Alphonsus Medical Center - Ontario",
   "Saint Alphonsus Medical Center Ontario")
_b("Saint Alphonsus Medical Center - Baker City", "Baker City", "OR", "Saint Alphonsus Medical Center - Baker City",
   "Saint Alphonsus Medical Center Baker City")
_b("Saint Agnes Medical Center", "Fresno", "CA", "Saint Agnes Medical Center")
for _n, _st in (("MercyOne Des Moines Medical Center", "IA"), ("MercyOne West Des Moines Medical Center", "IA"),
                ("MercyOne Newton Medical Center", "IA"), ("MercyOne Centerville Medical Center", "IA"),
                ("MercyOne Elkader Medical Center", "IA"), ("MercyOne North Iowa Medical Center", "IA"),
                ("MercyOne New Hampton Medical Center", "IA"), ("MercyOne Dubuque Medical Center", "IA"),
                ("MercyOne Dyersville Medical Center", "IA"), ("MercyOne Clinton Medical Center", "IA"),
                ("MercyOne Waterloo Medical Center", "IA"), ("MercyOne Oelwein Medical Center", "IA"),
                ("MercyOne Cedar Falls Medical Center", "IA")):
    _b(_n, "", _st)
_b("Genesis Medical Center Davenport", "Davenport", "IA", "MercyOne Genesis Davenport Medical Center",
   "MercyOne Davenport Medical Center", "Genesis Medical Center Davenport", "Genesis Medical Center - Davenport")
_b("Genesis Medical Center DeWitt", "DeWitt", "IA", "MercyOne Genesis DeWitt Medical Center",
   "MercyOne DeWitt Medical Center", "Genesis Medical Center DeWitt")

_HOSP_BY_NAME_ST = {(h[0], h[2]): h for h in SPEC.hospitals}
for _k, (_n, _st, _sp) in BODY_NAMES.items():
    if (_n, _st) not in _HOSP_BY_NAME_ST:
        raise ValueError(f"{TRINITY}: body name for unknown hospital {_n!r} ({_st})")


def _spelling_rx(s):
    words = re.findall(r"[a-z0-9]+", name_key(s))
    return r"(?<![a-z0-9])" + r"\s+".join(map(re.escape, words)) + r"(?![a-z0-9])"


# per state: one alternation, longest spelling first, each group -> hospital
_BODY_RX: dict[str, tuple[re.Pattern, list]] = {}
for _st in sorted({v[1] for v in BODY_NAMES.values()}):
    pairs = sorted(((sp, v[0]) for v in BODY_NAMES.values() if v[1] == _st for sp in v[2]),
                   key=lambda p: -len(name_key(p[0])))
    _BODY_RX[_st] = (re.compile("|".join("(" + _spelling_rx(sp) + ")" for sp, _ in pairs)), [n for _, n in pairs])

# "Trinity Health Grand Rapids Foundation", "Trinity Health Muskegon Medical
# Group", "Holy Cross Hospital Home Care": the ministry or one of its
# non-hospital parts, not the hospital
_NOT_HOSPITAL_AFTER = re.compile(
    r"\s*(?:foundation|medical group|physician|physicians|partners|iha|hospice|home|homecare|pace|life|senior|"
    r"primary|medical plaza|health center|health park|urgent|clinic|clinics|family|pharmacy|imaging|"
    r"rehabilitation services|outpatient|medical office|professional building|vns|vna)\b")
_SENT_RX = re.compile(r"(?<=[.!?])\s+|\s*\n\s*|\s+[•*]\s+")


def own_sentences(description, spec=SPEC) -> list:
    """The posting's own sentences: the whole body cleaned, without the
    boilerplate, without the system's inventory (INVENTORY_RX)."""
    boiler = T.BOILERPLATE.get(spec.system, frozenset())
    out = []
    for x in _SENT_RX.split(str(description or "")):
        x = clean_text(x)
        if not x:
            continue
        if T.INVENTORY_RX.search(x):
            continue
        if boiler and T.sentence_key(x) in boiler:
            continue
        out.append(x)
    return out


def named_in_text(title, description, state) -> list:
    """The Trinity hospitals of `state` the posting's own sentences (and its
    title) name; a sentence naming two different ones names none."""
    st = (state or "").strip().upper()
    hit = _BODY_RX.get(st)
    if hit is None:
        return []
    rx, names = hit
    found = []
    for s in [clean_text(title)] + own_sentences(description):
        k = " " + name_key(s) + " "
        here = set()
        for m in rx.finditer(k):
            if _NOT_HOSPITAL_AFTER.match(k[m.end():]):
                continue
            here.add(names[m.lastindex - 1])
        if len(here) == 1:
            found.append(next(iter(here)))
    return sorted(set(found))


# ═══════════════════════════════════════════════════════════════════════════
# 3. Trinity's site kinds.
# ═══════════════════════════════════════════════════════════════════════════
# region sites: a ministry or region descriptor that names no work town (the
# row's town is stamped); state of the ministry
REGION = {
    W.key("St-Marys-Health-Care-System-Georgia"): "GA",
    W.key("Capital-Region---NYS---Albany-New-York"): "NY",
    W.key("Saint-Alphonsus-Health-System---Meridian-Idaho"): "ID",
}
# campus sites: the site names one hospital's campus but also hosts its clinics
# and offices; a posting there is read like a posting in that hospital's town
CAMPUS = {
    W.key("Saint-Joseph-Mercy-Health-System-Hospital-Campus---Brighton-Mi"):
        ("Trinity Health Livingston Hospital", "Brighton", "MI"),
    W.key("Medical-Offices-Plymouth-Medical-Center"): ("Saint Joseph Regional Medical Center - Plymouth", "Plymouth", "IN"),
    W.key("MercyOne---Mason-City-Iowa"): ("MercyOne North Iowa Medical Center", "Mason City", "IA"),
}
_ST_NAMES = {k: v for k, v in W._STATES.items()}
_ST_CODES = set(_ST_NAMES.values())


def bare_town(seg, city="", state=""):
    """(town, ST) when a Workday site segment is only a town: "Town-StateName",
    "Town-ST", or the row's own town with nothing else ("Langhorne")."""
    s = (seg or "").strip("-")
    if not s:
        return None
    t = s.replace("-", " ").strip()
    ts = W.town_state_name(t)
    if ts:
        words = t.split()
        # the whole segment must be the town and the state, nothing else
        for n in (2, 1):
            if len(words) > n and _ST_NAMES.get(" ".join(words[-n:]).lower()) == ts[1] \
                    and town_key(" ".join(words[:-n])) == town_key(ts[0]):
                return ts
    m = re.match(r"^(.*?)[ -]([A-Z]{2})$", s)
    if m and m.group(2) in _ST_CODES:
        town = known_town(m.group(1).replace("-", " "), m.group(2))
        if town and town_key(town) == town_key(m.group(1)):
            return town, m.group(2)
    st = (state or "").strip().upper()
    if st and town_key(t) and town_key(t) == town_key(city):
        town = known_town(t, st)
        if town:
            return town, st
    return None


def site_kind(url, city="", state=""):
    """("town", (town, ST)), ("region", ST), ("campus", (name, town, ST)) or
    ("site", None) for a Trinity row's Workday site (its link's segment)."""
    seg = W.path_segment(url or "")
    k = W.key(seg)
    if k in CAMPUS:
        return "campus", CAMPUS[k]
    if k in REGION:
        return "region", REGION[k]
    bt = bare_town(seg, city, state)
    if bt:
        return "town", bt
    return "site", None


# ═══════════════════════════════════════════════════════════════════════════
# 4. Trinity sites the Workday table did not list. "site | name or - | city |
# ST | source": a name shows the site's own name; "-" keeps the system name
# and gives the town. Read off the stored links of the label rows on
# 2026-10-29. The state of a ministry-code site is the ministry's: SPNY (St.
# Peter's Health Partners) New York; NESF / NESM / NEJM (Trinity Health Of New
# England's Saint Francis, Saint Mary's, Johnson Memorial) Connecticut; MSEPA
# and SMLPA (Mercy and St. Mary, southeastern Pennsylvania) Pennsylvania;
# MOELK (MercyOne Elkader) Iowa; THIHA, THWMI, THSEMI Michigan.
# ═══════════════════════════════════════════════════════════════════════════
_SITES = """
# named non-hospital sites that give their town
Mercy-Medical-Center---Springfield-MA | Mercy Medical Center | Springfield | MA | cms 220066
Kossuth-Regional-Health-Center---Algona-Iowa | Kossuth Regional Health Center | Algona | IA | cms 161353
St-Peters-Nursing--Rehabilitation-Center---Albany-New-York | St. Peter's Nursing and Rehabilitation Center | Albany | NY | site
Ellen-Kennedy-Living-Center---Dyersville-Iowa | Ellen Kennedy Living Center | Dyersville | IA | site
Mercy-Village---Fort-Gratiot-Mi | Mercy Village | Fort Gratiot | MI | site
Highland-Haven---Highland-Mi | Highland Haven | Highland | MI | site
Marian-Oakland---Farmington-Hills-Mi | Marian Oakland | Farmington Hills | MI | site
The-Oaks---Muskegon-Mi | Oaks | Muskegon | MI | site
The-Mercy-Community---West-Hartford-Connecticut | Mercy Community | West Hartford | CT | site
Highland-Hills-Village | Highland Hills Village | Watkinsville | GA | rows (posting text: "St. Mary's Highland Hills, a premier assisted living facility in Watkinsville, Georgia")
St-Agnes-Home-Health--Hospice---Fresno-California | Saint Agnes Home Health and Hospice | Fresno | CA | site
Mercy-Health-VNS---Muskegon-MI | Mercy Health VNS | Muskegon | MI | site
Mercy-Health-Home-Care---Grand-Rapids-MI | Mercy Health Home Care | Grand Rapids | MI | site
Saint-Joseph-PACE---Mishawaka-Indiana | Saint Joseph PACE | Mishawaka | IN | site
MercyOne-North-Iowa-Hospice---Mason-City-Iowa | MercyOne North Iowa Hospice | Mason City | IA | site
MercyOne-North-Iowa-Rehab-Services-Chelsea-Creek---Mason-City-Iowa | MercyOne North Iowa Rehab Services Chelsea Creek | Mason City | IA | site
Mercy-LIFE-Chester-County | Mercy LIFE Chester County | Coatesville | PA | rows (posting text: Coatesville, PA)
Mercy-LIFE---Montgomery-County | Mercy LIFE Norristown | Norristown | PA | rows (posting text: "Location: Norristown, PA", "Mercy LIFE Norristown, part of Trinity Health PACE")
Mercy-Life-Montgomery-County | Mercy LIFE Norristown | Norristown | PA | rows (posting text: "Location: Norristown, PA", "Mercy LIFE Norristown, part of Trinity Health PACE")
Mercy-LIFE---Delaware-County---Valley-View | Mercy LIFE Valley View | Elwyn | PA | rows (posting text: "Location: Elwyn, PA", "Mercy LIFE Valley View , part of Trinity Health PACE"; review A 2026-10-29: "Valley View" is the centre, not a town)
Saint-Francis-LIFE-College-Avenue | Saint Francis LIFE | Newark | DE | rows (posting text: Newark, DE)
Hospice-of-North-Ottawa-Community | Hospice of North Ottawa Community | Grand Haven | MI | rows (posting text: Grand Haven, MI)
THSC---Providence-Place | Providence Place | Holyoke | MA | rows (posting text: Holyoke, MA)
SPARC-Guilderland---Guilderland-New-York | SPARC Guilderland | Guilderland | NY | site
SPNY---SPARC---Latham | SPARC Latham | Latham | NY | site
Seton-Health-Rehabilitation--Clifton-Park---Clifton-Park-New-York | Seton Health Rehabilitation Clifton Park | Clifton Park | NY | site
Saugerties-Wellness-Center---Saugerties-New-York | Saugerties Wellness Center | Saugerties | NY | site
THHS---St-Francis-Home-Care---Newark-DE | St. Francis Home Care | Newark | DE | site
Athens-Home-Care--Hospice | St. Mary's Home Care & Hospice | Athens | GA | site; rows (posting text: St. Mary's Home Care & Hospice, Athens, Georgia)
Trinity-Health-PACE-of-Pensacola | Trinity Health PACE of Pensacola | Pensacola | FL | site
Trinity-Health-PACE-of-Alexandria | Trinity Health PACE Alexandria | Alexandria | LA | rows (posting text: Trinity Health PACE Alexandria, Central Louisiana)
MercyOne-Des-Moines-HH-and-Hospice | MercyOne Des Moines Home Health and Hospice | Des Moines | IA | site
NESM---Saint-Marys-Hospital | Saint Mary's Hospital | Waterbury | CT | cms 070016
SPNY---St-Peters-Hospital-3200310SM | St. Peter's Hospital | Albany | NY | cms 330057
SPNY---St-Peters-Hospital-3200317SM | St. Peter's Hospital | Albany | NY | cms 330057
SPNY---St-Peters-Hospital-3200319SM | St. Peter's Hospital | Albany | NY | cms 330057
SPNY---Samaritan-Hospital--St-Marys-Campus | Samaritan Hospital - St. Mary's Campus | Troy | NY | address 1300 Massachusetts Ave, Troy
SPNY---Marjorie-Doyle-Rockwell-Center | Eddy Village Green at Cohoes | Cohoes | NY | site (the tenant's own site: Eddy Village Green at Cohoes (Marjorie Doyle Rockwell Center) - Cohoes, New York)
# offices, clinics and street sites whose descriptor names the town (the
# system name stays; the town and state are the posting's own)
SPNY---63-Shaker-Rd-Albany | - | Albany | NY | site
SPNY---400-Patroon-Creek-Blvd-Albany | - | Albany | NY | site
SPNY---4-Palisades-Dr-Albany | - | Albany | NY | site
SPNY---5-Palisades-Dr-Albany | - | Albany | NY | site
SPNY---3-Atrium-Dr-Albany | - | Albany | NY | site
SPNY---1444-Western-Ave-Albany | - | Albany | NY | site
SPNY---1440-Western-Ave-Albany | - | Albany | NY | site
SPNY---1375-Washington-Ave-Albany | - | Albany | NY | site
SPNY---4-Executive-Park-Albany | - | Albany | NY | site
SPNY---855-Rt-146-Clifton-Park | - | Clifton Park | NY | site
SPNY---1735-US-9-Clifton-Park | - | Clifton Park | NY | site
SPNY---990-Route-67-Ballston-Spa | - | Ballston Spa | NY | site
2537-US-9-Ballston-Spa-NY | - | Ballston Spa | NY | site
SPNY---279-Troy-Road-Rensselaer | - | Rensselaer | NY | site
SPNY---713-Troy-Schenectady-Rd-Latham | - | Latham | NY | site
SPNY---30-Community-Way-East-Greenbush | - | East Greenbush | NY | site
SPNY---2-Empire-Dr-East-Greenbush | - | East Greenbush | NY | site
SPNY---10-Blacksmith-Dr-Malta | - | Malta | NY | site
SPNY---1-Bell-Tower-Dr-Watervliet | - | Watervliet | NY | site
SPNY---31-Lower-Hudson-St-Green-Island | - | Green Island | NY | site
# review A 2026-10-29: the site's own postings name their work site elsewhere ("Location: Northeast Home
# Medical Equipment, 60 Cohoes Ave, Green Island"), so the descriptor is a building, not a named site
Riverside-Medical-Center---Green-Island-New-York | - | Green Island | NY | rows (posting text: Northeast Home Medical Equipment, 60 Cohoes Ave, Green Island, NY)
SPNY---147-Hoosick-St-Troy | - | Troy | NY | site
SPNY---720-Hoosick-Road-Troy | - | Troy | NY | site
SPNY---1-Maple-Commons-Drive-Saratoga-Springs | - | Saratoga Springs | NY | site
SPNY---OBGYN-Troy | - | Troy | NY | site
NESM---Middlebury-THOfNEMG---1579-Straits-TPKE | - | Middlebury | CT | site
NESM---Wolcott-THOfNEMG---503-Wolcott-Rd | - | Wolcott | CT | site
NESM---WATERTOWN-THOfNEMG---486-Straits-TPKE | - | Watertown | CT | site
NESM---Prospect-THOfNEMG---166-Waterbury-RD | - | Prospect | CT | site
NESM---Waterbury-THOfNEMG---3801-E-Main-St | - | Waterbury | CT | site
NESM---Waterbury-THOfNEMG---1981-E-Main-St | - | Waterbury | CT | site
NESM---1312-West-Main-St-Waterbury | - | Waterbury | CT | site
NESM---133-Scovill-St-Waterbury-CT | - | Waterbury | CT | site
NESM---Naugatuck-THOfNEMG---58-Maple-Street-Naugatuck-CT | - | Naugatuck | CT | site
NESM---THOfNE-Medical-Group---Cheshire-154-Highland-Ave | - | Cheshire | CT | site
NESF---Windsor-Locks-THOfNEMG---2-Concorde-Way | - | Windsor Locks | CT | site
NESF---West-Hartford-THOfNEMG---345-N-Main | - | West Hartford | CT | site
NESF---THOfNE-Medical-Group---Canton-110-Albany-TPK | - | Canton | CT | site
NESF---27-Sycamore-St-Glastonbury-CT | - | Glastonbury | CT | site
NESF---1000-Asylum-Ave-Hartford-CT | - | Hartford | CT | site
NESF---THOfNE-Gengras-Building-CIN-LLC--1000-Asylum-Ave | - | Hartford | CT | site (1000 Asylum Ave, Hartford)
NESF---THOfNE-EMS---Broad-Street-Hartford | - | Hartford | CT | site
NEJM---140-Hazard-Ave-Enfield-CT | - | Enfield | CT | site
MSEPA---Mercy-Home-Health---Broad-St | - | Philadelphia | PA | rows (posting text: Philadelphia, PA)
SMLPA---OBGYN-Bucks-County | - | Langhorne | PA | rows (posting text: Langhorne, PA)
SMLPA---St-Mary-Health-at-New-Falls | - | Levittown | PA | rows (posting text: Levittown, PA)
St-Mary-Family-Medicine-Bensalem | - | Bensalem | PA | site
2300-State-Rt-256-Reynoldsburg | - | Reynoldsburg | OH | site
3570-Henry-St-Norton-Shores-MI | - | Norton Shores | MI | site
SJHS---Health-Insurance-Services---Plymouth | - | Plymouth | IN | site
NTT---Field-Services---Fort-Lauderdale | - | Ft. Lauderdale | FL | site
Nazareth-POB | - | Philadelphia | PA | rows (posting text: Philadelphia, PA)
Mercy-Catholic-Medical-Center---Mercy-Philadelphia-Campus | - | Philadelphia | PA | site (an outpatient campus since 2020)
SMLPA---St-Clare-Building | - | Langhorne | PA | rows (posting text: Langhorne, PA)
THWMI-Allendale-Primary-Care | - | Allendale | MI | site
MOELK---Elkader-Clinic | - | Elkader | IA | site
Above--Beyond-Monticello | - | Monticello | IA | rows (posting text: Monticello, IA)
"""
# Named sites no posting gives a town for, and offices whose descriptor names
# only their state: (name or None for the system name, state or None). A
# state None is taken only from the posting's own text, when it names exactly
# one state (posting_state). The row keeps its town field as it is.
NAMED_NO_TOWN = {
    W.key("Holy-Cross-Home-Care-and-Hospice"): ("Holy Cross Home Care and Hospice", None),
    W.key("Trinity-Health-Of-New-England-At-Home---CT"): ("Trinity Health Of New England At Home", "CT"),
    W.key("Pittsburgh-Mercy---Frech-Center"): ("Pittsburgh Mercy", "PA"),
    W.key("Pittsburgh-Mercy---Reedsdale-Center"): ("Pittsburgh Mercy", "PA"),
    W.key("Trinity-Health-PACE-of-Montgomery-County"): ("Trinity Health PACE of Montgomery County", None),
    W.key("Trinity-Health-PACE---LIFE-at-St-Mary"): ("LIFE at St. Mary", None),
    W.key("Trinity-Health-PACE---LIFE-at-Lourdes"): ("LIFE at Lourdes", "NJ"),   # "Location: PACE New Jersey"
    W.key("MercyOne-North-Iowa-HH-and-Hospice"): ("MercyOne North Iowa Home Health and Hospice", "IA"),
    W.key("Mercy-Home-Health---Bucks-County"): ("Mercy Home Health", "PA"),
    W.key("Mercy-Home-Health---Montgomery-County"): ("Mercy Home Health", "PA"),
    W.key("Mercy-Home-Health---Delaware-County"): ("Mercy Home Health", "PA"),
    W.key("Mercy-Home-Health---Springfield"): ("Mercy Home Health", "PA"),
    W.key("MSEPA---St-Mary-Home-Care"): ("St. Mary Home Care", "PA"),
    W.key("THHS---Johnson-Memorial-Home-Care--Hospice"): ("Johnson Memorial Home Care and Hospice", None),
    W.key("SPNY---Eddy-Senior-Care"): ("Eddy Senior Care", "NY"),
    W.key("THSC---St-Lukes"): ("St. Luke's Home", None),
    W.key("Saint-Joseph-VNA-Home-Care"): ("Saint Joseph VNA Home Care", None),
    W.key("SO---West-Lake-Surgical-Center"): ("West Lake Surgical Center", None),
    # offices: the system name, the state their ministry code gives
    W.key("NESF---THOfNE-MG-Multispecialty-11-South-RD"): (None, "CT"),
    W.key("NESF---THOfNEMG-UroGynocology---580-Cottage-Grove"): (None, "CT"),
    W.key("NESM---THOfNEMG-Bariartrics---590-Middlebury-Rd"): (None, "CT"),
    W.key("NEJM---Johnson-Memorial-Cancer-Center"): (None, "CT"),
    W.key("Loyola-Center-for-Health-on-Roosevelt-Rd"): (None, None),
    W.key("THSEMI---IHA"): (None, "MI"),
    W.key("SMLPA---Comprehensive-Cardiology-Consult"): (None, "PA"),
    W.key("Trinity-Health-of-New-England-Medical-Group"): (None, None),
    W.key("THIHA---Obstetrics--Gynecology-Schoolcraft-Campus"): (None, "MI"),
    W.key("SPNY---SPHP-Warehouse"): (None, "NY"),
    W.key("MOELK---Mona-Clinic"): (None, "IA"),
}
_LIVONIA_MI_RX = re.compile(r"\bLivonia,?\s+Mich(?:igan|\.)?", re.I)


def posting_state(description) -> str:
    """The one US state the posting's own sentences name (by name, or a
    "Town, ST" pair), or "". Trinity's own "Trinity Health of Livonia,
    Michigan" is not the posting's state."""
    found = set()
    own = [_LIVONIA_MI_RX.sub(" ", x) for x in own_sentences(description)]
    for x in own:
        for nm, code in _ST_NAMES.items():
            if re.search(r"\b" + re.escape(nm) + r"\b", x, re.I):
                found.add(code)
        for m in re.finditer(r",\s*([A-Z]{2})\b(?:\s+\d{5})?", x):
            if m.group(1) in _ST_CODES:
                found.add(m.group(1))
    if "WV" in found:
        found.discard("VA")     # "West Virginia" also reads "Virginia"
    return next(iter(found)) if len(found) == 1 else ""


_ADDED_SITES: dict[str, tuple] = {}
for _line in _SITES.strip().splitlines():
    _line = _line.strip()
    if not _line or _line.startswith("#"):
        continue
    _site, _name, _city, _state, _src = [p.strip() for p in _line.split("|")]
    _k = W.key(_site)
    _tbl = W.TABLES.setdefault(TRINITY, {})
    _old = _tbl.get(_k)
    _entry = (None if _name == "-" else _name, _city, _state, _src)
    if _old is not None and _old[:3] != _entry[:3]:
        raise ValueError(f"{TRINITY}: {_site!r} already listed as {_old[:3]!r}")
    _tbl[_k] = _entry
    _ADDED_SITES[_k] = _entry


# ═══════════════════════════════════════════════════════════════════════════
# The Trinity decision.
# ═══════════════════════════════════════════════════════════════════════════
def _keep(why):
    return None, None, None, why


# Trinity's regional float postings ("Float Registered Nurse - Clinical
# Staffing Center"): no one hospital's.
TRINITY_POOL_RX = re.compile(r"\b(?:clinical\s+staffing|staffing\s+(?:center|centre|office|pool)|"
                             r"regional\s+float|system\s+float)\b", re.I)


# Review A, 2026-10-29: a posting that says it is worked mostly from home
# ("This is a hybrid position with opportunity to work most of the schedule
# from home", "This position is a hybrid position. Spending 2 days a week in
# one of the offices ... and 3 days remote") is no one site's (decision 2:
# remote postings show the system name), even when its location line or its
# Workday site names one. A hybrid operating room or a hybrid shift is not
# matched.
HYBRID_RX = re.compile(
    r"\b(?:hybrid\s+(?:position|role|work\s+model|work\s+arrangement|schedule\s+with)|"
    r"this\s+(?:position|role|job)\s+is\s+(?:a\s+)?hybrid|"
    r"(?:\d|one|two|three|four)\s+days?\s+(?:a\s+week\s+|per\s+week\s+)?remote(?:ly)?|"
    r"work\s+(?:most\s+of\s+the\s+(?:schedule|week)\s+)?from\s+home|fully\s+remote|100%\s+remote)\b", re.I)
# A regional leader ("Regional Administrative Director, Heart and Vascular
# Services ... across the West Michigan market", "Regional Service Line
# Director - Oncology Services"): no one hospital's (review A, 2026-10-29).
REGIONAL_ROLE_RX = re.compile(
    r"\bregional\s+(?:administrative\s+|service\s+line\s+|clinical\s+|operations\s+|executive\s+|senior\s+|"
    r"sr\.?\s+|medical\s+|nursing\s+)*(?:director|manager|leader|vice\s+president|vp|officer|executive)\b", re.I)


def is_hybrid(title, description) -> bool:
    return bool(HYBRID_RX.search(clean_text(title)) or HYBRID_RX.search(clean_text(str(description or "")[:4000])))


def site_table_keep(title, description) -> str:
    """Why a row the Workday site table gave a named non-hospital site keeps
    the system name instead ("remote"), or "" (review A, 2026-10-29)."""
    if T.is_remote(title, description) or is_hybrid(title, description):
        return "remote"
    return ""


def _exclusions(system, title, description):
    """Why a posting is no one hospital's (the TOWN family's tests), or ""."""
    if T.is_remote(title, description):
        return "remote"
    if T.is_care_site(system, title):
        return "care-site"
    if T.is_multi_site(title, description):
        return "multi-site"
    if T.where_not_hospital(description) or T.SYSTEM_ROLE_RX.search(clean_text(title)):
        return "offsite"
    return ""


def decide_trinity(hospital_name, city, state, title="", description="", url=""):
    """(hospital_name, city, state, how) for one Trinity label row, or
    (None, None, None, how) when it keeps the system name.
    Named: "text" (the posting's own text names its hospital), "campus" (a
    hospital campus site, the posting is no clinic's or office's).
    Left to the town rule (facilities_town.decide): "town-rule".
    Kept: "named-already", "site" (a clinic, office, street or other
    non-hospital site), "region" (a region site and the text names no
    hospital), "text-elsewhere" (the text names a hospital in another town or
    state), "text-several", and the TOWN family's exclusions."""
    if not SPEC.is_label(hospital_name):
        return _keep("named-already")
    title = clean_text(title).replace("_", " ")     # "Medical Oncologist_ Plymouth"
    st = (state or "").strip().upper()
    kind, info = site_kind(url, city, st)
    # review A, 2026-10-29: a hybrid / mostly-from-home posting or a regional
    # leader's keeps the system name, and the town rule is told to leave it
    if is_hybrid(title, description):
        return _keep("remote")
    if REGIONAL_ROLE_RX.search(title):
        return _keep("multi-site")
    if kind == "site":
        nt = NAMED_NO_TOWN.get(W.key(W.path_segment(url or "")))
        if nt is None:
            return _keep("site")
        name, nst = nt
        if name and T.is_remote(title, description):
            return _keep("remote")
        new_st = st or nst or posting_state(description)
        if name:
            return name, clean_text(city), new_st, "site-name"
        if new_st and new_st != st:
            return TRINITY, clean_text(city), new_st, "site-state"
        return _keep("site")
    why = _exclusions(TRINITY, title, description)
    if kind == "region":
        st = info
    elif kind == "campus":
        st = info[2]
    if TRINITY_POOL_RX.search(title):
        # a staffing-centre float works across the region's hospitals: no one
        # hospital's, and the town rule is told to leave it
        return _keep("multi-site")
    if why:
        return _keep(why) if kind != "town" else (None, None, None, "town-rule")
    names = named_in_text(title, description, st)
    if len(names) > 1:
        # two of the system's hospitals in the posting's own sentences
        return _keep("text-several") if kind != "town" else (None, None, None, "town-rule")
    offsite = T._offsite_by_title(title, description, SPEC) or T.is_offsite(title, description, SPEC)
    if len(names) == 1:
        h = _HOSP_BY_NAME_ST[(names[0], st)]
        towns = SPEC.hosp_towns.get(h[0], set())
        if kind == "town":
            if (town_key(info[0]), info[1]) not in towns:
                return _keep("text-elsewhere")
            if offsite:
                return None, None, None, "town-rule"
            return h[0], T._town_spelling(SPEC, city, st) if clean_text(city) else info[0], info[1], "text"
        if kind == "campus":
            if h[0] != info[0]:
                return _keep("text-elsewhere")
            if offsite:
                return _keep("offsite")
            return h[0], info[1], info[2], "text"
        # region: the town on the row is stamped; the hospital's own town
        if offsite:
            return _keep("offsite")
        return h[0], h[1], h[2], "text"
    if kind == "campus":
        if offsite:
            return _keep("offsite")
        return info[0], info[1], info[2], "campus"
    if kind == "region":
        return _keep("region")
    return None, None, None, "town-rule"


# ═══════════════════════════════════════════════════════════════════════════
# SANFORD: Good Samaritan Society communities.
# ═══════════════════════════════════════════════════════════════════════════
# communities whose name is not their town (the postings' own spellings), by town
GSS_COMMUNITIES = {
    ("brainerd", "MN"): ("Bethany", "Woodland"),
    ("st paul", "MN"): ("Maplewood",),
    ("mandan", "ND"): ("Miller Pointe",),
    ("bismarck", "ND"): ("St. Vincent's",),
    ("robbinsdale", "MN"): ("Specialty Care Community",),
}
# communities a title names by their full name (the department line says Good
# Samaritan but not which community): pattern -> (name, town, ST)
GSS_TITLE_COMMUNITIES = {
    r"\bPohai\s+Nani\b": ("Pohai Nani", "Kaneohe", "HI"),
    r"\bLodge\s+of\s+Taylors\s+Falls\b": ("The Lodge of Taylors Falls", "Taylors Falls", "MN"),
}
_GSS_STOP = re.compile(r"\b(?:our|is|offers|provides|has|located|where|specializ\w*|serves|community|facility|"
                       r"campus|agency|team|in|seeks|seeking|and|we|the|a|an|at|of)\b", re.I)
_GSS_PATTERNS = [
    # "Good Samaritan Society - Mandan Sunset", "Good Samaritan - Bethany", "Good Samaritan-Scotland"
    re.compile(r"\bGood\s+Samaritan(?:\s+Society)?\s*[-–]\s*([A-Z][\w.'&]*(?:\s+[A-Z][\w.'&]*){0,4})"),
    # "At Good Samaritan, Atkinson, our vision", "At St. Vincent's, Good Samaritan, Bismarck, ND"
    re.compile(r"\bAt\s+(?:([A-Z][\w.']*(?:\s+[A-Z][\w.']*){0,3}),\s+)?Good\s+Samaritan(?:\s+Society)?,\s+"
               r"([A-Z][\w.']*(?:\s+[A-Z][\w.']*){0,2})(?:,\s*[A-Z]{2})?,\s+our\s+vision"),
    # "At Villisca Good Samaritan", "At our Beatrice Good Samaritan facility", "At the Mason Good Samaritan"
    re.compile(r"\bAt\s+(?:our\s+|the\s+)?([A-Z][\w.']*(?:\s+[A-Z][\w.']*){0,2})\s+Good\s+Samaritan\b"),
    # "Good Samaritan Society in Mountain Lake", "Good Samaritan in Tyndall offers"
    re.compile(r"\bGood\s+Samaritan(?:\s+Society)?\s+(?:in|of)\s+([A-Z][\w.']*(?:\s+[A-Z][\w.']*){0,2})"),
    # "Good Samaritan Albion offers", "Good Samaritan Hastings Village is"
    re.compile(r"\bGood\s+Samaritan\s+(?!Society\b)([A-Z][\w.']*(?:\s+[A-Z][\w.']*){0,3})"),
]
_GSS_NOT_ONE = re.compile(
    r"\b(?:locations\s+in|traveler|travel\s+required|multiple\s+locations|home\s+health|hospice|agency|"
    r"remote|several\s+locations|surrounding\s+(?:communities|locations)|all\s+locations|region(?:al)?)\b", re.I)


def _gss_clean(name) -> str:
    words = []
    for w in clean_text(name).split():
        if _GSS_STOP.fullmatch(w.strip(".,")):
            break
        words.append(w.strip(",."))
    return " ".join(words).strip()


def gss_community(title, description, city, state):
    """("Good Samaritan Society - <community>", how) when the posting's own
    department sentence names its Good Samaritan Society community in the
    row's own town, else (None, why)."""
    st = (state or "").strip().upper()
    c = clean_text(city)
    if not c or not st:
        return None, "no-town"
    d = clean_text(str(description or "")[:2500])
    m = re.search(r"Department Details\s*(.{0,500})", d)
    if not m:
        return None, "no-department"
    head = m.group(1)
    if "Good Samaritan" not in head:
        return None, "not-gss"
    if _GSS_NOT_ONE.search(clean_text(title)) or _GSS_NOT_ONE.search(head[:300]):
        return None, "not-one-site"
    if T.is_remote(title, description) or T.is_multi_site(title, description):
        return None, "not-one-site"
    tk = town_key(c)
    listed = GSS_COMMUNITIES.get((name_key(c).replace("saint", "st"), st), ())
    for pat, (nm, ntown, nst) in GSS_TITLE_COMMUNITIES.items():
        if (town_key(ntown), nst) == (tk, st) and re.search(pat, clean_text(title), re.I):
            return f"Good Samaritan Society - {nm}", "gss-title"
    for rx in _GSS_PATTERNS:
        for mm in rx.finditer(head):
            groups = [g for g in mm.groups() if g]
            if not groups:
                continue
            cand = [_gss_clean(g) for g in groups]
            cand = [x for x in cand if x]
            for x in cand:
                xk = town_key(x)
                if xk == tk:
                    return f"Good Samaritan Society - {c}", "gss-town"
                if xk.startswith(tk) and len(x.split()) <= 4:
                    return f"Good Samaritan Society - {x}", "gss-named"
                for nm in listed:
                    if name_key(x) == name_key(nm):
                        return f"Good Samaritan Society - {nm}", "gss-listed"
    return None, "gss-unnamed"


# ═══════════════════════════════════════════════════════════════════════════
# GENTIVA and ELARA: the branch the posting names.
# ═══════════════════════════════════════════════════════════════════════════
GENTIVA_BRANDS = {
    "gentiva hospice": "Gentiva Hospice", "heartland hospice": "Heartland Hospice",
    "southercare hospice": "SouthernCare Hospice", "southerncare hospice": "SouthernCare Hospice",
    "hospice advantage": "Hospice Advantage", "wesley hospice": "Wesley Hospice",
    "harbor light hospice": "Harbor Light Hospice", "gentiva home health": "Gentiva Home Health",
    "gentiva palliative care": "Gentiva Palliative Care",
}
_GENTIVA_RX = re.compile(r"\bLocation:\s*((?:[A-Z][\w']*\s+){1,3}(?:Hospice|Home\s+Health|Palliative\s+Care))\s+Branch,"
                         r"[^|]{0,160}?,\s*([A-Z][A-Za-z.' ]+?),\s*([A-Z]{2}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b")
_ELARA_RX = re.compile(r"\bBranch:\s*([A-Z][A-Za-z.' ]+?),\s*([A-Z]{2}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b")


def _state_code(s):
    s = (s or "").strip()
    if s.upper() in _ST_CODES:
        return s.upper()
    return _ST_NAMES.get(s.lower(), "")


def branch_name(system, title, description, city, state):
    """("<brand> - <town>", how) for a Gentiva / Elara posting whose own text
    names its branch in the row's own town, else (None, why)."""
    c, st = clean_text(city), (state or "").strip().upper()
    if not c or not st:
        return None, "no-town"
    if T.is_remote(title, description):
        return None, "remote"
    d = clean_text(str(description or "")[:4000])
    if system == GENTIVA:
        m = _GENTIVA_RX.search(d)
        if not m:
            return None, "no-branch"
        brand = GENTIVA_BRANDS.get(name_key(m.group(1)))
        if not brand:
            return None, "unknown-brand"
        town, bst = clean_text(m.group(2)), _state_code(m.group(3))
    elif system == ELARA:
        m = _ELARA_RX.search(d)
        if not m:
            return None, "no-branch"
        brand = "Elara Caring"
        town, bst = clean_text(m.group(1)), _state_code(m.group(2))
    else:
        return None, "not-table"
    if bst != st or town_key(town) != town_key(c):
        return None, "branch-elsewhere"
    return f"{brand} - {c}", "branch"


# ═══════════════════════════════════════════════════════════════════════════
# Run time.
# ═══════════════════════════════════════════════════════════════════════════
UNKNOWN: dict = {}
_ROW_FIELDS = ("hospital_name", "city", "state", "location")


def _set(job, name, city, state):
    job.hospital_name = name
    if state and (city, state) != (clean_text(job.city), (job.state or "").strip().upper()):
        job.city, job.state = city, state
        job.location = f"{city}, {state}" if city else state


def _row_guard(fn, job, *args):
    """A row that raises keeps the name and town it came with ("error")."""
    keep = [(a, getattr(job, a)) for a in _ROW_FIELDS if hasattr(job, a)]
    try:
        return fn(job, *args)
    except Exception:
        for a, v in keep:
            try:
                setattr(job, a, v)
            except Exception:
                pass
        return "error"


def _has_body(job) -> bool:
    return len(str(job.description or "").strip()) >= T.NO_BODY_CHARS


def _trinity_no_body(job) -> str:
    """A night without a body (a known body the scraper sends blank, or a
    list teaser): the stored decision stands (facilities_town.STORED); a row
    the map does not hold is decided only by what needs no body (a named
    site's name, a bare-town site left to the town rule); every other site
    keeps the system name and the town rule is told to leave it."""
    kept = T.STORED.get((TRINITY, str(job.job_id)))
    if kept is not None:
        name, city, st = kept
        if name and not SPEC.is_label(name):
            job.hospital_name = name
            if not clean_text(job.city) and city:
                job.city = city
            if not (job.state or "").strip() and st:
                job.state = st
            return "kept"
        if not (job.state or "").strip() and st:
            job.state = st
        setattr(job, SKIP_ATTR, True)
        return "kept-label"
    kind, _info = site_kind(job.url, job.city, job.state)
    if kind == "town":
        return "no-body"
    nt = NAMED_NO_TOWN.get(W.key(W.path_segment(job.url or "")))
    if nt and nt[0]:
        _set(job, nt[0], clean_text(job.city), (job.state or "").strip().upper() or (nt[1] or ""))
        return "site-name"
    setattr(job, SKIP_ATTR, True)
    return "no-body-skip"


def _trinity_one(job) -> str:
    if not _has_body(job):
        return _trinity_no_body(job)
    name, city, st, how = decide_trinity(job.hospital_name, job.city, job.state, job.title, job.description,
                                         job.url)
    if name:
        _set(job, name, city, st)
    if (not name or name == TRINITY) and how != "town-rule":
        setattr(job, SKIP_ATTR, True)
    return how


def _trinity_site_guard(job) -> str:
    """A row the Workday site table named (a named non-hospital site of
    _SITES) whose posting says it is worked mostly from home goes back to the
    system name, with no town and the site's state (review A, 2026-10-29:
    "Administrative Assistant/CSR local to Hartford, Ct." on the Newark, DE
    home-care site). On a night without a body the stored decision stands:
    a row the database holds on the system name keeps it (and its stored
    town), so the site table does not name it again ("site-kept-label").
    Returns "site-remote", "site-kept-label", or "" when nothing changes."""
    entry = _ADDED_SITES.get(W.key(W.path_segment(job.url or "")))
    if not entry or not entry[0] or clean_text(job.hospital_name) != entry[0]:
        return ""
    if not _has_body(job):
        kept = T.STORED.get((TRINITY, str(job.job_id)))
        if not kept or kept[0] != TRINITY:
            return ""
        job.hospital_name = TRINITY
        job.city, job.state = kept[1], kept[2]
        job.location = f"{kept[1]}, {kept[2]}" if kept[1] and kept[2] else kept[1] or kept[2]
        setattr(job, SKIP_ATTR, True)
        return "site-kept-label"
    if not site_table_keep(job.title, job.description):
        return ""
    job.hospital_name = TRINITY
    job.city, job.state, job.location = "", entry[2], entry[2]
    setattr(job, SKIP_ATTR, True)
    return "site-remote"


def _stored(system, job):
    return T.STORED.get((system, str(job.job_id)))


def _sanford_one(job) -> str:
    if not _has_body(job):
        return "no-body"          # facilities_town's stored decision (Sanford is a TownSpec)
    name, how = gss_community(job.title, job.description, job.city, job.state)
    if name:
        job.hospital_name = name
    return how


def _branch_one(job, system) -> str:
    if not _has_body(job):
        kept = _stored(system, job)
        if kept and kept[0] and kept[0] != system:
            job.hospital_name = kept[0]
            return "kept"
        return "no-body"
    name, how = branch_name(system, job.title, job.description, job.city, job.state)
    if name:
        job.hospital_name = name
    return how


def apply_jobs(jobs, canon=None) -> dict:
    """finalize_jobs' pass, after the adapters' hooks and the other families'
    passes, before the town tables: Trinity's text, campus and site
    decisions (the town rule follows in facilities_town), Sanford's Good
    Samaritan Society communities, Gentiva's and Elara's named branches.
    Returns {system: {how: rows}} for the label rows it read."""
    stats: dict = {}
    for job in jobs:
        system = canon(job.hospital_system) if canon else job.hospital_system
        if system == TRINITY and clean_text(job.hospital_name) not in ("", system):
            how = _row_guard(_trinity_site_guard, job)
            if how:
                d = stats.setdefault(system, {})
                d[how] = d.get(how, 0) + 1
            continue
        if clean_text(job.hospital_name) not in ("", system):
            continue
        if system == TRINITY:
            how = _row_guard(_trinity_one, job)
        elif system == SANFORD:
            how = _row_guard(_sanford_one, job)
        elif system in (GENTIVA, ELARA):
            how = _row_guard(_branch_one, job, system)
        else:
            continue
        d = stats.setdefault(system, {})
        d[how] = d.get(how, 0) + 1
    return stats


# Gentiva's and Elara's stored branch names, for nights without a body. They
# are no TownSpec systems, so facilities_town.load_stored does not read them;
# load_stored below adds their stored rows to the same map.
BRANCH_SYSTEMS = (GENTIVA, ELARA)


def load_stored() -> int:
    """Read-only PostgREST passes over Gentiva's and Elara's active rows whose
    name is a branch (not the system label), into facilities_town.STORED.
    Without credentials nothing is read. Returns the rows added."""
    import time
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    try:
        import sb_keys
        sb_url = os.environ.get("SUPABASE_URL", "")
        sb_key = sb_keys.supabase_key()
    except Exception:
        sb_url, sb_key = "", ""
    if not sb_url or not sb_key:
        return 0
    n = 0
    for system in BRANCH_SYSTEMS:
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        nflt = _urlparse.quote(f"neq.{system}", safe=".")
        last = 0
        try:
            while True:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,job_id,hospital_name,city,state"
                     f"&is_active=is.true&hospital_system={flt}&hospital_name={nflt}&id=gt.{last}&order=id.asc"
                     f"&limit={T.STORED_PAGE}")
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
                for r in chunk:
                    T.STORED[(system, str(r.get("job_id")))] = (clean_text(r.get("hospital_name")),
                                                               clean_text(r.get("city")),
                                                               clean_text(r.get("state")).upper())
                    n += 1
                last = chunk[-1]["id"]
                if len(chunk) < T.STORED_PAGE:
                    break
        except Exception:
            continue
    return n
