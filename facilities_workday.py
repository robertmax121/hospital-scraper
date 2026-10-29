"""The hospital and its real town on Workday rows filed under a system name (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard, and applies are
retro-assigned to the right hospital ("any multi system hospital is subject
to this"). His two rules:
  1. cards and job pages show the HOSPITAL ("Apply to <hospital>"); the
     system name and logo stay on the job page and the system hub
     (hospital_system is never touched here: the upsert key, hub, filters,
     logo);
  2. named sites (hospitals, named clinics and centers) show their own name;
     generic offices, home-based and remote jobs show the system name with
     the town; a town is never guessed.

Every Workday posting names its primary site twice: the list's locationsText
(the location descriptor, or "2 Locations" on a posting with more than one)
and the first segment of its externalPath, which is that same primary
descriptor slugged ("NYP/Weill Cornell Medical Center" ->
"/job/NYPWeill-Cornell-Medical-Center/..."). The stored url keeps the
segment, so the same key places live rows and expired rows that carry
applies. advocate_facilities.py (2026-10-29) did this for Advocate Health;
this module does it for the other Workday systems, one table each.

  TABLES[system]          site key -> (hospital_name or None, city, state,
                          source). The key is the descriptor (or the path
                          segment) lower-cased, letters and digits only, so
                          "NYP/Weill Cornell Medical Center" and
                          "NYPWeill-Cornell-Medical-Center" are one key.
                          hospital_name None keeps the system label (an
                          office, a town-only or street-only site) and gives
                          the town; a site listed with no town is never
                          placed by the town rules below (its town would be
                          a guess). Source: "cms <id>" (the CMS hospital
                          list in the database), "address <street, town>"
                          (the facility's published street address), "site"
                          (the descriptor itself names the town), "rows"
                          (the town the postings' own text or parse names).
  facility(system, loc, path)  (hospital_name, city, state) for one posting,
                          or None to place it as before.
  unknown_site(system, loc, path)  a site that reads like a hospital or a
                          medical center the table does not know, for the
                          run log ("" otherwise).
  placed(system, name, city, state)  True when (name, city, state) is a
                          place this module gives: the head-office override
                          (FORCE_LOCATION_OVERRIDE) must not move it.
  town_state_name(text)   (town, ST) for a "Town, StateName" or
                          "Town-StateName" site ("Mt. Pleasant, Iowa",
                          "Mt-Pleasant-Iowa", "Cut-Bank-Montana"), the town
                          checked against the known-town list.
  town_lead(text, state)  (town, ST) for a "Town - street" site ("Moorestown
                          - 401 Young Ave"), only on boards in TOWN_SITE, the
                          town checked against the known-town list.
  town_in_site(text, state)  the last known town of `state` in the site text
                          that is not part of a street name.

A table town is the facility's own street address and wins over the parse
(the parse of a site name is no town); a town the board itself writes keeps
the board's spelling. Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase2/workday_work/changes.py),
so the database can hold what the scraper keeps.
"""
from __future__ import annotations

import re

from city_utils import known_town

TABLES: dict[str, dict[str, tuple[str | None, str, str, str]]] = {}
# system label -> the label the rows carry when it differs from the Workday
# emit name (the database name, HOSPITAL_SYSTEM_ALIASES); both are accepted.
ALSO: dict[str, str] = {}
# boards whose clinic and office descriptors name their town: system ->
# (state, "lead") for "Town - street" or a bare "Town" (town_lead), or
# (state, "any") for a town anywhere in the site text that is not a street
# name ("NCO-15-S-Mchenry-Rd-Buffalo-Grove" is Buffalo Grove, not McHenry:
# town_in_site). Reviewed per board against its stored sites.
TOWN_SITE: dict[str, tuple[str, str]] = {}
# site-key prefixes (an entity code that opens every site of one hospital,
# "EVH-..." at Endeavor): system -> [(prefix key, entry)], longest first.
PREFIXES: dict[str, list[tuple[str, tuple[str | None, str, str, str]]]] = {}
# boards whose every descriptor is "Town, StateName": the parsed town is the
# posting's (FORCE_LOCATION_OVERRIDE must not move it).
TOWN_BOARDS: set[str] = set()

_N_LOCATIONS_RX = re.compile(r"^\s*\d+\s+locations?\s*$", re.I)
_HOSPITALISH_RX = re.compile(r"hospital|medical center|med ctr|medical ctr|regional health|health center|"
                             r"\bcampus\b|children'?s|rehab", re.I)
_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


def key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def path_segment(external_path: str) -> str:
    """The site segment of an externalPath ("/job/<site>/<title>_<id>") or of
    a stored Workday url ("https://.../<board>/job/<site>/<title>_<id>")."""
    p = external_path or ""
    if "/job/" not in p:
        return ""
    m = re.match(r"([^/]+)/", p.split("/job/", 1)[1])
    return m.group(1) if m else ""


def _t(system: str, rows: str, also: str | None = None) -> None:
    """One system's table from "site | name or - | city | ST | source" lines
    (the site as the board writes it, or its path segment)."""
    tbl = TABLES.setdefault(system, {})
    for line in rows.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        site, name, city, state, source = [p.strip() for p in line.split("|")]
        if site.endswith("*"):
            PREFIXES.setdefault(system, []).append((key(site[:-1]), (None if name == "-" else name, city, state, source)))
            PREFIXES[system].sort(key=lambda kv: -len(kv[0]))
            continue
        k = key(site)
        if not k:
            raise ValueError(f"{system}: empty site key in {line!r}")
        if k in tbl and tbl[k][:3] != ((None if name == "-" else name), city, state):
            raise ValueError(f"{system}: {site!r} listed twice with different places")
        tbl[k] = (None if name == "-" else name, city, state, source)
    if also:
        ALSO[also] = system


def _system(system: str) -> str | None:
    if system in TABLES or system in TOWN_SITE or system in TOWN_BOARDS or system in PREFIXES:
        return system
    return ALSO.get(system)


def _keys(loc: str, external_path: str) -> list[str]:
    """The posting's site key: the descriptor when locationsText is one site,
    else (a "N Locations" posting, or no text) the externalPath segment, the
    primary site. A posting whose location was edited keeps its first slug,
    so the descriptor wins when there is one (as advocate_facilities)."""
    loc = (loc or "").strip()
    if loc and not _N_LOCATIONS_RX.match(loc):
        return [k for k in (key(loc),) if k]
    seg = path_segment(external_path) if external_path else ""
    return [k for k in (key(seg),) if k]


def _site_text(loc: str, external_path: str) -> str:
    loc = (loc or "").strip()
    if loc and not _N_LOCATIONS_RX.match(loc):
        return loc
    seg = path_segment(external_path) if external_path else ""
    # "Moorestown---401-Young-Ave" -> "Moorestown - 401 Young Ave" (" - " is
    # "---" in the slug, a space is "-")
    return " - ".join(c.replace("-", " ").strip() for c in re.split(r"-{3,}", seg) if c.strip("-"))


def town_state_name(text: str) -> tuple[str, str] | None:
    """(town, ST) for "Town, StateName" / "Town-StateName" / "Town StateName";
    None unless the town is a known town of that state."""
    t = re.sub(r"[\s_-]+", " ", (text or "").replace(",", " ")).strip()
    words = t.split(" ")
    for n in (2, 1):
        if len(words) > n:
            st = _STATES.get(" ".join(words[-n:]).lower())
            if st:
                town = known_town(" ".join(words[:-n]).strip(" ."), st)
                if town:
                    return town, st
    return None


def town_lead(text: str, state: str) -> tuple[str, str] | None:
    """(town, ST) for "Town - street" or a bare "Town" in `state`; None unless
    the part before the street is a known town of that state."""
    head = re.split(r"\s+-\s+|\s*-{2,}\s*", (text or "").strip())[0].strip()
    if not head or re.search(r"\d", head):
        return None
    town = known_town(head, state)
    return (town, state) if town else None


_STREET_WORDS = {"rd", "road", "st", "street", "ave", "avenue", "blvd", "boulevard", "dr", "drive", "pkwy",
                 "parkway", "ln", "lane", "way", "ct", "court", "hwy", "highway", "route", "rt", "pike", "cir",
                 "circle", "pl", "place", "ter", "terrace", "trl", "trail", "plaza", "sq", "square", "turnpike"}


def town_in_site(text: str, state: str) -> tuple[str, str] | None:
    """(town, ST): the last known town of `state` named in the site text,
    longest spelling first, skipping a name the next word makes a street
    ("Waukegan Road", "Milwaukee Ave") or that sits in or just after a
    street number ("100 Bowman Drive", "700 W Lincoln"); None when the text
    names none."""
    words = [w for w in re.split(r"[\s_/,()-]+", text or "") if w]
    best = None
    i = 0
    while i < len(words):
        hit = None
        for n in (3, 2, 1):
            if i + n > len(words):
                continue
            nxt = words[i + n].lower().strip(".") if i + n < len(words) else ""
            if nxt in _STREET_WORDS:
                continue
            if any(re.search(r"\d", w) for w in words[max(0, i - 2):i + n]):
                continue   # a street address ("700 W Lincoln"), not a town
            town = known_town(" ".join(words[i:i + n]), state)
            if town:
                hit = (town, n)
                break
        if hit:
            best = hit[0]
            i += hit[1]
        else:
            i += 1
    return (best, state) if best else None


def facility(system: str, loc: str, external_path: str = "") -> tuple[str, str, str] | None:
    """(hospital_name, city, state) for one posting of `system`, or None."""
    s = _system(system)
    if not s:
        return None
    tbl = TABLES.get(s, {})
    keys = _keys(loc, external_path)
    for k in keys:
        hit = tbl.get(k)
        if hit:
            # a site listed with no town is one the table refuses to place
            # (its town would be a guess): placed as before
            return ((hit[0] or system), hit[1], hit[2]) if hit[1] and hit[2] else None
    for k in keys:
        for pre, hit in PREFIXES.get(s, ()):
            if k.startswith(pre):
                return (hit[0] or system), hit[1], hit[2]
    text = _site_text(loc, external_path)
    if s in TOWN_BOARDS:
        ts = town_state_name(text)
        if ts:
            return system, ts[0], ts[1]
    if s in TOWN_SITE:
        state, mode = TOWN_SITE[s]
        tl = town_lead(text, state) if mode == "lead" else town_in_site(text, state)
        if tl:
            return system, tl[0], tl[1]
    return None


def unknown_site(system: str, loc: str, external_path: str = "") -> str:
    """A site that reads like a hospital or medical center and that the table
    does not know ("" otherwise), for the run log."""
    s = _system(system)
    if not s or facility(system, loc, external_path):
        return ""
    text = _site_text(loc, external_path)
    return text if _HOSPITALISH_RX.search(text) else ""


def placed(system: str, name: str, city: str, state: str) -> bool:
    """True when (name, city, state) is a place this module gives for
    `system` (a table entry, or a town a TOWN_BOARDS / TOWN_SITE board
    writes): the head-office override must leave it."""
    s = _system(system or "")
    if not s or not city or not state:
        return False
    entries = list(TABLES.get(s, {}).values()) + [e for _k, e in PREFIXES.get(s, ())]
    for nm, c, st, _src in entries:
        if c and (c, st) == (city, state) and (nm or system) == name:
            return True
    if (s in TOWN_BOARDS or s in TOWN_SITE) and name == system and known_town(city, state) == city:
        return True
    return False


def covers(system: str) -> bool:
    """True when this module places `system`'s rows."""
    return _system(system or "") is not None


def resolver(system: str):
    """facility() for one system, in the WD_SITE_FACILITY shape (loc, path)."""
    return lambda loc, external_path="": facility(system, loc, external_path)


def unknown_resolver(system: str):
    return lambda loc, external_path="": unknown_site(system, loc, external_path)


def systems() -> list[str]:
    """Every Workday emit name this module places."""
    return sorted(set(TABLES) | set(TOWN_SITE) | set(TOWN_BOARDS) | set(PREFIXES))


def names(system: str) -> list[str]:
    entries = list(TABLES.get(system, {}).values()) + [e for _k, e in PREFIXES.get(system, ())]
    return sorted({v[0] for v in entries if v[0]})


# ═══════════════════════════════════════════════════════════════════════════
# The tables. Sites read off each system's stored Workday links (active rows
# and expired rows with applies or submissions) on 2026-10-29, ranked by
# applies and submissions, then by rows. Hospital names are each facility's
# public name, with no leading "The" (as uhs_facilities); a name already on
# the rows (WD_FACILITY_MAP, facility_names) is kept. Towns are the
# facility's own street address.
# ═══════════════════════════════════════════════════════════════════════════

# ── NewYork-Presbyterian (nyp.wd1, nypcareers): "NYP/<campus>" ─────────────
# The campus sites are the hospitals; the medical groups, ambulatory and
# home sites keep the system label and the town WD_FACILITY_MAP gives.
_t("NewYork-Presbyterian", """
NYPWeill-Cornell-Medical-Center | NewYork-Presbyterian/Weill Cornell Medical Center | New York | NY | cms 330101; address 525 E 68th St, New York
NYPColumbia-University-Irving-Medical-Center | NewYork-Presbyterian/Columbia University Irving Medical Center | New York | NY | cms 330101; address 622 W 168th St, New York
NYPMorgan-Stanley-Childrens-Hospital | NewYork-Presbyterian Morgan Stanley Children's Hospital | New York | NY | address 3959 Broadway, New York
NYPAllen-Hospital | NewYork-Presbyterian Allen Hospital | New York | NY | address 5141 Broadway, New York
NYPLower-Manhattan-Hospital | NewYork-Presbyterian Lower Manhattan Hospital | New York | NY | address 170 William St, New York
NYPBrooklyn-Methodist-Hospital | NewYork-Presbyterian Brooklyn Methodist Hospital | Brooklyn | NY | address 506 6th St, Brooklyn
NYPQueens | NewYork-Presbyterian Queens | Flushing | NY | cms 330055
NYPHudson-Valley | NewYork-Presbyterian Hudson Valley Hospital | Cortlandt Manor | NY | cms 330267
NYPWestchester | NewYork-Presbyterian Westchester | Bronxville | NY | address 55 Palmer Ave, Bronxville
NYPWestchester-Behavioral-Health-Center | NewYork-Presbyterian Westchester Behavioral Health Center | White Plains | NY | address 21 Bloomingdale Rd, White Plains
Gracie-Square-Hospital | Gracie Square Hospital | New York | NY | cms 334048
""")

# ── MUSC Health (musc.wd1, MUSC): most sites are a town ("Charleston",
# "Florence"), which keeps the system label and its town; the named
# hospitals below. The bare town sites name no hospital (Columbia has two).
_t("MUSC Health", """
UNIVERSITY-HOSPITAL | MUSC Health University Medical Center | Charleston | SC | cms 420004
ASHLEY-RIVER-TOWER---25-COURTENAY-DR | MUSC Health University Medical Center | Charleston | SC | cms 420004; address 25 Courtenay Dr, Charleston
MUSC-Health-Marion-Medical-Center | MUSC Health Marion Medical Center | Mullins | SC | cms 420055
MUSC-Health-Black-River-Medical-Center | MUSC Health Black River Medical Center | Cades | SC | address 4568 Hemingway Hwy, Cades
MUSC-Health-Lancaster-Medical-Center | MUSC Health Lancaster Medical Center | Lancaster | SC | cms 420036
MUSC-Health-Chester-Medical-Center | MUSC Health Chester Medical Center | Chester | SC | cms 420019
DNU-MUSC-Health-Chester-Medical-Center | MUSC Health Chester Medical Center | Chester | SC | cms 420019
MUSC-Health-Kershaw-Medical-Center | MUSC Health Kershaw Medical Center | Camden | SC | cms 420048
MUSC-Health-Columbia-Medical-Center-Downtown | MUSC Health Columbia Medical Center Downtown | Columbia | SC | address 2435 Forest Dr, Columbia
MUSC-HEALTH-EAST-COOPER | MUSC Health East Cooper Medical Center | Mount Pleasant | SC | address 2000 Hospital Dr, Mount Pleasant
MUSC-Health-Bamberg-Barnwell-Emergency--Imaging | MUSC Health Bamberg-Barnwell Emergency and Imaging | Denmark | SC | address 4888 Hwy 70, Denmark
MUSC-Health-Bluffton-Medical-Pavilion | MUSC Health Bluffton Medical Pavilion | Bluffton | SC | site
""")

# ── Endeavor Health (nshs.wd1, ns-eeh): "<code>-<site>-<town>". The
# hospital entity codes open every site of that hospital's campus (EVH
# Evanston, NCH Northwest Community, EDH Edward, EMH Elmhurst, GBH
# Glenbrook, SWH Swedish, HPH Highland Park, SKH Skokie, LOH Linden Oaks;
# review 2026-10-29: EDH and EMH were missing from the first pass); the
# outpatient and office codes (NSO, NCO, EDO, EMO, SWO, LOO, SRO, ENDVR) keep
# the system label and take the town the site names (town_in_site), never
# the Evanston default.
_t("Endeavor Health", """
EVH-* | Endeavor Health Evanston Hospital | Evanston | IL | cms 140010
NCH-800-W-Central-Rd-Arlington-Heights | Endeavor Health Northwest Community Hospital | Arlington Heights | IL | address 800 W Central Rd, Arlington Heights
EDH-* | Endeavor Health Edward Hospital | Naperville | IL | cms 140231
EMH-* | Endeavor Health Elmhurst Hospital | Elmhurst | IL | cms 140200
GBH-* | Endeavor Health Glenbrook Hospital | Glenview | IL | address 2100 Pfingsten Rd, Glenview
SWH-* | Endeavor Health Swedish Hospital | Chicago | IL | cms 140114
HPH-* | Endeavor Health Highland Park Hospital | Highland Park | IL | address 777 Park Ave W, Highland Park
SKH-* | Endeavor Health Skokie Hospital | Skokie | IL | address 9600 Gross Point Rd, Skokie
LOH-* | Endeavor Health Linden Oaks Hospital | Naperville | IL | cms 144035
""")
TOWN_SITE["Endeavor Health"] = ("IL", "any")

# ── Mary Washington Healthcare (FORCE_LOCATION_OVERRIDE put every row in
# Fredericksburg): the town sites keep the system label and their own town.
_t("Mary Washington Healthcare", """
Stafford | - | Stafford | VA | site
King-George | - | King George | VA | site
King-George-Journal-Parkway | - | King George | VA | site
MWHC-Urgent-Care---King-George-Safety-Incident | - | King George | VA | site
""")

# ── Sutter Health (sutterhealth): every site is a bare town ("San-Francisco",
# "Fremont"); a "2 Locations" posting took the Sacramento default although
# its path names the primary site's town. The town sites keep the system
# label (several towns hold more than one Sutter hospital).
TOWN_SITE["Sutter Health"] = ("CA", "lead")

# ── Banner Health (bannerhealth.wd108): "<site> <street>"; the town and
# state ride in bulletFields and were already right. The hospitals and
# named centers below take their names.
_t("Banner Health", """
BUMC-Phoenix-1111-E-McDowell-Rd | Banner - University Medical Center Phoenix | Phoenix | AZ | cms 030002
BUMC-Tucson-1625-N-Campbell-Ave | Banner - University Medical Center Tucson | Tucson | AZ | cms 030064
BUMC-South-Tucson-2800-E-Ajo-Way | Banner - University Medical Center South | Tucson | AZ | address 2800 E Ajo Way, Tucson
Banner-Desert-Med-Ctr-1400-S-Dobson-Rd | Banner Desert Medical Center | Mesa | AZ | cms 030065
Banner-Childrens-at-Desert-1400-S-Dobson-Rd | Banner Children's at Desert | Mesa | AZ | address 1400 S Dobson Rd, Mesa
Wyoming-Medical-Center-Central-1233-E-Second-St | Banner Wyoming Medical Center | Casper | WY | cms 530012
Banner-Estrella-Med-Ctr-9201-W-Thomas-Rd | Banner Estrella Medical Center | Phoenix | AZ | cms 030115
Banner-North-Colorado-Medical-Center-1801-16th-St | Banner North Colorado Medical Center | Greeley | CO | cms 060001
Banner-Thunderbird-Med-Ctr-5555-W-Thunderbird-Rd | Banner Thunderbird Medical Center | Glendale | AZ | cms 030089
Banner-Boswell-Med-Ctr-10401-W-Thunderbird-Blvd | Banner Boswell Medical Center | Sun City | AZ | cms 030061
Banner-Del-Webb-Med-Ctr-14502-W-Meeker-Blvd | Banner Del E. Webb Medical Center | Sun City West | AZ | cms 030093
Banner-Baywood-Med-Ctr-6644-E-Baywood-Ave | Banner Baywood Medical Center | Mesa | AZ | cms 030088
Banner-Gateway-Med-Ctr-1900-N-Higley-Rd | Banner Gateway Medical Center | Gilbert | AZ | cms 030122
Banner-Heart-Hosp-6750-E-Baywood-Ave | Banner Heart Hospital | Mesa | AZ | address 6750 E Baywood Ave, Mesa
Banner-Ocotillo-Med-Ctr-1405-S-Alma-School-Rd | Banner Ocotillo Medical Center | Chandler | AZ | cms 030147
Banner-Ironwood-Med-Ctr-37000-N-Gantzel-Rd | Banner Ironwood Medical Center | Queen Creek | AZ | cms 030130
Banner-Goldfield-Med-Ctr-2050-W-Southern-Ave | Banner Goldfield Medical Center | Apache Junction | AZ | cms 030134
Banner-Casa-Grande-Med-Ctr-1800-E-Florence-Blvd | Banner Casa Grande Medical Center | Casa Grande | AZ | cms 030016
Banner-Payson-Med-Ctr-807-S-Ponderosa-St | Banner Payson Medical Center | Payson | AZ | cms 031318
Page-Hosp-501-N-Navajo-Drive | Page Hospital | Page | AZ | cms 031304
Behav-Health-Hosp-7575-E-Earll-Dr | Banner Behavioral Health Hospital | Scottsdale | AZ | address 7575 E Earll Dr, Scottsdale
Banner-MD-Anderson-Cancer-Center-2946-E-Gateway-Dr | Banner MD Anderson Cancer Center | Gilbert | AZ | address 2946 E Banner Gateway Dr, Gilbert
Banner-MD-Anderson-Cancer-Center-at-Boswell-13041-N-Del-Webb-Blvd | Banner MD Anderson Cancer Center at Boswell | Sun City | AZ | address 13041 N Del Webb Blvd, Sun City
Banner-Fort-Collins-Med-Ctr-4700-Lady-Moon-Dr | Banner Fort Collins Medical Center | Fort Collins | CO | cms 060126
East-Morgan-County-Hosp-2400-W-Edison | East Morgan County Hospital | Brush | CO | cms 061303
Sterling-Regional-Med-Ctr-615-Fairhurst-St | Sterling Regional MedCenter | Sterling | CO | cms 060076
Torrington-Community-Hosp-2000-Campbell-Dr | Torrington Community Hospital | Torrington | WY | address 2000 Campbell Dr, Torrington
Platte-County-Memorial-Hosp-201-14th-St | Platte County Memorial Hospital | Wheatland | WY | cms 531305
Washakie-Medical-Center-400-S-15th-St | Washakie Medical Center | Worland | WY | cms 531306
Banner-Churchill-Comm-Hosp-801-E-Williams-Ave | Banner Churchill Community Hospital | Fallon | NV | cms 291313
Banner-Lassen-Med-Ctr-1800-Spring-Ridge-Dr | Banner Lassen Medical Center | Susanville | CA | cms 051320
Ogallala-Community-Hosp-2601-N-Spruce-St | Ogallala Community Hospital | Ogallala | NE | cms 281355
""")

# ── Allina Health (allina.wd5): the site is the facility; clinics name their
# town ("Allina Health Coon Rapids Clinic"), which beats the Minneapolis
# default (town_in_site, Minnesota only).
_t("Allina Health", """
Abbott-Northwestern-Hospital | Abbott Northwestern Hospital | Minneapolis | MN | cms 240057
Abbott-Northwestern---WestHealth | Abbott Northwestern - WestHealth | Plymouth | MN | address 2855 Campus Dr, Plymouth
United-Hospital | United Hospital | Saint Paul | MN | cms 240038
United-Hospital---Hastings-Regina-Campus | United Hospital - Hastings Regina Campus | Hastings | MN | address 1175 Nininger Rd, Hastings
Mercy-Hospital | Mercy Hospital | Coon Rapids | MN | cms 240115
Mercy-Hospital-Unity-Campus | Mercy Hospital - Unity Campus | Fridley | MN | address 550 Osborne Rd NE, Fridley
New-Ulm-Medical-Center | New Ulm Medical Center | New Ulm | MN | cms 241378
St-Francis-Regional-Medical-Center | St. Francis Regional Medical Center | Shakopee | MN | cms 240104
Buffalo-Hospital | Buffalo Hospital | Buffalo | MN | cms 240076
Cambridge-Medical-Center | Cambridge Medical Center | Cambridge | MN | cms 240020
AH-Faribault-Medical-Center | Allina Health Faribault Medical Center | Faribault | MN | cms 240071
Owatonna-Hospital | Owatonna Hospital | Owatonna | MN | cms 240069
River-Falls-Area-Hospital | River Falls Area Hospital | River Falls | WI | cms 521349
Courage-Kenny-Rehabilitation-Institute-Golden-Valley | Courage Kenny Rehabilitation Institute | Golden Valley | MN | site
""")
TOWN_SITE["Allina Health"] = ("MN", "any")

# ── Trinity Health (trinityhealth.wd1, Jobs): "<site> - <town> <state>"; the
# towns were mostly right (the site names them), the hospital was not. The
# hospitals and rehabilitation hospitals below; clinics, home care, PACE,
# senior living and the medical groups keep the label WD_FACILITY_MAP gives
# (the MercyOne central Iowa clinics keep "MercyOne Des Moines Medical
# Center" as on 2026-10-20). MOQ (Quad Cities) and MNEIA (northeast Iowa)
# sites took Bettendorf and Waterloo whatever town they name, under a region
# label ("MercyOne Quad Cities") or the Waterloo hospital's name; their
# hospitals are named here, and the clinics that name a town keep the system
# label with that town (the owner's rule 2: a clinic is not a hospital).
_t("Trinity Health", """
SJHSYR-MAINCAMPUS | St. Joseph's Health Hospital | Syracuse | NY | cms 330140; address 301 Prospect Ave, Syracuse
St-Peters-Hospital---Albany-New-York | St. Peter's Hospital | Albany | NY | cms 330057
Albany-Memorial-Hospital---Albany-New-York | Albany Memorial Hospital | Albany | NY | address 600 Northern Blvd, Albany
Samaritan-Hospital---Troy-New-York | Samaritan Hospital | Troy | NY | cms 330180
SPNY---Samaritan-Hospital | Samaritan Hospital | Troy | NY | cms 330180
St-Marys-Hospital---Troy-New-York | Samaritan Hospital - St. Mary's Campus | Troy | NY | address 1300 Massachusetts Ave, Troy
Sunnyview-Rehabilitation-Hospital---Schenectady-New-York | Sunnyview Rehabilitation Hospital | Schenectady | NY | cms 330406
SPNY---Sunnyview-Hospital | Sunnyview Rehabilitation Hospital | Schenectady | NY | cms 330406
Saint-Alphonsus-Regional-Medical-Center---Boise-Idaho | Saint Alphonsus Regional Medical Center | Boise | ID | cms 130007
Saint-Alphonsus-Medical-Center---Nampa-Idaho | Saint Alphonsus Medical Center - Nampa | Nampa | ID | cms 130013
Saint-Alphonsus-Medical-Center---Ontario-Oregon | Saint Alphonsus Medical Center - Ontario | Ontario | OR | cms 380052
Saint-Alphonsus-Medical-Center---Baker-City-Oregon | Saint Alphonsus Medical Center - Baker City | Baker City | OR | cms 381315
Saint-Francis-Hospital---Hartford-CT | Saint Francis Hospital | Hartford | CT | cms 070002
NESF---Saint-Francis-Hospital | Saint Francis Hospital | Hartford | CT | cms 070002
Mt-Sinai-Rehab-Hospital---Hartford-CT | Mount Sinai Rehabilitation Hospital | Hartford | CT | address 490 Blue Hills Ave, Hartford
Saint-Marys-Hospital-Waterbury-CT | Saint Mary's Hospital | Waterbury | CT | cms 070016
Johnson-Memorial-Medical-Center---Stafford-Springs-CT | Johnson Memorial Hospital | Stafford Springs | CT | cms 070008
NESF---95-Woodland-Street | - | Hartford | CT | address 95 Woodland St, Hartford
Saint-Francis-Healthcare-Service---Wilmington-Delaware | Saint Francis Healthcare | Wilmington | DE | address 701 N Clayton St, Wilmington
Saint-Joseph-Mercy-Health-System-Hospital-Campus---Ann-Arbor-Mi | Trinity Health Ann Arbor Hospital | Ann Arbor | MI | cms 230156
Saint-Joseph-Mercy-Health-System-Hospital-Campus---Pontiac-Mi | Trinity Health Oakland Hospital | Pontiac | MI | cms 230029
Saint-Joseph-Mercy-Health-System-Hospital-Campus---Chelsea-Mi | Trinity Health Chelsea Hospital | Chelsea | MI | cms 230259
Saint-Joseph-Mercy-Health-System-Hospital-Campus---Livonia-Mi | Trinity Health Livonia Hospital | Livonia | MI | cms 230002
Saint-Joseph-Mercy-Health-System-Hospital-Campus---Howell-Mi | Trinity Health Livingston Hospital | Howell | MI | cms 230069
Mercy-Health-West-Michigan-Hospital-Campus---Grand-Rapids-Mi | Trinity Health Grand Rapids Hospital | Grand Rapids | MI | cms 230059
Mercy-Health-West-Michigan-Hospital-Campus---Muskegon-Mi | Trinity Health Muskegon Hospital | Muskegon | MI | cms 230066
Mercy-Health-West-Michigan-Hospital-Campus---Shelby-Mi | Trinity Health Shelby Hospital | Shelby | MI | address 72 S State St, Shelby
THWMI---Trinity-Health-Grand-Haven-Hospital | Trinity Health Grand Haven Hospital | Grand Haven | MI | cms 230174
Mishawaka-Medical-Center---Mishawaka-Indiana | Saint Joseph Health System Mishawaka Medical Center | Mishawaka | IN | cms 150012
Medical-Offices-Plymouth-Medical-Center | - | Plymouth | IN | cms 150076, the Plymouth Medical Center campus
Loyola-Medicine---Loyola-University-Medical-Center | Loyola University Medical Center | Maywood | IL | cms 140276
Loyola-Medicine---MacNeal-Hospital | MacNeal Hospital | Berwyn | IL | cms 140054
LUHS-MacNeal-Hospital | MacNeal Hospital | Berwyn | IL | cms 140054
Loyola-Medicine---Gottlieb-Memorial-Hospital | Gottlieb Memorial Hospital | Melrose Park | IL | cms 140008
Mount-Carmel-East | Mount Carmel East | Columbus | OH | cms 360035
Mount-Carmel-St-Anns | Mount Carmel St. Ann's | Westerville | OH | cms 360012
Mount-Carmel-Grove-City | Mount Carmel Grove City | Grove City | OH | address 5300 N Meadows Dr, Grove City
Mount-Carmel-New-Albany | Mount Carmel New Albany Surgical Hospital | New Albany | OH | cms 360266
Diley-Ridge-Medical-Center | Diley Ridge Medical Center | Canal Winchester | OH | cms 360358
Holy-Cross-Hospital---Ft-Lauderdale-Florida | Holy Cross Hospital | Ft. Lauderdale | FL | cms 100073
Saint-Agnes-Medical-Center---Fresno-California | Saint Agnes Medical Center | Fresno | CA | cms 050093
Mercy-Catholic-Medical-Center---Mercy-Fitzgerald-Campus | Mercy Fitzgerald Hospital | Darby | PA | cms 390156
Nazareth-Hospital | Nazareth Hospital | Philadelphia | PA | cms 390204
Pittsburgh-Mercy---South-9th-Street-Campus | - | Pittsburgh | PA | site
St-Marys-Sacred-Heart-Hospital | St. Mary's Sacred Heart Hospital | Lavonia | GA | address 367 Clear Creek Pkwy, Lavonia
St-Marys-Good-Samaritan-Hospital | St. Mary's Good Samaritan Hospital | Greensboro | GA | cms 111329
MMCIA---MercyOne-Des-Moines-Medical-Center-Central-IA | MercyOne Des Moines Medical Center | Des Moines | IA | cms 160083
MMCIA---MercyOne-West-Des-Moines-Medical-Center-Central-IA | MercyOne West Des Moines Medical Center | West Des Moines | IA | address 1755 59th Pl, West Des Moines
MMCIA---MercyOne-Newton-Medical-Center-Central-IA | MercyOne Newton Medical Center | Newton | IA | cms 160032
MMCIA---MercyOne-Centerville-Medical-Center-Central-IA | MercyOne Centerville Medical Center | Centerville | IA | cms 161377
MMCIA---MercyOne-Elkader-Central-Community-Hospital | MercyOne Elkader Medical Center | Elkader | IA | cms 161319
MMCIA---MercyOne-Dallas-County-Hospital-Central-IA | Dallas County Hospital | Perry | IA | cms 161322
MercyOne-North-Iowa-Medical-Center---East-Campus-Mason-City-Iowa | MercyOne North Iowa Medical Center | Mason City | IA | cms 160064
MercyOne-North-Iowa-Medical-Center-West-Campus---Mason-City-Iowa | MercyOne North Iowa Medical Center | Mason City | IA | cms 160064
MercyOne-New-Hampton-Medical-Center---New-Hampton-Iowa | MercyOne New Hampton Medical Center | New Hampton | IA | address 308 N Maple Ave, New Hampton
Mercy-Medical-Center---Dubuque-Iowa | MercyOne Dubuque Medical Center | Dubuque | IA | cms 160069
Mercy-Medical-Center---Dyersville-Iowa | MercyOne Dyersville Medical Center | Dyersville | IA | address 1111 3rd St SW, Dyersville
Mercy-Medical-Center---Clinton-Iowa | MercyOne Clinton Medical Center | Clinton | IA | cms 160080
MNEIA----Waterloo-Medical-Center | MercyOne Waterloo Medical Center | Waterloo | IA | cms 160067
MNEIA----Oelwein-Medical-Center | MercyOne Oelwein Medical Center | Oelwein | IA | cms 161338
MNEIA----Oelwein | - | Oelwein | IA | site
MNEIA----Cedar-Falls-Medical-Center | MercyOne Cedar Falls Medical Center | Cedar Falls | IA | address 515 College St, Cedar Falls
MNEIA----Parkersburg | - | Parkersburg | IA | site
MNEIA----Fairbank | - | Fairbank | IA | site
MOQ---Genesis-Medical-Center-Silvis | Genesis Medical Center Silvis | Silvis | IL | address 801 Illini Dr, Silvis
MOQ---Genesis-Medical-Center-Aledo | Genesis Medical Center Aledo | Aledo | IL | cms 141304
MOQ---Genesis-Medical-Center-DeWitt | Genesis Medical Center DeWitt | DeWitt | IA | cms 161313
MOQ---GMC-West-Central-Park-Campus | Genesis Medical Center Davenport | Davenport | IA | cms 160033; address 1401 W Central Park Ave, Davenport
MOQ---GHG-Silvis | - | Silvis | IL | site
MOQ---GHG-Peds-Silvis | - | Silvis | IL | site
MOQ---HealthPlex---Moline | - | Moline | IL | site
MOQ---OB-Moline | - | Moline | IL | site
MOQ---GHG-Moline-Endocrinology | - | Moline | IL | site
MOQ---GHG-Davenport | - | Davenport | IA | site
MOQ---GHG-Davenport-Clinic | - | Davenport | IA | site
MOQ---Davenport-HealthPlex | - | Davenport | IA | site
MOQ---Psych-Associates-Davenport | - | Davenport | IA | site
MOQ---1828-E-Locust-St-Davenport | - | Davenport | IA | site
MOQ---GHG-Eldridge | - | Eldridge | IA | site
MOQ---GHG-DeWitt | - | DeWitt | IA | site
MOQ---VNA-Rock-Island | - | Rock Island | IL | site
""")

# ── Sentara Healthcare (sentara.wd1, scs): town sites ("Norfolk-VA") keep
# the system label and their town; a site that names a hospital sat at the
# Norfolk default whatever town the hospital is in.
_t("Sentara Healthcare", """
Sentara-Norfolk-General-Hospital | Sentara Norfolk General Hospital | Norfolk | VA | cms 490007
Sentara-Leigh-Hospital | Sentara Leigh Hospital | Norfolk | VA | cms 490046
Sentara-Virginia-Beach-General-Hospital | Sentara Virginia Beach General Hospital | Virginia Beach | VA | cms 490057
Sentara-Princess-Anne-Hospital | Sentara Princess Anne Hospital | Virginia Beach | VA | cms 490119
Sentara-Williamsburg-Regional-Medical-Center | Sentara Williamsburg Regional Medical Center | Williamsburg | VA | cms 490066
Sentara-Northern-Virginia-Medical-Center | Sentara Northern Virginia Medical Center | Woodbridge | VA | cms 490113
Sentara-Louise-Obici-Hospital | Sentara Obici Hospital | Suffolk | VA | cms 490044
Sentara-RMH-Medical-Center | Sentara RMH Medical Center | Harrisonburg | VA | cms 490004
Sentara-Careplex-Hospital | Sentara CarePlex Hospital | Hampton | VA | cms 490093
Sentara-Martha-Jefferson-Hospital | Sentara Martha Jefferson Hospital | Charlottesville | VA | cms 490077
Sentara-Halifax-Regional-Hospital | Sentara Halifax Regional Hospital | South Boston | VA | cms 490013
Sentara-Albemarle-Medical-Center-Halstead-Blvd | Sentara Albemarle Medical Center | Elizabeth City | NC | cms 340109
ZZZ_Sentara-Albemarle-Regional-Medical-Center | Sentara Albemarle Medical Center | Elizabeth City | NC | cms 340109
""")

# ── Memorial Healthcare System (memorialhealthcare.wd1, mhs_careers): the
# site is the facility; every row sat at the Hollywood default.
_t("Memorial Healthcare System", """
Memorial-Regional-Hospital | Memorial Regional Hospital | Hollywood | FL | address 3501 Johnson St, Hollywood
MRH---Memorial-Regional-Hospital | Memorial Regional Hospital | Hollywood | FL | address 3501 Johnson St, Hollywood
Memorial-Regional-Hospital-South | Memorial Regional Hospital South | Hollywood | FL | address 3600 Washington St, Hollywood
Joe-DiMaggio-Childrens-Hospital | Joe DiMaggio Children's Hospital | Hollywood | FL | address 1005 Joe DiMaggio Dr, Hollywood
Memorial-Hospital-West | Memorial Hospital West | Pembroke Pines | FL | cms 100281
Memorial-Hospital-West-Medical-Office-Building | - | Pembroke Pines | FL | address 703 N Flamingo Rd, Pembroke Pines (the Memorial Hospital West campus)
Memorial-Hospital-West-Medical-Office-Building-II | - | Pembroke Pines | FL | address 703 N Flamingo Rd, Pembroke Pines (the Memorial Hospital West campus)
Memorial-Hospital-Pembroke | Memorial Hospital Pembroke | Pembroke Pines | FL | cms 100230
Memorial-Hospital-Miramar | Memorial Hospital Miramar | Miramar | FL | cms 100285
""")

# ── UofL Health (uoflhealth): town sites ("Louisville-Kentucky") name no
# hospital (Louisville holds several); the hospital entity sites below.
_t("UofL Health", """
UofL-Hospital | UofL Health - UofL Hospital | Louisville | KY | cms 180141
UofL-Hospital---ULP---AMG | UofL Health - UofL Hospital | Louisville | KY | cms 180141
JHHS---Jewish-Hospital---ULP---AMG | UofL Health - Jewish Hospital | Louisville | KY | address 200 Abraham Flexner Way, Louisville
JHHS---Jewish-Hospital---ULP---CMG | UofL Health - Jewish Hospital | Louisville | KY | address 200 Abraham Flexner Way, Louisville
Peace---Louisville | UofL Health - Peace Hospital | Louisville | KY | address 2020 Newburg Rd, Louisville
UofL-Health---Brown-Cancer-Center---UMC | UofL Health - Brown Cancer Center | Louisville | KY | address 529 S Jackson St, Louisville
""")

# ── Mass General Brigham (facility_names reads the body's "Site:" line);
# town sites keep the label; three Longwood and Fruit Street buildings are
# their hospital's campus.
_t("Mass General Brigham", """
75-Francis-Street-Boston--Amory-Lab | Brigham and Women's Hospital | Boston | MA | cms 220110; address 75 Francis St, Boston
20-Shattuck-Street--Boston-Thorn-Research-Building | Brigham and Women's Hospital | Boston | MA | cms 220110; address 20 Shattuck St, Boston (the Brigham campus)
32-Fruit-Street-Boston-Yawkey-Center | Massachusetts General Hospital | Boston | MA | cms 220071; address 32 Fruit St, Boston (the MGH campus)
""")

# ── Summit BHC (summitbhc): every site is one treatment center; the town is
# the one the stored rows already carry (WD_FACILITY_MAP).
_t("Summit BHC", """
The-Pavilion-at-Williamsburg-Place | Pavilion at Williamsburg Place | Williamsburg | VA | cms 494032
The-Farley-Center | Farley Center | Williamsburg | VA | rows
Miramont-Behavioral-Health | Miramont Behavioral Health | Middleton | WI | cms 524042
Highland-Hospital | Highland Hospital | Charleston | WV | cms 514001
Peak-Behavioral-Health | Peak Behavioral Health | Santa Teresa | NM | rows
Eagle-View-Behavioral-Health | Eagle View Behavioral Health | Bettendorf | IA | cms 164006
Anabranch-Recovery-Center | Anabranch Recovery Center | Terre Haute | IN | rows
Willow-Creek-Behavioral-Health | Willow Creek Behavioral Health | Green Bay | WI | rows
Las-Cruces-Recovery-Center | Las Cruces Recovery Center | Las Cruces | NM | rows
Mountain-Laurel-Recovery-Center | Mountain Laurel Recovery Center | Westfield | PA | rows
Raleigh-Oaks-Behavioral-Health | Raleigh Oaks Behavioral Health | Garner | NC | cms 344035
Summit-Healthcare-Mgmt | - | Franklin | TN | rows (the management office)
Willingway | Willingway | Statesboro | GA | rows
St-Joseph-Institute-for-Addiction | St. Joseph Institute for Addiction | Port Matilda | PA | rows
Creekside-Behavioral-Health | Creekside Behavioral Health | Kingsport | TN | cms 444027
Peak-View-Behavioral-Health | Peak View Behavioral Health | Colorado Springs | CO | cms 064026
Carolina-Dunes-Behavioral-Health | Carolina Dunes Behavioral Health | Leland | NC | rows
Palms-Behavioral-Health | Palms Behavioral Health | Harlingen | TX | cms 454141
Safe-Harbor-Recovery-Center | Safe Harbor Recovery Center | Portsmouth | VA | rows
Wooded-Glen-Recovery-Center | Wooded Glen Recovery Center | Henryville | IN | rows
Twin-Lakes-Recovery-Center | Twin Lakes Recovery Center | Monroe | GA | rows
Great-Oaks-Recovery-Center | Great Oaks Recovery Center | Egypt | TX | rows
Stone-River-Recovery-Center | Stone River Recovery Center | San Antonio | TX | rows
Ivory-Plains-Recovery-Center | Ivory Plains Recovery Center | Adair | IA | rows
Raleigh-Recovery-Center | Raleigh Recovery Center | Raleigh | NC | rows
The-Ranch-at-Dove-Tree | Ranch at Dove Tree | Lubbock | TX | rows
St-Gregory-Recovery-Center | St. Gregory Recovery Center | Bayard | IA | rows
English-Mountain-Recovery | English Mountain Recovery | Sevierville | TN | rows
Bel-Aire-Recovery-Center | Bel Aire Recovery Center | Bel Aire | KS | rows
Waypoint-Recovery-Center | Waypoint Recovery Center | Cameron | SC | rows
Canyon-Vista-Recovery-Center | Canyon Vista Recovery Center | Mesa | AZ | rows
Fair-Oaks-Recovery-Center | Fair Oaks Recovery Center | Sacramento | CA | rows
The-Aviary-Recovery-Center | Aviary Recovery Center | Eolia | MO | rows
Northern-Path-Recovery-Center | Northern Path Recovery Center | Fort Wayne | IN | rows
Cottonwood-Tucson | Cottonwood Tucson | Tucson | AZ | rows
Iron-Bridge-Recovery-Center | Iron Bridge Recovery Center | Chester | VA | rows
""")

# ── Wellstar Health (wellstar.wd1): the site is the facility; towns were
# right (WD_FACILITY_MAP), names were not.
_t("Wellstar Health", """
Kennestone-Hospital | Wellstar Kennestone Regional Medical Center | Marietta | GA | cms 110035
Windy-Hill-Hospital | Wellstar Windy Hill Hospital | Marietta | GA | address 2540 Windy Hill Rd, Marietta
Cobb-Hospital | Wellstar Cobb Medical Center | Austell | GA | cms 110143
Douglas-Hospital | Wellstar Douglas Medical Center | Douglasville | GA | cms 110184
Paulding-Hospital | Wellstar Paulding Medical Center | Hiram | GA | cms 110042
North-Fulton-Hospital | Wellstar North Fulton Medical Center | Roswell | GA | cms 110198
Spalding-Medical-Center | Wellstar Spalding Medical Center | Griffin | GA | cms 110031
Sylvan-Grove-Hospital | Wellstar Sylvan Grove Medical Center | Jackson | GA | cms 111319
Wellstar-West-Georgia-Medical-Center | Wellstar West Georgia Medical Center | LaGrange | GA | cms 110016
Wellstar-MCG-Health | Wellstar MCG Health Medical Center | Augusta | GA | address 1120 15th St, Augusta
AU-Medical-Center-Inc | Wellstar MCG Health Medical Center | Augusta | GA | address 1120 15th St, Augusta
Columbia-County-Medical-Center-Hospital | Wellstar Columbia County Medical Center | Grovetown | GA | rows
Wellstar-MCG-Health-Warm-Springs-Inc | Warm Springs Medical Center | Warm Springs | GA | cms 111316
Mountain-Lakes-Medical-Center | Mountain Lakes Medical Center | Clayton | GA | cms 111336
East-Cobb-Health-Park | Wellstar East Cobb Health Park | Marietta | GA | rows
Vinings-Health-Park | Wellstar Vinings Health Park | Smyrna | GA | rows
""", also="Wellstar Health (Providers)")

# ── Memorial Hermann (memorialhermann.wd5, External): the site is the
# facility; FORCE_LOCATION_OVERRIDE put every row in Houston. The hospitals
# take their own town (placed() keeps it); the medical plazas named for a
# suburban campus keep the system label and that campus's town.
_t("Memorial Hermann", """
TMCChildrens-Hospital | Children's Memorial Hermann Hospital | Houston | TX | cms 450068; address 6411 Fannin St, Houston
Memorial-City-Hospital | Memorial Hermann Memorial City Medical Center | Houston | TX | cms 450610
Greater-Heights-Hospital | Memorial Hermann Greater Heights Hospital | Houston | TX | address 1635 N Loop W, Houston
Southeast-Hospital | Memorial Hermann Southeast Hospital | Houston | TX | address 11800 Astoria Blvd, Houston
Southwest-Hospital | Memorial Hermann Southwest Hospital | Houston | TX | address 7600 Beechnut St, Houston
TIRR | TIRR Memorial Hermann | Houston | TX | address 1333 Moursund St, Houston
Northeast-Hospital | Memorial Hermann Northeast Hospital | Humble | TX | cms 450684
Northeast-Medical-Office-Building | - | Humble | TX | address 18951 N Memorial Dr, Humble (the Northeast campus)
Katy-Hospital | Memorial Hermann Katy Hospital | Katy | TX | cms 450847
Katy-Rehab-Hospital | Memorial Hermann Katy Rehabilitation Hospital | Katy | TX | address 21720 Kingsland Blvd, Katy
Katy-Medical-Plaza-2 | - | Katy | TX | site
Sugar-Land-Hospital | Memorial Hermann Sugar Land Hospital | Sugar Land | TX | cms 450848
Sugar-Land-Medical-Plaza-1 | - | Sugar Land | TX | site
Sugar-Land-Medical-Plaza-2 | - | Sugar Land | TX | site
The-Woodlands-Hospital | Memorial Hermann The Woodlands Medical Center | The Woodlands | TX | address 9250 Pinecroft Dr, The Woodlands
The-Woodlands-Medical-Plaza-3 | - | The Woodlands | TX | site
The-Woodlands-Medical-Plaza-4 | - | The Woodlands | TX | site
Cypress-Hospital | Memorial Hermann Cypress Hospital | Cypress | TX | address 27800 Northwest Fwy, Cypress
Cypress-Medical-Plaza-1 | - | Cypress | TX | site
Pearland-Hospital | Memorial Hermann Pearland Hospital | Pearland | TX | address 16100 South Fwy, Pearland
Alvin-Free-Standing-Emergency-Room | - | Alvin | TX | site
MH-Rockets-Orthopedic-Hospital-MHROH | Memorial Hermann Rockets Orthopedic Hospital | Bellaire | TX | address 5410 W Loop S, Bellaire
""")

# ── Intermountain Health (imh.wd108): WD_FACILITY_MAP names most hospitals;
# the sites the board renamed ("St Marys Regional Hospital", "Intermountain
# Health Lutheran Hospital", "... St Vincent Regional Hospital") fell through
# to the label. Names already on the rows are kept.
_t("Intermountain Health (IMH)", """
St-Marys-Regional-Hospital | St. Mary's Regional Hospital | Grand Junction | CO | address 2635 N 7th St, Grand Junction
Intermountain-Health-St-Vincent-Regional-Hospital | St. Vincent Regional Hospital | Billings | MT | cms 270049
Intermountain-Health-Lutheran-Hospital | Lutheran Medical Center | Wheat Ridge | CO | cms 060009
Good-Samaritan-Hospital | Good Samaritan Medical Center | Lafayette | CO | cms 060116
Intermountain-Health-St-George-Regional-Hospital | St. George Regional Hospital | St George | UT | address 1380 E Medical Center Dr, St. George (the town as the rows and the town list write it)
Platte-Valley-Hospital | Platte Valley Hospital | Brighton | CO | cms 060004
St-James-Hospital | St. James Hospital | Butte | MT | cms 270017
Holy-Rosary-Hospital | Holy Rosary Hospital | Miles City | MT | cms 271347
Saint-Joseph-Hospital | Saint Joseph Hospital | Denver | CO | cms 060028
Intermountain-Health-Utah-Valley-Hospital | Utah Valley Hospital | Provo | UT | cms 460001
Intermountain-Health-Park-City-Hospital | Park City Hospital | Park City | UT | cms 460057
Budge-Clinic | Intermountain Health Budge Clinic | Logan | UT | rows
Badura-Clinic | Intermountain Health Badura Clinic | Las Vegas | NV | rows
Mile-High-Clinic | Intermountain Health Mile High Clinic | Denver | CO | rows
Superior-Clinic | - | Superior | CO | site
""", also="Intermountain Healthcare")

# ── Cape Fear Valley Health (capefearvalley): the site is the entity code;
# FORCE_LOCATION_OVERRIDE put every row in Fayetteville.
_t("Cape Fear Valley Health", """
CFV | Cape Fear Valley Medical Center | Fayetteville | NC | cms 340028
HRSH | Highsmith-Rainey Specialty Hospital | Fayetteville | NC | address 150 Robeson St, Fayetteville
CHH | Cape Fear Valley Central Harnett Hospital | Lillington | NC | address 215 Brightwater Dr, Lillington
BJH | Cape Fear Valley Betsy Johnson Hospital | Dunn | NC | cms 340071
Hoke | Cape Fear Valley Hoke Hospital | Raeford | NC | cms 340188
BCH | Cape Fear Valley Bladen County Hospital | Elizabethtown | NC | address 501 S Poplar St, Elizabethtown
""")

# ── West Tennessee Healthcare (wth): "<town> Hospital" sites sat at the
# Jackson default (or carried the site as the town).
_t("West Tennessee Healthcare", """
Jackson-General | Jackson-Madison County General Hospital | Jackson | TN | cms 440002
North-Jackson-Hospital | West Tennessee Healthcare North Hospital | Jackson | TN | address 367 Hospital Blvd, Jackson
Dyersburg-Hospital | West Tennessee Healthcare Dyersburg Hospital | Dyersburg | TN | cms 440072
Martin-Hospital | West Tennessee Healthcare Volunteer Hospital | Martin | TN | address 161 Mt Pelia Rd, Martin
Milan-Hospital | West Tennessee Healthcare Milan Hospital | Milan | TN | site
Camden-Hospital | West Tennessee Healthcare Camden Hospital | Camden | TN | site
Bolivar-Hospital | West Tennessee Healthcare Bolivar Hospital | Bolivar | TN | site
Kirkland-Cancer-Center | Kirkland Cancer Center | Jackson | TN | rows
Pathways-Main-Office | Pathways of Tennessee | Jackson | TN | cms 444010
""")

# ── Prisma Health (both tenants): town sites keep the label; two named
# hospitals.
_t("Prisma Health", """
Greenville-Memorial-Hospital | Prisma Health Greenville Memorial Hospital | Greenville | SC | cms 420078
Blount-Memorial-Hospital | Blount Memorial Hospital | Maryville | TN | cms 440011
""", also="Prisma Health (Providers)")

# ── Virtua Health (virtua.wd1): "<town> - <street>"; every row took the
# Marlton default. The hospitals below; the other sites take the town that
# leads the descriptor (town_lead), except the streets named for a place
# that is not their town.
_t("Virtua Health", """
Voorhees---100-Bowman-Drive | Virtua Voorhees Hospital | Voorhees | NJ | address 100 Bowman Dr, Voorhees
200-Bowman-Drive | - | Voorhees | NJ | address 200 Bowman Dr, Voorhees
Lourdes-Health-System---1600-Haddon-Ave | Virtua Our Lady of Lourdes Hospital | Camden | NJ | address 1600 Haddon Ave, Camden
Mount-Holly---175-Madison-Avenue | Virtua Mount Holly Hospital | Mount Holly | NJ | address 175 Madison Ave, Mount Holly
Burlington-Hospital---218-A-Sunset-Rd | Virtua Willingboro Hospital | Willingboro | NJ | address 218-A Sunset Rd, Willingboro
Marlton---Rt-73-and-Brick-Road | Virtua Marlton Hospital | Marlton | NJ | address Route 73 and Brick Rd, Marlton
Washington---239-Hurffville-CrossKeys-Road | - | Sewell | NJ | address 239 Hurffville-Cross Keys Rd, Sewell (Washington Township)
Dover---200-Banning-Street | - |  |  | not placed: a Dover in another state, not a Virtua town
""")
TOWN_SITE["Virtua Health"] = ("NJ", "lead")

# ── Beth Israel Lahey Health (bilh.wd1): WD_FACILITY_MAP names the hospitals
# from locationsText; the table also reads the path ("2 Locations"
# postings) and places Lahey's Peabody site, which the map's "lahey clinic"
# prefix put in Burlington.
_t("Beth Israel Lahey Health", """
Beth-Israel-Deaconess-Medical-Center | Beth Israel Deaconess Medical Center | Boston | MA | cms 220086
Lahey-Clinic | Lahey Hospital & Medical Center | Burlington | MA | cms 220171
Lahey-Clinic-Peabody | Lahey Medical Center, Peabody | Peabody | MA | address 1 Essex Center Dr, Peabody
Winchester-Hospital | Winchester Hospital | Winchester | MA | cms 220105
Mount-Auburn-Hospital | Mount Auburn Hospital | Cambridge | MA | cms 220002
Beth-Israel-Deaconess-Hospital-Plymouth | Beth Israel Deaconess Hospital-Plymouth | Plymouth | MA | cms 220060
Beth-Israel-Deaconess-Hospital-Needham | Beth Israel Deaconess Hospital-Needham | Needham | MA | cms 220083
Beth-Israel-Deaconess-Hospital-Milton | Beth Israel Deaconess Hospital-Milton | Milton | MA | cms 220108
Beverly-Hospital | Beverly Hospital | Beverly | MA | cms 220033
Exeter-Hospital | Exeter Hospital | Exeter | NH | cms 300023
New-England-Baptist-Hospital | New England Baptist Hospital | Boston | MA | cms 220088
Anna-Jaques-Hospital | Anna Jaques Hospital | Newburyport | MA | cms 220029
Addison-Gilbert-Hospital | Addison Gilbert Hospital | Gloucester | MA | rows
BayRidge-Hospital | BayRidge Hospital | Lynn | MA | rows
Joslin-Diabetes | Joslin Diabetes Center | Boston | MA | address 1 Joslin Pl, Boston
""")

# ── ProMedica (promedica): town sites keep the label; the hospital sites.
_t("ProMedica", """
Ebeid-Childrens-Hospital | ProMedica Russell J. Ebeid Children's Hospital | Toledo | OH | address 2142 N Cove Blvd, Toledo
Monroe-Regional-Hospital | ProMedica Monroe Regional Hospital | Monroe | MI | cms 230099
Fostoria-Community-Hospital | ProMedica Fostoria Community Hospital | Fostoria | OH | cms 361318
Hickman-Hospital | ProMedica Charles and Virginia Hickman Hospital | Adrian | MI | cms 230005
""")

# ── UMass Memorial Health (ummhc): the medical center's campuses and one
# named hospital site; town sites keep the label.
_t("UMass Memorial Health", """
Worcester-MA-University-Campus | UMass Memorial Medical Center - University Campus | Worcester | MA | cms 220163
Worcester-MA-Memorial-Campus | UMass Memorial Medical Center - Memorial Campus | Worcester | MA | address 119 Belmont St, Worcester
Worcester-MA-Hahnemann-Campus | UMass Memorial Medical Center - Hahnemann Campus | Worcester | MA | address 281 Lincoln St, Worcester
Milford-Regional-Medical-Center | Milford Regional Medical Center | Milford | MA | cms 220090
""")

# ── Nationwide Children's Hospital: its second hospital.
_t("Nationwide Children's Hospital", """
2213-Cherry-Street-Toledo-OH | Nationwide Children's Hospital Toledo | Toledo | OH | cms 363309
""")

# ── Methodist Le Bonheur (methodisthealth.wd5, MLH): the site is the
# facility; FORCE_LOCATION_OVERRIDE put every row in Memphis.
_t("Methodist Le Bonheur", """
Methodist-University-Hospital | Methodist University Hospital | Memphis | TN | address 1265 Union Ave, Memphis
Le-Bonheur-Childrens-Hospital | Le Bonheur Children's Hospital | Memphis | TN | address 848 Adams Ave, Memphis
Methodist-South-Hospital | Methodist South Hospital | Memphis | TN | address 1300 Wesley Dr, Memphis
Methodist-North-Hospital | Methodist North Hospital | Memphis | TN | address 3960 New Covington Pike, Memphis
Methodist-Le-Bonheur-Germantown-Hospital | Methodist Le Bonheur Germantown Hospital | Germantown | TN | address 7691 Poplar Ave, Germantown
Methodist-Olive-Branch-Hospital | Methodist Olive Branch Hospital | Olive Branch | MS | address 4250 Bethel Rd, Olive Branch
Le-Bonheur-Childrens-Outpatient-Center---Jackson | - | Jackson | TN | site
""")

# ── Bronson Healthcare (bronsonhg): "<town> <street> - <facility>"; every
# row took the Kalamazoo default. The hospitals below; other sites take the
# town the descriptor opens with (town_in_site, Michigan).
_t("Bronson Healthcare", """
Kalamazoo-601-John-St---Bronson-Methodist-Hospital | Bronson Methodist Hospital | Kalamazoo | MI | cms 230017
Kalamazoo-252-E-Lovell-St---Bronson-Methodist-Hospital | Bronson Methodist Hospital | Kalamazoo | MI | cms 230017
Kalamazoo-601-John-St-Bronson-Methodist-Hospital-South-Campus-South-Pavilion | Bronson Methodist Hospital | Kalamazoo | MI | cms 230017
Bronson-Methodist-Hospital | Bronson Methodist Hospital | Kalamazoo | MI | cms 230017
Battle-Creek--300-North-Ave---Bronson-Battle-Creek-Hospital | Bronson Battle Creek Hospital | Battle Creek | MI | cms 230075
Paw-Paw-408-Hazen-St---Bronson-Lakeview-Hospital | Bronson LakeView Hospital | Paw Paw | MI | cms 231332
South-Haven-955-S-Bailey-Ave---Bronson-South-Haven-Hospital | Bronson South Haven Hospital | South Haven | MI | cms 230085
""")
TOWN_SITE["Bronson Healthcare"] = ("MI", "any")

# ── BestCare Health (the Nebraska Methodist Health System tenant): "<site>
# - <town> <ST>"; towns were right.
_t("BestCare Health", """
Methodist-Hospital---Omaha-NE | Methodist Hospital | Omaha | NE | cms 280040
Methodist-Hospital | Methodist Hospital | Omaha | NE | cms 280040
Methodist-Womens-Hospital---Omaha-NE | Methodist Women's Hospital | Omaha | NE | address 707 N 190th Plaza, Omaha
Methodist-Jennie-Edmundson-Hospital---Council-Bluffs-IA | Methodist Jennie Edmundson Hospital | Council Bluffs | IA | cms 160047
Methodist-Fremont-Hospital---Fremont-NE | Methodist Fremont Health | Fremont | NE | cms 280077
""")

# ── Tufts Medicine (tuftsmedicine): the hospitals; the corporate and
# clinic sites keep the label WD_FACILITY_MAP gives.
_t("Tufts Medicine", """
Tufts-Medical-Center | Tufts Medical Center | Boston | MA | cms 220116
Lowell-General-Hospital | Lowell General Hospital | Lowell | MA | cms 220063
Lowell-General-Hospital---1-Hospital-Drive | Lowell General Hospital | Lowell | MA | cms 220063; address 1 Hospital Dr, Lowell
MelroseWakefield-Hospital | MelroseWakefield Hospital | Melrose | MA | cms 220070
Lawrence-Memorial-Hospital | Lawrence Memorial Hospital of Medford | Medford | MA | address 170 Governors Ave, Medford
""")

# ── Cleveland Clinic (ccf.wd1): the site is the facility; every Ohio row
# took the Cleveland default. Florida names as WD_FACILITY_MAP has them.
_t("Cleveland Clinic", """
Cleveland-Clinic-Main-Campus | Cleveland Clinic Main Campus | Cleveland | OH | cms 360180
Fairview-Hospital | Cleveland Clinic Fairview Hospital | Cleveland | OH | cms 360077
Lutheran-Hospital | Cleveland Clinic Lutheran Hospital | Cleveland | OH | cms 360087
Childrens-Hospital-Rehabilitation | Cleveland Clinic Children's Hospital for Rehabilitation | Cleveland | OH | cms 363304
Akron-General-Medical-Center | Cleveland Clinic Akron General | Akron | OH | cms 360027
Hillcrest-Hospital | Cleveland Clinic Hillcrest Hospital | Mayfield Heights | OH | cms 360230
Mercy-Hospital | Cleveland Clinic Mercy Hospital | Canton | OH | cms 360070
South-Pointe-Hospital | Cleveland Clinic South Pointe Hospital | Warrensville Heights | OH | cms 360144
Marymount-Hospital | Cleveland Clinic Marymount Hospital | Garfield Heights | OH | cms 360143
Medina-Hospital | Cleveland Clinic Medina Hospital | Medina | OH | cms 360091
Euclid-Hospital | Cleveland Clinic Euclid Hospital | Euclid | OH | cms 360082
Union-Hospital | Cleveland Clinic Union Hospital | Dover | OH | cms 360010
Avon-Hospital | Cleveland Clinic Avon Hospital | Avon | OH | cms 360364
Mentor-Hospital | Cleveland Clinic Mentor Hospital | Mentor | OH | site
Lodi-Community-Hospital | Lodi Community Hospital | Lodi | OH | cms 361303
The-Richard-Jacobs-Health-Center | Richard E. Jacobs Health Center | Avon | OH | address 33100 Cleveland Clinic Blvd, Avon
Twinsburg-Family-Health-Center | - | Twinsburg | OH | site
Beachwood-Family-Health-and-Surgery-Center | - | Beachwood | OH | site
Lorain-Family-Health-Surgery-Center | - | Lorain | OH | site
Independence-Family-Health-Center | - | Independence | OH | site
Willoughby-Hills-Family-Health-Center | - | Willoughby Hills | OH | site
Strongsville-Fam-Health-Surg-Center | - | Strongsville | OH | site
Solon-Family-Health-Center | - | Solon | OH | site
Chagrin-Falls-Family-Health-Center | - | Chagrin Falls | OH | site
Middleburg-Heights-Family-Health-Center | - | Middleburg Heights | OH | site
Westlake-Medical-Office-Building | - | Westlake | OH | site
Stow-Health-and-Wellness | - | Stow | OH | site
Florida-Weston-Hospital | Cleveland Clinic Hospital | Weston | FL | cms 100289
Indian-River-Hospital | Cleveland Clinic Indian River Hospital | Vero Beach | FL | cms 100105
Martin-Health-North | Cleveland Clinic Martin North Hospital | Stuart | FL | cms 100044
Martin-Hospital-South | Cleveland Clinic Martin South Hospital | Stuart | FL | address 2100 SE Salerno Rd, Stuart
Tradition-Hospital | Cleveland Clinic Tradition Hospital | Port St. Lucie | FL | address 10000 SW Innovation Way, Port St. Lucie
""")

# ── Rochester Regional Health (rrhs.wd5): the site is the facility; every
# row took the Rochester default (or carried the site as its town).
_t("Rochester Regional Health", """
Rochester-General-Hospital-RGH | Rochester General Hospital | Rochester | NY | cms 330125
Unity-Hospital | Unity Hospital | Rochester | NY | cms 330226
Clifton-Springs-Hospital | Clifton Springs Hospital & Clinic | Clifton Springs | NY | cms 330265
Clifton-Springs-Nursing-Home | - | Clifton Springs | NY | cms 330265 (the hospital's campus)
Newark-Wayne-Community-Hospital | Newark-Wayne Community Hospital | Newark | NY | cms 330030
DeMay | DeMay Living Center | Newark | NY | address Driving Park Ave, Newark
UMMC---North-Street-Campus | United Memorial Medical Center | Batavia | NY | address 127 North St, Batavia
Batavia-Medical-Campus | - | Batavia | NY | site
Geneva-Medical-Campus | - | Geneva | NY | site
Henrietta-Medical-Campus | - | Henrietta | NY | site
SLH---CPH-Main-375 | Canton-Potsdam Hospital | Potsdam | NY | cms 330197
SLH---CPH-Main-40 | Canton-Potsdam Hospital | Potsdam | NY | cms 330197
SLH---MH-Main | Massena Hospital | Massena | NY | address 1 Hospital Dr, Massena
SLH---GH-Main-40 | Gouverneur Hospital | Gouverneur | NY | cms 331315
Gouverneur-Rescue-Squad | - | Gouverneur | NY | site
Rochester-Regional-Health-Home-Care---Monroe-Ave | - | Rochester | NY | site (Monroe Ave, Rochester)
Unity-Living-Center | - | Rochester | NY | address 1565 Long Pond Rd, Rochester (the Unity campus)
Imaging-Center---Unity-POB | - | Rochester | NY | address 1561 Long Pond Rd, Rochester (the Unity campus)
Park-Ridge-Living-Center | - | Rochester | NY | address 1555 Long Pond Rd, Rochester (the Unity campus)
ElderONE---Hudson | - | Rochester | NY | site (Hudson Ave, Rochester)
""")

# ── OhioHealth (ohiohealth.wd5): the site is the facility; towns right.
_t("OhioHealth", """
RIVERSIDE-METHODIST-HOSPITAL | OhioHealth Riverside Methodist Hospital | Columbus | OH | cms 360006
HYBRID---RIVERSIDE-METHODIST-HOSPITAL | OhioHealth Riverside Methodist Hospital | Columbus | OH | cms 360006
GRANT-MEDICAL-CENTER | OhioHealth Grant Medical Center | Columbus | OH | cms 360017
DOCTORS-HOSPITAL | OhioHealth Doctors Hospital | Columbus | OH | cms 360152
DUBLIN-METHODIST-HOSPITAL | OhioHealth Dublin Methodist Hospital | Dublin | OH | cms 360348
GROVE-CITY-METHODIST-HOSPITAL | OhioHealth Grove City Methodist Hospital | Grove City | OH | rows
PICKERINGTON-METHODIST-HOSPITAL | OhioHealth Pickerington Methodist Hospital | Pickerington | OH | rows
GRADY-MEMORIAL-HOSPITAL | OhioHealth Grady Memorial Hospital | Delaware | OH | cms 360210
HYBRID---GRADY-MEMORIAL-HOSPITAL | OhioHealth Grady Memorial Hospital | Delaware | OH | cms 360210
MANSFIELD-HOSPITAL | OhioHealth Mansfield Hospital | Mansfield | OH | cms 360118
HYBRID---MANSFIELD-HOSPITAL | OhioHealth Mansfield Hospital | Mansfield | OH | cms 360118
SHELBY-HOSPITAL | OhioHealth Shelby Hospital | Shelby | OH | cms 361324
MARION-GENERAL-HOSPITAL | OhioHealth Marion General Hospital | Marion | OH | cms 360011
SOUTHEASTERN-MEDICAL-CENTER | OhioHealth Southeastern Medical Center | Cambridge | OH | cms 360203
OBLENESS-HOSPITAL | OhioHealth O'Bleness Hospital | Athens | OH | cms 360014
BERGER-HOSPITAL | OhioHealth Berger Hospital | Circleville | OH | cms 360170
VAN-WERT-HOSPITAL | OhioHealth Van Wert Hospital | Van Wert | OH | cms 360071
HARDIN-MEMORIAL-HOSPITAL | OhioHealth Hardin Memorial Hospital | Kenton | OH | cms 361315
MORROW-COUNTY-HOSPITAL | OhioHealth Morrow County Hospital | Mount Gilead | OH | rows
BING-CANCER-CENTER | OhioHealth Bing Cancer Center | Columbus | OH | rows
MCCONNELL-HEART-HEALTH-CENTER | OhioHealth McConnell Heart Health Center | Columbus | OH | rows
""")

# ── Riverside Health System (riversideonline): "<facility> - <town>
# Virginia"; towns right.
_t("Riverside Health System", """
Riverside-Regional-Medical-Center---Newport-News-Virginia | Riverside Regional Medical Center | Newport News | VA | cms 490052
Riverside-Shore-Memorial-Hospital---Onancock-Virginia | Riverside Shore Memorial Hospital | Onancock | VA | cms 490037
Riverside-Smithfield-Hospital---Smithfield-VA | Riverside Smithfield Hospital | Smithfield | VA | rows
Riverside-Doctors-Hospital-Williamsburg---Williamsburg-Virginia | Riverside Doctors' Hospital Williamsburg | Williamsburg | VA | cms 490143
Riverside-Walter-Reed-Hospital---Gloucester-Virginia | Riverside Walter Reed Hospital | Gloucester | VA | cms 490130
Riverside-Mental-Health-and-Recovery-Center----Hampton-Virginia | Riverside Mental Health and Recovery Center | Hampton | VA | rows
""")

# ── WVU Medicine: WD_FACILITY_MAP names the hospitals from locationsText;
# the table reads the path too ("2 Locations" postings) and adds three
# named sites the map does not know.
_t("WVU Medicine", """
Ruby-Memorial-Hospital-WVUH | J.W. Ruby Memorial Hospital | Morgantown | WV | address 1 Medical Center Dr, Morgantown
Berkeley-Medical-Center-BMC | Berkeley Medical Center | Martinsburg | WV | cms 510008
Wheeling-Hospital-WHH | Wheeling Hospital | Wheeling | WV | cms 510050
Thomas-Memorial-Hospital-TMH | Thomas Memorial Hospital | South Charleston | WV | cms 510029
Princeton-Community-Hospital-PCH | Princeton Community Hospital | Princeton | WV | cms 510046
Garrett-Regional-Medical-Center-GRMC | Garrett Regional Medical Center | Oakland | MD | cms 210017
Barnesville-Hospital-BARNS | Barnesville Hospital | Barnesville | OH | cms 361321
Fairmont-Medical-Center | Fairmont Medical Center | Fairmont | WV | rows
Saint-Francis-Hospital-SFH | Saint Francis Hospital | Charleston | WV | rows
Mary-Babb-Randolph-Cancer-Center | Mary Babb Randolph Cancer Center | Morgantown | WV | rows
""")

# ── Saint Francis Health (Tulsa): "<campus> - <building>"; the Muskogee,
# Vinita and Glenpool hospitals sat at the Tulsa default.
_t("Saint Francis Health", """
Yale-Campus---Hospital | Saint Francis Hospital | Tulsa | OK | address 6161 S Yale Ave, Tulsa
Yale-Campus---Trauma-Tower | Saint Francis Hospital | Tulsa | OK | address 6161 S Yale Ave, Tulsa
Yale-Campus---Heart-Hospital | Saint Francis Heart Hospital | Tulsa | OK | rows
Yale-Campus---Childrens-Hospital | Children's Hospital at Saint Francis | Tulsa | OK | rows
Yale-Campus---Kelly-Building | - | Tulsa | OK | site (the Yale campus, Tulsa)
South-Campus---Hospital | Saint Francis Hospital South | Tulsa | OK | address 10501 E 91st St, Tulsa
South-Campus---Cardinal-Building | - | Tulsa | OK | site (the South campus, Tulsa)
Laureate-Campus | Laureate Psychiatric Clinic and Hospital | Tulsa | OK | address 6655 S Yale Ave, Tulsa
Cancer-Center-Campus---Hospital | Saint Francis Cancer Center | Tulsa | OK | rows
Muskogee-Campus---Hospital | Saint Francis Hospital Muskogee | Muskogee | OK | address 300 Rockefeller Dr, Muskogee
Muskogee-Campus---Tower | Saint Francis Hospital Muskogee | Muskogee | OK | address 300 Rockefeller Dr, Muskogee
Vinita-Campus---Hospital | Saint Francis Hospital Vinita | Vinita | OK | address 735 N Foreman St, Vinita
Glenpool-Campus---Hospital | Saint Francis Hospital Glenpool | Glenpool | OK | site
Warren-Clinic---TBJ-Tulsa-Main | - | Tulsa | OK | site
""")

# ── Freeman Health System (freemanhealth): the site is the facility.
_t("Freeman Health System", """
1102-W-32nd-Street-Freeman-Hospital-West | Freeman Hospital West | Joplin | MO | address 1102 W 32nd St, Joplin
Joplin---Freeman-West-Hospital | Freeman Hospital West | Joplin | MO | address 1102 W 32nd St, Joplin
932-E-34th-St-Freeman-Hospital-East | Freeman Hospital East | Joplin | MO | address 932 E 34th St, Joplin
Freeman-Springdale-Hospital | Freeman Springdale Hospital | Springdale | AR | rows
Freeman-Bentonville-Hospital | Freeman Bentonville Hospital | Bentonville | AR | rows
Freeman-Willow-Creek-Hospital | Freeman Willow Creek Hospital | Johnson | AR | rows
Freeman-Siloam-Springs-Hospital | Freeman Siloam Springs Hospital | Siloam Springs | AR | rows
Freeman-Heart-and-Vascular-Institute | Freeman Heart and Vascular Institute | Joplin | MO | rows
""", also="Freeman Health")

# ── King's Daughters Medical Center (kdmc): its Ohio hospital sat at the
# Ashland default.
_t("King's Daughters Medical Center", """
UKKD---Ohio-Main-Campus | King's Daughters Medical Center Ohio | Portsmouth | OH | address 1901 Argonne Rd, Portsmouth
""")

# ── Presbyterian Healthcare Services (phs): the site is the facility;
# Rust Medical Center sat at the Albuquerque default.
_t("Presbyterian Healthcare Services", """
Presbyterian-Hospital | Presbyterian Hospital | Albuquerque | NM | cms 320021
Presbyterian-Kaseman-Hospital | Presbyterian Kaseman Hospital | Albuquerque | NM | address 8300 Constitution Ave NE, Albuquerque
Presbyterian-Rust-Medical-Center | Presbyterian Rust Medical Center | Rio Rancho | NM | address 2400 Unser Blvd SE, Rio Rancho
Plains-Regional-Medical-Center | Plains Regional Medical Center | Clovis | NM | cms 320022
Presbyterian-Espanola-Hospital | Presbyterian Espanola Hospital | Espanola | NM | cms 320011
Lincoln-County-Medical-Center | Lincoln County Medical Center | Ruidoso | NM | cms 321306
Socorro-General-Hospital | Socorro General Hospital | Socorro | NM | cms 321301
Presbyterian-Santa-Fe-Medical-Center | Presbyterian Santa Fe Medical Center | Santa Fe | NM | cms 320090
Dr-Dan-C-Trigg-Memorial-Hospital | Dr. Dan C. Trigg Memorial Hospital | Tucumcari | NM | cms 321302
PRESNow-247-Urgent-and-Emergency-Care---Isleta | - | Albuquerque | NM | site (Isleta Blvd, Albuquerque)
PRESNow-247-Urgent-and-Emergency-Care---Coors | - | Albuquerque | NM | site (Coors Blvd, Albuquerque)
PRESNow-247-Urgent-and-Emergency-Care---Uptown | - | Albuquerque | NM | site (Uptown, Albuquerque)
PRESNow-247-Urgent-and-Emergency-Care---Paseo | - | Albuquerque | NM | site (Paseo del Norte, Albuquerque)
""")

# ── Saint Luke's Health System (saintlukes): "<facility> - <street> -
# <town> - <ST>"; towns right.
_t("Saint Luke's Health System", """
Saint-Lukes-Hospital------4401-Wornall-Rd------Kansas-City------MO | Saint Luke's Hospital of Kansas City | Kansas City | MO | cms 260138
Saint-Lukes-North-Hospital------5830-NW-Barry-Rd------Kansas-City------MO | Saint Luke's North Hospital | Kansas City | MO | address 5830 NW Barry Rd, Kansas City
Saint-Lukes-East-Hospital------100-NE-Saint-Lukes-Blvd------Lees-Summit------MO | Saint Luke's East Hospital | Lees Summit | MO | address 100 NE Saint Luke's Blvd, Lee's Summit
Saint-Lukes-South-Hospital------12300-Metcalf-Ave------Overland-Park------KS | Saint Luke's South Hospital | Overland Park | KS | address 12300 Metcalf Ave, Overland Park
Crittenton-Childrens-Center------Elm-Ave------Kansas-City------MO | Crittenton Children's Center | Kansas City | MO | rows
Anderson-County-Hospital------421-S-Maple-St------Garnett------KS | Anderson County Hospital | Garnett | KS | cms 171316
Hedrick-Medical-Center------2799-North-Washington-St------Chillicothe------MO | Hedrick Medical Center | Chillicothe | MO | cms 261321
Wright-Memorial-Hospital------191-Iowa-Blvd------Trenton------MO | Wright Memorial Hospital | Trenton | MO | cms 261309
""")

# ── HonorHealth (honorhealth): "<facility> - <street town ST ZIP>"; towns
# right.
_t("HonorHealth", """
Shea-Medical-Center---9003-E-Shea-Blvd-Scottsdale-AZ-85260 | HonorHealth Scottsdale Shea Medical Center | Scottsdale | AZ | cms 030087
Osborn-Medical-Center---7400-E-Osborn-Rd-Scottsdale-AZ-85251 | HonorHealth Scottsdale Osborn Medical Center | Scottsdale | AZ | cms 030038
Thompson-Peak-Medical-Center---7400-E-Thompson-Peak-Pkwy-Scottsdale-AZ-85255 | HonorHealth Scottsdale Thompson Peak Medical Center | Scottsdale | AZ | address 7400 E Thompson Peak Pkwy, Scottsdale
John-C-Lincoln---250-E-Dunlap-Ave-Phoenix-AZ-85020 | HonorHealth John C. Lincoln Medical Center | Phoenix | AZ | cms 030014
Deer-Valley---19829-N-27th-Ave-Phoenix-AZ-85027 | HonorHealth Deer Valley Medical Center | Phoenix | AZ | cms 030092
Sonoran-Crossing-Medical-Center---33400-N-32nd-Ave-Phoenix-AZ-85085 | HonorHealth Sonoran Crossing Medical Center | Phoenix | AZ | cms 030146
Four-Peaks-Medical-Center---1301-S-Crismon-Rd-Mesa-AZ-85209 | HonorHealth Four Peaks Medical Center | Mesa | AZ | site
Tempe-Medical-Center---1500-S-Mill-Ave-Tempe-AZ-85281 | HonorHealth Tempe Medical Center | Tempe | AZ | cms 030037
Florence-Medical-Center---4545-N-Hunt-Hwy-Florence-AZ-85132 | HonorHealth Florence Medical Center | Florence | AZ | site
Greenbaum-Specialty-Surgical-Hospital---3535-N-Scottsdale-Rd-Scottsdale-AZ-85251 | HonorHealth Greenbaum Surgical Specialty Hospital | Scottsdale | AZ | site
""")

# ── ThedaCare (thedacare): the site is the facility; towns right.
_t("ThedaCare", """
ThedaCare-Regional-Medical-Center---Neenah | ThedaCare Regional Medical Center - Neenah | Neenah | WI | cms 520045
ThedaCare-Regional-Medical-Center---Appleton | ThedaCare Regional Medical Center - Appleton | Appleton | WI | cms 520160
ThedaCare-Medical-Center---Shawano | ThedaCare Medical Center - Shawano | Shawano | WI | cms 521346
ThedaCare-Medical-Center---Waupaca | ThedaCare Medical Center - Waupaca | Waupaca | WI | cms 521334
ThedaCare-Medical-Center---New-London | ThedaCare Medical Center - New London | New London | WI | cms 521326
ThedaCare-Medical-Center---Berlin | ThedaCare Medical Center - Berlin | Berlin | WI | cms 521355
ThedaCare-Medical-Center---Wild-Rose | ThedaCare Medical Center - Wild Rose | Wild Rose | WI | cms 521303
Orthopedic-Spine-and-Pain-Hospital | ThedaCare Orthopedic, Spine and Pain Hospital | Appleton | WI | rows
Oshkosh-Expansion-Hospital | ThedaCare Medical Center - Oshkosh | Oshkosh | WI | rows
ThedaCare-Medical-Center---Fond-du-Lac-Expansion | ThedaCare Medical Center - Fond du Lac | Fond du Lac | WI | site
""")

# ── HSHS (HSHS Hospitals tenant): town sites keep the label; the hospital
# sites carried no town at all.
_t("HSHS Hospitals", """
Springfield---St-Johns-Hospital | HSHS St. John's Hospital | Springfield | IL | cms 140053
OFallon---St-Elizabeths-Hospital | HSHS St. Elizabeth's Hospital | O'Fallon | IL | cms 140187
Green-Bay---St-Vincents-Hospital | HSHS St. Vincent Hospital | Green Bay | WI | address 835 S Van Buren St, Green Bay
Sheboygan---St-Nicholas-Hospital | HSHS St. Nicholas Hospital | Sheboygan | WI | cms 520044
""", also="HSHS")

# ── United Health Services (NY): town sites keep the label.
_t("United Health Services", """
UHS-Wilson-Medical-Center-Johnson-City-NY | UHS Wilson Medical Center | Johnson City | NY | address 33-57 Harrison St, Johnson City
Wilson-Square-UHS-Wilson-Medical-Campus-Johnson-City-NY | UHS Wilson Medical Center | Johnson City | NY | address 33-57 Harrison St, Johnson City
UHS-Binghamton-General-Hospital-Binghamton-NY | UHS Binghamton General Hospital | Binghamton | NY | address 10-42 Mitchell Ave, Binghamton
UHS-Delaware-Valley-Hospital-Walton-NY | UHS Delaware Valley Hospital | Walton | NY | cms 331312
UHS-Chenango-Memorial-Hospital-Norwich-NY | UHS Chenango Memorial Hospital | Norwich | NY | cms 330033
""")

# ── Sharp HealthCare: town sites keep the label; Tri-City sat at the San
# Diego default.
_t("Sharp HealthCare", """
STCMC-Tri-City-MedicalCenter-Main-Campus---4002-Vista-Way | Sharp Tri-City Medical Center | Oceanside | CA | cms 050128; address 4002 Vista Way, Oceanside
""")

# ── Southern Illinois Health (sih): the site is the facility; the system
# office and Carbondale buildings carried their site as the town.
_t("Southern Illinois Health", """
Memorial-Hospital-of-Carbondale | SIH Memorial Hospital of Carbondale | Carbondale | IL | cms 140164
Herrin-Hospital | SIH Herrin Hospital | Herrin | IL | cms 140011
St-Joseph-Memorial-Hospital | SIH St. Joseph Memorial Hospital | Murphysboro | IL | cms 141334
Harrisburg-Medical-Center | Harrisburg Medical Center | Harrisburg | IL | cms 140210
Cancer-Institute | SIH Cancer Institute | Carterville | IL | address 1100 Innovation Way, Carterville
System-Office | - | Carbondale | IL | address 1239 E Main St, Carbondale
Center-for-Medical-Arts | - | Carbondale | IL | address 2601 W Main St, Carbondale
Professional-Office-Building-Carbondale | - | Carbondale | IL | site
""")

# ── Samaritan Health (Watertown NY).
_t("Samaritan Health NY", """
Samaritan-Medical-Center | Samaritan Medical Center | Watertown | NY | cms 330157
Samaritan-Keep-Home | Samaritan Keep Home | Watertown | NY | address 133 Pratt St, Watertown
Samaritan-Summit-Village | Samaritan Summit Village | Watertown | NY | address 22691 Campus Dr, Watertown
""", also="Samaritan Health")

# ── Genesis HealthCare System (Zanesville OH).
_t("Genesis HealthCare System", """
Genesis-Hospital | Genesis Hospital | Zanesville | OH | cms 360039
Genesis-Coshocton-Medical-Center | Genesis Coshocton Medical Center | Coshocton | OH | cms 360109
""")

# ── Owensboro Health: "<facility> - <town> KY".
_t("Owensboro Health", """
Owensboro-Health-Regional-Hospital---Owensboro-KY | Owensboro Health Regional Hospital | Owensboro | KY | cms 180038
Owensboro-Health-Twin-Lakes-Medical-Center---Leitchfield-KY | Owensboro Health Twin Lakes Medical Center | Leitchfield | KY | rows
Owensboro-Health-Muhlenberg-Community-Hospital---Greenville-KY | Owensboro Health Muhlenberg Community Hospital | Greenville | KY | rows
""")

# ── Rogers Behavioral Health: the three hospital campuses.
_t("Rogers Behavioral Health", """
Oconomowoc-Main-Campus-Oconomowoc-WI | Rogers Behavioral Health - Oconomowoc | Oconomowoc | WI | site
Brown-Deer-Main-Campus-Brown-Deer-WI | Rogers Behavioral Health - Brown Deer | Brown Deer | WI | site
West-Allis-Main-Campus-West-Allis-WI | Rogers Behavioral Health - West Allis | West Allis | WI | site
Executive-Drive-Oconomowoc-WI | - | Oconomowoc | WI | site
""")

# ── Sarah Bush Lincoln Health (sarahbush): every row took the Mattoon
# default; the two hospitals, then the town each clinic names
# (town_in_site, Illinois).
_t("Sarah Bush Lincoln Health", """
Sarah-Bush-Lincoln-Health-Center | Sarah Bush Lincoln Health Center | Mattoon | IL | cms 140189
Fayette-County-Hospital | Fayette County Hospital | Vandalia | IL | cms 141346
""")
TOWN_SITE["Sarah Bush Lincoln Health"] = ("IL", "any")

# ── Logan Health and Great River Health: every descriptor is "Town,
# StateName"; FORCE_LOCATION_OVERRIDE moved them all to the head office
# (Kalispell, West Burlington). The town is the posting's (town_state_name);
# town sites keep the system label.
_t("Logan Health", """
Logan-Health-Medical-Center | Logan Health Medical Center | Kalispell | MT | cms 270051
Logan-Health-Whitefish | Logan Health Whitefish | Whitefish | MT | cms 271336
""")
TOWN_BOARDS.update({"Logan Health", "Great River Health"})
