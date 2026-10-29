"""Named sites of the senior living, rehab and post-acute chains (2026-10-29,
the facility scale-up, phase 3, senior-rehab group).

The owner, 2026-10-29: every site of every large employer that posts under one
name is named on its own postings ("i thought you were gonna edit all the
large multihospital systems"). His two rules:
  1. cards and job pages show the SITE ("Apply to <site>"); the system name
     and logo stay on the job page and the system hub (hospital_system is never
     touched here: the upsert key, the hub, the filters, the logo);
  2. named sites (hospitals, named communities, centers, an agency's named
     branch) show their own name; a generic office, a home-based or a remote
     job, a recruiting pool or a posting for several sites shows the system
     name with its town; a town is never guessed.

Every row of these seven chains carried the system's own name on 2026-10-29
(17:30 UTC, active rows): Encompass Health 2,307, Brookdale Senior Living
2,037, Sunrise Senior Living 1,744, Trilogy Health Services 1,425,
PruittHealth 1,417, Signature HealthCARE 832, Life Care Services 774. None of
them is in a phase 2 family table, so the nightly on main names none of them.
Where each feed names the site (read 2026-10-29, a few requests per board):

  Sunrise (Workday)    the location descriptor, "Town, ST - <community>"
                       ("Kennebunk, ME - Huntington Common"), also the first
                       segment of every stored link; the board's own facet
                       lists all of them (SUNRISE_DESCRIPTORS, 252 US ones)
  Trilogy (Jibe)       location_name, the campus ("The Meadows of Kalida");
                       the stored body closes with the same campus and its
                       street ("LOCATION / US-OH-Kalida / The Meadows of
                       Kalida / 755 Ottawa Street ...")
  Signature (Jibe)     location_name / tags3, the center ("Signature HealthCARE
                       of Norfolk", "Oakview Nursing & Rehab Center"); the
                       stored body opens "About Us / <center> is a 100-bed ..."
  PruittHealth (Phenom) the list's workLocation / orgBusiness ("PruittHealth -
                       Aiken", "PruittHealth @ Home - Rome", "PruittPlace -
                       Kennesaw"); nothing stored names it
  Brookdale (Oracle)   the requisition's work location in the list payload,
                       "<5-digit id>-Brookdale <community> (ST)" with the
                       community's own town; nothing stored names it
  Life Care Services (Oracle, work locations hidden in list and detail)
                       the posting body names the community ("When you work at
                       Carillon Senior Living, ...", "Join the Friendship
                       Village Tempe Community!"); stored and read on the
                       nights the detail pass fetches the body
  Encompass Health (Oracle, work locations hidden in list and detail; the
                       facet shows "03028200-Wesley Chapel") the posting's
                       town: one inpatient rehab hospital per town
                       (ENCOMPASS_HOSPITALS, the CMS town of each), the same
                       rule as facilities_town (a posting in a town with two
                       of its hospitals, at the head office or with an area /
                       regional title keeps the label)

hospital_system is never touched. Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/senior_rehab_work/
build_changes.py), so the database holds what the scraper keeps.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

logger = logging.getLogger("scraper.facilities_senior_rehab")

SUNRISE = "Sunrise Senior Living"
TRILOGY = "Trilogy Health Services"
SIGNATURE = "Signature HealthCARE"
PRUITT = "PruittHealth"
BROOKDALE = "Brookdale Senior Living"
LCS = "Life Care Services"
ENCOMPASS = "Encompass Health"
SYSTEMS = frozenset({SUNRISE, TRILOGY, SIGNATURE, PRUITT, BROOKDALE, LCS, ENCOMPASS})

JIBE_SYSTEMS = frozenset({TRILOGY, SIGNATURE})
PHENOM_SYSTEMS = frozenset({PRUITT})
ORACLE_SYSTEMS = frozenset({BROOKDALE})
WORKDAY_SYSTEMS = frozenset({SUNRISE})

_US = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
       "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
       "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
       "PR"}


def key(s) -> str:
    """Lookup key: lower case, "&" -> "and", "saint" / "st." -> "st", every
    other non-alphanumeric dropped ("Winston-Salem, NC - Brighton Gardens"
    and the link segment "Winston---Salem-NC---Brighton-Gardens" are one)."""
    s = str(s or "").lower().replace("’", "'").replace("&", " and ")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bst\.", "st", s)
    return re.sub(r"[^a-z0-9]", "", s)


def clean(s) -> str:
    s = str(s or "").replace("’", "'").replace("–", "-").replace("—", "-").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip()


def town_case(city: str) -> str:
    """An all-capitals town written the usual way ("SOUTH PITTSBURG" ->
    "South Pittsburg"); any other spelling is left as the board wrote it."""
    c = clean(city)
    if len(c) > 3 and c.isupper():
        return " ".join(w[:1].upper() + w[1:].lower() for w in c.split(" "))
    return c


def _town_key(city: str) -> str:
    return re.sub(r"[^a-z]", "", (city or "").lower().replace("saint", "st"))


# ═══════════════════════════ SUNRISE (Workday) ═════════════════════════════
# The board's own location facet (sunriseseniorliving wd12, SUNRISE_EXT_CAREERS,
# 2026-10-29, one request): every US descriptor, as the board writes it. The
# Canadian ones ("Richmond Hill, ON - ...") are left out (non_us_reason drops
# those postings). Each "Town, ST - <community>" names the community and its
# town; a trailing state code the board adds to tell two communities apart
# ("Sunrise of Arlington MA") is not part of the name.
SUNRISE_DESCRIPTORS = """
Abington, PA - Sunrise of Abington
Ackworth, GA - Varenita of West Cobb
Albuquerque, NM - Bear Canyon Estates
Alexandria, VA - Sunrise at Mount Vernon
Alexandria, VA - Sunrise of Alexandria
Annapolis, MD - Sunrise at Bay Village
Annapolis, MD - Sunrise of Annapolis
Arlington, MA - Sunrise of Arlington MA
Arlington, VA - Sunrise at Bluemont Park
Arlington, VA - Sunrise of Arlington VA
Arlington, VA - The Jefferson
Atlanta Metro - OTP East
Atlanta Metro - OTP North
Baton Rouge, LA - Sunrise of Baton Rouge
Bellevue, WA - Sunrise of Bellevue
Bethesda, MD - Fox Hill
Bethesda, MD - Maplewood Park Place
Bethesda, MD - Sunrise of Bethesda
Beverly Hills, CA - Sunrise of Beverly Hills
Bloomfield Hills, MI - Sunrise of Bloomfield Hills
Bloomingdale, IL - Sunrise of Bloomingdale
Boca Raton, FL - Stratford Court of Boca Pointe
Boston Metro Area
Boston Metro Area - Metro West
Boston Metro Area - North Shore
Boston Metro Area - South Shore
Boulder, CO - Sunrise of Boulder
Braintree, MA - Sunrise of Braintree
Bridgewater, NJ - Sunrise of Bridgewater
Brooklyn, NY - Sunrise at Mill Basin
Brooklyn, NY - Sunrise at Sheepshead Bay
Broomfield, CO - Highland Trail
Broomfield, CO - Sunrise at FlatIrons
Buffalo Grove, IL - Sunrise of Buffalo Grove
Burke, VA - Sunrise at Silas Burke House
Burlington, MA - Sunrise of Burlington  MA
Carlsbad, CA - Sunrise at La Costa
Carmel, IN - Sunrise on Old Meridian
Carmichael, CA - Sunrise of Carmichael
Cary, NC - Sunrise of Cary
Central Suffolk County Metro Area
Chandler, AZ - Sunrise of Chandler
Charlotte, NC - Brighton Gardens of Charlotte
Charlotte, NC - Metro Area
Charlotte, NC - Sunrise on Providence
Charlottesville, VA - The Colonnades
Chesterfield, MO - Sunrise of Chesterfield
Chevy Chase, MD - Brighton Gardens of Friendship Heights
Chevy Chase, MD - Sunrise of Chevy Chase
Cheyenne, WY - Whispering Chase
Chicago, IL - Sunrise of Lincoln Park
Cleveland Metro
Cohasset, MA - Sunrise of Cohasset
Columbia, MD - Brighton Gardens of Columbia
Coral Gables, FL - Sunrise of Coral Gables
Cresskill, NJ - Sunrise of Cresskill
Cupertino, CA - Sunrise of Cupertino
Cuyahoga Falls, OH - Sunrise of Cuyahoga Falls
Dallas, TX - Sunrise of Hillcrest
Danville, CA - Sunrise of Danville
DC Metro Area
Denver Metro Area
Denver, CO - Quincy Place
Denver, CO - Sunrise at Cherry Creek
Denver, CO - Sunrise of Pinehurst
Des Peres, MO - Sunrise of Des Peres
Detroit Metro Area
Dix Hills, NY - Sunrise of Dix Hills
Dublin, CA- Emerald Valley
Dublin, OH - Sunrise of Dublin
East Brunswick, NJ - Sunrise of East Brunswick
East Meadow, NY - Sunrise of East Meadow
East Setauket, NY - Sunrise of East Setauket
Edgewater, NJ - Sunrise of Edgewater
Edina, MN - Sunrise of Edina
Edison, NJ - Brighton Gardens of Edison
Edmonds, WA - Sunrise of Edmonds
Emerald Valley
Fair Oaks, CA - Sunrise of Fair Oaks
Fairfax, VA - Sunrise at Fair Oaks VA
Fairfax, VA - Sunrise of Fairfax
Fairfax, VA - The Braddock Glen
Fairfield, CT - Sunrise of Fairfield
Fairfield, NJ - Sunrise at West Essex
Falls Church, VA - Sunrise of Falls Church
Findlay, OH - Sunrise of Findlay
Florham Park, NJ - Brighton Gardens of Florham Park
Flossmoor, IL - Sunrise of Flossmoor
Fort Belvoir, VA - The Fairfax
Fort Collins, CO - Parkwood Estates
Fort Worth, TX - Sunrise of Fort Worth
Franklin Lakes, NJ - Sunrise of Franklin Lakes
Frisco, TX - Sunrise of Frisco
Glen Cove, NY - Sunrise of Glen Cove
Grand Rapids, MI - Sunrise of Cascade
Greater Orange County Metro Area
Greeley, CO - Garden Square at Westlake
Greeley, CO - Greeley Place
Grosse Pointe Woods, MI - Sunrise Grosse Pointe Woods
Grosse Pointe Woods, MI - Sunrise on Vernier
Gurnee, IL - Sunrise of Gurnee
Haverford, PA - Sunrise of Haverford
Haverford, PA - The Quadrangle
Henderson, NV - Sunrise of Henderson
Hermosa Beach, CA - Sunrise of Hermosa Beach
Highland Park, IL - Sunrise of Highland Park
Holbrook, NY - Sunrise of Holbrook
Huntington Station, NY - Sunrise of Huntington
Irvine Metro Area
Issaquah, WA - Sunrise of Issaquah
Jackson, NJ - Sunrise of Jackson
Jacksonville, FL - Sunrise of Jacksonville
Katy, TX - Sunrise of Cinco Ranch
Kennebunk, ME - Huntington Common
Lafayette Hill, PA - Sunrise of Lafayette Hill
Lakewood, CO - Lakewood Estates
Lakewood, CO - The Courtyard at Lakewood
Leawood, KS - Sunrise of Leawood
Leominster, MA - Sunrise of Leominster
Levittown, NY - At Home New York
Lilburn, GA - Sunrise at Five Forks
Lincroft, NJ - Sunrise of Lincroft
Littleton, CO - Sunrise at Orchard
Livingston, NJ - Sunrise of Livingston
Lombard, IL - Sunrise at Fountain Square
Long Beach, CA- Sunrise of Long Beach
Los Angeles Metro Area
Loveland, CO - Sugar Valley Estates
Lynbrook, NY - Sunrise of North Lynbrook
Lynnfield, MA - Sunrise of Lynnfield
Madison, NJ - Sunrise of Madison
Marlboro, NJ - Sunrise of Marlboro
McLean, VA - Community Support Office
McLean, VA - Sunrise of McLean
McLean, VA - Sunrise of McLean Village
Media, PA - Sunrise of Granite Run
Metairie, LA - Sunrise of Metairie
Middlesex County Metro Area
Middletown, NJ - Brighton Gardens of Middletown
Mission Viejo, CA - Sunrise of Mission Viejo
Montgomery Village, MD - Sunrise at Montgomery Village
Morris Plains, NJ - Sunrise of Morris Plains
Mountain Lakes, NJ - Sunrise of Mountain Lakes
Mountainside, NJ - Brighton Gardens of Mountainside
Mt. Vernon, NY - Sunrise at Fleetwood
Naperville, IL - Sunrise of Naperville
Naperville, IL - Sunrise of Naperville North
New City, NY - Sunrise of New City
New York Metro Area
New York, NY - Sunrise at East 56th
New York, NY - The Apsley
New York, NY - The Capstone Battery Park City
Newton, MA - Sunrise of Newton
Newtown Square, PA - Sunrise of Newtown Square
Northern Virginia Metro Area - Arlington
Northville, MI - Sunrise of Northville
Norwood, MA - Sunrise of Norwood
Novato, CA- The Bluffs at Hamilton Hill
Oakland Metro Area
Oakton, VA - Sunrise at Hunter Mill
Oceanside, CA - Sunrise of Oceanside
Old Tappan, NJ - Sunrise of Old Tappan
Olympia Fields, IL - Sunrise of Olympia Fields
Orange, CA - Sunrise of Orange
Overland Park, KS - Sunrise of Overland Park
Palos Park, IL - Sunrise of Palos Park
Park Ridge, IL - Sunrise of Park Ridge
Parma, OH - Sunrise at Parma
Peabody, MA - Sunrise at Gardner Park
Pikesville, MD - Sunrise of Pikesville
Plainview, NY - Sunrise of Plainview
Plymouth, MA - Sunrise of Plymouth Beach
Potomac Falls, VA - Sunrise at Countryside
Prospect Heights, IL - Sunrise of Prospect Heights
Pueblo, CO - Pueblo Regent
Raleigh, NC - Brighton Gardens of Raleigh
Raleigh, NC - Sunrise at North Hills
Raleigh, NC - Sunrise of Raleigh
Randolph, NJ - Sunrise of Randolph
Rapid City, SD - Holiday Hill Estates
Redmond, WA - Sunrise of Redmond
Reston, VA - Sunrise of Reston Town Center
Richmond, VA - Sunrise of Bon Air
Richmond, VA - Sunrise of Richmond
Richmond, VA - Sunrise Villa Tuckahoe
River Forest, IL - Sunrise of River Forest
Riverside, CA - Sunrise at Canyon Crest
Rocklin, CA - Sunrise of Rocklin
Rockville, MD - Sunrise of Rockville
Sacramento Metro Area
Saddle River, NJ - Brighton Gardens of Saddle River
Salt Lake City, UT - Sunrise at Holladay
San Clemente, CA- San Clemente Villas
San Diego Metro Area
San Francisco Metro Area
San Mateo, CA - Sunrise of San Mateo
San Mateo, CA - The Stratford
Sandy Springs, GA - Huntcliff Summit I
Sandy Springs, GA - Sunrise at Huntcliff Summit II
Sandy, UT - Sunrise of Sandy
Scarsdale, NY- Sunrise of Scarsdale
Schaumburg, IL - Sunrise of Schaumburg
Scottsdale, AZ - Sunrise of North Scottsdale
Scottsdale, AZ - Sunrise of Scottsdale
Severna Park, MD - Sunrise of Severna Park
Shrewsbury, NJ - Sunrise of Shrewsbury
Silver Spring, MD - Bedford Court
Silver Spring, MD - Sunrise of Silver Spring
Simi Valley, CA - Varenita of Simi Valley
Smithtown, NY - Sunrise of Smithtown
Springfield, VA - Sunrise of Springfield
St. Charles, IL - Brighton Gardens of St. Charles
Stamford, CT - Brighton Gardens of Stamford
Staten Island, NY - Sunrise of New Dorp
Staten Island, NY - Sunrise of Staten Island
Summit, NJ - Sunrise of Summit
Sunnyvale, CA - Sunrise of Sunnyvale
Sunrise at Holladay
Sunrise of Burlington MA
Sunrise of Fair Oaks
Sunrise of North Severna Park
Sunrise of Scarsdale
Tarrytown, NY- Sunrise of Tarrytown
The Bluffs at Hamilton Hill
The Quadrangle
Thousand Oaks, CA - Sunrise of Thousand Oaks
Troy, MI - Sunrise of Troy
Tucson, AZ - Sunrise at River Road
Valencia, CA - Sunrise at Sterling Canyon
Vienna, VA - Sunrise of Vienna
Wall, NJ - Sunrise of Wall
Washington, DC - Sunrise on Connecticut Avenue
Wayland, MA - Sunrise of Wayland
Wayne, NJ - Sunrise of Wayne
Webster Groves, MO - Sunrise of Webster Groves
West Babylon, NY - Sunrise of West Babylon
West Chester, PA - Sunrise of Westtown
West Orange, NJ - Brighton Gardens of West Orange
Westfield, NJ - Sunrise of Westfield
Westlake Village, CA - Sunrise of Westlake Village
Westlake Village, CA - Varenita of Westlake Village
Westminster, CO - Sunrise of Westminster
Westmont, IL - Sunrise of Westmont
Wheaton, IL - Brighton Gardens of Wheaton
Willowbrook, IL - Sunrise of Willowbrook
Wilmington, DE - Sunrise of Wilmington
Wilton, CT - Sunrise of Wilton
Winston - Salem, NC - Brighton Gardens of Winston - Salem
Woodcliff Lake, NJ - Sunrise of Woodcliff Lake
Yardley, PA - Sunrise of Lower Makefield
Yonkers, NY - Sunrise of Crestwood
Yorba Linda, CA - Sunrise of Yorba Linda
"""
# Descriptors that are no one community (rule 2: the system name). The
# head office keeps its own town; a recruiting pool ("Boston Metro Area",
# "Atlanta Metro - OTP North") or home care ("At Home New York": caregivers
# work in clients' homes) keeps the system name, a pool with no town.
SUNRISE_OFFICE_RX = re.compile(r"\bmetro\b|\bcommunity support office\b|^at home\b", re.I)


def _sunrise_parse(desc: str):
    """(name or None, town, ST) for one board descriptor, or None when it is
    not "Town, ST - name" nor a bare name."""
    d = clean(desc)
    m = re.match(r"^(.+?),\s*([A-Z]{2})\s*-\s*(.+)$", d)
    if m:
        town, st, name = clean(m.group(1)), m.group(2), clean(m.group(3))
        if " - " in town:                          # "Winston - Salem, NC - Brighton Gardens of Winston - Salem"
            spaced, town = town, re.sub(r"\s+-\s+", "-", town)
            name = name.replace(spaced, town)
        if st not in _US:
            return None
        if SUNRISE_OFFICE_RX.search(name) or SUNRISE_OFFICE_RX.search(d):
            return (None, town if not re.search(r"\bmetro\b", town, re.I) else "", st)
        name = re.sub(rf"\s+{st}$", "", name)      # "Sunrise of Arlington MA" -> "Sunrise of Arlington"
        return (name, town, st)
    if SUNRISE_OFFICE_RX.search(d):
        return (None, "", "")
    return (d, "", "")


def _build_sunrise() -> dict:
    """{key(descriptor): (name or None, town, ST, source)}. A bare name (no
    town in the descriptor: "The Quadrangle") takes the town of the board's
    one "Town, ST - <same name>" descriptor ("Haverford, PA - The
    Quadrangle"); with none or two such descriptors it gets no town."""
    parsed = {}
    for line in SUNRISE_DESCRIPTORS.strip().splitlines():
        p = _sunrise_parse(line)
        if p is not None:
            parsed[line.strip()] = p
    towns_by_name: dict = {}
    for desc, (name, town, st) in parsed.items():
        if name and town:
            towns_by_name.setdefault(key(name), set()).add((town, st))
            # the descriptor's own spelling with the state code too ("Sunrise of Burlington  MA")
            m = re.match(r"^.+?,\s*[A-Z]{2}\s*-\s*(.+)$", desc)
            if m and key(m.group(1)) != key(name):
                towns_by_name.setdefault(key(m.group(1)), set()).add((town, st))
    out = {}
    for desc, (name, town, st) in parsed.items():
        src = "site"
        if name and not town:
            hits = towns_by_name.get(key(name), set())
            if len(hits) == 1:
                town, st = next(iter(hits))
                src = "site: the board's own descriptor for the same community"
            else:
                src = "no town: the board names none"
            m = re.match(rf"^(.+?)\s+({'|'.join(sorted(_US))})$", name)
            if m and (town, st) != ("", "") and m.group(2) == st:
                name = m.group(1)
        out[key(desc)] = (name, town, st, src)
    return out


SUNRISE_SITES = _build_sunrise()


def sunrise_site(loc: str, external_path: str = ""):
    """(hospital_name, city, state) for one Sunrise posting from its
    descriptor (locationsText), else from its link's first path segment (the
    primary descriptor slugged, the only place a "2 Locations" posting names
    it); None when neither is a descriptor the board listed (placed as
    before). A community gives its own name and the board's town; an office
    or a pool gives the system name (its town where it has one)."""
    cands = []
    if (loc or "").strip() and not re.match(r"^\s*\d+\s+locations?\s*$", loc, re.I):
        cands.append(loc)
    seg = _path_segment(external_path)
    if seg:
        cands.append(seg)
    for c in cands:
        hit = SUNRISE_SITES.get(key(c))
        if hit is not None:
            name, town, st, _src = hit
            return (name or SUNRISE, town, st)
    # A descriptor the facet did not list yet, in the board's usual shape.
    if cands and cands[0] == loc:
        p = _sunrise_parse(loc)
        if p is not None and p[1] and p[2]:
            return (p[0] or SUNRISE, p[1], p[2])
    return None


def sunrise_unknown(loc: str, external_path: str = "") -> str:
    """A descriptor no table entry names (for the run log), or ""."""
    if sunrise_site(loc, external_path) is None and (loc or "").strip():
        return clean(loc)
    return ""


def _path_segment(path: str) -> str:
    p = path or ""
    if "/job/" not in p:
        return ""
    m = re.match(r"([^/]+)/", p.split("/job/", 1)[1])
    return m.group(1) if m else ""


# ═══════════════════════════ TRILOGY (Jibe) ════════════════════════════════
# location_name is the campus as the employer names it ("Cedar Creek Health
# Campus", "The Willows at East Lansing", "Wellbrooke of Carmel"); 205 values
# on the 1,425 stored rows. Not one site (rule 2: the system name): the home
# office, the divisions and regions, a bare town, the travel pool, the
# therapy company's generic value ("Synchrony Rehab -"), and its contract
# sites at other operators' buildings ("Synchrony Rehab at Logan Acres Care
# Center": another operator's site is never named as Trilogy's).
TRILOGY_OFFICES = {key(s) for s in (
    "Trilogy Health Services", "Trilogy Health Services - Travel", "Great Lakes Division",
    "Central South Division", "North Ohio Division", "Northwest Indiana Division", "Western Michigan",
    "Southwest Indiana - Jasper", "Synchrony Rehab -", "Synchrony Rehab")}
TRILOGY_OFFICE_RX = re.compile(r"\bdivision\b|^trilogy\b|^synchrony rehab\b(?! *-? *$)|^[A-Za-z .']+, [A-Z]{2}$", re.I)
# spellings written clean
TRILOGY_RENAME = {
    key("Rehab - Kingston Senior Living of Perrysburg"): "Kingston Senior Living of Perrysburg",
    key("Kingston Residences of Marion OPT"): "Kingston Residences of Marion",
    key("Shelby Farms Senior Living, LLC"): "Shelby Farms Senior Living",
    key("The Springs Of Mooresville"): "The Springs of Mooresville",
}


def trilogy_name(value: str) -> str:
    """The campus a Trilogy location value names, or "" (the system name)."""
    v = clean(value)
    if not v:
        return ""
    k = key(v)
    if k in TRILOGY_RENAME:
        return TRILOGY_RENAME[k]
    if k in TRILOGY_OFFICES or TRILOGY_OFFICE_RX.search(v):
        return ""
    return re.sub(r",?\s+LLC\.?$", "", v)


_TRILOGY_BLOCK_RX = re.compile(r"(?:^|\n)\s*LOCATION\s*\n+\s*US-(?:[A-Z]{2}-)?[^\n]*\n+\s*([^\n]+)")


def trilogy_body_site(description: str) -> str:
    """The campus the body's closing LOCATION block names ("LOCATION / US-IN-
    Lowell / Cedar Creek Health Campus / 18275 ..."), or ""."""
    m = _TRILOGY_BLOCK_RX.search(description or "")
    if not m:
        return ""
    v = clean(m.group(1))
    if v.upper() == "BENEFITS":
        return ""
    return trilogy_name(v)


# ═══════════════════════════ SIGNATURE (Jibe) ══════════════════════════════
# The feed's location_name / tags3 (the careers board's Facility facet, 87
# values on 2026-10-29) and the stored body's "About Us" sentence name the
# center. One clean name per center: the center's own spelling in its body
# ("Signature HealthCARE at North Hardin Rehab & Wellness Center"), the feed's
# where the body's is cut ("Clinton County Care & Rehab Center"). Towns are
# the feed's own (city and state of the posting: every center's postings sit
# in one town).
SIGNATURE_CENTERS: dict[str, tuple] = {}     # key(spelling) -> clean name


def _sig(name, *spellings):
    for s in (name,) + spellings:
        k = key(s)
        old = SIGNATURE_CENTERS.get(k)
        if old is not None and old != name:
            raise ValueError(f"Signature: {s!r} names {old!r} and {name!r}")
        SIGNATURE_CENTERS[k] = name


_sig("Bluegrass Care and Rehab Center", "Bluegrass Care & Rehab Center")
_sig("Clinton County Care & Rehab Center", "Clinton County Care & Rehabilitation")
_sig("Danville Centre for Health & Rehab")
_sig("Fountain Circle Care & Rehab Center")
_sig("Harrodsburg Health & Rehab Center")
_sig("Hermitage Care & Rehabilitation Center", "Hermitage Care and Rehab Center")
_sig("Lee County Care & Rehabilitation Center", "Lee County Care & Rehab Center")
_sig("Liberty Care and Rehabilitation Center", "Liberty Care & Rehabilitation Cente")
_sig("Mayfair Manor")
_sig("Morgantown Care & Rehabilitation Center", "Morgantown Care & Rehab Center")
_sig("Mountain City Care & Rehabilitation Center", "Mountain City Care & Rehab Center")
_sig("Northwest Florida Hospital")
_sig("Oakview Nursing and Rehabilitation Center", "Oakview Nursing & Rehab Center")
_sig("Pickett Care & Rehabilitation Center", "Pickett Care and Rehab Center")
_sig("Prestonsburg Health Care Center")
_sig("Princeton Transitional Care & Assisted Living", "Princeton", "Princeton Transitional & ALF")
_sig("Riverside Care & Rehabilitation Center", "Riverside Care & Rehab Center")
_sig("Riverview HealthCARE Center", "Riverview Health Care Center")
_sig("Rockcastle Health & Rehabilitation Center", "Rockcastle Health & Rehab Center")
_sig("Signature HealthCARE at Colonial Rehab & Wellness Center", "Signature HealthCARE at Colonial Rehab & Wellness")
_sig("Signature HealthCARE at Heritage Hall Rehab & Wellness Center",
     "Signature HealthCARE at Heritage Hall Rehab & Wellness")
_sig("Signature HealthCARE at Hillcrest")
_sig("Signature HealthCARE at Jackson Manor Rehab & Wellness Center",
     "Signature HealthCARE at Jackson Manor Rehab & Wellness")
_sig("Signature HealthCARE at Jefferson Manor Rehab & Wellness Center",
     "Signature HealthCARE at Jefferson Manor Rehab & Wellness")
_sig("Signature HealthCARE at Jefferson Place Rehab & Wellness Center",
     "Signature HealthCARE at Jefferson Place Rehab & Wellness")
_sig("Signature HealthCARE at North Hardin Rehab & Wellness Center",
     "Signature HealthCARE of North Hardin Rehab & Wellness")
_sig("Signature HealthCARE at Rockford Rehab & Wellness Center", "Signature Healthcare at Rockford")
_sig("Signature HealthCARE at Summerfield Rehab & Wellness Center", "Signature Healthcare At Summerfield")
_sig("Signature HealthCARE at Summit Manor Rehab & Wellness Center",
     "Signature HealthCARE at Summit Manor Rehab & Wellness")
_sig("Signature HealthCARE of Bowling Green")
_sig("Signature HealthCARE of Bremen")
_sig("Signature HealthCARE of Carrollton Rehab & Wellness Center", "Signature HealthCARE of Carrollton Rehab & Wellness")
_sig("Signature HealthCARE of Chapel Hill")
_sig("Signature HealthCARE of Clarksville")
_sig("Signature HealthCARE of Cleveland")
_sig("Signature HealthCARE of East Louisville", "Signature HealthCARE East Louisville")
_sig("Signature HealthCARE of Elizabethton Rehab & Wellness Center")
_sig("Signature HealthCARE of Elizabethtown")
_sig("Signature HealthCARE of Erin")
_sig("Signature HealthCARE of Fentress County", "Signature HealthCARE Fentress Co.")
_sig("Signature HealthCARE of Georgetown")
_sig("Signature HealthCARE of Glasgow Rehab & Wellness Center")
_sig("Signature HealthCARE of Greeneville")
_sig("Signature HealthCARE of Hart County Rehab & Wellness Center", "Signature HealthCARE of Hart County Rehab & Wellness")
_sig("Signature HealthCARE of Hartford Rehab & Wellness Center", "Signature HealthCARE of Hartford Rehab & Wellness")
_sig("Signature HealthCARE of Kinston")
_sig("Signature HealthCARE of McCreary County Rehab & Wellness Center",
     "Signature HealthCARE of McCreary County Rehab & Wellness")
_sig("Signature HealthCARE of Memphis")
_sig("Signature HealthCARE of Monroe County Rehab & Wellness Center",
     "Signature HealthCARE of Monroe County Rehab & Wellness")
_sig("Signature HealthCARE of Monteagle Rehab & Wellness Center")
_sig("Signature HealthCARE of Muncie", "Signature Healthcare of Muncie")
_sig("Signature HealthCARE of Norfolk")
_sig("Signature HealthCARE of Parkwood")
_sig("Signature HealthCARE of Portland Rehab & Wellness Center")
_sig("Signature HealthCARE of Primacy")
_sig("Signature HealthCARE of Putnam County", "Signature HealthCARE of Putnam Co")
_sig("Signature HealthCARE of Ridgely Rehab & Wellness Center")
_sig("Signature HealthCARE of Roanoke Rapids")
_sig("Signature HealthCARE of Rockwood Rehab & Wellness Center")
_sig("Signature HealthCARE of Rogersville")
_sig("Signature HealthCARE of South Louisville", "Signature HealthCARE South Louisville")
_sig("Signature HealthCARE of South Pittsburg Rehab & Wellness Center")
_sig("Signature HealthCARE of Spencer County", "Signature HealthCARE of Spencer Co.")
_sig("Signature HealthCARE of Terre Haute")
_sig("Silver Angels of Tennessee - Sevier", "Silver Angels of Tennessee - Sevier, LLC")
_sig("Spring City Care and Rehab Center", "Spring City Care and Rehabilitation")
_sig("Standing Stone Care & Rehabilitation Center", "Standing Stone Care and Rehab Center")
_sig("Sunrise Manor", "Sunrise Manor Nursing Home")
_sig("Westmoreland Care & Rehabilitation Center", "Westmoreland Care & Rehab Center")
# Offices, regional and support units (rule 2: the system name with the
# town): "Signature HealthCARE, LLC", "Signature HealthCARE KY, Lexington",
# "Signature HealthCARE TN, Memphis", "... - North Carolina Regional",
# "Signature Consulting Services LLC", "Signature HealthCARE Owensboro" (a
# regional office: the Owensboro center is "Signature HealthCARE at
# Hillcrest"). The bodies of these postings open with the company profile
# ("Signature HealthCARE is a family-based healthcare company ...").
SIGNATURE_OFFICE_RX = re.compile(
    r"^Signature HealthCARE(?:,? LLC\b| [A-Z]{2},| - Home Office| Owensboro$)|^Signature Consulting|\bRegional\b",
    re.I)


def signature_name(value: str) -> str:
    """The center a Signature location value (or body spelling) names, or ""
    (the system name). A value the table does not list is the center's own
    name as the board writes it (and is logged), unless it reads like an
    office."""
    v = clean(value)
    if not v:
        return ""
    hit = SIGNATURE_CENTERS.get(key(v))
    if hit:
        return hit
    if SIGNATURE_OFFICE_RX.search(v):
        return ""
    UNKNOWN.setdefault(SIGNATURE, {})
    UNKNOWN[SIGNATURE][v] = UNKNOWN[SIGNATURE].get(v, 0) + 1
    return re.sub(r",?\s+LLC\.?$", "", v)


_SIG_ABOUT_RX = re.compile(
    r"^\s*About Us\s*\n+\s*(?:Welcome to\s+)?(.+?)(?:\s*,\s*(?:established|a|located|an|it)\b|\s+(?:is|has|was|offers|provides|opened|sits)\b)",
    re.I)


def signature_body_site(description: str) -> str:
    """The center the body's "About Us" sentence names, or "" (the company
    profile, an office, a body that names none). Only a spelling the table
    lists counts here."""
    m = _SIG_ABOUT_RX.search(description or "")
    if m:
        hit = SIGNATURE_CENTERS.get(key(clean(m.group(1))), "")
        if hit:
            return hit
    # the line after "About Us" alone: "Silver Angels of Tennessee - Sevier office!"
    m = re.match(r"^\s*About Us\s*\n+\s*([^\n]+)", description or "", re.I)
    if not m:
        return ""
    v = re.sub(r"\s+office!?$", "", clean(m.group(1)), flags=re.I)
    return SIGNATURE_CENTERS.get(key(v), "")


# ═══════════════════════════ JIBE HOOK ═════════════════════════════════════
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run


def jibe_facility(system: str, j: dict):
    """(hospital_name, how) for one /api/jobs row of a JIBE_SYSTEMS board, or
    None for any other board. how: "site" (named), "office" (the system name:
    an office, a division, a posting for several sites), "none" (no value)."""
    if system not in JIBE_SYSTEMS:
        return None
    j = j or {}
    if j.get("multipleLocations") is True:
        return system, "office"
    raw = clean(j.get("location_name"))
    if not raw:
        tag = j.get("tags5" if system == TRILOGY else "tags3")
        if isinstance(tag, (list, tuple)):
            tag = next((str(t) for t in tag if str(t or "").strip()), "")
        raw = clean(tag)
    if not raw:
        return system, "none"
    name = trilogy_name(raw) if system == TRILOGY else signature_name(raw)
    return (name, "site") if name else (system, "office")


# ═══════════════════════════ PRUITTHEALTH (Phenom) ═════════════════════════
# The Phenom list carries workLocation (= orgBusiness, the board's Business
# facet: 296 values on 2026-10-29). Named sites (rule 2): the nursing and
# rehab centers ("PruittHealth - Aiken"), assisted living ("PruittPlace -
# Kennesaw", "The Oaks at Whitaker Glen"), the home health, home care and
# hospice agencies' named branches ("PruittHealth @ Home - Rome", "PH @ Home -
# Albany", "PruittHealth Hospice Macon", "PH Hospice Triangle"), and the four
# North Carolina State Veterans Homes PruittHealth runs ("NCVA Salisbury",
# "NCSVH - Kernersville"). The therapy staff posted to a center ("TS SN
# Aiken") work at that center: they take the center's name when the board
# lists that center ("PruittHealth - Aiken"), else the system name.
# The system name: corporate and support units ("PH Executive", "PH Finance",
# "PruittHealth Pharmacy Services Atlanta", "PH Food and Linen Svc-New Bern",
# "PruittHealth Home Health - Home Office"), outpatient therapy ("TS OPT
# Kennesaw"), bare towns ("NORCROSS", "Toomsboro") and the Maryland veterans
# service line.
_PH_OFFICE_RX = re.compile(
    r"^PH (?:Executive|Finance|Compliance|General Counsel|Health Services|Care Integration|Chief|Pharmacy|"
    r"Food and Linen|VA Services|Veteran Services)\b|"
    r"^PruittHealth (?:Care Management|Construction|Consulting|Diagnostic|Medical Supply|Premier|Pharmacy|"
    r"Home Health - Home Office)\b|^PruittHealth - Family First$|^TS OPT\b|^Main Group$|^Corporate$", re.I)
_PH_CENTER_RX = re.compile(r"^Pruitt ?Health\s*-\s*(.+?)(?:,?\s+LLC\.?)?$", re.I)
_PH_NAMED_RX = re.compile(
    r"^(?:Pruitt ?Health @ Home|PH @ Home)\b|^(?:Pruitt ?Health|PH) Hospice\b|^PruittHealth Home First\b|"
    r"^PruittPlace\b|^The Oaks\b|^Parkwood Developmental Center$|^Sparks Inn Retirement Center$|"
    r"^PruittHealth Home Health - (?!Home Office)", re.I)
_PH_VETERANS_RX = re.compile(r"^(?:NCVA|NCSVH)\s*-?\s*(.+)$", re.I)
_PH_THERAPY_RX = re.compile(r"^TS SN\s+(.+)$", re.I)
# Therapy-at-a-center spellings that differ from the center's own.
_PH_THERAPY_ALIAS = {key("Old Capital"): key("Old Capitol"), key("Covington Manor"): key("Covington"),
                     key("Magnolia Manor South"): key("Magnolia Manor")}


def _ph_clean(v: str) -> str:
    v = re.sub(r",?\s+LLC\.?$", "", clean(v))
    v = re.sub(r"^Pruitthealth\b", "PruittHealth", v)
    v = re.sub(r"^PH @ Home\b\s*-?\s*", "PruittHealth @ Home - ", v)
    v = re.sub(r"^PH Hospice\b\s*", "PruittHealth Hospice ", v)
    return v


def pruitt_name(value: str, centers: dict | None = None) -> str:
    """The site a PruittHealth workLocation / orgBusiness value names, or ""
    (the system name). `centers` is {key(center short name): clean name}
    (PRUITT_CENTERS by default)."""
    v = clean(value)
    if not v or _PH_OFFICE_RX.search(v):
        return ""
    centers = PRUITT_CENTERS if centers is None else centers
    m = _PH_CENTER_RX.match(v)
    if m:
        return f"PruittHealth - {clean(m.group(1))}"
    m = _PH_THERAPY_RX.match(v)
    if m:
        k = key(m.group(1))
        k = _PH_THERAPY_ALIAS.get(k, k)
        return centers.get(k, "")
    m = _PH_VETERANS_RX.match(v)
    if m:
        return f"North Carolina State Veterans Home - {clean(m.group(1))}"
    if _PH_NAMED_RX.search(v):
        return _ph_clean(v)
    return ""


# The centers the board lists (orgBusiness, 2026-10-29): short name -> name.
PRUITT_CENTERS = {key(s): f"PruittHealth - {s}" for s in (
    "Aiken", "Ashburn", "Athens Heritage", "Augusta", "Augusta Hills", "Austell", "Bamberg", "Barnwell",
    "Bethany", "Blue Ridge", "Blythewood", "Brevard", "Brookhaven", "Brunswick", "Carolina Point", "Carrollton",
    "Christian City", "Columbia", "Conway", "Covington", "Creekside", "Crestwood", "Crystal Coast", "Decatur",
    "Dillon", "Durham", "Eastside", "Elkin", "Estill", "Evans", "Fairburn", "Farmville", "Fitzgerald",
    "Fleming Island", "Forsyth", "Fort Oglethorpe", "Franklin", "Grandview", "Greenville", "Griffin",
    "Holly Hill", "Jasper", "LaFayette", "Lakehaven", "Lakeside", "Lanier", "Lilburn", "Limestone", "Lithonia",
    "Macon", "Magnolia Manor", "Marietta", "Moncks Corner", "Monroe", "Moultrie", "Neuse", "North Augusta",
    "North Tampa", "Ocilla", "Old Capitol", "Orangeburg", "Palmyra", "Panama City", "Peake", "Pensacola",
    "Pickens", "Raleigh", "Richmond", "Ridgeway", "Rock Hill", "Rockingham", "Rome", "Santa Rosa", "Savannah",
    "Scenic View", "Seaside", "Shepherd Hills", "SouthWood", "Spring Valley", "St. Marys", "Sunrise",
    "Swainsboro", "Sylvester", "The Lodge", "Toccoa", "Toomsboro", "Town Center", "Trent", "Union Pointe",
    "Valdosta", "Virginia Park", "Walterboro", "Warner Robins", "Washington", "West Atlanta")}


def phenom_facility(system: str, doc: dict) -> str | None:
    """hospital_name for one Phenom list doc of a PHENOM_SYSTEMS board (the
    system name when the value names no site), or None for any other board."""
    if system not in PHENOM_SYSTEMS:
        return None
    doc = doc or {}
    v = doc.get("workLocation") or doc.get("orgBusiness") or ""
    if isinstance(v, (list, tuple)):
        v = next((str(x) for x in v if str(x or "").strip()), "")
    return pruitt_name(str(v)) or system


# ═══════════════════════════ BROOKDALE (Oracle) ════════════════════════════
# The work location: "50810-Brookdale Westlake Village (OH)" is the community
# Brookdale Westlake Village, at the work location's own town (its address,
# TownOrCity / Region2; the requisition's PrimaryLocation is a recruiting
# geography: a Brookdale Montclair Poulsbo posting listed at Seattle). A
# building number some communities carry ("Brookdale Burlington 2") is not
# part of the public name. "59000-Nashville Office, TN" and the other offices
# keep the system name and the posting's own town.
_BKD_RX = re.compile(r"^\s*\d{3,6}\s*-\s*(Brookdale\s.+?)\s*\(([A-Z]{2})\)\s*$")


def brookdale_name(location_name: str) -> str:
    m = _BKD_RX.match(location_name or "")
    if not m:
        return ""
    name = clean(m.group(1))
    return re.sub(r"\s+\d$", "", name)


def oracle_facility(system: str, j: dict, city: str = "", state: str = ""):
    """(hospital_name, city, state) for one Oracle requisition of an
    ORACLE_SYSTEMS tenant, or None for any other tenant. The first work
    location names the site; a community takes its own town when the work
    location gives one in the same state (or the posting gives no state)."""
    if system not in ORACLE_SYSTEMS:
        return None
    wl = (j or {}).get("workLocation", (j or {}).get("WorkLocation"))
    items = wl.get("items") if isinstance(wl, dict) else wl
    for w in items if isinstance(items, list) else []:
        if not isinstance(w, dict):
            continue
        ln = str(w.get("LocationName") or "").strip()
        if not ln:
            continue
        name = brookdale_name(ln)
        if not name:
            return system, city, state
        wt = clean(w.get("TownOrCity"))
        ws = clean(w.get("Region2")).upper()
        if wt and ws in _US and (not state or ws == state):
            return name, town_case(wt), ws
        return name, city, state
    return system, city, state


# ═══════════════════════════ LIFE CARE SERVICES (body text) ═════════════════
# The communities Life Care Services manages, each under its own name, read
# off the stored bodies (2026-10-29: 774 active rows; the community's own
# "When you work at <name>," line, "Join the <name> Community!", "<name> is an
# upscale ...") and the tenant's work-location facet ("1720 Sedgebrook").
# name -> (town, ST, spellings...). Matched exactly (case, punctuation, "&"
# aside) inside the body's opening, and only the communities of the
# posting's own town are looked for (a short spelling, "Rose Senior Living",
# "The Avalon", "Friendship Village", names the one community of that brand
# in that town); a posting with no town is read against them all. A body
# that names two communities names none. The head office ("LCS", Des Moines)
# and the company-wide programs keep the system name.
LCS_COMMUNITIES = {
    "Friendship Village Tempe": ("Tempe", "AZ"),
    "Friendship Village Kalamazoo": ("Kalamazoo", "MI", "Friendship Village"),
    "Acacia Health Center": ("Phoenix", "AZ"),
    "Residences at Vantage Point": ("Columbia", "MD"),
    "Clarendale at Bellevue Place": ("Nashville", "TN"),
    "Delaney South Shore": ("League City", "TX"),
    "Heritage Village": ("Gainesville", "VA"),
    "The Cypress of Raleigh": ("Raleigh", "NC"),
    "Sedgebrook": ("Lincolnshire", "IL", "Radford Green at Sedgebrook"),
    "The Delaney at The Vale": ("Woburn", "MA", "Delaney at the Vale"),
    "The Forum": ("Cupertino", "CA"),
    "Freedom Pointe at The Villages": ("The Villages", "FL"),
    "Cypress Village": ("Jacksonville", "FL"),
    "Freedom Square of Seminole": ("Seminole", "FL"),
    "Lake Seminole Square": ("Seminole", "FL"),
    "Freedom Village at Brandywine": ("Coatesville", "PA", "Freedom Village Brandywine"),
    "The Cypress of Charlotte": ("Charlotte", "NC"),
    "Rolling Green Village": ("Greenville", "SC"),
    "The Glenview at Pelican Bay": ("Naples", "FL"),
    "The Arlington": ("Naples", "FL"),
    "Danberry at Inverness": ("Hoover", "AL"),
    "Galleria Woods": ("Hoover", "AL"),
    "Harbour Village": ("Greendale", "WI"),
    "Clarendale Arcadia": ("Phoenix", "AZ"),
    "Moldaw Residences": ("Palo Alto", "CA"),
    "Freedom Village of Bradenton": ("Bradenton", "FL", "Freedom Village Bradenton"),
    "South Port Square": ("Port Charlotte", "FL"),
    "Clarendale Six Corners": ("Chicago", "IL"),
    "The Clare": ("Chicago", "IL"),
    "Clarendale of St. Peters": ("Saint Peters", "MO"),
    "The Emerson at St. Peters": ("Saint Peters", "MO", "Emerson at St. Peters"),
    "The Cedars of Chapel Hill": ("Chapel Hill", "NC"),
    "The Cypress of Hilton Head": ("Hilton Head Island", "SC", "The Cypress Club of Hilton Head"),
    "The Bayshore on Hilton Head Island": ("Hilton Head Island", "SC", "The Bayshore on HHI", "The Bayshore"),
    "Carillon Senior Living": ("Lubbock", "TX"),
    "Essex Meadows": ("Essex", "CT"),
    "StoneRidge": ("Mystic", "CT"),
    "Lake Port Square": ("Leesburg", "FL"),
    "Brandon Wilde": ("Evans", "GA"),
    "Green Hills": ("Ames", "IA", "Green Hills Community"),
    "Porters Neck Village": ("Wilmington", "NC"),
    "Rose Senior Living Beachwood": ("Beachwood", "OH", "Rose Senior Living"),
    "The Avalon of Lewis Center": ("Lewis Center", "OH", "The Avalon"),
    "Green Country Village Senior Living": ("Bartlesville", "OK"),
    "Bellingham at West Chester": ("West Chester", "PA", "Bellingham West Chester"),
    "The Marshes of Skidaway Island": ("Savannah", "GA"),
    "The Oaks at Bartlett": ("Bartlett", "IL"),
    "Hearthwood Senior Living": ("Bartlett", "IL", "The Hearthwood"),
    "Greenwood Village South": ("Greenwood", "IN"),
    "Clarendale West End": ("Nashville", "TN"),
    "Clarendale of Mokena": ("Mokena", "IL", "Clarendale Mokena"),
    "Clarendale of Addison": ("Addison", "IL"),
    "Richmond Place Senior Living": ("Lexington", "KY"),
    "Clarendale Ann Arbor": ("Ann Arbor", "MI"),
    "Rose Senior Living Avon": ("Avon", "OH", "Rose Senior Living"),
    "Laural Parke": ("Houston", "TX"),
    "Amavida": ("Fort Myers", "FL"),
    "Sterling Estates of West Cobb": ("Marietta", "GA"),
    "Sterling Estates East Cobb": ("Marietta", "GA"),
    "Monarch Landing": ("Naperville", "IL"),
    "Marquette": ("Indianapolis", "IN"),
    "Trillium Woods": ("Plymouth", "MN"),
    "Havenwood of Buffalo": ("Buffalo", "MN"),
    "Havenwood of Burnsville": ("Burnsville", "MN"),
    "Havenwood of Richfield": ("Richfield", "MN"),
    "Havenwood of Minnetonka": ("Minnetonka", "MN"),
    "Havenwood of Maple Grove": ("Maple Grove", "MN"),
    "The Heritage at Irene Woods": ("Memphis", "TN", "Heritage at Irene Woods"),
    "Regency Oaks": ("Clearwater", "FL"),
    "Wyndemere": ("Wheaton", "IL", "Wyndemere Senior Living"),
    "Clarendale of Algonquin": ("Algonquin", "IL"),
    "Rose Senior Living Carmel": ("Carmel", "IN", "Rose Senior Living - Carmel", "Rose Senior Living"),
    "The Avalon of Bloomfield Township": ("Bloomfield", "MI", "The Avalon"),
    "The Avalon of Commerce Township": ("Commerce Township", "MI", "Avalon of Commerce Township", "The Avalon"),
    "Rose Senior Living Clinton Township": ("Clinton Township", "MI", "Rose Senior Living"),
    "Rose Senior Living Farmington Hills": ("Farmington Hills", "MI", "Rose Senior Living"),
    "Rose Senior Living at Providence Park": ("Novi", "MI", "Rose Senior Living"),
    "The Village at Gleannloch Farms": ("Spring", "TX"),
    "Oakmont Gardens": ("Santa Rosa", "CA"),
    "Millcroft Living": ("Newark", "DE"),
    "Windsor at Celebration": ("Celebration", "FL"),
    "Clarendale Clayton": ("Clayton", "MO"),
    "The Delaney at The Green": ("Florham Park", "NJ"),
    "The Delaney at Lake Waco": ("Waco", "TX"),
    "Broadway Cityview": ("Fort Worth", "TX"),
    "The Delaney at Parkway Lakes": ("Richmond", "TX", "The Delaney Parkway Lakes"),
    "Sandhill Cove": ("Palm City", "FL", "Sandhill Cove Retirement Community"),
    "The Lodge of Northbrook": ("Northbrook", "IL"),
    "The Avalon of Auburn Hills": ("Auburn Hills", "MI", "The Avalon"),
    "The Delaney of Bridgewater": ("Bridgewater", "NJ"),
    "The Heritage at Brentwood": ("Brentwood", "TN"),
    "Clarendale at Indian Lake": ("Hendersonville", "TN"),
    "The Delaney at Georgetown Village": ("Georgetown", "TX", "Delaney at Georgetown Village", "Delaney Georgetown"),
    "The Virginian": ("Fairfax", "VA"),
    "Timber Ridge at Talus": ("Issaquah", "WA"),
    "Clarendale of Chandler": ("Chandler", "AZ"),
    "Casa de las Campanas": ("San Diego", "CA"),
    "Freedom Plaza": ("Sun City Center", "FL"),
    "The Roosevelt at Salt Creek": ("Elmhurst", "IL"),
    "Blakehurst": ("Towson", "MD"),
    "Freedom Village at Holland": ("Holland", "MI", "Freedom Village"),
    "The Avalon of New Albany": ("New Albany", "OH", "The Avalon"),
    "The Village at The Woodlands Waterway": ("The Woodlands", "TX"),
    "Eastcastle Place": ("Milwaukee", "WI"),
}
LCS_BODY_WINDOW = 1500      # the opening of the body, where the community introduces itself


def _lcs_build():
    spell = []
    for name, v in LCS_COMMUNITIES.items():
        town, st = v[0], v[1]
        for s in (name,) + tuple(v[2:]):
            words = [re.escape(w) for w in re.split(r"\s+", clean(s).replace("&", "and"))]
            rx = re.compile(r"(?<![A-Za-z0-9])" + r"[\s\-]+".join(words) + r"(?![A-Za-z0-9])", re.I)
            spell.append((rx, len(s), name, town, st))
    spell.sort(key=lambda t: -t[1])      # longest spelling first ("Freedom Village at Holland" before "Freedom Village")
    return spell


_LCS_SPELL = _lcs_build()


def lcs_body_site(description: str, city: str = "", state: str = "") -> str:
    """The community the body's opening names, or "": exactly one community
    (its spellings inside a longer one's match are not a second one), and the
    posting in that community's town or in none."""
    text = clean((description or "")[:LCS_BODY_WINDOW]).replace("&", "and")
    if not text:
        return ""
    here = (_town_key(city), (state or "").upper()) if (city and state) else None
    found, spans = set(), []
    for rx, _n, name, town, st in _LCS_SPELL:
        if here is not None and (_town_key(town), st) != here:
            continue
        for m in rx.finditer(text):
            if any(a <= m.start() and m.end() <= b for a, b in spans):
                continue
            spans.append((m.start(), m.end()))
            found.add((name, town, st))
    if len({f[0] for f in found}) != 1:
        return ""
    name, town, st = next(iter(found))
    if city or state:
        if state and state != st:
            return ""
        if city and _town_key(city) != _town_key(town):
            return ""
    return name


# ═══════════════════════════ ENCOMPASS HEALTH (town) ═══════════════════════
# One inpatient rehabilitation hospital per town: (public name, town, ST,
# source). Towns: the CMS / AHRQ hospital list in the database ("cms <id>",
# system_hospitals), else the postings' own town where the employer's site
# lists the hospital under that town ("rows"). Names: the hospital's CMS name
# where it is complete; the employer's own naming, "Encompass Health
# Rehabilitation Hospital of <place>", where its location page is
# encompasshealth.com/locations/<place>-rehab; a joint venture's own name
# before ", an affiliate of Encompass Health". Left out (their rows keep the
# label): a town with two Encompass hospitals (Houston, San Antonio, Las
# Vegas, Tucson, Memphis, Knoxville), the head office town (Birmingham, AL:
# Lakeshore and the corporate office), Danville, PA (the Geisinger hospital and
# its outpatient sites), and every hospital whose public name could not be
# sourced (Braintree, Fayetteville AR, Kingsport, Martin, Naples, Stuart, Vero
# Beach, Elizabethtown, Rockford, Shenandoah, Wildwood, Little River, Murrells
# Inlet, Norwood, Overland Park, North Charleston, Audubon, Avondale, Lowell,
# Town and Country, St. Peters, Puerto Rico).
def E(name, town, st, source):
    return (name, town, st, source)


ENCOMPASS_HOSPITALS = [
    E("Encompass Health Rehabilitation Hospital of Dothan", "Dothan", "AL", "cms 013030; name: encompasshealth.com/locations/dothan-rehab"),
    E("Encompass Health Rehabilitation Hospital of Gadsden", "Gadsden", "AL", "cms 013032; name: encompasshealth.com/locations/gadsden-rehab"),
    E("Encompass Health Rehabilitation Hospital of Montgomery", "Montgomery", "AL", "cms 013028; name: encompasshealth.com/locations/montgomery-rehab"),
    E("Encompass Health Rehabilitation Hospital of North Alabama", "Huntsville", "AL", "cms 013029; name: the hospital's posted name"),
    E("Encompass Health Rehabilitation Hospital of Shelby County", "Pelham", "AL", "cms 013031; name: encompasshealth.com/locations/shelby-county-rehab"),
    E("Regional Rehabilitation Hospital", "Phenix City", "AL", "cms 013033"),
    E("Encompass Health Rehabilitation Hospital of Fort Smith", "Fort Smith", "AR", "cms 043028; name: encompasshealth.com/locations/fort-smith-rehab"),
    E("Encompass Health Rehabilitation Hospital of Jonesboro", "Jonesboro", "AR", "cms 043029; name: encompasshealth.com/locations/jonesboro-rehab"),
    E("CHI St. Vincent Hot Springs Rehabilitation Hospital", "Hot Springs", "AR", "cms 043035"),
    E("CHI St. Vincent Sherwood Rehabilitation Hospital", "Sherwood", "AR", "cms 043031"),
    E("Encompass Health Valley of the Sun Rehabilitation Hospital", "Glendale", "AZ", "cms 033032; name: encompasshealth.com/locations/valley-of-the-sun-rehab"),
    E("Encompass Health Rehabilitation Hospital of East Valley", "Mesa", "AZ", "cms 033037; name: encompasshealth.com/locations/east-valley-rehab"),
    E("Encompass Health Rehabilitation Hospital of Scottsdale", "Scottsdale", "AZ", "cms 033025; name: encompasshealth.com/locations/scottsdale-rehab"),
    E("Yuma Rehabilitation Hospital", "Yuma", "AZ", "cms 033034"),
    E("Encompass Health Rehabilitation Hospital of Bakersfield", "Bakersfield", "CA", "cms 053031; name: encompasshealth.com/locations/bakersfield-rehab"),
    E("Encompass Health Rehabilitation Hospital of Modesto", "Modesto", "CA", "cms 053040; name: encompasshealth.com/locations/modesto-rehab"),
    E("Encompass Health Rehabilitation Hospital of Murrieta", "Murrieta", "CA", "cms 053042; name: encompasshealth.com/locations/murrieta-rehab"),
    E("Encompass Health Rehabilitation Hospital of Tustin", "Tustin", "CA", "cms 053034; name: encompasshealth.com/locations/tustin-rehab"),
    E("Encompass Health Rehabilitation Hospital of Colorado Springs", "Colorado Springs", "CO", "cms 063030; name: encompasshealth.com/locations/colorado-springs-rehab"),
    E("Encompass Health Rehabilitation Hospital of Littleton", "Littleton", "CO", "cms 063034; name: encompasshealth.com/locations/littleton-rehab"),
    E("Encompass Health Rehabilitation Hospital of Danbury", "Danbury", "CT", "rows; name: encompasshealth.com/locations/danbury-rehab"),
    E("Encompass Health Rehabilitation Hospital of Middletown", "Middletown", "DE", "cms 083026; name: encompasshealth.com/locations/middletown-rehab"),
    E("Encompass Health Rehabilitation Hospital of Altamonte Springs", "Altamonte Springs", "FL", "cms 103045; name: encompasshealth.com/locations/altamonte-springs-rehab"),
    E("Encompass Health Rehabilitation Hospital of Spring Hill", "Brooksville", "FL", "cms 103042; name: encompasshealth.com/locations/springhill-rehab"),
    E("Encompass Health Rehabilitation Hospital of Cape Coral", "Cape Coral", "FL", "cms 103053; name: encompasshealth.com/locations/cape-coral-rehab"),
    E("Encompass Health Rehabilitation Hospital of Clermont", "Clermont", "FL", "cms 103055"),
    E("Encompass Health Rehabilitation Hospital of Daytona Beach", "Daytona Beach", "FL", "rows; name: encompasshealth.com/locations/daytona-beach-rehab"),
    E("Encompass Health Rehabilitation Hospital of Jacksonville", "Jacksonville", "FL", "cms 103052; name: encompasshealth.com/locations/jacksonville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Kissimmee", "Kissimmee", "FL", "rows; name: encompasshealth.com/locations/kissimmee-rehab"),
    E("Encompass Health Rehabilitation Hospital of Lake Worth", "Lake Worth", "FL", "rows; name: encompasshealth.com/locations/lake-worth-rehab"),
    E("Encompass Health Rehabilitation Hospital of Lakeland", "Lakeland", "FL", "cms 103050; name: encompasshealth.com/locations/lakeland-rehab"),
    E("Encompass Health Rehabilitation Hospital of Largo", "Largo", "FL", "cms 103037; name: encompasshealth.com/locations/largo-rehab"),
    E("Encompass Health Rehabilitation Hospital of North Tampa", "Lutz", "FL", "cms 103047; name: encompasshealth.com/locations/north-tampa-rehab"),
    E("Sea Pines Rehabilitation Hospital", "Melbourne", "FL", "cms 103034"),
    E("Encompass Health Rehabilitation Hospital of Ocala", "Ocala", "FL", "cms 103043; name: encompasshealth.com/locations/ocala-rehab"),
    E("Encompass Health Rehabilitation Hospital of Panama City", "Panama City", "FL", "cms 103040; name: encompasshealth.com/locations/panama-city-rehab"),
    E("Encompass Health Rehabilitation Hospital of Pensacola", "Pensacola", "FL", "cms 103048; name: encompasshealth.com/locations/pensacola-rehab"),
    E("Encompass Health Rehabilitation Hospital of St. Augustine", "Saint Augustine", "FL", "cms 103049; name: encompasshealth.com/locations/st-augustine-rehab"),
    E("Encompass Health Rehabilitation Hospital of St. Petersburg", "Saint Petersburg", "FL", "rows; name: encompasshealth.com/locations/st-petersburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Sarasota", "Sarasota", "FL", "cms 103031; name: encompasshealth.com/locations/sarasota-rehab"),
    E("Encompass Health Rehabilitation Hospital of Sunrise", "Sunrise", "FL", "cms 103028; name: encompasshealth.com/locations/sunrise-rehab"),
    E("Encompass Health Rehabilitation Hospital of Tallahassee", "Tallahassee", "FL", "cms 103033; name: encompasshealth.com/locations/tallahassee-rehab"),
    E("Encompass Health Rehabilitation Hospital of Wesley Chapel", "Wesley Chapel", "FL", "rows; name: encompasshealth.com/locations/wesley-chapel-rehab"),
    E("Encompass Health Rehabilitation Hospital of Atlanta", "Atlanta", "GA", "rows; name: encompasshealth.com/locations/atlanta-rehab"),
    E("Walton Rehabilitation Hospital", "Augusta", "GA", "cms 113030"),
    E("Encompass Health Rehabilitation Hospital of Cumming", "Cumming", "GA", "cms 113034; name: encompasshealth.com/locations/cumming-rehab"),
    E("Rehabilitation Hospital of Henry", "McDonough", "GA", "cms 113035"),
    E("Rehabilitation Hospital of Newnan", "Newnan", "GA", "cms 113032"),
    E("Encompass Health Rehabilitation Hospital of Savannah", "Savannah", "GA", "cms 113033; name: encompasshealth.com/locations/savannah-rehab"),
    E("University of Iowa Health Network Rehabilitation Hospital", "Coralville", "IA", "cms 163027"),
    E("Saint Alphonsus Regional Rehabilitation Hospital", "Boise", "ID", "cms 133028"),
    E("Encompass Health Rehabilitation Hospital of Libertyville", "Libertyville", "IL", "cms 143031; name: encompasshealth.com/locations/libertyville-rehab"),
    E("The Quad Cities Rehabilitation Institute", "Moline", "IL", "cms 143032"),
    E("The Rehabilitation Institute of Southern Illinois", "Shiloh", "IL", "cms 143030"),
    E("Encompass Health Deaconess Rehabilitation Hospital", "Newburgh", "IN", "cms 153025"),
    E("Kansas Rehabilitation Hospital", "Topeka", "KS", "cms 173025"),
    E("Encompass Health Rehabilitation Hospital of Northern Kentucky", "Edgewood", "KY", "cms 183027; name: the hospital's posted name"),
    E("Cardinal Hill Rehabilitation Hospital", "Lexington", "KY", "cms 183026"),
    E("Encompass Health Rehabilitation Hospital of Alexandria", "Alexandria", "LA", "cms 193031; name: encompasshealth.com/locations/alexandria-rehab"),
    E("Encompass Health Rehabilitation Hospital of Shreveport", "Shreveport", "LA", "cms 713028; name: encompasshealth.com/locations/shreveport-rehab"),
    E("Encompass Health Rehabilitation Hospital of New England at Beverly", "Beverly", "MA", "rows; name: encompasshealth.com/locations/new-england-rehab-beverly"),
    E("Encompass Health Rehabilitation Hospital of Western Massachusetts", "Ludlow", "MA", "cms 223030; name: encompasshealth.com/locations/western-mass-rehab"),
    E("Encompass Health Rehabilitation Hospital of New England", "Woburn", "MA", "cms 223026; name: encompasshealth.com/locations/new-england-rehab"),
    E("Fairlawn Rehabilitation Hospital", "Worcester", "MA", "cms 223029"),
    E("Encompass Health Rehabilitation Hospital of Bowie", "Bowie", "MD", "cms 213030"),
    E("Encompass Health Rehabilitation Hospital of Salisbury", "Salisbury", "MD", "cms 213028; name: encompasshealth.com/locations/salisbury-rehab"),
    E("Encompass Health Rehabilitation Hospital of Bangor", "Bangor", "ME", "rows; name: encompasshealth.com/locations/bangor-rehab"),
    E("New England Rehabilitation Hospital of Portland", "Portland", "ME", "cms 203025; name: encompasshealth.com/locations/nerhp"),
    E("Rusk Rehabilitation Hospital", "Columbia", "MO", "cms 263027"),
    E("The Rehabilitation Institute of St. Louis", "Saint Louis", "MO", "rows; name: encompasshealth.com/locations/rehab-institute-stl"),
    E("Encompass Health Rehabilitation Hospital of Gulfport", "Gulfport", "MS", "cms 253027; name: encompasshealth.com/locations/gulfport-rehab"),
    E("Novant Health Rehabilitation Hospital", "Winston-Salem", "NC", "cms 343027"),
    E("Encompass Health Rehabilitation Hospital of Grand Forks", "Grand Forks", "ND", "rows; name: encompasshealth.com/locations/grandforks-rehab"),
    E("Encompass Health Rehabilitation Hospital of Concord", "Concord", "NH", "cms 303027; name: encompasshealth.com/locations/concord-rehab"),
    E("Encompass Health Rehabilitation Hospital of Tinton Falls", "Tinton Falls", "NJ", "cms 313035; name: encompasshealth.com/locations/tinton-falls-rehab"),
    E("Encompass Health Rehabilitation Hospital of Toms River", "Toms River", "NJ", "cms 313029; name: encompasshealth.com/locations/toms-river-rehab"),
    E("Encompass Health Rehabilitation Hospital of Vineland", "Vineland", "NJ", "cms 313036; name: encompasshealth.com/locations/vineland-rehab"),
    E("Encompass Health Rehabilitation Hospital of Albuquerque", "Albuquerque", "NM", "cms 323027; name: encompasshealth.com/locations/albuquerque-rehab"),
    E("Encompass Health Rehabilitation Hospital of Henderson", "Henderson", "NV", "cms 293032; name: encompasshealth.com/locations/henderson-rehab"),
    E("The Rehabilitation Institute of Ohio", "Dayton", "OH", "cms 363033"),
    E("Encompass Health Rehabilitation Hospital of Toledo", "Toledo", "OH", "cms 363043; name: encompasshealth.com/locations/toledo-rehab"),
    E("Mount Carmel Rehabilitation Hospital", "Westerville", "OH", "cms 363042"),
    E("St. John Rehabilitation Hospital", "Broken Arrow", "OK", "cms 373034"),
    E("Ascension Saint John Rehabilitation Hospital of Owasso", "Owasso", "OK", "cms 373039"),
    E("Encompass Health Rehabilitation Hospital of Altoona", "Altoona", "PA", "cms 393040; name: encompasshealth.com/locations/altoona-rehab"),
    E("Encompass Health Rehabilitation Hospital of Concordville", "Concordville", "PA", "rows; name: encompasshealth.com/locations/concordville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Erie", "Erie", "PA", "cms 393046; name: encompasshealth.com/locations/erie-rehab"),
    E("Encompass Health Rehabilitation Hospital of Mechanicsburg", "Mechanicsburg", "PA", "cms 393031; name: encompasshealth.com/locations/mechanicsburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Harmarville", "Pittsburgh", "PA", "cms 393027; name: encompasshealth.com/locations/harmarville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Nittany Valley", "Pleasant Gap", "PA", "cms 393039; name: encompasshealth.com/locations/nittany-valley-rehab"),
    E("Encompass Health Rehabilitation Hospital of Reading", "Reading", "PA", "cms 393026; name: encompasshealth.com/locations/reading-rehab"),
    E("Encompass Health Rehabilitation Hospital of Sewickley", "Sewickley", "PA", "cms 393045; name: encompasshealth.com/locations/sewickley-rehab"),
    E("Encompass Health Rehabilitation Hospital of York", "York", "PA", "cms 393037; name: encompasshealth.com/locations/york-rehab"),
    E("Encompass Health Rehabilitation Hospital of Johnston", "Johnston", "RI", "rows; name: encompasshealth.com/locations/johnston-rehab"),
    E("AnMed Health Rehabilitation Hospital", "Anderson", "SC", "cms 423029"),
    E("Encompass Health Rehabilitation Hospital of Bluffton", "Bluffton", "SC", "cms 423032; name: encompasshealth.com/locations/bluffton-rehab"),
    E("Encompass Health Rehabilitation Hospital of Columbia", "Columbia", "SC", "cms 423025; name: encompasshealth.com/locations/columbia-rehab"),
    E("Encompass Health Rehabilitation Hospital of Florence", "Florence", "SC", "cms 423026; name: encompasshealth.com/locations/florence-rehab"),
    E("Encompass Health Rehabilitation Hospital of Fort Mill", "Fort Mill", "SC", "rows; name: encompasshealth.com/locations/fort-mill-rehab"),
    E("Encompass Health Rehabilitation Hospital of Greenville", "Greenville", "SC", "cms 423035; name: encompasshealth.com/locations/greenville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Rock Hill", "Rock Hill", "SC", "cms 423028; name: encompasshealth.com/locations/rock-hill-rehab"),
    E("Encompass Health Rehabilitation Hospital of Sioux Falls", "Sioux Falls", "SD", "cms 433027; name: encompasshealth.com/locations/sioux-falls-rehab"),
    E("Encompass Health Rehabilitation Hospital of Chattanooga", "Chattanooga", "TN", "cms 443032; name: encompasshealth.com/locations/chattanooga-rehab"),
    E("Encompass Health Rehabilitation Hospital of Cleveland", "Cleveland", "TN", "rows; name: encompasshealth.com/locations/cleveland-rehab"),
    E("Encompass Health Rehabilitation Hospital of Cookeville", "Cookeville", "TN", "rows; name: encompasshealth.com/locations/cookeville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Franklin", "Franklin", "TN", "cms 443035; name: encompasshealth.com/locations/franklin-rehab"),
    E("Encompass Health Rehabilitation Hospital of Jackson", "Jackson", "TN", "cms 443036; name: encompasshealth.com/locations/jackson-rehab"),
    E("Quillen Rehabilitation Hospital", "Johnson City", "TN", "cms 443033"),
    E("Vanderbilt Stallworth Rehabilitation Hospital", "Nashville", "TN", "cms 443028"),
    E("Encompass Health Rehabilitation Hospital of Abilene", "Abilene", "TX", "cms 673039; name: encompasshealth.com/locations/abilene-rehab"),
    E("Encompass Health Rehabilitation Hospital of Amarillo", "Amarillo", "TX", "rows; name: encompasshealth.com/locations/amarillo-rehab"),
    E("Encompass Health Rehabilitation Hospital of Arlington", "Arlington", "TX", "cms 453040; name: encompasshealth.com/locations/arlington-rehab"),
    E("Encompass Health Rehabilitation Hospital of Austin", "Austin", "TX", "rows; name: encompasshealth.com/locations/austin-rehab"),
    E("Encompass Health Rehabilitation Hospital of the Mid-Cities", "Bedford", "TX", "cms 673044; name: encompasshealth.com/locations/mid-cities-rehab"),
    E("CHI St. Joseph Health Rehabilitation Hospital", "Bryan", "TX", "cms 673065"),
    E("Encompass Health Rehabilitation Hospital of The Woodlands", "Conroe", "TX", "cms 453059; name: encompasshealth.com/locations/the-woodlands-rehab"),
    E("Encompass Health Rehabilitation Hospital of Dallas", "Dallas", "TX", "cms 673043; name: encompasshealth.com/locations/dallas-rehab"),
    E("Encompass Health Rehabilitation Hospital of City View", "Fort Worth", "TX", "cms 453042; name: encompasshealth.com/locations/city-view-rehab"),
    E("Encompass Health Rehabilitation Hospital of Humble", "Humble", "TX", "cms 453029; name: encompasshealth.com/locations/humble-rehab"),
    E("Encompass Health Rehabilitation Hospital of Katy", "Katy", "TX", "cms 673071; name: encompasshealth.com/locations/katy-rehab"),
    E("South Plains Rehabilitation Hospital", "Lubbock", "TX", "cms 673070"),
    E("Encompass Health Rehabilitation Hospital of Midland Odessa", "Midland", "TX", "cms 453057; name: the hospital's posted name"),
    E("Encompass Health Rehabilitation Hospital of Pearland", "Pearland", "TX", "cms 673066; name: encompasshealth.com/locations/pearland-rehab"),
    E("Encompass Health Rehabilitation Hospital of Plano", "Plano", "TX", "cms 453047; name: encompasshealth.com/locations/plano-rehab"),
    E("Encompass Health Rehabilitation Hospital of Prosper", "Prosper", "TX", "rows; name: encompasshealth.com/locations/prosper-rehab"),
    E("Encompass Health Rehabilitation Hospital of Richardson", "Richardson", "TX", "cms 673029; name: encompasshealth.com/locations/richardson-rehab"),
    E("Encompass Health Rehabilitation Hospital of Waco", "Robinson", "TX", "cms 673079; name: encompasshealth.com/locations/waco-rehab"),
    E("Encompass Health Rehabilitation Hospital of Round Rock", "Round Rock", "TX", "cms 673032; name: encompasshealth.com/locations/round-rock-rehab"),
    E("Shannon Rehabilitation Hospital", "San Angelo", "TX", "cms 673078"),
    E("Encompass Health Rehabilitation Hospital of Sugar Land", "Sugar Land", "TX", "cms 673042; name: encompasshealth.com/locations/sugarland-rehab"),
    E("Encompass Health Rehabilitation Hospital of Texarkana", "Texarkana", "TX", "cms 453053; name: encompasshealth.com/locations/texarkana-rehab"),
    E("CHRISTUS Trinity Mother Frances Rehabilitation Hospital", "Tyler", "TX", "cms 453056"),
    E("Encompass Health Rehabilitation Hospital of Wichita Falls", "Wichita Falls", "TX", "cms 453054; name: encompasshealth.com/locations/wichita-falls-rehab"),
    E("Encompass Health Rehabilitation Hospital of Utah", "Sandy", "UT", "cms 463025; name: encompasshealth.com/locations/utah-rehab"),
    E("Encompass Health Rehabilitation Hospital of St. George", "Saint George", "UT", "rows; name: encompasshealth.com/locations/st-george-rehab"),
    E("Encompass Health Rehabilitation Hospital of Northern Virginia", "Aldie", "VA", "cms 493033; name: encompasshealth.com/locations/northern-virginia-rehab"),
    E("Rehabilitation Hospital of Bristol", "Bristol", "VA", "cms 493034"),
    E("UVA Encompass Health Rehabilitation Hospital", "Charlottesville", "VA", "cms 493029"),
    E("Encompass Health Rehabilitation Hospital of Fredericksburg", "Fredericksburg", "VA", "cms 493032; name: encompasshealth.com/locations/fredericksburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Petersburg", "Petersburg", "VA", "cms 493031; name: encompasshealth.com/locations/petersburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Richmond", "Richmond", "VA", "cms 493028; name: encompasshealth.com/locations/richmond-rehab"),
    E("Rehabilitation Hospital of Western Wisconsin", "Eau Claire", "WI", "cms 523030"),
    E("Encompass Health Rehabilitation Hospital of Fitchburg", "Fitchburg", "WI", "rows; name: encompasshealth.com/locations/fitchburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Huntington", "Huntington", "WV", "cms 513028; name: encompasshealth.com/locations/huntington-rehab"),
    E("Encompass Health Rehabilitation Hospital of Morgantown", "Morgantown", "WV", "cms 513030; name: encompasshealth.com/locations/morgantown-rehab"),
    E("Encompass Health Rehabilitation Hospital of Parkersburg", "Parkersburg", "WV", "cms 513027; name: encompasshealth.com/locations/parkersburg-rehab"),
    E("Encompass Health Rehabilitation Hospital of Princeton", "Princeton", "WV", "cms 513026; name: encompasshealth.com/locations/princeton-rehab"),
    # opened after the CMS / AHRQ list: the town is the postings' own, the
    # employer's location page is that town's
    E("Encompass Health Rehabilitation Hospital of Apollo Beach", "Apollo Beach", "FL", "rows; name: encompasshealth.com/locations/apollo-beach-rehab"),
    E("Encompass Health Rehabilitation Hospital of Fort Myers", "Fort Myers", "FL", "rows; name: encompasshealth.com/locations/fort-myers-rehab"),
    E("Encompass Health Rehabilitation Hospital of Miami", "Miami", "FL", "rows; name: encompasshealth.com/locations/miami-rehab"),
    E("Encompass Health Rehabilitation Hospital of Athens", "Athens", "GA", "rows; name: encompasshealth.com/locations/athens-rehab"),
    E("Encompass Health Rehabilitation Hospital of Columbus", "Columbus", "GA", "rows; name: encompasshealth.com/locations/columbus-rehab"),
    E("Encompass Health Rehabilitation Hospital of Loganville", "Loganville", "GA", "rows; name: encompasshealth.com/locations/loganville-rehab"),
    E("Encompass Health Rehabilitation Hospital of Irmo", "Irmo", "SC", "rows; name: encompasshealth.com/locations/irmo-rehab"),
    E("Encompass Health Rehabilitation Hospital of Haslet", "Haslet", "TX", "rows; name: encompasshealth.com/locations/haslet-rehab"),
]
# Towns with two or more of its hospitals, or the head office: never a town match.
ENCOMPASS_AMBIGUOUS = {("houston", "TX"), ("sanantonio", "TX"), ("lasvegas", "NV"), ("tucson", "AZ"),
                       ("memphis", "TN"), ("knoxville", "TN"), ("birmingham", "AL"), ("danville", "PA")}
# A posting for an area, a region, a division or the corporate office is no one hospital's.
ENCOMPASS_NOT_SITE_RX = re.compile(
    r"\b(?:area|regional|region|divisional|division|market|corporate|remote|travel(?:ing)?|home office|"
    r"national|enterprise|multi[- ]?site)\b", re.I)


def _enc_build() -> dict:
    out: dict = {}
    for name, town, st, _src in ENCOMPASS_HOSPITALS:
        k = (_town_key(town), st)
        if k in out:
            raise ValueError(f"Encompass: two hospitals in {town}, {st}")
        if k in ENCOMPASS_AMBIGUOUS:
            raise ValueError(f"Encompass: {town}, {st} is listed as ambiguous")
        out[k] = name
    return out


ENCOMPASS_BY_TOWN = _enc_build()


def encompass_hospital(title: str, city: str, state: str) -> str:
    """The one Encompass hospital in the posting's town, or "" (no town, a
    town with none or two, the head office, an area / regional / corporate
    posting)."""
    if not city or not state or ENCOMPASS_NOT_SITE_RX.search(title or ""):
        return ""
    k = (_town_key(city), (state or "").upper())
    if k in ENCOMPASS_AMBIGUOUS:
        return ""
    return ENCOMPASS_BY_TOWN.get(k, "")


# ═══════════════════════════ STORED NAMES (nights without a body) ═══════════
# Life Care Services' community comes from the body, which the detail pass
# reads only on some nights (a known body is sent blank). On the other nights
# the name stored on the row is kept, as facilities_text does.
_KNOWN: dict = {}           # (system, job_id) -> stored hospital_name
KNOWN_SYSTEMS = (LCS,)
KNOWN_PAGE = 1000


def set_known_facilities(rows) -> int:
    _KNOWN.clear()
    for r in rows or []:
        s, jid, name = r.get("hospital_system"), str(r.get("job_id") or ""), clean(r.get("hospital_name"))
        if s in KNOWN_SYSTEMS and jid and name and name != s:
            _KNOWN[(s, jid)] = name
    return len(_KNOWN)


def load_known_facilities() -> int:
    """Read-only PostgREST pass (the SUPABASE_URL / key the upsert uses) over
    the active rows of KNOWN_SYSTEMS whose hospital_name is not the system
    label. Without credentials the map stays empty. Never raises."""
    try:
        import sb_keys
        sb_url = os.environ.get("SUPABASE_URL", "")
        sb_key = sb_keys.supabase_key()
    except Exception:
        sb_url, sb_key = "", ""
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _p
    import urllib.request as _r
    rows = []
    for system in KNOWN_SYSTEMS:
        flt = _p.quote(f"eq.{system}", safe=".")
        nflt = _p.quote(f"neq.{system}", safe=".")
        last = 0
        try:
            while True:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
                     f"&is_active=is.true&hospital_system={flt}&hospital_name={nflt}"
                     f"&id=gt.{last}&order=id.asc&limit={KNOWN_PAGE}")
                rq = _r.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
                chunk = None
                for attempt in range(3):
                    try:
                        with _r.urlopen(rq, timeout=60) as resp:
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
            logger.warning(f"Senior/rehab facilities: {system} stored names stopped ({e})")
    try:
        return set_known_facilities(rows)
    except Exception:
        set_known_facilities([])
        return 0


# ═══════════════════════════ FINALIZE PASS ══════════════════════════════════
def apply_job(job) -> str:
    """Name one Job still on its system label (finalize_jobs, before the town
    tables). Returns what happened ("" when nothing changed)."""
    system = getattr(job, "hospital_system", "") or ""
    if system not in SYSTEMS:
        return ""
    if system in (SIGNATURE, TRILOGY):
        tc = town_case(getattr(job, "city", "") or "")
        if tc and tc != job.city:
            job.city = tc
            if job.state:
                job.location = f"{tc}, {job.state}"
    if (getattr(job, "hospital_name", "") or "") != system:
        return ""
    if system == LCS:
        body = getattr(job, "description", "") or ""
        name = lcs_body_site(body, job.city or "", job.state or "") if body.strip() else ""
        if name:
            job.hospital_name = name
            if not (job.city or "").strip() and not (job.state or "").strip():
                job.city, job.state = LCS_COMMUNITIES[name][0], LCS_COMMUNITIES[name][1]   # its own town
                job.location = f"{job.city}, {job.state}"
            return "text"
        if not body.strip():
            kept = _KNOWN.get((system, str(getattr(job, "job_id", "") or "")))
            if kept:
                job.hospital_name = kept
                return "kept"
        return ""
    if system == ENCOMPASS:
        name = encompass_hospital(getattr(job, "title", "") or "", job.city or "", job.state or "")
        if name:
            job.hospital_name = name
            return "town"
        return ""
    if system == TRILOGY:
        name = trilogy_body_site(getattr(job, "description", "") or "")
        if name:
            job.hospital_name = name
            return "text"
    if system == SIGNATURE:
        name = signature_body_site(getattr(job, "description", "") or "")
        if name:
            job.hospital_name = name
            return "text"
    return ""


def apply_jobs(jobs) -> dict:
    """apply_job over a run's rows. Returns {system: {how: rows}}."""
    out: dict = {}
    for j in jobs:
        how = apply_job(j)
        if how:
            d = out.setdefault(j.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return out


def unknown_summary(top=5) -> str:
    parts = []
    for s, d in sorted(UNKNOWN.items()):
        items = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{s}: " + ", ".join(f"{k!r} {v}" for k, v in items))
    return "; ".join(parts)


# ═══════════════════════════ STORED ROWS (the change list) ═════════════════
def stored_facility(system: str, row: dict):
    """(hospital_name, city, state, source) for one stored row, from what the
    database holds (title, url, city, state, description), or None when the
    stored row cannot name its site (Brookdale and PruittHealth: the nightly
    reads the feed's field). hospital_name is the system name for an office."""
    city, state = row.get("city") or "", row.get("state") or ""
    body = row.get("description") or row.get("d") or ""
    if system == SUNRISE:
        seg = _path_segment(row.get("url") or "")
        hit = SUNRISE_SITES.get(key(seg)) if seg else None
        if hit is None:
            return None
        name, town, st, src = hit
        if not town:
            # no town in the descriptor: keep a real stored town, drop the descriptor echoed as one
            echo = key(city) == key(seg) or key(city) == key(name or "")
            town, st = ("", "") if echo else (city, state)
        return (name or SUNRISE, town, st, f"link site {seg} ({src})")
    if system == TRILOGY:
        name = trilogy_body_site(body)
        if name:
            return (name, town_case(city), state, "body LOCATION block")
        if _TRILOGY_BLOCK_RX.search(body):
            return (TRILOGY, town_case(city), state, "body LOCATION block: office or several sites")
        return None
    if system == SIGNATURE:
        name = signature_body_site(body)
        if name:
            return (name, town_case(city), state, "body About Us")
        if re.match(r"^\s*Signature HealthCARE is a family-based", body or "", re.I):
            return (SIGNATURE, town_case(city), state, "body company profile: office")
        return None
    if system == LCS:
        name = lcs_body_site(body, city, state)
        if not name:
            return None
        if not city and not state:      # a posting with no town: the community's own
            return (name, LCS_COMMUNITIES[name][0], LCS_COMMUNITIES[name][1], "body names the community; its town")
        return (name, city, state, "body names the community")
    if system == ENCOMPASS:
        name = encompass_hospital(row.get("title") or "", city, state)
        return (name, city, state, "town: the one Encompass hospital there") if name else None
    return None
